// Command tidepool runs the ActivityPub→atproto bridge. main stays thin:
// config, database, migrations (dev only), a chi router that later tasks
// register their subsystems on, and graceful shutdown.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"tidepool/internal/ap"
	"tidepool/internal/config"
	"tidepool/internal/db"
	"tidepool/internal/identity"
	"tidepool/internal/ingest"
	"tidepool/internal/materialize"
	"tidepool/internal/repo"
	"tidepool/internal/store"
	tidepoolsync "tidepool/internal/sync"
	"tidepool/internal/votes"
)

const (
	readHeaderTimeout = 10 * time.Second
	writeTimeout      = 30 * time.Second
	idleTimeout       = 2 * time.Minute
	shutdownTimeout   = 15 * time.Second
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	if err := run(logger); err != nil {
		logger.Error("tidepool exited with error", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(logger)
	if err != nil {
		return err
	}

	database, err := db.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer func() { _ = database.Close() }()

	if cfg.IsDevelopment() {
		logger.Info("development environment: applying migrations on start")
		if err := db.MigrateUp(ctx, database); err != nil {
			return err
		}
	}

	router := chi.NewRouter()
	router.Use(middleware.RequestID)
	router.Use(middleware.Recoverer)

	router.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := database.PingContext(r.Context()); err != nil {
			logger.Error("health check failed", "error", err)
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// Handle resolution for the bridged handle space (task 03). Bridged
	// handles are subdomains of BRIDGE_HOSTNAME; wildcard DNS routes them
	// all here (see README, "Handle resolution & DNS").
	actors := store.NewBridgedActors(database)
	resolver := identity.NewStoreResolver(actors, cfg.BridgeHostname, cfg.BridgeServiceDID)
	router.Get("/xrpc/com.atproto.identity.resolveHandle", identity.ResolveHandleHandler(resolver, logger))
	router.Get("/.well-known/atproto-did", identity.WellKnownDIDHandler(resolver, logger))

	// The sync surface (task 04): com.atproto.sync.* + subscribeRepos,
	// describeServer, _health — everything a relay or Jetstream needs to
	// treat Tidepool as a subscribeRepos upstream.
	custodian, err := identity.NewCustodian(cfg.BridgeKEK)
	if err != nil {
		return err
	}
	repoManager, err := repo.NewManager(database, identity.NewActorKeys(actors, custodian), logger)
	if err != nil {
		return err
	}
	broadcaster, err := tidepoolsync.NewBroadcaster(cfg.DatabaseURL, 0, logger)
	if err != nil {
		return err
	}
	defer func() { _ = broadcaster.Close() }()
	go broadcaster.Run(ctx)
	syncServer, err := tidepoolsync.NewServer(tidepoolsync.Options{
		Repo:        repoManager,
		Broadcaster: broadcaster,
		Logger:      logger,
		Hostname:    cfg.BridgeHostname,
		ServiceDID:  cfg.BridgeServiceDID,
	})
	if err != nil {
		return err
	}
	syncServer.Routes(router)

	// Firehose retention: prune events older than FIREHOSE_RETENTION so the
	// replay window (and the table) stays bounded.
	go tidepoolsync.RunPruner(ctx, repoManager, cfg.FirehoseRetention, 0, logger)

	// Ask configured relays to crawl us. Development hosts are not publicly
	// reachable, so dev only logs what it would have sent (never touches a
	// live relay from a laptop) — unless ALLOW_DEV_REQUEST_CRAWL opts in,
	// which exists for the e2e harness's LOCAL BigSky relay and is refused
	// by config in production. Even then dev must not be able to poke public
	// infrastructure, so the dev-override path uses the private-only client:
	// it refuses any non-loopback/private/link-local destination at dial
	// time. Production keeps the standard SSRF-guarded client, where public
	// relays are the point.
	if len(cfg.RelayHosts) > 0 {
		if cfg.IsDevelopment() && !cfg.AllowDevRequestCrawl {
			logger.Info("development environment: skipping requestCrawl (set ALLOW_DEV_REQUEST_CRAWL=1 to send to a local relay)",
				"relays", cfg.RelayHosts, "hostname", cfg.BridgeHostname)
		} else {
			crawlClient := ap.NewGuardedHTTPClient(cfg.AllowPrivateAddresses, 30*time.Second)
			if cfg.IsDevelopment() {
				// Reached only under ALLOW_DEV_REQUEST_CRAWL: local relays only.
				crawlClient = ap.NewPrivateOnlyHTTPClient(30 * time.Second)
			}
			go tidepoolsync.RequestCrawlAll(ctx, crawlClient, cfg.RelayHosts, cfg.BridgeHostname, logger)
		}
	}

	// The ingestion pipeline (task 06): AP inbox + signature verification,
	// the durable work queue, activity dispatch into the materializer, the
	// community Follow lifecycle, outbox backfill, and consent enforcement.
	serviceKeys := store.NewServiceKeys(database)
	serviceActor, err := ap.LoadOrCreateServiceActor(ctx, serviceKeys, cfg.BridgeHostname, cfg.BridgeScheme)
	if err != nil {
		return err
	}
	apClient := ap.NewClient(ap.ClientOptions{
		UserAgent:             cfg.UserAgent,
		Signer:                serviceActor.Signer(),
		AllowPrivateAddresses: cfg.AllowPrivateAddresses,
	})

	rotationKey, err := identity.LoadOrCreateRotationKey(ctx, serviceKeys, custodian)
	if err != nil {
		return err
	}
	minter, err := identity.NewMinter(identity.MinterOptions{
		PLCDirectoryURL: cfg.PLCDirectoryURL,
		BridgeHostname:  cfg.BridgeHostname,
		BridgeScheme:    cfg.BridgeScheme,
		RotationKey:     rotationKey,
		Custodian:       custodian,
		Actors:          actors,
		HTTPClient:      ap.NewGuardedHTTPClient(cfg.AllowPrivateAddresses, 30*time.Second),
		UserAgent:       cfg.UserAgent,
		Logger:          logger,
	})
	if err != nil {
		return err
	}
	// Inbound AP activity can trigger DID minting (unseen authors), so the
	// materializer's minter goes through the rate gate.
	mintGate, err := ingest.NewMintGate(minter, cfg.MintRatePerMinute, cfg.MintBurst, logger)
	if err != nil {
		return err
	}

	// The bridge's own DID (community.profile createdBy/hostedBy). An
	// operator may pre-provision one; otherwise the bridge identifies as
	// did:web on its own hostname.
	serviceDID := cfg.BridgeServiceDID
	if serviceDID == "" {
		serviceDID = "did:web:" + cfg.BridgeHostname
		logger.Info("BRIDGE_SERVICE_DID not set, deriving from hostname", "did", serviceDID)
	}

	objects := store.NewAPObjects(database)
	communities := store.NewCommunities(database)
	tombstones := store.NewTombstones(database)
	inboxEvents := store.NewInboxEvents(database)

	materializer, err := materialize.New(materialize.Options{
		Fetcher:           apClient,
		Objects:           objects,
		Actors:            actors,
		Communities:       communities,
		Repos:             repoManager,
		Minter:            mintGate,
		ServiceDID:        serviceDID,
		ProfileRefreshTTL: cfg.ProfileRefreshTTL,
		MaxBlobBytes:      cfg.MaxBlobBytes,
		StrictValidation:  cfg.IsDevelopment(),
		Logger:            logger,
	})
	if err != nil {
		return err
	}

	// The vote aggregation side channel (task 07): Like/Dislike activities
	// maintain bridge-side counts (never records), served over
	// social.coves.bridge.getVoteAggregates.
	voteAggregator, err := votes.NewAggregator(database, objects, communities, repoManager, logger)
	if err != nil {
		return err
	}
	// Seeding imports historical scores for backfilled posts from the origin
	// instance's public API (AP alone cannot provide them).
	var seeder ingest.CountSeeder
	if cfg.SeedCountsFromAPI {
		lemmySeeder, err := votes.NewLemmySeeder(voteAggregator,
			ap.NewGuardedHTTPClient(cfg.AllowPrivateAddresses, 30*time.Second), cfg.UserAgent, logger)
		if err != nil {
			return err
		}
		seeder = lemmySeeder
	}

	backfill, err := ingest.NewBackfill(ingest.BackfillOptions{
		Fetcher:      apClient,
		Materializer: materializer,
		Communities:  communities,
		Tombstones:   tombstones,
		Seeder:       seeder,
		MaxPosts:     cfg.BackfillMaxPosts,
		// Async runs derive from the run context so a mid-run backfill stops
		// pulling remote pages once shutdown starts; the drain below waits for
		// it, and an interrupted run leaves last_backfill_at unset (resumable).
		BaseContext: ctx,
		Logger:      logger,
	})
	if err != nil {
		return err
	}
	handler, err := ingest.NewHandler(ingest.HandlerOptions{
		Materializer:   materializer,
		Fetcher:        apClient,
		Objects:        objects,
		Communities:    communities,
		Tombstones:     tombstones,
		Votes:          voteAggregator,
		Backfill:       backfill,
		ServiceActorID: serviceActor.ID,
		Logger:         logger,
	})
	if err != nil {
		return err
	}
	queue, err := ingest.NewQueue(ingest.QueueOptions{
		Events:    inboxEvents,
		Processor: handler,
		Workers:   cfg.IngestWorkers,
		Logger:    logger,
	})
	if err != nil {
		return err
	}
	go queue.Run(ctx)

	inbox, err := ingest.NewInbox(ingest.InboxOptions{
		Verifier: ap.NewVerifier(apClient),
		Events:   inboxEvents,
		Queue:    queue,
		Service:  serviceActor,
		Fetcher:  apClient,
		Logger:   logger,
	})
	if err != nil {
		return err
	}
	inbox.Routes(router)

	admin, err := ingest.NewAdmin(ingest.AdminOptions{
		Token:        cfg.AdminToken,
		Client:       apClient,
		Materializer: materializer,
		Communities:  communities,
		Service:      serviceActor,
		Backfill:     backfill,
		Logger:       logger,
	})
	if err != nil {
		return err
	}
	admin.Routes(router)

	// The vote-aggregate XRPC (the AppView's side-channel read).
	votesXRPC, err := votes.NewXRPC(votes.XRPCOptions{DB: database, Logger: logger})
	if err != nil {
		return err
	}
	votesXRPC.Routes(router)

	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           router,
		ReadHeaderTimeout: readHeaderTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
	}

	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("tidepool listening",
			"addr", cfg.ListenAddr,
			"environment", cfg.Environment,
			"bridge_hostname", cfg.BridgeHostname,
		)
		serverErrors <- server.ListenAndServe()
	}()

	select {
	case err := <-serverErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	case <-ctx.Done():
		logger.Info("shutdown signal received, draining connections")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("graceful shutdown: %w", err)
		}
		// Drain in-flight backfill. ctx is already cancelled, so async runs
		// are unwinding (they stop pulling remote pages and leave
		// last_backfill_at unset, which is resumable). Wait bounded so a stuck
		// run can't hold shutdown open past the deadline.
		drained := make(chan struct{})
		go func() { backfill.Wait(); close(drained) }()
		select {
		case <-drained:
		case <-shutdownCtx.Done():
			logger.Warn("backfill drain timed out; abandoning in-flight run (resumable on restart)")
		}
		// ListenAndServe has returned by now (Shutdown guarantees it);
		// drain its error so a bind failure racing the signal still exits
		// non-zero instead of being lost in the buffered channel.
		if err := <-serverErrors; err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: %w", err)
		}
		logger.Info("shutdown complete")
		return nil
	}
}
