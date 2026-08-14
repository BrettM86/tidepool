// Command tidepool runs the ActivityPub→atproto bridge. main stays thin:
// config, database, migrations (dev only), a chi router that later tasks
// register their subsystems on, and graceful shutdown.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"tidepool/internal/accept"
	"tidepool/internal/ap"
	"tidepool/internal/config"
	"tidepool/internal/consume"
	"tidepool/internal/db"
	"tidepool/internal/echo"
	"tidepool/internal/identity"
	"tidepool/internal/ingest"
	"tidepool/internal/materialize"
	"tidepool/internal/optout"
	"tidepool/internal/outbound"
	"tidepool/internal/personas"
	"tidepool/internal/prune"
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

	// outboundInboxTTL memoizes a community's resolved delivery inbox; a
	// rotation is caught by the worker's cache-bypassing re-resolve on a 4xx.
	outboundInboxTTL = time.Hour
	// outboundWorkerIdle is how long a delivery worker sleeps when the queue is
	// empty before polling ClaimNext again.
	outboundWorkerIdle = time.Second
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	slog.SetDefault(logger)

	if err := dispatch(logger, os.Args[1:]); err != nil {
		logger.Error("tidepool exited with error", "error", err)
		os.Exit(1)
	}
}

// dispatch selects the long-running server or an operational one-shot command.
// Keeping migrations in the same image guarantees the schema bundled with the
// binary is exactly the schema applied by deployment automation.
func dispatch(logger *slog.Logger, args []string) error {
	switch {
	case len(args) == 0:
		return run(logger)
	case len(args) == 1 && args[0] == "migrate":
		return runMigrations(logger)
	default:
		return fmt.Errorf("usage: tidepool [migrate]")
	}
}

// runMigrations applies every pending embedded Goose migration and exits. It
// intentionally reads only DATABASE_URL: migration jobs should not need HTTP,
// PLC, relay, or key-custody configuration merely to update the schema.
func runMigrations(logger *slog.Logger) error {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return fmt.Errorf("migrate: DATABASE_URL is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	database, err := db.Open(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	defer func() { _ = database.Close() }()

	if err := db.MigrateUp(ctx, database); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	logger.Info("database migrations completed")
	return nil
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

	// The bridge's own DID (community.profile createdBy/hostedBy, the bare
	// hostname's handle, and /.well-known/did.json). An operator may
	// pre-provision one; otherwise the bridge identifies as did:web on its
	// own hostname. Derived HERE, before the resolver, so the bare-hostname
	// handle resolves to it and the did:web document is served — consumers
	// (the Coves AppView's hostedBy verification) resolve that document and
	// fail closed when it is missing.
	serviceDID := cfg.BridgeServiceDID
	if serviceDID == "" {
		serviceDID = "did:web:" + cfg.BridgeHostname
		logger.Info("BRIDGE_SERVICE_DID not set, deriving from hostname", "did", serviceDID)
	}

	// Handle resolution for the bridged handle space (task 03). Bridged
	// handles are subdomains of BRIDGE_HOSTNAME; wildcard DNS routes them
	// all here (see README, "Handle resolution & DNS").
	actors := store.NewBridgedActors(database)
	resolver := identity.NewStoreResolver(actors, cfg.BridgeHostname, serviceDID)
	router.Get("/xrpc/com.atproto.identity.resolveHandle", identity.ResolveHandleHandler(resolver, logger))
	router.Get("/.well-known/atproto-did", identity.WellKnownDIDHandler(resolver, logger))
	// Cert-issuance gate for TLS-terminating proxies with on-demand
	// issuance (production Caddy asks here before requesting a cert for a
	// bridged-handle subdomain; see docker-compose.prod.yml header).
	router.Get("/.well-known/tidepool-tls-ask", identity.TLSAskHandler(resolver, logger))
	// The bridge's own did:web document (404 when a non-did:web
	// BRIDGE_SERVICE_DID is provisioned).
	router.Get("/.well-known/did.json", identity.DIDWebHandler(serviceDID, cfg.BridgeHostname))

	// The sync surface (task 04): com.atproto.sync.* + subscribeRepos,
	// describeServer, _health — everything a relay or Jetstream needs to
	// treat Tidepool as a subscribeRepos upstream.
	custodian, err := identity.NewCustodian(cfg.BridgeKEK)
	if err != nil {
		return err
	}
	repoManager, err := repo.NewManager(database, identity.NewActorKeys(actors, custodian), logger,
		repo.WithTreeCacheSize(cfg.MSTCacheSize))
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
		Repo:           repoManager,
		Broadcaster:    broadcaster,
		Logger:         logger,
		Hostname:       cfg.BridgeHostname,
		ServiceDID:     cfg.BridgeServiceDID,
		HandleResolver: resolver,
		RatePerSecond:  float64(cfg.SyncRatePerSecond),
		RateBurst:      cfg.SyncRateBurst,
		MaxSubscribers: cfg.SyncMaxSubscribers,
	})
	if err != nil {
		return err
	}
	syncServer.Routes(router)

	// Firehose retention: prune events older than FIREHOSE_RETENTION so the
	// replay window (and the table) stays bounded.
	go tidepoolsync.RunPruner(ctx, repoManager, cfg.FirehoseRetention, 0, logger)
	// Blocks GC (task 12): reclaim head-unreachable blocks older than
	// BLOCKS_GC_RETENTION (the invariant lives in internal/repo/gc.go). A
	// sweep walks every repo's live MST — heavier than the row pruners — so
	// it runs every 6h instead of the runner's hourly default.
	go prune.Run(ctx, "blocks(unreachable)", cfg.BlocksGCRetention, 6*time.Hour, repoManager.GCBlocks, logger)

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
		UserAgent: cfg.UserAgent,
		Signer:    serviceActor.Signer(),
		// MAX_BLOB_BYTES governs media downloads only; the JSON-object cap
		// keeps its own (smaller) default.
		MaxMediaBytes:         cfg.MaxBlobBytes,
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

	objects := store.NewAPObjects(database)
	communities := store.NewCommunities(database)
	tombstones := store.NewTombstones(database)
	inboxEvents := store.NewInboxEvents(database)

	// The echo classifier reads the four tables the user origin serves from, so
	// "is this ours?" is answered against the ids this process actually minted
	// and actually serves. It is built HERE, before its first consumer: both
	// the vote aggregator's voter probe and the ingest dispatcher's envelope
	// suppression are the same classifier over the same state.
	echoClassifier, err := echo.New(echo.Options{
		Objects:         objects,
		OutboundObjects: store.NewOutboundObjects(database),
		Activities:      store.NewOutboundActivities(database),
		Actors:          store.NewAPActors(database),
	})
	if err != nil {
		return err
	}

	// The vote aggregation side channel (task 07): Like/Dislike activities
	// maintain bridge-side counts (never records), served over
	// social.coves.bridge.getVoteAggregates. Built before the materializer
	// because the materializer's actor scrub erases a deleted voter's
	// vote_events rows through it.
	voteAggregator, err := votes.NewAggregator(database, objects, communities, repoManager,
		echoClassifier, logger)
	if err != nil {
		return err
	}

	materializer, err := materialize.New(materialize.Options{
		Fetcher:     apClient,
		Objects:     objects,
		Actors:      actors,
		Communities: communities,
		Repos:       repoManager,
		Minter:      mintGate,
		Votes:       voteAggregator,
		// Restoring a NATIVE post reads its pinned CID from here: the author's
		// repo is not one this bridge hosts.
		OutboundObjects: store.NewOutboundObjects(database),
		// An inbound moderation decision updates the admissions ledger too, so
		// the operator surface reflects it when the MODERATOR acts.
		Ledger:            accept.NewAdmissions(database),
		ServiceDID:        serviceDID,
		ProfileRefreshTTL: cfg.ProfileRefreshTTL,
		MaxBlobBytes:      cfg.MaxBlobBytes,
		StrictValidation:  cfg.IsDevelopment(),
		Logger:            logger,
	})
	if err != nil {
		return err
	}

	// Task 11 retention pruners: ap_tombstones markers and undone
	// vote_events rows age out like firehose events do.
	go prune.Run(ctx, "ap_tombstones", cfg.TombstoneRetention, 0, tombstones.Prune, logger)
	go prune.Run(ctx, "vote_events(undone)", cfg.VoteEventRetention, 0, voteAggregator.PruneUndoneEvents, logger)

	// Bridged-vote-stats refresher (FOLLOWUPS locked decision 7 final
	// direction): fold changed vote_aggregates counts onto each subject's
	// materialized post/comment record as an optional bridgedStats field,
	// debounced so a hot post's votes coalesce into one record update per
	// sweep instead of one firehose event per vote. Emits through the
	// materializer (lexicon validation + mapping-CID bookkeeping) exactly
	// like every other record write.
	statsRefresher, err := votes.NewRefresher(database, objects, materializer,
		cfg.StatsRefreshInterval, cfg.StatsRefreshBatch, logger)
	if err != nil {
		return err
	}
	go statsRefresher.Run(ctx)
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
		Echo:         echoClassifier,
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
		Materializer: materializer,
		Fetcher:      apClient,
		Objects:      objects,
		Actors:       actors,
		Communities:  communities,
		Tombstones:   tombstones,
		Records:      repoManager,
		Votes:        voteAggregator,
		Backfill:     backfill,
		Echo:         echoClassifier,
		// Passed explicitly rather than left to NewHandler's default: this is the
		// store every inbound moderation decision is RECORDED in, and production
		// should not depend on a type assertion to have one.
		Moderation:     store.NewObjectModeration(database),
		Bans:           store.NewCommunityBans(database),
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
		Verifier:                      ap.NewVerifier(apClient),
		Events:                        inboxEvents,
		Queue:                         queue,
		Service:                       serviceActor,
		Fetcher:                       apClient,
		Logger:                        logger,
		IPRatePerSecond:               float64(cfg.InboxIPRatePerSecond),
		IPRateBurst:                   cfg.InboxIPRateBurst,
		SignerRatePerSecond:           float64(cfg.InboxSignerRatePerSecond),
		SignerRateBurst:               cfg.InboxSignerRateBurst,
		TombstoneConfirmRatePerSecond: float64(cfg.InboxTombstoneConfirmsPerMinute) / 60,
		TombstoneConfirmBurst:         cfg.InboxTombstoneConfirmBurst,
	})
	if err != nil {
		return err
	}
	inbox.Routes(router)

	// Automatic Follow re-send for subscriptions stuck in pending (the
	// Lemmy first-contact Accept race; task 11).
	followRetrier, err := ingest.NewFollowRetrier(ingest.FollowRetrierOptions{
		Client:      apClient,
		Communities: communities,
		Service:     serviceActor,
		Logger:      logger,
	})
	if err != nil {
		return err
	}
	go followRetrier.Run(ctx)

	admin, err := ingest.NewAdmin(ingest.AdminOptions{
		Token:        cfg.AdminToken,
		Client:       apClient,
		Materializer: materializer,
		Communities:  communities,
		Service:      serviceActor,
		Backfill:     backfill,
		Repos:        repoManager,
		Sweeper:      handler,
		Deliveries:   store.NewOutboundDeliveries(database),
		Logger:       logger,
	})
	if err != nil {
		return err
	}
	admin.Routes(router)

	// Declarative follow list (FOLLOW_LIST_PATH): converge subscriptions to
	// the repo-committed YAML on startup and every FOLLOW_LIST_INTERVAL. A
	// missing or malformed file fails startup (fail fast on a bad deploy,
	// like the migration gate); the reconciler goroutine itself only ever
	// logs — a file that breaks AFTER startup skips sweeps rather than
	// unfollowing anything.
	if cfg.FollowListPath != "" {
		if _, err := ingest.ParseFollowList(cfg.FollowListPath); err != nil {
			return err
		}
		reconciler, err := ingest.NewFollowReconciler(ingest.FollowReconcilerOptions{
			Admin:    admin,
			Path:     cfg.FollowListPath,
			Interval: cfg.FollowListInterval,
			Logger:   logger,
		})
		if err != nil {
			return err
		}
		admin.SetFollowReconciler(reconciler)
		go reconciler.Run(ctx)
	}

	// The vote-aggregate XRPC (the AppView's side-channel read).
	votesXRPC, err := votes.NewXRPC(votes.XRPCOptions{DB: database, Logger: logger})
	if err != nil {
		return err
	}
	votesXRPC.Routes(router)

	// The Coves user origin (task 13): AP Person actors for Coves users,
	// served on AP_USER_ORIGIN's Host. It shares this listener with the
	// bridge's own surface, and the Host router below decides which one a
	// request belongs to. The inbox is handed the EXISTING ingest handler —
	// the user origin publishes a shared inbox but never a second verify
	// pipeline.
	personasService, err := personas.New(personas.Options{
		DB:           database,
		Custodian:    custodian,
		UserOrigin:   cfg.APUserOrigin,
		ServiceActor: serviceActor,
		InboxHandler: inbox.InboxHandler(),
	})
	if err != nil {
		return fmt.Errorf("user origin: %w", err)
	}

	// The Jetstream consumer (task 14): the atproto half of the world this
	// bridge does not host. Default OFF until task 18 wires the e2e path —
	// it writes durable outbound state, so a deployment that has not been
	// wired end to end must not start accumulating it.
	var consumerDone <-chan struct{}
	if cfg.ConsumerEnabled {
		var acceptEngine *accept.Engine
		consumerDone, acceptEngine, err = startConsumer(ctx, cfg, database, repoManager, personasService, apClient, personasService, logger)
		if err != nil {
			return err
		}
		// The acceptance-engine admin surface (task 16): list admissions with
		// their reasons + force re-admit. It shares the /admin bearer and mounts
		// only WITH the consumer, because a force re-admit needs the engine. A
		// deployment with the consumer off has no admissions to inspect.
		acceptAdmin, err := accept.NewAdmin(accept.AdminOptions{
			Token:      cfg.AdminToken,
			Admissions: accept.NewAdmissions(database),
			Engine:     acceptEngine,
			Logger:     logger,
		})
		if err != nil {
			return err
		}
		acceptAdmin.Routes(router)
	}

	// Host routing wraps everything: the chi router keeps answering for the
	// bridge hostname and its bridged-handle subdomains, the user origin
	// answers for its own Host, and an unrecognized Host is refused with 421
	// unless AP_HOST_FALLTHROUGH_DEV is on. Both hosts naming one authority
	// (the dev default) composes by path instead.
	userHost, err := url.Parse(cfg.APUserOrigin)
	if err != nil || userHost.Host == "" {
		return fmt.Errorf("user origin: AP_USER_ORIGIN %q is not an absolute origin URL", cfg.APUserOrigin)
	}
	hostRouter, err := personas.NewHostRouter(personas.HostRouterOptions{
		ServiceHost:    cfg.BridgeHostname,
		ServiceHandler: router,
		UserHost:       userHost.Host,
		UserHandler:    personasService,
		DevFallthrough: cfg.APHostFallthroughDev,
	})
	if err != nil {
		return fmt.Errorf("host router: %w", err)
	}

	// The host router runs OUTSIDE the chi router, so chi's middleware no
	// longer covers the user origin's requests. The two that must apply to
	// every request on this listener are re-applied here in the same order
	// chi chains them (RequestID first, so a panic recovered below is logged
	// with one): without Recoverer a panic in the user surface would kill the
	// whole process, taking the bridge down with it.
	server := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           middleware.RequestID(middleware.Recoverer(hostRouter)),
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
		// Wait for the consumer's read loop to exit. Its shutdown path flushes
		// the cursor on a fresh context, so cutting the process short here
		// would lose the progress since the last periodic flush and replay it
		// on the next boot.
		if consumerDone != nil {
			select {
			case <-consumerDone:
			case <-shutdownCtx.Done():
				logger.Warn("jetstream consumer did not stop in time; its cursor may replay on restart")
			}
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

// startConsumer wires the Jetstream consumer (task 14) and starts its read
// loop. The returned channel closes when the connector's loop has exited, so
// shutdown can wait for the final cursor flush instead of racing it.
//
// Outbound delivery (task 15) is fully wired here:
//
//   - Enqueuer: the real persisting enqueuer is wired whenever CONSUMER_ENABLED
//     (this function only runs then), so intents past the rev gate always
//     persist to outbound_activities/deliveries. OUTBOUND_WORKERS>0 additionally
//     starts the delivery workers that POST them; the noop enqueuer is used only
//     when the consumer is disabled (this function does not run at all).
//
// The task-16/17 seams are still nil ON PURPOSE, each a no-op the consumer
// announces rather than a silent gap:
//
//   - Engine (task 16) nil means postv2 events are skipped at debug.
//   - RemoteDeleter (task 17) nil means a deleteRemote opt-out is recorded and
//     logged rather than acted on.
//   - Terminator (task 17) nil means a deleted account is logged rather than
//     withdrawn — never quietly downgraded to a delivery pause.
func startConsumer(
	ctx context.Context,
	cfg *config.Config,
	database *sql.DB,
	repoManager *repo.Manager,
	minter consume.ActorMinter,
	apClient *ap.Client,
	signers outbound.SignerProvider,
	logger *slog.Logger,
) (<-chan struct{}, *accept.Engine, error) {
	// The most SSRF-exposed egress in the bridge: the well-known host comes
	// from a DID document a stranger controls, so it shares the AP client's
	// guard rather than using a bare http.Client.
	resolver, err := consume.NewHandleResolver(consume.ResolverOptions{
		PLCDirectoryURL: cfg.PLCDirectoryURL,
		HTTPClient:      ap.NewGuardedHTTPClient(cfg.AllowPrivateAddresses, 30*time.Second),
		UserAgent:       cfg.UserAgent,
		// DNS is the first half of handle verification and covers every
		// self-hosted handle that publishes no well-known.
		LookupTXT: consume.DefaultLookupTXT,
		Logger:    logger,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("consumer: handle resolver: %w", err)
	}

	// The outbound delivery pipe (task 15). Because this function runs ONLY when
	// the consumer is enabled, the REAL persisting enqueuer is always wired: it
	// writes outbound_activities/deliveries inside the consumer's gate tx, so an
	// intent past the gate is never dropped. OUTBOUND_WORKERS gates only whether
	// the delivery WORKER goroutines run — with workers=0, state accumulates but
	// nothing is POSTed. (The noop enqueuer is reserved for the consumer-disabled
	// path, where nothing runs at all.)
	inboxes := outbound.NewInboxResolver(apClient, outboundInboxTTL)
	enqueuer, err := outbound.NewEnqueuer(outbound.EnqueuerOptions{
		DB:         database,
		Translator: outbound.NewTranslator(cfg.APUserOrigin),
		Inboxes:    inboxes,
		Actors:     store.NewAPActors(database),
		UserOrigin: cfg.APUserOrigin,
		Logger:     logger,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("consumer: outbound enqueuer: %w", err)
	}

	var worker *outbound.Worker
	if cfg.OutboundWorkers > 0 {
		worker, err = outbound.NewWorker(outbound.WorkerOptions{
			DB:      database,
			Actors:  store.NewAPActors(database),
			Prefs:   store.NewFederationPrefs(database),
			Signers: signers,
			Inboxes: inboxes,
			Sender:  apClient,
			Switches: outbound.ConfigSwitches{
				Disabled:            cfg.OutboundDisabled,
				Dry:                 cfg.OutboundDryRun,
				DisabledHosts:       cfg.OutboundDisabledHosts,
				DisabledCommunities: cfg.OutboundDisabledCommunities,
				DisabledActors:      cfg.OutboundDisabledActors,
			},
			Logger: logger,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("consumer: outbound worker: %w", err)
		}
	}

	// The acceptance engine (task 16): a native postv2 targeting a bridged
	// community is admitted here, the community-signed acceptance is written, and
	// the Create{Page} enqueued atomically with it. Wired whenever the consumer
	// runs, so postv2 events are admitted rather than skipped at debug.
	engine, err := accept.NewEngine(accept.Options{
		Repos:       repoManager,
		Enqueuer:    enqueuer,
		Actors:      minter,
		Resolver:    resolver,
		Communities: store.NewCommunities(database),
		Objects:     store.NewOutboundObjects(database),
		Prefs:       store.NewFederationPrefs(database),
		Admissions:  accept.NewAdmissions(database),
		// Passed explicitly rather than left to NewEngine's default: the ban gate
		// is what keeps a banned author's posts out of a community, and
		// production should not depend on a type assertion to have one.
		Bans:                     store.NewCommunityBans(database),
		APActors:                 store.NewAPActors(database),
		MaxPerAuthorPerCommunity: cfg.AdmissionMaxPerAuthorPerCommunity,
		UserOrigin:               cfg.APUserOrigin,
		Logger:                   logger,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("consumer: acceptance engine: %w", err)
	}

	// The DESTRUCTIVE tier (decision 11's second tier): one Delete{Person,
	// removeData:true} to every inbox this actor's content reached, an Undo for
	// every vote peers still hold, and a 410 on the actor document. Reached from
	// TWO doors — an explicit deleteRemote=true record, and a CONFIRMED account
	// deletion — and never inferred from either.
	purger := outbound.NewPurger(database, cfg.APUserOrigin, enqueuer).WithLogger(logger)

	// The TERMINAL tier (decision 19): a #account status of deleted is a claim
	// about a moment that may have passed, so this confirms it against PLC and
	// the PDS before anything irreversible is sent. The resolver is the
	// confirmer — it already holds the guarded egress and the directory URL —
	// and the purger only runs once that confirm comes back true.
	terminator, err := optout.NewTerminator(optout.Options{
		Confirmer: resolver,
		Prefs:     store.NewFederationPrefs(database),
		Deleter:   purger,
		Logger:    logger,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("consumer: account terminator: %w", err)
	}

	dispatcher, err := consume.NewDispatcher(consume.Options{
		DB:         database,
		Actors:     minter,
		Resolver:   resolver,
		Enqueuer:   enqueuer,
		Engine:     engine,
		Terminator: terminator,
		// The record door: enabled=false + deleteRemote=true, written by the
		// user themselves, so no confirmation is owed — the record IS the
		// instruction.
		RemoteDeleter: purger,
		// Reads committed records so a subject's community resolves for
		// mappings written before migration 016 filled community_did.
		Records:    repoManager,
		UserOrigin: cfg.APUserOrigin,
		Logger:     logger,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("consumer: dispatcher: %w", err)
	}

	// The collection filter is load-bearing: without wantedCollections this
	// would subscribe to the entire network's firehose and discard it record
	// by record.
	subscribeURL, err := consume.SubscribeURL(cfg.JetstreamURL, consume.WantedCollections())
	if err != nil {
		return nil, nil, fmt.Errorf("consumer: %w", err)
	}

	state := consume.NewPostgresStateStore(database, consume.CursorSchemaVersion)
	connector := consume.NewConnector(consume.ConsumerNative, subscribeURL, dispatcher,
		consume.WithCursorStore(state),
		consume.WithDeadLetterWriter(state),
		consume.WithConnectorLogger(logger))

	// The redriver makes transient failures self-healing: an event captured
	// during a postgres blip is replayed once the blip clears, without anyone
	// being paged.
	go consume.NewDeadLetterRedriver(state,
		map[string]consume.EventHandler{consume.ConsumerNative: dispatcher}).Run(ctx)

	// Cursor age and dead-letter depth are what make a STALLED consumer
	// visible: the process stays up and the health check stays green while
	// events quietly stop arriving.
	consume.PublishMetrics(context.Background(), connector, state)

	// The connector loop and every delivery worker share one WaitGroup, so the
	// returned done channel closes only after ALL of them have drained on ctx
	// cancellation — shutdown joins delivery the same way it joins the consumer.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := connector.Start(ctx); err != nil {
			logger.Error("jetstream consumer stopped", "error", err)
		}
	}()
	if worker != nil {
		for i := 0; i < cfg.OutboundWorkers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := worker.Run(ctx, outboundWorkerIdle); err != nil && !errors.Is(err, context.Canceled) {
					logger.Error("outbound worker stopped", "error", err)
				}
			}()
		}
		logger.Info("outbound delivery workers started",
			"count", cfg.OutboundWorkers, "dry_run", cfg.OutboundDryRun, "global_disabled", cfg.OutboundDisabled)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	logger.Info("jetstream consumer started", "url", subscribeURL)
	return done, engine, nil
}
