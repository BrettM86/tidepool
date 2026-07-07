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
	"tidepool/internal/repo"
	"tidepool/internal/store"
	tidepoolsync "tidepool/internal/sync"
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
	// live relay from a laptop).
	if len(cfg.RelayHosts) > 0 {
		if cfg.IsDevelopment() {
			logger.Info("development environment: skipping requestCrawl",
				"relays", cfg.RelayHosts, "hostname", cfg.BridgeHostname)
		} else {
			crawlClient := ap.NewGuardedHTTPClient(cfg.AllowPrivateAddresses, 30*time.Second)
			go tidepoolsync.RequestCrawlAll(ctx, crawlClient, cfg.RelayHosts, cfg.BridgeHostname, logger)
		}
	}

	// Later tasks register here: AP inbox + WebFinger (02/06),
	// vote aggregates XRPC (07).

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
