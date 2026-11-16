package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"

	"tidepool/internal/blobstore"
	"tidepool/internal/config"
	"tidepool/internal/db"
)

// errBlobStoreNotS3 is migrate-blobs refusing to run outside s3 mode: there
// is no bucket to copy into.
var errBlobStoreNotS3 = errors.New("migrate-blobs: BLOB_STORE must be s3")

// defaultMigrateBlobsConcurrency is how many rows migrate-blobs copies at
// once without --concurrency.
const defaultMigrateBlobsConcurrency = 4

// parseMigrateBlobsArgs parses the arguments after "migrate-blobs" and
// returns --concurrency (default 4). A non-positive or non-integer value, an
// unknown flag or a positional argument is refused.
func parseMigrateBlobsArgs(args []string) (int, error) {
	flags := flag.NewFlagSet("migrate-blobs", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	concurrency := flags.Int("concurrency", defaultMigrateBlobsConcurrency, "rows copied at once")
	if err := flags.Parse(args); err != nil {
		return 0, fmt.Errorf("migrate-blobs: %w", err)
	}
	if flags.NArg() > 0 {
		return 0, fmt.Errorf("migrate-blobs: unexpected argument %q", flags.Arg(0))
	}
	if *concurrency <= 0 {
		return 0, fmt.Errorf("migrate-blobs: --concurrency must be a positive integer, got %d", *concurrency)
	}
	return *concurrency, nil
}

// runMigrateBlobs copies every blobs row still holding its bytes into the
// configured bucket (the runbook is in docs/PRD_BLOB_OBJECT_STORAGE.md). It
// reads DATABASE_URL, ENVIRONMENT, BLOB_STORE and BLOB_S3_* only and never
// calls config.Load, so a maintenance shell with just those variables can run
// it. Anything but BLOB_STORE=s3 is refused before the database is dialled.
// Transport nil means the guarded production transport. It logs one line per
// mismatched row and one summary line, and fails when any row failed.
func runMigrateBlobs(ctx context.Context, logger *slog.Logger, concurrency int, transport http.RoundTripper) error {
	settings, err := config.LoadBlobStore()
	if err != nil {
		return fmt.Errorf("migrate-blobs: %w", err)
	}
	if settings.Mode != config.BlobStoreS3 {
		return fmt.Errorf("%w, got %s", errBlobStoreNotS3, settings.Mode)
	}
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return fmt.Errorf("migrate-blobs: DATABASE_URL is required")
	}
	database, err := db.Open(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("migrate-blobs: %w", err)
	}
	defer func() { _ = database.Close() }()

	// LoadBlobStore already pins a development endpoint to a local host, so
	// the guard is relaxed only there.
	environment := os.Getenv("ENVIRONMENT")
	cfg := &config.Config{
		BlobStore:             settings.Mode,
		BlobS3:                settings.S3,
		AllowPrivateAddresses: environment == "" || environment == config.EnvironmentDevelopment,
	}
	_, s3Store, err := openBlobStore(ctx, database, cfg, logger, transport)
	if err != nil {
		return fmt.Errorf("migrate-blobs: %w", err)
	}

	report, migrateErr := s3Store.MigratePostgresBytes(ctx, blobstore.MigrationOptions{Concurrency: concurrency})
	for _, mismatch := range report.Mismatches {
		logger.Warn("blob bytes do not match their cid; row left in place",
			"code", "blob_cid_mismatch", "did", mismatch.DID, "cid", mismatch.CID)
	}
	logger.Info("blob migration summary",
		"copied", report.Copied,
		"skipped", report.Skipped,
		"mismatched", report.Mismatched,
		"failed", report.Failed)
	if migrateErr != nil {
		return fmt.Errorf("migrate-blobs: the run stopped early and the counts above are partial: %w", migrateErr)
	}
	if report.Failed > 0 {
		return fmt.Errorf("migrate-blobs: %d rows failed to copy and keep their bytes; re-run once the cause is fixed", report.Failed)
	}
	return nil
}
