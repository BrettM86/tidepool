package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"

	"tidepool/internal/blobstore"
	"tidepool/internal/config"
)

// openBlobStore builds the blob byte store cfg.BlobStore selects. Postgres
// mode returns the blobs-table store and a nil *blobstore.S3. S3 mode builds
// the bucket client (transport nil means the SSRF-guarded production
// transport, honouring cfg.AllowPrivateAddresses) and checks the bucket with
// one HEAD; a failure returns an error that names the endpoint host and the
// bucket and never carries the raw endpoint or the secret access key. The caller starts the S3
// store's delete sweeper. The secret access key is never logged or returned.
func openBlobStore(ctx context.Context, database *sql.DB, cfg *config.Config, logger *slog.Logger, transport http.RoundTripper) (blobstore.Store, *blobstore.S3, error) {
	if cfg.BlobStore != config.BlobStoreS3 {
		logger.Info("blob store", "mode", config.BlobStorePostgres)
		return blobstore.NewPostgres(database), nil, nil
	}

	// The raw endpoint is never logged or returned: config validated it as
	// scheme and host only, and on any doubt the placeholder is named.
	endpointHost := "<invalid endpoint>"
	if parsed, err := url.Parse(cfg.BlobS3.Endpoint); err == nil && parsed.User == nil && parsed.Host != "" {
		endpointHost = parsed.Host
	}
	s3Store, err := blobstore.NewS3(database, blobstore.S3Config{
		Endpoint:              cfg.BlobS3.Endpoint,
		Region:                cfg.BlobS3.Region,
		Bucket:                cfg.BlobS3.Bucket,
		AccessKeyID:           cfg.BlobS3.AccessKeyID,
		SecretAccessKey:       cfg.BlobS3.SecretAccessKey,
		AllowPrivateAddresses: cfg.AllowPrivateAddresses,
		Logger:                logger,
		Transport:             transport,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("blob store: s3 bucket %s at %s: %w", cfg.BlobS3.Bucket, endpointHost, err)
	}
	if err := s3Store.HeadBucket(ctx); err != nil {
		return nil, nil, fmt.Errorf("blob store: s3 bucket check: %w", err)
	}
	logger.Info("blob store", "mode", config.BlobStoreS3, "endpoint", endpointHost, "bucket", cfg.BlobS3.Bucket)
	return s3Store, s3Store, nil
}
