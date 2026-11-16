package main

import (
	"context"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"tidepool/internal/config"
	"tidepool/internal/testutil"
)

// TestOpenBlobStoreS3 (B4): s3 mode checks the bucket at startup. A bucket
// that does not exist, or credentials MinIO rejects, fail with an error that
// names the endpoint host and the bucket and never the secret; a reachable
// bucket with correct credentials opens a store.
func TestOpenBlobStoreS3(t *testing.T) {
	const wrongSecret = "wrong-secret-9d2b"
	database := testutil.DB(t)
	bucket := testutil.NewS3Bucket(t)
	endpoint, err := url.Parse(bucket.Endpoint)
	require.NoError(t, err)

	for _, tc := range []struct {
		name       string
		bucketName string
		secret     string
		wantError  bool
	}{
		{name: "bucket does not exist", bucketName: bucket.Name + "-absent", secret: bucket.SecretAccessKey, wantError: true},
		{name: "wrong secret", bucketName: bucket.Name, secret: wrongSecret, wantError: true},
		{name: "existing bucket and correct credentials", bucketName: bucket.Name, secret: bucket.SecretAccessKey},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{
				BlobStore: "s3",
				BlobS3: config.S3Settings{
					Endpoint:        bucket.Endpoint,
					Region:          bucket.Region,
					Bucket:          tc.bucketName,
					AccessKeyID:     bucket.AccessKeyID,
					SecretAccessKey: tc.secret,
				},
				AllowPrivateAddresses: true,
			}

			store, s3, err := openBlobStore(context.Background(), database, cfg, testLogger(), testutil.LocalS3Transport())
			if tc.wantError {
				require.Error(t, err)
				require.Contains(t, err.Error(), endpoint.Host)
				require.Contains(t, err.Error(), tc.bucketName)
				require.NotContains(t, err.Error(), tc.secret)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, store)
			require.NotNil(t, s3)
		})
	}
}
