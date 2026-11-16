package materialize

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"tidepool/internal/blobstore"
	"tidepool/internal/repo"
	"tidepool/internal/testutil"
)

// blobMode is one byte-store mode a test runs in. setUp returns the repo
// options selecting the mode's store, its test bucket (nil in postgres
// mode, as testutil.BlobsHoldingBytes takes it) and the s3 store (nil in
// postgres mode).
type blobMode struct {
	name  string
	setUp func(t *testing.T) ([]repo.Option, *testutil.S3Bucket, *blobstore.S3)
}

// blobModes is the shared mode table for tests that run in both byte-store
// modes: postgres (bytes in the blobs row) and s3 (bytes in a fresh test
// bucket).
func blobModes() []blobMode {
	return []blobMode{
		{name: "postgres", setUp: func(t *testing.T) ([]repo.Option, *testutil.S3Bucket, *blobstore.S3) {
			return nil, nil, nil
		}},
		{name: "s3", setUp: func(t *testing.T) ([]repo.Option, *testutil.S3Bucket, *blobstore.S3) {
			t.Helper()
			bucket := testutil.NewS3Bucket(t)
			require.NotNil(t, bucket.Client, "testutil.NewS3Bucket must return a bucket with a client")
			store, err := blobstore.NewS3(testutil.DB(t), blobstore.S3Config{
				Endpoint:              bucket.Endpoint,
				Region:                bucket.Region,
				Bucket:                bucket.Name,
				AccessKeyID:           bucket.AccessKeyID,
				SecretAccessKey:       bucket.SecretAccessKey,
				AllowPrivateAddresses: true,
				Transport:             testutil.LocalS3Transport(),
			})
			require.NoError(t, err)
			return []repo.Option{repo.WithBlobStore(store)}, bucket, store
		}},
	}
}

// closedLoopbackEndpoint returns an http endpoint on a loopback port that
// was listened on and then closed, so connections to it are refused: the
// object store cut off.
func closedLoopbackEndpoint(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	return "http://" + address
}
