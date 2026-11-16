package ingest

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"tidepool/internal/blobstore"
	"tidepool/internal/repo"
	"tidepool/internal/testutil"
)

// newHarnessBehindMinIOCut is newHarnessInMode over a real s3 store whose
// MinIO is reached through a testutil.MinIOCut: the harness's repo manager, and so its
// materializer, handler and admin, store and read bytes through it from the
// start. The proxy is up on return.
func newHarnessBehindMinIOCut(t *testing.T) (*harness, *testutil.MinIOCut) {
	t.Helper()
	var proxy *testutil.MinIOCut
	h := newHarnessInMode(t, blobMode{name: "s3 behind a cuttable proxy",
		setUp: func(t *testing.T) ([]repo.Option, *testutil.S3Bucket) {
			t.Helper()
			bucket := testutil.NewS3Bucket(t)
			endpoint, err := url.Parse(bucket.Endpoint)
			require.NoError(t, err)
			proxy = testutil.NewMinIOCut(t, endpoint.Host)
			byteStore, err := blobstore.NewS3(testutil.DB(t), blobstore.S3Config{
				Endpoint:              proxy.Endpoint(),
				Region:                bucket.Region,
				Bucket:                bucket.Name,
				AccessKeyID:           bucket.AccessKeyID,
				SecretAccessKey:       bucket.SecretAccessKey,
				AllowPrivateAddresses: true,
				Transport:             testutil.LocalS3Transport(),
			})
			require.NoError(t, err)
			return []repo.Option{repo.WithBlobStore(byteStore)}, bucket
		}})
	return h, proxy
}
