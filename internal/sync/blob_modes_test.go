package sync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"tidepool/internal/blobstore"
	"tidepool/internal/repo"
	"tidepool/internal/testutil"
)

// blobMode is one byte-store mode a getBlob test runs in. setUp returns the
// repo options selecting the mode's store (none in postgres mode).
type blobMode struct {
	name  string
	setUp func(t *testing.T) []repo.Option
}

// blobModes is the mode table for the getBlob tests: postgres (bytes in the
// blobs row) and s3 (bytes in a fresh test bucket).
func blobModes() []blobMode {
	return []blobMode{
		{name: "postgres", setUp: func(*testing.T) []repo.Option { return nil }},
		{name: "s3", setUp: func(t *testing.T) []repo.Option {
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
			return []repo.Option{repo.WithBlobStore(store)}
		}},
	}
}

// newHarnessInMode is newHarness over mode's byte store; blob_object_deletes
// is empty before and after the test.
func newHarnessInMode(t *testing.T, mode blobMode) *harness {
	t.Helper()
	h := newHarnessWithRepoOptions(t, 500*time.Millisecond, mode.setUp(t))
	t.Cleanup(func() { testutil.Truncate(t, h.db, "blob_object_deletes") })
	return h
}
