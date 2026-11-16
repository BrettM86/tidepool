package ingest

import (
	"testing"

	"github.com/stretchr/testify/require"

	"tidepool/internal/blobstore"
	"tidepool/internal/repo"
	"tidepool/internal/testutil"
)

// blobMode is one byte-store mode a test runs in. setUp returns the repo
// options selecting the mode's store and its test bucket (nil in postgres
// mode, as testutil.BlobsHoldingBytes takes it).
type blobMode struct {
	name  string
	setUp func(t *testing.T) ([]repo.Option, *testutil.S3Bucket)
}

// blobModes is the shared mode table for tests that run in both byte-store
// modes: postgres (bytes in the blobs row) and s3 (bytes in a fresh test
// bucket).
func blobModes() []blobMode {
	return []blobMode{
		{name: "postgres", setUp: func(t *testing.T) ([]repo.Option, *testutil.S3Bucket) {
			return nil, nil
		}},
		{name: "s3", setUp: func(t *testing.T) ([]repo.Option, *testutil.S3Bucket) {
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
			return []repo.Option{repo.WithBlobStore(store)}, bucket
		}},
	}
}

// newHarnessInMode is newHarness over mode's byte store, with h.blobBucket
// set to its bucket; blob_object_deletes is empty before and after the test.
func newHarnessInMode(t *testing.T, mode blobMode) *harness {
	t.Helper()
	repoOptions, bucket := mode.setUp(t)
	h := newHarnessWithRepoOptions(t, repoOptions)
	h.blobBucket = bucket
	testutil.Truncate(t, h.db, "blob_object_deletes")
	t.Cleanup(func() { testutil.Truncate(t, h.db, "blob_object_deletes") })
	return h
}

// storedCIDsOfBytes returns the CIDs of the blobs rows under did whose stored
// bytes (row or object, per h's mode) equal data.
func storedCIDsOfBytes(t *testing.T, h *harness, did string, data []byte) []string {
	t.Helper()
	var cids []string
	for _, stored := range testutil.BlobsHoldingBytes(t, h.db, h.blobBucket, data) {
		if stored.DID == did {
			cids = append(cids, stored.CID)
		}
	}
	return cids
}
