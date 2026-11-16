package materialize

import (
	"context"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/testutil"
)

// queuedObjectDeletes lists the blob_object_deletes rows as did/cid pairs,
// ordered.
func queuedObjectDeletes(t *testing.T) [][2]string {
	t.Helper()
	rows, err := testutil.DB(t).Query(`SELECT did, cid FROM blob_object_deletes ORDER BY did, cid`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var queued [][2]string
	for rows.Next() {
		var pair [2]string
		require.NoError(t, rows.Scan(&pair[0], &pair[1]))
		queued = append(queued, pair)
	}
	require.NoError(t, rows.Err())
	return queued
}

// objectExists reports whether key is in the bucket.
func objectExists(t *testing.T, bucket *testutil.S3Bucket, key string) bool {
	t.Helper()
	_, err := bucket.Client.StatObject(context.Background(), bucket.Name, key, minio.StatObjectOptions{})
	if err != nil && minio.ToErrorResponse(err).Code == minio.NoSuchKey {
		return false
	}
	require.NoError(t, err, "stat object %s", key)
	return true
}

// TestAuthorDeleteQueuesImageObjectDelete (task 04 B7): an author's delete of
// a post whose image no other record references removes the image's blobs
// row in the delete's commit (deleteAuthorRecord). In s3 mode that commit
// queues the object's delete and leaves the object for the sweeper: after it
// the queue row is there and the object still exists, and one sweep removes
// both. In postgres mode nothing is queued.
func TestAuthorDeleteQueuesImageObjectDelete(t *testing.T) {
	const (
		postID = "https://lemmy.world/post/author-delete-object-queue"
		// The author's DID and the raw sha-256 CIDv1 of imageBytes, computed
		// once outside the code under test.
		authorDID = "did:plc:tkvgcn4onp7yaeidusiiv4o2"
		imageCID  = "bafkreia2tixy5cw2qaqndvg7tdopj2mx5as6426owb3x5xy2bomaqqphuq"
		objectKey = "blobs/" + authorDID + "/" + imageCID
	)
	imageBytes := []byte("author-delete-object-queue-image-bytes")
	for _, mode := range blobModes() {
		t.Run(mode.name, func(t *testing.T) {
			repoOptions, bucket, byteStore := mode.setUp(t)
			h := newHarnessWithRepoOptions(t, repoOptions)
			h.serveLemmyWorldFixtures()
			ctx := context.Background()

			materializeImagePost(t, h, postID, "/media/author-delete-object-queue.png", imageBytes)
			require.Equal(t, imageCID, embedImageRef(t, h.recordFor(t, postID)).CID, "setup: the post's image")
			require.Equal(t, []string{authorDID}, blobHoldersOfCID(t, imageCID), "setup: the image is stored under the author")
			testutil.Truncate(t, testutil.DB(t), "blob_object_deletes")
			require.Empty(t, queuedObjectDeletes(t), "setup: blob_object_deletes is empty before the delete")

			require.NoError(t, h.m.HandleAuthorDeleteRecord(ctx, postID))

			assert.Empty(t, blobHoldersOfCID(t, imageCID), "the author's delete removes the image's blobs row")
			if byteStore == nil {
				assert.Empty(t, queuedObjectDeletes(t), "postgres mode queues no object delete")
				return
			}
			assert.Equal(t, [][2]string{{authorDID, imageCID}}, queuedObjectDeletes(t),
				"the delete's commit queues the image's object delete")
			assert.True(t, objectExists(t, bucket, objectKey), "the object is left for the sweeper")

			require.NoError(t, byteStore.SweepObjectDeletes(ctx))
			assert.Empty(t, queuedObjectDeletes(t), "the sweep drops the queue row")
			assert.False(t, objectExists(t, bucket, objectKey), "the sweep deletes the object")
		})
	}
}
