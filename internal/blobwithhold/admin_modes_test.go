package blobwithhold_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"

	"tidepool/internal/testutil"
)

// adminMode is the byte-store mode an admin world runs in: postgres (bytes
// in the blobs row) or s3 (bytes in a fresh test bucket, reached through an
// objectDeleteGate).
type adminMode struct {
	name string
	s3   bool
}

// adminModes is the mode table for the admin tests that store blobs.
func adminModes() []adminMode {
	return []adminMode{{name: "postgres"}, {name: "s3", s3: true}}
}

// objectDeleteGate is a loopback reverse proxy in front of a test bucket's
// MinIO. While RefuseDeletes is set it answers every object delete (a
// DELETE, or a POST ?delete multi-object delete) with 500 instead of
// forwarding it; every other request reaches MinIO. The Host header is
// passed through, so SigV4 signatures made for the proxy verify at MinIO.
type objectDeleteGate struct {
	URL           string
	RefuseDeletes atomic.Bool
}

// newObjectDeleteGate starts a gate forwarding to bucket's endpoint, closed
// at cleanup.
func newObjectDeleteGate(t *testing.T, bucket *testutil.S3Bucket) *objectDeleteGate {
	t.Helper()
	target, err := url.Parse(bucket.Endpoint)
	require.NoError(t, err, "test bucket endpoint")
	forward := httputil.NewSingleHostReverseProxy(target)
	forward.Transport = testutil.LocalS3Transport()
	gate := &objectDeleteGate{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, multiDelete := r.URL.Query()["delete"]
		isDelete := r.Method == http.MethodDelete || (r.Method == http.MethodPost && multiDelete)
		if isDelete && gate.RefuseDeletes.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		forward.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	gate.URL = server.URL
	return gate
}

// bucketKeys lists every object key in the bucket.
func bucketKeys(t *testing.T, bucket *testutil.S3Bucket) []string {
	t.Helper()
	var keys []string
	for object := range bucket.Client.ListObjects(context.Background(), bucket.Name, minio.ListObjectsOptions{Recursive: true}) {
		require.NoError(t, object.Err, "list objects")
		keys = append(keys, object.Key)
	}
	return keys
}

// putOrphanObject stores data as the object blobs/<did>/<cid> out of band
// and queues its delete, with no blobs row: what an s3 PutBlob whose commit
// failed after the upload leaves.
func (w *adminWorld) putOrphanObject(t *testing.T, did, cidStr string, data []byte) {
	t.Helper()
	require.NotNil(t, w.bucket, "setup: an orphan object needs s3 mode")
	_, err := w.bucket.Client.PutObject(context.Background(), w.bucket.Name, "blobs/"+did+"/"+cidStr,
		bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{ContentType: blobMimeType})
	require.NoError(t, err, "setup: put the orphan object")
	_, err = w.db.Exec(`INSERT INTO blob_object_deletes (did, cid) VALUES ($1, $2)`, did, cidStr)
	require.NoError(t, err, "setup: queue the orphan object's delete")
}

// drainQueue empties blob_object_deletes (the write-ahead rows a successful
// s3 PutBlob leaves) and checks it is empty.
func (w *adminWorld) drainQueue(t *testing.T) {
	t.Helper()
	testutil.Truncate(t, w.db, "blob_object_deletes")
	require.Empty(t, w.queuedDIDs(t, ""), "blob_object_deletes is empty after the drain")
}

// queuedDIDs lists the DIDs of the blob_object_deletes rows for cidStr,
// ordered; an empty cidStr lists every row's DID.
func (w *adminWorld) queuedDIDs(t *testing.T, cidStr string) []string {
	t.Helper()
	rows, err := w.db.Query(`
		SELECT did FROM blob_object_deletes WHERE $1 = '' OR cid = $1 ORDER BY did, cid`, cidStr)
	require.NoError(t, err)
	defer rows.Close()
	var dids []string
	for rows.Next() {
		var did string
		require.NoError(t, rows.Scan(&did))
		dids = append(dids, did)
	}
	require.NoError(t, rows.Err())
	return dids
}

// rawPendingObjectDeletes returns the raw JSON value of the response body's
// "pending_object_deletes" key, requiring the key to be present.
func rawPendingObjectDeletes(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fields), "body: %s", rec.Body.String())
	raw, ok := fields["pending_object_deletes"]
	require.True(t, ok, "the body carries pending_object_deletes: %s", rec.Body.String())
	return string(raw)
}
