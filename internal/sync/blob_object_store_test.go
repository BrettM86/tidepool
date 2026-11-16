package sync

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetBlobOfObjectStoreRowAnswers500 (B2 wiring): in postgres mode
// com.atproto.sync.getBlob answers 500 for a blobs row whose bytes are NULL
// (held in the object store), not 404 and not an empty 200.
func TestGetBlobOfObjectStoreRowAnswers500(t *testing.T) {
	const cidStr = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"
	h := newHarness(t)
	putRecord(t, h, "b2os", "repo exists")
	t.Cleanup(func() {
		if _, err := h.db.Exec(`DELETE FROM blobs WHERE did = $1 AND cid = $2`, testDID, cidStr); err != nil {
			t.Errorf("cleanup blobs: %v", err)
		}
	})
	_, err := h.db.ExecContext(context.Background(), `
		INSERT INTO blobs (did, cid, mime_type, size, bytes)
		VALUES ($1, $2, 'image/png', 11, NULL)`, testDID, cidStr)
	require.NoError(t, err, "a blobs row with NULL bytes")

	status, _ := getRaw(t, h.http.URL+"/xrpc/com.atproto.sync.getBlob?did="+testDID+"&cid="+cidStr)
	assert.Equal(t, http.StatusInternalServerError, status)
}
