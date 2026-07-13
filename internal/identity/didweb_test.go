package identity

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDIDWebHandler(t *testing.T) {
	get := func(h http.HandlerFunc) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h(rec, httptest.NewRequest(http.MethodGet, "/.well-known/did.json", nil))
		return rec
	}

	t.Run("serves the document for the derived did:web identity", func(t *testing.T) {
		rec := get(DIDWebHandler("did:web:tidepool.example", "tidepool.example"))
		require.Equal(t, http.StatusOK, rec.Code)
		var doc struct {
			Context     []string `json:"@context"`
			ID          string   `json:"id"`
			AlsoKnownAs []string `json:"alsoKnownAs"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &doc))
		// The exact fields the Coves AppView's bidirectional hostedBy
		// verification requires: id equal to the DID, and alsoKnownAs
		// containing at://<handle domain>.
		assert.Equal(t, "did:web:tidepool.example", doc.ID)
		assert.Contains(t, doc.AlsoKnownAs, "at://tidepool.example")
		assert.Contains(t, doc.Context, "https://www.w3.org/ns/did/v1")
	})

	t.Run("hostname is case-normalized", func(t *testing.T) {
		rec := get(DIDWebHandler("did:web:tidepool.example", "Tidepool.Example"))
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("404 when a non-did:web service DID is provisioned", func(t *testing.T) {
		rec := get(DIDWebHandler("did:plc:ewvi7nxzyoun6zhxrhs64oiz", "tidepool.example"))
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("404 when the did:web is for a different host", func(t *testing.T) {
		rec := get(DIDWebHandler("did:web:other.example", "tidepool.example"))
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}
