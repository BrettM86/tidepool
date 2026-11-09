package blobwithhold

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

// One raw sha-256 CIDv1 written in three multibases. The base32 form is the
// canonical one; the others were computed once outside the code under test.
const (
	canonicalCIDBase32 = "bafkreifz66dvhrlfogqmw5hbni5legpgixk4hqddnq22z24zzjj5rfw5gq"
	sameCIDBase58BTC   = "zb2rhjADXrwnpQfQ5TEbRf6sjA8ZG36MDeoEAaCjnynWXmS3V"
	sameCIDBase16      = "f01551220b9f78753c56571a0cb74e16a3ab219e645d5c3c0636c35aceb99ca53d896dd34"
)

// TestCanonicalize (B1): every multibase spelling of one CID canonicalizes
// to the same base32 string; garbage is a validation error.
func TestCanonicalize(t *testing.T) {
	for _, tc := range []struct{ encoding, input string }{
		{"base32", canonicalCIDBase32},
		{"base58btc", sameCIDBase58BTC},
		{"base16", sameCIDBase16},
	} {
		t.Run(tc.encoding, func(t *testing.T) {
			got, err := Canonicalize(tc.input)
			require.NoError(t, err)
			assert.Equal(t, canonicalCIDBase32, got, "canonical form of %s", tc.input)
		})
	}
	t.Run("garbage", func(t *testing.T) {
		_, err := Canonicalize("not-a-cid")
		require.Error(t, err, "an unparseable cid must be rejected")
		assert.True(t, errors.IsValidation(err), "an unparseable cid is a validation error, got %v", err)
	})
}

// TestAdminRejectsMalformedInput (B1): the admin routes answer 400 for an
// unparseable cid on every route that takes one, and for a reason_code
// outside {illegal-content, csam, operator-discretion}. The store has no
// database, so these must be rejected before any DB access.
func TestAdminRejectsMalformedInput(t *testing.T) {
	const token = "b1-admin-token"
	admin, err := NewAdmin(AdminOptions{Token: token, Store: NewStore(nil)})
	require.NoError(t, err)
	router := chi.NewRouter()
	admin.Routes(router)

	cases := []struct {
		name, method, path, body string
	}{
		{"withhold unparseable cid", http.MethodPost, "/admin/blobs/withhold",
			`{"cid":"not-a-cid","reason_code":"csam","actor":"alice"}`},
		{"lift unparseable cid", http.MethodPost, "/admin/blobs/lift",
			`{"cid":"not-a-cid","actor":"alice"}`},
		{"delete-bytes unparseable cid", http.MethodPost, "/admin/blobs/delete-bytes",
			`{"cid":"not-a-cid","actor":"alice","confirm":true}`},
		{"inspect unparseable cid", http.MethodGet, "/admin/blobs?cid=not-a-cid", ""},
		{"withhold unknown reason_code", http.MethodPost, "/admin/blobs/withhold",
			`{"cid":"` + canonicalCIDBase32 + `","reason_code":"spam","actor":"alice"}`},
		{"withhold non-operator reason_code", http.MethodPost, "/admin/blobs/withhold",
			`{"cid":"` + canonicalCIDBase32 + `","reason_code":"community-removal","actor":"alice"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, "https://bridge.example"+tc.path, strings.NewReader(tc.body))
			req.Header.Set("Authorization", "Bearer "+token)
			if tc.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			assert.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body.String())
		})
	}
}
