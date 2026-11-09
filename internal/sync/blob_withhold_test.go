package sync

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/blobwithhold"
)

// getRaw fetches url and returns its status and raw body.
func getRaw(t *testing.T, url string) (int, []byte) {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, body
}

// TestGetBlobWithheldAnswersAsAbsent (B2): a blob stored under two DIDs and
// withheld by the operator (CID given in base58btc) is served by
// com.atproto.sync.getBlob for both DIDs exactly as a never-stored CID is —
// same status, XRPC error name and body, with nothing saying "withheld".
// Inspect and Covers accept the CID in any multibase.
func TestGetBlobWithheldAnswersAsAbsent(t *testing.T) {
	const (
		otherDID = "did:plc:b2secondownerxxxxxxxxxxx"
		// The raw sha-256 CIDv1 of payload, in three multibases (computed
		// once outside the code under test).
		wantCIDBase32    = "bafkreiet3spndhanmbvtcma43thcotyqstpt5mbegss2e5qg5pw2vyjkfe"
		wantCIDBase58BTC = "zb2rhgbUF2Pgfkd5tv9EQmdB6tPUAVMVNo61UW1RxKT7pryBn"
		wantCIDBase16    = "f0155122093dc9ed19c0d606b31301cdcce274f1094df3eb02434a5a27606ebedaae12a29"
		// Never stored anywhere.
		neverStoredCID = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"
	)
	payload := []byte("tidepool B2 shared blob bytes")

	h := newHarness(t)
	ctx := context.Background()
	t.Cleanup(func() {
		if _, err := h.db.Exec(`DELETE FROM blob_withholds WHERE cid = $1`, wantCIDBase32); err != nil {
			t.Errorf("cleanup blob_withholds: %v", err)
		}
	})

	owners := []string{testDID, otherDID}
	for i, did := range owners {
		// PutBlob alone creates no repo; getBlob needs an active one.
		_, err := h.manager.PutRecord(ctx, did, testCollection, "b2r"+string(rune('a'+i)), testRecord("repo exists"))
		require.NoError(t, err)
		blob, err := h.manager.PutBlob(ctx, did, "image/png", payload)
		require.NoError(t, err)
		require.Equal(t, wantCIDBase32, cid.Cid(blob.Ref).String())
	}

	blobURL := func(did, cidStr string) string {
		return h.http.URL + "/xrpc/com.atproto.sync.getBlob?did=" + did + "&cid=" + cidStr
	}
	for _, did := range owners {
		status, body := getRaw(t, blobURL(did, wantCIDBase32))
		require.Equal(t, http.StatusOK, status, "setup: %s serves the blob before the withhold", did)
		require.Equal(t, payload, body)
	}

	state, err := blobwithhold.NewStore(h.db).Withhold(ctx, blobwithhold.OperatorRequest{
		CID:        wantCIDBase58BTC,
		ReasonCode: blobwithhold.ReasonIllegalContent,
		Actor:      "b2-operator",
	})
	require.NoError(t, err)

	for _, did := range owners {
		absentStatus, absentBody := getRaw(t, blobURL(did, neverStoredCID))
		require.Equal(t, http.StatusNotFound, absentStatus, "setup: a never-stored CID is 404")

		status, body := getRaw(t, blobURL(did, wantCIDBase32))
		assert.Equal(t, absentStatus, status, "%s: a withheld blob answers with the never-stored status", did)
		assert.Equal(t, string(absentBody), string(body), "%s: a withheld blob answers with the never-stored body", did)
		var xrpcErr map[string]string
		if assert.NoError(t, json.Unmarshal(body, &xrpcErr), "%s: body %s", did, body) {
			assert.Equal(t, "BlobNotFound", xrpcErr["error"], "%s", did)
		}
		assert.NotContains(t, string(body), "withheld", "%s: the answer must not reveal a withhold", did)
	}

	assert.Equal(t, wantCIDBase32, state.CID, "Withhold returns the canonical cid")
	store := blobwithhold.NewStore(h.db)
	for _, form := range []string{wantCIDBase32, wantCIDBase58BTC, wantCIDBase16} {
		inspected, err := store.Inspect(ctx, form)
		require.NoError(t, err)
		assert.Equal(t, wantCIDBase32, inspected.CID, "Inspect(%s) reports the canonical cid", form)
		if assert.Len(t, inspected.Withholds, 1, "Inspect(%s)", form) {
			assert.Equal(t, wantCIDBase32, inspected.Withholds[0].CID, "the persisted cid is canonical")
			assert.Nil(t, inspected.Withholds[0].LiftedAt, "the withhold is active")
		}
		for _, did := range owners {
			covered, err := store.Covers(ctx, did, form)
			require.NoError(t, err)
			assert.True(t, covered, "Covers(%s, %s)", did, form)
		}
	}
}
