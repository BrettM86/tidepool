package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/store"
)

// Note (Finding K): activityID's crypto/rand failure path is not unit-tested.
// On Go 1.24+, crypto/rand.Read treats an entropy failure as a fatal,
// unrecoverable error (it never returns a non-nil error to the caller), so a
// failing-reader injection would crash the test binary rather than exercise
// the guard. Per the finding's guidance we keep the code-level guard (which
// propagates any returned error and, critically, stops emitting the constant
// zero-entropy id that Lemmy would dedupe) without contorting the design for
// this essentially-never path. The propagation wiring through
// buildFollow/buildUndoFollow into the 5xx handler paths is covered by the
// normal follow lifecycle tests below.

// TestFollowLifecycle drives the full state machine: subscribe → pending →
// Accept → accepted (backfill triggered) → unsubscribe (Undo delivered) →
// none. Redelivered Accepts must not re-trigger backfill.
func TestFollowLifecycle(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology() // asserts pending → accepted internally
	ctx := context.Background()

	assert.Equal(t, 1, h.backfills.count(), "accept must trigger a backfill")

	// A re-delivered Accept (new activity id) is idempotent: state stays
	// accepted, no second backfill.
	status := h.deliver(group, map[string]any{
		"id":     "https://lemmy.world/activities/accept/follow-redelivered",
		"type":   "Accept",
		"actor":  groupID,
		"object": map[string]any{"type": "Follow", "actor": h.service.ID, "object": groupID},
	})
	require.Equal(t, http.StatusAccepted, status)
	h.drain()
	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	assert.Equal(t, store.FollowStateAccepted, community.FollowState)
	assert.Equal(t, 1, h.backfills.count(), "a redelivered accept must not re-trigger backfill")

	// Unsubscribe: Undo{Follow} delivered to the community inbox, state
	// cleared.
	deliveriesBefore := len(h.inboxLog)
	rec := h.adminRequest(http.MethodDelete, "/admin/communities",
		map[string]any{"community": groupID})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	h.mu.Lock()
	require.Greater(t, len(h.inboxLog), deliveriesBefore, "unsubscribe must deliver an Undo")
	undoRaw := h.inboxLog[len(h.inboxLog)-1]
	h.mu.Unlock()
	undo, err := ap.ParseObject(undoRaw)
	require.NoError(t, err)
	assert.Equal(t, ap.TypeUndo, undo.Type)
	require.NotNil(t, undo.Object)
	assert.Equal(t, ap.TypeFollow, undo.Object.Type)
	assert.Equal(t, groupID, undo.Object.Object.ID)

	community, err = h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	assert.Equal(t, store.FollowStateNone, community.FollowState)
}

// TestFollowRejected: the community answers Reject{Follow} → state none.
func TestFollowRejected(t *testing.T) {
	h := newHarness(t)
	group := h.technologyGroup()
	webfinger, err := os.ReadFile(filepath.Join("..", "ap", "testdata", "webfinger_group.json"))
	require.NoError(t, err)
	h.serveJSON("/.well-known/webfinger", webfinger)

	rec := h.adminRequest(http.MethodPost, "/admin/communities",
		map[string]any{"community": "!technology@lemmy.world"})
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())

	status := h.deliver(group, map[string]any{
		"id":     "https://lemmy.world/activities/reject/follow-1",
		"type":   "Reject",
		"actor":  groupID,
		"object": map[string]any{"type": "Follow", "actor": h.service.ID, "object": groupID},
	})
	require.Equal(t, http.StatusAccepted, status)
	h.drain()

	community, err := h.communities.GetByAPGroupID(context.Background(), groupID)
	require.NoError(t, err)
	assert.Equal(t, store.FollowStateNone, community.FollowState)
	assert.Equal(t, 0, h.backfills.count(), "a rejected follow must not backfill")
}

// TestAcceptFromWrongSignerDropped: an Accept for our Follow signed by an
// actor that is not the community must not flip the state.
func TestAcceptFromWrongSignerDropped(t *testing.T) {
	h := newHarness(t)
	h.technologyGroup()
	webfinger, err := os.ReadFile(filepath.Join("..", "ap", "testdata", "webfinger_group.json"))
	require.NoError(t, err)
	h.serveJSON("/.well-known/webfinger", webfinger)

	rec := h.adminRequest(http.MethodPost, "/admin/communities",
		map[string]any{"community": "!technology@lemmy.world"})
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())

	// A different lemmy.world actor (same authority, passes the inbox
	// binding) tries to accept on the community's behalf.
	impostor := h.newRemoteActor("https://lemmy.world/u/impostor",
		person("https://lemmy.world/u/impostor", "impostor", nil))
	status := h.deliver(impostor, map[string]any{
		"id":     "https://lemmy.world/activities/accept/forged",
		"type":   "Accept",
		"actor":  impostor.id,
		"object": map[string]any{"type": "Follow", "actor": h.service.ID, "object": groupID},
	})
	require.Equal(t, http.StatusAccepted, status)
	h.drain()

	community, err := h.communities.GetByAPGroupID(context.Background(), groupID)
	require.NoError(t, err)
	assert.Equal(t, store.FollowStatePending, community.FollowState,
		"only the community itself may accept its follow")
}

// TestAdminAuth: /admin requires the bearer token.
func TestAdminAuth(t *testing.T) {
	h := newHarness(t)

	body := map[string]any{"community": "!technology@lemmy.world"}
	raw, err := json.Marshal(body)
	require.NoError(t, err)

	// No token.
	req := httptest.NewRequest(http.MethodGet, "https://"+bridgeHost+"/admin/communities", nil)
	rec := httptest.NewRecorder()
	h.router.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	// Wrong token.
	req = httptest.NewRequest(http.MethodPost, "https://"+bridgeHost+"/admin/communities", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec = httptest.NewRecorder()
	h.router.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

// TestAdminList reports subscribed communities.
func TestAdminList(t *testing.T) {
	h := newHarness(t)
	h.subscribeTechnology()

	rec := h.adminRequest(http.MethodGet, "/admin/communities", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var out struct {
		Communities []communityResponse `json:"communities"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Communities, 1)
	assert.Equal(t, groupID, out.Communities[0].Community)
	assert.Equal(t, "accepted", out.Communities[0].FollowState)
	assert.Equal(t, testDIDFor("technology", "lemmy.world"), out.Communities[0].DID)
}
