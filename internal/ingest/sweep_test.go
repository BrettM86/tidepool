package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sweepResponse mirrors handleSweepDeleted's JSON body.
type sweepResponse struct {
	Requested int                 `json:"requested"`
	Swept     int                 `json:"swept"`
	Deleted   int                 `json:"deleted"`
	Failed    int                 `json:"failed"`
	Truncated bool                `json:"truncated"`
	Result    []DeleteSweepResult `json:"result"`
}

func (h *harness) sweep(apIDs ...string) sweepResponse {
	h.t.Helper()
	rec := h.adminRequest(http.MethodPost, "/admin/objects/sweep-deleted",
		map[string]any{"ap_ids": apIDs})
	require.Equal(h.t, http.StatusOK, rec.Code)
	var out sweepResponse
	require.NoError(h.t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(h.t, out.Result, len(apIDs))
	// A completed pass must account for every id it was handed; only a
	// mid-batch client disconnect may report fewer.
	require.Equal(h.t, len(apIDs), out.Requested)
	require.Equal(h.t, len(apIDs), out.Swept)
	require.False(h.t, out.Truncated)
	return out
}

// assertUntouched: the mapping is still live and no create-after-delete
// marker was recorded — what every non-tombstone sweep outcome must leave.
func (h *harness) assertUntouched(apID string) {
	h.t.Helper()
	ctx := context.Background()
	mapping, err := h.objects.GetByAPID(ctx, apID)
	require.NoError(h.t, err)
	assert.False(h.t, mapping.IsDeleted(), "the mapping must still be live")
	tombstoned, err := h.tombstones.ExistsFor(ctx, apID, "")
	require.NoError(h.t, err)
	assert.False(h.t, tombstoned, "no tombstone marker may be recorded")
}

// TestSweepDeletedAppliesOriginTombstone: the missed-delete cleanup path. A
// live object is a no-op; once the origin serves a Tombstone the sweep
// applies the delete (record gone, mapping soft-deleted, marker recorded);
// re-running is idempotent.
func TestSweepDeletedAppliesOriginTombstone(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	ctx := context.Background()

	require.Equal(t, http.StatusAccepted,
		h.deliver(group, loadFixture(t, "announce_create_page_lemmy_world.json")))
	h.drain()
	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	require.False(t, mapping.IsDeleted())

	// Origin still serves the object: nothing may change.
	out := h.sweep(pageID)
	assert.Equal(t, OutcomeStillLive, out.Result[0].Outcome)
	assert.Zero(t, out.Deleted)
	mapping, err = h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	assert.False(t, mapping.IsDeleted(), "a live object must never be swept")

	// The origin now serves Lemmy's deleted-object shape.
	h.serveObject(urlPath(t, pageID), map[string]any{"id": pageID, "type": "Tombstone"})
	out = h.sweep(pageID)
	assert.Equal(t, OutcomeDeleted, out.Result[0].Outcome)
	assert.Equal(t, 1, out.Deleted)

	mapping, err = h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	assert.True(t, mapping.IsDeleted(), "the mapping must be soft-deleted")
	_, _, err = h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	assert.Error(t, err, "the record must be deleted from the repo")
	tombstoned, err := h.tombstones.ExistsFor(ctx, pageID, "")
	require.NoError(t, err)
	assert.True(t, tombstoned, "the create-after-delete marker must be recorded")

	out = h.sweep(pageID)
	assert.Equal(t, OutcomeAlreadyDeleted, out.Result[0].Outcome)
	assert.Zero(t, out.Deleted)
}

// TestSweepDeletedLeavesUnavailableObjectAlone is THE safety rule: a 404 is
// not a delete. ap.Client maps 401/403 to the same not-found error, so a
// secure-mode instance hiding an object we may not fetch is indistinguishable
// from a missing one — neither may cost the user their bridged record.
func TestSweepDeletedLeavesUnavailableObjectAlone(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()

	// Nothing is registered for this post's path, so the fixture mux 404s it.
	postID := h.bridgePost(group, "unavailable-404")

	out := h.sweep(postID)
	assert.Equal(t, OutcomeUnavailable, out.Result[0].Outcome)
	assert.Zero(t, out.Deleted)
	assert.Zero(t, out.Failed)
	h.assertUntouched(postID)
}

// TestSweepDeletedReportsFetchError: an origin that answers 5xx has told us
// nothing, so the id is reported failed and left alone — an outage must
// never read as a delete.
func TestSweepDeletedReportsFetchError(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()

	const slug = "origin-outage"
	h.mux.HandleFunc("GET /post/"+slug, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	postID := h.bridgePost(group, slug)

	out := h.sweep(postID)
	assert.Equal(t, OutcomeError, out.Result[0].Outcome)
	assert.NotEmpty(t, out.Result[0].Error, "a failed id must carry its reason")
	assert.Equal(t, 1, out.Failed)
	assert.Zero(t, out.Deleted)
	h.assertUntouched(postID)
}

// TestSweepDeletedRejectsCrossAuthorityRedirect: the sweep's fetch IS its
// authorization to delete, so a redirect off the object's own origin (an
// open-redirect bug, a compromised origin) to a host serving a tombstone
// must fail the fetch instead of deleting the record.
func TestSweepDeletedRejectsCrossAuthorityRedirect(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()

	const slug = "redirect-hijack"
	h.mux.HandleFunc("GET /post/"+slug, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "https://evil.example/post/attacker-tombstone")
		w.WriteHeader(http.StatusFound)
	})
	// The attacker host happily serves the tombstone the sweep is looking for.
	h.mux.HandleFunc("GET /post/attacker-tombstone", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusGone)
	})
	postID := h.bridgePost(group, slug)

	out := h.sweep(postID)
	assert.Equal(t, OutcomeError, out.Result[0].Outcome)
	assert.Contains(t, out.Result[0].Error, "authority",
		"the refusal must name the authority hop, not some later failure")
	assert.Zero(t, out.Deleted)
	h.assertUntouched(postID)
}

// TestSweepDeletedRefusals: ids the sweep must never touch — actor profiles
// (the Delete(Actor) scrub is deliberate, never swept) and unmapped ids —
// plus the request-shape refusals (empty list, oversized batch).
func TestSweepDeletedRefusals(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()

	require.Equal(t, http.StatusAccepted,
		h.deliver(group, loadFixture(t, "announce_create_page_lemmy_world.json")))
	h.drain()

	out := h.sweep(personID, "https://lemmy.world/post/999999")
	assert.Equal(t, OutcomeActorSkipped, out.Result[0].Outcome)
	assert.Equal(t, OutcomeUnknown, out.Result[1].Outcome)
	assert.Zero(t, out.Deleted)
	assert.Equal(t, 2, out.Requested)
	assert.Equal(t, 2, out.Swept)
	assert.False(t, out.Truncated, "a batch that ran to completion is not truncated")

	rec := h.adminRequest(http.MethodPost, "/admin/objects/sweep-deleted", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "an empty id list is a request error")

	oversized := make([]string, maxSweepBatch+1)
	for i := range oversized {
		oversized[i] = fmt.Sprintf("https://lemmy.world/post/%d", i)
	}
	rec = h.adminRequest(http.MethodPost, "/admin/objects/sweep-deleted",
		map[string]any{"ap_ids": oversized})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "the batch cap is a request error")
	assert.Contains(t, rec.Body.String(), "chunk", "the refusal must tell the operator what to do")
}
