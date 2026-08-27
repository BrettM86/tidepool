package ingest

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// communityProfile reads the fixture community's committed profile record.
func (h *harness) communityProfile(t *testing.T) map[string]any {
	t.Helper()
	community, err := h.communities.GetByAPGroupID(context.Background(), groupID)
	require.NoError(t, err)
	record, _, err := h.manager.GetRecord(context.Background(), community.DID,
		materialize.CollectionCommunityProfile, "self")
	require.NoError(t, err)
	return record
}

// TestRefreshProfileSingleCommunity: the endpoint re-fetches the Group
// actor regardless of the profile TTL (the subscribe a moment earlier left
// the profile fresh, so an un-forced ensure would have been a no-op) and
// commits whatever the instance says now. The origin host rides along on
// every materialization — this is the field the endpoint exists to backfill
// into dormant communities.
func TestRefreshProfileSingleCommunity(t *testing.T) {
	h := newHarness(t)
	h.subscribeTechnology()

	record := h.communityProfile(t)
	assert.Equal(t, "lemmy.world", record["origin"], "origin must be the Group's home host")
	assert.Equal(t, "technology", record["name"])

	// The instance renames the community; the stored profile is still
	// within the 24h TTL, so only a FORCED refresh sees it.
	doc := loadFixture(t, "group_lemmy_world.json")
	doc["name"] = "Technology (renamed)"
	h.serveObject("/c/technology", doc)
	before := h.hitCount("/c/technology")

	rec := h.adminRequest(http.MethodPost, "/admin/communities/refresh-profile",
		map[string]any{"community": "!technology@lemmy.world"})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Greater(t, h.hitCount("/c/technology"), before, "refresh must bypass the profile TTL and re-fetch the actor")

	record = h.communityProfile(t)
	assert.Equal(t, "Technology (renamed)", record["displayName"])
	assert.Equal(t, "lemmy.world", record["origin"])
	assert.Equal(t, "technology", record["name"], "renames change displayName, never the slug")
}

// TestRefreshProfileRejectsBadRequests: a refresh must never mint. An
// unbridged community is a 404 (not a subscribe), and a body naming
// nothing or both shapes is a 400.
func TestRefreshProfileRejectsBadRequests(t *testing.T) {
	h := newHarness(t)
	h.subscribeTechnology()

	rec := h.adminRequest(http.MethodPost, "/admin/communities/refresh-profile",
		map[string]any{"community": "https://lemmy.world/c/nothere"})
	assert.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
	_, err := h.communities.GetByAPGroupID(context.Background(), "https://lemmy.world/c/nothere")
	assert.Error(t, err, "a refresh of an unknown community must not bridge it")

	rec = h.adminRequest(http.MethodPost, "/admin/communities/refresh-profile", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())

	rec = h.adminRequest(http.MethodPost, "/admin/communities/refresh-profile",
		map[string]any{"all": true, "community": "!technology@lemmy.world"})
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
}

// TestRefreshProfileWalk: all=true walks every bridged community in the
// background and commits what the instance says now — with one community
// bridged the count is exactly one, and the walk must WRITE (the rename
// lands), not merely re-fetch. Follow state is untouched.
func TestRefreshProfileWalk(t *testing.T) {
	h := newHarness(t)
	h.subscribeTechnology()

	doc := loadFixture(t, "group_lemmy_world.json")
	doc["name"] = "Technology (walked)"
	h.serveObject("/c/technology", doc)

	rec := h.adminRequest(http.MethodPost, "/admin/communities/refresh-profile",
		map[string]any{"all": true})
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"communities":1`)
	// Wait for the walk (Wait is the same drain shutdown uses) so the next
	// test's harness does not inherit a running goroutine.
	h.admin.Wait()

	record := h.communityProfile(t)
	assert.Equal(t, "Technology (walked)", record["displayName"], "the walk must commit the refreshed profile")
	assert.Equal(t, "lemmy.world", record["origin"])

	community, err := h.communities.GetByAPGroupID(context.Background(), groupID)
	require.NoError(t, err)
	assert.Equal(t, store.FollowStateAccepted, community.FollowState, "a refresh must not touch follow state")
}

// TestRefreshProfileWalkConflict: only one walk at a time — a second
// all=true while one runs is a 409, and the gate is the Admin's own, so the
// synchronous single-community path is never blocked by it.
func TestRefreshProfileWalkConflict(t *testing.T) {
	h := newHarness(t)
	h.subscribeTechnology()

	require.True(t, h.admin.walkMu.TryLock())
	t.Cleanup(h.admin.walkMu.Unlock)

	rec := h.adminRequest(http.MethodPost, "/admin/communities/refresh-profile",
		map[string]any{"all": true})
	assert.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())

	rec = h.adminRequest(http.MethodPost, "/admin/communities/refresh-profile",
		map[string]any{"community": "!technology@lemmy.world"})
	assert.Equal(t, http.StatusOK, rec.Code, "a running walk must not block a single refresh")
}
