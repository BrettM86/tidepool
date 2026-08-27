package ingest

import (
	"context"
	"net/http"
	"testing"
	"time"

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
// unbridged community is a 404 (not a subscribe), a body naming nothing or
// both shapes is a 400, and a missing bearer is refused like every /admin.
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

	// all=true walks every state; a walk on one community must converge it
	// too, and with nothing else bridged the count is exactly one.
	before := h.hitCount("/c/technology")
	rec = h.adminRequest(http.MethodPost, "/admin/communities/refresh-profile",
		map[string]any{"all": true})
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"communities":1`)
	require.Eventually(t, func() bool { return h.hitCount("/c/technology") > before },
		5*time.Second, 10*time.Millisecond, "the walk must re-fetch the community actor")
	// The walk releases its lock only after the last refresh; wait for it so
	// the next test's harness does not inherit a running goroutine.
	require.Eventually(t, func() bool {
		if refreshProfileWalk.TryLock() {
			refreshProfileWalk.Unlock()
			return true
		}
		return false
	}, 5*time.Second, 10*time.Millisecond)

	community, err := h.communities.GetByAPGroupID(context.Background(), groupID)
	require.NoError(t, err)
	assert.Equal(t, store.FollowStateAccepted, community.FollowState, "a refresh must not touch follow state")
}
