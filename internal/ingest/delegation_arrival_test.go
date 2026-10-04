package ingest

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// The delegation bar counts a post or comment only when the followed
// community's own Announce carried it. These tests drive real inbox deliveries
// and read the result through the same contribution query the reconciler uses.

const (
	arrivalAuthorID = "https://lemmy.world/u/arrivalauthor"
	arrivalPostID   = "https://lemmy.world/post/7300001"
	// fakeMinter mints <username>.<instance with dashes>.bridge.test, so every
	// lemmy.world actor in the harness lands on this label.
	arrivalLabel = "lemmy-world"
	// arrivalPostURI is where arrivalPage materializes: the fake minter's DID
	// for arrivalauthor@lemmy.world and the rkey derived from the page's id and
	// published time.
	arrivalPostURI = "at://did:plc:56demzzr6cdn4afwayil4xsa/social.coves.community.postv2/3mq5ls44gerpn"
)

func arrivalPage() map[string]any {
	return map[string]any{
		"type":         "Page",
		"id":           arrivalPostID,
		"attributedTo": arrivalAuthorID,
		"to":           []any{groupID, ap.PublicAudience},
		"audience":     groupID,
		"name":         "a post for the delegation bar",
		"source":       map[string]any{"content": "body", "mediaType": "text/markdown"},
		"published":    "2026-07-08T17:00:00.000000Z",
	}
}

// deliverBareCreate has the author deliver Create{obj} straight to the bridge
// inbox, with no community Announce around it.
func (h *harness) deliverBareCreate(author *remoteActor, activityID string, obj map[string]any) {
	h.t.Helper()
	require.Equal(h.t, http.StatusAccepted, h.deliver(author, map[string]any{
		"id":       activityID,
		"type":     "Create",
		"actor":    author.id,
		"to":       []any{ap.PublicAudience},
		"audience": groupID,
		"object":   obj,
	}))
	h.drain()
}

// arrival reads the provenance code stored for an AP object.
func (h *harness) arrival(apID string) string {
	h.t.Helper()
	var arrival string
	require.NoError(h.t, h.db.QueryRowContext(context.Background(),
		`SELECT arrival FROM ap_objects WHERE ap_id = $1`, apID).Scan(&arrival))
	return arrival
}

// labelContributionURIs lists the AT-URIs the delegation bar would count for
// the harness's lemmy.world label right now.
func (h *harness) labelContributionURIs() []string {
	h.t.Helper()
	contributions, err := h.actors.ListLabelContributions(context.Background(), bridgeHost, arrivalLabel,
		time.Now().Add(time.Hour), store.ContributionCursor{}, 100)
	require.NoError(h.t, err)
	uris := []string{}
	for _, contribution := range contributions {
		uris = append(uris, contribution.ATURI)
	}
	return uris
}

func (h *harness) mappedATURI(apID string) string {
	h.t.Helper()
	mapping, err := h.objects.GetByAPID(context.Background(), apID)
	require.NoError(h.t, err)
	require.False(h.t, mapping.IsDeleted())
	return mapping.ATURI
}

func TestCommunityAnnouncedPostCountsTowardDelegation(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.newRemoteActor(arrivalAuthorID, person(arrivalAuthorID, "arrivalauthor", nil))

	h.announceCreate(group, "https://lemmy.world/activities/announce/arrival-announced", arrivalPage())

	postURI := h.mappedATURI(arrivalPostID)
	assert.Equal(t, "community_announced", h.arrival(arrivalPostID))
	assert.Equal(t, []string{postURI}, h.labelContributionURIs())
}

func TestBareCreateDoesNotCountTowardDelegation(t *testing.T) {
	h := newHarness(t)
	h.subscribeTechnology()
	author := h.newRemoteActor(arrivalAuthorID, person(arrivalAuthorID, "arrivalauthor", nil))

	h.deliverBareCreate(author, "https://lemmy.world/activities/create/arrival-bare", arrivalPage())

	// A bare Create is dropped, so there is no object to count.
	_, err := h.objects.GetByAPID(context.Background(), arrivalPostID)
	assert.True(t, errors.IsNotFound(err), "a bare Create must never be materialized")
	assert.Empty(t, h.labelContributionURIs())
}

func TestBareThenAnnouncedPostCountsTowardDelegation(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	author := h.newRemoteActor(arrivalAuthorID, person(arrivalAuthorID, "arrivalauthor", nil))

	h.deliverBareCreate(author, "https://lemmy.world/activities/create/arrival-first", arrivalPage())
	_, err := h.objects.GetByAPID(context.Background(), arrivalPostID)
	require.True(t, errors.IsNotFound(err), "the bare delivery is dropped: no mapping")
	require.Empty(t, h.labelContributionURIs(), "the bare copy alone must not count")

	// The community's Announce materializes the post and marks it.
	h.announceCreate(group, "https://lemmy.world/activities/announce/arrival-second", arrivalPage())
	assert.Equal(t, arrivalPostURI, h.mappedATURI(arrivalPostID))
	assert.Equal(t, "community_announced", h.arrival(arrivalPostID))
	assert.Equal(t, []string{arrivalPostURI}, h.labelContributionURIs())

	// A later bare delivery of the same object changes nothing.
	h.deliverBareCreate(author, "https://lemmy.world/activities/create/arrival-third", arrivalPage())
	assert.Equal(t, arrivalPostURI, h.mappedATURI(arrivalPostID))
	assert.Equal(t, "community_announced", h.arrival(arrivalPostID))
	assert.Equal(t, []string{arrivalPostURI}, h.labelContributionURIs())
}

func TestAnnouncedCommentDoesNotMarkFetchedAncestors(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	// The thread root (the lemmy.world Page fixture) is unmapped, so the
	// ancestor walk fetches and materializes it. Its author is on the same
	// label, so marking it would add it to the contributions below.
	h.serveLemmyWorldContent()
	h.newRemoteActor(arrivalAuthorID, person(arrivalAuthorID, "arrivalauthor", nil))
	const commentID = "https://lemmy.world/comment/7300002"
	comment := note(commentID, arrivalAuthorID, pageID, "a reply", "2026-07-08T17:05:00.000000Z")

	h.announceCreate(group, "https://lemmy.world/activities/announce/arrival-comment", comment)

	commentURI := h.mappedATURI(commentID)
	h.mappedATURI(pageID)
	assert.Equal(t, "community_announced", h.arrival(commentID))
	assert.Equal(t, "not_announced", h.arrival(pageID), "a fetched ancestor was not announced")
	assert.Equal(t, []string{commentURI}, h.labelContributionURIs())
}
