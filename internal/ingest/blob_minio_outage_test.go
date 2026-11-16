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

// TestInboxImagePostMinIOCutParksTheEvent (B5): with the harness's real s3
// store cut off from MinIO, an announced image post parks its event (no
// attempt spent, not processed, deferred or poisoned) and bridges nothing.
// Once MinIO is reachable again and the event comes due, the post lands with
// its image, stored in the bucket and served by getBlob.
func TestInboxImagePostMinIOCutParksTheEvent(t *testing.T) {
	const (
		authorID   = "https://lemmy.world/u/minio-cut-poster"
		postID     = "https://lemmy.world/post/minio-cut-image"
		announceID = "https://lemmy.world/activities/announce/create/minio-cut-image"
	)
	ctx := context.Background()
	h, proxy := newHarnessBehindMinIOCut(t)
	group := h.subscribeTechnology()
	// One attempt: a failure the queue does not park would poison the event.
	h.rebuildDispatch(h.mat, h.client, QueueOptions{MaxAttempts: 1})
	h.serveObject("/u/minio-cut-poster", person(authorID, "minio-cut-poster", nil))
	announce := announceCreateNote(announceID, authorID,
		outageImagePage(postID, authorID, "https://lemmy.world/pictrs/image/minio-cut-image.png"))
	proxy.Cut()

	require.Equal(t, http.StatusAccepted, h.deliver(group, announce))
	h.drain()

	parked := h.storedEvent(announceID)
	assert.Nil(t, parked.ProcessedAt, "the event is parked, not processed")
	assert.Nil(t, parked.FailedAt, "a MinIO outage never poisons the event")
	assert.Equal(t, 0, parked.Attempts, "a park spends no attempt")
	assert.Equal(t, []store.DeferredItem{}, deferredRows(t, h), "a MinIO outage is not deferred")
	requireNotMaterialized(t, h, postID)

	proxy.Restore()
	h.makeEventDue(announceID)
	h.drain()

	landed := h.storedEvent(announceID)
	assert.NotNil(t, landed.ProcessedAt, "the event lands once MinIO is back")
	assert.Nil(t, landed.FailedAt)
	mapping, err := h.objects.GetByAPID(ctx, postID)
	require.NoError(t, err, "the post is bridged once MinIO is back")
	cids := recordBlobCIDs(t, h, postID)
	require.Len(t, cids, 1, "the post's record references its image")
	var bytesInObject bool
	require.NoError(t, h.db.QueryRowContext(ctx,
		`SELECT bytes IS NULL FROM blobs WHERE did = $1 AND cid = $2`, mapping.DID, cids[0]).Scan(&bytesInObject))
	assert.True(t, bytesInObject, "the image's bytes are in the bucket, not the row")
	data, mimeType, err := h.manager.GetBlob(ctx, mapping.DID, cids[0])
	require.NoError(t, err, "getBlob serves the post's image")
	assert.Equal(t, pngBytes, data, "getBlob serves the bytes the image server served")
	assert.Equal(t, "image/png", mimeType)
}

// TestBackfillImagePostMinIOCutFailsTheWalk (B6): a walk that reaches an
// image post while the harness's real s3 store is cut off from MinIO fails:
// the walk is not stamped complete, nothing is deferred, no host-gone streak
// is recorded for the item's host and the image host's existing streak is
// untouched.
func TestBackfillImagePostMinIOCutFailsTheWalk(t *testing.T) {
	const (
		authorID  = "https://lemmy.world/u/minio-cut-walker"
		postID    = "https://lemmy.world/post/minio-cut-walk"
		imageHost = "pics.example"
	)
	ctx := context.Background()
	h, proxy := newHarnessBehindMinIOCut(t)
	h.subscribeTechnology()
	h.serveObject("/u/minio-cut-walker", person(authorID, "minio-cut-walker", nil))
	h.serveObject("/c/technology/outbox", map[string]any{
		"type":       "OrderedCollection",
		"id":         groupID + "/outbox",
		"totalItems": 1,
		"orderedItems": []any{
			outageImagePage(postID, authorID, "https://"+imageHost+"/pictrs/image/minio-cut-walk.png"),
		},
	})
	seedHostGoneFailures(t, h, imageHost, 48*time.Hour)
	require.Nil(t, h.lastBackfillAt(groupID), "precondition: never walked")
	b := newBackfillOver(t, h, h.mat, 100)
	b.now = func() time.Time { return backfillNow }
	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	proxy.Cut()

	runErr := b.Run(ctx, community, true)

	assert.Error(t, runErr, "the walk reports the failed item")
	assert.Nil(t, h.lastBackfillAt(groupID), "a MinIO outage leaves the walk unfinished")
	requireNotMaterialized(t, h, postID)
	assert.Equal(t, []store.DeferredItem{}, deferredRows(t, h), "a MinIO outage is not deferred")
	assert.False(t, hostGoneSince(t, h, "lemmy.world").Valid, "a MinIO outage records no host-gone streak")
	since := hostGoneSince(t, h, imageHost)
	require.True(t, since.Valid, "the image host's streak stays")
	assert.True(t, time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC).Equal(since.Time),
		"the image host's streak still starts 48h before the walk, got %v", since.Time)
}

// TestRefreshProfileMinIOCutAnswers503 (B7): a community whose profile
// (avatar and banner) was stored through the harness's real s3 store, then
// force-refreshed with a renamed Group and a changed avatar while the store
// is cut off from MinIO, answers 503 "blob store unavailable" and keeps its
// stored profile record, repo head and avatar and banner refs.
func TestRefreshProfileMinIOCutAnswers503(t *testing.T) {
	ctx := context.Background()
	h, proxy := newHarnessBehindMinIOCut(t)
	h.subscribeTechnology()
	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	before := h.communityProfile(t)
	require.NotNil(t, before["avatar"], "precondition: the stored profile has an avatar")
	require.NotNil(t, before["banner"], "precondition: the stored profile has a banner")
	var profileBlobsInObjects int
	require.NoError(t, h.db.QueryRowContext(ctx,
		`SELECT count(*) FROM blobs WHERE did = $1 AND bytes IS NULL`, community.DID).Scan(&profileBlobsInObjects))
	require.Equal(t, 1, profileBlobsInObjects,
		"precondition: the profile image (avatar and banner share its bytes) is in the bucket")
	_, recordCIDBefore, err := h.manager.GetRecord(ctx, community.DID, materialize.CollectionCommunityProfile, "self")
	require.NoError(t, err)
	headBefore, _, err := h.manager.Head(ctx, community.DID)
	require.NoError(t, err)
	doc := loadFixture(t, "group_lemmy_world.json")
	doc["name"] = "Technology (renamed)"
	// A JPEG avatar: new bytes, so the refresh needs a new upload.
	doc["icon"] = map[string]any{"type": "Image", "url": "https://lemmy.world/pictrs/image/minio-cut-avatar.jpeg"}
	h.serveObject("/c/technology", doc)
	proxy.Cut()

	rec := h.adminRequest(http.MethodPost, "/admin/communities/refresh-profile",
		map[string]any{"community": "!technology@lemmy.world"})

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "blob store unavailable")
	_, recordCIDAfter, err := h.manager.GetRecord(ctx, community.DID, materialize.CollectionCommunityProfile, "self")
	require.NoError(t, err)
	assert.Equal(t, recordCIDBefore, recordCIDAfter, "the stored profile record is unchanged")
	headAfter, _, err := h.manager.Head(ctx, community.DID)
	require.NoError(t, err)
	assert.Equal(t, headBefore, headAfter, "nothing is committed to the community repo")
	after := h.communityProfile(t)
	assert.Equal(t, "Technology", after["displayName"], "the old name stays")
	assert.Equal(t, before["avatar"], after["avatar"], "the avatar ref stays")
	assert.Equal(t, before["banner"], after["banner"], "the banner ref stays")
}
