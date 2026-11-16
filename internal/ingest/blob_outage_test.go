package ingest

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/blobstore"
	"tidepool/internal/errors"
	"tidepool/internal/identity"
	"tidepool/internal/materialize"
	"tidepool/internal/repo"
	"tidepool/internal/store"
)

// errByteStoreDown is what failingBlobStore.Put returns while failing.
var errByteStoreDown = stderrors.New("test byte store is down")

// failingBlobStore is the Postgres byte store whose Put fails while failing
// is set, standing in for an unreachable object store.
type failingBlobStore struct {
	*blobstore.Postgres
	failing atomic.Bool
}

func (s *failingBlobStore) Put(ctx context.Context, tx *sql.Tx, did, cid, mimeType string, data []byte) error {
	if s.failing.Load() {
		return errByteStoreDown
	}
	return s.Postgres.Put(ctx, tx, did, cid, mimeType, data)
}

// newOutageMaterializer builds a materializer over a second repo.Manager on
// the harness database whose byte store the test can fail, and points
// h.materializerOptions.Repos at that manager so rigs built afterwards from
// the harness options write through it too.
func newOutageMaterializer(t *testing.T, h *harness) (*materialize.Materializer, *failingBlobStore) {
	t.Helper()
	byteStore := &failingBlobStore{Postgres: blobstore.NewPostgres(h.db)}
	manager, err := repo.NewManager(h.db, identity.NewActorKeys(h.actors, h.custodian), nil, repo.WithBlobStore(byteStore))
	require.NoError(t, err)
	h.materializerOptions.Repos = manager
	materializer, err := materialize.New(h.materializerOptions)
	require.NoError(t, err)
	return materializer, byteStore
}

// newOutageAdmin is the harness Admin over materializer.
func newOutageAdmin(t *testing.T, h *harness, materializer *materialize.Materializer) *Admin {
	t.Helper()
	options := h.adminOptions()
	options.Materializer = materializer
	admin, err := NewAdmin(options)
	require.NoError(t, err)
	return admin
}

// outageImagePage is a Page by author in the technology community with one
// image attachment at imageURL.
func outageImagePage(id, author, imageURL string) map[string]any {
	return map[string]any{
		"id":           id,
		"type":         "Page",
		"attributedTo": author,
		"audience":     groupID,
		"to":           []any{groupID, ap.PublicAudience},
		"name":         "an image post",
		"content":      "<p>an image post</p>",
		"published":    "2026-11-14T08:00:00.000000Z",
		"attachment": []any{map[string]any{
			"type":      "Image",
			"href":      imageURL,
			"mediaType": "image/png",
		}},
	}
}

// TestInboxImagePostByteStoreOutageParksTheEvent (D4): an announced image
// post whose byte store is down parks its event as PLC throttling does (no
// attempt spent, not processed, deferred or poisoned) and bridges nothing for
// the post. Once the store is back and the event comes due, the post lands
// with its image. The community is subscribed and the author has no avatar,
// so the post's image is the only byte store write.
func TestInboxImagePostByteStoreOutageParksTheEvent(t *testing.T) {
	const (
		authorID   = "https://lemmy.world/u/outage-poster"
		postID     = "https://lemmy.world/post/outage-image"
		announceID = "https://lemmy.world/activities/announce/create/outage-image"
	)
	ctx := context.Background()
	h := newHarness(t)
	group := h.subscribeTechnology()
	materializer, byteStore := newOutageMaterializer(t, h)
	// One attempt: a failure the queue does not park would poison the event.
	h.rebuildDispatch(materializer, h.client, QueueOptions{MaxAttempts: 1})
	h.serveObject("/u/outage-poster", person(authorID, "outage-poster", nil))
	announce := announceCreateNote(announceID, authorID,
		outageImagePage(postID, authorID, "https://lemmy.world/pictrs/image/outage-image.png"))
	byteStore.failing.Store(true)

	require.Equal(t, http.StatusAccepted, h.deliver(group, announce))
	h.drain()

	parked := h.storedEvent(announceID)
	assert.Nil(t, parked.ProcessedAt, "the event is parked, not processed")
	assert.Nil(t, parked.FailedAt, "a byte store outage never poisons the event")
	assert.Equal(t, 0, parked.Attempts, "a park spends no attempt")
	assert.Equal(t, []store.DeferredItem{}, deferredRows(t, h), "a byte store outage is not deferred")
	requireNotMaterialized(t, h, postID)

	byteStore.failing.Store(false)
	h.makeEventDue(announceID)
	h.drain()

	landed := h.storedEvent(announceID)
	assert.NotNil(t, landed.ProcessedAt, "the event lands once the store is back")
	assert.Nil(t, landed.FailedAt)
	mapping, err := h.objects.GetByAPID(ctx, postID)
	require.NoError(t, err, "the post is bridged once the store is back")
	cids := recordBlobCIDs(t, h, postID)
	require.Len(t, cids, 1, "the post's record references its image")
	assertBlobServed(t, h, mapping.DID, cids[0], "the post's image")
}

// TestBackfillImagePostByteStoreOutageFailsTheWalk (D4): a walk that reaches
// an image post (same-authority, trusted as embedded) while the byte store is
// down fails the item: the walk is not stamped complete, nothing is deferred,
// no host-gone streak is recorded for the item's host and the image host's
// existing streak is untouched (a media fetch never clears one).
func TestBackfillImagePostByteStoreOutageFailsTheWalk(t *testing.T) {
	const (
		authorID  = "https://lemmy.world/u/outage-walker"
		postID    = "https://lemmy.world/post/outage-walk"
		imageHost = "pics.example"
	)
	ctx := context.Background()
	h := newHarness(t)
	h.subscribeTechnology()
	materializer, byteStore := newOutageMaterializer(t, h)
	h.serveObject("/u/outage-walker", person(authorID, "outage-walker", nil))
	h.serveObject("/c/technology/outbox", map[string]any{
		"type":       "OrderedCollection",
		"id":         groupID + "/outbox",
		"totalItems": 1,
		"orderedItems": []any{
			outageImagePage(postID, authorID, "https://"+imageHost+"/pictrs/image/outage-walk.png"),
		},
	})
	seedHostGoneFailures(t, h, imageHost, 48*time.Hour)
	require.Nil(t, h.lastBackfillAt(groupID), "precondition: never walked")
	b := newBackfillOver(t, h, materializer, 100)
	b.now = func() time.Time { return backfillNow }
	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	byteStore.failing.Store(true)

	runErr := b.Run(ctx, community, true)

	assert.Nil(t, h.lastBackfillAt(groupID), "a byte store outage leaves the walk unfinished")
	assert.Error(t, runErr, "the walk reports the failed item")
	requireNotMaterialized(t, h, postID)
	assert.Equal(t, []store.DeferredItem{}, deferredRows(t, h), "a byte store outage is not deferred")
	assert.False(t, hostGoneSince(t, h, "lemmy.world").Valid, "a byte store outage records no host-gone streak")
	since := hostGoneSince(t, h, imageHost)
	require.True(t, since.Valid, "the image host's streak stays")
	assert.True(t, time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC).Equal(since.Time),
		"the image host's streak still starts 48h before the walk, got %v", since.Time)
}

// TestSweepByteStoreOutageStopsTheAdds (D4): an add whose Group avatar hits a
// byte store outage stops the sweep as a throttled add does. (a) Explicit
// follow-list entries: the entry after it is not fetched. (b) Listed
// candidates: the candidate after it is not fetched, and the failed one gets
// no failed-add skip, so the next sweep at the same instant adds both.
func TestSweepByteStoreOutageStopsTheAdds(t *testing.T) {
	avatar := func(id string) map[string]any {
		return map[string]any{"icon": map[string]any{
			"type": "Image",
			"url":  originOf(id) + "/pictrs/image/" + id[len(originOf(id))+len("/c/"):] + ".png",
		}}
	}

	t.Run("explicit follow-list entries", func(t *testing.T) {
		const secondPeerID = "https://peer.example/c/explicit-two"
		h := newHarness(t)
		materializer, byteStore := newOutageMaterializer(t, h)
		rig := newInstanceRig(t, h, `communities:
  - "https://peer.example/c/explicit-one"
  - "https://peer.example/c/explicit-two"
`, instanceRigOptions{admin: newOutageAdmin(t, h, materializer)})
		rig.serveGroup(peerExplicitID, avatar(peerExplicitID))
		rig.serveGroup(secondPeerID, avatar(secondPeerID))
		byteStore.failing.Store(true)

		result := rig.sweep()

		assert.Equal(t, []int{0}, rig.fetchCounts(secondPeerID), "nothing is tried after the outage")
		assert.Equal(t, []string{peerExplicitID, secondPeerID}, result.Failed)
		assert.Empty(t, result.Subscribed)
	})

	t.Run("listed candidates", func(t *testing.T) {
		const (
			firstID  = "https://lemmy.world/c/outage-first"
			secondID = "https://lemmy.world/c/outage-second"
		)
		h := newHarness(t)
		materializer, byteStore := newOutageMaterializer(t, h)
		rig := newInstanceRig(t, h, instanceFollowList(lemmyFixtureHost),
			instanceRigOptions{admin: newOutageAdmin(t, h, materializer)})
		for _, id := range []string{firstID, secondID} {
			rig.fixture.add(lemmyFixtureHost, lemmyCommunity{name: id[len("https://lemmy.world/c/"):]})
			rig.serveGroup(id, avatar(id))
		}
		byteStore.failing.Store(true)

		result := rig.sweep()

		assert.Equal(t, []int{0}, rig.fetchCounts(secondID), "nothing is tried after the outage")
		assert.Empty(t, result.Subscribed)

		byteStore.failing.Store(false)
		result = rig.sweep()

		assert.Equal(t, []string{firstID, secondID}, result.Subscribed,
			"the outage left no failed-add skip, so both are added at the same instant")
	})
}

// TestRefreshProfileByteStoreOutageAnswers503 (D4): a forced profile refresh
// whose avatar or banner put hits a byte store outage answers 503 "blob store
// unavailable" and leaves the stored profile record and the repo head as they
// were. The Group is renamed so a profile committed anyway would show.
func TestRefreshProfileByteStoreOutageAnswers503(t *testing.T) {
	tests := []struct {
		name string
		// edit changes the served Group so the refresh reaches the row's put.
		edit func(doc map[string]any)
	}{
		{
			name: "avatar",
			edit: func(doc map[string]any) {},
		},
		{
			name: "banner only",
			edit: func(doc map[string]any) { delete(doc, "icon") },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			h := newHarness(t)
			h.subscribeTechnology()
			community, err := h.communities.GetByAPGroupID(ctx, groupID)
			require.NoError(t, err)
			before := h.communityProfile(t)
			require.NotNil(t, before["avatar"], "precondition: the stored profile has an avatar")
			require.NotNil(t, before["banner"], "precondition: the stored profile has a banner")
			_, recordCIDBefore, err := h.manager.GetRecord(ctx, community.DID, materialize.CollectionCommunityProfile, "self")
			require.NoError(t, err)
			headBefore, _, err := h.manager.Head(ctx, community.DID)
			require.NoError(t, err)

			materializer, byteStore := newOutageMaterializer(t, h)
			router := chi.NewRouter()
			newOutageAdmin(t, h, materializer).Routes(router)
			h.router = router
			doc := loadFixture(t, "group_lemmy_world.json")
			doc["name"] = "Technology (renamed)"
			tt.edit(doc)
			h.serveObject("/c/technology", doc)
			byteStore.failing.Store(true)

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
		})
	}
}

// TestBackfillReplyAuthorAvatarByteStoreOutageFailsTheWalk (D4): a walk whose
// media-free post commits but whose reply's new author advertises an avatar
// fails the reply while the byte store is down: the walk is not stamped, the
// reply is not bridged and nothing is deferred. Once the store is back, the
// next walk bridges the reply and stamps the walk.
func TestBackfillReplyAuthorAvatarByteStoreOutageFailsTheWalk(t *testing.T) {
	const (
		postAuthorID  = "https://lemmy.world/u/outage-plain-poster"
		replyAuthorID = "https://lemmy.world/u/outage-avatar-replier"
		postID        = "https://lemmy.world/post/outage-plain"
		replyID       = "https://lemmy.world/comment/outage-reply"
	)
	ctx := context.Background()
	h := newHarness(t)
	h.subscribeTechnology()
	materializer, byteStore := newOutageMaterializer(t, h)
	h.serveObject("/u/outage-plain-poster", person(postAuthorID, "outage-plain-poster", nil))
	h.serveObject("/u/outage-avatar-replier", person(replyAuthorID, "outage-avatar-replier", map[string]any{
		"icon": map[string]any{
			"type": "Image",
			"url":  "https://lemmy.world/pictrs/image/outage-avatar-replier.png",
		},
	}))
	post := outageImagePage(postID, postAuthorID, "")
	delete(post, "attachment")
	post["replies"] = postID + "/replies"
	h.serveObject("/c/technology/outbox", map[string]any{
		"type":         "OrderedCollection",
		"id":           groupID + "/outbox",
		"totalItems":   1,
		"orderedItems": []any{post},
	})
	h.serveObject("/post/outage-plain/replies", map[string]any{
		"type":       "OrderedCollection",
		"id":         postID + "/replies",
		"totalItems": 1,
		"orderedItems": []any{
			note(replyID, replyAuthorID, postID, "a reply by an author with an avatar", "2026-11-14T09:00:00.000000Z"),
		},
	})
	require.Nil(t, h.lastBackfillAt(groupID), "precondition: never walked")
	b := newBackfillOver(t, h, materializer, 100)
	b.now = func() time.Time { return backfillNow }
	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	byteStore.failing.Store(true)

	runErr := b.Run(ctx, community, true)

	assert.Error(t, runErr, "the walk reports the failed reply")
	assert.Nil(t, h.lastBackfillAt(groupID), "a byte store outage leaves the walk unfinished")
	_, err = h.objects.GetByAPID(ctx, postID)
	require.NoError(t, err, "the media-free post commits")
	requireNotMaterialized(t, h, replyID)
	assert.Equal(t, []store.DeferredItem{}, deferredRows(t, h), "a byte store outage is not deferred")

	byteStore.failing.Store(false)
	community, err = h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)

	require.NoError(t, b.Run(ctx, community, true))

	mapping, err := h.objects.GetByAPID(ctx, replyID)
	require.NoError(t, err, "the reply is bridged once the store is back")
	assert.Equal(t, materialize.CollectionComment, mapping.Collection)
	assert.NotNil(t, h.lastBackfillAt(groupID), "the walk is stamped once the store is back")
}

// TestRetryDeferredByteStoreOutageKeepsAnExpiredItem (D4): a deferred image
// post past its 90-day retry whose retry hits a byte store outage is kept
// with its attempts and schedule unchanged (still due), not dropped as
// retries exhausted. Once the store is back, the next sweep lands it and its
// row goes.
func TestRetryDeferredByteStoreOutageKeepsAnExpiredItem(t *testing.T) {
	const (
		authorID = "https://lemmy.world/u/outage-deferred"
		postID   = "https://lemmy.world/post/outage-deferred"
	)
	ctx := context.Background()
	h := newHarness(t)
	h.subscribeTechnology()
	materializer, byteStore := newOutageMaterializer(t, h)
	h.serveObject("/u/outage-deferred", person(authorID, "outage-deferred", nil))
	h.serveObject("/post/outage-deferred",
		outageImagePage(postID, authorID, "https://lemmy.world/pictrs/image/outage-deferred.png"))
	expired := store.DeferredItem{
		ObjectIRI:     postID,
		CommunityAPID: groupID,
		CommunityDID:  h.communityDID(groupID),
		Host:          "lemmy.world",
		ObjectType:    "Page",
		Attempts:      4,
		CreatedAt:     time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC),
		NextRetryAt:   time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC),
	}
	require.NoError(t, store.NewDeferredBackfillItems(h.db).DeferItem(ctx, expired))
	b := newBackfillOver(t, h, materializer, 100)
	b.now = func() time.Time { return backfillNow }
	byteStore.failing.Store(true)

	require.NoError(t, b.RetryDeferred(ctx, 50))

	assert.Equal(t, []store.DeferredItem{expired}, deferredRows(t, h),
		"a byte store outage keeps the expired row with its attempts and schedule")
	requireNotMaterialized(t, h, postID)

	byteStore.failing.Store(false)

	require.NoError(t, b.RetryDeferred(ctx, 50))

	assert.Equal(t, []store.DeferredItem{}, deferredRows(t, h), "the landed row goes")
	mapping, err := h.objects.GetByAPID(ctx, postID)
	require.NoError(t, err, "the post is bridged once the store is back")
	cids := recordBlobCIDs(t, h, postID)
	require.Len(t, cids, 1, "the post's record references its image")
	assertBlobServed(t, h, mapping.DID, cids[0], "the post's image")
}

// outageCommentMaterializer fails MaterializeComment on a byte store outage
// after committing ancestor.
type outageCommentMaterializer struct {
	Materializer
	ancestor string
}

func (m outageCommentMaterializer) MaterializeComment(context.Context, *ap.Object, string) (*materialize.Result, error) {
	return &materialize.Result{Ancestors: []string{m.ancestor}},
		fmt.Errorf("materialize: store comment author avatar: %w", errors.ErrBlobStoreUnavailable)
}

// failingTombstoneChecks is the harness tombstones whose ExistsFor fails.
type failingTombstoneChecks struct {
	store.Tombstones
}

func (failingTombstoneChecks) ExistsFor(context.Context, string, string) (bool, error) {
	return false, stderrors.New("tombstone read failed")
}

// TestMaterializeCommentKeepsByteStoreOutageWhenAncestorRecheckFails (D4): a
// comment that fails on a byte store outage while its ancestor's tombstone
// recheck also fails still reports the outage (errors.IsBlobStoreUnavailable),
// so the reply pass fails the walk instead of dropping the reply, and the
// recheck failure is reported too.
func TestMaterializeCommentKeepsByteStoreOutageWhenAncestorRecheckFails(t *testing.T) {
	const (
		ancestorID = "https://lemmy.world/post/outage-ancestor"
		commentID  = "https://lemmy.world/comment/outage-comment"
	)
	h := newHarness(t)
	options := h.backfillOptions(outageCommentMaterializer{ancestor: ancestorID}, 100)
	options.Tombstones = failingTombstoneChecks{Tombstones: h.tombstones}
	b, err := NewBackfill(options)
	require.NoError(t, err)

	removed, err := b.materializeComment(context.Background(), &ap.Object{ID: commentID, Type: ap.TypeNote}, groupID)

	assert.False(t, removed)
	require.Error(t, err)
	assert.True(t, errors.IsBlobStoreUnavailable(err), "the byte store outage survives the recheck failure: %v", err)
	assert.Contains(t, err.Error(), "tombstone read failed", "the recheck failure is reported too")
}
