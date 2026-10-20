package ingest

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// UPSTREAM BANS REMOVE BRIDGED CONTENT.
//
// Two Lemmy bans carry `removeData: true` and must take a BRIDGED Lemmy user's
// content off Coves, because Lemmy already removed it upstream:
//
//   - a SITE ban, delivered DIRECTLY (never announced) by the user's home
//     instance admin, targeting the instance root: the user's content
//     published before the ban goes from every community (records deleted);
//   - a COMMUNITY ban announced by the community about a bridged user: their
//     earlier postv2 in that community gets an author-banned removal, their
//     earlier comments there are deleted.
//
// Only content published BEFORE the ban time goes (Block.published, else the
// inbox event's received_at). Lemmy restores nothing on unban, so Undo{Block}
// restores nothing; and there is no gate on the user's future content.
const (
	// H: the banned user's home instance and its admin.
	ubHomeHost   = "https://programming.dev"
	ubUserID     = ubHomeHost + "/u/SmileMask"
	ubAdminID    = ubHomeHost + "/u/Spyro"
	ubHomeRoot   = ubHomeHost + "/"
	ubUnknownID  = ubHomeHost + "/u/NeverBridged"
	ubHomeCommID = ubHomeHost + "/c/programming"

	// J: another instance, with its own admin and its own bridged user.
	ubOtherHost         = "https://lemmy.ml"
	ubOtherAdminID      = ubOtherHost + "/u/MlAdmin"
	ubOtherRoot         = ubOtherHost + "/"
	ubOtherUserID       = ubOtherHost + "/u/OtherPoster"
	ubCommunityDGroupID = "https://lemmy.world/c/linux"
	ubCommunityDName    = "linux"

	// lemmy.world, C and D's own instance, and its admin.
	ubWorldRoot    = "https://lemmy.world/"
	ubWorldAdminID = "https://lemmy.world/u/WorldAdmin"
)

// The fixed timeline. The ban is received (or published) at ubBanTime; content
// before it goes, content at or after it stays.
var (
	ubBanTime       = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	ubBeforeBan     = "2026-08-10T12:00:00Z"
	ubBeforeBan2    = "2026-08-11T12:00:00Z"
	ubBeforeBan3    = "2026-08-12T12:00:00Z"
	ubBeforeBan4    = "2026-08-13T12:00:00Z"
	ubAfterBan      = "2026-09-15T00:00:00Z"
	ubWellAfterBan  = "2026-10-01T00:00:00Z"
	ubLapsedExpiry  = "2026-09-08T00:00:00Z"
	ubUserDID       = testDIDFor("SmileMask", "programming.dev")
	ubCommunityCDID = testDIDFor("technology", "lemmy.world")
	ubCommunityDDID = testDIDFor(ubCommunityDName, "lemmy.world")
)

// upstreamBanWorld is two followed communities on lemmy.world (C = technology,
// D = linux), a bridged user U on programming.dev, U's home admin, and a second
// instance's admin.
type upstreamBanWorld struct {
	h          *harness
	communityC *remoteActor
	communityD *remoteActor
	admin      *remoteActor
	otherAdmin *remoteActor
}

func newUpstreamBanWorld(t *testing.T) *upstreamBanWorld {
	t.Helper()
	h := newHarness(t)
	world := &upstreamBanWorld{h: h}
	world.communityC = h.subscribeTechnology()
	world.communityD = h.subscribeCommunityURL(ubCommunityDGroupID, ubCommunityDName)
	h.newRemoteActor(ubUserID, person(ubUserID, "SmileMask", nil))
	h.newRemoteActor(ubOtherUserID, person(ubOtherUserID, "OtherPoster", nil))
	world.admin = h.newRemoteActor(ubAdminID, person(ubAdminID, "Spyro", nil))
	world.otherAdmin = h.newRemoteActor(ubOtherAdminID, person(ubOtherAdminID, "MlAdmin", nil))
	return world
}

// postBy bridges one Page by author into community, announced by the community
// as a bare id (the cross-authority shape: the bridge fetches it from the
// author's origin). withImage gives the post its own image with bytes no other
// post shares, so its blob CID is unique to it. Slugs must be unique across the
// world: the fixture server routes by path alone.
func (w *upstreamBanWorld) postBy(author string, community *remoteActor, slug, published string, withImage bool) string {
	w.h.t.Helper()
	imageSeed := ""
	if withImage {
		imageSeed = slug
	}
	return w.postWithImageSeed(author, community, slug, published, imageSeed)
}

// postWithImageSeed is postBy with control over the image bytes: posts given
// the same imageSeed carry byte-identical images (one shared blob CID in the
// author's repo, as a spammer reusing an image produces); "" means no image.
func (w *upstreamBanWorld) postWithImageSeed(author string, community *remoteActor, slug, published, imageSeed string) string {
	w.h.t.Helper()
	postID := originOf(author) + "/post/" + slug
	page := map[string]any{
		"type":         "Page",
		"id":           postID,
		"attributedTo": author,
		"to":           []any{community.id, ap.PublicAudience},
		"cc":           []any{},
		"name":         "upstream ban fixture post " + slug,
		"mediaType":    "text/html",
		"audience":     community.id,
		"sensitive":    false,
		"published":    published,
	}
	if imageSeed != "" {
		imagePath := "/media/" + slug + ".png"
		imageBytes := append(append([]byte{}, pngBytes...), []byte(imageSeed)...)
		w.h.mux.HandleFunc("GET "+imagePath, func(rw http.ResponseWriter, _ *http.Request) {
			rw.Header().Set("Content-Type", "image/png")
			_, _ = rw.Write(imageBytes)
		})
		page["image"] = map[string]any{"type": "Image", "url": originOf(author) + imagePath}
	}
	w.h.serveObject("/post/"+slug, page)
	w.announceBareID(community, "create/"+slug, postID)
	return postID
}

// commentBy bridges one Note by author replying to parentID in community.
func (w *upstreamBanWorld) commentBy(author string, community *remoteActor, slug, parentID, published string) string {
	w.h.t.Helper()
	commentID := originOf(author) + "/comment/" + slug
	w.h.serveObject("/comment/"+slug, map[string]any{
		"type":         "Note",
		"id":           commentID,
		"attributedTo": author,
		"to":           []any{ap.PublicAudience},
		"cc":           []any{community.id},
		"audience":     community.id,
		"content":      "<p>upstream ban fixture comment " + slug + "</p>",
		"source":       map[string]any{"content": "upstream ban fixture comment " + slug, "mediaType": "text/markdown"},
		"published":    published,
		"inReplyTo":    parentID,
	})
	w.announceBareID(community, "create/"+slug, commentID)
	return commentID
}

func (w *upstreamBanWorld) announceBareID(community *remoteActor, suffix, objectID string) {
	w.h.t.Helper()
	require.Equal(w.h.t, http.StatusAccepted, w.h.deliver(community, map[string]any{
		"id":       community.id + "/activities/announce/" + suffix,
		"type":     "Announce",
		"actor":    community.id,
		"audience": community.id,
		"object":   map[string]any{"id": objectID},
	}))
	w.h.drain()
	mapping, err := w.h.objects.GetByAPID(context.Background(), objectID)
	require.NoError(w.h.t, err, "precondition: %s materializes", objectID)
	require.False(w.h.t, mapping.IsDeleted(), "precondition: %s is live", objectID)
	_, _, err = w.h.manager.GetRecord(context.Background(), mapping.DID, mapping.Collection, mapping.RKey)
	require.NoError(w.h.t, err, "precondition: %s has its record", objectID)
}

// communityRemoves is the community's own moderator removal of a post (an
// announced Delete carrying `summary`): acceptance out, removal record in, the
// author's record untouched.
func (w *upstreamBanWorld) communityRemoves(community *remoteActor, slug, postID string) {
	w.h.t.Helper()
	require.Equal(w.h.t, http.StatusAccepted, w.h.deliver(community, map[string]any{
		"id":       community.id + "/activities/announce/delete/" + slug,
		"type":     "Announce",
		"actor":    community.id,
		"audience": community.id,
		"object": map[string]any{
			"id":      "https://lemmy.world/activities/delete/" + slug,
			"type":    "Delete",
			"actor":   modActorID,
			"object":  postID,
			"summary": "Rule 1",
		},
	}))
	w.h.drain()
}

// siteBlock is Lemmy 0.19's home-instance site ban: a Block signed by the
// admin, targeting the instance root, with no `published`. A nil value in
// extra removes that key.
func siteBlock(activityID, actor, subject, target string, extra map[string]any) map[string]any {
	block := map[string]any{
		"type":       "Block",
		"id":         activityID,
		"actor":      actor,
		"object":     subject,
		"target":     target,
		"removeData": true,
		"summary":    "Spam",
	}
	for key, value := range extra {
		if value == nil {
			delete(block, key)
			continue
		}
		block[key] = value
	}
	return block
}

// deliverAt delivers activity to the shared inbox signed by signer, stamps the
// queued event's received_at (the store does not persist a supplied
// ReceivedAt), and drains the real queue.
func (w *upstreamBanWorld) deliverAt(signer *remoteActor, activity map[string]any, receivedAt *time.Time) {
	w.h.t.Helper()
	require.Equal(w.h.t, http.StatusAccepted, w.h.deliver(signer, activity))
	if receivedAt != nil {
		backdateReceived(w.h, activity["id"].(string), *receivedAt)
	}
	w.h.drain()
}

func backdateReceived(h *harness, activityID string, receivedAt time.Time) {
	h.t.Helper()
	result, err := h.db.ExecContext(context.Background(),
		`UPDATE inbox_events SET received_at = $1 WHERE activity_id = $2`, receivedAt, activityID)
	require.NoError(h.t, err)
	rows, err := result.RowsAffected()
	require.NoError(h.t, err)
	require.Equal(h.t, int64(1), rows, "precondition: %s is queued", activityID)
}

// announceCommunityBlock is the community's Announce{Block} naming subject,
// targeting the community itself, received at receivedAt.
func (w *upstreamBanWorld) announceCommunityBlock(community *remoteActor, activityID, subject string, extra map[string]any, receivedAt time.Time) {
	w.h.t.Helper()
	w.deliverAt(community, communityBlockAnnounce(community, activityID, subject, extra), &receivedAt)
}

// communityBlockAnnounce builds the community's Announce{Block} of subject
// (removeData true, target the community itself). A nil value in extra removes
// that key from the inner Block.
func communityBlockAnnounce(community *remoteActor, activityID, subject string, extra map[string]any) map[string]any {
	block := map[string]any{
		"id":         activityID + "/block",
		"type":       "Block",
		"actor":      modActorID,
		"object":     subject,
		"target":     community.id,
		"audience":   community.id,
		"to":         []any{ap.PublicAudience},
		"cc":         []any{community.id},
		"removeData": true,
		"summary":    "Spam",
	}
	for key, value := range extra {
		if value == nil {
			delete(block, key)
			continue
		}
		block[key] = value
	}
	return map[string]any{
		"id":       activityID,
		"type":     "Announce",
		"actor":    community.id,
		"audience": community.id,
		"cc":       []any{community.id + "/followers"},
		"object":   block,
	}
}

func (w *upstreamBanWorld) announceCommunityUndoBlock(community *remoteActor, activityID, blockActivityID, subject string) {
	w.h.t.Helper()
	w.deliverAt(community, map[string]any{
		"id":       activityID,
		"type":     "Announce",
		"actor":    community.id,
		"audience": community.id,
		"cc":       []any{community.id + "/followers"},
		"object": map[string]any{
			"id":       activityID + "/undo",
			"type":     "Undo",
			"actor":    modActorID,
			"audience": community.id,
			"cc":       []any{community.id},
			"object": map[string]any{
				"id":         blockActivityID + "/block",
				"type":       "Block",
				"actor":      modActorID,
				"object":     subject,
				"target":     community.id,
				"audience":   community.id,
				"removeData": true,
			},
		},
	}, nil)
}

// assertDeleted: mapping soft-deleted AND the record gone from its repo.
func assertDeleted(t *testing.T, h *harness, apID, why string) {
	t.Helper()
	ctx := context.Background()
	mapping, err := h.objects.GetByAPID(ctx, apID)
	require.NoError(t, err)
	assert.True(t, mapping.IsDeleted(), "%s: mapping deleted_at must be set (%s)", apID, why)
	_, _, err = h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	assert.True(t, errors.IsNotFound(err), "%s: record must be gone from %s (%s); err=%v",
		apID, mapping.DID, why, err)
}

// assertLive: mapping live AND the record present in its repo.
func assertLive(t *testing.T, h *harness, apID, why string) {
	t.Helper()
	ctx := context.Background()
	mapping, err := h.objects.GetByAPID(ctx, apID)
	require.NoError(t, err)
	assert.False(t, mapping.IsDeleted(), "%s: mapping must stay live (%s)", apID, why)
	_, _, err = h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	assert.NoError(t, err, "%s: record must stay in %s (%s)", apID, mapping.DID, why)
}

// postBlobCID reads the post's one image blob CID while its record exists.
func postBlobCID(t *testing.T, h *harness, postID string) string {
	t.Helper()
	ctx := context.Background()
	mapping, err := h.objects.GetByAPID(ctx, postID)
	require.NoError(t, err)
	record, _, err := h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	require.NoError(t, err)
	blobs := atdata.ExtractBlobs(record)
	require.Len(t, blobs, 1, "precondition: %s carries exactly its own image blob", postID)
	blobCID := cid.Cid(blobs[0].Ref).String()
	_, _, err = h.manager.GetBlob(ctx, mapping.DID, blobCID)
	require.NoError(t, err, "precondition: %s's blob is retrievable before the ban", postID)
	return blobCID
}

func acceptanceExists(t *testing.T, h *harness, communityDID, postID string) bool {
	t.Helper()
	ctx := context.Background()
	mapping, err := h.objects.GetByAPID(ctx, postID)
	require.NoError(t, err)
	_, _, err = h.manager.GetRecord(ctx, communityDID, materialize.CollectionAcceptance, testDigestRKey(mapping.ATURI))
	if errors.IsNotFound(err) {
		return false
	}
	require.NoError(t, err)
	return true
}

// removalCode returns the community's removal record code for postID, or "".
func removalCode(t *testing.T, h *harness, communityDID, postID string) string {
	t.Helper()
	ctx := context.Background()
	mapping, err := h.objects.GetByAPID(ctx, postID)
	require.NoError(t, err)
	removal, _, err := h.manager.GetRecord(ctx, communityDID, materialize.CollectionRemoval, testDigestRKey(mapping.ATURI))
	if errors.IsNotFound(err) {
		return ""
	}
	require.NoError(t, err)
	code, _ := removal["code"].(string)
	return code
}

func assertProcessed(t *testing.T, h *harness, activityID string) {
	t.Helper()
	event, err := h.events.GetEvent(context.Background(), activityID)
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt, "%s: processed once, not left retrying: %s", activityID, event.Error)
	assert.Nil(t, event.FailedAt, "%s: not poisoned: %s", activityID, event.Error)
}

// TestASiteBanWithRemoveDataDeletesTheAuthorsEarlierContentEverywhere (B1).
//
// GIVEN bridged user U with, before the ban, a postv2 and a comment in C, a
// postv2 in D, and a postv2 in C that C's moderators had already removed (each
// post with its own image), plus a post published after the ban; WHEN U's home
// admin delivers a direct site Block with removeData — permanent, or already
// expired when it arrived (Lemmy removed the data at ban time either way) —
// THEN every pre-ban post and comment in both communities is deleted with its
// blobs, the post-ban post and U's profile are untouched, and the event is
// processed. U's later content still materializes, and Undo{Block} restores
// nothing.
func TestASiteBanWithRemoveDataDeletesTheAuthorsEarlierContentEverywhere(t *testing.T) {
	for _, tc := range []struct {
		name  string
		extra map[string]any
	}{
		{name: "permanent ban"},
		{name: "ban already expired when it arrived", extra: map[string]any{"expires": ubLapsedExpiry}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			world := newUpstreamBanWorld(t)
			h := world.h
			ctx := context.Background()

			postInC := world.postBy(ubUserID, world.communityC, "ub-site-c1", ubBeforeBan, true)
			commentInC := world.commentBy(ubUserID, world.communityC, "ub-site-c1-reply", postInC, ubBeforeBan2)
			postInD := world.postBy(ubUserID, world.communityD, "ub-site-d1", ubBeforeBan3, true)
			removedPostInC := world.postBy(ubUserID, world.communityC, "ub-site-c2", ubBeforeBan4, true)
			world.communityRemoves(world.communityC, "ub-site-c2", removedPostInC)
			require.False(t, acceptanceExists(t, h, ubCommunityCDID, removedPostInC),
				"precondition: C's moderators already removed this post")
			postAfterBan := world.postBy(ubUserID, world.communityC, "ub-site-c3", ubAfterBan, true)

			blobC := postBlobCID(t, h, postInC)
			blobD := postBlobCID(t, h, postInD)
			blobRemoved := postBlobCID(t, h, removedPostInC)
			blobAfter := postBlobCID(t, h, postAfterBan)
			require.Len(t, map[string]bool{blobC: true, blobD: true, blobRemoved: true, blobAfter: true}, 4,
				"precondition: every post's image is its own blob")

			const banID = ubHomeHost + "/activities/block/ub-site-ban"
			block := siteBlock(banID, ubAdminID, ubUserID, ubHomeRoot, tc.extra)
			banTime := ubBanTime
			world.deliverAt(world.admin, block, &banTime)

			assertDeleted(t, h, postInC, "pre-ban post in C")
			assertDeleted(t, h, commentInC, "pre-ban comment in C")
			assertDeleted(t, h, postInD, "pre-ban post in D: a site ban spans every community")
			assertDeleted(t, h, removedPostInC,
				"a site ban deletes the record even when a community had already removed it")
			for postID, blobCID := range map[string]string{postInC: blobC, postInD: blobD, removedPostInC: blobRemoved} {
				_, _, err := h.manager.GetBlob(ctx, ubUserDID, blobCID)
				assert.True(t, errors.IsNotFound(err), "%s: its unshared image blob goes with it; err=%v", postID, err)
			}

			assertLive(t, h, postAfterBan, "published after the ban time")
			_, _, err := h.manager.GetBlob(ctx, ubUserDID, blobAfter)
			assert.NoError(t, err, "the post-ban post keeps its image")
			_, _, err = h.manager.GetRecord(ctx, ubUserDID, materialize.CollectionActorProfile, materialize.ProfileRKey)
			assert.NoError(t, err, "U's profile is not content: it stays")
			assertProcessed(t, h, banID)

			// No future gate: U's next post and comment still materialize.
			newPost := world.postBy(ubUserID, world.communityD, "ub-site-d2", ubWellAfterBan, false)
			world.commentBy(ubUserID, world.communityD, "ub-site-d2-reply", newPost, ubWellAfterBan)

			// Lemmy restores nothing on unban; neither do we.
			const undoID = ubHomeHost + "/activities/undo/ub-site-ban"
			world.deliverAt(world.admin, map[string]any{
				"type":   "Undo",
				"id":     undoID,
				"actor":  ubAdminID,
				"object": block,
			}, nil)
			assertProcessed(t, h, undoID)
			assertDeleted(t, h, postInC, "Undo{Block} restores nothing")
			assertDeleted(t, h, commentInC, "Undo{Block} restores nothing")
			assertDeleted(t, h, postInD, "Undo{Block} restores nothing")
		})
	}
}

// TestADirectBlockThatIsNotAHomeSiteBanRemovesNothing (B2): only U's own home
// instance, banning U from that instance, with removeData true, qualifies.
func TestADirectBlockThatIsNotAHomeSiteBanRemovesNothing(t *testing.T) {
	for _, tc := range []struct {
		name    string
		signer  func(*upstreamBanWorld) *remoteActor
		actor   string
		subject string
		target  string
		extra   map[string]any
	}{
		{
			name:   "a foreign instance's admin bans U from U's home instance",
			signer: func(w *upstreamBanWorld) *remoteActor { return w.otherAdmin },
			actor:  ubOtherAdminID, subject: ubUserID, target: ubHomeRoot,
		},
		{
			name:   "target is a community, not the instance root",
			signer: func(w *upstreamBanWorld) *remoteActor { return w.admin },
			actor:  ubAdminID, subject: ubUserID, target: ubHomeCommID,
		},
		{
			name:   "removeData false",
			signer: func(w *upstreamBanWorld) *remoteActor { return w.admin },
			actor:  ubAdminID, subject: ubUserID, target: ubHomeRoot,
			extra: map[string]any{"removeData": false},
		},
		{
			name:   "removeData absent",
			signer: func(w *upstreamBanWorld) *remoteActor { return w.admin },
			actor:  ubAdminID, subject: ubUserID, target: ubHomeRoot,
			extra: map[string]any{"removeData": nil},
		},
		{
			name:   "subject is not a bridged actor",
			signer: func(w *upstreamBanWorld) *remoteActor { return w.admin },
			actor:  ubAdminID, subject: ubUnknownID, target: ubHomeRoot,
		},
		{
			name:   "home admin targets a different instance's root",
			signer: func(w *upstreamBanWorld) *remoteActor { return w.admin },
			actor:  ubAdminID, subject: ubUserID, target: ubOtherRoot,
		},
		{
			name:   "home admin bans a bridged user from another instance",
			signer: func(w *upstreamBanWorld) *remoteActor { return w.admin },
			actor:  ubAdminID, subject: ubOtherUserID, target: ubHomeRoot,
		},
		{
			// A bridged Group's DID is its community repo: "its content" would
			// be every post in that community, whoever wrote it.
			name: "the community's own host admin bans the bridged community itself",
			signer: func(w *upstreamBanWorld) *remoteActor {
				return w.h.newRemoteActor(ubWorldAdminID, person(ubWorldAdminID, "WorldAdmin", nil))
			},
			actor: ubWorldAdminID, subject: groupID, target: ubWorldRoot,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			world := newUpstreamBanWorld(t)
			h := world.h

			postInC := world.postBy(ubUserID, world.communityC, "ub-q-c1", ubBeforeBan, false)
			commentInC := world.commentBy(ubUserID, world.communityC, "ub-q-c1-reply", postInC, ubBeforeBan2)
			postInD := world.postBy(ubUserID, world.communityD, "ub-q-d1", ubBeforeBan3, false)
			otherUsersPost := world.postBy(ubOtherUserID, world.communityC, "ub-q-v1", ubBeforeBan4, false)
			otherUsersLegacyPost := world.legacyPostByOtherUser(t)

			activityID := originOf(tc.actor) + "/activities/block/ub-q-ban"
			banTime := ubBanTime
			world.deliverAt(tc.signer(world),
				siteBlock(activityID, tc.actor, tc.subject, tc.target, tc.extra), &banTime)

			assertLive(t, h, postInC, tc.name)
			assertLive(t, h, commentInC, tc.name)
			assertLive(t, h, postInD, tc.name)
			assertLive(t, h, otherUsersPost, tc.name)
			assertLive(t, h, otherUsersLegacyPost, tc.name)
			assertProcessed(t, h, activityID)
		})
	}
}

// legacyPostByOtherUser writes V's pre-ban legacy post into C's repo — the
// record a ban whose subject resolved to C's own DID would select — and
// checks C is bridged as a Group, the subject shape that must be refused.
func (w *upstreamBanWorld) legacyPostByOtherUser(t *testing.T) string {
	t.Helper()
	const legacyPostID = "https://lemmy.ml/post/ub-legacy-v"
	putLegacyPost(t, w.h, ubCommunityCDID, replayOtherUserDID, legacyPostID, replayLegacyRKey,
		time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC))
	group, err := w.h.actors.GetByAPActorID(context.Background(), groupID)
	require.NoError(t, err, "precondition: C is a bridged actor")
	require.Equal(t, store.ActorTypeGroup, group.ActorType, "precondition: C is bridged as a Group")
	require.Equal(t, ubCommunityCDID, group.DID, "precondition: C's DID is the repo holding the legacy post")
	assertLive(t, w.h, legacyPostID, "precondition: V's legacy post in C's repo is live")
	return legacyPostID
}

// TestACommunityBanOfABridgedUserRemovesTheirEarlierContentThere (B3).
//
// GIVEN bridged user U with, before the ban, a postv2 and a comment in C and a
// postv2 in D, plus a post in C after the ban; WHEN C announces a Block of U
// with removeData (permanent, or already lapsed) THEN U's pre-ban post in C is
// withdrawn with an author-banned removal (U's record itself stays), the
// pre-ban comment in C is deleted, and D and the post-ban post are untouched.
// U's later content in C still materializes (no ban row is written for a
// foreign subject), and Undo{Block} restores nothing.
func TestACommunityBanOfABridgedUserRemovesTheirEarlierContentThere(t *testing.T) {
	for _, tc := range []struct {
		name    string
		subject string
		extra   map[string]any
		removes bool
	}{
		{name: "permanent ban", subject: ubUserID, removes: true},
		{name: "ban already lapsed when it arrived", subject: ubUserID,
			extra: map[string]any{"expires": ubLapsedExpiry}, removes: true},
		{name: "removeData false", subject: ubUserID,
			extra: map[string]any{"removeData": false}, removes: false},
		{name: "subject is neither a persona nor a bridged actor", subject: ubUnknownID, removes: false},
		// A bridged Group's DID is its community repo: "its content" would be
		// every post in C, whoever wrote it.
		{name: "subject is the bridged community itself", subject: groupID, removes: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			world := newUpstreamBanWorld(t)
			h := world.h
			ctx := context.Background()

			postInC := world.postBy(ubUserID, world.communityC, "ub-cb-c1", ubBeforeBan, false)
			commentInC := world.commentBy(ubUserID, world.communityC, "ub-cb-c1-reply", postInC, ubBeforeBan2)
			postInD := world.postBy(ubUserID, world.communityD, "ub-cb-d1", ubBeforeBan3, false)
			postAfterBan := world.postBy(ubUserID, world.communityC, "ub-cb-c2", ubAfterBan, false)
			otherUsersLegacyPost := world.legacyPostByOtherUser(t)
			require.True(t, acceptanceExists(t, h, ubCommunityCDID, postInC), "precondition: accepted in C")

			const banID = "https://lemmy.world/activities/announce/block/ub-cb-ban"
			world.announceCommunityBlock(world.communityC, banID, tc.subject, tc.extra, ubBanTime)
			assertProcessed(t, h, banID)
			assert.Zero(t, bansIn(t, h.db, ubCommunityCDID),
				"a ban of a bridged Lemmy user records no community_bans row: there is no future gate")

			if tc.removes {
				assert.False(t, acceptanceExists(t, h, ubCommunityCDID, postInC),
					"U's pre-ban post is withdrawn from C")
				assert.Equal(t, materialize.RemovalCodeAuthorBanned, removalCode(t, h, ubCommunityCDID, postInC),
					"and C records why: its author was banned")
				mapping, err := h.objects.GetByAPID(ctx, postInC)
				require.NoError(t, err)
				_, _, err = h.manager.GetRecord(ctx, ubUserDID, materialize.CollectionPostV2, mapping.RKey)
				assert.NoError(t, err, "U's postv2 record itself stays: a community ban is community-scoped")
				assertDeleted(t, h, commentInC, "pre-ban comment in C")
			} else {
				assert.True(t, acceptanceExists(t, h, ubCommunityCDID, postInC), "nothing removed: %s", tc.name)
				assert.Empty(t, removalCode(t, h, ubCommunityCDID, postInC), "no removal record: %s", tc.name)
				assertLive(t, h, commentInC, tc.name)
			}
			assertLive(t, h, postInD, "a community ban does not reach another community")
			assert.True(t, acceptanceExists(t, h, ubCommunityDDID, postInD), "still accepted in D")
			assert.Empty(t, removalCode(t, h, ubCommunityDDID, postInD), "no removal in D")
			assertLive(t, h, postAfterBan, "published after the ban time")
			assert.True(t, acceptanceExists(t, h, ubCommunityCDID, postAfterBan), "post-ban post stays accepted in C")
			assertLive(t, h, otherUsersLegacyPost, "V was banned by no one")

			// No future gate: U's next post and comment in C materialize and are accepted.
			newPost := world.postBy(ubUserID, world.communityC, "ub-cb-c3", ubWellAfterBan, false)
			world.commentBy(ubUserID, world.communityC, "ub-cb-c3-reply", newPost, ubWellAfterBan)
			assert.True(t, acceptanceExists(t, h, ubCommunityCDID, newPost), "U's new post is accepted in C")

			// Undo{Block} restores nothing.
			const undoID = "https://lemmy.world/activities/announce/undo/ub-cb-ban"
			world.announceCommunityUndoBlock(world.communityC, undoID, banID, tc.subject)
			assertProcessed(t, h, undoID)
			if tc.removes {
				assert.False(t, acceptanceExists(t, h, ubCommunityCDID, postInC), "Undo{Block} restores nothing")
				assert.Equal(t, materialize.RemovalCodeAuthorBanned, removalCode(t, h, ubCommunityCDID, postInC),
					"the removal record remains after Undo{Block}")
				assertDeleted(t, h, commentInC, "Undo{Block} restores nothing")
			}
		})
	}
}

// TestAnUpstreamBanRemovesOnlyContentPublishedBeforeItsCutoff (B7) owns the
// cutoff: Block.published when present and no later than the event's
// received_at, else received_at (never the processing time); strictly before;
// a mapping with no published time is weighed by its indexed_at.
func TestAnUpstreamBanRemovesOnlyContentPublishedBeforeItsCutoff(t *testing.T) {
	const (
		t0 = "2026-08-20T00:00:00Z"
		t1 = "2026-09-01T00:00:00Z"
		t2 = "2026-09-05T00:00:00Z"
		t3 = 9 * 24 * time.Hour // received T1 + 9d = 2026-09-10
	)
	received := func(offset time.Duration) *time.Time {
		when := ubBanTime.Add(offset)
		return &when
	}
	for _, tc := range []struct {
		name string
		// blockPublished is the Block's `published`, or "" for none.
		blockPublished string
		receivedAt     *time.Time
		// removedPublished / survivorPublished are the two posts' `published`.
		removedPublished  string
		survivorPublished string
		// nilPublished re-stamps both mappings with no published time and these
		// indexed_at values.
		nilPublished      bool
		removedIndexedAt  time.Time
		survivorIndexedAt time.Time
	}{
		{
			name:           "published T1, received T3: content at T2 survives",
			blockPublished: t1, receivedAt: received(t3),
			removedPublished: t0, survivorPublished: t2,
		},
		{
			name:             "no published, received T1, processed later: content at T2 survives",
			receivedAt:       received(0),
			removedPublished: t0, survivorPublished: t2,
		},
		{
			// A `published` later than receipt is a claim about a ban not yet
			// made: the receipt time bounds it, so content written between the
			// two is not the ban's business.
			name:           "published after receipt: received T1 is the cutoff, content at T2 survives",
			blockPublished: "2026-09-20T00:00:00Z", receivedAt: received(0),
			removedPublished: t0, survivorPublished: t2,
		},
		{
			name:           "content published exactly at the cutoff survives",
			blockPublished: t1, receivedAt: received(t3),
			removedPublished: t0, survivorPublished: t1,
		},
		{
			name:           "a mapping with no published time is weighed by its indexed_at",
			blockPublished: t1, receivedAt: received(t3),
			removedPublished: t0, survivorPublished: t0,
			nilPublished:      true,
			removedIndexedAt:  time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC),
			survivorIndexedAt: time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			world := newUpstreamBanWorld(t)
			h := world.h

			removed := world.postBy(ubUserID, world.communityC, "ub-cut-before", tc.removedPublished, false)
			survivor := world.postBy(ubUserID, world.communityC, "ub-cut-after", tc.survivorPublished, false)
			if tc.nilPublished {
				for apID, indexedAt := range map[string]time.Time{removed: tc.removedIndexedAt, survivor: tc.survivorIndexedAt} {
					result, err := h.db.ExecContext(context.Background(),
						`UPDATE ap_objects SET ap_published_at = NULL, indexed_at = $1 WHERE ap_id = $2`,
						indexedAt, apID)
					require.NoError(t, err)
					rows, err := result.RowsAffected()
					require.NoError(t, err)
					require.Equal(t, int64(1), rows)
				}
			}

			const banID = ubHomeHost + "/activities/block/ub-cut-ban"
			var extra map[string]any
			if tc.blockPublished != "" {
				extra = map[string]any{"published": tc.blockPublished}
			}
			world.deliverAt(world.admin, siteBlock(banID, ubAdminID, ubUserID, ubHomeRoot, extra), tc.receivedAt)

			assertDeleted(t, h, removed, "published before the cutoff")
			assertLive(t, h, survivor, "not published before the cutoff")
			assertProcessed(t, h, banID)
		})
	}
}

// TestAnUpstreamBanKeepsAnImageBlobAnotherLiveRecordStillUses (B4).
//
// GIVEN bridged user U with post A (published T0) and post B (published T2)
// carrying byte-identical images (one blob X in U's repo), and post A2
// (published T0) with its own image Y, and posts Z1 and Z2 (published T0)
// sharing image Z; WHEN U's home admin site-bans U with removeData at T1
// (T0 < T1 < T2) THEN A, A2, Z1 and Z2 are deleted, Y goes with A2, Z goes
// because the same ban deleted every record sharing it, and X stays
// retrievable because B still references it. WHEN a later re-ban
// at T3 > T2 deletes B, the last reference, THEN X goes too.
func TestAnUpstreamBanKeepsAnImageBlobAnotherLiveRecordStillUses(t *testing.T) {
	const (
		t0 = "2026-08-10T12:00:00Z"
		t2 = "2026-09-15T00:00:00Z"
	)
	var (
		t1 = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
		t3 = time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	)
	world := newUpstreamBanWorld(t)
	h := world.h
	ctx := context.Background()

	postA := world.postWithImageSeed(ubUserID, world.communityC, "ub-blob-a", t0, "ub-blob-shared-x")
	postA2 := world.postWithImageSeed(ubUserID, world.communityD, "ub-blob-a2", t0, "ub-blob-own-y")
	postB := world.postWithImageSeed(ubUserID, world.communityC, "ub-blob-b", t2, "ub-blob-shared-x")
	postZ1 := world.postWithImageSeed(ubUserID, world.communityC, "ub-blob-z1", t0, "ub-blob-shared-z")
	postZ2 := world.postWithImageSeed(ubUserID, world.communityD, "ub-blob-z2", t0, "ub-blob-shared-z")

	blobX := postBlobCID(t, h, postA)
	require.Equal(t, blobX, postBlobCID(t, h, postB), "precondition: A and B share one image blob X")
	blobY := postBlobCID(t, h, postA2)
	require.NotEqual(t, blobX, blobY, "precondition: A2's image Y is its own blob")
	blobZ := postBlobCID(t, h, postZ1)
	require.Equal(t, blobZ, postBlobCID(t, h, postZ2), "precondition: Z1 and Z2 share one image blob Z")
	require.NotContains(t, []string{blobX, blobY}, blobZ, "precondition: Z is neither X nor Y")

	const firstBanID = ubHomeHost + "/activities/block/ub-blob-ban-1"
	world.deliverAt(world.admin, siteBlock(firstBanID, ubAdminID, ubUserID, ubHomeRoot, nil), &t1)
	assertProcessed(t, h, firstBanID)

	assertDeleted(t, h, postA, "published T0, before the T1 ban")
	assertDeleted(t, h, postA2, "published T0, before the T1 ban")
	assertLive(t, h, postB, "published T2, after the T1 ban")
	_, _, err := h.manager.GetBlob(ctx, ubUserDID, blobY)
	assert.True(t, errors.IsNotFound(err), "A2's unshared image Y goes with it; err=%v", err)
	_, _, err = h.manager.GetBlob(ctx, ubUserDID, blobX)
	assert.NoError(t, err, "image X stays: live post B still references it")
	assertDeleted(t, h, postZ1, "published T0, before the T1 ban")
	assertDeleted(t, h, postZ2, "published T0, before the T1 ban")
	_, _, err = h.manager.GetBlob(ctx, ubUserDID, blobZ)
	assert.True(t, errors.IsNotFound(err),
		"image Z goes: both records sharing it were deleted by the same ban; err=%v", err)

	// Lemmy re-bans U later (a distinct activity); now B predates the cutoff.
	const secondBanID = ubHomeHost + "/activities/block/ub-blob-ban-2"
	world.deliverAt(world.admin, siteBlock(secondBanID, ubAdminID, ubUserID, ubHomeRoot, nil), &t3)
	assertProcessed(t, h, secondBanID)

	assertDeleted(t, h, postB, "published T2, before the T3 re-ban")
	_, _, err = h.manager.GetBlob(ctx, ubUserDID, blobX)
	assert.True(t, errors.IsNotFound(err), "image X goes once its last referencing record is deleted; err=%v", err)
}

// TestACommunityBanDeletesACommentWhoseThreadRootWasAlreadyDeleted (B3, review).
//
// GIVEN bridged user U's postv2 P in C and bridged user V's comment on P, whose
// mapping records no community (the pre-016 shape), both published before
// either ban; WHEN U's home admin site-bans U (deleting P's record while P's
// mapping keeps its community binding) and THEN C announces a ban of V with
// removeData THEN V's comment is still found to be C's content and deleted: a
// thread root already gone must not shield the replies under it.
func TestACommunityBanDeletesACommentWhoseThreadRootWasAlreadyDeleted(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	ctx := context.Background()

	rootPost := world.postBy(ubUserID, world.communityC, "ub-rd-u-c1", ubBeforeBan, false)
	otherComment := world.commentBy(ubOtherUserID, world.communityC, "ub-rd-v-c1-reply", rootPost, ubBeforeBan2)
	clearCommunityDID(t, h, otherComment)

	const siteBanID = ubHomeHost + "/activities/block/ub-rd-site"
	siteBanTime := ubBanTime
	world.deliverAt(world.admin, siteBlock(siteBanID, ubAdminID, ubUserID, ubHomeRoot, nil), &siteBanTime)
	assertProcessed(t, h, siteBanID)
	assertDeleted(t, h, rootPost, "precondition: U's site ban deleted the thread root")
	rootMapping, err := h.objects.GetByAPID(ctx, rootPost)
	require.NoError(t, err)
	require.Equal(t, ubCommunityCDID, rootMapping.CommunityDID,
		"precondition: the deleted root's mapping still records its community")
	assertLive(t, h, otherComment, "precondition: U's site ban does not reach V's comment")

	const communityBanID = "https://lemmy.world/activities/announce/block/ub-rd-community"
	world.announceCommunityBlock(world.communityC, communityBanID, ubOtherUserID, nil,
		time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC))
	assertProcessed(t, h, communityBanID)

	assertDeleted(t, h, otherComment,
		"V's pre-ban comment in C, its community resolved through a thread root that is already deleted")
}
