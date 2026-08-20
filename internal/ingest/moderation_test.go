package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/base32"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/repo"
	"tidepool/internal/store"
)

// Lemmy moderation, end to end through the announced-delete path.
//
// The wire shapes here are the ones captured off live Lemmy 0.19.20: a mod
// removal is Announce{Delete} whose INNER Delete is attributed to the
// moderator (a /u/ actor, not the community) and carries a `summary`; the
// summary is an EMPTY STRING when the moderator gave no reason. A self-delete
// is the same activity with NO summary key. A restore is
// Announce{Undo{Delete}} carrying the delete inline.
//
// The distinction is the whole point: mod removal leaves the author's post
// intact and records a community-side removal, while a self-delete takes the
// post away. Reading one as the other either destroys an author's post over a
// moderator's hidden action, or fabricates a moderation record against an
// author who moderated nobody.

const modActorID = "https://lemmy.world/u/moderator"

// testDigestRKey re-derives the acceptance/removal record key INDEPENDENTLY
// (unpadded lowercase base32 of SHA-256 of the subject at-uri). Deliberately
// not materialize.SubjectRKey: this tier asserts the rkey the bridge actually
// wrote to, and a test that called the production helper could not detect a
// change in it because both sides would move together. The golden vectors
// live in internal/materialize/subject_rkey_test.go.
func testDigestRKey(subjectURI string) string {
	digest := sha256.Sum256([]byte(subjectURI))
	return strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(digest[:]))
}

// moderatedPost is the fixture every test here starts from: the captured
// lemmy.world page, announced into a followed community and materialized as a
// postv2 in the author's repo plus an acceptance in the community's.
type moderatedPost struct {
	group        *remoteActor
	communityDID string
	authorDID    string
	rkey         string
	postURI      string
	digestRKey   string
	postCID      string
	acceptedCID  string
}

func setupModeratedPost(t *testing.T, h *harness) moderatedPost {
	t.Helper()
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	ctx := context.Background()

	require.Equal(t, http.StatusAccepted,
		h.deliver(group, loadFixture(t, "announce_create_page_lemmy_world.json")))
	h.drain()

	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	require.False(t, mapping.IsDeleted())
	require.Equal(t, materialize.CollectionPostV2, mapping.Collection,
		"precondition: the post materialized as a postv2")

	communityDID := testDIDFor("technology", "lemmy.world")
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
	postURI := "at://" + authorDID + "/" + materialize.CollectionPostV2 + "/" + mapping.RKey
	digest := testDigestRKey(postURI)

	_, postCID, err := h.manager.GetRecord(ctx, authorDID, materialize.CollectionPostV2, mapping.RKey)
	require.NoError(t, err)

	acceptance, _, err := h.manager.GetRecord(ctx, communityDID, materialize.CollectionAcceptance, digest)
	require.NoError(t, err, "precondition: the community accepted the post")
	subject, ok := acceptance["subject"].(map[string]any)
	require.True(t, ok)

	return moderatedPost{
		group:        group,
		communityDID: communityDID,
		authorDID:    authorDID,
		rkey:         mapping.RKey,
		postURI:      postURI,
		digestRKey:   digest,
		postCID:      postCID,
		acceptedCID:  strongRefCID(subject["cid"]),
	}
}

// strongRefCID normalizes a strongRef cid (typed CID-link or {"$link":...}).
func strongRefCID(v any) string {
	switch value := v.(type) {
	case string:
		return value
	case map[string]any:
		if link, ok := value["$link"].(string); ok {
			return link
		}
	case interface{ String() string }:
		return value.String()
	}
	return ""
}

// announceDeleteWithSummary delivers the live mod-removal shape: the inner
// Delete is the MODERATOR's, announced by the community, with `summary`
// PRESENT. A nil summary omits the key entirely — the self-delete shape.
func (h *harness) announceDeleteWithSummary(group *remoteActor, activityID, targetID string, summary *string) {
	h.t.Helper()
	h.announceDeleteBy(group, activityID, targetID, modActorID, summary)
}

// announceDeleteBy is announceDeleteWithSummary with an explicit inner actor.
// WHO the Delete is attributed to decides which semantics apply: the AUTHOR
// deleting their own post destroys the record, anyone else may at most remove
// it from the community.
func (h *harness) announceDeleteBy(group *remoteActor, activityID, targetID, actor string, summary *string) {
	h.t.Helper()
	inner := map[string]any{
		"id":       activityID + "/delete",
		"type":     "Delete",
		"actor":    actor,
		"object":   targetID,
		"audience": group.id,
		"cc":       []any{group.id},
	}
	if summary != nil {
		inner["summary"] = *summary
	}
	require.Equal(h.t, http.StatusAccepted, h.deliver(group, map[string]any{
		"id":       activityID,
		"type":     "Announce",
		"actor":    group.id,
		"audience": group.id,
		"cc":       []any{group.id + "/followers"},
		"object":   inner,
	}))
	h.drain()
}

// announceUndoDelete delivers the live restore shape: Announce{Undo{Delete}}
// with the delete activity carried INLINE (Lemmy embeds it rather than
// referencing its id).
func (h *harness) announceUndoDelete(group *remoteActor, activityID, deleteActivityID, targetID string, summary *string) {
	h.t.Helper()
	inner := map[string]any{
		"id":       deleteActivityID,
		"type":     "Delete",
		"actor":    modActorID,
		"object":   targetID,
		"audience": group.id,
		"cc":       []any{group.id},
	}
	if summary != nil {
		inner["summary"] = *summary
	}
	require.Equal(h.t, http.StatusAccepted, h.deliver(group, map[string]any{
		"id":       activityID,
		"type":     "Announce",
		"actor":    group.id,
		"audience": group.id,
		"cc":       []any{group.id + "/followers"},
		"object": map[string]any{
			"id":       activityID + "/undo",
			"type":     "Undo",
			"actor":    modActorID,
			"audience": group.id,
			"cc":       []any{group.id},
			"object":   inner,
		},
	}))
	h.drain()
}

// communityEvents returns the firehose events for one repo.
func communityEvents(t *testing.T, h *harness, did string) []*repo.Event {
	t.Helper()
	events, err := h.manager.ListEvents(context.Background(), 0, 1000)
	require.NoError(t, err)
	var out []*repo.Event
	for _, event := range events {
		if event.DID == did {
			out = append(out, event)
		}
	}
	return out
}

// TestModRemovalWritesRemovalAtomically (R1): the flow the task exists for.
// One commit in the community repo turns "accepted" into "removed"; the
// author's post is untouched, because a mod removing a post from a community
// does not delete what the author wrote.
func TestModRemovalWritesRemovalAtomically(t *testing.T) {
	h := newHarness(t)
	post := setupModeratedPost(t, h)
	ctx := context.Background()
	eventsBefore := len(communityEvents(t, h, post.communityDID))

	reason := "spam wave"
	h.announceDeleteWithSummary(post.group,
		"https://lemmy.world/activities/announce/delete/mod-removal", pageID, &reason)

	// ONE commit carrying BOTH ops: a consumer must never see the acceptance
	// gone without the removal present, or the post reads as neither accepted
	// nor removed.
	events := communityEvents(t, h, post.communityDID)
	require.Len(t, events, eventsBefore+1,
		"a mod removal must be exactly one community-repo commit")
	ops := events[len(events)-1].Ops
	require.Len(t, ops, 2, "the acceptance delete and the removal write ride one commit, got %v", ops)
	byPath := map[string]repo.Op{}
	for _, op := range ops {
		byPath[op.Path] = op
	}
	acceptanceOp, ok := byPath[materialize.CollectionAcceptance+"/"+post.digestRKey]
	require.True(t, ok, "the acceptance delete must be on the commit, got %v", byPath)
	assert.Equal(t, repo.OpActionDelete, acceptanceOp.Action)
	_, ok = byPath[materialize.CollectionRemoval+"/"+post.digestRKey]
	require.True(t, ok, "the removal write must be on the SAME commit, got %v", byPath)

	// The acceptance is gone.
	_, _, err := h.manager.GetRecord(ctx, post.communityDID, materialize.CollectionAcceptance, post.digestRKey)
	assert.True(t, errors.IsNotFound(err), "a removed post must not keep its acceptance (err=%v)", err)

	// The removal names the post, at the SAME digest rkey the acceptance had.
	removal, _, err := h.manager.GetRecord(ctx, post.communityDID, materialize.CollectionRemoval, post.digestRKey)
	require.NoError(t, err,
		"a mod removal must write a removal record at the subject digest rkey")
	assert.Equal(t, materialize.CollectionRemoval, removal["$type"])
	subject, ok := removal["subject"].(map[string]any)
	require.True(t, ok, "removal.subject must be a strongRef, got %#v", removal["subject"])
	assert.Equal(t, post.postURI, subject["uri"])
	assert.Equal(t, post.acceptedCID, strongRefCID(subject["cid"]),
		"the removal pins the version that was accepted at removal time (audit metadata)")
	assert.Equal(t, "moderator-discretion", removal["code"],
		"Lemmy sends no machine-readable code, so the open knownValues set's default applies")
	assert.Equal(t, reason, removal["reason"], "the moderator's text is the human-readable reason")
	assert.NotEmpty(t, removal["createdAt"])

	// The author's post lives on, and so does its mapping.
	_, _, err = h.manager.GetRecord(ctx, post.authorDID, materialize.CollectionPostV2, post.rkey)
	assert.NoError(t, err,
		"a community removing a post must not delete the author's record — removal is community-scoped")
	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	assert.False(t, mapping.IsDeleted(),
		"the post still exists; tombstoning its mapping would block every later edit and vote")
}

// TestModRemovalWithEmptySummary (R2): Lemmy sends `"summary": ""` when the
// moderator typed no reason. That is the ORDINARY removal, not an edge case,
// and it must produce a removal with the default code and no reason field —
// an empty-string reason would render as a blank explanation in the
// moderation log rather than as "none given".
func TestModRemovalWithEmptySummary(t *testing.T) {
	h := newHarness(t)
	post := setupModeratedPost(t, h)
	ctx := context.Background()

	empty := ""
	h.announceDeleteWithSummary(post.group,
		"https://lemmy.world/activities/announce/delete/mod-removal-noreason", pageID, &empty)

	_, _, err := h.manager.GetRecord(ctx, post.communityDID, materialize.CollectionAcceptance, post.digestRKey)
	assert.True(t, errors.IsNotFound(err), "the acceptance must be gone (err=%v)", err)

	removal, _, err := h.manager.GetRecord(ctx, post.communityDID, materialize.CollectionRemoval, post.digestRKey)
	require.NoError(t, err,
		"a PRESENT-but-empty summary is still a mod removal: it is how Lemmy spells "+
			"'removed, no reason given'")
	assert.Equal(t, "moderator-discretion", removal["code"])
	assert.NotContains(t, removal, "reason",
		"no reason was given, so the field is omitted rather than written blank")
	assert.NotEmpty(t, removal["createdAt"])

	_, _, err = h.manager.GetRecord(ctx, post.authorDID, materialize.CollectionPostV2, post.rkey)
	assert.NoError(t, err, "the author's post survives a reasonless removal too")
}

// TestSelfDeleteRemovesPostAndAcceptance (R3): no summary key AND the delete
// attributed to the post's own author means the AUTHOR deleted their own post. The post goes, its acceptance goes with it (an
// acceptance whose subject is gone is inert), and NO removal is written —
// author deletion is not moderation, and recording it as such would put a
// moderation action in the log against someone who was never moderated.
func TestSelfDeleteRemovesPostAndAcceptance(t *testing.T) {
	h := newHarness(t)
	post := setupModeratedPost(t, h)
	ctx := context.Background()

	h.announceDeleteBy(post.group,
		"https://lemmy.world/activities/announce/delete/self", pageID, personID, nil)

	_, _, err := h.manager.GetRecord(ctx, post.authorDID, materialize.CollectionPostV2, post.rkey)
	assert.True(t, errors.IsNotFound(err),
		"a self-delete removes the author's post (err=%v)", err)
	_, _, err = h.manager.GetRecord(ctx, post.communityDID, materialize.CollectionAcceptance, post.digestRKey)
	assert.True(t, errors.IsNotFound(err),
		"the acceptance of a deleted post must not linger (err=%v)", err)
	_, _, err = h.manager.GetRecord(ctx, post.communityDID, materialize.CollectionRemoval, post.digestRKey)
	assert.True(t, errors.IsNotFound(err),
		"an author deleting their own post is not a moderation action: no removal record (err=%v)", err)

	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	assert.True(t, mapping.IsDeleted(),
		"v1 tombstone semantics: a self-deleted post's mapping is soft-deleted so a "+
			"re-delivered Create cannot resurrect it")
}

// TestRestoreDeletesRemovalAndReacceptsAtomically (R4): a moderator undoing
// their removal must leave the community in the state it was in before —
// accepted, pinning the CURRENT post version — and must do it in ONE commit,
// for the same reason the removal was atomic.
func TestRestoreDeletesRemovalAndReacceptsAtomically(t *testing.T) {
	h := newHarness(t)
	post := setupModeratedPost(t, h)
	ctx := context.Background()

	reason := "spam wave"
	h.announceDeleteWithSummary(post.group,
		"https://lemmy.world/activities/announce/delete/to-be-undone", pageID, &reason)
	_, _, err := h.manager.GetRecord(ctx, post.communityDID, materialize.CollectionRemoval, post.digestRKey)
	require.NoError(t, err, "precondition: the removal landed")

	eventsBefore := len(communityEvents(t, h, post.communityDID))

	h.announceUndoDelete(post.group,
		"https://lemmy.world/activities/announce/undo/restore",
		"https://lemmy.world/activities/announce/delete/to-be-undone/delete",
		pageID, &reason)

	events := communityEvents(t, h, post.communityDID)
	require.Len(t, events, eventsBefore+1,
		"a restore must be exactly one community-repo commit")
	ops := events[len(events)-1].Ops
	require.Len(t, ops, 2, "the removal delete and the fresh acceptance ride one commit, got %v", ops)
	byPath := map[string]repo.Op{}
	for _, op := range ops {
		byPath[op.Path] = op
	}
	removalOp, ok := byPath[materialize.CollectionRemoval+"/"+post.digestRKey]
	require.True(t, ok, "the removal delete must be on the commit, got %v", byPath)
	assert.Equal(t, repo.OpActionDelete, removalOp.Action)
	_, ok = byPath[materialize.CollectionAcceptance+"/"+post.digestRKey]
	require.True(t, ok, "the fresh acceptance must be on the SAME commit, got %v", byPath)

	_, _, err = h.manager.GetRecord(ctx, post.communityDID, materialize.CollectionRemoval, post.digestRKey)
	assert.True(t, errors.IsNotFound(err), "the removal must be gone after a restore (err=%v)", err)

	acceptance, _, err := h.manager.GetRecord(ctx, post.communityDID, materialize.CollectionAcceptance, post.digestRKey)
	require.NoError(t, err, "a restore re-accepts the post")
	subject, ok := acceptance["subject"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, post.postURI, subject["uri"])
	_, currentCID, err := h.manager.GetRecord(ctx, post.authorDID, materialize.CollectionPostV2, post.rkey)
	require.NoError(t, err)
	assert.Equal(t, currentCID, strongRefCID(subject["cid"]),
		"the fresh acceptance pins the CURRENT post version, not the one removed")

	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	assert.False(t, mapping.IsDeleted(), "a restored post's mapping is live")
}

// TestRemovalSurvivesRedeliveredCreate (R5) is the terminality guard. A
// removal is exited ONLY by an explicit restore. Redelivery of the original
// Create is routine — the queue re-runs it, and so does a backfill — and
// re-materializing the post drives the acceptance write. If nothing stops
// that write, a fresh acceptance IS a restore: the post reappears in the
// community, having been un-removed by a redelivery nobody intended as one.
func TestRemovalSurvivesRedeliveredCreate(t *testing.T) {
	h := newHarness(t)
	post := setupModeratedPost(t, h)
	ctx := context.Background()

	reason := "spam wave"
	h.announceDeleteWithSummary(post.group,
		"https://lemmy.world/activities/announce/delete/terminal", pageID, &reason)
	removalBefore, removalCIDBefore, err := h.manager.GetRecord(ctx,
		post.communityDID, materialize.CollectionRemoval, post.digestRKey)
	require.NoError(t, err, "precondition: the removal landed")

	// The same post is announced again under a FRESH announce id — what a
	// real re-announce looks like, and the only shape that reaches the
	// materializer at all: the inbox dedupes by activity id, so replaying the
	// original announce would be absorbed there and prove nothing about the
	// terminality guard.
	const reAnnounceID = "https://lemmy.world/activities/announce/create/re-announce"
	h.announceCreate(post.group, reAnnounceID, loadFixture(t, "page_lemmy_world.json"))

	// Self-proof: the re-announce must actually have been PROCESSED, not
	// absorbed by inbox dedupe or dropped by authorization. Without this the
	// assertions below would pass on an activity that never reached the
	// materializer, and the guard would be untested.
	event, err := h.events.GetEvent(ctx, reAnnounceID)
	require.NoError(t, err, "the re-announce must have been accepted as a new activity")
	require.NotNil(t, event.ProcessedAt, "the re-announce must have been processed")
	require.Empty(t, event.Error)
	_, _, err = h.manager.GetRecord(ctx, post.authorDID, materialize.CollectionPostV2, post.rkey)
	require.NoError(t, err, "the re-announce re-materialized the post itself")

	_, _, err = h.manager.GetRecord(ctx, post.communityDID, materialize.CollectionAcceptance, post.digestRKey)
	assert.True(t, errors.IsNotFound(err),
		"a redelivered Create must NOT re-accept a removed post: a fresh acceptance is exactly "+
			"what a restore is, so this would silently un-remove it (err=%v)", err)

	removalAfter, removalCIDAfter, err := h.manager.GetRecord(ctx,
		post.communityDID, materialize.CollectionRemoval, post.digestRKey)
	require.NoError(t, err, "the removal must still stand")
	assert.Equal(t, removalCIDBefore, removalCIDAfter, "the removal record must not be rewritten")
	assert.Equal(t, removalBefore["createdAt"], removalAfter["createdAt"])
}

// TestSweepDeletedPostV2RemovesAcceptanceWithoutRemoval (N2): the
// origin-verified delete sweep is CLEANUP, not moderation. When the origin
// stops serving a post, the bridge stops carrying it — the postv2 goes, and
// its acceptance must go with it or the community is left attesting to a
// record that no longer exists. No removal record is written: nobody
// moderated anything, and a removal would put a moderation action in the
// community's log against an author whose instance simply deleted the post.
//
// Note the sweep's safety rule stands unchanged: a 404 is NOT a delete (an
// instance hiding an object it will not serve us is indistinguishable from a
// missing one), so only the origin's explicit Tombstone triggers this.
func TestSweepDeletedPostV2RemovesAcceptanceWithoutRemoval(t *testing.T) {
	h := newHarness(t)
	post := setupModeratedPost(t, h)
	ctx := context.Background()

	// The origin now serves Lemmy's deleted-object shape.
	h.serveObject(urlPath(t, pageID), map[string]any{"id": pageID, "type": "Tombstone"})
	out := h.sweep(pageID)
	require.Equal(t, OutcomeDeleted, out.Result[0].Outcome)
	require.Equal(t, 1, out.Deleted)

	_, _, err := h.manager.GetRecord(ctx, post.authorDID, materialize.CollectionPostV2, post.rkey)
	assert.True(t, errors.IsNotFound(err),
		"the swept postv2 must be deleted from the author's repo (err=%v)", err)
	_, _, err = h.manager.GetRecord(ctx, post.communityDID, materialize.CollectionAcceptance, post.digestRKey)
	assert.True(t, errors.IsNotFound(err),
		"the acceptance must not outlive the post it attests to (err=%v)", err)
	_, _, err = h.manager.GetRecord(ctx, post.communityDID, materialize.CollectionRemoval, post.digestRKey)
	assert.True(t, errors.IsNotFound(err),
		"an origin-verified sweep is cleanup, not moderation: no removal record (err=%v)", err)

	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	assert.True(t, mapping.IsDeleted(), "the mapping must be soft-deleted")
	tombstoned, err := h.tombstones.ExistsFor(ctx, pageID, "")
	require.NoError(t, err)
	assert.True(t, tombstoned, "the create-after-delete marker must be recorded")
}

// TestLegacyPostModRemovalKeepsV1Semantics (R6) is the mixed-era control.
// Moderation records are postv2-only in this task: a pre-flip post has no
// acceptance to delete, and writing a removal for it would announce a
// visibility mechanism Coves does not consult for that collection. The v1
// behaviour — delete the record, tombstone the mapping — must be untouched.
func TestLegacyPostModRemovalKeepsV1Semantics(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	ctx := context.Background()

	communityDID := testDIDFor("technology", "lemmy.world")
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")

	// A pre-flip post: in the COMMUNITY's repo, deprecated collection, with
	// the in-record author the old lexicon required.
	const legacyAPID = "https://lemmy.world/post/424242"
	const legacyRKey = "3kjzl5kcb2s2v"
	published := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
	commit, err := h.manager.PutRecord(ctx, communityDID, materialize.CollectionPost, legacyRKey,
		map[string]any{
			"$type":     materialize.CollectionPost,
			"community": communityDID,
			"author":    authorDID,
			"createdAt": "2026-06-01T09:00:00.000Z",
			"title":     "a pre-flip post",
		})
	require.NoError(t, err)
	_, err = h.objects.PutMapping(ctx, store.APObjectMapping{
		APID:           legacyAPID,
		APType:         "Page",
		OriginInstance: "lemmy.world",
		Origin:         store.OriginFediverse,
		DID:            communityDID,
		AuthorDID:      authorDID,
		CommunityDID:   communityDID,
		Collection:     materialize.CollectionPost,
		RKey:           legacyRKey,
		CID:            commit.RecordCID,
		PublishedAt:    &published,
	})
	require.NoError(t, err)

	reason := "spam wave"
	h.announceDeleteWithSummary(group,
		"https://lemmy.world/activities/announce/delete/legacy", legacyAPID, &reason)

	_, _, err = h.manager.GetRecord(ctx, communityDID, materialize.CollectionPost, legacyRKey)
	assert.True(t, errors.IsNotFound(err),
		"v1 semantics: a moderated legacy post's record is deleted outright (err=%v)", err)

	mapping, err := h.objects.GetByAPID(ctx, legacyAPID)
	require.NoError(t, err)
	assert.True(t, mapping.IsDeleted(), "v1 semantics: the mapping is tombstoned")

	legacyURI := "at://" + communityDID + "/" + materialize.CollectionPost + "/" + legacyRKey
	_, _, err = h.manager.GetRecord(ctx, communityDID, materialize.CollectionRemoval, testDigestRKey(legacyURI))
	assert.True(t, errors.IsNotFound(err),
		"moderation records are postv2-only in this task: a legacy post writes none (err=%v)", err)

	entries, err := h.manager.ListRecords(ctx, communityDID)
	require.NoError(t, err)
	for _, entry := range entries {
		assert.NotEqual(t, materialize.CollectionRemoval, entry.Collection,
			"no removal record may exist for the legacy era (rkey %s)", entry.Rkey)
	}
}

// TestNonAuthorDeleteWithoutSummaryKeepsAuthorRecord (F3): a Delete carrying
// no summary is the SELF-delete shape, and self-delete destroys the author's
// record. That semantics may only be granted to the author.
//
// The inner Delete's actor is not covered by the HTTP signature — the
// announcing community's key is — so the inner attribution is a claim, not a
// proof. A community that announces a summary-less Delete attributed to
// someone other than the author is claiming an authority it does not have: at
// most it may withdraw the post from ITSELF (its acceptance), never delete a
// record out of a repo it does not own. Granting it the author's path lets one
// followed community destroy any bridged author's content with one activity,
// with no moderation record to show for it.
//
// The control — the same shape attributed to the actual author — is
// TestSelfDeleteRemovesPostAndAcceptance above, which still asserts full v1
// self-delete semantics.
func TestNonAuthorDeleteWithoutSummaryKeepsAuthorRecord(t *testing.T) {
	h := newHarness(t)
	post := setupModeratedPost(t, h)
	ctx := context.Background()

	// A moderator, not the author, and NO summary key.
	h.announceDeleteBy(post.group,
		"https://lemmy.world/activities/announce/delete/not-the-author",
		pageID, modActorID, nil)

	_, _, err := h.manager.GetRecord(ctx, post.authorDID, materialize.CollectionPostV2, post.rkey)
	assert.NoError(t, err,
		"a summary-less Delete from someone other than the author must NOT delete the author's "+
			"record: the inner actor is an unverified claim, and self-delete semantics belong to "+
			"the author alone")

	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	assert.False(t, mapping.IsDeleted(),
		"the mapping must stay live: tombstoning it blocks every later edit and vote for a post "+
			"whose author never asked for it to go")
}

// TestBareUndoDeleteDoesNotRestoreRemovedPost (F4): a removal is exited only
// by an explicit moderator restore, and a BARE Undo{Delete} is not one.
//
// The bare path exists so an ORIGIN can un-delete content it re-serves, and it
// is deliberately permissive: same-authority signer, existing mapping, pinned
// re-fetch. None of that says anything about a COMMUNITY's decision to remove
// the post from itself. Letting the bare path write a fresh acceptance would
// let the author's own instance overturn a moderator's removal by re-serving
// the post — the restore gate has to be the community's, not the origin's.
//
// The announced restore (which IS the moderator's decision) is asserted by
// TestModeration_RemoveRestoreAndSelfDelete at the e2e tier and by R4 here.
func TestBareUndoDeleteDoesNotRestoreRemovedPost(t *testing.T) {
	h := newHarness(t)
	post := setupModeratedPost(t, h)
	ctx := context.Background()

	reason := "spam wave"
	h.announceDeleteWithSummary(post.group,
		"https://lemmy.world/activities/announce/delete/bare-undo", pageID, &reason)
	removalBefore, removalCIDBefore, err := h.manager.GetRecord(ctx,
		post.communityDID, materialize.CollectionRemoval, post.digestRKey)
	require.NoError(t, err, "precondition: the removal landed")

	// A BARE Undo{Delete} — delivered by the post's own origin, no announcer.
	origin := h.newRemoteActor(personID, map[string]any{
		"type":              "Person",
		"id":                personID,
		"preferredUsername": "LeftLeaningFreedomFighters",
		"inbox":             personID + "/inbox",
		"published":         "2024-01-01T00:00:00.000000Z",
	})
	require.Equal(t, http.StatusAccepted, h.deliver(origin, map[string]any{
		"id":    "https://lemmy.world/activities/undo/bare-restore",
		"type":  "Undo",
		"actor": personID,
		"object": map[string]any{
			"id":     "https://lemmy.world/activities/delete/bare-restore-inner",
			"type":   "Delete",
			"actor":  personID,
			"object": pageID,
		},
	}))
	h.drain()

	_, _, err = h.manager.GetRecord(ctx, post.communityDID, materialize.CollectionAcceptance, post.digestRKey)
	assert.True(t, errors.IsNotFound(err),
		"a bare Undo{Delete} must NOT write an acceptance for a post the community removed: a "+
			"fresh acceptance IS the restore, so this would let the origin overturn moderation "+
			"by re-serving the post (err=%v)", err)

	removalAfter, removalCIDAfter, err := h.manager.GetRecord(ctx,
		post.communityDID, materialize.CollectionRemoval, post.digestRKey)
	require.NoError(t, err, "the removal must still stand")
	assert.Equal(t, removalCIDBefore, removalCIDAfter, "the removal record must not be rewritten")
	assert.Equal(t, removalBefore["createdAt"], removalAfter["createdAt"])
}
