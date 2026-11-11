package ingest

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/materialize"
)

// COMMUNITY REMOVAL WITHHOLDS A BRIDGED COMMENT'S BLOBS, AND A COMMUNITY BAN
// WITH "REMOVE CONTENT" WITHDRAWS COMMENTS INSTEAD OF DELETING THEM.
//
// Bridged comments carry no embed of their own today (the comment builder
// writes none), so every comment fixture here seeds an images embed on the
// stored comment record through the repo manager after the comment was
// materialized, and checks the record references the seeded image before
// acting.

// imageComment bridges U's comment replying to parentID in community, then
// seeds an images embed on its stored record holding one image of bytes unique
// to seed. It returns the comment's ap id and the seeded image's CID (read from
// the record after the seeding).
func (w *upstreamBanWorld) imageComment(community *remoteActor, slug, parentID, published, seed string) (string, string) {
	w.h.t.Helper()
	return w.imageCommentBy(ubUserID, community, slug, parentID, published, seed)
}

// imageCommentBy is imageComment for a comment by author.
func (w *upstreamBanWorld) imageCommentBy(author string, community *remoteActor, slug, parentID, published, seed string) (string, string) {
	t := w.h.t
	t.Helper()
	ctx := context.Background()
	commentID := w.commentBy(author, community, slug, parentID, published)
	require.Empty(t, recordBlobCIDs(t, w.h, commentID), "precondition: a bridged comment carries no embed of its own")

	mapping, err := w.h.objects.GetByAPID(ctx, commentID)
	require.NoError(t, err)
	require.Equal(t, materialize.CollectionComment, mapping.Collection, "precondition: %s is a comment", commentID)
	record, _, err := w.h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	require.NoError(t, err)
	blob, err := w.h.manager.PutBlob(ctx, mapping.DID, "image/png", rwImageBytes(seed))
	require.NoError(t, err)
	record["embed"] = map[string]any{
		"$type":  "social.coves.embed.images",
		"images": []any{map[string]any{"image": *blob}},
	}
	commit, err := w.h.manager.PutRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey, record)
	require.NoError(t, err)
	mapping.CID = commit.RecordCID
	_, err = w.h.objects.PutMapping(ctx, *mapping)
	require.NoError(t, err)

	cids := recordBlobCIDs(t, w.h, commentID)
	require.Equal(t, []string{cid.Cid(blob.Ref).String()}, cids,
		"precondition: the comment record references exactly the seeded image")
	assertBlobServed(t, w.h, mapping.DID, cids[0], "precondition: the seeded image is served before any removal")
	return commentID, cids[0]
}

// TestACommunityRemovalWithholdsABridgedCommentsImageUntilItsRestore (B7).
//
// GIVEN U's bridged comment in C with a seeded image; WHEN C removes it
//
//	(a) with an announced Delete carrying a summary, then announces its Undo;
//	(b) with a summary-less announced Delete attributed to a moderator (not the
//	    author), then announces its Undo;
//	(c) as in (a), but the Undo's re-fetch of the comment answers 410 Gone;
//
// THEN while removed the image's bytes stay stored and getBlob refuses it (one
// active community-removal row naming the comment); after the restore in (a)
// and (b) getBlob serves it; in (c) the removal stands and it stays refused.
func TestACommunityRemovalWithholdsABridgedCommentsImageUntilItsRestore(t *testing.T) {
	for _, tc := range []struct {
		name    string
		slug    string
		summary *string
		gone    bool
	}{
		{name: "summary-bearing Delete and its Undo", slug: "rw-b7a", summary: ptr(rwSummary)},
		{name: "summary-less Delete by a moderator and its Undo", slug: "rw-b7b"},
		{name: "Undo whose re-fetch answers 410", slug: "rw-b7c", summary: ptr(rwSummary), gone: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			world := newUpstreamBanWorld(t)
			h := world.h

			rootID := world.postBy(ubOtherUserID, world.communityC, tc.slug+"-root", ubBeforeBan, false)
			commentID, imageCID := world.imageComment(world.communityC, tc.slug, rootID, ubBeforeBan2, tc.slug+"-image")
			subject := atURIOf(t, h, commentID)

			deleteID := "https://lemmy.world/activities/delete/" + tc.slug
			h.announceDeleteAs(world.communityC, world.communityC.id+"/activities/announce/delete/"+tc.slug,
				deleteID, commentID, modActorID, tc.summary)
			require.Equal(t, materialize.RemovalCodeModeratorDiscretion,
				removalCode(t, h, ubCommunityCDID, commentID), "precondition: C's removal record for the comment stands")
			assertLive(t, h, commentID, "a moderator removal leaves the author's comment")

			assertBlobStored(t, h, ubUserDID, imageCID, "a removal keeps the bytes")
			assertBlobRefused(t, h, ubUserDID, imageCID, "the image of a community-removed comment")
			assertOneRemovalWithhold(t, h, imageCID, subject, ubUserDID)

			if tc.gone {
				h.serveGone("/comment/" + tc.slug)
			}
			h.announceUndoDelete(world.communityC, world.communityC.id+"/activities/announce/undo/"+tc.slug,
				deleteID, commentID, tc.summary)

			if tc.gone {
				require.Equal(t, materialize.RemovalCodeModeratorDiscretion,
					removalCode(t, h, ubCommunityCDID, commentID),
					"precondition: an origin still serving the comment as gone leaves the removal standing")
				assertBlobStored(t, h, ubUserDID, imageCID, "a standing removal keeps the bytes")
				assertBlobRefused(t, h, ubUserDID, imageCID, "the removal still stands")
				assertOneRemovalWithhold(t, h, imageCID, subject, ubUserDID)
				return
			}
			require.Empty(t, removalCode(t, h, ubCommunityCDID, commentID),
				"precondition: C's restore deleted the comment's removal record")
			assert.Empty(t, activeRemovalWithholds(t, h, imageCID), "the restore lifts the comment's community-removal row")
			assertBlobServed(t, h, ubUserDID, imageCID, "after the restore")
		})
	}
}

// TestAnEditOfARemovedBridgedCommentKeepsTheImageItDroppedRefused (B7e).
//
// GIVEN U's bridged comment in C with a seeded image, removed by C (the image
// withheld); WHEN U's origin edits the comment (announced Update) and the
// rebuilt record carries no embed THEN the dropped image keeps its active
// community-removal row naming the comment and stays refused (its bytes are
// still stored, and only a restore lifts), and the removal record still
// stands.
func TestAnEditOfARemovedBridgedCommentKeepsTheImageItDroppedRefused(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h

	const slug = "rw-b7e"
	rootID := world.postBy(ubOtherUserID, world.communityC, slug+"-root", ubBeforeBan, false)
	commentID, imageCID := world.imageComment(world.communityC, slug, rootID, ubBeforeBan2, slug+"-image")
	subject := atURIOf(t, h, commentID)

	world.communityRemoves(world.communityC, slug, commentID)
	require.Equal(t, materialize.RemovalCodeModeratorDiscretion, removalCode(t, h, ubCommunityCDID, commentID),
		"precondition: C's removal record for the comment stands")
	assertBlobStored(t, h, ubUserDID, imageCID, "a removal keeps the bytes")
	assertOneRemovalWithhold(t, h, imageCID, subject, ubUserDID)

	edited := map[string]any{
		"type":         "Note",
		"id":           commentID,
		"attributedTo": ubUserID,
		"to":           []any{"https://www.w3.org/ns/activitystreams#Public"},
		"cc":           []any{world.communityC.id},
		"audience":     world.communityC.id,
		"content":      "<p>an edit written while the comment was removed</p>",
		"source":       map[string]any{"content": "an edit written while the comment was removed", "mediaType": "text/markdown"},
		"published":    ubBeforeBan2,
		"updated":      "2026-08-20T12:00:00Z",
		"inReplyTo":    rootID,
	}
	h.serveObject("/comment/"+slug, edited)
	require.Equal(t, http.StatusAccepted, h.deliver(world.communityC, map[string]any{
		"id":       world.communityC.id + "/activities/announce/update/" + slug,
		"type":     "Announce",
		"actor":    world.communityC.id,
		"audience": world.communityC.id,
		"object": map[string]any{
			"id":       originOf(ubUserID) + "/activities/update/" + slug,
			"type":     "Update",
			"actor":    ubUserID,
			"audience": world.communityC.id,
			"object":   edited,
		},
	}))
	h.drain()

	record := liveRecord(t, h, commentID)
	require.Equal(t, "an edit written while the comment was removed", record["content"],
		"precondition: the edit re-materialized the removed comment")
	require.Empty(t, recordBlobCIDs(t, h, commentID), "precondition: the rebuilt comment record carries no embed")

	assertBlobStored(t, h, ubUserDID, imageCID, "an edit keeps the dropped image's bytes")
	assertOneRemovalWithhold(t, h, imageCID, subject, ubUserDID)
	assertBlobRefused(t, h, ubUserDID, imageCID, "the image a still-removed comment dropped")
	assert.Equal(t, materialize.RemovalCodeModeratorDiscretion, removalCode(t, h, ubCommunityCDID, commentID),
		"the edit leaves C's removal record standing")
}

// communityBanFixture is U's content in C before a community ban: a postv2
// with an image and two comments with seeded images, plus another user's post
// the first comment replies to.
type communityBanFixture struct {
	postID     string
	postCIDs   []string
	comments   []string
	commentCID map[string]string
}

func (w *upstreamBanWorld) seedCommunityBanFixture(prefix string) communityBanFixture {
	w.h.t.Helper()
	rootID := w.postBy(ubOtherUserID, w.communityC, prefix+"-root", ubBeforeBan, false)
	postID := w.imagePost(w.communityC, prefix+"-post", prefix+"-post-image")
	commentOne, cidOne := w.imageComment(w.communityC, prefix+"-comment-one", rootID, ubBeforeBan2, prefix+"-comment-one-image")
	commentTwo, cidTwo := w.imageComment(w.communityC, prefix+"-comment-two", postID, ubBeforeBan3, prefix+"-comment-two-image")
	fixture := communityBanFixture{
		postID:     postID,
		postCIDs:   recordBlobCIDs(w.h.t, w.h, postID),
		comments:   []string{commentOne, commentTwo},
		commentCID: map[string]string{commentOne: cidOne, commentTwo: cidTwo},
	}
	require.NotContains(w.h.t, fixture.postCIDs, cidOne, "precondition: distinct images")
	require.NotContains(w.h.t, fixture.postCIDs, cidTwo, "precondition: distinct images")
	require.NotEqual(w.h.t, cidOne, cidTwo, "precondition: distinct images")
	require.True(w.h.t, acceptanceExists(w.h.t, w.h, ubCommunityCDID, postID), "precondition: the post is accepted in C")
	return fixture
}

// assertCommunityBanWithheld is the outcome of C's ban of U with removeData
// over the fixture: each comment record stays in U's repo with an
// author-banned removal record carrying the ban's reason, the postv2 is
// withdrawn with an author-banned removal, every image's bytes are still
// stored, and getBlob refuses each image under its subject's
// community-removal row.
func assertCommunityBanWithheld(t *testing.T, h *harness, fixture communityBanFixture, banReason string) {
	t.Helper()
	for _, commentID := range fixture.comments {
		assertLive(t, h, commentID, "a community ban leaves the comment record in its author's repo")
		removal, _, err := h.manager.GetRecord(context.Background(),
			ubCommunityCDID, materialize.CollectionRemoval, testDigestRKey(atURIOf(t, h, commentID)))
		if assert.NoError(t, err, "C writes a removal record for %s", commentID) {
			assert.Equal(t, materialize.RemovalCodeAuthorBanned, removal["code"], "%s: its author was banned", commentID)
			assert.Equal(t, banReason, removal["reason"], "%s: the ban's reason", commentID)
		}
		imageCID := fixture.commentCID[commentID]
		assertBlobStored(t, h, ubUserDID, imageCID, "a community ban keeps the comment image's bytes")
		assertBlobRefused(t, h, ubUserDID, imageCID, "the image of a comment withdrawn by a community ban")
		assertOneRemovalWithhold(t, h, imageCID, atURIOf(t, h, commentID), ubUserDID)
	}

	assert.False(t, acceptanceExists(t, h, ubCommunityCDID, fixture.postID), "U's postv2 is withdrawn from C")
	assert.Equal(t, materialize.RemovalCodeAuthorBanned, removalCode(t, h, ubCommunityCDID, fixture.postID),
		"C records why the post went: its author was banned")
	assertLive(t, h, fixture.postID, "a community ban leaves U's postv2 record")
	for _, c := range fixture.postCIDs {
		assertBlobStored(t, h, ubUserDID, c, "a community ban keeps the post image's bytes")
		assertBlobRefused(t, h, ubUserDID, c, "the image of a post withdrawn by a community ban")
		assertOneRemovalWithhold(t, h, c, atURIOf(t, h, fixture.postID), ubUserDID)
	}
}

// TestACommunityBanWithdrawsTheBannedUsersCommentsAndWithholdsTheirImages (B8).
//
// GIVEN U's postv2 with an image and two comments with seeded images in C,
// all before the ban; WHEN C announces a Block of U with removeData THEN each
// comment record stays, with an author-banned removal record carrying the
// ban's reason; the postv2 is withdrawn as before; every image's bytes are
// still stored; and getBlob refuses each image.
func TestACommunityBanWithdrawsTheBannedUsersCommentsAndWithholdsTheirImages(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	fixture := world.seedCommunityBanFixture("rw-b8")

	const banID = "https://lemmy.world/activities/announce/block/rw-b8-ban"
	world.announceCommunityBlock(world.communityC, banID, ubUserID, nil, ubBanTime)
	assertProcessed(t, h, banID)

	assertCommunityBanWithheld(t, h, fixture, "Spam")
}

// TestABanReasonLongerThanTheRemovalLimitStillWithdrawsTheContent.
//
// GIVEN the B8 fixture; WHEN C announces a Block of U with removeData whose
// summary is 1500 graphemes (within the ban record's 2000, past the removal
// record's 1000) THEN, under strict validation, the event is processed: the
// ban record carries the whole reason, and the post and each comment get an
// author-banned removal record whose reason is the summary cut to 1000
// graphemes (ending in an ellipsis), with every image refused.
func TestABanReasonLongerThanTheRemovalLimitStillWithdrawsTheContent(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	fixture := world.seedCommunityBanFixture("rw-long-reason")
	longReason := strings.Repeat("x", 1500)
	// truncateText ends a cut with an ellipsis, 1000 graphemes in all.
	removalReason := strings.Repeat("x", 999) + "…"

	const banID = "https://lemmy.world/activities/announce/block/rw-long-reason-ban"
	world.announceCommunityBlock(world.communityC, banID, ubUserID, map[string]any{"summary": longReason}, ubBanTime)
	assertProcessed(t, h, banID)

	ban, _, ok := banRecord(t, h, ubCommunityCDID, ubUserDID)
	if assert.True(t, ok, "C publishes U's ban record") {
		assert.Equal(t, longReason, ban["reason"], "the ban record keeps the whole reason")
	}
	assertCommunityBanWithheld(t, h, fixture, removalReason)
	postRemoval, _, err := h.manager.GetRecord(context.Background(),
		ubCommunityCDID, materialize.CollectionRemoval, testDigestRKey(atURIOf(t, h, fixture.postID)))
	if assert.NoError(t, err, "C writes a removal record for the post") {
		assert.Equal(t, removalReason, postRemoval["reason"], "the post removal's reason is cut to the removal limit")
	}
}

// TestReplayingACommunityBanWithdrawsCommentsAndADryRunWritesNothing (B9).
//
// GIVEN the B8 fixture and C's ban of U recorded as an already-processed inbox
// event; WHEN the operator replays bans as a dry run THEN the report lists the
// post and both comments and nothing is written (no removal records, no
// withhold rows, records intact); WHEN replayed for real THEN the outcome is
// B8's; WHEN replayed again THEN nothing is listed.
func TestReplayingACommunityBanWithdrawsCommentsAndADryRunWritesNothing(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	fixture := world.seedCommunityBanFixture("rw-b9")

	const banID = "https://lemmy.world/activities/announce/block/rw-b9-ban"
	enqueueProcessed(t, h, world.communityC, communityBlockAnnounce(world.communityC, banID, ubUserID, nil), ubBanTime)

	listing := func(dryRun bool) string {
		return mustJSON(t, map[string]any{
			"dry_run": dryRun,
			"bans": []any{
				map[string]any{
					"activity_id": banID,
					"subject":     ubUserID,
					"scope":       "community",
					"community":   world.communityC.id,
					"removed": sortedStrings(atURIOf(t, h, fixture.postID),
						atURIOf(t, h, fixture.comments[0]), atURIOf(t, h, fixture.comments[1])),
				},
			},
			"removed_total": 3,
		})
	}

	before := replayStateSnapshot(t, h)
	dryRun := h.adminRequest(http.MethodPost, replayBansPath, map[string]any{"dry_run": true})
	require.Equal(t, http.StatusOK, dryRun.Code, dryRun.Body.String())
	assert.JSONEq(t, listing(true), dryRun.Body.String(), "the dry run lists the comments it would withdraw")
	after := replayStateSnapshot(t, h)
	for table, rows := range before {
		assert.Equal(t, rows, after[table], "a dry run writes nothing: %s changed", table)
	}
	for _, commentID := range fixture.comments {
		assert.Empty(t, removalCode(t, h, ubCommunityCDID, commentID), "a dry run writes no removal record")
		assertLive(t, h, commentID, "a dry run deletes nothing")
	}

	realRun := h.adminRequest(http.MethodPost, replayBansPath, map[string]any{"dry_run": false})
	require.Equal(t, http.StatusOK, realRun.Code, realRun.Body.String())
	assert.JSONEq(t, listing(false), realRun.Body.String(), "the real run does what the dry run listed")
	assertCommunityBanWithheld(t, h, fixture, "Spam")

	again := h.adminRequest(http.MethodPost, replayBansPath, map[string]any{"dry_run": false})
	require.Equal(t, http.StatusOK, again.Code, again.Body.String())
	assert.JSONEq(t, `{"dry_run": false, "bans": [], "removed_total": 0}`, again.Body.String(),
		"a withdrawn comment's standing removal record leaves the ban nothing to redo")
}

// rejectRemovalWithholdsTrigger fails every blob_withholds insert owned by U,
// so a removal whose withhold write fails can be observed. Test-unique name.
const rejectRemovalWithholdsTrigger = "rw_b12_reject_withholds"

func installRejectWithholdsTrigger(t *testing.T, h *harness) (drop func()) {
	t.Helper()
	drop = func() {
		_, err := h.db.Exec(`DROP TRIGGER IF EXISTS ` + rejectRemovalWithholdsTrigger + ` ON blob_withholds`)
		require.NoError(t, err)
		_, err = h.db.Exec(`DROP FUNCTION IF EXISTS ` + rejectRemovalWithholdsTrigger + `()`)
		require.NoError(t, err)
	}
	drop()
	t.Cleanup(drop)
	_, err := h.db.Exec(`CREATE FUNCTION ` + rejectRemovalWithholdsTrigger + `() RETURNS trigger
		LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'blob withhold insert rejected by test'; END $$`)
	require.NoError(t, err)
	_, err = h.db.Exec(fmt.Sprintf(`CREATE TRIGGER %s
		BEFORE INSERT ON blob_withholds FOR EACH ROW
		WHEN (NEW.owner_did = '%s')
		EXECUTE FUNCTION %s()`, rejectRemovalWithholdsTrigger, ubUserDID, rejectRemovalWithholdsTrigger))
	require.NoError(t, err)
	return drop
}

// assertNoRemovalRecord: communityDID holds no removal record for apID.
func assertNoRemovalRecord(t *testing.T, h *harness, communityDID, apID, why string) {
	t.Helper()
	_, _, err := h.manager.GetRecord(context.Background(),
		communityDID, materialize.CollectionRemoval, testDigestRKey(atURIOf(t, h, apID)))
	assert.True(t, errors.IsNotFound(err), "%s: no removal record may stand (%s); err=%v", apID, why, err)
}

// TestARemovalWhoseWithholdWriteFailsIsNotRecordedAndARetryConverges (B12).
//
// GIVEN blob_withholds inserts for U failing:
//
//	(a) WHEN C's moderator removes U's image postv2 THEN the activity fails and
//	    no removal record is written;
//	(b) WHEN C bans U with removeData over U's image postv2 and a comment with
//	    a seeded image THEN no removal record is written for either and the
//	    comment record stays;
//
// WHEN the fault is cleared and the failed activity is retried THEN the removal
// records and the withhold rows are both present.
func TestARemovalWhoseWithholdWriteFailsIsNotRecordedAndARetryConverges(t *testing.T) {
	for _, tc := range []struct {
		name string
		slug string
		// act delivers the removal under a fresh id suffix and returns that
		// activity's id.
		act func(world *upstreamBanWorld, fixture communityBanFixture, suffix string) string
		// wantCode is the removal code the converged removal carries.
		wantCode string
	}{
		{
			name: "moderator removal of a bridged post",
			slug: "rw-b12a",
			act: func(world *upstreamBanWorld, fixture communityBanFixture, suffix string) string {
				world.communityRemoves(world.communityC, "rw-b12a"+suffix, fixture.postID)
				return world.communityC.id + "/activities/announce/delete/rw-b12a" + suffix
			},
			wantCode: materialize.RemovalCodeModeratorDiscretion,
		},
		{
			name: "community ban with removeData over a bridged comment",
			slug: "rw-b12b",
			act: func(world *upstreamBanWorld, fixture communityBanFixture, suffix string) string {
				banID := "https://lemmy.world/activities/announce/block/rw-b12b" + suffix
				world.announceCommunityBlock(world.communityC, banID, ubUserID, nil, ubBanTime)
				return banID
			},
			wantCode: materialize.RemovalCodeAuthorBanned,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			world := newUpstreamBanWorld(t)
			h := world.h
			rootID := world.postBy(ubOtherUserID, world.communityC, tc.slug+"-root", ubBeforeBan, false)
			postID := world.imagePost(world.communityC, tc.slug+"-post", tc.slug+"-post-image")
			commentID, commentCID := world.imageComment(world.communityC, tc.slug+"-comment", rootID, ubBeforeBan2, tc.slug+"-comment-image")
			fixture := communityBanFixture{
				postID:     postID,
				postCIDs:   recordBlobCIDs(t, h, postID),
				comments:   []string{commentID},
				commentCID: map[string]string{commentID: commentCID},
			}
			// subjects are what this row's removal acts on.
			subjects := []string{postID}
			if tc.wantCode == materialize.RemovalCodeAuthorBanned {
				subjects = append(subjects, commentID)
			}

			drop := installRejectWithholdsTrigger(t, h)
			failedID := tc.act(world, fixture, "")
			for _, subject := range subjects {
				assertNoRemovalRecord(t, h, ubCommunityCDID, subject, "its withhold write failed")
			}
			assertLive(t, h, commentID, "a failed removal destroys nothing")
			event, err := h.events.GetEvent(context.Background(), failedID)
			require.NoError(t, err)
			assert.Nil(t, event.ProcessedAt, "the activity whose withhold write failed is not processed")
			for _, c := range append(append([]string{}, fixture.postCIDs...), commentCID) {
				assertBlobStored(t, h, ubUserDID, c, "a failed removal keeps the bytes")
			}

			drop()
			// The failed activity backs off and, as its community's oldest
			// pending event, holds every later one; bring its retry due now.
			_, err = h.db.ExecContext(context.Background(),
				`UPDATE inbox_events SET next_attempt_at = now() WHERE activity_id = $1`, failedID)
			require.NoError(t, err)
			h.drain()
			event, err = h.events.GetEvent(context.Background(), failedID)
			require.NoError(t, err)
			assert.NotNil(t, event.ProcessedAt, "the retried activity is processed")
			assert.Nil(t, event.FailedAt, "the activity whose withhold write failed was retried, not poisoned")

			for _, subject := range subjects {
				assert.Equal(t, tc.wantCode, removalCode(t, h, ubCommunityCDID, subject),
					"%s: the retried removal is recorded", subject)
				assertLive(t, h, subject, "a community removal keeps the author's record")
			}
			for _, c := range fixture.postCIDs {
				assertOneRemovalWithhold(t, h, c, atURIOf(t, h, postID), ubUserDID)
				assertBlobRefused(t, h, ubUserDID, c, "the converged removal withholds the post's image")
			}
			if tc.wantCode == materialize.RemovalCodeAuthorBanned {
				assertOneRemovalWithhold(t, h, commentCID, atURIOf(t, h, commentID), ubUserDID)
				assertBlobRefused(t, h, ubUserDID, commentCID, "the converged ban withholds the comment's image")
			}
		})
	}
}

// rejectWithholdLiftsTrigger fails every blob_withholds update of a row owned
// by U, so a restore whose withhold lift fails can be observed. Test-unique
// name.
const rejectWithholdLiftsTrigger = "rw_restore_reject_lifts"

func installRejectWithholdLiftsTrigger(t *testing.T, h *harness) (drop func()) {
	t.Helper()
	drop = func() {
		_, err := h.db.Exec(`DROP TRIGGER IF EXISTS ` + rejectWithholdLiftsTrigger + ` ON blob_withholds`)
		require.NoError(t, err)
		_, err = h.db.Exec(`DROP FUNCTION IF EXISTS ` + rejectWithholdLiftsTrigger + `()`)
		require.NoError(t, err)
	}
	drop()
	t.Cleanup(drop)
	_, err := h.db.Exec(`CREATE FUNCTION ` + rejectWithholdLiftsTrigger + `() RETURNS trigger
		LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'blob withhold update rejected by test'; END $$`)
	require.NoError(t, err)
	_, err = h.db.Exec(fmt.Sprintf(`CREATE TRIGGER %s
		BEFORE UPDATE ON blob_withholds FOR EACH ROW
		WHEN (OLD.owner_did = '%s')
		EXECUTE FUNCTION %s()`, rejectWithholdLiftsTrigger, ubUserDID, rejectWithholdLiftsTrigger))
	require.NoError(t, err)
	return drop
}

// TestARestoreWhoseWithholdLiftFailsIsNotRecordedAndARetryLiftsTheRows.
//
// GIVEN U's image postv2 removed by C (the image withheld) and blob_withholds
// updates of U's rows failing; WHEN C restores the post (announced Undo of the
// Delete) THEN the activity fails, the removal record still stands, no
// acceptance is written, and the image stays refused; WHEN the fault is
// cleared and the activity retried THEN the removal is gone, the post's
// community-removal rows are lifted, and getBlob serves the image.
func TestARestoreWhoseWithholdLiftFailsIsNotRecordedAndARetryLiftsTheRows(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h

	const slug = "rw-restore-lift-fault"
	postID := world.imagePost(world.communityC, slug, slug+"-image")
	imageCID := recordBlobCIDs(t, h, postID)[0]
	subject := atURIOf(t, h, postID)
	world.removeFromC(slug, postID)
	assertOneRemovalWithhold(t, h, imageCID, subject, ubUserDID)

	drop := installRejectWithholdLiftsTrigger(t, h)
	summary := rwSummary
	restoreID := world.communityC.id + "/activities/announce/undo/" + slug
	h.announceUndoDelete(world.communityC, restoreID, "https://lemmy.world/activities/delete/"+slug, postID, &summary)
	assertRetryable(t, h, restoreID, "the restore's withhold lift failed")
	assert.Equal(t, materialize.RemovalCodeModeratorDiscretion, removalCode(t, h, ubCommunityCDID, postID),
		"a restore whose lift failed leaves the removal record standing")
	assert.False(t, acceptanceExists(t, h, ubCommunityCDID, postID), "a restore whose lift failed writes no acceptance")
	assertOneRemovalWithhold(t, h, imageCID, subject, ubUserDID)
	assertBlobRefused(t, h, ubUserDID, imageCID, "the restore did not commit")

	drop()
	retryNow(t, h, restoreID)
	assertProcessed(t, h, restoreID)
	assert.Empty(t, removalCode(t, h, ubCommunityCDID, postID), "the retried restore deletes the removal record")
	assert.True(t, acceptanceExists(t, h, ubCommunityCDID, postID), "the retried restore accepts the post again")
	assert.Empty(t, activeRemovalRowsOf(t, h, subject), "the retried restore lifts the post's community-removal rows")
	assertBlobServed(t, h, ubUserDID, imageCID, "the restored post's image")
}
