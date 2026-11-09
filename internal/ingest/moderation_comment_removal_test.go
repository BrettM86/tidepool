package ingest

import (
	"context"
	"database/sql"
	stderrors "errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/consume"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
)

// PRD MODERATION GAPS, TASK 02 — A COMMENT REMOVAL IS A COMMUNITY RECORD.
//
// A Lemmy moderator's removal of a native comment is published into the owning
// community's repo as a social.coves.community.removal record at the digest
// rkey of the comment's at-uri, and the moderator's Undo{Delete} deletes it.
// The bridge-side object_moderation row stays the source the reply and edit
// gates read, and its removed_at is the record's createdAt.

// mtRemovalLateReply is a native reply to mtDirectReply written AFTER the
// removal is lifted: a new record, not a retry of a refused one.
var mtRemovalLateReply = nativeComment{
	did: mtAuthorDID, rkey: "3lzmtcomment07",
	root:      nativeRef{mtPostATURI, mtPostCID},
	parent:    nativeRef{"at://" + mtCommenterDID + "/social.coves.community.comment/3lzmtcomment01", "bafyreih5xbmigkq5ikyhqiqhqzbwuqjxeitgtzwyxvjhfsfvswsxmnnf6i"},
	createRev: "3lzmtrev000040", createCID: "bafyreih5xbmigkq5ikyhqiqhqzbwuqjxeitgtzwyxvjhfsfvswsxmnnf6w",
	editRev: "3lzmtrev000041", editCID: "bafyreih5xbmigkq5ikyhqiqhqzbwuqjxeitgtzwyxvjhfsfvswsxmnnf6x",
	timeUS: 1_775_000_000_000_400,
}

// The second edit of mtDirectReply, written after the removal is lifted. Its
// rev orders after mtDirectReply.editRev, so the rev gate admits it whatever
// the refused edit did to the gate.
const (
	mtDirectReplyLateEditRev = "3lzmtrev000042"
	mtDirectReplyLateEditCID = "bafyreih5xbmigkq5ikyhqiqhqzbwuqjxeitgtzwyxvjhfsfvswsxmnnf6y"
)

// TestARemovalRecordForANativeCommentSurvivesRedeliveryAndReRemoval covers B1's
// lifecycle: the record's fields, a redelivered Delete, and a re-removal with a
// new reason. The empty-summary row is how Lemmy spells "removed, no reason
// given": the record is written without a reason field.
func TestARemovalRecordForANativeCommentSurvivesRedeliveryAndReRemoval(t *testing.T) {
	cases := []struct {
		name          string
		firstSummary  string
		firstReason   *string
		secondSummary string
	}{
		{name: "reason given", firstSummary: "rule 3: no personal attacks",
			firstReason: ptr("rule 3: no personal attacks"), secondSummary: "rule 5: spam"},
		{name: "empty summary", firstSummary: "", firstReason: nil, secondSummary: "rule 5: spam"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			ctx := context.Background()
			world := newModerationWorld(t, h)
			seedNativeComment(t, world)

			commentATURI, commentAPID := mtDirectReply.atURI(), mtDirectReply.apID()
			mappingBefore, err := h.objects.GetByAPID(ctx, commentAPID)
			require.NoError(t, err, "precondition: the enqueuer mapped the federated comment")
			otherFramesBefore := framesOutside(t, h, world.communityADID)

			const firstDelete = "https://lemmy.world/activities/announce/delete/cr-first/delete"
			summary := tc.firstSummary
			h.announceDeleteAs(world.groupA,
				"https://lemmy.world/activities/announce/delete/cr-first", firstDelete,
				commentAPID, modActorID, &summary)

			firstCreatedAt, firstCID := assertNativeCommentRemovalRecord(t, h,
				world.communityADID, commentATURI, mtDirectReply.createCID, tc.firstReason)

			// The comment is untouched on Tidepool's side.
			mapping, err := h.objects.GetByAPID(ctx, commentAPID)
			require.NoError(t, err)
			assert.False(t, mapping.IsDeleted(), "the comment's mapping stays live")
			assert.Equal(t, mappingBefore.CID, mapping.CID, "at the same CID")
			assert.Equal(t, otherFramesBefore, framesOutside(t, h, world.communityADID),
				"and no repo other than the community's gets a frame")

			// --- The SAME inner Delete, redelivered under a fresh Announce id.
			h.announceDeleteAs(world.groupA,
				"https://lemmy.world/activities/announce/delete/cr-redelivered", firstDelete,
				commentAPID, modActorID, &summary)

			_, redeliveredCID, err := h.manager.GetRecord(ctx,
				world.communityADID, materialize.CollectionRemoval, testDigestRKey(commentATURI))
			assert.NoError(t, err, "the removal record still stands after the redelivery")
			assert.Equal(t, firstCID, redeliveredCID,
				"a redelivered removal rewrites nothing: the record's CID is unchanged")

			// --- A re-removal with a different reason (fresh Announce and Delete ids).
			second := tc.secondSummary
			h.announceDeleteWithSummary(world.groupA,
				"https://lemmy.world/activities/announce/delete/cr-second", commentAPID, &second)

			removal, _, err := h.manager.GetRecord(ctx,
				world.communityADID, materialize.CollectionRemoval, testDigestRKey(commentATURI))
			if assert.NoError(t, err, "the re-removal updates the record at the same rkey") {
				assert.Equal(t, tc.secondSummary, removal["reason"],
					"carrying the new reason")
				assert.Equal(t, firstCreatedAt, removal["createdAt"],
					"and the ORIGINAL createdAt: the moderators removed it once, at that time")
			}
			assert.Equal(t, otherFramesBefore, framesOutside(t, h, world.communityADID),
				"and still no frame in any other repo")
		})
	}
}

// TestAReplyOrEditBeneathARemovedNativeCommentIsNotFederated is B4: a removed
// native comment is gated the way a locked thread is. A native reply to it
// enqueues no Create{Note}, its author's edit enqueues no Update{Note}, and
// once the community lifts the removal a NEW reply and a NEW edit go out.
func TestAReplyOrEditBeneathARemovedNativeCommentIsNotFederated(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)
	seedNestedThread(t, world)

	reason := "rule 3: no personal attacks"
	h.announceDeleteWithSummary(world.groupA,
		"https://lemmy.world/activities/announce/delete/cr-gate", mtDirectReply.apID(), &reason)
	state, found := removalStateFor(t, h.db, mtDirectReply.atURI())
	require.True(t, found && state.removed, "precondition: the comment is removed")

	activitiesBefore := rowCount(t, h.db, "outbound_activities")
	deliveriesBefore := rowCount(t, h.db, "outbound_deliveries")

	// --- A native reply to the removed comment.
	err := world.dispatcher.HandleEvent(ctx, mtLateNestedReply.create(t))
	require.Error(t, err, "a reply to a removed comment is refused")
	assert.True(t, stderrors.Is(err, consume.ErrPermanentEvent),
		"PERMANENTLY, so the event dead-letters and the cursor moves on (err=%v)", err)
	assert.Contains(t, err.Error(), "parent-removed:",
		"with the reason in the message the dead letter stores")
	assert.Zero(t, outboundRowsFor(t, h.db, mtLateNestedReply.atURI()),
		"a reply to a removed comment is not federated: no outbound state is written for it")

	// --- The removed comment's author edits it.
	err = world.dispatcher.HandleEvent(ctx, mtDirectReply.edit(t))
	require.Error(t, err, "an edit of a removed comment is refused")
	assert.True(t, stderrors.Is(err, consume.ErrPermanentEvent),
		"PERMANENTLY, so the event dead-letters and the cursor moves on (err=%v)", err)
	assert.Contains(t, err.Error(), "removed:",
		"with the reason in the message the dead letter stores")
	assert.Equal(t, mtDirectReply.createCID, outboundCIDFor(t, h.db, mtDirectReply.atURI()),
		"an edit of a removed comment is not federated: the outbound state still names the "+
			"version that went out before the removal")

	assert.Equal(t, activitiesBefore, rowCount(t, h.db, "outbound_activities"),
		"neither the reply nor the edit enqueues an activity")
	assert.Equal(t, deliveriesBefore, rowCount(t, h.db, "outbound_deliveries"),
		"...and nothing is delivered")

	// --- The community lifts the removal.
	h.announceUndoDelete(world.groupA,
		"https://lemmy.world/activities/announce/undo/cr-gate",
		"https://lemmy.world/activities/announce/delete/cr-gate/delete",
		mtDirectReply.apID(), &reason)
	after, found := removalStateFor(t, h.db, mtDirectReply.atURI())
	require.False(t, found && after.removed, "precondition: the removal is lifted")

	require.NoError(t, world.dispatcher.HandleEvent(ctx, mtRemovalLateReply.create(t)),
		"a new reply to the restored comment is admitted")
	assert.Equal(t, 1, outboundRowsFor(t, h.db, mtRemovalLateReply.atURI()),
		"and federated")

	require.NoError(t, world.dispatcher.HandleEvent(ctx,
		mtDirectReply.commit(t, "update", mtDirectReplyLateEditRev, mtDirectReplyLateEditCID,
			mtDirectReply.timeUS+5)),
		"a new edit of the restored comment is admitted")
	assert.Equal(t, mtDirectReplyLateEditCID, outboundCIDFor(t, h.db, mtDirectReply.atURI()),
		"and federated: the outbound state names the new version")

	assert.Greater(t, rowCount(t, h.db, "outbound_activities"), activitiesBefore,
		"the new reply and edit enqueue activities")
	assert.Greater(t, rowCount(t, h.db, "outbound_deliveries"), deliveriesBefore,
		"and deliveries")
}

// rejectCommunityCommitsTrigger makes every commit to one community repo fail,
// by refusing its firehose_events insert inside the commit transaction.
const rejectCommunityCommitsTrigger = "test_reject_comment_removal_commits"

// rejectCommunityCommits makes every commit to the repo of did fail until the
// returned func is called (also registered with t.Cleanup).
func rejectCommunityCommits(t *testing.T, h *harness, did string) (restore func()) {
	t.Helper()
	dropTrigger := func() {
		_, err := h.db.Exec(`DROP TRIGGER IF EXISTS ` + rejectCommunityCommitsTrigger + ` ON firehose_events`)
		require.NoError(t, err)
		_, err = h.db.Exec(`DROP FUNCTION IF EXISTS ` + rejectCommunityCommitsTrigger + `()`)
		require.NoError(t, err)
	}
	dropTrigger()
	t.Cleanup(dropTrigger)
	_, err := h.db.Exec(`CREATE FUNCTION ` + rejectCommunityCommitsTrigger + `() RETURNS trigger
		LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'community commit rejected by test'; END $$`)
	require.NoError(t, err)
	_, err = h.db.Exec(`CREATE TRIGGER ` + rejectCommunityCommitsTrigger + `
		BEFORE INSERT ON firehose_events FOR EACH ROW
		WHEN (NEW.did = '` + strings.ReplaceAll(did, "'", "''") + `')
		EXECUTE FUNCTION ` + rejectCommunityCommitsTrigger + `()`)
	require.NoError(t, err)
	return dropTrigger
}

// TestAFailedRemovalRecordDeleteOnUndoIsRetried is B8: the Undo clears the
// object_moderation removal, then the community-repo delete of the removal
// record fails. The event must stay retryable, and the retry of the SAME event
// must finish the job even though there is no removal left to clear.
func TestAFailedRemovalRecordDeleteOnUndoIsRetried(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)
	seedNativeComment(t, world)
	commentATURI := mtDirectReply.atURI()

	reason := "rule 3: no personal attacks"
	h.announceDeleteWithSummary(world.groupA,
		"https://lemmy.world/activities/announce/delete/cr-retry", mtDirectReply.apID(), &reason)
	_, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionRemoval, testDigestRKey(commentATURI))
	assert.NoError(t, err, "the removal wrote a removal record into the community repo")

	dropTrigger := rejectCommunityCommits(t, h, world.communityADID)

	const undoActivity = "https://lemmy.world/activities/announce/undo/cr-retry"
	h.announceUndoDelete(world.groupA, undoActivity,
		"https://lemmy.world/activities/announce/delete/cr-retry/delete",
		mtDirectReply.apID(), &reason)

	event, err := h.events.GetEvent(ctx, undoActivity)
	require.NoError(t, err)
	assert.Nil(t, event.ProcessedAt,
		"a failed record delete is not a decision: the Undo must stay retryable")
	assert.Nil(t, event.FailedAt, "and it is not poisoned either")
	state, found := removalStateFor(t, h.db, commentATURI)
	assert.False(t, found && state.removed,
		"the object_moderation removal was cleared before the record delete failed")
	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionRemoval, testDigestRKey(commentATURI))
	assert.NoError(t, err, "and the removal record is still there, because its delete failed")

	// --- The repo works again; the SAME event is made due by the database clock.
	dropTrigger()
	_, err = h.db.ExecContext(ctx,
		`UPDATE inbox_events SET next_attempt_at = CURRENT_TIMESTAMP WHERE activity_id = $1`,
		undoActivity)
	require.NoError(t, err)
	h.drain()

	event, err = h.events.GetEvent(ctx, undoActivity)
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt, "the retry completes the Undo: %s", event.Error)
	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionRemoval, testDigestRKey(commentATURI))
	assert.True(t, errors.IsNotFound(err),
		"the retry deletes the removal record left behind (err=%v)", err)
	state, found = removalStateFor(t, h.db, commentATURI)
	assert.False(t, found && state.removed, "and the object_moderation removal stays cleared")
}

// assertNativeCommentRemovalRecord checks the removal record for one comment in
// one community repo and returns its createdAt and record CID. wantReason nil
// means the record must carry no reason field.
func assertNativeCommentRemovalRecord(t *testing.T, h *harness, communityDID, commentATURI,
	wantCID string, wantReason *string) (createdAt, recordCID string) {
	t.Helper()
	removal, recordCID, err := h.manager.GetRecord(context.Background(),
		communityDID, materialize.CollectionRemoval, testDigestRKey(commentATURI))
	if !assert.NoError(t, err,
		"a removal record must exist in the community repo at the digest rkey of the comment's at-uri") {
		return "", ""
	}
	assert.Equal(t, materialize.CollectionRemoval, removal["$type"])
	subject, ok := removal["subject"].(map[string]any)
	if assert.True(t, ok, "removal.subject must be a strongRef, got %#v", removal["subject"]) {
		assert.Equal(t, commentATURI, subject["uri"], "the subject names the comment")
		assert.Equal(t, wantCID, strongRefCID(subject["cid"]), "at the comment's CID")
	}
	assert.Equal(t, "moderator-discretion", removal["code"])
	if wantReason == nil {
		assert.NotContains(t, removal, "reason", "no reason given: the field is omitted")
	} else {
		assert.Equal(t, *wantReason, removal["reason"], "the moderator's reason")
	}

	createdAt, _ = removal["createdAt"].(string)
	parsed, err := time.Parse(time.RFC3339Nano, createdAt)
	if assert.NoError(t, err, "createdAt must be an RFC3339 datetime, got %q", createdAt) {
		removedAt := removedAtFor(t, h.db, commentATURI)
		assert.True(t,
			parsed.Equal(removedAt.Truncate(time.Millisecond)) ||
				parsed.Equal(removedAt.Truncate(time.Microsecond)),
			"createdAt %s must be object_moderation.removed_at %s", createdAt, removedAt.UTC())
	}
	return createdAt, recordCID
}

// removedAtFor reads object_moderation.removed_at for one object.
func removedAtFor(t *testing.T, db *sql.DB, atURI string) time.Time {
	t.Helper()
	var removedAt sql.NullTime
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT removed_at FROM object_moderation WHERE at_uri = $1`, atURI).Scan(&removedAt),
		"read removed_at for %s", atURI)
	require.True(t, removedAt.Valid, "removed_at is set for %s", atURI)
	return removedAt.Time
}

// framesOutside counts firehose events in every repo except one.
func framesOutside(t *testing.T, h *harness, did string) int {
	t.Helper()
	events, err := h.manager.ListEvents(context.Background(), 0, 1000)
	require.NoError(t, err)
	n := 0
	for _, event := range events {
		if event.DID != did {
			n++
		}
	}
	return n
}

func ptr(s string) *string { return &s }
