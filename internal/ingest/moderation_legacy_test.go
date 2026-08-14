package ingest

import (
	"context"
	"database/sql"
	stderrors "errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/consume"
)

// TASK 17c-2 — THE MIGRATION PATH.
//
// The lock tests all build their threads through the CURRENT code, so every
// snapshot they write names its thread root and the resolution never has to
// work for it. That leaves the half of the code that exists ONLY for content
// already in production — walkThreadRoot, its fediverse boundary, its dead end,
// and the fork that weighs an unknown thread against the locks that exist —
// asserted in prose alone.
//
// It is the wrong half to leave untested. New content is written by the code
// under review and can be re-derived if it is wrong; the rows already out there
// are the ones nobody can rewrite, they are the majority on the day the change
// ships, and they are exactly the comments most likely to be sitting in an old
// thread a moderator is about to close.
//
// FIXTURE-FORGING, DELIBERATE AND NARROW: these tests build a comment through
// the real path and then REMOVE the fields the previous version did not write.
// That is not inventing a state — it is the state of every row written before
// this change, reproduced by subtraction, which is the only honest way to hold
// one. makeLegacySnapshot refuses to run if the field it strips was not there,
// so it can never quietly forge nothing.

// mtDeepReply hangs under mtNestedReply — three levels below the post, so a
// climb to the thread root has to take more than one hop.
var mtDeepReply = nativeComment{
	did: mtCommenterDID, rkey: "3lzmtcomment07",
	root:      nativeRef{mtPostATURI, mtPostCID},
	parent:    nativeRef{mtNestedReply.atURI(), mtNestedReply.createCID},
	createRev: "3lzmtrev000040", createCID: "bafyreih5xbmigkq5ikyhqiqhqzbwuqjxeitgtzwyxvjhfsfvswsxmnnf7a",
	editRev: "3lzmtrev000041", editCID: "bafyreih5xbmigkq5ikyhqiqhqzbwuqjxeitgtzwyxvjhfsfvswsxmnnf7b",
	timeUS: 1_775_000_000_000_400,
}

// TestALegacyThreadIsClimbedToItsLockedRoot is the CLIMB, and it PASSES TODAY.
//
// A reply arriving under comments written before the thread root was recorded
// has to be placed by following what those rows DO name — their parents — up to
// something that answers. Every hop is a row the bridge wrote in an earlier
// version, and the answer decides whether a moderator's lock reaches the reply.
//
// Getting this wrong is invisible in exactly one direction: the reply federates,
// Lemmy rejects it, and the poisoned delivery names a thread nobody connected to
// the lock. So the climb is pinned on a chain more than one hop long — a
// one-hop fixture would pass against an implementation that only ever looked at
// the parent.
func TestALegacyThreadIsClimbedToItsLockedRoot(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)
	seedNestedThread(t, world)

	// Both comments become PRE-CHANGE rows: neither names its thread, so the
	// only way to the post is up the parent chain, two hops.
	makeLegacySnapshot(t, h.db, mtDirectReply.atURI(), "rootAtUri")
	makeLegacySnapshot(t, h.db, mtNestedReply.atURI(), "rootAtUri")

	deliveriesBefore := rowCount(t, h.db, "outbound_deliveries")
	h.announceLock(world.groupA, mtLockActivity, mtPostAPID)

	err := world.dispatcher.HandleEvent(ctx, mtDeepReply.create(t))
	require.Error(t, err,
		"a reply under legacy rows is still a reply in the locked thread: the rows predate "+
			"the recorded root, and the lock is on the post two levels above them")
	assert.True(t, stderrors.Is(err, consume.ErrPermanentEvent),
		"refused with the same permanence as a reply under a modern row — an author must not "+
			"discover that whether their comment posts depends on when the comment above it "+
			"was written (err=%v)", err)
	assert.Contains(t, err.Error(), "parent-locked", "and with the same reason")
	assert.Zero(t, outboundRowsFor(t, h.db, mtDeepReply.atURI()))
	assert.Equal(t, deliveriesBefore, rowCount(t, h.db, "outbound_deliveries"))

	// The EDIT path climbs too, from the edited comment's own at-uri: an update
	// never re-resolves its thread, so a legacy row being edited has to be
	// placed the same way.
	editErr := world.dispatcher.HandleEvent(ctx, mtNestedReply.edit(t))
	require.Error(t, editErr,
		"editing a legacy comment inside a locked thread is refused for the same reason: an "+
			"Update{Note} is a delivery, and the community that closed the thread has stopped "+
			"accepting them")
	assert.Contains(t, editErr.Error(), "parent-locked")

	// --- And the climb resolves the other way once the thread reopens.
	h.announceUndoLock(world.groupA, mtUnlockActivity, mtLockActivity+"/lock", mtPostAPID)
	require.NoError(t, world.dispatcher.HandleEvent(ctx, mtDeepReply.create(t)),
		"with the lock lifted the same climb ends at an open thread and the reply federates")
	assert.Equal(t, 1, outboundRowsFor(t, h.db, mtDeepReply.atURI()))
}

// TestADeadEndedThreadHoldsTheReplyRatherThanGuessing pins BOTH forks of the
// undeterminable case, and it PASSES TODAY.
//
// A chain that stops on state this consumer never wrote — a nested row naming no
// parent — cannot be resolved, and "we do not know which thread this is" is not
// "this thread is not locked". But it must not become a permanent refusal
// either: the reply is fine, the state is what is missing.
//
// So the question narrows to the only one still answerable. If the community
// holds NO lock, nothing could have been missed and the reply goes. If it holds
// one, the honest answer is a retryable failure naming the row the chain stopped
// at — retryable because a lifted lock makes it answerable, and named because
// "undeterminable thread" with nothing to look up is not a report an operator
// can act on.
func TestADeadEndedThreadHoldsTheReplyRatherThanGuessing(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)
	seedNestedThread(t, world)

	// mtDirectReply names neither its thread nor its parent: the chain has
	// nowhere to go from here. mtNestedReply above it is an ordinary legacy row,
	// so a reply beneath THAT has to climb into the dead end rather than start
	// in it — the walk must be what discovers the break, not the fixture.
	makeLegacySnapshot(t, h.db, mtDirectReply.atURI(), "rootAtUri", "parentAtUri")
	makeLegacySnapshot(t, h.db, mtNestedReply.atURI(), "rootAtUri")

	// FORK ONE: no lock stands anywhere in the community, so an unresolvable
	// thread cannot be a locked one.
	require.NoError(t, world.dispatcher.HandleEvent(ctx, mtLateNestedReply.create(t)),
		"with no lock in the community there is provably nothing to miss, and holding the "+
			"reply would strand ordinary conversation under rows whose only sin is being old")
	require.Equal(t, 1, outboundRowsFor(t, h.db, mtLateNestedReply.atURI()))

	// FORK TWO: a lock now stands. The same unresolvable thread might be it.
	h.announceLock(world.groupA, mtLockActivity, mtPostAPID)
	deliveriesBefore := rowCount(t, h.db, "outbound_deliveries")

	err := world.dispatcher.HandleEvent(ctx, mtDeepReply.create(t))
	require.Error(t, err,
		"an unresolvable thread beside a standing lock must NOT be read as unlocked: that is "+
			"the fail-open the empty root exists to prevent, and it would let exactly the "+
			"oldest threads leak replies past a lock")
	assert.False(t, stderrors.Is(err, consume.ErrPermanentEvent),
		"and it must be RETRYABLE, not permanent: nothing about this comment is wrong — the "+
			"answer is missing, and it becomes knowable the moment the lock is lifted, so "+
			"dead-lettering it discards a reply that was always going to be fine (err=%v)", err)
	assert.Contains(t, err.Error(), mtDirectReply.atURI(),
		"and it must NAME the row the chain stopped at: an operator holding 'thread "+
			"undeterminable' with no object to open cannot tell a data bug from a lock")
	assert.Equal(t, deliveriesBefore, rowCount(t, h.db, "outbound_deliveries"),
		"nothing is delivered while the question is open")

	// And it resolves itself when the lock lifts — the whole reason it is held
	// rather than dropped.
	h.announceUndoLock(world.groupA, mtUnlockActivity, mtLockActivity+"/lock", mtPostAPID)
	require.NoError(t, world.dispatcher.HandleEvent(ctx, mtDeepReply.create(t)),
		"the held reply federates on retry once no lock stands")
}

// TestARematerializationCannotMoveACommentOutOfItsLockedThread PASSES TODAY and
// pins an invariant that is otherwise only claimed in a comment.
//
// The thread a comment hangs in is read back on the moderation path, and the
// delivery that re-materializes it is EDITABLE by the instance sending it. If a
// re-delivery could re-derive the thread from its own inReplyTo, then editing a
// comment out from under a lock would be one Update away — an unprivileged
// reopening of a closed thread, performed by anyone who can get an Update
// announced.
func TestARematerializationCannotMoveACommentOutOfItsLockedThread(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)
	thread := seedFediverseThread(t, h, world)

	h.announceLock(world.groupA, mtLockActivity, thread.postAPID)
	require.Error(t, world.dispatcher.HandleEvent(ctx, thread.reply.create(t)),
		"precondition: a reply under the Lemmy comment is refused while the post is locked")

	// A second, UNLOCKED post in the same community, and an Update of the Lemmy
	// comment that re-parents it there.
	otherPost := h.bridgePost(world.groupA, "77777")
	moved := note(mlLemmyComment, mlReplier, otherPost, "re-parented out of the locked thread",
		"2026-08-13T10:00:00.000000Z")
	require.Equal(t, http.StatusAccepted, h.deliver(world.groupA,
		echoAnnounce("https://lemmy.world/activities/announce/update/ml-9101", map[string]any{
			"id":       "https://lemmy.world/activities/update/ml-9101",
			"type":     "Update",
			"actor":    mlReplier,
			"audience": groupID,
			"object":   moved,
		})))
	h.drain()

	mapping, err := h.objects.GetByAPID(ctx, mlLemmyComment)
	require.NoError(t, err)
	assert.Equal(t, thread.postATURI, mapping.ThreadRootATURI,
		"the recorded thread must SURVIVE the re-materialization: a comment cannot change "+
			"threads, and a binding re-derived from an edited delivery is a binding the sender "+
			"chooses")

	err = world.dispatcher.HandleEvent(ctx, thread.reply.create(t))
	require.Error(t, err,
		"so the reply is still refused: if an Update could move the comment, reopening a "+
			"locked thread would cost one announced edit and leave no moderation trace")
	assert.Contains(t, err.Error(), "parent-locked")
}

// TestALegacyReplyUnderAFediverseCommentResolvesThroughTheMapping is the bypass
// the climb left open.
//
// walkThreadRoot stops when it reaches something with no outbound state and
// calls that the top of the thread. For a NATIVE parent that is right — the
// bridge wrote every row above it. For a FEDIVERSE parent it is wrong: a Lemmy
// comment has no outbound row by construction, and the thread above it is
// recorded on its MAPPING, in the very row the walk just read past.
//
// The shape is ordinary: a native comment written before the root was recorded,
// hanging under a Lemmy comment. Its replies resolve their thread to that Lemmy
// comment, so a lock on the post above never reaches them — and unlike the
// legacy climb, this does not heal when the parent is re-materialized, because
// the child is what is missing the root.
func TestALegacyReplyUnderAFediverseCommentResolvesThroughTheMapping(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)
	thread := seedFediverseThread(t, h, world)

	// The pre-change native comment: federated for real, then reduced to the
	// row the previous version would have written.
	require.NoError(t, world.dispatcher.HandleEvent(ctx, thread.reply.create(t)),
		"precondition: a native reply under the Lemmy comment federates while the thread is open")
	makeLegacySnapshot(t, h.db, thread.reply.atURI(), "rootAtUri")

	deliveriesBefore := rowCount(t, h.db, "outbound_deliveries")
	h.announceLock(world.groupA, mtLockActivity, thread.postAPID)

	// A reply to that legacy comment. The climb reaches the LEMMY comment, which
	// has no outbound row — and stops there unless it asks the mapping.
	child := legacyChildOf(thread.reply)
	err := world.dispatcher.HandleEvent(ctx, child.create(t))
	require.Error(t, err,
		"the thread above a fediverse comment is recorded on its mapping, and the walk reads "+
			"that very row on its way past: stopping there puts every legacy reply under a "+
			"Lemmy comment outside its own locked thread, which is most of Lemmy")
	assert.True(t, stderrors.Is(err, consume.ErrPermanentEvent),
		"refused permanently, like every other reply in a locked thread (err=%v)", err)
	assert.Contains(t, err.Error(), "parent-locked")
	assert.Zero(t, outboundRowsFor(t, h.db, child.atURI()))
	assert.Equal(t, deliveriesBefore, rowCount(t, h.db, "outbound_deliveries"))
}

// TestALegacyEditUnderAFediverseCommentResolvesThroughTheMapping is the same
// bypass on the update path, where it is worse: the comment being edited IS the
// legacy row, so the walk starts at it and dead-reckons off its fediverse parent
// every time — an edit surface that stays live inside a closed thread, for as
// long as the row is never re-created.
func TestALegacyEditUnderAFediverseCommentResolvesThroughTheMapping(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)
	thread := seedFediverseThread(t, h, world)

	require.NoError(t, world.dispatcher.HandleEvent(ctx, thread.reply.create(t)),
		"precondition: the native reply federated while the thread was open")
	makeLegacySnapshot(t, h.db, thread.reply.atURI(), "rootAtUri")

	deliveriesBefore := rowCount(t, h.db, "outbound_deliveries")
	h.announceLock(world.groupA, mtLockActivity, thread.postAPID)

	err := world.dispatcher.HandleEvent(ctx, thread.reply.edit(t))
	require.Error(t, err,
		"an edit inside a locked thread must be refused even when the row predates the "+
			"recorded root: the community stopped accepting deliveries on this thread, and an "+
			"Update is a delivery")
	assert.Contains(t, err.Error(), "parent-locked")
	assert.Equal(t, deliveriesBefore, rowCount(t, h.db, "outbound_deliveries"),
		"and nothing goes out")
}

// TestAnUndoRestoresALegacySuppressedNativeComment is the permanent-brick case.
//
// Undoing a comment removal clears the bridge-side removal state — and NOTHING
// else. But a native comment's mapping can be soft-deleted and its AP id
// tombstoned by paths that predate (or sit beside) the comment-removal branch:
// the v1 delete path did exactly that before 17c-1 taught moderateAnnouncedDelete
// to take native comments, and the origin-verified delete sweep still can.
//
// A comment in that state is unreachable from both directions at once:
// moderateAnnouncedDelete declines forever on IsDeleted(), so no removal can be
// recorded, and the Undo that would repair it skips straight to the state clear
// without touching either marker. The community owns the content and has said,
// on the wire, that it wants it back — and nothing the bridge does can produce
// that outcome.
//
// The post path already treats both clears as legacy repair for exactly this
// reason; the comment path routes past them.
func TestAnUndoRestoresALegacySuppressedNativeComment(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)
	seedNativeComment(t, world)
	commentAPID := mtDirectReply.apID()

	// The state a pre-17c announced delete left behind: our own mapping soft
	// deleted, our own AP id tombstoned under the announcing community.
	require.NoError(t, h.objects.SoftDelete(ctx, commentAPID))
	require.NoError(t, h.tombstones.Record(ctx, commentAPID, groupID))
	suppressed, err := h.objects.GetByAPID(ctx, commentAPID)
	require.NoError(t, err)
	require.True(t, suppressed.IsDeleted(), "precondition: the comment is legacy-suppressed")

	// The community lifts it, honestly signed and announced.
	reason := "restored after appeal"
	h.announceUndoDelete(world.groupA,
		"https://lemmy.world/activities/announce/undo/ml-suppressed",
		"https://lemmy.world/activities/announce/delete/ml-suppressed/delete",
		commentAPID, &reason)

	mapping, err := h.objects.GetByAPID(ctx, commentAPID)
	require.NoError(t, err)
	assert.False(t, mapping.IsDeleted(),
		"the restore must clear the SOFT DELETE too: leaving it makes the comment "+
			"permanently unmoderatable — moderateAnnouncedDelete declines on IsDeleted(), so "+
			"the community can never remove it again either, and the Undo it just sent is the "+
			"only repair anyone was going to attempt")

	tombstoned, err := h.tombstones.ExistsFor(ctx, commentAPID, groupID)
	require.NoError(t, err)
	assert.False(t, tombstoned,
		"and the marker with it: a standing tombstone against our own AP id suppresses this "+
			"comment's later activities and drops the Lemmy replies beneath it")

	// The proof that the repair is real rather than cosmetic: the community can
	// moderate the comment again.
	h.announceDeleteWithSummary(world.groupA,
		"https://lemmy.world/activities/announce/delete/ml-again", commentAPID, &reason)
	state, found := removalStateFor(t, h.db, mtDirectReply.atURI())
	assert.True(t, found && state.removed,
		"a restored comment is moderatable again: that round trip — removed, restored, "+
			"removable — is the whole difference between state and a brick")
}

// legacyChildOf is a new reply hanging under an existing comment.
func legacyChildOf(parent nativeComment) nativeComment {
	return nativeComment{
		did: mtAuthorDID, rkey: "3lzmtcomment08",
		root:      nativeRef{mtPostATURI, mtPostCID},
		parent:    nativeRef{parent.atURI(), parent.createCID},
		createRev: "3lzmtrev000050", createCID: "bafyreih5xbmigkq5ikyhqiqhqzbwuqjxeitgtzwyxvjhfsfvswsxmnnf7c",
		editRev: "3lzmtrev000051", editCID: "bafyreih5xbmigkq5ikyhqiqhqzbwuqjxeitgtzwyxvjhfsfvswsxmnnf7d",
		timeUS: 1_775_000_000_000_500,
	}
}

// makeLegacySnapshot reduces a stored snapshot to the shape the PREVIOUS
// version wrote, by removing fields it did not have.
//
// It REFUSES to strip a field that is not there. That check is the whole
// integrity of this fixture: if the current path ever stops writing one of these
// keys — renamed, moved, dropped — a silent no-op here would leave every legacy
// test passing against ordinary modern rows, asserting nothing about the
// migration path they exist to cover.
func makeLegacySnapshot(t *testing.T, db *sql.DB, atURI string, fields ...string) {
	t.Helper()
	ctx := context.Background()
	for _, field := range fields {
		var present bool
		require.NoError(t, db.QueryRowContext(ctx,
			`SELECT jsonb_exists(translated_snapshot, $2) FROM outbound_objects WHERE at_uri = $1`,
			atURI, field).Scan(&present), "read the snapshot of %s", atURI)
		require.True(t, present,
			"precondition: the CURRENT path writes %q into %s's snapshot, so removing it "+
				"produces the row the previous version wrote — a field that is already absent "+
				"means this fixture is forging nothing", field, atURI)

		_, err := db.ExecContext(ctx,
			`UPDATE outbound_objects SET translated_snapshot = translated_snapshot - $2::text WHERE at_uri = $1`,
			atURI, field)
		require.NoError(t, err, "strip %q from %s", field, atURI)
	}
}
