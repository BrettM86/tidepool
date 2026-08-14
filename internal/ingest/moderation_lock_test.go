package ingest

import (
	"context"
	"database/sql"
	"encoding/json"
	stderrors "errors"
	"expvar"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/consume"
	"tidepool/internal/echo"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// TASK 17c-2 — A LOCK IS MODERATION STATE THE BRIDGE OWNS.
//
// A lock is the first moderation decision with no home in either repo. A
// removal lives in the community's repo as a record; a lock has nowhere to go —
// the post is the author's, the acceptance says only that it was admitted, and
// Lemmy's own model keeps the flag on the post row. So the bridge has to hold
// it, and holding it is worth nothing unless something READS it: the whole
// point of a lock is that the next comment does not go out.
//
// That last half is what this file exists for. Recording a lock and then
// federating a reply under it is worse than not recording it at all: Lemmy
// rejects comments on locked posts server-side, so the delivery fails, retries,
// and eventually poisons — the author sees their comment sitting in their own
// repo forever with no explanation anywhere, while the operator sees a poisoned
// delivery whose cause is a moderator decision three tables away.
//
// THE FIXTURE HAS TWO COMMUNITIES, CO-HOSTED, AND TWO NATIVE ACTORS.
// Decision 18's rule is a CONJUNCTION — the signer must BE the community AND
// the target must belong to it — and in a one-community world those are the
// same fact, so an implementation checking either conjunct passes every test.
// The second native actor is here for the same reason one step ahead: a ban is
// (community, actor, content), and a one-actor fixture cannot tell "cancel that
// actor's deliveries to that community" from "cancel everything".
const (
	mtLockActivity   = "https://lemmy.world/activities/announce/lock/mt-lock"
	mtUnlockActivity = "https://lemmy.world/activities/announce/undo/mt-lock"
	mtCrossLock      = "https://lemmy.world/activities/announce/lock/mt-cross-lock"
	mtOtherLock      = "https://lemmy.world/activities/announce/lock/mt-other-lock"

	// The fediverse half of the fixture: a Lemmy human, and their comment on the
	// Lemmy post the standard page fixture materializes.
	mlReplier      = "https://lemmy.world/u/replier"
	mlLemmyComment = "https://lemmy.world/comment/9101"

	// The SECOND native post in community A: another thread, in the same
	// community, that no moderator has touched. It is the control for the
	// coarsest wrong fix — refusing everything once anything is locked — which a
	// one-thread fixture cannot see, exactly as a one-community fixture cannot
	// see a per-community over-reach.
	mtOtherPostRKey  = "3lzmtpost00002"
	mtOtherPostRev   = "3lzmtrev000020"
	mtOtherPostATURI = "at://" + mtAuthorDID + "/social.coves.community.postv2/" + mtOtherPostRKey
	mtOtherPostTime  = int64(1_775_000_000_000_200)
)

// The comment fixtures. Their revs and CIDs are reused VERBATIM on every retry:
// "an identical retry is admitted" is the contract, and a retry that changed
// the rev would be admitted by the rev gate for reasons that have nothing to do
// with the lock being lifted.
var (
	// mtDirectReply hangs DIRECTLY under the post. Its parent IS the locked
	// object, so the parent alone answers the question.
	mtDirectReply = nativeComment{
		did: mtCommenterDID, rkey: "3lzmtcomment01",
		root:      nativeRef{mtPostATURI, mtPostCID},
		parent:    nativeRef{mtPostATURI, mtPostCID},
		createRev: "3lzmtrev000010", createCID: "bafyreih5xbmigkq5ikyhqiqhqzbwuqjxeitgtzwyxvjhfsfvswsxmnnf6i",
		editRev: "3lzmtrev000011", editCID: "bafyreih5xbmigkq5ikyhqiqhqzbwuqjxeitgtzwyxvjhfsfvswsxmnnf6j",
		timeUS: 1_775_000_000_000_100,
	}

	// mtNestedReply hangs under mtDirectReply — one level down, which is where
	// an ordinary conversation goes. Its PARENT is a comment no moderator has
	// touched; its thread ROOT is the post.
	mtNestedReply = nativeComment{
		did: mtAuthorDID, rkey: "3lzmtcomment02",
		root:      nativeRef{mtPostATURI, mtPostCID},
		parent:    nativeRef{"at://" + mtCommenterDID + "/social.coves.community.comment/3lzmtcomment01", "bafyreih5xbmigkq5ikyhqiqhqzbwuqjxeitgtzwyxvjhfsfvswsxmnnf6i"},
		createRev: "3lzmtrev000012", createCID: "bafyreih5xbmigkq5ikyhqiqhqzbwuqjxeitgtzwyxvjhfsfvswsxmnnf6k",
		editRev: "3lzmtrev000013", editCID: "bafyreih5xbmigkq5ikyhqiqhqzbwuqjxeitgtzwyxvjhfsfvswsxmnnf6m",
		timeUS: 1_775_000_000_000_120,
	}

	// mtLateNestedReply is the same shape, arriving AFTER the lock: the reply
	// somebody writes to a conversation the moderators have just closed.
	mtLateNestedReply = nativeComment{
		did: mtCommenterDID, rkey: "3lzmtcomment03",
		root:      nativeRef{mtPostATURI, mtPostCID},
		parent:    nativeRef{"at://" + mtCommenterDID + "/social.coves.community.comment/3lzmtcomment01", "bafyreih5xbmigkq5ikyhqiqhqzbwuqjxeitgtzwyxvjhfsfvswsxmnnf6i"},
		createRev: "3lzmtrev000014", createCID: "bafyreih5xbmigkq5ikyhqiqhqzbwuqjxeitgtzwyxvjhfsfvswsxmnnf6n",
		editRev: "3lzmtrev000015", editCID: "bafyreih5xbmigkq5ikyhqiqhqzbwuqjxeitgtzwyxvjhfsfvswsxmnnf6p",
		timeUS: 1_775_000_000_000_140,
	}

	// The untouched thread: a direct reply to the OTHER post, and a nested reply
	// beneath it. Same author, same community, same shape — different thread.
	mtOtherDirectReply = nativeComment{
		did: mtCommenterDID, rkey: "3lzmtcomment04",
		root:      nativeRef{mtOtherPostATURI, mtPostCID},
		parent:    nativeRef{mtOtherPostATURI, mtPostCID},
		createRev: "3lzmtrev000016", createCID: "bafyreih5xbmigkq5ikyhqiqhqzbwuqjxeitgtzwyxvjhfsfvswsxmnnf6q",
		editRev: "3lzmtrev000017", editCID: "bafyreih5xbmigkq5ikyhqiqhqzbwuqjxeitgtzwyxvjhfsfvswsxmnnf6r",
		timeUS: 1_775_000_000_000_160,
	}
	mtOtherNestedReply = nativeComment{
		did: mtAuthorDID, rkey: "3lzmtcomment05",
		root:      nativeRef{mtOtherPostATURI, mtPostCID},
		parent:    nativeRef{"at://" + mtCommenterDID + "/social.coves.community.comment/3lzmtcomment04", "bafyreih5xbmigkq5ikyhqiqhqzbwuqjxeitgtzwyxvjhfsfvswsxmnnf6q"},
		createRev: "3lzmtrev000018", createCID: "bafyreih5xbmigkq5ikyhqiqhqzbwuqjxeitgtzwyxvjhfsfvswsxmnnf6s",
		editRev: "3lzmtrev000019", editCID: "bafyreih5xbmigkq5ikyhqiqhqzbwuqjxeitgtzwyxvjhfsfvswsxmnnf6t",
		timeUS: 1_775_000_000_000_180,
	}
)

// TestALockedPostRefusesNativeCommentsUntilItIsLifted is the OUTER CONTRACT for
// the lock vertical.
//
// GIVEN a native post accepted into community A, WHEN A announces a Lock for
// it, THEN the lock is recorded, a native comment on that post is REFUSED with
// a parent-locked reason an operator can read, and WHEN A announces Undo{Lock}
// the identical comment is admitted and federates.
//
// Every step runs through the real path: the signed inbox, the real queue and
// handler, the real dispatcher, engine and enqueuer. The lock and the comment
// arrive on OPPOSITE SIDES of the bridge — one over HTTP from Lemmy, one over
// Jetstream from the author's PDS — and the only thing that can join them is
// state the bridge durably owns. A test that reached into a store to set the
// flag would prove the reader works while leaving the writer, and the seam
// between them, entirely untested.
func TestALockedPostRefusesNativeCommentsUntilItIsLifted(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	// Snapshotted BEFORE the lock, so an enqueue caused by the LOCK ITSELF
	// cannot be folded into the baseline. Boomerang suppression for moderation
	// is structural today (the materializer's paths take no side effect and so
	// cannot enqueue), and a lock that echoes back is an activity aimed at the
	// moderators who just sent it.
	activitiesBefore := rowCount(t, h.db, "outbound_activities")
	deliveriesBefore := rowCount(t, h.db, "outbound_deliveries")
	dropsBefore := dropSnapshot()

	// --- WHEN: community A locks its own post, honestly signed.
	h.announceLock(world.groupA, mtLockActivity, mtPostAPID)

	// The echo classifier must NOT have taken this. carriesPayload is an
	// ALLOWLIST (Announce|Create|Update|Undo) and Lock is deliberately outside
	// it, because a Lock's `object` is its TARGET — and the target of every
	// inbound moderation action against native content is, by definition, one of
	// OUR ids. Adding the new verbs to that allowlist "for completeness" would
	// drop every moderation action the bridge receives AND score each one as a
	// successful suppression, which is 17a's HIGH re-committed in new vocabulary.
	assert.Equal(t, dropsBefore, dropSnapshot(),
		"an announced Lock of our own post is GENUINE remote traffic: the id it names is "+
			"ours precisely because the community is moderating our content, and reading that "+
			"as an echo silently disables inbound moderation while the drop counter reports "+
			"it as working")

	event, err := h.events.GetEvent(ctx, mtLockActivity)
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt, "the lock is DECIDED, not left retrying: %s", event.Error)
	assert.Nil(t, event.FailedAt, "nor poisoned")

	// --- THEN: a native comment on the locked post is REFUSED.
	//
	// require, not assert: everything after this point is about what a refusal
	// looks like, and if the comment federated instead there is nothing left to
	// characterise — the harm (a Note delivered to a community that rejects
	// comments on that post) has already happened.
	err = world.dispatcher.HandleEvent(ctx, mtCommentEvent(t))
	require.Error(t, err,
		"a comment under a locked post must be REFUSED: Lemmy rejects it server-side, so "+
			"federating it buys a failed delivery, a retry loop and finally a poisoned row "+
			"whose cause is a moderator decision nothing in the delivery names")

	assert.True(t, stderrors.Is(err, consume.ErrPermanentEvent),
		"and refused PERMANENTLY, so the event dead-letters and the cursor moves on: a "+
			"transient error would block every other native user's traffic behind one locked "+
			"thread, retrying a decision only a moderator can change (err=%v)", err)
	assert.Contains(t, err.Error(), "parent-locked",
		"with the REASON in the message: the connector stores err.Error() as the dead "+
			"letter's last_error, and that string is the only surface an operator triaging "+
			"the queue — or answering the author asking where their comment went — has to "+
			"go on. A silent skip returning nil is the failure mode this asserts against")

	assert.Zero(t, outboundRowsFor(t, h.db, mtDirectReply.atURI()),
		"and NOTHING is written for the refused comment: an outbound_objects row is what a "+
			"later delete is rebuilt from, so a row here means the bridge believes it "+
			"federated a comment it never sent")
	assert.Equal(t, activitiesBefore, rowCount(t, h.db, "outbound_activities"),
		"nor is anything enqueued — not by the lock, and not by the comment it refused")
	assert.Equal(t, deliveriesBefore, rowCount(t, h.db, "outbound_deliveries"),
		"...and no delivery")

	// --- AND: the lock is RECORDED, bound to the community that made it.
	//
	// The binding is not bookkeeping. The reader that lifts a lock has to know
	// whose lock it is, and an unbound row lets any co-hosted community's
	// Undo{Lock} clear a decision it did not make.
	communityDID, locked, found := lockStateFor(t, h.db, mtPostATURI)
	require.True(t, found,
		"the bridge must own this state: neither repo can hold it — the post is the "+
			"author's and the acceptance says only that it was admitted — so a lock that is "+
			"not recorded here survives nothing, not a restart and not the next comment")
	assert.True(t, locked, "and it must read as locked")
	assert.Equal(t, world.communityADID, communityDID,
		"bound to community A, the community that locked it")

	// --- WHEN: the moderators lift it.
	h.announceUndoLock(world.groupA, mtUnlockActivity, mtLockActivity+"/lock", mtPostAPID)

	_, stillLocked, _ := lockStateFor(t, h.db, mtPostATURI)
	assert.False(t, stillLocked,
		"Undo{Lock} clears the lock: a lock the moderators lifted that still refuses "+
			"comments is moderation state nobody can reach — no later activity clears it, "+
			"because Lemmy has already sent the only one it will ever send")

	// --- THEN: the IDENTICAL comment — same rkey, same rev, same cid — is
	//     admitted and federates.
	//
	// Identical on purpose: the refusal must have left NO trace that makes a
	// retry a no-op. The rev gate is claimed inside the same transaction the
	// handler runs in, so a refusal that advanced the gate would swallow the
	// retry as a stale replay and the comment would be lost for good — a lock
	// lifted, an author retrying, and silence.
	require.NoError(t, world.dispatcher.HandleEvent(ctx, mtCommentEvent(t)),
		"the identical comment must be admitted once the lock is lifted: the refusal is a "+
			"gate, not a verdict on the record")

	assert.Equal(t, 1, outboundRowsFor(t, h.db, mtDirectReply.atURI()),
		"the comment now has outbound state")
	assert.Greater(t, rowCount(t, h.db, "outbound_deliveries"), deliveriesBefore,
		"and a delivery to the community: 'admitted' means it went out, not merely that "+
			"the handler stopped returning an error")
}

// TestAReplyBeneathALockedThreadIsRefused closes the bypass.
//
// The refusal asks about the resolved PARENT. A reply to the post has the post
// as its parent, so it is caught; a reply to a COMMENT that federated before
// the lock has an unlocked parent and goes out anyway. That is not an edge
// case, it is the ordinary shape of a conversation — Lemmy locks THREADS, and
// anyone can keep talking simply by hitting reply one level down.
//
// What comes back is the exact harm the lock exists to prevent, with an extra
// step: Lemmy rejects the comment server-side, the delivery retries and
// poisons, and the operator now has a poisoned row for a thread the moderators
// closed, on a post whose lock the bridge did record correctly.
//
// The answer has to come from STATE, not from the record: reply.root is written
// by the author, so trusting it would let anyone reopen a locked thread by
// naming a different root.
func TestAReplyBeneathALockedThreadIsRefused(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)
	seedNestedThread(t, world)

	deliveriesBefore := rowCount(t, h.db, "outbound_deliveries")
	h.announceLock(world.groupA, mtLockActivity, mtPostAPID)

	// A reply to the FIRST comment. Its parent is a comment nobody moderated;
	// its thread root is the post the moderators just closed.
	err := world.dispatcher.HandleEvent(ctx, mtLateNestedReply.create(t))
	require.Error(t, err,
		"a reply one level down is still a reply in a locked THREAD: a lock that only "+
			"stops direct replies stops nothing, because the reply button under every "+
			"existing comment is the normal way a conversation continues")

	assert.True(t, stderrors.Is(err, consume.ErrPermanentEvent),
		"refused with the same permanence as a direct reply: an author must not learn "+
			"that where they clicked reply decides whether they get an answer (err=%v)", err)
	assert.Contains(t, err.Error(), "parent-locked",
		"and with the same reason, so the DLQ shows one cause for one moderator decision "+
			"rather than a locked thread's replies landing under two different stories")

	assert.Zero(t, outboundRowsFor(t, h.db, mtLateNestedReply.atURI()),
		"nothing is written for the refused reply")
	assert.Equal(t, deliveriesBefore, rowCount(t, h.db, "outbound_deliveries"),
		"and nothing is sent: the delivery is the harm — Lemmy rejects it, the row "+
			"retries, and it poisons with a cause three tables away")

	// --- And it comes back when the moderators reopen the thread.
	h.announceUndoLock(world.groupA, mtUnlockActivity, mtLockActivity+"/lock", mtPostAPID)

	require.NoError(t, world.dispatcher.HandleEvent(ctx, mtLateNestedReply.create(t)),
		"the identical reply is admitted once the lock is lifted")
	assert.Equal(t, 1, outboundRowsFor(t, h.db, mtLateNestedReply.atURI()))
	assert.Greater(t, rowCount(t, h.db, "outbound_deliveries"), deliveriesBefore,
		"and it federates: a thread that never reopens for nested replies is a lock "+
			"nobody can lift")
}

// TestAnEditBeneathALockedThreadIsRefused is the same bypass on the OTHER path,
// and it is the one a create-side fix leaves behind.
//
// An update never re-resolves its thread — it reads back the state its create
// wrote, deliberately, so an edit cannot move a comment between communities or
// up the thread. So whatever the create path learns about the thread ROOT has
// to be on that stored state, or the edit sails through a lock the create is
// refused by: the author of an existing reply keeps a live, editable surface
// inside a closed thread, and every edit is delivered to a community that has
// stopped accepting comments on it.
func TestAnEditBeneathALockedThreadIsRefused(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)
	seedNestedThread(t, world)

	deliveriesBefore := rowCount(t, h.db, "outbound_deliveries")
	h.announceLock(world.groupA, mtLockActivity, mtPostAPID)

	err := world.dispatcher.HandleEvent(ctx, mtNestedReply.edit(t))
	require.Error(t, err,
		"an edit to a reply in a locked thread must be refused too: an Update{Note} is a "+
			"delivery like any other, and the community that closed the thread has to accept "+
			"it for the edit to mean anything")

	assert.True(t, stderrors.Is(err, consume.ErrPermanentEvent),
		"with the same permanence (err=%v)", err)
	assert.Contains(t, err.Error(), "parent-locked", "and the same reason")

	assert.Equal(t, mtNestedReply.createCID, outboundCIDFor(t, h.db, mtNestedReply.atURI()),
		"and the refused edit wrote NOTHING: outbound state that advanced to the edited "+
			"version is state claiming the bridge federated an edit it never sent, and every "+
			"later delete would be rebuilt from it")
	assert.Equal(t, deliveriesBefore, rowCount(t, h.db, "outbound_deliveries"),
		"nor was anything delivered")

	// --- And the edit lands once the thread reopens.
	h.announceUndoLock(world.groupA, mtUnlockActivity, mtLockActivity+"/lock", mtPostAPID)

	require.NoError(t, world.dispatcher.HandleEvent(ctx, mtNestedReply.edit(t)),
		"the identical edit is admitted after the unlock: the refusal is a gate, not a "+
			"verdict on the record")
	assert.Equal(t, mtNestedReply.editCID, outboundCIDFor(t, h.db, mtNestedReply.atURI()),
		"and now the outbound state names the edited version")
	assert.Greater(t, rowCount(t, h.db, "outbound_deliveries"), deliveriesBefore,
		"and the edit went out")
}

// TestALockReachesOnlyItsOwnThread is the CONTROL, and it PASSES TODAY — nothing
// refuses a nested reply at all, so it can only fail once a root-aware refusal
// exists to over-reach. It is here as the negative half of the two tests above,
// and it has been tooth-checked (an unconditional refusal in
// refuseUnderLockedParent turns it red).
//
// The scope it pins is per-OBJECT. A second thread in a DIFFERENT community
// could not pin it: an implementation that refused every comment in a community
// holding any locked post would pass that test and fail this one. Same
// community, same author, same nesting — the only difference is which post the
// thread hangs from, which is exactly the difference the lock is keyed on.
func TestALockReachesOnlyItsOwnThread(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	// A second native post in community A, admitted through the real engine.
	require.NoError(t, world.dispatcher.HandleEvent(ctx,
		mtPostEventFor(t, mtOtherPostRKey, "create", mtOtherPostRev, mtPostCID, mtOtherPostTime)),
		"precondition: a second thread exists in the same community")
	require.NoError(t, world.dispatcher.HandleEvent(ctx, mtOtherDirectReply.create(t)),
		"precondition: it has a reply to nest under")

	deliveriesBefore := rowCount(t, h.db, "outbound_deliveries")
	h.announceLock(world.groupA, mtOtherLock, mtPostAPID)

	// Everything about the untouched thread keeps working: a nested reply,
	// whose ROOT is the other post...
	require.NoError(t, world.dispatcher.HandleEvent(ctx, mtOtherNestedReply.create(t)),
		"a reply in an UNLOCKED thread must still federate: a lock is per-object, and one "+
			"closed thread that silences a community is a moderator action nobody asked for "+
			"and nobody can see — the replies simply stop arriving")
	assert.Equal(t, 1, outboundRowsFor(t, h.db, mtOtherNestedReply.atURI()))

	// ...and an edit to the reply already standing in it.
	require.NoError(t, world.dispatcher.HandleEvent(ctx, mtOtherDirectReply.edit(t)),
		"and an edit in that thread lands too")
	assert.Equal(t, mtOtherDirectReply.editCID, outboundCIDFor(t, h.db, mtOtherDirectReply.atURI()))

	assert.Greater(t, rowCount(t, h.db, "outbound_deliveries"), deliveriesBefore,
		"both went out")

	// The locked thread is genuinely locked, so this is a scope test and not an
	// accident of the lock never having been recorded.
	_, locked, found := lockStateFor(t, h.db, mtPostATURI)
	require.True(t, found)
	require.True(t, locked, "precondition: the OTHER post really is locked")
}

// seedNestedThread federates the two comments a lock bypass needs to exist
// before the lock: a direct reply to the post, and a reply to THAT. Both are
// admitted through the real path while the thread is open — which is what makes
// them the shape a later lock has to reach, rather than fixture rows asserting
// their own conclusion.
func seedNestedThread(t *testing.T, world moderationWorld) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, world.dispatcher.HandleEvent(ctx, mtDirectReply.create(t)),
		"precondition: a direct reply federates while the thread is open")
	require.NoError(t, world.dispatcher.HandleEvent(ctx, mtNestedReply.create(t)),
		"precondition: a reply to that reply federates too")
}

// outboundCIDFor reads the version the bridge believes it last federated.
func outboundCIDFor(t *testing.T, db *sql.DB, atURI string) string {
	t.Helper()
	var cid string
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT last_cid FROM outbound_objects WHERE at_uri = $1`, atURI).Scan(&cid),
		"read outbound state for %s", atURI)
	return cid
}

// TestCrossCommunityLockIsRefused is the RELATIONAL case: community B, on the
// same instance as A, announces a Lock for A's post.
//
// Decision 18's conjunction collapses in a one-community fixture, so this is the
// only shape that can tell "the signer IS the community" from "the target is IN
// the community". Nothing about this delivery is malformed — B is followed, B
// signs as itself, and SameAuthority is true across every community lemmy.world
// hosts. It is simply not B's post to lock.
//
// The consequence of getting it wrong is quiet and total: one moderator team
// could freeze every thread in every community co-hosted with theirs, and the
// only visible symptom would be authors' comments dead-lettering with a reason
// that names a lock nobody in their community made.
func TestCrossCommunityLockIsRefused(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	h.announceLock(world.groupB, mtCrossLock, mtPostAPID)

	_, locked, found := lockStateFor(t, h.db, mtPostATURI)
	assert.False(t, found && locked,
		"community B may not lock community A's post: the target's community mapping is "+
			"the authorization input, and a signer that merely shares an instance with it "+
			"has no claim on it")

	// A's post is untouched, and the strongest evidence of that is not the
	// absence of a row — it is that the thread still works.
	_, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, world.digestRKey)
	assert.NoError(t, err, "A's acceptance stands: a refused lock changes nothing about it")

	require.NoError(t, world.dispatcher.HandleEvent(ctx, mtCommentEvent(t)),
		"and a native comment still federates: a refusal that half-applied — recorded "+
			"nowhere but read somewhere — would be indistinguishable from a legitimate lock "+
			"to everyone except the community that never made it")
	assert.Equal(t, 1, outboundRowsFor(t, h.db, mtDirectReply.atURI()),
		"the comment has outbound state, so it really did go out")
}

// announceLock delivers Lemmy's lock shape: Announce{Lock} from the community,
// whose INNER Lock is attributed to the acting MODERATOR (a /u/ actor) and whose
// `object` is the post being locked — the target, not a payload.
func (h *harness) announceLock(group *remoteActor, activityID, targetID string) {
	h.t.Helper()
	require.Equal(h.t, http.StatusAccepted, h.deliver(group, map[string]any{
		"id":       activityID,
		"type":     "Announce",
		"actor":    group.id,
		"audience": group.id,
		"cc":       []any{group.id + "/followers"},
		"object":   lockActivity(group, activityID+"/lock", targetID),
	}))
	h.drain()
}

// announceUndoLock delivers the unlock shape: Announce{Undo{Lock}} with the Lock
// carried INLINE (Lemmy embeds it rather than referencing its id).
func (h *harness) announceUndoLock(group *remoteActor, activityID, lockActivityID, targetID string) {
	h.t.Helper()
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
			"object":   lockActivity(group, lockActivityID, targetID),
		},
	}))
	h.drain()
}

func lockActivity(group *remoteActor, activityID, targetID string) map[string]any {
	return map[string]any{
		"id":       activityID,
		"type":     "Lock",
		"actor":    modActorID,
		"object":   targetID,
		"audience": group.id,
		"to":       []any{ap.PublicAudience},
		"cc":       []any{group.id},
	}
}

// nativeComment is one native reply, described by everything a commit frame
// needs and nothing else. Building the create and the edit from ONE fixture is
// what makes "the identical retry is admitted" expressible: every call returns
// the same bytes, so a retry differs from the refused attempt in nothing at all
// — not the rev, which the gate would otherwise admit for its own reasons.
type nativeComment struct {
	did  string
	rkey string
	// root and parent are the reply refs as the RECORD asserts them. The record
	// is the author's, so these are claims; what the bridge does with them is
	// the point of the tests below.
	root, parent nativeRef
	// The create and the edit carry different revs and CIDs, because they are
	// different commits on one record and the rev gate orders them.
	createRev, createCID string
	editRev, editCID     string
	timeUS               int64
}

type nativeRef struct{ uri, cid string }

func (c nativeComment) atURI() string {
	return "at://" + c.did + "/" + materialize.CollectionComment + "/" + c.rkey
}

// apID is the AP object id this comment federates under — the id a community
// names when it announces a moderation action against it.
func (c nativeComment) apID() string {
	return mtUserOrigin + "/ap/object/" + c.did + "/" + materialize.CollectionComment + "/" + c.rkey
}

func (c nativeComment) create(t *testing.T) *consume.JetstreamEvent {
	t.Helper()
	return c.commit(t, "create", c.createRev, c.createCID, c.timeUS)
}

func (c nativeComment) edit(t *testing.T) *consume.JetstreamEvent {
	t.Helper()
	return c.commit(t, "update", c.editRev, c.editCID, c.timeUS+1)
}

func (c nativeComment) commit(t *testing.T, operation, rev, cid string, timeUS int64) *consume.JetstreamEvent {
	t.Helper()
	frame := fmt.Sprintf(`{
  "did": %q, "time_us": %d, "kind": "commit",
  "commit": {
    "rev": %q, "operation": %q,
    "collection": %q,
    "rkey": %q, "cid": %q,
    "record": {
      "$type": %q,
      "reply": {
        "root":   {"uri": %q, "cid": %q},
        "parent": {"uri": %q, "cid": %q}
      },
      "content": "a reply whose fate the community's lock decides",
      "createdAt": "2026-08-13T11:00:00.000Z"
    }
  }
}`, c.did, timeUS, rev, operation,
		materialize.CollectionComment, c.rkey, cid,
		materialize.CollectionComment,
		c.root.uri, c.root.cid, c.parent.uri, c.parent.cid)
	var event consume.JetstreamEvent
	require.NoError(t, json.Unmarshal([]byte(frame), &event), "the frame must be valid wire JSON")
	return &event
}

// mtCommentEvent is the direct reply the outer contract uses.
func mtCommentEvent(t *testing.T) *consume.JetstreamEvent {
	t.Helper()
	return mtDirectReply.create(t)
}

// lockStateFor reads the bridge's own moderation state for one object.
//
// Read as SQL rather than through a store: this tier is asserting that the
// state is DURABLE and BOUND to a community, and a test that went through the
// same accessor the implementation writes with could not tell a persisted lock
// from one held in memory.
func lockStateFor(t *testing.T, db *sql.DB, atURI string) (communityDID string, locked, found bool) {
	t.Helper()
	err := db.QueryRowContext(context.Background(), `
		SELECT community_did, locked_at IS NOT NULL
		FROM object_moderation
		WHERE at_uri = $1`, atURI).Scan(&communityDID, &locked)
	if err == sql.ErrNoRows {
		return "", false, false
	}
	require.NoError(t, err, "read the bridge-owned moderation state for %s", atURI)
	return communityDID, locked, true
}

// outboundRowsFor counts the outbound state rows for one at-uri.
func outboundRowsFor(t *testing.T, db *sql.DB, atURI string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM outbound_objects WHERE at_uri = $1`, atURI).Scan(&n))
	return n
}

// TASK 17c-2, CYCLE 2 — THE THREAD ABOVE A FEDIVERSE COMMENT.
//
// A native reply to a LEMMY comment resolves its thread root to that comment:
// the bridge holds no outbound state for fediverse content, and "no state" is
// read as "the top of the thread" everywhere else (it is the same boundary
// recordedState draws for depth). So a lock recorded on the Lemmy POST above it
// never reaches the reply.
//
// Lemmy threads are mostly Lemmy comments, so this is not the exotic corner —
// it is the ordinary one. A moderator closes a busy thread, a native user hits
// reply under any existing comment in it, and the bridge federates a comment
// Lemmy rejects server-side: failed delivery, retry loop, poisoned row. Exactly
// the noise the lock exists to prevent, on the shape it will meet most often.
//
// The answer has to come from the materialized RECORD's reply.root — a read of
// a repo the bridge hosts (the bridged author's), not a network call.

// fediverseThread is a Lemmy post with a Lemmy comment under it, both
// materialized into community A, plus a native reply hanging off the comment.
type fediverseThread struct {
	postAPID     string
	postATURI    string
	commentATURI string
	reply        nativeComment
}

// seedFediverseThread materializes the Lemmy half through the REAL announce
// path: the post, then a Lemmy human's comment on it. Both land in bridged
// authors' repos, which is where the reply.root a lock has to be read from
// lives.
func seedFediverseThread(t *testing.T, h *harness, world moderationWorld) fediverseThread {
	t.Helper()
	ctx := context.Background()

	require.Equal(t, http.StatusAccepted,
		h.deliver(world.groupA, loadFixture(t, "announce_create_page_lemmy_world.json")))
	h.drain()
	post, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err, "precondition: the Lemmy post materialized into community A")

	h.serveObject("/u/replier", person(mlReplier, "replier", nil))
	comment := note(mlLemmyComment, mlReplier, pageID, "a Lemmy comment in the thread",
		"2026-08-13T10:00:00.000000Z")
	h.serveObject("/comment/9101", comment)
	require.Equal(t, http.StatusAccepted, h.deliver(world.groupA,
		announceCreateNote("https://lemmy.world/activities/announce/create/ml-9101", mlReplier, comment)))
	h.drain()
	lemmyComment, err := h.objects.GetByAPID(ctx, mlLemmyComment)
	require.NoError(t, err, "precondition: the Lemmy comment materialized under it")

	return fediverseThread{
		postAPID:     pageID,
		postATURI:    post.ATURI,
		commentATURI: lemmyComment.ATURI,
		reply: nativeComment{
			did: mtCommenterDID, rkey: "3lzmtcomment06",
			// The record's own claim about its thread. It is the AUTHOR's claim,
			// so nothing may be decided on it — but it is what a real client
			// writes, so the fixture writes it too.
			root:      nativeRef{post.ATURI, post.CID},
			parent:    nativeRef{lemmyComment.ATURI, lemmyComment.CID},
			createRev: "3lzmtrev000030",
			createCID: "bafyreih5xbmigkq5ikyhqiqhqzbwuqjxeitgtzwyxvjhfsfvswsxmnnf6u",
			editRev:   "3lzmtrev000031",
			editCID:   "bafyreih5xbmigkq5ikyhqiqhqzbwuqjxeitgtzwyxvjhfsfvswsxmnnf6v",
			timeUS:    1_775_000_000_000_300,
		},
	}
}

// TestAReplyBeneathALockedFediverseThreadIsRefused is the common case.
func TestAReplyBeneathALockedFediverseThreadIsRefused(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)
	thread := seedFediverseThread(t, h, world)

	deliveriesBefore := rowCount(t, h.db, "outbound_deliveries")
	h.announceLock(world.groupA, mtLockActivity, thread.postAPID)

	_, locked, found := lockStateFor(t, h.db, thread.postATURI)
	require.True(t, found && locked,
		"precondition: a community can lock a LEMMY post it hosts — locks are not a "+
			"native-content feature, and most locked threads will be this shape")

	err := world.dispatcher.HandleEvent(ctx, thread.reply.create(t))
	require.Error(t, err,
		"a native reply under a Lemmy comment is a reply in the Lemmy POST's thread: our "+
			"state stops at the comment because we hold no outbound row for fediverse "+
			"content, but Lemmy's lock is on the post, and Lemmy is what rejects the reply")

	assert.True(t, stderrors.Is(err, consume.ErrPermanentEvent),
		"refused with the same permanence as a native thread: the author who replied under "+
			"a Lemmy comment did the same thing as the author who replied under a native one "+
			"(err=%v)", err)
	assert.Contains(t, err.Error(), "parent-locked",
		"and with the same reason — one moderator decision, one story in the DLQ")

	assert.Zero(t, outboundRowsFor(t, h.db, thread.reply.atURI()),
		"nothing is written for the refused reply")
	assert.Equal(t, deliveriesBefore, rowCount(t, h.db, "outbound_deliveries"),
		"and nothing is sent to a community that will reject it")

	// --- And it comes back when the moderators reopen the thread.
	h.announceUndoLock(world.groupA, mtUnlockActivity, mtLockActivity+"/lock", thread.postAPID)

	require.NoError(t, world.dispatcher.HandleEvent(ctx, thread.reply.create(t)),
		"the identical reply is admitted once the lock is lifted")
	assert.Equal(t, 1, outboundRowsFor(t, h.db, thread.reply.atURI()))
	assert.Greater(t, rowCount(t, h.db, "outbound_deliveries"), deliveriesBefore,
		"and it federates")
}

// TestAReplyBeneathAnUnlockedFediverseThreadFederates is the CONTROL, and it
// PASSES TODAY — nothing refuses these replies at all.
//
// It is the half that costs something to get wrong in the other direction:
// reading a thread root out of a materialized record is a read that can fail,
// be absent, or dead-end, and every one of those must resolve toward federating.
// A native reply into an ordinary open Lemmy thread is the single most common
// write this consumer handles.
func TestAReplyBeneathAnUnlockedFediverseThreadFederates(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)
	thread := seedFediverseThread(t, h, world)

	deliveriesBefore := rowCount(t, h.db, "outbound_deliveries")
	require.NoError(t, world.dispatcher.HandleEvent(ctx, thread.reply.create(t)),
		"a reply in an open Lemmy thread must federate: this is the ordinary case, and a "+
			"lock check that refuses when it cannot read the thread takes the whole comment "+
			"path down for every community that has ever locked anything")
	assert.Equal(t, 1, outboundRowsFor(t, h.db, thread.reply.atURI()))

	require.NoError(t, world.dispatcher.HandleEvent(ctx, thread.reply.edit(t)),
		"and an edit to it lands too")
	assert.Greater(t, rowCount(t, h.db, "outbound_deliveries"), deliveriesBefore)
}

// TestAFediverseThreadIsUnaffectedByALockElsewhere is the SCOPE control for the
// same read, and it also PASSES TODAY.
//
// The community holds a real, standing lock — on the NATIVE post — while the
// Lemmy thread beside it is open. This is the case a conservative fallback
// gets wrong: answering "we could not establish this thread, and this community
// holds locks, so refuse" would park every reply to every fediverse comment in
// any community that has ever locked one post. The refusal must be about THIS
// thread or it is not about a thread at all.
func TestAFediverseThreadIsUnaffectedByALockElsewhere(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)
	thread := seedFediverseThread(t, h, world)

	h.announceLock(world.groupA, mtOtherLock, mtPostAPID)
	_, locked, found := lockStateFor(t, h.db, mtPostATURI)
	require.True(t, found && locked, "precondition: the community holds a standing lock")

	require.NoError(t, world.dispatcher.HandleEvent(ctx, thread.reply.create(t)),
		"a reply in a DIFFERENT, open thread must still federate: a lock is per-object, "+
			"and one locked post that silences every fediverse thread in the community is a "+
			"moderator action nobody took and nobody can see")
	assert.Equal(t, 1, outboundRowsFor(t, h.db, thread.reply.atURI()))
}

// TASK 17c-2, CYCLE 3 — A COMMUNITY'S REMOVAL OF A NATIVE COMMENT IS RECORDED.
//
// 17c-1 left this a DECIDED non-action: an announced delete of a native comment
// was TAKEN and counted, never declined, because declining would have run the v1
// destructive path against a record in the author's own repo — soft-deleting our
// own mapping and tombstoning our own AP id, after which the comment is
// permanently unmoderatable and every reply beneath it silently disappears.
//
// The state goes in the one place that can hold it. There is nothing to write
// Coves-side: the removal lexicon is POST-scoped, the comment-subject extension
// is Coves-owned and has not landed, and inventing a record shape here would
// publish a vocabulary the read path does not consult — a moderation decision
// that looks acted upon and is not. That gap is asserted below as the CURRENT
// CONTRACT, so the day the extension lands, a test fails and says so.

// nativeCommentRemoval is the fixture: one native comment, federated into
// community A through the real path, that a moderator then removes.
func seedNativeComment(t *testing.T, world moderationWorld) {
	t.Helper()
	require.NoError(t, world.dispatcher.HandleEvent(context.Background(), mtDirectReply.create(t)),
		"precondition: the native comment federated into community A")
}

// TestACommunitysRemovalOfANativeCommentIsRecorded is cycle 3's contract.
func TestACommunitysRemovalOfANativeCommentIsRecorded(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)
	seedNativeComment(t, world)

	commentATURI, commentAPID := mtDirectReply.atURI(), mtDirectReply.apID()
	mappingBefore, err := h.objects.GetByAPID(ctx, commentAPID)
	require.NoError(t, err, "precondition: the enqueuer mapped the federated comment")
	require.Equal(t, world.communityADID, mappingBefore.CommunityDID,
		"precondition: bound to the community it federated into — the authorization input")

	activitiesBefore := rowCount(t, h.db, "outbound_activities")
	deliveriesBefore := rowCount(t, h.db, "outbound_deliveries")
	communityFramesBefore := len(communityEvents(t, h, world.communityADID))
	require.NotZero(t, communityFramesBefore,
		"precondition: the community repo HAS a firehose history (the post's acceptance), so "+
			"'no new frame' below is a measurement and not an empty counter agreeing with itself")
	deferredBefore := deferredCommentModerations()

	// --- A moderator of community A removes the comment: Delete WITH summary,
	//     announced by the community that owns its mapping.
	reason := "rule 3: no personal attacks"
	h.announceDeleteWithSummary(world.groupA,
		"https://lemmy.world/activities/announce/delete/mc-removal", commentAPID, &reason)

	// --- THEN: the bridge records the decision.
	state, found := removalStateFor(t, h.db, commentATURI)
	require.True(t, found,
		"the removal must be RECORDED: a moderator decision the bridge takes and then "+
			"holds nowhere is one no admin surface can answer for and no later Undo can "+
			"reverse — and the alternative was destroying the author's own record")
	assert.True(t, state.removed, "removed_at stamps WHEN, so the decision has a time")
	assert.Equal(t, world.communityADID, state.communityDID,
		"bound to the community that made it: unbound, any co-hosted community's Undo "+
			"could lift a decision it had no part in")
	assert.Equal(t, "moderator-discretion", state.code,
		"with the SAME code a post removal writes: one moderator action must not read as "+
			"two different things depending on whether they removed a post or a comment")
	assert.Equal(t, reason, state.reason, "and the moderator's own reason, intact")

	assert.Equal(t, deferredBefore, deferredCommentModerations(),
		"and the placeholder is RETIRED: a removal that is both recorded and counted as "+
			"deferred reports a feature as missing while it works, which is how the counter "+
			"stops meaning anything")

	// --- AND: nothing Coves-visible was written. This is the documented gap,
	//     asserted as a contract rather than left as an absence nobody checks.
	assert.Equal(t, communityFramesBefore, len(communityEvents(t, h, world.communityADID)),
		"NO commit frame in the community repo: the removal lexicon is post-scoped and the "+
			"comment-subject extension is Coves-owned and unlanded, so any record written "+
			"here would publish a vocabulary Coves' read path does not consult — a removal "+
			"that looks honored and hides nothing. When that extension lands, this assertion "+
			"is the one that must change")
	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionRemoval, testDigestRKey(commentATURI))
	assert.True(t, errors.IsNotFound(err),
		"and specifically no removal record for the comment (err=%v)", err)
	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, world.digestRKey)
	assert.NoError(t, err,
		"the POST's acceptance is untouched: removing a comment says nothing about the "+
			"post it hangs under")

	// --- AND: the v1 destructive path was not entered.
	mapping, err := h.objects.GetByAPID(ctx, commentAPID)
	require.NoError(t, err)
	assert.False(t, mapping.IsDeleted(),
		"our own mapping stays live: soft-deleting it makes moderateAnnouncedDelete decline "+
			"forever on IsDeleted(), so the removal could never be undone")
	assert.Equal(t, store.OriginBridge, mapping.Origin, "and it is still ours")

	tombstoned, err := h.tombstones.ExistsFor(ctx, commentAPID, groupID)
	require.NoError(t, err)
	assert.False(t, tombstoned,
		"no tombstone against our own AP id: it suppresses this comment's own later "+
			"activities and drops every Lemmy reply beneath it")
	assert.False(t, outboundTombstoned(t, h.db, commentATURI),
		"and the bridge's outbound state is not withdrawn: the comment still exists in the "+
			"AUTHOR's repo, and tombstoning our copy would make their own later delete a no-op")

	assert.Equal(t, activitiesBefore, rowCount(t, h.db, "outbound_activities"),
		"NOTHING is enqueued: an inbound moderation action is the community telling US what "+
			"it did, and echoing it back is a Delete aimed at the moderators who sent it")
	assert.Equal(t, deliveriesBefore, rowCount(t, h.db, "outbound_deliveries"), "...and no delivery")

	event, err := h.events.GetEvent(ctx, "https://lemmy.world/activities/announce/delete/mc-removal")
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt, "the removal is DECIDED, not left retrying: %s", event.Error)
	assert.Nil(t, event.FailedAt, "nor poisoned")

	// --- AND: the moderators can lift it.
	h.announceUndoDelete(world.groupA,
		"https://lemmy.world/activities/announce/undo/mc-removal",
		"https://lemmy.world/activities/announce/delete/mc-removal/delete",
		commentAPID, &reason)

	after, found := removalStateFor(t, h.db, commentATURI)
	assert.False(t, found && after.removed,
		"Undo clears the removal: a decision the moderators reversed that still stands is "+
			"moderation nobody can reach — Lemmy sends no second activity to clear it")
	assert.Equal(t, deliveriesBefore, rowCount(t, h.db, "outbound_deliveries"),
		"and the restore enqueues nothing either")
}

// TestCrossCommunityRemovalOfANativeCommentIsRefused is the RELATIONAL control,
// and it PASSES TODAY — no removal is recorded for anyone yet.
func TestCrossCommunityRemovalOfANativeCommentIsRefused(t *testing.T) {
	h := newHarness(t)
	world := newModerationWorld(t, h)
	seedNativeComment(t, world)

	reason := "not your community's comment"
	h.announceDeleteWithSummary(world.groupB,
		"https://lemmy.world/activities/announce/delete/mc-cross", mtDirectReply.apID(), &reason)

	state, found := removalStateFor(t, h.db, mtDirectReply.atURI())
	assert.False(t, found && state.removed,
		"community B may not remove community A's comment: one moderator team would "+
			"otherwise be able to withdraw content from every community co-hosted with it")

	mapping, err := h.objects.GetByAPID(context.Background(), mtDirectReply.apID())
	require.NoError(t, err)
	assert.False(t, mapping.IsDeleted(),
		"and the refusal leaves the comment exactly as A admitted it")
}

// TestAPostRemovalWritesNoBridgeSideRemovalState is the SCOPE control for ruling
// C, and it PASSES TODAY.
//
// removed_at is COMMENTS ONLY. A post's removal is a record in the community's
// own repo, written by acceptrec in ONE commit with the withdrawal of the
// acceptance it replaces. A second copy here would be a second source of truth
// for one decision, and they would disagree the first time the commit succeeded
// and this write did not — leaving a post that reads as removed to the bridge
// and accepted to Coves, or the reverse.
func TestAPostRemovalWritesNoBridgeSideRemovalState(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	reason := "off topic for this community"
	h.announceDeleteWithSummary(world.groupA,
		"https://lemmy.world/activities/announce/delete/mc-post", mtPostAPID, &reason)

	_, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionRemoval, world.digestRKey)
	require.NoError(t, err,
		"precondition: the post's removal really did happen, in the community's repo where "+
			"it belongs")

	state, found := removalStateFor(t, h.db, mtPostATURI)
	assert.False(t, found && state.removed,
		"and it wrote NO bridge-side removal state: the community repo record is the single "+
			"source of truth for a post, and a duplicate here can only ever disagree with it")
}

// removalStateFor reads the bridge-owned removal state for one object.
type bridgeRemoval struct {
	communityDID string
	code         string
	reason       string
	removed      bool
}

func removalStateFor(t *testing.T, db *sql.DB, atURI string) (bridgeRemoval, bool) {
	t.Helper()
	var state bridgeRemoval
	err := db.QueryRowContext(context.Background(), `
		SELECT community_did, removal_code, removal_reason, removed_at IS NOT NULL
		FROM object_moderation
		WHERE at_uri = $1`, atURI).Scan(&state.communityDID, &state.code, &state.reason, &state.removed)
	if err == sql.ErrNoRows {
		return bridgeRemoval{}, false
	}
	require.NoError(t, err, "read the bridge-owned moderation state for %s", atURI)
	return state, true
}

// outboundTombstoned reports whether the bridge withdrew its own outbound state.
func outboundTombstoned(t *testing.T, db *sql.DB, atURI string) bool {
	t.Helper()
	var tombstoned bool
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT tombstoned_at IS NOT NULL FROM outbound_objects WHERE at_uri = $1`,
		atURI).Scan(&tombstoned), "read outbound state for %s", atURI)
	return tombstoned
}

// deferredCommentModerations reads the 17c-1 placeholder counter, tolerating its
// RETIREMENT: once the removal is real the counter has no reason to exist, and a
// test that could not survive its deletion would force it to be kept.
func deferredCommentModerations() int64 {
	counter, _ := expvar.Get("tidepool_moderation_native_comment_deferred").(*expvar.Int)
	if counter == nil {
		return 0
	}
	return counter.Value()
}

// TestASummarylessDeleteOfANativeCommentRecordsNothing pins the OTHER half of
// the discriminator, and it is the half that writes nothing.
//
// Lemmy's convention — the same one the post path already turns on, by PRESENCE
// and not emptiness — is that `summary` present means a moderator removed it and
// `summary` absent means the author deleted it themselves. So a summary-less
// announced delete of native content is an author's own delete coming home, not
// a moderator's decision, and recording moderator-discretion for it would assert
// that somebody moderated when nobody did: a removal record naming a moderator
// team that took no action, against an author who moderated nobody.
//
// It must still not fall into the v1 destructive path. That path is what the
// 17c-1 branch exists to keep native comments away from: it soft-deletes our own
// mapping and tombstones our own AP id, after which the comment can never be
// moderated again and every Lemmy reply beneath it is dropped. So the branch
// keeps TAKING this activity. It simply records nothing, and says so
// distinguishably — because "we removed it because a moderator said so" and "we
// did nothing because the author deleted their own comment" are different
// answers to the only question an operator ever asks here.
func TestASummarylessDeleteOfANativeCommentRecordsNothing(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)
	seedNativeComment(t, world)
	require.NoError(t, world.dispatcher.HandleEvent(ctx, mtNestedReply.create(t)),
		"precondition: a second native comment to aim the other shape at")

	activitiesBefore := rowCount(t, h.db, "outbound_activities")
	deliveriesBefore := rowCount(t, h.db, "outbound_deliveries")

	// The CONTRAST, delivered first: a moderator removal of the other comment.
	// Its outcome is the contract test's; what matters here is that the bridge
	// does not tell the same story about both.
	removalReason := "rule 3: no personal attacks"
	const removalActivity = "https://lemmy.world/activities/announce/delete/ms-removal"
	h.announceDeleteWithSummary(world.groupA, removalActivity, mtDirectReply.apID(), &removalReason)

	// The shape under test: NO summary key at all, attributed to a Lemmy
	// moderator — a foreign attribution, so the echo classifier cannot be what
	// decides this (see the sibling test below for the case where it is).
	const selfDeleteActivity = "https://lemmy.world/activities/announce/delete/ms-selfdelete"
	h.announceDeleteBy(world.groupA, selfDeleteActivity, mtNestedReply.apID(), modActorID, nil)

	// --- THEN: nothing is recorded about it.
	_, found := removalStateFor(t, h.db, mtNestedReply.atURI())
	assert.False(t, found,
		"a summary-less delete writes NO moderation state: `summary` present is what marks a "+
			"moderator removal, so recording one here asserts a decision no moderator made — "+
			"and it would stand until somebody sent an Undo for an action that never happened")

	// --- AND: the v1 destructive path is still not entered. This is what the
	//     branch exists for, and it must survive the branch learning to write.
	mapping, err := h.objects.GetByAPID(ctx, mtNestedReply.apID())
	require.NoError(t, err)
	assert.False(t, mapping.IsDeleted(),
		"our own mapping stays live: the v1 path soft-deletes it, and moderateAnnouncedDelete "+
			"then declines forever on IsDeleted() — the comment becomes unmoderatable by a "+
			"delete that was never a moderation action")
	assert.Equal(t, store.OriginBridge, mapping.Origin, "and it is still ours")
	tombstoned, err := h.tombstones.ExistsFor(ctx, mtNestedReply.apID(), groupID)
	require.NoError(t, err)
	assert.False(t, tombstoned,
		"no tombstone against our own AP id: it drops every Lemmy reply beneath the comment")
	assert.False(t, outboundTombstoned(t, h.db, mtNestedReply.atURI()),
		"and the bridge does not withdraw its own outbound state on a claim it cannot verify")

	assert.Equal(t, activitiesBefore, rowCount(t, h.db, "outbound_activities"),
		"nothing is enqueued by either shape")
	assert.Equal(t, deliveriesBefore, rowCount(t, h.db, "outbound_deliveries"), "...and no delivery")

	// --- AND: it is DECIDED, once, with its own reason.
	event, err := h.events.GetEvent(ctx, selfDeleteActivity)
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt,
		"a non-action is a decision and must be marked processed, not retried: %s", event.Error)
	assert.Nil(t, event.FailedAt, "nor poisoned")

	assert.NotEqual(t,
		skipReasonFor(t, h, removalActivity, mtDirectReply.apID()),
		skipReasonFor(t, h, selfDeleteActivity, mtNestedReply.apID()),
		"and the bridge must not tell the SAME story about both: today a moderator's removal "+
			"of a comment and an author's own delete are logged with one reason and counted in "+
			"one counter, so the operator asking 'did a moderator remove this?' reads an answer "+
			"that cannot distinguish yes from no")
}

// TestASummarylessDeleteOfANativeCommentByOurPersonaIsDroppedAsAnEcho records
// which mechanism is actually load-bearing when the attribution is TRUTHFUL.
//
// CHARACTERIZATION: this passes today, and it is the comment-shaped twin of the
// post case 17c-1 pinned. A native comment's author IS one of our personas, so a
// truthful self-delete announced back at us is indistinguishable from our own
// Delete coming home — and the echo classifier takes it by the inner ACTOR,
// before any authorization or moderation branch runs.
//
// It is worth pinning because it means the summary-less branch's unverified
// attribution is unreachable for native comments from either direction: a
// persona attribution is dropped here, and a foreign attribution is bounded by
// the ownership conjunct. Take this guard away — by descending into a Delete's
// target, or by probing the wrong table for "ours" — and an author's own delete
// starts arriving at a branch that is about to learn how to write moderation
// state.
func TestASummarylessDeleteOfANativeCommentByOurPersonaIsDroppedAsAnEcho(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)
	seedNativeComment(t, world)
	before := dropSnapshot()

	// Community A — the OWNER, so ownership cannot be what refuses this — and the
	// inner Delete attributed to the comment's real author, one of our personas.
	h.announceDeleteBy(world.groupA,
		"https://lemmy.world/activities/announce/delete/ms-persona",
		mtDirectReply.apID(), mtUserOrigin+"/ap/actor/"+mtCommenterDID, nil)

	assert.Equal(t, before[echo.ClassLocalActor]+1, echo.Drops(echo.ClassLocalActor),
		"it is dropped as an ECHO, by the inner actor: our own Delete of a native comment "+
			"comes back announced, and the only thing that can tell it from a moderator's is "+
			"whose activity it is")

	_, found := removalStateFor(t, h.db, mtDirectReply.atURI())
	assert.False(t, found,
		"so no moderation state is recorded for it: an echo of the author's own delete must "+
			"never become a community-signed removal — the exact failure 17a's M1 was fixed to "+
			"prevent, one collection over")

	mapping, err := h.objects.GetByAPID(ctx, mtDirectReply.apID())
	require.NoError(t, err)
	assert.False(t, mapping.IsDeleted(), "and the mapping is untouched")
}

// skipReasonFor returns the reason the queue logged when it skipped an activity,
// or "" if it logged no skip for it. The reason is not stored on the event row —
// the queue logs it and marks the event processed — so this line is the only
// place the bridge says WHY it decided to do nothing.
//
// target is REDACTED out of the answer, and that is not cosmetic: a skip reason
// carries the id it is about, so two reasons about two different objects differ
// as strings no matter how identical their wording. Comparing them without
// redacting reads "the bridge distinguished these two cases" off nothing but the
// two objects having different names — a test that passes before the behaviour
// it describes exists.
func skipReasonFor(t *testing.T, h *harness, activityID, target string) string {
	t.Helper()
	for _, line := range strings.Split(h.logs.String(), "\n") {
		if !strings.Contains(line, "inbox event skipped") || !strings.Contains(line, activityID) {
			continue
		}
		_, reason, ok := strings.Cut(line, "reason=")
		if !ok {
			return ""
		}
		return strings.ReplaceAll(reason, target, "<target>")
	}
	return ""
}
