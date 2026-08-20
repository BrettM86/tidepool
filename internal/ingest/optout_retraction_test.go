package ingest

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/consume"
)

// TASK 17d REVIEW — WHAT A STOPPED USER IS STILL OWED.
//
// "Stop federating for me" and "forget what you already sent" are different
// instructions, and only the first one is ever asked for. The worker has said so
// since task 15 — a Delete or an Undo skips the consent recheck, because a
// retraction is the ONLY way an opted-out user takes down what is already
// published — but the queue-side cancellation is one layer up, where nothing
// observed it.
//
// Two consequences, and the second is the one that eats the whole tier:
//
//  1. A user who deletes a post and THEN opts out has their pending self-delete
//     cancelled, so the post stays on Lemmy forever. They asked to be forgotten
//     and got the opposite of both halves.
//  2. THE DESTRUCTIVE TIER CANCELS ITSELF ON REPLAY. The purge commits on its own
//     transaction; the rev gate commits later. A failure in between replays the
//     record — and the replay's cancellation runs FIRST, sweeping the
//     Delete{Person} fan-out and the vote Undos the previous attempt just
//     committed. Nothing repairs it: the delivery insert returns the standing
//     (cancelled) row by design, and the votes are already flipped to 'undone' so
//     the re-run enumerates nothing to retract. The actor is tombstoned, the
//     preference is recorded, the log says "destructive opt-out applied", and not
//     one peer was ever told.
//
// (2) is invisible from inside a single pass: every row exists, every counter
// moves. Only replaying the record — which is what a rolled-back gate DOES —
// shows it.

const (
	orSelfDeleteRKey = "3lzorpost00001"
	orOptOutRev      = "3lzorrev000001"
	orPurgeRev       = "3lzorrev000002"
	orPurgeRKey      = "3lzorpost00002"
)

// TestASelfDeleteStillGoesOutAfterTheAuthorOptsOut is (1).
//
// The delete is already in the queue when the opt-out lands. Cancelling it is
// the one cancellation that leaves MORE of the user's content on the fediverse
// than doing nothing would have.
func TestASelfDeleteStillGoesOutAfterTheAuthorOptsOut(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	// --- GIVEN: an accepted post the author then takes down.
	admitPost(t, world, mtAuthorDID, orSelfDeleteRKey, world.communityADID,
		"3lzorrev000010", 1_775_000_050_000_001)
	require.NoError(t, world.dispatcher.HandleEvent(ctx,
		postDeleteEvent(t, mtAuthorDID, orSelfDeleteRKey, "3lzorrev000011", 1_775_000_050_000_002)))

	deleteActivity := activityOfKind(t, h.db, mtAuthorDID, "Delete")
	require.NotEmpty(t, deleteActivity,
		"precondition: the author's own delete really did enqueue a retraction")
	require.Equal(t, []string{"pending"}, deliveryStatesForActivity(t, h.db, deleteActivity),
		"precondition: and it has not gone out yet — this is the window the opt-out lands in")

	// --- WHEN: they opt out. Soft tier.
	require.NoError(t, world.dispatcher.HandleEvent(ctx,
		odFederationEvent(t, mtAuthorDID, orOptOutRev, "create", false, false, 1_775_000_051_000_001)))

	// --- THEN: the retraction survives.
	assert.Equal(t, []string{"pending"}, deliveryStatesForActivity(t, h.db, deleteActivity),
		"a Delete is how an opted-out user takes down what is ALREADY federated. Cancelling "+
			"it leaves the post standing on Lemmy forever — the user asked to stop federating "+
			"and the queue answered by keeping their content published. The worker has always "+
			"exempted retractions from the consent recheck; a cancellation one layer up "+
			"reverses that decision where the worker can no longer see it")
}

// TestAReplayedDestructiveOptOutDoesNotCancelItsOwnWithdrawal is (2).
//
// The gate rollback is spelled the way it actually happens: the purge committed,
// the gate did not, so no rev row survives and the SAME record is delivered
// again. Everything the second pass does must leave the first pass's withdrawal
// deliverable.
func TestAReplayedDestructiveOptOutDoesNotCancelItsOwnWithdrawal(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	// --- GIVEN: content on two instances and one vote a peer still holds.
	admitPost(t, world, mtAuthorDID, orPurgeRKey, world.communityADID,
		"3lzorrev000020", 1_775_000_052_000_001)
	h.subscribeCommunityURL(odFarCommunity, odFarName)
	admitPost(t, world, mtAuthorDID, "3lzorpost00003", testDIDFor(odFarName, "lemmy.zip"),
		"3lzorrev000021", 1_775_000_052_000_002)
	liveVote := seedDeliveredVote(t, h.db, mtAuthorDID, mtPostATURI, world.communityADID, "delivered")

	// --- WHEN: the destructive record is applied...
	event := odFederationEvent(t, mtAuthorDID, orPurgeRev, "create", false, true, 1_775_000_053_000_001)
	require.NoError(t, world.dispatcher.HandleEvent(ctx, event))

	deleteActivity := activityOfKind(t, h.db, mtAuthorDID, "Delete")
	require.NotEmpty(t, deleteActivity, "precondition: the withdrawal was enqueued")
	inboxes := deliveryStatesForActivity(t, h.db, deleteActivity)
	require.Len(t, inboxes, 2, "precondition: fanned out to both instances")
	require.Equal(t, 1, undoActivitiesFor(t, h.db, mtAuthorDID),
		"precondition: and the live vote's Undo with it")
	require.Equal(t, "undone", voteState(t, h.db, liveVote),
		"precondition: the vote is already flipped, so a re-run enumerates nothing — which is "+
			"exactly why a cancelled Undo is unrecoverable")

	// --- ...and the gate transaction rolls back. Nothing marks the record
	//     applied, so the connector redelivers it and the handler runs again.
	clearRevGate(t, h.db, mtAuthorDID)
	require.NoError(t, world.dispatcher.HandleEvent(ctx, event),
		"the replay is the ordinary consequence of a rolled-back gate, not an error")

	// --- THEN: the withdrawal is still deliverable.
	states := deliveryStatesForActivity(t, h.db, deleteActivity)
	require.Len(t, states, 2,
		"the replay must not lose an inbox: the standing row wins, so the fan-out is neither "+
			"duplicated nor dropped")
	for i, state := range states {
		assert.Equal(t, "pending", state,
			"delivery %d of %d: a replayed opt-out must not cancel the Delete{Person} the "+
				"previous pass committed. The cancellation runs before the purge and cannot "+
				"tell its own withdrawal from the user's ordinary queued work — and nothing "+
				"repairs it, because the re-enqueue returns the standing cancelled row and the "+
				"votes are already retracted. The result is an actor tombstoned, a preference "+
				"recorded, a log line saying it was applied, and zero peers told", i+1, len(states))
	}

	undoActivity := activityOfKind(t, h.db, mtAuthorDID, "Undo")
	require.NotEmpty(t, undoActivity)
	for _, state := range deliveryStatesForActivity(t, h.db, undoActivity) {
		assert.Equal(t, "pending", state,
			"and the vote Undo with it: the peer holds a vote from an actor we have just "+
				"tombstoned, and this delivery is the only thing that will ever tell them")
	}
}

// postDeleteEvent is the author's own take-down: a delete commit, which carries
// no record and no CID — everything the Delete{Page} needs is read from the
// stored outbound state.
func postDeleteEvent(t *testing.T, did, rkey, rev string, timeUS int64) *consume.JetstreamEvent {
	t.Helper()
	frame := fmt.Sprintf(
		`{"did":%q,"time_us":%d,"kind":"commit","commit":{"rev":%q,"operation":"delete",`+
			`"collection":"social.coves.community.postv2","rkey":%q}}`,
		did, timeUS, rev, rkey)
	var event consume.JetstreamEvent
	require.NoError(t, json.Unmarshal([]byte(frame), &event), "the frame must be valid wire JSON")
	return &event
}

// clearRevGate deletes the DID's rev-gate rows, which is the state a ROLLED-BACK
// gate transaction leaves behind: the handler's own writes may have committed
// separately (the purge takes its own transaction), and nothing records that the
// record was applied, so the connector redelivers it.
func clearRevGate(t *testing.T, db *sql.DB, did string) {
	t.Helper()
	_, err := db.ExecContext(context.Background(),
		`DELETE FROM jetstream_record_revs WHERE record_uri LIKE 'at://' || $1 || '/%'`, did)
	require.NoError(t, err)
}

// deliveryStatesForActivity lists every delivery state for one activity, in a
// stable order.
func deliveryStatesForActivity(t *testing.T, db *sql.DB, activityID string) []string {
	t.Helper()
	return queryStrings(t, db,
		`SELECT state FROM outbound_deliveries WHERE activity_id = $1 ORDER BY target_inbox`,
		activityID)
}
