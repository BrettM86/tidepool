package outbound

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// TASK 17d REVIEW — THE VOTE THE ERASURE NEVER SEES.
//
// The purge enumerates what to retract from outbound_votes where delivered_state
// = 'delivered', read BEFORE its transaction opens. A vote whose delivery is
// HELD FOR SETTLEMENT is invisible to that read: the peer HAS the Like — the
// POST was confirmed — and the ledger still says 'pending' because only the
// local settlement failed.
//
// The hold is not a lost cause; it is the opposite. The worker comes back,
// resumes at the settlement, and flips the row to 'delivered'. So the sequence
// that this test drives is the ordinary one:
//
//	POST confirmed → ledger write fails → row HELD, vote 'pending'
//	purge runs     → enumerates nothing → no Undo owed
//	worker resumes → settlement lands   → vote 'delivered', actor tombstoned
//
// and it ends with a vote standing on a peer, attributed to an actor this bridge
// has told the world is gone, with no Undo ever enqueued and nothing left that
// will ever notice — the purge is terminal and never re-runs. 17b's reseed keeps
// subtracting it from the served score forever, and the erasure has silently
// failed at the one thing it exists to do.
//
// THE ORDERING GAP IS THE BUG, not the hold. Whatever closes it — enumerating
// held deliveries too, retracting inside the transaction, or a settlement that
// checks for a tombstoned actor — the property below is the same, and it is the
// property a user asked for.

const pvVoteATURI = "at://" + wActorDID + "/social.coves.feed.vote/3lzpvvote001"

// purgeTestDB is workerTestDB plus the community table the purge resolves each
// vote's target through.
func purgeTestDB(t *testing.T) *sql.DB {
	t.Helper()
	conn := workerTestDB(t)
	testutil.Truncate(t, conn, "communities")
	_, err := store.NewCommunities(conn).UpsertCommunity(context.Background(), store.Community{
		APGroupID:         wCommunityAPID,
		DID:               wCommunityDID,
		PreferredUsername: "tech",
		Instance:          "lemmy.world",
		FollowState:       store.FollowStateAccepted,
	})
	require.NoError(t, err)
	return conn
}

// pvFaultyVotes fails the delivery-success ledger write on demand, which is what
// puts a confirmed POST into the held state.
type pvFaultyVotes struct {
	store.OutboundVotes
	failSet bool
}

func (f *pvFaultyVotes) SetDeliveredState(ctx context.Context, voteATURI string, state store.DeliveredState) error {
	if f.failSet {
		return stderrors.New("ledger write failed")
	}
	return f.OutboundVotes.SetDeliveredState(ctx, voteATURI, state)
}

// heldVoteWorld is one persona's vote, confirmed on the wire, with its ledger
// write faulted — the state both tests below start from.
type heldVoteWorld struct {
	conn   *sql.DB
	worker *Worker
	votes  store.OutboundVotes
	faulty *pvFaultyVotes
	purger *Purger
	likeID string
}

func newHeldVoteWorld(t *testing.T) *heldVoteWorld {
	t.Helper()
	conn := purgeTestDB(t)
	ctx := context.Background()
	seedWorkerActor(t, conn, true, false)

	likeID := "https://coves.social/ap/activity/" + repeatHex64("Like")
	votes := store.NewOutboundVotes(conn)
	_, err := votes.Upsert(ctx, store.OutboundVote{
		VoteATURI:         pvVoteATURI,
		ActorDID:          wActorDID,
		SubjectATURI:      "at://" + wCommunityDID + "/social.coves.community.postv2/3lzpost",
		SubjectAPID:       "https://lemmy.world/post/1",
		CommunityDID:      wCommunityDID,
		Direction:         "up",
		CurrentActivityID: likeID,
	})
	require.NoError(t, err)
	seedDelivery(t, conn, "Like", "", []byte(fmt.Sprintf(
		`{"id":%q,"type":"Like","actor":%q,"object":"https://lemmy.world/post/1"}`, likeID, wActorID)))

	// The Like reaches the peer and the ledger write does not commit.
	faulty := &pvFaultyVotes{OutboundVotes: votes, failSet: true}
	sender := &fakeSender{}
	w := newWorker(t, conn, sender, func(o *WorkerOptions) { o.Votes = faulty })
	_, err = w.DeliverNext(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, sender.count(),
		"precondition: the vote really is AT the peer — everything here is about what happens "+
			"after the wire said yes")

	held := getDelivery(t, conn, likeID)
	require.Equal(t, store.DeliveryStatePending, held.State)
	require.Equal(t, store.DeliveryHeldForSettlement, held.LastErrorClass,
		"precondition: the delivery is held for settlement, so the worker WILL come back to it")
	require.Equal(t, 1, held.Attempts,
		"a settlement hold KEEPS its attempt, unlike a park. The asymmetry is deliberate: a park "+
			"is a hold nobody tried, while this one already POSTed and its retries are spaced by a "+
			"backoff computed from this very counter — hand the attempt back and a persistently "+
			"failing local write spins at the base delay forever")
	stored, err := votes.GetByATURI(ctx, pvVoteATURI)
	require.NoError(t, err)
	require.Equal(t, store.DeliveredStatePending, stored.DeliveredState,
		"precondition: and the ledger row still says pending, which is the whole gap")

	enqueuer, err := NewEnqueuer(EnqueuerOptions{
		DB:         conn,
		Translator: NewTranslator("https://coves.social"),
		Inboxes:    staticInbox{inbox: wInbox},
		Actors:     store.NewAPActors(conn),
		UserOrigin: "https://coves.social",
	})
	require.NoError(t, err)

	return &heldVoteWorld{
		conn: conn, worker: w, votes: votes, faulty: faulty,
		purger: NewPurger(conn, "https://coves.social", enqueuer), likeID: likeID,
	}
}

// settle lets the held delivery finish the job it was held for, and asserts it
// really did — on the DELIVERY, which is the thing the hold is about. Asserting
// on the vote row instead would make the precondition depend on the very value
// the terminality test exists to pin.
func (w *heldVoteWorld) settle(t *testing.T) {
	t.Helper()
	w.faulty.failSet = false
	_, err := w.worker.DeliverNext(context.Background())
	require.NoError(t, err)
	settled := getDelivery(t, w.conn, w.likeID)
	require.Equal(t, store.DeliveryStateDelivered, settled.State,
		"precondition: the held settlement completed and the delivery reached its terminal "+
			"state — this is the recovery working, not a fault")
	require.Equal(t, 2, settled.Attempts,
		"and the ledger counted BOTH claims: settlement retries accumulate, so each one waits "+
			"longer than the last")
}

func TestPurge_DoesNotLeaveAHeldVoteStandingOnAPeer(t *testing.T) {
	world := newHeldVoteWorld(t)
	ctx := context.Background()
	conn := world.conn

	// --- WHEN: the user's account is withdrawn, and then the settlement the
	//     hold was waiting for lands.
	require.NoError(t, world.purger.DeleteRemoteContent(ctx, wActorDID))
	world.settle(t)

	// --- THEN: the peer must not be left holding a vote from a withdrawn actor.
	assert.Equal(t, 1, activitiesOfKind(t, conn, "Undo"),
		"the erasure owes this peer an Undo. The purge read outbound_votes BEFORE its "+
			"transaction and the row said 'pending', so it enumerated nothing — but the peer "+
			"had already accepted the Like, and the settlement that followed flipped the row to "+
			"'delivered' for an actor we have since tombstoned. Nothing re-runs a purge: the "+
			"vote stands on that instance forever, and 17b's reseed keeps subtracting it from a "+
			"score readers see. A hold is a delivery that SUCCEEDED, so the enumeration that "+
			"decides what a withdrawal owes cannot read only the rows whose bookkeeping "+
			"happened to finish first")
}

// TestPurge_ARetractedVoteIsNotResurrectedByALateSettlement is the OTHER half,
// and it is the half the enumeration fix cannot reach.
//
// Enumerating the held vote is what gets the Undo sent. It does nothing about
// what happens NEXT: the worker still comes back, the held settlement still
// runs, and SetDeliveredState still writes 'delivered' over the 'undone' the
// purge just recorded. The Undo is on the wire and the ledger says the vote is
// live — so 17b's reseed subtracts it from a served score forever, and an
// operator auditing a withdrawn identity sees votes it supposedly still holds.
//
// `undone` is the one value in this column that records OUR decision rather
// than the message's progress. A decision cannot be overwritten by a later fact
// about the thing it was a decision ABOUT, and this is the one place the two
// arrive out of order.
func TestPurge_ARetractedVoteIsNotResurrectedByALateSettlement(t *testing.T) {
	world := newHeldVoteWorld(t)
	ctx := context.Background()

	require.NoError(t, world.purger.DeleteRemoteContent(ctx, wActorDID))
	retracted, err := world.votes.GetByATURI(ctx, pvVoteATURI)
	require.NoError(t, err)
	require.Equal(t, store.DeliveredStateUndone, retracted.DeliveredState,
		"precondition: the purge retracted this vote — the decision is on the record")

	// The held settlement lands afterwards, which is the ordinary sequence: the
	// hold is what made the purge see this vote at all.
	world.settle(t)

	final, err := world.votes.GetByATURI(ctx, pvVoteATURI)
	require.NoError(t, err, "the row still exists: a purge retracts a vote, it does not delete it")
	assert.Equal(t, store.DeliveredStateUndone, final.DeliveredState,
		"the vote must STILL be undone. The settlement is a true fact about the Like — the peer "+
			"accepted it — arriving after a decision that supersedes it: this actor is gone and "+
			"their votes are withdrawn. Writing 'delivered' over it re-counts a tombstoned "+
			"identity's vote in the fediverse-only tally 17b feeds, permanently, with no purge "+
			"left to re-run and no reseed that corrects it")
}

// TestPurge_AReplayedPurgeDoesNotMintASecondUndo replays the purge — the step
// the two tests above stop short of, and the step both opt-out doors document
// as ORDINARY: the purge commits on its own transaction while the rev gate
// commits later, so a failure between the two re-runs the whole request.
//
// At the replay, the retracted vote's Like delivery still sits held for
// settlement — pending + ledger_unsettled, waiting on the worker — which is the
// exact shape the standing enumeration's EXISTS arm matches. If that arm does
// not also read the VOTE's own state, the replay re-enumerates a vote whose
// retraction is already on the record: undoLiveVotes runs again, activity_seq
// bumps, and a SECOND Undo goes out under a FRESH activity id. The peer already
// applied (or holds) the first, so the duplicate is typically refused, retried,
// and poisoned — permanently inflating the delivery-unknown-refused divergence
// count 17e says must stay small.
//
// The purge's idempotency claim is "reaches inboxes the first attempt missed
// without re-sending anything to the ones it reached" — deterministic ids make
// that true for the Delete{Person}, and only the vote's `undone` state can make
// it true for the Undo, because the Undo's id is seq-derived and the seq moves
// on every re-run.
func TestPurge_AReplayedPurgeDoesNotMintASecondUndo(t *testing.T) {
	world := newHeldVoteWorld(t)
	ctx := context.Background()

	require.NoError(t, world.purger.DeleteRemoteContent(ctx, wActorDID))
	// The replay, BEFORE the held settlement completes — so the Like delivery
	// still reads pending + held, and only the vote row remembers the
	// retraction already enqueued.
	require.NoError(t, world.purger.DeleteRemoteContent(ctx, wActorDID))

	assert.Equal(t, 1, activitiesOfKind(t, world.conn, "Undo"),
		"one retraction was decided, so one Undo may exist. A replayed purge that "+
			"re-enumerates the undone vote mints a NEW seq-derived activity id and enqueues a "+
			"duplicate Undo the peer will refuse — and a refused delivery retries into poison, "+
			"a permanent divergence for a withdrawal that actually succeeded the first time")
}

// activitiesOfKind counts enqueued activities of one kind.
func activitiesOfKind(t *testing.T, conn *sql.DB, kind string) int {
	t.Helper()
	var n int
	require.NoError(t, conn.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM outbound_activities WHERE kind = $1`, kind).Scan(&n))
	return n
}
