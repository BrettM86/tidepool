package votes

import (
	"context"
	stderrors "errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/outbound"
	"tidepool/internal/store"
)

// THE LEDGER AND THE QUEUE MUST NEVER DISAGREE PERMANENTLY.
//
// Worker.deliverSuccess marks the delivery delivered — which makes the queue
// entry TERMINAL, never re-claimed — and only then runs voteCallback, as a
// separate autocommit. Everything between those two writes is a window in which
// a crash, a cancelled context or a database blip leaves the two records saying
// different things forever, because nothing ever revisits a terminal delivery.
//
// Before 17b that was cosmetic: outbound_votes was write-side bookkeeping.
// 17b made it an INPUT to a number users read, so each of these windows now has
// a permanent, user-visible price:
//
//	(a) Like delivered, SetDeliveredState fails  → row 'pending' forever, the
//	    vote sits at Lemmy uncounted-for → served total OVER-counts by one
//	(b) Undo delivered, Delete fails             → row 'delivered' forever, the
//	    peer dropped the vote → served total UNDER-counts by one
//	(c) the stale-claim branch returns BEFORE the callback → a POST that
//	    succeeded leaves the ledger untouched
//
// Nothing self-heals them: SeedAggregates' only caller is the backfill's post
// walk, behind a config flag and a freshness window, so a wrong tally can
// outlive the incident by weeks.
//
// These tests pin the PROPERTY, not a mechanism. GREEN may make the pair atomic
// in one transaction or run an idempotent fenced callback before the terminal
// transition; either satisfies the same two assertions — a terminal delivery
// implies a settled ledger row, and the tally the user sees is right once the
// worker stops having anything to do.

// faultyVotes fails exactly one of the two ledger writes, on demand, so a fault
// can be injected AFTER the delivery has been marked — the window under test.
type faultyVotes struct {
	store.OutboundVotes
	failSet    bool
	failDelete bool
	err        error
}

func (f *faultyVotes) SetDeliveredState(ctx context.Context, voteATURI string, state store.DeliveredState) error {
	if f.failSet {
		return f.err
	}
	return f.OutboundVotes.SetDeliveredState(ctx, voteATURI, state)
}

func (f *faultyVotes) Delete(ctx context.Context, voteATURI string) error {
	if f.failDelete {
		return f.err
	}
	return f.OutboundVotes.Delete(ctx, voteATURI)
}

// deliveryStates returns every delivery row's state, newest last.
func deliveryStates(t *testing.T, l *lifecycle) []string {
	t.Helper()
	rows, err := l.db.Query(`SELECT state FROM outbound_deliveries ORDER BY seq`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var states []string
	for rows.Next() {
		var state string
		require.NoError(t, rows.Scan(&state))
		states = append(states, state)
	}
	require.NoError(t, rows.Err())
	return states
}

// deliverTolerating runs the worker until it drains, allowing errors (a fault
// injected into the callback surfaces as one).
func deliverTolerating(t *testing.T, l *lifecycle) {
	t.Helper()
	for i := 0; i < 6; i++ {
		worked, err := l.worker.DeliverNext(context.Background())
		if err != nil {
			continue // the fault under test; the queue's own state is the assertion
		}
		if !worked {
			return
		}
	}
}

// releaseHeldDelivery lets the worker pick the delivery up again. A settlement
// that fails after a confirmed POST HOLDS its delivery — still pending, still
// claimed for the rest of the lease — so a redelivery is a minute away in
// production and unreachable inside a test that refuses to sleep. Clearing the
// claim is how the lease lapsing is spelled here; what is under test is what
// happens when the worker gets its next look, not the clock that gives it one.
func releaseHeldDelivery(t *testing.T, l *lifecycle) {
	t.Helper()
	_, err := l.db.ExecContext(context.Background(),
		`UPDATE outbound_deliveries SET claimed_until = NULL, next_attempt_at = now()
		 WHERE state = 'pending'`)
	require.NoError(t, err)
}

// TestDeliveredLikeNeverStrandsThePendingLedgerRow is (a).
//
// The POST succeeded — Lemmy holds the vote — and the delivery is recorded as
// delivered. If the ledger row is still 'pending' and the delivery is terminal,
// nothing will ever reconcile them: the seed will keep handing the community a
// total that includes a vote we cast on their behalf.
func TestDeliveredLikeNeverStrandsThePendingLedgerRow(t *testing.T) {
	faulty := &faultyVotes{err: stderrors.New("ledger write failed")}
	l := newLifecycle(t, 5, func(o *outbound.WorkerOptions) {
		faulty.OutboundVotes = store.NewOutboundVotes(o.DB)
		faulty.failSet = true
		o.Votes = faulty
	})
	ctx := context.Background()
	require.NoError(t, l.agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, tpSubject), ""))

	l.castVote(t, "3lztprev00001", directionDown)
	deliverTolerating(t, l)

	require.Equal(t, []string{"Dislike"}, l.sender.kinds(),
		"precondition: the vote really did reach the peer — this is a fault AFTER the wire")

	states := deliveryStates(t, l)
	require.Len(t, states, 1)
	if states[0] == string(store.DeliveryStateDelivered) {
		assert.Equal(t, string(store.DeliveredStateDelivered), l.state(t),
			"a TERMINAL delivery with a 'pending' ledger row is a permanent disagreement: "+
				"the queue will never revisit it, so the vote sits at Lemmy while we account "+
				"for nothing — the served total over-counts by one, forever")
	}

	// Whatever the mechanism, once the fault clears the worker must be able to
	// finish the job — a delivery it can no longer claim cannot be finished.
	faulty.failSet = false
	releaseHeldDelivery(t, l)
	deliverTolerating(t, l)
	assert.Equal(t, string(store.DeliveredStateDelivered), l.state(t),
		"after the blip passes the ledger must catch up: the vote IS at Lemmy")

	require.NoError(t, l.agg.SeedAggregates(ctx, tpSubject, 3, 2))
	up, down, found := counts(t, l.db, tpSubject)
	require.True(t, found)
	assert.Equal(t, 3, up)
	assert.Equal(t, 1, down,
		"and the number the community reads is right: our delivered vote is netted out")
}

// TestDeliveredUndoNeverStrandsTheDeliveredLedgerRow is (b), the mirror.
//
// The Undo reached Lemmy, so the peer no longer holds the vote. A ledger row
// left saying 'delivered' subtracts it from every future seed — the community's
// score is one lower than the truth, permanently, and in the direction nobody
// investigates because a missing vote looks like a vote never cast.
func TestDeliveredUndoNeverStrandsTheDeliveredLedgerRow(t *testing.T) {
	faulty := &faultyVotes{err: stderrors.New("ledger delete failed")}
	l := newLifecycle(t, 5, func(o *outbound.WorkerOptions) {
		faulty.OutboundVotes = store.NewOutboundVotes(o.DB)
		o.Votes = faulty
	})
	ctx := context.Background()
	require.NoError(t, l.agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, tpSubject), ""))

	// The vote is cast and delivered cleanly.
	l.castVote(t, "3lztprev00001", directionDown)
	deliverTolerating(t, l)
	require.Equal(t, string(store.DeliveredStateDelivered), l.state(t))

	// The user withdraws it. The Undo reaches Lemmy — and the ledger delete
	// fails right after the delivery is marked.
	faulty.failDelete = true
	l.deleteVote(t, "3lztprev00002")
	deliverTolerating(t, l)
	require.Equal(t, []string{"Dislike", "Undo"}, l.sender.kinds(),
		"precondition: the withdrawal really did reach the peer")

	states := deliveryStates(t, l)
	require.Len(t, states, 2)
	if states[1] == string(store.DeliveryStateDelivered) {
		assert.Equal(t, "", l.state(t),
			"a TERMINAL Undo delivery with the row still 'delivered' is the mirror "+
				"disagreement: the peer dropped the vote, we subtract it forever, and the "+
				"community's score is permanently one short")
	}

	faulty.failDelete = false
	releaseHeldDelivery(t, l)
	deliverTolerating(t, l)
	assert.Equal(t, "", l.state(t),
		"once the blip passes the row must go: the peer is not holding this vote")

	require.NoError(t, l.agg.SeedAggregates(ctx, tpSubject, 3, 1))
	up, down, _ := counts(t, l.db, tpSubject)
	assert.Equal(t, 3, up)
	assert.Equal(t, 1, down, "nothing left of ours to subtract")
}

// TestStaleClaimStillReachesTheLedger is (c).
//
// A worker whose lease expires while its POST is in flight loses the fencing
// race: MarkDelivered no-ops and deliverSuccess returns BEFORE voteCallback. The
// vote is at Lemmy and the ledger was never touched.
//
// The lease expiry is injected where it actually happens — during the send —
// rather than by editing rows around the worker, so the interleaving is the real
// one. Whether the recovery is a redelivery or a fenced callback is GREEN's
// choice; that it recovers is not.
func TestStaleClaimStillReachesTheLedger(t *testing.T) {
	l := newLifecycle(t, 5)
	ctx := context.Background()
	require.NoError(t, l.agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, tpSubject), ""))
	l.castVote(t, "3lztprev00001", directionDown)

	// While this worker is on the wire, another one re-claims the delivery:
	// claimed_until IS the fencing token, so the sender's own claim is now
	// stale and its MarkDelivered will not apply.
	var stolen bool
	l.sender.onSend = func() {
		if stolen {
			return
		}
		stolen = true
		_, err := l.db.ExecContext(ctx,
			`UPDATE outbound_deliveries SET claimed_until = now() + interval '2 seconds'`)
		require.NoError(t, err)
	}
	deliverTolerating(t, l)
	require.Equal(t, []string{"Dislike"}, l.sender.kinds(),
		"precondition: the losing worker's POST SUCCEEDED — the vote is at Lemmy")

	// The interloper's claim lapses; the queue is free to make progress again.
	l.sender.onSend = nil
	_, err := l.db.ExecContext(ctx,
		`UPDATE outbound_deliveries SET claimed_until = NULL, next_attempt_at = now()
		 WHERE state = 'pending'`)
	require.NoError(t, err)
	deliverTolerating(t, l)

	assert.Equal(t, string(store.DeliveredStateDelivered), l.state(t),
		"a POST that succeeded must reach the ledger EVENTUALLY, whichever worker lost the "+
			"fencing race: the alternative is a vote standing at Lemmy that we never account "+
			"for, and no re-seed corrects it because the ledger is what the seed reads")

	require.NoError(t, l.agg.SeedAggregates(ctx, tpSubject, 3, 2))
	_, down, _ := counts(t, l.db, tpSubject)
	assert.Equal(t, 1, down, "and the served total reflects it")
}
