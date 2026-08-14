package votes

import (
	"context"
	stderrors "errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/outbound"
	"tidepool/internal/store"
)

// TASK 17d, CYCLE 3 — THE ledger_unsettled PATH IS NOT A CONSENT QUESTION.
//
// 17b established a state no other delivery reaches: a POST the peer CONFIRMED
// whose local settlement failed. That delivery is HELD — last_error_class
// ledger_unsettled, still pending, still re-claimable — and it RESUMES AT THE
// SETTLEMENT, deliberately skipping the kill switch, the causal gate and the
// consent recheck, because all three answer "should this go out?" and the peer
// already answered it.
//
// 17d adds a consent-shaped reason to cancel work: an opt-out disables the actor
// and cancels their queued deliveries. Both halves of that must stay off this
// path, and the harm is the same in either direction — the vote is AT LEMMY, and
// a delivery that never settles leaves outbound_votes saying 'pending' forever:
//
//   - 17b's reseed subtracts only DELIVERED rows, so the fediverse-only tally it
//     serves keeps counting our persona's vote as a stranger's, permanently, on
//     a score readers see;
//   - and 17d's own destructive tier enumerates live votes from that same column,
//     so the Undo that erasure owes the peer is never enqueued — the purged
//     actor's vote stands on Lemmy forever, which is precisely what the tier
//     exists to prevent.
//
// The two tests below are the two doors into that outcome. Each drives the vote
// through the REAL path (consumer → enqueue → worker → wire) with the ledger
// write faulted, so the held state is one the system actually produced.

// heldForSettlement reports the single delivery's state and outcome class.
func heldForSettlement(t *testing.T, l *lifecycle) (state, class string) {
	t.Helper()
	require.NoError(t, l.db.QueryRow(
		`SELECT state, COALESCE(last_error_class, '') FROM outbound_deliveries`).Scan(&state, &class))
	return state, class
}

// newHeldVote casts one persona down-vote, lets it reach the peer, and faults
// the ledger write behind it. It returns the fault switch so a caller can clear
// it once the state under test has been established.
func newHeldVote(t *testing.T) (*lifecycle, *faultyVotes) {
	t.Helper()
	faulty := &faultyVotes{err: stderrors.New("ledger write failed")}
	l := newLifecycle(t, 5, func(o *outbound.WorkerOptions) {
		faulty.OutboundVotes = store.NewOutboundVotes(o.DB)
		faulty.failSet = true
		o.Votes = faulty
	})
	ctx := context.Background()

	// A Lemmy human's live up-vote, so the subject has a real score to be wrong
	// about.
	require.NoError(t, l.agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, tpSubject), ""))

	l.castVote(t, "3lztprev00001", directionDown)
	deliverTolerating(t, l)

	require.Equal(t, []string{"Dislike"}, l.sender.kinds(),
		"precondition: the vote really did reach the peer — everything here is about what "+
			"happens AFTER the wire said yes")
	state, class := heldForSettlement(t, l)
	require.Equal(t, "pending", state,
		"precondition: a settlement that fails after a confirmed POST HOLDS the delivery — "+
			"non-terminal, so the worker comes back to it")
	require.Equal(t, "ledger_unsettled", class,
		"precondition: and it carries the class that tells the next claim to resume at the "+
			"settlement rather than at the wire")
	require.Equal(t, string(store.DeliveredStatePending), l.state(t),
		"precondition: the ledger row is the one the hold exists to settle, and it is unsettled")

	return l, faulty
}

// settleAndAssert clears the fault, gives the worker its next look, and asserts
// the ledger caught up. The message is the same for both doors because the
// damage is: the peer holds this vote, and only this row can say so.
func settleAndAssert(t *testing.T, l *lifecycle, faulty *faultyVotes) {
	t.Helper()
	faulty.failSet = false
	releaseHeldDelivery(t, l)
	deliverTolerating(t, l)

	assert.Equal(t, string(store.DeliveredStateDelivered), l.state(t),
		"the held delivery must still settle: Lemmy holds this vote, and outbound_votes is "+
			"the only record of that. A row stranded at 'pending' over-counts the served score "+
			"forever AND hides the vote from the destructive tier's Undo enumeration, so an "+
			"erased user's vote stands on a peer that was never told")
	assert.Equal(t, []string{"Dislike"}, l.sender.kinds(),
		"and it settles WITHOUT going back to the wire: the peer accepted this activity once, "+
			"and the resume path exists because re-POSTing it is not the recovery")
}

// TestAHeldSettlementResumesEvenForADisabledActor is the WORKER door.
//
// The actor is disabled and opted out — everything the claim-time consent
// recheck reads says "do not speak for this user" — and that recheck must not be
// reachable from the resume path. A consent check re-introduced ahead of it
// cancels the exact row the delivery was held to settle.
func TestAHeldSettlementResumesEvenForADisabledActor(t *testing.T) {
	l, faulty := newHeldVote(t)
	ctx := context.Background()

	// The two facts the consent recheck reads, written directly: this test is
	// about the WORKER's decision, so the opt-out arrives here as state rather
	// than as an event with side effects of its own (that is the next test).
	require.NoError(t, store.NewAPActors(l.db).SetEnabled(ctx, tpNativeDID, false))
	_, err := store.NewFederationPrefs(l.db).Upsert(ctx, store.FederationPref{
		DID: tpNativeDID, Enabled: false, Source: store.FederationPrefSourceRecord,
	})
	require.NoError(t, err)

	settleAndAssert(t, l, faulty)

	state, _ := heldForSettlement(t, l)
	assert.Equal(t, "delivered", state,
		"and the delivery reaches its terminal state rather than being cancelled: cancelling "+
			"a delivery the peer already accepted is not a withdrawal, it is a lost record of "+
			"something that happened")
}

// TestAnOptOutDoesNotCancelADeliveryHeldForSettlement is the CONSUMER door, and
// the one 17d opened.
//
// handleFederation cancels this actor's PENDING deliveries, and a held
// settlement is pending by construction — that is what makes it re-claimable.
// Cancelling is terminal, so the worker never comes back, and the ledger row the
// hold existed to settle is stranded at 'pending' forever.
//
// The cancellation is right about everything it was written for: work that has
// NOT gone out must not go out. This row already went out. "Stop sending" and
// "forget what was sent" are different instructions, and only the first one was
// asked for.
func TestAnOptOutDoesNotCancelADeliveryHeldForSettlement(t *testing.T) {
	l, faulty := newHeldVote(t)

	// The user opts out, through the real consumer: the soft tier, which
	// disables the actor and cancels their queued work in one transaction.
	l.handle(t, fmt.Sprintf(
		`{"did":%q,"time_us":9700,"kind":"commit","commit":{"rev":"3lztprev00009","operation":"create",`+
			`"collection":"social.coves.bridge.federation","rkey":"self","cid":%q,`+
			`"record":{"$type":"social.coves.bridge.federation","enabled":false}}}`,
		tpNativeDID, testCID))

	enabled, err := store.NewAPActors(l.db).GetByDID(context.Background(), tpNativeDID)
	require.NoError(t, err)
	require.False(t, enabled.Enabled,
		"precondition: the opt-out really was applied — the assertions below must not pass "+
			"because nothing happened")

	settleAndAssert(t, l, faulty)
}
