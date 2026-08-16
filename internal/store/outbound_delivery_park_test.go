package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ReleaseParked — the attempt-neutral release. ClaimNext charges an attempt to
// every claim, which is right for a delivery that was TRIED and wrong for one
// that was HELD: a kill switch, a dry run and a causal wait all put the delivery
// back untouched, and the retry budget they consume is budget the delivery no
// longer has when the hold lifts. ReleaseParked is Release with that one
// increment handed back, under the identical fence — so a hold is free, and a
// failure still costs exactly one.

// parkedAttempts reads the attempt ledger of the delivery under test — the one
// column every assertion in this file is really about.
func parkedAttempts(t *testing.T, repo OutboundDeliveries) int {
	t.Helper()
	got, err := repo.Get(context.Background(), delActivityID, delTargetInbox)
	require.NoError(t, err)
	return got.Attempts
}

func TestOutboundDeliveries_ReleaseParkedIsAttemptNeutral(t *testing.T) {
	database := deliveryTestDB(t)
	activities := NewOutboundActivities(database)
	repo := NewOutboundDeliveries(database)
	ctx := context.Background()

	seedActivity(t, activities, testActivity())
	_, err := repo.Enqueue(ctx, testDelivery())
	require.NoError(t, err)

	claimed, err := repo.ClaimNext(ctx, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed.ClaimedUntil)
	require.Equal(t, 1, claimed.Attempts, "the claim charged its attempt (that is what a park hands back)")
	token := *claimed.ClaimedUntil

	next := time.Now().Add(5 * time.Second).UTC()
	exists, applied, err := repo.ReleaseParked(ctx, delActivityID, delTargetInbox,
		"switch_parked", "outbound kill switch engaged", 0, next, token)
	require.NoError(t, err)
	assert.True(t, exists, "the row is there to be parked")
	assert.True(t, applied, "the claim holder parks its own delivery")

	got, err := repo.Get(ctx, delActivityID, delTargetInbox)
	require.NoError(t, err)
	assert.Equal(t, 0, got.Attempts,
		"a park is ATTEMPT-NEUTRAL: it hands back the increment its own claim took, so a delivery "+
			"held by an operator switch keeps every retry it had before the hold")
	assert.Equal(t, DeliveryStatePending, got.State, "a parked delivery stays pending — a hold is not a failure")
	assert.Nil(t, got.ClaimedUntil, "the lease is cleared so the delivery can be re-claimed when the hold lifts")
	assert.WithinDuration(t, next, got.NextAttemptAt, time.Second, "the park delay is stored")
	assert.Equal(t, "switch_parked", got.LastErrorClass,
		"the hold's reason is recorded, so an operator can see WHY the queue is idle")
}

func TestOutboundDeliveries_ReleaseParkedFencing(t *testing.T) {
	// The give-back is the dangerous half of this method: an unfenced decrement
	// would let a delivery lose attempts it genuinely spent. Both cases below
	// are a park arriving too late to be anyone's business.

	t.Run("stale token cannot un-count a newer claim", func(t *testing.T) {
		database := deliveryTestDB(t)
		activities := NewOutboundActivities(database)
		repo := NewOutboundDeliveries(database)
		ctx := context.Background()

		seedActivity(t, activities, testActivity())
		_, err := repo.Enqueue(ctx, testDelivery())
		require.NoError(t, err)

		claimed, err := repo.ClaimNext(ctx, time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claimed.ClaimedUntil)
		staleToken := *claimed.ClaimedUntil

		// That worker wedged; its lease lapses and a second worker takes the
		// delivery (attempts = 2). Then the first wakes up and tries to park.
		_, err = database.ExecContext(ctx,
			`UPDATE outbound_deliveries SET claimed_until = now() - interval '1 minute' WHERE activity_id = $1`,
			delActivityID)
		require.NoError(t, err)

		reclaimed, err := repo.ClaimNext(ctx, time.Minute)
		require.NoError(t, err)
		require.Equal(t, 2, reclaimed.Attempts)
		require.NotNil(t, reclaimed.ClaimedUntil)

		_, applied, err := repo.ReleaseParked(ctx, delActivityID, delTargetInbox,
			"switch_parked", "kill switch", 0, time.Now().Add(5*time.Second), staleToken)
		require.NoError(t, err)
		assert.False(t, applied, "a stale fencing token must not park a delivery someone else now holds")

		got, err := repo.Get(ctx, delActivityID, delTargetInbox)
		require.NoError(t, err)
		assert.Equal(t, 2, got.Attempts,
			"the give-back is fenced: a stale park must never un-count an attempt the CURRENT claim spent")
		require.NotNil(t, got.ClaimedUntil, "the current worker's lease survives a stale park")
		assert.WithinDuration(t, *reclaimed.ClaimedUntil, *got.ClaimedUntil, time.Second,
			"the lease is the second worker's, untouched")

		// The other side of the fence, so "nothing happened" above is the FENCE
		// refusing and not the method declining to work at all: the worker that
		// actually holds the claim parks, and gets its own attempt back.
		_, applied, err = repo.ReleaseParked(ctx, delActivityID, delTargetInbox,
			"switch_parked", "kill switch", 0, time.Now().Add(5*time.Second), *reclaimed.ClaimedUntil)
		require.NoError(t, err)
		assert.True(t, applied, "the CURRENT claim holder parks — the fence blocks the stale worker, not the real one")
		assert.Equal(t, 1, parkedAttempts(t, repo),
			"the holder's park hands back its OWN claim only: the earlier, genuinely-spent attempt remains")
	})

	t.Run("terminal row is untouched", func(t *testing.T) {
		database := deliveryTestDB(t)
		activities := NewOutboundActivities(database)
		repo := NewOutboundDeliveries(database)
		ctx := context.Background()

		seedActivity(t, activities, testActivity())
		_, err := repo.Enqueue(ctx, testDelivery())
		require.NoError(t, err)

		claimed, err := repo.ClaimNext(ctx, time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claimed.ClaimedUntil)
		token := *claimed.ClaimedUntil

		// The actor opts out mid-flight: the claimed row is cancelled under the
		// worker. Its park then arrives against a terminal row.
		cancelled, err := repo.CancelForActor(ctx, testDID)
		require.NoError(t, err)
		require.Equal(t, int64(1), cancelled)

		exists, applied, err := repo.ReleaseParked(ctx, delActivityID, delTargetInbox,
			"switch_parked", "kill switch", 0, time.Now().Add(5*time.Second), token)
		require.NoError(t, err)
		assert.True(t, exists, "the row exists — it is simply no longer parkable")
		assert.False(t, applied, "a park must not apply to a TERMINAL delivery")

		got, err := repo.Get(ctx, delActivityID, delTargetInbox)
		require.NoError(t, err)
		assert.Equal(t, DeliveryStateCancelled, got.State,
			"a late park must never resurrect a cancelled delivery back into the queue")
		assert.Equal(t, 1, got.Attempts, "and must not rewrite the attempt ledger of a decided row")
	})
}

func TestOutboundDeliveries_ParksAreNetZeroAndFailuresRetain(t *testing.T) {
	database := deliveryTestDB(t)
	activities := NewOutboundActivities(database)
	repo := NewOutboundDeliveries(database)
	ctx := context.Background()

	seedActivity(t, activities, testActivity())
	_, err := repo.Enqueue(ctx, testDelivery())
	require.NoError(t, err)

	// claimNow re-claims the delivery immediately; the releases below schedule
	// next_attempt_at in the past so no test ever waits on a backoff.
	claimNow := func(wantAttempts int, why string) time.Time {
		t.Helper()
		claimed, err := repo.ClaimNext(ctx, time.Minute)
		require.NoError(t, err, "re-claim: %s", why)
		require.NotNil(t, claimed.ClaimedUntil)
		require.Equalf(t, wantAttempts, claimed.Attempts, "attempts after the claim: %s", why)
		return *claimed.ClaimedUntil
	}
	past := func() time.Time { return time.Now().Add(-time.Second).UTC() }
	attempts := func() int { return parkedAttempts(t, repo) }

	// A REAL failure: the attempt is spent and stays spent.
	token := claimNow(1, "first attempt")
	_, applied, err := repo.Release(ctx, delActivityID, delTargetInbox, "5xx", "bad gateway", 502, past(), token)
	require.NoError(t, err)
	require.True(t, applied)
	require.Equal(t, 1, attempts(), "a genuine failure keeps its attempt")

	// A PARK on top of that history: net zero. The failure below it is not
	// forgotten, and the park itself leaves no trace in the budget.
	token = claimNow(2, "the park's own claim")
	_, applied, err = repo.ReleaseParked(ctx, delActivityID, delTargetInbox,
		"switch_parked", "kill switch", 0, past(), token)
	require.NoError(t, err)
	assert.True(t, applied, "the claim holder parks")
	require.Equal(t, 1, attempts(),
		"parks are net-zero and failures retain: after a park the ledger reads exactly the ONE real "+
			"failure that came before it — a hold neither spends budget nor erases history")

	// And the next real failure resumes the count where the failures left it.
	token = claimNow(2, "second real attempt")
	_, applied, err = repo.Release(ctx, delActivityID, delTargetInbox, "5xx", "bad gateway", 502, past(), token)
	require.NoError(t, err)
	require.True(t, applied)
	assert.Equal(t, 2, attempts(),
		"two real failures with a park between them cost exactly two attempts — an attempt cap counts "+
			"what was TRIED, never what was held")
}
