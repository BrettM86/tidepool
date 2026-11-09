package outbound

import (
	"context"
	"database/sql"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// The park/poison budget. A kill switch is an OPERATOR PAUSE: it exists to keep
// deliveries safe while something is wrong. It must therefore cost the paused
// delivery nothing — the retries it still has when the switch clears must be
// exactly the retries it had when the switch engaged.
//
// This is the outer, operator-visible framing of that invariant: it drives the
// worker through the full claim → park → re-claim cycle against the real store,
// so it observes the budget the way an operator does (the row's state after the
// switch lifts), not the way the worker's internals count attempts.

// clearParkDelay makes a parked row immediately re-claimable by rewinding ONLY
// its schedule — the house idiom for time control (cf. setAttempts), so a test
// never sleeps out parkDelay.
//
// It deliberately touches nothing but next_attempt_at: resetting `state` too
// (as resetDeliveryToPending does) would revive a poisoned row and hide the very
// failure this test is here to catch.
func clearParkDelay(t *testing.T, conn *sql.DB, activityID string) {
	t.Helper()
	result, err := conn.ExecContext(context.Background(),
		`UPDATE outbound_deliveries SET next_attempt_at = now() WHERE activity_id = $1`, activityID)
	require.NoError(t, err)
	// A helper that silently matches no row would turn every caller's loop into
	// a claim that never happens, failing somewhere far from the drift that
	// caused it.
	affected, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), affected, "clearParkDelay must rewind exactly the delivery under test")
}

func TestWorker_KillSwitchDoesNotSpendPoisonBudget(t *testing.T) {
	conn := workerTestDB(t)
	seedWorkerActor(t, conn, true, false)
	id := seedDelivery(t, conn, "Create", "", createPayload("x"))

	switches := &fakeSwitches{allow: false}
	// Exactly one retryable failure after the switch lifts: a 503 is the
	// canonical "come back later", so the delivery's fate here is decided
	// purely by whether it still has budget to come back with.
	sender := &fakeSender{respond: func(call int, _ string) error {
		if call == 1 {
			return httpErr(http.StatusServiceUnavailable, "peer restarting")
		}
		return nil
	}}
	// Both of these are spelled out rather than inherited: MaxAttempts 3 is the
	// TEST HELPER's default (the package default is 8), and a helper default is
	// not a contract this test should silently depend on. BackoffBase must be
	// REAL time here — the helper's 1ms would put next_attempt_at in the past by
	// the time we read it, making the reschedule assertion vacuous.
	w := newWorker(t, conn, sender, func(o *WorkerOptions) {
		o.Switches = switches
		o.MaxAttempts = 3
		o.BackoffBase = 30 * time.Second
	})

	ctx := context.Background()

	// Well past MaxAttempts worth of parks: an operator block held for a minute
	// is an ordinary Tuesday, and the worker re-claims a parked row every
	// parkDelay for as long as it lasts.
	for i := 0; i < 6; i++ {
		worked, err := w.DeliverNext(ctx)
		require.NoError(t, err)
		require.Truef(t, worked, "cycle %d: the parked delivery is still claimable", i)
		require.Equalf(t, store.DeliveryStatePending, getDelivery(t, conn, id).State,
			"cycle %d: parking never leaves the pending state", i)
		clearParkDelay(t, conn, id)
	}
	require.Zero(t, sender.count(), "nothing is POSTed while the switch is engaged")

	// The operator lifts the block. The queue should resume exactly where it
	// paused.
	switches.allow = true

	worked, err := w.DeliverNext(ctx)
	require.NoError(t, err)
	require.True(t, worked)
	require.Equal(t, 1, sender.count(), "the delivery is POSTed once the switch clears")

	d := getDelivery(t, conn, id)
	assert.Equal(t, store.DeliveryStatePending, d.State,
		"a kill switch must not destroy the retries of the deliveries it exists to protect: "+
			"after the switch lifts, one retryable 503 is a RETRY, not a poisoning — "+
			"time spent parked is not an attempt")
	assert.True(t, d.NextAttemptAt.After(time.Now()),
		"the delivery is rescheduled into the future for its retry — an operator who lifts a "+
			"kill switch gets their queue back, not a dead-letter pile to redrive by hand")
}

// The media wait is the same promise as the kill switch from the other side: a
// delivery held while the image proxy answers 404 must come out of the hold
// with every retry it went in with. A post whose images the AppView indexes
// slowly is otherwise poisoned by its first real 503.
func TestWorker_MediaHoldDoesNotSpendPoisonBudget(t *testing.T) {
	conn := workerTestDB(t)
	seedWorkerActor(t, conn, true, false)

	var proxyStatus atomic.Int32
	proxyStatus.Store(http.StatusNotFound)
	proxy := newParkImageProxy(t, &proxyStatus)
	id := seedDelivery(t, conn, "Create", "", parkMediaPayload(proxy.URL))

	// Exactly one retryable failure once the image is served, as in the kill
	// switch test above.
	sender := &fakeSender{respond: func(call int, _ string) error {
		if call == 1 {
			return httpErr(http.StatusServiceUnavailable, "peer restarting")
		}
		return nil
	}}
	w := newWorker(t, conn, sender, func(o *WorkerOptions) {
		withParkMediaGate(proxy.URL)(o)
		o.MaxAttempts = 3
		o.BackoffBase = 30 * time.Second
	})

	ctx := context.Background()

	// Twice MaxAttempts worth of media parks.
	for i := 0; i < 6; i++ {
		worked, err := w.DeliverNext(ctx)
		require.NoError(t, err)
		require.Truef(t, worked, "cycle %d: the media-held delivery is still claimable", i)
		d := getDelivery(t, conn, id)
		require.Equalf(t, store.DeliveryStatePending, d.State, "cycle %d: a media park stays pending", i)
		require.Equalf(t, "media_pending", d.LastErrorClass, "cycle %d: the hold is a media park", i)
		clearParkDelay(t, conn, id)
	}
	require.Zero(t, sender.count(), "nothing is POSTed while the image is unserved")

	// The AppView indexes the image.
	proxyStatus.Store(http.StatusOK)

	worked, err := w.DeliverNext(ctx)
	require.NoError(t, err)
	require.True(t, worked)
	require.Equal(t, 1, sender.count(), "the delivery is POSTed once the image is served")

	d := getDelivery(t, conn, id)
	assert.Equal(t, store.DeliveryStatePending, d.State,
		"six media parks cost the delivery nothing: after the image is served, one retryable 503 "+
			"is a RETRY, not a poisoning")
	assert.Equal(t, 1, d.Attempts, "the only attempt on the ledger is the POST that got the 503")
	assert.True(t, d.NextAttemptAt.After(time.Now()),
		"the delivery is rescheduled into the future for its retry")
}
