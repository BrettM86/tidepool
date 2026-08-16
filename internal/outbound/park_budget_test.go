package outbound

import (
	"context"
	"database/sql"
	"net/http"
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
	_, err := conn.ExecContext(context.Background(),
		`UPDATE outbound_deliveries SET next_attempt_at = now() WHERE activity_id = $1`, activityID)
	require.NoError(t, err)
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
	// MaxAttempts 3 is the package default; BackoffBase must be REAL time here
	// (the helper's 1ms default would put next_attempt_at in the past by the
	// time we read it, making the reschedule assertion vacuous).
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
