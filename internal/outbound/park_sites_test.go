package outbound

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// Every place the worker HOLDS a delivery instead of trying it must cost that
// delivery nothing. There are three such sites — the kill switch, dry-run, and
// the causal wait — and they are one behavior, not three: a hold is not an
// attempt, whatever the reason for holding.
//
// Pinning all three together is deliberate. The switch site is the one an
// operator feels (see the outer acceptance test), but a per-site fix would leave
// dry-run silently burning the budget of a queue nobody is watching, and the
// causal wait burning the budget of every reply whose parent is a beat behind.

func TestWorker_ParkSitesAreAttemptNeutral(t *testing.T) {
	for _, tc := range []struct {
		name        string
		class       string
		parentATURI string
		seed        func(t *testing.T, conn *sql.DB)
		opts        func(*WorkerOptions)
	}{
		{
			name:  "kill switch",
			class: "switch_parked",
			opts:  func(o *WorkerOptions) { o.Switches = &fakeSwitches{allow: false} },
		},
		{
			name:  "dry run",
			class: "dry_run",
			opts:  func(o *WorkerOptions) { o.Switches = &fakeSwitches{allow: true, dryRun: true} },
		},
		{
			name:  "causal wait",
			class: "parent_pending",
			// A bridge-origin parent that has not been accepted yet: the reply
			// is held until it lands (see causal_gating_test.go).
			parentATURI: gParentATURI,
			seed:        func(t *testing.T, conn *sql.DB) { seedBridgeParent(t, conn, false) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := workerTestDB(t)
			seedWorkerActor(t, conn, true, false)
			if tc.seed != nil {
				tc.seed(t, conn)
			}
			id := seedDelivery(t, conn, "Create", tc.parentATURI, createPayload("x"))
			require.Zero(t, getDelivery(t, conn, id).Attempts,
				"a freshly enqueued delivery has spent nothing yet — this is the value a park must restore")

			sender := &fakeSender{}
			w := newWorker(t, conn, sender, tc.opts)

			worked, err := w.DeliverNext(context.Background())
			require.NoError(t, err)
			require.True(t, worked, "the delivery was claimed and handled")
			require.Zero(t, sender.count(), "a park POSTs nothing — the delivery was HELD, not tried")

			got := getDelivery(t, conn, id)
			assert.Equal(t, 0, got.Attempts,
				"a full claim/park cycle leaves the attempt ledger where it started: the hold gives back "+
					"the increment its own claim took, so the delivery arrives at the attempt cap only "+
					"through attempts it actually made")
			assert.Equal(t, store.DeliveryStatePending, got.State,
				"a parked delivery stays pending — holding is not failing")
			assert.Equal(t, tc.class, got.LastErrorClass,
				"the reason for the hold is recorded, so the give-back is auditable rather than a silent rewind")
		})
	}
}

// The causal wait is the one hold whose OUTCOME is not "wait forever": it
// poisons on a wall-clock deadline. That makes it the site where the two
// mechanisms could quietly be wired to each other — and they must not be.
//
// This guard holds both facts down at once, because each is a plausible way to
// break the other: an attempt-neutral park built by suppressing the release
// bookkeeping would take the causal deadline's accounting with it (the child
// waits forever on a parent that never lands), and a deadline defended by
// leaning on the attempt counter would put the budget back in the park's hands.
// Neither passes this test. Cf. causal_hardening_test.go, which pins the same
// deadline from the other direction (attempts=99 must NOT poison early).
func TestCausalGating_ParksAreFreeButTheWallClockStillPoisons(t *testing.T) {
	conn := workerTestDB(t)
	ctx := context.Background()
	seedWorkerActor(t, conn, true, false)
	seedBridgeParent(t, conn, false) // parent never accepted
	id := seedDelivery(t, conn, "Create", gParentATURI, createPayload("x"))

	sender := &fakeSender{}
	w := newWorker(t, conn, sender, func(o *WorkerOptions) { o.CausalWaitBudget = time.Hour })

	// The spin a child does while its parent is in flight. parkCausal stamps
	// next_attempt_at from the APP clock while ClaimNext compares against the
	// DB's, so on a host running a few hundred microseconds ahead of Postgres a
	// back-to-back re-claim can miss by a hair; the nudge re-stamps the schedule
	// from the DB's own now() so each cycle here is deterministic. It is the
	// schedule column only — a poison would still be plainly visible.
	for i := 0; i < 5; i++ {
		worked, err := w.DeliverNext(ctx)
		require.NoError(t, err)
		require.Truef(t, worked, "cycle %d: the held child stays claimable", i)
		clearParkDelay(t, conn, id)
	}
	require.Zero(t, sender.count(), "a held child is never POSTed")

	held := getDelivery(t, conn, id)
	assert.Equal(t, 0, held.Attempts,
		"five causal parks cost the child nothing: the retries it will need once its parent lands "+
			"are still there")
	assert.Equal(t, store.DeliveryStatePending, held.State, "and it is still waiting, not decided")

	// Now the wall clock, and only the wall clock, decides. The attempt ledger
	// reads zero — a counter-driven deadline would wait forever here.
	_, err := conn.ExecContext(ctx,
		`UPDATE outbound_deliveries SET created_at = now() - interval '2 hours' WHERE activity_id = $1`, id)
	require.NoError(t, err)

	worked, err := w.DeliverNext(ctx)
	require.NoError(t, err)
	require.True(t, worked)

	expired := getDelivery(t, conn, id)
	assert.Equal(t, store.DeliveryStatePoisoned, expired.State,
		"the causal budget is bounded by ELAPSED TIME, not by attempts: a parent that never landed "+
			"still poisons its child on the deadline, however cheap the waiting was")
	assert.Equal(t, store.PoisonClassParentUnaccepted, expired.LastErrorClass,
		"and it poisons for the causal reason, so the outcome stays queryable and redrivable")
}
