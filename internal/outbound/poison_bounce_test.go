package outbound

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// The poison counter was the last outcome recorder left ungated by `applied`.
//
// MarkPoisoned carries the same (exists, applied) fence as every other terminal
// mark — a stale worker whose lease lapsed writes zero rows and is told so — and
// delivered, cancelled and parked all gate their metric on that answer.
// tidepool_outbound_poisoned did not: it bumped whether or not anything was
// poisoned.
//
// It is the counter operators are told to read as the queue's verdict (DEPLOY.md
// walks a redrive off it), and a dead-letter number that climbs while nothing is
// dead-lettered sends an incident to the wrong place — the more so because the
// row itself is fine here, so the miscount is the only trace the bounce leaves.
// Cf. TestWorker_BouncedParkIsNotCounted, which is the same rule one state over.
func TestWorker_BouncedPoisonIsNotCounted(t *testing.T) {
	conn := workerTestDB(t)
	ctx := context.Background()
	seedWorkerActor(t, conn, true, false)
	id := seedDelivery(t, conn, "Create", "", createPayload("x"))
	deliveries := store.NewOutboundDeliveries(conn)

	// A worker claims the delivery and then wedges.
	stale, err := deliveries.ClaimNext(ctx, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, stale.ClaimedUntil)

	// Its lease lapses and a second worker takes the row.
	_, err = conn.ExecContext(ctx,
		`UPDATE outbound_deliveries SET claimed_until = now() - interval '1 minute' WHERE activity_id = $1`, id)
	require.NoError(t, err)
	reclaimed, err := deliveries.ClaimNext(ctx, time.Minute)
	require.NoError(t, err)
	require.Equal(t, 2, reclaimed.Attempts, "the second worker owns the claim now")

	// The wedged worker wakes up holding a token nobody honours and tries to
	// dead-letter what it thinks is still its delivery.
	w := newWorker(t, conn, &fakeSender{}, nil)
	before := metricPoisoned.Value()
	require.NoError(t, w.poison(ctx, stale, "4xx", "rejected", 400))

	assert.Equal(t, before, metricPoisoned.Value(),
		"a poison the fence REFUSED must not be counted: tidepool_outbound_poisoned is the "+
			"number an operator reads as the queue's verdict, and a stale worker bouncing off "+
			"a claim somebody else owns dead-lettered nothing")

	got := getDelivery(t, conn, id)
	assert.Equal(t, store.DeliveryStatePending, got.State,
		"and the row is untouched — the fence saw to that, which is exactly why the miscount is "+
			"the only trace the bounce leaves")
}
