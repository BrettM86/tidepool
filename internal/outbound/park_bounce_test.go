package outbound

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// A park that the fence REFUSED is not a park. Both park helpers ask the store
// to hold the delivery and the store answers applied=false when the caller no
// longer owns the claim — and an operator reading tidepool_outbound_parked has
// no other way to tell a queue that is genuinely held from a stale worker
// shouting at a row somebody else moved on.
//
// The counter is the only observable here on purpose: the ROW is already
// correct in this scenario (the fence protects it, as the store tests pin), so
// the bounce is invisible everywhere except the metric. That is exactly the kind
// of miscount that makes a parked-delivery dashboard lie during an incident,
// which is when it is read.
func TestWorker_BouncedParkIsNotCounted(t *testing.T) {
	for _, tc := range []struct {
		name  string
		class string
		park  func(w *Worker, ctx context.Context, d *store.OutboundDelivery) error
	}{
		{
			name:  "switch park",
			class: "switch_parked",
			park: func(w *Worker, ctx context.Context, d *store.OutboundDelivery) error {
				return w.park(ctx, d, "switch_parked", "outbound kill switch engaged")
			},
		},
		{
			name:  "causal park",
			class: "parent_pending",
			park: func(w *Worker, ctx context.Context, d *store.OutboundDelivery) error {
				return w.parkCausal(ctx, d, "parent_pending", "waiting for bridge-origin parent",
					causalParkDelay)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := workerTestDB(t)
			ctx := context.Background()
			seedWorkerActor(t, conn, true, false)
			id := seedDelivery(t, conn, "Create", "", createPayload("x"))
			deliveries := store.NewOutboundDeliveries(conn)

			// A worker claims the delivery and then wedges.
			stale, err := deliveries.ClaimNext(ctx, time.Minute)
			require.NoError(t, err)
			require.NotNil(t, stale.ClaimedUntil)
			require.Equal(t, 1, stale.Attempts)

			// Its lease lapses and a second worker takes the row.
			_, err = conn.ExecContext(ctx,
				`UPDATE outbound_deliveries SET claimed_until = now() - interval '1 minute' WHERE activity_id = $1`, id)
			require.NoError(t, err)
			reclaimed, err := deliveries.ClaimNext(ctx, time.Minute)
			require.NoError(t, err)
			require.Equal(t, 2, reclaimed.Attempts, "the second worker owns the claim now")

			// The wedged worker wakes up holding a token nobody honours and
			// tries to park what it thinks is still its delivery.
			w := newWorker(t, conn, &fakeSender{}, nil)
			before := metricParked.Value()
			require.NoError(t, tc.park(w, ctx, stale))

			assert.Equal(t, before, metricParked.Value(),
				"a park the fence REFUSED must not be counted: tidepool_outbound_parked is how an "+
					"operator sizes the held queue, and a stale worker bouncing off the fence held nothing")

			got := getDelivery(t, conn, id)
			assert.Equal(t, 2, got.Attempts,
				"and the bounced park changes nothing about the row — the current claim's attempt stands")
			assert.Equal(t, store.DeliveryStatePending, got.State)
		})
	}
}
