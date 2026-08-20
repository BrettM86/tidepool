package outbound

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// Second-opinion H4b + H6: causal gating must be TIME-bounded (not attempt-
// bounded) and keyed on the child's ACTUAL parent (not any lower-seq poison on
// the serial line).

func objectURLFor(atURI string) string {
	return "https://coves.social/ap/object/" + strings.TrimPrefix(atURI, "at://")
}

// ---------------------------------------------------------------------------
// H4b: causal wait is wall-clock-bounded, not attempt-bounded
// ---------------------------------------------------------------------------

func TestCausalGating_ManyAttemptsWithinBudgetStayPending(t *testing.T) {
	conn := workerTestDB(t)
	seedWorkerActor(t, conn, true, false)
	seedBridgeParent(t, conn, false) // parent never accepted
	id := seedDelivery(t, conn, "Create", gParentATURI, createPayload("x"))

	// Attempts far past the delivery-failure cap, but the delivery was created
	// moments ago and the causal budget is an hour: a parent legitimately taking
	// minutes to be admitted must NOT be poisoned just because the child was
	// claimed a few times. The attempts counter must NOT drive the causal wait.
	setAttempts(t, conn, id, 99)

	w := newWorker(t, conn, &fakeSender{}, nil) // CausalWaitBudget defaults to 1h in the helper
	_, err := w.DeliverNext(context.Background())
	require.NoError(t, err)

	assert.Equal(t, store.DeliveryStatePending, getDelivery(t, conn, id).State,
		"a causal wait is bounded by WALL CLOCK, not attempts — a recent delivery stays pending "+
			"no matter how many times it was claimed")
}

func TestCausalGating_PoisonsOnlyAfterWallClockDeadline(t *testing.T) {
	conn := workerTestDB(t)
	seedWorkerActor(t, conn, true, false)
	seedBridgeParent(t, conn, false)
	id := seedDelivery(t, conn, "Create", gParentATURI, createPayload("x"))

	// The delivery is older than the causal budget: NOW the parent has provably
	// never landed, so it poisons — but only because wall-clock time passed, not
	// because of the attempt count (which is zero here).
	_, err := conn.ExecContext(context.Background(),
		`UPDATE outbound_deliveries SET created_at = now() - interval '2 hours' WHERE activity_id = $1`, id)
	require.NoError(t, err)

	w := newWorker(t, conn, &fakeSender{}, func(o *WorkerOptions) { o.CausalWaitBudget = time.Hour })
	_, err = w.DeliverNext(context.Background())
	require.NoError(t, err)

	d := getDelivery(t, conn, id)
	assert.Equal(t, store.DeliveryStatePoisoned, d.State,
		"once the wall-clock causal budget is exceeded, a never-accepted parent poisons the child")
	assert.Contains(t, d.LastErrorClass, "parent_unaccepted",
		"the poison reason is queryable: parent_unaccepted")
}

// ---------------------------------------------------------------------------
// H6: the gate keys on the child's ACTUAL parent, not any lower-seq poison
// ---------------------------------------------------------------------------

func TestCausalGating_UnrelatedPoisonDoesNotPoisonChild(t *testing.T) {
	conn := workerTestDB(t)
	ctx := context.Background()
	seedWorkerActor(t, conn, true, false)
	seedBridgeParent(t, conn, false) // the child's ACTUAL parent P: pending, not poisoned

	// An UNRELATED poisoned activity on the same community/inbox line — its
	// federated object is a DIFFERENT at-uri, nothing to do with this child.
	unrelatedID := "https://coves.social/ap/activity/" + repeatHex64("unrelated")
	_, err := store.NewOutboundActivities(conn).Insert(ctx, store.OutboundActivity{
		ActivityID: unrelatedID,
		ActorDID:   wActorDID,
		Kind:       "Create",
		Payload: []byte(fmt.Sprintf(`{"id":%q,"type":"Create","object":{"type":"Note","id":%q}}`,
			unrelatedID, objectURLFor("at://did:plc:someoneelse/social.coves.community.comment/other"))),
	})
	require.NoError(t, err)
	_, err = store.NewOutboundDeliveries(conn).Enqueue(ctx, store.OutboundDelivery{
		ActivityID: unrelatedID, TargetInbox: wInbox, OrderingKey: wCommunityAPID,
	})
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `UPDATE outbound_deliveries SET state='poisoned' WHERE activity_id=$1`, unrelatedID)
	require.NoError(t, err)

	// The child, whose actual parent (gParentATURI) is merely PENDING.
	id := seedDelivery(t, conn, "Create", gParentATURI, createPayload("child"))

	w := newWorker(t, conn, &fakeSender{}, nil)
	_, err = w.DeliverNext(ctx)
	require.NoError(t, err)

	assert.Equal(t, store.DeliveryStatePending, getDelivery(t, conn, id).State,
		"a poisoned UNRELATED activity on the same line must NOT poison this child — its actual "+
			"parent is only pending, so it causal-WAITS (keying on 'any lower-seq poison' is the bug)")
}

func TestCausalGating_ActualParentPoisonPoisonsChild(t *testing.T) {
	conn := workerTestDB(t)
	ctx := context.Background()
	seedWorkerActor(t, conn, true, false)
	seedBridgeParent(t, conn, false) // parent object exists, not accepted

	// The child's ACTUAL parent (gParentATURI) has a POISONED delivery: its
	// federated object.id maps back to gParentATURI.
	parentID := "https://coves.social/ap/activity/" + repeatHex64("realparent")
	_, err := store.NewOutboundActivities(conn).Insert(ctx, store.OutboundActivity{
		ActivityID: parentID,
		ActorDID:   wActorDID,
		Kind:       "Create",
		Payload: []byte(fmt.Sprintf(`{"id":%q,"type":"Create","object":{"type":"Page","id":%q}}`,
			parentID, objectURLFor(gParentATURI))),
	})
	require.NoError(t, err)
	_, err = store.NewOutboundDeliveries(conn).Enqueue(ctx, store.OutboundDelivery{
		ActivityID: parentID, TargetInbox: wInbox, OrderingKey: wCommunityAPID,
	})
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `UPDATE outbound_deliveries SET state='poisoned' WHERE activity_id=$1`, parentID)
	require.NoError(t, err)

	id := seedDelivery(t, conn, "Create", gParentATURI, createPayload("child"))

	w := newWorker(t, conn, &fakeSender{}, nil)
	_, err = w.DeliverNext(ctx)
	require.NoError(t, err)

	d := getDelivery(t, conn, id)
	assert.Equal(t, store.DeliveryStatePoisoned, d.State,
		"when the child's ACTUAL parent delivery is poisoned, the child poisons")
	assert.Contains(t, d.LastErrorClass, "parent_poisoned",
		"the reason is parent_poisoned — distinct from parent_unaccepted")
}
