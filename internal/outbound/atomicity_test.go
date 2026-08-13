package outbound

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// Second-opinion H5: the causal forward edge (a bridge-origin parent's delivery
// stamps accepted_at, releasing its held children) must be driven end-to-end,
// and the stamp must be ATOMIC with MarkDelivered — a delivered-but-unaccepted
// parent strands every child forever.

// pageParentPayload is a Create{Page} whose object.id maps back to atURI, so
// stampAccepted resolves the object it federated.
func pageParentPayload(atURI string) []byte {
	return []byte(fmt.Sprintf(
		`{"id":"https://coves.social/ap/activity/parent","type":"Create",`+
			`"object":{"type":"Page","id":%q}}`, objectURLFor(atURI)))
}

func seedParentDelivery(t *testing.T, conn *sql.DB, payload []byte) string {
	t.Helper()
	ctx := context.Background()
	activityID := "https://coves.social/ap/activity/" + repeatHex64("parentpage")
	_, err := store.NewOutboundActivities(conn).Insert(ctx, store.OutboundActivity{
		ActivityID: activityID, ActorDID: wActorDID, Kind: "Create", Payload: payload,
	})
	require.NoError(t, err)
	_, err = store.NewOutboundDeliveries(conn).Enqueue(ctx, store.OutboundDelivery{
		ActivityID: activityID, TargetInbox: wInbox, OrderingKey: wCommunityAPID,
	})
	require.NoError(t, err)
	return activityID
}

func rawAccepted(t *testing.T, conn *sql.DB, atURI string) bool {
	t.Helper()
	var acceptedAt sql.NullTime
	require.NoError(t, conn.QueryRowContext(context.Background(),
		`SELECT accepted_at FROM outbound_objects WHERE at_uri = $1`, atURI).Scan(&acceptedAt))
	return acceptedAt.Valid
}

func TestWorker_ParentDeliverySuccessAcceptsAndReleasesHeldChild(t *testing.T) {
	conn := workerTestDB(t)
	ctx := context.Background()
	seedWorkerActor(t, conn, true, false)
	seedBridgeParent(t, conn, false) // parent object, accepted_at NULL

	seedParentDelivery(t, conn, pageParentPayload(gParentATURI))                     // seq 1: the parent
	childID := seedDelivery(t, conn, "Create", gParentATURI, createPayload("child")) // seq 2: held child

	w := newWorker(t, conn, &fakeSender{}, nil)

	// 1) Deliver the parent (the head). This must stamp accepted_at.
	worked, err := w.DeliverNext(ctx)
	require.NoError(t, err)
	require.True(t, worked)
	assert.True(t, rawAccepted(t, conn, gParentATURI),
		"a successful bridge-origin parent delivery stamps its outbound_objects.accepted_at")

	// 2) The previously-held child is now eligible and delivers — the full
	//    forward edge the test-analyzer flagged as untested.
	worked, err = w.DeliverNext(ctx)
	require.NoError(t, err)
	require.True(t, worked)
	assert.Equal(t, store.DeliveryStateDelivered, getDelivery(t, conn, childID).State,
		"once the parent is accepted, the held child delivers end-to-end")
}

// faultObjects fails SetAccepted to prove the stamp rides the SAME committed
// transition as MarkDelivered.
type faultObjects struct {
	store.OutboundObjects
	failSetAccepted bool
}

func (o *faultObjects) SetAccepted(ctx context.Context, atURI string) error {
	if o.failSetAccepted {
		return fmt.Errorf("injected: SetAccepted failed")
	}
	return o.OutboundObjects.SetAccepted(ctx, atURI)
}

func TestWorker_AcceptedStampIsAtomicWithMarkDelivered(t *testing.T) {
	conn := workerTestDB(t)
	ctx := context.Background()
	seedWorkerActor(t, conn, true, false)
	seedBridgeParent(t, conn, false)
	parentID := seedParentDelivery(t, conn, pageParentPayload(gParentATURI))

	objects := &faultObjects{OutboundObjects: store.NewOutboundObjects(conn), failSetAccepted: true}
	w := newWorker(t, conn, &fakeSender{}, func(o *WorkerOptions) { o.Objects = objects })

	// The POST succeeds but the accepted_at stamp fails.
	_, _ = w.DeliverNext(ctx)

	// Consistency invariant: the delivery must NEVER be 'delivered' while the
	// object is unaccepted. If the stamp cannot commit, the delivered mark must
	// roll back with it, so a retry re-runs both — otherwise the parent is
	// delivered-but-unaccepted and every child is stranded forever.
	deliveredButUnaccepted := getDelivery(t, conn, parentID).State == store.DeliveryStateDelivered &&
		!rawAccepted(t, conn, gParentATURI)
	assert.False(t, deliveredButUnaccepted,
		"MarkDelivered and SetAccepted must be atomic: a parent must never be delivered while "+
			"its accepted_at is unset (that strands every child)")
}
