package outbound

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// THE CAUSAL GATE HAS A THIRD TERMINAL PARENT, and it used to have no verdict
// for it.
//
// The gate reads three outcomes off a bridge-origin parent: accepted → the child
// is eligible, its delivery poisoned → the child is poisoned, anything else →
// wait. But a parent delivery can go terminal a THIRD way — CANCELLED, by the
// consent recheck at claim time, an operator cancel, or an actor/community
// sweep. A cancelled parent leaves the pending index, never gets accepted_at,
// and is not poisoned, so it fell into "anything else": wait forever.
//
// Forever is the cheap word for it. parkCausal released the child at
// next_attempt_at = now(), which makes it INSTANTLY re-claimable, and DeliverNext
// reports worked=true so Run never sleeps — a claim/park loop of two UPDATEs and
// two or three SELECTs per iteration, hammering Postgres at whatever rate the
// round trip allows, for the whole six-hour CausalWaitBudget. The trigger is
// ordinary: author A's comment is consent-cancelled at claim time while B's
// reply sits behind it on the same ordering key.
//
// The child is POISONED rather than cancelled, and the choice is about recovery
// rather than about tone. Cancelled is the queue's terminal state for "this must
// not go out", and it is unreachable afterwards: RedrivePoisoned matches only
// poisoned rows, so a cancelled child could never be revived. But the parent's
// own cancellation may well be redressed — a delivery_paused actor is a
// transient #account state, an operator cancel is reversed by re-enqueueing —
// and when it is, the operator needs both rows back. Poisoning also records WHY
// (last_error_class = parent_cancelled) where a cancel records nothing, and
// store.PoisonClassParentCancelled joins the never-reached-the-wire set so the
// divergence sweep does not report a delivery nobody ever sent as an unknown
// outcome.

// seedParentDeliveryInState writes the parent's OWN activity + delivery — the activity
// whose object.id maps back to gParentATURI, which is what the causal gate keys
// on — and drives it to the given terminal state.
func seedParentDeliveryInState(t *testing.T, conn *sql.DB, state store.DeliveryState) {
	t.Helper()
	ctx := context.Background()
	parentID := "https://coves.social/ap/activity/" + repeatHex64("cancelledparent")
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
	_, err = conn.ExecContext(ctx,
		`UPDATE outbound_deliveries SET state = $2 WHERE activity_id = $1`, parentID, string(state))
	require.NoError(t, err)
}

func TestCausalGating_CancelledParentDoesNotSpin(t *testing.T) {
	conn := workerTestDB(t)
	ctx := context.Background()
	seedWorkerActor(t, conn, true, false)
	seedBridgeParent(t, conn, false) // the parent object exists and was never accepted
	seedParentDeliveryInState(t, conn, store.DeliveryStateCancelled)

	id := seedDelivery(t, conn, "Create", gParentATURI, createPayload("child"))

	sender := &fakeSender{}
	w := newWorker(t, conn, sender, nil)

	worked, err := w.DeliverNext(ctx)
	require.NoError(t, err)
	require.True(t, worked)
	require.Zero(t, sender.count(), "a child of cancelled content is never POSTed")

	d := getDelivery(t, conn, id)
	assert.Equal(t, store.DeliveryStatePoisoned, d.State,
		"a parent whose delivery was CANCELLED is terminal-and-never-accepted: the child cannot "+
			"land behind it, so it is decided now rather than waiting for a parent that will "+
			"never arrive")
	assert.Equal(t, store.PoisonClassParentCancelled, d.LastErrorClass,
		"and it says WHY, distinctly from parent_poisoned (a failure) and parent_unaccepted "+
			"(a deadline) — the three reach the same state from different causes and an "+
			"operator triages them differently")

	// The spin itself: the second claim must find nothing. Before the fix the
	// child was released at now(), so this returned worked=true immediately and
	// went on doing so for the full six-hour budget.
	worked, err = w.DeliverNext(ctx)
	require.NoError(t, err)
	assert.False(t, worked,
		"and the queue is EMPTY afterwards: a child that stays instantly re-claimable turns "+
			"DeliverNext into a hot claim/park loop that Run never sleeps out of")
}

// The delay is the second half of the fix, and it stands on its own: whatever
// verdict the gate reaches, a child that is genuinely still WAITING must not be
// re-claimable at database speed. parkCausal used to stamp next_attempt_at =
// now(), which is a spin under any residual wait path.
func TestCausalGating_ParkedChildIsNotInstantlyReclaimable(t *testing.T) {
	conn := workerTestDB(t)
	ctx := context.Background()
	seedWorkerActor(t, conn, true, false)
	seedBridgeParent(t, conn, false) // merely pending: the child legitimately waits
	id := seedDelivery(t, conn, "Create", gParentATURI, createPayload("child"))

	w := newWorker(t, conn, &fakeSender{}, nil)
	worked, err := w.DeliverNext(ctx)
	require.NoError(t, err)
	require.True(t, worked)

	d := getDelivery(t, conn, id)
	require.Equal(t, store.DeliveryStatePending, d.State, "it is held, not decided")
	assert.True(t, d.NextAttemptAt.After(time.Now()),
		"a causal park schedules the child a real interval out: at now() the very next claim "+
			"picks it straight back up, and the loop runs at whatever rate the round trip allows")

	worked, err = w.DeliverNext(ctx)
	require.NoError(t, err)
	assert.False(t, worked,
		"so back-to-back claims find nothing — which is what turns the wait from a busy loop "+
			"into a wait")
}

// failingObjects is the real store with its parent lookup broken: a connection
// blip, a statement timeout, the transient the gate is meant to hold through.
type failingObjects struct {
	store.OutboundObjects
}

func (failingObjects) GetByATURI(context.Context, string) (*store.OutboundObject, error) {
	return nil, fmt.Errorf("dial tcp: connection reset by peer")
}

// A lookup ERROR takes the same "hold rather than poison" verdict a pending
// parent does — correctly, since a blip must not poison anybody's reply — and so
// it inherited the same zero-delay release. The result is worse than the
// cancelled-parent spin: it is a hot loop against a database that is ALREADY in
// trouble, for every gated delivery at once.
func TestCausalGating_ParentLookupErrorBacksOff(t *testing.T) {
	conn := workerTestDB(t)
	ctx := context.Background()
	seedWorkerActor(t, conn, true, false)
	seedBridgeParent(t, conn, false)
	id := seedDelivery(t, conn, "Create", gParentATURI, createPayload("child"))

	sender := &fakeSender{}
	// A REAL backoff base: the helper's 1ms would put next_attempt_at in the past
	// before the row could be read, making the assertion vacuous.
	w := newWorker(t, conn, sender, func(o *WorkerOptions) {
		o.Objects = failingObjects{OutboundObjects: store.NewOutboundObjects(conn)}
		o.BackoffBase = 30 * time.Second
	})

	worked, err := w.DeliverNext(ctx)
	require.NoError(t, err)
	require.True(t, worked)
	require.Zero(t, sender.count(),
		"a delivery whose causal eligibility could not be READ is not delivered on a guess")

	d := getDelivery(t, conn, id)
	assert.Equal(t, store.DeliveryStatePending, d.State,
		"a store blip holds the delivery — it must never poison somebody's reply")
	assert.Equal(t, "parent_lookup_failed", d.LastErrorClass,
		"and it is held under its own class, so an operator can tell a database problem from a "+
			"parent that is simply a beat behind")
	assert.True(t, d.NextAttemptAt.After(time.Now().Add(time.Second)),
		"with a REAL backoff: re-asking a database that just failed, as fast as the round trip "+
			"allows, is the loop that turns a blip into an outage")
	assert.Zero(t, d.Attempts,
		"and the hold is still attempt-neutral — the delivery was never tried")
}

// The whole reason the cancelled parent had no verdict is that the gate could
// only ask one question of the parent's delivery. This pins the store answer
// underneath the worker: the three dispositions must come apart, and an ordinary
// pending parent must read as neither.
func TestParentDeliveryDisposition_SeparatesPoisonedFromCancelled(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state store.DeliveryState
		want  store.ParentDeliveryDisposition
	}{
		{"pending", store.DeliveryStatePending, store.ParentDeliveryOpen},
		{"delivered", store.DeliveryStateDelivered, store.ParentDeliveryOpen},
		{"poisoned", store.DeliveryStatePoisoned, store.ParentDeliveryPoisoned},
		{"cancelled", store.DeliveryStateCancelled, store.ParentDeliveryCancelled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := workerTestDB(t)
			ctx := context.Background()
			seedWorkerActor(t, conn, true, false)
			seedParentDeliveryInState(t, conn, tc.state)

			got, err := store.NewOutboundDeliveries(conn).
				ParentDeliveryDisposition(ctx, gParentATURI, wInbox)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// A parent with NO delivery row at all reads open, not cancelled: "every row is
// cancelled" over an empty set is vacuously true, and taking that as a verdict
// would poison every child of a fediverse-origin parent the moment the worker
// asked.
func TestParentDeliveryDisposition_NoParentRowIsOpen(t *testing.T) {
	conn := workerTestDB(t)
	got, err := store.NewOutboundDeliveries(conn).
		ParentDeliveryDisposition(context.Background(), gParentATURI, wInbox)
	require.NoError(t, err)
	assert.Equal(t, store.ParentDeliveryOpen, got,
		"no parent delivery is not a cancelled parent — an empty set must not answer a "+
			"question about what happened to the parent")
}

// One object can be federated by more than one activity (a Create and a later
// Update carry the same object.id), so the parent's disposition is read across
// ALL of them. A single cancelled row beside a live one does not condemn the
// child: something can still make the parent land.
func TestParentDeliveryDisposition_ALiveSiblingKeepsTheParentOpen(t *testing.T) {
	conn := workerTestDB(t)
	ctx := context.Background()
	seedWorkerActor(t, conn, true, false)
	seedParentDeliveryInState(t, conn, store.DeliveryStateCancelled)

	// The Update of the same object, still pending.
	updateID := "https://coves.social/ap/activity/" + repeatHex64("parentupdate")
	_, err := store.NewOutboundActivities(conn).Insert(ctx, store.OutboundActivity{
		ActivityID: updateID,
		ActorDID:   wActorDID,
		Kind:       "Update",
		Payload: []byte(fmt.Sprintf(`{"id":%q,"type":"Update","object":{"type":"Page","id":%q}}`,
			updateID, objectURLFor(gParentATURI))),
	})
	require.NoError(t, err)
	_, err = store.NewOutboundDeliveries(conn).Enqueue(ctx, store.OutboundDelivery{
		ActivityID: updateID, TargetInbox: wInbox, OrderingKey: wCommunityAPID,
	})
	require.NoError(t, err)

	got, err := store.NewOutboundDeliveries(conn).ParentDeliveryDisposition(ctx, gParentATURI, wInbox)
	require.NoError(t, err)
	assert.Equal(t, store.ParentDeliveryOpen, got,
		"one cancelled activity for the parent object is not the parent's fate while another is "+
			"still in flight — the child waits rather than being poisoned out from under a "+
			"delivery that may still land")

	// Poison still wins outright, which is the pre-existing contract: a poisoned
	// parent delivery poisons the child whatever else is on the object.
	_, err = conn.ExecContext(ctx,
		`UPDATE outbound_deliveries SET state = 'poisoned' WHERE activity_id = $1`, updateID)
	require.NoError(t, err)
	got, err = store.NewOutboundDeliveries(conn).ParentDeliveryDisposition(ctx, gParentATURI, wInbox)
	require.NoError(t, err)
	assert.Equal(t, store.ParentDeliveryPoisoned, got,
		"and a poisoned row outranks a cancelled one: parent_poisoned is the louder verdict and "+
			"the pre-existing one")
}

// A guard on the assumption the fix rests on: errors.IsNotFound is what the gate
// reads to call a parent fediverse-origin, and the disposition query must not
// change that path.
func TestCausalGating_CancelledParentStillEligibleWhenObjectIsFediverseOrigin(t *testing.T) {
	conn := workerTestDB(t)
	ctx := context.Background()
	seedWorkerActor(t, conn, true, false)
	// No outbound_objects row: a Lemmy post. Whatever deliveries exist against
	// that at-uri, the reply is eligible — the object is already on the peer.
	_, err := store.NewOutboundObjects(conn).GetByATURI(ctx, gParentATURI)
	require.True(t, errors.IsNotFound(err))
	seedParentDeliveryInState(t, conn, store.DeliveryStateCancelled)

	id := seedDelivery(t, conn, "Create", gParentATURI, createPayload("child"))
	sender := &fakeSender{}
	w := newWorker(t, conn, sender, nil)
	worked, err := w.DeliverNext(ctx)
	require.NoError(t, err)
	require.True(t, worked)

	assert.Equal(t, 1, sender.count(),
		"a reply whose parent has no outbound_objects row delivers immediately — the "+
			"cancelled-parent verdict must not reach past the bridge-origin test")
	assert.Equal(t, store.DeliveryStateDelivered, getDelivery(t, conn, id).State)
}
