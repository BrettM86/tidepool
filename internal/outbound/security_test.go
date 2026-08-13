package outbound

import (
	"context"
	"database/sql"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/store"
)

// Second-opinion H1 (inbox SSRF), H4a (park backoff), + importants
// (401-not-rotation, case-insensitive host switch).

// ---------------------------------------------------------------------------
// H1: inbox SSRF — a Group doc must not be able to point delivery at a foreign
// host.
// ---------------------------------------------------------------------------

func TestInboxResolver_RejectsCrossAuthorityInbox(t *testing.T) {
	// The community lives on lemmy.world but its Group doc advertises an inbox
	// on an attacker host. Delivering there would let any community redirect a
	// signed activity to an arbitrary origin.
	fetcher := &countingFetcher{doc: &ap.Object{
		ID:        wCommunityAPID, // https://lemmy.world/c/tech
		Type:      "Group",
		Inbox:     "https://evil.example/inbox",
		Endpoints: &ap.Endpoints{SharedInbox: "https://evil.example/inbox"},
	}}
	resolver := NewInboxResolver(fetcher, time.Minute)

	_, err := resolver.ResolveInbox(context.Background(), wCommunityAPID)
	require.Error(t, err,
		"a resolved inbox whose host is not same-authority with the community must be REFUSED, "+
			"not returned as the delivery target")
}

func TestInboxResolver_AcceptsSameAuthorityInbox(t *testing.T) {
	shared := "https://lemmy.world/c/tech/inbox"
	fetcher := &countingFetcher{doc: &ap.Object{
		ID:        wCommunityAPID,
		Type:      "Group",
		Inbox:     shared,
		Endpoints: &ap.Endpoints{SharedInbox: shared},
	}}
	resolver := NewInboxResolver(fetcher, time.Minute)

	inbox, err := resolver.ResolveInbox(context.Background(), wCommunityAPID)
	require.NoError(t, err, "a same-authority inbox is fine")
	assert.Equal(t, shared, inbox)
}

func TestWorker_DoesNotDeliverToCrossAuthorityInbox(t *testing.T) {
	conn := workerTestDB(t)
	seedWorkerActor(t, conn, true, false)
	// A delivery whose stored target_inbox is on a DIFFERENT host than its
	// community (ordering key) — a poisoned/tampered enqueue must never cause a
	// signed POST to the foreign host.
	id := seedDeliveryTo(t, conn, "Create", "https://evil.example/inbox")

	sender := &fakeSender{}
	w := newWorker(t, conn, sender, nil)
	_, err := w.DeliverNext(context.Background())
	require.NoError(t, err)

	assert.Zero(t, sender.count(),
		"the worker must NOT POST to an inbox that is not same-authority with the community")
	// The row lives at the cross-authority inbox, so read state by activity id
	// (getDelivery hardcodes wInbox). The worker refuses it — never 'delivered'.
	assert.NotEqual(t, store.DeliveryStateDelivered, deliveryState(t, conn, id),
		"a cross-authority inbox delivery must never reach 'delivered'")
}

// ---------------------------------------------------------------------------
// H4a: a parked delivery is not immediately re-claimable (real backoff)
// ---------------------------------------------------------------------------

func TestWorker_ParkedDeliveryIsNotImmediatelyReclaimable(t *testing.T) {
	conn := workerTestDB(t)
	seedWorkerActor(t, conn, true, false)

	// ONE delivery: it is the head ClaimNext returns, so it is the one the kill
	// switch parks (a spurious sibling would be parked instead, leaving this
	// row's next_attempt_at at seed-time).
	id := seedDelivery(t, conn, "Create", "", createPayload("x"))
	w := newWorker(t, conn, &fakeSender{}, func(o *WorkerOptions) {
		o.Switches = &fakeSwitches{allow: false}
	})
	_, err := w.DeliverNext(context.Background())
	require.NoError(t, err)

	// Park must advance next_attempt_at by a REAL delay, or a parked delivery
	// spins the worker in a hot loop (and, on the causal path, burns the attempt
	// budget). Asserting the scheduled time directly (not a re-claim, which is
	// app↔DB clock-skew sensitive) keeps the pin deterministic.
	d := getDelivery(t, conn, id)
	assert.Truef(t, d.NextAttemptAt.After(time.Now().Add(time.Second)),
		"a just-parked delivery must be scheduled a real delay into the future, got next_attempt_at=%s (now=%s)",
		d.NextAttemptAt, time.Now())
	assert.Equal(t, store.DeliveryStatePending, d.State, "parked stays pending")
}

// ---------------------------------------------------------------------------
// Important: 401 is NOT an inbox-rotation signal
// ---------------------------------------------------------------------------

func TestWorker_Unauthorized401IsTransientNotRotationPoison(t *testing.T) {
	conn := workerTestDB(t)
	seedWorkerActor(t, conn, true, false)
	id := seedDelivery(t, conn, "Create", "", createPayload("x"))

	// A resolver that CAN rotate, and a peer that 401s everywhere. A 401 is an
	// auth problem (our signature / their secure-mode), NOT an endpoint
	// rotation, so it must not consume the single re-resolve and then poison.
	resolver := &rotatingResolver{normal: wInbox, fresh: "https://lemmy.world/c/tech/inbox-v2"}
	sender := senderReturning(httpErr(http.StatusUnauthorized, ""))
	w := newWorker(t, conn, sender, func(o *WorkerOptions) { o.Inboxes = resolver })

	_, err := w.DeliverNext(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 0, resolver.freshCalls,
		"a 401 must NOT trigger inbox re-resolution — it is not an endpoint rotation")
	assert.Equal(t, store.DeliveryStatePending, getDelivery(t, conn, id).State,
		"a 401 is transient (retry), not a poison-after-one-rotation")
}

// ---------------------------------------------------------------------------
// Important: ConfigSwitches host matching is case-insensitive
// ---------------------------------------------------------------------------

func TestConfigSwitches_HostMatchIsCaseInsensitive(t *testing.T) {
	sw := ConfigSwitches{
		DisabledHosts: map[string]struct{}{"Lemmy.World": {}},
	}
	assert.False(t, sw.OutboundAllowed(DeliveryScope{InboxHost: "lemmy.world"}),
		"a disabled-host entry must block regardless of case — hostOf lowercases the scope, so a "+
			"mixed-case switch value must still block the (lowercased) host")
}

// seedDeliveryTo is seedDelivery with an explicit target inbox.
func seedDeliveryTo(t *testing.T, conn *sql.DB, kind, inbox string) string {
	t.Helper()
	ctx := context.Background()
	activityID := "https://coves.social/ap/activity/" + repeatHex64(kind+inbox)
	_, err := store.NewOutboundActivities(conn).Insert(ctx, store.OutboundActivity{
		ActivityID: activityID,
		ActorDID:   wActorDID,
		Kind:       kind,
		Payload:    createPayload(activityID),
	})
	require.NoError(t, err)
	_, err = store.NewOutboundDeliveries(conn).Enqueue(ctx, store.OutboundDelivery{
		ActivityID:  activityID,
		TargetInbox: inbox,
		OrderingKey: wCommunityAPID,
	})
	require.NoError(t, err)
	return activityID
}
