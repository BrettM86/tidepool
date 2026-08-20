package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/testutil"
)

// Task 15 cycle A: the outbound DELIVERY queue — canonical activities
// (immutable wire payloads) fanned out to per-inbox deliveries (decision 15).
// The delivery half generalizes the inbox_events fenced queue: claimed_until
// fencing, per-ordering-key serialization via a loose index scan, SKIP LOCKED.
// Every pin here is really about "can at-least-once delivery survive a crash
// between deliver and mark, without double-delivering or clobbering a re-claim?"

// deliveryTestDB returns a migrated connection with task 15's tables emptied.
// The two are truncated in ONE statement so the FK (deliveries → activities)
// is satisfied without CASCADE, and RESTART IDENTITY resets the delivery seq so
// ordering assertions are deterministic across runs.
func deliveryTestDB(t *testing.T) *sql.DB {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database,
		"outbound_deliveries", "outbound_activities", "outbound_objects")
	return database
}

const (
	delTargetInbox = "https://lemmy.world/c/technology/inbox"
	delOrderingKey = "https://lemmy.world/c/technology"
	delParentATURI = "at://" + testCommunityDID + "/social.coves.community.postv2/3lzpostaaaaaa"
)

// Activity ids embed a 64-hex digest built at runtime (repeatHex is a func), so
// these are vars, not consts.
var (
	delActivityID      = "https://coves.social/ap/activity/" + repeatHex('a')
	delOtherActivityID = "https://coves.social/ap/activity/" + repeatHex('b')
)

func testActivity() OutboundActivity {
	return OutboundActivity{
		ActivityID:  delActivityID,
		ActorDID:    testDID,
		Kind:        "Create",
		Payload:     []byte(`{"type":"Create","object":{"type":"Note","content":"hi"}}`),
		ParentATURI: delParentATURI,
	}
}

func testDelivery() OutboundDelivery {
	return OutboundDelivery{
		ActivityID:  delActivityID,
		TargetInbox: delTargetInbox,
		OrderingKey: delOrderingKey,
	}
}

// seedActivity inserts the canonical activity a delivery fans out from (the FK
// target), failing the test if the insert did not report a fresh row.
func seedActivity(t *testing.T, repo OutboundActivities, activity OutboundActivity) {
	t.Helper()
	inserted, err := repo.Insert(context.Background(), activity)
	require.NoError(t, err, "seed activity %s", activity.ActivityID)
	require.True(t, inserted, "seed activity %s must be a fresh insert", activity.ActivityID)
}

// ---------------------------------------------------------------------------
// outbound_activities — the immutable canonical payload
// ---------------------------------------------------------------------------

func TestOutboundActivities_InsertIsIdempotentAndImmutable(t *testing.T) {
	database := deliveryTestDB(t)
	repo := NewOutboundActivities(database)
	ctx := context.Background()

	inserted, err := repo.Insert(ctx, testActivity())
	require.NoError(t, err, "first insert of %s", delActivityID)
	assert.True(t, inserted, "a fresh activity id must report inserted=true")

	// A redelivery re-derives the SAME id and re-inserts. The payload a peer may
	// already hold must NOT be overwritten — ON CONFLICT DO NOTHING.
	rewrite := testActivity()
	rewrite.Payload = []byte(`{"type":"Create","object":{"type":"Note","content":"TAMPERED"}}`)
	inserted, err = repo.Insert(ctx, rewrite)
	require.NoError(t, err, "re-insert of an existing activity id is not an error")
	assert.False(t, inserted,
		"an already-stored activity id must report inserted=false, not overwrite the row")

	got, err := repo.Get(ctx, delActivityID)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.JSONEq(t, `{"type":"Create","object":{"type":"Note","content":"hi"}}`, string(got.Payload),
		"the canonical payload is immutable: a re-insert must not rewrite it (activities are "+
			"byte-stable across later edits)")
	assert.Equal(t, testDID, got.ActorDID, "the signing persona is recorded")
	assert.Equal(t, "Create", got.Kind)
	assert.Equal(t, delParentATURI, got.ParentATURI,
		"the causal dependency rides the activity row")
}

func TestOutboundActivities_GetMissingIsNotFound(t *testing.T) {
	database := deliveryTestDB(t)
	repo := NewOutboundActivities(database)

	_, err := repo.Get(context.Background(), delActivityID)
	require.Error(t, err, "an unknown activity id must not silently return a zero row")
	assert.True(t, errors.IsNotFound(err), "want NotFound, got %v", err)
}

func TestOutboundActivities_InsertTxRidesTheTransaction(t *testing.T) {
	database := deliveryTestDB(t)
	repo := NewOutboundActivities(database)
	ctx := context.Background()

	// Rolled back: the enqueue rides the rev-gate claim, so a gate rollback
	// must leave NO activity row — otherwise a replay finds the activity but no
	// gate advance.
	tx, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	inserted, err := repo.InsertTx(ctx, tx, testActivity())
	require.NoError(t, err, "InsertTx inside a transaction")
	assert.True(t, inserted)
	require.NoError(t, tx.Rollback())

	_, err = repo.Get(ctx, delActivityID)
	require.Error(t, err, "a rolled-back InsertTx must leave no activity row")
	assert.True(t, errors.IsNotFound(err), "want NotFound after rollback, got %v", err)

	// Committed: the row lands.
	tx, err = database.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = repo.InsertTx(ctx, tx, testActivity())
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	got, err := repo.Get(ctx, delActivityID)
	require.NoError(t, err, "a committed InsertTx must be visible")
	require.NotNil(t, got)

	_, err = repo.InsertTx(ctx, nil, testActivity())
	require.Error(t, err, "InsertTx with a nil tx must not silently fall back to the pool")
	assert.True(t, errors.IsValidation(err), "want validation error, got %v", err)
}

// ---------------------------------------------------------------------------
// outbound_deliveries — enqueue + read
// ---------------------------------------------------------------------------

func TestOutboundDeliveries_EnqueueStartsPending(t *testing.T) {
	database := deliveryTestDB(t)
	activities := NewOutboundActivities(database)
	repo := NewOutboundDeliveries(database)
	ctx := context.Background()

	seedActivity(t, activities, testActivity())

	stored, err := repo.Enqueue(ctx, testDelivery())
	require.NoError(t, err, "enqueue delivery for %s", delActivityID)
	require.NotNil(t, stored, "Enqueue must return the stored row")
	assert.Equal(t, DeliveryStatePending, stored.State, "a fresh delivery is pending")
	assert.Equal(t, 0, stored.Attempts, "no attempt has been made yet")
	assert.Nil(t, stored.ClaimedUntil, "a fresh delivery is unclaimed")
	assert.Nil(t, stored.DeliveredAt)
	assert.Positive(t, stored.Seq, "the monotonic ordering seq is assigned on enqueue")

	got, err := repo.Get(ctx, delActivityID, delTargetInbox)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, delOrderingKey, got.OrderingKey)

	// The (activity, inbox) pair is the primary key: a second enqueue of the
	// same pair must refuse, not spawn a duplicate delivery.
	_, err = repo.Enqueue(ctx, testDelivery())
	require.Error(t, err, "a duplicate (activity, inbox) delivery must be refused")
	assert.True(t, errors.IsAlreadyExists(err), "want AlreadyExists, got %v", err)
}

func TestOutboundDeliveries_EnqueueTxRidesTheTransaction(t *testing.T) {
	database := deliveryTestDB(t)
	activities := NewOutboundActivities(database)
	repo := NewOutboundDeliveries(database)
	ctx := context.Background()

	seedActivity(t, activities, testActivity())

	tx, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = repo.EnqueueTx(ctx, tx, testDelivery())
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())

	_, err = repo.Get(ctx, delActivityID, delTargetInbox)
	require.Error(t, err, "a rolled-back EnqueueTx must leave no delivery row")
	assert.True(t, errors.IsNotFound(err), "want NotFound after rollback, got %v", err)

	_, err = repo.EnqueueTx(ctx, nil, testDelivery())
	require.Error(t, err, "EnqueueTx with a nil tx must not silently fall back to the pool")
	assert.True(t, errors.IsValidation(err), "want validation error, got %v", err)
}

// ---------------------------------------------------------------------------
// outbound_deliveries — ClaimNext, fencing, per-key serialization
// ---------------------------------------------------------------------------

func TestOutboundDeliveries_ClaimNextStampsFencingAndBumpsAttempts(t *testing.T) {
	database := deliveryTestDB(t)
	activities := NewOutboundActivities(database)
	repo := NewOutboundDeliveries(database)
	ctx := context.Background()

	seedActivity(t, activities, testActivity())
	_, err := repo.Enqueue(ctx, testDelivery())
	require.NoError(t, err)

	claimed, err := repo.ClaimNext(ctx, time.Minute)
	require.NoError(t, err, "ClaimNext must return the head-of-key pending delivery")
	require.NotNil(t, claimed)
	assert.Equal(t, delActivityID, claimed.ActivityID)
	assert.Equal(t, delTargetInbox, claimed.TargetInbox)
	assert.Equal(t, 1, claimed.Attempts, "ClaimNext increments the attempt counter")
	require.NotNil(t, claimed.ClaimedUntil,
		"the claim must stamp a fencing token (claimed_until) the Mark* methods verify")
	assert.True(t, claimed.ClaimedUntil.After(time.Now().Add(-time.Second)),
		"the lease is stamped into the future")

	// A claimed, unexpired delivery is invisible to a second claim (SKIP LOCKED
	// + the lease guard) — no two workers may hold the same delivery.
	_, err = repo.ClaimNext(ctx, time.Minute)
	require.Error(t, err, "a claimed, unexpired delivery must be invisible to a second ClaimNext")
	assert.True(t, errors.IsNotFound(err), "want NotFound (empty queue), got %v", err)
}

func TestOutboundDeliveries_ExpiredLeaseIsReclaimable(t *testing.T) {
	database := deliveryTestDB(t)
	activities := NewOutboundActivities(database)
	repo := NewOutboundDeliveries(database)
	ctx := context.Background()

	seedActivity(t, activities, testActivity())
	_, err := repo.Enqueue(ctx, testDelivery())
	require.NoError(t, err)

	claimed, err := repo.ClaimNext(ctx, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)

	// A worker that claimed this delivery then crashed: its lease lapses. The
	// whole point of a lease is that the delivery becomes re-claimable — a stuck
	// worker must never strand a delivery forever.
	_, err = database.ExecContext(ctx,
		`UPDATE outbound_deliveries SET claimed_until = now() - interval '1 minute' WHERE activity_id = $1`,
		delActivityID)
	require.NoError(t, err)

	reclaimed, err := repo.ClaimNext(ctx, time.Minute)
	require.NoError(t, err, "a delivery whose lease has EXPIRED must be re-claimable")
	require.NotNil(t, reclaimed)
	assert.Equal(t, delActivityID, reclaimed.ActivityID)
	require.NotNil(t, reclaimed.ClaimedUntil)
	assert.True(t, reclaimed.ClaimedUntil.After(time.Now()),
		"the re-claim stamps a fresh future lease")
}

func TestOutboundDeliveries_PerOrderingKeySerialization(t *testing.T) {
	database := deliveryTestDB(t)
	activities := NewOutboundActivities(database)
	repo := NewOutboundDeliveries(database)
	ctx := context.Background()

	// Two deliveries on the SAME ordering key (the same community). The younger
	// one must be invisible while the older PENDING sibling is unclaimed.
	seedActivity(t, activities, testActivity())
	older := testDelivery()
	_, err := repo.Enqueue(ctx, older)
	require.NoError(t, err)

	second := testActivity()
	second.ActivityID = delOtherActivityID
	seedActivity(t, activities, second)
	younger := OutboundDelivery{
		ActivityID:  delOtherActivityID,
		TargetInbox: delTargetInbox,
		OrderingKey: delOrderingKey,
	}
	_, err = repo.Enqueue(ctx, younger)
	require.NoError(t, err)

	// The head of the key is the older delivery.
	claimed, err := repo.ClaimNext(ctx, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, delActivityID, claimed.ActivityID,
		"ClaimNext returns the min-seq pending row of the ordering key")

	// While the older sibling is claimed (pending, leased), the younger is
	// still blocked: per-community serialization is head-of-line.
	_, err = repo.ClaimNext(ctx, time.Minute)
	require.Error(t, err,
		"a younger delivery on a key must be invisible while an older pending sibling exists")
	assert.True(t, errors.IsNotFound(err), "want NotFound, got %v", err)

	// Once the older sibling reaches a terminal state (delivered), the younger
	// unblocks.
	exists, applied, err := repo.MarkDelivered(ctx, claimed.ActivityID, claimed.TargetInbox, 202, *claimed.ClaimedUntil)
	require.NoError(t, err)
	require.True(t, exists)
	require.True(t, applied, "the claim holder marks its delivery delivered")

	next, err := repo.ClaimNext(ctx, time.Minute)
	require.NoError(t, err, "a delivered sibling stops blocking the ordering key")
	require.NotNil(t, next)
	assert.Equal(t, delOtherActivityID, next.ActivityID,
		"the younger delivery becomes the head once the older is terminal")
}

func TestOutboundDeliveries_MarkDeliveredFencing(t *testing.T) {
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

	// A STALE token (a worker whose lease lapsed and was re-claimed by another)
	// must not clobber the newer attempt: applied=false, no error, state
	// untouched.
	wrongToken := staleToken.Add(-time.Hour)
	exists, applied, err := repo.MarkDelivered(ctx, delActivityID, delTargetInbox, 202, wrongToken)
	require.NoError(t, err)
	assert.True(t, exists, "the delivery row exists")
	assert.False(t, applied, "a stale fencing token must be a no-op, not a clobber")

	got, err := repo.Get(ctx, delActivityID, delTargetInbox)
	require.NoError(t, err)
	assert.Equal(t, DeliveryStatePending, got.State, "the stale mark must not have moved state")

	// The current claim holder marks it delivered.
	exists, applied, err = repo.MarkDelivered(ctx, delActivityID, delTargetInbox, 202, staleToken)
	require.NoError(t, err)
	assert.True(t, exists)
	assert.True(t, applied, "the current claim holder's mark applies")

	got, err = repo.Get(ctx, delActivityID, delTargetInbox)
	require.NoError(t, err)
	assert.Equal(t, DeliveryStateDelivered, got.State)
	require.NotNil(t, got.DeliveredAt, "delivered_at is stamped")
	require.NotNil(t, got.LastStatusCode)
	assert.Equal(t, 202, *got.LastStatusCode, "the accepting status is recorded")
	assert.Nil(t, got.ClaimedUntil, "the lease is cleared on a terminal outcome")

	// A missing (activity, inbox) reports exists=false.
	exists, applied, err = repo.MarkDelivered(ctx, delOtherActivityID, delTargetInbox, 202, staleToken)
	require.NoError(t, err)
	assert.False(t, exists, "a missing delivery reports exists=false")
	assert.False(t, applied)
}

func TestOutboundDeliveries_ReleaseReschedulesUnderFencing(t *testing.T) {
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

	next := time.Now().Add(30 * time.Second).UTC()
	exists, applied, err := repo.Release(ctx, delActivityID, delTargetInbox, "5xx", "bad gateway", 502, next, token)
	require.NoError(t, err)
	assert.True(t, exists)
	assert.True(t, applied, "the claim holder reschedules the retry")

	got, err := repo.Get(ctx, delActivityID, delTargetInbox)
	require.NoError(t, err)
	assert.Equal(t, DeliveryStatePending, got.State, "a released delivery stays pending for retry")
	assert.Nil(t, got.ClaimedUntil, "the lease is cleared so a retry can re-claim")
	assert.Equal(t, "5xx", got.LastErrorClass, "the retry taxonomy label is recorded")
	require.NotNil(t, got.LastStatusCode)
	assert.Equal(t, 502, *got.LastStatusCode)
	assert.WithinDuration(t, next, got.NextAttemptAt, time.Second, "the backoff schedule is stored")

	// A stale token cannot reschedule.
	_, applied, err = repo.Release(ctx, delActivityID, delTargetInbox, "5xx", "x", 502,
		next, token.Add(-time.Hour))
	require.NoError(t, err)
	assert.False(t, applied, "a stale fencing token must not reschedule")
}

func TestOutboundDeliveries_MarkPoisonedStopsBlockingKey(t *testing.T) {
	database := deliveryTestDB(t)
	activities := NewOutboundActivities(database)
	repo := NewOutboundDeliveries(database)
	ctx := context.Background()

	// Older poisons; younger on the same key must then unblock (a poisoned
	// sibling stops blocking, exactly like inbox_events).
	seedActivity(t, activities, testActivity())
	_, err := repo.Enqueue(ctx, testDelivery())
	require.NoError(t, err)

	second := testActivity()
	second.ActivityID = delOtherActivityID
	seedActivity(t, activities, second)
	_, err = repo.Enqueue(ctx, OutboundDelivery{
		ActivityID:  delOtherActivityID,
		TargetInbox: delTargetInbox,
		OrderingKey: delOrderingKey,
	})
	require.NoError(t, err)

	claimed, err := repo.ClaimNext(ctx, time.Minute)
	require.NoError(t, err)
	require.Equal(t, delActivityID, claimed.ActivityID)

	exists, applied, err := repo.MarkPoisoned(ctx, delActivityID, delTargetInbox, "4xx", "bad request", 400, *claimed.ClaimedUntil)
	require.NoError(t, err)
	assert.True(t, exists)
	assert.True(t, applied)

	got, err := repo.Get(ctx, delActivityID, delTargetInbox)
	require.NoError(t, err)
	assert.Equal(t, DeliveryStatePoisoned, got.State)
	assert.Equal(t, "4xx", got.LastErrorClass)

	next, err := repo.ClaimNext(ctx, time.Minute)
	require.NoError(t, err, "a poisoned sibling must stop blocking its ordering key")
	require.NotNil(t, next)
	assert.Equal(t, delOtherActivityID, next.ActivityID)

	// A stale token cannot poison a delivery a retry already completed.
	_, applied, err = repo.MarkPoisoned(ctx, delActivityID, delTargetInbox, "4xx", "x", 400,
		claimed.ClaimedUntil.Add(-time.Hour))
	require.NoError(t, err)
	assert.False(t, applied, "a stale fencing token must not poison")
}

// ---------------------------------------------------------------------------
// outbound_deliveries — consent / kill-switch cancellation
// ---------------------------------------------------------------------------

func TestOutboundDeliveries_CancelForActorParksPending(t *testing.T) {
	database := deliveryTestDB(t)
	activities := NewOutboundActivities(database)
	repo := NewOutboundDeliveries(database)
	ctx := context.Background()

	// Three deliveries on DISTINCT ordering keys, so head-of-line blocking never
	// leaves a testDID delivery stranded pending by accident — the kill switch
	// must cancel EVERY pending delivery of the actor, so the fixture must not
	// smuggle in a second pending sibling and pretend it is terminal:
	//   - testDID / key A  → driven to DELIVERED (terminal, must survive Cancel);
	//   - testDID / key B  → left PENDING (the one Cancel parks);
	//   - testSecondDID/key C → left PENDING (out of scope, must survive).
	deliveredKey := "https://lemmy.world/c/technology"
	pendingKey := "https://lemmy.world/c/science"
	otherKey := "https://lemmy.world/c/gaming"

	// testDID's soon-to-be-delivered activity, on key A.
	delivered := testActivity() // actor testDID, id delActivityID
	seedActivity(t, activities, delivered)
	_, err := repo.Enqueue(ctx, OutboundDelivery{
		ActivityID:  delActivityID,
		TargetInbox: delTargetInbox + "/delivered",
		OrderingKey: deliveredKey,
	})
	require.NoError(t, err)

	// testDID's still-pending activity, on key B.
	pending := testActivity()
	pending.ActivityID = delOtherActivityID
	seedActivity(t, activities, pending)
	_, err = repo.Enqueue(ctx, OutboundDelivery{
		ActivityID:  delOtherActivityID,
		TargetInbox: delTargetInbox + "/pending",
		OrderingKey: pendingKey,
	})
	require.NoError(t, err)

	// A DIFFERENT actor's pending activity, on key C.
	otherActor := OutboundActivity{
		ActivityID: "https://coves.social/ap/activity/" + repeatHex('c'),
		ActorDID:   testSecondDID,
		Kind:       "Create",
		Payload:    []byte(`{"type":"Create"}`),
	}
	seedActivity(t, activities, otherActor)
	_, err = repo.Enqueue(ctx, OutboundDelivery{
		ActivityID:  otherActor.ActivityID,
		TargetInbox: delTargetInbox + "/other",
		OrderingKey: otherKey,
	})
	require.NoError(t, err)

	// Deliver testDID's key-A delivery so Cancel must leave that terminal row
	// alone. It is the head of its own key, so ClaimNext reaches it; loop until
	// it is the one claimed (distinct keys → whichever head has the lower seq
	// comes first, and key A was enqueued first).
	c, err := repo.ClaimNext(ctx, time.Minute)
	require.NoError(t, err)
	require.Equal(t, delActivityID, c.ActivityID,
		"the first-enqueued delivery is the lowest-seq head across the keys")
	_, applied, err := repo.MarkDelivered(ctx, c.ActivityID, c.TargetInbox, 202, *c.ClaimedUntil)
	require.NoError(t, err)
	require.True(t, applied)

	cancelled, err := repo.CancelForActor(ctx, testDID)
	require.NoError(t, err, "CancelForActor sweeps EVERY pending delivery of the actor")
	assert.EqualValues(t, 1, cancelled,
		"exactly the one still-pending delivery for testDID is cancelled (the delivered one is "+
			"terminal, the other actor's is out of scope)")

	// The pending testDID delivery is parked.
	got, err := repo.Get(ctx, delOtherActivityID, delTargetInbox+"/pending")
	require.NoError(t, err)
	assert.Equal(t, DeliveryStateCancelled, got.State, "a cancelled delivery is parked, not poisoned")

	// The DELIVERED testDID delivery is untouched — sparing a pending head would
	// be a broken kill switch, but a TERMINAL row must never be rewritten.
	got, err = repo.Get(ctx, delActivityID, delTargetInbox+"/delivered")
	require.NoError(t, err)
	assert.Equal(t, DeliveryStateDelivered, got.State,
		"CancelForActor must not touch terminal deliveries")

	// The other actor's pending delivery is untouched.
	got, err = repo.Get(ctx, otherActor.ActivityID, delTargetInbox+"/other")
	require.NoError(t, err)
	assert.Equal(t, DeliveryStatePending, got.State, "CancelForActor is scoped to the named actor")
}

func TestOutboundDeliveries_CancelForCommunityParksPending(t *testing.T) {
	database := deliveryTestDB(t)
	activities := NewOutboundActivities(database)
	repo := NewOutboundDeliveries(database)
	ctx := context.Background()

	seedActivity(t, activities, testActivity())
	_, err := repo.Enqueue(ctx, testDelivery())
	require.NoError(t, err)

	// A delivery on a DIFFERENT ordering key must survive.
	other := testActivity()
	other.ActivityID = delOtherActivityID
	seedActivity(t, activities, other)
	otherKey := "https://lemmy.world/c/science"
	_, err = repo.Enqueue(ctx, OutboundDelivery{
		ActivityID:  delOtherActivityID,
		TargetInbox: "https://lemmy.world/c/science/inbox",
		OrderingKey: otherKey,
	})
	require.NoError(t, err)

	cancelled, err := repo.CancelForCommunity(ctx, delOrderingKey)
	require.NoError(t, err, "CancelForCommunity parks a community's pending work (unfollow/delete)")
	assert.EqualValues(t, 1, cancelled)

	got, err := repo.Get(ctx, delActivityID, delTargetInbox)
	require.NoError(t, err)
	assert.Equal(t, DeliveryStateCancelled, got.State)

	got, err = repo.Get(ctx, delOtherActivityID, "https://lemmy.world/c/science/inbox")
	require.NoError(t, err)
	assert.Equal(t, DeliveryStatePending, got.State,
		"CancelForCommunity is scoped to the named ordering key")
}

// ---------------------------------------------------------------------------
// outbound_objects.accepted_at — the causal-gating marker (migration 020)
// ---------------------------------------------------------------------------

func TestOutboundObjects_SetAcceptedStampsAndIsIdempotent(t *testing.T) {
	database := deliveryTestDB(t)
	repo := NewOutboundObjects(database)
	ctx := context.Background()

	_, err := repo.Upsert(ctx, testOutboundObject())
	require.NoError(t, err)

	// accepted_at starts NULL: a not-yet-delivered object gates its bridge-origin
	// children.
	assert.Nil(t, rawAcceptedAt(t, database, testCommentATURI),
		"a freshly written outbound object must have a NULL accepted_at (not yet delivered)")

	require.NoError(t, repo.SetAccepted(ctx, testCommentATURI),
		"delivery success stamps accepted_at")
	first := rawAcceptedAt(t, database, testCommentATURI)
	require.NotNil(t, first, "SetAccepted must stamp accepted_at")

	// Idempotent: a redelivery re-stamps but must NOT move the causal boundary.
	require.NoError(t, repo.SetAccepted(ctx, testCommentATURI))
	second := rawAcceptedAt(t, database, testCommentATURI)
	require.NotNil(t, second)
	assert.Equal(t, *first, *second,
		"re-accepting preserves the original accepted_at: a redelivery must not move the marker")

	err = repo.SetAccepted(ctx, "at://did:plc:nobody/social.coves.community.comment/nope")
	require.Error(t, err, "accepting an object we hold no state for is a bug, not a no-op")
	assert.True(t, errors.IsNotFound(err), "want NotFound, got %v", err)
}

// rawAcceptedAt reads outbound_objects.accepted_at directly so the pin does not
// depend on the struct/scan carrying the new column yet.
func rawAcceptedAt(t *testing.T, database *sql.DB, atURI string) *time.Time {
	t.Helper()
	var accepted sql.NullTime
	err := database.QueryRowContext(context.Background(),
		`SELECT accepted_at FROM outbound_objects WHERE at_uri = $1`, atURI).Scan(&accepted)
	require.NoError(t, err, "read accepted_at for %s", atURI)
	if !accepted.Valid {
		return nil
	}
	return &accepted.Time
}
