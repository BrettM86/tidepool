package outbound

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/consume"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// Task 15 cycle E: the real Enqueuer. It translates one intent and writes its
// canonical activity, its per-inbox delivery, AND the ap_objects mapping (so the
// object is fetchable), ALL on the caller's rev-gate transaction. The whole
// point is atomicity: the enqueue commits with the gate advance or leaves
// nothing behind, so a replay can always reproduce it.

func enqueuerTestDB(t *testing.T) *sql.DB {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database,
		"outbound_deliveries", "outbound_activities", "ap_objects", "ap_actors")
	return database
}

// fakeInboxResolver records the communities it is asked to resolve and returns a
// canned inbox (or error).
type fakeInboxResolver struct {
	inbox      string
	err        error
	calledWith []string
}

func (r *fakeInboxResolver) ResolveInbox(_ context.Context, communityAPID string) (string, error) {
	r.calledWith = append(r.calledWith, communityAPID)
	return r.inbox, r.err
}

// seedEnqueuerActor writes the ap_actors row the enqueuer resolves the actor id
// off of (actorDID -> attributedTo single string).
func seedEnqueuerActor(t *testing.T, conn *sql.DB) string {
	t.Helper()
	actorID := outUserOrigin + "/ap/actor/" + outCommenterDID
	_, err := store.NewAPActors(conn).Create(context.Background(), store.APActor{
		DID:              outCommenterDID,
		Kind:             store.ActorTypePerson,
		ActorID:          actorID,
		NormalizedOrigin: "coves.social",
		LocalPart:        "alice",
		RSAKeySealed:     []byte("sealed-key-bytes"),
		RSAKeyVersion:    1,
		PublicKeyPEM:     "-----BEGIN PUBLIC KEY-----\nstub\n-----END PUBLIC KEY-----\n",
	})
	require.NoError(t, err, "seed ap_actors row for the commenter")
	return actorID
}

func newTestEnqueuer(t *testing.T, conn *sql.DB, inboxes InboxResolver) *Enqueuer {
	t.Helper()
	enq, err := NewEnqueuer(EnqueuerOptions{
		DB:         conn,
		Translator: NewTranslator(outUserOrigin),
		Inboxes:    inboxes,
		Actors:     store.NewAPActors(conn),
		UserOrigin: outUserOrigin,
	})
	require.NoError(t, err)
	return enq
}

func commentEnqueueIntent(t *testing.T) consume.CommentIntent {
	return consume.CommentIntent{
		Op:            "create",
		ATURI:         outCommentATURI,
		ID:            consume.ActivityID(outUserOrigin, outCommentATURI, "create", 0),
		CommunityAPID: outCommunityAPID,
		ParentAPID:    outRootAPID,
		Snapshot:      commentSnapshot(t),
	}
}

func TestEnqueuer_CommittedTxWritesActivityDeliveryAndMapping(t *testing.T) {
	conn := enqueuerTestDB(t)
	ctx := context.Background()
	seedEnqueuerActor(t, conn)

	resolver := &fakeInboxResolver{inbox: outSharedInbox}
	enq := newTestEnqueuer(t, conn, resolver)
	intent := commentEnqueueIntent(t)

	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, enq.EnqueueActivity(ctx, tx, outCommenterDID, outCommunityAPID, outRootATURI, intent),
		"EnqueueActivity must write its rows on the given tx")
	require.NoError(t, tx.Commit())

	// The inbox resolver was consulted with the community, and its answer is the
	// delivery target.
	require.Contains(t, resolver.calledWith, outCommunityAPID,
		"the enqueuer resolves the community's inbox at enqueue time (the delivery PK needs it)")

	require.Equal(t, 1, count(t, conn, "outbound_activities"), "exactly one canonical activity")
	require.Equal(t, 1, count(t, conn, "outbound_deliveries"), "exactly one per-inbox delivery")

	activity, err := store.NewOutboundActivities(conn).Get(ctx, intent.ID)
	require.NoError(t, err)
	require.NotNil(t, activity)
	assert.Equal(t, outCommenterDID, activity.ActorDID)
	assert.Equal(t, "Create", activity.Kind, "the translated kind rides the activity row")
	assert.Equal(t, outRootATURI, activity.ParentATURI, "the causal parent is copied onto the row")
	assert.NotEmpty(t, activity.Payload, "the canonical payload is stored")

	delivery, err := store.NewOutboundDeliveries(conn).Get(ctx, intent.ID, outSharedInbox)
	require.NoError(t, err, "the delivery is keyed by (activity, resolved inbox)")
	require.NotNil(t, delivery)
	assert.Equal(t, outSharedInbox, delivery.TargetInbox, "target_inbox is the resolver's answer")
	assert.Equal(t, outCommunityAPID, delivery.OrderingKey, "ordering_key is the community AP id")
	assert.Equal(t, store.DeliveryStatePending, delivery.State)

	// The object is fetchable: an ap_objects mapping was written, origin=bridge.
	mapping, err := store.NewAPObjects(conn).GetByAPID(ctx, outCommentAPID)
	require.NoError(t, err,
		"the enqueuer writes an ap_objects mapping so GET /ap/object can serve the record")
	require.NotNil(t, mapping)
	assert.Equal(t, store.OriginBridge, mapping.Origin, "a bridge-emitted object must say origin=bridge")
	assert.Equal(t, outCommenterDID, mapping.DID)
}

func TestEnqueuer_RolledBackTxWritesNothing(t *testing.T) {
	conn := enqueuerTestDB(t)
	ctx := context.Background()
	seedEnqueuerActor(t, conn)

	enq := newTestEnqueuer(t, conn, &fakeInboxResolver{inbox: outSharedInbox})
	intent := commentEnqueueIntent(t)

	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, enq.EnqueueActivity(ctx, tx, outCommenterDID, outCommunityAPID, outRootATURI, intent))
	require.NoError(t, tx.Rollback())

	assert.Zero(t, count(t, conn, "outbound_activities"),
		"a rolled-back gate tx must leave NO activity — this is the seam that makes replay safe")
	assert.Zero(t, count(t, conn, "outbound_deliveries"), "...and no delivery")
	assert.Zero(t, count(t, conn, "ap_objects"), "...and no mapping")
}

func TestEnqueuer_ReEnqueueIsIdempotent(t *testing.T) {
	conn := enqueuerTestDB(t)
	ctx := context.Background()
	seedEnqueuerActor(t, conn)

	enq := newTestEnqueuer(t, conn, &fakeInboxResolver{inbox: outSharedInbox})
	intent := commentEnqueueIntent(t)

	for i := 0; i < 2; i++ {
		tx, err := conn.BeginTx(ctx, nil)
		require.NoError(t, err)
		require.NoError(t, enq.EnqueueActivity(ctx, tx, outCommenterDID, outCommunityAPID, outRootATURI, intent),
			"a redelivered intent re-derives the same activity id and must not error")
		require.NoError(t, tx.Commit())
	}

	assert.Equal(t, 1, count(t, conn, "outbound_activities"),
		"a re-enqueue of the same activity id must not write a second activity (ON CONFLICT DO NOTHING)")
	assert.Equal(t, 1, count(t, conn, "outbound_deliveries"),
		"...nor a duplicate delivery for the same (activity, inbox)")
}

func TestEnqueuer_InboxResolutionFailureRollsBack(t *testing.T) {
	conn := enqueuerTestDB(t)
	ctx := context.Background()
	seedEnqueuerActor(t, conn)

	enq := newTestEnqueuer(t, conn, &fakeInboxResolver{err: assert.AnError})
	intent := commentEnqueueIntent(t)

	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	err = enq.EnqueueActivity(ctx, tx, outCommenterDID, outCommunityAPID, outRootATURI, intent)
	require.Error(t, err,
		"a community whose inbox cannot be resolved has no delivery target — the enqueue must fail, "+
			"not write a delivery to nowhere")
	_ = tx.Rollback()

	assert.Zero(t, count(t, conn, "outbound_activities"), "a failed enqueue leaves no partial rows")
	assert.Zero(t, count(t, conn, "outbound_deliveries"))
}

func count(t *testing.T, conn *sql.DB, table string) int {
	t.Helper()
	var n int
	require.NoError(t, conn.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM `+table).Scan(&n))
	return n
}
