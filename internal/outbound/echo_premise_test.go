package outbound

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/consume"
	"tidepool/internal/echo"
	"tidepool/internal/errors"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// THE ECHO CLASSIFIER'S PREMISE (task 17a).
//
// The classifier answers "is this ours?" from stored state alone. That is only
// sound if state cannot lag delivery: an activity a peer can echo back at us
// must already be recognizable when it goes out. The task spec calls the
// mapping-arrival race "only theoretical" and says to ASSERT that rather than
// assume it.
//
// The property is NOT write ordering inside the transaction — ordering within a
// commit is invisible to everyone outside it. It is ATOMICITY plus a FOREIGN
// KEY:
//
//   - the enqueuer writes the activity, its per-inbox delivery, and the
//     bridge-origin ap_objects mapping on the CALLER'S transaction
//     (enqueuer.go:122-159) — the acceptance commit for a post, the rev-gate tx
//     for a comment or vote — so either all three commit or none do;
//   - the FK on outbound_deliveries.activity_id makes a delivery without its
//     activity unrepresentable, including for code that bypasses the enqueuer
//     entirely.
//
// The mapping matters as much as the activity. A surviving delivery whose
// mapping rolled back would put an echo on the wire for content the classifier
// cannot recognize — it would answer "not ours" and re-materialize our own post
// as if a stranger had written it.
//
// PRE-EXISTING COVERAGE (deliberately not duplicated): consume/atomicity_test.go
// pins that a FAILED enqueue leaves no outbound_objects row and no gate row, and
// outer_acceptance_test.go pins the activity+delivery halves of tx rollback for a
// COMMENT intent. Neither covers the ap_objects mapping, the FK, or the positive
// "recognizable the instant it is deliverable" statement — which is the half the
// classifier's soundness rests on.

const premiseInbox = "https://lemmy.world/inbox"

// The InboxResolver seam is pinned to one inbox (worker_test.go's staticInbox):
// this file is about what the enqueue WRITES, not about discovering where it
// goes.

// premiseWorld is a database with the persona the enqueuer signs as, plus the
// enqueuer and a classifier reading the same tables.
func premiseWorld(t *testing.T) (*sql.DB, *Enqueuer, *echo.Classifier) {
	t.Helper()
	conn := testutil.DB(t)
	testutil.Truncate(t, conn,
		"outbound_deliveries", "outbound_activities", "outbound_objects",
		"ap_objects", "ap_actors", "communities")

	_, err := store.NewAPActors(conn).Create(context.Background(), store.APActor{
		DID:              pageAuthorDID,
		Kind:             store.ActorTypePerson,
		ActorID:          pageActorID,
		NormalizedOrigin: "coves.social",
		LocalPart:        "premiseauthor",
		RSAKeySealed:     []byte{0x01, 0x02, 0x03},
		RSAKeyVersion:    1,
		PublicKeyPEM:     "-----BEGIN PUBLIC KEY-----\nMIIB\n-----END PUBLIC KEY-----\n",
	})
	require.NoError(t, err, "mint the persona the activity is signed as")

	enqueuer, err := NewEnqueuer(EnqueuerOptions{
		DB:         conn,
		Translator: NewTranslator(pageUserOrigin),
		Inboxes:    staticInbox{inbox: premiseInbox},
		Actors:     store.NewAPActors(conn),
		UserOrigin: pageUserOrigin,
	})
	require.NoError(t, err)

	classifier, err := echo.New(echo.Options{
		Objects:         store.NewAPObjects(conn),
		OutboundObjects: store.NewOutboundObjects(conn),
		Activities:      store.NewOutboundActivities(conn),
		Actors:          store.NewAPActors(conn),
	})
	require.NoError(t, err)
	return conn, enqueuer, classifier
}

func premisePostIntent(t *testing.T) consume.PostIntent {
	t.Helper()
	return consume.PostIntent{
		Op:            "create",
		ATURI:         pagePostATURI,
		ID:            consume.ActivityID(pageUserOrigin, pagePostATURI, "create", 0),
		CommunityAPID: pageCommunityAPI,
		Snapshot:      postSnapshot(t, linkPostRecord()),
	}
}

// TestDeliveryCannotExistWithoutItsActivity is A1: the invariant as SCHEMA, so
// it holds for code that has not been written yet.
//
// Every other guarantee here is a property of one function. This one is a
// property of the database: whatever writes a delivery — a future fan-out, a
// backfill, an admin repair — cannot create one naming an activity we hold no
// payload for. That is what makes "an echo always names an id we can look up" a
// statement about the system rather than about the enqueuer.
func TestDeliveryCannotExistWithoutItsActivity(t *testing.T) {
	conn, _, _ := premiseWorld(t)

	_, err := conn.ExecContext(context.Background(), `
		INSERT INTO outbound_deliveries (activity_id, target_inbox, ordering_key)
		VALUES ($1, $2, $3)`,
		pageUserOrigin+"/ap/activity/never-minted-by-anyone", premiseInbox, pageCommunityAPI)

	require.Error(t, err,
		"a delivery whose activity we never recorded must be UNREPRESENTABLE: it would put "+
			"an activity on the wire whose echo the classifier cannot recognize, and the "+
			"echo would then re-enter as somebody else's content")
	assert.Contains(t, err.Error(), "outbound_deliveries_activity_id_fkey",
		"the rejection comes from the foreign key, not from an application check that a "+
			"future writer could forget: %v", err)
	assert.Zero(t, countRows(t, conn, "outbound_deliveries"))
}

// TestRolledBackEnqueueLeavesNoActivityDeliveryOrMapping is A2. The enqueue runs
// on the CALLER's transaction — the acceptance commit for a post — so a rolled
// back acceptance must leave nothing at all. A surviving mapping would be worse
// than a surviving activity: the activity is inert without its delivery, but a
// mapping is what the classifier reads.
func TestRolledBackEnqueueLeavesNoActivityDeliveryOrMapping(t *testing.T) {
	conn, enqueuer, classifier := premiseWorld(t)
	ctx := context.Background()
	intent := premisePostIntent(t)

	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, enqueuer.EnqueueActivity(ctx, tx, pageAuthorDID, pageCommunityAPI, "", intent),
		"the enqueue itself succeeds; it is the CALLER's commit that decides")
	require.NoError(t, tx.Rollback())

	assert.Zero(t, countRows(t, conn, "outbound_activities"),
		"no activity may survive a rolled-back acceptance")
	assert.Zero(t, countRows(t, conn, "outbound_deliveries"),
		"and no delivery: nothing was federated, so nothing may be on the queue")

	_, err = store.NewAPObjects(conn).GetByAPID(ctx, pagePostAPID)
	assert.True(t, errors.IsNotFound(err),
		"and NO bridge-origin mapping: a mapping that outlives its transaction claims we "+
			"federated a post we never did (err=%v)", err)

	// Stated the way the classifier sees it: there is nothing half-there.
	for _, id := range []string{intent.ID, pagePostAPID} {
		identity, err := classifier.Identify(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, echo.ClassNone, identity.Class,
			"%s must be unknown to the classifier — a rolled-back enqueue leaves no id to "+
				"half-recognize", id)
	}
}

// TestCommittedEnqueueIsImmediatelyRecognizableAsOurs is A3: the premise stated
// POSITIVELY, and the reason the mapping-arrival race is theoretical rather than
// merely unobserved.
//
// The moment the acceptance commits, the activity is deliverable — the worker
// can claim it on the next tick. In that same instant the classifier must
// already answer "ours" for BOTH ids a community can echo back: the activity id
// (Announce{Create}, Announce of a bare IRI) and the object id (Announce{Page}).
// There is no window in between, because there is no second commit.
func TestCommittedEnqueueIsImmediatelyRecognizableAsOurs(t *testing.T) {
	conn, enqueuer, classifier := premiseWorld(t)
	ctx := context.Background()
	intent := premisePostIntent(t)

	tx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, enqueuer.EnqueueActivity(ctx, tx, pageAuthorDID, pageCommunityAPI, "", intent))
	require.NoError(t, tx.Commit())

	// All three, from one commit.
	activity, err := store.NewOutboundActivities(conn).Get(ctx, intent.ID)
	require.NoError(t, err, "the activity id is persisted BEFORE any delivery can claim it")
	assert.Equal(t, pageAuthorDID, activity.ActorDID)
	delivery, err := store.NewOutboundDeliveries(conn).Get(ctx, intent.ID, premiseInbox)
	require.NoError(t, err, "the delivery exists — the activity is now on the wire's doorstep")
	assert.Equal(t, store.DeliveryStatePending, delivery.State)
	mapping, err := store.NewAPObjects(conn).GetByAPID(ctx, pagePostAPID)
	require.NoError(t, err, "and the bridge-origin mapping is there, in the same commit")
	assert.Equal(t, store.OriginBridge, mapping.Origin)

	// And therefore: every id this activity can come back under is already ours.
	activityIdentity, err := classifier.Identify(ctx, intent.ID)
	require.NoError(t, err)
	assert.Equal(t, echo.ClassLocalActivity, activityIdentity.Class,
		"Announce{Create{...}} and Announce{<activity IRI>} both resolve through this id")
	assert.Equal(t, pageAuthorDID, activityIdentity.DID)

	objectIdentity, err := classifier.Identify(ctx, pagePostAPID)
	require.NoError(t, err)
	assert.Equal(t, echo.ClassMappedObject, objectIdentity.Class,
		"Announce{Page} carries no activity id at all, so the object id must answer too")
	assert.Equal(t, pagePostATURI, objectIdentity.ATURI)
}
