package outbound

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/consume"
	"tidepool/internal/store"
)

// TASK 17d — ONE ACTIVITY, MANY INBOXES.
//
// Every activity this bridge has sent so far goes to exactly one community, so
// the enqueuer's idempotency shortcut has been correct by coincidence: when the
// activity row is already there, its delivery is already there too, and
// returning early saves a PK violation.
//
//	inserted, err := e.activities.InsertTx(...)
//	if !inserted { return nil }   // ← skips the delivery insert
//
// The destructive opt-out tier breaks that premise. `Delete{Person,
// removeData:true}` is ONE activity addressed to EVERY community this actor
// ever delivered to — the fan-out the outbound schema was built for (migration
// 020's comment names this exact case). The moment the second inbox is enqueued
// under an activity id the first one already inserted, the shortcut returns
// before writing anything, and the user's erasure request reaches ONE instance
// out of however many hold their content.
//
// The failure is silent and looks like success: an activity row exists, a
// delivery row exists, the worker delivers it, the metrics are clean. Only a
// count of delivery rows against the actor's delivery history tells the truth —
// and nothing counts that. For a request that cannot be re-asked (peers do not
// un-delete, and asking twice risks deleting content a re-enabled user has since
// restored) reaching one instance is worse than failing loudly.
//
// TWO PASSES, because they fail differently. The FIRST pass is the fan-out
// itself. The SECOND is the redelivery — and the redelivery is the one that
// matters most in production, since the destructive tier is precisely the work
// most likely to be retried after a crash. A test that enqueues once and counts
// rows would pass on an implementation that reaches one instance on every
// retry.
const (
	fanCommunityB = "https://lemmy.zip/c/technology"
	fanInboxA     = "https://lemmy.world/inbox"
	fanInboxB     = "https://lemmy.zip/inbox"
)

// perCommunityInboxes answers a DIFFERENT inbox per community — the real shape,
// since each instance hosts its own. (Co-hosted communities share one, which is
// why the delivery PK is (activity, inbox) and the ordering key is the
// community: the two are independent axes.)
type perCommunityInboxes struct {
	inboxes    map[string]string
	calledWith []string
}

func (r *perCommunityInboxes) ResolveInbox(_ context.Context, communityAPID string) (string, error) {
	r.calledWith = append(r.calledWith, communityAPID)
	return r.inboxes[communityAPID], nil
}

// TestEnqueuer_OneActivityReachesEveryInboxNotJustTheFirst is the fan-out
// contract the destructive tier rests on.
func TestEnqueuer_OneActivityReachesEveryInboxNotJustTheFirst(t *testing.T) {
	conn := enqueuerTestDB(t)
	ctx := context.Background()
	seedEnqueuerActor(t, conn)

	resolver := &perCommunityInboxes{inboxes: map[string]string{
		outCommunityAPID: fanInboxA,
		fanCommunityB:    fanInboxB,
	}}
	enq := newTestEnqueuer(t, conn, resolver)

	// ONE activity id, addressed to two communities on two instances. The intent
	// type is incidental — Delete{Person} has no intent yet, and the property is
	// about the ACTIVITY and DELIVERY rows, which are shared by every intent.
	base := commentEnqueueIntent(t)
	fanOut := func(t *testing.T, pass string) {
		t.Helper()
		for _, community := range []string{outCommunityAPID, fanCommunityB} {
			intent := base
			intent.CommunityAPID = community
			tx, err := conn.BeginTx(ctx, nil)
			require.NoError(t, err)
			require.NoError(t,
				enq.EnqueueActivity(ctx, tx, outCommenterDID, outCommenterDID, "", intent),
				"%s: enqueueing %s must not fail — an erasure request that errors half way "+
					"through its fan-out leaves the user's intent partly applied with no record "+
					"of which peers were reached", pass, community)
			require.NoError(t, tx.Commit())
		}
	}

	// --- PASS ONE: the fan-out.
	fanOut(t, "first pass")

	assert.Equal(t, 1, count(t, conn, "outbound_activities"),
		"ONE canonical activity: the same erasure request, not one per peer — the id is what "+
			"makes a redelivery recognisable to the peer as the same activity")
	assert.Equal(t, 2, count(t, conn, "outbound_deliveries"),
		"and a delivery PER INBOX: the fan-out schema exists for exactly this, and one row "+
			"for two instances means the user's content stays live on every instance but the "+
			"first — silently, with every metric reading clean")

	require.Contains(t, resolver.calledWith, fanCommunityB,
		"the second community's inbox must even be RESOLVED: the early return fires before "+
			"the resolver is consulted, so a fan-out that never asks is one that never intended "+
			"to deliver")

	for _, target := range []struct{ inbox, orderingKey string }{
		{fanInboxA, outCommunityAPID},
		{fanInboxB, fanCommunityB},
	} {
		delivery, err := store.NewOutboundDeliveries(conn).Get(ctx, base.ID, target.inbox)
		require.NoError(t, err,
			"a delivery must exist for %s: this is the row that carries the request to that "+
				"instance, and its absence is the erasure silently not happening there",
			target.inbox)
		assert.Equal(t, store.DeliveryStatePending, delivery.State)
		assert.Equal(t, target.orderingKey, delivery.OrderingKey,
			"addressed on that community's own serial line: the inbox is shared by co-hosted "+
				"communities, so the ordering key is what keeps the lines independent")
	}

	// --- PASS TWO: the redelivery. Same activity id, same inboxes, again.
	//
	// This is the shape production actually meets — the destructive tier is the
	// work most likely to be retried — and it is where the idempotency the early
	// return exists to provide has to hold WITHOUT costing the second inbox.
	fanOut(t, "redelivery")

	assert.Equal(t, 1, count(t, conn, "outbound_activities"),
		"still ONE activity after a redelivery: the id is deterministic, and a second row "+
			"would present the peer with a second erasure to apply")
	assert.Equal(t, 2, count(t, conn, "outbound_deliveries"),
		"and still exactly TWO deliveries — no duplicates, none lost. Idempotency and "+
			"completeness are not in tension here: the delivery PK is (activity, inbox), so "+
			"re-enqueueing the same pair is a no-op while a NEW pair is a row that must be "+
			"written")
}

// TestEnqueuer_ARedeliveredActivityToTheSameInboxStaysOneRow keeps the reason
// the early return exists.
//
// The shortcut is not arbitrary: the delivery PK is (activity_id, target_inbox),
// so re-inserting the same pair inside the caller's transaction would raise a
// unique violation and poison it — taking down the rev-gate advance riding the
// same tx with it. Whatever replaces the shortcut has to keep this true, or the
// fix for a silent under-delivery becomes a loud failure on every ordinary
// redelivery.
func TestEnqueuer_ARedeliveredActivityToTheSameInboxStaysOneRow(t *testing.T) {
	conn := enqueuerTestDB(t)
	ctx := context.Background()
	seedEnqueuerActor(t, conn)

	enq := newTestEnqueuer(t, conn, &fakeInboxResolver{inbox: outSharedInbox})
	intent := commentEnqueueIntent(t)

	for pass := 1; pass <= 2; pass++ {
		tx, err := conn.BeginTx(ctx, nil)
		require.NoError(t, err)
		require.NoError(t, enq.EnqueueActivity(ctx, tx, outCommenterDID, outCommunityAPID, outRootATURI, intent),
			"pass %d must not error: a redelivered intent re-derives the same activity id, and "+
				"a unique violation here would poison the caller's transaction — the rev-gate "+
				"advance rides it, so the event would replay forever", pass)
		require.NoError(t, tx.Commit())
	}

	assert.Equal(t, 1, count(t, conn, "outbound_activities"), "one activity")
	assert.Equal(t, 1, count(t, conn, "outbound_deliveries"),
		"and ONE delivery: the same (activity, inbox) pair twice is the same delivery, and a "+
			"second row would send the peer a duplicate it has to recognise and discard")
}

// consumeIntentCompileGuard keeps the fan-out fixture honest about the seam it
// stands in for: whatever intent the destructive tier introduces, it reaches
// these same two tables through this same method.
var _ consume.Intent = consume.CommentIntent{}
