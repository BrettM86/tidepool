package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/testutil"
)

// TASK 17d REVIEW — TWO RULES EVERY CANCELLATION AND EVERY REPLAY RESTS ON.
//
// (R1) THE STANDING ROW WINS. EnqueueTx is ON CONFLICT DO NOTHING plus a
// read-back, so re-enqueueing a delivery that already exists returns what is
// there rather than resetting it. That is deliberate and it is load-bearing in a
// direction nothing pins: the destructive tier commits on its own transaction
// while the rev gate commits later, so a failure in between REPLAYS the whole
// record — and every part of the purge is re-run. If a re-enqueue reset a
// delivered row to pending, the replay would re-POST a Delete{Person} the peer
// already applied; if it revived a cancelled one, a user's withdrawal would be
// un-withdrawn by a retry they never asked for.
//
// Today the rule is pinned only for pending↔pending, where nothing observable
// changes either way. These two tests pin it where the difference is real, so a
// future "fix" that resets the row reads as the regression it is instead of as
// an improvement.
//
// (R2) A DELIVERY HELD FOR SETTLEMENT IS NOT CANCELLABLE. notHeldForSettlement
// is pasted into FOUR statements and tested against one. The two below are the
// doors that had no test: 17c-3's community ban, and the community-wide sweep.
// The rule is the same everywhere — a cancellation answers "this must not go
// out", and a held delivery already went out, so cancelling it strands the
// ledger row it was held to settle — and the harm is remote from the code:
// a stranded row over-counts a served score forever AND hides the vote from the
// destructive tier's Undo enumeration.

// standingTestDB is deliveryTestDB plus the ban table, which the ban door writes.
func standingTestDB(t *testing.T) *sql.DB {
	t.Helper()
	database := deliveryTestDB(t)
	testutil.Truncate(t, database, "community_bans")
	return database
}

// seedOneDelivery inserts an activity and one pending delivery for it, and
// returns the delivery's key.
func seedOneDelivery(t *testing.T, database *sql.DB, activityID, actorDID, inbox, orderingKey string) {
	t.Helper()
	seedActivity(t, NewOutboundActivities(database), OutboundActivity{
		ActivityID: activityID,
		ActorDID:   actorDID,
		Kind:       "Create",
		Payload:    []byte(`{"type":"Create"}`),
	})
	_, err := NewOutboundDeliveries(database).Enqueue(context.Background(), OutboundDelivery{
		ActivityID: activityID, TargetInbox: inbox, OrderingKey: orderingKey,
	})
	require.NoError(t, err)
}

// holdForSettlement drives a pending delivery into the held state through the
// real path — claim, then release under the settlement class — so the row is
// one the worker actually produces rather than one an UPDATE invented.
func holdForSettlement(t *testing.T, repo OutboundDeliveries, activityID, inbox string) {
	t.Helper()
	ctx := context.Background()
	claimed, err := repo.ClaimNext(ctx, time.Minute)
	require.NoError(t, err)
	require.Equal(t, activityID, claimed.ActivityID, "the delivery under test is the one claimed")
	require.Equal(t, inbox, claimed.TargetInbox)
	_, applied, err := repo.Release(ctx, activityID, inbox, DeliveryHeldForSettlement,
		"the peer accepted it; the local ledger write did not commit", 202,
		time.Now().Add(time.Millisecond), *claimed.ClaimedUntil)
	require.NoError(t, err)
	require.True(t, applied, "the hold must actually be recorded")

	held, err := repo.Get(ctx, activityID, inbox)
	require.NoError(t, err)
	require.Equal(t, DeliveryStatePending, held.State,
		"precondition: a held delivery is PENDING — that is what makes it re-claimable, and "+
			"also what puts it in reach of every cancellation")
	require.Equal(t, DeliveryHeldForSettlement, held.LastErrorClass)
}

// ---------------------------------------------------------------------------
// R1 — the standing row wins
// ---------------------------------------------------------------------------

func TestOutboundDeliveries_EnqueueTxNeverRevivesATerminalDelivery(t *testing.T) {
	for _, tc := range []struct {
		name  string
		drive func(t *testing.T, repo OutboundDeliveries, activityID, inbox string)
		want  DeliveryState
		why   string
	}{
		{
			name: "cancelled",
			drive: func(t *testing.T, repo OutboundDeliveries, _, _ string) {
				cancelled, err := repo.CancelForActor(context.Background(), testDID)
				require.NoError(t, err)
				require.EqualValues(t, 1, cancelled)
			},
			want: DeliveryStateCancelled,
			why: "a cancelled delivery was withdrawn at the USER's request. A replay that " +
				"revived it would publish on their behalf something they had already taken " +
				"back — and the replay is not hypothetical: the destructive tier commits " +
				"separately from the rev gate, so a rollback re-runs the whole enqueue",
		},
		{
			name: "delivered",
			drive: func(t *testing.T, repo OutboundDeliveries, activityID, inbox string) {
				ctx := context.Background()
				claimed, err := repo.ClaimNext(ctx, time.Minute)
				require.NoError(t, err)
				_, applied, err := repo.MarkDelivered(ctx, activityID, inbox, 202, *claimed.ClaimedUntil)
				require.NoError(t, err)
				require.True(t, applied)
			},
			want: DeliveryStateDelivered,
			why: "a delivered row is the record that a peer HAS this activity. Resetting it to " +
				"pending re-POSTs a Delete{Person} that instance already applied, and for the " +
				"one tier that must never be asked twice",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			database := standingTestDB(t)
			repo := NewOutboundDeliveries(database)
			ctx := context.Background()
			seedOneDelivery(t, database, delActivityID, testDID, delTargetInbox, delOrderingKey)
			tc.drive(t, repo, delActivityID, delTargetInbox)

			// The replay: the same activity enqueued to the same inbox again.
			tx, err := database.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			returned, err := repo.EnqueueTx(ctx, tx, OutboundDelivery{
				ActivityID: delActivityID, TargetInbox: delTargetInbox, OrderingKey: delOrderingKey,
			})
			require.NoError(t, err, "a re-enqueue is not an error: the caller's contract is a row that exists")
			require.NoError(t, tx.Commit())

			require.NotNil(t, returned)
			assert.Equal(t, tc.want, returned.State,
				"EnqueueTx must hand back the STANDING row, unchanged: %s", tc.why)

			stored, err := repo.Get(ctx, delActivityID, delTargetInbox)
			require.NoError(t, err)
			assert.Equal(t, tc.want, stored.State, "and the row in the table is unchanged too: %s", tc.why)
			assert.Equal(t, 1, countDeliveries(t, database),
				"with no second row beside it — the pair (activity, inbox) is one delivery, "+
					"however many times a replay asks for it")
		})
	}
}

func countDeliveries(t *testing.T, database *sql.DB) int {
	t.Helper()
	var n int
	require.NoError(t, database.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM outbound_deliveries`).Scan(&n))
	return n
}

// ---------------------------------------------------------------------------
// R2 — the other two doors into a held delivery
// ---------------------------------------------------------------------------

// TestCommunityBans_BanLeavesADeliveryHeldForSettlement is the 17c-3 door.
//
// A ban cancels the banned author's pending work in that community, and a held
// delivery is pending. The vote it was holding to settle is one the community
// ALREADY has — banning the author does not un-cast it — so stranding the ledger
// row leaves that vote counted against the community's own score forever.
func TestCommunityBans_BanLeavesADeliveryHeldForSettlement(t *testing.T) {
	database := standingTestDB(t)
	repo := NewOutboundDeliveries(database)
	ctx := context.Background()

	// The held one, and an ordinary pending one beside it in the SAME community:
	// without the second row, "the ban cancelled nothing" and "the ban spared the
	// held row" are the same observation.
	seedOneDelivery(t, database, delActivityID, testDID, delTargetInbox, delOrderingKey)
	holdForSettlement(t, repo, delActivityID, delTargetInbox)
	seedOneDelivery(t, database, delOtherActivityID, testDID, delTargetInbox+"/second", delOrderingKey)

	cancelled, err := NewCommunityBans(database).Ban(ctx, CommunityBan{
		CommunityDID:  testCommunityDID,
		SubjectDID:    testDID,
		CommunityAPID: delOrderingKey,
		Reason:        "spam",
	})
	require.NoError(t, err)
	assert.EqualValues(t, 1, cancelled,
		"the ban cancels the author's ordinary queued work — the fixture is live")

	held, err := repo.Get(ctx, delActivityID, delTargetInbox)
	require.NoError(t, err)
	assert.Equal(t, DeliveryStatePending, held.State,
		"but NOT the delivery held for settlement: the peer already has that activity, so "+
			"there is nothing left to stop — cancelling is terminal, the worker never returns, "+
			"and the ledger row it was held to settle is stranded where no reseed corrects it")
	assert.Equal(t, DeliveryHeldForSettlement, held.LastErrorClass,
		"and it keeps the class that tells the next claim to resume at the settlement")

	other, err := repo.Get(ctx, delOtherActivityID, delTargetInbox+"/second")
	require.NoError(t, err)
	assert.Equal(t, DeliveryStateCancelled, other.State,
		"the ban still does its job on the work that has NOT gone out")
}

// TestOutboundDeliveries_CancelForCommunityLeavesADeliveryHeldForSettlement is
// the community-wide door: a community deleted or unfollowed out from under
// pending work. Same rule, and the same reason — what has already reached the
// peer is not ours to un-send.
func TestOutboundDeliveries_CancelForCommunityLeavesADeliveryHeldForSettlement(t *testing.T) {
	database := standingTestDB(t)
	repo := NewOutboundDeliveries(database)
	ctx := context.Background()

	seedOneDelivery(t, database, delActivityID, testDID, delTargetInbox, delOrderingKey)
	holdForSettlement(t, repo, delActivityID, delTargetInbox)
	seedOneDelivery(t, database, delOtherActivityID, testDID, delTargetInbox+"/second", delOrderingKey)

	cancelled, err := repo.CancelForCommunity(ctx, delOrderingKey)
	require.NoError(t, err)
	assert.EqualValues(t, 1, cancelled, "the ordinary pending delivery is cancelled — the fixture is live")

	held, err := repo.Get(ctx, delActivityID, delTargetInbox)
	require.NoError(t, err)
	assert.Equal(t, DeliveryStatePending, held.State,
		"the held delivery survives a community-wide cancellation too: the settlement it is "+
			"waiting to finish is about an activity the community already received")
}
