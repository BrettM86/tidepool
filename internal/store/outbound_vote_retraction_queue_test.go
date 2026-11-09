package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/testutil"
)

// Vote Undos in the delivery queue after a vote flip: the redrive of a
// superseded Undo, and which Undos a community ban spares.

// voteQueueTestDB is standingTestDB plus the vote rows.
func voteQueueTestDB(t *testing.T) *sql.DB {
	t.Helper()
	database := standingTestDB(t)
	testutil.Truncate(t, database, "outbound_votes")
	return database
}

// seedQueued inserts an activity with the given payload and one delivery for
// it, then forces the delivery into state (a fixture shortcut: the claim path
// is not what these tests are about).
func seedQueued(t *testing.T, database *sql.DB, kind, activityID string, payload map[string]any, state DeliveryState) {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	seedActivity(t, NewOutboundActivities(database), OutboundActivity{
		ActivityID: activityID, ActorDID: testDID, Kind: kind, Payload: raw,
	})
	_, err = NewOutboundDeliveries(database).Enqueue(context.Background(), OutboundDelivery{
		ActivityID: activityID, TargetInbox: delTargetInbox, OrderingKey: delOrderingKey,
	})
	require.NoError(t, err)
	_, err = database.ExecContext(context.Background(),
		`UPDATE outbound_deliveries SET state = $2 WHERE activity_id = $1`, activityID, string(state))
	require.NoError(t, err)
}

func votePayload(voteType, activityID, subjectAPID string) map[string]any {
	return map[string]any{"type": voteType, "id": activityID, "object": subjectAPID}
}

func voteUndoPayload(undoID, voteType, innerID, subjectAPID string) map[string]any {
	return map[string]any{"type": "Undo", "id": undoID, "object": votePayload(voteType, innerID, subjectAPID)}
}

func deliveryStateOf(t *testing.T, repo OutboundDeliveries, activityID string) DeliveryState {
	t.Helper()
	got, err := repo.Get(context.Background(), activityID, delTargetInbox)
	require.NoError(t, err)
	return got.State
}

var (
	flipLikeA        = "https://coves.social/ap/activity/" + repeatHex('5')
	flipUndoA        = "https://coves.social/ap/activity/" + repeatHex('6')
	flipDislikeB     = "https://coves.social/ap/activity/" + repeatHex('7')
	otherSubjectAPID = "https://lemmy.world/post/67890"
	otherLike        = "https://coves.social/ap/activity/" + repeatHex('8')
	otherUndo        = "https://coves.social/ap/activity/" + repeatHex('9')
	poisonedCreate   = "https://coves.social/ap/activity/" + repeatHex('e')
)

// TestRedrivePoisoned_RefusesAVoteUndoSupersededByALiveVote: A's Undo poisons,
// the flip's Dislike B is delivered, then an operator redrives. Lemmy matches
// an Undo{vote} on (actor, object), so the redriven Undo would withdraw B on
// the peer while B's row still reads delivered. It must stay poisoned, while
// other poisoned rows are redriven as before.
func TestRedrivePoisoned_RefusesAVoteUndoSupersededByALiveVote(t *testing.T) {
	database := voteQueueTestDB(t)
	deliveries := NewOutboundDeliveries(database)
	votes := NewOutboundVotes(database)
	ctx := context.Background()

	voteA := testOutboundVote()
	voteA.CurrentActivityID = flipLikeA
	voteA.DeliveredState = DeliveredStateDelivered
	voteA.RetractionActivityID = flipUndoA
	_, err := votes.Upsert(ctx, voteA)
	require.NoError(t, err)
	voteB := testOutboundVote()
	voteB.VoteATURI = testOtherVoteATURI
	voteB.Direction = "down"
	voteB.CurrentActivityID = flipDislikeB
	voteB.DeliveredState = DeliveredStateDelivered
	_, err = votes.Upsert(ctx, voteB)
	require.NoError(t, err)

	seedQueued(t, database, "Like", flipLikeA, votePayload("Like", flipLikeA, testSubjectAPID), DeliveryStateDelivered)
	seedQueued(t, database, "Undo", flipUndoA, voteUndoPayload(flipUndoA, "Like", flipLikeA, testSubjectAPID), DeliveryStatePoisoned)
	seedQueued(t, database, "Dislike", flipDislikeB, votePayload("Dislike", flipDislikeB, testSubjectAPID), DeliveryStateDelivered)

	// A vote Undo nothing replaced (the purge's shape: the row is still live
	// under the very Like the Undo withdraws) is redriven.
	other := testOutboundVote()
	other.VoteATURI = "at://" + testDID + "/social.coves.feed.vote/3lzvoteothrrr"
	other.SubjectATURI = "at://" + testCommunityDID + "/social.coves.community.postv2/3lzpostothrrr"
	other.SubjectAPID = otherSubjectAPID
	other.CurrentActivityID = otherLike
	_, err = votes.Upsert(ctx, other)
	require.NoError(t, err)
	seedQueued(t, database, "Undo", otherUndo, voteUndoPayload(otherUndo, "Like", otherLike, otherSubjectAPID), DeliveryStatePoisoned)
	seedQueued(t, database, "Create", poisonedCreate, map[string]any{"type": "Create"}, DeliveryStatePoisoned)

	redriven, superseded, err := deliveries.RedrivePoisoned(ctx, flipUndoA, "")
	require.NoError(t, err)
	assert.Equal(t, [2]int64{0, 1}, [2]int64{redriven, superseded}, "a scoped redrive refuses it too")

	redriven, superseded, err = deliveries.RedrivePoisoned(ctx, "", "")
	require.NoError(t, err)

	assert.Equal(t, DeliveryStatePoisoned, deliveryStateOf(t, deliveries, flipUndoA),
		"the superseded Undo is not requeued")
	assert.Equal(t, DeliveryStatePending, deliveryStateOf(t, deliveries, otherUndo), "an unsuperseded Undo is redriven")
	assert.Equal(t, DeliveryStatePending, deliveryStateOf(t, deliveries, poisonedCreate), "a poisoned Create is redriven")
	assert.Equal(t, [2]int64{2, 1}, [2]int64{redriven, superseded}, "redriven, superseded")
}

// TestRedrivePoisoned_RedrivesAVoteUndoWhoseReplacementCannotDeliver: A is
// delivered, its Undo poisons, and the flip's Like B is queued behind it. While
// B's delivery is pending the peer can still receive B, so the Undo stays
// superseded. Once B's delivery is cancelled (a ban, an opt-out, an operator
// cancel) nothing will replace A on the peer: the Undo is owed again and the
// redrive must requeue it, or the peer keeps A for good.
func TestRedrivePoisoned_RedrivesAVoteUndoWhoseReplacementCannotDeliver(t *testing.T) {
	database := voteQueueTestDB(t)
	deliveries := NewOutboundDeliveries(database)
	votes := NewOutboundVotes(database)
	ctx := context.Background()

	voteA := testOutboundVote()
	voteA.CurrentActivityID = flipLikeA
	voteA.DeliveredState = DeliveredStateDelivered
	voteA.RetractionActivityID = flipUndoA
	_, err := votes.Upsert(ctx, voteA)
	require.NoError(t, err)
	voteB := testOutboundVote()
	voteB.VoteATURI = testOtherVoteATURI
	voteB.Direction = "down"
	voteB.CurrentActivityID = flipDislikeB
	voteB.DeliveredState = DeliveredStatePending
	_, err = votes.Upsert(ctx, voteB)
	require.NoError(t, err)

	seedQueued(t, database, "Like", flipLikeA, votePayload("Like", flipLikeA, testSubjectAPID), DeliveryStateDelivered)
	seedQueued(t, database, "Undo", flipUndoA, voteUndoPayload(flipUndoA, "Like", flipLikeA, testSubjectAPID), DeliveryStatePoisoned)
	seedQueued(t, database, "Dislike", flipDislikeB, votePayload("Dislike", flipDislikeB, testSubjectAPID), DeliveryStatePending)

	redriven, superseded, err := deliveries.RedrivePoisoned(ctx, "", "")
	require.NoError(t, err)
	assert.Equal(t, [2]int64{0, 1}, [2]int64{redriven, superseded},
		"a replacement still pending delivery supersedes the Undo")
	assert.Equal(t, DeliveryStatePoisoned, deliveryStateOf(t, deliveries, flipUndoA))

	_, err = database.ExecContext(ctx,
		`UPDATE outbound_deliveries SET state = $2 WHERE activity_id = $1`, flipDislikeB, string(DeliveryStateCancelled))
	require.NoError(t, err)

	redriven, superseded, err = deliveries.RedrivePoisoned(ctx, "", "")
	require.NoError(t, err)
	assert.Equal(t, [2]int64{1, 0}, [2]int64{redriven, superseded},
		"a replacement whose delivery was cancelled does not supersede the Undo")
	assert.Equal(t, DeliveryStatePending, deliveryStateOf(t, deliveries, flipUndoA), "the owed Undo is requeued")
}
