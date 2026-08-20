package outbound

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// THE DEDUPE CLASSIFICATION IS THE ONE PLACE A REJECTION BECOMES A SUCCESS.
//
// classify() maps Lemmy's received-activity dedupe — a 400 whose body says the
// activity was already received — onto DELIVERED, because a redelivery after a
// crash is expected and safe. Everything else about a 400 is a refusal.
//
// Matching on the bare word "already" cannot tell those apart. Lemmy answers 400
// with a whole family of slugs carrying it — banned_from_community,
// person_is_blocked, already_invalid, duplicate_title — and reading any of them
// as a delivery is not a missed retry, it is a FABRICATED one:
//
//   - a Like the peer refused flips outbound_votes.delivered_state, and that
//     column is an input to the score users are served (task 17b);
//   - stampAccepted opens the causal gate for an object that never landed, so
//     every child delivered behind it is rejected in turn;
//   - the delivered path stores no excerpt, so the body that would have shown
//     an operator what really happened is discarded.
//
// Each case below is a real 400 body containing "already" that is NOT the
// dedupe, and each must follow the ordinary 4xx path.

func TestWorker_Non_DedupeFourHundredContainingAlreadyIsNotDelivered(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"banned from community", `{"error":"person_is_banned_from_community","message":"this person is already banned"}`},
		{"blocked by the recipient", `{"error":"You have already been blocked from this community"}`},
		{"already invalid", `{"error":"already_invalid"}`},
		{"duplicate title", `{"error":"duplicate_title","message":"a post with this title already exists"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := workerTestDB(t)
			seedWorkerActor(t, conn, true, false)
			id := seedDelivery(t, conn, "Create", "", createPayload("x"))
			setAttempts(t, conn, id, 2) // MaxAttempts is 3: the next 4xx decides

			w := newWorker(t, conn, senderReturning(httpErr(http.StatusBadRequest, tc.body)), nil)
			worked, err := w.DeliverNext(context.Background())
			require.NoError(t, err)
			require.True(t, worked)

			d := getDelivery(t, conn, id)
			assert.Equal(t, store.DeliveryStatePoisoned, d.State,
				"a 400 that merely CONTAINS \"already\" is a genuine rejection, not the "+
					"received-activity dedupe: it must follow the normal 4xx classification")
			assert.Equal(t, "4xx", d.LastErrorClass,
				"and it is labelled as the rejection it is")
			assert.Contains(t, d.ResponseExcerpt, "already",
				"with the peer's own words kept, which is the evidence the delivered path throws away")
		})
	}
}

// The Like case is the one with a number attached: a vote the peer REFUSED must
// not be recorded as delivered, because that column is subtracted from the score
// the API serves and nothing reconciles it afterwards.
func TestWorker_RefusedLikeDoesNotFlipTheVoteLedger(t *testing.T) {
	conn := workerTestDB(t)
	ctx := context.Background()
	seedWorkerActor(t, conn, true, false)

	likeID := "https://coves.social/ap/activity/" + repeatHex64("Like")
	voteATURI := "at://" + wActorDID + "/social.coves.feed.vote/3lzvotebanned"
	_, err := store.NewOutboundVotes(conn).Upsert(ctx, store.OutboundVote{
		VoteATURI:         voteATURI,
		ActorDID:          wActorDID,
		SubjectATURI:      "at://" + wCommunityDID + "/social.coves.community.postv2/3lzpost",
		SubjectAPID:       "https://lemmy.world/post/1",
		CommunityDID:      wCommunityDID,
		Direction:         "up",
		CurrentActivityID: likeID,
	})
	require.NoError(t, err)

	id := seedDelivery(t, conn, "Like", "", []byte(fmt.Sprintf(
		`{"id":%q,"type":"Like","actor":%q,"object":"https://lemmy.world/post/1"}`, likeID, wActorID)))
	setAttempts(t, conn, id, 2)

	// The author is banned in that community, so the vote is refused.
	w := newWorker(t, conn, senderReturning(
		httpErr(http.StatusBadRequest,
			`{"error":"person_is_banned_from_community","message":"this person is already banned"}`)), nil)
	worked, err := w.DeliverNext(ctx)
	require.NoError(t, err)
	require.True(t, worked)

	assert.Equal(t, store.DeliveryStatePoisoned, getDelivery(t, conn, id).State,
		"a refused Like is a refusal")

	vote, err := store.NewOutboundVotes(conn).GetByATURI(ctx, voteATURI)
	require.NoError(t, err)
	assert.Equal(t, store.DeliveredStatePending, vote.DeliveredState,
		"and the vote ledger must NOT read delivered for a vote the peer refused — that column "+
			"is an input to the score users are served, and nothing reconciles it later")
}

// The dedupe itself still lands, in both spellings the peer stack can produce:
// the prose sentence the fake Lemmy in the outer acceptance test returns, and
// the underscored slug an error enum serializes to. Without this the fix above
// could be "classify nothing as a duplicate", which re-poisons every redelivery
// after a crash.
func TestWorker_ReceivedActivityDedupeStillDelivers(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"prose", `{"error":"activity was already received"}`},
		{"capitalised prose", `{"error":"Activity was already received"}`},
		{"slug", `{"error":"already_received"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := workerTestDB(t)
			seedWorkerActor(t, conn, true, false)
			id := seedDelivery(t, conn, "Create", "", createPayload("x"))

			w := newWorker(t, conn, senderReturning(httpErr(http.StatusBadRequest, tc.body)), nil)
			worked, err := w.DeliverNext(context.Background())
			require.NoError(t, err)
			require.True(t, worked)

			assert.Equal(t, store.DeliveryStateDelivered, getDelivery(t, conn, id).State,
				"a body reporting the activity was ALREADY RECEIVED is our own redelivery coming "+
					"back: it is delivered, never poisoned")
		})
	}
}
