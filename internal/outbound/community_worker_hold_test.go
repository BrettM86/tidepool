package outbound

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// crUnboundParent is a bridged post whose ap_objects row names no community:
// a reply to it is bound to the removed community only by its ordering key,
// so neither its own object nor its parent holds it.
var crUnboundParent = crBridgedPost{rkey: "3lzcrbrdg0006", apID: "https://lemmy.world/post/5206"}

// crReplyToUnboundParent is a delivery-only native reply to crUnboundParent.
var crReplyToUnboundParent = tdReply{rkey: "3lztkdncmnt08", rev: "3lztkdncrev008",
	parentATURI: crUnboundParent.atURI(), parentAPID: crUnboundParent.apID}

// TestCommunityRemoval_WorkLeasedAtTheLabelIsHeldAtItsNextClaim: a delivery
// in lease when the community label commits is skipped by the label's hold;
// its POST fails, and the worker's next claim holds it on the community
// instead of delivering it. Covered for a Create bound to the community only
// by its ordering key and for a vote.
func TestCommunityRemoval_WorkLeasedAtTheLabelIsHeldAtItsNextClaim(t *testing.T) {
	for _, tc := range []struct {
		name    string
		enqueue func(t *testing.T, w *communityRemovalWorld) string
	}{
		{name: "reply", enqueue: func(t *testing.T, w *communityRemovalWorld) string {
			_, err := store.NewAPObjects(w.conn).PutMapping(context.Background(), store.APObjectMapping{
				APID:           crUnboundParent.apID,
				APType:         "Page",
				OriginInstance: outLemmyHost,
				DID:            crBridgedAuthorDID,
				AuthorDID:      crBridgedAuthorDID,
				Collection:     "social.coves.community.postv2",
				RKey:           crUnboundParent.rkey,
				CID:            tdPostCID,
			})
			require.NoError(t, err, "map the unbound parent")
			w.enqueueDeliveryOnlyReply(t, crReplyToUnboundParent)
			return crReplyToUnboundParent.createID()
		}},
		{name: "vote", enqueue: func(t *testing.T, w *communityRemovalWorld) string {
			return w.vote(t, "3lzcrvote0021", crNewVoteSubject)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newCommunityRemovalWorld(t)
			activityID := tc.enqueue(t, w)
			require.Equal(t, store.DeliveryStatePending, w.deliveryStateOf(t, activityID), "setup: the work is queued")

			w.answerUnavailable(1)
			w.commitLabelWhilePOSTInFlight(t, outCommunityDID)
			require.Equal(t, 1, w.rejectedPosts(), "setup: the leased attempt's POST failed")
			require.Equal(t, store.DeliveryStatePending, w.deliveryStateOf(t, activityID),
				"setup: the failed attempt returned the row to pending, unheld")

			w.drain(t)
			assert.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, activityID),
				"the next claim holds the row instead of delivering it")
			assert.Equal(t, store.HeldReasonCommunity, w.heldReasonOf(t, activityID), "it is held on the community")
			assert.Empty(t, w.receivedLines(t), "Lemmy received nothing")
		})
	}
}
