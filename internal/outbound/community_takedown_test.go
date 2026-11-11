package outbound

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/labels"
	"tidepool/internal/labels/labelstest"
	"tidepool/internal/store"
)

// TestCommunityRemoval_NativeContentInTheCommunityIsDeletedOrHeldUntilTheNegation
// (B6): the `!takedown` on a bridged community's bare DID sends a Delete for
// the native post delivered there (Pn) and nothing for the delivered vote
// (Vd); the queued native comment (Cn), the queued reply under a bridged post
// (Rn), the delivery-only reply whose bridged parent is bound to the
// community only through ap_objects (Rd) and the pending vote (Vq) are held,
// as are a comment and a vote enqueued after the label. A native post in
// another community is delivered during the removal. The negation sends
// Undo{Delete} for Pn, then delivers every held row; nothing is cancelled.
func TestCommunityRemoval_NativeContentInTheCommunityIsDeletedOrHeldUntilTheNegation(t *testing.T) {
	w := newCommunityRemovalWorld(t)
	w.deliverPosts(t, tdPost)
	deliveredVote := w.vote(t, "3lzcrvote0001", crDeliveredVoteSubject)
	w.drain(t)
	require.Equal(t, []string{"Create " + tdPostAPID, "Like " + crDeliveredVoteSubject.apID}, w.receivedLines(t),
		"precondition: Pn and Vd are delivered")
	require.Equal(t, store.DeliveryStateDelivered, w.deliveryStateOf(t, deliveredVote))

	cn := commentEnqueueIntent(t).ID
	w.enqueueCommentCreate(t)
	w.enqueueReply(t, tdReplyToBridged)
	w.enqueueDeliveryOnlyReply(t, crDeliveryOnlyReply)
	pendingVote := w.vote(t, "3lzcrvote0002", crPendingVoteSubject)
	queued := map[string]string{
		"Cn": cn,
		"Rn": tdReplyToBridged.createID(),
		"Rd": crDeliveryOnlyReply.createID(),
		"Vq": pendingVote,
	}
	for name, id := range queued {
		require.Equal(t, store.DeliveryStatePending, w.deliveryStateOf(t, id), "setup: %s is queued", name)
	}

	w.commitCommunityLabel(t, false)
	w.drain(t)
	assert.Equal(t, []string{"Delete " + tdPostAPID}, w.receivedLinesSince(t, 2),
		"the community label sends Pn's Delete only: no Undo for Vd, nothing queued is delivered")
	for name, id := range queued {
		assert.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, id), "%s is held", name)
	}

	w.enqueueReply(t, crLaterComment)
	assert.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, crLaterComment.createID()),
		"a native comment enqueued into the removed community is held at enqueue")
	w.enqueueReply(t, crLaterReplyToBridged)
	assert.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, crLaterReplyToBridged.createID()),
		"a native reply to a bridged post in the removed community is held at enqueue")
	assert.Equal(t, store.HeldReasonParent, w.heldReasonOf(t, crLaterReplyToBridged.createID()),
		"setup: the later reply is held on its bridged parent")
	laterVote := w.vote(t, "3lzcrvote0003", crNewVoteSubject)
	assert.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, laterVote),
		"a new native vote on a bridged post in the removed community is held")

	otherPost := "Create " + crOtherCommunityPost.apID()
	w.enqueueOtherCommunityPost(t)
	w.drain(t)
	assert.Equal(t, []string{otherPost}, w.receivedLinesSince(t, 3),
		"control: a native post in another community is delivered during the removal")

	w.commitCommunityLabel(t, true)
	w.drain(t)
	restored := w.receivedLinesSince(t, 4)
	if assert.NotEmpty(t, restored, "the negation sends something") {
		assert.Equal(t, "Undo{Delete} "+tdPostAPID, restored[0], "the negation restores Pn first")
		assert.ElementsMatch(t, []string{
			"Create " + outCommentAPID,
			"Create " + tdReplyToBridged.apID(),
			"Create " + crDeliveryOnlyReply.apID(),
			"Like " + crPendingVoteSubject.apID,
			"Create " + crLaterComment.apID(),
			"Create " + crLaterReplyToBridged.apID(),
			"Like " + crNewVoteSubject.apID,
		}, restored[1:], "then every held row is delivered")
	}
	queued["later comment"] = crLaterComment.createID()
	queued["later reply to a bridged post"] = crLaterReplyToBridged.createID()
	queued["later vote"] = laterVote
	for name, id := range queued {
		assert.Equal(t, store.DeliveryStateDelivered, w.deliveryStateOf(t, id), "%s is delivered after the negation", name)
	}
	assert.Zero(t, cancelledDeliveries(t, w), "no row is cancelled")
}

// TestCommunityRemoval_ARecordLabelOnAPostInTheCommunityOutlivesTheNegation
// (B6, source isolation): Pk in the removed community also carries its own
// record `!takedown`. The community label sends it no second Delete, the
// community negation sends it no Undo{Delete}, and negating its record label
// then restores it.
func TestCommunityRemoval_ARecordLabelOnAPostInTheCommunityOutlivesTheNegation(t *testing.T) {
	w := newCommunityRemovalWorld(t)
	pk := tdSecondPost
	w.deliverPosts(t, tdPost, pk)
	w.commitLabel(t, pk.atURI(), false)
	w.drain(t)
	require.Equal(t, []string{"Delete " + pk.apID()}, w.receivedLinesSince(t, 2),
		"precondition: Pk's record label sends its Delete")

	w.commitCommunityLabel(t, false)
	w.drain(t)
	assert.Equal(t, []string{"Delete " + tdPostAPID}, w.receivedLinesSince(t, 3),
		"the community label deletes Pn and sends Pk no second Delete")

	w.commitCommunityLabel(t, true)
	w.drain(t)
	assert.Equal(t, []string{"Undo{Delete} " + tdPostAPID}, w.receivedLinesSince(t, 4),
		"the community negation restores Pn and sends Pk no Undo{Delete}")

	w.commitLabel(t, pk.atURI(), true)
	w.drain(t)
	assert.Equal(t, []string{"Undo{Delete} " + pk.apID()}, w.receivedLinesSince(t, 5),
		"negating Pk's record label restores it")
}

// TestCommunityRemoval_StartActivatesACommunityLabelStoredByTask02 (B6,
// activation): task 02's store (no community-aware applier) committed the
// community label at seq N with Pn delivered. A consumer with this chunk
// starting at cursor N, on a stream that sends no frame, deletes Pn.
func TestCommunityRemoval_StartActivatesACommunityLabelStoredByTask02(t *testing.T) {
	const storedSeq = 9
	w := newCommunityRemovalWorld(t)
	w.deliverPost(t)

	commitTakedownLabel(t, labels.NewPostgresStore(w.conn), w.labelKey, storedSeq, outCommunityDID, false)
	require.Zero(t, activityCount(t, w.conn, "Delete"), "precondition: task 02's commit enqueued nothing")

	server := labelstest.NewServer(t, func(int64) labelstest.Reply { return labelstest.Reply{Hold: true} })
	runLabelConsumer(t, w.labelStore, server, w.labelKey, storedSeq)
	w.drain(t)
	assert.Equal(t, []string{"Create " + tdPostAPID, "Delete " + tdPostAPID}, w.receivedLines(t),
		"the start deletes Pn")
}
