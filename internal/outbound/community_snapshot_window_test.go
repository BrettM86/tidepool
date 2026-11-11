package outbound

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCommunityRemoval_AnObjectDeliveredWhileTheLabelTxIsOpenGetsItsDelete: a
// native comment into the community is enqueued and delivered after the
// community label's tx read the community's objects and before it took their
// locks. Neither the enqueue nor the worker sees the uncommitted label, so the
// comment reaches Lemmy; the label must still take it down and send its
// Delete.
func TestCommunityRemoval_AnObjectDeliveredWhileTheLabelTxIsOpenGetsItsDelete(t *testing.T) {
	w := newCommunityRemovalWorld(t)
	w.deliverPosts(t, tdPost)

	var once sync.Once
	communityObjectsReadHook = func() {
		once.Do(func() {
			w.enqueueReply(t, crLaterComment)
			w.drain(t)
		})
	}
	t.Cleanup(func() { communityObjectsReadHook = nil })

	w.commitCommunityLabel(t, false)
	communityObjectsReadHook = nil
	require.Equal(t, []string{"Create " + crLaterComment.apID()}, w.receivedLinesSince(t, 1),
		"setup: the comment was delivered while the label tx was open")
	w.drain(t)

	assert.ElementsMatch(t, []string{"Delete " + tdPostAPID, "Delete " + crLaterComment.apID()},
		w.receivedLinesSince(t, 2), "the label deletes the post and the comment delivered during its tx")
}
