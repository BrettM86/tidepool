package outbound

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// TestAccountTakedown_AnObjectDeliveredWhileTheLabelTxIsOpenGetsItsDelete: a
// native comment by the labeled author is enqueued and delivered after the
// account label's tx read the author's objects and before it took their
// locks. Neither the enqueue nor the worker sees the uncommitted pause, so the
// comment reaches Lemmy; the label must still take it down and send its
// Delete.
func TestAccountTakedown_AnObjectDeliveredWhileTheLabelTxIsOpenGetsItsDelete(t *testing.T) {
	w := newAccountStatusWorld(t)
	w.deliverPosts(t, tdPost)

	var once sync.Once
	accountObjectsReadHook = func() {
		once.Do(func() {
			w.enqueueReply(t, crLaterComment)
			w.drain(t)
		})
	}
	t.Cleanup(func() { accountObjectsReadHook = nil })

	w.commitAccountLabel(t, tdAuthorDID, false)
	accountObjectsReadHook = nil
	require.Equal(t, []string{"Create " + crLaterComment.apID()}, w.receivedLinesSince(t, 1),
		"setup: the comment was delivered while the label tx was open")
	w.drain(t)

	assert.ElementsMatch(t, []string{"Delete " + tdPostAPID, "Delete " + crLaterComment.apID()},
		w.receivedLinesSince(t, 2), "the label deletes the post and the comment delivered during its tx")
}

// TestAccountTakedown_AnObjectEnqueuedAfterTheLabelLocksWaitsForThePause: a
// native comment by the labeled author is enqueued once the account label's
// tx holds its subject locks. The enqueue waits on the actor's subject lock
// until the label commits, so the worker reads the pause and holds the
// comment: Lemmy never receives it, and no Delete is owed.
func TestAccountTakedown_AnObjectEnqueuedAfterTheLabelLocksWaitsForThePause(t *testing.T) {
	w := newAccountStatusWorld(t)
	w.deliverPosts(t, tdPost)

	enqueued := make(chan struct{})
	var once sync.Once
	accountSubjectsLockedHook = func() {
		once.Do(func() {
			go func() {
				defer close(enqueued)
				w.enqueueReply(t, crLaterComment)
			}()
			// Until the enqueue either waits on the author's takedown-subject
			// lock ("td", hashtext of the DID) or commits.
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				select {
				case <-enqueued:
					// The enqueue did not wait: the worker delivers the
					// comment before the pause commits.
					w.drain(t)
					return
				default:
				}
				var waiting int
				require.NoError(t, w.conn.QueryRow(`
					SELECT count(*) FROM pg_locks
					WHERE locktype = 'advisory' AND NOT granted AND classid = x'7464'::int::oid
					  AND objid::bigint = (hashtext($1)::bigint & 4294967295)`, tdAuthorDID).Scan(&waiting))
				if waiting > 0 {
					return
				}
				time.Sleep(5 * time.Millisecond)
			}
			t.Error("the enqueue neither waited on a lock nor committed within 5s")
		})
	}
	t.Cleanup(func() { accountSubjectsLockedHook = nil })

	w.commitAccountLabel(t, tdAuthorDID, false)
	accountSubjectsLockedHook = nil
	select {
	case <-enqueued:
	case <-time.After(10 * time.Second):
		t.Fatal("the enqueue did not finish after the label committed")
	}
	w.drain(t)

	assert.Equal(t, []string{"Delete " + tdPostAPID}, w.receivedLinesSince(t, 1),
		"the label deletes the post, and the comment never reaches Lemmy")
	assert.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, crLaterComment.createID()),
		"the comment is held on the pause")
}
