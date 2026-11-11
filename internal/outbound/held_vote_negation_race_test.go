package outbound

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/consume"
	"tidepool/internal/store"
)

// A vote write that cancels its held Like/Dislike races the community label's
// negation, which releases the work held on the community while it holds the
// community's takedown-subject lock. A re-cast takes the subject locks before
// the held delivery's row, as the negation does; a delete's Undo takes none.
// Neither waits on the negation in a cycle (40P01).

// waitingInHeldVoteCancel reports whether a session in this database waits on
// a lock in CancelHeldVoteTx's update (matched by its WHERE line, which the
// release's per-row updates do not share).
func waitingInHeldVoteCancel(conn *sql.DB) bool {
	var waiting bool
	err := conn.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock'
			  AND query LIKE '%WHERE activity_id = $1 AND state = ''held''%')`).Scan(&waiting)
	return err == nil && waiting
}

// voteWriteRacingNegation handles vote while the community label's negation
// commits. The test row-locks the held Like's delivery first, so the vote
// write waits on that row and the negation then waits on the vote write or
// on the same row; the lock is then let go and both must commit.
func (w *communityRemovalWorld) voteWriteRacingNegation(t *testing.T, heldActivityID string, vote *consume.JetstreamEvent) {
	t.Helper()
	ctx := context.Background()
	negation := w.signedTakedownLabel(t, outCommunityDID, true)

	rowLock, err := w.conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = rowLock.Rollback() }()
	var locked int
	require.NoError(t, rowLock.QueryRowContext(ctx, `SELECT count(*) FROM (
		SELECT 1 FROM outbound_deliveries WHERE activity_id = $1 FOR UPDATE) l`, heldActivityID).Scan(&locked))
	require.Equal(t, 1, locked, "row-lock %s's delivery", heldActivityID)

	voted := make(chan error, 1)
	go func() { voted <- w.handle(vote) }()
	waitFor(t, "the vote write to wait in its cancel of the held Like", voted, func() bool {
		return waitingInHeldVoteCancel(w.conn)
	})

	negated := make(chan error, 1)
	go func() { negated <- w.labelStore.Commit(ctx, negation.at, negation.label) }()
	waitFor(t, "the negation to wait on the vote write or the held row", negated, func() bool {
		return waitingInHeldRowsRead(w.conn) || waitingOnTakedownSubjectLock(w.conn)
	})

	require.NoError(t, rowLock.Rollback())
	for _, result := range []struct {
		name string
		done chan error
	}{{"the vote write", voted}, {"the negation", negated}} {
		select {
		case err := <-result.done:
			require.NoError(t, err, "%s commits", result.name)
		case <-time.After(10 * time.Second):
			t.Fatalf("%s did not return within 10s", result.name)
		}
	}
}

// TestCommunityRemoval_AVoteReplacedDuringTheNegationDoesNotDeadlock: a Like
// held on the community is re-cast as a Dislike while the negation releases
// the community's held work. Both commit; the replaced Like is cancelled and
// only the Dislike reaches Lemmy.
func TestCommunityRemoval_AVoteReplacedDuringTheNegationDoesNotDeadlock(t *testing.T) {
	w := newCommunityRemovalWorld(t)
	w.commitCommunityLabel(t, false)
	w.drain(t)
	sent := len(w.received())

	const rkey = "3lzcrvote0031"
	like := w.vote(t, rkey, crNewVoteSubject)
	w.drain(t)
	require.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, like), "setup: the Like is held on the community")

	w.voteWriteRacingNegation(t, like, w.voteEvent("update", rkey, crNewVoteSubject, "down"))
	w.drain(t)

	dislike := consume.ActivityID(outUserOrigin, voteRKeyATURI(rkey), "create", 1)
	assert.Equal(t, []string{"Dislike " + crNewVoteSubject.apID}, likeLines(w.receivedLinesSince(t, sent)),
		"the replacement is the only vote delivered")
	assert.Equal(t, store.DeliveryStateCancelled, w.deliveryStateOf(t, like), "the replaced Like is cancelled")
	assert.Equal(t, store.DeliveryStateDelivered, w.deliveryStateOf(t, dislike), "the Dislike is delivered")
}

// TestCommunityRemoval_AVoteDeletedDuringTheNegationDoesNotDeadlock: a Like
// held on the community is deleted while the negation releases the
// community's held work. Both commit; the Like is cancelled and never reaches
// Lemmy.
func TestCommunityRemoval_AVoteDeletedDuringTheNegationDoesNotDeadlock(t *testing.T) {
	w := newCommunityRemovalWorld(t)
	w.commitCommunityLabel(t, false)
	w.drain(t)
	sent := len(w.received())

	const rkey = "3lzcrvote0032"
	like := w.vote(t, rkey, crNewVoteSubject)
	w.drain(t)
	require.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, like), "setup: the Like is held on the community")

	w.voteWriteRacingNegation(t, like, w.voteEvent("delete", rkey, crNewVoteSubject, ""))
	w.drain(t)

	got := w.receivedLinesSince(t, sent)
	assert.Empty(t, likeLines(got), "no Like re-casts the withdrawn vote; received %v", got)
	assert.Equal(t, store.DeliveryStateCancelled, w.deliveryStateOf(t, like), "the withdrawn vote's Like is cancelled")
}
