package outbound

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/labeling"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/consume"
	"tidepool/internal/labels"
	"tidepool/internal/labels/labelstest"
	"tidepool/internal/store"
)

// TestTakedown_ATakedownDuringAConsentReleaseFollowsTheReleasedUndo: the
// author's unpause releases the consent-held restore Undo{Delete} of a post;
// after the release has read the post as not removed and before it draws the
// Undo's seq, a new `!takedown` on the post commits. The post must end down on
// Lemmy: the old episode's Undo{Delete} never follows the new Delete.
func TestTakedown_ATakedownDuringAConsentReleaseFollowsTheReleasedUndo(t *testing.T) {
	w := newTakedownWorld(t)
	ctx := context.Background()
	w.deliverPost(t)
	w.commitLabel(t, tdPostATURI, false)
	w.drain(t)
	require.NoError(t, store.NewAPActors(w.conn).SetPaused(ctx, tdAuthorDID, true))
	w.commitLabel(t, tdPostATURI, true)
	w.drain(t)
	undo := w.deliveryOf(t, "Undo")
	require.Equal(t, store.DeliveryStateHeld, undo.State, "precondition: the paused author's restore Undo is held")
	require.Equal(t, "consent", w.heldReasonOf(t, undo.ActivityID), "precondition: held on consent")

	takedown := w.signedTakedownLabel(t, tdPostATURI, false)
	w.consentReleaseRacing(t, tdAuthorDID, undo.ActivityID, func(ctx context.Context) error {
		return w.labelStore.Commit(ctx, takedown.at, takedown.label)
	})
	w.drain(t)

	assert.Equal(t, []string{
		"Create " + tdPostAPID,
		"Delete " + tdPostAPID,
		"Undo{Delete} " + tdPostAPID,
		"Delete " + tdPostAPID,
	}, w.receivedLines(t), "the released Undo goes out before the new takedown's Delete, never after it")
	assert.Equal(t, 2, activityCount(t, w.conn, "Delete"), "the new takedown sends its own Delete")
}

// TestTakedown_AParentRestoreDuringAConsentReleaseDeliversTheReply: a reply's
// edit is held on consent behind the reply's consent-held restore while its
// parent is taken down. The unpause's release reads the parent as removed;
// before it moves the edit onto the parent, the parent's restore commits. The
// edit is not left held on a parent whose release already ran: it is
// delivered.
func TestTakedown_AParentRestoreDuringAConsentReleaseDeliversTheReply(t *testing.T) {
	w := newTakedownWorld(t)
	ctx := context.Background()
	reply := tdReplyToPost
	w.deliverPost(t)
	w.enqueueReply(t, reply)
	w.drain(t)
	w.commitLabel(t, reply.atURI(), false)
	w.drain(t)
	w.commitLabel(t, tdPostATURI, false)
	w.drain(t)
	require.Equal(t, []string{
		"Create " + tdPostAPID,
		"Create " + reply.apID(),
		"Delete " + reply.apID(),
		"Delete " + tdPostAPID,
	}, w.receivedLines(t), "precondition: the reply and its parent were delivered, then taken down")

	require.NoError(t, store.NewAPActors(w.conn).SetPaused(ctx, tdAuthorDID, true))
	w.commitLabel(t, reply.atURI(), true)
	w.drain(t)
	undo := w.deliveryOf(t, "Undo")
	require.Equal(t, store.DeliveryStateHeld, undo.State, "precondition: the reply's restore Undo is held")
	require.Equal(t, "consent", w.heldReasonOf(t, undo.ActivityID), "precondition: held on consent")

	// The reply's edit, held on consent behind its restore: the state the
	// enqueue's consent hold (or the worker's, for work queued before it)
	// leaves it in, set here so the test does not depend on which one ran.
	editID := w.enqueueReplyEdit(t, reply, 1)
	_, err := w.conn.Exec(`UPDATE outbound_deliveries
		SET state = 'held', held_reason = 'consent', held_subject = $2, claimed_until = NULL
		WHERE activity_id = $1`, editID, reply.atURI())
	require.NoError(t, err)

	restore := w.signedTakedownLabel(t, tdPostATURI, true)
	w.consentReleaseRacing(t, tdAuthorDID, editID, func(ctx context.Context) error {
		return w.labelStore.Commit(ctx, restore.at, restore.label)
	})
	w.drain(t)

	assert.Equal(t, store.DeliveryStateDelivered, w.deliveryStateOf(t, editID),
		"the reply's edit is released by the parent's restore, not left held")
	assert.Contains(t, w.receivedLines(t), "Update "+reply.apID(), "Lemmy receives the reply's edit")
}

// tdSignedLabel is a signed label and the cursor to commit it at.
type tdSignedLabel struct {
	at    labels.Cursor
	label labeling.Label
}

// signedTakedownLabel signs a `!takedown` on subject (negate: its negation) at
// the world's next label seq, for a commit made off the test goroutine.
func (w *takedownWorld) signedTakedownLabel(t *testing.T, subject string, negate bool) tdSignedLabel {
	t.Helper()
	w.labelSeq++
	l := labeling.Label{SourceDID: tdLabelerDID, URI: subject, Val: tdLabelTakedown}
	if negate {
		l.Negated = &negate
	}
	return tdSignedLabel{at: labels.Cursor{Epoch: 0, Seq: w.labelSeq}, label: labelstest.Sign(t, w.labelKey, l)}
}

// consentReleaseRacing runs did's unpause and consent release in one
// transaction and runs racer between the release's read of its held rows and
// its writes. The test row-locks the held delivery of lockedActivityID, so
// the release's locking read waits on it with its snapshot already taken:
// once let go it settles on what it read before racer ran, and draws its
// seqs after. The release is let go once racer has returned or is waiting on
// a takedown-subject lock; both must then commit.
func (w *takedownWorld) consentReleaseRacing(t *testing.T, did, lockedActivityID string,
	racer func(ctx context.Context) error) {
	t.Helper()
	ctx := context.Background()
	rowLock, err := w.conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = rowLock.Rollback() }()
	var locked int
	require.NoError(t, rowLock.QueryRowContext(ctx, `SELECT count(*) FROM (
		SELECT 1 FROM outbound_deliveries WHERE activity_id = $1 FOR UPDATE) l`, lockedActivityID).Scan(&locked))
	require.Equal(t, 1, locked, "row-lock %s's delivery", lockedActivityID)

	released := make(chan error, 1)
	go func() {
		released <- func() error {
			tx, err := w.conn.BeginTx(ctx, nil)
			if err != nil {
				return err
			}
			defer func() { _ = tx.Rollback() }()
			if _, err := tx.ExecContext(ctx,
				`UPDATE ap_actors SET delivery_paused = false WHERE did = $1`, did); err != nil {
				return err
			}
			if _, err := store.NewOutboundDeliveries(w.conn).ReleaseConsentHeldTx(ctx, tx, did); err != nil {
				return err
			}
			return tx.Commit()
		}()
	}()
	waitFor(t, "the consent release to wait in its locking read of the held rows", released, func() bool {
		return waitingInHeldRowsRead(w.conn)
	})

	raced := make(chan error, 1)
	go func() { raced <- racer(ctx) }()
	racerDone := false
	var racerErr error
	deadline := time.After(5 * time.Second)
	poll := time.NewTicker(5 * time.Millisecond)
	defer poll.Stop()
wait:
	for {
		select {
		case racerErr = <-raced:
			racerDone = true
			break wait
		case <-deadline:
			_ = rowLock.Rollback()
			t.Fatal("the racer neither returned nor waited on a takedown-subject lock within 5s")
		case <-poll.C:
			if waitingOnTakedownSubjectLock(w.conn) {
				break wait
			}
		}
	}

	require.NoError(t, rowLock.Rollback())
	select {
	case err := <-released:
		require.NoError(t, err, "the consent release commits")
	case <-time.After(5 * time.Second):
		t.Fatal("the consent release did not commit within 5s")
	}
	if !racerDone {
		select {
		case racerErr = <-raced:
		case <-time.After(5 * time.Second):
			t.Fatal("the racer did not return within 5s of the release")
		}
	}
	require.NoError(t, racerErr, "the racing transaction commits")
}

// waitFor polls cond until it holds; it fails when early receives first (the
// awaited transaction returned before reaching the state) or after 5s.
func waitFor(t *testing.T, what string, early chan error, cond func() bool) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	poll := time.NewTicker(5 * time.Millisecond)
	defer poll.Stop()
	for !cond() {
		select {
		case err := <-early:
			t.Fatalf("waiting for %s: it returned first (err %v)", what, err)
		case <-deadline:
			t.Fatalf("waiting for %s: not within 5s", what)
		case <-poll.C:
		}
	}
}

// waitingInHeldRowsRead reports whether a session in this database waits on
// a lock in the held-row read (readHeldRows, matched by its opening line:
// pg_stat_activity truncates the query text).
func waitingInHeldRowsRead(conn *sql.DB) bool {
	var waiting bool
	err := conn.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock'
			  AND query LIKE '%SELECT d.activity_id, d.target_inbox, o.at_uri, a.parent_at_uri%')`).Scan(&waiting)
	return err == nil && waiting
}

// enqueueReplyEdit enqueues reply's seq-th Update{Note} through the real
// enqueuer and returns its activity id.
func (w *takedownWorld) enqueueReplyEdit(t *testing.T, reply tdReply, seq int) string {
	t.Helper()
	rev := fmt.Sprintf("%s-edit%d", reply.rev, seq)
	snapshot, err := json.Marshal(map[string]any{
		"atUri":      reply.atURI(),
		"cid":        outCommentCID,
		"rev":        rev,
		"collection": "social.coves.community.comment",
		"record": map[string]any{
			"$type": "social.coves.community.comment",
			"reply": map[string]any{
				"root":   map[string]any{"uri": reply.parentATURI, "cid": tdPostCID},
				"parent": map[string]any{"uri": reply.parentATURI, "cid": tdPostCID},
			},
			"content":   fmt.Sprintf("a native reply, edit %d", seq),
			"createdAt": "2026-11-01T11:00:00.000Z",
		},
		"parentAtUri":   reply.parentATURI,
		"parentApId":    reply.parentAPID,
		"communityApId": outCommunityAPID,
	})
	require.NoError(t, err)
	id := consume.ActivityID(outUserOrigin, reply.atURI(), "update", seq)
	w.enqueueNative(t, store.OutboundObject{
		ATURI:              reply.atURI(),
		APObjectID:         reply.apID(),
		LastCID:            outCommentCID,
		LastRev:            rev,
		CommunityDID:       outCommunityDID,
		CommunityAPID:      outCommunityAPID,
		TranslatedSnapshot: snapshot,
		Depth:              1,
		LastActivitySeq:    seq,
	}, reply.parentATURI, consume.CommentIntent{
		Op:            "update",
		ATURI:         reply.atURI(),
		ID:            id,
		CommunityAPID: outCommunityAPID,
		ParentAPID:    reply.parentAPID,
		Snapshot:      snapshot,
	})
	return id
}
