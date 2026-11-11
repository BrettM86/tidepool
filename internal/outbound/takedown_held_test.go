package outbound

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/consume"
	"tidepool/internal/store"
)

// TestTakedown_QueuedCreateIsHeldAndRestoredAtTheTailOfItsKey (B7): a native
// post whose Create is still queued is taken down. Its Create is held and no
// Delete is sent, so another author's Create behind it on the same key goes
// next; an edit made during the takedown is held too. On restore Lemmy
// receives the work enqueued during the takedown first, then the post's
// Create, then its Update, and never a Delete or Undo.
func TestTakedown_QueuedCreateIsHeldAndRestoredAtTheTailOfItsKey(t *testing.T) {
	w := newTakedownWorld(t)
	w.mintOtherAuthor(t)
	tdCreateID := consume.ActivityID(outUserOrigin, tdPostATURI, "create", 0)
	tdUpdateID := consume.ActivityID(outUserOrigin, tdPostATURI, "update", 1)

	w.enqueuePostCreate(t)
	w.enqueuePost(t, tdOtherPost, "create", 0)

	w.commitLabel(t, tdPostATURI, false)
	w.drain(t)
	assert.Equal(t, []string{"Create " + tdOtherPost.apID()}, w.receivedLines(t),
		"the held Create does not block the other author's Create behind it")
	assert.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, tdCreateID), "the queued Create is held")

	// During the takedown: the post is edited, and the author writes another post.
	w.enqueuePost(t, tdPost, "update", 1)
	w.enqueuePost(t, tdSecondPost, "create", 0)
	assert.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, tdUpdateID), "the edit is held")

	w.commitLabel(t, tdPostATURI, true)
	w.drain(t)
	assert.Equal(t, []string{
		"Create " + tdOtherPost.apID(),
		"Create " + tdSecondPost.apID(),
		"Create " + tdPostAPID,
		"Update " + tdPostAPID,
	}, w.receivedLines(t), "the restore puts the post's Create, then its Update, at the tail of the key")
	assert.Zero(t, activityCount(t, w.conn, "Delete"), "a never-delivered post gets no Delete")
	assert.Zero(t, activityCount(t, w.conn, "Undo"), "and no Undo")
}

// TestTakedown_AHeldCreateDoesNotBlockALaterTakedownDelete (B8): object A's
// held Create sits at the head of the key; taking down a delivered object B
// on the same key sends B's Delete while A stays held.
func TestTakedown_AHeldCreateDoesNotBlockALaterTakedownDelete(t *testing.T) {
	w := newTakedownWorld(t)
	w.mintOtherAuthor(t)
	w.enqueuePost(t, tdOtherPost, "create", 0)
	w.drain(t)
	require.Equal(t, []string{"Create " + tdOtherPost.apID()}, w.receivedLines(t),
		"precondition: object B is delivered")

	w.enqueuePostCreate(t)
	w.commitLabel(t, tdPostATURI, false)
	w.commitLabel(t, tdOtherPost.atURI(), false)
	w.drain(t)

	assert.Equal(t, []string{
		"Create " + tdOtherPost.apID(),
		"Delete " + tdOtherPost.apID(),
	}, w.receivedLines(t), "B's takedown Delete goes out past A's held Create, and A's Create does not")
	if got := w.receivedSince(1); assert.Len(t, got, 1) {
		assert.Equal(t, tdOtherAuthorActorID, got[0].verifiedActorID, "B's Delete is signed by B's author")
	}
	assert.Equal(t, store.DeliveryStateHeld,
		w.deliveryStateOf(t, consume.ActivityID(outUserOrigin, tdPostATURI, "create", 0)), "A stays held")
}

// TestTakedown_AnEditDuringTheTakedownFollowsTheUndo (B9): a delivered post is
// taken down and then edited; on restore Lemmy receives Undo{Delete} before
// the held Update.
func TestTakedown_AnEditDuringTheTakedownFollowsTheUndo(t *testing.T) {
	w := newTakedownWorld(t)
	w.deliverPost(t)

	w.commitLabel(t, tdPostATURI, false)
	w.drain(t)
	w.enqueuePost(t, tdPost, "update", 1)
	w.drain(t)
	assert.Equal(t, []string{"Create " + tdPostAPID, "Delete " + tdPostAPID}, w.receivedLines(t),
		"the edit made during the takedown is not delivered")

	w.commitLabel(t, tdPostATURI, true)
	w.drain(t)
	require.Equal(t, []string{
		"Create " + tdPostAPID,
		"Delete " + tdPostAPID,
		"Undo{Delete} " + tdPostAPID,
		"Update " + tdPostAPID,
	}, w.receivedLines(t), "the restore sends Undo{Delete} before the held Update")
	got := w.received()
	assertTakedownUndo(t, got[2], got[1])
}

// TestTakedown_ARestoreHeldOnConsentDoesNotBlockTheCommunity (B10): the
// takedown Delete passes a pause; the restore Undo{Delete} of a paused
// author is held on consent, off the community's queue, together with the
// post's edit queued behind it. Another author's takedown Delete on the same
// community goes out meanwhile. The unpause releases the Undo, then the edit.
func TestTakedown_ARestoreHeldOnConsentDoesNotBlockTheCommunity(t *testing.T) {
	w := newTakedownWorld(t)
	ctx := context.Background()
	w.mintOtherAuthor(t)
	w.deliverPost(t)
	w.enqueuePost(t, tdOtherPost, "create", 0)
	w.commitLabel(t, tdPostATURI, false)
	w.drain(t)
	require.Equal(t, []string{
		"Create " + tdPostAPID,
		"Create " + tdOtherPost.apID(),
		"Delete " + tdPostAPID,
	}, w.receivedLines(t), "precondition: both posts delivered, A's taken down")

	actors := store.NewAPActors(w.conn)
	require.NoError(t, actors.SetPaused(ctx, tdAuthorDID, true))
	w.commitLabel(t, tdPostATURI, true)
	w.enqueuePost(t, tdPost, "update", 1)
	w.drain(t)
	undo := w.deliveryOf(t, "Undo")
	updateID := consume.ActivityID(outUserOrigin, tdPostATURI, "update", 1)
	assert.Equal(t, store.DeliveryStateHeld, undo.State, "the paused author's restore Undo is held")
	assert.Equal(t, "consent", w.heldReasonOf(t, undo.ActivityID), "held on consent")
	assert.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, updateID), "the edit behind it is held too")
	assert.Equal(t, "consent", w.heldReasonOf(t, updateID), "on consent")

	w.commitLabel(t, tdOtherPost.atURI(), false)
	w.drain(t)
	assert.Equal(t, []string{
		"Create " + tdPostAPID,
		"Create " + tdOtherPost.apID(),
		"Delete " + tdPostAPID,
		"Delete " + tdOtherPost.apID(),
	}, w.receivedLines(t), "B's takedown Delete goes out while A's restore is held")

	w.restoreConsent(t, tdAuthorDID, func(tx *sql.Tx) {
		_, err := tx.ExecContext(ctx, `UPDATE ap_actors SET delivery_paused = false WHERE did = $1`, tdAuthorDID)
		require.NoError(t, err)
	})
	w.drain(t)
	require.Equal(t, []string{
		"Create " + tdPostAPID,
		"Create " + tdOtherPost.apID(),
		"Delete " + tdPostAPID,
		"Delete " + tdOtherPost.apID(),
		"Undo{Delete} " + tdPostAPID,
		"Update " + tdPostAPID,
	}, w.receivedLines(t), "the unpause releases the Undo, then the edit")
	got := w.received()
	assertTakedownUndo(t, got[4], got[2])
}

// TestTakedown_ARestoreHeldOnAnOptOutIsReleasedByTheOptIn (B10b): the restore
// Undo{Delete} of an opted-out author is held on consent and delivered once
// the author opts back in.
func TestTakedown_ARestoreHeldOnAnOptOutIsReleasedByTheOptIn(t *testing.T) {
	w := newTakedownWorld(t)
	ctx := context.Background()
	w.deliverPost(t)
	w.commitLabel(t, tdPostATURI, false)
	w.drain(t)
	actors := store.NewAPActors(w.conn)
	prefs := store.NewFederationPrefs(w.conn)
	_, err := prefs.Upsert(ctx, store.FederationPref{
		DID: tdAuthorDID, Enabled: false, Source: store.FederationPrefSourceRecord})
	require.NoError(t, err, "opt the author out")
	require.NoError(t, actors.SetEnabled(ctx, tdAuthorDID, false))

	w.commitLabel(t, tdPostATURI, true)
	w.drain(t)
	require.Equal(t, []string{"Create " + tdPostAPID, "Delete " + tdPostAPID}, w.receivedLines(t),
		"the restore is not delivered while the author is opted out")
	undo := w.deliveryOf(t, "Undo")
	assert.Equal(t, store.DeliveryStateHeld, undo.State, "the opted-out author's restore Undo is held")
	assert.Equal(t, "consent", w.heldReasonOf(t, undo.ActivityID), "held on consent")

	w.restoreConsent(t, tdAuthorDID, func(tx *sql.Tx) {
		_, err := prefs.DeleteTx(ctx, tx, tdAuthorDID)
		require.NoError(t, err)
		require.NoError(t, actors.SetEnabledTx(ctx, tx, tdAuthorDID, true))
	})
	w.drain(t)
	require.Equal(t, []string{
		"Create " + tdPostAPID,
		"Delete " + tdPostAPID,
		"Undo{Delete} " + tdPostAPID,
	}, w.receivedLines(t), "the opt-in releases the Undo")
	got := w.received()
	assertTakedownUndo(t, got[2], got[1])
}

// TestTakedown_NoUndoForADeletedObjectOrATombstonedAuthor (B11): when the
// takedown lifts on an object its author deleted, or by an author the purger
// tombstoned, no Undo{Delete} is sent or enqueued, and the purger is never
// invoked (no Delete{Person}).
func TestTakedown_NoUndoForADeletedObjectOrATombstonedAuthor(t *testing.T) {
	for _, tc := range []struct {
		name      string
		tombstone func(t *testing.T, w *takedownWorld)
	}{
		{"the author deleted the object", func(t *testing.T, w *takedownWorld) {
			_, err := store.NewOutboundObjects(w.conn).Tombstone(context.Background(), tdPostATURI)
			require.NoError(t, err)
		}},
		{"the purger tombstoned the author", func(t *testing.T, w *takedownWorld) {
			require.NoError(t, store.NewAPActors(w.conn).Tombstone(context.Background(), tdAuthorDID))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newTakedownWorld(t)
			w.deliverPost(t)
			w.commitLabel(t, tdPostATURI, false)
			w.drain(t)
			require.Equal(t, []string{"Create " + tdPostAPID, "Delete " + tdPostAPID}, w.receivedLines(t),
				"precondition: the takedown Delete was delivered")

			tc.tombstone(t, w)
			w.commitLabel(t, tdPostATURI, true)
			w.drain(t)

			assert.Equal(t, []string{"Create " + tdPostAPID, "Delete " + tdPostAPID}, w.receivedLines(t),
				"the lift sends nothing")
			assert.Zero(t, activityCount(t, w.conn, "Undo"), "no Undo{Delete} is enqueued")
			assert.Equal(t, 1, activityCount(t, w.conn, "Delete"),
				"the only Delete is the takedown's: no Delete{Person}")
		})
	}
}

// TestTakedown_AnInFlightCreateThatFailsIsHeldOnItsRetry: a native post's
// Create is claimed and its POST in flight when the record `!takedown`
// commits, and Lemmy answers that POST 503, so the row goes back to pending
// for a retry. While the post is taken down the retry sends nothing; once the
// takedown lifts, Lemmy receives the Create exactly once and never a Delete or
// Undo, because Lemmy never accepted the Create.
func TestTakedown_AnInFlightCreateThatFailsIsHeldOnItsRetry(t *testing.T) {
	w := newTakedownWorld(t)
	createID := consume.ActivityID(outUserOrigin, tdPostATURI, "create", 0)

	w.enqueuePostCreate(t)
	w.answerUnavailable(1)
	w.commitLabelWhilePOSTInFlight(t, tdPostATURI)
	require.Equal(t, 1, w.rejectedPosts(), "precondition: Lemmy answered the in-flight Create 503")
	require.Empty(t, w.receivedLines(t), "precondition: Lemmy accepted nothing")
	require.Equal(t, store.DeliveryStatePending, w.deliveryStateOf(t, createID),
		"precondition: the failed Create is pending for a retry")

	w.makeDue(t, createID)
	worked, err := w.worker.DeliverNext(context.Background())
	require.NoError(t, err)
	require.True(t, worked, "the retry was claimed")
	assert.Equal(t, 1, w.rejectedPosts(), "the retry does not POST the Create while the post is down")
	assert.Empty(t, w.receivedLines(t), "Lemmy receives nothing while the post is down")
	assert.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, createID), "the retried Create is held")

	w.commitLabel(t, tdPostATURI, true)
	w.drain(t)
	assert.Equal(t, []string{"Create " + tdPostAPID}, w.receivedLines(t),
		"after the restore Lemmy receives the Create exactly once")
	assert.Equal(t, 1, w.rejectedPosts(), "no further failed POSTs")
	assert.Equal(t, store.DeliveryStateDelivered, w.deliveryStateOf(t, createID), "the Create is delivered")
	assert.True(t, rawAccepted(t, w.conn, tdPostATURI), "the delivered Create stamps accepted_at")
	assert.Zero(t, activityCount(t, w.conn, "Delete"), "a never-accepted post gets no Delete")
	assert.Zero(t, activityCount(t, w.conn, "Undo"), "and no Undo")
}
