package outbound

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/labels"
	"tidepool/internal/labels/labelstest"
	"tidepool/internal/store"
)

// TestTakedown_DeliveredNativePostIsDeletedAndRestoredOverTwoEpisodes (B2,
// acceptance): a record `!takedown` on a delivered native post sends one
// Delete from the author's persona; a second source sends nothing; negating
// the label while the second source stands sends nothing; removing the last
// source sends Undo{Delete} embedding the Delete Lemmy received. A second
// episode repeats it with fresh activity ids.
func TestTakedown_DeliveredNativePostIsDeletedAndRestoredOverTwoEpisodes(t *testing.T) {
	w := newTakedownWorld(t)
	w.deliverPost(t)
	accountSource := store.TakedownSource{Kind: store.TakedownSourceAccountLabel, Key: tdAuthorDID}

	// --- Episode 1: the record label takes the post down.
	w.commitLabel(t, tdPostATURI, false)
	w.drain(t)
	got := w.receivedSince(1)
	require.Len(t, got, 1, "the takedown sends exactly one activity, got types %v", w.receivedTypes())
	firstDelete := got[0]
	assertTakedownDelete(t, firstDelete)

	// --- A second source enqueues nothing more.
	w.changeSource(t, tdPostATURI, accountSource, true)
	w.drain(t)
	assert.Equal(t, []string{"Create", "Delete"}, w.receivedTypes(), "a second source sends nothing")

	// --- Negating the label removes one of two sources: nothing is sent.
	w.commitLabel(t, tdPostATURI, true)
	w.drain(t)
	assert.Equal(t, []string{"Create", "Delete"}, w.receivedTypes(),
		"removing one of two sources sends nothing")

	// --- Removing the last source sends Undo{Delete}.
	w.changeSource(t, tdPostATURI, accountSource, false)
	w.drain(t)
	got = w.receivedSince(2)
	require.Len(t, got, 1, "removing the last source sends exactly one activity, got types %v", w.receivedTypes())
	firstUndo := got[0]
	assertTakedownUndo(t, firstUndo, firstDelete)

	// --- Episode 2: the same label again, then its negation.
	w.commitLabel(t, tdPostATURI, false)
	w.drain(t)
	got = w.receivedSince(3)
	require.Len(t, got, 1, "the second takedown sends exactly one activity, got types %v", w.receivedTypes())
	secondDelete := got[0]
	assertTakedownDelete(t, secondDelete)
	assert.NotEqual(t, firstDelete.body["id"], secondDelete.body["id"], "the second episode's Delete has a fresh id")

	w.commitLabel(t, tdPostATURI, true)
	w.drain(t)
	got = w.receivedSince(4)
	require.Len(t, got, 1, "the second restore sends exactly one activity, got types %v", w.receivedTypes())
	secondUndo := got[0]
	assertTakedownUndo(t, secondUndo, secondDelete)
	assert.NotEqual(t, firstUndo.body["id"], secondUndo.body["id"], "the second episode's Undo has a fresh id")

	assert.Equal(t, []string{"Create", "Delete", "Undo", "Delete", "Undo"}, w.receivedTypes())
}

// assertTakedownDelete checks a received takedown Delete of tdPost.
func assertTakedownDelete(t *testing.T, got receivedActivity) {
	t.Helper()
	assert.NoError(t, got.verifyErr, "the Delete's signature verifies")
	assert.Equal(t, tdAuthorActorID, got.verifiedActorID, "the Delete is signed by the author's persona")
	assert.Equal(t, "Delete", got.body["type"])
	assert.Equal(t, tdAuthorActorID, got.body["actor"])
	assert.Equal(t, tdPostAPID, got.body["object"], "the Delete names the post's object URL")
	assert.NotContains(t, got.body, "summary", "a takedown Delete carries no summary")
}

// assertTakedownUndo checks a received Undo embeds exactly the received Delete.
func assertTakedownUndo(t *testing.T, got, deleted receivedActivity) {
	t.Helper()
	assert.NoError(t, got.verifyErr, "the Undo's signature verifies")
	assert.Equal(t, tdAuthorActorID, got.verifiedActorID, "the Undo is signed by the author's persona")
	assert.Equal(t, "Undo", got.body["type"])
	assert.Equal(t, tdAuthorActorID, got.body["actor"])
	assert.Equal(t, deleted.body, got.body["object"], "the Undo embeds the Delete Lemmy received")
}

// TestTakedown_LabelForAnUnseenCommentHoldsItsCreateUntilNegated (B3): a
// `!takedown` stored before the comment has an outbound object holds the
// comment's Create; the negation delivers it, and no Delete or Undo is sent.
func TestTakedown_LabelForAnUnseenCommentHoldsItsCreateUntilNegated(t *testing.T) {
	w := newTakedownWorld(t)
	seedAcceptedParent(t, w.conn)

	w.commitLabel(t, outCommentATURI, false)
	w.enqueueCommentCreate(t)
	w.drain(t)
	assert.Empty(t, w.receivedTypes(), "a comment whose label stands is not delivered")

	w.commitLabel(t, outCommentATURI, true)
	w.drain(t)
	require.Equal(t, []string{"Create"}, w.receivedTypes(),
		"the negation delivers the held Create and nothing else")
	note := asMap(t, w.received()[0].body["object"], "Create.object")
	assert.Equal(t, outCommentAPID, note["id"], "the Create is the comment's")
}

// TestTakedown_StartActivatesALabelStoredBeforeTheApplierExisted (B4): a
// `!takedown` task 02 stored at seq N, with no takedown applier, for a
// delivered native post; a consumer with the applier starting at cursor N and
// receiving no frame enqueues one takedown Delete; a second start enqueues
// nothing more.
func TestTakedown_StartActivatesALabelStoredBeforeTheApplierExisted(t *testing.T) {
	const storedSeq = 7
	w := newTakedownWorld(t)
	w.deliverPost(t)

	// Task 02's store: no takedown applier.
	commitTakedownLabel(t, labels.NewPostgresStore(w.conn), w.labelKey, storedSeq, tdPostATURI, false)
	require.Zero(t, activityCount(t, w.conn, "Delete"), "precondition: task 02's commit enqueued nothing")

	server := labelstest.NewServer(t, func(int64) labelstest.Reply { return labelstest.Reply{Hold: true} })

	runLabelConsumer(t, w.labelStore, server, w.labelKey, storedSeq)
	require.Equal(t, 1, activityCount(t, w.conn, "Delete"), "the first start enqueues one takedown Delete")

	runLabelConsumer(t, labels.NewPostgresStore(w.conn, w.takedowns.LabelApplier()), server, w.labelKey, storedSeq)
	assert.Equal(t, 1, activityCount(t, w.conn, "Delete"), "a second start enqueues nothing more")

	w.drain(t)
	assert.Equal(t, []string{"Create", "Delete"}, w.receivedTypes(), "Lemmy receives the one Delete")
	if got := w.receivedSince(1); assert.Len(t, got, 1) {
		assertTakedownDelete(t, got[0])
	}
}
