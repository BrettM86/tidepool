package outbound

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/consume"
	"tidepool/internal/store"
)

// A vote withdrawn (deleted) or replaced (re-cast under the same rkey) while
// its Like/Dislike is held must not reach Lemmy when the hold lifts: the
// withdrawal's Undo is never held and goes out during the hold, so a held
// Like released after it would re-cast the vote the user withdrew.

// voteRKeyATURI is tdAuthorDID's vote record at-uri for rkey.
func voteRKeyATURI(rkey string) string {
	return "at://" + tdAuthorDID + "/social.coves.feed.vote/" + rkey
}

// voteCommit runs one vote commit (op create, update or delete) for rkey on
// subject in direction through the dispatcher's vote path.
func (w *communityRemovalWorld) voteCommit(t *testing.T, op, rkey string, subject crBridgedPost, direction string) {
	t.Helper()
	require.NoError(t, w.handle(w.voteEvent(op, rkey, subject, direction)), "the vote %s %s is handled", op, rkey)
}

// voteEvent is the next vote commit event (op create, update or delete) for
// rkey on subject in direction.
func (w *communityRemovalWorld) voteEvent(op, rkey string, subject crBridgedPost, direction string) *consume.JetstreamEvent {
	w.voteSeq++
	commit := &consume.CommitEvent{
		Rev: fmt.Sprintf("3mzakfuhe%04d", w.voteSeq), Operation: op,
		Collection: "social.coves.feed.vote", RKey: rkey,
	}
	if op != "delete" {
		commit.CID = outCommentCID
		commit.Record = map[string]any{
			"$type":     "social.coves.feed.vote",
			"subject":   map[string]any{"uri": subject.atURI(), "cid": tdPostCID},
			"direction": direction,
			"createdAt": "2026-11-01T10:30:00.000Z",
		}
	}
	return &consume.JetstreamEvent{
		DID: tdAuthorDID, TimeUS: 1_793_527_200_000_000 + w.voteSeq, Kind: "commit", Commit: commit,
	}
}

// likeLines are the received lines that cast a vote (Like or Dislike).
func likeLines(lines []string) []string {
	casts := []string{}
	for _, line := range lines {
		if len(line) >= 4 && (line[:4] == "Like" || (len(line) >= 7 && line[:7] == "Dislike")) {
			casts = append(casts, line)
		}
	}
	return casts
}

// TestCommunityRemoval_AVoteDeletedWhileHeldIsNotRecastAtTheNegation: a Like
// held on the community label is withdrawn during the removal. Its Undo goes
// out, and the negation delivers no Like.
func TestCommunityRemoval_AVoteDeletedWhileHeldIsNotRecastAtTheNegation(t *testing.T) {
	w := newCommunityRemovalWorld(t)
	w.commitCommunityLabel(t, false)
	w.drain(t)
	sent := len(w.received())

	like := w.vote(t, "3lzcrvote0011", crNewVoteSubject)
	w.drain(t)
	require.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, like), "setup: the Like is held on the community")

	w.voteCommit(t, "delete", "3lzcrvote0011", crNewVoteSubject, "")
	w.commitCommunityLabel(t, true)
	w.drain(t)

	got := w.receivedLinesSince(t, sent)
	assert.Empty(t, likeLines(got), "no Like re-casts the withdrawn vote; received %v", got)
	assert.Equal(t, store.DeliveryStateCancelled, w.deliveryStateOf(t, like), "the withdrawn vote's Like is cancelled")
}

// TestCommunityRemoval_AVoteReplacedWhileHeldDeliversOnlyTheReplacement: a
// Like held on the community label is re-cast as a Dislike under the same
// rkey during the removal. The negation delivers the Dislike and never the
// replaced Like.
func TestCommunityRemoval_AVoteReplacedWhileHeldDeliversOnlyTheReplacement(t *testing.T) {
	w := newCommunityRemovalWorld(t)
	w.commitCommunityLabel(t, false)
	w.drain(t)
	sent := len(w.received())

	like := w.vote(t, "3lzcrvote0012", crNewVoteSubject)
	w.drain(t)
	require.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, like), "setup: the Like is held on the community")

	w.voteCommit(t, "update", "3lzcrvote0012", crNewVoteSubject, "down")
	dislike := consume.ActivityID(outUserOrigin, voteRKeyATURI("3lzcrvote0012"), "create", 1)
	w.drain(t)
	require.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, dislike), "setup: the Dislike is held on the community")

	w.commitCommunityLabel(t, true)
	w.drain(t)

	assert.Equal(t, []string{"Dislike " + crNewVoteSubject.apID}, likeLines(w.receivedLinesSince(t, sent)),
		"the negation delivers the replacement only")
	assert.Equal(t, store.DeliveryStateCancelled, w.deliveryStateOf(t, like), "the replaced Like is cancelled")
	assert.Equal(t, store.DeliveryStateDelivered, w.deliveryStateOf(t, dislike), "the Dislike is delivered")
}

// TestCommunityRemoval_AVoteReplacedThenDeletedWhileHeldIsNotRecast: the held
// Like is re-cast as a Dislike, then deleted. The Undo names the Dislike, and
// neither the Like nor the Dislike reaches Lemmy at the negation.
func TestCommunityRemoval_AVoteReplacedThenDeletedWhileHeldIsNotRecast(t *testing.T) {
	w := newCommunityRemovalWorld(t)
	w.commitCommunityLabel(t, false)
	w.drain(t)
	sent := len(w.received())

	like := w.vote(t, "3lzcrvote0013", crNewVoteSubject)
	w.drain(t)
	require.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, like), "setup: the Like is held")
	w.voteCommit(t, "update", "3lzcrvote0013", crNewVoteSubject, "down")
	dislike := consume.ActivityID(outUserOrigin, voteRKeyATURI("3lzcrvote0013"), "create", 1)
	w.drain(t)
	require.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, dislike), "setup: the Dislike is held")

	w.voteCommit(t, "delete", "3lzcrvote0013", crNewVoteSubject, "")
	w.commitCommunityLabel(t, true)
	w.drain(t)

	got := w.receivedLinesSince(t, sent)
	assert.Empty(t, likeLines(got), "no vote re-casts the withdrawn vote; received %v", got)
	assert.Equal(t, store.DeliveryStateCancelled, w.deliveryStateOf(t, like), "the Like is cancelled")
	assert.Equal(t, store.DeliveryStateCancelled, w.deliveryStateOf(t, dislike), "the Dislike is cancelled")
}

// TestAccountTakedown_AVoteDeletedWhilePauseHeldIsNotRecastAtTheLift: a Like
// held on the persona's takedown pause is withdrawn during the pause. The
// account label's negation delivers no Like.
func TestAccountTakedown_AVoteDeletedWhilePauseHeldIsNotRecastAtTheLift(t *testing.T) {
	w := newCommunityRemovalWorld(t)
	w.commitAccountLabel(t, tdAuthorDID, false)
	w.drain(t)
	require.True(t, w.paused(t, tdAuthorDID), "setup: the account label pauses the persona")
	sent := len(w.received())

	like := w.vote(t, "3lzcrvote0014", crNewVoteSubject)
	w.drain(t)
	require.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, like), "setup: the Like is held")
	require.Equal(t, store.HeldReasonPause, w.heldReasonOf(t, like), "setup: the Like is held on the pause")

	w.voteCommit(t, "delete", "3lzcrvote0014", crNewVoteSubject, "")
	w.commitAccountLabel(t, tdAuthorDID, true)
	w.drain(t)
	require.False(t, w.paused(t, tdAuthorDID), "setup: the negation lifts the pause")

	got := w.receivedLinesSince(t, sent)
	assert.Empty(t, likeLines(got), "no Like re-casts the withdrawn vote; received %v", got)
	assert.Equal(t, store.DeliveryStateCancelled, w.deliveryStateOf(t, like), "the withdrawn vote's Like is cancelled")
}

// TestAccountTakedown_AVoteReplacedWhilePauseHeldDeliversOnlyTheReplacement:
// the pause-held Like is re-cast as a Dislike; the lift delivers the Dislike
// only.
func TestAccountTakedown_AVoteReplacedWhilePauseHeldDeliversOnlyTheReplacement(t *testing.T) {
	w := newCommunityRemovalWorld(t)
	w.commitAccountLabel(t, tdAuthorDID, false)
	w.drain(t)
	sent := len(w.received())

	like := w.vote(t, "3lzcrvote0015", crNewVoteSubject)
	w.drain(t)
	require.Equal(t, store.HeldReasonPause, w.heldReasonOf(t, like), "setup: the Like is held on the pause")
	w.voteCommit(t, "update", "3lzcrvote0015", crNewVoteSubject, "down")
	dislike := consume.ActivityID(outUserOrigin, voteRKeyATURI("3lzcrvote0015"), "create", 1)
	w.drain(t)

	w.commitAccountLabel(t, tdAuthorDID, true)
	w.drain(t)

	assert.Equal(t, []string{"Dislike " + crNewVoteSubject.apID}, likeLines(w.receivedLinesSince(t, sent)),
		"the lift delivers the replacement only")
	assert.Equal(t, store.DeliveryStateCancelled, w.deliveryStateOf(t, like), "the replaced Like is cancelled")
	assert.Equal(t, store.DeliveryStateDelivered, w.deliveryStateOf(t, dislike), "the Dislike is delivered")
}

// TestCommunityRemoval_AVoteDeletedWhileQueuedThenHeldIsNotRecast: the Like
// and its Undo are both queued when the community label commits. The label
// holds the Like and not the Undo, which goes out; the negation then finds
// the withdrawn vote's Like and cancels it instead of releasing it.
func TestCommunityRemoval_AVoteDeletedWhileQueuedThenHeldIsNotRecast(t *testing.T) {
	w := newCommunityRemovalWorld(t)
	like := w.vote(t, "3lzcrvote0016", crNewVoteSubject)
	w.voteCommit(t, "delete", "3lzcrvote0016", crNewVoteSubject, "")
	require.Equal(t, store.DeliveryStatePending, w.deliveryStateOf(t, like), "setup: the Like is still queued")

	w.commitCommunityLabel(t, false)
	require.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, like), "setup: the label holds the queued Like")
	w.drain(t)
	w.commitCommunityLabel(t, true)
	w.drain(t)

	got := w.receivedLines(t)
	assert.Empty(t, likeLines(got), "no Like re-casts the withdrawn vote; received %v", got)
	assert.Equal(t, store.DeliveryStateCancelled, w.deliveryStateOf(t, like), "the withdrawn vote's Like is cancelled")
}

// TestCommunityRemoval_AHeldVoteOnALabeledSubjectIsReleasedAtTheNegation: a
// Like held on the community label whose subject is itself labeled during the
// removal is delivered at the negation. A vote never waits on its subject, so
// the release must not re-key it onto the subject as a removed parent.
func TestCommunityRemoval_AHeldVoteOnALabeledSubjectIsReleasedAtTheNegation(t *testing.T) {
	w := newCommunityRemovalWorld(t)
	w.commitCommunityLabel(t, false)
	w.drain(t)
	sent := len(w.received())

	like := w.vote(t, "3lzcrvote0031", crNewVoteSubject)
	w.drain(t)
	require.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, like), "setup: the Like is held on the community")

	w.commitLabel(t, crNewVoteSubject.atURI(), false)
	w.commitCommunityLabel(t, true)
	w.drain(t)

	assert.Equal(t, []string{"Like " + crNewVoteSubject.apID}, likeLines(w.receivedLinesSince(t, sent)),
		"the negation delivers the held Like")
	assert.Equal(t, store.DeliveryStateDelivered, w.deliveryStateOf(t, like), "the Like is delivered")
}
