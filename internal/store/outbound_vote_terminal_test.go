package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

// TASK 17d — `undone` IS A DECISION, NOT A STAGE.
//
// Every other value in this column records where a vote GOT TO: pending is an
// intent, delivered is a peer's acceptance. `undone` records something else —
// OUR decision to stop counting a vote, written by the purge at the moment an
// actor is withdrawn, together with the Undo. It is the only value that is
// about an identity rather than about a message.
//
// So it is the only one that must not be overwritten by a later fact about the
// message. The route is real and it is the ordinary sequence, not a race:
//
//	POST confirmed → ledger write fails → delivery HELD, vote 'pending'
//	purge          → enumerates the held vote, enqueues the Undo, marks 'undone'
//	worker resumes → the held settlement lands → SetDeliveredState('delivered')
//
// and the last step un-retracts a vote for an actor this bridge has told the
// world is gone. Nothing revisits it: the purge is terminal and never re-runs,
// so 17b's reseed subtracts that vote from a served score forever, and a later
// operator reading the ledger sees a withdrawn user still holding live votes.
//
// THE GUARD BELONGS HERE, in the only writer, and not at its call sites: the
// settlement path cannot know it is racing a withdrawal, and any check it made
// would be a read outside the UPDATE's own snapshot.
//
// AND IT MUST SUCCEED. A guarded no-op is not a failure — the caller asked for
// something that is already decided, and the answer is "that is settled". If it
// came back as an error the worker would hold the delivery for settlement
// forever, retrying a write that can never apply, against a decision that will
// never change. That is why the missing-row case below is in the same file: the
// two must stay TELLABLE APART, and collapsing them is the tempting shortcut
// (`affected == 0 → nil`) that turns a real disagreement between the intent and
// the queue into a silence.

// TestOutboundVotes_SetDeliveredStateCannotResurrectARetractedVote is the
// terminality itself.
func TestOutboundVotes_SetDeliveredStateCannotResurrectARetractedVote(t *testing.T) {
	database := outboundTestDB(t)
	repo := NewOutboundVotes(database)
	ctx := context.Background()

	_, err := repo.Upsert(ctx, testOutboundVote())
	require.NoError(t, err)
	require.NoError(t, repo.SetDeliveredState(ctx, testVoteATURI, DeliveredStateUndone),
		"the purge's own write: the actor is withdrawn and this vote is retracted")

	// The late settlement, arriving exactly as the worker sends it.
	err = repo.SetDeliveredState(ctx, testVoteATURI, DeliveredStateDelivered)
	assert.NoError(t, err,
		"a settlement that lands after a retraction is not an ERROR: the delivery it belongs "+
			"to succeeded, and the write it is asking for is simply already decided. Returning "+
			"an error here holds that delivery for settlement forever, retrying a write that "+
			"can never apply against a decision that will never change")

	got, err := repo.GetByATURI(ctx, testVoteATURI)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, DeliveredStateUndone, got.DeliveredState,
		"and the row STAYS undone: `undone` is this bridge's decision to stop counting the "+
			"vote of an actor it has withdrawn, not a stage the message passes through. "+
			"Flipping it back to delivered re-counts a vote for a tombstoned identity — the "+
			"reseed subtracts it from a served score forever, and nothing re-runs a purge")
}

// TestOutboundVotes_SetDeliveredStateStillMovesALiveVote is the non-vacuity
// half, and it is not ceremony: the cheapest way to satisfy the test above is a
// guard that stops writing altogether.
func TestOutboundVotes_SetDeliveredStateStillMovesALiveVote(t *testing.T) {
	database := outboundTestDB(t)
	repo := NewOutboundVotes(database)
	ctx := context.Background()

	_, err := repo.Upsert(ctx, testOutboundVote())
	require.NoError(t, err)

	require.NoError(t, repo.SetDeliveredState(ctx, testVoteATURI, DeliveredStateDelivered))
	got, err := repo.GetByATURI(ctx, testVoteATURI)
	require.NoError(t, err)
	assert.Equal(t, DeliveredStateDelivered, got.DeliveredState,
		"an ordinary delivery success still flips pending -> delivered: the terminality is "+
			"about ONE value, and a guard that froze the column would take the vote ledger "+
			"— which 17b made an input to the number users read — permanently out of date")

	require.NoError(t, repo.SetDeliveredState(ctx, testVoteATURI, DeliveredStateUndone))
	got, err = repo.GetByATURI(ctx, testVoteATURI)
	require.NoError(t, err)
	assert.Equal(t, DeliveredStateUndone, got.DeliveredState,
		"and delivered -> undone still applies: that transition IS the purge")
}

// TestOutboundVotes_SetDeliveredStateOnAMissingVoteIsStillNotFound keeps the
// two nothings apart.
//
// A guarded no-op means "this is already decided". A missing row means the
// intent and the queue disagree about what exists — a delivery settling a vote
// nobody recorded. If the guard is written as "no rows changed, report success",
// the second becomes invisible, and the only signal that the two halves of the
// vote pipeline have diverged is gone.
func TestOutboundVotes_SetDeliveredStateOnAMissingVoteIsStillNotFound(t *testing.T) {
	database := outboundTestDB(t)
	repo := NewOutboundVotes(database)
	ctx := context.Background()

	_, err := repo.Upsert(ctx, testOutboundVote())
	require.NoError(t, err)
	require.NoError(t, repo.SetDeliveredState(ctx, testVoteATURI, DeliveredStateUndone),
		"a retracted row exists in the table, so 'no row was updated' is genuinely ambiguous "+
			"unless the two cases are distinguished by more than the row count")

	err = repo.SetDeliveredState(ctx, testOtherVoteATURI, DeliveredStateDelivered)
	require.Error(t, err,
		"settling a vote we hold no state for is a real disagreement between the intent and "+
			"the delivery, and it must not be swallowed by the same branch that answers "+
			"'already retracted'")
	assert.True(t, errors.IsNotFound(err), "want NotFound, got %v", err)
}
