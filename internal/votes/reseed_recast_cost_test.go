package votes

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// THE ACCEPTED COST OF THE RE-CAST GUARD, WRITTEN DOWN.
//
// The guard (FOLLOWUPS 17e leg 1) stops a re-cast from resetting delivered_state
// to 'pending'. It buys the thing that matters: a flipped vote stays visible to
// the erasure purge and to this seed, instead of vanishing from both the moment
// the user changes their mind. What it costs is that ONE row now carries two
// facts from different moments — `delivered`, which is true of the OLD activity
// the peer accepted, and `direction`, which is already the NEW one.
//
// The seed subtracts BY DIRECTION (SeedAggregates' `ours` term), so for as long
// as the new delivery has not landed, it gets the subtraction wrong in both
// directions at once:
//
//	the up the peer IS holding  → not subtracted (the row no longer says 'up')
//	the down the peer is NOT    → subtracted (the row says 'down' now)
//
// Neither error is silent-by-design — the second one drives the baseline
// negative, and the clamp signal is exactly the thing 17b built to make that
// observable. But the window is real, and the alternative was strictly worse:
// before the guard the row read 'pending', so it was not `ours` at all, the
// purge could not enumerate it, and nothing anywhere recorded that a vote of
// ours stood on that instance. A wrong-by-one subtraction that FIRES A COUNTER
// beats a vote nobody can see.
//
// This is characterization: it passes on the day it is written. Its job is to
// make the drift a recorded number rather than something an operator rediscovers
// from a score that will not add up.

// flipDeliveredVote drives ONE outbound row through the two-step history this
// file is about — delivered as an up-vote, then re-cast down — through the real
// store, so the GUARD is what leaves delivered_state where it is. A hand-written
// row would be whatever steady state a fixture author picked; the whole
// condition here is a row whose history has two steps that disagree.
func flipDeliveredVote(t *testing.T, agg *Aggregator, subjectAPID, subjectATURI string) *store.OutboundVote {
	t.Helper()
	ctx := context.Background()
	votes := store.NewOutboundVotes(agg.db)

	const did = "did:plc:recastcostpersona"
	const voteATURI = "at://" + did + "/social.coves.feed.vote/3lzrecastcost1"
	row := store.OutboundVote{
		VoteATURI:         voteATURI,
		ActorDID:          did,
		SubjectATURI:      subjectATURI,
		SubjectAPID:       subjectAPID,
		CommunityDID:      rsCommunityDID,
		Direction:         directionUp,
		CurrentActivityID: "https://coves.social/ap/activity/recast-cost-0",
		DeliveredState:    store.DeliveredStatePending,
	}

	_, err := votes.Upsert(ctx, row)
	require.NoError(t, err)
	require.NoError(t, votes.SetDeliveredState(ctx, voteATURI, store.DeliveredStateDelivered),
		"the peer accepted the UP-vote — this is the fact the guard exists to preserve")

	// The flip. Same record, opposite direction, a new activity id, and the
	// 'pending' the consumer states on every cast because it records intent.
	row.Direction = directionDown
	row.CurrentActivityID = "https://coves.social/ap/activity/recast-cost-1"
	row.DeliveredState = store.DeliveredStatePending
	flipped, err := votes.Upsert(ctx, row)
	require.NoError(t, err)
	return flipped
}

// TestReseedDuringARecastWindowMisreadsBothDirections pins the arithmetic of the
// window, exactly as it is.
func TestReseedDuringARecastWindowMisreadsBothDirections(t *testing.T) {
	agg, logs, objects := clampWorld(t)
	ctx := context.Background()
	subjectATURI := bridgeSubject(t, objects, subjectPost, "3lzrecastcost1")

	flipped := flipDeliveredVote(t, agg, subjectPost, subjectATURI)
	require.Equal(t, store.DeliveredStateDelivered, flipped.DeliveredState,
		"precondition: the guard KEPT the delivered state through the flip — without it this "+
			"row would read 'pending', drop out of the `ours` term entirely, and the whole "+
			"window below would be invisible instead of merely wrong")
	require.Equal(t, directionDown, flipped.Direction,
		"precondition: while the direction is ALREADY the new one — the two facts this row now "+
			"carries come from different moments")

	// The origin's totals still contain the up-vote the peer is holding, and
	// nothing of the down that has not been delivered. No inbound vote_events
	// exist: the echo of our own vote has not come back either.
	oursBefore := SeedOursSubtracted.Value()
	clampedBefore := SeedBaselineClamped.Value()
	require.NoError(t, agg.SeedAggregates(ctx, subjectPost, 1, 0))

	seededUp, seededDown := seededCounts(t, agg.db, subjectPost)
	assert.Equal(t, 1, seededUp,
		"1 origin − 0 live − 0 ours: the up the peer IS holding was NOT subtracted, because "+
			"the row no longer says 'up'. It therefore stays in the baseline, and when the "+
			"echo of that vote arrives as a live event it will be counted a second time")
	assert.Equal(t, 0, seededDown,
		"0 origin − 0 live − 1 ours = −1, floored by GREATEST(0, …): a down the peer has NOT "+
			"accepted was subtracted from a total that never contained it. The clamp is what "+
			"stops that becoming a negative served score")

	up, down, found := counts(t, agg.db, subjectPost)
	require.True(t, found)
	assert.Equal(t, 1, up)
	assert.Equal(t, 0, down)

	// The two errors happen to cancel in the SERVED number here, which is
	// precisely why the counters below are the assertion that matters: reading
	// 1/0 off vote_aggregates, this subject looks perfectly healthy.
	assert.Equal(t, oursBefore+1, SeedOursSubtracted.Value(),
		"the flipped row IS counted among `ours` — this is the guard's payoff, and the one "+
			"number that distinguishes this state from the pre-guard one, where the row read "+
			"'pending' and was subtracted from nothing")
	assert.Equal(t, clampedBefore+1, SeedBaselineClamped.Value(),
		"and the mis-subtraction is SIGNALLED rather than swallowed: the down baseline went "+
			"negative, which is the condition 17b's clamp counter exists to surface. An "+
			"operator seeing this on one subject sees a flip mid-flight; seeing it on many "+
			"sees an origin discarding votes")

	line := logs.String()
	assert.Contains(t, line, "direction=down", "the breach is down-only")
	assert.Contains(t, line, "deficit_down=-1", "by exactly the one vote that was flipped away")
	assert.Contains(t, line, "ours_down=1",
		"and the line names it as OURS — the difference between 'we mis-subtracted our own "+
			"in-flight flip' and 'the origin lost somebody else's votes'")
}
