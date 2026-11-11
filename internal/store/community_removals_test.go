package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/testutil"
)

// TestCommunityRemovals_OwedUnfollowSurvivesTheRemovalsCommit: a removal
// marks its Undo{Follow} owed in its own transaction; with no send after the
// commit (a crash) the Undo{Follow} is still listed owed, until cleared.
func TestCommunityRemovals_OwedUnfollowSurvivesTheRemovalsCommit(t *testing.T) {
	database := testutil.DB(t)
	testutil.Truncate(t, database, "communities", "community_removal_suppressions")
	communities := NewCommunities(database)
	removals := communities.(CommunityRemovals)
	ctx := context.Background()
	const (
		groupIRI = "https://lemmy.test/c/owed"
		did      = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	)
	_, err := communities.UpsertCommunity(ctx, Community{
		APGroupID: groupIRI, DID: did, PreferredUsername: "owed", Instance: "lemmy.test",
	})
	require.NoError(t, err)
	require.NoError(t, communities.SetFollowState(ctx, groupIRI, FollowStateAccepted))

	tx, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = AddCommunityRemovalTx(ctx, tx, CommunityRemoval{
		CommunityDID: did, LabelerDID: "did:web:coves.test", FollowStateAtRemoval: FollowStateAccepted,
	})
	require.NoError(t, err)
	require.NoError(t, SetFollowStateTx(ctx, tx, groupIRI, FollowStateNone))
	removalID, err := MarkUnfollowOwedTx(ctx, tx, did)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	owed, err := removals.ListOwedUnfollows(ctx)
	require.NoError(t, err)
	assert.Equal(t, []OwedUnfollow{{RemovalID: removalID, APGroupID: groupIRI, FollowState: FollowStateNone}}, owed)

	require.NoError(t, removals.ClearOwedUnfollow(ctx, removalID))
	owed, err = removals.ListOwedUnfollows(ctx)
	require.NoError(t, err)
	assert.Empty(t, owed)
}

// TestCommunityRemovals_AcceptWaitsForTheRestoredFollow: a negation's owed
// Follow (RestoreFollowTx, not sent) counts no attempt and refuses an Accept;
// once a Follow is sent the Accept follows the community.
func TestCommunityRemovals_AcceptWaitsForTheRestoredFollow(t *testing.T) {
	database := testutil.DB(t)
	testutil.Truncate(t, database, "communities", "community_removal_suppressions")
	communities := NewCommunities(database)
	removals := communities.(CommunityRemovals)
	ctx := context.Background()
	const groupIRI = "https://lemmy.test/c/restored"
	_, err := communities.UpsertCommunity(ctx, Community{
		APGroupID: groupIRI, DID: "did:plc:ewvi7nxzyoun6zhxrhs64oiz", PreferredUsername: "restored", Instance: "lemmy.test",
	})
	require.NoError(t, err)

	tx, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, RestoreFollowTx(ctx, tx, groupIRI, false))
	require.NoError(t, tx.Commit())

	stored, err := communities.GetByAPGroupID(ctx, groupIRI)
	require.NoError(t, err)
	assert.Equal(t, FollowStatePending, stored.FollowState)
	assert.Nil(t, stored.FollowRequestedAt)
	assert.Zero(t, stored.FollowAttempts, "an owed Follow is not an attempt")

	accepted, err := removals.AcceptPendingFollow(ctx, groupIRI)
	require.NoError(t, err)
	assert.False(t, accepted, "an Accept before the owed Follow is sent is refused")

	require.NoError(t, communities.SetFollowState(ctx, groupIRI, FollowStatePending))
	accepted, err = removals.AcceptPendingFollow(ctx, groupIRI)
	require.NoError(t, err)
	assert.True(t, accepted, "the Accept of the sent Follow follows the community")
}

// TestCommunityRemovals_CompleteOwedUnfollow: a delivered Undo{Follow} of a
// community still unfollowed is cleared. One of a community followed again
// while it was in flight is cleared and re-pends the community with its
// Follow owed, so a stale Accept is refused.
func TestCommunityRemovals_CompleteOwedUnfollow(t *testing.T) {
	for _, test := range []struct {
		name         string
		stateAfter   FollowState
		refollowOwed bool
	}{
		{name: "still unfollowed", stateAfter: FollowStateNone, refollowOwed: false},
		{name: "followed again", stateAfter: FollowStateAccepted, refollowOwed: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			database := testutil.DB(t)
			testutil.Truncate(t, database, "communities", "community_removal_suppressions")
			// The active removal left behind would suppress later tests'
			// communities on the same DID.
			t.Cleanup(func() { testutil.Truncate(t, database, "community_removal_suppressions") })
			communities := NewCommunities(database)
			removals := communities.(CommunityRemovals)
			ctx := context.Background()
			const (
				groupIRI = "https://lemmy.test/c/completed"
				did      = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
			)
			_, err := communities.UpsertCommunity(ctx, Community{
				APGroupID: groupIRI, DID: did, PreferredUsername: "completed", Instance: "lemmy.test",
			})
			require.NoError(t, err)
			require.NoError(t, communities.SetFollowState(ctx, groupIRI, FollowStateAccepted))
			tx, err := database.BeginTx(ctx, nil)
			require.NoError(t, err)
			_, err = AddCommunityRemovalTx(ctx, tx, CommunityRemoval{
				CommunityDID: did, LabelerDID: "did:web:coves.test", FollowStateAtRemoval: FollowStateAccepted,
			})
			require.NoError(t, err)
			require.NoError(t, SetFollowStateTx(ctx, tx, groupIRI, FollowStateNone))
			removalID, err := MarkUnfollowOwedTx(ctx, tx, did)
			require.NoError(t, err)
			require.NoError(t, tx.Commit())
			if test.stateAfter != FollowStateNone {
				require.NoError(t, communities.SetFollowState(ctx, groupIRI, FollowStatePending))
				require.NoError(t, communities.SetFollowState(ctx, groupIRI, test.stateAfter))
			}
			before, err := communities.GetByAPGroupID(ctx, groupIRI)
			require.NoError(t, err)

			refollowOwed, err := removals.CompleteOwedUnfollow(ctx, removalID)

			require.NoError(t, err)
			assert.Equal(t, test.refollowOwed, refollowOwed)
			owed, err := removals.ListOwedUnfollows(ctx)
			require.NoError(t, err)
			assert.Empty(t, owed)
			stored, err := communities.GetByAPGroupID(ctx, groupIRI)
			require.NoError(t, err)
			if !test.refollowOwed {
				assert.Equal(t, before.FollowAttempts, stored.FollowAttempts)
				assert.Equal(t, FollowStateNone, stored.FollowState)
				return
			}
			assert.Equal(t, FollowStatePending, stored.FollowState)
			assert.Nil(t, stored.FollowRequestedAt, "the replacement Follow is owed, not sent")
			assert.Zero(t, stored.FollowAttempts, "the replacement Follow starts a fresh attempt budget")
			accepted, err := removals.AcceptPendingFollow(ctx, groupIRI)
			require.NoError(t, err)
			assert.False(t, accepted, "an Accept of the undone Follow is refused")
			// Once the removal is negated, the replacement is claimed at a
			// budget of one attempt, which the undone Follow used.
			tx, err = database.BeginTx(ctx, nil)
			require.NoError(t, err)
			_, err = ClearCommunityRemovalTx(ctx, tx, did)
			require.NoError(t, err)
			require.NoError(t, tx.Commit())
			require.Equal(t, 1, before.FollowAttempts)
			claimed, err := communities.ClaimStalePendingFollows(ctx, time.Now(), before.FollowAttempts)
			require.NoError(t, err)
			require.Len(t, claimed, 1, "the replacement Follow is claimed at a budget the undone Follow used up")
			assert.Equal(t, groupIRI, claimed[0].APGroupID)
		})
	}
}
