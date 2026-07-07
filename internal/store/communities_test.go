package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

func TestCommunities_UpsertIsIdempotent(t *testing.T) {
	database := testDB(t)
	repo := NewCommunities(database)
	ctx := context.Background()

	first, err := repo.UpsertCommunity(ctx, testCommunity())
	require.NoError(t, err)
	assert.Equal(t, FollowStateNone, first.FollowState, "follow state defaults to none")

	renamed := testCommunity()
	renamed.PreferredUsername = "tech"
	second, err := repo.UpsertCommunity(ctx, renamed)
	require.NoError(t, err)

	assert.Equal(t, first.ID, second.ID, "upsert must reuse the existing row")
	assert.Equal(t, "tech", second.PreferredUsername)
	assert.Equal(t, first.DID, second.DID)

	var total int
	require.NoError(t, database.QueryRow(`SELECT COUNT(*) FROM communities`).Scan(&total))
	assert.Equal(t, 1, total)
}

func TestCommunities_UpsertRejectsIdentityDrift(t *testing.T) {
	repo := NewCommunities(testDB(t))
	ctx := context.Background()

	original, err := repo.UpsertCommunity(ctx, testCommunity())
	require.NoError(t, err)

	// Same AP group id arriving with a different DID must be a conflict,
	// not a silent success that keeps the old DID.
	drifted := testCommunity()
	drifted.DID = testSecondDID
	_, err = repo.UpsertCommunity(ctx, drifted)
	assert.True(t, errors.IsAlreadyExists(err), "changed DID must conflict, got %v", err)

	// Same for a changed instance.
	moved := testCommunity()
	moved.Instance = "lemmy.ml"
	_, err = repo.UpsertCommunity(ctx, moved)
	assert.True(t, errors.IsAlreadyExists(err), "changed instance must conflict, got %v", err)

	// The stored row is untouched by the rejected upserts.
	stored, err := repo.GetByAPGroupID(ctx, testAPGroupID)
	require.NoError(t, err)
	assert.Equal(t, original.DID, stored.DID)
	assert.Equal(t, original.Instance, stored.Instance)
}

func TestCommunities_UpsertPreservesFollowState(t *testing.T) {
	repo := NewCommunities(testDB(t))
	ctx := context.Background()

	_, err := repo.UpsertCommunity(ctx, testCommunity())
	require.NoError(t, err)
	require.NoError(t, repo.SetFollowState(ctx, testAPGroupID, FollowStateAccepted))

	refreshed, err := repo.UpsertCommunity(ctx, testCommunity())
	require.NoError(t, err)
	assert.Equal(t, FollowStateAccepted, refreshed.FollowState)
	assert.NotNil(t, refreshed.FollowedAt)
}

func TestCommunities_Get(t *testing.T) {
	repo := NewCommunities(testDB(t))
	ctx := context.Background()

	stored, err := repo.UpsertCommunity(ctx, testCommunity())
	require.NoError(t, err)

	byGroupID, err := repo.GetByAPGroupID(ctx, testAPGroupID)
	require.NoError(t, err)
	assert.Equal(t, stored.ID, byGroupID.ID)

	byDID, err := repo.GetByDID(ctx, testDID)
	require.NoError(t, err)
	assert.Equal(t, stored.ID, byDID.ID)

	_, err = repo.GetByAPGroupID(ctx, "https://lemmy.world/c/missing")
	assert.True(t, errors.IsNotFound(err), "expected IsNotFound, got %v", err)
	_, err = repo.GetByDID(ctx, testSecondDID)
	assert.True(t, errors.IsNotFound(err), "expected IsNotFound, got %v", err)
}

func TestCommunities_FollowStateLifecycle(t *testing.T) {
	repo := NewCommunities(testDB(t))
	ctx := context.Background()

	_, err := repo.UpsertCommunity(ctx, testCommunity())
	require.NoError(t, err)

	// none -> pending: Follow sent, awaiting Accept. No followed_at yet.
	require.NoError(t, repo.SetFollowState(ctx, testAPGroupID, FollowStatePending))
	community, err := repo.GetByAPGroupID(ctx, testAPGroupID)
	require.NoError(t, err)
	assert.Equal(t, FollowStatePending, community.FollowState)
	assert.Nil(t, community.FollowedAt)

	// pending -> accepted stamps followed_at.
	require.NoError(t, repo.SetFollowState(ctx, testAPGroupID, FollowStateAccepted))
	community, err = repo.GetByAPGroupID(ctx, testAPGroupID)
	require.NoError(t, err)
	assert.Equal(t, FollowStateAccepted, community.FollowState)
	assert.NotNil(t, community.FollowedAt)

	// AP redelivers Accept: re-setting accepted must NOT re-stamp the
	// original followed_at.
	firstFollowedAt := community.FollowedAt
	require.NoError(t, repo.SetFollowState(ctx, testAPGroupID, FollowStateAccepted))
	community, err = repo.GetByAPGroupID(ctx, testAPGroupID)
	require.NoError(t, err)
	require.NotNil(t, community.FollowedAt)
	assert.True(t, community.FollowedAt.Equal(*firstFollowedAt),
		"double-accept must preserve the original followed_at")

	// accepted -> none (unfollow) clears followed_at.
	require.NoError(t, repo.SetFollowState(ctx, testAPGroupID, FollowStateNone))
	community, err = repo.GetByAPGroupID(ctx, testAPGroupID)
	require.NoError(t, err)
	assert.Equal(t, FollowStateNone, community.FollowState)
	assert.Nil(t, community.FollowedAt)

	// Unknown states and unknown groups are rejected.
	err = repo.SetFollowState(ctx, testAPGroupID, FollowState("blocked"))
	assert.True(t, errors.IsValidation(err), "expected validation error, got %v", err)
	err = repo.SetFollowState(ctx, "https://lemmy.world/c/missing", FollowStatePending)
	assert.True(t, errors.IsNotFound(err), "expected IsNotFound, got %v", err)
}

func TestCommunities_SetLastBackfill(t *testing.T) {
	repo := NewCommunities(testDB(t))
	ctx := context.Background()

	_, err := repo.UpsertCommunity(ctx, testCommunity())
	require.NoError(t, err)

	backfilledAt := time.Date(2026, 6, 15, 3, 0, 0, 0, time.UTC)
	require.NoError(t, repo.SetLastBackfill(ctx, testAPGroupID, backfilledAt))

	community, err := repo.GetByAPGroupID(ctx, testAPGroupID)
	require.NoError(t, err)
	require.NotNil(t, community.LastBackfillAt)
	assert.True(t, community.LastBackfillAt.Equal(backfilledAt))

	err = repo.SetLastBackfill(ctx, "https://lemmy.world/c/missing", backfilledAt)
	assert.True(t, errors.IsNotFound(err), "expected IsNotFound, got %v", err)
}

func TestCommunities_ListByFollowState(t *testing.T) {
	repo := NewCommunities(testDB(t))
	ctx := context.Background()

	first := testCommunity()
	_, err := repo.UpsertCommunity(ctx, first)
	require.NoError(t, err)

	second := Community{
		APGroupID:         "https://lemmy.world/c/golang",
		DID:               testSecondDID,
		PreferredUsername: "golang",
		Instance:          testInstance,
	}
	_, err = repo.UpsertCommunity(ctx, second)
	require.NoError(t, err)
	require.NoError(t, repo.SetFollowState(ctx, second.APGroupID, FollowStateAccepted))

	pending, err := repo.ListByFollowState(ctx, FollowStatePending)
	require.NoError(t, err)
	assert.Empty(t, pending)

	none, err := repo.ListByFollowState(ctx, FollowStateNone)
	require.NoError(t, err)
	require.Len(t, none, 1)
	assert.Equal(t, first.APGroupID, none[0].APGroupID)

	accepted, err := repo.ListByFollowState(ctx, FollowStateAccepted)
	require.NoError(t, err)
	require.Len(t, accepted, 1)
	assert.Equal(t, second.APGroupID, accepted[0].APGroupID)

	_, err = repo.ListByFollowState(ctx, FollowState("blocked"))
	assert.True(t, errors.IsValidation(err), "expected validation error, got %v", err)
}

// TestCommunities_UpsertValidation is pure input validation: it never
// touches postgres, so it runs without TIDEPOOL_TEST_DATABASE_URL.
func TestCommunities_UpsertValidation(t *testing.T) {
	repo := NewCommunities(nil)
	ctx := context.Background()

	cases := []struct {
		name   string
		mutate func(*Community)
	}{
		{"empty ap_group_id", func(c *Community) { c.APGroupID = "" }},
		{"invalid did", func(c *Community) { c.DID = "not-a-did" }},
		{"empty preferred_username", func(c *Community) { c.PreferredUsername = "" }},
		{"empty instance", func(c *Community) { c.Instance = "" }},
		{"invalid follow_state", func(c *Community) { c.FollowState = "blocked" }},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			community := testCommunity()
			testCase.mutate(&community)
			_, err := repo.UpsertCommunity(ctx, community)
			assert.True(t, errors.IsValidation(err), "expected validation error, got %v", err)
		})
	}
}

func TestCommunities_DIDCollisionIsConflict(t *testing.T) {
	repo := NewCommunities(testDB(t))
	ctx := context.Background()

	_, err := repo.UpsertCommunity(ctx, testCommunity())
	require.NoError(t, err)

	collision := testCommunity()
	collision.APGroupID = "https://lemmy.world/c/imposter"
	_, err = repo.UpsertCommunity(ctx, collision)
	assert.True(t, errors.IsAlreadyExists(err), "expected IsAlreadyExists, got %v", err)
}
