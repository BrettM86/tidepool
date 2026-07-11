package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/testutil"
)

// Task 11 hardening tests: tombstone pruning, follow-retry bookkeeping, and
// the transactional mapping write.

func TestTombstonesPrune(t *testing.T) {
	database := testutil.DB(t)
	testutil.Truncate(t, database, "ap_tombstones")
	tombstones := NewTombstones(database)
	ctx := context.Background()

	for _, id := range []string{"https://l.test/post/old1", "https://l.test/post/old2", "https://l.test/post/fresh"} {
		require.NoError(t, tombstones.Record(ctx, id))
	}
	_, err := database.Exec(
		`UPDATE ap_tombstones SET deleted_at = NOW() - INTERVAL '40 days' WHERE ap_id LIKE '%old%'`)
	require.NoError(t, err)

	n, err := tombstones.Prune(ctx, time.Now().Add(-30*24*time.Hour))
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)

	gone, err := tombstones.Exists(ctx, "https://l.test/post/old1")
	require.NoError(t, err)
	assert.False(t, gone, "aged marker pruned")
	kept, err := tombstones.Exists(ctx, "https://l.test/post/fresh")
	require.NoError(t, err)
	assert.True(t, kept, "fresh marker survives")

	// Nothing left to prune: zero, no error.
	n, err = tombstones.Prune(ctx, time.Now().Add(-30*24*time.Hour))
	require.NoError(t, err)
	assert.Zero(t, n)
}

func TestCommunitiesFollowRetryBookkeeping(t *testing.T) {
	database := testutil.DB(t)
	testutil.Truncate(t, database, "communities")
	communities := NewCommunities(database)
	ctx := context.Background()

	const groupIRI = "https://lemmy.test/c/retry"
	_, err := communities.UpsertCommunity(ctx, Community{
		APGroupID:         groupIRI,
		DID:               "did:plc:ewvi7nxzyoun6zhxrhs64oiz",
		PreferredUsername: "retry",
		Instance:          "lemmy.test",
	})
	require.NoError(t, err)

	// Every set-to-pending records one Follow send: stamp + increment.
	require.NoError(t, communities.SetFollowState(ctx, groupIRI, FollowStatePending))
	stored, err := communities.GetByAPGroupID(ctx, groupIRI)
	require.NoError(t, err)
	require.NotNil(t, stored.FollowRequestedAt, "pending must stamp follow_requested_at")
	assert.Equal(t, 1, stored.FollowAttempts)

	require.NoError(t, communities.SetFollowState(ctx, groupIRI, FollowStatePending))
	stored, err = communities.GetByAPGroupID(ctx, groupIRI)
	require.NoError(t, err)
	assert.Equal(t, 2, stored.FollowAttempts, "a re-send consumes an attempt")

	// Accept preserves the bookkeeping (post-mortems); none resets it.
	require.NoError(t, communities.SetFollowState(ctx, groupIRI, FollowStateAccepted))
	stored, err = communities.GetByAPGroupID(ctx, groupIRI)
	require.NoError(t, err)
	assert.Equal(t, 2, stored.FollowAttempts)
	require.NotNil(t, stored.FollowRequestedAt)

	require.NoError(t, communities.SetFollowState(ctx, groupIRI, FollowStateNone))
	stored, err = communities.GetByAPGroupID(ctx, groupIRI)
	require.NoError(t, err)
	assert.Zero(t, stored.FollowAttempts, "unsubscribe resets the budget")
	assert.Nil(t, stored.FollowRequestedAt)
}

func TestClaimStalePendingFollows(t *testing.T) {
	database := testutil.DB(t)
	testutil.Truncate(t, database, "communities")
	communities := NewCommunities(database)
	ctx := context.Background()

	seed := func(slug, did string) string {
		iri := "https://lemmy.test/c/" + slug
		_, err := communities.UpsertCommunity(ctx, Community{
			APGroupID: iri, DID: did, PreferredUsername: slug, Instance: "lemmy.test",
		})
		require.NoError(t, err)
		require.NoError(t, communities.SetFollowState(ctx, iri, FollowStatePending))
		return iri
	}
	stale := seed("stale", "did:plc:ewvi7nxzyoun6zhxrhs64oiz")
	fresh := seed("fresh", "did:plc:yk4dd2qkboz2yv6tpubpc6co")
	exhausted := seed("exhausted", "did:plc:44ybard66vv44zksje25o7dz")

	// Age the stale and exhausted rows past any threshold; burn the
	// exhausted row's budget.
	_, err := database.Exec(`
		UPDATE communities SET follow_requested_at = NOW() - INTERVAL '1 hour'
		WHERE ap_group_id IN ($1, $2)`, stale, exhausted)
	require.NoError(t, err)
	_, err = database.Exec(
		`UPDATE communities SET follow_attempts = 5 WHERE ap_group_id = $1`, exhausted)
	require.NoError(t, err)

	// The claim returns only the stale, under-budget pending row — and as a
	// side effect consumes its attempt and re-stamps follow_requested_at.
	got, err := communities.ClaimStalePendingFollows(ctx, time.Now().Add(-time.Minute), 5)
	require.NoError(t, err)
	require.Len(t, got, 1, "only the stale, under-budget pending row qualifies")
	assert.Equal(t, stale, got[0].APGroupID)
	assert.Equal(t, 2, got[0].FollowAttempts, "the claim returns the post-increment attempt count")
	_ = fresh

	// The claim is not idempotent-read: immediately re-claiming finds nothing
	// (the row it just claimed was re-stamped fresh — no double-send).
	got, err = communities.ClaimStalePendingFollows(ctx, time.Now().Add(-time.Minute), 5)
	require.NoError(t, err)
	assert.Empty(t, got, "a just-claimed row is fresh again and must not be re-claimed")

	// A NULL follow_requested_at (legacy pending row) counts as stale and is
	// claimed.
	_, err = database.Exec(
		`UPDATE communities SET follow_requested_at = NULL WHERE ap_group_id = $1`, fresh)
	require.NoError(t, err)
	got, err = communities.ClaimStalePendingFollows(ctx, time.Now().Add(-time.Minute), 5)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, fresh, got[0].APGroupID)

	// An Accept landing between sweeps (accepted state) is never claimed, so
	// the claim can never downgrade it back to pending.
	require.NoError(t, communities.SetFollowState(ctx, stale, FollowStateAccepted))
	_, err = database.Exec(`
		UPDATE communities SET follow_requested_at = NOW() - INTERVAL '1 hour'
		WHERE ap_group_id = $1`, stale)
	require.NoError(t, err)
	got, err = communities.ClaimStalePendingFollows(ctx, time.Now().Add(-time.Minute), 5)
	require.NoError(t, err)
	for _, c := range got {
		assert.NotEqual(t, stale, c.APGroupID, "an accepted row must never be claimed")
	}
	accepted, err := communities.GetByAPGroupID(ctx, stale)
	require.NoError(t, err)
	assert.Equal(t, FollowStateAccepted, accepted.FollowState, "claim must not clobber accepted back to pending")

	_, err = communities.ClaimStalePendingFollows(ctx, time.Now(), 0)
	require.Error(t, err)
	assert.True(t, errors.IsValidation(err))
}

func TestPutMappingTxAtomicity(t *testing.T) {
	database := testutil.DB(t)
	testutil.Truncate(t, database, "ap_objects")
	objects := NewAPObjects(database)
	ctx := context.Background()

	mapping := APObjectMapping{
		APID:           "https://lemmy.test/post/tx1",
		APType:         "Page",
		OriginInstance: "lemmy.test",
		DID:            "did:plc:ewvi7nxzyoun6zhxrhs64oiz",
		Collection:     "social.coves.community.post",
		RKey:           "3jzfcijpj2z2a",
		CID:            "bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqrsqxi3jgxte",
	}

	// A rolled-back transaction leaves no mapping behind.
	tx, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = objects.PutMappingTx(ctx, tx, mapping)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	_, err = objects.GetByAPID(ctx, mapping.APID)
	assert.True(t, errors.IsNotFound(err), "rollback must discard the mapping")

	// A committed one persists.
	tx, err = database.BeginTx(ctx, nil)
	require.NoError(t, err)
	stored, err := objects.PutMappingTx(ctx, tx, mapping)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	assert.NotEmpty(t, stored.ATURI)
	got, err := objects.GetByAPID(ctx, mapping.APID)
	require.NoError(t, err)
	assert.Equal(t, stored.ATURI, got.ATURI)

	// Nil tx is refused.
	_, err = objects.PutMappingTx(ctx, nil, mapping)
	require.Error(t, err)
	assert.True(t, errors.IsValidation(err))
}
