package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TASK 17d — A TOMBSTONED ACTOR CAN NEVER BE RE-ENABLED.
//
// The destructive tier told every peer this identity was withdrawn and its
// document answers 410 forever. An enable arriving afterwards — enabled=true,
// or a DELETE of the opt-out record, which under default-on means the same
// thing — would resume signing new content as an actor peers were explicitly
// told is gone. The refusal lives in setEnabled's SQL (`enabled = ($2 AND
// tombstoned_at IS NULL)`) rather than in the callers, because forgetting it
// is silent and unrecoverable — which also makes it a one-line revert: put
// `enabled = $2` back and, until this file existed, the suite stayed green.
// (The one consume-level re-enable-after-purge test runs against a STUB purger
// that tombstones nothing, so it exercises exactly the non-terminal path.)

// TestAPActors_SetEnabledCannotResurrectATombstonedActor is the force itself.
// The one-line deletion this kills: reverting `enabled = ($2 AND tombstoned_at
// IS NULL)` to plain `enabled = $2` — a purged identity re-enables on the next
// come-back record and the bridge signs for a ghost.
func TestAPActors_SetEnabledCannotResurrectATombstonedActor(t *testing.T) {
	database := apActorsTestDB(t)
	repo := NewAPActors(database)
	ctx := t.Context()

	created, err := repo.Create(ctx, testAPActor())
	require.NoError(t, err)
	require.NotNil(t, created.EnabledAt)

	require.NoError(t, repo.Tombstone(ctx, testDID), "the destructive tier withdraws the identity")
	dead := mustGetByDID(t, repo, testDID)
	require.NotNil(t, dead.TombstonedAt, "precondition: the withdrawal is stamped")
	require.False(t, dead.Enabled, "precondition: tombstoning disables in the same statement")

	// The come-back. Per the SQL's contract the row still MATCHES — the caller
	// gets a normal one-row result, not a spurious NotFound that would make the
	// re-enable path treat a withdrawn actor as a missing one.
	err = repo.SetEnabled(ctx, testDID, true)
	require.NoError(t, err,
		"the refused enable SUCCEEDS: it is the VALUE that is forced, not the match — an "+
			"error here would fail the user's come-back event and replay it forever against "+
			"a decision that will never change")

	after := mustGetByDID(t, repo, testDID)
	assert.False(t, after.Enabled,
		"the actor STAYS disabled: peers were told this identity is gone, and an enable "+
			"that landed here would resume webfinger resolution and new signed content for it")
	require.NotNil(t, after.TombstonedAt, "the tombstone is untouched by the attempt")
	assert.True(t, after.TombstonedAt.Equal(*dead.TombstonedAt),
		"and keeps its original stamp — the only record of when the user was withdrawn")
	assert.NotNil(t, after.DisabledAt,
		"disabled_at still answers 'currently disabled, and since when': the forced branch "+
			"must not take the re-enable's CLEAR of that column")
	require.NotNil(t, after.EnabledAt)
	assert.True(t, after.EnabledAt.Equal(*created.EnabledAt),
		"enabled_at is not re-stamped by an enable that did not happen")
}

// TestAPActors_SetEnabledStillEnablesALiveDisabledActor is the positive
// control beside the guard (the full lifecycle is
// TestAPActors_LifecycleAndProfileUpdates): the cheapest way to satisfy the
// test above is `enabled = FALSE` — a SetEnabled that can only ever disable —
// and that would strand every soft-opted-out user who changes their mind.
func TestAPActors_SetEnabledStillEnablesALiveDisabledActor(t *testing.T) {
	database := apActorsTestDB(t)
	repo := NewAPActors(database)
	ctx := t.Context()

	_, err := repo.Create(ctx, testAPActor())
	require.NoError(t, err)
	require.NoError(t, repo.SetEnabled(ctx, testDID, false), "the soft opt-out")

	require.NoError(t, repo.SetEnabled(ctx, testDID, true))
	back := mustGetByDID(t, repo, testDID)
	assert.True(t, back.Enabled,
		"a NEVER-tombstoned actor re-enables exactly as before: the terminality is about "+
			"one column being set, and a guard that could only refuse would make the soft "+
			"tier — the reversible one users can safely choose — irreversible too")
	assert.Nil(t, back.TombstonedAt)
	assert.Nil(t, back.DisabledAt, "the ordinary re-enable still clears disabled_at")
}
