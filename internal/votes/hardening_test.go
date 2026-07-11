package votes

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

// Task 11 hardening tests: the seeded-count sanity cap, the actor-delete
// vote scrub, and the undone-event pruner.

// TestSeedAggregatesSanityCap: a hostile origin API must not be able to
// inject absurd baselines — values over MaxSeededCount are a validation
// error and the previous baseline survives.
func TestSeedAggregatesSanityCap(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	require.NoError(t, agg.SeedAggregates(ctx, subjectPost, 7, 2))

	err := agg.SeedAggregates(ctx, subjectPost, MaxSeededCount+1, 0)
	require.Error(t, err)
	assert.True(t, errors.IsValidation(err), "an absurd upvote baseline is a validation error")
	err = agg.SeedAggregates(ctx, subjectPost, 0, MaxSeededCount+1)
	require.Error(t, err)
	assert.True(t, errors.IsValidation(err), "an absurd downvote baseline is a validation error")

	// The previous good baseline survives the refused re-seed.
	up, down, found := counts(t, database, subjectPost)
	require.True(t, found)
	assert.Equal(t, 7, up)
	assert.Equal(t, 2, down)

	// Exactly at the cap is allowed (the cap is a bound, not off-by-one).
	require.NoError(t, agg.SeedAggregates(ctx, subjectPost, MaxSeededCount, 0))
}

// TestScrubVoterErasesRowsAndRecounts: the actor-delete scrub removes every
// vote_events row a voter produced — live AND undone — and recomputes every
// affected aggregate, leaving other voters and seeded baselines untouched.
func TestScrubVoterErasesRowsAndRecounts(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	bridgeSubject(t, objects, subjectComment, "3jzfcijpj3aaa")
	ctx := context.Background()

	// A seeded baseline plus live votes from two voters on two subjects;
	// alice also has an undone (superseded) row from a flip.
	require.NoError(t, agg.SeedAggregates(ctx, subjectPost, 10, 1))
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	require.NoError(t, agg.ApplyVote(ctx, dislike(activityID(t, 2), voterAlice, subjectPost), "")) // flip: supersedes 1
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 3), voterBob, subjectPost), ""))
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 4), voterAlice, subjectComment), ""))

	require.NoError(t, agg.ScrubVoter(ctx, voterAlice))

	// Every alice row is GONE (not just undone — the scrub is a privacy
	// erase, not a retraction).
	var aliceRows int
	require.NoError(t, database.QueryRow(
		`SELECT COUNT(*) FROM vote_events WHERE voter_ap_id = $1`, voterAlice).Scan(&aliceRows))
	assert.Zero(t, aliceRows, "scrub must delete all of the voter's rows")

	// Post: baseline 10/1 + bob's live like; alice's live dislike removed.
	up, down, _ := counts(t, database, subjectPost)
	assert.Equal(t, 11, up)
	assert.Equal(t, 1, down)
	// Comment: alice was the only voter → back to zero.
	up, down, _ = counts(t, database, subjectComment)
	assert.Zero(t, up)
	assert.Zero(t, down)

	// Idempotent: scrubbing again (and scrubbing a never-seen voter) is a
	// no-op success.
	require.NoError(t, agg.ScrubVoter(ctx, voterAlice))
	require.NoError(t, agg.ScrubVoter(ctx, "https://lemmy.world/u/never-voted"))
}

// TestScrubVoter_RecomputesEveryDeletedSubject pins the DELETE ... RETURNING
// recompute set (task 11 lost-update fix): every subject the voter had a row
// on is recomputed, even when the served aggregate has drifted to a phantom
// value. A single-transaction unit test cannot reproduce the true concurrent
// race (a vote landing on a not-previously-voted subject between the snapshot
// lock and the DELETE), so it pins the load-bearing property directly — a
// deleted subject is ALWAYS in the recompute set, never left phantom.
func TestScrubVoter_RecomputesEveryDeletedSubject(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	// Alice's live upvote creates the post aggregate at up=1.
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))

	// Force the served upvotes to a phantom value that no longer matches the
	// live rows — exactly the drift a lost update would leave behind.
	_, err := database.Exec(
		`UPDATE vote_aggregates SET upvotes = 99 WHERE subject_ap_id = $1`, subjectPost)
	require.NoError(t, err)

	// Scrubbing alice deletes her row on the post; the recompute set is
	// driven by DELETE ... RETURNING, so the subject is recomputed back to the
	// truth (no live votes, no seeded baseline) rather than keeping the
	// phantom.
	require.NoError(t, agg.ScrubVoter(ctx, voterAlice))
	up, down, _ := counts(t, database, subjectPost)
	assert.Zero(t, up, "the deleted subject is recomputed away from its phantom count")
	assert.Zero(t, down)
}

// TestPruneUndoneEventsKeepsLiveRows: only undone rows older than the
// cutoff are reclaimed; live votes (the counts!) and fresh undone rows
// survive, and served aggregates do not change.
func TestPruneUndoneEventsKeepsLiveRows(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	require.NoError(t, agg.ApplyVote(ctx, dislike(activityID(t, 2), voterAlice, subjectPost), "")) // supersedes 1 → undone
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 3), voterBob, subjectPost), ""))
	require.NoError(t, agg.RetractVote(ctx, like(activityID(t, 3), voterBob, subjectPost), "")) // undone

	// Age alice's undone row past the cutoff; bob's undone row stays fresh.
	_, err := database.Exec(
		`UPDATE vote_events SET created_at = NOW() - INTERVAL '100 days' WHERE activity_id = $1`,
		activityID(t, 1))
	require.NoError(t, err)

	n, err := agg.PruneUndoneEvents(ctx, time.Now().Add(-90*24*time.Hour))
	require.NoError(t, err)
	assert.Equal(t, int64(1), n, "exactly the aged undone row is pruned")

	var live, undone int
	require.NoError(t, database.QueryRow(
		`SELECT COUNT(*) FILTER (WHERE NOT undone), COUNT(*) FILTER (WHERE undone) FROM vote_events`).
		Scan(&live, &undone))
	assert.Equal(t, 1, live, "alice's live dislike survives")
	assert.Equal(t, 1, undone, "bob's fresh undone row survives")

	up, down, _ := counts(t, database, subjectPost)
	assert.Zero(t, up)
	assert.Equal(t, 1, down, "served counts unchanged by pruning")
}
