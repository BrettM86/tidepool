package votes

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// Lifecycle of bridged vote records: the voter scrub deletes records whatever
// the switches say, the sweeper scrubs voters who stopped being eligible, and
// pruning keeps rows still owed a record delete.

// voterRows counts every vote_events row of one voter.
func voterRows(t *testing.T, database *sql.DB, voterAPID string) int {
	t.Helper()
	var n int
	require.NoError(t, database.QueryRow(
		`SELECT COUNT(*) FROM vote_events WHERE voter_ap_id = $1`, voterAPID).Scan(&n))
	return n
}

// B13.
func TestScrubDeletesTheVotersRecords(t *testing.T) {
	dryRunBoth := RecordWriterOptions{Mode: RecordsBoth, DryRun: true, Interval: time.Minute, Batch: 20}
	tests := []struct {
		name string
		act  func(t *testing.T, w *recordsWorld)
	}{
		{
			name: "ScrubVoter",
			act: func(t *testing.T, w *recordsWorld) {
				require.NoError(t, w.agg.ScrubVoter(context.Background(), voterAlice))
			},
		},
		{
			name: "ScrubVoter with dry-run on",
			act: func(t *testing.T, w *recordsWorld) {
				w.restartWriter(t, dryRunBoth)
				require.NoError(t, w.agg.ScrubVoter(context.Background(), voterAlice))
			},
		},
		{
			name: "Sweep scrubs a voter whose consent became nobridge",
			act: func(t *testing.T, w *recordsWorld) {
				_, err := w.db.Exec(
					`UPDATE bridged_actors SET consent_state = 'nobridge' WHERE ap_actor_id = $1`, voterAlice)
				require.NoError(t, err)
				require.NoError(t, w.writer.Sweep(context.Background()))
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := newRecordsWorld(t, recordsBoth)
			ctx := context.Background()
			w.secondSubject(t)
			// Alice's Dislike on B arrives before she has a repo (stays
			// aggregate); her Like on A arrives after (recorded). Bob has no
			// bridged actor.
			w.bridgedVoter(t, voterAlice, aliceDID, store.ConsentStateOK, false)
			require.NoError(t, w.agg.ApplyVote(ctx, dislike(activityID(t, 1), voterAlice, subjectPostB), ""))
			w.giveRepo(t, aliceDID)
			require.NoError(t, w.agg.ApplyVote(ctx, like(activityID(t, 2), voterAlice, subjectPost), ""))
			require.NoError(t, w.agg.ApplyVote(ctx, like(activityID(t, 3), voterBob, subjectPost), ""))
			require.Len(t, w.voteRecords(t, aliceDID), 1, "alice's Like must be recorded before the scrub")
			assertServed(t, w.db, subjectPost, 1, 0)
			assertServed(t, w.db, subjectPostB, 0, 1)

			tc.act(t, w)

			assert.Empty(t, w.voteRecords(t, aliceDID), "the scrub deletes alice's vote record")
			assert.Equal(t, 0, voterRows(t, w.db, voterAlice), "alice's vote rows are gone")
			assertServed(t, w.db, subjectPost, 1, 0)
			assertServed(t, w.db, subjectPostB, 0, 0)
		})
	}
}

// B14.
func TestPruneUndoneEventsKeepsRowsOwedARecordDelete(t *testing.T) {
	// The committer lets the Like's create through, then fails the Undo's
	// delete: alice's undone row stays 'written'.
	var gate *failFirstCommitter
	w := newRecordsWorld(t, recordsBoth, withCommitter(func(_ *recordsWorld, next RecordCommitter) RecordCommitter {
		gate = &failFirstCommitter{next: next, failed: true}
		return gate
	}))
	ctx := context.Background()
	w.eligibleVoter(t, voterAlice, aliceDID)
	aliceLike, bobLike := activityID(t, 1), activityID(t, 2)
	require.NoError(t, w.agg.ApplyVote(ctx, like(aliceLike, voterAlice, subjectPost), ""))
	require.Len(t, w.voteRecords(t, aliceDID), 1, "alice's Like must be recorded")
	gate.failed = false
	require.NoError(t, w.agg.RetractVote(ctx, like(activityID(t, 98), voterAlice, subjectPost), ""))
	require.NoError(t, w.agg.ApplyVote(ctx, like(bobLike, voterBob, subjectPost), ""))
	require.NoError(t, w.agg.RetractVote(ctx, like(activityID(t, 99), voterBob, subjectPost), ""))
	aliceRow := readVoteRow(t, w.db, aliceLike)
	require.True(t, aliceRow.undone)
	require.Equal(t, written(), aliceRow.recordState, "alice's undone row is still owed a delete")
	_, err := w.db.Exec(`UPDATE vote_events SET created_at = NOW() - INTERVAL '100 days'`)
	require.NoError(t, err)

	n, err := w.agg.PruneUndoneEvents(ctx, time.Now().Add(-90*24*time.Hour))

	require.NoError(t, err)
	assert.Equal(t, int64(1), n, "only bob's aggregate undone row is pruned")
	assert.Equal(t, written(), readVoteRow(t, w.db, aliceLike).recordState)
	assert.Equal(t, 0, voterRows(t, w.db, voterBob))
}

// Without a RecordWriter the scrub deletes only rows owed no record: a
// 'written' row must survive until a writer can delete its record.
func TestScrubWithoutARecordWriterKeepsWrittenRows(t *testing.T) {
	w := newRecordsWorld(t, recordsBoth)
	ctx := context.Background()
	w.secondSubject(t)
	// Alice's Dislike on B arrives before she has a repo (unrecorded); her
	// Like on A arrives after (written).
	w.bridgedVoter(t, voterAlice, aliceDID, store.ConsentStateOK, false)
	unrecorded, recorded := activityID(t, 1), activityID(t, 2)
	require.NoError(t, w.agg.ApplyVote(ctx, dislike(unrecorded, voterAlice, subjectPostB), ""))
	w.giveRepo(t, aliceDID)
	require.NoError(t, w.agg.ApplyVote(ctx, like(recorded, voterAlice, subjectPost), ""))
	require.Equal(t, written(), readVoteRow(t, w.db, recorded).recordState)
	plain, _ := testAggregator(t, w.db)

	require.NoError(t, plain.ScrubVoter(ctx, voterAlice))

	assert.Equal(t, 1, voterRows(t, w.db, voterAlice), "only the written row is left")
	assert.Equal(t, written(), readVoteRow(t, w.db, recorded).recordState)
	assert.Len(t, w.voteRecords(t, aliceDID), 1)
	assertServed(t, w.db, subjectPostB, 0, 0)
}

// Scrubbing a voter with more records than one commit holds deletes them all
// in commits of at most 100 ops.
func TestScrubDeletesMoreRecordsThanOneCommitHolds(t *testing.T) {
	w := newRecordsWorld(t, recordsBoth)
	ctx := context.Background()
	w.bridgedVoter(t, voterAlice, aliceDID, store.ConsentStateOK, false)
	for i := range 101 {
		subject := fmt.Sprintf("https://lemmy.world/post/%d", 3000+i)
		bridgeSubject(t, w.objects, subject, fmt.Sprintf("scrub%03d", i))
		require.NoError(t, w.agg.ApplyVote(ctx, like(activityID(t, i), voterAlice, subject), ""))
	}
	w.giveRepo(t, aliceDID)
	for range 3 {
		require.NoError(t, w.writer.Sweep(ctx))
	}
	require.Len(t, w.voteRecords(t, aliceDID), 101)
	seq := latestSeq(t, w.db)

	require.NoError(t, w.agg.ScrubVoter(ctx, voterAlice))

	deletes := 0
	for _, ops := range firehoseOpsSince(t, w.db, aliceDID, seq) {
		assert.LessOrEqual(t, len(ops), 100, "ops in one commit")
		deletes += len(ops)
	}
	assert.Equal(t, 101, deletes)
	assert.Empty(t, w.voteRecords(t, aliceDID))
	assert.Equal(t, 0, voterRows(t, w.db, voterAlice))
}
