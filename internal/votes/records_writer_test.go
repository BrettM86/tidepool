package votes

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/repo"
	"tidepool/internal/store"
)

// The RecordWriter's second phase and sweeper: catch-up conversion, the
// bridgedStats ordering barrier, repair of failed commits, stale-work
// rollback, the switches and pacing.

const (
	// A second bridged subject, at://testDID/testCollection/subjectRKeyB.
	subjectPostB = "https://lemmy.world/post/101"
	subjectRKeyB = "3jzfcijpj2z2b"

	carolDID = "did:plc:recordscarol000000000000"
)

// secondSubject bridges subjectPostB.
func (w *recordsWorld) secondSubject(t *testing.T) {
	t.Helper()
	bridgeSubject(t, w.objects, subjectPostB, subjectRKeyB)
}

// ageVotes moves every vote_events row of a voter days into the past.
func ageVotes(t *testing.T, database *sql.DB, voterAPID string, days int) {
	t.Helper()
	_, err := database.Exec(`
		UPDATE vote_events SET created_at = created_at - make_interval(days => $2)
		WHERE voter_ap_id = $1`, voterAPID, days)
	require.NoError(t, err)
}

// recordPath is the {collection}/{rkey} repo path of an at-uri.
func recordPath(t *testing.T, atURI string) string {
	t.Helper()
	parsed, err := syntax.ParseATURI(atURI)
	require.NoError(t, err)
	return parsed.Collection().String() + "/" + parsed.RecordKey().String()
}

// opSeqs returns the seq of every commit for did that carries an op of the
// given action on path, in seq order.
func opSeqs(t *testing.T, database *sql.DB, did, path string, action repo.OpAction) []int64 {
	t.Helper()
	rows, err := database.Query(`SELECT seq, ops FROM firehose_events WHERE did = $1 ORDER BY seq`, did)
	require.NoError(t, err)
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var seq int64
		var raw []byte
		require.NoError(t, rows.Scan(&seq, &raw))
		for _, op := range decodeOps(t, raw) {
			if op.Path == path && op.Action == action {
				out = append(out, seq)
			}
		}
	}
	require.NoError(t, rows.Err())
	return out
}

func decodeOps(t *testing.T, raw []byte) []repo.Op {
	t.Helper()
	var ops []repo.Op
	require.NoError(t, json.Unmarshal(raw, &ops))
	return ops
}

// eventsSince counts firehose events (any DID) after seq.
func eventsSince(t *testing.T, database *sql.DB, seq int64) int {
	t.Helper()
	var n int
	require.NoError(t, database.QueryRow(`SELECT COUNT(*) FROM firehose_events WHERE seq > $1`, seq).Scan(&n))
	return n
}

// recordStates counts vote_events rows by record_state ("" for NULL).
func recordStates(t *testing.T, database *sql.DB) map[string]int {
	t.Helper()
	rows, err := database.Query(`
		SELECT COALESCE(record_state, ''), COUNT(*) FROM vote_events GROUP BY 1`)
	require.NoError(t, err)
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var state string
		var n int
		require.NoError(t, rows.Scan(&state, &n))
		out[state] = n
	}
	require.NoError(t, rows.Err())
	return out
}

// assertServed asserts a subject's served counts.
func assertServed(t *testing.T, database *sql.DB, subject string, wantUp, wantDown int) {
	t.Helper()
	up, down, found := counts(t, database, subject)
	require.True(t, found, "%s has no aggregate", subject)
	assert.Equal(t, wantUp, up, "%s served upvotes", subject)
	assert.Equal(t, wantDown, down, "%s served downvotes", subject)
}

func written() sql.NullString { return sql.NullString{String: "written", Valid: true} }

// failFirstCommitter fails its first ApplyOpsTx without committing, then
// delegates.
type failFirstCommitter struct {
	next   RecordCommitter
	failed bool
}

func (c *failFirstCommitter) ApplyOpsTx(ctx context.Context, did string, ops []repo.RecordOp, sideEffect repo.TxSideEffect) (*repo.CommitResult, error) {
	if !c.failed {
		c.failed = true
		return nil, fmt.Errorf("injected commit failure")
	}
	return c.next.ApplyOpsTx(ctx, did, ops, sideEffect)
}

func withFailingFirstCommit() recordsWorldOption {
	return withCommitter(func(_ *recordsWorld, next RecordCommitter) RecordCommitter {
		return &failFirstCommitter{next: next}
	})
}

// B7.
func TestSweepConvertsVotesOfAVoterWhoGotARepoLater(t *testing.T) {
	tests := []struct {
		name string
		// opts is the writer the votes arrive under.
		opts RecordWriterOptions
		// beforeVotes runs before the votes, afterVotes after they are aged.
		beforeVotes, afterVotes func(t *testing.T, w *recordsWorld)
	}{
		{
			name:        "no bridged actor when voting",
			opts:        recordsBoth,
			beforeVotes: func(*testing.T, *recordsWorld) {},
			afterVotes:  func(t *testing.T, w *recordsWorld) { w.eligibleVoter(t, voterAlice, aliceDID) },
		},
		{
			name: "bridged person without a repo when voting",
			opts: recordsBoth,
			beforeVotes: func(t *testing.T, w *recordsWorld) {
				w.bridgedVoter(t, voterAlice, aliceDID, store.ConsentStateOK, false)
			},
			afterVotes: func(t *testing.T, w *recordsWorld) { w.giveRepo(t, aliceDID) },
		},
		{
			name:        "repo existed while records were off",
			opts:        RecordWriterOptions{Mode: RecordsOff, Interval: time.Minute, Batch: 20},
			beforeVotes: func(t *testing.T, w *recordsWorld) { w.eligibleVoter(t, voterAlice, aliceDID) },
			afterVotes:  func(t *testing.T, w *recordsWorld) { w.restartWriter(t, recordsBoth) },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := newRecordsWorld(t, tc.opts)
			ctx := context.Background()
			w.secondSubject(t)
			tc.beforeVotes(t, w)

			likeA, likeB := activityID(t, 1), activityID(t, 2)
			require.NoError(t, w.agg.ApplyVote(ctx, like(likeA, voterAlice, subjectPost), ""))
			require.NoError(t, w.agg.ApplyVote(ctx, like(likeB, voterAlice, subjectPostB), ""))
			ageVotes(t, w.db, voterAlice, 60)
			assertServed(t, w.db, subjectPost, 1, 0)
			assertServed(t, w.db, subjectPostB, 1, 0)
			tc.afterVotes(t, w)

			require.NoError(t, w.writer.Sweep(ctx))

			require.Len(t, w.voteRecords(t, aliceDID), 2, "both aged Likes must become records")
			assertServed(t, w.db, subjectPost, 0, 0)
			assertServed(t, w.db, subjectPostB, 0, 0)
			for _, id := range []string{likeA, likeB} {
				row := readVoteRow(t, w.db, id)
				assert.Equal(t, written(), row.recordState, id)
				require.True(t, row.recordATURI.Valid, id)
				rkey := row.recordATURI.String[strings.LastIndex(row.recordATURI.String, "/")+1:]
				tid, err := syntax.ParseTID(rkey)
				require.NoError(t, err)
				assert.True(t, tid.Time().Equal(row.createdAt.Truncate(time.Microsecond)),
					"rkey time %s must be the aged created_at %s", tid.Time().UTC(), row.createdAt.UTC())
			}

			commits := firehoseEvents(t, w.db, aliceDID)
			require.NoError(t, w.writer.Sweep(ctx))
			assert.Equal(t, commits, firehoseEvents(t, w.db, aliceDID), "a converted voter commits nothing more")
		})
	}
}

// statsCommit is one bridgedStats repin the committing refresher made.
type statsCommit struct {
	subject            string
	seq                int64
	upvotes, downvotes int
}

// committingRefresher repins bridgedStats the way the real refresher does:
// a real commit to the subject's record carrying the served counts it reads
// at call time.
type committingRefresher struct {
	w       *recordsWorld
	fail    string
	commits []statsCommit
}

func (r *committingRefresher) RefreshSubject(ctx context.Context, subject string) error {
	if subject == r.fail {
		return fmt.Errorf("injected refresh failure for %s", subject)
	}
	var up, down int
	if err := r.w.db.QueryRowContext(ctx, `
		SELECT upvotes, downvotes FROM vote_aggregates WHERE subject_ap_id = $1`, subject).Scan(&up, &down); err != nil {
		return err
	}
	mapping, err := r.w.objects.GetByAPID(ctx, subject)
	if err != nil {
		return err
	}
	res, err := r.w.manager.PutRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey, map[string]any{
		"$type": testCollection,
		"title": "subject " + mapping.RKey,
		"bridgedStats": map[string]any{
			"upvotes":   int64(up),
			"downvotes": int64(down),
		},
	})
	if err != nil {
		return err
	}
	r.commits = append(r.commits, statsCommit{subject: subject, seq: res.Seq, upvotes: up, downvotes: down})
	return nil
}

// B8.
func TestSweepRepinsStatsBeforeCommittingTheVote(t *testing.T) {
	tests := []struct {
		name        string
		failSubject string
	}{
		{name: "both subjects repinned"},
		{name: "refresh fails for subject B", failSubject: subjectPostB},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			refresher := &committingRefresher{fail: tc.failSubject}
			w := newRecordsWorld(t, recordsBoth, withRefresher(func(w *recordsWorld) SubjectRefresher {
				refresher.w = w
				return refresher
			}))
			ctx := context.Background()
			w.secondSubject(t)
			for _, rkey := range []string{subjectRKey, subjectRKeyB} {
				_, err := w.manager.PutRecord(ctx, testDID, testCollection, rkey, map[string]any{
					"$type": testCollection,
					"title": "subject " + rkey,
				})
				require.NoError(t, err)
			}
			w.bridgedVoter(t, voterAlice, aliceDID, store.ConsentStateOK, false)

			likeA, likeB := activityID(t, 1), activityID(t, 2)
			require.NoError(t, w.agg.ApplyVote(ctx, like(likeA, voterAlice, subjectPost), ""))
			require.NoError(t, w.agg.ApplyVote(ctx, like(likeB, voterAlice, subjectPostB), ""))
			// Bob stays aggregate: subject A's stats must still show his Like.
			require.NoError(t, w.agg.ApplyVote(ctx, like(activityID(t, 3), voterBob, subjectPost), ""))
			w.giveRepo(t, aliceDID)

			sweepErr := w.writer.Sweep(ctx)
			if tc.failSubject == "" {
				require.NoError(t, sweepErr)
			}

			wantStats := map[string]int{subjectPost: 1, subjectPostB: 0}
			wantWritten := map[string]string{subjectPost: likeA}
			if tc.failSubject == "" {
				wantWritten[subjectPostB] = likeB
			}
			require.Len(t, w.voteRecords(t, aliceDID), len(wantWritten))
			for subject, id := range wantWritten {
				row := readVoteRow(t, w.db, id)
				require.Equal(t, written(), row.recordState, subject)
				createSeqs := opSeqs(t, w.db, aliceDID, recordPath(t, row.recordATURI.String), repo.OpActionCreate)
				require.Len(t, createSeqs, 1, subject)
				var before []statsCommit
				for _, commit := range refresher.commits {
					if commit.subject == subject && commit.seq < createSeqs[0] {
						before = append(before, commit)
					}
				}
				require.NotEmpty(t, before, "%s: a stats commit must precede the vote create", subject)
				last := before[len(before)-1]
				assert.Equal(t, wantStats[subject], last.upvotes, "%s: repinned stats carry the dropped count", subject)
				assert.Equal(t, 0, last.downvotes, subject)
			}
			if tc.failSubject != "" {
				row := readVoteRow(t, w.db, likeB)
				assert.Equal(t, sql.NullString{String: "pending", Valid: true}, row.recordState)
				require.True(t, row.recordATURI.Valid)
				assert.Empty(t, opSeqs(t, w.db, aliceDID, recordPath(t, row.recordATURI.String), repo.OpActionCreate))
				assertServed(t, w.db, subjectPostB, 0, 0)
			}
		})
	}
}

// B9.
func TestSweepRepairsAFailedRecordCommit(t *testing.T) {
	tests := []struct {
		name string
		undo bool
	}{
		{name: "pending create is written"},
		{name: "pending create undone before the sweep", undo: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := newRecordsWorld(t, recordsBoth, withFailingFirstCommit())
			ctx := context.Background()
			w.eligibleVoter(t, voterAlice, aliceDID)
			commits := firehoseEvents(t, w.db, aliceDID)

			likeID := activityID(t, 1)
			require.NoError(t, w.agg.ApplyVote(ctx, like(likeID, voterAlice, subjectPost), ""))
			assertServed(t, w.db, subjectPost, 0, 0)
			assert.Empty(t, w.voteRecords(t, aliceDID))
			assert.Equal(t, sql.NullString{String: "pending", Valid: true}, readVoteRow(t, w.db, likeID).recordState)

			if tc.undo {
				require.NoError(t, w.agg.RetractVote(ctx, like(activityID(t, 99), voterAlice, subjectPost), ""))
			}
			require.NoError(t, w.writer.Sweep(ctx))

			row := readVoteRow(t, w.db, likeID)
			assertServed(t, w.db, subjectPost, 0, 0)
			if tc.undo {
				assert.True(t, row.undone)
				assert.Equal(t, sql.NullString{String: "removed", Valid: true}, row.recordState)
				assert.Empty(t, w.voteRecords(t, aliceDID))
				assert.Equal(t, commits, firehoseEvents(t, w.db, aliceDID), "a never-written record needs no commit")
				return
			}
			assert.Equal(t, written(), row.recordState)
			assert.Len(t, w.voteRecords(t, aliceDID), 1)
		})
	}
}

// retractFirstCommitter retracts alice's subject-A Like through a second
// aggregator (another process, no record writer) before delegating its first
// call: the ops it was handed are now stale.
type retractFirstCommitter struct {
	t         *testing.T
	next      RecordCommitter
	retractor *Aggregator
	done      bool
}

func (c *retractFirstCommitter) ApplyOpsTx(ctx context.Context, did string, ops []repo.RecordOp, sideEffect repo.TxSideEffect) (*repo.CommitResult, error) {
	if !c.done {
		c.done = true
		if err := c.retractor.RetractVote(ctx, like(activityID(c.t, 99), voterAlice, subjectPost), ""); err != nil {
			return nil, err
		}
	}
	return c.next.ApplyOpsTx(ctx, did, ops, sideEffect)
}

// B10.
func TestStaleRecordWorkRollsBackTheWholeBatch(t *testing.T) {
	tests := []struct {
		name string
		// viaSweep delivers Likes on A and B before alice has a repo, so the
		// sweep's one batch creates both; otherwise the vote path creates A.
		viaSweep bool
	}{
		{name: "vote path, the only create goes stale"},
		{name: "sweep batch of two creates, one goes stale", viaSweep: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := newRecordsWorld(t, recordsBoth, withCommitter(func(w *recordsWorld, next RecordCommitter) RecordCommitter {
				retractor, _ := testAggregator(t, w.db)
				return &retractFirstCommitter{t: t, next: next, retractor: retractor}
			}))
			ctx := context.Background()
			w.secondSubject(t)
			likeA, likeB := activityID(t, 1), activityID(t, 2)
			if tc.viaSweep {
				w.bridgedVoter(t, voterAlice, aliceDID, store.ConsentStateOK, false)
				require.NoError(t, w.agg.ApplyVote(ctx, like(likeA, voterAlice, subjectPost), ""))
				require.NoError(t, w.agg.ApplyVote(ctx, like(likeB, voterAlice, subjectPostB), ""))
				w.giveRepo(t, aliceDID)
				require.NoError(t, w.writer.Sweep(ctx))
			} else {
				w.eligibleVoter(t, voterAlice, aliceDID)
				require.NoError(t, w.agg.ApplyVote(ctx, like(likeA, voterAlice, subjectPost), ""))
			}

			assertServed(t, w.db, subjectPost, 0, 0)
			rowA := readVoteRow(t, w.db, likeA)
			assert.True(t, rowA.undone)
			assert.Equal(t, sql.NullString{String: "removed", Valid: true}, rowA.recordState)
			require.True(t, rowA.recordATURI.Valid, "the stale vote was marked for a record")
			pathA := recordPath(t, rowA.recordATURI.String)
			assert.Empty(t, opSeqs(t, w.db, aliceDID, pathA, repo.OpActionCreate), "the stale create never reaches the firehose")

			if !tc.viaSweep {
				assert.Empty(t, w.voteRecords(t, aliceDID))
				return
			}
			records := w.voteRecords(t, aliceDID)
			require.Len(t, records, 1)
			rowB := readVoteRow(t, w.db, likeB)
			assert.Equal(t, written(), rowB.recordState)
			assert.Equal(t, recordPath(t, rowB.recordATURI.String), voteCollection+"/"+records[0].Rkey)
			assertServed(t, w.db, subjectPostB, 0, 0)
		})
	}
}

// B11.
func TestRecordSwitches(t *testing.T) {
	recordsUp := RecordWriterOptions{Mode: RecordsUp, Interval: time.Minute, Batch: 20}
	recordsOff := RecordWriterOptions{Mode: RecordsOff, Interval: time.Minute, Batch: 20}
	dryRunBoth := RecordWriterOptions{Mode: RecordsBoth, DryRun: true, Interval: time.Minute, Batch: 20}
	sweep := func(t *testing.T, w *recordsWorld) { require.NoError(t, w.writer.Sweep(context.Background())) }
	// convertibleLater delivers votes while alice has no repo, then gives her one.
	convertibleLater := func(votes ...func(t *testing.T, w *recordsWorld)) func(t *testing.T, w *recordsWorld) {
		return func(t *testing.T, w *recordsWorld) {
			w.bridgedVoter(t, voterAlice, aliceDID, store.ConsentStateOK, false)
			for _, vote := range votes {
				vote(t, w)
			}
			w.giveRepo(t, aliceDID)
		}
	}
	// apply delivers vote activityID(t, n) through the current aggregator.
	apply := func(n int, build func(id, voter, subject string) *ap.Object, voter, subject string) func(t *testing.T, w *recordsWorld) {
		return func(t *testing.T, w *recordsWorld) {
			t.Helper()
			require.NoError(t, w.agg.ApplyVote(context.Background(), build(activityID(t, n), voter, subject), ""))
		}
	}
	likeOf, dislikeOf := like, dislike

	tests := []struct {
		name    string
		opts    RecordWriterOptions
		options []recordsWorldOption
		setup   func(t *testing.T, w *recordsWorld)
		act     func(t *testing.T, w *recordsWorld)
		// wantStates maps activityID(t, n) to record_state ("" = NULL).
		wantStates  map[int]string
		wantRecords int
		wantServed  map[string][2]int
		wantEvents  int // firehose events during act
	}{
		{
			name:        "off converts nothing",
			opts:        recordsOff,
			setup:       convertibleLater(apply(1, likeOf, voterAlice, subjectPost)),
			act:         sweep,
			wantStates:  map[int]string{1: ""},
			wantServed:  map[string][2]int{subjectPost: {1, 0}},
			wantRecords: 0, wantEvents: 0,
		},
		{
			name:    "dry run with pending work writes and marks nothing",
			opts:    recordsBoth,
			options: []recordsWorldOption{withFailingFirstCommit()},
			setup: func(t *testing.T, w *recordsWorld) {
				w.eligibleVoter(t, voterAlice, aliceDID)
				apply(1, likeOf, voterAlice, subjectPost)(t, w) // commit fails: pending
				w.bridgedVoter(t, voterCarol, carolDID, store.ConsentStateOK, false)
				apply(2, likeOf, voterCarol, subjectPost)(t, w)
				w.giveRepo(t, carolDID) // carol is now convertible
				w.restartWriter(t, dryRunBoth)
			},
			act:         sweep,
			wantStates:  map[int]string{1: "pending", 2: ""},
			wantServed:  map[string][2]int{subjectPost: {1, 0}},
			wantRecords: 0, wantEvents: 0,
		},
		{
			name: "up converts the Like and leaves the Dislike aggregate",
			opts: recordsUp,
			setup: func(t *testing.T, w *recordsWorld) {
				w.secondSubject(t)
				convertibleLater(apply(1, likeOf, voterAlice, subjectPost), apply(2, dislikeOf, voterAlice, subjectPostB))(t, w)
			},
			act:         sweep,
			wantStates:  map[int]string{1: "written", 2: ""},
			wantServed:  map[string][2]int{subjectPost: {0, 0}, subjectPostB: {0, 1}},
			wantRecords: 1, wantEvents: 1,
		},
		{
			name: "up and a written Like flipped to Dislike",
			opts: recordsUp,
			setup: func(t *testing.T, w *recordsWorld) {
				w.eligibleVoter(t, voterAlice, aliceDID)
				apply(1, likeOf, voterAlice, subjectPost)(t, w)
			},
			act:         apply(2, dislikeOf, voterAlice, subjectPost),
			wantStates:  map[int]string{1: "removed", 2: ""},
			wantServed:  map[string][2]int{subjectPost: {0, 1}},
			wantRecords: 0, wantEvents: 1,
		},
		{
			name: "off still deletes a written Like on Undo",
			opts: recordsBoth,
			setup: func(t *testing.T, w *recordsWorld) {
				w.eligibleVoter(t, voterAlice, aliceDID)
				apply(1, likeOf, voterAlice, subjectPost)(t, w)
				w.restartWriter(t, recordsOff)
			},
			act: func(t *testing.T, w *recordsWorld) {
				require.NoError(t, w.agg.RetractVote(context.Background(), like(activityID(t, 99), voterAlice, subjectPost), ""))
			},
			wantStates:  map[int]string{1: "removed"},
			wantServed:  map[string][2]int{subjectPost: {0, 0}},
			wantRecords: 0, wantEvents: 1,
		},
		{
			name:    "restart with off drains a pending create",
			opts:    recordsBoth,
			options: []recordsWorldOption{withFailingFirstCommit()},
			setup: func(t *testing.T, w *recordsWorld) {
				w.eligibleVoter(t, voterAlice, aliceDID)
				apply(1, likeOf, voterAlice, subjectPost)(t, w) // commit fails: pending
				w.restartWriter(t, recordsOff)
			},
			act:         sweep,
			wantStates:  map[int]string{1: "written"},
			wantServed:  map[string][2]int{subjectPost: {0, 0}},
			wantRecords: 1, wantEvents: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := newRecordsWorld(t, tc.opts, tc.options...)
			tc.setup(t, w)
			seq := latestSeq(t, w.db)

			tc.act(t, w)

			assert.Equal(t, tc.wantEvents, eventsSince(t, w.db, seq), "firehose events")
			assert.Len(t, w.voteRecords(t, aliceDID), tc.wantRecords, "alice's vote records")
			for subject, want := range tc.wantServed {
				assertServed(t, w.db, subject, want[0], want[1])
			}
			for n, want := range tc.wantStates {
				row := readVoteRow(t, w.db, activityID(t, n))
				assert.Equal(t, sql.NullString{String: want, Valid: want != ""}, row.recordState, "vote %d", n)
			}
		})
	}
}

// B12: Batch bounds voters per sweep.
func TestSweepConvertsAtMostBatchVotersPerSweep(t *testing.T) {
	w := newRecordsWorld(t, RecordWriterOptions{Mode: RecordsBoth, Interval: time.Minute, Batch: 1})
	ctx := context.Background()
	w.bridgedVoter(t, voterAlice, aliceDID, store.ConsentStateOK, false)
	w.bridgedVoter(t, voterBob, bobDID, store.ConsentStateOK, false)
	require.NoError(t, w.agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	require.NoError(t, w.agg.ApplyVote(ctx, like(activityID(t, 2), voterBob, subjectPost), ""))
	w.giveRepo(t, aliceDID)
	w.giveRepo(t, bobDID)

	recorded := func() int { return len(w.voteRecords(t, aliceDID)) + len(w.voteRecords(t, bobDID)) }

	require.NoError(t, w.writer.Sweep(ctx))
	require.Equal(t, 1, recorded(), "first sweep converts one voter")
	assertServed(t, w.db, subjectPost, 1, 0)
	assert.Equal(t, map[string]int{"written": 1, "": 1}, recordStates(t, w.db))

	require.NoError(t, w.writer.Sweep(ctx))
	require.Equal(t, 2, recorded(), "second sweep converts the other")
	assertServed(t, w.db, subjectPost, 0, 0)
	assert.Equal(t, map[string]int{"written": 2}, recordStates(t, w.db))
}

// B12: no commit carries more than 100 ops; a larger backlog drains.
func TestSweepCapsOpsPerCommit(t *testing.T) {
	w := newRecordsWorld(t, recordsBoth)
	ctx := context.Background()
	w.bridgedVoter(t, voterAlice, aliceDID, store.ConsentStateOK, false)
	for i := range 101 {
		subject := fmt.Sprintf("https://lemmy.world/post/%d", 1000+i)
		bridgeSubject(t, w.objects, subject, fmt.Sprintf("pacing%03d", i))
		require.NoError(t, w.agg.ApplyVote(ctx, like(activityID(t, i), voterAlice, subject), ""))
	}
	w.giveRepo(t, aliceDID)

	for range 5 {
		require.NoError(t, w.writer.Sweep(ctx))
		if len(w.voteRecords(t, aliceDID)) == 101 {
			break
		}
	}

	require.Len(t, w.voteRecords(t, aliceDID), 101)
	var servedUp int
	require.NoError(t, w.db.QueryRow(`SELECT COALESCE(SUM(upvotes), 0) FROM vote_aggregates`).Scan(&servedUp))
	assert.Equal(t, 0, servedUp)
	creates := 0
	for _, ops := range firehoseOpsSince(t, w.db, aliceDID, 0) {
		assert.LessOrEqual(t, len(ops), 100, "ops in one commit")
		for _, op := range ops {
			if op.Action == repo.OpActionCreate && strings.HasPrefix(op.Path, voteCollection+"/") {
				creates++
			}
		}
	}
	assert.Equal(t, 101, creates)
	assert.Equal(t, map[string]int{"written": 101}, recordStates(t, w.db))
}

// optOutFirstCommitter opts alice out of bridging before delegating its first
// call: the create it was handed was built while she was still eligible.
type optOutFirstCommitter struct {
	database *sql.DB
	next     RecordCommitter
	done     bool
}

func (c *optOutFirstCommitter) ApplyOpsTx(ctx context.Context, did string, ops []repo.RecordOp, sideEffect repo.TxSideEffect) (*repo.CommitResult, error) {
	if !c.done {
		c.done = true
		if _, err := c.database.ExecContext(ctx,
			`UPDATE bridged_actors SET consent_state = 'nobridge' WHERE ap_actor_id = $1`, voterAlice); err != nil {
			return nil, err
		}
	}
	return c.next.ApplyOpsTx(ctx, did, ops, sideEffect)
}

// An opt-out between the writer's read and its commit stops the create; the
// next sweep scrubs the voter's rows.
func TestOptOutBeforeTheRecordCommitStopsTheCreate(t *testing.T) {
	w := newRecordsWorld(t, recordsBoth, withCommitter(func(w *recordsWorld, next RecordCommitter) RecordCommitter {
		return &optOutFirstCommitter{database: w.db, next: next}
	}))
	ctx := context.Background()
	w.eligibleVoter(t, voterAlice, aliceDID)
	likeID := activityID(t, 1)

	require.NoError(t, w.agg.ApplyVote(ctx, like(likeID, voterAlice, subjectPost), ""))

	row := readVoteRow(t, w.db, likeID)
	require.True(t, row.recordATURI.Valid, "the vote was marked for a record while alice was eligible")
	path := recordPath(t, row.recordATURI.String)
	assert.Empty(t, w.voteRecords(t, aliceDID), "no record lands for a voter who opted out mid-commit")
	assert.Empty(t, opSeqs(t, w.db, aliceDID, path, repo.OpActionCreate), "the create never reaches the firehose")

	require.NoError(t, w.writer.Sweep(ctx))

	assert.Empty(t, w.voteRecords(t, aliceDID))
	assert.Empty(t, opSeqs(t, w.db, aliceDID, path, repo.OpActionCreate))
	var rows int
	require.NoError(t, w.db.QueryRow(`SELECT COUNT(*) FROM vote_events WHERE voter_ap_id = $1`, voterAlice).Scan(&rows))
	assert.Equal(t, 0, rows, "the scrub removes the opted-out voter's vote rows")
	assertServed(t, w.db, subjectPost, 0, 0)
}
