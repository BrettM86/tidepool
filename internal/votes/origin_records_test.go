package votes

import (
	"context"
	"database/sql"
	"expvar"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/store"
)

// Origin verification in the record writer (design-verify.md): only a vote
// verified at the voter's origin becomes a record; the verdict is stored on
// vote_events; the sweep verifies oldest-first inside the window, paced per
// host, and reports its backlog.

const (
	voterDave = "https://lemmy.ml/u/dave"
	daveDID   = "did:plc:recordsdave0000000000000"
)

// originWorld is a records world whose writer verifies through the real
// verifier over an in-process fake origin, on one fake clock.
func originWorld(t *testing.T, opts RecordWriterOptions, hostRPS, globalRPS float64, options ...recordsWorldOption) (*recordsWorld, *fakeOrigin, *fakeClock) {
	t.Helper()
	origin, clock := newFakeOrigin(), newFakeClock()
	verifier := newTestVerifier(t, origin, clock, hostRPS, globalRPS)
	w := newRecordsWorld(t, opts, append([]recordsWorldOption{withOrigin(verifier, clock)}, options...)...)
	return w, origin, clock
}

// likeClaim is the claim a Like by voter on subject makes.
func likeClaim(id, voter, subject string) VoteClaim {
	return VoteClaim{ActivityID: id, Actor: voter, Object: subject, Type: ap.TypeLike}
}

// voteIDOn mints a per-test activity id on host.
func voteIDOn(t *testing.T, host string, n int) string {
	return fmt.Sprintf("https://%s/activities/like/%s-%d", host, t.Name(), n)
}

// verifySubject bridges a numbered subject and returns its AP id.
func (w *recordsWorld) verifySubject(t *testing.T, n int) string {
	t.Helper()
	subject := fmt.Sprintf("https://lemmy.world/post/%d", 5000+n)
	bridgeSubject(t, w.objects, subject, fmt.Sprintf("verify%04d", n))
	return subject
}

// originCheck is a vote_events row's stored origin verdict.
type originCheck struct {
	verdict   sql.NullString
	checkedAt sql.NullTime
}

func readOriginCheck(t *testing.T, database *sql.DB, activity string) originCheck {
	t.Helper()
	var c originCheck
	require.NoError(t, database.QueryRow(`
		SELECT origin_check, origin_checked_at FROM vote_events WHERE activity_id = $1`, activity).Scan(
		&c.verdict, &c.checkedAt))
	return c
}

func verdict(v string) sql.NullString { return sql.NullString{String: v, Valid: true} }

// setOriginCheck stores a verdict checked ago in the past.
func setOriginCheck(t *testing.T, database *sql.DB, activity, verdict string, ago time.Duration) {
	t.Helper()
	_, err := database.Exec(`
		UPDATE vote_events SET origin_check = $2, origin_checked_at = now() - make_interval(secs => $3)
		WHERE activity_id = $1`, activity, verdict, ago.Seconds())
	require.NoError(t, err)
}

// uncheckedCandidates counts live unrecorded votes never checked at origin.
func uncheckedCandidates(t *testing.T, database *sql.DB) int {
	t.Helper()
	var n int
	require.NoError(t, database.QueryRow(`
		SELECT COUNT(*) FROM vote_events
		WHERE NOT undone AND record_at_uri IS NULL AND origin_check IS NULL`).Scan(&n))
	return n
}

// O2: the live path verifies an eligible voter's vote before it becomes a
// record.
func TestLiveVoteIsVerifiedAtOrigin(t *testing.T) {
	tests := []struct {
		name string
		// origin configures the fake origin for alice's Like on subjectPost.
		origin func(o *fakeOrigin, claim VoteClaim)
		// spendToken first delivers a verified Like on subjectPostB at the
		// same instant, so alice's host has no token left.
		spendToken  bool
		wantRecord  bool
		wantServed  int
		wantVerdict string // "" = NULL
	}{
		{name: "origin serves the matching activity",
			origin:     func(o *fakeOrigin, c VoteClaim) { o.serve(c) },
			wantRecord: true, wantServed: 0, wantVerdict: "verified"},
		{name: "origin serves a different vote",
			origin:     func(o *fakeOrigin, c VoteClaim) { o.serveForged(c) },
			wantRecord: false, wantServed: 1, wantVerdict: "mismatch"},
		{name: "origin has no such activity",
			origin:     func(*fakeOrigin, VoteClaim) {},
			wantRecord: false, wantServed: 1, wantVerdict: "not_found"},
		{name: "origin is down",
			origin:     func(o *fakeOrigin, c VoteClaim) { o.fail(c, ap.HTTPError{URL: c.ActivityID, StatusCode: 503}) },
			wantRecord: false, wantServed: 1, wantVerdict: "unavailable"},
		{name: "no fetch slot: deferred",
			origin:     func(o *fakeOrigin, c VoteClaim) { o.serve(c) },
			spendToken: true,
			wantRecord: false, wantServed: 1, wantVerdict: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w, origin, clock := originWorld(t, recordsBoth, 1, 5)
			ctx := context.Background()
			w.secondSubject(t)
			w.eligibleVoter(t, voterAlice, aliceDID)
			wantRecords := 0
			if tc.spendToken {
				spent := activityID(t, 2)
				origin.serve(likeClaim(spent, voterAlice, subjectPostB))
				require.NoError(t, w.agg.ApplyVote(ctx, like(spent, voterAlice, subjectPostB), ""))
				require.Equal(t, written(), readVoteRow(t, w.db, spent).recordState, "the first Like takes the token")
				wantRecords = 1
			}
			id := activityID(t, 1)
			tc.origin(origin, likeClaim(id, voterAlice, subjectPost))

			require.NoError(t, w.agg.ApplyVote(ctx, like(id, voterAlice, subjectPost), ""))

			if tc.wantRecord {
				wantRecords++
			}
			assert.Len(t, w.voteRecords(t, aliceDID), wantRecords, "alice's records")
			row := readVoteRow(t, w.db, id)
			if tc.wantRecord {
				assert.Equal(t, written(), row.recordState)
			} else {
				assert.Equal(t, sql.NullString{}, row.recordState, "an unverified vote stays aggregate")
			}
			assertServed(t, w.db, subjectPost, tc.wantServed, 0)
			check := readOriginCheck(t, w.db, id)
			assert.Equal(t, sql.NullString{String: tc.wantVerdict, Valid: tc.wantVerdict != ""}, check.verdict)
			assert.Equal(t, tc.wantVerdict != "", check.checkedAt.Valid, "origin_checked_at is set with the verdict")

			if !tc.spendToken {
				return
			}
			// The next sweep has a token: it verifies and converts the vote.
			clock.Advance(time.Second)
			require.NoError(t, w.writer.Sweep(ctx))
			assert.Len(t, w.voteRecords(t, aliceDID), 2)
			assert.Equal(t, written(), readVoteRow(t, w.db, id).recordState)
			assert.Equal(t, verdict("verified"), readOriginCheck(t, w.db, id).verdict)
			assertServed(t, w.db, subjectPost, 0, 0)
		})
	}
}

// O3: a voter's verified vote never carries their unverified one into a
// record.
func TestSweepConvertsOnlyTheVotesVerifiedAtOrigin(t *testing.T) {
	w, origin, _ := originWorld(t, recordsBoth, 1, 5)
	ctx := context.Background()
	w.secondSubject(t)
	w.bridgedVoter(t, voterAlice, aliceDID, store.ConsentStateOK, false)
	honest, forged := activityID(t, 1), activityID(t, 2)
	origin.serve(likeClaim(honest, voterAlice, subjectPost))
	origin.serveForged(likeClaim(forged, voterAlice, subjectPostB))
	require.NoError(t, w.agg.ApplyVote(ctx, like(honest, voterAlice, subjectPost), ""))
	require.NoError(t, w.agg.ApplyVote(ctx, like(forged, voterAlice, subjectPostB), ""))
	w.giveRepo(t, aliceDID)

	require.NoError(t, w.writer.Sweep(ctx))

	records := w.voteRecords(t, aliceDID)
	require.Len(t, records, 1, "only the verified Like becomes a record")
	assert.Equal(t, map[string]any{"uri": subjectATURI, "cid": testCID}, records[0].Value["subject"])
	assert.Equal(t, written(), readVoteRow(t, w.db, honest).recordState)
	assert.Equal(t, verdict("verified"), readOriginCheck(t, w.db, honest).verdict)
	assert.Equal(t, sql.NullString{}, readVoteRow(t, w.db, forged).recordState, "the forged Like stays aggregate")
	assert.Equal(t, verdict("mismatch"), readOriginCheck(t, w.db, forged).verdict)
	assertServed(t, w.db, subjectPost, 0, 0)
	assertServed(t, w.db, subjectPostB, 1, 0)
}

// O4: only votes inside the window are checked; an unavailable vote that
// aged out is expired.
func TestSweepVerifiesOnlyInsideTheWindow(t *testing.T) {
	w, origin, _ := originWorld(t, recordsBoth, 1, 5)
	ctx := context.Background()
	w.bridgedVoter(t, voterAlice, aliceDID, store.ConsentStateOK, false)
	type vote struct {
		age time.Duration
		// unavailable stores an 'unavailable' verdict checked when the vote
		// arrived.
		unavailable bool
		wantVerdict string // "" = NULL
		wantRecord  bool
		wantFetches int
	}
	votes := []vote{
		{age: 8 * 24 * time.Hour, wantVerdict: "", wantRecord: false, wantFetches: 0},
		{age: 3 * 24 * time.Hour, wantVerdict: "verified", wantRecord: true, wantFetches: 1},
		{age: 8 * 24 * time.Hour, unavailable: true, wantVerdict: "expired", wantRecord: false, wantFetches: 0},
	}
	ids := make([]string, len(votes))
	subjects := make([]string, len(votes))
	for i, v := range votes {
		ids[i], subjects[i] = activityID(t, i), w.verifySubject(t, i)
		origin.serve(likeClaim(ids[i], voterAlice, subjects[i]))
		require.NoError(t, w.agg.ApplyVote(ctx, like(ids[i], voterAlice, subjects[i]), ""))
		ageVote(t, w.db, ids[i], v.age)
		if v.unavailable {
			setOriginCheck(t, w.db, ids[i], "unavailable", v.age)
		}
	}
	w.giveRepo(t, aliceDID)

	require.NoError(t, w.writer.Sweep(ctx))

	assert.Len(t, w.voteRecords(t, aliceDID), 1)
	for i, v := range votes {
		check := readOriginCheck(t, w.db, ids[i])
		assert.Equal(t, sql.NullString{String: v.wantVerdict, Valid: v.wantVerdict != ""}, check.verdict, "vote %d", i)
		assert.Equal(t, v.wantFetches, origin.fetches(ids[i]), "vote %d fetches", i)
		if v.wantRecord {
			assert.Equal(t, written(), readVoteRow(t, w.db, ids[i]).recordState, "vote %d", i)
			assertServed(t, w.db, subjects[i], 0, 0)
		} else {
			assert.Equal(t, sql.NullString{}, readVoteRow(t, w.db, ids[i]).recordState, "vote %d", i)
			assertServed(t, w.db, subjects[i], 1, 0)
		}
	}
}

// O5: an unavailable host holding the oldest candidates does not starve a
// healthy host, and each host's oldest candidate goes first.
func TestSweepVerifiesOldestFirstWithoutStarvingHealthyHosts(t *testing.T) {
	// One fetch per host per 100s: only the first fetch on each host fits in
	// a sweep's budget (80% of a minute).
	w, origin, _ := originWorld(t, recordsBoth, 0.01, 100)
	ctx := context.Background()
	w.bridgedVoter(t, voterDave, daveDID, store.ConsentStateOK, false)
	w.bridgedVoter(t, voterAlice, aliceDID, store.ConsentStateOK, false)
	type vote struct {
		voter       string
		id          string
		age         time.Duration
		down        bool // the origin answers 503
		wantVerdict string
		wantRecord  bool
	}
	votes := []vote{
		{voter: voterDave, id: voteIDOn(t, "lemmy.ml", 1), age: 5 * 24 * time.Hour, down: true, wantVerdict: "unavailable"},
		{voter: voterDave, id: voteIDOn(t, "lemmy.ml", 2), age: 4 * 24 * time.Hour, down: true, wantVerdict: ""},
		{voter: voterAlice, id: activityID(t, 3), age: 2 * 24 * time.Hour, wantVerdict: "verified", wantRecord: true},
		{voter: voterAlice, id: activityID(t, 4), age: 1 * 24 * time.Hour, wantVerdict: ""},
	}
	subjects := make([]string, len(votes))
	for i, v := range votes {
		subjects[i] = w.verifySubject(t, i)
		claim := likeClaim(v.id, v.voter, subjects[i])
		if v.down {
			origin.fail(claim, ap.HTTPError{URL: v.id, StatusCode: 503})
		} else {
			origin.serve(claim)
		}
		require.NoError(t, w.agg.ApplyVote(ctx, like(v.id, v.voter, subjects[i]), ""))
		ageVote(t, w.db, v.id, v.age)
	}
	w.giveRepo(t, daveDID)
	w.giveRepo(t, aliceDID)

	require.NoError(t, w.writer.Sweep(ctx))

	assert.Empty(t, w.voteRecords(t, daveDID))
	assert.Len(t, w.voteRecords(t, aliceDID), 1, "the healthy host's oldest Like is converted")
	for i, v := range votes {
		check := readOriginCheck(t, w.db, v.id)
		assert.Equal(t, sql.NullString{String: v.wantVerdict, Valid: v.wantVerdict != ""}, check.verdict, "vote %d", i)
		row := readVoteRow(t, w.db, v.id)
		if v.wantRecord {
			assert.Equal(t, written(), row.recordState, "vote %d", i)
			assertServed(t, w.db, subjects[i], 0, 0)
		} else {
			assert.Equal(t, sql.NullString{}, row.recordState, "vote %d", i)
			assertServed(t, w.db, subjects[i], 1, 0)
		}
	}
	assert.Equal(t, 0, origin.fetches(votes[1].id), "the host in backoff is not fetched again")
	assert.Equal(t, 0, origin.fetches(votes[3].id), "the newer Like waits for the next token")
}

// O6: verification drains a backlog when arrivals are below its rate, and the
// backlog grows when they are above it.
func TestVerificationDrainsFasterThanArrivals(t *testing.T) {
	const (
		sweepsWithArrivals = 4
		idleSweeps         = 2
	)
	tests := []struct {
		name string
		// arrivals is the number of new Likes before each sweep. One host at
		// 1 rps with a 10s interval (8s budget) verifies about 9 per sweep.
		arrivals int
		drains   bool
	}{
		{name: "arrivals below capacity", arrivals: 3, drains: true},
		{name: "arrivals above capacity", arrivals: 20, drains: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := RecordWriterOptions{Mode: RecordsBoth, Interval: 10 * time.Second, Batch: 20}
			w, origin, clock := originWorld(t, opts, 1, 100)
			ctx := context.Background()
			w.eligibleVoter(t, voterAlice, aliceDID)

			var backlog []int
			n := 0
			for sweep := range sweepsWithArrivals + idleSweeps {
				clock.Advance(opts.Interval)
				if sweep < sweepsWithArrivals {
					for range tc.arrivals {
						id, subject := activityID(t, n), w.verifySubject(t, n)
						origin.serve(likeClaim(id, voterAlice, subject))
						require.NoError(t, w.agg.ApplyVote(ctx, like(id, voterAlice, subject), ""))
						n++
					}
				}
				require.NoError(t, w.writer.Sweep(ctx))
				backlog = append(backlog, uncheckedCandidates(t, w.db))
			}

			if tc.drains {
				for i := 1; i < len(backlog); i++ {
					assert.LessOrEqual(t, backlog[i], backlog[i-1], "backlog after sweep %d grew: %v", i, backlog)
				}
				assert.Equal(t, 0, backlog[len(backlog)-1], "backlog drains once arrivals stop: %v", backlog)
				assert.Len(t, w.voteRecords(t, aliceDID), sweepsWithArrivals*tc.arrivals)
				return
			}
			require.Greater(t, backlog[0], 0, "backlog: %v", backlog)
			for i := 1; i < sweepsWithArrivals; i++ {
				assert.Greater(t, backlog[i], backlog[i-1], "backlog after sweep %d must grow: %v", i, backlog)
			}
		})
	}
}

// deferringVerifier never has a fetch slot.
type deferringVerifier struct{}

func (deferringVerifier) Verify(context.Context, VoteClaim) (OriginVerdict, bool) { return "", true }

// capturingHandler keeps every log record.
type capturingHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *capturingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *capturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capturingHandler) WithGroup(string) slog.Handler      { return h }

func (h *capturingHandler) count(level slog.Level) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range h.records {
		if r.Level == level {
			n++
		}
	}
	return n
}

// expvarFloat reads a published expvar number.
func expvarFloat(t *testing.T, name string) float64 {
	t.Helper()
	v := expvar.Get(name)
	require.NotNil(t, v, "expvar %s is not published", name)
	f, err := strconv.ParseFloat(v.String(), 64)
	require.NoError(t, err, "expvar %s = %q", name, v.String())
	return f
}

// O7: the sweep publishes the verification backlog and the oldest pending
// age, and warns when the oldest nears the window's edge.
func TestSweepReportsTheVerificationBacklog(t *testing.T) {
	tests := []struct {
		name       string
		oldest     time.Duration
		wantWarned int
	}{
		{name: "oldest within a day of the window edge", oldest: 5*24*time.Hour + time.Hour, wantWarned: 1},
		{name: "oldest well inside the window", oldest: 4 * 24 * time.Hour, wantWarned: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logs := &capturingHandler{}
			clock := newFakeClock()
			opts := RecordWriterOptions{Mode: RecordsBoth, Interval: time.Second, Batch: 20, Window: 6 * 24 * time.Hour}
			w := newRecordsWorld(t, opts, withOrigin(deferringVerifier{}, clock), withLogger(logs))
			ctx := context.Background()
			w.secondSubject(t)
			w.eligibleVoter(t, voterAlice, aliceDID)
			oldest, newer := activityID(t, 1), activityID(t, 2)
			require.NoError(t, w.agg.ApplyVote(ctx, like(oldest, voterAlice, subjectPost), ""))
			require.NoError(t, w.agg.ApplyVote(ctx, like(newer, voterAlice, subjectPostB), ""))
			ageVote(t, w.db, oldest, tc.oldest)
			ageVote(t, w.db, newer, 24*time.Hour)
			warnsBefore := logs.count(slog.LevelWarn)

			require.NoError(t, w.writer.Sweep(ctx))

			assert.Equal(t, float64(2), expvarFloat(t, "tidepool_vote_verify_backlog"))
			assert.InDelta(t, tc.oldest.Seconds(), expvarFloat(t, "tidepool_vote_verify_oldest_pending_seconds"), 120)
			assert.Equal(t, tc.wantWarned, logs.count(slog.LevelWarn)-warnsBefore, "warnings during the sweep")
		})
	}
}

// O8: a verified vote returned to the aggregate (subject soft-deleted) is
// converted after the restore without fetching it again, even once it is
// outside the window.
func TestVerifiedVoteReturnedToTheAggregateConvertsWithoutRefetch(t *testing.T) {
	tests := []struct {
		name string
		age  time.Duration
	}{
		{name: "inside the window"},
		{name: "aged outside the window", age: 8 * 24 * time.Hour},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			deleted := false
			w, origin, _ := originWorld(t, recordsBoth, 1, 5, withRefresher(func(w *recordsWorld) SubjectRefresher {
				return refresherFunc(func(ctx context.Context, subject string) error {
					if !deleted && subject == subjectPost {
						deleted = true
						return w.objects.SoftDelete(ctx, subject)
					}
					return nil
				})
			}))
			ctx := context.Background()
			w.eligibleVoter(t, voterAlice, aliceDID)
			id := activityID(t, 1)
			origin.serve(likeClaim(id, voterAlice, subjectPost))

			require.NoError(t, w.agg.ApplyVote(ctx, like(id, voterAlice, subjectPost), ""))
			require.True(t, deleted, "the refresh hook must have soft-deleted the subject")
			require.Equal(t, sql.NullString{}, readVoteRow(t, w.db, id).recordState, "the vote is back in the aggregate")
			require.Equal(t, verdict("verified"), readOriginCheck(t, w.db, id).verdict, "returning to the aggregate keeps the verdict")
			fetches := origin.fetches(id)
			require.Equal(t, 1, fetches)
			if tc.age > 0 {
				ageVote(t, w.db, id, tc.age)
			}

			require.NoError(t, w.objects.Restore(ctx, subjectPost))
			require.NoError(t, w.writer.Sweep(ctx))

			assert.Len(t, w.voteRecords(t, aliceDID), 1, "the restored subject's verified Like becomes a record")
			assert.Equal(t, written(), readVoteRow(t, w.db, id).recordState)
			assert.Equal(t, fetches, origin.fetches(id), "no new fetch for a verified vote")
			assertServed(t, w.db, subjectPost, 0, 0)
		})
	}
}

// voterOn is the n-th bridged test voter on host: its AP id and DID.
func voterOn(host string, n int) (apID, did string) {
	return fmt.Sprintf("https://%s/u/voter%d", host, n), fmt.Sprintf("did:plc:recordshost%02d00000000000", n)
}

// countOriginChecks counts vote_events rows holding verdict.
func countOriginChecks(t *testing.T, database *sql.DB, verdict string) int {
	t.Helper()
	var n int
	require.NoError(t, database.QueryRow(`
		SELECT COUNT(*) FROM vote_events WHERE origin_check = $1`, verdict).Scan(&n))
	return n
}

// O5: the sweep's verification budget is wall-clock: time spent fetching
// counts, not only the pauses between rounds.
func TestSweepVerificationBudgetCountsFetchTime(t *testing.T) {
	// A one-minute interval is a 48s budget; every fetch takes 10s.
	opts := RecordWriterOptions{Mode: RecordsBoth, Interval: time.Minute, Batch: 20}
	w, origin, clock := originWorld(t, opts, 1, 100)
	ctx := context.Background()
	const hosts = 10
	dids := make([]string, hosts)
	for i := range hosts {
		host := fmt.Sprintf("lemmy%d.example", i)
		voter, did := voterOn(host, i)
		dids[i] = did
		w.bridgedVoter(t, voter, did, store.ConsentStateOK, false)
		id, subject := voteIDOn(t, host, i), w.verifySubject(t, i)
		origin.serve(likeClaim(id, voter, subject))
		require.NoError(t, w.agg.ApplyVote(ctx, like(id, voter, subject), ""))
	}
	for _, did := range dids {
		w.giveRepo(t, did)
	}
	origin.mu.Lock()
	origin.onFetch = func() { clock.Advance(10 * time.Second) }
	origin.mu.Unlock()

	require.NoError(t, w.writer.Sweep(ctx))

	assert.Equal(t, 5, origin.totalFetches(), "fetches starting at 0s, 10s, 20s, 30s and 40s fit a 48s budget")
	assert.Equal(t, 5, countOriginChecks(t, w.db, "verified"))
	assert.Equal(t, 5, uncheckedCandidates(t, w.db), "the rest stay unchecked")

	require.NoError(t, w.writer.Sweep(ctx))

	assert.Equal(t, 10, origin.totalFetches())
	assert.Equal(t, 10, countOriginChecks(t, w.db, "verified"), "the next sweep verifies the rest")
	for _, did := range dids {
		assert.Len(t, w.voteRecords(t, did), 1, "%s's records", did)
	}
}

// O5: candidates are selected round-robin across voter hosts, so hosts with
// many old candidates cannot fill the batch.
func TestSweepSelectsCandidatesAcrossHosts(t *testing.T) {
	opts := RecordWriterOptions{Mode: RecordsBoth, Interval: time.Minute, Batch: 20, VerifyBatch: 6}
	w, origin, _ := originWorld(t, opts, 1, 100)
	ctx := context.Background()
	var dids []string
	n := 0
	for i := range 5 {
		host := fmt.Sprintf("stalled%d.example", i)
		voter, did := voterOn(host, i)
		dids = append(dids, did)
		w.bridgedVoter(t, voter, did, store.ConsentStateOK, false)
		for j := range 3 {
			id, subject := voteIDOn(t, host, n), w.verifySubject(t, n)
			origin.fail(likeClaim(id, voter, subject), ap.HTTPError{URL: id, StatusCode: 503})
			require.NoError(t, w.agg.ApplyVote(ctx, like(id, voter, subject), ""))
			ageVote(t, w.db, id, 5*24*time.Hour-time.Duration(i)*time.Hour-time.Duration(j)*time.Minute)
			n++
		}
	}
	w.bridgedVoter(t, voterAlice, aliceDID, store.ConsentStateOK, false)
	healthy, subject := activityID(t, n), w.verifySubject(t, n)
	origin.serve(likeClaim(healthy, voterAlice, subject))
	require.NoError(t, w.agg.ApplyVote(ctx, like(healthy, voterAlice, subject), ""))
	ageVote(t, w.db, healthy, time.Hour)
	for _, did := range append(dids, aliceDID) {
		w.giveRepo(t, did)
	}

	require.NoError(t, w.writer.Sweep(ctx))

	assert.Equal(t, verdict("verified"), readOriginCheck(t, w.db, healthy).verdict, "the healthy host's newer vote is selected")
	assert.Equal(t, written(), readVoteRow(t, w.db, healthy).recordState)
	assert.Len(t, w.voteRecords(t, aliceDID), 1)
	assertServed(t, w.db, subject, 0, 0)
}

// O5: a host's never-checked votes go before its retryable 'unavailable'
// ones, so one failing activity does not block the host's fresh votes.
func TestSweepChecksFreshVotesBeforeRetryingUnavailableOnes(t *testing.T) {
	// One fetch per 25s on the host: two fit a 48s budget.
	w, origin, _ := originWorld(t, recordsBoth, 0.04, 100)
	ctx := context.Background()
	w.bridgedVoter(t, voterDave, daveDID, store.ConsentStateOK, false)
	type vote struct {
		id          string
		age         time.Duration
		down        bool
		wantVerdict string
		wantRecord  bool
	}
	votes := []vote{
		{id: voteIDOn(t, "lemmy.ml", 1), age: time.Hour, down: true, wantVerdict: "unavailable"},
		{id: voteIDOn(t, "lemmy.ml", 2), age: 30 * time.Minute, wantVerdict: "verified", wantRecord: true},
		{id: voteIDOn(t, "lemmy.ml", 3), age: 20 * time.Minute, wantVerdict: "verified", wantRecord: true},
	}
	subjects := make([]string, len(votes))
	for i, v := range votes {
		subjects[i] = w.verifySubject(t, i)
		claim := likeClaim(v.id, voterDave, subjects[i])
		if v.down {
			origin.fail(claim, ap.HTTPError{URL: v.id, StatusCode: 503})
		} else {
			origin.serve(claim)
		}
		require.NoError(t, w.agg.ApplyVote(ctx, like(v.id, voterDave, subjects[i]), ""))
		ageVote(t, w.db, v.id, v.age)
		if v.down {
			setOriginCheck(t, w.db, v.id, "unavailable", time.Minute)
		}
	}
	w.giveRepo(t, daveDID)

	require.NoError(t, w.writer.Sweep(ctx))

	assert.Len(t, w.voteRecords(t, daveDID), 2, "both fresh votes are recorded")
	for i, v := range votes {
		assert.Equal(t, verdict(v.wantVerdict), readOriginCheck(t, w.db, v.id).verdict, "vote %d", i)
		if v.wantRecord {
			assert.Equal(t, written(), readVoteRow(t, w.db, v.id).recordState, "vote %d", i)
			assertServed(t, w.db, subjects[i], 0, 0)
		} else {
			assert.Equal(t, sql.NullString{}, readVoteRow(t, w.db, v.id).recordState, "vote %d", i)
			assertServed(t, w.db, subjects[i], 1, 0)
		}
	}
	assert.Equal(t, 0, origin.fetches(votes[0].id), "the retryable vote waits behind the fresh ones")
}

// O2: at most one inline origin fetch is in flight; a live vote arriving
// meanwhile is not fetched and stays aggregate for the sweep.
func TestLiveVerificationHoldsOneInlineFetch(t *testing.T) {
	origin, clock := newFakeOrigin(), newFakeClock()
	verifier, err := NewOriginVerifier(origin, OriginVerifierOptions{
		HostRPS: 1, GlobalRPS: 5, Timeout: time.Minute, Now: clock.Now,
	}, slog.Default())
	require.NoError(t, err)
	w := newRecordsWorld(t, recordsBoth, withOrigin(verifier, clock))
	ctx := context.Background()
	w.secondSubject(t)
	w.eligibleVoter(t, voterAlice, aliceDID)
	w.eligibleVoter(t, voterDave, daveDID)

	held := activityID(t, 1)
	started, release := make(chan struct{}, 1), make(chan struct{})
	var releaseOnce sync.Once
	releaseHeld := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseHeld)
	origin.answer(held, originAnswer{
		object: matchingActivity(likeClaim(held, voterAlice, subjectPost)), started: started, release: release,
	})
	heldDone := make(chan error, 1)
	go func() { heldDone <- w.agg.ApplyVote(ctx, like(held, voterAlice, subjectPost), "") }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the first live vote never reached its origin")
	}
	// The global bucket has a token again; only the inline slot is taken.
	clock.Advance(time.Second)

	second := voteIDOn(t, "lemmy.ml", 2)
	origin.serve(likeClaim(second, voterDave, subjectPostB))
	secondDone := make(chan error, 1)
	go func() { secondDone <- w.agg.ApplyVote(ctx, like(second, voterDave, subjectPostB), "") }()
	select {
	case err := <-secondDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("a live vote waited for another vote's origin fetch")
	}

	assert.Equal(t, 0, origin.fetches(second), "no inline fetch while one is in flight")
	assert.Equal(t, originCheck{}, readOriginCheck(t, w.db, second), "nothing stored: the sweep picks it up")
	assert.Equal(t, sql.NullString{}, readVoteRow(t, w.db, second).recordState)
	assertServed(t, w.db, subjectPostB, 1, 0)

	releaseHeld()
	select {
	case err := <-heldDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the held vote never finished")
	}
	// The slot is free again: the next live vote is checked inline.
	clock.Advance(time.Second)
	third, subject := voteIDOn(t, "lemmy.ml", 3), w.verifySubject(t, 3)
	origin.serve(likeClaim(third, voterDave, subject))
	require.NoError(t, w.agg.ApplyVote(ctx, like(third, voterDave, subject), ""))
	assert.Equal(t, verdict("verified"), readOriginCheck(t, w.db, third).verdict)
	assert.Equal(t, written(), readVoteRow(t, w.db, third).recordState)
}

// O4: an 'unavailable' vote inside the window is fetched again once its retry
// delay has passed, and not before.
func TestSweepRetriesAnUnavailableVoteAfterItsDelay(t *testing.T) {
	w, origin, _ := originWorld(t, recordsBoth, 1, 5)
	ctx := context.Background()
	w.bridgedVoter(t, voterDave, daveDID, store.ConsentStateOK, false)
	w.bridgedVoter(t, voterAlice, aliceDID, store.ConsentStateOK, false)
	retried, retriedSubject := voteIDOn(t, "lemmy.ml", 1), w.verifySubject(t, 1)
	recent, recentSubject := activityID(t, 2), w.verifySubject(t, 2)
	origin.serve(likeClaim(retried, voterDave, retriedSubject))
	origin.serve(likeClaim(recent, voterAlice, recentSubject))
	require.NoError(t, w.agg.ApplyVote(ctx, like(retried, voterDave, retriedSubject), ""))
	require.NoError(t, w.agg.ApplyVote(ctx, like(recent, voterAlice, recentSubject), ""))
	ageVote(t, w.db, retried, time.Hour)
	ageVote(t, w.db, recent, time.Hour)
	setOriginCheck(t, w.db, retried, "unavailable", time.Minute)
	setOriginCheck(t, w.db, recent, "unavailable", 10*time.Second)
	w.giveRepo(t, daveDID)
	w.giveRepo(t, aliceDID)

	require.NoError(t, w.writer.Sweep(ctx))

	assert.Equal(t, 1, origin.fetches(retried))
	assert.Equal(t, verdict("verified"), readOriginCheck(t, w.db, retried).verdict)
	assert.Equal(t, written(), readVoteRow(t, w.db, retried).recordState)
	assert.Len(t, w.voteRecords(t, daveDID), 1)
	assertServed(t, w.db, retriedSubject, 0, 0)

	assert.Equal(t, 0, origin.fetches(recent), "checked 10s ago: not retried yet")
	assert.Equal(t, verdict("unavailable"), readOriginCheck(t, w.db, recent).verdict)
	assert.Empty(t, w.voteRecords(t, aliceDID))
	assertServed(t, w.db, recentSubject, 1, 0)
}

// O4: the sweep never fetches a vote that could not become a record.
func TestSweepDoesNotCheckVotesThatStayAggregate(t *testing.T) {
	tests := []struct {
		name     string
		opts     RecordWriterOptions
		eligible bool
		voter    string
		claim    func(t *testing.T, voter, subject string) VoteClaim
		wantUp   int
		wantDown int
	}{
		{name: "a voter who is not bridged", opts: recordsBoth, voter: voterBob,
			claim: func(t *testing.T, voter, subject string) VoteClaim {
				return likeClaim(voteIDOn(t, "lemmy.zip", 1), voter, subject)
			},
			wantUp: 1},
		{name: "a Dislike under mode up", opts: RecordWriterOptions{Mode: RecordsUp, Interval: time.Minute, Batch: 20},
			eligible: true, voter: voterAlice,
			claim: func(t *testing.T, voter, subject string) VoteClaim {
				return VoteClaim{ActivityID: fmt.Sprintf("https://lemmy.world/activities/dislike/%s-1", t.Name()),
					Actor: voter, Object: subject, Type: ap.TypeDislike}
			},
			wantDown: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w, origin, _ := originWorld(t, tc.opts, 1, 5)
			ctx := context.Background()
			if tc.eligible {
				w.eligibleVoter(t, tc.voter, aliceDID)
			}
			claim := tc.claim(t, tc.voter, subjectPost)
			origin.serve(claim)
			require.NoError(t, w.agg.ApplyVote(ctx, vote(claim.Type, claim.ActivityID, tc.voter, subjectPost), ""))

			require.NoError(t, w.writer.Sweep(ctx))

			assert.Equal(t, 0, origin.totalFetches())
			assert.Equal(t, originCheck{}, readOriginCheck(t, w.db, claim.ActivityID))
			assert.Equal(t, sql.NullString{}, readVoteRow(t, w.db, claim.ActivityID).recordState)
			assertServed(t, w.db, subjectPost, tc.wantUp, tc.wantDown)
		})
	}
}
