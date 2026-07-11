package votes

import (
	"context"
	"database/sql"
	stderrors "errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// The real materializer is the production StatsEmitter; pin the interface so a
// signature drift is a compile error, not an e2e surprise.
var _ StatsEmitter = (*materialize.Materializer)(nil)

// emitCall records one SetBridgedStats invocation.
type emitCall struct {
	subject  string
	up, down int
	asOf     time.Time
}

// fakeEmitter stands in for the materializer: it records calls and returns a
// configurable committed/error so the watermark logic can be exercised in
// isolation (the real record write is covered in internal/materialize).
type fakeEmitter struct {
	mu    sync.Mutex
	calls []emitCall
	fn    func(mapping *store.APObjectMapping, up, down int) (committed bool, err error)
}

func (f *fakeEmitter) EmitBridgedStats(_ context.Context, mapping *store.APObjectMapping, up, down int, asOf time.Time) (bool, error) {
	f.mu.Lock()
	f.calls = append(f.calls, emitCall{subject: mapping.APID, up: up, down: down, asOf: asOf})
	fn := f.fn
	f.mu.Unlock()
	if fn != nil {
		return fn(mapping, up, down)
	}
	return true, nil
}

func (f *fakeEmitter) snapshot() []emitCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]emitCall(nil), f.calls...)
}

// testRefresher builds a Refresher over the test database with a fake emitter.
func testRefresher(t *testing.T, database *sql.DB, emitter StatsEmitter) *Refresher {
	t.Helper()
	r, err := NewRefresher(database, store.NewAPObjects(database), emitter, 0, 0, slog.Default())
	require.NoError(t, err)
	return r
}

// aggregateTimes reads a subject's updated_at and stats_emitted_at watermark.
func aggregateTimes(t *testing.T, database *sql.DB, subject string) (updated time.Time, emitted sql.NullTime, found bool) {
	t.Helper()
	err := database.QueryRow(`
		SELECT updated_at, stats_emitted_at FROM vote_aggregates WHERE subject_ap_id = $1`,
		subject).Scan(&updated, &emitted)
	if stderrors.Is(err, sql.ErrNoRows) {
		return time.Time{}, sql.NullTime{}, false
	}
	require.NoError(t, err)
	return updated, emitted, true
}

// TestRefresherEmitsDueRowOnce: a row with votes is emitted once, its
// watermark is stamped with the row's updated_at (the counts' asOf), and a
// second sweep with no new vote does not re-emit.
func TestRefresherEmitsDueRowOnce(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	require.NoError(t, agg.ApplyVote(ctx, dislike(activityID(t, 2), voterBob, subjectPost), ""))

	emitter := &fakeEmitter{}
	r := testRefresher(t, database, emitter)
	r.Sweep(ctx)

	calls := emitter.snapshot()
	require.Len(t, calls, 1, "the dirty row is emitted exactly once")
	assert.Equal(t, subjectPost, calls[0].subject)
	assert.Equal(t, 1, calls[0].up)
	assert.Equal(t, 1, calls[0].down)

	updated, emitted, found := aggregateTimes(t, database, subjectPost)
	require.True(t, found)
	require.True(t, emitted.Valid, "the watermark was stamped")
	assert.True(t, emitted.Time.Equal(updated), "watermark is the row's updated_at, not now()")
	assert.True(t, calls[0].asOf.Equal(updated), "asOf is the counts' updated_at")

	// Nothing changed: a second sweep must not re-emit.
	r.Sweep(ctx)
	assert.Len(t, emitter.snapshot(), 1, "an un-dirtied row is not re-emitted")
}

// TestRefresherConcurrentUpdateRedirties: a vote landing after the watermark
// was stamped bumps updated_at past it, so the next sweep re-emits with the
// fresh counts (the debounce never drops an update).
func TestRefresherConcurrentUpdateRedirties(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	emitter := &fakeEmitter{}
	r := testRefresher(t, database, emitter)
	r.Sweep(ctx)
	require.Len(t, emitter.snapshot(), 1)

	// A new vote lands (updated_at moves past the watermark).
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 2), voterBob, subjectPost), ""))
	r.Sweep(ctx)

	calls := emitter.snapshot()
	require.Len(t, calls, 2, "the re-dirtied row is emitted again")
	assert.Equal(t, 2, calls[1].up, "the re-emit carries the fresh count")
}

// TestRefresherGuardedAdvanceKeepsMidSweepVoteDue is the watermark-strand
// regression: a vote that commits BETWEEN the sweep's read of a due row and the
// sweep's watermark advance must keep the row due. The emitter fn injects that
// vote — a real ApplyVote whose recompute stamps updated_at via clock_timestamp
// (strictly newer than the value the sweep read). The advance is guarded
// (`AND updated_at = $2`), so it matches no row and does NOT stamp the stale
// watermark; the row stays due and the next sweep re-emits the fresh counts.
// Without the guard the advance would stamp the stale updated_at — and with the
// pre-fix CURRENT_TIMESTAMP recompute (transaction-start time) the concurrent
// vote could even regress updated_at below that watermark and be lost forever.
func TestRefresherGuardedAdvanceKeepsMidSweepVoteDue(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	before, _, found := aggregateTimes(t, database, subjectPost)
	require.True(t, found)

	injected := false
	emitter := &fakeEmitter{fn: func(_ *store.APObjectMapping, _, _ int) (bool, error) {
		// Simulate a vote landing mid-sweep: after the sweep read this row but
		// before it advances the watermark. Exactly once, or the second sweep's
		// emit would inject again.
		if !injected {
			injected = true
			require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 2), voterBob, subjectPost), ""))
		}
		return true, nil
	}}
	r := testRefresher(t, database, emitter)
	r.Sweep(ctx)

	after, emitted, _ := aggregateTimes(t, database, subjectPost)
	assert.True(t, after.After(before), "clock_timestamp recompute must strictly advance updated_at per subject")
	assert.False(t, emitted.Valid,
		"the guarded advance must not stamp a watermark the mid-sweep vote already superseded — the row stays due")

	// The next sweep re-emits with the fresh counts (the concurrent vote is not lost).
	r.Sweep(ctx)
	calls := emitter.snapshot()
	require.Len(t, calls, 2, "the still-due row is swept again")
	assert.Equal(t, 2, calls[1].up, "the re-sweep carries the mid-sweep vote's count")
	_, emitted2, _ := aggregateTimes(t, database, subjectPost)
	assert.True(t, emitted2.Valid, "with no further votes, the second sweep advances the watermark")
}

// TestRefresherUnchangedCountsAdvancesWithoutCommit: when the emitter reports
// NoOp (the record already carried these counts, only asOf would move), the
// refresher still advances the watermark — the row must not be re-swept every
// interval forever.
func TestRefresherUnchangedCountsAdvancesWithoutCommit(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))

	emitter := &fakeEmitter{fn: func(_ *store.APObjectMapping, _, _ int) (bool, error) {
		return false, nil // NoOp: no commit happened
	}}
	r := testRefresher(t, database, emitter)
	r.Sweep(ctx)
	require.Len(t, emitter.snapshot(), 1)

	_, emitted, _ := aggregateTimes(t, database, subjectPost)
	require.True(t, emitted.Valid, "a NoOp emit still advances the watermark")

	r.Sweep(ctx)
	assert.Len(t, emitter.snapshot(), 1, "a NoOp-emitted row is not re-swept")
}

// TestRefresherDeletedMappingSkippedAndAdvanced: a subject whose mapping is
// soft-deleted (content deleted after the vote) is never handed to the
// emitter, and its watermark is advanced so it stops being reconsidered.
func TestRefresherDeletedMappingSkippedAndAdvanced(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	require.NoError(t, objects.SoftDelete(ctx, subjectPost))

	emitter := &fakeEmitter{}
	r := testRefresher(t, database, emitter)
	r.Sweep(ctx)

	assert.Empty(t, emitter.snapshot(), "a soft-deleted subject is never emitted")
	_, emitted, _ := aggregateTimes(t, database, subjectPost)
	assert.True(t, emitted.Valid, "the deleted-mapping row is advanced past")
}

// TestRefresherMissingMappingAdvanced: an aggregate whose ap_objects row was
// hard-removed (no mapping at all) is advanced past, not retried forever.
func TestRefresherMissingMappingAdvanced(t *testing.T) {
	database := testDB(t)
	testAggregator(t, database) // migrate + truncate
	ctx := context.Background()

	// A bare aggregate with no backing mapping.
	orphan := "https://lemmy.world/post/orphan"
	_, err := database.ExecContext(ctx, `
		INSERT INTO vote_aggregates (subject_ap_id, subject_at_uri, upvotes, downvotes)
		VALUES ($1, $2, 3, 1)`, orphan, "at://"+testDID+"/"+testCollection+"/3jzorphanzzza")
	require.NoError(t, err)

	emitter := &fakeEmitter{}
	r := testRefresher(t, database, emitter)
	r.Sweep(ctx)

	assert.Empty(t, emitter.snapshot(), "no mapping means nothing to stamp")
	_, emitted, _ := aggregateTimes(t, database, orphan)
	assert.True(t, emitted.Valid, "an orphan aggregate is advanced past")
}

// TestRefresherTransientErrorLeavesDirty: a transient emit error does NOT
// advance the watermark, so the next sweep retries the still-dirty row.
func TestRefresherTransientErrorLeavesDirty(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))

	fail := true
	emitter := &fakeEmitter{fn: func(_ *store.APObjectMapping, _, _ int) (bool, error) {
		if fail {
			return false, stderrors.New("db connection reset")
		}
		return true, nil
	}}
	r := testRefresher(t, database, emitter)
	r.Sweep(ctx)

	_, emitted, _ := aggregateTimes(t, database, subjectPost)
	assert.False(t, emitted.Valid, "a transient failure leaves the watermark unset (still dirty)")

	// The next sweep retries; this time it succeeds and advances.
	fail = false
	r.Sweep(ctx)
	assert.Len(t, emitter.snapshot(), 2, "the still-dirty row is retried")
	_, emitted, _ = aggregateTimes(t, database, subjectPost)
	assert.True(t, emitted.Valid, "the successful retry advances the watermark")
}

// TestRefresherTombstonedRepoAdvances: a consent-frozen repo rejects the
// commit with errors.IsTombstoned — a PERMANENT failure the refresher advances
// past (never retries forever).
func TestRefresherTombstonedRepoAdvances(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))

	emitter := &fakeEmitter{fn: func(_ *store.APObjectMapping, _, _ int) (bool, error) {
		return false, errors.NewTombstonedError("repo", subjectPost)
	}}
	r := testRefresher(t, database, emitter)
	r.Sweep(ctx)

	_, emitted, _ := aggregateTimes(t, database, subjectPost)
	assert.True(t, emitted.Valid, "a tombstoned repo is a permanent skip: advance the watermark")
	r.Sweep(ctx)
	assert.Len(t, emitter.snapshot(), 1, "a tombstoned row is not re-swept")
}

// TestRefresherRecordGoneDuringEmitAdvances: EmitBridgedStats returning the
// record-gone sentinel (record deleted between the mapping read and the commit)
// is a permanent skip.
func TestRefresherRecordGoneDuringEmitAdvances(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))

	emitter := &fakeEmitter{fn: func(_ *store.APObjectMapping, _, _ int) (bool, error) {
		return false, errors.ErrRecordGone
	}}
	r := testRefresher(t, database, emitter)
	r.Sweep(ctx)

	_, emitted, _ := aggregateTimes(t, database, subjectPost)
	assert.True(t, emitted.Valid, "a vanished record is a permanent skip")
}

// TestRefresherPlainNotFoundStaysTransient: a bare NotFound from the emit path
// (NOT the record-gone sentinel) — e.g. a missing bridged_actor row or signing
// key surfaced from deep in the commit — must NOT advance the watermark. Such a
// key-escrow inconsistency is transient: silently advancing every watermark it
// touched would drop those subjects' counts.
func TestRefresherPlainNotFoundStaysTransient(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))

	emitter := &fakeEmitter{fn: func(_ *store.APObjectMapping, _, _ int) (bool, error) {
		return false, errors.NewNotFoundError("signing_key", subjectPost)
	}}
	r := testRefresher(t, database, emitter)
	r.Sweep(ctx)

	_, emitted, _ := aggregateTimes(t, database, subjectPost)
	assert.False(t, emitted.Valid, "a non-record-gone NotFound must leave the row dirty for retry")
}

// TestRefresherValidationErrorAdvances: a persistently invalid record (lexicon
// validation failure in strict mode) must not wedge the sweep at the head of
// the batch — the refresher advances past it loudly.
func TestRefresherValidationErrorAdvances(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))

	emitter := &fakeEmitter{fn: func(_ *store.APObjectMapping, _, _ int) (bool, error) {
		return false, errors.NewValidationError("record", "fails lexicon validation")
	}}
	r := testRefresher(t, database, emitter)
	r.Sweep(ctx)

	_, emitted, _ := aggregateTimes(t, database, subjectPost)
	assert.True(t, emitted.Valid, "a persistently invalid record is advanced past, not retried forever")
	r.Sweep(ctx)
	assert.Len(t, emitter.snapshot(), 1, "a validation-wedged row is not re-swept")
}

// TestRefresherSeededOnlySubjectEmits: a subject with only a seeded baseline
// (no live vote events) is still due and still emitted — the counts came from
// the origin's public API, not the firehose, but they must still ride the
// record.
func TestRefresherSeededOnlySubjectEmits(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	require.NoError(t, agg.SeedAggregates(ctx, subjectPost, 25, 4))

	emitter := &fakeEmitter{}
	r := testRefresher(t, database, emitter)
	r.Sweep(ctx)

	calls := emitter.snapshot()
	require.Len(t, calls, 1, "a seeded-only subject is emitted")
	assert.Equal(t, 25, calls[0].up)
	assert.Equal(t, 4, calls[0].down)
}

// TestRefresherBatchBounded: a sweep emits at most `batch` rows; the rest wait
// for the next sweep (commits are globally serialized — a sweep must not flood
// the lock).
func TestRefresherBatchBounded(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		subject := subjectPost + "/" + string(rune('a'+i))
		bridgeSubject(t, objects, subject, "3jzfcijpj2z2"+string(rune('a'+i)))
		require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, i), voterAlice, subject), ""))
	}

	emitter := &fakeEmitter{}
	r, err := NewRefresher(database, objects, emitter, time.Second, 2, slog.Default())
	require.NoError(t, err)

	r.Sweep(ctx)
	assert.Len(t, emitter.snapshot(), 2, "one sweep emits at most `batch` rows")
	r.Sweep(ctx)
	assert.Len(t, emitter.snapshot(), 4, "the next sweep picks up the next batch")
	r.Sweep(ctx)
	assert.Len(t, emitter.snapshot(), 5, "and the remainder")
}
