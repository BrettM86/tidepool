package votes

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// Refresher is the debounced sweeper that keeps each bridged post/comment
// record's bridgedStats field in step with its vote_aggregates row (FOLLOWUPS
// "Votes-as-records", locked decision 7 final direction: bridged counts ride
// the CONTENT record, not per-vote records). Votes maintain aggregate counts
// synchronously (Aggregator); this asynchronously folds those counts onto the
// materialized records on an interval, coalescing a burst of votes on one
// subject into at most one record update per sweep.
//
// Watermark model (the stats_emitted_at column, migration 014): a row is DUE
// when a vote has landed since it was last emitted — updated_at >
// stats_emitted_at, or stats_emitted_at IS NULL (never emitted). After
// emitting, the watermark is set to the updated_at value READ during the
// sweep, never now(): a vote landing mid-sweep bumps updated_at past that
// value and re-dirties the row for the next sweep, so no update is lost.
//
// Why a sweeper and not emit-on-vote: commits are globally serialized (the
// repo advisory lock, ~300/s ceiling shared by ALL writes) and each commit is
// one firehose event. A hot post taking a hundred votes a minute must not mint
// a hundred record updates; the sweep collapses them to one, bounded per run.
type Refresher struct {
	db       *sql.DB
	objects  store.APObjects
	emitter  StatsEmitter
	interval time.Duration
	batch    int
	logger   *slog.Logger
}

// StatsEmitter writes an aggregate's counts onto its materialized record and
// reports whether a real commit happened (counts-unchanged is committed=false).
// *materialize.Materializer implements it via EmitBridgedStats. The seam is
// deliberately narrow — a bool, not *materialize.Result — so internal/votes
// need not import internal/materialize just to drive the sweep, and so the
// sweep/watermark logic here is testable against a fake emitter without a full
// repo stack.
type StatsEmitter interface {
	EmitBridgedStats(ctx context.Context, mapping *store.APObjectMapping, upvotes, downvotes int, asOf time.Time) (committed bool, err error)
}

// DefaultRefreshInterval is how often the sweep runs when the caller passes a
// non-positive interval.
const DefaultRefreshInterval = 30 * time.Second

// DefaultRefreshBatch bounds how many due aggregates one sweep emits when the
// caller passes a non-positive batch. Sized so a sweep cannot flood the global
// commit lock: at the benchmarked ~3 ms steady-state commit, 200 updates is
// well under a second of the shared lock even in the worst case where every
// due row actually commits.
const DefaultRefreshBatch = 200

// NewRefresher validates dependencies and builds a Refresher. A non-positive
// interval or batch falls back to the package default (config parsing already
// rejects non-positive values; this is the library-level guard).
func NewRefresher(db *sql.DB, objects store.APObjects, emitter StatsEmitter, interval time.Duration, batch int, logger *slog.Logger) (*Refresher, error) {
	if db == nil {
		return nil, errors.NewValidationError("db", "must not be nil")
	}
	if objects == nil {
		return nil, errors.NewValidationError("objects", "must not be nil")
	}
	if emitter == nil {
		return nil, errors.NewValidationError("emitter", "must not be nil")
	}
	if interval <= 0 {
		interval = DefaultRefreshInterval
	}
	if batch <= 0 {
		batch = DefaultRefreshBatch
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Refresher{db: db, objects: objects, emitter: emitter, interval: interval, batch: batch, logger: logger}, nil
}

// Run sweeps once immediately and then every interval until ctx is cancelled
// (the background-runner shape the pruners and FollowRetrier use).
func (r *Refresher) Run(ctx context.Context) {
	r.Sweep(ctx)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.Sweep(ctx)
		}
	}
}

// dueRow is one aggregate that needs (re-)emitting: a consistent snapshot of
// its counts and the updated_at those counts correspond to (which becomes both
// the record's asOf and, on success/permanent-skip, the new watermark).
type dueRow struct {
	subject   string
	upvotes   int
	downvotes int
	updatedAt time.Time
}

// Sweep emits one bounded batch of due aggregates. Exported for the tests to
// drive one deterministic pass without the ticker.
func (r *Refresher) Sweep(ctx context.Context) {
	// Read the whole batch into memory FIRST, then emit. Each emit runs its own
	// commit transaction taking the global advisory lock; holding this SELECT's
	// cursor (and its connection) open across those commits would pin a
	// connection for the whole sweep.
	//
	// Ordering caveat (oldest-dirty first, ORDER BY updated_at ASC): a
	// TRANSIENT-failing row keeps its old updated_at, so it sorts to the HEAD of
	// every batch and is retried FIRST, not behind fresh work — and ≥batch
	// persistently-failing rows would monopolize the batch and starve emission.
	// That is tolerable ONLY because the two permanent-failure classes advance
	// the watermark out of the due set instead of failing forever: a genuinely
	// gone/frozen subject (record-gone, tombstoned) and a persistently invalid
	// record (lexicon-validation failure — see refreshOne's validation arm).
	// What remains at the head is a truly transient fault (a DB blip), which is
	// the right thing to retry first.
	rows, err := r.db.QueryContext(ctx, `
		SELECT subject_ap_id, upvotes, downvotes, updated_at
		FROM vote_aggregates
		WHERE stats_emitted_at IS NULL OR updated_at > stats_emitted_at
		ORDER BY updated_at ASC
		LIMIT $1`, r.batch)
	if err != nil {
		if ctx.Err() == nil {
			r.logger.Error("stats refresh: select due aggregates failed", "error", err)
		}
		return
	}
	var due []dueRow
	for rows.Next() {
		var d dueRow
		if err := rows.Scan(&d.subject, &d.upvotes, &d.downvotes, &d.updatedAt); err != nil {
			_ = rows.Close()
			r.logger.Error("stats refresh: scan due aggregate failed", "error", err)
			return
		}
		due = append(due, d)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		if ctx.Err() == nil {
			r.logger.Error("stats refresh: iterate due aggregates failed", "error", err)
		}
		return
	}
	_ = rows.Close()

	var emitted, failed int
	for _, d := range due {
		if ctx.Err() != nil {
			return
		}
		committed, transientFail := r.refreshOne(ctx, d)
		if committed {
			emitted++
		}
		if transientFail {
			failed++
		}
	}
	// Summarize whenever anything committed OR anything failed transiently — the
	// failed count is how a wedged sweep (a batch head jammed by failing rows)
	// becomes visible instead of silently emitting nothing every interval.
	if emitted > 0 || failed > 0 {
		r.logger.Info("bridged vote stats sweep", "emitted", emitted, "failed", failed, "due", len(due))
	}
}

// refreshOne emits one due row. It returns committed (a record commit actually
// happened, for the emitted count) and transientFail (a non-permanent error
// that left the row dirty, for the sweep's failed count and wedge visibility).
//
// Watermark discipline: advance on success AND on every PERMANENT skip
// (missing/soft-deleted mapping, record gone, consent-frozen repo,
// persistently invalid record) so those rows stop being reconsidered every
// sweep; do NOT advance on a transient error (a DB hiccup) so the next sweep
// retries with the row still dirty. Every advance-on-skip logs its reason at
// Debug (precedent: the aggregator's vote-drop debug logs) so a silently
// skipped subject is still traceable.
func (r *Refresher) refreshOne(ctx context.Context, d dueRow) (committed, transientFail bool) {
	mapping, err := r.objects.GetByAPID(ctx, d.subject)
	switch {
	case err == nil:
	case errors.IsNotFound(err):
		// An aggregate implies its subject was mapped once; a missing mapping
		// means the ap_objects row was hard-removed. Nothing to stamp, and it
		// will never come back under this id — advance so we stop looking.
		r.logger.Debug("stats refresh: skip, mapping hard-removed; advancing watermark", "subject", d.subject)
		r.advance(ctx, d.subject, d.updatedAt)
		return false, false
	default:
		r.logger.Error("stats refresh: resolve subject failed", "subject", d.subject, "error", err)
		return false, true // transient: leave dirty, retry next sweep
	}
	if mapping.IsDeleted() {
		// Content deleted after the vote landed (unordered AP delivery). The
		// record is gone; its asserted counts are moot. Permanent: advance.
		r.logger.Debug("stats refresh: skip, mapping soft-deleted; advancing watermark", "subject", d.subject, "at_uri", mapping.ATURI)
		r.advance(ctx, d.subject, d.updatedAt)
		return false, false
	}

	committed, err = r.emitter.EmitBridgedStats(ctx, mapping, d.upvotes, d.downvotes, d.updatedAt)
	switch {
	case err == nil:
		r.advance(ctx, d.subject, d.updatedAt)
		return committed, false
	case errors.IsRecordGone(err):
		// The record was deleted out from under its aggregate (or its mapping
		// soft-deleted inside the commit). A DISTINCT sentinel, not a bare
		// NotFound: a NotFound raised deeper in the commit (missing
		// bridged_actor row or signing key) is a key-escrow inconsistency to
		// retry, and must NOT advance every watermark it touches.
		r.logger.Debug("stats refresh: skip, record gone; advancing watermark", "subject", d.subject, "at_uri", mapping.ATURI)
		r.advance(ctx, d.subject, d.updatedAt)
		return false, false
	case errors.IsTombstoned(err):
		// Repo frozen by a consent revocation: the commit's signing-key gate
		// refuses writes to a tombstoned actor, and always will. Permanent:
		// advance so a frozen actor's stale row stops being swept forever.
		r.logger.Debug("stats refresh: skip, repo consent-frozen; advancing watermark", "subject", d.subject, "at_uri", mapping.ATURI)
		r.advance(ctx, d.subject, d.updatedAt)
		return false, false
	case errors.IsValidation(err):
		// The record fails lexicon validation (strict mode). It keeps its old
		// updated_at and would sort to the HEAD of every batch, wedging the
		// sweep for every row behind it — so advance past it, loudly. A
		// persistently invalid record is a bug to investigate, not a reason to
		// stop emitting everyone else's counts.
		r.logger.Warn("stats refresh: record fails lexicon validation; advancing watermark to avoid wedging the sweep (investigate)",
			"subject", d.subject, "at_uri", mapping.ATURI, "error", err)
		r.advance(ctx, d.subject, d.updatedAt)
		return false, false
	default:
		// Transient (a DB blip, a lock timeout, persistent CAS churn): keep the
		// row dirty so the next sweep retries it.
		r.logger.Error("stats refresh: emit failed", "subject", d.subject, "at_uri", mapping.ATURI, "error", err)
		return false, true
	}
}

// advance stamps the watermark with the updated_at READ during the sweep
// (never now()). The `AND updated_at = $2` guard is load-bearing: if a
// concurrent recompute bumped updated_at after this sweep read it — which
// clock_timestamp() in recomputeAggregate makes strictly newer, per subject —
// the UPDATE matches no row, the watermark does NOT advance, and
// `updated_at > stats_emitted_at` keeps the row due for the next sweep. Without
// the guard, advancing to the stale value could still leave a just-committed
// vote below the (older, transaction-start CURRENT_TIMESTAMP) watermark and
// never re-emit it. The row is debounced, never dropped.
func (r *Refresher) advance(ctx context.Context, subject string, watermark time.Time) {
	if _, err := r.db.ExecContext(ctx,
		`UPDATE vote_aggregates SET stats_emitted_at = $2 WHERE subject_ap_id = $1 AND updated_at = $2`,
		subject, watermark); err != nil && ctx.Err() == nil {
		// A failed watermark advance is self-healing: the row stays due and the
		// next sweep re-emits (an idempotent no-op if the record already carries
		// the counts) and re-advances. Log, don't wedge the sweep.
		r.logger.Error("stats refresh: advance watermark failed", "subject", subject, "error", err)
	}
}
