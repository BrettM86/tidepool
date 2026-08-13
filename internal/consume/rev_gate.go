package consume

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"log/slog"
)

// This file is the rev gate: the ordering guard that makes the whole pipeline
// replay-safe. It is a PORT of the Coves AppView's rev_gate.go.
//
// WHY IT EXISTS: cursor rewinds on reconnect deliberately replay a few seconds
// of events, and a cursor behind Jetstream's retention replays the entire
// store. Stable activity ids CANNOT substitute for this — a peer has no way to
// reject a Create for an id it has never seen, so a replay past a delete would
// resurrect content the user removed.
//
// THE MECHANISM: every commit event carries `rev`, the repo's monotonic TID —
// a fixed-length base32-sortable string, so plain lexicographic comparison IS
// commit order within one repo. The last APPLIED rev per record URI lives in
// jetstream_record_revs, and an incoming create/update/delete applies only
// when its rev is strictly greater. Equal rev = the same event replayed (a
// no-op, which subsumes duplicate handling); smaller rev = a stale copy
// (skipped). The gate row SURVIVES deletes, so it doubles as the tombstone
// that rejects the stale create.
//
// Events with an empty rev bypass the gate entirely. In practice only
// synthetic events are rev-less: real Jetstream frames always carry rev, and
// the DLQ stores the raw frame, so redriven events keep theirs.

// revGateQuerier is the subset of *sql.DB / *sql.Tx the gate needs, so the
// same statements run standalone or inside a caller's transaction.
type revGateQuerier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// tryAdvanceRecordRev atomically claims rev for the record URI. It reports
// true when the event WINS (no gate row yet, or a strictly greater rev) and
// the gate row was written; false when the stored rev is greater or equal — a
// duplicate replay or a stale copy the caller must skip. An empty rev bypasses
// the gate (true, without writing).
func tryAdvanceRecordRev(ctx context.Context, q revGateQuerier, uri, rev string) (bool, error) {
	if rev == "" {
		// No row is written on purpose: an empty rev compares below every real
		// TID, so storing it would gate the record at the bottom forever.
		return true, nil
	}
	result, err := q.ExecContext(ctx, `
		INSERT INTO jetstream_record_revs (record_uri, rev)
		VALUES ($1, $2)
		ON CONFLICT (record_uri) DO UPDATE
		SET rev = EXCLUDED.rev, updated_at = now()
		WHERE jetstream_record_revs.rev < EXCLUDED.rev`,
		uri, rev)
	if err != nil {
		return false, fmt.Errorf("advance record rev for %s: %w", uri, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("check record rev advance for %s: %w", uri, err)
	}
	return rows > 0, nil
}

// recordRevIsStale reports whether the stored rev for the record URI is
// greater than or equal to the incoming one (i.e. the event must be skipped).
// No gate row, or an empty incoming rev, means not stale.
func recordRevIsStale(ctx context.Context, q revGateQuerier, uri, rev string) (bool, error) {
	if rev == "" {
		return false, nil
	}
	var stale bool
	err := q.QueryRowContext(ctx,
		`SELECT rev >= $2 FROM jetstream_record_revs WHERE record_uri = $1`, uri, rev,
	).Scan(&stale)
	if stderrors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check record rev for %s: %w", uri, err)
	}
	return stale, nil
}

// commitRecordURI builds the AT-URI of the record a commit event addresses —
// the gate's key.
func commitRecordURI(did string, commit *CommitEvent) string {
	return fmt.Sprintf("at://%s/%s/%s", did, commit.Collection, commit.RKey)
}

// logSkippedStaleRev is the single, grep-able line for gate skips. A rejected
// stale event is the system WORKING, so it is logged at debug and phrased so
// it cannot be mistaken for trouble.
func logSkippedStaleRev(consumer, operation, uri, rev string) {
	slog.Debug("rev-gate skipped stale event",
		slog.String("consumer", consumer),
		slog.String("operation", operation),
		slog.String("uri", uri),
		slog.String("rev", rev))
}

// RevGate carries the gate's own DB handle. applyGated uses it to open the
// claim transaction held across apply; IsStale/Advance expose the
// non-transactional check→write→advance flavor for paths whose write is an
// idempotent last-write-wins update (the profile cache), where a brief
// unguarded window is acceptable. A nil *RevGate disables gating entirely.
type RevGate struct {
	db *sql.DB
}

// NewRevGate creates a rev gate backed by the bridge database.
func NewRevGate(db *sql.DB) *RevGate {
	return &RevGate{db: db}
}

// IsStale reports whether the event's rev is superseded by the stored rev for
// the record URI. Nil-safe: a nil gate never reports stale.
func (g *RevGate) IsStale(ctx context.Context, uri, rev string) (bool, error) {
	if g == nil {
		return false, nil
	}
	return recordRevIsStale(ctx, g.db, uri, rev)
}

// Advance records rev as the last applied rev for the record URI, keeping
// whichever is greater — a late Advance for an older event must not lower the
// gate. Called AFTER the write succeeds, so a failure in between replays the
// event instead of losing it. Nil-safe: a nil gate is a no-op.
func (g *RevGate) Advance(ctx context.Context, uri, rev string) error {
	if g == nil {
		return nil
	}
	_, err := tryAdvanceRecordRev(ctx, g.db, uri, rev)
	return err
}

// applyGated runs apply under a TRANSACTIONAL rev-gate claim for a commit
// event. The gate row is claimed as the FIRST statement of a transaction on
// the gate's own DB handle, apply() runs while that claim is held, and the
// transaction commits only after apply succeeds. The claim's row lock makes
// two handlers for the SAME record URI serialize for the full duration of
// apply — the loser blocks on the claim, then observes the winner's rev and
// skips — so no check→write window exists for a stale copy to sneak through,
// no matter how long apply takes.
//
// Deadlock note: apply's writes go through repository methods on their own
// connections, which is deliberate and safe — the gate transaction touches
// ONLY jetstream_record_revs, a table no repository write path ever touches,
// so the gate row lock acts as a pure per-record mutex around apply.
//
// An apply error — or a panic, which the deferred rollback covers equally —
// releases the claim WITHOUT advancing, so the connector's retry/redrive
// replays the event instead of losing it behind its own gate entry.
//
// A gate SKIP returns nil, not an error: the event is fully accounted for and
// the cursor must advance past it.
func applyGated(ctx context.Context, gate *RevGate, consumer, did string, commit *CommitEvent, apply func() error) error {
	if gate == nil || commit.Rev == "" {
		return apply()
	}
	uri := commitRecordURI(did, commit)
	tx, err := gate.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin rev-gate transaction for %s: %w", uri, err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !stderrors.Is(rollbackErr, sql.ErrTxDone) {
			slog.Error("failed to roll back rev-gate transaction",
				slog.String("uri", uri), slog.String("error", rollbackErr.Error()))
		}
	}()

	won, err := tryAdvanceRecordRev(ctx, tx, uri, commit.Rev)
	if err != nil {
		return err
	}
	if !won {
		logSkippedStaleRev(consumer, commit.Operation, uri, commit.Rev)
		return nil
	}
	if err := apply(); err != nil {
		return err // the deferred rollback releases the claim un-advanced
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit rev-gate transaction for %s: %w", uri, err)
	}
	return nil
}
