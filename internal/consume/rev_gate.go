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

// GateResetRequiredOnSchemaBump records the C6 coupling (second-opinion): the
// gate (jetstream_record_revs) is keyed by record_uri alone, with no schema
// version, so a CursorSchemaVersion bump that expects a from-scratch replay
// must first reset the gate and the DLQ — otherwise every record reads as
// already-applied and the replay is silently a no-op.
//
// OPERATIONAL REQUIREMENT (the machine-readable half of the ruling in
// consume.CursorSchemaVersion's doc): whoever bumps CursorSchemaVersion to
// force a clean replay MUST, in the same deploy, TRUNCATE jetstream_record_revs
// and jetstream_dead_letters. Skip it and the new handlers process nothing —
// the versioned cursor's promised replay is a no-op. The better long-term fix
// is a schema-scoped gate (a schema_version column threaded through every gate
// call, plus a migration); it is deferred until a real v2 lands, and doing it
// is what would let this flag flip back to false.
const GateResetRequiredOnSchemaBump = true

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

// RevGate carries the gate's own DB handle. applyGated/applyGatedTx use it to
// open the claim transaction held across apply, which is how EVERY commit
// handler is currently gated.
//
// IsStale/Advance expose a non-transactional check→write→advance flavor for
// FUTURE non-commit paths whose write is an idempotent last-write-wins update
// and can tolerate a brief unguarded window. Nothing in production uses them
// today — every commit handler runs under applyGated — so they exist only as
// the exported seam (and are exercised by the rev_gate tests). A nil *RevGate
// disables gating entirely.
type RevGate struct {
	db *sql.DB
}

// NewRevGate creates a rev gate backed by the bridge database.
func NewRevGate(db *sql.DB) *RevGate {
	return &RevGate{db: db}
}

// IsStale reports whether the event's rev is superseded by the stored rev for
// the record URI. Nil-safe: a nil gate never reports stale.
//
// Part of the non-transactional seam described on RevGate: currently unused by
// production code (all commit handling goes through applyGated), retained for
// future non-commit paths.
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
//
// Part of the non-transactional seam described on RevGate: currently unused by
// production code (all commit handling goes through applyGated), retained for
// future non-commit paths.
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
// DEADLOCK NOTE, and it is no longer as simple as it once was. The gate
// transaction originally touched ONLY jetstream_record_revs, which made the
// gate row lock a pure per-record mutex around apply. applyGatedTx now hands
// that transaction to handlers so their durable state commits with the gate
// advance, and they use it: the comment path writes outbound_objects on it, and
// the federation path writes outbound_deliveries and ap_actors.
//
// The rule that replaces the old proof: A HANDLER WRITING ON THIS TRANSACTION
// MUST NOT THEN CALL SOMETHING THAT OPENS A SECOND TRANSACTION TOUCHING THE
// SAME ROWS. It cannot release what it holds without committing, and the
// handler is synchronously waiting, so the two deadlock until a timeout. The
// destructive opt-out tier is exactly that shape — the seam takes no
// transaction and opens its own — which is why handleFederation defers its
// ap_actors write until after that call.
//
// An apply error — or a panic, which the deferred rollback covers equally —
// releases the claim WITHOUT advancing, so the connector's retry/redrive
// replays the event instead of losing it behind its own gate entry.
//
// A gate SKIP returns nil, not an error: the event is fully accounted for and
// the cursor must advance past it.
func applyGated(ctx context.Context, gate *RevGate, consumer, did string, commit *CommitEvent, apply func() error) error {
	return applyGatedTx(ctx, gate, consumer, did, commit, func(*sql.Tx) error { return apply() })
}

// applyGatedTx is applyGated with the claim's transaction handed to apply, so a
// handler can write its durable outbound state ON that transaction
// (UpsertTx/TombstoneTx). The state write, the gate advance and the enqueue
// then commit as ONE unit: a failed enqueue returns an error and the deferred
// rollback releases the row AND the gate advance together.
//
// This is what closes the C2 split. Without it the handler wrote outbound state
// on its own autocommit connection, then the gate advanced, then the enqueue
// ran — so a failed enqueue left a committed row behind an unadvanced gate, and
// a replay would bump the seq off that phantom base into a SECOND, distinct
// activity id for one operation, which a peer sees as two Creates for one
// comment.
//
// The tx passed to apply is nil only when the gate is bypassed (nil gate or an
// empty rev — synthetic test events); a handler that writes state on nil would
// get a validation error from UpsertTx, which is the correct refusal for a
// path that has opted out of gating.
func applyGatedTx(ctx context.Context, gate *RevGate, consumer, did string, commit *CommitEvent, apply func(tx *sql.Tx) error) error {
	if gate == nil || commit.Rev == "" {
		return apply(nil)
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
	if err := apply(tx); err != nil {
		return err // the deferred rollback releases the claim un-advanced
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit rev-gate transaction for %s: %w", uri, err)
	}
	return nil
}
