package consume

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"hash/fnv"
	"log/slog"
)

// The #account ordering guard (second-opinion C4). #account frames are UNGATED
// by the rev gate — they carry no rev — so their monotonic per-DID seq is the
// only thing standing between a reconnect rewind and a silently regressed
// account state. ap_actors.last_account_seq (migration 019) holds the
// high-water mark per actor, and this file is the CLAIM that brackets the
// handler, exactly like applyGatedTx brackets a commit.
//
// The seq lives on ap_actors because the gate only ever runs for a DID that
// already has an actor: the actor-existence check precedes it, so an actorless
// account event is skipped before any seq is consulted or recorded.
//
// WHY A CLAIM AND NOT A READ. main runs connector.Start and the DLQ redriver as
// two goroutines over ONE Dispatcher, so a redriven frame and a live frame for
// the same DID are handled concurrently. Read-decide-write-advance as four
// autocommit statements loses both races that matters here: two racers read the
// same watermark, both pass the gate, and the STALE one's write can land last
// (a reactivated user silently left paused until the next live frame, which may
// be days away) — or, on status="deleted", both reach the terminal, irreversible
// seam and two Delete{Person} activities go out for one deletion.

// accountClaimNamespace scopes this package's advisory locks. The two-argument
// advisory lock functions take (namespace, key) as a pair of int32s and occupy a
// lock space DISTINCT from the one-argument int64 form — which is what keeps
// this from colliding with internal/repo's commit lock or testutil's
// process-wide harness lock, both of which use the one-argument form.
const accountClaimNamespace = 0x6163 // "ac"

// accountClaimKey is the per-DID half of the advisory lock key.
//
// Hashed in GO rather than with postgres's hashtext(): that function is
// undocumented and its algorithm has changed across major versions, and a lock
// key is not something to inherit from an implementation detail. FNV-1a is
// fixed here forever, which is all a lock key has to be.
//
// A COLLISION IS SAFE, not merely unlikely: two DIDs sharing a key serialize
// against each other, which costs a little contention on the rarest event kind
// the consumer handles and changes no outcome. Correctness needs only that one
// DID always maps to one key.
func accountClaimKey(did string) int32 {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(did)) // hash.Write never returns an error
	return int32(hash.Sum32())     //nolint:gosec // wraparound is fine: this is a lock key, not a value
}

// errAccountUnapplied is the sentinel an apply function returns to ABANDON its
// claim without failing the event: the transition did not happen, so the seq
// must not advance, but there is nothing to retry either (an unwired terminal
// tier, an actor that vanished mid-flight). The claim rolls back and the event
// is reported handled, which leaves it replayable — the rollback is the whole
// point, and it must not be mistaken for a storage failure.
var errAccountUnapplied = stderrors.New("account event left unapplied")

// applyAccountClaimed runs apply under an exclusive per-DID claim on the
// #account seq, and advances the seq only if apply succeeds. It is
// applyGatedTx's shape — claim first, apply under it, commit last — with ONE
// deliberate divergence, and the divergence is the whole reason this is a
// separate function rather than a call into the rev gate:
//
// THE CLAIM IS AN ADVISORY LOCK, NOT THE ap_actors ROW LOCK. The obvious
// implementation is `UPDATE ap_actors SET last_account_seq = $2 WHERE did = $1
// AND last_account_seq < $2` as the first statement, holding that row's lock
// across apply. It deadlocks. The deleted path's apply calls the terminal tier,
// which takes NO transaction and opens its own — and the destructive tier it
// reaches (outbound.Purger) tombstones the very ap_actors row this transaction
// would be holding. That is exactly the hazard rev_gate.go's DEADLOCK NOTE
// names, and the reason handleFederation defers its own ap_actors write until
// after the destructive seam returns: this transaction cannot release what it
// holds without committing, and it is synchronously waiting on the call that
// needs it.
//
// An advisory lock excludes the other #account handlers — the only writers of
// last_account_seq — while contending with nothing the terminal tier touches.
// The row is written LAST, so its lock is held for the commit and not a
// microsecond longer.
//
// THE CLAIM IS HELD ACROSS THE TERMINAL TIER'S NETWORK PROBE, on purpose. That
// tier confirms the deletion against PLC and the PDS before sending anything,
// and the alternative shapes are worse: releasing the claim around the probe
// re-opens precisely the window in which two racers both confirm and both send
// Delete{Person}, and claiming BEFORE the probe (advancing the seq, then
// probing) consumes the event so a failed confirm can never be retried — the
// case account_confirm_test.go pins as the reason the seam exists. The cost is a
// pooled connection held for the length of a bounded HTTP round trip on the
// rarest event kind the consumer handles.
//
// A claim LOSS returns nil: the event is a duplicate or a stale copy, fully
// accounted for, and the cursor must advance past it.
func (d *Dispatcher) applyAccountClaimed(ctx context.Context, did string, seq int64, apply func(tx *sql.Tx) error) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin account claim for %s: %w", did, err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !stderrors.Is(rollbackErr, sql.ErrTxDone) {
			d.logger.Error("failed to roll back account claim",
				slog.String("did", did), slog.String("error", rollbackErr.Error()))
		}
	}()

	// FIRST STATEMENT. Everything below — the watermark read, the decision, the
	// transition, the advance — happens with every other #account handler for
	// this DID shut out, which is what makes the read-then-write below a claim
	// rather than a guess.
	if _, err := tx.ExecContext(ctx,
		`SELECT pg_advisory_xact_lock($1, $2)`,
		accountClaimNamespace, accountClaimKey(did)); err != nil {
		return fmt.Errorf("claim account seq for %s: %w", did, err)
	}

	var lastSeq int64
	err = tx.QueryRowContext(ctx,
		`SELECT last_account_seq FROM ap_actors WHERE did = $1`, did).Scan(&lastSeq)
	if stderrors.Is(err, sql.ErrNoRows) {
		// The actor vanished between the existence check and the claim. Nothing
		// to pause and nothing to withdraw; no seq is recorded, so a redelivery
		// still applies if the actor is minted again.
		d.logger.Debug("account claim found no actor", slog.String("did", did))
		return nil
	}
	if err != nil {
		return fmt.Errorf("read account seq for %s: %w", did, err)
	}
	if seq <= lastSeq {
		d.logger.Debug("skipping stale or duplicate #account",
			slog.String("did", did), slog.Int64("seq", seq), slog.Int64("last_seq", lastSeq))
		return nil
	}

	if err := apply(tx); err != nil {
		if stderrors.Is(err, errAccountUnapplied) {
			// Deliberately un-advanced: the deferred rollback releases the claim
			// with the watermark where it was, so the event can still be applied
			// by a build (or a moment) that can act on it.
			return nil
		}
		return err // the deferred rollback releases the claim un-advanced
	}

	// The advance is the LAST write, and it keeps its own monotonic WHERE. The
	// claim already guarantees no concurrent advance; the predicate costs
	// nothing and keeps the statement correct on its own terms.
	if _, err := tx.ExecContext(ctx, `
		UPDATE ap_actors SET last_account_seq = $2, updated_at = now()
		WHERE did = $1 AND last_account_seq < $2`, did, seq); err != nil {
		return fmt.Errorf("advance account seq for %s: %w", did, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit account claim for %s: %w", did, err)
	}
	return nil
}

// setActorPausedTx writes the transient #account state ON THE CLAIM's
// transaction, so the pause and the seq advance land together or not at all.
//
// It is raw SQL here rather than a store.APActors method for the same reason
// the seq itself is: this is the consume package's own gate surface, the
// statement is the mirror of store's SetPaused (delivery_paused alone — pausing
// delivery must never disable the actor), and the claim needs it on a
// transaction the store's interface does not offer one for.
//
// A missing actor reports errAccountUnapplied: the row disappeared under us, so
// nothing was applied and the seq must not move.
func setActorPausedTx(ctx context.Context, tx *sql.Tx, did string, paused bool) error {
	result, err := tx.ExecContext(ctx,
		`UPDATE ap_actors SET delivery_paused = $2, updated_at = now() WHERE did = $1`,
		did, paused)
	if err != nil {
		return fmt.Errorf("set delivery paused for %s: %w", did, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("set delivery paused for %s: rows affected: %w", did, err)
	}
	if affected == 0 {
		return fmt.Errorf("%w: no actor for %s", errAccountUnapplied, did)
	}
	return nil
}
