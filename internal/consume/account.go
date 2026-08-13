package consume

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
)

// The #account ordering guard (second-opinion C4). #account frames are UNGATED
// by the rev gate — they carry no rev — so their monotonic per-DID seq is the
// only thing standing between a reconnect rewind and a silently regressed
// account state. ap_actors.last_account_seq (migration 019) holds the
// high-water mark per actor, and these two helpers are the read and the
// monotonic advance that bracket the handler, exactly like jetstream_record_revs
// brackets a commit.
//
// The seq lives on ap_actors because the gate only ever runs for a DID that
// already has an actor: the actor-existence check precedes it, so an actorless
// account event is skipped before any seq is consulted or recorded.

// lastAccountSeq returns the highest #account seq applied for the DID's actor.
// A DID with no actor row reports 0 — but the caller reaches this only after
// confirming the actor exists, so in practice the row is always present.
func (d *Dispatcher) lastAccountSeq(ctx context.Context, did string) (int64, error) {
	var seq int64
	err := d.db.QueryRowContext(ctx,
		`SELECT last_account_seq FROM ap_actors WHERE did = $1`, did).Scan(&seq)
	if stderrors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read account seq for %s: %w", did, err)
	}
	return seq, nil
}

// advanceAccountSeq records seq as the last applied #account seq for the DID's
// actor. The WHERE clause keeps it MONOTONIC: a late or out-of-order write can
// only ever raise the mark, never lower it, so an advance that races a newer
// one cannot reopen the door to a stale replay.
func (d *Dispatcher) advanceAccountSeq(ctx context.Context, did string, seq int64) error {
	_, err := d.db.ExecContext(ctx, `
		UPDATE ap_actors SET last_account_seq = $2, updated_at = now()
		WHERE did = $1 AND last_account_seq < $2`,
		did, seq)
	if err != nil {
		return fmt.Errorf("advance account seq for %s: %w", did, err)
	}
	return nil
}
