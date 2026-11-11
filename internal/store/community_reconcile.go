package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"time"

	"github.com/lib/pq"

	"tidepool/internal/errors"
)

// Bridged community restore reconcile. A label negation that clears a
// community's active removal records the community's reconcile pending
// (communities.reconcile_pending_at, migration 064) in the label's
// transaction. A reconcile pass clears it, by compare-and-set on the value it
// read at its start, only once every kept object got a final outcome, so a
// newer negation during a pass keeps it pending. The first pass for a marker
// stamps communities.reconcile_first_attempt_at; a new marker and the clear
// both reset it.

// PendingCommunityReconcile is a community whose reconcile is pending, with
// the marker value a pass clears.
type PendingCommunityReconcile struct {
	DID       string
	APGroupID string
	PendingAt time.Time
}

// readyCommunityReconcileCondition is the SQL condition that the communities
// row c has a pending reconcile and is ready for a pass: not removed, its
// follow not pending, and an accepted follow's backfill stamped. A community
// left unfollowed is ready right away.
func readyCommunityReconcileCondition() string {
	return `c.reconcile_pending_at IS NOT NULL
		AND c.follow_state <> '` + string(FollowStatePending) + `'
		AND (c.follow_state <> '` + string(FollowStateAccepted) + `' OR c.last_backfill_at IS NOT NULL)
		AND NOT ` + communityRemovedCondition("c.did")
}

// MarkCommunityReconcilePendingTx records the reconcile of the community
// with did pending, in the caller's transaction.
func MarkCommunityReconcilePendingTx(ctx context.Context, tx *sql.Tx, did string) error {
	if tx == nil {
		return errors.NewValidationError("tx", "must not be nil")
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE communities SET reconcile_pending_at = clock_timestamp(), reconcile_first_attempt_at = NULL
		WHERE did = $1`, did); err != nil {
		return fmt.Errorf("mark reconcile of community %s pending: %w", did, err)
	}
	return nil
}

// ListReadyCommunityReconciles lists every community with a pending reconcile
// that is ready for a pass.
func ListReadyCommunityReconciles(ctx context.Context, db *sql.DB) ([]PendingCommunityReconcile, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT c.did, c.ap_group_id, c.reconcile_pending_at FROM communities c
		WHERE `+readyCommunityReconcileCondition()+`
		ORDER BY c.id`)
	if err != nil {
		return nil, fmt.Errorf("list ready community reconciles: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var pending []PendingCommunityReconcile
	for rows.Next() {
		var p PendingCommunityReconcile
		if err := rows.Scan(&p.DID, &p.APGroupID, &p.PendingAt); err != nil {
			return nil, fmt.Errorf("scan ready community reconcile: %w", err)
		}
		pending = append(pending, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list ready community reconciles: %w", err)
	}
	return pending, nil
}

// ReadyCommunityReconcile reads the pending reconcile of the community with
// did, or nil when none is pending or the community is not ready for a pass.
func ReadyCommunityReconcile(ctx context.Context, db *sql.DB, did string) (*PendingCommunityReconcile, error) {
	var p PendingCommunityReconcile
	err := db.QueryRowContext(ctx, `
		SELECT c.did, c.ap_group_id, c.reconcile_pending_at FROM communities c
		WHERE c.did = $1 AND `+readyCommunityReconcileCondition(), did).
		Scan(&p.DID, &p.APGroupID, &p.PendingAt)
	if stderrors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read reconcile of community %s: %w", did, err)
	}
	return &p, nil
}

// StampCommunityReconcileFirstAttempt records now as the first attempt of the
// pending reconcile of the community with did, unless one is already
// recorded, when its marker still holds pendingAt. It returns the recorded
// first attempt, and false when the marker no longer holds pendingAt.
func StampCommunityReconcileFirstAttempt(ctx context.Context, db *sql.DB, did string, pendingAt time.Time) (time.Time, bool, error) {
	var firstAttemptAt time.Time
	err := db.QueryRowContext(ctx, `
		UPDATE communities
		SET reconcile_first_attempt_at = COALESCE(reconcile_first_attempt_at, clock_timestamp())
		WHERE did = $1 AND reconcile_pending_at = $2
		RETURNING reconcile_first_attempt_at`, did, pendingAt).Scan(&firstAttemptAt)
	if stderrors.Is(err, sql.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("stamp first reconcile attempt of community %s: %w", did, err)
	}
	return firstAttemptAt, true, nil
}

// ClearCommunityReconcile clears the pending reconcile of the community with
// did, and its first attempt, when its marker still holds pendingAt and the
// community is not removed again, and reports whether it did.
func ClearCommunityReconcile(ctx context.Context, db *sql.DB, did string, pendingAt time.Time) (bool, error) {
	result, err := db.ExecContext(ctx, `
		UPDATE communities SET reconcile_pending_at = NULL, reconcile_first_attempt_at = NULL
		WHERE did = $1 AND reconcile_pending_at = $2
			AND NOT `+communityRemovedCondition("communities.did"), did, pendingAt)
	if err != nil {
		return false, fmt.Errorf("clear reconcile of community %s: %w", did, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("clear reconcile of community %s: rows affected: %w", did, err)
	}
	return affected == 1, nil
}

// ListCommunityKeptObjects lists the community's kept objects in the given
// collections: live fediverse mappings bound to communityDID.
func ListCommunityKeptObjects(ctx context.Context, db *sql.DB, communityDID string, collections []string) ([]*APObjectMapping, error) {
	if communityDID == "" {
		return nil, errors.NewValidationError("community_did", "must not be empty")
	}
	rows, err := db.QueryContext(ctx, `SELECT`+apObjectColumns+`
		FROM ap_objects
		WHERE community_did = $1 AND origin = $2 AND deleted_at IS NULL AND collection = ANY($3::text[])
		ORDER BY id`, communityDID, string(OriginFediverse), pq.Array(collections))
	if err != nil {
		return nil, fmt.Errorf("list kept objects of community %s: %w", communityDID, err)
	}
	defer func() { _ = rows.Close() }()
	var mappings []*APObjectMapping
	for rows.Next() {
		mapping, err := scanAPObject(rows)
		if err != nil {
			return nil, fmt.Errorf("scan kept object of community %s: %w", communityDID, err)
		}
		mappings = append(mappings, mapping)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list kept objects of community %s: %w", communityDID, err)
	}
	return mappings, nil
}
