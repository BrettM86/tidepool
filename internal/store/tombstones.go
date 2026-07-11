package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"tidepool/internal/errors"
)

type postgresTombstones struct {
	db *sql.DB
}

// NewTombstones creates the postgres-backed ap_tombstones repository.
func NewTombstones(db *sql.DB) Tombstones {
	return &postgresTombstones{db: db}
}

func (r *postgresTombstones) Record(ctx context.Context, apID string) error {
	if apID == "" {
		return errors.NewValidationError("ap_id", "must not be empty")
	}
	query := `
		INSERT INTO ap_tombstones (ap_id)
		VALUES ($1)
		ON CONFLICT (ap_id) DO NOTHING`
	if _, err := r.db.ExecContext(ctx, query, apID); err != nil {
		return fmt.Errorf("record tombstone %q: %w", apID, err)
	}
	return nil
}

func (r *postgresTombstones) Exists(ctx context.Context, apID string) (bool, error) {
	if apID == "" {
		return false, errors.NewValidationError("ap_id", "must not be empty")
	}
	var exists bool
	query := `SELECT EXISTS (SELECT 1 FROM ap_tombstones WHERE ap_id = $1)`
	if err := r.db.QueryRowContext(ctx, query, apID).Scan(&exists); err != nil {
		return false, fmt.Errorf("check tombstone %q: %w", apID, err)
	}
	return exists, nil
}

func (r *postgresTombstones) Remove(ctx context.Context, apID string) error {
	if apID == "" {
		return errors.NewValidationError("ap_id", "must not be empty")
	}
	if _, err := r.db.ExecContext(ctx,
		`DELETE FROM ap_tombstones WHERE ap_id = $1`, apID); err != nil {
		return fmt.Errorf("remove tombstone %q: %w", apID, err)
	}
	return nil
}

// tombstonePruneBatchSize bounds one DELETE inside Prune (same reasoning as
// the firehose pruner: short statements never stall the write path).
const tombstonePruneBatchSize = 1000

func (r *postgresTombstones) Prune(ctx context.Context, cutoff time.Time) (int64, error) {
	var total int64
	for {
		res, err := r.db.ExecContext(ctx, `
			DELETE FROM ap_tombstones WHERE ap_id IN (
				SELECT ap_id FROM ap_tombstones WHERE deleted_at < $1 LIMIT $2
			)`, cutoff, tombstonePruneBatchSize)
		if err != nil {
			return total, fmt.Errorf("prune tombstones before %s: %w", cutoff.Format(time.RFC3339), err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return total, fmt.Errorf("prune tombstones: rows affected: %w", err)
		}
		total += n
		if n < tombstonePruneBatchSize {
			return total, nil
		}
	}
}
