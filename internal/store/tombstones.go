package store

import (
	"context"
	"database/sql"
	"fmt"

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
