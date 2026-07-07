package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"

	"tidepool/internal/errors"
)

type postgresInboxEvents struct {
	db *sql.DB
}

// NewInboxEvents creates the postgres-backed inbox_events repository.
func NewInboxEvents(db *sql.DB) InboxEvents {
	return &postgresInboxEvents{db: db}
}

func (r *postgresInboxEvents) RecordEvent(ctx context.Context, activityID, activityType string) (bool, error) {
	if activityID == "" {
		return false, errors.NewValidationError("activity_id", "must not be empty")
	}
	if activityType == "" {
		return false, errors.NewValidationError("type", "must not be empty")
	}

	query := `
		INSERT INTO inbox_events (activity_id, type)
		VALUES ($1, $2)
		ON CONFLICT (activity_id) DO NOTHING`

	result, err := r.db.ExecContext(ctx, query, activityID, activityType)
	if err != nil {
		return false, fmt.Errorf("record inbox event %q: %w", activityID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("record inbox event %q: rows affected: %w", activityID, err)
	}
	return affected == 1, nil
}

func (r *postgresInboxEvents) MarkProcessed(ctx context.Context, activityID string) error {
	query := `
		UPDATE inbox_events
		SET processed_at = CURRENT_TIMESTAMP, error = NULL
		WHERE activity_id = $1`

	result, err := r.db.ExecContext(ctx, query, activityID)
	if err != nil {
		return fmt.Errorf("mark inbox event %q processed: %w", activityID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("mark inbox event %q processed: rows affected: %w", activityID, err)
	}
	if affected == 0 {
		return errors.NewNotFoundError("inbox_event", activityID)
	}
	return nil
}

func (r *postgresInboxEvents) MarkFailed(ctx context.Context, activityID string, message string) error {
	if message == "" {
		return errors.NewValidationError("error", "must not be empty")
	}

	// Only unprocessed events accept failures: a late failure report from
	// a stale worker must not un-process an event a successful retry
	// already completed. One statement distinguishes "row missing"
	// (not found) from "row already processed" (no-op success).
	query := `
		WITH updated AS (
			UPDATE inbox_events
			SET error = $2
			WHERE activity_id = $1 AND processed_at IS NULL
			RETURNING 1
		)
		SELECT EXISTS (SELECT 1 FROM inbox_events WHERE activity_id = $1)`

	var exists bool
	if err := r.db.QueryRowContext(ctx, query, activityID, message).Scan(&exists); err != nil {
		return fmt.Errorf("mark inbox event %q failed: %w", activityID, err)
	}
	if !exists {
		return errors.NewNotFoundError("inbox_event", activityID)
	}
	return nil
}

func (r *postgresInboxEvents) GetEvent(ctx context.Context, activityID string) (*InboxEvent, error) {
	query := `
		SELECT id, activity_id, type, received_at, processed_at, COALESCE(error, '')
		FROM inbox_events WHERE activity_id = $1`

	var event InboxEvent
	err := r.db.QueryRowContext(ctx, query, activityID).Scan(
		&event.ID, &event.ActivityID, &event.Type,
		&event.ReceivedAt, &event.ProcessedAt, &event.Error,
	)
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFoundError("inbox_event", activityID)
		}
		return nil, fmt.Errorf("get inbox event %q: %w", activityID, err)
	}
	return &event, nil
}
