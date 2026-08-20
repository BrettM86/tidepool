package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"

	"tidepool/internal/errors"
)

type postgresOutboundActivities struct {
	db *sql.DB
}

// NewOutboundActivities creates the postgres-backed outbound_activities
// repository.
func NewOutboundActivities(db *sql.DB) OutboundActivities {
	return &postgresOutboundActivities{db: db}
}

const outboundActivityColumns = `
	activity_id, actor_did, kind, payload, parent_at_uri, created_at`

func (r *postgresOutboundActivities) Insert(ctx context.Context, activity OutboundActivity) (bool, error) {
	return r.insert(ctx, r.db, activity)
}

func (r *postgresOutboundActivities) InsertTx(ctx context.Context, tx *sql.Tx, activity OutboundActivity) (bool, error) {
	if tx == nil {
		return false, errors.NewValidationError("tx", "must not be nil")
	}
	return r.insert(ctx, tx, activity)
}

func (r *postgresOutboundActivities) insert(ctx context.Context, q execer, activity OutboundActivity) (bool, error) {
	// ON CONFLICT DO NOTHING makes the canonical payload immutable: a
	// redelivery re-derives the SAME id and re-inserts, and the row a peer may
	// already hold must never be rewritten under it. inserted reports whether a
	// fresh row landed (rows affected == 1), never overwriting.
	query := `
		INSERT INTO outbound_activities (activity_id, actor_did, kind, payload, parent_at_uri)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (activity_id) DO NOTHING`

	result, err := q.ExecContext(ctx, query,
		activity.ActivityID, activity.ActorDID, activity.Kind, activity.Payload, activity.ParentATURI)
	if err != nil {
		return false, fmt.Errorf("insert outbound_activity %q: %w", activity.ActivityID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("insert outbound_activity %q: rows affected: %w", activity.ActivityID, err)
	}
	return affected == 1, nil
}

func (r *postgresOutboundActivities) Get(ctx context.Context, activityID string) (*OutboundActivity, error) {
	query := `SELECT` + outboundActivityColumns + ` FROM outbound_activities WHERE activity_id = $1`
	activity, err := scanOutboundActivity(r.db.QueryRowContext(ctx, query, activityID))
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFoundError("outbound_activity", activityID)
		}
		return nil, fmt.Errorf("get outbound_activity %q: %w", activityID, err)
	}
	return activity, nil
}

func scanOutboundActivity(row rowScanner) (*OutboundActivity, error) {
	var activity OutboundActivity
	err := row.Scan(
		&activity.ActivityID, &activity.ActorDID, &activity.Kind,
		&activity.Payload, &activity.ParentATURI, &activity.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &activity, nil
}
