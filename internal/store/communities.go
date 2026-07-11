package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"

	"tidepool/internal/errors"
)

type postgresCommunities struct {
	db *sql.DB
}

// NewCommunities creates the postgres-backed communities repository.
func NewCommunities(db *sql.DB) Communities {
	return &postgresCommunities{db: db}
}

const communityColumns = `
	id, ap_group_id, did, preferred_username, instance,
	follow_state, followed_at, last_backfill_at, created_at,
	follow_requested_at, follow_attempts`

func (r *postgresCommunities) UpsertCommunity(ctx context.Context, community Community) (*Community, error) {
	if err := validateCommunity(&community); err != nil {
		return nil, err
	}

	// On conflict only preferred_username updates (display renames happen
	// upstream); did, instance, follow state, and timestamps are preserved.
	// The DO UPDATE's WHERE refuses identity drift (a changed DID or
	// instance for a known ap_group_id): the excluded row surfaces as "no
	// row returned" and is reported as a conflict below.
	query := `
		INSERT INTO communities (
			ap_group_id, did, preferred_username, instance, follow_state
		) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (ap_group_id) DO UPDATE SET
			preferred_username = EXCLUDED.preferred_username
		WHERE communities.did = EXCLUDED.did
		  AND communities.instance = EXCLUDED.instance
		RETURNING` + communityColumns

	followState := community.FollowState
	if followState == "" {
		followState = FollowStateNone
	}

	row := r.db.QueryRowContext(ctx, query,
		community.APGroupID, community.DID, community.PreferredUsername,
		community.Instance, string(followState),
	)
	stored, err := scanCommunity(row)
	if stderrors.Is(err, sql.ErrNoRows) {
		// The DO UPDATE's WHERE excluded the existing row: the caller's
		// identity fields diverge from the stored ones.
		existing, getErr := r.GetByAPGroupID(ctx, community.APGroupID)
		if getErr != nil {
			return nil, fmt.Errorf("upsert community %q: recheck after excluded update: %w", community.APGroupID, getErr)
		}
		if existing.DID != community.DID {
			return nil, errors.NewConflictError("community", "did", community.DID)
		}
		return nil, errors.NewConflictError("community", "instance", community.Instance)
	}
	if err != nil {
		// ap_group_id conflicts are absorbed by the upsert, so the only
		// expected unique violation is the did constraint.
		if constraint, ok := uniqueViolation(err); ok && constraint == "communities_did_key" {
			return nil, errors.NewConflictError("community", "did", community.DID)
		}
		return nil, fmt.Errorf("upsert community %q: %w", community.APGroupID, err)
	}
	return stored, nil
}

func (r *postgresCommunities) GetByAPGroupID(ctx context.Context, apGroupID string) (*Community, error) {
	query := `SELECT` + communityColumns + ` FROM communities WHERE ap_group_id = $1`
	community, err := scanCommunity(r.db.QueryRowContext(ctx, query, apGroupID))
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFoundError("community", apGroupID)
		}
		return nil, fmt.Errorf("get community by ap_group_id %q: %w", apGroupID, err)
	}
	return community, nil
}

func (r *postgresCommunities) GetByDID(ctx context.Context, did string) (*Community, error) {
	query := `SELECT` + communityColumns + ` FROM communities WHERE did = $1`
	community, err := scanCommunity(r.db.QueryRowContext(ctx, query, did))
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFoundError("community", did)
		}
		return nil, fmt.Errorf("get community by did %q: %w", did, err)
	}
	return community, nil
}

func (r *postgresCommunities) SetFollowState(ctx context.Context, apGroupID string, state FollowState) error {
	if !state.Valid() {
		return errors.NewValidationError("follow_state", fmt.Sprintf("unknown state %q", state))
	}

	// followed_at stamps only on the transition INTO accepted — AP happily
	// redelivers Accept, and a re-accept must not re-stamp the original
	// time. none clears it; pending leaves it alone.
	//
	// The follow-retry bookkeeping (task 11) rides the same statement:
	// every set-to-pending means "a Follow was just sent" (admin subscribe
	// and the retrier both send before recording), so it stamps
	// follow_requested_at and increments follow_attempts; none resets both
	// (a fresh subscription starts a fresh budget); accepted leaves them
	// for post-mortems.
	query := `
		UPDATE communities
		SET followed_at = CASE
		        WHEN $2 = 'accepted' AND follow_state <> 'accepted' THEN CURRENT_TIMESTAMP
		        WHEN $2 = 'none' THEN NULL
		        ELSE followed_at
		    END,
		    follow_requested_at = CASE
		        WHEN $2 = 'pending' THEN CURRENT_TIMESTAMP
		        WHEN $2 = 'none' THEN NULL
		        ELSE follow_requested_at
		    END,
		    follow_attempts = CASE
		        WHEN $2 = 'pending' THEN follow_attempts + 1
		        WHEN $2 = 'none' THEN 0
		        ELSE follow_attempts
		    END,
		    follow_state = $2
		WHERE ap_group_id = $1`

	result, err := r.db.ExecContext(ctx, query, apGroupID, string(state))
	if err != nil {
		return fmt.Errorf("set follow_state for %q: %w", apGroupID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("set follow_state for %q: rows affected: %w", apGroupID, err)
	}
	if affected == 0 {
		return errors.NewNotFoundError("community", apGroupID)
	}
	return nil
}

func (r *postgresCommunities) SetLastBackfill(ctx context.Context, apGroupID string, backfilledAt time.Time) error {
	query := `UPDATE communities SET last_backfill_at = $2 WHERE ap_group_id = $1`
	result, err := r.db.ExecContext(ctx, query, apGroupID, backfilledAt)
	if err != nil {
		return fmt.Errorf("set last_backfill_at for %q: %w", apGroupID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("set last_backfill_at for %q: rows affected: %w", apGroupID, err)
	}
	if affected == 0 {
		return errors.NewNotFoundError("community", apGroupID)
	}
	return nil
}

func (r *postgresCommunities) ListByFollowState(ctx context.Context, state FollowState) ([]*Community, error) {
	if !state.Valid() {
		return nil, errors.NewValidationError("follow_state", fmt.Sprintf("unknown state %q", state))
	}

	query := `SELECT` + communityColumns + `
		FROM communities WHERE follow_state = $1 ORDER BY created_at, id`

	rows, err := r.db.QueryContext(ctx, query, string(state))
	if err != nil {
		return nil, fmt.Errorf("list communities by follow_state %q: %w", state, err)
	}
	defer func() { _ = rows.Close() }()

	var communities []*Community
	for rows.Next() {
		community, err := scanCommunity(rows)
		if err != nil {
			return nil, fmt.Errorf("list communities by follow_state %q: scan: %w", state, err)
		}
		communities = append(communities, community)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list communities by follow_state %q: rows: %w", state, err)
	}
	return communities, nil
}

func (r *postgresCommunities) ClaimStalePendingFollows(ctx context.Context, requestedBefore time.Time, maxAttempts int) ([]*Community, error) {
	if maxAttempts <= 0 {
		return nil, errors.NewValidationError("max_attempts", "must be positive")
	}
	// One atomic conditional claim: the UPDATE consumes an attempt and
	// re-stamps follow_requested_at on exactly the rows it matches, and
	// RETURNING hands back only those (post-increment) rows for the retrier
	// to send. Because the claim IS the match, a concurrent sweep and an
	// Accept can never be clobbered:
	//   - two overlapping sweeps: the second UPDATE re-evaluates its WHERE
	//     against the first sweep's just-written follow_requested_at (row is
	//     row-locked until the first commits), which is no longer < the
	//     cutoff, so it claims nothing — no double-send;
	//   - an Accept between sweeps flips follow_state to 'accepted', which
	//     the WHERE excludes, so the claim leaves it untouched (no downgrade
	//     back to pending).
	// NULL follow_requested_at (legacy rows from before migration 012) is
	// treated as stale: a pending row with no recorded send time has, by
	// definition, waited longer than any threshold.
	query := `
		UPDATE communities
		SET follow_attempts = follow_attempts + 1,
		    follow_requested_at = CURRENT_TIMESTAMP
		WHERE follow_state = 'pending'
		  AND follow_attempts < $2
		  AND (follow_requested_at IS NULL OR follow_requested_at < $1)
		RETURNING` + communityColumns

	rows, err := r.db.QueryContext(ctx, query, requestedBefore, maxAttempts)
	if err != nil {
		return nil, fmt.Errorf("claim stale pending follows: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var communities []*Community
	for rows.Next() {
		community, err := scanCommunity(rows)
		if err != nil {
			return nil, fmt.Errorf("claim stale pending follows: scan: %w", err)
		}
		communities = append(communities, community)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim stale pending follows: rows: %w", err)
	}
	return communities, nil
}

func validateCommunity(community *Community) error {
	if community.APGroupID == "" {
		return errors.NewValidationError("ap_group_id", "must not be empty")
	}
	if _, err := syntax.ParseDID(community.DID); err != nil {
		return errors.NewValidationError("did", err.Error())
	}
	if community.PreferredUsername == "" {
		return errors.NewValidationError("preferred_username", "must not be empty")
	}
	if community.Instance == "" {
		return errors.NewValidationError("instance", "must not be empty")
	}
	// Unlike consent, follow state has a safe zero value: "" defaults to
	// none in UpsertCommunity.
	if community.FollowState != "" && !community.FollowState.Valid() {
		return errors.NewValidationError("follow_state",
			fmt.Sprintf("unknown state %q", community.FollowState))
	}
	return nil
}

func scanCommunity(row rowScanner) (*Community, error) {
	var community Community
	var followState string
	err := row.Scan(
		&community.ID, &community.APGroupID, &community.DID,
		&community.PreferredUsername, &community.Instance,
		&followState, &community.FollowedAt, &community.LastBackfillAt,
		&community.CreatedAt,
		&community.FollowRequestedAt, &community.FollowAttempts,
	)
	if err != nil {
		return nil, err
	}
	community.FollowState = FollowState(followState)
	return &community, nil
}
