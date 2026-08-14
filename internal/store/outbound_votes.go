package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"

	"tidepool/internal/errors"
)

type postgresOutboundVotes struct {
	db *sql.DB
}

// NewOutboundVotes creates the postgres-backed outbound_votes repository.
func NewOutboundVotes(db *sql.DB) OutboundVotes {
	return &postgresOutboundVotes{db: db}
}

const outboundVoteColumns = `
	vote_at_uri, actor_did, subject_at_uri, subject_ap_id, community_did,
	direction, current_activity_id, delivered_state, activity_seq,
	created_at, updated_at`

func (r *postgresOutboundVotes) Upsert(ctx context.Context, vote OutboundVote) (*OutboundVote, error) {
	return r.upsert(ctx, r.db, vote)
}

func (r *postgresOutboundVotes) UpsertTx(ctx context.Context, tx *sql.Tx, vote OutboundVote) (*OutboundVote, error) {
	if tx == nil {
		return nil, errors.NewValidationError("tx", "must not be nil")
	}
	return r.upsert(ctx, tx, vote)
}

func (r *postgresOutboundVotes) upsert(ctx context.Context, q execer, vote OutboundVote) (*OutboundVote, error) {
	// An unstated delivered_state is pending, never delivered: the consumer
	// records INTENT, and only task 15 may claim delivery — on success, from
	// the wire. Defaulting the zero value the other way would silently mark a
	// vote as delivered that no peer ever saw, and its Undo would then look
	// unnecessary.
	if vote.DeliveredState == "" {
		vote.DeliveredState = DeliveredStatePending
	}
	if !vote.DeliveredState.Valid() {
		return nil, errors.NewValidationError("delivered_state", "unknown state "+string(vote.DeliveredState))
	}

	// ON CONFLICT names the PRIMARY KEY only. The (actor_did, subject_at_uri)
	// constraint is deliberately NOT an upsert target: a different vote record
	// for a pair that already holds one must FAIL, because overwriting the row
	// would strand the Undo still owed for the first vote.
	query := `
		INSERT INTO outbound_votes (
			vote_at_uri, actor_did, subject_at_uri, subject_ap_id, community_did,
			direction, current_activity_id, delivered_state
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (vote_at_uri) DO UPDATE SET
			actor_did = EXCLUDED.actor_did,
			subject_at_uri = EXCLUDED.subject_at_uri,
			subject_ap_id = EXCLUDED.subject_ap_id,
			community_did = EXCLUDED.community_did,
			direction = EXCLUDED.direction,
			current_activity_id = EXCLUDED.current_activity_id,
			delivered_state = EXCLUDED.delivered_state,
			activity_seq = outbound_votes.activity_seq + 1,
			updated_at = now()
		RETURNING` + outboundVoteColumns

	row := q.QueryRowContext(ctx, query,
		vote.VoteATURI, vote.ActorDID, vote.SubjectATURI, vote.SubjectAPID,
		vote.CommunityDID, vote.Direction, vote.CurrentActivityID, string(vote.DeliveredState),
	)
	stored, err := scanOutboundVote(row)
	if err != nil {
		// Mapped from the constraint name rather than pre-checked: a
		// SELECT-then-INSERT pre-check races with a concurrent handler.
		if constraint, ok := uniqueViolation(err); ok && constraint == "outbound_votes_actor_subject_key" {
			return nil, errors.NewConflictError("outbound_vote", "actor_subject", vote.ActorDID+" "+vote.SubjectATURI)
		}
		return nil, fmt.Errorf("upsert outbound_vote %q: %w", vote.VoteATURI, err)
	}
	return stored, nil
}

func (r *postgresOutboundVotes) GetByATURI(ctx context.Context, voteATURI string) (*OutboundVote, error) {
	query := `SELECT` + outboundVoteColumns + ` FROM outbound_votes WHERE vote_at_uri = $1`
	vote, err := scanOutboundVote(r.db.QueryRowContext(ctx, query, voteATURI))
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFoundError("outbound_vote", voteATURI)
		}
		return nil, fmt.Errorf("get outbound_vote %q: %w", voteATURI, err)
	}
	return vote, nil
}

func (r *postgresOutboundVotes) GetByActorSubject(ctx context.Context, actorDID, subjectATURI string) (*OutboundVote, error) {
	query := `SELECT` + outboundVoteColumns + `
		FROM outbound_votes WHERE actor_did = $1 AND subject_at_uri = $2`
	vote, err := scanOutboundVote(r.db.QueryRowContext(ctx, query, actorDID, subjectATURI))
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFoundError("outbound_vote", actorDID+" on "+subjectATURI)
		}
		return nil, fmt.Errorf("get outbound_vote for %q on %q: %w", actorDID, subjectATURI, err)
	}
	return vote, nil
}

// GetByActivityID looks a vote up by its current activity id — the delivery
// callback's lookup: a Like/Dislike is delivered under CurrentActivityID and an
// Undo embeds that same id, so both success callbacks resolve the vote row from
// the one id. A miss is a NotFound.
func (r *postgresOutboundVotes) GetByActivityID(ctx context.Context, activityID string) (*OutboundVote, error) {
	query := `SELECT` + outboundVoteColumns + ` FROM outbound_votes WHERE current_activity_id = $1`
	vote, err := scanOutboundVote(r.db.QueryRowContext(ctx, query, activityID))
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFoundError("outbound_vote", activityID)
		}
		return nil, fmt.Errorf("get outbound_vote by activity id %q: %w", activityID, err)
	}
	return vote, nil
}

func (r *postgresOutboundVotes) ListDeliveredForActor(ctx context.Context, actorDID string) ([]OutboundVote, error) {
	if actorDID == "" {
		return nil, errors.NewValidationError("actor_did", "must not be empty")
	}
	// LIVE means exactly delivered_state = 'delivered' — POSITIVE equality, per
	// decision 16: those are the votes a peer still holds, and the same set the
	// reseed subtracts from the origin's API tally. A purged actor leaving them
	// standing is a number the reseed keeps subtracting from a score readers
	// see, forever, on behalf of somebody who no longer exists.
	//
	// Served by the partial index on the same predicate (migration 029).
	query := `SELECT` + outboundVoteColumns + `
		FROM outbound_votes
		WHERE actor_did = $1 AND delivered_state = 'delivered'
		ORDER BY vote_at_uri`

	rows, err := r.db.QueryContext(ctx, query, actorDID)
	if err != nil {
		return nil, fmt.Errorf("list delivered votes for %q: %w", actorDID, err)
	}
	defer func() { _ = rows.Close() }()

	var votes []OutboundVote
	for rows.Next() {
		vote, err := scanOutboundVote(rows)
		if err != nil {
			return nil, fmt.Errorf("scan delivered vote for %q: %w", actorDID, err)
		}
		votes = append(votes, *vote)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list delivered votes for %q: %w", actorDID, err)
	}
	return votes, nil
}

func (r *postgresOutboundVotes) SetDeliveredState(ctx context.Context, voteATURI string, state DeliveredState) error {
	// Validated in Go rather than left to the CHECK constraint: an unknown
	// state is a caller bug, and the caller needs it back as a validation
	// error, not as a wrapped SQLSTATE it has no way to interpret.
	if !state.Valid() {
		return errors.NewValidationError("delivered_state", "unknown state "+string(state))
	}
	result, err := r.db.ExecContext(ctx,
		`UPDATE outbound_votes SET delivered_state = $2, updated_at = now() WHERE vote_at_uri = $1`,
		voteATURI, string(state))
	if err != nil {
		return fmt.Errorf("set delivered_state for outbound_vote %q: %w", voteATURI, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("set delivered_state for outbound_vote %q: rows affected: %w", voteATURI, err)
	}
	if affected == 0 {
		// Delivering a vote we hold no state for means the intent and the
		// delivery disagree about what exists. That is a bug worth surfacing,
		// not a no-op to swallow.
		return errors.NewNotFoundError("outbound_vote", voteATURI)
	}
	return nil
}

func (r *postgresOutboundVotes) Delete(ctx context.Context, voteATURI string) error {
	// A missing row is success: the delivery callback that clears vote state
	// may re-fire, and the desired end state — no live vote — already holds.
	if _, err := r.db.ExecContext(ctx,
		`DELETE FROM outbound_votes WHERE vote_at_uri = $1`, voteATURI); err != nil {
		return fmt.Errorf("delete outbound_vote %q: %w", voteATURI, err)
	}
	return nil
}

func scanOutboundVote(row rowScanner) (*OutboundVote, error) {
	var vote OutboundVote
	var deliveredState string
	err := row.Scan(
		&vote.VoteATURI, &vote.ActorDID, &vote.SubjectATURI, &vote.SubjectAPID,
		&vote.CommunityDID, &vote.Direction, &vote.CurrentActivityID,
		&deliveredState, &vote.ActivitySeq, &vote.CreatedAt, &vote.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	vote.DeliveredState = DeliveredState(deliveredState)
	return &vote, nil
}
