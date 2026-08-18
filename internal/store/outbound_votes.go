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
	//
	// This decides only what the caller ASKED FOR, not what the row ends up
	// holding: on a re-cast the ON CONFLICT below may keep a stored
	// `delivered` over the `pending` defaulted here. The two rules do not
	// disagree — this one refuses to INVENT a delivery nobody witnessed, that
	// one refuses to DISCARD one that was.
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
	//
	// The CASE on delivered_state defends ONE transition: `delivered` must not
	// be overwritten by `pending`. A re-cast REPLACES a vote the peer still
	// holds — it does not withdraw it — and the consumer states `pending` on
	// every write because it records intent and cannot know what the wire said.
	// Letting that land would erase the only record that a delivery ever
	// happened, unrecoverably: no event re-fires it, the vote drops out of the
	// standing list the erasure purge enumerates, and it is left un-retractable
	// on someone else's instance.
	//
	// Only that transition, because this is the INTENT writer and every other
	// caller here is restating the row on purpose: the purge retracts THROUGH
	// this upsert with `undone` (outbound.Purger.undoLiveVotes), in the same
	// statement that bumps the seq for the Undo it enqueues, so a guard that
	// defended `delivered` against everything would silently drop it. The
	// asymmetry with SetDeliveredState — where undone IS terminal — is
	// deliberate: that method fields late settlements, stale facts about an old
	// message, while this one fields new writes by a live human.
	//
	// It lives in SQL, not Go: the consumer's state read is non-transactional,
	// so a read-then-decide guard races the delivery worker's settlement. The
	// CASE evaluates under the row lock ON CONFLICT already holds.
	//
	// FREEZING `direction` ALONGSIDE IT WAS REJECTED. It looks like the
	// consistent move — keep every fact about the delivered vote together —
	// but consume.applyVoteWrite builds the OUTGOING intent from the row this
	// statement RETURNS, so a frozen direction would federate the flip in the
	// direction the user just abandoned, and the peer would keep counting the
	// vote they changed away from. It is also the rejected "what the peer
	// holds" vs "what the user wants" column pair collapsed into one column,
	// carrying the same defect: two facts in one place with no way to tell
	// which a reader meant. The row therefore states the newest intent and the
	// older delivery TOGETHER, on purpose (store.DeliveredStateDelivered).
	//
	// THE ACCEPTED COST, so it is not rediscovered as a fresh bug: delete a
	// vote and re-cast the SAME rkey before the Undo settles, and the late
	// callback resolves the OLD activity id, misses (GetByActivityID →
	// NotFound → no-op), and this row keeps `delivered` for a vote the peer no
	// longer holds — until the next flip delivers or an undo lands. Narrow and
	// known, and chosen over the pre-fix behaviour, where the same sequence
	// left the vote invisible to BOTH the erasure purge and the reseed rather
	// than merely stale to one of them.
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
			delivered_state = CASE
				WHEN outbound_votes.delivered_state = '` + string(DeliveredStateDelivered) + `'
				 AND EXCLUDED.delivered_state = '` + string(DeliveredStatePending) + `'
				THEN outbound_votes.delivered_state
				ELSE EXCLUDED.delivered_state
			END,
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

func (r *postgresOutboundVotes) ListStandingForActor(ctx context.Context, actorDID string) ([]OutboundVote, error) {
	if actorDID == "" {
		return nil, errors.NewValidationError("actor_did", "must not be empty")
	}
	// STANDING ON A PEER is a WIDER set than delivered_state = 'delivered', and
	// the difference is a real vote on a real instance.
	//
	// A delivery HELD FOR SETTLEMENT has already been accepted by the peer — the
	// POST returned, only our own bookkeeping failed — while its ledger row
	// still reads 'pending' until the worker comes back to finish. Enumerating
	// 'delivered' alone therefore misses a vote the peer demonstrably holds, and
	// the miss is permanent rather than transient: that settlement lands AFTER
	// the withdrawal meant to retract it, and a purge never re-runs. 17b's
	// reseed then subtracts it from a served score forever, for somebody who no
	// longer exists.
	//
	// BUT NOT WIDER THAN THE RETRACTION LEDGER: the held arm must still read the
	// VOTE's own state, because a vote flipped `undone` by a previous purge
	// keeps its Like delivery held until the worker settles it — pending, held,
	// and matching the EXISTS below. A purge replay is ordinary (its
	// transaction commits before the rev gate's), and re-enumerating that vote
	// re-runs the retraction: activity_seq bumps, a NEW seq-derived Undo id is
	// minted, and a duplicate Undo goes out that the peer refuses into poison.
	// `undone` records that the retraction is already on the books — the
	// replay's job for it is done, however the delivery's bookkeeping stands.
	//
	// The first term is served by the partial index (migration 029); the second
	// is an EXISTS against the delivery whose id the vote already carries.
	query := `SELECT` + outboundVoteColumns + `
		FROM outbound_votes v
		WHERE v.actor_did = $1
		  AND (v.delivered_state = 'delivered'
		       OR (v.delivered_state <> '` + string(DeliveredStateUndone) + `'
		           AND EXISTS (
		                SELECT 1 FROM outbound_deliveries d
		                 WHERE d.activity_id = v.current_activity_id
		                   AND d.state = 'pending'
		                   AND d.last_error_class = '` + DeliveryHeldForSettlement + `')))
		ORDER BY v.vote_at_uri`

	rows, err := r.db.QueryContext(ctx, query, actorDID)
	if err != nil {
		return nil, fmt.Errorf("list standing votes for %q: %w", actorDID, err)
	}
	defer func() { _ = rows.Close() }()

	var votes []OutboundVote
	for rows.Next() {
		vote, err := scanOutboundVote(rows)
		if err != nil {
			return nil, fmt.Errorf("scan standing vote for %q: %w", actorDID, err)
		}
		votes = append(votes, *vote)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list standing votes for %q: %w", actorDID, err)
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
	// UNDONE IS TERMINAL HERE, and the ordering that makes this necessary is
	// ordinary rather than exotic. A delivery HELD FOR SETTLEMENT has already
	// been accepted by the peer, so a withdrawal can legitimately retract the
	// vote while the worker is still on its way back to finish the bookkeeping;
	// when it arrives it calls this method with `delivered`. Letting that late
	// settlement win would re-establish exactly the state the erasure removed —
	// the peer holds a vote we told them to drop, and 17b's reseed subtracts it
	// from a served score forever.
	//
	// Re-setting undone stays allowed, so the write is idempotent.
	result, err := r.db.ExecContext(ctx, `
		UPDATE outbound_votes SET delivered_state = $2, updated_at = now()
		 WHERE vote_at_uri = $1
		   AND (delivered_state <> $3 OR $2 = $3)`,
		voteATURI, string(state), string(DeliveredStateUndone))
	if err != nil {
		return fmt.Errorf("set delivered_state for outbound_vote %q: %w", voteATURI, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("set delivered_state for outbound_vote %q: rows affected: %w", voteATURI, err)
	}
	if affected == 0 {
		// TWO DIFFERENT NOTHINGS, and they cannot share a branch.
		//
		// A row that is already retracted was deliberately not moved by the
		// guard above: that is a DECIDED no-op and must report success. An error
		// would fail the settlement, leaving the delivery held and retrying a
		// write that can never apply, against a decision that will never change.
		//
		// A row that does not exist at all is the original finding this branch
		// was written for — the intent and the delivery disagree about what
		// exists — and is still worth surfacing.
		if _, err := r.GetByATURI(ctx, voteATURI); err != nil {
			return err
		}
		return nil
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
