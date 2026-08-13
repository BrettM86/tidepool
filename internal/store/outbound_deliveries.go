package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"time"

	"tidepool/internal/errors"
)

type postgresOutboundDeliveries struct {
	db *sql.DB
}

// NewOutboundDeliveries creates the postgres-backed outbound_deliveries
// repository — the per-inbox delivery queue (task 15).
func NewOutboundDeliveries(db *sql.DB) OutboundDeliveries {
	return &postgresOutboundDeliveries{db: db}
}

// deliveryColumns is the SELECT/RETURNING list for a full OutboundDelivery row.
const deliveryColumns = `
	seq, activity_id, target_inbox, ordering_key, state, attempts,
	next_attempt_at, claimed_until, delivered_at, last_status_code,
	last_error_class, response_excerpt, created_at, updated_at`

func (r *postgresOutboundDeliveries) Enqueue(ctx context.Context, delivery OutboundDelivery) (*OutboundDelivery, error) {
	return r.enqueue(ctx, r.db, delivery)
}

func (r *postgresOutboundDeliveries) EnqueueTx(ctx context.Context, tx *sql.Tx, delivery OutboundDelivery) (*OutboundDelivery, error) {
	if tx == nil {
		return nil, errors.NewValidationError("tx", "must not be nil")
	}
	return r.enqueue(ctx, tx, delivery)
}

func (r *postgresOutboundDeliveries) enqueue(ctx context.Context, q execer, delivery OutboundDelivery) (*OutboundDelivery, error) {
	// A fresh delivery is pending, unattempted, unclaimed: state, attempts,
	// next_attempt_at and seq are all defaulted by the table. A duplicate
	// (activity, inbox) pair violates the PK — mapped to AlreadyExists rather
	// than pre-checked, since a SELECT-then-INSERT races a concurrent enqueue.
	query := `
		INSERT INTO outbound_deliveries (activity_id, target_inbox, ordering_key)
		VALUES ($1, $2, $3)
		RETURNING` + deliveryColumns

	stored, err := scanOutboundDelivery(q.QueryRowContext(ctx, query,
		delivery.ActivityID, delivery.TargetInbox, delivery.OrderingKey))
	if err != nil {
		if _, ok := uniqueViolation(err); ok {
			return nil, errors.NewConflictError("outbound_delivery", "activity_inbox",
				delivery.ActivityID+" "+delivery.TargetInbox)
		}
		return nil, fmt.Errorf("enqueue outbound_delivery %q -> %q: %w",
			delivery.ActivityID, delivery.TargetInbox, err)
	}
	return stored, nil
}

func (r *postgresOutboundDeliveries) ClaimNext(ctx context.Context, lease time.Duration) (*OutboundDelivery, error) {
	if lease <= 0 {
		return nil, errors.NewValidationError("lease", "must be positive")
	}

	// Generalizes inbox_events.ClaimNext (task 12's lesson): the candidate is
	// the head of an ordering key — its min-seq PENDING row — that is also past
	// its next_attempt_at and unleased. Per-community serialization is
	// head-of-line: while an older pending sibling on a key exists (queued,
	// leased, or backing off) every younger delivery on that key is invisible;
	// a delivered/poisoned/cancelled sibling leaves the partial index and stops
	// blocking.
	//
	// The heads are found by a recursive CTE emulating a loose index scan over
	// idx_outbound_deliveries_queue (ordering_key, seq WHERE state='pending'):
	// one index descent per DISTINCT pending key jumps to each key's head, so
	// the claim is O(pending keys × log N) regardless of any one community's
	// backlog depth — never O(backlog) as a per-row NOT EXISTS would be. They
	// are materialized with ARRAY(...) — not a plain IN or a correlated EXISTS —
	// so the planner fetches exactly those rows by seq. The outer SELECT
	// re-applies every claimability predicate on the locked row (a claim
	// committed between the CTE snapshot and the lock is then seen and skipped).
	// FOR UPDATE ... SKIP LOCKED lets concurrent workers race without
	// serializing on row locks; the UPDATE stamps the lease and counts the
	// attempt atomically.
	query := `
		UPDATE outbound_deliveries
		SET claimed_until = CURRENT_TIMESTAMP + make_interval(secs => $1),
		    attempts = attempts + 1,
		    updated_at = now()
		WHERE seq = (
			SELECT c.seq FROM outbound_deliveries c
			WHERE c.seq = ANY (ARRAY(
				WITH RECURSIVE key_heads AS (
					SELECT h.seq, h.ordering_key FROM (
						SELECT e.seq, e.ordering_key
						FROM outbound_deliveries e
						WHERE e.state = 'pending'
						ORDER BY e.ordering_key, e.seq
						LIMIT 1
					) h
					UNION ALL
					SELECT n.seq, n.ordering_key FROM key_heads k
					CROSS JOIN LATERAL (
						SELECT e.seq, e.ordering_key
						FROM outbound_deliveries e
						WHERE e.state = 'pending'
						  AND e.ordering_key > k.ordering_key
						ORDER BY e.ordering_key, e.seq
						LIMIT 1
					) n
				)
				SELECT seq FROM key_heads))
			  AND c.state = 'pending'
			  AND c.next_attempt_at <= CURRENT_TIMESTAMP
			  AND (c.claimed_until IS NULL OR c.claimed_until <= CURRENT_TIMESTAMP)
			ORDER BY c.seq
			LIMIT 1
			FOR UPDATE OF c SKIP LOCKED)
		RETURNING` + deliveryColumns

	delivery, err := scanOutboundDelivery(r.db.QueryRowContext(ctx, query, lease.Seconds()))
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFoundError("outbound_delivery", "next claimable")
		}
		return nil, fmt.Errorf("claim next outbound_delivery: %w", err)
	}
	return delivery, nil
}

func (r *postgresOutboundDeliveries) MarkDelivered(ctx context.Context, activityID, targetInbox string, lastStatusCode int, claimToken time.Time) (bool, bool, error) {
	// Fencing: only the worker still holding the claim (claimed_until ==
	// claimToken) may record the outcome, and only while the row is
	// non-terminal (state = 'pending'). A stale worker whose lease lapsed and
	// was re-claimed no longer matches, writes 0 rows, and is reported
	// applied=false so it cannot clobber the newer attempt.
	query := `
		WITH updated AS (
			UPDATE outbound_deliveries
			SET state = 'delivered', delivered_at = CURRENT_TIMESTAMP,
			    claimed_until = NULL, last_status_code = $3, updated_at = now()
			WHERE activity_id = $1 AND target_inbox = $2
			  AND state = 'pending'
			  AND claimed_until = $4
			RETURNING 1
		)
		SELECT
			EXISTS (SELECT 1 FROM outbound_deliveries WHERE activity_id = $1 AND target_inbox = $2),
			EXISTS (SELECT 1 FROM updated)`

	return r.markResult(ctx, "mark delivered", query,
		activityID, targetInbox, lastStatusCode, claimToken.UTC())
}

func (r *postgresOutboundDeliveries) Release(ctx context.Context, activityID, targetInbox, errorClass, excerpt string, lastStatusCode int, nextAttempt, claimToken time.Time) (bool, bool, error) {
	// Fencing + non-terminal guard: only the current claim holder reschedules
	// (claimed_until == claimToken, state = 'pending'), so a stale worker's late
	// release cannot resurrect a delivery a newer attempt already drove to a
	// terminal state. The row stays pending with the lease cleared so a retry
	// can re-claim after the backoff.
	query := `
		WITH updated AS (
			UPDATE outbound_deliveries
			SET claimed_until = NULL, next_attempt_at = $3,
			    last_error_class = $4, response_excerpt = $5,
			    last_status_code = $6, updated_at = now()
			WHERE activity_id = $1 AND target_inbox = $2
			  AND state = 'pending'
			  AND claimed_until = $7
			RETURNING 1
		)
		SELECT
			EXISTS (SELECT 1 FROM outbound_deliveries WHERE activity_id = $1 AND target_inbox = $2),
			EXISTS (SELECT 1 FROM updated)`

	return r.markResult(ctx, "release", query,
		activityID, targetInbox, nextAttempt.UTC(), errorClass, excerpt, lastStatusCode, claimToken.UTC())
}

func (r *postgresOutboundDeliveries) MarkPoisoned(ctx context.Context, activityID, targetInbox, errorClass, excerpt string, lastStatusCode int, claimToken time.Time) (bool, bool, error) {
	// Fencing + non-terminal guard: only the current claim holder may poison
	// (claimed_until == claimToken, state = 'pending'). A poisoned delivery
	// leaves the pending partial index and stops blocking its ordering key.
	query := `
		WITH updated AS (
			UPDATE outbound_deliveries
			SET state = 'poisoned', claimed_until = NULL,
			    last_error_class = $3, response_excerpt = $4,
			    last_status_code = $5, updated_at = now()
			WHERE activity_id = $1 AND target_inbox = $2
			  AND state = 'pending'
			  AND claimed_until = $6
			RETURNING 1
		)
		SELECT
			EXISTS (SELECT 1 FROM outbound_deliveries WHERE activity_id = $1 AND target_inbox = $2),
			EXISTS (SELECT 1 FROM updated)`

	return r.markResult(ctx, "poison", query,
		activityID, targetInbox, errorClass, excerpt, lastStatusCode, claimToken.UTC())
}

// markResult runs a fenced (exists, applied) mark statement and maps a missing
// (activity, inbox) pair to NotFound.
func (r *postgresOutboundDeliveries) markResult(ctx context.Context, op, query string, args ...any) (bool, bool, error) {
	var exists, applied bool
	if err := r.db.QueryRowContext(ctx, query, args...).Scan(&exists, &applied); err != nil {
		return false, false, fmt.Errorf("%s outbound_delivery: %w", op, err)
	}
	return exists, applied, nil
}

func (r *postgresOutboundDeliveries) CancelForActor(ctx context.Context, actorDID string) (int64, error) {
	// Consent/kill-switch withdrawal: park the actor's PENDING work as
	// cancelled (never poisoned — this is not a failure). Terminal deliveries
	// are left untouched. Joined through outbound_activities.actor_did.
	query := `
		UPDATE outbound_deliveries d
		SET state = 'cancelled', claimed_until = NULL, updated_at = now()
		FROM outbound_activities a
		WHERE d.activity_id = a.activity_id
		  AND a.actor_did = $1
		  AND d.state = 'pending'`

	return r.cancel(ctx, "cancel outbound_deliveries for actor", query, actorDID)
}

func (r *postgresOutboundDeliveries) CancelForCommunity(ctx context.Context, orderingKey string) (int64, error) {
	// A community deleted or unfollowed out from under pending work: park every
	// PENDING delivery on the ordering key as cancelled, leaving terminal rows.
	query := `
		UPDATE outbound_deliveries
		SET state = 'cancelled', claimed_until = NULL, updated_at = now()
		WHERE ordering_key = $1 AND state = 'pending'`

	return r.cancel(ctx, "cancel outbound_deliveries for community", query, orderingKey)
}

func (r *postgresOutboundDeliveries) cancel(ctx context.Context, op, query string, arg string) (int64, error) {
	result, err := r.db.ExecContext(ctx, query, arg)
	if err != nil {
		return 0, fmt.Errorf("%s %q: %w", op, arg, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("%s %q: rows affected: %w", op, arg, err)
	}
	return affected, nil
}

func (r *postgresOutboundDeliveries) Get(ctx context.Context, activityID, targetInbox string) (*OutboundDelivery, error) {
	query := `SELECT` + deliveryColumns + `
		FROM outbound_deliveries WHERE activity_id = $1 AND target_inbox = $2`
	delivery, err := scanOutboundDelivery(r.db.QueryRowContext(ctx, query, activityID, targetInbox))
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFoundError("outbound_delivery", activityID+" -> "+targetInbox)
		}
		return nil, fmt.Errorf("get outbound_delivery %q -> %q: %w", activityID, targetInbox, err)
	}
	return delivery, nil
}

func (r *postgresOutboundDeliveries) HasPoisonedPredecessor(ctx context.Context, orderingKey, targetInbox string, seq int64) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM outbound_deliveries
			WHERE ordering_key = $1 AND target_inbox = $2
			  AND state = 'poisoned' AND seq < $3)`,
		orderingKey, targetInbox, seq).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check poisoned predecessor on %q: %w", orderingKey, err)
	}
	return exists, nil
}

func (r *postgresOutboundDeliveries) CountsByState(ctx context.Context) (map[DeliveryState]int, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT state, COUNT(*) FROM outbound_deliveries GROUP BY state`)
	if err != nil {
		return nil, fmt.Errorf("count outbound_deliveries by state: %w", err)
	}
	defer rows.Close()

	counts := make(map[DeliveryState]int)
	for rows.Next() {
		var state string
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			return nil, fmt.Errorf("scan delivery state count: %w", err)
		}
		counts[DeliveryState(state)] = n
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate delivery state counts: %w", err)
	}
	return counts, nil
}

func (r *postgresOutboundDeliveries) RedrivePoisoned(ctx context.Context, activityID, orderingKey string) (int64, error) {
	// Empty filters pass through the NULLIF/COALESCE guard: a blank $2/$3 means
	// "any row", so the sweep can target one activity, one community, or all
	// poisoned deliveries. Reset attempts + next_attempt_at so a redriven
	// delivery gets a fresh budget and is immediately claimable.
	result, err := r.db.ExecContext(ctx, `
		UPDATE outbound_deliveries
		SET state = 'pending', attempts = 0, claimed_until = NULL, next_attempt_at = now(),
		    updated_at = now()
		WHERE state = 'poisoned'
		  AND ($1 = '' OR activity_id = $1)
		  AND ($2 = '' OR ordering_key = $2)`,
		activityID, orderingKey)
	if err != nil {
		return 0, fmt.Errorf("redrive poisoned deliveries: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("redrive poisoned deliveries: rows affected: %w", err)
	}
	return affected, nil
}

func scanOutboundDelivery(row rowScanner) (*OutboundDelivery, error) {
	var delivery OutboundDelivery
	var state string
	var claimedUntil, deliveredAt sql.NullTime
	var lastStatus sql.NullInt64
	err := row.Scan(
		&delivery.Seq, &delivery.ActivityID, &delivery.TargetInbox, &delivery.OrderingKey,
		&state, &delivery.Attempts, &delivery.NextAttemptAt, &claimedUntil, &deliveredAt,
		&lastStatus, &delivery.LastErrorClass, &delivery.ResponseExcerpt,
		&delivery.CreatedAt, &delivery.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	delivery.State = DeliveryState(state)
	if claimedUntil.Valid {
		delivery.ClaimedUntil = &claimedUntil.Time
	}
	if deliveredAt.Valid {
		delivery.DeliveredAt = &deliveredAt.Time
	}
	if lastStatus.Valid {
		code := int(lastStatus.Int64)
		delivery.LastStatusCode = &code
	}
	return &delivery, nil
}
