package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"time"

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
	return r.Enqueue(ctx, InboxEvent{ActivityID: activityID, Type: activityType})
}

func (r *postgresInboxEvents) Enqueue(ctx context.Context, event InboxEvent) (bool, error) {
	if event.ActivityID == "" {
		return false, errors.NewValidationError("activity_id", "must not be empty")
	}
	if event.Type == "" {
		return false, errors.NewValidationError("type", "must not be empty")
	}

	query := `
		INSERT INTO inbox_events (activity_id, type, payload, actor_id, ordering_key)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (activity_id) DO NOTHING`

	result, err := r.db.ExecContext(ctx, query,
		event.ActivityID, event.Type, event.Payload, event.ActorID, event.OrderingKey)
	if err != nil {
		return false, fmt.Errorf("enqueue inbox event %q: %w", event.ActivityID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("enqueue inbox event %q: rows affected: %w", event.ActivityID, err)
	}
	return affected == 1, nil
}

// eventColumns is the SELECT list shared by every query that scans a full
// InboxEvent row.
const eventColumns = `id, activity_id, type, COALESCE(payload, ''::bytea), actor_id,
	ordering_key, attempts, next_attempt_at, claimed_until, failed_at,
	received_at, processed_at, COALESCE(error, '')`

func (r *postgresInboxEvents) ClaimNext(ctx context.Context, lease time.Duration) (*InboxEvent, error) {
	if lease <= 0 {
		return nil, errors.NewValidationError("lease", "must be positive")
	}

	// The candidate is the oldest processable event: unprocessed, not
	// poisoned, past its retry schedule, unleased (or the lease expired),
	// and with NO older unprocessed, unpoisoned sibling on the same
	// ordering key — the per-community serialization: while an older event
	// is pending (claimed, backing off, or simply queued), every younger
	// event on that key is invisible to workers. A poisoned sibling stops
	// blocking (poison → skip).
	//
	// "No older pending sibling" is equivalent to "is the min-id pending
	// row of its ordering key", so instead of scanning pending rows in id
	// order and probing NOT EXISTS per row — O(backlog) whenever one
	// community's queue backs up behind a failing head — the recursive CTE
	// emulates a loose index scan over idx_inbox_events_queue
	// (ordering_key, id, partial on pending): one index descent per
	// DISTINCT pending key jumps straight to each key's head, and only
	// those heads are filtered for claimability. Work is O(pending keys ×
	// log N) regardless of any one key's backlog depth.
	//
	// The heads are materialized with ARRAY(...) — not a plain IN — so the
	// planner fetches exactly those rows by primary key (a merge/semi join
	// against an inlined CTE was observed walking the pkey through the
	// whole backlog again). The outer SELECT re-applies every claimability
	// condition on the locked row: under READ COMMITTED the row is
	// re-evaluated after the lock is acquired, so a claim committed between
	// the CTE's snapshot and the lock is seen and the row skipped. The
	// head-of-its-key property itself needs no re-check — ids only grow and
	// processed_at/failed_at are never unset, so a key's pending-min is
	// stable once observed (one caveat: id assignment order is not
	// commit-visibility order, so a smaller-id enqueue can become visible
	// after the claim's snapshot and retroactively lower a key's
	// pending-min; the previous NOT EXISTS query had the identical
	// single-snapshot blind spot, so this changes nothing about the claim
	// semantics). FOR UPDATE SKIP LOCKED lets concurrent workers
	// race without serializing on row locks; the claiming UPDATE stamps the
	// lease and counts the attempt atomically.
	query := `
		UPDATE inbox_events
		SET claimed_until = CURRENT_TIMESTAMP + make_interval(secs => $1),
		    attempts = attempts + 1
		WHERE id = (
			SELECT c.id FROM inbox_events c
			WHERE c.id = ANY (ARRAY(
				WITH RECURSIVE key_heads AS (
					SELECT h.id, h.ordering_key FROM (
						SELECT e.id, e.ordering_key
						FROM inbox_events e
						WHERE e.processed_at IS NULL AND e.failed_at IS NULL
						ORDER BY e.ordering_key, e.id
						LIMIT 1
					) h
					UNION ALL
					SELECT n.id, n.ordering_key FROM key_heads k
					CROSS JOIN LATERAL (
						SELECT e.id, e.ordering_key
						FROM inbox_events e
						WHERE e.processed_at IS NULL AND e.failed_at IS NULL
						  AND e.ordering_key > k.ordering_key
						ORDER BY e.ordering_key, e.id
						LIMIT 1
					) n
				)
				SELECT id FROM key_heads))
			  AND c.processed_at IS NULL
			  AND c.failed_at IS NULL
			  AND c.next_attempt_at <= CURRENT_TIMESTAMP
			  AND (c.claimed_until IS NULL OR c.claimed_until <= CURRENT_TIMESTAMP)
			ORDER BY c.id
			LIMIT 1
			FOR UPDATE OF c SKIP LOCKED)
		RETURNING ` + eventColumns

	event, err := scanInboxEvent(r.db.QueryRowContext(ctx, query, lease.Seconds()))
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFoundError("inbox_event", "next claimable")
		}
		return nil, fmt.Errorf("claim next inbox event: %w", err)
	}
	return event, nil
}

func (r *postgresInboxEvents) MarkProcessed(ctx context.Context, activityID string, claimToken time.Time) (bool, error) {
	// Fencing: only the worker that still holds the claim (claimed_until ==
	// claimToken, the value ClaimNext stamped) may record the outcome. A
	// stale worker whose lease expired and was re-claimed by a second worker
	// no longer matches claimed_until, writes 0 rows, and is reported as a
	// no-op (applied=false) so it cannot clobber the newer attempt. The
	// failed_at guard keeps processed and poisoned mutually exclusive, so a
	// late finish can never stamp processed_at onto a poisoned row.
	query := `
		WITH updated AS (
			UPDATE inbox_events
			SET processed_at = CURRENT_TIMESTAMP, error = NULL, claimed_until = NULL
			WHERE activity_id = $1
			  AND processed_at IS NULL
			  AND failed_at IS NULL
			  AND claimed_until = $2
			RETURNING 1
		)
		SELECT
			EXISTS (SELECT 1 FROM inbox_events WHERE activity_id = $1),
			EXISTS (SELECT 1 FROM updated)`

	var exists, applied bool
	if err := r.db.QueryRowContext(ctx, query, activityID, claimToken).Scan(&exists, &applied); err != nil {
		return false, fmt.Errorf("mark inbox event %q processed: %w", activityID, err)
	}
	if !exists {
		return false, errors.NewNotFoundError("inbox_event", activityID)
	}
	return applied, nil
}

func (r *postgresInboxEvents) Release(ctx context.Context, activityID, message string, nextAttempt time.Time, claimToken time.Time) (bool, error) {
	if message == "" {
		return false, errors.NewValidationError("error", "must not be empty")
	}

	// Fencing + already-processed guard: only the current claim holder may
	// reschedule (claimed_until == claimToken), and a stale worker's late
	// release must not reschedule an event a successful retry completed
	// (processed_at IS NULL). A mismatch writes 0 rows → applied=false.
	query := `
		WITH updated AS (
			UPDATE inbox_events
			SET error = $2, claimed_until = NULL, next_attempt_at = $3
			WHERE activity_id = $1 AND processed_at IS NULL AND claimed_until = $4
			RETURNING 1
		)
		SELECT
			EXISTS (SELECT 1 FROM inbox_events WHERE activity_id = $1),
			EXISTS (SELECT 1 FROM updated)`

	var exists, applied bool
	if err := r.db.QueryRowContext(ctx, query, activityID, message, nextAttempt.UTC(), claimToken).Scan(&exists, &applied); err != nil {
		return false, fmt.Errorf("release inbox event %q: %w", activityID, err)
	}
	if !exists {
		return false, errors.NewNotFoundError("inbox_event", activityID)
	}
	return applied, nil
}

func (r *postgresInboxEvents) MarkPoisoned(ctx context.Context, activityID, message string, claimToken time.Time) (bool, error) {
	if message == "" {
		return false, errors.NewValidationError("error", "must not be empty")
	}

	// Fencing + already-processed guard: only the current claim holder may
	// poison (claimed_until == claimToken), and a stale worker must not
	// poison an event a successful retry completed (processed_at IS NULL). A
	// mismatch writes 0 rows → applied=false.
	query := `
		WITH updated AS (
			UPDATE inbox_events
			SET error = $2, claimed_until = NULL,
			    failed_at = COALESCE(failed_at, CURRENT_TIMESTAMP)
			WHERE activity_id = $1 AND processed_at IS NULL AND claimed_until = $3
			RETURNING 1
		)
		SELECT
			EXISTS (SELECT 1 FROM inbox_events WHERE activity_id = $1),
			EXISTS (SELECT 1 FROM updated)`

	var exists, applied bool
	if err := r.db.QueryRowContext(ctx, query, activityID, message, claimToken).Scan(&exists, &applied); err != nil {
		return false, fmt.Errorf("poison inbox event %q: %w", activityID, err)
	}
	if !exists {
		return false, errors.NewNotFoundError("inbox_event", activityID)
	}
	return applied, nil
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
	query := `SELECT ` + eventColumns + ` FROM inbox_events WHERE activity_id = $1`

	event, err := scanInboxEvent(r.db.QueryRowContext(ctx, query, activityID))
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFoundError("inbox_event", activityID)
		}
		return nil, fmt.Errorf("get inbox event %q: %w", activityID, err)
	}
	return event, nil
}

func scanInboxEvent(row rowScanner) (*InboxEvent, error) {
	var event InboxEvent
	err := row.Scan(
		&event.ID, &event.ActivityID, &event.Type, &event.Payload, &event.ActorID,
		&event.OrderingKey, &event.Attempts, &event.NextAttemptAt, &event.ClaimedUntil,
		&event.FailedAt, &event.ReceivedAt, &event.ProcessedAt, &event.Error,
	)
	if err != nil {
		return nil, err
	}
	if len(event.Payload) == 0 {
		event.Payload = nil
	}
	return &event, nil
}
