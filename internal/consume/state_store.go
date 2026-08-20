package consume

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// The SQL here is a PORT of the Coves AppView's state_store.go — the monotonic
// SaveCursor WHERE clause and the `ON CONFLICT DO NOTHING` dead-letter dedup
// above all — with the one Tidepool divergence: cursors are keyed
// (consumer_name, schema_version), not consumer_name alone.

// PostgresStateStore persists Jetstream consumer state: per-consumer cursors
// and the dead letter queue. It lives in this package rather than
// internal/store because the state is private to the consumer pipeline — no
// other domain reads these tables.
type PostgresStateStore struct {
	db *sql.DB
	// schemaVersion is the handler-contract version this store's cursor rows
	// belong to. It is fixed per store, never a per-call argument: mixing
	// versions inside one consumer run is exactly what the column exists to
	// prevent.
	schemaVersion int
}

// NewPostgresStateStore creates a store for consumer state at the given
// handler schema version.
func NewPostgresStateStore(db *sql.DB, schemaVersion int) *PostgresStateStore {
	return &PostgresStateStore{db: db, schemaVersion: schemaVersion}
}

// Compile-time interface satisfaction checks.
var (
	_ CursorStore     = (*PostgresStateStore)(nil)
	_ DeadLetterQueue = (*PostgresStateStore)(nil)
)

// GetCursor returns the persisted cursor for the consumer at this store's
// schema version, or 0 if none exists.
func (s *PostgresStateStore) GetCursor(ctx context.Context, consumerName string) (int64, error) {
	var cursorTimeUS int64
	err := s.db.QueryRowContext(ctx,
		`SELECT cursor_time_us FROM consumer_cursors
		  WHERE consumer_name = $1 AND schema_version = $2`,
		consumerName, s.schemaVersion,
	).Scan(&cursorTimeUS)
	if errors.Is(err, sql.ErrNoRows) {
		// A first run is not an error: it is a live tail. Note that a NEW
		// schema version legitimately lands here even while the production
		// version's row sits at the head of the stream — replaying from
		// scratch is the whole reason the version bump happened.
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get cursor for %s (schema %d): %w", consumerName, s.schemaVersion, err)
	}
	return cursorTimeUS, nil
}

// SaveCursor upserts the cursor for the consumer at this store's schema
// version. Monotonic: a smaller value than the stored one is a no-op.
func (s *PostgresStateStore) SaveCursor(ctx context.Context, consumerName string, cursorTimeUS int64) error {
	// The WHERE on the DO UPDATE is the monotonicity guard: an out-of-order
	// flush (a late goroutine, a reconnect rewind) silently does nothing
	// rather than walking the cursor backwards over events already accounted
	// for — or un-advancing past a poison frame the DLQ already absorbed.
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO consumer_cursors (consumer_name, schema_version, cursor_time_us, updated_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (consumer_name, schema_version) DO UPDATE
		SET cursor_time_us = EXCLUDED.cursor_time_us, updated_at = now()
		WHERE consumer_cursors.cursor_time_us < EXCLUDED.cursor_time_us`,
		consumerName, s.schemaVersion, cursorTimeUS,
	)
	if err != nil {
		return fmt.Errorf("save cursor for %s (schema %d): %w", consumerName, s.schemaVersion, err)
	}
	return nil
}

// AddDeadLetter stores a failed event for later redrive. Re-adding an
// already-captured event is a no-op success (the dedup index absorbs it) so
// the cursor may advance past a poison frame.
func (s *PostgresStateStore) AddDeadLetter(ctx context.Context, consumerName string, eventTimeUS int64, eventData []byte, handleErr string, redriveAttempts int) error {
	// event_data is written as raw bytes so byte-corrupt frames are capturable.
	// last_error is TEXT, so the write goes through execDeadLetterWrite, which
	// sanitizes it at the last point before the SQL — see the comment there for
	// why every writer of this column must.
	//
	// redriveAttempts seeds the budget: 0 for transient failures, and
	// MaxRedriveAttempts for permanent ones, which are kept for forensics only.
	err := s.execDeadLetterWrite(ctx, `
		INSERT INTO jetstream_dead_letters (consumer_name, event_time_us, event_data, attempts, last_error)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT DO NOTHING`,
		consumerName, eventTimeUS, eventData, redriveAttempts, handleErr,
	)
	if err != nil {
		return fmt.Errorf("add dead letter for %s: %w", consumerName, err)
	}
	return nil
}

// execDeadLetterWrite runs a statement against jetstream_dead_letters whose
// LAST positional argument is the last_error value, and sanitizes that value
// here — one funnel, at the last point before the SQL.
//
// The funnel exists because sanitizing at the CALL SITES did not hold. Three
// statements write this column and only the INSERT was scrubbed; the two
// UPDATEs on the redrive path passed the string straight through. A NUL there
// does not merely lose a diagnostic — it fails the UPDATE, so `attempts` never
// increments, the row can never reach MaxRedriveAttempts, and it is re-selected
// and fully re-handled on every redrive pass forever while redriveAll's
// forward-progress guard parks the rest of the drain behind it. A fourth writer
// reaching for s.db.ExecContext directly would reopen exactly that hole, so
// last_error is written through here and nowhere else.
func (s *PostgresStateStore) execDeadLetterWrite(ctx context.Context, query string, args ...any) error {
	last := len(args) - 1
	if last < 0 {
		return fmt.Errorf("dead letter write: no arguments, so no last_error to sanitize")
	}
	errorText, ok := args[last].(string)
	if !ok {
		// A mistake in this file, not a runtime condition: the convention the
		// funnel enforces is "the last argument is last_error".
		return fmt.Errorf("dead letter write: last argument must be the last_error string, got %T", args[last])
	}
	args[last] = sanitizeErrorText(errorText)
	_, err := s.db.ExecContext(ctx, query, args...)
	return err
}

// Bounds on what one dead letter's diagnostic may cost. last_error is
// operator-facing EVIDENCE, not a transcript: the errors that reach it can
// quote a body fetched from a host a stranger named, and a redriven row
// rewrites the column once per pass.
const (
	maxLastErrorBytes         = 4096
	lastErrorTruncationMarker = " …(truncated)"
)

// sanitizeErrorText makes an error string safe for a postgres TEXT column
// while keeping it readable. NUL bytes are stripped (postgres rejects them
// outright) and any remaining invalid UTF-8 is coerced to the replacement
// rune, so an operator can still read the diagnostic out of the DLQ. Scrubbing
// the bad bytes, not discarding the message.
//
// The result is also capped, keeping the HEAD: what identifies a failure is the
// front of its message, and the tail is where an echoed remote body would sit.
func sanitizeErrorText(s string) string {
	if s == "" {
		return s
	}
	s = strings.ReplaceAll(s, "\x00", "")
	truncated := len(s) > maxLastErrorBytes
	if truncated {
		// Cut first, validate second: the cut can land mid-rune, and
		// ToValidUTF8 then turns that trailing fragment into the replacement
		// rune rather than leaving bytes postgres would reject.
		s = s[:maxLastErrorBytes]
	}
	s = strings.ToValidUTF8(s, "�")
	if truncated {
		s += lastErrorTruncationMarker
	}
	return s
}

// ListRetryable returns up to limit dead letters for the consumer that have
// not exhausted their redrive attempts, oldest first.
func (s *PostgresStateStore) ListRetryable(ctx context.Context, consumerName string, maxAttempts, limit int) ([]DeadLetterEvent, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, consumer_name, event_time_us, event_data, last_error, attempts, created_at, updated_at
		FROM jetstream_dead_letters
		WHERE consumer_name = $1 AND attempts < $2
		ORDER BY id ASC
		LIMIT $3`,
		consumerName, maxAttempts, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list retryable dead letters for %s: %w", consumerName, err)
	}
	defer func() {
		_ = rows.Close() // iteration errors surface via rows.Err()
	}()

	var deadLetters []DeadLetterEvent
	for rows.Next() {
		var deadLetter DeadLetterEvent
		if err := rows.Scan(
			&deadLetter.ID,
			&deadLetter.ConsumerName,
			&deadLetter.EventTimeUS,
			&deadLetter.EventData,
			&deadLetter.LastError,
			&deadLetter.Attempts,
			&deadLetter.CreatedAt,
			&deadLetter.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan dead letter: %w", err)
		}
		deadLetters = append(deadLetters, deadLetter)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dead letters for %s: %w", consumerName, err)
	}
	return deadLetters, nil
}

// DeleteDeadLetter removes a successfully redriven event.
func (s *PostgresStateStore) DeleteDeadLetter(ctx context.Context, id int64) error {
	// A missing row is success: the redriver may re-run a pass, and the end
	// state it wants — the event gone from the queue — already holds.
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM jetstream_dead_letters WHERE id = $1`, id); err != nil {
		return fmt.Errorf("delete dead letter %d: %w", id, err)
	}
	return nil
}

// MarkRedriveAttempt increments the attempt counter after a failed redrive.
func (s *PostgresStateStore) MarkRedriveAttempt(ctx context.Context, id int64, handleErr string) error {
	// The counter and the error ride ONE statement, through the sanitizing
	// funnel: this write is what makes a row's budget finite, so a diagnostic
	// the column cannot hold must never be able to take the increment down
	// with it.
	err := s.execDeadLetterWrite(ctx, `
		UPDATE jetstream_dead_letters
		SET attempts = attempts + 1, last_error = $2, updated_at = now()
		WHERE id = $1`,
		id, handleErr,
	)
	if err != nil {
		return fmt.Errorf("mark redrive attempt on dead letter %d: %w", id, err)
	}
	return nil
}

// RetireDeadLetter exhausts a dead letter's redrive budget in one step.
func (s *PostgresStateStore) RetireDeadLetter(ctx context.Context, id int64, reason string) error {
	// GREATEST, and a jump straight to MaxRedriveAttempts: an event that can
	// never succeed (an unparseable frame, a lexicon rejection) must stop
	// costing redrive passes after ONE call, not after the whole budget is
	// burnt down one attempt at a time. The row STAYS — retiring is about the
	// redriver, not about forgetting.
	//
	// Through the same funnel, and for the same reason: the reason string
	// carries the parse error of a frame that would not parse, which is exactly
	// the frame whose bytes the TEXT column cannot hold.
	err := s.execDeadLetterWrite(ctx, `
		UPDATE jetstream_dead_letters
		SET attempts = GREATEST(attempts, $2), last_error = $3, updated_at = now()
		WHERE id = $1`,
		id, MaxRedriveAttempts, reason,
	)
	if err != nil {
		return fmt.Errorf("retire dead letter %d: %w", id, err)
	}
	return nil
}

// CountDeadLetters returns the dead letter backlog per consumer.
func (s *PostgresStateStore) CountDeadLetters(ctx context.Context) (map[string]int64, error) {
	// Exhausted rows are counted too: the redriver ignores them, but an
	// operator watching the backlog must still see them.
	rows, err := s.db.QueryContext(ctx, `
		SELECT consumer_name, COUNT(*)
		FROM jetstream_dead_letters
		GROUP BY consumer_name`,
	)
	if err != nil {
		return nil, fmt.Errorf("count dead letters: %w", err)
	}
	defer func() {
		_ = rows.Close() // iteration errors surface via rows.Err()
	}()

	counts := make(map[string]int64)
	for rows.Next() {
		var consumerName string
		var count int64
		if err := rows.Scan(&consumerName, &count); err != nil {
			return nil, fmt.Errorf("scan dead letter count: %w", err)
		}
		counts[consumerName] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dead letter counts: %w", err)
	}
	return counts, nil
}
