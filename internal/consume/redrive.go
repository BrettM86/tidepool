package consume

import (
	"context"
	"time"
)

// MaxRedriveAttempts is the redrive budget for a dead letter. Rows at or above
// this many attempts are skipped by the redriver but stay in the table for
// manual inspection and still count toward the backlog. The connector
// dead-letters permanent failures (ErrPermanentEvent) with attempts already at
// this value so the redriver never touches them.
const MaxRedriveAttempts = 10

// DeadLetterEvent is a dead-lettered Jetstream event awaiting redrive.
type DeadLetterEvent struct {
	ID           int64
	ConsumerName string
	EventTimeUS  int64
	EventData    []byte
	LastError    string
	Attempts     int
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// DeadLetterQueue is the full dead letter store used by the redriver.
// Connectors only need the narrower DeadLetterWriter.
type DeadLetterQueue interface {
	DeadLetterWriter

	// ListRetryable returns up to limit dead letters for the consumer with
	// fewer than maxAttempts redrive attempts, oldest first.
	ListRetryable(ctx context.Context, consumerName string, maxAttempts, limit int) ([]DeadLetterEvent, error)

	// DeleteDeadLetter removes a successfully redriven event.
	DeleteDeadLetter(ctx context.Context, id int64) error

	// MarkRedriveAttempt increments the attempt counter after a failed
	// redrive and records the error.
	MarkRedriveAttempt(ctx context.Context, id int64, handleErr string) error

	// RetireDeadLetter marks a dead letter permanently exhausted in ONE step
	// (attempts jump straight to MaxRedriveAttempts) so it stops consuming
	// redrive passes. The row stays for forensics and stays in the backlog
	// count.
	RetireDeadLetter(ctx context.Context, id int64, reason string) error

	// CountDeadLetters returns the dead letter backlog per consumer.
	CountDeadLetters(ctx context.Context) (map[string]int64, error)
}
