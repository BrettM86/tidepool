package consume

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"log/slog"
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

// DeadLetterRedriver periodically replays dead-lettered events against the
// same handlers that originally failed them. This is what makes a transient
// failure self-healing: the event lands in the DLQ, the failure clears (a
// postgres blip ends, a deploy fixes a bug), and the next pass indexes it.
type DeadLetterRedriver struct {
	queue       DeadLetterQueue
	handlers    map[string]EventHandler // keyed by consumer name
	logger      *slog.Logger
	interval    time.Duration
	batchSize   int
	maxAttempts int
}

// RedriverOption configures a DeadLetterRedriver.
type RedriverOption func(*DeadLetterRedriver)

// WithRedriveInterval overrides how often a redrive pass runs.
func WithRedriveInterval(d time.Duration) RedriverOption {
	return func(r *DeadLetterRedriver) { r.interval = d }
}

// WithRedriveBatchSize overrides how many rows one pass claims per consumer.
// A pass keeps claiming batches until the backlog drains, so this bounds one
// claim's memory, not how much a pass can clear.
func WithRedriveBatchSize(n int) RedriverOption {
	return func(r *DeadLetterRedriver) { r.batchSize = n }
}

// WithRedriveLogger overrides the logger. Defaults to slog.Default().
func WithRedriveLogger(logger *slog.Logger) RedriverOption {
	return func(r *DeadLetterRedriver) {
		if logger != nil {
			r.logger = logger
		}
	}
}

// NewDeadLetterRedriver creates a redriver over the given handlers, keyed by
// consumer name. Events that exhaust MaxRedriveAttempts stay in the table for
// manual inspection and are surfaced in the backlog counts.
func NewDeadLetterRedriver(queue DeadLetterQueue, handlers map[string]EventHandler, opts ...RedriverOption) *DeadLetterRedriver {
	r := &DeadLetterRedriver{
		queue:       queue,
		handlers:    handlers,
		logger:      slog.Default(),
		interval:    5 * time.Minute,
		batchSize:   100,
		maxAttempts: MaxRedriveAttempts,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Run redrives dead letters on an interval until ctx is cancelled, starting
// with an immediate pass at boot: a backlog that accumulated while the process
// was down must not have to wait out the first full interval.
func (r *DeadLetterRedriver) Run(ctx context.Context) {
	r.redriveAll(ctx)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("jetstream dead letter redriver stopped")
			return
		case <-ticker.C:
			r.redriveAll(ctx)
		}
	}
}

// redriveAll runs one pass across every registered consumer, DRAINING each
// consumer's retryable backlog batch by batch: a backlog larger than one batch
// must not take one tick per batch to clear, or a burst of failures during an
// outage would still be arriving hours later.
func (r *DeadLetterRedriver) redriveAll(ctx context.Context) {
	for consumerName, handler := range r.handlers {
		if ctx.Err() != nil {
			return
		}

		totalRedriven, totalFailed := 0, 0
		for {
			if ctx.Err() != nil {
				return
			}
			redriven, retired, failed, listed, err := r.redriveConsumer(ctx, consumerName, handler)
			if err != nil {
				r.logger.Error("jetstream dead letter redrive pass failed",
					slog.String("consumer", consumerName), slog.String("error", err.Error()))
				break
			}
			totalRedriven += redriven
			totalFailed += failed
			// A short batch means the retryable backlog is drained.
			if listed < r.batchSize {
				break
			}
			// A FULL batch that removed NO row from the retryable set (every
			// row was a handler failure, only marked +1) means the next
			// ListRetryable returns the SAME oldest rows. Re-selecting them
			// here would burn every row's whole redrive budget in this one
			// pass instead of one attempt per scheduled pass. Stop and let the
			// interval bring the next attempt. Rows that were redriven
			// (deleted) or retired (exhausted) DID leave the set, so forward
			// progress keeps the drain going.
			if redriven+retired == 0 {
				break
			}
		}
		if totalRedriven > 0 || totalFailed > 0 {
			r.logger.Info("jetstream dead letter redrive pass completed",
				slog.String("consumer", consumerName),
				slog.Int("redriven", totalRedriven),
				slog.Int("still_failing", totalFailed))
		}
	}
}

// redriveConsumer replays one batch of dead letters for a single consumer.
// listed reports how many rows the batch held so the caller can tell when the
// backlog is drained.
//
// The claim is scoped to ONE consumer name. A handler must only ever see rows
// filed under its own name — replaying another consumer's events through it
// would apply them wrongly — so an unregistered consumer's backlog is left
// untouched rather than handed to whoever is available.
// redriven counts rows successfully re-handled and deleted; retired counts
// rows exhausted in one step (unparseable). Both LEAVE the retryable set, which
// is how the caller tells forward progress from a batch that only burned
// attempts on rows that will be re-selected.
func (r *DeadLetterRedriver) redriveConsumer(ctx context.Context, consumerName string, handler EventHandler) (redriven, retired, failed, listed int, err error) {
	deadLetters, err := r.queue.ListRetryable(ctx, consumerName, r.maxAttempts, r.batchSize)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	listed = len(deadLetters)

	for _, deadLetter := range deadLetters {
		if ctx.Err() != nil {
			return redriven, retired, failed, listed, nil
		}

		var event JetstreamEvent
		if parseErr := json.Unmarshal(deadLetter.EventData, &event); parseErr != nil {
			// A payload that will not parse can never succeed and never
			// reaches a handler. Retire it in ONE step so it stops consuming
			// redrive passes, but KEEP the row: it is the forensic record of
			// what actually arrived.
			failed++
			if retireErr := r.queue.RetireDeadLetter(ctx, deadLetter.ID, "unparseable event: "+parseErr.Error()); retireErr != nil {
				r.logger.Error("failed to retire unparseable dead letter",
					slog.Int64("id", deadLetter.ID), slog.String("error", retireErr.Error()))
			} else {
				retired++ // exhausted, so it leaves the retryable set
			}
			continue
		}

		if handleErr := handler.HandleEvent(ctx, &event); handleErr != nil {
			// A failure caused by shutdown is not the event's fault: return
			// without burning one of its redrive attempts.
			if ctx.Err() != nil || stderrors.Is(handleErr, context.Canceled) || stderrors.Is(handleErr, context.DeadlineExceeded) {
				return redriven, retired, failed, listed, nil
			}
			failed++
			// The NEWEST error replaces the captured one, so the row explains
			// why it is STILL failing rather than why it first did.
			if markErr := r.queue.MarkRedriveAttempt(ctx, deadLetter.ID, handleErr.Error()); markErr != nil {
				r.logger.Error("failed to mark dead letter redrive attempt",
					slog.Int64("id", deadLetter.ID), slog.String("error", markErr.Error()))
			}
			continue
		}

		if deleteErr := r.queue.DeleteDeadLetter(ctx, deadLetter.ID); deleteErr != nil {
			// The event was handled but the row remains; the next pass replays
			// it, which is safe because handlers are idempotent.
			r.logger.Error("failed to delete redriven dead letter (will replay next pass)",
				slog.Int64("id", deadLetter.ID), slog.String("error", deleteErr.Error()))
			continue
		}
		redriven++
	}
	return redriven, retired, failed, listed, nil
}
