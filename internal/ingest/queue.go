package ingest

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// Queue defaults, overridable through QueueOptions.
const (
	defaultWorkers        = 4
	defaultMaxAttempts    = 5
	defaultRetryBaseDelay = 30 * time.Second
	defaultMaxRetryDelay  = time.Hour
	defaultLease          = 2 * time.Minute
	defaultPollInterval   = time.Second
	// bookkeepingTimeout bounds the outcome-recording writes, which run on a
	// context detached from the worker's (see processNext) so a completed
	// event is durably acked even as the pool is shutting down.
	bookkeepingTimeout = 10 * time.Second
)

// Processor consumes one claimed inbox event. *Handler implements it.
// Return contract: nil or IsSkip → processed; IsValidation → poisoned;
// anything else → retried with backoff until the attempt cap, then
// poisoned.
type Processor interface {
	Process(ctx context.Context, event *store.InboxEvent) error
}

// QueueOptions configures NewQueue. Events and Processor are required.
type QueueOptions struct {
	Events    store.InboxEvents
	Processor Processor
	// Workers is the pool size (default 4). Ordering stays correct at any
	// size: ClaimNext never hands out an event whose ordering key has an
	// older pending sibling.
	Workers int
	// MaxAttempts poisons an event after this many failed claims
	// (default 5).
	MaxAttempts int
	// RetryBaseDelay is the first backoff step, doubling per attempt up to
	// an hour (default 30s).
	RetryBaseDelay time.Duration
	// Lease is how long a claim lasts before a crashed worker's event
	// becomes claimable again; it also bounds one event's processing time
	// (default 2m).
	Lease time.Duration
	// PollInterval is the idle re-check period (default 1s). The inbox also
	// nudges the pool on every enqueue, so this is only the fallback for
	// retries and missed nudges.
	PollInterval time.Duration
	Logger       *slog.Logger
}

// Queue is the durable ingestion work loop: a worker pool draining
// inbox_events (the postgres-backed queue — no external broker), with
// per-community serial ordering, retry with backoff, and poison handling.
type Queue struct {
	events         store.InboxEvents
	processor      Processor
	workers        int
	maxAttempts    int
	retryBaseDelay time.Duration
	lease          time.Duration
	pollInterval   time.Duration
	logger         *slog.Logger

	nudge chan struct{}
	// now is a test seam for backoff scheduling.
	now func() time.Time
}

// NewQueue validates options and builds a Queue.
func NewQueue(opts QueueOptions) (*Queue, error) {
	if opts.Events == nil {
		return nil, errors.NewValidationError("events", "must not be nil")
	}
	if opts.Processor == nil {
		return nil, errors.NewValidationError("processor", "must not be nil")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	q := &Queue{
		events:         opts.Events,
		processor:      opts.Processor,
		workers:        opts.Workers,
		maxAttempts:    opts.MaxAttempts,
		retryBaseDelay: opts.RetryBaseDelay,
		lease:          opts.Lease,
		pollInterval:   opts.PollInterval,
		logger:         logger,
		nudge:          make(chan struct{}, 1),
		now:            time.Now,
	}
	if q.workers <= 0 {
		q.workers = defaultWorkers
	}
	if q.maxAttempts <= 0 {
		q.maxAttempts = defaultMaxAttempts
	}
	if q.retryBaseDelay <= 0 {
		q.retryBaseDelay = defaultRetryBaseDelay
	}
	if q.lease <= 0 {
		q.lease = defaultLease
	}
	if q.pollInterval <= 0 {
		q.pollInterval = defaultPollInterval
	}
	return q, nil
}

// Nudge wakes the worker pool without waiting for the poll interval. The
// inbox calls it after every enqueue (same process, no broker round-trip).
func (q *Queue) Nudge() {
	select {
	case q.nudge <- struct{}{}:
	default:
	}
}

// Run drives the worker pool until ctx is cancelled.
func (q *Queue) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < q.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			q.workerLoop(ctx)
		}()
	}
	wg.Wait()
}

// workerLoop drains the queue, then sleeps until a nudge or the poll tick.
func (q *Queue) workerLoop(ctx context.Context) {
	ticker := time.NewTicker(q.pollInterval)
	defer ticker.Stop()
	for {
		// Drain: keep claiming while there is work.
		for {
			if ctx.Err() != nil {
				return
			}
			claimed := q.processNext(ctx)
			if !claimed {
				break
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-q.nudge:
		case <-ticker.C:
		}
	}
}

// processNext claims and processes one event, reporting whether anything
// was claimed (false = queue idle). Every outcome is recorded on the row;
// bookkeeping failures are logged, never fatal — the lease expiry
// re-delivers the event.
func (q *Queue) processNext(ctx context.Context) bool {
	event, err := q.events.ClaimNext(ctx, q.lease)
	if err != nil {
		if !errors.IsNotFound(err) && ctx.Err() == nil {
			q.logger.Error("claim next inbox event", "error", err)
		}
		return false
	}

	// Cascade the wakeup: this worker is now busy, so nudge an idle peer to
	// claim the next event instead of leaving a burst of enqueues to drain
	// one worker at a time. ClaimNext's NotFound path (empty queue) does not
	// nudge, so this self-terminates.
	q.Nudge()

	// The lease ClaimNext stamped is this attempt's fencing token; the
	// outcome writes below present it so a stale worker cannot clobber a
	// re-claim.
	token := claimToken(event)

	// Bound processing by the lease: a worker must not outlive its claim,
	// or a second worker could process the same event concurrently.
	procCtx, cancel := context.WithTimeout(ctx, q.lease)
	err = q.processor.Process(procCtx, event)
	cancel()

	// Shutdown is not a processing failure. If the parent context was
	// cancelled, a non-nil err is (almost certainly) cancellation, not a
	// real fault: do not classify it — leave the row untouched so the lease
	// lapses and the event is redelivered intact, rather than burning the
	// attempt into a retry/poison. A successful Process (err == nil) still
	// falls through so its outcome is durably acked below.
	if err != nil && ctx.Err() != nil {
		q.logger.Info("shutdown interrupted inbox event processing; leaving for redelivery",
			"activity_id", event.ActivityID, "type", event.Type)
		return true
	}

	// Record the outcome on a context detached from the worker's, so an
	// event that just completed is durably acked even though shutdown has
	// cancelled ctx. Bounded so a stuck DB cannot hang shutdown forever.
	bookCtx, bookCancel := context.WithTimeout(context.WithoutCancel(ctx), bookkeepingTimeout)
	defer bookCancel()

	switch {
	case err == nil:
		q.finish(bookCtx, event, token)
	case materialize.IsSkip(err):
		// Deliberate drops: log the reason, mark processed, never retry.
		q.logger.Info("inbox event skipped",
			"activity_id", event.ActivityID, "type", event.Type, "reason", err.Error())
		q.finish(bookCtx, event, token)
	case errors.IsValidation(err):
		// Malformed payloads never get better; poison immediately.
		q.logger.Warn("inbox event poisoned (invalid payload)",
			"activity_id", event.ActivityID, "type", event.Type, "error", err)
		q.poison(bookCtx, event, token, err.Error())
	case event.Attempts >= q.maxAttempts:
		q.logger.Error("inbox event poisoned (attempt cap reached)",
			"activity_id", event.ActivityID, "type", event.Type,
			"attempts", event.Attempts, "error", err)
		q.poison(bookCtx, event, token, err.Error())
	default:
		delay := q.backoff(event.Attempts)
		q.logger.Warn("inbox event failed; scheduling retry",
			"activity_id", event.ActivityID, "type", event.Type,
			"attempt", event.Attempts, "retry_in", delay, "error", err)
		applied, rerr := q.events.Release(bookCtx, event.ActivityID, err.Error(), q.now().Add(delay), token)
		if rerr != nil {
			q.logger.Error("release inbox event", "activity_id", event.ActivityID, "error", rerr)
		} else if !applied {
			q.logger.Warn("stale claim; retry release discarded", "activity_id", event.ActivityID)
		}
	}
	return true
}

func (q *Queue) finish(ctx context.Context, event *store.InboxEvent, token time.Time) {
	applied, err := q.events.MarkProcessed(ctx, event.ActivityID, token)
	if err != nil {
		q.logger.Error("mark inbox event processed", "activity_id", event.ActivityID, "error", err)
		return
	}
	if !applied {
		q.logger.Warn("stale claim; processed outcome discarded", "activity_id", event.ActivityID)
	}
}

func (q *Queue) poison(ctx context.Context, event *store.InboxEvent, token time.Time, message string) {
	applied, err := q.events.MarkPoisoned(ctx, event.ActivityID, message, token)
	if err != nil {
		q.logger.Error("poison inbox event", "activity_id", event.ActivityID, "error", err)
		return
	}
	if !applied {
		q.logger.Warn("stale claim; poison outcome discarded", "activity_id", event.ActivityID)
	}
}

// claimToken extracts the fencing token (the lease deadline ClaimNext
// stamped) from a freshly claimed event. A claimed row always has
// ClaimedUntil set; the zero fallback is defensive and never matches a live
// row, so a would-be write is a safe no-op.
func claimToken(event *store.InboxEvent) time.Time {
	if event.ClaimedUntil == nil {
		return time.Time{}
	}
	return *event.ClaimedUntil
}

// backoff doubles the base delay per completed attempt, capped at an hour.
func (q *Queue) backoff(attempts int) time.Duration {
	delay := q.retryBaseDelay
	for i := 1; i < attempts && delay < defaultMaxRetryDelay; i++ {
		delay *= 2
	}
	if delay > defaultMaxRetryDelay {
		delay = defaultMaxRetryDelay
	}
	return delay
}
