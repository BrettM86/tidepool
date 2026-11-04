package ingest

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/identity"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// queueHarness is a lighter setup for queue-semantics tests: real
// inbox_events store, scriptable processor.
type queueHarness struct {
	database *sql.DB
	events   store.InboxEvents
}

func newQueueHarness(t *testing.T) *queueHarness {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database, "inbox_events")
	return &queueHarness{database: database, events: store.NewInboxEvents(database)}
}

func (qh *queueHarness) enqueue(t *testing.T, id, orderingKey string) {
	t.Helper()
	isNew, err := qh.events.Enqueue(context.Background(), store.InboxEvent{
		ActivityID:  id,
		Type:        "Announce",
		Payload:     []byte(`{"id":"` + id + `","type":"Announce"}`),
		ActorID:     "https://lemmy.world/c/x",
		OrderingKey: orderingKey,
	})
	require.NoError(t, err)
	require.True(t, isNew)
}

// TestQueueOrderingPerKey: events sharing an ordering key process strictly
// serially and in arrival order; different keys interleave freely.
func TestQueueOrderingPerKey(t *testing.T) {
	qh := newQueueHarness(t)
	ctx := context.Background()
	qh.enqueue(t, "a-1", "community-a")
	qh.enqueue(t, "a-2", "community-a")
	qh.enqueue(t, "b-1", "community-b")

	// First claim: the oldest event overall.
	first, err := qh.events.ClaimNext(ctx, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, "a-1", first.ActivityID)
	assert.Equal(t, 1, first.Attempts)

	// While a-1 is in flight, a-2 (same key) is invisible; b-1 (other key)
	// is claimable.
	second, err := qh.events.ClaimNext(ctx, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, "b-1", second.ActivityID,
		"a community's next event must wait for the in-flight one; other communities proceed")

	// Queue is now drained from a claimer's perspective.
	_, err = qh.events.ClaimNext(ctx, time.Minute)
	assert.True(t, errors.IsNotFound(err))

	// Completing a-1 releases a-2. The claim holder presents its fencing
	// token (the ClaimedUntil ClaimNext stamped).
	require.NotNil(t, first.ClaimedUntil)
	applied, err := qh.events.MarkProcessed(ctx, "a-1", *first.ClaimedUntil)
	require.NoError(t, err)
	require.True(t, applied)
	third, err := qh.events.ClaimNext(ctx, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, "a-2", third.ActivityID)
}

// TestQueueRetryBackoff: a released event is invisible until its
// next_attempt_at, then claimable again with the attempt count preserved.
func TestQueueRetryBackoff(t *testing.T) {
	qh := newQueueHarness(t)
	ctx := context.Background()
	qh.enqueue(t, "retry-1", "community-a")

	event, err := qh.events.ClaimNext(ctx, time.Minute)
	require.NoError(t, err)
	require.Equal(t, 1, event.Attempts)
	require.NotNil(t, event.ClaimedUntil)

	// Fail with a short (but comfortably-margined) backoff: releasing
	// requires the claim's fencing token. Not claimable until it elapses...
	backoff := 150 * time.Millisecond
	applied, err := qh.events.Release(ctx, "retry-1", "remote 503", time.Now().Add(backoff), *event.ClaimedUntil)
	require.NoError(t, err)
	require.True(t, applied)
	_, err = qh.events.ClaimNext(ctx, time.Minute)
	assert.True(t, errors.IsNotFound(err), "backed-off events must not be claimable")

	// ...and it still blocks younger events on the same key (serial order).
	qh.enqueue(t, "retry-2", "community-a")
	_, err = qh.events.ClaimNext(ctx, time.Minute)
	assert.True(t, errors.IsNotFound(err),
		"a backing-off event must keep blocking its ordering key")

	// Once the backoff elapses it is claimable again, attempts accumulate,
	// and the last failure is recorded.
	time.Sleep(2 * backoff)
	event, err = qh.events.ClaimNext(ctx, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, "retry-1", event.ActivityID)
	assert.Equal(t, 2, event.Attempts)
	assert.Equal(t, "remote 503", event.Error, "the last failure is recorded")
}

// TestQueueFencingToken: a worker whose lease expired and was re-claimed by
// a second worker cannot overwrite the newer attempt's outcome. This is the
// correctness guarantee task 07's vote arithmetic depends on.
func TestQueueFencingToken(t *testing.T) {
	qh := newQueueHarness(t)
	ctx := context.Background()
	qh.enqueue(t, "fence-1", "community-a")

	// Worker A claims with a short lease, then its lease expires.
	claimA, err := qh.events.ClaimNext(ctx, 60*time.Millisecond)
	require.NoError(t, err)
	require.NotNil(t, claimA.ClaimedUntil)
	tokenA := *claimA.ClaimedUntil

	time.Sleep(120 * time.Millisecond)

	// Worker B re-claims the same event (a new attempt, a new token).
	claimB, err := qh.events.ClaimNext(ctx, time.Minute)
	require.NoError(t, err)
	require.Equal(t, "fence-1", claimB.ActivityID)
	require.NotNil(t, claimB.ClaimedUntil)
	tokenB := *claimB.ClaimedUntil
	assert.Equal(t, 2, claimB.Attempts)
	assert.False(t, tokenB.Equal(tokenA), "re-claim must mint a distinct token")

	// A's stale outcomes are all no-ops: it no longer holds the claim.
	applied, err := qh.events.MarkProcessed(ctx, "fence-1", tokenA)
	require.NoError(t, err)
	assert.False(t, applied, "stale MarkProcessed must be discarded")

	applied, err = qh.events.Release(ctx, "fence-1", "A late failure", time.Now().Add(time.Hour), tokenA)
	require.NoError(t, err)
	assert.False(t, applied, "stale Release must be discarded")

	applied, err = qh.events.MarkPoisoned(ctx, "fence-1", "A late poison", tokenA)
	require.NoError(t, err)
	assert.False(t, applied, "stale MarkPoisoned must be discarded")

	// B still holds the event: A's writes changed nothing.
	stored, err := qh.events.GetEvent(ctx, "fence-1")
	require.NoError(t, err)
	assert.Nil(t, stored.ProcessedAt)
	assert.Nil(t, stored.FailedAt)
	assert.Empty(t, stored.Error)

	// B's outcome wins.
	applied, err = qh.events.MarkProcessed(ctx, "fence-1", tokenB)
	require.NoError(t, err)
	assert.True(t, applied, "the current claim holder's outcome must be applied")

	// And A cannot un-process what B completed.
	applied, err = qh.events.Release(ctx, "fence-1", "A even later", time.Now().Add(time.Hour), tokenA)
	require.NoError(t, err)
	assert.False(t, applied)
	stored, err = qh.events.GetEvent(ctx, "fence-1")
	require.NoError(t, err)
	assert.NotNil(t, stored.ProcessedAt, "B's success must stand")
}

// TestQueueMarkProcessedAndPoisonMutuallyExclusive: a stale finish cannot
// stamp processed_at onto a poisoned row (the failed_at guard), keeping the
// two terminal states disjoint for ops/metrics.
func TestQueueMarkProcessedAndPoisonMutuallyExclusive(t *testing.T) {
	qh := newQueueHarness(t)
	ctx := context.Background()
	qh.enqueue(t, "excl-1", "community-a")

	claim, err := qh.events.ClaimNext(ctx, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claim.ClaimedUntil)
	token := *claim.ClaimedUntil

	applied, err := qh.events.MarkPoisoned(ctx, "excl-1", "bad payload", token)
	require.NoError(t, err)
	require.True(t, applied)

	// A late finish (same worker, same token) must not overwrite the poison.
	applied, err = qh.events.MarkProcessed(ctx, "excl-1", token)
	require.NoError(t, err)
	assert.False(t, applied, "a finish must not resurrect a poisoned row")

	stored, err := qh.events.GetEvent(ctx, "excl-1")
	require.NoError(t, err)
	assert.NotNil(t, stored.FailedAt)
	assert.Nil(t, stored.ProcessedAt, "poisoned and processed must stay mutually exclusive")
}

// TestQueuePoison: a poisoned event is never claimed again, keeps its
// error, and stops blocking its ordering key.
func TestQueuePoison(t *testing.T) {
	qh := newQueueHarness(t)
	ctx := context.Background()
	qh.enqueue(t, "poison-1", "community-a")
	qh.enqueue(t, "poison-2", "community-a")

	event, err := qh.events.ClaimNext(ctx, time.Minute)
	require.NoError(t, err)
	require.Equal(t, "poison-1", event.ActivityID)
	require.NotNil(t, event.ClaimedUntil)
	applied, err := qh.events.MarkPoisoned(ctx, "poison-1", "unparseable payload", *event.ClaimedUntil)
	require.NoError(t, err)
	require.True(t, applied)

	stored, err := qh.events.GetEvent(ctx, "poison-1")
	require.NoError(t, err)
	assert.NotNil(t, stored.FailedAt)
	assert.Equal(t, "unparseable payload", stored.Error)
	assert.Nil(t, stored.ProcessedAt)

	// The poisoned head no longer blocks the key.
	next, err := qh.events.ClaimNext(ctx, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, "poison-2", next.ActivityID)
	require.NotNil(t, next.ClaimedUntil)
	applied, err = qh.events.MarkProcessed(ctx, "poison-2", *next.ClaimedUntil)
	require.NoError(t, err)
	require.True(t, applied)

	_, err = qh.events.ClaimNext(ctx, time.Minute)
	assert.True(t, errors.IsNotFound(err), "poisoned events are never re-claimed")
}

// TestQueueLeaseExpiry: a crashed worker's claim expires and the event is
// re-delivered to another worker.
func TestQueueLeaseExpiry(t *testing.T) {
	qh := newQueueHarness(t)
	ctx := context.Background()
	qh.enqueue(t, "lease-1", "community-a")

	_, err := qh.events.ClaimNext(ctx, 50*time.Millisecond)
	require.NoError(t, err)
	_, err = qh.events.ClaimNext(ctx, time.Minute)
	require.True(t, errors.IsNotFound(err), "a live lease blocks re-claiming")

	time.Sleep(80 * time.Millisecond)
	event, err := qh.events.ClaimNext(ctx, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, "lease-1", event.ActivityID)
	assert.Equal(t, 2, event.Attempts, "the re-delivery counts as a new attempt")
}

// scriptedProcessor fails a configurable number of times, then succeeds.
type scriptedProcessor struct {
	failures  int
	processed []string
	errs      int
}

func (p *scriptedProcessor) Process(_ context.Context, event *store.InboxEvent) error {
	if p.errs < p.failures {
		p.errs++
		return stderrors.New("transient failure")
	}
	p.processed = append(p.processed, event.ActivityID)
	return nil
}

// TestQueueWorkerRetryThenSuccess drives the worker loop itself: a
// transient failure schedules a retry (visible via next_attempt_at), and a
// later pass processes the event.
func TestQueueWorkerRetryThenSuccess(t *testing.T) {
	qh := newQueueHarness(t)
	ctx := context.Background()
	processor := &scriptedProcessor{failures: 1}
	q, err := NewQueue(QueueOptions{
		Events:         qh.events,
		Processor:      processor,
		MaxAttempts:    3,
		RetryBaseDelay: time.Millisecond,
		Lease:          time.Minute,
	})
	require.NoError(t, err)

	qh.enqueue(t, "flaky-1", "community-a")
	require.True(t, q.processNext(ctx), "first pass claims and fails")
	stored, err := qh.events.GetEvent(ctx, "flaky-1")
	require.NoError(t, err)
	assert.Nil(t, stored.ProcessedAt)
	assert.Equal(t, "transient failure", stored.Error)

	// After the (tiny) backoff, the retry succeeds.
	time.Sleep(20 * time.Millisecond)
	require.True(t, q.processNext(ctx))
	stored, err = qh.events.GetEvent(ctx, "flaky-1")
	require.NoError(t, err)
	assert.NotNil(t, stored.ProcessedAt)
	assert.Equal(t, []string{"flaky-1"}, processor.processed)
}

// TestQueueWorkerPoisonsAfterMaxAttempts: persistent failures hit the
// attempt cap and the event is poisoned with the error recorded.
func TestQueueWorkerPoisonsAfterMaxAttempts(t *testing.T) {
	qh := newQueueHarness(t)
	ctx := context.Background()
	processor := &scriptedProcessor{failures: 99}
	q, err := NewQueue(QueueOptions{
		Events:         qh.events,
		Processor:      processor,
		MaxAttempts:    2,
		RetryBaseDelay: time.Millisecond,
		Lease:          time.Minute,
	})
	require.NoError(t, err)

	qh.enqueue(t, "doomed-1", "community-a")
	require.True(t, q.processNext(ctx)) // attempt 1 → retry scheduled
	time.Sleep(20 * time.Millisecond)
	require.True(t, q.processNext(ctx)) // attempt 2 → cap reached → poison

	stored, err := qh.events.GetEvent(ctx, "doomed-1")
	require.NoError(t, err)
	assert.NotNil(t, stored.FailedAt, "the event must be poisoned at the attempt cap")
	assert.Equal(t, "transient failure", stored.Error)
	assert.False(t, q.processNext(ctx), "a poisoned event is not claimable")
}

// blockingProcessor signals when Process is entered, then blocks until the
// given context is cancelled, returning that cancellation error. It models a
// worker whose in-flight Process is interrupted by shutdown.
type blockingProcessor struct {
	entered chan struct{}
}

func (p *blockingProcessor) Process(ctx context.Context, _ *store.InboxEvent) error {
	close(p.entered)
	<-ctx.Done()
	return ctx.Err()
}

// TestQueueShutdownDoesNotClassifyOutcome (Finding E-a): when the parent
// context is cancelled mid-Process, the interrupted work is NOT classified
// as a failure — the row is left untouched (no error recorded, not poisoned,
// not rescheduled) so the lease lapses and the event is redelivered intact.
func TestQueueShutdownDoesNotClassifyOutcome(t *testing.T) {
	qh := newQueueHarness(t)
	processor := &blockingProcessor{entered: make(chan struct{})}
	q, err := NewQueue(QueueOptions{
		Events:      qh.events,
		Processor:   processor,
		MaxAttempts: 3,
		Lease:       time.Minute,
	})
	require.NoError(t, err)

	qh.enqueue(t, "shutdown-1", "community-a")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() { done <- q.processNext(ctx) }()

	<-processor.entered // Process is now in flight
	cancel()            // graceful shutdown
	assert.True(t, <-done, "processNext still reports it claimed an event")

	// The event must be untouched: no error, not poisoned, not processed,
	// and no extra attempt burned beyond the one the claim counted.
	stored, err := qh.events.GetEvent(context.Background(), "shutdown-1")
	require.NoError(t, err)
	assert.Nil(t, stored.ProcessedAt, "cancellation must not mark processed")
	assert.Nil(t, stored.FailedAt, "cancellation must not poison")
	assert.Empty(t, stored.Error, "cancellation must not record a failure")
	assert.Equal(t, 1, stored.Attempts, "cancellation must not burn a retry as a failure")
}

// gatedSuccessProcessor blocks until released, then reports success. It
// models a Process that finishes right as shutdown cancels the parent ctx.
type gatedSuccessProcessor struct {
	entered chan struct{}
	release chan struct{}
}

func (p *gatedSuccessProcessor) Process(_ context.Context, _ *store.InboxEvent) error {
	close(p.entered)
	<-p.release
	return nil
}

// TestQueueSuccessDuringShutdownIsAcked (Finding E-b): an event that
// SUCCEEDS just as shutdown cancels ctx is still durably marked processed,
// because the outcome write runs on a context detached from the worker's.
func TestQueueSuccessDuringShutdownIsAcked(t *testing.T) {
	qh := newQueueHarness(t)
	processor := &gatedSuccessProcessor{entered: make(chan struct{}), release: make(chan struct{})}
	q, err := NewQueue(QueueOptions{
		Events:      qh.events,
		Processor:   processor,
		MaxAttempts: 3,
		Lease:       time.Minute,
	})
	require.NoError(t, err)

	qh.enqueue(t, "shutdown-ok-1", "community-a")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool, 1)
	go func() { done <- q.processNext(ctx) }()

	<-processor.entered
	cancel()                 // shutdown hits...
	close(processor.release) // ...just as Process completes successfully
	assert.True(t, <-done)

	stored, err := qh.events.GetEvent(context.Background(), "shutdown-ok-1")
	require.NoError(t, err)
	assert.NotNil(t, stored.ProcessedAt, "completed work must be acked despite shutdown")
	assert.Empty(t, stored.Error)
}

// claimScriptedProcessor returns results[i] from its i-th claim, and nil once
// the script runs out. It records what each claim saw, in claim order.
type claimScriptedProcessor struct {
	results     []error
	activityIDs []string
	attempts    []int
	errorTexts  []string
}

func (p *claimScriptedProcessor) Process(_ context.Context, event *store.InboxEvent) error {
	claim := len(p.activityIDs)
	p.activityIDs = append(p.activityIDs, event.ActivityID)
	p.attempts = append(p.attempts, event.Attempts)
	p.errorTexts = append(p.errorTexts, event.Error)
	if claim < len(p.results) {
		return p.results[claim]
	}
	return nil
}

// newParkingQueue builds a queue with the default attempt cap and a retry
// backoff short enough that retries come due at once.
func (qh *queueHarness) newParkingQueue(t *testing.T, processor Processor, parkDelay time.Duration) *Queue {
	t.Helper()
	q, err := NewQueue(QueueOptions{
		Events:         qh.events,
		Processor:      processor,
		RetryBaseDelay: time.Millisecond,
		ParkDelay:      parkDelay,
		Lease:          time.Minute,
	})
	require.NoError(t, err)
	return q
}

// finished reports whether the event has reached either terminal state.
func (qh *queueHarness) finished(activityID string) func() bool {
	return func() bool {
		stored, err := qh.events.GetEvent(context.Background(), activityID)
		return err == nil && (stored.ProcessedAt != nil || stored.FailedAt != nil)
	}
}

// driveQueueUntil runs one processNext per poll until done reports true.
func driveQueueUntil(t *testing.T, q *Queue, done func() bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		q.processNext(context.Background())
		return done()
	}, 5*time.Second, time.Millisecond)
}

// TestQueueThrottledEventNeverPoisons: a throttled event is held rather than
// failed, so it waits out any number of refusals and still lands.
func TestQueueThrottledEventNeverPoisons(t *testing.T) {
	tests := []struct {
		name        string
		refusal     error
		wantRefusal string
	}{
		{
			name:        "mint rate gate",
			refusal:     fmt.Errorf("mint actor: %w", ErrMintRateExceeded),
			wantRefusal: "DID mint rate exceeded",
		},
		{
			name: "PLC rate limit",
			refusal: fmt.Errorf("mint actor: %w",
				&identity.PLCRateLimitedError{Headers: map[string]string{"Retry-After": "120"}}),
			wantRefusal: `rate limited, Retry-After "120"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			qh := newQueueHarness(t)
			processor := &claimScriptedProcessor{results: slices.Repeat([]error{test.refusal}, 10)}
			q := qh.newParkingQueue(t, processor, time.Millisecond)

			qh.enqueue(t, "throttled-1", "community-a")
			driveQueueUntil(t, q, qh.finished("throttled-1"))

			assert.Equal(t, slices.Repeat([]int{1}, 11), processor.attempts,
				"ten refusals and the landing claim each see one attempt: a park costs nothing")
			for claim, errorText := range processor.errorTexts {
				if claim == 0 {
					assert.Empty(t, errorText, "the first claim has no earlier refusal to see")
					continue
				}
				assert.Contains(t, errorText, test.wantRefusal,
					"claim %d must see the refusal the park recorded", claim+1)
			}

			stored, err := qh.events.GetEvent(context.Background(), "throttled-1")
			require.NoError(t, err)
			assert.NotNil(t, stored.ProcessedAt, "the event lands once the throttle lifts")
			assert.Nil(t, stored.FailedAt, "a throttled event is never poisoned")
		})
	}
}

// TestQueueParkedHeadHoldsItsKey: a parked head keeps its younger sibling
// waiting behind it without holding up other keys. The retry backoff is 1ms, so
// only the hour-long park can be what holds a-1 back.
func TestQueueParkedHeadHoldsItsKey(t *testing.T) {
	qh := newQueueHarness(t)
	ctx := context.Background()
	refusal := fmt.Errorf("mint actor: %w", ErrMintRateExceeded)
	processor := &claimScriptedProcessor{results: []error{refusal}}
	q := qh.newParkingQueue(t, processor, time.Hour)

	qh.enqueue(t, "a-1", "community-a")
	qh.enqueue(t, "a-2", "community-a")
	qh.enqueue(t, "b-1", "community-b")

	require.True(t, q.processNext(ctx), "a-1 is claimed and refused")
	parked, err := qh.events.GetEvent(ctx, "a-1")
	require.NoError(t, err)
	require.Truef(t, parked.NextAttemptAt.After(time.Now().Add(59*time.Minute)),
		"a-1 must be parked for the hour, next_attempt_at is %s", parked.NextAttemptAt)

	require.True(t, q.processNext(ctx), "another key's event still lands")
	assert.False(t, q.processNext(ctx), "a-1 is parked and a-2 waits behind it")
	assert.Equal(t, []string{"a-1", "b-1"}, processor.activityIDs)

	_, err = qh.database.ExecContext(ctx,
		`UPDATE inbox_events SET next_attempt_at = CURRENT_TIMESTAMP - interval '1 second' WHERE activity_id = $1`,
		"a-1")
	require.NoError(t, err)

	require.True(t, q.processNext(ctx))
	require.True(t, q.processNext(ctx))
	assert.Equal(t, []string{"a-1", "b-1", "a-1", "a-2"}, processor.activityIDs)
	for _, activityID := range []string{"a-1", "a-2", "b-1"} {
		stored, err := qh.events.GetEvent(ctx, activityID)
		require.NoError(t, err)
		assert.NotNil(t, stored.ProcessedAt, "%s must land", activityID)
	}
}

// TestQueueOrdinaryFailuresStillPoisonAtTheCap: only throttling parks. A
// wrapped ErrPLCTransient is the row that matters: PLCRateLimitedError unwraps
// to it, so classifying on the wider sentinel would park every transient
// failure forever.
func TestQueueOrdinaryFailuresStillPoisonAtTheCap(t *testing.T) {
	tests := []struct {
		name    string
		failure error
	}{
		{name: "plain error", failure: stderrors.New("remote 503")},
		{name: "wrapped PLC transient", failure: fmt.Errorf("mint actor: %w", identity.ErrPLCTransient)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			qh := newQueueHarness(t)
			processor := &claimScriptedProcessor{results: slices.Repeat([]error{test.failure}, 20)}
			q := qh.newParkingQueue(t, processor, time.Millisecond)

			qh.enqueue(t, "doomed-1", "community-a")
			driveQueueUntil(t, q, qh.finished("doomed-1"))

			assert.Equal(t, []int{1, 2, 3, 4, 5}, processor.attempts)
			stored, err := qh.events.GetEvent(context.Background(), "doomed-1")
			require.NoError(t, err)
			assert.NotNil(t, stored.FailedAt, "the event is poisoned at the cap")
			assert.Nil(t, stored.ProcessedAt)
			assert.Equal(t, 5, stored.Attempts)
		})
	}
}

// parkRecordingEvents is a queue store that hands out one claimed event per
// ClaimNext and records each park's next attempt. Every other method is the
// embedded nil interface, so a retry or poison write panics.
type parkRecordingEvents struct {
	store.InboxEvents
	attempts     int
	nextAttempts []time.Time
}

func (e *parkRecordingEvents) ClaimNext(context.Context, time.Duration) (*store.InboxEvent, error) {
	token := time.Date(2026, 11, 1, 12, 2, 0, 0, time.UTC)
	return &store.InboxEvent{
		ActivityID:   "park-1",
		Type:         "Announce",
		Attempts:     e.attempts,
		ClaimedUntil: &token,
	}, nil
}

func (e *parkRecordingEvents) ReleaseParked(_ context.Context, _, _ string, nextAttempt, _ time.Time) (bool, error) {
	e.nextAttempts = append(e.nextAttempts, nextAttempt)
	return true, nil
}

// TestQueueParkDelayIsFixedWithJitter: the park is two minutes plus up to 30
// seconds of jitter whatever the attempt count, so it is not a backoff.
func TestQueueParkDelayIsFixedWithJitter(t *testing.T) {
	const draws = 200

	tests := []struct {
		name     string
		attempts int
	}{
		{name: "first attempt", attempts: 1},
		{name: "at the attempt cap", attempts: 5},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			now := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
			events := &parkRecordingEvents{attempts: test.attempts}
			refusal := fmt.Errorf("mint actor: %w", ErrMintRateExceeded)
			q, err := NewQueue(QueueOptions{
				Events:    events,
				Processor: &claimScriptedProcessor{results: slices.Repeat([]error{refusal}, draws)},
				Logger:    slog.New(slog.DiscardHandler),
			})
			require.NoError(t, err)
			q.now = func() time.Time { return now }

			for range draws {
				require.NotPanics(t, func() { q.processNext(context.Background()) },
					"a throttled event must be parked, not retried or poisoned")
			}

			require.Len(t, events.nextAttempts, draws)
			distinct := make(map[time.Duration]struct{})
			for _, nextAttempt := range events.nextAttempts {
				delay := nextAttempt.Sub(now)
				assert.GreaterOrEqual(t, delay, 2*time.Minute)
				assert.LessOrEqual(t, delay, 2*time.Minute+30*time.Second)
				distinct[delay] = struct{}{}
			}
			assert.Greater(t, len(distinct), 1, "the delay must be jittered")
		})
	}
}

// TestQueueThrottleAtTheCapParksInsteadOfPoisoning: four real failures followed
// by a refusal on the fifth claim, the one that would hit the attempt cap. The
// refusal parks, so the throttled case must sit before the cap.
func TestQueueThrottleAtTheCapParksInsteadOfPoisoning(t *testing.T) {
	qh := newQueueHarness(t)
	ctx := context.Background()
	failure := stderrors.New("remote 503")
	refusal := fmt.Errorf("mint actor: %w", ErrMintRateExceeded)
	processor := &claimScriptedProcessor{results: []error{failure, failure, failure, failure, refusal}}
	q := qh.newParkingQueue(t, processor, time.Millisecond)

	qh.enqueue(t, "capped-1", "community-a")
	driveQueueUntil(t, q, func() bool { return len(processor.attempts) == 5 })
	require.Equal(t, []int{1, 2, 3, 4, 5}, processor.attempts, "the refusal lands on the fifth claim")

	stored, err := qh.events.GetEvent(ctx, "capped-1")
	require.NoError(t, err)
	assert.Equal(t, 4, stored.Attempts, "the park hands back the fifth claim")
	assert.Nil(t, stored.FailedAt, "a refusal at the cap must not poison")

	driveQueueUntil(t, q, qh.finished("capped-1"))
	stored, err = qh.events.GetEvent(ctx, "capped-1")
	require.NoError(t, err)
	assert.NotNil(t, stored.ProcessedAt, "the event lands on the claim after the park")
	assert.Nil(t, stored.FailedAt)
}
