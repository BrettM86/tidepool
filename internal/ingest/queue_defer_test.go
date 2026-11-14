package ingest

import (
	"context"
	stderrors "errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/identity"
	"tidepool/internal/store"
)

// fakeEventDeferrer answers DeferEvent from a script (the i-th call gets
// results[i], the last entry repeating) and ObjectNotServed with notServed.
// consulted records every cause either method was handed.
type fakeEventDeferrer struct {
	results   []deferResult
	notServed bool
	consulted []error
}

type deferResult struct {
	deferred bool
	err      error
}

func (d *fakeEventDeferrer) DeferEvent(_ context.Context, _ *store.InboxEvent, cause error) (bool, error) {
	d.consulted = append(d.consulted, cause)
	if len(d.results) == 0 {
		return false, nil
	}
	result := d.results[0]
	if len(d.results) > 1 {
		d.results = d.results[1:]
	}
	return result.deferred, result.err
}

func (d *fakeEventDeferrer) ObjectNotServed(_ context.Context, _ *store.InboxEvent, cause error) bool {
	d.consulted = append(d.consulted, cause)
	return d.notServed
}

// deferQueueClock is the fixed time every failure in these tests happens at.
var deferQueueClock = time.Date(2026, 11, 13, 9, 0, 0, 0, time.UTC)

// newDeferringQueue builds a queue with the default attempt cap and backoff,
// the given deferrer and a clock fixed at deferQueueClock.
func (qh *queueHarness) newDeferringQueue(t *testing.T, processor Processor, deferrer EventDeferrer) *Queue {
	t.Helper()
	q, err := NewQueue(QueueOptions{
		Events:    qh.events,
		Processor: processor,
		Deferrer:  deferrer,
		ParkDelay: time.Hour,
		Lease:     time.Minute,
	})
	require.NoError(t, err)
	q.now = func() time.Time { return deferQueueClock }
	return q
}

// makeDue moves activityID's next attempt into the past so the next claim
// takes it, without sleeping through its backoff.
func (qh *queueHarness) makeDue(t *testing.T, activityID string) {
	t.Helper()
	_, err := qh.database.ExecContext(context.Background(),
		`UPDATE inbox_events SET next_attempt_at = CURRENT_TIMESTAMP - interval '1 second' WHERE activity_id = $1`,
		activityID)
	require.NoError(t, err)
}

func (qh *queueHarness) stored(t *testing.T, activityID string) *store.InboxEvent {
	t.Helper()
	stored, err := qh.events.GetEvent(context.Background(), activityID)
	require.NoError(t, err)
	return stored
}

// hostGoneFailure is a fetch failure on x.example, which refused the dial.
func hostGoneFailure() error {
	return fmt.Errorf("ingest: fetch https://x.example/post/1: %w",
		errors.HostGoneError{Host: "x.example", Reason: errors.HostGoneReasonConnectionRefused})
}

// TestQueueDefersHostGoneOnFirstAttempt (Q1): a host-gone failure the
// deferrer accepts finishes the event on its first attempt.
func TestQueueDefersHostGoneOnFirstAttempt(t *testing.T) {
	qh := newQueueHarness(t)
	processor := &claimScriptedProcessor{results: slices.Repeat([]error{hostGoneFailure()}, 5)}
	q := qh.newDeferringQueue(t, processor, &fakeEventDeferrer{results: []deferResult{{deferred: true}}})

	qh.enqueue(t, "gone-1", "community-a")
	require.True(t, q.processNext(context.Background()))

	stored := qh.stored(t, "gone-1")
	assert.NotNil(t, stored.ProcessedAt, "a deferred event is processed")
	assert.Nil(t, stored.FailedAt, "a deferred event is not poisoned")
	assert.Equal(t, 1, stored.Attempts)
	assert.False(t, q.processNext(context.Background()), "nothing is left to claim")
}

// TestQueueRetriesThenDefersAtTheCap (Q2): an ordinary failure is retried
// 30s then 60s after it happened; the third failure, at the default cap, is
// deferred and the event processed.
func TestQueueRetriesThenDefersAtTheCap(t *testing.T) {
	qh := newQueueHarness(t)
	failure := stderrors.New("remote 502")
	processor := &claimScriptedProcessor{results: slices.Repeat([]error{failure}, 5)}
	q := qh.newDeferringQueue(t, processor, &fakeEventDeferrer{results: []deferResult{{deferred: true}}})
	ctx := context.Background()

	qh.enqueue(t, "flaky-1", "community-a")
	wantNext := []time.Time{
		time.Date(2026, 11, 13, 9, 0, 30, 0, time.UTC),
		time.Date(2026, 11, 13, 9, 1, 0, 0, time.UTC),
	}
	for attempt, want := range wantNext {
		require.True(t, q.processNext(ctx), "attempt %d is claimed", attempt+1)
		stored := qh.stored(t, "flaky-1")
		assert.Nil(t, stored.ProcessedAt, "attempt %d is retried, not finished", attempt+1)
		assert.Nil(t, stored.FailedAt, "attempt %d is retried, not poisoned", attempt+1)
		assert.Equal(t, attempt+1, stored.Attempts)
		assert.Truef(t, want.Equal(stored.NextAttemptAt),
			"attempt %d retries at %s, got %s", attempt+1, want, stored.NextAttemptAt.UTC())
		qh.makeDue(t, "flaky-1")
	}

	require.True(t, q.processNext(ctx), "attempt 3 is claimed")
	stored := qh.stored(t, "flaky-1")
	assert.NotNil(t, stored.ProcessedAt, "the third failure is deferred and the event processed")
	assert.Nil(t, stored.FailedAt, "a deferred event is not poisoned")
	assert.Equal(t, 3, stored.Attempts)
}

// TestQueuePoisonsUndeferrableAtThreeAttempts (Q3): with MaxAttempts unset,
// an event the deferrer refuses is poisoned on its third failed attempt.
func TestQueuePoisonsUndeferrableAtThreeAttempts(t *testing.T) {
	qh := newQueueHarness(t)
	failure := stderrors.New("remote 502")
	processor := &claimScriptedProcessor{results: slices.Repeat([]error{failure}, 10)}
	q := qh.newDeferringQueue(t, processor, &fakeEventDeferrer{results: []deferResult{{deferred: false}}})
	ctx := context.Background()

	qh.enqueue(t, "doomed-1", "community-a")
	for attempt := 1; attempt <= 2; attempt++ {
		require.True(t, q.processNext(ctx))
		require.Nil(t, qh.stored(t, "doomed-1").FailedAt, "attempt %d is not the cap", attempt)
		qh.makeDue(t, "doomed-1")
	}
	require.True(t, q.processNext(ctx))

	stored := qh.stored(t, "doomed-1")
	assert.NotNil(t, stored.FailedAt, "the third failed attempt poisons an undeferrable event")
	assert.Nil(t, stored.ProcessedAt)
	assert.Equal(t, 3, stored.Attempts)
	assert.Equal(t, "remote 502", stored.Error)
}

// TestQueueDeferWriteFailureKeepsTheEvent (Q4): a host-gone failure whose
// deferral cannot be written is released for retry, neither finished nor
// poisoned; the retry, whose deferral is written, finishes it.
func TestQueueDeferWriteFailureKeepsTheEvent(t *testing.T) {
	qh := newQueueHarness(t)
	processor := &claimScriptedProcessor{results: slices.Repeat([]error{hostGoneFailure()}, 5)}
	deferrer := &fakeEventDeferrer{results: []deferResult{
		{err: stderrors.New("deferred store unavailable")},
		{deferred: true},
	}}
	q := qh.newDeferringQueue(t, processor, deferrer)
	ctx := context.Background()

	qh.enqueue(t, "gone-1", "community-a")
	require.True(t, q.processNext(ctx))
	stored := qh.stored(t, "gone-1")
	assert.Nil(t, stored.ProcessedAt, "an event whose deferral failed is not finished")
	assert.Nil(t, stored.FailedAt, "an event whose deferral failed is not poisoned")
	assert.Equal(t, 1, stored.Attempts)
	assert.Truef(t, time.Date(2026, 11, 13, 9, 0, 30, 0, time.UTC).Equal(stored.NextAttemptAt),
		"the event is retried 30s later, got %s", stored.NextAttemptAt.UTC())

	qh.makeDue(t, "gone-1")
	require.True(t, q.processNext(ctx))
	stored = qh.stored(t, "gone-1")
	assert.NotNil(t, stored.ProcessedAt, "the retry's deferral is written and the event processed")
	assert.Nil(t, stored.FailedAt)
	assert.Equal(t, 2, stored.Attempts)
}

// TestQueueValidationAndThrottlingNeverDefer (Q5): a validation error poisons
// on attempt 1 and throttling parks at no attempt cost, without consulting a
// deferrer that would accept, even when the cause is also host-gone.
func TestQueueValidationAndThrottlingNeverDefer(t *testing.T) {
	tests := []struct {
		name       string
		cause      error
		wantPoison bool
	}{
		{
			name:       "validation error",
			cause:      stderrors.Join(errors.NewValidationError("object", "missing id"), hostGoneFailure()),
			wantPoison: true,
		},
		{
			name:  "mint rate gate",
			cause: stderrors.Join(fmt.Errorf("mint actor: %w", ErrMintRateExceeded), hostGoneFailure()),
		},
		{
			name: "PLC rate limit",
			cause: stderrors.Join(fmt.Errorf("mint actor: %w",
				&identity.PLCRateLimitedError{Headers: map[string]string{"Retry-After": "120"}}), hostGoneFailure()),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			qh := newQueueHarness(t)
			processor := &claimScriptedProcessor{results: []error{tt.cause}}
			deferrer := &fakeEventDeferrer{results: []deferResult{{deferred: true}}, notServed: true}
			q := qh.newDeferringQueue(t, processor, deferrer)

			qh.enqueue(t, "held-1", "community-a")
			require.True(t, q.processNext(context.Background()))

			stored := qh.stored(t, "held-1")
			assert.Nil(t, stored.ProcessedAt, "the event is not finished")
			if tt.wantPoison {
				assert.NotNil(t, stored.FailedAt, "a validation error poisons on attempt 1")
				assert.Equal(t, 1, stored.Attempts)
			} else {
				assert.Nil(t, stored.FailedAt, "a throttled event is parked, not poisoned")
				assert.Equal(t, 0, stored.Attempts, "a park spends no attempt")
			}
			assert.Empty(t, deferrer.consulted, "the deferrer is never consulted")
		})
	}
}

// TestQueueSkipsObjectNotServed (Q6): a failure the deferrer reports as an
// object its host does not serve is finished as a skip on attempt 1.
func TestQueueSkipsObjectNotServed(t *testing.T) {
	qh := newQueueHarness(t)
	failure := stderrors.New("fetch https://x.example/post/1: non-JSON response")
	processor := &claimScriptedProcessor{results: slices.Repeat([]error{failure}, 5)}
	q := qh.newDeferringQueue(t, processor, &fakeEventDeferrer{notServed: true})

	qh.enqueue(t, "html-1", "community-a")
	require.True(t, q.processNext(context.Background()))

	stored := qh.stored(t, "html-1")
	assert.NotNil(t, stored.ProcessedAt, "an object its host does not serve is skipped")
	assert.Nil(t, stored.FailedAt)
	assert.Equal(t, 1, stored.Attempts)
}

// probeBlockingDeferrer is a deferrer whose ObjectNotServed probe never
// answers: it blocks until its ctx is done, then reports false.
type probeBlockingDeferrer struct {
	fakeEventDeferrer
}

func (d *probeBlockingDeferrer) ObjectNotServed(ctx context.Context, _ *store.InboxEvent, _ error) bool {
	<-ctx.Done()
	return false
}

// TestQueueSlowProbeStillRecordsTheRetry (F1): a not-served probe that runs
// out its own timeout does not spend the context the outcome is written on:
// the failed event is still released for its retry 30s later, one attempt
// spent, its lease cleared.
func TestQueueSlowProbeStillRecordsTheRetry(t *testing.T) {
	qh := newQueueHarness(t)
	failure := stderrors.New("fetch https://x.example/post/1: non-JSON response")
	processor := &claimScriptedProcessor{results: slices.Repeat([]error{failure}, 5)}
	q := qh.newDeferringQueue(t, processor, &probeBlockingDeferrer{})
	q.probeTimeout = 5 * time.Millisecond

	qh.enqueue(t, "slow-probe-1", "community-a")
	require.True(t, q.processNext(context.Background()))

	stored := qh.stored(t, "slow-probe-1")
	assert.Nil(t, stored.ProcessedAt, "the event is retried, not finished")
	assert.Nil(t, stored.FailedAt, "the event is retried, not poisoned")
	assert.Equal(t, 1, stored.Attempts)
	assert.Nil(t, stored.ClaimedUntil, "the event's lease is cleared by its release")
	assert.Truef(t, time.Date(2026, 11, 13, 9, 0, 30, 0, time.UTC).Equal(stored.NextAttemptAt),
		"the event is retried 30s later, got %s", stored.NextAttemptAt.UTC())
}

// TestQueueDeferWriteFailureAtTheCapRetries (F4): at the attempt cap, a
// deferral the deferrer accepts but cannot write releases the event for a
// retry with backoff (120s after the third failure) instead of poisoning
// it; the next attempt, whose deferral is written, finishes it.
func TestQueueDeferWriteFailureAtTheCapRetries(t *testing.T) {
	qh := newQueueHarness(t)
	failure := stderrors.New("remote 502")
	processor := &claimScriptedProcessor{results: slices.Repeat([]error{failure}, 10)}
	deferrer := &fakeEventDeferrer{results: []deferResult{
		{err: stderrors.New("deferred store unavailable")},
		{deferred: true},
	}}
	q := qh.newDeferringQueue(t, processor, deferrer)
	ctx := context.Background()

	qh.enqueue(t, "cap-write-1", "community-a")
	for attempt := 1; attempt <= 2; attempt++ {
		require.True(t, q.processNext(ctx), "attempt %d is claimed", attempt)
		qh.makeDue(t, "cap-write-1")
	}
	require.True(t, q.processNext(ctx), "attempt 3 is claimed")

	stored := qh.stored(t, "cap-write-1")
	assert.Nil(t, stored.FailedAt, "a deferral write failure at the cap does not poison the event")
	assert.Nil(t, stored.ProcessedAt, "an event whose deferral failed is not finished")
	assert.Equal(t, 3, stored.Attempts)
	assert.Truef(t, time.Date(2026, 11, 13, 9, 2, 0, 0, time.UTC).Equal(stored.NextAttemptAt),
		"the event is retried 120s later, got %s", stored.NextAttemptAt.UTC())

	qh.makeDue(t, "cap-write-1")
	require.True(t, q.processNext(ctx), "attempt 4 is claimed")
	stored = qh.stored(t, "cap-write-1")
	assert.NotNil(t, stored.ProcessedAt, "the retry's deferral is written and the event processed")
	assert.Nil(t, stored.FailedAt)
	assert.Equal(t, 4, stored.Attempts)
}
