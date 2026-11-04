package identity

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

// stallDeadline bounds every blocking step. It only turns a stall into a
// failure; it never establishes ordering.
const stallDeadline = 3 * time.Second

var clockStart = time.Date(2026, time.October, 18, 21, 58, 0, 0, time.UTC)

// testBudget is a budget for tests that do not care about pacing: one write
// every 100 microseconds on the real clock.
func testBudget(t *testing.T) *PLCWriteBudget {
	t.Helper()
	budget, err := NewPLCWriteBudget(PLCWriteBudgetOptions{WritesPerMinute: 600000})
	require.NoError(t, err)
	return budget
}

// manualClock feeds PLCWriteBudgetOptions{Now, Wait}. Time only moves through
// Advance. AwaitWaiters must be called from the test goroutine.
type manualClock struct {
	t       *testing.T
	mutex   sync.Mutex
	now     time.Time
	waiters map[*clockWaiter]struct{}
}

type clockWaiter struct {
	deadline time.Time
	released chan struct{}
}

func newManualClock(t *testing.T) *manualClock {
	return &manualClock{t: t, now: clockStart, waiters: make(map[*clockWaiter]struct{})}
}

func (c *manualClock) Now() time.Time {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.now
}

func (c *manualClock) Advance(delay time.Duration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.now = c.now.Add(delay)
	for waiter := range c.waiters {
		if !waiter.deadline.After(c.now) {
			delete(c.waiters, waiter)
			close(waiter.released)
		}
	}
}

// Wait blocks until the clock reaches now+delay or ctx ends.
func (c *manualClock) Wait(ctx context.Context, delay time.Duration) error {
	c.mutex.Lock()
	if delay <= 0 {
		c.mutex.Unlock()
		return ctx.Err()
	}
	waiter := &clockWaiter{deadline: c.now.Add(delay), released: make(chan struct{})}
	c.waiters[waiter] = struct{}{}
	c.mutex.Unlock()
	select {
	case <-waiter.released:
		return ctx.Err()
	case <-ctx.Done():
		c.mutex.Lock()
		delete(c.waiters, waiter)
		c.mutex.Unlock()
		return ctx.Err()
	}
}

func (c *manualClock) waiterCount() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return len(c.waiters)
}

// AwaitWaiters blocks until exactly n goroutines sit in Wait. A listed call
// that returns meanwhile fails the test at once: it was meant to stay blocked.
func (c *manualClock) AwaitWaiters(n int, blocked ...*blockingCall) {
	c.t.Helper()
	awaitCondition(c.t, func() bool {
		for _, call := range blocked {
			if call.returned() {
				c.t.Fatalf("%s returned while it had to stay blocked, at clock offset %s: %v",
					call.name, c.Now().Sub(clockStart), call.err)
			}
		}
		return c.waiterCount() == n
	}, "want %d goroutines in Wait", n)
}

// awaitCondition polls condition on the test goroutine until it holds, and
// fails after stallDeadline.
func awaitCondition(t *testing.T, condition func() bool, format string, args ...any) {
	t.Helper()
	stall := time.After(stallDeadline)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !condition() {
		select {
		case <-tick.C:
		case <-stall:
			t.Fatalf("stalled: %s", fmt.Sprintf(format, args...))
		}
	}
}

// blockingCall is a call running on its own goroutine.
type blockingCall struct {
	name     string
	finished chan struct{}
	err      error
}

func startCall(name string, run func() error) *blockingCall {
	call := &blockingCall{name: name, finished: make(chan struct{})}
	go func() {
		defer close(call.finished)
		call.err = run()
	}()
	return call
}

func (c *blockingCall) returned() bool {
	select {
	case <-c.finished:
		return true
	default:
		return false
	}
}

// result waits for the call to return and gives its error.
func (c *blockingCall) result(t *testing.T) error {
	t.Helper()
	select {
	case <-c.finished:
		return c.err
	case <-time.After(stallDeadline):
		t.Fatalf("stalled: %s did not return", c.name)
		return nil
	}
}

// newClockBudget is a budget of 5 a minute, one write every 12 seconds.
func newClockBudget(t *testing.T, clock *manualClock) *PLCWriteBudget {
	t.Helper()
	budget, err := NewPLCWriteBudget(PLCWriteBudgetOptions{
		WritesPerMinute: 5, Logger: slog.New(slog.DiscardHandler), Now: clock.Now, Wait: clock.Wait,
	})
	require.NoError(t, err)
	return budget
}

func cancellableContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return ctx, cancel
}

func TestNewPLCWriteBudget_RejectsNonPositiveRate(t *testing.T) {
	for _, writesPerMinute := range []int{0, -1} {
		t.Run(fmt.Sprint(writesPerMinute), func(t *testing.T) {
			_, err := NewPLCWriteBudget(PLCWriteBudgetOptions{WritesPerMinute: writesPerMinute})
			require.Error(t, err)
			assert.True(t, errors.IsValidation(err), "got %v", err)
		})
	}
}

func TestPLCWriteBudget_AllowPacesOneWritePerSlot(t *testing.T) {
	clock := newManualClock(t)
	budget := newClockBudget(t, clock)

	require.True(t, budget.Allow(), "a fresh budget has one token")
	require.False(t, budget.Allow(), "burst is 1")
	clock.Advance(11999 * time.Millisecond)
	require.False(t, budget.Allow(), "the next slot is at +12s")
	clock.Advance(time.Millisecond)
	require.True(t, budget.Allow(), "the next slot is at +12s")
}

func TestRateLimitPause(t *testing.T) {
	now := time.Date(2026, time.October, 18, 21, 58, 0, 0, time.UTC)
	for _, tc := range []struct {
		name       string
		retryAfter string
		want       time.Duration
	}{
		{"missing header waits an hour", "", time.Hour},
		{"delta seconds", "120", 2 * time.Minute},
		{"zero seconds", "0", 0},
		{"thirty seconds", "30", 30 * time.Second},
		{"two days is not clamped", "172800", 48 * time.Hour},
		{"future HTTP date", "Sun, 18 Oct 2026 22:28:00 GMT", 30 * time.Minute},
		{"past HTTP date", "Sun, 18 Oct 2026 21:00:00 GMT", 0},
		{"unparseable", "soon", time.Hour},
		{"negative seconds", "-5", time.Hour},
		{"fractional seconds", "1.5", time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, rateLimitPause(tc.retryAfter, now))
		})
	}
}

func TestPLCWriteBudget_RateLimitedPausesAllow(t *testing.T) {
	clock := newManualClock(t)
	budget := newClockBudget(t, clock)

	budget.RateLimited("120")
	require.False(t, budget.Allow(), "paused from the moment of the 429")
	clock.Advance(119999 * time.Millisecond)
	require.False(t, budget.Allow(), "the pause ends at +120s")
	clock.Advance(time.Millisecond)
	require.True(t, budget.Allow(), "the pause ended at +120s")
}

func TestPLCWriteBudget_RateLimitedPauseEnds(t *testing.T) {
	for _, tc := range []struct {
		name       string
		retryAfter string
		wantPause  time.Duration
	}{
		{"future HTTP date", clockStart.Add(30 * time.Minute).Format(http.TimeFormat), 30 * time.Minute},
		{"past HTTP date", clockStart.Add(-58 * time.Minute).Format(http.TimeFormat), 0},
		{"zero seconds", "0", 0},
		{"unparseable", "soon", time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := newManualClock(t)
			budget := newClockBudget(t, clock)

			budget.RateLimited(tc.retryAfter)
			if tc.wantPause > 0 {
				require.False(t, budget.Allow(), "paused from the moment of the 429")
				clock.Advance(tc.wantPause - time.Millisecond)
				require.False(t, budget.Allow(), "the pause ends at +%s", tc.wantPause)
				clock.Advance(time.Millisecond)
			}
			require.True(t, budget.Allow(), "the pause ended at +%s", tc.wantPause)
		})
	}
}

func TestPLCWriteBudget_WaitReturnsAtPauseEnd(t *testing.T) {
	clock := newManualClock(t)
	budget := newClockBudget(t, clock)
	ctx, _ := cancellableContext(t)

	budget.RateLimited("120")
	wait := startCall("Wait", func() error { return budget.Wait(ctx) })
	clock.AwaitWaiters(1, wait)
	clock.Advance(119999 * time.Millisecond)
	clock.AwaitWaiters(1, wait)
	clock.Advance(time.Millisecond)
	require.NoError(t, wait.result(t))
}

// The pause is shorter than the 12 second slot, so a token charged by the
// cancelled call at any moment would still be unspendable at the pause's end.
func TestPLCWriteBudget_CancelDuringPauseChargesNothing(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(*PLCWriteBudget, context.Context) error
	}{
		{"Wait", (*PLCWriteBudget).Wait},
		{"WaitForResume", (*PLCWriteBudget).WaitForResume},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := newManualClock(t)
			budget := newClockBudget(t, clock)
			ctx, cancel := cancellableContext(t)

			budget.RateLimited("5")
			call := startCall(tc.name, func() error { return tc.call(budget, ctx) })
			clock.AwaitWaiters(1, call)
			clock.Advance(4 * time.Second)
			clock.AwaitWaiters(1, call)
			cancel()
			require.ErrorIs(t, call.result(t), context.Canceled)
			clock.Advance(time.Second)
			require.True(t, budget.Allow(), "a cancelled call must not hold a token")
		})
	}
}

// The later 429 arrives at +10s. A Wait parked before it sleeps to the old end
// whatever the new end is, so the fresh Allow at probeAt is what sees the end
// that RateLimited stored: +41s is past a shortened +40s end, +121s is past the
// unextended +120s end.
func TestPLCWriteBudget_LaterRateLimitedOnlyMovesPauseEndLater(t *testing.T) {
	for _, tc := range []struct {
		name      string
		later     string
		probeAt   time.Duration // from the clock start
		beforeEnd time.Duration // from the clock start, 1ms before the end
		wantEnd   time.Duration
	}{
		{name: "shorter does not shorten", later: "30", probeAt: 41 * time.Second, beforeEnd: 119999 * time.Millisecond, wantEnd: 120 * time.Second},
		{name: "longer extends", later: "300", probeAt: 121 * time.Second, beforeEnd: 309999 * time.Millisecond, wantEnd: 310 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := newManualClock(t)
			budget := newClockBudget(t, clock)
			ctx, _ := cancellableContext(t)
			advanceTo := func(offset time.Duration) {
				clock.Advance(clockStart.Add(offset).Sub(clock.Now()))
			}

			budget.RateLimited("120")
			wait := startCall("Wait", func() error { return budget.Wait(ctx) })
			clock.AwaitWaiters(1, wait)
			clock.Advance(10 * time.Second)
			clock.AwaitWaiters(1, wait)
			budget.RateLimited(tc.later)
			advanceTo(tc.probeAt)
			clock.AwaitWaiters(1, wait)
			require.False(t, budget.Allow(), "the pause is still in force at %s", tc.probeAt)
			advanceTo(tc.beforeEnd)
			clock.AwaitWaiters(1, wait)
			clock.Advance(time.Millisecond)
			require.NoError(t, wait.result(t))
			require.Equal(t, clockStart.Add(tc.wantEnd), clock.Now())
		})
	}
}

func TestPLCWriteBudget_WaitBlockedOnPacingReturnsAtPauseEndAndChargesOneToken(t *testing.T) {
	clock := newManualClock(t)
	budget := newClockBudget(t, clock)
	ctx, _ := cancellableContext(t)

	require.NoError(t, budget.Wait(ctx), "a fresh budget admits one write")
	wait := startCall("Wait", func() error { return budget.Wait(ctx) })
	clock.AwaitWaiters(1, wait)
	clock.Advance(6 * time.Second)
	clock.AwaitWaiters(1, wait)
	budget.RateLimited("60") // The pause ends at +66s.
	clock.Advance(59999 * time.Millisecond)
	clock.AwaitWaiters(1, wait)
	clock.Advance(time.Millisecond)
	require.NoError(t, wait.result(t))

	require.False(t, budget.Allow(), "the released Wait took the token at the pause's end")
	clock.Advance(11999 * time.Millisecond)
	require.False(t, budget.Allow())
	clock.Advance(time.Millisecond)
	require.True(t, budget.Allow(), "exactly one token was charged, at the pause's end")
}

func TestPLCWriteBudget_WaitsReleasedByPauseEndStaySpaced(t *testing.T) {
	clock := newManualClock(t)
	budget := newClockBudget(t, clock)
	ctx, _ := cancellableContext(t)

	budget.RateLimited("60")
	first := startCall("first Wait", func() error { return budget.Wait(ctx) })
	second := startCall("second Wait", func() error { return budget.Wait(ctx) })
	clock.AwaitWaiters(2, first, second)
	clock.Advance(60 * time.Second)

	awaitCondition(t, func() bool {
		if first.returned() && second.returned() {
			t.Fatal("both Waits returned in the same slot")
		}
		return (first.returned() || second.returned()) && clock.waiterCount() == 1
	}, "one Wait returns at the pause's end and the other waits for the next slot")
	remaining := first
	if first.returned() {
		require.NoError(t, first.err)
		remaining = second
	} else {
		require.NoError(t, second.err)
	}

	clock.Advance(11999 * time.Millisecond)
	clock.AwaitWaiters(1, remaining)
	clock.Advance(time.Millisecond)
	require.NoError(t, remaining.result(t))
}

func TestPLCWriteBudget_WaitForResumeReturnsAtPauseEndWithoutCharge(t *testing.T) {
	clock := newManualClock(t)
	budget := newClockBudget(t, clock)
	ctx, _ := cancellableContext(t)

	budget.RateLimited("5")
	resume := startCall("WaitForResume", func() error { return budget.WaitForResume(ctx) })
	clock.AwaitWaiters(1, resume)
	clock.Advance(4999 * time.Millisecond)
	clock.AwaitWaiters(1, resume)
	clock.Advance(time.Millisecond)
	require.NoError(t, resume.result(t))
	require.True(t, budget.Allow(), "WaitForResume takes no token")
}

func TestPLCWriteBudget_WaitForResumeIgnoresPacing(t *testing.T) {
	clock := newManualClock(t)
	budget := newClockBudget(t, clock)
	ctx, _ := cancellableContext(t)

	require.NoError(t, budget.Wait(ctx), "a fresh budget admits one write")
	resume := startCall("WaitForResume", func() error { return budget.WaitForResume(ctx) })
	require.NoError(t, resume.result(t), "no pause: returns with no clock advance")
	clock.Advance(12 * time.Second)
	require.True(t, budget.Allow(), "WaitForResume takes no token")
}

func TestPLCWriteBudget_Paused(t *testing.T) {
	clock := newManualClock(t)
	budget := newClockBudget(t, clock)

	require.False(t, budget.Paused(), "a fresh budget is not paused")
	require.True(t, budget.Allow(), "a fresh budget has one token")
	require.False(t, budget.Paused(), "waiting for pacing is not a pause")

	budget.RateLimited("120")
	require.True(t, budget.Paused(), "paused from the moment of the 429")
	clock.Advance(119999 * time.Millisecond)
	require.True(t, budget.Paused(), "the pause ends at +120s")
	clock.Advance(time.Millisecond)
	require.False(t, budget.Paused(), "the pause ended at +120s")

	budget.RateLimited("0")
	require.False(t, budget.Paused(), "a Retry-After of zero pauses nothing")
}

func TestPLCWriteBudget_WaitUnlessPausedPaces(t *testing.T) {
	clock := newManualClock(t)
	budget := newClockBudget(t, clock)
	ctx, _ := cancellableContext(t)

	require.NoError(t, budget.WaitUnlessPaused(ctx), "a fresh budget admits one write")
	wait := startCall("WaitUnlessPaused", func() error { return budget.WaitUnlessPaused(ctx) })
	clock.AwaitWaiters(1, wait)
	clock.Advance(11999 * time.Millisecond)
	clock.AwaitWaiters(1, wait)
	clock.Advance(time.Millisecond)
	require.NoError(t, wait.result(t))

	require.False(t, budget.Allow(), "the released call took the token at +12s")
	clock.Advance(12 * time.Second)
	require.True(t, budget.Allow(), "the next slot is at +24s")
}

func TestPLCWriteBudget_WaitUnlessPausedRefusesDuringAPause(t *testing.T) {
	clock := newManualClock(t)
	budget := newClockBudget(t, clock)
	ctx, _ := cancellableContext(t)

	budget.RateLimited("120")
	require.ErrorIs(t, budget.WaitUnlessPaused(ctx), ErrPLCWritesPaused)
	require.Equal(t, clockStart, clock.Now(), "refused with no clock advance")
	require.Zero(t, clock.waiterCount())

	clock.Advance(120 * time.Second)
	require.True(t, budget.Allow(), "a refused call must not hold a token")
}

// The pause outlives the slot in one row and ends before it in the other, so
// waking when the slot comes round cannot stand in for the notification, and a
// token charged by the refused call would still be unspendable at +12s.
func TestPLCWriteBudget_WaitUnlessPausedReturnsWhenAPauseStarts(t *testing.T) {
	for _, tc := range []struct {
		name       string
		retryAfter string
		slotFreeIn time.Duration
	}{
		{"pause ending before the slot", "5", 6 * time.Second},
		{"pause ending after the slot", "120", 120 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := newManualClock(t)
			budget := newClockBudget(t, clock)
			ctx, _ := cancellableContext(t)

			require.True(t, budget.Allow(), "a fresh budget has one token")
			wait := startCall("WaitUnlessPaused", func() error { return budget.WaitUnlessPaused(ctx) })
			clock.AwaitWaiters(1, wait)
			clock.Advance(6 * time.Second)
			clock.AwaitWaiters(1, wait)

			pausedAt := clock.Now()
			budget.RateLimited(tc.retryAfter)
			require.ErrorIs(t, wait.result(t), ErrPLCWritesPaused)
			require.Equal(t, pausedAt, clock.Now(), "returned with no clock advance")

			clock.Advance(tc.slotFreeIn - time.Millisecond)
			require.False(t, budget.Allow(), "the slot is still taken by the first token or the pause")
			clock.Advance(time.Millisecond)
			require.True(t, budget.Allow(), "the refused call must not hold a token")
		})
	}
}

func TestPLCWriteBudget_APauseReleasesEveryWaitUnlessPaused(t *testing.T) {
	clock := newManualClock(t)
	budget := newClockBudget(t, clock)
	ctx, _ := cancellableContext(t)

	require.True(t, budget.Allow(), "a fresh budget has one token")
	first := startCall("first WaitUnlessPaused", func() error { return budget.WaitUnlessPaused(ctx) })
	second := startCall("second WaitUnlessPaused", func() error { return budget.WaitUnlessPaused(ctx) })
	clock.AwaitWaiters(2, first, second)

	budget.RateLimited("120")
	require.ErrorIs(t, first.result(t), ErrPLCWritesPaused)
	require.ErrorIs(t, second.result(t), ErrPLCWritesPaused)
}

// A Retry-After of zero pauses nothing: Allow stays usable, so the wait goes on.
func TestPLCWriteBudget_WaitUnlessPausedSitsThroughAZeroPause(t *testing.T) {
	clock := newManualClock(t)
	budget := newClockBudget(t, clock)
	ctx, _ := cancellableContext(t)

	require.True(t, budget.Allow(), "a fresh budget has one token")
	wait := startCall("WaitUnlessPaused", func() error { return budget.WaitUnlessPaused(ctx) })
	clock.AwaitWaiters(1, wait)

	budget.RateLimited("0")
	clock.AwaitWaiters(1, wait)
	clock.Advance(12 * time.Second)
	require.NoError(t, wait.result(t))
}

func TestPLCWriteBudget_WaitUnlessPausedCancelChargesNothing(t *testing.T) {
	clock := newManualClock(t)
	budget := newClockBudget(t, clock)
	ctx, cancel := cancellableContext(t)

	require.True(t, budget.Allow(), "a fresh budget has one token")
	wait := startCall("WaitUnlessPaused", func() error { return budget.WaitUnlessPaused(ctx) })
	clock.AwaitWaiters(1, wait)
	cancel()
	require.ErrorIs(t, wait.result(t), context.Canceled)

	clock.Advance(12 * time.Second)
	require.True(t, budget.Allow(), "a cancelled call must not hold a token")
}

func TestPLCWriteBudget_WaitUnlessPausedWithACancelledContextChargesNothing(t *testing.T) {
	clock := newManualClock(t)
	budget := newClockBudget(t, clock)
	ctx, cancel := cancellableContext(t)
	cancel()

	require.ErrorIs(t, budget.WaitUnlessPaused(ctx), context.Canceled)
	require.True(t, budget.Allow(), "a cancelled call must not take the free token")
}
