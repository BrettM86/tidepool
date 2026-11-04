package identity

import (
	"context"
	stderrors "errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"tidepool/internal/errors"
)

// ErrPLCWritesPaused is WaitUnlessPaused's answer during a pause.
var ErrPLCWritesPaused = stderrors.New("identity: PLC writes paused")

// defaultRateLimitPause is the pause when PLC sends no usable Retry-After.
const defaultRateLimitPause = time.Hour

// rateLimitPause is how long to pause after a PLC 429 carrying retryAfter:
// delta-seconds or an HTTP-date, honoured exactly. A past date pauses for 0.
// A missing, unparseable, negative, fractional or out-of-range value pauses
// for an hour.
func rateLimitPause(retryAfter string, now time.Time) time.Duration {
	retryAfter = strings.TrimSpace(retryAfter)
	if retryAfter == "" {
		return defaultRateLimitPause
	}
	if strings.Trim(retryAfter, "0123456789") == "" {
		seconds, err := strconv.ParseInt(retryAfter, 10, 64)
		if err != nil || seconds > math.MaxInt64/int64(time.Second) {
			return defaultRateLimitPause
		}
		return time.Duration(seconds) * time.Second
	}
	resumeAt, err := http.ParseTime(retryAfter)
	if err != nil {
		return defaultRateLimitPause
	}
	return max(resumeAt.Sub(now), 0)
}

// PLCWriteBudgetOptions configures NewPLCWriteBudget.
type PLCWriteBudgetOptions struct {
	// WritesPerMinute is the pace of PLC POSTs and must be positive. The
	// burst is always 1.
	WritesPerMinute int
	// Logger defaults to slog.Default.
	Logger *slog.Logger
	// Now defaults to time.Now.
	Now func() time.Time
	// Wait sleeps for delay or until ctx ends; nil uses a timer.
	Wait func(ctx context.Context, delay time.Duration) error
}

// PLCWriteBudget paces every PLC POST and pauses them all after a PLC 429.
// One instance is shared by everything that writes to the directory.
//
// A token is charged only at the moment Allow, Wait or WaitUnlessPaused returns
// success, so no caller holds a token across a pause and a cancelled wait
// charges nothing.
type PLCWriteBudget struct {
	interval time.Duration
	logger   *slog.Logger
	now      func() time.Time
	wait     func(ctx context.Context, delay time.Duration) error

	mutex       sync.Mutex
	nextWrite   time.Time
	pausedUntil time.Time
	// pauseMoved is closed and replaced each time the pause end moves, which
	// wakes every WaitUnlessPaused at once.
	pauseMoved chan struct{}
}

// NewPLCWriteBudget validates the options and builds a budget with its first
// write available at once.
func NewPLCWriteBudget(opts PLCWriteBudgetOptions) (*PLCWriteBudget, error) {
	if opts.WritesPerMinute <= 0 {
		return nil, errors.NewValidationError("writes_per_minute",
			fmt.Sprintf("must be positive, got %d", opts.WritesPerMinute))
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	wait := opts.Wait
	if wait == nil {
		wait = waitForDelay
	}
	return &PLCWriteBudget{
		interval:   time.Minute / time.Duration(opts.WritesPerMinute),
		logger:     logger,
		now:        now,
		wait:       wait,
		pauseMoved: make(chan struct{}),
	}, nil
}

// Allow takes a token if one is free now. It never blocks, and it is false
// during a pause or before the next slot.
func (b *PLCWriteBudget) Allow() bool {
	return b.takeToken() == 0
}

// Wait blocks through pacing and any pause, then takes one token. On
// cancellation it returns the context's error and charges nothing. Each
// release re-checks the budget, so waiters freed by a pause end stay spaced.
func (b *PLCWriteBudget) Wait(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		remaining := b.takeToken()
		if remaining == 0 {
			return nil
		}
		if err := b.wait(ctx, remaining); err != nil {
			return err
		}
	}
}

// WaitForResume blocks until no pause is in force and takes no token. It
// ignores pacing. On cancellation it returns the context's error.
func (b *PLCWriteBudget) WaitForResume(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		remaining := b.pauseRemaining()
		if remaining == 0 {
			return nil
		}
		if err := b.wait(ctx, remaining); err != nil {
			return err
		}
	}
}

// Paused reports whether a pause is in force.
func (b *PLCWriteBudget) Paused() bool {
	return b.pauseRemaining() > 0
}

// WaitUnlessPaused paces like Wait, but returns ErrPLCWritesPaused at once, with
// no token, when a pause is active or starts while it waits. On cancellation it
// returns the context's error and charges nothing.
func (b *PLCWriteBudget) WaitUnlessPaused(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		remaining, pauseMoved, paused := b.takeTokenUnlessPaused()
		if paused {
			return ErrPLCWritesPaused
		}
		if remaining == 0 {
			return nil
		}
		if err := b.waitForSlot(ctx, remaining, pauseMoved); err != nil {
			return err
		}
	}
}

// waitForSlot sleeps for delay, or until ctx ends or pauseMoved closes. A closed
// pauseMoved is no error: the caller reads the budget again, because a Retry-After
// of zero moves the pause end and pauses nothing.
func (b *PLCWriteBudget) waitForSlot(ctx context.Context, delay time.Duration, pauseMoved <-chan struct{}) error {
	waitContext, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-pauseMoved:
			cancel()
		case <-waitContext.Done():
		}
	}()
	err := b.wait(waitContext, delay)
	select {
	case <-pauseMoved:
		return nil
	default:
		return err
	}
}

// RateLimited pauses every write for retryAfter, the value of a PLC 429's
// Retry-After header, or for defaultRateLimitPause when it is unusable. The
// pause end only ever moves later.
func (b *PLCWriteBudget) RateLimited(retryAfter string) {
	b.mutex.Lock()
	now := b.now()
	pause := rateLimitPause(retryAfter, now)
	resumeAt := now.Add(pause)
	extended := resumeAt.After(b.pausedUntil)
	if extended {
		b.pausedUntil = resumeAt
		close(b.pauseMoved)
		b.pauseMoved = make(chan struct{})
	}
	b.mutex.Unlock()
	if extended {
		b.logger.Warn("PLC writes paused", "pause", pause.String(), "resume_at", resumeAt)
	}
}

// takeToken charges one token and returns 0, or charges nothing and returns
// how long until a token can be free.
func (b *PLCWriteBudget) takeToken() time.Duration {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.takeTokenLocked(b.now())
}

// takeTokenUnlessPaused is takeToken refused during a pause. It reads pauseMoved
// under the same lock as the pause, so a pause starting after this read is seen
// on that channel and cannot be missed.
func (b *PLCWriteBudget) takeTokenUnlessPaused() (remaining time.Duration, pauseMoved <-chan struct{}, paused bool) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	now := b.now()
	if b.pausedUntil.After(now) {
		return 0, nil, true
	}
	return b.takeTokenLocked(now), b.pauseMoved, false
}

// takeTokenLocked is takeToken for a caller holding the mutex.
func (b *PLCWriteBudget) takeTokenLocked(now time.Time) time.Duration {
	resumeAt := b.nextWrite
	if b.pausedUntil.After(resumeAt) {
		resumeAt = b.pausedUntil
	}
	if resumeAt.After(now) {
		return resumeAt.Sub(now)
	}
	b.nextWrite = now.Add(b.interval)
	return 0
}

// pauseRemaining is how long the current pause has left, 0 when there is none.
func (b *PLCWriteBudget) pauseRemaining() time.Duration {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return max(b.pausedUntil.Sub(b.now()), 0)
}

// waitForDelay sleeps for delay on a timer, or until ctx ends.
func waitForDelay(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
