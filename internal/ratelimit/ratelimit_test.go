package ratelimit

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testClock is a hand-cranked clock for driving the limiter's TTL, sweep,
// and refill paths deterministically.
type testClock struct{ t time.Time }

func newTestClock() *testClock {
	return &testClock{t: time.Date(2026, 7, 8, 12, 0, 0, 0, time.UTC)}
}

func (c *testClock) now() time.Time          { return c.t }
func (c *testClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func TestLimiterIndependentBuckets(t *testing.T) {
	limiter := New(0.001, 1)

	require.True(t, limiter.Allow("192.0.2.1"))
	require.False(t, limiter.Allow("192.0.2.1"), "the first key's burst is spent")
	assert.True(t, limiter.Allow("192.0.2.2"), "a second key must have its own bucket")
}

func TestLimiterHardCap(t *testing.T) {
	limiter := New(0.001, 1)
	limiter.MaxBuckets = 3

	// Many distinct keys: the map must never grow past the cap.
	for i := 0; i < 50; i++ {
		limiter.Allow(fmt.Sprintf("2001:db8::%x", i))
		require.LessOrEqual(t, limiter.Size(), 3, "after key %d", i)
	}
	assert.Equal(t, 3, limiter.Size())

	// At the cap: unknown keys are refused, established ones still tracked.
	assert.False(t, limiter.Allow("198.51.100.9"), "unknown key at cap is refused")
	assert.False(t, limiter.Allow("2001:db8::0"),
		"a known key still hits its own (spent) bucket, not the cap")
}

func TestLimiterCapRecoversAfterSweep(t *testing.T) {
	clock := newTestClock()
	limiter := New(0.001, 1)
	limiter.Now = clock.now
	limiter.MaxBuckets = 2
	limiter.SweepThreshold = 1

	require.True(t, limiter.Allow("192.0.2.1"))
	require.True(t, limiter.Allow("192.0.2.2"))
	require.False(t, limiter.Allow("192.0.2.3"), "the map is at its cap")

	// Once the residents idle past the TTL and the sweep throttle window
	// passes, the next request sweeps them out and the new key fits.
	clock.advance(DefaultIdleTTL + DefaultSweepInterval)
	assert.True(t, limiter.Allow("192.0.2.3"))
	assert.Equal(t, 1, limiter.Size())
}

func TestLimiterSweepThrottled(t *testing.T) {
	clock := newTestClock()
	limiter := New(0.001, 1)
	limiter.Now = clock.now
	limiter.SweepThreshold = 1
	limiter.IdleTTL = time.Second

	limiter.Allow("192.0.2.1")
	limiter.Allow("192.0.2.2") // crosses the threshold: sweeps, stamps lastSweep

	// Both buckets idle past the (tiny) TTL, but the throttle window has
	// not elapsed — the next Allow must not sweep them.
	clock.advance(2 * time.Second)
	limiter.Allow("192.0.2.3")
	assert.Equal(t, 3, limiter.Size(), "no sweep inside the throttle window")

	// Past the window, the sweep runs and reclaims every idle bucket.
	clock.advance(DefaultSweepInterval)
	limiter.Allow("192.0.2.4")
	assert.Equal(t, 1, limiter.Size(), "idle buckets reclaimed once the window passes")
}

func TestLimiterRefillFollowsClock(t *testing.T) {
	clock := newTestClock()
	limiter := New(1, 1)
	limiter.Now = clock.now

	require.True(t, limiter.Allow("192.0.2.1"))
	require.False(t, limiter.Allow("192.0.2.1"), "burst spent, no time has passed")

	clock.advance(1500 * time.Millisecond)
	assert.True(t, limiter.Allow("192.0.2.1"), "one token refills after a second")
}
