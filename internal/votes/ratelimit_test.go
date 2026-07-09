package votes

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

func TestIPLimiterIndependentBuckets(t *testing.T) {
	limiter := newIPLimiter(0.001, 1)

	require.True(t, limiter.allow("192.0.2.1"))
	require.False(t, limiter.allow("192.0.2.1"), "the first IP's burst is spent")
	assert.True(t, limiter.allow("192.0.2.2"), "a second IP must have its own bucket")
}

func TestIPLimiterHardCap(t *testing.T) {
	limiter := newIPLimiter(0.001, 1)
	limiter.maxBuckets = 3

	// Many distinct IPs: the map must never grow past the cap.
	for i := 0; i < 50; i++ {
		limiter.allow(fmt.Sprintf("2001:db8::%x", i))
		require.LessOrEqual(t, len(limiter.buckets), 3, "after IP %d", i)
	}
	assert.Len(t, limiter.buckets, 3)

	// At the cap: unknown IPs are refused, established ones still tracked.
	assert.False(t, limiter.allow("198.51.100.9"), "unknown IP at cap is refused")
	assert.False(t, limiter.allow("2001:db8::0"),
		"a known IP still hits its own (spent) bucket, not the cap")
}

func TestIPLimiterCapRecoversAfterSweep(t *testing.T) {
	clock := newTestClock()
	limiter := newIPLimiter(0.001, 1)
	limiter.now = clock.now
	limiter.maxBuckets = 2
	limiter.sweepThreshold = 1

	require.True(t, limiter.allow("192.0.2.1"))
	require.True(t, limiter.allow("192.0.2.2"))
	require.False(t, limiter.allow("192.0.2.3"), "the map is at its cap")

	// Once the residents idle past the TTL and the sweep throttle window
	// passes, the next request sweeps them out and the new IP fits.
	clock.advance(limiterIdleTTL + limiterSweepInterval)
	assert.True(t, limiter.allow("192.0.2.3"))
	assert.Len(t, limiter.buckets, 1)
}

func TestIPLimiterSweepThrottled(t *testing.T) {
	clock := newTestClock()
	limiter := newIPLimiter(0.001, 1)
	limiter.now = clock.now
	limiter.sweepThreshold = 1
	limiter.idleTTL = time.Second

	limiter.allow("192.0.2.1")
	limiter.allow("192.0.2.2") // crosses the threshold: sweeps, stamps lastSweep

	// Both buckets idle past the (tiny) TTL, but the throttle window has
	// not elapsed — the next allow must not sweep them.
	clock.advance(2 * time.Second)
	limiter.allow("192.0.2.3")
	assert.Len(t, limiter.buckets, 3, "no sweep inside the throttle window")

	// Past the window, the sweep runs and reclaims every idle bucket.
	clock.advance(limiterSweepInterval)
	limiter.allow("192.0.2.4")
	assert.Len(t, limiter.buckets, 1, "idle buckets reclaimed once the window passes")
}

func TestIPLimiterRefillFollowsClock(t *testing.T) {
	clock := newTestClock()
	limiter := newIPLimiter(1, 1)
	limiter.now = clock.now

	require.True(t, limiter.allow("192.0.2.1"))
	require.False(t, limiter.allow("192.0.2.1"), "burst spent, no time has passed")

	clock.advance(1500 * time.Millisecond)
	assert.True(t, limiter.allow("192.0.2.1"), "one token refills after a second")
}
