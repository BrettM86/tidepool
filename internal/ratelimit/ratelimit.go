// Package ratelimit is the bridge's shared admission-control primitive: a
// keyed token-bucket set (golang.org/x/time buckets keyed by client IP,
// signer id, or any other admission key) with idle-bucket sweeping and a
// hard fail-closed cap. It started life as the votes XRPC per-IP limiter
// (task 07) and was extracted in task 11 so the inbox and the public sync
// surface enforce the same discipline:
//
//   - stale buckets are swept inline, at most once per sweep interval, so a
//     flood of distinct keys cannot turn every request into an O(n) scan
//     under the mutex (the limiter must never become the DoS vector);
//   - the bucket map is hard-capped and FAIL-CLOSED: at the cap, requests
//     from unknown keys are refused. The cap is only reachable during a
//     key-rotation flood (one IPv6 /64 is effectively unlimited addresses),
//     refusing unknown keys leaves established clients untouched, and
//     admitting them instead would hand every rotated key a fresh full
//     burst — exactly the abuse the limiter exists to stop. Idle buckets
//     age out via the TTL sweep, so a genuinely new client is locked out
//     only while the flood lasts.
package ratelimit

import (
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// Sampler throttles noisy log lines: it admits at most one event per its
// interval, lock-free, so a refusal flood stays VISIBLE (a sampled Warn)
// without drowning the log — the paired expvar counter carries the true
// rate. Shared by every admission surface (inbox, sync) that logs refusals.
type Sampler struct {
	last     atomic.Int64
	interval int64
}

// NewSampler builds a Sampler admitting at most one event per interval.
func NewSampler(interval time.Duration) *Sampler {
	return &Sampler{interval: int64(interval)}
}

// Allow reports whether an event may be logged now, advancing the window
// when it returns true. Concurrent callers within one window: at most one
// wins.
func (s *Sampler) Allow(now time.Time) bool {
	n := now.UnixNano()
	prev := s.last.Load()
	if n-prev < s.interval {
		return false
	}
	return s.last.CompareAndSwap(prev, n)
}

// Defaults for the sweeping/cap tunables. Exported so consumers' tests can
// reason about them; the rate and burst themselves have no default — every
// surface must choose its own.
const (
	// DefaultIdleTTL is how long an idle bucket survives before a sweep may
	// reclaim it (a reclaimed bucket restarts full, which only ever favors
	// the client).
	DefaultIdleTTL = 10 * time.Minute
	// DefaultSweepInterval throttles sweeps to at most one full-map scan per
	// interval, however hot the surface runs.
	DefaultSweepInterval = time.Minute
	// DefaultSweepThreshold is the map size below which sweeps don't bother
	// running: a few thousand lingering buckets cost less than the scans
	// that would reclaim them.
	DefaultSweepThreshold = 10_000
	// DefaultMaxBuckets hard-caps the bucket map (see the package comment
	// for why the cap fails closed).
	DefaultMaxBuckets = 50_000
)

// Limiter is a keyed token-bucket set. Construct with New; the exported
// tunables may be adjusted before first use (tests shrink them).
type Limiter struct {
	perSecond rate.Limit
	burst     int

	// Tunables default to the package constants. They are plain fields (like
	// Now) so tests can shrink them; mutate only before concurrent use.
	IdleTTL        time.Duration
	SweepInterval  time.Duration
	SweepThreshold int
	MaxBuckets     int
	// Now is the clock (test seam).
	Now func() time.Time

	mu        sync.Mutex
	lastSweep time.Time
	buckets   map[string]*bucket
}

type bucket struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// New builds a Limiter allowing perSecond sustained requests per key with
// the given burst.
func New(perSecond float64, burst int) *Limiter {
	return &Limiter{
		perSecond:      rate.Limit(perSecond),
		burst:          burst,
		IdleTTL:        DefaultIdleTTL,
		SweepInterval:  DefaultSweepInterval,
		SweepThreshold: DefaultSweepThreshold,
		MaxBuckets:     DefaultMaxBuckets,
		Now:            time.Now,
		buckets:        map[string]*bucket{},
	}
}

// Allow reports whether one request from key fits in its bucket. Unknown
// keys are refused outright while the bucket map sits at MaxBuckets (fail
// closed; see the package comment).
func (l *Limiter) Allow(key string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.Now()

	if len(l.buckets) >= l.SweepThreshold && now.Sub(l.lastSweep) >= l.SweepInterval {
		l.lastSweep = now
		for k, b := range l.buckets {
			if now.Sub(b.lastSeen) > l.IdleTTL {
				delete(l.buckets, k)
			}
		}
	}

	b, ok := l.buckets[key]
	if !ok {
		if len(l.buckets) >= l.MaxBuckets {
			return false
		}
		b = &bucket{limiter: rate.NewLimiter(l.perSecond, l.burst)}
		l.buckets[key] = b
	}
	b.lastSeen = now
	return b.limiter.AllowN(now, 1)
}

// Size reports the current bucket count (tests).
func (l *Limiter) Size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.buckets)
}

// ClientIP extracts the connection's remote IP from a request. Deliberately
// not X-Forwarded-For: the bridge cannot know which proxies to trust, and a
// spoofable header would let one client exhaust every bucket. Deployments
// behind a load balancer rate-limit the real client at the edge.
func ClientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
