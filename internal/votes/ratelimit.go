package votes

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	// limiterIdleTTL is how long an idle per-IP bucket survives before a
	// sweep may reclaim it (a reclaimed bucket restarts full, which only
	// ever favors the client).
	limiterIdleTTL = 10 * time.Minute
	// limiterSweepInterval throttles sweeps to at most one full-map scan
	// per interval, however hot the endpoint runs — without it a flood of
	// distinct IPs would provoke an O(n) scan under the mutex on every
	// request, turning the limiter itself into the DoS vector.
	limiterSweepInterval = time.Minute
	// limiterSweepThreshold is the map size below which sweeps don't
	// bother running: a few thousand lingering buckets cost less than the
	// scans that would reclaim them.
	limiterSweepThreshold = 10_000
	// limiterMaxBuckets hard-caps the bucket map. At the cap, requests
	// from IPs without a bucket are refused (fail closed): the cap is only
	// reachable during an address-rotation flood (one IPv6 /64 is
	// effectively unlimited addresses, none idle long enough for the TTL
	// sweep), refusing unknown IPs leaves established clients — the
	// AppView's long-lived poller — untouched, and admitting them instead
	// would hand every rotated address a fresh full burst, which is
	// exactly the abuse the limiter exists to stop. Idle buckets age out
	// via the TTL sweep, so a genuinely new client is locked out only
	// while the flood lasts.
	limiterMaxBuckets = 50_000
)

// ipLimiter is a per-client-IP token bucket set (the same golang.org/x/time
// machinery as ingest's mint gate, keyed by IP). Stale buckets are swept
// inline — at most once per sweepInterval — and the map is hard-capped, so
// an address-rotating scraper can balloon neither memory nor scan time.
type ipLimiter struct {
	perSecond rate.Limit
	burst     int

	// Tunables default to the package constants; they are fields (like
	// now) so tests can shrink them.
	idleTTL        time.Duration
	sweepInterval  time.Duration
	sweepThreshold int
	maxBuckets     int
	now            func() time.Time

	mu        sync.Mutex
	lastSweep time.Time
	buckets   map[string]*ipBucket
}

type ipBucket struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

func newIPLimiter(perSecond float64, burst int) *ipLimiter {
	return &ipLimiter{
		perSecond:      rate.Limit(perSecond),
		burst:          burst,
		idleTTL:        limiterIdleTTL,
		sweepInterval:  limiterSweepInterval,
		sweepThreshold: limiterSweepThreshold,
		maxBuckets:     limiterMaxBuckets,
		now:            time.Now,
		buckets:        map[string]*ipBucket{},
	}
}

// allow reports whether one request from ip fits in its bucket. Unknown IPs
// are refused outright while the bucket map sits at maxBuckets (see the
// constant for why fail-closed is the right shape there).
func (l *ipLimiter) allow(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()

	if len(l.buckets) >= l.sweepThreshold && now.Sub(l.lastSweep) >= l.sweepInterval {
		l.lastSweep = now
		for key, bucket := range l.buckets {
			if now.Sub(bucket.lastSeen) > l.idleTTL {
				delete(l.buckets, key)
			}
		}
	}

	bucket, ok := l.buckets[ip]
	if !ok {
		if len(l.buckets) >= l.maxBuckets {
			return false
		}
		bucket = &ipBucket{limiter: rate.NewLimiter(l.perSecond, l.burst)}
		l.buckets[ip] = bucket
	}
	bucket.lastSeen = now
	return bucket.limiter.AllowN(now, 1)
}
