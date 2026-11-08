package votes

import (
	"context"
	stderrors "errors"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
)

// Origin verification (PLAN.md decision 21). An inbound vote reaches us inside
// the community's Announce, which signs only the community; nothing proves the
// voter cast it. Before a vote may become a record in the voter's repo, its
// activity is fetched from the voter's own instance and must match the claim.
// Anything not verified stays in the aggregate.

// VoteClaim is what an inbound vote claims: its activity id, voter, subject
// and type (ap.TypeLike or ap.TypeDislike).
type VoteClaim struct {
	ActivityID, Actor, Object, Type string
}

// OriginVerdict is the stored outcome of checking a vote at the voter's origin.
type OriginVerdict string

const (
	// OriginVerified: the origin serves the claimed activity. Final.
	OriginVerified OriginVerdict = "verified"
	// OriginMismatch: the activity id is not a vote activity URL on the
	// voter's host, the origin answered with a redirect, or the served
	// document is a different activity (an empty id included). Final.
	OriginMismatch OriginVerdict = "mismatch"
	// OriginNotFound: 400/401/403/404/410 or a Tombstone. Final: an origin
	// that refuses our signed fetch (401/403) is not asked again.
	OriginNotFound OriginVerdict = "not_found"
	// OriginExpired: still unavailable when the vote left the window. Final.
	OriginExpired OriginVerdict = "expired"
	// OriginUnavailable: timeout, network error, 429, 5xx, an unparseable
	// body or an egress refusal (the SSRF guard). Retried inside the window.
	OriginUnavailable OriginVerdict = "unavailable"
)

// OriginVerifier checks a vote claim at the voter's origin. deferred means no
// fetch slot was free (rate limit or host backoff) or the caller's context
// ended first (the inline 2s, the sweep's per-fetch deadline, shutdown); the
// verdict is then "" and nothing is stored.
type OriginVerifier interface {
	Verify(ctx context.Context, claim VoteClaim) (verdict OriginVerdict, deferred bool)
}

// OriginFetcher fetches an object whose final URL keeps the requested
// authority (*ap.Client).
type OriginFetcher interface {
	FetchObjectSameAuthority(ctx context.Context, iri string) (*ap.Object, error)
	RefusesRedirects() bool
}

// OriginVerifierOptions pace the verifier. HostRPS and GlobalRPS are fetch
// START rates (burst 1); Timeout bounds the caller's wait for one fetch. In
// production the fetcher is a dedicated client that never retries.
type OriginVerifierOptions struct {
	HostRPS   float64
	GlobalRPS float64
	Timeout   time.Duration
	Now       func() time.Time
}

const (
	// DefaultVerifyHostRPS is the default fetch start rate per voter host.
	DefaultVerifyHostRPS = 1
	// DefaultVerifyGlobalRPS is the default fetch start rate over all hosts.
	DefaultVerifyGlobalRPS = 5
	// DefaultVerifyTimeout is the default bound on one fetch; a caller's
	// shorter deadline wins.
	DefaultVerifyTimeout = 10 * time.Second

	// A host that answered 'unavailable' is not fetched for originFirstBackoff,
	// doubling per further failure (at most originMaxBackoffShift doublings)
	// up to originMaxBackoff; any definitive answer resets it.
	originFirstBackoff    = 30 * time.Second
	originMaxBackoff      = 15 * time.Minute
	originMaxBackoffShift = 5
)

// NewOriginVerifier builds the origin verifier. Non-positive options take the
// defaults.
func NewOriginVerifier(fetcher OriginFetcher, opts OriginVerifierOptions, logger *slog.Logger) (OriginVerifier, error) {
	if fetcher == nil {
		return nil, errors.NewValidationError("fetcher", "must not be nil")
	}
	if !fetcher.RefusesRedirects() {
		// An open redirect on the voter's host could serve any document as
		// the activity.
		return nil, errors.NewValidationError("fetcher", "must refuse redirects")
	}
	if opts.HostRPS <= 0 {
		opts.HostRPS = DefaultVerifyHostRPS
	}
	if opts.GlobalRPS <= 0 {
		opts.GlobalRPS = DefaultVerifyGlobalRPS
	}
	if opts.Timeout <= 0 {
		opts.Timeout = DefaultVerifyTimeout
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &originVerifier{
		fetcher: fetcher, hostRPS: rate.Limit(opts.HostRPS), timeout: opts.Timeout, now: opts.Now, logger: logger,
		global: rate.NewLimiter(rate.Limit(opts.GlobalRPS), 1),
		hosts:  map[string]*originHost{},
	}, nil
}

type originVerifier struct {
	fetcher OriginFetcher
	hostRPS rate.Limit
	timeout time.Duration
	now     func() time.Time
	logger  *slog.Logger

	// mu guards the buckets and backoffs; it is never held during a fetch.
	// The host map grows with the hosts of eligible voters only (all in
	// memory; a restart starts every bucket full).
	mu     sync.Mutex
	global *rate.Limiter
	hosts  map[string]*originHost
}

type originHost struct {
	bucket       *rate.Limiter
	backoffUntil time.Time
	failures     int
}

func (v *originVerifier) Verify(ctx context.Context, claim VoteClaim) (OriginVerdict, bool) {
	if !voteActivityURL(claim.ActivityID) || !ap.SameAuthority(claim.ActivityID, claim.Actor) {
		return OriginMismatch, false
	}
	host := hostOf(claim.Actor)
	if !v.admit(host) {
		return "", true
	}
	fetchCtx, cancel := context.WithTimeout(ctx, v.timeout)
	fetched, err := v.fetcher.FetchObjectSameAuthority(fetchCtx, claim.ActivityID)
	cancel()
	if ctx.Err() != nil {
		// Our caller gave up (shutdown), not the origin: nothing to store.
		return "", true
	}
	verdict := classifyOrigin(claim, fetched, err)
	v.settle(host, verdict)
	if verdict == OriginUnavailable {
		v.logger.Debug("vote origin unavailable", "activity", claim.ActivityID, "error", err)
	}
	return verdict, false
}

// admit is the paired admission check: a fetch starts only when the host is
// not backing off and BOTH the host's and the global bucket hold a token now.
// A refusal consumes neither.
func (v *originVerifier) admit(host string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	now := v.now()
	h := v.hosts[host]
	if h == nil {
		h = &originHost{bucket: rate.NewLimiter(v.hostRPS, 1)}
		v.hosts[host] = h
	}
	if now.Before(h.backoffUntil) || h.bucket.TokensAt(now) < 1 || v.global.TokensAt(now) < 1 {
		return false
	}
	h.bucket.AllowN(now, 1)
	v.global.AllowN(now, 1)
	return true
}

// settle moves the host's backoff after a fetch.
func (v *originVerifier) settle(host string, verdict OriginVerdict) {
	v.mu.Lock()
	defer v.mu.Unlock()
	h := v.hosts[host]
	if verdict != OriginUnavailable {
		h.failures, h.backoffUntil = 0, time.Time{}
		return
	}
	backoff := originFirstBackoff << min(h.failures, originMaxBackoffShift)
	h.failures++
	h.backoffUntil = v.now().Add(min(backoff, originMaxBackoff))
}

// classifyOrigin maps a fetch outcome to a verdict.
func classifyOrigin(claim VoteClaim, fetched *ap.Object, err error) OriginVerdict {
	if err != nil {
		var httpErr ap.HTTPError
		switch {
		case errors.IsNotFound(err), errors.IsTombstoned(err):
			return OriginNotFound
		case isRefusedRedirect(err):
			return OriginMismatch
		case stderrors.As(err, &httpErr):
			switch httpErr.StatusCode {
			case 400, 401, 403, 404, 410:
				// Lemmy answers a missing activity with 400.
				return OriginNotFound
			}
		}
		return OriginUnavailable
	}
	if fetched == nil {
		return OriginUnavailable
	}
	if fetched.IsTombstone() {
		return OriginNotFound
	}
	if fetched.ID != claim.ActivityID || fetched.Type != claim.Type ||
		refID(fetched.Actor) != claim.Actor || refID(fetched.Object) != claim.Object {
		return OriginMismatch
	}
	return OriginVerified
}

// isRefusedRedirect reports the fetcher's refusal to follow a redirect (the
// verification client follows none). Other validation errors (the egress guard, a bad URL)
// prove nothing about the vote and are 'unavailable'.
func isRefusedRedirect(err error) bool {
	var validation errors.ValidationError
	return stderrors.As(err, &validation) && validation.Field == "redirect"
}

// voteActivityURL reports whether id has the shape Lemmy and PieFed give vote
// activities: a path of exactly /activities/like/<segment> or
// /activities/dislike/<segment> (prefix case-insensitive), one non-empty
// segment that is not "." or "..", no percent-escape anywhere, no query or
// fragment. Anything else could resolve to a user upload on the voter's host
// that merely looks like the claimed activity.
func voteActivityURL(id string) bool {
	u, err := url.Parse(id)
	if err != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(id, "#") {
		return false
	}
	path := u.EscapedPath()
	if strings.Contains(path, "%") {
		return false
	}
	lower := strings.ToLower(path)
	var segment string
	switch {
	case strings.HasPrefix(lower, "/activities/like/"):
		segment = path[len("/activities/like/"):]
	case strings.HasPrefix(lower, "/activities/dislike/"):
		segment = path[len("/activities/dislike/"):]
	default:
		return false
	}
	return segment != "" && segment != "." && segment != ".." && !strings.Contains(segment, "/")
}

// hostOf is the lower-cased host of an absolute URL ("" when unparseable).
func hostOf(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Host)
}
