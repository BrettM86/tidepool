package ap

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	"golang.org/x/time/rate"

	"tidepool/internal/errors"
)

// Client defaults. All are overridable through ClientOptions.
const (
	// DefaultMaxResponseBytes caps AP response bodies. Lemmy objects are a
	// few KB; a 50-item outbox page is a few hundred KB. Anything past a few
	// MB is either abuse or a collection we should be paging through.
	DefaultMaxResponseBytes = 5 << 20 // 5 MiB
	// DefaultPerHostRPS rate-limits outbound requests per remote host.
	DefaultPerHostRPS = 5
	// DefaultPerHostBurst allows short bursts (ancestor-chain fetches).
	DefaultPerHostBurst = 10
	// DefaultMaxAttempts bounds retries for transient failures (5xx, 429,
	// network errors): initial request plus two retries.
	DefaultMaxAttempts = 3
	// DefaultRetryBaseDelay is the first backoff step (doubles per attempt,
	// with jitter).
	DefaultRetryBaseDelay = 500 * time.Millisecond
	// DefaultMaxCollectionPages caps how many pages FetchCollection walks.
	DefaultMaxCollectionPages = 10
	// DefaultRequestTimeout bounds a single HTTP attempt.
	DefaultRequestTimeout = 30 * time.Second
	// DefaultKeyCacheTTL is how long resolved actor public keys are cached.
	// bridgy-fed caches keys aggressively for the same reason: key PEMs are
	// stable and re-fetching actors per-delivery is the main cost of
	// signature verification.
	DefaultKeyCacheTTL = time.Hour
	// maxRedirects bounds manually-followed redirects. Redirects are
	// re-signed per hop ((request-target) changes), like bridgy-fed does.
	maxRedirects = 5
	// defaultMaxKeyCacheEntries bounds the resolved-key cache so unique,
	// attacker-controlled keyIds cannot grow it without limit.
	defaultMaxKeyCacheEntries = 4096
	// defaultMaxLimiters bounds the per-host rate-limiter map for the same
	// reason (unique hostnames).
	defaultMaxLimiters = 4096
)

// ErrStop can be returned by a FetchCollection visit callback to stop paging
// early without reporting an error.
var ErrStop = stderrors.New("ap: stop iteration")

// ErrCollectionTruncated marks a FetchCollection walk that stopped before
// reaching the end of the collection (page cap or a next-pointer loop). It is
// distinct from a clean completion so backfill (task 05) can resume or log
// rather than silently miss older items. Use errors.As to recover the
// *CollectionTruncatedError for the resume point.
var ErrCollectionTruncated = stderrors.New("ap: collection truncated")

// CollectionTruncatedError carries where a truncated walk stopped.
type CollectionTruncatedError struct {
	// Pages is how many pages were walked before stopping.
	Pages int
	// Next is the unfetched next-page IRI (empty if truncation was a loop).
	Next string
}

func (e *CollectionTruncatedError) Error() string {
	if e.Next != "" {
		return fmt.Sprintf("ap: collection truncated after %d pages, next=%s", e.Pages, e.Next)
	}
	return fmt.Sprintf("ap: collection truncated after %d pages", e.Pages)
}

func (e *CollectionTruncatedError) Unwrap() error { return ErrCollectionTruncated }

// HTTPError reports a non-success AP response that is not otherwise mapped
// to a sentinel (404 → ErrNotFound, 410 → ErrTombstoned are mapped instead).
type HTTPError struct {
	URL        string
	StatusCode int
}

func (e HTTPError) Error() string {
	return fmt.Sprintf("ap: GET %s: unexpected status %d", e.URL, e.StatusCode)
}

// ClientOptions configures a Client. The zero value of every field means
// "use the default above".
type ClientOptions struct {
	// UserAgent is sent on every request (config.UserAgent).
	UserAgent string
	// Signer signs outbound requests. Optional: nil sends unsigned requests
	// (many instances require authorized fetch, so production always sets
	// it; tests may not).
	Signer *Signer
	// HTTPClient overrides the underlying client (tests). Redirects are
	// handled by Client itself; any CheckRedirect on this client is
	// replaced.
	HTTPClient *http.Client
	// MaxResponseBytes caps response bodies.
	MaxResponseBytes int64
	// PerHostRPS / PerHostBurst configure per-host rate limiting.
	PerHostRPS   float64
	PerHostBurst int
	// MaxAttempts bounds attempts per request (1 = no retries).
	MaxAttempts int
	// RetryBaseDelay is the first backoff step.
	RetryBaseDelay time.Duration
	// MaxCollectionPages caps FetchCollection paging.
	MaxCollectionPages int
	// KeyCacheTTL is how long resolved public keys are cached.
	KeyCacheTTL time.Duration
	// AllowPrivateAddresses disables the SSRF egress guard so the client may
	// fetch loopback/private addresses. Default false (guard on); wire it from
	// config.AllowPrivateAddresses. Tests that hit httptest servers on
	// 127.0.0.1 set it true.
	AllowPrivateAddresses bool
}

// Client fetches ActivityPub objects with signed GETs, per-host rate
// limiting, retries, response-size caps, and in-flight dedupe. It also sends
// signed POSTs (task 06 uses this for Follow) and implements KeyResolver for
// signature verification.
type Client struct {
	httpClient         *http.Client
	userAgent          string
	signer             *Signer
	maxResponseBytes   int64
	perHostRPS         rate.Limit
	perHostBurst       int
	maxAttempts        int
	retryBaseDelay     time.Duration
	maxCollectionPages int
	keyCacheTTL        time.Duration
	guard              *egressGuard
	// requestBudget bounds a deduped fetch's detached lifetime (see
	// getDeduped): the shared request outlives the initiating caller's
	// context, so it needs its own upper bound.
	requestBudget time.Duration

	inflight singleflight.Group

	mu       sync.Mutex
	limiters map[string]*limiterEntry
	keyCache map[string]cachedKey
	// maxKeyCacheEntries / maxLimiters bound the per-keyID and per-host maps
	// so an attacker minting unique keyIds/hosts cannot grow them without
	// limit. Overridable in tests.
	maxKeyCacheEntries int
	maxLimiters        int

	// sleep is stubbed in tests to avoid real backoff waits.
	sleep func(ctx context.Context, d time.Duration) error
	// now is stubbed in tests for key-cache expiry.
	now func() time.Time
}

type cachedKey struct {
	key       *rsa.PublicKey
	ownerID   string
	expiresAt time.Time
}

type limiterEntry struct {
	limiter  *rate.Limiter
	lastUsed time.Time
}

// NewClient creates a Client with the given options.
func NewClient(opts ClientOptions) *Client {
	guard := newEgressGuard(opts.AllowPrivateAddresses)

	// Shallow-copy the caller's client so we never mutate their value (setting
	// CheckRedirect/Transport on a shared *http.Client is a surprising side
	// effect), and default the request timeout when unset.
	var httpClient http.Client
	if opts.HTTPClient != nil {
		httpClient = *opts.HTTPClient
	}
	if httpClient.Timeout == 0 {
		httpClient.Timeout = DefaultRequestTimeout
	}
	// Redirects are followed manually so each hop gets a fresh signature.
	httpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}
	// Install the SSRF dial guard on the transport so the actually-resolved IP
	// is validated (defeating DNS rebinding), not just the hostname string.
	httpClient.Transport = guardedTransport(httpClient.Transport, guard)

	c := &Client{
		httpClient:         &httpClient,
		userAgent:          opts.UserAgent,
		signer:             opts.Signer,
		maxResponseBytes:   opts.MaxResponseBytes,
		perHostRPS:         rate.Limit(opts.PerHostRPS),
		perHostBurst:       opts.PerHostBurst,
		maxAttempts:        opts.MaxAttempts,
		retryBaseDelay:     opts.RetryBaseDelay,
		maxCollectionPages: opts.MaxCollectionPages,
		keyCacheTTL:        opts.KeyCacheTTL,
		guard:              guard,
		limiters:           make(map[string]*limiterEntry),
		keyCache:           make(map[string]cachedKey),
		maxKeyCacheEntries: defaultMaxKeyCacheEntries,
		maxLimiters:        defaultMaxLimiters,
		now:                time.Now,
		sleep: func(ctx context.Context, d time.Duration) error {
			timer := time.NewTimer(d)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		},
	}
	if c.userAgent == "" {
		c.userAgent = "tidepool/0.1"
	}
	if c.maxResponseBytes <= 0 {
		c.maxResponseBytes = DefaultMaxResponseBytes
	}
	if c.perHostRPS <= 0 {
		c.perHostRPS = DefaultPerHostRPS
	}
	if c.perHostBurst <= 0 {
		c.perHostBurst = DefaultPerHostBurst
	}
	if c.maxAttempts <= 0 {
		c.maxAttempts = DefaultMaxAttempts
	}
	if c.retryBaseDelay <= 0 {
		c.retryBaseDelay = DefaultRetryBaseDelay
	}
	if c.maxCollectionPages <= 0 {
		c.maxCollectionPages = DefaultMaxCollectionPages
	}
	if c.keyCacheTTL <= 0 {
		c.keyCacheTTL = DefaultKeyCacheTTL
	}
	// A deduped fetch runs detached from the initiating caller's context, so
	// bound it: worst case is every attempt timing out plus its backoff.
	c.requestBudget = time.Duration(c.maxAttempts)*DefaultRequestTimeout +
		time.Duration(c.maxAttempts)*c.retryBaseDelay
	return c
}

// guardedTransport returns a RoundTripper that validates the resolved IP at
// dial time. When base is a *http.Transport (the common case, including the
// default and httptest clients) it clones it and wraps DialContext; any other
// RoundTripper is returned unchanged (URL-level checks in waitForHost still
// apply, but such transports are only used in bespoke tests).
func guardedTransport(base http.RoundTripper, guard *egressGuard) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	t, ok := base.(*http.Transport)
	if !ok {
		return base
	}
	cloned := t.Clone()
	baseDial := cloned.DialContext
	if baseDial == nil {
		baseDial = (&net.Dialer{Timeout: DefaultRequestTimeout, KeepAlive: 30 * time.Second}).DialContext
	}
	cloned.DialContext = guard.dialContext(baseDial)
	return cloned
}

// FetchObject fetches an AP object by IRI with a signed GET.
//
// Error mapping (typed so the materializer can branch):
//   - 404 (and 401/403, which secure-mode instances return for objects we
//     cannot see) → errors.IsNotFound;
//   - 410 Gone → errors.IsTombstoned — the object was deleted upstream, the
//     caller must treat it as a tombstone, never re-fetch;
//   - a 200 whose body is a Tombstone object → errors.IsTombstoned (Lemmy
//     serves Tombstones with 410, Mastodon with 404, others with 200);
//   - other non-2xx → HTTPError.
func (c *Client) FetchObject(ctx context.Context, iri string) (*Object, error) {
	return c.fetchObject(ctx, iri, fetchModeObject)
}

// fetchObject is FetchObject parameterized by fetch mode (status mapping and
// redirect policy — see fetchMode).
func (c *Client) fetchObject(ctx context.Context, iri string, mode fetchMode) (*Object, error) {
	body, err := c.getDedupedMode(ctx, iri, mode)
	if err != nil {
		return nil, err
	}
	obj, err := ParseObject(body)
	if err != nil {
		return nil, fmt.Errorf("ap: fetch %s: %w", iri, err)
	}
	if obj.IsTombstone() {
		return nil, errors.NewTombstonedError("ap_object", iri)
	}
	return obj, nil
}

// SameAuthority reports whether two absolute URLs share scheme and host (the
// authority). Exported so consumers that key storage on a fetched object's
// self-asserted id (the materializer's ap_objects mapping) can reject a body
// claiming an id on a different instance than the one it was fetched from.
func SameAuthority(a, b string) bool { return sameAuthority(a, b) }

// FetchActor fetches an AP actor document and validates that it is one
// (Group/Person/Application/Service with an id).
func (c *Client) FetchActor(ctx context.Context, iri string) (*Object, error) {
	return c.fetchActor(ctx, iri, fetchModeObject)
}

// FetchActorSameAuthority is FetchActor with the redirect authority pinned to
// the requested IRI: any redirect hop whose target does not share the IRI's
// scheme+host fails the fetch with errors.IsValidation. The inbox's tombstone
// confirmation (ingest.tombstonedSelfDelete) uses it because that fetch is an
// authorization decision about the IRI's own origin — a compromised or
// open-redirecting origin must not be able to bounce the confirmation to an
// attacker host that serves 410. Same-authority redirects (a path move within
// the origin) are still followed; ordinary fetches keep the permissive
// redirect behavior.
func (c *Client) FetchActorSameAuthority(ctx context.Context, iri string) (*Object, error) {
	return c.fetchActor(ctx, iri, fetchModeObjectPinnedAuthority)
}

// fetchActor is FetchActor parameterized by fetch mode.
func (c *Client) fetchActor(ctx context.Context, iri string, mode fetchMode) (*Object, error) {
	obj, err := c.fetchObject(ctx, iri, mode)
	if err != nil {
		return nil, err
	}
	if !obj.IsActor() {
		return nil, errors.NewValidationError("actor",
			fmt.Sprintf("object %s has type %q, want an actor type", iri, obj.Type))
	}
	if obj.ID == "" {
		return nil, errors.NewValidationError("actor", "actor document has no id")
	}
	return obj, nil
}

// FetchCollection pages through a Collection/OrderedCollection, calling
// visit for every item. It handles both inline items (Lemmy outboxes are a
// single OrderedCollection with orderedItems) and paged collections
// (first/next chains, Mastodon style). Paging stops after MaxCollectionPages
// pages, when next is absent, or when visit returns an error (ErrStop stops
// silently; anything else propagates).
func (c *Client) FetchCollection(ctx context.Context, iri string, visit func(*Object) error) error {
	coll, err := c.FetchObject(ctx, iri)
	if err != nil {
		return err
	}
	if !coll.IsCollection() {
		return errors.NewValidationError("collection",
			fmt.Sprintf("object %s has type %q, want a collection type", iri, coll.Type))
	}

	seen := map[string]bool{}
	if coll.ID != "" {
		seen[coll.ID] = true
	}
	page := coll
	for pageCount := 1; ; pageCount++ {
		if err := visitItems(page, visit); err != nil {
			if stderrors.Is(err, ErrStop) {
				return nil
			}
			return err
		}

		nextIRI := ""
		switch {
		case page.Next != nil && page.Next.ID != "":
			nextIRI = page.Next.ID
		case len(page.Items)+len(page.OrderedItems) == 0 && page.First != nil:
			// A collection header without inline items: descend into first,
			// which may be an inline page object or a bare IRI.
			first := page.First
			if len(first.Items)+len(first.OrderedItems) > 0 || first.Type != "" {
				page = first
				if page.ID != "" {
					if seen[page.ID] {
						return &CollectionTruncatedError{Pages: pageCount}
					}
					seen[page.ID] = true
				}
				continue
			}
			nextIRI = first.ID
		}
		if nextIRI == "" {
			// No further pages: the walk reached the end of the collection.
			return nil
		}
		if pageCount >= c.maxCollectionPages {
			// Page cap hit with more to fetch: report truncation so the caller
			// can resume rather than mistaking it for a complete walk.
			return &CollectionTruncatedError{Pages: pageCount, Next: nextIRI}
		}
		if seen[nextIRI] {
			// A next-pointer loop: stop, but signal that we did not reach the
			// natural end of the collection.
			return &CollectionTruncatedError{Pages: pageCount, Next: nextIRI}
		}
		seen[nextIRI] = true

		page, err = c.FetchObject(ctx, nextIRI)
		if err != nil {
			return fmt.Errorf("ap: fetch collection page %s: %w", nextIRI, err)
		}
		// A next pointer must resolve to a collection page. If it resolves to
		// something else (a Note, an actor, an error page served with 200),
		// treat it as a broken chain rather than a clean end-of-collection.
		if !page.IsCollection() {
			return errors.NewValidationError("collection",
				fmt.Sprintf("collection page %s has type %q, want a collection page type", nextIRI, page.Type))
		}
	}
}

func visitItems(page *Object, visit func(*Object) error) error {
	for i := range page.OrderedItems {
		if err := visit(&page.OrderedItems[i]); err != nil {
			return err
		}
	}
	for i := range page.Items {
		if err := visit(&page.Items[i]); err != nil {
			return err
		}
	}
	return nil
}

// FetchMedia fetches a binary resource (avatar, banner, post image) with the
// same SSRF egress guard, per-host rate limiting, retries, redirect handling,
// and signing as AP object fetches. maxBytes caps the response body for this
// call (values <= 0 or above the client cap fall back to the client cap) —
// the materializer passes its blob-size budget so oversized media fails
// closed at read time instead of buffering the client-wide maximum.
//
// It returns the body and the response Content-Type (as sent by the server,
// unparsed; empty when the server sent none). Status mapping matches
// FetchObject: 404/401/403 → IsNotFound, 410 → IsTombstoned, other non-2xx
// → HTTPError. Content-type policy (images only) is the caller's job — the
// transport layer cannot know which lexicon slot the bytes are for.
func (c *Client) FetchMedia(ctx context.Context, iri string, maxBytes int64) (data []byte, contentType string, err error) {
	if maxBytes <= 0 || maxBytes > c.maxResponseBytes {
		maxBytes = c.maxResponseBytes
	}
	var lastErr error
	for attempt := 0; attempt < c.maxAttempts; attempt++ {
		if err := c.backoff(ctx, attempt); err != nil {
			return nil, "", err
		}
		data, contentType, retryable, err := c.fetchMediaOnce(ctx, iri, maxBytes)
		if err == nil {
			return data, contentType, nil
		}
		if !retryable {
			return nil, "", err
		}
		lastErr = err
	}
	return nil, "", lastErr
}

// fetchMediaOnce performs one media GET, following up to maxRedirects
// manually (re-signing each hop, like getOnce).
func (c *Client) fetchMediaOnce(ctx context.Context, iri string, maxBytes int64) (data []byte, contentType string, retryable bool, err error) {
	target := iri
	for redirects := 0; ; redirects++ {
		if err := c.waitForHost(ctx, target); err != nil {
			return nil, "", false, err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return nil, "", false, fmt.Errorf("ap: build GET %s: %w", target, err)
		}
		req.Header.Set("User-Agent", c.userAgent)
		req.Header.Set("Accept", "image/*, */*;q=0.5")
		if c.signer != nil {
			if err := c.signer.SignRequest(req, nil); err != nil {
				return nil, "", false, err
			}
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, "", true, fmt.Errorf("ap: GET %s: %w", target, err)
		}

		if isRedirect(resp.StatusCode) {
			location := resp.Header.Get("Location")
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			if location == "" {
				return nil, "", false, HTTPError{URL: target, StatusCode: resp.StatusCode}
			}
			if redirects >= maxRedirects {
				return nil, "", false, fmt.Errorf("ap: GET %s: too many redirects", iri)
			}
			next, err := url.Parse(location)
			if err != nil {
				return nil, "", false, fmt.Errorf("ap: GET %s: bad redirect location %q: %w", target, location, err)
			}
			resolved := req.URL.ResolveReference(next)
			if err := c.checkRedirectScheme(req.URL, resolved); err != nil {
				return nil, "", false, fmt.Errorf("ap: GET %s: %w", target, err)
			}
			target = resolved.String()
			continue
		}

		body, readErr := func() ([]byte, error) {
			defer func() { _ = resp.Body.Close() }()
			body, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
			if err != nil {
				return nil, fmt.Errorf("read response: %w", err)
			}
			if int64(len(body)) > maxBytes {
				return nil, fmt.Errorf("response exceeds %d byte cap", maxBytes)
			}
			return body, nil
		}()
		if readErr != nil {
			return nil, "", false, fmt.Errorf("ap: GET %s: %w", target, readErr)
		}

		switch {
		case resp.StatusCode >= 200 && resp.StatusCode < 300:
			return body, resp.Header.Get("Content-Type"), false, nil
		case resp.StatusCode == http.StatusGone:
			return nil, "", false, errors.NewTombstonedError("ap_media", iri)
		case resp.StatusCode == http.StatusNotFound,
			resp.StatusCode == http.StatusUnauthorized,
			resp.StatusCode == http.StatusForbidden:
			return nil, "", false, errors.NewNotFoundError("ap_media", iri)
		case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
			return nil, "", true, HTTPError{URL: iri, StatusCode: resp.StatusCode}
		default:
			return nil, "", false, HTTPError{URL: iri, StatusCode: resp.StatusCode}
		}
	}
}

// SendActivity signed-POSTs an activity to a remote inbox (task 06 uses this
// for Follow/Undo). The activity is JSON-encoded as-is; a Signer must be
// configured.
func (c *Client) SendActivity(ctx context.Context, inboxURL string, activity any) error {
	if c.signer == nil {
		return errors.NewValidationError("signer", "SendActivity requires a configured Signer")
	}
	payload, err := json.Marshal(activity)
	if err != nil {
		return fmt.Errorf("ap: encode activity: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt < c.maxAttempts; attempt++ {
		if err := c.backoff(ctx, attempt); err != nil {
			return err
		}
		if err := c.waitForHost(ctx, inboxURL); err != nil {
			return err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, inboxURL, bytes.NewReader(payload))
		if err != nil {
			return fmt.Errorf("ap: build POST %s: %w", inboxURL, err)
		}
		req.Header.Set("User-Agent", c.userAgent)
		req.Header.Set("Content-Type", ContentTypeActivityJSON)
		if err := c.signer.SignRequest(req, payload); err != nil {
			return err
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("ap: POST %s: %w", inboxURL, err)
			continue
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()

		switch {
		case resp.StatusCode >= 200 && resp.StatusCode < 300:
			return nil
		case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
			lastErr = HTTPError{URL: inboxURL, StatusCode: resp.StatusCode}
			continue
		default:
			return HTTPError{URL: inboxURL, StatusCode: resp.StatusCode}
		}
	}
	return lastErr
}

// ResolveKey implements KeyResolver: it resolves a signature keyId to the
// owning actor's RSA public key, caching results for KeyCacheTTL. The actor
// document is fetched from the keyId minus fragment (Lemmy convention:
// "{actor}#main-key").
func (c *Client) ResolveKey(ctx context.Context, keyID string) (*rsa.PublicKey, string, error) {
	key, ownerID, _, err := c.ResolveKeyDetailed(ctx, keyID)
	return key, ownerID, err
}

// ResolveKeyDetailed is ResolveKey plus cache provenance: fromCache reports
// whether the key was served from the positive cache. The Verifier uses it
// to gate its rotation retry — only a cached key can be stale, so only a
// cached key that fails verification earns a fresh re-fetch (bounding the
// outbound-fetch amplification a forged delivery can cause).
func (c *Client) ResolveKeyDetailed(ctx context.Context, keyID string) (key *rsa.PublicKey, ownerID string, fromCache bool, err error) {
	c.mu.Lock()
	cached, ok := c.keyCache[keyID]
	c.mu.Unlock()
	if ok && c.now().Before(cached.expiresAt) {
		return cached.key, cached.ownerID, true, nil
	}
	key, ownerID, err = c.resolveKeyUncached(ctx, keyID)
	return key, ownerID, false, err
}

// resolveKeyUncached fetches and validates the actor for keyID, bypassing the
// positive cache. It writes a positive cache entry only on success — a
// spoofed or mismatched resolution is never cached.
func (c *Client) resolveKeyUncached(ctx context.Context, keyID string) (*rsa.PublicKey, string, error) {
	// The URL we will fetch is derived solely from the keyId. The signer's
	// claimed identity (actor.ID) is NOT trusted until it is shown to belong
	// to the same authority as this fetch URL — otherwise a malicious host at
	// https://evil.example/spoof could serve an actor document claiming
	// id=https://lemmy.world/u/alice and we would attribute the signature to
	// alice.
	fetchURL := ActorIDFromKeyID(keyID)
	actor, err := c.FetchActor(ctx, fetchURL)
	if err != nil {
		return nil, "", err
	}
	if actor.PublicKey == nil || actor.PublicKey.PublicKeyPem == "" {
		return nil, "", errors.NewValidationError("publicKey",
			fmt.Sprintf("actor %s publishes no publicKeyPem", fetchURL))
	}
	// The self-asserted actor id must live on the same authority (scheme+host
	// +port) as the keyId-derived fetch URL. This is the core actor-identity
	// binding: it stops a cross-host actor document from claiming another
	// host's identity.
	if !sameAuthority(actor.ID, fetchURL) {
		return nil, "", SignatureError{Reason: fmt.Sprintf(
			"actor id %q is not on the same authority as key id %q", actor.ID, keyID)}
	}
	// If the actor names its key, it must be the one we were asked for —
	// otherwise any actor could satisfy any keyId.
	if actor.PublicKey.ID != "" && actor.PublicKey.ID != keyID {
		return nil, "", SignatureError{Reason: fmt.Sprintf(
			"actor %s publishes key %q, not %q", actor.ID, actor.PublicKey.ID, keyID)}
	}
	// When the key names its owner, it must be the actor that published it.
	if actor.PublicKey.Owner != "" && actor.PublicKey.Owner != actor.ID {
		return nil, "", SignatureError{Reason: fmt.Sprintf(
			"key %q claims owner %q but is published by %q", keyID, actor.PublicKey.Owner, actor.ID)}
	}
	key, err := ParsePublicKeyPEM([]byte(actor.PublicKey.PublicKeyPem))
	if err != nil {
		return nil, "", err
	}

	c.mu.Lock()
	c.evictKeyCacheLocked()
	c.keyCache[keyID] = cachedKey{key: key, ownerID: actor.ID, expiresAt: c.now().Add(c.keyCacheTTL)}
	c.mu.Unlock()
	return key, actor.ID, nil
}

// InvalidateKey drops any cached key for keyID so the next ResolveKey
// re-fetches. Task 06 calls this (via ResolveKeyFresh) when a cached key
// stops verifying, so a remote key rotation doesn't blackhole deliveries for
// the full cache TTL.
func (c *Client) InvalidateKey(keyID string) {
	c.mu.Lock()
	delete(c.keyCache, keyID)
	c.mu.Unlock()
}

// ResolveKeyFresh invalidates any cached key for keyID and resolves it again
// from the network. It implements the freshKeyResolver hook the Verifier uses
// for a single retry after a cached key fails to verify (key rotation).
func (c *Client) ResolveKeyFresh(ctx context.Context, keyID string) (*rsa.PublicKey, string, error) {
	c.InvalidateKey(keyID)
	return c.resolveKeyUncached(ctx, keyID)
}

// evictKeyCacheLocked keeps keyCache within maxKeyCacheEntries. It first
// sweeps expired entries, then, if still at the cap, evicts the entry closest
// to expiry. Callers must hold c.mu.
func (c *Client) evictKeyCacheLocked() {
	if len(c.keyCache) < c.maxKeyCacheEntries {
		return
	}
	now := c.now()
	for k, v := range c.keyCache {
		if !now.Before(v.expiresAt) {
			delete(c.keyCache, k)
		}
	}
	for len(c.keyCache) >= c.maxKeyCacheEntries {
		var oldestKey string
		var oldest time.Time
		first := true
		for k, v := range c.keyCache {
			if first || v.expiresAt.Before(oldest) {
				oldestKey, oldest, first = k, v.expiresAt, false
			}
		}
		delete(c.keyCache, oldestKey)
	}
}

// sameAuthority reports whether two absolute URLs share scheme, host, and
// port (the authority). Unparseable or non-absolute inputs never match.
func sameAuthority(a, b string) bool {
	ua, err := url.Parse(a)
	if err != nil || ua.Host == "" {
		return false
	}
	ub, err := url.Parse(b)
	if err != nil || ub.Host == "" {
		return false
	}
	return strings.EqualFold(ua.Scheme, ub.Scheme) &&
		strings.EqualFold(ua.Host, ub.Host)
}

// getDeduped collapses concurrent GETs for the same IRI into one request.
//
// The shared fetch runs on a context DETACHED from the initiating caller's
// context (with its own budget), so that if the initiator cancels, the fetch
// — which every waiter depends on — is not cancelled out from under the
// others. Each caller still selects on its own ctx.Done, so an individual
// caller that cancels stops waiting immediately; it just doesn't poison the
// shared request. Values from the caller's ctx are preserved for the fetch.
func (c *Client) getDeduped(ctx context.Context, iri string) ([]byte, error) {
	return c.getDedupedMode(ctx, iri, fetchModeObject)
}

func (c *Client) getDedupedMode(ctx context.Context, iri string, mode fetchMode) ([]byte, error) {
	// The dedupe key includes the mode: a pinned-authority fetch must never
	// share the result of a plain fetch for the same IRI (the plain fetch may
	// have followed a cross-authority redirect the pinned one forbids).
	ch := c.inflight.DoChan(fmt.Sprintf("%d\x00%s", mode, iri), func() (any, error) {
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.requestBudget)
		defer cancel()
		return c.get(fetchCtx, iri, mode)
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-ch:
		if result.Err != nil {
			return nil, result.Err
		}
		body, ok := result.Val.([]byte)
		if !ok {
			return nil, fmt.Errorf("ap: unexpected dedupe result type %T", result.Val)
		}
		return body, nil
	}
}

// fetchMode selects how non-success statuses map to errors and how redirects
// are policed. AP object fetches treat 401/403 as "unavailable" (NotFound);
// WebFinger must NOT, so a Cloudflare 403 / defederation is distinguishable
// from a genuinely missing account.
type fetchMode int

const (
	fetchModeObject fetchMode = iota
	fetchModeWebFinger
	// fetchModeObjectPinnedAuthority maps statuses like fetchModeObject but
	// rejects any redirect hop leaving the original IRI's scheme+host (see
	// FetchActorSameAuthority).
	fetchModeObjectPinnedAuthority
)

// get performs a signed GET with retries, redirects, rate limiting, and the
// response-size cap, returning the response body.
func (c *Client) get(ctx context.Context, iri string, mode fetchMode) ([]byte, error) {
	var lastErr error
	for attempt := 0; attempt < c.maxAttempts; attempt++ {
		if err := c.backoff(ctx, attempt); err != nil {
			return nil, err
		}
		body, retryable, err := c.getOnce(ctx, iri, mode)
		if err == nil {
			return body, nil
		}
		if !retryable {
			return nil, err
		}
		lastErr = err
	}
	return nil, lastErr
}

// getOnce performs one GET (following up to maxRedirects manually, re-signing
// each hop). retryable reports whether the failure is transient.
func (c *Client) getOnce(ctx context.Context, iri string, mode fetchMode) (body []byte, retryable bool, err error) {
	target := iri
	for redirects := 0; ; redirects++ {
		if err := c.waitForHost(ctx, target); err != nil {
			return nil, false, err
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return nil, false, fmt.Errorf("ap: build GET %s: %w", target, err)
		}
		req.Header.Set("User-Agent", c.userAgent)
		req.Header.Set("Accept", acceptActivityJSON)
		if c.signer != nil {
			if err := c.signer.SignRequest(req, nil); err != nil {
				return nil, false, err
			}
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return nil, true, fmt.Errorf("ap: GET %s: %w", target, err)
		}

		if isRedirect(resp.StatusCode) {
			location := resp.Header.Get("Location")
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			if location == "" {
				return nil, false, HTTPError{URL: target, StatusCode: resp.StatusCode}
			}
			if redirects >= maxRedirects {
				return nil, false, fmt.Errorf("ap: GET %s: too many redirects", iri)
			}
			next, err := url.Parse(location)
			if err != nil {
				return nil, false, fmt.Errorf("ap: GET %s: bad redirect location %q: %w", target, location, err)
			}
			resolved := req.URL.ResolveReference(next)
			if err := c.checkRedirectScheme(req.URL, resolved); err != nil {
				return nil, false, fmt.Errorf("ap: GET %s: %w", target, err)
			}
			if mode == fetchModeObjectPinnedAuthority && !sameAuthority(iri, resolved.String()) {
				// The pinned-authority modes exist for fetches whose RESULT is
				// an authorization statement about iri's own origin; a hop off
				// that origin is a definitive (non-retryable) refusal, not a
				// transient failure.
				return nil, false, errors.NewValidationError("redirect",
					fmt.Sprintf("redirect from %q to %q leaves the authority of %q", target, resolved.String(), iri))
			}
			target = resolved.String()
			continue
		}

		body, err := c.readBody(resp)
		if err != nil {
			return nil, false, fmt.Errorf("ap: GET %s: %w", target, err)
		}

		switch {
		case resp.StatusCode >= 200 && resp.StatusCode < 300:
			return body, false, nil
		case resp.StatusCode == http.StatusGone:
			// 410 Gone is how Lemmy serves deleted objects (with a Tombstone
			// body). Surface tombstone semantics so the materializer maps it
			// to "drop, never re-fetch".
			return nil, false, errors.NewTombstonedError("ap_object", iri)
		case resp.StatusCode == http.StatusNotFound:
			return nil, false, errors.NewNotFoundError("ap_object", iri)
		case resp.StatusCode == http.StatusUnauthorized,
			resp.StatusCode == http.StatusForbidden:
			// For AP objects, 401/403 mean secure-mode instances declined an
			// unauthorized fetch of an object that may well exist; from the
			// bridge's perspective it is unavailable, treated as NotFound.
			// WebFinger is different: a 401/403 there (Cloudflare, a
			// defederating instance) must NOT be flattened to "account does
			// not exist", so surface it as a distinguishable HTTPError that
			// preserves the status.
			if mode == fetchModeWebFinger {
				return nil, false, HTTPError{URL: iri, StatusCode: resp.StatusCode}
			}
			return nil, false, errors.NewNotFoundError("ap_object", iri)
		case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
			return nil, true, HTTPError{URL: iri, StatusCode: resp.StatusCode}
		default:
			return nil, false, HTTPError{URL: iri, StatusCode: resp.StatusCode}
		}
	}
}

// readBody drains the response body through the size cap and closes it.
func (c *Client) readBody(resp *http.Response) ([]byte, error) {
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, c.maxResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if int64(len(body)) > c.maxResponseBytes {
		return nil, fmt.Errorf("response exceeds %d byte cap", c.maxResponseBytes)
	}
	return body, nil
}

// backoff sleeps before retry attempts (none before the first), honoring ctx.
func (c *Client) backoff(ctx context.Context, attempt int) error {
	if attempt == 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		return nil
	}
	delay := c.retryBaseDelay << (attempt - 1)
	// Full jitter: uniform in [delay/2, delay].
	delay = delay/2 + time.Duration(rand.Int64N(int64(delay/2)+1))
	return c.sleep(ctx, delay)
}

// checkRedirectScheme rejects a redirect hop that downgrades https to http.
// In production every fetch starts (and must stay) https; following an
// http Location from an https response would move the exchange to plaintext
// (MITM/downgrade vector). The only exception is the dev/e2e relaxation
// (ALLOW_PRIVATE_FETCH → guard.allowPrivate), where plain-HTTP peers on the
// compose network are expected. Same-scheme hops and http→https upgrades
// are always allowed.
func (c *Client) checkRedirectScheme(from, to *url.URL) error {
	if c.guard.allowPrivate {
		return nil
	}
	if from.Scheme == "https" && to.Scheme == "http" {
		return errors.NewValidationError("redirect",
			fmt.Sprintf("redirect to %q downgrades https to http", to.String()))
	}
	return nil
}

// waitForHost validates the URL against the egress guard (scheme, userinfo,
// IP literals) and applies the per-host rate limit. The resolved-IP guard
// runs later at dial time (defeating DNS rebinding).
func (c *Client) waitForHost(ctx context.Context, rawURL string) error {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return errors.NewValidationError("url", fmt.Sprintf("unparseable url %q: %v", rawURL, err))
	}
	if err := c.guard.checkURL(parsed); err != nil {
		return err
	}
	host := strings.ToLower(parsed.Hostname())

	c.mu.Lock()
	entry, ok := c.limiters[host]
	if !ok {
		c.evictLimitersLocked()
		entry = &limiterEntry{limiter: rate.NewLimiter(c.perHostRPS, c.perHostBurst)}
		c.limiters[host] = entry
	}
	entry.lastUsed = c.now()
	limiter := entry.limiter
	c.mu.Unlock()

	if err := limiter.Wait(ctx); err != nil {
		return fmt.Errorf("ap: rate limit wait for %s: %w", host, err)
	}
	return nil
}

// evictLimitersLocked keeps the per-host limiter map within maxLimiters by
// evicting the least-recently-used entry. Callers must hold c.mu.
func (c *Client) evictLimitersLocked() {
	for len(c.limiters) >= c.maxLimiters {
		var oldestHost string
		var oldest time.Time
		first := true
		for h, e := range c.limiters {
			if first || e.lastUsed.Before(oldest) {
				oldestHost, oldest, first = h, e.lastUsed, false
			}
		}
		delete(c.limiters, oldestHost)
	}
}

func isRedirect(status int) bool {
	switch status {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther,
		http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		return true
	}
	return false
}
