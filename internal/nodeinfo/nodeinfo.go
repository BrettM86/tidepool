// Package nodeinfo discovers the software an ActivityPub instance runs
// (PRD_HANDLE_RENAME §5.6 originSoftware): /.well-known/nodeinfo, then the
// linked nodeinfo 2.0 or 2.1 document's software.name. The value is
// self-reported by the instance and is for display only.
package nodeinfo

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"

	"golang.org/x/sync/singleflight"

	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// DefaultDiscoveryDeadline bounds one Cache.Software call that runs a
// host's discovery cycle: the row reads, the well-known request and the
// linked document request together, with a share reserved for storing the
// outcome (see recordReserveDivisor).
const DefaultDiscoveryDeadline = 5 * time.Second

// recordReserveDivisor reserves 1/recordReserveDivisor of the deadline for
// storing a cycle's outcome: the reads and requests must finish by the
// deadline less that reserve, so a healthy store records the outcome before
// the waiters are released at the deadline.
const recordReserveDivisor = 5

// RefreshInterval is how long a stored discovery result, success or
// failure, stays fresh.
const RefreshInterval = 24 * time.Hour

// maxDocumentBytes caps each nodeinfo response body (well-known listing and
// linked document); a larger body is a failed discovery.
const maxDocumentBytes = 1 << 20

// maxSoftwareNameBytes is the lexicon's maxLength for originSoftware
// (atproto string maxLength counts UTF-8 bytes).
const maxSoftwareNameBytes = 64

// recordTimeout bounds storing a discovery outcome. Recording runs on its
// own detached context, so an expired discovery still records its failure,
// even after the waiters have been released.
const recordTimeout = 5 * time.Second

// schemaRels are the well-known link relations whose documents carry
// software.name in the shape read here. 1.0 and 1.1 are never followed.
var schemaRels = map[string]bool{
	"http://nodeinfo.diaspora.software/ns/schema/2.0": true,
	"http://nodeinfo.diaspora.software/ns/schema/2.1": true,
}

// Fetcher runs nodeinfo discovery against one host.
type Fetcher struct {
	client    *http.Client
	userAgent string
}

// NewFetcher builds a Fetcher. client carries the egress policy (production
// passes the guarded client) and must not be nil.
func NewFetcher(client *http.Client, userAgent string) (*Fetcher, error) {
	if client == nil {
		return nil, errors.NewValidationError("client", "must not be nil")
	}
	return &Fetcher{client: client, userAgent: userAgent}, nil
}

// SoftwareName runs one discovery cycle for host, reached over scheme
// ("http" or "https", the instance's actor ids' own scheme), and returns its
// software name, trimmed and lowercased. Any failure returns "" and a
// non-nil error.
func (f *Fetcher) SoftwareName(ctx context.Context, scheme, host string) (string, error) {
	if scheme != "http" && scheme != "https" {
		return "", errors.NewValidationError("scheme", "must be http or https")
	}
	if host == "" || strings.ContainsAny(host, "/?#@\\ ") {
		return "", errors.NewValidationError("host", "must be a bare host name")
	}
	wellKnownURL := (&url.URL{Scheme: scheme, Host: host, Path: "/.well-known/nodeinfo"}).String()

	var listing struct {
		Links []struct {
			Rel  string `json:"rel"`
			Href string `json:"href"`
		} `json:"links"`
	}
	if err := f.getJSON(ctx, wellKnownURL, &listing); err != nil {
		return "", err
	}
	var documentURL string
	for _, link := range listing.Links {
		if schemaRels[link.Rel] {
			documentURL = link.Href
			break
		}
	}
	if documentURL == "" {
		return "", fmt.Errorf("nodeinfo for %s: no 2.0 or 2.1 schema link", host)
	}
	// The link may use the instance's own scheme or https; an https
	// instance linking to http is a downgrade and is refused.
	if parsed, err := url.Parse(documentURL); err != nil ||
		(parsed.Scheme != "https" && parsed.Scheme != scheme) || parsed.Host == "" {
		return "", fmt.Errorf("nodeinfo for %s: schema link is not an absolute %s URL", host, scheme)
	}

	var document struct {
		Software struct {
			Name string `json:"name"`
		} `json:"software"`
	}
	if err := f.getJSON(ctx, documentURL, &document); err != nil {
		return "", err
	}
	name := strings.ToLower(strings.TrimSpace(document.Software.Name))
	switch {
	case name == "":
		return "", fmt.Errorf("nodeinfo for %s: no software.name", host)
	case len(name) > maxSoftwareNameBytes:
		return "", fmt.Errorf("nodeinfo for %s: software.name over %d bytes", host, maxSoftwareNameBytes)
	case strings.IndexFunc(name, unicode.IsControl) >= 0:
		return "", fmt.Errorf("nodeinfo for %s: software.name contains control characters", host)
	}
	return name, nil
}

// getJSON GETs target and decodes its body, capped at maxDocumentBytes,
// into out. A non-2xx status is a failure.
func (f *Fetcher) getJSON(ctx context.Context, target string, out any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return fmt.Errorf("nodeinfo: build request for %s: %w", target, err)
	}
	request.Header.Set("Accept", "application/json")
	if f.userAgent != "" {
		request.Header.Set("User-Agent", f.userAgent)
	}
	response, err := f.client.Do(request)
	if err != nil {
		return fmt.Errorf("nodeinfo: fetch %s: %w", target, err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		return fmt.Errorf("nodeinfo: fetch %s: status %d", target, response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxDocumentBytes+1))
	if err != nil {
		return fmt.Errorf("nodeinfo: read %s: %w", target, err)
	}
	if len(body) > maxDocumentBytes {
		return fmt.Errorf("nodeinfo: %s exceeds %d bytes", target, maxDocumentBytes)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("nodeinfo: decode %s: %w", target, err)
	}
	return nil
}

// CacheOptions configures a Cache. Fetcher and Store are required.
type CacheOptions struct {
	Fetcher *Fetcher
	Store   store.Instances
	// DiscoveryDeadline bounds one host's discovery cycle
	// (0 = DefaultDiscoveryDeadline).
	DiscoveryDeadline time.Duration
	Logger            *slog.Logger
}

// Cache serves stored software names, refreshing missing or stale ones at
// most once per RefreshInterval per host.
type Cache struct {
	fetcher  *Fetcher
	store    store.Instances
	deadline time.Duration
	logger   *slog.Logger
	// inFlight coordinates discovery per host: concurrent callers that find
	// a host missing or stale share one cycle.
	inFlight singleflight.Group
}

// NewCache builds a Cache.
func NewCache(opts CacheOptions) (*Cache, error) {
	if opts.Fetcher == nil {
		return nil, errors.NewValidationError("fetcher", "must not be nil")
	}
	if opts.Store == nil {
		return nil, errors.NewValidationError("store", "must not be nil")
	}
	deadline := opts.DiscoveryDeadline
	if deadline <= 0 {
		deadline = DefaultDiscoveryDeadline
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Cache{fetcher: opts.Fetcher, store: opts.Store, deadline: deadline, logger: logger}, nil
}

// Software returns the stored software name for host, or "" when none is
// known. A missing or stale row first runs one coordinated discovery cycle
// over scheme, keyed on host alone. It never fails: every problem degrades
// to the stored name or "".
//
// Every call returns within the discovery deadline, whatever ctx carries:
// its own row read is bounded by it, and a caller waiting on a cycle is
// released at it with the last name it read. A cycle whose outcome is
// stored by then (a healthy store; see recordReserveDivisor) returns the
// stored outcome instead.
func (c *Cache) Software(ctx context.Context, scheme, host string) string {
	if host == "" {
		return ""
	}
	deadline := time.Now().Add(c.deadline)
	readCtx, cancelRead := context.WithDeadline(ctx, deadline)
	name, fresh, ok := c.stored(readCtx, host)
	cancelRead()
	if !ok || fresh {
		return name
	}
	// Only the first caller's function runs, on that caller's deadline; a
	// caller joining a running cycle has a deadline no earlier than it.
	result := c.inFlight.DoChan(host, func() (any, error) {
		return c.refresh(ctx, scheme, host, deadline), nil
	})
	expired := time.NewTimer(time.Until(deadline))
	defer expired.Stop()
	select {
	case r := <-result:
		// A cycle that could not read the row knows no name; the one this
		// caller read is still the last known.
		if refreshed, _ := r.Val.(string); refreshed != "" {
			return refreshed
		}
		return name
	case <-expired.C:
		return name
	case <-ctx.Done():
		return ""
	}
}

// stored reads host's row. ok is false when the read failed (logged); fresh
// reports a row fetched within RefreshInterval.
func (c *Cache) stored(ctx context.Context, host string) (name string, fresh, ok bool) {
	instance, err := c.store.Get(ctx, host)
	switch {
	case errors.IsNotFound(err):
		return "", false, true
	case err != nil:
		c.logger.Warn("nodeinfo: read instance row failed", "host", host, "error", err)
		return "", false, false
	}
	return instance.SoftwareName, time.Since(instance.FetchedAt) < RefreshInterval, true
}

// refresh is the shared work of one coordinated discovery: it re-reads the
// row (a cycle that finished just before this one started leaves it
// fresh) and runs discovery, both on a context detached from any one caller
// and ending at deadline less the record reserve, then stores the outcome
// on its own detached context and returns the stored name.
func (c *Cache) refresh(ctx context.Context, scheme, host string, deadline time.Time) string {
	detached := context.WithoutCancel(ctx)
	cycleCtx, cancelCycle := context.WithDeadline(detached, deadline.Add(-c.deadline/recordReserveDivisor))
	defer cancelCycle()
	name, fresh, ok := c.stored(cycleCtx, host)
	if !ok || fresh {
		return name
	}

	discovered, fetchErr := c.fetcher.SoftwareName(cycleCtx, scheme, host)
	cancelCycle()

	recordCtx, cancelRecord := context.WithTimeout(detached, recordTimeout)
	defer cancelRecord()
	if fetchErr != nil {
		c.logger.Debug("nodeinfo: discovery failed", "host", host, "error", fetchErr)
		if err := c.store.RecordFetchFailure(recordCtx, host, time.Now()); err != nil {
			c.logger.Warn("nodeinfo: record discovery failure failed", "host", host, "error", err)
		}
		return name
	}
	if err := c.store.RecordSoftware(recordCtx, host, discovered, time.Now()); err != nil {
		c.logger.Warn("nodeinfo: record discovered software failed", "host", host, "error", err)
	}
	return discovered
}
