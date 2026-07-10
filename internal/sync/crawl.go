package sync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	gosync "sync"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"

	"tidepool/internal/errors"
)

// Startup crawl announcements retry on a bounded budget: requestCrawl is
// sent while the process is still booting, and the receiving relay VALIDATES
// the hostname by calling straight back into this bridge's
// /xrpc/com.atproto.server.describeServer before subscribing (verified in
// indigo bgs/handlers.go) — so the very first attempt can race our own HTTP
// listener coming up, and the relay itself may still be starting. Transient
// startup failures are normal; a relay that stays unreachable for the whole
// budget is logged and dropped (the next process restart re-announces).
// Vars, not consts, so tests can compress the schedule.
//
// Budget arithmetic: each attempt is capped at crawlAttemptTimeout, so the
// worst case against a relay that hangs every attempt is
// crawlMaxAttempts × (crawlAttemptTimeout + crawlRetryInterval)
// = 24 × (10s + 5s) = 6 minutes. The common failure mode (connection
// refused while the relay boots) fails near-instantly, giving
// ≈ 24 × 5s = 2 minutes.
var (
	crawlRetryInterval  = 5 * time.Second
	crawlAttemptTimeout = 10 * time.Second // per-attempt cap so a hanging relay cannot blow the budget
	crawlMaxAttempts    = 24
)

// RequestCrawl asks one relay to start crawling this host, via
// com.atproto.sync.requestCrawl. relay may be a bare hostname (https is
// assumed) or a full http(s) URL; hostname is this bridge's public hostname.
// The caller supplies the HTTP client so production keeps the SSRF-guarded
// egress transport (ap.NewGuardedHTTPClient) and tests can hit httptest
// servers.
func RequestCrawl(ctx context.Context, client *http.Client, relay, hostname string) error {
	if err := validateRequestCrawlInput(client, relay, hostname); err != nil {
		return err
	}
	base := strings.TrimSuffix(strings.TrimSpace(relay), "/")
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		base = "https://" + base
	}

	body, err := json.Marshal(comatproto.SyncRequestCrawl_Input{Hostname: hostname})
	if err != nil {
		return fmt.Errorf("sync: encode requestCrawl input: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		base+"/xrpc/com.atproto.sync.requestCrawl", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("sync: build requestCrawl request for %s: %w", base, err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Everything past this point touches the wire; failures here may be
	// transient and are retried by requestCrawlWithRetry.

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("sync: requestCrawl to %s: %w", base, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("sync: requestCrawl to %s: status %d: %s", base, resp.StatusCode, string(snippet))
	}
	return nil
}

// validateRequestCrawlInput runs the pre-flight caller-bug checks that
// RequestCrawl performs before touching the wire: nil client, empty
// hostname, empty relay. requestCrawlWithRetry runs these ONCE ahead of its
// retry loop and treats only these as terminal — a caller bug cannot be
// fixed by retrying, but any error from an actual HTTP attempt might be
// transient and stays retryable.
func validateRequestCrawlInput(client *http.Client, relay, hostname string) error {
	if client == nil {
		return errors.NewValidationError("client", "must not be nil")
	}
	if hostname == "" {
		return errors.NewValidationError("hostname", "must not be empty")
	}
	if strings.TrimSuffix(strings.TrimSpace(relay), "/") == "" {
		return errors.NewValidationError("relay", "must not be empty")
	}
	return nil
}

// RequestCrawlAll announces this host to every configured relay, logging
// per-relay outcomes instead of failing fast — relays are independent and a
// dead one must not block the others (each gets its own goroutine and retry
// budget; the call returns when every relay has succeeded or exhausted its
// budget). Callers gate on environment: in development this function is
// only invoked under ALLOW_DEV_REQUEST_CRAWL (the e2e stack's local relay);
// otherwise dev logs the would-be request, because dev hosts are not
// publicly reachable and must never poke real relays.
func RequestCrawlAll(ctx context.Context, client *http.Client, relays []string, hostname string, logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	var wg gosync.WaitGroup
	for _, relay := range relays {
		wg.Add(1)
		go func(relay string) {
			defer wg.Done()
			requestCrawlWithRetry(ctx, client, relay, hostname, logger)
		}(relay)
	}
	wg.Wait()
}

// requestCrawlWithRetry drives one relay's announcement to a terminal
// outcome: success, a pre-flight validation error (caller bug — retrying
// cannot fix it), context cancellation, or budget exhaustion.
func requestCrawlWithRetry(ctx context.Context, client *http.Client, relay, hostname string, logger *slog.Logger) {
	// Pre-flight caller-bug checks run once, before the loop, and are the
	// ONLY terminal errors. Everything that comes back from an actual HTTP
	// attempt is retried within the budget — deliberately including
	// transport-layer ValidationErrors from the egress guard (a transient
	// "host did not resolve to any address" must not abandon the relay) and
	// ALL HTTP statuses including 4xx: bigsky answers 400 while the
	// describeServer callback race is unresolved (it probes our listener
	// before subscribing), and that 400 clears once our listener is up.
	// Do not "fix" this by short-circuiting on client errors.
	if err := validateRequestCrawlInput(client, relay, hostname); err != nil {
		logger.Error("requestCrawl rejected (not retrying)", "relay", relay, "error", err)
		return
	}
	for attempt := 1; ; attempt++ {
		// Cap each attempt so a hanging relay cannot stretch the budget past
		// the arithmetic documented on crawlMaxAttempts (the injected
		// client's own timeout is typically longer, e.g. 30s in main).
		attemptCtx, cancel := context.WithTimeout(ctx, crawlAttemptTimeout)
		err := RequestCrawl(attemptCtx, client, relay, hostname)
		cancel()
		if err == nil {
			logger.Info("requested crawl", "relay", relay, "hostname", hostname, "attempt", attempt)
			return
		}
		// A shutdown can surface as the in-flight attempt's error; report it
		// as shutdown, not as a relay failure or budget exhaustion.
		if ctx.Err() != nil {
			logger.Warn("requestCrawl abandoned", "relay", relay, "cause", ctx.Err())
			return
		}
		if attempt >= crawlMaxAttempts {
			logger.Error("requestCrawl failed, budget exhausted", "relay", relay, "attempts", attempt, "error", err)
			return
		}
		logger.Warn("requestCrawl failed, will retry", "relay", relay, "attempt", attempt, "error", err)
		select {
		case <-ctx.Done():
			logger.Warn("requestCrawl abandoned", "relay", relay, "cause", ctx.Err())
			return
		case <-time.After(crawlRetryInterval):
		}
	}
}
