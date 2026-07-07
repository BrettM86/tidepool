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

	comatproto "github.com/bluesky-social/indigo/api/atproto"

	"tidepool/internal/errors"
)

// RequestCrawl asks one relay to start crawling this host, via
// com.atproto.sync.requestCrawl. relay may be a bare hostname (https is
// assumed) or a full http(s) URL; hostname is this bridge's public hostname.
// The caller supplies the HTTP client so production keeps the SSRF-guarded
// egress transport (ap.NewGuardedHTTPClient) and tests can hit httptest
// servers.
func RequestCrawl(ctx context.Context, client *http.Client, relay, hostname string) error {
	if client == nil {
		return errors.NewValidationError("client", "must not be nil")
	}
	if hostname == "" {
		return errors.NewValidationError("hostname", "must not be empty")
	}
	base := strings.TrimSuffix(strings.TrimSpace(relay), "/")
	if base == "" {
		return errors.NewValidationError("relay", "must not be empty")
	}
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

// RequestCrawlAll announces this host to every configured relay, logging
// per-relay outcomes instead of failing fast — relays are independent and a
// dead one must not block the others. Callers gate on environment: in
// development this function should not be invoked at all (main logs the
// would-be request instead), because dev hosts are not publicly reachable
// and must never poke real relays.
func RequestCrawlAll(ctx context.Context, client *http.Client, relays []string, hostname string, logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	for _, relay := range relays {
		if err := RequestCrawl(ctx, client, relay, hostname); err != nil {
			logger.Error("requestCrawl failed", "relay", relay, "error", err)
			continue
		}
		logger.Info("requested crawl", "relay", relay, "hostname", hostname)
	}
}
