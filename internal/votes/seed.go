package votes

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"tidepool/internal/errors"
)

// maxSeedResponseBytes caps a Lemmy API counts response — a full post_view
// is a few KB; anything near the cap is not the endpoint we think it is.
const maxSeedResponseBytes = 1 << 20 // 1 MiB

// SeedStore is the slice of *Aggregator the seeder needs (tests inject
// recorders).
type SeedStore interface {
	SeedAggregates(ctx context.Context, subjectAPID string, upvotes, downvotes int) error
}

// LemmySeeder imports historical vote counts for backfilled posts from
// Lemmy's public HTTP API (GET /api/v3/post?id=N, the `counts` field) —
// the seeding hook task 06's backfill calls, gated behind
// SEED_COUNTS_FROM_API. AP alone cannot provide this: Lemmy outboxes
// announce historical Likes only sparsely, so without seeding backfilled
// posts start near zero. Comment counts are deliberately not seeded in v1
// (one API call per comment would triple backfill egress for garnish);
// comments accumulate live votes only.
type LemmySeeder struct {
	store     SeedStore
	client    *http.Client
	userAgent string
	logger    *slog.Logger
}

// NewLemmySeeder builds a seeder. client must be an SSRF-guarded HTTP
// client (ap.NewGuardedHTTPClient) — the API URL is derived from a remote
// object's self-asserted AP id.
func NewLemmySeeder(store SeedStore, client *http.Client, userAgent string, logger *slog.Logger) (*LemmySeeder, error) {
	if store == nil {
		return nil, errors.NewValidationError("store", "must not be nil")
	}
	if client == nil {
		return nil, errors.NewValidationError("client", "must not be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &LemmySeeder{store: store, client: client, userAgent: userAgent, logger: logger}, nil
}

// lemmyCountsResponse is the slice of Lemmy's GET /api/v3/post response the
// seeder reads. Every level is a pointer so ABSENCE is detectable: a 200
// whose JSON lacks the expected nesting (a proxy error page served as JSON,
// renamed fields in a future Lemmy API, the wrong endpoint) must fail
// loudly, not decode cleanly to zeros — re-seeding REPLACES the baseline, so
// a silent 0/0 would overwrite a previously good one.
type lemmyCountsResponse struct {
	PostView *struct {
		Counts *struct {
			Upvotes   *int `json:"upvotes"`
			Downvotes *int `json:"downvotes"`
		} `json:"counts"`
	} `json:"post_view"`
}

// SeedPostCounts fetches the post's current score from its origin instance
// and stores it as the subject's seeded baseline. Non-Lemmy-shaped AP ids
// are a silent no-op (only Lemmy's URL scheme is recognized in v1); fetch
// and decode failures return an error the caller logs — seeding is
// best-effort garnish on backfill, never fatal.
func (s *LemmySeeder) SeedPostCounts(ctx context.Context, postAPID string) error {
	apiURL, ok := lemmyPostAPIURL(postAPID)
	if !ok {
		return nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return fmt.Errorf("votes: build counts request for %s: %w", postAPID, err)
	}
	req.Header.Set("Accept", "application/json")
	if s.userAgent != "" {
		req.Header.Set("User-Agent", s.userAgent)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("votes: fetch counts for %s: %w", postAPID, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("votes: fetch counts for %s: unexpected status %d", postAPID, resp.StatusCode)
	}

	var payload lemmyCountsResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxSeedResponseBytes)).Decode(&payload); err != nil {
		return fmt.Errorf("votes: decode counts for %s: %w", postAPID, err)
	}
	if payload.PostView == nil || payload.PostView.Counts == nil ||
		payload.PostView.Counts.Upvotes == nil || payload.PostView.Counts.Downvotes == nil {
		return fmt.Errorf("votes: counts response for %s lacks post_view.counts.{upvotes,downvotes}", postAPID)
	}
	upvotes, downvotes := *payload.PostView.Counts.Upvotes, *payload.PostView.Counts.Downvotes
	if upvotes < 0 || downvotes < 0 {
		return fmt.Errorf("votes: counts for %s are negative (up=%d down=%d)",
			postAPID, upvotes, downvotes)
	}

	if err := s.store.SeedAggregates(ctx, postAPID, upvotes, downvotes); err != nil {
		return err
	}
	s.logger.Debug("vote counts seeded from origin API",
		"post", postAPID, "upvotes", upvotes, "downvotes", downvotes)
	return nil
}

// lemmyPostAPIURL derives the public API counts endpoint from a Lemmy post
// AP id: https://host/post/123 → https://host/api/v3/post?id=123. Anything
// else (comments, non-Lemmy URL shapes) reports ok=false.
func lemmyPostAPIURL(postAPID string) (string, bool) {
	parsed, err := url.Parse(postAPID)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return "", false
	}
	postID, ok := strings.CutPrefix(parsed.Path, "/post/")
	if !ok || postID == "" || strings.ContainsRune(postID, '/') {
		return "", false
	}
	for _, r := range postID {
		if r < '0' || r > '9' {
			return "", false
		}
	}
	api := url.URL{
		Scheme:   parsed.Scheme,
		Host:     parsed.Host,
		Path:     "/api/v3/post",
		RawQuery: "id=" + postID,
	}
	return api.String(), true
}
