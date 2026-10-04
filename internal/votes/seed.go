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

// maxSeedResponseBytes caps a Lemmy API counts response — a post_view or a
// resolve_object post is a few KB; anything near the cap is not the endpoint
// we think it is.
const maxSeedResponseBytes = 1 << 20 // 1 MiB

// SeedStore is the slice of *Aggregator the seeder needs (tests inject
// recorders).
type SeedStore interface {
	SeedAggregates(ctx context.Context, subjectAPID string, upvotes, downvotes int) error
}

// LemmySeeder imports historical vote counts for backfilled posts from
// Lemmy's public HTTP API — the seeding hook task 06's backfill calls, gated
// behind SEED_COUNTS_FROM_API. AP alone cannot provide this: Lemmy outboxes
// announce historical Likes only sparsely, so without seeding backfilled
// posts start near zero. Counts are asked of the COMMUNITY's host, never a
// cross-host post's own: a post authored on host A in a community on host B
// is seeded from B's resolve_object view of it, because A could assert any
// score it likes for its own post. The community IRI a caller passes must be
// the community the post is stored under: the seeder trusts that host for
// the post's counts and does not verify the binding, so callers must.
// Comment counts are deliberately not seeded in v1 (one API call per comment
// would triple backfill egress for garnish); comments accumulate live votes
// only.
type LemmySeeder struct {
	store     SeedStore
	client    *http.Client
	userAgent string
	logger    *slog.Logger
}

// NewLemmySeeder builds a seeder. client must be an SSRF-guarded HTTP
// client (ap.NewGuardedHTTPClient) — the API URL is derived from remote
// objects' self-asserted AP ids.
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

// lemmyCounts is the counts object both Lemmy responses carry. Every level
// of each response is a pointer so ABSENCE is detectable: a 200 whose JSON
// lacks the expected nesting (a proxy error page served as JSON, renamed
// fields in a future Lemmy API, the wrong endpoint) must fail loudly, not
// decode cleanly to zeros — re-seeding REPLACES the baseline, so a silent
// 0/0 would overwrite a previously good one.
type lemmyCounts struct {
	Upvotes   *int `json:"upvotes"`
	Downvotes *int `json:"downvotes"`
}

// countsResponse is a decoded Lemmy response that may carry post counts.
type countsResponse interface {
	counts() *lemmyCounts
	// countsPath names where the counts live, for the missing-fields error.
	countsPath() string
	// isMiss reports whether a non-200 answer means "this host has no counts
	// for the post" rather than a failure.
	isMiss(status int, body io.Reader) bool
}

// lemmyCountsResponse is the slice of GET /api/v3/post?id=N the seeder reads.
type lemmyCountsResponse struct {
	PostView *struct {
		Counts *lemmyCounts `json:"counts"`
	} `json:"post_view"`
}

func (r *lemmyCountsResponse) counts() *lemmyCounts {
	if r.PostView == nil {
		return nil
	}
	return r.PostView.Counts
}

func (r *lemmyCountsResponse) countsPath() string { return "post_view.counts" }

// isMiss is always false: the community host stores the post, so any non-200
// is a failure.
func (r *lemmyCountsResponse) isMiss(int, io.Reader) bool { return false }

// lemmyResolveResponse is the slice of GET /api/v3/resolve_object?q=<ap id>
// the seeder reads when the object is a post.
type lemmyResolveResponse struct {
	Post *struct {
		Counts *lemmyCounts `json:"counts"`
	} `json:"post"`
}

func (r *lemmyResolveResponse) counts() *lemmyCounts {
	if r.Post == nil {
		return nil
	}
	return r.Post.Counts
}

func (r *lemmyResolveResponse) countsPath() string { return "post.counts" }

// isMiss reports Lemmy 0.19's resolve_object miss: 400
// {"error":"couldnt_find_object"}. Any other non-200, a 404 included, is a
// host not answering as Lemmy and therefore a failure.
func (r *lemmyResolveResponse) isMiss(status int, body io.Reader) bool {
	if status != http.StatusBadRequest {
		return false
	}
	var lemmyError struct {
		Error string `json:"error"`
	}
	return json.NewDecoder(body).Decode(&lemmyError) == nil && lemmyError.Error == "couldnt_find_object"
}

// SeedPostCounts fetches the post's current score from the community's host
// and stores it as the subject's seeded baseline. communityIRI must be the
// community the post is stored under: the seeder trusts that host for the
// post's counts and does not verify the binding, so callers must. Every
// request goes to the community's host. When the post and the community
// share a host that is GET /api/v3/post?id=N; otherwise it is the community
// host's anonymous resolve_object, which answers only from its local copy,
// and Lemmy's miss there (400 {"error":"couldnt_find_object"}: the host does
// not know the post) is a quiet no-op. The post's own host is never asked
// about a cross-host post. Non-Lemmy-shaped AP ids (only Lemmy's URL scheme
// is recognized in v1) are silent no-ops. An unusable communityIRI, and
// fetch and decode failures, return an error the caller logs — seeding is
// best-effort garnish on backfill, never fatal.
func (s *LemmySeeder) SeedPostCounts(ctx context.Context, postAPID, communityIRI string) error {
	post, postID, ok := lemmyPostRef(postAPID)
	if !ok {
		return nil
	}
	community, err := url.Parse(communityIRI)
	if err != nil || community.Host == "" || (community.Scheme != "http" && community.Scheme != "https") {
		return fmt.Errorf("votes: unusable community IRI %q for %s", communityIRI, postAPID)
	}

	// Both request URLs are built from the community's scheme and host; the
	// post's host only decides which endpoint to ask.
	api := url.URL{Scheme: community.Scheme, Host: community.Host}
	var payload countsResponse
	if post.Host == community.Host {
		api.Path, api.RawQuery = "/api/v3/post", "id="+postID
		payload = &lemmyCountsResponse{}
	} else {
		api.Path, api.RawQuery = "/api/v3/resolve_object", url.Values{"q": {postAPID}}.Encode()
		payload = &lemmyResolveResponse{}
	}
	apiURL := api.String()

	found, err := s.fetchCounts(ctx, postAPID, apiURL, payload)
	if err != nil {
		return err
	}
	if !found {
		s.logger.Debug("vote counts not seeded: community host has no counts for the post",
			"post", postAPID, "community", communityIRI)
		return nil
	}
	counts := payload.counts()
	if counts == nil || counts.Upvotes == nil || counts.Downvotes == nil {
		return fmt.Errorf("votes: counts response for %s from %s lacks %s.{upvotes,downvotes}",
			postAPID, apiURL, payload.countsPath())
	}
	upvotes, downvotes := *counts.Upvotes, *counts.Downvotes
	if upvotes < 0 || downvotes < 0 {
		return fmt.Errorf("votes: counts for %s from %s are negative (up=%d down=%d)",
			postAPID, apiURL, upvotes, downvotes)
	}

	if err := s.store.SeedAggregates(ctx, postAPID, upvotes, downvotes); err != nil {
		return err
	}
	s.logger.Debug("vote counts seeded from Lemmy API",
		"post", postAPID, "community", communityIRI, "upvotes", upvotes, "downvotes", downvotes)
	return nil
}

// fetchCounts GETs apiURL and decodes the bounded JSON body into payload. A
// non-200 that payload.isMiss accepts reports found=false and no error; every
// other non-200 is an error.
func (s *LemmySeeder) fetchCounts(ctx context.Context, postAPID, apiURL string, payload countsResponse) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return false, fmt.Errorf("votes: build counts request for %s from %s: %w", postAPID, apiURL, err)
	}
	req.Header.Set("Accept", "application/json")
	if s.userAgent != "" {
		req.Header.Set("User-Agent", s.userAgent)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return false, fmt.Errorf("votes: fetch counts for %s from %s: %w", postAPID, apiURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body := io.LimitReader(resp.Body, maxSeedResponseBytes)
	if resp.StatusCode != http.StatusOK {
		if payload.isMiss(resp.StatusCode, body) {
			return false, nil
		}
		return false, fmt.Errorf("votes: fetch counts for %s from %s: unexpected status %d",
			postAPID, apiURL, resp.StatusCode)
	}

	if err := json.NewDecoder(body).Decode(payload); err != nil {
		return false, fmt.Errorf("votes: decode counts for %s from %s: %w", postAPID, apiURL, err)
	}
	return true, nil
}

// lemmyPostRef validates a Lemmy post AP id (http(s)://host/post/123) and
// returns it parsed with its numeric post id. Anything else (comments,
// non-Lemmy URL shapes, native Coves objects) reports ok=false; these are the
// shapes TestLemmyPostAPIURL pins.
func lemmyPostRef(postAPID string) (post *url.URL, postID string, ok bool) {
	post, err := url.Parse(postAPID)
	if err != nil || post.Host == "" || (post.Scheme != "http" && post.Scheme != "https") {
		return nil, "", false
	}
	postID, ok = strings.CutPrefix(post.Path, "/post/")
	if !ok || postID == "" || strings.ContainsRune(postID, '/') {
		return nil, "", false
	}
	for _, r := range postID {
		if r < '0' || r > '9' {
			return nil, "", false
		}
	}
	return post, postID, true
}

// lemmyPostAPIURL is the post's own host's counts endpoint for a Lemmy post
// AP id: https://host/post/123 → https://host/api/v3/post?id=123, ok=false
// for every shape lemmyPostRef refuses. SeedPostCounts does not request it:
// it builds every request from the community's host.
func lemmyPostAPIURL(postAPID string) (string, bool) {
	post, postID, ok := lemmyPostRef(postAPID)
	if !ok {
		return "", false
	}
	api := url.URL{Scheme: post.Scheme, Host: post.Host, Path: "/api/v3/post", RawQuery: "id=" + postID}
	return api.String(), true
}
