package votes

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/go-chi/chi/v5"
	"github.com/lib/pq"

	"tidepool/internal/errors"
)

// NSIDGetVoteAggregates is the sanctioned side-channel query
// (lexicons/social/coves/bridge/getVoteAggregates.json). The nsid is the
// version: breaking changes mean a new name, never a changed shape.
const NSIDGetVoteAggregates = "social.coves.bridge.getVoteAggregates"

const (
	// maxURIsPerQuery caps one getVoteAggregates call (mirrors the lexicon's
	// maxLength).
	maxURIsPerQuery = 100
	// cacheMaxAge is the Cache-Control freshness window. Vote counts are
	// eventually consistent by nature; 30s keeps AppView polling cheap
	// without making scores feel stale.
	cacheMaxAge = 30 * time.Second

	// Rate-limit defaults (per client IP): the endpoint is a public read the
	// AppView polls in batches, so sustained per-IP throughput can stay low
	// while the burst absorbs a page-load fan-out.
	defaultRatePerSecond = 10
	defaultRateBurst     = 30
)

// XRPCOptions configures NewXRPC. DB is required.
type XRPCOptions struct {
	DB     *sql.DB
	Logger *slog.Logger
	// RatePerSecond / RateBurst tune the per-IP token bucket; zero values
	// take the defaults. Tests shrink them.
	RatePerSecond float64
	RateBurst     int
}

// XRPC serves the vote-aggregate side channel: the one sanctioned read the
// AppView uses to display scores for bridged content (PLAN.md decision 7).
// Public, cacheable, rate limited by client IP.
type XRPC struct {
	db      *sql.DB
	limiter *ipLimiter
	logger  *slog.Logger
}

// NewXRPC validates options and builds the handler.
func NewXRPC(opts XRPCOptions) (*XRPC, error) {
	if opts.DB == nil {
		return nil, errors.NewValidationError("db", "must not be nil")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	perSecond := opts.RatePerSecond
	if perSecond <= 0 {
		perSecond = defaultRatePerSecond
	}
	burst := opts.RateBurst
	if burst <= 0 {
		burst = defaultRateBurst
	}
	return &XRPC{
		db:      opts.DB,
		limiter: newIPLimiter(perSecond, burst),
		logger:  logger,
	}, nil
}

// Routes mounts the side channel on a chi router.
func (x *XRPC) Routes(r chi.Router) {
	r.Get("/xrpc/"+NSIDGetVoteAggregates, x.handleGetVoteAggregates)
}

// aggregateView is one output entry ({uri, upvotes, downvotes, updatedAt}).
type aggregateView struct {
	URI       string `json:"uri"`
	Upvotes   int64  `json:"upvotes"`
	Downvotes int64  `json:"downvotes"`
	UpdatedAt string `json:"updatedAt"`
}

// handleGetVoteAggregates serves GET /xrpc/social.coves.bridge.
// getVoteAggregates?uris=at://…&uris=at://… (repeated params, the atproto
// convention; comma-separated values inside one param are accepted too).
// Well-formed but unknown uris are omitted from the response, never an
// error — the AppView batches optimistically over content that may predate
// vote ingestion. Malformed at-uris are InvalidRequest.
func (x *XRPC) handleGetVoteAggregates(w http.ResponseWriter, r *http.Request) {
	if !x.limiter.allow(clientIP(r)) {
		x.writeXRPCError(w, http.StatusTooManyRequests, "RateLimitExceeded", "rate limit exceeded")
		return
	}

	uris, err := parseURIs(r.URL.Query()["uris"])
	if err != nil {
		x.writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	}
	if len(uris) == 0 {
		x.writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "missing required parameter: uris")
		return
	}

	rows, err := x.db.QueryContext(r.Context(), `
		SELECT subject_at_uri, upvotes, downvotes, updated_at
		FROM vote_aggregates
		WHERE subject_at_uri = ANY($1)`,
		pq.Array(uris))
	if err != nil {
		x.logger.Error("votes: query aggregates", "error", err)
		x.writeXRPCError(w, http.StatusInternalServerError, "InternalServerError", "internal error")
		return
	}
	defer func() { _ = rows.Close() }()

	byURI := make(map[string]aggregateView, len(uris))
	for rows.Next() {
		var view aggregateView
		var updatedAt time.Time
		if err := rows.Scan(&view.URI, &view.Upvotes, &view.Downvotes, &updatedAt); err != nil {
			x.logger.Error("votes: scan aggregate", "error", err)
			x.writeXRPCError(w, http.StatusInternalServerError, "InternalServerError", "internal error")
			return
		}
		// Millisecond-precision UTC, the datetime shape Coves records use.
		view.UpdatedAt = updatedAt.UTC().Format("2006-01-02T15:04:05.000Z")
		byURI[view.URI] = view
	}
	if err := rows.Err(); err != nil {
		x.logger.Error("votes: iterate aggregates", "error", err)
		x.writeXRPCError(w, http.StatusInternalServerError, "InternalServerError", "internal error")
		return
	}

	// Respond in request order; unknown uris are simply absent.
	aggregates := make([]aggregateView, 0, len(byURI))
	for _, uri := range uris {
		if view, ok := byURI[uri]; ok {
			aggregates = append(aggregates, view)
		}
	}

	w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d", int(cacheMaxAge.Seconds())))
	x.writeJSON(w, http.StatusOK, map[string]any{"aggregates": aggregates})
}

// parseURIs flattens repeated uris params and comma-separated lists into a
// deduplicated, order-preserving slice of syntactically valid at-uris. A
// malformed at-uri is an error (the lexicon declares format at-uri; only
// well-formed-but-unknown uris are silently omitted downstream). The
// 100-uri cap counts raw values, before de-duplication — a request carrying
// more than the lexicon's maxLength has already broken the contract even if
// it contains duplicates — which also lets the check short-circuit before
// allocating anything proportional to an arbitrarily long parameter list.
func parseURIs(params []string) ([]string, error) {
	var uris []string
	raw := 0
	seen := map[string]bool{}
	for _, param := range params {
		for value := range strings.SplitSeq(param, ",") {
			value = strings.TrimSpace(value)
			if value == "" {
				continue
			}
			raw++
			if raw > maxURIsPerQuery {
				return nil, fmt.Errorf("too many uris: more than the maximum of %d", maxURIsPerQuery)
			}
			if seen[value] {
				continue
			}
			if _, err := syntax.ParseATURI(value); err != nil {
				return nil, fmt.Errorf("malformed at-uri: %q", value)
			}
			seen[value] = true
			uris = append(uris, value)
		}
	}
	return uris, nil
}

// clientIP extracts the connection's remote IP. Deliberately not
// X-Forwarded-For: the bridge cannot know which proxies to trust, and a
// spoofable header would let one client exhaust every bucket. Deployments
// behind a load balancer rate-limit the real client at the edge.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (x *XRPC) writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		// The status line and headers are already out, so nothing is
		// recoverable — but a truncated body should leave a trace.
		x.logger.Debug("votes: write response body", "error", err)
	}
}

func (x *XRPC) writeXRPCError(w http.ResponseWriter, status int, code, message string) {
	x.writeJSON(w, status, map[string]string{"error": code, "message": message})
}
