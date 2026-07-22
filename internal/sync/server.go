package sync

import (
	"context"
	"encoding/json"
	"expvar"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
	"github.com/ipfs/go-cid"

	"tidepool/internal/errors"
	"tidepool/internal/ratelimit"
	"tidepool/internal/repo"
)

// SyncRateLimited counts public sync-surface requests refused by admission
// control — the per-client-IP 429s on the read/dial surface and the
// subscribeRepos connection-cap refusals. Published to the admin /metrics
// surface as "tidepool_sync_ratelimited" (mirrors
// materialize.ValidationFailures): a misconfigured tight limit silently
// dropping all sync traffic is otherwise invisible.
var SyncRateLimited = expvar.NewInt("tidepool_sync_ratelimited")

// healthVersion is reported by /xrpc/_health, mirroring what the reference
// PDS exposes (crawlers and Jetstream use it as a liveness probe).
const healthVersion = "tidepool 0.1"

const (
	// defaultWriteTimeout bounds every WebSocket write; a subscriber that
	// cannot drain a frame within it is evicted (the DB-backed cursor lets
	// it reconnect and resume without loss).
	defaultWriteTimeout = 10 * time.Second
	// defaultPingInterval keeps idle streams alive through proxies and
	// detects dead peers (the read deadline is derived from it).
	defaultPingInterval = 30 * time.Second
	// defaultReplayBatch is how many stored events are loaded per query
	// while a subscriber catches up.
	defaultReplayBatch = 100

	// listRepos limit bounds per com.atproto.sync.listRepos.
	defaultListReposLimit = 500
	maxListReposLimit     = 1000

	// Admission-control defaults for the public sync surface (task 11,
	// pre-internet-facing hardening). Sized as DoS backstops, not fairness
	// controls: a relay indexing the bridge for the first time bursts
	// getRepo/getRecord per new DID, so the burst absorbs a whole first
	// crawl and the sustained rate stays far above anything a legitimate
	// consumer needs (relays hold ONE WebSocket, not request storms).
	defaultSyncRatePerSecond = 25
	defaultSyncRateBurst     = 200
	// defaultMaxSubscribers caps concurrent subscribeRepos connections. A
	// bridge has a handful of legitimate firehose consumers (relays,
	// Jetstream, debug taps); each one costs a goroutine pair and a DB
	// cursor, so the cap bounds that multiplication.
	defaultMaxSubscribers = 100
)

// HandleResolver resolves a bridged handle to its DID. Satisfied by
// identity.Resolver implementations; unknown handles are errors satisfying
// errors.IsNotFound, and syntactically degenerate handles may instead
// satisfy errors.IsValidation — the repo.getRecord handler folds both into
// repo-not-found (see handleRepoGetRecord).
type HandleResolver interface {
	ResolveHandle(ctx context.Context, handle string) (string, error)
}

// Server implements the sync XRPC surface over a repo.Manager. Zero-valued
// tunables in Options fall back to production defaults; tests shrink them.
type Server struct {
	repo        *repo.Manager
	broadcaster *Broadcaster
	logger      *slog.Logger
	// handles resolves the repo parameter of com.atproto.repo.getRecord when
	// a consumer passes a handle instead of a DID. Optional: nil restricts
	// that parameter to DIDs.
	handles HandleResolver

	hostname   string
	serviceDID string

	writeTimeout time.Duration
	pingInterval time.Duration
	replayBatch  int

	// limiter is the per-client-IP admission limiter for the whole public
	// surface (HTTP reads and WebSocket dials alike); subscribers counts
	// live subscribeRepos connections against maxSubscribers.
	limiter        *ratelimit.Limiter
	maxSubscribers int64
	subscribers    atomic.Int64
	// refusalLog samples the (otherwise noisy) rate-limit refusal Warn logs.
	refusalLog *ratelimit.Sampler

	// onUpgrade, when set, runs on every accepted subscribeRepos socket
	// before streaming starts. Test seam: the slow-consumer eviction test
	// shrinks the kernel send buffer so the write deadline is reachable
	// without megabytes of backlog. Never set in production.
	onUpgrade func(*websocket.Conn)

	// exportCAR streams a DID's repo as a CAR; always repo.ExportCARTo in
	// production. Test seam: the getRepo mid-stream-failure tests substitute
	// exporters that fail before and after the first byte.
	exportCAR func(ctx context.Context, did string, w io.Writer) error
}

// Options configures NewServer. Repo and Broadcaster are required; Hostname
// is the bridge's public hostname (describeServer's handle-domain suffix).
type Options struct {
	Repo        *repo.Manager
	Broadcaster *Broadcaster
	Logger      *slog.Logger
	Hostname    string
	// ServiceDID is the bridge's own DID for describeServer. May be empty
	// until service-identity bootstrap (task 06) provisions one; a did:web
	// derived from Hostname is served in the meantime.
	ServiceDID string
	// HandleResolver lets com.atproto.repo.getRecord accept bridged handles
	// in its repo parameter. Optional; nil means DIDs only.
	HandleResolver HandleResolver

	WriteTimeout time.Duration
	PingInterval time.Duration
	ReplayBatch  int

	// RatePerSecond / RateBurst tune the per-client-IP token bucket guarding
	// every public sync endpoint (config SYNC_RATE_PER_SECOND /
	// SYNC_RATE_BURST); zero values take generous defaults. MaxSubscribers
	// caps concurrent subscribeRepos connections (SYNC_MAX_SUBSCRIBERS).
	RatePerSecond  float64
	RateBurst      int
	MaxSubscribers int
}

// NewServer validates options and builds the server.
func NewServer(opts Options) (*Server, error) {
	if opts.Repo == nil {
		return nil, errors.NewValidationError("repo", "must not be nil")
	}
	if opts.Broadcaster == nil {
		return nil, errors.NewValidationError("broadcaster", "must not be nil")
	}
	if opts.Hostname == "" {
		return nil, errors.NewValidationError("hostname", "must not be empty")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	serviceDID := opts.ServiceDID
	if serviceDID == "" {
		serviceDID = "did:web:" + opts.Hostname
	}
	s := &Server{
		repo:         opts.Repo,
		broadcaster:  opts.Broadcaster,
		logger:       logger,
		handles:      opts.HandleResolver,
		hostname:     opts.Hostname,
		serviceDID:   serviceDID,
		writeTimeout: opts.WriteTimeout,
		pingInterval: opts.PingInterval,
		replayBatch:  opts.ReplayBatch,
	}
	if s.writeTimeout <= 0 {
		s.writeTimeout = defaultWriteTimeout
	}
	if s.pingInterval <= 0 {
		s.pingInterval = defaultPingInterval
	}
	if s.replayBatch <= 0 {
		s.replayBatch = defaultReplayBatch
	}
	perSecond := opts.RatePerSecond
	if perSecond <= 0 {
		perSecond = defaultSyncRatePerSecond
	}
	burst := opts.RateBurst
	if burst <= 0 {
		burst = defaultSyncRateBurst
	}
	s.limiter = ratelimit.New(perSecond, burst)
	s.refusalLog = ratelimit.NewSampler(time.Second)
	s.exportCAR = s.repo.ExportCARTo
	s.maxSubscribers = int64(opts.MaxSubscribers)
	if s.maxSubscribers <= 0 {
		s.maxSubscribers = defaultMaxSubscribers
	}
	return s, nil
}

// Routes mounts the sync surface on a chi router. resolveHandle and
// /.well-known/atproto-did stay in cmd/tidepool (task 03 wired them; they
// belong to the identity package). Every endpoint except _health sits
// behind the per-IP admission limiter (_health is the container healthcheck
// probe — a rate-limited healthcheck would flap the whole stack, and it
// does no per-request work worth guarding).
func (s *Server) Routes(r chi.Router) {
	r.Get("/xrpc/com.atproto.sync.subscribeRepos", s.limited(s.handleSubscribeRepos))
	r.Get("/xrpc/com.atproto.sync.getRepo", s.limited(s.handleGetRepo))
	r.Get("/xrpc/com.atproto.sync.getLatestCommit", s.limited(s.handleGetLatestCommit))
	r.Get("/xrpc/com.atproto.sync.getRecord", s.limited(s.handleGetRecord))
	r.Get("/xrpc/com.atproto.sync.getBlob", s.limited(s.handleGetBlob))
	r.Get("/xrpc/com.atproto.sync.listRepos", s.limited(s.handleListRepos))
	r.Get("/xrpc/com.atproto.sync.getRepoStatus", s.limited(s.handleGetRepoStatus))
	r.Get("/xrpc/com.atproto.repo.getRecord", s.limited(s.handleRepoGetRecord))
	r.Get("/xrpc/com.atproto.server.describeServer", s.limited(s.handleDescribeServer))
	r.Get("/xrpc/_health", s.handleHealth)
}

// limited wraps a handler with the per-client-IP token bucket. Refusals are
// 429 RateLimitExceeded (the XRPC convention; sync consumers are pollers and
// reconnecting WebSocket clients, both of which handle 429 by backing off).
func (s *Server) limited(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.limiter.Allow(ratelimit.ClientIP(r)) {
			SyncRateLimited.Add(1)
			if s.refusalLog.Allow(time.Now()) {
				s.logger.Warn("sync request rate-limited (sampled)", "remote", r.RemoteAddr, "path", r.URL.Path)
			}
			writeXRPCError(w, http.StatusTooManyRequests, "RateLimitExceeded", "rate limit exceeded")
			return
		}
		next(w, r)
	}
}

// loadActiveRepo resolves the did query parameter to an active repo,
// writing the appropriate XRPC error and returning nil when it cannot.
// Deactivated (consent-revoked/tombstoned) repos are refused across the
// whole read surface: a frozen repo's content should not keep being served.
func (s *Server) loadActiveRepo(w http.ResponseWriter, r *http.Request) *repo.RepoInfo {
	did := r.URL.Query().Get("did")
	if did == "" {
		writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "missing required parameter: did")
		return nil
	}
	return s.loadActiveRepoByDID(w, r, did)
}

// loadActiveRepoByDID is loadActiveRepo for handlers that obtain the DID some
// other way than the did query parameter (repo.getRecord's repo parameter,
// possibly via handle resolution).
func (s *Server) loadActiveRepoByDID(w http.ResponseWriter, r *http.Request, did string) *repo.RepoInfo {
	info, err := s.repo.GetRepoInfo(r.Context(), did)
	switch {
	case err == nil:
	case errors.IsNotFound(err):
		writeXRPCError(w, http.StatusNotFound, "RepoNotFound", "repo not found: "+did)
		return nil
	default:
		s.logger.Error("sync: load repo info", "did", did, "error", err)
		writeXRPCError(w, http.StatusInternalServerError, "InternalServerError", "internal error")
		return nil
	}
	if !info.Active {
		writeXRPCError(w, http.StatusBadRequest, "RepoDeactivated", "repo is deactivated: "+did)
		return nil
	}
	return info
}

// handleGetRepo serves com.atproto.sync.getRepo: the repo as a CARv1 stream.
// The CAR is streamed block-by-block (ExportCARTo) so a large community repo
// never buffers whole in memory, and it carries only the reachable set from
// the current head (commit + live MST + record blocks) — correct CAR readers
// (indigo, bigsky) traverse from the root, so omitting superseded blocks is
// transparent.
//
// The optional `since` parameter (diff export) is not implemented — consumers
// that need incremental sync use subscribeRepos; a `since` request gets the
// full CAR, which the spec permits.
//
// Streaming means the response status cannot change once the first block is
// written: existence/consent were already checked by loadActiveRepo, and a
// missing repo (delete race) or unreadable/undecodable head commit fails
// before the first byte — the countingWriter lets those still send a proper
// XRPC error (404 RepoNotFound for the vanished repo, 500 otherwise). A
// failure during the reachable walk happens mid-stream: returning normally
// would let the server write the terminating chunk and hand the client a
// transport-complete 200 wrapping a silently truncated CAR (block-boundary
// truncation is invisible at both the HTTP and CAR-framing layers), so the
// handler aborts the connection instead and the client sees a transport-level
// failure. The exception is the client's own disconnect, which is logged
// quietly and otherwise ignored.
func (s *Server) handleGetRepo(w http.ResponseWriter, r *http.Request) {
	info := s.loadActiveRepo(w, r)
	if info == nil {
		return
	}
	w.Header().Set("Content-Type", "application/vnd.ipld.car")
	cw := &countingWriter{w: w}
	err := s.exportCAR(r.Context(), info.DID, cw)
	switch {
	case err == nil:
	case r.Context().Err() != nil:
		// The client hung up mid-download. Routine on a public sync surface;
		// keep it out of the Error stream so real export failures stay
		// visible above the disconnect noise.
		s.logger.Debug("sync: export CAR abandoned by client", "did", info.DID, "wrote", cw.n, "error", err)
	case cw.n == 0 && errors.IsNotFound(err):
		// The repo vanished between loadActiveRepo and the export; nothing
		// has been written yet, so the same 404 the earlier existence check
		// produces is still deliverable.
		writeXRPCError(w, http.StatusNotFound, "RepoNotFound", "repo not found: "+info.DID)
	case cw.n == 0:
		s.logger.Error("sync: export CAR", "did", info.DID, "error", err)
		writeXRPCError(w, http.StatusInternalServerError, "InternalServerError", "internal error")
	default:
		// Bytes are already on the wire: the 200 cannot be revoked, and a
		// normal return would let the server finish the chunked body around
		// a truncated CAR. Reset the connection so the truncation is visible
		// at the transport layer; http.ErrAbortHandler is the stdlib's
		// sanctioned abort (its stack trace is suppressed).
		s.logger.Error("sync: export CAR failed mid-stream", "did", info.DID, "wrote", cw.n, "error", err)
		panic(http.ErrAbortHandler)
	}
}

// countingWriter tracks whether any bytes have reached the client, so a
// streaming handler knows whether it can still send an HTTP error status.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// handleGetLatestCommit serves com.atproto.sync.getLatestCommit.
func (s *Server) handleGetLatestCommit(w http.ResponseWriter, r *http.Request) {
	info := s.loadActiveRepo(w, r)
	if info == nil {
		return
	}
	writeJSON(w, http.StatusOK, comatproto.SyncGetLatestCommit_Output{
		Cid: info.HeadCID,
		Rev: info.Rev,
	})
}

// handleGetRecord serves com.atproto.sync.getRecord: a proof CAR containing
// the commit block, the MST path, and the record block.
func (s *Server) handleGetRecord(w http.ResponseWriter, r *http.Request) {
	info := s.loadActiveRepo(w, r)
	if info == nil {
		return
	}
	q := r.URL.Query()
	collection, rkey := q.Get("collection"), q.Get("rkey")
	if collection == "" || rkey == "" {
		writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "missing required parameter: collection and rkey")
		return
	}
	proof, err := s.repo.GetRecordProof(r.Context(), info.DID, collection, rkey)
	switch {
	case err == nil:
	case errors.IsNotFound(err):
		writeXRPCError(w, http.StatusNotFound, "RecordNotFound", "record not found")
		return
	case errors.IsValidation(err):
		writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	default:
		s.logger.Error("sync: build record proof",
			"did", info.DID, "collection", collection, "rkey", rkey, "error", err)
		writeXRPCError(w, http.StatusInternalServerError, "InternalServerError", "internal error")
		return
	}
	w.Header().Set("Content-Type", "application/vnd.ipld.car")
	w.Header().Set("Content-Length", strconv.Itoa(len(proof)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(proof)
}

// repoGetRecordOutput mirrors com.atproto.repo.getRecord's output schema.
// indigo's RepoGetRecord_Output wraps Value in a LexiconTypeDecoder, which
// round-trips through registered lexicon structs; the bridge stores records
// as plain maps, so a local shape serves them unmodified.
type repoGetRecordOutput struct {
	URI   string         `json:"uri"`
	CID   string         `json:"cid"`
	Value map[string]any `json:"value"`
}

// handleRepoGetRecord serves com.atproto.repo.getRecord: the JSON form of a
// single record. The sync.* CAR endpoints cover relays, but AppView-side
// reconcilers (the Coves profile backfill) speak the repo.* JSON surface —
// without this endpoint a missed firehose event cannot be backfilled by
// consumers that speak only repo.* reads.
//
// The repo parameter is an at-identifier: a DID, or a bridged handle when a
// HandleResolver is configured (a leading @ is tolerated, matching
// resolveHandle). Handle-resolution failures — unknown and malformed alike
// — are folded into RepoNotFound so a prober learns nothing the DID path
// would not also reveal. The optional cid parameter pins a specific
// version; the bridge retains only the current one, so any other CID is a
// RecordNotFound — the same answer a reference PDS gives, since its
// repo.getRecord also serves only the current version.
func (s *Server) handleRepoGetRecord(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	repoID := strings.TrimPrefix(q.Get("repo"), "@")
	collection, rkey := q.Get("collection"), q.Get("rkey")
	if repoID == "" || collection == "" || rkey == "" {
		writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "missing required parameter: repo, collection, and rkey")
		return
	}
	did := repoID
	if !strings.HasPrefix(did, "did:") {
		if s.handles == nil {
			writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "repo must be a DID")
			return
		}
		resolved, err := s.handles.ResolveHandle(r.Context(), repoID)
		switch {
		case err == nil:
			did = resolved
		case errors.IsNotFound(err), errors.IsValidation(err):
			writeXRPCError(w, http.StatusNotFound, "RepoNotFound", "repo not found: "+repoID)
			return
		default:
			s.logger.Error("sync: resolve repo handle", "handle", repoID, "error", err)
			writeXRPCError(w, http.StatusInternalServerError, "InternalServerError", "internal error")
			return
		}
	}
	info := s.loadActiveRepoByDID(w, r, did)
	if info == nil {
		return
	}
	record, recordCID, err := s.repo.GetRecord(r.Context(), info.DID, collection, rkey)
	switch {
	case err == nil:
	case errors.IsNotFound(err):
		writeXRPCError(w, http.StatusNotFound, "RecordNotFound", "record not found")
		return
	case errors.IsValidation(err):
		writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	default:
		s.logger.Error("sync: get record",
			"did", info.DID, "collection", collection, "rkey", rkey, "error", err)
		writeXRPCError(w, http.StatusInternalServerError, "InternalServerError", "internal error")
		return
	}
	if want := q.Get("cid"); want != "" {
		// Parse rather than string-compare: a valid CID in a non-canonical
		// multibase encoding must still match, and a malformed pin is the
		// caller's bug (400), not data absence (404).
		wantCID, err := cid.Parse(want)
		if err != nil {
			writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "invalid cid parameter")
			return
		}
		if wantCID.String() != recordCID {
			writeXRPCError(w, http.StatusNotFound, "RecordNotFound",
				"record not found at cid: only the current version is retained")
			return
		}
	}
	writeJSON(w, http.StatusOK, repoGetRecordOutput{
		URI:   "at://" + info.DID + "/" + collection + "/" + rkey,
		CID:   recordCID,
		Value: record,
	})
}

// handleGetBlob serves com.atproto.sync.getBlob: the raw bytes of a stored
// blob (avatars, banners, post images the materializer fetched). Like the
// other content endpoints it refuses deactivated repos.
func (s *Server) handleGetBlob(w http.ResponseWriter, r *http.Request) {
	info := s.loadActiveRepo(w, r)
	if info == nil {
		return
	}
	cidStr := r.URL.Query().Get("cid")
	if cidStr == "" {
		writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "missing required parameter: cid")
		return
	}
	data, mimeType, err := s.repo.GetBlob(r.Context(), info.DID, cidStr)
	switch {
	case err == nil:
	case errors.IsNotFound(err):
		writeXRPCError(w, http.StatusNotFound, "BlobNotFound", "blob not found")
		return
	case errors.IsValidation(err):
		writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	default:
		s.logger.Error("sync: get blob", "did", info.DID, "cid", cidStr, "error", err)
		writeXRPCError(w, http.StatusInternalServerError, "InternalServerError", "internal error")
		return
	}
	w.Header().Set("Content-Type", mimeType)
	// Blob bytes are attacker-influenced media from remote instances: make
	// sure browsers never sniff them into something executable.
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; sandbox")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// handleListRepos serves com.atproto.sync.listRepos with DID-keyed
// pagination.
func (s *Server) handleListRepos(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := defaultListReposLimit
	if raw := q.Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "limit must be a positive integer")
			return
		}
		limit = min(parsed, maxListReposLimit)
	}
	infos, err := s.repo.ListRepos(r.Context(), q.Get("cursor"), limit)
	if err != nil {
		s.logger.Error("sync: list repos", "error", err)
		writeXRPCError(w, http.StatusInternalServerError, "InternalServerError", "internal error")
		return
	}
	out := comatproto.SyncListRepos_Output{Repos: make([]*comatproto.SyncListRepos_Repo, 0, len(infos))}
	for _, info := range infos {
		entry := &comatproto.SyncListRepos_Repo{
			Did:    info.DID,
			Head:   info.HeadCID,
			Rev:    info.Rev,
			Active: boolPtr(info.Active),
		}
		if info.Status != "" {
			entry.Status = strPtr(info.Status)
		}
		out.Repos = append(out.Repos, entry)
	}
	if len(infos) == limit {
		out.Cursor = strPtr(infos[len(infos)-1].DID)
	}
	writeJSON(w, http.StatusOK, out)
}

// handleGetRepoStatus serves com.atproto.sync.getRepoStatus. Unlike the
// content endpoints it answers for deactivated repos too — that is its job.
func (s *Server) handleGetRepoStatus(w http.ResponseWriter, r *http.Request) {
	did := r.URL.Query().Get("did")
	if did == "" {
		writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "missing required parameter: did")
		return
	}
	info, err := s.repo.GetRepoInfo(r.Context(), did)
	switch {
	case err == nil:
	case errors.IsNotFound(err):
		writeXRPCError(w, http.StatusNotFound, "RepoNotFound", "repo not found: "+did)
		return
	default:
		s.logger.Error("sync: get repo status", "did", did, "error", err)
		writeXRPCError(w, http.StatusInternalServerError, "InternalServerError", "internal error")
		return
	}
	out := comatproto.SyncGetRepoStatus_Output{Did: info.DID, Active: info.Active}
	if info.Active {
		out.Rev = strPtr(info.Rev)
	} else if info.Status != "" {
		out.Status = strPtr(info.Status)
	}
	writeJSON(w, http.StatusOK, out)
}

// handleDescribeServer serves com.atproto.server.describeServer — the
// minimum relays and crawlers expect from a PDS-shaped host.
func (s *Server) handleDescribeServer(w http.ResponseWriter, r *http.Request) {
	inviteRequired := true // no account creation on the bridge, ever
	writeJSON(w, http.StatusOK, comatproto.ServerDescribeServer_Output{
		AvailableUserDomains: []string{"." + s.hostname},
		Did:                  s.serviceDID,
		InviteCodeRequired:   &inviteRequired,
	})
}

// handleHealth serves /xrpc/_health.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"version": healthVersion})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeXRPCError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]string{"error": code, "message": message})
}

func strPtr(s string) *string { return &s }
func boolPtr(b bool) *bool    { return &b }
