package sync

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"

	"tidepool/internal/errors"
	"tidepool/internal/repo"
)

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
)

// Server implements the sync XRPC surface over a repo.Manager. Zero-valued
// tunables in Options fall back to production defaults; tests shrink them.
type Server struct {
	repo        *repo.Manager
	broadcaster *Broadcaster
	logger      *slog.Logger

	hostname   string
	serviceDID string

	writeTimeout time.Duration
	pingInterval time.Duration
	replayBatch  int

	// onUpgrade, when set, runs on every accepted subscribeRepos socket
	// before streaming starts. Test seam: the slow-consumer eviction test
	// shrinks the kernel send buffer so the write deadline is reachable
	// without megabytes of backlog. Never set in production.
	onUpgrade func(*websocket.Conn)
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

	WriteTimeout time.Duration
	PingInterval time.Duration
	ReplayBatch  int
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
	return s, nil
}

// Routes mounts the sync surface on a chi router. resolveHandle and
// /.well-known/atproto-did stay in cmd/tidepool (task 03 wired them; they
// belong to the identity package).
func (s *Server) Routes(r chi.Router) {
	r.Get("/xrpc/com.atproto.sync.subscribeRepos", s.handleSubscribeRepos)
	r.Get("/xrpc/com.atproto.sync.getRepo", s.handleGetRepo)
	r.Get("/xrpc/com.atproto.sync.getLatestCommit", s.handleGetLatestCommit)
	r.Get("/xrpc/com.atproto.sync.getRecord", s.handleGetRecord)
	r.Get("/xrpc/com.atproto.sync.listRepos", s.handleListRepos)
	r.Get("/xrpc/com.atproto.sync.getRepoStatus", s.handleGetRepoStatus)
	r.Get("/xrpc/com.atproto.server.describeServer", s.handleDescribeServer)
	r.Get("/xrpc/_health", s.handleHealth)
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

// handleGetRepo serves com.atproto.sync.getRepo: the full repo as a CARv1
// stream. The optional `since` parameter (diff export) is not implemented —
// consumers that need incremental sync use subscribeRepos; a `since` request
// gets the full CAR, which the spec permits (extra blocks are legal).
//
// NOTE: until block GC exists the export includes historical (unreachable)
// blocks. CAR consumers traverse from the root commit, so extra blocks are
// harmless — Jetstream and indigo's LoadRepoFromCAR both tolerate them — and
// a minimal reachable-set walk over large community repos would cost far
// more than it saves at bridge scale. Revisit alongside block GC.
func (s *Server) handleGetRepo(w http.ResponseWriter, r *http.Request) {
	info := s.loadActiveRepo(w, r)
	if info == nil {
		return
	}
	carBytes, err := s.repo.ExportCAR(r.Context(), info.DID)
	if err != nil {
		s.logger.Error("sync: export CAR", "did", info.DID, "error", err)
		writeXRPCError(w, http.StatusInternalServerError, "InternalServerError", "internal error")
		return
	}
	w.Header().Set("Content-Type", "application/vnd.ipld.car")
	w.Header().Set("Content-Length", strconv.Itoa(len(carBytes)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(carBytes)
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
