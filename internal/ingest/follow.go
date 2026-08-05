package ingest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	stderrors "errors"
	"expvar"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// FollowClient is the slice of *ap.Client the follow lifecycle needs.
type FollowClient interface {
	ResolveHandle(ctx context.Context, handle string) (string, error)
	FetchActor(ctx context.Context, iri string) (*ap.Object, error)
	SendActivity(ctx context.Context, inboxURL string, activity any) error
}

// AdminOptions configures NewAdmin. Token, Client, Materializer,
// Communities, and Service are required; Backfill, Repos, Sweeper, and
// Logger are optional (each missing dependency's endpoint answers 501).
type AdminOptions struct {
	// Token is the bearer token protecting /admin (config.AdminToken).
	Token string
	// Client resolves handles, fetches Group actors, and delivers the
	// signed Follow/Undo activities.
	Client FollowClient
	// Materializer bridges the community before we follow it (profile
	// record first, PLAN decision 3).
	Materializer Materializer
	Communities  store.Communities
	// Service is the AP actor the Follow is issued by.
	Service *ap.ServiceActor
	// Backfill serves the on-demand backfill endpoint (optional).
	Backfill Backfiller
	// Repos serves POST /admin/reemit (optional; the endpoint answers 501
	// when nil). See reemit.go for what re-emission is for.
	Repos RepoReemitter
	// Sweeper serves POST /admin/objects/sweep-deleted (optional; the
	// endpoint answers 501 when nil). See sweep.go.
	Sweeper DeleteSweeper
	Logger  *slog.Logger
}

// Admin is the operator API driving the community subscription lifecycle,
// plus the maintenance endpoints for bridged objects, repos, and counters:
//
//	POST   /admin/communities           {"community":"!tech@lemmy.world"}
//	DELETE /admin/communities           {"community":"!tech@lemmy.world"}
//	GET    /admin/communities
//	POST   /admin/communities/backfill  {"community":"!tech@lemmy.world"}
//	POST   /admin/communities/reconcile (follow list configured only)
//	POST   /admin/reemit                {"did":"did:plc:..."} (or {} for all)
//	POST   /admin/objects/sweep-deleted {"ap_ids":["https://..."]}
//	GET    /admin/metrics               (tidepool's own expvar counters)
//
// All endpoints require "Authorization: Bearer $ADMIN_TOKEN".
type Admin struct {
	token       string
	client      FollowClient
	mat         Materializer
	communities store.Communities
	service     *ap.ServiceActor
	backfill    Backfiller
	repos       RepoReemitter
	sweeper     DeleteSweeper
	logger      *slog.Logger
	// reconciler serves POST /admin/communities/reconcile; nil (the
	// endpoint answers 501) unless a follow list is configured. Set once
	// during startup via SetFollowReconciler, before the server listens.
	reconciler *FollowReconciler
}

// SetFollowReconciler wires the optional follow-list reconciler in after
// construction (the reconciler itself needs the Admin's subscribe cores, so
// it is necessarily built second).
func (a *Admin) SetFollowReconciler(r *FollowReconciler) { a.reconciler = r }

// NewAdmin validates options and builds the Admin API.
func NewAdmin(opts AdminOptions) (*Admin, error) {
	if opts.Token == "" {
		return nil, errors.NewValidationError("token", "must not be empty")
	}
	if opts.Client == nil {
		return nil, errors.NewValidationError("client", "must not be nil")
	}
	if opts.Materializer == nil {
		return nil, errors.NewValidationError("materializer", "must not be nil")
	}
	if opts.Communities == nil {
		return nil, errors.NewValidationError("communities", "must not be nil")
	}
	if opts.Service == nil {
		return nil, errors.NewValidationError("service", "must not be nil")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Admin{
		token:       opts.Token,
		client:      opts.Client,
		mat:         opts.Materializer,
		communities: opts.Communities,
		service:     opts.Service,
		backfill:    opts.Backfill,
		repos:       opts.Repos,
		sweeper:     opts.Sweeper,
		logger:      logger,
	}, nil
}

// Routes mounts the admin API on a chi router. /admin/metrics serves ONLY
// tidepool's own expvar counters (keys prefixed "tidepool_": the lexicon
// validation-failure counter and the per-surface rate-limit refusal
// counters), bearer-protected like the rest of /admin. It deliberately does
// NOT use expvar.Handler(), which would also expose Go's global "cmdline"
// and "memstats" — process internals no operator asked to publish.
func (a *Admin) Routes(r chi.Router) {
	r.Route("/admin", func(r chi.Router) {
		r.Use(requireBearer(a.token, a.logger))
		r.Post("/communities", a.handleSubscribe)
		r.Delete("/communities", a.handleUnsubscribe)
		r.Get("/communities", a.handleList)
		r.Post("/communities/backfill", a.handleBackfill)
		r.Post("/communities/reconcile", a.handleReconcile)
		r.Post("/reemit", a.handleReemit)
		r.Post("/objects/sweep-deleted", a.handleSweepDeleted)
		r.Method(http.MethodGet, "/metrics", http.HandlerFunc(scopedMetrics))
	})
}

// scopedMetrics writes the JSON expvar map filtered to tidepool's own
// counters — the same wire format as expvar.Handler(), minus Go's global
// cmdline/memstats.
func scopedMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = io.WriteString(w, "{")
	first := true
	expvar.Do(func(kv expvar.KeyValue) {
		if !strings.HasPrefix(kv.Key, "tidepool") {
			return
		}
		if !first {
			_, _ = io.WriteString(w, ",")
		}
		first = false
		_, _ = fmt.Fprintf(w, "%q: %s", kv.Key, kv.Value)
	})
	_, _ = io.WriteString(w, "}\n")
}

// communityRequest is the shared request body: a Lemmy-style handle
// ("!tech@lemmy.world") or the Group's AP id URL.
type communityRequest struct {
	Community string `json:"community"`
}

// communityResponse reports a community's bridge state.
type communityResponse struct {
	Community      string `json:"community"`
	DID            string `json:"did"`
	FollowState    string `json:"follow_state"`
	LastBackfillAt string `json:"last_backfill_at,omitempty"`
}

func communityJSON(c *store.Community) communityResponse {
	resp := communityResponse{
		Community:   c.APGroupID,
		DID:         c.DID,
		FollowState: string(c.FollowState),
	}
	if c.LastBackfillAt != nil {
		resp.LastBackfillAt = c.LastBackfillAt.UTC().Format("2006-01-02T15:04:05Z")
	}
	return resp
}

// adminError carries the admin API's HTTP mapping for a failed
// subscribe/unsubscribe step, so the HTTP handlers and the follow-list
// reconciler can share one transport-agnostic core. The core logs each
// failure at the site that understands it; handlers only translate.
type adminError struct {
	status int    // HTTP status the admin API reports
	public string // operator-facing message (response body)
	err    error  // underlying cause; nil for pure policy refusals
}

func (e *adminError) Error() string {
	if e.err != nil {
		return e.public + ": " + e.err.Error()
	}
	return e.public
}

func (e *adminError) Unwrap() error { return e.err }

// writeAdminError translates a subscribe/unsubscribe core failure into the
// HTTP response. Non-adminError errors cannot happen today (the cores wrap
// everything), but map to 500 rather than panicking on a future oversight.
func writeAdminError(w http.ResponseWriter, err error) {
	var ae *adminError
	if stderrors.As(err, &ae) {
		http.Error(w, ae.public, ae.status)
		return
	}
	http.Error(w, "internal error", http.StatusInternalServerError)
}

// handleSubscribe resolves, bridges, and follows a community:
// WebFinger → fetch Group → materialize community profile → signed Follow
// from the service actor → follow_state pending (Accept arrives via the
// inbox and flips it to accepted, which triggers backfill).
func (a *Admin) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	var req communityRequest
	if err := decodeJSONBody(r, &req); err != nil || strings.TrimSpace(req.Community) == "" {
		http.Error(w, `body must be {"community":"!name@instance"}`, http.StatusBadRequest)
		return
	}

	community, err := a.subscribe(r.Context(), req.Community)
	if err != nil {
		writeAdminError(w, err)
		return
	}
	if community.FollowState == store.FollowStateAccepted {
		// Already subscribed; idempotent success.
		writeJSON(w, http.StatusOK, communityJSON(community))
		return
	}
	writeJSON(w, http.StatusAccepted, communityJSON(community))
}

// subscribe is the transport-agnostic core of handleSubscribe, shared with
// the follow-list reconciler. On success the returned community is either
// already accepted (idempotent no-op) or freshly pending.
func (a *Admin) subscribe(ctx context.Context, ref string) (*store.Community, error) {
	groupIRI, err := a.resolveCommunity(ctx, ref)
	if err != nil {
		return nil, a.resolveError(ref, err)
	}

	// Bridge the community first: DID minted, community.profile committed —
	// content referencing it can land the moment announces start.
	community, err := a.mat.EnsureCommunity(ctx, &ap.Object{ID: groupIRI})
	if err != nil {
		if materialize.IsSkip(err) {
			a.logger.Warn("subscribe refused", "community", groupIRI, "reason", err.Error())
			return nil, &adminError{status: http.StatusUnprocessableEntity,
				public: "community cannot be bridged: " + err.Error(), err: err}
		}
		a.logger.Error("subscribe: materialize community", "community", groupIRI, "error", err)
		return nil, &adminError{status: http.StatusBadGateway,
			public: "failed to bridge community", err: err}
	}

	if community.FollowState == store.FollowStateAccepted {
		return community, nil
	}

	group, err := a.client.FetchActor(ctx, groupIRI)
	if err != nil {
		a.logger.Error("subscribe: fetch group", "community", groupIRI, "error", err)
		return nil, &adminError{status: http.StatusBadGateway,
			public: "failed to fetch community actor", err: err}
	}
	inbox := group.SharedInboxOrInbox()
	if inbox == "" {
		return nil, &adminError{status: http.StatusUnprocessableEntity,
			public: "community actor advertises no inbox"}
	}

	follow, err := a.buildFollow(groupIRI)
	if err != nil {
		a.logger.Error("subscribe: build follow", "community", groupIRI, "error", err)
		return nil, &adminError{status: http.StatusInternalServerError,
			public: "failed to build Follow", err: err}
	}
	if err := a.client.SendActivity(ctx, inbox, follow); err != nil {
		a.logger.Error("subscribe: deliver follow", "community", groupIRI, "error", err)
		return nil, &adminError{status: http.StatusBadGateway,
			public: "failed to deliver Follow", err: err}
	}
	if err := a.communities.SetFollowState(ctx, groupIRI, store.FollowStatePending); err != nil {
		a.logger.Error("subscribe: record pending follow", "community", groupIRI, "error", err)
		return nil, &adminError{status: http.StatusInternalServerError,
			public: "failed to record follow state", err: err}
	}
	a.logger.Info("follow sent; awaiting accept", "community", groupIRI, "did", community.DID)

	community.FollowState = store.FollowStatePending
	return community, nil
}

// handleUnsubscribe sends Undo{Follow} and clears the follow state. The
// local state is cleared even when the remote can no longer be reached —
// unsubscribing from a dead instance must succeed.
func (a *Admin) handleUnsubscribe(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var req communityRequest
	if err := decodeJSONBody(r, &req); err != nil || strings.TrimSpace(req.Community) == "" {
		http.Error(w, `body must be {"community":"!name@instance"}`, http.StatusBadRequest)
		return
	}

	groupIRI, err := a.resolveCommunity(ctx, req.Community)
	if err != nil {
		a.writeResolveError(w, req.Community, err)
		return
	}
	community, err := a.unsubscribeByGroupIRI(ctx, groupIRI)
	if err != nil {
		writeAdminError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, communityJSON(community))
}

// unsubscribeByGroupIRI is the transport-agnostic core of handleUnsubscribe,
// shared with the follow-list reconciler (which diffs by canonical group IRI
// and so never needs the resolve step). Undo{Follow} delivery is best-effort;
// clearing local follow state is the authoritative effect. Materialized
// records are never deleted — content just stops flowing.
func (a *Admin) unsubscribeByGroupIRI(ctx context.Context, groupIRI string) (*store.Community, error) {
	community, err := a.communities.GetByAPGroupID(ctx, groupIRI)
	if errors.IsNotFound(err) {
		return nil, &adminError{status: http.StatusNotFound,
			public: "community is not bridged", err: err}
	}
	if err != nil {
		a.logger.Error("unsubscribe: look up community", "community", groupIRI, "error", err)
		return nil, &adminError{status: http.StatusInternalServerError,
			public: "internal error", err: err}
	}

	// Best-effort remote notification.
	if group, err := a.client.FetchActor(ctx, groupIRI); err == nil {
		if inbox := group.SharedInboxOrInbox(); inbox != "" {
			undo, err := a.buildUndoFollow(groupIRI)
			if err != nil {
				// A local crypto/rand failure: skip the notification rather
				// than emitting a zero-entropy id, but still clear local state
				// (unsubscribing must succeed even if the Undo can't be sent).
				a.logger.Warn("unsubscribe: build undo failed (state cleared anyway)",
					"community", groupIRI, "error", err)
			} else if err := a.client.SendActivity(ctx, inbox, undo); err != nil {
				a.logger.Warn("unsubscribe: deliver undo failed (state cleared anyway)",
					"community", groupIRI, "error", err)
			}
		}
	} else {
		a.logger.Warn("unsubscribe: fetch group failed (state cleared anyway)",
			"community", groupIRI, "error", err)
	}

	if err := a.communities.SetFollowState(ctx, groupIRI, store.FollowStateNone); err != nil {
		a.logger.Error("unsubscribe: clear follow state", "community", groupIRI, "error", err)
		return nil, &adminError{status: http.StatusInternalServerError,
			public: "failed to clear follow state", err: err}
	}
	a.logger.Info("community unfollowed", "community", groupIRI)
	community.FollowState = store.FollowStateNone
	return community, nil
}

// handleList reports every community in accepted or pending state.
func (a *Admin) handleList(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	out := []communityResponse{}
	for _, state := range []store.FollowState{store.FollowStateAccepted, store.FollowStatePending} {
		communities, err := a.communities.ListByFollowState(ctx, state)
		if err != nil {
			a.logger.Error("list communities", "state", state, "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		for _, c := range communities {
			out = append(out, communityJSON(c))
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"communities": out})
}

// handleBackfill triggers an on-demand backfill for a subscribed community.
func (a *Admin) handleBackfill(w http.ResponseWriter, r *http.Request) {
	if a.backfill == nil {
		http.Error(w, "backfill is not configured", http.StatusNotImplemented)
		return
	}
	ctx := r.Context()
	var req communityRequest
	if err := decodeJSONBody(r, &req); err != nil || strings.TrimSpace(req.Community) == "" {
		http.Error(w, `body must be {"community":"!name@instance"}`, http.StatusBadRequest)
		return
	}
	groupIRI, err := a.resolveCommunity(ctx, req.Community)
	if err != nil {
		a.writeResolveError(w, req.Community, err)
		return
	}
	community, err := a.communities.GetByAPGroupID(ctx, groupIRI)
	if errors.IsNotFound(err) {
		http.Error(w, "community is not bridged", http.StatusNotFound)
		return
	}
	if err != nil {
		a.logger.Error("backfill: look up community", "community", groupIRI, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	a.backfill.TriggerAsync(community, true)
	writeJSON(w, http.StatusAccepted, communityJSON(community))
}

// handleReconcile runs one synchronous follow-list sweep on demand, so an
// operator can converge right after editing the file instead of waiting for
// the next tick. 501 when no follow list is configured (the handleBackfill
// nil-dependency pattern).
func (a *Admin) handleReconcile(w http.ResponseWriter, r *http.Request) {
	if a.reconciler == nil {
		http.Error(w, "follow list reconciliation is not configured", http.StatusNotImplemented)
		return
	}
	result, err := a.reconciler.Sweep(r.Context())
	if err != nil {
		// Parse/list failures abort the pass with no state changes; the
		// admin API is operator-facing, so the real reason (file path,
		// entry number) goes straight back.
		http.Error(w, "reconcile failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// resolveCommunity turns the request's community reference into the Group's
// AP id: URLs pass through, handles go through WebFinger.
func (a *Admin) resolveCommunity(ctx context.Context, ref string) (string, error) {
	ref = strings.TrimSpace(ref)
	if strings.HasPrefix(ref, "https://") || strings.HasPrefix(ref, "http://") {
		return ref, nil
	}
	return a.client.ResolveHandle(ctx, ref)
}

// resolveError maps a resolveCommunity failure onto the admin API's HTTP
// vocabulary (shared by the HTTP handlers and the reconciler-driven
// subscribe core).
func (a *Admin) resolveError(ref string, err error) *adminError {
	switch {
	case errors.IsValidation(err):
		return &adminError{status: http.StatusBadRequest, public: err.Error(), err: err}
	case errors.IsNotFound(err):
		return &adminError{status: http.StatusNotFound, public: "community not found: " + ref, err: err}
	default:
		a.logger.Error("resolve community", "community", ref, "error", err)
		return &adminError{status: http.StatusBadGateway, public: "failed to resolve community", err: err}
	}
}

func (a *Admin) writeResolveError(w http.ResponseWriter, ref string, err error) {
	e := a.resolveError(ref, err)
	http.Error(w, e.public, e.status)
}

// buildFollow constructs the signed Follow activity (delivery signing
// happens in the client; this is the payload Lemmy validates).
func (a *Admin) buildFollow(groupIRI string) (*ap.Object, error) {
	return buildFollowActivity(a.service, groupIRI)
}

// buildFollowActivity is the package-level Follow constructor, shared by
// the admin subscribe path and the follow retrier. Every call mints a
// FRESH activity id — Lemmy dedupes by id, so a re-sent Follow must never
// reuse one (the whole point of the retry is provoking a fresh Accept).
func buildFollowActivity(service *ap.ServiceActor, groupIRI string) (*ap.Object, error) {
	id, err := mintActivityID(service, "follow")
	if err != nil {
		return nil, err
	}
	return &ap.Object{
		Context: json.RawMessage(`"https://www.w3.org/ns/activitystreams"`),
		ID:      id,
		Type:    ap.TypeFollow,
		Actor:   &ap.Object{ID: service.ID},
		Object:  &ap.Object{ID: groupIRI},
		To:      ap.Audience{groupIRI},
	}, nil
}

// buildUndoFollow constructs Undo{Follow}. Lemmy matches the undo by the
// inner Follow's actor and object.
func (a *Admin) buildUndoFollow(groupIRI string) (*ap.Object, error) {
	inner, err := a.buildFollow(groupIRI)
	if err != nil {
		return nil, err
	}
	inner.Context = nil
	id, err := a.activityID("undo")
	if err != nil {
		return nil, err
	}
	return &ap.Object{
		Context: json.RawMessage(`"https://www.w3.org/ns/activitystreams"`),
		ID:      id,
		Type:    ap.TypeUndo,
		Actor:   &ap.Object{ID: a.service.ID},
		Object:  inner,
		To:      ap.Audience{groupIRI},
	}, nil
}

// activityID mints a unique bridge-side activity id. A crypto/rand failure
// is propagated rather than swallowed: proceeding with the zero buffer would
// emit a constant, remote-deduped id (…/kind/000…0), and Lemmy dedupes by
// activity id — so the first such activity would land and every later
// Follow/Undo{Follow} would be silently ignored while the admin API still
// reports success. The buildFollow/buildUndoFollow callers map this to a 5xx.
func (a *Admin) activityID(kind string) (string, error) {
	return mintActivityID(a.service, kind)
}

func mintActivityID(service *ap.ServiceActor, kind string) (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("ingest: mint activity id: %w", err)
	}
	return fmt.Sprintf("%s/activities/%s/%s", service.BaseURL(), kind, hex.EncodeToString(buf[:])), nil
}

// writeJSON writes a JSON response body.
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
