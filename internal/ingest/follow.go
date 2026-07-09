package ingest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
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

// AdminOptions configures NewAdmin. Everything except Logger is required.
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
	Logger   *slog.Logger
}

// Admin is the operator API driving the community subscription lifecycle:
//
//	POST   /admin/communities          {"community":"!tech@lemmy.world"}
//	DELETE /admin/communities          {"community":"!tech@lemmy.world"}
//	GET    /admin/communities
//	POST   /admin/communities/backfill {"community":"!tech@lemmy.world"}
//
// All endpoints require "Authorization: Bearer $ADMIN_TOKEN".
type Admin struct {
	token       string
	client      FollowClient
	mat         Materializer
	communities store.Communities
	service     *ap.ServiceActor
	backfill    Backfiller
	logger      *slog.Logger
}

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
		logger:      logger,
	}, nil
}

// Routes mounts the admin API on a chi router.
func (a *Admin) Routes(r chi.Router) {
	r.Route("/admin", func(r chi.Router) {
		r.Use(requireBearer(a.token, a.logger))
		r.Post("/communities", a.handleSubscribe)
		r.Delete("/communities", a.handleUnsubscribe)
		r.Get("/communities", a.handleList)
		r.Post("/communities/backfill", a.handleBackfill)
	})
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

// handleSubscribe resolves, bridges, and follows a community:
// WebFinger → fetch Group → materialize community profile → signed Follow
// from the service actor → follow_state pending (Accept arrives via the
// inbox and flips it to accepted, which triggers backfill).
func (a *Admin) handleSubscribe(w http.ResponseWriter, r *http.Request) {
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

	// Bridge the community first: DID minted, community.profile committed —
	// content referencing it can land the moment announces start.
	community, err := a.mat.EnsureCommunity(ctx, &ap.Object{ID: groupIRI})
	if err != nil {
		if materialize.IsSkip(err) {
			a.logger.Warn("subscribe refused", "community", groupIRI, "reason", err.Error())
			http.Error(w, "community cannot be bridged: "+err.Error(), http.StatusUnprocessableEntity)
			return
		}
		a.logger.Error("subscribe: materialize community", "community", groupIRI, "error", err)
		http.Error(w, "failed to bridge community", http.StatusBadGateway)
		return
	}

	if community.FollowState == store.FollowStateAccepted {
		// Already subscribed; idempotent success.
		writeJSON(w, http.StatusOK, communityJSON(community))
		return
	}

	group, err := a.client.FetchActor(ctx, groupIRI)
	if err != nil {
		a.logger.Error("subscribe: fetch group", "community", groupIRI, "error", err)
		http.Error(w, "failed to fetch community actor", http.StatusBadGateway)
		return
	}
	inbox := group.SharedInboxOrInbox()
	if inbox == "" {
		http.Error(w, "community actor advertises no inbox", http.StatusUnprocessableEntity)
		return
	}

	follow, err := a.buildFollow(groupIRI)
	if err != nil {
		a.logger.Error("subscribe: build follow", "community", groupIRI, "error", err)
		http.Error(w, "failed to build Follow", http.StatusInternalServerError)
		return
	}
	if err := a.client.SendActivity(ctx, inbox, follow); err != nil {
		a.logger.Error("subscribe: deliver follow", "community", groupIRI, "error", err)
		http.Error(w, "failed to deliver Follow", http.StatusBadGateway)
		return
	}
	if err := a.communities.SetFollowState(ctx, groupIRI, store.FollowStatePending); err != nil {
		a.logger.Error("subscribe: record pending follow", "community", groupIRI, "error", err)
		http.Error(w, "failed to record follow state", http.StatusInternalServerError)
		return
	}
	a.logger.Info("follow sent; awaiting accept", "community", groupIRI, "did", community.DID)

	community.FollowState = store.FollowStatePending
	writeJSON(w, http.StatusAccepted, communityJSON(community))
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
	community, err := a.communities.GetByAPGroupID(ctx, groupIRI)
	if errors.IsNotFound(err) {
		http.Error(w, "community is not bridged", http.StatusNotFound)
		return
	}
	if err != nil {
		a.logger.Error("unsubscribe: look up community", "community", groupIRI, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
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
		http.Error(w, "failed to clear follow state", http.StatusInternalServerError)
		return
	}
	a.logger.Info("community unfollowed", "community", groupIRI)
	community.FollowState = store.FollowStateNone
	writeJSON(w, http.StatusOK, communityJSON(community))
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

// resolveCommunity turns the request's community reference into the Group's
// AP id: URLs pass through, handles go through WebFinger.
func (a *Admin) resolveCommunity(ctx context.Context, ref string) (string, error) {
	ref = strings.TrimSpace(ref)
	if strings.HasPrefix(ref, "https://") || strings.HasPrefix(ref, "http://") {
		return ref, nil
	}
	return a.client.ResolveHandle(ctx, ref)
}

func (a *Admin) writeResolveError(w http.ResponseWriter, ref string, err error) {
	switch {
	case errors.IsValidation(err):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.IsNotFound(err):
		http.Error(w, "community not found: "+ref, http.StatusNotFound)
	default:
		a.logger.Error("resolve community", "community", ref, "error", err)
		http.Error(w, "failed to resolve community", http.StatusBadGateway)
	}
}

// buildFollow constructs the signed Follow activity (delivery signing
// happens in the client; this is the payload Lemmy validates).
func (a *Admin) buildFollow(groupIRI string) (*ap.Object, error) {
	id, err := a.activityID("follow")
	if err != nil {
		return nil, err
	}
	return &ap.Object{
		Context: json.RawMessage(`"https://www.w3.org/ns/activitystreams"`),
		ID:      id,
		Type:    ap.TypeFollow,
		Actor:   &ap.Object{ID: a.service.ID},
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
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("ingest: mint activity id: %w", err)
	}
	return fmt.Sprintf("https://%s/activities/%s/%s", a.service.Hostname, kind, hex.EncodeToString(buf[:])), nil
}

// writeJSON writes a JSON response body.
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
