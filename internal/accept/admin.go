package accept

import (
	"crypto/subtle"
	"encoding/json"
	stderrors "errors"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"

	"tidepool/internal/errors"
)

// AdminOptions wires the admissions admin surface.
type AdminOptions struct {
	// Token is the bearer token protecting /admin (config.AdminToken), matching
	// the ingest.Admin pattern.
	Token string
	// Admissions is the decision ledger the list endpoint reads.
	Admissions *Admissions
	// Engine serves the force re-admit (Readmit).
	Engine *Engine
	// Logger receives rejection/authz warnings. Nil uses slog.Default().
	Logger *slog.Logger
}

// Admin is the operator API for the acceptance engine's admission ledger:
//
//	GET  /admin/admissions          ?status=&community=  (list decisions + reasons)
//	POST /admin/admissions/readmit  {"post":"at://..."}  (force re-admit one post)
//
// Both endpoints require "Authorization: Bearer $ADMIN_TOKEN". post.getStatus
// reads a post's admission state from the firehose-visible acceptance/removal
// records; THIS surface is the bridge operator's own window on the WHY of every
// rejection, which those records cannot carry.
type Admin struct {
	token      string
	admissions *Admissions
	engine     *Engine
	logger     *slog.Logger
}

// NewAdmin validates options and builds the admin API.
func NewAdmin(opts AdminOptions) (*Admin, error) {
	switch {
	case opts.Token == "":
		return nil, errors.NewValidationError("token", "must not be empty")
	case opts.Admissions == nil:
		return nil, errors.NewValidationError("admissions", "must not be nil")
	case opts.Engine == nil:
		return nil, errors.NewValidationError("engine", "must not be nil")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Admin{token: opts.Token, admissions: opts.Admissions, engine: opts.Engine, logger: logger}, nil
}

// Routes mounts the admissions admin API, bearer-protected like the rest of
// /admin. It registers its full /admin/... paths inside a middleware GROUP
// rather than a Route("/admin") subrouter, so it composes onto a router that
// already mounts a /admin subtree (ingest.Admin) instead of colliding with it.
func (a *Admin) Routes(r chi.Router) {
	r.Group(func(r chi.Router) {
		r.Use(adminBearer(a.token, a.logger))
		r.Get("/admin/admissions", a.handleList)
		r.Post("/admin/admissions/readmit", a.handleReadmit)
	})
}

// adminListItem is one admission on the wire: the operator's triage view of a
// decision. The evaluated_snapshot is deliberately omitted (it can be large and
// is only readmit's input).
type adminListItem struct {
	Post         string `json:"post"`
	Community    string `json:"community"`
	Status       string `json:"status"`
	DecisionCode string `json:"decisionCode"`
	EvaluatedCID string `json:"evaluatedCid"`
}

// handleList lists admissions with their status, decision_code and evaluated_cid,
// filterable by ?status= and/or ?community=.
func (a *Admin) handleList(w http.ResponseWriter, r *http.Request) {
	admissions, err := a.admissions.List(r.Context(), AdmissionFilter{
		Status:    r.URL.Query().Get("status"),
		Community: r.URL.Query().Get("community"),
	})
	if err != nil {
		a.logger.Error("admin list admissions failed", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}
	items := make([]adminListItem, 0, len(admissions))
	for _, adm := range admissions {
		items = append(items, adminListItem{
			Post:         adm.PostURI,
			Community:    adm.CommunityDID,
			Status:       adm.Status,
			DecisionCode: adm.DecisionCode,
			EvaluatedCID: adm.EvaluatedCID,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"admissions": items})
}

// readmitRequest is the POST body naming the post to force re-admit.
type readmitRequest struct {
	Post string `json:"post"`
}

// readmitResult is the wire outcome of a force re-admit.
type readmitResult struct {
	Post         string `json:"post"`
	Status       string `json:"status"`
	DecisionCode string `json:"decisionCode"`
	Enqueued     bool   `json:"enqueued"`
}

// handleReadmit force re-runs admission for one post (Engine.Readmit). A post
// that passes now is accepted + enqueued; one that still fails returns the
// current rejection (a reported outcome, HTTP 200, not an error). An unknown post
// is 404; a post whose stored snapshot did not survive is 422 (unrecoverable
// without a task-18 getRecord fetch).
func (a *Admin) handleReadmit(w http.ResponseWriter, r *http.Request) {
	var body readmitRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Post == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body must be {\"post\":\"at://...\"}"})
		return
	}
	result, err := a.engine.Readmit(r.Context(), body.Post)
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, readmitResult{
			Post:         result.PostURI,
			Status:       result.Status,
			DecisionCode: result.DecisionCode,
			Enqueued:     result.Enqueued,
		})
	case errors.IsNotFound(err):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no admission for " + body.Post})
	case stderrors.Is(err, ErrUnrecoverableReadmit):
		// Surfaced, never a silent no-op: the record body the post was decided
		// against did not survive (legacy row, pre-migration-022), so there is
		// nothing to re-run admission from until a getRecord fetch lands (task 18).
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{
			"error": "readmit unrecoverable: no stored record snapshot", "post": body.Post})
	default:
		a.logger.Error("admin readmit failed", "post", body.Post, "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
	}
}

// adminBearer is the constant-time bearer guard, mirroring ingest.requireBearer
// (kept local so this surface stays self-contained).
func adminBearer(token string, logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			const prefix = "Bearer "
			header := r.Header.Get("Authorization")
			if len(header) <= len(prefix) || header[:len(prefix)] != prefix ||
				subtle.ConstantTimeCompare([]byte(header[len(prefix):]), []byte(token)) != 1 {
				logger.Warn("admin request rejected: bad bearer token", "path", r.URL.Path)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// writeJSON is the shared JSON responder GREEN's handlers use.
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
