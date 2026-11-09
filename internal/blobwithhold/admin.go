package blobwithhold

import (
	"crypto/subtle"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"

	"tidepool/internal/errors"
)

// AdminOptions wires the blob withhold admin API.
type AdminOptions struct {
	// Token is the bearer token protecting /admin (config.AdminToken).
	Token string
	Store *Store
	// Logger receives one line per mutation. Nil uses slog.Default().
	Logger *slog.Logger
}

// Admin is the bearer-token operator API for blob withholds:
//
//	POST /admin/blobs/withhold      {"cid","reason_code","actor","moderation_action_id"?,"csam_case_id"?}
//	POST /admin/blobs/lift          {"cid","actor"}
//	POST /admin/blobs/delete-bytes  {"cid","actor","confirm":true}
//	GET  /admin/blobs?cid=          (inspect one CID)
//	GET  /admin/blobs/withholds     (active operator withholds)
//
// Every CID call answers with the CID's State. Log lines carry the action,
// canonical CID, reason code and outcome, never the actor label or external
// references the request carried.
type Admin struct {
	token  string
	store  *Store
	logger *slog.Logger
}

// NewAdmin validates options and builds the admin API.
func NewAdmin(opts AdminOptions) (*Admin, error) {
	switch {
	case opts.Token == "":
		return nil, errors.NewValidationError("token", "must not be empty")
	case opts.Store == nil:
		return nil, errors.NewValidationError("store", "must not be nil")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Admin{token: opts.Token, store: opts.Store, logger: logger}, nil
}

// Routes mounts the admin routes inside a bearer-guarded group, registering
// full /admin/... paths so they compose onto a router that already mounts
// other /admin routes.
func (a *Admin) Routes(r chi.Router) {
	r.Group(func(r chi.Router) {
		r.Use(a.requireBearer)
		r.Post("/admin/blobs/withhold", a.handleWithhold)
		r.Post("/admin/blobs/lift", a.handleLift)
		r.Post("/admin/blobs/delete-bytes", a.handleDeleteBytes)
		r.Get("/admin/blobs", a.handleInspect)
		r.Get("/admin/blobs/withholds", a.handleList)
	})
}

type withholdRequest struct {
	CID                string `json:"cid"`
	ReasonCode         string `json:"reason_code"`
	Actor              string `json:"actor"`
	ModerationActionID string `json:"moderation_action_id"`
	CSAMCaseID         string `json:"csam_case_id"`
}

type liftRequest struct {
	CID   string `json:"cid"`
	Actor string `json:"actor"`
}

type deleteBytesRequest struct {
	CID     string `json:"cid"`
	Actor   string `json:"actor"`
	Confirm bool   `json:"confirm"`
}

func (a *Admin) handleWithhold(w http.ResponseWriter, r *http.Request) {
	var body withholdRequest
	if !decodeBody(w, r, &body) {
		return
	}
	canonical, ok := canonicalOrBadRequest(w, body.CID)
	if !ok {
		return
	}
	if !ValidOperatorReason(body.ReasonCode) {
		writeError(w, http.StatusBadRequest, "reason_code must be one of illegal-content, csam, operator-discretion")
		return
	}
	if body.Actor == "" {
		writeError(w, http.StatusBadRequest, "actor must not be empty")
		return
	}
	state, err := a.store.Withhold(r.Context(), OperatorRequest{
		CID:                canonical,
		ReasonCode:         body.ReasonCode,
		Actor:              body.Actor,
		ModerationActionID: body.ModerationActionID,
		CSAMCaseID:         body.CSAMCaseID,
	})
	a.respond(w, "withhold", canonical, body.ReasonCode, state, err)
}

func (a *Admin) handleLift(w http.ResponseWriter, r *http.Request) {
	var body liftRequest
	if !decodeBody(w, r, &body) {
		return
	}
	canonical, ok := canonicalOrBadRequest(w, body.CID)
	if !ok {
		return
	}
	if body.Actor == "" {
		writeError(w, http.StatusBadRequest, "actor must not be empty")
		return
	}
	state, err := a.store.Lift(r.Context(), canonical, body.Actor)
	a.respond(w, "lift", canonical, "", state, err)
}

func (a *Admin) handleDeleteBytes(w http.ResponseWriter, r *http.Request) {
	var body deleteBytesRequest
	if !decodeBody(w, r, &body) {
		return
	}
	canonical, ok := canonicalOrBadRequest(w, body.CID)
	if !ok {
		return
	}
	if body.Actor == "" {
		writeError(w, http.StatusBadRequest, "actor must not be empty")
		return
	}
	if !body.Confirm {
		writeError(w, http.StatusBadRequest, "delete-bytes requires \"confirm\":true")
		return
	}
	state, err := a.store.DeleteBytes(r.Context(), canonical, body.Actor)
	a.respond(w, "delete-bytes", canonical, "", state, err)
}

func (a *Admin) handleInspect(w http.ResponseWriter, r *http.Request) {
	canonical, ok := canonicalOrBadRequest(w, r.URL.Query().Get("cid"))
	if !ok {
		return
	}
	state, err := a.store.Inspect(r.Context(), canonical)
	if err != nil {
		a.logger.Error("blob withhold admin failed", "action", "inspect", "cid", canonical, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, state)
}

func (a *Admin) handleList(w http.ResponseWriter, r *http.Request) {
	withholds, err := a.store.ListActiveOperator(r.Context())
	if err != nil {
		a.logger.Error("blob withhold admin failed", "action", "list", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"withholds": withholds})
}

// respond writes a mutation's outcome and logs it. The log carries the
// action, canonical CID, reason code and outcome only.
func (a *Admin) respond(w http.ResponseWriter, action, canonical, reasonCode string, state State, err error) {
	attrs := []any{"action", action, "cid", canonical}
	if reasonCode != "" {
		attrs = append(attrs, "reason_code", reasonCode)
	}
	switch {
	case err == nil:
		a.logger.Info("blob withhold admin", append(attrs, "outcome", "ok")...)
		writeJSON(w, http.StatusOK, state)
	case errors.IsNotFound(err):
		a.logger.Info("blob withhold admin", append(attrs, "outcome", "no active operator withhold")...)
		writeError(w, http.StatusNotFound, "no active operator withhold for "+canonical)
	case errors.IsValidation(err):
		a.logger.Info("blob withhold admin", append(attrs, "outcome", "invalid request")...)
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		a.logger.Error("blob withhold admin failed", append(attrs, "outcome", "error", "error", err)...)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

// decodeBody decodes a JSON request body, answering 400 on failure.
func decodeBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "body must be a JSON object")
		return false
	}
	return true
}

// canonicalOrBadRequest canonicalizes cidStr, answering 400 when it does
// not parse.
func canonicalOrBadRequest(w http.ResponseWriter, cidStr string) (string, bool) {
	canonical, err := Canonicalize(cidStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "cid must be a parseable CID")
		return "", false
	}
	return canonical, true
}

// requireBearer is the constant-time bearer guard the other /admin surfaces
// use.
func (a *Admin) requireBearer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const prefix = "Bearer "
		header := r.Header.Get("Authorization")
		if len(header) <= len(prefix) || header[:len(prefix)] != prefix ||
			subtle.ConstantTimeCompare([]byte(header[len(prefix):]), []byte(a.token)) != 1 {
			a.logger.Warn("admin request rejected: bad bearer token", "path", r.URL.Path)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
