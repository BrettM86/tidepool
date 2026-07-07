package identity

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"strings"

	comatproto "github.com/bluesky-social/indigo/api/atproto"

	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// Handle resolution for the bridge's handle space.
//
// Every bridged handle is a subdomain of BRIDGE_HOSTNAME
// (alice.lemmy-world.<bridge>), so the operator must run wildcard DNS
// pointing *.<BRIDGE_HOSTNAME> (which in DNS matches multiple label levels)
// at the bridge, with TLS termination covering those names. With that in
// place both atproto resolution paths work against this package:
//
//   - com.atproto.identity.resolveHandle XRPC (ResolveHandleHandler),
//   - the HTTPS well-known method: GET https://<handle>/.well-known/atproto-did
//     lands here because of the wildcard DNS, and WellKnownDIDHandler answers
//     from the Host header.
//
// The DNS requirement is documented in the README.

// Resolver answers "which DID owns this bridged handle". Implemented by
// StoreResolver; an interface so the HTTP handlers are testable without
// postgres.
type Resolver interface {
	// ResolveHandle returns the DID for a handle. Unknown handles — and
	// handles of tombstoned actors, whose bridged identity is frozen — are
	// errors satisfying errors.IsNotFound.
	ResolveHandle(ctx context.Context, handle string) (string, error)
}

// StoreResolver resolves bridged handles from the bridged_actors table,
// plus the bridge's own service DID for the bare bridge hostname.
type StoreResolver struct {
	actors store.BridgedActors
	// bridgeHostname (lowercased) resolves to serviceDID when non-empty:
	// the bridge's own service identity (config.BridgeServiceDID).
	bridgeHostname string
	serviceDID     string
}

// NewStoreResolver builds the store-backed resolver. serviceDID may be
// empty (the bridge's own handle then does not resolve).
func NewStoreResolver(actors store.BridgedActors, bridgeHostname, serviceDID string) *StoreResolver {
	return &StoreResolver{
		actors:         actors,
		bridgeHostname: strings.ToLower(bridgeHostname),
		serviceDID:     serviceDID,
	}
}

func (r *StoreResolver) ResolveHandle(ctx context.Context, handle string) (string, error) {
	handle = strings.ToLower(strings.TrimSuffix(handle, "."))
	if handle == "" {
		return "", errors.NewValidationError("handle", "must not be empty")
	}
	if r.serviceDID != "" && handle == r.bridgeHostname {
		return r.serviceDID, nil
	}
	actor, err := r.actors.GetByHandle(ctx, handle)
	if err != nil {
		return "", err
	}
	// A tombstoned actor's identity is frozen (task-01 semantics): its
	// handle stops resolving rather than advertising a dead repo.
	if actor.ConsentState == store.ConsentStateDeleted {
		return "", errors.NewNotFoundError("handle", handle)
	}
	return actor.DID, nil
}

// ResolveHandleHandler implements the com.atproto.identity.resolveHandle
// XRPC query: GET /xrpc/com.atproto.identity.resolveHandle?handle=<handle>
// → {"did": "..."}. Unresolvable handles return the standard XRPC error
// shape with status 400, matching the reference implementation.
func ResolveHandleHandler(resolver Resolver, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		handle := strings.TrimPrefix(r.URL.Query().Get("handle"), "@")
		if handle == "" {
			writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "missing required parameter: handle")
			return
		}
		did, err := resolver.ResolveHandle(r.Context(), handle)
		switch {
		case err == nil:
			writeJSON(w, http.StatusOK, comatproto.IdentityResolveHandle_Output{Did: did})
		case errors.IsNotFound(err):
			writeXRPCError(w, http.StatusBadRequest, "HandleNotFound", "Unable to resolve handle")
		case errors.IsValidation(err):
			writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "invalid handle")
		default:
			logger.Error("resolveHandle failed", "handle", handle, "error", err)
			writeXRPCError(w, http.StatusInternalServerError, "InternalServerError", "internal error")
		}
	}
}

// WellKnownDIDHandler serves GET /.well-known/atproto-did, treating the
// request's Host as the handle being verified (that is how the HTTPS
// resolution method addresses a handle; wildcard DNS routes every bridged
// subdomain here). Responds text/plain with the bare DID.
func WellKnownDIDHandler(resolver Resolver, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		did, err := resolver.ResolveHandle(r.Context(), host)
		switch {
		case err == nil:
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(did))
		case errors.IsNotFound(err) || errors.IsValidation(err):
			http.Error(w, "no atproto DID for this host", http.StatusNotFound)
		default:
			logger.Error("well-known atproto-did lookup failed", "host", host, "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeXRPCError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]string{"error": code, "message": message})
}
