package personas

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"time"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/store"
)

const (
	webfingerPath = "/.well-known/webfinger"
	// actorPathPrefix and inboxPath are the origin's URL shape. Serving
	// derives every absolute URL from the stored actor_id rather than from
	// these plus config (decision 10), so a vanity-origin actor advertises
	// its OWN origin, not AP_USER_ORIGIN.
	actorPathPrefix = "/ap/actor/"
	outboxSuffix    = "/outbox"
	inboxPath       = "/ap/inbox"

	// jrdContentType is WebFinger's media type (v1 precedent:
	// ingest/inbox.go's service-actor webfinger).
	jrdContentType = "application/jrd+json"
	// asNamespace is the ActivityStreams context every served document
	// names; securityNamespace is what makes publicKey meaningful to
	// Mastodon's and Lemmy's parsers.
	asNamespace       = "https://www.w3.org/ns/activitystreams"
	securityNamespace = "https://w3id.org/security/v1"
)

// ServeHTTP serves the user-origin surface: /.well-known/webfinger,
// /ap/actor/{did}, and /ap/actor/{did}/outbox.
//
// Routing reads r.URL.Path, which net/http has already percent-decoded. A
// DID's colons are legal unescaped, so both "did:plc:x" and "did%3Aplc%3Ax"
// arrive from real implementations and must reach the same actor; matching on
// the raw path would answer one spelling and 404 the other.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	switch {
	case path == webfingerPath:
		if !isGET(w, r) {
			return
		}
		s.handleWebFinger(w, r)
	case strings.HasPrefix(path, actorPathPrefix):
		rest := strings.TrimPrefix(path, actorPathPrefix)
		if !isGET(w, r) {
			return
		}
		if did, isOutbox := strings.CutSuffix(rest, outboxSuffix); isOutbox {
			s.handleOutbox(w, r, did)
			return
		}
		if rest == "" || strings.Contains(rest, "/") {
			http.NotFound(w, r)
			return
		}
		s.handleActorDocument(w, r, rest)
	default:
		http.NotFound(w, r)
	}
}

func isGET(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return false
	}
	return true
}

// handleActorDocument serves the Person document. A DISABLED actor still
// serves it: disabling removes an actor from discovery, not from the network,
// and already-federated references to it must not become dangling. Paused
// actors are invisible here too — delivery_paused is about outbound traffic
// and has no read-side meaning at all.
//
// Nothing on this path unseals a key: the published PEM is a stored column,
// so serving an actor document never touches the KEK.
func (s *Service) handleActorDocument(w http.ResponseWriter, r *http.Request, did string) {
	actor, err := s.actors.GetByDID(r.Context(), did)
	if err != nil {
		writeStoreError(w, err)
		return
	}

	origin := actorOrigin(actor, s.userOrigin)
	inbox := origin + inboxPath
	// The display name falls back to the local part: Lemmy renders `name`,
	// and an empty one shows as a blank user until task 14's profile sync
	// fills the cache.
	name := actor.DisplayName
	if name == "" {
		name = actor.LocalPart
	}

	doc := map[string]any{
		"@context":          []any{asNamespace, securityNamespace},
		"id":                actor.ActorID,
		"type":              ap.TypePerson,
		"preferredUsername": actor.LocalPart,
		"name":              name,
		"inbox":             inbox,
		// One shared inbox serves every actor on the origin.
		"endpoints": map[string]any{"sharedInbox": inbox},
		// outbox is REQUIRED for Lemmy's Person deserialization: omitting
		// it rejects the whole actor, not just the collection.
		"outbox": actor.ActorID + outboxSuffix,
		"publicKey": map[string]any{
			"id":           actor.ActorID + "#main-key",
			"owner":        actor.ActorID,
			"publicKeyPem": actor.PublicKeyPEM,
		},
		"published": actor.CreatedAt.UTC().Format(time.RFC3339),
	}
	// Empty profile fields are OMITTED rather than served blank: a present
	// but empty summary or icon is a claim about the user that the cache
	// cannot yet support.
	if actor.Summary != "" {
		doc["summary"] = actor.Summary
	}
	if actor.AvatarURL != "" {
		// An Image object, not a bare URL string: Lemmy's parser rejects
		// the string form.
		doc["icon"] = map[string]any{"type": ap.TypeImage, "url": actor.AvatarURL}
	}

	writeJSON(w, ap.ContentTypeActivityJSON, doc)
}

// handleOutbox serves the (empty) collection Lemmy dereferences after parsing
// the actor. Task 13 mints identities only, so no content flows yet — but a
// MISSING outbox breaks the actor, so it is served empty rather than not at
// all.
func (s *Service) handleOutbox(w http.ResponseWriter, r *http.Request, did string) {
	actor, err := s.actors.GetByDID(r.Context(), did)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, ap.ContentTypeActivityJSON, map[string]any{
		"@context":     asNamespace,
		"id":           actor.ActorID + outboxSuffix,
		"type":         ap.TypeOrderedCollection,
		"totalItems":   0,
		"orderedItems": []any{},
	})
}

// handleWebFinger answers discovery for the local parts hosted on the ROUTED
// Host. The lookup is (host, local part), so this origin can never answer for
// an account it does not host, and two origins hosting the same local part
// stay two different people.
func (s *Service) handleWebFinger(w http.ResponseWriter, r *http.Request) {
	resource := strings.TrimSpace(r.URL.Query().Get("resource"))
	if resource == "" {
		// Malformed, not a miss: remote resolvers cache 404s as "no such
		// account" but read 400s as our bug, so the distinction has to be
		// right at the protocol layer.
		http.Error(w, "missing resource parameter", http.StatusBadRequest)
		return
	}

	host := requestHost(r)
	actor, err := s.lookupResource(r.Context(), resource, host)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	if !actor.Enabled {
		// Discovery is exactly what disabling removes.
		http.Error(w, "resource not found", http.StatusNotFound)
		return
	}

	// The subject echoes the canonical acct form whichever spelling was
	// asked for, so both resolutions answer identically.
	writeJSON(w, jrdContentType, ap.WebFingerResponse{
		Subject: "acct:" + actor.LocalPart + "@" + actor.NormalizedOrigin,
		Aliases: []string{actor.ActorID},
		// Two rel=self links, activity+json FIRST: resolvers that take the
		// first match must land on the AP document, and Lemmy accepts
		// either type.
		Links: []ap.WebFingerLink{
			{Rel: "self", Type: ap.ContentTypeActivityJSON, Href: actor.ActorID},
			{Rel: "self", Type: ap.ContentTypeLDJSON, Href: actor.ActorID},
		},
	})
}

// lookupResource resolves a WebFinger resource to an actor on host. Both
// spellings are accepted — acct:local@host and the actor URL itself (v1
// precedent: ingest/inbox.go's service-actor webfinger accepts both) — and
// each is bound to the routed Host, which is what stops this origin from
// answering for accounts hosted elsewhere.
func (s *Service) lookupResource(ctx context.Context, resource, host string) (*store.APActor, error) {
	if !strings.Contains(resource, "://") {
		local, acctHost, err := ap.ParseHandle(resource)
		if err != nil {
			return nil, err
		}
		if canonicalHost(acctHost) != host {
			return nil, errors.NewNotFoundError("ap_actor", resource)
		}
		return s.actors.GetByOriginLocalPart(ctx, host, strings.ToLower(local))
	}

	parsed, err := url.Parse(resource)
	if err != nil {
		return nil, errors.NewValidationError("resource", err.Error())
	}
	if canonicalHost(parsed.Host) != host {
		return nil, errors.NewNotFoundError("ap_actor", resource)
	}
	did, ok := strings.CutPrefix(parsed.Path, actorPathPrefix)
	if !ok || did == "" {
		return nil, errors.NewNotFoundError("ap_actor", resource)
	}
	actor, err := s.actors.GetByDID(ctx, did)
	if err != nil {
		return nil, err
	}
	// The DID is global but this answer must not be: an actor minted on
	// another origin does not resolve here.
	if actor.NormalizedOrigin != host {
		return nil, errors.NewNotFoundError("ap_actor", resource)
	}
	return actor, nil
}

// requestHost is the routed authority in the form ap_actors.normalized_origin
// stores it. The scheme's DEFAULT port is noise and is stripped; any other
// port is part of the authority and stays — dev runs the origin on
// localhost:8091, and coves.social:8443 is a different origin from
// coves.social, not a sloppy spelling of it.
func requestHost(r *http.Request) string {
	host := strings.ToLower(strings.TrimSpace(r.Host))
	defaultPort := ":80"
	if r.TLS != nil {
		defaultPort = ":443"
	}
	return canonicalHost(strings.TrimSuffix(host, defaultPort))
}

// canonicalHost lowercases a host and drops the trailing dot of a
// fully-qualified name, which names the same host.
func canonicalHost(host string) string {
	return strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
}

// actorOrigin recovers the scheme+host an actor was minted under from its
// stored actor_id, so a vanity-origin actor advertises its own inbox rather
// than the configured one. The configured origin is only the fallback for an
// actor_id that cannot be parsed.
func actorOrigin(actor *store.APActor, fallback string) string {
	parsed, err := url.Parse(actor.ActorID)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return fallback
	}
	return parsed.Scheme + "://" + parsed.Host
}

func writeJSON(w http.ResponseWriter, contentType string, doc any) {
	w.Header().Set("Content-Type", contentType)
	_ = json.NewEncoder(w).Encode(doc)
}

// writeStoreError maps a store/validation error onto the status a remote
// resolver will read correctly: a miss is cacheable as "no such account", a
// malformed request is the caller's fault, and anything else is ours.
func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.IsNotFound(err):
		http.Error(w, "resource not found", http.StatusNotFound)
	case errors.IsValidation(err):
		http.Error(w, "malformed request", http.StatusBadRequest)
	default:
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}
