package personas

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"tidepool/internal/ap"
	"tidepool/internal/apobject"
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
	// objectPathPrefix and activityPathPrefix are the outbound serving surface
	// (task 15): a peer re-fetches a native object or activity by the id the
	// bridge minted under it.
	objectPathPrefix   = "/ap/object/"
	activityPathPrefix = "/ap/activity/"

	// jrdContentType is WebFinger's media type (v1 precedent:
	// ingest/inbox.go's service-actor webfinger).
	jrdContentType = "application/jrd+json"
	// asNamespace is the ActivityStreams context every served document
	// names; securityNamespace is what makes publicKey meaningful to
	// Mastodon's and Lemmy's parsers.
	asNamespace       = "https://www.w3.org/ns/activitystreams"
	securityNamespace = "https://w3id.org/security/v1"
)

// ServeHTTP serves the user-origin surface:
//
//	GET  /.well-known/webfinger   discovery for a local part on the routed Host
//	GET  /ap/actor/{did}          the user's Person document
//	GET  /ap/actor/{did}/outbox   the (empty) outbox Lemmy requires
//	POST /ap/inbox                shared inbox, dispatched to the ingest inbox
//	GET  /                        the origin's instance (Application) actor
//	GET  /.well-known/nodeinfo    nodeinfo discovery
//	GET  /nodeinfo/2.0            nodeinfo 2.0 document
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
	case path == "/":
		if !isGET(w, r) {
			return
		}
		s.handleInstanceActor(w, r)
	case path == nodeInfoDiscoveryPath:
		if !isGET(w, r) {
			return
		}
		s.handleNodeInfoDiscovery(w, r)
	case path == nodeInfoSchemaPath:
		if !isGET(w, r) {
			return
		}
		s.handleNodeInfo(w, r)
	case path == inboxPath:
		// The method check comes FIRST, so a GET learns only that an inbox
		// is write-only — never whether one is wired up here.
		if !requireMethod(w, r, http.MethodPost) {
			return
		}
		s.handleInbox(w, r)
	case strings.HasPrefix(path, objectPathPrefix):
		if !isGET(w, r) {
			return
		}
		s.handleObject(w, r, strings.TrimPrefix(path, objectPathPrefix))
	case strings.HasPrefix(path, activityPathPrefix):
		if !isGET(w, r) {
			return
		}
		s.handleActivity(w, r, strings.TrimPrefix(path, activityPathPrefix))
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

// handleObject serves a native record as its AP object (Note/Page), rendered
// from the outbound_objects SNAPSHOT so it survives a PDS outage — Lemmy
// re-fetches a delivered object by id, and the snapshot is the byte-stable
// source the delivery itself was built from. A tombstoned row is 410 Gone (the
// record was deleted, and serving the stale body would resurrect it); an object
// we hold no state for is 404. Serving is bound to the origin's own host, like
// the actor document: the object id sits on this authority, and answering under
// another Host would publish a cross-authority claim.
func (s *Service) handleObject(w http.ResponseWriter, r *http.Request, rest string) {
	if normalizeHost(r.Host) != s.userHost {
		http.NotFound(w, r)
		return
	}
	// rest is did/collection/rkey — the at-uri's three parts.
	parts := strings.SplitN(rest, "/", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		http.NotFound(w, r)
		return
	}
	atURI := "at://" + parts[0] + "/" + parts[1] + "/" + parts[2]

	object, err := s.outboundObjects.GetByATURI(r.Context(), atURI)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	if object.IsTombstoned() {
		http.Error(w, "gone", http.StatusGone)
		return
	}

	doc, err := apobject.RenderObject(s.userOrigin, object.TranslatedSnapshot)
	if err != nil {
		s.logger.Error("failed to render served object from snapshot",
			"at_uri", atURI, "host", r.Host, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, ap.ContentTypeActivityJSON, doc)
}

// handleActivity serves a canonical activity payload VERBATIM. Activities are
// immutable (objects are current): an already-served activity must not change
// when the object it created is later edited, so the stored payload is written
// byte-for-byte. An activity id we never minted is 404.
func (s *Service) handleActivity(w http.ResponseWriter, r *http.Request, hash string) {
	if normalizeHost(r.Host) != s.userHost {
		http.NotFound(w, r)
		return
	}
	if hash == "" || strings.Contains(hash, "/") {
		http.NotFound(w, r)
		return
	}
	activity, err := s.outboundActivities.Get(r.Context(), s.userOrigin+activityPathPrefix+hash)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	w.Header().Set("Content-Type", ap.ContentTypeActivityJSON)
	_, _ = w.Write(activity.Payload)
}

func isGET(w http.ResponseWriter, r *http.Request) bool {
	return requireMethod(w, r, http.MethodGet)
}

func requireMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method != method {
		w.Header().Set("Allow", method)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return false
	}
	return true
}

// handleInbox hands the delivery to the ingest inbox VERBATIM — same request,
// same body, same headers. The user origin publishes a shared inbox but must
// not grow a second verify pipeline: signature verification, actor binding,
// dedupe, admission control, and the refusal taxonomy Lemmy reads to decide
// retry-vs-drop all stay in one implementation, and the inbox's own response
// is what the remote sees.
func (s *Service) handleInbox(w http.ResponseWriter, r *http.Request) {
	if s.inboxHandler == nil {
		// Advertising an inbox this origin cannot serve; 404 is the honest
		// answer.
		http.NotFound(w, r)
		return
	}
	s.inboxHandler.ServeHTTP(w, r)
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
		s.writeStoreError(w, r, err)
		return
	}
	if !s.servesActor(actor, r) {
		http.NotFound(w, r)
		return
	}
	// WITHDRAWN (task 17d's destructive tier): 410 Gone, and specifically not
	// 404. Gone is the statement "this existed and was withdrawn", which is what
	// lets a peer stop re-fetching and clean up its own copy; 404 reads as
	// "never heard of them", which several implementations treat as a transient
	// lookup failure and retry indefinitely.
	//
	// This is the ONE observable difference between the two opt-out tiers, which
	// is why it is a status code rather than a flag: a soft-disabled actor's
	// document keeps resolving, because every Note and Page already delivered
	// names it and revoking it would orphan the author reference on every
	// existing thread.
	//
	// THE BODY IS AN AS2 TOMBSTONE CARRYING THE PUBLIC KEY, and the key is there
	// for a race this tier cannot otherwise win. The tombstone commits when the
	// withdrawal is ENQUEUED; the worker POSTs it minutes later, with retries. In
	// that window a peer verifying the signature on the very Delete that
	// announces the withdrawal may re-dereference this actor, and a bare 410
	// leaves it with no key to verify with — so the erasure fails, retries and
	// poisons, and the user is never actually withdrawn anywhere.
	//
	// Serving the key inside the Tombstone costs nothing a withdrawal cares
	// about (the key was already public, and publicKey alone federates nothing)
	// and gives a peer that reads the body what it needs to accept the last
	// activity we will ever send as this actor.
	//
	// RESIDUAL, and it is real: a peer that reads only the STATUS still cannot
	// verify. The complete fix is to keep serving the actor document until every
	// delivery of the person-delete is terminal, which is a contract change —
	// the tier's own test pins 410 immediately — and belongs to whoever changes
	// that test.
	if actor.IsTombstoned() {
		s.writeActorTombstone(w, actor)
		return
	}

	origin, err := actorOrigin(actor)
	if err != nil {
		s.logger.Error("stored actor_id is not an absolute URL",
			"did", actor.DID, "actor_id", actor.ActorID, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
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
		s.writeStoreError(w, r, err)
		return
	}
	if !s.servesActor(actor, r) {
		http.NotFound(w, r)
		return
	}
	// The outbox answers the same way the actor does. An actor that is Gone with
	// a collection that is still 200 is a contradiction a peer has to resolve,
	// and it invites exactly the re-fetch loop the 410 exists to end.
	if actor.IsTombstoned() {
		s.writeActorTombstone(w, actor)
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

// writeActorTombstone answers 410 Gone with an AS2 Tombstone for a withdrawn
// identity. formerType and deleted are what tell a peer WHAT is gone and WHEN,
// so it can retire its own copy rather than treat the status as a fetch failure.
func (s *Service) writeActorTombstone(w http.ResponseWriter, actor *store.APActor) {
	w.Header().Set("Content-Type", ap.ContentTypeActivityJSON)
	w.WriteHeader(http.StatusGone)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"@context":   []any{asNamespace, securityNamespace},
		"id":         actor.ActorID,
		"type":       ap.TypeTombstone,
		"formerType": ap.TypePerson,
		"deleted":    actor.TombstonedAt.UTC().Format(time.RFC3339),
		// See handleActorDocument: this is here so a peer can still verify the
		// signature on the withdrawal activity itself, which may arrive after
		// this document started answering Gone.
		"publicKey": map[string]any{
			"id":           actor.ActorID + "#main-key",
			"owner":        actor.ActorID,
			"publicKeyPem": actor.PublicKeyPEM,
		},
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

	host := normalizeHost(r.Host)
	actor, err := s.lookupResource(r.Context(), resource, host)
	if err != nil {
		s.writeStoreError(w, r, err)
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
		if normalizeHost(acctHost) != host {
			return nil, errors.NewNotFoundError("ap_actor", resource)
		}
		return s.actors.GetByOriginLocalPart(ctx, host, strings.ToLower(local))
	}

	parsed, err := url.Parse(resource)
	if err != nil {
		return nil, errors.NewValidationError("resource", err.Error())
	}
	if normalizeHost(parsed.Host) != host {
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

// servesActor reports whether the routed Host is the actor's OWN origin. The
// DID is global but the actor is not: serving a vanity-origin actor's
// document under another Host would publish a document whose id sits on a
// different authority — the cross-authority claim ap.Client's key resolution
// refuses, and the mirror image of the binding webfinger already enforces.
func (s *Service) servesActor(actor *store.APActor, r *http.Request) bool {
	return actor.NormalizedOrigin == normalizeHost(r.Host)
}

// actorOrigin recovers the scheme+host an actor was minted under from its
// stored actor_id, so a vanity-origin actor advertises its own inbox rather
// than the configured one. It FAILS CLOSED: an actor_id that will not parse
// means the row is corrupt, and falling back to the configured origin would
// publish a document whose inbox and key belong to a different authority than
// its id — quietly, and cached by every peer that fetched it.
func actorOrigin(actor *store.APActor) (string, error) {
	parsed, err := url.Parse(actor.ActorID)
	if err != nil {
		return "", fmt.Errorf("parse actor_id %q: %w", actor.ActorID, err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("actor_id %q is not an absolute URL", actor.ActorID)
	}
	return parsed.Scheme + "://" + parsed.Host, nil
}

func writeJSON(w http.ResponseWriter, contentType string, doc any) {
	w.Header().Set("Content-Type", contentType)
	_ = json.NewEncoder(w).Encode(doc)
}

// writeStoreError maps a store/validation error onto the status a remote
// resolver will read correctly: a miss is cacheable as "no such account", a
// malformed request is the caller's fault, and anything else is ours — and
// the last case is LOGGED, because a 500 body says nothing and a database
// that has started failing under a peer's discovery traffic is otherwise
// invisible.
func (s *Service) writeStoreError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.IsNotFound(err):
		http.Error(w, "resource not found", http.StatusNotFound)
	case errors.IsValidation(err):
		http.Error(w, "malformed request", http.StatusBadRequest)
	default:
		s.logger.Error("user origin request failed",
			"path", r.URL.Path, "host", r.Host, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}
