// Package ingest wires the fediverse to the materializer: the shared AP
// inbox (HTTP-signature verified, deduplicated, durably queued), the
// FEP-1b12 Announce dispatcher, the community Follow lifecycle, outbox
// backfill, and consent enforcement. After this package the bridge is
// functionally complete end-to-end (minus vote aggregates, task 07).
package ingest

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// maxInboxBodyBytes caps inbound activity payloads. Lemmy activities are a
// few KB; anything approaching a megabyte is abuse.
const maxInboxBodyBytes = 1 << 20

// softwareName is what nodeinfo reports; Lemmy admins allowlist by this
// name.
const (
	softwareName    = "tidepool"
	softwareVersion = "0.1.0"
)

// ActorFetcher fetches an AP actor document by IRI — the slice of
// *ap.Client the inbox needs to confirm an actor tombstone (see
// tombstonedSelfDelete). The same-authority variant is required because that
// fetch is an authorization decision about the actor's own origin: a redirect
// off the origin must not be able to satisfy it (ap.Client pins every hop to
// the IRI's scheme+host).
type ActorFetcher interface {
	FetchActorSameAuthority(ctx context.Context, iri string) (*ap.Object, error)
}

// InboxOptions configures NewInbox. Verifier, Events, Service, Queue, and
// Fetcher are required.
type InboxOptions struct {
	// Verifier checks inbound HTTP signatures (task 02).
	Verifier *ap.Verifier
	// Events is the dedupe + queue store.
	Events store.InboxEvents
	// Queue is nudged after every enqueue.
	Queue *Queue
	// Service is the bridge's AP service actor (document served at /actor).
	Service *ap.ServiceActor
	// Fetcher independently confirms actor tombstones for the
	// deleted-actor Delete path (Lemmy signs account deletions with a key
	// whose actor document it already serves as 410 Gone).
	Fetcher ActorFetcher
	Logger  *slog.Logger
}

// Inbox is the HTTP face of ingestion: POST /inbox (+ the actor inbox
// alias), the service actor document, WebFinger for it, and nodeinfo.
type Inbox struct {
	verifier *ap.Verifier
	events   store.InboxEvents
	queue    *Queue
	service  *ap.ServiceActor
	fetcher  ActorFetcher
	logger   *slog.Logger
}

// NewInbox validates options and builds the Inbox.
func NewInbox(opts InboxOptions) (*Inbox, error) {
	if opts.Verifier == nil {
		return nil, errors.NewValidationError("verifier", "must not be nil")
	}
	if opts.Events == nil {
		return nil, errors.NewValidationError("events", "must not be nil")
	}
	if opts.Queue == nil {
		return nil, errors.NewValidationError("queue", "must not be nil")
	}
	if opts.Service == nil {
		return nil, errors.NewValidationError("service", "must not be nil")
	}
	if opts.Fetcher == nil {
		return nil, errors.NewValidationError("fetcher", "must not be nil")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Inbox{
		verifier: opts.Verifier,
		events:   opts.Events,
		queue:    opts.Queue,
		service:  opts.Service,
		fetcher:  opts.Fetcher,
		logger:   logger,
	}, nil
}

// Routes mounts the AP-facing surface on a chi router.
func (ib *Inbox) Routes(r chi.Router) {
	r.Post("/inbox", ib.handleInbox)
	// The service actor's own inbox: Lemmy delivers Accepts wherever the
	// Follow actor's document says; both spellings land here.
	r.Post("/actor/inbox", ib.handleInbox)
	r.Get(ap.ServiceActorPath, ib.handleActor)
	// The INSTANCE actor at the origin apex. Lemmy resolves every peer's
	// instance ("Site") actor here and delivers its send-to-all-instances
	// activities — account deletions above all — only to the inbox this
	// document advertises; without it those activities are silently
	// skipped ("no inboxes"). See ap.ServiceActor.InstanceDocumentJSON.
	r.Get("/", ib.handleInstanceActor)
	r.Get("/.well-known/webfinger", ib.handleWebFinger)
	r.Get("/.well-known/nodeinfo", ib.handleNodeInfoDiscovery)
	r.Get("/nodeinfo/2.0", ib.handleNodeInfo)
}

// handleInbox receives one AP delivery: verify the HTTP signature, bind the
// activity's actor to the signer, dedupe by activity id, enqueue for the
// worker pool, 202. Everything heavier happens async — remote instances
// time deliveries and treat slow inboxes as dead.
func (ib *Inbox) handleInbox(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxInboxBodyBytes))
	if err != nil {
		http.Error(w, "request body too large or unreadable", http.StatusRequestEntityTooLarge)
		return
	}

	actorID, err := ib.verifier.Verify(r.Context(), r, body)
	var activity *ap.Object
	if err != nil {
		selfDelete, tombstonedActor, deferConfirm := ib.tombstonedSelfDelete(r.Context(), body, err)
		switch {
		case tombstonedActor != "":
			// A self-Delete whose actor is verifiably GONE at its own
			// origin: accept it on that origin evidence (see
			// tombstonedSelfDelete) — there is no key left to verify.
			actorID = tombstonedActor
			activity = selfDelete
		case deferConfirm:
			// The tombstone confirmation fetch failed for a reason that says
			// nothing about the actor (network blip, timeout, origin 5xx):
			// defer so the sender's queue redelivers. A transient failure
			// must never permanently drop a legitimate account deletion —
			// there is no rediscovery path for a deleted actor.
			// (tombstonedSelfDelete already logged the fetch error.)
			http.Error(w, "tombstone confirmation unavailable", http.StatusServiceUnavailable)
			return
		case errors.IsValidation(err):
			// Bad/expired/missing signature: a definitive rejection.
			ib.logger.Warn("inbox delivery rejected: signature", "error", err)
			http.Error(w, "signature verification failed", http.StatusUnauthorized)
			return
		case errors.IsTombstoned(err):
			// The signing key's actor is gone but the payload is not an
			// acceptable self-Delete — or it was one and the confirmation
			// fetch definitively did NOT show a tombstone (actor alive,
			// not-an-actor document, 404): definitive — retrying can never
			// succeed, and a 5xx would head-of-line block the sender's
			// per-instance delivery queue forever.
			ib.logger.Warn("inbox delivery rejected: signer tombstoned", "error", err)
			http.Error(w, "signing actor is gone", http.StatusUnauthorized)
			return
		default:
			// Key resolution failed (network, remote down): tell the sender
			// to retry rather than swallowing the delivery.
			ib.logger.Warn("inbox delivery deferred: key resolution", "error", err)
			http.Error(w, "signature key unavailable", http.StatusServiceUnavailable)
			return
		}
	}

	// The tombstoned-self-delete path already parsed the body; every other
	// path parses it here.
	if activity == nil {
		activity, err = ap.ParseObject(body)
		if err != nil {
			http.Error(w, "payload is not an AP activity", http.StatusBadRequest)
			return
		}
	}
	if activity.ID == "" || activity.Type == "" {
		http.Error(w, "activity must carry id and type", http.StatusBadRequest)
		return
	}

	// Bind the activity's claimed actor to the verified signer. Exact
	// equality is the common case (Lemmy signs as the acting actor); same
	// authority tolerates instance-actor signing (Mastodon secure-mode
	// relays) without letting host A speak for host B. The QUEUED actor id
	// is the activity's actor — downstream authorization (followed
	// community, delete authority) keys off it.
	boundActor := actorID
	if claimed := refID(activity.Actor); claimed != "" {
		if !ap.SameAuthority(claimed, actorID) {
			ib.logger.Warn("inbox delivery rejected: actor/signer authority mismatch",
				"activity_actor", claimed, "signer", actorID)
			http.Error(w, "activity actor does not match signature", http.StatusForbidden)
			return
		}
		boundActor = claimed
	}

	isNew, err := ib.events.Enqueue(r.Context(), store.InboxEvent{
		ActivityID:  activity.ID,
		Type:        activity.Type,
		Payload:     body,
		ActorID:     boundActor,
		OrderingKey: orderingKeyFor(activity, boundActor),
	})
	if err != nil {
		ib.logger.Error("enqueue inbox event", "activity_id", activity.ID, "error", err)
		http.Error(w, "failed to record delivery", http.StatusInternalServerError)
		return
	}
	if !isNew {
		// Duplicate delivery: acknowledged, not re-queued.
		w.WriteHeader(http.StatusOK)
		return
	}
	ib.queue.Nudge()
	w.WriteHeader(http.StatusAccepted)
}

// tombstonedSelfDelete decides whether an unverifiable delivery must be
// accepted as an account deletion. On acceptance it returns the parsed
// activity and the deleted actor's IRI. Otherwise activity is nil and
// actorID is "": deferDelivery=false means the outcome is DEFINITIVE (fall
// through to the normal rejection paths, ultimately 401), deferDelivery=true
// means the tombstone confirmation was inconclusive and the caller must
// answer retryable (503) so the sender redelivers.
//
// Lemmy federates an account deletion as ONE Delete{Person} signed by the
// deleted user — whose actor document the origin, by the time the delivery
// lands, already serves as 410 Gone (observed live against Lemmy 0.19.19).
// Unless that user's key happens to sit in our cache from an earlier
// delivery, the signature can NEVER verify, and rejecting the Delete would
// mean Lemmy account deletions never tombstone bridged repos — a consent
// failure.
//
// The acceptance is NOT trust in the unverifiable signature. It requires,
// in order:
//   - verification failed specifically because a key fetch hit a tombstone
//     (IsTombstoned), for an otherwise well-formed delivery — Verify checks
//     date skew and the body digest before ever resolving the key;
//   - the payload is a bare self-referential Delete (actor == object) with
//     an id (the id gates the confirmation fetch: an unenqueueable activity
//     never earns an outbound request);
//   - an INDEPENDENT fetch of that actor's own IRI — SSRF-guarded, derived
//     from the payload, not from any attacker-controllable keyId, and with
//     every redirect hop pinned to the IRI's own authority
//     (FetchActorSameAuthority), so an open redirect on the origin cannot
//     bounce the fetch to an attacker host serving 410 — confirms the origin
//     serves it as Gone.
//
// That last fetch is the authority: the actor's own origin asserting "this
// actor no longer exists" is exactly the evidence the bridge would act on
// if it discovered the tombstone itself (and host-granularity origin trust
// is the same unit bare-Delete authorization already uses). A forger can
// therefore only "forge" a deletion that is already true at the origin.
// Two scope notes, both deliberate:
//   - Confirmation requires the origin to answer 410 or a Tombstone body.
//     Mastodon-style origins that serve 404 for deleted actors fall to the
//     definitive 401 — this path is Lemmy-scoped by design; the PieFed/Mbin
//     follow-up must revisit the 404 case.
//   - The signing key's host and the claimed actor's host may differ (a
//     tombstoned keyId on host A, the actor on host B): that cross-authority
//     shape is allowed by design, because the confirmation fetch of the
//     actor's OWN origin is the sole authority — where the unverifiable
//     signature came from carries no weight either way.
//
// The confirmation outcome is three-way, never boolean:
//   - origin says Gone (IsTombstoned) → accept;
//   - origin answered definitively otherwise — a live actor document (nil
//     error), a not-an-actor/off-authority-redirect result (IsValidation),
//     or 404/401/403 (IsNotFound) → definitive reject;
//   - anything else (transport error, timeout, 5xx, context cancellation)
//     → defer: the failure says nothing about the actor, and permanently
//     dropping a legitimate deletion is unrecoverable.
func (ib *Inbox) tombstonedSelfDelete(ctx context.Context, body []byte, verifyErr error) (activity *ap.Object, actorID string, deferDelivery bool) {
	if !errors.IsTombstoned(verifyErr) {
		return nil, "", false
	}
	parsed, err := ap.ParseObject(body)
	if err != nil || parsed.Type != ap.TypeDelete || parsed.ID == "" {
		return nil, "", false
	}
	actorIRI, objectIRI := refID(parsed.Actor), refID(parsed.Object)
	if actorIRI == "" || actorIRI != objectIRI {
		return nil, "", false
	}
	_, fetchErr := ib.fetcher.FetchActorSameAuthority(ctx, actorIRI)
	switch {
	case errors.IsTombstoned(fetchErr):
		ib.logger.Info("accepting unverifiable self-delete: actor is tombstoned at its origin",
			"actor", actorIRI, "verify_error", verifyErr)
		return parsed, actorIRI, false
	case fetchErr == nil, errors.IsValidation(fetchErr), errors.IsNotFound(fetchErr):
		// The origin answered and did NOT say Gone: the actor is
		// demonstrably alive (nil), the document is not an actor / the
		// fetch was redirected off-authority (IsValidation), or the origin
		// serves 404/401/403 (IsNotFound). Definitive — fall through to the
		// caller's 401.
		ib.logger.Warn("rejecting unverifiable self-delete: origin does not confirm tombstone",
			"actor", actorIRI, "fetch_error", fetchErr, "verify_error", verifyErr)
		return nil, "", false
	default:
		// Transport failure, timeout, origin 5xx, context cancellation:
		// inconclusive. Defer so the sender's queue redelivers.
		ib.logger.Warn("deferring unverifiable self-delete: tombstone confirmation inconclusive",
			"actor", actorIRI, "fetch_error", fetchErr, "verify_error", verifyErr)
		return nil, "", true
	}
}

// orderingKeyFor derives the per-community serialization key without any
// network round-trip: the community IRI when the activity (or its nested
// objects) is addressed to one, otherwise the bound actor — so one
// community's events apply in order, and unrelated actors never block each
// other.
func orderingKeyFor(activity *ap.Object, boundActor string) string {
	for probe := activity; probe != nil; probe = probe.Object {
		if iri := communityIRIFrom(probe); iri != "" {
			return iri
		}
	}
	return boundActor
}

// handleActor serves the bridge's Service actor document (Lemmy fetches
// it to validate our Follow signatures).
func (ib *Inbox) handleActor(w http.ResponseWriter, _ *http.Request) {
	doc, err := ib.service.DocumentJSON()
	if err != nil {
		ib.logger.Error("render service actor document", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", ap.ContentTypeActivityJSON)
	_, _ = w.Write(doc)
}

// handleInstanceActor serves the bridge's instance actor document at the
// origin apex (the id Lemmy derives for a peer's Site actor).
func (ib *Inbox) handleInstanceActor(w http.ResponseWriter, _ *http.Request) {
	doc, err := ib.service.InstanceDocumentJSON()
	if err != nil {
		ib.logger.Error("render instance actor document", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", ap.ContentTypeActivityJSON)
	_, _ = w.Write(doc)
}

// handleWebFinger answers for the service actor only (needed so Lemmy
// accepts our Follow: it resolves the follower's account). The bridged
// handle space is atproto-side and never surfaces here.
func (ib *Inbox) handleWebFinger(w http.ResponseWriter, r *http.Request) {
	resource := r.URL.Query().Get("resource")
	acct := fmt.Sprintf("acct:%s@%s", ib.service.Hostname, ib.service.Hostname)
	if resource != acct && resource != ib.service.ID {
		http.Error(w, "resource not found", http.StatusNotFound)
		return
	}
	response := ap.WebFingerResponse{
		Subject: acct,
		Aliases: []string{ib.service.ID},
		Links: []ap.WebFingerLink{{
			Rel:  "self",
			Type: ap.ContentTypeActivityJSON,
			Href: ib.service.ID,
		}},
	}
	w.Header().Set("Content-Type", "application/jrd+json")
	_ = json.NewEncoder(w).Encode(response)
}

// handleNodeInfoDiscovery serves the nodeinfo well-known discovery document.
func (ib *Inbox) handleNodeInfoDiscovery(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"links": []any{map[string]any{
			"rel":  "http://nodeinfo.diaspora.software/ns/schema/2.0",
			"href": ib.service.BaseURL() + "/nodeinfo/2.0",
		}},
	})
}

// handleNodeInfo serves a minimal nodeinfo 2.0 document. Lemmy reads
// software.name for its instance allow/block lists, so the bridge must
// identify itself here.
func (ib *Inbox) handleNodeInfo(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"version": "2.0",
		"software": map[string]any{
			"name":    softwareName,
			"version": softwareVersion,
		},
		"protocols":         []any{"activitypub"},
		"services":          map[string]any{"inbound": []any{}, "outbound": []any{}},
		"openRegistrations": false,
		"usage":             map[string]any{"users": map[string]any{}},
		"metadata":          map[string]any{},
	})
}

// requireBearer is the admin-auth middleware shared with follow.go: a
// constant-time bearer-token check against ADMIN_TOKEN.
func requireBearer(token string, logger *slog.Logger) func(http.Handler) http.Handler {
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

// decodeJSONBody decodes a small JSON request body into dst.
func decodeJSONBody(r *http.Request, dst any) error {
	defer func() { _, _ = io.Copy(io.Discard, r.Body) }()
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<16))
	if err := decoder.Decode(dst); err != nil {
		return fmt.Errorf("decode request body: %w", err)
	}
	return nil
}
