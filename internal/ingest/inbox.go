// Package ingest wires the fediverse to the materializer: the shared AP
// inbox (HTTP-signature verified, deduplicated, durably queued), the
// FEP-1b12 Announce dispatcher, the community Follow lifecycle, outbox
// backfill, and consent enforcement. After this package the bridge is
// functionally complete end-to-end (minus vote aggregates, task 07).
package ingest

import (
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

// InboxOptions configures NewInbox. Verifier, Events, Service, and Queue
// are required.
type InboxOptions struct {
	// Verifier checks inbound HTTP signatures (task 02).
	Verifier *ap.Verifier
	// Events is the dedupe + queue store.
	Events store.InboxEvents
	// Queue is nudged after every enqueue.
	Queue *Queue
	// Service is the bridge's AP service actor (document served at /actor).
	Service *ap.ServiceActor
	Logger  *slog.Logger
}

// Inbox is the HTTP face of ingestion: POST /inbox (+ the actor inbox
// alias), the service actor document, WebFinger for it, and nodeinfo.
type Inbox struct {
	verifier *ap.Verifier
	events   store.InboxEvents
	queue    *Queue
	service  *ap.ServiceActor
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
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Inbox{
		verifier: opts.Verifier,
		events:   opts.Events,
		queue:    opts.Queue,
		service:  opts.Service,
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
	if err != nil {
		if errors.IsValidation(err) {
			// Bad/expired/missing signature: a definitive rejection.
			ib.logger.Warn("inbox delivery rejected: signature", "error", err)
			http.Error(w, "signature verification failed", http.StatusUnauthorized)
			return
		}
		// Key resolution failed (network, remote down): tell the sender to
		// retry rather than swallowing the delivery.
		ib.logger.Warn("inbox delivery deferred: key resolution", "error", err)
		http.Error(w, "signature key unavailable", http.StatusServiceUnavailable)
		return
	}

	activity, err := ap.ParseObject(body)
	if err != nil {
		http.Error(w, "payload is not an AP activity", http.StatusBadRequest)
		return
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
