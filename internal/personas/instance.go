package personas

import (
	"net/http"
	"time"

	"tidepool/internal/ap"
)

const (
	nodeInfoDiscoveryPath = "/.well-known/nodeinfo"
	nodeInfoSchemaPath    = "/nodeinfo/2.0"
	nodeInfoSchemaRel     = "http://nodeinfo.diaspora.software/ns/schema/2.0"
	// instanceOutboxPath is advertised, not served — the same shape the
	// service surface's instance actor publishes. Lemmy's Instance parser
	// REQUIRES the field but never dereferences it.
	instanceOutboxPath = "/ap/outbox"
)

// handleInstanceActor serves the user origin's instance ("Site") actor at the
// apex. This is load-bearing for inbound federation, not decoration: Lemmy
// resolves every federating peer's instance actor by fetching the peer's
// origin apex, and delivers its send-to-all-instances activities —
// Delete{Person} above all — ONLY to the inbox on that row. An origin with no
// instance actor silently never receives them.
//
// The document republishes the BRIDGE's key: one identity presenting two
// origins, which is also what lets a peer verify signatures from either. Type
// must be exactly "Application" — Lemmy's Instance enum accepts nothing else,
// the opposite of the /actor rule where its Person enum rejects Application.
//
// Content negotiation is deliberately absent: Caddy owns that in production
// (task 18), and a peer that sends ld+json, activity+json, or no Accept at all
// must still get the actor.
func (s *Service) handleInstanceActor(w http.ResponseWriter, r *http.Request) {
	if s.serviceActor == nil {
		// No bridge identity configured means no key to publish. Lemmy
		// tolerates a missing instance actor, and the mint-only wiring has
		// no use for one.
		http.NotFound(w, r)
		return
	}

	publicPEM, err := ap.EncodePublicKeyPEM(&s.serviceActor.Key.PublicKey)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// The apex id carries a trailing slash: it is exactly the URL Lemmy
	// derives when it clears the path of an object id from this origin.
	id := s.userOrigin + "/"
	doc := map[string]any{
		"@context":          []any{asNamespace, securityNamespace},
		"type":              ap.TypeApplication,
		"id":                id,
		"name":              s.userHost,
		"preferredUsername": s.userHost,
		// The inbox is THIS origin's shared inbox, not the bridge's: an
		// instance delivery must land where this origin actually listens.
		"inbox":  s.userOrigin + inboxPath,
		"outbox": s.userOrigin + instanceOutboxPath,
		"publicKey": map[string]any{
			"id":           id + "#main-key",
			"owner":        id,
			"publicKeyPem": string(publicPEM),
		},
	}
	// published is when the bridge's key was provisioned. An unknown time is
	// omitted rather than invented: a date the bridge does not know is a lie
	// every peer caches.
	if !s.serviceActor.CreatedAt.IsZero() {
		doc["published"] = s.serviceActor.CreatedAt.UTC().Format(time.RFC3339)
	}

	writeJSON(w, ap.ContentTypeActivityJSON, doc)
}

// handleNodeInfoDiscovery points at THIS origin's nodeinfo document. Lemmy's
// scheduled task reads it to learn what software a peer runs; it never gates
// federation, but an origin that answers nothing here shows up as an unknown
// implementation in every peer's instance list.
func (s *Service) handleNodeInfoDiscovery(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, "application/json", map[string]any{
		"links": []any{map[string]any{
			"rel":  nodeInfoSchemaRel,
			"href": s.userOrigin + nodeInfoSchemaPath,
		}},
	})
}

// handleNodeInfo serves the nodeinfo 2.0 document, mirroring the shape the
// service surface publishes.
func (s *Service) handleNodeInfo(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, "application/json", map[string]any{
		"version": "2.0",
		"software": map[string]any{
			"name":    ap.SoftwareName,
			"version": ap.SoftwareVersion,
		},
		"protocols": []any{"activitypub"},
		"services":  map[string]any{"inbound": []any{}, "outbound": []any{}},
		// Accounts here are minted from atproto identities, never registered.
		"openRegistrations": false,
		"usage":             map[string]any{"users": map[string]any{}},
		"metadata":          map[string]any{},
	})
}
