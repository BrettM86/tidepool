package personas

import (
	"database/sql"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/identity"
)

// serviceActorCreatedAt is the provisioning time of the bridge's key — the
// instance actor's published timestamp.
var serviceActorCreatedAt = time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

// newInstanceService builds a Service that knows the bridge's own identity,
// which is what lets the user origin publish an instance actor.
func newInstanceService(t *testing.T, database *sql.DB) (*Service, *ap.ServiceActor) {
	t.Helper()
	key, err := ap.GenerateRSAKey()
	require.NoError(t, err)
	bridge := &ap.ServiceActor{
		ID:        "https://" + serviceHost + ap.ServiceActorPath,
		Hostname:  serviceHost,
		Scheme:    "https",
		Key:       key,
		CreatedAt: serviceActorCreatedAt,
	}
	custodian, err := identity.NewCustodian(testKEK)
	require.NoError(t, err)
	svc, err := New(Options{
		DB:           database,
		Custodian:    custodian,
		UserOrigin:   userOrigin,
		ServiceActor: bridge,
	})
	require.NoError(t, err)
	require.NotNil(t, svc)
	return svc, bridge
}

// TestServeInstanceActor: Lemmy resolves a peer's instance ("Site") actor by
// fetching the origin apex, and delivers send-to-all-instances activities —
// Delete{Person} above all — only to the inbox on that row. A user origin
// with no instance actor silently never receives them.
func TestServeInstanceActor(t *testing.T) {
	database := personasTestDB(t)
	svc, bridge := newInstanceService(t, database)

	instanceID := userOrigin + "/"

	// Accept is not negotiated here — Caddy owns content negotiation in
	// production (task 18), and a peer sending ld+json or nothing at all
	// must still get the actor.
	for _, accept := range []string{
		ap.ContentTypeActivityJSON,
		ap.ContentTypeLDJSON,
		"",
	} {
		header := http.Header{}
		if accept != "" {
			header.Set("Accept", accept)
		}
		rec := serveOnUserOrigin(svc, http.MethodGet, "/", header)
		require.Equal(t, http.StatusOK, rec.Code,
			"GET / with Accept %q must serve the instance actor; body=%s", accept, rec.Body.String())
		assert.Equal(t, ap.ContentTypeActivityJSON, rec.Header().Get("Content-Type"))

		doc := decodeJSON(t, rec)
		assert.Equal(t, "Application", doc["type"],
			"Lemmy's Instance enum accepts Application and nothing else")
		assert.Equal(t, instanceID, doc["id"],
			"the id is the origin apex WITH a trailing slash — the URL Lemmy derives")
		assert.True(t, contextIncludes(doc["@context"], "https://www.w3.org/ns/activitystreams"),
			"@context must name the ActivityStreams namespace, got %v", doc["@context"])
		assert.NotEmpty(t, doc["name"], "Lemmy requires name on the Instance document")
		assert.Equal(t, userHost, doc["preferredUsername"],
			"the instance actor's username is its host")
		assert.Equal(t, userOrigin+"/ap/inbox", doc["inbox"],
			"instance deliveries must land on the inbox this origin actually serves")

		outbox, ok := doc["outbox"].(string)
		require.True(t, ok, "outbox is required, got %v", doc["outbox"])
		assert.True(t, strings.HasPrefix(outbox, userOrigin+"/"),
			"the outbox must live on this origin, got %q", outbox)

		publicKey, ok := doc["publicKey"].(map[string]any)
		require.True(t, ok, "publicKey block is required, got %v", doc["publicKey"])
		assert.Equal(t, instanceID+"#main-key", publicKey["id"])
		assert.Equal(t, instanceID, publicKey["owner"])
		pem, ok := publicKey["publicKeyPem"].(string)
		require.True(t, ok)
		parsed, err := ap.ParsePublicKeyPEM([]byte(pem))
		require.NoError(t, err)
		assert.True(t, bridge.Key.PublicKey.Equal(parsed),
			"the instance actor republishes the bridge's own key: one identity, two origins")

		published, ok := doc["published"].(string)
		require.True(t, ok, "Lemmy requires published, got %v", doc["published"])
		publishedAt, err := time.Parse(time.RFC3339, published)
		require.NoError(t, err, "published must be RFC3339, got %q", published)
		assert.True(t, publishedAt.Equal(serviceActorCreatedAt),
			"published is when the bridge's key was provisioned, got %s", published)
	}
}

// TestServeInstanceActor_Unconfigured: a Service built without the bridge's
// identity has no key to publish, so the apex is simply absent. Lemmy
// tolerates a missing instance actor ("probably not a lemmy instance"), and
// New must not require one — the mint-only wiring has no use for it.
func TestServeInstanceActor_Unconfigured(t *testing.T) {
	database := personasTestDB(t)
	svc, _ := newTestService(t, database)

	rec := serveOnUserOrigin(svc, http.MethodGet, "/",
		http.Header{"Accept": []string{ap.ContentTypeActivityJSON}})
	assert.Equal(t, http.StatusNotFound, rec.Code,
		"without a service actor the origin apex publishes nothing")
}

// TestServeNodeInfo: Lemmy's scheduled task reads software.name for its
// instance allow/block lists. It never gates federation, but an origin that
// answers nothing here shows up as an unknown implementation.
func TestServeNodeInfo(t *testing.T) {
	database := personasTestDB(t)
	svc, _ := newInstanceService(t, database)

	discovery := serveOnUserOrigin(svc, http.MethodGet, "/.well-known/nodeinfo", nil)
	require.Equal(t, http.StatusOK, discovery.Code, "body=%s", discovery.Body.String())
	assert.Contains(t, discovery.Header().Get("Content-Type"), "application/json")

	doc := decodeJSON(t, discovery)
	links, ok := doc["links"].([]any)
	require.True(t, ok, "nodeinfo discovery must carry links, got %v", doc["links"])
	require.Len(t, links, 1)
	link, ok := links[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "http://nodeinfo.diaspora.software/ns/schema/2.0", link["rel"])
	assert.Equal(t, userOrigin+"/nodeinfo/2.0", link["href"],
		"the discovery link must point at THIS origin, not the service origin")

	schema := serveOnUserOrigin(svc, http.MethodGet, "/nodeinfo/2.0", nil)
	require.Equal(t, http.StatusOK, schema.Code, "body=%s", schema.Body.String())
	assert.Contains(t, schema.Header().Get("Content-Type"), "application/json")

	info := decodeJSON(t, schema)
	assert.Equal(t, "2.0", info["version"])
	software, ok := info["software"].(map[string]any)
	require.True(t, ok, "software block is required, got %v", info["software"])
	assert.Equal(t, "tidepool", software["name"],
		"Lemmy admins allowlist by this exact string")
	assert.NotEmpty(t, software["version"])
	protocols, ok := info["protocols"].([]any)
	require.True(t, ok, "protocols is required, got %v", info["protocols"])
	assert.Contains(t, protocols, "activitypub")
	assert.Equal(t, false, info["openRegistrations"],
		"accounts are minted from atproto identities, never registered here")
}
