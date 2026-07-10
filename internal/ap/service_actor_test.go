package ap

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// fakeServiceKeys is an in-memory store.ServiceKeys. The postgres
// implementation is covered by internal/store's real-DB tests; these tests
// exercise the bootstrap logic.
type fakeServiceKeys struct {
	mu   sync.Mutex
	rows map[string][]byte
	// createHook runs inside Create before the insert (to simulate races).
	createHook func()
}

func newFakeServiceKeys() *fakeServiceKeys {
	return &fakeServiceKeys{rows: map[string][]byte{}}
}

func (f *fakeServiceKeys) Create(_ context.Context, name string, pem []byte) (*store.ServiceKey, error) {
	if f.createHook != nil {
		f.createHook()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.rows[name]; exists {
		return nil, errors.NewConflictError("service_key", "name", name)
	}
	f.rows[name] = pem
	return &store.ServiceKey{ID: 1, Name: name, PrivateKeyPEM: pem}, nil
}

func (f *fakeServiceKeys) Get(_ context.Context, name string) (*store.ServiceKey, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	pem, ok := f.rows[name]
	if !ok {
		return nil, errors.NewNotFoundError("service_key", name)
	}
	return &store.ServiceKey{ID: 1, Name: name, PrivateKeyPEM: pem}, nil
}

func TestLoadOrCreateServiceActor_GeneratesThenLoads(t *testing.T) {
	keys := newFakeServiceKeys()
	ctx := context.Background()

	first, err := LoadOrCreateServiceActor(ctx, keys, "bridge.example", "")
	require.NoError(t, err)
	assert.Equal(t, "https://bridge.example/actor", first.ID)
	assert.Equal(t, "https://bridge.example/actor#main-key", first.KeyID())
	assert.Equal(t, "https://bridge.example/inbox", first.InboxURL())
	require.NotNil(t, first.Key)

	// The key must have been persisted as parseable PKCS#8 PEM.
	stored, err := keys.Get(ctx, ServiceKeyName)
	require.NoError(t, err)
	storedKey, err := ParsePrivateKeyPEM(stored.PrivateKeyPEM)
	require.NoError(t, err)
	assert.True(t, first.Key.Equal(storedKey))

	// A second bootstrap loads the same key instead of generating a new one.
	second, err := LoadOrCreateServiceActor(ctx, keys, "bridge.example", "")
	require.NoError(t, err)
	assert.True(t, first.Key.Equal(second.Key), "restarts must reuse the persisted key")
}

func TestLoadOrCreateServiceActor_LosesBootstrapRace(t *testing.T) {
	keys := newFakeServiceKeys()
	ctx := context.Background()

	// Simulate a concurrent instance winning the insert between our Get
	// (not found) and Create.
	winnerKey, err := GenerateRSAKey()
	require.NoError(t, err)
	winnerPEM, err := EncodePrivateKeyPEM(winnerKey)
	require.NoError(t, err)
	keys.createHook = func() {
		keys.mu.Lock()
		if _, exists := keys.rows[ServiceKeyName]; !exists {
			keys.rows[ServiceKeyName] = winnerPEM
		}
		keys.mu.Unlock()
	}

	actor, err := LoadOrCreateServiceActor(ctx, keys, "bridge.example", "")
	require.NoError(t, err)
	assert.True(t, winnerKey.Equal(actor.Key),
		"losing the create race must converge on the winner's key")
}

func TestLoadOrCreateServiceActor_RequiresHostname(t *testing.T) {
	_, err := LoadOrCreateServiceActor(context.Background(), newFakeServiceKeys(), "", "")
	require.Error(t, err)
	assert.True(t, errors.IsValidation(err))
}

func TestLoadOrCreateServiceActor_CorruptKey(t *testing.T) {
	keys := newFakeServiceKeys()
	_, err := keys.Create(context.Background(), ServiceKeyName, []byte("not a pem"))
	require.NoError(t, err)

	_, err = LoadOrCreateServiceActor(context.Background(), keys, "bridge.example", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "corrupt")
}

func TestServiceActorDocument(t *testing.T) {
	keys := newFakeServiceKeys()
	actor, err := LoadOrCreateServiceActor(context.Background(), keys, "bridge.example", "")
	require.NoError(t, err)

	docJSON, err := actor.DocumentJSON()
	require.NoError(t, err)

	// The document must parse back through our own tolerant vocab and look
	// like an actor Lemmy can resolve keys from.
	doc, err := ParseObject(docJSON)
	require.NoError(t, err)
	assert.True(t, doc.IsActor())
	assert.Equal(t, TypeService, doc.Type)
	assert.Equal(t, "https://bridge.example/actor", doc.ID)
	assert.Equal(t, "https://bridge.example/inbox", doc.Inbox)
	assert.Equal(t, "https://bridge.example/outbox", doc.Outbox)
	assert.NotEmpty(t, doc.PreferredUsername, "webfinger reverse resolution needs preferredUsername")

	require.NotNil(t, doc.PublicKey)
	assert.Equal(t, "https://bridge.example/actor#main-key", doc.PublicKey.ID,
		"keyId must carry the #main-key fragment Lemmy's regex expects")
	assert.Equal(t, doc.ID, doc.PublicKey.Owner)
	published, err := ParsePublicKeyPEM([]byte(doc.PublicKey.PublicKeyPem))
	require.NoError(t, err)
	assert.True(t, actor.Key.PublicKey.Equal(published))

	// The JSON-LD context must include core ActivityStreams and the security
	// vocabulary that defines publicKey.
	assert.Contains(t, string(doc.Context), "https://www.w3.org/ns/activitystreams")
	assert.Contains(t, string(doc.Context), "https://w3id.org/security/v1")
}

// TestInstanceActorDocument pins the wire shape Lemmy 0.19.19's Instance
// protocol requires of the apex ("Site") actor — the document that makes
// Lemmy deliver send-to-all-instances activities (account deletions!) to
// the bridge. type must be exactly Application (NOT Service — the opposite
// of the /actor rule), and id/name/inbox/outbox/publicKey/published are
// required.
func TestInstanceActorDocument(t *testing.T) {
	keys := newFakeServiceKeys()
	actor, err := LoadOrCreateServiceActor(context.Background(), keys, "bridge.example", "")
	require.NoError(t, err)

	docJSON, err := actor.InstanceDocumentJSON()
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(docJSON, &doc))

	assert.Equal(t, "Application", doc["type"],
		"Lemmy's Instance enum accepts only Application")
	assert.Equal(t, "https://bridge.example/", doc["id"],
		"the id must be the origin apex WITH trailing slash — the exact URL Lemmy derives")
	assert.Equal(t, "https://bridge.example/inbox", doc["inbox"],
		"this inbox is where Lemmy will deliver Delete{Person}")
	assert.Equal(t, "https://bridge.example/outbox", doc["outbox"])
	assert.Contains(t, doc["@context"], "https://www.w3.org/ns/activitystreams",
		"the JSON-LD context must include core ActivityStreams")
	for _, field := range []string{"name", "published"} {
		s, _ := doc[field].(string)
		assert.NotEmpty(t, s, "Lemmy requires %s on the Instance document", field)
	}
	if published, ok := doc["published"].(string); assert.True(t, ok) {
		_, err := time.Parse(time.RFC3339, published)
		assert.NoError(t, err, "published must be RFC3339")
	}

	pk, ok := doc["publicKey"].(map[string]any)
	require.True(t, ok, "publicKey block is required")
	assert.Equal(t, "https://bridge.example/#main-key", pk["id"])
	assert.Equal(t, "https://bridge.example/", pk["owner"])
	pem, _ := pk["publicKeyPem"].(string)
	parsed, err := ParsePublicKeyPEM([]byte(pem))
	require.NoError(t, err)
	assert.True(t, actor.Key.PublicKey.Equal(parsed),
		"the instance actor reuses the service RSA key (Lemmy reads only publicKeyPem)")
}

func TestServiceActorSigner_RoundTrip(t *testing.T) {
	keys := newFakeServiceKeys()
	actor, err := LoadOrCreateServiceActor(context.Background(), keys, "bridge.example", "")
	require.NoError(t, err)

	// Resolve the verification key exactly the way a remote instance would:
	// from the actor document's publicKeyPem.
	doc, err := actor.Document()
	require.NoError(t, err)
	remoteKey, err := ParsePublicKeyPEM([]byte(doc.PublicKey.PublicKeyPem))
	require.NoError(t, err)
	verifier := NewVerifier(staticResolver(remoteKey))

	req := httptest.NewRequest("GET", "https://lemmy.world/c/technology", nil)
	require.NoError(t, actor.Signer().SignRequest(req, nil))
	signerActorID, err := verifier.Verify(context.Background(), req, nil)
	require.NoError(t, err)
	assert.Equal(t, actor.ID, signerActorID)
}

func TestLoadOrCreateServiceActor_HTTPScheme(t *testing.T) {
	actor, err := LoadOrCreateServiceActor(context.Background(), newFakeServiceKeys(), "tidepool", "http")
	require.NoError(t, err)
	assert.Equal(t, "http://tidepool/actor", actor.ID)
	assert.Equal(t, "http://tidepool/inbox", actor.InboxURL())
	assert.Equal(t, "http://tidepool/outbox", actor.OutboxURL())
	assert.Equal(t, "http://tidepool", actor.BaseURL())

	doc, err := actor.Document()
	require.NoError(t, err)
	assert.Equal(t, "http://tidepool/actor", doc.ID)
	assert.Equal(t, "http://tidepool/inbox", doc.Inbox)
}

func TestLoadOrCreateServiceActor_RejectsBadScheme(t *testing.T) {
	_, err := LoadOrCreateServiceActor(context.Background(), newFakeServiceKeys(), "bridge.example", "gopher")
	require.Error(t, err)
	assert.True(t, errors.IsValidation(err))
}

func TestServiceActor_BaseURLDefaultsToHTTPS(t *testing.T) {
	actor := &ServiceActor{Hostname: "bridge.example"}
	assert.Equal(t, "https://bridge.example", actor.BaseURL(),
		"hand-built literals without a scheme must render https, never a schemeless URL")
}
