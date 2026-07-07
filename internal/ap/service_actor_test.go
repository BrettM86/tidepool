package ap

import (
	"context"
	"net/http/httptest"
	"sync"
	"testing"

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

	first, err := LoadOrCreateServiceActor(ctx, keys, "bridge.example")
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
	second, err := LoadOrCreateServiceActor(ctx, keys, "bridge.example")
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

	actor, err := LoadOrCreateServiceActor(ctx, keys, "bridge.example")
	require.NoError(t, err)
	assert.True(t, winnerKey.Equal(actor.Key),
		"losing the create race must converge on the winner's key")
}

func TestLoadOrCreateServiceActor_RequiresHostname(t *testing.T) {
	_, err := LoadOrCreateServiceActor(context.Background(), newFakeServiceKeys(), "")
	require.Error(t, err)
	assert.True(t, errors.IsValidation(err))
}

func TestLoadOrCreateServiceActor_CorruptKey(t *testing.T) {
	keys := newFakeServiceKeys()
	_, err := keys.Create(context.Background(), ServiceKeyName, []byte("not a pem"))
	require.NoError(t, err)

	_, err = LoadOrCreateServiceActor(context.Background(), keys, "bridge.example")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "corrupt")
}

func TestServiceActorDocument(t *testing.T) {
	keys := newFakeServiceKeys()
	actor, err := LoadOrCreateServiceActor(context.Background(), keys, "bridge.example")
	require.NoError(t, err)

	docJSON, err := actor.DocumentJSON()
	require.NoError(t, err)

	// The document must parse back through our own tolerant vocab and look
	// like an actor Lemmy can resolve keys from.
	doc, err := ParseObject(docJSON)
	require.NoError(t, err)
	assert.True(t, doc.IsActor())
	assert.Equal(t, TypeApplication, doc.Type)
	assert.Equal(t, "https://bridge.example/actor", doc.ID)
	assert.Equal(t, "https://bridge.example/inbox", doc.Inbox)
	assert.NotEmpty(t, doc.PreferredUsername, "webfinger reverse resolution needs preferredUsername")

	require.NotNil(t, doc.PublicKey)
	assert.Equal(t, "https://bridge.example/actor#main-key", doc.PublicKey.ID,
		"keyId must carry the #main-key fragment Lemmy's regex expects")
	assert.Equal(t, doc.ID, doc.PublicKey.Owner)
	published, err := ParsePublicKeyPEM([]byte(doc.PublicKey.PublicKeyPem))
	require.NoError(t, err)
	assert.True(t, actor.Key.PublicKey.Equal(published))

	// The JSON-LD context must include the security vocabulary that defines
	// publicKey.
	assert.Contains(t, string(doc.Context), "https://w3id.org/security/v1")
}

func TestServiceActorSigner_RoundTrip(t *testing.T) {
	keys := newFakeServiceKeys()
	actor, err := LoadOrCreateServiceActor(context.Background(), keys, "bridge.example")
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
