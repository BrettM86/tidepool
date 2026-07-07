package ap

import (
	"context"
	"crypto/rsa"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

// testKey generates a throwaway RSA key once per test binary — key
// generation is the slow part of these tests.
var testKeyOnce = struct {
	key *rsa.PrivateKey
	err error
	ok  bool
}{}

func testRSAKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	if !testKeyOnce.ok {
		testKeyOnce.key, testKeyOnce.err = GenerateRSAKey()
		testKeyOnce.ok = true
	}
	require.NoError(t, testKeyOnce.err)
	return testKeyOnce.key
}

const testActorID = "https://bridge.example/actor"
const testKeyID = testActorID + "#main-key"

func staticResolver(key *rsa.PublicKey) KeyResolver {
	return KeyResolverFunc(func(_ context.Context, keyID string) (*rsa.PublicKey, string, error) {
		return key, ActorIDFromKeyID(keyID), nil
	})
}

func TestSignRequest_GETHeaderSet(t *testing.T) {
	key := testRSAKey(t)
	signer := NewSigner(testKeyID, key)

	req := httptest.NewRequest(http.MethodGet, "https://lemmy.world/c/technology?page=2", nil)
	require.NoError(t, signer.SignRequest(req, nil))

	// The exact header set Lemmy's verifier accepts for GETs. Digest MUST be
	// present and signed even on GET: Lemmy's verify_signature_inner applies
	// require_digest() to every request (activitypub-federation-rust
	// src/http_signatures.rs), and bridgy-fed ships the same set.
	assert.NotEmpty(t, req.Header.Get("Date"))
	_, err := http.ParseTime(req.Header.Get("Date"))
	assert.NoError(t, err, "Date must be a valid HTTP date")
	// SHA-256 of the empty body.
	assert.Equal(t, "SHA-256=47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=", req.Header.Get("Digest"))

	fields := parseSignatureHeader(req.Header.Get("Signature"))
	assert.Equal(t, testKeyID, fields["keyid"])
	assert.Equal(t, "rsa-sha256", fields["algorithm"])
	assert.Equal(t, "(request-target) host date digest", fields["headers"])
	assert.NotEmpty(t, fields["signature"])

	// The signing string must include the query in (request-target).
	assert.Equal(t, "https://lemmy.world/c/technology?page=2", req.URL.String())
}

func TestSignRequest_POSTHeaderSet(t *testing.T) {
	key := testRSAKey(t)
	signer := NewSigner(testKeyID, key)
	body := []byte(`{"type":"Follow"}`)

	req := httptest.NewRequest(http.MethodPost, "https://lemmy.world/c/technology/inbox", nil)
	req.Header.Set("Content-Type", ContentTypeActivityJSON)
	require.NoError(t, signer.SignRequest(req, body))

	fields := parseSignatureHeader(req.Header.Get("Signature"))
	assert.Equal(t, "(request-target) host date digest content-type", fields["headers"],
		"POSTs sign content-type too, matching Lemmy's own outbound set")
	assert.NotEmpty(t, req.Header.Get("Digest"))
	assert.NotEqual(t, "SHA-256=47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=", req.Header.Get("Digest"),
		"POST digest covers the body, not the empty string")
}

func TestSignVerifyRoundTrip_GET(t *testing.T) {
	key := testRSAKey(t)
	signer := NewSigner(testKeyID, key)
	verifier := NewVerifier(staticResolver(&key.PublicKey))

	req := httptest.NewRequest(http.MethodGet, "https://lemmy.world/post/49131386", nil)
	require.NoError(t, signer.SignRequest(req, nil))

	actorID, err := verifier.Verify(context.Background(), req, nil)
	require.NoError(t, err)
	assert.Equal(t, testActorID, actorID)
}

func TestSignVerifyRoundTrip_POST(t *testing.T) {
	key := testRSAKey(t)
	signer := NewSigner(testKeyID, key)
	verifier := NewVerifier(staticResolver(&key.PublicKey))
	body := []byte(`{"type":"Announce","id":"https://lemmy.world/activities/announce/x"}`)

	req := httptest.NewRequest(http.MethodPost, "https://bridge.example/inbox", nil)
	require.NoError(t, signer.SignRequest(req, body))

	actorID, err := verifier.Verify(context.Background(), req, body)
	require.NoError(t, err)
	assert.Equal(t, testActorID, actorID)
}

func TestVerify_RejectsTamperedBody(t *testing.T) {
	key := testRSAKey(t)
	signer := NewSigner(testKeyID, key)
	verifier := NewVerifier(staticResolver(&key.PublicKey))
	body := []byte(`{"type":"Announce"}`)

	req := httptest.NewRequest(http.MethodPost, "https://bridge.example/inbox", nil)
	require.NoError(t, signer.SignRequest(req, body))

	_, err := verifier.Verify(context.Background(), req, []byte(`{"type":"Delete"}`))
	require.Error(t, err)
	assert.True(t, errors.IsValidation(err), "signature failures unwrap to ErrInvalidInput, got %v", err)
	assert.Contains(t, err.Error(), "digest")
}

func TestVerify_RejectsTamperedTarget(t *testing.T) {
	key := testRSAKey(t)
	signer := NewSigner(testKeyID, key)
	verifier := NewVerifier(staticResolver(&key.PublicKey))

	req := httptest.NewRequest(http.MethodGet, "https://bridge.example/actor", nil)
	require.NoError(t, signer.SignRequest(req, nil))
	// Replay the signed headers against a different path.
	replayed := httptest.NewRequest(http.MethodGet, "https://bridge.example/inbox", nil)
	replayed.Header = req.Header.Clone()
	replayed.Host = req.Host

	_, err := verifier.Verify(context.Background(), replayed, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not verify")
}

func TestVerify_RejectsWrongKey(t *testing.T) {
	key := testRSAKey(t)
	otherKey, err := GenerateRSAKey()
	require.NoError(t, err)

	signer := NewSigner(testKeyID, key)
	verifier := NewVerifier(staticResolver(&otherKey.PublicKey))

	req := httptest.NewRequest(http.MethodGet, "https://bridge.example/actor", nil)
	require.NoError(t, signer.SignRequest(req, nil))

	_, err = verifier.Verify(context.Background(), req, nil)
	require.Error(t, err)
	assert.True(t, errors.IsValidation(err))
}

func TestVerify_RejectsDateSkew(t *testing.T) {
	key := testRSAKey(t)
	signer := NewSigner(testKeyID, key)
	verifier := NewVerifier(staticResolver(&key.PublicKey))
	// Pretend the request arrives 90 minutes later — past Lemmy's one-hour
	// window.
	verifier.now = func() time.Time { return time.Now().Add(90 * time.Minute) }

	req := httptest.NewRequest(http.MethodGet, "https://bridge.example/actor", nil)
	require.NoError(t, signer.SignRequest(req, nil))

	_, err := verifier.Verify(context.Background(), req, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "skew")
}

func TestVerify_RequiresSignature(t *testing.T) {
	key := testRSAKey(t)
	verifier := NewVerifier(staticResolver(&key.PublicKey))

	req := httptest.NewRequest(http.MethodPost, "https://bridge.example/inbox", nil)
	_, err := verifier.Verify(context.Background(), req, []byte(`{}`))
	require.Error(t, err)
	assert.True(t, errors.IsValidation(err))
}

func TestVerify_RequiresDigestOnBody(t *testing.T) {
	key := testRSAKey(t)
	signer := NewSigner(testKeyID, key)
	verifier := NewVerifier(staticResolver(&key.PublicKey))
	body := []byte(`{"type":"Like"}`)

	// Sign as GET (no digest in signed set)…
	req := httptest.NewRequest(http.MethodPost, "https://bridge.example/inbox", nil)
	require.NoError(t, signer.SignRequest(req, nil))
	// …then claim a body: must be rejected because digest isn't signed over
	// the actual payload.
	_, err := verifier.Verify(context.Background(), req, body)
	require.Error(t, err)
	assert.True(t, errors.IsValidation(err))
}

func TestVerify_RequiresHostSigned(t *testing.T) {
	key := testRSAKey(t)
	signer := NewSigner(testKeyID, key)
	verifier := NewVerifier(staticResolver(&key.PublicKey))

	req := httptest.NewRequest(http.MethodGet, "https://bridge.example/actor", nil)
	require.NoError(t, signer.SignRequest(req, nil))
	// Strip host from the signed header set (still present in the header map).
	sig := req.Header.Get("Signature")
	sig = strings.Replace(sig, "(request-target) host date digest", "(request-target) date digest", 1)
	req.Header.Set("Signature", sig)

	_, err := verifier.Verify(context.Background(), req, nil)
	require.Error(t, err)
	assert.True(t, errors.IsValidation(err))
	assert.Contains(t, err.Error(), "host")
}

func TestVerify_AcceptsHS2019Algorithm(t *testing.T) {
	key := testRSAKey(t)
	signer := NewSigner(testKeyID, key)
	verifier := NewVerifier(staticResolver(&key.PublicKey))

	req := httptest.NewRequest(http.MethodGet, "https://bridge.example/actor", nil)
	require.NoError(t, signer.SignRequest(req, nil))
	// Mastodon labels RSA-SHA256 signatures "hs2019"; the bytes are the same.
	req.Header.Set("Signature", strings.Replace(req.Header.Get("Signature"),
		`algorithm="rsa-sha256"`, `algorithm="hs2019"`, 1))

	_, err := verifier.Verify(context.Background(), req, nil)
	require.NoError(t, err)
}

func TestVerify_RejectsUnknownAlgorithm(t *testing.T) {
	key := testRSAKey(t)
	signer := NewSigner(testKeyID, key)
	verifier := NewVerifier(staticResolver(&key.PublicKey))

	req := httptest.NewRequest(http.MethodGet, "https://bridge.example/actor", nil)
	require.NoError(t, signer.SignRequest(req, nil))
	req.Header.Set("Signature", strings.Replace(req.Header.Get("Signature"),
		`algorithm="rsa-sha256"`, `algorithm="ed25519"`, 1))

	_, err := verifier.Verify(context.Background(), req, nil)
	require.Error(t, err)
}

func TestParseSignatureHeader(t *testing.T) {
	// Real-world shape (Lemmy emits exactly this ordering).
	fields := parseSignatureHeader(
		`keyId="https://lemmy.world/c/technology#main-key",algorithm="rsa-sha256",` +
			`headers="(request-target) content-type date digest host",signature="c2ln"`)
	assert.Equal(t, "https://lemmy.world/c/technology#main-key", fields["keyid"])
	assert.Equal(t, "rsa-sha256", fields["algorithm"])
	assert.Equal(t, "(request-target) content-type date digest host", fields["headers"])
	assert.Equal(t, "c2ln", fields["signature"])

	// Spacing and unquoted values tolerated.
	fields = parseSignatureHeader(`keyId="k" , algorithm=hs2019, signature="s"`)
	assert.Equal(t, "k", fields["keyid"])
	assert.Equal(t, "hs2019", fields["algorithm"])
	assert.Equal(t, "s", fields["signature"])
}

func TestDigestMatches(t *testing.T) {
	assert.True(t, digestMatches("SHA-256=abc", "abc"))
	assert.True(t, digestMatches("sha-256=abc", "abc"), "algorithm name is case-insensitive")
	assert.True(t, digestMatches("SHA-512=zzz, SHA-256=abc", "abc"), "multi-valued digest headers")
	assert.False(t, digestMatches("SHA-256=xyz", "abc"))
	assert.False(t, digestMatches("", "abc"))
}

func TestKeyPEMRoundTrip(t *testing.T) {
	key := testRSAKey(t)

	privatePEM, err := EncodePrivateKeyPEM(key)
	require.NoError(t, err)
	assert.Contains(t, string(privatePEM), "BEGIN PRIVATE KEY")
	parsedPrivate, err := ParsePrivateKeyPEM(privatePEM)
	require.NoError(t, err)
	assert.True(t, key.Equal(parsedPrivate))

	publicPEM, err := EncodePublicKeyPEM(&key.PublicKey)
	require.NoError(t, err)
	assert.Contains(t, string(publicPEM), "BEGIN PUBLIC KEY")
	parsedPublic, err := ParsePublicKeyPEM(publicPEM)
	require.NoError(t, err)
	assert.True(t, key.PublicKey.Equal(parsedPublic))
}

func TestParsePublicKeyPEM_LemmyFixture(t *testing.T) {
	// The publicKeyPem published by a real Lemmy actor must parse.
	person := parseFixture(t, "person_lemmy_world.json")
	require.NotNil(t, person.PublicKey)
	key, err := ParsePublicKeyPEM([]byte(person.PublicKey.PublicKeyPem))
	require.NoError(t, err)
	assert.Equal(t, ServiceKeyBits, key.Size()*8, "Lemmy actor keys are RSA-2048")
}

func TestActorIDFromKeyID(t *testing.T) {
	assert.Equal(t, "https://lemmy.world/u/alice", ActorIDFromKeyID("https://lemmy.world/u/alice#main-key"))
	assert.Equal(t, "https://lemmy.world/u/alice", ActorIDFromKeyID("https://lemmy.world/u/alice"))
}
