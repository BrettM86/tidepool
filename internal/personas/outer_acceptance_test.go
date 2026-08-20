package personas

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/identity"
)

// The user origin under test. AP_USER_ORIGIN's production value; the config
// var itself is not wired here — the acceptance test constructs the Service
// directly rather than booting main.
const (
	userOrigin = "https://coves.social"
	userHost   = "coves.social"

	// A native Coves handle: the local part derives to "alice" (a
	// non-native handle like bretton.dev would keep the full handle).
	testHandle    = "alice.coves.social"
	testLocalPart = "alice"
)

// testKEK seals the test actor's AP RSA key (32 bytes, AES-256).
var testKEK = []byte("0123456789abcdef0123456789abcdef")

// TestUserOriginActorSurface is the outer acceptance test for task 13.
//
// GIVEN the bridge configured with user origin https://coves.social,
// WHEN CreateActorForDID mints a Person actor for a Coves DID,
// THEN, driven over HTTP with Host: coves.social —
//
//  1. WebFinger resolves acct:alice@coves.social to the actor URL;
//  2. that URL serves a Lemmy-parseable Person document;
//  3. a request signed with that actor's key verifies through the
//     EXISTING ap.Verifier, whose resolver is a real ap.Client fetching
//     the served document (authority binding included);
//  4. the private key is nowhere in the row in the clear.
//
// No network: the only outbound host the client may dial is coves.social,
// and that is rewritten onto the httptest listener.
func TestUserOriginActorSurface(t *testing.T) {
	// Start from an empty namespace: a sibling test's alice would push this
	// actor's frozen local part to alice-2 and the webfinger assertions
	// below would be asserting the wrong name.
	conn := personasTestDB(t)
	ctx := context.Background()

	custodian, err := identity.NewCustodian(testKEK)
	require.NoError(t, err, "build custodian")

	svc, err := New(Options{DB: conn, Custodian: custodian, UserOrigin: userOrigin})
	require.NoError(t, err, "build personas service")
	require.NotNil(t, svc, "personas.New must return a service")

	did := testDID(t)

	// WHEN: the DID's first federating interaction mints its actor.
	actor, err := svc.CreateActorForDID(ctx, did, testHandle)
	require.NoError(t, err, "CreateActorForDID must mint an actor for %s", did)
	require.NotNil(t, actor, "CreateActorForDID must return the minted actor")

	wantActorID := userOrigin + "/ap/actor/" + did
	require.Equal(t, wantActorID, actor.ActorID,
		"the stored actor_id is the full actor URL, origin included")
	require.Equal(t, testLocalPart, actor.LocalPart,
		"a native handle %q derives the local part %q", testHandle, testLocalPart)

	// ---------------------------------------------------------------
	// 1. WebFinger on the user origin.
	// ---------------------------------------------------------------
	resource := fmt.Sprintf("acct:%s@%s", testLocalPart, userHost)
	rec := serveOnUserOrigin(svc, http.MethodGet,
		"/.well-known/webfinger?resource="+url.QueryEscape(resource), nil)
	require.Equal(t, http.StatusOK, rec.Code,
		"GET webfinger for %s (Host %s) must resolve the minted actor; body=%s",
		resource, userHost, rec.Body.String())

	var jrd ap.WebFingerResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &jrd),
		"webfinger must serve a JRD document, got %s", rec.Body.String())
	selfHref := ""
	for _, link := range jrd.Links {
		if link.Rel == "self" && strings.Contains(link.Type, "activity+json") {
			selfHref = link.Href
			break
		}
	}
	require.Equal(t, wantActorID, selfHref,
		`the rel="self" link (type application/activity+json) must href the actor URL; JRD=%s`,
		rec.Body.String())

	// ---------------------------------------------------------------
	// 2. The Person document at the href webfinger just handed out.
	// ---------------------------------------------------------------
	actorPath := mustPath(t, selfHref)
	rec = serveOnUserOrigin(svc, http.MethodGet, actorPath,
		http.Header{"Accept": []string{ap.ContentTypeActivityJSON}})
	require.Equal(t, http.StatusOK, rec.Code,
		"GET %s (Host %s) must serve the actor document; body=%s",
		actorPath, userHost, rec.Body.String())
	require.Contains(t, rec.Header().Get("Content-Type"), "activity+json",
		"the actor document must be served as application/activity+json")

	var doc map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &doc),
		"actor document must be JSON, got %s", rec.Body.String())

	require.Equal(t, wantActorID, doc["id"], "actor id")
	require.Equal(t, "Person", doc["type"],
		"Person, NOT Service: Lemmy rejects votes from Service actors as bots")
	require.Equal(t, testLocalPart, doc["preferredUsername"], "preferredUsername")

	publicKey, ok := doc["publicKey"].(map[string]any)
	require.True(t, ok, "actor document must carry a publicKey object, got %v", doc["publicKey"])
	require.Equal(t, wantActorID+"#main-key", publicKey["id"], "publicKey.id")
	require.Equal(t, wantActorID, publicKey["owner"], "publicKey.owner")
	publishedPEM, ok := publicKey["publicKeyPem"].(string)
	require.True(t, ok, "publicKey.publicKeyPem must be a string, got %v", publicKey["publicKeyPem"])
	publishedKey, err := ap.ParsePublicKeyPEM([]byte(publishedPEM))
	require.NoError(t, err, "publicKeyPem must parse as an RSA public key")
	require.NotNil(t, publishedKey)

	wantInbox := userOrigin + "/ap/inbox"
	require.Equal(t, wantInbox, doc["inbox"], "inbox")
	endpoints, ok := doc["endpoints"].(map[string]any)
	require.True(t, ok, "actor document must carry endpoints, got %v", doc["endpoints"])
	require.Equal(t, wantInbox, endpoints["sharedInbox"], "endpoints.sharedInbox")

	// outbox is REQUIRED for Lemmy's Person deserialization — omitting it
	// rejects the whole actor. A URL string or an inline collection object
	// both satisfy the shape.
	outbox, present := doc["outbox"]
	require.True(t, present, "actor document must carry an outbox field (Lemmy requires it)")
	switch v := outbox.(type) {
	case string:
		require.NotEmpty(t, v, "outbox URL must not be empty")
	case map[string]any:
		require.NotEmpty(t, v, "inline outbox collection must not be empty")
	default:
		require.Failf(t, "bad outbox shape",
			"outbox must be a URL string or a collection object, got %T (%v)", outbox, outbox)
	}

	published, ok := doc["published"].(string)
	require.True(t, ok, "actor document must carry published, got %v", doc["published"])
	_, err = time.Parse(time.RFC3339, published)
	require.NoError(t, err, "published must be RFC3339, got %q", published)

	// ---------------------------------------------------------------
	// 3. A signed request from this actor verifies through the EXISTING
	//    verifier, resolving the key off the document we just served.
	// ---------------------------------------------------------------
	origin := httptest.NewServer(svc)
	t.Cleanup(origin.Close)

	// The rewrite transport is deliberately NOT an *http.Transport, so
	// ap.NewClient's guardedTransport passes it through unchanged
	// (client.go:279-294) — the request keeps its https://coves.social URL
	// and Host while the bytes go to the httptest listener. That is what
	// makes the client's authority binding (fetched id must match the
	// fetch URL's authority) a real assertion here.
	client := ap.NewClient(ap.ClientOptions{
		HTTPClient: &http.Client{Transport: originRewriteTransport{
			host:   userHost,
			target: origin.Listener.Addr().String(),
		}},
	})
	verifier := ap.NewVerifier(client)

	signer, err := svc.actorSigner(ctx, did)
	require.NoError(t, err, "the actor's sealed key must unseal into a signer")
	require.NotNil(t, signer, "actorSigner must return a signer for %s", did)
	require.Equal(t, wantActorID+"#main-key", signer.KeyID(),
		"the signer's keyId must be the one published in the actor document")

	body := []byte(`{"@context":"https://www.w3.org/ns/activitystreams",` +
		`"id":"https://coves.social/ap/activity/acceptance-probe","type":"Create"}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, wantInbox, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", ap.ContentTypeActivityJSON)
	require.NoError(t, signer.SignRequest(req, body), "sign the inbox POST")

	verifiedActorID, err := verifier.Verify(ctx, req, body)
	require.NoError(t, err,
		"the served actor document must feed the production key resolver")
	require.Equal(t, wantActorID, verifiedActorID,
		"Verify must attribute the signature to the minted actor")

	// ---------------------------------------------------------------
	// 4. Sealed-key proof: the private key is never at rest in the clear.
	//    Expected schema (migration 017): ap_actors(did PK,
	//    rsa_key_sealed BYTEA, public_key_pem TEXT, ...).
	// ---------------------------------------------------------------
	var sealed, storedPubPEM []byte
	err = conn.QueryRowContext(ctx,
		`SELECT rsa_key_sealed, public_key_pem FROM ap_actors WHERE did = $1`, did).
		Scan(&sealed, &storedPubPEM)
	if err != nil {
		require.Failf(t, "ap_actors key columns not readable",
			"SELECT rsa_key_sealed, public_key_pem FROM ap_actors WHERE did = %q: %v\n"+
				"(migration 017 must create ap_actors with a sealed private-key column "+
				"and a public-key PEM column)", did, err)
	}
	require.NotEmpty(t, sealed, "ap_actors.rsa_key_sealed must hold the sealed key")
	require.NotContains(t, string(sealed), "-----BEGIN",
		"the AP RSA private key must never be stored unsealed")
	require.NotEmpty(t, storedPubPEM, "ap_actors.public_key_pem must hold the published key")

	storedPub, err := ap.ParsePublicKeyPEM(storedPubPEM)
	require.NoError(t, err, "the stored public-key PEM must parse")
	require.True(t, storedPub.Equal(publishedKey),
		"the stored public key must be the one published in the actor document")

	opened, err := custodian.DecryptActorRSAKey(did, sealed)
	require.NoError(t, err, "the custodian must open the sealed key under its DID")
	require.NotNil(t, opened, "DecryptActorRSAKey must return the RSA private key")
	require.True(t, opened.PublicKey.Equal(publishedKey),
		"the sealed private key must match the published public key")
}

// serveOnUserOrigin drives the personas handler directly with Host set to
// the user origin.
func serveOnUserOrigin(h http.Handler, method, target string, header http.Header) *httptest.ResponseRecorder {
	return serveOnHost(h, userHost, method, target, header)
}

// originRewriteTransport sends requests for the user origin to the local
// httptest listener while preserving the request's URL and Host, so the AP
// client believes it is talking to https://coves.social. Anything else is
// refused: these tests never touch the network.
type originRewriteTransport struct {
	host   string
	target string
}

func (rt originRewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if !strings.EqualFold(req.URL.Hostname(), rt.host) {
		return nil, fmt.Errorf("refusing outbound request to %s: tests may only reach %s",
			req.URL, rt.host)
	}
	clone := req.Clone(req.Context())
	clone.Host = req.URL.Host
	clone.URL.Scheme = "http"
	clone.URL.Host = rt.target
	return http.DefaultTransport.RoundTrip(clone)
}

// mustPath returns the path (plus query) of an absolute URL.
func mustPath(t *testing.T, raw string) string {
	t.Helper()
	parsed, err := url.Parse(raw)
	require.NoError(t, err, "parse actor URL %q", raw)
	require.Equal(t, userHost, parsed.Host, "the actor URL must live on the user origin")
	if parsed.RawQuery != "" {
		return parsed.EscapedPath() + "?" + parsed.RawQuery
	}
	return parsed.EscapedPath()
}

// testDID returns a fresh did:plc-shaped identifier so reruns never collide.
func testDID(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 15)
	_, err := rand.Read(raw)
	require.NoError(t, err)
	return "did:plc:" + strings.ToLower(base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw))
}
