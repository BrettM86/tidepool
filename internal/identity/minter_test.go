package identity

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

func TestHandleLabels(t *testing.T) {
	tests := []struct {
		name         string
		username     string
		instance     string
		wantName     string
		wantInstance string
		wantErr      bool
	}{
		{name: "community", username: "technology", instance: "lemmy.world", wantName: "technology", wantInstance: "lemmy-world"},
		{name: "user", username: "alice", instance: "lemmy.world", wantName: "alice", wantInstance: "lemmy-world"},
		{name: "underscores become dashes", username: "cool_user", instance: "lemmy.world", wantName: "cool-user", wantInstance: "lemmy-world"},
		{name: "uppercase lowered", username: "Alice", instance: "Lemmy.World", wantName: "alice", wantInstance: "lemmy-world"},
		{name: "multi-label instance", username: "news", instance: "lemmy.sdf.org", wantName: "news", wantInstance: "lemmy-sdf-org"},
		{name: "weird chars collapse", username: "a__b!!c", instance: "lemmy.world", wantName: "a-b-c", wantInstance: "lemmy-world"},
		{name: "trailing junk trimmed", username: "alice_", instance: "lemmy.world", wantName: "alice", wantInstance: "lemmy-world"},
		// Usernames with no representable characters fall back to a
		// deterministic hash label instead of failing the mint.
		{name: "unrepresentable username", username: "___", instance: "lemmy.world", wantName: fallbackLabel("___"), wantInstance: "lemmy-world"},
		{name: "cjk username", username: "日本語ユーザー", instance: "lemmy.world", wantName: fallbackLabel("日本語ユーザー"), wantInstance: "lemmy-world"},
		{name: "emoji username", username: "🦀🦀", instance: "lemmy.world", wantName: fallbackLabel("🦀🦀"), wantInstance: "lemmy-world"},
		{name: "empty username", username: "", instance: "lemmy.world", wantErr: true},
		{name: "empty instance", username: "alice", instance: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, instance, err := handleLabels(tt.username, tt.instance)
			if tt.wantErr {
				require.Error(t, err)
				assert.True(t, errors.IsValidation(err))
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantName, name)
			assert.Equal(t, tt.wantInstance, instance)
		})
	}
}

func TestNormalizeLabel_TruncatesTo63(t *testing.T) {
	long := normalizeLabel(bytesRepeat('a', 80))
	assert.Len(t, long, 63)
}

func TestFallbackLabel_DeterministicAndValid(t *testing.T) {
	a := fallbackLabel("日本語ユーザー")
	assert.Equal(t, a, fallbackLabel("日本語ユーザー"), "fallback must be deterministic")
	assert.NotEqual(t, a, fallbackLabel("другой"), "different inputs must differ")
	assert.Regexp(t, `^u[0-9a-f]{10}$`, a)
}

func TestSuffixLabel_KeepsDNSLabelLimit(t *testing.T) {
	// normalizeLabel truncates to 63 BEFORE suffixing; the suffix must eat
	// into the base, not overflow the label.
	base := normalizeLabel(bytesRepeat('a', 80))
	require.Len(t, base, 63)

	for _, suffix := range []string{"-2", "-10", "-50"} {
		got := suffixLabel(base, suffix)
		assert.LessOrEqual(t, len(got), 63, "suffix %s must not overflow the label", suffix)
		assert.True(t, strings.HasSuffix(got, suffix))
	}

	// Short bases are left alone.
	assert.Equal(t, "alice-2", suffixLabel("alice", "-2"))
}

func bytesRepeat(b byte, n int) string {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return string(out)
}

// TestGenesisOpEncoding pins the DAG-CBOR assumptions the PLC op signing
// depends on: nil encodes as CBOR null (the genesis op's prev), and the
// derived DID is deterministic with the did:plc shape.
func TestGenesisOpEncoding(t *testing.T) {
	encoded, err := atdata.MarshalCBOR(map[string]any{"prev": nil})
	require.NoError(t, err)
	// A1 (map of 1) 64 "prev" F6 (null)
	assert.Equal(t, []byte{0xa1, 0x64, 'p', 'r', 'e', 'v', 0xf6}, encoded,
		"nil map values must encode as DAG-CBOR null")

	op := genesisOperation("did:key:zRotation", "did:key:zSigning",
		"technology.lemmy-world.tidepool.example", "https://tidepool.example")
	services, ok := op["services"].(map[string]any)
	require.True(t, ok)
	pds, ok := services["atproto_pds"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "https://tidepool.example", pds["endpoint"],
		"the op must advertise the endpoint exactly as passed (scheme included)")
	op["sig"] = "fakesig"
	did1, err := didForOperation(op)
	require.NoError(t, err)
	did2, err := didForOperation(op)
	require.NoError(t, err)
	assert.Equal(t, did1, did2, "DID derivation must be deterministic")
	assert.Regexp(t, regexp.MustCompile(`^did:plc:[a-z2-7]{24}$`), did1)
}

// --- PLC directory integration ---
//
// These tests exercise a REAL local PLC directory (did-method-plc). They
// never touch the public https://plc.directory: testPLCURL hard-fails on
// any non-loopback host. They skip under -short and when the local
// directory is unreachable (docker compose --profile plc up -d, or the
// Coves dev PLC on the same port).

const defaultTestPLCURL = "http://localhost:3002"

// testPLCURL returns the PLC directory URL for integration tests, skipping
// when unavailable and refusing to run against anything but loopback.
func testPLCURL(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("short mode: skipping PLC directory integration test")
	}
	plcURL := os.Getenv("TIDEPOOL_TEST_PLC_URL")
	if plcURL == "" {
		plcURL = defaultTestPLCURL
	}

	// Safety rail: minting tests create DIDs; they must never spam the
	// public directory. Fail (not skip) so a misconfigured environment is
	// loud.
	parsed, err := url.Parse(plcURL)
	require.NoError(t, err, "TIDEPOOL_TEST_PLC_URL must be a URL")
	host := parsed.Hostname()
	if ip := net.ParseIP(host); ip != nil {
		if !ip.IsLoopback() {
			t.Fatalf("refusing to run PLC minting tests against non-loopback %q: "+
				"tests must never create DIDs on a public directory", plcURL)
		}
	} else if host != "localhost" {
		t.Fatalf("refusing to run PLC minting tests against non-localhost %q: "+
			"tests must never create DIDs on a public directory", plcURL)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, plcURL+"/_health", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Skipf("local PLC directory not reachable at %s (start it with "+
			"`docker compose -f docker-compose.dev.yml --profile plc up -d`): %v", plcURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Skipf("local PLC directory unhealthy at %s: status %d", plcURL, resp.StatusCode)
	}
	return plcURL
}

// testMinter builds a Minter against the local PLC directory and real
// postgres. Handles get a unique per-run bridge hostname so reruns against
// a shared PLC container never collide.
func testMinter(t *testing.T, actors store.BridgedActors) (*Minter, *Custodian, *atcrypto.PrivateKeyK256, string) {
	t.Helper()
	plcURL := testPLCURL(t)
	custodian := testCustodian(t)
	rotationKey, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	bridgeHostname := fmt.Sprintf("t%d.tidepool.example", time.Now().UnixNano())

	minter, err := NewMinter(MinterOptions{
		PLCDirectoryURL: plcURL,
		BridgeHostname:  bridgeHostname,
		RotationKey:     rotationKey,
		Custodian:       custodian,
		Actors:          actors,
		// The PLC client shares the AP client's SSRF guard; localhost needs
		// the dev/test override, exactly like the httptest-based AP tests.
		HTTPClient: ap.NewGuardedHTTPClient(true, 10*time.Second),
	})
	require.NoError(t, err)
	return minter, custodian, rotationKey, bridgeHostname
}

func TestMintActor_AgainstRealPLC(t *testing.T) {
	database := testutil.DB(t)
	testutil.Truncate(t, database, "bridged_actors")
	actors := store.NewBridgedActors(database)
	minter, custodian, rotationKey, bridgeHostname := testMinter(t, actors)
	ctx := t.Context()

	identity, err := minter.MintActor(ctx, MintRequest{
		ActorType:         store.ActorTypeGroup,
		PreferredUsername: "technology",
		Instance:          "lemmy.world",
	})
	require.NoError(t, err)

	assert.Regexp(t, `^did:plc:[a-z2-7]{24}$`, identity.DID)
	assert.Equal(t, "technology.lemmy-world."+bridgeHostname, identity.Handle)

	// The sealed signing key must decrypt back to the key registered as the
	// verification key.
	signingKey, err := custodian.DecryptActorKey(identity.DID, identity.SigningKeyEncrypted)
	require.NoError(t, err)
	signingPub, err := signingKey.PublicKey()
	require.NoError(t, err)
	assert.Equal(t, identity.DIDKey, signingPub.DIDKey())

	// Resolve the DID document from the directory and verify every claim.
	doc := fetchDIDDoc(t, minter.plcURL, identity.DID)
	assert.Equal(t, identity.DID, doc.ID)
	require.NotEmpty(t, doc.AlsoKnownAs)
	assert.Equal(t, "at://"+identity.Handle, doc.AlsoKnownAs[0])

	require.Len(t, doc.VerificationMethod, 1)
	assert.Equal(t, "Multikey", doc.VerificationMethod[0].Type)
	assert.Equal(t, identity.DIDKey, "did:key:"+doc.VerificationMethod[0].PublicKeyMultibase,
		"the DID doc's verification key must be the per-actor signing key")

	require.Len(t, doc.Service, 1)
	assert.Equal(t, "AtprotoPersonalDataServer", doc.Service[0].Type)
	assert.Equal(t, "https://"+bridgeHostname, doc.Service[0].ServiceEndpoint,
		"the PDS endpoint in the DID doc must be the bridge")

	// The directory's audit log must show the escrow rotation key.
	rotationPub, err := rotationKey.PublicKey()
	require.NoError(t, err)
	log := fetchAuditLog(t, minter.plcURL, identity.DID)
	require.NotEmpty(t, log)
	assert.Contains(t, log[0].Operation.RotationKeys, rotationPub.DIDKey(),
		"genesis op must carry the bridge escrow rotation key")
}

func TestMintActor_HandleCollisionSuffixes(t *testing.T) {
	database := testutil.DB(t)
	testutil.Truncate(t, database, "bridged_actors")
	actors := store.NewBridgedActors(database)
	minter, _, _, bridgeHostname := testMinter(t, actors)
	ctx := t.Context()

	req := MintRequest{
		ActorType:         store.ActorTypePerson,
		PreferredUsername: "alice",
		Instance:          "lemmy.world",
	}

	first, err := minter.MintActor(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, "alice.lemmy-world."+bridgeHostname, first.Handle)

	// Persist the first identity so its handle is taken (the minter's
	// availability check reads bridged_actors).
	_, err = actors.UpsertActor(ctx, store.BridgedActor{
		APActorID:           "https://lemmy.world/u/alice",
		ActorType:           store.ActorTypePerson,
		DID:                 first.DID,
		Handle:              first.Handle,
		SigningKeyEncrypted: first.SigningKeyEncrypted,
		ConsentState:        store.ConsentStateOK,
	})
	require.NoError(t, err)

	// A different AP actor normalizing to the same handle (e.g.
	// alice@lemmy.world vs Alice@lemmy.world after a rename) gets -2.
	second, err := minter.MintActor(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, "alice-2.lemmy-world."+bridgeHostname, second.Handle)
	assert.NotEqual(t, first.DID, second.DID)
}

// didDoc is the subset of a PLC DID document the tests verify.
type didDoc struct {
	ID                 string   `json:"id"`
	AlsoKnownAs        []string `json:"alsoKnownAs"`
	VerificationMethod []struct {
		ID                 string `json:"id"`
		Type               string `json:"type"`
		PublicKeyMultibase string `json:"publicKeyMultibase"`
	} `json:"verificationMethod"`
	Service []struct {
		ID              string `json:"id"`
		Type            string `json:"type"`
		ServiceEndpoint string `json:"serviceEndpoint"`
	} `json:"service"`
}

func fetchDIDDoc(t *testing.T, plcURL, did string) didDoc {
	t.Helper()
	var doc didDoc
	fetchJSON(t, plcURL+"/"+did, &doc)
	return doc
}

type auditEntry struct {
	Operation struct {
		RotationKeys []string `json:"rotationKeys"`
	} `json:"operation"`
}

func fetchAuditLog(t *testing.T, plcURL, did string) []auditEntry {
	t.Helper()
	var log []auditEntry
	fetchJSON(t, plcURL+"/"+did+"/log/audit", &log)
	return log
}

func fetchJSON(t *testing.T, rawURL string, out any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "GET %s: %s", rawURL, body)
	require.NoError(t, json.Unmarshal(body, out))
}

// Guard against accidental reintroduction of a live-directory default: the
// minter must always be told its directory explicitly.
func TestNewMinter_RequiresExplicitConfig(t *testing.T) {
	_, err := NewMinter(MinterOptions{})
	require.Error(t, err)
	assert.True(t, errors.IsValidation(err))
	assert.NotContains(t, err.Error(), "plc.directory")
}

// TestNewMinter_PDSEndpointScheme pins how BridgeScheme threads into the PDS
// endpoint advertised in minted DID documents: empty defaults to https (the
// same defensive default as ap.ServiceActor.BaseURL), http is honored (local
// e2e harness, BRIDGE_SCHEME=http), anything else is rejected. Needs no PLC
// or postgres: nothing is dialed.
func TestNewMinter_PDSEndpointScheme(t *testing.T) {
	rotationKey, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	base := MinterOptions{
		PLCDirectoryURL: "https://plc.invalid",
		BridgeHostname:  "Bridge.Example",
		RotationKey:     rotationKey,
		Custodian:       testCustodian(t),
		Actors:          struct{ store.BridgedActors }{},
		HTTPClient:      http.DefaultClient,
	}

	cases := []struct {
		scheme  string
		want    string
		wantErr bool
	}{
		{scheme: "", want: "https://bridge.example"},
		{scheme: "https", want: "https://bridge.example"},
		{scheme: "http", want: "http://bridge.example"},
		{scheme: "gopher", wantErr: true},
	}
	for _, tc := range cases {
		opts := base
		opts.BridgeScheme = tc.scheme
		m, err := NewMinter(opts)
		if tc.wantErr {
			require.Error(t, err, "scheme %q", tc.scheme)
			assert.True(t, errors.IsValidation(err), "scheme %q must be a validation error", tc.scheme)
			continue
		}
		require.NoError(t, err, "scheme %q", tc.scheme)
		assert.Equal(t, tc.want, m.pdsEndpoint(), "scheme %q", tc.scheme)
	}
}
