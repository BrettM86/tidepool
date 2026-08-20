package personas

import (
	"database/sql"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/identity"
	"tidepool/internal/store"
)

// probeHost is the authority the origin-spelling matrix mints under. It is not
// coves.social so a broken canonicalization here cannot borrow a row minted by
// another test.
const probeHost = "probe.example"

// originSpellings is every legal AP_USER_ORIGIN spelling of ONE authority,
// including the two MIRROR pairs (http on :443, https on :80) that the
// scheme-aware canonicalizer used to keep and the Host router always folds.
var originSpellings = []string{
	"https://" + probeHost,
	"https://" + probeHost + ":443",
	"https://" + probeHost + ":80",
	"http://" + probeHost,
	"http://" + probeHost + ":80",
	"http://" + probeHost + ":443",
}

// hostSpellings is every way that authority can arrive in a Host header once a
// proxy, a client, or a resolver has had its say.
var hostSpellings = []string{
	probeHost,
	probeHost + ":443",
	probeHost + ":80",
	"PROBE.EXAMPLE",
	probeHost + ".",
}

// newServiceOnOrigin builds a Service minting under an arbitrary origin.
func newServiceOnOrigin(t *testing.T, database *sql.DB, origin string) *Service {
	t.Helper()
	custodian, err := identity.NewCustodian(testKEK)
	require.NoError(t, err)
	svc, err := New(Options{DB: database, Custodian: custodian, UserOrigin: origin})
	require.NoError(t, err, "origin %q must be accepted", origin)
	return svc
}

// TestCanonicalizeOriginRoundTripsThroughHostRouting is the property the
// ":443 bricks actors forever" trap violated: whatever CanonicalizeOrigin
// freezes into normalized_origin must be a FIXED POINT of the normalization
// the Host router applies to every incoming request. If the two disagree by so
// much as a port, the minted actor answers under no Host spelling at all, and
// actor_id is frozen at mint, so the mistake is permanent.
func TestCanonicalizeOriginRoundTripsThroughHostRouting(t *testing.T) {
	for _, raw := range originSpellings {
		t.Run(raw, func(t *testing.T) {
			_, host, err := CanonicalizeOrigin(raw)
			require.NoError(t, err)
			assert.Equal(t, host, normalizeHost(host),
				"canonical host %q must survive the router's own normalization, "+
					"or every minted actor 404s under every Host spelling", host)
		})
	}
}

// TestCanonicalizeOriginKeepsPortlessHTTPSUnchanged pins the production
// spellings. Rows already minted under these must keep resolving: widening the
// port fold must be a NO-OP here, or the fix is itself a permanent break.
func TestCanonicalizeOriginKeepsPortlessHTTPSUnchanged(t *testing.T) {
	for _, tc := range []struct{ raw, wantOrigin, wantHost string }{
		{"https://coves.social", "https://coves.social", "coves.social"},
		{"https://tdpl.io", "https://tdpl.io", "tdpl.io"},
		{"http://localhost:8091", "http://localhost:8091", "localhost:8091"},
		{"https://coves.social:8443", "https://coves.social:8443", "coves.social:8443"},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			origin, host, err := CanonicalizeOrigin(tc.raw)
			require.NoError(t, err)
			assert.Equal(t, tc.wantOrigin, origin)
			assert.Equal(t, tc.wantHost, host)
		})
	}
}

// TestMintedActorServesUnderEveryHostSpelling is the review's probe, driven
// over HTTP. AP_USER_ORIGIN="http://probe.example:443" is legal and config
// accepts it; before the fix it minted normalized_origin "probe.example:443",
// which normalizeHost folds away on EVERY request, so the actor was
// permanently unreachable under every spelling of its own name.
func TestMintedActorServesUnderEveryHostSpelling(t *testing.T) {
	database := personasTestDB(t)
	for _, origin := range originSpellings {
		t.Run(origin, func(t *testing.T) {
			svc := newServiceOnOrigin(t, database, origin)
			actor, err := svc.CreateActorForDID(t.Context(), testDID(t), "alice."+probeHost)
			require.NoError(t, err)
			assert.Equal(t, probeHost, actor.NormalizedOrigin,
				"every spelling of one authority must mint ONE namespace")

			for _, host := range hostSpellings {
				rec := serveOnHost(svc, host, http.MethodGet, actorPath(actor.DID), nil)
				require.Equal(t, http.StatusOK, rec.Code,
					"actor minted under %q must serve under Host %q, body=%s",
					origin, host, rec.Body.String())
				assert.Equal(t, actor.ActorID, decodeJSON(t, rec)["id"])
			}
		})
	}
}

// insertCorruptActor writes an ap_actors row directly, bypassing the mint path,
// so serving is exercised against a row that no longer satisfies the invariant
// CreateActorForDID establishes.
func insertCorruptActor(t *testing.T, database *sql.DB, localPart, actorID string) *store.APActor {
	t.Helper()
	actors := store.NewAPActors(database)
	created, err := actors.Create(t.Context(), store.APActor{
		DID:              testDID(t),
		Kind:             store.ActorTypePerson,
		ActorID:          actorID,
		NormalizedOrigin: userHost,
		LocalPart:        localPart,
		RSAKeySealed:     []byte("sealed"),
		RSAKeyVersion:    currentRSAKeyVersion,
		PublicKeyPEM:     "-----BEGIN PUBLIC KEY-----\nnot-a-key\n-----END PUBLIC KEY-----\n",
	})
	require.NoError(t, err)
	return created
}

// TestServingFailsClosedOnCorruptActorIDEverywhere: the fail-closed actor_id
// guard has to cover EVERY surface that publishes the value, not just the actor
// document. The outbox publishes actor_id+"/outbox" and WebFinger publishes it
// as the alias AND both rel=self hrefs — the href every remote resolver caches.
// A 200 from either one is the cross-authority claim the guard exists to stop,
// cached by every peer that asked.
func TestServingFailsClosedOnCorruptActorIDEverywhere(t *testing.T) {
	database := personasTestDB(t)
	svc, _ := newTestService(t, database)

	for _, tc := range []struct {
		name      string
		localPart string
		actorID   string
	}{
		{
			name:      "actor_id does not parse as a URL",
			localPart: "unparseable",
			actorID:   "://not a url",
		},
		{
			name:      "actor_id is not absolute",
			localPart: "relative",
			actorID:   "/ap/actor/did:plc:whatever",
		},
		{
			// The row claims this origin hosts it, but its id names another
			// authority: publishing that pairs OUR inbox and key with THEIR id.
			name:      "actor_id belongs to a foreign authority",
			localPart: "foreign",
			actorID:   "https://evil.example/ap/actor/did:plc:whatever",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actor := insertCorruptActor(t, database, tc.localPart, tc.actorID)

			for _, target := range []string{
				actorPath(actor.DID),
				actorPath(actor.DID) + "/outbox",
				webfingerTarget("acct:" + tc.localPart + "@" + userHost),
			} {
				rec := serveOnUserOrigin(svc, http.MethodGet, target, nil)
				assert.NotEqual(t, http.StatusOK, rec.Code,
					"%s must refuse a corrupt actor_id, not publish it: body=%s",
					target, rec.Body.String())
			}
		})
	}
}

// TestCreateActorForDIDRefusesUnservableDID: the DID is frozen into actor_id,
// keyId, the WebFinger href, and the signing identity — every bit as
// irreversibly as the handle-derived local part, which gets a full syntax gate.
// A DID that cannot round-trip through the serving path mints an actor that is
// permanently unfetchable, and re-minting it is not possible.
func TestCreateActorForDIDRefusesUnservableDID(t *testing.T) {
	database := personasTestDB(t)
	svc, _ := newTestService(t, database)

	for _, tc := range []struct{ name, did string }{
		{"empty", ""},
		{"not a DID at all", "alice"},
		{"wrong scheme", "web:example.com"},
		// ServeHTTP 404s any /ap/actor/ rest containing "/", so this actor
		// could never serve its own document.
		{"contains a path separator", "did:plc:abc/evil"},
		// Stored verbatim, but r.URL.Path arrives percent-DECODED, so the
		// lookup by the served path can never find this row again.
		{"percent-encoded", "did:web:example.com%3A8080"},
		{"whitespace", "did:plc:abc def"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actor, err := svc.CreateActorForDID(t.Context(), tc.did, testHandle)
			require.Error(t, err, "minting %q freezes an unservable identity", tc.did)
			assert.Nil(t, actor)
			assert.True(t, errors.IsValidation(err),
				"a malformed DID is permanent, not retryable: got %#v", err)

			_, getErr := svc.actors.GetByDID(t.Context(), tc.did)
			assert.True(t, errors.IsNotFound(getErr),
				"the row must be refused BEFORE keygen and INSERT, got %v", getErr)
		})
	}
}
