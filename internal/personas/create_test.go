package personas

import (
	"bytes"
	"context"
	"crypto/rsa"
	"database/sql"
	stderrors "errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/identity"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// personasTestDB returns a migrated connection with ap_actors emptied, so
// the collision tests start from a known namespace.
func personasTestDB(t *testing.T) *sql.DB {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database, "ap_actors")
	return database
}

func newTestService(t *testing.T, database *sql.DB) (*Service, *identity.Custodian) {
	t.Helper()
	custodian, err := identity.NewCustodian(testKEK)
	require.NoError(t, err)
	svc, err := New(Options{DB: database, Custodian: custodian, UserOrigin: userOrigin})
	require.NoError(t, err)
	require.NotNil(t, svc, "personas.New must return a service")
	return svc, custodian
}

// TestCreateActorForDID_Mint is the happy path: one call turns a Coves DID
// into a complete, sealed, persisted AP identity.
func TestCreateActorForDID_Mint(t *testing.T) {
	database := personasTestDB(t)
	svc, custodian := newTestService(t, database)
	ctx := t.Context()
	did := testDID(t)

	actor, err := svc.CreateActorForDID(ctx, did, testHandle)
	require.NoError(t, err)
	require.NotNil(t, actor, "CreateActorForDID must return the minted actor")

	assert.Equal(t, did, actor.DID)
	assert.Equal(t, store.ActorTypePerson, actor.Kind,
		"Person, not Service: Lemmy rejects votes from Service actors as bots")
	assert.Equal(t, userOrigin+"/ap/actor/"+did, actor.ActorID,
		"actor_id is the full URL, origin included")
	assert.Equal(t, userHost, actor.NormalizedOrigin,
		"normalized_origin is the scheme-less lowercase host — exactly what Host routing yields")
	assert.Equal(t, testLocalPart, actor.LocalPart)
	assert.True(t, actor.Enabled, "federation is default-on")

	// The private half exists only as ciphertext.
	require.NotEmpty(t, actor.RSAKeySealed, "the minted key must be sealed into the row")
	assert.NotContains(t, string(actor.RSAKeySealed), "-----BEGIN",
		"the RSA private key must never be stored unsealed")
	assert.Equal(t, 1, actor.RSAKeyVersion, "the first key is version 1")

	// The published half is real, and it is the public half of the sealed key.
	published, err := ap.ParsePublicKeyPEM([]byte(actor.PublicKeyPEM))
	require.NoError(t, err, "public_key_pem must parse")
	opened, err := custodian.DecryptActorRSAKey(did, actor.RSAKeySealed)
	require.NoError(t, err, "the custodian must open the sealed key under its DID")
	require.NotNil(t, opened)
	assert.True(t, opened.PublicKey.Equal(published),
		"the published key must be the public half of the sealed private key")
	assert.Equal(t, ap.ServiceKeyBits, opened.N.BitLen(), "Lemmy expects 2048-bit RSA")

	// The row is persisted, not just returned.
	stored, err := store.NewAPActors(database).GetByDID(ctx, did)
	require.NoError(t, err, "the minted actor must be readable from ap_actors")
	require.NotNil(t, stored)
	assert.Equal(t, actor.ActorID, stored.ActorID)
	assert.Equal(t, actor.LocalPart, stored.LocalPart)
	assert.Equal(t, actor.NormalizedOrigin, stored.NormalizedOrigin)
	assert.True(t, bytes.Equal(actor.RSAKeySealed, stored.RSAKeySealed))
	assert.Equal(t, actor.PublicKeyPEM, stored.PublicKeyPEM)
}

// TestCreateActorForDID_Idempotent covers the lazy get-or-create contract:
// task 14/16 call this on every federating interaction, and a handle change
// must not re-mint or re-derive anything.
func TestCreateActorForDID_Idempotent(t *testing.T) {
	database := personasTestDB(t)
	svc, _ := newTestService(t, database)
	ctx := t.Context()
	did := testDID(t)

	first, err := svc.CreateActorForDID(ctx, did, testHandle)
	require.NoError(t, err)
	require.NotNil(t, first)

	// Same DID, DIFFERENT handle: the user renamed. The local part is
	// FROZEN — federated mentions of @alice@coves.social must keep
	// resolving — so this returns the existing row untouched.
	second, err := svc.CreateActorForDID(ctx, did, "alicia.coves.social")
	require.NoError(t, err, "get-or-create must be idempotent")
	require.NotNil(t, second)

	assert.Equal(t, first.LocalPart, second.LocalPart,
		"the local part is frozen at creation; a handle change must not re-derive it")
	assert.Equal(t, testLocalPart, second.LocalPart)
	assert.Equal(t, first.ActorID, second.ActorID)
	assert.True(t, bytes.Equal(first.RSAKeySealed, second.RSAKeySealed),
		"re-minting a key would invalidate every signature the old one made")
	assert.Equal(t, first.CreatedAt, second.CreatedAt)

	assert.Equal(t, 1, countActors(t, database, did), "exactly one row per DID")
}

// TestCreateActorForDID_CollisionSuffix pins the suffix FORM: the first
// claimant keeps the bare local part, later ones get -2, -3, ...
func TestCreateActorForDID_CollisionSuffix(t *testing.T) {
	database := personasTestDB(t)
	svc, _ := newTestService(t, database)
	ctx := t.Context()

	firstDID, secondDID, thirdDID := testDID(t), testDID(t), testDID(t)

	first, err := svc.CreateActorForDID(ctx, firstDID, testHandle)
	require.NoError(t, err)
	require.NotNil(t, first)
	assert.Equal(t, "alice", first.LocalPart)

	second, err := svc.CreateActorForDID(ctx, secondDID, testHandle)
	require.NoError(t, err)
	require.NotNil(t, second)
	assert.Equal(t, "alice-2", second.LocalPart)
	assert.Equal(t, userOrigin+"/ap/actor/"+secondDID, second.ActorID,
		"the actor URL keys on the DID; only the local part is suffixed")

	third, err := svc.CreateActorForDID(ctx, thirdDID, testHandle)
	require.NoError(t, err)
	require.NotNil(t, third)
	assert.Equal(t, "alice-3", third.LocalPart)

	// Each is independently resolvable under its own local part.
	actors := store.NewAPActors(database)
	for _, want := range []*store.APActor{first, second, third} {
		got, err := actors.GetByOriginLocalPart(ctx, userHost, want.LocalPart)
		require.NoError(t, err, "webfinger must resolve %q", want.LocalPart)
		require.NotNil(t, got)
		assert.Equal(t, want.DID, got.DID)
	}
}

// TestCreateActorForDID_ConcurrentDistinctDIDs proves the suffix search is
// driven by the unique violation, not by a SELECT-then-INSERT pre-check: N
// simultaneous mints of the same derived local part must all land, each with
// its own name.
func TestCreateActorForDID_ConcurrentDistinctDIDs(t *testing.T) {
	database := personasTestDB(t)
	svc, _ := newTestService(t, database)
	ctx := t.Context()

	const racers = 5
	dids := make([]string, racers)
	for i := range dids {
		dids[i] = testDID(t)
	}

	results := make([]*store.APActor, racers)
	failures := make([]error, racers)
	var wg sync.WaitGroup
	for i := range dids {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], failures[i] = svc.CreateActorForDID(ctx, dids[i], testHandle)
		}(i)
	}
	wg.Wait()

	seen := make(map[string]string, racers)
	for i, err := range failures {
		require.NoError(t, err, "concurrent mint %d must not fail", i)
		require.NotNil(t, results[i], "concurrent mint %d returned no actor", i)
		local := results[i].LocalPart
		if other, dup := seen[local]; dup {
			require.Failf(t, "duplicate local part",
				"%s and %s both claimed %q", other, dids[i], local)
		}
		seen[local] = dids[i]
		assert.True(t, local == "alice" || strings.HasPrefix(local, "alice-"),
			"every racer derives from alice, got %q", local)
	}
	require.Len(t, seen, racers, "each racer must end up with its own local part")
	assert.Contains(t, seen, "alice", "exactly one racer keeps the bare local part")
}

// TestCreateActorForDID_ConcurrentSameDID: the get-or-create race. Two
// callers minting the same DID must converge on one row — losing the insert
// must return the winner's actor, not an error and not a second key.
func TestCreateActorForDID_ConcurrentSameDID(t *testing.T) {
	database := personasTestDB(t)
	svc, _ := newTestService(t, database)
	ctx := t.Context()
	did := testDID(t)

	var (
		wg      sync.WaitGroup
		actors  [2]*store.APActor
		failure [2]error
	)
	for i := range actors {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			actors[i], failure[i] = svc.CreateActorForDID(ctx, did, testHandle)
		}(i)
	}
	wg.Wait()

	require.NoError(t, failure[0])
	require.NoError(t, failure[1], "the loser of the mint race must get the winner's row")
	require.NotNil(t, actors[0])
	require.NotNil(t, actors[1])
	assert.Equal(t, actors[0].ActorID, actors[1].ActorID)
	assert.Equal(t, actors[0].LocalPart, actors[1].LocalPart)
	assert.True(t, bytes.Equal(actors[0].RSAKeySealed, actors[1].RSAKeySealed),
		"both callers must see the same key: a second key would orphan signatures")
	assert.Equal(t, 1, countActors(t, database, did))
}

// TestActorSigner signs with the sealed key and verifies against the
// PUBLISHED key. The resolver is a stub on purpose: the outer acceptance
// test owns the real client/served-document path.
func TestActorSigner(t *testing.T) {
	database := personasTestDB(t)
	svc, _ := newTestService(t, database)
	ctx := t.Context()
	did := testDID(t)

	actor, err := svc.CreateActorForDID(ctx, did, testHandle)
	require.NoError(t, err)
	require.NotNil(t, actor)

	signer, err := svc.actorSigner(ctx, did)
	require.NoError(t, err)
	require.NotNil(t, signer, "actorSigner must return a signer for a minted actor")
	require.Equal(t, actor.ActorID+"#main-key", signer.KeyID(),
		"the keyId must be the one the actor document publishes")

	published, err := ap.ParsePublicKeyPEM([]byte(actor.PublicKeyPEM))
	require.NoError(t, err)
	verifier := ap.NewVerifier(ap.KeyResolverFunc(
		func(_ context.Context, keyID string) (*rsa.PublicKey, string, error) {
			require.Equal(t, actor.ActorID+"#main-key", keyID)
			return published, actor.ActorID, nil
		}))

	body := []byte(`{"@context":"https://www.w3.org/ns/activitystreams","type":"Create"}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		userOrigin+"/ap/inbox", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", ap.ContentTypeActivityJSON)
	require.NoError(t, signer.SignRequest(req, body))

	verified, err := verifier.Verify(ctx, req, body)
	require.NoError(t, err, "a signature from the unsealed key must verify against the published key")
	assert.Equal(t, actor.ActorID, verified)

	// An unminted DID has no key to hand out.
	_, err = svc.actorSigner(ctx, testDID(t))
	assert.True(t, errors.IsNotFound(err), "unknown DID must be IsNotFound, got %v", err)
}

// TestCreateActorForDID_NoProfileFetch pins that minting is local-only: the
// profile cache starts empty (task 14 owns the sync) and nothing reaches the
// network — this harness has no fixture server, so an egress attempt would
// stall or fail rather than quietly succeed.
func TestCreateActorForDID_NoProfileFetch(t *testing.T) {
	database := personasTestDB(t)
	svc, _ := newTestService(t, database)
	did := testDID(t)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	start := time.Now()
	actor, err := svc.CreateActorForDID(ctx, did, testHandle)
	require.NoError(t, err, "minting must not depend on a reachable appview/PDS")
	require.NotNil(t, actor)
	assert.Less(t, time.Since(start), 5*time.Second,
		"minting is a local operation: key generation plus one INSERT")

	assert.Empty(t, actor.DisplayName, "the profile cache is task 14's to fill")
	assert.Empty(t, actor.Summary)
	assert.Empty(t, actor.AvatarURL)
}

func countActors(t *testing.T, database *sql.DB, did string) int {
	t.Helper()
	var count int
	require.NoError(t, database.QueryRowContext(context.Background(),
		`SELECT count(*) FROM ap_actors WHERE did = $1`, did).Scan(&count))
	return count
}

// TestNew_CanonicalizesUserOrigin: config canonicalizes AP_USER_ORIGIN, but
// personas.New is also constructed directly (tests, future callers), so it
// normalizes defensively. A ":443" that slipped through would mint actors
// under a SECOND namespace — normalized_origin "coves.social:443" — that
// Host routing, which sees the canonical authority, could never resolve.
func TestNew_CanonicalizesUserOrigin(t *testing.T) {
	database := personasTestDB(t)
	custodian, err := identity.NewCustodian(testKEK)
	require.NoError(t, err)

	svc, err := New(Options{
		DB:         database,
		Custodian:  custodian,
		UserOrigin: "https://coves.social:443",
	})
	require.NoError(t, err)
	require.NotNil(t, svc)

	did := testDID(t)
	actor, err := svc.CreateActorForDID(t.Context(), did, testHandle)
	require.NoError(t, err)
	require.NotNil(t, actor)

	assert.Equal(t, userHost, actor.NormalizedOrigin,
		"the default https port is not part of the authority webfinger routes on")
	assert.Equal(t, userOrigin+"/ap/actor/"+did, actor.ActorID,
		"the actor id must carry the canonical origin: it is frozen at mint")
}

// TestCreateActorForDID_NamespaceExhaustion: the suffix search is bounded,
// and the error at the end must not read as a uniqueness conflict. A caller
// treating IsAlreadyExists as "someone else won, re-read the row" would
// spin forever on a name that has no free suffix left.
func TestCreateActorForDID_NamespaceExhaustion(t *testing.T) {
	database := personasTestDB(t)
	svc, _ := newTestService(t, database)
	actors := store.NewAPActors(database)
	ctx := t.Context()

	// Seed the whole suffix range directly: alice, alice-2 ... alice-99.
	// Going through CreateActorForDID would generate 99 RSA keys for no
	// added coverage.
	for attempt := 1; attempt <= 99; attempt++ {
		local := testLocalPart
		if attempt > 1 {
			local = fmt.Sprintf("%s-%d", testLocalPart, attempt)
		}
		did := testDID(t)
		_, err := actors.Create(ctx, store.APActor{
			DID:              did,
			Kind:             store.ActorTypePerson,
			ActorID:          userOrigin + "/ap/actor/" + did,
			NormalizedOrigin: userHost,
			LocalPart:        local,
			RSAKeySealed:     []byte{0x01, 0x02, 0x03},
			RSAKeyVersion:    1,
			PublicKeyPEM:     "seeded",
		})
		require.NoError(t, err, "seed %q", local)
	}

	_, err := svc.CreateActorForDID(ctx, testDID(t), testHandle)
	require.Error(t, err, "the 100th claimant of one name has nowhere to go")
	assert.True(t, stderrors.Is(err, ErrLocalPartExhausted),
		"exhaustion is a branchable condition, not a message to grep: got %v", err)
	assert.False(t, errors.IsAlreadyExists(err),
		"exhaustion is not a conflict a caller can resolve by re-reading: got %v", err)
}
