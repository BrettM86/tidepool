package identity

import (
	"bytes"
	"context"
	"crypto/rsa"
	"database/sql"
	"testing"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/repo"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// The KEK rotation drill, end to end.
//
// Run 1 gave the bridge a second key to READ under (BRIDGE_KEK_PREVIOUS), so
// an operator could change BRIDGE_KEK without orphaning material sealed under
// the old one. That is a half-rotation: until every sealed blob has been
// moved onto the current key, the previous KEK is still load-bearing, and an
// operator who unsets it loses the escrow rotation key, every bridged actor's
// signing key, and every Coves user's AP signing key at once.
//
// Reseal is the other half. These tests are written from the operator's seat:
// what they must be able to do afterwards (drop BRIDGE_KEK_PREVIOUS and boot
// on the current key alone), and what the report must tell them before they
// dare to.
//
// Three domains hold KEK-sealed bytes, each with its own AAD:
//   - bridged_actors.signing_key   nullable BYTEA, actor-signing-key:v1:<did>
//   - ap_actors.rsa_key_sealed     NOT NULL BYTEA, actor-rsa-key:v1:<did>
//   - service_keys 'plc-rotation'  sealed, plc-rotation-key:v1
//
// And one row that must never be touched: service_keys 'service-actor' lives
// in the SAME column as the rotation key but holds PLAINTEXT PEM (migration
// 013 documents the per-row encoding). Re-sealing it would encrypt a value
// every reader parses as PEM; even counting it would tell an operator the
// walk covers a row it must not.

const (
	// The live bridged actor: still consented, signs commits every day.
	resealLiveDID = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	// The tombstoned bridged actor. UpsertActor's DO UPDATE freezes deleted
	// rows on purpose, so the re-seal walk cannot go through it — but the
	// key must still move, because scrubbing a deleted actor's records is
	// exactly what its key is still held for.
	resealTombstonedDID = "did:plc:44ybard66vv44zksje25o7dz"
	// A bridged actor with no escrowed key at all (NULL signing_key): there
	// is nothing to re-seal, and nothing wrong either.
	resealKeylessDID = "did:plc:z72i7hdynmk6r22z27h6tvur"
	// The Coves user whose AP-side RSA key is sealed in ap_actors.
	resealAPActorDID = "did:plc:7iza6de2dwap2sbkpav7c6c6"

	resealLiveAPActorID       = "https://lemmy.world/u/alice"
	resealTombstonedAPActorID = "https://lemmy.world/u/bob"
	resealKeylessAPActorID    = "https://lemmy.world/u/carol"
)

// resealFixture is the seeded pre-rotation world plus the plaintext originals
// every post-drill assertion compares against.
type resealFixture struct {
	database    *sql.DB
	actors      store.BridgedActors
	apActors    store.APActors
	serviceKeys store.ServiceKeys

	liveKey       *atcrypto.PrivateKeyK256
	tombstonedKey *atcrypto.PrivateKeyK256
	apRSAKey      *rsa.PrivateKey
	rotationKey   *atcrypto.PrivateKeyK256
	// serviceActorPEM is the plaintext the untouchable row must still hold.
	serviceActorPEM []byte
}

// seedRotationDrill builds a database as it stands the moment before an
// operator rotates: everything sealed under KEK A, through the same store
// paths production writes through.
func seedRotationDrill(t *testing.T) *resealFixture {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database, "bridged_actors", "ap_actors", "service_keys")
	ctx := t.Context()

	custodianA, err := NewCustodian(previousTestKEK())
	require.NoError(t, err)

	fixture := &resealFixture{
		database:    database,
		actors:      store.NewBridgedActors(database),
		apActors:    store.NewAPActors(database),
		serviceKeys: store.NewServiceKeys(database),
	}

	// Two escrowed bridged actors.
	fixture.liveKey = seedBridgedActor(t, ctx, fixture.actors, custodianA,
		resealLiveAPActorID, resealLiveDID, "alice.lemmy-world.tidepool.example")
	fixture.tombstonedKey = seedBridgedActor(t, ctx, fixture.actors, custodianA,
		resealTombstonedAPActorID, resealTombstonedDID, "bob.lemmy-world.tidepool.example")
	require.NoError(t, fixture.actors.SetConsentState(ctx, resealTombstonedAPActorID, store.ConsentStateDeleted))

	// One with no escrowed key: minted before escrow, or opted out before a
	// key was ever cut.
	_, err = fixture.actors.UpsertActor(ctx, store.BridgedActor{
		APActorID:    resealKeylessAPActorID,
		ActorType:    store.ActorTypePerson,
		DID:          resealKeylessDID,
		Handle:       "carol.lemmy-world.tidepool.example",
		ConsentState: store.ConsentStateOK,
	})
	require.NoError(t, err)
	require.Nil(t, storedSigningKey(t, database, resealKeylessDID),
		"the fixture's keyless actor must really have a NULL signing_key, or the skip case is never exercised")

	// One Coves user's AP-side RSA key.
	fixture.apRSAKey = testRSAKey(t)
	sealedRSA, err := custodianA.EncryptActorRSAKey(resealAPActorDID, fixture.apRSAKey)
	require.NoError(t, err)
	_, err = fixture.apActors.Create(ctx, store.APActor{
		DID:              resealAPActorDID,
		Kind:             store.ActorTypePerson,
		ActorID:          "https://coves.social/ap/actor/" + resealAPActorDID,
		NormalizedOrigin: "coves.social",
		LocalPart:        "dave",
		RSAKeySealed:     sealedRSA,
		RSAKeyVersion:    1,
		PublicKeyPEM:     "-----BEGIN PUBLIC KEY-----\nTEST\n-----END PUBLIC KEY-----\n",
	})
	require.NoError(t, err)

	// The escrow rotation key, through its real boot path.
	fixture.rotationKey, err = LoadOrCreateRotationKey(ctx, fixture.serviceKeys, custodianA)
	require.NoError(t, err)

	// And the row that shares that table and must never be re-sealed: the
	// AP service-actor key, plaintext PEM, seeded the way ap seeds it.
	serviceRSA, err := ap.GenerateRSAKey()
	require.NoError(t, err)
	fixture.serviceActorPEM, err = ap.EncodePrivateKeyPEM(serviceRSA)
	require.NoError(t, err)
	_, err = fixture.serviceKeys.Create(ctx, ap.ServiceKeyName, fixture.serviceActorPEM)
	require.NoError(t, err)
	require.Contains(t, string(storedServiceKeyMaterial(t, database, ap.ServiceKeyName)), "-----BEGIN",
		"the service-actor fixture must be plaintext PEM at rest, or the 'never touch it' assertions prove nothing")

	return fixture
}

// seedBridgedActor mints a signing key, seals it under custodian, and stores
// the actor through the production upsert. It returns the plaintext key.
func seedBridgedActor(t *testing.T, ctx context.Context, actors store.BridgedActors, custodian *Custodian, apActorID, did, handle string) *atcrypto.PrivateKeyK256 {
	t.Helper()
	key, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	sealed, err := custodian.EncryptActorKey(did, key)
	require.NoError(t, err)
	_, err = actors.UpsertActor(ctx, store.BridgedActor{
		APActorID:           apActorID,
		ActorType:           store.ActorTypePerson,
		DID:                 did,
		Handle:              handle,
		SigningKeyEncrypted: sealed,
		ConsentState:        store.ConsentStateOK,
	})
	require.NoError(t, err)
	return key
}

// storedSigningKey reads bridged_actors.signing_key raw, so a test can prove
// bytes changed (or did not) without going through a custodian.
func storedSigningKey(t *testing.T, database *sql.DB, did string) []byte {
	t.Helper()
	var raw []byte
	require.NoError(t, database.QueryRow(
		`SELECT signing_key FROM bridged_actors WHERE did = $1`, did).Scan(&raw))
	return raw
}

func storedRSAKeySealed(t *testing.T, database *sql.DB, did string) []byte {
	t.Helper()
	var raw []byte
	require.NoError(t, database.QueryRow(
		`SELECT rsa_key_sealed FROM ap_actors WHERE did = $1`, did).Scan(&raw))
	return raw
}

func storedServiceKeyMaterial(t *testing.T, database *sql.DB, name string) []byte {
	t.Helper()
	var raw []byte
	require.NoError(t, database.QueryRow(
		`SELECT key_material FROM service_keys WHERE name = $1`, name).Scan(&raw))
	return raw
}

// currentOnly is the custodian the operator ends up with: BRIDGE_KEK set to
// the new key, BRIDGE_KEK_PREVIOUS unset. Every post-drill read goes through
// one of these, because that — not the report — is the state the operator is
// betting the deployment on.
func currentOnly(t *testing.T) *Custodian {
	t.Helper()
	custodian, err := NewCustodian(currentTestKEK())
	require.NoError(t, err)
	return custodian
}

// requireSigningKeyEquals resolves a DID's escrow key through the production
// resolver and asserts it is the original.
func requireSigningKeyEquals(t *testing.T, ctx context.Context, actorKeys *ActorKeys, did string, use repo.KeyUse, want *atcrypto.PrivateKeyK256, msg string) {
	t.Helper()
	got, err := actorKeys.SigningKey(ctx, did, use)
	require.NoError(t, err, msg)
	gotK256, ok := got.(*atcrypto.PrivateKeyK256)
	require.True(t, ok, "signing key for %s must be a K256 key, got %T", did, got)
	assert.True(t, bytes.Equal(want.Bytes(), gotK256.Bytes()), msg)
}

// TestReseal_RotationDrill is the acceptance contract.
//
// GIVEN a database sealed entirely under KEK A, WHEN the operator sets
// BRIDGE_KEK=B with BRIDGE_KEK_PREVIOUS=A and runs the drill, THEN every
// sealed blob opens under B ALONE — the same plaintext as before — the
// plaintext service-actor row is untouched, and a second run reports zero
// work left to do.
func TestReseal_RotationDrill(t *testing.T) {
	fixture := seedRotationDrill(t)
	ctx := t.Context()
	kekA, kekB := previousTestKEK(), currentTestKEK()

	// NEGATIVE CONTROL: under B alone, the pre-rotation world is unreadable.
	// Without this the drill could pass vacuously against a database that was
	// already sealed under the current key.
	beforeKeys := NewActorKeys(fixture.actors, currentOnly(t))
	_, err := beforeKeys.SigningKey(ctx, resealLiveDID, repo.KeyUseWrite)
	require.Error(t, err,
		"before the drill the live actor's key must NOT open under the new KEK alone; if it does, this test is not exercising a rotation")
	_, err = beforeKeys.SigningKey(ctx, resealTombstonedDID, repo.KeyUseDelete)
	require.Error(t, err,
		"before the drill the tombstoned actor's key must NOT open under the new KEK alone")
	_, err = currentOnly(t).DecryptActorRSAKey(resealAPActorDID, storedRSAKeySealed(t, fixture.database, resealAPActorDID))
	require.Error(t, err,
		"before the drill the AP RSA key must NOT open under the new KEK alone")
	_, err = LoadOrCreateRotationKey(ctx, fixture.serviceKeys, currentOnly(t))
	require.Error(t, err,
		"before the drill the escrow rotation key must NOT open under the new KEK alone — this is the boot canary that stops the process")

	// THE DRILL.
	report, err := Reseal(ctx, fixture.database, kekB, kekA)
	require.NoError(t, err,
		"a clean database must re-seal without error; an operator who sees an error here has no way to tell a broken row from a broken tool")
	require.NotNil(t, report, "the drill must hand back an inventory, not a bare nil")

	// 1. The inventory. Every row is accounted for in exactly one bucket, and
	// the plaintext service-actor row is in none of them.
	assert.Equal(t, ResealCounts{Resealed: 2, AlreadyCurrent: 0, Skipped: 1, Failed: 0}, report.BridgedActors,
		"both escrowed bridged actors must be re-sealed and the NULL-key one skipped; a skip counted as a failure sends an operator hunting for corruption that does not exist, and a skip counted as a re-seal claims work that never happened")
	assert.Equal(t, ResealCounts{Resealed: 1, AlreadyCurrent: 0, Skipped: 0, Failed: 0}, report.APActors,
		"the Coves user's AP signing key must be re-sealed; leaving it behind means the user cannot sign a single outbound request once BRIDGE_KEK_PREVIOUS is unset")
	assert.Equal(t, ResealCounts{Resealed: 1, AlreadyCurrent: 0, Skipped: 0, Failed: 0}, report.ServiceKeys,
		"exactly ONE service_keys row is sealed material — plc-rotation. The service-actor row is plaintext PEM in the same column and must not appear in any bucket, not even Skipped: a count that includes it tells the operator the walk covers a row it must never write")
	assert.Empty(t, report.Failures,
		"a clean rotation must report no failures; a spurious failure is indistinguishable to the operator from a real one and stops the rotation dead")

	// 2. The state the operator is actually betting on: BRIDGE_KEK_PREVIOUS
	// gone, everything still readable, and the SAME key material as before.
	afterKeys := NewActorKeys(fixture.actors, currentOnly(t))
	requireSigningKeyEquals(t, ctx, afterKeys, resealLiveDID, repo.KeyUseWrite, fixture.liveKey,
		"after the drill the live actor's signing key must open under the current KEK alone and be the ORIGINAL key; a different key means its repo can never be signed for again")

	// The tombstoned row is the one the store's upsert refuses to touch. Its
	// key is still held for exactly one purpose — scrubbing the deleted
	// actor's records — so a re-seal that skipped frozen rows would strand
	// that obligation the moment the old KEK is retired.
	requireSigningKeyEquals(t, ctx, afterKeys, resealTombstonedDID, repo.KeyUseDelete, fixture.tombstonedKey,
		"a tombstoned actor's key must survive the re-seal: it is what deletes their records, and a re-seal that cannot write frozen rows leaves the bridge unable to honour a deletion once BRIDGE_KEK_PREVIOUS is unset")

	// ... and the tombstone itself is unchanged. Re-sealing moves ciphertext,
	// never consent.
	_, err = afterKeys.SigningKey(ctx, resealTombstonedDID, repo.KeyUseWrite)
	assert.True(t, errors.IsTombstoned(err),
		"the write freeze on a tombstoned actor must survive the re-seal; a drill that un-freezes a deleted repo turns a key-management chore into a consent violation, got %v", err)
	tombstoned, err := fixture.actors.GetByDID(ctx, resealTombstonedDID)
	require.NoError(t, err)
	assert.Equal(t, store.ConsentStateDeleted, tombstoned.ConsentState,
		"re-sealing must not resurrect a deleted actor's consent state")

	// The keyless actor is still keyless: skipped means untouched, not filled in.
	assert.Nil(t, storedSigningKey(t, fixture.database, resealKeylessDID),
		"a NULL signing_key must still be NULL after the drill; inventing a key for an actor that never had one would hand the bridge a repo it was never given")

	// The AP-side key.
	openedRSA, err := currentOnly(t).DecryptActorRSAKey(resealAPActorDID, storedRSAKeySealed(t, fixture.database, resealAPActorDID))
	require.NoError(t, err,
		"after the drill the AP RSA key must open under the current KEK alone")
	assert.True(t, openedRSA.Equal(fixture.apRSAKey),
		"the re-sealed AP RSA key must be the ORIGINAL key; a different one silently breaks every HTTP signature this user sends, and peers reject them without telling us")

	// The escrow rotation key — the one that controls every bridged DID.
	rescued, err := LoadOrCreateRotationKey(ctx, fixture.serviceKeys, currentOnly(t))
	require.NoError(t, err,
		"after the drill the bridge must boot on the current KEK alone; if this fails the operator can never unset BRIDGE_KEK_PREVIOUS")
	assert.True(t, bytes.Equal(fixture.rotationKey.Bytes(), rescued.Bytes()),
		"the re-sealed rotation key must be the ORIGINAL key, not a freshly minted one: it is the rotation authority named in every bridged DID document, and replacing it is an unrecoverable loss of control over those DIDs")

	// 3. The row that must never be touched.
	assert.True(t, bytes.Equal(fixture.serviceActorPEM, storedServiceKeyMaterial(t, fixture.database, ap.ServiceKeyName)),
		"the service-actor row holds PLAINTEXT PEM in the same column as the sealed rotation key; sealing it would leave every reader parsing ciphertext as PEM and the bridge unable to sign a single AP request")

	// 4. The zero-run gate. The operator's decision to unset
	// BRIDGE_KEK_PREVIOUS rests on a re-run that finds nothing to do.
	afterFirst := struct {
		live, tombstoned, apRSA, rotation []byte
	}{
		live:       storedSigningKey(t, fixture.database, resealLiveDID),
		tombstoned: storedSigningKey(t, fixture.database, resealTombstonedDID),
		apRSA:      storedRSAKeySealed(t, fixture.database, resealAPActorDID),
		rotation:   storedServiceKeyMaterial(t, fixture.database, RotationKeyName),
	}

	second, err := Reseal(ctx, fixture.database, kekB, kekA)
	require.NoError(t, err, "a second run over an already-rotated database must be a no-op, not an error")
	require.NotNil(t, second)

	assert.Equal(t, ResealCounts{Resealed: 0, AlreadyCurrent: 2, Skipped: 1, Failed: 0}, second.BridgedActors,
		"the zero-run gate must read zero re-seals or the operator can never safely unset BRIDGE_KEK_PREVIOUS; a walk that re-seals rows it already moved can never converge and the gate never opens")
	assert.Equal(t, ResealCounts{Resealed: 0, AlreadyCurrent: 1, Skipped: 0, Failed: 0}, second.APActors,
		"an ap_actors row already under the current KEK must count as AlreadyCurrent, not as work done")
	assert.Equal(t, ResealCounts{Resealed: 0, AlreadyCurrent: 1, Skipped: 0, Failed: 0}, second.ServiceKeys,
		"the rotation key already under the current KEK must count as AlreadyCurrent")
	assert.Empty(t, second.Failures, "an already-rotated database has nothing to fail on")

	assert.True(t, bytes.Equal(afterFirst.live, storedSigningKey(t, fixture.database, resealLiveDID)),
		"AlreadyCurrent must mean the bytes were left alone; re-sealing an already-current blob churns ciphertext on every run and makes 'did anything change?' unanswerable from the database")
	assert.True(t, bytes.Equal(afterFirst.tombstoned, storedSigningKey(t, fixture.database, resealTombstonedDID)))
	assert.True(t, bytes.Equal(afterFirst.apRSA, storedRSAKeySealed(t, fixture.database, resealAPActorDID)))
	assert.True(t, bytes.Equal(afterFirst.rotation, storedServiceKeyMaterial(t, fixture.database, RotationKeyName)))
	assert.True(t, bytes.Equal(fixture.serviceActorPEM, storedServiceKeyMaterial(t, fixture.database, ap.ServiceKeyName)),
		"the plaintext service-actor row must survive every re-run untouched")
}

// TestReseal_UnreadableBlobsFailLoudlyAndInPlace covers the case that decides
// whether the drill is trustworthy: rows the walk CANNOT move.
//
// One blob is sealed under a KEK the bridge has never held (a restore from
// two rotations ago, or bytes pasted from another deployment); another is
// corrupt. Both must be reported, classified, and left exactly as they are —
// and the walk must still finish the rows it CAN move, because an operator
// needs one pass to produce the whole inventory. Above all it must exit with
// an error: a drill that reports failures in a struct but returns nil is a
// drill an operator's deploy script reads as success, and the next step of
// that script is unsetting BRIDGE_KEK_PREVIOUS.
func TestReseal_UnreadableBlobsFailLoudlyAndInPlace(t *testing.T) {
	fixture := seedRotationDrill(t)
	ctx := t.Context()
	kekA, kekB := previousTestKEK(), currentTestKEK()

	// A blob under a THIRD KEK — neither current nor previous.
	strangerCustodian, err := NewCustodian(strangerTestKEK())
	require.NoError(t, err)
	strandedKey, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	underC, err := strangerCustodian.EncryptActorKey(resealLiveDID, strandedKey)
	require.NoError(t, err)
	_, err = fixture.database.ExecContext(ctx,
		`UPDATE bridged_actors SET signing_key = $2 WHERE did = $1`, resealLiveDID, underC)
	require.NoError(t, err)

	// And a corrupt blob: full length, unknown version byte. It was never
	// decrypted under either key, so it is a storage problem, not a key one.
	corrupt := append([]byte(nil), storedRSAKeySealed(t, fixture.database, resealAPActorDID)...)
	corrupt[0] = 99
	_, err = fixture.database.ExecContext(ctx,
		`UPDATE ap_actors SET rsa_key_sealed = $2 WHERE did = $1`, resealAPActorDID, corrupt)
	require.NoError(t, err)

	report, err := Reseal(ctx, fixture.database, kekB, kekA)

	require.Error(t, err,
		"a re-seal that could not move every blob MUST exit with an error; a zero exit is the operator's signal to unset BRIDGE_KEK_PREVIOUS, and doing that here destroys the stranded rows for good")
	require.NotNil(t, report,
		"the inventory must come back WITH the error: an operator handed only an error has to re-run the drill to find out how much of the database is affected")

	// The walk keeps going past a bad row, so one pass is the whole picture.
	assert.Equal(t, ResealCounts{Resealed: 1, AlreadyCurrent: 0, Skipped: 1, Failed: 1}, report.BridgedActors,
		"one unreadable row must not abort the table: the tombstoned actor still had to be re-sealed and the NULL-key row still had to be skipped, or the operator's inventory stops at the first problem and understates the work left")
	assert.Equal(t, ResealCounts{Resealed: 0, AlreadyCurrent: 0, Skipped: 0, Failed: 1}, report.APActors,
		"a corrupt ap_actors blob is a failure, not a skip: skipping it would quietly drop a row from the rotation and the operator would unset the previous KEK believing it was covered")
	assert.Equal(t, ResealCounts{Resealed: 1, AlreadyCurrent: 0, Skipped: 0, Failed: 0}, report.ServiceKeys,
		"failures in one table must not stop another: the rotation key was readable and had to be moved")

	assert.ElementsMatch(t, []ResealFailure{
		{Table: "bridged_actors", ID: resealLiveDID, Reason: ResealFailureWrongKey},
		{Table: "ap_actors", ID: resealAPActorDID, Reason: ResealFailureMalformed},
	}, report.Failures,
		"each failure must name its table and row so the operator can go straight to it, and classify it: wrong-key sends them to key history (is there a third KEK we forgot?), malformed sends them to backups. Swapping the two classes sends them to the wrong place with a database in a half-rotated state")

	// Nothing the walk could not read may be written. A blob it cannot open
	// it cannot re-seal, and a half-written row would destroy the one copy
	// that a recovered third KEK could still have opened.
	assert.True(t, bytes.Equal(underC, storedSigningKey(t, fixture.database, resealLiveDID)),
		"a blob under an unknown KEK must be left byte-identical; if the drill overwrites it, finding the missing KEK later no longer helps and the actor's repo is lost")
	assert.True(t, bytes.Equal(corrupt, storedRSAKeySealed(t, fixture.database, resealAPActorDID)),
		"a corrupt blob must be left byte-identical, so restoring that single row from a backup is still a repair")

	// ... and everything the walk DID move is genuinely readable under the
	// current KEK alone. A count of 1 that is not backed by a working key is
	// worse than a count of 0.
	afterKeys := NewActorKeys(fixture.actors, currentOnly(t))
	requireSigningKeyEquals(t, ctx, afterKeys, resealTombstonedDID, repo.KeyUseDelete, fixture.tombstonedKey,
		"a row reported as Resealed must actually open under the current KEK alone; a report that counts work it did not do is what an operator trusts when they unset BRIDGE_KEK_PREVIOUS")
	rescued, err := LoadOrCreateRotationKey(ctx, fixture.serviceKeys, currentOnly(t))
	require.NoError(t, err,
		"the rotation key was reported Resealed, so it must open under the current KEK alone")
	assert.True(t, bytes.Equal(fixture.rotationKey.Bytes(), rescued.Bytes()),
		"the re-sealed rotation key must still be the original")

	// The untouchable row stays untouchable even on the error path.
	assert.True(t, bytes.Equal(fixture.serviceActorPEM, storedServiceKeyMaterial(t, fixture.database, ap.ServiceKeyName)),
		"the plaintext service-actor row must be untouched on the failure path too")
}
