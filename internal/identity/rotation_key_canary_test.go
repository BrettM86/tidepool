package identity

import (
	"context"
	"database/sql"
	"testing"

	"github.com/bluesky-social/indigo/atproto/atcrypto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// The boot canary's OTHER half.
//
// rotation_key_boot_test.go pins the canary on the path where the plc-rotation
// row EXISTS: it will not open under the wrong BRIDGE_KEK, boot fails, nothing
// is written. That test's own comment leans on "the create path is unreachable
// on a provisioned database" — which is true only while the row is there.
//
// These tests are about the state where it is NOT: a restore that missed
// service_keys, a hand-rolled provisioning step, a fresh volume attached to a
// populated database. LoadOrCreateRotationKey takes its create branch, seals a
// brand-new key under whatever BRIDGE_KEK it was handed, and returns success —
// so the wrong-KEK config sails past the one check that was supposed to catch
// it. Every actor key sealed under the real KEK then fails to open one at a
// time, deep in the delivery path, while newly minted actors work perfectly.
//
// A canary that can satisfy itself by writing the thing it was meant to read is
// not a canary. The KEK has to be proven against material the bridge did not
// just create.

const (
	canaryAPActorDID   = "did:plc:7iza6de2dwap2sbkpav7c6c6"
	canaryBridgedDID   = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	canaryBridgedActor = "https://lemmy.world/u/canary"
)

// TestLoadOrCreateRotationKey_WrongKEKWithNoRotationRowMustNotSelfSatisfy is
// the red for the canary hole.
//
// GIVEN a populated database whose actor keys are sealed under KEK A and whose
// plc-rotation row is missing, WHEN the bridge boots under KEK B alone, THEN
// the load must FAIL — and must not mint a replacement rotation key under the
// wrong KEK, because that both hides the misconfiguration and strands every
// bridged DID under a rotation authority nobody holds.
func TestLoadOrCreateRotationKey_WrongKEKWithNoRotationRowMustNotSelfSatisfy(t *testing.T) {
	database := testutil.DB(t)
	testutil.Truncate(t, database, "bridged_actors", "ap_actors", "service_keys")
	ctx := t.Context()

	custodianA, err := NewCustodian(previousTestKEK())
	require.NoError(t, err)

	// A Coves user's AP signing key, sealed under the REAL KEK.
	sealedRSA, err := custodianA.EncryptActorRSAKey(canaryAPActorDID, testRSAKey(t))
	require.NoError(t, err)
	_, err = store.NewAPActors(database).Create(ctx, store.APActor{
		DID:              canaryAPActorDID,
		Kind:             store.ActorTypePerson,
		ActorID:          "https://coves.social/ap/actor/" + canaryAPActorDID,
		NormalizedOrigin: "coves.social",
		LocalPart:        "canary",
		RSAKeySealed:     sealedRSA,
		RSAKeyVersion:    1,
		PublicKeyPEM:     "-----BEGIN PUBLIC KEY-----\nTEST\n-----END PUBLIC KEY-----\n",
	})
	require.NoError(t, err)

	// And the plc-rotation row is simply absent: the restore missed it.
	keys := store.NewServiceKeys(database)
	require.Equal(t, 0, countServiceKeys(t, database),
		"the fixture is a POPULATED database with NO rotation key; that is the whole scenario")

	// WHEN: the bridge boots under a different BRIDGE_KEK.
	custodianB, err := NewCustodian(currentTestKEK())
	require.NoError(t, err)
	_, err = LoadOrCreateRotationKey(ctx, database, keys, custodianB)

	// THEN: boot fails, exactly as it does when the row is present.
	require.Error(t, err,
		"a wrong BRIDGE_KEK must fail the boot canary whether or not the plc-rotation row happens to exist. With the row absent the create branch seals a fresh key under the wrong KEK and reports success — so the bridge serves traffic, mints new actors that work, and fails to open every pre-existing actor key one delivery at a time")
	assert.True(t, IsKeyUnsealable(err),
		"the failure must be classified as a KEK that does not open the material at rest, not as a generic load error: it is the difference between an operator checking BRIDGE_KEK and an operator suspecting database corruption")
	assert.Contains(t, err.Error(), "BRIDGE_KEK",
		"and it must name the variable, because that is what the operator greps")
	assert.Equal(t, 0, countServiceKeys(t, database),
		"a canary that fails must write NOTHING. Minting a rotation key under the wrong KEK is unrecoverable in the direction that matters: every bridged DID names the old rotation authority, and the row that would have proved the KEK wrong is now a row that proves it right")
}

// TestLoadOrCreateRotationKey_FreshInstallStillCreates is the guard on the fix.
//
// The check above must not turn a genuinely fresh install into a boot failure:
// with nothing sealed anywhere, there is no material to prove the KEK against
// and no wrong answer to give. The first boot mints the rotation key, and that
// is correct.
func TestLoadOrCreateRotationKey_FreshInstallStillCreates(t *testing.T) {
	database := testutil.DB(t)
	testutil.Truncate(t, database, "bridged_actors", "ap_actors", "service_keys")
	ctx := t.Context()

	custodian, err := NewCustodian(currentTestKEK())
	require.NoError(t, err)
	keys := store.NewServiceKeys(database)

	fresh, err := LoadOrCreateRotationKey(ctx, database, keys, custodian)
	require.NoError(t, err,
		"an empty database has no sealed material to check a KEK against, so the first boot must still mint the rotation key")
	require.NotNil(t, fresh)
	assert.Equal(t, 1, countServiceKeys(t, database))

	// And the second boot reads back what the first one wrote.
	again, err := LoadOrCreateRotationKey(ctx, database, keys, custodian)
	require.NoError(t, err)
	assert.Equal(t, fresh.Bytes(), again.Bytes())
}

// seedCanaryBridgedActor puts one escrowed atproto signing key at rest, sealed
// under kek, and returns the ciphertext as stored.
func seedCanaryBridgedActor(t *testing.T, ctx context.Context, database *sql.DB, kek []byte) []byte {
	t.Helper()
	custodian, err := NewCustodian(kek)
	require.NoError(t, err)
	key, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	sealed, err := custodian.EncryptActorKey(canaryBridgedDID, key)
	require.NoError(t, err)
	_, err = store.NewBridgedActors(database).UpsertActor(ctx, store.BridgedActor{
		APActorID:           canaryBridgedActor,
		ActorType:           store.ActorTypePerson,
		DID:                 canaryBridgedDID,
		Handle:              "canary.lemmy-world.tidepool.example",
		SigningKeyEncrypted: sealed,
		ConsentState:        store.ConsentStateOK,
	})
	require.NoError(t, err)
	return sealed
}

// TestLoadOrCreateRotationKey_CanaryReadsTheEscrowKeysToo pins that the proof
// is not limited to one table.
//
// A bridge that has only ever bridged fediverse actors has escrowed signing
// keys in bridged_actors and nothing in ap_actors. Its KEK is just as provable,
// and a canary that only knew about the Coves-side table would wave that
// database through.
func TestLoadOrCreateRotationKey_CanaryReadsTheEscrowKeysToo(t *testing.T) {
	database := testutil.DB(t)
	testutil.Truncate(t, database, "bridged_actors", "ap_actors", "service_keys")
	ctx := t.Context()

	seedCanaryBridgedActor(t, ctx, database, previousTestKEK())

	custodianB, err := NewCustodian(currentTestKEK())
	require.NoError(t, err)
	_, err = LoadOrCreateRotationKey(ctx, database, store.NewServiceKeys(database), custodianB)

	require.Error(t, err,
		"escrowed bridged-actor keys prove the KEK exactly as well as the Coves-side ones; a bridge with only fediverse actors must not be waved through")
	assert.True(t, IsKeyUnsealable(err))
	assert.Equal(t, 0, countServiceKeys(t, database))
}

// TestLoadOrCreateRotationKey_MidRotationMayStillMint is the rotation window's
// half of the rule, and the reason the check is "opens under ANY configured
// KEK" rather than "opens under BRIDGE_KEK".
//
// An operator mid-rotation runs with BRIDGE_KEK_PREVIOUS set. Their material is
// still sealed under the old key and their configuration is correct — so if the
// rotation row is missing (the restore that lost it), the canary must let the
// boot mint a replacement rather than reading a legitimate rotation as a
// misconfiguration.
func TestLoadOrCreateRotationKey_MidRotationMayStillMint(t *testing.T) {
	database := testutil.DB(t)
	testutil.Truncate(t, database, "bridged_actors", "ap_actors", "service_keys")
	ctx := t.Context()

	seedCanaryBridgedActor(t, ctx, database, previousTestKEK())

	minted, err := LoadOrCreateRotationKey(ctx, database, store.NewServiceKeys(database), rotatingCustodian(t))
	require.NoError(t, err,
		"a custodian that can open the material at rest — under either of its two keys — has proven itself; refusing here would make a documented rotation step a boot failure")
	require.NotNil(t, minted)
	require.Equal(t, 1, countServiceKeys(t, database))

	// And what it minted is sealed under the CURRENT key, which is what makes
	// the eventual BRIDGE_KEK_PREVIOUS retirement safe.
	underCurrentOnly, err := NewCustodian(currentTestKEK())
	require.NoError(t, err)
	reopened, err := LoadOrCreateRotationKey(ctx, database, store.NewServiceKeys(database), underCurrentOnly)
	require.NoError(t, err,
		"a key minted mid-rotation must be sealed under the CURRENT KEK, or unsetting BRIDGE_KEK_PREVIOUS later strands it")
	assert.Equal(t, minted.Bytes(), reopened.Bytes())
}

// TestLoadOrCreateRotationKey_CanaryRefusesWhenEveryProbeIsDamaged covers the
// third answer, which is neither proof nor disproof.
//
// Damaged bytes are rejected on their shape before any key is consulted, so
// they cannot testify about the KEK. A database whose sealed material is ALL
// damaged therefore leaves the KEK unproven — and minting an escrow rotation
// key over a populated database on an unproven key is the exact move this
// canary exists to stop. It refuses, and says restore rather than reconfigure.
func TestLoadOrCreateRotationKey_CanaryRefusesWhenEveryProbeIsDamaged(t *testing.T) {
	database := testutil.DB(t)
	testutil.Truncate(t, database, "bridged_actors", "ap_actors", "service_keys")
	ctx := t.Context()

	sealed := seedCanaryBridgedActor(t, ctx, database, currentTestKEK())
	// Full length, unknown version byte: read, never opened.
	damaged := append([]byte{99}, sealed[1:]...)
	_, err := database.ExecContext(ctx,
		`UPDATE bridged_actors SET signing_key = $2 WHERE did = $1`, canaryBridgedDID, damaged)
	require.NoError(t, err)

	custodian, err := NewCustodian(currentTestKEK())
	require.NoError(t, err)
	_, err = LoadOrCreateRotationKey(ctx, database, store.NewServiceKeys(database), custodian)

	require.Error(t, err,
		"unreadable material proves nothing about the KEK, and minting on an unproven KEK over a populated database is the failure this check exists for")
	assert.False(t, IsKeyUnsealable(err),
		"but it must NOT be reported as a wrong KEK: the operator's move here is a restore, and sending them to rotate a key that may be perfectly correct wastes the outage")
	assert.Equal(t, 0, countServiceKeys(t, database))
}
