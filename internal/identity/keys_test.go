package identity

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/repo"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

func testKEK() []byte {
	sum := sha256.Sum256([]byte("tidepool-test-kek"))
	return sum[:]
}

func testCustodian(t *testing.T) *Custodian {
	t.Helper()
	c, err := NewCustodian(testKEK())
	require.NoError(t, err)
	return c
}

func TestNewCustodian_RejectsBadKEKLength(t *testing.T) {
	_, err := NewCustodian([]byte("short"))
	require.Error(t, err)
	assert.True(t, errors.IsValidation(err))
}

func TestCustodian_ActorKeyRoundTrip(t *testing.T) {
	custodian := testCustodian(t)
	key, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)

	const did = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	sealed, err := custodian.EncryptActorKey(did, key)
	require.NoError(t, err)
	assert.NotContains(t, string(sealed), string(key.Bytes()),
		"ciphertext must not contain the raw key material")

	opened, err := custodian.DecryptActorKey(did, sealed)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(key.Bytes(), opened.Bytes()), "decrypted key must equal the original")
}

func TestCustodian_CiphertextBoundToDID(t *testing.T) {
	custodian := testCustodian(t)
	key, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)

	sealed, err := custodian.EncryptActorKey("did:plc:ewvi7nxzyoun6zhxrhs64oiz", key)
	require.NoError(t, err)

	_, err = custodian.DecryptActorKey("did:plc:44ybard66vv44zksje25o7dz", sealed)
	require.Error(t, err, "a key sealed for one DID must not open under another (AAD binding)")
}

func TestCustodian_WrongKEKFails(t *testing.T) {
	custodian := testCustodian(t)
	key, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)

	const did = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	sealed, err := custodian.EncryptActorKey(did, key)
	require.NoError(t, err)

	otherKEK := sha256.Sum256([]byte("a-different-kek"))
	other, err := NewCustodian(otherKEK[:])
	require.NoError(t, err)
	_, err = other.DecryptActorKey(did, sealed)
	require.Error(t, err)
}

func TestCustodian_RejectsUnknownVersionAndTruncation(t *testing.T) {
	custodian := testCustodian(t)
	key, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)

	const did = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	sealed, err := custodian.EncryptActorKey(did, key)
	require.NoError(t, err)

	tampered := append([]byte{}, sealed...)
	tampered[0] = 99
	_, err = custodian.DecryptActorKey(did, tampered)
	require.Error(t, err, "unknown ciphertext version must be rejected")

	_, err = custodian.DecryptActorKey(did, sealed[:8])
	require.Error(t, err, "truncated ciphertext must be rejected")
}

func TestCustodian_CrossContextAADRejected(t *testing.T) {
	// The AAD strings bind each ciphertext to its purpose, not just its
	// owner: a sealed rotation key pasted into bridged_actors.signing_key
	// (or an actor key into the service_keys rotation row) must fail to
	// open, even under the same KEK.
	custodian := testCustodian(t)
	key, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	const did = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"

	// Rotation-key ciphertext must not open as an actor key.
	rotationSealed, err := custodian.seal(key.Bytes(), []byte(rotationKeyAAD))
	require.NoError(t, err)
	_, err = custodian.DecryptActorKey(did, rotationSealed)
	require.Error(t, err,
		"a sealed rotation key must not open as an actor signing key")

	// Actor-key ciphertext must not open as the rotation key (via the real
	// production open path, decryptRotationKey).
	actorSealed, err := custodian.EncryptActorKey(did, key)
	require.NoError(t, err)
	_, err = decryptRotationKey(custodian, actorSealed)
	require.Error(t, err,
		"a sealed actor key must not open as the rotation key")
}

// The rotation-key and ActorKeys tests run against real postgres (skipped
// without TIDEPOOL_TEST_DATABASE_URL, per repo convention).

func TestLoadOrCreateRotationKey_PersistsAcrossLoads(t *testing.T) {
	database := testutil.DB(t)
	testutil.Truncate(t, database, "service_keys")
	keys := store.NewServiceKeys(database)
	custodian := testCustodian(t)
	ctx := t.Context()

	first, err := LoadOrCreateRotationKey(ctx, keys, custodian)
	require.NoError(t, err)
	second, err := LoadOrCreateRotationKey(ctx, keys, custodian)
	require.NoError(t, err)

	assert.True(t, bytes.Equal(first.Bytes(), second.Bytes()),
		"second load must return the persisted key, not a fresh one")

	// The stored bytes must be sealed, not the raw key.
	stored, err := keys.Get(ctx, RotationKeyName)
	require.NoError(t, err)
	assert.False(t, bytes.Contains(stored.KeyMaterial, first.Bytes()),
		"rotation key must be encrypted at rest")
}

func TestActorKeys_SigningKeyLifecycle(t *testing.T) {
	database := testutil.DB(t)
	testutil.Truncate(t, database, "bridged_actors")
	actors := store.NewBridgedActors(database)
	custodian := testCustodian(t)
	actorKeys := NewActorKeys(actors, custodian)
	ctx := t.Context()

	signing, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	const did = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	sealed, err := custodian.EncryptActorKey(did, signing)
	require.NoError(t, err)

	const apActorID = "https://lemmy.world/u/alice"
	_, err = actors.UpsertActor(ctx, store.BridgedActor{
		APActorID:           apActorID,
		ActorType:           store.ActorTypePerson,
		DID:                 did,
		Handle:              "alice.lemmy-world.tidepool.example",
		SigningKeyEncrypted: sealed,
		ConsentState:        store.ConsentStateOK,
	})
	require.NoError(t, err)

	got, err := actorKeys.SigningKey(ctx, did, repo.KeyUseWrite)
	require.NoError(t, err)
	gotK256, ok := got.(*atcrypto.PrivateKeyK256)
	require.True(t, ok)
	assert.True(t, bytes.Equal(signing.Bytes(), gotK256.Bytes()))

	// Unknown DID → not found.
	_, err = actorKeys.SigningKey(ctx, "did:plc:44ybard66vv44zksje25o7dz", repo.KeyUseWrite)
	assert.True(t, errors.IsNotFound(err))

	// Tombstoned actor → frozen for writes: no key for KeyUseWrite.
	require.NoError(t, actors.SetConsentState(ctx, apActorID, store.ConsentStateDeleted))
	_, err = actorKeys.SigningKey(ctx, did, repo.KeyUseWrite)
	assert.True(t, errors.IsTombstoned(err),
		"tombstoned actors' repos are frozen via key custody")
	assert.False(t, errors.IsNotFound(err), "tombstoned must not read as missing")

	// ... but deletes still get the key: scrubbing a deleted actor's
	// records must always be possible (task 05's Delete(Actor) flow).
	gotDel, err := actorKeys.SigningKey(ctx, did, repo.KeyUseDelete)
	require.NoError(t, err,
		"tombstoned actor's key must still be released for KeyUseDelete")
	gotDelK256, ok := gotDel.(*atcrypto.PrivateKeyK256)
	require.True(t, ok)
	assert.True(t, bytes.Equal(signing.Bytes(), gotDelK256.Bytes()))
}
