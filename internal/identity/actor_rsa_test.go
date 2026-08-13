package identity

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"sync"
	"testing"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

// The AP-side RSA keys of task 13's coves.social Person actors are sealed by
// the same KEK as the K256 escrow keys, so these tests reuse keys_test.go's
// testKEK/testCustodian helpers. No database is involved.

const rsaTestDID = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"

// testRSAKey returns a shared 2048-bit RSA key: generating one per test
// dominates the runtime of a package that otherwise does no crypto work.
var (
	rsaKeyOnce sync.Once
	rsaKey     *rsa.PrivateKey
	rsaKeyErr  error
)

func testRSAKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	rsaKeyOnce.Do(func() {
		rsaKey, rsaKeyErr = rsa.GenerateKey(rand.Reader, 2048)
	})
	require.NoError(t, rsaKeyErr)
	return rsaKey
}

func TestCustodian_ActorRSAKeyRoundTrip(t *testing.T) {
	custodian := testCustodian(t)
	key := testRSAKey(t)

	sealed, err := custodian.EncryptActorRSAKey(rsaTestDID, key)
	require.NoError(t, err)
	require.NotEmpty(t, sealed, "EncryptActorRSAKey must return the sealed key")

	opened, err := custodian.DecryptActorRSAKey(rsaTestDID, sealed)
	require.NoError(t, err)
	require.NotNil(t, opened, "DecryptActorRSAKey must return the key")
	assert.True(t, opened.PublicKey.Equal(&key.PublicKey),
		"decrypted key's public half must equal the original's")
	assert.True(t, opened.Equal(key), "decrypted key must equal the original")
}

func TestCustodian_ActorRSAKeyBoundToDID(t *testing.T) {
	custodian := testCustodian(t)
	key := testRSAKey(t)

	sealed, err := custodian.EncryptActorRSAKey(rsaTestDID, key)
	require.NoError(t, err)
	require.NotEmpty(t, sealed)

	_, err = custodian.DecryptActorRSAKey("did:plc:44ybard66vv44zksje25o7dz", sealed)
	require.Error(t, err,
		"an RSA key sealed for one DID must not open under another (AAD binding)")
}

func TestCustodian_ActorRSAKeyAADDistinctFromK256(t *testing.T) {
	// The RSA surface must NOT reuse the K256 actor-key AAD: a sealed AP
	// signing key copied into bridged_actors.signing_key (or the reverse)
	// must fail to open, even under the same KEK and the same DID. Only a
	// cross test proves the constants differ.
	custodian := testCustodian(t)
	key := testRSAKey(t)

	rsaSealed, err := custodian.EncryptActorRSAKey(rsaTestDID, key)
	require.NoError(t, err)
	require.NotEmpty(t, rsaSealed)

	k256, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	k256Sealed, err := custodian.EncryptActorKey(rsaTestDID, k256)
	require.NoError(t, err)

	// Isolated at the AAD layer (like TestCustodian_CrossContextAADRejected):
	// if this open succeeded, the two AADs would be the same string.
	_, err = custodian.open(rsaSealed, []byte(actorKeyAADPrefix+rsaTestDID))
	require.Error(t, err,
		"the AP RSA AAD must differ from the K256 actor-key AAD for the same DID")

	// ... and through the production wrappers, both directions.
	_, err = custodian.DecryptActorKey(rsaTestDID, rsaSealed)
	require.Error(t, err, "a sealed AP RSA key must not open as a K256 signing key")

	_, err = custodian.DecryptActorRSAKey(rsaTestDID, k256Sealed)
	require.Error(t, err, "a sealed K256 signing key must not open as an AP RSA key")
}

func TestCustodian_ActorRSAKeyCiphertextLeaksNothing(t *testing.T) {
	custodian := testCustodian(t)
	key := testRSAKey(t)

	first, err := custodian.EncryptActorRSAKey(rsaTestDID, key)
	require.NoError(t, err)
	require.NotEmpty(t, first)
	second, err := custodian.EncryptActorRSAKey(rsaTestDID, key)
	require.NoError(t, err)
	require.NotEmpty(t, second)

	assert.NotContains(t, string(first), "-----BEGIN",
		"a sealed key must never carry a PEM header (the ap_actors grep test)")
	assert.False(t, bytes.Contains(first, key.D.Bytes()),
		"ciphertext must not contain the raw private exponent")
	assert.False(t, bytes.Equal(first, second),
		"each seal must use a fresh nonce")
}

func TestCustodian_ActorRSAKeyRejectsEmptyDID(t *testing.T) {
	custodian := testCustodian(t)
	key := testRSAKey(t)

	_, err := custodian.EncryptActorRSAKey("", key)
	require.Error(t, err, "sealing without a DID has no AAD to bind to")
	assert.True(t, errors.IsValidation(err), "got %v", err)

	sealed, err := custodian.EncryptActorRSAKey(rsaTestDID, key)
	require.NoError(t, err)
	require.NotEmpty(t, sealed)

	_, err = custodian.DecryptActorRSAKey("", sealed)
	require.Error(t, err, "opening without a DID must be refused explicitly")
	assert.True(t, errors.IsValidation(err), "got %v", err)
}
