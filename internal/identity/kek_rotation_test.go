package identity

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

// The KEKs of a rotation, plus a third that was never the bridge's key — the
// stand-in for a ciphertext that arrived from somewhere it should not have.
func previousTestKEK() []byte {
	sum := sha256.Sum256([]byte("tidepool-test-kek-previous"))
	return sum[:]
}

func currentTestKEK() []byte {
	sum := sha256.Sum256([]byte("tidepool-test-kek-current"))
	return sum[:]
}

func strangerTestKEK() []byte {
	sum := sha256.Sum256([]byte("tidepool-test-kek-stranger"))
	return sum[:]
}

const rotationTestDID = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"

// rotatingCustodian is the custodian a bridge runs mid-rotation.
func rotatingCustodian(t *testing.T) *Custodian {
	t.Helper()
	c, err := NewCustodianWithPrevious(currentTestKEK(), previousTestKEK())
	require.NoError(t, err)
	return c
}

// sealedUnder returns an actor signing key and its ciphertext under kek.
func sealedUnder(t *testing.T, kek []byte) (*atcrypto.PrivateKeyK256, []byte) {
	t.Helper()
	custodian, err := NewCustodian(kek)
	require.NoError(t, err)
	key, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	sealed, err := custodian.EncryptActorKey(rotationTestDID, key)
	require.NoError(t, err)
	return key, sealed
}

func TestCustodianWithPrevious_OpensKeySealedUnderPrevious(t *testing.T) {
	key, sealed := sealedUnder(t, previousTestKEK())

	opened, err := rotatingCustodian(t).DecryptActorKey(rotationTestDID, sealed)
	require.NoError(t, err,
		"the whole point of a previous KEK: material sealed before the rotation must still open")
	assert.True(t, bytes.Equal(key.Bytes(), opened.Bytes()),
		"the key recovered under the previous KEK must be the original, not merely something that decrypted")
}

func TestCustodianWithPrevious_SealsUnderCurrentOnly(t *testing.T) {
	custodian := rotatingCustodian(t)
	key, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	sealed, err := custodian.EncryptActorKey(rotationTestDID, key)
	require.NoError(t, err)

	currentOnly, err := NewCustodian(currentTestKEK())
	require.NoError(t, err)
	opened, err := currentOnly.DecryptActorKey(rotationTestDID, sealed)
	require.NoError(t, err,
		"seal must always use the current KEK; if it ever used the previous one the rotation could never be finished and the old key could never be retired")
	assert.True(t, bytes.Equal(key.Bytes(), opened.Bytes()))

	previousOnly, err := NewCustodian(previousTestKEK())
	require.NoError(t, err)
	_, err = previousOnly.DecryptActorKey(rotationTestDID, sealed)
	require.Error(t, err,
		"a key sealed during the rotation window must NOT open under the retired KEK; if it does, the old key is still live key material and retiring it is a data-loss event")
}

func TestCustodianWithPrevious_BothKeysFailReportsOneError(t *testing.T) {
	// A blob sealed under a KEK the bridge has never held: pasted from
	// another deployment, or restored from a backup taken two rotations ago.
	// Neither key opens it, and the operator must see ONE failure describing
	// that — not a pair of stacked attempts they have to read past.
	_, stranger := sealedUnder(t, strangerTestKEK())

	currentOnly, err := NewCustodian(currentTestKEK())
	require.NoError(t, err)
	_, singleErr := currentOnly.DecryptActorKey(rotationTestDID, stranger)
	require.Error(t, singleErr)

	_, dualErr := rotatingCustodian(t).DecryptActorKey(rotationTestDID, stranger)
	require.Error(t, dualErr,
		"a blob under an unknown KEK must not open just because two keys were tried")

	assert.Equal(t, 1, strings.Count(dualErr.Error(), "does not open under"),
		"a failed retry must not stack a second open error onto the first; one unreadable blob is one incident to the operator reading the log")

	// ONE INCIDENT, ONE CLASS — but not one sentence. This assertion used to
	// require the two messages to be byte-identical, so that alerts written
	// before a rotation still matched during it. Matching on the message is now
	// the wrong seam: ErrKeyUnsealable is the stable thing to key on, and it is
	// identical in both cases, while the TEXT has a job the identical version
	// could not do. "BRIDGE_KEK failed", read mid-rotation, tells an operator to
	// supply the previous key — which is already set, and already failing.
	assert.True(t, IsKeyUnsealable(singleErr), "one KEK, refused: unsealable")
	assert.True(t, IsKeyUnsealable(dualErr), "two KEKs, both refused: the same class")
	assert.Contains(t, singleErr.Error(), "does not open under BRIDGE_KEK:",
		"with a single key configured the message must name that one key and stop there")
	assert.Contains(t, dualErr.Error(), "does not open under BRIDGE_KEK or BRIDGE_KEK_PREVIOUS",
		"with both configured it must say both were tried, or the operator's next move is the one they already made")
}

func TestCustodianWithPrevious_MalformedBlobIsNotAKEKProblem(t *testing.T) {
	// Retry belongs on authentication failure alone. A blob that is truncated
	// or carries an unknown version byte is corrupt storage, not a wrong key:
	// it must be reported as malformed under one KEK or two, so an operator
	// does not go hunting through key history for a database problem.
	_, sealed := sealedUnder(t, currentTestKEK())

	badVersion := append([]byte{}, sealed...)
	badVersion[0] = 99

	for _, tt := range []struct {
		name string
		blob []byte
	}{
		{name: "truncated", blob: sealed[:8]},
		{name: "unknown version byte", blob: badVersion},
	} {
		t.Run(tt.name, func(t *testing.T) {
			currentOnly, err := NewCustodian(currentTestKEK())
			require.NoError(t, err)
			_, singleErr := currentOnly.DecryptActorKey(rotationTestDID, tt.blob)
			require.Error(t, singleErr)

			_, dualErr := rotatingCustodian(t).DecryptActorKey(rotationTestDID, tt.blob)
			require.Error(t, dualErr)

			assert.True(t, errors.IsValidation(dualErr),
				"a malformed sealed blob must stay a validation error with a previous KEK configured; classifying it otherwise turns a storage-corruption incident into a key-management goose chase")
			assert.NotContains(t, dualErr.Error(), "message authentication failed",
				"a malformed blob must not be reported as an authentication failure: it was never decrypted under either key, so nothing about the KEKs is in question")
			assert.Equal(t, singleErr.Error(), dualErr.Error(),
				"a malformed blob must read exactly the same with and without a previous KEK")
		})
	}
}

func TestCustodianWithPrevious_CrossAADRejectedUnderBothKeys(t *testing.T) {
	// The AAD prefixes keep the K256 escrow domain and the AP RSA domain
	// apart. A second KEK widens which keys can decrypt; it must not widen
	// which domains a ciphertext is accepted in.
	custodian := rotatingCustodian(t)

	// An actor signing key sealed under the PREVIOUS KEK — the blob the
	// retry path is there to open — offered to the RSA domain.
	_, sealedK256 := sealedUnder(t, previousTestKEK())
	_, err := custodian.DecryptActorRSAKey(rotationTestDID, sealedK256)
	require.Error(t, err,
		"an escrow signing key must not open as an AP RSA key even when the previous KEK is the one that can decrypt it; the retry must re-try the KEK, never the AAD")

	// And the reverse, also sealed under the previous KEK.
	previousOnly, err := NewCustodian(previousTestKEK())
	require.NoError(t, err)
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	sealedRSA, err := previousOnly.EncryptActorRSAKey(rotationTestDID, rsaKey)
	require.NoError(t, err)
	_, err = custodian.DecryptActorKey(rotationTestDID, sealedRSA)
	require.Error(t, err,
		"an AP RSA key must not open as an escrow signing key under either KEK")

	// The DID binding survives the second key too.
	_, sealedForOwner := sealedUnder(t, previousTestKEK())
	_, err = custodian.DecryptActorKey("did:plc:44ybard66vv44zksje25o7dz", sealedForOwner)
	require.Error(t, err,
		"a key sealed for one DID must not open under another, whichever KEK decrypts it")
}

func TestCustodianWithPrevious_NilPreviousMatchesNewCustodian(t *testing.T) {
	// Run 1 ships alone: a bridge that never sets BRIDGE_KEK_PREVIOUS must
	// behave exactly as it did before rotation support existed.
	dual, err := NewCustodianWithPrevious(currentTestKEK(), nil)
	require.NoError(t, err,
		"a nil previous KEK is the normal case, not an error")

	_, sealedUnderOld := sealedUnder(t, previousTestKEK())
	_, err = dual.DecryptActorKey(rotationTestDID, sealedUnderOld)
	require.Error(t, err,
		"without a previous KEK the custodian must refuse old material exactly as NewCustodian does; anything else would mean a second key was being used that the operator never configured")

	key, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	sealed, err := dual.EncryptActorKey(rotationTestDID, key)
	require.NoError(t, err)
	opened, err := dual.DecryptActorKey(rotationTestDID, sealed)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(key.Bytes(), opened.Bytes()),
		"the ordinary seal/open round trip must be untouched when no previous KEK is set")

	// And what it seals is readable by a plain single-KEK custodian, so the
	// two constructors are interchangeable at rest.
	plain, err := NewCustodian(currentTestKEK())
	require.NoError(t, err)
	_, err = plain.DecryptActorKey(rotationTestDID, sealed)
	require.NoError(t, err,
		"ciphertext written with a nil previous KEK must be indistinguishable from ciphertext written by NewCustodian")
}

func TestNewCustodianWithPrevious_RejectsBadPreviousLength(t *testing.T) {
	_, err := NewCustodianWithPrevious(currentTestKEK(), []byte("0123456789abcdef"))
	require.Error(t, err,
		"a 16-byte previous KEK cannot build an AES-256 cipher; accepting it would mean discovering the problem only when a pre-rotation key failed to open")
	assert.True(t, errors.IsValidation(err))
	assert.Contains(t, strings.ToLower(err.Error()), "previous",
		"the error must say WHICH key is the wrong length; an operator told only that 'the KEK' is bad will check the one that is fine")
}

func TestNewCustodianWithPrevious_RejectsBadCurrentLength(t *testing.T) {
	_, err := NewCustodianWithPrevious([]byte("short"), previousTestKEK())
	require.Error(t, err, "the current KEK is validated exactly as NewCustodian validates it")
	assert.True(t, errors.IsValidation(err))
}
