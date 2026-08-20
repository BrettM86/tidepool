package identity

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

// Telling "the KEK is wrong" apart from everything else that can go wrong with
// a sealed blob.
//
// Every failure to unseal used to arrive as one opaque fmt.Errorf chain, and
// the only consumer that classifies it — the outbound delivery worker — read
// the whole chain as transient and retried. A permanently wrong BRIDGE_KEK
// therefore burned the retry budget on every actor, for hours, and then
// poisoned under a message reading "message authentication failed": the exact
// wording that sends an operator looking for data corruption instead of the
// config line they changed.
//
// The four conditions are categorically different and only one of them is
// about the KEK:
//
//	wrong KEK      the blob is well formed and the AEAD refused it → ErrKeyUnsealable
//	corrupt blob   truncated, or an unknown version byte → validation
//	non-RSA key    the plaintext opened but is not a key we can sign with
//	missing actor  no row, no ciphertext → not found
//
// These tests pin the split at the seam that produces it, because the worker's
// decision (retry vs. poison immediately) is only as good as the classification
// it is handed.

const unsealableDID = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"

// custodianFor is a single-KEK custodian, or a fatal test failure.
func custodianFor(t *testing.T, kek []byte) *Custodian {
	t.Helper()
	c, err := NewCustodian(kek)
	require.NoError(t, err)
	return c
}

// TestDecryptActorRSAKey_WrongKEKIsUnsealable is the headline case: the AP
// signing key of a Coves user, sealed before a KEK change and opened after it.
func TestDecryptActorRSAKey_WrongKEKIsUnsealable(t *testing.T) {
	sealed, err := custodianFor(t, previousTestKEK()).EncryptActorRSAKey(unsealableDID, testRSAKey(t))
	require.NoError(t, err)

	_, err = custodianFor(t, currentTestKEK()).DecryptActorRSAKey(unsealableDID, sealed)
	require.Error(t, err)

	assert.True(t, IsKeyUnsealable(err),
		"a well-formed blob the AEAD refused is THE wrong-KEK signal, and it has to be its own class: it is the one unsealing failure that no amount of retrying can fix, and the one that a config line caused")
	assert.False(t, errors.IsValidation(err),
		"a wrong KEK is not a malformed input. Folding it into validation puts it in the same bucket as a truncated blob, which sends the operator to the storage layer")
	assert.False(t, errors.IsNotFound(err),
		"the ciphertext is right there; nothing is missing")
	assert.Contains(t, err.Error(), "BRIDGE_KEK",
		"the message must name the environment variable, because the operator's first move is to grep for it")
}

// TestDecryptActorKey_WrongKEKIsUnsealable is the same for the atproto escrow
// keys — a different AAD domain and a different key type, one classification.
func TestDecryptActorKey_WrongKEKIsUnsealable(t *testing.T) {
	_, sealed := sealedUnder(t, previousTestKEK())

	_, err := custodianFor(t, currentTestKEK()).DecryptActorKey(rotationTestDID, sealed)
	require.Error(t, err)
	assert.True(t, IsKeyUnsealable(err),
		"both sealed domains must classify a refused AEAD the same way; a caller that has to know which column the blob came from cannot classify anything")
	assert.Contains(t, err.Error(), "BRIDGE_KEK")
}

// TestUnsealable_MalformedBlobIsNotAKEKProblem is the boundary the reseal drill
// already depends on, restated where the classification now lives: bytes
// rejected BEFORE the AEAD is consulted say nothing about any KEK.
func TestUnsealable_MalformedBlobIsNotAKEKProblem(t *testing.T) {
	_, sealed := sealedUnder(t, previousTestKEK())
	current := custodianFor(t, currentTestKEK())

	for _, tc := range []struct {
		name string
		blob []byte
	}{
		{"truncated", sealed[:1+current.aead.NonceSize()]},
		{"unknown version byte", append([]byte{99}, sealed[1:]...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := current.DecryptActorKey(rotationTestDID, tc.blob)
			require.Error(t, err)
			assert.True(t, errors.IsValidation(err),
				"a blob rejected on its shape never reached the cipher, so it stays a validation error")
			assert.False(t, IsKeyUnsealable(err),
				"reporting damaged bytes as a KEK problem would send an operator to rotate a key that is working perfectly — and, at the worker, would poison a delivery under a config alarm that is not the fault")
		})
	}
}

// TestUnsealable_MidRotationCustodianNamesBothKeys pins what an operator sees
// when BRIDGE_KEK_PREVIOUS is set and the blob opens under neither: the message
// has to say both were tried, or the operator's next move is to set the
// variable they already set.
func TestUnsealable_MidRotationCustodianNamesBothKeys(t *testing.T) {
	_, sealed := sealedUnder(t, strangerTestKEK())

	_, err := rotatingCustodian(t).DecryptActorKey(rotationTestDID, sealed)
	require.Error(t, err)
	assert.True(t, IsKeyUnsealable(err))
	assert.True(t, strings.Contains(err.Error(), "BRIDGE_KEK_PREVIOUS"),
		"mid-rotation, a blob that opens under neither key must say so: an operator told only that BRIDGE_KEK failed will 'fix' it by supplying the previous key that is already configured and already failing")
}

// TestUnsealable_RightKEKStillOpens is the control. A classification that fires
// on the happy path would poison every delivery on the bridge.
func TestUnsealable_RightKEKStillOpens(t *testing.T) {
	key, sealed := sealedUnder(t, currentTestKEK())

	opened, err := custodianFor(t, currentTestKEK()).DecryptActorKey(rotationTestDID, sealed)
	require.NoError(t, err)
	assert.Equal(t, key.Bytes(), opened.Bytes())
	assert.False(t, IsKeyUnsealable(nil),
		"the predicate must be false for a nil error; a helper that says yes to success is a helper that poisons everything")
}
