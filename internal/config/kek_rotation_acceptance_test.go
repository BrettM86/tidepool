package config

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/identity"
)

// The two sides of a KEK rotation: the key every escrowed signing key was
// sealed under before the operator edited the environment, and the key set
// afterwards. They are deliberately unrelated values — if they were equal the
// test below would pass without the bridge ever learning to read two keys.
//
// This test lives in package config rather than package identity because the
// behaviour under test only exists at the seam between them: config decides
// what a rotating operator's environment means, identity decides what a
// custodian built from it can open. identity does not import config, so the
// test package can reach across.
const (
	previousBridgeKEKHex = "8b0acd1f108e192f37bf930a3807e39f7cea324ee8a699d9d554b71d4031344d"
	currentBridgeKEKHex  = "a869bfdb413006ffa9adce77b6bccc7ed45e1cb0965033d64528506ef3776d6f"
)

// rotationActorDID owns the escrowed signing key that has to survive the
// rotation. The value matters only because EncryptActorKey binds the
// ciphertext to it via AAD — the blob will not open under any other DID.
const rotationActorDID = "did:plc:7iza6de2dwap2sbkpav7c6c6"

func mustDecodeKEKHex(t *testing.T, encoded string) []byte {
	t.Helper()
	raw, err := hex.DecodeString(encoded)
	require.NoError(t, err)
	require.Len(t, raw, identity.KEKSize)
	return raw
}

// loadRotatingConfig runs the real Load over an otherwise ordinary production
// environment whose only interesting values are the two KEKs. previous == ""
// stands for an operator who has not set BRIDGE_KEK_PREVIOUS at all.
func loadRotatingConfig(t *testing.T, current, previous string) *Config {
	t.Helper()
	setProductionEnv(t)
	t.Setenv("BRIDGE_KEK", current)
	t.Setenv("BRIDGE_KEK_PREVIOUS", previous)
	cfg, err := Load(discardLogger())
	require.NoError(t, err)
	return cfg
}

// TestBridgeKEKRotation_ReadsUnderTwoKEKsWritesUnderOne is the outer
// acceptance test for KEK rotation, observed from where an operator stands:
// the environment goes in, a custodian comes out, and the key material that
// was already on disk still opens.
//
// GIVEN an actor signing key sealed by a custodian that only ever knew the
// OLD KEK — the bytes sitting in bridged_actors.signing_key at the moment the
// operator edits BRIDGE_KEK — and an environment carrying the new key as
// BRIDGE_KEK and the old one as BRIDGE_KEK_PREVIOUS,
//
// WHEN config.Load parses that environment and the server builds its
// custodian from the loaded config,
//
// THEN:
//
//  1. the pre-rotation blob still opens, and yields the original key;
//  2. anything sealed AFTER the rotation opens under the new KEK alone, so the
//     operator can eventually drop BRIDGE_KEK_PREVIOUS;
//  3. with BRIDGE_KEK_PREVIOUS unset, the pre-rotation blob does NOT open —
//     the negative control that keeps (1) from passing for free.
func TestBridgeKEKRotation_ReadsUnderTwoKEKsWritesUnderOne(t *testing.T) {
	previousKEK := mustDecodeKEKHex(t, previousBridgeKEKHex)
	currentKEK := mustDecodeKEKHex(t, currentBridgeKEKHex)
	require.False(t, bytes.Equal(previousKEK, currentKEK),
		"the two KEKs must differ, otherwise nothing in this test is a rotation")

	// GIVEN: key material sealed before the rotation, by a custodian that has
	// never heard of the new KEK.
	previousOnly, err := identity.NewCustodian(previousKEK)
	require.NoError(t, err)
	actorKey, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	sealedBeforeRotation, err := previousOnly.EncryptActorKey(rotationActorDID, actorKey)
	require.NoError(t, err)

	// WHEN: the operator promotes the new key and keeps the old one alongside
	// it, and the server builds the custodian it will run on.
	cfg := loadRotatingConfig(t, currentBridgeKEKHex, previousBridgeKEKHex)
	rotating, err := identity.NewCustodianWithPrevious(cfg.BridgeKEK, cfg.BridgeKEKPrevious)
	require.NoError(t, err)

	// THEN 1: the bridge can still open what it sealed yesterday. If this
	// fails, changing BRIDGE_KEK orphans every escrowed signing key at once:
	// no bridged actor can sign a commit, no deletion can be scrubbed, and the
	// only way back is restoring the old key from wherever the operator kept it.
	reopened, err := rotating.DecryptActorKey(rotationActorDID, sealedBeforeRotation)
	if assert.NoError(t, err,
		"a signing key sealed under BRIDGE_KEK_PREVIOUS must still open after BRIDGE_KEK is rotated; otherwise rotation locks every bridged actor out of its own repo") {
		assert.True(t, bytes.Equal(actorKey.Bytes(), reopened.Bytes()),
			"the key recovered under the previous KEK must be the original signing key byte for byte; anything else would sign commits that no DID document vouches for")
	}

	// THEN 2: the rotation never writes backwards. Sealing always uses the
	// CURRENT KEK, so once the old material is re-sealed the operator can drop
	// BRIDGE_KEK_PREVIOUS and the bridge keeps working.
	sealedAfterRotation, err := rotating.EncryptActorKey(rotationActorDID, actorKey)
	require.NoError(t, err)
	currentOnly, err := identity.NewCustodian(currentKEK)
	require.NoError(t, err)
	resealed, err := currentOnly.DecryptActorKey(rotationActorDID, sealedAfterRotation)
	if assert.NoError(t, err,
		"a key sealed during the rotation window must open under BRIDGE_KEK alone; if seal ever used the previous KEK the operator could never finish the rotation") {
		assert.True(t, bytes.Equal(actorKey.Bytes(), resealed.Bytes()),
			"the freshly sealed key must round-trip under the current KEK alone")
	}

	// THEN 3: the negative control. An operator who never set
	// BRIDGE_KEK_PREVIOUS gets exactly the old single-key behaviour — proof
	// that THEN 1 above is measuring the second KEK and not some accident of
	// the ciphertext format.
	single := loadRotatingConfig(t, currentBridgeKEKHex, "")
	require.Nil(t, single.BridgeKEKPrevious,
		"an unset BRIDGE_KEK_PREVIOUS must yield no previous KEK at all, not an empty one that a custodian might treat as a key")
	singleCustodian, err := identity.NewCustodianWithPrevious(single.BridgeKEK, single.BridgeKEKPrevious)
	require.NoError(t, err)
	_, err = singleCustodian.DecryptActorKey(rotationActorDID, sealedBeforeRotation)
	assert.Error(t, err,
		"without BRIDGE_KEK_PREVIOUS the bridge must refuse material sealed under the old KEK; if it opens anyway, this whole test proves nothing about the second KEK")
}
