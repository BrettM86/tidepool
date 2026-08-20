package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The base64 spellings of the same two KEKs the acceptance test writes in
// hex. BRIDGE_KEK has always accepted either form; BRIDGE_KEK_PREVIOUS must
// too, or an operator who generated both keys the same way gets told only one
// of them is malformed.
const (
	previousBridgeKEKBase64 = "iwrNHxCOGS83v5MKOAfjn3zqMk7oppnZ1VS3HUAxNE0="
	currentBridgeKEKBase64  = "qGm/20EwBv+prc53trzMftReHLCWUDPWRShQbvN3bW8="
)

// loadWithPreviousKEK runs Load over a production environment carrying both
// KEKs and returns whatever Load decided — failures included, unlike
// loadRotatingConfig in the acceptance test.
func loadWithPreviousKEK(t *testing.T, current, previous string) (*Config, error) {
	t.Helper()
	setProductionEnv(t)
	t.Setenv("BRIDGE_KEK", current)
	t.Setenv("BRIDGE_KEK_PREVIOUS", previous)
	return Load(discardLogger())
}

// namesOnlyPreviousKEK asserts that an error blames BRIDGE_KEK_PREVIOUS and
// never the current key. The subtlety: "BRIDGE_KEK_PREVIOUS" contains
// "BRIDGE_KEK" as a substring, so a naive NotContains would reject a correct
// message. Cut the full variable name out first, then look for what is left.
func namesOnlyPreviousKEK(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "BRIDGE_KEK_PREVIOUS",
		"a malformed previous KEK must name BRIDGE_KEK_PREVIOUS, so the operator edits the variable they actually broke")
	assert.NotContains(t, strings.ReplaceAll(msg, "BRIDGE_KEK_PREVIOUS", ""), "BRIDGE_KEK",
		"the message must not also blame BRIDGE_KEK: an operator mid-rotation sent to fix a key that is already correct will rotate the wrong one and orphan every sealed key")
}

func TestLoad_RejectsMalformedPreviousKEK(t *testing.T) {
	tests := []struct {
		name     string
		previous string
	}{
		{
			// 63 characters: not decodable as hex (odd length), not
			// decodable as base64 (length not a multiple of 4). The classic
			// truncated-paste.
			name:     "odd length hex",
			previous: "8b0acd1f108e192f37bf930a3807e39f7cea324ee8a699d9d554b71d4031344",
		},
		{
			// Valid base64, but only 16 bytes — AES-256 needs 32.
			name:     "base64 of the wrong number of bytes",
			previous: "MDEyMzQ1Njc4OWFiY2RlZg==",
		},
		{
			name:     "not an encoding at all",
			previous: "the-old-key-is-in-the-password-manager",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadWithPreviousKEK(t, currentBridgeKEKHex, tt.previous)
			require.Error(t, err,
				"a BRIDGE_KEK_PREVIOUS that cannot be decoded must stop startup; silently ignoring it would leave the bridge unable to open pre-rotation keys with no sign of why")
			namesOnlyPreviousKEK(t, err)
		})
	}
}

func TestLoad_RejectsPreviousKEKEqualToCurrent(t *testing.T) {
	// Same bytes, same spelling: the operator copied the old value into the
	// new variable and changed nothing.
	_, err := loadWithPreviousKEK(t, currentBridgeKEKHex, currentBridgeKEKHex)
	require.Error(t, err,
		"BRIDGE_KEK_PREVIOUS equal to BRIDGE_KEK is not a rotation; accepting it lets an operator believe they have rotated when every key is still sealed under the original")
	assert.Contains(t, err.Error(), "BRIDGE_KEK_PREVIOUS",
		"the refusal must name the variable the operator has to change")

	// Same bytes, different spelling. The check is on the decoded key, not
	// the string: hex here, base64 there, still one key and still not a
	// rotation.
	_, err = loadWithPreviousKEK(t, currentBridgeKEKHex, currentBridgeKEKBase64)
	require.Error(t, err,
		"the same key written in hex and base64 is still the same key; comparing the raw strings instead of the decoded bytes would wave this through")
}

func TestLoad_AcceptsPreviousKEKInEitherEncoding(t *testing.T) {
	expected := mustDecodeKEKHex(t, previousBridgeKEKHex)

	for _, tt := range []struct {
		name     string
		previous string
	}{
		{name: "hex", previous: previousBridgeKEKHex},
		{name: "base64", previous: previousBridgeKEKBase64},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := loadWithPreviousKEK(t, currentBridgeKEKHex, tt.previous)
			require.NoError(t, err)
			assert.Equal(t, expected, cfg.BridgeKEKPrevious,
				"BRIDGE_KEK_PREVIOUS must decode to the same 32 bytes BRIDGE_KEK would, in whichever encoding the operator pasted it")
		})
	}
}

func TestLoad_PreviousKEKIsOptionalInProduction(t *testing.T) {
	// The steady state, and by far the common one: no rotation in progress.
	// BRIDGE_KEK is required in production; BRIDGE_KEK_PREVIOUS must not be,
	// or adding rotation support breaks every existing deployment on restart.
	setProductionEnv(t)
	t.Setenv("BRIDGE_KEK", currentBridgeKEKHex)

	cfg, err := Load(discardLogger())
	require.NoError(t, err,
		"production must start with BRIDGE_KEK alone; requiring BRIDGE_KEK_PREVIOUS would refuse to boot every bridge that has never rotated")
	assert.Nil(t, cfg.BridgeKEKPrevious,
		"an unset BRIDGE_KEK_PREVIOUS must yield no previous key at all, not empty bytes a custodian might try to build a cipher from")
}

func TestLoad_PreviousKEKIsOptionalInDevelopment(t *testing.T) {
	// Development defaults BRIDGE_KEK to a fixed public key. There is no
	// corresponding default for the previous key: a dev bridge has nothing
	// sealed under an older one.
	clearConfigEnv(t)

	cfg, err := Load(discardLogger())
	require.NoError(t, err)
	assert.Nil(t, cfg.BridgeKEKPrevious,
		"development must not invent a previous KEK; a defaulted second key would make every dev custodian silently accept material no production bridge would")
}
