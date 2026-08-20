package identity

import (
	stderrors "errors"
	"fmt"
)

// The classification of "this sealed blob did not open", which exists because
// the four ways it can happen send an operator to four different places and
// only ONE of them is fixable by waiting.
//
//	wrong KEK      well-formed bytes the AEAD refused → ErrKeyUnsealable, below.
//	               The key at rest and the key in BRIDGE_KEK are different keys.
//	               No amount of retrying changes that; a config change does.
//	corrupt blob   truncated, or an unknown version byte. Rejected before the
//	               cipher is consulted (Custodian.open), so it says nothing
//	               about any KEK: it stays errors.ErrInvalidInput.
//	wrong key type the plaintext opened but is not the key type the caller
//	               needs. The KEK is right; the column holds the wrong thing.
//	missing        no row, no ciphertext: errors.ErrNotFound, and the only one
//	               of the four that a replication lag can produce.
//
// Before this split every one of them arrived as one opaque fmt.Errorf chain,
// and the only consumer that classifies signer errors — the outbound delivery
// worker — read the whole chain as transient. A permanently wrong BRIDGE_KEK
// spent the full retry budget per delivery and then poisoned, hours later,
// under the AEAD's own words: "message authentication failed". That sentence
// sends an operator to look for corrupted bytes. The bytes are fine.

// ErrKeyUnsealable marks a sealed blob that is well formed and failed AEAD
// authentication under EVERY configured KEK — the signal that the material at
// rest was sealed under a key this process does not hold.
//
// It is deliberately NOT a validation error: a caller distinguishing "damaged
// bytes" from "wrong key" has to be able to ask the two questions separately,
// and the reseal drill (reseal.go) already leans on malformed being the
// validation case.
//
// A wrong KEK is the headline cause but not the only one that can produce it:
// a single flipped bit in the ciphertext or the GCM tag lands here too, and so
// does a blob copied out of another row or another column, because every
// ciphertext is AAD-bound to its DID and its domain. What the class means
// precisely is "the AEAD said no", and what follows from that — for every
// caller — is that retrying the same bytes with the same configuration will
// get the same answer forever.
var ErrKeyUnsealable = stderrors.New("identity: sealed key does not open under the configured BRIDGE_KEK")

// KeyUnsealableError names the blob that would not open and the keys that were
// tried, so the message an operator finds in a log line or a dead-letter row
// carries the environment variable they need to grep for.
type KeyUnsealableError struct {
	// AAD is the ciphertext's domain+owner binding, which is also the most
	// useful identifier: it names both the column the blob came from and the
	// DID it belongs to.
	AAD string
	// TriedPrevious reports that BRIDGE_KEK_PREVIOUS was also configured and
	// also failed. Mid-rotation this is the difference between an operator
	// supplying the previous key and an operator discovering that the key they
	// already supplied is not the right one either.
	TriedPrevious bool
}

func (e KeyUnsealableError) Error() string {
	keys := "BRIDGE_KEK"
	if e.TriedPrevious {
		keys = "BRIDGE_KEK or BRIDGE_KEK_PREVIOUS"
	}
	return fmt.Sprintf(
		"identity: sealed key %q does not open under %s: the material at rest was sealed "+
			"under a different key-encryption key than this process is configured with. "+
			"This is a KEK CONFIGURATION problem, not data corruption — retrying cannot "+
			"change the answer (cipher: message authentication failed)",
		e.AAD, keys)
}

// Unwrap makes errors.Is(err, ErrKeyUnsealable) true.
func (e KeyUnsealableError) Unwrap() error { return ErrKeyUnsealable }

// IsKeyUnsealable reports whether err is, wraps, or unwraps to
// ErrKeyUnsealable. It is false for a nil error: a predicate that said yes to
// success would poison every delivery on the bridge.
func IsKeyUnsealable(err error) bool { return stderrors.Is(err, ErrKeyUnsealable) }
