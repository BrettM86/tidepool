package identity

import (
	"crypto/rsa"
	"crypto/x509"
	"fmt"

	"tidepool/internal/errors"
)

// This file holds the custodian's AP-side RSA surface (task 13): per-actor
// RSA private keys sealed under the bridge KEK, AAD-bound to their DID with
// a constant DISTINCT from actorKeyAADPrefix (the K256 escrow keys), so a
// ciphertext moved between columns fails to open. New keys never touch the
// plaintext-PEM path the v1 SERVICE-actor key still travels — that key is
// PEM-encoded by ap.EncodePrivateKeyPEM and stored in service_keys as text
// (see ap.LoadOrCreateServiceActor); every per-actor AP key minted here is
// ciphertext at rest instead.
//
// The sealed plaintext is PKCS#8 DER, not PEM: PEM is base64 with a header,
// so it would be ~40% larger and would put the string "-----BEGIN" inside a
// value that is only ever handled as opaque bytes. DER keeps the ciphertext
// tight and keeps the accidental-plaintext greps honest.

// EncryptActorRSAKey seals a Coves user's AP signing key for storage in
// ap_actors.rsa_key_sealed, bound to the owning DID. Decrypting the result
// under any other DID — or under the K256 escrow AAD — fails authentication.
func (c *Custodian) EncryptActorRSAKey(did string, key *rsa.PrivateKey) ([]byte, error) {
	if did == "" {
		return nil, errors.NewValidationError("did", "must not be empty")
	}
	if key == nil {
		return nil, errors.NewValidationError("key", "must not be nil")
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, fmt.Errorf("identity: marshal AP RSA key for %s: %w", did, err)
	}
	return c.seal(der, []byte(actorRSAKeyAADPrefix+did))
}

// DecryptActorRSAKey opens a sealed AP signing key. did must be the DID the
// key was sealed for; an empty DID is refused outright rather than being
// folded into an AAD that could only ever fail, so the caller sees a
// validation error instead of a bare authentication failure.
func (c *Custodian) DecryptActorRSAKey(did string, ciphertext []byte) (*rsa.PrivateKey, error) {
	if did == "" {
		return nil, errors.NewValidationError("did", "must not be empty")
	}
	der, err := c.open(ciphertext, []byte(actorRSAKeyAADPrefix+did))
	if err != nil {
		return nil, fmt.Errorf("identity: decrypt AP RSA key for %s: %w", did, err)
	}
	parsed, err := x509.ParsePKCS8PrivateKey(der)
	if err != nil {
		return nil, fmt.Errorf("identity: parse AP RSA key for %s: %w", did, err)
	}
	key, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("identity: AP key for %s is %T, want *rsa.PrivateKey", did, parsed)
	}
	return key, nil
}
