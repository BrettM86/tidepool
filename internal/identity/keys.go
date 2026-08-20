// Package identity mints and custodies the atproto identities of bridged
// fediverse actors: did:plc creation against a PLC directory, per-actor
// secp256k1 signing keys held in escrow (AES-GCM encrypted at rest), and
// handle resolution for the bridge's subdomain handle space.
package identity

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"fmt"

	"github.com/bluesky-social/indigo/atproto/atcrypto"

	"tidepool/internal/errors"
	"tidepool/internal/repo"
	"tidepool/internal/store"
)

// KEKSize is the required byte length of the bridge key-encryption key
// (BRIDGE_KEK): 32 bytes for AES-256-GCM.
const KEKSize = 32

// ciphertextVersion prefixes every sealed key so the format can evolve
// (e.g. KEK rotation with key IDs) without a schema change.
const ciphertextVersion byte = 1

// RotationKeyName is the service_keys row holding the bridge's escrow
// rotation key (encrypted with the KEK, unlike the AP-side RSA key that
// task 02 documented as stored in the clear — the rotation key controls
// every bridged DID, so it gets the stronger treatment).
const RotationKeyName = "plc-rotation"

// aadContext strings bind each ciphertext to its purpose and owner so a
// sealed key copied into another row (or another column) fails to open.
const (
	actorKeyAADPrefix = "tidepool:actor-signing-key:v1:"
	// actorRSAKeyAADPrefix seals the AP-side RSA keys of task 13. It is
	// deliberately distinct from actorKeyAADPrefix even though both bind the
	// same DID under the same KEK: the atproto escrow key and the
	// ActivityPub signing key are different trust domains, and a ciphertext
	// that wandered from one column into the other must fail to open rather
	// than silently authenticate in the wrong domain.
	actorRSAKeyAADPrefix = "tidepool:actor-rsa-key:v1:"
	rotationKeyAAD       = "tidepool:plc-rotation-key:v1"
)

// Custodian seals and opens the bridge's two per-actor key domains with the
// bridge KEK (AES-256-GCM): the atproto secp256k1 escrow keys of bridged
// fediverse actors (EncryptActorKey/DecryptActorKey) and the ActivityPub RSA
// keys of Coves users' own actors (EncryptActorRSAKey/DecryptActorRSAKey, in
// actor_rsa.go). One KEK, two AAD prefixes: a ciphertext from one domain
// must never open in the other, so the constants above are distinct by
// construction and a cross test pins it.
//
// Key claiming/migration is out of scope for v1, but the storage design
// allows it later: each actor's key is independent, bound to its DID via AAD,
// and exportable by decrypting and handing the key material to the user
// during a future claim flow.
type Custodian struct {
	// aead is built once at construction and reused: DecryptActorKey sits
	// on every commit's hot path, and cipher.AEAD is safe for concurrent
	// use.
	aead cipher.AEAD
	// previous is the KEK being rotated away from, nil outside a rotation.
	// It only ever opens: seal uses aead unconditionally, so material written
	// during the rotation window is readable by the current key alone and the
	// operator can eventually drop the old one.
	previous cipher.AEAD
}

// NewCustodian validates the KEK and returns a Custodian.
func NewCustodian(kek []byte) (*Custodian, error) {
	if len(kek) != KEKSize {
		return nil, errors.NewValidationError("bridge_kek",
			fmt.Sprintf("must be %d bytes, got %d", KEKSize, len(kek)))
	}
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, fmt.Errorf("identity: init AES: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("identity: init GCM: %w", err)
	}
	return &Custodian{aead: gcm}, nil
}

// NewCustodianWithPrevious returns a Custodian that SEALS under current and
// OPENS under current or previous, so an operator can rotate the bridge KEK
// without orphaning key material sealed under the old one. previous may be
// nil, in which case the result behaves exactly like NewCustodian(current).
//
// A wrong-length previous key is rejected here rather than at first use: the
// alternative is discovering it only when some pre-rotation key fails to open,
// long after the deploy that introduced it.
func NewCustodianWithPrevious(current, previous []byte) (*Custodian, error) {
	custodian, err := NewCustodian(current)
	if err != nil {
		return nil, err
	}
	if len(previous) == 0 {
		return custodian, nil
	}
	if len(previous) != KEKSize {
		return nil, errors.NewValidationError("bridge_kek_previous",
			fmt.Sprintf("must be %d bytes, got %d", KEKSize, len(previous)))
	}
	block, err := aes.NewCipher(previous)
	if err != nil {
		return nil, fmt.Errorf("identity: init AES for previous KEK: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("identity: init GCM for previous KEK: %w", err)
	}
	custodian.previous = gcm
	return custodian, nil
}

// EncryptActorKey seals an actor's signing key for storage in
// bridged_actors.signing_key. The ciphertext is bound to the actor's DID:
// decrypting it under any other DID fails authentication.
func (c *Custodian) EncryptActorKey(did string, key *atcrypto.PrivateKeyK256) ([]byte, error) {
	if did == "" {
		return nil, errors.NewValidationError("did", "must not be empty")
	}
	return c.seal(key.Bytes(), []byte(actorKeyAADPrefix+did))
}

// DecryptActorKey opens a sealed actor signing key from
// bridged_actors.signing_key. did must be the DID the key was sealed for.
func (c *Custodian) DecryptActorKey(did string, ciphertext []byte) (*atcrypto.PrivateKeyK256, error) {
	raw, err := c.open(ciphertext, []byte(actorKeyAADPrefix+did))
	if err != nil {
		return nil, fmt.Errorf("identity: decrypt signing key for %s: %w", did, err)
	}
	key, err := atcrypto.ParsePrivateBytesK256(raw)
	if err != nil {
		return nil, fmt.Errorf("identity: parse signing key for %s: %w", did, err)
	}
	return key, nil
}

// seal encrypts plaintext with AES-256-GCM under the KEK. Layout:
// version byte || 12-byte random nonce || GCM ciphertext+tag.
func (c *Custodian) seal(plaintext, aad []byte) ([]byte, error) {
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("identity: generate nonce: %w", err)
	}
	out := make([]byte, 0, 1+len(nonce)+len(plaintext)+c.aead.Overhead())
	out = append(out, ciphertextVersion)
	out = append(out, nonce...)
	return c.aead.Seal(out, nonce, plaintext, aad), nil
}

// open reverses seal.
func (c *Custodian) open(ciphertext, aad []byte) ([]byte, error) {
	if len(ciphertext) < 1+c.aead.NonceSize()+c.aead.Overhead() {
		return nil, errors.NewValidationError("ciphertext", "too short to be a sealed key")
	}
	if ciphertext[0] != ciphertextVersion {
		return nil, errors.NewValidationError("ciphertext",
			fmt.Sprintf("unknown ciphertext version %d", ciphertext[0]))
	}
	nonce := ciphertext[1 : 1+c.aead.NonceSize()]
	sealed := ciphertext[1+c.aead.NonceSize():]
	plaintext, err := c.aead.Open(nil, nonce, sealed, aad)
	if err == nil {
		return plaintext, nil
	}
	// The format checks above ran first, so reaching here means the blob is
	// well-formed and merely failed GCM authentication — the one condition a
	// second KEK can fix. A truncated or wrong-version blob never gets here,
	// and so is never misreported as a key problem.
	if c.previous != nil {
		// Same aad: the retry re-tries the KEK, never the domain binding, so
		// a ciphertext that wandered between AAD domains stays rejected.
		if plaintext, prevErr := c.previous.Open(nil, nonce, sealed, aad); prevErr == nil {
			return plaintext, nil
		}
	}
	// EVERY configured key has now refused well-formed bytes, which is a
	// CLASS, not a message: no caller can fix it by trying again, and the
	// operator's move is to look at BRIDGE_KEK. Returning it typed is what lets
	// the delivery worker poison immediately instead of spending a retry budget
	// on an answer that cannot change (worker.go, the SignerFor branch).
	//
	// The text deliberately differs between the single-KEK and mid-rotation
	// cases — it used to be byte-identical so that pre-rotation alerts still
	// matched — because "BRIDGE_KEK failed" during a rotation reads as an
	// instruction to supply the previous key that is already configured and
	// already failing. Anything matching on the old wording should match on
	// ErrKeyUnsealable instead, which is exactly why it exists.
	return nil, KeyUnsealableError{AAD: string(aad), TriedPrevious: c.previous != nil}
}

// LoadOrCreateRotationKey returns the bridge's escrow rotation key,
// generating and persisting it (sealed with the KEK) on first run. The
// service_keys create-once semantics make the bootstrap race safe: a loser
// re-reads the winner's key.
//
// It is also the bridge's BOOT CANARY for BRIDGE_KEK, which is why it takes the
// database and not just the service_keys store. On the load path the canary is
// the load itself: a row that will not open fails the boot. On the CREATE path
// there is nothing to open, so the KEK is proven against the actor keys already
// at rest instead (VerifyKEKAgainstSealedMaterial) — without that step the
// create branch seals a fresh rotation key under whatever key it was handed and
// reports success, which is a canary satisfying itself with a row it just wrote.
//
// db must not be nil. The alternative — treating a nil database as "skip the
// check" — puts the hole back one caller at a time.
func LoadOrCreateRotationKey(ctx context.Context, db *sql.DB, keys store.ServiceKeys, custodian *Custodian) (*atcrypto.PrivateKeyK256, error) {
	if db == nil {
		return nil, errors.NewValidationError("db",
			"must not be nil: LoadOrCreateRotationKey is the boot canary for BRIDGE_KEK and needs the material at rest to check it against")
	}
	stored, err := keys.Get(ctx, RotationKeyName)
	if err == nil {
		return decryptRotationKey(custodian, stored.KeyMaterial)
	}
	if !errors.IsNotFound(err) {
		return nil, fmt.Errorf("identity: load rotation key: %w", err)
	}

	// The create branch, and the one place a wrong KEK could otherwise pass
	// unnoticed. On a populated database this refuses; on an empty one it is a
	// no-op and the first boot mints as it always did.
	if err := VerifyKEKAgainstSealedMaterial(ctx, db, custodian); err != nil {
		return nil, fmt.Errorf("identity: refusing to mint an escrow rotation key: %w", err)
	}

	fresh, err := atcrypto.GeneratePrivateKeyK256()
	if err != nil {
		return nil, fmt.Errorf("identity: generate rotation key: %w", err)
	}
	sealed, err := custodian.seal(fresh.Bytes(), []byte(rotationKeyAAD))
	if err != nil {
		return nil, fmt.Errorf("identity: seal rotation key: %w", err)
	}
	// NOTE: key_material holds plaintext PEM for the RSA service-actor row
	// but sealed ciphertext for this one; the per-row encoding is documented
	// on the column (migration 013).
	if _, err := keys.Create(ctx, RotationKeyName, sealed); err != nil {
		if errors.IsAlreadyExists(err) {
			// Lost the bootstrap race: use the winner's key.
			winner, getErr := keys.Get(ctx, RotationKeyName)
			if getErr != nil {
				return nil, fmt.Errorf("identity: reload rotation key after race: %w", getErr)
			}
			return decryptRotationKey(custodian, winner.KeyMaterial)
		}
		return nil, fmt.Errorf("identity: persist rotation key: %w", err)
	}
	return fresh, nil
}

func decryptRotationKey(custodian *Custodian, sealed []byte) (*atcrypto.PrivateKeyK256, error) {
	raw, err := custodian.open(sealed, []byte(rotationKeyAAD))
	if err != nil {
		return nil, fmt.Errorf("identity: decrypt rotation key: %w", err)
	}
	key, err := atcrypto.ParsePrivateBytesK256(raw)
	if err != nil {
		return nil, fmt.Errorf("identity: parse rotation key: %w", err)
	}
	return key, nil
}

// ActorKeys resolves the signing key for a bridged DID, for the repo layer
// to sign commits with. It implements repo.SigningKeys.
type ActorKeys struct {
	actors    store.BridgedActors
	custodian *Custodian
}

// NewActorKeys builds the store-backed signing-key resolver.
func NewActorKeys(actors store.BridgedActors, custodian *Custodian) *ActorKeys {
	return &ActorKeys{actors: actors, custodian: custodian}
}

// SigningKey returns the decrypted signing key for a bridged DID. The
// consent gate is per KeyUse: a tombstoned actor (consent_state=deleted,
// terminal) never gets a key for repo.KeyUseWrite — that freeze is how
// deleted repos are prevented from growing — but repo.KeyUseDelete still
// releases the key, because scrubbing a deleted actor's records must always
// be possible (that IS the consent intent; task 05's Delete(Actor) →
// scrub-records flow depends on it). The write-freeze error satisfies
// errors.IsTombstoned; an actor without an escrowed key satisfies
// errors.IsNotFound.
func (a *ActorKeys) SigningKey(ctx context.Context, did string, use repo.KeyUse) (atcrypto.PrivateKey, error) {
	actor, err := a.actors.GetByDID(ctx, did)
	if err != nil {
		return nil, err
	}
	if actor.ConsentState == store.ConsentStateDeleted && use != repo.KeyUseDelete {
		return nil, errors.NewTombstonedError("bridged_actor", did)
	}
	if len(actor.SigningKeyEncrypted) == 0 {
		return nil, errors.NewNotFoundError("signing_key", did)
	}
	return a.custodian.DecryptActorKey(did, actor.SigningKeyEncrypted)
}
