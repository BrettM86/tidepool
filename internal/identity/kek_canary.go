package identity

import (
	"context"
	"database/sql"
	"fmt"

	"tidepool/internal/errors"
)

// The boot canary's proof of the KEK.
//
// Opening the plc-rotation row is what normally proves BRIDGE_KEK at startup:
// the row is sealed, it opens, the key is right. That proof has a hole exactly
// where the row is ABSENT — a restore that missed service_keys, a
// hand-provisioned deploy, a database attached to a service that never booted
// against it. LoadOrCreateRotationKey then takes its create branch, seals a
// fresh key under whatever BRIDGE_KEK it was handed, and reports success. The
// canary satisfied itself with a row it wrote a microsecond earlier.
//
// What follows is the most confusing failure this bridge can produce: newly
// minted actors seal and open under the wrong key perfectly consistently and
// federate fine, while every actor minted before the change fails to unseal,
// one delivery at a time. Half the bridge works. Nothing points at the config.
//
// So when there is nothing at rest to read, the KEK is proven against the OTHER
// sealed material instead — the actor keys — and only a database with none of
// it at all is allowed to mint.

// kekProbeSampleSize is how many sealed rows per table the probe reads.
//
// More than one, because a single damaged blob would otherwise fail a boot on a
// perfectly good KEK. Few, because this runs on the startup path and one row
// that opens is a complete proof: the question is whether this process holds
// the key the database was sealed with, and any one row answers it.
const kekProbeSampleSize = 5

// sealedSample is one blob the probe tried, with the binding it was sealed
// under.
type sealedSample struct {
	table string
	id    string
	aad   []byte
	blob  []byte
}

// VerifyKEKAgainstSealedMaterial reports whether custodian can open the key
// material this database already holds.
//
// It returns nil in the two healthy cases and only in those: at least one
// sampled blob opened (the KEK is proven), or the database holds no sealed
// actor material at all (a fresh install, where there is nothing to be wrong
// about). A sample that is entirely unsealable returns an error satisfying
// IsKeyUnsealable.
//
// It is exported because it is a boot-time assertion an operator may want to
// run on its own — a preflight before a KEK change, and the check a restore
// drill wants before it declares the restore good.
func VerifyKEKAgainstSealedMaterial(ctx context.Context, db *sql.DB, custodian *Custodian) error {
	if db == nil {
		return errors.NewValidationError("db", "must not be nil: the KEK cannot be proven without the material at rest")
	}
	samples, err := sampleSealedMaterial(ctx, db)
	if err != nil {
		return err
	}
	if len(samples) == 0 {
		// Nothing sealed anywhere. A fresh install, and the only state in which
		// an unproven KEK is the right answer: there is no material it could be
		// wrong about, and the keys minted from here on define what "right"
		// means for this database.
		return nil
	}

	var unsealable, malformed int
	var firstUnsealable error
	for _, sample := range samples {
		switch _, err := custodian.open(sample.blob, sample.aad); {
		case err == nil:
			// One row that opens settles it. Whatever else is in the table,
			// this process holds the key this database was sealed with.
			return nil
		case IsKeyUnsealable(err):
			unsealable++
			if firstUnsealable == nil {
				firstUnsealable = fmt.Errorf("identity: %s %s: %w", sample.table, sample.id, err)
			}
		case errors.IsValidation(err):
			// Damaged bytes: rejected on shape before any key was consulted, so
			// this row is not evidence either way. It is counted so that a
			// sample made ENTIRELY of damage cannot be mistaken for a proof.
			malformed++
		default:
			return fmt.Errorf("identity: verify KEK against %s %s: %w", sample.table, sample.id, err)
		}
	}

	if unsealable > 0 {
		return fmt.Errorf(
			"identity: BRIDGE_KEK does not open the key material already in this database "+
				"(%d of %d sampled actor keys refused, %d unreadable): %w",
			unsealable, len(samples), malformed, firstUnsealable)
	}
	// Every sampled row was damaged, so the KEK is neither proven nor
	// disproven. Booting on regardless would mint a rotation key under an
	// unverified key over a populated database, which is the failure this whole
	// file exists to prevent, so the refusal stands — with its own wording,
	// because the operator's move here is a restore, not a config change.
	return fmt.Errorf(
		"identity: cannot verify BRIDGE_KEK: all %d sampled sealed keys in this database are "+
			"malformed (truncated or an unknown ciphertext version), so none of them can prove "+
			"or disprove the configured key; refusing to mint an escrow rotation key over a "+
			"populated database whose key material cannot be read", malformed)
}

// sampleSealedMaterial reads a few sealed blobs from each actor table, newest
// binding first, skipping the NULL and zero-length columns that carry no
// ciphertext to test.
func sampleSealedMaterial(ctx context.Context, db *sql.DB) ([]sealedSample, error) {
	queries := []struct {
		table  string
		aadPre string
		query  string
	}{
		{"ap_actors", actorRSAKeyAADPrefix, `
			SELECT did, rsa_key_sealed FROM ap_actors
			WHERE rsa_key_sealed IS NOT NULL AND octet_length(rsa_key_sealed) > 0
			ORDER BY did LIMIT $1`},
		{"bridged_actors", actorKeyAADPrefix, `
			SELECT did, signing_key FROM bridged_actors
			WHERE signing_key IS NOT NULL AND octet_length(signing_key) > 0
			ORDER BY id LIMIT $1`},
	}

	var samples []sealedSample
	for _, q := range queries {
		rows, err := db.QueryContext(ctx, q.query, kekProbeSampleSize)
		if err != nil {
			return nil, fmt.Errorf("identity: sample sealed %s: %w", q.table, err)
		}
		for rows.Next() {
			var did string
			var blob []byte
			if err := rows.Scan(&did, &blob); err != nil {
				rows.Close()
				return nil, fmt.Errorf("identity: scan sealed %s: %w", q.table, err)
			}
			samples = append(samples, sealedSample{
				table: q.table, id: did, aad: []byte(q.aadPre + did), blob: blob,
			})
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("identity: read sealed %s: %w", q.table, err)
		}
		rows.Close()
	}
	return samples, nil
}
