package identity

import (
	"bytes"
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"

	"tidepool/internal/errors"
)

// This file is the KEK re-seal drill: the walk that moves every KEK-sealed
// blob from the previous KEK onto the current one, so an operator mid-rotation
// can finish the job and unset BRIDGE_KEK_PREVIOUS. Run 1 taught the bridge to
// READ under two keys; this is what lets it stop needing to. The counts are
// the operator's gate, and the acceptance test in kek_reseal_test.go says what
// they mean.

// ResealCounts is one table's tally from a re-seal walk. The four buckets are
// disjoint and together account for every row the walk considered.
type ResealCounts struct {
	// Resealed opened under the previous KEK and was written back under the
	// current one.
	Resealed int
	// AlreadyCurrent opened under the current KEK and was left alone. A
	// second run reports everything here — that is the operator's zero-run
	// gate for unsetting BRIDGE_KEK_PREVIOUS.
	AlreadyCurrent int
	// Skipped had nothing to move, with nothing wrong: a NULL signing_key
	// (an actor minted before escrow, or one that opted out before a key was
	// cut), or — in any table — a row that was deleted out from under the
	// walk between its read and its write. A ZERO-LENGTH but non-NULL blob is
	// NOT this: no writer here emits one, so it is damage, and it is counted
	// Failed.
	Skipped int
	// Failed could not be moved onto the current KEK, for any of the reasons
	// in ResealFailureReason — most of them a blob that opened under neither
	// key, but also a row that should exist and does not. Every one of them
	// also appears by name in ResealReport.Failures.
	Failed int
}

// ResealFailureReason classifies why a row could not be re-sealed, because
// the classes send an operator somewhere different: wrong-key is a
// key-history question, malformed is storage corruption, missing is a restore
// to redo, and contended is simply a re-run.
type ResealFailureReason string

const (
	// ResealFailureWrongKey means the blob is well formed — right length,
	// known version byte — but FAILED AUTHENTICATION UNDER BOTH KEYS. A
	// third KEK the bridge no longer holds is the headline cause, but it is
	// not the only one: a single flipped bit anywhere in the ciphertext or
	// the GCM tag lands here, and so does a blob copied from another row or
	// another column, because the AAD binds each ciphertext to its DID and
	// its domain. So this class means "the AEAD said no", not "a KEK is
	// missing" — key history is where to look FIRST, not the only place.
	ResealFailureWrongKey ResealFailureReason = "wrong-key"
	// ResealFailureMalformed means the blob is truncated or carries an
	// unknown version byte: it was never decrypted under either key, so
	// nothing about the KEKs is in question.
	ResealFailureMalformed ResealFailureReason = "malformed"
	// ResealFailureMissing means the row the walk expected is not there at
	// all. Only service_keys 'plc-rotation' can report it, and only on a
	// database that already holds bridged identities: the next boot mints a
	// replacement rotation key, and every DID that names the old one loses
	// its recovery path for good.
	ResealFailureMissing ResealFailureReason = "missing"
	// ResealFailureContended means the row was rewritten by someone else
	// between every read and every guarded write this walk attempted, so the
	// walk never had a safe moment to swap the bytes. Nothing is wrong with
	// the blob or with either KEK: the operator's move is to re-run the
	// drill, which is why this must never be reported as wrong-key.
	ResealFailureContended ResealFailureReason = "contended"
)

// ResealFailure names one row the walk could not move.
type ResealFailure struct {
	// Table is the postgres table name, as an operator would type it.
	Table string
	// ID is the row's did (bridged_actors, ap_actors) or name
	// (service_keys).
	ID string
	// Reason is the class of failure.
	Reason ResealFailureReason
}

// ResealReport is the inventory a re-seal walk hands back.
type ResealReport struct {
	BridgedActors ResealCounts
	APActors      ResealCounts
	ServiceKeys   ResealCounts
	Failures      []ResealFailure
}

// resealBatchSize is how many rows one keyset page carries. The walk pages by
// primary key rather than OFFSET so that rows re-sealed by an earlier page can
// never shift a later page's window and hide a row from the inventory.
const resealBatchSize = 100

// maxResealWriteAttempts caps the read-decide-write cycle for a single row.
// Each lost guard means a concurrent writer already replaced the blob, and
// production writers always seal under the CURRENT KEK, so one retry is enough
// in practice; the cap is what keeps a pathological row from spinning forever.
const maxResealWriteAttempts = 3

// ErrResealIncomplete is the one error class that means the walk RAN TO
// COMPLETION: every table was paged to the end, and the report beside it is
// the whole inventory. Rows in it could not be moved, so the run still exits
// nonzero, but the counts are complete.
//
// Every OTHER error Reseal returns aborts the walk mid-table, which makes the
// accompanying report a prefix rather than an inventory. Callers that print
// the counts need to tell the two apart before they let an operator read the
// numbers as coverage, so the distinction is an errors.Is target rather than
// a string match on the message.
var ErrResealIncomplete = stderrors.New("identity: reseal: sealed blob(s) could not be moved onto the current KEK")

// Reseal walks every KEK-sealed blob and re-seals it under current, so the
// previous KEK can be retired. It returns the report even when it also
// returns an error: an operator running the drill needs the full inventory
// from one pass, not the prefix before the first bad row.
func Reseal(ctx context.Context, db *sql.DB, current, previous []byte) (*ResealReport, error) {
	// Two SINGLE-KEK custodians, not one dual-read custodian: the whole job
	// here is telling apart "opened under current" from "opened under
	// previous", and a custodian that transparently tries both cannot say
	// which one worked.
	underCurrent, err := NewCustodian(current)
	if err != nil {
		return nil, fmt.Errorf("identity: reseal: current KEK: %w", err)
	}
	if len(previous) == 0 {
		return nil, errors.NewValidationError("bridge_kek_previous",
			"must be set to re-seal: with no previous KEK there is nothing to move onto the current one")
	}
	// One key in both roles is not a rotation, and the walk's output would be
	// the most dangerous report it can produce: every blob AlreadyCurrent,
	// nothing re-sealed, zero failures — the exact shape of the clean zero-run
	// that clears an operator to unset BRIDGE_KEK_PREVIOUS, over a database
	// where every blob is still sealed under it.
	//
	// The guard lives HERE, not only in the callers that happen to have one
	// today (config.Load at boot, runRotateKEK before it dials), because the
	// two keys arrive as adjacent []byte parameters of an exported function:
	// whoever calls it next inherits the check instead of re-deriving it.
	if bytes.Equal(current, previous) {
		return nil, errors.NewValidationError("bridge_kek_previous",
			"decodes to the same key as BRIDGE_KEK; a rotation needs two different keys, and re-sealing a key onto itself would report a clean zero-run over a database nothing had moved off")
	}
	underPrevious, err := NewCustodian(previous)
	if err != nil {
		return nil, fmt.Errorf("identity: reseal: previous KEK: %w", err)
	}

	walk := &resealer{
		db:            db,
		underCurrent:  underCurrent,
		underPrevious: underPrevious,
		report:        &ResealReport{},
	}
	for _, domain := range []func(context.Context) error{
		walk.bridgedActors,
		walk.apActors,
		walk.rotationKey,
	} {
		if err := domain(ctx); err != nil {
			return walk.report, err
		}
	}

	// The gate reads the failure LIST, not a hand-rolled sum of the per-table
	// Failed counters. Both are written by the same fail() choke point, so
	// they agree today — but a fourth sealed domain added to the loop above
	// would arrive with a fourth ResealCounts that a sum has to be taught
	// about by hand, and forgetting to would let its failures exit zero. The
	// list needs no such maintenance.
	if len(walk.report.Failures) > 0 {
		// The report alone is not enough: a zero exit is what a deploy script
		// reads as permission to unset BRIDGE_KEK_PREVIOUS, and doing that
		// with unmoved blobs destroys them.
		return walk.report, fmt.Errorf("%w: %d row(s); BRIDGE_KEK_PREVIOUS must stay set",
			ErrResealIncomplete, len(walk.report.Failures))
	}
	return walk.report, nil
}

// resealer carries the walk's state across the three sealed domains.
type resealer struct {
	db            *sql.DB
	underCurrent  *Custodian
	underPrevious *Custodian
	report        *ResealReport
	// sawIdentities records that the walk found at least one bridged_actors
	// or ap_actors row. It is what lets rotationKey tell a fresh install
	// (no identities, no rotation key, nothing wrong) from a restore that
	// dropped the rotation key out from under identities that name it. The
	// domain loop runs the two actor tables before rotationKey, so this is
	// always settled by the time it is read.
	sawIdentities bool
}

// resealOutcome is what the walk decided to do with one blob.
type resealOutcome int

const (
	outcomeAlreadyCurrent resealOutcome = iota
	outcomeReseal
	outcomeFailed
)

// resealDecision is the verdict on one blob's bytes.
type resealDecision struct {
	// fresh is the re-sealed replacement, set only for outcomeReseal.
	fresh   []byte
	outcome resealOutcome
	// reason is set only for outcomeFailed.
	reason ResealFailureReason
}

// blobRow is one sealed value the walk is trying to move, together with the
// two statements that can act on it. Both statements are dedicated SQL rather
// than store methods: bridged_actors' UpsertActor deliberately freezes rows
// whose consent_state is 'deleted', and a tombstoned actor's key is exactly
// the one still needed (it is what scrubs their records), so the re-seal must
// not travel that path.
type blobRow struct {
	table string
	// id is the did or key name, as an operator would type it into a WHERE.
	id   string
	aad  []byte
	blob []byte
	// writeBack swaps old for fresh guarded on old, returning rows affected;
	// 0 means a concurrent writer replaced the bytes first.
	writeBack func(ctx context.Context, fresh, old []byte) (int64, error)
	// reread returns the row's bytes as they stand now, or nil if the row is
	// gone.
	reread func(ctx context.Context) ([]byte, error)
}

// decide classifies one blob and, when it opens under the previous KEK only,
// returns its replacement sealed under the current one.
func (r *resealer) decide(blob, aad []byte) (resealDecision, error) {
	if _, err := r.underCurrent.open(blob, aad); err == nil {
		return resealDecision{outcome: outcomeAlreadyCurrent}, nil
	} else if errors.IsValidation(err) {
		// A short blob or an unknown version byte is rejected before the AEAD
		// is ever consulted, so no KEK is in question and the previous key
		// would fail identically: this is storage corruption, not key history.
		return resealDecision{outcome: outcomeFailed, reason: ResealFailureMalformed}, nil
	}

	plaintext, err := r.underPrevious.open(blob, aad)
	if err != nil {
		return resealDecision{outcome: outcomeFailed, reason: ResealFailureWrongKey}, nil
	}
	fresh, err := r.underCurrent.seal(plaintext, aad)
	if err != nil {
		return resealDecision{}, fmt.Errorf("identity: reseal %s: %w", aad, err)
	}
	return resealDecision{fresh: fresh, outcome: outcomeReseal}, nil
}

// move applies one row's decision and tallies it. A blob the walk could not
// open is counted and left byte-identical: overwriting it would destroy the
// one copy a recovered third KEK — or a single-row restore — could still fix.
func (r *resealer) move(ctx context.Context, counts *ResealCounts, row blobRow) error {
	blob := row.blob
	for attempt := 0; attempt < maxResealWriteAttempts; attempt++ {
		decision, err := r.decide(blob, row.aad)
		if err != nil {
			return err
		}
		switch decision.outcome {
		case outcomeAlreadyCurrent:
			counts.AlreadyCurrent++
			return nil
		case outcomeFailed:
			r.fail(counts, row.table, row.id, decision.reason)
			return nil
		}

		affected, err := row.writeBack(ctx, decision.fresh, blob)
		if err != nil {
			return fmt.Errorf("identity: reseal: write back %s %s: %w", row.table, row.id, err)
		}
		if affected > 0 {
			counts.Resealed++
			return nil
		}

		// The guard lost: someone rewrote the row between the read and the
		// UPDATE. Re-read and decide again on the bytes that are actually
		// there rather than clobbering a value this walk never opened.
		blob, err = row.reread(ctx)
		if err != nil {
			return fmt.Errorf("identity: reseal: re-read %s %s: %w", row.table, row.id, err)
		}
		if blob == nil {
			// The row was deleted, or its nullable key column was cleared,
			// while the walk ran: there is no longer anything to move. Only
			// a NIL blob means that. A non-nil EMPTY one is a row that is
			// still there holding zero bytes, which no writer here produces
			// — so it goes back around the loop and decide() classifies it.
			counts.Skipped++
			return nil
		}
	}
	r.fail(counts, row.table, row.id, ResealFailureContended)
	return nil
}

func (r *resealer) fail(counts *ResealCounts, table, id string, reason ResealFailureReason) {
	counts.Failed++
	r.report.Failures = append(r.report.Failures, ResealFailure{Table: table, ID: id, Reason: reason})
}

// bridgedActors moves the escrowed atproto signing keys.
func (r *resealer) bridgedActors(ctx context.Context) error {
	type actorRow struct {
		id   int64
		did  string
		blob []byte
	}
	var lastID int64
	for {
		// Keyset pagination on the BIGSERIAL pk: the walk rewrites the very
		// column it is reading, so an OFFSET window could shift under it and
		// skip rows the inventory then claims to have covered.
		rows, err := r.db.QueryContext(ctx, `
			SELECT id, did, signing_key
			FROM bridged_actors
			WHERE id > $1
			ORDER BY id
			LIMIT $2`, lastID, resealBatchSize)
		if err != nil {
			return fmt.Errorf("identity: reseal: list bridged_actors: %w", err)
		}
		var page []actorRow
		for rows.Next() {
			var row actorRow
			if err := rows.Scan(&row.id, &row.did, &row.blob); err != nil {
				rows.Close()
				return fmt.Errorf("identity: reseal: scan bridged_actor: %w", err)
			}
			page = append(page, row)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("identity: reseal: read bridged_actors: %w", err)
		}
		rows.Close()
		if len(page) == 0 {
			return nil
		}

		r.sawIdentities = true
		for _, row := range page {
			lastID = row.id
			if row.blob == nil {
				// NULL signing_key: minted before escrow, or opted out before
				// a key was cut. Nothing to move, and nothing wrong.
				//
				// NIL, not zero-length. A NULL column scans as a nil slice; a
				// zero-length bytea scans as a non-nil empty one, and that is
				// a row someone or something damaged — seal() never emits
				// fewer than a version byte, a nonce and a tag. Letting it
				// fall through to move() sends it to decide(), whose length
				// check classifies it malformed, so the operator hears about
				// it instead of finding it in the bucket labelled "nothing
				// wrong".
				r.report.BridgedActors.Skipped++
				continue
			}
			id := row.id
			err := r.move(ctx, &r.report.BridgedActors, blobRow{
				table: "bridged_actors",
				id:    row.did,
				aad:   []byte(actorKeyAADPrefix + row.did),
				blob:  row.blob,
				writeBack: func(ctx context.Context, fresh, old []byte) (int64, error) {
					result, err := r.db.ExecContext(ctx, `
						UPDATE bridged_actors SET signing_key = $2
						WHERE id = $1 AND signing_key = $3`, id, fresh, old)
					if err != nil {
						return 0, err
					}
					return result.RowsAffected()
				},
				reread: func(ctx context.Context) ([]byte, error) {
					return r.rereadBlob(ctx, `SELECT signing_key FROM bridged_actors WHERE id = $1`, id)
				},
			})
			if err != nil {
				return err
			}
		}
	}
}

// apActors moves the Coves users' AP-side RSA signing keys.
func (r *resealer) apActors(ctx context.Context) error {
	type apRow struct {
		did  string
		blob []byte
	}
	lastDID := ""
	for {
		// ap_actors is keyed by did, so that is the keyset column.
		rows, err := r.db.QueryContext(ctx, `
			SELECT did, rsa_key_sealed
			FROM ap_actors
			WHERE did > $1
			ORDER BY did
			LIMIT $2`, lastDID, resealBatchSize)
		if err != nil {
			return fmt.Errorf("identity: reseal: list ap_actors: %w", err)
		}
		var page []apRow
		for rows.Next() {
			var row apRow
			if err := rows.Scan(&row.did, &row.blob); err != nil {
				rows.Close()
				return fmt.Errorf("identity: reseal: scan ap_actor: %w", err)
			}
			page = append(page, row)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("identity: reseal: read ap_actors: %w", err)
		}
		rows.Close()
		if len(page) == 0 {
			return nil
		}

		r.sawIdentities = true
		for _, row := range page {
			lastDID = row.did
			did := row.did
			err := r.move(ctx, &r.report.APActors, blobRow{
				table: "ap_actors",
				id:    did,
				aad:   []byte(actorRSAKeyAADPrefix + did),
				blob:  row.blob,
				writeBack: func(ctx context.Context, fresh, old []byte) (int64, error) {
					result, err := r.db.ExecContext(ctx, `
						UPDATE ap_actors SET rsa_key_sealed = $2
						WHERE did = $1 AND rsa_key_sealed = $3`, did, fresh, old)
					if err != nil {
						return 0, err
					}
					return result.RowsAffected()
				},
				reread: func(ctx context.Context) ([]byte, error) {
					return r.rereadBlob(ctx, `SELECT rsa_key_sealed FROM ap_actors WHERE did = $1`, did)
				},
			})
			if err != nil {
				return err
			}
		}
	}
}

// rotationKey moves the escrow rotation key — and only that row. service_keys
// stores per-row encodings in one column: 'plc-rotation' is KEK ciphertext,
// but the 'service-actor' row beside it is plaintext PEM (migration 013), and
// sealing that would leave every reader parsing ciphertext as PEM. The name
// filter is why the walk can never even read it into a bucket.
func (r *resealer) rotationKey(ctx context.Context) error {
	var blob []byte
	err := r.db.QueryRowContext(ctx,
		`SELECT key_material FROM service_keys WHERE name = $1`, RotationKeyName).Scan(&blob)
	if stderrors.Is(err, sql.ErrNoRows) {
		if !r.sawIdentities {
			// A fresh install: no identities, and therefore no boot has ever
			// needed a rotation key. The next one mints it under the current
			// KEK, which is exactly right. Nothing to move, nothing wrong,
			// and no bucket to put it in.
			return nil
		}
		// A POPULATED database missing its rotation key is the opposite
		// story. Those bridged identities each name this key as their
		// rotation authority in their DID document, so the row existed when
		// they were minted and something — almost always a restore that
		// missed service_keys — has since lost it. LoadOrCreateRotationKey
		// cannot tell: the next boot mints a REPLACEMENT, and every one of
		// those DIDs is left naming a key nobody holds, permanently beyond
		// recovery.
		//
		// The walk cannot fix it (the bytes are gone), so it does the one
		// thing that helps: counts it Failed, which makes the run exit
		// nonzero and stops the deploy script before that boot happens, and
		// puts it in the report by name so the operator restores the row
		// rather than restarting the service.
		r.fail(&r.report.ServiceKeys, "service_keys", RotationKeyName, ResealFailureMissing)
		return nil
	}
	if err != nil {
		return fmt.Errorf("identity: reseal: read rotation key: %w", err)
	}
	return r.move(ctx, &r.report.ServiceKeys, blobRow{
		table: "service_keys",
		id:    RotationKeyName,
		aad:   []byte(rotationKeyAAD),
		blob:  blob,
		writeBack: func(ctx context.Context, fresh, old []byte) (int64, error) {
			result, err := r.db.ExecContext(ctx, `
				UPDATE service_keys SET key_material = $2
				WHERE name = $1 AND key_material = $3`, RotationKeyName, fresh, old)
			if err != nil {
				return 0, err
			}
			return result.RowsAffected()
		},
		reread: func(ctx context.Context) ([]byte, error) {
			return r.rereadBlob(ctx, `SELECT key_material FROM service_keys WHERE name = $1`, RotationKeyName)
		},
	})
}

// rereadBlob fetches a single blob after a lost optimistic guard. A row that
// vanished reads as nil, not as an error: the walk has nothing left to move.
func (r *resealer) rereadBlob(ctx context.Context, query string, arg any) ([]byte, error) {
	var blob []byte
	err := r.db.QueryRowContext(ctx, query, arg).Scan(&blob)
	if stderrors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return blob, nil
}
