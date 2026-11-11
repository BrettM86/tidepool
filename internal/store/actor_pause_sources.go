package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"hash/fnv"

	"tidepool/internal/errors"
)

// Pause source kinds: what keeps a persona's delivery paused. The persona is
// paused while any source stands, and each source is added and removed only by
// its own path, so a label lift never clears a status pause or the reverse.
const (
	PauseSourceAccountLabel  = "account-label"
	PauseSourceAccountStatus = "account-status"
)

// PauseSource is one member of a persona's pause source set. Status is the
// #account status an account-status source last applied ("" for a label).
type PauseSource struct{ Kind, Key, Status string }

// setKey is the source's key in ap_actors.pause_sources.
func (s PauseSource) setKey() string { return s.Kind + ":" + s.Key }

// AccountLockNamespace scopes the per-DID account advisory locks ("ac"). The
// #account claim, the account label applier and persona minting all take the
// lock for a DID, so a label, a status change and a mint of one DID serialize.
const AccountLockNamespace = 0x6163

// AccountLockKey is the per-DID half of the account advisory lock key. It is
// FNV-1a computed in Go so every caller derives the key the same way without
// a SQL round trip. hashtext() would also work: xact advisory locks live only
// for one transaction, so the key never needs to stay stable across Postgres
// versions. A collision only serializes two DIDs against each other.
func AccountLockKey(did string) int32 {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(did)) // hash.Write never returns an error
	return int32(hash.Sum32())     //nolint:gosec // wraparound is fine: this is a lock key, not a value
}

// LockAccountTx takes did's account advisory lock in tx, held until tx ends.
func LockAccountTx(ctx context.Context, tx *sql.Tx, did string) error {
	if tx == nil {
		return errors.NewValidationError("tx", "must not be nil")
	}
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1, $2)`,
		AccountLockNamespace, AccountLockKey(did)); err != nil {
		return fmt.Errorf("lock account %s: %w", did, err)
	}
	return nil
}

// AddPauseSourceTx adds src to did's pause source set (updating an
// account-status source's status) and pauses the persona, in one statement
// that takes the persona's ap_actors row lock. A missing persona is an error
// satisfying errors.IsNotFound.
func AddPauseSourceTx(ctx context.Context, tx *sql.Tx, did string, src PauseSource) error {
	if err := checkPauseSource(tx, src); err != nil {
		return err
	}
	return execOneRow(ctx, tx, "add pause source", did, `
		UPDATE ap_actors SET
			pause_sources = pause_sources || jsonb_build_object($2::text, $3::text),
			delivery_paused = true,
			updated_at = now()
		WHERE did = $1`, did, src.setKey(), src.Status)
}

// AddPauseSourceKeepingTakedownTx is AddPauseSourceTx for a status no PDS
// confirmed: when did's source under src's key already carries a confirmed
// takedown status (takendown or suspended, not an UnconfirmedTakedownStatus),
// that status is kept and src.Status is not written. A missing persona is an
// error satisfying errors.IsNotFound.
func AddPauseSourceKeepingTakedownTx(ctx context.Context, tx *sql.Tx, did string, src PauseSource) error {
	if err := checkPauseSource(tx, src); err != nil {
		return err
	}
	return execOneRow(ctx, tx, "add pause source keeping a takedown status", did, `
		UPDATE ap_actors SET
			pause_sources = pause_sources || jsonb_build_object($2::text,
				CASE WHEN pause_sources ->> $2::text IN ('`+accountStatusTakendown+`', '`+accountStatusSuspended+`')
				     THEN pause_sources ->> $2::text
				     ELSE $3::text END),
			delivery_paused = true,
			updated_at = now()
		WHERE did = $1`, did, src.setKey(), src.Status)
}

// RemovePauseSourceTx removes src (by kind and key) from did's pause source
// set and unpauses the persona when no source is left, in one statement that
// takes the persona's ap_actors row lock. A missing persona is an error
// satisfying errors.IsNotFound.
func RemovePauseSourceTx(ctx context.Context, tx *sql.Tx, did string, src PauseSource) error {
	if err := checkPauseSource(tx, src); err != nil {
		return err
	}
	return execOneRow(ctx, tx, "remove pause source", did, `
		UPDATE ap_actors SET
			pause_sources = pause_sources - $2::text,
			delivery_paused = (pause_sources - $2::text) <> '{}'::jsonb,
			updated_at = now()
		WHERE did = $1`, did, src.setKey())
}

// pauseUnderStandingAccountLabelTx adds the account-label pause source for a
// standing account `!takedown` on did (the bare DID's latest label, not
// negated and not lapsed). Persona minting calls it in the mint tx under the
// account lock, so a label committed before the mint pauses the new persona
// and one committed after finds the persona.
func pauseUnderStandingAccountLabelTx(ctx context.Context, tx *sql.Tx, did string) error {
	if _, err := tx.ExecContext(ctx, `
		UPDATE ap_actors SET
			pause_sources = pause_sources || jsonb_build_object($2 || current.src, ''),
			delivery_paused = true,
			updated_at = now()
		FROM (
			SELECT src, neg, exp FROM coves_labels
			WHERE val = '`+takedownLabelValue+`' AND uri = $1
			ORDER BY reset_epoch DESC, seq DESC LIMIT 1
		) current
		WHERE ap_actors.did = $1 AND `+StandingLabelCondition("current"),
		did, PauseSourceAccountLabel+":"); err != nil {
		return fmt.Errorf("pause %s under its standing account label: %w", did, err)
	}
	return nil
}

// Account statuses whose pause is a takedown pause: the persona's work waits
// for the pause to clear instead of being cancelled. A deactivated or
// throttled pause is the user's own (or a rate) pause and cancels outward work
// as before.
const (
	accountStatusTakendown = "takendown"
	accountStatusSuspended = "suspended"
)

// IsTakedownAccountStatus reports whether an #account status is a takedown
// status: takendown or suspended.
func IsTakedownAccountStatus(status string) bool {
	return status == accountStatusTakendown || status == accountStatusSuspended
}

// unconfirmedStatusPrefix marks an account-status source's takedown status as
// the relay's, not yet confirmed against the PDS.
const unconfirmedStatusPrefix = "unconfirmed:"

// UnconfirmedTakedownStatus is the stored form of a takedown status the relay
// reported and no PDS has confirmed yet. It holds the persona's work like a
// confirmed takedown status, but a later unconfirmed write replaces it where a
// confirmed one is kept (AddPauseSourceKeepingTakedownTx).
func UnconfirmedTakedownStatus(status string) string {
	return unconfirmedStatusPrefix + status
}

// TakedownPausedTx reports whether did's persona is held back only for a
// takedown: it is enabled, has no federation opt-out, and at least one of its
// pause sources is a takedown source (an account label, or an account-status
// source whose status is takendown or suspended). The caller has already
// locked the ap_actors row (LockActorConsentTx). A missing persona is false.
func TakedownPausedTx(ctx context.Context, tx *sql.Tx, did string) (bool, error) {
	if tx == nil {
		return false, errors.NewValidationError("tx", "must not be nil")
	}
	var paused bool
	err := tx.QueryRowContext(ctx, `
		SELECT a.enabled
		       AND COALESCE((SELECT p.enabled FROM federation_prefs p WHERE p.did = a.did), true)
		       AND `+takedownPauseSourceCondition("a.pause_sources")+`
		FROM ap_actors a
		WHERE a.did = $1`, did).Scan(&paused)
	if stderrors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read takedown pause of %s: %w", did, err)
	}
	return paused, nil
}

// takedownPauseSourceCondition is the SQL condition that the pause source
// set in column holds a takedown source: an account label, or an
// account-status source whose status is takendown or suspended, confirmed or
// not (UnconfirmedTakedownStatus). Every value
// it names is a constant of this package.
func takedownPauseSourceCondition(column string) string {
	return `EXISTS (SELECT 1 FROM jsonb_each_text(` + column + `) s
		WHERE starts_with(s.key, '` + PauseSourceAccountLabel + `:')
		   OR (starts_with(s.key, '` + PauseSourceAccountStatus + `:')
		       AND s.value IN ('` + accountStatusTakendown + `', '` + accountStatusSuspended + `',
		                       '` + unconfirmedStatusPrefix + accountStatusTakendown + `',
		                       '` + unconfirmedStatusPrefix + accountStatusSuspended + `')))`
}

// StandingLabelCondition is the SQL condition that the coves_labels row
// aliased alias stands: it is not a negation and its exp, if any, is still
// ahead of the database clock. It applies to the latest row of a (uri, val).
func StandingLabelCondition(alias string) string {
	return `(NOT ` + alias + `.neg AND (` + alias + `.exp IS NULL OR ` + alias + `.exp::timestamptz > now()))`
}

func checkPauseSource(tx *sql.Tx, src PauseSource) error {
	if tx == nil {
		return errors.NewValidationError("tx", "must not be nil")
	}
	switch src.Kind {
	case PauseSourceAccountLabel, PauseSourceAccountStatus:
	default:
		return errors.NewValidationError("source_kind", "unknown pause source kind "+src.Kind)
	}
	if src.Key == "" {
		return errors.NewValidationError("source_key", "must not be empty")
	}
	return nil
}
