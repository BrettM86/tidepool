package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"

	"tidepool/internal/errors"
)

type postgresFederationPrefs struct {
	db *sql.DB
}

// NewFederationPrefs creates the postgres-backed federation_prefs repository.
func NewFederationPrefs(db *sql.DB) FederationPrefs {
	return &postgresFederationPrefs{db: db}
}

const federationPrefColumns = `did, enabled, delete_remote, source, purged_at, updated_at`

func (r *postgresFederationPrefs) Upsert(ctx context.Context, pref FederationPref) (*FederationPref, error) {
	return r.upsert(ctx, r.db, pref)
}

func (r *postgresFederationPrefs) UpsertTx(ctx context.Context, tx *sql.Tx, pref FederationPref) (*FederationPref, error) {
	if tx == nil {
		return nil, errors.NewValidationError("tx", "must not be nil")
	}
	return r.upsert(ctx, tx, pref)
}

func (r *postgresFederationPrefs) upsert(ctx context.Context, ex execer, pref FederationPref) (*FederationPref, error) {
	if !pref.Source.Valid() {
		// Including the zero value: an unstated source hides whether the
		// preference came off a record we saw or a probe we made, and those
		// two have different staleness.
		return nil, errors.NewValidationError("source", "unknown source "+string(pref.Source))
	}

	// EVERY field is overwritten, delete_remote included. Re-enabling a user
	// must clear a previously requested deleteRemote: a stale destructive flag
	// sitting on an enabled row is a loaded gun pointed at task 17.
	//
	// EXCEPT purged_at, which is not in this statement at all — not in the
	// INSERT, not in the DO UPDATE. It records that peers were actually asked to
	// delete this user's content, which no preference write can make untrue, and
	// leaving it out is what makes that impossible to undo by accident: a
	// re-delivered account event upserting the same row cannot blank it, and no
	// caller can set it by constructing a model. MarkPurged is the only writer.
	query := `
		INSERT INTO federation_prefs (did, enabled, delete_remote, source, updated_at)
		VALUES ($1, $2, $3, $4, now())
		ON CONFLICT (did) DO UPDATE SET
			enabled = EXCLUDED.enabled,
			delete_remote = EXCLUDED.delete_remote,
			source = EXCLUDED.source,
			updated_at = now()
		RETURNING ` + federationPrefColumns

	row := ex.QueryRowContext(ctx, query,
		pref.DID, pref.Enabled, pref.DeleteRemote, string(pref.Source))
	stored, err := scanFederationPref(row)
	if err != nil {
		return nil, fmt.Errorf("upsert federation_pref %q: %w", pref.DID, err)
	}
	return stored, nil
}

func (r *postgresFederationPrefs) Get(ctx context.Context, did string) (*FederationPref, error) {
	query := `SELECT ` + federationPrefColumns + ` FROM federation_prefs WHERE did = $1`
	pref, err := scanFederationPref(r.db.QueryRowContext(ctx, query, did))
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			// NotFound MEANS default-on. It is reported as an absence rather
			// than synthesized into an enabled row so callers can still tell a
			// re-enable from a first sighting.
			return nil, errors.NewNotFoundError("federation_pref", did)
		}
		return nil, fmt.Errorf("get federation_pref %q: %w", did, err)
	}
	return pref, nil
}

func (r *postgresFederationPrefs) Delete(ctx context.Context, did string) error {
	// Deleting a preference that never existed is success: an opt-out record
	// delete for a user who never opted out is the COMMON case, and absence is
	// exactly the state the delete is asking for.
	//
	// A COMMITTED PURGE IS NOT DELETABLE. Absence means default-on, so removing
	// this row is how a user comes back — and a user whose content peers were
	// already told to delete has nothing to come back to. The guard is in the
	// statement because this delete is reachable from an ordinary record delete,
	// which is the most innocuous-looking way to undo an irreversible decision.
	// The caller is told nothing changed by reading the row back, which
	// restoreDefaultFederation does before it says anything to an operator.
	_, err := deleteFederationPref(ctx, r.db, did)
	return err
}

func (r *postgresFederationPrefs) DeleteTx(ctx context.Context, tx *sql.Tx, did string) (bool, error) {
	if tx == nil {
		return false, errors.NewValidationError("tx", "must not be nil")
	}
	return deleteFederationPref(ctx, tx, did)
}

func deleteFederationPref(ctx context.Context, ex execer, did string) (bool, error) {
	result, err := ex.ExecContext(ctx,
		`DELETE FROM federation_prefs WHERE did = $1 AND purged_at IS NULL`, did)
	if err != nil {
		return false, fmt.Errorf("delete federation_pref %q: %w", did, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("delete federation_pref %q: rows affected: %w", did, err)
	}
	return affected > 0, nil
}

func (r *postgresFederationPrefs) MarkPurgedTx(ctx context.Context, tx *sql.Tx, did string) error {
	if tx == nil {
		return errors.NewValidationError("tx", "must not be nil")
	}
	return markPurged(ctx, tx, did)
}

func (r *postgresFederationPrefs) MarkPurged(ctx context.Context, did string) error {
	return markPurged(ctx, r.db, did)
}

func markPurged(ctx context.Context, ex execer, did string) error {
	// COALESCE: the FIRST commit is the one that happened. A retry that
	// re-enqueues an idempotent withdrawal must not move the date, which is the
	// only record of when this user's content was actually withdrawn.
	result, err := ex.ExecContext(ctx, `
		UPDATE federation_prefs
		   SET purged_at = COALESCE(purged_at, now()), updated_at = now()
		 WHERE did = $1`, did)
	if err != nil {
		return fmt.Errorf("mark federation_pref %q purged: %w", did, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("mark federation_pref %q purged: rows affected: %w", did, err)
	}
	if affected == 0 {
		return errors.NewNotFoundError("federation_pref", did)
	}
	return nil
}

func (r *postgresFederationPrefs) ClearRequestedPurge(ctx context.Context, did string) (bool, error) {
	// The predicate is the whole method, and both terms are load-bearing.
	//
	// source = 'account' — only a preference THIS tier wrote on the strength of
	// a deletion event may be withdrawn by this tier. A user's own opt-out
	// record says the same thing (enabled=false) and means something completely
	// different: they asked. Clearing that would re-enable federation for
	// somebody who never came back to ask for it.
	//
	// purged_at IS NULL — only a REQUEST may be withdrawn. Once peers have been
	// asked to delete, the account being live again does not undo it, and
	// resuming federation would publish under an identity those peers were told
	// was gone.
	result, err := r.db.ExecContext(ctx, `
		DELETE FROM federation_prefs
		 WHERE did = $1 AND source = $2 AND purged_at IS NULL`,
		did, string(FederationPrefSourceAccount))
	if err != nil {
		return false, fmt.Errorf("clear requested purge for %q: %w", did, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("clear requested purge for %q: rows affected: %w", did, err)
	}
	return affected > 0, nil
}

func scanFederationPref(row rowScanner) (*FederationPref, error) {
	var pref FederationPref
	var source string
	var purgedAt sql.NullTime
	if err := row.Scan(&pref.DID, &pref.Enabled, &pref.DeleteRemote, &source, &purgedAt, &pref.UpdatedAt); err != nil {
		return nil, err
	}
	if purgedAt.Valid {
		pref.PurgedAt = &purgedAt.Time
	}
	pref.Source = FederationPrefSource(source)
	return &pref, nil
}
