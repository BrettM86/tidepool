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

const federationPrefColumns = `did, enabled, delete_remote, source, updated_at`

func (r *postgresFederationPrefs) Upsert(ctx context.Context, pref FederationPref) (*FederationPref, error) {
	if !pref.Source.Valid() {
		// Including the zero value: an unstated source hides whether the
		// preference came off a record we saw or a probe we made, and those
		// two have different staleness.
		return nil, errors.NewValidationError("source", "unknown source "+string(pref.Source))
	}

	// EVERY field is overwritten, delete_remote included. Re-enabling a user
	// must clear a previously requested deleteRemote: a stale destructive flag
	// sitting on an enabled row is a loaded gun pointed at task 17.
	query := `
		INSERT INTO federation_prefs (did, enabled, delete_remote, source, updated_at)
		VALUES ($1, $2, $3, $4, now())
		ON CONFLICT (did) DO UPDATE SET
			enabled = EXCLUDED.enabled,
			delete_remote = EXCLUDED.delete_remote,
			source = EXCLUDED.source,
			updated_at = now()
		RETURNING ` + federationPrefColumns

	row := r.db.QueryRowContext(ctx, query,
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
	if _, err := r.db.ExecContext(ctx,
		`DELETE FROM federation_prefs WHERE did = $1`, did); err != nil {
		return fmt.Errorf("delete federation_pref %q: %w", did, err)
	}
	return nil
}

func scanFederationPref(row rowScanner) (*FederationPref, error) {
	var pref FederationPref
	var source string
	if err := row.Scan(&pref.DID, &pref.Enabled, &pref.DeleteRemote, &source, &pref.UpdatedAt); err != nil {
		return nil, err
	}
	pref.Source = FederationPrefSource(source)
	return &pref, nil
}
