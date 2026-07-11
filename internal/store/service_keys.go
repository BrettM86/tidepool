package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"

	"tidepool/internal/errors"
)

type postgresServiceKeys struct {
	db *sql.DB
}

// NewServiceKeys creates the postgres-backed service_keys repository.
func NewServiceKeys(db *sql.DB) ServiceKeys {
	return &postgresServiceKeys{db: db}
}

const serviceKeyColumns = ` id, name, key_material, created_at`

func (r *postgresServiceKeys) Create(ctx context.Context, name string, keyMaterial []byte) (*ServiceKey, error) {
	if name == "" {
		return nil, errors.NewValidationError("name", "must not be empty")
	}
	if len(keyMaterial) == 0 {
		return nil, errors.NewValidationError("key_material", "must not be empty")
	}

	query := `
		INSERT INTO service_keys (name, key_material)
		VALUES ($1, $2)
		RETURNING` + serviceKeyColumns

	key, err := scanServiceKey(r.db.QueryRowContext(ctx, query, name, keyMaterial))
	if err != nil {
		// Keys are create-once by design: a concurrent bootstrap losing the
		// insert race must re-Get the winner's key, never overwrite it.
		if constraint, ok := uniqueViolation(err); ok && constraint == "service_keys_name_key" {
			return nil, errors.NewConflictError("service_key", "name", name)
		}
		return nil, fmt.Errorf("create service_key %q: %w", name, err)
	}
	return key, nil
}

func (r *postgresServiceKeys) Get(ctx context.Context, name string) (*ServiceKey, error) {
	query := `SELECT` + serviceKeyColumns + ` FROM service_keys WHERE name = $1`
	key, err := scanServiceKey(r.db.QueryRowContext(ctx, query, name))
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFoundError("service_key", name)
		}
		return nil, fmt.Errorf("get service_key %q: %w", name, err)
	}
	return key, nil
}

func scanServiceKey(row rowScanner) (*ServiceKey, error) {
	var key ServiceKey
	if err := row.Scan(&key.ID, &key.Name, &key.KeyMaterial, &key.CreatedAt); err != nil {
		return nil, err
	}
	return &key, nil
}
