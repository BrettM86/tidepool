package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"time"

	"tidepool/internal/errors"
)

// Instance is one remote instance's nodeinfo discovery state (migration
// 042). SoftwareName is "" when no discovery has succeeded yet.
type Instance struct {
	Host         string
	SoftwareName string
	FetchedAt    time.Time
}

// Instances stores per-host nodeinfo discovery results.
type Instances interface {
	// Get returns host's row, or a NotFound error when there is none.
	Get(ctx context.Context, host string) (*Instance, error)
	// RecordSoftware stores a successful discovery: name and fetchedAt.
	RecordSoftware(ctx context.Context, host, name string, fetchedAt time.Time) error
	// RecordFetchFailure stores a failed discovery: fetchedAt advances and
	// any stored name is kept (a new row stores no name).
	RecordFetchFailure(ctx context.Context, host string, fetchedAt time.Time) error
}

type postgresInstances struct {
	db *sql.DB
}

// NewInstances creates the postgres-backed instances repository.
func NewInstances(db *sql.DB) Instances {
	return &postgresInstances{db: db}
}

func (r *postgresInstances) Get(ctx context.Context, host string) (*Instance, error) {
	if host == "" {
		return nil, errors.NewValidationError("host", "must not be empty")
	}
	var (
		instance Instance
		name     sql.NullString
	)
	err := r.db.QueryRowContext(ctx,
		`SELECT host, software_name, fetched_at FROM instances WHERE host = $1`, host).
		Scan(&instance.Host, &name, &instance.FetchedAt)
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFoundError("instance", host)
		}
		return nil, fmt.Errorf("get instance %q: %w", host, err)
	}
	instance.SoftwareName = name.String
	return &instance, nil
}

func (r *postgresInstances) RecordSoftware(ctx context.Context, host, name string, fetchedAt time.Time) error {
	switch {
	case host == "":
		return errors.NewValidationError("host", "must not be empty")
	case name == "":
		return errors.NewValidationError("software_name", "must not be empty")
	case fetchedAt.IsZero():
		return errors.NewValidationError("fetched_at", "must be set")
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO instances (host, software_name, fetched_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (host) DO UPDATE
		SET software_name = EXCLUDED.software_name, fetched_at = EXCLUDED.fetched_at`,
		host, name, fetchedAt)
	if err != nil {
		return fmt.Errorf("record instance software for %q: %w", host, err)
	}
	return nil
}

func (r *postgresInstances) RecordFetchFailure(ctx context.Context, host string, fetchedAt time.Time) error {
	switch {
	case host == "":
		return errors.NewValidationError("host", "must not be empty")
	case fetchedAt.IsZero():
		return errors.NewValidationError("fetched_at", "must be set")
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO instances (host, software_name, fetched_at)
		VALUES ($1, NULL, $2)
		ON CONFLICT (host) DO UPDATE
		SET fetched_at = EXCLUDED.fetched_at`,
		host, fetchedAt)
	if err != nil {
		return fmt.Errorf("record instance fetch failure for %q: %w", host, err)
	}
	return nil
}
