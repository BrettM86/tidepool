package db

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// newMigrationProvider builds a goose provider over the embedded migration
// files. Providers hold no global state, so tests can create their own.
func newMigrationProvider(conn *sql.DB) (*goose.Provider, error) {
	migrations, err := fs.Sub(migrationsFS, "migrations")
	if err != nil {
		return nil, fmt.Errorf("db: sub filesystem: %w", err)
	}
	provider, err := goose.NewProvider(database.DialectPostgres, conn, migrations)
	if err != nil {
		return nil, fmt.Errorf("db: migration provider: %w", err)
	}
	return provider, nil
}

// MigrateUp applies all pending embedded migrations.
func MigrateUp(ctx context.Context, conn *sql.DB) error {
	provider, err := newMigrationProvider(conn)
	if err != nil {
		return err
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("db: migrate up: %w", err)
	}
	return nil
}

// MigrateDownTo rolls migrations back down to (and including) the given
// version; version 0 tears everything down. Used by tests to verify the
// down migrations actually work.
func MigrateDownTo(ctx context.Context, conn *sql.DB, version int64) error {
	provider, err := newMigrationProvider(conn)
	if err != nil {
		return err
	}
	if _, err := provider.DownTo(ctx, version); err != nil {
		return fmt.Errorf("db: migrate down to %d: %w", version, err)
	}
	return nil
}
