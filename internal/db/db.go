// Package db opens the Tidepool postgres database and manages goose
// migrations (embedded so the binary and the tests can migrate without a
// goose CLI install).
package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq" // postgres driver
)

const (
	maxOpenConnections = 25
	maxIdleConnections = 5
	connectionLifetime = 5 * time.Minute
	pingTimeout        = 5 * time.Second
)

// Open connects to postgres, applies pool settings, and verifies the
// connection with a ping bounded by ctx.
func Open(ctx context.Context, databaseURL string) (*sql.DB, error) {
	database, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("db: open: %w", err)
	}

	database.SetMaxOpenConns(maxOpenConnections)
	database.SetMaxIdleConns(maxIdleConnections)
	database.SetConnMaxLifetime(connectionLifetime)

	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	if err := database.PingContext(pingCtx); err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("db: ping: %w", err)
	}

	return database, nil
}
