// Package testutil provides the shared postgres harness for Tidepool's
// integration tests. `go test ./...` runs package binaries in parallel, so
// each package gets its own database, created next to the one
// TIDEPOOL_TEST_DATABASE_URL names and migrated on first use, and packages
// run side by side. Each test process also holds an advisory lock in its
// database, so two runs of the same package in the same checkout take turns.
// (Within one package, tests already run sequentially.)
package testutil

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lib/pq"

	"tidepool/internal/db"
)

// advisoryLockKey is arbitrary. It MUST stay distinct from internal/repo's
// commitAdvisoryLockKey (0x7469646570636d): this lock is session-scoped and
// held for an entire test binary, while the repo one is transaction-scoped
// and taken by every commit — sharing a key would deadlock every repo
// commit made from tests.
const advisoryLockKey int64 = 0x7469646570 // "tidep"

var (
	once sync.Once
	conn *sql.DB
	// databaseURL is the package database's URL, set by DB.
	databaseURL string
	// lockConn pins one session that holds the advisory lock until this
	// test process exits (postgres releases advisory locks on session end).
	lockConn *sql.Conn
	setupErr error
)

// DB returns a migrated connection to this package's test database, guarded
// by the advisory lock. It skips the test when
// TIDEPOOL_TEST_DATABASE_URL is unset — except in CI, where a missing
// database fails loudly (skipping every postgres-backed test would let the
// suite go green while testing nothing). `make test` starts the
// postgres-test container and sets the variable.
func DB(t testing.TB) *sql.DB {
	t.Helper()

	configuredURL := os.Getenv("TIDEPOOL_TEST_DATABASE_URL")
	if configuredURL == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("CI is set but TIDEPOOL_TEST_DATABASE_URL is not; " +
				"the postgres-backed tests must run in CI")
		}
		t.Skip("TIDEPOOL_TEST_DATABASE_URL not set; skipping postgres-backed tests " +
			"(run `make test` to start the postgres-test container and set it)")
	}

	once.Do(func() {
		// Generous timeout: acquiring the advisory lock may wait for another
		// run of this package's whole test binary to finish.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		databaseURL, setupErr = packageDatabase(ctx, configuredURL)
		if setupErr != nil {
			return
		}
		conn, setupErr = db.Open(ctx, databaseURL)
		if setupErr != nil {
			return
		}
		lockConn, setupErr = conn.Conn(ctx)
		if setupErr != nil {
			return
		}
		if _, setupErr = lockConn.ExecContext(ctx,
			`SELECT pg_advisory_lock($1)`, advisoryLockKey); setupErr != nil {
			return
		}
		setupErr = db.MigrateUp(ctx, conn)
	})
	if setupErr != nil {
		t.Fatalf("connect and migrate test database: %v", setupErr)
	}
	return conn
}

// DatabaseURL returns the URL of the database DB connects to, for a test that
// opens its own connection or hands the URL to the code under test.
func DatabaseURL(t testing.TB) string {
	t.Helper()
	DB(t)
	return databaseURL
}

// packageDatabase creates, when missing, the database for the package under
// test and returns its URL. Its name comes from packageDatabaseName with the
// working directory, which `go test` sets to the package's source directory.
// Creating one needs the CREATEDB privilege; the lookup first spares reruns
// from needing it.
func packageDatabase(ctx context.Context, databaseURL string) (string, error) {
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		return "", fmt.Errorf("parse test database URL: %w", err)
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		return "", fmt.Errorf("test database URL must be a postgres:// URL, got scheme %q", parsed.Scheme)
	}
	base := strings.TrimPrefix(parsed.Path, "/")
	if base == "" {
		return "", errors.New("test database URL names no database")
	}
	directory, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("working directory: %w", err)
	}
	name := packageDatabaseName(base, directory)

	admin, err := db.Open(ctx, databaseURL)
	if err != nil {
		return "", fmt.Errorf("open configured test database %s: %w", base, err)
	}
	defer func() { _ = admin.Close() }()
	var exists bool
	if err := admin.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)`, name).Scan(&exists); err != nil {
		return "", fmt.Errorf("look up database %s: %w", name, err)
	}
	if !exists {
		_, err := admin.ExecContext(ctx, `CREATE DATABASE `+pq.QuoteIdentifier(name))
		// A concurrent run of the same package created it first: 42P04
		// duplicate_database, or 23505 unique_violation when both creates
		// reach pg_database's name index.
		var pqErr *pq.Error
		switch {
		case err == nil:
		case errors.As(err, &pqErr) && (pqErr.Code == "42P04" || pqErr.Code == "23505"):
		case errors.As(err, &pqErr) && pqErr.Code == "42501":
			return "", fmt.Errorf("create database %s: the test role needs CREATEDB, or create it beforehand: %w", name, err)
		default:
			return "", fmt.Errorf("create database %s: %w", name, err)
		}
	}
	parsed.Path = "/" + name
	return parsed.String(), nil
}

// packageDatabaseName is the configured database's name, cut to fit, plus
// "_p" and the FNV-1a hash of the package directory, so two packages, or one
// package in two checkouts, get different databases. Each checkout keeps its
// set until the test Postgres is recreated or the set is dropped.
func packageDatabaseName(base, directory string) string {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(directory))
	// Postgres truncates names past 63 bytes; 53 + "_p" + 8 hex digits fits.
	return fmt.Sprintf("%s_p%08x", base[:min(len(base), 53)], hash.Sum32())
}

// Truncate empties the given tables and resets their sequences, so a test
// starts from a clean slate.
func Truncate(t testing.TB, conn *sql.DB, tables ...string) {
	t.Helper()
	if len(tables) == 0 {
		return
	}
	// CASCADE so truncating a table that is the target of a foreign key also
	// clears its referencing tables (e.g. outbound_activities ← outbound_
	// deliveries): setup-time truncation wants a clean slate, and a caller that
	// lists the parent but not the child would otherwise fail with a 0A000 FK
	// error. Truncation runs before any seeding, so cascading never wipes data a
	// test meant to keep.
	_, err := conn.ExecContext(context.Background(),
		fmt.Sprintf(`TRUNCATE %s RESTART IDENTITY CASCADE`, strings.Join(tables, ", ")))
	if err != nil {
		t.Fatalf("truncate test tables: %v", err)
	}
}

// TruncateIfExists empties each listed table that exists, so a harness can
// clear a table a pending migration is about to add without failing before it
// lands.
func TruncateIfExists(t testing.TB, conn *sql.DB, tables ...string) {
	t.Helper()
	for _, table := range tables {
		var exists bool
		if err := conn.QueryRowContext(context.Background(),
			`SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists); err != nil {
			t.Fatalf("look up test table %s: %v", table, err)
		}
		if exists {
			Truncate(t, conn, table)
		}
	}
}
