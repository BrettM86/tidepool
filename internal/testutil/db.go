// Package testutil provides the shared postgres harness for Tidepool's
// integration tests. Multiple packages (store, repo, identity, ...) test
// against the same database, and `go test ./...` runs package binaries in
// parallel — so the harness takes a postgres advisory lock for the lifetime
// of each test process, serializing the packages that share the schema.
// (Within one package, tests already run sequentially.)
package testutil

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"tidepool/internal/db"
)

// advisoryLockKey is arbitrary but must be shared by every package using
// the test database. It MUST stay distinct from internal/repo's
// commitAdvisoryLockKey (0x7469646570636d): this lock is session-scoped and
// held for an entire test binary, while the repo one is transaction-scoped
// and taken by every commit — sharing a key would deadlock every repo
// commit made from tests.
const advisoryLockKey int64 = 0x7469646570 // "tidep"

var (
	once sync.Once
	conn *sql.DB
	// lockConn pins one session that holds the advisory lock until this
	// test process exits (postgres releases advisory locks on session end).
	lockConn *sql.Conn
	setupErr error
)

// DB returns a migrated connection to the test database, guarded by the
// cross-package advisory lock. It skips the test when
// TIDEPOOL_TEST_DATABASE_URL is unset — except in CI, where a missing
// database fails loudly (skipping every postgres-backed test would let the
// suite go green while testing nothing). `make test` starts the
// postgres-test container and sets the variable.
func DB(t testing.TB) *sql.DB {
	t.Helper()

	databaseURL := os.Getenv("TIDEPOOL_TEST_DATABASE_URL")
	if databaseURL == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("CI is set but TIDEPOOL_TEST_DATABASE_URL is not; " +
				"the postgres-backed tests must run in CI")
		}
		t.Skip("TIDEPOOL_TEST_DATABASE_URL not set; skipping postgres-backed tests " +
			"(run `make test` to start the postgres-test container and set it)")
	}

	once.Do(func() {
		// Generous timeout: acquiring the advisory lock may wait for
		// another package's whole test binary to finish.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()

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

// Truncate empties the given tables and resets their sequences, so a test
// starts from a clean slate.
func Truncate(t testing.TB, conn *sql.DB, tables ...string) {
	t.Helper()
	if len(tables) == 0 {
		return
	}
	_, err := conn.ExecContext(context.Background(),
		fmt.Sprintf(`TRUNCATE %s RESTART IDENTITY`, strings.Join(tables, ", ")))
	if err != nil {
		t.Fatalf("truncate test tables: %v", err)
	}
}
