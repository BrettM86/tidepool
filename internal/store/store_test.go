package store

import (
	"context"
	"database/sql"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"tidepool/internal/db"
)

// The store tests run against a real postgres database (Coves convention:
// real infrastructure, no mocks). They skip cleanly when
// TIDEPOOL_TEST_DATABASE_URL is unset; `make test` starts the postgres-test
// container and sets it.

var (
	testDatabaseOnce sync.Once
	testDatabase     *sql.DB
	testDatabaseErr  error
)

// testDB returns a migrated connection to the test database, truncating all
// Tidepool tables so each test starts clean.
func testDB(t *testing.T) *sql.DB {
	t.Helper()

	databaseURL := os.Getenv("TIDEPOOL_TEST_DATABASE_URL")
	if databaseURL == "" {
		// In CI a missing database must fail loudly: skipping every
		// postgres-backed test would let the suite go green while testing
		// nothing.
		if os.Getenv("CI") != "" {
			t.Fatal("CI is set but TIDEPOOL_TEST_DATABASE_URL is not; " +
				"the postgres-backed store tests must run in CI")
		}
		t.Skip("TIDEPOOL_TEST_DATABASE_URL not set; skipping postgres-backed store tests " +
			"(run `make test` to start the postgres-test container and set it)")
	}

	testDatabaseOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		testDatabase, testDatabaseErr = db.Open(ctx, databaseURL)
		if testDatabaseErr != nil {
			return
		}
		testDatabaseErr = db.MigrateUp(ctx, testDatabase)
	})
	require.NoError(t, testDatabaseErr, "connect and migrate test database")

	_, err := testDatabase.ExecContext(context.Background(),
		`TRUNCATE ap_objects, bridged_actors, communities, inbox_events, service_keys RESTART IDENTITY`)
	require.NoError(t, err, "truncate test tables")

	return testDatabase
}

// Shared fixtures for readable tests.
const (
	testDID           = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	testSecondDID     = "did:plc:44ybard66vv44zksje25o7dz"
	testCollection    = "social.coves.community.post"
	testRKey          = "3jzfcijpj2z2a"
	testCID           = "bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqrsqxi3jgxte"
	testUpdatedCID    = "bafyreievgu2ty7qbiaaom5zhmkznsnajuzideek3lo7e65dwqlrvrxnmo4"
	testAPObjectID    = "https://lemmy.world/post/12345"
	testAPActorID     = "https://lemmy.world/u/alice"
	testAPGroupID     = "https://lemmy.world/c/technology"
	testInstance      = "lemmy.world"
	testExpectedATURI = "at://" + testDID + "/" + testCollection + "/" + testRKey
)

func testMapping() APObjectMapping {
	publishedAt := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	return APObjectMapping{
		APID:           testAPObjectID,
		APType:         "Page",
		OriginInstance: testInstance,
		DID:            testDID,
		Collection:     testCollection,
		RKey:           testRKey,
		CID:            testCID,
		PublishedAt:    &publishedAt,
	}
}

func testActor() BridgedActor {
	return BridgedActor{
		APActorID:           testAPActorID,
		ActorType:           ActorTypePerson,
		DID:                 testDID,
		Handle:              "alice.lemmy-world.tidepool.example",
		SigningKeyEncrypted: []byte("test-signing-key-bytes"),
		// Consent is always stated explicitly: the zero value is rejected
		// by validation, never coerced to "consented".
		ConsentState: ConsentStateOK,
	}
}

func testCommunity() Community {
	return Community{
		APGroupID:         testAPGroupID,
		DID:               testDID,
		PreferredUsername: "technology",
		Instance:          testInstance,
	}
}
