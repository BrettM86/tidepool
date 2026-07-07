package store

import (
	"database/sql"
	"testing"
	"time"

	"tidepool/internal/testutil"
)

// The store tests run against a real postgres database (Coves convention:
// real infrastructure, no mocks). They skip cleanly when
// TIDEPOOL_TEST_DATABASE_URL is unset; `make test` starts the postgres-test
// container and sets it. testutil.DB holds the cross-package advisory lock
// that serializes the packages sharing this database.

// testDB returns a migrated connection to the test database, truncating all
// tables this package touches so each test starts clean.
func testDB(t *testing.T) *sql.DB {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database,
		"ap_objects", "bridged_actors", "communities", "inbox_events", "service_keys")
	return database
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
