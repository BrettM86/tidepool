package votes

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/echo"
	"tidepool/internal/errors"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// The votes tests run against a real postgres database (Coves convention:
// real infrastructure, no mocks). They skip cleanly when
// TIDEPOOL_TEST_DATABASE_URL is unset; `make test` starts the postgres-test
// container and sets it.

// Shared fixtures.
const (
	testDID        = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	testCID        = "bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqrsqxi3jgxte"
	testCollection = "social.coves.community.post"
	testInstance   = "lemmy.world"

	subjectPost    = "https://lemmy.world/post/100"
	subjectComment = "https://lemmy.world/comment/200"

	voterAlice = "https://lemmy.world/u/alice"
	voterBob   = "https://lemmy.zip/u/bob"
	voterCarol = "https://sh.itjust.works/u/carol"
)

// voteTablesToTruncate is the ONE list every helper in this package starts
// from. There were four divergent lists before, and the omission that mattered
// was invisible: 17b made outbound_votes an INPUT to the seed, so a leftover
// delivered row silently subtracted one from every later test that seeded that
// subject — green on the first run, red on the second. A helper that starts
// from this list gains the next shared-state input automatically instead of
// waiting for someone to notice a discrepancy four places at once.
//
// Truncating more than a test needs is free; truncating less is a bug that
// surfaces somewhere else, later, as an off-by-one.
var voteTablesToTruncate = []string{
	// The aggregate spine.
	"vote_events", "vote_aggregates", "ap_objects", "communities",
	// The outbound side: an input to the seed since 17b.
	"outbound_votes", "outbound_deliveries", "outbound_activities", "outbound_objects",
	// Identity: ap_actors is what the voter probe reads, bridged_actors is what
	// it must NOT read.
	"ap_actors", "bridged_actors",
	// Consumer state, for the tests that drive the real write path.
	"federation_prefs", "jetstream_record_revs", "jetstream_dead_letters",
	"consumer_cursors", "repo_state",
}

// truncateVoteTables clears everything in voteTablesToTruncate.
func truncateVoteTables(t *testing.T, database *sql.DB) {
	t.Helper()
	testutil.Truncate(t, database, voteTablesToTruncate...)
}

// testDB returns a migrated connection with every table this package's
// fixtures touch truncated.
func testDB(t *testing.T) *sql.DB {
	t.Helper()
	database := testutil.DB(t)
	truncateVoteTables(t, database)
	return database
}

// testAggregator builds an Aggregator over the test database.
func testAggregator(t *testing.T, database *sql.DB) (*Aggregator, store.APObjects) {
	t.Helper()
	agg, objects, _ := testAggregatorWithRecords(t, database)
	return agg, objects
}

// testAggregatorWithRecords additionally exposes the fake record reader the
// aggregator resolves comment thread roots through (the real repo layer is
// exercised in internal/repo and internal/materialize; here the record read
// is one narrow seam).
func testAggregatorWithRecords(t *testing.T, database *sql.DB) (*Aggregator, store.APObjects, *fakeRecords) {
	t.Helper()
	objects := store.NewAPObjects(database)
	records := &fakeRecords{records: map[string]map[string]any{}}
	// The REAL voter probe over the same database: every existing case votes as
	// a fediverse user, so it answers ClassNone for all of them — and would stop
	// answering that the moment the probe started matching the wrong table.
	probe, err := echo.New(echo.Options{
		Objects:         objects,
		OutboundObjects: store.NewOutboundObjects(database),
		Activities:      store.NewOutboundActivities(database),
		Actors:          store.NewAPActors(database),
	})
	require.NoError(t, err)
	agg, err := NewAggregator(database, objects, store.NewCommunities(database), records, probe, slog.Default())
	require.NoError(t, err)
	return agg, objects, records
}

// fakeRecords is a map-backed RecordReader.
type fakeRecords struct{ records map[string]map[string]any }

func (f *fakeRecords) put(did, collection, rkey string, record map[string]any) {
	f.records[did+"/"+collection+"/"+rkey] = record
}

func (f *fakeRecords) GetRecord(_ context.Context, did, collection, rkey string) (map[string]any, string, error) {
	record, ok := f.records[did+"/"+collection+"/"+rkey]
	if !ok {
		return nil, "", errors.NewNotFoundError("record", did+"/"+collection+"/"+rkey)
	}
	return record, testCID, nil
}

// followCommunity registers an announcing community (group IRI → repo DID)
// for announced-vote binding tests.
func followCommunity(t *testing.T, database *sql.DB, groupIRI, did string) {
	t.Helper()
	communities := store.NewCommunities(database)
	_, err := communities.UpsertCommunity(context.Background(), store.Community{
		APGroupID:         groupIRI,
		DID:               did,
		PreferredUsername: "votes-test",
		Instance:          testInstance,
	})
	require.NoError(t, err)
}

// bridgeSubject materializes a fake mapping for an AP id (the subject must
// exist in ap_objects for votes to count) and returns its at-uri.
func bridgeSubject(t *testing.T, objects store.APObjects, apID, rkey string) string {
	t.Helper()
	return bridgeSubjectAs(t, objects, apID, rkey, testDID, testCollection)
}

// bridgeSubjectAs is bridgeSubject with an explicit repo DID and collection
// (announced-vote binding tests place subjects in specific community repos).
func bridgeSubjectAs(t *testing.T, objects store.APObjects, apID, rkey, did, collection string) string {
	t.Helper()
	publishedAt := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	mapping, err := objects.PutMapping(context.Background(), store.APObjectMapping{
		APID:           apID,
		APType:         "Page",
		OriginInstance: testInstance,
		DID:            did,
		Collection:     collection,
		RKey:           rkey,
		CID:            testCID,
		PublishedAt:    &publishedAt,
	})
	require.NoError(t, err)
	return mapping.ATURI
}

// like/dislike build vote activities the way the dispatcher hands them over
// (Announce-unwrapped: Type, ID, Actor, Object).
func like(activityID, voter, subject string) *ap.Object {
	return vote(ap.TypeLike, activityID, voter, subject)
}

func dislike(activityID, voter, subject string) *ap.Object {
	return vote(ap.TypeDislike, activityID, voter, subject)
}

func vote(voteType, activityID, voter, subject string) *ap.Object {
	return &ap.Object{
		ID:     activityID,
		Type:   voteType,
		Actor:  &ap.Object{ID: voter},
		Object: &ap.Object{ID: subject},
	}
}

// activityID mints per-test activity ids.
func activityID(t *testing.T, n int) string {
	return fmt.Sprintf("https://lemmy.world/activities/like/%s-%d", t.Name(), n)
}

// counts reads the served aggregate for a subject straight from the table.
// found=false means no aggregate row exists at all.
func counts(t *testing.T, database *sql.DB, subject string) (up, down int, found bool) {
	t.Helper()
	err := database.QueryRow(`
		SELECT upvotes, downvotes FROM vote_aggregates WHERE subject_ap_id = $1`,
		subject).Scan(&up, &down)
	if stderrors.Is(err, sql.ErrNoRows) {
		return 0, 0, false
	}
	require.NoError(t, err)
	return up, down, true
}
