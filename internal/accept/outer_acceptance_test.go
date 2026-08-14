package accept

import (
	"context"
	"database/sql"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"testing"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/acceptrec"
	"tidepool/internal/consume"
	"tidepool/internal/identity"
	"tidepool/internal/outbound"
	"tidepool/internal/personas"
	"tidepool/internal/repo"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// The world this outer acceptance test builds.
const (
	acUserOrigin = "https://coves.social"

	acCommunityDID   = "did:plc:44ybard66vv44zksje25o7dz"
	acCommunityAPID  = "https://lemmy.world/c/technology"
	acCommunityHost  = "lemmy.world"
	acCommunityName  = "technology"
	acCommunityInbox = "https://lemmy.world/c/technology/inbox"

	// The author. Unseen: no ap_actors, no repo_state, no anything. Their actor
	// must be minted by the act of getting a post accepted.
	acAuthorDID    = "did:plc:7iza6de2dwap2sbkpav7c6c6"
	acAuthorHandle = "author.coves.social"

	acPostRKey = "3lzpostaaaa11"
	acPostCID  = "bafyreievgu2ty7qbiaaom5zhmkznsnajuzideek3lo7e65dwqlrvrxnmo4"
	acPostRev  = "3lzpostrev001"
	acPostURI  = "at://" + acAuthorDID + "/social.coves.community.postv2/" + acPostRKey

	acPostTimeUS = int64(1_775_000_000_000_000)
)

// acKEK seals minted actors' AP RSA keys (32 bytes, AES-256).
var acKEK = []byte("0123456789abcdef0123456789abcdef")

// TestEngineAdmitsUnseenNativePost is the OUTER acceptance test for task 16.
//
// GIVEN a bridged community and an UNSEEN native author, WHEN a postv2 create
// event targeting that community arrives through the dispatcher→engine seam,
// THEN:
//
//  1. the author's AP actor is lazily minted (first federating interaction);
//  2. a social.coves.community.acceptance record exists in the COMMUNITY repo at
//     the digest rkey, pinning the postv2 uri + CID;
//  3. EXACTLY ONE Create{Page} activity is enqueued (outbound_activities +
//     outbound_deliveries) under the deterministic activity id;
//  4. an outbound_objects row for the post exists (community_did + snapshot) —
//     the state a later Delete{Page} is rebuilt from;
//  5. an admissions row records status=accepted.
//
// AND a full replay of the same event enqueues nothing new and writes no second
// acceptance (the rev gate + idempotent acceptance/enqueue).
//
// No network: the community inbox is resolved through a stub, and minting is
// local (RSA keygen + a DB row, no PLC).
func TestEngineAdmitsUnseenNativePost(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()

	seedBridgedCommunity(t, conn)
	requireUnseenAuthor(t, conn)

	repos := newRepos(t, conn)
	engine := wireEngine(t, conn, repos, realEnqueuer(t, conn))
	dispatcher := wireDispatcher(t, conn, engine, realEnqueuer(t, conn))

	require.NoError(t, dispatcher.HandleEvent(ctx, postV2CreateEvent(t)),
		"a well-formed postv2 to a bridged community must be admitted, not dead-lettered")

	// 1. Lazy mint.
	assert.Equal(t, 1, countWhere(t, conn, "ap_actors", "did", acAuthorDID),
		"the author's first accepted post must lazily mint exactly one AP actor")

	// 2. The acceptance record in the community repo.
	rkey := acceptrec.SubjectRKey(acPostURI)
	record, _, err := repos.GetRecord(ctx, acCommunityDID, acceptrec.CollectionAcceptance, rkey)
	require.NoError(t, err,
		"a social.coves.community.acceptance must exist in the COMMUNITY repo at SubjectRKey(%s)", acPostURI)
	assert.Equal(t, acceptrec.CollectionAcceptance, record["$type"])
	subject, ok := record["subject"].(map[string]any)
	require.True(t, ok, "the acceptance pins a strongRef subject, got %v", record["subject"])
	assert.Equal(t, acPostURI, subject["uri"], "the acceptance pins the postv2 at-uri")
	assert.Equal(t, acPostCID, subject["cid"], "the acceptance pins the postv2 CID it evaluated")

	// 3. Exactly one Create{Page} activity + one delivery, deterministic id.
	require.Equal(t, 1, countRows(t, conn, "outbound_activities"),
		"admission enqueues exactly one canonical activity for the post")
	require.Equal(t, 1, countRows(t, conn, "outbound_deliveries"),
		"...and exactly one per-community delivery")

	wantID := consume.ActivityID(acUserOrigin, acPostURI, "create", 0)
	var kind, objType string
	require.NoError(t, conn.QueryRowContext(ctx,
		`SELECT kind, payload->'object'->>'type' FROM outbound_activities WHERE activity_id = $1`,
		wantID).Scan(&kind, &objType),
		"the activity is keyed by the deterministic id ActivityID(origin, postURI, create, 0)")
	assert.Equal(t, "Create", kind, "a post create federates as Create{Page}")
	assert.Equal(t, "Page", objType, "the inner object is a Page (the Note/Page addressing split)")

	// 4. Outbound state for the post.
	stored, err := store.NewOutboundObjects(conn).GetByATURI(ctx, acPostURI)
	require.NoError(t, err,
		"outbound_objects must hold the post's state: a later Delete{Page} carries no body")
	assert.Equal(t, acCommunityDID, stored.CommunityDID, "the post's community is recorded")
	assert.NotEmpty(t, stored.TranslatedSnapshot, "the snapshot the Delete is rebuilt from is stored")

	// 5. The admissions ledger.
	status, code := admissionOf(t, conn, acCommunityDID, acPostURI)
	assert.Equal(t, StatusAccepted, status, "the ledger records the post as accepted")
	assert.Empty(t, code, "a clean accept carries no rejection code")

	// -------------------------------------------------------------------
	// Replay: the same event enqueues nothing new and writes no second acceptance.
	// -------------------------------------------------------------------
	require.NoError(t, dispatcher.HandleEvent(ctx, postV2CreateEvent(t)),
		"a replay is a normal skip, so the cursor still advances")

	assert.Equal(t, 1, countRows(t, conn, "outbound_activities"),
		"a replay must not enqueue a second activity (rev gate + idempotent id)")
	assert.Equal(t, 1, countRows(t, conn, "outbound_deliveries"))
	assert.Equal(t, 1, countWhere(t, conn, "ap_actors", "did", acAuthorDID),
		"a replay must not mint a second actor")
}

// TestEngineCrashInjectionIsAtomic is the crash-injection DoD: a fault-injected
// enqueuer that errors inside the acceptance commit's side effect must leave
// NEITHER the acceptance record NOR the outbound rows — the acceptance write and
// the enqueue are one transaction (acceptrec's side-effect commit), so a failing
// enqueue rolls the acceptance back too, and the failure PROPAGATES for retry.
func TestEngineCrashInjectionIsAtomic(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()

	seedBridgedCommunity(t, conn)
	repos := newRepos(t, conn)

	faulty := &faultyEnqueuer{err: stderrors.New("enqueue exploded mid-commit")}
	engine := wireEngine(t, conn, repos, faulty)
	dispatcher := wireDispatcher(t, conn, engine, faulty)

	err := dispatcher.HandleEvent(ctx, postV2CreateEvent(t))
	require.Error(t, err,
		"an enqueue failure inside the acceptance commit must PROPAGATE so the event retries — "+
			"a swallowed error would strand a post with an acceptance nobody delivered")

	rkey := acceptrec.SubjectRKey(acPostURI)
	_, _, gerr := repos.GetRecord(ctx, acCommunityDID, acceptrec.CollectionAcceptance, rkey)
	assert.Error(t, gerr, "no acceptance record may survive a failed enqueue (atomic)")

	assert.Zero(t, countRows(t, conn, "outbound_activities"),
		"and no outbound activity: the whole acceptance+enqueue transaction rolled back")
	assert.Zero(t, countRows(t, conn, "outbound_deliveries"))

	// The side effect writes the outbound_objects row AND the admissions ledger
	// row on the SAME acceptance tx, so a rollback must leave NEITHER. A non-tx
	// write of either would leak past the rollback undetected — pin both absent.
	assert.Zero(t, countRows(t, conn, "outbound_objects"),
		"the post's outbound_objects row must roll back with the acceptance (written on the tx)")
	assert.Zero(t, countRows(t, conn, "admissions"),
		"the accepted admissions row must roll back too — no ledger row may claim the post was "+
			"accepted when the acceptance itself did not commit")
}

// TestEngineRecordsOptedOutRejection pins the opt-out check MOVED into the
// engine: an opted-out author's postv2 now reaches the engine (the consumer no
// longer gates it), and the engine RECORDS a rejection with decision_code
// opted-out — writing no acceptance and enqueueing nothing.
func TestEngineRecordsOptedOutRejection(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()

	seedBridgedCommunity(t, conn)
	_, err := store.NewFederationPrefs(conn).Upsert(ctx, store.FederationPref{
		DID:    acAuthorDID,
		Source: store.FederationPrefSourceRecord,
	})
	require.NoError(t, err, "seed the author's opt-out record")

	repos := newRepos(t, conn)
	engine := wireEngine(t, conn, repos, realEnqueuer(t, conn))
	dispatcher := wireDispatcher(t, conn, engine, realEnqueuer(t, conn))

	require.NoError(t, dispatcher.HandleEvent(ctx, postV2CreateEvent(t)),
		"an opted-out author's post is a recorded rejection, not a failure to retry")

	status, code := admissionOf(t, conn, acCommunityDID, acPostURI)
	assert.Equal(t, StatusRejected, status, "the engine records the post as rejected")
	assert.Equal(t, DecisionOptedOut, code,
		"the rejection carries a distinct machine-readable reason the admin surface can show")

	rkey := acceptrec.SubjectRKey(acPostURI)
	_, _, gerr := repos.GetRecord(ctx, acCommunityDID, acceptrec.CollectionAcceptance, rkey)
	assert.Error(t, gerr, "a rejected post gets no acceptance record")
	assert.Zero(t, countRows(t, conn, "outbound_activities"),
		"and nothing is federated for a post authored under an opt-out")
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

func acceptanceDB(t *testing.T) *sql.DB {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database,
		"ap_actors", "ap_objects", "communities", "repo_state", "blocks", "firehose_events",
		"outbound_activities", "outbound_deliveries", "outbound_objects", "outbound_votes",
		"federation_prefs", "jetstream_record_revs", "jetstream_dead_letters", "admissions",
		// community_bans (migration 027). THIRD package to need this line, and
		// the failure never names it: a ban left by one test makes decide()
		// reject the NEXT test's author, which surfaces as "no acceptance
		// record" and "no actor was minted" — the engine looking broken rather
		// than the fixture being dirty. Any table a gate READS belongs here the
		// moment a test WRITES it.
		"community_bans")
	return database
}

// staticKeys signs the community's acceptance commit with one fixed key.
type staticKeys struct{ key *atcrypto.PrivateKeyK256 }

func (s staticKeys) SigningKey(context.Context, string, repo.KeyUse) (atcrypto.PrivateKey, error) {
	return s.key, nil
}

func newRepos(t *testing.T, conn *sql.DB) *repo.Manager {
	t.Helper()
	key, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	manager, err := repo.NewManager(conn, staticKeys{key: key}, nil)
	require.NoError(t, err)
	return manager
}

func newMinter(t *testing.T, conn *sql.DB) *personas.Service {
	t.Helper()
	custodian, err := identity.NewCustodian(acKEK)
	require.NoError(t, err)
	svc, err := personas.New(personas.Options{DB: conn, Custodian: custodian, UserOrigin: acUserOrigin})
	require.NoError(t, err)
	return svc
}

func realEnqueuer(t *testing.T, conn *sql.DB) *outbound.Enqueuer {
	t.Helper()
	enq, err := outbound.NewEnqueuer(outbound.EnqueuerOptions{
		DB:         conn,
		Translator: outbound.NewTranslator(acUserOrigin),
		Inboxes:    stubInboxResolver{},
		Actors:     store.NewAPActors(conn),
		UserOrigin: acUserOrigin,
	})
	require.NoError(t, err)
	return enq
}

// wireEngine takes the RepoManager INTERFACE, not *repo.Manager, so a test can
// interpose on the two operations the terminality decision spans (the removal
// read and the commit that acts on it) without a second wiring helper.
func wireEngine(t *testing.T, conn *sql.DB, repos acceptrec.RepoManager, enqueuer consume.OutboundEnqueuer) *Engine {
	t.Helper()
	engine, err := NewEngine(Options{
		Repos:       repos,
		Enqueuer:    enqueuer,
		Actors:      newMinter(t, conn),
		Resolver:    stubResolver{},
		Communities: store.NewCommunities(conn),
		Objects:     store.NewOutboundObjects(conn),
		Prefs:       store.NewFederationPrefs(conn),
		Admissions:  NewAdmissions(conn),
		UserOrigin:  acUserOrigin,
	})
	require.NoError(t, err)
	return engine
}

func wireDispatcher(t *testing.T, conn *sql.DB, engine consume.AcceptanceEngine, enqueuer consume.OutboundEnqueuer) *consume.Dispatcher {
	t.Helper()
	dispatcher, err := consume.NewDispatcher(consume.Options{
		DB:         conn,
		Actors:     newMinter(t, conn),
		Enqueuer:   enqueuer,
		Resolver:   stubResolver{},
		Engine:     engine,
		UserOrigin: acUserOrigin,
	})
	require.NoError(t, err)
	return dispatcher
}

// seedBridgedCommunity registers the community (bridged = a communities row). It
// deliberately gets NO repo_state row: the engine genesis-commits the community
// repo on the first acceptance, exactly as production does.
func seedBridgedCommunity(t *testing.T, conn *sql.DB) {
	t.Helper()
	_, err := store.NewCommunities(conn).UpsertCommunity(context.Background(), store.Community{
		APGroupID:         acCommunityAPID,
		DID:               acCommunityDID,
		PreferredUsername: acCommunityName,
		Instance:          acCommunityHost,
		FollowState:       store.FollowStateAccepted,
	})
	require.NoError(t, err, "seed bridged community")
}

func requireUnseenAuthor(t *testing.T, conn *sql.DB) {
	t.Helper()
	assert.Zero(t, countWhere(t, conn, "ap_actors", "did", acAuthorDID),
		"the author must be unseen so the lazy-mint assertion is real")
	assert.Zero(t, countWhere(t, conn, "repo_state", "did", acAuthorDID))
}

// postV2CreateEvent builds the postv2 create frame as literal wire JSON (pinning
// the shape) and parses it into the JetstreamEvent the dispatcher consumes.
func postV2CreateEvent(t *testing.T) *consume.JetstreamEvent {
	t.Helper()
	frame := []byte(fmt.Sprintf(`{
  "did": %q,
  "time_us": %d,
  "kind": "commit",
  "commit": {
    "rev": %q,
    "operation": "create",
    "collection": "social.coves.community.postv2",
    "rkey": %q,
    "cid": %q,
    "record": {
      "$type": "social.coves.community.postv2",
      "community": %q,
      "title": "hello from atproto",
      "content": "the body of the post",
      "createdAt": "2026-08-12T10:00:00.000Z"
    }
  }
}`, acAuthorDID, acPostTimeUS, acPostRev, acPostRKey, acPostCID, acCommunityDID))

	var event consume.JetstreamEvent
	require.NoError(t, json.Unmarshal(frame, &event), "the test frame must be valid wire JSON")
	return &event
}

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

// stubResolver returns the author's handle without touching the network (the
// handle verification itself has its own tests in consume).
type stubResolver struct{}

func (stubResolver) ResolveDIDHandle(context.Context, string) (string, error) {
	return acAuthorHandle, nil
}

// stubInboxResolver answers the community's inbox from a fixed map — the outer
// test never dials Lemmy.
type stubInboxResolver struct{}

func (stubInboxResolver) ResolveInbox(_ context.Context, communityAPID string) (string, error) {
	if communityAPID == acCommunityAPID {
		return acCommunityInbox, nil
	}
	return "", fmt.Errorf("no inbox route for %s", communityAPID)
}

// faultyEnqueuer fails EnqueueActivity to exercise the crash-injection atomicity
// path: it stands in for a delivery seam that errors inside the acceptance
// commit's side effect.
type faultyEnqueuer struct{ err error }

func (f *faultyEnqueuer) EnqueueActivity(context.Context, *sql.Tx, string, string, string, consume.Intent) error {
	return f.err
}

// ---------------------------------------------------------------------------
// Query helpers
// ---------------------------------------------------------------------------

func countRows(t *testing.T, conn *sql.DB, table string) int {
	t.Helper()
	var n int
	require.NoError(t, conn.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM `+table).Scan(&n))
	return n
}

func countWhere(t *testing.T, conn *sql.DB, table, col, val string) int {
	t.Helper()
	var n int
	require.NoError(t, conn.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM `+table+` WHERE `+col+` = $1`, val).Scan(&n))
	return n
}

func admissionOf(t *testing.T, conn *sql.DB, communityDID, postURI string) (status, code string) {
	t.Helper()
	err := conn.QueryRowContext(context.Background(),
		`SELECT status, decision_code FROM admissions WHERE community_did = $1 AND post_uri = $2`,
		communityDID, postURI).Scan(&status, &code)
	require.NoError(t, err,
		"an admissions row must exist for (%s, %s): the engine records every decision", communityDID, postURI)
	return status, code
}
