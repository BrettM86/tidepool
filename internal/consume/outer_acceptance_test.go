package consume

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/identity"
	"tidepool/internal/personas"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// The world this acceptance test builds.
const (
	acceptUserOrigin = "https://coves.social"

	// A community Tidepool hosts: it has a communities row AND a repo_state
	// row, which is what makes its DID "a repo we commit into".
	acceptCommunityDID  = "did:plc:44ybard66vv44zksje25o7dz"
	acceptCommunityAPID = "https://lemmy.world/c/technology"
	acceptCommunityHost = "lemmy.world"
	acceptCommunityName = "technology"
	acceptCommunityHead = "bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqrsqxi3jgxte"
	acceptCommunityRev  = "3lzhead000001"
	acceptRootAuthorDID = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	acceptRootRKey      = "3lzroot2222aa"
	acceptRootCID       = "bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqrsqxi3jgxte"
	acceptRootAPID      = acceptUserOrigin + "/ap/object/" + acceptRootRKey
	acceptRootATURI     = "at://" + acceptRootAuthorDID + "/social.coves.community.postv2/" + acceptRootRKey

	// The commenter. This DID has NO row anywhere: no ap_actors, no
	// bridged_actors, no communities, no repo_state. Its actor must be minted
	// by the act of commenting.
	acceptCommenterDID = "did:plc:7iza6de2dwap2sbkpav7c6c6"
	// The commenter's handle, which the bridge has to discover and VERIFY on
	// its own: the commit carries no handle, and the local part it derives is
	// frozen at creation.
	acceptCommenterHandle    = "alice.coves.social"
	acceptCommenterLocalPart = "alice"
	acceptCommentRKey        = "3lzcmnt3333bb"
	acceptCommentCID         = "bafyreievgu2ty7qbiaaom5zhmkznsnajuzideek3lo7e65dwqlrvrxnmo4"
	acceptCommentRev         = "3lzcmntrev001"
	acceptCommentATURI       = "at://" + acceptCommenterDID + "/social.coves.community.comment/" + acceptCommentRKey
	acceptCommentTimeUS      = int64(1_775_000_000_000_000)
)

// acceptKEK seals minted actors' AP RSA keys (32 bytes, AES-256).
var acceptKEK = []byte("0123456789abcdef0123456789abcdef")

// acceptActivityIDPattern is the deterministic activity id shape (decision
// 12): the user origin, the fixed /ap/activity/ path, and a sha256 digest.
// Deterministic because a redelivery must reuse the id a peer already saw and
// a Delete must be buildable when the record body is long gone.
const acceptActivityIDPattern = `^https://coves\.social/ap/activity/[0-9a-f]{64}$`

// TestConsumerFederatesAnUnseenNativeComment is the OUTER acceptance test for
// task 14.
//
// GIVEN a bridged community Tidepool hosts, a thread root already mapped in
// ap_objects, and a native commenter whose DID the bridge has never seen,
// WHEN a scripted Jetstream commit creating a social.coves.community.comment
// in that thread arrives over a real WebSocket,
// THEN:
//
//  1. the commenter's AP actor is MINTED lazily (first federating
//     interaction — no eager mint anywhere else);
//  2. outbound_objects holds the durable state a later Delete will be built
//     from, carrying the community the comment belongs to;
//  3. EXACTLY ONE outbound intent is handed to the task 15 seam, under a
//     deterministic activity id;
//  4. the cursor is persisted so a restart resumes instead of live-tailing.
//
// AND THEN, the property everything else rests on: replaying the SAME script
// from cursor 0 into a fresh connector produces ZERO new intents and ZERO
// state changes. That is the rev gate doing its job — stable activity ids
// alone cannot prevent a rewind from resurrecting stale state.
//
// No network: the only endpoint dialled is the httptest fake.
func TestConsumerFederatesAnUnseenNativeComment(t *testing.T) {
	conn := consumeTestDB(t)
	ctx := context.Background()

	seedBridgedCommunity(t, conn)
	seedThreadRoot(t, conn)
	requireNoRowsForDID(t, conn, acceptCommenterDID)

	minter := newPersonasService(t, conn)
	state := NewPostgresStateStore(conn, CursorSchemaVersion)
	objects := store.NewOutboundObjects(conn)

	// The atproto identity world: a PLC directory serving the commenter's DID
	// document, and the handle's own well-known claiming the DID back. Both on
	// httptest — no test ever reaches the network.
	identity := newFakeIdentity(t)
	identity.claim(acceptCommenterDID, acceptCommenterHandle)
	resolver := identity.resolver(t)

	// -------------------------------------------------------------------
	// Run 1: first sighting.
	// -------------------------------------------------------------------
	firstEnqueuer := &recordingEnqueuer{}
	runConnector(t, conn, minter, resolver, state, firstEnqueuer, acceptCommentTimeUS,
		commentCreateFrame(acceptCommentTimeUS, acceptCommentRev))

	// 1. Lazy mint, through resolution. The commit carries no handle, so the
	//    bridge had to fetch the DID document, read the handle it claims, and
	//    confirm the handle claims the DID back — before minting, because the
	//    local part is frozen at creation.
	var mintedActors int
	require.NoError(t, conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM ap_actors WHERE did = $1`, acceptCommenterDID).Scan(&mintedActors))
	require.Equal(t, 1, mintedActors,
		"the commenter's first federating interaction must lazily mint exactly one AP actor "+
			"for %s (task 13's get-or-create)", acceptCommenterDID)

	var mintedLocalPart string
	require.NoError(t, conn.QueryRowContext(ctx,
		`SELECT local_part FROM ap_actors WHERE did = $1`, acceptCommenterDID).Scan(&mintedLocalPart))
	require.Equal(t, acceptCommenterLocalPart, mintedLocalPart,
		"the frozen local part derives from the VERIFIED handle %q — this is the whole "+
			"reason the consumer resolves before minting", acceptCommenterHandle)

	require.Positive(t, identity.PLCHits(), "the DID document was fetched")
	require.Positive(t, identity.WellKnownHits(),
		"and the handle was asked to claim the DID back: one-way trust would let any "+
			"DID freeze somebody else's name")

	// 2. Durable outbound state, keyed by the comment's at-uri.
	stored, err := objects.GetByATURI(ctx, acceptCommentATURI)
	require.NoError(t, err,
		"outbound_objects must hold state for %s: a later delete commit carries no "+
			"record body and no CID, so the Delete can only be built from here",
		acceptCommentATURI)
	require.NotNil(t, stored)
	assert.Equal(t, acceptCommunityDID, stored.CommunityDID,
		"the comment's community is resolved through the thread root's ap_objects mapping")
	assert.NotEmpty(t, stored.APObjectID, "the AP id the comment federates as must be recorded")
	assert.False(t, stored.IsTombstoned(), "a create must not tombstone anything")

	// 3. Exactly one intent, deterministic id.
	calls := firstEnqueuer.Calls()
	require.Len(t, calls, 1,
		"a single comment create must produce exactly one outbound intent, got %d", len(calls))
	call := calls[0]

	assert.Equal(t, acceptCommenterDID, call.ActorDID, "the intent is attributed to the commenter")
	assert.Equal(t, acceptRootATURI, call.ParentATURI,
		"parentATURI carries the causal dependency (decision 15): the reply must not be "+
			"delivered before the thing it replies to")
	assert.NotEmpty(t, call.OrderingKey, "an intent must carry an ordering key")

	intent, ok := call.Intent.(CommentIntent)
	require.True(t, ok, "a comment create must enqueue a CommentIntent, got %T", call.Intent)
	assert.Equal(t, "create", intent.Op)
	assert.Equal(t, acceptCommentATURI, intent.ATURI)
	assert.Equal(t, acceptCommunityAPID, intent.CommunityAPID,
		"the intent targets the community's AP Group id")

	require.Regexp(t, acceptActivityIDPattern, intent.ActivityID(),
		"outbound activity ids are {origin}/ap/activity/{sha256} (decision 12)")
	require.Equal(t, ActivityID(acceptUserOrigin, acceptCommentATURI, "create", 0),
		intent.ActivityID(),
		"the id must come from the ONE exported derivation, seeded with "+
			"last_activity_seq 0 for a create")

	// 4. The cursor survived the run.
	persisted := readCursor(t, conn, ConsumerNative, CursorSchemaVersion)
	require.GreaterOrEqual(t, persisted, acceptCommentTimeUS,
		"the cursor must be persisted past the processed event: a restart that live-tails "+
			"is the exact data loss cursors exist to prevent")

	// Nothing failed quietly.
	assert.Zero(t, countRows(t, conn, "jetstream_dead_letters"),
		"a well-formed comment in a bridged community must not dead-letter")

	// The rev gate recorded the applied revision.
	var gatedRev string
	err = conn.QueryRowContext(ctx,
		`SELECT rev FROM jetstream_record_revs WHERE record_uri = $1`, acceptCommentATURI).Scan(&gatedRev)
	require.NoError(t, err,
		"jetstream_record_revs must hold the applied rev for %s — it is the gate a "+
			"replay is rejected against", acceptCommentATURI)
	assert.Equal(t, acceptCommentRev, gatedRev)

	// -------------------------------------------------------------------
	// Run 2: full replay from cursor 0 in a fresh connector.
	// -------------------------------------------------------------------
	before := snapshotState(t, conn)

	// Cursor 0 = the worst case the FOLLOWUPS note warns about: a cursor
	// behind Jetstream's retention replays the ENTIRE store.
	_, err = conn.ExecContext(ctx, `DELETE FROM consumer_cursors`)
	require.NoError(t, err)

	replayEnqueuer := &recordingEnqueuer{}
	runConnector(t, conn, minter, resolver, state, replayEnqueuer, acceptCommentTimeUS,
		commentCreateFrame(acceptCommentTimeUS, acceptCommentRev))

	assert.Empty(t, replayEnqueuer.Calls(),
		"a full replay must enqueue NOTHING: the rev gate rejects the already-applied "+
			"revision, so the peer is never asked to process the comment twice")

	after := snapshotState(t, conn)
	assert.Equal(t, before, after,
		"a full replay must leave every row byte-identical — no reseeded seq, no "+
			"refreshed updated_at, no second actor")

	assert.Zero(t, countRows(t, conn, "jetstream_dead_letters"),
		"a replayed event is a skip, not a failure: it must not land in the DLQ")
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

// consumeTestDB returns a migrated connection with every table this test
// touches emptied.
func consumeTestDB(t *testing.T) *sql.DB {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database,
		"ap_actors", "ap_objects", "communities", "repo_state",
		"outbound_objects", "outbound_votes", "federation_prefs",
		"consumer_cursors", "jetstream_record_revs", "jetstream_dead_letters")
	return database
}

func newPersonasService(t *testing.T, database *sql.DB) *personas.Service {
	t.Helper()
	custodian, err := identity.NewCustodian(acceptKEK)
	require.NoError(t, err, "build custodian")
	svc, err := personas.New(personas.Options{
		DB:         database,
		Custodian:  custodian,
		UserOrigin: acceptUserOrigin,
	})
	require.NoError(t, err, "build personas service")
	return svc
}

// runConnector wires a fresh Connector + Dispatcher over a fresh fake
// Jetstream, runs it until the connector accounts for the scripted event, then
// shuts it down cleanly (which flushes the cursor).
func runConnector(
	t *testing.T,
	database *sql.DB,
	minter ActorMinter,
	resolver DIDResolver,
	state *PostgresStateStore,
	enqueuer OutboundEnqueuer,
	lastEventTimeUS int64,
	script ...[]byte,
) {
	t.Helper()

	fake := newFakeJetstream(t, script...)

	dispatcher, err := NewDispatcher(Options{
		DB:         database,
		Actors:     minter,
		Resolver:   resolver,
		Enqueuer:   enqueuer,
		UserOrigin: acceptUserOrigin,
	})
	require.NoError(t, err, "build dispatcher")
	require.NotNil(t, dispatcher)

	connector := NewConnector(ConsumerNative, fake.URL(), dispatcher,
		WithCursorStore(state),
		WithDeadLetterWriter(state),
		WithCursorFlushInterval(20*time.Millisecond),
		WithReconnectDelay(50*time.Millisecond),
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- connector.Start(ctx) }()

	// The connector's own accounting is the sentinel: the cursor moves past an
	// event once it is fully handled (or safely dead-lettered), so waiting on
	// it means every assertion below runs against a settled pipeline.
	deadline := time.Now().Add(5 * time.Second)
	settled := false
	for time.Now().Before(deadline) {
		if connector.Status().CursorTimeUS >= lastEventTimeUS {
			settled = true
			break
		}
		select {
		case err := <-done:
			cancel()
			t.Fatalf("connector Start returned early (%v) without consuming the scripted "+
				"event; dials=%d", err, fake.Dials())
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !settled {
		cancel()
		<-done
		t.Fatalf("timed out after 5s: the connector never accounted for the scripted event "+
			"(want cursor >= %d, got %d; dials=%d). The consumer must dial the Jetstream "+
			"URL, read frames, dispatch them, and advance its cursor.",
			lastEventTimeUS, connector.Status().CursorTimeUS, fake.Dials())
	}

	cancel()
	select {
	case <-done: // the shutdown path flushes the cursor with a fresh context
	case <-time.After(5 * time.Second):
		t.Fatal("connector did not shut down within 5s of context cancellation")
	}
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// commentCreateFrame is the raw Jetstream frame for a comment create. It is
// written as literal JSON, not marshalled from CommitEvent, so it pins the
// WIRE shape rather than agreeing with our own structs.
func commentCreateFrame(timeUS int64, rev string) []byte {
	return []byte(fmt.Sprintf(`{
  "did": %q,
  "time_us": %d,
  "kind": "commit",
  "commit": {
    "rev": %q,
    "operation": "create",
    "collection": "social.coves.community.comment",
    "rkey": %q,
    "cid": %q,
    "record": {
      "$type": "social.coves.community.comment",
      "reply": {
        "root":   {"uri": %q, "cid": %q},
        "parent": {"uri": %q, "cid": %q}
      },
      "content": "first reply from atproto",
      "createdAt": "2026-08-12T10:00:00.000Z"
    }
  }
}`,
		acceptCommenterDID, timeUS, rev, acceptCommentRKey, acceptCommentCID,
		acceptRootATURI, acceptRootCID, acceptRootATURI, acceptRootCID))
}

// seedBridgedCommunity registers the community AND gives it a repo_state row.
// repo_state is the hosted-repo filter's membership test (a PK lookup, not an
// enumeration): a DID with a row there is a repo Tidepool commits into, whose
// events must never be consumed back in — their outbound is enqueued at write
// time, and consuming them too would double-deliver.
func seedBridgedCommunity(t *testing.T, database *sql.DB) {
	t.Helper()
	ctx := context.Background()

	_, err := store.NewCommunities(database).UpsertCommunity(ctx, store.Community{
		APGroupID:         acceptCommunityAPID,
		DID:               acceptCommunityDID,
		PreferredUsername: acceptCommunityName,
		Instance:          acceptCommunityHost,
		FollowState:       store.FollowStateAccepted,
	})
	require.NoError(t, err, "seed community")

	_, err = database.ExecContext(ctx,
		`INSERT INTO repo_state (did, head_cid, rev) VALUES ($1, $2, $3)`,
		acceptCommunityDID, acceptCommunityHead, acceptCommunityRev)
	require.NoError(t, err, "seed repo_state for the hosted community repo")
}

// seedThreadRoot maps the post the comment replies to. This is how the
// consumer learns the comment belongs to a bridged community: it resolves
// reply.root through ap_objects rather than trusting anything in the comment.
func seedThreadRoot(t *testing.T, database *sql.DB) {
	t.Helper()
	published := time.Date(2026, 8, 11, 9, 0, 0, 0, time.UTC)
	_, err := store.NewAPObjects(database).PutMapping(context.Background(), store.APObjectMapping{
		APID:           acceptRootAPID,
		APType:         "Page",
		OriginInstance: "coves.social",
		Origin:         store.OriginBridge,
		DID:            acceptRootAuthorDID,
		AuthorDID:      acceptRootAuthorDID,
		CommunityDID:   acceptCommunityDID,
		Collection:     "social.coves.community.postv2",
		RKey:           acceptRootRKey,
		CID:            acceptRootCID,
		PublishedAt:    &published,
	})
	require.NoError(t, err, "seed thread root mapping")
}

// requireNoRowsForDID proves the commenter really is unseen, so the mint
// assertion cannot pass on a row some other fixture left behind.
func requireNoRowsForDID(t *testing.T, database *sql.DB, did string) {
	t.Helper()
	ctx := context.Background()
	for _, q := range []struct{ what, query string }{
		{"ap_actors", `SELECT COUNT(*) FROM ap_actors WHERE did = $1`},
		{"bridged_actors", `SELECT COUNT(*) FROM bridged_actors WHERE did = $1`},
		{"communities", `SELECT COUNT(*) FROM communities WHERE did = $1`},
		{"repo_state", `SELECT COUNT(*) FROM repo_state WHERE did = $1`},
		{"ap_objects", `SELECT COUNT(*) FROM ap_objects WHERE did = $1`},
	} {
		var n int
		require.NoError(t, database.QueryRowContext(ctx, q.query, did).Scan(&n))
		require.Zero(t, n, "%s must hold no row for the unseen commenter %s", q.what, did)
	}
}

// ---------------------------------------------------------------------------
// State snapshots (the replay proof)
// ---------------------------------------------------------------------------

// replaySnapshot is every mutable fact the replay must leave alone.
type replaySnapshot struct {
	ActorCount      int
	ActorLocalPart  string
	ActorCreatedAt  time.Time
	ObjectSeq       int
	ObjectUpdatedAt time.Time
	ObjectTombstone bool
	ObjectSnapshot  string
	GateRev         string
}

func snapshotState(t *testing.T, database *sql.DB) replaySnapshot {
	t.Helper()
	ctx := context.Background()

	var snap replaySnapshot
	require.NoError(t, database.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM ap_actors WHERE did = $1`, acceptCommenterDID).Scan(&snap.ActorCount))
	require.NoError(t, database.QueryRowContext(ctx,
		`SELECT local_part, created_at FROM ap_actors WHERE did = $1`, acceptCommenterDID).
		Scan(&snap.ActorLocalPart, &snap.ActorCreatedAt))

	var tombstonedAt sql.NullTime
	var snapshotJSON sql.NullString
	require.NoError(t, database.QueryRowContext(ctx,
		`SELECT last_activity_seq, updated_at, tombstoned_at, translated_snapshot::text
		   FROM outbound_objects WHERE at_uri = $1`, acceptCommentATURI).
		Scan(&snap.ObjectSeq, &snap.ObjectUpdatedAt, &tombstonedAt, &snapshotJSON))
	snap.ObjectTombstone = tombstonedAt.Valid
	snap.ObjectSnapshot = snapshotJSON.String

	require.NoError(t, database.QueryRowContext(ctx,
		`SELECT rev FROM jetstream_record_revs WHERE record_uri = $1`, acceptCommentATURI).
		Scan(&snap.GateRev))
	return snap
}

func readCursor(t *testing.T, database *sql.DB, consumerName string, schemaVersion int) int64 {
	t.Helper()
	var cursor int64
	err := database.QueryRowContext(context.Background(),
		`SELECT cursor_time_us FROM consumer_cursors
		  WHERE consumer_name = $1 AND schema_version = $2`,
		consumerName, schemaVersion).Scan(&cursor)
	require.NoError(t, err,
		"consumer_cursors must hold a row for (%s, %d) after a run", consumerName, schemaVersion)
	return cursor
}

func countRows(t *testing.T, database *sql.DB, table string) int {
	t.Helper()
	var n int
	require.NoError(t, database.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM `+table).Scan(&n))
	return n
}
