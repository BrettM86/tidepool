package consume

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// Task 14 cycle D: the dispatcher skeleton — which events reach which handler,
// which events never reach one at all, and the id derivation everything
// outbound is keyed by.

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

func dispatchTestDB(t *testing.T) *sql.DB {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database,
		"ap_actors", "ap_objects", "communities", "repo_state",
		"outbound_objects", "outbound_votes", "federation_prefs",
		"consumer_cursors", "jetstream_record_revs", "jetstream_dead_letters",
		// The gates the comment and vote paths now read (17c-2 locks, 17c-3
		// bans). FIFTH package to need this: any table a GATE READS belongs
		// here the moment any test WRITES it, or a leftover row refuses the
		// next test's author and the failure names neither the ban nor the
		// test that left one.
		"object_moderation", "community_bans")
	return database
}

// recordingMinter stands in for personas.Service. Using the seam rather than
// the real service keeps these cycles independent of the handle resolver
// (cycle F) while still pinning WHETHER a mint was attempted, which is the
// ordering question the opt-out gate is about.
type recordingMinter struct {
	// db makes the double behave like the real service in the one way that
	// matters here: it actually CREATES the ap_actors row, so a second event
	// for the same DID finds an existing actor. Without that, "resolve only
	// before the FIRST mint" could not be observed through this seam.
	db *sql.DB

	mu    sync.Mutex
	calls []struct{ DID, Handle string }
	err   error
}

func (m *recordingMinter) CreateActorForDID(ctx context.Context, did, handle string) (*store.APActor, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, struct{ DID, Handle string }{did, handle})
	if m.err != nil {
		return nil, m.err
	}
	localPart := "minted"
	if handle != "" {
		localPart, _, _ = strings.Cut(handle, ".")
	}
	if m.db != nil {
		// Get-or-create, like the real service.
		_, err := m.db.ExecContext(ctx, `
			INSERT INTO ap_actors (did, kind, actor_id, normalized_origin, local_part,
			                       rsa_key_sealed, rsa_key_version, public_key_pem)
			VALUES ($1, 'person', $2, 'coves.social', $3, '\x00'::bytea, 1, 'pem')
			ON CONFLICT (did) DO NOTHING`,
			did, acceptUserOrigin+"/ap/actor/"+did, localPart)
		if err != nil {
			return nil, err
		}
	}
	return &store.APActor{DID: did, Kind: store.ActorTypePerson, LocalPart: localPart}, nil
}

func (m *recordingMinter) Handles() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	handles := []string{}
	for _, call := range m.calls {
		handles = append(handles, call.Handle)
	}
	return handles
}

func (m *recordingMinter) DIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	dids := []string{}
	for _, call := range m.calls {
		dids = append(dids, call.DID)
	}
	return dids
}

// recordingEngine stands in for the task 16 acceptance engine.
type recordingEngine struct {
	mu      sync.Mutex
	commits []CommitEvent
	dids    []string
	err     error
}

func (e *recordingEngine) AdmitPost(_ context.Context, did string, commit *CommitEvent) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.dids = append(e.dids, did)
	e.commits = append(e.commits, *commit)
	return e.err
}

func (e *recordingEngine) Calls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.commits)
}

func (e *recordingEngine) Commits() []CommitEvent {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]CommitEvent(nil), e.commits...)
}

func (e *recordingEngine) DIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.dids...)
}

// recordingDeleter stands in for the task 17 destructive seam.
type recordingDeleter struct {
	mu   sync.Mutex
	dids []string
	err  error
}

func (d *recordingDeleter) DeleteRemoteContent(_ context.Context, did string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dids = append(d.dids, did)
	return d.err
}

func (d *recordingDeleter) DIDs() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.dids...)
}

// dispatchFixture is the world a dispatcher test runs against, plus every
// recorder wired into it.
type dispatchFixture struct {
	db         *sql.DB
	dispatcher *Dispatcher
	minter     *recordingMinter
	enqueuer   *recordingEnqueuer
	engine     *recordingEngine
	deleter    *recordingDeleter
	resolver   *recordingResolver
}

func newDispatchFixture(t *testing.T, database *sql.DB, mutate ...func(*Options)) *dispatchFixture {
	t.Helper()
	fixture := &dispatchFixture{
		db:       database,
		minter:   &recordingMinter{db: database},
		enqueuer: &recordingEnqueuer{},
		engine:   &recordingEngine{},
		deleter:  &recordingDeleter{},
		resolver: &recordingResolver{handle: dispatchNativeHandle},
	}
	opts := Options{
		DB:            database,
		Actors:        fixture.minter,
		Enqueuer:      fixture.enqueuer,
		Engine:        fixture.engine,
		RemoteDeleter: fixture.deleter,
		Resolver:      fixture.resolver,
		UserOrigin:    acceptUserOrigin,
	}
	for _, m := range mutate {
		m(&opts)
	}
	dispatcher, err := NewDispatcher(opts)
	require.NoError(t, err, "build dispatcher")
	require.NotNil(t, dispatcher)
	fixture.dispatcher = dispatcher
	return fixture
}

// handle parses a raw wire frame and dispatches it, exactly as the connector
// would. Frames stay raw JSON so the dispatcher tests pin the same wire shape
// the connector tests do.
func (f *dispatchFixture) handle(t *testing.T, frame []byte) error {
	t.Helper()
	event := parseFrame(t, frame)
	return f.dispatcher.HandleEvent(context.Background(), event)
}

func parseFrame(t *testing.T, frame []byte) *JetstreamEvent {
	t.Helper()
	var event JetstreamEvent
	require.NoError(t, json.Unmarshal(frame, &event), "the test frame must be valid wire JSON")
	return &event
}

// ---------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------

const (
	dispatchNativeDID = "did:plc:7iza6de2dwap2sbkpav7c6c6"
	// dispatchNativeHandle is what the resolver verifies for that DID; the
	// local part derives from its first label.
	dispatchNativeHandle = "nativeuser.coves.social"
	dispatchRev          = "3lzrev0000001"
	dispatchRevHigher    = "3lzrev0000002"
)

// federationFrame is the opt-out record (literal:self).
func federationFrame(did, rev, operation string, enabled, deleteRemote bool) []byte {
	record := ""
	if operation != "delete" {
		record = fmt.Sprintf(`,"cid":"bafyreievgu2ty7qbiaaom5zhmkznsnajuzideek3lo7e65dwqlrvrxnmo4",`+
			`"record":{"$type":"social.coves.bridge.federation","enabled":%t,"deleteRemote":%t}`,
			enabled, deleteRemote)
	}
	return []byte(fmt.Sprintf(
		`{"did":%q,"time_us":6100,"kind":"commit","commit":{"rev":%q,"operation":%q,`+
			`"collection":"social.coves.bridge.federation","rkey":"self"%s}}`,
		did, rev, operation, record))
}

// federationFrameNoDeleteRemote omits the field entirely — the common shape,
// since deleteRemote is optional and most opt-outs are the soft tier.
func federationFrameNoDeleteRemote(did, rev string, enabled bool) []byte {
	return []byte(fmt.Sprintf(
		`{"did":%q,"time_us":6100,"kind":"commit","commit":{"rev":%q,"operation":"create",`+
			`"collection":"social.coves.bridge.federation","rkey":"self",`+
			`"cid":"bafyreievgu2ty7qbiaaom5zhmkznsnajuzideek3lo7e65dwqlrvrxnmo4",`+
			`"record":{"$type":"social.coves.bridge.federation","enabled":%t}}}`,
		did, rev, enabled))
}

func postV2Frame(did, rev, rkey, communityDID string) []byte {
	return []byte(fmt.Sprintf(
		`{"did":%q,"time_us":6200,"kind":"commit","commit":{"rev":%q,"operation":"create",`+
			`"collection":"social.coves.community.postv2","rkey":%q,`+
			`"cid":"bafyreievgu2ty7qbiaaom5zhmkznsnajuzideek3lo7e65dwqlrvrxnmo4",`+
			`"record":{"$type":"social.coves.community.postv2","community":%q,`+
			`"title":"hello","createdAt":"2026-08-12T10:00:00.000Z"}}}`,
		did, rev, rkey, communityDID))
}

// unwantedFrame is a collection this consumer never subscribed to. Jetstream's
// wantedCollections should keep them off the wire, but a shared feed or a
// redriven legacy frame can still deliver one.
func unwantedFrame(did, rev string) []byte {
	return []byte(fmt.Sprintf(
		`{"did":%q,"time_us":6300,"kind":"commit","commit":{"rev":%q,"operation":"create",`+
			`"collection":"social.coves.actor.block","rkey":"3lzblock11111",`+
			`"cid":"bafyreievgu2ty7qbiaaom5zhmkznsnajuzideek3lo7e65dwqlrvrxnmo4",`+
			`"record":{"$type":"social.coves.actor.block","subject":"did:plc:someone"}}}`,
		did, rev))
}

// commentFrameFor is the acceptance test's comment, re-pointed at an arbitrary
// author so the opt-out tests can pick who is speaking.
func commentFrameFor(did, rev, rkey string) []byte {
	return []byte(fmt.Sprintf(
		`{"did":%q,"time_us":6400,"kind":"commit","commit":{"rev":%q,"operation":"create",`+
			`"collection":"social.coves.community.comment","rkey":%q,`+
			`"cid":"bafyreievgu2ty7qbiaaom5zhmkznsnajuzideek3lo7e65dwqlrvrxnmo4",`+
			`"record":{"$type":"social.coves.community.comment","reply":{`+
			`"root":{"uri":%q,"cid":%q},"parent":{"uri":%q,"cid":%q}},`+
			`"content":"hi","createdAt":"2026-08-12T10:00:00.000Z"}}}`,
		did, rev, rkey, acceptRootATURI, acceptRootCID, acceptRootATURI, acceptRootCID))
}

// accountSeqCounter hands out a strictly increasing seq to every
// accountFrameFor frame. #account carries a monotonic seq per DID, and the
// consumer now rejects a stale one (C4), so a deactivate→reactivate flow built
// from two frames needs the second to out-rank the first. Tests that pin the
// seq gate itself use accountFrameSeq with explicit values instead.
var accountSeqCounter atomic.Int64

func accountFrameFor(did string, active bool, status string) []byte {
	seq := accountSeqCounter.Add(1)
	return []byte(fmt.Sprintf(
		`{"did":%q,"time_us":%d,"kind":"account",`+
			`"account":{"did":%q,"seq":%d,"time":"2026-08-12T10:00:00.000Z","active":%t,"status":%q}}`,
		did, 6500+seq, did, seq, active, status))
}

func identityFrameFor(did, handle string) []byte {
	return []byte(fmt.Sprintf(
		`{"did":%q,"time_us":6600,"kind":"identity",`+
			`"identity":{"did":%q,"handle":%q,"seq":10,"time":"2026-08-12T10:00:00.000Z"}}`,
		did, did, handle))
}

func deliveryPaused(t *testing.T, database *sql.DB, did string) bool {
	t.Helper()
	var paused bool
	require.NoError(t, database.QueryRowContext(context.Background(),
		`SELECT delivery_paused FROM ap_actors WHERE did = $1`, did).Scan(&paused))
	return paused
}

// seedAPActor inserts a minimal ap_actors row directly. The lifecycle columns
// are what the tests read; the key material is filler.
func seedAPActor(t *testing.T, database *sql.DB, did, localPart string) {
	t.Helper()
	_, err := database.ExecContext(context.Background(), `
		INSERT INTO ap_actors (did, kind, actor_id, normalized_origin, local_part,
		                       rsa_key_sealed, rsa_key_version, public_key_pem)
		VALUES ($1, 'person', $2, 'coves.social', $3, '\x00'::bytea, 1, 'pem')`,
		did, acceptUserOrigin+"/ap/actor/"+did, localPart)
	require.NoError(t, err, "seed ap_actors row for %s", did)
}

func actorEnabled(t *testing.T, database *sql.DB, did string) (enabled bool, disabledAt *string) {
	t.Helper()
	var disabled sql.NullString
	require.NoError(t, database.QueryRowContext(context.Background(),
		`SELECT enabled, disabled_at::text FROM ap_actors WHERE did = $1`, did).
		Scan(&enabled, &disabled))
	if disabled.Valid {
		return enabled, &disabled.String
	}
	return enabled, nil
}

// ---------------------------------------------------------------------------
// D1 — routing
// ---------------------------------------------------------------------------

func TestDispatcher_RoutesFederationRecordsToThePreferenceHandler(t *testing.T) {
	database := dispatchTestDB(t)
	fixture := newDispatchFixture(t, database)

	require.NoError(t, fixture.handle(t,
		federationFrameNoDeleteRemote(dispatchNativeDID, dispatchRev, false)))

	pref, err := store.NewFederationPrefs(database).Get(context.Background(), dispatchNativeDID)
	require.NoError(t, err,
		"a social.coves.bridge.federation commit must reach the preference handler")
	require.NotNil(t, pref)
	assert.False(t, pref.Enabled)

	assert.Zero(t, fixture.engine.Calls(), "a federation record is not a post")
	assert.Empty(t, fixture.minter.DIDs(),
		"NO eager mint on an opt-out record: actors mint at the first federating "+
			"interaction, and an opt-out is the opposite of one")
}

func TestDispatcher_RoutesPostV2ToTheAcceptanceEngine(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	fixture := newDispatchFixture(t, database)

	require.NoError(t, fixture.handle(t,
		postV2Frame(dispatchNativeDID, dispatchRev, "3lzpost111111", acceptCommunityDID)))

	require.Equal(t, 1, fixture.engine.Calls(),
		"a community.postv2 commit is handed to the task 16 acceptance engine — "+
			"admission, the acceptance write and the enqueue must ride ONE commit, "+
			"which only the engine can do")
	commits := fixture.engine.Commits()
	assert.Equal(t, CollectionPostV2, commits[0].Collection)
	assert.Equal(t, "3lzpost111111", commits[0].RKey)
	assert.Equal(t, dispatchRev, commits[0].Rev,
		"the engine receives the commit's rev so its own writes stay rev-gated")
	assert.Equal(t, []string{dispatchNativeDID}, fixture.engine.DIDs())

	assert.Zero(t, len(fixture.enqueuer.Calls()),
		"this consumer never enqueues outbound for a post: the engine owns that, "+
			"inside the acceptance commit")
}

func TestDispatcher_UnwantedCollectionIsANoOp(t *testing.T) {
	database := dispatchTestDB(t)
	fixture := newDispatchFixture(t, database)

	require.NoError(t, fixture.handle(t, unwantedFrame(dispatchNativeDID, dispatchRev)),
		"an unsubscribed collection is a SKIP, not a failure: dead-lettering every "+
			"unrelated record would bury the queue in noise")

	assert.Zero(t, fixture.engine.Calls())
	assert.Empty(t, fixture.minter.DIDs())
	assert.Zero(t, countRows(t, database, "federation_prefs"))
	assert.Zero(t, countRows(t, database, "outbound_objects"))
	assert.Zero(t, countRows(t, database, "jetstream_record_revs"),
		"a collection we do not handle must not claim a gate row: the row would "+
			"outlive this event and reject a future one for the same record")
}

func TestDispatcher_RoutesAccountStatusToTheDeliveryPause(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	fixture := newDispatchFixture(t, database)

	// Transient inactivity. Decision 19: deactivated/suspended/takendown are
	// NOT deletion — pausing delivery keeps the identity, and every federated
	// reference to it, intact for when the user comes back.
	require.NoError(t, fixture.handle(t, accountFrameFor(dispatchNativeDID, false, "deactivated")))
	assert.True(t, deliveryPaused(t, database, dispatchNativeDID),
		"a transient inactive status pauses DELIVERY")
	enabled, _ := actorEnabled(t, database, dispatchNativeDID)
	assert.True(t, enabled,
		"pausing is independent of enabled: a paused actor still resolves and still "+
			"serves its document")

	require.NoError(t, fixture.handle(t, accountFrameFor(dispatchNativeDID, true, "active")))
	assert.False(t, deliveryPaused(t, database, dispatchNativeDID),
		"reactivation unpauses")
}

func TestDispatcher_IdentityEventNeverTouchesTheFrozenLocalPart(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	fixture := newDispatchFixture(t, database)

	require.NoError(t, fixture.handle(t, identityFrameFor(dispatchNativeDID, "bob.coves.social")))

	var localPart string
	require.NoError(t, database.QueryRowContext(context.Background(),
		`SELECT local_part FROM ap_actors WHERE did = $1`, dispatchNativeDID).Scan(&localPart))
	assert.Equal(t, "alice", localPart,
		"a handle change refreshes the profile CACHE and nothing else: the local part "+
			"is frozen at creation, and re-deriving it would strand every federated "+
			"mention of the old name")
}

func TestDispatcher_ReplayedCommitReachesNoHandlerSideEffects(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	ctx := context.Background()

	t.Run("federation handler", func(t *testing.T) {
		fixture := newDispatchFixture(t, database)
		frame := federationFrameNoDeleteRemote(dispatchNativeDID, dispatchRev, false)

		require.NoError(t, fixture.handle(t, frame))
		_, err := store.NewFederationPrefs(database).Get(ctx, dispatchNativeDID)
		require.NoError(t, err, "the first delivery applies")

		// Remove the row the handler wrote. If the replay re-creates it, the
		// event reached the handler — which means the gate is not in the path.
		_, err = database.ExecContext(ctx, `DELETE FROM federation_prefs`)
		require.NoError(t, err)

		require.NoError(t, fixture.handle(t, frame),
			"a gate skip is a normal outcome, so the cursor still advances")
		assert.Zero(t, countRows(t, database, "federation_prefs"),
			"the replayed commit carries the same rev, so the gate rejects it BEFORE "+
				"the handler runs — an idempotent handler would hide this, deleting "+
				"the row first is what makes the gate observable")
	})

	t.Run("acceptance engine", func(t *testing.T) {
		fixture := newDispatchFixture(t, database)
		frame := postV2Frame(dispatchNativeDID, dispatchRevHigher, "3lzpost222222", acceptCommunityDID)

		require.NoError(t, fixture.handle(t, frame))
		require.Equal(t, 1, fixture.engine.Calls())

		require.NoError(t, fixture.handle(t, frame))
		assert.Equal(t, 1, fixture.engine.Calls(),
			"replaying a postv2 must not invoke the acceptance engine twice: a second "+
				"admission would write a second acceptance and enqueue a duplicate")
	})
}

// ---------------------------------------------------------------------------
// D2 — the hosted-repo filter
// ---------------------------------------------------------------------------

func TestDispatcher_SkipsTidepoolHostedReposBeforeAnythingElse(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database) // gives acceptCommunityDID a repo_state row
	fixture := newDispatchFixture(t, database)

	// A commit in a repo Tidepool itself writes to. Its outbound was already
	// enqueued at write time; consuming it here would deliver everything twice.
	require.NoError(t, fixture.handle(t,
		federationFrameNoDeleteRemote(acceptCommunityDID, dispatchRev, false)),
		"a filtered event is processed, not failed")

	assert.Zero(t, countRows(t, database, "federation_prefs"),
		"no handler runs for a Tidepool-hosted repo")
	assert.Zero(t, fixture.engine.Calls())
	assert.Empty(t, fixture.minter.DIDs())
	assert.Zero(t, countRows(t, database, "jetstream_record_revs"),
		"and NO gate row is claimed: the filter runs UPSTREAM of the gate, because a "+
			"gate row written here would silently reject the legitimate event that "+
			"later carries the same record URI")

	// Positive control: the same event from a native DID does reach the gate.
	require.NoError(t, fixture.handle(t,
		federationFrameNoDeleteRemote(dispatchNativeDID, dispatchRev, false)))
	assert.Equal(t, 1, countRows(t, database, "federation_prefs"),
		"a DID with no repo_state row is native and proceeds")
	assert.Equal(t, 1, countRows(t, database, "jetstream_record_revs"),
		"and claims its gate row")
}

func TestHostedRepos_CachesPositivesAndRefreshesOnMiss(t *testing.T) {
	ctx := context.Background()

	var mu sync.Mutex
	probes := map[string]int{}
	hostedSet := map[string]bool{"did:plc:hosted": true}
	filter := newHostedReposWithProbe(func(_ context.Context, did string) (bool, error) {
		mu.Lock()
		defer mu.Unlock()
		probes[did]++
		return hostedSet[did], nil
	})

	// A hosted DID is probed once and cached: a repo never stops being hosted,
	// so the answer can never go stale.
	for i := 0; i < 3; i++ {
		hosted, err := filter.IsHosted(ctx, "did:plc:hosted")
		require.NoError(t, err)
		assert.True(t, hosted)
	}
	assert.Equal(t, 1, probes["did:plc:hosted"],
		"a positive answer is cached: three events from one hosted repo cost ONE probe")

	// A native DID is re-probed every time. Caching the negative would be a
	// correctness bug, not an optimization: the instant a community repo is
	// minted its DID becomes hosted, and a stale "not hosted" would
	// double-deliver everything that community writes.
	for i := 0; i < 3; i++ {
		hosted, err := filter.IsHosted(ctx, "did:plc:native")
		require.NoError(t, err)
		assert.False(t, hosted)
	}
	assert.Equal(t, 3, probes["did:plc:native"],
		"a negative answer is NOT cached — the miss re-probes, which is one indexed "+
			"primary-key lookup")

	// And the refresh actually sees the change.
	mu.Lock()
	hostedSet["did:plc:native"] = true
	mu.Unlock()

	hosted, err := filter.IsHosted(ctx, "did:plc:native")
	require.NoError(t, err)
	assert.True(t, hosted,
		"a DID that becomes hosted is filtered from its very next event, with no "+
			"cache invalidation to remember to call")
}

func TestHostedRepos_ProbeErrorIsNotSwallowed(t *testing.T) {
	wantErr := fmt.Errorf("postgres is down")
	filter := newHostedReposWithProbe(func(context.Context, string) (bool, error) {
		return false, wantErr
	})

	_, err := filter.IsHosted(context.Background(), "did:plc:whoever")
	require.ErrorIs(t, err, wantErr,
		"a failed membership probe must surface as a transient error, never as "+
			"'not hosted' — failing open here double-delivers Tidepool's own writes")
}

func TestHostedRepos_ReadsRepoState(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	filter := newHostedRepos(database)
	ctx := context.Background()

	hosted, err := filter.IsHosted(ctx, acceptCommunityDID)
	require.NoError(t, err)
	assert.True(t, hosted,
		"membership is a repo_state PRIMARY KEY lookup: exactly one row per repo "+
			"Tidepool commits into, which is the question being asked")

	hosted, err = filter.IsHosted(ctx, dispatchNativeDID)
	require.NoError(t, err)
	assert.False(t, hosted, "a native user's repo has no repo_state row")
}

// ---------------------------------------------------------------------------
// D3 — ActivityID golden vectors
// ---------------------------------------------------------------------------

// The vectors below were computed OUTSIDE Go (python hashlib) against the
// specified preimage, so they pin the derivation itself rather than agreeing
// with whatever the implementation happens to do:
//
//	origin + "/ap/activity/" + hex(sha256(
//	    "tidepool:activity:v1" + "\n" + atURI + "\n" + op + "\n" + decimal(seq)))
const (
	goldenCommentATURI = "at://did:plc:7iza6de2dwap2sbkpav7c6c6/social.coves.community.comment/3lzcmnt3333bb"
	goldenVoteATURI    = "at://did:plc:7iza6de2dwap2sbkpav7c6c6/social.coves.feed.vote/3lzvoteaaaaaa"
)

func TestActivityID_GoldenVectors(t *testing.T) {
	tests := []struct {
		name   string
		origin string
		atURI  string
		op     string
		seq    int
		want   string
	}{
		{
			name: "comment create at seq 0", origin: acceptUserOrigin,
			atURI: goldenCommentATURI, op: "create", seq: 0,
			want: "https://coves.social/ap/activity/" +
				"b1179a09ab1a57a314719e1bdeb61504ddfaaea11746c45475a1541b69e2ccae",
		},
		{
			name: "the op changes the id", origin: acceptUserOrigin,
			atURI: goldenCommentATURI, op: "update", seq: 1,
			want: "https://coves.social/ap/activity/" +
				"11165ec7ad26180c3db7178e1965c5df66a871b231bbcb077af3038ddfe12136",
		},
		{
			name: "a delete is buildable from at-uri, op and seq alone", origin: acceptUserOrigin,
			atURI: goldenCommentATURI, op: "delete", seq: 2,
			want: "https://coves.social/ap/activity/" +
				"b946f5ab8b24bf253abff930a058950af34cd483efc797b1bd58896ac612a6e8",
		},
		{
			name: "a different record is a different id", origin: acceptUserOrigin,
			atURI: goldenVoteATURI, op: "create", seq: 0,
			want: "https://coves.social/ap/activity/" +
				"1635a05ef4b0d1f97045e61eea3d798bcddcf1f1523bfd7afc158780ef5a71ec",
		},
		{
			// A vanity origin serves the same user's activities under its own
			// host; only the prefix moves, the digest does not.
			name: "the origin is a prefix, not part of the digest", origin: "https://vanity.example",
			atURI: goldenCommentATURI, op: "create", seq: 0,
			want: "https://vanity.example/ap/activity/" +
				"b1179a09ab1a57a314719e1bdeb61504ddfaaea11746c45475a1541b69e2ccae",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, ActivityID(tc.origin, tc.atURI, tc.op, tc.seq),
				"a redelivery must reuse the id a peer has already seen, so this "+
					"derivation is a wire contract, not an implementation detail")
		})
	}
}

func TestActivityID_PreimageIsVersionTaggedAndDelimited(t *testing.T) {
	// These are the digests the SAME inputs produce under the two schemes this
	// one deliberately is not. Asserting inequality pins the version tag and
	// the separators by VECTOR — inspecting the returned string for "v1" would
	// pin nothing, because the tag is hashed, not printed.
	untagged := "https://coves.social/ap/activity/" +
		"ccc83241cf63baf785d0e7034e57c7f168185bfe21d19ad618468d6273d9f3aa"
	naivelyConcatenated := "https://coves.social/ap/activity/" +
		"25c9931ea9b063aaaf2c5ce621580c4b83b8cb00e2c594998c0e7976f253f3d2"

	got := ActivityID(acceptUserOrigin, goldenCommentATURI, "create", 0)

	assert.NotEqual(t, untagged, got,
		"the preimage carries a version tag, so a future derivation change can bump "+
			"the tag instead of silently minting an old id for a new activity")
	assert.NotEqual(t, naivelyConcatenated, got,
		"the preimage is newline-delimited, not concatenated")

	// The separator ambiguity concatenation would create, in the concrete: two
	// genuinely different activities that a concatenating scheme collapses into
	// one id, so the second would be swallowed by a peer that has the first.
	assert.NotEqual(t,
		ActivityID(acceptUserOrigin, goldenCommentATURI, "create", 12),
		ActivityID(acceptUserOrigin, goldenCommentATURI, "create1", 2),
		"(uri, create, 12) and (uri, create1, 2) must not collide")
}

func TestActivityID_IsStableAcrossCalls(t *testing.T) {
	first := ActivityID(acceptUserOrigin, goldenCommentATURI, "create", 0)
	second := ActivityID(acceptUserOrigin, goldenCommentATURI, "create", 0)
	assert.Equal(t, first, second, "the derivation is pure")
	assert.NotEmpty(t, first)
}
