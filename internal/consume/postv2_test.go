package consume

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// Task 14 cycle G: community.postv2 → the task 16 acceptance engine.
//
// This consumer owns none of a post. Admission, the acceptance record in the
// community's repo, and the outbound enqueue must all ride ONE commit, which
// only the engine can do — so the handler's entire job is deciding WHICH
// events reach the seam, and writing nothing itself.

func postV2DeleteFrame(did, rev, rkey string) []byte {
	return []byte(fmt.Sprintf(
		`{"did":%q,"time_us":8100,"kind":"commit","commit":{"rev":%q,"operation":"delete",`+
			`"collection":"social.coves.community.postv2","rkey":%q}}`,
		did, rev, rkey))
}

func postV2FrameNoCommunity(did, rev, rkey string) []byte {
	return []byte(fmt.Sprintf(
		`{"did":%q,"time_us":8200,"kind":"commit","commit":{"rev":%q,"operation":"create",`+
			`"collection":"social.coves.community.postv2","rkey":%q,`+
			`"cid":"bafyreievgu2ty7qbiaaom5zhmkznsnajuzideek3lo7e65dwqlrvrxnmo4",`+
			`"record":{"$type":"social.coves.community.postv2","title":"orphan",`+
			`"createdAt":"2026-08-13T10:00:00.000Z"}}}`,
		did, rev, rkey))
}

// ---------------------------------------------------------------------------
// G1 — gating on the target community
// ---------------------------------------------------------------------------

// The happy path and the replay proof (same rev → the engine is invoked once,
// not twice) are pinned in
// TestDispatcher_RoutesPostV2ToTheAcceptanceEngine and
// TestDispatcher_ReplayedCommitReachesNoHandlerSideEffects/acceptance_engine.
// What follows is the gating those two do not cover.

func TestPostV2_NonBridgedCommunityNeverReachesTheEngine(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database) // a DIFFERENT community is bridged
	fixture := newDispatchFixture(t, database)

	// A post to a community Tidepool does not federate. Most Coves posts are
	// exactly this.
	require.NoError(t, fixture.handle(t, postV2Frame(dispatchNativeDID, dispatchRev,
		"3lzpostaaa111", "did:plc:someothercommunity000")),
		"a post to a non-bridged community is a SKIP at debug, not a failure: it is the "+
			"common case, and dead-lettering it would bury the queue")

	assert.Zero(t, fixture.engine.Calls(),
		"the acceptance engine must not be asked to admit a post into a community this "+
			"bridge does not federate — admission would write an acceptance record into "+
			"a repo that has no business existing")
}

func TestPostV2_BridgedCommunityIsDecidedByTheCommunitiesTable(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	fixture := newDispatchFixture(t, database)

	require.NoError(t, fixture.handle(t,
		postV2Frame(dispatchNativeDID, dispatchRev, "3lzpostbbb222", acceptCommunityDID)))

	require.Equal(t, 1, fixture.engine.Calls(),
		"a post whose community has a communities row reaches the engine")
	assert.Equal(t, acceptCommunityDID,
		fixture.engine.Commits()[0].Record["community"],
		"the engine receives the community the record names, unaltered")
}

func TestPostV2_MissingCommunityIsPermanent(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	fixture := newDispatchFixture(t, database)

	err := fixture.handle(t, postV2FrameNoCommunity(dispatchNativeDID, dispatchRev, "3lzpostccc333"))

	require.Error(t, err,
		"the lexicon REQUIRES community; a postv2 without one is malformed and belongs "+
			"in the DLQ where a lexicon rollout mistake stays visible")
	assert.ErrorIs(t, err, ErrPermanentEvent,
		"no amount of retrying puts a community field into a record that has none")
	assert.Zero(t, fixture.engine.Calls())
}

func TestPostV2_NilEngineSkipsWithoutFailing(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	// A deployment where task 16 has not landed yet.
	fixture := newDispatchFixture(t, database, func(opts *Options) { opts.Engine = nil })

	require.NoError(t, fixture.handle(t,
		postV2Frame(dispatchNativeDID, dispatchRev, "3lzpostddd444", acceptCommunityDID)),
		"a missing engine must not panic and must not fail the event")

	assert.Zero(t, countRows(t, database, "outbound_objects"))
	assert.Empty(t, fixture.enqueuer.Calls())
}

// ---------------------------------------------------------------------------
// G2 — the immutability fence
// ---------------------------------------------------------------------------

// The lexicon makes a postv2's `community` immutable, and an update that
// changes it must be discarded WHOLE — consumers may retain neither half.
//
// Enforcement belongs to the ACCEPTANCE ENGINE (task 16), not here. The engine
// is the only party that knows a post's prior community: it wrote the
// acceptance record into that community's repo, so the previous value is its
// own state. This consumer has no memory of the create at all — it keeps no
// row for a postv2, by design (see the fence below).
//
// See the cycle G report for the one consumer-side memory that WILL exist once
// task 16 lands, and why it is worth revisiting then.
func TestPostV2_HandlerRetainsNoStateOfItsOwn(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	fixture := newDispatchFixture(t, database)

	require.NoError(t, fixture.handle(t,
		postV2Frame(dispatchNativeDID, dispatchRev, "3lzposteee555", acceptCommunityDID)))
	require.Equal(t, 1, fixture.engine.Calls())

	// An update that MOVES the post to another community — the hijack the
	// immutability rule exists to stop.
	require.NoError(t, fixture.handle(t, postV2Frame(dispatchNativeDID, dispatchRevHigher,
		"3lzposteee555", acceptCommunityDID)))

	assert.Zero(t, countRows(t, database, "outbound_objects"),
		"the consumer writes NO outbound state for a post. That is what makes the "+
			"immutability rule enforceable in one place: there is no consumer-side row "+
			"for a community change to corrupt, and no second copy of the answer for "+
			"the engine's to disagree with")
	assert.Empty(t, fixture.enqueuer.Calls(),
		"and it enqueues nothing: the post's outbound rides the engine's acceptance "+
			"commit, so an enqueue here would be a duplicate delivery")
}

// ---------------------------------------------------------------------------
// G3 — deletes
// ---------------------------------------------------------------------------

func TestPostV2_DeleteReachesTheEngine(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	fixture := newDispatchFixture(t, database)

	require.NoError(t, fixture.handle(t,
		postV2Frame(dispatchNativeDID, dispatchRev, "3lzpostfff666", acceptCommunityDID)))
	require.Equal(t, 1, fixture.engine.Calls())

	require.NoError(t, fixture.handle(t,
		postV2DeleteFrame(dispatchNativeDID, dispatchRevHigher, "3lzpostfff666")))

	require.Equal(t, 2, fixture.engine.Calls(),
		"an author deleting their post must reach the engine too: the acceptance record "+
			"in the community repo has to come down with it, and only the engine can "+
			"take it down")

	deleted := fixture.engine.Commits()[1]
	assert.Equal(t, "delete", deleted.Operation,
		"the seam already carries the operation, so a delete needs no separate method")
	assert.Equal(t, "3lzpostfff666", deleted.RKey)
	assert.Nil(t, deleted.Record,
		"a delete commit carries NO record body — the engine identifies the post by "+
			"repo + collection + rkey, exactly as this consumer does")
}

func TestPostV2_DeleteIsNotGatedOnACommunityItCannotSee(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	fixture := newDispatchFixture(t, database)

	// No create was ever seen, and the delete frame carries no record — so
	// there is no community field to check against the communities table.
	require.NoError(t, fixture.handle(t,
		postV2DeleteFrame(dispatchNativeDID, dispatchRev, "3lzpostggg777")))

	assert.Equal(t, 1, fixture.engine.Calls(),
		"the bridged-community gate applies to records that HAVE a community field. A "+
			"delete has none, so gating it would silently drop every author delete and "+
			"strand the acceptance records they were supposed to remove; the engine "+
			"already knows which posts it accepted and can no-op the rest")
}

// ---------------------------------------------------------------------------
// Second-opinion C3: the opt-out gate applies to postv2 too
// ---------------------------------------------------------------------------
//
// A postv2 create/update pushes the author's content OUTWARD (into the
// community's repo, via the acceptance engine), so an opted-out author's post
// must not be admitted — the same gate comments and votes already carry. A
// delete is a retraction and stays ungated, for the same reason it does on the
// comment path: removing content is always safe, and it is the only way an
// opted-out user can take down what is already federated.

func TestPostV2_OptedOutAuthorCreateNeverReachesTheEngine(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	ctx := context.Background()

	_, err := store.NewFederationPrefs(database).Upsert(ctx, store.FederationPref{
		DID:    dispatchNativeDID,
		Source: store.FederationPrefSourceRecord,
	})
	require.NoError(t, err)

	fixture := newDispatchFixture(t, database)
	require.NoError(t, fixture.handle(t,
		postV2Frame(dispatchNativeDID, dispatchRev, "3lzpostopt01", acceptCommunityDID)))

	assert.Zero(t, fixture.engine.Calls(),
		"an opted-out author's post must not be admitted: admission writes an "+
			"acceptance record and federates the post, which is exactly the outward "+
			"push the opt-out forbids")
}

func TestPostV2_OptedOutAuthorUpdateNeverReachesTheEngine(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	ctx := context.Background()

	_, err := store.NewFederationPrefs(database).Upsert(ctx, store.FederationPref{
		DID:    dispatchNativeDID,
		Source: store.FederationPrefSourceRecord,
	})
	require.NoError(t, err)

	fixture := newDispatchFixture(t, database)
	frame := []byte(fmt.Sprintf(
		`{"did":%q,"time_us":8300,"kind":"commit","commit":{"rev":%q,"operation":"update",`+
			`"collection":"social.coves.community.postv2","rkey":"3lzpostopt02",`+
			`"cid":"bafyreievgu2ty7qbiaaom5zhmkznsnajuzideek3lo7e65dwqlrvrxnmo4",`+
			`"record":{"$type":"social.coves.community.postv2","community":%q,`+
			`"title":"edited","createdAt":"2026-08-13T10:00:00.000Z"}}}`,
		dispatchNativeDID, dispatchRev, acceptCommunityDID))
	require.NoError(t, fixture.handle(t, frame))

	assert.Zero(t, fixture.engine.Calls(),
		"an edit is still an outward push, so it is gated exactly like a create")
}

func TestPostV2_OptedOutAuthorDeleteStillReachesTheEngine(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	ctx := context.Background()

	_, err := store.NewFederationPrefs(database).Upsert(ctx, store.FederationPref{
		DID:    dispatchNativeDID,
		Source: store.FederationPrefSourceRecord,
	})
	require.NoError(t, err)

	fixture := newDispatchFixture(t, database)
	require.NoError(t, fixture.handle(t,
		postV2DeleteFrame(dispatchNativeDID, dispatchRev, "3lzpostopt03")))

	assert.Equal(t, 1, fixture.engine.Calls(),
		"a delete is a retraction and stays ungated: the acceptance record has to come "+
			"down even for an author who has since opted out, or their post stands on "+
			"the fediverse forever — the same asymmetry comments and votes carry")
}

// ---------------------------------------------------------------------------
// Second-opinion C7: a nil-engine skip must not claim a gate row
// ---------------------------------------------------------------------------

func TestPostV2_NilEngineSkipLeavesNoGateRow(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	// A deployment where the acceptance engine (task 16) is not wired yet.
	fixture := newDispatchFixture(t, database, func(opts *Options) { opts.Engine = nil })

	require.NoError(t, fixture.handle(t,
		postV2Frame(dispatchNativeDID, dispatchRev, "3lzpostnil01", acceptCommunityDID)))

	assert.Zero(t, countRows(t, database, "jetstream_record_revs"),
		"a postv2 skipped because the engine is nil must NOT advance the gate: the "+
			"whole point of leaving it unhandled is that a later build WITH the engine "+
			"replays and admits it. A gate row here would make that replay a no-op, "+
			"silently dropping the post forever")
}
