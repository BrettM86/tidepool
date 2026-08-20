package consume

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// Task 14 cycle H: the rest of the comment path — update, delete, the Lemmy
// depth cap, and the second place a parent can live.
//
// The delete case is the one the whole outbound_objects table exists for. A
// Jetstream delete commit carries the repo DID, the collection and the rkey
// and NOTHING else: no record body, no CID, no reply refs. Every fact the
// Delete activity needs has to be read back out of state written at create
// time, and this file is where that is proven.

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// commentFrameFull builds a comment commit of any operation. A delete carries
// no record at all, which is the point.
func commentFrameFull(did, rev, rkey, operation, content, parentATURI string) []byte {
	if operation == "delete" {
		return []byte(fmt.Sprintf(
			`{"did":%q,"time_us":9100,"kind":"commit","commit":{"rev":%q,"operation":"delete",`+
				`"collection":"social.coves.community.comment","rkey":%q}}`,
			did, rev, rkey))
	}
	return []byte(fmt.Sprintf(
		`{"did":%q,"time_us":9100,"kind":"commit","commit":{"rev":%q,"operation":%q,`+
			`"collection":"social.coves.community.comment","rkey":%q,`+
			`"cid":"bafyreievgu2ty7qbiaaom5zhmkznsnajuzideek3lo7e65dwqlrvrxnmo4",`+
			`"record":{"$type":"social.coves.community.comment","reply":{`+
			`"root":{"uri":%q,"cid":%q},"parent":{"uri":%q,"cid":%q}},`+
			`"content":%q,"createdAt":"2026-08-13T10:00:00.000Z"}}}`,
		did, rev, operation, rkey, acceptRootATURI, acceptRootCID,
		parentATURI, acceptRootCID, content))
}

func commentATURIFor(did, rkey string) string {
	return "at://" + did + "/" + CollectionComment + "/" + rkey
}

// seedOutboundParent writes the outbound_objects row the ACCEPTANCE ENGINE (a
// native postv2 root) or an earlier native comment would have left behind.
// Nothing maps these into ap_objects — they were never materialized from the
// fediverse — so this row is the only record that they federate at all.
func seedOutboundParent(t *testing.T, database *sql.DB, atURI string, depth int) *store.OutboundObject {
	t.Helper()
	stored, err := store.NewOutboundObjects(database).Upsert(context.Background(), store.OutboundObject{
		ATURI:              atURI,
		APObjectID:         acceptUserOrigin + "/ap/object/" + atURI,
		CommunityDID:       acceptCommunityDID,
		CommunityAPID:      acceptCommunityAPID,
		TranslatedSnapshot: []byte(`{"seeded":"parent"}`),
		Depth:              depth,
	})
	require.NoError(t, err, "seed outbound parent %s", atURI)
	require.NotNil(t, stored)
	return stored
}

// createComment runs the create half so update/delete tests start from real
// state rather than a hand-written row.
func createComment(t *testing.T, fixture *dispatchFixture, rkey, content string) {
	t.Helper()
	require.NoError(t, fixture.handle(t, commentFrameFull(
		dispatchNativeDID, dispatchRev, rkey, "create", content, acceptRootATURI)))
}

// ---------------------------------------------------------------------------
// H1 — update
// ---------------------------------------------------------------------------

func TestCommentUpdate_BumpsSeqAndReplacesTheSnapshot(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)
	fixture := newDispatchFixture(t, database)

	const rkey = "3lzcmntupd001"
	atURI := commentATURIFor(dispatchNativeDID, rkey)

	createComment(t, fixture, rkey, "first draft")
	created, err := store.NewOutboundObjects(database).GetByATURI(context.Background(), atURI)
	require.NoError(t, err)
	require.Equal(t, 0, created.LastActivitySeq)

	require.NoError(t, fixture.handle(t, commentFrameFull(
		dispatchNativeDID, dispatchRevHigher, rkey, "update", "edited text", acceptRootATURI)))

	updated, err := store.NewOutboundObjects(database).GetByATURI(context.Background(), atURI)
	require.NoError(t, err)
	require.NotNil(t, updated)

	assert.Equal(t, 1, updated.LastActivitySeq,
		"an applied update is a SECOND activity: reusing the create's seq would reuse "+
			"its id, and a peer that already has that id would drop the edit")
	assert.Contains(t, string(updated.TranslatedSnapshot), "edited text",
		"the snapshot is replaced, because it is what a later Delete is rebuilt from")
	assert.NotContains(t, string(updated.TranslatedSnapshot), "first draft")
	assert.Equal(t, dispatchRevHigher, updated.LastRev)

	assert.Equal(t, created.CommunityDID, updated.CommunityDID,
		"an edit never moves a comment between communities")
	assert.Equal(t, created.CommunityAPID, updated.CommunityAPID)
	assert.Equal(t, created.Depth, updated.Depth, "nor changes where it sits in the thread")
	assert.Nil(t, updated.TombstonedAt)

	calls := fixture.enqueuer.Calls()
	require.Len(t, calls, 2, "one intent for the create, one for the update")
	intent, ok := calls[1].Intent.(CommentIntent)
	require.True(t, ok, "want CommentIntent, got %T", calls[1].Intent)
	assert.Equal(t, "update", intent.Op)
	assert.Equal(t, atURI, intent.ATURI)
	assert.Equal(t, ActivityID(acceptUserOrigin, atURI, "update", 1), intent.ActivityID(),
		"the id is derived from the op and the BUMPED seq")
	assert.NotEqual(t, calls[0].Intent.ActivityID(), intent.ActivityID(),
		"and is therefore distinct from the create's")
}

func TestCommentUpdate_OfAnUnknownCommentIsSkipped(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)
	fixture := newDispatchFixture(t, database)

	// An edit whose create was never federated — the author opted out at the
	// time, or the create predates the bridge.
	require.NoError(t, fixture.handle(t, commentFrameFull(
		dispatchNativeDID, dispatchRev, "3lzcmntupd002", "update", "edited", acceptRootATURI)),
		"an update with no prior state is not an error")
}

// ---------------------------------------------------------------------------
// H2 — delete, rebuilt entirely from state
// ---------------------------------------------------------------------------

func TestCommentDelete_IsBuiltFromStateBecauseTheFrameCarriesNothing(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)
	fixture := newDispatchFixture(t, database)

	const rkey = "3lzcmntdel001"
	atURI := commentATURIFor(dispatchNativeDID, rkey)

	createComment(t, fixture, rkey, "goodbye cruel world")
	require.Len(t, fixture.enqueuer.Calls(), 1)

	// The delete frame has no record, no CID, no reply refs. Everything the
	// Delete activity needs must come out of outbound_objects.
	require.NoError(t, fixture.handle(t, commentFrameFull(
		dispatchNativeDID, dispatchRevHigher, rkey, "delete", "", "")))

	dead, err := store.NewOutboundObjects(database).GetByATURI(context.Background(), atURI)
	require.NoError(t, err, "the row SURVIVES the delete — it is what a replayed create "+
		"is rejected against")
	require.NotNil(t, dead)
	require.NotNil(t, dead.TombstonedAt, "tombstoned_at is stamped")
	assert.Equal(t, 1, dead.LastActivitySeq, "the Delete is the next activity")

	calls := fixture.enqueuer.Calls()
	require.Len(t, calls, 2, "exactly one delete intent")
	call := calls[1]

	assert.Equal(t, dispatchNativeDID, call.ActorDID)
	assert.Equal(t, acceptRootATURI, call.ParentATURI,
		"the parent at-uri comes from STATE: the delete frame does not carry reply refs, "+
			"so without it the causal ordering task 15 needs would be lost")

	intent, ok := call.Intent.(CommentIntent)
	require.True(t, ok, "want CommentIntent, got %T", call.Intent)
	assert.Equal(t, "delete", intent.Op)
	assert.Equal(t, atURI, intent.ATURI)
	assert.Equal(t, acceptCommunityAPID, intent.CommunityAPID,
		"the community the Delete is addressed to comes from state")
	assert.Equal(t, acceptRootAPID, intent.ParentAPID)
	assert.Equal(t, ActivityID(acceptUserOrigin, atURI, "delete", 1), intent.ActivityID(),
		"the id derives from the seq the tombstone bumped")
	assert.Contains(t, string(intent.Snapshot), "goodbye cruel world",
		"and the snapshot travels with it, because task 15 renders the Delete from the "+
			"object it is deleting")
}

func TestCommentDelete_IsIdempotent(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)
	fixture := newDispatchFixture(t, database)

	const rkey = "3lzcmntdel002"
	atURI := commentATURIFor(dispatchNativeDID, rkey)

	createComment(t, fixture, rkey, "bye")
	require.NoError(t, fixture.handle(t, commentFrameFull(
		dispatchNativeDID, dispatchRevHigher, rkey, "delete", "", "")))

	first, err := store.NewOutboundObjects(database).GetByATURI(context.Background(), atURI)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.NotNil(t, first.TombstonedAt, "the first delete must tombstone the row")
	firstCalls := fixture.enqueuer.Calls()
	require.Len(t, firstCalls, 2, "create then delete")
	firstIntent := firstCalls[1].Intent.ActivityID()

	// A second delete with a HIGHER rev clears the gate and reaches the
	// handler — a redelivery Jetstream is entitled to make.
	require.NoError(t, fixture.handle(t, commentFrameFull(
		dispatchNativeDID, "3lzrev0000003", rkey, "delete", "", "")))

	second, err := store.NewOutboundObjects(database).GetByATURI(context.Background(), atURI)
	require.NoError(t, err)
	require.NotNil(t, second)
	require.NotNil(t, second.TombstonedAt)
	assert.Equal(t, *first.TombstonedAt, *second.TombstonedAt,
		"the original tombstone time is preserved")
	assert.Equal(t, first.LastActivitySeq, second.LastActivitySeq,
		"and the seq does NOT bump again")

	lastCalls := fixture.enqueuer.Calls()
	require.NotEmpty(t, lastCalls)
	last := lastCalls[len(lastCalls)-1]
	assert.Equal(t, firstIntent, last.Intent.ActivityID(),
		"so a redelivered delete reuses the id the first one sent, and the peer "+
			"recognises it as the same activity instead of processing it twice")
}

func TestCommentDelete_OfAnUnknownCommentIsSkipped(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)
	fixture := newDispatchFixture(t, database)

	// A comment this bridge never federated — a native thread, an opted-out
	// author, or content older than the bridge.
	require.NoError(t, fixture.handle(t, commentFrameFull(
		dispatchNativeDID, dispatchRev, "3lzcmntdel003", "delete", "", "")),
		"a delete with no state is a skip: there is nothing to withdraw, and most "+
			"native comment deletes are exactly this")

	assert.Empty(t, fixture.enqueuer.Calls(),
		"nothing may be enqueued for an object no peer was ever told about")
}

// ---------------------------------------------------------------------------
// H3 — the Lemmy depth cap
// ---------------------------------------------------------------------------

func TestCommentDepth_CountsFromTheParentsRecordedDepth(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	fixture := newDispatchFixture(t, database)

	// A reply to a comment that is itself five deep.
	parentATURI := commentATURIFor(acceptRootAuthorDID, "3lzcmntpar005")
	seedOutboundParent(t, database, parentATURI, 5)

	const rkey = "3lzcmntdep001"
	require.NoError(t, fixture.handle(t, commentFrameFull(
		dispatchNativeDID, dispatchRev, rkey, "create", "deep reply", parentATURI)))

	stored, err := store.NewOutboundObjects(database).GetByATURI(
		context.Background(), commentATURIFor(dispatchNativeDID, rkey))
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, 6, stored.Depth,
		"depth is the parent's plus one, read from the parent's own recorded depth "+
			"rather than by walking the thread on every comment")
}

func TestCommentDepth_AtTheCapStillFederates(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	fixture := newDispatchFixture(t, database)

	parentATURI := commentATURIFor(acceptRootAuthorDID, "3lzcmntpar049")
	seedOutboundParent(t, database, parentATURI, 49)

	const rkey = "3lzcmntdep050"
	require.NoError(t, fixture.handle(t, commentFrameFull(
		dispatchNativeDID, dispatchRev, rkey, "create", "at the limit", parentATURI)),
		"depth 50 is the last one Lemmy accepts, so it must federate")

	stored, err := store.NewOutboundObjects(database).GetByATURI(
		context.Background(), commentATURIFor(dispatchNativeDID, rkey))
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, 50, stored.Depth)
	assert.Len(t, fixture.enqueuer.Calls(), 1)
}

func TestCommentDepth_BeyondTheCapDeadLettersWithANamedReason(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	fixture := newDispatchFixture(t, database)

	parentATURI := commentATURIFor(acceptRootAuthorDID, "3lzcmntpar050")
	seedOutboundParent(t, database, parentATURI, 50)

	const rkey = "3lzcmntdep051"
	err := fixture.handle(t, commentFrameFull(
		dispatchNativeDID, dispatchRev, rkey, "create", "too deep", parentATURI))

	require.Error(t, err,
		"Lemmy caps comment depth at 50. A deeper comment must be VISIBLE — the DLQ — "+
			"rather than dropped at debug like an ordinary skip, because it is a real "+
			"comment a real user wrote that will never appear")
	assert.Contains(t, err.Error(), "depth",
		"the reason must name depth: the connector stores err.Error() as the dead "+
			"letter's last_error, and that string is the only thing an operator "+
			"triaging the queue has to go on")
	assert.ErrorIs(t, err, ErrPermanentEvent,
		"a comment cannot become shallower, so retrying it ten times only delays the "+
			"same answer")

	assert.Equal(t, 1, countRows(t, database, "outbound_objects"),
		"and no state is written for it — the seeded parent stays the only row")
	assert.Empty(t, fixture.enqueuer.Calls())
}

// ---------------------------------------------------------------------------
// H4 — the two places a parent can live
// ---------------------------------------------------------------------------

func TestCommentParent_ResolvesFromOutboundStateWhenNotMapped(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	fixture := newDispatchFixture(t, database)

	// A NATIVE postv2 root the acceptance engine admitted. It was never
	// materialized from the fediverse, so it has no ap_objects mapping at all
	// — its outbound_objects row is the only evidence it federates.
	rootATURI := "at://" + acceptRootAuthorDID + "/" + CollectionPostV2 + "/3lznativert01"
	parent := seedOutboundParent(t, database, rootATURI, 0)
	require.Zero(t, countRows(t, database, "ap_objects"),
		"the fixture deliberately maps nothing")

	const rkey = "3lzcmntsrc001"
	require.NoError(t, fixture.handle(t, commentFrameFull(
		dispatchNativeDID, dispatchRev, rkey, "create", "reply to a native post", rootATURI)))

	stored, err := store.NewOutboundObjects(database).GetByATURI(
		context.Background(), commentATURIFor(dispatchNativeDID, rkey))
	require.NoError(t, err,
		"a reply to a native post must federate: those posts ARE the bridged content "+
			"the engine just accepted, and dropping their replies would leave every "+
			"native thread half-bridged")
	require.NotNil(t, stored)

	assert.Equal(t, acceptCommunityDID, stored.CommunityDID,
		"the community comes from the parent's outbound state, the same answer the "+
			"ap_objects path gives")
	assert.Equal(t, acceptCommunityAPID, stored.CommunityAPID)
	assert.Equal(t, 1, stored.Depth)

	calls := fixture.enqueuer.Calls()
	require.Len(t, calls, 1)
	assert.Equal(t, rootATURI, calls[0].ParentATURI)
	intent, ok := calls[0].Intent.(CommentIntent)
	require.True(t, ok)
	assert.Equal(t, parent.APObjectID, intent.ParentAPID,
		"and the parent's AP id comes from the row the engine wrote")
}

// ---------------------------------------------------------------------------
// H5 — opting out does not trap content
// ---------------------------------------------------------------------------

func TestCommentDelete_ProcessesEvenForAnOptedOutAuthor(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)
	fixture := newDispatchFixture(t, database)
	ctx := context.Background()

	const rkey = "3lzcmntopt001"
	atURI := commentATURIFor(dispatchNativeDID, rkey)
	createComment(t, fixture, rkey, "already federated")
	require.Len(t, fixture.enqueuer.Calls(), 1)

	// The author opts out AFTER the comment is already out on the fediverse.
	_, err := store.NewFederationPrefs(database).Upsert(ctx, store.FederationPref{
		DID:    dispatchNativeDID,
		Source: store.FederationPrefSourceRecord,
	})
	require.NoError(t, err)

	require.NoError(t, fixture.handle(t, commentFrameFull(
		dispatchNativeDID, dispatchRevHigher, rkey, "delete", "", "")))

	dead, err := store.NewOutboundObjects(database).GetByATURI(ctx, atURI)
	require.NoError(t, err)
	require.NotNil(t, dead)
	require.NotNil(t, dead.TombstonedAt)

	calls := fixture.enqueuer.Calls()
	require.Len(t, calls, 2,
		"the opt-out gate must NOT block a delete. Blocking it would leave the peer's "+
			"copy standing forever — the exact opposite of what a user asking to stop "+
			"federating means. A delete only ever REMOVES content, so it is always "+
			"safe, and it is the only way an opted-out user can retract what is "+
			"already out there")
	intent, ok := calls[1].Intent.(CommentIntent)
	require.True(t, ok)
	assert.Equal(t, "delete", intent.Op)
}

func TestCommentUpdate_IsBlockedForAnOptedOutAuthor(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)
	fixture := newDispatchFixture(t, database)
	ctx := context.Background()

	const rkey = "3lzcmntopt002"
	atURI := commentATURIFor(dispatchNativeDID, rkey)
	createComment(t, fixture, rkey, "original")

	_, err := store.NewFederationPrefs(database).Upsert(ctx, store.FederationPref{
		DID:    dispatchNativeDID,
		Source: store.FederationPrefSourceRecord,
	})
	require.NoError(t, err)

	require.NoError(t, fixture.handle(t, commentFrameFull(
		dispatchNativeDID, dispatchRevHigher, rkey, "update", "new text", acceptRootATURI)))

	assert.Len(t, fixture.enqueuer.Calls(), 1,
		"an UPDATE pushes new content outward, so the opt-out gate does block it — the "+
			"asymmetry with delete is the whole point: stop sending, but never trap "+
			"what is already sent")

	stored, err := store.NewOutboundObjects(database).GetByATURI(ctx, atURI)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.NotContains(t, string(stored.TranslatedSnapshot), "new text",
		"and the state keeps the last version that actually federated")
}
