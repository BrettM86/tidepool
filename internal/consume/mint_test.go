package consume

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// Task 14 cycle F2: the mint runs THROUGH resolution, and the comment create
// path completes.
//
// SCOPE PULLED FROM CYCLE H: finishing the outer test needs the comment
// create path end to end (resolve the thread through ap_objects → write
// outbound_objects → enqueue one intent), so it is pinned here. Cycle H still
// owns update, delete, the depth cap, and resolving a parent that is itself a
// comment or a Lemmy object.

func TestCommentMint_UsesTheVerifiedHandle(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)

	fixture := newDispatchFixture(t, database)
	fixture.resolver.handle = "alice.coves.social"

	require.NoError(t, fixture.handle(t,
		commentFrameFor(dispatchNativeDID, dispatchRev, "3lzcmnt7777ff")))

	assert.Equal(t, []string{dispatchNativeDID}, fixture.resolver.Calls(),
		"the first federating interaction resolves the author's handle")
	assert.Equal(t, []string{"alice.coves.social"}, fixture.minter.Handles(),
		"the VERIFIED handle is what reaches CreateActorForDID — the commit carries "+
			"none, and the local part it derives is frozen forever")
}

func TestCommentMint_ResolvesOnlyBeforeTheFirstMint(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)

	fixture := newDispatchFixture(t, database)

	require.NoError(t, fixture.handle(t,
		commentFrameFor(dispatchNativeDID, dispatchRev, "3lzcmnt8888gg")))
	require.Len(t, fixture.resolver.Calls(), 1)

	// A second comment from the same author, now that the actor exists.
	require.NoError(t, fixture.handle(t,
		commentFrameFor(dispatchNativeDID, dispatchRevHigher, "3lzcmnt9999hh")))

	assert.Len(t, fixture.resolver.Calls(), 1,
		"an existing actor short-circuits resolution: the local part is already frozen, "+
			"so re-resolving would put two network round-trips (PLC + well-known) in "+
			"front of EVERY comment for a name that can no longer change")
	assert.Len(t, fixture.minter.Handles(), 1,
		"and the mint is not re-attempted either")
}

func TestCommentMint_ResolverFailureWritesNothing(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)

	fixture := newDispatchFixture(t, database)
	fixture.resolver.err = fmt.Errorf("plc directory unreachable")

	err := fixture.handle(t, commentFrameFor(dispatchNativeDID, dispatchRev, "3lzcmntaaaaii"))

	require.Error(t, err,
		"an unresolvable handle must FAIL the event, not skip it: skipping would drop "+
			"the comment silently, and the connector's retry/DLQ is the recovery path")
	assert.NotErrorIs(t, err, ErrPermanentEvent,
		"a directory outage is transient, so the redriver gets to replay it")

	assert.Empty(t, fixture.minter.Handles(),
		"NO MINT until the handle is verified: minting on a fallback would freeze the "+
			"wrong local part, and freezing is not undoable")
	assert.Zero(t, countRows(t, database, "ap_actors"))
	assert.Zero(t, countRows(t, database, "outbound_objects"),
		"and no outbound state is written for an actor that does not exist")
	assert.Empty(t, fixture.enqueuer.Calls(), "and nothing reaches task 15")
}

func TestCommentMint_ResolverFailureLeavesTheGateUnadvanced(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)

	fixture := newDispatchFixture(t, database)
	fixture.resolver.err = fmt.Errorf("plc directory unreachable")

	require.Error(t, fixture.handle(t,
		commentFrameFor(dispatchNativeDID, dispatchRev, "3lzcmntbbbbjj")))

	assert.Zero(t, countRows(t, database, "jetstream_record_revs"),
		"the failed event must not leave a gate row behind: the redrive replays the "+
			"SAME rev, and a claimed gate would reject the retry that was supposed to "+
			"be the recovery")
}

func TestCommentCreate_WritesOutboundStateAndEnqueuesOneIntent(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)

	fixture := newDispatchFixture(t, database)
	const rkey = "3lzcmntccccKK"
	commentATURI := "at://" + dispatchNativeDID + "/" + CollectionComment + "/" + rkey

	require.NoError(t, fixture.handle(t, commentFrameFor(dispatchNativeDID, dispatchRev, rkey)))

	stored, err := store.NewOutboundObjects(database).GetByATURI(context.Background(), commentATURI)
	require.NoError(t, err,
		"the create must leave state behind: the DELETE commit that follows it one day "+
			"carries no record body and no CID, so this row is the only thing a "+
			"Delete{Note} can be built from")
	require.NotNil(t, stored)

	assert.Equal(t, acceptCommunityDID, stored.CommunityDID,
		"the community comes from the thread root's ap_objects mapping, not from "+
			"anything the comment asserts about itself")
	assert.Equal(t, acceptCommunityAPID, stored.CommunityAPID)
	assert.Equal(t, dispatchRev, stored.LastRev, "provenance: the rev that was applied")
	assert.Equal(t, 0, stored.LastActivitySeq, "a create is activity 0")
	assert.Equal(t, 1, stored.Depth,
		"a direct reply to the thread root is depth 1 (Lemmy caps comments at 50; the "+
			"cap itself is cycle H)")
	assert.False(t, stored.IsTombstoned())

	assert.True(t, strings.HasPrefix(stored.APObjectID, acceptUserOrigin+"/"),
		"the AP id must live on the user origin — it is a URL this bridge has to be "+
			"able to serve. Its PATH is deliberately unpinned; task 15 owns AP "+
			"vocabulary. Got %q", stored.APObjectID)
	assert.Contains(t, string(stored.TranslatedSnapshot), "hi",
		"the snapshot carries the comment's content, because task 17 restores the "+
			"object from it after a tombstone")

	calls := fixture.enqueuer.Calls()
	require.Len(t, calls, 1, "exactly one intent per create")
	call := calls[0]
	assert.Equal(t, dispatchNativeDID, call.ActorDID)
	assert.Equal(t, acceptRootATURI, call.ParentATURI,
		"parentATURI carries the causal dependency: the reply must not be delivered "+
			"before the thing it replies to")

	intent, ok := call.Intent.(CommentIntent)
	require.True(t, ok, "want CommentIntent, got %T", call.Intent)
	assert.Equal(t, "create", intent.Op)
	assert.Equal(t, commentATURI, intent.ATURI)
	assert.Equal(t, acceptCommunityAPID, intent.CommunityAPID)
	assert.Equal(t, acceptRootAPID, intent.ParentAPID,
		"the parent's AP id is resolved through ap_objects so task 15 need not look it up")
	assert.Equal(t, ActivityID(acceptUserOrigin, commentATURI, "create", 0), intent.ActivityID())
}

func TestCommentCreate_UnresolvableParentIsSkippedNotFailed(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	// No thread root seeded: a native thread in a NATIVE community, which this
	// bridge has no business federating.

	fixture := newDispatchFixture(t, database)
	require.NoError(t, fixture.handle(t,
		commentFrameFor(dispatchNativeDID, dispatchRev, "3lzcmntddddll")),
		"an unresolvable parent is a SKIP at debug, not an error: most native comments "+
			"live in native communities, and dead-lettering them all would bury the queue")

	assert.Empty(t, fixture.enqueuer.Calls())
	assert.Zero(t, countRows(t, database, "outbound_objects"))
	assert.Empty(t, fixture.minter.Handles(),
		"and no identity is minted for a comment that was never going to federate")
}
