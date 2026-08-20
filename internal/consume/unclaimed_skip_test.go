package consume

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// Chunk 3 finding 6 (CRITICAL, silent failure): a handler skip INSIDE the rev
// gate commits the claim, and the retry that would fix it then loses at
// `rev < EXCLUDED.rev` and is logged at Debug as a stale replay.
//
// The user-visible loss: a vote or a comment that arrives a few hundred
// milliseconds before the thing it hangs on has materialized is skipped, the
// gate row is written anyway, and the byte-identical redelivery — the cursor
// rewind that exists precisely to recover this — is swallowed. No DLQ row, no
// metric, no log above Debug.
//
// These tests split the skips by whether the answer can CHANGE:
//
//   - TRANSIENT skips must leave NO gate row (errSkipUnclaimed), so the same
//     frame re-enters the handler on redelivery and applies.
//   - PERMANENT skips must still claim, or a redelivery would re-execute a
//     decision that is already final.

// ---------------------------------------------------------------------------
// Transient skips: the gate row must NOT be written
// ---------------------------------------------------------------------------

func TestUnclaimedSkip_VoteBeforeItsSubjectMaterializesAppliesOnRedelivery(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	// Deliberately NO seedThreadRoot: the subject this vote names has not been
	// materialized yet, which is the ordinary few-hundred-millisecond race
	// between a post being bridged and somebody voting on it.
	ctx := context.Background()

	const rkey = "3lzskipvote01"
	voteATURI := voteATURIFor(dispatchNativeDID, rkey)
	frame := voteFrame(dispatchNativeDID, dispatchRev, rkey, acceptRootATURI, "up")

	fixture := newDispatchFixture(t, database)
	require.NoError(t, fixture.handle(t, frame),
		"an unresolved subject is a skip, not a failure: native users vote in native "+
			"communities constantly and dead-lettering that would bury the queue")

	assert.Empty(t, storedRev(t, database, voteATURI),
		"NO gate row may be claimed for an event no handler applied. The claim is what "+
			"turns the redelivery into a silent stale-replay skip, which is how the "+
			"vote is lost")
	assert.Zero(t, countRows(t, database, "outbound_votes"))
	assert.Empty(t, fixture.enqueuer.Calls())

	// The parent materializes, and the cursor rewind redelivers the SAME frame.
	seedThreadRoot(t, database)
	replay := newDispatchFixture(t, database)
	require.NoError(t, replay.handle(t, frame))

	stored, err := store.NewOutboundVotes(database).GetByATURI(ctx, voteATURI)
	require.NoError(t, err,
		"the byte-identical redelivery now applies: nothing was claimed the first time, "+
			"so the gate has nothing to reject it with")
	require.NotNil(t, stored)
	assert.Equal(t, "up", stored.Direction)
	assert.Len(t, replay.enqueuer.Calls(), 1, "and the Like finally federates")
	assert.Equal(t, dispatchRev, storedRev(t, database, voteATURI),
		"and NOW the gate is claimed, so a further replay is the ordinary no-op")
}

func TestUnclaimedSkip_CommentBeforeItsThreadMaterializesAppliesOnRedelivery(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	ctx := context.Background()

	const rkey = "3lzskipcmnt01"
	atURI := commentATURIFor(dispatchNativeDID, rkey)
	frame := commentFrameFull(dispatchNativeDID, dispatchRev, rkey, "create", "hi", acceptRootATURI)

	fixture := newDispatchFixture(t, database)
	require.NoError(t, fixture.handle(t, frame))

	assert.Empty(t, storedRev(t, database, atURI),
		"a comment whose thread does not resolve YET must not claim its gate row")
	assert.Zero(t, countRows(t, database, "outbound_objects"))

	seedThreadRoot(t, database)
	replay := newDispatchFixture(t, database)
	require.NoError(t, replay.handle(t, frame))

	stored, err := store.NewOutboundObjects(database).GetByATURI(ctx, atURI)
	require.NoError(t, err, "the redelivered comment federates once its thread exists")
	require.NotNil(t, stored)
	assert.Len(t, replay.enqueuer.Calls(), 1)
}

func TestUnclaimedSkip_PostV2ForANotYetBridgedCommunityAppliesOnRedelivery(t *testing.T) {
	database := dispatchTestDB(t)
	// No community row: this community is not bridged at the moment the post
	// arrives. Bridging one is an ordinary operator action that happens later.

	const rkey = "3lzskippost01"
	postATURI := "at://" + dispatchNativeDID + "/" + CollectionPostV2 + "/" + rkey
	frame := postV2Frame(dispatchNativeDID, dispatchRev, rkey, acceptCommunityDID)

	fixture := newDispatchFixture(t, database)
	require.NoError(t, fixture.handle(t, frame))
	assert.Zero(t, fixture.engine.Calls())
	assert.Empty(t, storedRev(t, database, postATURI),
		"a post for a community this bridge does not federate YET must not claim its "+
			"gate row: the community being bridged tomorrow is the whole recovery path")

	seedBridgedCommunity(t, database)
	replay := newDispatchFixture(t, database)
	require.NoError(t, replay.handle(t, frame))
	assert.Equal(t, 1, replay.engine.Calls(),
		"once the community is bridged, the redelivered post reaches the acceptance engine")
}

func TestUnclaimedSkip_UnrecognisedVoteDirectionStaysReplayable(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)

	const rkey = "3lzskipdir001"
	voteATURI := voteATURIFor(dispatchNativeDID, rkey)

	fixture := newDispatchFixture(t, database)
	require.NoError(t, fixture.handle(t,
		voteFrame(dispatchNativeDID, dispatchRev, rkey, acceptRootATURI, "sideways")))

	assert.Zero(t, countRows(t, database, "outbound_votes"),
		"nothing is stored: guessing a direction would push a vote the user never cast")
	assert.Empty(t, storedRev(t, database, voteATURI),
		"direction is an OPEN enum, so the record is forward-compatible rather than "+
			"malformed and a LATER BUILD is its recovery path. Claiming the gate here "+
			"would make the from-scratch replay that build depends on a silent no-op")
}

// ---------------------------------------------------------------------------
// Permanent skips: the claim still commits
// ---------------------------------------------------------------------------

func TestClaimedSkip_AnOptedOutAuthorsVoteStillClaimsTheGate(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)
	ctx := context.Background()

	_, err := store.NewFederationPrefs(database).Upsert(ctx, store.FederationPref{
		DID:    dispatchNativeDID,
		Source: store.FederationPrefSourceRecord,
	})
	require.NoError(t, err)

	const rkey = "3lzskipout001"
	voteATURI := voteATURIFor(dispatchNativeDID, rkey)
	frame := voteFrame(dispatchNativeDID, dispatchRev, rkey, acceptRootATURI, "up")

	fixture := newDispatchFixture(t, database)
	require.NoError(t, fixture.handle(t, frame))
	assert.Equal(t, dispatchRev, storedRev(t, database, voteATURI),
		"PERMANENT: the author said no. The decision cannot become untrue for THIS "+
			"record, so the claim commits and the replay is rejected at the gate")

	// Even after the preference is gone, the identical frame is a gate skip: a
	// re-enable federates what the user writes NEXT, not what they wrote while
	// opted out.
	require.NoError(t, store.NewFederationPrefs(database).Delete(ctx, dispatchNativeDID))
	replay := newDispatchFixture(t, database)
	require.NoError(t, replay.handle(t, frame))
	assert.Zero(t, countRows(t, database, "outbound_votes"))
	assert.Empty(t, replay.enqueuer.Calls(),
		"the replay loses at `rev < EXCLUDED.rev` — which is the gate working")
}

func TestClaimedSkip_AVoteDeleteWithNoStateKeepsItsTombstone(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)

	const rkey = "3lzskipdel001"
	voteATURI := voteATURIFor(dispatchNativeDID, rkey)

	// The delete arrives first (a rewind can reorder nothing, but a delete for a
	// vote cast before this bridge existed looks exactly like this).
	fixture := newDispatchFixture(t, database)
	require.NoError(t, fixture.handle(t,
		voteDeleteFrame(dispatchNativeDID, dispatchRevHigher, rkey)))
	assert.Equal(t, dispatchRevHigher, storedRev(t, database, voteATURI),
		"PERMANENT, and load-bearing: the gate row a delete leaves IS the tombstone "+
			"that rejects a stale create. Releasing it unclaimed would let a replay "+
			"past the delete resurrect the vote")

	require.NoError(t, fixture.handle(t,
		voteFrame(dispatchNativeDID, dispatchRev, rkey, acceptRootATURI, "up")))
	assert.Zero(t, countRows(t, database, "outbound_votes"),
		"and the stale create loses at the tombstone rather than re-casting the vote")
}

// ---------------------------------------------------------------------------
// The counter
// ---------------------------------------------------------------------------

func TestUnclaimedSkip_IsCounted(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)

	before := unclaimedSkips.Value()

	fixture := newDispatchFixture(t, database)
	require.NoError(t, fixture.handle(t,
		voteFrame(dispatchNativeDID, dispatchRev, "3lzskipcnt01", acceptRootATURI, "up")))

	assert.Equal(t, before+1, unclaimedSkips.Value(),
		"a transient skip is the one outcome with NO other trace — no DLQ row, no "+
			"failed event — so the counter is the only thing that makes a stuck "+
			"backlog of them visible")
}
