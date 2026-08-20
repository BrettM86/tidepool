package consume

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// Task 14 cycle I: social.coves.feed.vote.
//
// A vote DELETE commit names the vote record and nothing else — not the
// subject, not the direction, not the id the Like went out under. But an Undo
// has to EMBED the activity it withdraws. That gap is the reason
// outbound_votes exists, and it is why the row is written before the intent
// and outlives the record it describes.

func voteFrame(did, rev, rkey, subjectATURI, direction string) []byte {
	return voteWriteFrame(did, rev, rkey, subjectATURI, direction, "create")
}

// voteRecastFrame is a vote FLIPPED in place. An up→down change is an edit of
// the existing record, not a delete plus a create, so it arrives as an update
// commit on the same rkey — and therefore the same at-uri the outbound_votes
// row is keyed by.
func voteRecastFrame(did, rev, rkey, subjectATURI, direction string) []byte {
	return voteWriteFrame(did, rev, rkey, subjectATURI, direction, "update")
}

func voteWriteFrame(did, rev, rkey, subjectATURI, direction, operation string) []byte {
	return []byte(fmt.Sprintf(
		`{"did":%q,"time_us":9500,"kind":"commit","commit":{"rev":%q,"operation":%q,`+
			`"collection":"social.coves.feed.vote","rkey":%q,`+
			`"cid":"bafyreievgu2ty7qbiaaom5zhmkznsnajuzideek3lo7e65dwqlrvrxnmo4",`+
			`"record":{"$type":"social.coves.feed.vote","subject":{"uri":%q,"cid":%q},`+
			`"direction":%q,"createdAt":"2026-08-13T10:00:00.000Z"}}}`,
		did, rev, operation, rkey, subjectATURI, acceptRootCID, direction))
}

func voteDeleteFrame(did, rev, rkey string) []byte {
	return []byte(fmt.Sprintf(
		`{"did":%q,"time_us":9600,"kind":"commit","commit":{"rev":%q,"operation":"delete",`+
			`"collection":"social.coves.feed.vote","rkey":%q}}`,
		did, rev, rkey))
}

func voteATURIFor(did, rkey string) string {
	return "at://" + did + "/" + CollectionVote + "/" + rkey
}

// ---------------------------------------------------------------------------
// I1 — a vote on a bridged (Lemmy-origin) subject
// ---------------------------------------------------------------------------

func TestVoteCreate_OnAMappedSubjectWritesStateAndOneIntent(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)
	fixture := newDispatchFixture(t, database)
	ctx := context.Background()

	const rkey = "3lzvote000001"
	voteATURI := voteATURIFor(dispatchNativeDID, rkey)

	require.NoError(t, fixture.handle(t,
		voteFrame(dispatchNativeDID, dispatchRev, rkey, acceptRootATURI, "up")))

	// The author had never federated anything, so this vote earned them an
	// identity — through the same verified-handle path a comment uses.
	assert.Equal(t, []string{dispatchNativeDID}, fixture.resolver.Calls(),
		"a vote is a federating interaction, so it mints — an earlier draft of the "+
			"task missed this gate entirely")
	assert.Equal(t, []string{dispatchNativeHandle}, fixture.minter.Handles())

	stored, err := store.NewOutboundVotes(database).GetByATURI(ctx, voteATURI)
	require.NoError(t, err, "the vote's state is keyed by the VOTE record's at-uri, "+
		"because that is the only thing its delete commit will carry")
	require.NotNil(t, stored)

	assert.Equal(t, dispatchNativeDID, stored.ActorDID)
	assert.Equal(t, acceptRootATURI, stored.SubjectATURI)
	assert.Equal(t, acceptRootAPID, stored.SubjectAPID,
		"the subject's AP id comes from its ap_objects mapping")
	assert.Equal(t, acceptCommunityDID, stored.CommunityDID)
	assert.Equal(t, "up", stored.Direction)
	assert.Equal(t, 0, stored.ActivitySeq)
	assert.Equal(t, store.DeliveredStatePending, stored.DeliveredState,
		"the consumer records INTENT only; task 15 claims delivery, and only on success")
	assert.Equal(t, ActivityID(acceptUserOrigin, voteATURI, "create", 0), stored.CurrentActivityID,
		"the id the Like goes out under is STORED, because the Undo has to embed it "+
			"long after the vote record is gone")

	calls := fixture.enqueuer.Calls()
	require.Len(t, calls, 1, "exactly one intent per vote")
	assert.Equal(t, dispatchNativeDID, calls[0].ActorDID)

	intent, ok := calls[0].Intent.(VoteIntent)
	require.True(t, ok, "want VoteIntent, got %T", calls[0].Intent)
	assert.Equal(t, "create", intent.Op)
	assert.Equal(t, voteATURI, intent.VoteATURI)
	assert.Equal(t, acceptRootAPID, intent.SubjectAPID)
	assert.Equal(t, "up", intent.Direction)
	assert.Equal(t, acceptCommunityAPID, intent.CommunityAPID)
	assert.Equal(t, stored.CurrentActivityID, intent.ActivityID(),
		"the intent and the stored state must agree on the id, or the Undo would "+
			"withdraw an activity the peer never saw")
}

func TestVoteCreate_DownvoteCarriesItsDirection(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)
	fixture := newDispatchFixture(t, database)

	require.NoError(t, fixture.handle(t,
		voteFrame(dispatchNativeDID, dispatchRev, "3lzvote000002", acceptRootATURI, "down")))

	stored, err := store.NewOutboundVotes(database).GetByATURI(
		context.Background(), voteATURIFor(dispatchNativeDID, "3lzvote000002"))
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, "down", stored.Direction,
		"direction is stored verbatim: it is what the Undo is reconstructed from, and "+
			"a Dislike withdrawn as a Like would corrupt the peer's count")
}

// ---------------------------------------------------------------------------
// I2 — a vote on a native accepted post
// ---------------------------------------------------------------------------

func TestVoteCreate_OnASubjectKnownOnlyFromOutboundState(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	fixture := newDispatchFixture(t, database)
	ctx := context.Background()

	// A native postv2 the acceptance engine admitted: never materialized from
	// the fediverse, so no ap_objects mapping exists for it at all.
	subjectATURI := "at://" + acceptRootAuthorDID + "/" + CollectionPostV2 + "/3lznativevt1"
	subject := seedOutboundParent(t, database, subjectATURI, 0)
	require.Zero(t, countRows(t, database, "ap_objects"))

	const rkey = "3lzvote000003"
	require.NoError(t, fixture.handle(t,
		voteFrame(dispatchNativeDID, dispatchRev, rkey, subjectATURI, "up")))

	stored, err := store.NewOutboundVotes(database).GetByATURI(
		ctx, voteATURIFor(dispatchNativeDID, rkey))
	require.NoError(t, err,
		"votes on native accepted posts must federate: those posts ARE the bridged "+
			"content, and dropping their votes would freeze every native thread's score")
	require.NotNil(t, stored)
	assert.Equal(t, subject.APObjectID, stored.SubjectAPID)
	assert.Equal(t, acceptCommunityDID, stored.CommunityDID,
		"the community comes from the subject's outbound state — the same answer the "+
			"ap_objects path gives")
}

func TestVoteCreate_OnAnUnknownSubjectIsSkipped(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	fixture := newDispatchFixture(t, database)

	require.NoError(t, fixture.handle(t, voteFrame(dispatchNativeDID, dispatchRev,
		"3lzvote000004", "at://did:plc:nobody/social.coves.community.postv2/nope", "up")),
		"a vote on a subject this bridge does not federate is a SKIP: native users vote "+
			"in native communities constantly, and dead-lettering that would bury the queue")

	assert.Zero(t, countRows(t, database, "outbound_votes"))
	assert.Empty(t, fixture.enqueuer.Calls())
	assert.Empty(t, fixture.minter.Handles(), "and nothing is minted for it")
}

// ---------------------------------------------------------------------------
// I3 — the Undo, rebuilt entirely from state
// ---------------------------------------------------------------------------

func TestVoteDelete_ReconstructsTheUndoFromState(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)
	fixture := newDispatchFixture(t, database)
	ctx := context.Background()

	const rkey = "3lzvote000005"
	voteATURI := voteATURIFor(dispatchNativeDID, rkey)

	require.NoError(t, fixture.handle(t,
		voteFrame(dispatchNativeDID, dispatchRev, rkey, acceptRootATURI, "down")))
	created, err := store.NewOutboundVotes(database).GetByATURI(ctx, voteATURI)
	require.NoError(t, err)
	require.NotNil(t, created)
	likeID := created.CurrentActivityID

	// The delete frame carries the vote's at-uri and nothing else.
	require.NoError(t, fixture.handle(t,
		voteDeleteFrame(dispatchNativeDID, dispatchRevHigher, rkey)))

	calls := fixture.enqueuer.Calls()
	require.Len(t, calls, 2, "one Like, one Undo")
	intent, ok := calls[1].Intent.(VoteIntent)
	require.True(t, ok, "want VoteIntent, got %T", calls[1].Intent)

	assert.Equal(t, "undo", intent.Op)
	assert.Equal(t, voteATURI, intent.VoteATURI)
	assert.Equal(t, "down", intent.Direction,
		"the direction is read back from state — the delete commit does not carry it, "+
			"and an Undo{Like} withdrawing a Dislike would move the peer's count the "+
			"wrong way")
	assert.Equal(t, acceptRootAPID, intent.SubjectAPID, "as is the subject")
	assert.Equal(t, likeID, intent.InnerActivityID,
		"the Undo EMBEDS the activity it withdraws, by the id the Like was delivered "+
			"under — a freshly derived id would name an activity the peer never saw")

	updated, err := store.NewOutboundVotes(database).GetByATURI(ctx, voteATURI)
	require.NoError(t, err, "the row SURVIVES the delete: task 15 needs it to retry the "+
		"Undo, and clears it only once delivery succeeds")
	require.NotNil(t, updated)
	assert.Equal(t, 1, updated.ActivitySeq, "the Undo is the next activity")
	assert.Equal(t, ActivityID(acceptUserOrigin, voteATURI, "undo", 1), intent.ActivityID(),
		"and its id derives from the bumped seq, so it can never collide with the Like's")
	assert.Equal(t, store.DeliveredStatePending, updated.DeliveredState,
		"the consumer still claims no delivery; task 15 flips this to undone on success")
}

func TestVoteDelete_OfAnUnknownVoteIsSkipped(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	fixture := newDispatchFixture(t, database)

	require.NoError(t, fixture.handle(t,
		voteDeleteFrame(dispatchNativeDID, dispatchRev, "3lzvote000006")),
		"a vote this bridge never federated has nothing to withdraw")

	assert.Empty(t, fixture.enqueuer.Calls(),
		"and no Undo may be sent for a Like no peer ever received")
}

// ---------------------------------------------------------------------------
// I4 — records the handler cannot act on
// ---------------------------------------------------------------------------

func TestVoteCreate_UnknownDirectionIsSkipped(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)
	fixture := newDispatchFixture(t, database)

	// knownValues is an OPEN enum: the lexicon may grow a direction this build
	// has never heard of.
	require.NoError(t, fixture.handle(t,
		voteFrame(dispatchNativeDID, dispatchRev, "3lzvote000007", acceptRootATURI, "sideways")),
		"RULED SKIP: a direction this build does not understand is a forward-compatible "+
			"record, not a malformed one. Dead-lettering it would turn a lexicon "+
			"rollout into a queue full of rows nobody can redrive")

	assert.Zero(t, countRows(t, database, "outbound_votes"),
		"nothing is stored: guessing a direction would push a vote the user never cast")
	assert.Empty(t, fixture.enqueuer.Calls())
}

func TestVoteCreate_SecondVoteForTheSameSubjectLeavesTheFirstIntact(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)
	fixture := newDispatchFixture(t, database)
	ctx := context.Background()

	require.NoError(t, fixture.handle(t,
		voteFrame(dispatchNativeDID, dispatchRev, "3lzvote000008", acceptRootATURI, "up")))
	first, err := store.NewOutboundVotes(database).GetByATURI(
		ctx, voteATURIFor(dispatchNativeDID, "3lzvote000008"))
	require.NoError(t, err)
	require.NotNil(t, first)

	// A SECOND vote record on the same subject while the first still stands.
	// One actor holds one live vote per subject, so the store refuses it.
	err = fixture.handle(t,
		voteFrame(dispatchNativeDID, dispatchRevHigher, "3lzvote000009", acceptRootATURI, "down"))

	require.Error(t, err,
		"RULED TRANSIENT. Changing a vote is a delete of the old record plus a create "+
			"of the new one, and the create can reach this handler first — on a cursor "+
			"rewind, or because the delete was itself skipped while the author was "+
			"opted out. Skipping the create would lose that vote PERMANENTLY: no event "+
			"ever re-fires it. Retrying costs nothing and succeeds the moment the "+
			"delete lands; a genuinely stuck one exhausts into the DLQ, where it is "+
			"visible instead of silent")
	assert.NotErrorIs(t, err, ErrPermanentEvent,
		"so the redriver must be allowed to replay it — a permanent classification "+
			"would spend the budget immediately and strand the vote for good")

	assert.Empty(t, storedRev(t, database, voteATURIFor(dispatchNativeDID, "3lzvote000009")),
		"and the failed event leaves NO gate row: the redrive replays the same rev, so "+
			"a claimed gate would reject the retry that was supposed to recover it")

	assert.Equal(t, 1, countRows(t, database, "outbound_votes"),
		"the second record is not stored")
	unchanged, err := store.NewOutboundVotes(database).GetByATURI(
		ctx, voteATURIFor(dispatchNativeDID, "3lzvote000008"))
	require.NoError(t, err)
	require.NotNil(t, unchanged)
	assert.Equal(t, "up", unchanged.Direction,
		"and the first vote is left exactly as it was — clobbering it would strand the "+
			"Undo that is still owed for the Like already delivered")
	assert.Equal(t, first.ActivitySeq, unchanged.ActivitySeq)

	assert.Len(t, fixture.enqueuer.Calls(), 1, "and no second intent goes out")
}

// ---------------------------------------------------------------------------
// I5 — the opt-out gate, and the retraction asymmetry
// ---------------------------------------------------------------------------

func TestVoteCreate_IsBlockedForAnOptedOutAuthor(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)
	ctx := context.Background()

	_, err := store.NewFederationPrefs(database).Upsert(ctx, store.FederationPref{
		DID:    dispatchNativeDID,
		Source: store.FederationPrefSourceRecord,
	})
	require.NoError(t, err)

	fixture := newDispatchFixture(t, database)
	require.NoError(t, fixture.handle(t,
		voteFrame(dispatchNativeDID, dispatchRev, "3lzvote000010", acceptRootATURI, "up")))

	assert.Empty(t, fixture.minter.Handles(),
		"the opt-out gate runs before the mint here exactly as it does for comments — "+
			"an earlier draft of the task missed this gate on the vote path")
	assert.Zero(t, countRows(t, database, "ap_actors"))
	assert.Zero(t, countRows(t, database, "outbound_votes"))
	assert.Empty(t, fixture.enqueuer.Calls())
}

func TestVoteDelete_ProcessesEvenForAnOptedOutAuthor(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)
	fixture := newDispatchFixture(t, database)
	ctx := context.Background()

	const rkey = "3lzvote000011"
	require.NoError(t, fixture.handle(t,
		voteFrame(dispatchNativeDID, dispatchRev, rkey, acceptRootATURI, "up")))
	require.Len(t, fixture.enqueuer.Calls(), 1)

	// The author opts out AFTER the Like is already out on the fediverse.
	_, err := store.NewFederationPrefs(database).Upsert(ctx, store.FederationPref{
		DID:    dispatchNativeDID,
		Source: store.FederationPrefSourceRecord,
	})
	require.NoError(t, err)

	require.NoError(t, fixture.handle(t,
		voteDeleteFrame(dispatchNativeDID, dispatchRevHigher, rkey)))

	calls := fixture.enqueuer.Calls()
	require.Len(t, calls, 2,
		"the same retraction asymmetry as comments: an opt-out stops new content going "+
			"out, but must never trap a Like the user is trying to withdraw — blocking "+
			"the Undo would leave their vote standing on the peer forever")
	intent, ok := calls[1].Intent.(VoteIntent)
	require.True(t, ok)
	assert.Equal(t, "undo", intent.Op)
}

// ---------------------------------------------------------------------------
// I6 — the re-cast guard
// ---------------------------------------------------------------------------

func TestVoteRecast_DeliveredStateSurvivesTheFlip(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)
	fixture := newDispatchFixture(t, database)
	votes := store.NewOutboundVotes(database)
	ctx := context.Background()

	const rkey = "3lzvote000012"
	voteATURI := voteATURIFor(dispatchNativeDID, rkey)

	require.NoError(t, fixture.handle(t,
		voteFrame(dispatchNativeDID, dispatchRev, rkey, acceptRootATURI, "up")))

	// The Like reached the peer, and the delivery worker settled it — the same
	// call task 15's settlement callback makes, from the wire, on success.
	require.NoError(t, votes.SetDeliveredState(ctx, voteATURI, store.DeliveredStateDelivered),
		"the vote must be genuinely delivered before the flip, or this test proves nothing")

	// The user flips up→down by EDITING the vote record, so the commit is an
	// update on the same rkey — the same at-uri the row above is keyed by.
	require.NoError(t, fixture.handle(t,
		voteRecastFrame(dispatchNativeDID, dispatchRevHigher, rkey, acceptRootATURI, "down")))

	recast, err := votes.GetByATURI(ctx, voteATURI)
	require.NoError(t, err)
	require.NotNil(t, recast)
	assert.Equal(t, "down", recast.Direction, "the flip itself is recorded")

	assert.Equal(t, store.DeliveredStateDelivered, recast.DeliveredState,
		"the peer STILL HOLDS a vote from this actor — the flip is a replacement, not a "+
			"retraction, and nothing has come back off the wire to say otherwise. Resetting "+
			"to pending erases the only record that a delivery ever happened, and the fact "+
			"is unrecoverable: no event re-fires it")

	// The guard is about the ledger, not the wire. The flip must still go out.
	calls := fixture.enqueuer.Calls()
	require.Len(t, calls, 2, "one Like, then the Dislike that replaces it")
	intent, ok := calls[1].Intent.(VoteIntent)
	require.True(t, ok, "want VoteIntent, got %T", calls[1].Intent)
	assert.Equal(t, "down", intent.Direction,
		"suppressing the outgoing flip would leave the peer counting the vote the user "+
			"just changed away from")
	assert.Equal(t, ActivityID(acceptUserOrigin, voteATURI, "create", 1), intent.ActivityID(),
		"under a BUMPED seq: an id colliding with the delivered Like's would be swallowed "+
			"as a duplicate by any peer that already has it")
	assert.Equal(t, recast.CurrentActivityID, intent.ActivityID(),
		"and the row carries that same id, because the Undo has to embed it")

	// The operator-visible consequence, and the reason the ledger fact matters:
	// this list is what the erasure purge enumerates.
	standing, err := votes.ListStandingForActor(ctx, dispatchNativeDID)
	require.NoError(t, err)
	require.Len(t, standing, 1,
		"a flipped vote must still be reachable by the purge — dropping out of this list "+
			"strands it un-retractable on the peer, so erasing the actor would leave their "+
			"vote standing on someone else's instance forever")
	assert.Equal(t, voteATURI, standing[0].VoteATURI)
}
