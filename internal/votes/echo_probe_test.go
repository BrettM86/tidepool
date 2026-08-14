package votes

import (
	"context"
	"database/sql"
	stderrors "errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/echo"
	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// The aggregator-level echo guard (task 17a, decision-16 AMENDMENT).
//
// The question here is NOT the ingest classifier's question. That one asks
// whether an ENVELOPE is our own traffic, at the dispatch boundary. This one
// asks whether a VOTER may ever appear in vote_events, at the mutation site —
// which is also the only guard covering the paths that never reach
// handleAnnounce: the bare /ap/inbox vote branch, the community-outbox
// backfill, and the seeder.
//
// The probe is on the VOTER, not the activity id. Measured (aggregator.go
// :197-214): Lemmy 0.19 sends Announce{Undo{Like}} carrying a RECONSTRUCTED
// inner vote with a FRESHLY GENERATED activity id, typed "Like" even when the
// live vote is a Dislike. An id-keyed probe against outbound_activities
// therefore misses EVERY echoed Undo while appearing to work.
//
// The probe table is ap_actors (personas WE mint and speak as), never
// bridged_actors (real fediverse humans we MIRROR into atproto). Both tables
// hold DIDs we minted, and confusing them drops every genuine inbound vote —
// the vote pipeline goes dark and every community's tallies silently zero.
// FP-1 below exists to catch exactly that.
const (
	probeOrigin  = "https://coves.social"
	probeHost    = "coves.social"
	personaDID   = "did:plc:nativepersona00001"
	personaActor = probeOrigin + "/ap/actor/" + personaDID

	// A real Lemmy human the bridge MIRRORS into atproto: a bridged_actors row
	// with a DID we minted for them. Genuine remote traffic.
	mirroredVoter = voterAlice
	mirroredDID   = "did:plc:mirroredhuman0001"

	// An id ON the actor route with no ap_actors row behind it. The voter must
	// be route-shaped or the probe answers ClassNone without reading anything:
	// personas are always {origin}/ap/actor/{did} — decision 10 varies the
	// ORIGIN, never the path — so an id off that route is definitively not
	// ours, and a failing store is never consulted. Only a route-shaped id
	// reaches the lookup this test is about.
	probeMissingPersona = probeOrigin + "/ap/actor/did:plc:neverminted00000"

	probeGroupIRI     = "https://lemmy.world/c/technology"
	probeCommunityDID = "did:plc:probecommunity001"
)

// probeWorld is testDB plus the two actor tables the probe distinguishes.
func probeWorld(t *testing.T) (*sql.DB, store.APObjects) {
	t.Helper()
	database := testDB(t)
	ctx := context.Background()

	_, err := store.NewAPActors(database).Create(ctx, store.APActor{
		DID:              personaDID,
		Kind:             store.ActorTypePerson,
		ActorID:          personaActor,
		NormalizedOrigin: probeHost,
		LocalPart:        "nativepersona",
		RSAKeySealed:     []byte{0x01, 0x02, 0x03},
		RSAKeyVersion:    1,
		PublicKeyPEM:     "-----BEGIN PUBLIC KEY-----\nMIIB\n-----END PUBLIC KEY-----\n",
	})
	require.NoError(t, err, "mint the native persona whose votes must never count as inbound")

	_, err = store.NewBridgedActors(database).UpsertActor(ctx, store.BridgedActor{
		APActorID:    mirroredVoter,
		ActorType:    store.ActorTypePerson,
		DID:          mirroredDID,
		Handle:       "alice.lemmy-world.bridge.test",
		ConsentState: store.ConsentStateOK,
	})
	require.NoError(t, err, "mirror the genuine Lemmy human into atproto")

	return database, store.NewAPObjects(database)
}

// probeAggregator builds an Aggregator whose voter probe reads the given
// stores, so a test can break exactly one of them.
func probeAggregator(t *testing.T, database *sql.DB, actors store.APActors) *Aggregator {
	t.Helper()
	objects := store.NewAPObjects(database)
	probe, err := echo.New(echo.Options{
		Objects:         objects,
		OutboundObjects: store.NewOutboundObjects(database),
		Activities:      store.NewOutboundActivities(database),
		Actors:          actors,
	})
	require.NoError(t, err)
	agg, err := NewAggregator(database, objects, store.NewCommunities(database),
		&fakeRecords{records: map[string]map[string]any{}}, probe, slog.Default())
	require.NoError(t, err)
	return agg
}

// liveVotes counts the live (non-undone) vote_events rows for one voter.
func liveVotes(t *testing.T, database *sql.DB, voter string) int {
	t.Helper()
	var n int
	require.NoError(t, database.QueryRow(`
		SELECT COUNT(*) FROM vote_events WHERE voter_ap_id = $1 AND NOT undone`, voter).Scan(&n))
	return n
}

// allVotes counts every vote_events row for one voter, undone included.
func allVotes(t *testing.T, database *sql.DB, voter string) int {
	t.Helper()
	var n int
	require.NoError(t, database.QueryRow(`
		SELECT COUNT(*) FROM vote_events WHERE voter_ap_id = $1`, voter).Scan(&n))
	return n
}

// TestOurOwnPersonasNeverEnterVoteEvents is M2 at the aggregator: a vote whose
// VOTER is one of our personas is our own outbound vote coming home. Counting
// it inflates the subject's score by exactly the votes we ourselves cast, and
// 17b's seeder subtraction assumes it never happens.
func TestOurOwnPersonasNeverEnterVoteEvents(t *testing.T) {
	database, objects := probeWorld(t)
	agg := probeAggregator(t, database, store.NewAPActors(database))
	ctx := context.Background()
	bridgeSubject(t, objects, subjectPost, "3lzprobe000001")

	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), personaActor, subjectPost), ""),
		"an echoed vote is dropped, not an error: there is nothing to retry")
	require.NoError(t, agg.ApplyVote(ctx, dislike(activityID(t, 2), personaActor, subjectPost), ""),
		"the same holds for a Dislike — direction is not what makes it ours")

	assert.Equal(t, 0, allVotes(t, database, personaActor),
		"a persona's vote must leave NO row in vote_events, live or undone")
	_, _, found := counts(t, database, subjectPost)
	assert.False(t, found,
		"and no aggregate either: a suppressed vote must not even mint a 0/0 row, which "+
			"the XRPC contract reads as 'this subject has been voted on'")

	// Control: the very same shape from a genuine Lemmy human IS counted, so
	// the assertions above cannot pass because the subject is unvotable.
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 3), mirroredVoter, subjectPost), ""))
	up, down, found := counts(t, database, subjectPost)
	require.True(t, found, "a genuine vote must still create the aggregate")
	assert.Equal(t, 1, up)
	assert.Equal(t, 0, down)
}

// TestSuppressionCoversLikeAndUndoSymmetrically is M4. Asymmetry is worse than
// no suppression: RetractVote's step-2 fallback retracts the voter's live vote
// REGARDLESS of activity id, so an unsuppressed echoed Undo mutates whatever
// row that voter has — a phantom retraction of a vote we never counted.
//
// The persona's live row here is the one suppression could not have prevented:
// a row written before this guard existed, or imported by a backfill. Cleaning
// those up is 17e's reconciliation job; an inbound echo must never be what
// touches them.
func TestSuppressionCoversLikeAndUndoSymmetrically(t *testing.T) {
	database, objects := probeWorld(t)
	agg := probeAggregator(t, database, store.NewAPActors(database))
	ctx := context.Background()
	bridgeSubject(t, objects, subjectPost, "3lzprobe000002")

	// A genuine human's upvote establishes the subject's aggregate row (which
	// RetractVote locks — without it every retraction is a vacuous no-op).
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), mirroredVoter, subjectPost), ""))

	// The persona's pre-existing live DISLIKE. Direction matters: Lemmy's
	// reconstructed Undo is typed Like, so a probe keyed on the inner type or
	// id would sail straight past this.
	_, err := database.ExecContext(ctx, `
		INSERT INTO vote_events (activity_id, voter_ap_id, subject_ap_id, direction)
		VALUES ($1, $2, $3, 'down')`,
		"https://coves.social/ap/activity/legacy-persona-vote", personaActor, subjectPost)
	require.NoError(t, err, "seed the persona's pre-suppression live vote")
	_, err = database.ExecContext(ctx, `
		UPDATE vote_aggregates SET upvotes = 1, downvotes = 1 WHERE subject_ap_id = $1`, subjectPost)
	require.NoError(t, err, "make the aggregate agree with the seeded row")

	// (a) Like direction: an echoed re-vote must not insert, and must not
	//     supersede the row that is already there.
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 2), personaActor, subjectPost), ""))
	assert.Equal(t, 1, liveVotes(t, database, personaActor),
		"an echoed Like must neither add a row nor supersede the persona's existing one")
	up, down, _ := counts(t, database, subjectPost)
	assert.Equal(t, 1, up, "aggregate untouched by an echoed Like")
	assert.Equal(t, 1, down)

	// (b) Undo direction, in LEMMY'S RECONSTRUCTED SHAPE: a freshly generated
	//     activity id the bridge has never seen, typed Like while the live vote
	//     is a Dislike. This is precisely what an id-keyed probe misses.
	reconstructed := vote("Like", "https://lemmy.world/activities/like/regenerated-9f2c",
		personaActor, subjectPost)
	require.NoError(t, agg.RetractVote(ctx, reconstructed, ""))
	assert.Equal(t, 1, liveVotes(t, database, personaActor),
		"an echoed Undo must retract NOTHING: suppression that covers Like but not Undo "+
			"retracts a vote our own suppression stopped us from ever recording")
	up, down, _ = counts(t, database, subjectPost)
	assert.Equal(t, 1, up, "aggregate untouched by an echoed Undo")
	assert.Equal(t, 1, down)

	// Control: the identical reconstructed shape from the genuine human DOES
	// retract — the fallback path is alive, and only the voter decides.
	require.NoError(t, agg.RetractVote(ctx,
		vote("Like", "https://lemmy.world/activities/like/regenerated-aaaa", mirroredVoter, subjectPost), ""))
	assert.Equal(t, 0, liveVotes(t, database, mirroredVoter),
		"a genuine Undo still retracts through the reconstructed-id fallback")
	up, _, _ = counts(t, database, subjectPost)
	assert.Equal(t, 0, up, "and the aggregate follows it")
}

// TestBridgedActorVotesAreAlwaysCounted is FP-1, the lethal false positive.
//
// A mirrored Lemmy human holds a DID the bridge minted and a bridged_actors
// row — everything about them looks "ours" except the one thing that matters:
// we do not speak as them. Probing the wrong table drops every genuine inbound
// vote in the network, silently, and the served tallies go to zero.
func TestBridgedActorVotesAreAlwaysCounted(t *testing.T) {
	database, objects := probeWorld(t)
	agg := probeAggregator(t, database, store.NewAPActors(database))
	ctx := context.Background()
	followCommunity(t, database, probeGroupIRI, probeCommunityDID)
	bridgeSubjectAs(t, objects, subjectPost, "3lzprobe000003", probeCommunityDID, testCollection)

	// The full announced shape, the way a community fans out its members' votes.
	require.NoError(t, agg.ApplyVote(ctx,
		like(activityID(t, 1), mirroredVoter, subjectPost), probeGroupIRI))

	assert.Equal(t, 1, liveVotes(t, database, mirroredVoter),
		"a mirrored Lemmy human's vote MUST be recorded: bridged_actors are remote humans, "+
			"ap_actors are the personas we speak as, and only the second is an echo")
	up, down, found := counts(t, database, subjectPost)
	require.True(t, found)
	assert.Equal(t, 1, up, "the aggregate must move for genuine remote votes")
	assert.Equal(t, 0, down)

	// And their retraction works too.
	require.NoError(t, agg.RetractVote(ctx,
		vote("Like", "https://lemmy.world/activities/like/regenerated-bbbb", mirroredVoter, subjectPost),
		probeGroupIRI))
	up, _, _ = counts(t, database, subjectPost)
	assert.Equal(t, 0, up, "a genuine human's Undo still lands")
}

// TestVoterProbeFailureIsRetryable is FP-6: the probe obeys the classifier's
// fail-safe direction. A database hiccup must not become a verdict — counting
// the vote double-counts our own, dropping it loses a genuine one, and only the
// retry is honest.
func TestVoterProbeFailureIsRetryable(t *testing.T) {
	database, objects := probeWorld(t)
	bridgeSubject(t, objects, subjectPost, "3lzprobe000004")
	boom := stderrors.New("connection reset by peer")
	agg := probeAggregator(t, database,
		failingProbeActors{APActors: store.NewAPActors(database), err: boom})
	ctx := context.Background()

	err := agg.ApplyVote(ctx, like(activityID(t, 1), probeMissingPersona, subjectPost), "")
	require.Error(t, err, "a probe that cannot answer must not be treated as 'not ours'")
	assert.ErrorIs(t, err, boom, "the transient cause must be preserved for the retry classifier")
	assert.False(t, errors.IsValidation(err),
		"a validation error poisons the event and the vote is lost for good")
	assert.False(t, errors.IsNotFound(err), "a failed lookup is not a miss")
	assert.Equal(t, 0, allVotes(t, database, probeMissingPersona),
		"nothing may be written on the failure path — the retry must find a clean slate")
	_, _, found := counts(t, database, subjectPost)
	assert.False(t, found, "and no aggregate row may be minted on the way to the failure")

	err = agg.RetractVote(ctx,
		vote("Like", "https://lemmy.world/activities/like/regenerated-cccc", probeMissingPersona, subjectPost), "")
	require.Error(t, err, "the retraction path owes the same contract")
	assert.ErrorIs(t, err, boom)
	assert.False(t, errors.IsValidation(err))
}

// failingProbeActors breaks the one read the actor route depends on.
type failingProbeActors struct {
	store.APActors
	err error
}

func (f failingProbeActors) GetByDID(context.Context, string) (*store.APActor, error) {
	return nil, f.err
}
