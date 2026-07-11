package votes

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/materialize"
)

// Fixtures for announced-vote community binding.
const (
	testGroupIRI = "https://lemmy.world/c/golang"

	// A second community's repo DID and a comment author's user DID.
	otherCommunityDID = "did:plc:yk4dd2qkboz2yv6tpubpc6co"
	commentAuthorDID  = "did:plc:44ybard66vv44zksje25o7dz"
)

func TestApplyVoteCountsDistinctVoters(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 2), voterBob, subjectPost), ""))
	require.NoError(t, agg.ApplyVote(ctx, dislike(activityID(t, 3), voterCarol, subjectPost), ""))

	up, down, found := counts(t, database, subjectPost)
	require.True(t, found)
	assert.Equal(t, 2, up)
	assert.Equal(t, 1, down)
}

func TestApplyVoteFlipCountsLatestState(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	// Alice upvotes, then flips to a downvote WITHOUT an Undo in between
	// (implementations differ; the count must reflect her latest state).
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	require.NoError(t, agg.ApplyVote(ctx, dislike(activityID(t, 2), voterAlice, subjectPost), ""))

	up, down, _ := counts(t, database, subjectPost)
	assert.Equal(t, 0, up, "the flipped-away upvote must not linger")
	assert.Equal(t, 1, down)

	// The Lemmy-style flip (Undo{Dislike} then Like) nets out too.
	require.NoError(t, agg.RetractVote(ctx, dislike(activityID(t, 2), voterAlice, subjectPost), ""))
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 3), voterAlice, subjectPost), ""))

	up, down, _ = counts(t, database, subjectPost)
	assert.Equal(t, 1, up)
	assert.Equal(t, 0, down)
}

func TestRetractVoteDecrements(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	require.NoError(t, agg.RetractVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))

	up, down, found := counts(t, database, subjectPost)
	require.True(t, found)
	assert.Equal(t, 0, up)
	assert.Equal(t, 0, down)

	// Redelivered undo: still zero, still no error.
	require.NoError(t, agg.RetractVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	up, down, _ = counts(t, database, subjectPost)
	assert.Equal(t, 0, up)
	assert.Equal(t, 0, down)
}

func TestDuplicateActivityIDIsNoOp(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	dup := like(activityID(t, 1), voterAlice, subjectPost)
	require.NoError(t, agg.ApplyVote(ctx, dup, ""))
	require.NoError(t, agg.ApplyVote(ctx, dup, ""))

	up, down, _ := counts(t, database, subjectPost)
	assert.Equal(t, 1, up, "a re-delivered vote must not double-count")
	assert.Equal(t, 0, down)

	// Even a forged reuse of the id (different voter) changes nothing.
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterBob, subjectPost), ""))
	up, _, _ = counts(t, database, subjectPost)
	assert.Equal(t, 1, up)
}

func TestUndoBeforeLikeIsNoOp(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	// The undo of a vote the bridge never saw must not error or go negative.
	require.NoError(t, agg.RetractVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	_, _, found := counts(t, database, subjectPost)
	assert.False(t, found, "an unactionable undo must not create an aggregate")

	// The (out-of-order) like still lands afterwards.
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	up, down, _ := counts(t, database, subjectPost)
	assert.Equal(t, 1, up)
	assert.Equal(t, 0, down)
}

func TestReplayedUndoDoesNotRetractReLike(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	// Like(A), Undo(A), re-Like(B), then Undo(A) REPLAYED (a fresh Announce
	// wrapper gets it past inbox dedupe): the undo must target activity A —
	// long since undone — and never B, which merely shares
	// (voter, subject, direction).
	likeA := like(activityID(t, 1), voterAlice, subjectPost)
	require.NoError(t, agg.ApplyVote(ctx, likeA, ""))
	require.NoError(t, agg.RetractVote(ctx, likeA, ""))
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 2), voterAlice, subjectPost), ""))
	require.NoError(t, agg.RetractVote(ctx, likeA, ""))

	up, down, _ := counts(t, database, subjectPost)
	assert.Equal(t, 1, up, "the replayed undo of A must not retract the newer like B")
	assert.Equal(t, 0, down)
}

// TestLemmyClearAfterFlipRetractsLiveVote pins the vote-clear wire behavior
// the e2e suite measured against a real Lemmy 0.19: a flip federates as a
// bare opposite vote (no Undo), and a clear federates as an Undo whose inner
// vote is RECONSTRUCTED — a freshly generated activity id, typed Like even
// though the voter's live vote is the dislike. The retraction must fall back
// to removing the voter's live vote regardless of the inner id/type.
// (Before this pin, every Lemmy vote-clear was a silent no-op.)
func TestLemmyClearAfterFlipRetractsLiveVote(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	// Like → flip (bare Dislike) → clear (Undo{Like} with a fresh id).
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	require.NoError(t, agg.ApplyVote(ctx, dislike(activityID(t, 2), voterAlice, subjectPost), ""))
	require.NoError(t, agg.RetractVote(ctx, like(activityID(t, 3), voterAlice, subjectPost), ""))

	up, down, found := counts(t, database, subjectPost)
	require.True(t, found)
	assert.Equal(t, 0, up)
	assert.Equal(t, 0, down, "the regenerated-id Undo{Like} must retract the live dislike")
}

// TestIDLessUndoRetractsLiveVote: an inline undo whose inner vote carries no
// id at all still removes the voter's live vote.
func TestIDLessUndoRetractsLiveVote(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	require.NoError(t, agg.RetractVote(ctx, like("", voterAlice, subjectPost), ""))

	up, down, found := counts(t, database, subjectPost)
	require.True(t, found)
	assert.Equal(t, 0, up)
	assert.Equal(t, 0, down)
}

// TestUndoByOtherVoterRetractsNothing: the unknown-id fallback is scoped to
// (voter, subject) — an undo by someone who never voted must not touch
// another voter's live vote.
func TestUndoByOtherVoterRetractsNothing(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	require.NoError(t, agg.RetractVote(ctx, like(activityID(t, 2), voterBob, subjectPost), ""))

	up, down, _ := counts(t, database, subjectPost)
	assert.Equal(t, 1, up, "bob's undo must not retract alice's live vote")
	assert.Equal(t, 0, down)
}

func TestDuplicateActivityIDOnAnotherSubjectMintsNoAggregate(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	bridgeSubject(t, objects, subjectComment, "3jzfcijpj2z3a")
	ctx := context.Background()

	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	// The same activity id aimed at a DIFFERENT bridged subject is a dedupe
	// no-op that must not mint a spurious 0/0 aggregate row — the XRPC
	// contract omits never-voted uris.
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectComment), ""))

	_, _, found := counts(t, database, subjectComment)
	assert.False(t, found, "a deduped vote must not create an aggregate for its claimed subject")
	up, down, _ := counts(t, database, subjectPost)
	assert.Equal(t, 1, up)
	assert.Equal(t, 0, down)
}

func TestReLikeAfterUndoCounts(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	require.NoError(t, agg.RetractVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 2), voterAlice, subjectPost), ""))

	up, down, _ := counts(t, database, subjectPost)
	assert.Equal(t, 1, up)
	assert.Equal(t, 0, down)
}

func TestVoteOnUnbridgedSubjectDropped(t *testing.T) {
	database := testDB(t)
	agg, _ := testAggregator(t, database)
	ctx := context.Background()

	// No ap_objects mapping exists: the vote is dropped, not retried.
	require.NoError(t, agg.ApplyVote(ctx,
		like(activityID(t, 1), voterAlice, "https://lemmy.world/post/999"), ""))

	_, _, found := counts(t, database, "https://lemmy.world/post/999")
	assert.False(t, found)
	var events int
	require.NoError(t, database.QueryRow(`SELECT COUNT(*) FROM vote_events`).Scan(&events))
	assert.Zero(t, events)
}

func TestVoteOnDeletedSubjectDropped(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()
	require.NoError(t, objects.SoftDelete(ctx, subjectPost))

	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	_, _, found := counts(t, database, subjectPost)
	assert.False(t, found, "votes on tombstoned content must not resurrect state")
}

func TestVotesAreKeyedPerSubject(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	bridgeSubject(t, objects, subjectComment, "3jzfcijpj2z3a")
	ctx := context.Background()

	// The same voter holds independent votes on different subjects.
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	require.NoError(t, agg.ApplyVote(ctx, dislike(activityID(t, 2), voterAlice, subjectComment), ""))

	up, down, _ := counts(t, database, subjectPost)
	assert.Equal(t, 1, up)
	assert.Equal(t, 0, down)
	up, down, _ = counts(t, database, subjectComment)
	assert.Equal(t, 0, up)
	assert.Equal(t, 1, down)
}

func TestMalformedVotesAreDroppedNotRetried(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	// The LOOP contract: nil/bare refs must be no-ops, never errors (an
	// error would wedge the community's ordering key on retries).
	require.NoError(t, agg.ApplyVote(ctx, nil, ""))
	require.NoError(t, agg.ApplyVote(ctx, &ap.Object{Type: ap.TypeLike}, ""))
	require.NoError(t, agg.ApplyVote(ctx,
		&ap.Object{ID: activityID(t, 1), Type: ap.TypeLike, Object: &ap.Object{ID: subjectPost}}, ""))
	require.NoError(t, agg.ApplyVote(ctx, // no activity id
		&ap.Object{Type: ap.TypeLike, Actor: &ap.Object{ID: voterAlice}, Object: &ap.Object{ID: subjectPost}}, ""))
	require.NoError(t, agg.ApplyVote(ctx, // not a vote type
		vote(ap.TypeAnnounce, activityID(t, 2), voterAlice, subjectPost), ""))

	require.NoError(t, agg.RetractVote(ctx, nil, ""))
	require.NoError(t, agg.RetractVote(ctx, &ap.Object{Type: ap.TypeLike}, ""))
	require.NoError(t, agg.RetractVote(ctx,
		&ap.Object{ID: activityID(t, 3), Type: ap.TypeLike, Actor: &ap.Object{ID: voterAlice}}, ""))

	var events int
	require.NoError(t, database.QueryRow(`SELECT COUNT(*) FROM vote_events`).Scan(&events))
	assert.Zero(t, events)
}

func TestSeedAggregates(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	atURI := bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	require.NoError(t, agg.SeedAggregates(ctx, subjectPost, 40, 3))
	up, down, found := counts(t, database, subjectPost)
	require.True(t, found)
	assert.Equal(t, 40, up)
	assert.Equal(t, 3, down)

	// Live votes stack on top of the baseline.
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	up, down, _ = counts(t, database, subjectPost)
	assert.Equal(t, 41, up)
	assert.Equal(t, 3, down)

	// Re-seeding (backfill redo) replaces the baseline. The origin's counts
	// are a TOTAL that already includes Alice's live vote, so the served
	// totals must equal them — not add her again.
	require.NoError(t, agg.SeedAggregates(ctx, subjectPost, 50, 5))
	up, down, _ = counts(t, database, subjectPost)
	assert.Equal(t, 50, up, "a re-seed must not double-count live votes")
	assert.Equal(t, 5, down)

	// The served at-uri is the mapping's.
	var storedURI string
	require.NoError(t, database.QueryRow(`
		SELECT subject_at_uri FROM vote_aggregates WHERE subject_ap_id = $1`,
		subjectPost).Scan(&storedURI))
	assert.Equal(t, atURI, storedURI)
}

func TestSeedAggregatesBeforeLiveVotesFoldsThem(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	// A live vote lands BEFORE the seed (announce raced the backfill): the
	// seed must not clobber the live row — and must not count it twice
	// either. Lemmy counted Alice's vote before announcing it, so the
	// fetched total (10 up) already includes her.
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	require.NoError(t, agg.SeedAggregates(ctx, subjectPost, 10, 1))

	up, down, _ := counts(t, database, subjectPost)
	assert.Equal(t, 10, up, "the origin total already includes the live vote")
	assert.Equal(t, 1, down)

	// Alice clears her vote: the live row is undone, and her upvote must
	// leave the served totals exactly once (it is no longer subtracted from
	// the baseline once it is not live).
	require.NoError(t, agg.RetractVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	up, down, _ = counts(t, database, subjectPost)
	assert.Equal(t, 9, up)
	assert.Equal(t, 1, down)
}

// TestReseedDoesNotDoubleCountLiveVotes pins the backfill re-seed contract:
// votes that federated live between two seeds appear in BOTH the origin's
// fetched totals and vote_events, and must be served exactly once. Before
// the net-of-live subtraction, every re-seed re-imported the live voters
// into the baseline and the recompute added them again — compounding on
// each subsequent redo.
func TestReseedDoesNotDoubleCountLiveVotes(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	// Initial backfill: the post has 10 up / 0 down of pre-subscribe history.
	require.NoError(t, agg.SeedAggregates(ctx, subjectPost, 10, 0))

	// Live federation since: two upvotes and a downvote. Lemmy's own counts
	// are now 12 up / 1 down.
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 2), voterBob, subjectPost), ""))
	require.NoError(t, agg.ApplyVote(ctx, dislike(activityID(t, 3), voterCarol, subjectPost), ""))
	up, down, _ := counts(t, database, subjectPost)
	require.Equal(t, 12, up)
	require.Equal(t, 1, down)

	// Backfill redo re-seeds with the origin's current totals. Served counts
	// must match them exactly, not 14/2.
	require.NoError(t, agg.SeedAggregates(ctx, subjectPost, 12, 1))
	up, down, _ = counts(t, database, subjectPost)
	assert.Equal(t, 12, up, "re-seed must not re-import live voters into the baseline")
	assert.Equal(t, 1, down)

	// And it must be idempotent: another redo with unchanged origin totals
	// changes nothing (no compounding).
	require.NoError(t, agg.SeedAggregates(ctx, subjectPost, 12, 1))
	up, down, _ = counts(t, database, subjectPost)
	assert.Equal(t, 12, up)
	assert.Equal(t, 1, down)
}

// TestReseedHealsBaselineVoterDrift: a voter counted only in the seeded
// baseline who flips federates a bare Dislike (Lemmy sends no Undo on
// flips), leaving the retired upvote in the baseline next to the new live
// downvote. Accepted drift while it lasts — but a re-seed must converge the
// served totals back to the origin's truth, which the raw-overwrite seed
// never did (it re-imported the flipped vote AND kept the live row).
func TestReseedHealsBaselineVoterDrift(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	// Alice's pre-subscribe upvote is part of the 10/0 baseline.
	require.NoError(t, agg.SeedAggregates(ctx, subjectPost, 10, 0))

	// She flips: a bare Dislike, no Undo. Known drift — her baseline upvote
	// lingers next to the live downvote (Lemmy's truth is 9/1).
	require.NoError(t, agg.ApplyVote(ctx, dislike(activityID(t, 1), voterAlice, subjectPost), ""))
	up, down, _ := counts(t, database, subjectPost)
	require.Equal(t, 10, up)
	require.Equal(t, 1, down)

	// Re-seed with the origin's current totals: served counts converge.
	require.NoError(t, agg.SeedAggregates(ctx, subjectPost, 9, 1))
	up, down, _ = counts(t, database, subjectPost)
	assert.Equal(t, 9, up, "re-seed must heal baseline-voter drift")
	assert.Equal(t, 1, down)
}

// TestSeedAggregatesClampsBaselineAtZero: an origin reporting totals LOWER
// than the bridge's live counts (author auto-upvote asymmetry, an origin
// that lost votes, a hostile API) must clamp the derived baseline at zero
// per direction — never go negative — and the live counts still serve.
func TestSeedAggregatesClampsBaselineAtZero(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 2), voterBob, subjectPost), ""))
	require.NoError(t, agg.SeedAggregates(ctx, subjectPost, 1, 0))

	up, down, _ := counts(t, database, subjectPost)
	assert.Equal(t, 2, up, "live votes serve even when the origin under-reports")
	assert.Equal(t, 0, down)
}

func TestSeedAggregatesUnbridgedSubjectDropped(t *testing.T) {
	database := testDB(t)
	agg, _ := testAggregator(t, database)

	require.NoError(t, agg.SeedAggregates(context.Background(),
		"https://lemmy.world/post/999", 7, 2))
	_, _, found := counts(t, database, "https://lemmy.world/post/999")
	assert.False(t, found)
}

// --- Announced-vote community binding ---
//
// An announced vote (communityIRI != "") counts only when its subject
// belongs to the announcing community; otherwise one malicious followed
// community could skew any bridged subject's score bridge-wide.

func TestAnnouncedVoteCrossCommunitySubjectDropped(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	// The announcer's repo DID differs from the DID of the community repo
	// the post lives in — a vote injected against another community's post.
	followCommunity(t, database, testGroupIRI, otherCommunityDID)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a") // repo DID: testDID
	ctx := context.Background()

	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), testGroupIRI))

	_, _, found := counts(t, database, subjectPost)
	assert.False(t, found, "a cross-community announced vote must not create an aggregate")
	var events int
	require.NoError(t, database.QueryRow(`SELECT COUNT(*) FROM vote_events`).Scan(&events))
	assert.Zero(t, events, "a cross-community announced vote must not record an event")
}

func TestAnnouncedVoteUnknownCommunityDropped(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	ctx := context.Background()

	// The announcer was never registered as a community: no binding is
	// possible, so the vote is dropped (nil, never a retryable error).
	require.NoError(t, agg.ApplyVote(ctx,
		like(activityID(t, 1), voterAlice, subjectPost), "https://lemmy.world/c/unknown"))
	_, _, found := counts(t, database, subjectPost)
	assert.False(t, found)
}

func TestAnnouncedVoteCrossInstanceAuthoredPostCounts(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	followCommunity(t, database, testGroupIRI, testDID)
	// Lemmy hosts a post's AP object on the AUTHOR's instance: a lemmy.zip
	// user's post in a lemmy.world community carries a lemmy.zip AP id. The
	// binding is by community repo DID, not IRI authority, so it must count.
	remotePost := "https://lemmy.zip/post/4242"
	bridgeSubjectAs(t, objects, remotePost, "3jzfcijpj2z4a", testDID, testCollection)
	ctx := context.Background()

	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterBob, remotePost), testGroupIRI))

	up, down, found := counts(t, database, remotePost)
	require.True(t, found, "a cross-instance-authored post in the announcing community must count")
	assert.Equal(t, 1, up)
	assert.Equal(t, 0, down)
}

func TestAnnouncedCommentVoteBindsThreadRootCommunity(t *testing.T) {
	database := testDB(t)
	agg, objects, records := testAggregatorWithRecords(t, database)
	followCommunity(t, database, testGroupIRI, testDID)
	ctx := context.Background()

	// A comment lives in its AUTHOR's repo; its stored record's reply.root
	// names the thread's root post in the community repo. Rooted in the
	// announcing community: counts.
	bridgeSubjectAs(t, objects, subjectComment, "3jzfcijpj2z3a",
		commentAuthorDID, materialize.CollectionComment)
	records.put(commentAuthorDID, materialize.CollectionComment, "3jzfcijpj2z3a", map[string]any{
		"reply": map[string]any{
			"root": map[string]any{
				"uri": "at://" + testDID + "/" + testCollection + "/3jzfcijpj2z2a",
				"cid": testCID,
			},
		},
	})
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectComment), testGroupIRI))
	up, down, found := counts(t, database, subjectComment)
	require.True(t, found)
	assert.Equal(t, 1, up)
	assert.Equal(t, 0, down)

	// Rooted in ANOTHER community's repo: dropped.
	otherComment := "https://lemmy.world/comment/300"
	bridgeSubjectAs(t, objects, otherComment, "3jzfcijpj2z5a",
		commentAuthorDID, materialize.CollectionComment)
	records.put(commentAuthorDID, materialize.CollectionComment, "3jzfcijpj2z5a", map[string]any{
		"reply": map[string]any{
			"root": map[string]any{
				"uri": "at://" + otherCommunityDID + "/" + testCollection + "/3jzfcijpj2z6a",
				"cid": testCID,
			},
		},
	})
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 2), voterAlice, otherComment), testGroupIRI))
	_, _, found = counts(t, database, otherComment)
	assert.False(t, found, "a comment rooted in another community must not be countable via this announcer")
}

func TestAnnouncedRetractCrossCommunityDropped(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	followCommunity(t, database, testGroupIRI, otherCommunityDID)
	bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a") // repo DID: testDID
	ctx := context.Background()

	// The vote lands bare (no announcer binding), then a DIFFERENT community
	// announces the undo: it must not deflate the score.
	likeA := like(activityID(t, 1), voterAlice, subjectPost)
	require.NoError(t, agg.ApplyVote(ctx, likeA, ""))
	require.NoError(t, agg.RetractVote(ctx, likeA, testGroupIRI))

	up, down, _ := counts(t, database, subjectPost)
	assert.Equal(t, 1, up, "a cross-community announced undo must not retract the vote")
	assert.Equal(t, 0, down)
}
