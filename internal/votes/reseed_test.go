package votes

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// TASK 17b — WHAT THE SERVED AGGREGATE MEANS.
//
// Coves keeps native and bridged tallies in SEPARATE columns
// (Coves' bridged_upvote_count column), so Tidepool's aggregate is the
// FEDIVERSE-ONLY tally:
//
//	served(subject) = api_total(subject) − { our personas' votes Lemmy currently holds }
//
// The origin's API total includes the votes TIDEPOOL ITSELF wrote back on
// behalf of native users. SeedAggregates nets out live INBOUND events (so a
// federated vote is not counted in both the baseline and the live term) but
// knows nothing about our own outbound votes — so a native user's vote is
// counted twice: once inside Coves' native column, once inside the bridged one.
//
// THE PREDICATE IS delivered_state = 'delivered', a POSITIVE EQUALITY:
//
//	pending (first delivery, retrying, or poisoned) → Lemmy does not hold it
//	delivered                                       → Lemmy holds it: subtract
//	delivered + Undo in flight                      → still subtract; applyVoteDelete
//	                                                  re-upserts 'delivered' precisely
//	                                                  because the peer has not processed
//	                                                  the withdrawal yet
//	row deleted (Undo delivered)                    → nothing to subtract
//	delivered + POISONED Undo                       → subtract forever, which is why
//	                                                  decision 16 bans queue-history
//	                                                  arithmetic ("delivered Likes minus
//	                                                  delivered Undos" gets this row
//	                                                  permanently wrong)
//
// A negation ("not undone", "!= pending") reads the same today only because
// 'undone' is never written; if a future policy starts writing it, it will mean
// THE PEER ACCEPTED THE WITHDRAWAL — not live — and every negation silently
// inverts while the equality stays correct.
const (
	rsPersonaDID   = "did:plc:reseedpersona0001"
	rsPersonaActor = "https://coves.social/ap/actor/" + rsPersonaDID
	rsVoteATURI    = "at://" + rsPersonaDID + "/social.coves.interaction.vote/3lzreseedvote1"
	rsCommunityDID = "did:plc:reseedcommunity01"
	rsSubjectRKey  = "3lzreseedpost1"
)

// reseedDB is testDB under a name that says what these fixtures are about.
func reseedDB(t *testing.T) *sql.DB {
	t.Helper()
	return testDB(t)
}

// seededCounts reads the stored BASELINE (as opposed to the served total).
func seededCounts(t *testing.T, database *sql.DB, subject string) (up, down int) {
	t.Helper()
	require.NoError(t, database.QueryRow(`
		SELECT seeded_upvotes, seeded_downvotes FROM vote_aggregates WHERE subject_ap_id = $1`,
		subject).Scan(&up, &down))
	return up, down
}

// TestSeedNetsOurDeliveredVotesPerDirection is the OUTER CONTRACT for 17b.
//
// GIVEN a bridged Lemmy post carrying BOTH subtrahends at once — one live
// INBOUND up-vote from a Lemmy human, and one DELIVERED outbound DOWN-vote from
// one of our native personas — WHEN the origin API reports 3 up / 2 down and the
// seed runs, THEN the SERVED total is 3 up / 1 down.
//
// Every number is distinct and the two directions are unequal ON PURPOSE. The
// symmetric fixture (one inbound up, one outbound up, api 2/0) passes under at
// least three WRONG implementations — subtracting totals instead of per
// direction, bucketing the outbound subtrahend into `up` unconditionally, and
// double-subtracting then clamping at zero — so it proves nothing. This is the
// MIXED-POPULATION subject: inbound and outbound, both directions, unequal
// counts, which nobody writes because each half already has its own passing
// test.
func TestSeedNetsOurDeliveredVotesPerDirection(t *testing.T) {
	database := reseedDB(t)
	agg, objects := testAggregator(t, database)
	ctx := context.Background()
	subjectATURI := bridgeSubject(t, objects, subjectPost, rsSubjectRKey)

	// --- Subtrahend 1: a Lemmy human's live inbound UP-vote. It is in the
	//     origin's total AND in vote_events, so the baseline must net it out or
	//     the recompute counts it twice. (This half already works.)
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))

	// --- Subtrahend 2: our own persona's DOWN-vote, delivered. It is in the
	//     origin's total and nowhere else — Coves counts it in its NATIVE
	//     column, so leaving it in the bridged tally counts one user's single
	//     vote twice across the two columns.
	_, err := store.NewAPActors(database).Create(ctx, store.APActor{
		DID:              rsPersonaDID,
		Kind:             store.ActorTypePerson,
		ActorID:          rsPersonaActor,
		NormalizedOrigin: "coves.social",
		LocalPart:        "reseedpersona",
		RSAKeySealed:     []byte{0x01, 0x02, 0x03},
		RSAKeyVersion:    1,
		PublicKeyPEM:     "-----BEGIN PUBLIC KEY-----\nMIIB\n-----END PUBLIC KEY-----\n",
	})
	require.NoError(t, err, "mint the persona whose vote Lemmy is holding")

	outboundVotes := store.NewOutboundVotes(database)
	// Written the way the write path writes it: the consumer records INTENT as
	// pending, and only delivery success flips the state — a row that starts
	// life 'delivered' is a state no code path produces.
	_, err = outboundVotes.Upsert(ctx, store.OutboundVote{
		VoteATURI:         rsVoteATURI,
		ActorDID:          rsPersonaDID,
		SubjectATURI:      subjectATURI,
		SubjectAPID:       subjectPost,
		CommunityDID:      rsCommunityDID,
		Direction:         directionDown,
		CurrentActivityID: "https://coves.social/ap/activity/reseed-dislike",
		DeliveredState:    store.DeliveredStatePending,
	})
	require.NoError(t, err)
	require.NoError(t, outboundVotes.SetDeliveredState(ctx, rsVoteATURI, store.DeliveredStateDelivered),
		"delivery success is what makes Lemmy the holder of this vote")

	// --- The origin's public API, which counts BOTH of the above.
	require.NoError(t, agg.SeedAggregates(ctx, subjectPost, 3, 2))

	// --- THE CONTRACT: the served total is the fediverse-only tally.
	up, down, found := counts(t, database, subjectPost)
	require.True(t, found, "the seed must create the aggregate")
	assert.Equal(t, 3, up,
		"UP: the origin's 3 includes the Lemmy human's live vote, which the baseline nets "+
			"out and the recompute adds back — 3 stays 3, and no outbound vote may be "+
			"subtracted from a direction it was never cast in")
	assert.Equal(t, 1, down,
		"DOWN: the origin's 2 includes OUR persona's delivered down-vote. Coves already "+
			"counts that vote in its native column, so the bridged tally must be 1 — "+
			"leaving it at 2 counts one person's one vote twice in the UI")

	// --- Secondary (the contract is the served number above): where the
	//     subtraction lands. The baseline is its natural home — it is computed
	//     once per seed, while the served total is recomputed on every single
	//     inbound vote.
	seededUp, seededDown := seededCounts(t, database, subjectPost)
	assert.Equal(t, 2, seededUp, "3 origin − 1 live inbound = 2 baseline up")
	assert.Equal(t, 1, seededDown, "2 origin − 0 live inbound − 1 delivered outbound = 1 baseline down")

	// --- And it SURVIVES the next recompute. recomputeAggregate runs on every
	//     ApplyVote, so a fix that patched the served columns after the seed
	//     instead of the baseline would be silently undone by the next vote to
	//     arrive — minutes later, with nothing to connect the drift to the seed.
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 2), voterBob, subjectPost), ""))
	up, down, _ = counts(t, database, subjectPost)
	assert.Equal(t, 4, up, "a second Lemmy human's up-vote stacks on the baseline")
	assert.Equal(t, 1, down,
		"and our persona's vote stays subtracted: the subtraction must live somewhere a "+
			"recompute preserves, not in the served columns it overwrites")
}
