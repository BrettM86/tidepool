package ingest

import (
	"context"
	"database/sql"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TASK 17d — THE DESTRUCTIVE TIER, AND WHAT SEPARATES IT FROM THE SOFT ONE.
//
// The soft tier stops the bridge SPEAKING for someone. The destructive tier
// withdraws what it already said. A user reaches it only by writing
// deleteRemote=true — nothing infers it — because peers that honour a Delete
// cannot restore what they dropped.
//
// Three things have to happen, and each is unreachable from the others:
//
//  1. Delete{Person, removeData:true} to EVERY community inbox this actor's
//     content ever reached. One inbox is not "most of it": the instances that
//     do not receive it keep serving the user's posts forever, and there is no
//     second attempt — asking twice risks deleting content a re-enabled user
//     has since restored.
//  2. An Undo for every vote those peers still hold. A purged actor leaving
//     standing tallies is not cosmetic: Tidepool's aggregate is the
//     fediverse-only tally, and the reseed SUBTRACTS live delivered outbound
//     votes from the API count. A vote nobody can attribute to a living actor
//     is a number the reseed keeps subtracting against, forever, on a subject
//     whose score is served to readers.
//  3. The actor document stops resolving — 410 Gone, not 404. Gone is the
//     statement "this existed and was withdrawn", which is what a peer needs to
//     stop retrying and clean up; 404 reads as "never heard of them", which
//     several implementations treat as a transient lookup failure.
//
// THE PAIRING IS THE POINT. The soft tier keeps serving the document, because
// every Note and Page already delivered names this actor and a broken author
// reference orphans every existing thread. The destructive tier revokes it. Both
// assertions live in this file, on two actors, in one run — the difference
// between the tiers is a status code, and prose is not where that belongs.

const (
	odDestructiveRev = "3lzodrev000100"
	odSoftRev        = "3lzodrev000101"

	odPurgeInARKey = "3lzodpurge0001"
	odPurgeInBRKey = "3lzodpurge0002"
	odSoftRKey     = "3lzodpurge0003"

	// A community on a DIFFERENT INSTANCE. The fixture's two communities are
	// co-hosted and therefore share one inbox — correct for the ban work, and
	// useless here: a fan-out that reaches "every inbox" is indistinguishable
	// from one that reaches the first when there is only one inbox to reach.
	// An erasure request is about INSTANCES, so the fixture needs two.
	odFarCommunity = "https://lemmy.zip/c/purgetest"
	odFarName      = "purgetest"
	odFarRKey      = "3lzodpurge0004"
)

// TestTheDestructiveTierWithdrawsEverythingTheSoftTierKeeps is the outer
// contract for deleteRemote=true.
func TestTheDestructiveTierWithdrawsEverythingTheSoftTierKeeps(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	// --- GIVEN: an author whose content reached TWO instances, holding one
	//     LIVE vote and one already-retracted one.
	admitPost(t, world, mtAuthorDID, odPurgeInARKey, world.communityADID, "3lzodrev000110", 1_775_000_030_000_001)
	admitPost(t, world, mtAuthorDID, odPurgeInBRKey, world.communityBDID, "3lzodrev000111", 1_775_000_030_000_002)

	// ...and one on another instance, which is where "every inbox" starts to
	// mean something.
	h.subscribeCommunityURL(odFarCommunity, odFarName)
	admitPost(t, world, mtAuthorDID, odFarRKey, testDIDFor(odFarName, "lemmy.zip"),
		"3lzodrev000113", 1_775_000_030_000_004)

	inboxes := deliveryInboxesFor(t, h.db, mtAuthorDID)
	require.Len(t, inboxes, 2,
		"precondition: this actor's content reached two distinct inboxes — one is a fan-out "+
			"that cannot be told from a single delivery")

	// A second actor takes the SOFT tier in the same run, so the two outcomes
	// are compared under one set of conditions.
	admitPost(t, world, mtCommenterDID, odSoftRKey, world.communityADID, "3lzodrev000112", 1_775_000_030_000_003)

	liveVote := seedDeliveredVote(t, h.db, mtAuthorDID, mtPostATURI, world.communityADID, "delivered")
	deadVote := seedDeliveredVote(t, h.db, mtAuthorDID, odVoteSubjectATURI, world.communityADID, "undone")

	activitiesBefore := rowCount(t, h.db, "outbound_activities")

	// --- WHEN: the destructive record, and the soft one beside it.
	require.NoError(t, world.dispatcher.HandleEvent(ctx,
		odFederationEvent(t, mtAuthorDID, odDestructiveRev, "create", false, true, 1_775_000_031_000_001)))
	require.NoError(t, world.dispatcher.HandleEvent(ctx,
		odFederationEvent(t, mtCommenterDID, odSoftRev, "create", false, false, 1_775_000_031_000_002)))

	// --- THEN (1): one Delete{Person}, delivered to every inbox they reached.
	deleteActivity := activityOfKind(t, h.db, mtAuthorDID, "Delete")
	require.NotEmpty(t, deleteActivity,
		"a Delete{Person} must be enqueued: the soft tier tells peers nothing, and this tier "+
			"is defined by telling them — a destructive opt-out that sends no activity is a "+
			"soft opt-out the user did not choose")
	assert.ElementsMatch(t, inboxes, deliveryInboxesForActivity(t, h.db, deleteActivity),
		"and it reaches EVERY inbox this actor's content ever reached: the instances it "+
			"misses keep serving their posts, and no second attempt is coming — this is the "+
			"fan-out the enqueuer's idempotency shortcut silently truncates to one")

	// --- THEN (2): an Undo for the vote peers still hold, and only that one.
	assert.Equal(t, 1, undoActivitiesFor(t, h.db, mtAuthorDID),
		"exactly ONE Undo: for the vote still standing on a peer, and NOT for the one already "+
			"retracted. A purged actor's live vote is a number the reseed keeps subtracting "+
			"from a served score forever; a second Undo for a vote nobody holds is an activity "+
			"the peer cannot match to anything")
	assert.Equal(t, "undone", voteState(t, h.db, liveVote),
		"and the live vote's own state says so afterwards, or the next reseed subtracts it again")
	assert.Equal(t, "undone", voteState(t, h.db, deadVote), "while the retracted one is unchanged")

	assert.Greater(t, rowCount(t, h.db, "outbound_activities"), activitiesBefore,
		"the tier really did enqueue: the assertions above must not be satisfied by silence")

	// --- THEN (3): the actor document is GONE, and the soft actor's is not.
	assert.Equal(t, http.StatusGone, actorDocStatus(t, h, mtAuthorDID),
		"the purged actor's document returns 410 GONE: the user asked to be withdrawn, and a "+
			"document that still resolves invites peers to keep re-fetching an identity we "+
			"promised to retract. 404 is the wrong answer — 'never heard of them' reads as a "+
			"lookup failure, while Gone is the statement that lets a peer stop asking")

	assert.Equal(t, http.StatusOK, actorDocStatus(t, h, mtCommenterDID),
		"while the SOFT opt-out's document still resolves: every Note and Page already "+
			"delivered names that actor, and revoking it orphans the author reference on every "+
			"existing thread. This is the whole difference between the tiers, and it is one "+
			"status code")
}

// odVoteSubjectATURI is a second subject to hang the already-retracted vote on.
const odVoteSubjectATURI = "at://" + mtAuthorDID + "/social.coves.community.postv2/3lzodvotesub01"

// seedDeliveredVote writes the outbound vote state a purge has to act on. The
// delivered/undone distinction is the whole question — "live" means a peer still
// holds it — and driving a real delivery would test the worker instead.
func seedDeliveredVote(t *testing.T, db *sql.DB, actorDID, subjectATURI, communityDID, state string) string {
	t.Helper()
	voteATURI := "at://" + actorDID + "/social.coves.feed.vote/" + state + "-vote"
	_, err := db.ExecContext(context.Background(), `
		INSERT INTO outbound_votes (
			vote_at_uri, actor_did, subject_at_uri, subject_ap_id, community_did,
			direction, current_activity_id, delivered_state)
		VALUES ($1, $2, $3, $4, $5, 'up', $6, $7)`,
		voteATURI, actorDID, subjectATURI, mtUserOrigin+"/ap/object/"+subjectATURI,
		communityDID, mtUserOrigin+"/ap/activity/"+state+"-vote", state)
	require.NoError(t, err, "seed a %s vote for %s", state, actorDID)
	return voteATURI
}

func voteState(t *testing.T, db *sql.DB, voteATURI string) string {
	t.Helper()
	var state string
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT delivered_state FROM outbound_votes WHERE vote_at_uri = $1`, voteATURI).Scan(&state))
	return state
}

// deliveryInboxesFor lists the distinct inboxes an actor's content has reached —
// the delivery history the fan-out is built from. No production query answers
// this today; that is new surface the tier needs.
func deliveryInboxesFor(t *testing.T, db *sql.DB, actorDID string) []string {
	t.Helper()
	return queryStrings(t, db, `
		SELECT DISTINCT d.target_inbox
		FROM outbound_deliveries d
		JOIN outbound_activities a ON a.activity_id = d.activity_id
		WHERE a.actor_did = $1`, actorDID)
}

func deliveryInboxesForActivity(t *testing.T, db *sql.DB, activityID string) []string {
	t.Helper()
	return queryStrings(t,
		db, `SELECT target_inbox FROM outbound_deliveries WHERE activity_id = $1`, activityID)
}

// activityOfKind returns one activity id of the given kind for an actor, or "".
func activityOfKind(t *testing.T, db *sql.DB, actorDID, kind string) string {
	t.Helper()
	ids := queryStrings(t, db,
		`SELECT activity_id FROM outbound_activities WHERE actor_did = $1 AND kind = $2`,
		actorDID, kind)
	if len(ids) == 0 {
		return ""
	}
	require.Len(t, ids, 1,
		"one %s activity for %s: the erasure is ONE request fanned out, not one per peer",
		kind, actorDID)
	return ids[0]
}

func undoActivitiesFor(t *testing.T, db *sql.DB, actorDID string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM outbound_activities WHERE actor_did = $1 AND kind = 'Undo'`,
		actorDID).Scan(&n))
	return n
}

// actorDocStatus fetches the persona's actor document from the origin the
// bridge really serves, and reports the status. It goes over HTTP on purpose:
// what a peer receives is the contract, and a store flag that no handler reads
// would satisfy any assertion made against the database instead.
func actorDocStatus(t *testing.T, h *harness, did string) int {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
		"http://"+h.fixtures.Listener.Addr().String()+"/ap/actor/"+did, nil)
	require.NoError(t, err)
	req.Host = "coves.social"
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	return resp.StatusCode
}

func queryStrings(t *testing.T, db *sql.DB, query string, args ...any) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), query, args...)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var out []string
	for rows.Next() {
		var value string
		require.NoError(t, rows.Scan(&value))
		out = append(out, value)
	}
	require.NoError(t, rows.Err())
	return out
}
