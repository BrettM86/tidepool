package ingest

import (
	"context"
	"database/sql"
	"expvar"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/accept"
	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/testutil"
)

// TASK 17c-3 — A COMMUNITY BAN IS THREE THINGS AT ONCE.
//
// A ban is not one fact, it is an intersection: THIS author, in THIS community.
// Everything that makes it hard follows from that, and so does every way of
// getting it wrong, because the two obvious implementations are already in the
// codebase and both are one dimension short:
//
//	CancelForActor(did)          — cancels the author EVERYWHERE. One community
//	                               bans them; every other community they write
//	                               to stops receiving their posts.
//	CancelForCommunity(key)      — cancels EVERYONE in that community. One user
//	                               is banned; the community goes dark.
//
// A fixture with one community passes the first. A fixture with one actor
// passes the second. Only a world with TWO CO-HOSTED COMMUNITIES and TWO NATIVE
// ACTORS can tell any of them apart — and co-hosted matters twice over, because
// communities on one Lemmy instance SHARE AN INBOX, so nothing about the
// delivery address distinguishes A's traffic from B's. The scope has to come
// from the ordering key, which is the community's own AP id.
//
// This is also why community B is now minted for real: an acceptance is a record
// in B's own repo, and "still accepted in B" cannot be asserted by a community
// that has no repo to accept into.
const (
	mbBlockActivity   = "https://lemmy.world/activities/announce/block/mb-ban"
	mbUnblockActivity = "https://lemmy.world/activities/announce/undo/mb-ban"

	// The banned author's posts: one to A, one to B, one to A after the ban,
	// and one to A after the ban is lifted.
	mbPostInARKey       = "3lzmbpost00001"
	mbPostInBRKey       = "3lzmbpost00002"
	mbPostAfterBanRKey  = "3lzmbpost00003"
	mbPostAfterUndoRKey = "3lzmbpost00004"
	// The OTHER actor's post to A: the same community, a different author.
	mbOtherPostRKey = "3lzmbpost00005"
)

func mbPostATURI(did, rkey string) string {
	return "at://" + did + "/" + materialize.CollectionPostV2 + "/" + rkey
}

// TestABanIsScopedToOneAuthorInOneCommunity is the OUTER CONTRACT for 17c-3.
//
// GIVEN a native author with accepted posts in community A and in community B,
// and a second author with an accepted post in A, WHEN A announces a Block
// naming the first author, THEN the ban is recorded against A, that author's
// pending deliveries TO A are cancelled while their deliveries to B and the
// other author's deliveries to A are untouched, their next post to A is REJECTED
// at admission as author-banned while their post to B is still accepted — and
// WHEN A announces Undo{Block}, admission to A resumes.
func TestABanIsScopedToOneAuthorInOneCommunity(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	resetBanState(t, h.db)
	world := newModerationWorld(t, h)
	communityBAPID := mtCommunityBAPID

	// --- GIVEN: three accepted posts across two communities and two authors.
	//     Every one of them is pending delivery — nothing runs the worker here,
	//     which is what makes "cancelled" a visible state change rather than a
	//     race with a delivery that already went out.
	admitPost(t, world, mtAuthorDID, mbPostInARKey, world.communityADID, "3lzmbrev00001", 1_775_000_001_000_001)
	admitPost(t, world, mtAuthorDID, mbPostInBRKey, world.communityBDID, "3lzmbrev00002", 1_775_000_001_000_002)
	admitPost(t, world, mtCommenterDID, mbOtherPostRKey, world.communityADID, "3lzmbrev00003", 1_775_000_001_000_003)

	requireEveryDelivery(t, h.db, mtAuthorDID, groupID, "pending",
		"precondition: the banned-to-be author has pending work for A")
	requireEveryDelivery(t, h.db, mtAuthorDID, communityBAPID, "pending",
		"precondition: and pending work for B")
	requireEveryDelivery(t, h.db, mtCommenterDID, groupID, "pending",
		"precondition: and the OTHER author has pending work for A")

	activitiesBefore := rowCount(t, h.db, "outbound_activities")
	deliveriesBefore := rowCount(t, h.db, "outbound_deliveries")
	dropsBefore := dropSnapshot()

	// --- WHEN: community A bans the author. Lemmy's shape: the inner Block is
	//     the MODERATOR's, targeted at the community, announced by the group.
	h.announceBlock(world.groupA, mbBlockActivity, mtAuthorDID, groupID, nil)

	// The echo classifier must not have taken it: a Block's `object` is the
	// banned actor, and for a native author that actor is OURS by definition.
	// Adding Block to carriesPayload would drop every ban the bridge receives
	// and count each one as a suppression.
	assert.Equal(t, dropsBefore, dropSnapshot(),
		"an announced Block naming our own persona is GENUINE remote traffic: the id it "+
			"names is ours precisely because the community is moderating our user")

	// --- THEN: the cancellation is the INTERSECTION, not either axis alone.
	requireEveryDelivery(t, h.db, mtAuthorDID, groupID, "cancelled",
		"the banned author's PENDING work for A is cancelled: Lemmy rejects a banned user's "+
			"posts, so every one of these is a delivery that fails, retries and poisons for a "+
			"reason nothing in the queue names")
	requireEveryDelivery(t, h.db, mtAuthorDID, communityBAPID, "pending",
		"but their work for B is UNTOUCHED: one community's moderators do not decide where "+
			"an author may speak — cancelling by actor alone silently unpublishes them across "+
			"every community they belong to")
	requireEveryDelivery(t, h.db, mtCommenterDID, groupID, "pending",
		"and the other author's work for A is UNTOUCHED: cancelling by community alone takes "+
			"the whole community dark over one user's ban")

	assert.Equal(t, activitiesBefore, rowCount(t, h.db, "outbound_activities"),
		"the ban itself enqueues NOTHING: an inbound moderation action is the community "+
			"telling US what it did, and echoing it back is an activity aimed at the "+
			"moderators who sent it")
	assert.Equal(t, deliveriesBefore, rowCount(t, h.db, "outbound_deliveries"),
		"and cancels rather than deletes: the row is the evidence, and a delivery that "+
			"vanishes leaves an operator no way to see why the author's post never arrived")

	// --- AND: the ban is recorded, bound to A — the durable half, which is what
	//     stops the SECOND post rather than the ones already queued.
	ban, found := banFor(t, h.db, world.communityADID, mtAuthorDID)
	require.True(t, found,
		"the ban must be RECORDED: it is the only thing standing between a banned author "+
			"and their next post, and Lemmy will not re-send it — a ban the bridge does not "+
			"hold is one that stops nothing from the second post onward")
	assert.Equal(t, groupID, ban.communityAPID,
		"with the community's AP id denormalized: the delivery side holds an ordering key, "+
			"not a DID, and a join it cannot make is a scope it cannot apply")
	assert.False(t, ban.expires.Valid,
		"and no expiry, because this Block named none — a permanent ban")

	// --- AND: admission refuses their next post to A, and only to A.
	admitPost(t, world, mtAuthorDID, mbPostAfterBanRKey, world.communityADID, "3lzmbrev00004", 1_775_000_001_000_004)
	bannedURI := mbPostATURI(mtAuthorDID, mbPostAfterBanRKey)
	status, code := admissionFor(t, h.db, world.communityADID, bannedURI)
	assert.Equal(t, accept.StatusRejected, status,
		"a banned author's post is REJECTED at admission: accepting it would sign the "+
			"community's name to content from someone that community has excluded")
	assert.Equal(t, "author-banned", code,
		"with the reason a reader can act on — the same vocabulary the removal lexicon "+
			"already spells, so post.getStatus and the admin surface answer 'why' without a "+
			"second dictionary")
	_, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, testDigestRKey(bannedURI))
	assert.True(t, errors.IsNotFound(err),
		"and no acceptance is written for it (err=%v)", err)

	// The same author, the same moment, the OTHER community.
	admitPost(t, world, mtAuthorDID, mbPostInBRKey+"b", world.communityBDID, "3lzmbrev00005", 1_775_000_001_000_005)
	_, _, err = h.manager.GetRecord(ctx, world.communityBDID, materialize.CollectionAcceptance,
		testDigestRKey(mbPostATURI(mtAuthorDID, mbPostInBRKey+"b")))
	assert.NoError(t, err,
		"while B still accepts them: a ban is a community's decision about its own space, "+
			"and one that follows the author off it is a site ban nobody issued")

	// --- WHEN: the moderators lift it.
	h.announceUndoBlock(world.groupA, mbUnblockActivity, mbBlockActivity+"/block",
		mtAuthorDID, groupID)

	_, stillBanned := banFor(t, h.db, world.communityADID, mtAuthorDID)
	assert.False(t, stillBanned,
		"Undo{Block} clears the ban: Lemmy sends this activity exactly once, so a ban that "+
			"survives it can never be lifted by anything")

	admitPost(t, world, mtAuthorDID, mbPostAfterUndoRKey, world.communityADID, "3lzmbrev00006", 1_775_000_001_000_006)
	restoredURI := mbPostATURI(mtAuthorDID, mbPostAfterUndoRKey)
	status, _ = admissionFor(t, h.db, world.communityADID, restoredURI)
	assert.Equal(t, accept.StatusAccepted, status,
		"and admission resumes: an unbanned author posting again is the ordinary case, and "+
			"a ban that outlives its Undo is indistinguishable to the author from being "+
			"silently shadowbanned")
	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, testDigestRKey(restoredURI))
	assert.NoError(t, err, "with the acceptance to prove it (err=%v)", err)
}

// admitPost drives one native post into one community through the real
// consumer, engine and enqueuer. It asserts only that the EVENT was handled —
// whether the post was accepted or rejected is what the caller is testing.
func admitPost(t *testing.T, world moderationWorld, did, rkey, communityDID, rev string, timeUS int64) {
	t.Helper()
	require.NoError(t, world.dispatcher.HandleEvent(context.Background(),
		mtPostEventBy(t, did, rkey, communityDID, "create", rev, mtPostCID, timeUS)))
}

// announceBlock delivers Lemmy's ban shape: Announce{Block} from the community,
// whose INNER Block is attributed to the acting MODERATOR (a Person, never the
// Group — which is why only the ANNOUNCED path can satisfy decision 18), names
// the banned actor as its object, and TARGETS the community the ban applies to.
func (h *harness) announceBlock(group *remoteActor, activityID, subjectDID, target string, extra map[string]any) {
	h.t.Helper()
	require.Equal(h.t, http.StatusAccepted, h.deliver(group, map[string]any{
		"id":       activityID,
		"type":     "Announce",
		"actor":    group.id,
		"audience": group.id,
		"cc":       []any{group.id + "/followers"},
		"object":   blockActivity(group, activityID+"/block", subjectDID, target, extra),
	}))
	h.drain()
}

// announceUndoBlock delivers the unban shape: Announce{Undo{Block}} with the
// Block carried INLINE.
func (h *harness) announceUndoBlock(group *remoteActor, activityID, blockActivityID, subjectDID, target string) {
	h.t.Helper()
	require.Equal(h.t, http.StatusAccepted, h.deliver(group, map[string]any{
		"id":       activityID,
		"type":     "Announce",
		"actor":    group.id,
		"audience": group.id,
		"cc":       []any{group.id + "/followers"},
		"object": map[string]any{
			"id":       activityID + "/undo",
			"type":     "Undo",
			"actor":    modActorID,
			"audience": group.id,
			"cc":       []any{group.id},
			"object":   blockActivity(group, blockActivityID, subjectDID, target, nil),
		},
	}))
	h.drain()
}

func blockActivity(group *remoteActor, activityID, subjectDID, target string, extra map[string]any) map[string]any {
	block := map[string]any{
		"id":       activityID,
		"type":     "Block",
		"actor":    modActorID,
		"object":   mtUserOrigin + "/ap/actor/" + subjectDID,
		"target":   target,
		"audience": group.id,
		"to":       []any{ap.PublicAudience},
		"cc":       []any{group.id},
	}
	for key, value := range extra {
		block[key] = value
	}
	return block
}

// communityBan is the bridge's record of one (community, author) ban.
type communityBan struct {
	communityAPID string
	expires       sql.NullTime
}

func banFor(t *testing.T, db *sql.DB, communityDID, subjectDID string) (communityBan, bool) {
	t.Helper()
	if !tableExists(t, db, "community_bans") {
		// A table that does not exist holds no bans, and saying so is not a
		// tolerance: every assertion in this file that REQUIRES a ban fails
		// loudly right here, so a table that is missing — or landed under
		// another name — is caught by the positive tests rather than hidden by
		// the negative ones. What it buys is that each test fails on ITS OWN
		// behaviour instead of six tests reporting one missing relation.
		return communityBan{}, false
	}
	var ban communityBan
	err := db.QueryRowContext(context.Background(), `
		SELECT community_ap_id, expires_at
		FROM community_bans
		WHERE community_did = $1 AND subject_did = $2`,
		communityDID, subjectDID).Scan(&ban.communityAPID, &ban.expires)
	if err == sql.ErrNoRows {
		return communityBan{}, false
	}
	require.NoError(t, err, "read the ban state for %s in %s", subjectDID, communityDID)
	return ban, true
}

// requireEveryDelivery asserts that the actor HAS work on that community's
// ordering key and that every row of it is in the wanted state.
//
// The non-empty check is not ceremony. "Every one of no rows is cancelled" is
// true of a cancellation that DELETED the rows, of a fixture whose posts never
// enqueued, and of a query with a typo in it — three different nothings that all
// read as success. The rows are also the evidence an operator needs afterwards,
// so their continued existence is part of the behaviour, not an artifact of it.
func requireEveryDelivery(t *testing.T, db *sql.DB, actorDID, orderingKey, want, why string) {
	t.Helper()
	states := deliveryStates(t, db, actorDID, orderingKey)
	require.NotEmpty(t, states,
		"%s — and there must BE deliveries to say that about: %s has no rows on %s at all",
		why, actorDID, orderingKey)
	for i, state := range states {
		require.Equal(t, want, state, "%s (delivery %d of %d)", why, i+1, len(states))
	}
}

// tableExists reports whether a table has been created yet.
func tableExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var relation sql.NullString
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT to_regclass($1)`, "public."+name).Scan(&relation))
	return relation.Valid
}

// resetBanState clears the ban table between runs.
//
// newHarness's truncate list cannot name it until it exists, and it MUST be
// cleared: the fixture's author and community are package-level constants, so a
// ban left by one run would refuse the next run's post before the test that
// issues the ban has run — green first, red second, which a single CI run never
// sees. Fold this into newHarness and delete it once the migration lands.
func resetBanState(t *testing.T, db *sql.DB) {
	t.Helper()
	if tableExists(t, db, "community_bans") {
		testutil.Truncate(t, db, "community_bans")
	}
}

// deliveryStates lists the states of one actor's deliveries on one community's
// ordering key.
//
// The ordering key is what makes a ban expressible at all: co-hosted communities
// SHARE an inbox, so the delivery address says nothing about which community's
// traffic a row carries. A cancellation scoped by inbox would take both.
func deliveryStates(t *testing.T, db *sql.DB, actorDID, orderingKey string) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `
		SELECT d.state
		FROM outbound_deliveries d
		JOIN outbound_activities a ON a.activity_id = d.activity_id
		WHERE a.actor_did = $1 AND d.ordering_key = $2
		ORDER BY d.seq`, actorDID, orderingKey)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()

	var states []string
	for rows.Next() {
		var state string
		require.NoError(t, rows.Scan(&state))
		states = append(states, state)
	}
	require.NoError(t, rows.Err())
	return states
}

// TestADirectBlockIsIgnored pins the DECIDED non-action at the other door.
//
// Lemmy sends a ban twice: announced through the community, and delivered
// DIRECTLY to the banned user's inbox. The two are not redundant copies of one
// decision — they are signed by different actors, and only one of them can be
// authorized.
//
// BlockUser's `actor` is the MODERATOR's Person. On the announced path the HTTP
// signature binds the community Group, so decision 18's conjunction (the signer
// IS the community that owns the target) can hold. On the direct path the signer
// is a Person, so that conjunction CANNOT pass by construction — no amount of
// implementation makes a person into a group. Refusing it is therefore not a gap
// left open, it is the only correct outcome, and it must be a visible decision
// rather than a silent drop: a ban that arrives only by the path we ignore looks
// exactly like a ban that never arrived.
func TestADirectBlockIsIgnored(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	resetBanState(t, h.db)
	world := newModerationWorld(t, h)

	// The moderator as a real signer: their own key, their own actor document.
	moderator := h.newRemoteActor(modActorID, person(modActorID, "moderator", nil))
	before := metricValue(mbDirectIgnoredMetric)

	const directBlock = "https://lemmy.world/activities/block/mb-direct"
	require.Equal(t, http.StatusAccepted, h.deliver(moderator,
		blockActivity(world.groupA, directBlock, mtAuthorDID, groupID, nil)))
	h.drain()

	_, found := banFor(t, h.db, world.communityADID, mtAuthorDID)
	assert.False(t, found,
		"a Person-signed Block records NO ban: the announced copy is the authoritative one, "+
			"and honouring this path would let any account on a Lemmy instance ban a native "+
			"author out of any community co-hosted there")

	assert.Equal(t, before+1, metricValue(mbDirectIgnoredMetric),
		"and it is COUNTED: a decided non-action that is invisible reads exactly like a ban "+
			"we never received, so the day the announced path breaks, this counter is the only "+
			"thing that distinguishes 'ignored on purpose' from 'silently lost'")

	event, err := h.events.GetEvent(ctx, directBlock)
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt,
		"skipped ONCE, not left retrying: nothing about this delivery improves on a second "+
			"attempt, and a wedged ordering key would hold every later activity behind it: %s",
		event.Error)
	assert.Nil(t, event.FailedAt, "nor poisoned")
}

// TestADirectBlockClaimingToBeTheGroupIsStillIgnored is SEC-1's fix doing work
// in a new place.
//
// The activity CLAIMS `actor` = the community Group while being SIGNED by a
// moderator's Person. Both live on lemmy.world, so the same-authority tolerance
// at the door lets the delivery in — and before SEC-1 the CLAIM was what got
// bound, which would have made this indistinguishable from the community's own
// announced ban. It is the exact shape that was exploitable three sub-runs ago,
// arriving now at a verb that hands out bans.
func TestADirectBlockClaimingToBeTheGroupIsStillIgnored(t *testing.T) {
	h := newHarness(t)
	resetBanState(t, h.db)
	world := newModerationWorld(t, h)
	moderator := h.newRemoteActor(modActorID, person(modActorID, "moderator", nil))

	const forged = "https://lemmy.world/activities/block/mb-forged"
	block := blockActivity(world.groupA, forged, mtAuthorDID, groupID, nil)
	block["actor"] = groupID // the claim: "I am the community"

	require.Equal(t, http.StatusAccepted, h.deliver(moderator, block))
	h.drain()

	_, found := banFor(t, h.db, world.communityADID, mtAuthorDID)
	assert.False(t, found,
		"the VERIFIED SIGNER decides, never the claim: binding the claimed actor would let "+
			"any account on the instance speak as the community — here, to ban a native author "+
			"from a community whose moderators did nothing")

	admitPost(t, world, mtAuthorDID, mbPostAfterBanRKey, world.communityADID,
		"3lzmbrev00010", 1_775_000_002_000_001)
	status, _ := admissionFor(t, h.db, world.communityADID, mbPostATURI(mtAuthorDID, mbPostAfterBanRKey))
	assert.Equal(t, accept.StatusAccepted, status,
		"and the author keeps posting: a forged ban that only half-applied would be a "+
			"shadowban nobody issued and nobody can lift")
}

// TestALapsedBanDoesNotRefuseAdmission is the half of `expires` that has no
// activity behind it.
//
// Lemmy's BlockUser carries `expires` for a temporary ban, and when that ban
// lapses Lemmy sends NOTHING — no Undo, no second activity, nothing. The ban
// simply stops applying on their side. So an implementation that stores the ban
// and ignores the column turns every 3-day ban into a permanent one, and there
// is no message that will ever clear it: the author is excluded forever by a
// moderator who chose three days.
func TestALapsedBanDoesNotRefuseAdmission(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	resetBanState(t, h.db)
	world := newModerationWorld(t, h)

	h.announceBlock(world.groupA, mbBlockActivity, mtAuthorDID, groupID,
		map[string]any{"expires": "2020-01-01T00:00:00Z"})

	if ban, found := banFor(t, h.db, world.communityADID, mtAuthorDID); found {
		require.True(t, ban.expires.Valid,
			"a ban recorded from an activity carrying `expires` must carry the expiry with it: "+
				"dropping the column is what makes the lapse unrepresentable")
		assert.True(t, ban.expires.Time.Before(timeNow()),
			"and it must be the moment the moderator chose, in the past")
	}

	admitPost(t, world, mtAuthorDID, mbPostAfterBanRKey, world.communityADID,
		"3lzmbrev00011", 1_775_000_003_000_001)
	postURI := mbPostATURI(mtAuthorDID, mbPostAfterBanRKey)
	status, code := admissionFor(t, h.db, world.communityADID, postURI)
	assert.Equal(t, accept.StatusAccepted, status,
		"a LAPSED ban must not refuse admission: every read has to be `expires_at IS NULL OR "+
			"expires_at > now()`, because no Undo is coming — the expiry IS the lift")
	assert.NotEqual(t, "author-banned", code, "and certainly not for being banned")

	_, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, testDigestRKey(postURI))
	assert.NoError(t, err, "with the acceptance to prove it (err=%v)", err)
}

// TestAStandingTimedBanRefusesAdmission is the other half: the same activity
// shape, an expiry that has NOT arrived, and a ban that bites.
//
// The pair is the point. An implementation that reads the column as "any expiry
// means expired" passes the lapsed test alone; one that ignores it passes this
// one alone. Only both together say the column is being read.
func TestAStandingTimedBanRefusesAdmission(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	resetBanState(t, h.db)
	world := newModerationWorld(t, h)

	h.announceBlock(world.groupA, mbBlockActivity, mtAuthorDID, groupID,
		map[string]any{"expires": "2099-01-01T00:00:00Z"})

	ban, found := banFor(t, h.db, world.communityADID, mtAuthorDID)
	require.True(t, found, "a temporary ban is still a ban and must be recorded")
	require.True(t, ban.expires.Valid, "carrying its expiry")
	assert.True(t, ban.expires.Time.After(timeNow()), "which has not arrived")

	admitPost(t, world, mtAuthorDID, mbPostAfterBanRKey, world.communityADID,
		"3lzmbrev00012", 1_775_000_004_000_001)
	postURI := mbPostATURI(mtAuthorDID, mbPostAfterBanRKey)
	status, code := admissionFor(t, h.db, world.communityADID, postURI)
	assert.Equal(t, accept.StatusRejected, status,
		"a ban whose expiry is still ahead applies exactly like a permanent one: a temporary "+
			"ban that admits posts is not a ban at all")
	assert.Equal(t, "author-banned", code, "with the same reason")

	_, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, testDigestRKey(postURI))
	assert.True(t, errors.IsNotFound(err), "and no acceptance (err=%v)", err)
}

// TestACrossCommunityBlockIsRefused is the RELATIONAL case for bans.
//
// Community B announces a Block whose TARGET is community A. B is followed, B
// signs as itself, and B shares an instance with A — nothing about the delivery
// is malformed. It is simply not B's decision: a ban is a community's ruling
// about its OWN space, and one community handing out exclusions from another's
// is the co-hosting failure decision 18 exists to prevent.
func TestACrossCommunityBlockIsRefused(t *testing.T) {
	h := newHarness(t)
	resetBanState(t, h.db)
	world := newModerationWorld(t, h)

	h.announceBlock(world.groupB, "https://lemmy.world/activities/announce/block/mb-cross",
		mtAuthorDID, groupID, nil)

	_, found := banFor(t, h.db, world.communityADID, mtAuthorDID)
	assert.False(t, found,
		"B may not ban an author out of A: one moderator team would otherwise be able to "+
			"exclude anyone from every community co-hosted with theirs")

	_, foundInB := banFor(t, h.db, world.communityBDID, mtAuthorDID)
	assert.False(t, foundInB,
		"nor may it be recorded against B instead: B announced a ruling about A's space, and "+
			"quietly applying it to B's own invents an exclusion B never issued")

	admitPost(t, world, mtAuthorDID, mbPostAfterBanRKey, world.communityADID,
		"3lzmbrev00013", 1_775_000_005_000_001)
	status, _ := admissionFor(t, h.db, world.communityADID, mbPostATURI(mtAuthorDID, mbPostAfterBanRKey))
	assert.Equal(t, accept.StatusAccepted, status, "and A keeps admitting the author's posts")
}

// TestASiteScopedBlockIsSkippedWithItsOwnReason covers the target this scope
// does not model.
//
// Lemmy issues instance-wide bans too, and they arrive as the same verb with
// `target` naming the SITE actor rather than a community. Scope A models
// per-community bans only — so the honest outcome is a refusal that says which
// kind it was. Silently no-op'ing one leaves the user posting into that instance
// and collecting 403s until their deliveries poison, with nothing anywhere
// naming the site ban as the cause.
func TestASiteScopedBlockIsSkippedWithItsOwnReason(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	resetBanState(t, h.db)
	world := newModerationWorld(t, h)

	// Lemmy's Site actor is the instance apex — which is a URL PREFIX of every
	// other id on that instance, so "the reason mentions the target" is true of
	// any message quoting any lemmy.world url. The distinction has to be drawn
	// against another outcome, not against a substring.
	const siteActor = "https://lemmy.world/"
	const siteBlock = "https://lemmy.world/activities/announce/block/mb-site"
	h.announceBlock(world.groupA, siteBlock, mtAuthorDID, siteActor, nil)

	_, found := banFor(t, h.db, world.communityADID, mtAuthorDID)
	assert.False(t, found,
		"a site ban is not a community ban: recording it against the announcing community "+
			"would understate it — the user is excluded from every community on that instance, "+
			"and we would enforce it in one")

	// The same activity, targeted at the community, is the shape that WORKS.
	const communityBlock = "https://lemmy.world/activities/announce/block/mb-site-control"
	h.announceBlock(world.groupA, communityBlock, mtCommenterDID, groupID, nil)

	assert.NotEqual(t,
		skipReasonFor(t, h, communityBlock, communityBlock),
		skipReasonFor(t, h, siteBlock, siteBlock),
		"a target this scope does not model must be distinguishable from one it does: today "+
			"both land in the same 'unsupported activity type' default, so an operator whose "+
			"user is collecting 403s across a whole instance reads the same line as someone "+
			"whose ban was applied — and silently no-op'ing the site ban leaves that user "+
			"posting into the instance until their deliveries poison")

	event, err := h.events.GetEvent(ctx, siteBlock)
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt, "decided once, not retried: %s", event.Error)
	assert.Nil(t, event.FailedAt, "nor poisoned")
}

// mbDirectIgnoredMetric is the counter for direct Blocks. Read through expvar by
// NAME so the test survives the var being renamed or moved — the counter's
// identity is its published name, which is what an operator's dashboard binds
// to, not the Go symbol.
const mbDirectIgnoredMetric = "tidepool_block_direct_ignored"

func metricValue(name string) int64 {
	counter, _ := expvar.Get(name).(*expvar.Int)
	if counter == nil {
		return 0
	}
	return counter.Value()
}

func timeNow() time.Time { return time.Now() }

// TestABanWithRemoveDataRemovesTheirPostsInThatCommunityOnly is the destructive
// half, and its scope is the whole design.
//
// `removeData: true` means Lemmy purged that author's content — in THAT
// community. The admissions ledger is the only table that knows which posts were
// ever ACCEPTED there (idx_admissions_author_community), which is why it is the
// input rather than "every post by this author".
//
// The removal code is `author-banned`, not moderator-discretion: they are
// different decisions with different reversals, and a reader that cannot tell
// them apart cannot tell "this post broke a rule" from "its author is no longer
// welcome here". That distinction is what RemovePost's new `code` parameter
// exists for — it hardcodes moderator-discretion today.
func TestABanWithRemoveDataRemovesTheirPostsInThatCommunityOnly(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	resetBanState(t, h.db)
	world := newModerationWorld(t, h)

	// The author has accepted posts in BOTH communities. The world already gave
	// them one in A; this is the one in B that must survive.
	admitPost(t, world, mtAuthorDID, mbPostInBRKey, world.communityBDID, "3lzmbrev00020", 1_775_000_006_000_001)
	postInB := mbPostATURI(mtAuthorDID, mbPostInBRKey)
	_, _, err := h.manager.GetRecord(ctx,
		world.communityBDID, materialize.CollectionAcceptance, testDigestRKey(postInB))
	require.NoError(t, err, "precondition: the author is accepted in B too")

	// And the OTHER author has an accepted post in A, which a ban on someone
	// else must not touch.
	admitPost(t, world, mtCommenterDID, mbOtherPostRKey, world.communityADID, "3lzmbrev00021", 1_775_000_006_000_002)
	otherPost := mbPostATURI(mtCommenterDID, mbOtherPostRKey)

	activitiesBefore := rowCount(t, h.db, "outbound_activities")
	deliveriesBefore := rowCount(t, h.db, "outbound_deliveries")

	h.announceBlock(world.groupA, mbBlockActivity, mtAuthorDID, groupID,
		map[string]any{"removeData": true})

	// --- Their post in A is removed, under the ban's own code.
	removal, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionRemoval, world.digestRKey)
	require.NoError(t, err,
		"removeData means their content in this community goes: Lemmy has already purged it "+
			"on their side, and leaving it accepted here publishes a community's endorsement "+
			"of content that community has removed")
	assert.Equal(t, "author-banned", removal["code"],
		"under author-banned, not moderator-discretion: this post was not judged, its AUTHOR "+
			"was — and the two have different reversals, so a reader that cannot tell them "+
			"apart cannot answer why the post went")
	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, world.digestRKey)
	assert.True(t, errors.IsNotFound(err), "with the acceptance withdrawn (err=%v)", err)

	// --- Their post in B is untouched.
	_, _, err = h.manager.GetRecord(ctx,
		world.communityBDID, materialize.CollectionAcceptance, testDigestRKey(postInB))
	assert.NoError(t, err,
		"their post in B STANDS: removeData is scoped to the banning community's own space, "+
			"and an author banned from one community losing their writing everywhere is a "+
			"site-wide purge issued by a single moderator team (err=%v)", err)
	_, _, err = h.manager.GetRecord(ctx,
		world.communityBDID, materialize.CollectionRemoval, testDigestRKey(postInB))
	assert.True(t, errors.IsNotFound(err), "and B records no removal of it (err=%v)", err)

	// --- The other author's post in A is untouched.
	otherStatus, _ := admissionFor(t, h.db, world.communityADID, otherPost)
	assert.Equal(t, accept.StatusAccepted, otherStatus,
		"and the OTHER author's post in A stands: the ledger query is (author, community), "+
			"and dropping the author term removes the whole community's backlog")

	// --- Nothing goes out, for two independent reasons.
	assert.Equal(t, activitiesBefore, rowCount(t, h.db, "outbound_activities"),
		"NOTHING is enqueued. Structurally: removal runs through the materializer, which uses "+
			"ApplyOps and takes no side effect, so it cannot enqueue. Semantically: Lemmy has "+
			"ALREADY removed this content — the ban is us learning what they did, not us "+
			"asking them to do it — so an outbound Delete would be redundant and aimed at the "+
			"moderators who just acted")
	assert.Equal(t, deliveriesBefore, rowCount(t, h.db, "outbound_deliveries"), "...and no new delivery")

	// --- And the unban does NOT bring the content back.
	h.announceUndoBlock(world.groupA, mbUnblockActivity, mbBlockActivity+"/block", mtAuthorDID, groupID)

	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionRemoval, world.digestRKey)
	assert.NoError(t, err,
		"the removal SURVIVES the unban: Lemmy models restoration as a separate restore_data "+
			"flag, so an Undo{Block} that quietly republished removed posts would reverse a "+
			"moderator's decision and push the content back at the community that removed it "+
			"— the same harm as reversing a removal on an author's edit, arriving by another "+
			"door (err=%v)", err)
	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, world.digestRKey)
	assert.True(t, errors.IsNotFound(err),
		"and no acceptance comes back with it (err=%v)", err)
}

// TestABanLeavesTerminalDeliveriesAlone pins what cancellation must NOT touch.
//
// Cancellation is for PENDING work. The two terminal states are terminal for
// different reasons and both must survive a ban:
//
//   - `delivered` cannot be un-sent. Rewriting it would make the ledger claim we
//     withdrew something the peer holds — and that column is not private
//     bookkeeping: the vote reseed subtracts live delivered outbound state from
//     the API tally, so falsifying it silently corrupts a user-visible score in
//     a subsystem nobody would think to look at from here.
//   - `poisoned` is an operator surface. A delivery that failed for its own
//     reason, swept into "cancelled" by a ban that arrived later, is a fault
//     nobody will ever diagnose — and redrive is how someone recovers it.
func TestABanLeavesTerminalDeliveriesAlone(t *testing.T) {
	h := newHarness(t)
	resetBanState(t, h.db)
	world := newModerationWorld(t, h)

	admitPost(t, world, mtAuthorDID, mbPostInARKey, world.communityADID, "3lzmbrev00030", 1_775_000_007_000_001)
	admitPost(t, world, mtAuthorDID, mbPostAfterUndoRKey, world.communityADID, "3lzmbrev00031", 1_775_000_007_000_002)

	// Force the two terminal states onto this author's work for A. A worker run
	// would be the honest way to reach 'delivered', but the state is what the
	// ban's scope is about, and driving it directly is what keeps the fixture
	// about the ban rather than about delivery.
	terminal := forceDeliveryStates(t, h.db, mtAuthorDID, groupID, "delivered", "poisoned")
	require.Len(t, terminal, 2, "precondition: two terminal deliveries to ban across")

	h.announceBlock(world.groupA, mbBlockActivity, mtAuthorDID, groupID, nil)

	assert.Equal(t, "delivered", deliveryState(t, h.db, terminal[0]),
		"a DELIVERED row is untouched: it cannot be un-sent, and the reseed reads exactly "+
			"this column to subtract our personas' live votes from the API tally — rewriting "+
			"it moves a number the user sees, from a subsystem this code has never heard of")
	assert.Equal(t, "poisoned", deliveryState(t, h.db, terminal[1]),
		"and a POISONED row is untouched: sweeping it hides a delivery that failed for its "+
			"own reason behind a ban that arrived afterwards, and redrive is how an operator "+
			"gets it back")
}

// forceDeliveryStates stamps states onto an actor's pending deliveries for one
// community, in seq order, and returns the activity ids it stamped.
func forceDeliveryStates(t *testing.T, db *sql.DB, actorDID, orderingKey string, states ...string) []string {
	t.Helper()
	ctx := context.Background()
	rows, err := db.QueryContext(ctx, `
		SELECT d.activity_id
		FROM outbound_deliveries d
		JOIN outbound_activities a ON a.activity_id = d.activity_id
		WHERE a.actor_did = $1 AND d.ordering_key = $2 AND d.state = 'pending'
		ORDER BY d.seq
		LIMIT $3`, actorDID, orderingKey, len(states))
	require.NoError(t, err)
	var ids []string
	for rows.Next() {
		var id string
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Len(t, ids, len(states),
		"the fixture needs %d pending deliveries for %s on %s to stamp", len(states), actorDID, orderingKey)

	for i, id := range ids {
		_, err := db.ExecContext(ctx,
			`UPDATE outbound_deliveries SET state = $2 WHERE activity_id = $1`, id, states[i])
		require.NoError(t, err)
	}
	return ids
}

func deliveryState(t *testing.T, db *sql.DB, activityID string) string {
	t.Helper()
	var state string
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT state FROM outbound_deliveries WHERE activity_id = $1`, activityID).Scan(&state))
	return state
}
