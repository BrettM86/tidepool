package ingest

import (
	"context"
	"database/sql"
	"encoding/json"
	stderrors "errors"
	"expvar"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/accept"
	"tidepool/internal/ap"
	"tidepool/internal/consume"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
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
	reason        string
	removeData    bool
}

func banFor(t *testing.T, db *sql.DB, communityDID, subjectDID string) (communityBan, bool) {
	t.Helper()
	var ban communityBan
	err := db.QueryRowContext(context.Background(), `
		SELECT community_ap_id, expires_at, reason, remove_data
		FROM community_bans
		WHERE community_did = $1 AND subject_did = $2`,
		communityDID, subjectDID).Scan(&ban.communityAPID, &ban.expires, &ban.reason, &ban.removeData)
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

// assertEveryDelivery is requireEveryDelivery without the abort. A test pinning
// SEVERAL independent consequences of one activity uses this so a broken first
// consequence does not hide the others — the vacuity guard stays fatal, because
// a test asserting over no rows has stopped measuring anything.
func assertEveryDelivery(t *testing.T, db *sql.DB, actorDID, orderingKey, want, why string) {
	t.Helper()
	states := deliveryStates(t, db, actorDID, orderingKey)
	require.NotEmpty(t, states,
		"%s — and there must BE deliveries to say that about: %s has no rows on %s at all",
		why, actorDID, orderingKey)
	for i, state := range states {
		assert.Equal(t, want, state, "%s (delivery %d of %d)", why, i+1, len(states))
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

// TestADirectBlockIsIgnoredWhateverItClaims pins that the announced-only rule is
// STRUCTURAL: it does not depend on who the activity says sent it.
//
// The activity CLAIMS `actor` = the community Group while being SIGNED by a
// moderator's Person. Both live on lemmy.world, so the same-authority tolerance
// at the door lets the delivery in, and the claim is the most authoritative
// thing a direct Block could possibly assert about itself. It still records
// nothing, because a direct delivery has no announcer — and the announcer is
// what the ban path requires, before anyone weighs an actor at all.
//
// WHAT THIS DOES NOT COVER, despite how it reads: SEC-1's signer binding. Binding
// the claimed actor instead of the verified signer leaves this test GREEN,
// because the refusal happens one gate earlier. That is defence in depth rather
// than dependence — but the binding itself is pinned by inbox_laundering_test.go
// and, for the ban path specifically, by the laundered ANNOUNCE below. A green
// test nobody re-derives the reason for is how a control ends up guarding
// nothing.
func TestADirectBlockIsIgnoredWhateverItClaims(t *testing.T) {
	h := newHarness(t)
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

// TestALapsedBanIsInert is the half of `expires` that has no activity behind it,
// and it covers ALL THREE things a lapsed Block must not do.
//
// Lemmy's BlockUser carries `expires` for a temporary ban, and when that ban
// lapses Lemmy sends NOTHING — no Undo, no second activity. The ban simply stops
// applying on their side. So a Block whose expiry has ALREADY PASSED when it
// reaches us — delayed in a queue, redelivered after an outage, replayed from a
// backfill — is a description of a ban that is already over.
//
// Reading it as live is destructive in two directions that no later activity can
// repair: it cancels queued work that was never banned, and with removeData it
// PERMANENTLY removes accepted posts. Both are unreachable from the admission
// gate, which is why an admission-only test of the lapsed case passes while both
// are broken — the expiry has to be weighed where the ban ACTS, not only where
// it is later read.
func TestALapsedBanIsInert(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	// Queued work, and an accepted post: the two things a live ban destroys.
	admitPost(t, world, mtAuthorDID, mbPostInARKey, world.communityADID, "3lzmbrev00050", 1_775_000_009_000_001)
	requireEveryDelivery(t, h.db, mtAuthorDID, groupID, "pending",
		"precondition: the author has queued work for A")
	_, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, world.digestRKey)
	require.NoError(t, err, "precondition: and an accepted post in A")

	// A ban that ended before it arrived — carrying removeData, so every
	// destructive branch is on the table.
	h.announceBlock(world.groupA, mbBlockActivity, mtAuthorDID, groupID,
		map[string]any{"expires": "2020-01-01T00:00:00Z", "removeData": true})

	// (1) It cancels nothing.
	assertEveryDelivery(t, h.db, mtAuthorDID, groupID, "pending",
		"a LAPSED ban cancels nothing: these posts were queued by an author who is not "+
			"banned now and was not banned when the ban expired — cancelling them silently "+
			"unpublishes work on the strength of an exclusion that has already ended, and "+
			"nothing re-queues a cancelled delivery")

	// (2) It removes nothing.
	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionRemoval, world.digestRKey)
	assert.True(t, errors.IsNotFound(err),
		"and it removes NOTHING: removeData on a lapsed ban strips accepted posts for an "+
			"exclusion that is over, and a removal is terminal — no Undo{Block} restores "+
			"content, by design, so this is unrecoverable (err=%v)", err)
	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, world.digestRKey)
	assert.NoError(t, err, "the acceptance stands (err=%v)", err)

	// (3) It refuses no admission — the half that was already covered, kept
	//     because the three only mean anything together.
	// CONDITIONAL, and deliberately so: both outcomes are correct here. Not
	// recording a lapsed ban at all is the stronger reading — a ban row is
	// CURRENT STATE, not a log (Lift deletes rather than marks), so a row for an
	// exclusion that is already over is state no reader wants and one more thing
	// that has to remember the expiry filter. Recording it with its expiry is
	// also sound, since every read is scoped by `expires_at > now()`.
	//
	// What this conditional CANNOT hide is the regression that matters: the only
	// dangerous row shape is one with a NULL expiry, because that reads as
	// permanent forever, and it fails inside the guard. An absent row cannot be
	// mistaken for a standing ban by anything.
	if ban, found := banFor(t, h.db, world.communityADID, mtAuthorDID); found {
		require.True(t, ban.expires.Valid,
			"a lapsed ban that IS recorded must carry its expiry: storing it unbounded turns "+
				"a ban that already ended into a permanent one, and no activity will ever "+
				"correct that — Lemmy sends nothing when a ban lapses")
		assert.True(t, ban.expires.Time.Before(time.Now()),
			"and it must be the moment the moderator chose, in the past")
	}

	admitPost(t, world, mtAuthorDID, mbPostAfterBanRKey, world.communityADID,
		"3lzmbrev00051", 1_775_000_009_000_002)
	postURI := mbPostATURI(mtAuthorDID, mbPostAfterBanRKey)
	status, code := admissionFor(t, h.db, world.communityADID, postURI)
	assert.Equal(t, accept.StatusAccepted, status,
		"a LAPSED ban must not refuse admission: every read has to be `expires_at IS NULL OR "+
			"expires_at > now()`, because no Undo is coming — the expiry IS the lift")
	assert.NotEqual(t, "author-banned", code, "and certainly not for being banned")

	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, testDigestRKey(postURI))
	assert.NoError(t, err, "with the acceptance to prove it (err=%v)", err)
}

// TestABanWhoseExpiryCannotBeReadIsNotStoredAsPermanent is the fail-safe
// direction on the one field whose absence means FOREVER.
//
// `expires` absent and `expires` present-but-unparseable are different facts,
// and the wire parser keeps them apart (a nil *Time versus a Time with
// Valid=false) precisely so a caller can. Collapsing them maps "we could not
// read how long" onto "no expiry", which is the most consequential possible
// misreading: the moderator asked for a time limit, and the author is excluded
// forever instead.
//
// Nothing ever repairs it. Lemmy sends no activity when a timed ban lapses, so
// there is no message whose arrival could correct the record, and no operator
// has a reason to look — from every side this reads like an ordinary permanent
// ban that a moderator chose.
//
// The honest outcome is to refuse the activity rather than store a ban we cannot
// bound: we know what they meant and we could not read how long.
func TestABanWhoseExpiryCannotBeReadIsNotStoredAsPermanent(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	const malformed = "https://lemmy.world/activities/announce/block/mb-badexpiry"
	h.announceBlock(world.groupA, malformed, mtAuthorDID, groupID,
		map[string]any{"expires": "next tuesday"})

	ban, found := banFor(t, h.db, world.communityADID, mtAuthorDID)
	if found {
		assert.True(t, ban.expires.Valid,
			"a ban stored from an activity that CARRIED an expiry must carry one: storing it "+
				"with a NULL expiry records a permanent exclusion the moderator did not ask "+
				"for, and nothing will ever clear it")
	} else {
		assert.False(t, found,
			"or it is not stored at all, which is the safer reading of an unbounded ban")
	}

	event, err := h.events.GetEvent(ctx, malformed)
	require.NoError(t, err)
	assert.Nil(t, event.ProcessedAt,
		"and the activity is NOT marked handled: an expiry we cannot read is a ban we cannot "+
			"bound, so the honest outcome is a failure an operator can see — retryable or "+
			"poisoned, either says 'we did not apply this'. Marking it processed tells the "+
			"community we honoured a ban whose duration we silently replaced with forever: %s",
		event.Error)

	admitPost(t, world, mtAuthorDID, mbPostAfterBanRKey, world.communityADID,
		"3lzmbrev00060", 1_775_000_010_000_001)
	status, code := admissionFor(t, h.db, world.communityADID, mbPostATURI(mtAuthorDID, mbPostAfterBanRKey))
	assert.Equal(t, accept.StatusAccepted, status,
		"and the author is not excluded on an unreadable instruction: the failure mode this "+
			"guards is a permanent shadowban created by a typo in someone else's software")
	assert.NotEqual(t, "author-banned", code, "least of all as author-banned")
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
	world := newModerationWorld(t, h)

	h.announceBlock(world.groupA, mbBlockActivity, mtAuthorDID, groupID,
		map[string]any{"expires": "2099-01-01T00:00:00Z"})

	ban, found := banFor(t, h.db, world.communityADID, mtAuthorDID)
	require.True(t, found, "a temporary ban is still a ban and must be recorded")
	require.True(t, ban.expires.Valid, "carrying its expiry")
	assert.True(t, ban.expires.Time.After(time.Now()), "which has not arrived")

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
	world := newModerationWorld(t, h)

	// Lemmy's Site actor is the instance apex — which is a URL PREFIX of every
	// other id on that instance, so "the reason mentions the target" is true of
	// any message quoting any lemmy.world url. The distinction has to be drawn
	// against another outcome, not against a substring.
	const siteActor = "https://lemmy.world/"
	const siteBlock = "https://lemmy.world/activities/announce/block/mb-site"
	unscopedBefore := metricValue(mbUnscopedTargetMetric)
	h.announceBlock(world.groupA, siteBlock, mtAuthorDID, siteActor, nil)

	_, found := banFor(t, h.db, world.communityADID, mtAuthorDID)
	assert.False(t, found,
		"a site ban is not a community ban: recording it against the announcing community "+
			"would understate it — the user is excluded from every community on that instance, "+
			"and we would enforce it in one")
	assert.Equal(t, unscopedBefore+1, metricValue(mbUnscopedTargetMetric),
		"and it is COUNTED under its own class: this is the scope we chose not to model, and "+
			"the counter is what tells an operator how much of it is arriving — a user "+
			"collecting 403s across a whole instance is invisible otherwise")

	// THE CONTROL, ASSERTED: the same activity targeted at the announcer's own
	// community WORKS. Without this the comparison below could be satisfied by a
	// control that silently failed too.
	const workingBlock = "https://lemmy.world/activities/announce/block/mb-site-control"
	// The control's subject needs an AP persona to be bannable at all — the
	// subject is resolved by ENTITY EXISTENCE, so an actor who has never
	// federated anything is refused as a foreign subject, and the control would
	// fail for a reason that has nothing to do with targets.
	admitPost(t, world, mtCommenterDID, mbOtherPostRKey, world.communityADID,
		"3lzmbrev00080", 1_775_000_012_000_001)
	h.announceBlock(world.groupA, workingBlock, mtCommenterDID, groupID, nil)
	_, worked := banFor(t, h.db, world.communityADID, mtCommenterDID)
	require.True(t, worked,
		"precondition: a community-targeted Block from the same announcer is recorded — the "+
			"shape under test differs from this one ONLY in its target")

	// And the discrimination is against another REFUSAL, not against a success:
	// a successful Block logs no skip at all, so comparing with it would reduce
	// to 'the site block produced some reason', which the generic
	// unsupported-type default already satisfies.
	const wrongCommunityBlock = "https://lemmy.world/activities/announce/block/mb-site-cross"
	h.announceBlock(world.groupB, wrongCommunityBlock, mtAuthorDID, groupID, nil)

	siteReason := skipReasonFor(t, h, siteBlock, siteBlock)
	crossReason := skipReasonFor(t, h, wrongCommunityBlock, wrongCommunityBlock)
	require.NotEmpty(t, siteReason, "the site-targeted Block must be skipped with a reason")
	require.NotEmpty(t, crossReason, "and so must the wrong-community one")
	assert.NotEqual(t, crossReason, siteReason,
		"a target this scope does not model must be distinguishable from a target that "+
			"belongs to somebody else: one means 'we do not implement instance bans' and the "+
			"other means 'that is not your community', and an operator who cannot tell them "+
			"apart cannot tell a missing feature from a rejected forgery")

	event, err := h.events.GetEvent(ctx, siteBlock)
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt, "decided once, not retried: %s", event.Error)
	assert.Nil(t, event.FailedAt, "nor poisoned")
}

// mbDirectIgnoredMetric is the counter for direct Blocks. Read through expvar by
// NAME so the test survives the var being renamed or moved — the counter's
// identity is its published name, which is what an operator's dashboard binds
// to, not the Go symbol.
const (
	mbDirectIgnoredMetric = "tidepool_block_direct_ignored"
	// mbUnscopedTargetMetric counts announced Blocks whose target is not a
	// community we follow — the instance-wide scope this bridge does not model.
	mbUnscopedTargetMetric = "tidepool_block_unscoped_target"
	// mbForeignSubjectMetric counts announced Blocks naming somebody who is not
	// one of our personas.
	mbForeignSubjectMetric = "tidepool_block_foreign_subject"
)

func metricValue(name string) int64 {
	counter, _ := expvar.Get(name).(*expvar.Int)
	if counter == nil {
		return 0
	}
	return counter.Value()
}

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
	world := newModerationWorld(t, h)

	admitPost(t, world, mtAuthorDID, mbPostInARKey, world.communityADID, "3lzmbrev00030", 1_775_000_007_000_001)
	admitPost(t, world, mtAuthorDID, mbPostAfterUndoRKey, world.communityADID, "3lzmbrev00031", 1_775_000_007_000_002)

	// Force the two terminal states onto this author's work for A. A worker run
	// would be the honest way to reach 'delivered', but the state is what the
	// ban's scope is about, and driving it directly is what keeps the fixture
	// about the ban rather than about delivery.
	terminal := forceDeliveryStates(t, h.db, mtAuthorDID, groupID, "delivered", "poisoned")
	require.Len(t, terminal, 2, "precondition: two terminal deliveries to ban across")
	pendingBefore := pendingActivityIDs(t, h.db, mtAuthorDID, groupID)
	require.NotEmpty(t, pendingBefore,
		"precondition: and a PENDING one beside them, so the ban has something to cancel")

	h.announceBlock(world.groupA, mbBlockActivity, mtAuthorDID, groupID, nil)

	assert.Equal(t, "delivered", deliveryState(t, h.db, terminal[0]),
		"a DELIVERED row is untouched: it cannot be un-sent, and the reseed reads exactly "+
			"this column to subtract our personas' live votes from the API tally — rewriting "+
			"it moves a number the user sees, from a subsystem this code has never heard of")
	assert.Equal(t, "poisoned", deliveryState(t, h.db, terminal[1]),
		"and a POISONED row is untouched: sweeping it hides a delivery that failed for its "+
			"own reason behind a ban that arrived afterwards, and redrive is how an operator "+
			"gets it back")

	// THE BAN MUST HAVE DONE SOMETHING. Every assertion above is satisfied by a
	// Block that was skipped entirely, which is the mis-attributed control in
	// its purest form: a test that proves cancellation is correctly scoped by
	// arranging for no cancellation to happen at all.
	_, found := banFor(t, h.db, world.communityADID, mtAuthorDID)
	require.True(t, found, "the ban was recorded — this test is about what it did NOT touch")
	for _, id := range pendingBefore {
		assert.Equal(t, "cancelled", deliveryState(t, h.db, id),
			"...having cancelled the PENDING row beside them: if that one survived too, "+
				"nothing was cancelled and the two assertions above proved nothing about "+
				"scope — they would hold just as well for a Block that was skipped entirely")
	}
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

// pendingActivityIDs lists an actor's pending deliveries on one ordering key.
func pendingActivityIDs(t *testing.T, db *sql.DB, actorDID, orderingKey string) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `
		SELECT d.activity_id
		FROM outbound_deliveries d
		JOIN outbound_activities a ON a.activity_id = d.activity_id
		WHERE a.actor_did = $1 AND d.ordering_key = $2 AND d.state = 'pending'
		ORDER BY d.seq`, actorDID, orderingKey)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var ids []string
	for rows.Next() {
		var id string
		require.NoError(t, rows.Scan(&id))
		ids = append(ids, id)
	}
	require.NoError(t, rows.Err())
	return ids
}

func deliveryState(t *testing.T, db *sql.DB, activityID string) string {
	t.Helper()
	var state string
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT state FROM outbound_deliveries WHERE activity_id = $1`, activityID).Scan(&state))
	return state
}

// TestAnAnnouncedBlockLaunderedThroughAPersonIsRefused is the SEC-1 shape aimed
// at the ban path — the one the direct-Block control above only appears to
// cover.
//
// Everything here is the announced shape the ban path DOES act on: an Announce
// carrying a Block, claiming `actor` = the community Group, addressed like every
// other fan-out. The only thing wrong with it is the signature: it is signed by
// a moderator's Person key, not the Group's. Both live on lemmy.world, so the
// same-authority tolerance at the door passes it through, and every downstream
// check reads the actor the inbox BOUND.
//
// That is why binding the claim rather than the verified signer was CRITICAL:
// it makes this delivery indistinguishable from the community's own ban. And a
// ban is the worst verb to lose it on — any account on a Lemmy instance could
// exclude any native author from any community co-hosted there, cancel their
// pending posts, and with removeData strip their accepted content, all under a
// community's name and with a moderation record to match.
func TestAnAnnouncedBlockLaunderedThroughAPersonIsRefused(t *testing.T) {
	h := newHarness(t)
	world := newModerationWorld(t, h)

	// A moderator who can really sign — their own key, their own actor document.
	// Nothing about this delivery is malformed; it is simply not the community.
	moderator := h.newRemoteActor(modActorID, person(modActorID, "moderator", nil))

	const laundered = "https://lemmy.world/activities/announce/block/mb-laundered"
	require.Equal(t, http.StatusAccepted, h.deliver(moderator, map[string]any{
		"id":       laundered,
		"type":     "Announce",
		"actor":    groupID, // the claim: "I am the community"
		"audience": groupID,
		"cc":       []any{groupID + "/followers"},
		"object":   blockActivity(world.groupA, laundered+"/block", mtAuthorDID, groupID, nil),
	}))
	h.drain()

	_, found := banFor(t, h.db, world.communityADID, mtAuthorDID)
	assert.False(t, found,
		"the VERIFIED SIGNER decides, never the claim: an announce is only the community's "+
			"if the community signed it, and binding the claimed actor would let any account "+
			"on the instance ban a native author out of a community whose moderators did "+
			"nothing — decision 18's conjunction reading a forgeable input")

	requireEveryDelivery(t, h.db, mtAuthorDID, groupID, "pending",
		"and their pending work stands: a forged ban that still cancelled deliveries would "+
			"unpublish an author on an unsigned claim")

	admitPost(t, world, mtAuthorDID, mbPostAfterBanRKey, world.communityADID,
		"3lzmbrev00040", 1_775_000_008_000_001)
	status, _ := admissionFor(t, h.db, world.communityADID, mbPostATURI(mtAuthorDID, mbPostAfterBanRKey))
	assert.Equal(t, accept.StatusAccepted, status,
		"and admission is unaffected: a half-applied forged ban is a shadowban nobody issued "+
			"and no Undo can lift, because no moderator ever made the decision to reverse")
}

// TASK 17c-3 REVIEW, P2 — WHAT THE BAN PATH RECORDS AND COUNTS.

// TestADirectUndoBlockIsCountedLikeADirectBlock closes an asymmetry in the one
// number that reports on this door.
//
// Lemmy sends BOTH halves of a ban directly to the banned user's inbox: the
// Block, and later the Undo{Block}. Only the announced copies can be authorized,
// so both direct copies are ignored — but only the Block is counted. Direct
// unban traffic is therefore invisible, and the counter that is supposed to say
// "this is how much of the ban conversation arrives on the door we do not open"
// reports half of it.
//
// That matters precisely when it is read: if the announced path ever breaks, the
// operator compares direct traffic against recorded bans. A counter that moves
// for bans and not unbans makes a broken announce path look like a community
// that bans and never forgives.
func TestADirectUndoBlockIsCountedLikeADirectBlock(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)
	moderator := h.newRemoteActor(modActorID, person(modActorID, "moderator", nil))

	before := metricValue(mbDirectIgnoredMetric)

	const directUndo = "https://lemmy.world/activities/undo/mb-direct-undo"
	require.Equal(t, http.StatusAccepted, h.deliver(moderator, map[string]any{
		"id":       directUndo,
		"type":     "Undo",
		"actor":    modActorID,
		"audience": groupID,
		"cc":       []any{groupID},
		"object":   blockActivity(world.groupA, directUndo+"/block", mtAuthorDID, groupID, nil),
	}))
	h.drain()

	assert.Equal(t, before+1, metricValue(mbDirectIgnoredMetric),
		"a direct Undo{Block} is the same decided non-action as a direct Block and must be "+
			"counted the same way: Lemmy sends both to this door, and a counter that moves for "+
			"one and not the other reports the door as quieter than it is")

	event, err := h.events.GetEvent(ctx, directUndo)
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt, "decided once, not retried: %s", event.Error)
	assert.Nil(t, event.FailedAt, "nor poisoned")
}

// TestABanRecordsTheModeratorsReasonAndScope pins the two fields the row carries
// for people rather than for logic.
//
// `removeData` decides whether content goes, and the reason is what a moderator
// typed. The schema holds both and the repository writes both — but nothing
// fills them in, so every ban reads as reasonless. It matters because the SAME
// moderator action already writes the reason somewhere else: a removeData
// removal carries the summary into the removal record. Two audit surfaces
// describing one decision, one of them blank, is worse than neither having it —
// whoever reads the empty one concludes no reason was given.
func TestABanRecordsTheModeratorsReasonAndScope(t *testing.T) {
	h := newHarness(t)
	world := newModerationWorld(t, h)

	const reason = "repeated rule 3 violations after two warnings"
	h.announceBlock(world.groupA, mbBlockActivity, mtAuthorDID, groupID,
		map[string]any{"summary": reason, "removeData": true})

	ban, found := banFor(t, h.db, world.communityADID, mtAuthorDID)
	require.True(t, found, "the ban is recorded")
	assert.Equal(t, reason, ban.reason,
		"with the moderator's own words: the column exists, the repository writes it, and the "+
			"same summary already reaches the removal records this ban produced — a blank here "+
			"tells an operator no reason was given while the removal beside it quotes one")
	assert.True(t, ban.removeData,
		"and with the scope it was issued under: whether content went is the difference "+
			"between an exclusion and a purge, and after the fact the row is the only place "+
			"that answers it")
}

// TestABanCancelsWorkAWorkerHasAlreadyClaimed is the honest half of the
// in-flight problem.
//
// A worker claims a delivery in its own committed transaction and then POSTs.
// If a ban lands between those, nothing can un-send the request — the bytes are
// on the wire, and fencing only stops the settlement afterwards. So what is
// pinned here is what remains true and reachable: a CLAIMED row is still
// cancelled, and its claim is released rather than left to expire, so no further
// attempt is made for it.
//
// The unfixable remainder is a pre-send re-check, which this test deliberately
// does not pretend to cover.
func TestABanCancelsWorkAWorkerHasAlreadyClaimed(t *testing.T) {
	h := newHarness(t)
	world := newModerationWorld(t, h)

	admitPost(t, world, mtAuthorDID, mbPostInARKey, world.communityADID, "3lzmbrev00070", 1_775_000_011_000_001)
	claimed := claimDeliveries(t, h.db, mtAuthorDID, groupID)
	require.NotZero(t, claimed, "precondition: a worker holds a claim on this author's work")

	h.announceBlock(world.groupA, mbBlockActivity, mtAuthorDID, groupID, nil)

	assertEveryDelivery(t, h.db, mtAuthorDID, groupID, "cancelled",
		"a claimed row is cancelled like any other: the claim is a lease, not an exemption, "+
			"and leaving claimed work pending would let it be re-attempted for the whole lease "+
			"after the community banned its author")
	assert.Zero(t, claimedDeliveries(t, h.db, mtAuthorDID, groupID),
		"and the claim is RELEASED rather than left to expire: a cancelled row still holding "+
			"a lease is one a worker sweep has to reason about, and the point of cancelling is "+
			"that no further attempt is made")
}

// claimDeliveries simulates a worker claim — a lease stamped in a transaction
// that has already committed, which is exactly the state a ban can arrive in the
// middle of. It returns how many rows it claimed.
func claimDeliveries(t *testing.T, db *sql.DB, actorDID, orderingKey string) int64 {
	t.Helper()
	result, err := db.ExecContext(context.Background(), `
		UPDATE outbound_deliveries d
		SET claimed_until = now() + interval '1 minute'
		FROM outbound_activities a
		WHERE d.activity_id = a.activity_id
		  AND a.actor_did = $1 AND d.ordering_key = $2 AND d.state = 'pending'`,
		actorDID, orderingKey)
	require.NoError(t, err)
	affected, err := result.RowsAffected()
	require.NoError(t, err)
	return affected
}

func claimedDeliveries(t *testing.T, db *sql.DB, actorDID, orderingKey string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM outbound_deliveries d
		JOIN outbound_activities a ON a.activity_id = d.activity_id
		WHERE a.actor_did = $1 AND d.ordering_key = $2 AND d.claimed_until IS NOT NULL`,
		actorDID, orderingKey).Scan(&n))
	return n
}

// TASK 17c-3 REVIEW, HIGH-1 — THE GATE COVERS POSTS ONLY.
//
// A ban excludes an AUTHOR from a community, and admission enforces it for
// postv2 alone. Comments gate on opt-out, thread, depth and the parent lock;
// votes gate on opt-out. So a banned author's replies and votes keep leaving for
// the community that banned them, Lemmy refuses each one, and the deliveries
// retry to poisoned with the cause three tables away — precisely the outcome the
// thread-lock refusal exists to prevent, and the same fix shape.
//
// The tell that this is a GAP and not a decision: the ban's own cancellation
// already sweeps this author's queued comments and votes. It joins actor_did and
// ordering_key, and never looks at what kind of activity a row carries. So the
// system already agrees that a banned author's comments and votes must not go to
// that community — it just enforces it on the traffic that happens to be queued
// when the ban lands, and not on anything written afterwards.

// TestABannedAuthorsCommentIsRefusedInThatCommunityOnly is HIGH-1 for comments.
func TestABannedAuthorsCommentIsRefusedInThatCommunityOnly(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	// The same author, accepted in BOTH communities: the ban must reach exactly
	// one of the two threads they can reply in.
	admitPost(t, world, mtAuthorDID, mbPostInBRKey, world.communityBDID, "3lzmbrev00090", 1_775_000_013_000_001)
	postInB := mbPostATURI(mtAuthorDID, mbPostInBRKey)

	h.announceBlock(world.groupA, mbBlockActivity, mtAuthorDID, groupID, nil)
	_, banned := banFor(t, h.db, world.communityADID, mtAuthorDID)
	require.True(t, banned, "precondition: the author is banned from A and not from B")

	deliveriesBefore := rowCount(t, h.db, "outbound_deliveries")

	// --- Their comment into the community that banned them.
	inA := mbCommentBy(mtAuthorDID, "3lzmbcomment01", mtPostATURI, mtPostCID, "3lzmbrev00091")
	err := world.dispatcher.HandleEvent(ctx, inA.create(t))
	require.Error(t, err,
		"a banned author's COMMENT must be refused: a ban excludes the author from the "+
			"community, not their postv2 records from admission — Lemmy rejects the comment "+
			"server-side, so federating it buys a failed delivery, a retry loop and a poisoned "+
			"row whose cause is a moderator decision nothing in the queue names")
	assert.True(t, stderrors.Is(err, consume.ErrPermanentEvent),
		"with the same permanence as a locked thread: retrying cannot change a moderator's "+
			"decision, and parking it holds every other native user behind it (err=%v)", err)
	assert.Contains(t, err.Error(), "author-banned",
		"and naming the cause in the one string the DLQ carries — the operator answering "+
			"'where did my comment go' has nothing else")
	assert.Zero(t, outboundRowsFor(t, h.db, inA.atURI()),
		"nothing is written for the refused comment")
	assert.Equal(t, deliveriesBefore, rowCount(t, h.db, "outbound_deliveries"),
		"and nothing is sent to a community that will reject it")

	// --- The same author, the same moment, the community they are NOT banned in.
	inB := mbCommentBy(mtAuthorDID, "3lzmbcomment02", postInB, mtPostCID, "3lzmbrev00092")
	require.NoError(t, world.dispatcher.HandleEvent(ctx, inB.create(t)),
		"their comment in B must still federate: a ban is one community's ruling about its "+
			"own space, and a gate that reads 'is this author banned anywhere' silences them "+
			"everywhere on one moderator's decision")
	assert.Equal(t, 1, outboundRowsFor(t, h.db, inB.atURI()))
	assert.Greater(t, rowCount(t, h.db, "outbound_deliveries"), deliveriesBefore,
		"and it really went out")
}

// TestABannedAuthorsVoteIsRefusedInThatCommunityOnly is HIGH-1 for votes.
//
// A vote is the smallest thing an excluded author can still send, and the one
// they will send most: a banned user scrolling their own feed upvotes as they
// read. Every one of those is a delivery to an instance that refuses it.
func TestABannedAuthorsVoteIsRefusedInThatCommunityOnly(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	admitPost(t, world, mtAuthorDID, mbPostInBRKey, world.communityBDID, "3lzmbrev00100", 1_775_000_014_000_001)
	postInB := mbPostATURI(mtAuthorDID, mbPostInBRKey)

	h.announceBlock(world.groupA, mbBlockActivity, mtAuthorDID, groupID, nil)
	_, banned := banFor(t, h.db, world.communityADID, mtAuthorDID)
	require.True(t, banned, "precondition: banned in A, not in B")

	votesBefore := rowCount(t, h.db, "outbound_votes")
	deliveriesBefore := rowCount(t, h.db, "outbound_deliveries")

	err := world.dispatcher.HandleEvent(ctx,
		mbVoteEvent(t, mtAuthorDID, "3lzmbvote00001", "3lzmbrev00101", mtPostATURI, "up", 1_775_000_014_000_002))
	require.Error(t, err,
		"a banned author's VOTE must be refused too: it is the highest-volume thing they can "+
			"still send into a community that refuses all of it")
	assert.True(t, stderrors.Is(err, consume.ErrPermanentEvent),
		"permanently, like every other refusal in this family (err=%v)", err)
	assert.Contains(t, err.Error(), "author-banned", "with the cause named")
	assert.Equal(t, votesBefore, rowCount(t, h.db, "outbound_votes"),
		"and NO outbound vote state is written: that row is what a later Undo is rebuilt "+
			"from, so recording one for a vote we never sent leaves a retraction with nothing "+
			"behind it")
	assert.Equal(t, deliveriesBefore, rowCount(t, h.db, "outbound_deliveries"))

	require.NoError(t, world.dispatcher.HandleEvent(ctx,
		mbVoteEvent(t, mtAuthorDID, "3lzmbvote00002", "3lzmbrev00102", postInB, "up", 1_775_000_014_000_003)),
		"their vote in B still counts: the ban is scoped to A's space")
	assert.Greater(t, rowCount(t, h.db, "outbound_votes"), votesBefore, "and it was recorded")
}

// TestABannedAuthorEditingAnAcceptedPostRemovesNothing is HIGH-2, and it is the
// destructive one.
//
// An edit re-runs admission. Once the ban gate rejects it, the post takes the
// "was accepted and now fails re-admission" path: the acceptance is DELETED, a
// removal is written, and a Delete{Page} is ENQUEUED to the community — which
// with removeData=false is content Lemmy explicitly KEPT. So a typo fix, minutes
// after a ban, deletes the author's post from the community view and pushes a
// Delete at the moderators who chose to leave it up.
//
// That is 17c-1's F2 in mirror image: there the engine reversed a moderator's
// removal on an edit; here it removes what a moderator preserved. And the
// removal it writes carries admission-revoked — the REVERSIBLE code, whose whole
// meaning is "our own decision, a corrective edit may undo it" — for a cause
// that no edit can ever satisfy.
func TestABannedAuthorEditingAnAcceptedPostRemovesNothing(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	_, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, world.digestRKey)
	require.NoError(t, err, "precondition: the post is accepted in A")

	h.announceBlock(world.groupA, mbBlockActivity, mtAuthorDID, groupID, nil)
	activitiesBefore := rowCount(t, h.db, "outbound_activities")
	deliveriesBefore := rowCount(t, h.db, "outbound_deliveries")

	// The ordinary thing an author does minutes after being banned: fix a typo.
	require.NoError(t, world.dispatcher.HandleEvent(ctx,
		mtPostEvent(t, "update", mtEditRev, mtEditCID, mtEditTime)))

	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, world.digestRKey)
	assert.NoError(t, err,
		"the ACCEPTANCE MUST STAND: the moderators banned the author and left this post up — "+
			"removing it on their next edit destroys content the community chose to keep, and "+
			"the post disappearing from Coves is the visible half (err=%v)", err)
	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionRemoval, world.digestRKey)
	assert.True(t, errors.IsNotFound(err),
		"and NO removal may be written — least of all under admission-revoked, whose meaning "+
			"is 'we withdrew this and a corrective edit may restore it', for a cause no edit "+
			"can ever satisfy (err=%v)", err)

	assert.Equal(t, activitiesBefore, rowCount(t, h.db, "outbound_activities"),
		"NOTHING may be enqueued: a Delete{Page} here is the unrecoverable half — it is on "+
			"the wire, aimed at the moderators who deliberately kept this post, and no later "+
			"fix retracts it")
	assert.Equal(t, deliveriesBefore, rowCount(t, h.db, "outbound_deliveries"), "...and no delivery")

	status, code := admissionFor(t, h.db, world.communityADID, mtPostATURI)
	assert.Equal(t, "author-banned", code,
		"and the ledger records WHY the edit did nothing: it is the surface an operator reads "+
			"when the author asks, and 'the edit silently vanished' is the only alternative")
	assert.NotEqual(t, accept.StatusRemoved, status,
		"while the post itself is not recorded as removed — it is still up, and the ledger "+
			"must not say otherwise")
}

// TestAnAnnouncedBlockNamingAFediverseUserIsNotOurs pins the branch that decides
// a Block is somebody else's business.
//
// Lemmy announces every ban it issues, including bans of its OWN users, and we
// follow those communities — so this arrives constantly. It is enforced entirely
// on their instance; we hold no state that could apply it and no persona it
// could be about. The counter is what separates "we saw it and it was not ours"
// from "we never received it", which is the question asked the day the ban path
// looks broken.
func TestAnAnnouncedBlockNamingAFediverseUserIsNotOurs(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)
	before := metricValue(mbForeignSubjectMetric)

	const foreign = "https://lemmy.world/activities/announce/block/mb-foreign"
	require.Equal(t, http.StatusAccepted, h.deliver(world.groupA, map[string]any{
		"id":       foreign,
		"type":     "Announce",
		"actor":    groupID,
		"audience": groupID,
		"cc":       []any{groupID + "/followers"},
		"object": map[string]any{
			"id":       foreign + "/block",
			"type":     "Block",
			"actor":    modActorID,
			"object":   "https://lemmy.world/u/SomeLemmyUser",
			"target":   groupID,
			"audience": groupID,
			"cc":       []any{groupID},
		},
	}))
	h.drain()

	assert.Zero(t, bansIn(t, h.db, world.communityADID),
		"a ban on a LEMMY user records nothing here: we hold no persona it could be about, "+
			"and inventing a row keyed on an id we cannot resolve to a DID would gate "+
			"admission on a subject that can never post")
	assert.Equal(t, before+1, metricValue(mbForeignSubjectMetric),
		"and it is counted as not-ours: every community we follow announces its own bans, so "+
			"this is the common case, and the day the ban path looks broken this counter is "+
			"what distinguishes 'none of them were ours' from 'we stopped receiving them'")

	event, err := h.events.GetEvent(ctx, foreign)
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt, "decided once, not retried: %s", event.Error)
	assert.Nil(t, event.FailedAt, "nor poisoned")
}

// mbCommentBy is one native comment by an author, replying to a post.
func mbCommentBy(did, rkey, parentURI, parentCID, rev string) nativeComment {
	return nativeComment{
		did: did, rkey: rkey,
		root:      nativeRef{parentURI, parentCID},
		parent:    nativeRef{parentURI, parentCID},
		createRev: rev, createCID: "bafyreih5xbmigkq5ikyhqiqhqzbwuqjxeitgtzwyxvjhfsfvswsxmnnf8a",
		editRev: rev + "e", editCID: "bafyreih5xbmigkq5ikyhqiqhqzbwuqjxeitgtzwyxvjhfsfvswsxmnnf8b",
		timeUS: 1_775_000_015_000_000,
	}
}

// mbVoteEvent builds a native vote commit.
func mbVoteEvent(t *testing.T, did, rkey, rev, subjectURI, direction string, timeUS int64) *consume.JetstreamEvent {
	t.Helper()
	frame := fmt.Sprintf(`{
  "did": %q, "time_us": %d, "kind": "commit",
  "commit": {
    "rev": %q, "operation": "create",
    "collection": "social.coves.feed.vote",
    "rkey": %q, "cid": %q,
    "record": {
      "$type": "social.coves.feed.vote",
      "subject": {"uri": %q, "cid": %q},
      "direction": %q,
      "createdAt": "2026-08-13T12:00:00.000Z"
    }
  }
}`, did, timeUS, rev, rkey, mtPostCID, subjectURI, mtPostCID, direction)
	var event consume.JetstreamEvent
	require.NoError(t, json.Unmarshal([]byte(frame), &event), "the frame must be valid wire JSON")
	return &event
}

// bansIn counts the bans standing in one community.
func bansIn(t *testing.T, db *sql.DB, communityDID string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM community_bans WHERE community_did = $1`, communityDID).Scan(&n))
	return n
}
