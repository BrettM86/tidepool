package ingest

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/accept"
	"tidepool/internal/consume"
	"tidepool/internal/echo"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/outbound"
	"tidepool/internal/personas"
	"tidepool/internal/store"
)

// TASK 17c-1 — A MODERATOR'S REMOVAL IS REAL, AND IT IS TERMINAL.
//
// The removal path itself is already built: materialize.RemovePost does the one
// multi-op commit and moderateAnnouncedDelete already calls it. What has kept it
// unreachable for NATIVE content is authorization — CommunityDIDOf returns ""
// for a bridge-origin mapping with no community_did, so an announced delete of a
// native post is refused before it can act.
//
// Populating that column is this sub-run's prerequisite, and it arms a loaded
// gun (F2): the engine's auto-restore does not read the standing removal's code.
// It was written for OUR decisions — an admission-revoked removal that a
// corrective edit SHOULD reverse. The moment a moderator-discretion removal can
// exist for a native post, the next author edit deletes the moderator's removal,
// writes a fresh acceptance, and enqueues Update{Page} BACK TO THE COMMUNITY
// THAT REMOVED IT.
//
// That is the failure this file exists to prevent, and it is unrecoverable in
// the way that matters: it is not a number that reads wrong internally, it is a
// moderation decision reversed and pushed outward, over the wire, at the
// moderators who made it.
//
// THE FIXTURE HAS TWO COMMUNITIES, CO-HOSTED. Decision 18's rule is a
// CONJUNCTION — the signer must BE the community AND the target must belong to
// it — and with one community those are the same fact, so an implementation
// checking either one passes. Lemmy hosts many communities per instance and
// SameAuthority is true across all of them, so B-moderates-A's-content is
// ordinary traffic, not a thought experiment.
const (
	mtUserOrigin = "https://coves.social"

	// Community A owns the post.
	mtCommunityAName = "technology"
	// Community B is a DIFFERENT community on the SAME instance. Same authority,
	// same signer host, no claim whatsoever on A's content.
	mtCommunityBName = "science"
	mtCommunityBAPID = "https://lemmy.world/c/" + mtCommunityBName

	mtAuthorDID    = "did:plc:mtnativeauthor0001"
	mtAuthorHandle = "mtauthor.coves.social"
	// A SECOND native actor, who never authored the post. Bans are
	// three-dimensional — (community, actor, content) — so a fixture with one
	// actor cannot tell "cancel that actor's deliveries to that community" from
	// "cancel every delivery", and one with one community cannot tell "that
	// community" from "everywhere". Both are in the world from the start.
	mtCommenterDID    = "did:plc:mtnativecommnter1"
	mtCommenterHandle = "mtcommenter.coves.social"

	mtPostRKey  = "3lzmtpost00001"
	mtPostATURI = "at://" + mtAuthorDID + "/social.coves.community.postv2/" + mtPostRKey
	mtPostAPID  = mtUserOrigin + "/ap/object/" + mtAuthorDID +
		"/social.coves.community.postv2/" + mtPostRKey
	mtPostCID  = "bafyreievgu2ty7qbiaaom5zhmkznsnajuzideek3lo7e65dwqlrvrxnmo4"
	mtEditCID  = "bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqrsqxi3jgxte"
	mtPostRev  = "3lzmtrev000001"
	mtEditRev  = "3lzmtrev000002"
	mtTimeUS   = int64(1_775_000_000_000_000)
	mtEditTime = int64(1_775_000_000_000_001)
)

// moderationWorld is a native post accepted into community A, with community B
// standing beside it on the same instance.
type moderationWorld struct {
	groupA        *remoteActor
	groupB        *remoteActor
	communityADID string
	communityBDID string
	dispatcher    *consume.Dispatcher
	digestRKey    string
}

func newModerationWorld(t *testing.T, h *harness) moderationWorld {
	t.Helper()
	ctx := context.Background()
	groupA := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	communityADID := testDIDFor(mtCommunityAName, "lemmy.world")

	// --- Community B: followed, co-hosted, and able to sign its own deliveries.
	communityBDID := testDIDFor(mtCommunityBName, "lemmy.world")
	_, err := h.communities.UpsertCommunity(ctx, store.Community{
		APGroupID:         mtCommunityBAPID,
		DID:               communityBDID,
		PreferredUsername: mtCommunityBName,
		Instance:          "lemmy.world",
	})
	require.NoError(t, err)
	require.NoError(t, h.communities.SetFollowState(ctx, mtCommunityBAPID, store.FollowStateAccepted))
	groupB := h.newRemoteActor(mtCommunityBAPID, map[string]any{
		"type":              "Group",
		"id":                mtCommunityBAPID,
		"preferredUsername": mtCommunityBName,
		"inbox":             mtCommunityBAPID + "/inbox",
		"published":         "2024-01-01T00:00:00.000000Z",
	})

	// --- The native side: a persona service, the real enqueuer, the real
	//     acceptance engine, and the real consumer in front of them.
	userOrigin, err := personas.New(personas.Options{
		DB:         h.db,
		Custodian:  h.custodian,
		UserOrigin: mtUserOrigin,
	})
	require.NoError(t, err)
	// The user origin SERVES the post for real. The restore path re-fetches its
	// target from that target's own authority and re-materializes what comes
	// back, so without a live origin a restore is refused by the fetch failing —
	// and a test asserting "the restore was refused" would be passing on the
	// wrong reason entirely.
	h.mux.Handle("/ap/", userOrigin)
	enqueuer, err := outbound.NewEnqueuer(outbound.EnqueuerOptions{
		DB:         h.db,
		Translator: outbound.NewTranslator(mtUserOrigin),
		Inboxes:    outbound.NewInboxResolver(h.client, time.Minute),
		Actors:     store.NewAPActors(h.db),
		UserOrigin: mtUserOrigin,
	})
	require.NoError(t, err)
	engine, err := accept.NewEngine(accept.Options{
		Repos:       h.manager,
		Enqueuer:    enqueuer,
		Actors:      userOrigin,
		Resolver:    mtResolver{},
		Communities: h.communities,
		Objects:     store.NewOutboundObjects(h.db),
		Prefs:       store.NewFederationPrefs(h.db),
		Admissions:  accept.NewAdmissions(h.db),
		UserOrigin:  mtUserOrigin,
	})
	require.NoError(t, err)
	dispatcher, err := consume.NewDispatcher(consume.Options{
		DB:         h.db,
		Actors:     userOrigin,
		Enqueuer:   enqueuer,
		Resolver:   mtResolver{},
		Engine:     engine,
		UserOrigin: mtUserOrigin,
	})
	require.NoError(t, err)

	// --- The post is admitted through the real path: acceptance record,
	//     outbound state, ap_objects mapping, one Create{Page} enqueued.
	require.NoError(t, dispatcher.HandleEvent(ctx, mtPostEvent(t, "create", mtPostRev, mtPostCID, mtTimeUS)))

	digest := testDigestRKey(mtPostATURI)
	_, _, err = h.manager.GetRecord(ctx, communityADID, materialize.CollectionAcceptance, digest)
	require.NoError(t, err, "precondition: the post is accepted into community A")

	// --- The 17c PREREQUISITE, asserted rather than constructed. Every
	//     moderation behaviour below is downstream of this column, so a fixture
	//     that WROTE it would let an implementation that never populates it pass
	//     the whole suite — the intent field, the enqueuer's copy, and the
	//     backfill would all be unpinned by the tests that depend on them most.
	mapping, err := h.objects.GetByAPID(ctx, mtPostAPID)
	require.NoError(t, err, "the enqueuer maps the federated post")
	require.Equal(t, store.OriginBridge, mapping.Origin)
	require.Equal(t, communityADID, mapping.CommunityDID,
		"the enqueuer must bind the mapping to the community it federated into: "+
			"CommunityDIDOf reads this column, and an empty one refuses every announced "+
			"moderation action before it is evaluated")

	return moderationWorld{
		groupA: groupA, groupB: groupB,
		communityADID: communityADID, communityBDID: communityBDID,
		dispatcher: dispatcher, digestRKey: digest,
	}
}

// mtPostEvent builds a postv2 commit frame the consumer accepts.
func mtPostEvent(t *testing.T, operation, rev, cid string, timeUS int64) *consume.JetstreamEvent {
	t.Helper()
	return mtPostEventFor(t, mtPostRKey, operation, rev, cid, timeUS)
}

// mtPostEventFor is mtPostEvent for an arbitrary record key: a lock is
// per-OBJECT, so telling that apart from per-community needs a second thread in
// the SAME community — which needs a second post to root it.
func mtPostEventFor(t *testing.T, rkey, operation, rev, cid string, timeUS int64) *consume.JetstreamEvent {
	t.Helper()
	frame := fmt.Sprintf(`{
  "did": %q, "time_us": %d, "kind": "commit",
  "commit": {
    "rev": %q, "operation": %q,
    "collection": "social.coves.community.postv2",
    "rkey": %q, "cid": %q,
    "record": {
      "$type": "social.coves.community.postv2",
      "community": %q,
      "title": "a native post the community moderates",
      "content": "the body, later edited",
      "createdAt": "2026-08-13T10:00:00.000Z"
    }
  }
}`, mtAuthorDID, timeUS, rev, operation, rkey, cid, testDIDFor(mtCommunityAName, "lemmy.world"))
	var event consume.JetstreamEvent
	require.NoError(t, json.Unmarshal([]byte(frame), &event), "the frame must be valid wire JSON")
	return &event
}

type mtResolver struct{}

// mtHandles is per-DID rather than one answer for every DID: the persona's
// local part is DERIVED from the handle and frozen at mint, so a resolver that
// answered the same handle for two native actors would mint two personas
// fighting over one name — and the collision search would quietly hand the
// second one a suffixed identity that no assertion about "that actor" matches.
var mtHandles = map[string]string{
	mtAuthorDID:    mtAuthorHandle,
	mtCommenterDID: mtCommenterHandle,
}

func (mtResolver) ResolveDIDHandle(_ context.Context, did string) (string, error) {
	handle, ok := mtHandles[did]
	if !ok {
		// Loud, not a fallback: minting on a guessed handle freezes the guess.
		return "", fmt.Errorf("no test handle registered for %s", did)
	}
	return handle, nil
}

// admissionFor reads the ledger row for one (community, post).
func admissionFor(t *testing.T, db *sql.DB, communityDID, postURI string) (status, code string) {
	t.Helper()
	err := db.QueryRow(`
		SELECT status, decision_code FROM admissions
		WHERE community_did = $1 AND post_uri = $2`, communityDID, postURI).Scan(&status, &code)
	if err == sql.ErrNoRows {
		return "", ""
	}
	require.NoError(t, err)
	return status, code
}

// TestModeratorRemovalSurvivesAnAuthorEdit is the OUTER CONTRACT for 17c-1.
//
// GIVEN a native post federated into community A and a moderator removal
// standing against it, WHEN the author edits the post, THEN the removal
// survives, no acceptance is written, NOTHING goes outbound, and the ledger
// records the terminal decision.
//
// The outbound assertion is not a nicety. The harm is not that the removal
// disappeared from our repo — it is that we take the moderators' decision,
// reverse it, and PUSH THE POST BACK AT THEM. Every other consequence is
// internal and correctable; that one is on the wire.
func TestModeratorRemovalSurvivesAnAuthorEdit(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	// Snapshotted BEFORE the removal, not after: an enqueue caused by the
	// REMOVAL ITSELF would otherwise be folded into the baseline and invisible.
	// Boomerang suppression is structural today (materialize.RemovePost uses
	// ApplyOps, which takes no side effect and so cannot enqueue), but nothing
	// stops a refactor to ApplyOpsTx, and the failure would be a Delete{Page}
	// sent back at the community that just removed the post.
	activitiesAtStart := rowCount(t, h.db, "outbound_activities")
	deliveriesAtStart := rowCount(t, h.db, "outbound_deliveries")

	// --- A moderator of community A removes the post: Delete WITH summary,
	//     announced by the community that owns it.
	reason := "off topic for this community"
	h.announceDeleteWithSummary(world.groupA,
		"https://lemmy.world/activities/announce/delete/mt-removal", mtPostAPID, &reason)

	removal, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionRemoval, world.digestRKey)
	require.NoError(t, err,
		"precondition: with community_did populated the removal path is REACHABLE — this is "+
			"the whole point of the column, and the rest of this test is its consequence")
	require.Equal(t, "moderator-discretion", removal["code"])
	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, world.digestRKey)
	require.True(t, errors.IsNotFound(err), "precondition: the acceptance was withdrawn (err=%v)", err)

	activitiesBefore := rowCount(t, h.db, "outbound_activities")
	deliveriesBefore := rowCount(t, h.db, "outbound_deliveries")
	assert.Equal(t, activitiesAtStart, activitiesBefore,
		"the REMOVAL itself must enqueue nothing: an inbound moderation action is the "+
			"community telling US what it did, and echoing it back is a Delete{Page} aimed "+
			"at the moderators who sent it")
	assert.Equal(t, deliveriesAtStart, deliveriesBefore, "...and no delivery")

	// --- WHEN: the author edits their post. An ordinary commit, the kind that
	//     happens minutes later when someone fixes a typo.
	require.NoError(t, world.dispatcher.HandleEvent(ctx,
		mtPostEvent(t, "update", mtEditRev, mtEditCID, mtEditTime)))

	// --- THEN: the moderators' decision stands.
	// assert, not require: every consequence below is a separate harm, and the
	// outbound one is the harm that leaves the building.
	stillRemoved, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionRemoval, world.digestRKey)
	if assert.NoError(t, err,
		"the removal MUST survive the edit: auto-restore exists to reverse OUR OWN "+
			"admission-revoked decisions, and applying it to a moderator's removal reverses "+
			"a decision we had no part in") {
		assert.Equal(t, "moderator-discretion", stillRemoved["code"],
			"and it must still be the moderator's removal, not one we rewrote")
		assert.Equal(t, reason, stillRemoved["reason"], "with their reason intact")
	}

	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, world.digestRKey)
	assert.True(t, errors.IsNotFound(err),
		"and NO acceptance may be written: an acceptance beside a standing removal is a post "+
			"that reads as both admitted and removed, and Coves resolves that by showing it "+
			"(err=%v)", err)

	assert.Equal(t, activitiesBefore, rowCount(t, h.db, "outbound_activities"),
		"NOTHING may be enqueued: this is the unrecoverable half — an Update{Page} here "+
			"pushes the removed post back at the very moderators who removed it, over the "+
			"wire, where no later fix can retract it")
	assert.Equal(t, deliveriesBefore, rowCount(t, h.db, "outbound_deliveries"),
		"...and no delivery either")

	// --- And the decision is legible afterwards: an operator asking why the
	//     edit did not publish must find the answer in the ledger.
	status, code := admissionFor(t, h.db, world.communityADID, mtPostATURI)
	assert.Equal(t, accept.StatusRemoved, status,
		"the ledger records the post as REMOVED, not accepted: it is the surface an operator "+
			"reads when the author asks why their edit did nothing")
	assert.NotEmpty(t, code, "with a machine-readable why")
	assert.NotEqual(t, accept.RemovalCodeAdmissionRevoked, code,
		"and it must NOT read as our own admission revocation — that code means 'we withdrew "+
			"this and a corrective edit may restore it', which is exactly the reasoning that "+
			"must not apply here. The specific string is GREEN's to choose")
}

// TestCrossCommunityRemovalIsRefused is the RELATIONAL case: community B, on the
// same instance as A, announces a removal of A's post.
//
// Decision 18's conjunction collapses in a one-community fixture, so this is the
// only shape that can tell "the signer IS the community" from "the target is IN
// the community". Lemmy co-hosts communities by design and SameAuthority is true
// across all of them, so nothing about this delivery is malformed — it is simply
// not B's post to moderate.
func TestCrossCommunityRemovalIsRefused(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	reason := "not your community's post"
	h.announceDeleteWithSummary(world.groupB,
		"https://lemmy.world/activities/announce/delete/mt-cross", mtPostAPID, &reason)

	_, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionRemoval, world.digestRKey)
	assert.True(t, errors.IsNotFound(err),
		"community B may not remove community A's post: one moderator team would otherwise "+
			"be able to withdraw content from every community co-hosted with it (err=%v)", err)

	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, world.digestRKey)
	assert.NoError(t, err,
		"and A's acceptance is untouched: the refusal must leave the post exactly as A "+
			"admitted it")

	_, _, err = h.manager.GetRecord(ctx,
		world.communityBDID, materialize.CollectionRemoval, world.digestRKey)
	assert.True(t, errors.IsNotFound(err),
		"nor may B record the removal in its OWN repo: a removal for a post that was never "+
			"accepted there is a moderation record about somebody else's content (err=%v)", err)

	// The post is still live for its own community, so an edit still publishes.
	require.NoError(t, world.dispatcher.HandleEvent(ctx,
		mtPostEvent(t, "update", mtEditRev, mtEditCID, mtEditTime)))
	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, world.digestRKey)
	assert.NoError(t, err,
		"a refused cross-community removal must not leave the post in a state where its own "+
			"community's edits stop working")
}

// TestAnnouncedRestoreOfANativePostLiftsTheRemovalCleanly is RESTORE-1.
//
// Populating community_did made announced Undo{Delete} reachable for
// BRIDGE-ORIGIN content for the first time, and that path has no origin guard.
// handleUndoDelete re-fetches the target from its own authority — which for a
// native post is OUR OWN /ap/object/… id — and hands the result to
// mat.HandleUpdate DIRECTLY, bypassing materializeContent's bridge-origin echo
// guard, the only place that says "this is ours".
//
// What follows is a chain of consequences, each worse than the last:
//
//	HandleUpdate → MaterializePost → EnsureActor(our own persona's actor id)
//	  → MINTS a PLC DID and a bridged_actors row for a native Coves user;
//	then commitRecord targets the AUTHOR's repo, which the bridge does not host,
//	  so signing fails and HandleUpdate errors;
//	then the error path compensates by SOFT-DELETING our own bridge-origin
//	  mapping and recording a tombstone for our own AP id — after which
//	  moderateAnnouncedDelete declines forever on mapping.IsDeleted().
//
// That last step is the one that does not wash out: the post becomes
// permanently unmoderatable, by the community's own legitimate restore.
//
// There is nothing to re-materialize here. The record lives in the author's
// repo and the removal never touched it; a restore of a native post is the
// acceptance coming back, nothing more.
func TestAnnouncedRestoreOfANativePostLiftsTheRemovalCleanly(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	reason := "removed, then reconsidered"
	h.announceDeleteWithSummary(world.groupA,
		"https://lemmy.world/activities/announce/delete/mt-restore", mtPostAPID, &reason)
	_, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionRemoval, world.digestRKey)
	require.NoError(t, err, "precondition: the removal stands")

	bridgedBefore := rowCount(t, h.db, "bridged_actors")

	// The community lifts its own removal, honestly signed.
	h.announceUndoDelete(world.groupA,
		"https://lemmy.world/activities/announce/undo/mt-restore",
		"https://lemmy.world/activities/announce/delete/mt-restore/delete",
		mtPostAPID, &reason)

	// --- The restore lands: the acceptance is back.
	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, world.digestRKey)
	assert.NoError(t, err,
		"the community's own Undo{Delete} must re-accept the post: a removal the moderators "+
			"lifted that stays standing is moderation nobody can undo")
	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionRemoval, world.digestRKey)
	assert.True(t, errors.IsNotFound(err),
		"and the removal is gone with it — acceptance and removal share one rkey and one "+
			"commit precisely so neither outlives the other (err=%v)", err)

	// --- And nothing of ours was mistaken for remote content on the way.
	assert.Equal(t, bridgedBefore, rowCount(t, h.db, "bridged_actors"),
		"NO bridged actor may be minted: EnsureActor runs on the re-materialization path, "+
			"and a native Coves user acquiring a second, bridge-minted fediverse identity is "+
			"the mint oracle this loop has closed twice already")

	mapping, err := h.objects.GetByAPID(ctx, mtPostAPID)
	require.NoError(t, err)
	assert.False(t, mapping.IsDeleted(),
		"our own mapping must not be soft-deleted: the compensation path does that when the "+
			"re-materialization fails, and moderateAnnouncedDelete then declines forever on "+
			"IsDeleted() — the post becomes permanently unmoderatable, by a legitimate restore")
	assert.Equal(t, store.OriginBridge, mapping.Origin,
		"and it must still be ours: re-materializing rewrites the row as fediverse-origin, "+
			"which silently disables the echo guard for this post")

	tombstoned, err := h.tombstones.ExistsFor(ctx, mtPostAPID, groupID)
	require.NoError(t, err)
	assert.False(t, tombstoned,
		"nor may a tombstone be recorded against our own AP id: it suppresses this post's "+
			"own later Creates and drops every Lemmy reply beneath it")

	event, err := h.events.GetEvent(ctx, "https://lemmy.world/activities/announce/undo/mt-restore")
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt,
		"and the restore is DECIDED, not left retrying: %s", event.Error)
	assert.Nil(t, event.FailedAt, "nor poisoned")
}

// TestRestoredAcceptancePinsTheEditedVersion is PIN-1.
//
// RestorePost's own doc says the fresh acceptance pins "the post's CURRENT
// version, not the one that was removed: the author may have edited it while it
// was out". Terminality made that false in the common case — an edit against a
// standing moderator removal is refused and writes NOTHING to outbound_objects,
// so LastCID keeps naming the pre-removal version, which is exactly the one that
// was removed.
//
// The consequence is not internal. The community SIGNS an acceptance whose
// strongRef names a CID that may no longer resolve in the author's PDS, and the
// "self-heals on the author's next edit" argument rests on an edit that may
// never come — the author has no reason to edit again, because from their side
// the post is back.
//
// The assertion is on the OUTCOME, not the source: an authority-pinned
// getRecord and the terminal admission's EvaluatedCID both satisfy it, and which
// one is right is GREEN's call.
func TestRestoredAcceptancePinsTheEditedVersion(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	// A moderator removes the post.
	reason := "removed pending an edit"
	h.announceDeleteWithSummary(world.groupA,
		"https://lemmy.world/activities/announce/delete/mt-pin", mtPostAPID, &reason)
	require.True(t, removalStandsFor(t, h, world), "precondition: the removal stands")

	// The author edits it WHILE REMOVED. The edit is refused (terminal) and
	// writes nothing outbound — which is precisely why LastCID goes stale.
	require.NoError(t, world.dispatcher.HandleEvent(ctx,
		mtPostEvent(t, "update", mtEditRev, mtEditCID, mtEditTime)))
	require.True(t, removalStandsFor(t, h, world),
		"precondition: the edit did not reverse the removal (17c-1's terminality)")

	// The moderators reconsider and restore it.
	h.announceUndoDelete(world.groupA,
		"https://lemmy.world/activities/announce/undo/mt-pin",
		"https://lemmy.world/activities/announce/delete/mt-pin/delete",
		mtPostAPID, &reason)

	acceptance, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, world.digestRKey)
	require.NoError(t, err, "the restore must re-accept the post")
	subject, ok := acceptance["subject"].(map[string]any)
	require.True(t, ok, "the acceptance carries a strongRef, got %#v", acceptance["subject"])

	assert.Equal(t, mtEditCID, subject["cid"],
		"the acceptance must pin the CURRENT record: the author edited while the post was "+
			"out, and pinning the pre-removal CID signs the community's name to a version "+
			"that may no longer resolve in the author's PDS — the one version we know the "+
			"moderators did NOT reinstate")
	assert.NotEqual(t, mtPostCID, subject["cid"],
		"and specifically not the version that was removed")
}

// removalStandsFor reports whether community A currently holds a removal for the
// fixture's post.
func removalStandsFor(t *testing.T, h *harness, world moderationWorld) bool {
	t.Helper()
	_, _, err := h.manager.GetRecord(context.Background(),
		world.communityADID, materialize.CollectionRemoval, world.digestRKey)
	if errors.IsNotFound(err) {
		return false
	}
	require.NoError(t, err)
	return true
}

// TestSummarylessCrossCommunityDeleteIsRefused bounds the one attribution the
// announced-delete path takes on trust.
//
// moderateAnnouncedDelete's summary-less branch asks whether the INNER Delete's
// actor is the post's author — an unverified claim, since only the announcing
// community's signature is checked. The bound is that authorization runs FIRST:
// whatever the inner actor claims, the announcer must own the target's mapping,
// so at most a community can withdraw a post from ITSELF.
//
// The inner actor here is a LEMMY moderator, not our persona: a summary-less
// delete attributed to one of OUR personas never reaches this branch at all (see
// the sibling test below), so attributing it that way would pin the echo guard
// while claiming to pin authorization.
func TestSummarylessCrossCommunityDeleteIsRefused(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	h.announceDeleteBy(world.groupB,
		"https://lemmy.world/activities/announce/delete/mt-summaryless",
		mtPostAPID, modActorID, nil)

	_, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionRemoval, world.digestRKey)
	assert.True(t, errors.IsNotFound(err),
		"a community that does not own the mapping decides nothing about it, whoever the "+
			"inner activity claims to be (err=%v)", err)
	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, world.digestRKey)
	assert.NoError(t, err, "A's acceptance is untouched")

	mapping, err := h.objects.GetByAPID(ctx, mtPostAPID)
	require.NoError(t, err)
	assert.False(t, mapping.IsDeleted(),
		"and the post is not deleted either: the summary-less branch's other outcome is the "+
			"author's own delete, which would destroy the record")
}

// TestSummarylessDeleteAttributedToOurPersonaIsDroppedAsAnEcho records where the
// unverified attribution actually lands for NATIVE content.
//
// A native post's author IS one of our personas, so a truthful summary-less
// self-delete announced back by the community is indistinguishable from our own
// Delete coming home — and the echo classifier takes it first, by the inner
// ACTOR, before any authorization runs.
//
// That is the M1 behaviour working as designed, and it means the attribution
// this branch trusts is unreachable for native posts from either direction: a
// forged persona attribution is dropped as an echo, and a foreign attribution is
// bounded by the ownership conjunct above.
func TestSummarylessDeleteAttributedToOurPersonaIsDroppedAsAnEcho(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)
	before := dropSnapshot()

	// Community A — the OWNER, so ownership cannot be what refuses this.
	h.announceDeleteBy(world.groupA,
		"https://lemmy.world/activities/announce/delete/mt-persona-attributed",
		mtPostAPID, mtUserOrigin+"/ap/actor/"+mtAuthorDID, nil)

	_, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, world.digestRKey)
	assert.NoError(t, err, "the post stays accepted")
	mapping, err := h.objects.GetByAPID(ctx, mtPostAPID)
	require.NoError(t, err)
	assert.False(t, mapping.IsDeleted(), "and its record is not destroyed")

	assert.Equal(t, before[echo.ClassLocalActor]+1, echo.Drops(echo.ClassLocalActor),
		"it is dropped as an ECHO, by the inner actor — which is what makes the summary-less "+
			"branch's unverified author attribution unreachable for native posts")
}

// TestRestoreWithNoInterveningEditPinsTheFederatedVersion covers the FALLBACK
// half of restorePin's pair.
//
// PIN-1 arose because a primary/fallback pair had only its fallback exercised;
// wiring the ledger fixes that and creates the mirror risk — every restore now
// takes the primary, and LastCID's correctness stops being tested at all.
//
// The case that must still work is the ordinary one: a moderator removes a post
// and reinstates it with NO author edit in between. The ledger holds no decision
// for that post beyond its acceptance, so the pin comes from outbound state —
// and there it is right, because nothing has changed since it was federated.
func TestRestoreWithNoInterveningEditPinsTheFederatedVersion(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	reason := "removed and reinstated, no edit in between"
	h.announceDeleteWithSummary(world.groupA,
		"https://lemmy.world/activities/announce/delete/mt-nofallback", mtPostAPID, &reason)
	require.True(t, removalStandsFor(t, h, world), "precondition: the removal stands")

	h.announceUndoDelete(world.groupA,
		"https://lemmy.world/activities/announce/undo/mt-nofallback",
		"https://lemmy.world/activities/announce/delete/mt-nofallback/delete",
		mtPostAPID, &reason)

	acceptance, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, world.digestRKey)
	require.NoError(t, err, "the restore re-accepts the post")
	subject, ok := acceptance["subject"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, mtPostCID, subject["cid"],
		"with no edit to supersede it, the version we federated IS the current one — the "+
			"fallback has to be right for the common case, or fixing the stale pin just moves "+
			"the staleness into whichever branch nobody exercises")
}
