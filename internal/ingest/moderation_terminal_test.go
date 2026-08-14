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
	mtPostRKey     = "3lzmtpost00001"
	mtPostATURI    = "at://" + mtAuthorDID + "/social.coves.community.postv2/" + mtPostRKey
	mtPostAPID     = mtUserOrigin + "/ap/object/" + mtAuthorDID +
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

	// --- The 17c PREREQUISITE, constructed the way 17c will leave the world.
	//     Without community_did on the mapping, CommunityDIDOf returns "" and
	//     every announced moderation action is refused before it is evaluated —
	//     which is the accident that has been standing in for authorization.
	//     GREEN carries this column through the intent; the fixture states the
	//     end state so these behaviours can be pinned against it.
	mapping, err := h.objects.GetByAPID(ctx, mtPostAPID)
	require.NoError(t, err, "the enqueuer maps the federated post")
	require.Equal(t, store.OriginBridge, mapping.Origin)
	mapping.CommunityDID = communityADID
	_, err = h.objects.PutMapping(ctx, *mapping)
	require.NoError(t, err)

	return moderationWorld{
		groupA: groupA, groupB: groupB,
		communityADID: communityADID, communityBDID: communityBDID,
		dispatcher: dispatcher, digestRKey: digest,
	}
}

// mtPostEvent builds a postv2 commit frame the consumer accepts.
func mtPostEvent(t *testing.T, operation, rev, cid string, timeUS int64) *consume.JetstreamEvent {
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
}`, mtAuthorDID, timeUS, rev, operation, mtPostRKey, cid, testDIDFor(mtCommunityAName, "lemmy.world"))
	var event consume.JetstreamEvent
	require.NoError(t, json.Unmarshal([]byte(frame), &event), "the frame must be valid wire JSON")
	return &event
}

type mtResolver struct{}

func (mtResolver) ResolveDIDHandle(context.Context, string) (string, error) {
	return mtAuthorHandle, nil
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
