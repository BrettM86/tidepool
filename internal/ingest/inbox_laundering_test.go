package ingest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// ACTOR LAUNDERING AT THE INBOX.
//
// The inbox verifies ONE actor's signature and then lets the activity's `actor`
// field name a DIFFERENT actor on the same host, queueing that CLAIM as the
// event's bound ActorID. Downstream, handleAnnounce resolves the announcing
// community from that id — so the attacker chooses which community is treated
// as verified.
//
// That was defensible while every downstream decision was AUTHORITY-based
// ("lemmy.world may speak for lemmy.world's users"). Decision 18's rule is
// IDENTITY-based: the signer must BE the community that owns the target. The
// binding silently downgrades identity to authority, and 17c-1 is what made the
// difference reachable — before it, moderation of native content was refused for
// unrelated reasons.
//
// WHY THE EXISTING CROSS-COMMUNITY TEST CANNOT SEE THIS: it has community B sign
// AS ITSELF and be refused by the membership rule. The attack is a third party
// CLAIMING TO BE community A, which passes membership because the claim IS A.
// The laundering happens one layer earlier than the check we pinned, so a
// fixture that signs honestly cannot express it.
//
// Every test here asserts STATE, not HTTP status: refusing at the inbox (403)
// and queueing-then-refusing at the handler are both legitimate fixes, and
// pinning the status would pick one for GREEN.
const (
	// An ordinary user account on the SAME instance as the community. Nothing
	// about this actor is privileged; that is the point.
	lnAttacker = "https://lemmy.world/u/attacker"
)

// laundered builds an Announce whose `actor` field claims to be claimedActor.
// The caller signs it with somebody else's key.
func laundered(activityID, claimedActor string, inner map[string]any) map[string]any {
	return map[string]any{
		"@context": "https://www.w3.org/ns/activitystreams",
		"id":       activityID,
		"type":     "Announce",
		"actor":    claimedActor,
		"audience": claimedActor,
		"cc":       []any{claimedActor + "/followers"},
		"object":   inner,
	}
}

// innerModDelete is the live Lemmy mod-removal shape: a Delete carrying a
// summary, attributed to a moderator.
func innerModDelete(activityID, targetID, community, summary string) map[string]any {
	return map[string]any{
		"id":       activityID + "/delete",
		"type":     "Delete",
		"actor":    modActorID,
		"object":   targetID,
		"summary":  summary,
		"audience": community,
		"cc":       []any{community},
	}
}

// TestForgedAnnouncerCannotRemoveNativeContent is L1: the attack itself.
//
// A user account signs the delivery; the body claims to be the community. Both
// are on lemmy.world, so the same-authority tolerance admits it and the queued
// ActorID becomes the community the ATTACKER named.
//
// If this passes to the handler, any account on any instance can withdraw native
// content from any community co-hosted with it — the moderation surface 17c-1
// opened, driven by whoever asks.
func TestForgedAnnouncerCannotRemoveNativeContent(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)
	attacker := h.newRemoteActor(lnAttacker, person(lnAttacker, "attacker", nil))

	activitiesBefore := rowCount(t, h.db, "outbound_activities")

	_ = h.deliver(attacker, laundered(
		"https://lemmy.world/activities/announce/delete/ln-forged",
		groupID, // the CLAIM: community A
		innerModDelete("https://lemmy.world/activities/announce/delete/ln-forged",
			mtPostAPID, groupID, "removed by someone who is not a moderator")))
	h.drain()

	_, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionRemoval, world.digestRKey)
	assert.True(t, errors.IsNotFound(err),
		"a delivery SIGNED BY a user account must never act as the community it names: the "+
			"signature is the only evidence of identity we have, and everything decision 18 "+
			"rules on is downstream of it (err=%v)", err)

	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, world.digestRKey)
	assert.NoError(t, err,
		"and the post stays accepted: the community said it belongs there and no one has "+
			"said otherwise")

	assert.Equal(t, activitiesBefore, rowCount(t, h.db, "outbound_activities"),
		"nothing may go outbound off a forged delivery")
}

// TestForgedAnnouncerCannotRestoreRemovedContent is L2, the mirror.
//
// Restore is the same authority in the other direction: a standing moderator
// removal must not be liftable by anyone who can spell the community's id. If
// only the removal path is fixed, an attacker cannot remove a post — but can
// reinstate every post the moderators removed, which is the same power.
func TestForgedAnnouncerCannotRestoreRemovedContent(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)
	attacker := h.newRemoteActor(lnAttacker, person(lnAttacker, "attacker", nil))

	// The community's own moderator removes the post, honestly signed.
	reason := "removed by an actual moderator"
	h.announceDeleteWithSummary(world.groupA,
		"https://lemmy.world/activities/announce/delete/ln-real", mtPostAPID, &reason)
	standing, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionRemoval, world.digestRKey)
	require.NoError(t, err, "precondition: a real removal stands")
	require.Equal(t, reason, standing["reason"])

	// The attacker tries to lift it, claiming to be the community.
	inner := innerModDelete("https://lemmy.world/activities/announce/delete/ln-real",
		mtPostAPID, groupID, reason)
	_ = h.deliver(attacker, laundered(
		"https://lemmy.world/activities/announce/undo/ln-forged-restore",
		groupID,
		map[string]any{
			"id":       "https://lemmy.world/activities/announce/undo/ln-forged-restore/undo",
			"type":     "Undo",
			"actor":    modActorID,
			"audience": groupID,
			"cc":       []any{groupID},
			"object":   inner,
		}))
	h.drain()

	stillRemoved, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionRemoval, world.digestRKey)
	assert.NoError(t, err,
		"the moderators' removal must survive a forged restore: reinstating content they "+
			"removed is the same power as removing content they kept")
	if err == nil {
		assert.Equal(t, reason, stillRemoved["reason"], "unchanged, with their reason")
	}

	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, world.digestRKey)
	assert.True(t, errors.IsNotFound(err),
		"and no acceptance may be written back: that is what would make the post visible "+
			"again in Coves (err=%v)", err)

	// THE REFUSAL MUST BE A DECISION. Today the forged restore is authorized and
	// reaches re-materialization; it fails only because that writes into the
	// native AUTHOR's repo, which the bridge does not host — an accident of
	// where native posts live, not a judgement about who sent this. The event is
	// then left retrying against a forgery, which is what an unrefused attack
	// looks like from the queue's side.
	event, err := h.events.GetEvent(ctx,
		"https://lemmy.world/activities/announce/undo/ln-forged-restore")
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt,
		"a forged delivery must be DECIDED — skipped, once. Left retrying, it means the "+
			"identity was accepted and only the write failed: the same forgery against a "+
			"target whose repo we DO host would land (last error: %s)", event.Error)
	assert.Nil(t, event.FailedAt, "and it must not poison either: nothing here is retryable")
}

// TestSiblingCommunityCannotImpersonateTheOwningCommunity is L4.
//
// This is the shape closest to the cross-community test that already passes, and
// the difference is the whole point: there, B signs as B and the MEMBERSHIP rule
// refuses it (A's post is not B's). Here B signs as B but CLAIMS to be A, so the
// membership rule compares A against A and passes.
//
// A fix that only tightens membership therefore cannot satisfy this test — the
// identity has to stop being forgeable.
func TestSiblingCommunityCannotImpersonateTheOwningCommunity(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	_ = h.deliver(world.groupB, laundered(
		"https://lemmy.world/activities/announce/delete/ln-sibling",
		groupID, // community B signs, but claims to be community A
		innerModDelete("https://lemmy.world/activities/announce/delete/ln-sibling",
			mtPostAPID, groupID, "sibling community claiming to be the owner")))
	h.drain()

	_, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionRemoval, world.digestRKey)
	assert.True(t, errors.IsNotFound(err),
		"community B may not become community A by saying so: membership passes here (the "+
			"claim IS the owning community), so only the signature can refuse it (err=%v)", err)

	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, world.digestRKey)
	assert.NoError(t, err, "A's acceptance is untouched")
}

// TestHonestlySignedModerationStillWorks is L3, the control.
//
// The tolerance being exploited exists to admit deliveries whose signer is not
// byte-identical to the activity's actor. Whatever GREEN does to it, the
// ordinary path must keep working: the community signs its own Announce and
// moderates its own content. A fix that refuses this refuses all moderation.
func TestHonestlySignedModerationStillWorks(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	reason := "genuinely off topic"
	h.announceDeleteWithSummary(world.groupA,
		"https://lemmy.world/activities/announce/delete/ln-honest", mtPostAPID, &reason)

	removal, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionRemoval, world.digestRKey)
	require.NoError(t, err,
		"the community moderating its OWN content is the behaviour 17c-1 exists to deliver; "+
			"a laundering fix that also refuses this has removed the feature")
	assert.Equal(t, reason, removal["reason"])
	assert.Equal(t, "moderator-discretion", removal["code"])
}

// TestCrossAuthorityClaimIsStillRejected pins the half of the binding that is
// unambiguously right, so a fix cannot regress it while rewriting the rest: a
// claimed actor on a DIFFERENT host than the signer is refused outright.
//
// This is the case the current comment is really about — "without letting host A
// speak for host B" — and it must survive whatever replaces the same-authority
// tolerance.
func TestCrossAuthorityClaimIsStillRejected(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)
	foreign := "https://evil.example/u/attacker"
	attacker := h.newRemoteActor(foreign, person(foreign, "attacker", nil))

	_ = h.deliver(attacker, laundered(
		"https://evil.example/activities/announce/delete/ln-cross-authority",
		groupID,
		innerModDelete("https://evil.example/activities/announce/delete/ln-cross-authority",
			mtPostAPID, groupID, "from another host entirely")))
	h.drain()

	_, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionRemoval, world.digestRKey)
	assert.True(t, errors.IsNotFound(err),
		"a cross-authority claim is refused at the door and must stay refused (err=%v)", err)
	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, world.digestRKey)
	assert.NoError(t, err)
}

// TestForgedAcceptCannotSubscribeUsToACommunity is the second reachable
// escalation, reported alongside the moderation one.
//
// handleAccept resolves the community from the same bound id and then checks
// `communityID != signer` — which, under laundering, compares the claim against
// itself and can never fail. So any account on the instance can drive a pending
// follow to ACCEPTED and trigger a backfill of that community's whole outbox.
//
// The follow state machine is not moderation, but it is the same forged
// identity, and it is the path that pulls content INTO the bridge.
func TestForgedAcceptCannotSubscribeUsToACommunity(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	_ = newModerationWorld(t, h)
	attacker := h.newRemoteActor(lnAttacker, person(lnAttacker, "attacker", nil))

	// A third community on the same instance, subscribed but NOT yet accepted.
	const pendingAPID = "https://lemmy.world/c/pending"
	_, err := h.communities.UpsertCommunity(ctx, store.Community{
		APGroupID:         pendingAPID,
		DID:               testDIDFor("pending", "lemmy.world"),
		PreferredUsername: "pending",
		Instance:          "lemmy.world",
	})
	require.NoError(t, err)
	require.NoError(t, h.communities.SetFollowState(ctx, pendingAPID, store.FollowStatePending))
	backfillsBefore := h.backfills.count()

	_ = h.deliver(attacker, map[string]any{
		"@context": "https://www.w3.org/ns/activitystreams",
		"id":       "https://lemmy.world/activities/accept/ln-forged",
		"type":     "Accept",
		"actor":    pendingAPID, // the CLAIM
		"object": map[string]any{
			"id":     "https://" + bridgeHost + "/activities/follow/whatever",
			"type":   "Follow",
			"actor":  h.service.ID,
			"object": pendingAPID,
		},
	})
	h.drain()

	community, err := h.communities.GetByAPGroupID(ctx, pendingAPID)
	require.NoError(t, err)
	assert.Equal(t, store.FollowStatePending, community.FollowState,
		"only the community itself can accept our Follow: followCommunity's check compares "+
			"the claimed community against the bound id, so under laundering it compares a "+
			"value with itself and cannot fail")
	assert.Equal(t, backfillsBefore, h.backfills.count(),
		"and no backfill may be triggered by a stranger: it is an outbound crawl of a whole "+
			"community's history, started by whoever asks")
}
