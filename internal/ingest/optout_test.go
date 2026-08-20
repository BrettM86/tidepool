package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/accept"
	"tidepool/internal/consume"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// TASK 17d — THE OPT-OUT LIFECYCLE, SOFT TIER.
//
// Federation is DEFAULT-ON, so this record is the only way a user says stop —
// and what "stop" has to mean is two facts at once: nothing new goes out, AND
// nothing already queued goes out either. Today only the first is applied. The
// preference is recorded and the actor mirror is flipped, and the deliveries the
// user's own earlier posts left in the queue keep leaving, minutes or hours
// after they asked us not to.
//
// The two writes must land TOGETHER. Half of this state is a silent wrong
// answer in either direction: an actor disabled with live deliveries keeps
// publishing for someone who opted out, and cancelled deliveries under an actor
// still marked enabled loses the user's queued work while the bridge believes it
// is still federating for them. Nothing retries either half — the record is
// applied once, under a rev gate that rejects the replay.
//
// WHAT IS NOT REVOKED: the actor DOCUMENT. Peers that already hold this user's
// ids must still be able to resolve them, or every existing thread on the
// fediverse side breaks its author reference. Opting out stops the bridge
// SPEAKING for someone; it does not retract who they were. That is the 410 in
// the destructive tier, and the difference between the tiers is the whole
// design.

const (
	// The opt-out record. rkey "self" is the lexicon's, and the rev is reused
	// verbatim on the re-enable so the two commits are ordered by the gate the
	// way a real client's would be.
	odOptOutRev   = "3lzodrev000001"
	odReEnableRev = "3lzodrev000002"

	// Posts that leave queued work in two communities for one author, and one
	// for the OTHER author — the axis that tells "cancel this actor's work" from
	// "cancel this community's".
	odPostInARKey = "3lzodpost00001"
	odPostInBRKey = "3lzodpost00002"
	odOtherRKey   = "3lzodpost00003"
	odAfterRKey   = "3lzodpost00004"
)

// TestOptingOutDisablesTheActorAndCancelsItsQueuedWork is the OUTER CONTRACT for
// 17d's soft tier.
//
// GIVEN a native author with pending deliveries to TWO communities, WHEN they
// write enabled=false, THEN the actor is disabled AND every pending delivery of
// theirs is cancelled, the other author's work is untouched, their actor
// document is still served, nothing is enqueued — and when they re-enable,
// admission resumes.
func TestOptingOutDisablesTheActorAndCancelsItsQueuedWork(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	// --- GIVEN: queued work in both communities, for two different authors.
	admitPost(t, world, mtAuthorDID, odPostInARKey, world.communityADID, "3lzodrev000010", 1_775_000_020_000_001)
	admitPost(t, world, mtAuthorDID, odPostInBRKey, world.communityBDID, "3lzodrev000011", 1_775_000_020_000_002)
	admitPost(t, world, mtCommenterDID, odOtherRKey, world.communityADID, "3lzodrev000012", 1_775_000_020_000_003)

	requireEveryDelivery(t, h.db, mtAuthorDID, groupID, "pending",
		"precondition: the opting-out author has queued work for A")
	requireEveryDelivery(t, h.db, mtAuthorDID, mtCommunityBAPID, "pending",
		"precondition: and for B — one community could not tell 'this actor's work' from "+
			"'this community's'")
	requireEveryDelivery(t, h.db, mtCommenterDID, groupID, "pending",
		"precondition: and the OTHER author has queued work of their own")

	activitiesBefore := rowCount(t, h.db, "outbound_activities")
	deliveriesBefore := rowCount(t, h.db, "outbound_deliveries")

	// --- WHEN: they opt out. Soft tier: no deleteRemote, nothing inferred.
	require.NoError(t, world.dispatcher.HandleEvent(ctx,
		odFederationEvent(t, mtAuthorDID, odOptOutRev, "create", false, false, 1_775_000_021_000_001)))

	// --- THEN: the actor is disabled...
	actor, err := store.NewAPActors(h.db).GetByDID(ctx, mtAuthorDID)
	require.NoError(t, err, "the actor still EXISTS: opting out disables it, it does not erase it")
	assert.False(t, actor.Enabled,
		"and it is disabled: this mirror is what the consumer, the admission gate and the "+
			"delivery claim all read to answer 'may we still speak for this user'")

	// ...and every delivery they had queued is cancelled, in BOTH communities.
	assertEveryDelivery(t, h.db, mtAuthorDID, groupID, "cancelled",
		"their queued work for A is cancelled: a user who has asked us to stop and then "+
			"watches their posts keep arriving on Lemmy for the next hour has been told no "+
			"twice — once by us, once by the queue")
	assertEveryDelivery(t, h.db, mtAuthorDID, mtCommunityBAPID, "cancelled",
		"and their work for B with it: the opt-out is about the ACTOR, so a cancellation "+
			"scoped to one community leaves them federating everywhere else they ever posted")

	// ...while the other author is untouched.
	assertEveryDelivery(t, h.db, mtCommenterDID, groupID, "pending",
		"the OTHER author's work stands: cancelling by community — or by anything but this "+
			"actor — silences people who asked for nothing")

	// --- AND: their actor document is STILL SERVED.
	doc, err := h.client.FetchActor(ctx, mtUserOrigin+"/ap/actor/"+mtAuthorDID)
	require.NoError(t, err,
		"the actor document must still resolve: every Note and Page this bridge already "+
			"delivered names this actor, and a 404 breaks the author reference on every one "+
			"of them — retracting the identity is the DESTRUCTIVE tier, and the difference "+
			"between the two is what the user chose")
	assert.Equal(t, mtUserOrigin+"/ap/actor/"+mtAuthorDID, doc.ID)

	// --- AND: nothing went out about it.
	assert.Equal(t, activitiesBefore, rowCount(t, h.db, "outbound_activities"),
		"NO outbound activity: the soft tier tells peers nothing — a Delete{Person} here "+
			"would be the destructive tier applied to a user who did not ask for it, and no "+
			"peer un-deletes")
	assert.Equal(t, deliveriesBefore, rowCount(t, h.db, "outbound_deliveries"),
		"and no new delivery; the cancellation changes state, it does not add rows")

	// --- WHEN: they change their mind.
	require.NoError(t, world.dispatcher.HandleEvent(ctx,
		odFederationEvent(t, mtAuthorDID, odReEnableRev, "create", true, false, 1_775_000_022_000_001)))

	reenabled, err := store.NewAPActors(h.db).GetByDID(ctx, mtAuthorDID)
	require.NoError(t, err)
	assert.True(t, reenabled.Enabled, "the actor is enabled again, under its ORIGINAL identity")

	admitPost(t, world, mtAuthorDID, odAfterRKey, world.communityADID, "3lzodrev000013", 1_775_000_023_000_001)
	afterURI := "at://" + mtAuthorDID + "/" + materialize.CollectionPostV2 + "/" + odAfterRKey
	status, _ := admissionFor(t, h.db, world.communityADID, afterURI)
	assert.Equal(t, accept.StatusAccepted, status,
		"and admission resumes: re-enabling is the same user under the same actor, so a "+
			"soft opt-out has to be fully reversible — that is what makes it the tier a user "+
			"can safely choose")
	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, testDigestRKey(afterURI))
	assert.NoError(t, err, "with the acceptance to prove it (err=%v)", err)

	// The cancelled deliveries stay cancelled. Re-enabling restores the user's
	// ability to federate; it does not resurrect work they cancelled by asking
	// us to stop — those posts were withdrawn, and re-sending them would publish
	// on their behalf something they had already taken back.
	assertEveryDelivery(t, h.db, mtAuthorDID, mtCommunityBAPID, "cancelled",
		"and the withdrawn work stays withdrawn: a re-enable is 'federate me from now on', "+
			"not 'replay what I stopped'")
}

// odFederationEvent builds a social.coves.bridge.federation commit.
func odFederationEvent(t *testing.T, did, rev, operation string, enabled, deleteRemote bool, timeUS int64) *consume.JetstreamEvent {
	t.Helper()
	record := ""
	if operation != "delete" {
		record = fmt.Sprintf(`,"cid":%q,"record":{"$type":%q,"enabled":%t,"deleteRemote":%t}`,
			mtPostCID, consume.CollectionFederation, enabled, deleteRemote)
	}
	frame := fmt.Sprintf(
		`{"did":%q,"time_us":%d,"kind":"commit","commit":{"rev":%q,"operation":%q,`+
			`"collection":%q,"rkey":"self"%s}}`,
		did, timeUS, rev, operation, consume.CollectionFederation, record)
	var event consume.JetstreamEvent
	require.NoError(t, json.Unmarshal([]byte(frame), &event), "the frame must be valid wire JSON")
	return &event
}
