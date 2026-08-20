package ingest

import (
	"context"
	"database/sql"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/consume"
	"tidepool/internal/store"
)

// TASK 17d, CYCLE 5 — THE TWO CONTROLS.
//
// Both are about the tier NOT reached, which is the half of a two-tier design
// nothing else measures. A soft opt-out that quietly minted an identity, or a
// destructive one that half-acted with no seam to act through, would each pass
// every assertion the tiers make about themselves — the damage is entirely in
// what happened BESIDES the thing under test.

const (
	// A DID that has never federated anything: no actor, and deliberately no
	// entry in mtHandles either. The resolver refuses an unregistered DID, so an
	// implementation that tried to mint here fails LOUDLY at the handle lookup
	// rather than quietly creating the identity — the control's own tripwire.
	ocStrangerDID = "did:plc:ocstranger000001"
	ocStrangerRev = "3lzocrev000001"

	ocUnwiredRev   = "3lzocrev000002"
	ocUnwiredRKey  = "3lzocpost00001"
	ocUnwiredBRKey = "3lzocpost00002"
)

// TestOptingOutFromADIDWithNoActorIsANoOpSuccess is the control the DEFAULT-ON
// design makes necessary.
//
// Under opt-out, the users who write this record are disproportionately the ones
// who have never federated anything — they read the disclosure and said no
// before their first bridged interaction. There is nothing to disable, and
// minting an actor in order to disable it would create the very identity the
// record asks the bridge not to create: a fediverse persona, discoverable and
// resolvable, brought into existence by the user's refusal.
//
// handleAccount already treats an actorless DID this way for #account. This
// pins it for the federation record, where the incentive to "make the mirror
// write succeed" is strongest.
func TestOptingOutFromADIDWithNoActorIsANoOpSuccess(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	actorsBefore := rowCount(t, h.db, "outbound_activities")
	require.Zero(t, actorCount(t, h.db, ocStrangerDID),
		"precondition: this DID has no AP identity — that is the whole case")

	// --- WHEN: they opt out, having never federated anything.
	require.NoError(t, world.dispatcher.HandleEvent(ctx,
		odFederationEvent(t, ocStrangerDID, ocStrangerRev, "create", false, false, 1_775_000_040_000_001)),
		"an opt-out for a DID with no actor is a no-op SUCCESS: the mirror write is allowed "+
			"to find nothing, and failing the event would dead-letter the most common opt-out "+
			"there is under a default-on design")

	// --- THEN: no identity was brought into existence to disable.
	assert.Zero(t, actorCount(t, h.db, ocStrangerDID),
		"NO actor may appear: a persona minted here is discoverable, resolvable and "+
			"webfinger-able — the bridge would have published a fediverse identity for a user "+
			"whose only instruction was not to")

	// --- AND: the preference is still recorded, because federation_prefs is the
	//     authority and answers for DIDs that have no actor to mirror onto.
	pref, err := store.NewFederationPrefs(h.db).Get(ctx, ocStrangerDID)
	require.NoError(t, err,
		"the preference IS recorded: it is what gates the user's first federating "+
			"interaction, and an opt-out that stored nothing would be honoured only until "+
			"they replied to something")
	assert.False(t, pref.Enabled)
	assert.False(t, pref.DeleteRemote, "nothing destructive is ever inferred from a soft opt-out")

	assert.Equal(t, actorsBefore, rowCount(t, h.db, "outbound_activities"),
		"and nothing was said about them on the wire: there is no identity to withdraw and "+
			"no peer that has heard of them")
}

// TestDeleteRemoteWithNoDestructiveSeamRecordsWithoutActing is the control for
// the deployment where the destructive tier is not wired.
//
// The requirement is that it behaves like a BACKLOG, not like a degraded tier.
// The preference is durable, so the erasure can be carried out when the seam
// lands; and nothing is half-done in the meantime — because every partial step
// is a lie told to a different reader. A tombstoned actor with no Delete{Person}
// sent tells peers to stop resolving an identity whose content nobody was asked
// to purge; a vote flipped to 'undone' with no Undo on the wire hides that vote
// from the enumeration the real purge will one day run, so the erasure MISSES it.
func TestDeleteRemoteWithNoDestructiveSeamRecordsWithoutActing(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	// A deployment where the destructive tier has not landed.
	world := newModerationWorld(t, h, func(o *consume.Options) { o.RemoteDeleter = nil })

	// --- GIVEN: an author with real federated content and a vote a peer still
	//     holds. Without the live vote, "no Undo was enqueued" is true of a
	//     fixture that had nothing to undo, which is the same sentence about
	//     nothing.
	admitPost(t, world, mtAuthorDID, ocUnwiredRKey, world.communityADID, "3lzocrev000010", 1_775_000_041_000_001)
	admitPost(t, world, mtAuthorDID, ocUnwiredBRKey, world.communityBDID, "3lzocrev000011", 1_775_000_041_000_002)
	requireEveryDelivery(t, h.db, mtAuthorDID, groupID, "pending",
		"precondition: the author has queued work to withdraw")

	liveVote := seedDeliveredVote(t, h.db, mtAuthorDID, mtPostATURI, world.communityADID, "delivered")
	require.Equal(t, "delivered", voteState(t, h.db, liveVote),
		"precondition: a peer really is holding one of this actor's votes")

	activitiesBefore := rowCount(t, h.db, "outbound_activities")

	// --- WHEN: they ask for the destructive tier and there is nowhere to send it.
	require.NoError(t, world.dispatcher.HandleEvent(ctx,
		odFederationEvent(t, mtAuthorDID, ocUnwiredRev, "create", false, true, 1_775_000_042_000_001)),
		"a missing destructive seam must not panic and must not fail the event: dead-lettering "+
			"it would lose the user's request entirely")

	// --- THEN: the request is RECORDED, in the tier the user asked for.
	pref, err := store.NewFederationPrefs(h.db).Get(ctx, mtAuthorDID)
	require.NoError(t, err)
	assert.False(t, pref.Enabled)
	assert.True(t, pref.DeleteRemote,
		"recorded as DESTRUCTIVE, not downgraded to the soft tier: the difference is the "+
			"user's own choice, and a preference that forgot it would leave their content "+
			"federated forever while the database says they were handled")

	// --- AND: the soft tier still ran, which is what makes the assertions below
	//     about restraint rather than about an event that did nothing.
	actor, err := store.NewAPActors(h.db).GetByDID(ctx, mtAuthorDID)
	require.NoError(t, err)
	assert.False(t, actor.Enabled, "the actor is disabled: everything reachable was still done")
	assertEveryDelivery(t, h.db, mtAuthorDID, groupID, "cancelled",
		"and their queued work is cancelled — the parts of the request that need no seam "+
			"are not held hostage by the part that does")

	// --- AND: NOTHING was acted on.
	assert.Equal(t, activitiesBefore, rowCount(t, h.db, "outbound_activities"),
		"no new activity: with no seam wired there is nothing to send, and a tier that "+
			"invented one would be sending an irreversible Delete from a code path nobody "+
			"has reviewed for this deployment")
	assert.Empty(t, activityOfKind(t, h.db, mtAuthorDID, "Delete"),
		"no Delete{Person} in particular: peers that honour one cannot restore what they drop")
	assert.Zero(t, undoActivitiesFor(t, h.db, mtAuthorDID),
		"and no Undo for the vote a peer still holds")
	assert.Equal(t, "delivered", voteState(t, h.db, liveVote),
		"whose ledger row is UNTOUCHED: flipping it to 'undone' without sending the Undo "+
			"would hide the vote from the enumeration the real purge runs when the seam "+
			"lands — the erasure would then miss the one vote it was written for")

	assert.Equal(t, http.StatusOK, actorDocStatus(t, h, mtAuthorDID),
		"and the actor document still resolves: the 410 is the destructive tier's own "+
			"statement, and making it here would tell every peer to stop resolving an "+
			"identity whose content nobody was ever asked to purge")
}

// actorCount reports how many ap_actors rows exist for a DID.
func actorCount(t *testing.T, db *sql.DB, did string) int {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM ap_actors WHERE did = $1`, did).Scan(&n))
	return n
}
