package consume

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// Task 14 cycle E: the social.coves.bridge.federation handler — the opt-out.
//
// Federation is DEFAULT-ON (decision 11) and this record is the only way a
// user turns it down, so the invariant running through every test here is that
// ABSENCE means enabled. The handler never writes an "enabled" row; it deletes
// the row that said otherwise.

// ---------------------------------------------------------------------------
// E1 — the record drives federation_prefs and the actor lifecycle
// ---------------------------------------------------------------------------

func TestFederationHandler_OptOutRecordsThePreference(t *testing.T) {
	database := dispatchTestDB(t)
	fixture := newDispatchFixture(t, database)
	ctx := context.Background()

	require.NoError(t, fixture.handle(t,
		federationFrameNoDeleteRemote(dispatchNativeDID, dispatchRev, false)))

	pref, err := store.NewFederationPrefs(database).Get(ctx, dispatchNativeDID)
	require.NoError(t, err, "an opt-out must be recorded")
	require.NotNil(t, pref)
	assert.False(t, pref.Enabled)
	assert.False(t, pref.DeleteRemote,
		"an omitted deleteRemote is the soft tier: nothing destructive is ever inferred")
	assert.Equal(t, store.FederationPrefSourceRecord, pref.Source,
		"the preference came from a record the consumer saw, not from a probe")

	assert.Empty(t, fixture.deleter.DIDs(),
		"the destructive seam is reached ONLY by an explicit deleteRemote=true")
}

func TestFederationHandler_OptOutDisablesAnExistingActor(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	fixture := newDispatchFixture(t, database)

	require.NoError(t, fixture.handle(t,
		federationFrameNoDeleteRemote(dispatchNativeDID, dispatchRev, false)))

	enabled, disabledAt := actorEnabled(t, database, dispatchNativeDID)
	assert.False(t, enabled, "the soft disable stops WebFinger resolution and delivery")
	assert.NotNil(t, disabledAt, "the transition is stamped, so a support question is answerable")
}

func TestFederationHandler_OptOutForAnActorlessDIDIsNotAMint(t *testing.T) {
	database := dispatchTestDB(t)
	fixture := newDispatchFixture(t, database)

	// A user who opted out before ever federating anything. Minting an actor
	// here just to disable it would create the very identity the record asks
	// the bridge not to create.
	require.NoError(t, fixture.handle(t,
		federationFrameNoDeleteRemote(dispatchNativeDID, dispatchRev, false)),
		"an opt-out for a DID with no actor is a no-op on the actor side, not an error")

	assert.Zero(t, countRows(t, database, "ap_actors"),
		"no ap_actors row may appear: actors mint at the first FEDERATING interaction, "+
			"and an opt-out is the opposite of one")
	assert.Empty(t, fixture.minter.DIDs(), "and the mint seam is never called")

	assert.Equal(t, 1, countRows(t, database, "federation_prefs"),
		"the preference is still recorded, so a later first interaction is already gated")
}

func TestFederationHandler_DeleteRemoteReachesTheDestructiveSeam(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	fixture := newDispatchFixture(t, database)
	ctx := context.Background()

	require.NoError(t, fixture.handle(t,
		federationFrame(dispatchNativeDID, dispatchRev, "create", false, true)))

	pref, err := store.NewFederationPrefs(database).Get(ctx, dispatchNativeDID)
	require.NoError(t, err)
	require.NotNil(t, pref)
	assert.False(t, pref.Enabled)
	assert.True(t, pref.DeleteRemote,
		"the destructive request is RECORDED before it is acted on: peers that honor "+
			"a Delete cannot restore what they dropped, so the intent must survive a "+
			"crash between recording and sending")

	assert.Equal(t, []string{dispatchNativeDID}, fixture.deleter.DIDs(),
		"enabled=false + deleteRemote=true escalates to the task 17 destructive tier "+
			"exactly once")

	enabled, _ := actorEnabled(t, database, dispatchNativeDID)
	assert.False(t, enabled, "the destructive tier also soft-disables")
}

func TestFederationHandler_DeleteRemoteWithNoSeamWiredStillRecordsThePreference(t *testing.T) {
	database := dispatchTestDB(t)
	// A deployment where task 17 has not landed yet.
	fixture := newDispatchFixture(t, database, func(opts *Options) { opts.RemoteDeleter = nil })
	ctx := context.Background()

	require.NoError(t, fixture.handle(t,
		federationFrame(dispatchNativeDID, dispatchRev, "create", false, true)),
		"a missing destructive seam must not panic and must not fail the event")

	pref, err := store.NewFederationPrefs(database).Get(ctx, dispatchNativeDID)
	require.NoError(t, err)
	require.NotNil(t, pref)
	assert.True(t, pref.DeleteRemote,
		"the request is recorded even with nowhere to send it, so task 17 can act on "+
			"the backlog when it lands instead of the user's wish being lost")
}

func TestFederationHandler_EnabledTrueDeletesThePreference(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	fixture := newDispatchFixture(t, database)
	ctx := context.Background()

	require.NoError(t, fixture.handle(t,
		federationFrame(dispatchNativeDID, dispatchRev, "create", false, true)))
	require.Equal(t, 1, countRows(t, database, "federation_prefs"))

	require.NoError(t, fixture.handle(t,
		federationFrameNoDeleteRemote(dispatchNativeDID, dispatchRevHigher, true)))

	_, err := store.NewFederationPrefs(database).Get(ctx, dispatchNativeDID)
	require.Error(t, err)
	assert.True(t, errors.IsNotFound(err),
		"re-enabling DELETES the row rather than storing enabled=true: absence IS the "+
			"default-on state, and a lingering row would also keep the stale "+
			"deleteRemote=true pointed at the user")

	enabled, _ := actorEnabled(t, database, dispatchNativeDID)
	assert.True(t, enabled,
		"the actor is re-enabled under its ORIGINAL identity — the local part was "+
			"frozen at creation and is never re-derived")
}

func TestFederationHandler_RecordDeleteIsTheSameAsReEnabling(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	fixture := newDispatchFixture(t, database)
	ctx := context.Background()

	require.NoError(t, fixture.handle(t,
		federationFrameNoDeleteRemote(dispatchNativeDID, dispatchRev, false)))
	require.Equal(t, 1, countRows(t, database, "federation_prefs"))
	enabled, _ := actorEnabled(t, database, dispatchNativeDID)
	require.False(t, enabled)

	// Deleting the record is the other way a user re-enables. The delete
	// commit carries no record body, which is fine here: the DID is the whole
	// question.
	require.NoError(t, fixture.handle(t,
		federationFrame(dispatchNativeDID, dispatchRevHigher, "delete", false, false)))

	_, err := store.NewFederationPrefs(database).Get(ctx, dispatchNativeDID)
	require.Error(t, err)
	assert.True(t, errors.IsNotFound(err),
		"deleting the opt-out record restores default-on, exactly like enabled=true")

	enabled, _ = actorEnabled(t, database, dispatchNativeDID)
	assert.True(t, enabled)
}

func TestFederationHandler_ReEnablingAnActorlessDIDIsANoOp(t *testing.T) {
	database := dispatchTestDB(t)
	fixture := newDispatchFixture(t, database)

	require.NoError(t, fixture.handle(t,
		federationFrameNoDeleteRemote(dispatchNativeDID, dispatchRev, true)),
		"an enable event for a DID that has never federated is a no-op, not an error")

	assert.Zero(t, countRows(t, database, "ap_actors"),
		"NO EAGER MINT: an enable event must not create the identity the user has not "+
			"yet earned by interacting")
	assert.Empty(t, fixture.minter.DIDs())
	assert.Zero(t, countRows(t, database, "federation_prefs"),
		"and nothing is recorded — absence already means enabled")
}

// ---------------------------------------------------------------------------
// E2 — the opt-out gate runs BEFORE the mint
// ---------------------------------------------------------------------------
//
// SCOPE: the comment handler proper is cycle H. What is pinned here is the
// ORDERING — an opted-out author's comment must be dropped before anything
// observable happens — using the ActorMinter seam as the positive control.
// That keeps this cycle independent of the handle resolver (cycle F) while
// still forcing the gate into the path: a dispatcher that ignored comments
// entirely would fail the "no prefs row" case.

func TestFederationGate_OptedOutAuthorsCommentIsDroppedBeforeAnythingHappens(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)
	ctx := context.Background()

	prefs := store.NewFederationPrefs(database)
	_, err := prefs.Upsert(ctx, store.FederationPref{
		DID:    dispatchNativeDID,
		Source: store.FederationPrefSourceRecord,
	})
	require.NoError(t, err, "seed the opt-out")

	fixture := newDispatchFixture(t, database)
	require.NoError(t, fixture.handle(t,
		commentFrameFor(dispatchNativeDID, dispatchRev, "3lzcmnt4444cc")),
		"an opted-out author's comment is SKIPPED, not failed")

	assert.Empty(t, fixture.minter.DIDs(),
		"no mint: the gate runs before the lazy mint, so opting out before ever "+
			"federating means no AP identity is ever created")
	assert.Zero(t, countRows(t, database, "ap_actors"))
	assert.Zero(t, countRows(t, database, "outbound_objects"),
		"no outbound state: nothing may be queued for a user who said no")
	assert.Empty(t, fixture.enqueuer.Calls(), "and no intent reaches task 15")
}

func TestFederationGate_AbsentPreferenceMeansTheCommentProceeds(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)

	fixture := newDispatchFixture(t, database)
	require.NoError(t, fixture.handle(t,
		commentFrameFor(dispatchNativeDID, dispatchRev, "3lzcmnt5555dd")))

	assert.Equal(t, []string{dispatchNativeDID}, fixture.minter.DIDs(),
		"NO federation_prefs row means default-on, so the comment proceeds and the "+
			"author's actor is minted lazily — this is the positive control that "+
			"stops the opt-out assertions above from passing vacuously")
}

func TestFederationGate_OptedOutAuthorStillBlocksAfterReEnablingAnotherUser(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)
	ctx := context.Background()

	_, err := store.NewFederationPrefs(database).Upsert(ctx, store.FederationPref{
		DID:    dispatchNativeDID,
		Source: store.FederationPrefSourceRecord,
	})
	require.NoError(t, err)

	fixture := newDispatchFixture(t, database)

	// Somebody ELSE re-enables. The gate is per-DID, so this must not lift the
	// first user's opt-out — a shared or cached "federation is on" flag would.
	require.NoError(t, fixture.handle(t,
		federationFrameNoDeleteRemote(acceptRootAuthorDID, dispatchRev, true)))

	require.NoError(t, fixture.handle(t,
		commentFrameFor(dispatchNativeDID, dispatchRevHigher, "3lzcmnt6666ee")))

	assert.Empty(t, fixture.minter.DIDs(),
		"the opt-out gate is keyed by the AUTHOR's DID; another user's preference "+
			"must never lift it")
}

// ---------------------------------------------------------------------------
// E3 — replay
// ---------------------------------------------------------------------------

func TestFederationHandler_ReplayedRecordHasNoSecondEffect(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	fixture := newDispatchFixture(t, database)
	ctx := context.Background()

	frame := federationFrame(dispatchNativeDID, dispatchRev, "create", false, true)

	require.NoError(t, fixture.handle(t, frame))
	require.Equal(t, []string{dispatchNativeDID}, fixture.deleter.DIDs())

	// Re-enable out of band, then replay the ORIGINAL opt-out. A cursor rewind
	// does exactly this.
	require.NoError(t, store.NewFederationPrefs(database).Delete(ctx, dispatchNativeDID))
	require.NoError(t, store.NewAPActors(database).SetEnabled(ctx, dispatchNativeDID, true))

	require.NoError(t, fixture.handle(t, frame))

	_, err := store.NewFederationPrefs(database).Get(ctx, dispatchNativeDID)
	require.Error(t, err)
	assert.True(t, errors.IsNotFound(err),
		"the replayed record carries the same rev, so the gate rejects it before the "+
			"handler runs — a stale opt-out must never resurrect over a newer re-enable")

	enabled, _ := actorEnabled(t, database, dispatchNativeDID)
	assert.True(t, enabled, "and the actor stays enabled")

	assert.Equal(t, []string{dispatchNativeDID}, fixture.deleter.DIDs(),
		"above all, the DESTRUCTIVE seam must not fire twice: asking peers a second "+
			"time to delete content the user has since re-enabled is unrecoverable")
}

func TestFederationHandler_HigherRevSupersedesTheStoredPreference(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	fixture := newDispatchFixture(t, database)
	ctx := context.Background()

	require.NoError(t, fixture.handle(t,
		federationFrameNoDeleteRemote(dispatchNativeDID, dispatchRev, false)))
	require.Equal(t, 1, countRows(t, database, "federation_prefs"))

	// A genuinely newer write must get through the gate.
	require.NoError(t, fixture.handle(t,
		federationFrameNoDeleteRemote(dispatchNativeDID, dispatchRevHigher, true)))

	_, err := store.NewFederationPrefs(database).Get(ctx, dispatchNativeDID)
	require.Error(t, err)
	assert.True(t, errors.IsNotFound(err),
		"a strictly greater rev is the user's next real decision and applies")

	enabled, _ := actorEnabled(t, database, dispatchNativeDID)
	assert.True(t, enabled)
}
