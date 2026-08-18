package ingest

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// TASK 17d — THE COME-BACK AFTER A REAL PURGE.
//
// The consume-level suite already replays a re-enable after deleteRemote=true —
// but against a STUB deleter that tombstones nothing and marks nothing purged,
// so what it exercises is exactly the NON-terminal path: the pref row is still
// an ordinary request, Delete removes it, SetEnabled has no tombstone to force
// against, and the actor comes back. Correct there, and a mask here: every
// terminality guard could be deleted and that test would still re-enable
// happily, because for it nothing terminal ever happened.
//
// This file drives the destructive tier through the REAL Purger (the harness
// wires outbound.NewPurger exactly as production does) and then delivers the
// user's come-back — both doors of it, enabled=true and the record delete —
// asserting the withdrawal holds end to end. The one-line deletions this kills
// are the store guards working IN CONCERT, observed from the event pipeline:
//
//   - revert setEnabled's `enabled = ($2 AND tombstoned_at IS NULL)` to `$2`
//     → the mirror re-enables and the bridge signs new content for an identity
//     every peer was told is 410 Gone;
//   - drop Delete's `AND purged_at IS NULL` → the pref row goes, absence means
//     default-on, and admission resumes for the erased user.
//
// Either revert alone flips an assertion below; neither is visible to any test
// that purges with a stub.
const (
	ocDestructiveRev = "3lzodrev000200"
	ocEnableRev      = "3lzodrev000201"
	ocDeleteRev      = "3lzodrev000202"
	ocSoftRev        = "3lzodrev000203"
	ocSoftEnableRev  = "3lzodrev000204"

	ocPurgedPostRKey = "3lzodcome00001"
	ocSoftPostRKey   = "3lzodcome00002"
)

// TestComeBackAfterARealPurgeIsRefused: GIVEN an author purged through the
// real destructive tier, WHEN their re-enable record (and then the record
// delete) arrives, THEN the events are handled without error, the actor stays
// tombstoned and disabled, the preference row survives with its purge stamp,
// nothing new is enqueued — and the soft-tier actor beside them comes back
// normally, so the refusal is provably about the purge and not a pipeline that
// lost the ability to re-enable anyone.
func TestComeBackAfterARealPurgeIsRefused(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	// --- GIVEN: two authors with real delivery history. The purge enumerates
	//     the inboxes an actor's content reached, so an actor with none would
	//     exercise the zero-target edge instead of an ordinary withdrawal.
	admitPost(t, world, mtAuthorDID, ocPurgedPostRKey, world.communityADID, "3lzodrev000210", 1_775_000_040_000_001)
	admitPost(t, world, mtCommenterDID, ocSoftPostRKey, world.communityADID, "3lzodrev000211", 1_775_000_040_000_002)

	// One takes the DESTRUCTIVE tier through the real Purger; the other takes
	// the soft tier and is this test's positive control.
	require.NoError(t, world.dispatcher.HandleEvent(ctx,
		odFederationEvent(t, mtAuthorDID, ocDestructiveRev, "create", false, true, 1_775_000_041_000_001)))
	require.NoError(t, world.dispatcher.HandleEvent(ctx,
		odFederationEvent(t, mtCommenterDID, ocSoftRev, "create", false, false, 1_775_000_041_000_002)))

	// Preconditions proving the purge REALLY committed — the two facts the
	// stub-purger test never had, and the whole reason this file exists.
	prefs := store.NewFederationPrefs(h.db)
	actors := store.NewAPActors(h.db)
	purgedPref, err := prefs.Get(ctx, mtAuthorDID)
	require.NoError(t, err)
	require.NotNil(t, purgedPref.PurgedAt,
		"precondition: the real purge marked the preference purged inside its own tx")
	deadActor, err := actors.GetByDID(ctx, mtAuthorDID)
	require.NoError(t, err)
	require.NotNil(t, deadActor.TombstonedAt, "precondition: the real purge tombstoned the actor")
	require.Equal(t, http.StatusGone, actorDocStatus(t, h, mtAuthorDID),
		"precondition: the withdrawal is on the wire — the actor document answers 410")

	activitiesBefore := rowCount(t, h.db, "outbound_activities")

	// --- WHEN: the purged user comes back through door one, enabled=true.
	require.NoError(t, world.dispatcher.HandleEvent(ctx,
		odFederationEvent(t, mtAuthorDID, ocEnableRev, "create", true, false, 1_775_000_042_000_001)),
		"the come-back event is HANDLED, not failed: the record was applied as far as it "+
			"is allowed to go, and failing it would replay a question whose answer is terminal")

	// --- THEN: the withdrawal holds, at every layer a peer or a gate reads.
	assertWithdrawalStands(t, h, purgedPref, deadActor,
		"after enabled=true: an ordinary re-enable record")

	// --- WHEN: door two, the record delete — under default-on it means the
	//     same thing, and it reaches the same store guards.
	require.NoError(t, world.dispatcher.HandleEvent(ctx,
		odFederationEvent(t, mtAuthorDID, ocDeleteRev, "delete", false, false, 1_775_000_042_000_002)))

	assertWithdrawalStands(t, h, purgedPref, deadActor,
		"after the opt-out record's delete: the most innocuous-looking way to undo an "+
			"irreversible decision")

	assert.Equal(t, activitiesBefore, rowCount(t, h.db, "outbound_activities"),
		"and NOTHING new was enqueued by either come-back: no second Delete{Person}, no "+
			"fresh publication — a refused resurrection is silent on the wire")

	// --- Positive control: the SOFT actor's come-back works in the same run.
	//     Without this, every refusal above is satisfiable by a pipeline that
	//     simply lost the ability to re-enable anyone.
	require.NoError(t, world.dispatcher.HandleEvent(ctx,
		odFederationEvent(t, mtCommenterDID, ocSoftEnableRev, "create", true, false, 1_775_000_042_000_003)))
	softBack, err := actors.GetByDID(ctx, mtCommenterDID)
	require.NoError(t, err)
	assert.True(t, softBack.Enabled,
		"the soft opt-out reverses: the terminality is the PURGE's, not the record's — "+
			"this is the whole difference between the tier a user can safely choose and "+
			"the one they were warned about")
	_, err = prefs.Get(ctx, mtCommenterDID)
	assert.True(t, errors.IsNotFound(err),
		"and their preference row is gone — absence is default-on restored, got %v", err)
}

// assertWithdrawalStands checks the full post-come-back state: pref row
// surviving with its original purge stamp, actor tombstoned and disabled with
// its original stamp, and the document still Gone.
func assertWithdrawalStands(t *testing.T, h *harness, purgedPref *store.FederationPref, deadActor *store.APActor, when string) {
	t.Helper()
	ctx := context.Background()

	pref, err := store.NewFederationPrefs(h.db).Get(ctx, mtAuthorDID)
	require.NoError(t, err,
		"%s: the preference row SURVIVES — Delete's purged_at guard is what stands between "+
			"an ordinary record delete and resumed federation for an erased user", when)
	require.NotNil(t, pref.PurgedAt, "%s: and it keeps the purge stamp", when)
	assert.True(t, pref.PurgedAt.Equal(*purgedPref.PurgedAt),
		"%s: the stamp is the original — the only record of when the withdrawal happened", when)

	actor, err := store.NewAPActors(h.db).GetByDID(ctx, mtAuthorDID)
	require.NoError(t, err)
	assert.False(t, actor.Enabled,
		"%s: the actor STAYS disabled — setEnabled's tombstone force is what stops the "+
			"bridge signing new content as an identity peers were told is gone", when)
	require.NotNil(t, actor.TombstonedAt, "%s: the tombstone is untouched", when)
	assert.True(t, actor.TombstonedAt.Equal(*deadActor.TombstonedAt),
		"%s: and keeps its original stamp", when)

	assert.Equal(t, http.StatusGone, actorDocStatus(t, h, mtAuthorDID),
		"%s: the document still answers 410 — what a peer sees is the contract", when)
}
