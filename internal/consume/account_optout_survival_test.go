package consume

import (
	"context"
	stderrors "errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// TASK 17d follow-up — THE TERMINAL TIER MUST NOT DESTROY A USER'S OWN OPT-OUT.
//
// The dangerous sequence: a user holds a record-sourced opt-out (enabled=false,
// source='record') → their account is reported deleted → the terminal tier
// records its request → the purge FAILS on transport (retryable, seq
// un-advanced) → the user reactivates → the redelivered frame confirms LIVE →
// clearStaleRequest withdraws the account-sourced request. If recording the
// request REPAINTED the user's row as source='account', the withdrawal deletes
// it — and absence is default-on, so the bridge resumes federating for someone
// whose opt-out record still stands and is never re-applied. That is the worst
// failure direction 17d names: publishing for someone who asked you to stop.
//
// So TerminateAccount reads before it writes: a standing non-account opt-out is
// left untouched, the purge proceeds against it anyway (MarkPurged is
// source-agnostic), and the confirmed-live withdrawal then correctly refuses to
// delete a row this tier never wrote.

// setErr arms or clears the deleter's failure, the transport-down half of the
// purge-fails window. (Defined here rather than at the recordingDeleter because
// this file is the first to exercise the failing seam.)
func (d *recordingDeleter) setErr(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.err = err
}

// seedRecordOptOut writes the user's own standing opt-out, exactly as the
// record tier would have.
func seedRecordOptOut(t *testing.T, world *terminalWorld) {
	t.Helper()
	_, err := store.NewFederationPrefs(world.db).Upsert(context.Background(), store.FederationPref{
		DID:     dispatchNativeDID,
		Enabled: false,
		Source:  store.FederationPrefSourceRecord,
	})
	require.NoError(t, err, "seed the user's record-sourced opt-out")
}

func TestFailedPurgeThenReactivation_PreservesTheUsersOwnOptOut(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	world := newTerminalWorld(t, database, true)
	seedRecordOptOut(t, world)

	// The account is reported and confirmed deleted, but the purge dies on
	// transport: the event must stay redrivable.
	world.confirmer.set(true, nil)
	world.purger.setErr(stderrors.New("peer inbox unreachable"))
	err := world.handle(t, accountFrameSeq(dispatchNativeDID, false, "deleted", 21))
	require.Error(t, err, "a failed purge fails the event, which is what brings it back")
	assert.Zero(t, appliedSeq(t, database, dispatchNativeDID),
		"the seq must not advance past a purge that did not happen")

	pref := storedPref(t, database, dispatchNativeDID)
	require.NotNil(t, pref, "the opt-out row must still exist inside the purge-fails window")
	assert.Equal(t, store.FederationPrefSourceRecord, pref.Source,
		"recording the terminal request must NOT repaint the user's own opt-out as "+
			"source='account': provenance is what stops the confirmed-live path from "+
			"deleting a preference the user wrote")
	assert.False(t, pref.Enabled)

	// The user reactivates before the retry; the redelivered frame now
	// confirms LIVE, and the tier withdraws only what IT wrote.
	world.confirmer.set(false, nil)
	world.purger.setErr(nil)
	require.NoError(t, world.handle(t, accountFrameSeq(dispatchNativeDID, false, "deleted", 21)),
		"the confirmed-live redelivery closes the event")
	assert.Equal(t, int64(21), appliedSeq(t, database, dispatchNativeDID))

	pref = storedPref(t, database, dispatchNativeDID)
	require.NotNil(t, pref,
		"THE USER'S OPT-OUT MUST SURVIVE THE ROUND TRIP: deleting it means absence, "+
			"absence means default-on, and the bridge resumes publishing for someone "+
			"whose opt-out record still stands — with nothing left to re-apply it")
	assert.Equal(t, store.FederationPrefSourceRecord, pref.Source,
		"still theirs: source='record' is the only trace of who asked")
	assert.False(t, pref.Enabled,
		"and still an opt-out — the account round trip changed nothing they said")
}

// TestFailedPurgeThenReactivation_StillClearsTheTiersOwnRequest is the positive
// control: with NO standing opt-out, the same round trip must end where it
// always did — the account-sourced request cleared, the user back to default-on.
func TestFailedPurgeThenReactivation_StillClearsTheTiersOwnRequest(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	world := newTerminalWorld(t, database, true)

	world.confirmer.set(true, nil)
	world.purger.setErr(stderrors.New("peer inbox unreachable"))
	require.Error(t, world.handle(t, accountFrameSeq(dispatchNativeDID, false, "deleted", 22)))

	pref := storedPref(t, database, dispatchNativeDID)
	require.NotNil(t, pref, "the terminal request was recorded before the send failed")
	assert.Equal(t, store.FederationPrefSourceAccount, pref.Source,
		"with no standing opt-out the request is the tier's own, attributed to the account")

	world.confirmer.set(false, nil)
	world.purger.setErr(nil)
	require.NoError(t, world.handle(t, accountFrameSeq(dispatchNativeDID, false, "deleted", 22)))

	assert.Nil(t, storedPref(t, database, dispatchNativeDID),
		"a request THIS tier wrote for a deletion that never happened is withdrawn: "+
			"leaving it would block a live user forever on the strength of a stale event")
	assert.Equal(t, int64(22), appliedSeq(t, database, dispatchNativeDID))
}
