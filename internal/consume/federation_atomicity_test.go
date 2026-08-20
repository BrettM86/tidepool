package consume

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// Chunk 3 finding 4: gate atomicity reached only two of the four handlers. The
// federation handler wrote its PREFERENCE row on its own autocommit connection
// while the actor mirror and the delivery cancellation rode the rev-gate
// transaction — so a failure between them left the preference committed under
// an unadvanced gate, and the handler's own doc comment asserted the opposite.
//
// These pin the two halves separately, because the two paths have different
// rulings and the difference is the whole design (see handleFederation):
//
//   - the SOFT opt-out and the re-enable write their state ON the gate tx;
//   - the DESTRUCTIVE opt-out deliberately does not, because the seam it calls
//     opens its own transaction against the same row.

func TestFederationAtomicity_SoftOptOutPreferenceRollsBackWithTheGate(t *testing.T) {
	database := dispatchTestDB(t)
	ctx := context.Background()

	// The cancellation runs AFTER the preference write, on the gate tx. Failing
	// it is the realistic shape of "everything up to here committed, then the
	// unit failed".
	deliveries := &failingOutboundDeliveries{
		OutboundDeliveries: store.NewOutboundDeliveries(database),
		err:                fmt.Errorf("statement timeout"),
	}
	fixture := newDispatchFixture(t, database, func(opts *Options) { opts.Deliveries = deliveries })

	err := fixture.handle(t, federationFrameNoDeleteRemote(dispatchNativeDID, dispatchRev, false))
	require.Error(t, err, "a failed cancellation must fail the event so it replays")

	assert.Zero(t, countRows(t, database, "federation_prefs"),
		"NO preference row may survive: it is the handler's durable state and it rides "+
			"the gate transaction, so a failure rolls it back with the gate advance")
	assert.Zero(t, countRows(t, database, "jetstream_record_revs"),
		"and the gate is unadvanced, so the replay re-enters the handler")

	// And the replay recovers the whole decision.
	replay := newDispatchFixture(t, database)
	require.NoError(t, replay.handle(t,
		federationFrameNoDeleteRemote(dispatchNativeDID, dispatchRev, false)))
	pref, err := store.NewFederationPrefs(database).Get(ctx, dispatchNativeDID)
	require.NoError(t, err)
	assert.False(t, pref.Enabled)
}

func TestFederationAtomicity_ReEnableClearsThePreferenceOnTheGateTx(t *testing.T) {
	database := dispatchTestDB(t)
	ctx := context.Background()
	prefs := store.NewFederationPrefs(database)

	_, err := prefs.Upsert(ctx, store.FederationPref{
		DID:    dispatchNativeDID,
		Source: store.FederationPrefSourceRecord,
	})
	require.NoError(t, err)

	fixture := newDispatchFixture(t, database)

	// White-box on purpose: the re-enable path has no injectable seam AFTER the
	// preference write, so the only way to observe which connection the delete
	// ran on is to hand it a transaction and roll that transaction back. If the
	// delete rode its own autocommit connection the row would be gone anyway.
	tx, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	require.NoError(t, fixture.dispatcher.restoreDefaultFederation(ctx, tx, dispatchNativeDID))
	require.NoError(t, tx.Rollback())

	pref, err := prefs.Get(ctx, dispatchNativeDID)
	require.NoError(t, err,
		"the preference SURVIVES a rolled-back gate transaction: absence means "+
			"default-on, so a delete that outlived a failed event would silently "+
			"re-enable federation for a user who asked us to stop")
	require.NotNil(t, pref)
	assert.False(t, pref.Enabled)
}

func TestFederationAtomicity_DestructiveOptOutCommitsThePreferenceBeforeTheSeam(t *testing.T) {
	database := dispatchTestDB(t)
	ctx := context.Background()

	// The destructive seam observes federation_prefs from its OWN transaction
	// (outbound.Purger marks the row purged there). If the preference were held
	// uncommitted on the gate tx, that read would miss it — and the purge's
	// UPDATE of the same row would block on a lock the handler cannot release
	// without committing, which is the deadlock rev_gate.go's DEADLOCK NOTE
	// names.
	probe := &prefProbingDeleter{db: database}
	fixture := newDispatchFixture(t, database, func(opts *Options) { opts.RemoteDeleter = probe })

	require.NoError(t, fixture.handle(t,
		federationFrame(dispatchNativeDID, dispatchRev, "create", false, true)))

	require.True(t, probe.called, "the destructive seam ran")
	assert.True(t, probe.sawPreference,
		"the user's intent is DURABLE before anything irreversible is sent: peers that "+
			"honour a Delete cannot restore what they dropped, and the purge marks this "+
			"very row from its own transaction")

	pref, err := store.NewFederationPrefs(database).Get(ctx, dispatchNativeDID)
	require.NoError(t, err)
	assert.True(t, pref.DeleteRemote)
}
