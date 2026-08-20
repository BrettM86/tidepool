package consume

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Task 14 cycle J: #account.
//
// Decision 19 in one sentence: active=false is NOT deletion. Deactivated,
// suspended, takendown and throttled are all states a user comes back from,
// and treating any of them as a deletion would send Delete{Person} to every
// peer — irreversibly destroying an identity over a temporary suspension.
// Only status="deleted" means gone, and even that goes to a seam that
// re-verifies before acting.

// recordingTerminator stands in for the task 17 terminal tier.
type recordingTerminator struct {
	mu   sync.Mutex
	dids []string
	err  error
}

func (t *recordingTerminator) TerminateAccount(_ context.Context, did string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.dids = append(t.dids, did)
	return t.err
}

func (t *recordingTerminator) DIDs() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.dids...)
}

func TestAccount_TransientStatusesPauseDeliveryWithoutTouchingIdentity(t *testing.T) {
	// Every inactive status atproto defines that is NOT a deletion. Each one
	// is a user who may be back tomorrow.
	for _, status := range []string{"deactivated", "suspended", "takendown", "throttled"} {
		t.Run(status, func(t *testing.T) {
			database := dispatchTestDB(t)
			seedAPActor(t, database, dispatchNativeDID, "alice")
			terminator := &recordingTerminator{}
			fixture := newDispatchFixture(t, database,
				func(opts *Options) { opts.Terminator = terminator })

			require.NoError(t, fixture.handle(t,
				accountFrameFor(dispatchNativeDID, false, status)))

			assert.True(t, deliveryPaused(t, database, dispatchNativeDID),
				"%s pauses DELIVERY", status)
			enabled, _ := actorEnabled(t, database, dispatchNativeDID)
			assert.True(t, enabled,
				"but leaves the identity enabled: the actor document and every "+
					"federated reference to it must survive a temporary state")
			assert.Empty(t, terminator.DIDs(),
				"and NEVER reaches the terminal tier — sending Delete{Person} over a "+
					"suspension would destroy an identity the user gets back")
		})
	}
}

func TestAccount_ReactivationUnpauses(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	fixture := newDispatchFixture(t, database)

	require.NoError(t, fixture.handle(t,
		accountFrameFor(dispatchNativeDID, false, "deactivated")))
	require.True(t, deliveryPaused(t, database, dispatchNativeDID))

	require.NoError(t, fixture.handle(t, accountFrameFor(dispatchNativeDID, true, "active")))
	assert.False(t, deliveryPaused(t, database, dispatchNativeDID),
		"coming back resumes delivery, under the same identity and the same frozen "+
			"local part")
}

func TestAccount_DeletedReachesTheTerminalSeamAndNotThePause(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	terminator := &recordingTerminator{}
	fixture := newDispatchFixture(t, database,
		func(opts *Options) { opts.Terminator = terminator })

	require.NoError(t, fixture.handle(t,
		accountFrameFor(dispatchNativeDID, false, "deleted")))

	assert.Equal(t, []string{dispatchNativeDID}, terminator.DIDs(),
		"status=deleted is the ONE value that means gone, and it goes to the tier that "+
			"re-verifies against PLC and the PDS before sending Delete{Person} — acting "+
			"on a stale deletion event is unrecoverable")

	assert.False(t, deliveryPaused(t, database, dispatchNativeDID),
		"and it is NOT a pause: pausing a deleted account would leave the identity "+
			"standing while pretending something had been done about it")
}

func TestAccount_DeletedWithNoTerminatorWiredChangesNothing(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	// A deployment where task 17 has not landed.
	fixture := newDispatchFixture(t, database, func(opts *Options) { opts.Terminator = nil })

	require.NoError(t, fixture.handle(t,
		accountFrameFor(dispatchNativeDID, false, "deleted")),
		"a missing terminal tier must not panic and must not fail the event")

	assert.False(t, deliveryPaused(t, database, dispatchNativeDID),
		"and must not quietly degrade into a pause — a deletion half-handled as a "+
			"pause looks handled in the database and is not")
	enabled, _ := actorEnabled(t, database, dispatchNativeDID)
	assert.True(t, enabled)
}

func TestAccount_ActorlessDIDIsSkippedEverywhere(t *testing.T) {
	for _, tc := range []struct {
		name   string
		active bool
		status string
	}{
		{"deactivated", false, "deactivated"},
		{"reactivated", true, "active"},
		{"deleted", false, "deleted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			database := dispatchTestDB(t)
			terminator := &recordingTerminator{}
			fixture := newDispatchFixture(t, database,
				func(opts *Options) { opts.Terminator = terminator })

			require.NoError(t, fixture.handle(t,
				accountFrameFor(dispatchNativeDID, tc.active, tc.status)),
				"an account event for a DID with no AP identity is a no-op")

			assert.Zero(t, countRows(t, database, "ap_actors"),
				"minting an actor to pause or delete it would create the identity the "+
					"event is about losing")
			assert.Empty(t, fixture.minter.Handles())
			assert.Empty(t, terminator.DIDs(),
				"and there is nothing for the terminal tier to withdraw: nothing was "+
					"ever federated under this DID")
		})
	}
}
