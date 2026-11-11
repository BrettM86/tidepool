package consume

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// Chunk 03: a takedown restore whose author has withdrawn consent is held on
// consent, and the consumer's consent-return paths release it.

const (
	consentUndoID      = "https://coves.social/ap/activity/consentundo0001"
	consentInbox       = "https://lemmy.world/inbox"
	consentOrderingKey = "https://lemmy.world/c/technology"
	consentSubject     = "at://" + dispatchNativeDID + "/social.coves.community.postv2/3lzconsent001"
)

// seedConsentHeldRestore writes a restore Undo by the native author and its
// delivery, held on consent the way the worker holds it.
func seedConsentHeldRestore(t *testing.T, database *sql.DB) {
	t.Helper()
	ctx := context.Background()
	_, err := store.NewOutboundActivities(database).Insert(ctx, store.OutboundActivity{
		ActivityID: consentUndoID,
		ActorDID:   dispatchNativeDID,
		Kind:       "Undo",
		Payload:    []byte(`{"type":"Undo","id":"` + consentUndoID + `"}`),
	})
	require.NoError(t, err)
	_, err = store.NewOutboundDeliveries(database).Enqueue(ctx, store.OutboundDelivery{
		ActivityID: consentUndoID, TargetInbox: consentInbox, OrderingKey: consentOrderingKey,
	})
	require.NoError(t, err)
	_, err = database.Exec(`UPDATE outbound_deliveries
		SET state = 'held', held_reason = 'consent', held_subject = $1
		WHERE activity_id = $2`, consentSubject, consentUndoID)
	require.NoError(t, err)
}

func consentRestoreState(t *testing.T, database *sql.DB) store.DeliveryState {
	t.Helper()
	delivery, err := store.NewOutboundDeliveries(database).Get(context.Background(), consentUndoID, consentInbox)
	require.NoError(t, err)
	return delivery.State
}

func TestAccount_ReactivationReleasesARestoreHeldOnConsent(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	fixture := newDispatchFixture(t, database)
	require.NoError(t, fixture.handle(t, accountFrameFor(dispatchNativeDID, false, "deactivated")))
	seedConsentHeldRestore(t, database)

	require.NoError(t, fixture.handle(t, accountFrameFor(dispatchNativeDID, false, "suspended")))
	assert.Equal(t, store.DeliveryStateHeld, consentRestoreState(t, database),
		"a status that keeps the account paused releases nothing")

	require.NoError(t, fixture.handle(t, accountFrameFor(dispatchNativeDID, true, "active")))
	assert.Equal(t, store.DeliveryStatePending, consentRestoreState(t, database),
		"the reactivation puts the held restore back on the queue")
}

func TestFederationHandler_ReEnablingReleasesARestoreHeldOnConsent(t *testing.T) {
	database := dispatchTestDB(t)
	seedAPActor(t, database, dispatchNativeDID, "alice")
	fixture := newDispatchFixture(t, database)
	require.NoError(t, fixture.handle(t, federationFrameNoDeleteRemote(dispatchNativeDID, dispatchRev, false)))
	seedConsentHeldRestore(t, database)

	require.NoError(t, fixture.handle(t, federationFrameNoDeleteRemote(dispatchNativeDID, dispatchRevHigher, true)))
	assert.Equal(t, store.DeliveryStatePending, consentRestoreState(t, database),
		"the opt-in puts the held restore back on the queue")
}
