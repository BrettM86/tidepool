package consume

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// The opt-out and the worker's consent hold (outbound.Worker.holdOnConsent)
// both touch the author's ap_actors row and the author's pending Create/Update
// rows. The hold takes the actor row (FOR SHARE) and then the delivery rows;
// the opt-out must take them in the same order, or the two deadlock.
//
// Each test plays the hold by hand on its own transaction, in the order the
// worker runs it, and parks it between its two steps while the opt-out runs:
//
//  1. the hold share-locks the paused author's actor row;
//  2. the opt-out starts and the test waits until it is blocked on ap_actors;
//  3. the hold then updates the object's pending Create.
//
// With the opt-out taking the delivery rows first, step 3 waits on the
// opt-out while the opt-out waits on step 1's share lock.

const (
	lockOrderCreateID = "https://coves.social/ap/activity/lockordercreate0001"
	lockOrderSubject  = "at://" + dispatchNativeDID + "/social.coves.community.postv2/3lzlockorder01"
)

// seedPausedAuthorWithPendingCreate seeds a delivery-paused native author with
// one pending, unclaimed Create of lockOrderSubject.
func seedPausedAuthorWithPendingCreate(t *testing.T, database *sql.DB) {
	t.Helper()
	ctx := context.Background()
	seedAPActor(t, database, dispatchNativeDID, "alice")
	_, err := database.ExecContext(ctx,
		`UPDATE ap_actors SET delivery_paused = true WHERE did = $1`, dispatchNativeDID)
	require.NoError(t, err)
	objectID := acceptUserOrigin + "/ap/object/" + lockOrderSubject[len("at://"):]
	_, err = store.NewOutboundActivities(database).Insert(ctx, store.OutboundActivity{
		ActivityID: lockOrderCreateID,
		ActorDID:   dispatchNativeDID,
		Kind:       "Create",
		Payload: []byte(`{"type":"Create","id":"` + lockOrderCreateID +
			`","object":{"id":"` + objectID + `"}}`),
	})
	require.NoError(t, err)
	_, err = store.NewOutboundDeliveries(database).Enqueue(ctx, store.OutboundDelivery{
		ActivityID: lockOrderCreateID, TargetInbox: consentInbox, OrderingKey: consentOrderingKey,
	})
	require.NoError(t, err)
}

// beginConsentHold opens the hold's transaction and share-locks the author's
// actor row, as holdOnConsent's first statement does. lock_timeout bounds a
// wait no deadlock detector can see, so a regression fails instead of hanging.
func beginConsentHold(t *testing.T, database *sql.DB) *sql.Tx {
	t.Helper()
	ctx := context.Background()
	tx, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	_, err = tx.ExecContext(ctx, `SET LOCAL lock_timeout = '5s'`)
	require.NoError(t, err)
	blocked, err := store.LockActorConsentTx(ctx, tx, dispatchNativeDID)
	require.NoError(t, err)
	require.True(t, blocked, "the paused author's consent is withdrawn")
	return tx
}

// waitForActorRowWaiter waits until some backend is blocked on a lock in a
// statement on ap_actors.
func waitForActorRowWaiter(t *testing.T, database *sql.DB, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			t.Fatalf("the opt-out finished before blocking on the held actor row: %v", err)
		default:
		}
		var waiting int
		require.NoError(t, database.QueryRow(`
			SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock'
			  AND query LIKE '%ap_actors%'`).Scan(&waiting))
		if waiting > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("the opt-out never blocked on the actor row the hold share-locks")
}

// finishConsentHold runs the hold's second step and commits it.
func finishConsentHold(t *testing.T, tx *sql.Tx, database *sql.DB) {
	t.Helper()
	ctx := context.Background()
	_, err := store.NewOutboundDeliveries(database).HoldObjectOnConsentTx(ctx, tx, lockOrderSubject)
	require.NoError(t, err, "the consent hold must not deadlock or wait on the opt-out")
	require.NoError(t, tx.Commit())
}

func awaitOptOut(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		require.NoError(t, err, "the opt-out must not deadlock against the consent hold")
	case <-time.After(30 * time.Second):
		t.Fatal("the opt-out never finished")
	}
}

func TestFederationOptOut_SoftTierTakesTheActorRowBeforeTheConsentHoldsDeliveryRows(t *testing.T) {
	database := dispatchTestDB(t)
	seedPausedAuthorWithPendingCreate(t, database)
	fixture := newDispatchFixture(t, database)

	hold := beginConsentHold(t, database)
	done := make(chan error, 1)
	event := parseFrame(t, federationFrameNoDeleteRemote(dispatchNativeDID, dispatchRev, false))
	go func() { done <- fixture.dispatcher.HandleEvent(context.Background(), event) }()
	waitForActorRowWaiter(t, database, done)
	finishConsentHold(t, hold, database)
	awaitOptOut(t, done)

	enabled, _ := actorEnabled(t, database, dispatchNativeDID)
	assert.False(t, enabled, "the opt-out mirrored onto the actor")
}

// actorWritingDeleter stands in for outbound.Purger's lock footprint: it
// updates the actor row from its own transaction, as the purge's tombstone
// does.
type actorWritingDeleter struct {
	db *sql.DB
}

func (d *actorWritingDeleter) DeleteRemoteContent(ctx context.Context, did string) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin purge: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `SET LOCAL lock_timeout = '5s'`); err != nil {
		return fmt.Errorf("set purge lock timeout: %w", err)
	}
	if err := store.NewAPActors(d.db).TombstoneTx(ctx, tx, did); err != nil {
		return fmt.Errorf("tombstone actor: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit purge: %w", err)
	}
	return nil
}

func TestFederationOptOut_DestructiveTierHoldsNoDeliveryRowAcrossThePurge(t *testing.T) {
	database := dispatchTestDB(t)
	seedPausedAuthorWithPendingCreate(t, database)
	fixture := newDispatchFixture(t, database, func(opts *Options) {
		opts.RemoteDeleter = &actorWritingDeleter{db: database}
	})

	hold := beginConsentHold(t, database)
	done := make(chan error, 1)
	event := parseFrame(t, federationFrame(dispatchNativeDID, dispatchRev, "create", false, true))
	go func() { done <- fixture.dispatcher.HandleEvent(context.Background(), event) }()
	// The purge's tombstone waits on the hold's share lock. If the gate
	// transaction already holds the pending Create, the hold waits on the gate,
	// which waits in Go on the purge: a cycle Postgres cannot see.
	waitForActorRowWaiter(t, database, done)
	finishConsentHold(t, hold, database)
	awaitOptOut(t, done)

	enabled, _ := actorEnabled(t, database, dispatchNativeDID)
	assert.False(t, enabled, "the opt-out mirrored onto the actor")
}
