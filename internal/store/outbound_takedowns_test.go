package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/testutil"
)

// Chunk 03 (B0): the per-object takedown source set and the held delivery
// state, on a caller's transaction. A delivery belongs to an object when its
// activity's payload names the object's AP id as object.id, the shape the
// Enqueuer writes for a Create or Update{Page}.

const (
	tkObjectATURI = "at://" + testDID + "/social.coves.community.postv2/3lztkobj00001"
	tkObjectAPID  = "https://coves.social/ap/object/" + testDID + "/social.coves.community.postv2/3lztkobj00001"
	tkOtherATURI  = "at://" + testDID + "/social.coves.community.postv2/3lztkobj00002"
	tkOtherAPID   = "https://coves.social/ap/object/" + testDID + "/social.coves.community.postv2/3lztkobj00002"
	// tkSettleInbox is a second community's inbox, so a row held for
	// settlement there does not sit at the head of delOrderingKey.
	tkSettleInbox = "https://lemmy.world/c/linux/inbox"
	tkSettleKey   = "https://lemmy.world/c/linux"
)

var (
	tkRecordLabel    = TakedownSource{Kind: TakedownSourceRecordLabel, Key: "did:web:coves.social"}
	tkAccountLabel   = TakedownSource{Kind: TakedownSourceAccountLabel, Key: testDID}
	tkCommunityLabel = TakedownSource{Kind: TakedownSourceCommunityLabel, Key: testCommunityDID}

	tkCreateID     = "https://coves.social/ap/activity/" + repeatHex('1')
	tkUpdateID     = "https://coves.social/ap/activity/" + repeatHex('2')
	tkOtherID      = "https://coves.social/ap/activity/" + repeatHex('3')
	tkSettlementID = "https://coves.social/ap/activity/" + repeatHex('4')
)

// takedownStoreDB is standingTestDB plus the planned takedown tables, when
// migration 055 has created them; a missing one is left for the test to fail on.
func takedownStoreDB(t *testing.T) *sql.DB {
	t.Helper()
	database := standingTestDB(t)
	for _, table := range []string{"outbound_takedown_sources", "outbound_takedowns"} {
		var exists bool
		require.NoError(t, database.QueryRow(`SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists))
		if exists {
			testutil.Truncate(t, database, table)
		}
	}
	return database
}

// seedTakedownObject writes the outbound object a hold is keyed on.
func seedTakedownObject(t *testing.T, database *sql.DB, atURI, apID string) {
	t.Helper()
	_, err := NewOutboundObjects(database).Upsert(context.Background(), OutboundObject{
		ATURI:              atURI,
		APObjectID:         apID,
		LastCID:            "bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqrsqxi3jgxte",
		LastRev:            "3lztkrev00001",
		CommunityDID:       testCommunityDID,
		CommunityAPID:      delOrderingKey,
		TranslatedSnapshot: []byte(`{"type":"Page"}`),
	})
	require.NoError(t, err, "seed outbound object %s", atURI)
}

// seedObjectDelivery inserts an activity of kind on the object apID, with the
// payload shape the Enqueuer writes, and one pending delivery for it.
func seedObjectDelivery(t *testing.T, database *sql.DB, kind, activityID, apID, inbox, orderingKey string) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"type":   kind,
		"id":     activityID,
		"actor":  "https://coves.social/ap/actor/" + testDID,
		"object": map[string]any{"type": "Page", "id": apID},
	})
	require.NoError(t, err)
	seedActivity(t, NewOutboundActivities(database), OutboundActivity{
		ActivityID: activityID, ActorDID: testDID, Kind: kind, Payload: payload,
	})
	_, err = NewOutboundDeliveries(database).Enqueue(context.Background(), OutboundDelivery{
		ActivityID: activityID, TargetInbox: inbox, OrderingKey: orderingKey,
	})
	require.NoError(t, err)
}

// committed runs fn on a transaction and commits it.
func committed(t *testing.T, database *sql.DB, fn func(tx *sql.Tx)) {
	t.Helper()
	tx, err := database.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	fn(tx)
	require.NoError(t, tx.Commit())
}

// rolledBack runs fn on a transaction and rolls it back.
func rolledBack(t *testing.T, database *sql.DB, fn func(tx *sql.Tx)) {
	t.Helper()
	tx, err := database.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	fn(tx)
	require.NoError(t, tx.Rollback())
}

// takeDown adds src to atURI's set and holds the object's queued rows in one
// committed transaction, returning how many rows the hold took.
func takeDown(t *testing.T, database *sql.DB, atURI string, src TakedownSource) int64 {
	t.Helper()
	ctx := context.Background()
	var held int64
	committed(t, database, func(tx *sql.Tx) {
		_, err := NewOutboundTakedowns(database).AddSourceTx(ctx, tx, atURI, src)
		require.NoError(t, err)
		held, err = NewOutboundDeliveries(database).HoldForObjectTx(ctx, tx, atURI)
		require.NoError(t, err)
	})
	return held
}

func getDelivery(t *testing.T, repo OutboundDeliveries, activityID, inbox string) *OutboundDelivery {
	t.Helper()
	got, err := repo.Get(context.Background(), activityID, inbox)
	require.NoError(t, err)
	return got
}

func TestOutboundTakedowns_SourceChangesReportTheSetTransition(t *testing.T) {
	database := takedownStoreDB(t)
	takedowns := NewOutboundTakedowns(database)
	ctx := context.Background()

	steps := []struct {
		name        string
		add         bool
		src         TakedownSource
		want        TakedownTransition
		wantRemoved bool
	}{
		{"first source on an empty set", true, tkRecordLabel, TakedownBecameRemoved, true},
		{"a second source", true, tkAccountLabel, TakedownUnchanged, true},
		{"the same source again", true, tkRecordLabel, TakedownUnchanged, true},
		{"remove one of two", false, tkAccountLabel, TakedownUnchanged, true},
		{"remove a source not in the set", false, tkCommunityLabel, TakedownUnchanged, true},
		{"remove the last source", false, tkRecordLabel, TakedownBecameClear, false},
		{"remove from an empty set", false, tkRecordLabel, TakedownUnchanged, false},
	}
	for _, step := range steps {
		var got TakedownTransition
		committed(t, database, func(tx *sql.Tx) {
			var err error
			if step.add {
				got, err = takedowns.AddSourceTx(ctx, tx, tkObjectATURI, step.src)
			} else {
				got, err = takedowns.RemoveSourceTx(ctx, tx, tkObjectATURI, step.src)
			}
			require.NoError(t, err, step.name)
		})
		require.Equal(t, step.want, got, "%s: transition", step.name)
		removed, err := takedowns.IsRemoved(ctx, tkObjectATURI)
		require.NoError(t, err)
		require.Equal(t, step.wantRemoved, removed, "%s: IsRemoved", step.name)
	}
}

func TestOutboundTakedowns_ARolledBackCallerTransactionChangesNothing(t *testing.T) {
	database := takedownStoreDB(t)
	takedowns := NewOutboundTakedowns(database)
	repo := NewOutboundDeliveries(database)
	ctx := context.Background()
	seedTakedownObject(t, database, tkObjectATURI, tkObjectAPID)
	seedObjectDelivery(t, database, "Create", tkCreateID, tkObjectAPID, delTargetInbox, delOrderingKey)
	seedObjectDelivery(t, database, "Update", tkUpdateID, tkObjectAPID, delTargetInbox, delOrderingKey)
	createSeq := getDelivery(t, repo, tkCreateID, delTargetInbox).Seq
	updateSeq := getDelivery(t, repo, tkUpdateID, delTargetInbox).Seq

	// A takedown whose transaction rolls back.
	rolledBack(t, database, func(tx *sql.Tx) {
		transition, err := takedowns.AddSourceTx(ctx, tx, tkObjectATURI, tkRecordLabel)
		require.NoError(t, err)
		require.Equal(t, TakedownBecameRemoved, transition, "inside the tx the set became non-empty")
		held, err := repo.HoldForObjectTx(ctx, tx, tkObjectATURI)
		require.NoError(t, err)
		require.EqualValues(t, 2, held, "inside the tx the hold took both queued rows")
	})
	removed, err := takedowns.IsRemoved(ctx, tkObjectATURI)
	require.NoError(t, err)
	assert.False(t, removed, "the rolled-back add left the set empty")
	for _, id := range []string{tkCreateID, tkUpdateID} {
		got := getDelivery(t, repo, id, delTargetInbox)
		assert.Equal(t, DeliveryStatePending, got.State, "the rolled-back hold left %s pending", id)
	}

	// A committed takedown, then a restore whose transaction rolls back.
	require.EqualValues(t, 2, takeDown(t, database, tkObjectATURI, tkRecordLabel))
	rolledBack(t, database, func(tx *sql.Tx) {
		transition, err := takedowns.RemoveSourceTx(ctx, tx, tkObjectATURI, tkRecordLabel)
		require.NoError(t, err)
		require.Equal(t, TakedownBecameClear, transition, "inside the tx the set became empty")
		released, err := repo.ReleaseHeldTx(ctx, tx, tkObjectATURI)
		require.NoError(t, err)
		require.EqualValues(t, 2, released, "inside the tx the release took both held rows")
	})
	removed, err = takedowns.IsRemoved(ctx, tkObjectATURI)
	require.NoError(t, err)
	assert.True(t, removed, "the rolled-back remove left the source in the set")
	for id, seq := range map[string]int64{tkCreateID: createSeq, tkUpdateID: updateSeq} {
		got := getDelivery(t, repo, id, delTargetInbox)
		assert.Equal(t, DeliveryStateHeld, got.State, "the rolled-back release left %s held", id)
		assert.Equal(t, seq, got.Seq, "the rolled-back release left %s's seq unchanged", id)
	}
}

func TestOutboundDeliveries_HoldForObjectTxTakesOnlyTheObjectsQueuedRows(t *testing.T) {
	database := takedownStoreDB(t)
	repo := NewOutboundDeliveries(database)
	seedTakedownObject(t, database, tkObjectATURI, tkObjectAPID)
	seedTakedownObject(t, database, tkOtherATURI, tkOtherAPID)
	// The object's Create already reached the peer in another community; only
	// its settlement is outstanding, so the hold leaves it.
	seedObjectDelivery(t, database, "Create", tkSettlementID, tkObjectAPID, tkSettleInbox, tkSettleKey)
	holdForSettlement(t, repo, tkSettlementID, tkSettleInbox)
	seedObjectDelivery(t, database, "Update", tkUpdateID, tkObjectAPID, delTargetInbox, delOrderingKey)
	seedObjectDelivery(t, database, "Create", tkOtherID, tkOtherAPID, delTargetInbox, delOrderingKey)

	assert.EqualValues(t, 1, takeDown(t, database, tkObjectATURI, tkRecordLabel), "the hold takes one row")

	assert.Equal(t, DeliveryStateHeld, getDelivery(t, repo, tkUpdateID, delTargetInbox).State,
		"the object's queued Update is held")
	settling := getDelivery(t, repo, tkSettlementID, tkSettleInbox)
	assert.Equal(t, DeliveryStatePending, settling.State, "the row held for settlement stays pending")
	assert.Equal(t, DeliveryHeldForSettlement, settling.LastErrorClass, "and keeps its settlement class")
	assert.Equal(t, DeliveryStatePending, getDelivery(t, repo, tkOtherID, delTargetInbox).State,
		"another object's row is not held")
}

func TestOutboundDeliveries_ClaimNextSkipsAHeldHeadOfItsKey(t *testing.T) {
	database := takedownStoreDB(t)
	repo := NewOutboundDeliveries(database)
	ctx := context.Background()
	seedTakedownObject(t, database, tkObjectATURI, tkObjectAPID)
	seedTakedownObject(t, database, tkOtherATURI, tkOtherAPID)
	seedObjectDelivery(t, database, "Create", tkCreateID, tkObjectAPID, delTargetInbox, delOrderingKey)
	seedObjectDelivery(t, database, "Create", tkOtherID, tkOtherAPID, delTargetInbox, delOrderingKey)

	takeDown(t, database, tkObjectATURI, tkRecordLabel)

	claimed, err := repo.ClaimNext(ctx, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, tkOtherID, claimed.ActivityID, "the held head no longer blocks its key")
	_, applied, err := repo.MarkDelivered(ctx, claimed.ActivityID, claimed.TargetInbox, 202, *claimed.ClaimedUntil)
	require.NoError(t, err)
	require.True(t, applied)

	_, err = repo.ClaimNext(ctx, time.Minute)
	assert.True(t, errors.IsNotFound(err), "a held row is never claimed, got %v", err)
}

func TestOutboundDeliveries_CancellationsLeaveHeldRows(t *testing.T) {
	cancels := []struct {
		name   string
		cancel func(t *testing.T, database *sql.DB) int64
	}{
		{"CancelForActor", func(t *testing.T, database *sql.DB) int64 {
			n, err := NewOutboundDeliveries(database).CancelForActor(context.Background(), testDID)
			require.NoError(t, err)
			return n
		}},
		{"CancelOutwardForActorTx", func(t *testing.T, database *sql.DB) int64 {
			var n int64
			committed(t, database, func(tx *sql.Tx) {
				var err error
				n, err = NewOutboundDeliveries(database).CancelOutwardForActorTx(context.Background(), tx, testDID)
				require.NoError(t, err)
			})
			return n
		}},
		{"CancelForCommunity", func(t *testing.T, database *sql.DB) int64 {
			n, err := NewOutboundDeliveries(database).CancelForCommunity(context.Background(), delOrderingKey)
			require.NoError(t, err)
			return n
		}},
		{"CommunityBans.Ban", func(t *testing.T, database *sql.DB) int64 {
			n, _, err := NewCommunityBans(database).Ban(context.Background(), CommunityBan{
				CommunityDID:  testCommunityDID,
				SubjectDID:    testDID,
				CommunityAPID: delOrderingKey,
				Reason:        "spam",
			})
			require.NoError(t, err)
			return n
		}},
	}
	for _, tc := range cancels {
		t.Run(tc.name, func(t *testing.T) {
			database := takedownStoreDB(t)
			repo := NewOutboundDeliveries(database)
			seedTakedownObject(t, database, tkObjectATURI, tkObjectAPID)
			seedTakedownObject(t, database, tkOtherATURI, tkOtherAPID)
			seedObjectDelivery(t, database, "Create", tkCreateID, tkObjectAPID, delTargetInbox, delOrderingKey)
			seedObjectDelivery(t, database, "Create", tkOtherID, tkOtherAPID, delTargetInbox, delOrderingKey)
			require.EqualValues(t, 1, takeDown(t, database, tkObjectATURI, tkRecordLabel),
				"the hold takes the object's Create")

			assert.EqualValues(t, 1, tc.cancel(t, database), "the cancel takes the ordinary pending row only")
			assert.Equal(t, DeliveryStateHeld, getDelivery(t, repo, tkCreateID, delTargetInbox).State,
				"the held row is outside the cancel")
			assert.Equal(t, DeliveryStateCancelled, getDelivery(t, repo, tkOtherID, delTargetInbox).State,
				"the cancel still applies to the ordinary pending row")
		})
	}
}

func TestOutboundDeliveries_ReleaseHeldTxRequeuesAtTheTailInOriginalOrder(t *testing.T) {
	database := takedownStoreDB(t)
	takedowns := NewOutboundTakedowns(database)
	repo := NewOutboundDeliveries(database)
	ctx := context.Background()
	seedTakedownObject(t, database, tkObjectATURI, tkObjectAPID)
	seedTakedownObject(t, database, tkOtherATURI, tkOtherAPID)
	seedObjectDelivery(t, database, "Create", tkCreateID, tkObjectAPID, delTargetInbox, delOrderingKey)
	seedObjectDelivery(t, database, "Update", tkUpdateID, tkObjectAPID, delTargetInbox, delOrderingKey)

	require.EqualValues(t, 2, takeDown(t, database, tkObjectATURI, tkRecordLabel), "the hold takes both rows")
	committed(t, database, func(tx *sql.Tx) {
		_, err := takedowns.AddSourceTx(ctx, tx, tkObjectATURI, tkAccountLabel)
		require.NoError(t, err)
	})
	// Another object's work enqueued during the hold.
	seedObjectDelivery(t, database, "Create", tkOtherID, tkOtherAPID, delTargetInbox, delOrderingKey)

	// One of two sources removed: the object is still removed, nothing is released.
	committed(t, database, func(tx *sql.Tx) {
		_, err := takedowns.RemoveSourceTx(ctx, tx, tkObjectATURI, tkAccountLabel)
		require.NoError(t, err)
		released, err := repo.ReleaseHeldTx(ctx, tx, tkObjectATURI)
		require.NoError(t, err)
		assert.EqualValues(t, 0, released, "a source still stands, so nothing is released")
	})
	assert.Equal(t, DeliveryStateHeld, getDelivery(t, repo, tkCreateID, delTargetInbox).State)

	// The last source removed: both rows go to the tail.
	committed(t, database, func(tx *sql.Tx) {
		_, err := takedowns.RemoveSourceTx(ctx, tx, tkObjectATURI, tkRecordLabel)
		require.NoError(t, err)
		released, err := repo.ReleaseHeldTx(ctx, tx, tkObjectATURI)
		require.NoError(t, err)
		assert.EqualValues(t, 2, released, "the release takes both held rows")
	})

	var order []string
	for range 3 {
		claimed, err := repo.ClaimNext(ctx, time.Minute)
		require.NoError(t, err, "claim %d of 3 (claimed so far %v)", len(order)+1, order)
		order = append(order, claimed.ActivityID)
		_, applied, err := repo.MarkDelivered(ctx, claimed.ActivityID, claimed.TargetInbox, 202, *claimed.ClaimedUntil)
		require.NoError(t, err)
		require.True(t, applied)
	}
	assert.Equal(t, []string{tkOtherID, tkCreateID, tkUpdateID}, order,
		"the released rows follow the work enqueued during the hold, Create before Update")
}

// tkUndoID is the restore Undo{Delete} of tkObjectATURI in the consent cases.
var tkUndoID = "https://coves.social/ap/activity/" + repeatHex('5')

// consentHeldWorld seeds testDID's actor, delivery-paused, and the
// restore Undo of tkObjectATURI then its Update, both held on consent the way
// the worker holds them; another object's Create queues behind them.
func consentHeldWorld(t *testing.T) *sql.DB {
	t.Helper()
	database := takedownStoreDB(t)
	testutil.Truncate(t, database, "ap_actors", "federation_prefs")
	ctx := context.Background()
	actors := NewAPActors(database)
	_, err := actors.Create(ctx, testAPActor())
	require.NoError(t, err)
	require.NoError(t, actors.SetPaused(ctx, testDID, true))
	seedTakedownObject(t, database, tkObjectATURI, tkObjectAPID)
	seedTakedownObject(t, database, tkOtherATURI, tkOtherAPID)
	seedObjectDelivery(t, database, "Undo", tkUndoID, tkObjectAPID, delTargetInbox, delOrderingKey)
	seedObjectDelivery(t, database, "Update", tkUpdateID, tkObjectAPID, delTargetInbox, delOrderingKey)
	_, err = database.Exec(`UPDATE outbound_deliveries
		SET state = 'held', held_reason = 'consent', held_subject = $1
		WHERE activity_id IN ($2, $3)`, tkObjectATURI, tkUndoID, tkUpdateID)
	require.NoError(t, err, "hold the restore and its Update on consent")
	seedObjectDelivery(t, database, "Create", tkOtherID, tkOtherAPID, delTargetInbox, delOrderingKey)
	return database
}

// releaseConsent unpauses testDID and runs the consent release in one
// committed transaction, returning how many rows it released.
func releaseConsent(t *testing.T, database *sql.DB) int64 {
	t.Helper()
	var released int64
	committed(t, database, func(tx *sql.Tx) {
		_, err := tx.Exec(`UPDATE ap_actors SET delivery_paused = false WHERE did = $1`, testDID)
		require.NoError(t, err)
		released, err = NewOutboundDeliveries(database).ReleaseConsentHeldTx(context.Background(), tx, testDID)
		require.NoError(t, err)
	})
	return released
}

func heldReason(t *testing.T, database *sql.DB, activityID string) string {
	t.Helper()
	var reason string
	require.NoError(t, database.QueryRow(`SELECT held_reason FROM outbound_deliveries
		WHERE activity_id = $1 AND target_inbox = $2`, activityID, delTargetInbox).Scan(&reason))
	return reason
}

func TestOutboundDeliveries_ReleaseConsentHeldTxRequeuesTheRestoreFirst(t *testing.T) {
	database := consentHeldWorld(t)
	repo := NewOutboundDeliveries(database)
	ctx := context.Background()

	committed(t, database, func(tx *sql.Tx) {
		released, err := repo.ReleaseHeldTx(ctx, tx, tkObjectATURI)
		require.NoError(t, err)
		assert.EqualValues(t, 0, released, "a takedown release leaves consent-held rows alone")
	})
	committed(t, database, func(tx *sql.Tx) {
		released, err := repo.ReleaseConsentHeldTx(ctx, tx, testDID)
		require.NoError(t, err)
		assert.EqualValues(t, 0, released, "the author is still paused, so nothing is released")
	})
	assert.Equal(t, DeliveryStateHeld, getDelivery(t, repo, tkUndoID, delTargetInbox).State)

	assert.EqualValues(t, 2, releaseConsent(t, database), "the unpause releases both rows")
	var order []string
	for range 3 {
		claimed, err := repo.ClaimNext(ctx, time.Minute)
		require.NoError(t, err, "claim %d of 3 (claimed so far %v)", len(order)+1, order)
		order = append(order, claimed.ActivityID)
		_, applied, err := repo.MarkDelivered(ctx, claimed.ActivityID, claimed.TargetInbox, 202, *claimed.ClaimedUntil)
		require.NoError(t, err)
		require.True(t, applied)
	}
	assert.Equal(t, []string{tkOtherID, tkUndoID, tkUpdateID}, order,
		"the released rows go to the tail, the restore Undo before the Update")
}

func TestOutboundDeliveries_ReleaseConsentHeldTxCancelsTheWorkOfADeletedObject(t *testing.T) {
	database := consentHeldWorld(t)
	repo := NewOutboundDeliveries(database)
	committed(t, database, func(tx *sql.Tx) {
		_, err := NewOutboundObjects(database).TombstoneTx(context.Background(), tx, tkObjectATURI)
		require.NoError(t, err)
	})

	assert.EqualValues(t, 0, releaseConsent(t, database), "nothing of a deleted object is released")
	assert.Equal(t, DeliveryStateCancelled, getDelivery(t, repo, tkUndoID, delTargetInbox).State,
		"the restore of a deleted object is cancelled")
	assert.Equal(t, DeliveryStateCancelled, getDelivery(t, repo, tkUpdateID, delTargetInbox).State,
		"and so is its Update")
}

func TestOutboundDeliveries_ReleaseConsentHeldTxOfAReRemovedObjectCancelsTheRestore(t *testing.T) {
	database := consentHeldWorld(t)
	repo := NewOutboundDeliveries(database)
	committed(t, database, func(tx *sql.Tx) {
		_, err := NewOutboundTakedowns(database).AddSourceTx(context.Background(), tx, tkObjectATURI, tkRecordLabel)
		require.NoError(t, err)
	})

	assert.EqualValues(t, 0, releaseConsent(t, database), "nothing of a removed object is released")
	assert.Equal(t, DeliveryStateCancelled, getDelivery(t, repo, tkUndoID, delTargetInbox).State,
		"the old episode's restore is cancelled")
	update := getDelivery(t, repo, tkUpdateID, delTargetInbox)
	assert.Equal(t, DeliveryStateHeld, update.State, "the Update stays held")
	assert.Equal(t, "object", heldReason(t, database, tkUpdateID), "now on the object's takedown")
}
