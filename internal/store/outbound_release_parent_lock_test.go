package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// parentAccountStatus is the confirmed `#account` takedown of the parent's
// author, an account-wide source on each of that author's records.
var parentAccountStatus = TakedownSource{Kind: TakedownSourceAccountStatus, Key: arBridgedDID}

// holdOnObject holds activityID's rows on its own object.
func holdOnObject(t *testing.T, database *sql.DB, activityID, objectATURI string) {
	t.Helper()
	_, err := database.Exec(`UPDATE outbound_deliveries
		SET state = 'held', held_reason = $2, held_subject = $3
		WHERE activity_id = $1`, activityID, HeldReasonObject, objectATURI)
	require.NoError(t, err, "hold %s on its object", activityID)
}

// waitingOnSubjectLock reports whether some backend waits for subject's
// takedown-subject lock.
func waitingOnSubjectLock(t *testing.T, database *sql.DB, subject string) bool {
	t.Helper()
	var waiting bool
	require.NoError(t, database.QueryRow(`
		SELECT EXISTS (SELECT 1 FROM pg_locks
		               WHERE locktype = 'advisory' AND NOT granted
		                 AND classid = $1 AND objid = hashtext($2)::oid
		                 AND objsubid = 2)`,
		arLockNamespace, subject).Scan(&waiting))
	return waiting
}

// A record-label lift of a reply (tx A) and the parent author's #account
// restore (tx B) interleave: A finds the parent still removed and rekeys the
// reply onto it, while B clears the parent and releases the rows held on it.
// A holds the parent's subject lock until it commits, so B's release runs
// after A's rekey and finds the reply held on the parent; without that lock B
// commits first, finds nothing held on the parent, and the reply stays held
// on a cleared parent for good.
func TestReleaseHeldTx_RekeyOntoTheParentSerializesWithTheParentsRestore(t *testing.T) {
	database := takedownStoreDB(t)
	ctx := context.Background()
	repo := NewOutboundDeliveries(database)
	takedowns := NewOutboundTakedowns(database)
	seedTakedownObject(t, database, tkObjectATURI, tkObjectAPID)
	seedParentHeldReply(t, database, arFirstReplyID, tkObjectAPID, false)
	holdOnObject(t, database, arFirstReplyID, tkObjectATURI)
	committed(t, database, func(tx *sql.Tx) {
		_, err := takedowns.AddSourceTx(ctx, tx, arParentATURI, parentAccountStatus)
		require.NoError(t, err)
	})

	// Tx A: the reply's own label is lifted; its parent is still removed.
	txA, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = txA.Rollback() }()
	released, err := repo.ReleaseHeldTx(ctx, txA, tkObjectATURI)
	require.NoError(t, err)
	assert.EqualValues(t, 0, released, "the parent still blocks the reply")

	// Tx B: the parent author's #account restore, which holds the parent's
	// subject lock (as lockAccountObjectsTx takes it) while it clears the
	// parent and releases the rows held on it.
	type outcome struct {
		released int64
		err      error
	}
	done := make(chan outcome, 1)
	go func() {
		released, err := func() (int64, error) {
			txB, err := database.BeginTx(ctx, nil)
			if err != nil {
				return 0, err
			}
			defer func() { _ = txB.Rollback() }()
			if err := LockTakedownSubjectsTx(ctx, txB, arParentATURI, arBridgedDID); err != nil {
				return 0, err
			}
			if _, err := takedowns.RemoveSourceTx(ctx, txB, arParentATURI, parentAccountStatus); err != nil {
				return 0, err
			}
			released, err := repo.ReleaseHeldTx(ctx, txB, arParentATURI)
			if err != nil {
				return 0, err
			}
			return released, txB.Commit()
		}()
		done <- outcome{released, err}
	}()

	// B either waits on the parent's lock or, unserialized, finishes.
	var finishedBeforeA *outcome
	deadline := time.Now().Add(5 * time.Second)
	for finishedBeforeA == nil && !waitingOnSubjectLock(t, database, arParentATURI) {
		select {
		case got := <-done:
			finishedBeforeA = &got
		case <-time.After(10 * time.Millisecond):
		}
		require.True(t, time.Now().Before(deadline), "tx B neither waited on the parent's lock nor finished")
	}

	require.NoError(t, txA.Commit())
	got := outcome{}
	if finishedBeforeA != nil {
		got = *finishedBeforeA
	} else {
		select {
		case got = <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("tx B did not finish after tx A committed")
		}
	}
	require.NoError(t, got.err)

	assert.Equal(t, DeliveryStatePending, getDelivery(t, repo, arFirstReplyID, delTargetInbox).State,
		"the reply goes back to the queue once both blockers clear (held_reason %q)",
		heldReason(t, database, arFirstReplyID))
	assert.EqualValues(t, 1, got.released, "the parent's restore releases the reply A rekeyed onto it")
	assert.Nil(t, finishedBeforeA, "the parent's restore waits for the release that read the parent")
}

// A row held on the subject between the release's parent read and its lock (a
// hold that committed in that window) is settled only under its parent's lock:
// the release locks again with the grown set, in one sorted call, instead of
// rekeying a row onto a parent it never locked.
func TestReleaseHeldTx_SettlesARowHeldAfterItsParentReadOnlyUnderThatParentsLock(t *testing.T) {
	database := takedownStoreDB(t)
	ctx := context.Background()
	repo := NewOutboundDeliveries(database)
	seedTakedownObject(t, database, tkObjectATURI, tkObjectAPID)
	seedParentHeldReply(t, database, arFirstReplyID, tkObjectAPID, false)
	holdOnObject(t, database, arFirstReplyID, tkObjectATURI)

	// A second reply on the same object answers another parent.
	const otherParentATURI = "at://did:plc:releaseotherparent0001/social.coves.community.comment/3lzrlparent02"
	payload, err := json.Marshal(map[string]any{
		"type": "Update", "id": arLateReplyID,
		"actor":  "https://coves.social/ap/actor/" + testDID,
		"object": map[string]any{"type": "Note", "id": tkObjectAPID},
	})
	require.NoError(t, err)
	seedActivity(t, NewOutboundActivities(database), OutboundActivity{
		ActivityID: arLateReplyID, ActorDID: testDID, Kind: "Update", Payload: payload,
		ParentATURI: otherParentATURI,
	})
	_, err = repo.Enqueue(ctx, OutboundDelivery{
		ActivityID: arLateReplyID, TargetInbox: delTargetInbox, OrderingKey: delOrderingKey,
	})
	require.NoError(t, err)

	reads := 0
	heldSubjectsReadHook = func() {
		reads++
		if reads == 1 {
			// A hold on the object commits before the lock.
			holdOnObject(t, database, arLateReplyID, tkObjectATURI)
		}
	}
	t.Cleanup(func() { heldSubjectsReadHook = nil })

	tx, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	released, err := repo.ReleaseHeldTx(ctx, tx, tkObjectATURI)
	require.NoError(t, err)
	assert.EqualValues(t, 2, released, "both rows go back to the queue")
	assert.Equal(t, 2, reads, "the grown set is read and locked again")
	assert.True(t, subjectLockedElsewhere(t, database, otherParentATURI),
		"the late row's parent is locked by the release that settled it")
	assert.True(t, subjectLockedElsewhere(t, database, arParentATURI))
	assert.True(t, subjectLockedElsewhere(t, database, tkObjectATURI))
	require.NoError(t, tx.Commit())

	assert.Equal(t, DeliveryStatePending, getDelivery(t, repo, arFirstReplyID, delTargetInbox).State)
	assert.Equal(t, DeliveryStatePending, getDelivery(t, repo, arLateReplyID, delTargetInbox).State)
}

// A reply held on its parent is then taken down itself (a hold updates only
// pending rows, so the row stays held on the parent). The parent's #account
// restore (tx A) finds the reply's own object removed and rekeys the row onto
// it, while the reply author's #account restore (tx B) clears the reply and
// releases the rows held on it. A holds the reply's subject lock until it
// commits, so B's release runs after A's rekey and finds the row held on the
// reply; without that lock B commits first, finds nothing held on the reply,
// and the row stays held on a cleared reply for good.
func TestReleaseHeldTx_RekeyOntoTheRowsObjectSerializesWithTheObjectsRestore(t *testing.T) {
	database := takedownStoreDB(t)
	ctx := context.Background()
	repo := NewOutboundDeliveries(database)
	takedowns := NewOutboundTakedowns(database)
	replyAccountStatus := TakedownSource{Kind: TakedownSourceAccountStatus, Key: testDID}
	seedTakedownObject(t, database, tkObjectATURI, tkObjectAPID)
	seedParentHeldReply(t, database, arFirstReplyID, tkObjectAPID, true)
	committed(t, database, func(tx *sql.Tx) {
		_, err := takedowns.AddSourceTx(ctx, tx, arParentATURI, parentAccountStatus)
		require.NoError(t, err)
		_, err = takedowns.AddSourceTx(ctx, tx, tkObjectATURI, replyAccountStatus)
		require.NoError(t, err)
	})
	require.Equal(t, HeldReasonParent, heldReason(t, database, arFirstReplyID),
		"the reply's takedown leaves its parent-held row on the parent")

	// Tx A: the parent author's #account restore clears the parent; the reply
	// is still removed.
	txA, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = txA.Rollback() }()
	require.NoError(t, LockTakedownSubjectsTx(ctx, txA, arParentATURI, arBridgedDID))
	_, err = takedowns.RemoveSourceTx(ctx, txA, arParentATURI, parentAccountStatus)
	require.NoError(t, err)
	released, err := repo.ReleaseHeldTx(ctx, txA, arParentATURI)
	require.NoError(t, err)
	assert.EqualValues(t, 0, released, "the reply's own takedown still blocks it")

	// Tx B: the reply author's #account restore, which holds the reply's
	// subject lock (as lockAccountObjectsTx takes it) while it clears the
	// reply and releases the rows held on it.
	type outcome struct {
		released int64
		err      error
	}
	done := make(chan outcome, 1)
	go func() {
		released, err := func() (int64, error) {
			txB, err := database.BeginTx(ctx, nil)
			if err != nil {
				return 0, err
			}
			defer func() { _ = txB.Rollback() }()
			if err := LockTakedownSubjectsTx(ctx, txB, tkObjectATURI, testDID); err != nil {
				return 0, err
			}
			if _, err := takedowns.RemoveSourceTx(ctx, txB, tkObjectATURI, replyAccountStatus); err != nil {
				return 0, err
			}
			released, err := repo.ReleaseHeldTx(ctx, txB, tkObjectATURI)
			if err != nil {
				return 0, err
			}
			return released, txB.Commit()
		}()
		done <- outcome{released, err}
	}()

	// B either waits on the reply's lock or, unserialized, finishes.
	var finishedBeforeA *outcome
	deadline := time.Now().Add(5 * time.Second)
	for finishedBeforeA == nil && !waitingOnSubjectLock(t, database, tkObjectATURI) {
		select {
		case got := <-done:
			finishedBeforeA = &got
		case <-time.After(10 * time.Millisecond):
		}
		require.True(t, time.Now().Before(deadline), "tx B neither waited on the reply's lock nor finished")
	}

	require.NoError(t, txA.Commit())
	got := outcome{}
	if finishedBeforeA != nil {
		got = *finishedBeforeA
	} else {
		select {
		case got = <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("tx B did not finish after tx A committed")
		}
	}
	require.NoError(t, got.err)

	assert.Equal(t, DeliveryStatePending, getDelivery(t, repo, arFirstReplyID, delTargetInbox).State,
		"the reply goes back to the queue once both blockers clear (held_reason %q)",
		heldReason(t, database, arFirstReplyID))
	assert.EqualValues(t, 1, got.released, "the reply's restore releases the row A rekeyed onto it")
	assert.Nil(t, finishedBeforeA, "the reply's restore waits for the release that read the reply")
}
