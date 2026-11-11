package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	arBridgedDID    = "did:plc:accountreleasebridged0001"
	arParentATURI   = "at://" + arBridgedDID + "/social.coves.community.comment/3lzarparent01"
	arLockNamespace = takedownSubjectLockNamespace
)

var (
	arFirstReplyID = "https://coves.social/ap/activity/" + repeatHex('a')
	arLateReplyID  = "https://coves.social/ap/activity/" + repeatHex('b')
)

// seedParentHeldReply inserts a native reply Create on the object apID
// answering arParentATURI, with one delivery; held on the parent when held.
func seedParentHeldReply(t *testing.T, database *sql.DB, activityID, apID string, held bool) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"type":   "Create",
		"id":     activityID,
		"actor":  "https://coves.social/ap/actor/" + testDID,
		"object": map[string]any{"type": "Note", "id": apID},
	})
	require.NoError(t, err)
	seedActivity(t, NewOutboundActivities(database), OutboundActivity{
		ActivityID: activityID, ActorDID: testDID, Kind: "Create", Payload: payload,
		ParentATURI: arParentATURI,
	})
	_, err = NewOutboundDeliveries(database).Enqueue(context.Background(), OutboundDelivery{
		ActivityID: activityID, TargetInbox: delTargetInbox, OrderingKey: delOrderingKey,
	})
	require.NoError(t, err)
	if held {
		holdOnArParent(t, database, activityID)
	}
}

func holdOnArParent(t *testing.T, database *sql.DB, activityID string) {
	t.Helper()
	_, err := database.Exec(`UPDATE outbound_deliveries
		SET state = 'held', held_reason = $2, held_subject = $3
		WHERE activity_id = $1`, activityID, HeldReasonParent, arParentATURI)
	require.NoError(t, err, "hold %s on its parent", activityID)
}

// subjectLockedElsewhere reports whether another transaction holds subject's
// takedown-subject lock.
func subjectLockedElsewhere(t *testing.T, database *sql.DB, subject string) bool {
	t.Helper()
	probe, err := database.Begin()
	require.NoError(t, err)
	defer func() { _ = probe.Rollback() }()
	var acquired bool
	require.NoError(t, probe.QueryRow(`SELECT pg_try_advisory_xact_lock($1, hashtext($2))`,
		arLockNamespace, subject).Scan(&acquired))
	return !acquired
}

// A reply held on a record of the account between the release's subject read
// and its lock (a holder that committed in that window) is settled only under
// its own object's lock: the release locks again with the grown set, in one
// sorted call, instead of settling a row whose object it never locked.
func TestReleaseAccountHeldTx_SettlesARowHeldAfterItsSubjectReadOnlyUnderThatRowsLocks(t *testing.T) {
	database := takedownStoreDB(t)
	ctx := context.Background()
	seedTakedownObject(t, database, tkObjectATURI, tkObjectAPID)
	seedTakedownObject(t, database, tkOtherATURI, tkOtherAPID)
	seedParentHeldReply(t, database, arFirstReplyID, tkObjectAPID, true)
	seedParentHeldReply(t, database, arLateReplyID, tkOtherAPID, false)

	reads := 0
	accountHeldSubjectsReadHook = func() {
		reads++
		if reads == 1 {
			// A worker's hold on the parent commits before the lock.
			holdOnArParent(t, database, arLateReplyID)
		}
	}
	t.Cleanup(func() { accountHeldSubjectsReadHook = nil })

	tx, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	released, err := ReleaseAccountHeldTx(ctx, tx, arBridgedDID)
	require.NoError(t, err)
	assert.EqualValues(t, 2, released, "both replies go back to the queue")
	assert.Equal(t, 2, reads, "the grown set is read and locked again")
	assert.True(t, subjectLockedElsewhere(t, database, tkOtherATURI),
		"the late reply's object is locked by the release that settled it")
	assert.True(t, subjectLockedElsewhere(t, database, tkObjectATURI))
	assert.True(t, subjectLockedElsewhere(t, database, arBridgedDID))
	require.NoError(t, tx.Commit())

	repo := NewOutboundDeliveries(database)
	assert.Equal(t, DeliveryStatePending, getDelivery(t, repo, arFirstReplyID, delTargetInbox).State)
	assert.Equal(t, DeliveryStatePending, getDelivery(t, repo, arLateReplyID, delTargetInbox).State)
}
