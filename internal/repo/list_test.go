package repo

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/testutil"
)

func TestListRecords(t *testing.T) {
	manager, _, _ := testManager(t)
	ctx := t.Context()

	want := map[string]string{} // path -> CID
	for i := 0; i < 5; i++ {
		rkey := testRKey(i)
		res, err := manager.PutRecord(ctx, testDID, testCollection, rkey, testRecord(fmt.Sprintf("post %d", i)))
		require.NoError(t, err)
		want[testCollection+"/"+rkey] = res.RecordCID
	}

	entries, err := manager.ListRecords(ctx, testDID)
	require.NoError(t, err)
	require.Len(t, entries, len(want))
	for _, e := range entries {
		path := e.Collection + "/" + e.Rkey
		assert.Equal(t, want[path], e.CID, "CID for %s", path)
		assert.Equal(t, testCollection, e.Collection)
		require.NotNil(t, e.Value)
		assert.Equal(t, testCollection, e.Value["$type"])
		assert.Contains(t, e.Value["text"], "post ")
	}

	t.Run("missing repo is NotFound", func(t *testing.T) {
		_, err := manager.ListRecords(ctx, "did:plc:doesnotexistanywhere1")
		require.Error(t, err)
		assert.True(t, errors.IsNotFound(err), "want NotFound, got %v", err)
	})

	t.Run("invalid did is an error", func(t *testing.T) {
		_, err := manager.ListRecords(ctx, "not-a-did")
		require.Error(t, err)
	})
}

func TestListRecords_DeleteThenRecreateKeepsCID(t *testing.T) {
	// The admin re-emit contract: delete + re-put of the identical value
	// must produce two real commits and land the record back under the
	// same CID (strongRefs stay valid).
	manager, _, _ := testManager(t)
	ctx := t.Context()

	rkey := testRKey(1)
	orig, err := manager.PutRecord(ctx, testDID, testCollection, rkey, testRecord("hello"))
	require.NoError(t, err)

	entries, err := manager.ListRecords(ctx, testDID)
	require.NoError(t, err)
	require.Len(t, entries, 1)

	del, err := manager.DeleteRecord(ctx, testDID, testCollection, rkey)
	require.NoError(t, err)
	require.False(t, del.NoOp)
	require.Greater(t, del.Seq, orig.Seq)

	back, err := manager.PutRecord(ctx, testDID, testCollection, rkey, entries[0].Value)
	require.NoError(t, err)
	require.False(t, back.NoOp, "recreate must be a real commit")
	require.Greater(t, back.Seq, del.Seq)
	assert.Equal(t, orig.RecordCID, back.RecordCID, "identical value must round-trip to the identical CID")
}

func TestListDIDs(t *testing.T) {
	manager, database, _ := testManager(t)
	testutil.Truncate(t, database, "bridged_actors")
	ctx := t.Context()

	const (
		activeDID  = "did:plc:activeactor11111111111a"
		deletedDID = "did:plc:deletedactor1111111111a"
		bareDID    = "did:plc:norowactor111111111111a" // repo without a bridged_actors row
	)
	for i, did := range []string{activeDID, deletedDID, bareDID} {
		_, err := manager.PutRecord(ctx, did, testCollection, testRKey(i), testRecord("x"))
		require.NoError(t, err)
	}
	_, err := database.ExecContext(ctx, `
		INSERT INTO bridged_actors (did, ap_actor_id, handle, actor_type, consent_state)
		VALUES ($1, 'https://x/u/a', 'a.x.test', 'person', 'ok'),
		       ($2, 'https://x/u/b', 'b.x.test', 'person', 'deleted')`,
		activeDID, deletedDID)
	require.NoError(t, err)

	dids, err := manager.ListDIDs(ctx)
	require.NoError(t, err)
	assert.Contains(t, dids, activeDID)
	assert.Contains(t, dids, bareDID, "repos without an actor row (e.g. legacy) must list")
	assert.NotContains(t, dids, deletedDID, "tombstoned actors are frozen and must never re-emit")
}
