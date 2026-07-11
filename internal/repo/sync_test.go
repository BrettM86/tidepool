package repo

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	indigorepo "github.com/bluesky-social/indigo/atproto/repo"
	"github.com/bluesky-social/indigo/atproto/repo/mst"

	"github.com/ipfs/go-cid"
	car "github.com/ipld/go-car"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

func TestListEventsAndSeqBounds(t *testing.T) {
	manager, _, _ := testManager(t)
	ctx := t.Context()

	// Empty log: bounds report "nothing retained, nothing ever assigned".
	oldest, newest, err := manager.SeqBounds(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), oldest)
	assert.Equal(t, int64(0), newest)

	const n = 5
	var results []*CommitResult
	for i := 0; i < n; i++ {
		res, err := manager.PutRecord(ctx, testDID, testCollection, testRKey(i),
			testRecord(fmt.Sprintf("post %d", i)))
		require.NoError(t, err)
		results = append(results, res)
	}

	events, err := manager.ListEvents(ctx, 0, 100)
	require.NoError(t, err)
	require.Len(t, events, n)
	for i, ev := range events {
		assert.Equal(t, results[i].Seq, ev.Seq)
		assert.Equal(t, testDID, ev.DID)
		assert.Equal(t, results[i].CommitCID, ev.CommitCID)
		assert.Equal(t, results[i].Rev, ev.Rev)
		assert.NotEmpty(t, ev.CAR)
		assert.False(t, ev.CreatedAt.IsZero())
		require.Len(t, ev.Ops, 1)
		assert.Equal(t, OpActionCreate, ev.Ops[0].Action)
		assert.Equal(t, testCollection+"/"+testRKey(i), ev.Ops[0].Path)
		assert.Equal(t, results[i].RecordCID, ev.Ops[0].CID)
		if i == 0 {
			// Genesis: no previous MST root, no previous rev.
			assert.Nil(t, ev.PrevDataCID, "genesis event must have nil prevData")
			assert.Nil(t, ev.SinceRev, "genesis event must have nil since")
		} else {
			require.NotNil(t, ev.PrevDataCID)
			require.NotNil(t, ev.SinceRev)
			assert.Equal(t, results[i-1].Rev, *ev.SinceRev)
		}
	}

	// Cursor and limit semantics: strictly greater-than, seq order.
	tail, err := manager.ListEvents(ctx, results[2].Seq, 100)
	require.NoError(t, err)
	require.Len(t, tail, 2)
	assert.Equal(t, results[3].Seq, tail[0].Seq)
	assert.Equal(t, results[4].Seq, tail[1].Seq)

	limited, err := manager.ListEvents(ctx, 0, 2)
	require.NoError(t, err)
	require.Len(t, limited, 2)
	assert.Equal(t, results[0].Seq, limited[0].Seq)

	_, err = manager.ListEvents(ctx, 0, 0)
	assert.True(t, errors.IsValidation(err))

	oldest, newest, err = manager.SeqBounds(ctx)
	require.NoError(t, err)
	assert.Equal(t, results[0].Seq, oldest)
	assert.Equal(t, results[n-1].Seq, newest)
}

func TestPruneEvents(t *testing.T) {
	manager, database, _ := testManager(t)
	ctx := t.Context()

	const n = 6
	var seqs []int64
	for i := 0; i < n; i++ {
		res, err := manager.PutRecord(ctx, testDID, testCollection, testRKey(i),
			testRecord(fmt.Sprintf("post %d", i)))
		require.NoError(t, err)
		seqs = append(seqs, res.Seq)
	}

	// Age the first three events past the retention cutoff.
	_, err := database.ExecContext(ctx,
		`UPDATE firehose_events SET created_at = created_at - interval '100 hours' WHERE seq <= $1`, seqs[2])
	require.NoError(t, err)

	deleted, err := manager.PruneEvents(ctx, time.Now().Add(-72*time.Hour))
	require.NoError(t, err)
	assert.Equal(t, int64(3), deleted)

	oldest, newest, err := manager.SeqBounds(ctx)
	require.NoError(t, err)
	assert.Equal(t, seqs[3], oldest)
	assert.Equal(t, seqs[5], newest)

	// Nothing expired: no-op.
	deleted, err = manager.PruneEvents(ctx, time.Now().Add(-72*time.Hour))
	require.NoError(t, err)
	assert.Zero(t, deleted)

	// Prune everything: bounds must still know the last-assigned seq so
	// cursor validation keeps working on an empty backlog.
	deleted, err = manager.PruneEvents(ctx, time.Now().Add(time.Hour))
	require.NoError(t, err)
	assert.Equal(t, int64(3), deleted)
	oldest, newest, err = manager.SeqBounds(ctx)
	require.NoError(t, err)
	assert.Equal(t, seqs[5]+1, oldest)
	assert.Equal(t, seqs[5], newest)

	// The next commit resumes the same seq sequence.
	res, err := manager.PutRecord(ctx, testDID, testCollection, testRKey(n),
		testRecord("post after prune"))
	require.NoError(t, err)
	assert.Greater(t, res.Seq, newest)
}

// readCAR decodes a CARv1 payload into (roots, cid->bytes).
func readCAR(t *testing.T, data []byte) ([]cid.Cid, map[cid.Cid][]byte) {
	t.Helper()
	reader, err := car.NewCarReader(bytes.NewReader(data))
	require.NoError(t, err)
	blocks := make(map[cid.Cid][]byte)
	for {
		blk, err := reader.Next()
		if err != nil {
			break
		}
		blocks[blk.Cid()] = blk.RawData()
	}
	return reader.Header.Roots, blocks
}

func TestGetRecordProof(t *testing.T) {
	manager, _, _ := testManager(t)
	ctx := t.Context()

	// Enough records that the MST has real depth and the proof is a strict
	// subset of the repo.
	const n = 30
	recordCIDs := make(map[string]string, n)
	for i := 0; i < n; i++ {
		res, err := manager.PutRecord(ctx, testDID, testCollection, testRKey(i),
			testRecord(fmt.Sprintf("post %d", i)))
		require.NoError(t, err)
		recordCIDs[testRKey(i)] = res.RecordCID
	}
	headCID, _, err := manager.Head(ctx, testDID)
	require.NoError(t, err)

	fullCAR, err := manager.ExportCAR(ctx, testDID)
	require.NoError(t, err)

	for _, i := range []int{0, 13, n - 1} {
		rkey := testRKey(i)
		proof, err := manager.GetRecordProof(ctx, testDID, testCollection, rkey)
		require.NoError(t, err)
		assert.Less(t, len(proof), len(fullCAR), "proof must be smaller than the full export")

		roots, blocks := readCAR(t, proof)
		require.Len(t, roots, 1)
		assert.Equal(t, headCID, roots[0].String(), "proof root must be the head commit")

		// The record block must be present and content-addressed correctly.
		recCID, err := cid.Parse(recordCIDs[rkey])
		require.NoError(t, err)
		recBytes, ok := blocks[recCID]
		require.True(t, ok, "proof must contain the record block")
		computed, err := cidForBlock(recBytes)
		require.NoError(t, err)
		assert.True(t, computed.Equals(recCID))

		// Walk the MST path using ONLY blocks in the proof: every node from
		// the root to the leaf must be included, terminating at the record.
		commitBytes, ok := blocks[roots[0]]
		require.True(t, ok, "proof must contain the commit block")
		var commit indigorepo.Commit
		require.NoError(t, commit.UnmarshalCBOR(bytes.NewReader(commitBytes)))

		key := []byte(testCollection + "/" + rkey)
		cur := commit.Data
		found := false
		for hop := 0; hop < 64 && !found; hop++ {
			nodeBytes, ok := blocks[cur]
			require.True(t, ok, "proof missing MST node %s on path", cur)
			nd, err := mst.NodeDataFromCBOR(bytes.NewReader(nodeBytes))
			require.NoError(t, err)
			node := nd.Node(&cur)
			for j := range node.Entries {
				e := &node.Entries[j]
				if e.IsValue() && bytes.Equal(e.Key, key) {
					assert.True(t, e.Value.Equals(recCID))
					found = true
					break
				}
			}
			if !found {
				child := coveringChild(&node, key)
				require.NotNil(t, child, "no covering child while record not found")
				cur = *child
			}
		}
		assert.True(t, found, "proof walk must reach the record entry")
	}

	// Missing record and missing repo both satisfy IsNotFound.
	_, err = manager.GetRecordProof(ctx, testDID, testCollection, testRKey(999))
	assert.True(t, errors.IsNotFound(err))
	_, err = manager.GetRecordProof(ctx, testOtherDID, testCollection, testRKey(0))
	assert.True(t, errors.IsNotFound(err))
}

func TestListReposAndGetRepoInfo(t *testing.T) {
	manager, database, _ := testManager(t)
	testutil.Truncate(t, database, "bridged_actors")
	ctx := t.Context()

	dids := []string{
		"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
		"did:plc:bbbbbbbbbbbbbbbbbbbbbbbb",
		"did:plc:cccccccccccccccccccccccc",
	}
	for i, did := range dids {
		_, err := manager.PutRecord(ctx, did, testCollection, testRKey(i),
			testRecord(fmt.Sprintf("repo %d", i)))
		require.NoError(t, err)
	}

	// Tombstone the middle DID's actor: its repo must report inactive (status "deleted").
	actors := store.NewBridgedActors(database)
	_, err := actors.UpsertActor(ctx, store.BridgedActor{
		APActorID:    "https://lemmy.example/u/tombstoned",
		ActorType:    store.ActorTypePerson,
		DID:          dids[1],
		ConsentState: store.ConsentStateDeleted,
	})
	require.NoError(t, err)

	// Pagination: DID order, cursor is the last DID of the previous page.
	page1, err := manager.ListRepos(ctx, "", 2)
	require.NoError(t, err)
	require.Len(t, page1, 2)
	assert.Equal(t, dids[0], page1[0].DID)
	assert.Equal(t, dids[1], page1[1].DID)
	page2, err := manager.ListRepos(ctx, page1[1].DID, 2)
	require.NoError(t, err)
	require.Len(t, page2, 1)
	assert.Equal(t, dids[2], page2[0].DID)

	assert.True(t, page1[0].Active)
	assert.Empty(t, page1[0].Status)
	assert.NotEmpty(t, page1[0].HeadCID)
	assert.NotEmpty(t, page1[0].Rev)
	assert.False(t, page1[1].Active, "tombstoned actor's repo must be inactive")
	assert.Equal(t, "deleted", page1[1].Status)

	info, err := manager.GetRepoInfo(ctx, dids[1])
	require.NoError(t, err)
	assert.False(t, info.Active)
	assert.Equal(t, "deleted", info.Status)

	head, rev, err := manager.Head(ctx, dids[0])
	require.NoError(t, err)
	info, err = manager.GetRepoInfo(ctx, dids[0])
	require.NoError(t, err)
	assert.Equal(t, head, info.HeadCID)
	assert.Equal(t, rev, info.Rev)

	_, err = manager.GetRepoInfo(ctx, "did:plc:doesnotexistatall")
	assert.True(t, errors.IsNotFound(err))

	_, err = manager.ListRepos(ctx, "", 0)
	assert.True(t, errors.IsValidation(err))
}

// TestFirehoseNotify verifies a commit delivers a pg_notify on the firehose
// channel carrying the event's seq — the wake-up contract internal/sync's
// broadcaster relies on. (The full LISTEN→WebSocket path is exercised end to
// end in internal/sync's live-tail tests.)
func TestFirehoseNotify(t *testing.T) {
	manager, _, _ := testManager(t)
	ctx := t.Context()

	dsn := os.Getenv("TIDEPOOL_TEST_DATABASE_URL")
	require.NotEmpty(t, dsn, "testutil.DB already validated this")
	listener := pq.NewListener(dsn, 100*time.Millisecond, time.Second, nil)
	defer listener.Close()
	require.NoError(t, listener.Listen(FirehoseNotifyChannel))

	res, err := manager.PutRecord(ctx, testDID, testCollection, testRKey(0), testRecord("notify me"))
	require.NoError(t, err)

	select {
	case n := <-listener.Notify:
		require.NotNil(t, n)
		assert.Equal(t, FirehoseNotifyChannel, n.Channel)
		assert.Equal(t, fmt.Sprintf("%d", res.Seq), n.Extra)
	case <-time.After(5 * time.Second):
		t.Fatal("no NOTIFY received for committed firehose event")
	}
}
