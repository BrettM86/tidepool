package repo

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	indigorepo "github.com/bluesky-social/indigo/atproto/repo"
	"github.com/bluesky-social/indigo/atproto/repo/mst"

	"github.com/ipfs/go-cid"
	car "github.com/ipld/go-car"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustCID(t *testing.T, data string) cid.Cid {
	t.Helper()
	c, err := cidForBlock([]byte(data))
	require.NoError(t, err)
	return c
}

func TestTreeCache_TakeDetachesAndValidatesHead(t *testing.T) {
	c := newTreeCache(4)
	tree := mst.NewEmptyTree()
	root := mustCID(t, "root")

	c.put("did:plc:a", "head1", root, &tree)
	require.Equal(t, 1, c.len())

	// Wrong head: entry is stale and must be dropped, not returned.
	_, _, ok := c.take("did:plc:a", "head2")
	assert.False(t, ok, "stale head must miss")
	assert.Equal(t, 0, c.len(), "stale entry must be evicted by the failed take")

	// Matching head: returned and detached.
	c.put("did:plc:a", "head1", root, &tree)
	got, gotRoot, ok := c.take("did:plc:a", "head1")
	require.True(t, ok)
	assert.Same(t, &tree, got)
	assert.True(t, root.Equals(gotRoot))
	assert.Equal(t, 0, c.len(), "take must detach the entry")

	// Detached: a second take misses (the caller owns the tree now).
	_, _, ok = c.take("did:plc:a", "head1")
	assert.False(t, ok)
}

func TestTreeCache_LRUEvictionAndDisable(t *testing.T) {
	c := newTreeCache(2)
	tree := mst.NewEmptyTree()
	root := mustCID(t, "root")

	c.put("did:plc:a", "h", root, &tree)
	c.put("did:plc:b", "h", root, &tree)
	// Touch a so b becomes least recently used, then overflow.
	_, _, ok := c.take("did:plc:a", "h")
	require.True(t, ok)
	c.put("did:plc:a", "h", root, &tree)
	c.put("did:plc:c", "h", root, &tree)
	assert.Equal(t, 2, c.len())
	_, _, ok = c.take("did:plc:b", "h")
	assert.False(t, ok, "least-recently-used entry must be evicted")

	// A non-positive size disables caching entirely.
	off := newTreeCache(0)
	off.put("did:plc:a", "h", root, &tree)
	assert.Equal(t, 0, off.len())
	_, _, ok = off.take("did:plc:a", "h")
	assert.False(t, ok)

	// nil receiver (defensive) is inert.
	var nilCache *mstTreeCache
	assert.Equal(t, 0, nilCache.len())
	_, _, ok = nilCache.take("did:plc:a", "h")
	assert.False(t, ok)
}

// TestPutRecord_CachedTreeMatchesReload pins the cache coherence model: after
// a run of commits served from the cached tree, the cached tree's root must
// equal what a cold load of the head from postgres produces, and the exported
// repo must still verify end to end.
func TestPutRecord_CachedTreeMatchesReload(t *testing.T) {
	manager, database, _ := testManager(t)
	ctx := t.Context()

	for i := 0; i < 10; i++ {
		_, err := manager.PutRecord(ctx, testDID, testCollection, testRKey(i), testRecord(fmt.Sprintf("post %d", i)))
		require.NoError(t, err)
	}
	// Update and delete exercise the non-create paths through the cache.
	_, err := manager.PutRecord(ctx, testDID, testCollection, testRKey(3), testRecord("post 3 edited"))
	require.NoError(t, err)
	_, err = manager.DeleteRecord(ctx, testDID, testCollection, testRKey(4))
	require.NoError(t, err)

	require.Equal(t, 1, manager.treeCache.len(), "one DID must occupy one cache slot")

	head, _, err := manager.Head(ctx, testDID)
	require.NoError(t, err)
	cached, cachedRoot, ok := manager.treeCache.take(testDID, head)
	require.True(t, ok, "cache must hold the tree for the current head")

	tx, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	fresh, freshRoot, err := loadTree(ctx, tx, testDID, head)
	require.NoError(t, err)

	cachedCID, err := cached.RootCID()
	require.NoError(t, err)
	freshCID, err := fresh.RootCID()
	require.NoError(t, err)
	assert.Equal(t, freshCID.String(), cachedCID.String(),
		"cached tree must encode exactly the durable head's MST")
	assert.Equal(t, freshRoot.String(), cachedRoot.String())

	// The repo built through cache-hit commits must round-trip through indigo.
	carBytes, err := manager.ExportCAR(ctx, testDID)
	require.NoError(t, err)
	commit, loaded, err := indigorepo.LoadRepoFromCAR(ctx, bytes.NewReader(carBytes))
	require.NoError(t, err)
	require.NoError(t, commit.VerifyStructure())
	root, err := loaded.MST.RootCID()
	require.NoError(t, err)
	assert.Equal(t, commit.Data.String(), root.String())
}

// TestPutRecord_CacheStaleAcrossManagers simulates a second process advancing
// the head: manager A's cached tree must be recognized as stale (head
// mismatch under the commit locks), dropped, and reloaded — never used.
func TestPutRecord_CacheStaleAcrossManagers(t *testing.T) {
	managerA, database, _, keys := testManagerWithKeys(t)
	ctx := t.Context()

	managerB, err := NewManager(database, keys, nil)
	require.NoError(t, err)

	_, err = managerA.PutRecord(ctx, testDID, testCollection, testRKey(0), testRecord("from A"))
	require.NoError(t, err)
	// Guard the premise: A's commit must actually have populated A's cache,
	// or the staleness scenario below silently tests nothing.
	require.Equal(t, 1, managerA.treeCache.len(), "manager A's commit must cache its tree")
	_, err = managerB.PutRecord(ctx, testDID, testCollection, testRKey(1), testRecord("from B"))
	require.NoError(t, err)
	// A's cached tree is now behind B's commit; this commit must detect the
	// head mismatch and reload.
	_, err = managerA.PutRecord(ctx, testDID, testCollection, testRKey(2), testRecord("from A again"))
	require.NoError(t, err)

	for i, want := range []string{"from A", "from B", "from A again"} {
		rec, _, err := managerA.GetRecord(ctx, testDID, testCollection, testRKey(i))
		require.NoError(t, err)
		assert.Equal(t, want, rec["text"])
	}
	carBytes, err := managerA.ExportCAR(ctx, testDID)
	require.NoError(t, err)
	_, _, err = indigorepo.LoadRepoFromCAR(ctx, bytes.NewReader(carBytes))
	require.NoError(t, err, "repo written by interleaved managers must stay loadable")
}

// TestPutRecord_NoOpRePutKeepsCacheValid pins the NoOp re-cache path: an
// identical re-put leaves the tree untouched (indigo returns the same value
// without dirtying), so the cache stays valid for the next real commit.
func TestPutRecord_NoOpRePutKeepsCacheValid(t *testing.T) {
	manager, _, _ := testManager(t)
	ctx := t.Context()

	first, err := manager.PutRecord(ctx, testDID, testCollection, testRKey(0), testRecord("v1"))
	require.NoError(t, err)
	noop, err := manager.PutRecord(ctx, testDID, testCollection, testRKey(0), testRecord("v1"))
	require.NoError(t, err)
	require.True(t, noop.NoOp)
	require.Equal(t, first.CommitCID, noop.CommitCID)
	require.Equal(t, 1, manager.treeCache.len(), "NoOp must re-cache the unmodified tree")

	// The next real commit rides the re-cached tree.
	second, err := manager.PutRecord(ctx, testDID, testCollection, testRKey(1), testRecord("v2"))
	require.NoError(t, err)
	require.False(t, second.NoOp)

	carBytes, err := manager.ExportCAR(ctx, testDID)
	require.NoError(t, err)
	_, _, err = indigorepo.LoadRepoFromCAR(ctx, bytes.NewReader(carBytes))
	require.NoError(t, err)
}

// TestPutRecord_FailedCommitDropsCacheEntry: take() detaches, and a commit
// that errors must NOT re-insert the tree — the next commit reloads from
// postgres and still succeeds.
func TestPutRecord_FailedCommitDropsCacheEntry(t *testing.T) {
	manager, _, _ := testManager(t)
	ctx := t.Context()

	_, err := manager.PutRecord(ctx, testDID, testCollection, testRKey(0), testRecord("v1"))
	require.NoError(t, err)
	require.Equal(t, 1, manager.treeCache.len())

	// Deleting a missing record fails AFTER the tree was taken from the
	// cache (the miss is only detectable post-ApplyOp).
	_, err = manager.DeleteRecord(ctx, testDID, testCollection, testRKey(9))
	require.Error(t, err)
	assert.Equal(t, 0, manager.treeCache.len(), "failed commit must not re-cache the tree")

	// Reload path still works and repopulates the cache.
	_, err = manager.PutRecord(ctx, testDID, testCollection, testRKey(1), testRecord("v2"))
	require.NoError(t, err)
	assert.Equal(t, 1, manager.treeCache.len())
	rec, _, err := manager.GetRecord(ctx, testDID, testCollection, testRKey(1))
	require.NoError(t, err)
	assert.Equal(t, "v2", rec["text"])
}

// TestPutRecord_CARSlicesIdenticalWithAndWithoutCache pins that the cache is
// invisible on the firehose: the same logical writes produce CAR slices with
// the same MST-diff and record blocks — in the same order — whether the tree
// came from the cache or a cold load. (indigo's WriteDiffBlocks clears dirty
// flags as it writes, so a reused tree emits only the new commit's diff —
// this is the test that breaks if that upstream behavior ever changes.) The
// sequence deliberately includes an update of an earlier rkey and a delete of
// another: the riskiest ops for a reused tree, whose dirty flags must mark
// exactly the changed path and nothing else.
func TestPutRecord_CARSlicesIdenticalWithAndWithoutCache(t *testing.T) {
	manager, database, _, keys := testManagerWithKeys(t)
	ctx := t.Context()

	uncachedManager, err := NewManager(database, keys, nil, WithTreeCacheSize(0))
	require.NoError(t, err)

	// Same records, two DIDs: MST node and record CIDs depend only on paths
	// and record bytes, so everything except the signed commit block must
	// come out identical.
	write := func(m *Manager, did string) [][]string {
		sliceCIDs := func(res *CommitResult) []string {
			var carBytes []byte
			require.NoError(t, database.QueryRowContext(ctx,
				`SELECT car FROM firehose_events WHERE seq = $1`, res.Seq).Scan(&carBytes))
			reader, err := car.NewCarReader(bytes.NewReader(carBytes))
			require.NoError(t, err)
			var cids []string
			for {
				blk, err := reader.Next()
				if err == io.EOF {
					break
				}
				require.NoError(t, err, "CAR slice read must end at EOF, not a real error")
				if blk.Cid().String() == res.CommitCID {
					continue // the commit block legitimately differs per DID
				}
				cids = append(cids, blk.Cid().String())
			}
			return cids
		}

		var perCommit [][]string
		for i := 0; i < 8; i++ {
			res, err := m.PutRecord(ctx, did, testCollection, testRKey(i), testRecord(fmt.Sprintf("post %d", i)))
			require.NoError(t, err)
			perCommit = append(perCommit, sliceCIDs(res))
		}
		// Update an earlier rkey, then delete another — the non-create paths
		// through a reused tree.
		res, err := m.PutRecord(ctx, did, testCollection, testRKey(3), testRecord("post 3 edited"))
		require.NoError(t, err)
		perCommit = append(perCommit, sliceCIDs(res))
		res, err = m.DeleteRecord(ctx, did, testCollection, testRKey(4))
		require.NoError(t, err)
		perCommit = append(perCommit, sliceCIDs(res))
		return perCommit
	}

	cached := write(manager, testDID)
	uncached := write(uncachedManager, testOtherDID)
	require.Len(t, cached, len(uncached))
	for i := range cached {
		// Ordered equality: both paths write the diff deterministically, so
		// the CAR slices must match block-for-block in order, pinning the
		// byte layout as closely as the differing signed commit blocks allow.
		assert.Equal(t, uncached[i], cached[i],
			"commit %d: cached-path CAR slice must carry exactly the cold-load path's blocks, in order", i)
	}
}
