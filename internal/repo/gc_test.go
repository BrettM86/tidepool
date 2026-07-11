package repo

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"testing"
	"time"

	indigorepo "github.com/bluesky-social/indigo/atproto/repo"

	"github.com/ipfs/go-cid"
	car "github.com/ipld/go-car"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

func countBlocks(t *testing.T, database *sql.DB, did string) int {
	t.Helper()
	var n int
	require.NoError(t, database.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM blocks WHERE did = $1`, did).Scan(&n))
	return n
}

func blockExists(t *testing.T, database *sql.DB, did, cid string) bool {
	t.Helper()
	var n int
	require.NoError(t, database.QueryRowContext(t.Context(),
		`SELECT COUNT(*) FROM blocks WHERE did = $1 AND cid = $2`, did, cid).Scan(&n))
	return n == 1
}

// churn builds a repo with garbage: creates, updates, and a delete, so the
// blocks table holds superseded record versions, dead MST nodes, and old
// commit blocks alongside the live tree.
func churn(t *testing.T, manager *Manager, did string) (liveRecordCID, deadRecordCID string) {
	t.Helper()
	ctx := t.Context()
	for i := 0; i < 8; i++ {
		_, err := manager.PutRecord(ctx, did, testCollection, testRKey(i), testRecord(fmt.Sprintf("post %d", i)))
		require.NoError(t, err)
	}
	v1, err := manager.PutRecord(ctx, did, testCollection, testRKey(20), testRecord("v1"))
	require.NoError(t, err)
	v2, err := manager.PutRecord(ctx, did, testCollection, testRKey(20), testRecord("v2"))
	require.NoError(t, err)
	_, err = manager.DeleteRecord(ctx, did, testCollection, testRKey(7))
	require.NoError(t, err)
	return v2.RecordCID, v1.RecordCID
}

// verifyRepoIntact asserts every reader still works after a sweep: GetRecord,
// GetRecordProof, and a full export that indigo loads and verifies.
func verifyRepoIntact(t *testing.T, manager *Manager, did string, liveRKeys []int) {
	t.Helper()
	ctx := t.Context()
	for _, i := range liveRKeys {
		_, _, err := manager.GetRecord(ctx, did, testCollection, testRKey(i))
		require.NoError(t, err, "GetRecord %d after GC", i)
		_, err = manager.GetRecordProof(ctx, did, testCollection, testRKey(i))
		require.NoError(t, err, "GetRecordProof %d after GC", i)
	}
	carBytes, err := manager.ExportCAR(ctx, did)
	require.NoError(t, err)
	commit, loaded, err := indigorepo.LoadRepoFromCAR(ctx, bytes.NewReader(carBytes))
	require.NoError(t, err, "repo must load from CAR after GC")
	require.NoError(t, commit.VerifyStructure())
	root, err := loaded.MST.RootCID()
	require.NoError(t, err)
	assert.Equal(t, commit.Data.String(), root.String())
}

func TestGCBlocks_ReclaimsUnreachableOnly(t *testing.T) {
	manager, database, _ := testManager(t)
	ctx := t.Context()

	liveCID, deadCID := churn(t, manager, testDID)
	before := countBlocks(t, database, testDID)
	require.True(t, blockExists(t, database, testDID, deadCID), "superseded record block present before GC")

	// Everything is older than a future cutoff, so only reachability
	// protects blocks — the sharpest version of rule (a).
	deleted, err := manager.GCBlocks(ctx, time.Now().Add(time.Minute))
	require.NoError(t, err)
	assert.Positive(t, deleted, "churn must have produced unreachable blocks")
	assert.Equal(t, before-int(deleted), countBlocks(t, database, testDID))

	assert.False(t, blockExists(t, database, testDID, deadCID), "superseded record version must be reclaimed")
	assert.True(t, blockExists(t, database, testDID, liveCID), "live record must survive")
	head, _, err := manager.Head(ctx, testDID)
	require.NoError(t, err)
	assert.True(t, blockExists(t, database, testDID, head), "head commit must survive")

	verifyRepoIntact(t, manager, testDID, []int{0, 1, 2, 3, 4, 5, 6, 20})
	_, _, err = manager.GetRecord(ctx, testDID, testCollection, testRKey(7))
	require.Error(t, err)
	assert.True(t, errors.IsNotFound(err), "deleted record stays deleted")

	// A second sweep over a clean table is a no-op.
	deleted, err = manager.GCBlocks(ctx, time.Now().Add(time.Minute))
	require.NoError(t, err)
	assert.Zero(t, deleted)

	// And the repo keeps committing normally afterwards.
	_, err = manager.PutRecord(ctx, testDID, testCollection, testRKey(30), testRecord("post-GC write"))
	require.NoError(t, err)
}

func TestGCBlocks_RetentionFloorProtectsEverything(t *testing.T) {
	manager, database, _ := testManager(t)

	churn(t, manager, testDID)
	before := countBlocks(t, database, testDID)

	// Cutoff in the past: every block is younger, rule (b) keeps them all
	// no matter how unreachable.
	deleted, err := manager.GCBlocks(t.Context(), time.Now().Add(-time.Hour))
	require.NoError(t, err)
	assert.Zero(t, deleted, "retention floor must protect young blocks")
	assert.Equal(t, before, countBlocks(t, database, testDID))
}

func TestGCBlocks_RepoIsolation(t *testing.T) {
	manager, database, _ := testManager(t)
	ctx := t.Context()

	churn(t, manager, testDID)
	// The other repo has no garbage: one clean commit.
	_, err := manager.PutRecord(ctx, testOtherDID, testCollection, testRKey(0), testRecord("clean"))
	require.NoError(t, err)
	otherBefore := countBlocks(t, database, testOtherDID)

	deleted, err := manager.GCBlocks(ctx, time.Now().Add(time.Minute))
	require.NoError(t, err)
	assert.Positive(t, deleted)
	assert.Equal(t, otherBefore, countBlocks(t, database, testOtherDID),
		"a repo with no unreachable blocks must be untouched")
	verifyRepoIntact(t, manager, testOtherDID, []int{0})
}

// TestGCBlocks_CommitRefreshesCreatedAt pins the commit path's ON CONFLICT
// created_at refresh — the mechanism that closes the compute→delete race in
// the invariant's rule (b). A block whose CID re-enters the live tree via a
// new commit must read as freshly written, so a sweep whose reachability
// snapshot predated that commit still cannot delete it.
func TestGCBlocks_CommitRefreshesCreatedAt(t *testing.T) {
	manager, database, _ := testManager(t)
	ctx := t.Context()

	v1, err := manager.PutRecord(ctx, testDID, testCollection, testRKey(0), testRecord("v1"))
	require.NoError(t, err)
	v2, err := manager.PutRecord(ctx, testDID, testCollection, testRKey(0), testRecord("v2"))
	require.NoError(t, err)
	require.NotEqual(t, v1.RecordCID, v2.RecordCID)

	// Age every block far past any cutoff.
	_, err = database.ExecContext(ctx,
		`UPDATE blocks SET created_at = TIMESTAMPTZ '2000-01-01' WHERE did = $1`, testDID)
	require.NoError(t, err)

	// Reverting to v1's exact content re-writes v1's record block (same CID,
	// identical bytes) inside the new commit — its created_at must refresh.
	v3, err := manager.PutRecord(ctx, testDID, testCollection, testRKey(0), testRecord("v1"))
	require.NoError(t, err)
	require.Equal(t, v1.RecordCID, v3.RecordCID, "reverted content must reproduce the CID")

	var ageSeconds float64
	require.NoError(t, database.QueryRowContext(ctx,
		`SELECT EXTRACT(EPOCH FROM now() - created_at) FROM blocks WHERE did = $1 AND cid = $2`,
		testDID, v1.RecordCID).Scan(&ageSeconds))
	assert.Less(t, ageSeconds, 60.0, "re-written block must read as freshly created")

	// A sweep with a recent cutoff now reclaims v2's superseded block (old
	// AND unreachable) but must keep the re-written v1 block despite the
	// backdating (rule (b) via the refresh).
	deleted, err := manager.GCBlocks(ctx, time.Now().Add(-time.Second))
	require.NoError(t, err)
	assert.Positive(t, deleted)
	assert.False(t, blockExists(t, database, testDID, v2.RecordCID))
	assert.True(t, blockExists(t, database, testDID, v1.RecordCID))

	rec, _, err := manager.GetRecord(ctx, testDID, testCollection, testRKey(0))
	require.NoError(t, err)
	assert.Equal(t, "v1", rec["text"])
	verifyRepoIntact(t, manager, testDID, []int{0})
}

// TestGCBlocks_DeleteRecheckSavesReintroducedBlock pins the DELETE-time
// `AND created_at < cutoff` re-check — rule (b) of the invariant, and the
// only thing standing between a stale victim snapshot and a live block. It
// drives the exact race interleaving: compute victims in a snapshot,
// re-introduce a victim via a real commit (same CID re-enters the live tree;
// the commit's ON CONFLICT refreshes created_at), then run the delete phase
// with the STALE victim list. The re-introduced block must survive. Delete
// the clause from deleteUnreachable's DELETE and this test fails.
func TestGCBlocks_DeleteRecheckSavesReintroducedBlock(t *testing.T) {
	manager, database, _ := testManager(t)
	ctx := t.Context()

	v1, err := manager.PutRecord(ctx, testDID, testCollection, testRKey(0), testRecord("v1"))
	require.NoError(t, err)
	_, err = manager.PutRecord(ctx, testDID, testCollection, testRKey(0), testRecord("v2"))
	require.NoError(t, err)
	// Backdate everything so rule (b) alone cannot save the victims.
	_, err = database.ExecContext(ctx,
		`UPDATE blocks SET created_at = TIMESTAMPTZ '2000-01-01' WHERE did = $1`, testDID)
	require.NoError(t, err)

	// Phase 1 (snapshot): v1's superseded record block is a legitimate victim.
	cutoff := time.Now()
	victims, err := manager.unreachableBlocks(ctx, testDID, cutoff)
	require.NoError(t, err)
	require.Contains(t, victims, v1.RecordCID, "superseded block must be a victim at snapshot time")

	// The racing commit: reverting to v1's content re-writes v1's record
	// block (same CID) into the live tree and refreshes its created_at.
	v3, err := manager.PutRecord(ctx, testDID, testCollection, testRKey(0), testRecord("v1"))
	require.NoError(t, err)
	require.Equal(t, v1.RecordCID, v3.RecordCID, "reverted content must reproduce the CID")

	// Phase 2 (delete) runs with the stale victim list: the re-introduced
	// block must survive on the re-check while genuinely dead victims go.
	deleted, err := manager.deleteUnreachable(ctx, testDID, victims, cutoff)
	require.NoError(t, err)
	assert.Positive(t, deleted, "stale-but-still-dead victims must still be reclaimed")
	assert.True(t, blockExists(t, database, testDID, v1.RecordCID),
		"re-introduced block must survive the stale delete phase")
	verifyRepoIntact(t, manager, testDID, []int{0})

	// Control: the identical interleaving WITHOUT the re-introducing commit
	// deletes the block — proving the survival above is the re-check at
	// work, not the victim simply never being deletable.
	c1, err := manager.PutRecord(ctx, testOtherDID, testCollection, testRKey(0), testRecord("v1"))
	require.NoError(t, err)
	_, err = manager.PutRecord(ctx, testOtherDID, testCollection, testRKey(0), testRecord("v2"))
	require.NoError(t, err)
	_, err = database.ExecContext(ctx,
		`UPDATE blocks SET created_at = TIMESTAMPTZ '2000-01-01' WHERE did = $1`, testOtherDID)
	require.NoError(t, err)
	controlCutoff := time.Now()
	controlVictims, err := manager.unreachableBlocks(ctx, testOtherDID, controlCutoff)
	require.NoError(t, err)
	require.Contains(t, controlVictims, c1.RecordCID)
	_, err = manager.deleteUnreachable(ctx, testOtherDID, controlVictims, controlCutoff)
	require.NoError(t, err)
	assert.False(t, blockExists(t, database, testOtherDID, c1.RecordCID),
		"without re-introduction the victim must be deleted")
}

// TestGCBlocks_BigRepoCrossesWalkBatchSize pins the reachable-set walk across
// the walkBatchSize=256 batching boundary: with ~300 live records, one MST
// level references >256 record blocks, so forEachBlock must split the fetch
// into multiple batches. An off-by-one that drops a batch element would make
// a reachable block invisible to the walk — GC would delete it — and the
// post-sweep full-integrity verification (export → indigo load → verify, plus
// per-record reads) catches exactly that. The same walk feeds getRepo
// exports, so this is also the scale test for that surface.
func TestGCBlocks_BigRepoCrossesWalkBatchSize(t *testing.T) {
	manager, database, _ := testManager(t)
	ctx := t.Context()

	const n = 300 // > walkBatchSize=256 record blocks on the MST's bottom level
	rkeys := make([]int, 0, n)
	for i := 0; i < n; i++ {
		_, err := manager.PutRecord(ctx, testDID, testCollection, testRKey(i), testRecord(fmt.Sprintf("post %d", i)))
		require.NoError(t, err)
		rkeys = append(rkeys, i)
	}
	before := countBlocks(t, database, testDID)
	require.Greater(t, before, walkBatchSize,
		"fixture must be large enough to force multi-batch walks")

	// Future cutoff: reachability alone protects blocks, at scale.
	deleted, err := manager.GCBlocks(ctx, time.Now().Add(time.Minute))
	require.NoError(t, err)
	assert.Positive(t, deleted, "300 sequential commits must leave unreachable garbage")
	assert.Equal(t, before-int(deleted), countBlocks(t, database, testDID))

	verifyRepoIntact(t, manager, testDID, rkeys)
}

// TestGCBlocks_SnapshotReaderSurvivesConcurrentSweep pins the isolation-level
// half of the invariant's reader audit: a REPEATABLE READ snapshot opened
// before a sweep keeps seeing the OLD head's entire block set even after GC
// (running against a newer head) deletes those blocks. This is the MVCC
// argument the file header makes for GetRecord/GetRecordProof/ExportCARTo —
// it fails if the reader transaction is demoted to READ COMMITTED, where each
// statement would see the committed deletes mid-walk.
func TestGCBlocks_SnapshotReaderSurvivesConcurrentSweep(t *testing.T) {
	manager, database, _ := testManager(t)
	ctx := t.Context()

	_, err := manager.PutRecord(ctx, testDID, testCollection, testRKey(0), testRecord("v1"))
	require.NoError(t, err)
	oldHead, _, err := manager.Head(ctx, testDID)
	require.NoError(t, err)

	// The reader: a REPEATABLE READ read-only tx, exactly what the audited
	// readers open. Its first query pins the snapshot at the old head.
	tx, err := database.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	snapState, err := readRepoState(ctx, tx, testDID, false)
	require.NoError(t, err)
	require.Equal(t, oldHead, snapState.headCID, "snapshot must be pinned at the old head")

	// Advance the repo past the snapshot: the old head's commit block, MST
	// root, and superseded record versions all become unreachable garbage...
	for _, text := range []string{"v2", "v3"} {
		_, err := manager.PutRecord(ctx, testDID, testCollection, testRKey(0), testRecord(text))
		require.NoError(t, err)
	}
	// ...which a sweep with a future cutoff reclaims, including the old head.
	deleted, err := manager.GCBlocks(ctx, time.Now().Add(time.Minute))
	require.NoError(t, err)
	require.Positive(t, deleted)
	require.False(t, blockExists(t, database, testDID, oldHead),
		"the old head commit block must actually be swept")

	// Inside the still-open snapshot, the old head must remain fully
	// walkable: commit block, every MST node, every record block. Under
	// READ COMMITTED this walk errors on the first swept block.
	oldHeadCID, err := cid.Parse(oldHead)
	require.NoError(t, err)
	src := &txBlockSource{tx: tx, did: testDID}
	headBlk, err := src.Get(ctx, oldHeadCID)
	require.NoError(t, err, "snapshot must still see the swept head commit block")
	var commit indigorepo.Commit
	require.NoError(t, commit.UnmarshalCBOR(bytes.NewReader(headBlk.RawData())))
	seen := map[string]struct{}{oldHead: {}}
	fetched := 0
	err = walkReachable(ctx, src, commit.Data, seen,
		func(cid.Cid, []byte) error { fetched++; return nil },
		func(records []cid.Cid) error {
			return forEachBlock(ctx, src, records, func(cid.Cid, []byte) error {
				fetched++
				return nil
			})
		})
	require.NoError(t, err, "old head's full block set must stay visible to the snapshot")
	assert.Equal(t, len(seen), fetched+1, "every reachable CID must have been fetched (head via Get)")
}

// TestGCBlocks_SkipsRepolessDIDs: blocks with no repo_state row (impossible
// by construction, conceivable after manual surgery) are never swept —
// nothing without a head is ever considered.
func TestGCBlocks_SkipsRepolessDIDs(t *testing.T) {
	manager, database, _ := testManager(t)
	ctx := t.Context()

	_, err := database.ExecContext(ctx, `
		INSERT INTO blocks (did, cid, bytes, created_at)
		VALUES ($1, $2, $3, TIMESTAMPTZ '2000-01-01')`,
		testDID, "bafyreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku", []byte{0x01})
	require.NoError(t, err)

	deleted, err := manager.GCBlocks(ctx, time.Now())
	require.NoError(t, err)
	assert.Zero(t, deleted)
	assert.Equal(t, 1, countBlocks(t, database, testDID))
}

// TestExportCAR_OmitsUnreachableBlocks pins the reachable-set export: a
// superseded record version stays in `blocks` (until GC) but must not ride
// getRepo responses.
func TestExportCAR_OmitsUnreachableBlocks(t *testing.T) {
	manager, database, _ := testManager(t)
	ctx := t.Context()

	v1, err := manager.PutRecord(ctx, testDID, testCollection, testRKey(0), testRecord("v1"))
	require.NoError(t, err)
	v2, err := manager.PutRecord(ctx, testDID, testCollection, testRKey(0), testRecord("v2"))
	require.NoError(t, err)
	require.True(t, blockExists(t, database, testDID, v1.RecordCID), "superseded block still stored")

	carBytes, err := manager.ExportCAR(ctx, testDID)
	require.NoError(t, err)
	reader, err := car.NewCarReader(bytes.NewReader(carBytes))
	require.NoError(t, err)
	exported := map[string]bool{}
	for {
		blk, err := reader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err, "CAR read must end at EOF, not a real error")
		exported[blk.Cid().String()] = true
	}
	assert.True(t, exported[v2.RecordCID], "live record block must be exported")
	assert.False(t, exported[v1.RecordCID], "superseded record block must not be exported")
	assert.False(t, exported[v1.CommitCID], "old commit block must not be exported")
	assert.True(t, exported[v2.CommitCID], "head commit block must be exported")

	_, _, err = indigorepo.LoadRepoFromCAR(ctx, bytes.NewReader(carBytes))
	require.NoError(t, err)
}

// TestExportCARTo_MissingRepoWritesNothing pins the streaming contract the
// sync handler relies on: every failable read happens before the first byte,
// so a 404/500 can still be sent.
func TestExportCARTo_MissingRepoWritesNothing(t *testing.T) {
	manager, _, _ := testManager(t)

	var buf bytes.Buffer
	err := manager.ExportCARTo(t.Context(), testDID, &buf)
	require.Error(t, err)
	assert.True(t, errors.IsNotFound(err))
	assert.Zero(t, buf.Len(), "no bytes may reach the writer before the head is validated")
}
