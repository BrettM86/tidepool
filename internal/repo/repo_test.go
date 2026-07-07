package repo

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	indigorepo "github.com/bluesky-social/indigo/atproto/repo"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/syntax"
	car "github.com/ipld/go-car"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/testutil"
)

const (
	testDID        = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	testOtherDID   = "did:plc:44ybard66vv44zksje25o7dz"
	testCollection = "social.coves.community.post"
)

// staticKeys signs every DID with one fixed key — the repo layer only needs
// "give me the signing key for this DID", so tests stay decoupled from the
// identity package (which has its own ActorKeys tests). It records the
// KeyUse of every request so tests can assert the consent-gate routing.
type staticKeys struct {
	key *atcrypto.PrivateKeyK256

	mu   sync.Mutex
	uses []KeyUse
}

func (s *staticKeys) SigningKey(_ context.Context, _ string, use KeyUse) (atcrypto.PrivateKey, error) {
	s.mu.Lock()
	s.uses = append(s.uses, use)
	s.mu.Unlock()
	return s.key, nil
}

func (s *staticKeys) recordedUses() []KeyUse {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]KeyUse(nil), s.uses...)
}

func testManager(t *testing.T) (*Manager, *sql.DB, *atcrypto.PrivateKeyK256) {
	t.Helper()
	manager, database, key, _ := testManagerWithKeys(t)
	return manager, database, key
}

func testManagerWithKeys(t *testing.T) (*Manager, *sql.DB, *atcrypto.PrivateKeyK256, *staticKeys) {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database, "blocks", "repo_state", "firehose_events")
	key, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	keys := &staticKeys{key: key}
	manager, err := NewManager(database, keys, nil)
	require.NoError(t, err)
	return manager, database, key, keys
}

func testRecord(text string) map[string]any {
	return map[string]any{
		"$type":     testCollection,
		"text":      text,
		"createdAt": "2026-05-01T12:00:00Z",
	}
}

func testRKey(i int) string {
	published := time.Date(2026, 5, 1, 12, 0, i, 0, time.UTC)
	tid, err := DeterministicTID(published, fmt.Sprintf("https://lemmy.world/post/%d", i))
	if err != nil {
		panic(err)
	}
	return tid.String()
}

func TestPutRecord_RoundTripThroughIndigoCAR(t *testing.T) {
	manager, _, key := testManager(t)
	ctx := t.Context()

	const n = 25
	wantCIDs := make(map[string]string, n) // path -> record CID
	var lastSeq int64
	for i := 0; i < n; i++ {
		rkey := testRKey(i)
		res, err := manager.PutRecord(ctx, testDID, testCollection, rkey, testRecord(fmt.Sprintf("post %d", i)))
		require.NoError(t, err)
		require.NotEmpty(t, res.RecordCID)
		require.NotEmpty(t, res.CommitCID)
		require.NotEmpty(t, res.Rev)
		require.False(t, res.NoOp)
		require.Greater(t, res.Seq, lastSeq, "seq must strictly increase per commit")
		lastSeq = res.Seq
		wantCIDs[testCollection+"/"+rkey] = res.RecordCID
	}

	// Read one back through the manager.
	record, gotCID, err := manager.GetRecord(ctx, testDID, testCollection, testRKey(7))
	require.NoError(t, err)
	assert.Equal(t, "post 7", record["text"])
	assert.Equal(t, wantCIDs[testCollection+"/"+testRKey(7)], gotCID)

	// Export the whole repo as CAR and reload it with indigo.
	carBytes, err := manager.ExportCAR(ctx, testDID)
	require.NoError(t, err)
	commit, loaded, err := indigorepo.LoadRepoFromCAR(ctx, bytes.NewReader(carBytes))
	require.NoError(t, err, "indigo must accept our CAR export")

	assert.Equal(t, testDID, commit.DID)
	require.NoError(t, commit.VerifyStructure())

	// The signature must verify with the signing key's public half.
	pub, err := key.PublicKey()
	require.NoError(t, err)
	require.NoError(t, commit.VerifySignature(pub), "commit signature must verify with the minted key")

	// The MST root recomputed from the loaded tree must match the signed
	// commit's data CID.
	root, err := loaded.MST.RootCID()
	require.NoError(t, err)
	assert.Equal(t, commit.Data.String(), root.String(), "recomputed MST root must match the signed commit")

	// Every record must be present with the CID we returned at write time.
	for path, want := range wantCIDs {
		nsid, rkey, err := syntax.ParseRepoPath(path)
		require.NoError(t, err)
		_, c, err := loaded.GetRecordBytes(ctx, nsid, rkey)
		require.NoError(t, err, "record %s must be in the reloaded repo", path)
		assert.Equal(t, want, c.String())
	}

	// Head bookkeeping matches the export.
	headCID, headRev, err := manager.Head(ctx, testDID)
	require.NoError(t, err)
	assert.Equal(t, commit.Rev, headRev)
	assert.NotEmpty(t, headCID)
}

func TestPutRecord_UpdateAndDelete(t *testing.T) {
	manager, _, _ := testManager(t)
	ctx := t.Context()
	rkey := testRKey(1)

	first, err := manager.PutRecord(ctx, testDID, testCollection, rkey, testRecord("v1"))
	require.NoError(t, err)

	second, err := manager.PutRecord(ctx, testDID, testCollection, rkey, testRecord("v2"))
	require.NoError(t, err)
	assert.NotEqual(t, first.RecordCID, second.RecordCID)
	assert.NotEqual(t, first.CommitCID, second.CommitCID)
	assert.Greater(t, second.Rev, first.Rev, "revs must be monotonic")
	assert.Greater(t, second.Seq, first.Seq, "seq must be monotonic")

	record, gotCID, err := manager.GetRecord(ctx, testDID, testCollection, rkey)
	require.NoError(t, err)
	assert.Equal(t, "v2", record["text"])
	assert.Equal(t, second.RecordCID, gotCID)

	deleted, err := manager.DeleteRecord(ctx, testDID, testCollection, rkey)
	require.NoError(t, err)
	assert.Greater(t, deleted.Rev, second.Rev)
	assert.Empty(t, deleted.RecordCID, "deletes carry no record CID")
	assert.False(t, deleted.NoOp)

	_, _, err = manager.GetRecord(ctx, testDID, testCollection, rkey)
	assert.True(t, errors.IsNotFound(err), "deleted record must read as not found")

	// Deleting again: gone.
	_, err = manager.DeleteRecord(ctx, testDID, testCollection, rkey)
	assert.True(t, errors.IsNotFound(err))
}

func TestPutRecord_IdenticalRePutIsNoOp(t *testing.T) {
	manager, database, _ := testManager(t)
	ctx := t.Context()
	rkey := testRKey(2)

	first, err := manager.PutRecord(ctx, testDID, testCollection, rkey, testRecord("same"))
	require.NoError(t, err)
	require.False(t, first.NoOp)

	second, err := manager.PutRecord(ctx, testDID, testCollection, rkey, testRecord("same"))
	require.NoError(t, err)
	assert.Equal(t, first.RecordCID, second.RecordCID, "identical content must produce the identical CID")
	assert.Equal(t, first.Rev, second.Rev, "no new commit for an identical re-put")
	assert.Equal(t, first.CommitCID, second.CommitCID, "no-op re-put reports the existing head")
	assert.True(t, second.NoOp, "identical re-put must be marked NoOp")
	assert.Zero(t, second.Seq, "no firehose event, no seq")

	var events int
	require.NoError(t, database.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM firehose_events WHERE did = $1`, testDID).Scan(&events))
	assert.Equal(t, 1, events, "idempotent re-put must not emit a firehose event")
}

func TestNewManager_ValidatesInputs(t *testing.T) {
	database := testutil.DB(t)
	key, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)

	_, err = NewManager(nil, &staticKeys{key: key}, nil)
	assert.True(t, errors.IsValidation(err), "nil db must be rejected")

	_, err = NewManager(database, nil, nil)
	assert.True(t, errors.IsValidation(err), "nil keys must be rejected")
}

func TestCommitWrite_KeyUseRouting(t *testing.T) {
	manager, _, _, keys := testManagerWithKeys(t)
	ctx := t.Context()
	rkey := testRKey(8)

	_, err := manager.PutRecord(ctx, testDID, testCollection, rkey, testRecord("v1"))
	require.NoError(t, err)
	_, err = manager.PutRecord(ctx, testDID, testCollection, rkey, testRecord("v2"))
	require.NoError(t, err)
	_, err = manager.DeleteRecord(ctx, testDID, testCollection, rkey)
	require.NoError(t, err)

	assert.Equal(t, []KeyUse{KeyUseWrite, KeyUseWrite, KeyUseDelete}, keys.recordedUses(),
		"puts must request KeyUseWrite, deletes KeyUseDelete (tombstone scrub depends on it)")
}

func TestDeleteRecord_MissingRepoOrRecord(t *testing.T) {
	manager, _, _ := testManager(t)
	ctx := t.Context()

	_, err := manager.DeleteRecord(ctx, testDID, testCollection, testRKey(3))
	assert.True(t, errors.IsNotFound(err), "delete on a repo that does not exist yet")

	_, _, err = manager.GetRecord(ctx, testDID, testCollection, testRKey(3))
	assert.True(t, errors.IsNotFound(err))

	_, err = manager.PutRecord(ctx, testDID, testCollection, testRKey(3), testRecord("x"))
	require.NoError(t, err)
	_, err = manager.DeleteRecord(ctx, testDID, testCollection, testRKey(4))
	assert.True(t, errors.IsNotFound(err), "delete of a record that was never written")
}

func TestPutRecord_ValidatesInputs(t *testing.T) {
	manager, _, _ := testManager(t)
	ctx := t.Context()

	_, err := manager.PutRecord(ctx, "not-a-did", testCollection, testRKey(0), testRecord("x"))
	assert.True(t, errors.IsValidation(err))

	_, err = manager.PutRecord(ctx, testDID, "NotAnNSID!!", testRKey(0), testRecord("x"))
	assert.True(t, errors.IsValidation(err))

	_, err = manager.PutRecord(ctx, testDID, testCollection, "bad rkey!", testRecord("x"))
	assert.True(t, errors.IsValidation(err))

	_, err = manager.PutRecord(ctx, testDID, testCollection, testRKey(0), nil)
	assert.True(t, errors.IsValidation(err))

	_, err = manager.PutRecord(ctx, testDID, testCollection, testRKey(0), map[string]any{"text": "no type"})
	assert.True(t, errors.IsValidation(err), "records must carry $type")
}

func TestFirehoseEvents_AtomicWithCommits(t *testing.T) {
	manager, database, _ := testManager(t)
	ctx := t.Context()

	rkey := testRKey(5)
	res1, err := manager.PutRecord(ctx, testDID, testCollection, rkey, testRecord("v1"))
	require.NoError(t, err)
	res2, err := manager.PutRecord(ctx, testDID, testCollection, rkey, testRecord("v2"))
	require.NoError(t, err)
	res3, err := manager.DeleteRecord(ctx, testDID, testCollection, rkey)
	require.NoError(t, err)
	cid1, rev1, rev2, rev3 := res1.RecordCID, res1.Rev, res2.Rev, res3.Rev

	rows, err := database.QueryContext(ctx, `
		SELECT seq, commit_cid, prev_data_cid, since_rev, rev, ops, car
		FROM firehose_events WHERE did = $1 ORDER BY seq`, testDID)
	require.NoError(t, err)
	defer rows.Close()

	type event struct {
		seq       int64
		commitCID string
		prevData  *string
		sinceRev  *string
		rev       string
		ops       []Op
		car       []byte
	}
	var events []event
	for rows.Next() {
		var ev event
		var opsJSON []byte
		require.NoError(t, rows.Scan(&ev.seq, &ev.commitCID, &ev.prevData, &ev.sinceRev, &ev.rev, &opsJSON, &ev.car))
		require.NoError(t, json.Unmarshal(opsJSON, &ev.ops))
		events = append(events, ev)
	}
	require.NoError(t, rows.Err())
	require.Len(t, events, 3, "every commit appends exactly one event")

	// seq strictly increasing, matching the CommitResults; revs match too.
	assert.Less(t, events[0].seq, events[1].seq)
	assert.Less(t, events[1].seq, events[2].seq)
	assert.Equal(t, []int64{res1.Seq, res2.Seq, res3.Seq},
		[]int64{events[0].seq, events[1].seq, events[2].seq},
		"CommitResult.Seq must be the stored firehose cursor")
	assert.Equal(t, []string{rev1, rev2, rev3},
		[]string{events[0].rev, events[1].rev, events[2].rev})
	assert.Equal(t, []string{res1.CommitCID, res2.CommitCID, res3.CommitCID},
		[]string{events[0].commitCID, events[1].commitCID, events[2].commitCID})

	// Ops: create → update (with prev) → delete (with prev).
	require.Len(t, events[0].ops, 1)
	assert.Equal(t, Op{Action: OpActionCreate, Path: testCollection + "/" + rkey, CID: cid1}, events[0].ops[0])
	assert.Equal(t, OpActionUpdate, events[1].ops[0].Action)
	assert.Equal(t, cid1, events[1].ops[0].Prev, "update op must carry the previous record CID (sync v1.1)")
	assert.Equal(t, OpActionDelete, events[2].ops[0].Action)
	assert.Empty(t, events[2].ops[0].CID)
	assert.NotEmpty(t, events[2].ops[0].Prev)

	// prevData: null on genesis, then the prior commit's MST root.
	assert.Nil(t, events[0].prevData, "genesis event has no prevData")
	require.NotNil(t, events[1].prevData)

	// since_rev: null on genesis, then the previous commit's rev (the
	// subscribeRepos #commit `since` field task 04 serves).
	assert.Nil(t, events[0].sinceRev, "genesis event has no since_rev")
	require.NotNil(t, events[1].sinceRev)
	assert.Equal(t, rev1, *events[1].sinceRev)
	require.NotNil(t, events[2].sinceRev)
	assert.Equal(t, rev2, *events[2].sinceRev)

	// The genesis CAR slice is a complete mini-repo: indigo can load it.
	commit0, loaded0, err := indigorepo.LoadRepoFromCAR(ctx, bytes.NewReader(events[0].car))
	require.NoError(t, err, "genesis CAR slice must parse as a CAR with the commit as root")
	assert.Equal(t, events[0].commitCID, mustRootCID(t, events[0].car))
	assert.Equal(t, rev1, commit0.Rev)
	nsid, rk, err := syntax.ParseRepoPath(testCollection + "/" + rkey)
	require.NoError(t, err)
	_, c, err := loaded0.GetRecordBytes(ctx, nsid, rk)
	require.NoError(t, err)
	assert.Equal(t, cid1, c.String())

	// Every event's CAR slice has the commit block as its root, and the
	// commit block is physically FIRST in the stream (atproto consumers
	// conventionally expect root-first).
	for _, ev := range events {
		commit, _, err := indigorepo.LoadCommitFromCAR(ctx, bytes.NewReader(ev.car))
		require.NoError(t, err, "event %d CAR slice must contain its commit block", ev.seq)
		assert.Equal(t, ev.rev, commit.Rev)
		assert.Equal(t, ev.commitCID, mustRootCID(t, ev.car))

		reader, err := car.NewCarReader(bytes.NewReader(ev.car))
		require.NoError(t, err)
		firstBlk, err := reader.Next()
		require.NoError(t, err)
		assert.Equal(t, ev.commitCID, firstBlk.Cid().String(),
			"commit (root) block must be written first in the CAR slice")
	}

	// prevData of event 2 equals the MST root of commit 1.
	commit1, _, err := indigorepo.LoadCommitFromCAR(ctx, bytes.NewReader(events[1].car))
	require.NoError(t, err)
	assert.Equal(t, commit1.Data.String(), *events[2].prevData)
}

func mustRootCID(t *testing.T, carBytes []byte) string {
	t.Helper()
	_, root, err := indigorepo.LoadCommitFromCAR(context.Background(), bytes.NewReader(carBytes))
	require.NoError(t, err)
	return root.String()
}

func TestPutRecord_ConcurrentWritesSerialize(t *testing.T) {
	manager, database, _ := testManager(t)
	ctx := t.Context()

	const n = 12
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = manager.PutRecord(ctx, testDID, testCollection, testRKey(i), testRecord(fmt.Sprintf("post %d", i)))
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "concurrent put %d", i)
	}

	// All records landed and revs are strictly increasing in seq order.
	rows, err := database.QueryContext(ctx,
		`SELECT rev FROM firehose_events WHERE did = $1 ORDER BY seq`, testDID)
	require.NoError(t, err)
	defer rows.Close()
	var prev string
	var count int
	for rows.Next() {
		var rev string
		require.NoError(t, rows.Scan(&rev))
		assert.Greater(t, rev, prev, "revs must be strictly increasing in seq order")
		prev = rev
		count++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, n, count)

	carBytes, err := manager.ExportCAR(ctx, testDID)
	require.NoError(t, err)
	_, loaded, err := indigorepo.LoadRepoFromCAR(ctx, bytes.NewReader(carBytes))
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		nsid, rk, err := syntax.ParseRepoPath(testCollection + "/" + testRKey(i))
		require.NoError(t, err)
		_, _, err = loaded.GetRecordBytes(ctx, nsid, rk)
		require.NoError(t, err, "record %d must survive concurrent writes", i)
	}
}

func TestPutRecord_RevContinuityAcrossManagerRestart(t *testing.T) {
	// A fresh Manager over the same database simulates a process restart:
	// rev monotonicity must come from repo_state (NextRev off the stored
	// rev), not from any in-memory clock the old Manager held.
	manager1, database, key := testManager(t)
	ctx := t.Context()

	first, err := manager1.PutRecord(ctx, testDID, testCollection, testRKey(1), testRecord("before restart"))
	require.NoError(t, err)

	manager2, err := NewManager(database, &staticKeys{key: key}, nil)
	require.NoError(t, err)

	second, err := manager2.PutRecord(ctx, testDID, testCollection, testRKey(2), testRecord("after restart"))
	require.NoError(t, err)
	assert.Greater(t, second.Rev, first.Rev,
		"rev must stay monotonic across a restart (NextRev derives from the stored rev)")

	// Exactly one new firehose event, chained to the pre-restart head.
	rows, err := database.QueryContext(ctx,
		`SELECT rev, since_rev FROM firehose_events WHERE did = $1 ORDER BY seq`, testDID)
	require.NoError(t, err)
	defer rows.Close()
	type ev struct {
		rev      string
		sinceRev *string
	}
	var events []ev
	for rows.Next() {
		var e ev
		require.NoError(t, rows.Scan(&e.rev, &e.sinceRev))
		events = append(events, e)
	}
	require.NoError(t, rows.Err())
	require.Len(t, events, 2, "one event per commit, before and after the restart")
	assert.Equal(t, second.Rev, events[1].rev)
	require.NotNil(t, events[1].sinceRev, "the post-restart commit is not a genesis")
	assert.Equal(t, first.Rev, *events[1].sinceRev,
		"post-restart event must chain to the pre-restart head rev")
}

func TestGenesisCommit_SerializedAcrossManagers(t *testing.T) {
	// Two separate Managers (simulating two processes: no shared per-DID
	// mutex) race first-writes to the same new DID. The global commit
	// advisory lock — not the repo_state row lock, which cannot lock a
	// missing row — must serialize genesis, so exactly one genesis event
	// exists and no commit gets silently overwritten.
	managerA, database, key := testManager(t)
	managerB, err := NewManager(database, &staticKeys{key: key}, nil)
	require.NoError(t, err)
	ctx := t.Context()

	const perManager = 3
	var wg sync.WaitGroup
	errs := make([]error, 2*perManager)
	for i := 0; i < perManager; i++ {
		for j, manager := range []*Manager{managerA, managerB} {
			wg.Add(1)
			go func(idx int, m *Manager) {
				defer wg.Done()
				_, errs[idx] = m.PutRecord(ctx, testDID, testCollection, testRKey(idx),
					testRecord(fmt.Sprintf("post %d", idx)))
			}(i*2+j, manager)
		}
	}
	wg.Wait()
	for i, err := range errs {
		require.NoError(t, err, "concurrent cross-manager put %d", i)
	}

	// Exactly one genesis event (since_rev IS NULL).
	var genesis int
	require.NoError(t, database.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM firehose_events WHERE did = $1 AND since_rev IS NULL`,
		testDID).Scan(&genesis))
	assert.Equal(t, 1, genesis, "exactly one genesis commit despite the cross-process race")

	// Revs strictly ordered in seq order across both managers.
	rows, err := database.QueryContext(ctx,
		`SELECT rev FROM firehose_events WHERE did = $1 ORDER BY seq`, testDID)
	require.NoError(t, err)
	defer rows.Close()
	var prev string
	var count int
	for rows.Next() {
		var rev string
		require.NoError(t, rows.Scan(&rev))
		assert.Greater(t, rev, prev, "revs must be strictly increasing in seq order")
		prev = rev
		count++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 2*perManager, count)

	// Every record is reachable from the final head: no write was lost to a
	// genesis overwrite.
	carBytes, err := managerA.ExportCAR(ctx, testDID)
	require.NoError(t, err)
	_, loaded, err := indigorepo.LoadRepoFromCAR(ctx, bytes.NewReader(carBytes))
	require.NoError(t, err)
	for i := 0; i < 2*perManager; i++ {
		nsid, rk, err := syntax.ParseRepoPath(testCollection + "/" + testRKey(i))
		require.NoError(t, err)
		_, _, err = loaded.GetRecordBytes(ctx, nsid, rk)
		require.NoError(t, err, "record %d must be reachable from the final head", i)
	}
}

func TestFirehoseEvents_NonGenesisCARSlicesCarryRecordBlocks(t *testing.T) {
	// The genesis CAR slice is covered by TestFirehoseEvents_AtomicWithCommits
	// (it parses as a complete mini-repo). This pins the NON-genesis slices:
	// a subscribeRepos consumer materializes updates from the event's blocks
	// alone, so the update slice must physically contain the new record
	// block — a commit-only slice would parse but be useless.
	manager, database, _ := testManager(t)
	ctx := t.Context()
	rkey := testRKey(9)

	_, err := manager.PutRecord(ctx, testDID, testCollection, rkey, testRecord("v1"))
	require.NoError(t, err)
	update, err := manager.PutRecord(ctx, testDID, testCollection, rkey, testRecord("v2"))
	require.NoError(t, err)
	deleted, err := manager.DeleteRecord(ctx, testDID, testCollection, rkey)
	require.NoError(t, err)

	for _, tc := range []struct {
		name          string
		res           *CommitResult
		wantRecordCID string // required in the slice when non-empty
	}{
		{name: "update", res: update, wantRecordCID: update.RecordCID},
		{name: "delete", res: deleted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var carBytes []byte
			require.NoError(t, database.QueryRowContext(ctx,
				`SELECT car FROM firehose_events WHERE seq = $1`, tc.res.Seq).Scan(&carBytes))

			// Indigo must parse the slice, with the commit block as root.
			commit, root, err := indigorepo.LoadCommitFromCAR(ctx, bytes.NewReader(carBytes))
			require.NoError(t, err)
			assert.Equal(t, tc.res.CommitCID, root.String(), "CAR root must be the commit block")
			assert.Equal(t, tc.res.Rev, commit.Rev)

			// Enumerate the physical blocks in stream order.
			reader, err := car.NewCarReader(bytes.NewReader(carBytes))
			require.NoError(t, err)
			var cids []string
			for {
				blk, err := reader.Next()
				if err != nil {
					break // io.EOF ends the stream
				}
				cids = append(cids, blk.Cid().String())
			}
			require.NotEmpty(t, cids)
			assert.Equal(t, tc.res.CommitCID, cids[0],
				"commit (root) block must be physically first in the slice")

			if tc.wantRecordCID != "" {
				assert.Contains(t, cids, tc.wantRecordCID,
					"update slice must carry the new record block, not just the commit")
			}
		})
	}
}

func TestRepos_IsolatedPerDID(t *testing.T) {
	manager, _, _ := testManager(t)
	ctx := t.Context()

	rkey := testRKey(6)
	_, err := manager.PutRecord(ctx, testDID, testCollection, rkey, testRecord("mine"))
	require.NoError(t, err)
	_, err = manager.PutRecord(ctx, testOtherDID, testCollection, rkey, testRecord("theirs"))
	require.NoError(t, err)

	mine, _, err := manager.GetRecord(ctx, testDID, testCollection, rkey)
	require.NoError(t, err)
	theirs, _, err := manager.GetRecord(ctx, testOtherDID, testCollection, rkey)
	require.NoError(t, err)
	assert.Equal(t, "mine", mine["text"])
	assert.Equal(t, "theirs", theirs["text"])

	headA, _, err := manager.Head(ctx, testDID)
	require.NoError(t, err)
	headB, _, err := manager.Head(ctx, testOtherDID)
	require.NoError(t, err)
	assert.NotEqual(t, headA, headB)
}
