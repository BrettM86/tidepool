package repo

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/blobstore"
	"tidepool/internal/blobwithhold"
	"tidepool/internal/errors"
	"tidepool/internal/testutil"
)

// A STORE IN FLIGHT AND A BLOB DELETE ARE SERIALIZED PER (DID, CID).
//
// A store holds the blob store lock from its withhold check through its
// insert, and a transactional delete takes it exclusively, so a store whose
// check passed before a withhold landed cannot insert after a delete (which
// may lift that withhold) has run.

// TestDeleteBlobTxWaitsForAStoreInFlight.
//
// GIVEN PutBlob of (did, cid) past its withhold check and stalled at its
// insert; WHEN a transactional delete of (did, cid) runs THEN it waits, and
// after the store inserts and commits, the delete removes the stored bytes.
// A PutBlob that released the lock before its insert would let the delete
// run first and find nothing, leaving the stored bytes behind.
func TestDeleteBlobTxWaitsForAStoreInFlight(t *testing.T) {
	manager, database, _ := testManager(t)
	testutil.Truncate(t, database, "blobs")
	ctx := context.Background()
	data := []byte("store-in-flight")
	blobCID, err := rawBlobPrefix.Sum(data)
	require.NoError(t, err)

	// PutBlob's insert stalls on stallKey, held by the stall session, until
	// the test releases it. The trigger is dropped after the release (cleanups
	// run last-registered first), so the drop never waits on the stalled insert.
	const stallKey int64 = 0x626c0001
	installBlobInsertStall(t, database, blobCID.String(), stallKey)
	stall, err := database.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stall.Close() })
	_, err = stall.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, stallKey)
	require.NoError(t, err)
	released := false
	release := func() {
		if !released {
			released = true
			_, unlockErr := stall.ExecContext(ctx, `SELECT pg_advisory_unlock($1)`, stallKey)
			require.NoError(t, unlockErr)
		}
	}
	t.Cleanup(release)

	stored := make(chan error, 1)
	go func() {
		_, putErr := manager.PutBlob(ctx, testDID, "image/png", data)
		stored <- putErr
	}()
	waitForStallWaiters(t, database, stallKey, 1)

	deleted := make(chan error, 1)
	go func() {
		deleted <- manager.WithCommitLock(ctx, testDID, func(ctx context.Context, tx *sql.Tx) error {
			_, deleteErr := manager.DeleteBlobTx(ctx, tx, testDID, blobCID.String())
			return deleteErr
		})
	}()
	waitForBlobStoreWaiter(t, database, blobstore.LockNamespace)

	release()
	require.NoError(t, <-stored, "the store inserts and commits")
	require.NoError(t, <-deleted, "the delete")

	_, _, err = manager.GetBlob(ctx, testDID, blobCID.String())
	assert.True(t, errors.IsNotFound(err), "the delete removes the bytes the store inserted; err=%v", err)
}

// TestPutBlobChecksWithholdsAfterADeleteInFlight.
//
// GIVEN a delete of (did, cid) holding the blob store lock; WHEN PutBlob of
// those bytes runs THEN it waits, and a withhold committed while it waits
// refuses the store once the delete commits.
func TestPutBlobChecksWithholdsAfterADeleteInFlight(t *testing.T) {
	manager, database, _ := testManager(t)
	testutil.Truncate(t, database, "blobs")
	ctx := context.Background()
	data := []byte("delete-in-flight")
	blobCID, err := rawBlobPrefix.Sum(data)
	require.NoError(t, err)
	t.Cleanup(func() {
		if _, cleanupErr := database.Exec(`DELETE FROM blob_withholds WHERE cid = $1`, blobCID.String()); cleanupErr != nil {
			t.Errorf("cleanup blob_withholds: %v", cleanupErr)
		}
	})

	deletion, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = deletion.Rollback() }()
	require.NoError(t, blobstore.Lock(ctx, deletion, testDID, blobCID.String(), false))

	stored := make(chan error, 1)
	go func() {
		_, putErr := manager.PutBlob(ctx, testDID, "image/png", data)
		stored <- putErr
	}()
	waitForBlobStoreWaiter(t, database, blobstore.LockNamespace)

	_, err = blobwithhold.NewStore(database, blobstore.NewPostgres(database)).Withhold(ctx, blobwithhold.OperatorRequest{
		CID:        blobCID.String(),
		ReasonCode: blobwithhold.ReasonIllegalContent,
		Actor:      "blob-store-lock-test",
	})
	require.NoError(t, err)
	require.NoError(t, deletion.Commit())

	putErr := <-stored
	assert.True(t, errors.IsBlobWithheld(putErr), "the store sees the withhold committed while it waited; err=%v", putErr)
	var count int
	require.NoError(t, database.QueryRowContext(ctx,
		`SELECT count(*) FROM blobs WHERE did = $1 AND cid = $2`, testDID, blobCID.String()).Scan(&count))
	assert.Zero(t, count, "the refused store inserts nothing")
}

// TestDeleteBytesWaitsForPutBlobsInFlight (B9).
//
// GIVEN two DIDs' PutBlobs of identical bytes, both past their withhold
// check and stalled at their insert; WHEN the operator withholds the CID and
// runs delete-bytes THEN delete-bytes waits on the per-CID lock, and once the
// stores commit it removes both rows. Both puts reaching the stall at once
// shows the per-CID lock PutBlob takes is shared and not tied to the owner.
// A delete-bytes that did not wait would run while the inserts are pending,
// find no rows, and leave both stored under a withhold that orders them gone.
// The withhold store deletes through the mode's byte store: afterwards no
// blobs row and no blob_object_deletes row remains for the CID, the state
// reports no pending object deletes, and in s3 mode the bucket holds no
// object for either owner.
func TestDeleteBytesWaitsForPutBlobsInFlight(t *testing.T) {
	const (
		otherDID = "did:plc:yk4dd2qkboz2yv6tpubpc6co"
		cidStr   = "bafkreiaapixwgo5mtxkvxfpw5unjrazb763fz6rdiitbuwro5c6b375ghe"
		stallKey = int64(0x626c0002)
	)
	for _, mode := range blobManagerModes() {
		t.Run(mode.name, func(t *testing.T) {
			env := mode.env(t)
			manager, database := env.Manager, env.DB
			ctx := context.Background()
			data := []byte("b9 delete bytes waits for puts")
			t.Cleanup(func() {
				if _, cleanupErr := database.Exec(`DELETE FROM blob_withholds WHERE cid = $1`, cidStr); cleanupErr != nil {
					t.Errorf("cleanup blob_withholds: %v", cleanupErr)
				}
			})

			installBlobInsertStall(t, database, cidStr, stallKey)
			stall, err := database.Conn(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { _ = stall.Close() })
			_, err = stall.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, stallKey)
			require.NoError(t, err)
			released := false
			release := func() {
				if !released {
					released = true
					_, unlockErr := stall.ExecContext(ctx, `SELECT pg_advisory_unlock($1)`, stallKey)
					require.NoError(t, unlockErr)
				}
			}
			t.Cleanup(release)

			stored := make(chan error, 2)
			for _, did := range []string{testDID, otherDID} {
				go func() {
					_, putErr := manager.PutBlob(ctx, did, "image/png", data)
					stored <- putErr
				}()
			}
			waitForStallWaiters(t, database, stallKey, 2)

			withholds := blobwithhold.NewStore(database, env.Store)
			_, err = withholds.Withhold(ctx, blobwithhold.OperatorRequest{
				CID:        cidStr,
				ReasonCode: blobwithhold.ReasonIllegalContent,
				Actor:      "blob-store-lock-test",
			})
			require.NoError(t, err)
			type deleteResult struct {
				state blobwithhold.State
				err   error
			}
			deleted := make(chan deleteResult, 1)
			go func() {
				state, deleteErr := withholds.DeleteBytes(ctx, cidStr, "blob-store-lock-test")
				deleted <- deleteResult{state: state, err: deleteErr}
			}()
			waitForBlobStoreWaiter(t, database, blobstore.CIDLockNamespace)

			release()
			require.NoError(t, <-stored, "a store past its withhold check commits")
			require.NoError(t, <-stored, "a store past its withhold check commits")
			result := <-deleted
			require.NoError(t, result.err, "delete-bytes")

			var remaining int
			require.NoError(t, database.QueryRowContext(ctx,
				`SELECT count(*) FROM blobs WHERE cid = $1`, cidStr).Scan(&remaining))
			assert.Zero(t, remaining, "delete-bytes removes the rows both stores committed")
			assert.Empty(t, queueRows(t, database), "no queued object delete remains")
			assert.Equal(t, 0, result.state.PendingObjectDeletes)
			if env.Bucket != nil {
				assert.Empty(t, bucketKeys(t, env.Bucket), "delete-bytes removes both owners' objects")
			}
		})
	}
}

// TestSweepObjectDeletesWaitsForAPutBlobInFlight (B9).
//
// GIVEN a queued object delete of (did, cid) with no blobs row and no
// object, and PutBlob of those bytes stalled at its insert after uploading
// the object (holding its shared (did, cid) lock); WHEN SweepObjectDeletes
// runs THEN it waits on the exclusive (did, cid) lock, and once the store
// commits it finds the live row and drops only the queue row. A sweeper that
// did not wait would see no blobs row and delete the object the store just
// uploaded, leaving a committed row whose bytes are gone.
func TestSweepObjectDeletesWaitsForAPutBlobInFlight(t *testing.T) {
	const (
		cidStr   = "bafkreifx2nbmzja3jcxhp6j6hpmcq34w7jfr6jaust72jqbvx37i5uilra"
		stallKey = int64(0x626c0003)
	)
	env := s3Manager(t)
	manager, database := env.Manager, env.DB
	ctx := context.Background()
	data := []byte("b9 sweep waits for put")
	blobCID, err := rawBlobPrefix.Sum(data)
	require.NoError(t, err)
	require.Equal(t, cidStr, blobCID.String())
	key, err := blobstore.ObjectKey(testDID, cidStr)
	require.NoError(t, err)

	testutil.Truncate(t, database, "blobs", "blob_object_deletes")
	t.Cleanup(func() { testutil.Truncate(t, database, "blobs", "blob_object_deletes") })
	_, err = database.ExecContext(ctx,
		`INSERT INTO blob_object_deletes (did, cid) VALUES ($1, $2)`, testDID, cidStr)
	require.NoError(t, err)
	require.Empty(t, bucketKeys(t, env.Bucket))

	installBlobInsertStall(t, database, cidStr, stallKey)
	stall, err := database.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stall.Close() })
	_, err = stall.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, stallKey)
	require.NoError(t, err)
	released := false
	release := func() {
		if !released {
			released = true
			_, unlockErr := stall.ExecContext(ctx, `SELECT pg_advisory_unlock($1)`, stallKey)
			require.NoError(t, unlockErr)
		}
	}
	t.Cleanup(release)

	stored := make(chan error, 1)
	go func() {
		_, putErr := manager.PutBlob(ctx, testDID, "image/png", data)
		stored <- putErr
	}()
	waitForStallWaiters(t, database, stallKey, 1)
	require.Equal(t, []string{key}, bucketKeys(t, env.Bucket), "the store uploaded the object before its stalled insert")

	swept := make(chan error, 1)
	go func() { swept <- env.Store.SweepObjectDeletes(ctx) }()
	waitForBlobStoreWaiter(t, database, blobstore.LockNamespace)

	release()
	require.NoError(t, <-stored, "the store inserts and commits")
	require.NoError(t, <-swept, "the sweep")

	_, _, _, found := blobRow(t, database, testDID, cidStr)
	assert.True(t, found, "the store's blobs row is committed")
	assert.Equal(t, []string{key}, bucketKeys(t, env.Bucket), "the sweep keeps the object of a live row")
	object, _ := readObject(t, env.Bucket, key)
	assert.Equal(t, data, object)
	assert.Empty(t, queueRows(t, database), "no queued object delete remains")
}

// waitForBlobStoreWaiter fails the test unless a session waits on a
// two-argument advisory lock of namespace (blobstore.LockNamespace for the
// (did, cid) lock, blobstore.CIDLockNamespace for the per-CID lock) within
// five seconds.
func waitForBlobStoreWaiter(t *testing.T, database *sql.DB, namespace int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting int
		require.NoError(t, database.QueryRowContext(context.Background(),
			`SELECT count(*) FROM pg_locks
			 WHERE locktype = 'advisory' AND NOT granted AND objsubid = 2 AND classid = $1`,
			namespace).Scan(&waiting))
		if waiting > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no session waits on a blob store lock in namespace %#x", namespace)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// installBlobInsertStall creates a BEFORE INSERT trigger on blobs that, for
// rows of cidStr, waits for the one-argument advisory lock stallKey before
// letting the row through. It is dropped at cleanup.
func installBlobInsertStall(t *testing.T, database *sql.DB, cidStr string, stallKey int64) {
	t.Helper()
	ctx := context.Background()
	_, err := database.ExecContext(ctx, fmt.Sprintf(`
		CREATE FUNCTION blob_store_lock_stall() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			PERFORM pg_advisory_lock(%[1]d);
			PERFORM pg_advisory_unlock(%[1]d);
			RETURN NEW;
		END $$`, stallKey))
	require.NoError(t, err)
	t.Cleanup(func() {
		if _, dropErr := database.Exec(`DROP FUNCTION IF EXISTS blob_store_lock_stall()`); dropErr != nil {
			t.Errorf("drop function blob_store_lock_stall: %v", dropErr)
		}
	})
	_, err = database.ExecContext(ctx, fmt.Sprintf(`
		CREATE TRIGGER blob_store_lock_stall BEFORE INSERT ON blobs
		FOR EACH ROW WHEN (NEW.cid = '%s') EXECUTE FUNCTION blob_store_lock_stall()`, cidStr))
	require.NoError(t, err)
	t.Cleanup(func() {
		if _, dropErr := database.Exec(`DROP TRIGGER IF EXISTS blob_store_lock_stall ON blobs`); dropErr != nil {
			t.Errorf("drop trigger blob_store_lock_stall: %v", dropErr)
		}
	})
}

// waitForStallWaiters fails the test unless at least n sessions wait on the
// one-argument advisory lock stallKey within five seconds.
func waitForStallWaiters(t *testing.T, database *sql.DB, stallKey int64, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting int
		require.NoError(t, database.QueryRowContext(context.Background(),
			`SELECT count(*) FROM pg_locks
			 WHERE locktype = 'advisory' AND NOT granted AND objsubid = 1
			 AND classid = ($1::bigint >> 32)::oid AND objid = ($1::bigint & 4294967295)::oid`,
			stallKey).Scan(&waiting))
		if waiting >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d sessions wait on the blob insert stall, want %d", waiting, n)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
