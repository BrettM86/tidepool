package repo

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	waitForStallWaiter(t, database, stallKey)

	deleted := make(chan error, 1)
	go func() {
		deleted <- manager.WithCommitLock(ctx, testDID, func(ctx context.Context, tx *sql.Tx) error {
			_, deleteErr := manager.DeleteBlobTx(ctx, tx, testDID, blobCID.String())
			return deleteErr
		})
	}()
	waitForBlobStoreWaiter(t, database)

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
	require.NoError(t, lockBlobStore(ctx, deletion, testDID, blobCID.String(), false))

	stored := make(chan error, 1)
	go func() {
		_, putErr := manager.PutBlob(ctx, testDID, "image/png", data)
		stored <- putErr
	}()
	waitForBlobStoreWaiter(t, database)

	_, err = blobwithhold.NewStore(database).Withhold(ctx, blobwithhold.OperatorRequest{
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

// waitForBlobStoreWaiter fails the test unless a session waits on a blob store
// lock within five seconds.
func waitForBlobStoreWaiter(t *testing.T, database *sql.DB) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting int
		require.NoError(t, database.QueryRowContext(context.Background(),
			`SELECT count(*) FROM pg_locks
			 WHERE locktype = 'advisory' AND NOT granted AND objsubid = 2 AND classid = $1`,
			blobStoreNamespace).Scan(&waiting))
		if waiting > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no session waits on the blob store lock")
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

// waitForStallWaiter fails the test unless a session waits on the
// one-argument advisory lock stallKey within five seconds.
func waitForStallWaiter(t *testing.T, database *sql.DB, stallKey int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting int
		require.NoError(t, database.QueryRowContext(context.Background(),
			`SELECT count(*) FROM pg_locks
			 WHERE locktype = 'advisory' AND NOT granted AND objsubid = 1
			 AND classid = ($1::bigint >> 32)::oid AND objid = ($1::bigint & 4294967295)::oid`,
			stallKey).Scan(&waiting))
		if waiting > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("no session waits on the blob insert stall")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
