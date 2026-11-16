package blobstore_test

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"testing"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/blobstore"
	"tidepool/internal/testutil"
)

const (
	testDID = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	cidOne  = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"
	cidTwo  = "bafkreiet3spndhanmbvtcma43thcotyqstpt5mbegss2e5qg5pw2vyjkfe"
)

// blobStoreDB returns the migrated test database with blobs and
// blob_object_deletes empty before and after the test.
func blobStoreDB(t *testing.T) *sql.DB {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database, "blobs")
	testutil.TruncateIfExists(t, database, "blob_object_deletes")
	t.Cleanup(func() {
		testutil.Truncate(t, database, "blobs")
		testutil.TruncateIfExists(t, database, "blob_object_deletes")
	})
	return database
}

func insertObjectStoreBlob(t *testing.T, database *sql.DB, cid string) {
	t.Helper()
	_, err := database.ExecContext(context.Background(), `
		INSERT INTO blobs (did, cid, mime_type, size, bytes)
		VALUES ($1, $2, 'image/png', 11, NULL)`, testDID, cid)
	require.NoError(t, err, "a blobs row with NULL bytes (bytes held in the object store)")
}

func insertObjectDelete(t *testing.T, database *sql.DB, cid string) error {
	t.Helper()
	_, err := database.ExecContext(context.Background(), `
		INSERT INTO blob_object_deletes (did, cid, queued_at, attempts)
		VALUES ($1, $2, now(), 0)`, testDID, cid)
	return err
}

// TestSchemaHoldsObjectStoreRows (B1): the migrated schema accepts a blobs
// row with NULL bytes and a blob_object_deletes row, and the queue's primary
// key rejects a second row for the same (did, cid).
func TestSchemaHoldsObjectStoreRows(t *testing.T) {
	database := blobStoreDB(t)

	insertObjectStoreBlob(t, database, cidOne)
	require.NoError(t, insertObjectDelete(t, database, cidOne), "a queued object delete")

	err := insertObjectDelete(t, database, cidOne)
	require.Error(t, err, "a duplicate (did, cid) queue row")
	var pqErr *pq.Error
	require.True(t, stderrors.As(err, &pqErr), "err=%v", err)
	assert.EqualValues(t, "23505", pqErr.Code, "unique_violation on the primary key")

	require.NoError(t, insertObjectDelete(t, database, cidTwo), "another cid under the same did is its own queue row")
}

// TestCheckPostgresMode (B6): postgres mode refuses a database that holds
// NULL-bytes blobs rows or queued object deletes, naming the code and count.
func TestCheckPostgresMode(t *testing.T) {
	for _, tc := range []struct {
		name string
		seed func(t *testing.T, database *sql.DB)
		want *blobstore.ModeError
	}{
		{
			name: "two NULL-bytes blobs rows",
			seed: func(t *testing.T, database *sql.DB) {
				insertObjectStoreBlob(t, database, cidOne)
				insertObjectStoreBlob(t, database, cidTwo)
			},
			want: &blobstore.ModeError{Code: "blob_bytes_in_object_store", Count: 2},
		},
		{
			name: "one queued object delete",
			seed: func(t *testing.T, database *sql.DB) {
				require.NoError(t, insertObjectDelete(t, database, cidOne), "a queued object delete")
			},
			want: &blobstore.ModeError{Code: "blob_object_deletes_pending", Count: 1},
		},
		{
			name: "only postgres-held bytes",
			seed: func(t *testing.T, database *sql.DB) {
				_, err := database.ExecContext(context.Background(), `
					INSERT INTO blobs (did, cid, mime_type, size, bytes)
					VALUES ($1, $2, 'image/png', 3, '\x010203'::bytea)`, testDID, cidOne)
				require.NoError(t, err)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			database := blobStoreDB(t)
			tc.seed(t, database)

			err := blobstore.CheckPostgresMode(context.Background(), database)
			if tc.want == nil {
				require.NoError(t, err)
				return
			}
			var modeErr *blobstore.ModeError
			require.True(t, stderrors.As(err, &modeErr), "want a *ModeError; err=%v", err)
			assert.Equal(t, *tc.want, *modeErr)
			assert.Contains(t, err.Error(), tc.want.Code, "the message names the code")
			assert.Contains(t, err.Error(), fmt.Sprintf(" %d ", tc.want.Count), "the message names the count")
		})
	}
}

// TestLockKeyIsPinned: the blob store lock keeps its namespace and key, so
// builds before and after the move still exclude each other. The key is the
// FNV-1a 32-bit hash of did+" "+cid, written here as a literal.
func TestLockKeyIsPinned(t *testing.T) {
	require.EqualValues(t, 0x626c, blobstore.LockNamespace, `the namespace is "bl"`)

	const wantObjectID = 1430128517 // FNV-1a 32 of testDID + " " + cidOne
	database := blobStoreDB(t)
	for _, tc := range []struct {
		shared   bool
		wantMode string
	}{
		{shared: false, wantMode: "ExclusiveLock"},
		{shared: true, wantMode: "ShareLock"},
	} {
		t.Run(tc.wantMode, func(t *testing.T) {
			ctx := context.Background()
			tx, err := database.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()

			require.NoError(t, blobstore.Lock(ctx, tx, testDID, cidOne, tc.shared))

			var (
				classID, objectID int64
				objectSubID       int
				mode              string
			)
			require.NoError(t, tx.QueryRowContext(ctx, `
				SELECT classid::int8, objid::int8, objsubid, mode FROM pg_locks
				WHERE locktype = 'advisory' AND pid = pg_backend_pid()`).
				Scan(&classID, &objectID, &objectSubID, &mode), "exactly one advisory lock held")
			assert.EqualValues(t, 25196, classID, "classid is the namespace 0x626c")
			assert.EqualValues(t, wantObjectID, objectID, "objid is the FNV-1a hash of did+\" \"+cid")
			assert.Equal(t, 2, objectSubID, "objsubid 2 marks the two-argument form")
			assert.Equal(t, tc.wantMode, mode)
		})
	}
}

// TestCIDLockKeyIsPinned: the per-CID blob lock keeps its namespace and key,
// so builds on either side of an overlapping deploy still exclude each
// other. The key is the FNV-1a 32-bit hash of the CID alone, written here as
// a literal; pg_locks shows it as an unsigned oid.
func TestCIDLockKeyIsPinned(t *testing.T) {
	require.EqualValues(t, 0x6263, blobstore.CIDLockNamespace, `the namespace is "bc"`)

	const wantObjectID = 4241426303 // FNV-1a 32 of cidOne
	database := blobStoreDB(t)
	for _, tc := range []struct {
		shared   bool
		wantMode string
	}{
		{shared: false, wantMode: "ExclusiveLock"},
		{shared: true, wantMode: "ShareLock"},
	} {
		t.Run(tc.wantMode, func(t *testing.T) {
			ctx := context.Background()
			tx, err := database.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()

			require.NoError(t, blobstore.LockCID(ctx, tx, cidOne, tc.shared))

			var (
				classID, objectID int64
				objectSubID       int
				mode              string
			)
			require.NoError(t, tx.QueryRowContext(ctx, `
				SELECT classid::int8, objid::int8, objsubid, mode FROM pg_locks
				WHERE locktype = 'advisory' AND pid = pg_backend_pid()`).
				Scan(&classID, &objectID, &objectSubID, &mode), "exactly one advisory lock held")
			assert.EqualValues(t, 25187, classID, "classid is the namespace 0x6263")
			assert.EqualValues(t, wantObjectID, objectID, "objid is the FNV-1a hash of the cid")
			assert.Equal(t, 2, objectSubID, "objsubid 2 marks the two-argument form")
			assert.Equal(t, tc.wantMode, mode)
		})
	}
}
