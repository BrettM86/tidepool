package blobstore_test

import (
	"bytes"
	"context"
	"database/sql"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/blobstore"
	"tidepool/internal/blobwithhold"
	"tidepool/internal/testutil"
)

// migrationWait bounds every wait in the migration choreography, so a copy
// that never blocks (or never finishes) fails the test instead of hanging.
const migrationWait = 5 * time.Second

// migrationDB is the test database with the blob tables empty before and
// after the test.
func migrationDB(t *testing.T) *sql.DB {
	t.Helper()
	database := blobStoreDB(t)
	testutil.Truncate(t, database, "blob_withholds")
	t.Cleanup(func() { testutil.Truncate(t, database, "blob_withholds") })
	return database
}

// newMigrationS3 is an s3 store on bucket whose requests go through
// transport.
func newMigrationS3(t *testing.T, database *sql.DB, bucket *testutil.S3Bucket, transport http.RoundTripper) *blobstore.S3 {
	t.Helper()
	store, err := blobstore.NewS3(database, blobstore.S3Config{
		Endpoint:              bucket.Endpoint,
		Region:                bucket.Region,
		Bucket:                bucket.Name,
		AccessKeyID:           bucket.AccessKeyID,
		SecretAccessKey:       bucket.SecretAccessKey,
		AllowPrivateAddresses: true,
		Transport:             transport,
	})
	require.NoError(t, err)
	return store
}

// insertPostgresBytesBlob inserts a blobs row for (did, cid) holding data.
func insertPostgresBytesBlob(t *testing.T, database *sql.DB, did, cid string, data []byte) {
	t.Helper()
	_, err := database.ExecContext(context.Background(), `
		INSERT INTO blobs (did, cid, mime_type, size, bytes) VALUES ($1, $2, 'image/png', $3, $4)`,
		did, cid, len(data), data)
	require.NoError(t, err)
}

// objectBytes reads the object for (did, cid); found is false when there is
// none.
func objectBytes(t *testing.T, bucket *testutil.S3Bucket, did, cid string) (data []byte, found bool) {
	t.Helper()
	return testutil.ReadS3Object(t, bucket, "blobs/"+did+"/"+cid)
}

// migrationRun is MigratePostgresBytes running in the background.
type migrationRun struct {
	done   chan struct{}
	report blobstore.MigrationReport
	err    error
}

func startMigration(store *blobstore.S3, options blobstore.MigrationOptions) *migrationRun {
	run := &migrationRun{done: make(chan struct{})}
	go func() {
		defer close(run.done)
		ctx, cancel := context.WithTimeout(context.Background(), 4*migrationWait)
		defer cancel()
		run.report, run.err = store.MigratePostgresBytes(ctx, options)
	}()
	return run
}

// wait returns the run's result, failing the test if it does not finish.
func (r *migrationRun) wait(t *testing.T) (blobstore.MigrationReport, error) {
	t.Helper()
	select {
	case <-r.done:
		return r.report, r.err
	case <-time.After(4 * migrationWait):
		t.Fatal("the migration did not finish")
		return blobstore.MigrationReport{}, nil
	}
}

// waitUntil polls condition until it holds. It fails the test when the
// deadline passes, or as soon as finished closes first (the work that was
// meant to block returned without blocking), naming what was awaited.
func waitUntil(t *testing.T, what string, finished <-chan struct{}, condition func() bool) {
	t.Helper()
	deadline := time.After(migrationWait)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for !condition() {
		select {
		case <-finished:
			if condition() {
				return
			}
			t.Fatalf("finished before %s", what)
		case <-deadline:
			t.Fatalf("timed out waiting until %s", what)
		case <-tick.C:
		}
	}
}

// blobStoreLockWaiters counts sessions waiting on a (did, cid) blob store
// lock (the two-argument advisory lock in LockNamespace) in this database.
func blobStoreLockWaiters(t *testing.T, database *sql.DB) int {
	t.Helper()
	var n int
	require.NoError(t, database.QueryRow(`
		SELECT count(*) FROM pg_locks
		WHERE locktype = 'advisory' AND NOT granted AND classid = $1 AND objsubid = 2
		  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`,
		blobstore.LockNamespace).Scan(&n))
	return n
}

// rowLockWaiters counts sessions in this database waiting on a row lock.
func rowLockWaiters(t *testing.T, database *sql.DB) int {
	t.Helper()
	var n int
	require.NoError(t, database.QueryRow(`
		SELECT count(*) FROM pg_stat_activity
		WHERE datname = current_database() AND wait_event_type = 'Lock'
		  AND wait_event IN ('transactionid', 'tuple')`).Scan(&n))
	return n
}

// holdBlobStoreLock takes the exclusive (did, cid) lock in a new tx and
// returns it; the caller commits or rolls back to release it.
func holdBlobStoreLock(t *testing.T, database *sql.DB, did, cid string) *sql.Tx {
	t.Helper()
	tx, err := database.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	require.NoError(t, blobstore.Lock(context.Background(), tx, did, cid, false))
	return tx
}

func blobRowCount(t *testing.T, database *sql.DB, cid string) int {
	t.Helper()
	var n int
	require.NoError(t, database.QueryRow(`SELECT count(*) FROM blobs WHERE cid = $1`, cid).Scan(&n))
	return n
}

// TestMigratePostgresBytesSkipsARowChangedWhileItWaits (B4): a row deleted,
// or already moved to the bucket, while the copy waits on its (did, cid)
// lock is counted skipped: a deleted row leaves no object, and a row whose
// bytes were already NULL keeps its existing object untouched.
func TestMigratePostgresBytesSkipsARowChangedWhileItWaits(t *testing.T) {
	const did = "did:plc:migratechangedaaaaaaaaaa"
	data := []byte("migrate changed while waiting")
	existingObject := []byte("an object already in the bucket")
	for _, tc := range []struct {
		name string
		// change runs in the tx holding the lock, before it commits.
		change func(t *testing.T, tx *sql.Tx, cid string)
		// check asserts the key's object afterwards.
		check func(t *testing.T, bucket *testutil.S3Bucket, cid string)
		// before runs once the row is seeded.
		before func(t *testing.T, bucket *testutil.S3Bucket, cid string)
	}{
		{
			name: "deleted",
			change: func(t *testing.T, tx *sql.Tx, cid string) {
				_, err := tx.Exec(`DELETE FROM blobs WHERE did = $1 AND cid = $2`, did, cid)
				require.NoError(t, err)
			},
			check: func(t *testing.T, bucket *testutil.S3Bucket, cid string) {
				_, found := objectBytes(t, bucket, did, cid)
				assert.False(t, found, "a deleted row leaves no object")
			},
		},
		{
			name: "already NULL",
			before: func(t *testing.T, bucket *testutil.S3Bucket, cid string) {
				ctx, cancel := context.WithTimeout(context.Background(), migrationWait)
				defer cancel()
				_, err := bucket.Client.PutObject(ctx, bucket.Name, "blobs/"+did+"/"+cid,
					bytes.NewReader(existingObject), int64(len(existingObject)), minio.PutObjectOptions{})
				require.NoError(t, err)
			},
			change: func(t *testing.T, tx *sql.Tx, cid string) {
				_, err := tx.Exec(`UPDATE blobs SET bytes = NULL WHERE did = $1 AND cid = $2`, did, cid)
				require.NoError(t, err)
			},
			check: func(t *testing.T, bucket *testutil.S3Bucket, cid string) {
				object, found := objectBytes(t, bucket, did, cid)
				require.True(t, found, "the existing object is kept")
				assert.Equal(t, existingObject, object, "the existing object is unchanged")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			database := migrationDB(t)
			bucket := testutil.NewS3Bucket(t)
			cid := testutil.RawBlobCID(t, data)
			insertPostgresBytesBlob(t, database, did, cid, data)
			if tc.before != nil {
				tc.before(t, bucket, cid)
			}
			store := newMigrationS3(t, database, bucket, testutil.LocalS3Transport())

			blocker := holdBlobStoreLock(t, database, did, cid)
			run := startMigration(store, blobstore.MigrationOptions{Concurrency: 1})
			waitUntil(t, "the copy waits on the (did, cid) lock", run.done,
				func() bool { return blobStoreLockWaiters(t, database) > 0 })
			tc.change(t, blocker, cid)
			require.NoError(t, blocker.Commit())

			report, err := run.wait(t)
			require.NoError(t, err)
			assert.Equal(t, 1, report.Skipped, "skipped")
			assert.Zero(t, report.Copied, "copied")
			assert.Zero(t, report.Mismatched, "mismatched")
			assert.Zero(t, report.Failed, "failed")
			tc.check(t, bucket, cid)
		})
	}
}

// pauseAfterPut forwards every request; the first PUT's response, once
// MinIO has acknowledged it, is held until release is closed.
type pauseAfterPut struct {
	next         http.RoundTripper
	acknowledged chan struct{}
	release      chan struct{}
	once         sync.Once
}

func newPauseAfterPut() *pauseAfterPut {
	return &pauseAfterPut{
		next:         testutil.LocalS3Transport(),
		acknowledged: make(chan struct{}),
		release:      make(chan struct{}),
	}
}

func (p *pauseAfterPut) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := p.next.RoundTrip(request)
	if request.Method != http.MethodPut {
		return response, err
	}
	first := false
	p.once.Do(func() { first = true })
	if first {
		close(p.acknowledged)
		<-p.release
	}
	return response, err
}

// deleteBytesRun is DeleteBytes running in the background.
type deleteBytesRun struct {
	done chan struct{}
	err  error
}

func startDeleteBytes(withholds *blobwithhold.Store, cid string) *deleteBytesRun {
	run := &deleteBytesRun{done: make(chan struct{})}
	go func() {
		defer close(run.done)
		ctx, cancel := context.WithTimeout(context.Background(), 4*migrationWait)
		defer cancel()
		_, run.err = withholds.DeleteBytes(ctx, cid, "migrate-test")
	}()
	return run
}

func (r *deleteBytesRun) wait(t *testing.T) error {
	t.Helper()
	select {
	case <-r.done:
		return r.err
	case <-time.After(4 * migrationWait):
		t.Fatal("delete-bytes did not finish")
		return nil
	}
}

// TestMigratePostgresBytesRacingDeleteBytes (B5): delete-bytes racing the
// copy of the same CID, whichever goes first, leaves no blobs row and no
// object for the CID.
func TestMigratePostgresBytesRacingDeleteBytes(t *testing.T) {
	const did = "did:plc:migratedeletebytesaaaaaa"
	data := []byte("migrate racing delete-bytes")
	for _, tc := range []struct {
		name string
		race func(t *testing.T, database *sql.DB, bucket *testutil.S3Bucket, withholds *blobwithhold.Store, cid string) (*migrationRun, *deleteBytesRun)
	}{
		{
			// The copy waits on the (did, cid) lock while delete-bytes
			// commits its row delete; its object delete then waits on the
			// same lock until the blocker goes.
			name: "delete first",
			race: func(t *testing.T, database *sql.DB, bucket *testutil.S3Bucket, withholds *blobwithhold.Store, cid string) (*migrationRun, *deleteBytesRun) {
				blocker := holdBlobStoreLock(t, database, did, cid)
				migration := startMigration(newMigrationS3(t, database, bucket, testutil.LocalS3Transport()),
					blobstore.MigrationOptions{Concurrency: 1})
				waitUntil(t, "the copy waits on the (did, cid) lock", migration.done,
					func() bool { return blobStoreLockWaiters(t, database) > 0 })
				deletion := startDeleteBytes(withholds, cid)
				waitUntil(t, "delete-bytes commits its row delete", deletion.done,
					func() bool { return blobRowCount(t, database, cid) == 0 })
				require.NoError(t, blocker.Rollback())
				return migration, deletion
			},
		},
		{
			// The copy's upload is acknowledged by MinIO and held there while
			// delete-bytes waits on the row the copy holds.
			name: "copy first",
			race: func(t *testing.T, database *sql.DB, bucket *testutil.S3Bucket, withholds *blobwithhold.Store, cid string) (*migrationRun, *deleteBytesRun) {
				pause := newPauseAfterPut()
				t.Cleanup(func() {
					select {
					case <-pause.release:
					default:
						close(pause.release)
					}
				})
				migration := startMigration(newMigrationS3(t, database, bucket, pause),
					blobstore.MigrationOptions{Concurrency: 1})
				waitUntil(t, "MinIO acknowledges the copy's upload", migration.done, func() bool {
					select {
					case <-pause.acknowledged:
						return true
					default:
						return false
					}
				})
				deletion := startDeleteBytes(withholds, cid)
				waitUntil(t, "delete-bytes waits on the row the copy holds", deletion.done,
					func() bool { return rowLockWaiters(t, database) > 0 })
				close(pause.release)
				return migration, deletion
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			database := migrationDB(t)
			bucket := testutil.NewS3Bucket(t)
			cid := testutil.RawBlobCID(t, data)
			insertPostgresBytesBlob(t, database, did, cid, data)
			withholds := blobwithhold.NewStore(database, newMigrationS3(t, database, bucket, testutil.LocalS3Transport()))
			_, err := withholds.Withhold(ctx, blobwithhold.OperatorRequest{
				CID: cid, ReasonCode: blobwithhold.ReasonOperatorDiscretion, Actor: "migrate-test",
			})
			require.NoError(t, err)

			migration, deletion := tc.race(t, database, bucket, withholds, cid)

			_, migrationErr := migration.wait(t)
			require.NoError(t, migrationErr)
			require.NoError(t, deletion.wait(t))
			assert.Zero(t, blobRowCount(t, database, cid), "no blobs row for the CID")
			_, found := objectBytes(t, bucket, did, cid)
			assert.False(t, found, "no object for the CID")
		})
	}
}

// TestMigratePostgresBytesPages (B9): with two rows a page, six rows (the
// same CID under two DIDs, and mismatched rows ending a page and ending the
// table) are each handled once, and the run returns within its deadline: the
// four good rows copied with NULL bytes and objects, each mismatched row
// listed once. A mismatched row keeps its bytes, so a page cursor on it is the
// only cursor the bytes filter cannot hide: re-reading the cursor row would
// list it twice, and re-reading the last row would loop until the deadline.
func TestMigratePostgresBytesPages(t *testing.T) {
	// Ordered by (did, cid) the pages are [A shared, A only], [B only,
	// C mismatched], [D shared, E mismatched]: each DID after A holds one row,
	// so a mismatched row is the cursor of the second page and the last row of
	// the table.
	const (
		didA = "did:plc:migratepagesaaaaaaaaaaa"
		didB = "did:plc:migratepagesbbbbbbbbbbb"
		didC = "did:plc:migratepagesccccccccccc"
		didD = "did:plc:migratepagesddddddddddd"
		didE = "did:plc:migratepageseeeeeeeeeee"
	)
	ctx := context.Background()
	database := migrationDB(t)
	bucket := testutil.NewS3Bucket(t)
	shared := []byte("migrate pages shared under two DIDs")
	onlyA := []byte("migrate pages only under A")
	onlyB := []byte("migrate pages only under B")
	good := []struct {
		did  string
		data []byte
	}{
		{didA, shared},
		{didA, onlyA},
		{didB, onlyB},
		{didD, shared},
	}
	for _, row := range good {
		insertPostgresBytesBlob(t, database, row.did, testutil.RawBlobCID(t, row.data), row.data)
	}
	mismatched := []struct {
		did  string
		cid  string
		data []byte
	}{
		{didC, testutil.RawBlobCID(t, []byte("migrate pages claimed under C")), []byte("migrate pages bytes under C not matching the CID")},
		{didE, testutil.RawBlobCID(t, []byte("migrate pages claimed under E")), []byte("migrate pages bytes under E not matching the CID")},
	}
	for _, row := range mismatched {
		insertPostgresBytesBlob(t, database, row.did, row.cid, row.data)
	}
	store := newMigrationS3(t, database, bucket, testutil.LocalS3Transport())

	deadline := 4 * migrationWait
	migrateCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	started := time.Now()
	report, err := store.MigratePostgresBytes(migrateCtx, blobstore.MigrationOptions{Concurrency: 2, PageSize: 2})
	elapsed := time.Since(started)

	require.NoError(t, err, "the run returns within its deadline")
	require.Less(t, elapsed, deadline, "the run returns within its deadline")
	assert.Equal(t, 4, report.Copied, "copied")
	assert.Equal(t, 2, report.Mismatched, "mismatched")
	assert.Zero(t, report.Skipped, "skipped")
	assert.Zero(t, report.Failed, "failed")
	assert.Equal(t, []blobstore.BlobKey{{DID: didC, CID: mismatched[0].cid}, {DID: didE, CID: mismatched[1].cid}},
		report.Mismatches, "each mismatched row listed once, in page order")
	for _, row := range good {
		cid := testutil.RawBlobCID(t, row.data)
		var bytesNull bool
		require.NoError(t, database.QueryRowContext(ctx,
			`SELECT bytes IS NULL FROM blobs WHERE did = $1 AND cid = $2`, row.did, cid).Scan(&bytesNull))
		assert.True(t, bytesNull, "%s/%s has NULL bytes", row.did, cid)
		object, found := objectBytes(t, bucket, row.did, cid)
		assert.True(t, found, "%s/%s has an object", row.did, cid)
		assert.Equal(t, row.data, object, "%s/%s object holds the original bytes", row.did, cid)
	}
	for _, row := range mismatched {
		assert.Equal(t, []testutil.StoredBlob{{DID: row.did, CID: row.cid}},
			testutil.BlobsHoldingBytes(t, database, bucket, row.data), "%s/%s keeps its bytes", row.did, row.cid)
		_, found := objectBytes(t, bucket, row.did, row.cid)
		assert.False(t, found, "%s/%s has no object", row.did, row.cid)
	}
}
