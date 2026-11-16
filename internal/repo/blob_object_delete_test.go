package repo

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

// queueRow is one blob_object_deletes row.
type queueRow struct {
	DID       string
	CID       string
	Attempts  int
	LastError string
}

// queueRows reads every blob_object_deletes row, ordered by did, cid. A NULL
// last_error reads as "".
func queueRows(t *testing.T, database *sql.DB) []queueRow {
	t.Helper()
	rows, err := database.QueryContext(context.Background(),
		`SELECT did, cid, attempts, COALESCE(last_error, '') FROM blob_object_deletes ORDER BY did, cid`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var queued []queueRow
	for rows.Next() {
		var row queueRow
		require.NoError(t, rows.Scan(&row.DID, &row.CID, &row.Attempts, &row.LastError))
		queued = append(queued, row)
	}
	require.NoError(t, rows.Err())
	return queued
}

// drainQueue empties blob_object_deletes (the write-ahead rows a successful
// s3 PutBlob leaves) and checks it is empty.
func drainQueue(t *testing.T, database *sql.DB) {
	t.Helper()
	testutil.Truncate(t, database, "blob_object_deletes")
	require.Empty(t, queueRows(t, database), "blob_object_deletes is empty before the delete")
}

// enqueueObjectDelete inserts a blob_object_deletes row for (did, cid)
// queued age ago.
func enqueueObjectDelete(t *testing.T, database *sql.DB, did, cid string, age time.Duration) {
	t.Helper()
	_, err := database.ExecContext(context.Background(), `
		INSERT INTO blob_object_deletes (did, cid, queued_at)
		VALUES ($1, $2, now() - make_interval(secs => $3))`, did, cid, age.Seconds())
	require.NoError(t, err)
}

// recordsWithLevelAndCode returns the JSON log records at level (such as
// "WARN") whose code attribute equals code.
func recordsWithLevelAndCode(t *testing.T, logs *bytes.Buffer, level, code string) []map[string]any {
	t.Helper()
	var matched []map[string]any
	for _, record := range logRecords(t, logs) {
		if record["level"] == level && record["code"] == code {
			matched = append(matched, record)
		}
	}
	return matched
}

// operatorWithhold puts an active operator withhold on cid, removed at
// cleanup.
func operatorWithhold(t *testing.T, database *sql.DB, cid string) {
	t.Helper()
	t.Cleanup(func() {
		if _, err := database.Exec(`DELETE FROM blob_withholds WHERE cid = $1`, cid); err != nil {
			t.Errorf("cleanup blob_withholds: %v", err)
		}
	})
	_, err := blobwithhold.NewStore(database, blobstore.NewPostgres(database)).Withhold(context.Background(), blobwithhold.OperatorRequest{
		CID:        cid,
		ReasonCode: blobwithhold.ReasonIllegalContent,
		Actor:      "blob-object-delete-test",
	})
	require.NoError(t, err)
}

// failObjectDeletes makes the proxy answer every DELETE request with 500
// while fail reports true for its object path.
func failObjectDeletes(proxy *s3Proxy, fail func(path string) bool) {
	proxy.SetHook(func(r *http.Request) int {
		if r.Method == http.MethodDelete && fail(r.URL.Path) {
			return http.StatusInternalServerError
		}
		return 0
	})
}

// TestDeleteBlobPathsQueueObjectDeletes (B1, acceptance): DeleteBlob and
// DeleteBlobsForDID remove the deleted blobs' rows and, in s3 mode, their
// bucket objects, leaving no queued object delete. A blob under an active
// operator withhold keeps its row (and object) and is not queued. The
// DeleteBlobsForDID row also holds a queue row already present for one of
// the deleted blobs.
func TestDeleteBlobPathsQueueObjectDeletes(t *testing.T) {
	const (
		cidOne      = "bafkreie6dxbnuq5clk3dmddrfz6elcnx3we3dvhtvztsl3sp4kuharmmrm"
		cidTwo      = "bafkreiauaqpcw6ca4cb3d4jfbctvt6pt3o4kuarhqklsdfo65qdxym7f6y"
		cidWithheld = "bafkreiah6mjsc67nk34cmnzjz3c5s637upbq2gbnrsduvpbkurizrwadjy"
		keyWithheld = "blobs/did:plc:ewvi7nxzyoun6zhxrhs64oiz/bafkreiah6mjsc67nk34cmnzjz3c5s637upbq2gbnrsduvpbkurizrwadjy"
	)
	for _, mode := range blobManagerModes() {
		t.Run(mode.name, func(t *testing.T) {
			for _, path := range []struct {
				name string
				// data are the deletable blobs stored under testDID.
				data []string
				// conflict seeds, in s3 mode, a queue row for (testDID,
				// cidOne) before the delete.
				conflict bool
				delete   func(t *testing.T, manager *Manager)
				gone     []string
			}{
				{
					name: "DeleteBlob",
					data: []string{"b1 deletable one"},
					delete: func(t *testing.T, manager *Manager) {
						ctx := context.Background()
						require.NoError(t, manager.DeleteBlob(ctx, testDID, cidOne))
						require.NoError(t, manager.DeleteBlob(ctx, testDID, cidWithheld))
					},
					gone: []string{cidOne},
				},
				{
					name:     "DeleteBlobsForDID",
					data:     []string{"b1 deletable one", "b1 deletable two"},
					conflict: true,
					delete: func(t *testing.T, manager *Manager) {
						n, err := manager.DeleteBlobsForDID(context.Background(), testDID)
						require.NoError(t, err)
						assert.EqualValues(t, 2, n, "the two deletable rows")
					},
					gone: []string{cidOne, cidTwo},
				},
			} {
				t.Run(path.name, func(t *testing.T) {
					env := mode.env(t)
					ctx := context.Background()
					for _, data := range append(path.data, "b1 withheld") {
						_, err := env.Manager.PutBlob(ctx, testDID, "image/png", []byte(data))
						require.NoError(t, err)
					}
					operatorWithhold(t, env.DB, cidWithheld)
					drainQueue(t, env.DB)
					if path.conflict && env.Bucket != nil {
						enqueueObjectDelete(t, env.DB, testDID, cidOne, 0)
					}

					path.delete(t, env.Manager)

					for _, cid := range path.gone {
						_, _, _, found := blobRow(t, env.DB, testDID, cid)
						assert.False(t, found, "row %s is deleted", cid)
					}
					_, _, _, found := blobRow(t, env.DB, testDID, cidWithheld)
					assert.True(t, found, "the withheld blob keeps its row")
					assert.Empty(t, queueRows(t, env.DB), "no queued object delete remains")
					if env.Bucket != nil {
						assert.Equal(t, []string{keyWithheld}, bucketKeys(t, env.Bucket),
							"only the withheld blob's object remains")
					}
				})
			}
		})
	}
}

// TestDeleteBlobObjectDeleteFails (B2a): in s3 mode with the bucket refusing
// DELETE, DeleteBlob still returns nil and removes the row, keeping the
// queue row with attempts 1 and the error, logged once at Warn with code
// blob_object_delete_failed; DeleteBlobsForDID returns its count and nil
// and keeps its queue rows.
func TestDeleteBlobObjectDeleteFails(t *testing.T) {
	ctx := context.Background()
	allDeletes := func(string) bool { return true }

	t.Run("DeleteBlob", func(t *testing.T) {
		const cid = "bafkreig7723o3wbmytutv2gzrppyjeb476n6ilrrlocxv7sro2wkq3yisi"
		env := s3Manager(t)
		_, err := env.Manager.PutBlob(ctx, testDID, "image/png", []byte("b2 delete fails"))
		require.NoError(t, err)
		drainQueue(t, env.DB)
		failObjectDeletes(env.Proxy, allDeletes)

		require.NoError(t, env.Manager.DeleteBlob(ctx, testDID, cid))

		_, _, _, found := blobRow(t, env.DB, testDID, cid)
		assert.False(t, found, "the row is deleted")
		queued := queueRows(t, env.DB)
		require.Len(t, queued, 1, "the failed object delete stays queued")
		assert.Equal(t, testDID, queued[0].DID)
		assert.Equal(t, cid, queued[0].CID)
		assert.Equal(t, 1, queued[0].Attempts)
		assert.NotEmpty(t, queued[0].LastError)
		warnings := recordsWithLevelAndCode(t, env.Logs, "WARN", "blob_object_delete_failed")
		require.Len(t, warnings, 1)
		assert.Equal(t, testDID, warnings[0]["did"])
		assert.Equal(t, cid, warnings[0]["cid"])
		assert.EqualValues(t, 1, warnings[0]["attempts"])
	})

	t.Run("DeleteBlobsForDID", func(t *testing.T) {
		const (
			cidOne = "bafkreiatzoavjid2sfbd5el3gk5svo3qdhfhu3z7exzae7h2mmw4o2ozlm"
			cidTwo = "bafkreid5plrywiidttrvpyxy5oc3ibklf66yssb4fjmn3dsq5f3t53hsgi"
		)
		env := s3Manager(t)
		for _, data := range []string{"b2 for did fails one", "b2 for did fails two"} {
			_, err := env.Manager.PutBlob(ctx, testDID, "image/png", []byte(data))
			require.NoError(t, err)
		}
		drainQueue(t, env.DB)
		failObjectDeletes(env.Proxy, allDeletes)

		n, err := env.Manager.DeleteBlobsForDID(ctx, testDID)
		require.NoError(t, err)
		assert.EqualValues(t, 2, n)

		assert.Zero(t, storedBlobCount(t, env.DB, testDID), "the rows are deleted")
		queued := queueRows(t, env.DB)
		require.Len(t, queued, 2, "both failed object deletes stay queued")
		assert.Equal(t, cidOne, queued[0].CID)
		assert.Equal(t, cidTwo, queued[1].CID)
	})
}

// TestDeleteBlobObjectDeleteConnectionCut (B2a, MinIO cut off): with the
// connection to MinIO cut (the proxy closed, so the DELETE gets a transport
// error and no S3 error response), DeleteBlob still returns nil and removes
// the row, keeping the queue row with attempts 1 and the error, logged once
// at Warn with code blob_object_delete_failed; the object stays in the
// bucket.
func TestDeleteBlobObjectDeleteConnectionCut(t *testing.T) {
	ctx := context.Background()
	env := s3Manager(t)
	blob, err := env.Manager.PutBlob(ctx, testDID, "image/png", []byte("b2 connection cut"))
	require.NoError(t, err)
	cid := blob.Ref.String()
	key := "blobs/" + testDID + "/" + cid
	drainQueue(t, env.DB)
	require.Equal(t, []string{key}, bucketKeys(t, env.Bucket), "the object is stored before the cut")
	env.Proxy.Close()

	require.NoError(t, env.Manager.DeleteBlob(ctx, testDID, cid))

	_, _, _, found := blobRow(t, env.DB, testDID, cid)
	assert.False(t, found, "the row is deleted")
	queued := queueRows(t, env.DB)
	require.Len(t, queued, 1, "the failed object delete stays queued")
	assert.Equal(t, testDID, queued[0].DID)
	assert.Equal(t, cid, queued[0].CID)
	assert.Equal(t, 1, queued[0].Attempts)
	assert.NotEmpty(t, queued[0].LastError)
	warnings := recordsWithLevelAndCode(t, env.Logs, "WARN", "blob_object_delete_failed")
	require.Len(t, warnings, 1)
	assert.Equal(t, testDID, warnings[0]["did"])
	assert.Equal(t, cid, warnings[0]["cid"])
	assert.EqualValues(t, 1, warnings[0]["attempts"])
	assert.Equal(t, []string{key}, bucketKeys(t, env.Bucket), "the object stays in the bucket")
}

// TestSweepObjectDeletes (B2b): one sweep handles every queue row on its
// own: a row whose object DELETE fails keeps its row with attempts counted
// per sweep, while the rows beside it (object present, object already
// absent) are removed with their object. Once the bucket accepts the DELETE,
// a sweep removes the failing row's object and row. The good rows sort on
// both sides of the failing row in (did, cid) order (present < failing <
// absent), so the outcome does not depend on the sweep's row order.
func TestSweepObjectDeletes(t *testing.T) {
	const (
		cidFailing = "bafkreiesikv6rxs3ndr45cwtulrdhukvm6f7op26fj7xrryzsjjqf3vwgy"
		cidPresent = "bafkreiahkiuxvakosxpde3buwfmvkon25ckyy5hlvmtpyme3srybvn6iji"
		cidAbsent  = "bafkreiewp2s4da7ezjdsjg5cpddrayi33y4mchvg3idpnqbsmdc7xbyrmy"
		keyFailing = "blobs/did:plc:ewvi7nxzyoun6zhxrhs64oiz/bafkreiesikv6rxs3ndr45cwtulrdhukvm6f7op26fj7xrryzsjjqf3vwgy"
		keyPresent = "blobs/did:plc:ewvi7nxzyoun6zhxrhs64oiz/bafkreiahkiuxvakosxpde3buwfmvkon25ckyy5hlvmtpyme3srybvn6iji"
	)
	env := s3Manager(t)
	ctx := context.Background()
	putObject(t, env.Bucket, keyFailing, "image/png", []byte("b2 sweep failing"))
	putObject(t, env.Bucket, keyPresent, "image/png", []byte("b2 sweep present"))
	for _, cid := range []string{cidFailing, cidPresent, cidAbsent} {
		enqueueObjectDelete(t, env.DB, testDID, cid, 0)
	}
	failingPath := "/" + env.Bucket.Name + "/" + keyFailing
	failObjectDeletes(env.Proxy, func(path string) bool { return path == failingPath })

	// The sweep's own return value is not the contract here: the per-row
	// outcome in the queue and the bucket is.
	_ = env.Store.SweepObjectDeletes(ctx)
	queued := queueRows(t, env.DB)
	require.Len(t, queued, 1, "only the failing row stays queued")
	assert.Equal(t, cidFailing, queued[0].CID)
	assert.Equal(t, 1, queued[0].Attempts)
	assert.Equal(t, []string{keyFailing}, bucketKeys(t, env.Bucket), "the present object is deleted")

	_ = env.Store.SweepObjectDeletes(ctx)
	queued = queueRows(t, env.DB)
	require.Len(t, queued, 1)
	assert.Equal(t, 2, queued[0].Attempts, "each sweep counts an attempt")

	env.Proxy.SetHook(nil)
	require.NoError(t, env.Store.SweepObjectDeletes(ctx))
	assert.Empty(t, queueRows(t, env.DB), "the row is dropped once its object is deleted")
	assert.Empty(t, bucketKeys(t, env.Bucket), "the failing row's object is deleted")
}

// TestSweepObjectDeletesLogsStuckRows (B2c): a failing row queued more than
// an hour ago logs one Error record with code blob_object_delete_stuck per
// sweep; a fresh failing row in the same sweeps logs none.
func TestSweepObjectDeletesLogsStuckRows(t *testing.T) {
	const (
		cidOld   = "bafkreibhhz5vyy4pysmumqi3amhri5caaxtzncfmz6ggzmax6ldfrcel4y"
		cidFresh = "bafkreig2nqtbhjdbfyio56pbpkqmpa467uzfv472op67ys2w7oa7qcghya"
	)
	env := s3Manager(t)
	ctx := context.Background()
	enqueueObjectDelete(t, env.DB, testDID, cidOld, 2*time.Hour)
	enqueueObjectDelete(t, env.DB, testDID, cidFresh, 0)
	failObjectDeletes(env.Proxy, func(string) bool { return true })

	_ = env.Store.SweepObjectDeletes(ctx)
	_ = env.Store.SweepObjectDeletes(ctx)

	stuck := recordsWithLevelAndCode(t, env.Logs, "ERROR", "blob_object_delete_stuck")
	require.Len(t, stuck, 2, "one stuck record per sweep")
	for _, record := range stuck {
		assert.Equal(t, cidOld, record["cid"], "only the overdue row is stuck")
	}
	assert.Len(t, queueRows(t, env.DB), 2, "both failing rows stay queued")
}

// commitPoisoningS3Store uploads through the real s3 store, then leaves the
// tx holding a violated deferred unique constraint, so PutBlob's COMMIT
// fails after the object is in the bucket.
type commitPoisoningS3Store struct {
	*blobstore.S3
}

func (s commitPoisoningS3Store) Put(ctx context.Context, tx *sql.Tx, did, cid, mimeType string, data []byte) error {
	if err := s.S3.Put(ctx, tx, did, cid, mimeType, data); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		CREATE TEMP TABLE blob_commit_failure (
			k int,
			CONSTRAINT blob_commit_failure_k UNIQUE (k) DEFERRABLE INITIALLY DEFERRED
		) ON COMMIT DROP`); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO blob_commit_failure (k) VALUES (1), (1)`)
	return err
}

// TestPutBlobCommitFailureLeavesQueuedObject (B3a): a PutBlob whose COMMIT
// fails after the upload leaves no blobs row, but its queue row for the
// uploaded object stays, so one sweep removes the object and the row.
func TestPutBlobCommitFailureLeavesQueuedObject(t *testing.T) {
	const (
		cid = "bafkreidfaov76ma5tqnd6oyswmr7hezkdhdezi3giubij652mkr4q4fgse"
		key = "blobs/did:plc:ewvi7nxzyoun6zhxrhs64oiz/bafkreidfaov76ma5tqnd6oyswmr7hezkdhdezi3giubij652mkr4q4fgse"
	)
	env := s3Manager(t)
	manager, database := managerWithLogger(t, env.Logger, WithBlobStore(commitPoisoningS3Store{env.Store}))
	ctx := context.Background()

	_, err := manager.PutBlob(ctx, testDID, "image/png", []byte("b3 commit fails after upload"))
	require.Error(t, err)

	_, _, _, found := blobRow(t, database, testDID, cid)
	assert.False(t, found, "no blobs row")
	assert.Equal(t, []string{key}, bucketKeys(t, env.Bucket), "the uploaded object is in the bucket")
	assert.Equal(t, []queueRow{{DID: testDID, CID: cid}}, queueRows(t, database),
		"the upload's queue row remains")

	require.NoError(t, env.Store.SweepObjectDeletes(ctx))
	assert.Empty(t, bucketKeys(t, env.Bucket), "the sweep deletes the orphan object")
	assert.Empty(t, queueRows(t, database), "and drops its queue row")
}

// TestPutBlobQueuesBeforeUpload (B3b): while PutBlob's object PUT is held at
// the proxy, its queue row for (did, cid) is already committed and visible
// from another connection, for a new upload and for the re-upload that
// repairs a NULL-bytes row whose object is missing.
func TestPutBlobQueuesBeforeUpload(t *testing.T) {
	for _, tc := range []struct {
		name string
		data string
		cid  string
		key  string
		// setup runs before the held PutBlob, queue drained after it.
		setup func(t *testing.T, env *s3TestEnv)
	}{
		{
			name: "new upload",
			data: "b3 enqueue before new upload",
			cid:  "bafkreiflmdknnnw6m7qilhhjdtiy7anpi4x44lmoosbfesgkfc23gniifi",
			key:  "blobs/did:plc:ewvi7nxzyoun6zhxrhs64oiz/bafkreiflmdknnnw6m7qilhhjdtiy7anpi4x44lmoosbfesgkfc23gniifi",
		},
		{
			name: "NULL-bytes row repair",
			data: "b3 enqueue before repair upload",
			cid:  "bafkreig6ptb3a4wd37miqvwh7hjnko7gc3g5k434wqhp3ft3gy4nckhz6i",
			key:  "blobs/did:plc:ewvi7nxzyoun6zhxrhs64oiz/bafkreig6ptb3a4wd37miqvwh7hjnko7gc3g5k434wqhp3ft3gy4nckhz6i",
			setup: func(t *testing.T, env *s3TestEnv) {
				ctx := context.Background()
				_, err := env.Manager.PutBlob(ctx, testDID, "image/png", []byte("b3 enqueue before repair upload"))
				require.NoError(t, err)
				require.NoError(t, env.Bucket.Client.RemoveObject(ctx, env.Bucket.Name,
					"blobs/did:plc:ewvi7nxzyoun6zhxrhs64oiz/bafkreig6ptb3a4wd37miqvwh7hjnko7gc3g5k434wqhp3ft3gy4nckhz6i",
					minio.RemoveObjectOptions{}))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := s3Manager(t)
			ctx := context.Background()
			if tc.setup != nil {
				tc.setup(t, env)
			}
			drainQueue(t, env.DB)

			objectPath := "/" + env.Bucket.Name + "/" + tc.key
			held := make(chan struct{})
			var heldOnce sync.Once
			release := make(chan struct{})
			var releaseOnce sync.Once
			releasePut := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(releasePut)
			env.Proxy.SetHook(func(r *http.Request) int {
				if r.Method == http.MethodPut && r.URL.Path == objectPath {
					heldOnce.Do(func() { close(held) })
					<-release
				}
				return 0
			})

			stored := make(chan error, 1)
			go func() {
				_, err := env.Manager.PutBlob(ctx, testDID, "image/png", []byte(tc.data))
				stored <- err
			}()
			select {
			case <-held:
			case <-time.After(5 * time.Second):
				releasePut()
				t.Fatal("PutBlob sent no object PUT within 5s")
			}

			assert.Equal(t, []queueRow{{DID: testDID, CID: tc.cid}}, queueRows(t, env.DB),
				"the queue row is committed before the upload")

			releasePut()
			require.NoError(t, <-stored)
		})
	}
}

// TestDeleteBlobTxLeavesQueueRowToTheSweeper (B5): in s3 mode DeleteBlobTx
// queues the object delete in the caller's tx: a rollback keeps the row and
// the object and queues nothing; a commit leaves the queue row and the
// object for the sweeper.
func TestDeleteBlobTxLeavesQueueRowToTheSweeper(t *testing.T) {
	for _, tc := range []struct {
		name      string
		data      string
		cid       string
		key       string
		commit    bool
		wantRow   bool
		wantQueue []queueRow
	}{
		{
			name:    "caller tx rolls back",
			data:    "b5 delete tx rolls back",
			cid:     "bafkreigt77fs4unkij3auo4muearqwiyrxuqextc2cpvuokcnl3pwlnsmy",
			key:     "blobs/did:plc:ewvi7nxzyoun6zhxrhs64oiz/bafkreigt77fs4unkij3auo4muearqwiyrxuqextc2cpvuokcnl3pwlnsmy",
			wantRow: true,
		},
		{
			name:   "caller tx commits",
			data:   "b5 delete tx commits",
			cid:    "bafkreibc2hoyqs3xr4h6lus3tfc37x425upgrb565bxa47pd3xmskpudre",
			key:    "blobs/did:plc:ewvi7nxzyoun6zhxrhs64oiz/bafkreibc2hoyqs3xr4h6lus3tfc37x425upgrb565bxa47pd3xmskpudre",
			commit: true,
			wantQueue: []queueRow{
				{DID: testDID, CID: "bafkreibc2hoyqs3xr4h6lus3tfc37x425upgrb565bxa47pd3xmskpudre"},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := s3Manager(t)
			ctx := context.Background()
			_, err := env.Manager.PutBlob(ctx, testDID, "image/png", []byte(tc.data))
			require.NoError(t, err)
			drainQueue(t, env.DB)

			tx, err := env.DB.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			gone, err := env.Manager.DeleteBlobTx(ctx, tx, testDID, tc.cid)
			require.NoError(t, err)
			assert.True(t, gone)
			if tc.commit {
				require.NoError(t, tx.Commit())
			} else {
				require.NoError(t, tx.Rollback())
			}

			_, _, _, found := blobRow(t, env.DB, testDID, tc.cid)
			assert.Equal(t, tc.wantRow, found, "blobs row present")
			assert.Equal(t, tc.wantQueue, queueRows(t, env.DB))
			assert.Equal(t, []string{tc.key}, bucketKeys(t, env.Bucket), "the object is left for the sweeper")
		})
	}
}
