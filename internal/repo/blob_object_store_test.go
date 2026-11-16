package repo

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	stderrors "errors"
	"log/slog"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/blobstore"
	"tidepool/internal/blobwithhold"
	"tidepool/internal/errors"
	"tidepool/internal/testutil"
)

// loggedManager builds a Manager whose logs land as JSON lines in the returned
// buffer, with blobs and blob_object_deletes empty before and after the test.
func loggedManager(t *testing.T, opts ...Option) (*Manager, *sql.DB, *bytes.Buffer) {
	t.Helper()
	logger, logs := capturedLogger()
	manager, database := managerWithLogger(t, logger, opts...)
	return manager, database, logs
}

// capturedLogger returns a logger writing JSON lines into the returned
// buffer, at every level from Info up.
func capturedLogger() (*slog.Logger, *bytes.Buffer) {
	var logs bytes.Buffer
	return slog.New(slog.NewJSONHandler(&logs, nil)), &logs
}

// managerWithLogger builds a Manager logging through logger, with blobs and
// blob_object_deletes empty before and after the test.
func managerWithLogger(t *testing.T, logger *slog.Logger, opts ...Option) (*Manager, *sql.DB) {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database, "blobs", "blob_object_deletes")
	t.Cleanup(func() { testutil.Truncate(t, database, "blobs", "blob_object_deletes") })
	key, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	manager, err := NewManager(database, &staticKeys{key: key}, logger, opts...)
	require.NoError(t, err)
	return manager, database
}

// logRecords parses every JSON log record in logs, at any level.
func logRecords(t *testing.T, logs *bytes.Buffer) []map[string]any {
	t.Helper()
	var records []map[string]any
	scanner := bufio.NewScanner(bytes.NewReader(logs.Bytes()))
	for scanner.Scan() {
		var record map[string]any
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &record))
		records = append(records, record)
	}
	require.NoError(t, scanner.Err())
	return records
}

// errorRecordsWithCode counts the Error-level JSON log records whose code
// attribute equals code.
func errorRecordsWithCode(t *testing.T, logs *bytes.Buffer, code string) int {
	t.Helper()
	count := 0
	for _, record := range logRecords(t, logs) {
		if record["level"] == "ERROR" && record["code"] == code {
			count++
		}
	}
	return count
}

func storedBlobCount(t *testing.T, database *sql.DB, did string) int {
	t.Helper()
	var count int
	require.NoError(t, database.QueryRowContext(context.Background(),
		`SELECT count(*) FROM blobs WHERE did = $1`, did).Scan(&count))
	return count
}

// TestGetBlobOfObjectStoreRowInPostgresMode (B2): a blobs row whose bytes are
// NULL (held in the object store) is an internal error in postgres mode —
// never NotFound, never nil data with no error — and is logged at Error with
// code blob_bytes_in_object_store.
func TestGetBlobOfObjectStoreRowInPostgresMode(t *testing.T) {
	const cidStr = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"
	manager, database, logs := loggedManager(t)
	_, err := database.ExecContext(context.Background(), `
		INSERT INTO blobs (did, cid, mime_type, size, bytes)
		VALUES ($1, $2, 'image/png', 11, NULL)`, testDID, cidStr)
	require.NoError(t, err, "a blobs row with NULL bytes")

	data, _, err := manager.GetBlob(context.Background(), testDID, cidStr)
	require.Error(t, err)
	assert.False(t, errors.IsNotFound(err), "the row exists; err=%v", err)
	assert.True(t, stderrors.Is(err, blobstore.ErrBytesInObjectStore), "err=%v", err)
	assert.Nil(t, data)
	assert.Equal(t, 1, errorRecordsWithCode(t, logs, "blob_bytes_in_object_store"))
}

// insertThenFailStore writes the blobs row through the caller's tx, then
// reports a byte store failure: PutBlob must roll the row back.
type insertThenFailStore struct {
	*blobstore.Postgres
}

func (insertThenFailStore) Put(ctx context.Context, tx *sql.Tx, did, cid, mimeType string, data []byte) error {
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO blobs (did, cid, mime_type, size, bytes) VALUES ($1, $2, $3, $4, $5)`,
		did, cid, mimeType, len(data), data); err != nil {
		return err
	}
	return stderrors.New("injected byte store failure")
}

// commitFailingStore writes the blobs row and succeeds, but leaves the tx
// holding a violated deferred unique constraint, so COMMIT fails.
type commitFailingStore struct {
	*blobstore.Postgres
}

// deadlineStore's byte write fails with its own deadline while the caller's
// context stays live.
type deadlineStore struct {
	*blobstore.Postgres
}

func (deadlineStore) Put(context.Context, *sql.Tx, string, string, string, []byte) error {
	return context.DeadlineExceeded
}

func (commitFailingStore) Put(ctx context.Context, tx *sql.Tx, did, cid, mimeType string, data []byte) error {
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO blobs (did, cid, mime_type, size, bytes) VALUES ($1, $2, $3, $4, $5)`,
		did, cid, mimeType, len(data), data); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		CREATE TEMP TABLE blob_commit_failure (
			k int,
			CONSTRAINT blob_commit_failure_k UNIQUE (k) DEFERRABLE INITIALLY DEFERRED
		) ON COMMIT DROP`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO blob_commit_failure (k) VALUES (1), (1)`); err != nil {
		return err
	}
	return nil
}

// TestPutBlobByteStoreFailure (B3, B8): a PutBlob whose byte write fails is
// classified IsBlobStoreUnavailable, logged at Error with code
// blob_object_put_failed, and stores nothing; a withheld put and an
// empty-bytes put are not that error. A put abandoned because the caller's
// own context ended (cancelled, past its deadline, or cancelled while waiting
// on the blob store lock) is still IsBlobStoreUnavailable but logs no
// blob_object_put_failed record; a byte store whose own deadline passes
// while the caller's context is live still does.
func TestPutBlobByteStoreFailure(t *testing.T) {
	cancelled := func() context.Context {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return ctx
	}
	pastDeadline := func() context.Context {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Minute))
		t.Cleanup(cancel)
		return ctx
	}
	for _, tc := range []struct {
		name string
		// store, when set, is injected through WithBlobStore.
		store func(database *sql.DB) blobstore.Store
		ctx   func() context.Context
		// cancelWhileLockWaits holds the (did, cid) blob store lock
		// exclusively in a test tx, and cancels the caller's context once
		// PutBlob waits on it.
		cancelWhileLockWaits bool
		data                 []byte
		withhold             bool
		// wantUnavailable rows must be IsBlobStoreUnavailable, with
		// wantPutFailedRecords blob_object_put_failed Error records; the
		// others must satisfy otherwise instead.
		wantUnavailable      bool
		wantPutFailedRecords int
		otherwise            func(error) bool
	}{
		{
			name:                 "byte write fails after inserting through the tx",
			store:                func(database *sql.DB) blobstore.Store { return insertThenFailStore{blobstore.NewPostgres(database)} },
			data:                 []byte("b3 insert then fail"),
			wantUnavailable:      true,
			wantPutFailedRecords: 1,
		},
		{
			name:            "caller context cancelled at entry",
			ctx:             cancelled,
			data:            []byte("b3 cancelled before begin"),
			wantUnavailable: true,
		},
		{
			name:            "caller context past its deadline",
			ctx:             pastDeadline,
			data:            []byte("b8 caller deadline passed"),
			wantUnavailable: true,
		},
		{
			name:                 "caller context cancelled while waiting on the blob store lock",
			cancelWhileLockWaits: true,
			data:                 []byte("b8 cancelled while waiting on the lock"),
			wantUnavailable:      true,
		},
		{
			name:                 "byte store deadline passes with the caller context live",
			store:                func(database *sql.DB) blobstore.Store { return deadlineStore{blobstore.NewPostgres(database)} },
			data:                 []byte("b8 store deadline"),
			wantUnavailable:      true,
			wantPutFailedRecords: 1,
		},
		{
			name:                 "commit fails after the byte write succeeds",
			store:                func(database *sql.DB) blobstore.Store { return commitFailingStore{blobstore.NewPostgres(database)} },
			data:                 []byte("b3 commit failure"),
			wantUnavailable:      true,
			wantPutFailedRecords: 1,
		},
		{
			name:      "withheld put",
			data:      []byte("b3 withheld"),
			withhold:  true,
			otherwise: errors.IsBlobWithheld,
		},
		{
			name:      "empty bytes",
			data:      []byte{},
			otherwise: errors.IsValidation,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			database := testutil.DB(t)
			var opts []Option
			if tc.store != nil {
				opts = append(opts, WithBlobStore(tc.store(database)))
			}
			manager, database, logs := loggedManager(t, opts...)
			if tc.withhold {
				blobCID, err := rawBlobPrefix.Sum(tc.data)
				require.NoError(t, err)
				t.Cleanup(func() {
					if _, cleanupErr := database.Exec(`DELETE FROM blob_withholds WHERE cid = $1`, blobCID.String()); cleanupErr != nil {
						t.Errorf("cleanup blob_withholds: %v", cleanupErr)
					}
				})
				_, err = blobwithhold.NewStore(database, blobstore.NewPostgres(database)).Withhold(context.Background(), blobwithhold.OperatorRequest{
					CID:        blobCID.String(),
					ReasonCode: blobwithhold.ReasonIllegalContent,
					Actor:      "blob-byte-store-failure-test",
				})
				require.NoError(t, err)
			}
			ctx := context.Background()
			if tc.ctx != nil {
				ctx = tc.ctx()
			}

			var err error
			if tc.cancelWhileLockWaits {
				blobCID, sumErr := rawBlobPrefix.Sum(tc.data)
				require.NoError(t, sumErr)
				holder, beginErr := database.BeginTx(context.Background(), nil)
				require.NoError(t, beginErr)
				defer func() { _ = holder.Rollback() }()
				require.NoError(t, blobstore.Lock(context.Background(), holder, testDID, blobCID.String(), false))
				putCtx, cancel := context.WithCancel(context.Background())
				defer cancel()
				stored := make(chan error, 1)
				go func() {
					_, putErr := manager.PutBlob(putCtx, testDID, "image/png", tc.data)
					stored <- putErr
				}()
				waitForBlobStoreWaiter(t, database, blobstore.LockNamespace)
				cancel()
				select {
				case err = <-stored:
				case <-time.After(5 * time.Second):
					t.Fatal("PutBlob did not return within 5s of its context being cancelled")
				}
			} else {
				_, err = manager.PutBlob(ctx, testDID, "image/png", tc.data)
			}
			require.Error(t, err)
			if tc.wantUnavailable {
				assert.True(t, errors.IsBlobStoreUnavailable(err), "err=%v", err)
				assert.Equal(t, tc.wantPutFailedRecords, errorRecordsWithCode(t, logs, "blob_object_put_failed"))
			} else {
				assert.False(t, errors.IsBlobStoreUnavailable(err), "err=%v", err)
				assert.True(t, tc.otherwise(err), "err=%v", err)
				assert.Zero(t, errorRecordsWithCode(t, logs, "blob_object_put_failed"))
			}
			assert.Zero(t, storedBlobCount(t, database, testDID), "nothing is stored")
		})
	}
}
