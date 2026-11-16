package repo

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/blobstore"
	"tidepool/internal/blobwithhold"
)

// pausingDeleteS3Store deletes through the real s3 store, then reports the
// delete on deleted and waits for release before returning: DeleteBlob
// pauses between its autocommit delete and processing its queued object
// delete, DeleteBytes between its delete and its commit.
type pausingDeleteS3Store struct {
	*blobstore.S3
	deleted chan<- struct{}
	release <-chan struct{}
}

func (s pausingDeleteS3Store) pause(ctx context.Context) error {
	s.deleted <- struct{}{}
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s pausingDeleteS3Store) Delete(ctx context.Context, q blobstore.Querier, did, cid string) (int64, error) {
	n, err := s.S3.Delete(ctx, q, did, cid)
	if err != nil {
		return n, err
	}
	return n, s.pause(ctx)
}

func (s pausingDeleteS3Store) DeleteCID(ctx context.Context, q blobstore.Querier, cid string) (int64, error) {
	n, err := s.S3.DeleteCID(ctx, q, cid)
	if err != nil {
		return n, err
	}
	return n, s.pause(ctx)
}

// sessionWaitsOnLock reports whether a session in this database other than
// the caller's waits on a lock in a query matching pattern (a LIKE pattern).
func sessionWaitsOnLock(t *testing.T, database *sql.DB, pattern string) bool {
	t.Helper()
	var waiting bool
	require.NoError(t, database.QueryRow(`
		SELECT EXISTS (
			SELECT 1 FROM pg_stat_activity
			WHERE datname = current_database() AND pid <> pg_backend_pid()
			  AND wait_event_type = 'Lock' AND query LIKE $1)`, pattern).Scan(&waiting))
	return waiting
}

// waitUntil polls condition until it holds, failing the test after 10s.
func waitUntil(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for !condition() {
		select {
		case <-deadline:
			t.Fatalf("waiting for %s: not within 10s", what)
		case <-poll.C:
		}
	}
}

// TestObjectDeleteProcessorRacesConcurrentDelete: the object delete
// processor reads (did, cid) as live, then a concurrent DeleteBlob or
// DeleteBytes deletes the blobs row before the processor drops the
// write-ahead queue row PutBlob left. The object is still deleted: no blobs
// row, no queue row and no bucket object remain.
//
// A test tx row-locks the queue row so the sweeper pauses on its queue-row
// delete after its live read. The deleter's store pauses after its delete
// statement until the sweeper has committed, so the deleter's own object
// delete processing runs after the sweeper's commit.
func TestObjectDeleteProcessorRacesConcurrentDelete(t *testing.T) {
	const cid = "bafkreigwdvwv5bdck6aqlzwm3cskqhaisfztosk75bs6ruvpdozvcptjwy"
	for _, deleter := range []struct {
		name string
		// withhold puts an active operator withhold on the CID first.
		withhold bool
		delete   func(ctx context.Context, env *s3TestEnv, store pausingDeleteS3Store, manager *Manager) error
	}{
		{
			name:     "DeleteBytes",
			withhold: true,
			delete: func(ctx context.Context, env *s3TestEnv, store pausingDeleteS3Store, _ *Manager) error {
				_, err := blobwithhold.NewStore(env.DB, store).DeleteBytes(ctx, cid, "blob-object-delete-race-test")
				return err
			},
		},
		{
			name: "DeleteBlob",
			delete: func(ctx context.Context, _ *s3TestEnv, _ pausingDeleteS3Store, manager *Manager) error {
				return manager.DeleteBlob(ctx, testDID, cid)
			},
		},
	} {
		t.Run(deleter.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			env := s3Manager(t)
			deleted := make(chan struct{}, 1)
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseDeleter := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(releaseDeleter)
			store := pausingDeleteS3Store{S3: env.Store, deleted: deleted, release: release}
			manager, database := managerWithLogger(t, env.Logger, WithBlobStore(store))

			stored, err := manager.PutBlob(ctx, testDID, "image/png", []byte("object delete race"))
			require.NoError(t, err)
			require.Equal(t, cid, stored.Ref.String())
			require.Equal(t, []queueRow{{DID: testDID, CID: cid}}, queueRows(t, database),
				"PutBlob leaves its write-ahead queue row")
			if deleter.withhold {
				operatorWithhold(t, database, cid)
			}

			blocker, err := database.BeginTx(ctx, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = blocker.Rollback() })
			_, err = blocker.ExecContext(ctx,
				`SELECT 1 FROM blob_object_deletes WHERE did = $1 AND cid = $2 FOR UPDATE`, testDID, cid)
			require.NoError(t, err)

			sweepDone := make(chan error, 1)
			go func() { sweepDone <- env.Store.SweepObjectDeletes(ctx) }()
			waitUntil(t, "the sweeper to wait on its queue-row delete", func() bool {
				return sessionWaitsOnLock(t, database, "DELETE FROM blob_object_deletes%")
			})

			deleteDone := make(chan error, 1)
			go func() { deleteDone <- deleter.delete(ctx, env, store, manager) }()
			deletedFirst := false
			waitUntil(t, "the deleter to delete or wait on a lock", func() bool {
				select {
				case <-deleted:
					deletedFirst = true
					return true
				default:
				}
				return sessionWaitsOnLock(t, database, "%DELETE FROM blobs WHERE%")
			})

			require.NoError(t, blocker.Rollback())
			select {
			case err := <-sweepDone:
				require.NoError(t, err, "the sweep")
			case <-ctx.Done():
				t.Fatal("the sweep did not finish")
			}
			if !deletedFirst {
				select {
				case <-deleted:
				case <-ctx.Done():
					t.Fatal("the deleter did not finish its delete")
				}
			}
			releaseDeleter()
			select {
			case err := <-deleteDone:
				require.NoError(t, err, "the deleter")
			case <-ctx.Done():
				t.Fatal("the deleter did not finish")
			}

			_, _, _, found := blobRow(t, database, testDID, cid)
			assert.False(t, found, "the blobs row is deleted")
			assert.Empty(t, queueRows(t, database), "no queue row is left behind")
			assert.Empty(t, bucketKeys(t, env.Bucket), "the object is deleted from the bucket")
		})
	}
}
