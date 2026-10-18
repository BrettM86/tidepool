package repo

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

const identityTestHandle = "alice.lemmy.world.ap.tidepool.test"

func TestAppendIdentityEventTx(t *testing.T) {
	t.Run("commit persists and notifies", func(t *testing.T) {
		manager, database, _ := testManager(t)
		ctx := t.Context()
		listener := pq.NewListener(os.Getenv("TIDEPOOL_TEST_DATABASE_URL"), 100*time.Millisecond, time.Second, nil)
		t.Cleanup(func() { _ = listener.Close() })
		require.NoError(t, listener.Listen(FirehoseNotifyChannel))

		tx, err := database.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		seq, err := AppendIdentityEventTx(ctx, tx, testDID, identityTestHandle)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		events, err := manager.ListEvents(ctx, 0, 10)
		require.NoError(t, err)
		require.Len(t, events, 1)
		require.Equal(t, EventKindIdentity, events[0].Kind)
		require.Equal(t, seq, events[0].Seq)
		require.Equal(t, testDID, events[0].DID)
		require.Equal(t, identityTestHandle, events[0].Handle)
		select {
		case notification := <-listener.Notify:
			require.NotNil(t, notification)
			require.Equal(t, FirehoseNotifyChannel, notification.Channel)
			require.Equal(t, fmt.Sprintf("%d", seq), notification.Extra)
		case <-time.After(5 * time.Second):
			t.Fatal("no identity notification received")
		}
	})

	t.Run("rollback hides row and notification", func(t *testing.T) {
		manager, database, _ := testManager(t)
		ctx := t.Context()
		listener := pq.NewListener(os.Getenv("TIDEPOOL_TEST_DATABASE_URL"), 100*time.Millisecond, time.Second, nil)
		t.Cleanup(func() { _ = listener.Close() })
		require.NoError(t, listener.Listen(FirehoseNotifyChannel))

		tx, err := database.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		rolledBackSeq, err := AppendIdentityEventTx(ctx, tx, testDID, identityTestHandle)
		require.NoError(t, err)
		require.Greater(t, rolledBackSeq, int64(0))
		require.NoError(t, tx.Rollback())
		events, err := manager.ListEvents(ctx, 0, 10)
		require.NoError(t, err)
		require.Empty(t, events)

		// The first notification after rollback must belong to a later write.
		sentinel, err := manager.PutRecord(ctx, testDID, testCollection, testRKey(1), testRecord("sentinel"))
		require.NoError(t, err)
		require.Greater(t, sentinel.Seq, rolledBackSeq)
		select {
		case notification := <-listener.Notify:
			require.NotNil(t, notification)
			require.Equal(t, FirehoseNotifyChannel, notification.Channel)
			require.Equal(t, fmt.Sprintf("%d", sentinel.Seq), notification.Extra)
		case <-time.After(5 * time.Second):
			t.Fatal("no sentinel notification received")
		}
	})

	for _, tc := range []struct{ name, did, handle string }{
		{"invalid DID", "not-a-did", identityTestHandle},
		{"invalid handle", testDID, "invalid handle"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager, database, _ := testManager(t)
			ctx := t.Context()
			tx, err := database.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer tx.Rollback()
			_, err = AppendIdentityEventTx(ctx, tx, tc.did, tc.handle)
			require.True(t, errors.IsValidation(err), "expected validation error, got %v", err)
			require.NoError(t, tx.Commit())
			events, err := manager.ListEvents(ctx, 0, 10)
			require.NoError(t, err)
			require.Empty(t, events)
		})
	}
}

// signingGate holds a commit after it has taken the global commit lock but
// before it allocates a firehose seq.
type signingGate struct {
	key     *atcrypto.PrivateKeyK256
	entered chan struct{}
	release chan struct{}
}

func (g *signingGate) SigningKey(ctx context.Context, _ string, _ KeyUse) (atcrypto.PrivateKey, error) {
	select {
	case g.entered <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-g.release:
		return g.key, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type identityAppendResult struct {
	seq int64
	err error
}

type recordPutResult struct {
	result *CommitResult
	err    error
}

// This observes only waiters for the commit key in this database: testutil's
// unrelated package lock is not evidence that either writer is serialized.
func commitLockWaiter(ctx context.Context, database *sql.DB) (bool, error) {
	var waiting bool
	err := database.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_locks
			WHERE locktype = 'advisory' AND NOT granted
			  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
			  AND classid::bigint = $1 AND objid::bigint = $2
		)`, uint64(commitAdvisoryLockKey)>>32, uint64(commitAdvisoryLockKey)&0xffffffff).Scan(&waiting)
	return waiting, err
}

func requireNoCursorSkip(t *testing.T, events []*Event, pendingSeq int64) {
	t.Helper()
	for _, ev := range events {
		if ev.Seq <= pendingSeq {
			continue
		}
		var seenPending bool
		for _, visible := range events {
			seenPending = seenPending || visible.Seq == pendingSeq
		}
		require.True(t, seenPending, "visible seq %d skipped uncommitted identity seq %d", ev.Seq, pendingSeq)
	}
}

func TestIdentityAndCommitVisibilityOrder(t *testing.T) {
	for _, schedule := range []string{"identity-first", "commit-first"} {
		t.Run(schedule, func(t *testing.T) {
			manager, database, key := testManager(t)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			_, base, err := manager.SeqBounds(ctx)
			require.NoError(t, err)

			if schedule == "identity-first" {
				tx, err := database.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer tx.Rollback()
				identitySeq, err := AppendIdentityEventTx(ctx, tx, testDID, identityTestHandle)
				require.NoError(t, err)
				puts := make(chan recordPutResult, 1)
				go func() {
					result, putErr := manager.PutRecord(ctx, testDID, testCollection, testRKey(1), testRecord("after identity"))
					puts <- recordPutResult{result, putErr}
				}()
				var put *recordPutResult
				require.Eventually(t, func() bool {
					select {
					case completed := <-puts:
						put = &completed
						return true
					default:
					}
					waiting, queryErr := commitLockWaiter(ctx, database)
					return queryErr == nil && waiting
				}, 5*time.Second, 10*time.Millisecond)
				visible, err := manager.ListEvents(ctx, base, 10)
				require.NoError(t, err)
				requireNoCursorSkip(t, visible, identitySeq)
				require.NoError(t, tx.Commit())
				if put == nil {
					completed := <-puts
					put = &completed
				}
				require.NoError(t, put.err)
				require.NotNil(t, put.result)
				require.Less(t, identitySeq, put.result.Seq)
				visible, err = manager.ListEvents(ctx, base, 10)
				require.NoError(t, err)
				require.Len(t, visible, 2)
				require.Equal(t, []string{EventKindIdentity, EventKindCommit}, []string{visible[0].Kind, visible[1].Kind})
				require.Equal(t, []int64{identitySeq, put.result.Seq}, []int64{visible[0].Seq, visible[1].Seq})
				return
			}

			gate := &signingGate{key: key, entered: make(chan struct{}, 1), release: make(chan struct{})}
			t.Cleanup(func() {
				select {
				case <-gate.release:
				default:
					close(gate.release)
				}
			})
			gatedManager, err := NewManager(database, gate, nil)
			require.NoError(t, err)
			puts := make(chan recordPutResult, 1)
			go func() {
				result, putErr := gatedManager.PutRecord(ctx, testDID, testCollection, testRKey(1), testRecord("before identity"))
				puts <- recordPutResult{result, putErr}
			}()
			select {
			case <-gate.entered:
			case <-ctx.Done():
				t.Fatal("commit never reached signing gate")
			}
			tx, err := database.BeginTx(ctx, nil)
			require.NoError(t, err)
			defer tx.Rollback()
			appends := make(chan identityAppendResult, 1)
			go func() {
				seq, appendErr := AppendIdentityEventTx(ctx, tx, testDID, identityTestHandle)
				appends <- identityAppendResult{seq, appendErr}
			}()
			var appendResult *identityAppendResult
			require.Eventually(t, func() bool {
				select {
				case completed := <-appends:
					appendResult = &completed
					return true
				default:
				}
				waiting, queryErr := commitLockWaiter(ctx, database)
				return queryErr == nil && waiting
			}, 5*time.Second, 10*time.Millisecond)
			close(gate.release)
			put := <-puts
			require.NoError(t, put.err)
			require.NotNil(t, put.result)
			if appendResult == nil {
				completed := <-appends
				appendResult = &completed
			}
			require.NoError(t, appendResult.err)
			visible, err := manager.ListEvents(ctx, base, 10)
			require.NoError(t, err)
			requireNoCursorSkip(t, visible, appendResult.seq)
			require.NoError(t, tx.Commit())
			require.Less(t, put.result.Seq, appendResult.seq)
			visible, err = manager.ListEvents(ctx, base, 10)
			require.NoError(t, err)
			require.Len(t, visible, 2)
			require.Equal(t, []string{EventKindCommit, EventKindIdentity}, []string{visible[0].Kind, visible[1].Kind})
			require.Equal(t, []int64{put.result.Seq, appendResult.seq}, []int64{visible[0].Seq, visible[1].Seq})
		})
	}
}
