package labels_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/labeling"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/labels"
	"tidepool/internal/labels/labelstest"
	"tidepool/internal/testutil"
)

// staticResolver answers every Resolve with one endpoint and key.
type staticResolver struct {
	endpoint string
	key      atcrypto.PublicKey
}

func (r staticResolver) Resolve(_ context.Context, did string) (labels.Labeler, error) {
	return labels.Labeler{DID: did, Endpoint: r.endpoint, Key: r.key}, nil
}

// countingApplier records the seq of every label it applies; while failOn
// is set it fails the label at that seq.
type countingApplier struct {
	mu      sync.Mutex
	applied []int64
	failOn  int64
}

func (a *countingApplier) ApplyLabel(_ context.Context, _ *sql.Tx, l labels.Stored) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.failOn != 0 && l.Seq == a.failOn {
		return errors.New("counting applier: refused")
	}
	a.applied = append(a.applied, l.Seq)
	return nil
}

func (a *countingApplier) seqs() []int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]int64(nil), a.applied...)
}

func (a *countingApplier) setFailOn(seq int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.failOn = seq
}

// newLabelDB clears the label tables of the migrated test database.
func newLabelDB(t *testing.T) *sql.DB {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database, "coves_labels", "coves_label_cursor")
	return database
}

// storedPositions is every stored label's (epoch, seq), in stream order.
func storedPositions(t *testing.T, database *sql.DB) []labels.Cursor {
	t.Helper()
	rows, err := database.Query(`SELECT reset_epoch, seq FROM coves_labels ORDER BY reset_epoch, seq`)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	positions := []labels.Cursor{}
	for rows.Next() {
		var at labels.Cursor
		require.NoError(t, rows.Scan(&at.Epoch, &at.Seq))
		positions = append(positions, at)
	}
	require.NoError(t, rows.Err())
	return positions
}

// awaitStoreCursor polls the store until its cursor is want, Run returns,
// or the bound passes; the caller asserts on what was stored.
func awaitStoreCursor(store labels.Store, returned <-chan struct{}, want labels.Cursor) {
	deadline := time.Now().Add(waitBound)
	for time.Now().Before(deadline) {
		if at, err := store.Cursor(context.Background()); err == nil && at == want {
			return
		}
		select {
		case <-returned:
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// subjectAt is the record URI the label at seq names, one subject per seq.
func subjectAt(seq int64) string {
	return fmt.Sprintf("at://did:plc:ewvi7nxzyoun6zhxrhs64oiz/social.coves.community.post/3m2abcdefg%03d", seq)
}

// TestPostgresStore_ConsumerResumesFromTheStoredCursorAfterARestart (B10):
// a consumer on the Postgres store commits seq 1..3 and stops; a new consumer
// on the same database resumes with cursor 3 and commits seq 4; every seq is
// stored and applied exactly once.
func TestPostgresStore_ConsumerResumesFromTheStoredCursorAfterARestart(t *testing.T) {
	database := newLabelDB(t)
	key := labelstest.NewKey(t)
	frame := func(seq int64) labelstest.Frame {
		return labelstest.Labels(t, seq, labelstest.Sign(t, key, labeling.Label{
			SourceDID: consumerLabelerDID, URI: subjectAt(seq), Val: "!takedown",
		}))
	}
	server := labelstest.NewServer(t, func(cursor int64) labelstest.Reply {
		switch cursor {
		case 0:
			return labelstest.Reply{Frames: []labelstest.Frame{frame(1), frame(2), frame(3)}, Hold: true}
		case 3:
			return labelstest.Reply{Frames: []labelstest.Frame{frame(4)}, Hold: true}
		default:
			return labelstest.Reply{Hold: true}
		}
	})
	resolver := staticResolver{endpoint: server.URL, key: labelstest.PublicKey(t, key)}
	applier := &countingApplier{}

	first := labels.NewPostgresStore(database, applier)
	_, returned, stop := runConsumer(t, first, resolver)
	awaitStoreCursor(first, returned, labels.Cursor{Epoch: 0, Seq: 3})
	stop()

	second := labels.NewPostgresStore(database, applier)
	_, returned, stop = runConsumer(t, second, resolver)
	awaitStoreCursor(second, returned, labels.Cursor{Epoch: 0, Seq: 4})
	stop()

	cursor, err := second.Cursor(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []int64{0, 3}, server.Cursors(), "the restarted consumer resumes from the stored cursor")
	assert.Equal(t, labels.Cursor{Epoch: 0, Seq: 4}, cursor)
	assert.Equal(t, []labels.Cursor{{Epoch: 0, Seq: 1}, {Epoch: 0, Seq: 2}, {Epoch: 0, Seq: 3}, {Epoch: 0, Seq: 4}},
		storedPositions(t, database), "the stored labels are exactly seq 1..4")
	assert.Equal(t, []int64{1, 2, 3, 4}, applier.seqs(), "each seq is applied once")
}

// TestPostgresStore_CommitIsAtomicWithItsAppliers (B14): when an applier
// fails, the label is not stored and the cursor does not move; the retry,
// with the applier succeeding, commits.
func TestPostgresStore_CommitIsAtomicWithItsAppliers(t *testing.T) {
	database := newLabelDB(t)
	ctx := context.Background()
	key := labelstest.NewKey(t)
	label := func(seq int64) labeling.Label {
		return labelstest.Sign(t, key, labeling.Label{SourceDID: consumerLabelerDID, URI: subjectAt(seq), Val: "!takedown"})
	}
	applier := &countingApplier{failOn: 2}
	store := labels.NewPostgresStore(database, applier)

	require.NoError(t, store.Commit(ctx, labels.Cursor{Epoch: 0, Seq: 1}, label(1)))
	assert.Error(t, store.Commit(ctx, labels.Cursor{Epoch: 0, Seq: 2}, label(2)), "a failing applier fails the commit")

	cursor, err := store.Cursor(ctx)
	require.NoError(t, err)
	assert.Equal(t, labels.Cursor{Epoch: 0, Seq: 1}, cursor, "the failed commit leaves the cursor at seq 1")
	assert.Equal(t, []labels.Cursor{{Epoch: 0, Seq: 1}}, storedPositions(t, database), "the failed commit stores no label")

	applier.setFailOn(0)
	require.NoError(t, store.Commit(ctx, labels.Cursor{Epoch: 0, Seq: 2}, label(2)), "the retry commits")

	cursor, err = store.Cursor(ctx)
	require.NoError(t, err)
	assert.Equal(t, labels.Cursor{Epoch: 0, Seq: 2}, cursor)
	assert.Equal(t, []labels.Cursor{{Epoch: 0, Seq: 1}, {Epoch: 0, Seq: 2}}, storedPositions(t, database))
	assert.Equal(t, []int64{1, 2}, applier.seqs())
}

// TestPostgresStore_ResetStoresReplayedSeqsBesideEarlierEpochRows (B11): a
// reset restarts seq at 1, so the replay's (1, 1) and (1, 2) are stored
// beside the epoch-0 row at seq 1 with no unique violation, and the cursor
// ends at (1, 2).
func TestPostgresStore_ResetStoresReplayedSeqsBesideEarlierEpochRows(t *testing.T) {
	database := newLabelDB(t)
	ctx := context.Background()
	key := labelstest.NewKey(t)
	label := func(seq int64) labeling.Label {
		return labelstest.Sign(t, key, labeling.Label{SourceDID: consumerLabelerDID, URI: subjectAt(seq), Val: "!takedown"})
	}
	store := labels.NewPostgresStore(database)

	require.NoError(t, store.Commit(ctx, labels.Cursor{Epoch: 0, Seq: 1}, label(1)))
	reset, err := store.Reset(ctx)
	require.NoError(t, err)
	require.Equal(t, labels.Cursor{Epoch: 1, Seq: 0}, reset)
	require.NoError(t, store.Commit(ctx, labels.Cursor{Epoch: 1, Seq: 1}, label(1)), "the replayed seq 1 is stored beside epoch 0's seq 1")
	require.NoError(t, store.Commit(ctx, labels.Cursor{Epoch: 1, Seq: 2}, label(2)))

	cursor, err := store.Cursor(ctx)
	require.NoError(t, err)
	assert.Equal(t, labels.Cursor{Epoch: 1, Seq: 2}, cursor)
	assert.Equal(t, []labels.Cursor{{Epoch: 0, Seq: 1}, {Epoch: 1, Seq: 1}, {Epoch: 1, Seq: 2}},
		storedPositions(t, database), "both epochs' rows are stored")
}

// standingApplier reads the subject's standing state in the label's
// transaction, as the materializer's applier does (StandingTx casts exp).
type standingApplier struct{}

func (standingApplier) ApplyLabel(ctx context.Context, tx *sql.Tx, l labels.Stored) error {
	_, err := labels.StandingTx(ctx, tx, l.URI, l.Val)
	return err
}

// TestPostgresStore_UnusableExpIsSkippedNotRetriedForever: a signed label
// whose exp passes atproto datetime syntax but is outside what Postgres can
// cast (year 0000, an offset of 16 hours or more) is not stored or applied;
// the cursor moves past it, the consumer counts it, the next label commits,
// and the lapse check keeps working.
func TestPostgresStore_UnusableExpIsSkippedNotRetriedForever(t *testing.T) {
	database := newLabelDB(t)
	key := labelstest.NewKey(t)
	yearZero := "0000-01-01T00:00:00Z"
	farOffset := "2027-01-01T00:00:00+16:00"
	frame := func(seq int64, exp *string) labelstest.Frame {
		return labelstest.Labels(t, seq, labelstest.Sign(t, key, labeling.Label{
			SourceDID: consumerLabelerDID, URI: subjectAt(seq), Val: "!takedown", ExpiresAt: exp,
		}))
	}
	server := labelstest.NewServer(t, func(cursor int64) labelstest.Reply {
		switch cursor {
		case 0:
			return labelstest.Reply{Frames: []labelstest.Frame{frame(1, &yearZero), frame(2, &farOffset), frame(3, nil)}, Hold: true}
		default:
			return labelstest.Reply{Hold: true}
		}
	})
	store := labels.NewPostgresStore(database, standingApplier{})
	consumer, returned, stop := runConsumer(t, store, staticResolver{endpoint: server.URL, key: labelstest.PublicKey(t, key)})
	awaitStoreCursor(store, returned, labels.Cursor{Epoch: 0, Seq: 3})
	stop()

	cursor, err := store.Cursor(context.Background())
	require.NoError(t, err)
	assert.Equal(t, labels.Cursor{Epoch: 0, Seq: 3}, cursor, "the cursor moves past the unusable labels")
	assert.Equal(t, []labels.Cursor{{Epoch: 0, Seq: 3}}, storedPositions(t, database), "only the usable label is stored")
	assert.Equal(t, uint64(2), consumer.Stats().UnusableLabels, "each skipped label is counted")
	_, err = store.ApplyLapsed(context.Background())
	assert.NoError(t, err, "the lapse check runs")
}
