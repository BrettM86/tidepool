package consume

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/testutil"
)

// Task 14 cycle A: the consumer's own state — cursors and the dead letter
// queue. These mirror the Coves AppView's shapes (state_store.go) with one
// deliberate divergence: cursors are keyed (consumer_name, schema_version).
//
// Why the version matters, learned the hard way (FOLLOWUPS): a cursor sitting
// between Jetstream's newest stored event and now replays the ENTIRE retained
// store. A future incompatible handler must be able to do that replay without
// stomping the production cursor — so the two rows coexist.

// consumeStateTestDB returns a migrated connection with the consumer's own
// tables emptied.
func consumeStateTestDB(t *testing.T) *sql.DB {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database, "consumer_cursors", "jetstream_dead_letters")
	return database
}

const otherConsumer = "other"

// ---------------------------------------------------------------------------
// consumer_cursors
// ---------------------------------------------------------------------------

func TestCursorStore_MissingCursorIsZero(t *testing.T) {
	database := consumeStateTestDB(t)
	store := NewPostgresStateStore(database, CursorSchemaVersion)
	ctx := context.Background()

	cursor, err := store.GetCursor(ctx, ConsumerNative)
	require.NoError(t, err, "a first run must not be an error, it must be a live tail")
	assert.Equal(t, int64(0), cursor)
}

func TestCursorStore_SaveAndGetRoundTrip(t *testing.T) {
	database := consumeStateTestDB(t)
	store := NewPostgresStateStore(database, CursorSchemaVersion)
	ctx := context.Background()

	require.NoError(t, store.SaveCursor(ctx, ConsumerNative, 1_700_000_000_000_000))

	cursor, err := store.GetCursor(ctx, ConsumerNative)
	require.NoError(t, err)
	assert.Equal(t, int64(1_700_000_000_000_000), cursor)

	// Consumers are isolated from each other.
	other, err := store.GetCursor(ctx, otherConsumer)
	require.NoError(t, err)
	assert.Equal(t, int64(0), other, "another consumer's cursor must be untouched")

	// The row is keyed by BOTH columns.
	var count int
	require.NoError(t, database.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM consumer_cursors WHERE consumer_name = $1 AND schema_version = $2`,
		ConsumerNative, CursorSchemaVersion).Scan(&count),
		"consumer_cursors must be keyed (consumer_name, schema_version)")
	assert.Equal(t, 1, count)
}

func TestCursorStore_SaveIsMonotonic(t *testing.T) {
	database := consumeStateTestDB(t)
	store := NewPostgresStateStore(database, CursorSchemaVersion)
	ctx := context.Background()

	require.NoError(t, store.SaveCursor(ctx, ConsumerNative, 2_000))

	// An out-of-order flush (a late goroutine, a reconnect rewind) must never
	// walk the cursor backwards: that would replay events the consumer has
	// already accounted for, or worse, un-advance past a poison frame.
	require.NoError(t, store.SaveCursor(ctx, ConsumerNative, 1_000),
		"saving a smaller cursor is a no-op, not an error")
	cursor, err := store.GetCursor(ctx, ConsumerNative)
	require.NoError(t, err)
	assert.Equal(t, int64(2_000), cursor, "a smaller cursor must not rewind the stored one")

	require.NoError(t, store.SaveCursor(ctx, ConsumerNative, 3_000))
	cursor, err = store.GetCursor(ctx, ConsumerNative)
	require.NoError(t, err)
	assert.Equal(t, int64(3_000), cursor, "a greater cursor advances")
}

func TestCursorStore_SchemaVersionsCoexist(t *testing.T) {
	database := consumeStateTestDB(t)
	ctx := context.Background()

	current := NewPostgresStateStore(database, CursorSchemaVersion)
	next := NewPostgresStateStore(database, CursorSchemaVersion+1)

	require.NoError(t, current.SaveCursor(ctx, ConsumerNative, 9_000))

	// The next handler version starts from scratch — it has never processed
	// anything, so it must NOT inherit the production cursor.
	cursor, err := next.GetCursor(ctx, ConsumerNative)
	require.NoError(t, err)
	assert.Equal(t, int64(0), cursor,
		"a new schema version starts at 0: inheriting the old cursor would skip the "+
			"replay the version bump exists to perform")

	// And its own replay must not stomp the production cursor, even though it
	// is far behind.
	require.NoError(t, next.SaveCursor(ctx, ConsumerNative, 100))

	cursor, err = current.GetCursor(ctx, ConsumerNative)
	require.NoError(t, err)
	assert.Equal(t, int64(9_000), cursor,
		"the production cursor must survive another version's replay")

	cursor, err = next.GetCursor(ctx, ConsumerNative)
	require.NoError(t, err)
	assert.Equal(t, int64(100), cursor)

	var rows int
	require.NoError(t, database.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM consumer_cursors WHERE consumer_name = $1`, ConsumerNative).Scan(&rows))
	assert.Equal(t, 2, rows, "two schema versions means two rows for one consumer name")
}

// ---------------------------------------------------------------------------
// jetstream_dead_letters
// ---------------------------------------------------------------------------

func TestDeadLetters_ReAddingTheSameEventIsANoOp(t *testing.T) {
	database := consumeStateTestDB(t)
	store := NewPostgresStateStore(database, CursorSchemaVersion)
	ctx := context.Background()

	payload := []byte(`{"kind":"commit","time_us":42,"commit":{"operation":"create"}}`)

	require.NoError(t, store.AddDeadLetter(ctx, ConsumerNative, 42, payload, "boom", 0))
	// A poison event replayed by the reconnect rewind hits the dedup index.
	// It MUST succeed as a no-op: an error here would block the cursor
	// forever on an event that is already safely captured.
	require.NoError(t, store.AddDeadLetter(ctx, ConsumerNative, 42, payload, "boom again", 0),
		"re-adding an already-captured event must be a no-op success so the cursor can advance")

	counts, err := store.CountDeadLetters(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), counts[ConsumerNative], "one row, not two")

	// A different payload at the same time_us is a different event.
	require.NoError(t, store.AddDeadLetter(ctx, ConsumerNative, 42,
		[]byte(`{"kind":"commit","time_us":42,"commit":{"operation":"delete"}}`), "boom", 0))
	counts, err = store.CountDeadLetters(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), counts[ConsumerNative],
		"the dedup key is (consumer, time_us, payload) — a different payload is a different event")

	// And so is the same payload under a different consumer.
	require.NoError(t, store.AddDeadLetter(ctx, otherConsumer, 42, payload, "boom", 0))
	counts, err = store.CountDeadLetters(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), counts[ConsumerNative])
	assert.Equal(t, int64(1), counts[otherConsumer], "the backlog is reported per consumer")
}

func TestDeadLetters_ListRetryableExcludesExhausted(t *testing.T) {
	database := consumeStateTestDB(t)
	store := NewPostgresStateStore(database, CursorSchemaVersion)
	ctx := context.Background()

	transient := []byte(`{"kind":"commit","time_us":1}`)
	permanent := []byte(`{"kind":"commit","time_us":2}`)

	require.NoError(t, store.AddDeadLetter(ctx, ConsumerNative, 1, transient, "pg blip", 0))
	// A permanent failure is captured with its budget already spent: replaying
	// a validation rejection can never succeed, so the redriver must never
	// pick it up. It stays for forensics.
	require.NoError(t, store.AddDeadLetter(ctx, ConsumerNative, 2, permanent,
		"lexicon rejection", MaxRedriveAttempts))

	retryable, err := store.ListRetryable(ctx, ConsumerNative, MaxRedriveAttempts, 10)
	require.NoError(t, err)
	require.Len(t, retryable, 1,
		"only the transient failure is retryable; the permanent one is already exhausted")
	assert.Equal(t, int64(1), retryable[0].EventTimeUS)
	assert.Equal(t, transient, retryable[0].EventData, "the RAW frame is what gets replayed")
	assert.Equal(t, "pg blip", retryable[0].LastError)
	assert.Equal(t, 0, retryable[0].Attempts)
	assert.Equal(t, ConsumerNative, retryable[0].ConsumerName)
	assert.NotZero(t, retryable[0].ID)

	// The exhausted row still counts toward the backlog operators watch.
	counts, err := store.CountDeadLetters(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), counts[ConsumerNative],
		"an exhausted row is skipped by the redriver but stays in the backlog count")

	// Another consumer's backlog is never handed to this one's redriver.
	require.NoError(t, store.AddDeadLetter(ctx, otherConsumer, 3, []byte(`{"time_us":3}`), "x", 0))
	retryable, err = store.ListRetryable(ctx, ConsumerNative, MaxRedriveAttempts, 10)
	require.NoError(t, err)
	assert.Len(t, retryable, 1, "ListRetryable is scoped to one consumer")
}

func TestDeadLetters_ListRetryableIsOldestFirstAndBounded(t *testing.T) {
	database := consumeStateTestDB(t)
	store := NewPostgresStateStore(database, CursorSchemaVersion)
	ctx := context.Background()

	for i := int64(1); i <= 3; i++ {
		require.NoError(t, store.AddDeadLetter(ctx, ConsumerNative, i,
			[]byte(`{"time_us":`+string(rune('0'+i))+`}`), "boom", 0))
	}

	all, err := store.ListRetryable(ctx, ConsumerNative, MaxRedriveAttempts, 10)
	require.NoError(t, err)
	require.Len(t, all, 3)
	assert.Equal(t, int64(1), all[0].EventTimeUS, "oldest first: events replay in arrival order")
	assert.Equal(t, int64(3), all[2].EventTimeUS)

	page, err := store.ListRetryable(ctx, ConsumerNative, MaxRedriveAttempts, 2)
	require.NoError(t, err)
	assert.Len(t, page, 2, "limit bounds the batch")
}

func TestDeadLetters_MarkRedriveAttempt(t *testing.T) {
	database := consumeStateTestDB(t)
	store := NewPostgresStateStore(database, CursorSchemaVersion)
	ctx := context.Background()

	require.NoError(t, store.AddDeadLetter(ctx, ConsumerNative, 1, []byte(`{"time_us":1}`), "first", 0))
	listed, err := store.ListRetryable(ctx, ConsumerNative, MaxRedriveAttempts, 10)
	require.NoError(t, err)
	require.Len(t, listed, 1)

	require.NoError(t, store.MarkRedriveAttempt(ctx, listed[0].ID, "still failing"))

	listed, err = store.ListRetryable(ctx, ConsumerNative, MaxRedriveAttempts, 10)
	require.NoError(t, err)
	require.Len(t, listed, 1, "one burnt attempt does not exhaust the budget")
	assert.Equal(t, 1, listed[0].Attempts)
	assert.Equal(t, "still failing", listed[0].LastError, "the newest error replaces the old one")

	// maxAttempts is the caller's cutoff, evaluated against the counter.
	listed, err = store.ListRetryable(ctx, ConsumerNative, 1, 10)
	require.NoError(t, err)
	assert.Empty(t, listed, "attempts >= maxAttempts is excluded")
}

func TestDeadLetters_RetireExhaustsInOneStep(t *testing.T) {
	database := consumeStateTestDB(t)
	store := NewPostgresStateStore(database, CursorSchemaVersion)
	ctx := context.Background()

	require.NoError(t, store.AddDeadLetter(ctx, ConsumerNative, 1, []byte(`not json at all`), "parse", 0))
	listed, err := store.ListRetryable(ctx, ConsumerNative, MaxRedriveAttempts, 10)
	require.NoError(t, err)
	require.Len(t, listed, 1)

	// An unparseable payload can never succeed. Retiring it must cost ONE
	// call, not MaxRedriveAttempts redrive passes.
	require.NoError(t, store.RetireDeadLetter(ctx, listed[0].ID, "unparseable event"))

	retryable, err := store.ListRetryable(ctx, ConsumerNative, MaxRedriveAttempts, 10)
	require.NoError(t, err)
	assert.Empty(t, retryable, "a retired dead letter is exhausted after exactly one call")

	counts, err := store.CountDeadLetters(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), counts[ConsumerNative],
		"the row is KEPT for forensics and stays in the backlog count")
}

func TestDeadLetters_DeleteRemovesRedrivenEvent(t *testing.T) {
	database := consumeStateTestDB(t)
	store := NewPostgresStateStore(database, CursorSchemaVersion)
	ctx := context.Background()

	require.NoError(t, store.AddDeadLetter(ctx, ConsumerNative, 1, []byte(`{"time_us":1}`), "boom", 0))
	listed, err := store.ListRetryable(ctx, ConsumerNative, MaxRedriveAttempts, 10)
	require.NoError(t, err)
	require.Len(t, listed, 1)

	require.NoError(t, store.DeleteDeadLetter(ctx, listed[0].ID))

	counts, err := store.CountDeadLetters(ctx)
	require.NoError(t, err)
	assert.Zero(t, counts[ConsumerNative], "a successfully redriven event leaves the queue")

	require.NoError(t, store.DeleteDeadLetter(ctx, listed[0].ID),
		"deleting an already-deleted dead letter is a no-op success")
}
