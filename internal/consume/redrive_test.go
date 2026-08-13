package consume

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Task 14 cycle R: the redriver. This is what turns the dead letter queue from
// a graveyard into a recovery mechanism — a postgres blip captures an event,
// the blip ends, and the next pass indexes it without anyone being paged.

func redriveTestDB(t *testing.T) *sql.DB {
	t.Helper()
	return connectorTestDB(t) // same tables, same truncation
}

// runRedriver starts a redriver whose tick is far beyond the test's lifetime,
// so anything that happens is the BOOT pass, not a tick.
func runRedriver(t *testing.T, r *DeadLetterRedriver) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("redriver did not stop within 5s of context cancellation")
		}
	})
}

func TestRedriver_ReplaysARetryableEventAndDeletesItOnSuccess(t *testing.T) {
	database := redriveTestDB(t)
	state := NewPostgresStateStore(database, CursorSchemaVersion)
	ctx := context.Background()

	frame := connFrame(4_100, "3lzrev0000001", "aaa")
	require.NoError(t, state.AddDeadLetter(ctx, ConsumerNative, 4_100, frame, "postgres blip", 0))

	handler := &recordingHandler{}
	runRedriver(t, NewDeadLetterRedriver(state,
		map[string]EventHandler{ConsumerNative: handler},
		WithRedriveInterval(time.Hour)))

	waitFor(t, "the dead letter to be replayed through its consumer's handler", func() bool {
		return handler.Calls() >= 1
	})

	events := handler.Events()
	require.Len(t, events, 1)
	assert.Equal(t, int64(4_100), events[0].TimeUS,
		"the redriver replays the raw frame it captured, parsed back into an event")
	require.NotNil(t, events[0].Commit)
	assert.Equal(t, CollectionComment, events[0].Commit.Collection)
	assert.Equal(t, "3lzrev0000001", events[0].Commit.Rev,
		"the stored frame keeps its rev, so a redriven event is still rev-gated")

	waitFor(t, "the successfully redriven row to be removed", func() bool {
		return countRows(t, database, "jetstream_dead_letters") == 0
	})
}

func TestRedriver_FailedRedriveBurnsOneAttemptAndKeepsTheRow(t *testing.T) {
	database := redriveTestDB(t)
	state := NewPostgresStateStore(database, CursorSchemaVersion)
	ctx := context.Background()

	require.NoError(t, state.AddDeadLetter(ctx, ConsumerNative, 4_200,
		connFrame(4_200, "3lzrev0000001", "aaa"), "first failure", 0))

	// Fails on the boot pass, then would succeed — but the interval is an hour,
	// so only the boot pass runs and exactly one attempt is burnt.
	handler := &recordingHandler{err: fmt.Errorf("still broken"), failFor: 1}
	runRedriver(t, NewDeadLetterRedriver(state,
		map[string]EventHandler{ConsumerNative: handler},
		WithRedriveInterval(time.Hour)))

	waitFor(t, "the failed redrive attempt to be recorded", func() bool {
		dead, err := state.ListRetryable(ctx, ConsumerNative, MaxRedriveAttempts, 10)
		return err == nil && len(dead) == 1 && dead[0].Attempts == 1
	})

	dead, err := state.ListRetryable(ctx, ConsumerNative, MaxRedriveAttempts, 10)
	require.NoError(t, err)
	require.Len(t, dead, 1, "a failed redrive keeps the row: the event is not lost")
	assert.Equal(t, 1, dead[0].Attempts, "exactly one attempt per pass, not a spin")
	assert.Contains(t, dead[0].LastError, "still broken",
		"the newest failure replaces the captured one, so the row shows why it is STILL failing")
}

func TestRedriver_UnparseablePayloadIsRetiredInOneStep(t *testing.T) {
	database := redriveTestDB(t)
	state := NewPostgresStateStore(database, CursorSchemaVersion)
	ctx := context.Background()

	require.NoError(t, state.AddDeadLetter(ctx, ConsumerNative, 0,
		[]byte(`{"kind":"commit","time_us":`), "failed to parse event", 0))

	handler := &recordingHandler{}
	runRedriver(t, NewDeadLetterRedriver(state,
		map[string]EventHandler{ConsumerNative: handler},
		WithRedriveInterval(time.Hour)))

	waitFor(t, "the unparseable row to be retired", func() bool {
		dead, err := state.ListRetryable(ctx, ConsumerNative, MaxRedriveAttempts, 10)
		return err == nil && len(dead) == 0
	})

	assert.Zero(t, handler.Calls(),
		"a payload that will not parse never reaches a handler")

	var attempts int
	var lastError string
	require.NoError(t, database.QueryRowContext(ctx,
		`SELECT attempts, last_error FROM jetstream_dead_letters`).Scan(&attempts, &lastError))
	assert.Equal(t, MaxRedriveAttempts, attempts,
		"a row that can never succeed is exhausted in ONE step rather than consuming "+
			"ten redrive passes to reach the same conclusion")
	assert.Contains(t, lastError, "unparseable",
		"the retirement reason replaces the original error so the row explains itself")

	counts, err := state.CountDeadLetters(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), counts[ConsumerNative],
		"the row stays for forensics and stays in the backlog operators watch")
}

func TestRedriver_RunsAnImmediatePassAtBoot(t *testing.T) {
	database := redriveTestDB(t)
	state := NewPostgresStateStore(database, CursorSchemaVersion)
	ctx := context.Background()

	require.NoError(t, state.AddDeadLetter(ctx, ConsumerNative, 4_400,
		connFrame(4_400, "3lzrev0000001", "aaa"), "captured while the process was down", 0))

	handler := &recordingHandler{}
	// An interval no test could ever wait out: if the backlog drains, it can
	// only have been the boot pass.
	runRedriver(t, NewDeadLetterRedriver(state,
		map[string]EventHandler{ConsumerNative: handler},
		WithRedriveInterval(24*time.Hour)))

	waitFor(t, "the boot pass to drain the backlog accumulated while the process was down",
		func() bool { return countRows(t, database, "jetstream_dead_letters") == 0 })

	assert.Equal(t, 1, handler.Calls(),
		"a backlog that built up during downtime must not wait out a full interval "+
			"before anyone looks at it")
}

func TestRedriver_OnlyHandsAConsumerItsOwnBacklog(t *testing.T) {
	database := redriveTestDB(t)
	state := NewPostgresStateStore(database, CursorSchemaVersion)
	ctx := context.Background()

	require.NoError(t, state.AddDeadLetter(ctx, ConsumerNative, 4_500,
		connFrame(4_500, "3lzrev0000001", "aaa"), "mine", 0))
	require.NoError(t, state.AddDeadLetter(ctx, otherConsumer, 4_600,
		connFrame(4_600, "3lzrev0000002", "bbb"), "not mine", 0))

	handler := &recordingHandler{}
	runRedriver(t, NewDeadLetterRedriver(state,
		map[string]EventHandler{ConsumerNative: handler},
		WithRedriveInterval(time.Hour)))

	waitFor(t, "this consumer's row to be redriven", func() bool {
		return handler.Calls() >= 1
	})

	// Give any (incorrect) second pass a chance to show up before asserting.
	waitFor(t, "the backlog to settle at one remaining row", func() bool {
		return countRows(t, database, "jetstream_dead_letters") == 1
	})

	assert.Equal(t, 1, handler.Calls(),
		"a consumer's handler must only ever see rows filed under ITS OWN name — "+
			"replaying another consumer's events through it would apply them wrongly")

	var owner string
	require.NoError(t, database.QueryRowContext(ctx,
		`SELECT consumer_name FROM jetstream_dead_letters`).Scan(&owner))
	assert.Equal(t, otherConsumer, owner,
		"the unregistered consumer's backlog is left untouched, not silently dropped")
}

func TestRedriver_DrainsABacklogLargerThanOneBatch(t *testing.T) {
	database := redriveTestDB(t)
	state := NewPostgresStateStore(database, CursorSchemaVersion)
	ctx := context.Background()

	const backlog = 5
	for i := 0; i < backlog; i++ {
		require.NoError(t, state.AddDeadLetter(ctx, ConsumerNative, int64(5_000+i),
			connFrame(int64(5_000+i), fmt.Sprintf("3lzrev000%04d", i), "aaa"), "blip", 0))
	}

	handler := &recordingHandler{}
	// Batch size 2 against a backlog of 5: one pass must keep claiming until
	// the backlog is drained, not clear two rows per tick.
	runRedriver(t, NewDeadLetterRedriver(state,
		map[string]EventHandler{ConsumerNative: handler},
		WithRedriveInterval(time.Hour), WithRedriveBatchSize(2)))

	waitFor(t, "one boot pass to drain a multi-batch backlog", func() bool {
		return countRows(t, database, "jetstream_dead_letters") == 0
	})
	assert.Equal(t, backlog, handler.Calls())
}
