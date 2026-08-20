package consume

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

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

// ---------------------------------------------------------------------------
// A row whose failure carries poison bytes must still burn its budget
// ---------------------------------------------------------------------------

// poisonHandler fails for ONE event time with an error carrying the bytes a
// remote body can plant — a NUL and invalid UTF-8 — and succeeds for every
// other. This is not exotic: resolver.go echoes a stranger's
// /.well-known/atproto-did body into a transient error, and that error is what
// MarkRedriveAttempt writes back into the last_error TEXT column.
type poisonHandler struct {
	mu           sync.Mutex
	poisonTimeUS int64
	calls        int
	poisonCalls  int
}

func (h *poisonHandler) HandleEvent(_ context.Context, event *JetstreamEvent) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls++
	if event.TimeUS == h.poisonTimeUS {
		h.poisonCalls++
		return fmt.Errorf("verify handle alice.coves.social: well-known claims \x00\xff\xfe, not did:plc:x")
	}
	return nil
}

func (h *poisonHandler) counts() (calls, poisonCalls int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls, h.poisonCalls
}

// TestRedriver_PoisonFailureStillBurnsItsBudgetAndUnblocksTheDrain is the
// redriver half of the NUL defense, and the reason it is CRITICAL rather than
// cosmetic.
//
// If MarkRedriveAttempt's UPDATE is rejected by postgres, `attempts` never
// increments. The row can therefore never reach MaxRedriveAttempts, so
// ListRetryable returns it again on the very next pass and the handler — DNS
// lookup, two outbound fetches and all — is fully re-executed FOREVER, with
// nothing in the attempts or backlog counters to show for it. And because it is
// always the OLDEST row, redriveAll's forward-progress guard (redriven+retired
// == 0 → break) parks the whole consumer's drain behind it: the good row
// queued after it is never reached.
//
// The passes are driven directly, one per call, so the assertion is about the
// redriver's pass semantics rather than about wall-clock time.
func TestRedriver_PoisonFailureStillBurnsItsBudgetAndUnblocksTheDrain(t *testing.T) {
	database := redriveTestDB(t)
	state := NewPostgresStateStore(database, CursorSchemaVersion)
	ctx := context.Background()

	const poisonTimeUS, healthyTimeUS = 6_000, 6_001
	// The poison row is OLDEST, so it is claimed first every pass and stands in
	// front of the healthy one.
	require.NoError(t, state.AddDeadLetter(ctx, ConsumerNative, poisonTimeUS,
		connFrame(poisonTimeUS, "3lzrev0000001", "aaa"), "first failure", 0))
	require.NoError(t, state.AddDeadLetter(ctx, ConsumerNative, healthyTimeUS,
		connFrame(healthyTimeUS, "3lzrev0000002", "bbb"), "postgres blip", 0))

	handler := &poisonHandler{poisonTimeUS: poisonTimeUS}
	// Batch size 1: one row per claim, so each pass is exactly one attempt on
	// the oldest retryable row.
	redriver := NewDeadLetterRedriver(state,
		map[string]EventHandler{ConsumerNative: handler},
		WithRedriveInterval(time.Hour), WithRedriveBatchSize(1))

	for pass := 1; pass <= MaxRedriveAttempts; pass++ {
		redriver.redriveAll(ctx)

		var attempts int
		require.NoError(t, database.QueryRowContext(ctx,
			`SELECT attempts FROM jetstream_dead_letters WHERE event_time_us = $1`,
			int64(poisonTimeUS)).Scan(&attempts))
		require.Equal(t, pass, attempts,
			"pass %d must have burnt an attempt on the poison row: a failed last_error "+
				"UPDATE leaves attempts at 0, and a row that cannot count its attempts "+
				"can never retire — it is re-handled on every pass forever", pass)
	}

	retryable, err := state.ListRetryable(ctx, ConsumerNative, MaxRedriveAttempts, 10)
	require.NoError(t, err)
	require.Len(t, retryable, 1,
		"the poison row has exhausted its budget and left the retryable set; only the "+
			"healthy row behind it is still queued")
	assert.Equal(t, int64(healthyTimeUS), retryable[0].EventTimeUS)

	// And the drain is no longer parked behind it.
	redriver.redriveAll(ctx)

	remaining, err := state.ListRetryable(ctx, ConsumerNative, MaxRedriveAttempts, 10)
	require.NoError(t, err)
	assert.Empty(t, remaining,
		"the row queued BEHIND the poison one is finally redriven — a row that can never "+
			"retire stalls the whole consumer's dead letter drain")

	_, poisonCalls := handler.counts()
	assert.Equal(t, MaxRedriveAttempts, poisonCalls,
		"the poison row costs exactly its budget of handler executions, not an unbounded "+
			"number: each replay re-runs the full handler, DNS and outbound fetches included")

	var stored string
	require.NoError(t, database.QueryRowContext(ctx,
		`SELECT last_error FROM jetstream_dead_letters WHERE event_time_us = $1`,
		int64(poisonTimeUS)).Scan(&stored))
	assert.False(t, strings.ContainsRune(stored, 0),
		"and the stored diagnostic carries no NUL")
	assert.True(t, utf8.ValidString(stored), "and is valid UTF-8 for the operator triaging it")
	assert.Contains(t, stored, "verify handle alice.coves.social",
		"while still saying why the row is STILL failing")
}
