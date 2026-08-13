package consume

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/testutil"
)

// Task 14 cycle B: the connector port. Every outcome below is one row of the
// table Coves' processMessage documents, and the reason each matters is
// availability, not tidiness — a consumer that drops events silently, or
// stalls forever on one poison frame, fails in the direction nobody notices.

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

func connectorTestDB(t *testing.T) *sql.DB {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database,
		"consumer_cursors", "jetstream_dead_letters", "jetstream_record_revs")
	return database
}

// waitFor polls cond until it holds or the bound expires. Bounded polling, not
// sleeps: a fixed sleep either flakes under load or wastes the whole suite's
// time budget.
func waitFor(t *testing.T, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out after 5s waiting for %s", desc)
}

// runningConnector is a Connector under test plus its shutdown handle. The
// shutdown is registered as a cleanup so a failed assertion still tears the
// connection down BEFORE the httptest server's own cleanup runs (LIFO), which
// would otherwise block on the still-open WebSocket.
type runningConnector struct {
	connector *Connector
	cancel    context.CancelFunc
	done      chan error
	once      sync.Once
	err       error
}

func startConnector(t *testing.T, c *Connector) *runningConnector {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	running := &runningConnector{connector: c, cancel: cancel, done: make(chan error, 1)}
	go func() { running.done <- c.Start(ctx) }()
	t.Cleanup(func() { running.stop(t) })
	return running
}

// stop cancels the Start context and waits for Start to return. It is
// idempotent so tests can stop explicitly and still rely on the cleanup.
func (r *runningConnector) stop(t *testing.T) error {
	t.Helper()
	r.once.Do(func() {
		r.cancel()
		select {
		case r.err = <-r.done:
		case <-time.After(5 * time.Second):
			t.Error("connector did not shut down within 5s of context cancellation")
		}
	})
	return r.err
}

// recordingHandler is an EventHandler that records what it saw and can be told
// to fail. failFor bounds the failures so a test can drive "fails, then
// succeeds"; a negative value fails forever.
type recordingHandler struct {
	mu      sync.Mutex
	events  []JetstreamEvent
	err     error
	failFor int
	calls   int
}

func (h *recordingHandler) HandleEvent(_ context.Context, event *JetstreamEvent) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls++
	h.events = append(h.events, *event)
	if h.err != nil && (h.failFor < 0 || h.calls <= h.failFor) {
		return h.err
	}
	return nil
}

func (h *recordingHandler) Calls() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls
}

func (h *recordingHandler) Events() []JetstreamEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]JetstreamEvent(nil), h.events...)
}

func (h *recordingHandler) TimeUSSeen() []int64 {
	seen := []int64{}
	for _, event := range h.Events() {
		seen = append(seen, event.TimeUS)
	}
	return seen
}

// faultyDeadLetters wraps a real DeadLetterWriter and fails the first failFor
// writes — the "postgres is down while an event is failing" case, which is the
// one situation where the connector must NOT advance its cursor.
type faultyDeadLetters struct {
	inner   DeadLetterWriter
	mu      sync.Mutex
	failFor int
	calls   int
}

func (f *faultyDeadLetters) AddDeadLetter(ctx context.Context, consumerName string, eventTimeUS int64, eventData []byte, handleErr string, redriveAttempts int) error {
	f.mu.Lock()
	f.calls++
	fail := f.calls <= f.failFor
	f.mu.Unlock()
	if fail {
		return fmt.Errorf("simulated dead letter write failure")
	}
	return f.inner.AddDeadLetter(ctx, consumerName, eventTimeUS, eventData, handleErr, redriveAttempts)
}

func (f *faultyDeadLetters) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// ---------------------------------------------------------------------------
// Frames (raw wire JSON — never marshalled from this package's own structs)
// ---------------------------------------------------------------------------

const connDID = "did:plc:7iza6de2dwap2sbkpav7c6c6"

func connFrame(timeUS int64, rev, rkey string) []byte {
	return []byte(fmt.Sprintf(
		`{"did":%q,"time_us":%d,"kind":"commit","commit":{"rev":%q,"operation":"create",`+
			`"collection":"social.coves.community.comment","rkey":%q,`+
			`"cid":"bafyreievgu2ty7qbiaaom5zhmkznsnajuzideek3lo7e65dwqlrvrxnmo4",`+
			`"record":{"$type":"social.coves.community.comment","content":"hi",`+
			`"createdAt":"2026-08-12T10:00:00.000Z"}}}`,
		connDID, timeUS, rev, rkey))
}

func accountFrame(timeUS int64, active bool, status string) []byte {
	return []byte(fmt.Sprintf(
		`{"did":%q,"time_us":%d,"kind":"account",`+
			`"account":{"did":%q,"seq":7,"time":"2026-08-12T10:00:00.000Z","active":%t,"status":%q}}`,
		connDID, timeUS, connDID, active, status))
}

func identityFrame(timeUS int64, handle string) []byte {
	return []byte(fmt.Sprintf(
		`{"did":%q,"time_us":%d,"kind":"identity",`+
			`"identity":{"did":%q,"handle":%q,"seq":8,"time":"2026-08-12T10:00:00.000Z"}}`,
		connDID, timeUS, connDID, handle))
}

// newTestConnectorKeepingRewind builds a connector with test-compressed
// timings but leaves cursorRewind at its PRODUCTION default, so a test can
// assert what that default is.
func newTestConnectorKeepingRewind(t *testing.T, wsURL string, handler EventHandler, state *PostgresStateStore, opts ...ConnectorOption) *Connector {
	t.Helper()
	base := []ConnectorOption{
		WithCursorStore(state),
		WithDeadLetterWriter(state),
		WithCursorFlushInterval(20 * time.Millisecond),
		WithReconnectDelay(20 * time.Millisecond),
		WithHandlerRetryDelays([]time.Duration{time.Millisecond, time.Millisecond}),
	}
	return NewConnector(ConsumerNative, wsURL, handler, append(base, opts...)...)
}

// newTestConnector is the same with the rewind zeroed, so the cursor a test
// dials with is exactly the last processed time_us.
func newTestConnector(t *testing.T, wsURL string, handler EventHandler, state *PostgresStateStore, opts ...ConnectorOption) *Connector {
	t.Helper()
	return newTestConnectorKeepingRewind(t, wsURL, handler, state,
		append([]ConnectorOption{WithCursorRewind(0)}, opts...)...)
}

// ---------------------------------------------------------------------------
// B1 — happy path
// ---------------------------------------------------------------------------

func TestConnector_ConsumesEventsInOrderAndNeverRegressesTheCursor(t *testing.T) {
	database := connectorTestDB(t)
	state := NewPostgresStateStore(database, CursorSchemaVersion)
	handler := &recordingHandler{}

	// The third frame is OLDER than the second: exactly what the reconnect
	// rewind produces. The connector must hand it to the handler (dedupe is
	// the rev gate's job, not the connector's) while refusing to walk its own
	// cursor backwards.
	fake := newFakeJetstream(t,
		connFrame(100, "3lzrev0000001", "aaa"),
		connFrame(200, "3lzrev0000002", "bbb"),
		connFrame(150, "3lzrev0000003", "ccc"),
	)

	connector := newTestConnector(t, fake.URL(), handler, state)
	running := startConnector(t, connector)

	waitFor(t, "all three scripted frames to reach the handler", func() bool {
		return handler.Calls() >= 3
	})

	assert.Equal(t, []int64{100, 200, 150}, handler.TimeUSSeen(),
		"events are delivered in stream order: a record's create precedes its update, "+
			"so skipping ahead would apply them out of order")

	status := connector.Status()
	assert.Equal(t, int64(200), status.CursorTimeUS,
		"the cursor holds the HIGHEST processed time_us: a replayed older event must "+
			"never rewind it")
	assert.Equal(t, uint64(3), status.EventsProcessed)
	assert.Zero(t, status.EventsDeadLettered)
	assert.True(t, status.Connected, "the connector reports itself connected while reading")
	require.NotNil(t, status.LastEventAt, "last-event age is an observability requirement")

	require.NoError(t, running.stop(t))
	assert.Equal(t, int64(200), readCursor(t, database, ConsumerNative, CursorSchemaVersion))
}

func TestConnector_FlushesCursorOnTheFlushInterval(t *testing.T) {
	database := connectorTestDB(t)
	state := NewPostgresStateStore(database, CursorSchemaVersion)
	handler := &recordingHandler{}

	fake := newFakeJetstream(t, connFrame(500, "3lzrev0000001", "aaa"))
	connector := newTestConnector(t, fake.URL(), handler, state,
		WithCursorFlushInterval(20*time.Millisecond))
	startConnector(t, connector)

	// The row must appear WHILE the connector is still running — that is the
	// periodic flusher, not the shutdown flush. Without it a hard crash (SIGKILL,
	// OOM) loses everything since the last restart.
	waitFor(t, "the periodic flusher to persist the cursor mid-run", func() bool {
		var cursor int64
		err := database.QueryRowContext(context.Background(),
			`SELECT cursor_time_us FROM consumer_cursors
			  WHERE consumer_name = $1 AND schema_version = $2`,
			ConsumerNative, CursorSchemaVersion).Scan(&cursor)
		return err == nil && cursor == 500
	})

	assert.Equal(t, int64(500), connector.Status().PersistedCursorTimeUS,
		"the connector tracks what it has actually persisted, not just what it has processed")
}

func TestConnector_FlushesCursorOnShutdownWithAFreshContext(t *testing.T) {
	database := connectorTestDB(t)
	state := NewPostgresStateStore(database, CursorSchemaVersion)
	handler := &recordingHandler{}

	fake := newFakeJetstream(t, connFrame(700, "3lzrev0000001", "aaa"))
	// An interval far longer than the test: the ONLY thing that can persist
	// this cursor is the shutdown flush.
	connector := newTestConnector(t, fake.URL(), handler, state,
		WithCursorFlushInterval(time.Hour))
	running := startConnector(t, connector)

	waitFor(t, "the event to be processed", func() bool { return handler.Calls() >= 1 })

	var before int
	require.NoError(t, database.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM consumer_cursors`).Scan(&before))
	require.Zero(t, before, "no periodic flush can have happened yet")

	require.NoError(t, running.stop(t),
		"a graceful shutdown returns nil: cancelling the context IS the shutdown signal, "+
			"so surfacing context.Canceled would make every clean deploy log an error")

	// The Start context is already CANCELLED at this point. The flush can only
	// have succeeded on a fresh context — a flush inheriting the cancelled one
	// would silently lose the last interval's progress on every clean deploy.
	assert.Equal(t, int64(700), readCursor(t, database, ConsumerNative, CursorSchemaVersion),
		"the final flush must run on a fresh context, because the Start context is "+
			"already cancelled by the time shutdown reaches it")
}

func TestConnector_StartIsRefusedTwice(t *testing.T) {
	database := connectorTestDB(t)
	state := NewPostgresStateStore(database, CursorSchemaVersion)

	fake := newFakeJetstream(t)
	connector := newTestConnector(t, fake.URL(), &recordingHandler{}, state)
	startConnector(t, connector)

	waitFor(t, "the connector to connect", func() bool { return fake.Dials() >= 1 })

	err := connector.Start(context.Background())
	require.Error(t, err,
		"a second Start would race two read loops over one cursor; it must be refused, "+
			"not silently allowed")
}

// ---------------------------------------------------------------------------
// B2 — reconnect
// ---------------------------------------------------------------------------

func TestConnector_ReconnectsFromCursorMinusRewind(t *testing.T) {
	const firstTimeUS = 3_000_000_000

	tests := []struct {
		name             string
		rewind           time.Duration
		useDefaultRewind bool
		firstTimeUS      int64
		wantDialCursor   string
	}{
		{
			name:           "no rewind dials the exact cursor",
			rewind:         0,
			firstTimeUS:    firstTimeUS,
			wantDialCursor: strconv.Itoa(firstTimeUS),
		},
		{
			name:           "the rewind is subtracted in microseconds",
			rewind:         2 * time.Second,
			firstTimeUS:    firstTimeUS,
			wantDialCursor: strconv.Itoa(firstTimeUS - 2_000_000),
		},
		{
			// Jetstream's reconnection guidance: come back a few seconds
			// behind the last received event to guarantee gapless playback.
			// Handlers are idempotent, so the overlap costs nothing.
			name:             "the default rewind is 5s",
			useDefaultRewind: true,
			firstTimeUS:      firstTimeUS,
			wantDialCursor:   strconv.Itoa(firstTimeUS - 5_000_000),
		},
		{
			name:           "a rewind past the beginning of time clamps at zero",
			rewind:         5 * time.Second,
			firstTimeUS:    1_000,
			wantDialCursor: "0",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			database := connectorTestDB(t)
			state := NewPostgresStateStore(database, CursorSchemaVersion)
			handler := &recordingHandler{}

			// The server hangs up after the first event; the second dial
			// carries the second one.
			fake := newSessionJetstream(t,
				jsSession{frames: [][]byte{connFrame(tc.firstTimeUS, "3lzrev0000001", "aaa")}, closeAfter: true},
				jsSession{frames: [][]byte{connFrame(tc.firstTimeUS+1_000_000, "3lzrev0000002", "bbb")}},
			)

			// The default case sets NO rewind option at all: whatever
			// NewConnector chose is what gets asserted.
			var connector *Connector
			if tc.useDefaultRewind {
				connector = newTestConnectorKeepingRewind(t, fake.URL(), handler, state)
			} else {
				connector = newTestConnector(t, fake.URL(), handler, state,
					WithCursorRewind(tc.rewind))
			}
			startConnector(t, connector)

			waitFor(t, "the connector to re-dial and consume the second event", func() bool {
				return handler.Calls() >= 2
			})

			cursors := fake.Cursors()
			require.GreaterOrEqual(t, len(cursors), 2,
				"the connector must re-dial after the server hangs up, not give up")
			assert.Equal(t, "", cursors[0],
				"the first dial has no cursor to resume from, so it must not send one "+
					"(an explicit cursor=0 would ask Jetstream to replay its entire store)")
			assert.Equal(t, tc.wantDialCursor, cursors[1],
				"the re-dial resumes from the last processed event minus the rewind")

			assert.Equal(t, []int64{tc.firstTimeUS, tc.firstTimeUS + 1_000_000}, handler.TimeUSSeen(),
				"no event is lost across the reconnect")
			assert.GreaterOrEqual(t, connector.Status().Reconnects, uint64(1),
				"reconnects are counted for /admin/metrics; the boot connection is not one")
		})
	}
}

// ---------------------------------------------------------------------------
// B3 — retry, dead-letter, and the one case that must NOT advance the cursor
// ---------------------------------------------------------------------------

func TestConnector_TransientHandlerErrorRetriesInLineThenDeadLetters(t *testing.T) {
	database := connectorTestDB(t)
	state := NewPostgresStateStore(database, CursorSchemaVersion)

	// Two retry delays = three total invocations. Retries block the stream on
	// purpose: events within one consumer are ordered, so skipping ahead would
	// apply a record's update before its create.
	handler := &recordingHandler{err: fmt.Errorf("postgres blip"), failFor: -1}

	frame := connFrame(900, "3lzrev0000001", "aaa")
	fake := newFakeJetstream(t, frame)
	connector := newTestConnector(t, fake.URL(), handler, state,
		WithHandlerRetryDelays([]time.Duration{time.Millisecond, time.Millisecond}))
	running := startConnector(t, connector)

	waitFor(t, "the event to be dead-lettered", func() bool {
		return countRows(t, database, "jetstream_dead_letters") == 1
	})

	assert.Equal(t, 3, handler.Calls(),
		"one initial attempt plus len(retryDelays) retries — no more, no fewer")

	dead, err := state.ListRetryable(context.Background(), ConsumerNative, MaxRedriveAttempts, 10)
	require.NoError(t, err)
	require.Len(t, dead, 1)
	assert.Equal(t, int64(900), dead[0].EventTimeUS)
	assert.Equal(t, frame, dead[0].EventData,
		"the RAW frame is stored, so a redrive replays exactly what arrived")
	assert.Contains(t, dead[0].LastError, "postgres blip")
	assert.Zero(t, dead[0].Attempts,
		"a transient failure seeds the redrive budget at 0 so the redriver retries it")

	waitFor(t, "the cursor to advance past the dead-lettered event", func() bool {
		return connector.Status().CursorTimeUS == 900
	})
	status := connector.Status()
	assert.Equal(t, uint64(1), status.EventsDeadLettered)
	assert.Zero(t, status.EventsProcessed,
		"a dead-lettered event is safe, not processed: a 100%-failing consumer must "+
			"not graph as healthy throughput")

	require.NoError(t, running.stop(t))
	assert.Equal(t, int64(900), readCursor(t, database, ConsumerNative, CursorSchemaVersion),
		"the event is safe in the DLQ, so the cursor advances past it")
}

func TestConnector_PermanentHandlerErrorSkipsRetriesAndBurnsTheBudget(t *testing.T) {
	database := connectorTestDB(t)
	state := NewPostgresStateStore(database, CursorSchemaVersion)

	// Before this distinction existed, an adversary emitting invalid records
	// could stall the consumer ~4s per event and grow a DLQ of rows that
	// redrive ten times each.
	handler := &recordingHandler{
		err:     fmt.Errorf("%w: repo DID does not match community DID", ErrPermanentEvent),
		failFor: -1,
	}

	fake := newFakeJetstream(t, connFrame(1_100, "3lzrev0000001", "aaa"))
	connector := newTestConnector(t, fake.URL(), handler, state,
		WithHandlerRetryDelays([]time.Duration{time.Second, time.Second, time.Second}))
	startConnector(t, connector)

	waitFor(t, "the permanent failure to be dead-lettered", func() bool {
		return countRows(t, database, "jetstream_dead_letters") == 1
	})

	assert.Equal(t, 1, handler.Calls(),
		"a permanent rejection is never retried in-line: retrying it can only ever "+
			"waste the retry schedule (3s here) on a guaranteed failure")

	// Already exhausted: the redriver must never pick it up.
	retryable, err := state.ListRetryable(context.Background(), ConsumerNative, MaxRedriveAttempts, 10)
	require.NoError(t, err)
	assert.Empty(t, retryable,
		"a permanent failure is dead-lettered with its redrive budget already spent")

	var attempts int
	require.NoError(t, database.QueryRowContext(context.Background(),
		`SELECT attempts FROM jetstream_dead_letters WHERE event_time_us = 1100`).Scan(&attempts))
	assert.Equal(t, MaxRedriveAttempts, attempts)

	waitFor(t, "the cursor to advance past the permanent failure", func() bool {
		return connector.Status().CursorTimeUS == 1_100
	})
}

func TestConnector_DeadLetterWriteFailureBlocksCursorAdvance(t *testing.T) {
	database := connectorTestDB(t)
	state := NewPostgresStateStore(database, CursorSchemaVersion)
	handler := &recordingHandler{err: fmt.Errorf("handler down"), failFor: -1}

	// The DLQ write fails once. That is the ONE case where advancing the
	// cursor would lose the event outright: it is neither handled nor
	// captured. The connection must drop instead, so the redial replays it.
	faulty := &faultyDeadLetters{inner: state, failFor: 1}

	fake := newFakeJetstream(t, connFrame(1_300, "3lzrev0000001", "aaa"))
	connector := newTestConnector(t, fake.URL(), handler, state,
		WithDeadLetterWriter(faulty),
		WithHandlerRetryDelays([]time.Duration{time.Millisecond}))
	startConnector(t, connector)

	waitFor(t, "the connector to re-dial after the failed dead-letter write", func() bool {
		return fake.Dials() >= 2
	})

	cursors := fake.Cursors()
	require.GreaterOrEqual(t, len(cursors), 2)
	assert.Equal(t, "", cursors[1],
		"the re-dial must NOT have advanced past the event whose capture failed — "+
			"the cursor is still 0, so the event replays instead of vanishing")

	waitFor(t, "the retried dead-letter write to succeed", func() bool {
		return countRows(t, database, "jetstream_dead_letters") == 1
	})
	waitFor(t, "the cursor to advance once the event is safely captured", func() bool {
		return connector.Status().CursorTimeUS == 1_300
	})

	assert.GreaterOrEqual(t, faulty.Calls(), 2,
		"the dead-letter write is attempted again on the redial")
}

// ---------------------------------------------------------------------------
// B4 — unparseable frames
// ---------------------------------------------------------------------------

func TestConnector_UnparseableFrameIsDeadLetteredWithoutMovingTheCursor(t *testing.T) {
	database := connectorTestDB(t)
	state := NewPostgresStateStore(database, CursorSchemaVersion)
	handler := &recordingHandler{}

	garbage := []byte(`{"kind":"commit","time_us":`)
	// The good frame comes FIRST so the cursor has a known value the garbage
	// must leave alone.
	fake := newFakeJetstream(t, connFrame(1_500, "3lzrev0000001", "aaa"), garbage)
	connector := newTestConnector(t, fake.URL(), handler, state)
	startConnector(t, connector)

	waitFor(t, "the unparseable frame to be dead-lettered", func() bool {
		return countRows(t, database, "jetstream_dead_letters") == 1
	})

	assert.Equal(t, 1, handler.Calls(),
		"an unparseable frame never reaches the handler")

	dead, err := state.ListRetryable(context.Background(), ConsumerNative, MaxRedriveAttempts, 10)
	require.NoError(t, err)
	require.Len(t, dead, 1)
	assert.Equal(t, int64(0), dead[0].EventTimeUS,
		"a frame that will not parse has no time_us to record, so it is captured at 0")
	assert.Equal(t, garbage, dead[0].EventData,
		"the raw bytes are kept for forensics: this is how a lexicon rollout mistake "+
			"stays recoverable instead of becoming silent permanent loss")

	assert.Equal(t, int64(1_500), connector.Status().CursorTimeUS,
		"an unparseable frame cannot advance the cursor — it has no position to "+
			"advance to — and must not rewind it either")
}

func TestConnector_ReplayedUnparseableFrameDedupesInsteadOfPilingUp(t *testing.T) {
	database := connectorTestDB(t)
	state := NewPostgresStateStore(database, CursorSchemaVersion)
	garbage := []byte(`{"kind":"commit","time_us":`)

	// Because an unparseable frame never advances the cursor, every reconnect
	// re-delivers it. Without the dedup index that would be a fresh row — and
	// a fresh redrive budget — on every pass.
	for run := 1; run <= 2; run++ {
		handler := &recordingHandler{}
		fake := newFakeJetstream(t, garbage)
		connector := newTestConnector(t, fake.URL(), handler, state)
		running := startConnector(t, connector)

		waitFor(t, fmt.Sprintf("run %d to capture the unparseable frame", run), func() bool {
			return connector.Status().EventsDeadLettered >= 1
		})
		require.NoError(t, running.stop(t))
	}

	assert.Equal(t, 1, countRows(t, database, "jetstream_dead_letters"),
		"a re-captured frame hits the dedup index and stays ONE row, so a poison "+
			"frame cannot grow the queue without bound")
}

// ---------------------------------------------------------------------------
// B5 — cursor survives restart; the quiet-stream replay is harmless
// ---------------------------------------------------------------------------

func TestConnector_FreshInstanceResumesFromThePersistedCursor(t *testing.T) {
	database := connectorTestDB(t)
	state := NewPostgresStateStore(database, CursorSchemaVersion)

	// Run 1: a separate process lifetime.
	first := &recordingHandler{}
	firstFake := newFakeJetstream(t, connFrame(2_000_000_000, "3lzrev0000001", "aaa"))
	firstConnector := newTestConnector(t, firstFake.URL(), first, state)
	firstRunning := startConnector(t, firstConnector)
	waitFor(t, "run 1 to process its event", func() bool { return first.Calls() >= 1 })
	require.NoError(t, firstRunning.stop(t))
	require.Equal(t, int64(2_000_000_000), readCursor(t, database, ConsumerNative, CursorSchemaVersion))

	// Run 2: a brand new Connector over the same postgres — a restart. The
	// quiet-stream case: Jetstream re-serves the same event because nothing
	// newer exists.
	second := &recordingHandler{}
	secondFake := newFakeJetstream(t, connFrame(2_000_000_000, "3lzrev0000001", "aaa"))
	secondConnector := newTestConnector(t, secondFake.URL(), second, state)
	secondRunning := startConnector(t, secondConnector)

	waitFor(t, "run 2 to dial", func() bool { return secondFake.Dials() >= 1 })
	assert.Equal(t, []string{"2000000000"}, secondFake.Cursors(),
		"a restart resumes from the persisted cursor; starting at the live tail is "+
			"the exact data loss cursors exist to prevent")

	waitFor(t, "run 2 to process the replayed event", func() bool { return second.Calls() >= 1 })
	assert.Equal(t, 1, second.Calls(),
		"the connector does not dedupe replays — it hands them to the handler, whose "+
			"idempotence (the rev gate) is what makes the replay harmless")

	require.NoError(t, secondRunning.stop(t))
	assert.Equal(t, int64(2_000_000_000), readCursor(t, database, ConsumerNative, CursorSchemaVersion),
		"replaying an already-processed event leaves the persisted cursor exactly where "+
			"it was: the monotonic save absorbs it")
}

// ---------------------------------------------------------------------------
// B6 — non-commit frames
// ---------------------------------------------------------------------------

func TestConnector_ParsesAccountAndIdentityFrames(t *testing.T) {
	database := connectorTestDB(t)
	state := NewPostgresStateStore(database, CursorSchemaVersion)
	handler := &recordingHandler{}

	fake := newFakeJetstream(t,
		accountFrame(3_100, false, "deactivated"),
		identityFrame(3_200, "alice.coves.social"),
	)
	connector := newTestConnector(t, fake.URL(), handler, state)
	startConnector(t, connector)

	waitFor(t, "both non-commit frames to reach the handler", func() bool {
		return handler.Calls() >= 2
	})

	events := handler.Events()
	require.Len(t, events, 2)

	account := events[0]
	assert.Equal(t, "account", account.Kind)
	require.NotNil(t, account.Account, "an #account frame must surface its account payload")
	assert.False(t, account.Account.Active)
	assert.Equal(t, "deactivated", account.Account.Status,
		"STATUS is what distinguishes a deactivation from a deletion (decision 19): "+
			"active=false alone must never be read as 'the account was deleted'")
	assert.Equal(t, connDID, account.Account.DID)
	assert.Equal(t, int64(7), account.Account.Seq)

	identity := events[1]
	assert.Equal(t, "identity", identity.Kind)
	require.NotNil(t, identity.Identity, "an #identity frame must surface its identity payload")
	assert.Equal(t, "alice.coves.social", identity.Identity.Handle)
	assert.Equal(t, connDID, identity.Identity.DID)
	assert.Equal(t, int64(8), identity.Identity.Seq)

	assert.Equal(t, int64(3_200), connector.Status().CursorTimeUS,
		"non-commit frames advance the cursor like any other event")
}
