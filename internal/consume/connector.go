package consume

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// The connector is a PORT of the Coves AppView's connector.go — the same
// dial/reconnect/cursor loop, the same in-line retry schedule, and above all
// the same rule that a dead-letter write failure tears the connection down
// WITHOUT advancing the cursor. Divergences are noted where they occur.

// CursorStore persists the per-consumer Jetstream cursor (time_us of the last
// fully processed event) so the consumer resumes where it left off instead of
// at the live tail. The schema version is baked into the store, not passed
// per call: the connector has no business knowing about handler versioning.
type CursorStore interface {
	// GetCursor returns the persisted cursor for the consumer, or 0 when the
	// consumer has never persisted one (first run → live tail).
	GetCursor(ctx context.Context, consumerName string) (int64, error)
	// SaveCursor upserts the cursor for the consumer. The write is monotonic:
	// a smaller value than the stored one is a no-op, never a rewind.
	SaveCursor(ctx context.Context, consumerName string, cursorTimeUS int64) error
}

// DeadLetterWriter captures events that failed all in-line retries so they can
// be replayed later instead of being silently dropped.
type DeadLetterWriter interface {
	// AddDeadLetter stores the raw event bytes. redriveAttempts seeds the
	// redrive budget: 0 for transient failures, MaxRedriveAttempts for
	// permanent ones. Re-adding an already-captured event (same consumer,
	// time_us and payload) must succeed as a no-op so the cursor can advance.
	AddDeadLetter(ctx context.Context, consumerName string, eventTimeUS int64, eventData []byte, handleErr string, redriveAttempts int) error
}

// ConnectorStatus is a point-in-time snapshot of a connector's health, fed to
// /admin/metrics (consumer lag, last-event age, reconnects, DLQ depth).
type ConnectorStatus struct {
	Name                  string     `json:"name"`
	Connected             bool       `json:"connected"`
	ConnectedSince        *time.Time `json:"connectedSince,omitempty"`
	DisconnectedSince     *time.Time `json:"disconnectedSince,omitempty"`
	LastEventAt           *time.Time `json:"lastEventAt,omitempty"`
	CursorTimeUS          int64      `json:"cursorTimeUs"`
	PersistedCursorTimeUS int64      `json:"persistedCursorTimeUs"`
	EventsProcessed       uint64     `json:"eventsProcessed"`
	EventsDeadLettered    uint64     `json:"eventsDeadLettered"`
	Reconnects            uint64     `json:"reconnects"`
	// LastError is excluded from JSON on purpose: raw error strings leak
	// hosts, SQL fragments and file paths.
	LastError   string     `json:"-"`
	LastErrorAt *time.Time `json:"-"`
}

// Connector maintains the WebSocket connection to the self-hosted Jetstream
// and feeds events to an EventHandler.
//
//   - Cursor persistence: reconnects (and restarts) resume from the last
//     processed time_us minus a small rewind, so no events are lost in the
//     gap. This requires idempotent handlers, which the rev gate provides.
//   - Retry then dead-letter: a handler error is retried in-line; if it still
//     fails the RAW event is written to the dead letter queue and the cursor
//     advances. If even the dead letter write fails the connection is dropped
//     WITHOUT advancing the cursor, so the event replays on reconnect.
//   - Graceful shutdown: cancelling the Start context unblocks the read loop,
//     waits for the in-flight handler, and flushes the cursor.
type Connector struct {
	name        string
	wsURL       string
	handler     EventHandler
	cursorStore CursorStore
	deadLetters DeadLetterWriter
	logger      *slog.Logger

	reconnectDelay      time.Duration
	cursorFlushInterval time.Duration
	cursorRewind        time.Duration
	retryDelays         []time.Duration

	started atomic.Bool // guards against a second Start racing a second read loop

	mu                 sync.Mutex
	cursorLoaded       bool
	cursorTimeUS       int64 // last fully processed event (in-memory)
	persistedTimeUS    int64 // last value written to the CursorStore
	connected          bool
	connectedSince     time.Time
	disconnectedSince  time.Time
	lastEventAt        time.Time
	dials              uint64
	reconnects         uint64
	eventsProcessed    uint64
	eventsDeadLettered uint64
	lastError          string
	lastErrorAt        time.Time
}

// maxWebSocketFrameBytes bounds a single WebSocket frame (16 MiB). Without a
// limit the endpoint could force unbounded allocation; the largest plausible
// Jetstream event — a commit with a full record embedded — is well under it.
const maxWebSocketFrameBytes = 16 << 20

// readDeadline bounds how long a silent connection is tolerated. Pings reset
// it, so a stall is detected in a bounded time rather than hanging forever on
// a half-open socket.
const readDeadline = 60 * time.Second

// pingInterval keeps the connection alive well inside readDeadline.
const pingInterval = 30 * time.Second

// ConnectorOption configures a Connector.
type ConnectorOption func(*Connector)

// WithCursorStore enables cursor persistence. Without it the connector
// live-tails, which is only ever appropriate in tests.
func WithCursorStore(store CursorStore) ConnectorOption {
	return func(c *Connector) { c.cursorStore = store }
}

// WithDeadLetterWriter enables the dead letter queue for events that fail all
// in-line retries.
func WithDeadLetterWriter(writer DeadLetterWriter) ConnectorOption {
	return func(c *Connector) { c.deadLetters = writer }
}

// WithReconnectDelay overrides the delay between reconnect attempts.
func WithReconnectDelay(d time.Duration) ConnectorOption {
	return func(c *Connector) { c.reconnectDelay = d }
}

// WithCursorFlushInterval overrides how often the in-memory cursor is
// persisted to the CursorStore.
func WithCursorFlushInterval(d time.Duration) ConnectorOption {
	return func(c *Connector) { c.cursorFlushInterval = d }
}

// WithCursorRewind overrides how far behind the last processed event a re-dial
// resumes. Zero dials the exact cursor.
func WithCursorRewind(d time.Duration) ConnectorOption {
	return func(c *Connector) { c.cursorRewind = d }
}

// WithHandlerRetryDelays overrides the in-line retry schedule for handler
// errors. len(delays)+1 total attempts are made.
func WithHandlerRetryDelays(delays []time.Duration) ConnectorOption {
	return func(c *Connector) { c.retryDelays = delays }
}

// WithConnectorLogger overrides the logger. Defaults to slog.Default().
func WithConnectorLogger(logger *slog.Logger) ConnectorOption {
	return func(c *Connector) {
		if logger != nil {
			c.logger = logger
		}
	}
}

// NewConnector creates the Jetstream connector for the named consumer. The
// name keys the persisted cursor and dead-letter rows, so it must be stable
// across releases.
func NewConnector(name, wsURL string, handler EventHandler, opts ...ConnectorOption) *Connector {
	c := &Connector{
		name:                name,
		wsURL:               wsURL,
		handler:             handler,
		logger:              slog.Default(),
		reconnectDelay:      5 * time.Second,
		cursorFlushInterval: 5 * time.Second,
		// Jetstream's reconnection guidance: come back a few seconds behind
		// the last received event to guarantee gapless playback. Handlers are
		// idempotent (the rev gate), so the overlap costs nothing.
		cursorRewind: 5 * time.Second,
		retryDelays:  []time.Duration{200 * time.Millisecond, time.Second, 3 * time.Second},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Start runs the connector until ctx is cancelled, reconnecting on errors and
// flushing the cursor on the way out. It may be called at most once: a second
// call would race two read loops over one cursor, so it returns immediately.
func (c *Connector) Start(ctx context.Context) error {
	if !c.started.CompareAndSwap(false, true) {
		return fmt.Errorf("connector %s: Start called twice", c.name)
	}

	c.logger.Info("starting jetstream consumer",
		slog.String("consumer", c.name), slog.String("url", c.wsURL))

	// Disconnected-since-boot: a consumer that never achieves its first
	// connection must still surface as stalled in /admin/metrics.
	c.setConnected(false)

	// Flush the cursor one final time on the way out, even on cancellation.
	defer c.flushCursorOnShutdown()

	if c.cursorStore != nil {
		go c.runCursorFlusher(ctx)
	}

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("jetstream consumer shutting down", slog.String("consumer", c.name))
			// A CANCELLED context is the stop signal, so a clean stop is not a
			// failure: returning context.Canceled would make every deploy log
			// an error for doing exactly what it was told. Any other context
			// error (a deadline expiring under a consumer that is meant to run
			// forever) is real and propagates.
			if stderrors.Is(ctx.Err(), context.Canceled) {
				return nil
			}
			return ctx.Err()
		default:
		}

		// The cursor load is INSIDE the loop so a transient DB failure at boot
		// retries instead of permanently degrading the process to live-tail.
		if err := c.loadCursorOnce(ctx); err != nil {
			c.recordError(fmt.Errorf("load cursor: %w", err))
			c.logger.Error("jetstream consumer failed to load cursor; retrying",
				slog.String("consumer", c.name), slog.String("error", err.Error()))
			sleepCtx(ctx, c.reconnectDelay)
			continue
		}

		if err := c.connect(ctx); err != nil {
			if stderrors.Is(err, context.Canceled) {
				continue // the loop re-checks ctx and exits cleanly
			}
			c.recordError(err)
			c.logger.Warn("jetstream connection error; retrying",
				slog.String("consumer", c.name),
				slog.String("error", err.Error()),
				slog.Duration("retry_in", c.reconnectDelay))
			sleepCtx(ctx, c.reconnectDelay)
		}
	}
}

// Status returns a snapshot of the connector's health.
func (c *Connector) Status() ConnectorStatus {
	c.mu.Lock()
	defer c.mu.Unlock()

	status := ConnectorStatus{
		Name:                  c.name,
		Connected:             c.connected,
		CursorTimeUS:          c.cursorTimeUS,
		PersistedCursorTimeUS: c.persistedTimeUS,
		EventsProcessed:       c.eventsProcessed,
		EventsDeadLettered:    c.eventsDeadLettered,
		Reconnects:            c.reconnects,
		LastError:             c.lastError,
	}
	if !c.connectedSince.IsZero() {
		t := c.connectedSince
		status.ConnectedSince = &t
	}
	if !c.disconnectedSince.IsZero() {
		t := c.disconnectedSince
		status.DisconnectedSince = &t
	}
	if !c.lastEventAt.IsZero() {
		t := c.lastEventAt
		status.LastEventAt = &t
	}
	if !c.lastErrorAt.IsZero() {
		t := c.lastErrorAt
		status.LastErrorAt = &t
	}
	return status
}

// loadCursorOnce loads the persisted cursor on the first successful call.
func (c *Connector) loadCursorOnce(ctx context.Context) error {
	c.mu.Lock()
	loaded := c.cursorLoaded
	c.mu.Unlock()
	if loaded || c.cursorStore == nil {
		return nil
	}

	cursor, err := c.cursorStore.GetCursor(ctx, c.name)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.cursorLoaded = true
	c.cursorTimeUS = cursor
	c.persistedTimeUS = cursor
	c.mu.Unlock()

	if cursor > 0 {
		c.logger.Info("jetstream consumer resuming from persisted cursor",
			slog.String("consumer", c.name),
			slog.Int64("cursor", cursor),
			slog.String("at", time.UnixMicro(cursor).UTC().Format(time.RFC3339)))
	}
	return nil
}

// dialURL returns the WebSocket URL, with the cursor query parameter set only
// when a cursor is actually known.
func (c *Connector) dialURL() (string, error) {
	c.mu.Lock()
	cursor := c.cursorTimeUS
	c.mu.Unlock()

	if cursor <= 0 {
		// NO cursor parameter on a first dial. An explicit cursor=0 does not
		// mean "live tail" to Jetstream — it asks for the ENTIRE retained
		// store, which is a very expensive way to say "I have no idea where I
		// left off".
		return c.wsURL, nil
	}

	parsed, err := url.Parse(c.wsURL)
	if err != nil {
		return "", fmt.Errorf("invalid Jetstream URL %q: %w", c.wsURL, err)
	}
	rewound := cursor - c.cursorRewind.Microseconds()
	if rewound < 0 {
		rewound = 0
	}
	query := parsed.Query()
	query.Set("cursor", strconv.FormatInt(rewound, 10))
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

// connect establishes the WebSocket connection and processes events until the
// connection drops or ctx is cancelled.
func (c *Connector) connect(ctx context.Context) error {
	dialURL, err := c.dialURL()
	if err != nil {
		return err
	}

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, dialURL, nil)
	if err != nil {
		return fmt.Errorf("connect to jetstream: %w", err)
	}

	c.countDial()
	c.setConnected(true)
	defer c.setConnected(false)
	defer func() {
		// The ctx-watcher goroutine below usually closes the connection first;
		// an already-closed error here is a clean shutdown, not a failure.
		if closeErr := conn.Close(); closeErr != nil && !stderrors.Is(closeErr, net.ErrClosed) {
			c.logger.Debug("jetstream close failed",
				slog.String("consumer", c.name), slog.String("error", closeErr.Error()))
		}
	}()

	c.logger.Debug("connected to jetstream", slog.String("consumer", c.name))

	// Cap the frame size so a misbehaving endpoint cannot force unbounded
	// allocation.
	conn.SetReadLimit(maxWebSocketFrameBytes)

	if err := conn.SetReadDeadline(time.Now().Add(readDeadline)); err != nil {
		c.logger.Debug("jetstream set read deadline failed",
			slog.String("consumer", c.name), slog.String("error", err.Error()))
	}
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(readDeadline))
	})

	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	done := make(chan struct{})
	var closeOnce sync.Once
	closeDone := func() { closeOnce.Do(func() { close(done) }) }
	defer closeDone()

	// Unblock the read loop whenever the connection must die: on shutdown (ctx
	// cancelled) and on ping failure (done closed). ReadMessage only returns
	// when the connection closes, so without this a SIGTERM would hang until
	// the next network event and a ping failure would wait out the full read
	// deadline before reconnecting.
	go func() {
		select {
		case <-ctx.Done():
		case <-done:
		}
		_ = conn.Close()
	}()

	go func() {
		for {
			select {
			case <-ticker.C:
				if err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(10*time.Second)); err != nil {
					c.logger.Warn("jetstream ping failed",
						slog.String("consumer", c.name), slog.String("error", err.Error()))
					closeDone()
					return
				}
			case <-done:
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			return fmt.Errorf("connection closed by ping failure")
		default:
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("read error: %w", err)
		}

		if err := conn.SetReadDeadline(time.Now().Add(readDeadline)); err != nil {
			c.logger.Debug("jetstream set read deadline failed",
				slog.String("consumer", c.name), slog.String("error", err.Error()))
		}

		if err := c.processMessage(ctx, message); err != nil {
			// Only fatal pipeline failures (dead letter write failed, ctx
			// cancelled) reach here. Drop the connection WITHOUT advancing the
			// cursor so the event replays on reconnect.
			return err
		}
	}
}

// processMessage parses and handles one raw Jetstream message.
//
// Outcomes:
//   - handled successfully              → cursor advances, counts as processed
//   - failed all retries, dead-lettered → cursor advances (the event is safe
//     in the DLQ) but does NOT count toward eventsProcessed
//   - permanent failure (ErrPermanentEvent) → dead-lettered already exhausted
//     (attempts = MaxRedriveAttempts), cursor advances
//   - dead letter write failed          → error returned, cursor does NOT
//     advance
//   - unparseable JSON                  → dead-lettered for forensics, cursor
//     unaffected; on reconnect the same frame may be dead-lettered again until
//     a later event advances the cursor — harmless, because the dedup index
//     makes the re-add a no-op instead of a fresh row.
func (c *Connector) processMessage(ctx context.Context, message []byte) error {
	var event JetstreamEvent
	if err := json.Unmarshal(message, &event); err != nil {
		c.logger.Error("jetstream event failed to parse; dead-lettering",
			slog.String("consumer", c.name), slog.String("error", err.Error()))
		c.recordError(fmt.Errorf("parse event: %w", err))
		// time_us 0: a frame that will not parse has no position to record,
		// and therefore none to advance the cursor to either.
		return c.deadLetter(ctx, 0, message, err, 0)
	}

	handleErr := c.handleWithRetry(ctx, &event)
	if handleErr != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Permanent failures are dead-lettered with their redrive budget
		// already spent: replaying a validation rejection can never succeed,
		// so the row is kept for forensics only.
		redriveAttempts := 0
		if stderrors.Is(handleErr, ErrPermanentEvent) {
			redriveAttempts = MaxRedriveAttempts
		}
		c.logger.Error("jetstream event failed; dead-lettering",
			slog.String("consumer", c.name),
			slog.String("did", event.DID),
			slog.String("kind", event.Kind),
			slog.Int64("time_us", event.TimeUS),
			slog.Bool("permanent", redriveAttempts == MaxRedriveAttempts),
			slog.String("error", handleErr.Error()))
		c.recordError(handleErr)
		if err := c.deadLetter(ctx, event.TimeUS, message, handleErr, redriveAttempts); err != nil {
			return err
		}
		// Safe in the DLQ: advance the cursor, but do NOT count the event as
		// processed throughput.
		c.advanceCursor(event.TimeUS)
		return nil
	}

	c.recordProcessed(event.TimeUS)
	return nil
}

// handleWithRetry invokes the handler, retrying transient failures in-line.
// Errors wrapped with ErrPermanentEvent short-circuit immediately: retrying a
// permanent rejection can only ever waste the retry schedule on a guaranteed
// failure, which is also how an adversary emitting invalid records would stall
// the consumer.
//
// Retries block subsequent events ON PURPOSE: events within one consumer's
// stream are ordered (a record's create precedes its update/delete), so
// skipping ahead would apply them out of order. Dead-lettering trades that
// ordering away only after every retry has failed.
func (c *Connector) handleWithRetry(ctx context.Context, event *JetstreamEvent) error {
	err := c.handler.HandleEvent(ctx, event)
	if err == nil || stderrors.Is(err, ErrPermanentEvent) {
		return err
	}

	for attempt, delay := range c.retryDelays {
		if !sleepCtx(ctx, delay) {
			return ctx.Err()
		}
		if err = c.handler.HandleEvent(ctx, event); err == nil {
			return nil
		}
		// The permanent wrapping can also appear on a later attempt (a
		// different code path fails this time); stop as soon as the failure is
		// known permanent.
		if stderrors.Is(err, ErrPermanentEvent) {
			return err
		}
		c.logger.Warn("jetstream handler retry failed",
			slog.String("consumer", c.name),
			slog.Int("attempt", attempt+1),
			slog.Int("of", len(c.retryDelays)),
			slog.String("error", err.Error()))
	}
	return err
}

// deadLetter writes a failed event to the dead letter queue. A write failure
// is returned to the caller, which tears down the connection WITHOUT advancing
// the in-memory cursor; the reconnect dials from that in-memory cursor (the
// persisted one only matters across a process restart), so the event replays
// instead of being lost — it is the one case where advancing would lose an
// event outright, since it is then neither handled nor captured.
func (c *Connector) deadLetter(ctx context.Context, timeUS int64, message []byte, cause error, redriveAttempts int) error {
	if c.deadLetters == nil {
		c.logger.Error("jetstream event DROPPED: no dead letter writer configured",
			slog.String("consumer", c.name), slog.String("error", cause.Error()))
		return nil
	}
	if err := c.deadLetters.AddDeadLetter(ctx, c.name, timeUS, message, cause.Error(), redriveAttempts); err != nil {
		return fmt.Errorf("dead-letter event (will replay from cursor): %w", err)
	}
	c.mu.Lock()
	c.eventsDeadLettered++
	c.mu.Unlock()
	return nil
}

// runCursorFlusher periodically persists the in-memory cursor. Without it a
// hard crash (SIGKILL, OOM) would lose everything since the process started.
func (c *Connector) runCursorFlusher(ctx context.Context) {
	ticker := time.NewTicker(c.cursorFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.flushCursor(ctx)
		}
	}
}

// flushCursor persists the cursor if it advanced since the last flush.
func (c *Connector) flushCursor(ctx context.Context) {
	if c.cursorStore == nil {
		return
	}
	c.mu.Lock()
	cursor := c.cursorTimeUS
	persisted := c.persistedTimeUS
	c.mu.Unlock()

	if cursor <= persisted {
		return
	}
	if err := c.cursorStore.SaveCursor(ctx, c.name, cursor); err != nil {
		c.logger.Error("failed to persist jetstream cursor",
			slog.String("consumer", c.name), slog.String("error", err.Error()))
		return
	}
	c.mu.Lock()
	if cursor > c.persistedTimeUS {
		c.persistedTimeUS = cursor
	}
	c.mu.Unlock()
}

// flushCursorOnShutdown persists the final cursor on a FRESH context: the
// Start context is already cancelled by the time shutdown reaches here, and a
// flush inheriting it would silently lose the last interval's progress on
// every clean deploy.
func (c *Connector) flushCursorOnShutdown() {
	if c.cursorStore == nil {
		return
	}
	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c.flushCursor(flushCtx)
}

func (c *Connector) setConnected(connected bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connected = connected
	if connected {
		c.connectedSince = time.Now()
		c.disconnectedSince = time.Time{}
	} else {
		c.disconnectedSince = time.Now()
		c.connectedSince = time.Time{}
	}
}

// countDial records a successful dial. The BOOT connection is not a reconnect:
// counting it would make every healthy process report one, and the metric
// exists to make an unstable link stand out from a stable one.
func (c *Connector) countDial() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dials > 0 {
		c.reconnects++
	}
	c.dials++
}

// advanceCursor moves the in-memory cursor and lastEventAt past an event that
// is fully accounted for (handled or safely dead-lettered) WITHOUT counting it
// as processed throughput — a 100%-failing consumer must not graph as healthy.
func (c *Connector) advanceCursor(timeUS int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastEventAt = time.Now()
	// Replayed events from the reconnect rewind carry older time_us; never
	// regress the cursor.
	if timeUS > c.cursorTimeUS {
		c.cursorTimeUS = timeUS
	}
}

// recordProcessed advances the cursor for a successfully handled event and
// counts it toward eventsProcessed.
func (c *Connector) recordProcessed(timeUS int64) {
	c.advanceCursor(timeUS)
	c.mu.Lock()
	c.eventsProcessed++
	c.mu.Unlock()
}

func (c *Connector) recordError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastError = err.Error()
	c.lastErrorAt = time.Now()
}

// sleepCtx sleeps for d unless ctx is cancelled first, reporting false on
// cancellation.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
