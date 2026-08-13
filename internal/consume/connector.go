package consume

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// STUB (task 14 RED): the shape is the Coves Connector's, the behavior is
// not implemented yet. Port connector.go's dial/reconnect/cursor loop, the
// in-line retry schedule, and the dead-letter-write-failure-blocks-cursor
// rule verbatim.

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
type Connector struct {
	name        string
	wsURL       string
	handler     EventHandler
	cursorStore CursorStore
	deadLetters DeadLetterWriter

	reconnectDelay      time.Duration
	cursorFlushInterval time.Duration
	cursorRewind        time.Duration
	retryDelays         []time.Duration

	started atomic.Bool

	mu     sync.Mutex
	status ConnectorStatus
}

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

// WithHandlerRetryDelays overrides the in-line retry schedule for handler
// errors. len(delays)+1 total attempts are made.
func WithHandlerRetryDelays(delays []time.Duration) ConnectorOption {
	return func(c *Connector) { c.retryDelays = delays }
}

// NewConnector creates the Jetstream connector for the named consumer. The
// name keys the persisted cursor and dead-letter rows, so it must be stable
// across releases.
func NewConnector(name, wsURL string, handler EventHandler, opts ...ConnectorOption) *Connector {
	c := &Connector{
		name:                name,
		wsURL:               wsURL,
		handler:             handler,
		reconnectDelay:      5 * time.Second,
		cursorFlushInterval: 5 * time.Second,
		cursorRewind:        5 * time.Second,
		retryDelays:         []time.Duration{200 * time.Millisecond, time.Second, 3 * time.Second},
		status:              ConnectorStatus{Name: name},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Start runs the connector until ctx is cancelled, reconnecting on errors and
// flushing the cursor on the way out.
func (c *Connector) Start(ctx context.Context) error {
	return nil // STUB
}

// Status returns a snapshot of the connector's health.
func (c *Connector) Status() ConnectorStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.status // STUB: never advances
}
