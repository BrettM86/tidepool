package labels

import (
	"bytes"
	"context"
	stderrors "errors"
	"expvar"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/labeling"
	"github.com/bluesky-social/indigo/events"
	"github.com/gorilla/websocket"
)

// Store persists the consumer's cursor and the labels it commits.
type Store interface {
	Cursor(ctx context.Context) (Cursor, error)
	Commit(ctx context.Context, at Cursor, l labeling.Label) error
	Reset(ctx context.Context) (Cursor, error)
	Activate(ctx context.Context) error
	ApplyLapsed(ctx context.Context) (int, error)
}

// KeyResolver resolves a labeler DID.
type KeyResolver interface {
	Resolve(ctx context.Context, did string) (Labeler, error)
}

// DialContextFunc dials the TCP connection under a websocket.
type DialContextFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// ConsumerOptions configures a Consumer.
type ConsumerOptions struct {
	LabelerDID             string
	Resolver               KeyResolver
	Store                  Store
	Logger                 *slog.Logger
	MinBackoff, MaxBackoff time.Duration
	// NetDialContext dials the subscribeLabels connection. Production passes
	// ap.NewGuardedDialContext so the stream goes through the SSRF egress
	// guard; nil dials directly (tests against httptest servers).
	NetDialContext DialContextFunc
	// LapseInterval is how often Run applies lapsed labels; zero is 1 minute.
	LapseInterval time.Duration
}

// ConsumerStats are the consumer's counters.
type ConsumerStats struct {
	Resets, VerificationFailures uint64
	// UnusableLabels counts verified labels the store skipped (see
	// ErrUnusableLabel).
	UnusableLabels uint64
}

const (
	subscribeLabelsPath = "/xrpc/com.atproto.label.subscribeLabels"
	futureCursorError   = "FutureCursor"
	// maxFrameBytes bounds one websocket frame; a label message is well
	// under a kilobyte.
	maxFrameBytes = 1 << 20
	// readTimeout is how long the connection may stay silent. Coves pings
	// every 30 s, so a minute without a ping or message is a dead peer.
	readTimeout       = 90 * time.Second
	handshakeTimeout  = 30 * time.Second
	defaultMinBackoff = time.Second
	defaultMaxBackoff = 5 * time.Minute
	// defaultLapseInterval is how often Run applies lapsed labels when
	// ConsumerOptions.LapseInterval is zero.
	defaultLapseInterval = time.Minute
)

// errResubscribe ends a session that must reconnect at once (after a reset).
var errResubscribe = stderrors.New("labels: resubscribe")

// Consumer follows the labeler's subscribeLabels stream.
type Consumer struct {
	labelerDID             string
	resolver               KeyResolver
	store                  Store
	logger                 *slog.Logger
	minBackoff, maxBackoff time.Duration
	lapseInterval          time.Duration
	dialer                 *websocket.Dialer

	resets               atomic.Uint64
	verificationFailures atomic.Uint64
	unusableLabels       atomic.Uint64
}

// NewConsumer builds a Consumer from opts.
func NewConsumer(opts ConsumerOptions) (*Consumer, error) {
	if opts.LabelerDID == "" {
		return nil, fmt.Errorf("labels: consumer needs a labeler DID")
	}
	if opts.Resolver == nil {
		return nil, fmt.Errorf("labels: consumer needs a resolver")
	}
	if opts.Store == nil {
		return nil, fmt.Errorf("labels: consumer needs a store")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	minBackoff, maxBackoff := opts.MinBackoff, opts.MaxBackoff
	if minBackoff <= 0 {
		minBackoff = defaultMinBackoff
	}
	if maxBackoff <= 0 {
		maxBackoff = defaultMaxBackoff
	}
	if maxBackoff < minBackoff {
		return nil, fmt.Errorf("labels: MaxBackoff %s is below MinBackoff %s", maxBackoff, minBackoff)
	}
	if opts.LapseInterval < 0 {
		return nil, fmt.Errorf("labels: LapseInterval %s is negative", opts.LapseInterval)
	}
	lapseInterval := opts.LapseInterval
	if lapseInterval == 0 {
		lapseInterval = defaultLapseInterval
	}
	dial := opts.NetDialContext
	if dial == nil {
		dial = (&net.Dialer{Timeout: handshakeTimeout}).DialContext
	}
	return &Consumer{
		labelerDID:    opts.LabelerDID,
		resolver:      opts.Resolver,
		store:         opts.Store,
		logger:        logger.With(slog.String("component", "labels"), slog.String("labeler", opts.LabelerDID)),
		minBackoff:    minBackoff,
		maxBackoff:    maxBackoff,
		lapseInterval: lapseInterval,
		dialer: &websocket.Dialer{
			NetDialContext:   dial,
			HandshakeTimeout: handshakeTimeout,
		},
	}, nil
}

// Run activates the store's standing labels once, then consumes the stream
// until ctx is done, reconnecting with backoff after every failure. A failed
// activation is retried with backoff before the first subscribe, so no frame
// is committed ahead of the standing labels' effects. From then on it applies
// lapsed labels every lapse interval on its own ticker, independent of the
// stream, which may stay silent for as long as the connection is open. It
// returns once ctx is done.
func (c *Consumer) Run(ctx context.Context) {
	if !c.activate(ctx) {
		return
	}
	lapseCtx, stopLapse := context.WithCancel(ctx)
	lapseDone := make(chan struct{})
	go func() {
		defer close(lapseDone)
		c.lapseLoop(lapseCtx)
	}()
	defer func() {
		stopLapse()
		<-lapseDone
	}()
	c.consume(ctx)
}

// lapseLoop applies lapsed labels at once and then every lapse interval
// until ctx is done. A failed check is logged and retried on the next tick.
func (c *Consumer) lapseLoop(ctx context.Context) {
	ticker := time.NewTicker(c.lapseInterval)
	defer ticker.Stop()
	for {
		applied, err := c.store.ApplyLapsed(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			c.logger.Warn("label lapse check failed; retrying on the next tick",
				slog.String("error", err.Error()), slog.Duration("interval", c.lapseInterval))
		} else if applied > 0 {
			c.logger.Info("applied lapsed labels", slog.Int("applied", applied))
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// activate runs the store's activation, retrying with backoff, and reports
// whether it succeeded before ctx was done.
func (c *Consumer) activate(ctx context.Context) bool {
	backoff := c.minBackoff
	for {
		err := c.store.Activate(ctx)
		if ctx.Err() != nil {
			return false
		}
		if err == nil {
			return true
		}
		c.logger.Warn("label activation failed; retrying",
			slog.String("error", err.Error()), slog.Duration("backoff", backoff))
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
		backoff = min(backoff*2, c.maxBackoff)
	}
}

// consume runs stream sessions until ctx is done, reconnecting with backoff.
func (c *Consumer) consume(ctx context.Context) {
	backoff := c.minBackoff
	for {
		committed, err := c.session(ctx)
		if ctx.Err() != nil {
			return
		}
		if stderrors.Is(err, errResubscribe) {
			backoff = c.minBackoff
			continue
		}
		if committed {
			backoff = c.minBackoff
		}
		c.logger.Warn("label stream disconnected; reconnecting",
			slog.String("error", err.Error()), slog.Duration("backoff", backoff))
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		backoff = min(backoff*2, c.maxBackoff)
	}
}

// Stats returns the consumer's counters.
func (c *Consumer) Stats() ConsumerStats {
	return ConsumerStats{
		Resets:               c.resets.Load(),
		VerificationFailures: c.verificationFailures.Load(),
		UnusableLabels:       c.unusableLabels.Load(),
	}
}

// session runs one connection: resolve the labeler, connect at the stored
// cursor, and commit labels until the connection ends. committed reports
// whether any label was committed.
func (c *Consumer) session(ctx context.Context) (committed bool, err error) {
	labeler, err := c.resolver.Resolve(ctx, c.labelerDID)
	if err != nil {
		return false, fmt.Errorf("resolve labeler: %w", err)
	}
	cursor, err := c.store.Cursor(ctx)
	if err != nil {
		return false, fmt.Errorf("read cursor: %w", err)
	}
	streamURL, err := subscribeURL(labeler.Endpoint, cursor.Seq)
	if err != nil {
		return false, err
	}
	conn, response, err := c.dialer.DialContext(ctx, streamURL, nil)
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		return false, fmt.Errorf("connect to %s: %w", labeler.Endpoint, err)
	}
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
		case <-done:
		}
		_ = conn.Close()
	}()
	conn.SetReadLimit(maxFrameBytes)
	extendDeadline := func() { _ = conn.SetReadDeadline(time.Now().Add(readTimeout)) }
	extendDeadline()
	conn.SetPingHandler(func(payload string) error {
		extendDeadline()
		err := conn.WriteControl(websocket.PongMessage, []byte(payload), time.Now().Add(10*time.Second))
		if err != nil && !stderrors.Is(err, websocket.ErrCloseSent) {
			return err
		}
		return nil
	})
	c.logger.Info("label stream connected",
		slog.String("endpoint", labeler.Endpoint), slog.Int64("epoch", cursor.Epoch), slog.Int64("cursor", cursor.Seq))

	for {
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			return committed, fmt.Errorf("read: %w", err)
		}
		extendDeadline()
		if messageType != websocket.BinaryMessage {
			continue
		}
		next, err := c.handleFrame(ctx, &labeler, cursor, payload)
		if err != nil {
			return committed, err
		}
		if next != cursor {
			committed = true
			cursor = next
		}
	}
}

// handleFrame decodes one frame and commits its label. It returns the cursor
// after the frame.
func (c *Consumer) handleFrame(ctx context.Context, labeler *Labeler, cursor Cursor, payload []byte) (Cursor, error) {
	reader := bytes.NewReader(payload)
	var header events.EventHeader
	if err := header.UnmarshalCBOR(reader); err != nil {
		return cursor, fmt.Errorf("decode frame header: %w", err)
	}
	switch header.Op {
	case events.EvtKindErrorFrame:
		var frame events.ErrorFrame
		if err := frame.UnmarshalCBOR(reader); err != nil {
			return cursor, fmt.Errorf("decode error frame: %w", err)
		}
		if frame.Error != futureCursorError {
			return cursor, fmt.Errorf("labeler error frame %s: %s", frame.Error, frame.Message)
		}
		if cursor.Seq == 0 {
			// Already at the start: a reset would only loop.
			return cursor, fmt.Errorf("labeler answered cursor 0 with %s", futureCursorError)
		}
		reset, err := c.store.Reset(ctx)
		if err != nil {
			return cursor, fmt.Errorf("reset cursor: %w", err)
		}
		c.resets.Add(1)
		c.logger.Warn("labeler reports the cursor is in the future; replaying from the start in a new epoch",
			slog.Int64("previous_epoch", cursor.Epoch), slog.Int64("previous_cursor", cursor.Seq),
			slog.Int64("epoch", reset.Epoch))
		return reset, errResubscribe
	case events.EvtKindMessage:
	default:
		return cursor, fmt.Errorf("unknown frame op %d", header.Op)
	}
	if header.MsgType != "#labels" {
		c.logger.Debug("ignoring label stream message", slog.String("type", header.MsgType))
		return cursor, nil
	}
	var message comatproto.LabelSubscribeLabels_Labels
	if err := message.UnmarshalCBOR(reader); err != nil {
		return cursor, fmt.Errorf("decode #labels body: %w", err)
	}
	if message.Seq <= cursor.Seq {
		c.logger.Debug("skipping label at or before the cursor", slog.Int64("seq", message.Seq))
		return cursor, nil
	}
	if len(message.Labels) != 1 || message.Labels[0] == nil {
		// One stored row per seq: Coves sends exactly one label per message.
		return cursor, fmt.Errorf("#labels seq %d carries %d labels, want 1", message.Seq, len(message.Labels))
	}
	label := labeling.FromLexicon(message.Labels[0])
	if err := c.verify(ctx, labeler, &label); err != nil {
		c.verificationFailures.Add(1)
		c.logger.Error("label failed verification; not committed",
			slog.Int64("epoch", cursor.Epoch), slog.Int64("seq", message.Seq),
			slog.String("val", label.Val), slog.String("error", err.Error()))
		return cursor, fmt.Errorf("verify seq %d: %w", message.Seq, err)
	}
	at := Cursor{Epoch: cursor.Epoch, Seq: message.Seq}
	if err := c.store.Commit(ctx, at, label); err != nil {
		if stderrors.Is(err, ErrUnusableLabel) {
			c.unusableLabels.Add(1)
			c.logger.Warn("label unusable; skipped",
				slog.Int64("epoch", at.Epoch), slog.Int64("seq", at.Seq),
				slog.String("val", label.Val), slog.String("error", err.Error()))
			return at, nil
		}
		return cursor, fmt.Errorf("commit seq %d: %w", message.Seq, err)
	}
	return at, nil
}

// verify checks label against the labeler's key, re-resolving the labeler
// once when it fails so a rotated key is picked up.
func (c *Consumer) verify(ctx context.Context, labeler *Labeler, label *labeling.Label) error {
	if Verify(label, c.labelerDID, labeler.Key) == nil {
		return nil
	}
	refreshed, err := c.resolver.Resolve(ctx, c.labelerDID)
	if err != nil {
		return fmt.Errorf("re-resolve labeler: %w", err)
	}
	*labeler = refreshed
	return Verify(label, c.labelerDID, labeler.Key)
}

// subscribeURL is the subscribeLabels websocket URL at cursor on endpoint
// (the http(s) #atproto_labeler service endpoint).
func subscribeURL(endpoint string, cursor int64) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("labeler endpoint %q: %w", endpoint, err)
	}
	switch parsed.Scheme {
	case "https":
		parsed.Scheme = "wss"
	case "http":
		parsed.Scheme = "ws"
	default:
		return "", fmt.Errorf("labeler endpoint %q is not http(s)", endpoint)
	}
	parsed.Path = strings.TrimSuffix(parsed.Path, "/") + subscribeLabelsPath
	parsed.RawQuery = url.Values{"cursor": {strconv.FormatInt(cursor, 10)}}.Encode()
	parsed.Fragment = ""
	return parsed.String(), nil
}

// Metric names published for /admin/metrics (the "tidepool_" prefix is the
// one the endpoint serves).
const (
	MetricResets               = "tidepool_labels_resets"
	MetricVerificationFailures = "tidepool_labels_verification_failures"
	MetricUnusableLabels       = "tidepool_labels_skipped_unusable"
)

// publishOnce guards the expvar registration, which panics on a duplicate
// name.
var publishOnce sync.Once

// PublishMetrics registers the consumer's counters with expvar. Idempotent:
// the first consumer handed in is the one the gauges read.
func PublishMetrics(c *Consumer) {
	publishOnce.Do(func() {
		expvar.Publish(MetricResets, expvar.Func(func() any { return c.Stats().Resets }))
		expvar.Publish(MetricVerificationFailures, expvar.Func(func() any { return c.Stats().VerificationFailures }))
		expvar.Publish(MetricUnusableLabels, expvar.Func(func() any { return c.Stats().UnusableLabels }))
	})
}
