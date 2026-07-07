package sync

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/bluesky-social/indigo/events"
	"github.com/gorilla/websocket"
)

// upgrader intentionally skips origin checks: subscribeRepos is a public
// read-only firehose (relays and Jetstream are not browsers, and no
// credentials ride the connection).
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1 << 10,
	WriteBufferSize: 1 << 16,
	CheckOrigin:     func(*http.Request) bool { return true },
}

// maxSubscriberMessageSize bounds what a subscriber may send us; the stream
// is server-to-client, so inbound traffic beyond control frames is noise.
const maxSubscriberMessageSize = 1 << 10

// Protocol tokens from the atproto event-stream spec, exported so consumers
// (and the task-08 harness) assert against the same constants we emit.
const (
	// InfoOutdatedCursor is the #info name sent when a requested cursor
	// precedes the retained window; the stream then continues from the
	// oldest retained event.
	InfoOutdatedCursor = "OutdatedCursor"
	// ErrorFutureCursor is the terminal error frame name for a cursor ahead
	// of the newest assigned seq.
	ErrorFutureCursor = "FutureCursor"
)

// handleSubscribeRepos serves com.atproto.sync.subscribeRepos.
//
// Connection anatomy: after validating the cursor and upgrading, a read pump
// goroutine consumes (and discards) client traffic to keep control-frame
// processing alive and to detect closure, while this goroutine is the
// per-connection outbox — it replays stored events from firehose_events
// (`WHERE seq > cursor`, gapless by task 03's commit serialization), then
// blocks on its broadcaster subscription and streams live events as they
// land. The outbox holds no queue in memory: its cursor into the durable log
// IS the queue, so a slow consumer costs one row read per frame and nothing
// else. A consumer that cannot drain a frame within writeTimeout is evicted;
// it can reconnect with its last cursor and resume without loss.
func (s *Server) handleSubscribeRepos(w http.ResponseWriter, r *http.Request) {
	var cursor *int64
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed < 0 {
			writeXRPCError(w, http.StatusBadRequest, "InvalidRequest", "cursor must be a non-negative integer")
			return
		}
		cursor = &parsed
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		// Upgrade already wrote the HTTP error response.
		s.logger.Info("subscribeRepos upgrade failed", "remote", r.RemoteAddr, "error", err)
		return
	}
	if s.onUpgrade != nil {
		s.onUpgrade(conn)
	}
	logger := s.logger.With("remote", r.RemoteAddr)
	logger.Info("subscribeRepos connected", "cursor", cursorForLog(cursor))

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	defer func() { _ = conn.Close() }()

	go s.readPump(cancel, conn)
	go s.pingPump(ctx, cancel, conn, logger)
	s.streamEvents(ctx, conn, cursor, logger)
	// Best-effort close handshake so well-behaved peers log a clean closure
	// instead of 1006; the deferred Close below is the enforcement.
	_ = conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(s.writeTimeout))
	logger.Info("subscribeRepos disconnected")
}

// pingPump owns peer liveness. It must be its own goroutine: the outbox loop
// can spend arbitrarily long draining full replay batches, and consumers
// only refresh readPump's deadline by ponging our pings — a ping loop that
// shares the outbox select starves during replay and evicts healthy
// consumers mid-backfill. gorilla documents WriteControl as safe to call
// concurrently with the outbox's WriteMessage.
func (s *Server) pingPump(ctx context.Context, cancel context.CancelFunc, conn *websocket.Conn, logger *slog.Logger) {
	defer cancel()
	ticker := time.NewTicker(s.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := conn.WriteControl(websocket.PingMessage, nil,
				time.Now().Add(s.writeTimeout)); err != nil {
				if ctx.Err() == nil {
					logger.Info("subscribeRepos: ping failed; dropping subscriber", "error", err)
				}
				return
			}
		}
	}
}

// readPump drains inbound traffic. Reading is what makes gorilla process
// incoming control frames (close, ping, pong); the payloads themselves are
// ignored. Any read error — including the peer closing — cancels the
// connection context so the outbox loop stops.
func (s *Server) readPump(cancel context.CancelFunc, conn *websocket.Conn) {
	defer cancel()
	conn.SetReadLimit(maxSubscriberMessageSize)
	deadline := func() time.Time { return time.Now().Add(3 * s.pingInterval) }
	_ = conn.SetReadDeadline(deadline())
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(deadline())
	})
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		_ = conn.SetReadDeadline(deadline())
	}
}

// streamEvents is the outbox loop described on handleSubscribeRepos.
//
// KNOWN GAP (deliberate, tracked for task 06): the stream carries only
// #commit frames and does not consult consent state, so a tombstoned actor's
// already-committed events remain replayable for the retention window even
// though the HTTP read surface refuses the repo. The atproto-native fix is
// emitting an #account{active:false} frame when consent flips (plus delete
// commits from the record scrub) so downstream consumers purge — that emit
// path must land with task 06's consent flows.
func (s *Server) streamEvents(ctx context.Context, conn *websocket.Conn, cursor *int64, logger *slog.Logger) {
	// Subscribe BEFORE reading the seq bounds: an event landing between the
	// bounds read and the first wait leaves a token in the wake channel, so
	// the gap between "replayed everything" and "waiting for wake-ups" can
	// never lose an event.
	sub := s.broadcaster.Subscribe()
	defer sub.Close()

	oldest, newest, err := s.repo.SeqBounds(ctx)
	if err != nil {
		logger.Error("subscribeRepos: read seq bounds", "error", err)
		return
	}

	// No cursor: live tail only, starting after the current newest event.
	since := newest
	if cursor != nil {
		switch {
		case *cursor > newest:
			// Per the event-stream spec a future cursor is a terminal error:
			// silently serving from the tip could mask a consumer pointed at
			// the wrong host.
			frame := errorFrame(ErrorFutureCursor,
				"cursor "+strconv.FormatInt(*cursor, 10)+" is ahead of current seq "+strconv.FormatInt(newest, 10))
			s.writeFrame(conn, frame, logger)
			return
		case *cursor < oldest-1:
			// Cursor points into the pruned (or never-existing) past: warn
			// with #info OutdatedCursor, then serve everything retained.
			if !s.writeFrame(conn, infoFrame(InfoOutdatedCursor,
				"requested cursor exceeded limit; possibly missing events"), logger) {
				return
			}
			if oldest > newest {
				// Empty log. `oldest` here derives from the raw sequence,
				// which counts nextval calls from still-uncommitted commit
				// transactions — resuming from oldest-1 could jump past an
				// in-flight event. Resuming from the cursor itself is safe:
				// nothing ≤ oldest-1 is retained, so nothing can duplicate,
				// and a racing commit lands with seq > cursor and is served.
				since = *cursor
			} else {
				since = oldest - 1
			}
		default:
			since = *cursor
		}
	}

	// Fallback re-poll so a lost wake-up degrades to latency, never a stall
	// (the broadcaster's own poll ticker is the primary backstop). Pings live
	// in pingPump, not here — see its comment.
	fallback := time.NewTicker(s.pingInterval)
	defer fallback.Stop()

	for {
		batch, err := s.repo.ListEvents(ctx, since, s.replayBatch)
		if err != nil {
			if ctx.Err() == nil {
				logger.Error("subscribeRepos: list events", "since", since, "error", err)
			}
			return
		}
		// Seq gaps are usually benign (rolled-back nextval), but they are
		// also the only symptom when the retention pruner overtakes a
		// consumer mid-replay — and the consumer cannot tell the two apart.
		// On any gap, re-check the retained window and signal real data loss
		// with OutdatedCursor instead of silently skipping it.
		if len(batch) > 0 && batch[0].Seq > since+1 {
			oldest, _, err := s.repo.SeqBounds(ctx)
			if err != nil {
				if ctx.Err() == nil {
					logger.Error("subscribeRepos: recheck seq bounds", "error", err)
				}
				return
			}
			if since < oldest-1 {
				logger.Warn("subscribeRepos: consumer overtaken by retention pruning",
					"since", since, "oldest", oldest)
				if !s.writeFrame(conn, infoFrame(InfoOutdatedCursor,
					"events pruned past the consumer's position; possibly missing events"), logger) {
					return
				}
			}
		}
		for _, ev := range batch {
			frame, err := commitFrame(ev)
			if err != nil {
				// A stored event that cannot be framed is data corruption;
				// dropping it silently would desync consumers, so terminate.
				logger.Error("subscribeRepos: build frame", "seq", ev.Seq, "error", err)
				return
			}
			if !s.writeFrame(conn, frame, logger) {
				return
			}
			since = ev.Seq
		}
		if len(batch) == s.replayBatch {
			continue // backlog remains; keep draining before sleeping
		}
		select {
		case <-ctx.Done():
			return
		case <-sub.Wake():
		case <-fallback.C:
		}
	}
}

// writeFrame sends one serialized frame, enforcing the slow-consumer write
// deadline. It reports false when the connection should be torn down.
func (s *Server) writeFrame(conn *websocket.Conn, frame *events.XRPCStreamEvent, logger *slog.Logger) bool {
	payload, err := serializeFrame(frame)
	if err != nil {
		logger.Error("subscribeRepos: serialize frame", "error", err)
		return false
	}
	_ = conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
	if err := conn.WriteMessage(websocket.BinaryMessage, payload); err != nil {
		logger.Info("subscribeRepos: write failed; evicting subscriber", "error", err)
		return false
	}
	return true
}

func cursorForLog(cursor *int64) any {
	if cursor == nil {
		return "live"
	}
	return *cursor
}
