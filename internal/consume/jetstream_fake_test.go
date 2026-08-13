package consume

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gorilla/websocket"
)

// fakeJetstream is a minimal stand-in for the self-hosted Jetstream: it
// upgrades the connection, writes a scripted sequence of raw frames, then
// HOLDS THE CONNECTION OPEN and drains reads until the client goes away.
//
// Holding open matters. If the server closed after the script, the connector
// would treat that as a dropped connection and reconnect in a loop, replaying
// the script on every pass — the test would then be measuring reconnect
// behavior instead of handler idempotence.
//
// Frames are RAW BYTES, never marshalled from this package's structs, so the
// tests pin the WIRE shape Jetstream actually emits rather than agreeing with
// our own types about it.
type fakeJetstream struct {
	server *httptest.Server
	script [][]byte

	mu      sync.Mutex
	dials   int
	cursors []string
	queries []string
}

// newFakeJetstream starts a fake Jetstream serving the given frames at
// /subscribe. It is closed when the test finishes.
func newFakeJetstream(t *testing.T, script ...[]byte) *fakeJetstream {
	t.Helper()

	fake := &fakeJetstream{script: script}
	upgrader := websocket.Upgrader{
		CheckOrigin: func(*http.Request) bool { return true },
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/subscribe", func(w http.ResponseWriter, r *http.Request) {
		fake.mu.Lock()
		fake.dials++
		fake.cursors = append(fake.cursors, r.URL.Query().Get("cursor"))
		fake.queries = append(fake.queries, r.URL.RawQuery)
		fake.mu.Unlock()

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("fake jetstream: upgrade: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()

		for _, frame := range fake.script {
			if err := conn.WriteMessage(websocket.TextMessage, frame); err != nil {
				return // client went away mid-script; nothing to report
			}
		}
		// Hold open. ReadMessage also services the connector's pings, and
		// returns as soon as the client closes.
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})

	fake.server = httptest.NewServer(mux)
	t.Cleanup(fake.server.Close)
	return fake
}

// URL is the ws:// endpoint a Connector dials.
func (f *fakeJetstream) URL() string {
	return "ws" + strings.TrimPrefix(f.server.URL, "http") + "/subscribe"
}

// Dials reports how many times a client connected.
func (f *fakeJetstream) Dials() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dials
}

// Cursors returns the cursor query parameter of every dial, in order.
func (f *fakeJetstream) Cursors() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.cursors...)
}

// recordedIntent is one OutboundEnqueuer call.
type recordedIntent struct {
	ActorDID    string
	OrderingKey string
	ParentATURI string
	Intent      Intent
}

// recordingEnqueuer is the task 15 seam, recorded. Task 15 owns AP vocabulary;
// this test only cares that the consumer decided on exactly one intent, with
// the right subject and a deterministic id.
type recordingEnqueuer struct {
	mu    sync.Mutex
	calls []recordedIntent
}

func (e *recordingEnqueuer) EnqueueActivity(_ context.Context, actorDID, orderingKey, parentATURI string, intent Intent) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls = append(e.calls, recordedIntent{
		ActorDID:    actorDID,
		OrderingKey: orderingKey,
		ParentATURI: parentATURI,
		Intent:      intent,
	})
	return nil
}

func (e *recordingEnqueuer) Calls() []recordedIntent {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]recordedIntent(nil), e.calls...)
}

func (e *recordingEnqueuer) Len() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.calls)
}
