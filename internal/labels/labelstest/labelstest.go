// Package labelstest is test support for the label consumer: a fake
// com.atproto.label.subscribeLabels server speaking the Coves wire format and
// a label-signing helper. It holds no production logic.
package labelstest

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/labeling"
	"github.com/bluesky-social/indigo/events"
	"github.com/gorilla/websocket"
)

// SubscribeLabelsPath is the XRPC path the fake server answers.
const SubscribeLabelsPath = "/xrpc/com.atproto.label.subscribeLabels"

// DefaultCreatedAt is the cts Sign fills in when the label has none.
const DefaultCreatedAt = "2026-11-08T12:00:00.000Z"

// Frame is one websocket binary message: a CBOR header then a CBOR body.
type Frame struct {
	payload []byte
}

// Labels is a `#labels` message carrying one label at seq.
func Labels(t testing.TB, seq int64, l labeling.Label) Frame {
	t.Helper()
	lexicon := l.ToLexicon()
	return encodeFrame(t,
		&events.EventHeader{Op: events.EvtKindMessage, MsgType: "#labels"},
		&comatproto.LabelSubscribeLabels_Labels{Seq: seq, Labels: []*comatproto.LabelDefs_Label{&lexicon}},
	)
}

// FutureCursor is the error frame Coves sends for a cursor past its head.
func FutureCursor(t testing.TB) Frame {
	t.Helper()
	return encodeFrame(t,
		&events.EventHeader{Op: events.EvtKindErrorFrame},
		&events.ErrorFrame{Error: "FutureCursor", Message: "cursor is in the future"},
	)
}

func encodeFrame(t testing.TB, header *events.EventHeader, body interface{ MarshalCBOR(io.Writer) error }) Frame {
	t.Helper()
	var buf bytes.Buffer
	if err := header.MarshalCBOR(&buf); err != nil {
		t.Fatalf("labelstest: marshal header: %v", err)
	}
	if err := body.MarshalCBOR(&buf); err != nil {
		t.Fatalf("labelstest: marshal body: %v", err)
	}
	return Frame{payload: buf.Bytes()}
}

// Reply is the server's answer to one connection: Frames in order, then
// either a close (Hold false) or an open connection with no further frames
// until the client goes away or the test ends (Hold true).
type Reply struct {
	Frames []Frame
	Hold   bool
}

// Server is a fake subscribeLabels endpoint.
type Server struct {
	// URL is the http:// base URL, the shape a DID document's
	// #atproto_labeler serviceEndpoint has.
	URL string

	connections chan int64
	done        chan struct{}

	mu   sync.Mutex
	seen []int64
}

// NewServer starts a fake server that answers each connection with
// reply(cursor). cursor is -1 when the request carries none.
func NewServer(t testing.TB, reply func(cursor int64) Reply) *Server {
	t.Helper()
	s := &Server{
		connections: make(chan int64, 256),
		done:        make(chan struct{}),
	}
	upgrader := websocket.Upgrader{}
	mux := http.NewServeMux()
	mux.HandleFunc(SubscribeLabelsPath, func(w http.ResponseWriter, r *http.Request) {
		cursor := int64(-1)
		if raw := r.URL.Query().Get("cursor"); raw != "" {
			parsed, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				http.Error(w, "bad cursor", http.StatusBadRequest)
				return
			}
			cursor = parsed
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		s.mu.Lock()
		s.seen = append(s.seen, cursor)
		s.mu.Unlock()
		select {
		case s.connections <- cursor:
		default:
		}
		answer := reply(cursor)
		for _, frame := range answer.Frames {
			if err := conn.WriteMessage(websocket.BinaryMessage, frame.payload); err != nil {
				return
			}
		}
		if !answer.Hold {
			_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			return
		}
		clientGone := make(chan struct{})
		go func() {
			defer close(clientGone)
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		}()
		select {
		case <-clientGone:
		case <-s.done:
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(s.done) })
	s.URL = srv.URL
	return s
}

// Connections delivers the cursor of each connection as it arrives.
func (s *Server) Connections() <-chan int64 { return s.connections }

// Cursors is every connection's cursor so far, in arrival order.
func (s *Server) Cursors() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int64(nil), s.seen...)
}

// Sign fills Version (1) and, when empty, CreatedAt, then signs l with key.
func Sign(t testing.TB, key atcrypto.PrivateKey, l labeling.Label) labeling.Label {
	t.Helper()
	l.Version = labeling.ATPROTO_LABEL_VERSION
	if l.CreatedAt == "" {
		l.CreatedAt = DefaultCreatedAt
	}
	if err := l.Sign(key); err != nil {
		t.Fatalf("labelstest: sign label: %v", err)
	}
	return l
}

// NewKey generates a K-256 signing key.
func NewKey(t testing.TB) *atcrypto.PrivateKeyK256 {
	t.Helper()
	key, err := atcrypto.GeneratePrivateKeyK256()
	if err != nil {
		t.Fatalf("labelstest: generate key: %v", err)
	}
	return key
}

// PublicKey is key's public half.
func PublicKey(t testing.TB, key atcrypto.PrivateKey) atcrypto.PublicKey {
	t.Helper()
	public, err := key.PublicKey()
	if err != nil {
		t.Fatalf("labelstest: public key: %v", err)
	}
	return public
}
