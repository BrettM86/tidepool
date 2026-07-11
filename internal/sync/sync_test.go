package sync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	comatproto "github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/events"
	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
	"github.com/ipfs/go-cid"
	car "github.com/ipld/go-car"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/repo"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

const (
	testDID        = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	testCollection = "social.coves.community.post"
	testHostname   = "bridge.test"
)

// staticKeys mirrors internal/repo's test double: one key signs every DID.
type staticKeys struct{ key *atcrypto.PrivateKeyK256 }

func (s *staticKeys) SigningKey(context.Context, string, repo.KeyUse) (atcrypto.PrivateKey, error) {
	return s.key, nil
}

type harness struct {
	manager     *repo.Manager
	broadcaster *Broadcaster
	server      *Server
	http        *httptest.Server
}

// newHarness builds the full serving stack over the real test database:
// repo.Manager → Broadcaster (LISTEN/NOTIFY) → Server → httptest. Tunables
// are shrunk so failures surface in milliseconds, not minutes.
func newHarness(t *testing.T) *harness {
	t.Helper()
	return newHarnessWithPoll(t, 500*time.Millisecond)
}

// newHarnessWithPoll lets a test pick the broadcaster poll fallback — an
// effectively-infinite interval makes the LISTEN/NOTIFY path load-bearing.
// Optional mutators adjust the server Options before construction (the
// admission-control tests shrink limits with them).
func newHarnessWithPoll(t *testing.T, pollInterval time.Duration, mutate ...func(*Options)) *harness {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database, "blocks", "repo_state", "firehose_events", "bridged_actors", "blobs")

	key, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	manager, err := repo.NewManager(database, &staticKeys{key: key}, nil)
	require.NoError(t, err)

	broadcaster, err := NewBroadcaster(os.Getenv("TIDEPOOL_TEST_DATABASE_URL"), pollInterval, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	go broadcaster.Run(ctx)
	t.Cleanup(func() {
		cancel()
		_ = broadcaster.Close()
	})

	opts := Options{
		Repo:         manager,
		Broadcaster:  broadcaster,
		Hostname:     testHostname,
		WriteTimeout: 2 * time.Second,
		PingInterval: time.Second,
		ReplayBatch:  10,
	}
	for _, m := range mutate {
		m(&opts)
	}
	server, err := NewServer(opts)
	require.NoError(t, err)

	router := chi.NewRouter()
	server.Routes(router)
	ts := httptest.NewServer(router)
	t.Cleanup(ts.Close)

	return &harness{manager: manager, broadcaster: broadcaster, server: server, http: ts}
}

func (h *harness) wsURL(cursor string) string {
	url := "ws" + strings.TrimPrefix(h.http.URL, "http") + "/xrpc/com.atproto.sync.subscribeRepos"
	if cursor != "" {
		url += "?cursor=" + cursor
	}
	return url
}

func (h *harness) dial(t *testing.T, cursor string) *websocket.Conn {
	t.Helper()
	conn, resp, err := websocket.DefaultDialer.Dial(h.wsURL(cursor), nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// readFrame reads and decodes one stream frame with a hard deadline; a
// stalled stream fails the test instead of hanging it.
func readFrame(t *testing.T, conn *websocket.Conn, timeout time.Duration) *events.XRPCStreamEvent {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(timeout)))
	msgType, payload, err := conn.ReadMessage()
	require.NoError(t, err, "reading stream frame")
	require.Equal(t, websocket.BinaryMessage, msgType)
	var evt events.XRPCStreamEvent
	require.NoError(t, evt.Deserialize(bytes.NewReader(payload)), "decoding stream frame")
	return &evt
}

func testRecord(text string) map[string]any {
	return map[string]any{
		"$type":     testCollection,
		"text":      text,
		"createdAt": "2026-07-07T00:00:00.000Z",
	}
}

func putRecord(t *testing.T, h *harness, rkey, text string) *repo.CommitResult {
	t.Helper()
	res, err := h.manager.PutRecord(context.Background(), testDID, testCollection, rkey, testRecord(text))
	require.NoError(t, err)
	return res
}

// waitForSubscribers blocks until the broadcaster sees n live subscriptions —
// writes made before a subscriber is registered would otherwise race its
// "live tail starts after current newest" snapshot.
func waitForSubscribers(t *testing.T, h *harness, n int) {
	t.Helper()
	require.Eventually(t, func() bool { return h.broadcaster.subscriberCount() == n },
		5*time.Second, 10*time.Millisecond, "waiting for %d subscribers", n)
}

// TestSubscribeRepos_GaplessCursorReplayThenLive is the task's money test:
// 100 records, connect at cursor 50, receive exactly 51..100 in order, then
// live events as they land.
func TestSubscribeRepos_GaplessCursorReplayThenLive(t *testing.T) {
	h := newHarness(t)

	results := make([]*repo.CommitResult, 100)
	for i := range results {
		results[i] = putRecord(t, h, fmt.Sprintf("r%03d", i), fmt.Sprintf("post %d", i))
	}

	conn := h.dial(t, fmt.Sprintf("%d", results[49].Seq))
	for i := 50; i < 100; i++ {
		evt := readFrame(t, conn, 5*time.Second)
		require.NotNil(t, evt.RepoCommit, "expected #commit frame at index %d", i)
		assert.Equal(t, results[i].Seq, evt.RepoCommit.Seq, "gapless order at index %d", i)
		assert.Equal(t, testDID, evt.RepoCommit.Repo)
		assert.Equal(t, results[i].Rev, evt.RepoCommit.Rev)
		require.NotNil(t, evt.RepoCommit.Since, "non-genesis commit must carry since")
		assert.Equal(t, results[i-1].Rev, *evt.RepoCommit.Since)
		assert.Equal(t, results[i].CommitCID, cid.Cid(evt.RepoCommit.Commit).String())
		assert.False(t, evt.RepoCommit.Rebase)
		assert.False(t, evt.RepoCommit.TooBig)
	}

	// Live tail: the broadcaster's LISTEN wake-up must deliver a new commit
	// without waiting for the poll fallback.
	live := putRecord(t, h, "r100", "live post")
	evt := readFrame(t, conn, 5*time.Second)
	require.NotNil(t, evt.RepoCommit)
	assert.Equal(t, live.Seq, evt.RepoCommit.Seq)
	assert.Equal(t, live.Rev, evt.RepoCommit.Rev)
}

// TestSubscribeRepos_FrameAnatomy pins the wire shape of a #commit frame:
// ops content and the CAR slice (root=commit block, record block included,
// decodable by indigo's reader).
func TestSubscribeRepos_FrameAnatomy(t *testing.T) {
	h := newHarness(t)

	genesis := putRecord(t, h, "anat1", "genesis post")
	update := putRecord(t, h, "anat2", "second post")

	conn := h.dial(t, "0")

	evt := readFrame(t, conn, 5*time.Second)
	require.NotNil(t, evt.RepoCommit)
	assert.Equal(t, genesis.Seq, evt.RepoCommit.Seq)
	assert.Nil(t, evt.RepoCommit.Since, "genesis commit must have null since")
	assert.Nil(t, evt.RepoCommit.PrevData, "genesis commit must have null prevData")
	require.Len(t, evt.RepoCommit.Ops, 1)
	assert.Equal(t, string(repo.OpActionCreate), evt.RepoCommit.Ops[0].Action)
	assert.Equal(t, testCollection+"/anat1", evt.RepoCommit.Ops[0].Path)
	require.NotNil(t, evt.RepoCommit.Ops[0].Cid)
	assert.Equal(t, genesis.RecordCID, cid.Cid(*evt.RepoCommit.Ops[0].Cid).String())
	require.NotEmpty(t, evt.RepoCommit.Time)
	_, err := time.Parse(time.RFC3339, evt.RepoCommit.Time)
	assert.NoError(t, err, "frame time must be RFC 3339")

	// CAR slice: header root == commit CID, commit block physically first,
	// record block present.
	reader, err := car.NewCarReader(bytes.NewReader(evt.RepoCommit.Blocks))
	require.NoError(t, err)
	require.Len(t, reader.Header.Roots, 1)
	assert.Equal(t, genesis.CommitCID, reader.Header.Roots[0].String())
	first, err := reader.Next()
	require.NoError(t, err)
	assert.Equal(t, genesis.CommitCID, first.Cid().String(), "commit/root block must come first")
	sawRecord := false
	for {
		block, err := reader.Next()
		if err != nil {
			break
		}
		if block.Cid().String() == genesis.RecordCID {
			sawRecord = true
		}
	}
	assert.True(t, sawRecord, "CAR slice must contain the record block")

	evt = readFrame(t, conn, 5*time.Second)
	require.NotNil(t, evt.RepoCommit)
	assert.Equal(t, update.Seq, evt.RepoCommit.Seq)
	require.NotNil(t, evt.RepoCommit.Since)
	assert.Equal(t, genesis.Rev, *evt.RepoCommit.Since)
	require.NotNil(t, evt.RepoCommit.PrevData, "non-genesis commit must carry prevData")
}

// TestSubscribeRepos_CaughtUpCursorReconnect pins the single most common
// real-world input: a consumer reconnecting with cursor == newest. It must
// get no error/info frame and then receive the next live event — a `>=`
// regression in the FutureCursor check would terminally error every
// caught-up reconnect.
func TestSubscribeRepos_CaughtUpCursorReconnect(t *testing.T) {
	h := newHarness(t)
	last := putRecord(t, h, "c01", "caught up")

	conn := h.dial(t, fmt.Sprintf("%d", last.Seq))
	waitForSubscribers(t, h, 1)

	live := putRecord(t, h, "c02", "next one")
	evt := readFrame(t, conn, 5*time.Second)
	require.NotNil(t, evt.RepoCommit, "caught-up reconnect must stream, not error: %+v", evt)
	assert.Equal(t, live.Seq, evt.RepoCommit.Seq)
}

// TestSubscribeRepos_UpdateAndDeleteFrames pushes update and delete commits
// through commitFrame on the wire — the branches Jetstream actually decodes
// for edits and deletions (op.Prev set; delete has no CID).
func TestSubscribeRepos_UpdateAndDeleteFrames(t *testing.T) {
	h := newHarness(t)
	created := putRecord(t, h, "ud1", "v1")
	updated := putRecord(t, h, "ud1", "v2")
	deleted, err := h.manager.DeleteRecord(context.Background(), testDID, testCollection, "ud1")
	require.NoError(t, err)

	conn := h.dial(t, fmt.Sprintf("%d", created.Seq))

	evt := readFrame(t, conn, 5*time.Second)
	require.NotNil(t, evt.RepoCommit)
	assert.Equal(t, updated.Seq, evt.RepoCommit.Seq)
	require.Len(t, evt.RepoCommit.Ops, 1)
	assert.Equal(t, string(repo.OpActionUpdate), evt.RepoCommit.Ops[0].Action)
	require.NotNil(t, evt.RepoCommit.Ops[0].Cid, "update op carries the new record CID")
	assert.Equal(t, updated.RecordCID, cid.Cid(*evt.RepoCommit.Ops[0].Cid).String())
	require.NotNil(t, evt.RepoCommit.Ops[0].Prev, "update op carries the previous record CID")
	assert.Equal(t, created.RecordCID, cid.Cid(*evt.RepoCommit.Ops[0].Prev).String())

	evt = readFrame(t, conn, 5*time.Second)
	require.NotNil(t, evt.RepoCommit)
	assert.Equal(t, deleted.Seq, evt.RepoCommit.Seq)
	require.Len(t, evt.RepoCommit.Ops, 1)
	assert.Equal(t, string(repo.OpActionDelete), evt.RepoCommit.Ops[0].Action)
	assert.Nil(t, evt.RepoCommit.Ops[0].Cid, "delete op has no new CID")
	require.NotNil(t, evt.RepoCommit.Ops[0].Prev)
	assert.Equal(t, updated.RecordCID, cid.Cid(*evt.RepoCommit.Ops[0].Prev).String())
	// The delete frame's CAR still parses and roots at the commit.
	reader, err := car.NewCarReader(bytes.NewReader(evt.RepoCommit.Blocks))
	require.NoError(t, err)
	assert.Equal(t, deleted.CommitCID, reader.Header.Roots[0].String())
}

// TestSubscribeRepos_ListenOnlyLiveTail makes the LISTEN/NOTIFY pipeline
// load-bearing: with the poll fallback effectively disabled, only a real
// NOTIFY → broadcaster → outbox wake-up can deliver the live event.
func TestSubscribeRepos_ListenOnlyLiveTail(t *testing.T) {
	h := newHarnessWithPoll(t, time.Hour)

	conn := h.dial(t, "")
	waitForSubscribers(t, h, 1)

	live := putRecord(t, h, "ln1", "listen-only")
	evt := readFrame(t, conn, 5*time.Second)
	require.NotNil(t, evt.RepoCommit)
	assert.Equal(t, live.Seq, evt.RepoCommit.Seq)
}

// TestSubscribeRepos_IdleConnectionSurvivesPings pins the pingPump contract:
// an idle consumer that answers pings must not be evicted, even across many
// multiples of the read deadline (3×pingInterval).
func TestSubscribeRepos_IdleConnectionSurvivesPings(t *testing.T) {
	h := newHarness(t)
	h.server.pingInterval = 100 * time.Millisecond // read deadline: 300ms

	conn := h.dial(t, "")
	waitForSubscribers(t, h, 1)

	// Write only after 4× the read deadline of idle time. The client blocks
	// in ReadMessage the whole while, which is what makes gorilla answer the
	// server's pings — a stalled pingPump would evict us long before the
	// frame arrives.
	seqCh := make(chan int64, 1)
	go func() {
		time.Sleep(1200 * time.Millisecond)
		res, err := h.manager.PutRecord(context.Background(), testDID, testCollection,
			"idle1", testRecord("still here"))
		if err != nil {
			seqCh <- -1
			return
		}
		seqCh <- res.Seq
	}()
	evt := readFrame(t, conn, 10*time.Second)
	require.NotNil(t, evt.RepoCommit, "idle connection answering pings must survive")
	assert.Equal(t, <-seqCh, evt.RepoCommit.Seq)
}

// TestSubscribeRepos_OutdatedCursorBoundary pins the oldest-1 boundary: a
// cursor exactly one below the oldest retained seq missed nothing and must
// NOT get the OutdatedCursor warning.
func TestSubscribeRepos_OutdatedCursorBoundary(t *testing.T) {
	h := newHarness(t)
	results := make([]*repo.CommitResult, 6)
	for i := range results {
		results[i] = putRecord(t, h, fmt.Sprintf("b%02d", i), fmt.Sprintf("post %d", i))
	}
	db := testutil.DB(t)
	_, err := db.Exec(`DELETE FROM firehose_events WHERE seq <= $1`, results[2].Seq)
	require.NoError(t, err)

	// oldest is results[3].Seq; cursor == oldest-1 == results[2].Seq.
	conn := h.dial(t, fmt.Sprintf("%d", results[2].Seq))
	evt := readFrame(t, conn, 5*time.Second)
	require.NotNil(t, evt.RepoCommit, "cursor == oldest-1 missed nothing; got %+v", evt)
	assert.Equal(t, results[3].Seq, evt.RepoCommit.Seq)
}

// TestSubscribeRepos_PruneOvertakesReplay covers the mid-stream data-loss
// signal: when the pruner deletes events past a connected consumer's
// position, the consumer must receive #info OutdatedCursor before the
// stream continues — a silent gap is indistinguishable from benign seq
// holes and would desync it forever.
func TestSubscribeRepos_PruneOvertakesReplay(t *testing.T) {
	// Poll fallback disabled: the subscriber must stay asleep while we build
	// the hole (SQL writes emit no NOTIFY), then wake exactly once on the
	// trigger commit and face the gap.
	h := newHarnessWithPoll(t, time.Hour)
	db := testutil.DB(t)

	results := make([]*repo.CommitResult, 4)
	for i := range results {
		results[i] = putRecord(t, h, fmt.Sprintf("po%02d", i), fmt.Sprintf("post %d", i))
	}

	// Connect fully caught up; the outbox idles awaiting a wake-up.
	conn := h.dial(t, fmt.Sprintf("%d", results[3].Seq))
	waitForSubscribers(t, h, 1)

	// Grow the log via raw SQL (no NOTIFY): three copies of the last event
	// take the next three seqs, then a prefix delete removes everything but
	// the last copy — a genuine pruned hole in front of the sleeping cursor.
	// One transaction, so a stray wake-up (stale NOTIFY from the setup
	// writes) can never observe the copies before the hole is carved.
	tx, err := db.Begin()
	require.NoError(t, err)
	var lastCopy int64
	for i := 0; i < 3; i++ {
		require.NoError(t, tx.QueryRow(`
			INSERT INTO firehose_events (did, commit_cid, prev_data_cid, since_rev, rev, ops, car, created_at)
			SELECT did, commit_cid, prev_data_cid, since_rev, rev, ops, car, created_at
			FROM firehose_events WHERE seq = $1
			RETURNING seq`, results[3].Seq).Scan(&lastCopy))
	}
	_, err = tx.Exec(`DELETE FROM firehose_events WHERE seq < $1`, lastCopy)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	trigger := putRecord(t, h, "po-live", "wake up")

	evt := readFrame(t, conn, 5*time.Second)
	require.NotNil(t, evt.RepoInfo, "pruned-past consumer must be told, got %+v", evt)
	assert.Equal(t, InfoOutdatedCursor, evt.RepoInfo.Name)

	evt = readFrame(t, conn, 5*time.Second)
	require.NotNil(t, evt.RepoCommit)
	assert.Equal(t, lastCopy, evt.RepoCommit.Seq, "stream resumes at the oldest retained event")
	evt = readFrame(t, conn, 5*time.Second)
	require.NotNil(t, evt.RepoCommit)
	assert.Equal(t, trigger.Seq, evt.RepoCommit.Seq)
}

// TestSubscribeRepos_EvictThenResumeAtCursor pins the documented recovery
// contract: an evicted (or dropped) consumer reconnects with its last seq
// and resumes exactly where it left off.
func TestSubscribeRepos_EvictThenResumeAtCursor(t *testing.T) {
	h := newHarness(t)
	results := make([]*repo.CommitResult, 6)
	for i := range results {
		results[i] = putRecord(t, h, fmt.Sprintf("rs%02d", i), fmt.Sprintf("post %d", i))
	}

	conn := h.dial(t, "0")
	var lastSeen int64
	for i := 0; i < 3; i++ {
		evt := readFrame(t, conn, 5*time.Second)
		require.NotNil(t, evt.RepoCommit)
		lastSeen = evt.RepoCommit.Seq
	}
	require.NoError(t, conn.Close()) // simulate drop/eviction mid-stream

	conn2 := h.dial(t, fmt.Sprintf("%d", lastSeen))
	for i := 3; i < 6; i++ {
		evt := readFrame(t, conn2, 5*time.Second)
		require.NotNil(t, evt.RepoCommit)
		assert.Equal(t, results[i].Seq, evt.RepoCommit.Seq, "resume must continue exactly after the last seen seq")
	}
}

// TestSubscribeRepos_OutdatedCursor prunes the head of the log, connects
// below the retained window, and expects #info OutdatedCursor followed by
// everything still retained.
func TestSubscribeRepos_OutdatedCursor(t *testing.T) {
	h := newHarness(t)

	results := make([]*repo.CommitResult, 10)
	for i := range results {
		results[i] = putRecord(t, h, fmt.Sprintf("o%02d", i), fmt.Sprintf("post %d", i))
	}
	// PruneEvents trims by age; carve the precise seq window with SQL so the
	// test doesn't depend on wall-clock timing.
	db := testutil.DB(t)
	_, err := db.Exec(`DELETE FROM firehose_events WHERE seq <= $1`, results[4].Seq)
	require.NoError(t, err)

	conn := h.dial(t, "1")
	evt := readFrame(t, conn, 5*time.Second)
	require.NotNil(t, evt.RepoInfo, "expected #info frame, got %+v", evt)
	assert.Equal(t, "OutdatedCursor", evt.RepoInfo.Name)

	for i := 5; i < 10; i++ {
		evt := readFrame(t, conn, 5*time.Second)
		require.NotNil(t, evt.RepoCommit)
		assert.Equal(t, results[i].Seq, evt.RepoCommit.Seq)
	}
}

// TestSubscribeRepos_FutureCursor expects a terminal error frame when the
// cursor is ahead of the newest seq.
func TestSubscribeRepos_FutureCursor(t *testing.T) {
	h := newHarness(t)
	putRecord(t, h, "f01", "only post")

	conn := h.dial(t, "999999")
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	msgType, payload, err := conn.ReadMessage()
	require.NoError(t, err)
	require.Equal(t, websocket.BinaryMessage, msgType)
	var evt events.XRPCStreamEvent
	require.NoError(t, evt.Deserialize(bytes.NewReader(payload)))
	require.NotNil(t, evt.Error, "expected error frame")
	assert.Equal(t, "FutureCursor", evt.Error.Error)

	// The spec requires the stream to close after an error frame.
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	_, _, err = conn.ReadMessage()
	assert.Error(t, err, "stream must close after a terminal error frame")
}

// TestSubscribeRepos_InvalidCursorRejected covers the pre-upgrade validation.
func TestSubscribeRepos_InvalidCursorRejected(t *testing.T) {
	h := newHarness(t)
	resp, err := http.Get(h.http.URL + "/xrpc/com.atproto.sync.subscribeRepos?cursor=banana")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// TestSubscribeRepos_SlowConsumerEvicted starves one subscriber (tiny socket
// buffers, client never reads) and asserts the server evicts it within the
// write timeout while a healthy subscriber keeps receiving everything.
func TestSubscribeRepos_SlowConsumerEvicted(t *testing.T) {
	h := newHarness(t)
	h.server.writeTimeout = 500 * time.Millisecond
	// Shrink the server-side send buffer for the FIRST connection only (the
	// slow client dials first): shrinking every socket would subject the
	// healthy subscriber to the same tight timeout under CI scheduler
	// stalls and flake the test in the wrong direction.
	var shrunk atomic.Bool
	h.server.onUpgrade = func(conn *websocket.Conn) {
		if !shrunk.CompareAndSwap(false, true) {
			return
		}
		if tcp, ok := conn.UnderlyingConn().(*net.TCPConn); ok {
			_ = tcp.SetWriteBuffer(1 << 10)
		}
	}

	// Slow client: tiny receive buffer, and we never read from it.
	dialer := websocket.Dialer{
		NetDialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			tcp, ok := conn.(*net.TCPConn)
			require.True(t, ok, "test requires a TCP conn to shrink buffers")
			_ = tcp.SetReadBuffer(1 << 10)
			return conn, nil
		},
	}
	slow, resp, err := dialer.Dial(h.wsURL(""), nil)
	require.NoError(t, err)
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	defer slow.Close()

	healthy := h.dial(t, "")
	waitForSubscribers(t, h, 2)

	// Big frames fill the tiny socket buffers fast; the slow consumer's
	// writes then block past writeTimeout and it gets dropped.
	payload := strings.Repeat("x", 1<<16)
	const writes = 20
	results := make([]*repo.CommitResult, writes)
	for i := range results {
		results[i] = putRecord(t, h, fmt.Sprintf("s%02d", i), payload)
	}

	for i := 0; i < writes; i++ {
		evt := readFrame(t, healthy, 10*time.Second)
		require.NotNil(t, evt.RepoCommit)
		assert.Equal(t, results[i].Seq, evt.RepoCommit.Seq, "healthy subscriber must see every event, in order")
	}

	require.Eventually(t, func() bool { return h.broadcaster.subscriberCount() == 1 },
		10*time.Second, 50*time.Millisecond, "slow consumer must be evicted")
}

// TestRunPruner ages part of the log past retention and waits for the
// background pruner to trim exactly that part.
func TestRunPruner(t *testing.T) {
	h := newHarness(t)
	db := testutil.DB(t)

	results := make([]*repo.CommitResult, 5)
	for i := range results {
		results[i] = putRecord(t, h, fmt.Sprintf("p%02d", i), fmt.Sprintf("post %d", i))
	}
	_, err := db.Exec(`UPDATE firehose_events SET created_at = now() - interval '2 hours' WHERE seq <= $1`,
		results[2].Seq)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go RunPruner(ctx, h.manager, time.Hour, 50*time.Millisecond, nil)

	require.Eventually(t, func() bool {
		oldest, newest, err := h.manager.SeqBounds(context.Background())
		return err == nil && oldest == results[3].Seq && newest == results[4].Seq
	}, 10*time.Second, 50*time.Millisecond, "pruner must trim events older than retention and nothing else")
}

func getJSON(t *testing.T, url string, out any) *http.Response {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	if out != nil {
		require.NoError(t, json.NewDecoder(resp.Body).Decode(out))
	}
	return resp
}

func TestHTTPEndpoints(t *testing.T) {
	h := newHarness(t)
	first := putRecord(t, h, "h01", "first")
	head := putRecord(t, h, "h02", "second")
	base := h.http.URL

	t.Run("health", func(t *testing.T) {
		var body map[string]string
		resp := getJSON(t, base+"/xrpc/_health", &body)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, healthVersion, body["version"])
	})

	t.Run("describeServer", func(t *testing.T) {
		var body comatproto.ServerDescribeServer_Output
		resp := getJSON(t, base+"/xrpc/com.atproto.server.describeServer", &body)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "did:web:"+testHostname, body.Did)
		assert.Equal(t, []string{"." + testHostname}, body.AvailableUserDomains)
		require.NotNil(t, body.InviteCodeRequired)
		assert.True(t, *body.InviteCodeRequired, "the bridge never offers account creation")
	})

	t.Run("getLatestCommit", func(t *testing.T) {
		var body comatproto.SyncGetLatestCommit_Output
		resp := getJSON(t, base+"/xrpc/com.atproto.sync.getLatestCommit?did="+testDID, &body)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, head.CommitCID, body.Cid)
		assert.Equal(t, head.Rev, body.Rev)
	})

	t.Run("getLatestCommit unknown DID", func(t *testing.T) {
		var body map[string]string
		resp := getJSON(t, base+"/xrpc/com.atproto.sync.getLatestCommit?did=did:plc:doesnotexistatall", &body)
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
		assert.Equal(t, "RepoNotFound", body["error"])
	})

	t.Run("getRecord proof CAR", func(t *testing.T) {
		client := &http.Client{Timeout: 5 * time.Second}
		resp, err := client.Get(base + "/xrpc/com.atproto.sync.getRecord?did=" + testDID +
			"&collection=" + testCollection + "&rkey=h01")
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, "application/vnd.ipld.car", resp.Header.Get("Content-Type"))
		reader, err := car.NewCarReader(resp.Body)
		require.NoError(t, err)
		require.Len(t, reader.Header.Roots, 1)
		assert.Equal(t, head.CommitCID, reader.Header.Roots[0].String(),
			"proof root must be the current head commit")
		sawRecord := false
		for {
			block, err := reader.Next()
			if err != nil {
				break
			}
			if block.Cid().String() == first.RecordCID {
				sawRecord = true
			}
		}
		assert.True(t, sawRecord, "proof CAR must contain the record block")
	})

	t.Run("getRecord missing", func(t *testing.T) {
		var body map[string]string
		resp := getJSON(t, base+"/xrpc/com.atproto.sync.getRecord?did="+testDID+
			"&collection="+testCollection+"&rkey=nope", &body)
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
		assert.Equal(t, "RecordNotFound", body["error"])
	})

	t.Run("getRepo full CAR", func(t *testing.T) {
		client := &http.Client{Timeout: 5 * time.Second}
		resp, err := client.Get(base + "/xrpc/com.atproto.sync.getRepo?did=" + testDID)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		reader, err := car.NewCarReader(resp.Body)
		require.NoError(t, err)
		require.Len(t, reader.Header.Roots, 1)
		assert.Equal(t, head.CommitCID, reader.Header.Roots[0].String())
	})

	t.Run("getRepo with since param serves full CAR", func(t *testing.T) {
		// The optional diff-export param is documented as ignored (full CAR
		// is spec-legal); it must not 400.
		client := &http.Client{Timeout: 5 * time.Second}
		resp, err := client.Get(base + "/xrpc/com.atproto.sync.getRepo?did=" + testDID + "&since=" + first.Rev)
		require.NoError(t, err)
		defer resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("getRepoStatus", func(t *testing.T) {
		var body comatproto.SyncGetRepoStatus_Output
		resp := getJSON(t, base+"/xrpc/com.atproto.sync.getRepoStatus?did="+testDID, &body)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, testDID, body.Did)
		assert.True(t, body.Active)
		require.NotNil(t, body.Rev)
		assert.Equal(t, head.Rev, *body.Rev)
	})
}

// TestHTTPEndpoints_DeactivatedRepo pins the consent surface: content
// endpoints refuse a tombstoned actor's repo; getRepoStatus reports it.
func TestHTTPEndpoints_DeactivatedRepo(t *testing.T) {
	h := newHarness(t)
	putRecord(t, h, "d01", "soon to be frozen")
	db := testutil.DB(t)

	actors := store.NewBridgedActors(db)
	_, err := actors.UpsertActor(context.Background(), store.BridgedActor{
		APActorID:    "https://lemmy.example/u/tombstoned",
		ActorType:    store.ActorTypePerson,
		DID:          testDID,
		ConsentState: store.ConsentStateDeleted,
	})
	require.NoError(t, err)

	base := h.http.URL
	for _, endpoint := range []string{
		"/xrpc/com.atproto.sync.getRepo?did=" + testDID,
		"/xrpc/com.atproto.sync.getLatestCommit?did=" + testDID,
		"/xrpc/com.atproto.sync.getRecord?did=" + testDID + "&collection=" + testCollection + "&rkey=d01",
	} {
		var body map[string]string
		resp := getJSON(t, base+endpoint, &body)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode, endpoint)
		assert.Equal(t, "RepoDeactivated", body["error"], endpoint)
	}

	var status comatproto.SyncGetRepoStatus_Output
	resp := getJSON(t, base+"/xrpc/com.atproto.sync.getRepoStatus?did="+testDID, &status)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.False(t, status.Active)
	require.NotNil(t, status.Status)
	assert.Equal(t, "deleted", *status.Status)
}

// TestGetRepo_MidStreamFailureAbortsConnection pins the streaming failure
// contract: once bytes are on the wire the 200 cannot be revoked, and letting
// the server write the terminating chunk would hand the client a
// transport-complete response wrapping a silently truncated CAR. The handler
// must abort the connection instead, so the client sees a transport-level
// read error rather than a clean EOF.
func TestGetRepo_MidStreamFailureAbortsConnection(t *testing.T) {
	h := newHarness(t)
	putRecord(t, h, "s01", "repo exists")

	h.server.exportCAR = func(ctx context.Context, did string, w io.Writer) error {
		if _, err := w.Write([]byte("carv1 header and first block")); err != nil {
			return err
		}
		// Push the 200 header and first bytes to the client before failing,
		// so the truncation lands mid-body rather than pre-header.
		w.(*countingWriter).w.(http.Flusher).Flush()
		return fmt.Errorf("reachable walk: block row unreadable")
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(h.http.URL + "/xrpc/com.atproto.sync.getRepo?did=" + testDID)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	_, readErr := io.ReadAll(resp.Body)
	require.Error(t, readErr, "truncated CAR must surface as a transport failure, not a clean EOF")
}

// TestGetRepo_VanishedRepoIs404 pins the delete race: a repo that disappears
// between loadActiveRepo and the export has written nothing yet, so the
// handler must still answer with the same 404 RepoNotFound the earlier
// existence check uses, not a 500.
func TestGetRepo_VanishedRepoIs404(t *testing.T) {
	h := newHarness(t)
	putRecord(t, h, "s02", "repo exists")

	h.server.exportCAR = func(ctx context.Context, did string, w io.Writer) error {
		return errors.NewNotFoundError("repo", did)
	}

	var body map[string]string
	resp := getJSON(t, h.http.URL+"/xrpc/com.atproto.sync.getRepo?did="+testDID, &body)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, "RepoNotFound", body["error"])
}

// TestGetBlob pins the com.atproto.sync.getBlob surface task 05 added: a
// stored blob round-trips with its content type, misses 404, and
// deactivated repos refuse to serve.
func TestGetBlob(t *testing.T) {
	h := newHarness(t)
	putRecord(t, h, "b01", "repo exists")

	payload := []byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 1, 2, 3}
	blob, err := h.manager.PutBlob(context.Background(), testDID, "image/png", payload)
	require.NoError(t, err)

	// Idempotent re-put of the same bytes yields the same CID.
	again, err := h.manager.PutBlob(context.Background(), testDID, "image/png", payload)
	require.NoError(t, err)
	assert.Equal(t, blob.Ref.String(), again.Ref.String())

	url := h.http.URL + "/xrpc/com.atproto.sync.getBlob?did=" + testDID + "&cid=" + blob.Ref.String()
	resp, err := http.Get(url)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "image/png", resp.Header.Get("Content-Type"))
	assert.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))
	assert.Equal(t, payload, body)

	// Unknown CID → BlobNotFound.
	var errBody map[string]string
	resp = getJSON(t, h.http.URL+"/xrpc/com.atproto.sync.getBlob?did="+testDID+
		"&cid=bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku", &errBody)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, "BlobNotFound", errBody["error"])

	// Deactivated repo refuses blobs like every other content endpoint.
	actors := store.NewBridgedActors(testutil.DB(t))
	_, err = actors.UpsertActor(context.Background(), store.BridgedActor{
		APActorID:    "https://lemmy.example/u/blobowner",
		ActorType:    store.ActorTypePerson,
		DID:          testDID,
		ConsentState: store.ConsentStateDeleted,
	})
	require.NoError(t, err)
	resp = getJSON(t, url, &errBody)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "RepoDeactivated", errBody["error"])
}

func TestListReposPagination(t *testing.T) {
	h := newHarness(t)
	dids := []string{
		"did:plc:aaaaaaaaaaaaaaaaaaaaaaaa",
		"did:plc:bbbbbbbbbbbbbbbbbbbbbbbb",
		"did:plc:cccccccccccccccccccccccc",
	}
	for i, did := range dids {
		_, err := h.manager.PutRecord(context.Background(), did, testCollection,
			fmt.Sprintf("l%02d", i), testRecord(fmt.Sprintf("repo %d", i)))
		require.NoError(t, err)
	}

	var page1 comatproto.SyncListRepos_Output
	resp := getJSON(t, h.http.URL+"/xrpc/com.atproto.sync.listRepos?limit=2", &page1)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	require.Len(t, page1.Repos, 2)
	assert.Equal(t, dids[0], page1.Repos[0].Did)
	assert.Equal(t, dids[1], page1.Repos[1].Did)
	require.NotNil(t, page1.Cursor)

	var page2 comatproto.SyncListRepos_Output
	resp = getJSON(t, h.http.URL+"/xrpc/com.atproto.sync.listRepos?limit=2&cursor="+*page1.Cursor, &page2)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	require.Len(t, page2.Repos, 1)
	assert.Equal(t, dids[2], page2.Repos[0].Did)
	assert.Nil(t, page2.Cursor, "final page must not return a cursor")
}

func TestRequestCrawl(t *testing.T) {
	var gotPath string
	var gotBody comatproto.SyncRequestCrawl_Input
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		w.WriteHeader(http.StatusOK)
	}))
	defer relay.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 5 * time.Second}

	require.NoError(t, RequestCrawl(ctx, client, relay.URL, testHostname))
	assert.Equal(t, "/xrpc/com.atproto.sync.requestCrawl", gotPath)
	assert.Equal(t, testHostname, gotBody.Hostname)

	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer failing.Close()
	assert.Error(t, RequestCrawl(ctx, client, failing.URL, testHostname))

	assert.Error(t, RequestCrawl(ctx, client, relay.URL, ""), "empty hostname must be rejected")
	assert.Error(t, RequestCrawl(ctx, nil, relay.URL, testHostname), "nil client must be rejected")
}

// testLogger returns a discard logger: the crawl tests assert outcomes via
// call counts, and retry chatter should never pollute test output.
func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// compressCrawlRetries shrinks the startup-crawl retry schedule for tests.
func compressCrawlRetries(t *testing.T, interval time.Duration, attempts int) {
	t.Helper()
	prevInterval, prevAttempts := crawlRetryInterval, crawlMaxAttempts
	crawlRetryInterval, crawlMaxAttempts = interval, attempts
	t.Cleanup(func() { crawlRetryInterval, crawlMaxAttempts = prevInterval, prevAttempts })
}

func TestRequestCrawlAll_RetriesTransientFailures(t *testing.T) {
	compressCrawlRetries(t, 10*time.Millisecond, 10)

	// The relay fails twice before accepting — the startup race in miniature
	// (relay booting / bridge listener not yet up when the relay probes back).
	var calls atomic.Int64
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 2 {
			http.Error(w, "still booting", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer relay.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	RequestCrawlAll(ctx, &http.Client{Timeout: time.Second}, []string{relay.URL}, testHostname, testLogger(t))
	assert.EqualValues(t, 3, calls.Load(), "two failures then the success — no extra attempts after that")
}

func TestRequestCrawlAll_BudgetExhaustionAndIndependence(t *testing.T) {
	compressCrawlRetries(t, time.Millisecond, 3)

	var deadCalls atomic.Int64
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		deadCalls.Add(1)
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer dead.Close()
	var liveCalls atomic.Int64
	live := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		liveCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer live.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	RequestCrawlAll(ctx, &http.Client{Timeout: time.Second}, []string{dead.URL, live.URL}, testHostname, testLogger(t))
	assert.EqualValues(t, 3, deadCalls.Load(), "the dead relay gets exactly its budget")
	assert.EqualValues(t, 1, liveCalls.Load(), "a dead relay must not block or repeat the healthy one")
}

func TestRequestCrawlAll_ContextCancelStopsRetrying(t *testing.T) {
	compressCrawlRetries(t, time.Hour, 100) // only cancellation can end the wait

	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	defer relay.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		RequestCrawlAll(ctx, &http.Client{Timeout: time.Second}, []string{relay.URL}, testHostname, testLogger(t))
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("RequestCrawlAll kept waiting after context cancellation")
	}
}

func TestRequestCrawlAll_ValidationErrorNotRetried(t *testing.T) {
	// The hour-long interval is the tripwire: if the pre-flight caller-bug
	// error were classified as retryable, the goroutine would sleep an hour
	// before attempt 2 and the deadline below would fire.
	compressCrawlRetries(t, time.Hour, 5)

	var calls atomic.Int64
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer relay.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		// Empty hostname is a caller bug (validation error): terminal, no retry.
		RequestCrawlAll(context.Background(), &http.Client{Timeout: time.Second}, []string{relay.URL}, "", testLogger(t))
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a pre-flight validation error was retried instead of being terminal")
	}
	assert.EqualValues(t, 0, calls.Load(), "validation failures must not reach the wire, let alone retry")
}

// TestRequestCrawlAll_BadRequestIsRetried pins DELIBERATE behavior: HTTP 4xx
// from a relay is retried like any other wire-level failure. Bigsky answers
// 400 while the describeServer callback race is unresolved (it probes our
// listener before subscribing), so treating 400 as terminal would defeat the
// exact race this retry loop exists for.
func TestRequestCrawlAll_BadRequestIsRetried(t *testing.T) {
	compressCrawlRetries(t, time.Millisecond, 10)

	var calls atomic.Int64
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 2 {
			http.Error(w, "host failed to verify", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer relay.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	RequestCrawlAll(ctx, &http.Client{Timeout: time.Second}, []string{relay.URL}, testHostname, testLogger(t))
	assert.EqualValues(t, 3, calls.Load(), "two 400s then the success — 4xx must be retried, and success must stop the loop")
}

// TestRequestCrawlAll_AttemptTimeoutBoundsHangingRelay pins the per-attempt
// cap (crawlAttemptTimeout): a relay that hangs cannot stretch each attempt
// to the HTTP client's full timeout, keeping the documented budget honest.
func TestRequestCrawlAll_AttemptTimeoutBoundsHangingRelay(t *testing.T) {
	compressCrawlRetries(t, time.Millisecond, 2)
	prevTimeout := crawlAttemptTimeout
	crawlAttemptTimeout = 50 * time.Millisecond
	t.Cleanup(func() { crawlAttemptTimeout = prevTimeout })

	release := make(chan struct{})
	var calls atomic.Int64
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		<-release // hang until the test ends
	}))
	defer relay.Close()
	defer close(release) // LIFO: unblock hung handlers before relay.Close waits on them

	start := time.Now()
	// The client's own timeout (10s) is far above the attempt cap: only the
	// per-attempt context can keep this fast.
	RequestCrawlAll(context.Background(), &http.Client{Timeout: 10 * time.Second}, []string{relay.URL}, testHostname, testLogger(t))
	elapsed := time.Since(start)
	assert.EqualValues(t, 2, calls.Load(), "a hanging relay still gets its full attempt budget")
	assert.Less(t, elapsed, 5*time.Second, "attempts must be bounded by crawlAttemptTimeout, not the client timeout")
}
