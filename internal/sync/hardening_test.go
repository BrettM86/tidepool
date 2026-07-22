package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/repo"
)

// Task 11 hardening tests: the #account frame on the wire, the
// subscribeRepos connection cap, and the per-IP rate limit on the public
// surface.

// TestSubscribeRepos_AccountFrame: an account event appended to the durable
// log is served as a real #account frame — replayed from a cursor AND
// live-tailed — with the commit that preceded it arriving first (per-repo
// order: scrub commits, then the purge signal).
func TestSubscribeRepos_AccountFrame(t *testing.T) {
	h := newHarness(t)

	commit := putRecord(t, h, "acct1", "doomed record")
	seq, err := h.manager.AppendAccountEvent(context.Background(), testDID, false, repo.AccountStatusDeleted)
	require.NoError(t, err)

	// Replay path: cursor 0 serves the commit, then the account frame.
	conn := h.dial(t, "0")
	evt := readFrame(t, conn, 5*time.Second)
	require.NotNil(t, evt.RepoCommit, "the scrub commit precedes the account frame")
	assert.Equal(t, commit.Seq, evt.RepoCommit.Seq)

	evt = readFrame(t, conn, 5*time.Second)
	require.NotNil(t, evt.RepoAccount, "expected an #account frame")
	assert.Equal(t, seq, evt.RepoAccount.Seq)
	assert.Equal(t, testDID, evt.RepoAccount.Did)
	assert.False(t, evt.RepoAccount.Active)
	require.NotNil(t, evt.RepoAccount.Status)
	assert.Equal(t, repo.AccountStatusDeleted, *evt.RepoAccount.Status)
	assert.NotEmpty(t, evt.RepoAccount.Time)

	// Live-tail path: a second subscriber sees a fresh account event pushed
	// through the broadcaster like any commit.
	live := h.dial(t, "")
	waitForSubscribers(t, h, 2)
	liveSeq, err := h.manager.AppendAccountEvent(context.Background(), testDID, false, repo.AccountStatusDeleted)
	require.NoError(t, err)
	evt = readFrame(t, live, 5*time.Second)
	require.NotNil(t, evt.RepoAccount)
	assert.Equal(t, liveSeq, evt.RepoAccount.Seq)
}

// TestSubscribeRepos_ConnectionCap: the cap refuses the N+1th concurrent
// subscriber with 429 and frees the slot when a connection closes.
func TestSubscribeRepos_ConnectionCap(t *testing.T) {
	h := newHarnessWithPoll(t, 500*time.Millisecond, func(o *Options) { o.MaxSubscribers = 2 })

	first := h.dial(t, "")
	_ = h.dial(t, "")
	waitForSubscribers(t, h, 2)

	_, resp, err := websocket.DefaultDialer.Dial(h.wsURL(""), nil)
	require.Error(t, err, "the third concurrent subscriber must be refused")
	require.NotNil(t, resp)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	var body struct {
		Error string `json:"error"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "SubscriberLimitExceeded", body.Error)

	// Closing one connection frees its slot.
	require.NoError(t, first.Close())
	require.Eventually(t, func() bool {
		conn, resp, err := websocket.DefaultDialer.Dial(h.wsURL(""), nil)
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, 5*time.Second, 50*time.Millisecond, "a freed slot must admit a new subscriber")
}

// TestSyncSurface_PerIPRateLimit: past the burst, HTTP reads answer 429
// RateLimitExceeded; _health stays exempt (container healthchecks must
// never flap on a limiter).
func TestSyncSurface_PerIPRateLimit(t *testing.T) {
	h := newHarnessWithPoll(t, 500*time.Millisecond, func(o *Options) {
		o.RatePerSecond = 0.001 // effectively no refill inside the test
		o.RateBurst = 3
	})

	url := h.http.URL + "/xrpc/com.atproto.sync.listRepos"
	status := func(u string) int {
		resp, err := http.Get(u)
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode
	}

	for i := 0; i < 3; i++ {
		assert.Equal(t, http.StatusOK, status(url), "request %d within the burst", i+1)
	}
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusTooManyRequests, resp.StatusCode)
	var body struct {
		Error string `json:"error"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "RateLimitExceeded", body.Error)

	// The limiter guards the whole surface, not just listRepos…
	assert.Equal(t, http.StatusTooManyRequests,
		status(h.http.URL+fmt.Sprintf("/xrpc/com.atproto.sync.getRepoStatus?did=%s", testDID)))
	assert.Equal(t, http.StatusTooManyRequests,
		status(h.http.URL+fmt.Sprintf("/xrpc/com.atproto.repo.getRecord?repo=%s&collection=%s&rkey=x", testDID, testCollection)))
	// …except the healthcheck probe.
	assert.Equal(t, http.StatusOK, status(h.http.URL+"/xrpc/_health"))
}
