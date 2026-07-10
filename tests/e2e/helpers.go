//go:build e2e

// Package e2e drives the docker-compose.e2e.yml stack end to end: a real
// Lemmy (debug build, plain-HTTP federation) federating with Tidepool, a
// real did:plc directory backing DID minting, and a real Jetstream decoding
// the bridge's subscribeRepos firehose.
//
// Coves-style: E2E tests test REAL infrastructure, not mocks. Run them with
// `make e2e` (compose up --build → wait for health → go test -tags e2e →
// compose down -v), or against an already-running stack with
// `go test -tags e2e ./tests/e2e/...`.
//
// Everything here is LOCAL-ONLY: the stack never touches plc.directory,
// public relays, or public Lemmy instances.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/lexicon"
	"github.com/gorilla/websocket"

	"tidepool/lexicons"
)

// ── Configuration ──────────────────────────────────────────────────────────

// envOr reads an env var with a default (the docker-compose.e2e.yml host
// ports).
func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func tidepoolURL() string { return envOr("TIDEPOOL_E2E_URL", "http://localhost:8092") }
func lemmyURL() string    { return envOr("LEMMY_E2E_URL", "http://localhost:8541") }
func jetstreamURL() string {
	return envOr("JETSTREAM_E2E_URL", "ws://localhost:6028")
}
func adminToken() string { return envOr("TIDEPOOL_E2E_ADMIN_TOKEN", "e2e-admin-token") }

// stackTimeout bounds the initial wait-for-healthy loop. Container startup
// (Lemmy migrations, PLC boot) can be slow on a cold machine; `make e2e`
// already waited for compose health, so this is usually instant.
const stackTimeout = 5 * time.Minute

// eventTimeout bounds one wait for a federation → firehose → Jetstream
// round trip. LEMMY_TEST_FAST_FEDERATION makes deliveries near-instant, but
// leave generous slack for slow CI.
const eventTimeout = 90 * time.Second

// ── Firehose vocabulary ────────────────────────────────────────────────────

// Collections the bridge emits (task 05's materializer).
const (
	colCommunityProfile = "social.coves.community.profile"
	colActorProfile     = "social.coves.actor.profile"
	colPost             = "social.coves.community.post"
	colComment          = "social.coves.community.comment"
)

// Event kinds, operations, and rkeys as they appear on the Jetstream wire.
// Consts, not inline strings: a typo'd operation in an await predicate would
// not fail compilation — it would burn a 90s timeout instead.
const (
	kindCommit = "commit"

	opCreate = "create"
	opUpdate = "update"
	opDelete = "delete"

	rkeySelf = "self"
)

// expectedCollections is the complete set of record collections that may
// legally appear on the firehose. Anything else — vote records above all
// (PLAN.md locked decision 7: votes NEVER become records) — is a bug, and
// the listener enforces this on every consumed commit event (vetEvent).
var expectedCollections = map[string]bool{
	colCommunityProfile: true,
	colActorProfile:     true,
	colPost:             true,
	colComment:          true,
}

// ── Harness ────────────────────────────────────────────────────────────────

// harness bundles the three service endpoints plus a logged-in Lemmy admin.
type harness struct {
	http  *http.Client
	admin *lemmyClient // Lemmy admin (setup credentials from lemmy.hjson)
	// suffix makes names unique per run so the suite can run repeatedly
	// against a long-lived stack (deterministic rkeys make true re-runs
	// idempotent, but distinct communities keep scenarios independent).
	suffix string
}

var (
	stackOnce  sync.Once
	stackErr   error
	setupOnce  sync.Once
	setupErr   error
	adminJWT   string
	nameSerial int
	nameMu     sync.Mutex
)

// newHarness waits for the stack once per process, logs in the Lemmy admin,
// and (once) applies the site settings the suite needs: open registration,
// no captcha, federation on, and the tidepool host allowlisted.
func newHarness(t *testing.T) *harness {
	t.Helper()

	stackOnce.Do(func() { stackErr = waitForStack() })
	if stackErr != nil {
		t.Fatalf("e2e stack not ready: %v (start it with `make e2e-up`)", stackErr)
	}

	h := &harness{
		http:   &http.Client{Timeout: 30 * time.Second},
		suffix: strings.ToLower(fmt.Sprintf("%x", time.Now().UnixNano()%0xffffff)),
	}

	setupOnce.Do(func() { setupErr = h.setupLemmySite() })
	if setupErr != nil {
		t.Fatalf("lemmy site setup: %v", setupErr)
	}
	h.admin = &lemmyClient{h: h, jwt: adminJWT}
	return h
}

// uniqueName mints a lemmy-legal name ([a-z0-9_], ≤20 chars) unique across
// the process and across runs. Truncating would cut off the unique suffix
// and silently collide across runs, so an over-long compose is fatal.
func (h *harness) uniqueName(t *testing.T, prefix string) string {
	t.Helper()
	nameMu.Lock()
	defer nameMu.Unlock()
	nameSerial++
	name := fmt.Sprintf("%s_%s%d", prefix, h.suffix, nameSerial)
	if len(name) > 20 {
		t.Fatalf("uniqueName(%q) = %q exceeds lemmy's 20-char limit — shorten the prefix", prefix, name)
	}
	return name
}

// waitForStack polls every service the tests talk to until healthy.
// Wait-for-healthy loops, not fixed sleeps: container startup time varies
// wildly between a warm laptop and cold CI.
func waitForStack() error {
	deadline := time.Now().Add(stackTimeout)
	client := &http.Client{Timeout: 5 * time.Second}

	probes := []struct {
		name  string
		check func() error
	}{
		{"tidepool /healthz", func() error {
			return probeHTTP(client, tidepoolURL()+"/healthz")
		}},
		{"lemmy /api/v3/site", func() error {
			return probeHTTP(client, lemmyURL()+"/api/v3/site")
		}},
		{"jetstream /subscribe", func() error {
			u := jetstreamURL() + "/subscribe?cursor=" + fmt.Sprint(time.Now().UnixMicro())
			conn, _, err := websocket.DefaultDialer.Dial(u, nil)
			if err != nil {
				return err
			}
			return conn.Close()
		}},
	}

	for _, probe := range probes {
		for {
			err := probe.check()
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("%s: %w", probe.name, err)
			}
			time.Sleep(2 * time.Second)
		}
	}
	return nil
}

func probeHTTP(client *http.Client, url string) error {
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: status %d", url, resp.StatusCode)
	}
	return nil
}

// setupLemmySite logs in the setup admin and applies idempotent site
// settings: open registration (the default is require-application, which
// blocks user creation), captcha off, federation on, and — per the task
// spec — the tidepool host allowlisted (Lemmy then federates ONLY with the
// bridge, which doubles as a guard against accidental external federation).
func (h *harness) setupLemmySite() error {
	admin := &lemmyClient{h: h}
	if err := admin.login("admin", "lemmylemmy"); err != nil {
		return fmt.Errorf("admin login: %w", err)
	}
	adminJWT = admin.jwt

	body := map[string]any{
		"registration_mode":  "Open",
		"captcha_enabled":    false,
		"federation_enabled": true,
		"allowed_instances":  []string{"tidepool"},
	}
	var out json.RawMessage
	if err := admin.do(http.MethodPut, "/api/v3/site", body, &out); err != nil {
		return fmt.Errorf("edit site: %w", err)
	}
	return nil
}

// ── Lemmy HTTP API client (v3, lemmy 0.19) ─────────────────────────────────

type lemmyClient struct {
	h   *harness
	jwt string
}

// do issues one JSON API call with the client's bearer token.
func (c *lemmyClient) do(method, path string, body, out any) error {
	var reqBody io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reqBody = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, lemmyURL()+path, reqBody)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.jwt != "" {
		req.Header.Set("Authorization", "Bearer "+c.jwt)
	}
	resp, err := c.h.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("lemmy %s %s: status %d: %s", method, path, resp.StatusCode, truncate(raw, 300))
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("lemmy %s %s: decode: %w (%s)", method, path, err, truncate(raw, 200))
		}
	}
	return nil
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n]) + "…"
	}
	return string(b)
}

func (c *lemmyClient) login(user, password string) error {
	var out struct {
		JWT string `json:"jwt"`
	}
	err := c.do(http.MethodPost, "/api/v3/user/login",
		map[string]any{"username_or_email": user, "password": password}, &out)
	if err != nil {
		return err
	}
	if out.JWT == "" {
		return fmt.Errorf("login %s: empty jwt", user)
	}
	c.jwt = out.JWT
	return nil
}

// registerUser creates a fresh Lemmy user and returns a logged-in client.
func (h *harness) registerUser(t *testing.T, username string) *lemmyClient {
	t.Helper()
	c := &lemmyClient{h: h}
	password := "password_" + username
	var out struct {
		JWT string `json:"jwt"`
	}
	err := c.do(http.MethodPost, "/api/v3/user/register", map[string]any{
		"username":        username,
		"password":        password,
		"password_verify": password,
		"show_nsfw":       true,
	}, &out)
	if err != nil {
		t.Fatalf("register lemmy user %s: %v", username, err)
	}
	if out.JWT == "" {
		t.Fatalf("register lemmy user %s: no jwt in response (site requires verification?)", username)
	}
	c.jwt = out.JWT
	return c
}

type lemmyCommunity struct {
	ID   int
	Name string
	APID string
}

func (c *lemmyClient) createCommunity(t *testing.T, name, title string) lemmyCommunity {
	t.Helper()
	var out struct {
		CommunityView struct {
			Community struct {
				ID      int    `json:"id"`
				Name    string `json:"name"`
				ActorID string `json:"actor_id"`
			} `json:"community"`
		} `json:"community_view"`
	}
	err := c.do(http.MethodPost, "/api/v3/community",
		map[string]any{"name": name, "title": title}, &out)
	if err != nil {
		t.Fatalf("create community %s: %v", name, err)
	}
	comm := out.CommunityView.Community
	return lemmyCommunity{ID: comm.ID, Name: comm.Name, APID: comm.ActorID}
}

type lemmyPost struct {
	ID   int
	APID string
}

func (c *lemmyClient) createPost(t *testing.T, communityID int, title, body string) lemmyPost {
	t.Helper()
	return c.createLinkPost(t, communityID, title, body, "")
}

// createLinkPost creates a post with a shared link (url ""), which the
// bridge materializes as an embed.external. The url must stay on the
// compose network (LOCAL-ONLY) — Lemmy fetches it for opengraph metadata.
func (c *lemmyClient) createLinkPost(t *testing.T, communityID int, title, body, url string) lemmyPost {
	t.Helper()
	req := map[string]any{"name": title, "community_id": communityID, "body": body}
	if url != "" {
		req["url"] = url
	}
	var out struct {
		PostView struct {
			Post struct {
				ID   int    `json:"id"`
				APID string `json:"ap_id"`
			} `json:"post"`
		} `json:"post_view"`
	}
	if err := c.do(http.MethodPost, "/api/v3/post", req, &out); err != nil {
		t.Fatalf("create post %q: %v", title, err)
	}
	return lemmyPost{ID: out.PostView.Post.ID, APID: out.PostView.Post.APID}
}

func (c *lemmyClient) editPost(t *testing.T, postID int, newBody string) {
	t.Helper()
	if err := c.do(http.MethodPut, "/api/v3/post",
		map[string]any{"post_id": postID, "body": newBody}, nil); err != nil {
		t.Fatalf("edit post %d: %v", postID, err)
	}
}

type lemmyComment struct {
	ID   int
	APID string
}

// createComment creates a comment; parentID 0 means a top-level comment.
func (c *lemmyClient) createComment(t *testing.T, postID, parentID int, content string) lemmyComment {
	t.Helper()
	body := map[string]any{"post_id": postID, "content": content}
	if parentID != 0 {
		body["parent_id"] = parentID
	}
	var out struct {
		CommentView struct {
			Comment struct {
				ID   int    `json:"id"`
				APID string `json:"ap_id"`
			} `json:"comment"`
		} `json:"comment_view"`
	}
	if err := c.do(http.MethodPost, "/api/v3/comment", body, &out); err != nil {
		t.Fatalf("create comment on post %d: %v", postID, err)
	}
	return lemmyComment{ID: out.CommentView.Comment.ID, APID: out.CommentView.Comment.APID}
}

func (c *lemmyClient) deleteComment(t *testing.T, commentID int) {
	t.Helper()
	if err := c.do(http.MethodPost, "/api/v3/comment/delete",
		map[string]any{"comment_id": commentID, "deleted": true}, nil); err != nil {
		t.Fatalf("delete comment %d: %v", commentID, err)
	}
}

// likePost casts a vote: score 1 (up), -1 (down), 0 (retract).
func (c *lemmyClient) likePost(t *testing.T, postID, score int) {
	t.Helper()
	if err := c.do(http.MethodPost, "/api/v3/post/like",
		map[string]any{"post_id": postID, "score": score}, nil); err != nil {
		t.Fatalf("vote %+d on post %d: %v", score, postID, err)
	}
}

// likeComment casts a comment vote: score 1 (up), -1 (down), 0 (retract).
func (c *lemmyClient) likeComment(t *testing.T, commentID, score int) {
	t.Helper()
	if err := c.do(http.MethodPost, "/api/v3/comment/like",
		map[string]any{"comment_id": commentID, "score": score}, nil); err != nil {
		t.Fatalf("vote %+d on comment %d: %v", score, commentID, err)
	}
}

// ── Tidepool admin client ──────────────────────────────────────────────────

type adminCommunity struct {
	Community      string `json:"community"`
	DID            string `json:"did"`
	FollowState    string `json:"follow_state"`
	LastBackfillAt string `json:"last_backfill_at"` // RFC3339, empty until first backfill completes
}

func (h *harness) adminDo(method, path string, body, out any) error {
	var reqBody io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reqBody = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, tidepoolURL()+path, reqBody)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+adminToken())
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := h.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("tidepool %s %s: status %d: %s", method, path, resp.StatusCode, truncate(raw, 300))
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("tidepool %s %s: decode: %w", method, path, err)
		}
	}
	return nil
}

// subscribeCommunity drives POST /admin/communities until the Follow is
// accepted and returns the community's DID.
//
// The retry loop is deliberate, not paranoia: Lemmy's federation queue
// skips all activities queued before an instance's per-instance worker
// first starts ("skip all past activities", crates/federate/src/worker.rs).
// On FIRST contact the instance row is created by our Follow itself, so the
// Accept usually races the worker spawn and is dropped. Re-sending the
// Follow (each send has a fresh activity id) makes Lemmy emit a new Accept,
// which the by-now-running worker delivers. Real deployments hit the same
// race at most once (usually) per peer instance.
//
// Only pending/empty states are retried: an explicit rejection or failure
// state is a real answer and fails immediately instead of masquerading as a
// timeout.
func (h *harness) subscribeCommunity(t *testing.T, community string) adminCommunity {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	attempt := 0
	requirePending := func(state adminCommunity) {
		switch state.FollowState {
		case "", "pending", "accepted":
		default:
			t.Fatalf("subscribe %s: follow state %q (community %s) — explicit non-pending answer, not retrying",
				community, state.FollowState, state.Community)
		}
	}
	var last adminCommunity
	for {
		attempt++
		var resp adminCommunity
		if err := h.adminDo(http.MethodPost, "/admin/communities",
			map[string]any{"community": community}, &resp); err != nil {
			t.Fatalf("subscribe %s (attempt %d): %v", community, attempt, err)
		}
		last = resp
		if resp.FollowState == "accepted" {
			return resp
		}
		requirePending(resp)
		// Poll for the Accept before re-sending.
		pollUntil := time.Now().Add(10 * time.Second)
		for time.Now().Before(pollUntil) {
			time.Sleep(time.Second)
			if state, ok := h.findCommunity(t, community, resp.Community); ok {
				last = state
				if state.FollowState == "accepted" {
					return state
				}
				requirePending(state)
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("subscribe %s: follow not accepted after %d attempts (last state %q, community %s)",
				community, attempt, last.FollowState, last.Community)
		}
		t.Logf("subscribe %s: still pending after attempt %d, re-sending follow (lemmy first-contact race)", community, attempt)
	}
}

// findCommunity looks a community up in GET /admin/communities by either
// the requested handle or the resolved group IRI.
func (h *harness) findCommunity(t *testing.T, handle, groupIRI string) (adminCommunity, bool) {
	t.Helper()
	var list struct {
		Communities []adminCommunity `json:"communities"`
	}
	if err := h.adminDo(http.MethodGet, "/admin/communities", nil, &list); err != nil {
		t.Fatalf("list communities: %v", err)
	}
	for _, c := range list.Communities {
		if c.Community == groupIRI || c.Community == handle {
			return c, true
		}
	}
	return adminCommunity{}, false
}

// triggerBackfill forces a backfill run for a subscribed community.
func (h *harness) triggerBackfill(t *testing.T, community string) {
	t.Helper()
	if err := h.adminDo(http.MethodPost, "/admin/communities/backfill",
		map[string]any{"community": community}, nil); err != nil {
		t.Fatalf("trigger backfill %s: %v", community, err)
	}
}

// getVoteAggregates reads the side-channel XRPC.
type voteAggregate struct {
	URI       string `json:"uri"`
	Upvotes   int64  `json:"upvotes"`
	Downvotes int64  `json:"downvotes"`
}

func (h *harness) getVoteAggregates(t *testing.T, uris ...string) map[string]voteAggregate {
	t.Helper()
	q := url.Values{}
	for _, u := range uris {
		q.Add("uris", u)
	}
	resp, err := h.http.Get(tidepoolURL() + "/xrpc/social.coves.bridge.getVoteAggregates?" + q.Encode())
	if err != nil {
		t.Fatalf("getVoteAggregates: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("getVoteAggregates: read body: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getVoteAggregates: status %d: %s", resp.StatusCode, truncate(raw, 200))
	}
	var out struct {
		Aggregates []voteAggregate `json:"aggregates"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("getVoteAggregates: decode: %v", err)
	}
	byURI := make(map[string]voteAggregate, len(out.Aggregates))
	for _, a := range out.Aggregates {
		byURI[a.URI] = a
	}
	return byURI
}

// ── Jetstream WebSocket listener ───────────────────────────────────────────

// jsEvent is Jetstream's JSON event shape (kind "commit" only — the bridge
// emits no identity/account frames yet).
type jsEvent struct {
	Did    string    `json:"did"`
	TimeUs int64     `json:"time_us"`
	Kind   string    `json:"kind"`
	Commit *jsCommit `json:"commit"`
}

type jsCommit struct {
	Rev        string          `json:"rev"`
	Operation  string          `json:"operation"`
	Collection string          `json:"collection"`
	RKey       string          `json:"rkey"`
	Record     json.RawMessage `json:"record"`
	CID        string          `json:"cid"`
}

func (e *jsEvent) String() string {
	if e.Commit == nil {
		return fmt.Sprintf("%s %s", e.Kind, e.Did)
	}
	return fmt.Sprintf("%s %s %s/%s (%s)", e.Commit.Operation, e.Did, e.Commit.Collection, e.Commit.RKey, e.Kind)
}

// atURI is the at-uri of the committed record.
func (e *jsEvent) atURI() string {
	return fmt.Sprintf("at://%s/%s/%s", e.Did, e.Commit.Collection, e.Commit.RKey)
}

// jsListener consumes /subscribe and buffers events for matching.
type jsListener struct {
	t      *testing.T
	conn   *websocket.Conn
	events chan *jsEvent
	closed chan struct{} // closed by close(): deliberate shutdown
	done   chan struct{} // closed by readLoop's defer: goroutine exited
	once   sync.Once

	mu      sync.Mutex
	readErr error // readLoop's terminal error, nil on deliberate close
}

// setReadErr records why readLoop died — unless the listener was closed
// deliberately, in which case the read error is just the connection teardown.
func (l *jsListener) setReadErr(err error) {
	select {
	case <-l.closed:
		return
	default:
	}
	l.mu.Lock()
	l.readErr = err
	l.mu.Unlock()
}

// readError returns readLoop's terminal error (nil if the listener was
// closed deliberately or is still running).
func (l *jsListener) readError() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.readErr
}

// newListener subscribes from cursorMicros (0 = now). Collections filter
// server-side via wantedCollections; none means all collections (used for
// negative assertions like "votes never hit the firehose").
//
// Anti-flake convention (from the Coves harness): capture the cursor BEFORE
// triggering the action under test, so a subscription opened after the
// write still replays it.
func (h *harness) newListener(t *testing.T, cursorMicros int64, collections ...string) *jsListener {
	t.Helper()
	q := url.Values{}
	if cursorMicros > 0 {
		q.Set("cursor", fmt.Sprint(cursorMicros))
	}
	for _, c := range collections {
		q.Add("wantedCollections", c)
	}
	u := jetstreamURL() + "/subscribe"
	if enc := q.Encode(); enc != "" {
		u += "?" + enc
	}
	// Dial with retries: Jetstream exits when its upstream drops (the
	// restart scenario provokes exactly that) and docker revives it, so a
	// fresh listener may race the reboot.
	var conn *websocket.Conn
	dialDeadline := time.Now().Add(eventTimeout)
	for {
		var err error
		conn, _, err = websocket.DefaultDialer.Dial(u, nil)
		if err == nil {
			break
		}
		if time.Now().After(dialDeadline) {
			t.Fatalf("dial jetstream %s: %v", u, err)
		}
		t.Logf("dial jetstream: %v (retrying)", err)
		time.Sleep(2 * time.Second)
	}
	l := &jsListener{
		t:      t,
		conn:   conn,
		events: make(chan *jsEvent, 1024),
		closed: make(chan struct{}),
		done:   make(chan struct{}),
	}
	go l.readLoop()
	t.Cleanup(l.close)
	return l
}

// cursorNow returns a Jetstream cursor a couple of seconds in the past —
// capture it immediately before the action under test.
func cursorNow() int64 {
	return time.Now().Add(-2 * time.Second).UnixMicro()
}

func (l *jsListener) readLoop() {
	defer close(l.done)
	defer close(l.events)
	for {
		select {
		case <-l.closed:
			return
		default:
		}
		_ = l.conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
		_, raw, err := l.conn.ReadMessage()
		if err != nil {
			l.setReadErr(err)
			return
		}
		var ev jsEvent
		if err := json.Unmarshal(raw, &ev); err != nil {
			// Guarded log: never Logf after a deliberate close (close()
			// joins this goroutine, but belt-and-braces).
			select {
			case <-l.closed:
				return
			default:
				l.t.Logf("jetstream: undecodable event: %v (%s)", err, truncate(raw, 200))
			}
			continue
		}
		select {
		case l.events <- &ev:
		case <-l.closed:
			return
		}
	}
}

// close shuts the listener down and JOINS the read goroutine, so readLoop
// can never touch t after the test (and its cleanup) completed.
func (l *jsListener) close() {
	l.once.Do(func() {
		close(l.closed)
		_ = l.conn.Close()
		<-l.done
	})
}

// vetEvent enforces the suite-wide wire contract on EVERY consumed commit
// event, matched or skipped:
//
//   - only the known emitted collections may appear — anything else, votes
//     above all (PLAN.md locked decision 7: votes NEVER become records), is
//     an immediate failure, no matter which scenario's await/drain window it
//     lands in;
//   - every create/update record must validate against the vendored Coves
//     lexicons (deletes carry no record).
func (l *jsListener) vetEvent(ev *jsEvent) {
	l.t.Helper()
	if ev.Kind != kindCommit || ev.Commit == nil {
		return
	}
	if !expectedCollections[ev.Commit.Collection] {
		l.t.Fatalf("unexpected collection on firehose: %s — only community/actor profiles, posts, and comments may ever appear (votes never become records)", ev)
	}
	if op := ev.Commit.Operation; op == opCreate || op == opUpdate {
		validateLexicon(l.t, ev.Commit)
	}
}

// await returns the first commit event matching pred, failing the test
// after eventTimeout. Non-matching events are vetted (vetEvent), logged,
// and kept out of the way (each scenario matches on its own
// community/author to stay independent of concurrent traffic).
func (l *jsListener) await(desc string, pred func(*jsEvent) bool) *jsEvent {
	l.t.Helper()
	timer := time.NewTimer(eventTimeout)
	defer timer.Stop()
	for {
		select {
		case ev, ok := <-l.events:
			if !ok {
				l.t.Fatalf("await %s: jetstream events channel closed unexpectedly (read error: %v)", desc, l.readError())
				return nil
			}
			l.vetEvent(ev)
			if ev.Kind == kindCommit && ev.Commit != nil && pred(ev) {
				l.t.Logf("await %s: matched %s", desc, ev)
				return ev
			}
			l.t.Logf("await %s: skipping %s", desc, ev)
		case <-timer.C:
			l.t.Fatalf("await %s: no matching jetstream event within %s", desc, eventTimeout)
			return nil
		}
	}
}

// drain collects everything that arrives within d (for negative
// assertions: "nothing else showed up"). A dead reader would make every
// negative assertion pass vacuously, so an unexpectedly closed channel is
// fatal — silence must mean "connected and nothing arrived".
func (l *jsListener) drain(d time.Duration) []*jsEvent {
	l.t.Helper()
	timer := time.NewTimer(d)
	defer timer.Stop()
	var out []*jsEvent
	for {
		select {
		case ev, ok := <-l.events:
			if !ok {
				l.t.Fatalf("drain: jetstream events channel closed unexpectedly after %d events (read error: %v)", len(out), l.readError())
				return out
			}
			l.vetEvent(ev)
			out = append(out, ev)
		case <-timer.C:
			return out
		}
	}
}

// ── Lexicon conformance ────────────────────────────────────────────────────

// validateLexicon checks a firehose record against the vendored Coves
// lexicons with the same indigo validator (and flags) the materializer uses
// — but on the CONSUMER side of the wire: what Jetstream decoded is what
// the AppView would index.
func validateLexicon(t *testing.T, commit *jsCommit) {
	t.Helper()
	catalog, err := lexicons.Catalog()
	if err != nil {
		t.Fatalf("load lexicon catalog: %v", err)
	}
	var data any
	if err := json.Unmarshal(commit.Record, &data); err != nil {
		t.Fatalf("decode %s record: %v", commit.Collection, err)
	}
	rec, ok := data.(map[string]any)
	if !ok {
		t.Fatalf("%s record is not an object", commit.Collection)
	}
	recType, _ := rec["$type"].(string)
	if recType != commit.Collection {
		t.Fatalf("record $type %q != collection %q", recType, commit.Collection)
	}
	if err := lexicon.ValidateRecord(catalog, data, recType, lexicon.ValidateFlags(0)); err != nil {
		t.Errorf("%s record fails lexicon validation: %v\nrecord: %s",
			recType, err, truncate(commit.Record, 600))
	}
}

// fieldOf digs a string field out of a raw record, reporting ok=false for
// anything that doesn't match: empty/null records (delete events!),
// undecodable bytes, non-object intermediates, missing keys, non-string
// leaves. Total over every event shape, so it is the ONLY record accessor
// allowed inside await predicates, which see foreign and delete events.
func fieldOf(record json.RawMessage, path ...string) (string, bool) {
	if len(record) == 0 {
		return "", false
	}
	var cur any
	if err := json.Unmarshal(record, &cur); err != nil {
		return "", false
	}
	for _, key := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return "", false
		}
		if cur, ok = m[key]; !ok {
			return "", false
		}
	}
	s, ok := cur.(string)
	return s, ok
}

// recordField is the fatal variant of fieldOf for post-match assertions on
// an event that is already known to carry the record.
func recordField(t *testing.T, record json.RawMessage, path ...string) string {
	t.Helper()
	s, ok := fieldOf(record, path...)
	if !ok {
		t.Fatalf("record field %q missing or not a string in: %s",
			strings.Join(path, "."), truncate(record, 300))
	}
	return s
}

// ── Container control (scenario 7) ─────────────────────────────────────────

// composeFile locates docker-compose.e2e.yml relative to this source file.
func composeFile(t *testing.T) string {
	t.Helper()
	if v := os.Getenv("TIDEPOOL_E2E_COMPOSE"); v != "" {
		return v
	}
	_, self, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test source for compose file discovery")
	}
	return filepath.Join(filepath.Dir(self), "..", "..", "docker-compose.e2e.yml")
}

// restartTidepool restarts the bridge container and waits for /healthz —
// the scenario-7 crash/redeploy simulation.
func (h *harness) restartTidepool(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "compose", "-f", composeFile(t), "restart", "tidepool")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("restart tidepool: %v\n%s", err, out)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		if err := probeHTTP(client, tidepoolURL()+"/healthz"); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("tidepool did not come back healthy after restart")
		}
		time.Sleep(time.Second)
	}
}
