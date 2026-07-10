//go:build e2e

// Package e2e drives the docker-compose.e2e.yml stack end to end: a real
// Lemmy (debug build, plain-HTTP federation) federating with Tidepool, a
// real did:plc directory backing DID minting, a real BigSky relay crawling
// the bridge (DID resolution against the local PLC, per-commit signature
// verification), and a real Jetstream decoding the RELAY's firehose — every
// event the suite consumes has therefore survived relay validation.
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
	"mime/multipart"
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

	"github.com/bluesky-social/indigo/atproto/atdata"
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
func relayURL() string    { return envOr("RELAY_E2E_URL", "http://localhost:2480") }
func jetstreamURL() string {
	return envOr("JETSTREAM_E2E_URL", "ws://localhost:6028")
}
func adminToken() string     { return envOr("TIDEPOOL_E2E_ADMIN_TOKEN", "e2e-admin-token") }
func relayAdminKey() string  { return envOr("RELAY_E2E_ADMIN_KEY", "e2e-relay-admin-key") }
func bridgeHostname() string { return envOr("TIDEPOOL_E2E_HOSTNAME", "tidepool") }

// stackTimeout bounds the initial wait-for-healthy loop. Container startup
// (Lemmy migrations, PLC boot) can be slow on a cold machine; `make e2e`
// already waited for compose health, so this is usually instant.
const stackTimeout = 5 * time.Minute

// eventTimeout bounds one wait for a federation → bridge firehose → relay →
// Jetstream round trip. LEMMY_TEST_FAST_FEDERATION makes Lemmy deliveries
// near-instant, but the relay hop adds real work per event — first sight of
// a DID costs the relay a PLC resolution plus handle verification, and on
// Apple Silicon the (amd64-only) relay image runs emulated — so this budget
// is deliberately looser than the pre-relay 90s.
const eventTimeout = 120 * time.Second

// burstTimeout bounds scenario 8's whole 12-post burst arriving (a single
// shared budget: the collection loop is drain-based rather than one
// eventTimeout per await).
const burstTimeout = 3 * time.Minute

// crawlTimeout bounds waiting for the bridge's startup requestCrawl to land
// in the relay's PDS registry. The announcement retries inside the bridge
// (internal/sync/crawl.go: 24 attempts × 5s interval, 10s per-attempt cap)
// because the relay validates the hostname by calling back into the bridge's
// describeServer — the budget must cover the realistic retry window (attempts
// fail near-instantly in this stack: connection-refused or bigsky's fast 400)
// plus the relay's first subscribe. A stack where every attempt hangs to the
// 10s cap is broken, and failing at this deadline is then the right signal.
const crawlTimeout = 3 * time.Minute

// negativeWindow bounds the "and nothing else arrived" drain in the bounded
// NEGATIVE assertions (consent suppression, unsubscribe). Every such window
// is anchored by a positive control that already arrived — where the control
// shares the suppressed event's queue ordering key (same community) the
// suppressed event's fate was decided BEFORE the control was emitted, so
// this window is a belt on top of causality, not the proof itself.
const negativeWindow = 8 * time.Second

// profileStaleWait guarantees a bridged actor's stored profile has gone
// stale: the e2e compose sets PROFILE_REFRESH_TTL=2s (see
// docker-compose.e2e.yml — Lemmy 0.19.x never federates Update{Person} on
// bio edits, so the TTL re-fetch on the actor's next activity is the ONLY
// path by which the bridge discovers a bio marker change). The wait is
// TTL + slack over the gap between the profile-sync stamp (materialization
// time) and our observation of the materialized event on Jetstream.
const profileStaleWait = 5 * time.Second

// sweepTimeout bounds the suite-end full-firehose replay reaching its
// sentinel: the whole suite's history replays from local disk in seconds,
// plus one fresh subscribe round trip for the sentinel itself.
const sweepTimeout = 3 * time.Minute

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

// pendingSoftCap is the listener's early-warning threshold for the pending
// buffer (await's consumed-but-unmatched events): crossing it t.Logf's once
// as a leak signal, without failing — a busy shared stack can legitimately
// buffer plenty of foreign traffic.
const pendingSoftCap = 512

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

// harness bundles the service endpoints plus a logged-in Lemmy admin.
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
		{"relay /xrpc/_health", func() error {
			return probeHTTP(client, relayURL()+"/xrpc/_health")
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
	// password is kept so account-lifecycle scenarios can call endpoints
	// that re-authenticate (POST /api/v3/user/delete_account requires it).
	password string
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
	c.password = password
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

// likePostErr is likePost for goroutines: t.Fatalf must only be called from
// the test goroutine, so the vote-hammer's concurrent voters collect errors
// instead.
func (c *lemmyClient) likePostErr(postID, score int) error {
	return c.do(http.MethodPost, "/api/v3/post/like",
		map[string]any{"post_id": postID, "score": score}, nil)
}

// saveBio sets the user's bio (markdown) via save_user_settings. NOTE
// (verified against Lemmy 0.19.19 source): this does NOT federate an
// Update{Person} — SendActivityData has no Update-Person variant at that
// tag — so remote instances only see a bio change when they next re-fetch
// the actor (the e2e compose shortens PROFILE_REFRESH_TTL for exactly this).
func (c *lemmyClient) saveBio(t *testing.T, bio string) {
	t.Helper()
	if err := c.do(http.MethodPut, "/api/v3/user/save_user_settings",
		map[string]any{"bio": bio}, nil); err != nil {
		t.Fatalf("save bio: %v", err)
	}
}

// deleteAccount deletes the Lemmy account (POST in 0.19.x, password
// re-auth). delete_content=true purges the user's posts/comments on Lemmy;
// federation-wise both variants arrive at the bridge as ONE Delete{Person}
// (with a nonstandard removeData flag) sent to all known instances — there
// is no per-object Delete for the content.
func (c *lemmyClient) deleteAccount(t *testing.T) {
	t.Helper()
	if c.password == "" {
		t.Fatal("deleteAccount: client has no stored password (only registerUser clients can delete themselves)")
	}
	if err := c.do(http.MethodPost, "/api/v3/user/delete_account",
		map[string]any{"password": c.password, "delete_content": true}, nil); err != nil {
		t.Fatalf("delete account: %v", err)
	}
}

// editCommunity renames/re-describes a community (caller must be a mod —
// the harness admin created every suite community). Federates as
// Announce{Update{Group}} to the community's followers.
func (c *lemmyClient) editCommunity(t *testing.T, communityID int, title, description string) {
	t.Helper()
	if err := c.do(http.MethodPut, "/api/v3/community",
		map[string]any{"community_id": communityID, "title": title, "description": description}, nil); err != nil {
		t.Fatalf("edit community %d: %v", communityID, err)
	}
}

// lemmyInternalOrigin is Lemmy's origin as seen INSIDE the compose network —
// the form that must appear in post URLs so both Lemmy (opengraph metadata
// fetch) and the bridge (blob fetch) can resolve it. LOCAL-ONLY by
// construction.
func lemmyInternalOrigin() string { return envOr("LEMMY_E2E_INTERNAL_ORIGIN", "http://lemmy") }

// uploadImage pushes image bytes through Lemmy's authenticated pictrs proxy
// (POST /pictrs/image, multipart field "images[]" — Lemmy streams the body
// to pict-rs unchanged) and returns the compose-internal image URL in the
// exact shape Lemmy's own clients embed (…/pictrs/image/<alias>).
func (c *lemmyClient) uploadImage(t *testing.T, filename string, data []byte) string {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	part, err := mw.CreateFormFile("images[]", filename)
	if err != nil {
		t.Fatalf("upload image: build multipart: %v", err)
	}
	if _, err := part.Write(data); err != nil {
		t.Fatalf("upload image: write multipart: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("upload image: close multipart: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, lemmyURL()+"/pictrs/image", &buf)
	if err != nil {
		t.Fatalf("upload image: build request: %v", err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+c.jwt)
	resp, err := c.h.http.Do(req)
	if err != nil {
		t.Fatalf("upload image: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("upload image: read body: %v", err)
	}
	if resp.StatusCode/100 != 2 {
		t.Fatalf("upload image: status %d: %s", resp.StatusCode, truncate(raw, 300))
	}
	var out struct {
		Msg   string `json:"msg"`
		Files []struct {
			File string `json:"file"`
		} `json:"files"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("upload image: decode: %v (%s)", err, truncate(raw, 200))
	}
	if out.Msg != "ok" || len(out.Files) == 0 || out.Files[0].File == "" {
		t.Fatalf("upload image: pictrs refused: %s", truncate(raw, 300))
	}
	return lemmyInternalOrigin() + "/pictrs/image/" + out.Files[0].File
}

// createImagePost creates a post whose url is an (already uploaded) image,
// optionally NSFW-flagged and alt-texted — the shape the materializer turns
// into an embed.images (+ nsfw self-label).
func (c *lemmyClient) createImagePost(t *testing.T, communityID int, title, imageURL, altText string, nsfw bool) lemmyPost {
	t.Helper()
	req := map[string]any{
		"name":         title,
		"community_id": communityID,
		"url":          imageURL,
		"nsfw":         nsfw,
	}
	if altText != "" {
		req["alt_text"] = altText
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
		t.Fatalf("create image post %q: %v", title, err)
	}
	return lemmyPost{ID: out.PostView.Post.ID, APID: out.PostView.Post.APID}
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

// unsubscribeCommunity drives DELETE /admin/communities: the bridge sends a
// best-effort Undo{Follow} to the group and records follow_state=none, after
// which Announces from that community are dropped (and Lemmy, having lost
// its follower, stops announcing at all).
func (h *harness) unsubscribeCommunity(t *testing.T, community string) adminCommunity {
	t.Helper()
	var resp adminCommunity
	if err := h.adminDo(http.MethodDelete, "/admin/communities",
		map[string]any{"community": community}, &resp); err != nil {
		t.Fatalf("unsubscribe %s: %v", community, err)
	}
	if resp.FollowState != "none" {
		t.Fatalf("unsubscribe %s: follow_state = %q, want %q", community, resp.FollowState, "none")
	}
	return resp
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

// ── Bridge sync-surface client ─────────────────────────────────────────────
//
// The bridge's OWN com.atproto.sync.* + identity endpoints (tidepool :8092).
// Repo lifecycle state (active/deactivated) must be asserted here rather
// than through the relay: bigsky serves no getRepoStatus, FILTERS
// tombstoned repos out of its listRepos, and learns account state upstream
// only from #account frames — which the bridge does not emit until task 11
// (FOLLOWUPS.md "Relay pipeline").

// xrpcResult is the decoded outcome of one bridge XRPC GET: the HTTP status
// plus either the success body or the standard XRPC error shape.
type xrpcResult struct {
	status  int
	body    []byte
	errCode string // "error" field of the XRPC error body, "" on 2xx
}

// bridgeXRPC issues one GET against the bridge and decodes the XRPC error
// envelope on non-2xx. Error-returning (transport only) so poll loops can
// wait out restarts; non-2xx statuses are DATA here, not errors — lifecycle
// scenarios assert on exact refusal codes.
func (h *harness) bridgeXRPC(path string, query url.Values) (xrpcResult, error) {
	u := tidepoolURL() + path
	if enc := query.Encode(); enc != "" {
		u += "?" + enc
	}
	resp, err := h.http.Get(u)
	if err != nil {
		return xrpcResult{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return xrpcResult{}, err
	}
	res := xrpcResult{status: resp.StatusCode, body: raw}
	if resp.StatusCode/100 != 2 {
		var xe struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(raw, &xe) // non-JSON error bodies leave errCode ""
		res.errCode = xe.Error
	}
	return res, nil
}

// repoStatus is the bridge's getRepoStatus / listRepos entry shape.
type repoStatus struct {
	Did    string `json:"did"`
	Active bool   `json:"active"`
	Status string `json:"status"`
	Rev    string `json:"rev"`
}

// bridgeGetRepoStatus reads com.atproto.sync.getRepoStatus from the bridge.
func (h *harness) bridgeGetRepoStatus(did string) (repoStatus, xrpcResult, error) {
	res, err := h.bridgeXRPC("/xrpc/com.atproto.sync.getRepoStatus", url.Values{"did": {did}})
	if err != nil || res.status != http.StatusOK {
		return repoStatus{}, res, err
	}
	var out repoStatus
	if err := json.Unmarshal(res.body, &out); err != nil {
		return repoStatus{}, res, fmt.Errorf("getRepoStatus(%s): decode: %w (%s)", did, err, truncate(res.body, 200))
	}
	return out, res, nil
}

// bridgeListRepos walks the bridge's own listRepos (which, unlike the
// relay's, DOES include deactivated repos with active:false).
func (h *harness) bridgeListRepos(t *testing.T) map[string]repoStatus {
	t.Helper()
	repos := map[string]repoStatus{}
	cursor := ""
	for page := 0; ; page++ {
		if page >= relayListReposPageCap {
			t.Fatalf("bridge listRepos: still paginating after %d pages — runaway paging", relayListReposPageCap)
		}
		q := url.Values{"limit": {"500"}}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		res, err := h.bridgeXRPC("/xrpc/com.atproto.sync.listRepos", q)
		if err != nil {
			t.Fatalf("bridge listRepos: %v", err)
		}
		if res.status != http.StatusOK {
			t.Fatalf("bridge listRepos: status %d: %s", res.status, truncate(res.body, 300))
		}
		var out struct {
			Cursor string       `json:"cursor"`
			Repos  []repoStatus `json:"repos"`
		}
		if err := json.Unmarshal(res.body, &out); err != nil {
			t.Fatalf("bridge listRepos: decode: %v", err)
		}
		for _, r := range out.Repos {
			repos[r.Did] = r
		}
		if out.Cursor == "" || out.Cursor == cursor {
			return repos
		}
		cursor = out.Cursor
	}
}

// bridgeResolveHandle resolves a handle via the bridge's
// com.atproto.identity.resolveHandle. Callers assert on the result: 200 +
// DID for live actors, 400 HandleNotFound for unknown AND tombstoned ones
// (a tombstoned actor's identity is frozen — its handle stops resolving).
func (h *harness) bridgeResolveHandle(t *testing.T, handle string) (did string, res xrpcResult) {
	t.Helper()
	res, err := h.bridgeXRPC("/xrpc/com.atproto.identity.resolveHandle", url.Values{"handle": {handle}})
	if err != nil {
		t.Fatalf("resolveHandle(%s): %v", handle, err)
	}
	if res.status == http.StatusOK {
		var out struct {
			Did string `json:"did"`
		}
		if err := json.Unmarshal(res.body, &out); err != nil {
			t.Fatalf("resolveHandle(%s): decode: %v", handle, err)
		}
		did = out.Did
	}
	return did, res
}

// bridgedHandle predicts the bridge handle (task-03 shape:
// name.instance.BRIDGE_HOSTNAME) for a uniqueName-generated Lemmy username
// ONLY — [a-z0-9_], ≤20 chars. Its sanitizer is a simplification of
// production normalizeLabel (which additionally collapses dash runs, trims
// edge dashes, and truncates to 63 chars); the two agree on uniqueName's
// output but NOT on arbitrary usernames — do not feed it any other input.
// The suite's uniqueName usernames also make collision suffixes (-2, -3…)
// impossible within a run.
func bridgedHandle(username string) string {
	var b strings.Builder
	dash := false
	for _, r := range strings.ToLower(username) {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-':
			b.WriteRune(r)
			dash = false
		default:
			if !dash {
				b.WriteByte('-')
				dash = true
			}
		}
	}
	return b.String() + ".lemmy." + bridgeHostname()
}

// bridgeGetBlob fetches a stored blob from the bridge's
// com.atproto.sync.getBlob (the AppView's media path for bridged images),
// returning the bytes and the served Content-Type.
func (h *harness) bridgeGetBlob(t *testing.T, did, cid string) (data []byte, contentType string) {
	t.Helper()
	q := url.Values{"did": {did}, "cid": {cid}}
	resp, err := h.http.Get(tidepoolURL() + "/xrpc/com.atproto.sync.getBlob?" + q.Encode())
	if err != nil {
		t.Fatalf("getBlob(%s, %s): %v", did, cid, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("getBlob(%s, %s): read body: %v", did, cid, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getBlob(%s, %s): status %d: %s", did, cid, resp.StatusCode, truncate(raw, 200))
	}
	return raw, resp.Header.Get("Content-Type")
}

// ── Relay (BigSky) clients ─────────────────────────────────────────────────

// relayPDS is the slice of bigsky's /admin/pds/list response the suite
// asserts on (an enriched gorm models.PDS; Go field names, no json tags).
type relayPDS struct {
	Host                string `json:"Host"`
	Registered          bool   `json:"Registered"`
	HasActiveConnection bool   `json:"HasActiveConnection"`
	RepoCount           int64  `json:"RepoCount"`
}

// relayPDSList reads the relay's crawled-PDS registry via its admin API.
// Error-returning (not Fatalf) because its only callers are poll loops: one
// transient admin-API hiccup must not kill a minutes-long wait — the loop
// logs the error and Fatalf's at its own deadline.
func (h *harness) relayPDSList() ([]relayPDS, error) {
	req, err := http.NewRequest(http.MethodGet, relayURL()+"/admin/pds/list", nil)
	if err != nil {
		return nil, fmt.Errorf("relay pds/list: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+relayAdminKey())
	resp, err := h.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("relay pds/list: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("relay pds/list: read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("relay pds/list: status %d: %s", resp.StatusCode, truncate(raw, 300))
	}
	var out []relayPDS
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("relay pds/list: decode: %w (%s)", err, truncate(raw, 300))
	}
	return out, nil
}

// relayListReposPageCap bounds relayListRepos pagination: the e2e relay
// holds a few dozen repos (≪ one 500-repo page), so hitting the cap — or a
// cursor that stops advancing — means bigsky's paging is broken, and the
// walk must error out rather than spin until the 20m test timeout.
const relayListReposPageCap = 100

// relayListRepos walks the relay's public com.atproto.sync.listRepos and
// returns did → head. Tombstoned/taken-down repos are filtered out by
// bigsky itself, so absence after a takedown is the observable signal.
// Error-returning for the same poll-loop reason as relayPDSList.
func (h *harness) relayListRepos() (map[string]string, error) {
	repos := map[string]string{}
	cursor := ""
	for page := 0; ; page++ {
		if page >= relayListReposPageCap {
			return nil, fmt.Errorf("relay listRepos: still paginating after %d pages (cursor %q) — runaway paging", relayListReposPageCap, cursor)
		}
		u := relayURL() + "/xrpc/com.atproto.sync.listRepos?limit=500"
		if cursor != "" {
			u += "&cursor=" + url.QueryEscape(cursor)
		}
		resp, err := h.http.Get(u)
		if err != nil {
			return nil, fmt.Errorf("relay listRepos: %w", err)
		}
		raw, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("relay listRepos: read body: %w", err)
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("relay listRepos: status %d: %s", resp.StatusCode, truncate(raw, 300))
		}
		var out struct {
			Cursor string `json:"cursor"`
			Repos  []struct {
				Did  string `json:"did"`
				Head string `json:"head"`
			} `json:"repos"`
		}
		if err := json.Unmarshal(raw, &out); err != nil {
			return nil, fmt.Errorf("relay listRepos: decode: %w", err)
		}
		for _, r := range out.Repos {
			repos[r.Did] = r.Head
		}
		if out.Cursor == "" {
			return repos, nil
		}
		if out.Cursor == cursor {
			return nil, fmt.Errorf("relay listRepos: cursor %q did not advance between pages", cursor)
		}
		cursor = out.Cursor
	}
}

// relayGetLatestCommit reads the relay's view of a repo head. Returns an
// error (rather than failing) so poll loops can wait out the relay's
// asynchronous indexing of a commit it just received.
func (h *harness) relayGetLatestCommit(did string) (cid, rev string, err error) {
	resp, err := h.http.Get(relayURL() + "/xrpc/com.atproto.sync.getLatestCommit?did=" + url.QueryEscape(did))
	if err != nil {
		return "", "", err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("relay getLatestCommit(%s): status %d: %s", did, resp.StatusCode, truncate(raw, 200))
	}
	var out struct {
		CID string `json:"cid"`
		Rev string `json:"rev"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", "", fmt.Errorf("relay getLatestCommit(%s): decode: %w", did, err)
	}
	return out.CID, out.Rev, nil
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

	// pending holds events consumed by an await that did not match its
	// predicate, for rescanning by LATER awaits. Load-bearing since the
	// relay entered the pipeline: bigsky indexes its inbound firehose with a
	// parallel scheduler keyed by repo DID (indigo events/schedulers/
	// parallel, 100 workers), so per-repo event order is preserved but
	// CROSS-repo order is not — an author's actor.profile (author repo) and
	// their post (community repo) may legally swap on the relay's output.
	// Sequential awaits would otherwise silently discard the reordered
	// event and burn a full eventTimeout. Only touched from the test
	// goroutine (await/drain), never from readLoop.
	pending []*jsEvent

	// lastRev tracks the newest commit rev THIS listener has consumed per
	// repo DID (revs are TIDs: strictly increasing per repo). The pending
	// buffer made the suite order-tolerant, but PLAN.md locked decision 3's
	// per-repo discipline — and the FOLLOWUPS "Relay pipeline" carve-out —
	// lean on bigsky preserving PER-repo order even while shuffling repos
	// against each other; vetEvent asserts that property on every consumed
	// event so a relay/bridge ordering regression cannot hide behind the
	// listener's own tolerance. Per-listener (like pending): overlapping
	// listeners each see a per-repo-ordered stream of their own. Only
	// touched from the test goroutine.
	lastRev map[string]string

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

// newListener subscribes from cursorMicros. A zero or negative cursor OMITS
// the cursor param entirely, which Jetstream treats as live-tail (no replay)
// — so a subscriber that wants to replay all retained history must pass
// cursor 1 (the sweep does; its 1 is load-bearing, not a stylistic 0).
// Collections filter server-side via wantedCollections; none means all
// collections (used for negative assertions like "votes never hit the
// firehose").
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
		t:       t,
		conn:    conn,
		events:  make(chan *jsEvent, 1024),
		closed:  make(chan struct{}),
		done:    make(chan struct{}),
		lastRev: map[string]string{},
	}
	go l.readLoop()
	t.Cleanup(l.close)
	return l
}

// cursorNow returns a Jetstream cursor a couple of seconds in the past —
// capture it immediately before the action under test.
//
// KNOWN SEMANTICS of the pinned Jetstream (measured live, task 10): a
// cursor at or before its newest stored event replays precisely from that
// point (inclusive, µs-exact); a cursor beyond server-now live-tails; but a
// cursor BETWEEN the newest stored event and now — which "now minus 2s" IS
// whenever the stream has been quiet for a couple of seconds, i.e. usually —
// replays the ENTIRE retained store. Listeners built on cursorNow therefore
// often receive full-history replays. That is safe for every await (they
// match on per-scenario titles/DIDs/rkeys and buffer the rest) but it means
// a negative assertion must NEVER be scoped by "this listener only sees new
// traffic" — scope negatives by content plus a STREAM-derived boundary (an
// observed event's TimeUs), as the unsubscribe scenario does.
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
//     lexicons (deletes carry no record);
//   - commit revs must be strictly increasing per repo DID (see lastRev):
//     the relay guarantees per-repo order even though cross-repo order is
//     lost, and this is the one place every consumed event passes through.
func (l *jsListener) vetEvent(ev *jsEvent) {
	l.t.Helper()
	if ev.Kind != kindCommit || ev.Commit == nil {
		return
	}
	if !expectedCollections[ev.Commit.Collection] {
		l.t.Fatalf("unexpected collection on firehose: %s — only community/actor profiles, posts, and comments may ever appear (votes never become records)", ev)
	}
	if prev, ok := l.lastRev[ev.Did]; ok && ev.Commit.Rev <= prev {
		l.t.Fatalf("per-repo rev order violated on firehose: %s has rev %q after rev %q — bigsky preserves per-repo commit order, so this is a relay/bridge ordering bug",
			ev, ev.Commit.Rev, prev)
	}
	l.lastRev[ev.Did] = ev.Commit.Rev
	if op := ev.Commit.Operation; op == opCreate || op == opUpdate {
		validateLexicon(l.t, ev.Commit)
	}
}

// await returns the first commit event matching pred — scanning events an
// earlier await consumed-but-buffered FIRST (see pending: the relay does
// not preserve cross-repo ordering), then the live stream — failing the
// test after eventTimeout. Non-matching events are vetted (vetEvent),
// logged, and buffered for later awaits (each scenario matches on its own
// community/author to stay independent of concurrent traffic). A matched
// buffered event is removed, so repeat awaits with the same predicate
// consume distinct events.
//
// Because buffered events are re-tested by later awaits' predicates,
// predicates must be PURE — accounting sweeps belong in drain loops (which
// return every consumed event exactly once), not in predicate side effects.
func (l *jsListener) await(desc string, pred func(*jsEvent) bool) *jsEvent {
	l.t.Helper()
	for i, ev := range l.pending {
		if pred(ev) {
			copy(l.pending[i:], l.pending[i+1:])
			l.pending[len(l.pending)-1] = nil // let the shifted-out *jsEvent GC
			l.pending = l.pending[:len(l.pending)-1]
			l.t.Logf("await %s: matched buffered %s", desc, ev)
			return ev
		}
	}
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
			if ev.Kind != kindCommit || ev.Commit == nil {
				l.t.Logf("await %s: skipping %s", desc, ev)
				continue
			}
			if pred(ev) {
				l.t.Logf("await %s: matched %s", desc, ev)
				return ev
			}
			l.pending = append(l.pending, ev)
			if len(l.pending) == pendingSoftCap {
				l.t.Logf("await %s: pending buffer reached %d unmatched events — a scenario may be leaking unmatched traffic (soft warning, not fatal)", desc, pendingSoftCap)
			}
			l.t.Logf("await %s: buffering %s", desc, ev)
		case <-timer.C:
			l.t.Fatalf("await %s: no matching jetstream event within %s", desc, eventTimeout)
			return nil
		}
	}
}

// drain returns everything the listener has consumed-but-not-matched so
// far (the pending buffer, cleared here so repeated drains cannot return
// an event twice) plus everything that arrives LIVE within d — for
// negative assertions ("nothing else showed up") and order-agnostic
// accounting sweeps. Including pending is load-bearing, not a convenience:
// a drain running AFTER an await on the same listener would otherwise
// silently miss events that await consumed and buffered, turning negative
// assertions vacuous. It cannot double-count either — pending events were
// vetted at consumption but never matched/counted by any await. A dead
// reader would make every negative assertion pass vacuously, so an
// unexpectedly closed channel is fatal — silence must mean "connected and
// nothing arrived".
func (l *jsListener) drain(d time.Duration) []*jsEvent {
	l.t.Helper()
	timer := time.NewTimer(d)
	defer timer.Stop()
	out := l.pending
	l.pending = nil
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
//
// The record is decoded with atdata.UnmarshalJSON, NOT plain encoding/json:
// indigo's validator demands the atproto data model — a blob must arrive as
// a typed atdata.Blob, which a raw map[string]any can never satisfy (its
// SchemaBlob.Validate fails with "expected a blob"). This mirrors the
// materializer's write-side validation (internal/materialize/
// materializer.go, lexiconData) and is what ANY JSON consumer of Jetstream
// — the Coves AppView included — must do before validating records that
// carry blobs. Discovered the hard way by the image-post scenario, the
// first record with a blob ever to cross the suite's validation.
func validateLexicon(t *testing.T, commit *jsCommit) {
	t.Helper()
	catalog, err := lexicons.Catalog()
	if err != nil {
		t.Fatalf("load lexicon catalog: %v", err)
	}
	rec, err := atdata.UnmarshalJSON(commit.Record)
	if err != nil {
		t.Fatalf("decode %s record as atproto data: %v (%s)",
			commit.Collection, err, truncate(commit.Record, 300))
	}
	recType, _ := rec["$type"].(string)
	if recType != commit.Collection {
		t.Fatalf("record $type %q != collection %q", recType, commit.Collection)
	}
	if err := lexicon.ValidateRecord(catalog, rec, recType, lexicon.ValidateFlags(0)); err != nil {
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
