package ingest

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// nodeInfoPath is the path the host prober requests.
const nodeInfoPath = "/.well-known/nodeinfo"

// unreachableHostsTransport is rewriteTransport, except that every request to
// a host in failures (other than its nodeinfo probe) fails at the transport
// with that host's error, the way a dial to a dead server fails. It counts
// those failed attempts per host.
type unreachableHostsTransport struct {
	target   string
	failures map[string]error

	mu       sync.Mutex
	attempts map[string]int
}

func (rt *unreachableHostsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	host := req.URL.Hostname()
	if failure, ok := rt.failures[host]; ok && req.URL.Path != nodeInfoPath {
		rt.mu.Lock()
		rt.attempts[host]++
		rt.mu.Unlock()
		return nil, failure
	}
	return rewriteTransport{target: rt.target}.RoundTrip(req)
}

func (rt *unreachableHostsTransport) attemptsTo(host string) int {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.attempts[host]
}

// probeAnswer is how a host answers its nodeinfo probe.
type probeAnswer int

const (
	// probeJSON answers 200 with a JSON NodeInfo discovery document.
	probeJSON probeAnswer = iota + 1
	// probeHTML answers 200 with a parked HTML page.
	probeHTML
)

// nodeInfoProbes serves /.well-known/nodeinfo on the fixture server per
// requesting host and counts the requests per host. A host with no answer
// configured gets 404 (the materializer's own software discovery lands here
// too, and tolerates it).
type nodeInfoProbes struct {
	answers map[string]probeAnswer

	mu       sync.Mutex
	requests map[string]int
}

func serveNodeInfoProbes(h *harness, answers map[string]probeAnswer) *nodeInfoProbes {
	probes := &nodeInfoProbes{answers: answers, requests: map[string]int{}}
	h.mux.HandleFunc("GET "+nodeInfoPath, func(w http.ResponseWriter, r *http.Request) {
		host := strings.ToLower(r.Host)
		probes.mu.Lock()
		probes.requests[host]++
		probes.mu.Unlock()
		switch probes.answers[host] {
		case probeJSON:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"links":[{"rel":"http://nodeinfo.diaspora.software/ns/schema/2.0","href":"https://` + host + `/nodeinfo/2.0"}]}`))
		case probeHTML:
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte("<html><body>This domain is for sale</body></html>"))
		default:
			http.NotFound(w, r)
		}
	})
	return probes
}

func (p *nodeInfoProbes) requestsTo(host string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.requests[host]
}

// countingHostGoneStreaks is the real store, counting the RecordHostGone
// calls made through it (the walk's; seeding goes to the store directly).
type countingHostGoneStreaks struct {
	store.HostGoneStreaks

	mu       sync.Mutex
	recorded []string
}

func (s *countingHostGoneStreaks) RecordHostGone(ctx context.Context, host string, at time.Time) (time.Time, error) {
	s.mu.Lock()
	s.recorded = append(s.recorded, host)
	s.mu.Unlock()
	return s.HostGoneStreaks.RecordHostGone(ctx, host, at)
}

func (s *countingHostGoneStreaks) recordedHosts() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.recorded...)
}

// unreachableRig is a Backfill wired the way production wires it, over one
// AP client whose transport fails the configured hosts: the walk's fetcher,
// the prober and (behind NewHostHealthFetcher) the materializer's fetcher are
// that same client, so the walk's shared path is the one under test.
type unreachableRig struct {
	h         *harness
	transport *unreachableHostsTransport
	probes    *nodeInfoProbes
	streaks   *countingHostGoneStreaks
	backfill  *Backfill
}

func newUnreachableRig(t *testing.T, h *harness, failures map[string]error, answers map[string]probeAnswer) *unreachableRig {
	t.Helper()
	return newUnreachableRigWith(t, h, failures, answers, nil)
}

// newUnreachableRigWith is newUnreachableRig with adjust, when set, applied
// to the Backfill options last (to swap one dependency for a decorator).
func newUnreachableRigWith(t *testing.T, h *harness, failures map[string]error, answers map[string]probeAnswer, adjust func(*BackfillOptions)) *unreachableRig {
	t.Helper()
	transport := &unreachableHostsTransport{
		target:   h.fixtures.Listener.Addr().String(),
		failures: failures,
		attempts: map[string]int{},
	}
	client := ap.NewClient(ap.ClientOptions{
		HTTPClient:            &http.Client{Transport: transport},
		Signer:                h.service.Signer(),
		AllowPrivateAddresses: true,
		PerHostRPS:            100000,
		PerHostBurst:          100000,
		MaxAttempts:           1,
	})
	logger := slog.New(slog.NewTextHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	materializerOptions := h.materializerOptions
	materializerOptions.Fetcher = NewHostHealthFetcher(client, h.materializerOptions.Instances, logger)
	materializer, err := materialize.New(materializerOptions)
	require.NoError(t, err)

	streaks := &countingHostGoneStreaks{HostGoneStreaks: h.materializerOptions.Instances}
	opts := h.backfillOptions(materializer, 100)
	opts.Fetcher = client
	opts.Prober = client
	opts.Instances = streaks
	opts.Logger = logger
	if adjust != nil {
		adjust(&opts)
	}
	b, err := NewBackfill(opts)
	require.NoError(t, err)
	b.now = func() time.Time { return backfillNow }
	return &unreachableRig{
		h:         h,
		transport: transport,
		probes:    serveNodeInfoProbes(h, answers),
		streaks:   streaks,
		backfill:  b,
	}
}

// run walks the technology community once, forced.
func (r *unreachableRig) run(t *testing.T) error {
	t.Helper()
	community, err := r.h.communities.GetByAPGroupID(context.Background(), groupID)
	require.NoError(t, err)
	return r.backfill.Run(context.Background(), community, true)
}

// unreachablePageIRI is the i-th cross-authority outbox post on x.example.
func unreachablePageIRI(i int) string { return fmt.Sprintf("https://x.example/post/%d", i) }

const (
	unreachableHost = "x.example"
	// ancestorItemID is a same-authority comment whose parent is on
	// x.example: its body is trusted as embedded, and only the materializer's
	// ancestor fetch reaches x.example.
	ancestorItemID     = "https://lemmy.world/comment/9001"
	ancestorItemAuthor = "https://lemmy.world/u/replier"
	ancestorItemParent = "https://x.example/post/4"
)

// serveUnreachableOutbox serves the technology outbox with pages
// cross-authority posts on x.example (embedded, so the walk re-fetches each
// from its origin) and, with ancestorItem, a final same-authority comment
// whose parent lives on x.example.
func serveUnreachableOutbox(h *harness, pages int, ancestorItem bool) {
	items := []any{}
	for i := 1; i <= pages; i++ {
		items = append(items, map[string]any{
			"id":           unreachablePageIRI(i),
			"type":         "Page",
			"attributedTo": "https://x.example/u/gone",
			"audience":     groupID,
			"to":           []any{groupID, ap.PublicAudience},
			"name":         fmt.Sprintf("post %d from an unreachable host", i),
			"content":      "<p>unreachable</p>",
			"published":    "2026-11-07T09:00:00.000000Z",
		})
	}
	if ancestorItem {
		h.serveObject("/u/replier", person(ancestorItemAuthor, "replier", nil))
		items = append(items, note(ancestorItemID, ancestorItemAuthor, ancestorItemParent,
			"a reply to a post on an unreachable host", "2026-11-07T10:00:00.000000Z"))
	}
	h.serveObject("/c/technology/outbox", map[string]any{
		"type":         "OrderedCollection",
		"id":           groupID + "/outbox",
		"totalItems":   len(items),
		"orderedItems": items,
	})
}

// connectionRefused is the transport error of a dial the server refuses.
func connectionRefused() error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}
}

// noSuchHost is the transport error of a lookup of host that answers NXDOMAIN.
func noSuchHost(host string) error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}}
}

// hostGoneSince reads host's host_gone_since (invalid when NULL or no row).
func hostGoneSince(t *testing.T, h *harness, host string) sql.NullTime {
	t.Helper()
	var since sql.NullTime
	err := h.db.QueryRowContext(context.Background(),
		`SELECT host_gone_since FROM instances WHERE host = $1`, host).Scan(&since)
	if stderrors.Is(err, sql.ErrNoRows) {
		return sql.NullTime{}
	}
	require.NoError(t, err)
	return since
}

// logLevels returns the level of every captured log line with message msg
// that also carries attr (a key=value pair), in order.
func logLevels(logs, msg, attr string) []string {
	var levels []string
	for _, line := range strings.Split(logs, "\n") {
		if !strings.Contains(line, `msg="`+msg+`"`) || !strings.Contains(line, attr) {
			continue
		}
		for _, field := range strings.Fields(line) {
			if level, ok := strings.CutPrefix(field, "level="); ok {
				levels = append(levels, level)
			}
		}
	}
	return levels
}

// requireNotMaterialized asserts none of ids was bridged.
func requireNotMaterialized(t *testing.T, h *harness, ids ...string) {
	t.Helper()
	for _, id := range ids {
		_, err := h.objects.GetByAPID(context.Background(), id)
		assert.True(t, errors.IsNotFound(err), "%s must not be materialized", id)
	}
}

// firstDeferredRetry is when an item deferred by a walk at backfillNow is
// first due: one hour after its first skip.
var firstDeferredRetry = time.Date(2026, 11, 8, 13, 0, 0, 0, time.UTC)

// walkDeferredRows is the backfill_deferred_items rows a walk at backfillNow
// writes for items, each (object IRI, object type) under the technology
// community and host, ordered as deferredRows reads them.
func walkDeferredRows(h *harness, host string, items ...[2]string) []store.DeferredItem {
	communityDID := h.communityDID(groupID)
	rows := []store.DeferredItem{}
	for _, item := range items {
		rows = append(rows, store.DeferredItem{
			ObjectIRI:     item[0],
			CommunityAPID: groupID,
			CommunityDID:  communityDID,
			Host:          host,
			ObjectType:    item[1],
			Attempts:      0,
			NextRetryAt:   firstDeferredRetry,
			CreatedAt:     backfillNow,
		})
	}
	return rows
}

// TestBackfillUnreachableHostDefersOnFirstWalk is the acceptance check: a
// walk over items on an unreachable host (three posts on it and a comment
// whose parent is on it) defers every one of them at once, whatever the
// host's streak and its reason, so the walk completes and stamps
// last_backfill_at. Each item gets one row, first due one hour after the
// walk. The host costs the walk one connection attempt and one streak record,
// and is never probed: a probe, were one sent, would answer JSON.
func TestBackfillUnreachableHostDefersOnFirstWalk(t *testing.T) {
	tests := []struct {
		name       string
		failure    error
		deadStreak bool
		// wantSince is x.example's host_gone_since after the walk.
		wantSince time.Time
	}{
		{
			name:      "a refusing host with no streak",
			failure:   connectionRefused(),
			wantSince: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC),
		},
		{
			name:       "a refusing host gone past DeadHostAfter",
			failure:    connectionRefused(),
			deadStreak: true,
			wantSince:  time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC),
		},
		{
			name:      "an NXDOMAIN host with no streak",
			failure:   noSuchHost(unreachableHost),
			wantSince: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.subscribeTechnology()
			serveUnreachableOutbox(h, 3, true)
			if tt.deadStreak {
				seedDeadHostStreak(t, h, unreachableHost)
			}
			rig := newUnreachableRig(t, h,
				map[string]error{unreachableHost: tt.failure},
				map[string]probeAnswer{unreachableHost: probeJSON})

			assert.NoError(t, rig.run(t), "every item on an unreachable host is deferred, not a failure")

			assert.NotNil(t, h.lastBackfillAt(groupID), "a walk whose only failures are deferred stamps last_backfill_at")
			assert.Equal(t, walkDeferredRows(h, unreachableHost,
				[2]string{ancestorItemID, "Note"},
				[2]string{"https://x.example/post/1", "Page"},
				[2]string{"https://x.example/post/2", "Page"},
				[2]string{"https://x.example/post/3", "Page"},
			), deferredRows(t, h), "one deferred row per item, first due 1h after the walk")
			requireNotMaterialized(t, h, unreachablePageIRI(1), unreachablePageIRI(2), unreachablePageIRI(3), ancestorItemID)
			assert.Equal(t, []string{"WARN"}, logLevels(h.logs.String(), "backfill complete", "host_gone=4"),
				"the walk completes with all four items counted host_gone")
			assert.Equal(t, 0, rig.probes.requestsTo(unreachableHost), "the walk never probes x.example before deferring")
			assert.Equal(t, 1, rig.transport.attemptsTo(unreachableHost), "x.example is dialed once per walk")
			assert.Equal(t, []string{unreachableHost}, rig.streaks.recordedHosts(), "the walk records x.example's failure once")
			since := hostGoneSince(t, h, unreachableHost)
			require.True(t, since.Valid, "x.example has a streak after the walk")
			assert.True(t, tt.wantSince.Equal(since.Time),
				"x.example's host_gone_since = %v, want %v", since.Time, tt.wantSince)
		})
	}
}

// TestBackfillObjectNotServed: a cross-authority item whose origin answers
// 200 text/html with no redirect, on a host whose nodeinfo answers JSON, is a
// host that is up but does not serve the object: a skip (reason
// object_not_served), not a host-gone failure, with one probe per walk. A
// probe that does not answer JSON leaves it a failure, and a same-host
// redirect to another HTML path is a failure with no probe.
func TestBackfillObjectNotServed(t *testing.T) {
	const servingHost = "y.example"
	html := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html><body>the instance frontend</body></html>"))
	}
	tests := []struct {
		name string
		// paths are the items' paths on y.example, each answered by route.
		paths      []string
		route      func(h *harness)
		probe      probeAnswer
		wantSkip   bool
		wantProbes int
	}{
		{
			name:  "two HTML objects on a host whose probe answers JSON are skipped with one probe",
			paths: []string{"/post/html1", "/post/html2"},
			route: func(h *harness) {
				h.mux.HandleFunc("GET /post/html1", html)
				h.mux.HandleFunc("GET /post/html2", html)
			},
			probe:      probeJSON,
			wantSkip:   true,
			wantProbes: 1,
		},
		{
			name:  "an HTML object on a host whose probe answers HTML is a failure",
			paths: []string{"/post/html1"},
			route: func(h *harness) {
				h.mux.HandleFunc("GET /post/html1", html)
			},
			probe:      probeHTML,
			wantSkip:   false,
			wantProbes: 1,
		},
		{
			name:  "a same-host redirect to another HTML path is a failure with no probe",
			paths: []string{"/post/moved"},
			route: func(h *harness) {
				h.mux.HandleFunc("GET /post/moved", func(w http.ResponseWriter, r *http.Request) {
					http.Redirect(w, r, "/post/other", http.StatusFound)
				})
				h.mux.HandleFunc("GET /post/other", html)
			},
			probe:      probeJSON,
			wantSkip:   false,
			wantProbes: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.subscribeTechnology()
			tt.route(h)
			items := []any{}
			for _, path := range tt.paths {
				items = append(items, map[string]any{"id": "https://" + servingHost + path, "type": "Page"})
			}
			h.serveObject("/c/technology/outbox", map[string]any{
				"type":         "OrderedCollection",
				"id":           groupID + "/outbox",
				"totalItems":   len(items),
				"orderedItems": items,
			})
			rig := newUnreachableRig(t, h, nil, map[string]probeAnswer{servingHost: tt.probe})

			runErr := rig.run(t)

			assert.Equal(t, tt.wantProbes, rig.probes.requestsTo(servingHost), "nodeinfo probes of y.example")
			if tt.wantSkip {
				require.NoError(t, runErr, "an object the live host does not serve is a skip")
				require.NotNil(t, h.lastBackfillAt(groupID), "a walk whose only items are skips stamps last_backfill_at")
				assert.Contains(t, h.logs.String(), "reason=object_not_served")
				assert.False(t, hostGoneSince(t, h, servingHost).Valid, "a live host gets no host-gone streak")
				return
			}
			require.Error(t, runErr, "the HTML item is a failure")
			require.Nil(t, h.lastBackfillAt(groupID), "a failed item leaves last_backfill_at unset")
		})
	}
}

// lemmyNotFound answers 400 with Lemmy's purged-object body for code.
func lemmyNotFound(code string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"` + code + `"}`))
	}
}

// TestBackfillLemmyNotFound: Lemmy answers a fetch of a purged object 400
// with an error code naming the object kind. Through resolveEmbedded, the code
// matching the embedded item's type (couldnt_find_post for a Page or Article,
// couldnt_find_comment for a Note) is a skip; a mismatched code, or a type
// with no matching code, stays a failure. On a post's replies walk the
// matching code logs the reply as skipped (INFO) and a mismatched one logs it
// as failed (WARN); neither fails the post.
func TestBackfillLemmyNotFound(t *testing.T) {
	const purgedIRI = "https://z.example/purged/1"
	outboxRows := []struct {
		name     string
		itemType string
		code     string
		wantSkip bool
	}{
		{name: "Page with couldnt_find_post is skipped", itemType: "Page", code: "couldnt_find_post", wantSkip: true},
		{name: "Article with couldnt_find_post is skipped", itemType: "Article", code: "couldnt_find_post", wantSkip: true},
		{name: "Note with couldnt_find_comment is skipped", itemType: "Note", code: "couldnt_find_comment", wantSkip: true},
		{name: "Page with couldnt_find_comment is a failure", itemType: "Page", code: "couldnt_find_comment", wantSkip: false},
		{name: "Question with couldnt_find_post is a failure", itemType: "Question", code: "couldnt_find_post", wantSkip: false},
	}
	for _, tt := range outboxRows {
		t.Run("outbox: "+tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.subscribeTechnology()
			h.mux.HandleFunc("GET /purged/1", lemmyNotFound(tt.code))
			h.serveObject("/c/technology/outbox", map[string]any{
				"type":         "OrderedCollection",
				"id":           groupID + "/outbox",
				"totalItems":   1,
				"orderedItems": []any{map[string]any{"id": purgedIRI, "type": tt.itemType}},
			})
			rig := newUnreachableRig(t, h, nil, nil)

			runErr := rig.run(t)

			if tt.wantSkip {
				require.NoError(t, runErr, "a purged object answered with its own kind's code is a skip")
				require.NotNil(t, h.lastBackfillAt(groupID), "a walk whose only item is a skip stamps last_backfill_at")
				return
			}
			require.Error(t, runErr, "a 400 whose code does not match the item's kind is a failure")
			require.Nil(t, h.lastBackfillAt(groupID), "a failed item leaves last_backfill_at unset")
		})
	}

	const purgedReply = "https://z.example/comment/1"
	replyRows := []struct {
		name          string
		code          string
		wantSkipped   []string
		wantFailedLog []string
	}{
		{name: "couldnt_find_comment logs the reply skipped", code: "couldnt_find_comment", wantSkipped: []string{"INFO"}, wantFailedLog: nil},
		{name: "couldnt_find_post logs the reply failed", code: "couldnt_find_post", wantSkipped: nil, wantFailedLog: []string{"WARN"}},
	}
	for _, tt := range replyRows {
		t.Run("replies: "+tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.subscribeTechnology()
			serveOutboxFixtures(t, h)
			h.mux.HandleFunc("GET /comment/1", lemmyNotFound(tt.code))
			// One reply whose id is cross-authority with the collection, so the
			// walk re-fetches it from z.example.
			h.serveObject("/post/49131386/replies", map[string]any{
				"type":         "OrderedCollection",
				"id":           pageID + "/replies",
				"totalItems":   1,
				"orderedItems": []any{note(purgedReply, "https://z.example/u/ghost", pageID, "a purged reply", "2026-07-07T05:00:00.000000Z")},
			})
			rig := newUnreachableRig(t, h, nil, nil)

			require.NoError(t, rig.run(t), "a reply's fetch failure never fails the post")

			_, err := h.objects.GetByAPID(context.Background(), pageID)
			require.NoError(t, err, "the post carrying the replies collection still materializes")
			logs := h.logs.String()
			assert.Equal(t, tt.wantSkipped, logLevels(logs, "backfill reply skipped", "reply="+purgedReply),
				"levels of the reply's skipped log lines")
			assert.Equal(t, tt.wantFailedLog, logLevels(logs, "backfill reply failed", "reply="+purgedReply),
				"levels of the reply's failed log lines")
		})
	}
}

// deferredRows reads every backfill_deferred_items row, ordered by object
// IRI then community, straight from the table (not through the store).
func deferredRows(t *testing.T, h *harness) []store.DeferredItem {
	t.Helper()
	rows, err := h.db.QueryContext(context.Background(), `
		SELECT object_iri, community_ap_id, community_did, host, object_type,
			attempts, next_retry_at, created_at
		FROM backfill_deferred_items
		ORDER BY object_iri, community_ap_id`)
	require.NoError(t, err)
	defer rows.Close()
	items := []store.DeferredItem{}
	for rows.Next() {
		var item store.DeferredItem
		require.NoError(t, rows.Scan(&item.ObjectIRI, &item.CommunityAPID, &item.CommunityDID, &item.Host,
			&item.ObjectType, &item.Attempts, &item.NextRetryAt, &item.CreatedAt))
		item.NextRetryAt = item.NextRetryAt.UTC()
		item.CreatedAt = item.CreatedAt.UTC()
		items = append(items, item)
	}
	require.NoError(t, rows.Err())
	return items
}

// failingDeferItems is the real deferred store whose DeferItem always fails.
type failingDeferItems struct {
	store.DeferredBackfillItems
}

func (failingDeferItems) DeferItem(context.Context, store.DeferredItem) error {
	return stderrors.New("deferred store unavailable")
}

// TestBackfillDefersUnreachableItems: an item or reply whose host is
// unreachable is deferred on the walk that first meets it, with no prior
// streak. A reply on such a host is deferred like an outbox item and its post
// lands. Any other failure in the same walk (an origin answering 502) still
// leaves it unstamped, with the unreachable item deferred all the same. An
// item or reply whose deferral cannot be written is a failure, so the walk
// does not stamp over it.
func TestBackfillDefersUnreachableItems(t *testing.T) {
	const (
		unreachableReplyIRI = "https://x.example/comment/1"
		badGatewayIRI       = "https://y.example/post/bad-gateway"
	)
	serveUnreachableReply := func(t *testing.T, h *harness) {
		serveOutboxFixtures(t, h)
		h.serveObject("/post/49131386/replies", map[string]any{
			"type":         "OrderedCollection",
			"id":           pageID + "/replies",
			"totalItems":   1,
			"orderedItems": []any{note(unreachableReplyIRI, "https://x.example/u/gone", pageID, "a reply on an unreachable host", "2026-07-07T05:00:00.000000Z")},
		})
	}
	tests := []struct {
		name      string
		serve     func(t *testing.T, h *harness)
		failDefer bool
		// wantRows lists the expected rows as (object IRI, object type), all
		// on x.example.
		wantRows     [][2]string
		wantStamped  bool
		materialized []string
	}{
		{
			name: "an unreachable item beside a 502 item is deferred and the walk stays unstamped",
			serve: func(_ *testing.T, h *harness) {
				h.mux.HandleFunc("GET /post/bad-gateway", func(w http.ResponseWriter, _ *http.Request) {
					http.Error(w, "bad gateway", http.StatusBadGateway)
				})
				h.serveObject("/c/technology/outbox", map[string]any{
					"type":       "OrderedCollection",
					"id":         groupID + "/outbox",
					"totalItems": 2,
					"orderedItems": []any{
						map[string]any{
							"id":           unreachablePageIRI(1),
							"type":         "Page",
							"attributedTo": "https://x.example/u/gone",
							"audience":     groupID,
							"to":           []any{groupID, ap.PublicAudience},
							"name":         "a post from an unreachable host",
							"content":      "<p>unreachable</p>",
							"published":    "2026-11-07T09:00:00.000000Z",
						},
						map[string]any{"id": badGatewayIRI, "type": "Page"},
					},
				})
			},
			wantRows:    [][2]string{{"https://x.example/post/1", "Page"}},
			wantStamped: false,
		},
		{
			name:        "an item whose deferral fails is a failure",
			serve:       func(_ *testing.T, h *harness) { serveUnreachableOutbox(h, 1, false) },
			failDefer:   true,
			wantRows:    [][2]string{},
			wantStamped: false,
		},
		{
			name:         "a reply on an unreachable host is deferred and its post lands",
			serve:        serveUnreachableReply,
			wantRows:     [][2]string{{unreachableReplyIRI, "Note"}},
			wantStamped:  true,
			materialized: []string{pageID},
		},
		{
			name:        "a reply whose deferral fails fails its post's item",
			serve:       serveUnreachableReply,
			failDefer:   true,
			wantRows:    [][2]string{},
			wantStamped: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.subscribeTechnology()
			tt.serve(t, h)
			adjust := func(opts *BackfillOptions) {
				if tt.failDefer {
					opts.Deferred = failingDeferItems{DeferredBackfillItems: opts.Deferred}
				}
			}
			rig := newUnreachableRigWith(t, h,
				map[string]error{unreachableHost: connectionRefused()},
				map[string]probeAnswer{unreachableHost: probeHTML}, adjust)

			runErr := rig.run(t)

			assert.Equal(t, walkDeferredRows(h, unreachableHost, tt.wantRows...), deferredRows(t, h), "deferred rows after the walk")
			if tt.wantStamped {
				assert.NoError(t, runErr, "a walk whose only non-successes are deferred items succeeds")
				assert.NotNil(t, h.lastBackfillAt(groupID), "a walk whose only non-successes are deferred items stamps last_backfill_at")
			} else {
				assert.Error(t, runErr, "the walk has a failed item")
				assert.Nil(t, h.lastBackfillAt(groupID), "a walk with a failed item leaves last_backfill_at unset")
			}
			for _, id := range tt.materialized {
				_, err := h.objects.GetByAPID(context.Background(), id)
				assert.NoError(t, err, "%s must be materialized", id)
			}
		})
	}
}

// TestBackfillCachedHitWithoutVerdictRecordsNothing: a cached unreachable-host
// error repeats an observation already made in the walk. Even when the walk
// holds no verdict for that host (the observation that seeded the cache was
// never recorded as a walk verdict), it must not be recorded into the host's
// streak again.
func TestBackfillCachedHitWithoutVerdictRecordsNothing(t *testing.T) {
	h := newHarness(t)
	h.subscribeTechnology()
	rig := newUnreachableRig(t, h,
		map[string]error{unreachableHost: connectionRefused()},
		map[string]probeAnswer{unreachableHost: probeHTML})
	ctx := withWalkHosts(ap.WithUnreachableHostCache(context.Background()))
	_, seedErr := rig.backfill.fetcher.FetchObject(ctx, unreachablePageIRI(1))
	require.True(t, errors.IsHostGone(seedErr), "precondition: the first fetch finds x.example unreachable: %v", seedErr)
	_, cachedErr := rig.backfill.fetcher.FetchObject(ctx, unreachablePageIRI(2))
	require.ErrorIs(t, cachedErr, ap.ErrCachedHostVerdict, "precondition: the second fetch is a cache hit")

	rig.backfill.recordHostGone(ctx, cachedErr)

	assert.Empty(t, rig.streaks.recordedHosts(), "a cache hit with no walk verdict records no streak")
}

// TestBackfillDeferNamesNoHost: a dead-host deferral whose cause names no
// host cannot be kept (the row's host is what recovery keys on), so it is an
// error and nothing is stored; the caller then counts the item a failure.
func TestBackfillDeferNamesNoHost(t *testing.T) {
	h := newHarness(t)
	h.subscribeTechnology()
	rig := newUnreachableRig(t, h, nil, nil)

	err := rig.backfill.deferDeadHostItem(context.Background(),
		&ap.Object{ID: unreachablePageIRI(1), Type: ap.TypePage}, groupID, h.communityDID(groupID),
		stderrors.New("a failure that names no host"))

	assert.Error(t, err, "a deferral whose cause names no host fails")
	assert.Equal(t, []store.DeferredItem{}, deferredRows(t, h), "no row is stored")
}
