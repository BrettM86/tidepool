package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/store"
)

// Task 11 hardening tests: inbox admission control (per-IP, per-signer, and
// the dedicated tombstone-confirmation cap) and the follow retrier.

// limitedInbox rebuilds the harness's inbox with shrunk admission limits
// and swaps it into a fresh router (the default limits are deliberately far
// too generous for a unit test to exhaust).
func (h *harness) limitedInbox(mutate func(*InboxOptions)) {
	h.t.Helper()
	opts := InboxOptions{
		Verifier: ap.NewVerifier(h.client),
		Events:   h.events,
		Queue:    h.queue,
		Service:  h.service,
		Fetcher:  h.client,
	}
	mutate(&opts)
	inbox, err := NewInbox(opts)
	require.NoError(h.t, err)
	h.inbox = inbox
	router := chi.NewRouter()
	h.inbox.Routes(router)
	h.admin.Routes(router)
	h.router = router
}

// TestInboxPerIPRateLimit: past the per-IP burst, deliveries are refused
// 503 (retryable — Lemmy redelivers; a 4xx would drop them forever) BEFORE
// signature verification, and nothing is enqueued.
func TestInboxPerIPRateLimit(t *testing.T) {
	h := newHarness(t)
	h.limitedInbox(func(o *InboxOptions) {
		o.IPRatePerSecond = 0.001
		o.IPRateBurst = 2
	})
	alice := h.newRemoteActor("https://lemmy.world/u/ipburst", person("https://lemmy.world/u/ipburst", "ipburst", nil))

	before := InboxRateLimited.Value()
	for i := 1; i <= 2; i++ {
		status := h.deliver(alice, likeActivity(fmt.Sprintf("https://lemmy.world/activities/like/ip-%d", i), alice.id))
		require.Equal(t, http.StatusAccepted, status, "delivery %d within the burst", i)
	}
	status := h.deliver(alice, likeActivity("https://lemmy.world/activities/like/ip-3", alice.id))
	assert.Equal(t, http.StatusServiceUnavailable, status,
		"past the burst the inbox must defer with 503, not drop with 4xx")
	_, err := h.events.GetEvent(context.Background(), "https://lemmy.world/activities/like/ip-3")
	assert.Error(t, err, "rate-limited deliveries must not be enqueued")
	assert.Greater(t, InboxRateLimited.Value(), before,
		"a refusal must increment the tidepool_inbox_ratelimited counter")
}

// TestAdminMetricsScoped: /admin/metrics exposes tidepool's own counters and
// NOT Go's global cmdline/memstats that expvar.Handler() would leak.
func TestAdminMetricsScoped(t *testing.T) {
	h := newHarness(t)
	InboxRateLimited.Add(1) // ensure the key is present

	rec := h.adminRequest(http.MethodGet, "/admin/metrics", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "tidepool_inbox_ratelimited")
	assert.NotContains(t, body, "cmdline", "the scoped metrics endpoint must not leak Go's cmdline")
	assert.NotContains(t, body, "memstats", "the scoped metrics endpoint must not leak Go's memstats")
}

// TestInboxPerSignerRateLimit: one identity is bounded even when the IP
// bucket has room; a different signer (same IP) keeps flowing.
func TestInboxPerSignerRateLimit(t *testing.T) {
	h := newHarness(t)
	h.limitedInbox(func(o *InboxOptions) {
		o.SignerRatePerSecond = 0.001
		o.SignerRateBurst = 2
	})
	spammer := h.newRemoteActor("https://lemmy.world/u/spammer", person("https://lemmy.world/u/spammer", "spammer", nil))
	bystander := h.newRemoteActor("https://lemmy.world/u/bystander", person("https://lemmy.world/u/bystander", "bystander", nil))

	for i := 1; i <= 2; i++ {
		status := h.deliver(spammer, likeActivity(fmt.Sprintf("https://lemmy.world/activities/like/sg-%d", i), spammer.id))
		require.Equal(t, http.StatusAccepted, status)
	}
	status := h.deliver(spammer, likeActivity("https://lemmy.world/activities/like/sg-3", spammer.id))
	assert.Equal(t, http.StatusServiceUnavailable, status, "the signer's budget is spent")

	status = h.deliver(bystander, likeActivity("https://lemmy.world/activities/like/sg-4", bystander.id))
	assert.Equal(t, http.StatusAccepted, status,
		"a different signer on the same IP must have its own bucket")
}

// TestInboxTombstoneConfirmationRateLimited: the dedicated per-IP cap on the
// tombstonedSelfDelete branch. Past the burst, the delivery is DEFERRED
// (503, sender redelivers — a legitimate deletion must never be dropped)
// and, critically, NO confirmation fetch goes out: the cap sits in front of
// the outbound request the branch can be farmed for.
func TestInboxTombstoneConfirmationRateLimited(t *testing.T) {
	h := newHarness(t)
	h.limitedInbox(func(o *InboxOptions) {
		o.TombstoneConfirmRatePerSecond = 0.001
		o.TombstoneConfirmBurst = 1
	})
	const ghost = "https://lemmy.world/u/ghost-limited"
	var ghostHits atomic.Int64
	h.mux.HandleFunc("GET "+urlPath(h.t, ghost), func(w http.ResponseWriter, _ *http.Request) {
		ghostHits.Add(1)
		w.WriteHeader(http.StatusGone)
	})
	key, err := ap.GenerateRSAKey()
	require.NoError(t, err)
	deleted := &remoteActor{id: ghost, key: key}

	// First self-delete: within the burst — confirmed and accepted.
	// Each allowed delivery costs two fetches of the actor IRI: key
	// resolution during Verify, then the independent confirmation.
	status := h.deliver(deleted, selfDelete("https://lemmy.world/activities/delete/lim-1", ghost))
	require.Equal(t, http.StatusAccepted, status)
	hitsAfterFirst := ghostHits.Load()
	require.Equal(t, int64(2), hitsAfterFirst, "an admitted self-delete costs key resolution + confirmation")

	// Second: the tombstone bucket is spent. Deferred, and only the key
	// resolution fetch happened — the confirmation fetch was never spent.
	status = h.deliver(deleted, selfDelete("https://lemmy.world/activities/delete/lim-2", ghost))
	assert.Equal(t, http.StatusServiceUnavailable, status,
		"a rate-limited tombstone confirmation must defer, not reject")
	assert.Equal(t, hitsAfterFirst+1, ghostHits.Load(),
		"the confirmation fetch must not go out while rate-limited")
	_, err = h.events.GetEvent(context.Background(), "https://lemmy.world/activities/delete/lim-2")
	assert.Error(t, err, "deferred deliveries must not be enqueued")
}

// TestFollowRetrierResendsStalePending: a subscription stuck in pending
// past the threshold gets a fresh Follow (new activity id) and consumes one
// attempt; an exhausted budget stops the retries.
func TestFollowRetrierResendsStalePending(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	// Subscribe but never deliver the Accept: the community sits pending
	// with attempts=1 (the subscribe's own Follow send).
	h.technologyGroup()
	webfinger, err := os.ReadFile(filepath.Join("..", "ap", "testdata", "webfinger_group.json"))
	require.NoError(t, err)
	h.serveJSON("/.well-known/webfinger", webfinger)
	rec := h.adminRequest(http.MethodPost, "/admin/communities",
		map[string]any{"community": "!technology@lemmy.world"})
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())

	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	require.Equal(t, store.FollowStatePending, community.FollowState)
	require.Equal(t, 1, community.FollowAttempts)
	h.mu.Lock()
	firstFollow := string(h.inboxLog[len(h.inboxLog)-1])
	deliveries := len(h.inboxLog)
	h.mu.Unlock()

	retrier, err := NewFollowRetrier(FollowRetrierOptions{
		Client:      h.client,
		Communities: h.communities,
		Service:     h.service,
		ResendAfter: time.Nanosecond, // everything pending is instantly stale
		MaxAttempts: 3,
	})
	require.NoError(t, err)

	retrier.Sweep(ctx)
	h.mu.Lock()
	require.Len(t, h.inboxLog, deliveries+1, "the sweep must re-send exactly one Follow")
	resent := h.inboxLog[len(h.inboxLog)-1]
	h.mu.Unlock()
	follow, err := ap.ParseObject(resent)
	require.NoError(t, err)
	assert.Equal(t, ap.TypeFollow, follow.Type)
	assert.Equal(t, groupID, follow.Object.ID)
	prev, err := ap.ParseObject([]byte(firstFollow))
	require.NoError(t, err)
	assert.NotEqual(t, prev.ID, follow.ID,
		"a re-sent Follow must mint a FRESH activity id (Lemmy dedupes by id)")

	community, err = h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	assert.Equal(t, 2, community.FollowAttempts)

	// One more sweep consumes the last attempt; after that the budget is
	// exhausted and sweeps stop sending.
	retrier.Sweep(ctx)
	retrier.Sweep(ctx)
	retrier.Sweep(ctx)
	h.mu.Lock()
	total := len(h.inboxLog)
	h.mu.Unlock()
	assert.Equal(t, deliveries+2, total, "retries stop at the attempt budget")
	community, err = h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	assert.Equal(t, 3, community.FollowAttempts)
	assert.Equal(t, store.FollowStatePending, community.FollowState,
		"an exhausted budget leaves the state pending for the operator to see")

	// An accepted community is never touched.
	require.NoError(t, h.communities.SetFollowState(ctx, groupID, store.FollowStateAccepted))
	retrier.Sweep(ctx)
	h.mu.Lock()
	assert.Len(t, h.inboxLog, total, "accepted subscriptions are not re-followed")
	h.mu.Unlock()
}

// captureHandler is a minimal slog.Handler recording the level and message of
// every emitted record, so a test can assert a loud (Error/Warn) log fired.
type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (c *captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (c *captureHandler) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records = append(c.records, r)
	return nil
}
func (c *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *captureHandler) WithGroup(string) slog.Handler      { return c }

func (c *captureHandler) has(level slog.Level, substr string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.records {
		if r.Level == level && strings.Contains(r.Message, substr) {
			return true
		}
	}
	return false
}

// TestFollowRetrierAcceptMidCycleNotClobbered pins the race the atomic claim
// closes: an Accept that flips a subscription to accepted AFTER the retrier
// claimed the row (but before it sends) must NOT be downgraded back to
// pending. resend writes no follow_state at all — the claim is the only
// state-touching step, and it only matches pending rows.
func TestFollowRetrierAcceptMidCycleNotClobbered(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	h.technologyGroup()
	webfinger, err := os.ReadFile(filepath.Join("..", "ap", "testdata", "webfinger_group.json"))
	require.NoError(t, err)
	h.serveJSON("/.well-known/webfinger", webfinger)
	rec := h.adminRequest(http.MethodPost, "/admin/communities",
		map[string]any{"community": "!technology@lemmy.world"})
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())

	retrier, err := NewFollowRetrier(FollowRetrierOptions{
		Client:      h.client,
		Communities: h.communities,
		Service:     h.service,
		ResendAfter: time.Nanosecond,
		MaxAttempts: 5,
	})
	require.NoError(t, err)

	// Claim the pending row exactly like Sweep would (attempts 1 → 2), then
	// simulate the Accept landing mid-cycle before the send.
	claimed, err := h.communities.ClaimStalePendingFollows(ctx, time.Now(), 5)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.NoError(t, h.communities.SetFollowState(ctx, groupID, store.FollowStateAccepted))

	h.mu.Lock()
	before := len(h.inboxLog)
	h.mu.Unlock()

	// resend sends the (now redundant, harmless) Follow but must not touch
	// state.
	retrier.resend(ctx, claimed[0])

	h.mu.Lock()
	assert.Len(t, h.inboxLog, before+1, "the claimed Follow is still delivered")
	h.mu.Unlock()
	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	assert.Equal(t, store.FollowStateAccepted, community.FollowState,
		"a mid-cycle Accept must not be clobbered back to pending")
}

// TestFollowRetrierExhaustionLogsLoudly: the final budgeted send emits a loud
// Error log — the subscription will stay pending forever (no later sweep can
// claim it) and only an operator re-subscribe recovers it, so the old
// misleading "will retry" must be gone.
func TestFollowRetrierExhaustionLogsLoudly(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	h.technologyGroup()
	webfinger, err := os.ReadFile(filepath.Join("..", "ap", "testdata", "webfinger_group.json"))
	require.NoError(t, err)
	h.serveJSON("/.well-known/webfinger", webfinger)
	rec := h.adminRequest(http.MethodPost, "/admin/communities",
		map[string]any{"community": "!technology@lemmy.world"})
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())

	capture := &captureHandler{}
	retrier, err := NewFollowRetrier(FollowRetrierOptions{
		Client:      h.client,
		Communities: h.communities,
		Service:     h.service,
		ResendAfter: time.Nanosecond,
		MaxAttempts: 2, // subscribe already burned attempt 1; the next send is final
		Logger:      slog.New(capture),
	})
	require.NoError(t, err)

	retrier.Sweep(ctx)

	assert.True(t, capture.has(slog.LevelError, "final Follow sent"),
		"the exhausting send must log a loud Error, not a misleading retry")
	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	assert.Equal(t, 2, community.FollowAttempts)
	assert.Equal(t, store.FollowStatePending, community.FollowState)
}
