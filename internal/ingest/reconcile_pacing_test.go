package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// newPacedRig is an instance rig whose adds mint for real on the fixture's 5 a
// minute budget, with the sweep's Now, the budget and PLC on one manual clock.
func newPacedRig(t *testing.T, h *harness, followList string) (*instanceRig, *mintFixture) {
	t.Helper()
	admin, mint := newMintingAdmin(t, h, nil)
	return newInstanceRig(t, h, followList, instanceRigOptions{admin: admin, budget: mint.budget, clock: mint.clock}), mint
}

// startSweep runs one pass on its own goroutine, filling in the result once it
// returns. Its context ends before the cleanup runs, so a failed test leaves no
// sweep running against the database.
func (r *instanceRig) startSweep(t *testing.T) (*blockingCall, *SweepResult) {
	t.Helper()
	ctx := t.Context()
	result := new(SweepResult)
	call := startCall("Sweep", func() error {
		swept, err := r.reconciler.Sweep(ctx)
		*result = swept
		return err
	})
	t.Cleanup(func() { <-call.finished })
	return call, result
}

// finishSweep pays each of the sweep's 12 second token waits until it returns.
func (r *instanceRig) finishSweep(t *testing.T, call *blockingCall) {
	t.Helper()
	for {
		r.clock.AwaitWaiters(call)
		if call.returned() {
			require.NoError(t, call.result(t))
			return
		}
		r.clock.Advance(12 * time.Second)
	}
}

// fetchCounts gives how often each id's actor document has been fetched.
func (r *instanceRig) fetchCounts(ids ...string) []int {
	r.h.t.Helper()
	counts := make([]int, 0, len(ids))
	for _, id := range ids {
		counts = append(counts, r.h.hitCount(urlPath(r.h.t, id)))
	}
	return counts
}

// addBrokenCommunity gives host a community whose actor document answers 503,
// so an add fails for neither the budget nor a skip.
func (r *instanceRig) addBrokenCommunity(host, name string) *actorOrigin {
	r.h.t.Helper()
	r.fixture.add(host, lemmyCommunity{name: name})
	origin := r.h.serveActorOrigin("/c/"+name, map[string]any{})
	origin.answer(http.StatusServiceUnavailable)
	return origin
}

// TestSweep_WaitsForTheRateBetweenListedAdds: at 5 a minute each add after the
// first holds for 12 seconds, and none is refused for it.
func TestSweep_WaitsForTheRateBetweenListedAdds(t *testing.T) {
	const host = lemmyFixtureHost
	h := newHarness(t)
	rig, mint := newPacedRig(t, h, instanceFollowList(host))
	for _, name := range numberedIDs("paced-", 5) {
		rig.addCommunity(host, name)
	}

	sweep, result := rig.startSweep(t)
	for posts := 1; posts < 5; posts++ {
		mint.plc.awaitPosts(t, posts)
		rig.clock.AwaitWaiters(sweep)
		requireStillBlocked(t, sweep, "the next add waits for its token")
		require.Len(t, mint.plc.recordedPosts(), posts)
		rig.clock.Advance(12 * time.Second)
	}

	require.NoError(t, sweep.result(t))
	assert.Equal(t, numberedIDs("https://lemmy.world/c/paced-", 5), result.Subscribed)
	assert.Empty(t, result.Failed)
	assert.Empty(t, result.Unsubscribed)
	var offsets []time.Duration
	for _, post := range mint.plc.recordedPosts() {
		offsets = append(offsets, post.at.Sub(clockStart))
	}
	assert.Equal(t, []time.Duration{0, 12 * time.Second, 24 * time.Second, 36 * time.Second, 48 * time.Second}, offsets)
}

// TestSweep_PausedBudgetAddsNothingUntilThePauseEnds: the explicit entry reports
// its failure, the listed community is a leftover and reports nothing, and
// neither is tried. The pause ends after an hour, on the first sweep past it.
func TestSweep_PausedBudgetAddsNothingUntilThePauseEnds(t *testing.T) {
	const host = lemmyFixtureHost
	h := newHarness(t)
	rig, mint := newPacedRig(t, h, `communities:
  - "https://peer.example/c/explicit-one"
instances:
  - host: lemmy.world
`)
	listedID := rig.addCommunity(host, "listed")
	rig.serveGroup(peerExplicitID, nil)
	mint.budget.RateLimited("3600")

	sweep, result := rig.startSweep(t)

	require.NoError(t, sweep.result(t))
	assert.Equal(t, []string{peerExplicitID}, result.Failed)
	assert.Empty(t, result.Subscribed)
	assert.Empty(t, result.Unsubscribed)
	assert.Empty(t, mint.plc.recordedPosts())
	assert.Equal(t, []int{0, 0}, rig.fetchCounts(peerExplicitID, listedID), "nothing is tried during a pause")
	assert.Equal(t, []listedPage{{host, "Old", 1}}, rig.pagesSince(0))

	rig.clock.Advance(time.Hour)
	sweep, result = rig.startSweep(t)
	rig.finishSweep(t, sweep)

	assert.Equal(t, []string{peerExplicitID, listedID}, result.Subscribed)
	assert.Empty(t, result.Failed)
	assert.Empty(t, result.Unsubscribed)
	assert.Len(t, mint.plc.recordedPosts(), 2)
	follows, undos := rig.deliveredSince(0)
	assert.Equal(t, []string{peerExplicitID, listedID}, follows)
	assert.Empty(t, undos)
}

// TestSweep_PauseStartingMidBatchStopsTheAdds: each row's pause lasts 60 seconds.
// The first sweep past it adds every community the pause stopped, which a
// failed add's 1 hour skip would forbid. Neither sweep inside the pause is held
// for it, and the clock does not move for them.
func TestSweep_PauseStartingMidBatchStopsTheAdds(t *testing.T) {
	const (
		host         = lemmyFixtureHost
		firstListed  = "https://lemmy.world/c/listed-01"
		secondListed = "https://lemmy.world/c/listed-02"
		thirdListed  = "https://lemmy.world/c/listed-03"
		secondPeerID = "https://peer.example/c/explicit-two"
	)
	listedOnly := instanceFollowList(host)
	explicitFirst := `communities:
  - "https://peer.example/c/explicit-one"
  - "https://peer.example/c/explicit-two"
instances:
  - host: lemmy.world
`
	awaitFirstAdd := func(rig *instanceRig, mint *mintFixture, sweep *blockingCall) {
		mint.plc.awaitPosts(rig.h.t, 1)
		rig.clock.AwaitWaiters(sweep)
		requireStillBlocked(rig.h.t, sweep, "the second add waits for its token")
	}

	for _, tc := range []struct {
		name            string
		followList      string
		rateLimitedPost int
		// start brings the pause on, with the sweep already running.
		start func(rig *instanceRig, mint *mintFixture, sweep *blockingCall)
		// A PLC 429 costs its add's POST, and a pause during the wait costs none.
		wantPosts          int
		wantAdded          []string
		wantFailed         []string
		wantNotTried       []string
		wantFailedInPause  []string
		wantAddedAfterward []string
	}{
		{
			name:            "a PLC 429 on a listed add",
			followList:      listedOnly,
			rateLimitedPost: 2,
			start: func(rig *instanceRig, mint *mintFixture, sweep *blockingCall) {
				awaitFirstAdd(rig, mint, sweep)
				rig.clock.Advance(12 * time.Second)
			},
			wantPosts:          2,
			wantAdded:          []string{firstListed},
			wantFailed:         []string{},
			wantNotTried:       []string{thirdListed},
			wantFailedInPause:  []string{},
			wantAddedAfterward: []string{secondListed, thirdListed},
		},
		{
			name:       "a pause while a listed add waits for its token",
			followList: listedOnly,
			start: func(rig *instanceRig, mint *mintFixture, sweep *blockingCall) {
				awaitFirstAdd(rig, mint, sweep)
				mint.budget.RateLimited("60")
			},
			wantPosts:          1,
			wantAdded:          []string{firstListed},
			wantFailed:         []string{},
			wantNotTried:       []string{thirdListed},
			wantFailedInPause:  []string{},
			wantAddedAfterward: []string{secondListed, thirdListed},
		},
		{
			name:            "a PLC 429 on an explicit add",
			followList:      explicitFirst,
			rateLimitedPost: 1,
			start:           func(*instanceRig, *mintFixture, *blockingCall) {},
			wantPosts:       1,
			wantAdded:       []string{},
			// The add the 429 stopped, then the entry left untried.
			wantFailed:         []string{peerExplicitID, secondPeerID},
			wantNotTried:       []string{secondPeerID, firstListed},
			wantFailedInPause:  []string{peerExplicitID, secondPeerID},
			wantAddedAfterward: []string{peerExplicitID, secondPeerID, firstListed, secondListed, thirdListed},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			rig, mint := newPacedRig(t, h, tc.followList)
			mint.plc.rateLimitedPost = tc.rateLimitedPost
			rig.serveGroup(peerExplicitID, nil)
			rig.serveGroup(secondPeerID, nil)
			for _, name := range numberedIDs("listed-", 3) {
				rig.addCommunity(host, name)
			}

			sweep, result := rig.startSweep(t)
			tc.start(rig, mint, sweep)

			require.NoError(t, sweep.result(t), "the sweep returns without waiting out the pause")
			require.Equal(t, tc.wantAdded, result.Subscribed)
			require.Equal(t, tc.wantFailed, result.Failed)
			require.Len(t, mint.plc.recordedPosts(), tc.wantPosts, "that add sends nothing more and no retry")
			for _, id := range tc.wantNotTried {
				assert.Zero(t, h.hitCount(urlPath(t, id)), "%s comes after the pause started", id)
			}
			require.Zero(t, rig.clock.waiterCount())

			everyID := []string{peerExplicitID, secondPeerID, firstListed, secondListed, thirdListed}
			fetched := rig.fetchCounts(everyID...)
			inPause := rig.clock.Now()

			held, heldResult := rig.startSweep(t)

			require.NoError(t, held.result(t), "a second sweep is not held for the pause")
			assert.Equal(t, inPause, rig.clock.Now())
			assert.Empty(t, heldResult.Subscribed)
			assert.Equal(t, tc.wantFailedInPause, heldResult.Failed)
			assert.Len(t, mint.plc.recordedPosts(), tc.wantPosts)
			assert.Equal(t, fetched, rig.fetchCounts(everyID...), "nothing is tried during a pause")

			mark := rig.deliveryCount()
			rig.clock.Advance(60 * time.Second)
			after, afterResult := rig.startSweep(t)
			rig.finishSweep(t, after)

			assert.Equal(t, tc.wantAddedAfterward, afterResult.Subscribed)
			assert.Empty(t, afterResult.Failed)
			assert.Len(t, mint.plc.recordedPosts(), tc.wantPosts+len(tc.wantAddedAfterward))
			follows, _ := rig.deliveredSince(mark)
			assert.Equal(t, tc.wantAddedAfterward, follows)
		})
	}
}

// TestSweep_ThrottledExplicitAddWithNoPauseStopsTheAdds: Retry-After 0 returns a
// throttled error and leaves no pause in force, so that error is all that stops
// the adds. It is not a row above because those rows sweep again inside a pause.
func TestSweep_ThrottledExplicitAddWithNoPauseStopsTheAdds(t *testing.T) {
	const secondPeerID = "https://peer.example/c/explicit-two"
	h := newHarness(t)
	rig, mint := newPacedRig(t, h, `communities:
  - "https://peer.example/c/explicit-one"
  - "https://peer.example/c/explicit-two"
instances:
  - host: lemmy.world
`)
	mint.plc.rateLimitedPost = 1
	mint.plc.retryAfter = "0"
	rig.serveGroup(peerExplicitID, nil)
	rig.serveGroup(secondPeerID, nil)
	listedID := rig.addCommunity(lemmyFixtureHost, "listed")

	sweep, result := rig.startSweep(t)

	require.NoError(t, sweep.result(t))
	require.False(t, mint.budget.Paused(), "this test needs no pause in force")
	assert.Equal(t, []string{peerExplicitID, secondPeerID}, result.Failed)
	assert.Empty(t, result.Subscribed)
	assert.Empty(t, result.Unsubscribed)
	assert.Len(t, mint.plc.recordedPosts(), 1)
	assert.Equal(t, []int{0, 0}, rig.fetchCounts(secondPeerID, listedID), "nothing is tried after the 429")
}

// TestAdminSubscribe_RefusesWhereASweepWaits: POST /admin/communities keeps the
// unmarked gate, so with no free token it answers at once and the same add under
// a sweep waits. The endpoint half is behaviour that must not change; the sweep
// half is what makes the difference real.
func TestAdminSubscribe_RefusesWhereASweepWaits(t *testing.T) {
	const id = "https://lemmy.world/c/manual"
	h := newHarness(t)
	rig, mint := newPacedRig(t, h, `communities:
  - "https://lemmy.world/c/manual"
`)
	rig.serveGroup(id, nil)
	router := chi.NewRouter()
	rig.reconciler.admin.Routes(router)
	h.router = router
	require.NoError(t, mint.budget.Wait(t.Context()), "a fresh budget admits one write")

	var response *httptest.ResponseRecorder
	subscribe := startCall("POST /admin/communities", func() error {
		response = h.adminRequest(http.MethodPost, "/admin/communities", map[string]any{"community": id})
		return nil
	})

	require.NoError(t, subscribe.result(t), "the endpoint must not wait for a token")
	assert.Equal(t, http.StatusBadGateway, response.Code)
	assert.Empty(t, mint.plc.recordedPosts())
	assert.Zero(t, mint.clock.waiterCount())

	sweep, result := rig.startSweep(t)
	rig.clock.AwaitWaiters(sweep)
	requireStillBlocked(t, sweep, "a sweep's add waits where the endpoint refused")
	rig.clock.Advance(12 * time.Second)

	require.NoError(t, sweep.result(t))
	assert.Equal(t, []string{id}, result.Subscribed)
	assert.Empty(t, result.Failed)
	assert.Len(t, mint.plc.recordedPosts(), 1)
}

// TestSweep_FailedAddsStepAsideForAnHour: 20 Groups that always fail sit at the
// head of a stable listing with 5 healthy ones behind them. A tried candidate
// that fails uses a slot and a skipped one uses none, so sweep 1 spends all 20
// slots on them and sweep 2 has all 20 free for the 5.
func TestSweep_FailedAddsStepAsideForAnHour(t *testing.T) {
	const host = lemmyFixtureHost
	h := newHarness(t)
	rig := newInstanceRig(t, h, instanceFollowList(host), instanceRigOptions{})
	failing := numberedIDs("https://lemmy.world/c/failing-", 20)
	healthy := numberedIDs("https://lemmy.world/c/healthy-", 5)
	origins := make([]*actorOrigin, 0, 20)
	for _, name := range numberedIDs("failing-", 20) {
		origins = append(origins, rig.addBrokenCommunity(host, name))
	}
	for _, name := range numberedIDs("healthy-", 5) {
		rig.addCommunity(host, name)
	}
	requireFetchedEach := func(want int, when string) {
		t.Helper()
		for index, origin := range origins {
			require.Equal(t, want, origin.fetchCount(), "%s: %s", when, failing[index])
		}
	}

	result := rig.sweep()

	require.Equal(t, failing, result.Failed)
	require.Empty(t, result.Subscribed)
	requireFetchedEach(1, "sweep 1")
	require.Equal(t, []int{0, 0, 0, 0, 0}, rig.fetchCounts(healthy...), "the 20 filled the batch")

	result = rig.sweep()

	require.Equal(t, healthy, result.Subscribed)
	require.Empty(t, result.Failed, "a skipped candidate reports nothing")
	requireFetchedEach(1, "sweep 2")

	rig.clock.Advance(59*time.Minute + 59*time.Second)
	result = rig.sweep()

	require.Empty(t, result.Subscribed)
	require.Empty(t, result.Failed)
	requireFetchedEach(1, "at 59m59s")

	rig.clock.Advance(time.Second)
	result = rig.sweep()

	assert.Equal(t, failing, result.Failed)
	assert.Empty(t, result.Subscribed)
	requireFetchedEach(2, "at 1h")
	follows, undos := rig.deliveredSince(0)
	assert.Equal(t, healthy, follows)
	assert.Empty(t, undos)
}

// TestSweep_ExplicitEntryInTheListingIsNotACandidate: the failing entry sorts
// ahead of 21 healthy communities. As a listed candidate it would be tried twice
// in a sweep, use one of the 20 slots and then be skipped for an hour.
func TestSweep_ExplicitEntryInTheListingIsNotACandidate(t *testing.T) {
	const (
		host     = lemmyFixtureHost
		brokenID = "https://lemmy.world/c/aa-broken"
	)
	h := newHarness(t)
	rig := newInstanceRig(t, h, `communities:
  - "https://lemmy.world/c/aa-broken"
instances:
  - host: lemmy.world
`, instanceRigOptions{})
	broken := rig.addBrokenCommunity(host, "aa-broken")
	healthy := numberedIDs("https://lemmy.world/c/healthy-", 21)
	for _, name := range numberedIDs("healthy-", 21) {
		rig.addCommunity(host, name)
	}

	result := rig.sweep()

	require.Equal(t, []string{brokenID}, result.Failed)
	require.Equal(t, healthy[:20], result.Subscribed, "the entry uses no listed slot")
	require.Equal(t, 1, broken.fetchCount())

	result = rig.sweep()

	assert.Equal(t, []string{brokenID}, result.Failed, "an explicit entry is never skipped")
	assert.Equal(t, healthy[20:], result.Subscribed)
	assert.Equal(t, 2, broken.fetchCount())
}

// TestSweep_RefusedCommunityIsFetchedOnceADay: a #nobridge Group is read once in
// 24 hours and read again at 24 hours, and it never gets a Follow.
func TestSweep_RefusedCommunityIsFetchedOnceADay(t *testing.T) {
	const host = lemmyFixtureHost
	refusedID := "https://lemmy.world/c/refusing"
	h := newHarness(t)
	rig := newInstanceRig(t, h, instanceFollowList(host), instanceRigOptions{})
	rig.fixture.add(host, lemmyCommunity{name: "refusing"})
	rig.serveGroup(refusedID, map[string]any{"summary": "<p>#nobridge</p>"})

	for _, step := range []struct {
		name        string
		advance     time.Duration
		wantFailed  []string
		wantFetches int
	}{
		{name: "sweep 1", wantFailed: []string{refusedID}, wantFetches: 1},
		{name: "sweep 2", wantFailed: []string{}, wantFetches: 1},
		{name: "at 23h59m59s", advance: 23*time.Hour + 59*time.Minute + 59*time.Second, wantFailed: []string{}, wantFetches: 1},
		{name: "at 24h", advance: time.Second, wantFailed: []string{refusedID}, wantFetches: 2},
	} {
		rig.clock.Advance(step.advance)

		result := rig.sweep()

		require.Equal(t, step.wantFailed, result.Failed, step.name)
		require.Empty(t, result.Subscribed, step.name)
		require.Equal(t, []int{step.wantFetches}, rig.fetchCounts(refusedID), step.name)
	}
	follows, undos := rig.deliveredSince(0)
	assert.Empty(t, follows, "a refused community stays unfollowed")
	assert.Empty(t, undos)
}

// TestSweep_SkippedRefusalUsesNoSlot: a refusal sorted ahead of 39 healthy
// candidates. A tried candidate that is refused uses a slot, so sweep 1 is the
// refusal and 19 healthy ones. A skipped candidate uses none, so sweep 2 adds
// the 20 left, which a slot spent on the refusal would cut to 19.
func TestSweep_SkippedRefusalUsesNoSlot(t *testing.T) {
	const host = lemmyFixtureHost
	refusedID := "https://lemmy.world/c/aa-refusing"
	h := newHarness(t)
	rig := newInstanceRig(t, h, instanceFollowList(host), instanceRigOptions{})
	rig.fixture.add(host, lemmyCommunity{name: "aa-refusing"})
	rig.serveGroup(refusedID, map[string]any{"summary": "<p>#nobridge</p>"})
	healthy := numberedIDs("https://lemmy.world/c/healthy-", 39)
	for _, name := range numberedIDs("healthy-", 39) {
		rig.addCommunity(host, name)
	}

	result := rig.sweep()

	require.Equal(t, []string{refusedID}, result.Failed)
	require.Equal(t, healthy[:19], result.Subscribed)

	result = rig.sweep()

	require.Equal(t, healthy[19:], result.Subscribed)
	assert.Empty(t, result.Failed)
	assert.Equal(t, []int{1}, rig.fetchCounts(refusedID))
	follows, undos := rig.deliveredSince(0)
	assert.Equal(t, healthy, follows)
	assert.Empty(t, undos)
}

const (
	heldCommunityID = "https://lemmy.world/c/held"
	// reconcileWriteTimeout stands in for the server's 30 seconds.
	reconcileWriteTimeout = 100 * time.Millisecond
)

// heldReconcile is POST /admin/communities/reconcile on a real loopback server
// with a short WriteTimeout, over a sweep held inside the Group fetch of the one
// explicit entry until release. A ResponseRecorder has no write deadline. The
// entry is explicit so the sweep reaches the hold before anything lists.
type heldReconcile struct {
	h       *harness
	rig     *instanceRig
	server  *httptest.Server
	entered chan struct{}
	release func()
	// requestContexts receives the server side context of each request.
	requestContexts chan context.Context
}

func newHeldReconcile(t *testing.T) *heldReconcile {
	t.Helper()
	h := newHarness(t)
	rig := newInstanceRig(t, h, `communities:
  - "https://lemmy.world/c/held"
`, instanceRigOptions{})
	rig.serveGroup(heldCommunityID, nil)
	h.admin.SetFollowReconciler(rig.reconciler)

	released := make(chan struct{})
	held := &heldReconcile{
		h: h, rig: rig,
		entered:         make(chan struct{}),
		release:         sync.OnceFunc(func() { close(released) }),
		requestContexts: make(chan context.Context, 1),
	}
	signalEntered := sync.OnceFunc(func() { close(held.entered) })
	h.mu.Lock()
	h.onFetch[urlPath(t, heldCommunityID)] = func() {
		signalEntered()
		<-released
	}
	h.mu.Unlock()

	held.server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		held.requestContexts <- r.Context()
		h.router.ServeHTTP(w, r)
	}))
	held.server.Config.WriteTimeout = reconcileWriteTimeout
	held.server.Start()
	t.Cleanup(held.server.Close)
	t.Cleanup(held.release) // Before the Close above, which waits for the handler.
	return held
}

// post sends the request on its own goroutine, filling in the status and the
// decoded result once it returns.
func (s *heldReconcile) post(t *testing.T, ctx context.Context) (*blockingCall, *int, *SweepResult) {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.server.URL+"/admin/communities/reconcile", nil)
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer "+testAdminToken)
	status, result := new(int), new(SweepResult)
	return startCall("POST /admin/communities/reconcile", func() error {
		response, err := s.server.Client().Do(request)
		if err != nil {
			return err
		}
		defer func() { _ = response.Body.Close() }()
		*status = response.StatusCode
		return json.NewDecoder(response.Body).Decode(result)
	}), status, result
}

// TestReconcile_AnswersPastTheWriteTimeout: the wait here is real, past the
// deadline the handler must extend.
func TestReconcile_AnswersPastTheWriteTimeout(t *testing.T) {
	held := newHeldReconcile(t)

	request, status, result := held.post(t, t.Context())
	requireClosed(t, held.entered, "the sweep must reach the Group fetch")
	<-time.After(3 * reconcileWriteTimeout)
	held.release()

	require.NoError(t, request.result(t), "the client must get the result")
	assert.Equal(t, http.StatusOK, *status)
	assert.Equal(t, SweepResult{Subscribed: []string{heldCommunityID}, Unsubscribed: []string{}, Failed: []string{}}, *result)
}

// TestReconcile_ClientDropDoesNotCancelTheSweep: the hold outlasts the moment the
// server sees the drop, so a sweep on the request's context would be cancelled
// before the fetch comes back.
func TestReconcile_ClientDropDoesNotCancelTheSweep(t *testing.T) {
	held := newHeldReconcile(t)
	ctx, drop := context.WithCancel(t.Context())
	defer drop()

	request, _, _ := held.post(t, ctx)
	requireClosed(t, held.entered, "the sweep must reach the Group fetch")
	requestContext := <-held.requestContexts
	drop()
	require.ErrorIs(t, request.result(t), context.Canceled)
	requireClosed(t, requestContext.Done(), "the server must see the client go")
	held.release()

	awaitCondition(t, func() bool {
		community, err := held.h.communities.GetByAPGroupID(context.Background(), heldCommunityID)
		return err == nil && community.FollowState == store.FollowStatePending
	}, "the add must complete in storage")
	follows, _ := held.rig.deliveredSince(0)
	assert.Equal(t, []string{heldCommunityID}, follows)
}

// TestSweep_LimitWarningWaitsForTheHostToMove: a read held by leftovers comes
// round again, and the warning belongs to the pass. Only the 25 newest of 10,001
// communities are untracked, so the sort=New read that reaches page 100 takes two
// sweeps to clear them.
func TestSweep_LimitWarningWaitsForTheHostToMove(t *testing.T) {
	const host = lemmyFixtureHost
	h := newHarness(t)
	rig := newInstanceRig(t, h, instanceFollowList(host), instanceRigOptions{})
	reconcilerLogs := &syncBuffer{}
	rig.reconciler.logger = slog.New(slog.NewJSONHandler(reconcilerLogs, nil))
	rig.fixture.addCommunities(host, 10001)
	rig.track(store.FollowStateAccepted, true, lemmyActorIDs(host, 1, 9976)...)
	for _, id := range lemmyActorIDs(host, 9977, 10001) {
		rig.serveGroup(id, nil)
	}
	limitWarnings := func() []map[string]any {
		records := append(listingLogRecords(t, rig.listerLogs, "WARN"), listingLogRecords(t, reconcilerLogs, "WARN")...)
		return slices.DeleteFunc(records, func(record map[string]any) bool {
			return !strings.Contains(fmt.Sprint(record["msg"]), "reached its limit")
		})
	}

	result := rig.sweep()
	requireConverged(t, result)
	require.Empty(t, result.Subscribed, "the sort=Old pass holds only tracked communities")
	require.Empty(t, limitWarnings(), "sort=Old does not reach the limit")

	mark := rig.requestCount()
	result = rig.sweep()
	requireConverged(t, result)
	require.Len(t, result.Subscribed, 20)
	require.Len(t, rig.pagesSince(mark), 100, "the read reaches sort=New page 100")
	assert.Empty(t, limitWarnings(), "the host is held, so the pass is not over")

	result = rig.sweep()
	requireConverged(t, result)
	require.Len(t, result.Subscribed, 5)
	warnings := limitWarnings()
	require.Len(t, warnings, 1, "one warning when the host moves")
	assert.Equal(t, host, warnings[0]["host"])
	assert.Equal(t, float64(10000), warnings[0]["limit"])

	mark = rig.requestCount()
	require.Empty(t, rig.sweep().Subscribed)
	assert.Equal(t, oldPages(host, 1, 100), rig.pagesSince(mark), "the host went back to sort=Old page 1")
	assert.Len(t, limitWarnings(), 1, "a sort=Old pass adds no warning")
}
