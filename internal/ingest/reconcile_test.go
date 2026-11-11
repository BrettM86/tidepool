package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/identity"
	"tidepool/internal/outbound"
	"tidepool/internal/store"
)

const gamingGroupID = "https://lemmy.world/c/gaming"

// writeFollowList writes a follow-list YAML into a temp dir and returns its
// path.
func writeFollowList(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "communities.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	return path
}

func (h *harness) newReconciler(path string) *FollowReconciler {
	h.t.Helper()
	return h.newSwitchedReconciler(path, nil)
}

// newSwitchedReconciler is newReconciler with an outbound switch setter.
func (h *harness) newSwitchedReconciler(path string, switches OutboundSwitchSetter) *FollowReconciler {
	h.t.Helper()
	rec, err := NewFollowReconciler(FollowReconcilerOptions{
		Admin: h.admin, Path: path, Switches: switches, Backfill: h.backfills,
		Lister: noCommunitiesLister{}, Posts: noPostsReader{}, Budget: unlimitedBudget(h.t),
	})
	require.NoError(h.t, err)
	return rec
}

// noCommunitiesLister is a CommunityLister for a follow list with no
// `instances`.
type noCommunitiesLister struct{}

func (noCommunitiesLister) ListCommunities(context.Context, string, ListingPosition) CommunityListing {
	return CommunityListing{Next: ListingPosition{Sort: "Old", Page: 1}}
}

// noPostsReader is a RecentPostReader for a follow list with no `instances`.
type noPostsReader struct{}

func (noPostsReader) RecentPostCommunities(context.Context, string, PostMark) PostWalk {
	return PostWalk{}
}

// unlimitedBudget is a real budget for tests that do not care about pacing:
// one write every 100 microseconds on the real clock.
func unlimitedBudget(t *testing.T) *identity.PLCWriteBudget {
	t.Helper()
	budget, err := identity.NewPLCWriteBudget(identity.PLCWriteBudgetOptions{
		WritesPerMinute: 600000, Logger: slog.New(slog.DiscardHandler),
	})
	require.NoError(t, err)
	return budget
}

// A reconciler built without retries would leave failed backfills unfinished
// for good, one without a lister, post reader or budget could not serve
// `instances`, and
// nothing tests main, so a missing dependency fails at boot.
func TestNewFollowReconciler_RejectsNilBackfill(t *testing.T) {
	_, err := NewFollowReconciler(FollowReconcilerOptions{
		Admin: &Admin{}, Path: "communities.yaml", Lister: noCommunitiesLister{}, Posts: noPostsReader{},
		Budget: unlimitedBudget(t),
	})
	require.Error(t, err)
	assert.True(t, errors.IsValidation(err), "got %v", err)
	assert.ErrorContains(t, err, "backfill")
}

func TestNewFollowReconciler_RejectsNilListerAndBudget(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options FollowReconcilerOptions
		want    string
	}{
		{"nil lister", FollowReconcilerOptions{Posts: noPostsReader{}, Budget: unlimitedBudget(t)}, "lister"},
		{"nil posts", FollowReconcilerOptions{Lister: noCommunitiesLister{}, Budget: unlimitedBudget(t)}, "posts"},
		{"nil budget", FollowReconcilerOptions{Lister: noCommunitiesLister{}, Posts: noPostsReader{}}, "budget"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.options.Admin = &Admin{}
			tc.options.Path = "communities.yaml"
			tc.options.Backfill = &recordingBackfill{}
			_, err := NewFollowReconciler(tc.options)
			require.Error(t, err)
			assert.True(t, errors.IsValidation(err), "got %v", err)
			assert.ErrorContains(t, err, tc.want)
		})
	}
}

// gamingGroup serves a second lemmy.world community (the technology fixture
// re-badged), reachable by URL so tests with two communities don't collide
// on the single webfinger fixture path.
func (h *harness) gamingGroup(extra map[string]any) *remoteActor {
	h.t.Helper()
	doc := loadFixture(h.t, "group_lemmy_world.json")
	doc["id"] = gamingGroupID
	doc["preferredUsername"] = "gaming"
	doc["name"] = "Gaming"
	for k, v := range extra {
		doc[k] = v
	}
	return h.newRemoteActor(gamingGroupID, doc)
}

// serveTechnologyWebfinger registers the webfinger fixture that resolves
// !technology@lemmy.world.
func (h *harness) serveTechnologyWebfinger() {
	h.t.Helper()
	webfinger, err := os.ReadFile(filepath.Join("..", "ap", "testdata", "webfinger_group.json"))
	require.NoError(h.t, err)
	h.serveJSON("/.well-known/webfinger", webfinger)
}

// TestReconcileConverges: an empty table converges to the file (one handle
// entry, one URL entry — both spellings subscribe), and a second sweep over
// the converged state is a complete no-op that never touches the network.
func TestReconcileConverges(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.technologyGroup()
	h.serveTechnologyWebfinger()
	h.gamingGroup(nil)

	rec := h.newReconciler(writeFollowList(t, `
communities:
  - "!technology@lemmy.world"
  - "https://lemmy.world/c/gaming"
`))

	result, err := rec.Sweep(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"!technology@lemmy.world", "https://lemmy.world/c/gaming"}, result.Subscribed)
	assert.Empty(t, result.Unsubscribed)
	assert.Empty(t, result.Failed)

	h.mu.Lock()
	follows := len(h.inboxLog)
	h.mu.Unlock()
	assert.Equal(t, 2, follows, "one Follow per subscribed community")
	for _, id := range []string{groupID, gamingGroupID} {
		community, err := h.communities.GetByAPGroupID(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, store.FollowStatePending, community.FollowState, id)
	}

	// The second sweep matches both rows offline: no writes, no deliveries,
	// and — the mass-unfollow fail-safe — no handle re-resolution.
	webfingerHits := h.hitCount("/.well-known/webfinger")
	result, err = rec.Sweep(ctx)
	require.NoError(t, err)
	assert.Empty(t, result.Subscribed)
	assert.Empty(t, result.Unsubscribed)
	assert.Empty(t, result.Failed)
	h.mu.Lock()
	assert.Equal(t, follows, len(h.inboxLog), "a converged sweep must deliver nothing")
	h.mu.Unlock()
	assert.Equal(t, webfingerHits, h.hitCount("/.well-known/webfinger"),
		"matched entries must never re-resolve (offline diff)")
}

// TestReconcileUnsubscribesExtra: a community the file no longer names is
// unfollowed (Undo delivered, state none) while its row, DID, and records
// survive; a new entry in the same sweep is subscribed.
func TestReconcileUnsubscribesExtra(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.subscribeTechnology() // accepted, via the admin API
	h.gamingGroup(nil)

	before, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	require.NotEmpty(t, before.DID)

	rec := h.newReconciler(writeFollowList(t, `
communities:
  - "https://lemmy.world/c/gaming"
`))
	deliveriesBefore := len(h.inboxLog)

	result, err := rec.Sweep(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{groupID}, result.Unsubscribed)
	assert.Equal(t, []string{gamingGroupID}, result.Subscribed)
	assert.Empty(t, result.Failed)

	// Unsubscribes go out before subscribes: Undo{Follow(technology)}, then
	// Follow(gaming).
	h.mu.Lock()
	require.Equal(t, deliveriesBefore+2, len(h.inboxLog))
	undoRaw := h.inboxLog[len(h.inboxLog)-2]
	followRaw := h.inboxLog[len(h.inboxLog)-1]
	h.mu.Unlock()
	undo, err := ap.ParseObject(undoRaw)
	require.NoError(t, err)
	assert.Equal(t, ap.TypeUndo, undo.Type)
	require.NotNil(t, undo.Object)
	assert.Equal(t, groupID, undo.Object.Object.ID)
	follow, err := ap.ParseObject(followRaw)
	require.NoError(t, err)
	assert.Equal(t, ap.TypeFollow, follow.Type)
	assert.Equal(t, gamingGroupID, follow.Object.ID)

	after, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	assert.Equal(t, store.FollowStateNone, after.FollowState)
	assert.Equal(t, before.DID, after.DID, "unfollowing must not touch the community's DID")
}

// TestReconcileEmptyListUnfollowsAll: an explicit `communities: []` is
// honored — every subscription is unfollowed.
func TestReconcileEmptyListUnfollowsAll(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.subscribeTechnology()

	rec := h.newReconciler(writeFollowList(t, "communities: []\n"))
	result, err := rec.Sweep(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{groupID}, result.Unsubscribed)

	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	assert.Equal(t, store.FollowStateNone, community.FollowState)
}

// TestReconcileBrokenFileKeepsState: a file that breaks after startup makes
// the sweep fail with NO state changes — a parse error must never read as
// "unfollow everything".
func TestReconcileBrokenFileKeepsState(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.subscribeTechnology()

	path := writeFollowList(t, `
communities:
  - "!technology@lemmy.world"
`)
	rec := h.newReconciler(path)

	deliveriesBefore := len(h.inboxLog)
	for _, broken := range []string{
		"communities:\n  - \"technology@lemmy.world\"\n", // missing the leading '!'
		"communites:\n  - \"!technology@lemmy.world\"\n", //nolint:misspell // deliberately typo'd key (KnownFields)
		"",               // truncated to empty ≠ empty list
		"{}\n",           // no communities key ≠ empty list
		"communities:\n", // present-but-null key ≠ empty list
	} {
		require.NoError(t, os.WriteFile(path, []byte(broken), 0o644))
		_, err := rec.Sweep(ctx)
		require.Error(t, err)
	}

	h.mu.Lock()
	assert.Equal(t, deliveriesBefore, len(h.inboxLog), "failed sweeps must deliver nothing")
	h.mu.Unlock()
	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	assert.Equal(t, store.FollowStateAccepted, community.FollowState)
}

// TestReconcileConcurrentSweeps: overlapping sweeps (ticker vs the admin
// trigger) are serialized — the loser runs against the converged state, so
// a community absent in both snapshots is subscribed exactly once (a
// duplicate EnsureCommunity race could orphan a permanent DID).
func TestReconcileConcurrentSweeps(t *testing.T) {
	h := newHarness(t)
	h.technologyGroup()
	h.serveTechnologyWebfinger()

	rec := h.newReconciler(writeFollowList(t, `
communities:
  - "!technology@lemmy.world"
`))

	const sweeps = 4
	results := make([]SweepResult, sweeps)
	var wg sync.WaitGroup
	for i := range sweeps {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := rec.Sweep(context.Background())
			require.NoError(t, err)
			results[i] = result
		}()
	}
	wg.Wait()

	subscribed := 0
	for _, result := range results {
		subscribed += len(result.Subscribed)
	}
	assert.Equal(t, 1, subscribed, "exactly one sweep must win the subscribe")
	h.mu.Lock()
	assert.Equal(t, 1, len(h.inboxLog), "concurrent sweeps must deliver exactly one Follow")
	h.mu.Unlock()
}

// gatedSweeps runs sweeps on their own goroutines around a gate the test holds
// shut. Build it after newHarness: the cleanup it registers then runs before the
// harness's, so a failed assertion still releases the gate and awaits every
// sweep instead of hanging fixture-server teardown on a held request.
type gatedSweeps struct {
	rec     *FollowReconciler
	gate    chan struct{}
	release func()
	running sync.WaitGroup
}

func (h *harness) newGatedSweeps(rec *FollowReconciler) *gatedSweeps {
	sweeps := &gatedSweeps{rec: rec, gate: make(chan struct{})}
	sweeps.release = sync.OnceFunc(func() { close(sweeps.gate) })
	h.t.Cleanup(func() {
		sweeps.release()
		sweeps.running.Wait()
	})
	return sweeps
}

// sweepRun is one sweep in flight. result and err are valid once done closes.
type sweepRun struct {
	done   chan struct{}
	result SweepResult
	err    error
}

func (s *gatedSweeps) start() *sweepRun {
	run := &sweepRun{done: make(chan struct{})}
	s.running.Go(func() {
		defer close(run.done)
		run.result, run.err = s.rec.Sweep(context.Background())
	})
	return run
}

func isClosed(signal <-chan struct{}) bool {
	select {
	case <-signal:
		return true
	default:
		return false
	}
}

// requireClosed polls signal with a deadline, so a sweep stuck behind the wrong
// lock fails the test rather than hanging it.
func requireClosed(t *testing.T, signal <-chan struct{}, what string) {
	t.Helper()
	require.Eventually(t, func() bool { return isClosed(signal) }, 5*time.Second, 5*time.Millisecond, what)
}

// TestReconcileBrokenFileKeepsLastGoodSwitches: after startup a file that no
// longer parses changes nothing, and the blocks it last set stay on. The host
// block and the community block are checked in scopes that do not overlap, so
// neither can hold on the other's behalf. A later valid file with no switch keys
// lifts both.
func TestReconcileBrokenFileKeepsLastGoodSwitches(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.subscribeTechnology()

	hostScope := outbound.DeliveryScope{InboxHost: "blocked.example", CommunityAPID: "https://open.example/c/tech"}
	communityScope := outbound.DeliveryScope{InboxHost: "open.example", CommunityAPID: "https://blocked.example/c/tech"}

	switches := &outbound.ConfigSwitches{}
	path := writeFollowList(t, `
communities:
  - "!technology@lemmy.world"
outbound_disabled_hosts:
  - "blocked.example"
outbound_disabled_communities:
  - "https://blocked.example/c/tech"
`)
	rec := h.newSwitchedReconciler(path, switches)
	_, err := rec.Sweep(ctx)
	require.NoError(t, err)
	require.False(t, switches.OutboundAllowed(hostScope), "the first sweep applies the host block")
	require.False(t, switches.OutboundAllowed(communityScope), "the first sweep applies the community block")

	h.mu.Lock()
	deliveriesBefore := len(h.inboxLog)
	h.mu.Unlock()

	// Each broken file also empties the follow list, so a parse that wrongly
	// succeeded would show up as an Undo.
	for _, tc := range []struct {
		name    string
		content string
	}{
		{"bad switch entry", "communities: []\noutbound_disabled_hosts:\n  - \"https://blocked.example\"\n"},
		{"malformed yaml", "communities: [unclosed\n"},
		{"instances key", "communities: []\ninstances:\n  - \"lemmy.world\"\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, os.WriteFile(path, []byte(tc.content), 0o644))
			_, err := rec.Sweep(ctx)
			require.ErrorContains(t, err, path)

			assert.False(t, switches.OutboundAllowed(hostScope), "the host block must survive a broken file")
			assert.False(t, switches.OutboundAllowed(communityScope), "the community block must survive a broken file")
			h.mu.Lock()
			assert.Equal(t, deliveriesBefore, len(h.inboxLog), "a failed sweep must deliver no Follow or Undo")
			h.mu.Unlock()
			community, err := h.communities.GetByAPGroupID(ctx, groupID)
			require.NoError(t, err)
			assert.Equal(t, store.FollowStateAccepted, community.FollowState)
		})
	}

	require.NoError(t, os.WriteFile(path, []byte("communities:\n  - \"!technology@lemmy.world\"\n"), 0o644))
	_, err = rec.Sweep(ctx)
	require.NoError(t, err)
	assert.True(t, switches.OutboundAllowed(hostScope), "a valid file with no switch keys lifts the host block")
	assert.True(t, switches.OutboundAllowed(communityScope), "a valid file with no switch keys lifts the community block")
}

// TestReconcileSwapDoesNotWaitBehindSlowSweep: a second sweep applies its switch
// sets while the first still holds r.mu in a blocked subscribe, and once it gets
// r.mu it converges to the file as it stands then, not to the parse it made on
// the way in.
func TestReconcileSwapDoesNotWaitBehindSlowSweep(t *testing.T) {
	h := newHarness(t)
	h.gamingGroup(nil)

	const host = "late.example"
	switches := &outbound.ConfigSwitches{}
	path := writeFollowList(t, "communities:\n  - \"https://lemmy.world/c/gaming\"\n")
	rec := h.newSwitchedReconciler(path, switches)
	sweeps := h.newGatedSweeps(rec)

	// Sweep 2's arrival at r.mu cannot be observed. Its first parse, the one that
	// sees host, can.
	hostParsed := make(chan struct{})
	signalHostParsed := sync.OnceFunc(func() { close(hostParsed) })
	rec.parse = func(path string) (*FollowList, error) {
		list, err := ParseFollowList(path)
		if list != nil {
			if _, named := list.OutboundDisabledHosts[host]; named {
				signalHostParsed()
			}
		}
		return list, err
	}

	fetchReached := make(chan struct{})
	signalFetchReached := sync.OnceFunc(func() { close(fetchReached) })
	h.mu.Lock()
	h.onFetch[urlPath(t, gamingGroupID)] = func() {
		signalFetchReached()
		<-sweeps.gate
	}
	h.mu.Unlock()

	first := sweeps.start()
	requireClosed(t, fetchReached, "sweep 1 must reach the Group fetch")

	require.NoError(t, os.WriteFile(path, []byte(`
communities:
  - "https://lemmy.world/c/gaming"
outbound_disabled_hosts:
  - "late.example"
`), 0o644))
	second := sweeps.start()

	require.Eventually(t, func() bool {
		return !switches.OutboundAllowed(outbound.DeliveryScope{InboxHost: host})
	}, 5*time.Second, 5*time.Millisecond, "the new host must be refused while sweep 1 is still blocked")
	require.False(t, isClosed(first.done), "sweep 1 must still be blocked when the swap lands")
	requireClosed(t, hostParsed, "sweep 2's first parse must have completed")

	// The community sweep 1 is subscribing leaves the file while sweep 2 waits.
	require.NoError(t, os.WriteFile(path, []byte(`
communities: []
outbound_disabled_hosts:
  - "late.example"
`), 0o644))
	sweeps.release()
	requireClosed(t, first.done, "sweep 1 must finish once released")
	requireClosed(t, second.done, "sweep 2 must finish once sweep 1 has")

	require.NoError(t, first.err)
	require.NoError(t, second.err)
	assert.Equal(t, []string{gamingGroupID}, first.result.Subscribed)
	assert.Equal(t, []string{gamingGroupID}, second.result.Unsubscribed,
		"sweep 2 must converge to the file as it stood when it held r.mu")
	community, err := h.communities.GetByAPGroupID(context.Background(), gamingGroupID)
	require.NoError(t, err)
	assert.Equal(t, store.FollowStateNone, community.FollowState)
}

// switchSets is one call to an OutboundSwitchSetter.
type switchSets struct {
	hosts       map[string]struct{}
	communities map[string]struct{}
}

// recordingSwitchSetter records every call and signals the first on called.
type recordingSwitchSetter struct {
	mu     sync.Mutex
	calls  []switchSets
	called chan struct{}
}

func (s *recordingSwitchSetter) SetDisabledSets(hosts, communities map[string]struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, switchSets{hosts: hosts, communities: communities})
	select {
	case s.called <- struct{}{}:
	default:
	}
}

func (s *recordingSwitchSetter) snapshot() []switchSets {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]switchSets(nil), s.calls...)
}

// TestReconcileOlderParseCannotOverwriteNewer: parse and swap are one critical
// section. Sweep 1 sits inside its parse holding the old sets while the file
// moves on and sweep 2 starts. Sweep 2 must not reach the setter ahead of it,
// because sweep 1 would then overwrite the newer sets with the older ones.
func TestReconcileOlderParseCannotOverwriteNewer(t *testing.T) {
	h := newHarness(t)

	setter := &recordingSwitchSetter{called: make(chan struct{}, 1)}
	path := writeFollowList(t, `
communities: []
outbound_disabled_hosts:
  - "old.example"
outbound_disabled_communities:
  - "https://old.example/c/tech"
`)
	rec := h.newSwitchedReconciler(path, setter)
	sweeps := h.newGatedSweeps(rec)

	inParse := make(chan struct{})
	var parses atomic.Int32
	rec.parse = func(path string) (*FollowList, error) {
		list, err := ParseFollowList(path)
		if parses.Add(1) == 1 {
			close(inParse)
			<-sweeps.gate
		}
		return list, err
	}

	first := sweeps.start()
	requireClosed(t, inParse, "sweep 1 must enter its parse")

	require.NoError(t, os.WriteFile(path, []byte(`
communities: []
outbound_disabled_hosts:
  - "new.example"
outbound_disabled_communities:
  - "https://new.example/c/tech"
`), 0o644))
	second := sweeps.start()

	// A negative wait: correct code never fires the first case, so this timer
	// cannot fail it.
	select {
	case <-setter.called:
		t.Fatal("the setter ran while sweep 1 was inside its parse: parse and swap must share one lock")
	case <-time.After(200 * time.Millisecond):
	}

	sweeps.release()
	requireClosed(t, first.done, "sweep 1 must finish once released")
	requireClosed(t, second.done, "sweep 2 must finish once released")
	assert.NoError(t, first.err)
	assert.NoError(t, second.err)

	calls := setter.snapshot()
	require.NotEmpty(t, calls, "both sweeps parsed, so the setter must have run")
	last := calls[len(calls)-1]
	assert.Equal(t, map[string]struct{}{"new.example": {}}, last.hosts)
	assert.Equal(t, map[string]struct{}{"https://new.example/c/tech": {}}, last.communities)
}

// TestReconcileConsentRefusal: a community advertising #nobridge stays
// refused no matter what the file says; the sweep records the failure and
// keeps going.
func TestReconcileConsentRefusal(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.gamingGroup(map[string]any{"summary": "<p>#nobridge</p>"})

	rec := h.newReconciler(writeFollowList(t, `
communities:
  - "https://lemmy.world/c/gaming"
`))
	result, err := rec.Sweep(ctx)
	require.NoError(t, err, "a consent refusal is a per-entry failure, not a sweep failure")
	assert.Equal(t, []string{gamingGroupID}, result.Failed)
	assert.Empty(t, result.Subscribed)

	_, err = h.communities.GetByAPGroupID(ctx, gamingGroupID)
	assert.True(t, errors.IsNotFound(err), "a refused community must not be bridged")
}

// TestAdminReconcileEndpoint: 501 without a configured follow list, a
// synchronous sweep with one, 500 (and no state changes) on a broken file.
func TestAdminReconcileEndpoint(t *testing.T) {
	h := newHarness(t)
	h.technologyGroup()
	h.serveTechnologyWebfinger()

	rec := h.adminRequest(http.MethodPost, "/admin/communities/reconcile", nil)
	assert.Equal(t, http.StatusNotImplemented, rec.Code)

	path := writeFollowList(t, `
communities:
  - "!technology@lemmy.world"
`)
	h.admin.SetFollowReconciler(h.newReconciler(path))

	rec = h.adminRequest(http.MethodPost, "/admin/communities/reconcile", nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var result SweepResult
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
	assert.Equal(t, []string{"!technology@lemmy.world"}, result.Subscribed)

	require.NoError(t, os.WriteFile(path, []byte("not: [valid"), 0o644))
	rec = h.adminRequest(http.MethodPost, "/admin/communities/reconcile", nil)
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

// TestParseFollowList covers entry validation, dedupe, and the guard rails
// around empty/malformed files.
func TestParseFollowList(t *testing.T) {
	parse := func(t *testing.T, content string) (*FollowList, error) {
		t.Helper()
		return ParseFollowList(writeFollowList(t, content))
	}

	t.Run("valid entries pass, case-insensitive dupes collapse", func(t *testing.T) {
		list, err := parse(t, `
communities:
  - "!technology@lemmy.world"
  - "https://lemmy.ml/c/linux"
  - "!Technology@Lemmy.World"
`)
		require.NoError(t, err)
		assert.Equal(t, []string{"!technology@lemmy.world", "https://lemmy.ml/c/linux"}, list.Communities)
	})

	t.Run("explicit empty list is valid", func(t *testing.T) {
		list, err := parse(t, "communities: []\n")
		require.NoError(t, err)
		assert.Empty(t, list.Communities)
	})

	t.Run("missing file", func(t *testing.T) {
		list, err := ParseFollowList(filepath.Join(t.TempDir(), "absent.yaml"))
		require.Error(t, err)
		assert.Nil(t, list)
	})

	for name, content := range map[string]string{
		"empty file is not an empty list": "",
		"missing communities key":         "{}\n",
		"null communities key":            "communities:\n",
		"explicitly null communities key": "communities: null\n",
		"handle without leading bang":     "communities:\n  - \"technology@lemmy.world\"\n",
		"handle with two ats":             "communities:\n  - \"!a@b@c\"\n",
		"handle without host":             "communities:\n  - \"!technology@\"\n",
		"URL without host":                "communities:\n  - \"https://\"\n",
		"entry with whitespace":           "communities:\n  - \"!tech @lemmy.world\"\n",
		"blank entry":                     "communities:\n  - \"\"\n",
		"unknown key":                     "communites:\n  - \"!technology@lemmy.world\"\n", //nolint:misspell // deliberate typo fixture
		"malformed yaml":                  "communities: [unclosed\n",
	} {
		t.Run(name, func(t *testing.T) {
			list, err := parse(t, content)
			require.Error(t, err)
			assert.Nil(t, list)
		})
	}
}

// TestParseFollowList_OutboundSwitchKeys: the two optional switch keys reach
// the struct. Community URLs match exactly, so they keep their case; hosts are
// lowercased because a delivery's inbox host is.
func TestParseFollowList_OutboundSwitchKeys(t *testing.T) {
	t.Run("both keys are carried", func(t *testing.T) {
		list, err := ParseFollowList(writeFollowList(t, `
communities:
  - "!technology@lemmy.world"
outbound_disabled_hosts:
  - "Lemmy.World"
  - "lemmy"
outbound_disabled_communities:
  - "https://Lemmy.World/c/TechNews"
  - "http://lemmy:8536/c/local"
`))
		require.NoError(t, err)
		assert.Equal(t, []string{"!technology@lemmy.world"}, list.Communities)
		assert.Equal(t, map[string]struct{}{"lemmy.world": {}, "lemmy": {}}, list.OutboundDisabledHosts)
		assert.Equal(t, map[string]struct{}{
			"https://Lemmy.World/c/TechNews": {},
			"http://lemmy:8536/c/local":      {},
		}, list.OutboundDisabledCommunities)
	})

	t.Run("communities alone leaves both sets empty", func(t *testing.T) {
		list, err := ParseFollowList(writeFollowList(t, "communities:\n  - \"!technology@lemmy.world\"\n"))
		require.NoError(t, err)
		assert.Empty(t, list.OutboundDisabledHosts)
		assert.Empty(t, list.OutboundDisabledCommunities)
	})
}

// TestParseFollowList_RepoFile is the only test that parses the shipped
// communities.yaml, so an edit to its comments that breaks the YAML or changes
// the followed set fails here.
func TestParseFollowList_RepoFile(t *testing.T) {
	list, err := ParseFollowList("../../communities.yaml")
	require.NoError(t, err)
	assert.Len(t, list.Communities, 13)
	assert.Equal(t, []InstanceEntry{
		{Host: "lemmy.zip"},
		{Host: "programming.dev"},
		{Host: "sh.itjust.works", Exclude: []string{"!boysjerking@sh.itjust.works", "!combatvideos@sh.itjust.works"}},
	}, list.Instances)
	assert.False(t, list.Desires("https://sh.itjust.works/c/boysjerking", "sh.itjust.works", ""))
	assert.False(t, list.Desires("https://sh.itjust.works/c/combatvideos", "sh.itjust.works", ""))
	assert.True(t, list.Desires("https://sh.itjust.works/c/opensim", "sh.itjust.works", ""))
	assert.Empty(t, list.OutboundDisabledHosts)
	assert.Empty(t, list.OutboundDisabledCommunities)
}

// assertFollowListRejected checks the file-boundary error contract: no struct,
// and an error naming the file and what is wrong with it. want is matched with
// the path removed, because the temp path carries both the test name and
// "communities.yaml" and would otherwise satisfy the match by itself.
func assertFollowListRejected(t *testing.T, path string, list *FollowList, err error, want string) {
	t.Helper()
	require.Error(t, err)
	assert.Nil(t, list)
	assert.Contains(t, err.Error(), path)
	assert.Contains(t, strings.ReplaceAll(err.Error(), path, ""), want)
}

// TestParseFollowList_RejectedSwitchEntries: a bad switch entry or a null list
// item stops the parse.
func TestParseFollowList_RejectedSwitchEntries(t *testing.T) {
	communitySwitch := func(entry string) string {
		return "communities: []\noutbound_disabled_communities:\n  - \"" + entry + "\"\n"
	}
	hostSwitch := func(entry string) string {
		return "communities: []\noutbound_disabled_hosts:\n  - \"" + entry + "\"\n"
	}
	for _, tc := range []struct {
		name    string
		content string
		want    string
	}{
		{"community with a non-http scheme", communitySwitch("ftp://lemmy.world/c/tech"), "ftp://lemmy.world/c/tech"},
		{"community that is not a URL", communitySwitch("not-a-url"), "not-a-url"},
		{"community written as a handle", communitySwitch("!tech@lemmy.world"), "!tech@lemmy.world"},
		{"community URL without a host", communitySwitch("https://"), "https://"},
		{"host with a scheme", hostSwitch("https://lemmy.world"), "https://lemmy.world"},
		{"host with a port", hostSwitch("lemmy.world:8536"), "lemmy.world:8536"},
		{"host with a path", hostSwitch("lemmy.world/path"), "lemmy.world/path"},
		{"host with an invalid character", hostSwitch("exa$mple.com"), "exa$mple.com"},
		{"community with a space in the path", communitySwitch("https://lemmy.world/c/te ch"), "https://lemmy.world/c/te ch"},
		{"community with an uppercase scheme", communitySwitch("HTTPS://lemmy.world/c/tech"), "HTTPS://lemmy.world/c/tech"},
		{"host with a trailing dot", hostSwitch("lemmy.world."), "lemmy.world."},
		{"host with a non-ASCII letter", hostSwitch("bücher.example"), "bücher.example"},
		{
			"empty host after a valid one",
			"communities: []\noutbound_disabled_hosts:\n  - \"lemmy.world\"\n  - \"\"\n",
			"entry 2",
		},
		{"bare dash under hosts", "communities: []\noutbound_disabled_hosts:\n  -\n", "outbound_disabled_hosts entry 1"},
		{"tilde under hosts", "communities: []\noutbound_disabled_hosts:\n  - ~\n", "outbound_disabled_hosts entry 1"},
		{"commented-out item under hosts", "communities: []\noutbound_disabled_hosts:\n  - # \"lemmy.world\"\n", "outbound_disabled_hosts entry 1"},
		{"null after a valid host", "communities: []\noutbound_disabled_hosts:\n  - \"lemmy.world\"\n  -\n", "outbound_disabled_hosts entry 2"},
		{"bare dash under communities switch", "communities: []\noutbound_disabled_communities:\n  -\n", "outbound_disabled_communities entry 1"},
		{"tilde under communities switch", "communities: []\noutbound_disabled_communities:\n  - ~\n", "outbound_disabled_communities entry 1"},
		{"commented-out item under communities switch", "communities: []\noutbound_disabled_communities:\n  - # \"https://lemmy.world/c/tech\"\n", "outbound_disabled_communities entry 1"},
		{"null after a valid community switch", "communities: []\noutbound_disabled_communities:\n  - \"https://lemmy.world/c/tech\"\n  -\n", "outbound_disabled_communities entry 2"},
		{"bare dash under communities", "communities:\n  -\n", "entry 1:"},
		{"commented-out item under communities", "communities:\n  - # \"!tech@lemmy.world\"\n", "entry 1:"},
		{"null after a valid community", "communities:\n  - \"!tech@lemmy.world\"\n  -\n", "entry 2:"},
		{
			"switch keys do not stand in for the follow key",
			"outbound_disabled_hosts:\n  - \"lemmy.world\"\n",
			"communities",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeFollowList(t, tc.content)
			list, err := ParseFollowList(path)
			assertFollowListRejected(t, path, list, err, tc.want)
		})
	}
}

// TestParseFollowList_Guard pins what the switch keys must not loosen. These
// pass before and after the change.
func TestParseFollowList_Guard(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string
		want    string
	}{
		{"guard missing communities key", "{}\n", "communities"},
		{"guard null communities key", "communities:\n", "communities"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeFollowList(t, tc.content)
			list, err := ParseFollowList(path)
			assertFollowListRejected(t, path, list, err, tc.want)
		})
	}
}
