package ingest

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
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
	rec, err := NewFollowReconciler(FollowReconcilerOptions{Admin: h.admin, Path: path})
	require.NoError(h.t, err)
	return rec
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
		"communites:\n  - \"!technology@lemmy.world\"\n", // typo'd key (KnownFields)
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
	parse := func(t *testing.T, content string) ([]string, error) {
		t.Helper()
		return ParseFollowList(writeFollowList(t, content))
	}

	t.Run("valid entries pass, case-insensitive dupes collapse", func(t *testing.T) {
		entries, err := parse(t, `
communities:
  - "!technology@lemmy.world"
  - "https://lemmy.ml/c/linux"
  - "!Technology@Lemmy.World"
`)
		require.NoError(t, err)
		assert.Equal(t, []string{"!technology@lemmy.world", "https://lemmy.ml/c/linux"}, entries)
	})

	t.Run("explicit empty list is valid", func(t *testing.T) {
		entries, err := parse(t, "communities: []\n")
		require.NoError(t, err)
		assert.Empty(t, entries)
	})

	t.Run("missing file", func(t *testing.T) {
		_, err := ParseFollowList(filepath.Join(t.TempDir(), "absent.yaml"))
		require.Error(t, err)
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
		"unknown key":                     "communites:\n  - \"!technology@lemmy.world\"\n",
		"malformed yaml":                  "communities: [unclosed\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parse(t, content)
			require.Error(t, err)
		})
	}
}
