package ingest

import (
	"context"
	"database/sql"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// BRIDGED COMMUNITY RESTORE RECONCILE: GUARDS.
//
// A pass stops once the community is removed again, before applying the
// answer of a fetch the removal landed during, and leaves its marker set;
// a pass already running for a community is not doubled, and a restored
// community that is not followed again is still reconciled.

// reconcilePendingAt reads communities.reconcile_pending_at of did.
func reconcilePendingAt(t *testing.T, h *harness, did string) sql.NullTime {
	t.Helper()
	var pendingAt sql.NullTime
	require.NoError(t, h.db.QueryRow(
		`SELECT reconcile_pending_at FROM communities WHERE did = $1`, did).Scan(&pendingAt))
	return pendingAt
}

// firehoseCommitCount counts the commits to did's repo.
func firehoseCommitCount(t *testing.T, h *harness, did string) int {
	t.Helper()
	var count int
	require.NoError(t, h.db.QueryRow(
		`SELECT count(*) FROM firehose_events WHERE did = $1`, did).Scan(&count))
	return count
}

// TestCommunityReconcile_ARemovalMidPassStopsThePass: C is removed again while
// the pass fetches its first kept post. The pass never fetches the second,
// logs an incomplete summary, and leaves C's reconcile pending.
func TestCommunityReconcile_ARemovalMidPassStopsThePass(t *testing.T) {
	world := newReconcileWorld(t)
	h := world.h
	ctx := context.Background()

	firstID := world.postBy(ubUserID, world.communityC, "rc-guard-first", ubBeforeBan, false)
	secondID := world.postBy(ubUserID, world.communityC, "rc-guard-second", ubBeforeBan, false)
	firstPath, secondPath := urlPath(t, firstID), urlPath(t, secondID)
	world.restoreAndReady()
	require.True(t, reconcilePendingAt(t, h, world.communityDID).Valid, "precondition: C's reconcile is pending")
	reconciler, logs := world.newReconciler(time.Hour)
	hitsBefore := h.hitCounts(firstPath, secondPath)

	var (
		hookMu     sync.Mutex
		hookRan    bool
		removalErr error
	)
	h.mu.Lock()
	h.onFetch[firstPath] = func() {
		hookMu.Lock()
		defer hookMu.Unlock()
		if hookRan {
			return
		}
		hookRan = true
		removalErr = world.committer.attempt(t, world.communityDID, communityTakedown, false)
	}
	h.mu.Unlock()
	t.Cleanup(func() {
		h.mu.Lock()
		delete(h.onFetch, firstPath)
		h.mu.Unlock()
	})

	require.NoError(t, reconciler.Sweep(ctx))

	hookMu.Lock()
	require.True(t, hookRan, "the first kept post was fetched")
	require.NoError(t, removalErr, "the removal committed during the first fetch")
	hookMu.Unlock()
	assert.Equal(t, hitsBefore[firstPath]+1, h.hitCount(firstPath), "the first kept post is fetched once")
	assert.Equal(t, hitsBefore[secondPath], h.hitCount(secondPath),
		"C is removed again: the pass does not fetch the second kept post")
	assert.Equal(t, []reconcilePass{{Community: world.communityDID, Complete: false}},
		reconcilePasses(t, logs), "the stopped pass logs one incomplete summary")
	assert.True(t, reconcilePendingAt(t, h, world.communityDID).Valid,
		"the stopped pass leaves C's reconcile pending")
}

// TestCommunityReconcile_APassAlreadyRunningIsNotDoubled: a Sweep that starts
// while C's pass is fetching its gone kept post runs no second pass. The post
// is fetched once and gets one removal record in one commit to C's repo.
func TestCommunityReconcile_APassAlreadyRunningIsNotDoubled(t *testing.T) {
	world := newReconcileWorld(t)
	h := world.h
	ctx := context.Background()

	goneID := world.postBy(ubUserID, world.communityC, "rc-guard-gone", ubBeforeBan, false)
	gonePath := urlPath(t, goneID)
	h.serveGone(gonePath)
	world.restoreAndReady()
	reconciler, logs := world.newReconciler(time.Hour)
	hitsBefore := h.hitCount(gonePath)
	commitsBefore := firehoseCommitCount(t, h, world.communityDID)
	_, revBefore, err := h.manager.Head(ctx, world.communityDID)
	require.NoError(t, err)
	removedBefore := reconcileCounter(t, "removed")
	passesCompletedBefore := reconcileCounter(t, "passes_completed")

	// The overlapping Sweep runs on its own goroutine, and the fetch waits for
	// it: the ap client coalesces concurrent fetches of one IRI, so a second
	// pass fetching this post would wait on this very fetch.
	var (
		hookMu             sync.Mutex
		innerSweeps        int
		innerSweepReturned bool
		innerSweepErr      error
	)
	innerSweepDone := make(chan struct{})
	h.mu.Lock()
	h.onFetch[gonePath] = func() {
		hookMu.Lock()
		if innerSweeps > 0 {
			hookMu.Unlock()
			return
		}
		innerSweeps++
		hookMu.Unlock()
		go func() {
			defer close(innerSweepDone)
			sweepErr := reconciler.Sweep(ctx)
			hookMu.Lock()
			innerSweepErr = sweepErr
			hookMu.Unlock()
		}()
		select {
		case <-innerSweepDone:
			hookMu.Lock()
			innerSweepReturned = true
			hookMu.Unlock()
		case <-time.After(5 * time.Second):
		}
	}
	h.mu.Unlock()
	t.Cleanup(func() {
		h.mu.Lock()
		delete(h.onFetch, gonePath)
		h.mu.Unlock()
	})

	require.NoError(t, reconciler.Sweep(ctx))
	select {
	case <-innerSweepDone:
	case <-time.After(10 * time.Second):
		t.Fatal("the overlapping Sweep did not return within 10s of the pass")
	}

	hookMu.Lock()
	require.Equal(t, 1, innerSweeps, "a second Sweep ran during the pass's fetch")
	assert.True(t, innerSweepReturned, "the overlapping Sweep returns while the pass's fetch is still open: no second pass")
	assert.NoError(t, innerSweepErr, "the overlapping Sweep returns nil")
	hookMu.Unlock()
	assert.Equal(t, hitsBefore+1, h.hitCount(gonePath), "the gone post is fetched once: no second pass ran")
	assert.Equal(t, materialize.RemovalCodeModeratorDiscretion, removalCode(t, h, world.communityDID, goneID),
		"the gone post gets C's removal record")
	assert.Equal(t, commitsBefore+1, firehoseCommitCount(t, h, world.communityDID),
		"one commit to C's repo: the removal record")
	_, revAfter, err := h.manager.Head(ctx, world.communityDID)
	require.NoError(t, err)
	assert.NotEqual(t, revBefore, revAfter, "C's repo rev advances for the removal")
	assert.Equal(t, []reconcilePass{{Community: world.communityDID, Complete: true}},
		reconcilePasses(t, logs), "one pass, complete")
	assert.Equal(t, removedBefore+1, reconcileCounter(t, "removed"), "one removal outcome")
	assert.Equal(t, passesCompletedBefore+1, reconcileCounter(t, "passes_completed"), "one completed pass")
	assert.False(t, reconcilePendingAt(t, h, world.communityDID).Valid, "the pass clears C's reconcile")
}

// TestCommunityReconcile_ACommunityNotFollowedAgainIsReconciled: C is
// unsubscribed before its removal, so the negation does not follow it again.
// Its reconcile is still pending and ready: one Sweep deletes the kept post
// its origin answers 404 for and clears the reconcile.
func TestCommunityReconcile_ACommunityNotFollowedAgainIsReconciled(t *testing.T) {
	world := newReconcileWorld(t)
	h := world.h
	ctx := context.Background()

	keptID := world.postBy(ubUserID, world.communityC, "rc-guard-unfollowed", ubBeforeBan, false)
	keptPath := urlPath(t, keptID)
	h.serveStatus(keptPath, http.StatusNotFound)

	rec := h.adminRequest(http.MethodDelete, "/admin/communities", map[string]any{"community": groupID})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, store.FollowStateNone, h.followStateOf(groupID), "precondition: C is unsubscribed")
	assertLive(t, h, keptID, "precondition: the unsubscribe keeps C's post")
	world.committer.removeCommunity(t, world.communityDID)
	world.committer.restoreCommunity(t, world.communityDID)
	h.newTestFollowRetrier(h.client).Sweep(ctx)
	require.Equal(t, store.FollowStateNone, h.followStateOf(groupID), "precondition: the negation does not follow C again")
	hitsBefore := h.hitCount(keptPath)
	reconciler, logs := world.newReconciler(time.Hour)

	require.NoError(t, reconciler.Sweep(ctx))

	assert.Equal(t, hitsBefore+1, h.hitCount(keptPath), "the kept post is fetched once")
	assertDeleted(t, h, keptID, "the origin answers 404 for the kept post of a community not followed again")
	assert.False(t, reconcilePendingAt(t, h, world.communityDID).Valid, "the pass clears C's reconcile")
	assert.Equal(t, []reconcilePass{{Community: world.communityDID, Complete: true}},
		reconcilePasses(t, logs), "one pass, complete")
}

// TestCommunityReconcile_ARemovalDuringTheLastFetchWritesNothing: C is removed
// again while the pass fetches its only kept post, which its origin answers
// 404. The pass applies nothing for that answer (the post keeps its record),
// logs an incomplete summary, and leaves C's reconcile pending.
func TestCommunityReconcile_ARemovalDuringTheLastFetchWritesNothing(t *testing.T) {
	world := newReconcileWorld(t)
	h := world.h
	ctx := context.Background()

	keptID := world.postBy(ubUserID, world.communityC, "rc-guard-last", ubBeforeBan, false)
	keptPath := urlPath(t, keptID)
	h.serveStatus(keptPath, http.StatusNotFound)
	world.restoreAndReady()
	require.True(t, reconcilePendingAt(t, h, world.communityDID).Valid, "precondition: C's reconcile is pending")
	reconciler, logs := world.newReconciler(time.Hour)
	hitsBefore := h.hitCount(keptPath)

	var (
		hookMu     sync.Mutex
		hookRan    bool
		removalErr error
	)
	h.mu.Lock()
	h.onFetch[keptPath] = func() {
		hookMu.Lock()
		defer hookMu.Unlock()
		if hookRan {
			return
		}
		hookRan = true
		removalErr = world.committer.attempt(t, world.communityDID, communityTakedown, false)
	}
	h.mu.Unlock()
	t.Cleanup(func() {
		h.mu.Lock()
		delete(h.onFetch, keptPath)
		h.mu.Unlock()
	})

	require.NoError(t, reconciler.Sweep(ctx))

	hookMu.Lock()
	require.True(t, hookRan, "the kept post was fetched")
	require.NoError(t, removalErr, "the removal committed during the fetch")
	hookMu.Unlock()
	assert.Equal(t, hitsBefore+1, h.hitCount(keptPath), "the kept post is fetched once")
	assertLive(t, h, keptID, "C is removed again: the 404 answer deletes nothing")
	assert.Equal(t, []reconcilePass{{Community: world.communityDID, Complete: false}},
		reconcilePasses(t, logs), "the stopped pass logs one incomplete summary")
	assert.True(t, reconcilePendingAt(t, h, world.communityDID).Valid,
		"the stopped pass leaves C's reconcile pending")
}

// TestCommunityReconcile_ARemovedCommunitysMarkerIsNotCleared: the marker
// clear is refused while C is removed again, so a removal that commits after
// a pass's last apply leaves C's reconcile pending.
func TestCommunityReconcile_ARemovedCommunitysMarkerIsNotCleared(t *testing.T) {
	world := newReconcileWorld(t)
	h := world.h
	ctx := context.Background()

	world.restoreAndReady()
	pending, err := store.ReadyCommunityReconcile(ctx, h.db, world.communityDID)
	require.NoError(t, err)
	require.NotNil(t, pending, "precondition: C's reconcile is pending and ready")
	require.NoError(t, world.committer.attempt(t, world.communityDID, communityTakedown, false))

	cleared, err := store.ClearCommunityReconcile(ctx, h.db, world.communityDID, pending.PendingAt)

	require.NoError(t, err)
	assert.False(t, cleared, "a removed community's marker is not cleared")
	assert.True(t, reconcilePendingAt(t, h, world.communityDID).Valid, "C's reconcile stays pending")
}
