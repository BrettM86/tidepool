package ingest

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// BRIDGED COMMUNITY RESTORE RECONCILE: WIRING (B6).
//
// A restored community's reconcile waits for the re-follow: no pass runs while
// the follow is pending, or while an accepted follow's backfill has not
// completed a clean walk. The clean walk's completion starts the pass
// (Backfill OnComplete → TriggerAsync). A community whose removal was never
// negated has nothing pending, so its objects are never fetched.

// technologyOutboxPath is C's outbox, as group_lemmy_world.json advertises it.
const technologyOutboxPath = "/c/technology/outbox"

// serveEmptyTechnologyOutbox answers C's outbox with an empty collection, so
// a backfill walk of C completes cleanly without materializing anything.
func (h *harness) serveEmptyTechnologyOutbox() {
	h.t.Helper()
	h.serveObject(technologyOutboxPath, map[string]any{
		"type":         "OrderedCollection",
		"id":           groupID + "/outbox",
		"totalItems":   0,
		"orderedItems": []any{},
	})
}

// restoreAndFollowAgain removes C and negates the removal through the label
// store, then lets the follow retrier send the owed Follow; C has not
// accepted it yet.
func (w *reconcileWorld) restoreAndFollowAgain() {
	t := w.h.t
	t.Helper()
	w.committer.removeCommunity(t, w.communityDID)
	require.Equal(t, store.FollowStateNone, w.h.followStateOf(groupID), "precondition: the label removes C")
	w.committer.restoreCommunity(t, w.communityDID)
	w.h.newTestFollowRetrier(w.h.client).Sweep(context.Background())
	require.Equal(t, store.FollowStatePending, w.h.followStateOf(groupID), "precondition: C's Follow is pending")
}

// technologyCommunity reads C's current row.
func (h *harness) technologyCommunity() *store.Community {
	h.t.Helper()
	community, err := h.communities.GetByAPGroupID(context.Background(), groupID)
	require.NoError(h.t, err)
	return community
}

// TestCommunityReconcile_BackfillCompletionStartsThePass (B6): while C's
// re-follow is pending a Sweep fetches nothing; after the Accept, a real
// backfill walk whose OnComplete is the reconciler's TriggerAsync reconciles
// the kept post with no explicit Sweep.
func TestCommunityReconcile_BackfillCompletionStartsThePass(t *testing.T) {
	world := newReconcileWorld(t)
	h := world.h
	ctx := context.Background()

	keptID := world.postBy(ubUserID, world.communityC, "rc-b6a-kept", ubBeforeBan, false)
	keptPath := "/post/rc-b6a-kept"
	h.serveStatus(keptPath, http.StatusNotFound)
	h.serveEmptyTechnologyOutbox()
	world.restoreAndFollowAgain()
	reconciler, _ := world.newReconciler(time.Hour)
	hitsBefore := h.hitCount(keptPath)

	require.NoError(t, reconciler.Sweep(ctx))

	assert.Equal(t, hitsBefore, h.hitCount(keptPath), "C's follow is pending: the Sweep does not fetch its kept post")
	assertLive(t, h, keptID, "no pass runs before the Accept")

	h.acceptLatestFollow(world.communityC)
	require.Equal(t, store.FollowStateAccepted, h.followStateOf(groupID), "precondition: C accepted the Follow")
	options := h.backfillOptions(h.mat, 10)
	options.OnComplete = reconciler.TriggerAsync
	backfill, err := NewBackfill(options)
	require.NoError(t, err)

	require.NoError(t, backfill.Run(ctx, h.technologyCommunity(), true))
	reconciler.Wait()

	require.NotNil(t, h.technologyCommunity().LastBackfillAt, "precondition: the walk was clean and stamped C")
	assert.Equal(t, hitsBefore+1, h.hitCount(keptPath), "the backfill's completion fetches the kept post once")
	assertDeleted(t, h, keptID, "the pass the backfill started applies the origin's 404")
}

// TestCommunityReconcile_FailedBackfillWalkHoldsThePass (B6): an accepted C
// whose backfill walk fails (the outbox answers 503, so C is not stamped) is
// not reconciled, by the walk's completion or by a Sweep; once a later clean
// walk stamps C, a Sweep reconciles the kept post.
func TestCommunityReconcile_FailedBackfillWalkHoldsThePass(t *testing.T) {
	world := newReconcileWorld(t)
	h := world.h
	ctx := context.Background()

	keptID := world.postBy(ubUserID, world.communityC, "rc-b6b-kept", ubBeforeBan, false)
	keptPath := "/post/rc-b6b-kept"
	h.serveStatus(keptPath, http.StatusNotFound)
	h.serveEmptyTechnologyOutbox()
	h.serveStatus(technologyOutboxPath, http.StatusServiceUnavailable)
	world.restoreAndFollowAgain()
	h.acceptLatestFollow(world.communityC)
	require.Equal(t, store.FollowStateAccepted, h.followStateOf(groupID), "precondition: C accepted the Follow")
	reconciler, _ := world.newReconciler(time.Hour)
	hitsBefore := h.hitCount(keptPath)
	failingOptions := h.backfillOptions(h.mat, 10)
	failingOptions.OnComplete = reconciler.TriggerAsync
	failingBackfill, err := NewBackfill(failingOptions)
	require.NoError(t, err)

	require.Error(t, failingBackfill.Run(ctx, h.technologyCommunity(), true), "precondition: the outbox answers 503")
	reconciler.Wait()
	require.Nil(t, h.technologyCommunity().LastBackfillAt, "precondition: the failed walk left C unstamped")
	require.NoError(t, reconciler.Sweep(ctx))

	assert.Equal(t, hitsBefore, h.hitCount(keptPath),
		"C's backfill has not completed: neither the failed walk nor the Sweep fetches its kept post")
	assertLive(t, h, keptID, "no pass runs before a clean walk")

	h.serveStatus(technologyOutboxPath, 0)
	require.NoError(t, newBackfill(t, h, 10).Run(ctx, h.technologyCommunity(), true))
	require.NotNil(t, h.technologyCommunity().LastBackfillAt, "precondition: the later walk was clean and stamped C")

	require.NoError(t, reconciler.Sweep(ctx))

	assert.Equal(t, hitsBefore+1, h.hitCount(keptPath), "once C is stamped the Sweep fetches the kept post")
	assertDeleted(t, h, keptID, "the Sweep applies the origin's 404")
}

// TestCommunityReconcile_NeverNegatedCommunityIsNotFetched (B6): D, followed
// and backfilled but never labelled, has no pending reconcile: a Sweep that
// reconciles restored C never fetches D's kept post.
func TestCommunityReconcile_NeverNegatedCommunityIsNotFetched(t *testing.T) {
	world := newReconcileWorld(t)
	h := world.h
	ctx := context.Background()

	restoredID := world.postBy(ubUserID, world.communityC, "rc-b6c-restored", ubBeforeBan, false)
	untouchedID := world.postBy(ubUserID, world.communityD, "rc-b6c-untouched", ubBeforeBan, false)
	restoredPath, untouchedPath := "/post/rc-b6c-restored", "/post/rc-b6c-untouched"
	h.serveStatus(restoredPath, http.StatusNotFound)
	h.serveStatus(untouchedPath, http.StatusNotFound)
	require.Equal(t, store.FollowStateAccepted, h.followStateOf(ubCommunityDGroupID), "precondition: D is followed")
	require.NoError(t, h.communities.SetLastBackfill(ctx, ubCommunityDGroupID, time.Now()))
	untouchedCID := currentRecordCID(t, h, untouchedID)
	world.restoreAndReady()
	reconciler, _ := world.newReconciler(time.Hour)
	hitsBefore := h.hitCounts(restoredPath, untouchedPath)

	require.NoError(t, reconciler.Sweep(ctx))

	assert.Equal(t, hitsBefore[restoredPath]+1, h.hitCount(restoredPath), "restored C's kept post is fetched")
	assertDeleted(t, h, restoredID, "restored C's kept post gets the origin's 404")
	assert.Equal(t, hitsBefore[untouchedPath], h.hitCount(untouchedPath), "D was never negated: its kept post is not fetched")
	assertLive(t, h, untouchedID, "D's kept post is not reconciled")
	assert.Equal(t, untouchedCID, currentRecordCID(t, h, untouchedID), "D's kept post is unchanged")
}
