package ingest

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/materialize"
)

// BRIDGED COMMUNITY RESTORE RECONCILE: A MODERATION LANDING DURING THE FETCH.
//
// The origin's answer is applied only to the removal record it was fetched
// under. A removal or a restore of the object that C's moderators send while
// the fetch is open leaves the answer unapplied, and C's reconcile pending, so
// a later pass fetches again.

// afterFetchFetcher is the reconcile's view of the origins, running action
// once after the fetch of apID has its answer and before the pass applies it:
// a moderation activity that lands while the fetch is open.
type afterFetchFetcher struct {
	OriginFetcher
	apID   string
	mu     sync.Mutex
	action func()
}

func (f *afterFetchFetcher) FetchObjectSameAuthority(ctx context.Context, iri string) (*ap.Object, error) {
	obj, err := f.OriginFetcher.FetchObjectSameAuthority(ctx, iri)
	var action func()
	if iri == f.apID {
		f.mu.Lock()
		action, f.action = f.action, nil
		f.mu.Unlock()
	}
	if action != nil {
		action()
	}
	return obj, err
}

// ran reports whether action ran.
func (f *afterFetchFetcher) ran() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.action == nil
}

// newRacingReconciler is newReconciler with fetcher in place of the harness
// client.
func (w *reconcileWorld) newRacingReconciler(fetcher OriginFetcher) (*CommunityReconciler, *syncBuffer) {
	t := w.h.t
	t.Helper()
	logs := &syncBuffer{}
	reconciler, err := NewCommunityReconciler(CommunityReconcilerOptions{
		DB:           w.h.db,
		Fetcher:      fetcher,
		Materializer: w.h.mat,
		Interval:     time.Hour,
		Logger:       slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	require.NoError(t, err)
	return reconciler, logs
}

// TestCommunityReconcile_ARemovalDuringALiveFetchIsNotRestored: C's origin
// serves a kept comment live, and C's moderators remove it while the pass
// fetches it. The removal stands, and C's reconcile stays pending.
func TestCommunityReconcile_ARemovalDuringALiveFetchIsNotRestored(t *testing.T) {
	world := newReconcileWorld(t)
	h := world.h
	rootID := world.postBy(ubOtherUserID, world.communityC, "rc-race-live-root", ubBeforeBan, false)
	commentID := world.commentBy(rcWorldUserID, world.communityC, "rc-race-live", rootID, ubBeforeBan2)
	require.Empty(t, removalCode(t, h, world.communityDID, commentID), "precondition: no removal record")
	world.restoreAndReady()
	fetcher := &afterFetchFetcher{OriginFetcher: h.client, apID: commentID, action: func() {
		world.removeCommentInC("rc-race-live", commentID)
	}}
	reconciler, logs := world.newRacingReconciler(fetcher)

	require.NoError(t, reconciler.Sweep(context.Background()))

	require.True(t, fetcher.ran(), "the kept comment was fetched")
	assert.Equal(t, materialize.RemovalCodeModeratorDiscretion, removalCode(t, h, world.communityDID, commentID),
		"the removal sent during the fetch stands")
	assert.Equal(t, []reconcilePass{{Community: world.communityDID, Complete: false}}, reconcilePasses(t, logs))
	assert.True(t, reconcilePendingAt(t, h, world.communityDID).Valid, "C's reconcile stays pending")
}

// TestCommunityReconcile_ARestoreDuringAGoneFetchIsNotRemovedAgain: C's
// origin answers 410 for a kept comment under a removal record, and C's
// moderators restore the comment while the pass fetches it. The pass writes
// no removal record over the restore, and C's reconcile stays pending.
func TestCommunityReconcile_ARestoreDuringAGoneFetchIsNotRemovedAgain(t *testing.T) {
	world := newReconcileWorld(t)
	h := world.h
	rootID := world.postBy(ubOtherUserID, world.communityC, "rc-race-gone-root", ubBeforeBan, false)
	commentID := world.commentBy(rcWorldUserID, world.communityC, "rc-race-gone", rootID, ubBeforeBan2)
	world.removeCommentInC("rc-race-gone", commentID)
	h.mu.Lock()
	liveBody := h.served["/comment/rc-race-gone"]
	h.mu.Unlock()
	h.serveTombstone("/comment/rc-race-gone", commentID, http.StatusGone)
	world.restoreAndReady()
	fetcher := &afterFetchFetcher{OriginFetcher: h.client, apID: commentID, action: func() {
		// The fetch holds the 410; the origin serves the restored comment
		// from now on.
		h.serveJSON("/comment/rc-race-gone", liveBody)
		h.serveStatus("/comment/rc-race-gone", 0)
		h.announceUndoDelete(world.communityC, world.communityC.id+"/activities/announce/undo/rc-race-gone",
			"https://lemmy.world/activities/delete/rc-race-gone", commentID, ptr(rwSummary))
		require.Empty(t, removalCode(t, h, world.communityDID, commentID), "precondition: the restore lifted the removal")
	}}
	reconciler, logs := world.newRacingReconciler(fetcher)

	require.NoError(t, reconciler.Sweep(context.Background()))

	require.True(t, fetcher.ran(), "the kept comment was fetched")
	assert.Empty(t, removalCode(t, h, world.communityDID, commentID),
		"the restore sent during the fetch is not undone by the stale gone answer")
	assert.Equal(t, []reconcilePass{{Community: world.communityDID, Complete: false}}, reconcilePasses(t, logs))
	assert.True(t, reconcilePendingAt(t, h, world.communityDID).Valid, "C's reconcile stays pending")
}
