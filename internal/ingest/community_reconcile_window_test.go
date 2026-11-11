package ingest

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/materialize"
)

// BRIDGED COMMUNITY RESTORE RECONCILE: THE DEAD-HOST WINDOW AND THE ORIGIN'S
// AUTHORITY.
//
// A kept object whose origin keeps failing to answer holds the pending
// reconcile only for DeadHostAfter from the pass's first attempt; after that
// a pass counts it unreachable, changes nothing for it, and clears the
// marker. A failed apply is not a fetch failure: it still holds the marker.
// A live answer restores an object under a standing removal record only when
// the object is on the community's own instance: another instance is
// authoritative for its own deletions, not for the community's removals.

// unreachableLogs returns the ap_ids of every Warn record of community in a
// reconciler's JSON logs that carries ap_ids.
func unreachableLogs(t *testing.T, logs *syncBuffer, community string) []string {
	t.Helper()
	var apIDs []string
	scanner := bufio.NewScanner(strings.NewReader(logs.String()))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		var record struct {
			Level     string   `json:"level"`
			Community string   `json:"community"`
			APIDs     []string `json:"ap_ids"`
		}
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &record), "log line %q", scanner.Text())
		if record.Level == "WARN" && record.Community == community {
			apIDs = append(apIDs, record.APIDs...)
		}
	}
	require.NoError(t, scanner.Err())
	return apIDs
}

// TestCommunityReconcile_DeadHostWindow (I1).
func TestCommunityReconcile_DeadHostWindow(t *testing.T) {
	// a: past the window, a kept object still answering 503 is counted
	// unreachable and left as it is; the other objects' answers apply in the
	// same pass and the marker clears.
	t.Run("a: past the window a fetch retry is unreachable and the marker clears", func(t *testing.T) {
		world := newReconcileWorld(t)
		h := world.h
		ctx := context.Background()

		rootID := world.postBy(ubOtherUserID, world.communityC, "rc-w1a-root", ubBeforeBan, false)
		flakyID := world.commentBy(ubUserID, world.communityC, "rc-w1a-flaky", rootID, ubBeforeBan2)
		goneID := world.commentBy(ubUserID, world.communityC, "rc-w1a-gone", rootID, ubBeforeBan2)
		missingID := world.commentBy(ubUserID, world.communityC, "rc-w1a-missing", rootID, ubBeforeBan2)
		flakyCID := currentRecordCID(t, h, flakyID)
		paths := []string{"/post/rc-w1a-root", "/comment/rc-w1a-flaky", "/comment/rc-w1a-gone", "/comment/rc-w1a-missing"}
		h.serveStatus("/comment/rc-w1a-flaky", http.StatusServiceUnavailable)
		h.serveGone("/comment/rc-w1a-gone")
		h.serveStatus("/comment/rc-w1a-missing", http.StatusNotFound)
		world.restoreAndReady()
		world.backdateFirstAttempt(DeadHostAfter + time.Hour)
		countersBefore := reconcileCounters(t)
		reconciler, logs := world.newReconciler(time.Hour)

		require.NoError(t, reconciler.Sweep(ctx))

		assertLive(t, h, flakyID, "an unreachable object is not deleted")
		assert.Equal(t, flakyCID, currentRecordCID(t, h, flakyID), "an unreachable object's record is unchanged")
		assert.Empty(t, removalCode(t, h, world.communityDID, flakyID), "an unreachable object gets no removal record")
		assert.Equal(t, materialize.RemovalCodeModeratorDiscretion, removalCode(t, h, world.communityDID, goneID),
			"the gone comment's removal applies in the same pass")
		assertDeleted(t, h, missingID, "the missing comment's delete applies in the same pass")
		assert.Contains(t, unreachableLogs(t, logs, world.communityDID), flakyID,
			"a Warn record names the unreachable object")
		pendingAt, firstAttemptAt := world.reconcileMarkers()
		assert.False(t, pendingAt.Valid, "the pass clears the pending reconcile")
		assert.False(t, firstAttemptAt.Valid, "the clear resets the first attempt")
		assert.Equal(t, []reconcilePass{{Community: world.communityDID, Complete: true}}, reconcilePasses(t, logs))
		assert.Equal(t, map[string]int64{
			"live": 1, "restored": 0, "deleted": 1, "removed": 1, "retry": 0, "unreachable": 1, "passes_completed": 1,
		}, reconcileCounterDeltas(t, countersBefore), "outcome counters of the pass")
		hitsAfter := h.hitCounts(paths...)

		require.NoError(t, reconciler.Sweep(ctx))

		assert.Equal(t, hitsAfter, h.hitCounts(paths...), "the cleared reconcile: a further Sweep fetches nothing")
	})

	// b: the first pass stamps the first attempt; a later pass keeps it; the
	// window counts from it.
	t.Run("b: the window counts from the first pass's attempt", func(t *testing.T) {
		world := newReconcileWorld(t)
		h := world.h
		ctx := context.Background()

		rootID := world.postBy(ubOtherUserID, world.communityC, "rc-w1b-root", ubBeforeBan, false)
		flakyID := world.commentBy(ubUserID, world.communityC, "rc-w1b-flaky", rootID, ubBeforeBan2)
		flakyPath := "/comment/rc-w1b-flaky"
		h.serveStatus(flakyPath, http.StatusServiceUnavailable)
		world.restoreAndReady()
		_, unstamped := world.reconcileMarkers()
		require.False(t, unstamped.Valid, "precondition: no pass has attempted the reconcile yet")
		countersBefore := reconcileCounters(t)
		reconciler, logs := world.newReconciler(time.Hour)

		require.NoError(t, reconciler.Sweep(ctx))

		pendingAt, firstAttemptAt := world.reconcileMarkers()
		require.True(t, pendingAt.Valid, "inside the window a fetch retry keeps the reconcile pending")
		require.True(t, firstAttemptAt.Valid, "the first pass stamps its attempt")
		assert.WithinDuration(t, time.Now(), firstAttemptAt.Time, time.Minute)

		require.NoError(t, reconciler.Sweep(ctx))

		_, secondAttemptAt := world.reconcileMarkers()
		require.True(t, secondAttemptAt.Valid)
		assert.True(t, firstAttemptAt.Time.Equal(secondAttemptAt.Time), "a later pass keeps the first attempt")
		assert.Empty(t, unreachableLogs(t, logs, world.communityDID), "inside the window nothing is unreachable")
		world.backdateFirstAttempt(DeadHostAfter + time.Minute)
		hitsBefore := h.hitCount(flakyPath)

		require.NoError(t, reconciler.Sweep(ctx))

		assert.Equal(t, hitsBefore+1, h.hitCount(flakyPath), "the pass past the window fetches the object once more")
		assertLive(t, h, flakyID, "an unreachable object is not deleted")
		pendingAt, _ = world.reconcileMarkers()
		assert.False(t, pendingAt.Valid, "past the window the pass clears the pending reconcile")
		assert.Equal(t, []string{flakyID}, unreachableLogs(t, logs, world.communityDID))
		assert.Equal(t, map[string]int64{
			"live": 3, "restored": 0, "deleted": 0, "removed": 0, "retry": 2, "unreachable": 1, "passes_completed": 1,
		}, reconcileCounterDeltas(t, countersBefore), "two passes in the window, one past it")
	})

	// c: a failed apply is not a fetch failure: past the window it still keeps
	// the reconcile pending, and the next pass applies it.
	t.Run("c: a failed apply past the window keeps the marker", func(t *testing.T) {
		world := newReconcileWorld(t)
		h := world.h
		ctx := context.Background()

		keptID := world.postBy(ubUserID, world.communityC, "rc-w1c-kept", ubBeforeBan, false)
		flakyID := world.commentBy(ubUserID, world.communityC, "rc-w1c-flaky", keptID, ubBeforeBan2)
		keptPath, flakyPath := "/post/rc-w1c-kept", "/comment/rc-w1c-flaky"
		h.serveStatus(keptPath, http.StatusNotFound)
		h.serveStatus(flakyPath, http.StatusServiceUnavailable)
		world.restoreAndReady()
		world.backdateFirstAttempt(DeadHostAfter + time.Hour)
		countersBefore := reconcileCounters(t)
		reconciler, logs := world.newReconciler(time.Hour)
		drop := failMappingDeletes(t, h)

		require.NoError(t, reconciler.Sweep(ctx))

		pendingAt, _ := world.reconcileMarkers()
		assert.True(t, pendingAt.Valid, "a failed apply keeps the reconcile pending past the window")
		assert.Empty(t, unreachableLogs(t, logs, world.communityDID), "a pass that keeps the marker counts nothing unreachable")
		assert.Equal(t, int64(0), reconcileCounterDeltas(t, countersBefore)["unreachable"])
		drop()
		hitsBefore := h.hitCounts(keptPath, flakyPath)

		require.NoError(t, reconciler.Sweep(ctx))

		assert.Equal(t, hitsBefore[keptPath]+1, h.hitCount(keptPath), "the kept post is fetched again")
		assertDeleted(t, h, keptID, "the next pass applies the delete")
		assertLive(t, h, flakyID, "an unreachable object is not deleted")
		pendingAt, _ = world.reconcileMarkers()
		assert.False(t, pendingAt.Valid, "with the apply done the pass clears the pending reconcile")
		assert.Equal(t, []string{flakyID}, unreachableLogs(t, logs, world.communityDID))
	})
}

// TestCommunityReconcile_NewNegationResetsTheFirstAttempt (I1): a new
// negation sets a newer marker and resets the first attempt, so the window
// starts again for the new reconcile.
func TestCommunityReconcile_NewNegationResetsTheFirstAttempt(t *testing.T) {
	world := newReconcileWorld(t)
	h := world.h
	ctx := context.Background()

	rootID := world.postBy(ubOtherUserID, world.communityC, "rc-w2-root", ubBeforeBan, false)
	world.commentBy(ubUserID, world.communityC, "rc-w2-flaky", rootID, ubBeforeBan2)
	h.serveStatus("/comment/rc-w2-flaky", http.StatusServiceUnavailable)
	world.restoreAndReady()
	reconciler, _ := world.newReconciler(time.Hour)
	require.NoError(t, reconciler.Sweep(ctx))
	firstPendingAt, firstAttemptAt := world.reconcileMarkers()
	require.True(t, firstPendingAt.Valid, "precondition: the fetch retry keeps the reconcile pending")
	require.True(t, firstAttemptAt.Valid, "precondition: the pass stamped its attempt")

	world.committer.removeCommunity(t, world.communityDID)
	world.committer.restoreCommunity(t, world.communityDID)

	pendingAt, resetAttemptAt := world.reconcileMarkers()
	require.True(t, pendingAt.Valid, "the new negation records the reconcile pending")
	assert.True(t, pendingAt.Time.After(firstPendingAt.Time), "the new negation sets a newer marker")
	assert.False(t, resetAttemptAt.Valid, "the new negation resets the first attempt")
}

// TestCommunityReconcile_LiveRestoresOnlyOnTheCommunitysInstance (I2): a
// live answer for a kept comment under C's standing removal record restores
// it only when the comment is on C's own instance (lemmy.world). A comment by
// a programming.dev user, served live by programming.dev, keeps C's removal
// record and its withhold, and is counted live.
func TestCommunityReconcile_LiveRestoresOnlyOnTheCommunitysInstance(t *testing.T) {
	world := newReconcileWorld(t)
	h := world.h
	ctx := context.Background()

	rootID := world.postBy(ubOtherUserID, world.communityC, "rc-i2-root", ubBeforeBan, false)
	crossID, crossImage := world.imageComment(world.communityC, "rc-i2-cross", rootID, ubBeforeBan2, "rc-i2-cross-image")
	sameID, sameImage := world.imageCommentBy(rcWorldUserID, world.communityC, "rc-i2-same", rootID, ubBeforeBan2, "rc-i2-same-image")
	crossURI, sameURI := atURIOf(t, h, crossID), atURIOf(t, h, sameID)
	world.removeCommentInC("rc-i2-cross", crossID)
	world.removeCommentInC("rc-i2-same", sameID)
	assertOneRemovalWithhold(t, h, crossImage, crossURI, ubUserDID)
	assertOneRemovalWithhold(t, h, sameImage, sameURI, rcWorldUserDID)
	world.restoreAndReady()
	countersBefore := reconcileCounters(t)
	reconciler, _ := world.newReconciler(time.Hour)

	require.NoError(t, reconciler.Sweep(ctx))

	assert.Equal(t, materialize.RemovalCodeModeratorDiscretion, removalCode(t, h, world.communityDID, crossID),
		"another instance serving its comment live does not lift C's removal")
	assertOneRemovalWithhold(t, h, crossImage, crossURI, ubUserDID)
	assertBlobRefused(t, h, ubUserDID, crossImage, "C's removal still withholds the cross-host comment's image")
	assert.Empty(t, removalCode(t, h, world.communityDID, sameID),
		"C's own instance serving the comment live restores it")
	assert.Empty(t, activeRemovalWithholds(t, h, sameImage), "the restore lifts the same-host comment's withhold")
	assertLive(t, h, crossID, "the cross-host comment keeps its record")
	assertLive(t, h, sameID, "the restored comment keeps its record")
	assert.Equal(t, map[string]int64{
		// live: the root and the cross-host comment.
		"live": 2, "restored": 1, "deleted": 0, "removed": 0, "retry": 0, "unreachable": 0, "passes_completed": 1,
	}, reconcileCounterDeltas(t, countersBefore))
}
