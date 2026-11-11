package ingest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/blobwithhold"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// BRIDGED COMMUNITY RESTORE RECONCILE.
//
// The negation of a community `!takedown` that cleared an active removal
// leaves the community's reconcile pending. Once the community is ready, a
// pass fetches every kept object (a live fediverse post or comment bound to
// the community) from its origin and applies what the origin answers: live
// under a standing removal record restores it, 404 deletes it, 410 or a
// Tombstone writes a moderator-discretion removal unless one stands, and a
// failed fetch changes nothing and leaves the reconcile pending.

// TestClassifyOrigin_MapsTheOriginsAnswer (B1): each answer of an origin to
// FetchObjectSameAuthority maps to one OriginState. Live needs the requested
// id and a content type ingest accepts for the collection; Lemmy's 400
// not-found counts only with the collection's own code.
func TestClassifyOrigin_MapsTheOriginsAnswer(t *testing.T) {
	mux := http.NewServeMux()
	origin := httptest.NewServer(mux)
	t.Cleanup(origin.Close)
	answer := func(path string, status int, contentType, body string) {
		mux.HandleFunc("GET "+path, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", contentType)
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
		})
	}
	answerJSON := func(path string, status int, body string) {
		answer(path, status, ap.ContentTypeActivityJSON, body)
	}
	answerJSON("/post/live", http.StatusOK, `{"type":"Page","id":"`+origin.URL+`/post/live",`+
		`"attributedTo":"`+origin.URL+`/u/author","name":"a live post","to":["`+ap.PublicAudience+`"]}`)
	answerJSON("/comment/live", http.StatusOK, `{"type":"Note","id":"`+origin.URL+`/comment/live",`+
		`"attributedTo":"`+origin.URL+`/u/author","content":"a live comment","to":["`+ap.PublicAudience+`"]}`)
	answerJSON("/post/missing", http.StatusNotFound, `{"error":"couldnt_find_object"}`)
	answerJSON("/post/gone", http.StatusGone, `{"id":"`+origin.URL+`/post/gone","type":"Tombstone"}`)
	answerJSON("/post/tombstone", http.StatusOK, `{"id":"`+origin.URL+`/post/tombstone","type":"Tombstone"}`)
	answerJSON("/post/other-tombstone", http.StatusOK, `{"id":"`+origin.URL+`/post/other","type":"Tombstone"}`)
	answerJSON("/post/forbidden", http.StatusForbidden, `{"error":"forbidden"}`)
	answerJSON("/post/purged", http.StatusBadRequest, `{"error":"couldnt_find_post"}`)
	answerJSON("/comment/purged", http.StatusBadRequest, `{"error":"couldnt_find_comment"}`)
	answerJSON("/post/rate-limited", http.StatusBadRequest, `{"error":"rate_limit_error"}`)
	answerJSON("/post/empty", http.StatusOK, `{}`)
	answerJSON("/post/error-body", http.StatusOK, `{"error":"unavailable"}`)
	answerJSON("/post/mismatched", http.StatusOK, `{"type":"Page","id":"`+origin.URL+`/post/other",`+
		`"attributedTo":"`+origin.URL+`/u/author","name":"another post","to":["`+ap.PublicAudience+`"]}`)
	answerJSON("/post/person", http.StatusOK, `{"type":"Person","id":"`+origin.URL+`/post/person"}`)
	answerJSON("/post/person-missing", http.StatusBadRequest, `{"error":"couldnt_find_person"}`)
	answerJSON("/post/comment-missing", http.StatusBadRequest, `{"error":"couldnt_find_comment"}`)
	answerJSON("/comment/post-missing", http.StatusBadRequest, `{"error":"couldnt_find_post"}`)
	answer("/post/html-400", http.StatusBadRequest, "text/html", `<html><body>Bad Request</body></html>`)

	closed := httptest.NewServer(http.NotFoundHandler())
	closedURL := closed.URL
	closed.Close()

	client := ap.NewClient(ap.ClientOptions{
		Signer:                testServiceActor(t).Signer(),
		AllowPrivateAddresses: true,
		PerHostRPS:            100000,
		PerHostBurst:          100000,
		MaxAttempts:           1,
	})
	post, comment := materialize.CollectionPostV2, materialize.CollectionComment
	for _, tc := range []struct {
		name       string
		collection string
		iri        string
		want       OriginState
	}{
		{name: "200 Page", collection: post, iri: origin.URL + "/post/live", want: OriginLive},
		{name: "200 Note for a comment", collection: comment, iri: origin.URL + "/comment/live", want: OriginLive},
		{name: "200 Page for a comment (wrong type)", collection: comment, iri: origin.URL + "/post/live", want: OriginRetry},
		{name: "200 Note for a post (wrong type)", collection: post, iri: origin.URL + "/comment/live", want: OriginRetry},
		{name: "404", collection: post, iri: origin.URL + "/post/missing", want: OriginGone404},
		{name: "410", collection: post, iri: origin.URL + "/post/gone", want: OriginGone410},
		{name: "200 Tombstone body", collection: post, iri: origin.URL + "/post/tombstone", want: OriginGone410},
		// The client reports a 200 Tombstone as IsTombstoned without its body,
		// so its id goes unchecked, as for a 410; see ClassifyOrigin.
		{name: "200 Tombstone with another id", collection: post, iri: origin.URL + "/post/other-tombstone", want: OriginGone410},
		{name: "transport error (closed server)", collection: post, iri: closedURL + "/post/closed", want: OriginRetry},
		{name: "403", collection: post, iri: origin.URL + "/post/forbidden", want: OriginRetry},
		{name: "400 couldnt_find_post (Lemmy purged post)", collection: post, iri: origin.URL + "/post/purged", want: OriginGone404},
		{name: "400 couldnt_find_comment (Lemmy purged comment)", collection: comment, iri: origin.URL + "/comment/purged", want: OriginGone404},
		{name: "400 rate_limit_error", collection: post, iri: origin.URL + "/post/rate-limited", want: OriginRetry},
		{name: "200 empty object", collection: post, iri: origin.URL + "/post/empty", want: OriginRetry},
		{name: "200 JSON error body", collection: post, iri: origin.URL + "/post/error-body", want: OriginRetry},
		{name: "200 Page with another id", collection: post, iri: origin.URL + "/post/mismatched", want: OriginRetry},
		{name: "200 Person (unsupported type)", collection: post, iri: origin.URL + "/post/person", want: OriginRetry},
		{name: "400 couldnt_find_person", collection: post, iri: origin.URL + "/post/person-missing", want: OriginRetry},
		{name: "400 couldnt_find_comment for a post", collection: post, iri: origin.URL + "/post/comment-missing", want: OriginRetry},
		{name: "400 couldnt_find_post for a comment", collection: comment, iri: origin.URL + "/comment/post-missing", want: OriginRetry},
		{name: "400 HTML body", collection: post, iri: origin.URL + "/post/html-400", want: OriginRetry},
	} {
		t.Run(tc.name, func(t *testing.T) {
			obj, err := client.FetchObjectSameAuthority(context.Background(), tc.iri)

			assert.Equal(t, tc.want, ClassifyOrigin(tc.collection, tc.iri, obj, err), "fetch result: obj=%v err=%v", obj, err)
		})
	}
}

// TestCommunityReconcile_LiveCommentRestoreLeavesOtherWithholds (B2): a kept
// comment on C's own instance under a standing removal record that its origin
// serves live is restored: the removal record is deleted and its community-removal withhold
// lifted. An operator withhold of the comment's image, and a record label's
// withhold of another kept post's image, stay.
func TestCommunityReconcile_LiveCommentRestoreLeavesOtherWithholds(t *testing.T) {
	world := newReconcileWorld(t)
	h := world.h
	ctx := context.Background()
	withholds := blobwithhold.NewStore(h.db)

	labelledID := world.postBy(ubUserID, world.communityC, "rc-b2-labelled", ubBeforeBan, true)
	labelledImage := postBlobCID(t, h, labelledID)
	labelledURI := atURIOf(t, h, labelledID)
	world.committer.commit(t, labelledURI, communityTakedown, false)
	require.Len(t, activeLabelWithholds(t, h, labelledImage, labelledURI), 1,
		"precondition: the record label withholds the labelled post's image")

	commentID, commentImage := world.imageCommentBy(rcWorldUserID, world.communityC, "rc-b2-comment", labelledID, ubBeforeBan2, "rc-b2-comment-image")
	commentURI := atURIOf(t, h, commentID)
	world.removeCommentInC("rc-b2-comment", commentID)
	assertOneRemovalWithhold(t, h, commentImage, commentURI, rcWorldUserDID)
	_, err := withholds.Withhold(ctx, blobwithhold.OperatorRequest{
		CID: commentImage, ReasonCode: "operator-discretion", Actor: "rc-b2-operator",
	})
	require.NoError(t, err)
	require.Len(t, activeOperatorWithholds(t, h, commentImage), 1, "precondition: the comment's image is operator-withheld")

	world.restoreAndReady()
	reconciler, _ := world.newReconciler(time.Hour)

	require.NoError(t, reconciler.Sweep(ctx))

	assert.Empty(t, removalCode(t, h, world.communityDID, commentID),
		"the origin serves the comment live: its removal record is deleted")
	assert.Empty(t, activeRemovalWithholds(t, h, commentImage), "the restore lifts the comment's community-removal withhold")
	assertLive(t, h, commentID, "a restored comment keeps its record")
	assert.Len(t, activeOperatorWithholds(t, h, commentImage), 1, "the operator withhold of the comment's image stays")
	covered, err := withholds.Covers(ctx, rcWorldUserDID, commentImage)
	require.NoError(t, err)
	assert.True(t, covered, "the operator withhold still covers the comment's image")
	assertBlobRefused(t, h, rcWorldUserDID, commentImage, "the operator withhold stands")
	assert.Len(t, activeLabelWithholds(t, h, labelledImage, labelledURI), 1, "the record label's withhold stays")
	assertBlobRefused(t, h, ubUserDID, labelledImage, "the record label stands")
}

// TestCommunityReconcile_A404DeletesOnlyTheCommunitysKeptPost (B3): a kept
// post its origin answers 404 for is deleted (record gone, mapping
// soft-deleted). A bridged post of another community and a bridge-origin
// mapping bound to the same community, both also answered 404, are never
// fetched and stay as they were.
func TestCommunityReconcile_A404DeletesOnlyTheCommunitysKeptPost(t *testing.T) {
	world := newReconcileWorld(t)
	h := world.h
	ctx := context.Background()

	keptID := world.postBy(ubUserID, world.communityC, "rc-b3-kept", ubBeforeBan, false)
	otherID := world.postBy(ubUserID, world.communityD, "rc-b3-other", ubBeforeBan, false)
	const nativeDID = "did:plc:communityreconcilenative"
	nativeRKey := "3lrcb3native2a"
	nativeID := "https://coves.test/ap/object/" + nativeDID + "/" + materialize.CollectionPostV2 + "/" + nativeRKey
	_, err := h.objects.PutMapping(ctx, store.APObjectMapping{
		APID:           nativeID,
		APType:         "Page",
		OriginInstance: "coves.test",
		Origin:         store.OriginBridge,
		DID:            nativeDID,
		AuthorDID:      nativeDID,
		CommunityDID:   world.communityDID,
		Collection:     materialize.CollectionPostV2,
		RKey:           nativeRKey,
		CID:            "bafyreievgu2ty7qbiaaom5zhmkznsnajuzideek3lo7e65dwqlrvrxnmo4",
	})
	require.NoError(t, err)
	keptPath, otherPath, nativePath := "/post/rc-b3-kept", "/post/rc-b3-other", urlPath(t, nativeID)
	for _, path := range []string{keptPath, otherPath, nativePath} {
		h.serveStatus(path, http.StatusNotFound)
	}
	hitsBefore := h.hitCounts(keptPath, otherPath, nativePath)
	otherCID := currentRecordCID(t, h, otherID)

	world.restoreAndReady()
	reconciler, _ := world.newReconciler(time.Hour)

	require.NoError(t, reconciler.Sweep(ctx))

	assertDeleted(t, h, keptID, "the origin answers 404 for the kept post")
	assert.Equal(t, hitsBefore[keptPath]+1, h.hitCount(keptPath), "the kept post is fetched once")
	assert.Equal(t, hitsBefore[otherPath], h.hitCount(otherPath), "another community's post is not fetched")
	assert.Equal(t, hitsBefore[nativePath], h.hitCount(nativePath), "a bridge-origin mapping is not fetched")
	assertLive(t, h, otherID, "another community's post is not reconciled")
	assert.Equal(t, otherCID, currentRecordCID(t, h, otherID), "another community's post is unchanged")
	native, err := h.objects.GetByAPID(ctx, nativeID)
	require.NoError(t, err)
	assert.False(t, native.IsDeleted(), "a bridge-origin mapping is not deleted")
}

// TestCommunityReconcile_GoneWritesARemovalAndKeepsTheRecord (B4): a kept
// object its origin answers gone for (410 with a Tombstone body, or 200 with
// a Tombstone body), with no removal record, gets a moderator-discretion
// removal record and keeps its record.
func TestCommunityReconcile_GoneWritesARemovalAndKeepsTheRecord(t *testing.T) {
	for _, tc := range []struct {
		name string
		// seed bridges the kept object into C, makes its origin answer gone,
		// and returns its ap id.
		seed func(world *reconcileWorld) string
	}{
		{
			name: "comment answered 410 with a Tombstone body",
			seed: func(world *reconcileWorld) string {
				rootID := world.postBy(ubOtherUserID, world.communityC, "rc-b4a-root", ubBeforeBan, false)
				commentID := world.commentBy(ubUserID, world.communityC, "rc-b4a", rootID, ubBeforeBan2)
				world.h.serveTombstone("/comment/rc-b4a", commentID, http.StatusGone)
				return commentID
			},
		},
		{
			name: "post answered 200 with a Tombstone body",
			seed: func(world *reconcileWorld) string {
				postID := world.postBy(ubUserID, world.communityC, "rc-b4b", ubBeforeBan, false)
				world.h.serveTombstone("/post/rc-b4b", postID, http.StatusOK)
				return postID
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			world := newReconcileWorld(t)
			h := world.h
			keptID := tc.seed(world)
			require.Empty(t, removalCode(t, h, world.communityDID, keptID), "precondition: no removal record")

			world.restoreAndReady()
			reconciler, _ := world.newReconciler(time.Hour)

			require.NoError(t, reconciler.Sweep(context.Background()))

			assert.Equal(t, materialize.RemovalCodeModeratorDiscretion, removalCode(t, h, world.communityDID, keptID),
				"the origin answers gone: C's removal record is written")
			assertLive(t, h, keptID, "a gone answer keeps the record")
		})
	}
}

// TestCommunityReconcile_RestartAndRetry (B5).
func TestCommunityReconcile_RestartAndRetry(t *testing.T) {
	// a: a pass interrupted during its third fetch is resumed by a new
	// reconciler's Run, which brings every kept object to its final outcome
	// and then fetches nothing more.
	t.Run("a: an interrupted pass is resumed by a restarted Run", func(t *testing.T) {
		world := newReconcileWorld(t)
		h := world.h
		ctx := context.Background()

		// The post is on C's own instance, so its live answer restores it.
		postID := world.postBy(rcWorldUserID, world.communityC, "rc-b5a-post", ubBeforeBan, true)
		postImage := postBlobCID(t, h, postID)
		missingID := world.commentBy(ubUserID, world.communityC, "rc-b5a-missing", postID, ubBeforeBan2)
		goneID, goneImage := world.imageComment(world.communityC, "rc-b5a-gone", postID, ubBeforeBan2, "rc-b5a-gone-image")
		liveID := world.commentBy(ubUserID, world.communityC, "rc-b5a-live", postID, ubBeforeBan2)
		world.removeFromC("rc-b5a-post", postID)
		liveCID := currentRecordCID(t, h, liveID)
		postPath, missingPath, gonePath, livePath :=
			"/post/rc-b5a-post", "/comment/rc-b5a-missing", "/comment/rc-b5a-gone", "/comment/rc-b5a-live"
		h.serveStatus(missingPath, http.StatusNotFound)
		h.serveGone(gonePath)
		world.restoreAndReady()
		countersBefore := reconcileCounters(t)

		interrupted, interruptedLogs := world.newReconciler(time.Hour)
		sweepCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		sweepReturned := make(chan struct{})
		var fetches atomic.Int32
		cancelOnThirdFetch := func() {
			if fetches.Add(1) != 3 {
				return
			}
			cancel()
			select {
			case <-sweepReturned:
			case <-time.After(10 * time.Second):
			}
		}
		paths := []string{postPath, missingPath, gonePath, livePath}
		h.mu.Lock()
		for _, path := range paths {
			h.onFetch[path] = cancelOnThirdFetch
		}
		h.mu.Unlock()

		err := interrupted.Sweep(sweepCtx)
		close(sweepReturned)
		h.mu.Lock()
		for _, path := range paths {
			delete(h.onFetch, path)
		}
		h.mu.Unlock()

		require.ErrorIs(t, err, context.Canceled, "the Sweep stops when its context is cancelled during the third fetch")
		assert.Equal(t, []reconcilePass{{Community: world.communityDID, Complete: false}},
			reconcilePasses(t, interruptedLogs), "the interrupted pass logs one incomplete summary")

		const interval = 25 * time.Millisecond
		restarted, restartedLogs := world.newReconciler(interval)
		runCtx, stopRun := context.WithCancel(ctx)
		runReturned := make(chan struct{})
		go func() {
			defer close(runReturned)
			restarted.Run(runCtx)
		}()
		t.Cleanup(func() {
			stopRun()
			select {
			case <-runReturned:
			case <-time.After(10 * time.Second):
				t.Error("Run did not return within 10s of its context's cancellation")
			}
		})

		require.Eventually(t, func() bool {
			missing, err := h.objects.GetByAPID(ctx, missingID)
			require.NoError(t, err)
			return removalCode(t, h, world.communityDID, postID) == "" &&
				acceptancePinnedCID(t, h, world.communityDID, postID) != "" &&
				missing.IsDeleted() &&
				removalCode(t, h, world.communityDID, goneID) == materialize.RemovalCodeModeratorDiscretion
		}, 10*time.Second, 20*time.Millisecond, "the restarted Run brings every kept object to its final outcome")
		require.Eventually(t, func() bool {
			for _, pass := range reconcilePasses(t, restartedLogs) {
				if pass.Complete {
					return true
				}
			}
			return false
		}, 10*time.Second, 20*time.Millisecond, "the restarted Run completes a pass")

		assert.Empty(t, removalCode(t, h, world.communityDID, postID), "the live post's removal record is deleted")
		assert.Equal(t, currentRecordCID(t, h, postID), acceptancePinnedCID(t, h, world.communityDID, postID),
			"the restored post is accepted at its kept record's CID")
		assertLive(t, h, postID, "a restored post keeps its record")
		assert.Empty(t, activeRemovalWithholds(t, h, postImage), "the restored post's image has no removal withhold")
		assertDeleted(t, h, missingID, "the origin answers 404 for the comment")
		assert.Equal(t, materialize.RemovalCodeModeratorDiscretion, removalCode(t, h, world.communityDID, goneID))
		assertLive(t, h, goneID, "a gone comment keeps its record")
		assertOneRemovalWithhold(t, h, goneImage, atURIOf(t, h, goneID), ubUserDID)
		assert.Empty(t, removalCode(t, h, world.communityDID, liveID), "the live comment has no removal record")
		assert.Equal(t, liveCID, currentRecordCID(t, h, liveID), "the live comment is unchanged")

		hitsAtCompletion := h.hitCounts(paths...)
		assert.Never(t, func() bool {
			for path, hits := range h.hitCounts(paths...) {
				if hits != hitsAtCompletion[path] {
					return true
				}
			}
			return false
		}, 20*interval, interval, "the completed pass cleared the reconcile: later Run intervals fetch nothing")
		assert.Equal(t, []reconcilePass{{Community: world.communityDID, Complete: true}},
			reconcilePasses(t, restartedLogs), "the restarted Run logs one complete summary")
		// live is left out: how many live answers the interrupted pass got
		// depends on the order it fetched the kept objects in. Case b counts it.
		deltas := reconcileCounterDeltas(t, countersBefore)
		delete(deltas, "live")
		assert.Equal(t, map[string]int64{
			"restored": 1, "deleted": 1, "removed": 1, "retry": 0, "unreachable": 0, "passes_completed": 1,
		}, deltas, "outcome counters over the interrupted and the resumed pass")
	})

	// b: a fetch failure leaves the pass pending; the next pass re-fetches
	// every kept object, writes nothing new for an outcome already applied,
	// and completes; a third finds nothing pending.
	t.Run("b: a retry re-fetches and replays without new writes", func(t *testing.T) {
		world := newReconcileWorld(t)
		h := world.h
		ctx := context.Background()

		rootID := world.postBy(ubOtherUserID, world.communityC, "rc-b5b-root", ubBeforeBan, false)
		goneID, goneImage := world.imageComment(world.communityC, "rc-b5b-gone", rootID, ubBeforeBan2, "rc-b5b-gone-image")
		flakyID := world.commentBy(ubUserID, world.communityC, "rc-b5b-flaky", rootID, ubBeforeBan2)
		rootPath, gonePath, flakyPath := "/post/rc-b5b-root", "/comment/rc-b5b-gone", "/comment/rc-b5b-flaky"
		h.serveGone(gonePath)
		h.serveStatus(flakyPath, http.StatusServiceUnavailable)
		world.restoreAndReady()
		countersBefore := reconcileCounters(t)
		reconciler, logs := world.newReconciler(time.Hour)
		hitsBefore := h.hitCounts(rootPath, gonePath, flakyPath)

		require.NoError(t, reconciler.Sweep(ctx))

		require.Equal(t, materialize.RemovalCodeModeratorDiscretion, removalCode(t, h, world.communityDID, goneID),
			"the first pass writes the gone comment's removal")
		assert.Equal(t, hitsBefore[flakyPath]+1, h.hitCount(flakyPath), "the first pass fetched the flaky comment")
		assertLive(t, h, flakyID, "a failed fetch changes nothing")
		goneDigest := testDigestRKey(atURIOf(t, h, goneID))
		removalBefore, removalCIDBefore, err := h.manager.GetRecord(ctx, world.communityDID, materialize.CollectionRemoval, goneDigest)
		require.NoError(t, err)
		_, revBefore, err := h.manager.Head(ctx, world.communityDID)
		require.NoError(t, err)
		withholdRowsBefore := withholdRowCount(t, h, goneImage)
		h.serveStatus(flakyPath, 0)

		require.NoError(t, reconciler.Sweep(ctx))

		assert.Equal(t, hitsBefore[gonePath]+2, h.hitCount(gonePath), "the second pass re-fetches the gone comment")
		assert.Equal(t, hitsBefore[flakyPath]+2, h.hitCount(flakyPath), "the second pass re-fetches the flaky comment")
		removalAfter, removalCIDAfter, err := h.manager.GetRecord(ctx, world.communityDID, materialize.CollectionRemoval, goneDigest)
		require.NoError(t, err)
		assert.Equal(t, removalCIDBefore, removalCIDAfter, "the replay does not rewrite the removal record")
		assert.Equal(t, removalBefore, removalAfter)
		_, revAfter, err := h.manager.Head(ctx, world.communityDID)
		require.NoError(t, err)
		assert.Equal(t, revBefore, revAfter, "the replay commits nothing to C's repo")
		assert.Equal(t, withholdRowsBefore, withholdRowCount(t, h, goneImage), "the replay adds no withhold rows")
		assert.Empty(t, removalCode(t, h, world.communityDID, flakyID), "the now-live comment has no removal record")
		assertLive(t, h, flakyID, "the now-live comment keeps its record")
		hitsAfterSecond := h.hitCounts(rootPath, gonePath, flakyPath)

		require.NoError(t, reconciler.Sweep(ctx))

		assert.Equal(t, hitsAfterSecond, h.hitCounts(rootPath, gonePath, flakyPath),
			"the completed pass cleared the reconcile: a third Sweep fetches nothing")
		assert.Equal(t, []reconcilePass{
			{Community: world.communityDID, Complete: false},
			{Community: world.communityDID, Complete: true},
		}, reconcilePasses(t, logs), "one summary per community pass")
		assert.Equal(t, map[string]int64{
			// live: the root in both passes and the flaky comment in the second.
			"live": 3, "restored": 0, "deleted": 0, "removed": 1, "retry": 1, "unreachable": 0, "passes_completed": 1,
		}, reconcileCounterDeltas(t, countersBefore), "outcome counters over the three Sweeps")
	})

	// c: a failed apply leaves the pass pending; the next pass re-fetches and
	// applies, and a third finds nothing pending.
	t.Run("c: a failed delete leaves the reconcile pending", func(t *testing.T) {
		world := newReconcileWorld(t)
		h := world.h
		ctx := context.Background()

		keptID := world.postBy(ubUserID, world.communityC, "rc-b5c-kept", ubBeforeBan, false)
		keptPath := "/post/rc-b5c-kept"
		h.serveStatus(keptPath, http.StatusNotFound)
		world.restoreAndReady()
		reconciler, _ := world.newReconciler(time.Hour)
		hitsBefore := h.hitCount(keptPath)
		drop := failMappingDeletes(t, h)

		_ = reconciler.Sweep(ctx)

		assert.Equal(t, hitsBefore+1, h.hitCount(keptPath), "the first pass fetched the kept post")
		mapping, err := h.objects.GetByAPID(ctx, keptID)
		require.NoError(t, err)
		require.False(t, mapping.IsDeleted(), "precondition: the failed delete left the mapping live")
		drop()

		require.NoError(t, reconciler.Sweep(ctx))

		assert.Equal(t, hitsBefore+2, h.hitCount(keptPath), "the failed delete left the reconcile pending: re-fetched")
		assertDeleted(t, h, keptID, "the second pass deletes the 404 post")

		require.NoError(t, reconciler.Sweep(ctx))

		assert.Equal(t, hitsBefore+2, h.hitCount(keptPath), "the completed pass cleared the reconcile: not fetched again")
	})
}

// TestCommunityReconcile_AnotherObjectsBodyIsLoggedAsNoAnswer: the origin
// answers a kept post's GET 200 with another object's body. The pass changes
// nothing for the post, and its no-answer log names the id and type served.
func TestCommunityReconcile_AnotherObjectsBodyIsLoggedAsNoAnswer(t *testing.T) {
	world := newReconcileWorld(t)
	h := world.h
	keptID := world.postBy(ubUserID, world.communityC, "rc-mismatch", ubBeforeBan, false)
	const servedID = "https://lemmy.world/post/rc-mismatch-other"
	h.serveObject(urlPath(t, keptID), map[string]any{"id": servedID, "type": "Page"})
	world.restoreAndReady()
	reconciler, logs := world.newReconciler(time.Hour)

	require.NoError(t, reconciler.Sweep(context.Background()))

	assertLive(t, h, keptID, "a body that is not the post changes nothing")
	assert.Contains(t, logs.String(), `"served_id":"`+servedID+`"`)
	assert.Contains(t, logs.String(), `"served_type":"Page"`)
}
