package ingest

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"expvar"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"tidepool/internal/blobwithhold"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// Shared fixtures for the bridged community restore reconcile tests: community
// C (lemmy.world technology, subscribed by hand) of the upstream-ban world is
// removed by a bare-DID `!takedown` and restored by its negation, both
// committed through the real label store, which sets C's pending reconcile.
// C's kept objects are then answered by their origins as each test needs.

// rcWorldUserID is a user on lemmy.world, C's own instance: C's authority over
// its removals extends to this user's content, not to a user on another host.
const rcWorldUserID = "https://lemmy.world/u/WorldPoster"

var rcWorldUserDID = testDIDFor("WorldPoster", "lemmy.world")

// reconcileWorld is the upstream-ban world plus a label committer running the
// materializer's label applier and the community applier, as production's
// label store does.
type reconcileWorld struct {
	*upstreamBanWorld
	committer    *labelCommitter
	communityDID string
}

func newReconcileWorld(t *testing.T) *reconcileWorld {
	t.Helper()
	world := newUpstreamBanWorld(t)
	h := world.h
	h.newRemoteActor(rcWorldUserID, person(rcWorldUserID, "WorldPoster", nil))
	return &reconcileWorld{
		upstreamBanWorld: world,
		committer:        newLabelCommitterWith(t, h, h.mat.LabelApplier(), h.admin.CommunityLabelApplier()),
		communityDID:     h.communityDID(groupID),
	}
}

// restoreAndReady removes C and negates the removal through the label store,
// then makes C ready for its reconcile: the follow retrier sends the owed
// Follow, C accepts it, and its backfill is stamped.
func (w *reconcileWorld) restoreAndReady() {
	t := w.h.t
	t.Helper()
	ctx := context.Background()
	w.committer.removeCommunity(t, w.communityDID)
	require.Equal(t, store.FollowStateNone, w.h.followStateOf(groupID), "precondition: the label removes C")
	w.committer.restoreCommunity(t, w.communityDID)
	require.Equal(t, store.FollowStatePending, w.h.followStateOf(groupID), "precondition: the negation follows C again")
	w.h.newTestFollowRetrier(w.h.client).Sweep(ctx)
	w.h.acceptLatestFollow(w.communityC)
	require.Equal(t, store.FollowStateAccepted, w.h.followStateOf(groupID), "precondition: C accepted the Follow")
	require.NoError(t, w.h.communities.SetLastBackfill(ctx, groupID, time.Now()))
}

// newReconciler builds a CommunityReconciler over the harness stack whose
// logs land in the returned buffer as JSON.
func (w *reconcileWorld) newReconciler(interval time.Duration) (*CommunityReconciler, *syncBuffer) {
	t := w.h.t
	t.Helper()
	logs := &syncBuffer{}
	reconciler, err := NewCommunityReconciler(CommunityReconcilerOptions{
		DB:           w.h.db,
		Fetcher:      w.h.client,
		Materializer: w.h.mat,
		Interval:     interval,
		Logger:       slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	require.NoError(t, err)
	require.NotNil(t, reconciler)
	return reconciler, logs
}

// removeCommentInC is C's moderator removal of commentID (an announced Delete
// carrying a summary), checked to have landed.
func (w *reconcileWorld) removeCommentInC(slug, commentID string) {
	t := w.h.t
	t.Helper()
	w.h.announceDeleteAs(w.communityC, w.communityC.id+"/activities/announce/delete/"+slug,
		"https://lemmy.world/activities/delete/"+slug, commentID, modActorID, ptr(rwSummary))
	require.Equal(t, materialize.RemovalCodeModeratorDiscretion, removalCode(t, w.h, w.communityDID, commentID),
		"precondition: C's removal record for %s stands", commentID)
}

// serveStatus makes a GET of path answer status (with the body served there,
// if any); 0 restores the served answer. An unserved path is registered.
func (h *harness) serveStatus(path string, status int) {
	h.mu.Lock()
	_, registered := h.served[path]
	h.mu.Unlock()
	if !registered {
		h.serveJSON(path, nil)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.statuses[path] = status
}

// serveTombstone answers path with status and a Tombstone body for apID.
func (h *harness) serveTombstone(path, apID string, status int) {
	h.t.Helper()
	h.serveObject(path, map[string]any{"id": apID, "type": "Tombstone", "formerType": "Note"})
	h.serveStatus(path, status)
}

// hitCounts reads the hit count of each path.
func (h *harness) hitCounts(paths ...string) map[string]int {
	counts := map[string]int{}
	for _, path := range paths {
		counts[path] = h.hitCount(path)
	}
	return counts
}

// reconcileCounter reads the expvar tidepool_community_reconcile_<name>.
func reconcileCounter(t *testing.T, name string) int64 {
	t.Helper()
	counter, ok := expvar.Get("tidepool_community_reconcile_" + name).(*expvar.Int)
	require.True(t, ok, "expvar tidepool_community_reconcile_%s is published", name)
	return counter.Value()
}

// reconcileCounterNames are the reconcile expvars, by suffix.
var reconcileCounterNames = []string{"live", "restored", "deleted", "removed", "retry", "unreachable", "passes_completed"}

// reconcileCounters snapshots every reconcile expvar.
func reconcileCounters(t *testing.T) map[string]int64 {
	t.Helper()
	snapshot := map[string]int64{}
	for _, name := range reconcileCounterNames {
		snapshot[name] = reconcileCounter(t, name)
	}
	return snapshot
}

// reconcileCounterDeltas is each reconcile expvar's change since before.
func reconcileCounterDeltas(t *testing.T, before map[string]int64) map[string]int64 {
	t.Helper()
	deltas := map[string]int64{}
	for name, value := range reconcileCounters(t) {
		deltas[name] = value - before[name]
	}
	return deltas
}

// reconcilePass is one `community reconcile pass` summary record.
type reconcilePass struct {
	Community string
	Complete  bool
}

// reconcilePasses parses the summary records out of a reconciler's JSON logs.
func reconcilePasses(t *testing.T, logs *syncBuffer) []reconcilePass {
	t.Helper()
	var passes []reconcilePass
	scanner := bufio.NewScanner(strings.NewReader(logs.String()))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		var record map[string]any
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &record), "log line %q", scanner.Text())
		if record["msg"] != "community reconcile pass" {
			continue
		}
		community, _ := record["community"].(string)
		complete, isBool := record["complete"].(bool)
		require.True(t, isBool, "the summary record carries complete as a bool: %q", scanner.Text())
		passes = append(passes, reconcilePass{Community: community, Complete: complete})
	}
	require.NoError(t, scanner.Err())
	return passes
}

// reconcileMarkers reads C's pending reconcile marker and the first attempt
// stamped for it (each invalid when NULL).
func (w *reconcileWorld) reconcileMarkers() (pendingAt, firstAttemptAt sql.NullTime) {
	t := w.h.t
	t.Helper()
	require.NoError(t, w.h.db.QueryRow(`
		SELECT reconcile_pending_at, reconcile_first_attempt_at FROM communities WHERE did = $1`,
		w.communityDID).Scan(&pendingAt, &firstAttemptAt))
	return pendingAt, firstAttemptAt
}

// backdateFirstAttempt sets C's first reconcile attempt to age ago, as if
// the pending reconcile's passes had been retrying for that long.
func (w *reconcileWorld) backdateFirstAttempt(age time.Duration) {
	t := w.h.t
	t.Helper()
	result, err := w.h.db.Exec(`
		UPDATE communities SET reconcile_first_attempt_at = clock_timestamp() - make_interval(secs => $2)
		WHERE did = $1 AND reconcile_pending_at IS NOT NULL`, w.communityDID, age.Seconds())
	require.NoError(t, err)
	affected, err := result.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, 1, affected, "precondition: C's reconcile is pending")
}

// withholdRowCount counts every blob_withholds row of cidStr, lifted or not.
func withholdRowCount(t *testing.T, h *harness, cidStr string) int {
	t.Helper()
	state, err := blobwithhold.NewStore(h.db).Inspect(context.Background(), cidStr)
	require.NoError(t, err)
	return len(state.Withholds)
}

// acceptancePinnedCID reads the subject CID of C's acceptance of postID, or
// "" when there is none.
func acceptancePinnedCID(t *testing.T, h *harness, communityDID, postID string) string {
	t.Helper()
	if !acceptanceExists(t, h, communityDID, postID) {
		return ""
	}
	acceptance, _, err := h.manager.GetRecord(context.Background(), communityDID,
		materialize.CollectionAcceptance, testDigestRKey(atURIOf(t, h, postID)))
	require.NoError(t, err)
	subject, ok := acceptance["subject"].(map[string]any)
	require.True(t, ok, "the acceptance subject is a strongRef, got %#v", acceptance["subject"])
	return strongRefCID(subject["cid"])
}

// currentRecordCID reads apID's current record CID from its repo.
func currentRecordCID(t *testing.T, h *harness, apID string) string {
	t.Helper()
	ctx := context.Background()
	mapping, err := h.objects.GetByAPID(ctx, apID)
	require.NoError(t, err)
	_, recordCID, err := h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	require.NoError(t, err, "%s has its record", apID)
	return recordCID
}

// failMappingDeletes makes every soft delete of an ap_objects mapping raise
// until the returned drop runs (also run at cleanup).
func failMappingDeletes(t *testing.T, h *harness) (drop func()) {
	t.Helper()
	drop = func() {
		_, err := h.db.Exec(`
			DROP TRIGGER IF EXISTS community_reconcile_test_fail_delete ON ap_objects;
			DROP FUNCTION IF EXISTS community_reconcile_test_fail_delete();`)
		require.NoError(t, err)
	}
	drop()
	t.Cleanup(drop)
	_, err := h.db.Exec(`
		CREATE FUNCTION community_reconcile_test_fail_delete() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			RAISE EXCEPTION 'community reconcile test: mapping delete refused';
		END $$;
		CREATE TRIGGER community_reconcile_test_fail_delete BEFORE UPDATE ON ap_objects
			FOR EACH ROW WHEN (OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL)
			EXECUTE FUNCTION community_reconcile_test_fail_delete();`)
	require.NoError(t, err)
	return drop
}
