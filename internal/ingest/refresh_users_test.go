package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

const refreshUsersPath = "/admin/users/refresh-profile"

// walkTestTimeout bounds every wait on the background walk, so a broken
// implementation fails the test instead of hanging it.
const walkTestTimeout = 15 * time.Second

// switchableActorDoc serves one actor document at one fixture path. It is
// registered once, before minting (a second registration on the same mux
// path panics), and then switched: a new body, a failing status, or a hold
// that parks each request until release.
type switchableActorDoc struct {
	mu      sync.Mutex
	body    []byte
	status  int
	hits    int
	holding bool
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func (h *harness) serveSwitchableActor(path string, doc map[string]any) *switchableActorDoc {
	h.t.Helper()
	d := &switchableActorDoc{
		status:  http.StatusOK,
		reached: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	d.setDoc(h.t, doc)
	h.mux.HandleFunc("GET "+path, d.ServeHTTP)
	return d
}

func (d *switchableActorDoc) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	d.mu.Lock()
	d.hits++
	holding := d.holding
	d.mu.Unlock()
	if holding {
		select {
		case d.reached <- struct{}{}:
		default:
		}
		select {
		case <-d.release:
		case <-r.Context().Done():
			return
		}
	}
	d.mu.Lock()
	body, status := d.body, d.status
	d.mu.Unlock()
	if status != http.StatusOK {
		http.Error(w, "upstream failure", status)
		return
	}
	w.Header().Set("Content-Type", ap.ContentTypeActivityJSON)
	_, _ = w.Write(body)
}

func (d *switchableActorDoc) setDoc(t *testing.T, doc map[string]any) {
	t.Helper()
	body, err := json.Marshal(doc)
	require.NoError(t, err)
	d.mu.Lock()
	d.body = body
	d.mu.Unlock()
}

func (d *switchableActorDoc) failWith(status int) {
	d.mu.Lock()
	d.status = status
	d.mu.Unlock()
}

// hold parks every later request until releaseHold; each parked request
// signals reached.
func (d *switchableActorDoc) hold() {
	d.mu.Lock()
	d.holding = true
	d.mu.Unlock()
}

func (d *switchableActorDoc) releaseHold() { d.once.Do(func() { close(d.release) }) }

func (d *switchableActorDoc) hitCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.hits
}

// awaitReached waits (bounded) until a held request has arrived.
func (d *switchableActorDoc) awaitReached(t *testing.T, what string) {
	t.Helper()
	select {
	case <-d.reached:
	case <-time.After(walkTestTimeout):
		require.FailNow(t, what+": the held actor document was never fetched")
	}
}

// postAdminAsync sends an authenticated POST with a raw body through router
// on its own goroutine, so a handler that blocks cannot hang the test.
func postAdminAsync(ctx context.Context, router http.Handler, path string, body []byte) <-chan *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "https://"+bridgeHost+path, bytes.NewReader(body)).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+testAdminToken)
	out := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		out <- rec
	}()
	return out
}

func awaitResponse(t *testing.T, responses <-chan *httptest.ResponseRecorder) *httptest.ResponseRecorder {
	t.Helper()
	select {
	case rec := <-responses:
		return rec
	case <-time.After(walkTestTimeout):
		require.FailNow(t, "the admin request did not answer in time")
		return nil
	}
}

// drainWalks waits (bounded) for admin's background walks to finish.
func drainWalks(t *testing.T, admin *Admin) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		admin.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(walkTestTimeout):
		t.Errorf("the background walk did not drain in time")
	}
}

// walkSummary returns the single summary line the named walk logged.
func walkSummary(t *testing.T, logs *syncBuffer, logName string) string {
	t.Helper()
	var summaries []string
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, `msg="`+logName+` finished"`) {
			summaries = append(summaries, line)
		}
	}
	require.Len(t, summaries, 1, "the walk logs its summary exactly once: %s", logs.String())
	return summaries[0]
}

func (h *harness) actorRow(t *testing.T, apID string) *store.BridgedActor {
	t.Helper()
	row, err := h.actors.GetByAPActorID(context.Background(), apID)
	require.NoError(t, err)
	return row
}

// TestRefreshUsersWalk: {"all":true} walks every non-deleted bridged person
// in the background, in id order, and pushes what each instance says now
// through the ordinary refresh path. The count excludes the tombstoned user
// and the community; a failing instance and an opted-out user do not stop
// the walk; a second walk while one runs is a 409; and a refreshed profile
// carries the origin address fields.
func TestRefreshUsersWalk(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	const (
		failingID   = "https://lemmy.world/u/walkfailing"
		nobridgeID  = "https://lemmy.world/u/walknobridge"
		tombstoneID = "https://lemmy.world/u/walktombstoned"
		walkGroupID = "https://lemmy.world/c/walkgroup"
		liveID      = "https://lemmy.world/u/Walk_User"
	)
	groupDoc := func(name string) map[string]any {
		return map[string]any{
			"type":              "Group",
			"id":                walkGroupID,
			"preferredUsername": "walkgroup",
			"name":              name,
			"inbox":             walkGroupID + "/inbox",
			"published":         "2024-01-01T00:00:00.000000Z",
		}
	}

	failing := h.serveSwitchableActor("/u/walkfailing", person(failingID, "walkfailing", nil))
	h.serveObject("/u/walknobridge", person(nobridgeID, "walknobridge", nil))
	h.serveObject("/u/walktombstoned", person(tombstoneID, "walktombstoned", nil))
	h.serveObject("/c/walkgroup", groupDoc("Walk Group"))
	live := h.serveSwitchableActor("/u/Walk_User",
		person(liveID, "Walk_User", map[string]any{"name": "Walk User"}))
	t.Cleanup(func() {
		live.releaseHold()
		failing.releaseHold()
		drainWalks(t, h.admin)
	})

	// Mint in walk order (ids ascend with insertion).
	for _, id := range []string{failingID, nobridgeID, tombstoneID} {
		_, err := h.mat.EnsureActor(ctx, &ap.Object{ID: id})
		require.NoError(t, err, id)
	}
	community, err := h.mat.EnsureCommunity(ctx, &ap.Object{ID: walkGroupID})
	require.NoError(t, err)
	_, err = h.mat.EnsureActor(ctx, &ap.Object{ID: liveID})
	require.NoError(t, err)

	// The nobridge user opts out through the consent path: its instance now
	// serves (and the walk will re-fetch) a profile carrying #nobridge.
	optedOut := person(nobridgeID, "walknobridge", map[string]any{"summary": "<p>no thanks #nobridge</p>"})
	h.serveObject("/u/walknobridge", optedOut)
	optedOutRaw, err := json.Marshal(optedOut)
	require.NoError(t, err)
	optedOutObj, err := ap.ParseObject(optedOutRaw)
	require.NoError(t, err)
	_, err = h.mat.HandleUpdate(ctx, optedOutObj, "")
	require.True(t, materialize.IsSkip(err), "opting out is a skip, got %v", err)
	nobridgeBefore := h.actorRow(t, nobridgeID)
	require.Equal(t, store.ConsentStateNoBridge, nobridgeBefore.ConsentState)
	require.NotNil(t, nobridgeBefore.ProfileSyncedAt)

	// The tombstoned user deletes their account.
	require.NoError(t, h.mat.DeleteActor(ctx, tombstoneID))
	tombstoned := h.actorRow(t, tombstoneID)
	require.Equal(t, store.ConsentStateDeleted, tombstoned.ConsentState)

	// The community renames upstream; a user walk must not touch it.
	groupProfile, _, err := h.manager.GetRecord(ctx, community.DID, materialize.CollectionCommunityProfile, materialize.ProfileRKey)
	require.NoError(t, err)
	require.Equal(t, "Walk Group", groupProfile["displayName"])
	h.serveObject("/c/walkgroup", groupDoc("Walk Group Renamed"))

	// Walk-time upstream state: the failing instance errors, the live user
	// renamed and its fetch is held so the walk can be caught mid-flight.
	failing.failWith(http.StatusInternalServerError)
	live.setDoc(t, person(liveID, "Walk_User", map[string]any{"name": "Walk User Renamed"}))
	live.hold()
	tombstoneHits := h.hitCount("/u/walktombstoned")

	rec := awaitResponse(t, postAdminAsync(ctx, h.router, refreshUsersPath, []byte(`{"all":true}`)))
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"users":3`)

	live.awaitReached(t, "the walk must reach the live user past the failing and opted-out ones")
	conflict := awaitResponse(t, postAdminAsync(ctx, h.router, refreshUsersPath, []byte(`{"all":true}`)))
	assert.Equal(t, http.StatusConflict, conflict.Code, conflict.Body.String())

	live.releaseHold()
	drainWalks(t, h.admin)
	// done counts users visited; the failing instance is a failure and the
	// opted-out user a skip, each counted once.
	assert.Contains(t, walkSummary(t, h.logs, "user refresh-profile walk"),
		"users=3 done=3 failed=1 skipped=1 aborted=false")

	liveRow := h.actorRow(t, liveID)
	profile, _, err := h.manager.GetRecord(ctx, liveRow.DID, materialize.CollectionActorProfile, materialize.ProfileRKey)
	require.NoError(t, err)
	assert.Equal(t, "Walk User Renamed", profile["displayName"])
	assert.Equal(t, "lemmy.world", profile["origin"])
	assert.Equal(t, "Walk_User", profile["originUsername"])
	assert.Equal(t, "https://lemmy.world/u/Walk_User", profile["originUrl"])

	nobridgeAfter := h.actorRow(t, nobridgeID)
	assert.Equal(t, store.ConsentStateNoBridge, nobridgeAfter.ConsentState)
	require.NotNil(t, nobridgeAfter.ProfileSyncedAt)
	assert.True(t, nobridgeAfter.ProfileSyncedAt.After(*nobridgeBefore.ProfileSyncedAt),
		"the walk must re-check the opted-out user (profile_synced_at %v, before %v)",
		*nobridgeAfter.ProfileSyncedAt, *nobridgeBefore.ProfileSyncedAt)
	_, _, err = h.manager.GetRecord(ctx, nobridgeAfter.DID, materialize.CollectionActorProfile, materialize.ProfileRKey)
	assert.True(t, errors.IsNotFound(err), "an opted-out user keeps no profile record, got %v", err)

	tombstonedAfter := h.actorRow(t, tombstoneID)
	assert.Equal(t, store.ConsentStateDeleted, tombstonedAfter.ConsentState)
	_, _, err = h.manager.GetRecord(ctx, tombstonedAfter.DID, materialize.CollectionActorProfile, materialize.ProfileRKey)
	assert.True(t, errors.IsNotFound(err), "a tombstoned user keeps no profile record, got %v", err)
	assert.Equal(t, tombstoneHits, h.hitCount("/u/walktombstoned"), "a tombstoned user's document is never fetched")

	groupProfile, _, err = h.manager.GetRecord(ctx, community.DID, materialize.CollectionCommunityProfile, materialize.ProfileRKey)
	require.NoError(t, err)
	assert.Equal(t, "Walk Group", groupProfile["displayName"], "a user walk must not rewrite a community profile")
}

// TestRefreshUsersRejectsBadRequests: the walk is an explicit opt-in; any
// other body is a 400 and fetches nothing.
func TestRefreshUsersRejectsBadRequests(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	const userID = "https://lemmy.world/u/walkreject"
	h.serveObject("/u/walkreject", person(userID, "walkreject", nil))
	_, err := h.mat.EnsureActor(ctx, &ap.Object{ID: userID})
	require.NoError(t, err)
	t.Cleanup(func() { drainWalks(t, h.admin) })
	before := h.hitCount("/u/walkreject")

	for _, tc := range []struct {
		name string
		body string
	}{
		{"empty object", `{}`},
		{"all false", `{"all":false}`},
		{"malformed JSON", `{"all":`},
		{"unknown field", `{"all":true,"user":"@x@y"}`},
		{"trailing value", `{"all":true}{"all":true}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := awaitResponse(t, postAdminAsync(ctx, h.router, refreshUsersPath, []byte(tc.body)))
			assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		})
	}
	drainWalks(t, h.admin)
	assert.Equal(t, before, h.hitCount("/u/walkreject"), "a rejected request must fetch nothing")
}

// TestRefreshUsersWalkLifecycle: the walk outlives the request that started
// it (it runs under the Admin's base context), Wait blocks while a refresh
// is in flight, and cancelling the base context stops the walk before the
// next user is fetched.
func TestRefreshUsersWalkLifecycle(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	baseCtx, cancelBase := context.WithCancel(context.Background())
	opts := h.adminOptions()
	opts.BaseContext = baseCtx
	admin, err := NewAdmin(opts)
	require.NoError(t, err)
	router := chi.NewRouter()
	admin.Routes(router)

	const (
		firstID  = "https://lemmy.world/u/lifefirst"
		secondID = "https://lemmy.world/u/lifesecond"
		thirdID  = "https://lemmy.world/u/lifethird"
		fourthID = "https://lemmy.world/u/lifefourth"
	)
	first := h.serveSwitchableActor("/u/lifefirst", person(firstID, "lifefirst", nil))
	second := h.serveSwitchableActor("/u/lifesecond", person(secondID, "lifesecond", nil))
	third := h.serveSwitchableActor("/u/lifethird", person(thirdID, "lifethird", nil))
	fourth := h.serveSwitchableActor("/u/lifefourth", person(fourthID, "lifefourth", nil))
	t.Cleanup(func() {
		cancelBase()
		first.releaseHold()
		second.releaseHold()
		third.releaseHold()
		fourth.releaseHold()
		drainWalks(t, admin)
	})
	for _, id := range []string{firstID, secondID, thirdID, fourthID} {
		_, err := h.mat.EnsureActor(ctx, &ap.Object{ID: id})
		require.NoError(t, err, id)
	}
	first.hold()
	second.hold()
	thirdHits := third.hitCount()
	fourthHits := fourth.hitCount()

	// The operator's request goes away the moment it is answered.
	reqCtx, cancelReq := context.WithCancel(context.Background())
	rec := awaitResponse(t, postAdminAsync(reqCtx, router, refreshUsersPath, []byte(`{"all":true}`)))
	cancelReq()
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"users":4`)

	// The first fetch is held while the request context is already gone;
	// releasing it must let the walk carry on to the second user.
	first.awaitReached(t, "the walk must start")
	first.releaseHold()
	second.awaitReached(t, "a cancelled request context must not stop an accepted walk")

	waited := make(chan struct{})
	go func() {
		admin.Wait()
		close(waited)
	}()
	select {
	case <-waited:
		require.FailNow(t, "Wait returned while a refresh was still in flight")
	case <-time.After(200 * time.Millisecond):
	}

	cancelBase()
	second.releaseHold()
	select {
	case <-waited:
	case <-time.After(walkTestTimeout):
		require.FailNow(t, "Wait did not return after the base context was cancelled")
	}
	assert.Equal(t, thirdHits, third.hitCount(), "a cancelled walk must not fetch the next user")
	assert.Equal(t, fourthHits, fourth.hitCount(), "a cancelled walk must not fetch any later user")
	// The walk stops at its next user instead of pausing over and failing
	// each remaining one: two users visited, the rest never attempted. The
	// one failure is the second user's in-flight fetch, cut by the
	// cancellation that preceded its release.
	assert.Contains(t, walkSummary(t, h.logs, "user refresh-profile walk"),
		"users=4 done=2 failed=1 skipped=0 aborted=true")
}
