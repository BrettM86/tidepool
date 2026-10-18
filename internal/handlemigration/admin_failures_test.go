package handlemigration

import (
	"context"
	stderrors "errors"
	"net/http"
	"sync"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// finishFailingRuns fails the first Finish call, as a database blip would.
type finishFailingRuns struct {
	store.HandleMigrationRuns
	mutex       sync.Mutex
	finishCalls int
}

func (r *finishFailingRuns) Finish(ctx context.Context, id int64) error {
	r.mutex.Lock()
	r.finishCalls++
	call := r.finishCalls
	r.mutex.Unlock()
	if call == 1 {
		return stderrors.New("injected finish failure")
	}
	return r.HandleMigrationRuns.Finish(ctx, id)
}

func (r *finishFailingRuns) calls() int {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.finishCalls
}

// disconnectingStartRuns models a client that disconnects while Start's INSERT
// commits: database/sql then reports the caller's cancellation even though the
// row is durable.
type disconnectingStartRuns struct {
	store.HandleMigrationRuns
	disconnect context.CancelFunc
}

func (r *disconnectingStartRuns) Start(ctx context.Context, mode store.HandleMigrationMode, dids []string) (*store.HandleMigrationRun, error) {
	r.disconnect()
	run, err := r.HandleMigrationRuns.Start(context.WithoutCancel(ctx), mode, dids)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return run, nil
}

// listingFailingActors fails the first candidate listing.
type listingFailingActors struct {
	store.BridgedActors
	mutex        sync.Mutex
	listingCalls int
}

func (a *listingFailingActors) ListForwardRenameCandidates(ctx context.Context, bridgeHostname, handleZoneRoot string) ([]string, error) {
	a.mutex.Lock()
	a.listingCalls++
	call := a.listingCalls
	a.mutex.Unlock()
	if call == 1 {
		return nil, stderrors.New("injected listing failure")
	}
	return a.BridgedActors.ListForwardRenameCandidates(ctx, bridgeHostname, handleZoneRoot)
}

func adminRouterWith(t *testing.T, runner *Runner, runs store.HandleMigrationRuns, base context.Context) (http.Handler, *Admin) {
	t.Helper()
	admin, err := NewAdmin(AdminOptions{Token: migrateToken, Runner: runner, BaseContext: base, Runs: runs})
	require.NoError(t, err)
	router := chi.NewRouter()
	admin.Routes(router)
	return router, admin
}

func activeRunCount(t *testing.T, h *renameHarness) int {
	t.Helper()
	var count int
	require.NoError(t, h.db.QueryRowContext(t.Context(),
		`SELECT count(*) FROM handle_migration_runs WHERE finished_at IS NULL`).Scan(&count))
	return count
}

func TestAdminMigrate_RetriesFailedFinish(t *testing.T) {
	h := newRenameHarness(t)
	const did = "did:plc:finishretry"
	h.insertOldActor(t, did, "https://example.com/u/retry", "retry.example-com.tidepool.test", "retry")
	runs := &finishFailingRuns{HandleMigrationRuns: store.NewHandleMigrationRuns(h.db)}
	router, admin := adminRouterWith(t, h.runner, runs, t.Context())
	require.Equal(t, http.StatusAccepted, migrationRequest(router, `{"dids":["`+did+`"]}`, true, t.Context()).Code)
	waitForAdmin(t, admin)
	progress := readMigrationProgress(t, migrationGetRequest(router, true, t.Context()))
	require.False(t, progress.Running, "a failed Finish left the run active")
	require.Equal(t, 0, activeRunCount(t, h))
	require.Equal(t, 2, runs.calls())
	require.Equal(t, http.StatusAccepted, migrationRequest(router, `{}`, true, t.Context()).Code)
	waitForAdmin(t, admin)
	require.Equal(t, 0, activeRunCount(t, h))
	assertFinalizedRename(t, h, did, "retry.example-com.tidepool.test", "retry.example.com.ap.tidepool.test")
}

func TestAdminMigrate_ClientDisconnectDuringStartKeepsRunOwned(t *testing.T) {
	h := newRenameHarness(t)
	const did = "did:plc:disconnectstart"
	h.insertOldActor(t, did, "https://example.com/u/gone", "gone.example-com.tidepool.test", "gone")
	requestContext, disconnect := context.WithCancel(t.Context())
	defer disconnect()
	runs := &disconnectingStartRuns{HandleMigrationRuns: store.NewHandleMigrationRuns(h.db), disconnect: disconnect}
	router, admin := adminRouterWith(t, h.runner, runs, t.Context())
	first := migrationRequest(router, `{"dids":["`+did+`"]}`, true, requestContext)
	waitForAdmin(t, admin)
	progress := readMigrationProgress(t, migrationGetRequest(router, true, t.Context()))
	require.False(t, progress.Running, "orphaned active run; first POST answered %d", first.Code)
	require.Equal(t, http.StatusAccepted, migrationRequest(router, `{}`, true, t.Context()).Code)
	waitForAdmin(t, admin)
	require.Equal(t, http.StatusAccepted, first.Code, first.Body.String())
	require.Equal(t, 0, activeRunCount(t, h))
	assertFinalizedRename(t, h, did, "gone.example-com.tidepool.test", "gone.example.com.ap.tidepool.test")
}

func TestAdminMigrate_ListingFailureFinishesRun(t *testing.T) {
	h := newRenameHarness(t)
	actors := &listingFailingActors{BridgedActors: h.actors}
	router, admin := adminRouterWith(t, h.runnerWithActors(t, actors), store.NewHandleMigrationRuns(h.db), t.Context())
	require.Equal(t, http.StatusAccepted, migrationRequest(router, `{}`, true, t.Context()).Code)
	waitForAdmin(t, admin)
	progress := readMigrationProgress(t, migrationGetRequest(router, true, t.Context()))
	require.False(t, progress.Running, "a run that failed on listing stayed active")
	require.Equal(t, http.StatusAccepted, migrationRequest(router, `{}`, true, t.Context()).Code)
	waitForAdmin(t, admin)
	require.Equal(t, 0, activeRunCount(t, h))
}

func TestAdminMigrate_PersistedActiveRunRejectsPost(t *testing.T) {
	h := newRenameHarness(t)
	const did = "did:plc:persistedactive"
	h.insertOldActor(t, did, "https://example.com/u/held", "held.example-com.tidepool.test", "held")
	_, err := h.db.ExecContext(t.Context(), `INSERT INTO handle_migration_runs (mode, dids) VALUES ('forward', NULL)`)
	require.NoError(t, err)
	router, admin := migrationRouter(t, h, t.Context())
	for _, body := range []string{`{}`, `{"dids":["` + did + `"]}`} {
		response := migrationRequest(router, body, true, t.Context())
		require.Equal(t, http.StatusConflict, response.Code, response.Body.String())
		require.Contains(t, response.Body.String(), "already running")
	}
	waitForAdmin(t, admin)
	requests, hookErrors := h.plc.snapshot()
	require.Empty(t, hookErrors)
	require.Empty(t, requests, "a rejected POST must not start a run")
	require.Equal(t, "held.example-com.tidepool.test", h.row(t, did).Handle)
	require.Equal(t, 1, activeRunCount(t, h))
}
