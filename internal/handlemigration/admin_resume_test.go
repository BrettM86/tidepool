package handlemigration

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"tidepool/internal/identity"
)

var resumeActors = []struct{ did, name string }{
	{"did:plc:resumeone", "one"},
	{"did:plc:resumetwo", "two"},
	{"did:plc:resumethree", "three"},
	{"did:plc:resumefour", "four"},
	{"did:plc:resumefive", "five"},
}

func seedResumeActors(t *testing.T, h *renameHarness) {
	t.Helper()
	for _, actor := range resumeActors {
		h.insertOldActor(t, actor.did, "https://example.com/u/"+actor.name,
			actor.name+".example-com.tidepool.test", actor.name)
	}
}

// The first arrival cancels the original base context and holds that request
// until its context is done; a subsequent boot cannot inherit this hook.
func cancelAtPLC(t *testing.T, h *renameHarness, did string, cancel context.CancelFunc) <-chan struct{} {
	t.Helper()
	entered := make(chan struct{}, 1)
	var once sync.Once
	h.plc.mutex.Lock()
	h.plc.onRequest = func(request plcRequest, r *http.Request) error {
		if request.Method == http.MethodGet && request.Path == "/"+did+"/log/last" {
			once.Do(func() {
				entered <- struct{}{}
				cancel()
				<-r.Context().Done()
			})
		}
		return nil
	}
	h.plc.mutex.Unlock()
	return entered
}

func holdAtPLC(t *testing.T, h *renameHarness, did string) (<-chan struct{}, func()) {
	t.Helper()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(release) }) }
	t.Cleanup(open)
	var fired sync.Once
	h.plc.mutex.Lock()
	h.plc.onRequest = func(request plcRequest, r *http.Request) error {
		if request.Method == http.MethodGet && (did == "" || request.Path == "/"+did+"/log/last") {
			fired.Do(func() {
				entered <- struct{}{}
				select {
				case <-release:
				case <-r.Context().Done():
				}
			})
		}
		return nil
	}
	h.plc.mutex.Unlock()
	return entered, open
}

func clearPLCHook(h *renameHarness) {
	h.plc.mutex.Lock()
	h.plc.onRequest = nil
	h.plc.mutex.Unlock()
}

func awaitPLC(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("run never reached the held PLC request")
	}
}

func TestAdminMigrate_InterruptedUnlimitedRunResumesAtBoot(t *testing.T) {
	h := newRenameHarness(t)
	seedResumeActors(t, h)
	base, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered := cancelAtPLC(t, h, "did:plc:resumethree", cancel)
	router, admin := migrationRouter(t, h, base)
	require.Equal(t, http.StatusAccepted, migrationRequest(router, `{}`, true, t.Context()).Code)
	awaitPLC(t, entered)
	waitForAdmin(t, admin)
	assertFinalizedRename(t, h, "did:plc:resumeone", "one.example-com.tidepool.test", "one.example.com.ap.tidepool.test")
	assertFinalizedRename(t, h, "did:plc:resumetwo", "two.example-com.tidepool.test", "two.example.com.ap.tidepool.test")
	row := h.row(t, "did:plc:resumethree")
	require.Equal(t, "three.example-com.tidepool.test", row.Handle)
	require.Equal(t, "three.example.com.ap.tidepool.test", row.PendingHandle)
	resolver := identity.NewStoreResolver(h.actors, "tidepool.test", "did:web:tidepool.test")
	resolved, err := resolver.ResolveHandle(t.Context(), row.PendingHandle)
	require.NoError(t, err)
	require.Equal(t, "did:plc:resumethree", resolved)
	for _, actor := range resumeActors[3:] {
		require.Equal(t, actor.name+".example-com.tidepool.test", h.row(t, actor.did).Handle)
	}
	before, hookErrors := h.plc.snapshot()
	require.Empty(t, hookErrors)
	clearPLCHook(h)
	_, resumed := bootMigration(t, h, t.Context())
	waitForAdmin(t, resumed)
	for _, actor := range resumeActors {
		assertFinalizedRename(t, h, actor.did, actor.name+".example-com.tidepool.test", actor.name+".example.com.ap.tidepool.test")
	}
	require.Equal(t, row.PendingHandle, h.row(t, "did:plc:resumethree").Handle)
	after, hookErrors := h.plc.snapshot()
	require.Empty(t, hookErrors)
	require.Equal(t, []string{"/did:plc:resumethree", "/did:plc:resumefour", "/did:plc:resumefive"}, postPaths(after[len(before):]))
	for _, request := range after[len(before):] {
		require.Contains(t, []string{"/did:plc:resumethree", "/did:plc:resumefour", "/did:plc:resumefive"},
			strings.TrimSuffix(request.Path, "/log/last"))
	}
	_, third := bootMigration(t, h, t.Context())
	waitForAdmin(t, third)
	final, _ := h.plc.snapshot()
	require.Len(t, final, len(after), "finished run must not resume on a third boot")
}

func postPaths(requests []plcRequest) []string {
	paths := make([]string, 0, len(requests))
	for _, request := range requests {
		if request.Method == http.MethodPost {
			paths = append(paths, request.Path)
		}
	}
	return paths
}

func TestAdminMigrate_ResumeRunModes(t *testing.T) {
	t.Run("limited run resumes only its remaining DIDs", func(t *testing.T) {
		h := newRenameHarness(t)
		seedResumeActors(t, h)
		base, cancel := context.WithCancel(t.Context())
		defer cancel()
		entered := cancelAtPLC(t, h, "did:plc:resumefour", cancel)
		router, admin := migrationRouter(t, h, base)
		require.Equal(t, http.StatusAccepted, migrationRequest(router, `{"dids":["did:plc:resumetwo","did:plc:resumefour"]}`, true, t.Context()).Code)
		awaitPLC(t, entered)
		waitForAdmin(t, admin)
		assertFinalizedRename(t, h, "did:plc:resumetwo", "two.example-com.tidepool.test", "two.example.com.ap.tidepool.test")
		before, _ := h.plc.snapshot()
		clearPLCHook(h)
		_, resumed := bootMigration(t, h, t.Context())
		waitForAdmin(t, resumed)
		assertFinalizedRename(t, h, "did:plc:resumefour", "four.example-com.tidepool.test", "four.example.com.ap.tidepool.test")
		after, hookErrors := h.plc.snapshot()
		require.Empty(t, hookErrors)
		require.Equal(t, []string{"/did:plc:resumefour"}, postPaths(after[len(before):]))
		for _, actor := range []struct{ did, name string }{resumeActors[0], resumeActors[2], resumeActors[4]} {
			require.Equal(t, actor.name+".example-com.tidepool.test", h.row(t, actor.did).Handle)
			require.Nil(t, h.row(t, actor.did).HandleMigratedAt)
			require.Empty(t, plcRequestsFor(after, http.MethodGet, "/"+actor.did+"/log/last"))
		}
	})
	t.Run("finished run does not resume", func(t *testing.T) {
		h := newRenameHarness(t)
		seedResumeActors(t, h)
		router, admin := migrationRouter(t, h, t.Context())
		require.Equal(t, http.StatusAccepted, migrationRequest(router, `{"dids":["did:plc:resumetwo"]}`, true, t.Context()).Code)
		waitForAdmin(t, admin)
		assertFinalizedRename(t, h, "did:plc:resumetwo", "two.example-com.tidepool.test", "two.example.com.ap.tidepool.test")
		before, _ := h.plc.snapshot()
		resumedRouter, resumed := bootMigration(t, h, t.Context())
		waitForAdmin(t, resumed)
		after, hookErrors := h.plc.snapshot()
		require.Empty(t, hookErrors)
		require.Len(t, after, len(before))
		for _, actor := range []struct{ did, name string }{resumeActors[0], resumeActors[2], resumeActors[3], resumeActors[4]} {
			require.Equal(t, actor.name+".example-com.tidepool.test", h.row(t, actor.did).Handle)
		}
		progress := readMigrationProgress(t, migrationGetRequest(resumedRouter, true, t.Context()))
		require.False(t, progress.Running)
		require.Equal(t, http.StatusAccepted, migrationRequest(resumedRouter, `{"dids":["did:plc:resumeone"]}`, true, t.Context()).Code)
		waitForAdmin(t, resumed)
	})
	t.Run("resumed run reports metadata and rejects POST", func(t *testing.T) {
		for _, mode := range []struct {
			name, body string
			limited    bool
		}{
			{"unlimited", `{}`, false},
			{"DID-limited", `{"dids":["did:plc:resumeone"]}`, true},
		} {
			t.Run(mode.name, func(t *testing.T) {
				h := newRenameHarness(t)
				h.insertOldActor(t, "did:plc:resumeone", "https://example.com/u/one", "one.example-com.tidepool.test", "one")
				base, cancel := context.WithCancel(t.Context())
				defer cancel()
				entered, release := holdAtPLC(t, h, "did:plc:resumeone")
				router, admin := migrationRouter(t, h, base)
				require.Equal(t, http.StatusAccepted, migrationRequest(router, mode.body, true, t.Context()).Code)
				awaitPLC(t, entered)
				original := readMigrationProgress(t, migrationGetRequest(router, true, t.Context()))
				require.True(t, original.Running)
				require.NotNil(t, original.StartedAt)
				_, err := time.Parse(time.RFC3339, *original.StartedAt)
				require.NoError(t, err)
				cancel()
				waitForAdmin(t, admin)
				release()
				clearPLCHook(h)
				resumedEntered, resumedRelease := holdAtPLC(t, h, "did:plc:resumeone")
				resumedRouter, resumed := bootMigration(t, h, t.Context())
				awaitPLC(t, resumedEntered)
				progress := readMigrationProgress(t, migrationGetRequest(resumedRouter, true, t.Context()))
				require.True(t, progress.Running)
				require.Equal(t, original.StartedAt, progress.StartedAt)
				require.Equal(t, mode.limited, progress.DidLimited)
				require.Equal(t, http.StatusConflict, migrationRequest(resumedRouter, mode.body, true, t.Context()).Code)
				resumedRelease()
				waitForAdmin(t, resumed)
				assertFinalizedRename(t, h, "did:plc:resumeone", "one.example-com.tidepool.test", "one.example.com.ap.tidepool.test")
			})
		}
	})
}

func TestAdminMigrate_PersistsRunBeforeAccepted(t *testing.T) {
	h := newRenameHarness(t)
	h.insertOldActor(t, "did:plc:resumeone", "https://example.com/u/one", "one.example-com.tidepool.test", "one")
	router, admin := migrationRouter(t, h, t.Context())
	lock, err := h.db.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	defer lock.Rollback()
	_, err = lock.ExecContext(t.Context(), `LOCK TABLE handle_migration_runs IN SHARE MODE`)
	require.NoError(t, err)
	responses := make(chan int, 1)
	go func() {
		responses <- migrationRequest(router, `{"dids":["did:plc:resumeone"]}`, true, t.Context()).Code
	}()
	require.Eventually(t, func() bool {
		var blocked bool
		err := h.db.QueryRowContext(t.Context(), `SELECT EXISTS (
			SELECT 1 FROM pg_locks AS locks JOIN pg_class AS relations ON relations.oid = locks.relation
			WHERE relations.relname = 'handle_migration_runs' AND locks.mode = 'RowExclusiveLock'
			AND NOT locks.granted AND locks.pid <> pg_backend_pid())`).Scan(&blocked)
		return err == nil && blocked
	}, 5*time.Second, 10*time.Millisecond)
	select {
	case status := <-responses:
		t.Fatalf("POST answered %d before the run was persisted", status)
	default:
	}
	requests, hookErrors := h.plc.snapshot()
	require.Empty(t, hookErrors)
	require.Empty(t, requests)
	require.NoError(t, lock.Rollback())
	select {
	case status := <-responses:
		require.Equal(t, http.StatusAccepted, status)
	case <-time.After(5 * time.Second):
		t.Fatal("POST did not answer after the run table lock was released")
	}
	waitForAdmin(t, admin)
	assertFinalizedRename(t, h, "did:plc:resumeone", "one.example-com.tidepool.test", "one.example.com.ap.tidepool.test")
}
