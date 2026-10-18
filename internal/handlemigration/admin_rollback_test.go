package handlemigration

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAdminMigrate_RollbackLimitedRunResumesAtBoot(t *testing.T) {
	h := newRenameHarness(t)
	for _, actor := range []struct{ did, name string }{
		{"did:plc:rollbackone", "one"},
		{"did:plc:rollbacktwo", "two"},
		{"did:plc:rollbackthree", "three"},
		{"did:plc:rollbackfour", "four"},
	} {
		h.insertOldActor(t, actor.did, "https://example.com/u/"+actor.name,
			actor.name+".example-com.tidepool.test", actor.name)
	}
	require.NoError(t, h.runner.Run(t.Context(), nil))
	beforeRollback, hookErrors := h.plc.snapshot()
	require.Empty(t, hookErrors)
	base, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered := cancelAtPLC(t, h, "did:plc:rollbacktwo", cancel)
	router, admin := migrationRouter(t, h, base)
	body := `{"rollback":true,"dids":["did:plc:rollbackone","did:plc:rollbacktwo","did:plc:rollbackthree"]}`
	require.Equal(t, http.StatusAccepted, migrationRequest(router, body, true, t.Context()).Code)
	awaitPLC(t, entered)
	waitForAdmin(t, admin)
	first := h.row(t, "did:plc:rollbackone")
	require.Equal(t, "one.example-com.tidepool.test", first.Handle)
	require.Equal(t, "one.example.com.ap.tidepool.test", first.PreviousHandle)
	require.Empty(t, first.PendingHandle)
	second := h.row(t, "did:plc:rollbacktwo")
	require.Equal(t, "two.example.com.ap.tidepool.test", second.Handle)
	require.Equal(t, "two.example-com.tidepool.test", second.PendingHandle)
	interrupted, hookErrors := h.plc.snapshot()
	require.Empty(t, hookErrors)
	require.Equal(t, []string{"/did:plc:rollbackone"}, postPaths(interrupted[len(beforeRollback):]))
	require.Empty(t, plcRequestsFor(interrupted[len(beforeRollback):], http.MethodPost, "/did:plc:rollbacktwo"))
	clearPLCHook(h)
	resumedRouter, resumed := bootMigration(t, h, t.Context())
	waitForAdmin(t, resumed)
	after, hookErrors := h.plc.snapshot()
	require.Empty(t, hookErrors)
	require.Equal(t, []string{"/did:plc:rollbacktwo", "/did:plc:rollbackthree"}, postPaths(after[len(interrupted):]))
	for _, posted := range []struct{ did, old string }{
		{"did:plc:rollbacktwo", "two.example-com.tidepool.test"},
		{"did:plc:rollbackthree", "three.example-com.tidepool.test"},
	} {
		posts := plcRequestsFor(after[len(interrupted):], http.MethodPost, "/"+posted.did)
		require.Len(t, posts, 1)
		require.Equal(t, []any{"at://" + posted.old}, posts[0].Body["alsoKnownAs"])
	}
	for _, request := range after[len(beforeRollback):] {
		require.NotContains(t, request.Path, "/did:plc:rollbackfour")
	}
	for _, actor := range []struct{ did, name string }{
		{"did:plc:rollbackone", "one"}, {"did:plc:rollbacktwo", "two"}, {"did:plc:rollbackthree", "three"},
	} {
		row := h.row(t, actor.did)
		require.Equal(t, actor.name+".example-com.tidepool.test", row.Handle)
		require.Equal(t, actor.name+".example.com.ap.tidepool.test", row.PreviousHandle)
		require.Empty(t, row.PendingHandle)
		require.Equal(t, []string{actor.name + ".example.com.ap.tidepool.test", actor.name + ".example-com.tidepool.test"}, h.identityHandles(t, actor.did))
	}
	fourth := h.row(t, "did:plc:rollbackfour")
	require.Equal(t, "four.example.com.ap.tidepool.test", fourth.Handle)
	require.Equal(t, "four.example-com.tidepool.test", fourth.PreviousHandle)
	require.Equal(t, []string{"four.example.com.ap.tidepool.test"}, h.identityHandles(t, "did:plc:rollbackfour"))
	require.Equal(t, http.StatusAccepted, migrationRequest(resumedRouter, body, true, t.Context()).Code)
	waitForAdmin(t, resumed)
	final, hookErrors := h.plc.snapshot()
	require.Empty(t, hookErrors)
	require.Len(t, final, len(after), "repeated rollback must make no PLC request")
	for _, actor := range []struct{ did, name string }{
		{"did:plc:rollbackone", "one"}, {"did:plc:rollbacktwo", "two"}, {"did:plc:rollbackthree", "three"},
	} {
		require.Equal(t, []string{actor.name + ".example.com.ap.tidepool.test", actor.name + ".example-com.tidepool.test"}, h.identityHandles(t, actor.did))
	}
}

func TestAdminMigrate_RollbackModeAndMutualExclusion(t *testing.T) {
	t.Run("forward running", func(t *testing.T) {
		h := newRenameHarness(t)
		h.insertOldActor(t, "did:plc:modeforward", "https://example.com/u/forward", "forward.example-com.tidepool.test", "forward")
		entered, release := holdAtPLC(t, h, "did:plc:modeforward")
		router, admin := migrationRouter(t, h, t.Context())
		require.Equal(t, http.StatusAccepted, migrationRequest(router, `{}`, true, t.Context()).Code)
		awaitPLC(t, entered)
		require.Equal(t, http.StatusConflict, migrationRequest(router, `{"rollback":true}`, true, t.Context()).Code)
		progress := readMigrationProgress(t, migrationGetRequest(router, true, t.Context()))
		require.True(t, progress.Running)
		require.NotNil(t, progress.Mode)
		require.Equal(t, "forward", *progress.Mode)
		release()
		waitForAdmin(t, admin)
	})
	t.Run("rollback running", func(t *testing.T) {
		h := newRenameHarness(t)
		for _, actor := range []struct{ did, name, handle, previous, pending string }{
			{"did:plc:moderenamed", "renamed", "renamed.example.com.ap.tidepool.test", "renamed.example-com.tidepool.test", ""},
			{"did:plc:modemidrollback", "midrollback", "midrollback.example.com.ap.tidepool.test", "midrollback.example-com.tidepool.test", "midrollback.example-com.tidepool.test"},
			{"did:plc:modemidforwardone", "midforwardone", "midforwardone.example-com.tidepool.test", "", "midforwardone.example.com.ap.tidepool.test"},
			{"did:plc:modemidforwardtwo", "midforwardtwo", "midforwardtwo.example-com.tidepool.test", "", "midforwardtwo.example.com.ap.tidepool.test"},
			{"did:plc:modemint", "mint", "mint.example.com.ap.tidepool.test", "", ""},
			{"did:plc:moderolledback", "rolledback", "rolledback.example-com.tidepool.test", "rolledback.example.com.ap.tidepool.test", ""},
			{"did:plc:modenever", "never", "never.example-com.tidepool.test", "", ""},
		} {
			h.insertOldActor(t, actor.did, "https://example.com/u/"+actor.name, actor.handle, actor.name)
			if actor.previous != "" {
				_, err := h.db.ExecContext(t.Context(), `UPDATE bridged_actors SET previous_handle = $1 WHERE did = $2`, actor.previous, actor.did)
				require.NoError(t, err)
			}
			if actor.pending != "" {
				require.NoError(t, h.actors.SetPendingHandle(t.Context(), actor.did, actor.pending))
			}
		}
		entered, release := holdAtPLC(t, h, "") // first PLC GET, whichever candidate runs first
		router, admin := migrationRouter(t, h, t.Context())
		require.Equal(t, http.StatusAccepted, migrationRequest(router, `{"rollback":true}`, true, t.Context()).Code)
		awaitPLC(t, entered)
		require.Equal(t, http.StatusConflict, migrationRequest(router, `{}`, true, t.Context()).Code)
		progress := readMigrationProgress(t, migrationGetRequest(router, true, t.Context()))
		require.True(t, progress.Running)
		require.NotNil(t, progress.Mode)
		require.Equal(t, "rollback", *progress.Mode)
		require.Equal(t, 4, progress.RollbackRemaining)
		release()
		waitForAdmin(t, admin)
	})
	t.Run("idle", func(t *testing.T) {
		h := newRenameHarness(t)
		router, _ := migrationRouter(t, h, t.Context())
		progress := readMigrationProgress(t, migrationGetRequest(router, true, t.Context()))
		require.False(t, progress.Running)
		require.Nil(t, progress.Mode)
	})
}
