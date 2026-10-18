package handlemigration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

const migrateToken = "rename-test-token"

func migrationRouter(t *testing.T, h *renameHarness, base context.Context) (http.Handler, *Admin) {
	t.Helper()
	admin, err := NewAdmin(AdminOptions{Token: migrateToken, Runner: h.runner, BaseContext: base,
		Runs: store.NewHandleMigrationRuns(h.db)})
	require.NoError(t, err)
	require.NotNil(t, admin)
	router := chi.NewRouter()
	admin.Routes(router)
	return router, admin
}

func bootMigration(t *testing.T, h *renameHarness, base context.Context) (http.Handler, *Admin) {
	t.Helper()
	runner := h.runnerWithActors(t, h.actors)
	admin, err := NewAdmin(AdminOptions{Token: migrateToken, Runner: runner, BaseContext: base,
		Runs: store.NewHandleMigrationRuns(h.db)})
	require.NoError(t, err)
	router := chi.NewRouter()
	admin.Routes(router)
	require.NoError(t, admin.ResumeActiveRun(base))
	return router, admin
}

func migrationRequest(router http.Handler, body string, authenticated bool, ctx context.Context) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/admin/handles/migrate", bytes.NewBufferString(body)).WithContext(ctx)
	if authenticated {
		req.Header.Set("Authorization", "Bearer "+migrateToken)
	}
	req.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	return recorder
}

func migrationGetRequest(router http.Handler, authenticated bool, ctx context.Context) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/admin/handles/migrate", nil).WithContext(ctx)
	if authenticated {
		req.Header.Set("Authorization", "Bearer "+migrateToken)
	}
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	return recorder
}

func waitForAdmin(t *testing.T, admin *Admin) {
	t.Helper()
	done := make(chan struct{})
	go func() { admin.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("admin Wait did not drain the run")
	}
}

type migrationProgress struct {
	Running           bool           `json:"running"`
	Mode              *string        `json:"mode"`
	StartedAt         *string        `json:"started_at"`
	DidLimited        bool           `json:"did_limited"`
	Remaining         int            `json:"remaining"`
	RollbackRemaining int            `json:"rollback_remaining"`
	Pending           int            `json:"pending"`
	Migrated          int            `json:"migrated"`
	Errors            map[string]int `json:"errors"`
}

func readMigrationProgress(t *testing.T, recorder *httptest.ResponseRecorder) migrationProgress {
	t.Helper()
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	var progress migrationProgress
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &progress))
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &fields))
	for _, key := range []string{"running", "mode", "started_at", "did_limited", "remaining", "rollback_remaining", "pending", "migrated", "errors"} {
		require.Contains(t, fields, key)
	}
	var errors map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(fields["errors"], &errors))
	for _, key := range []string{"plc_rejected", "plc_not_controlled", "handle_invalid", "username_unavailable"} {
		require.Contains(t, errors, key)
	}
	return progress
}

func TestAdminMigrate_GetProgress(t *testing.T) {
	h := newRenameHarness(t)
	router, admin := migrationRouter(t, h, t.Context())
	zero := readMigrationProgress(t, migrationGetRequest(router, true, t.Context()))
	require.Equal(t, migrationProgress{Errors: map[string]int{
		"plc_rejected": 0, "plc_not_controlled": 0, "handle_invalid": 0, "username_unavailable": 0,
	}}, zero)

	const (
		ok        = "did:plc:progressok"
		transient = "did:plc:progresstransient"
	)
	h.insertOldActor(t, "did:plc:progressseeded", "https://example.com/u/seeded", "seeded.example.com.ap.tidepool.test", "seeded")
	_, err := h.db.ExecContext(t.Context(), `UPDATE bridged_actors SET handle_migrated_at = $1 WHERE did = $2`,
		time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC), "did:plc:progressseeded")
	require.NoError(t, err)
	h.insertOldActor(t, ok, "https://example.com/u/ok", "ok.example-com.tidepool.test", "ok")
	h.insertOldActor(t, transient, "https://example.com/u/transient", "transient.example-com.tidepool.test", "transient")
	for _, item := range []struct {
		did, name string
		code      store.HandleMigrationErrorCode
	}{
		{"did:plc:progressrejected", "rejected", store.HandleMigrationPLCRejected},
		{"did:plc:progressuncontrolled", "uncontrolled", store.HandleMigrationPLCNotControlled},
		{"did:plc:progressinvalid", "invalid", store.HandleMigrationHandleInvalid},
		{"did:plc:progressunavailable", "unavailable", store.HandleMigrationUsernameUnavailable},
	} {
		h.insertOldActor(t, item.did, "https://example.com/u/"+item.name, item.name+".example-com.tidepool.test", item.name)
		require.NoError(t, h.actors.RecordHandleMigrationError(t.Context(), item.did, item.code, "seeded failure"))
	}
	h.insertOldActor(t, "did:plc:progressuntouched", "https://example.com/u/untouched", "untouched.example-com.tidepool.test", "untouched")
	deleted := h.insertOldActor(t, "did:plc:progressdeleted", "https://example.com/u/deleted", "deleted.example-com.tidepool.test", "deleted")
	require.NoError(t, h.actors.SetConsentState(t.Context(), deleted.APActorID, store.ConsentStateDeleted))
	h.insertOldActor(t, "did:plc:progresscurrent", "https://example.com/u/current", "current.example.com.ap.tidepool.test", "current")
	h.insertOldActor(t, "did:plc:progressexternal", "https://example.com/u/external", "ext.example.net", "external")

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releasePLC := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releasePLC)
	h.plc.mutex.Lock()
	h.plc.postStatuses = map[string]int{"/" + transient: http.StatusServiceUnavailable}
	h.plc.onRequest = func(request plcRequest, r *http.Request) error {
		if request.Method == http.MethodGet && request.Path == "/"+ok+"/log/last" {
			select {
			case entered <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}
		return nil
	}
	h.plc.mutex.Unlock()
	body := `{"dids":["` + ok + `","` + transient + `"]}`
	require.Equal(t, http.StatusAccepted, migrationRequest(router, body, true, t.Context()).Code)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("run never reached the held PLC request")
	}
	running := readMigrationProgress(t, migrationGetRequest(router, true, t.Context()))
	require.True(t, running.Running)
	require.True(t, running.DidLimited)
	require.NotNil(t, running.StartedAt)
	_, err = time.Parse(time.RFC3339, *running.StartedAt)
	require.NoError(t, err)
	require.Equal(t, http.StatusUnauthorized, migrationGetRequest(router, false, t.Context()).Code)
	releasePLC()
	waitForAdmin(t, admin)
	finished := readMigrationProgress(t, migrationGetRequest(router, true, t.Context()))
	require.Equal(t, migrationProgress{Remaining: 6, RollbackRemaining: 2, Pending: 1, Migrated: 2,
		Errors: map[string]int{"plc_rejected": 1, "plc_not_controlled": 1, "handle_invalid": 1, "username_unavailable": 1}}, finished)
	assertFinalizedRename(t, h, ok, "ok.example-com.tidepool.test", "ok.example.com.ap.tidepool.test")
	require.Equal(t, "transient.example-com.tidepool.test", h.row(t, transient).Handle)
	require.Equal(t, "transient.example.com.ap.tidepool.test", h.row(t, transient).PendingHandle)
	code, _ := migrationFailure(t, h, transient)
	require.False(t, code.Valid)
	requests, hookErrors := h.plc.snapshot()
	require.Empty(t, hookErrors)
	require.Len(t, plcRequestsFor(requests, http.MethodPost, "/"+transient), 5)
}

func TestAdminMigrate_FiltersSixDIDsAfterRequestCancellation(t *testing.T) {
	h := newRenameHarness(t)
	const user = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	const community = "did:plc:44ybard66vv44zksje25o7dz"
	const nobridge = "did:plc:7ikqf3uogk56buxcgy4mbfnb"
	const current = "did:plc:snkscieydkgzpdxfdxu2r7te"
	const rollback = "did:plc:xv7v6cjrqwr7mgzo4i76hehi"
	const deleted = "did:plc:fgc2icd46rkay7c2iaxbmfci"
	h.insertOldActor(t, user, "https://example.com/u/alice", "alice.example-com.tidepool.test", "alice")
	group, err := h.actors.UpsertActor(t.Context(), store.BridgedActor{APActorID: "https://example.com/c/garden", ActorType: store.ActorTypeGroup,
		DID: community, Handle: "garden.example-com.tidepool.test", PreferredUsername: "garden", ConsentState: store.ConsentStateOK})
	require.NoError(t, err)
	require.NotNil(t, group)
	nobridgeActor := h.insertOldActor(t, nobridge, "https://example.com/u/nobridge", "nobridge.example-com.tidepool.test", "nobridge")
	require.NoError(t, h.actors.SetConsentState(t.Context(), nobridgeActor.APActorID, store.ConsentStateNoBridge))
	h.insertOldActor(t, current, "https://example.com/u/current", "current.example.com.ap.tidepool.test", "current")
	h.insertOldActor(t, rollback, "https://example.com/u/rollback", "rollback.example.com.ap.tidepool.test", "rollback")
	_, err = h.db.ExecContext(t.Context(), `UPDATE bridged_actors SET pending_handle = $1 WHERE did = $2`, "rollback.example-com.tidepool.test", rollback)
	require.NoError(t, err)
	deletedActor := h.insertOldActor(t, deleted, "https://example.com/u/deleted", "deleted.example-com.tidepool.test", "deleted")
	require.NoError(t, h.actors.SetConsentState(t.Context(), deletedActor.APActorID, store.ConsentStateDeleted))
	router, admin := migrationRouter(t, h, t.Context())
	requestContext, cancel := context.WithCancel(t.Context())
	defer cancel()
	response := migrationRequest(router, `{"dids":["`+user+`","`+community+`","`+nobridge+`","`+current+`","`+rollback+`","`+deleted+`"]}`, true, requestContext)
	require.Equal(t, http.StatusAccepted, response.Code, response.Body.String())
	cancel()
	waitForAdmin(t, admin)
	for did, want := range map[string]string{user: "alice.example.com.ap.tidepool.test", community: "c-garden.example.com.ap.tidepool.test", nobridge: "nobridge.example.com.ap.tidepool.test"} {
		require.Equal(t, want, h.row(t, did).Handle, did)
	}
	for did, want := range map[string]string{current: "current.example.com.ap.tidepool.test", rollback: "rollback.example.com.ap.tidepool.test", deleted: "deleted.example-com.tidepool.test"} {
		require.Equal(t, want, h.row(t, did).Handle, did)
	}
	require.Equal(t, "rollback.example-com.tidepool.test", h.row(t, rollback).PendingHandle)
	requests, _ := h.plc.snapshot()
	for _, request := range requests {
		for _, did := range []string{current, rollback, deleted} {
			require.NotEqual(t, "/"+did, request.Path)
			require.NotEqual(t, "/"+did+"/log/last", request.Path)
		}
	}
}

func TestAdminMigrate_RejectsConcurrentRun(t *testing.T) {
	h := newRenameHarness(t)
	const did = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	h.insertOldActor(t, did, "https://example.com/u/alice", "alice.example-com.tidepool.test", "alice")
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releasePLC := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releasePLC)
	h.plc.onRequest = func(request plcRequest, r *http.Request) error {
		if request.Path == "/"+did+"/log/last" {
			select {
			case entered <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}
		return nil
	}
	router, admin := migrationRouter(t, h, t.Context())
	body := `{"dids":["` + did + `"]}`
	require.Equal(t, http.StatusAccepted, migrationRequest(router, body, true, t.Context()).Code)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first run never reached PLC")
	}
	require.Equal(t, http.StatusConflict, migrationRequest(router, body, true, t.Context()).Code)
	releasePLC()
	waitForAdmin(t, admin)
}

func TestAdminMigrate_ValidatesBodyAndBearer(t *testing.T) {
	h := newRenameHarness(t)
	router, admin := migrationRouter(t, h, t.Context())
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{"missing dids", `{}`, http.StatusAccepted},
		{"null dids", `{"dids":null}`, http.StatusAccepted},
		{"empty body", ``, http.StatusAccepted},
		{"explicit forward", `{"rollback":false}`, http.StatusAccepted},
		{"empty dids", `{"dids":[]}`, http.StatusBadRequest},
		{"malformed JSON", `{`, http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.status, migrationRequest(router, tc.body, true, t.Context()).Code)
			waitForAdmin(t, admin)
		})
	}
	const did = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	h.insertOldActor(t, did, "https://example.com/u/alice", "alice.example-com.tidepool.test", "alice")
	body := `{"dids":["` + did + `"]}`
	require.Equal(t, http.StatusUnauthorized, migrationRequest(router, body, false, t.Context()).Code)
	wrongToken := httptest.NewRequest(http.MethodPost, "/admin/handles/migrate", bytes.NewBufferString(body)).WithContext(t.Context())
	wrongToken.Header.Set("Authorization", "Bearer wrong-token")
	wrongToken.Header.Set("Content-Type", "application/json")
	wrongTokenResponse := httptest.NewRecorder()
	router.ServeHTTP(wrongTokenResponse, wrongToken)
	require.Equal(t, http.StatusUnauthorized, wrongTokenResponse.Code)
	waitForAdmin(t, admin)
	requests, _ := h.plc.snapshot()
	require.Empty(t, requests, "a rejected bearer must not start a run")
	require.Equal(t, "alice.example-com.tidepool.test", h.row(t, did).Handle)
}

func TestAdminMigrate_RejectsUnknownFieldsAndNullRollback(t *testing.T) {
	h := newRenameHarness(t)
	router, admin := migrationRouter(t, h, t.Context())
	const did = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	h.insertOldActor(t, did, "https://example.com/u/alice", "alice.example-com.tidepool.test", "alice")
	for _, tc := range []struct{ name, body string }{
		{"mode key", `{"mode":"rollback"}`},
		{"misspelled rollback key", `{"roll_back":true}`},
		{"null rollback", `{"rollback":null}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, http.StatusBadRequest, migrationRequest(router, tc.body, true, t.Context()).Code)
			waitForAdmin(t, admin)
		})
	}
	var runCount int
	require.NoError(t, h.db.QueryRowContext(t.Context(), `SELECT count(*) FROM handle_migration_runs`).Scan(&runCount))
	require.Zero(t, runCount, "a rejected body must not persist a run")
	requests, _ := h.plc.snapshot()
	require.Empty(t, requests, "a rejected body must not start a run")
	require.Equal(t, "alice.example-com.tidepool.test", h.row(t, did).Handle)
}

func TestAdminMigrate_BaseContextCancellationDrains(t *testing.T) {
	h := newRenameHarness(t)
	const first = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	const second = "did:plc:44ybard66vv44zksje25o7dz"
	h.insertOldActor(t, first, "https://example.com/u/alice", "alice.example-com.tidepool.test", "alice")
	h.insertOldActor(t, second, "https://example.com/u/bob", "bob.example-com.tidepool.test", "bob")
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	h.plc.onRequest = func(request plcRequest, r *http.Request) error {
		if request.Path == "/"+first+"/log/last" {
			select {
			case entered <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}
		return nil
	}
	base, cancel := context.WithCancel(t.Context())
	defer cancel()
	router, admin := migrationRouter(t, h, base)
	response := migrationRequest(router, `{"dids":["`+first+`","`+second+`"]}`, true, t.Context())
	require.Equal(t, http.StatusAccepted, response.Code)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first run never reached PLC")
	}
	cancel()
	waitForAdmin(t, admin)
	requests, _ := h.plc.snapshot()
	for _, request := range requests {
		require.NotEqual(t, "/"+second, request.Path)
		require.NotEqual(t, "/"+second+"/log/last", request.Path)
	}
}
