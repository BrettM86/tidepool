package handlemigration

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"tidepool/internal/identity"
	"tidepool/internal/store"
)

type observeUsernameBeforeReservation struct {
	store.BridgedActors
	database *sql.DB
	test     *testing.T
	checked  bool
}

func (a *observeUsernameBeforeReservation) SetPendingHandle(ctx context.Context, did, handle string) error {
	a.test.Helper()
	var preferred string
	require.NoError(a.test, a.database.QueryRowContext(ctx,
		`SELECT COALESCE(preferred_username, '') FROM bridged_actors WHERE did = $1`, did).Scan(&preferred))
	require.Equal(a.test, "Alice_B", preferred, "preferred username must be committed before reservation")
	a.checked = true
	return a.BridgedActors.SetPendingHandle(ctx, did, handle)
}

func TestRunner_FetchesMissingPreferredUsernameBeforeReservation(t *testing.T) {
	const (
		did       = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
		apID      = "https://lemmy.example/users/8f2c"
		oldHandle = "alice.lemmy-example.tidepool.test"
		newHandle = "alice-b.lemmy.example.ap.tidepool.test"
	)
	var observer *observeUsernameBeforeReservation
	h := newRenameHarnessWithActors(t, func(actors store.BridgedActors) store.BridgedActors {
		observer = &observeUsernameBeforeReservation{BridgedActors: actors, test: t}
		return observer
	})
	observer.database = h.db
	h.actorDocs.handlers["/users/8f2c"] = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/activity+json")
		_, _ = w.Write([]byte(`{"type":"Person","id":"https://lemmy.example/users/8f2c","preferredUsername":"Alice_B","icon":{"type":"Image","url":"https://lemmy.example/icon.png"},"summary":"#nobridge"}`))
	}
	h.actorDocs.handlers["/icon.png"] = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		w.WriteHeader(http.StatusOK)
	}
	actor := h.insertOldActor(t, did, apID, oldHandle, "")
	require.NoError(t, h.actors.MarkProfileSynced(t.Context(), apID,
		time.Date(2026, time.October, 1, 12, 0, 0, 0, time.UTC), ""))
	var beforeSynced sql.NullTime
	var beforeConsent string
	require.NoError(t, h.db.QueryRowContext(t.Context(),
		`SELECT profile_synced_at, consent_state FROM bridged_actors WHERE did = $1`, did).
		Scan(&beforeSynced, &beforeConsent))
	h.actorDocs.onArrival = func(request actorRequest) error {
		if request.Method != http.MethodGet || request.Path != "/users/8f2c" {
			return nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		var pending string
		if err := h.db.QueryRowContext(ctx,
			`SELECT COALESCE(pending_handle, '') FROM bridged_actors WHERE did = $1`, did).Scan(&pending); err != nil {
			return err
		}
		if pending != "" {
			return fmt.Errorf("pending handle at actor GET: %q", pending)
		}
		rows, err := h.db.QueryContext(ctx, `SELECT handle FROM handle_claims WHERE actor_id = $1 ORDER BY handle`, actor.ID)
		if err != nil {
			return err
		}
		var claims []string
		for rows.Next() {
			var handle string
			if err := rows.Scan(&handle); err != nil {
				_ = rows.Close()
				return err
			}
			claims = append(claims, handle)
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return err
		}
		if len(claims) != 1 || claims[0] != oldHandle {
			return fmt.Errorf("claims at actor GET: %v", claims)
		}
		requests, _ := h.plc.snapshot()
		if len(requests) != 0 {
			return fmt.Errorf("PLC requests at actor GET: %v", requests)
		}
		return nil
	}
	h.reservationAtPLC(did, newHandle, actor.ID)
	runner := h.runnerWithActors(t, observer)
	require.NoError(t, runner.Run(t.Context(), []string{did}))
	actorRequests, actorHookErrors := h.actorDocs.snapshot()
	require.Equal(t, []actorRequest{{Method: http.MethodGet, Path: "/users/8f2c"}}, actorRequests,
		"exactly one actor GET and no icon fetch")
	require.Empty(t, actorHookErrors)
	require.True(t, observer.checked, "reservation must follow the committed username")
	plcRequests, plcHookErrors := h.plc.snapshot()
	require.NotEmpty(t, plcRequests)
	require.Empty(t, plcHookErrors)
	assertFinalizedRename(t, h, did, oldHandle, newHandle)
	var preferred, afterConsent string
	var afterSynced sql.NullTime
	require.NoError(t, h.db.QueryRowContext(t.Context(),
		`SELECT COALESCE(preferred_username, ''), profile_synced_at, consent_state FROM bridged_actors WHERE did = $1`, did).
		Scan(&preferred, &afterSynced, &afterConsent))
	require.Equal(t, "Alice_B", preferred)
	require.Equal(t, beforeSynced, afterSynced)
	require.Equal(t, beforeConsent, afterConsent)
	var commits int
	require.NoError(t, h.db.QueryRowContext(t.Context(),
		`SELECT count(*) FROM firehose_events WHERE kind = 'commit' AND did = $1`, did).Scan(&commits))
	require.Zero(t, commits)
}

func TestRunner_CancelDuringActorFetchStopsWithoutRecordingFailure(t *testing.T) {
	const (
		did       = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
		oldHandle = "alice.my-lemmy-org.tidepool.test"
		actorPath = "/u/alice"
	)
	h := newRenameHarness(t)
	h.insertOldActor(t, did, "https://my-lemmy.org"+actorPath, oldHandle, "")
	arrived := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	var arrivedOnce sync.Once
	h.actorDocs.handlers[actorPath] = func(w http.ResponseWriter, r *http.Request) {
		arrivedOnce.Do(func() { close(arrived) })
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		select {
		case <-arrived:
			cancel()
		case <-ctx.Done():
		}
	}()
	err := h.runner.Run(ctx, []string{did})
	require.True(t, errors.Is(err, context.Canceled), "Run must return context.Canceled, got %v", err)
	code, message := migrationFailure(t, h, did)
	require.False(t, code.Valid, "a cancelled fetch must not record a failure")
	require.False(t, message.Valid)
	row := h.row(t, did)
	require.Equal(t, oldHandle, row.Handle)
	require.Empty(t, row.PendingHandle)
	requests, hookErrors := h.actorDocs.snapshot()
	require.Empty(t, hookErrors)
	require.Equal(t, []actorRequest{{Method: http.MethodGet, Path: actorPath}}, requests)
	plcRequests, _ := h.plc.snapshot()
	require.Empty(t, plcRequests)
}

type unusedActors struct{ store.BridgedActors }
type unusedIdentity struct{ Identity }

func TestNewRunner_RequiresActorFetcher(t *testing.T) {
	clock := newVirtualClock()
	_, err := NewRunner(Options{
		Actors: unusedActors{}, Identity: unusedIdentity{},
		BridgeHostname: "tidepool.test", HandleZoneRoot: "ap.tidepool.test",
		ActorsPerSecond: 2, Now: clock.Now, Wait: clock.Wait,
	})
	require.Error(t, err)
}

func TestRunner_StoredPendingHandleNeedsNoActorFetch(t *testing.T) {
	const (
		did       = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
		oldHandle = "alice.my-lemmy-org.tidepool.test"
		pending   = "alice.my-lemmy.org.ap.tidepool.test"
	)
	h := newRenameHarness(t)
	h.insertOldActor(t, did, "https://my-lemmy.org/u/alice", oldHandle, "")
	require.NoError(t, h.actors.SetPendingHandle(t.Context(), did, pending))
	require.NoError(t, h.runner.Run(t.Context(), []string{did}))
	requests, hookErrors := h.actorDocs.snapshot()
	require.Empty(t, requests)
	require.Empty(t, hookErrors)
	assertFinalizedRename(t, h, did, oldHandle, pending)
}

func TestRunner_UnavailableActorUsernameKeepsOldHandleAndContinues(t *testing.T) {
	const (
		firstDID  = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
		secondDID = "did:plc:44ybard66vv44zksje25o7dz"
		firstOld  = "alice.my-lemmy-org.tidepool.test"
		secondOld = "bob.my-lemmy-org.tidepool.test"
		secondNew = "bob.my-lemmy.org.ap.tidepool.test"
		actorPath = "/u/alice"
	)
	const notFoundOrRefused = "actor document not found or access refused (HTTP 401/403/404)"
	for _, tc := range []struct {
		name               string
		actorID            string
		body               string
		status             int
		redirectTo         string
		timeout            bool
		blockPrivate       bool
		message            string
		loggedError        string
		loggedErrorPattern string
		messageExcludes    string
	}{
		{name: "HTTP 500", status: http.StatusInternalServerError,
			message: "actor fetch failed: HTTP 500", loggedError: "unexpected status 500"},
		{name: "HTTP 404", status: http.StatusNotFound, message: notFoundOrRefused, loggedError: "not found"},
		{name: "HTTP 401", status: http.StatusUnauthorized, message: notFoundOrRefused, loggedError: "not found"},
		{name: "HTTP 403", status: http.StatusForbidden, message: notFoundOrRefused, loggedError: "not found"},
		{name: "HTTP 410", status: http.StatusGone, message: "actor document tombstoned", loggedError: "tombstoned"},
		// net/http reports a client timeout as "Client.Timeout exceeded" or as a
		// bare "context deadline exceeded", depending on which deadline fires first.
		{name: "timeout", timeout: true, message: "actor fetch timed out",
			loggedErrorPattern: `Client\.Timeout exceeded|context deadline exceeded`},
		{name: "different document id", body: `{"type":"Person","id":"https://my-lemmy.org/u/someone-else","preferredUsername":"alice"}`,
			message: "actor document id mismatch", messageExcludes: "someone-else"},
		{name: "missing preferredUsername", body: `{"type":"Person","id":"https://my-lemmy.org/u/alice"}`,
			message: "actor document has no preferredUsername"},
		{name: "NUL in preferredUsername", body: `{"type":"Person","id":"https://my-lemmy.org/u/alice","preferredUsername":"al\u0000ice"}`,
			message: "actor document has invalid preferredUsername"},
		{name: "non-actor document", body: `{"type":"SENTINEL-TYPE-7f3a","id":"https://my-lemmy.org/u/alice","preferredUsername":"alice"}`,
			message: "actor document is not an actor", messageExcludes: "SENTINEL-TYPE-7f3a"},
		{name: "egress guard refusal", actorID: "http://127.0.0.1/u/alice", blockPrivate: true,
			message: "actor fetch refused: invalid or disallowed URL", loggedError: "validation error on field 'url'"},
		{name: "off-origin redirect", redirectTo: "https://elsewhere.example/u/alice-copy",
			message: "actor fetch refused: redirect left origin or downgraded", loggedError: "validation error on field 'redirect'",
			messageExcludes: "mallory"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRenameHarness(t)
			var logs bytes.Buffer
			h.logger = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
			firstActorID := tc.actorID
			if firstActorID == "" {
				firstActorID = "https://my-lemmy.org/u/alice"
			}
			first := h.insertOldActor(t, firstDID, firstActorID, firstOld, "")
			h.insertOldActor(t, secondDID, "https://my-lemmy.org/u/bob", secondOld, "bob")
			unblock := make(chan struct{})
			t.Cleanup(func() { close(unblock) })
			h.actorDocs.handlers[actorPath] = func(w http.ResponseWriter, r *http.Request) {
				if tc.timeout {
					select {
					case <-r.Context().Done():
					case <-unblock:
					}
					return
				}
				if tc.redirectTo != "" {
					http.Redirect(w, r, tc.redirectTo, http.StatusFound)
					return
				}
				if tc.status != 0 {
					w.WriteHeader(tc.status)
					return
				}
				w.Header().Set("Content-Type", "application/activity+json")
				_, _ = w.Write([]byte(tc.body))
			}
			// The off-origin redirect target claims the original actor's id.
			h.actorDocs.handlers["/u/alice-copy"] = func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/activity+json")
				_, _ = w.Write([]byte(`{"type":"Person","id":"https://my-lemmy.org/u/alice","preferredUsername":"mallory"}`))
			}
			if tc.blockPrivate {
				h.actorClient = h.actorFetcher(false)
			}
			plcRequestsBefore, _ := h.plc.snapshot()
			require.Empty(t, plcRequestsBefore)
			require.NoError(t, h.runnerWithActors(t, h.actors).Run(t.Context(), []string{firstDID, secondDID}))
			code, message := migrationFailure(t, h, firstDID)
			require.Equal(t, sql.NullString{String: "username_unavailable", Valid: true}, code)
			require.Equal(t, sql.NullString{String: tc.message, Valid: true}, message)
			requireFetchFailureWarning(t, logs.Bytes(), firstDID, tc.message, tc.loggedError, tc.loggedErrorPattern)
			if tc.messageExcludes != "" {
				require.NotContains(t, logs.String(), tc.messageExcludes)
			}
			row := h.row(t, firstDID)
			require.Equal(t, firstOld, row.Handle)
			require.Empty(t, row.PendingHandle)
			require.Nil(t, row.HandleMigratedAt)
			var preferred string
			require.NoError(t, h.db.QueryRowContext(t.Context(),
				`SELECT COALESCE(preferred_username, '') FROM bridged_actors WHERE did = $1`, firstDID).Scan(&preferred))
			require.Empty(t, preferred)
			claims, err := h.db.QueryContext(t.Context(),
				`SELECT handle, actor_id FROM handle_claims WHERE actor_id = $1`, first.ID)
			require.NoError(t, err)
			var claimedHandle string
			var owner int64
			require.True(t, claims.Next())
			require.NoError(t, claims.Scan(&claimedHandle, &owner))
			require.Equal(t, firstOld, claimedHandle)
			require.Equal(t, first.ID, owner)
			require.False(t, claims.Next(), "the old handle must be the only claim")
			require.NoError(t, claims.Err())
			require.NoError(t, claims.Close())
			plcRequests, plcHookErrors := h.plc.snapshot()
			require.Empty(t, plcHookErrors)
			for _, request := range plcRequests {
				require.False(t, strings.Contains(request.Path, firstDID), "unexpected PLC request for failed actor: %s", request.Path)
			}
			resolved, err := identity.NewStoreResolver(h.actors, "tidepool.test", "did:web:tidepool.test").ResolveHandle(t.Context(), firstOld)
			require.NoError(t, err)
			require.Equal(t, firstDID, resolved)
			actorRequests, actorHookErrors := h.actorDocs.snapshot()
			require.Empty(t, actorHookErrors)
			if tc.blockPrivate {
				require.Empty(t, actorRequests, "the egress guard must refuse before reaching the fixture")
			} else {
				require.Equal(t, []actorRequest{{Method: http.MethodGet, Path: actorPath}}, actorRequests)
			}
			assertFinalizedRename(t, h, secondDID, secondOld, secondNew)
		})
	}
}

// requireFetchFailureWarning finds the Warn record for a username fetch
// failure. A non-empty loggedError must appear in its "error" attribute, and a
// non-empty loggedErrorPattern must match it.
func requireFetchFailureWarning(t *testing.T, logs []byte, did, reason, loggedError, loggedErrorPattern string) {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(logs))
	for decoder.More() {
		var record map[string]any
		require.NoError(t, decoder.Decode(&record))
		if record["level"] != "WARN" || record["did"] != did || record["reason"] != reason {
			continue
		}
		logged, _ := record["error"].(string)
		if loggedError != "" {
			require.Contains(t, logged, loggedError)
		}
		if loggedErrorPattern != "" {
			require.Regexp(t, loggedErrorPattern, logged)
		}
		return
	}
	t.Fatalf("no Warn record with did %q and reason %q in logs:\n%s", did, reason, logs)
}

func TestRunner_RetriesUnavailableActorUsername(t *testing.T) {
	const (
		did       = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
		oldHandle = "carol.my-lemmy-org.tidepool.test"
		newHandle = "carol.my-lemmy.org.ap.tidepool.test"
		actorPath = "/u/Carol"
	)
	h := newRenameHarness(t)
	h.insertOldActor(t, did, "https://my-lemmy.org"+actorPath, oldHandle, "")
	h.actorDocs.handlers[actorPath] = func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}
	require.NoError(t, h.runner.Run(t.Context(), []string{did}))
	code, message := migrationFailure(t, h, did)
	require.Equal(t, sql.NullString{String: "username_unavailable", Valid: true}, code)
	require.True(t, message.Valid)
	h.actorDocs.mutex.Lock()
	h.actorDocs.handlers[actorPath] = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/activity+json")
		_, _ = w.Write([]byte(`{"type":"Person","id":"https://my-lemmy.org/u/Carol","preferredUsername":"Carol"}`))
	}
	h.actorDocs.mutex.Unlock()
	require.NoError(t, h.runner.Run(t.Context(), []string{did}))
	requests, hookErrors := h.actorDocs.snapshot()
	require.Empty(t, hookErrors)
	require.Equal(t, []actorRequest{{Method: http.MethodGet, Path: actorPath}, {Method: http.MethodGet, Path: actorPath}}, requests)
	assertFinalizedRename(t, h, did, oldHandle, newHandle)
	code, message = migrationFailure(t, h, did)
	require.False(t, code.Valid)
	require.False(t, message.Valid)
	var preferred string
	require.NoError(t, h.db.QueryRowContext(t.Context(),
		`SELECT COALESCE(preferred_username, '') FROM bridged_actors WHERE did = $1`, did).Scan(&preferred))
	require.Equal(t, "Carol", preferred)
}
