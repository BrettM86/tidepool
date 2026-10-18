package materialize

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

func TestRealMinter_MaterializeActorsOnSameHostWithDifferentPorts(t *testing.T) {
	h := newRealMinterHarness(t)
	ctx := t.Context()
	// The transport preserves the original Host, including the port.
	h.mux.HandleFunc("GET /u/alice", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", ap.ContentTypeActivityJSON)
		_ = json.NewEncoder(w).Encode(person("https://"+r.Host+"/u/alice", "alice", nil))
	})
	for _, tc := range []struct{ id, want string }{
		{"https://example.com/u/alice", "alice.example.com.ap.tidepool.test"},
		{"https://example.com:8443/u/alice", "alice-2.example.com.ap.tidepool.test"},
	} {
		t.Run(tc.id, func(t *testing.T) {
			_, err := h.m.EnsureActor(ctx, &ap.Object{ID: tc.id})
			require.NoError(t, err)
			stored, err := h.actors.GetByAPActorID(ctx, tc.id)
			require.NoError(t, err)
			assert.Equal(t, tc.want, stored.Handle)
		})
	}
}

func TestRealMinter_MintUpsertStoresOriginalPreferredUsername(t *testing.T) {
	for _, tc := range []struct {
		name, id, username string
		actorType          store.ActorType
	}{
		{"person", "https://lemmy.world/u/Alice_B", "Alice_B", store.ActorTypePerson},
		{"group", "https://lemmy.world/c/Rust_Lang", "Rust_Lang", store.ActorTypeGroup},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRealMinterHarness(t)
			bridged, inserted, err := h.m.mintAndUpsert(t.Context(), &ap.Object{
				ID: tc.id, PreferredUsername: tc.username,
			}, tc.actorType, "lemmy.world")
			require.NoError(t, err)
			require.True(t, inserted)
			require.NotNil(t, bridged)
			stored, err := h.actors.GetByAPActorID(t.Context(), tc.id)
			require.NoError(t, err)
			assert.Equal(t, tc.username, stored.PreferredUsername)
		})
	}
}

func TestRealMinter_NULPreferredUsernameIsSkippedBeforeMint(t *testing.T) {
	for _, tc := range []struct {
		name, id, path, username string
		groupActor               bool
	}{
		{"person", "https://lemmy.world/u/alice", "/u/alice", "ali\x00ce", false},
		{"group", "https://lemmy.world/c/rust", "/c/rust", "ru\x00st", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, plcRequests := newRealMinterHarnessCountingPLCRequests(t)
			ctx := t.Context()
			var doc map[string]any
			if tc.groupActor {
				doc = group(tc.id, tc.username, nil)
			} else {
				doc = person(tc.id, tc.username, nil)
			}
			h.mux.HandleFunc("GET "+tc.path, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", ap.ContentTypeActivityJSON)
				_ = json.NewEncoder(w).Encode(doc)
			})
			ref := &ap.Object{ID: tc.id}
			var err error
			if tc.groupActor {
				_, err = h.m.EnsureCommunity(ctx, ref)
			} else {
				_, err = h.m.EnsureActor(ctx, ref)
			}
			require.True(t, IsSkip(err), "got %v", err)
			assert.Zero(t, plcRequests.Load(), "no PLC operation may be submitted")
			_, err = h.actors.GetByAPActorID(ctx, tc.id)
			assert.True(t, errors.IsNotFound(err), "got %v", err)
		})
	}
}

func TestRealMinter_ProfileSyncPreservesOrReplacesPreferredUsername(t *testing.T) {
	const (
		personID, personPath = "https://lemmy.world/u/alice", "/u/alice"
		groupID, groupPath   = "https://lemmy.world/c/rust", "/c/rust"
	)
	for _, tc := range []struct {
		name, id, path, initialUsername, refreshedUsername string
		groupActor, nobridge                               bool
		storedBefore                                       any // nil (NULL) or "stale"
		wantStored                                         string
	}{
		{"person/forced/replace-null", personID, personPath, "alice", "Alice_B", false, false, nil, "Alice_B"},
		{"person/forced/replace-stale", personID, personPath, "alice", "Alice_B", false, false, "stale", "Alice_B"},
		{"person/forced/empty", personID, personPath, "alice", "", false, false, "stale", "stale"},
		{"person/forced/nul", personID, personPath, "alice", "Ali\x00ce", false, false, "stale", "stale"},
		{"person/nobridge/replace", personID, personPath, "alice", "Alice_B", false, true, "stale", "Alice_B"},
		{"person/nobridge/empty", personID, personPath, "alice", "", false, true, "stale", "stale"},
		{"person/nobridge/nul", personID, personPath, "alice", "Ali\x00ce", false, true, "stale", "stale"},
		{"group/forced/replace-null", groupID, groupPath, "rust", "Rust_Lang", true, false, nil, "Rust_Lang"},
		{"group/forced/replace-stale", groupID, groupPath, "rust", "Rust_Lang", true, false, "stale", "Rust_Lang"},
		{"group/forced/empty", groupID, groupPath, "rust", "", true, false, "stale", "stale"},
		{"group/forced/nul", groupID, groupPath, "rust", "Ru\x00st", true, false, "stale", "stale"},
		{"group/nobridge/replace", groupID, groupPath, "rust", "Rust_Lang", true, true, "stale", "Rust_Lang"},
		{"group/nobridge/empty", groupID, groupPath, "rust", "", true, true, "stale", "stale"},
		{"group/nobridge/nul", groupID, groupPath, "rust", "Ru\x00st", true, true, "stale", "stale"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRealMinterHarness(t)
			ctx := context.Background()
			clock := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			h.m.now = func() time.Time { return clock }
			var doc map[string]any
			if tc.groupActor {
				doc = group(tc.id, tc.initialUsername, nil)
			} else {
				doc = person(tc.id, tc.initialUsername, nil)
			}
			h.mux.HandleFunc("GET "+tc.path, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", ap.ContentTypeActivityJSON)
				_ = json.NewEncoder(w).Encode(doc)
			})
			ref := &ap.Object{ID: tc.id}
			var err error
			if tc.groupActor {
				_, err = h.m.EnsureCommunity(ctx, ref)
			} else {
				_, err = h.m.EnsureActor(ctx, ref)
			}
			require.NoError(t, err)
			before, err := h.actors.GetByAPActorID(ctx, tc.id)
			require.NoError(t, err)
			require.NotNil(t, before.ProfileSyncedAt)

			_, err = testutil.DB(t).ExecContext(ctx,
				`UPDATE bridged_actors SET preferred_username = $1 WHERE ap_actor_id = $2`, tc.storedBefore, tc.id)
			require.NoError(t, err)
			if tc.nobridge {
				require.NoError(t, h.actors.SetConsentState(ctx, tc.id, store.ConsentStateNoBridge))
				doc["summary"] = "#nobridge"
			}
			doc["preferredUsername"] = tc.refreshedUsername
			clock = clock.Add(time.Minute)
			if tc.groupActor {
				_, err = h.m.RefreshCommunity(ctx, ref)
			} else {
				_, err = h.m.RefreshActor(ctx, ref)
			}
			if tc.nobridge {
				require.True(t, IsSkip(err), "got %v", err)
			} else {
				require.NoError(t, err)
			}
			stored, err := h.actors.GetByAPActorID(ctx, tc.id)
			require.NoError(t, err)
			assert.Equal(t, tc.wantStored, stored.PreferredUsername)
			require.NotNil(t, stored.ProfileSyncedAt)
			assert.True(t, stored.ProfileSyncedAt.After(*before.ProfileSyncedAt))
		})
	}
}
