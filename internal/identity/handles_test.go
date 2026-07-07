package identity

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// fakeResolver backs the HTTP handler unit tests.
type fakeResolver struct {
	handles map[string]string
	err     error
}

func (f *fakeResolver) ResolveHandle(_ context.Context, handle string) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	if did, ok := f.handles[handle]; ok {
		return did, nil
	}
	return "", errors.NewNotFoundError("handle", handle)
}

func testLogger() *slog.Logger { return slog.Default() }

func TestResolveHandleHandler(t *testing.T) {
	resolver := &fakeResolver{handles: map[string]string{
		"alice.lemmy-world.tidepool.example": "did:plc:ewvi7nxzyoun6zhxrhs64oiz",
	}}
	handler := ResolveHandleHandler(resolver, testLogger())

	t.Run("resolves known handle", func(t *testing.T) {
		rec := httptest.NewRecorder()
		handler(rec, httptest.NewRequest(http.MethodGet,
			"/xrpc/com.atproto.identity.resolveHandle?handle=alice.lemmy-world.tidepool.example", nil))
		require.Equal(t, http.StatusOK, rec.Code)
		var out struct {
			Did string `json:"did"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Equal(t, "did:plc:ewvi7nxzyoun6zhxrhs64oiz", out.Did)
	})

	t.Run("strips leading @", func(t *testing.T) {
		rec := httptest.NewRecorder()
		handler(rec, httptest.NewRequest(http.MethodGet,
			"/xrpc/com.atproto.identity.resolveHandle?handle=%40alice.lemmy-world.tidepool.example", nil))
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("unknown handle is 400 HandleNotFound", func(t *testing.T) {
		rec := httptest.NewRecorder()
		handler(rec, httptest.NewRequest(http.MethodGet,
			"/xrpc/com.atproto.identity.resolveHandle?handle=nobody.example.com", nil))
		require.Equal(t, http.StatusBadRequest, rec.Code)
		var out struct {
			Error string `json:"error"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Equal(t, "HandleNotFound", out.Error)
	})

	t.Run("missing parameter is 400 InvalidRequest", func(t *testing.T) {
		rec := httptest.NewRecorder()
		handler(rec, httptest.NewRequest(http.MethodGet,
			"/xrpc/com.atproto.identity.resolveHandle", nil))
		require.Equal(t, http.StatusBadRequest, rec.Code)
		var out struct {
			Error string `json:"error"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Equal(t, "InvalidRequest", out.Error)
	})

	t.Run("internal error is 500", func(t *testing.T) {
		broken := ResolveHandleHandler(&fakeResolver{err: context.DeadlineExceeded}, testLogger())
		rec := httptest.NewRecorder()
		broken(rec, httptest.NewRequest(http.MethodGet,
			"/xrpc/com.atproto.identity.resolveHandle?handle=x.example.com", nil))
		assert.Equal(t, http.StatusInternalServerError, rec.Code)
	})
}

func TestWellKnownDIDHandler(t *testing.T) {
	resolver := &fakeResolver{handles: map[string]string{
		"alice.lemmy-world.tidepool.example": "did:plc:ewvi7nxzyoun6zhxrhs64oiz",
	}}
	handler := WellKnownDIDHandler(resolver, testLogger())

	t.Run("serves DID for known host", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/.well-known/atproto-did", nil)
		req.Host = "alice.lemmy-world.tidepool.example"
		rec := httptest.NewRecorder()
		handler(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "did:plc:ewvi7nxzyoun6zhxrhs64oiz", rec.Body.String())
	})

	t.Run("strips port from host", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/.well-known/atproto-did", nil)
		req.Host = "alice.lemmy-world.tidepool.example:8091"
		rec := httptest.NewRecorder()
		handler(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "did:plc:ewvi7nxzyoun6zhxrhs64oiz", rec.Body.String(),
			"the port-stripped host must resolve to the handle's DID")
	})

	t.Run("unknown host is 404", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/.well-known/atproto-did", nil)
		req.Host = "unknown.tidepool.example"
		rec := httptest.NewRecorder()
		handler(rec, req)
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})
}

func TestStoreResolver(t *testing.T) {
	database := testutil.DB(t)
	testutil.Truncate(t, database, "bridged_actors")
	actors := store.NewBridgedActors(database)
	resolver := NewStoreResolver(actors, "tidepool.example", "did:plc:44ybard66vv44zksje25o7dz")
	ctx := t.Context()

	const (
		did    = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
		handle = "alice.lemmy-world.tidepool.example"
	)
	_, err := actors.UpsertActor(ctx, store.BridgedActor{
		APActorID:    "https://lemmy.world/u/alice",
		ActorType:    store.ActorTypePerson,
		DID:          did,
		Handle:       handle,
		ConsentState: store.ConsentStateOK,
	})
	require.NoError(t, err)

	t.Run("resolves bridged handle", func(t *testing.T) {
		got, err := resolver.ResolveHandle(ctx, handle)
		require.NoError(t, err)
		assert.Equal(t, did, got)
	})

	t.Run("case-insensitive with trailing dot", func(t *testing.T) {
		got, err := resolver.ResolveHandle(ctx, "Alice.Lemmy-World.Tidepool.Example.")
		require.NoError(t, err)
		assert.Equal(t, did, got)
	})

	t.Run("bridge hostname resolves to service DID", func(t *testing.T) {
		got, err := resolver.ResolveHandle(ctx, "tidepool.example")
		require.NoError(t, err)
		assert.Equal(t, "did:plc:44ybard66vv44zksje25o7dz", got)
	})

	t.Run("unknown handle not found", func(t *testing.T) {
		_, err := resolver.ResolveHandle(ctx, "nobody.lemmy-world.tidepool.example")
		assert.True(t, errors.IsNotFound(err))
	})

	t.Run("tombstoned actor stops resolving", func(t *testing.T) {
		require.NoError(t, actors.SetConsentState(ctx, "https://lemmy.world/u/alice", store.ConsentStateDeleted))
		_, err := resolver.ResolveHandle(ctx, handle)
		assert.True(t, errors.IsNotFound(err),
			"a tombstoned actor's handle must not advertise its frozen repo")
	})
}
