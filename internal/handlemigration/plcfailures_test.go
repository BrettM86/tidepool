package handlemigration

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"tidepool/internal/identity"
)

const (
	plcFailureDID = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	plcNextDID    = "did:plc:44ybard66vv44zksje25o7dz"
	plcOldHandle  = "alice.example-com.tidepool.test"
	plcNewHandle  = "alice.example.com.ap.tidepool.test"
)

func signedHead(t *testing.T, key *atcrypto.PrivateKeyK256, original map[string]any, changes map[string]any) map[string]any {
	t.Helper()
	head := make(map[string]any, len(original))
	for field, value := range original {
		head[field] = value
	}
	delete(head, "sig")
	for field, value := range changes {
		head[field] = value
	}
	unsigned, err := atdata.MarshalCBOR(head)
	require.NoError(t, err)
	signature, err := key.HashAndSign(unsigned)
	require.NoError(t, err)
	head["sig"] = base64.RawURLEncoding.EncodeToString(signature)
	return head
}

func headCID(t *testing.T, head map[string]any) string {
	t.Helper()
	encoded, err := atdata.MarshalCBOR(head)
	require.NoError(t, err)
	previous, err := (cid.Prefix{Version: 1, Codec: cid.DagCBOR, MhType: multihash.SHA2_256, MhLength: 32}).Sum(encoded)
	require.NoError(t, err)
	return previous.String()
}

func plcRequestsFor(requests []plcRequest, method, path string) []plcRequest {
	var matching []plcRequest
	for _, request := range requests {
		if request.Method == method && request.Path == path {
			matching = append(matching, request)
		}
	}
	return matching
}

func assertPendingRename(t *testing.T, h *renameHarness, did, old, pending string, actorID int64) {
	t.Helper()
	row := h.row(t, did)
	require.Equal(t, old, row.Handle)
	require.Equal(t, pending, row.PendingHandle)
	require.Empty(t, row.PreviousHandle)
	require.Nil(t, row.HandleMigratedAt)
	owner, claimed := h.claim(t, pending)
	require.True(t, claimed)
	require.Equal(t, actorID, owner)
	require.Empty(t, h.identityHandles(t, did))
}

func assertFinalizedRename(t *testing.T, h *renameHarness, did, old, handle string) {
	t.Helper()
	row := h.row(t, did)
	require.Equal(t, handle, row.Handle)
	require.Equal(t, old, row.PreviousHandle)
	require.Empty(t, row.PendingHandle)
	require.NotNil(t, row.HandleMigratedAt)
	require.Equal(t, []string{handle}, h.identityHandles(t, did))
}

func migrationFailure(t *testing.T, h *renameHarness, did string) (sql.NullString, sql.NullString) {
	t.Helper()
	var code, message sql.NullString
	require.NoError(t, h.db.QueryRowContext(t.Context(),
		`SELECT handle_migration_error, handle_migration_message FROM bridged_actors WHERE did = $1`, did).
		Scan(&code, &message))
	return code, message
}

func TestRunner_FinalizesPLCUpdateAcceptedBeforeCrash(t *testing.T) {
	h := newRenameHarness(t)
	h.insertOldActor(t, plcFailureDID, "https://example.com/u/alice", plcOldHandle, "alice")
	require.NoError(t, h.actors.SetPendingHandle(t.Context(), plcFailureDID, plcNewHandle))
	h.plc.heads = map[string][]map[string]any{"/" + plcFailureDID: {
		signedHead(t, h.rotationKey, h.plc.last, map[string]any{"alsoKnownAs": []any{"at://alice.example.com.ap.tidepool.test"}}),
	}}
	require.NoError(t, h.runner.Run(t.Context(), []string{plcFailureDID}))
	requests, hookErrors := h.plc.snapshot()
	require.Empty(t, hookErrors)
	require.Len(t, plcRequestsFor(requests, http.MethodPost, "/"+plcFailureDID), 0, "already-applied head must not be POSTed again")
	assertFinalizedRename(t, h, plcFailureDID, plcOldHandle, plcNewHandle)
}

func TestRunner_PLCNotControlledKeepsReservationAndContinues(t *testing.T) {
	h := newRenameHarness(t)
	first := h.insertOldActor(t, plcFailureDID, "https://example.com/u/alice", plcOldHandle, "alice")
	h.insertOldActor(t, plcNextDID, "https://example.com/u/bob", "bob.example-com.tidepool.test", "bob")
	foreign, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	public, err := foreign.PublicKey()
	require.NoError(t, err)
	h.plc.heads = map[string][]map[string]any{"/" + plcFailureDID: {
		signedHead(t, foreign, h.plc.last, map[string]any{"rotationKeys": []any{public.DIDKey()}}),
	}}
	require.NoError(t, h.runner.Run(t.Context(), []string{plcFailureDID, plcNextDID}))
	requests, _ := h.plc.snapshot()
	require.Empty(t, plcRequestsFor(requests, http.MethodPost, "/"+plcFailureDID))
	assertPendingRename(t, h, plcFailureDID, plcOldHandle, plcNewHandle, first.ID)
	code, _ := migrationFailure(t, h, plcFailureDID)
	require.Equal(t, sql.NullString{String: "plc_not_controlled", Valid: true}, code)
	assertFinalizedRename(t, h, plcNextDID, "bob.example-com.tidepool.test", "bob.example.com.ap.tidepool.test")
}

func TestRunner_PLCNonOperationHeadIsNotControlledAndContinues(t *testing.T) {
	for _, tc := range []struct {
		name string
		head map[string]any
	}{
		{"tombstone head", map[string]any{"type": "plc_tombstone", "prev": "bafyreiexample", "sig": "signature"}},
		{"legacy create head", map[string]any{
			"type": "create", "signingKey": "did:key:zTestSigningKey", "recoveryKey": "did:key:zTestSigningKey",
			"handle": "alice.example.com", "service": "https://pds.example", "prev": nil, "sig": "signature",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRenameHarness(t)
			first := h.insertOldActor(t, plcFailureDID, "https://example.com/u/alice", plcOldHandle, "alice")
			h.insertOldActor(t, plcNextDID, "https://example.com/u/bob", "bob.example-com.tidepool.test", "bob")
			h.plc.heads = map[string][]map[string]any{"/" + plcFailureDID: {tc.head}}
			capture := &capturingIdentity{Identity: h.minter}
			runner, err := NewRunner(Options{Actors: h.actors, Identity: capture, ActorFetcher: h.actorClient,
				BridgeHostname: "tidepool.test", HandleZoneRoot: "ap.tidepool.test",
				ActorsPerSecond: 2, Now: h.clock.Now, Wait: h.clock.Wait})
			require.NoError(t, err)
			require.NoError(t, runner.Run(t.Context(), []string{plcFailureDID, plcNextDID}))
			requests, _ := h.plc.snapshot()
			require.Empty(t, plcRequestsFor(requests, http.MethodPost, "/"+plcFailureDID))
			require.Len(t, capture.errors, 2)
			require.NoError(t, capture.errors[0])
			assertPendingRename(t, h, plcFailureDID, plcOldHandle, plcNewHandle, first.ID)
			code, _ := migrationFailure(t, h, plcFailureDID)
			require.Equal(t, sql.NullString{String: "plc_not_controlled", Valid: true}, code)
			assertFinalizedRename(t, h, plcNextDID, "bob.example-com.tidepool.test", "bob.example.com.ap.tidepool.test")
		})
	}
}

func TestRunner_PLCLastOperationGoneFailsWithoutRetryAndContinues(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusGone} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			h := newRenameHarness(t)
			first := h.insertOldActor(t, plcFailureDID, "https://example.com/u/alice", plcOldHandle, "alice")
			h.insertOldActor(t, plcNextDID, "https://example.com/u/bob", "bob.example-com.tidepool.test", "bob")
			h.plc.getStatusSequence = map[string][]int{"/" + plcFailureDID: {status}}
			h.plc.getBodies = map[string]string{"/" + plcFailureDID: `{"message":"DID not registered"}`}
			capture := &capturingIdentity{Identity: h.minter}
			runner, err := NewRunner(Options{Actors: h.actors, Identity: capture, ActorFetcher: h.actorClient,
				BridgeHostname: "tidepool.test", HandleZoneRoot: "ap.tidepool.test",
				ActorsPerSecond: 2, Now: h.clock.Now, Wait: h.clock.Wait})
			require.NoError(t, err)
			require.NoError(t, runner.Run(t.Context(), []string{plcFailureDID, plcNextDID}))
			requests, _ := h.plc.snapshot()
			require.Len(t, plcRequestsFor(requests, http.MethodGet, "/"+plcFailureDID+"/log/last"), 1)
			require.Empty(t, plcRequestsFor(requests, http.MethodPost, "/"+plcFailureDID))
			require.Empty(t, h.delays())
			require.Len(t, capture.errors, 2)
			require.Error(t, capture.errors[0])
			require.False(t, errors.Is(capture.errors[0], identity.ErrPLCTransient), "a missing DID log is not transient: %v", capture.errors[0])
			require.Contains(t, capture.errors[0].Error(), "DID not registered")
			assertPendingRename(t, h, plcFailureDID, plcOldHandle, plcNewHandle, first.ID)
			code, _ := migrationFailure(t, h, plcFailureDID)
			require.False(t, code.Valid)
			assertFinalizedRename(t, h, plcNextDID, "bob.example-com.tidepool.test", "bob.example.com.ap.tidepool.test")
		})
	}
}

func TestRunner_RetriesTransientPLCHeadReadThenFinalizes(t *testing.T) {
	h := newRenameHarness(t)
	h.insertOldActor(t, plcFailureDID, "https://example.com/u/alice", plcOldHandle, "alice")
	h.plc.getStatusSequence = map[string][]int{"/" + plcFailureDID: {http.StatusTooManyRequests, http.StatusOK}}
	require.NoError(t, h.runner.Run(t.Context(), []string{plcFailureDID}))
	requests, _ := h.plc.snapshot()
	require.Len(t, plcRequestsFor(requests, http.MethodGet, "/"+plcFailureDID+"/log/last"), 3)
	require.Len(t, plcRequestsFor(requests, http.MethodPost, "/"+plcFailureDID), 1)
	delays := h.delays()
	require.Len(t, delays, 1)
	require.Positive(t, delays[0])
	assertFinalizedRename(t, h, plcFailureDID, plcOldHandle, plcNewHandle)
}

func TestRunner_ExhaustedPLCHeadReadRetriesKeepReservationAndContinue(t *testing.T) {
	h := newRenameHarness(t)
	first := h.insertOldActor(t, plcFailureDID, "https://example.com/u/alice", plcOldHandle, "alice")
	h.insertOldActor(t, plcNextDID, "https://example.com/u/bob", "bob.example-com.tidepool.test", "bob")
	h.plc.getStatusSequence = map[string][]int{"/" + plcFailureDID: {http.StatusServiceUnavailable}}
	capture := &capturingIdentity{Identity: h.minter}
	runner, err := NewRunner(Options{Actors: h.actors, Identity: capture, ActorFetcher: h.actorClient,
		BridgeHostname: "tidepool.test", HandleZoneRoot: "ap.tidepool.test",
		ActorsPerSecond: 2, Now: h.clock.Now, Wait: h.clock.Wait})
	require.NoError(t, err)
	require.NoError(t, runner.Run(t.Context(), []string{plcFailureDID, plcNextDID}))
	requests, _ := h.plc.snapshot()
	require.Len(t, plcRequestsFor(requests, http.MethodGet, "/"+plcFailureDID+"/log/last"), 5)
	require.Empty(t, plcRequestsFor(requests, http.MethodPost, "/"+plcFailureDID))
	delays := h.delays()
	require.Len(t, delays, 4)
	require.Positive(t, delays[0])
	for index := 1; index < len(delays); index++ {
		require.Greater(t, delays[index], delays[index-1])
	}
	require.Len(t, capture.errors, 2)
	require.ErrorIs(t, capture.errors[0], identity.ErrPLCTransient)
	assertPendingRename(t, h, plcFailureDID, plcOldHandle, plcNewHandle, first.ID)
	code, _ := migrationFailure(t, h, plcFailureDID)
	require.False(t, code.Valid, "transient failures must remain eligible for another run")
	assertFinalizedRename(t, h, plcNextDID, "bob.example-com.tidepool.test", "bob.example.com.ap.tidepool.test")
}

type cancelAfterPostTransport struct {
	base   http.RoundTripper
	path   string
	cancel context.CancelFunc
}

func (transport *cancelAfterPostTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := transport.base.RoundTrip(request)
	if err == nil && request.Method == http.MethodPost && request.URL.Path == transport.path {
		transport.cancel()
	}
	return response, err
}

func TestRunner_FinalizesPLCAcceptedWhenContextCancelledAfterResponse(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	h := newRenameHarnessWithTransport(t, nil, func(base http.RoundTripper) http.RoundTripper {
		return &cancelAfterPostTransport{base: base, path: "/" + plcFailureDID, cancel: cancel}
	})
	h.insertOldActor(t, plcFailureDID, "https://example.com/u/alice", plcOldHandle, "alice")
	if err := h.runner.Run(ctx, []string{plcFailureDID}); err != nil {
		require.ErrorIs(t, err, context.Canceled)
	}
	require.Error(t, ctx.Err(), "the transport must cancel the run after PLC answered")
	requests, _ := h.plc.snapshot()
	require.Len(t, plcRequestsFor(requests, http.MethodPost, "/"+plcFailureDID), 1)
	assertFinalizedRename(t, h, plcFailureDID, plcOldHandle, plcNewHandle)
}

func TestRunner_PLC400RejectedKeepsReservationAndContinues(t *testing.T) {
	h := newRenameHarness(t)
	first := h.insertOldActor(t, plcFailureDID, "https://example.com/u/alice", plcOldHandle, "alice")
	h.insertOldActor(t, plcNextDID, "https://example.com/u/bob", "bob.example-com.tidepool.test", "bob")
	h.plc.postStatuses = map[string]int{"/" + plcFailureDID: http.StatusBadRequest}
	h.plc.postBodies = map[string]string{"/" + plcFailureDID: `{"message":"Invalid signature"}`}
	require.NoError(t, h.runner.Run(t.Context(), []string{plcFailureDID, plcNextDID}))
	requests, _ := h.plc.snapshot()
	require.Len(t, plcRequestsFor(requests, http.MethodPost, "/"+plcFailureDID), 1)
	assertPendingRename(t, h, plcFailureDID, plcOldHandle, plcNewHandle, first.ID)
	code, message := migrationFailure(t, h, plcFailureDID)
	require.Equal(t, sql.NullString{String: "plc_rejected", Valid: true}, code)
	require.True(t, message.Valid)
	require.Contains(t, message.String, "Invalid signature")
	resolver := identity.NewStoreResolver(h.actors, "tidepool.test", "")
	for _, handle := range []string{plcOldHandle, plcNewHandle} {
		resolved, err := resolver.ResolveHandle(t.Context(), handle)
		require.NoError(t, err)
		require.Equal(t, plcFailureDID, resolved)
	}
	assertFinalizedRename(t, h, plcNextDID, "bob.example-com.tidepool.test", "bob.example.com.ap.tidepool.test")
}

type plcTimeout struct{}

func (plcTimeout) Error() string   { return "PLC POST timed out" }
func (plcTimeout) Timeout() bool   { return true }
func (plcTimeout) Temporary() bool { return true }

type postTimeoutTransport struct {
	base     http.RoundTripper
	path     string
	land     bool
	attempts int
}

func (transport *postTimeoutTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.Method != http.MethodPost || request.URL.Path != transport.path {
		return transport.base.RoundTrip(request)
	}
	transport.attempts++
	if transport.land {
		response, err := transport.base.RoundTrip(request)
		if err != nil {
			return nil, err
		}
		_ = response.Body.Close()
	}
	return nil, plcTimeout{}
}

type capturingIdentity struct {
	Identity
	errors []error
}

func (capture *capturingIdentity) UpdateHandle(ctx context.Context, did, handle string, beforeSubmit func(context.Context) error) (identity.HandleUpdateResult, error) {
	result, err := capture.Identity.UpdateHandle(ctx, did, handle, beforeSubmit)
	capture.errors = append(capture.errors, err)
	return result, err
}

func TestRunner_ReconcilesUnclearPLCPostOutcome(t *testing.T) {
	for _, tc := range []struct {
		name, response  string
		land, transient bool
	}{
		{"400 after landing", "400", true, false},
		{"timeout after landing", "timeout", true, false},
		{"timeout without landing", "timeout", false, true},
		{"400 with failed head reread", "unreadable", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var transport *postTimeoutTransport
			h := newRenameHarnessWithTransport(t, nil, func(base http.RoundTripper) http.RoundTripper {
				if tc.response != "timeout" {
					return base
				}
				transport = &postTimeoutTransport{base: base, path: "/" + plcFailureDID, land: tc.land}
				return transport
			})
			actor := h.insertOldActor(t, plcFailureDID, "https://example.com/u/alice", plcOldHandle, "alice")
			if tc.response == "400" || tc.response == "unreadable" {
				h.plc.postStatuses = map[string]int{"/" + plcFailureDID: http.StatusBadRequest}
			}
			if tc.land {
				h.plc.landPosts = map[string]bool{"/" + plcFailureDID: true}
			}
			if tc.response == "unreadable" {
				h.plc.getStatusAfterPost = map[string]int{"/" + plcFailureDID: http.StatusInternalServerError}
			}
			capture := &capturingIdentity{Identity: h.minter}
			runner, err := NewRunner(Options{Actors: h.actors, Identity: capture, ActorFetcher: h.actorClient,
				BridgeHostname: "tidepool.test", HandleZoneRoot: "ap.tidepool.test",
				ActorsPerSecond: 2, Now: h.clock.Now, Wait: h.clock.Wait})
			require.NoError(t, err)
			require.NoError(t, runner.Run(t.Context(), []string{plcFailureDID}))
			requests, _ := h.plc.snapshot()
			if transport != nil {
				require.Equal(t, 1, transport.attempts, "exactly one attempted POST")
				if !tc.land {
					require.Empty(t, plcRequestsFor(requests, http.MethodPost, "/"+plcFailureDID))
				}
			} else {
				require.Len(t, plcRequestsFor(requests, http.MethodPost, "/"+plcFailureDID), 1)
			}
			if tc.transient {
				require.Len(t, capture.errors, 1)
				require.True(t, errors.Is(capture.errors[0], identity.ErrPLCTransient), "unknown PLC outcome must be retryable: %v", capture.errors[0])
				assertPendingRename(t, h, plcFailureDID, plcOldHandle, plcNewHandle, actor.ID)
				code, _ := migrationFailure(t, h, plcFailureDID)
				require.False(t, code.Valid)
			} else {
				assertFinalizedRename(t, h, plcFailureDID, plcOldHandle, plcNewHandle)
			}
		})
	}
}

func TestRunner_RechecksAdvancingPLCHead(t *testing.T) {
	for _, tc := range []struct {
		name, handle, endpoint, signingKey string
		dropKey, wantPost, wantError       bool
	}{
		{"already applied by another writer", plcNewHandle, "https://tidepool.test", "", false, false, false},
		{"other writer changed services", "someone.else.example", "https://changed-pds.example", "", false, true, false},
		{"other writer removed escrow key", "someone.else.example", "https://tidepool.test", "", true, false, true},
		{"target handle but changed services", plcNewHandle, "https://changed-pds.example", "", false, true, false},
		{"target handle but changed verificationMethods", plcNewHandle, "https://tidepool.test", "did:key:zChangedSigningKey", false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRenameHarness(t)
			actor := h.insertOldActor(t, plcFailureDID, "https://example.com/u/alice", plcOldHandle, "alice")
			first := h.plc.last
			changes := map[string]any{
				"alsoKnownAs": []any{"at://" + tc.handle},
				"prev":        headCID(t, first),
				"services":    map[string]any{"atproto_pds": map[string]any{"type": "AtprotoPersonalDataServer", "endpoint": tc.endpoint}},
			}
			if tc.signingKey != "" {
				changes["verificationMethods"] = map[string]any{"atproto": tc.signingKey}
			}
			key := h.rotationKey
			if tc.dropKey {
				var err error
				key, err = atcrypto.GeneratePrivateKeyK256()
				require.NoError(t, err)
				public, err := key.PublicKey()
				require.NoError(t, err)
				changes["rotationKeys"] = []any{public.DIDKey()}
			}
			second := signedHead(t, key, first, changes)
			h.plc.heads = map[string][]map[string]any{"/" + plcFailureDID: {first, second}}
			require.NoError(t, h.runner.Run(t.Context(), []string{plcFailureDID}))
			requests, _ := h.plc.snapshot()
			require.Len(t, plcRequestsFor(requests, http.MethodGet, "/"+plcFailureDID+"/log/last"), 2)
			posts := plcRequestsFor(requests, http.MethodPost, "/"+plcFailureDID)
			if !tc.wantPost {
				require.Empty(t, posts)
			} else {
				require.Len(t, posts, 1)
				require.Equal(t, headCID(t, second), posts[0].Body["prev"])
				require.Equal(t, second["services"], posts[0].Body["services"])
				require.Equal(t, second["verificationMethods"], posts[0].Body["verificationMethods"])
				if tc.name == "other writer changed services" {
					signature, ok := posts[0].Body["sig"].(string)
					require.True(t, ok)
					decoded, err := base64.RawURLEncoding.DecodeString(signature)
					require.NoError(t, err)
					unsigned := make(map[string]any, len(posts[0].Body)-1)
					for field, value := range posts[0].Body {
						if field != "sig" {
							unsigned[field] = value
						}
					}
					encoded, err := atdata.MarshalCBOR(unsigned)
					require.NoError(t, err)
					public, err := h.rotationKey.PublicKey()
					require.NoError(t, err)
					require.NoError(t, public.HashAndVerify(encoded, decoded))
				}
			}
			if tc.wantError {
				assertPendingRename(t, h, plcFailureDID, plcOldHandle, plcNewHandle, actor.ID)
				code, _ := migrationFailure(t, h, plcFailureDID)
				require.Equal(t, sql.NullString{String: "plc_not_controlled", Valid: true}, code)
			} else {
				assertFinalizedRename(t, h, plcFailureDID, plcOldHandle, plcNewHandle)
			}
		})
	}
}

func TestRunner_InvalidHandleKeepsOldActorAndContinues(t *testing.T) {
	h := newRenameHarness(t)
	longHost := strings.Join([]string{strings.Repeat("a", 60), strings.Repeat("b", 60), strings.Repeat("c", 60), strings.Repeat("d", 60)}, ".")
	first := h.insertOldActor(t, plcFailureDID, "https://"+longHost+"/u/alice", plcOldHandle, "alice")
	h.insertOldActor(t, plcNextDID, "https://example.com/u/bob", "bob.example-com.tidepool.test", "bob")
	require.NoError(t, h.runner.Run(t.Context(), []string{plcFailureDID, plcNextDID}))
	requests, _ := h.plc.snapshot()
	for _, request := range requests {
		require.NotEqual(t, "/"+plcFailureDID, request.Path)
		require.NotEqual(t, "/"+plcFailureDID+"/log/last", request.Path)
	}
	row := h.row(t, plcFailureDID)
	require.Equal(t, plcOldHandle, row.Handle)
	require.Empty(t, row.PendingHandle)
	require.Nil(t, row.HandleMigratedAt)
	var additionalClaims int
	require.NoError(t, h.db.QueryRowContext(t.Context(),
		`SELECT count(*) FROM handle_claims WHERE actor_id = $1 AND handle <> $2`, first.ID, plcOldHandle).Scan(&additionalClaims))
	require.Zero(t, additionalClaims)
	resolver := identity.NewStoreResolver(h.actors, "tidepool.test", "")
	resolved, err := resolver.ResolveHandle(t.Context(), plcOldHandle)
	require.NoError(t, err)
	require.Equal(t, plcFailureDID, resolved)
	code, _ := migrationFailure(t, h, plcFailureDID)
	require.Equal(t, sql.NullString{String: "handle_invalid", Valid: true}, code)
	assertFinalizedRename(t, h, plcNextDID, "bob.example-com.tidepool.test", "bob.example.com.ap.tidepool.test")
}

func TestRunner_RetriesTransientPLCResponsesThenFinalizes(t *testing.T) {
	h := newRenameHarness(t)
	h.insertOldActor(t, plcFailureDID, "https://example.com/u/alice", plcOldHandle, "alice")
	h.plc.postStatusSequence = map[string][]int{"/" + plcFailureDID: {
		http.StatusTooManyRequests, http.StatusServiceUnavailable, http.StatusOK,
	}}
	require.NoError(t, h.runner.Run(t.Context(), []string{plcFailureDID}))
	requests, _ := h.plc.snapshot()
	require.Len(t, plcRequestsFor(requests, http.MethodPost, "/"+plcFailureDID), 3)
	delays := h.delays()
	require.Len(t, delays, 2)
	require.Positive(t, delays[0])
	require.Greater(t, delays[1], delays[0])
	assertFinalizedRename(t, h, plcFailureDID, plcOldHandle, plcNewHandle)
}

func TestRunner_ExhaustedPLCRetriesKeepReservationAndContinue(t *testing.T) {
	h := newRenameHarness(t)
	first := h.insertOldActor(t, plcFailureDID, "https://example.com/u/alice", plcOldHandle, "alice")
	h.insertOldActor(t, plcNextDID, "https://example.com/u/bob", "bob.example-com.tidepool.test", "bob")
	h.plc.postStatuses = map[string]int{"/" + plcFailureDID: http.StatusServiceUnavailable}
	capture := &capturingIdentity{Identity: h.minter}
	runner, err := NewRunner(Options{Actors: h.actors, Identity: capture, ActorFetcher: h.actorClient,
		BridgeHostname: "tidepool.test", HandleZoneRoot: "ap.tidepool.test",
		ActorsPerSecond: 2, Now: h.clock.Now, Wait: h.clock.Wait})
	require.NoError(t, err)
	require.NoError(t, runner.Run(t.Context(), []string{plcFailureDID, plcNextDID}))
	requests, _ := h.plc.snapshot()
	require.Len(t, plcRequestsFor(requests, http.MethodPost, "/"+plcFailureDID), 5)
	delays := h.delays()
	require.Len(t, delays, 4)
	require.Positive(t, delays[0])
	for index := 1; index < len(delays); index++ {
		require.Greater(t, delays[index], delays[index-1])
	}
	require.Len(t, capture.errors, 2)
	require.ErrorIs(t, capture.errors[0], identity.ErrPLCTransient)
	assertPendingRename(t, h, plcFailureDID, plcOldHandle, plcNewHandle, first.ID)
	code, _ := migrationFailure(t, h, plcFailureDID)
	require.False(t, code.Valid, "transient failures must remain eligible for another run")
	resolver := identity.NewStoreResolver(h.actors, "tidepool.test", "")
	for _, handle := range []string{plcOldHandle, plcNewHandle} {
		resolved, err := resolver.ResolveHandle(t.Context(), handle)
		require.NoError(t, err)
		require.Equal(t, plcFailureDID, resolved)
	}
	candidate, err := h.actors.IsForwardRenameCandidate(t.Context(), plcFailureDID, "tidepool.test", "ap.tidepool.test")
	require.NoError(t, err)
	require.True(t, candidate)
	assertFinalizedRename(t, h, plcNextDID, "bob.example-com.tidepool.test", "bob.example.com.ap.tidepool.test")
	h.plc.mutex.Lock()
	h.plc.postStatuses["/"+plcFailureDID] = http.StatusOK
	h.plc.mutex.Unlock()
	require.NoError(t, runner.Run(t.Context(), []string{plcFailureDID}))
	requests, _ = h.plc.snapshot()
	posts := plcRequestsFor(requests, http.MethodPost, "/"+plcFailureDID)
	require.Len(t, posts, 6)
	require.Equal(t, []any{"at://alice.example.com.ap.tidepool.test"}, posts[5].Body["alsoKnownAs"])
	assertFinalizedRename(t, h, plcFailureDID, plcOldHandle, plcNewHandle)
}

func TestRunner_FinalizationClearsPriorMigrationError(t *testing.T) {
	h := newRenameHarness(t)
	h.insertOldActor(t, plcFailureDID, "https://example.com/u/alice", plcOldHandle, "alice")
	require.NoError(t, h.actors.SetPendingHandle(t.Context(), plcFailureDID, plcNewHandle))
	_, err := h.db.ExecContext(t.Context(), `UPDATE bridged_actors SET handle_migration_error = $1,
		handle_migration_message = $2 WHERE did = $3`, "plc_rejected", "Invalid signature", plcFailureDID)
	require.NoError(t, err)
	require.NoError(t, h.runner.Run(t.Context(), []string{plcFailureDID}))
	requests, _ := h.plc.snapshot()
	posts := plcRequestsFor(requests, http.MethodPost, "/"+plcFailureDID)
	require.Len(t, posts, 1)
	require.Equal(t, []any{"at://alice.example.com.ap.tidepool.test"}, posts[0].Body["alsoKnownAs"])
	assertFinalizedRename(t, h, plcFailureDID, plcOldHandle, plcNewHandle)
	code, message := migrationFailure(t, h, plcFailureDID)
	require.False(t, code.Valid)
	require.False(t, message.Valid)
}
