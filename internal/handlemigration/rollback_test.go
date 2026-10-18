package handlemigration

import (
	"context"
	"database/sql"
	stderrors "errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/stretchr/testify/require"

	internalerrors "tidepool/internal/errors"
	"tidepool/internal/identity"
	"tidepool/internal/store"
)

const (
	rollbackDID = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	rollbackOld = "alice.example-com.tidepool.test"
	rollbackNew = "alice.example.com.ap.tidepool.test"
)

func forwardRenamedForRollback(t *testing.T, h *renameHarness) *store.BridgedActor {
	t.Helper()
	actor := h.insertOldActor(t, rollbackDID, "https://example.com/u/alice", rollbackOld, "alice")
	require.NoError(t, h.runner.Run(t.Context(), []string{rollbackDID}))
	row := h.row(t, rollbackDID)
	require.Equal(t, rollbackNew, row.Handle)
	require.Equal(t, rollbackOld, row.PreviousHandle)
	require.NotNil(t, row.HandleMigratedAt)
	return actor
}

func TestRunner_RollbackReservesAndFinalizesRenamedActor(t *testing.T) {
	h := newRenameHarness(t)
	actor := forwardRenamedForRollback(t, h)
	forwardTime := *h.row(t, rollbackDID).HandleMigratedAt
	prior, _ := h.plc.snapshot()
	h.reservationAtPLCWithResolution(rollbackDID, rollbackOld, actor.ID, []string{rollbackOld, rollbackNew})
	require.NoError(t, h.runner.Rollback(t.Context(), []string{rollbackDID}))
	requests, hookErrors := h.plc.snapshot()
	requests = requests[len(prior):]
	require.NotEmpty(t, requests, "rollback must reach PLC with the old handle reserved")
	require.Equal(t, http.MethodGet, requests[0].Method)
	require.Equal(t, "/"+rollbackDID+"/log/last", requests[0].Path)
	require.Empty(t, hookErrors, "first PLC read must see the old claim and both resolving handles")
	posts := plcRequestsFor(requests, http.MethodPost, "/"+rollbackDID)
	require.Len(t, posts, 1)
	require.Equal(t, []any{"at://alice.example-com.tidepool.test"}, posts[0].Body["alsoKnownAs"])
	row := h.row(t, rollbackDID)
	require.Equal(t, rollbackOld, row.Handle)
	require.Equal(t, rollbackNew, row.PreviousHandle)
	require.Empty(t, row.PendingHandle)
	require.NotNil(t, row.HandleMigratedAt)
	require.False(t, row.HandleMigratedAt.Equal(forwardTime))
	_, claimed := h.claim(t, rollbackNew)
	require.False(t, claimed)
	resolver := identity.NewStoreResolver(h.actors, "tidepool.test", "did:web:tidepool.test")
	_, err := resolver.ResolveHandle(t.Context(), rollbackNew)
	require.True(t, internalerrors.IsNotFound(err), "new handle must not resolve after rollback: %v", err)
	resolved, err := resolver.ResolveHandle(t.Context(), rollbackOld)
	require.NoError(t, err)
	require.Equal(t, rollbackDID, resolved)
	require.Equal(t, []string{rollbackNew, rollbackOld}, h.identityHandles(t, rollbackDID))
}

func TestRunner_RollbackFinalizesAlreadyAppliedPLCHead(t *testing.T) {
	h := newRenameHarness(t)
	actor := forwardRenamedForRollback(t, h)
	require.NoError(t, h.actors.SetPendingHandle(t.Context(), rollbackDID, rollbackOld))
	forwardHead := h.plc.heads["/"+rollbackDID][0]
	h.plc.heads["/"+rollbackDID] = []map[string]any{
		signedHead(t, h.rotationKey, forwardHead, map[string]any{
			"alsoKnownAs": []any{"at://alice.example-com.tidepool.test"}, "prev": headCID(t, forwardHead),
		}),
	}
	prior, _ := h.plc.snapshot()
	require.NoError(t, h.runner.Rollback(t.Context(), []string{rollbackDID}))
	requests, hookErrors := h.plc.snapshot()
	require.Empty(t, hookErrors)
	require.Empty(t, plcRequestsFor(requests[len(prior):], http.MethodPost, "/"+rollbackDID))
	row := h.row(t, rollbackDID)
	require.Equal(t, rollbackOld, row.Handle)
	require.Equal(t, rollbackNew, row.PreviousHandle)
	require.Empty(t, row.PendingHandle)
	require.NotNil(t, row.HandleMigratedAt)
	owner, claimed := h.claim(t, rollbackOld)
	require.True(t, claimed)
	require.Equal(t, actor.ID, owner)
	_, claimed = h.claim(t, rollbackNew)
	require.False(t, claimed)
	require.Equal(t, []string{rollbackNew, rollbackOld}, h.identityHandles(t, rollbackDID))
}

func TestRunner_RollbackPLCRejectionKeepsReservation(t *testing.T) {
	h := newRenameHarness(t)
	actor := forwardRenamedForRollback(t, h)
	h.plc.postStatuses = map[string]int{"/" + rollbackDID: http.StatusBadRequest}
	h.plc.postBodies = map[string]string{"/" + rollbackDID: `{"message":"Rollback signature rejected"}`}
	require.NoError(t, h.runner.Rollback(t.Context(), []string{rollbackDID}))
	code, message := migrationFailure(t, h, rollbackDID)
	require.Equal(t, sql.NullString{String: "plc_rejected", Valid: true}, code)
	require.True(t, message.Valid)
	require.Contains(t, message.String, "Rollback signature rejected")
	row := h.row(t, rollbackDID)
	require.Equal(t, rollbackNew, row.Handle)
	require.Equal(t, rollbackOld, row.PendingHandle)
	owner, claimed := h.claim(t, rollbackOld)
	require.True(t, claimed)
	require.Equal(t, actor.ID, owner)
	got, err := h.actors.ListRollbackCandidates(t.Context(), "tidepool.test", "ap.tidepool.test", nil)
	require.NoError(t, err)
	require.Equal(t, []string{rollbackDID}, got)
}

func TestRunner_RollbackMidForward(t *testing.T) {
	for _, tc := range []struct {
		name         string
		head         string
		postStatus   int
		landPost     bool
		wantPost     bool
		wantIdentity bool
		wantPending  bool
	}{
		{"head lists N, POST accepted", "new", http.StatusOK, false, true, true, false},
		{"head lists O", "old", 0, false, false, false, false},
		{"head lists N, POST 400 not landed", "new", http.StatusBadRequest, false, true, false, true},
		{"head lists N, POST lands but answers 400", "new", http.StatusBadRequest, true, true, true, false},
		// Tidepool cannot sign over a head it does not control, so the
		// database stops claiming N without a PLC change or an event.
		{"foreign-signed head lists N", "foreign new", 0, false, false, false, false},
		{"foreign-signed head lists O", "foreign old", 0, false, false, false, false},
		{"tombstoned PLC head", "tombstone", 0, false, false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRenameHarness(t)
			actor := h.insertOldActor(t, rollbackDID, "https://example.com/u/alice", rollbackOld, "alice")
			require.NoError(t, h.actors.SetPendingHandle(t.Context(), rollbackDID, rollbackNew))
			seededAt := time.Date(2026, 8, 2, 11, 0, 0, 0, time.UTC)
			_, err := h.db.ExecContext(t.Context(), `UPDATE bridged_actors SET previous_handle = $1,
				handle_migrated_at = $2 WHERE did = $3`, "older.example.com.ap.tidepool.test", seededAt, rollbackDID)
			require.NoError(t, err)
			require.NoError(t, h.actors.RecordHandleMigrationError(t.Context(), rollbackDID, store.HandleMigrationPLCRejected, "Earlier rejection"))
			foreignKey, err := atcrypto.GeneratePrivateKeyK256()
			require.NoError(t, err)
			foreignPublic, err := foreignKey.PublicKey()
			require.NoError(t, err)
			foreignRotation := map[string]any{"rotationKeys": []any{foreignPublic.DIDKey()}, "prev": headCID(t, h.plc.last)}
			var servedHead map[string]any
			switch tc.head {
			case "new":
				servedHead = signedHead(t, h.rotationKey, h.plc.last, map[string]any{
					"alsoKnownAs": []any{"at://alice.example.com.ap.tidepool.test"}, "prev": headCID(t, h.plc.last),
				})
			case "old":
				servedHead = signedHead(t, h.rotationKey, h.plc.last, map[string]any{
					"alsoKnownAs": []any{"at://alice.example-com.tidepool.test"}, "prev": headCID(t, h.plc.last),
				})
			case "foreign new":
				foreignRotation["alsoKnownAs"] = []any{"at://alice.example.com.ap.tidepool.test"}
				servedHead = signedHead(t, foreignKey, h.plc.last, foreignRotation)
			case "foreign old":
				foreignRotation["alsoKnownAs"] = []any{"at://alice.example-com.tidepool.test"}
				servedHead = signedHead(t, foreignKey, h.plc.last, foreignRotation)
			case "tombstone":
				servedHead = signedHead(t, foreignKey, map[string]any{"type": "plc_tombstone", "prev": headCID(t, h.plc.last)}, nil)
			default:
				t.Fatalf("unknown head %q", tc.head)
			}
			h.plc.heads = map[string][]map[string]any{"/" + rollbackDID: {servedHead}}
			if tc.postStatus != 0 {
				h.plc.postStatuses = map[string]int{"/" + rollbackDID: tc.postStatus}
			}
			if tc.postStatus == http.StatusBadRequest {
				h.plc.postBodies = map[string]string{"/" + rollbackDID: `{"message":"Rollback rejected by PLC"}`}
			}
			if tc.landPost {
				h.plc.landPosts = map[string]bool{"/" + rollbackDID: true}
			}
			require.NoError(t, h.runner.Rollback(t.Context(), []string{rollbackDID}))
			requests, hookErrors := h.plc.snapshot()
			require.Empty(t, hookErrors)
			posts := plcRequestsFor(requests, http.MethodPost, "/"+rollbackDID)
			if tc.wantPost {
				require.Len(t, posts, 1)
				require.Equal(t, []any{"at://alice.example-com.tidepool.test"}, posts[0].Body["alsoKnownAs"])
				require.Equal(t, headCID(t, servedHead), posts[0].Body["prev"])
			} else {
				require.Empty(t, posts)
			}
			row := h.row(t, rollbackDID)
			require.Equal(t, rollbackOld, row.Handle)
			require.Equal(t, "older.example.com.ap.tidepool.test", row.PreviousHandle)
			require.NotNil(t, row.HandleMigratedAt)
			require.True(t, row.HandleMigratedAt.Equal(seededAt))
			code, message := migrationFailure(t, h, rollbackDID)
			if tc.wantPending {
				require.Equal(t, rollbackNew, row.PendingHandle)
				owner, claimed := h.claim(t, rollbackNew)
				require.True(t, claimed)
				require.Equal(t, actor.ID, owner)
				require.Equal(t, sql.NullString{String: "plc_rejected", Valid: true}, code)
				require.True(t, message.Valid)
				require.Contains(t, message.String, "Rollback rejected by PLC")
				require.Empty(t, h.identityHandles(t, rollbackDID))
				candidates, err := h.actors.ListRollbackCandidates(t.Context(), "tidepool.test", "ap.tidepool.test", nil)
				require.NoError(t, err)
				require.Equal(t, []string{rollbackDID}, candidates)
				return
			}
			require.Empty(t, row.PendingHandle)
			_, claimed := h.claim(t, rollbackNew)
			require.False(t, claimed)
			require.False(t, code.Valid)
			require.False(t, message.Valid)
			wantHandles := []string(nil)
			if tc.wantIdentity {
				wantHandles = []string{rollbackOld}
			}
			require.Equal(t, wantHandles, h.identityHandles(t, rollbackDID))
			if tc.wantPost {
				resolver := identity.NewStoreResolver(h.actors, "tidepool.test", "did:web:tidepool.test")
				_, err := resolver.ResolveHandle(t.Context(), rollbackNew)
				require.True(t, internalerrors.IsNotFound(err), "abandoned new handle must not resolve: %v", err)
			}
			candidates, err := h.actors.ListRollbackCandidates(t.Context(), "tidepool.test", "ap.tidepool.test", nil)
			require.NoError(t, err)
			require.Empty(t, candidates)
			forward, err := h.actors.IsForwardRenameCandidate(t.Context(), rollbackDID, "tidepool.test", "ap.tidepool.test")
			require.NoError(t, err)
			require.True(t, forward)
		})
	}
}

func TestRunner_ForwardAfterRollbackUsesCurrentUsername(t *testing.T) {
	h := newRenameHarness(t)
	forwardRenamedForRollback(t, h)
	require.NoError(t, h.runner.Rollback(t.Context(), []string{rollbackDID}))
	require.NoError(t, h.actors.SetPreferredUsernameByDID(t.Context(), rollbackDID, "robin"))
	before, hookErrors := h.plc.snapshot()
	require.Empty(t, hookErrors)
	require.NoError(t, h.runner.Run(t.Context(), nil))
	after, hookErrors := h.plc.snapshot()
	require.Empty(t, hookErrors)
	posts := plcRequestsFor(after[len(before):], http.MethodPost, "/"+rollbackDID)
	require.Len(t, posts, 1, "rolled-back actor with a changed username must be selected for forward rename")
	require.Equal(t, []any{"at://robin.example.com.ap.tidepool.test"}, posts[0].Body["alsoKnownAs"])
	row := h.row(t, rollbackDID)
	require.Equal(t, "robin.example.com.ap.tidepool.test", row.Handle)
	require.Equal(t, rollbackOld, row.PreviousHandle)
	require.Empty(t, row.PendingHandle)
	require.Equal(t, []string{rollbackNew, rollbackOld, "robin.example.com.ap.tidepool.test"}, h.identityHandles(t, rollbackDID))
}

// midForwardForRollback seeds an actor caught mid-forward: handle O, pending
// N with its claim, and a Tidepool-signed PLC head that lists N.
func midForwardForRollback(t *testing.T, h *renameHarness) *store.BridgedActor {
	t.Helper()
	actor := h.insertOldActor(t, rollbackDID, "https://example.com/u/alice", rollbackOld, "alice")
	require.NoError(t, h.actors.SetPendingHandle(t.Context(), rollbackDID, rollbackNew))
	h.plc.heads = map[string][]map[string]any{"/" + rollbackDID: {
		signedHead(t, h.rotationKey, h.plc.last, map[string]any{
			"alsoKnownAs": []any{"at://" + rollbackNew}, "prev": headCID(t, h.plc.last),
		}),
	}}
	return actor
}

type failAbandonOnce struct {
	store.BridgedActors
	failed bool
}

func (a *failAbandonOnce) AbandonPendingHandle(ctx context.Context, did, expectedPending string, onPLCReverted func(ctx context.Context, tx *sql.Tx, actor *store.BridgedActor) error) (*store.BridgedActor, error) {
	if !a.failed {
		a.failed = true
		return nil, stderrors.New("abandon unavailable")
	}
	return a.BridgedActors.AbandonPendingHandle(ctx, did, expectedPending, onPLCReverted)
}

// cancelDuringPostTransport lets the POST land at PLC, then cancels the run
// before the runner sees the answer.
type cancelDuringPostTransport struct {
	base   http.RoundTripper
	path   string
	cancel context.CancelFunc
}

func (transport *cancelDuringPostTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.Method != http.MethodPost || request.URL.Path != transport.path {
		return transport.base.RoundTrip(request)
	}
	response, err := transport.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	_ = response.Body.Close()
	transport.cancel()
	return nil, context.Canceled
}

func TestRunner_RollbackMidForwardRevertLandedBeforeAbandonCommitted(t *testing.T) {
	assertReverted := func(t *testing.T, h *renameHarness) {
		t.Helper()
		row := h.row(t, rollbackDID)
		require.Equal(t, rollbackOld, row.Handle)
		require.Empty(t, row.PendingHandle)
		_, claimed := h.claim(t, rollbackNew)
		require.False(t, claimed)
		require.Equal(t, []string{rollbackOld}, h.identityHandles(t, rollbackDID), "PLC moved from N back to O, so exactly one #identity O")
		requests, hookErrors := h.plc.snapshot()
		require.Empty(t, hookErrors)
		require.Len(t, plcRequestsFor(requests, http.MethodPost, "/"+rollbackDID), 1)
		candidates, err := h.actors.ListRollbackCandidates(t.Context(), "tidepool.test", "ap.tidepool.test", nil)
		require.NoError(t, err)
		require.Empty(t, candidates)
	}

	t.Run("cancelled during POST, fresh run resumes", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		h := newRenameHarnessWithTransport(t, nil, func(base http.RoundTripper) http.RoundTripper {
			return &cancelDuringPostTransport{base: base, path: "/" + rollbackDID, cancel: cancel}
		})
		midForwardForRollback(t, h)
		require.ErrorIs(t, h.runner.Rollback(ctx, []string{rollbackDID}), context.Canceled)
		require.Equal(t, rollbackNew, h.row(t, rollbackDID).PendingHandle, "the PLC result was unknown to the cancelled run")
		require.NoError(t, h.runner.Rollback(t.Context(), []string{rollbackDID}))
		assertReverted(t, h)
	})

	t.Run("abandon fails once, second run resumes", func(t *testing.T) {
		h := newRenameHarness(t)
		midForwardForRollback(t, h)
		runner := h.runnerWithActors(t, &failAbandonOnce{BridgedActors: h.actors})
		require.NoError(t, runner.Rollback(t.Context(), []string{rollbackDID}))
		require.Equal(t, rollbackNew, h.row(t, rollbackDID).PendingHandle)
		require.Empty(t, h.identityHandles(t, rollbackDID))
		require.NoError(t, runner.Rollback(t.Context(), []string{rollbackDID}))
		assertReverted(t, h)
	})

	t.Run("cancelled after PLC accepted", func(t *testing.T) {
		h := newRenameHarness(t)
		midForwardForRollback(t, h)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		runner, err := NewRunner(Options{Actors: h.actors, Identity: cancelAfterPLCAccepted{Identity: h.minter, cancel: cancel},
			ActorFetcher: h.actorClient, BridgeHostname: "tidepool.test", HandleZoneRoot: "ap.tidepool.test",
			ActorsPerSecond: 2, Now: h.clock.Now, Wait: h.clock.Wait})
		require.NoError(t, err)
		_ = runner.Rollback(ctx, []string{rollbackDID}) // cancellation may be returned or consumed
		require.Error(t, ctx.Err())
		// Checked before any resume: the abandon must commit despite the cancellation.
		assertReverted(t, h)
		require.NoError(t, h.runner.Rollback(t.Context(), nil))
		assertReverted(t, h)
	})
}

func TestRunner_RollbackMidForwardTombstonedDuringPLCDoesNotAbandon(t *testing.T) {
	h := newRenameHarness(t)
	actor := midForwardForRollback(t, h)
	h.plc.mutex.Lock()
	h.plc.onRequest = func(request plcRequest, _ *http.Request) error {
		if request.Method != http.MethodPost || request.Path != "/"+rollbackDID {
			return nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		return h.actors.SetConsentState(ctx, actor.APActorID, store.ConsentStateDeleted)
	}
	h.plc.mutex.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	require.NoError(t, h.runner.Rollback(ctx, []string{rollbackDID}))
	requests, hookErrors := h.plc.snapshot()
	require.Empty(t, hookErrors)
	require.Len(t, plcRequestsFor(requests, http.MethodPost, "/"+rollbackDID), 1, "the PLC operation was submitted before the tombstone")
	row := h.row(t, rollbackDID)
	require.Equal(t, rollbackOld, row.Handle, "a tombstoned row is frozen")
	require.Equal(t, rollbackNew, row.PendingHandle, "a tombstoned row is frozen")
	owner, claimed := h.claim(t, rollbackNew)
	require.True(t, claimed)
	require.Equal(t, actor.ID, owner)
	require.Empty(t, h.identityHandles(t, rollbackDID), "no #identity frame may follow the tombstone")
}

func TestRunner_RollbackPreviousHandleHeldByAnotherActor(t *testing.T) {
	h := newRenameHarness(t)
	forwardRenamedForRollback(t, h)
	const otherDID = "did:plc:44ybard66vv44zksje25o7dz"
	h.insertOldActor(t, otherDID, "https://example.com/u/other", "other.example-com.tidepool.test", "other")
	_, err := h.db.ExecContext(t.Context(), `UPDATE bridged_actors SET handle = $1 WHERE did = $2`, rollbackOld, otherDID)
	require.NoError(t, err)
	before := h.row(t, rollbackDID)
	prior, _ := h.plc.snapshot()
	require.NoError(t, h.runner.Rollback(t.Context(), []string{rollbackDID}))
	requests, hookErrors := h.plc.snapshot()
	require.Empty(t, hookErrors)
	require.Empty(t, requests[len(prior):], "no PLC request without the previous handle reserved")
	code, message := migrationFailure(t, h, rollbackDID)
	require.Equal(t, sql.NullString{String: "handle_invalid", Valid: true}, code)
	require.Equal(t, sql.NullString{String: "previous handle is held by another actor", Valid: true}, message)
	require.Equal(t, before, h.row(t, rollbackDID))
	_, claimed := h.claim(t, rollbackOld)
	require.False(t, claimed)
	require.Equal(t, []string{rollbackNew}, h.identityHandles(t, rollbackDID))
	candidates, err := h.actors.ListRollbackCandidates(t.Context(), "tidepool.test", "ap.tidepool.test", nil)
	require.NoError(t, err)
	require.Equal(t, []string{rollbackDID}, candidates)
}

func TestRunner_RollbackPacesPLCSubmissions(t *testing.T) {
	const half = 500 * time.Millisecond
	h := newRenameHarness(t)
	dids := []string{"did:plc:rollbackpaceone", "did:plc:rollbackpacetwo", "did:plc:rollbackpacethree"}
	for index, name := range []string{"one", "two", "three"} {
		h.insertOldActor(t, dids[index], "https://example.com/u/"+name, name+".example-com.tidepool.test", name)
	}
	require.NoError(t, h.runner.Run(t.Context(), nil))
	var mutex sync.Mutex
	var arrivals []time.Time
	var paths []string
	h.plc.mutex.Lock()
	h.plc.onArrival = func(request plcRequest) error {
		if request.Method == http.MethodPost {
			mutex.Lock()
			arrivals = append(arrivals, h.clock.Now())
			paths = append(paths, request.Path)
			mutex.Unlock()
		}
		return nil
	}
	h.plc.mutex.Unlock()
	require.NoError(t, h.runner.Rollback(t.Context(), nil))
	_, hookErrors := h.plc.snapshot()
	require.Empty(t, hookErrors)
	mutex.Lock()
	times := append([]time.Time(nil), arrivals...)
	posted := append([]string(nil), paths...)
	mutex.Unlock()
	require.Equal(t, []string{"/" + dids[0], "/" + dids[1], "/" + dids[2]}, posted)
	var gaps []time.Duration
	for index := 1; index < len(times); index++ {
		gaps = append(gaps, times[index].Sub(times[index-1]))
	}
	require.Equal(t, []time.Duration{half, half}, gaps)
	for index, name := range []string{"one", "two", "three"} {
		require.Equal(t, name+".example-com.tidepool.test", h.row(t, dids[index]).Handle)
	}
}
