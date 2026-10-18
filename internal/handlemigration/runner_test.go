package handlemigration

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	internalerrors "tidepool/internal/errors"
	"tidepool/internal/identity"
	"tidepool/internal/repo"
	"tidepool/internal/store"
)

func TestRunner_ReservesBeforePLC(t *testing.T) {
	h := newRenameHarness(t)
	const (
		did       = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
		old       = "alice.my-lemmy-org.tidepool.test"
		newHandle = "alice.my-lemmy.org.ap.tidepool.test"
	)
	actor := h.insertOldActor(t, did, "https://my-lemmy.org/u/alice", old, "alice")
	h.reservationAtPLC(did, newHandle, actor.ID)
	require.NoError(t, h.runner.Run(t.Context(), []string{did}))
	requests, hookErrors := h.plc.snapshot()
	require.NotEmpty(t, requests, "the runner must reach PLC after reserving the handle")
	require.Equal(t, http.MethodGet, requests[0].Method)
	require.Equal(t, "/"+did+"/log/last", requests[0].Path)
	require.Empty(t, hookErrors, "a separate DB connection must see the pending handle and its claim at the first PLC request")
}

func TestRunner_NilDIDsRenamesEveryCandidateInInsertionOrder(t *testing.T) {
	h := newRenameHarness(t)
	const (
		user      = "did:plc:zuser"
		deleted   = "did:plc:adeleted"
		community = "did:plc:ycommunity"
		current   = "did:plc:bcurrent"
		external  = "did:plc:cexternal"
		nobridge  = "did:plc:dnobridge"
	)
	h.insertOldActor(t, user, "https://example.com/u/alice", "alice.example-com.tidepool.test", "alice")
	deletedActor := h.insertOldActor(t, deleted, "https://example.com/u/deleted", "deleted.example-com.tidepool.test", "deleted")
	require.NoError(t, h.actors.SetConsentState(t.Context(), deletedActor.APActorID, store.ConsentStateDeleted))
	group, err := h.actors.UpsertActor(t.Context(), store.BridgedActor{
		APActorID: "https://example.com/c/garden", ActorType: store.ActorTypeGroup,
		DID: community, Handle: "garden.example-com.tidepool.test", PreferredUsername: "garden", ConsentState: store.ConsentStateOK,
	})
	require.NoError(t, err)
	require.NotNil(t, group)
	h.insertOldActor(t, current, "https://example.com/u/current", "current.example.com.ap.tidepool.test", "current")
	h.insertOldActor(t, external, "https://example.com/u/external", "ext.example.net", "external")
	nobridgeActor := h.insertOldActor(t, nobridge, "https://example.com/u/nobridge", "nobridge.example-com.tidepool.test", "nobridge")
	require.NoError(t, h.actors.SetConsentState(t.Context(), nobridgeActor.APActorID, store.ConsentStateNoBridge))
	unchanged := map[string]renameRow{
		deleted: h.row(t, deleted), current: h.row(t, current), external: h.row(t, external),
	}

	require.NoError(t, h.runner.Run(t.Context(), nil))
	requests, hookErrors := h.plc.snapshot()
	require.Empty(t, hookErrors)
	var posts []string
	for _, request := range requests {
		if request.Method == http.MethodPost {
			posts = append(posts, request.Path)
		}
		for _, did := range []string{deleted, current, external} {
			require.NotEqual(t, "/"+did, request.Path)
			require.NotEqual(t, "/"+did+"/log/last", request.Path)
		}
	}
	require.Equal(t, []string{"/" + user, "/" + community, "/" + nobridge}, posts)
	assertFinalizedRename(t, h, user, "alice.example-com.tidepool.test", "alice.example.com.ap.tidepool.test")
	assertFinalizedRename(t, h, community, "garden.example-com.tidepool.test", "c-garden.example.com.ap.tidepool.test")
	assertFinalizedRename(t, h, nobridge, "nobridge.example-com.tidepool.test", "nobridge.example.com.ap.tidepool.test")
	for did, before := range unchanged {
		require.Equal(t, before, h.row(t, did), did)
	}
}

func TestRunner_PacesCandidatesWithoutCatchUpBursts(t *testing.T) {
	const (
		quarter = 250 * time.Millisecond
		half    = 500 * time.Millisecond
	)
	// Gaps are between consecutive PLC POST arrivals on the virtual clock.
	for _, tc := range []struct {
		name     string
		rate     float64
		explicit bool
		// slowPost advances the clock inside pacetwo's POST, after its arrival.
		slowPost time.Duration
		// slowHead advances the clock inside pacetwo's first PLC head read.
		slowHead time.Duration
		wantGaps []time.Duration
	}{
		{name: "nil dids", rate: 2, wantGaps: []time.Duration{half, half, half, half}},
		{name: "explicit dids", rate: 2, explicit: true, wantGaps: []time.Duration{half, half, half, half}},
		{name: "four per second", rate: 4, explicit: true, wantGaps: []time.Duration{quarter, quarter, quarter, quarter}},
		{name: "slow second actor", rate: 2, explicit: true, slowPost: 2 * time.Second,
			wantGaps: []time.Duration{half, 2 * time.Second, half, half}},
		{name: "slow head read", rate: 2, explicit: true, slowHead: 2 * time.Second,
			wantGaps: []time.Duration{2 * time.Second, half, half, half}},
		{name: "head read shorter than interval", rate: 2, explicit: true, slowHead: 200 * time.Millisecond,
			wantGaps: []time.Duration{half, half, half, half}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRenameHarness(t)
			runner, err := NewRunner(Options{Actors: h.actors, Identity: h.minter, ActorFetcher: h.actorClient,
				BridgeHostname: "tidepool.test", HandleZoneRoot: "ap.tidepool.test", Logger: h.logger,
				ActorsPerSecond: tc.rate, Now: h.clock.Now, Wait: h.clock.Wait})
			require.NoError(t, err)
			actors := []struct{ did, name, old, handle string }{
				{"did:plc:paceone", "one", "one.example-com.tidepool.test", "one.example.com.ap.tidepool.test"},
				{"did:plc:pacetwo", "two", "two.example-com.tidepool.test", "two.example.com.ap.tidepool.test"},
				{"did:plc:pacethree", "three", "three.example-com.tidepool.test", "three.example.com.ap.tidepool.test"},
				{"did:plc:pacefour", "four", "four.example-com.tidepool.test", "four.example.com.ap.tidepool.test"},
				{"did:plc:pacefive", "five", "five.example-com.tidepool.test", "five.example.com.ap.tidepool.test"},
			}
			for _, actor := range actors {
				h.insertOldActor(t, actor.did, "https://example.com/u/"+actor.name, actor.old, actor.name)
			}
			var mutex sync.Mutex
			var arrivals []time.Time
			inFlight, maximumInFlight := 0, 0
			headDelayed := false
			h.plc.mutex.Lock()
			h.plc.onArrival = func(request plcRequest) error {
				if request.Method == http.MethodPost {
					mutex.Lock()
					arrivals = append(arrivals, h.clock.Now())
					mutex.Unlock()
				}
				return nil
			}
			h.plc.onRequest = func(request plcRequest, _ *http.Request) error {
				mutex.Lock()
				inFlight++
				if inFlight > maximumInFlight {
					maximumInFlight = inFlight
				}
				mutex.Unlock()
				defer func() {
					mutex.Lock()
					inFlight--
					mutex.Unlock()
				}()
				if request.Method == http.MethodPost && request.Path == "/did:plc:pacetwo" {
					h.clock.Advance(tc.slowPost)
				}
				if request.Method == http.MethodGet && request.Path == "/did:plc:pacetwo/log/last" {
					mutex.Lock()
					first := !headDelayed
					headDelayed = true
					mutex.Unlock()
					if first {
						h.clock.Advance(tc.slowHead)
					}
				}
				return nil
			}
			h.plc.mutex.Unlock()
			var dids []string
			if tc.explicit {
				dids = []string{"did:plc:paceone", "did:plc:pacetwo", "did:plc:pacethree", "did:plc:pacefour", "did:plc:pacefive"}
			}
			require.NoError(t, runner.Run(t.Context(), dids))
			requests, hookErrors := h.plc.snapshot()
			require.Empty(t, hookErrors)
			var posts []string
			for _, request := range requests {
				if request.Method == http.MethodPost {
					posts = append(posts, request.Path)
				}
			}
			require.Equal(t, []string{"/did:plc:paceone", "/did:plc:pacetwo", "/did:plc:pacethree", "/did:plc:pacefour", "/did:plc:pacefive"}, posts)
			mutex.Lock()
			times := append([]time.Time(nil), arrivals...)
			maximum := maximumInFlight
			mutex.Unlock()
			require.Equal(t, 1, maximum)
			require.Len(t, times, 5)
			var gaps []time.Duration
			for index := 1; index < len(times); index++ {
				gaps = append(gaps, times[index].Sub(times[index-1]))
			}
			require.Equal(t, tc.wantGaps, gaps)
			for _, actor := range actors {
				assertFinalizedRename(t, h, actor.did, actor.old, actor.handle)
			}
		})
	}
}

type postTimesTransport struct {
	base  http.RoundTripper
	mutex sync.Mutex
	times []time.Time
}

func (transport *postTimesTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.Method == http.MethodPost {
		transport.mutex.Lock()
		transport.times = append(transport.times, time.Now())
		transport.mutex.Unlock()
	}
	return transport.base.RoundTrip(request)
}

func TestRunner_DefaultClockPacesPLCSubmissions(t *testing.T) {
	const rate = 10
	interval := time.Second / rate
	// The runner stamps a submission inside its gate and the transport sees
	// that POST a few microseconds later, so the earlier POST of a pair can be
	// recorded slightly late. The tolerance covers only that in-process gap.
	const tolerance = 5 * time.Millisecond
	recorder := &postTimesTransport{}
	h := newRenameHarnessWithTransport(t, nil, func(base http.RoundTripper) http.RoundTripper {
		recorder.base = base
		return recorder
	})
	var dids []string
	for _, name := range []string{"one", "two", "three", "four", "five"} {
		did := "did:plc:clock" + name
		h.insertOldActor(t, did, "https://example.com/u/"+name, name+".example-com.tidepool.test", name)
		dids = append(dids, did)
	}
	// Now and Wait are left nil so the runner uses the real clock and timer.
	runner, err := NewRunner(Options{Actors: h.actors, Identity: h.minter, ActorFetcher: h.actorClient,
		BridgeHostname: "tidepool.test", HandleZoneRoot: "ap.tidepool.test", Logger: h.logger,
		ActorsPerSecond: rate})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	require.NoError(t, runner.Run(ctx, dids))
	recorder.mutex.Lock()
	times := append([]time.Time(nil), recorder.times...)
	recorder.mutex.Unlock()
	require.Len(t, times, len(dids))
	for index := 1; index < len(times); index++ {
		require.GreaterOrEqual(t, times[index].Sub(times[index-1]), interval-tolerance, "gap before POST %d", index+1)
	}
	for _, name := range []string{"one", "two", "three", "four", "five"} {
		assertFinalizedRename(t, h, "did:plc:clock"+name, name+".example-com.tidepool.test", name+".example.com.ap.tidepool.test")
	}
}

func TestRunner_CancelledPacingSendsNoPLCUpdate(t *testing.T) {
	h := newRenameHarness(t)
	h.insertOldActor(t, "did:plc:paceone", "https://example.com/u/one", "one.example-com.tidepool.test", "one")
	h.insertOldActor(t, "did:plc:pacetwo", "https://example.com/u/two", "two.example-com.tidepool.test", "two")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var logs bytes.Buffer
	runner, err := NewRunner(Options{Actors: h.actors, Identity: h.minter, ActorFetcher: h.actorClient,
		BridgeHostname: "tidepool.test", HandleZoneRoot: "ap.tidepool.test",
		Logger:          slog.New(slog.NewTextHandler(&logs, nil)),
		ActorsPerSecond: 2, Now: h.clock.Now,
		Wait: func(ctx context.Context, _ time.Duration) error {
			cancel()
			return ctx.Err()
		}})
	require.NoError(t, err)
	err = runner.Run(ctx, []string{"did:plc:paceone", "did:plc:pacetwo"})
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorContains(t, err, "pace handle migration")
	requests, _ := h.plc.snapshot()
	var posts []string
	for _, request := range requests {
		if request.Method == http.MethodPost {
			posts = append(posts, request.Path)
		}
	}
	require.Equal(t, []string{"/did:plc:paceone"}, posts)
	require.NotContains(t, logs.String(), "PLC result unknown", "no POST was sent for the paced actor")
	row := h.row(t, "did:plc:pacetwo")
	require.Equal(t, "two.example-com.tidepool.test", row.Handle)
	require.Equal(t, "two.example.com.ap.tidepool.test", row.PendingHandle, "the reservation is kept for the next run")
}

type racingAvailability struct {
	store.BridgedActors
	database  *sql.DB
	withClaim bool
	competing *store.BridgedActor
	checked   bool
}

func (a *racingAvailability) HandleTaken(ctx context.Context, handle string) (bool, error) {
	if handle != "bob.example.com.ap.tidepool.test" || a.checked {
		return a.BridgedActors.HandleTaken(ctx, handle)
	}
	a.checked = true
	actor, err := a.UpsertActor(ctx, store.BridgedActor{
		APActorID: "https://example.com/u/competitor", ActorType: store.ActorTypePerson,
		DID: "did:plc:44ybard66vv44zksje25o7dz", Handle: handle,
		PreferredUsername: "competitor", ConsentState: store.ConsentStateOK,
	})
	if err == nil {
		a.competing = actor
		if !a.withClaim {
			_, err = a.database.ExecContext(ctx, `DELETE FROM handle_claims WHERE handle = $1`, handle)
		}
	}
	return false, err
}

func TestRunner_RetriesHandleTakenRace(t *testing.T) {
	for _, tc := range []struct {
		name      string
		withClaim bool
	}{
		{"competing claim", true}, {"legacy handle without claim", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var availability *racingAvailability
			h := newRenameHarnessWithActors(t, func(actors store.BridgedActors) store.BridgedActors {
				availability = &racingAvailability{BridgedActors: actors, withClaim: tc.withClaim}
				return availability
			})
			availability.database = h.db
			const did = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
			actor := h.insertOldActor(t, did, "https://example.com/u/bob", "bob.example-com.tidepool.test", "bob")
			h.reservationAtPLC(did, "bob-2.example.com.ap.tidepool.test", actor.ID)
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			require.NoError(t, h.runner.Run(ctx, []string{did}))
			requests, hookErrors := h.plc.snapshot()
			require.NotEmpty(t, requests, "retry must reach PLC")
			require.Empty(t, hookErrors, "suffixed reservation must precede PLC")
			require.True(t, availability.checked, "the original availability check must race")
			require.NotNil(t, availability.competing)
			require.Equal(t, "bob-2.example.com.ap.tidepool.test", h.row(t, did).Handle)
			require.Equal(t, "bob.example.com.ap.tidepool.test", h.row(t, availability.competing.DID).Handle)
			owner, claimed := h.claim(t, "bob.example.com.ap.tidepool.test")
			require.Equal(t, tc.withClaim, claimed)
			if tc.withClaim {
				require.Equal(t, availability.competing.ID, owner)
			}
		})
	}
}

type cancelAfterReservation struct {
	store.BridgedActors
	cancel context.CancelFunc
}

func (a cancelAfterReservation) SetPendingHandle(ctx context.Context, did, handle string) error {
	err := a.BridgedActors.SetPendingHandle(ctx, did, handle)
	if err == nil {
		a.cancel()
	}
	return err
}

func TestRunner_ResumesOriginalReservationAfterCancellation(t *testing.T) {
	h := newRenameHarness(t)
	const did = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	const apID = "https://example.com/u/alice"
	actor := h.insertOldActor(t, did, apID, "alice.example-com.tidepool.test", "alice")
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	runner := h.runnerWithActors(t, cancelAfterReservation{BridgedActors: h.actors, cancel: cancel})
	_ = runner.Run(ctx, []string{did}) // cancellation may be returned or consumed
	requests, _ := h.plc.snapshot()
	require.Empty(t, requests, "cancelled run must not contact PLC")
	require.Equal(t, "alice.example.com.ap.tidepool.test", h.row(t, did).PendingHandle)
	require.NoError(t, h.actors.MarkProfileSynced(t.Context(), apID, time.Now(), "renamed"))
	resumeContext, stop := context.WithTimeout(t.Context(), 15*time.Second)
	defer stop()
	require.NoError(t, h.runner.Run(resumeContext, []string{did}))
	requests, _ = h.plc.snapshot()
	require.NotEmpty(t, requests, "resume must update PLC")
	post := requests[len(requests)-1]
	require.Equal(t, http.MethodPost, post.Method)
	require.Equal(t, "/"+did, post.Path)
	require.Equal(t, []any{"at://alice.example.com.ap.tidepool.test"}, post.Body["alsoKnownAs"])
	require.Equal(t, "alice.example.com.ap.tidepool.test", h.row(t, did).Handle)
	owner, claimed := h.claim(t, "alice.example.com.ap.tidepool.test")
	require.True(t, claimed)
	require.Equal(t, actor.ID, owner)
	_, claimed = h.claim(t, "renamed.example.com.ap.tidepool.test")
	require.False(t, claimed)
}

func TestRunner_FinalizesAcceptedRename(t *testing.T) {
	h := newRenameHarness(t)
	const (
		did       = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
		old       = "alice.my-lemmy-org.tidepool.test"
		newHandle = "alice.my-lemmy.org.ap.tidepool.test"
	)
	actor := h.insertOldActor(t, did, "https://my-lemmy.org/u/alice", old, "alice")
	require.NoError(t, h.runner.Run(t.Context(), []string{did}))
	requests, _ := h.plc.snapshot()
	require.NotEmpty(t, requests, "the accepted PLC update must be sent")
	row := h.row(t, did)
	require.Equal(t, old, row.PreviousHandle)
	require.Equal(t, newHandle, row.Handle)
	require.Empty(t, row.PendingHandle)
	require.NotNil(t, row.HandleMigratedAt)
	owner, claimed := h.claim(t, newHandle)
	require.True(t, claimed)
	require.Equal(t, actor.ID, owner)
	_, claimed = h.claim(t, old)
	require.False(t, claimed)
	resolver := identity.NewStoreResolver(h.actors, "tidepool.test", "")
	_, err := resolver.ResolveHandle(t.Context(), old)
	require.True(t, internalerrors.IsNotFound(err), "old handle must no longer resolve: %v", err)
	got, err := resolver.ResolveHandle(t.Context(), newHandle)
	require.NoError(t, err)
	require.Equal(t, did, got)
	require.Equal(t, []string{newHandle}, h.identityHandles(t, did))
}

func TestFinalizeHandleRename_RollsBackCallbackAndEvent(t *testing.T) {
	h := newRenameHarness(t)
	const (
		did       = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
		old       = "alice.my-lemmy-org.tidepool.test"
		newHandle = "alice.my-lemmy.org.ap.tidepool.test"
	)
	actor := h.insertOldActor(t, did, "https://my-lemmy.org/u/alice", old, "alice")
	require.NoError(t, h.actors.SetPendingHandle(t.Context(), did, newHandle))
	before := h.row(t, did)
	require.Equal(t, newHandle, before.PendingHandle)
	oldOwner, oldClaimed := h.claim(t, old)
	newOwner, newClaimed := h.claim(t, newHandle)
	require.True(t, oldClaimed)
	require.True(t, newClaimed)
	require.Equal(t, actor.ID, newOwner)
	beforeEvents := h.identityHandles(t, did)
	callbackError := errors.New("reject transaction after identity append")
	renamed, err := h.actors.FinalizeHandleRename(t.Context(), did, newHandle,
		func(ctx context.Context, tx *sql.Tx, renamed *store.BridgedActor) error {
			if renamed == nil {
				return errors.New("finalize callback received nil actor")
			}
			if _, err := repo.AppendIdentityEventTx(ctx, tx, did, renamed.Handle); err != nil {
				return err
			}
			return callbackError
		})
	require.ErrorIs(t, err, callbackError)
	require.Nil(t, renamed)
	require.Equal(t, before, h.row(t, did))
	owner, claimed := h.claim(t, old)
	require.Equal(t, oldClaimed, claimed)
	require.Equal(t, oldOwner, owner)
	owner, claimed = h.claim(t, newHandle)
	require.Equal(t, newClaimed, claimed)
	require.Equal(t, newOwner, owner)
	require.Equal(t, beforeEvents, h.identityHandles(t, did))
}

func TestRunner_UsesStoredPreferredUsername(t *testing.T) {
	const did = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	for _, tc := range []struct {
		name, old, preferred, apID, want string
	}{
		{"hashed old label", "u77710aedc7.example-com.tidepool.test", "日本語", "https://example.com/u/japanese", "xn--wgv71a119e.example.com.ap.tidepool.test"},
		{"cross-host collision suffix", "alice-2.my-lemmy-org.tidepool.test", "alice", "https://my-lemmy.org/u/alice", "alice.my-lemmy.org.ap.tidepool.test"},
		{"truncated old label", "long-preferred.example-com.tidepool.test", "long-preferred-username", "https://example.com/u/long", "long-preferred-username.example.com.ap.tidepool.test"},
		{"actor id path has no username", "carol.example-com.tidepool.test", "carol", "https://example.com/ap/users/48213", "carol.example.com.ap.tidepool.test"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRenameHarness(t)
			h.insertOldActor(t, did, tc.apID, tc.old, tc.preferred)
			h.plc.onArrival = func(request plcRequest) error {
				if request.Path != "/"+did+"/log/last" {
					return nil
				}
				var pending string
				if err := h.db.QueryRowContext(context.Background(),
					`SELECT COALESCE(pending_handle, '') FROM bridged_actors WHERE did = $1`, did).Scan(&pending); err != nil {
					return err
				}
				if pending != tc.want {
					return fmt.Errorf("pending handle at PLC: got %q, want %q", pending, tc.want)
				}
				return nil
			}
			require.NoError(t, h.runner.Run(t.Context(), []string{did}))
			requests, hookErrors := h.plc.snapshot()
			require.NotEmpty(t, requests, "runner must update PLC using the saved preferred_username")
			require.Empty(t, hookErrors)
			require.Equal(t, tc.want, h.row(t, did).Handle)
			require.Equal(t, http.MethodPost, requests[len(requests)-1].Method)
		})
	}
}

type tombstoneAfterReservation struct {
	store.BridgedActors
	apActorID string
}

func (a tombstoneAfterReservation) SetPendingHandle(ctx context.Context, did, handle string) error {
	if err := a.BridgedActors.SetPendingHandle(ctx, did, handle); err != nil {
		return err
	}
	return a.SetConsentState(ctx, a.apActorID, store.ConsentStateDeleted)
}

func TestRunner_TombstonedAfterReservationSkipsPLC(t *testing.T) {
	h := newRenameHarness(t)
	const did = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	const apID = "https://example.com/u/alice"
	const old = "alice.example-com.tidepool.test"
	h.insertOldActor(t, did, apID, old, "alice")
	runner := h.runnerWithActors(t, tombstoneAfterReservation{BridgedActors: h.actors, apActorID: apID})
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	require.NoError(t, runner.Run(ctx, []string{did}))
	requests, _ := h.plc.snapshot()
	require.Empty(t, requests, "a tombstoned actor must not get a PLC operation")
	row := h.row(t, did)
	require.Equal(t, old, row.Handle)
	require.Empty(t, row.PreviousHandle)
	require.Nil(t, row.HandleMigratedAt)
	require.Empty(t, h.identityHandles(t, did))
}

func TestRunner_TombstonedDuringPLCDoesNotFinalize(t *testing.T) {
	h := newRenameHarness(t)
	const did = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	const apID = "https://example.com/u/alice"
	const old = "alice.example-com.tidepool.test"
	h.insertOldActor(t, did, apID, old, "alice")
	h.plc.mutex.Lock()
	h.plc.onRequest = func(request plcRequest, _ *http.Request) error {
		if request.Method != http.MethodPost || request.Path != "/"+did {
			return nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		return h.actors.SetConsentState(ctx, apID, store.ConsentStateDeleted)
	}
	h.plc.mutex.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	require.NoError(t, h.runner.Run(ctx, []string{did}))
	requests, hookErrors := h.plc.snapshot()
	require.Empty(t, hookErrors)
	require.NotEmpty(t, requests)
	require.Equal(t, http.MethodPost, requests[len(requests)-1].Method, "the PLC operation was submitted before the tombstone")
	row := h.row(t, did)
	require.Equal(t, old, row.Handle, "a tombstoned row is frozen")
	require.Empty(t, row.PreviousHandle)
	require.Nil(t, row.HandleMigratedAt)
	require.Empty(t, h.identityHandles(t, did), "no #identity frame may follow the tombstone")
}

type cancelAfterPLCAccepted struct {
	Identity
	cancel context.CancelFunc
}

func (i cancelAfterPLCAccepted) UpdateHandle(ctx context.Context, did, newHandle string, beforeSubmit func(context.Context) error) (identity.HandleUpdateResult, error) {
	result, err := i.Identity.UpdateHandle(ctx, did, newHandle, beforeSubmit)
	if err == nil {
		i.cancel()
	}
	return result, err
}

func TestRunner_FinalizesAfterCancellationOncePLCAccepted(t *testing.T) {
	h := newRenameHarness(t)
	const did = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	const old = "alice.example-com.tidepool.test"
	const newHandle = "alice.example.com.ap.tidepool.test"
	h.insertOldActor(t, did, "https://example.com/u/alice", old, "alice")
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	runner, err := NewRunner(Options{Actors: h.actors, Identity: cancelAfterPLCAccepted{Identity: h.minter, cancel: cancel}, ActorFetcher: h.actorClient,
		BridgeHostname: "tidepool.test", HandleZoneRoot: "ap.tidepool.test",
		ActorsPerSecond: 2, Now: h.clock.Now, Wait: h.clock.Wait})
	require.NoError(t, err)
	_ = runner.Run(ctx, []string{did}) // cancellation may be returned or consumed
	requests, _ := h.plc.snapshot()
	require.NotEmpty(t, requests)
	require.Equal(t, http.MethodPost, requests[len(requests)-1].Method)
	row := h.row(t, did)
	require.Equal(t, newHandle, row.Handle, "a PLC-accepted rename must be finalized despite cancellation")
	require.Equal(t, old, row.PreviousHandle)
	require.Empty(t, row.PendingHandle)
	require.Equal(t, []string{newHandle}, h.identityHandles(t, did))
}

func TestRunner_StoredPendingConflictKeepsReservation(t *testing.T) {
	h := newRenameHarness(t)
	const did = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	const otherDID = "did:plc:44ybard66vv44zksje25o7dz"
	const old = "alice.example-com.tidepool.test"
	const stored = "alice.example.com.ap.tidepool.test"
	actor := h.insertOldActor(t, did, "https://example.com/u/alice", old, "alice")
	require.NoError(t, h.actors.SetPendingHandle(t.Context(), did, stored))
	// A legacy row holding the stored pending handle as its live handle makes
	// the re-reservation conflict.
	h.insertOldActor(t, otherDID, "https://example.com/u/other", "other.example-com.tidepool.test", "other")
	_, err := h.db.ExecContext(t.Context(), `UPDATE bridged_actors SET handle = $1 WHERE did = $2`, stored, otherDID)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	require.NoError(t, h.runner.Run(ctx, []string{did}))
	requests, _ := h.plc.snapshot()
	require.Empty(t, requests, "an actor with a stored target must not get a second target")
	row := h.row(t, did)
	require.Equal(t, old, row.Handle)
	require.Equal(t, stored, row.PendingHandle)
	owner, claimed := h.claim(t, stored)
	require.True(t, claimed)
	require.Equal(t, actor.ID, owner)
	var claims int
	require.NoError(t, h.db.QueryRowContext(t.Context(), `SELECT count(*) FROM handle_claims WHERE actor_id = $1`, actor.ID).Scan(&claims))
	require.Equal(t, 2, claims, "only the live and stored pending claims may exist")
	var code sql.NullString
	require.NoError(t, h.db.QueryRowContext(t.Context(), `SELECT handle_migration_error FROM bridged_actors WHERE did = $1`, did).Scan(&code))
	require.False(t, code.Valid, "a reservation conflict is not an invalid handle, got %q", code.String)
}
