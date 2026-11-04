package handlemigration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"tidepool/internal/identity"
	"tidepool/internal/store"
)

// pacingWait is the interval of the harness budget at 120 writes a minute.
const pacingWait = 500 * time.Millisecond

// rateLimitRecorder holds the budget delays longer than pacing, each with the
// number of PLC requests that had arrived when the delay began.
type rateLimitRecorder struct {
	pauses          []time.Duration
	requestsAtPause []int
}

// newRateLimitRunner builds a runner on the harness budget. The budget's Wait
// records every rate-limit pause, separately from the identity retry delays
// in h.delays(), and onPause runs before the clock advances.
func newRateLimitRunner(t *testing.T, h *renameHarness, onPause func()) (*Runner, *rateLimitRecorder) {
	t.Helper()
	recorder := &rateLimitRecorder{}
	h.clock.onWait = func(delay time.Duration) {
		if delay == pacingWait {
			return
		}
		requests, _ := h.plc.snapshot()
		recorder.pauses = append(recorder.pauses, delay)
		recorder.requestsAtPause = append(recorder.requestsAtPause, len(requests))
		if onPause != nil {
			onPause()
		}
	}
	runner, err := NewRunner(Options{Actors: h.actors, Identity: h.minter, ActorFetcher: h.actorClient,
		BridgeHostname: "tidepool.test", HandleZoneRoot: "ap.tidepool.test", Logger: h.logger, Budget: h.budget})
	require.NoError(t, err)
	return runner, recorder
}

// captureRunnerLogs sends the JSON log of runners built after it to the
// returned buffer. Read it after the run returns.
func captureRunnerLogs(h *renameHarness) *bytes.Buffer {
	logs := &bytes.Buffer{}
	h.logger = slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return logs
}

// runnerLogMessages returns the messages logged at level for did.
func runnerLogMessages(t *testing.T, logs *bytes.Buffer, level, did string) []string {
	t.Helper()
	var messages []string
	for _, line := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var record map[string]any
		require.NoError(t, json.Unmarshal(line, &record))
		if record["level"] == level && record["did"] == did {
			messages = append(messages, fmt.Sprint(record["msg"]))
		}
	}
	return messages
}

type warnRecord struct {
	fields map[string]any
	raw    string
}

// warnRecordsFor returns the Minter's rate-limit warnings for did.
func warnRecordsFor(t *testing.T, logs *bytes.Buffer, did string) []warnRecord {
	t.Helper()
	var records []warnRecord
	for _, line := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var record map[string]any
		require.NoError(t, json.Unmarshal(line, &record))
		if record["level"] == "WARN" && record["msg"] == "PLC rate limited" && record["did"] == did {
			records = append(records, warnRecord{fields: record, raw: string(line)})
		}
	}
	return records
}

func isPLCRequestFor(request plcRequest, did string) bool {
	return request.Path == "/"+did || request.Path == "/"+did+"/log/last"
}

func TestRunner_PLCRateLimitPausesThenRetriesSameActor(t *testing.T) {
	const tooMany = http.StatusTooManyRequests
	for _, tc := range []struct {
		name string
		did  string
		// Every 429 pauses at once, so there is one pause per 429 and the POST
		// after each 429 arrives at that pause's end.
		wantPauses []time.Duration
		// wantPostOffsets are the POST arrivals of the run from the harness t0.
		wantPostOffsets []time.Duration
		wantHeaders     map[string]any
		setup           func(t *testing.T, h *renameHarness)
		run             func(ctx context.Context, runner *Runner) error
		verify          func(t *testing.T, h *renameHarness, requests []plcRequest)
	}{
		{
			name: "forward run honours Retry-After before the next DID",
			did:  plcFailureDID,
			wantPauses: []time.Duration{
				2 * time.Minute, 2 * time.Minute, 2 * time.Minute, 2 * time.Minute, 2 * time.Minute,
				2 * time.Minute, 2 * time.Minute, 2 * time.Minute, 2 * time.Minute, 2 * time.Minute,
			},
			wantPostOffsets: []time.Duration{
				0, 2 * time.Minute, 4 * time.Minute, 6 * time.Minute, 8 * time.Minute,
				10 * time.Minute, 12 * time.Minute, 14 * time.Minute, 16 * time.Minute, 18 * time.Minute,
				20 * time.Minute,            // the accepted POST
				20*time.Minute + pacingWait, // the next DID, one interval later
			},
			wantHeaders: map[string]any{"Retry-After": "120", "Ratelimit-Remaining": "0"},
			setup: func(t *testing.T, h *renameHarness) {
				h.insertOldActor(t, plcFailureDID, "https://example.com/u/alice", plcOldHandle, "alice")
				h.insertOldActor(t, plcNextDID, "https://example.com/u/bob", "bob.example-com.tidepool.test", "bob")
				h.plc.postStatusSequence = map[string][]int{"/" + plcFailureDID: {
					tooMany, tooMany, tooMany, tooMany, tooMany,
					tooMany, tooMany, tooMany, tooMany, tooMany,
					http.StatusOK,
				}}
				h.plc.postHeaders = map[string]map[string]string{"/" + plcFailureDID: {
					"Retry-After": "120", "Ratelimit-Remaining": "0", "Set-Cookie": "session=not-for-logs",
				}}
			},
			run: func(ctx context.Context, runner *Runner) error {
				return runner.Run(ctx, []string{plcFailureDID, plcNextDID})
			},
			verify: func(t *testing.T, h *renameHarness, requests []plcRequest) {
				lastFirstPost, firstNext := -1, -1
				for index, request := range requests {
					if request.Method == http.MethodPost && request.Path == "/"+plcFailureDID {
						lastFirstPost = index
					}
					if firstNext == -1 && isPLCRequestFor(request, plcNextDID) {
						firstNext = index
					}
				}
				require.NotEqual(t, -1, firstNext, "the next DID must be processed after the rate limit clears")
				require.Less(t, lastFirstPost, firstNext, "no request for the next DID before the rate-limited DID's accepted POST")
				assertFinalizedRename(t, h, plcFailureDID, plcOldHandle, plcNewHandle)
				assertFinalizedRename(t, h, plcNextDID, "bob.example-com.tidepool.test", "bob.example.com.ap.tidepool.test")
				for _, did := range []string{plcFailureDID, plcNextDID} {
					code, _ := migrationFailure(t, h, did)
					require.False(t, code.Valid, did)
				}
			},
		},
		{
			name:       "rollback waits an hour when Retry-After is missing",
			did:        rollbackDID,
			wantPauses: []time.Duration{time.Hour, time.Hour, time.Hour, time.Hour, time.Hour},
			// The forward rename's POST at t0 spaces the first of these by one interval.
			wantPostOffsets: []time.Duration{
				pacingWait, time.Hour + pacingWait, 2*time.Hour + pacingWait, 3*time.Hour + pacingWait,
				4*time.Hour + pacingWait, 5*time.Hour + pacingWait,
			},
			wantHeaders: map[string]any{},
			setup: func(t *testing.T, h *renameHarness) {
				// Index 0 is the forward rename's POST.
				h.plc.postStatusSequence = map[string][]int{"/" + rollbackDID: {
					http.StatusOK, tooMany, tooMany, tooMany, tooMany, tooMany, http.StatusOK,
				}}
				forwardRenamedForRollback(t, h)
			},
			run: func(ctx context.Context, runner *Runner) error {
				return runner.Rollback(ctx, []string{rollbackDID})
			},
			verify: func(t *testing.T, h *renameHarness, _ []plcRequest) {
				row := h.row(t, rollbackDID)
				require.Equal(t, rollbackOld, row.Handle)
				require.Equal(t, rollbackNew, row.PreviousHandle)
				require.Empty(t, row.PendingHandle)
				code, _ := migrationFailure(t, h, rollbackDID)
				require.False(t, code.Valid)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRenameHarness(t)
			t0 := h.clock.Now()
			tc.setup(t, h)
			prior, _ := h.plc.snapshot()
			runner, recorder := newRateLimitRunner(t, h, nil)
			require.NoError(t, tc.run(t.Context(), runner))
			requests, hookErrors := h.plc.snapshot()
			require.Empty(t, hookErrors)
			var postOffsets []time.Duration
			for _, request := range requests[len(prior):] {
				if request.Method == http.MethodPost {
					postOffsets = append(postOffsets, request.Arrived.Sub(t0))
				}
			}
			require.Equal(t, tc.wantPostOffsets, postOffsets, "POST arrivals from t0")
			require.Equal(t, tc.wantPauses, recorder.pauses, "budget waits other than pacing")
			warnings := warnRecordsFor(t, &h.minterLogs, tc.did)
			require.Len(t, warnings, len(tc.wantPauses), "one Minter warning per 429; logs: %s", h.minterLogs.String())
			for _, warning := range warnings {
				require.Equal(t, tc.wantHeaders, warning.fields["headers"], "only allowlisted headers: %s", warning.raw)
			}
			tc.verify(t, h, requests)
		})
	}
}

func TestRunner_PLCRateLimitPauseCancelledKeepsActorPending(t *testing.T) {
	h := newRenameHarness(t)
	first := h.insertOldActor(t, plcFailureDID, "https://example.com/u/alice", plcOldHandle, "alice")
	h.insertOldActor(t, plcNextDID, "https://example.com/u/bob", "bob.example-com.tidepool.test", "bob")
	h.plc.postStatuses = map[string]int{"/" + plcFailureDID: http.StatusTooManyRequests}
	h.plc.postHeaders = map[string]map[string]string{"/" + plcFailureDID: {"Retry-After": "120"}}
	logs := captureRunnerLogs(h)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runner, recorder := newRateLimitRunner(t, h, cancel)
	done := make(chan struct{})
	var runErr error
	go func() {
		defer close(done)
		runErr = runner.Run(ctx, []string{plcFailureDID, plcNextDID})
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after the pause was cancelled")
	}
	require.ErrorIs(t, runErr, context.Canceled)
	require.Empty(t, runnerLogMessages(t, logs, "ERROR", plcFailureDID), "stopping between attempts is not an error")
	require.Equal(t, []string{"PLC handle update stopped between attempts: the next run reconciles the PLC head"},
		runnerLogMessages(t, logs, "WARN", plcFailureDID))
	require.Equal(t, []time.Duration{2 * time.Minute}, recorder.pauses, "the rate limit must pause the run")
	requests, _ := h.plc.snapshot()
	require.Empty(t, requests[recorder.requestsAtPause[0]:], "no PLC request after the cancelled pause")
	for _, request := range requests {
		require.False(t, isPLCRequestFor(request, plcNextDID), "the next DID must not be reached")
	}
	assertPendingRename(t, h, plcFailureDID, plcOldHandle, plcNewHandle, first.ID)
	code, _ := migrationFailure(t, h, plcFailureDID)
	require.False(t, code.Valid)
	next := h.row(t, plcNextDID)
	require.Equal(t, "bob.example-com.tidepool.test", next.Handle)
	require.Empty(t, next.PendingHandle)
}

func TestRunner_TombstonedDuringPauseSendsNoFurtherPOST(t *testing.T) {
	const tooMany = http.StatusTooManyRequests
	for _, tc := range []struct {
		name string
		// setup makes the pause the actor is tombstoned in.
		setup func(h *renameHarness)
		// tombstoneInPause is the 1-based pause the tombstone lands in.
		tombstoneInPause int
		wantPOSTs        int
	}{
		{
			name: "pause from another writer before the first POST",
			setup: func(h *renameHarness) {
				h.budget.RateLimited("")
			},
			tombstoneInPause: 1,
			wantPOSTs:        0,
		},
		{
			name: "pause after a POST 429",
			setup: func(h *renameHarness) {
				h.plc.postStatusSequence = map[string][]int{"/" + plcFailureDID: {tooMany, http.StatusOK}}
			},
			tombstoneInPause: 1,
			wantPOSTs:        1,
		},
		{
			name: "pause after a second POST 429",
			setup: func(h *renameHarness) {
				h.plc.postStatusSequence = map[string][]int{"/" + plcFailureDID: {tooMany, tooMany, http.StatusOK}}
			},
			tombstoneInPause: 2,
			wantPOSTs:        2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRenameHarness(t)
			actor := h.insertOldActor(t, plcFailureDID, "https://example.com/u/alice", plcOldHandle, "alice")
			tc.setup(h)
			logs := captureRunnerLogs(h)
			pauses := 0
			runner, _ := newRateLimitRunner(t, h, func() {
				pauses++
				if pauses == tc.tombstoneInPause {
					require.NoError(t, h.actors.SetConsentState(t.Context(), actor.APActorID, store.ConsentStateDeleted))
				}
			})
			require.NoError(t, runner.Run(t.Context(), []string{plcFailureDID}))
			requests, hookErrors := h.plc.snapshot()
			require.Empty(t, hookErrors)
			require.Len(t, plcRequestsFor(requests, http.MethodPost, "/"+plcFailureDID), tc.wantPOSTs,
				"no POST may follow the tombstone")
			assertPendingRename(t, h, plcFailureDID, plcOldHandle, plcNewHandle, actor.ID)
			require.Empty(t, runnerLogMessages(t, logs, "ERROR", plcFailureDID))
			require.Equal(t, []string{"rename skipped: actor tombstoned before PLC update"},
				runnerLogMessages(t, logs, "WARN", plcFailureDID))
		})
	}
}

// countingRevertMark counts the write the rollback hook makes before each POST.
type countingRevertMark struct {
	store.BridgedActors
	calls int
}

func (a *countingRevertMark) MarkPendingRevertSubmitted(ctx context.Context, did, expectedPending string) error {
	a.calls++
	return a.BridgedActors.MarkPendingRevertSubmitted(ctx, did, expectedPending)
}

func TestRunner_RollbackTombstonedDuringPauseSendsNoFurtherPOST(t *testing.T) {
	var mark *countingRevertMark
	h := newRenameHarnessWithActors(t, func(actors store.BridgedActors) store.BridgedActors {
		mark = &countingRevertMark{BridgedActors: actors}
		return mark
	})
	actor := midForwardForRollback(t, h)
	h.plc.postStatusSequence = map[string][]int{"/" + rollbackDID: {http.StatusTooManyRequests, http.StatusOK}}
	logs := captureRunnerLogs(h)
	h.clock.onWait = func(delay time.Duration) {
		if delay == time.Hour {
			require.NoError(t, h.actors.SetConsentState(t.Context(), actor.APActorID, store.ConsentStateDeleted))
		}
	}
	require.NoError(t, h.runnerWithActors(t, mark).Rollback(t.Context(), []string{rollbackDID}))
	requests, hookErrors := h.plc.snapshot()
	require.Empty(t, hookErrors)
	require.Len(t, plcRequestsFor(requests, http.MethodPost, "/"+rollbackDID), 1, "no POST may follow the tombstone")
	require.Equal(t, 1, mark.calls, "the hook must not run for the POST the tombstone stopped")
	row := h.row(t, rollbackDID)
	require.Equal(t, rollbackOld, row.Handle, "a tombstoned row is frozen")
	require.Equal(t, rollbackNew, row.PendingHandle, "a tombstoned row is frozen")
	require.Empty(t, h.identityHandles(t, rollbackDID), "no #identity frame may follow the tombstone")
	require.Empty(t, runnerLogMessages(t, logs, "ERROR", rollbackDID))
	require.Equal(t, []string{"rename skipped: actor tombstoned before PLC update"},
		runnerLogMessages(t, logs, "WARN", rollbackDID))
}

// failingActorRead fails GetByDID while failing is set, cancelling first when
// cancel is set.
type failingActorRead struct {
	store.BridgedActors
	failing bool
	cancel  context.CancelFunc
}

func (a *failingActorRead) GetByDID(ctx context.Context, did string) (*store.BridgedActor, error) {
	if !a.failing {
		return a.BridgedActors.GetByDID(ctx, did)
	}
	if a.cancel != nil {
		a.cancel()
		return nil, ctx.Err()
	}
	return nil, errors.New("actor read failed")
}

func TestRunner_ActorReadFailureAfterPauseSendsNoPOST(t *testing.T) {
	setup := func(t *testing.T) (*renameHarness, *failingActorRead, *store.BridgedActor, *bytes.Buffer) {
		var reader *failingActorRead
		h := newRenameHarnessWithActors(t, func(actors store.BridgedActors) store.BridgedActors {
			reader = &failingActorRead{BridgedActors: actors}
			return reader
		})
		actor := h.insertOldActor(t, plcFailureDID, "https://example.com/u/alice", plcOldHandle, "alice")
		h.plc.postStatusSequence = map[string][]int{"/" + plcFailureDID: {http.StatusTooManyRequests, http.StatusOK}}
		h.clock.onWait = func(delay time.Duration) {
			if delay == time.Hour {
				reader.failing = true
			}
		}
		return h, reader, actor, captureRunnerLogs(h)
	}
	assertOnePOST := func(t *testing.T, h *renameHarness, actor *store.BridgedActor) {
		requests, hookErrors := h.plc.snapshot()
		require.Empty(t, hookErrors)
		require.Len(t, plcRequestsFor(requests, http.MethodPost, "/"+plcFailureDID), 1, "no POST after the failed read")
		assertPendingRename(t, h, plcFailureDID, plcOldHandle, plcNewHandle, actor.ID)
	}

	t.Run("a read error skips the actor", func(t *testing.T) {
		h, reader, actor, logs := setup(t)
		require.NoError(t, h.runnerWithActors(t, reader).Run(t.Context(), []string{plcFailureDID}))
		assertOnePOST(t, h, actor)
		require.Equal(t, []string{"re-read rename actor failed"}, runnerLogMessages(t, logs, "ERROR", plcFailureDID))
	})

	t.Run("a cancelled read stops the run", func(t *testing.T) {
		h, reader, actor, logs := setup(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		reader.cancel = cancel
		require.ErrorIs(t, h.runnerWithActors(t, reader).Run(ctx, []string{plcFailureDID}), context.Canceled)
		assertOnePOST(t, h, actor)
		require.Empty(t, runnerLogMessages(t, logs, "ERROR", plcFailureDID))
		require.Equal(t, []string{"PLC handle update stopped between attempts: the next run reconciles the PLC head"},
			runnerLogMessages(t, logs, "WARN", plcFailureDID))
	})
}

type updateHandleCall struct {
	did string
	at  time.Time
}

// rateLimitedOnceIdentity answers its first UpdateHandle as the Minter answers
// a 429 on the head read: the budget is told, no beforeSubmit runs, and the
// error wraps *identity.PLCRateLimitedError. Later calls submit and succeed.
type rateLimitedOnceIdentity struct {
	Identity
	budget *identity.PLCWriteBudget
	clock  *virtualClock
	calls  []updateHandleCall
}

func (i *rateLimitedOnceIdentity) UpdateHandle(ctx context.Context, did, _ string, beforeSubmit func(context.Context) error) (identity.HandleUpdateResult, error) {
	i.calls = append(i.calls, updateHandleCall{did: did, at: i.clock.Now()})
	if len(i.calls) == 1 {
		i.budget.RateLimited("120")
		return identity.HandleUpdateResult{}, fmt.Errorf("identity: GET PLC last operation for %s: %w",
			did, &identity.PLCRateLimitedError{Headers: map[string]string{"Retry-After": "120"}})
	}
	if err := beforeSubmit(ctx); err != nil {
		return identity.HandleUpdateResult{}, err
	}
	return identity.HandleUpdateResult{Outcome: identity.HandleUpdateApplied}, nil
}

func newRateLimitedOnceRunner(t *testing.T, h *renameHarness) (*Runner, *rateLimitedOnceIdentity) {
	t.Helper()
	fake := &rateLimitedOnceIdentity{Identity: h.minter, budget: h.budget, clock: h.clock}
	runner, err := NewRunner(Options{Actors: h.actors, Identity: fake, ActorFetcher: h.actorClient,
		BridgeHostname: "tidepool.test", HandleZoneRoot: "ap.tidepool.test", Budget: h.budget})
	require.NoError(t, err)
	return runner, fake
}

func TestRunner_RateLimitBeforeSubmitWaitsOutPauseAndRetriesSameActor(t *testing.T) {
	h := newRenameHarness(t)
	h.insertOldActor(t, plcFailureDID, "https://example.com/u/alice", plcOldHandle, "alice")
	runner, fake := newRateLimitedOnceRunner(t, h)
	require.NoError(t, runner.Run(t.Context(), []string{plcFailureDID}))
	require.Len(t, fake.calls, 2, "one call before the pause's end, one after it")
	// The harness clock only moves when the budget waits, so an offset of 0
	// is a retry that spun past the pause.
	require.Equal(t, 120*time.Second, fake.calls[1].at.Sub(fake.calls[0].at))
	require.Equal(t, plcFailureDID, fake.calls[1].did)
	assertFinalizedRename(t, h, plcFailureDID, plcOldHandle, plcNewHandle)
}

func TestRunner_RateLimitBeforeSubmitCancelledDuringPauseKeepsActorPending(t *testing.T) {
	h := newRenameHarness(t)
	actor := h.insertOldActor(t, plcFailureDID, "https://example.com/u/alice", plcOldHandle, "alice")
	runner, fake := newRateLimitedOnceRunner(t, h)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	h.clock.onWait = func(delay time.Duration) {
		if delay == 120*time.Second {
			cancel()
		}
	}
	err := runner.Run(ctx, []string{plcFailureDID})
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, fake.calls, 1)
	assertPendingRename(t, h, plcFailureDID, plcOldHandle, plcNewHandle, actor.ID)
}

// failingRevertMark fails the write the rollback hook makes before each POST.
type failingRevertMark struct {
	store.BridgedActors
	failed bool
}

func (a *failingRevertMark) MarkPendingRevertSubmitted(context.Context, string, string) error {
	a.failed = true
	return errors.New("mark pending revert submitted failed")
}

func TestRunner_RollbackHookFailureLeavesBudgetUncharged(t *testing.T) {
	var mark *failingRevertMark
	h := newRenameHarnessWithActors(t, func(actors store.BridgedActors) store.BridgedActors {
		mark = &failingRevertMark{BridgedActors: actors}
		return mark
	})
	midForwardForRollback(t, h)
	require.NoError(t, h.runnerWithActors(t, mark).Rollback(t.Context(), []string{rollbackDID}))
	require.True(t, mark.failed, "the rollback must reach the hook")
	requests, _ := h.plc.snapshot()
	require.Empty(t, plcRequestsFor(requests, http.MethodPost, "/"+rollbackDID), "a failed hook sends no POST")
	require.True(t, h.budget.Allow(), "a failed hook charges no token: Allow at the same instant")
}
