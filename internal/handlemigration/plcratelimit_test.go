package handlemigration

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// pacingWait is the harness pacing interval (1/ActorsPerSecond at 2/s).
const pacingWait = 500 * time.Millisecond

type observedPause struct {
	delay          time.Duration
	start          time.Time
	requestsBefore int
}

type rateLimitRecorder struct {
	mutex  sync.Mutex
	waits  []time.Duration
	pauses []observedPause
}

func (recorder *rateLimitRecorder) snapshot() ([]time.Duration, []observedPause) {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	return append([]time.Duration(nil), recorder.waits...), append([]observedPause(nil), recorder.pauses...)
}

// newRateLimitRunner builds a runner on the harness whose Wait records every
// runner delay, separately from the identity retry delays in h.delays(). A
// delay equal to pauseDelay is a rate-limit pause: its start on the virtual
// clock and the number of PLC requests that had arrived are recorded, and
// onPause runs before the clock advances.
func newRateLimitRunner(t *testing.T, h *renameHarness, pauseDelay time.Duration, onPause func()) (*Runner, *rateLimitRecorder, *bytes.Buffer) {
	t.Helper()
	recorder := &rateLimitRecorder{}
	var logs bytes.Buffer
	runner, err := NewRunner(Options{Actors: h.actors, Identity: h.minter, ActorFetcher: h.actorClient,
		BridgeHostname: "tidepool.test", HandleZoneRoot: "ap.tidepool.test",
		Logger:          slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		ActorsPerSecond: 2, Now: h.clock.Now,
		Wait: func(ctx context.Context, delay time.Duration) error {
			recorder.mutex.Lock()
			recorder.waits = append(recorder.waits, delay)
			if delay == pauseDelay {
				requests, _ := h.plc.snapshot()
				recorder.pauses = append(recorder.pauses, observedPause{delay: delay, start: h.clock.Now(), requestsBefore: len(requests)})
			}
			recorder.mutex.Unlock()
			if delay == pauseDelay && onPause != nil {
				onPause()
			}
			return h.clock.Wait(ctx, delay)
		}})
	require.NoError(t, err)
	return runner, recorder, &logs
}

type warnRecord struct {
	fields map[string]any
	raw    string
}

func warnRecordsFor(t *testing.T, logs *bytes.Buffer, did string) []warnRecord {
	t.Helper()
	var records []warnRecord
	for _, line := range bytes.Split(bytes.TrimSpace(logs.Bytes()), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var record map[string]any
		require.NoError(t, json.Unmarshal(line, &record))
		if record["level"] == "WARN" && record["did"] == did {
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
		name           string
		did            string
		pauseDelay     time.Duration
		wantPauses     []time.Duration
		wantWarnValues []string
		setup          func(t *testing.T, h *renameHarness)
		run            func(ctx context.Context, runner *Runner) error
		verify         func(t *testing.T, h *renameHarness, requests []plcRequest)
	}{
		{
			name:       "forward run honours Retry-After before the next DID",
			did:        plcFailureDID,
			pauseDelay: 2 * time.Minute, wantPauses: []time.Duration{2 * time.Minute, 2 * time.Minute},
			wantWarnValues: []string{"120"},
			setup: func(t *testing.T, h *renameHarness) {
				h.insertOldActor(t, plcFailureDID, "https://example.com/u/alice", plcOldHandle, "alice")
				h.insertOldActor(t, plcNextDID, "https://example.com/u/bob", "bob.example-com.tidepool.test", "bob")
				h.plc.postStatusSequence = map[string][]int{"/" + plcFailureDID: {
					tooMany, tooMany, tooMany, tooMany, tooMany,
					tooMany, tooMany, tooMany, tooMany, tooMany,
					http.StatusOK,
				}}
				h.plc.postHeaders = map[string]map[string]string{"/" + plcFailureDID: {"Retry-After": "120"}}
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
			pauseDelay: time.Hour, wantPauses: []time.Duration{time.Hour},
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
			tc.setup(t, h)
			prior, _ := h.plc.snapshot()
			runner, recorder, logs := newRateLimitRunner(t, h, tc.pauseDelay, nil)
			require.NoError(t, tc.run(t.Context(), runner))
			requests, hookErrors := h.plc.snapshot()
			require.Empty(t, hookErrors)
			waits, pauses := recorder.snapshot()
			var runnerPauses []time.Duration
			for _, delay := range waits {
				if delay != pacingWait {
					runnerPauses = append(runnerPauses, delay)
				}
			}
			require.Equal(t, tc.wantPauses, runnerPauses, "runner waits other than pacing")
			require.Len(t, pauses, len(tc.wantPauses))
			boundary := len(prior)
			for index, pause := range pauses {
				require.Len(t, plcRequestsFor(requests[boundary:pause.requestsBefore], http.MethodPost, "/"+tc.did), 5,
					"POST attempts before pause %d", index)
				for _, request := range requests[:pause.requestsBefore] {
					require.False(t, request.Arrived.After(pause.start), "request %s %s arrived after pause %d started", request.Method, request.Path, index)
				}
				require.Greater(t, len(requests), pause.requestsBefore, "a request must follow pause %d", index)
				resume := pause.start.Add(tc.wantPauses[index])
				require.False(t, requests[pause.requestsBefore].Arrived.Before(resume),
					"first request after pause %d arrived at %s, before %s", index, requests[pause.requestsBefore].Arrived, resume)
				boundary = pause.requestsBefore
			}
			require.Len(t, plcRequestsFor(requests[boundary:], http.MethodPost, "/"+tc.did), 1, "POST attempts after the last pause")
			warnings := warnRecordsFor(t, logs, tc.did)
			require.Len(t, warnings, len(tc.wantPauses), "one Warn per pause; logs: %s", logs.String())
			for _, warning := range warnings {
				require.Contains(t, warning.fields, "pause", warning.raw)
				require.Contains(t, warning.fields, "resume_at", warning.raw)
				for _, value := range tc.wantWarnValues {
					require.True(t, strings.Contains(warning.raw, `"`+value+`"`), "the pause record carries header value %q: %s", value, warning.raw)
				}
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
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	runner, recorder, _ := newRateLimitRunner(t, h, 2*time.Minute, cancel)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = runner.Run(ctx, []string{plcFailureDID, plcNextDID})
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after the pause was cancelled")
	}
	_, pauses := recorder.snapshot()
	require.Len(t, pauses, 1, "the rate limit must pause the run")
	requests, _ := h.plc.snapshot()
	require.Empty(t, requests[pauses[0].requestsBefore:], "no PLC request after the cancelled pause")
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
