package identity

import (
	"bytes"
	"context"
	"encoding/json"
	stderrors "errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/store"
)

const (
	orphanLogText    = "DID may be orphaned"
	budgetTestDID    = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	budgetTestHandle = "bob.example.com.ap.tidepool.test"
)

var budgetMintRequest = MintRequest{
	ActorType: store.ActorTypePerson, PreferredUsername: "alice", Instance: "lemmy.world",
}

var plcRateLimited120 = plcResponse{
	status: http.StatusTooManyRequests, headers: map[string]string{"Retry-After": "120"},
}

// plcResponse is one scripted directory answer. A zero status is the ordinary
// success answer for the request.
type plcResponse struct {
	status  int
	headers map[string]string
}

func (r plcResponse) write(w http.ResponseWriter) {
	for name, value := range r.headers {
		w.Header().Set(name, value)
	}
	w.WriteHeader(r.status)
}

type plcPost struct {
	path string
	at   time.Time
}

// scriptedPLC is an httptest PLC directory stamping POST arrival with the
// manual clock.
type scriptedPLC struct {
	clock *manualClock
	last  map[string]any
	// get answers GET /log/last by attempt, from 1; nil always succeeds.
	get func(attempt int) plcResponse
	// post answers a POST by path and attempt on that path, from 1; nil
	// always succeeds.
	post func(path string, attempt int) plcResponse

	mutex    sync.Mutex
	gets     int
	attempts map[string]int
	posts    []plcPost
}

func (p *scriptedPLC) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/"+budgetTestDID+"/log/last":
		p.mutex.Lock()
		p.gets++
		attempt := p.gets
		p.mutex.Unlock()
		if p.get != nil {
			if response := p.get(attempt); response.status != 0 {
				response.write(w)
				return
			}
		}
		_ = json.NewEncoder(w).Encode(p.last)
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/did:plc:"):
		p.mutex.Lock()
		p.attempts[r.URL.Path]++
		attempt := p.attempts[r.URL.Path]
		p.posts = append(p.posts, plcPost{path: r.URL.Path, at: p.clock.Now()})
		p.mutex.Unlock()
		if p.post != nil {
			if response := p.post(r.URL.Path, attempt); response.status != 0 {
				response.write(w)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	default:
		http.NotFound(w, r)
	}
}

func (p *scriptedPLC) postCount() int {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return len(p.posts)
}

// postTimes lists POST arrival times in arrival order.
func (p *scriptedPLC) postTimes() []time.Time {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	times := make([]time.Time, 0, len(p.posts))
	for _, post := range p.posts {
		times = append(times, post.at)
	}
	return times
}

func (p *scriptedPLC) awaitPosts(t *testing.T, count int) {
	t.Helper()
	awaitCondition(t, func() bool { return p.postCount() >= count }, "want %d POSTs", count)
}

// countingActors answers every handle free and counts the checks.
type countingActors struct{ checks atomic.Int64 }

func (a *countingActors) HandleTaken(context.Context, string) (bool, error) {
	a.checks.Add(1)
	return false, nil
}

type budgetFixtureOptions struct {
	get  func(attempt int) plcResponse
	post func(path string, attempt int) plcResponse
	// retried, when set, runs in place of each PLC retry delay.
	retried func(*PLCWriteBudget)
	// transport, when set, wraps the HTTP client's transport.
	transport func(http.RoundTripper) http.RoundTripper
}

// budgetFixture is a Minter on a 5 a minute budget and a manual clock, against
// a scripted PLC directory. PLC retry delays are no-ops.
type budgetFixture struct {
	clock  *manualClock
	budget *PLCWriteBudget
	plc    *scriptedPLC
	actors *countingActors
	minter *Minter
	logs   *bytes.Buffer
}

func newBudgetFixture(t *testing.T, options budgetFixtureOptions) *budgetFixture {
	t.Helper()
	rotationKey, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	rotationPublic, err := rotationKey.PublicKey()
	require.NoError(t, err)
	clock := newManualClock(t)
	plc := &scriptedPLC{
		clock: clock, get: options.get, post: options.post, attempts: make(map[string]int),
		last: map[string]any{
			"type": "plc_operation", "rotationKeys": []any{rotationPublic.DIDKey()},
			"verificationMethods": map[string]any{"atproto": rotationPublic.DIDKey()},
			"services":            map[string]any{"atproto_pds": map[string]any{"type": "AtprotoPersonalDataServer", "endpoint": "https://pds.example"}},
			"alsoKnownAs":         []any{"at://alice.example.com.tidepool.test"}, "prev": nil, "sig": "signature",
		},
	}
	server := httptest.NewServer(plc)
	t.Cleanup(server.Close)

	logs := new(bytes.Buffer)
	logger := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	budget, err := NewPLCWriteBudget(PLCWriteBudgetOptions{
		WritesPerMinute: 5, Logger: logger, Now: clock.Now, Wait: clock.Wait,
	})
	require.NoError(t, err)
	client := ap.NewGuardedHTTPClient(true, 10*time.Second)
	if options.transport != nil {
		client.Transport = options.transport(client.Transport)
	}
	actors := new(countingActors)
	minter, err := NewMinter(MinterOptions{
		PLCDirectoryURL: server.URL, BridgeHostname: "tidepool.test", HandleZoneRoot: "ap.tidepool.test",
		RotationKey: rotationKey, Custodian: testCustodian(t), Actors: actors,
		HTTPClient: client, Logger: logger, Budget: budget,
		PLCRetryWait: func(context.Context, time.Duration) error {
			if options.retried != nil {
				options.retried(budget)
			}
			return nil
		},
	})
	require.NoError(t, err)
	return &budgetFixture{clock: clock, budget: budget, plc: plc, actors: actors, minter: minter, logs: logs}
}

// startUpdate runs UpdateHandle with the hook the migration runner uses for
// pacing, budget.Wait. The result is valid once the call has returned.
func (f *budgetFixture) startUpdate(ctx context.Context) (*blockingCall, *HandleUpdateResult) {
	result := new(HandleUpdateResult)
	call := startCall("UpdateHandle", func() error {
		var err error
		*result, err = f.minter.UpdateHandle(ctx, budgetTestDID, budgetTestHandle, f.budget.Wait)
		return err
	})
	return call, result
}

func (f *budgetFixture) startMint(ctx context.Context, request MintRequest) *blockingCall {
	return startCall("MintActor", func() error {
		_, err := f.minter.MintActor(ctx, request)
		return err
	})
}

func TestMintActor_RateLimitedPausesBudget(t *testing.T) {
	fixture := newBudgetFixture(t, budgetFixtureOptions{
		post: func(string, int) plcResponse {
			return plcResponse{status: http.StatusTooManyRequests, headers: map[string]string{
				"Retry-After": "120", "RateLimit-Limit": "100", "RateLimit-Remaining": "0", "Set-Cookie": "secret=1",
			}}
		},
	})

	_, err := fixture.minter.MintActor(t.Context(), budgetMintRequest)

	var rateLimited *PLCRateLimitedError
	require.True(t, stderrors.As(err, &rateLimited), "a mint answered 429 must be a rate limit: %v", err)
	require.Equal(t, map[string]string{
		"Retry-After": "120", "Ratelimit-Limit": "100", "Ratelimit-Remaining": "0",
	}, rateLimited.Headers)
	require.NotContains(t, fixture.logs.String(), orphanLogText, "a 429 proves the op was not registered")
	require.NotContains(t, fixture.logs.String(), "secret=1", "only rate-limit headers may be logged")

	fixture.clock.Advance(12 * time.Second)
	require.False(t, fixture.budget.Allow(), "the ordinary slot has recovered but the pause holds")
	fixture.clock.Advance(108 * time.Second)
	require.True(t, fixture.budget.Allow(), "the pause ended at +120s")
}

func TestMintActor_ServerErrorKeepsOrphanLogAndDoesNotPause(t *testing.T) {
	fixture := newBudgetFixture(t, budgetFixtureOptions{
		post: func(string, int) plcResponse { return plcResponse{status: http.StatusInternalServerError} },
	})

	_, err := fixture.minter.MintActor(t.Context(), budgetMintRequest)

	require.Error(t, err)
	var rateLimited *PLCRateLimitedError
	require.False(t, stderrors.As(err, &rateLimited), "a 5xx is not a rate limit: %v", err)
	require.Contains(t, fixture.logs.String(), orphanLogText, "a 5xx may have registered the op")
	fixture.clock.Advance(12 * time.Second)
	require.True(t, fixture.budget.Allow(), "a 5xx must not pause the budget")
}

func TestMintActor_BeforeSubmitErrorSendsNothing(t *testing.T) {
	refused := stderrors.New("refused")
	fixture := newBudgetFixture(t, budgetFixtureOptions{})
	request := budgetMintRequest
	request.BeforeSubmit = func(context.Context) (bool, error) { return false, refused }

	_, err := fixture.minter.MintActor(t.Context(), request)

	require.ErrorIs(t, err, refused)
	require.Zero(t, fixture.plc.postCount())
	require.NotContains(t, fixture.logs.String(), orphanLogText, "nothing was submitted")
}

func TestMintActor_BeforeSubmitRunsBetweenHandleCheckAndPOST(t *testing.T) {
	fixture := newBudgetFixture(t, budgetFixtureOptions{})
	var handleChecksBefore int64
	postsBefore := -1
	request := budgetMintRequest
	request.BeforeSubmit = func(context.Context) (bool, error) {
		handleChecksBefore = fixture.actors.checks.Load()
		postsBefore = fixture.plc.postCount()
		return false, nil
	}

	_, err := fixture.minter.MintActor(t.Context(), request)

	require.NoError(t, err)
	require.EqualValues(t, 1, handleChecksBefore, "BeforeSubmit runs after HandleTaken")
	require.Zero(t, postsBefore, "BeforeSubmit runs before the POST")
	require.Equal(t, 1, fixture.plc.postCount())
}

func TestMintActor_RecheckLearnsWhetherTheMintWaited(t *testing.T) {
	for _, tc := range []struct {
		name         string
		beforeSubmit func(context.Context) (bool, error)
		want         bool
	}{
		{name: "nil BeforeSubmit may have blocked", want: true},
		{
			name:         "BeforeSubmit reports a wait",
			beforeSubmit: func(context.Context) (bool, error) { return true, nil },
			want:         true,
		},
		{
			name:         "BeforeSubmit reports no wait",
			beforeSubmit: func(context.Context) (bool, error) { return false, nil },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newBudgetFixture(t, budgetFixtureOptions{})
			var got []bool
			request := budgetMintRequest
			request.BeforeSubmit = tc.beforeSubmit
			request.Recheck = func(_ context.Context, waited bool) error {
				got = append(got, waited)
				return nil
			}

			_, err := fixture.minter.MintActor(t.Context(), request)

			require.NoError(t, err)
			require.Equal(t, []bool{tc.want}, got)
		})
	}
}

func TestMintActor_NilBeforeSubmitCancelledWhileWaitingSendsNothing(t *testing.T) {
	fixture := newBudgetFixture(t, budgetFixtureOptions{})
	ctx, cancel := cancellableContext(t)

	fixture.budget.RateLimited("120")
	mint := fixture.startMint(ctx, budgetMintRequest)
	fixture.clock.AwaitWaiters(1, mint)
	cancel()

	require.ErrorIs(t, mint.result(t), context.Canceled)
	require.Zero(t, fixture.plc.postCount())
	require.NotContains(t, fixture.logs.String(), orphanLogText, "nothing was submitted")
}

func TestNewMinter_RejectsNilBudget(t *testing.T) {
	rotationKey, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)

	_, err = NewMinter(MinterOptions{
		PLCDirectoryURL: "https://plc.invalid", BridgeHostname: "tidepool.test", HandleZoneRoot: "ap.tidepool.test",
		RotationKey: rotationKey, Custodian: testCustodian(t), Actors: new(countingActors),
		HTTPClient: http.DefaultClient,
	})

	require.Error(t, err)
	assert.True(t, errors.IsValidation(err), "got %v", err)
}

// heldBody blocks every Read until released, and reports the first Read.
type heldBody struct {
	reading chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *heldBody) Read([]byte) (int, error) {
	b.once.Do(func() { close(b.reading) })
	<-b.release
	return 0, io.EOF
}

func (b *heldBody) Close() error { return nil }

// heldBodyTransport answers every POST with 429 headers and body; everything
// else goes to inner.
type heldBodyTransport struct {
	inner http.RoundTripper
	body  *heldBody
}

func (h heldBodyTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.Method != http.MethodPost {
		return h.inner.RoundTrip(request)
	}
	if request.Body != nil {
		_ = request.Body.Close()
	}
	return &http.Response{
		Status: "429 Too Many Requests", StatusCode: http.StatusTooManyRequests,
		Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header: http.Header{"Retry-After": {"120"}}, Body: h.body, ContentLength: -1, Request: request,
	}, nil
}

func TestRateLimit_BudgetPausedWhileResponseBodyHeld(t *testing.T) {
	for _, tc := range []struct {
		name  string
		start func(*budgetFixture, context.Context) *blockingCall
	}{
		{"MintActor", func(fixture *budgetFixture, ctx context.Context) *blockingCall {
			return fixture.startMint(ctx, budgetMintRequest)
		}},
		{"UpdateHandle", func(fixture *budgetFixture, ctx context.Context) *blockingCall {
			call, _ := fixture.startUpdate(ctx)
			return call
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &heldBody{reading: make(chan struct{}), release: make(chan struct{})}
			fixture := newBudgetFixture(t, budgetFixtureOptions{
				transport: func(inner http.RoundTripper) http.RoundTripper {
					return heldBodyTransport{inner: inner, body: body}
				},
			})
			ctx, cancel := cancellableContext(t)
			t.Cleanup(func() {
				close(body.release)
				cancel()
			})

			call := tc.start(fixture, ctx)
			select {
			case <-body.reading:
			case <-call.finished:
			case <-time.After(stallDeadline):
				t.Fatal("stalled: the client neither read the body nor returned")
			}

			fixture.clock.Advance(12 * time.Second)
			require.False(t, fixture.budget.Allow(), "the status alone pauses the budget, with the body held")
			fixture.clock.Advance(108 * time.Second)
			require.True(t, fixture.budget.Allow(), "the pause came from the held response's Retry-After 120")
		})
	}
}
