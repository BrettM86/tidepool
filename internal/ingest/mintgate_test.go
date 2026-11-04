package ingest

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/identity"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

const (
	// stallDeadline bounds every blocking step. It only turns a stall into a
	// failure; it never establishes ordering.
	stallDeadline = 3 * time.Second

	orphanLogText = "DID may be orphaned"
	renameDID     = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	renameHandle  = "bob.example.com.ap.tidepool.test"
)

var clockStart = time.Date(2026, time.October, 18, 21, 58, 0, 0, time.UTC)

// manualClock feeds PLCWriteBudgetOptions{Now, Wait}. Time only moves through
// Advance.
type manualClock struct {
	t       *testing.T
	mutex   sync.Mutex
	now     time.Time
	waiters map[*clockWaiter]struct{}
}

type clockWaiter struct {
	deadline time.Time
	released chan struct{}
}

func newManualClock(t *testing.T) *manualClock {
	return &manualClock{t: t, now: clockStart, waiters: make(map[*clockWaiter]struct{})}
}

func (c *manualClock) Now() time.Time {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.now
}

func (c *manualClock) Advance(delay time.Duration) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.now = c.now.Add(delay)
	for waiter := range c.waiters {
		if !waiter.deadline.After(c.now) {
			delete(c.waiters, waiter)
			close(waiter.released)
		}
	}
}

// Wait blocks until the clock reaches now+delay or ctx ends.
func (c *manualClock) Wait(ctx context.Context, delay time.Duration) error {
	c.mutex.Lock()
	if delay <= 0 {
		c.mutex.Unlock()
		return ctx.Err()
	}
	waiter := &clockWaiter{deadline: c.now.Add(delay), released: make(chan struct{})}
	c.waiters[waiter] = struct{}{}
	c.mutex.Unlock()
	select {
	case <-waiter.released:
		return ctx.Err()
	case <-ctx.Done():
		c.mutex.Lock()
		delete(c.waiters, waiter)
		c.mutex.Unlock()
		return ctx.Err()
	}
}

func (c *manualClock) waiterCount() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return len(c.waiters)
}

// AwaitWaiters blocks until every listed call has returned or sits in Wait. A
// call that should have stayed blocked but returned fails at the test's own
// assertion, not here.
func (c *manualClock) AwaitWaiters(calls ...*blockingCall) {
	c.t.Helper()
	awaitCondition(c.t, func() bool {
		running := 0
		for _, call := range calls {
			if !call.returned() {
				running++
			}
		}
		return c.waiterCount() == running
	}, "want every running call in Wait")
}

// awaitCondition polls condition on the test goroutine until it holds, and
// fails after stallDeadline.
func awaitCondition(t *testing.T, condition func() bool, format string, args ...any) {
	t.Helper()
	stall := time.After(stallDeadline)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !condition() {
		select {
		case <-tick.C:
		case <-stall:
			t.Fatalf("stalled: %s", fmt.Sprintf(format, args...))
		}
	}
}

// blockingCall is a call running on its own goroutine.
type blockingCall struct {
	name     string
	finished chan struct{}
	err      error
}

func startCall(name string, run func() error) *blockingCall {
	call := &blockingCall{name: name, finished: make(chan struct{})}
	go func() {
		defer close(call.finished)
		call.err = run()
	}()
	return call
}

func (c *blockingCall) returned() bool {
	select {
	case <-c.finished:
		return true
	default:
		return false
	}
}

// result waits for the call to return and gives its error.
func (c *blockingCall) result(t *testing.T) error {
	t.Helper()
	select {
	case <-c.finished:
		return c.err
	case <-time.After(stallDeadline):
		t.Fatalf("stalled: %s did not return", c.name)
		return nil
	}
}

type plcPost struct {
	path string
	at   time.Time
}

// scriptedPLC is an httptest PLC directory stamping POST arrival with the
// manual clock. It answers every POST 200, except that with
// renameRateLimited the first POST to renameDID is answered 429 Retry-After 60,
// with mintRateLimited so is the first POST to any other DID, and with
// rateLimitedPost so is the POST of that ordinal, counting from 1. A set
// retryAfter replaces the 60.
type scriptedPLC struct {
	clock             *manualClock
	last              map[string]any
	renameRateLimited bool
	mintRateLimited   bool
	rateLimitedPost   int
	retryAfter        string

	mutex sync.Mutex
	posts []plcPost
}

func (p *scriptedPLC) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/"+renameDID+"/log/last":
		_ = json.NewEncoder(w).Encode(p.last)
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/did:plc:"):
		p.mutex.Lock()
		p.posts = append(p.posts, plcPost{path: r.URL.Path, at: p.clock.Now()})
		rateLimited := p.renameRateLimited && r.URL.Path == "/"+renameDID
		p.renameRateLimited = p.renameRateLimited && !rateLimited
		if p.mintRateLimited && r.URL.Path != "/"+renameDID {
			p.mintRateLimited = false
			rateLimited = true
		}
		if p.rateLimitedPost == len(p.posts) {
			rateLimited = true
		}
		p.mutex.Unlock()
		if rateLimited {
			w.Header().Set("Retry-After", cmp.Or(p.retryAfter, "60"))
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	default:
		http.NotFound(w, r)
	}
}

// recordedPosts lists POSTs in arrival order.
func (p *scriptedPLC) recordedPosts() []plcPost {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return append([]plcPost(nil), p.posts...)
}

func (p *scriptedPLC) awaitPosts(t *testing.T, count int) {
	t.Helper()
	awaitCondition(t, func() bool { return len(p.recordedPosts()) >= count }, "want %d POSTs", count)
}

// heldActors answers every handle free, each check held until release closes.
type heldActors struct {
	entered chan struct{}
	release chan struct{}
	enter   sync.Once
}

func (a *heldActors) HandleTaken(ctx context.Context, _ string) (bool, error) {
	a.enter.Do(func() { close(a.entered) })
	select {
	case <-a.release:
		return false, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (a *heldActors) awaitEntered(t *testing.T) {
	t.Helper()
	select {
	case <-a.entered:
	case <-time.After(stallDeadline):
		t.Fatal("stalled: the mint never reached the handle check")
	}
}

// mintFixture is a real Minter on a 5 a minute budget and a manual clock,
// against a scripted PLC directory. PLC retry delays are no-ops.
type mintFixture struct {
	clock  *manualClock
	budget *identity.PLCWriteBudget
	plc    *scriptedPLC
	actors *heldActors
	minter *identity.Minter
	logger *slog.Logger
	logs   *bytes.Buffer
}

func newMintFixture(t *testing.T, renameRateLimited bool) *mintFixture {
	t.Helper()
	custodian, err := identity.NewCustodian(testKEK)
	require.NoError(t, err)
	actors := &heldActors{entered: make(chan struct{}), release: make(chan struct{})}
	fixture := newMintFixtureOver(t, actors, custodian)
	fixture.actors = actors
	fixture.plc.renameRateLimited = renameRateLimited
	return fixture
}

// handleChecker is the Minter's handle-collision lookup.
type handleChecker interface {
	HandleTaken(ctx context.Context, handle string) (bool, error)
}

// newMintFixtureOver is the fixture over the given handle lookup and custodian.
// The actors field stays nil.
func newMintFixtureOver(t *testing.T, actors handleChecker, custodian *identity.Custodian) *mintFixture {
	t.Helper()
	rotationKey, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	rotationPublic, err := rotationKey.PublicKey()
	require.NoError(t, err)

	clock := newManualClock(t)
	plc := &scriptedPLC{
		clock: clock,
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
	budget, err := identity.NewPLCWriteBudget(identity.PLCWriteBudgetOptions{
		WritesPerMinute: 5, Logger: logger, Now: clock.Now, Wait: clock.Wait,
	})
	require.NoError(t, err)
	minter, err := identity.NewMinter(identity.MinterOptions{
		PLCDirectoryURL: server.URL, BridgeHostname: "tidepool.test", HandleZoneRoot: "ap.tidepool.test",
		RotationKey: rotationKey, Custodian: custodian, Actors: actors,
		HTTPClient: ap.NewGuardedHTTPClient(true, 10*time.Second), Logger: logger, Budget: budget,
		PLCRetryWait: func(context.Context, time.Duration) error { return nil },
	})
	require.NoError(t, err)
	return &mintFixture{clock: clock, budget: budget, plc: plc, minter: minter, logger: logger, logs: logs}
}

func (f *mintFixture) gate(t *testing.T) *MintGate {
	t.Helper()
	gate, err := NewMintGate(f.minter, f.budget, f.logger)
	require.NoError(t, err)
	return gate
}

func (f *mintFixture) startMint(ctx context.Context, minter materialize.ActorMinter, username string) *blockingCall {
	call, _ := f.startMintKeepingIdentity(ctx, minter, username)
	return call
}

// startMintKeepingIdentity is startMint with the minted identity, filled in
// once the call returns.
func (f *mintFixture) startMintKeepingIdentity(ctx context.Context, minter materialize.ActorMinter, username string) (*blockingCall, *identity.Identity) {
	minted := new(identity.Identity)
	call := startCall("MintActor", func() error {
		result, err := minter.MintActor(ctx, identity.MintRequest{
			ActorType: store.ActorTypePerson, PreferredUsername: username, Instance: "evil.example",
		})
		if err != nil {
			return err
		}
		*minted = *result
		return nil
	})
	return call, minted
}

func requireStillBlocked(t *testing.T, call *blockingCall, reason string) {
	t.Helper()
	if call.returned() {
		t.Fatalf("%s: %s returned: %v", reason, call.name, call.err)
	}
}

// TestMintGate_RefusesPastTheBudgetWithoutBlocking: the clock never advances,
// so a gate that waited for a slot stalls and fails. The second row is the
// deep-thread amplification attack: many distinct authors arriving at once.
func TestMintGate_RefusesPastTheBudgetWithoutBlocking(t *testing.T) {
	for _, tc := range []struct {
		name        string
		exhausted   bool
		authors     int
		wantPosts   int
		wantRefused int
	}{
		{name: "exhausted budget", exhausted: true, authors: 1, wantPosts: 0, wantRefused: 1},
		{name: "50 distinct authors on a full budget", authors: 50, wantPosts: 1, wantRefused: 49},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newMintFixture(t, false)
			close(fixture.actors.release)
			gate := fixture.gate(t)
			ctx := t.Context()
			if tc.exhausted {
				require.NoError(t, fixture.budget.Wait(ctx), "a fresh budget admits one write")
			}

			refused := 0
			for author := range tc.authors {
				err := fixture.startMint(ctx, gate, fmt.Sprintf("troll-%d", author)).result(t)
				if err == nil {
					continue
				}
				require.ErrorIs(t, err, ErrMintRateExceeded)
				require.False(t, materialize.IsSkip(err), "a refused mint must be retryable, not a skip")
				refused++
			}

			require.Equal(t, tc.wantPosts, len(fixture.plc.recordedPosts()))
			require.Equal(t, tc.wantRefused, refused)
			require.NotContains(t, fixture.logs.String(), orphanLogText, "a refused mint submitted nothing")
		})
	}
}

// TestMintGate_MarkedMintWaitsOutTheBudget: the backfill's mint holds until the
// budget frees a slot rather than being refused.
func TestMintGate_MarkedMintWaitsOutTheBudget(t *testing.T) {
	fixture := newMintFixture(t, false)
	close(fixture.actors.release)
	gate := fixture.gate(t)
	ctx := withBudgetWait(t.Context())
	require.NoError(t, fixture.budget.Wait(ctx), "a fresh budget admits one write")

	mint, minted := fixture.startMintKeepingIdentity(ctx, gate, "alice")
	fixture.clock.AwaitWaiters(mint)
	requireStillBlocked(t, mint, "a marked mint past the budget waits")
	require.Empty(t, fixture.plc.recordedPosts())

	fixture.clock.Advance(12 * time.Second)
	require.NoError(t, mint.result(t))
	assert.True(t, strings.HasPrefix(minted.DID, "did:plc:"), "got DID %q", minted.DID)
	require.Len(t, fixture.plc.recordedPosts(), 1)
}

// TestMintGate_PLCRateLimitsTheMint: a PLC 429 registers nothing. A marked mint
// waits out the pause and mints again; an unmarked one hands the 429 back,
// because a retry would hold an inbox worker for the whole pause.
func TestMintGate_PLCRateLimitsTheMint(t *testing.T) {
	newRateLimitedFixture := func(t *testing.T) *mintFixture {
		t.Helper()
		fixture := newMintFixture(t, false)
		fixture.plc.mintRateLimited = true
		close(fixture.actors.release)
		return fixture
	}

	t.Run("marked mint resumes when the pause ends", func(t *testing.T) {
		fixture := newRateLimitedFixture(t)
		mint, minted := fixture.startMintKeepingIdentity(withBudgetWait(t.Context()), fixture.gate(t), "alice")
		fixture.plc.awaitPosts(t, 1)
		fixture.clock.AwaitWaiters(mint)
		requireStillBlocked(t, mint, "a marked mint waits out the pause")

		fixture.clock.Advance(59 * time.Second)
		fixture.clock.AwaitWaiters(mint)
		requireStillBlocked(t, mint, "the pause lasts 60 seconds")
		require.Len(t, fixture.plc.recordedPosts(), 1)

		fixture.clock.Advance(time.Second)
		require.NoError(t, mint.result(t))
		assert.True(t, strings.HasPrefix(minted.DID, "did:plc:"), "got DID %q", minted.DID)
		require.Len(t, fixture.plc.recordedPosts(), 2)
		require.NotContains(t, fixture.logs.String(), orphanLogText, "a 429 registered nothing")
	})

	t.Run("marked mint cancelled during the pause", func(t *testing.T) {
		fixture := newRateLimitedFixture(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		mint := fixture.startMint(withBudgetWait(ctx), fixture.gate(t), "alice")
		fixture.plc.awaitPosts(t, 1)
		fixture.clock.AwaitWaiters(mint)
		requireStillBlocked(t, mint, "a marked mint waits out the pause")

		cancel()
		require.ErrorIs(t, mint.result(t), context.Canceled)
		require.Len(t, fixture.plc.recordedPosts(), 1)
	})

	t.Run("unmarked mint returns the 429 at once", func(t *testing.T) {
		fixture := newRateLimitedFixture(t)
		err := fixture.startMint(t.Context(), fixture.gate(t), "alice").result(t)
		var rateLimited *identity.PLCRateLimitedError
		require.ErrorAs(t, err, &rateLimited)
		require.Len(t, fixture.plc.recordedPosts(), 1)
	})
}

// TestMintGate_SweepMintWaitsForATokenButNotForAPause: under withSweepMint a
// spent token is waited for, and a pause or a PLC 429 hands the mint straight
// back, so a sweep never sits in a pause holding its lock.
func TestMintGate_SweepMintWaitsForATokenButNotForAPause(t *testing.T) {
	fixture := newMintFixture(t, false)
	fixture.plc.rateLimitedPost = 2
	close(fixture.actors.release)
	gate := fixture.gate(t)
	ctx := withSweepMint(t.Context())
	require.NoError(t, fixture.budget.Wait(ctx), "a fresh budget admits one write")

	waited := fixture.startMint(ctx, gate, "waited")
	fixture.clock.AwaitWaiters(waited)
	requireStillBlocked(t, waited, "a sweep mint past the budget waits for its token")
	require.Empty(t, fixture.plc.recordedPosts())
	fixture.clock.Advance(12 * time.Second)
	require.NoError(t, waited.result(t))
	require.Len(t, fixture.plc.recordedPosts(), 1)

	paused := fixture.startMint(ctx, gate, "paused")
	fixture.clock.AwaitWaiters(paused)
	requireStillBlocked(t, paused, "the next token is 12 seconds away")
	fixture.budget.RateLimited("60")
	err := paused.result(t)
	require.ErrorIs(t, err, ErrMintRateExceeded)
	require.False(t, materialize.IsSkip(err), "a pause must not read as a refusal")

	during := fixture.startMint(ctx, gate, "during")
	require.ErrorIs(t, during.result(t), ErrMintRateExceeded)
	require.Len(t, fixture.plc.recordedPosts(), 1, "neither mint reached PLC")

	fixture.clock.Advance(60 * time.Second)
	limited := fixture.startMint(ctx, gate, "limited")
	var rateLimited *identity.PLCRateLimitedError
	require.ErrorAs(t, limited.result(t), &rateLimited)
	require.Len(t, fixture.plc.recordedPosts(), 2, "a 429 is not retried")
	require.Zero(t, fixture.clock.waiterCount())
}

// TestMintGate_SweepMintThatWaitedReadsTheActorAgain: a Group that opts out while
// a sweep's mint waits for its token must not get a DID, which is permanent.
func TestMintGate_SweepMintThatWaitedReadsTheActorAgain(t *testing.T) {
	const actorID = "https://lemmy.world/u/sweptauthor"
	h := newHarness(t)
	origin := h.serveActorOrigin("/u/sweptauthor", person(actorID, "sweptauthor", nil))
	materializer, fixture := newBudgetedMaterializer(t, h)
	ctx := withSweepMint(t.Context())
	require.NoError(t, fixture.budget.Wait(ctx), "a fresh budget admits one write")

	mint := startEnsureActor(t, ctx, materializer, actorID)
	fixture.clock.AwaitWaiters(mint)
	requireStillBlocked(t, mint, "the mint waits for the budget")
	origin.serve(t, person(actorID, "sweptauthor", map[string]any{"summary": "<p>#nobridge</p>"}))
	fixture.clock.Advance(12 * time.Second)

	err := mint.result(t)
	require.True(t, materialize.IsSkip(err), "want a skip, got %v", err)
	assert.Empty(t, fixture.plc.recordedPosts())
	requireNoActorRow(t, h, actorID)
}

// TestMintGate_SweepMintStopsForAPauseDuringTheRecheck: the second add waits for
// its token, then reads its Group again, and a pause starts during that read.
// It sends no POST, which PLC would answer with a 429 extending the pause. The
// add is a leftover, not a failure, so the first sweep past the pause adds it.
func TestMintGate_SweepMintStopsForAPauseDuringTheRecheck(t *testing.T) {
	h := newHarness(t)
	rig, mint := newPacedRig(t, h, instanceFollowList(lemmyFixtureHost))
	for _, name := range numberedIDs("listed-", 3) {
		rig.addCommunity(lemmyFixtureHost, name)
	}
	ids := numberedIDs("https://lemmy.world/c/listed-", 3)

	sweep, result := rig.startSweep(t)
	mint.plc.awaitPosts(t, 1)
	rig.clock.AwaitWaiters(sweep)
	requireStillBlocked(t, sweep, "the second add waits for its token")

	// Set only now, so the next fetch of the Group is the read after the wait.
	entered, released := make(chan struct{}), make(chan struct{})
	release := sync.OnceFunc(func() { close(released) })
	t.Cleanup(release)
	signalEntered := sync.OnceFunc(func() { close(entered) })
	h.mu.Lock()
	h.onFetch[urlPath(t, ids[1])] = func() {
		signalEntered()
		<-released
	}
	h.mu.Unlock()
	rig.clock.Advance(12 * time.Second)
	requireClosed(t, entered, "the waited mint must read the Group again")
	mint.budget.RateLimited("60")
	release()

	require.NoError(t, sweep.result(t), "the sweep returns without waiting out the pause")
	require.Len(t, mint.plc.recordedPosts(), 1, "a pause during the read stops the POST")
	assert.Equal(t, ids[:1], result.Subscribed)
	assert.Empty(t, result.Failed)
	assert.Zero(t, h.hitCount(urlPath(t, ids[2])), "the batch stops at the pause")
	rig.reconciler.mu.Lock()
	assert.NotContains(t, rig.reconciler.skips, ids[1], "the pause is no failure of the add")
	rig.reconciler.mu.Unlock()

	mark := rig.deliveryCount()
	rig.clock.Advance(60 * time.Second)
	after, afterResult := rig.startSweep(t)
	rig.finishSweep(t, after)

	assert.Equal(t, ids[1:], afterResult.Subscribed)
	assert.Empty(t, afterResult.Failed)
	assert.Len(t, mint.plc.recordedPosts(), 3)
	follows, _ := rig.deliveredSince(mark)
	assert.Equal(t, ids[1:], follows)
}

// actorOrigin serves one actor document, answers whatever status a test sets
// and counts fetches.
type actorOrigin struct {
	mutex   sync.Mutex
	status  int
	body    []byte
	fetches int
}

func (h *harness) serveActorOrigin(path string, doc map[string]any) *actorOrigin {
	h.t.Helper()
	origin := &actorOrigin{status: http.StatusOK}
	origin.serve(h.t, doc)
	h.mux.Handle("GET "+path, origin)
	return origin
}

func (o *actorOrigin) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	o.mutex.Lock()
	status, body := o.status, o.body
	o.fetches++
	o.mutex.Unlock()
	if status != http.StatusOK {
		w.WriteHeader(status)
		return
	}
	w.Header().Set("Content-Type", ap.ContentTypeActivityJSON)
	_, _ = w.Write(body)
}

func (o *actorOrigin) serve(t *testing.T, doc map[string]any) {
	t.Helper()
	body, err := json.Marshal(doc)
	require.NoError(t, err)
	o.mutex.Lock()
	defer o.mutex.Unlock()
	o.body = body
}

func (o *actorOrigin) answer(status int) {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	o.status = status
}

func (o *actorOrigin) fetchCount() int {
	o.mutex.Lock()
	defer o.mutex.Unlock()
	return o.fetches
}

// startEnsureActor bridges actorID on its own goroutine. ctx ends before the
// cleanup runs, so a failed test leaves no call running against the database.
func startEnsureActor(t *testing.T, ctx context.Context, materializer *materialize.Materializer, actorID string) *blockingCall {
	t.Helper()
	call := startCall("EnsureActor", func() error {
		_, err := materializer.EnsureActor(ctx, &ap.Object{ID: actorID})
		return err
	})
	t.Cleanup(func() { <-call.finished })
	return call
}

func requireNoActorRow(t *testing.T, h *harness, actorID string) {
	t.Helper()
	_, err := h.actors.GetByAPActorID(t.Context(), actorID)
	require.True(t, errors.IsNotFound(err), "no bridged_actors row may exist, got %v", err)
}

// TestMintGate_WaitedMintReadsTheActorAgain: an actor with no row leaves no trace
// of opting out, so the document read before a wait says nothing about the
// moment of an irreversible mint. The read after it decides, and a read that
// fails stops the mint for the walk to retry.
func TestMintGate_WaitedMintReadsTheActorAgain(t *testing.T) {
	const actorID = "https://lemmy.world/u/waitingauthor"
	marked := person(actorID, "waitingauthor", map[string]any{"summary": "<p>#nobridge</p>"})

	for _, tc := range []struct {
		name     string
		change   func(t *testing.T, origin *actorOrigin)
		wantSkip bool
	}{
		{
			name:     "document gains the marker",
			change:   func(t *testing.T, origin *actorOrigin) { origin.serve(t, marked) },
			wantSkip: true,
		},
		{
			name:     "document serves 404",
			change:   func(_ *testing.T, origin *actorOrigin) { origin.answer(http.StatusNotFound) },
			wantSkip: true,
		},
		{
			name:     "document serves 410",
			change:   func(_ *testing.T, origin *actorOrigin) { origin.answer(http.StatusGone) },
			wantSkip: true,
		},
		{
			name:   "document fetch fails with a 503",
			change: func(_ *testing.T, origin *actorOrigin) { origin.answer(http.StatusServiceUnavailable) },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			origin := h.serveActorOrigin("/u/waitingauthor", person(actorID, "waitingauthor", nil))
			materializer, fixture := newBudgetedMaterializer(t, h)
			ctx := withBudgetWait(t.Context())
			require.NoError(t, fixture.budget.Wait(ctx), "a fresh budget admits one write")

			mint := startEnsureActor(t, ctx, materializer, actorID)
			fixture.clock.AwaitWaiters(mint)
			requireStillBlocked(t, mint, "the mint waits for the budget")
			tc.change(t, origin)
			fixture.clock.Advance(12 * time.Second)

			err := mint.result(t)
			require.Error(t, err)
			assert.Equal(t, tc.wantSkip, materialize.IsSkip(err), "got %v", err)
			assert.Empty(t, fixture.plc.recordedPosts())
			requireNoActorRow(t, h, actorID)
		})
	}
}

// TestMintGate_MintThatDidNotWaitReadsTheActorOnce: the read before the mint is
// still current, so neither kind of mint pays for a second one.
func TestMintGate_MintThatDidNotWaitReadsTheActorOnce(t *testing.T) {
	const actorID = "https://lemmy.world/u/promptauthor"

	for _, tc := range []struct {
		name    string
		context func(context.Context) context.Context
	}{
		{name: "marked mint with a free token", context: withBudgetWait},
		{name: "unmarked mint", context: func(ctx context.Context) context.Context { return ctx }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			origin := h.serveActorOrigin("/u/promptauthor", person(actorID, "promptauthor", nil))
			materializer, fixture := newBudgetedMaterializer(t, h)

			_, err := materializer.EnsureActor(tc.context(t.Context()), &ap.Object{ID: actorID})

			require.NoError(t, err)
			assert.Equal(t, 1, origin.fetchCount())
			assert.Len(t, fixture.plc.recordedPosts(), 1)
		})
	}
}

// TestMintGate_RetryAfterPauseReadsTheActorAgain: the pause is a wait even though
// a token is free when it ends.
func TestMintGate_RetryAfterPauseReadsTheActorAgain(t *testing.T) {
	const actorID = "https://lemmy.world/u/pausedauthor"
	h := newHarness(t)
	origin := h.serveActorOrigin("/u/pausedauthor", person(actorID, "pausedauthor", nil))
	materializer, fixture := newBudgetedMaterializer(t, h)
	fixture.plc.mintRateLimited = true
	ctx := withBudgetWait(t.Context())

	mint := startEnsureActor(t, ctx, materializer, actorID)
	fixture.plc.awaitPosts(t, 1)
	fixture.clock.AwaitWaiters(mint)
	requireStillBlocked(t, mint, "the mint waits out the pause")
	origin.serve(t, person(actorID, "pausedauthor", map[string]any{"summary": "<p>#nobridge</p>"}))
	fixture.clock.Advance(60 * time.Second)

	err := mint.result(t)
	require.True(t, materialize.IsSkip(err), "want a skip, got %v", err)
	assert.Len(t, fixture.plc.recordedPosts(), 1, "the retry sends nothing")
	assert.Equal(t, 2, origin.fetchCount())
	requireNoActorRow(t, h, actorID)
}

func TestNewMintGate_RejectsNilDependencies(t *testing.T) {
	fixture := newMintFixture(t, false)
	for _, tc := range []struct {
		name   string
		inner  materialize.ActorMinter
		budget *identity.PLCWriteBudget
	}{
		{name: "nil inner", budget: fixture.budget},
		{name: "nil budget", inner: fixture.minter},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewMintGate(tc.inner, tc.budget, nil)
			require.Error(t, err)
			assert.True(t, errors.IsValidation(err), "got %v", err)
		})
	}
}

// TestMint_HeldInHandleCheckTakesItsTokenAtSubmit: a mint held inside
// HandleTaken from t=0 meets the budget when it reaches the POST at t=13, after
// a rename took the token at t=12. A token charged on entry would let it POST.
func TestMint_HeldInHandleCheckTakesItsTokenAtSubmit(t *testing.T) {
	for _, tc := range []struct {
		name              string
		renameRateLimited bool
		bareMinter        bool
	}{
		{name: "rename answered 429, mint behind the gate", renameRateLimited: true},
		{name: "rename answered 429, bare minter", renameRateLimited: true, bareMinter: true},
		{name: "rename answered 200, mint behind the gate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newMintFixture(t, tc.renameRateLimited)
			ctx := t.Context()
			var minter materialize.ActorMinter = fixture.minter
			if !tc.bareMinter {
				minter = fixture.gate(t)
			}

			mint := fixture.startMint(ctx, minter, "alice")
			fixture.actors.awaitEntered(t)

			fixture.clock.Advance(12 * time.Second)
			update := startCall("UpdateHandle", func() error {
				_, err := fixture.minter.UpdateHandle(ctx, renameDID, renameHandle, fixture.budget.Wait)
				return err
			})
			fixture.plc.awaitPosts(t, 1)
			fixture.clock.AwaitWaiters(update)

			fixture.clock.Advance(time.Second)
			close(fixture.actors.release)

			renamePost := plcPost{path: "/" + renameDID, at: clockStart.Add(12 * time.Second)}
			if !tc.bareMinter {
				require.ErrorIs(t, mint.result(t), ErrMintRateExceeded)
				require.Equal(t, []plcPost{renamePost}, fixture.plc.recordedPosts(), "the mint POSTed nothing")
				require.NotContains(t, fixture.logs.String(), orphanLogText, "a refused mint submitted nothing")
				return
			}

			// The pause ends at t=72. One call takes that slot and the other the
			// next, in either order.
			fixture.clock.AwaitWaiters(update, mint)
			fixture.clock.Advance(59 * time.Second)
			fixture.clock.AwaitWaiters(update, mint)
			fixture.clock.Advance(12 * time.Second)
			require.NoError(t, mint.result(t))
			require.NoError(t, update.result(t))

			var mintPosts []plcPost
			for _, post := range fixture.plc.recordedPosts() {
				if post.path != renamePost.path {
					mintPosts = append(mintPosts, post)
				}
			}
			require.Len(t, mintPosts, 1)
			require.False(t, mintPosts[0].at.Before(clockStart.Add(72*time.Second)),
				"the mint POST arrived at clock offset %s, inside the pause", mintPosts[0].at.Sub(clockStart))
		})
	}
}
