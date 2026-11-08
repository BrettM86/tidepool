package votes

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
)

// Origin verification (design-verify.md): a vote becomes a record only after
// its activity is fetched from the voter's origin and matches the claim. These
// are unit tests over a fake fetcher and a fake clock.

// fakeClock is the one clock a test drives: the verifier's Now and the
// writer's Sleep both read and advance it.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Sleep is the writer's sleep seam on this clock: it advances instead of
// waiting.
func (c *fakeClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.Advance(d)
	return nil
}

// originAnswer is what the fake origin returns for one IRI. blockUntilDone
// waits for the fetch context to end (a hung origin). release, when set, holds
// the fetch until it is closed (whatever the context), after signalling
// started.
type originAnswer struct {
	object         *ap.Object
	err            error
	blockUntilDone bool
	started        chan<- struct{}
	release        <-chan struct{}
}

// fakeOrigin is an in-process OriginFetcher: IRIs it was told to serve answer
// as configured, any other IRI is a 404. It counts fetches per IRI. onFetch,
// when set, runs at the start of every fetch (a slow origin advances the
// test's clock).
type fakeOrigin struct {
	mu      sync.Mutex
	answers map[string]originAnswer
	hits    map[string]int
	onFetch func()
}

func newFakeOrigin() *fakeOrigin {
	return &fakeOrigin{answers: map[string]originAnswer{}, hits: map[string]int{}}
}

func (o *fakeOrigin) answer(iri string, a originAnswer) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.answers[iri] = a
}

// serve answers the claim's activity id with the matching activity.
func (o *fakeOrigin) serve(claim VoteClaim) {
	o.answer(claim.ActivityID, originAnswer{object: matchingActivity(claim)})
}

// serveForged answers the claim's activity id with an activity whose object is
// another subject: the origin's voter never cast the claimed vote.
func (o *fakeOrigin) serveForged(claim VoteClaim) {
	forged := matchingActivity(claim)
	forged.Object = &ap.Object{ID: claim.Object + "-elsewhere"}
	o.answer(claim.ActivityID, originAnswer{object: forged})
}

// fail answers the claim's activity id with err.
func (o *fakeOrigin) fail(claim VoteClaim, err error) {
	o.answer(claim.ActivityID, originAnswer{err: err})
}

func (o *fakeOrigin) fetches(iri string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.hits[iri]
}

func (o *fakeOrigin) totalFetches() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	n := 0
	for _, hits := range o.hits {
		n += hits
	}
	return n
}

func (o *fakeOrigin) FetchObjectSameAuthority(ctx context.Context, iri string) (*ap.Object, error) {
	o.mu.Lock()
	o.hits[iri]++
	a, ok := o.answers[iri]
	onFetch := o.onFetch
	o.mu.Unlock()
	if onFetch != nil {
		onFetch()
	}
	if a.release != nil {
		if a.started != nil {
			a.started <- struct{}{}
		}
		<-a.release
	}
	if !ok {
		return nil, errors.NewNotFoundError("activity", iri)
	}
	if a.blockUntilDone {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Second):
			// The verifier applied no timeout: answer something that is NOT
			// unavailable so the assertion fails instead of passing late.
			return nil, errors.NewNotFoundError("activity", iri)
		}
	}
	if a.object != nil {
		copied := *a.object
		return &copied, nil
	}
	return nil, a.err
}

// matchingActivity is the activity the voter's origin serves for an honest
// claim.
func matchingActivity(claim VoteClaim) *ap.Object {
	return &ap.Object{
		ID:     claim.ActivityID,
		Type:   claim.Type,
		Actor:  &ap.Object{ID: claim.Actor},
		Object: &ap.Object{ID: claim.Object},
	}
}

// claimOn is a Like by a voter on host, with an activity id on that host.
func claimOn(host string, n int) VoteClaim {
	return VoteClaim{
		ActivityID: fmt.Sprintf("https://%s/activities/like/origin-%d", host, n),
		Actor:      "https://" + host + "/u/voter",
		Object:     "https://lemmy.zip/post/5",
		Type:       ap.TypeLike,
	}
}

func newTestVerifier(t *testing.T, origin OriginFetcher, clock *fakeClock, hostRPS, globalRPS float64) OriginVerifier {
	t.Helper()
	verifier, err := NewOriginVerifier(origin, OriginVerifierOptions{
		HostRPS: hostRPS, GlobalRPS: globalRPS, Timeout: 20 * time.Millisecond, Now: clock.Now,
	}, slog.Default())
	require.NoError(t, err)
	require.NotNil(t, verifier)
	return verifier
}

// O1: one claim, one verdict.
func TestOriginVerifierVerdicts(t *testing.T) {
	honest := claimOn("lemmy.world", 1)
	tests := []struct {
		name  string
		claim VoteClaim
		// origin configures the fake origin for the claim.
		origin      func(o *fakeOrigin, claim VoteClaim)
		want        OriginVerdict
		wantFetches int
	}{
		{name: "matching activity", claim: honest,
			origin: func(o *fakeOrigin, c VoteClaim) { o.serve(c) },
			want:   OriginVerified, wantFetches: 1},
		{name: "matching Dislike", claim: VoteClaim{ActivityID: honest.ActivityID, Actor: honest.Actor, Object: honest.Object, Type: ap.TypeDislike},
			origin: func(o *fakeOrigin, c VoteClaim) { o.serve(c) },
			want:   OriginVerified, wantFetches: 1},
		{name: "matching activity served without an id", claim: honest,
			origin: func(o *fakeOrigin, c VoteClaim) {
				a := matchingActivity(c)
				a.ID = ""
				o.answer(c.ActivityID, originAnswer{object: a})
			},
			want: OriginMismatch, wantFetches: 1},
		{name: "activities path in upper case",
			claim:  VoteClaim{ActivityID: "https://lemmy.world/Activities/Like/1", Actor: honest.Actor, Object: honest.Object, Type: ap.TypeLike},
			origin: func(o *fakeOrigin, c VoteClaim) { o.serve(c) },
			want:   OriginVerified, wantFetches: 1},
		{name: "activity id with a query",
			claim:  VoteClaim{ActivityID: honest.ActivityID + "?q=1", Actor: honest.Actor, Object: honest.Object, Type: ap.TypeLike},
			origin: func(o *fakeOrigin, c VoteClaim) { o.serve(c) },
			want:   OriginMismatch, wantFetches: 0},
		{name: "activity id with a fragment",
			claim:  VoteClaim{ActivityID: honest.ActivityID + "#main", Actor: honest.Actor, Object: honest.Object, Type: ap.TypeLike},
			origin: func(o *fakeOrigin, c VoteClaim) { o.serve(c) },
			want:   OriginMismatch, wantFetches: 0},
		{name: "activity id outside the activities path",
			claim:  VoteClaim{ActivityID: "https://lemmy.world/files/abc", Actor: honest.Actor, Object: honest.Object, Type: ap.TypeLike},
			origin: func(o *fakeOrigin, c VoteClaim) { o.serve(c) },
			want:   OriginMismatch, wantFetches: 0},
		{name: "activity id host differs from the actor host",
			claim:  VoteClaim{ActivityID: "https://evil.example/activities/like/1", Actor: honest.Actor, Object: honest.Object, Type: ap.TypeLike},
			origin: func(o *fakeOrigin, c VoteClaim) { o.serve(c) },
			want:   OriginMismatch, wantFetches: 0},
		{name: "fetched type differs", claim: honest,
			origin: func(o *fakeOrigin, c VoteClaim) {
				a := matchingActivity(c)
				a.Type = ap.TypeDislike
				o.answer(c.ActivityID, originAnswer{object: a})
			},
			want: OriginMismatch, wantFetches: 1},
		{name: "fetched actor differs", claim: honest,
			origin: func(o *fakeOrigin, c VoteClaim) {
				a := matchingActivity(c)
				a.Actor = &ap.Object{ID: "https://lemmy.world/u/mallory"}
				o.answer(c.ActivityID, originAnswer{object: a})
			},
			want: OriginMismatch, wantFetches: 1},
		{name: "fetched object differs", claim: honest,
			origin: func(o *fakeOrigin, c VoteClaim) { o.serveForged(c) },
			want:   OriginMismatch, wantFetches: 1},
		{name: "fetched id differs", claim: honest,
			origin: func(o *fakeOrigin, c VoteClaim) {
				a := matchingActivity(c)
				a.ID = "https://lemmy.world/activities/like/other"
				o.answer(c.ActivityID, originAnswer{object: a})
			},
			want: OriginMismatch, wantFetches: 1},
		{name: "off-host redirect", claim: honest,
			origin: func(o *fakeOrigin, c VoteClaim) {
				o.fail(c, errors.NewValidationError("redirect", "redirected to another authority"))
			},
			want: OriginMismatch, wantFetches: 1},
		{name: "egress guard refuses the address", claim: honest,
			origin: func(o *fakeOrigin, c VoteClaim) {
				o.fail(c, errors.NewValidationError("address", "resolves to a private address"))
			},
			want: OriginUnavailable, wantFetches: 1},
		{name: "not found", claim: honest,
			origin: func(o *fakeOrigin, c VoteClaim) { o.fail(c, errors.NewNotFoundError("activity", c.ActivityID)) },
			want:   OriginNotFound, wantFetches: 1},
		{name: "tombstoned", claim: honest,
			origin: func(o *fakeOrigin, c VoteClaim) { o.fail(c, errors.NewTombstonedError("activity", c.ActivityID)) },
			want:   OriginNotFound, wantFetches: 1},
		{name: "400, Lemmy's missing activity", claim: honest,
			origin: func(o *fakeOrigin, c VoteClaim) { o.fail(c, ap.HTTPError{URL: c.ActivityID, StatusCode: 400}) },
			want:   OriginNotFound, wantFetches: 1},
		{name: "403", claim: honest,
			origin: func(o *fakeOrigin, c VoteClaim) { o.fail(c, ap.HTTPError{URL: c.ActivityID, StatusCode: 403}) },
			want:   OriginNotFound, wantFetches: 1},
		{name: "503", claim: honest,
			origin: func(o *fakeOrigin, c VoteClaim) { o.fail(c, ap.HTTPError{URL: c.ActivityID, StatusCode: 503}) },
			want:   OriginUnavailable, wantFetches: 1},
		{name: "429", claim: honest,
			origin: func(o *fakeOrigin, c VoteClaim) { o.fail(c, ap.HTTPError{URL: c.ActivityID, StatusCode: 429}) },
			want:   OriginUnavailable, wantFetches: 1},
		{name: "origin hangs past the timeout", claim: honest,
			origin: func(o *fakeOrigin, c VoteClaim) { o.answer(c.ActivityID, originAnswer{blockUntilDone: true}) },
			want:   OriginUnavailable, wantFetches: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			origin := newFakeOrigin()
			tc.origin(origin, tc.claim)
			verifier := newTestVerifier(t, origin, newFakeClock(), 1, 5)

			verdict, deferred := verifier.Verify(context.Background(), tc.claim)

			assert.False(t, deferred, "a fresh verifier has a token")
			assert.Equal(t, tc.want, verdict)
			assert.Equal(t, tc.wantFetches, origin.totalFetches(), "fetches")
		})
	}
}

// O1: an unavailable host is backed off; other hosts are not.
func TestOriginVerifierBacksOffAnUnavailableHost(t *testing.T) {
	tests := []struct {
		name string
		fail func(o *fakeOrigin, claim VoteClaim)
	}{
		{name: "503", fail: func(o *fakeOrigin, c VoteClaim) {
			o.fail(c, ap.HTTPError{URL: c.ActivityID, StatusCode: 503})
		}},
		{name: "timeout", fail: func(o *fakeOrigin, c VoteClaim) {
			o.answer(c.ActivityID, originAnswer{blockUntilDone: true})
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			origin, clock := newFakeOrigin(), newFakeClock()
			verifier := newTestVerifier(t, origin, clock, 1, 5)
			down1, down2 := claimOn("lemmy.ml", 1), claimOn("lemmy.ml", 2)
			healthy := claimOn("lemmy.world", 1)
			tc.fail(origin, down1)
			origin.serve(down2)
			origin.serve(healthy)

			verdict, deferred := verifier.Verify(ctx, down1)
			require.False(t, deferred)
			require.Equal(t, OriginUnavailable, verdict)

			// A second later the host's bucket has a token again, but the host
			// is in backoff: no fetch.
			clock.Advance(time.Second)
			verdict, deferred = verifier.Verify(ctx, down2)
			assert.True(t, deferred, "a host in backoff is deferred")
			assert.Equal(t, OriginVerdict(""), verdict)
			assert.Equal(t, 0, origin.fetches(down2.ActivityID), "no fetch while in backoff")

			verdict, deferred = verifier.Verify(ctx, healthy)
			assert.False(t, deferred, "another host is admitted")
			assert.Equal(t, OriginVerified, verdict)

			// The first backoff is 30s.
			clock.Advance(31 * time.Second)
			verdict, deferred = verifier.Verify(ctx, down2)
			assert.False(t, deferred, "the backoff has passed")
			assert.Equal(t, OriginVerified, verdict)
		})
	}
}

// O1: admission is one paired check of the voter host's bucket and the global
// bucket; a refusal consumes neither.
func TestOriginVerifierAdmission(t *testing.T) {
	type step struct {
		advance      time.Duration
		claim        VoteClaim
		wantDeferred bool
	}
	hostA1, hostA2 := claimOn("lemmy.world", 1), claimOn("lemmy.world", 2)
	tests := []struct {
		name               string
		hostRPS, globalRPS float64
		steps              []step
	}{
		{
			name:    "host bucket refills after a second",
			hostRPS: 1, globalRPS: 5,
			steps: []step{
				{claim: hostA1},
				{claim: hostA2, wantDeferred: true},
				{advance: time.Second, claim: hostA2},
			},
		},
		{
			// Five refusals on host A at t0+0.5s must not drain the global
			// bucket (2/s): host B is admitted right after them.
			name:    "a host refusal does not consume the global token",
			hostRPS: 1, globalRPS: 2,
			steps: []step{
				{claim: hostA1},
				{advance: 500 * time.Millisecond, claim: hostA2, wantDeferred: true},
				{claim: claimOn("lemmy.world", 3), wantDeferred: true},
				{claim: claimOn("lemmy.world", 4), wantDeferred: true},
				{claim: claimOn("lemmy.world", 5), wantDeferred: true},
				{claim: claimOn("lemmy.world", 6), wantDeferred: true},
				{claim: claimOn("lemmy.zip", 1)},
			},
		},
		{
			name:    "global bucket exhausted defers every host",
			hostRPS: 10, globalRPS: 1,
			steps: []step{
				{claim: hostA1},
				{claim: claimOn("lemmy.zip", 1), wantDeferred: true},
				{claim: claimOn("sh.itjust.works", 1), wantDeferred: true},
				{claim: hostA2, wantDeferred: true},
				{advance: time.Second, claim: claimOn("lemmy.zip", 1)},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			origin, clock := newFakeOrigin(), newFakeClock()
			verifier := newTestVerifier(t, origin, clock, tc.hostRPS, tc.globalRPS)
			for i, s := range tc.steps {
				origin.serve(s.claim)
				clock.Advance(s.advance)
				before := origin.fetches(s.claim.ActivityID)

				verdict, deferred := verifier.Verify(context.Background(), s.claim)

				assert.Equal(t, s.wantDeferred, deferred, "step %d (%s)", i, s.claim.ActivityID)
				if s.wantDeferred {
					assert.Equal(t, OriginVerdict(""), verdict, "step %d", i)
					assert.Equal(t, before, origin.fetches(s.claim.ActivityID), "step %d: a deferred claim is not fetched", i)
				} else {
					assert.Equal(t, OriginVerified, verdict, "step %d", i)
				}
			}
		})
	}
}

// O1: the host backoff doubles per further failure and resets after any
// definitive answer.
func TestOriginVerifierBackoffDoublesAndResets(t *testing.T) {
	ctx := context.Background()
	origin, clock := newFakeOrigin(), newFakeClock()
	verifier := newTestVerifier(t, origin, clock, 1, 5)
	down := func(c VoteClaim) { origin.fail(c, ap.HTTPError{URL: c.ActivityID, StatusCode: 503}) }
	type step struct {
		advance      time.Duration
		claim        VoteClaim
		down         bool
		wantDeferred bool
		want         OriginVerdict
	}
	steps := []step{
		{claim: claimOn("lemmy.ml", 1), down: true, want: OriginUnavailable},
		// The first backoff (30s) has passed: a second failure.
		{advance: 31 * time.Second, claim: claimOn("lemmy.ml", 2), down: true, want: OriginUnavailable},
		// The second backoff is 60s.
		{advance: 31 * time.Second, claim: claimOn("lemmy.ml", 3), wantDeferred: true},
		{advance: 30 * time.Second, claim: claimOn("lemmy.ml", 3), want: OriginVerified},
		// The verified answer reset the backoff: the next failure backs off 30s.
		{advance: time.Second, claim: claimOn("lemmy.ml", 4), down: true, want: OriginUnavailable},
		{advance: 31 * time.Second, claim: claimOn("lemmy.ml", 5), want: OriginVerified},
	}
	for i, s := range steps {
		if s.down {
			down(s.claim)
		} else {
			origin.serve(s.claim)
		}
		clock.Advance(s.advance)

		verdict, deferred := verifier.Verify(ctx, s.claim)

		assert.Equal(t, s.wantDeferred, deferred, "step %d", i)
		assert.Equal(t, s.want, verdict, "step %d", i)
	}
}
