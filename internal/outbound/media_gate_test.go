package outbound

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// The readiness gate's decision rules, without a database or a network: a fake
// prober answers each URL from a script and records what it was asked, and the
// clock is two fixed instants. Persistence, the park write, the POST and the
// metric are the worker's and are pinned in media_gate_worker_test.go.

const (
	gateBase = "https://img.example"
	gateURL0 = "https://img.example/img/content_full/plain/did:plc:abc/bafkreihak3l6grklnl53pnqjtrwfph7wyojvrsaazb7nmptucuhobephqy"
	gateURL1 = "https://img.example/img/content_full/plain/did:plc:abc/bafkreihh33brlfst3dssji2k4fllfvevfp6q6c2zrqfm2soybkt2pq3lui"
)

// scriptedProber answers each URL with its scripted status or error and records
// every URL it was asked about. An unscripted URL is an error, so a probe the
// row did not expect cannot pass as served.
type scriptedProber struct {
	mu     sync.Mutex
	calls  []string
	status map[string]int
	errs   map[string]error
}

func (p *scriptedProber) Probe(_ context.Context, imageURL string) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, imageURL)
	if err, ok := p.errs[imageURL]; ok {
		return 0, err
	}
	if status, ok := p.status[imageURL]; ok {
		return status, nil
	}
	return 0, errors.New("unscripted probe")
}

func (p *scriptedProber) sortedCalls() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	calls := append([]string{}, p.calls...)
	sort.Strings(calls)
	return calls
}

// Payloads in the shape the worker stores: the Translator's activity with its
// Note/Page object carrying HTML content, markdown source and attachments.
const (
	gateBothImagesPayload = `{"@context":"https://www.w3.org/ns/activitystreams","id":"https://coves.social/ap/activity/a1","type":"%s","actor":"https://coves.social/ap/actor/did:plc:abc",` +
		`"object":{"type":"Note","id":"https://coves.social/ap/object/did:plc:abc/social.coves.community.comment/r1",` +
		`"content":"<p>a <img src=\"https://img.example/img/content_full/plain/did:plc:abc/bafkreihak3l6grklnl53pnqjtrwfph7wyojvrsaazb7nmptucuhobephqy\" alt=\"one\"> b <img src=\"https://img.example/img/content_full/plain/did:plc:abc/bafkreihh33brlfst3dssji2k4fllfvevfp6q6c2zrqfm2soybkt2pq3lui\" alt=\"two\"></p>\n",` +
		`"mediaType":"text/html",` +
		`"source":{"content":"a ![one](https://img.example/img/content_full/plain/did:plc:abc/bafkreihak3l6grklnl53pnqjtrwfph7wyojvrsaazb7nmptucuhobephqy) b ![two](https://img.example/img/content_full/plain/did:plc:abc/bafkreihh33brlfst3dssji2k4fllfvevfp6q6c2zrqfm2soybkt2pq3lui)","mediaType":"text/markdown"}}}`

	gateOneImagePayload = `{"@context":"https://www.w3.org/ns/activitystreams","id":"https://coves.social/ap/activity/a2","type":"%s","actor":"https://coves.social/ap/actor/did:plc:abc",` +
		`"object":{"type":"Note","id":"https://coves.social/ap/object/did:plc:abc/social.coves.community.comment/r2",` +
		`"content":"<p><img src=\"https://img.example/img/content_full/plain/did:plc:abc/bafkreihak3l6grklnl53pnqjtrwfph7wyojvrsaazb7nmptucuhobephqy\" alt=\"one\"></p>\n",` +
		`"mediaType":"text/html",` +
		`"source":{"content":"![one](https://img.example/img/content_full/plain/did:plc:abc/bafkreihak3l6grklnl53pnqjtrwfph7wyojvrsaazb7nmptucuhobephqy)","mediaType":"text/markdown"}}}`
)

// gatePayload fills a payload literal's activity type.
func gatePayload(format, kind string) []byte {
	return []byte(fmt.Sprintf(format, kind))
}

func TestMediaGate_Decide(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	recent := now.Add(-30 * time.Second) // well inside the 2-minute budget
	stale := now.Add(-3 * time.Minute)   // past it
	const parkDelay = 10 * time.Second
	const budget = 2 * time.Minute

	type row struct {
		name       string
		kind       string
		payload    []byte
		mediaReady bool
		createdAt  time.Time
		status     map[string]int
		errs       map[string]error
		want       MediaGateOutcome
		// wantCalls, when non-nil, is the exact sorted set of URLs probed: the
		// rows where which URLs are probed (each once, none, never off-base) is
		// the behavior.
		wantCalls []string
	}
	rows := []row{
		{
			name:      "create with every image 2xx is served, each distinct URL probed once",
			kind:      "Create",
			payload:   gatePayload(gateBothImagesPayload, "Create"),
			createdAt: recent,
			status:    map[string]int{gateURL0: 200, gateURL1: 204},
			want:      MediaAllServed,
			wantCalls: []string{gateURL0, gateURL1},
		},
		{
			name:      "update with an unavailable image parks",
			kind:      "Update",
			payload:   gatePayload(gateOneImagePayload, "Update"),
			createdAt: recent,
			status:    map[string]int{gateURL0: 404},
			want:      MediaParked,
		},
		{
			name:      "404 within the budget parks",
			kind:      "Create",
			payload:   gatePayload(gateOneImagePayload, "Create"),
			createdAt: recent,
			status:    map[string]int{gateURL0: 404},
			want:      MediaParked,
		},
		{
			name:      "503 within the budget parks",
			kind:      "Create",
			payload:   gatePayload(gateOneImagePayload, "Create"),
			createdAt: recent,
			status:    map[string]int{gateURL0: 503},
			want:      MediaParked,
		},
		{
			name:      "a prober error (timeout) within the budget parks",
			kind:      "Create",
			payload:   gatePayload(gateOneImagePayload, "Create"),
			createdAt: recent,
			errs:      map[string]error{gateURL0: context.DeadlineExceeded},
			want:      MediaParked,
		},
		{
			name:      "500 within the budget parks",
			kind:      "Create",
			payload:   gatePayload(gateOneImagePayload, "Create"),
			createdAt: recent,
			status:    map[string]int{gateURL0: 500},
			want:      MediaParked,
		},
		{
			name:      "first image served, second 404 parks",
			kind:      "Create",
			payload:   gatePayload(gateBothImagesPayload, "Create"),
			createdAt: recent,
			status:    map[string]int{gateURL0: 200, gateURL1: 404},
			want:      MediaParked,
		},
		{
			name:      "first image 404, second served parks",
			kind:      "Create",
			payload:   gatePayload(gateBothImagesPayload, "Create"),
			createdAt: recent,
			status:    map[string]int{gateURL0: 404, gateURL1: 200},
			want:      MediaParked,
		},
		{
			name:      "a failing probe past the budget delivers anyway",
			kind:      "Create",
			payload:   gatePayload(gateOneImagePayload, "Create"),
			createdAt: stale,
			status:    map[string]int{gateURL0: 404},
			want:      MediaBudgetExhausted,
		},
		{
			name:       "an activity already marked ready is not probed",
			kind:       "Create",
			payload:    gatePayload(gateBothImagesPayload, "Create"),
			mediaReady: true,
			createdAt:  recent,
			want:       MediaNotGated,
			wantCalls:  []string{},
		},
		{
			name: "a payload with no image URLs is not probed",
			kind: "Create",
			payload: []byte(`{"id":"https://coves.social/ap/activity/a3","type":"Create","actor":"https://coves.social/ap/actor/did:plc:abc",` +
				`"object":{"type":"Note","content":"<p>hi</p>\n","source":{"content":"hi","mediaType":"text/markdown"}}}`),
			createdAt: recent,
			want:      MediaNotGated,
			wantCalls: []string{},
		},
		{
			name:      "a Delete carrying image URLs is not probed",
			kind:      "Delete",
			payload:   gatePayload(gateBothImagesPayload, "Delete"),
			createdAt: recent,
			want:      MediaNotGated,
			wantCalls: []string{},
		},
		{
			name:      "a Like carrying image URLs is not probed",
			kind:      "Like",
			payload:   gatePayload(gateBothImagesPayload, "Like"),
			createdAt: recent,
			want:      MediaNotGated,
			wantCalls: []string{},
		},
		{
			name:      "an Undo carrying image URLs is not probed",
			kind:      "Undo",
			payload:   gatePayload(gateBothImagesPayload, "Undo"),
			createdAt: recent,
			want:      MediaNotGated,
			wantCalls: []string{},
		},
		{
			name: "URLs outside the base are never probed while the one under it is",
			kind: "Create",
			payload: []byte(`{"id":"https://coves.social/ap/activity/a4","type":"Create","actor":"https://coves.social/ap/actor/did:plc:abc",` +
				`"object":{"type":"Note",` +
				`"content":"<p><img src=\"https://evil.example/img/content_full/plain/did:plc:abc/bafkreihh33brlfst3dssji2k4fllfvevfp6q6c2zrqfm2soybkt2pq3lui\" alt=\"x\"> ` +
				`<img src=\"https://img.example.evil.example/img/content_full/plain/did:plc:abc/bafkreihh33brlfst3dssji2k4fllfvevfp6q6c2zrqfm2soybkt2pq3lui\" alt=\"y\"> ` +
				`<img src=\"https://img.example/img/content_full/plain/did:plc:abc/bafkreihak3l6grklnl53pnqjtrwfph7wyojvrsaazb7nmptucuhobephqy\" alt=\"one\"></p>\n",` +
				`"source":{"content":"![x](https://evil.example/img/content_full/plain/did:plc:abc/bafkreihh33brlfst3dssji2k4fllfvevfp6q6c2zrqfm2soybkt2pq3lui) ` +
				`![y](https://img.example.evil.example/img/content_full/plain/did:plc:abc/bafkreihh33brlfst3dssji2k4fllfvevfp6q6c2zrqfm2soybkt2pq3lui) ` +
				`![one](https://img.example/img/content_full/plain/did:plc:abc/bafkreihak3l6grklnl53pnqjtrwfph7wyojvrsaazb7nmptucuhobephqy)","mediaType":"text/markdown"},` +
				`"attachment":[{"type":"Image","url":"https://evil.example/img/content_full/plain/did:plc:abc/bafkreihh33brlfst3dssji2k4fllfvevfp6q6c2zrqfm2soybkt2pq3lui"}]}}`),
			createdAt: recent,
			status:    map[string]int{gateURL0: 200},
			want:      MediaAllServed,
			wantCalls: []string{gateURL0},
		},
		{
			name: "an image only in content <img src> is probed",
			kind: "Create",
			payload: []byte(`{"id":"https://coves.social/ap/activity/a5","type":"Create","actor":"https://coves.social/ap/actor/did:plc:abc",` +
				`"object":{"type":"Note",` +
				`"content":"<p>look <img src=\"https://img.example/img/content_full/plain/did:plc:abc/bafkreihak3l6grklnl53pnqjtrwfph7wyojvrsaazb7nmptucuhobephqy\" alt=\"one\"></p>\n",` +
				`"source":{"content":"look","mediaType":"text/markdown"}}}`),
			createdAt: recent,
			status:    map[string]int{gateURL0: 200},
			want:      MediaAllServed,
			wantCalls: []string{gateURL0},
		},
		{
			name: "an image only in attachment[].url is probed",
			kind: "Create",
			payload: []byte(`{"id":"https://coves.social/ap/activity/a7","type":"Create","actor":"https://coves.social/ap/actor/did:plc:abc",` +
				`"object":{"type":"Page","name":"t",` +
				`"content":"<p>look</p>\n",` +
				`"source":{"content":"look","mediaType":"text/markdown"},` +
				`"attachment":[{"type":"Image","url":"https://img.example/img/content_full/plain/did:plc:abc/bafkreihak3l6grklnl53pnqjtrwfph7wyojvrsaazb7nmptucuhobephqy"}]}}`),
			createdAt: recent,
			status:    map[string]int{gateURL0: 200},
			want:      MediaAllServed,
			wantCalls: []string{gateURL0},
		},
		{
			name: "an author-escaped image in source with only a link in content is not probed",
			kind: "Create",
			payload: []byte(`{"id":"https://coves.social/ap/activity/a8","type":"Create","actor":"https://coves.social/ap/actor/did:plc:abc",` +
				`"object":{"type":"Note",` +
				`"content":"<p>!<a href=\"https://img.example/img/content_full/plain/did:plc:abc/bafkreihak3l6grklnl53pnqjtrwfph7wyojvrsaazb7nmptucuhobephqy\">photo</a></p>\n",` +
				`"source":{"content":"\\![photo](https://img.example/img/content_full/plain/did:plc:abc/bafkreihak3l6grklnl53pnqjtrwfph7wyojvrsaazb7nmptucuhobephqy)","mediaType":"text/markdown"}}}`),
			createdAt: recent,
			want:      MediaNotGated,
			wantCalls: []string{},
		},
		{
			name: "markdown image syntax inside a code facet with no img or attachment is not probed",
			kind: "Create",
			payload: []byte(`{"id":"https://coves.social/ap/activity/a9","type":"Create","actor":"https://coves.social/ap/actor/did:plc:abc",` +
				`"object":{"type":"Note",` +
				`"content":"<p>try <code>![x](https://img.example/img/anything)</code></p>\n",` +
				`"source":{"content":"try ` + "`" + `![x](https://img.example/img/anything)` + "`" + `","mediaType":"text/markdown"}}}`),
			createdAt: recent,
			want:      MediaNotGated,
			wantCalls: []string{},
		},
	}

	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			prober := &scriptedProber{status: tc.status, errs: tc.errs}
			gate := MediaGate{BaseURL: gateBase, Prober: prober, ParkDelay: parkDelay, Budget: budget}

			got := gate.Decide(context.Background(), MediaGateInput{
				Kind:       tc.kind,
				Payload:    tc.payload,
				MediaReady: tc.mediaReady,
				CreatedAt:  tc.createdAt,
				Now:        func() time.Time { return now },
			})

			assert.Equal(t, tc.want, got.Outcome, "gate outcome")
			if tc.want == MediaParked {
				assert.Equal(t, time.Date(2026, 11, 5, 12, 0, 10, 0, time.UTC), got.NextAttempt,
					"a parked delivery is retried one park delay after now")
			}
			if tc.wantCalls != nil {
				assert.Equal(t, tc.wantCalls, prober.sortedCalls(), "URLs probed")
			}
		})
	}
}

// clockedProber answers every URL with status and moves the fake clock on by
// step per probe, standing in for a slow proxy.
type clockedProber struct {
	mu     sync.Mutex
	clock  time.Time
	step   time.Duration
	status int
}

func (p *clockedProber) Probe(context.Context, string) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.clock = p.clock.Add(p.step)
	return p.status, nil
}

func (p *clockedProber) now() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.clock
}

// blockingProber blocks until the probe's context is done and returns its
// error, after signalling started. sawDeadline records whether the probe's
// context carried a deadline.
type blockingProber struct {
	started     chan struct{}
	once        sync.Once
	sawDeadline bool
}

func (p *blockingProber) Probe(ctx context.Context, _ string) (int, error) {
	_, p.sawDeadline = ctx.Deadline()
	p.once.Do(func() { close(p.started) })
	<-ctx.Done()
	return 0, ctx.Err()
}

func TestMediaGate_DecideChargesProbeTimeAgainstTheBudget(t *testing.T) {
	start := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name            string
		step            time.Duration
		want            MediaGateOutcome
		wantNextAttempt time.Time
	}{
		{
			// Created 30s before the probe started: inside the 2-minute budget
			// before probing, 2m30s old once the 2-minute probe returns.
			name: "a probe that ends past the budget exhausts it",
			step: 2 * time.Minute,
			want: MediaBudgetExhausted,
		},
		{
			name:            "a park after a slow probe is scheduled from the post-probe time",
			step:            20 * time.Second,
			want:            MediaParked,
			wantNextAttempt: time.Date(2026, 11, 5, 12, 0, 30, 0, time.UTC),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prober := &clockedProber{clock: start, step: tc.step, status: 404}
			gate := MediaGate{BaseURL: gateBase, Prober: prober, ParkDelay: 10 * time.Second, Budget: 2 * time.Minute}

			got := gate.Decide(context.Background(), MediaGateInput{
				Kind:      "Create",
				Payload:   gatePayload(gateOneImagePayload, "Create"),
				CreatedAt: start.Add(-30 * time.Second),
				Now:       prober.now,
			})

			assert.Equal(t, tc.want, got.Outcome)
			assert.Equal(t, tc.wantNextAttempt, got.NextAttempt)
		})
	}
}

func TestMediaGate_DecideBoundsTheProbeSequence(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	prober := &blockingProber{started: make(chan struct{})}
	gate := MediaGate{
		BaseURL: gateBase, Prober: prober, ParkDelay: 10 * time.Second, Budget: 2 * time.Minute,
		ProbeTimeout: 50 * time.Millisecond,
	}
	// The parent deadline only keeps a regression from hanging the test.
	parent, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	began := time.Now()
	got := gate.Decide(parent, MediaGateInput{
		Kind:      "Create",
		Payload:   gatePayload(gateOneImagePayload, "Create"),
		CreatedAt: now.Add(-30 * time.Second),
		Now:       func() time.Time { return now },
	})

	assert.Less(t, time.Since(began), time.Second, "the probe sequence stops at the probe timeout")
	assert.True(t, prober.sawDeadline, "the probe runs under a deadline")
	assert.Equal(t, MediaParked, got.Outcome, "a probe cut off by the probe timeout is not served")
	assert.Equal(t, MediaFailureTimeout, got.FailureClass)
	assert.Equal(t, time.Date(2026, 11, 5, 12, 0, 10, 0, time.UTC), got.NextAttempt)
}

func TestMediaGate_DecideAbortsWhenTheCallerIsCancelled(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		createdAt time.Time
	}{
		{"inside the budget", now.Add(-30 * time.Second)},
		{"past the budget", now.Add(-3 * time.Minute)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prober := &blockingProber{started: make(chan struct{})}
			gate := MediaGate{BaseURL: gateBase, Prober: prober, ParkDelay: 10 * time.Second, Budget: 2 * time.Minute}
			parent, cancel := context.WithCancel(context.Background())
			go func() {
				<-prober.started
				cancel()
			}()
			defer cancel()

			got := gate.Decide(parent, MediaGateInput{
				Kind:      "Create",
				Payload:   gatePayload(gateOneImagePayload, "Create"),
				CreatedAt: tc.createdAt,
				Now:       func() time.Time { return now },
			})

			assert.Equal(t, MediaAborted, got.Outcome, "a cancelled caller decides nothing")
			assert.Zero(t, got.NextAttempt)
		})
	}
}

// netTimeoutError is a net.Error that reports a timeout, as the HTTP client's
// own Timeout produces.
type netTimeoutError struct{}

func (netTimeoutError) Error() string   { return "i/o timeout" }
func (netTimeoutError) Timeout() bool   { return true }
func (netTimeoutError) Temporary() bool { return false }

func TestMediaGate_DecideClassifiesTheFailure(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		status int
		err    error
		stale  bool
		want   MediaFailureClass
	}{
		{name: "404", status: 404, want: MediaFailureStatus404},
		{name: "503", status: 503, want: MediaFailureStatus503},
		{name: "500", status: 500, want: MediaFailureStatusOther},
		{name: "401", status: 401, want: MediaFailureStatusOther},
		{name: "302", status: 302, want: MediaFailureRedirect},
		{name: "301", status: 301, want: MediaFailureRedirect},
		{
			name: "a client timeout wrapped in a url.Error",
			err:  fmt.Errorf("media probe: %w", &url.Error{Op: "Head", URL: gateURL0, Err: netTimeoutError{}}),
			want: MediaFailureTimeout,
		},
		{name: "a deadline", err: fmt.Errorf("media probe: %w", context.DeadlineExceeded), want: MediaFailureTimeout},
		{
			name: "a refused connection",
			err:  fmt.Errorf("media probe: %w", &url.Error{Op: "Head", URL: gateURL0, Err: errors.New("connection refused")}),
			want: MediaFailureTransport,
		},
		{name: "404 past the budget", status: 404, stale: true, want: MediaFailureStatus404},
		{
			name:  "a refused connection past the budget",
			err:   errors.New("dial refused"),
			stale: true,
			want:  MediaFailureTransport,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prober := &scriptedProber{status: map[string]int{gateURL0: tc.status}}
			if tc.err != nil {
				prober.errs = map[string]error{gateURL0: tc.err}
			}
			createdAt, wantOutcome := now.Add(-30*time.Second), MediaParked
			if tc.stale {
				createdAt, wantOutcome = now.Add(-3*time.Minute), MediaBudgetExhausted
			}
			gate := MediaGate{BaseURL: gateBase, Prober: prober, ParkDelay: 10 * time.Second, Budget: 2 * time.Minute}

			got := gate.Decide(context.Background(), MediaGateInput{
				Kind:      "Create",
				Payload:   gatePayload(gateOneImagePayload, "Create"),
				CreatedAt: createdAt,
				Now:       func() time.Time { return now },
			})

			assert.Equal(t, wantOutcome, got.Outcome)
			assert.Equal(t, tc.want, got.FailureClass)
		})
	}
}

func TestMediaGate_DecideServedHasNoFailureClass(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	gate := MediaGate{
		BaseURL: gateBase, Prober: &scriptedProber{status: map[string]int{gateURL0: 200}},
		ParkDelay: 10 * time.Second, Budget: 2 * time.Minute,
	}
	got := gate.Decide(context.Background(), MediaGateInput{
		Kind: "Create", Payload: gatePayload(gateOneImagePayload, "Create"),
		CreatedAt: now, Now: func() time.Time { return now },
	})
	assert.Equal(t, MediaGateDecision{Outcome: MediaAllServed}, got)
}
