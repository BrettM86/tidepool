package outbound

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/store"
)

// The readiness gate as the worker runs it against the real store and a real
// (local httptest) image proxy: what is persisted, parked, POSTed and counted.
// The decision rules themselves are pinned in media_gate_test.go.

const (
	mediaCID0  = "bafkreihak3l6grklnl53pnqjtrwfph7wyojvrsaazb7nmptucuhobephqy"
	mediaCID1  = "bafkreihh33brlfst3dssji2k4fllfvevfp6q6c2zrqfm2soybkt2pq3lui"
	mediaPath0 = "/img/content_full/plain/" + wActorDID + "/" + mediaCID0
	mediaPath1 = "/img/content_full/plain/" + wActorDID + "/" + mediaCID1
	// mediaParkDelay is far from the helper's 1ms backoff, so a park and a
	// retry release are told apart by next_attempt_at.
	mediaParkDelay = time.Minute
)

// mediaProxy is a local image proxy: it counts HEADs per path and answers
// every request with respond.
type mediaProxy struct {
	server  *httptest.Server
	mu      sync.Mutex
	heads   map[string]int
	respond http.HandlerFunc
}

func newMediaProxy(t *testing.T, respond http.HandlerFunc) *mediaProxy {
	t.Helper()
	p := &mediaProxy{heads: map[string]int{}, respond: respond}
	p.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			p.mu.Lock()
			p.heads[r.URL.Path]++
			p.mu.Unlock()
		}
		p.respond(w, r)
	}))
	t.Cleanup(p.server.Close)
	return p
}

func (p *mediaProxy) headCounts() map[string]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := map[string]int{}
	for k, v := range p.heads {
		out[k] = v
	}
	return out
}

func (p *mediaProxy) totalHeads() int {
	n := 0
	for _, v := range p.headCounts() {
		n += v
	}
	return n
}

func answerStatus(status int) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }
}

// mediaCreatePayload is a stored Create{Note} with two images under base, in
// both its HTML content and its markdown source.
func mediaCreatePayload(base string) []byte {
	return []byte(fmt.Sprintf(`{"@context":"https://www.w3.org/ns/activitystreams","id":"x","type":"Create","actor":%q,`+
		`"object":{"type":"Note","content":"<p><img src=\"%[2]s%[3]s\" alt=\"one\"> <img src=\"%[2]s%[4]s\" alt=\"two\"></p>\n",`+
		`"mediaType":"text/html","source":{"content":"![one](%[2]s%[3]s) ![two](%[2]s%[4]s)","mediaType":"text/markdown"}}}`,
		wActorID, base, mediaPath0, mediaPath1))
}

// withMediaGate points the worker's gate at proxyURL with a local-capable
// guarded client.
func withMediaGate(proxyURL string, clientTimeout, budget time.Duration) func(*WorkerOptions) {
	return func(o *WorkerOptions) {
		o.MediaBaseURL = proxyURL
		o.MediaProbeClient = ap.NewGuardedHTTPClient(true, clientTimeout)
		o.MediaParkDelay = mediaParkDelay
		o.MediaWaitBudget = budget
	}
}

func getActivity(t *testing.T, activities store.OutboundActivities, activityID string) *store.OutboundActivity {
	t.Helper()
	activity, err := activities.Get(context.Background(), activityID)
	require.NoError(t, err)
	return activity
}

func TestWorker_MediaGateServedImagesDeliverAndMarkOnce(t *testing.T) {
	conn := workerTestDB(t)
	ctx := context.Background()
	seedWorkerActor(t, conn, true, false)
	proxy := newMediaProxy(t, answerStatus(http.StatusOK))
	id := seedDelivery(t, conn, "Create", "", mediaCreatePayload(proxy.server.URL))
	// A second inbox of the same community (same authority, same ordering
	// key), so it is claimed after the first.
	const secondInbox = "https://lemmy.world/inbox"
	_, err := store.NewOutboundDeliveries(conn).Enqueue(ctx, store.OutboundDelivery{
		ActivityID:  id,
		TargetInbox: secondInbox,
		OrderingKey: wCommunityAPID,
	})
	require.NoError(t, err)

	sender := &fakeSender{}
	w := newWorker(t, conn, sender, withMediaGate(proxy.server.URL, 2*time.Second, time.Hour))
	activities := store.NewOutboundActivities(conn)

	worked, err := w.DeliverNext(ctx)
	require.NoError(t, err)
	require.True(t, worked)

	assert.Equal(t, map[string]int{mediaPath0: 1, mediaPath1: 1}, proxy.headCounts(),
		"each image URL is HEADed once before the first delivery")
	assert.Equal(t, 1, sender.count(), "the served activity is POSTed")
	assert.Equal(t, store.DeliveryStateDelivered, getDelivery(t, conn, id).State)
	assert.NotNil(t, getActivity(t, activities, id).MediaReadyAt,
		"all images served persists the activity's media-ready marker")

	worked, err = w.DeliverNext(ctx)
	require.NoError(t, err)
	require.True(t, worked)

	assert.Equal(t, map[string]int{mediaPath0: 1, mediaPath1: 1}, proxy.headCounts(),
		"a second delivery of a media-ready activity sends no HEAD")
	assert.Equal(t, 2, sender.count(), "the second inbox is POSTed")
}

func TestWorker_MediaGateUnservedImageParksAttemptNeutral(t *testing.T) {
	for _, tc := range []struct {
		name    string
		respond func(t *testing.T) http.HandlerFunc
	}{
		{"404", func(*testing.T) http.HandlerFunc { return answerStatus(http.StatusNotFound) }},
		{"503", func(*testing.T) http.HandlerFunc { return answerStatus(http.StatusServiceUnavailable) }},
		{"timeout", func(t *testing.T) http.HandlerFunc {
			// The handler answers only after the test ends, so the probe's
			// 200ms client timeout is what fires.
			release := make(chan struct{})
			t.Cleanup(func() { close(release) })
			return func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-release:
				case <-r.Context().Done():
				}
				w.WriteHeader(http.StatusOK)
			}
		}},
		{"500", func(*testing.T) http.HandlerFunc { return answerStatus(http.StatusInternalServerError) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := workerTestDB(t)
			seedWorkerActor(t, conn, true, false)
			proxy := newMediaProxy(t, tc.respond(t))
			id := seedDelivery(t, conn, "Create", "", mediaCreatePayload(proxy.server.URL))

			sender := &fakeSender{}
			w := newWorker(t, conn, sender, withMediaGate(proxy.server.URL, 200*time.Millisecond, time.Hour))

			worked, err := w.DeliverNext(context.Background())
			require.NoError(t, err)
			require.True(t, worked)
			handledAt := time.Now()

			got := getDelivery(t, conn, id)
			assert.Equal(t, store.DeliveryStatePending, got.State, "an unserved image holds the delivery")
			assert.Equal(t, "media_pending", got.LastErrorClass, "the hold is recorded as media_pending")
			assert.Equal(t, 0, got.Attempts, "the media hold gives back its claim's attempt")
			assert.WithinDuration(t, handledAt.Add(mediaParkDelay), got.NextAttemptAt, 5*time.Second,
				"the held delivery comes back one park delay later")
			assert.Zero(t, sender.count(), "nothing is POSTed while an image is unserved")
			assert.Nil(t, getActivity(t, store.NewOutboundActivities(conn), id).MediaReadyAt,
				"a parked activity is not marked media-ready")
		})
	}
}

// A worker built with no park delay or wait budget parks an unserved image for
// the documented default of ten seconds, not for zero (an immediate re-claim).
func TestWorker_MediaGateZeroParkDelayUsesTenSecondDefault(t *testing.T) {
	conn := workerTestDB(t)
	seedWorkerActor(t, conn, true, false)
	proxy := newMediaProxy(t, answerStatus(http.StatusNotFound))
	id := seedDelivery(t, conn, "Create", "", mediaCreatePayload(proxy.server.URL))

	sender := &fakeSender{}
	w := newWorker(t, conn, sender, func(o *WorkerOptions) {
		o.MediaBaseURL = proxy.server.URL
		o.MediaProbeClient = ap.NewGuardedHTTPClient(true, 2*time.Second)
		o.MediaParkDelay = 0
		o.MediaWaitBudget = 0
	})

	worked, err := w.DeliverNext(context.Background())
	require.NoError(t, err)
	require.True(t, worked)
	handledAt := time.Now()

	got := getDelivery(t, conn, id)
	assert.Equal(t, store.DeliveryStatePending, got.State, "an unserved image holds the delivery")
	assert.Equal(t, "media_pending", got.LastErrorClass, "the hold is recorded as media_pending")
	assert.WithinDuration(t, handledAt.Add(10*time.Second), got.NextAttemptAt, 2*time.Second,
		"a zero park delay falls back to the ten-second default")
	assert.Zero(t, sender.count(), "nothing is POSTed while an image is unserved")
}

func TestWorker_MediaGateBudgetExhaustedDeliversCountsAndMarks(t *testing.T) {
	conn := workerTestDB(t)
	ctx := context.Background()
	seedWorkerActor(t, conn, true, false)
	proxy := newMediaProxy(t, answerStatus(http.StatusNotFound))
	id := seedDelivery(t, conn, "Create", "", mediaCreatePayload(proxy.server.URL))
	_, err := conn.ExecContext(ctx,
		`UPDATE outbound_deliveries SET created_at = now() - interval '10 minutes' WHERE activity_id = $1`, id)
	require.NoError(t, err)

	sender := senderReturning(httpErr(http.StatusInternalServerError, "peer down"))
	w := newWorker(t, conn, sender, withMediaGate(proxy.server.URL, 2*time.Second, 2*time.Minute))
	before := metricMediaBudgetExhausted.Value()

	worked, err := w.DeliverNext(ctx)
	require.NoError(t, err)
	require.True(t, worked)

	assert.Equal(t, 1, sender.count(), "past the budget the delivery is POSTed despite the unserved image")
	assert.Equal(t, before+1, metricMediaBudgetExhausted.Value(), "the budget exhaustion is counted once")
	assert.NotNil(t, getActivity(t, store.NewOutboundActivities(conn), id).MediaReadyAt,
		"budget exhaustion persists the media-ready marker")

	// The inbox's 500 left the delivery pending for a retry; bring it due now.
	clearParkDelay(t, conn, id)
	headsBefore := proxy.totalHeads()

	worked, err = w.DeliverNext(ctx)
	require.NoError(t, err)
	require.True(t, worked)

	assert.Equal(t, headsBefore, proxy.totalHeads(), "the retry of a media-ready activity sends no HEAD")
	assert.Equal(t, 2, sender.count(), "the retry is POSTed")
	assert.Equal(t, before+1, metricMediaBudgetExhausted.Value(), "the retry does not count again")
}

func TestWorker_MediaGateDoesNotFollowRedirects(t *testing.T) {
	conn := workerTestDB(t)
	seedWorkerActor(t, conn, true, false)

	var redirectedMu sync.Mutex
	redirected := 0
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirectedMu.Lock()
		redirected++
		redirectedMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(target.Close)
	proxy := newMediaProxy(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/image", http.StatusFound)
	})
	id := seedDelivery(t, conn, "Create", "", mediaCreatePayload(proxy.server.URL))

	sender := &fakeSender{}
	w := newWorker(t, conn, sender, withMediaGate(proxy.server.URL, 2*time.Second, time.Hour))

	worked, err := w.DeliverNext(context.Background())
	require.NoError(t, err)
	require.True(t, worked)

	got := getDelivery(t, conn, id)
	assert.Equal(t, store.DeliveryStatePending, got.State, "a redirected probe is a failure: the delivery is held")
	assert.Equal(t, "media_pending", got.LastErrorClass)
	assert.Zero(t, sender.count(), "nothing is POSTed")
	redirectedMu.Lock()
	defer redirectedMu.Unlock()
	assert.Zero(t, redirected, "the redirect's Location origin receives no request")
}

func TestWorker_MediaGateShutdownMidProbeDecidesNothing(t *testing.T) {
	conn := workerTestDB(t)
	seedWorkerActor(t, conn, true, false)
	headArrived := make(chan struct{})
	var once sync.Once
	proxy := newMediaProxy(t, func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(headArrived) })
		<-r.Context().Done()
	})
	id := seedDelivery(t, conn, "Create", "", mediaCreatePayload(proxy.server.URL))
	// Past the budget, so a cancelled probe misread as a failure would count
	// an exhaustion, mark the activity and deliver.
	_, err := conn.ExecContext(context.Background(),
		`UPDATE outbound_deliveries SET created_at = now() - interval '10 minutes' WHERE activity_id = $1`, id)
	require.NoError(t, err)

	sender := &fakeSender{}
	w := newWorker(t, conn, sender, withMediaGate(proxy.server.URL, 10*time.Second, 2*time.Minute))
	before := metricMediaBudgetExhausted.Value()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-headArrived
		cancel()
	}()

	_, err = w.DeliverNext(ctx)

	require.ErrorIs(t, err, context.Canceled, "a shutdown mid-probe surfaces the cancellation")
	assert.Equal(t, before, metricMediaBudgetExhausted.Value(), "a shutdown is not a budget exhaustion")
	assert.Nil(t, getActivity(t, store.NewOutboundActivities(conn), id).MediaReadyAt,
		"a shutdown does not mark the activity media-ready")
	assert.Zero(t, sender.count(), "a shutdown delivers nothing")
	assert.Equal(t, store.DeliveryStatePending, getDelivery(t, conn, id).State,
		"the delivery stays pending for the next claim")
}

// TestWorker_MediaGateConsentRecheckedAfterProbe pins the retraction-asymmetry
// invariant across the probe: the consent recheck sits immediately before the
// POST, so an actor disabled while the gate is probing is cancelled, not
// delivered on a decision taken before the probe.
func TestWorker_MediaGateConsentRecheckedAfterProbe(t *testing.T) {
	conn := workerTestDB(t)
	ctx := context.Background()
	seedWorkerActor(t, conn, true, false)
	actors := store.NewAPActors(conn)
	var once sync.Once
	proxy := newMediaProxy(t, func(w http.ResponseWriter, _ *http.Request) {
		// The actor is disabled while the first probe is in flight.
		once.Do(func() { require.NoError(t, actors.SetEnabled(ctx, wActorDID, false)) })
		w.WriteHeader(http.StatusOK)
	})
	id := seedDelivery(t, conn, "Create", "", mediaCreatePayload(proxy.server.URL))

	sender := &fakeSender{}
	w := newWorker(t, conn, sender, withMediaGate(proxy.server.URL, 2*time.Second, time.Hour))

	worked, err := w.DeliverNext(ctx)
	require.NoError(t, err)
	require.True(t, worked)

	assert.Positive(t, proxy.totalHeads(), "the gate probed before the consent decision")
	assert.Equal(t, 0, sender.count(), "an actor disabled during the probe is not POSTed")
	assert.Equal(t, store.DeliveryStateCancelled, getDelivery(t, conn, id).State)
}
