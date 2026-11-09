package outbound

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/store"
)

// parkImagePath is the one image a media-park payload carries, under the Coves
// image proxy's /img/ route.
const parkImagePath = "/img/content_full/plain/" + wActorDID +
	"/bafkreihak3l6grklnl53pnqjtrwfph7wyojvrsaazb7nmptucuhobephqy"

// newParkImageProxy is a loopback stand-in for the Coves image proxy. It
// answers every request with the status in status, so a test can start it at
// 404 (the AppView has not indexed the image) and flip it to 200 later.
func newParkImageProxy(t *testing.T, status *atomic.Int32) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(int(status.Load()))
	}))
	t.Cleanup(server.Close)
	return server
}

// parkMediaPayload is a stored Create{Note} with one image under base, the
// same base the worker's media gate is configured with.
func parkMediaPayload(base string) []byte {
	return []byte(fmt.Sprintf(`{"@context":"https://www.w3.org/ns/activitystreams","id":"x","type":"Create",`+
		`"actor":%q,"object":{"type":"Note","content":"<p><img src=\"%s%s\" alt=\"one\"></p>","mediaType":"text/html"}}`,
		wActorID, base, parkImagePath))
}

// withParkMediaGate turns the worker's media gate on against base, probing
// through the guarded client with loopback allowed (the proxy is local), and
// with a wait budget long enough that no park here ends in exhaustion.
func withParkMediaGate(base string) func(*WorkerOptions) {
	return func(o *WorkerOptions) {
		o.MediaBaseURL = base
		o.MediaProbeClient = ap.NewGuardedHTTPClient(true, 2*time.Second)
		o.MediaParkDelay = time.Minute
		o.MediaWaitBudget = time.Hour
	}
}

// Every place the worker HOLDS a delivery instead of trying it must cost that
// delivery nothing. There are four such sites — the kill switch, dry-run, the
// causal wait and the media wait — and they are one behavior, not four: a hold
// is not an attempt, whatever the reason for holding. Each one is also a park
// the fence accepted, so each one counts in tidepool_outbound_parked.
//
// Pinning all three together is deliberate. The switch site is the one an
// operator feels (see the outer acceptance test), but a per-site fix would leave
// dry-run silently burning the budget of a queue nobody is watching, and the
// causal wait burning the budget of every reply whose parent is a beat behind.

func TestWorker_ParkSitesAreAttemptNeutral(t *testing.T) {
	var proxyStatus atomic.Int32
	proxyStatus.Store(http.StatusNotFound)
	proxy := newParkImageProxy(t, &proxyStatus)

	for _, tc := range []struct {
		name        string
		class       string
		parentATURI string
		payload     []byte
		seed        func(t *testing.T, conn *sql.DB)
		opts        func(*WorkerOptions)
	}{
		{
			name:  "kill switch",
			class: "switch_parked",
			opts:  func(o *WorkerOptions) { o.Switches = &fakeSwitches{allow: false} },
		},
		{
			name:  "dry run",
			class: "dry_run",
			opts:  func(o *WorkerOptions) { o.Switches = &fakeSwitches{allow: true, dryRun: true} },
		},
		{
			name:  "causal wait",
			class: "parent_pending",
			// A bridge-origin parent that has not been accepted yet: the reply
			// is held until it lands (see causal_gating_test.go).
			parentATURI: gParentATURI,
			seed:        func(t *testing.T, conn *sql.DB) { seedBridgeParent(t, conn, false) },
		},
		{
			name:  "media wait",
			class: "media_pending",
			// The image proxy answers 404 for the payload's image: the AppView
			// has not indexed it yet.
			payload: parkMediaPayload(proxy.URL),
			opts:    withParkMediaGate(proxy.URL),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := workerTestDB(t)
			seedWorkerActor(t, conn, true, false)
			if tc.seed != nil {
				tc.seed(t, conn)
			}
			payload := tc.payload
			if payload == nil {
				payload = createPayload("x")
			}
			id := seedDelivery(t, conn, "Create", tc.parentATURI, payload)
			require.Zero(t, getDelivery(t, conn, id).Attempts,
				"a freshly enqueued delivery has spent nothing yet — this is the value a park must restore")

			sender := &fakeSender{}
			w := newWorker(t, conn, sender, tc.opts)
			parkedBefore := metricParked.Value()

			worked, err := w.DeliverNext(context.Background())
			require.NoError(t, err)
			require.True(t, worked, "the delivery was claimed and handled")
			require.Zero(t, sender.count(), "a park POSTs nothing — the delivery was HELD, not tried")

			got := getDelivery(t, conn, id)
			assert.Equal(t, 0, got.Attempts,
				"a full claim/park cycle leaves the attempt ledger where it started: the hold gives back "+
					"the increment its own claim took, so the delivery arrives at the attempt cap only "+
					"through attempts it actually made")
			assert.Equal(t, store.DeliveryStatePending, got.State,
				"a parked delivery stays pending — holding is not failing")
			assert.Equal(t, tc.class, got.LastErrorClass,
				"the reason for the hold is recorded, so the give-back is auditable rather than a silent rewind")
			assert.Equal(t, parkedBefore+1, metricParked.Value(),
				"a park the fence accepted is counted once in tidepool_outbound_parked, the only signal "+
					"that tells a held pending row from one merely waiting its turn")
		})
	}
}

// The causal wait is the one hold whose OUTCOME is not "wait forever": it
// poisons on a wall-clock deadline. That makes it the site where the two
// mechanisms could quietly be wired to each other — and they must not be.
//
// This guard holds both facts down at once, because each is a plausible way to
// break the other: an attempt-neutral park built by suppressing the release
// bookkeeping would take the causal deadline's accounting with it (the child
// waits forever on a parent that never lands), and a deadline defended by
// leaning on the attempt counter would put the budget back in the park's hands.
// Neither passes this test. Cf. causal_hardening_test.go, which pins the same
// deadline from the other direction (attempts=99 must NOT poison early).
func TestCausalGating_ParksAreFreeButTheWallClockStillPoisons(t *testing.T) {
	conn := workerTestDB(t)
	ctx := context.Background()
	seedWorkerActor(t, conn, true, false)
	seedBridgeParent(t, conn, false) // parent never accepted
	id := seedDelivery(t, conn, "Create", gParentATURI, createPayload("x"))

	sender := &fakeSender{}
	w := newWorker(t, conn, sender, func(o *WorkerOptions) { o.CausalWaitBudget = time.Hour })

	// The spin a child does while its parent is in flight. parkCausal stamps
	// next_attempt_at from the APP clock while ClaimNext compares against the
	// DB's, so on a host running a few hundred microseconds ahead of Postgres a
	// back-to-back re-claim can miss by a hair; the nudge re-stamps the schedule
	// from the DB's own now() so each cycle here is deterministic. It is the
	// schedule column only — a poison would still be plainly visible.
	for i := 0; i < 5; i++ {
		worked, err := w.DeliverNext(ctx)
		require.NoError(t, err)
		require.Truef(t, worked, "cycle %d: the held child stays claimable", i)
		clearParkDelay(t, conn, id)
	}
	require.Zero(t, sender.count(), "a held child is never POSTed")

	held := getDelivery(t, conn, id)
	assert.Equal(t, 0, held.Attempts,
		"five causal parks cost the child nothing: the retries it will need once its parent lands "+
			"are still there")
	assert.Equal(t, store.DeliveryStatePending, held.State, "and it is still waiting, not decided")

	// Now the wall clock, and only the wall clock, decides. The attempt ledger
	// reads zero — a counter-driven deadline would wait forever here.
	_, err := conn.ExecContext(ctx,
		`UPDATE outbound_deliveries SET created_at = now() - interval '2 hours' WHERE activity_id = $1`, id)
	require.NoError(t, err)

	worked, err := w.DeliverNext(ctx)
	require.NoError(t, err)
	require.True(t, worked)

	expired := getDelivery(t, conn, id)
	assert.Equal(t, store.DeliveryStatePoisoned, expired.State,
		"the causal budget is bounded by ELAPSED TIME, not by attempts: a parent that never landed "+
			"still poisons its child on the deadline, however cheap the waiting was")
	assert.Equal(t, store.PoisonClassParentUnaccepted, expired.LastErrorClass,
		"and it poisons for the causal reason, so the outcome stays queryable and redrivable")
}
