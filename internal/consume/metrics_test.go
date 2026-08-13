package consume

import (
	"context"
	"expvar"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Task 14 cycle K2: observability.
//
// A stalled consumer is the failure this task most has to make visible,
// because it is otherwise invisible: the process is up, the health check is
// green, and events simply stop arriving. Cursor age and dead-letter depth are
// the two numbers that say so.

// metricValue reads a published expvar as a float. A missing var fails the
// test rather than returning a zero that would look like a healthy gauge.
func metricValue(t *testing.T, name string) float64 {
	t.Helper()
	published := expvar.Get(name)
	require.NotNil(t, published, "expvar %q must be published", name)
	value, err := strconv.ParseFloat(strings.Trim(published.String(), `"`), 64)
	require.NoError(t, err, "expvar %q must publish a number, got %s", name, published.String())
	return value
}

func TestPublishMetrics_ExposesTheGaugesAnOperatorNeeds(t *testing.T) {
	database := connectorTestDB(t)
	state := NewPostgresStateStore(database, CursorSchemaVersion)
	handler := &recordingHandler{}

	// A recent event, so cursor age is a small number rather than the decades
	// a synthetic time_us would produce.
	nowUS := time.Now().UnixMicro()
	fake := newFakeJetstream(t, connFrame(nowUS, "3lzrevmetric1", "aaa"))
	connector := newTestConnector(t, fake.URL(), handler, state)
	startConnector(t, connector)

	waitFor(t, "the scripted event to be processed", func() bool { return handler.Calls() >= 1 })

	require.NoError(t, state.AddDeadLetter(context.Background(), ConsumerNative, 1,
		[]byte(`{"time_us":1}`), "boom", 0))

	PublishMetrics(context.Background(), connector, state)

	cursorAge := metricValue(t, MetricCursorAgeSeconds)
	assert.GreaterOrEqual(t, cursorAge, 0.0)
	assert.Less(t, cursorAge, 60.0,
		"cursor age is how far behind the stream the consumer is; a consumer that just "+
			"processed a live event must read near zero, or the number is measuring "+
			"something else")

	lastEventAge := metricValue(t, MetricLastEventAgeSeconds)
	assert.GreaterOrEqual(t, lastEventAge, 0.0)
	assert.Less(t, lastEventAge, 60.0,
		"last-event age is the other half: a QUIET stream keeps the cursor current "+
			"while this number grows, which is how a dead upstream is told apart from "+
			"a dead consumer")

	assert.Equal(t, 1.0, metricValue(t, MetricDeadLetterDepth),
		"the DLQ backlog is read from storage at scrape time, not counted in memory — "+
			"a restart must not reset it to zero and declare the backlog gone")
	assert.Equal(t, 1.0, metricValue(t, MetricEventsProcessed))
	assert.Equal(t, 0.0, metricValue(t, MetricEventsDeadLettered))
	assert.GreaterOrEqual(t, metricValue(t, MetricReconnects), 0.0)
}

func TestPublishMetrics_NamesAreServedByTheAdminEndpoint(t *testing.T) {
	database := connectorTestDB(t)
	state := NewPostgresStateStore(database, CursorSchemaVersion)
	fake := newFakeJetstream(t)
	connector := newTestConnector(t, fake.URL(), &recordingHandler{}, state)

	PublishMetrics(context.Background(), connector, state)

	for _, name := range []string{
		MetricCursorAgeSeconds, MetricLastEventAgeSeconds, MetricDeadLetterDepth,
		MetricReconnects, MetricEventsProcessed, MetricEventsDeadLettered,
	} {
		assert.True(t, strings.HasPrefix(name, "tidepool_"),
			"ingest.scopedMetrics serves ONLY the tidepool_ prefix, so a gauge named "+
				"without it is published to expvar and then filtered out of "+
				"/admin/metrics — indistinguishable from a gauge nobody updates. %q", name)
		assert.NotNil(t, expvar.Get(name), "%q must be published", name)
	}
}

func TestPublishMetrics_IsIdempotent(t *testing.T) {
	database := connectorTestDB(t)
	state := NewPostgresStateStore(database, CursorSchemaVersion)
	fake := newFakeJetstream(t)
	connector := newTestConnector(t, fake.URL(), &recordingHandler{}, state)

	PublishMetrics(context.Background(), connector, state)
	assert.NotPanics(t, func() { PublishMetrics(context.Background(), connector, state) },
		"expvar panics on a duplicate name, and both main and the tests call this — a "+
			"second call must be a no-op rather than taking the process down at boot")
}

func TestPublishMetrics_SurvivesAStoreThatCannotBeReached(t *testing.T) {
	database := connectorTestDB(t)
	state := NewPostgresStateStore(database, CursorSchemaVersion)
	fake := newFakeJetstream(t)
	connector := newTestConnector(t, fake.URL(), &recordingHandler{}, state)

	PublishMetrics(context.Background(), connector, failingDeadLetters{})

	assert.NotPanics(t, func() { _ = expvar.Get(MetricDeadLetterDepth).String() },
		"a gauge is read while an operator is looking at a broken system, so a failing "+
			"storage read must produce a value rather than panic inside the metrics "+
			"handler and take the endpoint down with it")
}

// failingDeadLetters answers every query with an error — postgres down, which
// is exactly when someone is reading the metrics.
type failingDeadLetters struct{ DeadLetterQueue }

func (failingDeadLetters) CountDeadLetters(context.Context) (map[string]int64, error) {
	return nil, assert.AnError
}

func TestNewNoopEnqueuer_AcceptsIntentsAndDeliversNothing(t *testing.T) {
	enqueuer := NewNoopEnqueuer(nil)
	require.NotNil(t, enqueuer,
		"a nil logger must default rather than nil-panic on the first intent")

	err := enqueuer.EnqueueActivity(context.Background(), dispatchNativeDID, "key", "at://parent",
		CommentIntent{Op: "create", ATURI: "at://x", ID: "https://coves.social/ap/activity/abc"})
	assert.NoError(t, err,
		"until task 15 lands, main wires this so the consumer still RUNS and writes its "+
			"durable state — disabling the whole path instead would leave everything "+
			"downstream of the cursor unexercised until delivery exists")

	assert.NoError(t, enqueuer.EnqueueActivity(context.Background(), dispatchNativeDID, "key", "",
		VoteIntent{Op: "undo", VoteATURI: "at://v", Direction: "up"}))
}
