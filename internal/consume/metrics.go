package consume

import (
	"context"
	"expvar"
	"sync"
	"time"
)

// Metric names published for /admin/metrics. They carry the "tidepool_"
// prefix because ingest.scopedMetrics serves ONLY that prefix — a gauge named
// without it is published to expvar and then filtered out of the endpoint,
// which looks exactly like a gauge that is not being updated.
const (
	MetricCursorAgeSeconds    = "tidepool_consumer_cursor_age_seconds"
	MetricLastEventAgeSeconds = "tidepool_consumer_last_event_age_seconds"
	MetricDeadLetterDepth     = "tidepool_consumer_dead_letters"
	MetricReconnects          = "tidepool_consumer_reconnects"
	MetricEventsProcessed     = "tidepool_consumer_events_processed"
	MetricEventsDeadLettered  = "tidepool_consumer_events_dead_lettered"
	MetricConnected           = "tidepool_consumer_connected"
	MetricDialFailures        = "tidepool_consumer_dial_failures"
	MetricDisconnectedSeconds = "tidepool_consumer_disconnected_seconds"
	MetricUnclaimedSkips      = "tidepool_consumer_unclaimed_skips"
)

// unclaimedSkips counts the handler skips that DELIBERATELY release the rev
// gate un-advanced (errSkipUnclaimed): a vote whose subject has not
// materialized, a comment whose thread has not, a post for a community nobody
// has bridged yet, a vote direction this build does not understand.
//
// It is the only trace such an event leaves. It is not a failure, so it never
// reaches the DLQ; it is not applied, so it writes no state; and the event is
// reported handled, so the cursor moves past it. A backlog of them is a real
// condition — a community that should have been bridged, a materializer that
// has stalled — and without this counter the only symptom is content quietly
// not federating.
//
// A COUNTER RATHER THAN A GAUGE, and it is a RATE that matters, not a total:
// the ordinary case (a native user voting in a native community) increments it
// constantly, so the number is meaningless in isolation and informative when it
// moves against its own baseline.
//
// Declared at package scope rather than inside PublishMetrics because the
// increment happens in the gate, which knows nothing about the connector the
// other gauges read; expvar.NewInt registers it exactly once per process.
var unclaimedSkips = expvar.NewInt(MetricUnclaimedSkips)

// deadLetterDepthUnavailable is what the backlog gauge reports when storage
// cannot be read. A negative value is impossible for a count, so it is
// unmistakable — where a 0 would claim the backlog is empty at exactly the
// moment nobody can tell.
const deadLetterDepthUnavailable = -1

// deadLetterScrapeTimeout bounds the per-scrape storage read. A gauge is read
// while an operator watches a possibly-broken system, so a hung query must not
// hang the metrics handler with it.
const deadLetterScrapeTimeout = 3 * time.Second

// publishOnce guards the expvar registration. expvar PANICS on a duplicate
// name, and both main and the tests call PublishMetrics.
var publishOnce sync.Once

// PublishMetrics registers the consumer's gauges with expvar.
//
// They are expvar.Func rather than counters the consumer pokes: cursor age and
// dead-letter depth are QUESTIONS about the current state, and a value pushed
// on every event would go stale precisely when the consumer stalls — which is
// the moment an operator looks at it.
//
// A stalled consumer is the failure this task most has to make visible,
// because it is otherwise invisible: the process is up, the health check is
// green, and events simply stop arriving. How to READ the gauges:
//
//   - cursor age HIGH, last-event age LOW: events are arriving but the
//     consumer's processed position trails them — it is lagging behind live
//     traffic. (Both ages move together on a quiet stream, so last-event age
//     alone cannot distinguish lag; the SPREAD between the two is the lag.)
//   - cursor age HIGH, last-event age HIGH: no events are arriving at all —
//     either the stream is quiet or the connection is dead. `connected` and
//     `dial_failures`/`disconnected_seconds` disambiguate: connected=1 with no
//     events is a quiet upstream; connected=0 with climbing dial_failures is
//     the consumer unable to reach Jetstream.
//   - connected=0 with disconnected_seconds climbing from boot and reconnects
//     still 0 is a consumer that has NEVER connected — the failure a
//     healthy-looking zero would otherwise hide.
//
// Idempotent: a second call is a no-op, so the FIRST connector and queue
// handed in are the ones the gauges read for the life of the process. The ctx
// is used only to derive per-scrape deadlines; it is not the scrape's own
// lifetime.
func PublishMetrics(ctx context.Context, connector *Connector, queue DeadLetterQueue) {
	publishOnce.Do(func() {
		expvar.Publish(MetricCursorAgeSeconds, expvar.Func(func() any {
			return ageSeconds(cursorTime(connector.Status().CursorTimeUS))
		}))
		expvar.Publish(MetricLastEventAgeSeconds, expvar.Func(func() any {
			status := connector.Status()
			if status.LastEventAt == nil {
				return 0.0
			}
			return ageSeconds(*status.LastEventAt)
		}))
		expvar.Publish(MetricDeadLetterDepth, expvar.Func(func() any {
			// Read from STORAGE at scrape time, not counted in memory: a
			// restart must not reset the backlog to zero and declare it gone.
			// Bounded by its own deadline so a hung query cannot hang the
			// scrape — the parent ctx supplies only cancellation, not a
			// wall-clock the scrape should wait out.
			scrapeCtx, cancel := context.WithTimeout(ctx, deadLetterScrapeTimeout)
			defer cancel()
			counts, err := queue.CountDeadLetters(scrapeCtx)
			if err != nil {
				// This gauge is read while somebody is looking at a broken
				// system, so a failing storage read reports unavailable rather
				// than panicking inside the metrics handler and taking the
				// endpoint down with it.
				return deadLetterDepthUnavailable
			}
			var total int64
			for _, count := range counts {
				total += count
			}
			return total
		}))
		expvar.Publish(MetricReconnects, expvar.Func(func() any {
			return connector.Status().Reconnects
		}))
		expvar.Publish(MetricEventsProcessed, expvar.Func(func() any {
			return connector.Status().EventsProcessed
		}))
		expvar.Publish(MetricEventsDeadLettered, expvar.Func(func() any {
			return connector.Status().EventsDeadLettered
		}))
		// Liveness gauges: without these a consumer that never achieves its
		// first connection reports every counter at a healthy-looking zero.
		expvar.Publish(MetricConnected, expvar.Func(func() any {
			if connector.Status().Connected {
				return 1
			}
			return 0
		}))
		expvar.Publish(MetricDialFailures, expvar.Func(func() any {
			return connector.Status().DialFailures
		}))
		expvar.Publish(MetricDisconnectedSeconds, expvar.Func(func() any {
			status := connector.Status()
			if status.Connected || status.DisconnectedSince == nil {
				return 0.0
			}
			return ageSeconds(*status.DisconnectedSince)
		}))
	})
}

// cursorTime converts a Jetstream cursor to wall time. A zero cursor means the
// consumer has never processed an event, which is reported as "no age" rather
// than the decades since the epoch.
func cursorTime(cursorTimeUS int64) time.Time {
	if cursorTimeUS <= 0 {
		return time.Time{}
	}
	return time.UnixMicro(cursorTimeUS)
}

// ageSeconds is how long ago a moment was, floored at zero (clock skew between
// the app and the event source must not produce a negative age).
func ageSeconds(moment time.Time) float64 {
	if moment.IsZero() {
		return 0
	}
	age := time.Since(moment).Seconds()
	if age < 0 {
		return 0
	}
	return age
}
