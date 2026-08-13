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
)

// deadLetterDepthUnavailable is what the backlog gauge reports when storage
// cannot be read. A negative value is impossible for a count, so it is
// unmistakable — where a 0 would claim the backlog is empty at exactly the
// moment nobody can tell.
const deadLetterDepthUnavailable = -1

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
// green, and events simply stop arriving. Cursor age and last-event age are
// the pair that tells a dead upstream from a dead consumer — a quiet stream
// keeps the cursor current while last-event age grows.
//
// Idempotent: a second call is a no-op, so the FIRST connector and queue
// handed in are the ones the gauges read for the life of the process.
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
			counts, err := queue.CountDeadLetters(ctx)
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
