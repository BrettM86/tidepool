package consume

import (
	"context"
	"database/sql"
	"log/slog"
)

// noopEnqueuer logs the outbound intents it is handed and delivers nothing.
// It is the consumer-DISABLED default: it drops intents by design so a path that
// is not running end to end makes the work it WOULD deliver visible in the log
// rather than accumulating it. It is NOT a pre-task-15 placeholder — whenever
// CONSUMER_ENABLED, main wires the real persisting outbound.Enqueuer instead.
type noopEnqueuer struct {
	logger *slog.Logger
}

// NewNoopEnqueuer returns the logging no-op OutboundEnqueuer. A nil logger
// uses slog.Default().
func NewNoopEnqueuer(logger *slog.Logger) OutboundEnqueuer {
	if logger == nil {
		logger = slog.Default()
	}
	return &noopEnqueuer{logger: logger}
}

// EnqueueActivity records what WOULD have been delivered and drops it. The
// activity id is logged because it is the one field a later delivery has to
// reproduce exactly: it is what a peer dedupes on.
func (e *noopEnqueuer) EnqueueActivity(_ context.Context, _ *sql.Tx, actorDID, orderingKey, parentATURI string, intent Intent) error {
	e.logger.Info("outbound intent dropped: no delivery queue wired",
		slog.String("actor", actorDID),
		slog.String("ordering_key", orderingKey),
		slog.String("parent", parentATURI),
		slog.String("activity", intent.ActivityID()))
	return nil
}
