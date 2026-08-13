package consume

import (
	"context"
	"log/slog"
)

// noopEnqueuer logs the outbound intents it is handed and delivers nothing.
// It is what main wires until task 15 lands, following the v1 precedent
// (ingest.NewNoopVotes): the consumer runs, writes its state, and makes the
// work it WOULD deliver visible, rather than being disabled entirely and
// leaving the whole path unexercised until delivery exists.
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
func (e *noopEnqueuer) EnqueueActivity(_ context.Context, actorDID, orderingKey, parentATURI string, intent Intent) error {
	e.logger.Info("outbound intent dropped: no delivery queue wired",
		slog.String("actor", actorDID),
		slog.String("ordering_key", orderingKey),
		slog.String("parent", parentATURI),
		slog.String("activity", intent.ActivityID()))
	return nil
}
