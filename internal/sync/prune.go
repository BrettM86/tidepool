package sync

import (
	"context"
	"log/slog"
	"time"

	"tidepool/internal/repo"
)

// defaultPruneInterval is how often the retention job runs; retention
// windows are hours-to-days, so hourly sweeps keep the backlog tight without
// meaningful load.
const defaultPruneInterval = time.Hour

// RunPruner enforces FIREHOSE_RETENTION: it deletes firehose events older
// than the retention window, once immediately and then every interval, until
// ctx is canceled. Consumers whose cursor falls off the retained window get
// an OutdatedCursor #info frame on reconnect (see streamEvents) — that is
// the documented contract of a bounded replay window, matching relay
// rollback behavior. interval <= 0 selects the default.
func RunPruner(ctx context.Context, mgr *repo.Manager, retention, interval time.Duration, logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	if retention <= 0 {
		// Fail closed: a zero retention would compute cutoff == now and
		// delete the entire retained backlog every sweep. Config validates
		// this too, but this function must not rely on one caller.
		logger.Error("firehose pruner refusing to run", "retention", retention.String(),
			"reason", "retention must be positive")
		return
	}
	if interval <= 0 {
		interval = defaultPruneInterval
	}
	prune := func() {
		cutoff := time.Now().Add(-retention)
		n, err := mgr.PruneEvents(ctx, cutoff)
		if err != nil {
			if ctx.Err() == nil {
				logger.Error("firehose retention prune failed", "error", err)
			}
			return
		}
		if n > 0 {
			logger.Info("pruned firehose events",
				"deleted", n, "retention", retention.String(), "cutoff", cutoff.UTC().Format(time.RFC3339))
		}
	}

	prune()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			prune()
		}
	}
}
