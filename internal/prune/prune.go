// Package prune is the shared retention-sweep loop: run a batched delete
// once immediately and then on an interval, fail closed on a non-positive
// retention, log what was reclaimed. The firehose pruner (task 04) defined
// the discipline; task 11 extracted it so ap_tombstones and vote_events
// retention behave identically.
package prune

import (
	"context"
	"log/slog"
	"time"
)

// DefaultInterval is how often a sweep runs when the caller passes zero;
// retention windows are hours-to-days, so hourly sweeps keep tables tight
// without meaningful load.
const DefaultInterval = time.Hour

// Func deletes rows older than the cutoff and reports how many it removed.
// Implementations batch internally so one sweep never holds long locks.
type Func func(ctx context.Context, cutoff time.Time) (int64, error)

// Run enforces one retention policy until ctx is cancelled. retention <= 0
// refuses to run (fail closed: a zero retention would compute cutoff == now
// and delete everything, every sweep); interval <= 0 selects
// DefaultInterval.
func Run(ctx context.Context, name string, retention, interval time.Duration, fn Func, logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	if retention <= 0 {
		logger.Error("pruner refusing to run",
			"pruner", name, "retention", retention.String(), "reason", "retention must be positive")
		return
	}
	if interval <= 0 {
		interval = DefaultInterval
	}
	sweep := func() {
		cutoff := time.Now().Add(-retention)
		n, err := fn(ctx, cutoff)
		if err != nil {
			if ctx.Err() == nil {
				// A batched pruner can fail mid-run after deleting some rows
				// (n>0, err): log the partial count so the reclaimed work is
				// not invisible.
				logger.Error("retention prune failed", "pruner", name, "deleted", n, "error", err)
			}
			return
		}
		if n > 0 {
			logger.Info("pruned expired rows", "pruner", name, "deleted", n,
				"retention", retention.String(), "cutoff", cutoff.UTC().Format(time.RFC3339))
		}
	}

	sweep()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweep()
		}
	}
}
