package sync

import (
	"context"
	"log/slog"
	"time"

	"tidepool/internal/prune"
	"tidepool/internal/repo"
)

// RunPruner enforces FIREHOSE_RETENTION: it deletes firehose events older
// than the retention window, once immediately and then every interval, until
// ctx is canceled. Consumers whose cursor falls off the retained window get
// an OutdatedCursor #info frame on reconnect (see streamEvents) — that is
// the documented contract of a bounded replay window, matching relay
// rollback behavior. interval <= 0 selects the default; retention <= 0
// refuses to run (fail closed — see internal/prune).
func RunPruner(ctx context.Context, mgr *repo.Manager, retention, interval time.Duration, logger *slog.Logger) {
	prune.Run(ctx, "firehose_events", retention, interval, mgr.PruneEvents, logger)
}
