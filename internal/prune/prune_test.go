package prune

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// A non-positive retention must fail closed: cutoff would be now-or-future, so
// a sweep would delete everything every tick. Run must refuse and never invoke
// the prune fn (this is the branch whose comment warns a zero retention would
// delete the whole table on each pass).
func TestRunFailsClosedOnNonPositiveRetention(t *testing.T) {
	for _, retention := range []time.Duration{0, -time.Hour} {
		calls := 0
		fn := func(_ context.Context, _ time.Time) (int64, error) {
			calls++
			return 0, nil
		}
		Run(context.Background(), "test", retention, time.Hour, fn, discardLogger())
		if calls != 0 {
			t.Fatalf("retention %s: prune fn ran %d time(s), want 0 (fail closed)", retention, calls)
		}
	}
}

// A positive retention sweeps once synchronously before the first tick. Cancel
// the context from inside that first sweep so the loop exits immediately after
// it — no real interval elapses, so the test stays fast and non-flaky.
func TestRunSweepsOnceBeforeFirstTick(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	fn := func(_ context.Context, _ time.Time) (int64, error) {
		calls++
		cancel() // stop the loop right after the immediate pre-tick sweep
		return 0, nil
	}
	// Interval is an hour, so only the synchronous sweep can run before ctx is done.
	Run(ctx, "test", time.Hour, time.Hour, fn, discardLogger())
	if calls != 1 {
		t.Fatalf("prune fn ran %d time(s), want exactly 1 synchronous sweep", calls)
	}
}
