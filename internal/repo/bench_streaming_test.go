package repo

// Streaming-path benchmark for the task-12 getRepo work. Separate from
// bench_test.go because ExportCARTo does not exist in the "before" tree that
// file also runs against.

import (
	"io"
	"testing"
)

func BenchmarkExportCARTo_BigRepo(b *testing.B) {
	manager := benchFixtureManager(b)
	ctx := b.Context()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := manager.ExportCARTo(ctx, benchDID, io.Discard); err != nil {
			b.Fatal(err)
		}
	}
}
