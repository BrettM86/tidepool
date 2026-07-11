package repo

// Task-12 before/after benchmarks. This file deliberately uses only API that
// exists both before and after the task (NewManager's variadic options make
// the 3-arg call compile on both sides), so the identical file can run
// against a HEAD checkout for the "before" numbers — recorded in the task-12
// commit message. One backport is still needed
// on the HEAD side: the two-line testutil.DB/testutil.Truncate *testing.T →
// testing.TB widening, which HEAD lacks (its *testing.T signatures reject
// this file's *testing.B callers). Run with a fixed iteration
// count so before/after repo growth matches:
//
//	go test ./internal/repo -bench 'PutRecord_BigRepo|ExportCAR_BigRepo' \
//	    -run '^$' -benchtime 20x -benchmem
//
// The streaming-path benchmark lives in bench_streaming_test.go (new API,
// working tree only).

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/bluesky-social/indigo/atproto/atcrypto"

	"tidepool/internal/testutil"
)

// benchRepoSize is the fixture size: a community repo a few months into
// bridging a mid-size Lemmy community (posts + comments).
const benchRepoSize = 2000

const benchDID = "did:plc:bbenchbenchbenchbenchbig"

var (
	benchOnce    sync.Once
	benchShared  *Manager
	benchPreload error
)

// benchFixtureManager returns ONE shared Manager (steady-state: whatever
// cache the build has is warm after the preload) over a repo preloaded with
// benchRepoSize records. The preload runs once per process; benchmarks that
// append use rkey indexes far above the fixture range. Run with -count=1 —
// a second in-process run would re-put identical records and measure the
// idempotent NoOp path instead.
func benchFixtureManager(b *testing.B) *Manager {
	b.Helper()
	database := testutil.DB(b)
	benchOnce.Do(func() {
		testutil.Truncate(b, database, "blocks", "repo_state", "firehose_events")
		key, err := atcrypto.GeneratePrivateKeyK256()
		if err != nil {
			benchPreload = err
			return
		}
		benchShared, err = NewManager(database, &staticKeys{key: key}, nil)
		if err != nil {
			benchPreload = err
			return
		}
		ctx := context.Background()
		for i := 0; i < benchRepoSize; i++ {
			if _, err := benchShared.PutRecord(ctx, benchDID, testCollection, testRKey(i),
				testRecord(fmt.Sprintf("fixture post %d", i))); err != nil {
				benchPreload = fmt.Errorf("preload record %d: %w", i, err)
				return
			}
		}
	})
	if benchPreload != nil {
		b.Fatal(benchPreload)
	}
	return benchShared
}

func BenchmarkPutRecord_BigRepo(b *testing.B) {
	manager := benchFixtureManager(b)
	ctx := b.Context()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := manager.PutRecord(ctx, benchDID, testCollection, testRKey(1_000_000+i),
			testRecord(fmt.Sprintf("bench post %d", i))); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExportCAR_BigRepo(b *testing.B) {
	manager := benchFixtureManager(b)
	ctx := b.Context()
	b.ReportAllocs()
	b.ResetTimer()
	var carLen int
	for i := 0; i < b.N; i++ {
		carBytes, err := manager.ExportCAR(ctx, benchDID)
		if err != nil {
			b.Fatal(err)
		}
		carLen = len(carBytes)
	}
	b.ReportMetric(float64(carLen), "car-bytes")
}
