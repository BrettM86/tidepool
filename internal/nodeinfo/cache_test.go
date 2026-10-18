package nodeinfo_test

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/nodeinfo"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

func TestCacheSoftwareDiscoversAndStoresUnknownHost(t *testing.T) {
	database := testutil.DB(t)

	tests := []struct {
		name    string
		host    string
		rel     string
		docPath string
		version string
		rawName string
		want    string
	}{
		{name: "2.0 Lemmy", host: "lemmy.test", rel: rel20, docPath: "/nodeinfo/2.0", version: "2.0", rawName: `"Lemmy"`, want: "lemmy"},
		{name: "2.1 mbin", host: "mbin.test", rel: rel21, docPath: "/nodeinfo/2.1", version: "2.1", rawName: `"mbin"`, want: "mbin"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testutil.Truncate(t, database, "instances")
			instance := newFixtureInstance(t, map[string]fixtureDocument{
				"/.well-known/nodeinfo": {body: wellKnown([2]string{tt.rel, "https://" + tt.host + tt.docPath})},
				tt.docPath:              {body: nodeinfoDoc(tt.version, tt.rawName)},
			})
			fetcher, err := nodeinfo.NewFetcher(instance.testClient(), testUserAgent)
			require.NoError(t, err)
			cache, err := nodeinfo.NewCache(nodeinfo.CacheOptions{
				Fetcher: fetcher,
				Store:   store.NewInstances(database),
			})
			require.NoError(t, err)

			// Postgres stores microseconds; widen the window to that grain.
			start := time.Now().Truncate(time.Microsecond)
			got := cache.Software(context.Background(), "https", tt.host)
			end := time.Now().Truncate(time.Microsecond).Add(time.Microsecond)

			assert.Equal(t, tt.want, got)

			var softwareName sql.NullString
			var fetchedAt time.Time
			err = database.QueryRowContext(context.Background(),
				`SELECT software_name, fetched_at FROM instances WHERE host = $1`, tt.host).
				Scan(&softwareName, &fetchedAt)
			require.NoError(t, err, "instances row for %s", tt.host)
			assert.Equal(t, sql.NullString{String: tt.want, Valid: true}, softwareName)
			assert.WithinRange(t, fetchedAt, start, end)
		})
	}
}

// blockingInstances is a store.Instances whose Get calls after the first
// unblocked ones hang until release closes: a stuck Postgres read. They
// give up when their context ends, as the driver does, unless ignoreContext
// is set (a read stuck past cancellation). Records succeed without storing
// anything.
type blockingInstances struct {
	stale         *store.Instance // returned by the unblocked Get calls
	unblocked     int             // how many Get calls return stale before hanging
	ignoreContext bool
	release       <-chan struct{}

	mu    sync.Mutex
	calls int
}

func (b *blockingInstances) Get(ctx context.Context, _ string) (*store.Instance, error) {
	b.mu.Lock()
	b.calls++
	call := b.calls
	b.mu.Unlock()
	if call <= b.unblocked {
		copied := *b.stale
		return &copied, nil
	}
	if b.ignoreContext {
		<-b.release
		return nil, context.Canceled
	}
	select {
	case <-b.release:
		return nil, context.Canceled
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (b *blockingInstances) RecordSoftware(context.Context, string, string, time.Time) error {
	return nil
}

func (b *blockingInstances) RecordFetchFailure(context.Context, string, time.Time) error {
	return nil
}

// TestCacheSoftwareBoundedByDeadlineWhenStoreHangs: a caller whose context
// has no deadline returns within the discovery deadline even when the
// store read hangs, with the last name it knows.
func TestCacheSoftwareBoundedByDeadlineWhenStoreHangs(t *testing.T) {
	const (
		host     = "lemmy.test"
		deadline = 200 * time.Millisecond
		// slack covers timer and scheduling latency; the unbounded cycle
		// never returns, and the old worst case was the deadline plus 5s.
		slack = 250 * time.Millisecond
	)
	stale := &store.Instance{Host: host, SoftwareName: "lemmy", FetchedAt: time.Now().Add(-25 * time.Hour)}

	tests := []struct {
		name          string
		unblocked     int
		ignoreContext bool
		want          string
	}{
		{name: "the shared cycle's re-read hangs past cancellation: the stale name is returned", unblocked: 1, ignoreContext: true, want: "lemmy"},
		{name: "the shared cycle's re-read hangs: the stale name is returned", unblocked: 1, want: "lemmy"},
		{name: "the caller's first read hangs: no name is returned", unblocked: 0, want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			release := make(chan struct{})
			t.Cleanup(func() { close(release) })
			instance := newFixtureInstance(t, map[string]fixtureDocument{})
			fetcher, err := nodeinfo.NewFetcher(instance.testClient(), testUserAgent)
			require.NoError(t, err)
			cache, err := nodeinfo.NewCache(nodeinfo.CacheOptions{
				Fetcher:           fetcher,
				Store:             &blockingInstances{stale: stale, unblocked: tt.unblocked, ignoreContext: tt.ignoreContext, release: release},
				DiscoveryDeadline: deadline,
			})
			require.NoError(t, err)

			returned := make(chan string, 1)
			start := time.Now()
			go func() { returned <- cache.Software(context.Background(), "https", host) }()

			select {
			case got := <-returned:
				elapsed := time.Since(start)
				assert.Equal(t, tt.want, got)
				assert.LessOrEqual(t, elapsed, deadline+slack)
			case <-time.After(deadline + 5*time.Second):
				t.Fatalf("Software did not return within %s of a %s deadline", deadline+5*time.Second, deadline)
			}
		})
	}
}
