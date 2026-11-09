package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/testutil"
)

// TestInstances_HostGoneStreak pins the host-gone streak bookkeeping backfill
// relies on: RecordHostGone returns when the host's current streak began, a
// failure within HostGoneStreakGap (48h) of the previous one continues the
// streak, a longer quiet gap or ClearHostGone starts a new one, and a host
// with no instances row yet can start a streak. The steps run in order
// against one database; each step's state is what the previous steps left.
func TestInstances_HostGoneStreak(t *testing.T) {
	database := testutil.DB(t)
	testutil.Truncate(t, database, "instances")
	repo := NewInstances(database)
	ctx := context.Background()

	const (
		discoveredHost = "dead.example"
		newHost        = "never-seen.example"
		lateHost       = "late.example"
	)
	discoveredAt := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	require.NoError(t, repo.RecordSoftware(ctx, discoveredHost, "lemmy", discoveredAt))

	start := time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)
	steps := []struct {
		name string
		host string
		// clearFirst calls ClearHostGone(host) before recording.
		clearFirst bool
		at         time.Time
		wantSince  time.Time
	}{
		{
			name:      "first failure starts the streak",
			host:      discoveredHost,
			at:        start,
			wantSince: start,
		},
		{
			name:      "failure 24h later continues the streak",
			host:      discoveredHost,
			at:        start.Add(24 * time.Hour),
			wantSince: start,
		},
		{
			name:      "failure exactly 48h after the previous one continues the streak",
			host:      discoveredHost,
			at:        start.Add(72 * time.Hour),
			wantSince: start,
		},
		{
			name:      "failure 49h after the previous one starts a new streak",
			host:      discoveredHost,
			at:        start.Add(121 * time.Hour),
			wantSince: start.Add(121 * time.Hour),
		},
		{
			name:       "ClearHostGone ends the streak so the next failure starts one",
			host:       discoveredHost,
			clearFirst: true,
			at:         start.Add(122 * time.Hour),
			wantSince:  start.Add(122 * time.Hour),
		},
		{
			name:      "host with no instances row starts a streak",
			host:      newHost,
			at:        start,
			wantSince: start,
		},
		{
			name:      "the new host's streak continues like any other",
			host:      newHost,
			at:        start.Add(24 * time.Hour),
			wantSince: start,
		},
		{
			name:      "late-record host starts a streak",
			host:      lateHost,
			at:        start,
			wantSince: start,
		},
		{
			name:      "late-record host fails again 40h later",
			host:      lateHost,
			at:        start.Add(40 * time.Hour),
			wantSince: start,
		},
		{
			name:      "an older failure recorded late continues the streak",
			host:      lateHost,
			at:        start.Add(1 * time.Hour),
			wantSince: start,
		},
		{
			// 40h after the latest failure, 79h after the late one: the late
			// record must not have moved the latest failure back to T+1h.
			name:      "a failure 40h after the latest continues the streak despite the late record",
			host:      lateHost,
			at:        start.Add(80 * time.Hour),
			wantSince: start,
		},
	}
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			if step.clearFirst {
				require.NoError(t, repo.ClearHostGone(ctx, step.host))
			}
			goneSince, err := repo.RecordHostGone(ctx, step.host, step.at)
			require.NoError(t, err)
			assert.True(t, step.wantSince.Equal(goneSince),
				"goneSince = %v, want %v", goneSince, step.wantSince)
		})
	}

	// Streak bookkeeping leaves nodeinfo discovery state alone.
	discovered, err := repo.Get(ctx, discoveredHost)
	require.NoError(t, err)
	assert.Equal(t, "lemmy", discovered.SoftwareName, "software_name survives host-gone records")
	assert.True(t, discoveredAt.Equal(discovered.FetchedAt),
		"fetched_at survives host-gone records: got %v, want %v", discovered.FetchedAt, discoveredAt)

	created, err := repo.Get(ctx, newHost)
	require.NoError(t, err, "RecordHostGone creates the row for a host never discovered")
	assert.Equal(t, "", created.SoftwareName, "a row created by a host-gone record has no software name")
	assert.True(t, created.FetchedAt.IsZero(),
		"a row created by a host-gone record has no discovery time, so nodeinfo treats it as stale: got %v", created.FetchedAt)
}
