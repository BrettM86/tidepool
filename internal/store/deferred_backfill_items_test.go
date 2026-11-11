package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/testutil"
)

const (
	deferredCommunity       = "https://lemmy.world/c/technology"
	deferredCommunityDID    = "did:plc:deferredtechnology00000"
	deferredSecondCommunity = "https://lemmy.world/c/gaming"
	deferredSecondDID       = "did:plc:deferredgaming0000000000"
)

// deferredTestDB returns the test database with the deferred-item and
// instances tables cleared (backfill_deferred_items has no foreign key, so
// no other truncation cascades into it).
func deferredTestDB(t *testing.T) *deferredItemsRig {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database, "instances", "backfill_deferred_items")
	return &deferredItemsRig{
		items:     NewDeferredBackfillItems(database),
		instances: NewInstances(database),
	}
}

// deferredItemsRig holds the stores one deferred-item test drives.
type deferredItemsRig struct {
	items     DeferredBackfillItems
	instances Instances
}

// dueItems lists the items due at now, with their times in UTC.
func (r *deferredItemsRig) dueItems(t *testing.T, now time.Time, limit int) []DeferredItem {
	t.Helper()
	items, err := r.items.ListDueItems(context.Background(), now, limit)
	require.NoError(t, err)
	normalized := []DeferredItem{}
	for _, item := range items {
		item.NextRetryAt = item.NextRetryAt.UTC()
		item.CreatedAt = item.CreatedAt.UTC()
		normalized = append(normalized, item)
	}
	return normalized
}

// TestDeferredBackfillItems pins the deferred-item store: a second deferral
// of the same object in the same community keeps the first one's schedule,
// the same object in another community is its own row, ListDueItems returns
// only due rows, oldest due first, up to its limit, RescheduleItem moves one
// row, and DeleteItem removes only that row. The steps run in order against
// one database.
func TestDeferredBackfillItems(t *testing.T) {
	rig := deferredTestDB(t)
	ctx := context.Background()
	at := func(day, hour int) time.Time { return time.Date(2026, 11, day, hour, 0, 0, 0, time.UTC) }

	first := DeferredItem{
		ObjectIRI: "https://x.example/post/1", CommunityAPID: deferredCommunity, CommunityDID: deferredCommunityDID,
		Host: "x.example", ObjectType: "Page", CreatedAt: at(1, 0), NextRetryAt: at(4, 0),
	}
	redeferred := first
	redeferred.CreatedAt = at(1, 1)
	redeferred.NextRetryAt = at(4, 1)
	otherCommunity := DeferredItem{
		ObjectIRI: "https://x.example/post/1", CommunityAPID: deferredSecondCommunity, CommunityDID: deferredSecondDID,
		Host: "x.example", ObjectType: "Page", CreatedAt: at(1, 2), NextRetryAt: at(4, 2),
	}
	earlier := DeferredItem{
		ObjectIRI: "https://y.example/comment/2", CommunityAPID: deferredCommunity, CommunityDID: deferredCommunityDID,
		Host: "y.example", ObjectType: "Note", CreatedAt: at(1, 0), NextRetryAt: at(1, 10),
	}
	for _, item := range []DeferredItem{first, redeferred, otherCommunity, earlier} {
		require.NoError(t, rig.items.DeferItem(ctx, item))
	}

	assert.Equal(t, []DeferredItem{earlier, first}, rig.dueItems(t, at(4, 0), 10),
		"due at 11-04 00:00: the 10:00 row then the first deferral's schedule, not the redeferral's or the other community's")
	assert.Equal(t, []DeferredItem{earlier, first}, rig.dueItems(t, at(5, 0), 2),
		"the limit cuts the oldest-due-first list")
	assert.Equal(t, []DeferredItem{earlier, first, otherCommunity}, rig.dueItems(t, at(5, 0), 10),
		"the same object in another community is its own row")
	assert.Empty(t, rig.dueItems(t, at(1, 9), 10), "nothing is due before the earliest next_retry_at")

	require.NoError(t, rig.items.RescheduleItem(ctx, first.ObjectIRI, deferredCommunity, 1, at(8, 0)))
	rescheduled := first
	rescheduled.Attempts = 1
	rescheduled.NextRetryAt = at(8, 0)
	assert.Equal(t, []DeferredItem{earlier, otherCommunity, rescheduled}, rig.dueItems(t, at(30, 0), 10),
		"RescheduleItem updates attempts and next_retry_at of that row only")

	require.NoError(t, rig.items.DeleteItem(ctx, first.ObjectIRI, deferredCommunity))
	assert.Equal(t, []DeferredItem{earlier, otherCommunity}, rig.dueItems(t, at(30, 0), 10),
		"DeleteItem removes that row only")
}

// TestInstances_ClearHostGoneMakesDeferredItemsDue: clearing a host's
// host-gone streak makes every item deferred on it due at once (next_retry_at
// pulled back to its created_at). ClearHostGone on a host with no streak
// leaves its deferred rows' schedule alone, and another host's rows keep
// their schedule.
func TestInstances_ClearHostGoneMakesDeferredItemsDue(t *testing.T) {
	rig := deferredTestDB(t)
	ctx := context.Background()
	at := func(day, hour int) time.Time { return time.Date(2026, 11, day, hour, 0, 0, 0, time.UTC) }
	row := func(iri, host string, created, next time.Time) DeferredItem {
		return DeferredItem{
			ObjectIRI: iri, CommunityAPID: deferredCommunity, CommunityDID: deferredCommunityDID,
			Host: host, ObjectType: "Page", CreatedAt: created, NextRetryAt: next,
		}
	}
	recovered1 := row("https://x.example/post/1", "x.example", at(1, 0), at(4, 0))
	recovered2 := row("https://x.example/post/2", "x.example", at(1, 2), at(4, 2))
	noStreak := row("https://z.example/post/3", "z.example", at(1, 1), at(4, 1))
	stillGone := row("https://y.example/post/4", "y.example", at(1, 0), at(4, 0))
	for _, item := range []DeferredItem{recovered1, recovered2, noStreak, stillGone} {
		require.NoError(t, rig.items.DeferItem(ctx, item))
	}
	for _, host := range []string{"x.example", "y.example"} {
		_, err := rig.instances.RecordHostGone(ctx, host, at(1, 0))
		require.NoError(t, err)
	}
	// z.example has an instances row but no host-gone streak.
	require.NoError(t, rig.instances.RecordSoftware(ctx, "z.example", "lemmy", at(1, 0)))

	require.NoError(t, rig.instances.ClearHostGone(ctx, "x.example"))
	require.NoError(t, rig.instances.ClearHostGone(ctx, "z.example"))

	pulled := func(item DeferredItem) DeferredItem {
		item.NextRetryAt = item.CreatedAt
		return item
	}
	assert.Equal(t, []DeferredItem{pulled(recovered1), pulled(recovered2)}, rig.dueItems(t, at(1, 3), 10),
		"x.example's rows are due from their created_at; z.example's (no streak) and y.example's are not")
	assert.Equal(t, []DeferredItem{pulled(recovered1), pulled(recovered2), stillGone, noStreak}, rig.dueItems(t, at(4, 1), 10),
		"z.example's row keeps its 11-04 01:00 schedule and y.example's its 11-04 00:00 schedule")

	// The streak itself was cleared: x.example's next failure, 4h after the
	// cleared streak began (within the 48h gap), starts a new streak.
	since, err := rig.instances.RecordHostGone(ctx, "x.example", at(1, 4))
	require.NoError(t, err)
	assert.True(t, at(1, 4).Equal(since), "x.example's streak was cleared: goneSince = %v, want %v", since, at(1, 4))
}
