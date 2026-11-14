package store

import (
	"context"
	"fmt"
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

	require.NoError(t, rig.items.RescheduleItem(ctx, first, 1, at(8, 0)))
	rescheduled := first
	rescheduled.Attempts = 1
	rescheduled.NextRetryAt = at(8, 0)
	assert.Equal(t, []DeferredItem{earlier, otherCommunity, rescheduled}, rig.dueItems(t, at(30, 0), 10),
		"RescheduleItem updates attempts and next_retry_at of that row only")

	require.NoError(t, rig.items.DeleteItem(ctx, first))
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

// TestDeferredBackfillItems_ListDueItemsPerHostCap: with 25 due rows on
// a.example across two communities and 3 on b.example, ListDueItems returns
// a.example's earliest 20 by (next_retry_at, object_iri, community_ap_id) and
// all of b.example's, merged in that same global order; a smaller limit still
// cuts the merged list.
func TestDeferredBackfillItems_ListDueItemsPerHostCap(t *testing.T) {
	rig := deferredTestDB(t)
	ctx := context.Background()
	minute := func(m int) time.Time { return time.Date(2026, 11, 4, 0, m, 0, 0, time.UTC) }
	// a is a.example's post/<object> in community, due at minute 12-object, so
	// post/12 is due first; both communities' rows of one object share it.
	a := func(object int, community, communityDID string) DeferredItem {
		return DeferredItem{
			ObjectIRI: fmt.Sprintf("https://a.example/post/%02d", object), CommunityAPID: community, CommunityDID: communityDID,
			Host: "a.example", ObjectType: "Page", CreatedAt: minute(0), NextRetryAt: minute(12 - object),
		}
	}
	technology := func(object int) DeferredItem { return a(object, deferredCommunity, deferredCommunityDID) }
	gaming := func(object int) DeferredItem { return a(object, deferredSecondCommunity, deferredSecondDID) }
	b := func(object, due int) DeferredItem {
		return DeferredItem{
			ObjectIRI: fmt.Sprintf("https://b.example/post/%02d", object), CommunityAPID: deferredCommunity, CommunityDID: deferredCommunityDID,
			Host: "b.example", ObjectType: "Page", CreatedAt: minute(0), NextRetryAt: minute(due),
		}
	}
	// a.example: post/00..post/12 in technology, post/00..post/11 in gaming.
	for object := 0; object <= 12; object++ {
		require.NoError(t, rig.items.DeferItem(ctx, technology(object)))
		if object <= 11 {
			require.NoError(t, rig.items.DeferItem(ctx, gaming(object)))
		}
	}
	for _, item := range []DeferredItem{b(3, 30), b(2, 5), b(1, 0)} {
		require.NoError(t, rig.items.DeferItem(ctx, item))
	}

	// "https://lemmy.world/c/gaming" sorts before ".../technology", and
	// a.example's IRIs before b.example's at the same minute.
	assert.Equal(t, []DeferredItem{
		technology(12), b(1, 0),
		gaming(11), technology(11),
		gaming(10), technology(10),
		gaming(9), technology(9),
		gaming(8), technology(8),
		gaming(7), technology(7), b(2, 5),
		gaming(6), technology(6),
		gaming(5), technology(5),
		gaming(4), technology(4),
		gaming(3), technology(3),
		gaming(2),
		b(3, 30),
	}, rig.dueItems(t, minute(59), 100), "a.example's earliest 20 and b.example's 3, in due order")
	assert.Equal(t, []DeferredItem{technology(12), b(1, 0), gaming(11), technology(11), gaming(10)},
		rig.dueItems(t, minute(59), 5), "the limit cuts the merged list")
}

// sweepMutations is the store's sweep-side mutations keyed by the swept item
// itself, so each is conditioned on the created_at the sweep read: a refresh
// (DeferInboxItem) gives the row a new created_at, its generation.
type sweepMutations interface {
	RescheduleItem(ctx context.Context, item DeferredItem, attempts int, nextRetryAt time.Time) error
	DeleteItem(ctx context.Context, item DeferredItem) error
	DeleteLandedItem(ctx context.Context, item DeferredItem, contentIRI string) error
}

// TestDeferredBackfillItems_StaleSweepMutationKeepsRefreshedRow (G1): a
// sweep lists a due row, an inbox deferral refreshes it (new created_at,
// attempts 0), and the sweep then applies its mutation with the item it
// read. The stale mutation succeeds without touching the refreshed row: it
// keeps the refresh's created_at, attempts 0 and next_retry_at.
func TestDeferredBackfillItems_StaleSweepMutationKeepsRefreshedRow(t *testing.T) {
	at := func(day, hour int) time.Time { return time.Date(2026, 11, day, hour, 0, 0, 0, time.UTC) }
	original := DeferredItem{
		ObjectIRI: "https://x.example/post/90", CommunityAPID: deferredCommunity, CommunityDID: deferredCommunityDID,
		Host: "x.example", ObjectType: "Page", Attempts: 3, CreatedAt: at(1, 0), NextRetryAt: at(4, 0),
	}
	refresh := original
	refresh.Attempts = 0
	refresh.CreatedAt = at(4, 1)
	refresh.NextRetryAt = at(4, 2)
	// The refresh keeps the earlier of the two schedules: the row's 11-04 00:00.
	wantRefreshed := DeferredItem{
		ObjectIRI: "https://x.example/post/90", CommunityAPID: deferredCommunity, CommunityDID: deferredCommunityDID,
		Host: "x.example", ObjectType: "Page", Attempts: 0, CreatedAt: at(4, 1), NextRetryAt: at(4, 0),
	}
	tests := []struct {
		name  string
		apply func(ctx context.Context, items sweepMutations, stale DeferredItem) error
	}{
		{
			name: "a stale DeleteItem",
			apply: func(ctx context.Context, items sweepMutations, stale DeferredItem) error {
				return items.DeleteItem(ctx, stale)
			},
		},
		{
			name: "a stale DeleteLandedItem",
			apply: func(ctx context.Context, items sweepMutations, stale DeferredItem) error {
				return items.DeleteLandedItem(ctx, stale, stale.ObjectIRI)
			},
		},
		{
			name: "a stale RescheduleItem",
			apply: func(ctx context.Context, items sweepMutations, stale DeferredItem) error {
				return items.RescheduleItem(ctx, stale, 4, at(5, 0))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rig := deferredTestDB(t)
			ctx := context.Background()
			require.NoError(t, rig.items.DeferItem(ctx, original))
			swept := rig.dueItems(t, at(4, 0), 10)
			require.Equal(t, []DeferredItem{original}, swept, "precondition: the sweep reads the original row")
			require.NoError(t, rig.items.DeferInboxItem(ctx, refresh))
			require.Equal(t, []DeferredItem{wantRefreshed}, rig.dueItems(t, at(30, 0), 10),
				"precondition: the inbox deferral refreshed the row")

			items, ok := any(rig.items).(sweepMutations)
			require.True(t, ok, "the sweep mutations take the swept item, so they can be conditioned on its created_at")
			assert.NoError(t, tt.apply(ctx, items, swept[0]), "a stale sweep mutation is not an error")

			assert.Equal(t, []DeferredItem{wantRefreshed}, rig.dueItems(t, at(30, 0), 10),
				"the refreshed row keeps its new created_at, attempts 0 and next_retry_at")
		})
	}
}
