package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/db"
)

// TestMigrations_UpDownUp proves every migration applies cleanly up AND
// down. It lives in this package (not internal/db) on purpose: `go test
// ./...` runs packages in parallel, and all tests sharing the postgres
// schema must stay in one package so they run sequentially.
func TestMigrations_UpDownUp(t *testing.T) {
	database := testDB(t) // already migrated up by the harness
	ctx := context.Background()

	require.NoError(t, db.MigrateDownTo(ctx, database, 0), "all down migrations must apply")

	var remaining int
	err := database.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = 'public'
		  AND table_name IN ('ap_objects', 'bridged_actors', 'communities', 'inbox_events')
	`).Scan(&remaining)
	require.NoError(t, err)
	assert.Zero(t, remaining, "down migrations must drop every Tidepool table")

	require.NoError(t, db.MigrateUp(ctx, database), "re-applying up migrations must succeed")

	// Leave the schema usable and prove it is: exercise a write.
	repo := NewAPObjects(database)
	_, err = repo.PutMapping(ctx, testMapping())
	require.NoError(t, err)
}

// TestMigrations_UniqueConstraintNames pins the explicit constraint and
// index names the store layer's uniqueViolation mapping depends on. If a
// migration renames one, the 23505 → ConflictError mapping silently
// degrades to wrapped internal errors — this test makes that loud.
func TestMigrations_UniqueConstraintNames(t *testing.T) {
	database := testDB(t)
	ctx := context.Background()

	expected := []string{
		"ap_objects_ap_id_key",
		"ap_objects_at_uri_key",
		"bridged_actors_ap_actor_id_key",
		"bridged_actors_did_key",
		"bridged_actors_handle_key", // partial unique index, not a table constraint
		"communities_ap_group_id_key",
		"communities_did_key",
		"inbox_events_activity_id_key",
	}
	for _, name := range expected {
		var exists bool
		err := database.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_indexes
				WHERE schemaname = 'public' AND indexname = $1
			)`, name).Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "unique constraint/index %q must exist with exactly this name", name)
	}
}
