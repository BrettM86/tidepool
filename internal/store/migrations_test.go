package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/db"
)

// TestMigrations_UpDownUp proves every migration applies cleanly up AND
// down. Packages sharing the postgres schema are serialized by
// testutil.DB's advisory lock, so tearing the schema down here cannot race
// another package's tests.
func TestMigrations_UpDownUp(t *testing.T) {
	database := testDB(t) // already migrated up by the harness
	ctx := context.Background()

	require.NoError(t, db.MigrateDownTo(ctx, database, 0), "all down migrations must apply")

	var remaining int
	err := database.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = 'public'
		  AND table_name IN ('ap_objects', 'ap_actors', 'bridged_actors', 'communities', 'inbox_events', 'service_keys',
		                     'blocks', 'repo_state', 'firehose_events', 'vote_aggregates', 'vote_events',
		                     -- task 14 (migration 018): the consumer's own state and the
		                     -- outbound state deletes are rebuilt from.
		                     'consumer_cursors', 'jetstream_record_revs', 'jetstream_dead_letters',
		                     'outbound_objects', 'outbound_votes', 'federation_prefs')
	`).Scan(&remaining)
	require.NoError(t, err)
	assert.Zero(t, remaining, "down migrations must drop every Tidepool table")

	require.NoError(t, db.MigrateUp(ctx, database), "re-applying up migrations must succeed")

	// The down list above only bites if the tables were there to begin with:
	// a migration whose Down forgets a table is caught only when its Up
	// created one. Assert the newest tables exist after the re-up so the two
	// halves stay in step.
	for _, table := range []string{
		"consumer_cursors", "jetstream_record_revs", "jetstream_dead_letters",
		"outbound_objects", "outbound_votes", "federation_prefs",
	} {
		var exists bool
		err = database.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM information_schema.tables
				WHERE table_schema = 'public' AND table_name = $1
			)`, table).Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "migration 018 must create %q", table)
	}

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
		// ap_actors (task 13). The composite name is EXPLICIT: postgres
		// would default it to ap_actors_normalized_origin_local_part_key,
		// so the migration must name the constraint itself.
		"ap_actors_pkey",
		"ap_actors_actor_id_key",
		"ap_actors_origin_local_part_key",
		"ap_objects_ap_id_key",
		"ap_objects_at_uri_key",
		"bridged_actors_ap_actor_id_key",
		"bridged_actors_did_key",
		"bridged_actors_handle_key", // partial unique index, not a table constraint
		"communities_ap_group_id_key",
		"communities_did_key",
		"inbox_events_activity_id_key",
		"service_keys_name_key",
		"vote_events_activity_id_key", // the vote dedupe key (task 07)

		// Task 14 (migration 018). The composite cursor key is the whole
		// point of consumer_cursors: (consumer_name, schema_version), so a
		// future incompatible handler replays without stomping production.
		"consumer_cursors_pkey",
		"jetstream_record_revs_pkey",
		// The dead-letter dedup index — same name as the Coves original this
		// is ported from. Without it a poison frame replayed by the reconnect
		// rewind grows a fresh row per pass instead of being absorbed, and
		// AddDeadLetter stops being the no-op success that lets the cursor
		// advance past it.
		"idx_jetstream_dead_letters_dedup",
		"outbound_objects_pkey",
		// outbound_votes is keyed by the VOTE record's at-uri, because that
		// is the only thing a vote delete commit carries. The (actor,
		// subject) pair is a second, EXPLICITLY NAMED unique constraint: the
		// store's 23505 → ConflictError mapping switches on the name.
		"outbound_votes_pkey",
		"outbound_votes_actor_subject_key",
		"federation_prefs_pkey",
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
