package store

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/db"
	"tidepool/internal/testutil"
)

// TestMigrations_UpDownUp proves every migration applies cleanly up AND
// down. Packages sharing the postgres schema are serialized by
// testutil.DB's advisory lock, so tearing the schema down here cannot race
// another package's tests.
func TestMigrations_UpDownUp(t *testing.T) {
	database := testDB(t) // already migrated up by the harness
	ctx := context.Background()
	// Other packages' tests leave identity events behind, and 035's Down
	// refuses to drop them; the tables are torn down below anyway.
	testutil.Truncate(t, database, "firehose_events")
	// 050's Down refuses while an (actor, subject) pair holds two vote rows.
	testutil.Truncate(t, database, "outbound_votes")

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
		                     'outbound_objects', 'outbound_votes', 'federation_prefs',
		                     -- task 15 (migration 020): the outbound delivery queue.
		                     'outbound_activities', 'outbound_deliveries',
		                     -- migration 042: per-host nodeinfo discovery results.
		                     'instances')
	`).Scan(&remaining)
	require.NoError(t, err)
	assert.Zero(t, remaining, "down migrations must drop every Tidepool table")

	require.NoError(t, db.MigrateUp(ctx, database), "re-applying up migrations must succeed")

	// The down list above only bites if the tables were there to begin with:
	// a migration whose Down forgets a table is caught only when its Up
	// created one. Assert the newest tables exist after the re-up so the two
	// halves stay in step.
	for _, tc := range []struct {
		table     string
		migration string
	}{
		{"consumer_cursors", "018"},
		{"jetstream_record_revs", "018"},
		{"jetstream_dead_letters", "018"},
		{"outbound_objects", "018"},
		{"outbound_votes", "018"},
		{"federation_prefs", "018"},
		{"outbound_activities", "020"},
		{"outbound_deliveries", "020"},
		{"instances", "042"},
	} {
		var exists bool
		err = database.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM information_schema.tables
				WHERE table_schema = 'public' AND table_name = $1
			)`, tc.table).Scan(&exists)
		require.NoError(t, err)
		assert.True(t, exists, "migration %s must create %q", tc.migration, tc.table)
	}

	// The causal-gating marker migration 020 ALTERs onto outbound_objects:
	// NULL accepted_at is what keeps a bridge-origin child ineligible until its
	// parent lands.
	var acceptedAtExists bool
	err = database.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'public'
			  AND table_name = 'outbound_objects'
			  AND column_name = 'accepted_at'
		)`).Scan(&acceptedAtExists)
	require.NoError(t, err)
	assert.True(t, acceptedAtExists, "migration 020 must add outbound_objects.accepted_at")

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

		// Task 15 (migration 020). The activity id is globally unique (one
		// canonical payload fans out to many inboxes); the delivery PK is the
		// (activity, inbox) fan-out key; the partial queue index is the
		// loose-index-scan support the generalized ClaimNext depends on.
		"outbound_activities_pkey",
		"outbound_deliveries_pkey",
		"idx_outbound_deliveries_queue",
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

// TestMigration035_DownRefusesWithIdentityEvents pins that rolling 035 back
// never discards relay-visible identity events: restoring 011's two-kind
// CHECK is only lossless with no identity rows, so Down must refuse (and its
// transaction leave the 035 schema intact) rather than delete them.
func TestMigration035_DownRefusesWithIdentityEvents(t *testing.T) {
	database := testDB(t) // already migrated up by the harness
	ctx := context.Background()

	var seq int64
	err := database.QueryRowContext(ctx, `
		INSERT INTO firehose_events (kind, did, identity_handle, created_at)
		VALUES ('identity', $1, 'alice.lemmy.world.ap.tidepool.test', NOW())
		RETURNING seq`, testDID).Scan(&seq)
	require.NoError(t, err)
	t.Cleanup(func() {
		// Later tests share this database: drop the row and make sure the
		// schema is back at the latest version whatever happened above.
		_, err := database.ExecContext(ctx, `DELETE FROM firehose_events WHERE seq = $1`, seq)
		assert.NoError(t, err)
		assert.NoError(t, db.MigrateUp(ctx, database))
	})

	// 035 is the newest migration, so DownTo(34) runs exactly its Down.
	require.Error(t, db.MigrateDownTo(ctx, database, 34),
		"035 Down must refuse while identity events exist")

	var handle string
	err = database.QueryRowContext(ctx,
		`SELECT identity_handle FROM firehose_events WHERE seq = $1 AND kind = 'identity'`, seq).Scan(&handle)
	require.NoError(t, err, "the identity event must survive the refused Down")
	assert.Equal(t, "alice.lemmy.world.ap.tidepool.test", handle)

	var version int64
	err = database.QueryRowContext(ctx, `
		SELECT version_id FROM goose_db_version
		WHERE is_applied ORDER BY id DESC LIMIT 1`).Scan(&version)
	require.NoError(t, err)
	assert.Equal(t, int64(35), version, "the refused Down must leave the schema at 035")
}

// migration050Vote inserts one outbound_votes row by raw SQL (the store layer
// may not match the schema version under test).
func migration050Vote(t *testing.T, database *sql.DB, voteATURI, subjectATURI, activityID, state string) {
	t.Helper()
	_, err := database.ExecContext(context.Background(), `
		INSERT INTO outbound_votes (vote_at_uri, actor_did, subject_at_uri, subject_ap_id,
			community_did, direction, current_activity_id, delivered_state)
		VALUES ($1, $2, $3, 'https://lemmy.world/post/1', $4, 'up', $5, $6)`,
		voteATURI, testDID, subjectATURI, testCommunityDID, activityID, state)
	require.NoError(t, err)
}

func migration050Undo(t *testing.T, database *sql.DB, undoID, innerID, createdAt string) {
	t.Helper()
	_, err := database.ExecContext(context.Background(), `
		INSERT INTO outbound_activities (activity_id, actor_did, kind, payload, created_at)
		VALUES ($1, $2, 'Undo', jsonb_build_object('type', 'Undo', 'object', jsonb_build_object('id', $3::text)), $4)`,
		undoID, testDID, innerID, createdAt)
	require.NoError(t, err)
}

func retractionOf(t *testing.T, database *sql.DB, voteATURI string) sql.NullString {
	t.Helper()
	var got sql.NullString
	require.NoError(t, database.QueryRowContext(context.Background(),
		`SELECT retraction_activity_id FROM outbound_votes WHERE vote_at_uri = $1`, voteATURI).Scan(&got))
	return got
}

// migration050Fixture clears the vote tables and restores the latest schema
// afterwards, whatever the test did to it.
func migration050Fixture(t *testing.T) *sql.DB {
	t.Helper()
	database := testDB(t)
	testutil.Truncate(t, database, "outbound_votes", "outbound_activities")
	t.Cleanup(func() {
		ctx := context.Background()
		_, err := database.ExecContext(ctx, `TRUNCATE outbound_votes, outbound_activities CASCADE`)
		assert.NoError(t, err)
		assert.NoError(t, db.MigrateUp(ctx, database))
	})
	return database
}

const (
	m050SubjectA    = "at://" + testCommunityDID + "/social.coves.community.postv2/3lzm050subja"
	m050SubjectB    = "at://" + testCommunityDID + "/social.coves.community.postv2/3lzm050subjb"
	m050SubjectC    = "at://" + testCommunityDID + "/social.coves.community.postv2/3lzm050subjc"
	m050SubjectD    = "at://" + testCommunityDID + "/social.coves.community.postv2/3lzm050subjd"
	m050VoteA       = "at://" + testDID + "/social.coves.feed.vote/3lzm050votea"
	m050VoteA2      = "at://" + testDID + "/social.coves.feed.vote/3lzm050votea2"
	m050VoteLive    = "at://" + testDID + "/social.coves.feed.vote/3lzm050livex"
	m050VoteUndone  = "at://" + testDID + "/social.coves.feed.vote/3lzm050undne"
	m050VotePending = "at://" + testDID + "/social.coves.feed.vote/3lzm050pendg"
	m050LikeA       = "https://coves.social/ap/activity/m050-like-a"
	m050LikeLive    = "https://coves.social/ap/activity/m050-like-live"
	m050LikeUndone  = "https://coves.social/ap/activity/m050-like-undone"
	m050LikePending = "https://coves.social/ap/activity/m050-like-pending"
	m050UndoOld     = "https://coves.social/ap/activity/m050-undo-old"
	m050UndoNew     = "https://coves.social/ap/activity/m050-undo-new"
	m050UndoUndone  = "https://coves.social/ap/activity/m050-undo-undone"
	m050UndoPending = "https://coves.social/ap/activity/m050-undo-pending"
)

// TestMigration050_BackfillsRetractionFromQueuedUndo: a row already withdrawn
// by a queued Undo when 050 lands must come up retracting, so the user can
// vote on that subject again.
func TestMigration050_BackfillsRetractionFromQueuedUndo(t *testing.T) {
	database := migration050Fixture(t)
	ctx := context.Background()

	require.NoError(t, db.MigrateDownTo(ctx, database, 49), "roll back exactly 050")

	migration050Vote(t, database, m050VoteA, m050SubjectA, m050LikeA, "delivered")
	migration050Undo(t, database, m050UndoOld, m050LikeA, "2026-01-01T00:00:00Z")
	migration050Undo(t, database, m050UndoNew, m050LikeA, "2026-01-02T00:00:00Z")
	migration050Vote(t, database, m050VoteLive, m050SubjectB, m050LikeLive, "delivered")
	migration050Vote(t, database, m050VoteUndone, m050SubjectC, m050LikeUndone, "undone")
	migration050Undo(t, database, m050UndoUndone, m050LikeUndone, "2026-01-01T00:00:00Z")
	// The delete landed before the Like was delivered: the row is still
	// pending, and it must come up retracting too.
	migration050Vote(t, database, m050VotePending, m050SubjectD, m050LikePending, "pending")
	migration050Undo(t, database, m050UndoPending, m050LikePending, "2026-01-01T00:00:00Z")

	require.NoError(t, db.MigrateUp(ctx, database))

	assert.Equal(t, sql.NullString{String: m050UndoNew, Valid: true}, retractionOf(t, database, m050VoteA),
		"latest Undo backfilled")
	assert.False(t, retractionOf(t, database, m050VoteLive).Valid, "unrelated row stays live")
	assert.False(t, retractionOf(t, database, m050VoteUndone).Valid, "undone row not backfilled")
	assert.Equal(t, sql.NullString{String: m050UndoPending, Valid: true}, retractionOf(t, database, m050VotePending),
		"pending row with a queued Undo backfilled")

	_, err := database.ExecContext(ctx, `
		INSERT INTO outbound_votes (vote_at_uri, actor_did, subject_at_uri, subject_ap_id,
			community_did, direction, current_activity_id)
		VALUES ($1, $2, $3, 'https://lemmy.world/post/1', $4, 'down', 'https://coves.social/ap/activity/m050-b')`,
		m050VoteA2, testDID, m050SubjectA, testCommunityDID)
	assert.NoError(t, err, "a new vote on A's subject is allowed")
}

// TestMigration050_DownRefusesWithTwoRowsPerPair: restoring the full unique
// constraint would have to drop a vote, so Down must refuse instead.
func TestMigration050_DownRefusesWithTwoRowsPerPair(t *testing.T) {
	database := migration050Fixture(t)
	ctx := context.Background()

	migration050Vote(t, database, m050VoteA, m050SubjectA, m050LikeA, "delivered")
	_, err := database.ExecContext(ctx,
		`UPDATE outbound_votes SET retraction_activity_id = $2 WHERE vote_at_uri = $1`, m050VoteA, m050UndoNew)
	require.NoError(t, err)
	migration050Vote(t, database, m050VoteA2, m050SubjectA, m050LikeLive, "pending")

	require.Error(t, db.MigrateDownTo(ctx, database, 49), "050 Down must refuse")

	var n int
	require.NoError(t, database.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM outbound_votes WHERE vote_at_uri IN ($1, $2)`, m050VoteA, m050VoteA2).Scan(&n))
	assert.Equal(t, 2, n, "both rows survive")
}
