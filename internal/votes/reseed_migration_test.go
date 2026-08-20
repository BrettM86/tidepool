package votes

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// MIGRATION 023's DATA STATEMENT.
//
// testutil.DB runs MigrateUp against an already-migrated database, so the
// migration's DELETE and recompute execute against nothing: they are
// "exercised" only in the sense that they parse. That is half of 17b's
// behaviour, and it is the half that touches rows production already has.
//
// The statement is READ FROM THE MIGRATION FILE rather than restated here. A
// copy would pass forever while the migration drifted underneath it — which is
// the failure mode this test exists to prevent, not one to reproduce.
const migrationPath = "../db/migrations/023_vote_seed_netting.sql"

// migrationCleanupStatement extracts the WITH-scrubbed statement from the Up
// section. It deliberately does NOT run the whole Up: CREATE INDEX would fail
// against an already-migrated database, and the index is not what is untested.
func migrationCleanupStatement(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(migrationPath))
	require.NoError(t, err, "migration 023 must be readable; this test tracks the file, not a copy")
	body := string(raw)
	up := body
	if down := strings.Index(body, "-- +goose Down"); down >= 0 {
		up = body[:down]
	}
	start := strings.Index(up, "WITH scrubbed")
	require.GreaterOrEqual(t, start, 0,
		"migration 023 must still carry a WITH-scrubbed cleanup statement; if it was renamed "+
			"or removed, this test is the place to find out")
	rest := up[start:]
	end := strings.Index(rest, ";")
	require.Greater(t, end, 0, "the cleanup statement must terminate")
	return rest[:end+1]
}

// TestMigration023RemovesOnlyOurOwnLegacyVoteRows drives the statement against
// rows of both kinds.
//
// A persona-authored vote_events row is unwritable since 17a's voter probe, so
// any surviving row predates the guard and is unconditionally garbage — but the
// recompute that follows it must re-derive the affected subjects' totals from
// what is LEFT, per direction, and must not touch any other subject: an
// unqualified recompute row-locks every aggregate and restamps updated_at,
// which migration 014's stats watermark reads as "due" and turns into a
// full re-emit sweep of record rewrites and firehose traffic.
func TestMigration023RemovesOnlyOurOwnLegacyVoteRows(t *testing.T) {
	database := testDB(t)
	ctx := context.Background()
	objects := store.NewAPObjects(database)
	statement := migrationCleanupStatement(t)

	// One of our personas, and a genuine Lemmy human.
	const personaActor = "https://coves.social/ap/actor/did:plc:legacypersona0001"
	_, err := store.NewAPActors(database).Create(ctx, store.APActor{
		DID:              "did:plc:legacypersona0001",
		Kind:             store.ActorTypePerson,
		ActorID:          personaActor,
		NormalizedOrigin: "coves.social",
		LocalPart:        "legacypersona",
		RSAKeySealed:     []byte{0x01, 0x02, 0x03},
		RSAKeyVersion:    1,
		PublicKeyPEM:     "-----BEGIN PUBLIC KEY-----\nMIIB\n-----END PUBLIC KEY-----\n",
	})
	require.NoError(t, err)

	// The AFFECTED subject: a genuine inbound up-vote, plus a legacy persona
	// DOWN-vote from before the guard existed.
	agg, _ := testAggregator(t, database)
	bridgeSubject(t, objects, subjectPost, "3lzmigsubject01")
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	insertLegacyVote(t, database, personaActor, subjectPost, directionDown)
	// The pre-guard world counted it, so the served totals include it.
	_, err = database.ExecContext(ctx, `
		UPDATE vote_aggregates SET seeded_upvotes = 4, seeded_downvotes = 2,
		    upvotes = 4 + 1, downvotes = 2 + 1 WHERE subject_ap_id = $1`, subjectPost)
	require.NoError(t, err)

	// The UNAFFECTED subject: genuine votes only. Nothing here may move.
	other := "https://lemmy.world/post/900"
	bridgeSubject(t, objects, other, "3lzmigsubject02")
	require.NoError(t, agg.ApplyVote(ctx, dislike(activityID(t, 2), voterBob, other), ""))
	otherStamp := updatedAt(t, database, other)
	otherUp, otherDown, _ := counts(t, database, other)

	// --- Run the migration's statement.
	_, err = database.ExecContext(ctx, statement)
	require.NoError(t, err, "the cleanup statement must run against real rows")

	assert.Zero(t, voteRowCount(t, database, personaActor),
		"a persona-authored vote row is unwritable today, so any that exists is garbage from "+
			"before the guard — it must not survive into the new accounting, where it would "+
			"be counted once as a live inbound vote AND once as a delivered outbound one")
	assert.Equal(t, 1, voteRowCount(t, database, voterAlice),
		"the genuine voter's row must be untouched: this DELETE is scoped to ap_actors, and "+
			"a wider predicate would erase the community's real votes")

	up, down, _ := counts(t, database, subjectPost)
	assert.Equal(t, 5, up, "served = seeded 4 + the ONE surviving live up-vote")
	assert.Equal(t, 2, down,
		"and the down column drops to its baseline: the only live down-vote was ours, and "+
			"the recompute re-derives per direction rather than subtracting a total")

	nowUp, nowDown, _ := counts(t, database, other)
	assert.Equal(t, otherUp, nowUp, "an unaffected subject's totals must not move")
	assert.Equal(t, otherDown, nowDown)
	assert.Equal(t, otherStamp, updatedAt(t, database, other),
		"nor its updated_at: the recompute is SCOPED to the subjects the DELETE touched, "+
			"because restamping every aggregate is what migration 014's stats watermark "+
			"reads as 'due' — a full re-emit sweep of the whole corpus")

	// --- Idempotence: a re-run (a redeployed migration, a manual re-apply)
	//     must be a true no-op, not a second subtraction.
	beforeUp, beforeDown, _ := counts(t, database, subjectPost)
	affectedStamp := updatedAt(t, database, subjectPost)
	_, err = database.ExecContext(ctx, statement)
	require.NoError(t, err)

	againUp, againDown, _ := counts(t, database, subjectPost)
	assert.Equal(t, beforeUp, againUp, "a second run changes nothing: there is nothing left to scrub")
	assert.Equal(t, beforeDown, againDown)
	assert.Equal(t, affectedStamp, updatedAt(t, database, subjectPost),
		"and it restamps nothing — an empty DELETE returns no subjects, so the recompute "+
			"matches no rows at all")
}

// insertLegacyVote writes a vote_events row directly: the voter probe refuses
// to create one for a persona, which is precisely why the migration exists.
func insertLegacyVote(t *testing.T, database *sql.DB, voter, subject, direction string) {
	t.Helper()
	_, err := database.ExecContext(context.Background(), `
		INSERT INTO vote_events (activity_id, voter_ap_id, subject_ap_id, direction)
		VALUES ($1, $2, $3, $4)`,
		"https://coves.social/ap/activity/legacy-"+direction, voter, subject, direction)
	require.NoError(t, err)
}

func voteRowCount(t *testing.T, database *sql.DB, voter string) int {
	t.Helper()
	var n int
	require.NoError(t, database.QueryRow(
		`SELECT COUNT(*) FROM vote_events WHERE voter_ap_id = $1`, voter).Scan(&n))
	return n
}

func updatedAt(t *testing.T, database *sql.DB, subject string) time.Time {
	t.Helper()
	var stamp time.Time
	require.NoError(t, database.QueryRow(
		`SELECT updated_at FROM vote_aggregates WHERE subject_ap_id = $1`, subject).Scan(&stamp))
	return stamp
}
