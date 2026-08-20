package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SECOND-OPINION (chunk 7, finding 5) — A BAN ROW IS NOT LAST-WRITER-WINS.
//
// Every ban write is an UPSERT on (community_did, subject_did), and the activity
// that carries it can arrive at any time in any order: Lemmy's Block is sent
// ONCE, but our own dead-letter redrive, a backfill replay and a delayed
// redelivery can all present an OLD Block again, under a new activity id that
// the inbox's dedup cannot recognize. Taking EXCLUDED.expires_at unconditionally
// makes the last message to arrive the current ban — so a lapsed three-day ban
// redriven a month later overwrites the PERMANENT ban the moderators issued in
// the meantime, Standing() reads unbanned from that moment on, and NOTHING ever
// re-applies it: the community already sent its Block, and no activity exists
// that says "that ban is still on".
//
// The rule these tests pin: an arriving ban that is NOT IN FORCE may never
// weaken one that IS. It is deliberately narrow — an arriving ban that IS in
// force is a moderator re-issuing, expiry included, and shortening a ban is a
// decision they are entitled to make.
const (
	cbrCommunityDID = "did:plc:cbrcommunity00001"
	cbrCommunityAP  = "https://lemmy.world/c/technology"
	cbrSubjectDID   = "did:plc:cbrsubject000001"
)

// storedBan is the row as the table holds it — the only place the difference
// between "recorded" and "in force" is visible.
type storedBan struct {
	communityAPID string
	expires       sql.NullTime
	reason        string
	removeData    bool
}

func storedBanFor(t *testing.T, database *sql.DB, communityDID, subjectDID string) (storedBan, bool) {
	t.Helper()
	var ban storedBan
	err := database.QueryRowContext(context.Background(), `
		SELECT community_ap_id, expires_at, reason, remove_data
		  FROM community_bans
		 WHERE community_did = $1 AND subject_did = $2`,
		communityDID, subjectDID).Scan(
		&ban.communityAPID, &ban.expires, &ban.reason, &ban.removeData)
	if err == sql.ErrNoRows {
		return storedBan{}, false
	}
	require.NoError(t, err, "read the ban row for %s in %s", subjectDID, communityDID)
	return ban, true
}

// TestCommunityBans_ALapsedReplayCannotWeakenAStandingBan is the finding itself.
//
// A temporary ban lapses; the moderators escalate to a permanent one; the OLD
// Block is redriven. Under last-writer-wins the permanent exclusion is gone —
// silently, and for good.
func TestCommunityBans_ALapsedReplayCannotWeakenAStandingBan(t *testing.T) {
	database := standingTestDB(t)
	repo := NewCommunityBans(database)
	ctx := context.Background()

	// The ban that stands: permanent, no expiry, the moderators' current ruling.
	_, err := repo.Ban(ctx, CommunityBan{
		CommunityDID:  cbrCommunityDID,
		SubjectDID:    cbrSubjectDID,
		CommunityAPID: cbrCommunityAP,
		Reason:        "repeated spam",
	})
	require.NoError(t, err)
	standing, err := repo.Standing(ctx, cbrCommunityDID, cbrSubjectDID)
	require.NoError(t, err)
	require.True(t, standing, "precondition: the permanent ban is in force")

	// The replay: the older, already-expired Block arriving again.
	lapsed := time.Now().Add(-72 * time.Hour)
	cancelled, err := repo.Ban(ctx, CommunityBan{
		CommunityDID:  cbrCommunityDID,
		SubjectDID:    cbrSubjectDID,
		CommunityAPID: cbrCommunityAP,
		ExpiresAt:     &lapsed,
		Reason:        "a three-day ban that ended before this arrived",
		RemoveData:    true,
	})
	require.NoError(t, err, "a stale replay is not an error: it is a message we decline to apply")
	assert.EqualValues(t, 0, cancelled,
		"and it cancels nothing, because it is not in force")

	standing, err = repo.Standing(ctx, cbrCommunityDID, cbrSubjectDID)
	require.NoError(t, err)
	assert.True(t, standing,
		"THE STANDING BAN SURVIVES. A lapsed Block that overwrote expires_at would end a "+
			"permanent exclusion the moderators never lifted — and nothing could ever restore "+
			"it, because Lemmy sent its Block exactly once and sends nothing at all when a "+
			"ban is still on")

	ban, found := storedBanFor(t, database, cbrCommunityDID, cbrSubjectDID)
	require.True(t, found, "the row is still there")
	assert.False(t, ban.expires.Valid,
		"with its PERMANENT expiry intact: a past timestamp here is the bug, whether or not "+
			"any reader has noticed yet")
	assert.Equal(t, "repeated spam", ban.reason,
		"and the standing ban's reason, not the stale one's: the same last-writer-wins that "+
			"moves the expiry rewrites the words an operator reads when asking why")
	assert.False(t, ban.removeData,
		"and its removeData: a replayed flag would misreport what was done to this author's "+
			"content under a ban that is not the one in force")
}

// TestCommunityBans_ALapsedBanIsStillRecordedWhenNothingStands keeps the guard
// honest in the other direction. A lapsed Block against a pair with NO standing
// ban is still written — it is a faithful account of what the moderators sent,
// and the row is what makes a redelivery idempotent. Only weakening is refused.
func TestCommunityBans_ALapsedBanIsStillRecordedWhenNothingStands(t *testing.T) {
	database := standingTestDB(t)
	repo := NewCommunityBans(database)
	ctx := context.Background()

	lapsed := time.Now().Add(-72 * time.Hour)
	cancelled, err := repo.Ban(ctx, CommunityBan{
		CommunityDID:  cbrCommunityDID,
		SubjectDID:    cbrSubjectDID,
		CommunityAPID: cbrCommunityAP,
		ExpiresAt:     &lapsed,
		Reason:        "three days, served",
	})
	require.NoError(t, err)
	assert.EqualValues(t, 0, cancelled, "a ban that is over cancels nothing")

	ban, found := storedBanFor(t, database, cbrCommunityDID, cbrSubjectDID)
	require.True(t, found,
		"the row IS written: the audit trail is the point, and without the row a redelivery "+
			"is not idempotent")
	require.True(t, ban.expires.Valid, "carrying the expiry it arrived with")
	assert.True(t, ban.expires.Time.Before(time.Now()), "in the past")

	standing, err := repo.Standing(ctx, cbrCommunityDID, cbrSubjectDID)
	require.NoError(t, err)
	assert.False(t, standing, "and it excludes nobody: the expiry IS the lift")

	// A second lapsed delivery of the same ban: still nothing in force, so the
	// guard must not fire — the row keeps taking the newest account of itself.
	_, err = repo.Ban(ctx, CommunityBan{
		CommunityDID:  cbrCommunityDID,
		SubjectDID:    cbrSubjectDID,
		CommunityAPID: cbrCommunityAP,
		ExpiresAt:     &lapsed,
		Reason:        "three days, served (redelivered)",
	})
	require.NoError(t, err)
	ban, _ = storedBanFor(t, database, cbrCommunityDID, cbrSubjectDID)
	assert.Equal(t, "three days, served (redelivered)", ban.reason,
		"a lapsed row is not protected from a lapsed write: nothing is in force, so there "+
			"is nothing to weaken")
}

// TestCommunityBans_AStaleUndoCannotLiftAStrongerBan is the same finding at the
// other door. Undo{Block} DELETES the row, so a redriven old unban lifts
// whatever is standing — including the permanent ban issued after it.
//
// The Undo carries no ban id we hold (Lemmy mints a fresh Block inside it), so
// the only thing to compare is the expiry it names: an Undo of a ban that ended
// at T is not an unban of a ban that outlives T.
func TestCommunityBans_AStaleUndoCannotLiftAStrongerBan(t *testing.T) {
	database := standingTestDB(t)
	repo := NewCommunityBans(database)
	ctx := context.Background()

	twoWeeks := time.Now().Add(14 * 24 * time.Hour)
	_, err := repo.Ban(ctx, CommunityBan{
		CommunityDID:  cbrCommunityDID,
		SubjectDID:    cbrSubjectDID,
		CommunityAPID: cbrCommunityAP,
		ExpiresAt:     &twoWeeks,
		Reason:        "two weeks",
	})
	require.NoError(t, err)

	// THE ORDINARY UNBAN STILL WORKS. This assertion is the guard's own guard:
	// a Lift that refused the matching Undo would leave every timed ban standing
	// after the moderators reversed it, which is the harm this finding is about
	// pointing the other way.
	lifted, retained, err := repo.Lift(ctx, cbrCommunityDID, cbrSubjectDID, &twoWeeks)
	require.NoError(t, err)
	require.True(t, lifted, "an Undo of the ban that IS standing lifts it")
	require.False(t, retained)

	// The moderators ban the author again, permanently.
	_, err = repo.Ban(ctx, CommunityBan{
		CommunityDID:  cbrCommunityDID,
		SubjectDID:    cbrSubjectDID,
		CommunityAPID: cbrCommunityAP,
		Reason:        "came back and did it again",
	})
	require.NoError(t, err)

	// The redrive: the SAME old Undo, arriving again.
	lifted, retained, err = repo.Lift(ctx, cbrCommunityDID, cbrSubjectDID, &twoWeeks)
	require.NoError(t, err, "a stale unban is not an error: it is a message we decline to apply")
	assert.False(t, lifted,
		"AN UNDO OF THE TWO-WEEK BAN CANNOT LIFT THE PERMANENT ONE: it reverses a decision "+
			"that is no longer the decision, and the delete is irreversible — Lemmy will send "+
			"no second Block to re-apply what this removed")
	assert.True(t, retained,
		"and the caller is TOLD the ban was kept: 'nothing to lift' and 'we refused to lift "+
			"this' are opposite findings for an operator, and only one of them means an author "+
			"may still be excluded here after a genuine unban")

	standing, err := repo.Standing(ctx, cbrCommunityDID, cbrSubjectDID)
	require.NoError(t, err)
	assert.True(t, standing, "the permanent ban still stands")

	// An Undo naming NO expiry is the documented residual: nothing in the row
	// distinguishes it from its own replay, so it lifts. Pinned so the gap is
	// visible rather than assumed closed.
	lifted, retained, err = repo.Lift(ctx, cbrCommunityDID, cbrSubjectDID, nil)
	require.NoError(t, err)
	assert.True(t, lifted,
		"an Undo of a PERMANENT ban carries nothing to compare, so it lifts — the residual "+
			"this guard cannot close without storing the ban's activity id or the moment the "+
			"Undo was first seen")
	assert.False(t, retained)

	// A row that has LAPSED is not protected: it excludes nobody, so a stale
	// Undo takes nothing away by removing it — and a guard that fired here would
	// warn an operator about an author who is not banned.
	longAgo := time.Now().Add(-30 * 24 * time.Hour)
	_, err = repo.Ban(ctx, CommunityBan{
		CommunityDID:  cbrCommunityDID,
		SubjectDID:    cbrSubjectDID,
		CommunityAPID: cbrCommunityAP,
		ExpiresAt:     &longAgo,
		Reason:        "served, a month ago",
	})
	require.NoError(t, err)
	evenLongerAgo := time.Now().Add(-60 * 24 * time.Hour)
	lifted, retained, err = repo.Lift(ctx, cbrCommunityDID, cbrSubjectDID, &evenLongerAgo)
	require.NoError(t, err)
	assert.True(t, lifted,
		"an Undo of an older ban still lifts a row that OUTLIVES it but has lapsed anyway: "+
			"what the guard protects is a ban IN FORCE, not a row — refusing here would hand "+
			"an operator a warning about an author nobody is excluding")
	assert.False(t, retained)

	// And nothing to lift at all is a third answer, distinct from both.
	lifted, retained, err = repo.Lift(ctx, cbrCommunityDID, cbrSubjectDID, nil)
	require.NoError(t, err)
	assert.False(t, lifted, "a re-delivered Undo with no ban left finds nothing")
	assert.False(t, retained, "and retains nothing, because there was nothing there")
}

// TestCommunityBans_AnInForceBanStillRewritesTheRow is the guard's blast-radius
// test: the ONLY thing refused is a lapsed write over a standing ban. Everything
// a moderator can genuinely re-issue must still land, including a re-issue that
// SHORTENS a permanent ban to a timed one — indistinguishable from a stale
// replay except that this one is still in force.
func TestCommunityBans_AnInForceBanStillRewritesTheRow(t *testing.T) {
	database := standingTestDB(t)
	repo := NewCommunityBans(database)
	ctx := context.Background()

	_, err := repo.Ban(ctx, CommunityBan{
		CommunityDID:  cbrCommunityDID,
		SubjectDID:    cbrSubjectDID,
		CommunityAPID: cbrCommunityAP,
		Reason:        "permanent, for now",
	})
	require.NoError(t, err)

	// Re-issued as a timed ban that has NOT run out.
	future := time.Now().Add(48 * time.Hour)
	_, err = repo.Ban(ctx, CommunityBan{
		CommunityDID:  cbrCommunityDID,
		SubjectDID:    cbrSubjectDID,
		CommunityAPID: cbrCommunityAP,
		ExpiresAt:     &future,
		Reason:        "reduced to two days on appeal",
	})
	require.NoError(t, err)

	ban, found := storedBanFor(t, database, cbrCommunityDID, cbrSubjectDID)
	require.True(t, found)
	require.True(t, ban.expires.Valid,
		"an IN-FORCE re-issue still takes effect: the guard is about bans that are OVER, and "+
			"a guard that also froze live re-issues would leave moderators unable to shorten "+
			"a ban they had already issued")
	assert.WithinDuration(t, future, ban.expires.Time, time.Second)
	assert.Equal(t, "reduced to two days on appeal", ban.reason)

	standing, err := repo.Standing(ctx, cbrCommunityDID, cbrSubjectDID)
	require.NoError(t, err)
	assert.True(t, standing, "and it is still a ban")

	// And the reverse order: a permanent ban over a row that has lapsed. The
	// escalation this whole finding is about must land.
	lapsed := time.Now().Add(-1 * time.Hour)
	_, err = repo.Ban(ctx, CommunityBan{
		CommunityDID:  cbrCommunityDID,
		SubjectDID:    "did:plc:cbrsubject000002",
		CommunityAPID: cbrCommunityAP,
		ExpiresAt:     &lapsed,
		Reason:        "served",
	})
	require.NoError(t, err)
	_, err = repo.Ban(ctx, CommunityBan{
		CommunityDID:  cbrCommunityDID,
		SubjectDID:    "did:plc:cbrsubject000002",
		CommunityAPID: cbrCommunityAP,
		Reason:        "escalated to permanent",
	})
	require.NoError(t, err)
	ban, found = storedBanFor(t, database, cbrCommunityDID, "did:plc:cbrsubject000002")
	require.True(t, found)
	assert.False(t, ban.expires.Valid,
		"a permanent ban over a lapsed row is the moderators escalating, and it must land — "+
			"this is the very sequence the replay guard exists to protect")
	assert.Equal(t, "escalated to permanent", ban.reason)
}
