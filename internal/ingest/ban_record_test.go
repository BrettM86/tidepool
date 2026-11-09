package ingest

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/repo"
)

// COMMUNITY BANS BECOME BAN RECORDS.
//
// Every accepted community Block of a Person (a native persona or a minted
// bridged person) writes a social.coves.moderation.ban record into the
// community repo at the digest rkey of the subject DID; the matching
// Undo{Block} deletes it under the stale-unban rule. Coves enforces bans only
// from these records, so a ban held only in community_bans is a ban Coves never
// sees.
const (
	banRecordCollection = "social.coves.moderation.ban"
	// mbUnmintedSubjectMetric counts announced community Blocks and
	// Undo{Block}s naming a Lemmy user the bridge never minted.
	mbUnmintedSubjectMetric = "tidepool_block_unminted_subject"
)

// banRecord reads the ban record for subjectDID in communityDID's repo.
func banRecord(t *testing.T, h *harness, communityDID, subjectDID string) (map[string]any, string, bool) {
	t.Helper()
	record, recordCID, err := h.manager.GetRecord(context.Background(),
		communityDID, banRecordCollection, testDigestRKey(subjectDID))
	if errors.IsNotFound(err) {
		return nil, "", false
	}
	require.NoError(t, err, "read the ban record for %s in %s", subjectDID, communityDID)
	return record, recordCID, true
}

// banRecordCount counts every ban record in the community repo, whatever its rkey.
func banRecordCount(t *testing.T, h *harness, communityDID string) int {
	t.Helper()
	entries, err := h.manager.ListRecords(context.Background(), communityDID)
	require.NoError(t, err)
	n := 0
	for _, entry := range entries {
		if entry.Collection == banRecordCollection {
			n++
		}
	}
	return n
}

// repoHead is the community repo's head commit, as "cid@rev".
func repoHead(t *testing.T, h *harness, did string) string {
	t.Helper()
	headCID, rev, err := h.manager.Head(context.Background(), did)
	require.NoError(t, err)
	return headCID + "@" + rev
}

// assertBanRecordFields checks a ban record's subject, reason and expiresAt
// (empty reason / nil expiry: the field must be absent) and that createdAt is
// a datetime.
func assertBanRecordFields(t *testing.T, record map[string]any, subjectDID, wantReason string, wantExpiry *time.Time) {
	t.Helper()
	assert.Equal(t, banRecordCollection, record["$type"])
	assert.Equal(t, subjectDID, record["subject"], "the record names the banned account's DID")
	if wantReason == "" {
		assert.NotContains(t, record, "reason", "no summary: the reason field is omitted")
	} else {
		assert.Equal(t, wantReason, record["reason"], "the reason is the Block's summary")
	}
	if wantExpiry == nil {
		assert.NotContains(t, record, "expiresAt", "a permanent ban carries no expiresAt")
	} else {
		raw, _ := record["expiresAt"].(string)
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if assert.NoError(t, err, "expiresAt must be an RFC3339 datetime, got %q", raw) {
			assert.True(t, parsed.Equal(*wantExpiry),
				"expiresAt %s must be the Block's expiry %s", raw, wantExpiry.Format(time.RFC3339))
		}
	}
	createdAt, _ := record["createdAt"].(string)
	_, err := time.Parse(time.RFC3339Nano, createdAt)
	assert.NoError(t, err, "createdAt must be an RFC3339 datetime, got %q", createdAt)
}

// assertRetryable: the event is neither processed (nor skipped) nor poisoned,
// and was attempted.
func assertRetryable(t *testing.T, h *harness, activityID, why string) {
	t.Helper()
	event, err := h.events.GetEvent(context.Background(), activityID)
	require.NoError(t, err)
	assert.Nil(t, event.ProcessedAt, "%s: %s must stay retryable, not processed or skipped", why, activityID)
	assert.Nil(t, event.FailedAt, "%s: %s must not be poisoned: %s", why, activityID, event.Error)
	assert.Positive(t, event.Attempts, "%s: %s was attempted", why, activityID)
}

// retryNow makes a backed-off event due by the database clock and drains.
func retryNow(t *testing.T, h *harness, activityID string) {
	t.Helper()
	_, err := h.db.ExecContext(context.Background(),
		`UPDATE inbox_events SET next_attempt_at = now() WHERE activity_id = $1`, activityID)
	require.NoError(t, err)
	h.drain()
}

// futureExpiry and pastExpiry are whole-second instants relative to now, the
// precision an RFC3339 `expires` carries.
func futureExpiry(d time.Duration) time.Time { return time.Now().Add(d).UTC().Truncate(time.Second) }
func pastExpiry() time.Time                  { return time.Now().Add(-time.Hour).UTC().Truncate(time.Second) }

// banSubject is one bannable Person in one followed community, behind the
// announced Block / Undo{Block} entry path its kind arrives through.
type banSubject struct {
	h            *harness
	communityDID string
	subjectDID   string
	// block announces a Block with the given extra inner fields; undo announces
	// an Undo of the Block announced as blockActivityID, with extra fields on
	// the undone Block (its expiry, if any).
	block func(activityID string, extra map[string]any)
	undo  func(activityID, blockActivityID string, extra map[string]any)
	// native subjects also hold a community_bans row.
	native bool
}

// nativeBanSubject is a native persona in community A of the moderation world.
func nativeBanSubject(t *testing.T) banSubject {
	t.Helper()
	h := newHarness(t)
	world := newModerationWorld(t, h)
	return banSubject{
		h: h, communityDID: world.communityADID, subjectDID: mtAuthorDID, native: true,
		block: func(activityID string, extra map[string]any) {
			h.announceBlock(world.groupA, activityID, mtAuthorDID, groupID, extra)
		},
		undo: func(activityID, blockActivityID string, extra map[string]any) {
			h.announceUndoBlockWithExpiry(world.groupA, activityID, blockActivityID+"/block",
				mtAuthorDID, groupID, extra)
		},
	}
}

// bridgedBanSubject is a minted bridged person (U, who has posted) in
// community C. Blocks carry removeData false unless extra says otherwise, so
// the only community-repo write a Block can make is its ban record.
func bridgedBanSubject(t *testing.T) banSubject {
	t.Helper()
	world := newUpstreamBanWorld(t)
	h := world.h
	world.postBy(ubUserID, world.communityC, "br-ban-mint", ubBeforeBan, false)
	return banSubject{
		h: h, communityDID: ubCommunityCDID, subjectDID: ubUserDID,
		block: func(activityID string, extra map[string]any) {
			fields := map[string]any{"removeData": false, "summary": nil}
			for key, value := range extra {
				fields[key] = value
			}
			world.announceCommunityBlock(world.communityC, activityID, ubUserID, fields, time.Now())
		},
		undo: func(activityID, blockActivityID string, extra map[string]any) {
			inner := map[string]any{
				"id":         blockActivityID + "/block",
				"type":       "Block",
				"actor":      modActorID,
				"object":     ubUserID,
				"target":     world.communityC.id,
				"audience":   world.communityC.id,
				"removeData": false,
			}
			for key, value := range extra {
				inner[key] = value
			}
			world.deliverAt(world.communityC, map[string]any{
				"id":       activityID,
				"type":     "Announce",
				"actor":    world.communityC.id,
				"audience": world.communityC.id,
				"cc":       []any{world.communityC.id + "/followers"},
				"object": map[string]any{
					"id":       activityID + "/undo",
					"type":     "Undo",
					"actor":    modActorID,
					"audience": world.communityC.id,
					"cc":       []any{world.communityC.id},
					"object":   inner,
				},
			}, nil)
		},
	}
}

// TestACommunityBanOfANativePersonaWritesAndOverwritesItsBanRecord is B2.
func TestACommunityBanOfANativePersonaWritesAndOverwritesItsBanRecord(t *testing.T) {
	h := newHarness(t)
	world := newModerationWorld(t, h)

	// --- A timed ban with a reason.
	firstExpiry := futureExpiry(48 * time.Hour)
	h.announceBlock(world.groupA, "https://lemmy.world/activities/announce/block/br-b2-first",
		mtAuthorDID, groupID, map[string]any{
			"summary": "rule 2: spam", "expires": firstExpiry.Format(time.RFC3339),
		})

	ban, found := banFor(t, h.db, world.communityADID, mtAuthorDID)
	require.True(t, found, "community_bans holds the ban")
	assert.True(t, ban.expires.Valid && ban.expires.Time.Equal(firstExpiry), "with its expiry")

	record, firstCID, found := banRecord(t, h, world.communityADID, mtAuthorDID)
	require.True(t, found,
		"the community repo holds a ban record at the digest rkey of the subject DID: Coves "+
			"enforces bans only from this record")
	assertBanRecordFields(t, record, mtAuthorDID, "rule 2: spam", &firstExpiry)

	// --- A second Block for the same subject overwrites it.
	secondExpiry := futureExpiry(96 * time.Hour)
	h.announceBlock(world.groupA, "https://lemmy.world/activities/announce/block/br-b2-second",
		mtAuthorDID, groupID, map[string]any{
			"summary": "rule 4: harassment", "expires": secondExpiry.Format(time.RFC3339),
		})
	record, secondCID, found := banRecord(t, h, world.communityADID, mtAuthorDID)
	require.True(t, found, "the ban record still stands at the same rkey")
	assert.NotEqual(t, firstCID, secondCID, "the second Block rewrote the record")
	assertBanRecordFields(t, record, mtAuthorDID, "rule 4: harassment", &secondExpiry)

	// --- A third, permanent Block with no summary drops both optional fields.
	h.announceBlock(world.groupA, "https://lemmy.world/activities/announce/block/br-b2-third",
		mtAuthorDID, groupID, nil)
	record, _, found = banRecord(t, h, world.communityADID, mtAuthorDID)
	require.True(t, found, "the ban record still stands at the same rkey")
	assertBanRecordFields(t, record, mtAuthorDID, "", nil)

	assert.Equal(t, 1, banRecordCount(t, h, world.communityADID),
		"one ban record per subject: each Block overwrote the record at the subject's rkey")
}

// TestAnUndoBlockOfANativePersonaDeletesItsBanRecordUnlessStale is B3.
func TestAnUndoBlockOfANativePersonaDeletesItsBanRecordUnlessStale(t *testing.T) {
	standing := futureExpiry(96 * time.Hour)
	earlier := futureExpiry(48 * time.Hour)
	for _, tc := range []struct {
		name      string
		undone    map[string]any
		wantStays bool
	}{
		{name: "current Undo naming the standing expiry",
			undone: map[string]any{"expires": standing.Format(time.RFC3339)}, wantStays: false},
		{name: "Undo naming no expiry", undone: nil, wantStays: false},
		{name: "stale Undo naming an earlier expiry",
			undone: map[string]any{"expires": earlier.Format(time.RFC3339)}, wantStays: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			subject := nativeBanSubject(t)
			h := subject.h
			const blockID = "https://lemmy.world/activities/announce/block/br-b3"
			subject.block(blockID, map[string]any{
				"summary": "rule 2: spam", "expires": standing.Format(time.RFC3339),
			})
			_, standingCID, found := banRecord(t, h, subject.communityDID, subject.subjectDID)
			require.True(t, found, "precondition: the Block wrote the ban record")

			const undoID = "https://lemmy.world/activities/announce/undo/br-b3"
			subject.undo(undoID, blockID, tc.undone)
			assertProcessed(t, h, undoID)

			_, rowStands := banFor(t, h.db, subject.communityDID, subject.subjectDID)
			_, recordCID, recordStands := banRecord(t, h, subject.communityDID, subject.subjectDID)
			if tc.wantStays {
				assert.True(t, rowStands, "a stale Undo that Lift retains keeps the community_bans row")
				assert.True(t, recordStands, "and keeps the ban record")
				assert.Equal(t, standingCID, recordCID, "unchanged")
			} else {
				assert.False(t, rowStands, "the Undo lifts the community_bans row")
				assert.False(t, recordStands, "and deletes the ban record")
			}
		})
	}
}

// TestAFailedBanRecordCommitIsRetriedUntilItConverges is B4.
func TestAFailedBanRecordCommitIsRetriedUntilItConverges(t *testing.T) {
	for _, tc := range []struct {
		name    string
		subject func(*testing.T) banSubject
		undo    bool
	}{
		{name: "native current Undo{Block}", subject: nativeBanSubject, undo: true},
		{name: "native in-force Block", subject: nativeBanSubject},
		{name: "bridged in-force Block", subject: bridgedBanSubject},
		{name: "bridged current Undo{Block}", subject: bridgedBanSubject, undo: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			subject := tc.subject(t)
			h := subject.h
			const blockID = "https://lemmy.world/activities/announce/block/br-b4"
			const undoID = "https://lemmy.world/activities/announce/undo/br-b4"
			blockFields := map[string]any{
				"summary": "rule 2: spam", "expires": futureExpiry(48 * time.Hour).Format(time.RFC3339),
			}

			faulted := blockID
			if tc.undo {
				subject.block(blockID, blockFields)
				_, _, found := banRecord(t, h, subject.communityDID, subject.subjectDID)
				require.True(t, found, "precondition: the Block wrote the ban record")
				faulted = undoID
			}

			restore := rejectCommunityCommits(t, h, subject.communityDID)
			if tc.undo {
				subject.undo(undoID, blockID, nil)
			} else {
				subject.block(blockID, blockFields)
			}
			assertRetryable(t, h, faulted,
				"a ban record commit that failed is not a decision")

			restore()
			retryNow(t, h, faulted)
			assertProcessed(t, h, faulted)

			_, _, recordStands := banRecord(t, h, subject.communityDID, subject.subjectDID)
			if tc.undo {
				assert.False(t, recordStands, "the retry deletes the ban record")
			} else {
				assert.True(t, recordStands, "the retry writes the ban record")
			}
			if subject.native {
				_, rowStands := banFor(t, h.db, subject.communityDID, subject.subjectDID)
				assert.Equal(t, !tc.undo, rowStands, "the community_bans row matches the outcome")
			}

			if subject.native && tc.undo {
				// A further Undo under a fresh activity id, with row and record
				// both gone, commits nothing.
				head := repoHead(t, h, subject.communityDID)
				const againID = "https://lemmy.world/activities/announce/undo/br-b4-again"
				subject.undo(againID, blockID, nil)
				assertProcessed(t, h, againID)
				assert.Equal(t, head, repoHead(t, h, subject.communityDID),
					"an Undo with nothing left to delete makes no commit")
			}
		})
	}
}

// TestABlockThatLapsedBeforeArrivalWritesNoBanRecord is B7: a Block whose
// expiry already passed writes no ban record and leaves a standing one alone.
func TestABlockThatLapsedBeforeArrivalWritesNoBanRecord(t *testing.T) {
	for _, tc := range []struct {
		name    string
		subject func(*testing.T) banSubject
	}{
		{name: "native persona", subject: nativeBanSubject},
		{name: "minted bridged person", subject: bridgedBanSubject},
	} {
		t.Run(tc.name, func(t *testing.T) {
			subject := tc.subject(t)
			h := subject.h

			// (a) No prior record.
			subject.block("https://lemmy.world/activities/announce/block/br-b7-lapsed-1",
				map[string]any{"summary": "served", "expires": pastExpiry().Format(time.RFC3339)})
			_, _, found := banRecord(t, h, subject.communityDID, subject.subjectDID)
			assert.False(t, found, "a Block that lapsed before it arrived writes no ban record")

			// (b) A standing record from an in-force Block.
			subject.block("https://lemmy.world/activities/announce/block/br-b7-in-force",
				map[string]any{"summary": "rule 2: spam",
					"expires": futureExpiry(48 * time.Hour).Format(time.RFC3339)})
			_, standingCID, found := banRecord(t, h, subject.communityDID, subject.subjectDID)
			require.True(t, found, "precondition: the in-force Block wrote the ban record")

			subject.block("https://lemmy.world/activities/announce/block/br-b7-lapsed-2",
				map[string]any{"summary": "served", "expires": pastExpiry().Format(time.RFC3339)})
			_, recordCID, found := banRecord(t, h, subject.communityDID, subject.subjectDID)
			assert.True(t, found, "the standing ban record survives a lapsed Block")
			assert.Equal(t, standingCID, recordCID, "unchanged")
		})
	}
}

// TestABanOfAnUnmintedLemmyUserWritesNothing is B8.
func TestABanOfAnUnmintedLemmyUserWritesNothing(t *testing.T) {
	for _, tc := range []struct {
		name     string
		activity string
		deliver  func(world *upstreamBanWorld, activityID string)
	}{
		{name: "Block", activity: "https://lemmy.world/activities/announce/block/br-b8",
			deliver: func(world *upstreamBanWorld, activityID string) {
				world.announceCommunityBlock(world.communityC, activityID, ubUnknownID,
					map[string]any{"removeData": false}, time.Now())
			}},
		{name: "Undo{Block}", activity: "https://lemmy.world/activities/announce/undo/br-b8",
			deliver: func(world *upstreamBanWorld, activityID string) {
				world.announceCommunityUndoBlock(world.communityC, activityID,
					"https://lemmy.world/activities/announce/block/br-b8", ubUnknownID)
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			world := newUpstreamBanWorld(t)
			h := world.h
			// A real Lemmy Person the bridge can fetch, who never posted: no
			// bridged actor row.
			h.newRemoteActor(ubUnknownID, person(ubUnknownID, "NeverBridged", nil))
			head := repoHead(t, h, ubCommunityCDID)
			before := metricValue(mbUnmintedSubjectMetric)

			tc.deliver(world, tc.activity)

			assertProcessed(t, h, tc.activity)
			assert.Equal(t, head, repoHead(t, h, ubCommunityCDID),
				"nothing is written to the community repo for a user the bridge never minted")
			assert.Zero(t, bansIn(t, h.db, ubCommunityCDID), "and no community_bans row")
			assert.Equal(t, before+1, metricValue(mbUnmintedSubjectMetric),
				"the skip is counted as an unminted subject")
		})
	}
}

// mbUndoStaleRefusedMetric counts Undo{Block}s refused as stale: the standing
// ban outlasts the one the Undo names.
const mbUndoStaleRefusedMetric = "tidepool_block_undo_stale_refused"

// seedBanRecord writes a ban record straight into the community repo, the
// shape a live Block writes, for a standing ban no live Block can leave behind
// (one already lapsed) or that a test must not create through the code under
// test. It returns the record's CID.
func seedBanRecord(t *testing.T, h *harness, communityDID, subjectDID, reason string, expiresAt *time.Time) string {
	t.Helper()
	record := map[string]any{
		"$type":     banRecordCollection,
		"subject":   subjectDID,
		"createdAt": "2026-08-01T00:00:00.000Z",
	}
	if reason != "" {
		record["reason"] = reason
	}
	if expiresAt != nil {
		record["expiresAt"] = expiresAt.Format(time.RFC3339)
	}
	_, err := h.manager.ApplyOps(context.Background(), communityDID, []repo.RecordOp{{
		Action: repo.OpActionCreate, Collection: banRecordCollection,
		RKey: testDigestRKey(subjectDID), Record: record,
	}})
	require.NoError(t, err, "seed the ban record for %s in %s", subjectDID, communityDID)
	_, seededCID, found := banRecord(t, h, communityDID, subjectDID)
	require.True(t, found, "precondition: the seeded ban record stands")
	return seededCID
}

// TestAnUndoBlockOfABridgedPersonDeletesTheirBanRecordUnlessStale is B6: the
// stale-unban rule, applied to the ban record itself (a bridged person has no
// community_bans row). An Undo{Block} naming undone expiry T leaves a standing
// record that is still in force and is permanent or expires after T; an Undo
// naming no expiry lifts whatever stands.
func TestAnUndoBlockOfABridgedPersonDeletesTheirBanRecordUnlessStale(t *testing.T) {
	earlier := futureExpiry(48 * time.Hour)
	later := futureExpiry(96 * time.Hour)
	lapsed := pastExpiry()
	lapsedEarlier := lapsed.Add(-time.Hour)
	at := func(when time.Time) map[string]any { return map[string]any{"expires": when.Format(time.RFC3339)} }

	for _, tc := range []struct {
		name string
		// standing is the standing record's expiresAt (nil: permanent).
		standing *time.Time
		// undone is the undone Block's fields the Undo carries.
		undone    map[string]any
		wantStays bool
	}{
		{name: "Undo naming an expiry earlier than the standing one is stale",
			standing: &later, undone: at(earlier), wantStays: true},
		{name: "Undo naming the standing expiry",
			standing: &later, undone: at(later)},
		{name: "Undo naming no expiry lifts whatever stands",
			standing: &later, undone: nil},
		{name: "permanent standing record and an Undo naming an expiry is stale",
			standing: nil, undone: at(later), wantStays: true},
		{name: "standing expiry earlier than the undone one",
			standing: &earlier, undone: at(later)},
		{name: "standing record already lapsed and an Undo naming an earlier expiry",
			standing: &lapsed, undone: at(lapsedEarlier)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			subject := bridgedBanSubject(t)
			h := subject.h
			const blockID = "https://lemmy.world/activities/announce/block/br-b6"

			// Seeded, not written by a live Block: the stale rule for a bridged
			// person reads the standing record itself, and seeding keeps this
			// test on the Undo path alone.
			standingCID := seedBanRecord(t, h, subject.communityDID, subject.subjectDID, "rule 2: spam", tc.standing)
			staleBefore := metricValue(mbUndoStaleRefusedMetric)

			const undoID = "https://lemmy.world/activities/announce/undo/br-b6"
			subject.undo(undoID, blockID, tc.undone)
			assertProcessed(t, h, undoID)

			_, recordCID, recordStands := banRecord(t, h, subject.communityDID, subject.subjectDID)
			if tc.wantStays {
				assert.True(t, recordStands, "a stale Undo{Block} leaves the standing ban record")
				assert.Equal(t, standingCID, recordCID, "unchanged")
				assert.Equal(t, staleBefore+1, metricValue(mbUndoStaleRefusedMetric),
					"the refusal is counted as a stale Undo")
			} else {
				assert.False(t, recordStands, "the Undo{Block} deletes the ban record")
				assert.Equal(t, staleBefore, metricValue(mbUndoStaleRefusedMetric),
					"a current Undo is not counted as stale")
			}
		})
	}
}

// communityUndoBlockAnnounce builds the community's Announce{Undo{Block}} of
// subject; extra sets fields on the undone Block (a nil value deletes one).
func communityUndoBlockAnnounce(community *remoteActor, activityID, blockActivityID, subject string, extra map[string]any) map[string]any {
	undone := communityBlockAnnounce(community, blockActivityID, subject, extra)["object"]
	return map[string]any{
		"id":       activityID,
		"type":     "Announce",
		"actor":    community.id,
		"audience": community.id,
		"cc":       []any{community.id + "/followers"},
		"object": map[string]any{
			"id":       activityID + "/undo",
			"type":     "Undo",
			"actor":    modActorID,
			"audience": community.id,
			"cc":       []any{community.id},
			"object":   undone,
		},
	}
}

// TestADryRunBanReplayWritesNoBanRecord is B9, a guard: replay-bans stays
// unchanged by ban records, and its dry run neither writes nor deletes one.
func TestADryRunBanReplayWritesNoBanRecord(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	world.postBy(ubUserID, world.communityC, "br-b9-u", ubBeforeBan, false)
	world.postBy(ubOtherUserID, world.communityC, "br-b9-v", ubBeforeBan2, false)

	// U's ban record stands; the recorded history lifts it, and bans V.
	standingExpiry := futureExpiry(96 * time.Hour)
	standingCID := seedBanRecord(t, h, ubCommunityCDID, ubUserDID, "Spam", &standingExpiry)

	const (
		userBanID   = "https://lemmy.world/activities/announce/block/br-b9-u"
		userUnbanID = "https://lemmy.world/activities/announce/undo/br-b9-u"
		otherBanID  = "https://lemmy.world/activities/announce/block/br-b9-v"
	)
	expiry := map[string]any{"expires": standingExpiry.Format(time.RFC3339)}
	enqueueProcessed(t, h, world.communityC,
		communityBlockAnnounce(world.communityC, userBanID, ubUserID, expiry),
		time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	enqueueProcessed(t, h, world.communityC,
		communityUndoBlockAnnounce(world.communityC, userUnbanID, userBanID, ubUserID, expiry),
		time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC))
	enqueueProcessed(t, h, world.communityC,
		communityBlockAnnounce(world.communityC, otherBanID, ubOtherUserID, nil),
		time.Date(2026, 9, 3, 0, 0, 0, 0, time.UTC))
	head := repoHead(t, h, ubCommunityCDID)

	dryRun := h.adminRequest(http.MethodPost, replayBansPath, map[string]any{"dry_run": true})
	require.Equal(t, http.StatusOK, dryRun.Code, dryRun.Body.String())
	require.Contains(t, dryRun.Body.String(), otherBanID,
		"precondition: the dry run walked the recorded history and listed V's ban")

	_, recordCID, found := banRecord(t, h, ubCommunityCDID, ubUserDID)
	assert.True(t, found, "a dry run deletes no ban record")
	assert.Equal(t, standingCID, recordCID, "U's ban record is unchanged")
	_, _, found = banRecord(t, h, ubCommunityCDID, replayOtherUserDID)
	assert.False(t, found, "a dry run writes no ban record")
	assert.Equal(t, 1, banRecordCount(t, h, ubCommunityCDID), "no ban record appeared")
	assert.Equal(t, head, repoHead(t, h, ubCommunityCDID), "a dry run commits nothing to C's repo")
}

// mbExpiryUnreadableMetric counts Blocks refused because their expiry could
// not be parsed.
const mbExpiryUnreadableMetric = "tidepool_block_expiry_unreadable"

// TestABridgedBlockWithAnUnreadableExpiryRemovesContentButWritesNoBanRecord:
// a minted bridged person's Block whose `expires` cannot be parsed publishes
// no ban record (written without its expiry it would be a permanent ban the
// moderator did not ask for) and is refused as a validation error, yet its
// removeData still removes the person's earlier content in the community, as
// it did before ban records existed and as replay-bans does.
func TestABridgedBlockWithAnUnreadableExpiryRemovesContentButWritesNoBanRecord(t *testing.T) {
	for _, tc := range []struct {
		name       string
		removeData bool
	}{
		{name: "with removeData", removeData: true},
		{name: "without removeData", removeData: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			world := newUpstreamBanWorld(t)
			h := world.h
			postInC := world.postBy(ubUserID, world.communityC, "br-unreadable-c1", ubBeforeBan, false)
			commentInC := world.commentBy(ubUserID, world.communityC, "br-unreadable-c1-reply", postInC, ubBeforeBan2)
			require.True(t, acceptanceExists(t, h, ubCommunityCDID, postInC), "precondition: accepted in C")
			before := metricValue(mbExpiryUnreadableMetric)

			const banID = "https://lemmy.world/activities/announce/block/br-unreadable"
			world.announceCommunityBlock(world.communityC, banID, ubUserID,
				map[string]any{"expires": "next tuesday", "removeData": tc.removeData}, ubBanTime)

			event, err := h.events.GetEvent(context.Background(), banID)
			require.NoError(t, err)
			assert.Nil(t, event.ProcessedAt, "a Block whose expiry cannot be read is not marked handled")
			assert.NotNil(t, event.FailedAt,
				"it is refused as a validation error (poisoned), not retried: %s", event.Error)
			assert.Contains(t, event.Error, "expiry that cannot be parsed",
				"the refusal is the unreadable-expiry validation error")
			assert.Equal(t, before+1, metricValue(mbExpiryUnreadableMetric), "the refusal is counted")

			_, _, found := banRecord(t, h, ubCommunityCDID, ubUserDID)
			assert.False(t, found, "no ban record: written without its expiry it would be permanent")
			assert.Zero(t, banRecordCount(t, h, ubCommunityCDID), "no ban record for anyone")

			if tc.removeData {
				assert.False(t, acceptanceExists(t, h, ubCommunityCDID, postInC),
					"removeData still withdraws U's pre-ban post from C")
				assert.Equal(t, materialize.RemovalCodeAuthorBanned, removalCode(t, h, ubCommunityCDID, postInC),
					"and C records why: its author was banned")
				assert.Equal(t, materialize.RemovalCodeAuthorBanned, removalCode(t, h, ubCommunityCDID, commentInC),
					"and the pre-ban comment's removal")
			} else {
				assert.True(t, acceptanceExists(t, h, ubCommunityCDID, postInC), "nothing removed without removeData")
				assert.Empty(t, removalCode(t, h, ubCommunityCDID, postInC), "no removal record")
				assert.Empty(t, removalCode(t, h, ubCommunityCDID, commentInC), "no comment removal record")
			}
		})
	}
}
