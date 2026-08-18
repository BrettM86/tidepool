package ingest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/accept"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
)

// SECOND-OPINION (chunk 7, top test gap) — THE OTHER SPELLING OF AN EXPIRY.
//
// Lemmy 0.19 spells a Block's expiry `expires`; newer versions spell the same
// fact with AS2's `endTime`. Every ban test in moderation_ban_test.go sends
// only `expires`, so before this file the entire endTime read (`return
// o.EndTime` in ap.Object.BanExpiry) could be deleted with the suite green —
// and the failure it re-opens is the one BanExpiry's own comment warns about:
// every timed ban from newer Lemmy reads as permanent, Lemmy sends NO Undo
// when a timed ban lapses, so nothing ever lifts it. A version upgrade on the
// far side silently converts moderation the moderators time-boxed into forever.
//
// The two halves here mirror the existing `expires` pair (lapsed is inert /
// standing bites) because that pair is the proof the column is READ — the same
// proof is owed to the second spelling. The precedence between the spellings
// is pinned at the end; its unreadable-shadows-readable quirk is pinned at the
// unit level in internal/ap/vocab_test.go (TestBanExpiryReadsBothSpellings).
const (
	mbeBlockActivity = "https://lemmy.world/activities/announce/block/mbe-ban"
)

// TestAStandingEndTimeBanRefusesAdmission mirrors
// TestAStandingTimedBanRefusesAdmission with the expiry spelled ONLY as
// `endTime` — the shape newer Lemmy sends. Deleting the endTime read leaves
// this ban recorded with a NULL expiry (permanent), which the first assertion
// catches before the admission half even runs.
func TestAStandingEndTimeBanRefusesAdmission(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	h.announceBlock(world.groupA, mbeBlockActivity, mtAuthorDID, groupID,
		map[string]any{"endTime": "2099-01-01T00:00:00Z"})

	ban, found := banFor(t, h.db, world.communityADID, mtAuthorDID)
	require.True(t, found, "a temporary ban is still a ban and must be recorded")
	require.True(t, ban.expires.Valid,
		"the endTime spelling must be READ into the row: a NULL expiry here means the "+
			"`return o.EndTime` fallback in BanExpiry is gone and this timed ban was stored "+
			"as PERMANENT — the moderator asked for a duration, Lemmy will send no Undo when "+
			"it lapses, and nothing will ever clear the exclusion")
	assert.True(t, ban.expires.Time.After(time.Now()), "carrying an expiry that has not arrived")

	admitPost(t, world, mtAuthorDID, mbPostAfterBanRKey, world.communityADID,
		"3lzmberev0001", 1_775_000_020_000_001)
	postURI := mbPostATURI(mtAuthorDID, mbPostAfterBanRKey)
	status, code := admissionFor(t, h.db, world.communityADID, postURI)
	assert.Equal(t, accept.StatusRejected, status,
		"an in-force endTime ban applies exactly like an in-force `expires` ban: the spelling "+
			"is the sender's version, not a different decision")
	assert.Equal(t, "author-banned", code, "with the same reason")

	_, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, testDigestRKey(postURI))
	assert.True(t, errors.IsNotFound(err), "and no acceptance (err=%v)", err)
}

// TestALapsedEndTimeOnlyBanIsInert mirrors TestALapsedBanIsInert with the
// expiry spelled ONLY as `endTime`, and it is the destructive half of the same
// deletion: an endTime that nobody reads makes this lapsed ban look PERMANENT,
// and a permanent ban with removeData acts — it cancels queued work and
// terminally removes accepted posts, both on the strength of an exclusion that
// ended before it arrived, and both unrecoverable by design.
func TestALapsedEndTimeOnlyBanIsInert(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	// Queued work, and an accepted post: the two things a live ban destroys.
	admitPost(t, world, mtAuthorDID, mbPostInARKey, world.communityADID,
		"3lzmberev0010", 1_775_000_021_000_001)
	requireEveryDelivery(t, h.db, mtAuthorDID, groupID, "pending",
		"precondition: the author has queued work for A")
	_, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, world.digestRKey)
	require.NoError(t, err, "precondition: and an accepted post in A")

	// A ban that ended before it arrived, its end spelled the newer-Lemmy way —
	// carrying removeData, so every destructive branch is on the table.
	h.announceBlock(world.groupA, mbeBlockActivity, mtAuthorDID, groupID,
		map[string]any{"endTime": "2020-01-01T00:00:00Z", "removeData": true})

	// (1) It cancels nothing.
	assertEveryDelivery(t, h.db, mtAuthorDID, groupID, "pending",
		"a lapsed endTime-only ban cancels NOTHING: reading it as permanent (the endTime "+
			"fallback deleted) silently unpublishes queued work over an exclusion that has "+
			"already ended, and nothing re-queues a cancelled delivery")

	// (2) It removes nothing.
	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionRemoval, world.digestRKey)
	assert.True(t, errors.IsNotFound(err),
		"and it removes NOTHING: removeData over a misread-as-permanent lapsed ban strips "+
			"accepted posts terminally — no Undo{Block} restores content, by design (err=%v)", err)
	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, world.digestRKey)
	assert.NoError(t, err, "the acceptance stands (err=%v)", err)

	// Same conditional as the `expires` twin: recording the lapsed row is
	// optional, an UNBOUNDED row is the one shape that may never exist — it
	// reads as permanent forever and fails inside this guard.
	if ban, found := banFor(t, h.db, world.communityADID, mtAuthorDID); found {
		require.True(t, ban.expires.Valid,
			"a lapsed endTime ban that IS recorded must carry its expiry: a NULL here IS the "+
				"endTime read failing, and it turns a ban that already ended into a permanent "+
				"one no activity will ever correct")
		assert.True(t, ban.expires.Time.Before(time.Now()),
			"and it must be the moment the moderator chose, in the past")
	}

	// (3) It refuses no admission.
	admitPost(t, world, mtAuthorDID, mbPostAfterBanRKey, world.communityADID,
		"3lzmberev0011", 1_775_000_021_000_002)
	postURI := mbPostATURI(mtAuthorDID, mbPostAfterBanRKey)
	status, code := admissionFor(t, h.db, world.communityADID, postURI)
	assert.Equal(t, accept.StatusAccepted, status,
		"a LAPSED endTime ban must not refuse admission: the expiry IS the lift, whichever "+
			"spelling carried it — no Undo is coming")
	assert.NotEqual(t, "author-banned", code, "and certainly not for being banned")
}

// TestBothExpirySpellingsExpiresWins pins the precedence BanExpiry implements:
// `Expires` is checked FIRST, and `endTime` only fills its absence. A sender
// spelling the expiry both ways gets the 0.19 spelling honoured.
//
// This pins CURRENT behavior, quirk included: precedence is decided on key
// presence, so an `expires` that is present but UNREADABLE shadows a readable
// `endTime` — applyBan then refuses the whole activity rather than quietly
// substituting the other spelling's duration. That shadow is the safe/refuse
// direction (chunk-7 report, "Unique catches"), and it is pinned at the unit
// level in ap.TestBanExpiryReadsBothSpellings; what this test pins is the
// readable-vs-readable half as the row the bridge actually stores.
func TestBothExpirySpellingsExpiresWins(t *testing.T) {
	h := newHarness(t)
	world := newModerationWorld(t, h)

	// Two different in-force expiries, one per spelling. Only the stored
	// timestamp can say which one was read.
	expiresTime := time.Date(2030, 6, 15, 12, 0, 0, 0, time.UTC)
	endTimeTime := time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC)
	h.announceBlock(world.groupA, mbeBlockActivity, mtAuthorDID, groupID, map[string]any{
		"expires": "2030-06-15T12:00:00Z",
		"endTime": "2099-01-01T00:00:00Z",
	})

	ban, found := banFor(t, h.db, world.communityADID, mtAuthorDID)
	require.True(t, found, "the ban is recorded")
	require.True(t, ban.expires.Valid, "with an expiry")
	assert.True(t, ban.expires.Time.Equal(expiresTime),
		"`expires` wins when both spellings are present: the stored expiry must be the "+
			"`expires` value, not endTime's — an implementation that lets endTime overwrite "+
			"it has inverted BanExpiry's precedence, and the ban would outlive the duration "+
			"the 0.19 spelling declared (stored %s)", ban.expires.Time)
	assert.False(t, ban.expires.Time.Equal(endTimeTime),
		"and specifically NOT the endTime value")
}
