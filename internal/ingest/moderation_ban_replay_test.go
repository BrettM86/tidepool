package ingest

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/accept"
	"tidepool/internal/ap"
)

// SECOND-OPINION (chunk 7, finding 5) — AN OLD MODERATION ACTIVITY ARRIVING
// TWICE MUST NOT UNDO THE CURRENT ONE.
//
// The inbox's dedup is by ACTIVITY ID, and every path that presents a ban a
// second time presents it under a NEW id: a dead-letter redrive re-wraps it, a
// backfill replays it, a delayed redelivery arrives after the moderators have
// already moved on. So the ban path has to survive out-of-order arrival on its
// own, and it has exactly one durable row per (community, author) to do it with.
//
// Two sequences, both ending with an author who is banned in Lemmy and unbanned
// here, permanently, with no activity left that could ever correct it:
//
//	(1) temp ban lapses → permanent ban → the OLD Block is redriven.
//	(2) temp ban → moderators lift it → permanent ban → the OLD Undo is redriven.
//
// Nothing else in the ban suite exercises a second Block or a second Undo for
// the same pair, so before these tests both sequences were entirely unpinned.
const (
	mbrLapsedBlock   = "https://lemmy.world/activities/announce/block/mbr-lapsed"
	mbrPermBlock     = "https://lemmy.world/activities/announce/block/mbr-permanent"
	mbrTempBlock     = "https://lemmy.world/activities/announce/block/mbr-temp"
	mbrUndo          = "https://lemmy.world/activities/announce/undo/mbr-undo"
	mbrUndoRedrive   = "https://lemmy.world/activities/announce/undo/mbr-undo-redrive"
	mbrPostRKey      = "3lzmbrpost00001"
	mbrPostAfterRKey = "3lzmbrpost00002"
)

// TestAReplayedLapsedBlockCannotLiftAStandingBan is sequence (1).
//
// The stale Block is a faithful record of a ban that is over. Written over a
// PERMANENT ban with last-writer-wins it becomes something else entirely: the
// exclusion the moderators are currently enforcing, ended by our own redrive.
func TestAReplayedLapsedBlockCannotLiftAStandingBan(t *testing.T) {
	h := newHarness(t)
	world := newModerationWorld(t, h)

	// --- GIVEN: the ban that stands. Permanent, because that is what the
	//     moderators escalated to after the timed one ran out.
	h.announceBlock(world.groupA, mbrPermBlock, mtAuthorDID, groupID,
		map[string]any{"summary": "repeated spam"})
	ban, found := banFor(t, h.db, world.communityADID, mtAuthorDID)
	require.True(t, found, "precondition: the permanent ban is recorded")
	require.False(t, ban.expires.Valid, "precondition: with no expiry")

	// --- WHEN: the OLD Block — the timed one that ended long ago — is redriven
	//     from the dead-letter queue under its own activity id, so the inbox's
	//     dedup has never seen it and cannot stop it.
	h.announceBlock(world.groupA, mbrLapsedBlock, mtAuthorDID, groupID, map[string]any{
		"expires": "2020-01-01T00:00:00Z",
		"summary": "three days, served",
	})

	// --- THEN: the standing ban is untouched.
	ban, found = banFor(t, h.db, world.communityADID, mtAuthorDID)
	require.True(t, found,
		"the ban row survives: the redrive is a message about a ban that ended, not an unban")
	assert.False(t, ban.expires.Valid,
		"AND IT IS STILL PERMANENT. A past expires_at written here reads as unbanned from "+
			"this moment on, and nothing re-applies it — Lemmy sent its Block once, and sends "+
			"NOTHING to say a ban is still in force")
	assert.Equal(t, "repeated spam", ban.reason,
		"with the standing ban's reason: the stale one describes a different, finished ban")

	// --- AND: the gate that actually excludes the author still refuses.
	admitPost(t, world, mtAuthorDID, mbrPostAfterRKey, world.communityADID,
		"3lzmbrrev0001", 1_775_000_030_000_001)
	postURI := mbPostATURI(mtAuthorDID, mbrPostAfterRKey)
	status, code := admissionFor(t, h.db, world.communityADID, postURI)
	assert.Equal(t, accept.StatusRejected, status,
		"the author is still banned where it counts: an admission that resumes here signs "+
			"the community's name to content from somebody it is currently excluding")
	assert.Equal(t, "author-banned", code, "for the reason the moderators gave")
}

// TestAStaleUndoBlockCannotLiftANewerBan is sequence (2).
//
// The Undo carries no id of the ban it lifts that we store, and Lemmy mints a
// fresh Block inside it — so the ONLY thing separating "the unban the moderators
// just sent" from "the unban they sent a month ago, redriven" is the expiry the
// undone Block names. An Undo of a ban that ended at T cannot be an unban of a
// ban that outlives T.
func TestAStaleUndoBlockCannotLiftANewerBan(t *testing.T) {
	h := newHarness(t)
	world := newModerationWorld(t, h)

	// --- GIVEN: a timed ban, and the moderators lifting it early.
	h.announceBlock(world.groupA, mbrTempBlock, mtAuthorDID, groupID,
		map[string]any{"expires": "2030-01-01T00:00:00Z", "summary": "two weeks"})
	require.True(t, hasBan(t, h, world), "precondition: the timed ban landed")

	h.announceUndoBlockWithExpiry(world.groupA, mbrUndo, mbrTempBlock+"/block",
		mtAuthorDID, groupID, map[string]any{"expires": "2030-01-01T00:00:00Z"})
	require.False(t, hasBan(t, h, world),
		"precondition: the unban lifted it — an Undo whose ban matches must still work, or "+
			"this guard has broken the ordinary case it was written to protect")

	// --- AND: the author earns a permanent ban afterwards.
	h.announceBlock(world.groupA, mbrPermBlock, mtAuthorDID, groupID,
		map[string]any{"summary": "came back and did it again"})
	ban, found := banFor(t, h.db, world.communityADID, mtAuthorDID)
	require.True(t, found, "precondition: the permanent ban is recorded")
	require.False(t, ban.expires.Valid, "precondition: with no expiry")

	// --- WHEN: the OLD Undo is redriven under a new activity id.
	h.announceUndoBlockWithExpiry(world.groupA, mbrUndoRedrive, mbrTempBlock+"/block",
		mtAuthorDID, groupID, map[string]any{"expires": "2030-01-01T00:00:00Z"})

	// --- THEN: the permanent ban survives it.
	ban, found = banFor(t, h.db, world.communityADID, mtAuthorDID)
	require.True(t, found,
		"THE NEWER BAN SURVIVES. An Undo of a two-week ban cannot lift a permanent one: the "+
			"moderators reversed a decision that is no longer the decision, and a delete that "+
			"took the row anyway leaves the author unbanned here forever — Lemmy will send no "+
			"second Block")
	assert.False(t, ban.expires.Valid, "unchanged, expiry and all")
	assert.Equal(t, "came back and did it again", ban.reason)

	admitPost(t, world, mtAuthorDID, mbrPostRKey, world.communityADID,
		"3lzmbrrev0002", 1_775_000_031_000_001)
	status, code := admissionFor(t, h.db, world.communityADID, mbPostATURI(mtAuthorDID, mbrPostRKey))
	assert.Equal(t, accept.StatusRejected, status,
		"and admission still refuses, which is the only place the author feels it")
	assert.Equal(t, "author-banned", code)
}

func hasBan(t *testing.T, h *harness, world moderationWorld) bool {
	t.Helper()
	_, found := banFor(t, h.db, world.communityADID, mtAuthorDID)
	return found
}

// announceUndoBlockWithExpiry is announceUndoBlock with the undone Block's own
// fields spelled out — the expiry in particular, which is the only description
// of the ban an Undo carries.
func (h *harness) announceUndoBlockWithExpiry(group *remoteActor, activityID, blockActivityID,
	subjectDID, target string, extra map[string]any) {

	h.t.Helper()
	require.Equal(h.t, http.StatusAccepted, h.deliver(group, map[string]any{
		"id":       activityID,
		"type":     "Announce",
		"actor":    group.id,
		"audience": group.id,
		"cc":       []any{group.id + "/followers"},
		"object": map[string]any{
			"id":       activityID + "/undo",
			"type":     ap.TypeUndo,
			"actor":    modActorID,
			"audience": group.id,
			"cc":       []any{group.id},
			"object":   blockActivity(group, blockActivityID, subjectDID, target, extra),
		},
	}))
	h.drain()
}
