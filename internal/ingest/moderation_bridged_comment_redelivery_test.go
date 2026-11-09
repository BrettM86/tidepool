package ingest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/materialize"
)

// TestARemovalRecordForABridgedCommentSurvivesRedeliveryAndReRemoval is the
// bridged half of the native lifecycle test: a redelivered moderator Delete
// rewrites nothing, and a re-removal with a new reason keeps the moderators'
// original createdAt.
func TestARemovalRecordForABridgedCommentSurvivesRedeliveryAndReRemoval(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	c := seedBridgedComment(t, h)

	const firstDelete = "https://lemmy.world/activities/announce/delete/bc-first/delete"
	firstReason := "rule 3: no personal attacks"
	h.announceDeleteAs(c.group, "https://lemmy.world/activities/announce/delete/bc-first",
		firstDelete, bcCommentID, modActorID, &firstReason)
	assertBridgedCommentRemovalRecord(t, h, c, c.cid, &firstReason)

	first, firstCID, err := h.manager.GetRecord(ctx,
		c.communityDID, materialize.CollectionRemoval, testDigestRKey(c.atURI))
	require.NoError(t, err, "precondition: the removal record stands")
	firstCreatedAt := first["createdAt"]
	require.NotEmpty(t, firstCreatedAt, "precondition: the removal record carries a createdAt")
	framesBefore := len(communityEvents(t, h, c.communityDID))

	// The record's createdAt has millisecond precision and the materializer
	// reads the wall clock: let the clock pass the first removal's millisecond so
	// a createdAt taken from "now" on a later write cannot equal the original.
	time.Sleep(5 * time.Millisecond)

	// --- The SAME inner Delete, redelivered under a fresh Announce id.
	h.announceDeleteAs(c.group, "https://lemmy.world/activities/announce/delete/bc-redelivered",
		firstDelete, bcCommentID, modActorID, &firstReason)

	_, redeliveredCID, err := h.manager.GetRecord(ctx,
		c.communityDID, materialize.CollectionRemoval, testDigestRKey(c.atURI))
	assert.NoError(t, err, "the removal record still stands after the redelivery")
	assert.Equal(t, firstCID, redeliveredCID,
		"a redelivered removal rewrites nothing: the record's CID is unchanged")
	assert.Equal(t, framesBefore, len(communityEvents(t, h, c.communityDID)),
		"and commits no frame to the community repo")

	// --- A re-removal with a different reason (fresh Announce and Delete ids).
	secondReason := "rule 5: spam"
	h.announceDeleteBy(c.group, "https://lemmy.world/activities/announce/delete/bc-second",
		bcCommentID, modActorID, &secondReason)

	assertCommentStillLive(t, h, c, c.cid)
	removal, _, err := h.manager.GetRecord(ctx,
		c.communityDID, materialize.CollectionRemoval, testDigestRKey(c.atURI))
	if assert.NoError(t, err, "the re-removal updates the record at the same rkey") {
		assert.Equal(t, secondReason, removal["reason"], "carrying the new reason")
		assert.Equal(t, firstCreatedAt, removal["createdAt"],
			"and the ORIGINAL createdAt: the moderators removed it once, at that time")
	}
}
