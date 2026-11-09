package ingest

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/accept"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
)

// TestAReplacementBanRecordRetriedAfterItsExpiryStillReplacesTheRecord: a
// permanent ban stands with its record; a timed replacement Block arrives in
// force, its row commits, its ban record commit fails, and the retry runs only
// after the timed expiry. Whether the ban was in force is a fact about its
// arrival, so the retry still publishes the timed record: the record ends
// matching the row instead of leaving the old permanent ban enforced forever.
func TestAReplacementBanRecordRetriedAfterItsExpiryStillReplacesTheRecord(t *testing.T) {
	for _, tc := range []struct {
		name    string
		subject func(*testing.T) banSubject
	}{
		{name: "native persona", subject: nativeBanSubject},
		{name: "minted bridged person", subject: bridgedBanSubject},
	} {
		t.Run(tc.name, func(t *testing.T) {
			replacementRetriedAfterExpiry(t, tc.subject(t))
		})
	}
}

func replacementRetriedAfterExpiry(t *testing.T, subject banSubject) {
	h := subject.h

	subject.block("https://lemmy.world/activities/announce/block/br-retry-permanent",
		map[string]any{"summary": "rule 1: permanent"})
	permanent, _, found := banRecord(t, h, subject.communityDID, subject.subjectDID)
	require.True(t, found, "precondition: the permanent Block wrote the ban record")
	require.NotContains(t, permanent, "expiresAt", "precondition: the standing record is permanent")

	const replacementID = "https://lemmy.world/activities/announce/block/br-retry-timed"
	timedExpiry := futureExpiry(48 * time.Hour)
	restore := rejectCommunityCommits(t, h, subject.communityDID)
	subject.block(replacementID, map[string]any{
		"summary": "rule 2: reduced", "expires": timedExpiry.Format(time.RFC3339),
	})
	assertRetryable(t, h, replacementID, "a replacement whose ban record commit failed")
	if subject.native {
		row, rowStands := banFor(t, h.db, subject.communityDID, subject.subjectDID)
		require.True(t, rowStands, "precondition: the timed row committed before the record write failed")
		require.NotNil(t, row.expires, "precondition: the row carries the timed expiry")
	}

	restore()
	realNow := timeNow
	timeNow = func() time.Time { return realNow().Add(72 * time.Hour) }
	t.Cleanup(func() { timeNow = realNow })
	retryNow(t, h, replacementID)
	assertProcessed(t, h, replacementID)

	record, _, found := banRecord(t, h, subject.communityDID, subject.subjectDID)
	require.True(t, found, "the retry publishes the replacement's ban record")
	assertBanRecordFields(t, record, subject.subjectDID, "rule 2: reduced", &timedExpiry)
}

// deliverNativeBlockReceivedAt announces a Block of the native author in
// community A, stamps its inbox event received at receivedAt, then drains, so
// the event is first processed now, after receipt.
func deliverNativeBlockReceivedAt(h *harness, world moderationWorld, activityID string,
	extra map[string]any, receivedAt time.Time) {
	h.t.Helper()
	require.Equal(h.t, http.StatusAccepted, h.deliver(world.groupA, map[string]any{
		"id":       activityID,
		"type":     "Announce",
		"actor":    world.groupA.id,
		"audience": world.groupA.id,
		"cc":       []any{world.groupA.id + "/followers"},
		"object":   blockActivity(world.groupA, activityID+"/block", mtAuthorDID, groupID, extra),
	}))
	backdateReceived(h, activityID, receivedAt)
	h.drain()
}

// TestATimedBanProcessedAfterItsExpiryLeavesAStandingPermanentBanAndItsRecord:
// a permanent ban stands with its row and record; a timed replacement Block
// arrives in force but is first processed after its expiry. Ban()'s guard
// refuses to weaken the standing row, so the record must stay permanent too:
// a timed, already-expired record over it would have Coves read the ban as
// over while Tidepool's row keeps enforcing it.
func TestATimedBanProcessedAfterItsExpiryLeavesAStandingPermanentBanAndItsRecord(t *testing.T) {
	h := newHarness(t)
	world := newModerationWorld(t, h)

	h.announceBlock(world.groupA, "https://lemmy.world/activities/announce/block/br-refused-permanent",
		mtAuthorDID, groupID, map[string]any{"summary": "rule 1: permanent"})
	permanent, _, found := banRecord(t, h, world.communityADID, mtAuthorDID)
	require.True(t, found, "precondition: the permanent Block wrote the ban record")
	require.NotContains(t, permanent, "expiresAt", "precondition: the standing record is permanent")

	const replacementID = "https://lemmy.world/activities/announce/block/br-refused-timed"
	expired := pastExpiry()
	deliverNativeBlockReceivedAt(h, world, replacementID, map[string]any{
		"summary": "rule 2: reduced", "expires": expired.Format(time.RFC3339),
	}, expired.Add(-time.Hour))
	assertProcessed(t, h, replacementID)

	row, rowStands := banFor(t, h.db, world.communityADID, mtAuthorDID)
	require.True(t, rowStands, "the permanent row stands")
	assert.False(t, row.expires.Valid, "the guard left the permanent row permanent")
	record, _, found := banRecord(t, h, world.communityADID, mtAuthorDID)
	require.True(t, found, "the permanent record stands")
	assertBanRecordFields(t, record, mtAuthorDID, "rule 1: permanent", nil)
}

// TestANativeRemoveDataRetriedAfterItsExpiryRemovesNoPostAdmittedSince: a
// timed native ban with removeData arrives in force, its record write fails,
// the ban expires, the author posts again and the post is admitted, and only
// then does the retry run. The retry still publishes the record (judged at
// receipt), but the purge is judged at processing time: run now, it would
// permanently remove a post admitted after the ban ended.
func TestANativeRemoveDataRetriedAfterItsExpiryRemovesNoPostAdmittedSince(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	const blockID = "https://lemmy.world/activities/announce/block/br-purge-timed"
	expired := pastExpiry()
	restore := rejectCommunityCommits(t, h, world.communityADID)
	deliverNativeBlockReceivedAt(h, world, blockID, map[string]any{
		"summary": "rule 3: timed purge", "expires": expired.Format(time.RFC3339), "removeData": true,
	}, expired.Add(-time.Hour))
	assertRetryable(t, h, blockID, "a native ban whose ban record commit failed")
	restore()

	admitPost(t, world, mtAuthorDID, mbPostAfterBanRKey, world.communityADID,
		"3lzmbrev00200", 1_775_000_020_000_001)
	postURI := mbPostATURI(mtAuthorDID, mbPostAfterBanRKey)
	status, _ := admissionFor(t, h.db, world.communityADID, postURI)
	require.Equal(t, accept.StatusAccepted, status, "precondition: the post after the expiry is admitted")

	retryNow(t, h, blockID)
	assertProcessed(t, h, blockID)

	record, _, found := banRecord(t, h, world.communityADID, mtAuthorDID)
	require.True(t, found, "the retry publishes the ban record")
	assertBanRecordFields(t, record, mtAuthorDID, "rule 3: timed purge", &expired)
	_, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionRemoval, testDigestRKey(postURI))
	assert.True(t, errors.IsNotFound(err),
		"a post admitted after the ban ended is not removed by its late purge (err=%v)", err)
	_, _, err = h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionAcceptance, testDigestRKey(postURI))
	assert.NoError(t, err, "and its acceptance stands (err=%v)", err)
}
