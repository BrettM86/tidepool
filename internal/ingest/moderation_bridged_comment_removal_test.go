package ingest

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// PRD MODERATION GAPS, TASK 02, BATCH 2 — BRIDGED LEMMY COMMENTS.
//
// A moderator's removal of a bridged Lemmy comment leaves the comment's record
// in its author's repo and writes a social.coves.community.removal record into
// the community repo that owns the comment's thread. The moderator's
// Undo{Delete} deletes that record and leaves the comment at its current
// content. Only the author's own summary-less Delete destroys the comment.

const (
	bcCommenterID = "https://lemmy.world/u/commenter"
	bcCommentPath = "/comment/3101"
	bcCommentID   = "https://lemmy.world" + bcCommentPath
	bcCommentBody = "a comment the moderators will remove"
	bcPublished   = "2026-07-07T05:00:00.000000Z"
)

// bridgedComment is one Lemmy comment materialized in the technology
// community's thread, read back from the mapping and the author's repo.
type bridgedComment struct {
	group        *remoteActor
	communityDID string
	mapping      *store.APObjectMapping
	atURI        string
	cid          string
}

// seedBridgedComment bridges the lemmy.world page and one comment under it.
func seedBridgedComment(t *testing.T, h *harness) bridgedComment {
	t.Helper()
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	ctx := context.Background()

	require.Equal(t, http.StatusAccepted,
		h.deliver(group, loadFixture(t, "announce_create_page_lemmy_world.json")))
	h.drain()

	h.serveObject("/u/commenter", person(bcCommenterID, "commenter", nil))
	commentDoc := note(bcCommentID, bcCommenterID, pageID, bcCommentBody, bcPublished)
	h.serveObject(bcCommentPath, commentDoc)
	h.announceCreate(group, "https://lemmy.world/activities/announce/create/3101", commentDoc)

	mapping, err := h.objects.GetByAPID(ctx, bcCommentID)
	require.NoError(t, err, "precondition: the comment is mapped")
	require.Equal(t, materialize.CollectionComment, mapping.Collection)
	require.False(t, mapping.IsDeleted())
	_, recordCID, err := h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	require.NoError(t, err, "precondition: the comment record is in its author's repo")

	return bridgedComment{
		group:        group,
		communityDID: testDIDFor("technology", "lemmy.world"),
		mapping:      mapping,
		atURI:        "at://" + mapping.DID + "/" + mapping.Collection + "/" + mapping.RKey,
		cid:          recordCID,
	}
}

// assertCommentStillLive checks the bridged comment's mapping and record
// survived a moderator action, at the version seeded.
func assertCommentStillLive(t *testing.T, h *harness, c bridgedComment, wantCID string) {
	t.Helper()
	ctx := context.Background()
	mapping, err := h.objects.GetByAPID(ctx, c.mapping.APID)
	require.NoError(t, err)
	assert.False(t, mapping.IsDeleted(), "the comment's mapping stays live")
	_, recordCID, err := h.manager.GetRecord(ctx, c.mapping.DID, c.mapping.Collection, c.mapping.RKey)
	if assert.NoError(t, err, "the comment's record stays in its author's repo") {
		assert.Equal(t, wantCID, recordCID, "at the same version")
	}
}

// assertBridgedCommentRemovalRecord checks the removal record for a bridged
// comment. wantReason nil means the record must carry no reason field.
func assertBridgedCommentRemovalRecord(t *testing.T, h *harness, c bridgedComment, wantCID string, wantReason *string) {
	t.Helper()
	removal, _, err := h.manager.GetRecord(context.Background(),
		c.communityDID, materialize.CollectionRemoval, testDigestRKey(c.atURI))
	if !assert.NoError(t, err,
		"a removal record must exist in the community repo owning the thread, at the digest "+
			"rkey of the comment's at-uri") {
		return
	}
	assert.Equal(t, materialize.CollectionRemoval, removal["$type"])
	subject, ok := removal["subject"].(map[string]any)
	if assert.True(t, ok, "removal.subject must be a strongRef, got %#v", removal["subject"]) {
		assert.Equal(t, c.atURI, subject["uri"], "the subject names the comment")
		assert.Equal(t, wantCID, strongRefCID(subject["cid"]), "at the comment's CID")
	}
	assert.Equal(t, "moderator-discretion", removal["code"])
	if wantReason == nil {
		assert.NotContains(t, removal, "reason", "no reason given: the field is omitted")
	} else {
		assert.Equal(t, *wantReason, removal["reason"], "the moderator's reason")
	}
}

// bridgedRemovalCases are the three Delete shapes that are a community removal
// of a bridged comment rather than its author's delete.
var bridgedRemovalCases = []struct {
	name       string
	actor      string
	summary    *string
	wantReason *string
}{
	{name: "moderator with a reason", actor: modActorID,
		summary: ptr("rule 3: no personal attacks"), wantReason: ptr("rule 3: no personal attacks")},
	{name: "summary-less, attributed to a moderator", actor: modActorID, summary: nil, wantReason: nil},
	{name: "attributed to the author but carrying an empty summary", actor: bcCommenterID,
		summary: ptr(""), wantReason: nil},
}

// TestAModeratorRemovalOfABridgedCommentWritesARemovalRecord is B5.
func TestAModeratorRemovalOfABridgedCommentWritesARemovalRecord(t *testing.T) {
	for _, tc := range bridgedRemovalCases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			c := seedBridgedComment(t, h)

			h.announceDeleteBy(c.group, "https://lemmy.world/activities/announce/delete/bc-remove",
				bcCommentID, tc.actor, tc.summary)

			assertCommentStillLive(t, h, c, c.cid)
			assertBridgedCommentRemovalRecord(t, h, c, c.cid, tc.wantReason)
		})
	}
}

// TestRestoringAModeratorRemovedBridgedCommentKeepsItsCurrentContent is B7: the
// comment is edited while removed, the origin serves the edit, and the Undo
// deletes the removal record and leaves the comment at the edited body. A
// second Undo under a fresh Announce id writes nothing to the community repo.
func TestRestoringAModeratorRemovedBridgedCommentKeepsItsCurrentContent(t *testing.T) {
	for _, tc := range bridgedRemovalCases[:2] {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			ctx := context.Background()
			c := seedBridgedComment(t, h)

			const deleteActivity = "https://lemmy.world/activities/announce/delete/bc-restore"
			h.announceDeleteBy(c.group, deleteActivity, bcCommentID, tc.actor, tc.summary)
			_, _, err := h.manager.GetRecord(ctx,
				c.communityDID, materialize.CollectionRemoval, testDigestRKey(c.atURI))
			assert.NoError(t, err, "the removal wrote a removal record into the community repo")

			// --- The author edits the removed comment; the origin serves the edit.
			const editedBody = "an edit written while the comment was removed"
			edited := note(bcCommentID, bcCommenterID, pageID, editedBody, bcPublished)
			edited["updated"] = "2026-07-07T06:00:00.000000Z"
			h.serveObject(bcCommentPath, edited)
			require.Equal(t, http.StatusAccepted, h.deliver(c.group, map[string]any{
				"id":       "https://lemmy.world/activities/announce/update/bc-restore",
				"type":     "Announce",
				"actor":    c.group.id,
				"audience": c.group.id,
				"object": map[string]any{
					"id":       "https://lemmy.world/activities/update/bc-restore",
					"type":     "Update",
					"actor":    bcCommenterID,
					"audience": c.group.id,
					"object":   edited,
				},
			}))
			h.drain()

			record, _, err := h.manager.GetRecord(ctx, c.mapping.DID, c.mapping.Collection, c.mapping.RKey)
			if assert.NoError(t, err, "a removed comment's record is still its author's to edit") {
				assert.Equal(t, editedBody, record["content"], "the edit is applied while removed")
			}

			// --- The moderators restore it.
			h.announceUndoDelete(c.group, "https://lemmy.world/activities/announce/undo/bc-restore",
				deleteActivity+"/delete", bcCommentID, tc.summary)

			_, _, err = h.manager.GetRecord(ctx,
				c.communityDID, materialize.CollectionRemoval, testDigestRKey(c.atURI))
			assert.True(t, errors.IsNotFound(err),
				"the restore deletes the removal record (err=%v)", err)
			_, _, err = h.manager.GetRecord(ctx,
				c.communityDID, materialize.CollectionAcceptance, testDigestRKey(c.atURI))
			assert.True(t, errors.IsNotFound(err),
				"a comment has no acceptance: the restore writes none (err=%v)", err)
			mapping, err := h.objects.GetByAPID(ctx, bcCommentID)
			require.NoError(t, err)
			assert.False(t, mapping.IsDeleted(), "the restored comment's mapping is live")
			record, _, err = h.manager.GetRecord(ctx, c.mapping.DID, c.mapping.Collection, c.mapping.RKey)
			if assert.NoError(t, err, "the restored comment's record exists") {
				assert.Equal(t, editedBody, record["content"],
					"the restored comment carries its current (edited) body")
			}

			// --- The same Undo again, under a fresh Announce id; the origin is unchanged.
			framesBefore := len(communityEvents(t, h, c.communityDID))
			h.announceUndoDelete(c.group, "https://lemmy.world/activities/announce/undo/bc-restore-again",
				deleteActivity+"/delete", bcCommentID, tc.summary)
			assert.Equal(t, framesBefore, len(communityEvents(t, h, c.communityDID)),
				"a repeated restore writes nothing to the community repo")
		})
	}
}

// TestRestoringABridgedCommentWhoseReMaterializationSkipsLiftsTheRemoval: the
// origin serves the removed comment with no content, so the restore's
// re-materialization skips. The comment was never deleted, so its mapping and
// record stay live, and the moderators' Undo still deletes the removal record.
func TestRestoringABridgedCommentWhoseReMaterializationSkipsLiftsTheRemoval(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	c := seedBridgedComment(t, h)

	const deleteActivity = "https://lemmy.world/activities/announce/delete/bc-skip"
	reason := ptr("rule 3: no personal attacks")
	h.announceDeleteBy(c.group, deleteActivity, bcCommentID, modActorID, reason)
	assertBridgedCommentRemovalRecord(t, h, c, c.cid, reason)

	h.serveObject(bcCommentPath, note(bcCommentID, bcCommenterID, pageID, "", bcPublished))
	h.announceUndoDelete(c.group, "https://lemmy.world/activities/announce/undo/bc-skip",
		deleteActivity+"/delete", bcCommentID, reason)

	assertCommentStillLive(t, h, c, c.cid)
	_, _, err := h.manager.GetRecord(ctx,
		c.communityDID, materialize.CollectionRemoval, testDigestRKey(c.atURI))
	assert.True(t, errors.IsNotFound(err),
		"the restore deletes the removal record even though re-materialization skipped (err=%v)", err)
}

// TestAnAuthorsUndoOfTheirOwnDeleteDoesNotLiftAModeratorRemoval: a standing
// moderator removal, then an announced summary-less Undo{Delete} attributed
// to the comment's author. That Undo is not a community decision, so the
// moderators' removal record stands.
func TestAnAuthorsUndoOfTheirOwnDeleteDoesNotLiftAModeratorRemoval(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	c := seedBridgedComment(t, h)

	reason := ptr("rule 3: no personal attacks")
	h.announceDeleteBy(c.group, "https://lemmy.world/activities/announce/delete/bc-author-undo",
		bcCommentID, modActorID, reason)
	assertBridgedCommentRemovalRecord(t, h, c, c.cid, reason)

	h.announceUndoDeleteBy(c.group, "https://lemmy.world/activities/announce/undo/bc-author-undo",
		"https://lemmy.world/activities/delete/bc-author-own", bcCommentID, bcCommenterID, nil)

	_, _, err := h.manager.GetRecord(ctx,
		c.communityDID, materialize.CollectionRemoval, testDigestRKey(c.atURI))
	assert.NoError(t, err, "the author's Undo leaves the moderators' removal record standing")
	assertCommentStillLive(t, h, c, c.cid)
}

// TestRestoringABridgedCommentTheOriginStillServesAsGoneKeepsTheRemoval: a
// standing moderator removal, then an Undo{Delete} whose fetch of the comment
// gets 410 Gone, which is how Lemmy serves a comment that is still removed (a
// stale Undo arriving late, for one). The origin's answer gates the lift, so
// the removal record stands.
func TestRestoringABridgedCommentTheOriginStillServesAsGoneKeepsTheRemoval(t *testing.T) {
	h := newHarness(t)
	c := seedBridgedComment(t, h)

	const deleteActivity = "https://lemmy.world/activities/announce/delete/bc-gone"
	reason := ptr("rule 3: no personal attacks")
	h.announceDeleteBy(c.group, deleteActivity, bcCommentID, modActorID, reason)
	assertBridgedCommentRemovalRecord(t, h, c, c.cid, reason)

	h.serveGone(bcCommentPath)
	h.announceUndoDelete(c.group, "https://lemmy.world/activities/announce/undo/bc-gone",
		deleteActivity+"/delete", bcCommentID, reason)

	// An origin still serving the comment as gone leaves the removal record standing.
	assertBridgedCommentRemovalRecord(t, h, c, c.cid, reason)
	assertCommentStillLive(t, h, c, c.cid)
}
