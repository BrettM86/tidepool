package ingest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/materialize"
)

// BACKLOG 2026-11-05 — A BAN WITH removeData ALSO REMOVES THE PERSONA'S
// NATIVE COMMENTS IN THAT COMMUNITY.
//
// Lemmy removes a banned user's comments in the community on its side. Before
// this, applyBan removed only the persona's posts (RemoveAuthorPosts), so their
// native comments in the community kept showing on Coves.

// TestABanWithRemoveDataRemovesTheirNativeCommentsInThatCommunityOnly: GIVEN a
// native persona with a native comment in community A and one in community B,
// and a different persona's native comment in A, WHEN A announces
// Block{removeData:true} of the first persona, THEN only their comment in A
// gets a community.removal record in A's repo (author-banned, the Block's
// summary as reason) and a bridge-side removal bound to A; a replay of the
// Block writes no new commit for that removal record.
func TestABanWithRemoveDataRemovesTheirNativeCommentsInThatCommunityOnly(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	// The author's post in B, so they can comment there.
	admitPost(t, world, mtAuthorDID, mbPostInBRKey, world.communityBDID, "3lzmbrev00100", 1_775_000_016_000_001)
	postInB := mbPostATURI(mtAuthorDID, mbPostInBRKey)

	authorInA := mbCommentBy(mtAuthorDID, "3lzmbcomment11", mtPostATURI, mtPostCID, "3lzmbrev00101")
	authorInB := mbCommentBy(mtAuthorDID, "3lzmbcomment12", postInB, mtPostCID, "3lzmbrev00102")
	for _, comment := range []nativeComment{authorInA, authorInB, mtDirectReply} {
		require.NoError(t, world.dispatcher.HandleEvent(ctx, comment.create(t)),
			"precondition: the native comment %s federated", comment.atURI())
	}
	for _, precondition := range []struct {
		comment      nativeComment
		communityDID string
	}{
		{authorInA, world.communityADID},
		{authorInB, world.communityBDID},
		{mtDirectReply, world.communityADID},
	} {
		mapping, err := h.objects.GetByAPID(ctx, precondition.comment.apID())
		require.NoError(t, err, "precondition: %s is mapped", precondition.comment.atURI())
		require.Equal(t, precondition.communityDID, mapping.CommunityDID,
			"precondition: %s is bound to the community it federated into", precondition.comment.atURI())
	}

	const banReason = "harassment across threads"
	h.announceBlock(world.groupA, "https://lemmy.world/activities/announce/block/mbc-ban",
		mtAuthorDID, groupID, map[string]any{"removeData": true, "summary": banReason})

	cases := []struct {
		name         string
		commentATURI string
		commentCID   string
		communityDID string
		wantRemoved  bool
	}{
		{
			name:         "the banned persona's comment in the banning community is removed",
			commentATURI: "at://did:plc:mtnativeauthor0001/social.coves.community.comment/3lzmbcomment11",
			commentCID:   "bafyreih5xbmigkq5ikyhqiqhqzbwuqjxeitgtzwyxvjhfsfvswsxmnnf8a",
			communityDID: world.communityADID,
			wantRemoved:  true,
		},
		{
			name:         "the banned persona's comment in another community stands",
			commentATURI: "at://did:plc:mtnativeauthor0001/social.coves.community.comment/3lzmbcomment12",
			communityDID: world.communityBDID,
			wantRemoved:  false,
		},
		{
			name:         "another persona's comment in the banning community stands",
			commentATURI: "at://did:plc:mtnativecommnter1/social.coves.community.comment/3lzmtcomment01",
			communityDID: world.communityADID,
			wantRemoved:  false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			removal, _, err := h.manager.GetRecord(ctx,
				tc.communityDID, materialize.CollectionRemoval, testDigestRKey(tc.commentATURI))
			state, found := removalStateFor(t, h.db, tc.commentATURI)
			if !tc.wantRemoved {
				assert.True(t, errors.IsNotFound(err),
					"no removal record for %s in %s (err=%v)", tc.commentATURI, tc.communityDID, err)
				// The comment in B must not get a record in A's repo either.
				_, _, errA := h.manager.GetRecord(ctx,
					world.communityADID, materialize.CollectionRemoval, testDigestRKey(tc.commentATURI))
				assert.True(t, errors.IsNotFound(errA),
					"and none in the banning community's repo (err=%v)", errA)
				assert.False(t, found && state.removed, "and no bridge-side removal")
				return
			}
			if !assert.NoError(t, err,
				"removeData must remove the banned persona's native comment in this community: "+
					"a social.coves.community.removal record in the community repo at the digest "+
					"rkey of the comment's at-uri") {
				return
			}
			assert.Equal(t, materialize.CollectionRemoval, removal["$type"])
			subject, ok := removal["subject"].(map[string]any)
			if assert.True(t, ok, "removal.subject must be a strongRef, got %#v", removal["subject"]) {
				assert.Equal(t, tc.commentATURI, subject["uri"], "the subject names the comment")
				assert.Equal(t, tc.commentCID, strongRefCID(subject["cid"]), "at the comment's CID")
			}
			assert.Equal(t, "author-banned", removal["code"],
				"under author-banned: the comment was not judged, its author was")
			assert.Equal(t, "harassment across threads", removal["reason"], "the Block's summary")

			require.True(t, found, "the removal is recorded bridge-side in object_moderation")
			assert.True(t, state.removed, "with removed_at set")
			assert.Equal(t, tc.communityDID, state.communityDID, "bound to the banning community")
			assert.Equal(t, "author-banned", state.code)
			assert.Equal(t, "harassment across threads", state.reason)
		})
	}

	// --- A replay of the same Block (a redelivery under a fresh Announce id)
	//     writes no new commit for the comment's removal record.
	h.announceBlock(world.groupA, "https://lemmy.world/activities/announce/block/mbc-ban-replay",
		mtAuthorDID, groupID, map[string]any{"removeData": true, "summary": banReason})
	removalPath := materialize.CollectionRemoval + "/" +
		testDigestRKey("at://did:plc:mtnativeauthor0001/social.coves.community.comment/3lzmbcomment11")
	commits := 0
	for _, event := range communityEvents(t, h, world.communityADID) {
		for _, op := range event.Ops {
			if op.Path == removalPath {
				commits++
			}
		}
	}
	assert.Equal(t, 1, commits,
		"exactly one commit ever touches the comment's removal record: a replayed Block rewrites nothing")
}

// TestABanWithRemoveDataLeavesANativeCommentTheCommunityAlreadyRemovedAlone:
// GIVEN the persona's native comment in A, already removed by A's moderators
// (an announced Delete with summary "doxxing", recorded under
// moderator-discretion), WHEN A then announces Block{removeData:true,
// summary:"spam wave"} of the persona, THEN the comment's removal keeps the
// moderators' code and reason in both the community repo record and
// object_moderation, and no second commit rewrites the record.
func TestABanWithRemoveDataLeavesANativeCommentTheCommunityAlreadyRemovedAlone(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)

	authorInA := mbCommentBy(mtAuthorDID, "3lzmbcomment21", mtPostATURI, mtPostCID, "3lzmbrev00121")
	require.NoError(t, world.dispatcher.HandleEvent(ctx, authorInA.create(t)),
		"precondition: the native comment federated")
	mapping, err := h.objects.GetByAPID(ctx, authorInA.apID())
	require.NoError(t, err, "precondition: the comment is mapped")
	require.Equal(t, world.communityADID, mapping.CommunityDID, "precondition: bound to A")

	h.announceDeleteWithSummary(world.groupA,
		"https://lemmy.world/activities/announce/delete/mbc-doxxing", authorInA.apID(), ptr("doxxing"))
	const commentATURI = "at://did:plc:mtnativeauthor0001/social.coves.community.comment/3lzmbcomment21"
	before, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionRemoval, testDigestRKey(commentATURI))
	require.NoError(t, err, "precondition: the moderators' removal record exists")
	require.Equal(t, "moderator-discretion", before["code"], "precondition: under moderator-discretion")

	h.announceBlock(world.groupA, "https://lemmy.world/activities/announce/block/mbc-ban-after-removal",
		mtAuthorDID, groupID, map[string]any{"removeData": true, "summary": "spam wave"})

	removal, _, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionRemoval, testDigestRKey(commentATURI))
	if assert.NoError(t, err, "the moderators' removal record still stands") {
		assert.Equal(t, "moderator-discretion", removal["code"],
			"the community already judged this comment: the ban must not relabel it author-banned")
		assert.Equal(t, "doxxing", removal["reason"], "the moderators' reason, not the ban's summary")
	}

	state, found := removalStateFor(t, h.db, commentATURI)
	require.True(t, found, "the removal is still recorded bridge-side")
	assert.True(t, state.removed, "with removed_at set")
	assert.Equal(t, "moderator-discretion", state.code, "object_moderation keeps the moderators' code")
	assert.Equal(t, "doxxing", state.reason, "and their reason")

	removalPath := materialize.CollectionRemoval + "/" + testDigestRKey(commentATURI)
	commits := 0
	for _, event := range communityEvents(t, h, world.communityADID) {
		for _, op := range event.Ops {
			if op.Path == removalPath {
				commits++
			}
		}
	}
	assert.Equal(t, 1, commits,
		"exactly one commit ever touches the removal record: the moderators' removal, not the ban")
}
