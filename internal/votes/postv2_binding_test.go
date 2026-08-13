package votes

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// Announced-vote community binding for the postv2 era (PLAN.md decision 20).
//
// An announced vote counts only for the community that owns its subject:
// without that binding, one followed community could Announce Like/Dislike
// against ANY bridged subject and skew every other community's scores.
//
// Which community owns a subject is answered by materialize.CommunityDIDOf.
// Until task 19 the answer was implicit in placement — a post lived in its
// community's repo, so the mapping's DID WAS the community, and a comment
// borrowed its thread root's repo DID. After the flip a postv2 lives in the
// AUTHOR's repo and matches no community DID at all, so ownership is recorded
// explicitly (ap_objects.community_did) with the record as the fallback.
//
// Both directions of that check fail CLOSED: a subject whose community cannot
// be determined is refused, never admitted. That is the safe direction, and
// also the quiet one — a binding that stopped recognising legitimate subjects
// would drop real votes silently, with nothing but flat counts to show for
// it, which is why these tests assert the POSITIVE cases as hard as the
// negative one.
//
// FIXTURE NOTE. These tests assert BEHAVIOR only — applied vs dropped — and
// never how the community is recovered. They deliberately populate the
// mapping's community linkage AND the stored postv2 record's `community`
// field, which is the shape a pre-016 row has: that keeps the record
// fallback in CommunityDIDOf exercised rather than only its column fast
// path, since rows written before the migration are answerable from the
// record alone.
const (
	// postAuthorDID is the repo a postv2 lives in: the AUTHOR's, never the
	// community's.
	postAuthorDID = "did:plc:z72i7hdynmk6r22z27h6tvur"

	// otherGroupIRI is a second FOLLOWED community — the announcer in the
	// negative control. Following it is exactly what makes it dangerous:
	// nothing but this binding stops it announcing votes at other
	// communities' content.
	otherGroupIRI = "https://lemmy.world/c/rust"

	subjectRootPost = "https://lemmy.world/post/101"
)

// bridgePostV2 places a postv2 subject in the AUTHOR's repo and records the
// community the post names, the way a real materialization leaves it.
func bridgePostV2(t *testing.T, objects store.APObjects, records *fakeRecords, apID, rkey, communityDID string) {
	t.Helper()
	bridgeSubjectAs(t, objects, apID, rkey, postAuthorDID, materialize.CollectionPostV2)
	records.put(postAuthorDID, materialize.CollectionPostV2, rkey, map[string]any{
		"$type":     materialize.CollectionPostV2,
		"community": communityDID,
		"createdAt": "2026-07-01T12:00:00.000Z",
		"title":     "a bridged post",
	})
}

// TestAnnouncedVoteOnPostV2Counts (V1): the announcing community owns the
// post, so its announced Like counts — even though the post's repo is the
// author's and matches no community DID at all.
func TestAnnouncedVoteOnPostV2Counts(t *testing.T) {
	database := testDB(t)
	agg, objects, records := testAggregatorWithRecords(t, database)
	followCommunity(t, database, testGroupIRI, testDID)
	bridgePostV2(t, objects, records, subjectPost, "3jzfcijpj2z2a", testDID)
	ctx := context.Background()

	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), testGroupIRI))

	up, down, found := counts(t, database, subjectPost)
	require.True(t, found,
		"an announced vote on a postv2 the announcer owns must count; the post lives in the "+
			"AUTHOR's repo, so binding by repo DID drops every legitimate vote in the community")
	assert.Equal(t, 1, up)
	assert.Equal(t, 0, down)
}

// TestAnnouncedCommentVoteOnPostV2ThreadCounts (V2): the comment's thread
// root is a postv2 in the author's repo. reply.root no longer names a
// community repo, so the root's OWN community must be recovered.
func TestAnnouncedCommentVoteOnPostV2ThreadCounts(t *testing.T) {
	database := testDB(t)
	agg, objects, records := testAggregatorWithRecords(t, database)
	followCommunity(t, database, testGroupIRI, testDID)
	ctx := context.Background()

	// The thread root: a postv2 in the author's repo, naming the community.
	bridgePostV2(t, objects, records, subjectRootPost, "3jzfcijpj2z2a", testDID)

	// The comment: in its own author's repo, rooted at that postv2.
	bridgeSubjectAs(t, objects, subjectComment, "3jzfcijpj2z3a",
		commentAuthorDID, materialize.CollectionComment)
	records.put(commentAuthorDID, materialize.CollectionComment, "3jzfcijpj2z3a", map[string]any{
		"reply": map[string]any{
			"root": map[string]any{
				"uri": "at://" + postAuthorDID + "/" + materialize.CollectionPostV2 + "/3jzfcijpj2z2a",
				"cid": testCID,
			},
		},
	})

	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectComment), testGroupIRI))

	up, down, found := counts(t, database, subjectComment)
	require.True(t, found,
		"a comment whose thread root is a postv2 in the author's repo must still bind to the "+
			"root post's community; reply.root's repo DID is no longer that community")
	assert.Equal(t, 1, up)
	assert.Equal(t, 0, down)
}

// TestAnnouncedVoteOnPostV2ByOtherCommunityDropped (V3) is the negative
// control that keeps V1's fix honest: recovering the community must not
// degenerate into accepting any followed announcer. A DIFFERENT followed
// community announcing at this post is exactly the attack the binding
// exists for — one malicious followed community skewing another's scores.
//
// It has always passed — before the flip because every postv2 subject was
// dropped, since it because the binding names the right owner — so its value
// is entirely as a regression guard: a binding that recovered the community
// but forgot to compare it, or that admitted any followed announcer, turns
// this red while V1 and V2 stay green.
func TestAnnouncedVoteOnPostV2ByOtherCommunityDropped(t *testing.T) {
	database := testDB(t)
	agg, objects, records := testAggregatorWithRecords(t, database)
	followCommunity(t, database, testGroupIRI, testDID)            // the post's community
	followCommunity(t, database, otherGroupIRI, otherCommunityDID) // a different followed community
	bridgePostV2(t, objects, records, subjectPost, "3jzfcijpj2z2a", testDID)
	ctx := context.Background()

	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), otherGroupIRI))

	_, _, found := counts(t, database, subjectPost)
	assert.False(t, found, "a followed community may not vote on a post it does not own")
	var events int
	require.NoError(t, database.QueryRow(`SELECT COUNT(*) FROM vote_events`).Scan(&events))
	assert.Zero(t, events, "a cross-community announced vote must not record an event")
}
