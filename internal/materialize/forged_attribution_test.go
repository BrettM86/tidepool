package materialize

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

// Forged attribution. Since the postv2 flip the repo a record lands in IS the
// authorship claim — the record is signed by that repo's key — so `attributedTo`
// on delivered content decides whose signature ends up on it. These tests pin
// the two rules that keeps honest: an already-materialized object never moves
// repos, and an object may only ever attribute itself to an actor on its own
// authority.

// TestCommentEditCannotReattributeAuthor: an Update{Note} that names a
// different author must not relocate the comment. The mapping is the authority
// on who wrote a bridged object — exactly as MaterializePost already treats it
// — because honouring the edit would sign the record with an unrelated user's
// repo key and strand the original copy live in the first author's repo.
func TestCommentEditCannotReattributeAuthor(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	h.serveObject("/u/alice", person("https://lemmy.world/u/alice", "alice", nil))
	h.serveObject("/u/victim", person("https://lemmy.world/u/victim", "victim", nil))
	ctx := context.Background()

	_, err := h.m.MaterializePost(ctx, loadFixtureObject(t, "page_lemmy_world.json"))
	require.NoError(t, err)

	const commentID = "https://lemmy.world/comment/80001"
	original := note(commentID, "https://lemmy.world/u/alice", pageID,
		"alice wrote this", "2026-07-08T16:00:00.000000Z")
	_, err = h.m.MaterializeComment(ctx, objectFromMap(t, original))
	require.NoError(t, err)

	aliceDID := testDIDFor("alice", "lemmy.world")
	victimDID := testDIDFor("victim", "lemmy.world")
	require.NotEqual(t, aliceDID, victimDID)

	before, err := h.objects.GetByAPID(ctx, commentID)
	require.NoError(t, err)
	require.Equal(t, aliceDID, before.DID, "precondition: the comment lives in alice's repo")
	require.Equal(t, aliceDID, before.AuthorDID)

	// The edit re-attributes the comment to another user on the SAME instance,
	// so the authority check below cannot be what refuses it.
	forged := note(commentID, "https://lemmy.world/u/victim", pageID,
		"alice wrote this (edited)", "2026-07-08T16:00:00.000000Z")
	_, err = h.m.HandleUpdate(ctx, objectFromMap(t, forged))
	require.NoError(t, err, "a re-attributed edit must not error — it must simply not re-attribute")

	after, err := h.objects.GetByAPID(ctx, commentID)
	require.NoError(t, err)
	assert.Equal(t, aliceDID, after.DID,
		"the comment must stay in the repo that authored it: the repo IS the authorship claim, so "+
			"moving it signs the victim's key over content they never wrote")
	assert.Equal(t, aliceDID, after.AuthorDID, "the stored author is fixed at first materialization")
	assert.Equal(t, before.ATURI, after.ATURI,
		"re-pointing the mapping would strand the original record live in alice's repo")

	_, _, err = h.manager.GetRecord(ctx, aliceDID, CollectionComment, after.RKey)
	require.NoError(t, err, "the comment must still be readable where it was written")

	entries, err := h.manager.ListRecords(ctx, victimDID)
	require.NoError(t, err)
	for _, entry := range entries {
		assert.NotEqual(t, CollectionComment, entry.Collection,
			"no comment may be written into the named victim's repo (rkey %s)", entry.Rkey)
	}
}

// TestCommentCrossAuthorityAttributionRefused: a Note served by one instance
// may not attribute itself to a user on another. Lemmy enforces the same rule
// on its own inbound path (verify_domains_match), so genuine traffic never
// trips this — but without it any instance can name any bridged user as the
// author of anything it delivers.
func TestCommentCrossAuthorityAttributionRefused(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	// The thread root is real and materialized, so a refusal below cannot be
	// the missing-parent protocol talking.
	_, err := h.m.MaterializePost(ctx, loadFixtureObject(t, "page_lemmy_world.json"))
	require.NoError(t, err)

	const victimIRI = "https://lemmy.world/u/victim"
	const forgedID = "https://evil.example/comment/1"
	h.serveObject("/u/victim", person(victimIRI, "victim", nil))

	res, err := h.m.MaterializeComment(ctx, objectFromMap(t,
		note(forgedID, victimIRI, pageID, "words the victim never wrote", "2026-07-08T16:10:00.000000Z")))
	require.Nil(t, res)
	require.Error(t, err)
	assert.True(t, IsSkip(err), "cross-authority attribution must be a skip, got %v", err)

	assert.Equal(t, 0, countMappings(t, h, forgedID))
	_, err = h.actors.GetByAPActorID(ctx, victimIRI)
	assert.True(t, errors.IsNotFound(err),
		"the refusal must precede the mint: naming a victim must not even bridge them")
}

// TestPostCrossAuthorityAttributionRefused is the same rule on the post path.
// A postv2 carries no `author` field at all — its repo is the whole claim — so
// an accepted forgery here is indistinguishable from a post the victim wrote.
func TestPostCrossAuthorityAttributionRefused(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	const forgedID = "https://evil.example/post/1"
	res, err := h.m.MaterializePost(ctx, mustObject(t,
		page(forgedID, personID, groupID, "not their post", "2026-07-08T16:20:00.000000Z")))
	require.Nil(t, res)
	require.Error(t, err)
	assert.True(t, IsSkip(err), "cross-authority attribution must be a skip, got %v", err)

	assert.Equal(t, 0, countMappings(t, h, forgedID))
	_, err = h.actors.GetByAPActorID(ctx, personID)
	assert.True(t, errors.IsNotFound(err),
		"the refusal must precede the mint: naming a victim must not even bridge them")
	_, err = h.communities.GetByAPGroupID(ctx, groupID)
	assert.True(t, errors.IsNotFound(err),
		"forged content must not bridge the community it claims either")
}
