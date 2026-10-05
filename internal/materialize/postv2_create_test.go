package materialize

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// Fixture ids for the postv2 creation-semantics tests. Distinct from the
// shared pageID so each test owns its object graph.
const (
	imagePageID        = "https://lemmy.world/post/70001"
	statsPageID        = "https://lemmy.world/post/70003"
	namePageID         = "https://lemmy.world/post/70004"
	hijackAuthorPageID = "https://lemmy.world/post/70005"
	otherGroupID       = "https://lemmy.world/c/elsewhere"
	embedImageURL      = "https://lemmy.world/media/postv2-embed.png"
)

// embedImageBytes are UNIQUE to these tests: the shared pngBytes are also what
// the profile avatar/banner handler serves, so a blob-placement assertion
// keyed on them could not tell an embed blob apart from a profile blob (same
// bytes → same CID → same row). These bytes appear in exactly one blob.
var embedImageBytes = []byte("postv2-flip-embed-image-bytes-not-a-real-png")

// blobHolders returns every repo DID holding a blob with exactly these bytes.
// Deliberately a direct read of the blobs table rather than a walk of the
// record: WHERE the blob was stored is the behavior under test, and it must be
// observable independently of where the record ended up.
func blobHolders(t *testing.T, data []byte) []string {
	t.Helper()
	rows, err := testutil.DB(t).Query(`SELECT did FROM blobs WHERE bytes = $1 ORDER BY did`, data)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var dids []string
	for rows.Next() {
		var did string
		require.NoError(t, rows.Scan(&did))
		dids = append(dids, did)
	}
	require.NoError(t, rows.Err())
	return dids
}

// TestPostV2EmbedBlobsLandInAuthorRepo (B1): a post's embed media is a blob in
// the repo that HOLDS the record — the AUTHOR's, since the flip.
//
// A blob ref resolves against the repo its record lives in and nowhere else,
// so the two must not be separated: bytes stored under the community DID for
// a record the community does not carry are unresolvable for every consumer,
// while the community pays to host them. The fetch DID therefore has to
// follow the record's repo rather than the community the post names.
func TestPostV2EmbedBlobsLandInAuthorRepo(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	h.mux.HandleFunc("GET /media/postv2-embed.png", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(embedImageBytes)
	})
	ctx := context.Background()

	imagePost := page(imagePageID, personID, groupID, "an image post", "2026-07-08T10:00:00.000000Z")
	imagePost["attachment"] = []any{map[string]any{
		"type":      "Image",
		"href":      embedImageURL,
		"mediaType": "image/png",
		"name":      "a picture",
	}}

	_, err := h.m.MaterializePost(ctx, mustObject(t, imagePost), groupID)
	require.NoError(t, err)

	communityDID := testDIDFor("technology", "lemmy.world")
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")

	holders := blobHolders(t, embedImageBytes)
	require.NotEmpty(t, holders, "the embed image must have been fetched and stored as a blob")
	assert.Contains(t, holders, authorDID,
		"the embed blob must be stored under the AUTHOR's DID — a blob ref resolves against the "+
			"repo its record lives in, and postv2 lives in the author's repo")
	assert.NotContains(t, holders, communityDID,
		"the community must NOT hold bytes for a record it does not carry")
	assert.Len(t, holders, 1, "the embed image is stored exactly once, got holders %v", holders)
}

// pageAttributedToInline builds a Page whose attributedTo is an INLINE actor
// object rather than a bare IRI — the shape a remote instance controls
// completely. Lemmy sends a bare IRI; anything richer arriving on a content
// path is, by definition, unverified.
func pageAttributedToInline(id, authorIRI, inlineName, groupIRI, title, published string) map[string]any {
	return map[string]any{
		"type": "Page",
		"id":   id,
		"attributedTo": map[string]any{
			"type": "Person",
			"id":   authorIRI,
			"name": inlineName,
		},
		"audience":  groupIRI,
		"to":        []any{ap.PublicAudience},
		"name":      title,
		"source":    map[string]any{"content": "body text", "mediaType": "text/markdown"},
		"published": published,
	}
}

// TestPostV2DisplayNameComesFromFetchedActorNotInlineRef: originalAuthor is
// PROVENANCE — a claim the bridge publishes about who wrote this upstream —
// so every field in it must come from the fetched, authority-bound actor
// document, never from the inline attributedTo object embedded in the
// content. The inline object is authored by whoever sent the content, which
// on a federated content path is any remote instance: letting it set
// displayName lets a hostile sender attach an arbitrary name to a real
// account's provenance, in a field consumers render.
//
// The bridge already draws this line for the actor document itself
// (actorDoc's allowEmbedded gate refuses an embedded actor on content
// paths); originalAuthor must be built from that same fetched document.
func TestPostV2DisplayNameComesFromFetchedActorNotInlineRef(t *testing.T) {
	const inlineLie = "Fake Display Name"

	t.Run("fetched document has a name", func(t *testing.T) {
		h := newHarness(t)
		h.serveLemmyWorldFixtures() // person_lemmy_world.json carries name "Surprised Neelix"
		ctx := context.Background()

		post := pageAttributedToInline(namePageID, personID, inlineLie, groupID,
			"a post with a lying inline author", "2026-07-08T13:00:00.000000Z")
		_, err := h.m.MaterializePost(ctx, mustObject(t, post), groupID)
		require.NoError(t, err)

		record := h.recordFor(t, namePageID)
		originalAuthor, ok := record["originalAuthor"].(map[string]any)
		require.True(t, ok)
		assert.Equal(t, "Surprised Neelix", originalAuthor["displayName"],
			"displayName must be the FETCHED actor document's name")
		assert.NotEqual(t, inlineLie, originalAuthor["displayName"],
			"the inline attributedTo object is attacker-controlled and must never reach provenance")
	})

	t.Run("fetched document has no name", func(t *testing.T) {
		h := newHarness(t)
		h.serveFixture("/c/technology", "group_lemmy_world.json")
		h.serveObject("/u/nameless", person("https://lemmy.world/u/nameless", "nameless", nil))
		ctx := context.Background()

		post := pageAttributedToInline(namePageID, "https://lemmy.world/u/nameless", inlineLie,
			groupID, "a post by a nameless author", "2026-07-08T13:00:00.000000Z")
		_, err := h.m.MaterializePost(ctx, mustObject(t, post), groupID)
		require.NoError(t, err)

		record := h.recordFor(t, namePageID)
		originalAuthor, ok := record["originalAuthor"].(map[string]any)
		require.True(t, ok)
		assert.NotContains(t, originalAuthor, "displayName",
			"the actor document asserts no name, so provenance asserts none — an absent field "+
				"is not an invitation to substitute the inline one")
	})
}

// TestPostV2CommunityIsImmutableAcrossUpdates (B2): a post's community is
// decided once, when it is first materialized, and a later delivery cannot
// move it. postv2.community is immutable to Coves (its consumers DISCARD any
// update event that changes it), and a post's community is what authorizes
// announced deletes and votes against it.
//
// The community a delivery speaks for is fixed by the delivery itself — the
// community that announced it — never read off the edited audience. So when C
// re-delivers a post stored in D, even one whose audience now names C, it is
// not C's post: the delivery is refused, and the stored record, its CID and
// its mapping are left exactly as they were. That holds for a postv2, for a
// postv2 whose mapping predates the community_did column, and for a post
// written in the deprecated era into D's own repo.
func TestPostV2CommunityIsImmutableAcrossUpdates(t *testing.T) {
	const postID = "https://lemmy.zip/post/92001"
	const title = "a post in D"
	publishedAt := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	const published = "2026-10-01T09:00:00.000000Z"
	communityD := testDIDFor("elsewhere", "lemmy.world")

	storedInD := func(t *testing.T, h *harness) {
		t.Helper()
		_, err := h.m.MaterializePost(context.Background(),
			mustObject(t, page(postID, bindingAuthor, bindingCommunityD, title, published)), bindingCommunityD)
		require.NoError(t, err)
	}
	cases := []struct {
		name    string
		prepare func(t *testing.T, h *harness)
	}{
		{name: "postv2 stored in D", prepare: storedInD},
		{name: "postv2 stored in D with no community_did column", prepare: func(t *testing.T, h *harness) {
			storedInD(t, h)
			clearCommunityColumn(t, postID)
		}},
		{name: "legacy community.post in D's repo", prepare: func(t *testing.T, h *harness) {
			ctx := context.Background()
			community, err := h.m.EnsureCommunity(ctx, &ap.Object{ID: bindingCommunityD})
			require.NoError(t, err)
			author, err := h.m.EnsureActor(ctx, &ap.Object{ID: bindingAuthor})
			require.NoError(t, err)
			rkey, err := recordRKey(mustObject(t, page(postID, bindingAuthor, bindingCommunityD, title, published)))
			require.NoError(t, err)
			commit, err := h.manager.PutRecord(ctx, community.DID, CollectionPost, rkey, map[string]any{
				"$type":     CollectionPost,
				"community": community.DID,
				"author":    author.DID,
				"createdAt": recordDatetime(publishedAt),
				"title":     title,
			})
			require.NoError(t, err)
			// Pre-016: the legacy era never filled community_did.
			_, err = h.objects.PutMapping(ctx, store.APObjectMapping{
				APID:           postID,
				APType:         "Page",
				OriginInstance: "lemmy.zip",
				Origin:         store.OriginFediverse,
				DID:            community.DID,
				AuthorDID:      author.DID,
				Collection:     CollectionPost,
				RKey:           rkey,
				CID:            commit.RecordCID,
				PublishedAt:    &publishedAt,
			})
			require.NoError(t, err)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			serveBindingWorld(h)
			ensureCommunities(t, h, bindingCommunityC, bindingCommunityD)
			ctx := context.Background()
			tc.prepare(t, h)

			before, err := h.objects.GetByAPID(ctx, postID)
			require.NoError(t, err)
			recordBefore, cidBefore, err := h.manager.GetRecord(ctx, before.DID, before.Collection, before.RKey)
			require.NoError(t, err)
			require.Equal(t, communityD, recordBefore["community"], "precondition: the post is stored in D")
			stateBefore := captureContentState(t, h)

			// C re-delivers it, edited, with an audience retargeted at C.
			retargeted := mustObject(t, page(postID, bindingAuthor, bindingCommunityC, title, published))
			retargeted.Source = &ap.Source{Content: "edited body text", MediaType: "text/markdown"}
			res, err := h.m.HandleUpdate(ctx, retargeted, bindingCommunityC)
			require.Error(t, err, "C may not re-deliver a post that is stored in D")
			assert.Nil(t, res)
			assert.True(t, IsSkip(err), "a re-delivery bound to another community is a skip, got %v", err)

			after, err := h.objects.GetByAPID(ctx, postID)
			require.NoError(t, err)
			assert.Equal(t, before.DID, after.DID, "the record must not be moved")
			assert.Equal(t, before.Collection, after.Collection)
			assert.Equal(t, before.RKey, after.RKey)
			assert.Equal(t, before.CID, after.CID, "the mapping must still name the stored version")
			assert.Equal(t, before.CommunityDID, after.CommunityDID, "the community binding must not change")

			recordAfter, cidAfter, err := h.manager.GetRecord(ctx, after.DID, after.Collection, after.RKey)
			require.NoError(t, err)
			assert.Equal(t, cidBefore, cidAfter, "the stored record must not be re-committed")
			assert.Equal(t, recordBefore, recordAfter)
			assert.Equal(t, communityD, recordAfter["community"])

			resolved, err := CommunityDIDOf(ctx, h.manager, after)
			require.NoError(t, err)
			assert.Equal(t, communityD, resolved, "the post still belongs to D")

			assert.Equal(t, stateBefore, captureContentState(t, h),
				"a refused re-delivery writes nothing: no record, no acceptance, no mapping change")
		})
	}
}

// TestPostV2EditCarriesBridgedStatsForward (B3): the vote refresher stamps
// bridgedStats onto the record; a later Lemmy EDIT rebuilds from AP data,
// which never carries stats. commitRecord's carry-forward must therefore
// cover postv2 as it covers the collection it replaced, or every edit
// silently drops the counts — and mints a needless firehose event doing it,
// breaking idempotent re-ingest.
//
// The record is read back through its MAPPING rather than at a hard-coded
// path, which keeps the two halves independent: the collection/DID assertions
// pin WHERE the edited post lives, and the bridgedStats assertion is
// exercised whichever repo and collection that turns out to be — so a
// carry-forward regression surfaces on its own line rather than hiding behind
// a placement failure.
func TestPostV2EditCarriesBridgedStatsForward(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	original := page(statsPageID, personID, groupID, "a post with votes", "2026-07-08T12:00:00.000000Z")
	_, err := h.m.MaterializePost(ctx, mustObject(t, original), groupID)
	require.NoError(t, err)

	mapping, err := h.objects.GetByAPID(ctx, statsPageID)
	require.NoError(t, err)
	_, err = h.m.SetBridgedStats(ctx, mapping, 40, 2, statsAsOf)
	require.NoError(t, err)

	// The upstream edit changes the body only.
	edited := mustObject(t, page(statsPageID, personID, groupID, "a post with votes", "2026-07-08T12:00:00.000000Z"))
	edited.Source = &ap.Source{Content: "edited body text", MediaType: "text/markdown"}
	res, err := h.m.HandleUpdate(ctx, edited, groupID)
	require.NoError(t, err)
	require.False(t, res.NoOp, "an edited body is a real commit")

	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
	after, err := h.objects.GetByAPID(ctx, statsPageID)
	require.NoError(t, err)
	assert.Equal(t, testPostV2Collection, after.Collection, "the edited post is a postv2 record")
	assert.Equal(t, authorDID, after.DID, "the edited postv2 record stays in the author's repo")

	record, _, err := h.manager.GetRecord(ctx, after.DID, after.Collection, after.RKey)
	require.NoError(t, err)
	assert.Equal(t, "edited body text", record["content"], "the edit applied")

	stats, ok := record["bridgedStats"].(map[string]any)
	require.True(t, ok,
		"the edit dropped bridgedStats: commitRecord's carryForward gate must include %s",
		testPostV2Collection)
	assert.EqualValues(t, 40, stats["upvotes"])
	assert.EqualValues(t, 2, stats["downvotes"])
	assert.Equal(t, recordDatetimeMicros(statsAsOf), stats["asOf"])
}

// TestPostV2AuthorIsImmutableAcrossUpdates (F5): the repo a postv2 lives in IS
// its authorship claim, so an edit may never move it.
//
// `attributedTo` on an updated Page is attacker-influenced content: whoever
// can deliver an Update for a post's AP id proposes it. If a rebuild honoured
// a changed value, one delivery would write a record into an UNRELATED
// bridged user's repo — the strongest authorship statement atproto has — and
// leave the real author's copy behind. The stored mapping is the authority on
// who authored a bridged object, exactly as the stored record is the authority
// on its community (B2).
func TestPostV2AuthorIsImmutableAcrossUpdates(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	h.serveObject("/u/impostor", person("https://lemmy.world/u/impostor", "impostor", nil))
	ctx := context.Background()

	original := page(hijackAuthorPageID, personID, groupID, "a post by its real author",
		"2026-07-08T14:00:00.000000Z")
	_, err := h.m.MaterializePost(ctx, mustObject(t, original), groupID)
	require.NoError(t, err)

	before, err := h.objects.GetByAPID(ctx, hijackAuthorPageID)
	require.NoError(t, err)

	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
	impostorDID := testDIDFor("impostor", "lemmy.world")
	require.NotEqual(t, authorDID, impostorDID)
	require.Equal(t, authorDID, before.DID, "precondition: the post is in its real author's repo")

	// The edit now claims a different author.
	hijacked := page(hijackAuthorPageID, "https://lemmy.world/u/impostor", groupID,
		"a post by its real author", "2026-07-08T14:00:00.000000Z")
	_, err = h.m.HandleUpdate(ctx, mustObject(t, hijacked), groupID)
	require.NoError(t, err,
		"a retargeted attributedTo must not error — it must simply not retarget the record")

	after, err := h.objects.GetByAPID(ctx, hijackAuthorPageID)
	require.NoError(t, err)
	assert.Equal(t, authorDID, after.DID,
		"the record must stay in the ORIGINAL author's repo: the repo is the authorship claim, "+
			"and an edit that moved it would write into an unrelated user's repo")
	assert.Equal(t, authorDID, after.AuthorDID, "the mapping's author must not be reassigned by an edit")

	record, _, err := h.manager.GetRecord(ctx, after.DID, after.Collection, after.RKey)
	require.NoError(t, err)
	originalAuthor, ok := record["originalAuthor"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, personID, originalAuthor["apId"],
		"originalAuthor is provenance about who wrote it upstream; an edit may not rewrite it")

	// And nothing was planted in the impostor's repo.
	_, _, err = h.manager.GetRecord(ctx, impostorDID, testPostV2Collection, after.RKey)
	assert.True(t, errors.IsNotFound(err),
		"no postv2 may appear in the claimed author's repo (err=%v)", err)
}

// TestCommentCommunityIsImmutableAcrossUpdates (B5): a comment's community is
// fixed by the thread it was posted in, and a later delivery may not move it.
//
// This is a SECURITY property rather than a tidiness one. A comment's
// community is what authorizes announced deletes and binds announced votes:
// whoever owns the comment may moderate it. So when C delivers an Update for a
// comment stored in D's thread — re-parented into a post in C, addressed to C —
// the comment is still D's, C cannot speak for it, and the delivery is refused
// with the record, its CID and its mapping unchanged. A comment whose mapping
// predates the community_did column is just as much D's.
func TestCommentCommunityIsImmutableAcrossUpdates(t *testing.T) {
	const (
		postInC   = "https://lemmy.zip/post/95001"
		postInD   = "https://lemmy.zip/post/95002"
		commentID = "https://lemmy.zip/comment/95003"
		published = "2026-10-01T16:02:00.000000Z"
	)
	communityD := testDIDFor("elsewhere", "lemmy.world")

	cases := []struct {
		name         string
		legacyColumn bool
	}{
		{name: "comment in D"},
		{name: "comment in D with no community_did column", legacyColumn: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			serveBindingWorld(h)
			ensureCommunities(t, h, bindingCommunityC, bindingCommunityD)
			ctx := context.Background()

			// A thread in C, and a thread in D holding the comment.
			threadC := page(postInC, bindingAuthor, bindingCommunityC, "thread root in C", "2026-10-01T16:00:00.000000Z")
			h.serveObject("/post/95001", threadC)
			_, err := h.m.MaterializePost(ctx, mustObject(t, threadC), bindingCommunityC)
			require.NoError(t, err)
			threadD := page(postInD, bindingAuthor, bindingCommunityD, "thread root in D", "2026-10-01T16:01:00.000000Z")
			h.serveObject("/post/95002", threadD)
			_, err = h.m.MaterializePost(ctx, mustObject(t, threadD), bindingCommunityD)
			require.NoError(t, err)
			comment := noteIn(commentID, bindingReplier, postInD, bindingCommunityD, "a comment in D", published)
			h.serveObject("/comment/95003", comment)
			_, err = h.m.MaterializeComment(ctx, mustObject(t, comment), bindingCommunityD)
			require.NoError(t, err)
			if tc.legacyColumn {
				clearCommunityColumn(t, commentID)
			}

			before, err := h.objects.GetByAPID(ctx, commentID)
			require.NoError(t, err)
			resolvedBefore, err := CommunityDIDOf(ctx, h.manager, before)
			require.NoError(t, err)
			require.Equal(t, communityD, resolvedBefore, "precondition: the comment belongs to D")
			recordBefore, cidBefore, err := h.manager.GetRecord(ctx, before.DID, before.Collection, before.RKey)
			require.NoError(t, err)
			stateBefore := captureContentState(t, h)

			// C delivers an edit re-parenting the comment into C's thread.
			hijacked := noteIn(commentID, bindingReplier, postInC, bindingCommunityC,
				"a comment in D (edited)", published)
			res, err := h.m.HandleUpdate(ctx, mustObject(t, hijacked), bindingCommunityC)
			require.Error(t, err, "C may not re-deliver a comment that is stored in D")
			assert.Nil(t, res)
			assert.True(t, IsSkip(err), "a re-delivery bound to another community is a skip, got %v", err)

			after, err := h.objects.GetByAPID(ctx, commentID)
			require.NoError(t, err)
			assert.Equal(t, before.DID, after.DID)
			assert.Equal(t, before.RKey, after.RKey)
			assert.Equal(t, before.CID, after.CID, "the mapping must still name the stored version")
			assert.Equal(t, before.CommunityDID, after.CommunityDID, "the community binding must not change")

			recordAfter, cidAfter, err := h.manager.GetRecord(ctx, after.DID, after.Collection, after.RKey)
			require.NoError(t, err)
			assert.Equal(t, cidBefore, cidAfter, "the stored record must not be re-committed")
			assert.Equal(t, recordBefore, recordAfter, "reply refs and content must be untouched")

			// CommunityDIDOf is what ingest's announced-delete authorization and
			// votes' announced-vote binding both consult.
			resolved, err := CommunityDIDOf(ctx, h.manager, after)
			require.NoError(t, err)
			assert.Equal(t, communityD, resolved,
				"the community that may moderate this comment must still be D")

			assert.Equal(t, stateBefore, captureContentState(t, h), "a refused re-delivery writes nothing")
		})
	}
}
