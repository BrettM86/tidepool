package materialize

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/testutil"
)

// Fixture ids for the postv2 creation-semantics tests. Distinct from the
// shared pageID so each test owns its object graph.
const (
	imagePageID   = "https://lemmy.world/post/70001"
	hijackPageID  = "https://lemmy.world/post/70002"
	statsPageID   = "https://lemmy.world/post/70003"
	namePageID    = "https://lemmy.world/post/70004"
	otherGroupID  = "https://lemmy.world/c/elsewhere"
	embedImageURL = "https://lemmy.world/media/postv2-embed.png"
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
// the repo that HOLDS the record, and after the flip that repo is the
// AUTHOR's. Today buildPostEmbed fetches every embed blob under the COMMUNITY
// DID (posts.go fetchBlob calls) — leaving the community hosting bytes for a
// record it does not carry, which no consumer can resolve: a blob ref is
// resolved against the repo the record lives in.
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

	_, err := h.m.MaterializePost(ctx, mustObject(t, imagePost))
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
		_, err := h.m.MaterializePost(ctx, mustObject(t, post))
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
		_, err := h.m.MaterializePost(ctx, mustObject(t, post))
		require.NoError(t, err)

		record := h.recordFor(t, namePageID)
		originalAuthor, ok := record["originalAuthor"].(map[string]any)
		require.True(t, ok)
		assert.NotContains(t, originalAuthor, "displayName",
			"the actor document asserts no name, so provenance asserts none — an absent field "+
				"is not an invitation to substitute the inline one")
	})
}

// TestPostV2CommunityIsImmutableAcrossUpdates (B2): postv2.community is
// immutable. Coves' consumers DISCARD any update event that changes it, so a
// bridge that re-derived the community from a hostile or merely edited
// `audience` would emit an event Coves throws away — silently freezing the
// post at its pre-edit version — and, worse, would relocate the record.
// The stored community must be carried forward from the stored record.
func TestPostV2CommunityIsImmutableAcrossUpdates(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	h.serveObject("/c/elsewhere", group(otherGroupID, "elsewhere", nil))
	ctx := context.Background()

	original := page(hijackPageID, personID, groupID, "a post in technology", "2026-07-08T11:00:00.000000Z")
	_, err := h.m.MaterializePost(ctx, mustObject(t, original))
	require.NoError(t, err)

	before, err := h.objects.GetByAPID(ctx, hijackPageID)
	require.NoError(t, err)

	communityDID := testDIDFor("technology", "lemmy.world")
	otherCommunityDID := testDIDFor("elsewhere", "lemmy.world")
	require.NotEqual(t, communityDID, otherCommunityDID)

	// The upstream edit now names a DIFFERENT group.
	hijacked := page(hijackPageID, personID, otherGroupID, "a post in technology", "2026-07-08T11:00:00.000000Z")
	_, err = h.m.HandleUpdate(ctx, mustObject(t, hijacked))
	require.NoError(t, err, "a retargeted audience must not error — it must simply not retarget the record")

	after, err := h.objects.GetByAPID(ctx, hijackPageID)
	require.NoError(t, err)
	assert.Equal(t, before.DID, after.DID, "the record must not be MOVED by an audience change")
	assert.Equal(t, before.RKey, after.RKey)

	record, _, err := h.manager.GetRecord(ctx, after.DID, after.Collection, after.RKey)
	require.NoError(t, err)
	assert.Equal(t, communityDID, record["community"],
		"community is immutable: it must be carried forward from the stored record, "+
			"NOT re-derived from the changed audience (Coves discards any event that changes it)")

	// And nothing may have been planted in the hijacked community's repo.
	_, _, err = h.manager.GetRecord(ctx, otherCommunityDID, testPostV2Collection, after.RKey)
	assert.True(t, errors.IsNotFound(err),
		"no postv2 record may appear in the retargeted community's repo (err=%v)", err)
	_, _, err = h.manager.GetRecord(ctx, otherCommunityDID, testLegacyPostCollection, after.RKey)
	assert.True(t, errors.IsNotFound(err),
		"no post record of any era may appear in the retargeted community's repo (err=%v)", err)
}

// TestPostV2EditCarriesBridgedStatsForward (B3): the vote refresher stamps
// bridgedStats onto the record; a later Lemmy EDIT rebuilds from AP data,
// which never carries stats. commitRecord's carryForward gate currently keys
// off CollectionPost/CollectionComment — once posts move to postv2 that gate
// must include the new collection, or every edit silently drops the counts
// (and mints a needless firehose event, breaking idempotent re-ingest).
//
// The record is read back through its MAPPING rather than at a hard-coded
// path, so the bridgedStats assertion is exercised whether or not the flip has
// landed: pre-flip the collection/DID assertions fail, post-flip-without-the-
// gate the bridgedStats assertion fails.
func TestPostV2EditCarriesBridgedStatsForward(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	original := page(statsPageID, personID, groupID, "a post with votes", "2026-07-08T12:00:00.000000Z")
	_, err := h.m.MaterializePost(ctx, mustObject(t, original))
	require.NoError(t, err)

	mapping, err := h.objects.GetByAPID(ctx, statsPageID)
	require.NoError(t, err)
	_, err = h.m.SetBridgedStats(ctx, mapping, 40, 2, statsAsOf)
	require.NoError(t, err)

	// The upstream edit changes the body only.
	edited := mustObject(t, page(statsPageID, personID, groupID, "a post with votes", "2026-07-08T12:00:00.000000Z"))
	edited.Source = &ap.Source{Content: "edited body text", MediaType: "text/markdown"}
	res, err := h.m.HandleUpdate(ctx, edited)
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
