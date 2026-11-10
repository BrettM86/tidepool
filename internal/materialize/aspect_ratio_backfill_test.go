package materialize

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/blobwithhold"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// backfillPost is one Page the back-fill test materializes: its attachments
// are served under https://lemmy.world/media/.
type backfillPost struct {
	id          string
	attachments map[string]servedMedia
	order       []string
	// legacy strips every aspectRatio after materializing, as a record
	// written before the bridge recorded them.
	legacy bool
}

// materializeBackfillPost materializes post and, for a legacy post, rewrites
// its record without aspect ratios, keeping the mapping and acceptance pinned
// to the rewritten version as the bridge would have left them.
func materializeBackfillPost(t *testing.T, h *harness, post backfillPost) {
	t.Helper()
	ctx := context.Background()
	var attachments []any
	for _, path := range post.order {
		served := post.attachments[path]
		data := served.data(t)
		h.mux.HandleFunc("GET /media/"+path, func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", served.contentType)
			_, _ = w.Write(data)
		})
		attachments = append(attachments, map[string]any{
			"type": "Image", "href": "https://lemmy.world/media/" + path, "name": "alt for " + path,
		})
	}
	object := page(post.id, personID, groupID, "post "+post.id, "2026-11-01T10:00:00.000000Z")
	if attachments != nil {
		object["attachment"] = attachments
	} else {
		object["url"] = "https://example.com/article"
	}
	_, err := h.m.MaterializePost(ctx, mustObject(t, object), groupID)
	require.NoError(t, err)
	if !post.legacy {
		return
	}

	mapping, err := h.objects.GetByAPID(ctx, post.id)
	require.NoError(t, err)
	record, _, err := h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	require.NoError(t, err)
	for _, image := range embedImages(t, record) {
		delete(image, "aspectRatio")
	}
	res, err := h.manager.PutRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey, record)
	require.NoError(t, err)
	mapping.CID = res.RecordCID
	_, err = h.objects.PutMapping(ctx, *mapping)
	require.NoError(t, err)
	require.NoError(t, h.m.repinAcceptance(ctx, mapping, res.RecordCID))
}

// embedImages returns the image entries of a record's images embed.
func embedImages(t *testing.T, record map[string]any) []map[string]any {
	t.Helper()
	embed, ok := record["embed"].(map[string]any)
	require.True(t, ok, "record has no embed")
	require.Equal(t, "social.coves.embed.images", embed["$type"])
	var images []map[string]any
	for _, entry := range embed["images"].([]any) {
		images = append(images, entry.(map[string]any))
	}
	return images
}

// postState is a post's record, its record CID, and the CIDs its mapping and
// acceptance pin.
type postState struct {
	record        map[string]any
	recordCID     string
	mappingCID    string
	acceptanceCID string
}

func readPostState(t *testing.T, h *harness, apID string) postState {
	t.Helper()
	ctx := context.Background()
	mapping, err := h.objects.GetByAPID(ctx, apID)
	require.NoError(t, err)
	record, recordCID, err := h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	require.NoError(t, err)
	state := postState{record: record, recordCID: recordCID, mappingCID: mapping.CID}
	communityDID := testDIDFor("technology", "lemmy.world")
	acceptance, _, err := h.manager.GetRecord(ctx, communityDID, testAcceptanceCollection, acceptanceFor(t, mapping.DID, mapping.RKey))
	require.NoError(t, err)
	subject := acceptance["subject"].(map[string]any)
	state.acceptanceCID = subject["cid"].(string)
	return state
}

func aspectRatioOf(image map[string]any) map[string]any {
	ratio, _ := image["aspectRatio"].(map[string]any)
	return ratio
}

func blobCIDOf(t *testing.T, image map[string]any) string {
	t.Helper()
	blob, ok := image["image"].(atdata.Blob)
	require.True(t, ok, "image entry is not a blob: %T", image["image"])
	return cid.Cid(blob.Ref).String()
}

// TestBackfillImageAspectRatios: the one-time back-fill gives every live
// bridged post image without an aspectRatio the ratio of its stored bytes,
// in one compare-and-swap commit per post that moves the mapping and the
// acceptance to the new version, across pages and for legacy-collection
// posts too. Images already carrying a ratio, images whose bytes are
// unreadable or withheld, and posts without an images embed are left alone.
// A dry run reports the same counts and writes nothing, and a second run
// finds nothing to do but heals an acceptance left on an old version.
func TestBackfillImageAspectRatios(t *testing.T) {
	const (
		tallID     = "https://lemmy.world/post/70401"
		galleryID  = "https://lemmy.world/post/70402"
		currentID  = "https://lemmy.world/post/70403"
		linkID     = "https://lemmy.world/post/70404"
		withheldID = "https://lemmy.world/post/70405"
		deletedID  = "https://lemmy.world/post/70406"
	)
	pageSize := aspectRatioBackfillPageSize
	aspectRatioBackfillPageSize = 2
	t.Cleanup(func() { aspectRatioBackfillPageSize = pageSize })

	h := newHarness(t)
	ctx := context.Background()
	h.serveLemmyWorldFixtures()

	posts := []backfillPost{
		{id: tallID, legacy: true, order: []string{"tall.png"},
			attachments: map[string]servedMedia{"tall.png": {"image/png", func(t *testing.T) []byte { return pngFixture(t, 300, 900) }}}},
		{id: galleryID, legacy: true, order: []string{"camera.jpg", "broken.png"},
			attachments: map[string]servedMedia{
				"camera.jpg": rotatedJPEG(exifFixture{orientation: 6}),
				"broken.png": {"image/png", func(t *testing.T) []byte { return []byte("backfill: not really a png") }},
			}},
		{id: currentID, order: []string{"current.png"},
			attachments: map[string]servedMedia{"current.png": {"image/png", func(t *testing.T) []byte { return pngFixture(t, 70, 20) }}}},
		{id: linkID},
		{id: withheldID, legacy: true, order: []string{"withheld.png"},
			attachments: map[string]servedMedia{"withheld.png": {"image/png", func(t *testing.T) []byte { return pngFixture(t, 33, 77) }}}},
		{id: deletedID, legacy: true, order: []string{"deleted.png"},
			attachments: map[string]servedMedia{"deleted.png": {"image/png", func(t *testing.T) []byte { return pngFixture(t, 44, 88) }}}},
	}
	for _, post := range posts {
		materializeBackfillPost(t, h, post)
	}

	legacy := seedLegacyImagePost(t, h, pngFixture(t, 120, 30))

	withheldCID := blobCIDOf(t, embedImages(t, h.recordFor(t, withheldID))[0])
	operatorWithhold(t, blobwithhold.NewStore(testutil.DB(t)), withheldCID)
	deletedMapping, err := h.objects.GetByAPID(ctx, deletedID)
	require.NoError(t, err)
	require.NoError(t, h.m.HandleDeleteRecord(ctx, deletedID))

	before := map[string]postState{}
	for _, id := range []string{tallID, galleryID, currentID, linkID, withheldID} {
		before[id] = readPostState(t, h, id)
	}

	wantReport := AspectRatioBackfillReport{
		Updated:           3, // tall, gallery, legacy
		ImagesSet:         3, // tall.png, camera.jpg, the legacy image
		AlreadySet:        1, // current.png
		UnreadableImages:  2, // broken.png, withheld.png
		WithoutImageEmbed: 1, // the link post
	}

	dry, err := h.m.BackfillImageAspectRatios(ctx, true)
	require.NoError(t, err)
	wantDry := wantReport
	wantDry.DryRun = true
	assert.Equal(t, wantDry, *dry, "a dry run reports what a live run would do")
	for id, state := range before {
		assert.Equal(t, state, readPostState(t, h, id), "a dry run writes nothing to %s", id)
	}

	live, err := h.m.BackfillImageAspectRatios(ctx, false)
	require.NoError(t, err)
	assert.Equal(t, wantReport, *live)

	tall := readPostState(t, h, tallID)
	assert.NotEqual(t, before[tallID].recordCID, tall.recordCID)
	assert.Equal(t, map[string]any{"width": int64(300), "height": int64(900)}, aspectRatioOf(embedImages(t, tall.record)[0]))
	assert.Equal(t, tall.recordCID, tall.mappingCID, "the mapping moves to the new version")
	assert.Equal(t, tall.recordCID, tall.acceptanceCID, "the acceptance moves to the new version")
	assert.Equal(t, "alt for tall.png", embedImages(t, tall.record)[0]["alt"], "the rest of the image entry is kept")
	assert.Equal(t, before[tallID].record["title"], tall.record["title"])

	gallery := readPostState(t, h, galleryID)
	galleryImages := embedImages(t, gallery.record)
	assert.Equal(t, map[string]any{"width": int64(40), "height": int64(160)}, aspectRatioOf(galleryImages[0]))
	assert.Nil(t, aspectRatioOf(galleryImages[1]), "an unreadable image stays without a ratio")
	assert.Equal(t, gallery.recordCID, gallery.mappingCID)
	assert.Equal(t, gallery.recordCID, gallery.acceptanceCID)

	for _, id := range []string{currentID, linkID, withheldID} {
		assert.Equal(t, before[id], readPostState(t, h, id), "%s needed no commit", id)
	}

	legacyRecord, legacyCID, err := h.manager.GetRecord(ctx, legacy.DID, legacy.Collection, legacy.RKey)
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"width": int64(120), "height": int64(30)}, aspectRatioOf(embedImages(t, legacyRecord)[0]),
		"a legacy-collection post gets its ratio")
	legacyMapping, err := h.objects.GetByAPID(ctx, legacy.APID)
	require.NoError(t, err)
	assert.Equal(t, legacyCID, legacyMapping.CID)

	// A run cut short between a post's commit and its repin leaves the
	// acceptance on the replaced version; the next run must heal it.
	tallMapping, err := h.objects.GetByAPID(ctx, tallID)
	require.NoError(t, err)
	require.NoError(t, h.m.repinAcceptance(ctx, tallMapping, before[tallID].recordCID))
	require.Equal(t, before[tallID].recordCID, readPostState(t, h, tallID).acceptanceCID, "precondition: a stale pin")
	// A live edit can also turn a post into one with no images embed, and
	// a late repin then leaves its pin stale; that post is healed too.
	linkMapping, err := h.objects.GetByAPID(ctx, linkID)
	require.NoError(t, err)
	require.NoError(t, h.m.repinAcceptance(ctx, linkMapping, before[tallID].recordCID))

	_, _, err = h.manager.GetRecord(ctx, deletedMapping.DID, deletedMapping.Collection, deletedMapping.RKey)
	assert.Error(t, err, "a deleted post is never re-created by the back-fill")

	again, err := h.m.BackfillImageAspectRatios(ctx, false)
	require.NoError(t, err)
	assert.Equal(t, AspectRatioBackfillReport{
		AlreadySet:        4, // tall.png, camera.jpg, current.png, the legacy image
		UnreadableImages:  2,
		WithoutImageEmbed: 1,
	}, *again, "a second run finds nothing to do")
	healed := readPostState(t, h, tallID)
	assert.Equal(t, healed.recordCID, healed.acceptanceCID, "a second run re-points a stale acceptance")
	healedLink := readPostState(t, h, linkID)
	assert.Equal(t, healedLink.recordCID, healedLink.acceptanceCID, "a post without images is healed too")
}

// seedLegacyImagePost writes a pre-flip image post (community repo, the
// deprecated collection, no aspectRatio) and its mapping, returning the
// mapping. The community and author repos must already exist.
func seedLegacyImagePost(t *testing.T, h *harness, imageBytes []byte) *store.APObjectMapping {
	t.Helper()
	ctx := context.Background()
	communityDID := testDIDFor("technology", "lemmy.world")
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
	blob, err := h.manager.PutBlob(ctx, communityDID, "image/png", imageBytes)
	require.NoError(t, err)
	const rkey = "3kjzl5kcb2s3a"
	published := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
	commit, err := h.manager.PutRecord(ctx, communityDID, CollectionPost, rkey, map[string]any{
		"$type":     CollectionPost,
		"community": communityDID,
		"author":    authorDID,
		"createdAt": recordDatetime(published),
		"title":     "a pre-flip image post",
		"embed": map[string]any{
			"$type":  "social.coves.embed.images",
			"images": []any{map[string]any{"image": *blob}},
		},
	})
	require.NoError(t, err)
	mapping, err := h.objects.PutMapping(ctx, store.APObjectMapping{
		APID:           "https://lemmy.world/post/70407",
		APType:         "Page",
		OriginInstance: "lemmy.world",
		Origin:         store.OriginFediverse,
		DID:            communityDID,
		AuthorDID:      authorDID,
		CommunityDID:   communityDID,
		Collection:     CollectionPost,
		RKey:           rkey,
		CID:            commit.RecordCID,
		PublishedAt:    &published,
	})
	require.NoError(t, err)
	return mapping
}
