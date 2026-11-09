package ingest

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/blobwithhold"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
)

// COMMUNITY REMOVAL WITHHOLDS A BRIDGED POST'S BLOBS.
//
// A Lemmy moderator's removal of a bridged postv2 leaves the author's record in
// place (it is the author's) and writes a community removal record. While that
// removal stands, every blob the post's CURRENT record references is withheld
// from com.atproto.sync.getBlob under the author's DID: one community-removal
// blob_withholds row per (subject, CID). The bytes stay stored, so a restore
// serves them again without a re-fetch.
//
// The fixtures reuse the upstream-ban world: community C (lemmy.world
// technology) and D (lemmy.world linux), bridged author U on programming.dev,
// posts announced as bare ids and fetched from U's origin.

// rwSummary is the moderator's reason on every removal here, and the summary
// the undone Delete carries on a restore.
const rwSummary = "Rule 1"

// rwImageBytes is a PNG with seed appended: distinct seeds give distinct
// bytes, hence distinct CIDs.
func rwImageBytes(seed string) []byte {
	return append(append([]byte{}, pngBytes...), []byte("removal-withhold:"+seed)...)
}

// rwPage is the Page U's origin serves for slug, with one image attachment
// per seed (each at its own path, so the attachment list can change on edit).
func (w *upstreamBanWorld) rwPage(community *remoteActor, slug, version string, attachments []any) map[string]any {
	postID := originOf(ubUserID) + "/post/" + slug
	page := map[string]any{
		"type":         "Page",
		"id":           postID,
		"attributedTo": ubUserID,
		"to":           []any{community.id, ap.PublicAudience},
		"cc":           []any{},
		"name":         "removal withhold fixture post " + slug,
		"mediaType":    "text/html",
		"audience":     community.id,
		"sensitive":    false,
		"published":    ubBeforeBan,
		"attachment":   attachments,
	}
	if version != "" {
		page["updated"] = version
	}
	return page
}

// rwImageAttachments serves each seed's image at a path unique to slug and
// returns the Page attachments naming them.
func (w *upstreamBanWorld) rwImageAttachments(slug string, seeds ...string) []any {
	w.h.t.Helper()
	var attachments []any
	for _, seed := range seeds {
		imagePath := "/rw-media/" + slug + "/" + seed + ".png"
		body := rwImageBytes(seed)
		w.h.mux.HandleFunc("GET "+imagePath, func(rw http.ResponseWriter, _ *http.Request) {
			rw.Header().Set("Content-Type", "image/png")
			_, _ = rw.Write(body)
		})
		attachments = append(attachments, map[string]any{
			"type":      "Link",
			"href":      originOf(ubUserID) + imagePath,
			"mediaType": "image/png",
		})
	}
	return attachments
}

// imagePost bridges a postv2 by U into community whose embed carries one image
// per seed, and returns its ap id after checking the record references exactly
// len(distinct seeds) image CIDs.
func (w *upstreamBanWorld) imagePost(community *remoteActor, slug string, seeds ...string) string {
	w.h.t.Helper()
	page := w.rwPage(community, slug, "", w.rwImageAttachments(slug, seeds...))
	w.h.serveObject("/post/"+slug, page)
	postID := page["id"].(string)
	w.announceBareID(community, "create/"+slug, postID)
	distinct := map[string]bool{}
	for _, seed := range seeds {
		distinct[seed] = true
	}
	cids := recordBlobCIDs(w.h.t, w.h, postID)
	require.Len(w.h.t, cids, len(distinct),
		"precondition: %s references one image CID per distinct image, got %v", postID, cids)
	return postID
}

// linkPostWithUnfurledThumb bridges a link postv2 by U into community whose
// Page carries no image, so its external embed's thumb comes from unfurling the
// link (the harness must have useConcreteLinkPreviews wired): the link's page
// names an og:image the fixture server answers with bytes unique to slug.
func (w *upstreamBanWorld) linkPostWithUnfurledThumb(community *remoteActor, slug string) string {
	w.h.t.Helper()
	link := "https://news.example/rw-article/" + slug
	thumbURL := "https://news.example/rw-thumb/" + slug + ".png"
	thumbBytes := rwImageBytes("unfurled-thumb-" + slug)
	w.h.mux.HandleFunc("GET /rw-article/"+slug, func(rw http.ResponseWriter, _ *http.Request) {
		rw.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = fmt.Fprintf(rw, `<html><head><meta property="og:image" content=%q></head><body>article</body></html>`, thumbURL)
	})
	w.h.mux.HandleFunc("GET /rw-thumb/"+slug+".png", func(rw http.ResponseWriter, _ *http.Request) {
		rw.Header().Set("Content-Type", "image/png")
		_, _ = rw.Write(thumbBytes)
	})
	page := w.rwPage(community, slug, "", []any{map[string]any{"type": "Link", "href": link}})
	w.h.serveObject("/post/"+slug, page)
	postID := page["id"].(string)
	w.announceBareID(community, "create/"+slug, postID)

	record := liveRecord(w.h.t, w.h, postID)
	embed, ok := record["embed"].(map[string]any)
	require.True(w.h.t, ok, "precondition: the link post carries an embed, got %v", record["embed"])
	require.Equal(w.h.t, "social.coves.embed.external", embed["$type"], "precondition: a link embed")
	external, ok := embed["external"].(map[string]any)
	require.True(w.h.t, ok, "precondition: the link embed carries external")
	require.Equal(w.h.t, link, external["uri"])
	require.Contains(w.h.t, external, "thumb", "precondition: the unfurl produced a thumbnail blob")
	cids := recordBlobCIDs(w.h.t, w.h, postID)
	require.Len(w.h.t, cids, 1, "precondition: the link post references exactly its unfurled thumb")
	data, _, err := w.h.manager.GetBlob(context.Background(), ubUserDID, cids[0])
	require.NoError(w.h.t, err, "precondition: the unfurled thumb is served before any removal")
	require.Equal(w.h.t, thumbBytes, data, "precondition: the thumb blob holds the unfurled image's bytes")
	return postID
}

// removeFromC is community C's moderator removal (announced Delete carrying a
// summary) of postID, checked to have landed: the removal record stands with
// the moderator-discretion code and the author's record is still there.
func (w *upstreamBanWorld) removeFromC(slug, postID string) {
	w.h.t.Helper()
	w.communityRemoves(w.communityC, slug, postID)
	require.Equal(w.h.t, materialize.RemovalCodeModeratorDiscretion, removalCode(w.h.t, w.h, ubCommunityCDID, postID),
		"precondition: C's removal record for %s stands", postID)
	assertLive(w.h.t, w.h, postID, "a moderator removal leaves the author's record")
}

// restoreInC is community C's announced Undo of the summary-bearing Delete
// removeFromC sent, checked to have landed: the removal record is gone.
func (w *upstreamBanWorld) restoreInC(slug, postID string) {
	w.h.t.Helper()
	summary := rwSummary
	w.h.announceUndoDelete(w.communityC,
		w.communityC.id+"/activities/announce/undo/"+slug,
		"https://lemmy.world/activities/delete/"+slug,
		postID, &summary)
	require.Empty(w.h.t, removalCode(w.h.t, w.h, ubCommunityCDID, postID),
		"precondition: C's restore of %s deleted its removal record", postID)
	assertLive(w.h.t, w.h, postID, "a restored post keeps its record")
}

// liveRecord reads apID's current record from its repo.
func liveRecord(t *testing.T, h *harness, apID string) map[string]any {
	t.Helper()
	ctx := context.Background()
	mapping, err := h.objects.GetByAPID(ctx, apID)
	require.NoError(t, err)
	record, _, err := h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	require.NoError(t, err, "%s has its record", apID)
	return record
}

// recordBlobCIDs returns the distinct blob CIDs apID's current record
// references, sorted.
func recordBlobCIDs(t *testing.T, h *harness, apID string) []string {
	t.Helper()
	seen := map[string]bool{}
	var cids []string
	for _, blob := range atdata.ExtractBlobs(liveRecord(t, h, apID)) {
		c := cid.Cid(blob.Ref).String()
		if !seen[c] {
			seen[c] = true
			cids = append(cids, c)
		}
	}
	sort.Strings(cids)
	return cids
}

// assertBlobRefused: getBlob answers not-found for (did, cidStr).
func assertBlobRefused(t *testing.T, h *harness, did, cidStr, why string) {
	t.Helper()
	data, _, err := h.manager.GetBlob(context.Background(), did, cidStr)
	if err == nil {
		t.Errorf("GetBlob served the %d bytes of %s under %s; want not-found (%s)", len(data), cidStr, did, why)
		return
	}
	assert.True(t, errors.IsNotFound(err), "GetBlob(%s, %s) must refuse as not-found (%s); err=%v", did, cidStr, why, err)
}

// assertBlobServed: getBlob returns (did, cidStr)'s bytes.
func assertBlobServed(t *testing.T, h *harness, did, cidStr, why string) {
	t.Helper()
	data, _, err := h.manager.GetBlob(context.Background(), did, cidStr)
	if assert.NoError(t, err, "GetBlob(%s, %s) must serve the bytes (%s)", did, cidStr, why) {
		assert.NotEmpty(t, data, "GetBlob(%s, %s) returned no bytes (%s)", did, cidStr, why)
	}
}

// assertBlobStored: the blobs row for (did, cidStr) is still present.
func assertBlobStored(t *testing.T, h *harness, did, cidStr, why string) {
	t.Helper()
	var count int
	require.NoError(t, h.db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM blobs WHERE did = $1 AND cid = $2`, did, cidStr).Scan(&count))
	assert.Equal(t, 1, count, "the bytes of %s must stay stored under %s (%s)", cidStr, did, why)
}

// activeRemovalWithholds returns cidStr's active community-removal rows.
func activeRemovalWithholds(t *testing.T, h *harness, cidStr string) []blobwithhold.Withhold {
	t.Helper()
	state, err := blobwithhold.NewStore(h.db).Inspect(context.Background(), cidStr)
	require.NoError(t, err)
	var active []blobwithhold.Withhold
	for _, row := range state.Withholds {
		if row.Source == "community-removal" && row.LiftedAt == nil {
			active = append(active, row)
		}
	}
	return active
}

// activeOperatorWithholds returns cidStr's active operator rows.
func activeOperatorWithholds(t *testing.T, h *harness, cidStr string) []blobwithhold.Withhold {
	t.Helper()
	state, err := blobwithhold.NewStore(h.db).Inspect(context.Background(), cidStr)
	require.NoError(t, err)
	var active []blobwithhold.Withhold
	for _, row := range state.Withholds {
		if row.Source == "operator" && row.LiftedAt == nil {
			active = append(active, row)
		}
	}
	return active
}

// assertOneRemovalWithhold: cidStr has exactly one active community-removal
// row, and it is the subject row for subjectURI owned by ownerDID, carrying
// the code of the removal of subjectURI standing in C or D.
func assertOneRemovalWithhold(t *testing.T, h *harness, cidStr, subjectURI, ownerDID string) {
	t.Helper()
	var codes []string
	for _, communityDID := range []string{ubCommunityCDID, ubCommunityDDID} {
		removal, _, err := h.manager.GetRecord(context.Background(), communityDID, materialize.CollectionRemoval, testDigestRKey(subjectURI))
		if errors.IsNotFound(err) {
			continue
		}
		require.NoError(t, err)
		code, _ := removal["code"].(string)
		codes = append(codes, code)
	}
	require.Len(t, codes, 1, "precondition: one removal of %s stands in C or D", subjectURI)
	active := activeRemovalWithholds(t, h, cidStr)
	if !assert.Len(t, active, 1, "%s: want one active community-removal row for %s, got %+v", cidStr, subjectURI, active) {
		return
	}
	row := active[0]
	assert.Equal(t, "subject", row.Scope, "%s: a removal withhold is subject-scoped", cidStr)
	assert.Equal(t, subjectURI, row.SubjectURI, "%s: the row names its post", cidStr)
	assert.Equal(t, ownerDID, row.OwnerDID, "%s: the row is owned by the post's repo DID", cidStr)
	assert.Equal(t, codes[0], row.ReasonCode, "%s: the row carries the removal record's code", cidStr)
}

// TestACommunityRemovalWithholdsEveryBlobOfTheRemovedPosts (B1, acceptance).
//
// GIVEN bridged author U's postv2 A with two distinct images and U's link
// postv2 B whose external thumb was unfurled from the link, both in community
// C; WHEN C's moderator removes each (announced Delete with summary) THEN all
// three CIDs are refused by getBlob under U's DID, both records still reference
// them, every blobs row is still stored, and each CID has exactly one active
// community-removal row naming its post.
func TestACommunityRemovalWithholdsEveryBlobOfTheRemovedPosts(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	h.useConcreteLinkPreviews()

	imagePostID := world.imagePost(world.communityC, "rw-b1-images", "b1-image-one", "b1-image-two")
	linkPostID := world.linkPostWithUnfurledThumb(world.communityC, "rw-b1-link")
	imageCIDs := recordBlobCIDs(t, h, imagePostID)
	linkCIDs := recordBlobCIDs(t, h, linkPostID)
	require.NotContains(t, imageCIDs, linkCIDs[0], "precondition: three distinct CIDs")

	world.removeFromC("rw-b1-images", imagePostID)
	world.removeFromC("rw-b1-link", linkPostID)

	subjects := map[string]string{}
	for _, c := range imageCIDs {
		subjects[c] = atURIOf(t, h, imagePostID)
	}
	subjects[linkCIDs[0]] = atURIOf(t, h, linkPostID)

	assert.Equal(t, imageCIDs, recordBlobCIDs(t, h, imagePostID), "the removed image post still references its images")
	assert.Equal(t, linkCIDs, recordBlobCIDs(t, h, linkPostID), "the removed link post still references its thumb")
	for c, subject := range subjects {
		assertBlobStored(t, h, ubUserDID, c, "a removal keeps the bytes")
		assertBlobRefused(t, h, ubUserDID, c, "a blob of a community-removed post")
		assertOneRemovalWithhold(t, h, c, subject, ubUserDID)
	}
}

// TestAnEditOfARemovedPostWithholdsItsNewImageAndKeepsTheDroppedOne (B2).
//
// GIVEN U's postv2 A with images one and two, removed by C; WHEN U's origin
// edits A to replace image one with a new distinct image three (announced
// Update) THEN getBlob refuses image three, image two stays refused, and image
// one keeps its community-removal row for A and stays refused (its bytes are
// still stored, and only a restore lifts).
func TestAnEditOfARemovedPostWithholdsItsNewImageAndKeepsTheDroppedOne(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	ctx := context.Background()

	const slug = "rw-b2"
	postID := world.imagePost(world.communityC, slug, "b2-image-one", "b2-image-two")
	before := recordBlobCIDs(t, h, postID)
	world.removeFromC(slug, postID)
	subject := atURIOf(t, h, postID)
	for _, c := range before {
		assertBlobStored(t, h, ubUserDID, c, "a removal keeps the bytes")
	}

	// Identify image one's and image two's CIDs from the stored blobs rows'
	// bytes (the fixture's own bytes), not from any code under test.
	cidOfSeed := func(seed string) string {
		t.Helper()
		var found string
		rows, err := h.db.QueryContext(ctx, `SELECT cid FROM blobs WHERE did = $1 AND bytes = $2`, ubUserDID, rwImageBytes(seed))
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			require.NoError(t, rows.Scan(&found))
		}
		require.NoError(t, rows.Err())
		require.NotEmpty(t, found, "precondition: %s's bytes are stored under U", seed)
		return found
	}
	imageOne := cidOfSeed("b2-image-one")
	imageTwo := cidOfSeed("b2-image-two")
	require.ElementsMatch(t, before, []string{imageOne, imageTwo}, "precondition: A references images one and two")

	edited := world.rwPage(world.communityC, slug, "2026-08-20T12:00:00Z",
		world.rwImageAttachments(slug+"-edit", "b2-image-three", "b2-image-two"))
	h.serveObject("/post/"+slug, edited)
	require.Equal(t, http.StatusAccepted, h.deliver(world.communityC, map[string]any{
		"id":       world.communityC.id + "/activities/announce/update/" + slug,
		"type":     "Announce",
		"actor":    world.communityC.id,
		"audience": world.communityC.id,
		"object": map[string]any{
			"id":       originOf(ubUserID) + "/activities/update/" + slug,
			"type":     "Update",
			"actor":    ubUserID,
			"audience": world.communityC.id,
			"object":   edited,
		},
	}))
	h.drain()

	imageThree := cidOfSeed("b2-image-three")
	require.ElementsMatch(t, []string{imageThree, imageTwo}, recordBlobCIDs(t, h, postID),
		"precondition: the edit re-materialized A with images three and two")
	require.Equal(t, materialize.RemovalCodeModeratorDiscretion, removalCode(t, h, ubCommunityCDID, postID),
		"precondition: the removal still stands after the edit")

	assertBlobStored(t, h, ubUserDID, imageThree, "the edit's new image is stored")
	assertBlobRefused(t, h, ubUserDID, imageThree, "the new image of a post that is still removed")
	assertOneRemovalWithhold(t, h, imageThree, subject, ubUserDID)
	assertBlobRefused(t, h, ubUserDID, imageTwo, "an image the removed post still references")
	assertOneRemovalWithhold(t, h, imageOne, subject, ubUserDID)
	assertBlobRefused(t, h, ubUserDID, imageOne, "the image a still-removed post dropped")
}

// TestARemovalWithholdsAnImageAnotherLivePostOfTheAuthorStillUses (B3).
//
// GIVEN U's postv2 in C and U's postv2 in D carrying byte-identical images (one
// CID in U's repo); WHEN C removes its post THEN getBlob refuses that CID under
// U's DID, even though the post in D is live and references it.
func TestARemovalWithholdsAnImageAnotherLivePostOfTheAuthorStillUses(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h

	removedID := world.imagePost(world.communityC, "rw-b3-removed", "b3-shared")
	liveID := world.imagePost(world.communityD, "rw-b3-live", "b3-shared")
	shared := recordBlobCIDs(t, h, removedID)
	require.Equal(t, shared, recordBlobCIDs(t, h, liveID), "precondition: both posts reference the one shared CID")

	world.removeFromC("rw-b3-removed", removedID)
	assertLive(t, h, liveID, "D's post is untouched by C's removal")
	assert.Equal(t, shared, recordBlobCIDs(t, h, liveID), "D's live post still references the shared image")

	assertBlobStored(t, h, ubUserDID, shared[0], "a removal keeps the bytes")
	assertBlobRefused(t, h, ubUserDID, shared[0], "an image of a removed post, also used by a live post")
	assertOneRemovalWithhold(t, h, shared[0], atURIOf(t, h, removedID), ubUserDID)
}

// TestARemovedPostRematerializedWhileItsImageOriginFailsKeepsTheImageRefused.
//
// GIVEN U's postv2 with one image in C, removed by C (the image withheld);
// WHEN U's origin re-announces an edit of the post while the image's URL
// answers 503, so the rebuilt record drops the image ref THEN the bytes stay
// stored, the post's community-removal row stays active, and getBlob still
// refuses the image; WHEN C then restores the post THEN the row is lifted and
// the image is served again.
func TestARemovedPostRematerializedWhileItsImageOriginFailsKeepsTheImageRefused(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h

	const slug = "rw-fetch-fail"
	const imagePath = "/rw-media/" + slug + "/image.png"
	var originFails atomic.Bool
	imageBytes := rwImageBytes("rw-fetch-fail-image")
	h.mux.HandleFunc("GET "+imagePath, func(rw http.ResponseWriter, _ *http.Request) {
		if originFails.Load() {
			rw.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		rw.Header().Set("Content-Type", "image/png")
		_, _ = rw.Write(imageBytes)
	})
	attachments := []any{map[string]any{
		"type":      "Link",
		"href":      originOf(ubUserID) + imagePath,
		"mediaType": "image/png",
	}}
	page := world.rwPage(world.communityC, slug, "", attachments)
	h.serveObject("/post/"+slug, page)
	postID := page["id"].(string)
	world.announceBareID(world.communityC, "create/"+slug, postID)
	cids := recordBlobCIDs(t, h, postID)
	require.Len(t, cids, 1, "precondition: the post references its one image")
	imageCID := cids[0]
	subject := atURIOf(t, h, postID)

	world.removeFromC(slug, postID)
	assertOneRemovalWithhold(t, h, imageCID, subject, ubUserDID)

	originFails.Store(true)
	edited := world.rwPage(world.communityC, slug, "2026-08-20T12:00:00Z", attachments)
	edited["name"] = "removal withhold fixture post " + slug + " (edited)"
	h.serveObject("/post/"+slug, edited)
	require.Equal(t, http.StatusAccepted, h.deliver(world.communityC, map[string]any{
		"id":       world.communityC.id + "/activities/announce/update/" + slug,
		"type":     "Announce",
		"actor":    world.communityC.id,
		"audience": world.communityC.id,
		"object": map[string]any{
			"id":       originOf(ubUserID) + "/activities/update/" + slug,
			"type":     "Update",
			"actor":    ubUserID,
			"audience": world.communityC.id,
			"object":   edited,
		},
	}))
	h.drain()
	require.Empty(t, recordBlobCIDs(t, h, postID),
		"precondition: the failed image fetch dropped the image ref from the rebuilt record")
	require.Equal(t, materialize.RemovalCodeModeratorDiscretion, removalCode(t, h, ubCommunityCDID, postID),
		"precondition: the removal still stands after the re-materialization")

	assertBlobStored(t, h, ubUserDID, imageCID, "a failed fetch leaves the stored bytes")
	assertOneRemovalWithhold(t, h, imageCID, subject, ubUserDID)
	assertBlobRefused(t, h, ubUserDID, imageCID, "the removed post's image after a failed re-fetch")

	world.restoreInC(slug, postID)
	assert.Empty(t, activeRemovalWithholds(t, h, imageCID), "a restore lifts the post's community-removal rows")
	assertBlobServed(t, h, ubUserDID, imageCID, "a restore serves the stored image again")
}

// TestARestoreLiftsTheRemovalWithholdsAndServesTheImagesAgain (B4).
//
// GIVEN U's postv2 with two images, removed by C (both refused); WHEN C
// announces the Undo of its summary-bearing Delete and the re-fetch serves the
// post THEN the post has no active community-removal rows and getBlob serves
// every image.
func TestARestoreLiftsTheRemovalWithholdsAndServesTheImagesAgain(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h

	const slug = "rw-b4"
	postID := world.imagePost(world.communityC, slug, "b4-image-one", "b4-image-two")
	cids := recordBlobCIDs(t, h, postID)
	subject := atURIOf(t, h, postID)

	world.removeFromC(slug, postID)
	for _, c := range cids {
		assertBlobStored(t, h, ubUserDID, c, "a removal keeps the bytes")
		assertBlobRefused(t, h, ubUserDID, c, "while the post is removed")
		assertOneRemovalWithhold(t, h, c, subject, ubUserDID)
	}

	world.restoreInC(slug, postID)
	require.Equal(t, cids, recordBlobCIDs(t, h, postID), "precondition: the restored post references the same images")
	for _, c := range cids {
		assert.Empty(t, activeRemovalWithholds(t, h, c), "%s: a restore lifts every community-removal row of the post", c)
		assertBlobServed(t, h, ubUserDID, c, "after the restore")
	}
}

// TestAnOperatorWithholdOutlivesACommunityRemovalAndItsRestore (B5).
//
// GIVEN U's postv2 with images one and two, an active operator withhold on
// image one, and C's removal of the post (both refused, each with its
// community-removal row):
//
//	(a) WHEN C restores the post THEN image one is still refused (its operator
//	    row stands, no community-removal row is active) and image two is served;
//	(b) WHEN the operator lifts image one's withhold while the post is still
//	    removed THEN image one is still refused (the removal still withholds it).
func TestAnOperatorWithholdOutlivesACommunityRemovalAndItsRestore(t *testing.T) {
	for _, tc := range []struct {
		name string
		slug string
		// act runs after the removal; it returns why image one must still be
		// refused and whether image two must now be served.
		act func(t *testing.T, world *upstreamBanWorld, postID, imageOne string) (why string, imageTwoServed bool)
	}{
		{
			name: "community restore leaves the operator withhold",
			slug: "rw-b5a",
			act: func(t *testing.T, world *upstreamBanWorld, postID, imageOne string) (string, bool) {
				world.restoreInC("rw-b5a", postID)
				assert.Len(t, activeOperatorWithholds(t, world.h, imageOne), 1,
					"a community restore must not lift the operator's withhold")
				assert.Empty(t, activeRemovalWithholds(t, world.h, imageOne),
					"the restore lifts the community-removal row")
				return "the operator withhold outlives the community restore", true
			},
		},
		{
			name: "operator lift leaves the removal withhold",
			slug: "rw-b5b",
			act: func(t *testing.T, world *upstreamBanWorld, postID, imageOne string) (string, bool) {
				_, err := blobwithhold.NewStore(world.h.db).Lift(context.Background(), imageOne, "b5b-operator")
				require.NoError(t, err)
				require.Empty(t, activeOperatorWithholds(t, world.h, imageOne), "precondition: the operator row is lifted")
				assertOneRemovalWithhold(t, world.h, imageOne, atURIOf(t, world.h, postID), ubUserDID)
				return "the post is still removed after the operator's lift", false
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			world := newUpstreamBanWorld(t)
			h := world.h
			ctx := context.Background()

			postID := world.imagePost(world.communityC, tc.slug, tc.slug+"-image-one", tc.slug+"-image-two")
			cids := recordBlobCIDs(t, h, postID)
			imageOne, imageTwo := cids[0], cids[1]
			_, err := blobwithhold.NewStore(h.db).Withhold(ctx, blobwithhold.OperatorRequest{
				CID:        imageOne,
				ReasonCode: "operator-discretion",
				Actor:      tc.slug + "-operator",
			})
			require.NoError(t, err)
			require.Len(t, activeOperatorWithholds(t, h, imageOne), 1, "precondition: image one is operator-withheld")
			assertBlobServed(t, h, ubUserDID, imageTwo, "precondition: image two is served before the removal")

			world.removeFromC(tc.slug, postID)
			subject := atURIOf(t, h, postID)
			for _, c := range cids {
				assertBlobStored(t, h, ubUserDID, c, "a removal keeps the bytes")
				assertBlobRefused(t, h, ubUserDID, c, "while the post is removed")
				assertOneRemovalWithhold(t, h, c, subject, ubUserDID)
			}

			why, imageTwoServed := tc.act(t, world, postID, imageOne)

			assertBlobRefused(t, h, ubUserDID, imageOne, why)
			if imageTwoServed {
				assertBlobServed(t, h, ubUserDID, imageTwo, "the restored post's unwithheld image")
			} else {
				assertBlobRefused(t, h, ubUserDID, imageTwo, "the post is still removed")
			}
		})
	}
}

// TestASharedImageStaysWithheldUntilEveryRemovedPostUsingItIsRestored (B6).
//
// GIVEN U's two postv2s in C carrying byte-identical images (one CID), both
// removed by C; WHEN C restores the first THEN the shared CID is still refused
// (the second post's removal withholds it); WHEN C then restores the second
// THEN it is served.
func TestASharedImageStaysWithheldUntilEveryRemovedPostUsingItIsRestored(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h

	firstID := world.imagePost(world.communityC, "rw-b6-first", "b6-shared")
	secondID := world.imagePost(world.communityC, "rw-b6-second", "b6-shared")
	shared := recordBlobCIDs(t, h, firstID)
	require.Equal(t, shared, recordBlobCIDs(t, h, secondID), "precondition: both posts reference the one shared CID")
	sharedCID := shared[0]

	world.removeFromC("rw-b6-first", firstID)
	world.removeFromC("rw-b6-second", secondID)
	assertBlobStored(t, h, ubUserDID, sharedCID, "the removals keep the bytes")
	active := activeRemovalWithholds(t, h, sharedCID)
	var subjects []string
	for _, row := range active {
		subjects = append(subjects, row.SubjectURI)
	}
	assert.ElementsMatch(t, []string{atURIOf(t, h, firstID), atURIOf(t, h, secondID)}, subjects,
		"one active community-removal row per removed post referencing the shared CID")

	world.restoreInC("rw-b6-first", firstID)
	assertBlobRefused(t, h, ubUserDID, sharedCID, "the second post is still removed")
	assertOneRemovalWithhold(t, h, sharedCID, atURIOf(t, h, secondID), ubUserDID)

	world.restoreInC("rw-b6-second", secondID)
	assert.Empty(t, activeRemovalWithholds(t, h, sharedCID), "no removed post references the shared CID")
	assertBlobServed(t, h, ubUserDID, sharedCID, "every post using it is restored")
}
