package ingest

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/blobwithhold"
	"tidepool/internal/materialize"
)

// AN AUTHOR'S DELETE DELETES THE IMAGE BYTES OF A SUBJECT NOBODY REMOVED.
//
// Decision 2026-11-07: when the author deletes their bridged post or comment
// (the announced, summary-less Delete attributed to the author), the bytes of
// every blob the subject used that no other live record in the author's repo
// still references are deleted with it. A CID another live record still
// references keeps its bytes. An operator-withheld CID keeps its bytes.
//
// Decision 2026-11-08: while a community removal of the subject stands, the
// author's delete deletes none of its bytes and lifts none of its
// community-removal rows, so an uploader cannot wipe the bridge's copy of a
// removed image before an operator withholds it; getBlob keeps refusing the
// CIDs. Every other delete — the delete sweep, a ban's scrub — and every
// moderator removal keeps the bytes.

// TestAnAuthorDeleteOfARemovedPostKeepsItsImageAndItsWithhold.
//
// GIVEN U's postv2 in C with its own image, removed by C (bytes kept, CID
// refused); WHEN U deletes the post upstream (announced Delete by the author)
// THEN the image's bytes stay stored under U's DID, the post's
// community-removal row stands, and getBlob answers not-found.
func TestAnAuthorDeleteOfARemovedPostKeepsItsImageAndItsWithhold(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h

	const slug = "rw-delete-author-own"
	postID := world.imagePost(world.communityC, slug, "rw-delete-author-own-image")
	imageCID := recordBlobCIDs(t, h, postID)[0]
	subject := atURIOf(t, h, postID)

	world.removeFromC(slug, postID)
	assertBlobStored(t, h, ubUserDID, imageCID, "a moderator removal keeps the bytes")
	assertOneRemovalWithhold(t, h, imageCID, subject, ubUserDID)

	h.announceDelete(world.communityC, world.communityC.id+"/activities/announce/author-delete/"+slug,
		ubUserID, postID)
	assertDeleted(t, h, postID, "the author deleted it")

	assertBlobStored(t, h, ubUserDID, imageCID, "an author delete keeps a removed post's bytes")
	assertRemovalWithholdStands(t, h, imageCID, subject, "an author delete lifts no row of a removed post")
	assertBlobRefused(t, h, ubUserDID, imageCID, "the removed post's image after the author deleted the post")
}

// TestARestoreOfAnAuthorDeletedRemovedPostKeepsItsImageRefused.
//
// GIVEN U's postv2 in C with its own image, removed by C, then deleted by U
// (bytes and row kept); WHEN U's origin answers a Tombstone for it and C's
// moderator restores it THEN the restore finds no record to re-accept: C's
// removal record and the post's community-removal row stand, the bytes stay
// stored, and getBlob refuses the image until an operator acts.
func TestARestoreOfAnAuthorDeletedRemovedPostKeepsItsImageRefused(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h

	const slug = "rw-delete-author-restore"
	postID := world.imagePost(world.communityC, slug, "rw-delete-author-restore-image")
	imageCID := recordBlobCIDs(t, h, postID)[0]
	subject := atURIOf(t, h, postID)
	world.removeFromC(slug, postID)
	h.announceDelete(world.communityC, world.communityC.id+"/activities/announce/author-delete/"+slug,
		ubUserID, postID)
	assertDeleted(t, h, postID, "the author deleted it")

	h.serveObject(urlPath(t, postID), map[string]any{"id": postID, "type": "Tombstone"})
	summary := rwSummary
	h.announceUndoDelete(world.communityC, world.communityC.id+"/activities/announce/undo/"+slug,
		"https://lemmy.world/activities/delete/"+slug, postID, &summary)
	h.drain()

	assertDeleted(t, h, postID, "the restore brings no deleted record back")
	assert.Equal(t, materialize.RemovalCodeModeratorDiscretion, removalCode(t, h, ubCommunityCDID, postID),
		"a restore with no record to re-accept leaves the removal standing")
	assertBlobStored(t, h, ubUserDID, imageCID, "the bytes stay until an operator acts")
	assertRemovalWithholdStands(t, h, imageCID, subject, "the row stands with the removal")
	assertBlobRefused(t, h, ubUserDID, imageCID, "the removed, deleted post's image")
}

// TestAnAuthorDeleteOfARemovedPostKeepsTheImageAnEditDropped.
//
// GIVEN U's postv2 in C with images one and two, removed by C, then edited to
// carry only image two (image one keeps its row and its bytes); WHEN U deletes
// the post THEN both images' bytes stay stored, both community-removal rows
// stand, and getBlob refuses both.
func TestAnAuthorDeleteOfARemovedPostKeepsTheImageAnEditDropped(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h

	const slug = "rw-delete-author-dropped"
	postID := world.imagePost(world.communityC, slug, "rw-dad-image-one", "rw-dad-image-two")
	before := recordBlobCIDs(t, h, postID)
	subject := atURIOf(t, h, postID)
	world.removeFromC(slug, postID)

	edited := world.rwPage(world.communityC, slug, "2026-08-20T12:00:00Z",
		world.rwImageAttachments(slug+"-edit", "rw-dad-image-two"))
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
	after := recordBlobCIDs(t, h, postID)
	require.Len(t, after, 1, "precondition: the edit left one image")
	for _, c := range before {
		assertBlobStored(t, h, ubUserDID, c, "precondition: the removed post's images keep their bytes")
		require.NotEmpty(t, activeRemovalWithholds(t, h, c), "precondition: %s has its removal row", c)
	}

	h.announceDelete(world.communityC, world.communityC.id+"/activities/announce/author-delete/"+slug,
		ubUserID, postID)
	assertDeleted(t, h, postID, "the author deleted it")

	for _, c := range before {
		assertBlobStored(t, h, ubUserDID, c, "an author delete keeps every image of a removed post")
		assertRemovalWithholdStands(t, h, c, subject, "an author delete lifts no row of a removed post")
		assertBlobRefused(t, h, ubUserDID, c, "the removed post's image after the author deleted the post")
	}
}

// TestAnAuthorDeleteOfARemovedPostKeepsAnImageAnotherRecordUses.
//
// GIVEN U's postv2 in C and U's postv2 in D carrying byte-identical images (one
// CID in U's repo), and C's moderator removal of the post in C (the CID is
// refused); WHEN U deletes the removed post upstream (announced Delete by the
// author) THEN the bytes stay stored (the post in D references them), the
// post's community-removal row stays active, and getBlob still refuses the CID
// under U's DID, also for the post in D.
func TestAnAuthorDeleteOfARemovedPostKeepsAnImageAnotherRecordUses(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h

	const slug = "rw-delete-author"
	removedID := world.imagePost(world.communityC, slug, "rw-delete-author-shared")
	liveID := world.imagePost(world.communityD, slug+"-live", "rw-delete-author-shared")
	shared := recordBlobCIDs(t, h, removedID)
	require.Equal(t, shared, recordBlobCIDs(t, h, liveID), "precondition: both posts reference the one shared CID")
	subject := atURIOf(t, h, removedID)

	world.removeFromC(slug, removedID)
	assertOneRemovalWithhold(t, h, shared[0], subject, ubUserDID)
	assertBlobRefused(t, h, ubUserDID, shared[0], "precondition: the removed post's image is withheld")

	h.announceDelete(world.communityC, world.communityC.id+"/activities/announce/author-delete/"+slug,
		ubUserID, removedID)
	assertDeleted(t, h, removedID, "the author deleted it")

	assertBlobStored(t, h, ubUserDID, shared[0], "another live record of the author references the bytes")
	assertRemovalWithholdStands(t, h, shared[0], subject, "a CID whose bytes are kept keeps its removal row")
	assertBlobRefused(t, h, ubUserDID, shared[0], "the removed post's image after the author deleted the post")
	assertLive(t, h, liveID, "D's post is untouched")
}

// TestAnAuthorDeleteOfARemovedPostKeepsItsOwnAndItsSharedImage.
//
// GIVEN U's postv2 in C carrying its own image X and an image S, U's postv2 in
// D also carrying S, and C's removal of the post in C (removal rows for X and
// S); WHEN U deletes the removed post THEN X and S both keep their bytes and
// their rows, and getBlob refuses both under U's DID.
func TestAnAuthorDeleteOfARemovedPostKeepsItsOwnAndItsSharedImage(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h

	const slug = "rw-delete-author-per-cid"
	removedID := world.imagePost(world.communityC, slug, "rw-dapc-own", "rw-dapc-shared")
	liveID := world.imagePost(world.communityD, slug+"-live", "rw-dapc-shared")
	shared := recordBlobCIDs(t, h, liveID)[0]
	var own string
	for _, c := range recordBlobCIDs(t, h, removedID) {
		if c != shared {
			own = c
		}
	}
	require.NotEmpty(t, own, "precondition: the removed post carries an image of its own")
	subject := atURIOf(t, h, removedID)

	world.removeFromC(slug, removedID)
	assertOneRemovalWithhold(t, h, own, subject, ubUserDID)
	assertOneRemovalWithhold(t, h, shared, subject, ubUserDID)

	h.announceDelete(world.communityC, world.communityC.id+"/activities/announce/author-delete/"+slug,
		ubUserID, removedID)
	assertDeleted(t, h, removedID, "the author deleted it")

	assertBlobStored(t, h, ubUserDID, own, "an author delete keeps a removed post's own image")
	assertRemovalWithholdStands(t, h, own, subject, "an author delete lifts no row of a removed post")
	assertBlobRefused(t, h, ubUserDID, own, "the removed post's own image")
	assertBlobStored(t, h, ubUserDID, shared, "the post in D references the shared image")
	assertRemovalWithholdStands(t, h, shared, subject, "the row of a kept image stands")
	assertBlobRefused(t, h, ubUserDID, shared, "the removed post's shared image")
	assertLive(t, h, liveID, "D's post is untouched")
}

// TestAnAuthorDeleteKeepsTheBytesOfAnOperatorWithheldImage.
//
// GIVEN U's postv2 in C with its own image, an active operator withhold on the
// image's CID, and C's removal of the post; WHEN U deletes the post THEN the
// bytes stay stored under U's DID (only the operator's delete-bytes action
// deletes them), the post's community-removal row stays active, and getBlob
// refuses the CID.
func TestAnAuthorDeleteKeepsTheBytesOfAnOperatorWithheldImage(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h

	const slug = "rw-delete-author-operator"
	postID := world.imagePost(world.communityC, slug, "rw-delete-author-operator-image")
	imageCID := recordBlobCIDs(t, h, postID)[0]
	subject := atURIOf(t, h, postID)
	t.Cleanup(func() {
		if _, err := h.db.Exec(`DELETE FROM blob_withholds WHERE cid = $1`, imageCID); err != nil {
			t.Errorf("cleanup blob_withholds: %v", err)
		}
	})
	_, err := blobwithhold.NewStore(h.db).Withhold(context.Background(), blobwithhold.OperatorRequest{
		CID:        imageCID,
		ReasonCode: blobwithhold.ReasonIllegalContent,
		Actor:      "rw-delete-operator",
	})
	require.NoError(t, err)
	world.removeFromC(slug, postID)

	h.announceDelete(world.communityC, world.communityC.id+"/activities/announce/author-delete/"+slug,
		ubUserID, postID)
	assertDeleted(t, h, postID, "the author deleted it")

	assertBlobStored(t, h, ubUserDID, imageCID, "an operator withhold keeps the bytes")
	assert.Len(t, activeOperatorWithholds(t, h, imageCID), 1, "the operator's withhold stands")
	assertRemovalWithholdStands(t, h, imageCID, subject, "a CID whose bytes are kept keeps its removal row")
	assertBlobRefused(t, h, ubUserDID, imageCID, "the operator-withheld image")
}

// TestAnAuthorDeleteOfARemovedCommentKeepsItsImageAndItsWithhold.
//
// GIVEN U's bridged comment in C with a seeded image, removed by C's
// moderator (bytes kept, CID refused); WHEN U deletes the comment (announced
// summary-less Delete attributed to U) THEN the image's bytes stay stored, the
// comment's community-removal row stands, and getBlob refuses the image.
func TestAnAuthorDeleteOfARemovedCommentKeepsItsImageAndItsWithhold(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h

	const slug = "rw-delete-author-comment"
	rootID := world.postBy(ubOtherUserID, world.communityC, slug+"-root", ubBeforeBan, false)
	commentID, imageCID := world.imageComment(world.communityC, slug, rootID, ubBeforeBan2, slug+"-image")
	subject := atURIOf(t, h, commentID)

	h.announceDeleteAs(world.communityC, world.communityC.id+"/activities/announce/delete/"+slug,
		"https://lemmy.world/activities/delete/"+slug, commentID, modActorID, ptr(rwSummary))
	require.Equal(t, materialize.RemovalCodeModeratorDiscretion, removalCode(t, h, ubCommunityCDID, commentID),
		"precondition: C's removal record for the comment stands")
	assertBlobStored(t, h, ubUserDID, imageCID, "a moderator removal keeps the bytes")
	assertOneRemovalWithhold(t, h, imageCID, subject, ubUserDID)

	h.announceDeleteAs(world.communityC, world.communityC.id+"/activities/announce/author-delete/"+slug,
		originOf(ubUserID)+"/activities/delete/"+slug, commentID, ubUserID, nil)
	assertDeleted(t, h, commentID, "the author deleted it")

	assertBlobStored(t, h, ubUserDID, imageCID, "an author delete keeps a removed comment's image")
	assertRemovalWithholdStands(t, h, imageCID, subject, "an author delete lifts no row of a removed comment")
	assertBlobRefused(t, h, ubUserDID, imageCID, "the removed comment's image")
}

// TestAnAuthorDeleteOfAPostRemovedWithoutWithholdRowsKeepsItsImage.
//
// GIVEN U's postv2 in C with its own image, removed by C, whose
// community-removal rows are gone (a removal written before removals withheld
// blobs and not yet back-filled); WHEN U deletes the post THEN the image's
// bytes stay stored, the delete's commit writes the row the removal lacked
// (the back-fill cannot once the record is gone), and getBlob refuses it.
func TestAnAuthorDeleteOfAPostRemovedWithoutWithholdRowsKeepsItsImage(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h

	const slug = "rw-delete-author-no-rows"
	postID := world.imagePost(world.communityC, slug, "rw-delete-author-no-rows-image")
	imageCID := recordBlobCIDs(t, h, postID)[0]
	subject := atURIOf(t, h, postID)

	world.removeFromC(slug, postID)
	_, err := h.db.Exec(`DELETE FROM blob_withholds WHERE source = 'community-removal' AND subject_uri = $1`, subject)
	require.NoError(t, err)
	require.Empty(t, activeRemovalWithholds(t, h, imageCID), "precondition: the removal has no withhold rows")

	h.announceDelete(world.communityC, world.communityC.id+"/activities/announce/author-delete/"+slug,
		ubUserID, postID)
	assertDeleted(t, h, postID, "the author deleted it")

	assertBlobStored(t, h, ubUserDID, imageCID, "a standing removal keeps the bytes")
	assertOneRemovalWithhold(t, h, imageCID, subject, ubUserDID)
	assertBlobRefused(t, h, ubUserDID, imageCID, "the kept image of a removed, deleted post")
}

// TestAnAuthorDeleteOfAPostNobodyRemovedDeletesItsImage.
//
// GIVEN U's live postv2 in C with its own image, never removed; WHEN U deletes
// it THEN the image's bytes are gone.
func TestAnAuthorDeleteOfAPostNobodyRemovedDeletesItsImage(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h

	const slug = "rw-delete-author-unremoved"
	postID := world.imagePost(world.communityC, slug, "rw-delete-author-unremoved-image")
	imageCID := recordBlobCIDs(t, h, postID)[0]
	assertBlobServed(t, h, ubUserDID, imageCID, "precondition: the image is served")

	h.announceDelete(world.communityC, world.communityC.id+"/activities/announce/author-delete/"+slug,
		ubUserID, postID)
	assertDeleted(t, h, postID, "the author deleted it")

	assertBlobGone(t, h, ubUserDID, imageCID, "the author's delete deletes the image")
}

// TestASweepDeleteOfARemovedPostKeepsItsImageRefused.
//
// GIVEN U's image postv2 in C, removed by C (the image refused); WHEN U's
// origin answers a Tombstone for it and the operator's delete sweep deletes it
// THEN the post's community-removal row stays active, the bytes stay stored,
// and getBlob still refuses the image under U's DID: Lemmy answers 410 for
// moderator-removed content too, so a sweep is not an author's delete.
func TestASweepDeleteOfARemovedPostKeepsItsImageRefused(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h

	const slug = "rw-delete-sweep"
	postID := world.imagePost(world.communityC, slug, "rw-delete-sweep-image")
	imageCID := recordBlobCIDs(t, h, postID)[0]
	subject := atURIOf(t, h, postID)

	world.removeFromC(slug, postID)
	assertOneRemovalWithhold(t, h, imageCID, subject, ubUserDID)
	assertBlobRefused(t, h, ubUserDID, imageCID, "precondition: the removed post's image is withheld")

	h.serveObject(urlPath(t, postID), map[string]any{"id": postID, "type": "Tombstone"})
	out := h.sweep(postID)
	require.Equal(t, OutcomeDeleted, out.Result[0].Outcome, "error: %s", out.Result[0].Error)
	assertDeleted(t, h, postID, "the sweep applied the origin's tombstone")

	assertBlobStored(t, h, ubUserDID, imageCID, "a sweep delete keeps the bytes")
	assertRemovalWithholdStands(t, h, imageCID, subject, "only a restore lifts a community-removal row")
	assertBlobRefused(t, h, ubUserDID, imageCID, "the removed post's image after the sweep deleted the post")
}

// assertRemovalWithholdStands: cidStr has an active community-removal row
// naming subjectURI.
func assertRemovalWithholdStands(t *testing.T, h *harness, cidStr, subjectURI, why string) {
	t.Helper()
	for _, row := range activeRemovalWithholds(t, h, cidStr) {
		if row.SubjectURI == subjectURI {
			return
		}
	}
	t.Errorf("%s: no active community-removal row names %s (%s)", cidStr, subjectURI, why)
}

// assertBlobGone: no blobs row for (did, cidStr) remains.
func assertBlobGone(t *testing.T, h *harness, did, cidStr, why string) {
	t.Helper()
	var count int
	require.NoError(t, h.db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM blobs WHERE did = $1 AND cid = $2`, did, cidStr).Scan(&count))
	assert.Zero(t, count, "the bytes of %s must be deleted under %s (%s)", cidStr, did, why)
}

// TestASiteBanAfterACommunityBanKeepsTheWithdrawnContentsImagesRefused.
//
// GIVEN U's postv2 with an image and two comments with seeded images in C,
// withdrawn by C's ban of U with removeData (each image withheld); WHEN U's
// home admin then site-bans U with removeData THEN the records are deleted,
// each subject's community-removal row stays active (only a restore lifts),
// and getBlob refuses every image.
func TestASiteBanAfterACommunityBanKeepsTheWithdrawnContentsImagesRefused(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	fixture := world.seedCommunityBanFixture("rw-delete-site")

	const communityBanID = "https://lemmy.world/activities/announce/block/rw-delete-site-community"
	world.announceCommunityBlock(world.communityC, communityBanID, ubUserID, nil, ubBanTime)
	assertProcessed(t, h, communityBanID)
	assertCommunityBanWithheld(t, h, fixture, "Spam")

	subjects := map[string]string{}
	for _, c := range fixture.postCIDs {
		subjects[c] = atURIOf(t, h, fixture.postID)
	}
	for _, commentID := range fixture.comments {
		subjects[fixture.commentCID[commentID]] = atURIOf(t, h, commentID)
	}

	const siteBanID = ubHomeHost + "/activities/block/rw-delete-site-ban"
	siteBanTime := ubBanTime.Add(24 * time.Hour)
	world.deliverAt(world.admin, siteBlock(siteBanID, ubAdminID, ubUserID, ubHomeRoot, nil), &siteBanTime)
	assertProcessed(t, h, siteBanID)

	assertDeleted(t, h, fixture.postID, "the site ban deletes the withdrawn post")
	for _, commentID := range fixture.comments {
		assertDeleted(t, h, commentID, "the site ban deletes the withdrawn comment")
	}
	for c, subject := range subjects {
		assertRemovalWithholdStands(t, h, c, subject, "only a restore lifts a community-removal row")
		assertBlobRefused(t, h, ubUserDID, c, "the withdrawn content's image after the site ban deleted it")
	}
}
