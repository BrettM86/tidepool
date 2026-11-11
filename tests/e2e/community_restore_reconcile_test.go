//go:build e2e

package e2e

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
)

// PRD moderation gaps, chunk 06 (B7): the Coves instance admin removes a
// bridged community and later restores it. While Tidepool does not follow the
// community, a Lemmy moderator restores a comment removed before the
// community removal and a Lemmy admin purges a post; neither reaches Tidepool.
// After restoreCommunity, Tidepool follows the community again and its
// reconcile re-fetches every kept object from Lemmy: the restored comment's
// removal record is deleted and Coves shows it live, the purged post ends in
// the outcome for the answer Lemmy gives for it (recorded by this test), and
// a post the admin labelled `!takedown` stays withheld.
func TestCommunityRestore_ReconcilesKeptObjectsWithLemmy(t *testing.T) {
	h := newHarness(t)
	db := openTidepoolDB(t)
	community, sub := setupSubscribedCommunity(t, h, "rcrec")
	h.awaitAppViewCommunityIndexed(t, sub.DID)
	h.admin.awaitRemoteFollowers(t, community.ID, 1, "after the subscribe")
	user := h.registerUser(t, h.uniqueName(t, "rcrec"))
	l := h.newListener(t, cursorNow(), colPostV2, colComment)

	// ── A thread whose comment a Lemmy moderator removes ─────────────────
	threadTitle := "Restore reconcile thread " + h.suffix
	thread := user.createPost(t, community.ID, threadTitle, "a thread whose comment is removed and restored")
	threadEv := l.await("thread postv2 create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "title")
		return e.Commit.Collection == colPostV2 && e.Commit.Operation == opCreate && got == threadTitle
	})
	threadURI := threadEv.atURI()
	commentBody := "a comment restored while the community is unfollowed " + h.suffix
	comment := user.createComment(t, thread.ID, 0, commentBody)
	commentEv := l.await("bridged comment create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "content")
		return e.Commit.Collection == colComment && e.Commit.Operation == opCreate && got == commentBody
	})
	commentURI := commentEv.atURI()
	commentDigest := subjectRKey(commentURI)

	// ── A post the Lemmy admin purges later ──────────────────────────────
	purgedTitle := "Restore reconcile purged post " + h.suffix
	purged := user.createPost(t, community.ID, purgedTitle, "purged by the admin while the community is unfollowed")
	purgedEv := l.await("purged postv2 create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "title")
		return e.Commit.Collection == colPostV2 && e.Commit.Operation == opCreate && got == purgedTitle
	})
	purgedDigest := subjectRKey(purgedEv.atURI())
	h.awaitRecordPresent(t, sub.DID, colAcceptance, purgedDigest, "the purged post's acceptance before the purge")

	// ── An image post the Coves admin labels `!takedown` ─────────────────
	// Bytes unique to this run: a withhold left by a failed run must not
	// reach another run's image.
	imageURL := user.uploadImage(t, "restore-reconcile.png", makeUniqueTestPNG(t, h.suffix+"/"+t.Name()))
	labelledTitle := "Restore reconcile labelled post " + h.suffix
	user.createImagePost(t, community.ID, labelledTitle, imageURL, "restore reconcile e2e image", false)
	labelledEv := l.await("labelled image postv2 create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "title")
		return e.Commit.Collection == colPostV2 && e.Commit.Operation == opCreate && got == labelledTitle
	})
	labelledAuthorDID := labelledEv.Did
	labelledBlobCID := stringAt(t, decodeRecord(t, labelledEv.Commit.Record), "embed", "images", "0", "image", "ref", "$link")
	if labelledBlobCID == "" {
		t.Fatal("the labelled post's embed image blob ref.$link is empty")
	}
	h.awaitBlobServed(t, labelledAuthorDID, labelledBlobCID, nil, "before the content label")

	pdsAddress, err := url.Parse(pdsURL())
	if err != nil {
		t.Fatalf("parse pdsURL %q: %v", pdsURL(), err)
	}
	signIn := covesSignIn{
		AppViewURL:    covesAppViewURL(),
		DialOverrides: map[string]string{"localhost:3001": "127.0.0.1:" + pdsAddress.Port()},
	}
	adminToken := covesSignInAs(t, signIn, covesAdminHandle, covesAdminPassword)

	// ── Before the community removal: a Lemmy removal and a Coves label ──
	h.admin.removeComment(t, comment.ID, true, "rule 3: no personal attacks")
	h.awaitRecordPresent(t, sub.DID, colRemoval, commentDigest, "the comment's removal record after the Lemmy removal")
	labelled := h.awaitCovesSubjectState(t, labelledEv.atURI(), adminToken, "the labelled post indexed", func(s covesSubjectState) bool {
		return s.RecordState == "present" && s.CurrentSubject != nil
	})
	h.covesModerationMutation(t, "social.coves.moderation.removeContent", adminToken, map[string]any{
		"subject":         labelled.CurrentSubject,
		"expectedVersion": labelled.Version,
		"idempotencyKey":  "b7-remove-content-" + h.suffix,
		"reason":          covesReasonModeratorDiscretion,
	})
	h.awaitBlobWithheld(t, labelledAuthorDID, labelledBlobCID, "after the content label")

	// ── The instance admin removes the community ─────────────────────────
	communitySubject := "at://" + sub.DID
	indexed := h.awaitCovesSubjectState(t, communitySubject, adminToken, "the community indexed", func(s covesSubjectState) bool {
		return s.Version != ""
	})
	// Registered before the removal, so a failure after it still restores
	// the community for the rest of the suite.
	t.Cleanup(func() { h.restoreRemovedCommunity(t, db, sub.DID, adminToken, "b7-cleanup-restore-"+h.suffix) })
	h.covesModerationMutation(t, "social.coves.moderation.removeCommunity", adminToken, map[string]any{
		"subject":         sub.DID,
		"expectedVersion": indexed.Version,
		"idempotencyKey":  "b7-remove-community-" + h.suffix,
		"reason":          covesReasonModeratorDiscretion,
	})
	awaitCovesTakedownLabel(t.Context(), t, db, sub.DID, false)
	h.admin.awaitRemoteFollowers(t, community.ID, 0, "after the community removal")

	// ── While unfollowed: Lemmy restores the comment and purges the post ─
	h.admin.removeComment(t, comment.ID, false, "")
	h.admin.purgePost(t, purged.ID, "restore reconcile e2e purge")
	purgeStatus, purgeBody := fetchLemmyObject(t, h, purged.APID)
	t.Logf("GET %s (application/activity+json) after the purge: status %d, body %s",
		purged.APID, purgeStatus, truncate(purgeBody, 200))
	var purgeAnswer struct {
		Type  string `json:"type"`
		Error string `json:"error"`
	}
	_ = json.Unmarshal(purgeBody, &purgeAnswer)
	// Lemmy 0.19 answers an admin-purged post with 400 couldnt_find_post: its
	// not-found answer, the same outcome as 404. Any other 400 code retries.
	lemmyNotFound := purgeStatus == http.StatusBadRequest && purgeAnswer.Error == "couldnt_find_post"
	var purgedIsRemoved bool
	switch {
	case purgeStatus == http.StatusNotFound, lemmyNotFound:
		purgedIsRemoved = false
	case purgeStatus == http.StatusGone, purgeAnswer.Type == "Tombstone":
		purgedIsRemoved = true
	default:
		t.Fatalf("Lemmy answers the purged post %s with status %d (%s): want 404, 400 couldnt_find_post, 410 or a Tombstone body",
			purged.APID, purgeStatus, truncate(purgeBody, 300))
	}
	if _, found := h.bridgeGetRecord(t, sub.DID, colRemoval, purgedDigest); found {
		t.Fatalf("precondition: the purge of %s reached Tidepool while the community was unfollowed", purged.APID)
	}
	if _, found := h.bridgeGetRecord(t, purgedEv.Did, colPostV2, purgedEv.Commit.RKey); !found {
		t.Fatalf("precondition: the purged post's record is gone before the restore")
	}

	// ── The instance admin restores the community ────────────────────────
	removedState := h.awaitCovesSubjectState(t, communitySubject, adminToken, "the community removed", func(s covesSubjectState) bool {
		return s.Version != "" && s.Version != indexed.Version
	})
	h.covesModerationMutation(t, "social.coves.moderation.restoreCommunity", adminToken, map[string]any{
		"subject":         sub.DID,
		"expectedVersion": removedState.Version,
		"idempotencyKey":  "b7-restore-community-" + h.suffix,
		"reason":          covesReasonModeratorDiscretion,
	})
	awaitCovesTakedownLabel(t.Context(), t, db, sub.DID, true)
	h.admin.awaitRemoteFollowers(t, community.ID, 1, "after the community restore")

	// ── The reconcile's outcomes ─────────────────────────────────────────
	h.awaitRecordGone(t, sub.DID, colRemoval, commentDigest, "the restored comment's removal record after the reconcile")
	restored := h.awaitCovesComment(t, threadURI, commentURI, "the comment live after the reconcile", func(view covesCommentView) bool {
		return !view.IsDeleted
	})
	var restoredRecord struct {
		Content string `json:"content"`
	}
	if err := json.Unmarshal(restored.Record, &restoredRecord); err != nil {
		t.Fatalf("restored comment record %s: %v", restored.Record, err)
	}
	if restoredRecord.Content != commentBody {
		t.Errorf("restored comment record.content = %q, want %q", restoredRecord.Content, commentBody)
	}

	if purgedIsRemoved {
		h.awaitRecordPresent(t, sub.DID, colRemoval, purgedDigest, "the purged post's removal record after the reconcile")
		if _, found := h.bridgeGetRecord(t, purgedEv.Did, colPostV2, purgedEv.Commit.RKey); !found {
			t.Errorf("Lemmy answers the purged post gone (%d): the reconcile keeps its record, but it is deleted", purgeStatus)
		}
	} else {
		h.awaitRecordGone(t, purgedEv.Did, colPostV2, purgedEv.Commit.RKey, "the purged post's record after the reconcile (Lemmy answers not found)")
	}

	h.awaitBlobWithheld(t, labelledAuthorDID, labelledBlobCID, "after the community restore's reconcile")
}
