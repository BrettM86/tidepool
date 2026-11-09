package materialize

import (
	"context"
	"net/http"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/blobwithhold"
	"tidepool/internal/errors"
	"tidepool/internal/testutil"
)

// TestActorScrubKeepsOperatorWithheldBlobs (B10b, B10c, B11): the actor
// scrubs delete the author's records and their blobs as today, except the
// bytes of a CID under an active operator withhold, which stay as evidence
// under the author's DID. A community-removal subject-scope row alone is not
// such a withhold: its bytes go as today. Nor is a lifted operator withhold:
// after a lift the bytes go as today.
//
// The author's postv2 embed images live under the AUTHOR's DID, so that is
// the (DID, CID) the scrub targets. The Delete{Actor} row also stores two
// blobs under the author's DID that no record references (one withheld, one
// not), which only the final DeleteBlobsForDID sweep reaches. The
// subject-row case stores one unreferenced blob held only by a subject-scope
// row, which that sweep still deletes.
func TestActorScrubKeepsOperatorWithheldBlobs(t *testing.T) {
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
	communityDID := testDIDFor("technology", "lemmy.world")

	deleteActor := func(ctx context.Context, h *harness) error { return h.m.DeleteActor(ctx, personID) }
	suppressActor := func(ctx context.Context, h *harness) error { return h.m.SuppressActor(ctx, personID) }

	cases := []struct {
		name string
		// seed makes this row's media bytes unique.
		seed string
		// subjectRowOnly holds the image with a community-removal
		// subject-scope row instead of an operator withhold.
		subjectRowOnly bool
		// orphans stores unreferenced blobs under the author's DID.
		orphans bool
		// subjectRowOrphan stores an unreferenced blob under the author's
		// DID held only by a community-removal subject-scope row.
		subjectRowOrphan bool
		// lift lifts the operator withholds before the scrub.
		lift        bool
		scrub       func(ctx context.Context, h *harness) error
		wantHeldRow []string
	}{
		{
			name:        "Delete{Actor} keeps an operator-withheld image and orphan blob",
			seed:        "b10b-delete-actor",
			orphans:     true,
			scrub:       deleteActor,
			wantHeldRow: []string{authorDID},
		},
		{
			name:        "nobridge suppression keeps an operator-withheld image",
			seed:        "b10c-nobridge",
			scrub:       suppressActor,
			wantHeldRow: []string{authorDID},
		},
		{
			name:             "Delete{Actor} deletes an image and orphan blob held only by a community-removal subject row",
			seed:             "b11-subject-row",
			subjectRowOnly:   true,
			subjectRowOrphan: true,
			scrub:            deleteActor,
			wantHeldRow:      nil,
		},
		{
			name:           "nobridge suppression deletes an image held only by a community-removal subject row",
			seed:           "b11-nobridge-subject-row",
			subjectRowOnly: true,
			scrub:          suppressActor,
			wantHeldRow:    nil,
		},
		{
			name:        "Delete{Actor} deletes an image and orphan blob whose operator withhold was lifted",
			seed:        "b10b-delete-actor-lifted",
			orphans:     true,
			lift:        true,
			scrub:       deleteActor,
			wantHeldRow: nil,
		},
		{
			name:        "nobridge suppression deletes an image whose operator withhold was lifted",
			seed:        "b10c-nobridge-lifted",
			lift:        true,
			scrub:       suppressActor,
			wantHeldRow: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.serveLemmyWorldFixtures()
			withholds := blobwithhold.NewStore(testutil.DB(t))
			ctx := context.Background()

			heldPostID := "https://lemmy.world/post/" + tc.seed + "-held"
			plainPostID := "https://lemmy.world/post/" + tc.seed + "-plain"
			materializeImagePost(t, h, heldPostID, "/media/"+tc.seed+"-held.png", []byte(tc.seed+"-held-image"))
			materializeImagePost(t, h, plainPostID, "/media/"+tc.seed+"-plain.png", []byte(tc.seed+"-plain-image"))
			heldCID := embedImageRef(t, h.recordFor(t, heldPostID)).CID
			plainCID := embedImageRef(t, h.recordFor(t, plainPostID)).CID
			require.NotEqual(t, heldCID, plainCID)
			require.Equal(t, []string{authorDID}, blobHoldersOfCID(t, heldCID), "setup: the image is stored under the author")
			require.Equal(t, []string{authorDID}, blobHoldersOfCID(t, plainCID), "setup: the image is stored under the author")

			var heldOrphanCID, plainOrphanCID string
			if tc.orphans {
				heldOrphan, err := h.manager.PutBlob(ctx, authorDID, "image/png", []byte(tc.seed+"-orphan-held"))
				require.NoError(t, err)
				heldOrphanCID = cid.Cid(heldOrphan.Ref).String()
				plainOrphan, err := h.manager.PutBlob(ctx, authorDID, "image/png", []byte(tc.seed+"-orphan-plain"))
				require.NoError(t, err)
				plainOrphanCID = cid.Cid(plainOrphan.Ref).String()
				operatorWithhold(t, withholds, heldOrphanCID)
			}

			var subjectRowOrphanCID string
			if tc.subjectRowOrphan {
				orphan, err := h.manager.PutBlob(ctx, authorDID, "image/png", []byte(tc.seed+"-orphan-subject-row"))
				require.NoError(t, err)
				subjectRowOrphanCID = cid.Cid(orphan.Ref).String()
				cleanupWithhold(t, subjectRowOrphanCID)
				_, err = withholds.Insert(ctx, blobwithhold.Withhold{
					CID:          subjectRowOrphanCID,
					Source:       blobwithhold.SourceCommunityRemoval,
					Scope:        blobwithhold.ScopeSubject,
					OwnerDID:     authorDID,
					SubjectURI:   "at://" + authorDID + "/social.coves.community.post/" + tc.seed,
					CommunityDID: communityDID,
					ReasonCode:   "moderator-discretion",
					CreatedBy:    "b11-community-moderator",
				})
				require.NoError(t, err)
			}

			if tc.subjectRowOnly {
				mapping, err := h.objects.GetByAPID(ctx, heldPostID)
				require.NoError(t, err)
				cleanupWithhold(t, heldCID)
				_, err = withholds.Insert(ctx, blobwithhold.Withhold{
					CID:          heldCID,
					Source:       blobwithhold.SourceCommunityRemoval,
					Scope:        blobwithhold.ScopeSubject,
					OwnerDID:     authorDID,
					SubjectURI:   mapping.ATURI,
					CommunityDID: communityDID,
					ReasonCode:   "moderator-discretion",
					CreatedBy:    "b11-community-moderator",
				})
				require.NoError(t, err)
			} else {
				operatorWithhold(t, withholds, heldCID)
			}

			if tc.lift {
				for _, liftCID := range []string{heldCID, heldOrphanCID} {
					if liftCID == "" {
						continue
					}
					_, err := withholds.Lift(ctx, liftCID, "b10-operator-lift")
					require.NoError(t, err)
				}
			}

			require.NoError(t, tc.scrub(ctx, h))

			for _, postID := range []string{heldPostID, plainPostID} {
				mapping, err := h.objects.GetByAPID(ctx, postID)
				require.NoError(t, err)
				assert.True(t, mapping.IsDeleted(), "%s: mapping deleted as today", postID)
				_, _, err = h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
				assert.True(t, errors.IsNotFound(err), "%s: record deleted as today; err=%v", postID, err)
			}
			assert.Empty(t, blobHoldersOfCID(t, plainCID), "the unwithheld image's bytes are deleted")
			assert.Equal(t, tc.wantHeldRow, blobHoldersOfCID(t, heldCID), "the held image's blobs rows after the scrub")
			if tc.orphans {
				if tc.lift {
					assert.Empty(t, blobHoldersOfCID(t, heldOrphanCID),
						"DeleteBlobsForDID deletes a blob whose operator withhold was lifted")
				} else {
					assert.Equal(t, []string{authorDID}, blobHoldersOfCID(t, heldOrphanCID),
						"DeleteBlobsForDID keeps an operator-withheld blob under the author's DID")
				}
				assert.Empty(t, blobHoldersOfCID(t, plainOrphanCID),
					"DeleteBlobsForDID still deletes an unwithheld blob under the author's DID")
			}
			if tc.subjectRowOrphan {
				assert.Empty(t, blobHoldersOfCID(t, subjectRowOrphanCID),
					"DeleteBlobsForDID deletes a blob held only by a subject-scope row")
			}
		})
	}
}

// materializeImagePost bridges a postv2 by the fixture author in the fixture
// community whose one image attachment serves imageBytes at mediaPath.
func materializeImagePost(t *testing.T, h *harness, postID, mediaPath string, imageBytes []byte) {
	t.Helper()
	h.mux.HandleFunc("GET "+mediaPath, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(imageBytes)
	})
	post := page(postID, personID, groupID, "an image post "+postID, "2026-10-22T10:00:00.000000Z")
	post["attachment"] = []any{map[string]any{
		"type": "Image", "href": "https://lemmy.world" + mediaPath,
		"mediaType": "image/png", "name": "a picture",
	}}
	_, err := h.m.MaterializePost(context.Background(), mustObject(t, post), groupID)
	require.NoError(t, err)
}
