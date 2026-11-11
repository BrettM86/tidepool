package ingest

import (
	"context"
	"testing"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/labeling"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/blobwithhold"
	"tidepool/internal/labels"
	"tidepool/internal/labels/labelstest"
	"tidepool/internal/materialize"
	"tidepool/internal/testutil"
)

// A Coves `!takedown` NEGATED WHILE ITS SUBJECT RECORD IS GONE.
//
// A delete that is not the author's own (the sweep, an actor scrub, any
// HandleDeleteRecord) removes the record but keeps its bytes. A negation that
// arrives while the record is gone lifts the subject's label rows, so a later
// restore of the record at the same at-uri, and any other record of the owner
// that shares a CID, is served again.

// labelSourceDID is the Coves labeler these tests' labels name.
const labelSourceDID = "did:web:coves.social"

// labelCommitter commits signed Coves labels into the Postgres label store
// with the harness materializer's applier, as the label consumer would.
type labelCommitter struct {
	store *labels.PostgresStore
	sign  func(labeling.Label) labeling.Label
	seq   int64
	// key signs the labels; a label consumer verifying them resolves its
	// public half.
	key *atcrypto.PrivateKeyK256
}

func newLabelCommitter(t *testing.T, h *harness) *labelCommitter {
	t.Helper()
	return newLabelCommitterWith(t, h, h.mat.LabelApplier())
}

// newLabelCommitterWith is newLabelCommitter over a store running appliers
// (none: the labels are stored and nothing acts on them).
func newLabelCommitterWith(t *testing.T, h *harness, appliers ...labels.Applier) *labelCommitter {
	t.Helper()
	testutil.Truncate(t, h.db, "coves_labels", "coves_label_cursor")
	key := labelstest.NewKey(t)
	return &labelCommitter{
		store: labels.NewPostgresStore(h.db, appliers...),
		sign: func(l labeling.Label) labeling.Label {
			l.SourceDID = labelSourceDID
			return labelstest.Sign(t, key, l)
		},
		key: key,
	}
}

// commit commits val on subject at the next seq; negate sends its negation.
func (c *labelCommitter) commit(t *testing.T, subject, val string, negate bool) {
	t.Helper()
	require.NoError(t, c.attempt(t, subject, val, negate), "commit label seq %d", c.seq+1)
}

// attempt is commit returning the store's error. The seq advances only when
// the commit succeeds, so a retry after a failure commits at the same seq.
func (c *labelCommitter) attempt(t *testing.T, subject, val string, negate bool) error {
	t.Helper()
	l := labeling.Label{URI: subject, Val: val}
	if negate {
		l.Negated = &negate
	}
	if err := c.store.Commit(context.Background(), labels.Cursor{Epoch: 0, Seq: c.seq + 1}, c.sign(l)); err != nil {
		return err
	}
	c.seq++
	return nil
}

// TestALabelNegatedWhileTheRecordIsDeletedLetsARestoreServeTheImage: a
// takedown on U's image post, a non-author delete of the record, the
// negation, then C's announced Undo{Delete} restoring the post at the same
// at-uri: the restored post's image is served.
func TestALabelNegatedWhileTheRecordIsDeletedLetsARestoreServeTheImage(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	labelsRig := newLabelCommitter(t, h)
	ctx := context.Background()

	const slug = "lw-restore"
	postID := world.imagePost(world.communityC, slug, "lw-restore-image")
	image := recordBlobCIDs(t, h, postID)[0]
	subject := atURIOf(t, h, postID)

	labelsRig.commit(t, subject, materialize.LabelTakedown, false)
	assertBlobRefused(t, h, ubUserDID, image, "while the takedown stands")

	require.NoError(t, h.mat.HandleDeleteRecord(ctx, postID), "a non-author delete of the post")
	assertBlobStored(t, h, ubUserDID, image, "a non-author delete keeps the bytes")

	labelsRig.commit(t, subject, materialize.LabelTakedown, true)

	h.announceUndoDelete(world.communityC,
		world.communityC.id+"/activities/announce/undo/"+slug,
		"https://lemmy.world/activities/delete/"+slug,
		postID, nil)
	require.Equal(t, subject, atURIOf(t, h, postID), "precondition: the restore re-materialized the same at-uri")
	require.Equal(t, []string{image}, recordBlobCIDs(t, h, postID), "precondition: the restored post references the image")
	assertBlobServed(t, h, ubUserDID, image, "the takedown was negated and the post restored")
}

// TestALabelNegatedWhileTheRecordIsDeletedServesTheOwnersOtherRecord: U's
// second post shares the labeled post's image. After a non-author delete of
// the labeled post and the negation, the second post's image is served.
func TestALabelNegatedWhileTheRecordIsDeletedServesTheOwnersOtherRecord(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	labelsRig := newLabelCommitter(t, h)
	ctx := context.Background()

	labeledID := world.imagePost(world.communityC, "lw-shared-labeled", "lw-shared-image")
	otherID := world.imagePost(world.communityD, "lw-shared-other", "lw-shared-image")
	image := recordBlobCIDs(t, h, labeledID)[0]
	require.Equal(t, []string{image}, recordBlobCIDs(t, h, otherID), "precondition: both posts reference one image CID")
	subject := atURIOf(t, h, labeledID)

	labelsRig.commit(t, subject, materialize.LabelTakedown, false)
	assertBlobRefused(t, h, ubUserDID, image, "the takedown refuses the owner's CID on every record")

	require.NoError(t, h.mat.HandleDeleteRecord(ctx, labeledID), "a non-author delete of the labeled post")
	labelsRig.commit(t, subject, materialize.LabelTakedown, true)

	assertLive(t, h, otherID, "the other post is untouched")
	assertBlobServed(t, h, ubUserDID, image, "the negation lifts the deleted subject's label rows")
}

// activeLabelWithholds returns the active label rows of subject on cidStr.
func activeLabelWithholds(t *testing.T, h *harness, cidStr, subject string) []blobwithhold.Withhold {
	t.Helper()
	state, err := blobwithhold.NewStore(h.db).Inspect(context.Background(), cidStr)
	require.NoError(t, err)
	var active []blobwithhold.Withhold
	for _, row := range state.Withholds {
		if row.Source == blobwithhold.SourceLabel && row.SubjectURI == subject && row.LiftedAt == nil {
			active = append(active, row)
		}
	}
	return active
}

// labelReasonCodes returns the reason codes of rows, in order.
func labelReasonCodes(rows []blobwithhold.Withhold) []string {
	codes := make([]string, 0, len(rows))
	for _, row := range rows {
		codes = append(codes, row.ReasonCode)
	}
	return codes
}

// TestAnAuthorsUndeleteAfterTheNegationServesTheImage: a takedown on U's
// image post, U's own delete on Lemmy (the rows are marked author-deleted and
// survive the negation), the negation, then U's own announced Undo{Delete}
// re-materializing the post at the same at-uri: no takedown stands, so the
// restored post's commit lifts the marked rows and the image is served.
func TestAnAuthorsUndeleteAfterTheNegationServesTheImage(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	labelsRig := newLabelCommitter(t, h)

	const slug = "lw-author-undelete-after-negation"
	postID := world.imagePost(world.communityC, slug, "lw-author-undelete-after-negation-image")
	image := recordBlobCIDs(t, h, postID)[0]
	subject := atURIOf(t, h, postID)

	labelsRig.commit(t, subject, materialize.LabelTakedown, false)
	assertBlobRefused(t, h, ubUserDID, image, "while the takedown stands")

	h.announceDeleteBy(world.communityC, world.communityC.id+"/activities/announce/delete/"+slug,
		postID, ubUserID, nil)
	require.Equal(t, []string{blobwithhold.ReasonLabelTakedownAuthorDeleted},
		labelReasonCodes(activeLabelWithholds(t, h, image, subject)),
		"precondition: the author's own delete marked the label row author-deleted")

	labelsRig.commit(t, subject, materialize.LabelTakedown, true)
	require.Len(t, activeLabelWithholds(t, h, image, subject), 1,
		"precondition: the negation keeps the author-deleted row while the record is gone")
	assertBlobStored(t, h, ubUserDID, image, "the author's delete under a standing label keeps the bytes")

	h.announceUndoDeleteBy(world.communityC, world.communityC.id+"/activities/announce/undo/"+slug,
		world.communityC.id+"/activities/announce/delete/"+slug+"/delete", postID, ubUserID, nil)
	require.Equal(t, subject, atURIOf(t, h, postID), "precondition: the undelete re-materialized the same at-uri")
	require.Equal(t, []string{image}, recordBlobCIDs(t, h, postID), "precondition: the restored post references the image")

	assert.Empty(t, labelReasonCodes(activeLabelWithholds(t, h, image, subject)),
		"no takedown stands, so the restored post's commit lifts the subject's label rows")
	assertBlobServed(t, h, ubUserDID, image, "the takedown was negated and the author restored the post")
}

// TestAnAuthorsUndeleteWhileTheLabelStandsRecodesTheRowsSoALaterNegationLiftsThem:
// a takedown on U's image post, U's own delete and own Undo{Delete} while the
// label stands: the restored post's commit re-codes the marked rows to
// label-takedown and keeps them active. A later non-author delete and the
// negation then lift them, so a moderator's restore serves the image.
func TestAnAuthorsUndeleteWhileTheLabelStandsRecodesTheRowsSoALaterNegationLiftsThem(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	labelsRig := newLabelCommitter(t, h)
	ctx := context.Background()

	const slug = "lw-author-undelete-while-standing"
	postID := world.imagePost(world.communityC, slug, "lw-author-undelete-while-standing-image")
	image := recordBlobCIDs(t, h, postID)[0]
	subject := atURIOf(t, h, postID)

	labelsRig.commit(t, subject, materialize.LabelTakedown, false)

	h.announceDeleteBy(world.communityC, world.communityC.id+"/activities/announce/delete/"+slug,
		postID, ubUserID, nil)
	require.Equal(t, []string{blobwithhold.ReasonLabelTakedownAuthorDeleted},
		labelReasonCodes(activeLabelWithholds(t, h, image, subject)),
		"precondition: the author's own delete marked the label row author-deleted")

	h.announceUndoDeleteBy(world.communityC, world.communityC.id+"/activities/announce/undo/"+slug,
		world.communityC.id+"/activities/announce/delete/"+slug+"/delete", postID, ubUserID, nil)
	require.Equal(t, subject, atURIOf(t, h, postID), "precondition: the undelete re-materialized the same at-uri")
	require.Equal(t, []string{image}, recordBlobCIDs(t, h, postID), "precondition: the restored post references the image")

	assert.Equal(t, []string{blobwithhold.ReasonLabelTakedown},
		labelReasonCodes(activeLabelWithholds(t, h, image, subject)),
		"the takedown still stands, so the restored post's commit re-codes the marked row to label-takedown")
	assertBlobRefused(t, h, ubUserDID, image, "the takedown still stands on the restored post")

	require.NoError(t, h.mat.HandleDeleteRecord(ctx, postID), "a non-author delete of the post")
	labelsRig.commit(t, subject, materialize.LabelTakedown, true)
	assert.Empty(t, labelReasonCodes(activeLabelWithholds(t, h, image, subject)),
		"the negation after a non-author delete lifts the re-coded rows")

	h.announceUndoDelete(world.communityC,
		world.communityC.id+"/activities/announce/undo/"+slug+"-moderator",
		"https://lemmy.world/activities/delete/"+slug+"-moderator",
		postID, nil)
	require.Equal(t, subject, atURIOf(t, h, postID), "precondition: the moderator's restore re-materialized the same at-uri")
	assertBlobServed(t, h, ubUserDID, image, "the takedown was negated and a moderator restored the post")
}
