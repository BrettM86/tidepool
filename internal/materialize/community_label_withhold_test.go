package materialize

import (
	"context"
	"testing"

	"github.com/bluesky-social/indigo/atproto/labeling"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/blobwithhold"
	"tidepool/internal/errors"
	"tidepool/internal/labels"
	"tidepool/internal/labels/labelstest"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// Community removal (chunk 05): a bare-DID `!takedown` on a bridged community
// (Group) DID withholds, for each record's owner, every blob CID of every
// hosted bridged record bound to that community (ap_objects.community_did),
// including records materialized or edited while the label stands. It is
// scoped by (owner, CID): the same bytes under an owner outside the community,
// and the owner's blobs outside it, are not covered. The negation lifts only
// the community's rows.

// Bridged persons on the community's host besides the fixture author (P1).
const (
	communityRemovalSecondAuthor = "https://lemmy.world/u/communityremovalsecond"
	communityRemovalThirdAuthor  = "https://lemmy.world/u/communityremovalthird"
)

// communityWithholdWorld is C (groupID, DID community) with P1's record
// (image X) and P2's record (image Y) bound to it, plus records in the other
// community: P2's image Z, P3's record with the same bytes as X, and P1's
// unrelated image U.
type communityWithholdWorld struct {
	h         *harness
	rig       *labelRig
	withholds *blobwithhold.Store

	community, p1, p2, p3 string
	postX, postY          string
	x, y, z, u            string
}

func newCommunityWithholdWorld(t *testing.T) *communityWithholdWorld {
	t.Helper()
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	h.serveObject("/c/elsewhere", group(otherGroupID, "elsewhere", nil))
	h.serveObject("/u/communityremovalsecond", person(communityRemovalSecondAuthor, "communityremovalsecond", nil))
	h.serveObject("/u/communityremovalthird", person(communityRemovalThirdAuthor, "communityremovalthird", nil))
	w := &communityWithholdWorld{
		h:         h,
		rig:       newLabelRig(t, h),
		withholds: blobwithhold.NewStore(testutil.DB(t)),
		community: testDIDFor("technology", "lemmy.world"),
		p1:        testDIDFor("LeftLeaningFreedomFighters", "lemmy.world"),
		p2:        testDIDFor("communityremovalsecond", "lemmy.world"),
		p3:        testDIDFor("communityremovalthird", "lemmy.world"),
		postX:     "https://lemmy.world/post/community-removal-x",
		postY:     "https://lemmy.world/post/community-removal-y",
	}
	xBytes := []byte("community-removal-image-x")
	const postZ = "https://lemmy.world/post/community-removal-z"
	const postX3 = "https://lemmy.world/post/community-removal-x-by-p3"
	const postU = "https://lemmy.world/post/community-removal-u"

	materializeImagePostBy(t, h, w.postX, personID, groupID, "/media/community-removal-x.png", xBytes)
	materializeImagePostBy(t, h, w.postY, communityRemovalSecondAuthor, groupID,
		"/media/community-removal-y.png", []byte("community-removal-image-y"))
	materializeImagePostBy(t, h, postZ, communityRemovalSecondAuthor, otherGroupID,
		"/media/community-removal-z.png", []byte("community-removal-image-z"))
	materializeImagePostBy(t, h, postX3, communityRemovalThirdAuthor, otherGroupID,
		"/media/community-removal-x-by-p3.png", xBytes)
	materializeImagePostBy(t, h, postU, personID, otherGroupID,
		"/media/community-removal-u.png", []byte("community-removal-image-u"))
	requireActorType(t, h, w.community, store.ActorTypeGroup)

	w.x = embedImageRef(t, h.recordFor(t, w.postX)).CID
	w.y = embedImageRef(t, h.recordFor(t, w.postY)).CID
	w.z = embedImageRef(t, h.recordFor(t, postZ)).CID
	w.u = embedImageRef(t, h.recordFor(t, postU)).CID
	require.Equal(t, w.x, embedImageRef(t, h.recordFor(t, postX3)).CID, "setup: P3's record references the same CID as X")
	for _, apID := range []string{w.postX, w.postY} {
		require.Equal(t, w.community, boundCommunityOf(t, apID), "setup: %s is bound to the community", apID)
	}
	for _, apID := range []string{postZ, postX3, postU} {
		require.NotEqual(t, w.community, boundCommunityOf(t, apID), "setup: %s is bound to another community", apID)
	}
	for _, c := range []struct{ owner, cid string }{{w.p1, w.x}, {w.p2, w.y}, {w.p2, w.z}, {w.p3, w.x}, {w.p1, w.u}} {
		require.False(t, covered(t, w.withholds, c.owner, c.cid), "setup: nothing withholds %s for %s yet", c.cid, c.owner)
	}
	return w
}

// boundCommunityOf is ap_objects.community_did of apID.
func boundCommunityOf(t *testing.T, apID string) string {
	t.Helper()
	var did string
	require.NoError(t, testutil.DB(t).QueryRow(
		`SELECT COALESCE(community_did, '') FROM ap_objects WHERE ap_id = $1`, apID).Scan(&did))
	return did
}

// commitCommunityLabel commits the community's bare-DID `!takedown` (or its
// negation) at seq.
func (w *communityWithholdWorld) commitCommunityLabel(t *testing.T, seq int64, neg bool) {
	t.Helper()
	l := labeling.Label{URI: w.community, Val: "!takedown"}
	if neg {
		l.Negated = negated()
	}
	w.rig.commit(t, seq, l)
}

// TestCommunityRemoval_LabelWithholdsTheCommunitysRecordBlobs (B5).
func TestCommunityRemoval_LabelWithholdsTheCommunitysRecordBlobs(t *testing.T) {
	t.Run("covers each record's CIDs for its owner, inside the community only", func(t *testing.T) {
		w := newCommunityWithholdWorld(t)
		w.commitCommunityLabel(t, 1, false)

		for _, tc := range []struct {
			name       string
			owner, cid string
			want       bool
		}{
			{name: "P1's image X in C", owner: w.p1, cid: w.x, want: true},
			{name: "P2's image Y in C", owner: w.p2, cid: w.y, want: true},
			{name: "P2's image Z in another community", owner: w.p2, cid: w.z, want: false},
			{name: "X's bytes under P3, outside C", owner: w.p3, cid: w.x, want: false},
			{name: "P1's unrelated image U outside C (no account-wide row)", owner: w.p1, cid: w.u, want: false},
		} {
			assert.Equal(t, tc.want, covered(t, w.withholds, tc.owner, tc.cid), tc.name)
		}
	})

	t.Run("records materialized or edited under the label are covered, and the negation lifts them", func(t *testing.T) {
		w := newCommunityWithholdWorld(t)
		ctx := context.Background()
		w.commitCommunityLabel(t, 1, false)
		require.True(t, covered(t, w.withholds, w.p1, w.x), "the community label covers P1's image X")

		const postW = "https://lemmy.world/post/community-removal-w"
		materializeImagePostBy(t, w.h, postW, personID, groupID, "/media/community-removal-w.png", []byte("community-removal-image-w"))
		imageW := embedImageRef(t, w.h.recordFor(t, postW)).CID
		require.Equal(t, w.community, boundCommunityOf(t, postW), "setup: the new record is bound to the community")
		assert.True(t, covered(t, w.withholds, w.p1, imageW), "a record materialized into C under the label is covered")

		serveImage(w.h, "/media/community-removal-v.png", []byte("community-removal-image-v"))
		_, err := w.h.m.HandleUpdate(ctx, mustObject(t, imagePostPage(t, w.postX, "/media/community-removal-v.png")), groupID)
		require.NoError(t, err)
		imageV := embedImageRef(t, w.h.recordFor(t, w.postX)).CID
		require.NotEqual(t, w.x, imageV, "setup: the edit added a new image")
		assert.True(t, covered(t, w.withholds, w.p1, imageV), "an edit of P1's record in C under the label is covered")

		w.commitCommunityLabel(t, 2, true)
		for _, tc := range []struct {
			name       string
			owner, cid string
		}{
			{name: "X", owner: w.p1, cid: w.x},
			{name: "Y", owner: w.p2, cid: w.y},
			{name: "W", owner: w.p1, cid: imageW},
			{name: "V", owner: w.p1, cid: imageV},
		} {
			assert.False(t, covered(t, w.withholds, tc.owner, tc.cid), "after the negation %s is no longer covered", tc.name)
		}
	})

	t.Run("the negation leaves an independent record label's withhold", func(t *testing.T) {
		w := newCommunityWithholdWorld(t)
		w.commitCommunityLabel(t, 1, false)
		require.True(t, covered(t, w.withholds, w.p2, w.y), "the community label covers P2's image Y")
		subjectY := subjectURIOf(t, w.h, w.postY)
		w.rig.commit(t, 2, labeling.Label{URI: subjectY, Val: "!takedown"})

		w.commitCommunityLabel(t, 3, true)
		assert.False(t, covered(t, w.withholds, w.p1, w.x), "the community negation lifts X")
		assert.True(t, covered(t, w.withholds, w.p2, w.y), "Y's record label still covers it after the community negation")

		w.rig.commit(t, 4, labeling.Label{URI: subjectY, Val: "!takedown", Negated: negated()})
		assert.False(t, covered(t, w.withholds, w.p2, w.y), "Y serves once its record label is negated too")
	})

	t.Run("a label stored before the applier existed is applied at consumer start", func(t *testing.T) {
		w := newCommunityWithholdWorld(t)
		// Task 02's consumer: the same tables, no materializer applier.
		withoutApplier := labels.NewPostgresStore(testutil.DB(t))
		require.NoError(t, withoutApplier.Commit(context.Background(), labels.Cursor{Epoch: 0, Seq: 1},
			w.rig.signed(t, labeling.Label{URI: w.community, Val: "!takedown"})))
		require.False(t, covered(t, w.withholds, w.p1, w.x), "setup: a store without the applier withholds nothing")

		server := labelstest.NewServer(t, func(int64) labelstest.Reply { return labelstest.Reply{Hold: true} })
		returned, _ := startLabelConsumer(t, w.rig.store,
			staticLabeler{endpoint: server.URL, key: labelstest.PublicKey(t, w.rig.key)})
		cursor, ok := awaitLabelConnection(server, returned)
		require.True(t, ok, "the consumer connects")
		assert.Equal(t, int64(1), cursor, "the consumer resumes from the stored cursor")
		assert.True(t, covered(t, w.withholds, w.p1, w.x), "the stored community label covers X before any new frame")
	})
}

// TestCommunityRemoval_ACommunityScanRacingARecordCommitLeavesNoBlobUncovered
// (B9): the community label's scan of the community's records and a record
// commit into the community, racing, end with every blob of the committed
// record covered (takedown) or with no community row active (negation), in
// either commit order. The gates are those of
// TestLabelTakedown_RacesAnEditOfTheSubject:
//   - label first (gateWithholdTable): the label transaction has listed the
//     community's records and stops at its first withhold write (X's row, or
//     the lift); the record commit then starts and must wait for the label to
//     commit, so it sees the label standing (or negated). If the scan did not
//     serialize with repo commits, the record would commit unseen by the scan
//     and without seeing the uncommitted label.
//   - record first (gateCommitLock): the record commit queues on the commit
//     lock first and the label transaction behind it; the record commits
//     without the label, and the label's scan then lists it.
func TestCommunityRemoval_ACommunityScanRacingARecordCommitLeavesNoBlobUncovered(t *testing.T) {
	tests := []struct {
		name        string
		slug        string
		negation    bool
		gate        labelRaceGate
		labelFirst  bool
		wantCovered bool
	}{
		{name: "takedown, label transaction first, new record", slug: "b9-takedown-label-first", gate: gateWithholdTable, labelFirst: true, wantCovered: true},
		{name: "takedown, record commit first, new record", slug: "b9-takedown-record-first", gate: gateCommitLock, wantCovered: true},
		{name: "negation, label transaction first, edit", slug: "b9-negation-label-first", negation: true, gate: gateWithholdTable, labelFirst: true},
		{name: "negation, edit first", slug: "b9-negation-edit-first", negation: true, gate: gateCommitLock},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := newCommunityWithholdWorld(t)
			ctx, cancel := context.WithTimeout(context.Background(), 2*labelWaitBound)
			defer cancel()
			seq := int64(1)
			if tc.negation {
				w.commitCommunityLabel(t, seq, false)
				require.True(t, covered(t, w.withholds, w.p1, w.x), "setup: the standing community label covers X")
				seq++
			}
			label := labeling.Label{URI: w.community, Val: "!takedown"}
			if tc.negation {
				label.Negated = negated()
			}
			at := labels.Cursor{Epoch: 0, Seq: seq}
			signed := w.rig.signed(t, label)

			// The racing record: a new image post by P1 in C (takedown), or
			// an edit of P1's record X to a new image (negation).
			postID := "https://lemmy.world/post/" + tc.slug
			if tc.negation {
				postID = w.postX
			}
			object := mustObject(t, imagePostPage(t, postID, "/media/"+tc.slug+".png"))
			commitRecord := func() error {
				_, err := w.h.m.MaterializePost(ctx, object, groupID)
				return err
			}
			if tc.negation {
				commitRecord = func() error {
					_, err := w.h.m.HandleUpdate(ctx, object, groupID)
					return err
				}
			}
			serveImage(w.h, "/media/"+tc.slug+".png", []byte(tc.slug+"-image"))
			commitLabel := func() error { return w.rig.store.Commit(ctx, at, signed) }

			release := holdLabelRaceGate(t, tc.gate)
			var labelSide, recordSide *raceSide
			if tc.labelFirst {
				labelSide = startRaceSide(commitLabel)
				awaitSidesBlocked(t, labelSide)
				require.False(t, labelSide.returned(), "setup: the label transaction is held at its withhold write")
				recordSide = startRaceSide(commitRecord)
			} else {
				recordSide = startRaceSide(commitRecord)
				awaitSidesBlocked(t, recordSide)
				require.False(t, recordSide.returned(), "setup: the record commit is held on the commit lock")
				labelSide = startRaceSide(commitLabel)
			}
			awaitSidesBlocked(t, labelSide, recordSide)
			release()
			awaitSide(t, labelSide, "the community label commit")
			awaitSide(t, recordSide, "the record commit")

			require.Equal(t, w.community, boundCommunityOf(t, postID), "setup: the racing record is bound to the community")
			image := embedImageRef(t, w.h.recordFor(t, postID)).CID
			require.NotEqual(t, w.x, image, "setup: the racing record references a new image")
			if !tc.negation {
				assert.True(t, covered(t, w.withholds, w.p1, image), "the new record's image is covered for P1")
				return
			}
			assert.False(t, covered(t, w.withholds, w.p1, image), "the edit's image is not covered after the negation")
			assert.False(t, covered(t, w.withholds, w.p1, w.x), "X is not covered after the negation")
			assert.False(t, covered(t, w.withholds, w.p2, w.y), "Y is not covered after the negation")
		})
	}
}

// TestCommunityRemoval_AuthorDeleteUnderTheCommunityLabelKeepsTheBytes: a
// community `!takedown` is a Coves takedown, so the author's delete of a
// record in the removed community keeps the image's bytes, with no record
// label and no moderator removal of the record. The community rows of the
// deleted record's (owner, CID) are marked author-deleted, so the community's
// negation leaves them refused while the community's live records serve.
func TestCommunityRemoval_AuthorDeleteUnderTheCommunityLabelKeepsTheBytes(t *testing.T) {
	w := newCommunityWithholdWorld(t)
	ctx := context.Background()
	subjectX := subjectURIOf(t, w.h, w.postX)
	var recordLabels int
	require.NoError(t, testutil.DB(t).QueryRow(`SELECT count(*) FROM coves_labels WHERE uri = $1`, subjectX).Scan(&recordLabels))
	require.Zero(t, recordLabels, "setup: the record has no label of its own")

	w.commitCommunityLabel(t, 1, false)
	require.True(t, covered(t, w.withholds, w.p1, w.x), "setup: the community label covers X")
	require.NoError(t, w.h.m.HandleAuthorDeleteRecord(ctx, w.postX))

	assert.Contains(t, blobHoldersOfCID(t, w.x), w.p1, "the delete under the community label keeps X's bytes under P1")
	assert.True(t, covered(t, w.withholds, w.p1, w.x), "the kept bytes stay withheld")
	_, _, err := w.h.manager.GetBlob(ctx, w.p1, w.x)
	assert.True(t, errors.IsNotFound(err), "getBlob refuses the kept bytes, got %v", err)

	w.commitCommunityLabel(t, 2, true)
	assert.True(t, covered(t, w.withholds, w.p1, w.x), "the negation keeps the deleted record's author-deleted row")
	assert.Contains(t, blobHoldersOfCID(t, w.x), w.p1, "the bytes stay stored after the negation")
	_, _, err = w.h.manager.GetBlob(ctx, w.p1, w.x)
	assert.True(t, errors.IsNotFound(err), "getBlob still refuses the kept bytes after the negation, got %v", err)
	assert.False(t, covered(t, w.withholds, w.p2, w.y), "the community's live record Y serves after the negation")
}

// TestCommunityRemoval_LabelResolvesRecordsWithNoStoredCommunity: a bridged
// record whose ap_objects.community_did is NULL (a postv2 written before the
// column existed) is bound to its community through its stored record. The
// community label covers its blobs, and the binding is written in the label's
// transaction, so the SQL readers of the column (the outbound parent check)
// see it too. A NULL row whose community cannot be derived is counted.
func TestCommunityRemoval_LabelResolvesRecordsWithNoStoredCommunity(t *testing.T) {
	w := newCommunityWithholdWorld(t)
	ctx := context.Background()
	database := testutil.DB(t)
	_, err := database.Exec(`UPDATE ap_objects SET community_did = NULL WHERE ap_id = $1`, w.postY)
	require.NoError(t, err)
	require.Empty(t, boundCommunityOf(t, w.postY), "setup: Y's row has no stored community")
	subjectY := subjectURIOf(t, w.h, w.postY)

	// A NULL row whose record is not in the repo: its community cannot be
	// derived.
	const unresolvableAPID = "https://lemmy.world/post/community-removal-unresolvable"
	unresolvableURI := "at://" + w.p2 + "/" + CollectionPostV2 + "/3zzzzzzzzzzzz"
	_, err = database.Exec(`
		INSERT INTO ap_objects (ap_id, ap_type, origin_instance, origin, did, collection, rkey, at_uri, cid)
		SELECT $1, ap_type, origin_instance, origin, did, collection, '3zzzzzzzzzzzz', $2, cid
		FROM ap_objects WHERE ap_id = $3`, unresolvableAPID, unresolvableURI, w.postY)
	require.NoError(t, err)

	unresolvedBefore := CommunityLabelUnresolvedRecords.Value()
	w.commitCommunityLabel(t, 1, false)

	assert.True(t, covered(t, w.withholds, w.p2, w.y), "Y's image is covered through its record's community")
	assert.Equal(t, w.community, boundCommunityOf(t, w.postY), "the label's transaction writes Y's derived binding")
	removed, err := store.NewOutboundTakedowns(database).IsParentRemoved(ctx, subjectY)
	require.NoError(t, err)
	assert.True(t, removed, "a reply to Y is blocked while the community label stands")
	assert.Empty(t, boundCommunityOf(t, unresolvableAPID), "an underivable row stays unbound")
	assert.Equal(t, unresolvedBefore+1, CommunityLabelUnresolvedRecords.Value(), "the underivable row is counted")

	// The commit side: an edit of a row with no stored community, committed
	// while the label stands, binds it from the record and covers its blobs.
	_, err = database.Exec(`UPDATE ap_objects SET community_did = NULL WHERE ap_id = $1`, w.postY)
	require.NoError(t, err)
	serveImage(w.h, "/media/community-removal-y-edit.png", []byte("community-removal-image-y-edit"))
	_, err = w.h.m.HandleUpdate(ctx, mustObject(t, imagePostPage(t, w.postY, "/media/community-removal-y-edit.png")), groupID)
	require.NoError(t, err)
	edited := embedImageRef(t, w.h.recordFor(t, w.postY)).CID
	require.NotEqual(t, w.y, edited, "setup: the edit references a new image")
	assert.Equal(t, w.community, boundCommunityOf(t, w.postY), "the edit's commit binds the row from its record")
	assert.True(t, covered(t, w.withholds, w.p2, edited), "the edit's image is covered in its own commit")
}

// TestCommunityRemoval_UndeleteUnderTheCommunityLabelClearsTheAuthorDeletedMark:
// the record's undelete while the community label stands gives its
// author-deleted rows the label-takedown code back, so the negation lifts
// them and the restored record's image serves again.
func TestCommunityRemoval_UndeleteUnderTheCommunityLabelClearsTheAuthorDeletedMark(t *testing.T) {
	w := newCommunityWithholdWorld(t)
	ctx := context.Background()
	w.commitCommunityLabel(t, 1, false)
	require.NoError(t, w.h.m.HandleAuthorDeleteRecord(ctx, w.postX))

	require.NoError(t, w.h.objects.Restore(ctx, w.postX))
	_, err := w.h.m.HandleUpdate(ctx, mustObject(t, imagePostPage(t, w.postX, "/media/community-removal-x.png")), groupID)
	require.NoError(t, err, "the undelete re-materializes the record")
	require.Equal(t, w.x, embedImageRef(t, w.h.recordFor(t, w.postX)).CID, "setup: the restored record references X again")
	assert.True(t, covered(t, w.withholds, w.p1, w.x), "the restored record's image stays covered while the label stands")

	w.commitCommunityLabel(t, 2, true)
	assert.False(t, covered(t, w.withholds, w.p1, w.x), "the negation lifts the restored record's row")
}

// TestCommunityRemoval_AuthorDeleteKeepsAnEarlierVersionsImageWithheld: an
// edit committed while the community label stands can drop or replace the
// image the label's scan withheld (an Announce that passed the follow-state
// gate before the label's transaction commits after it). The author's later
// delete marks the earlier version's row author-deleted too, since no
// remaining record of the owner references that CID, so the community's
// negation keeps the kept bytes refused.
func TestCommunityRemoval_AuthorDeleteKeepsAnEarlierVersionsImageWithheld(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(t *testing.T, w *communityWithholdWorld) map[string]any
	}{
		{
			name: "edit drops the image",
			edit: func(t *testing.T, w *communityWithholdWorld) map[string]any {
				return page(w.postX, personID, groupID, "the image was removed", imagePostPublished)
			},
		},
		{
			name: "edit replaces the image",
			edit: func(t *testing.T, w *communityWithholdWorld) map[string]any {
				serveImage(w.h, "/media/community-removal-x-replacement.png", []byte("community-removal-image-x-replacement"))
				return imagePostPage(t, w.postX, "/media/community-removal-x-replacement.png")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newCommunityWithholdWorld(t)
			ctx := context.Background()
			w.commitCommunityLabel(t, 1, false)
			require.True(t, covered(t, w.withholds, w.p1, w.x), "setup: the community label covers X")

			_, err := w.h.m.HandleUpdate(ctx, mustObject(t, tc.edit(t, w)), groupID)
			require.NoError(t, err)
			current := recordBlobCIDs(w.h.recordFor(t, w.postX))
			require.NotContains(t, current, w.x, "setup: the edit no longer references X")
			require.Contains(t, blobHoldersOfCID(t, w.x), w.p1, "setup: X's bytes stay stored after the edit")

			require.NoError(t, w.h.m.HandleAuthorDeleteRecord(ctx, w.postX))
			w.commitCommunityLabel(t, 2, true)

			assert.True(t, covered(t, w.withholds, w.p1, w.x), "the negation keeps the earlier version's image withheld")
			assert.Contains(t, blobHoldersOfCID(t, w.x), w.p1, "X's bytes stay stored")
			_, _, err = w.h.manager.GetBlob(ctx, w.p1, w.x)
			assert.True(t, errors.IsNotFound(err), "getBlob refuses the earlier version's image after the negation, got %v", err)
			for _, c := range current {
				assert.True(t, covered(t, w.withholds, w.p1, c), "the deleted version's image %s stays withheld too", c)
			}
			assert.False(t, covered(t, w.withholds, w.p2, w.y), "the community's live record Y serves after the negation")
		})
	}
}

// TestCommunityRemoval_AuthorDeleteUnderAnotherRemovalSourceKeepsAnEarlierVersionsImageWithheld:
// the earlier version's image stays refused after the community's negation
// when a record `!takedown` or a moderator removal also stands for the deleted
// record, and those sources keep their own rows on the deleted version's
// image as before.
func TestCommunityRemoval_AuthorDeleteUnderAnotherRemovalSourceKeepsAnEarlierVersionsImageWithheld(t *testing.T) {
	for _, tc := range []struct {
		name string
		// moderate applies the other source to the edited record at seq 2.
		moderate func(t *testing.T, w *communityWithholdWorld, mapping *store.APObjectMapping)
		// wantRows checks the other source's rows on the deleted version's
		// images after the community's negation.
		wantRows func(t *testing.T, w *communityWithholdWorld, mapping *store.APObjectMapping, current []string)
	}{
		{
			name: "record takedown label",
			moderate: func(t *testing.T, w *communityWithholdWorld, mapping *store.APObjectMapping) {
				w.rig.commit(t, 2, labeling.Label{URI: mapping.ATURI, Val: "!takedown"})
			},
			wantRows: func(t *testing.T, w *communityWithholdWorld, mapping *store.APObjectMapping, current []string) {
				var want []labelRowState
				for _, c := range current {
					want = append(want, labelRowState{CID: c, Lifted: false})
				}
				assert.Equal(t, want, labelRowsOfSubject(t, mapping.ATURI), "the record label's rows stay active")
				var reasonCodes []string
				rows, err := testutil.DB(t).Query(`
					SELECT reason_code FROM blob_withholds WHERE source = 'label' AND subject_uri = $1`, mapping.ATURI)
				require.NoError(t, err)
				defer func() { _ = rows.Close() }()
				for rows.Next() {
					var code string
					require.NoError(t, rows.Scan(&code))
					reasonCodes = append(reasonCodes, code)
				}
				require.NoError(t, rows.Err())
				for _, code := range reasonCodes {
					assert.Equal(t, blobwithhold.ReasonLabelTakedownAuthorDeleted, code,
						"the author's delete marks the record label's rows")
				}
			},
		},
		{
			name: "moderator removal",
			moderate: func(t *testing.T, w *communityWithholdWorld, mapping *store.APObjectMapping) {
				require.NoError(t, w.h.m.RemovePost(context.Background(), mapping, "spam wave"))
			},
			wantRows: func(t *testing.T, w *communityWithholdWorld, mapping *store.APObjectMapping, current []string) {
				var active []string
				rows, err := testutil.DB(t).Query(`
					SELECT cid FROM blob_withholds
					WHERE source = 'community-removal' AND subject_uri = $1 AND lifted_at IS NULL ORDER BY cid`, mapping.ATURI)
				require.NoError(t, err)
				defer func() { _ = rows.Close() }()
				for rows.Next() {
					var c string
					require.NoError(t, rows.Scan(&c))
					active = append(active, c)
				}
				require.NoError(t, rows.Err())
				assert.ElementsMatch(t, current, active, "the removal's rows stay active on the deleted version's images")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newCommunityWithholdWorld(t)
			ctx := context.Background()
			w.commitCommunityLabel(t, 1, false)
			require.True(t, covered(t, w.withholds, w.p1, w.x), "setup: the community label covers X")

			serveImage(w.h, "/media/community-removal-x-replacement.png", []byte("community-removal-image-x-replacement"))
			_, err := w.h.m.HandleUpdate(ctx, mustObject(t, imagePostPage(t, w.postX, "/media/community-removal-x-replacement.png")), groupID)
			require.NoError(t, err)
			current := recordBlobCIDs(w.h.recordFor(t, w.postX))
			require.NotEmpty(t, current, "setup: the edit references a replacement image")
			require.NotContains(t, current, w.x, "setup: the edit no longer references X")
			mapping, err := w.h.objects.GetByAPID(ctx, w.postX)
			require.NoError(t, err)
			tc.moderate(t, w, mapping)

			require.NoError(t, w.h.m.HandleAuthorDeleteRecord(ctx, w.postX))
			w.commitCommunityLabel(t, 3, true)

			assert.True(t, covered(t, w.withholds, w.p1, w.x), "the negation keeps the earlier version's image withheld")
			assert.Contains(t, blobHoldersOfCID(t, w.x), w.p1, "X's bytes stay stored")
			_, _, err = w.h.manager.GetBlob(ctx, w.p1, w.x)
			assert.True(t, errors.IsNotFound(err), "getBlob refuses the earlier version's image after the negation, got %v", err)
			for _, c := range current {
				assert.True(t, covered(t, w.withholds, w.p1, c), "the deleted version's image %s stays withheld", c)
				assert.Contains(t, blobHoldersOfCID(t, c), w.p1, "the deleted version's image %s stays stored", c)
			}
			tc.wantRows(t, w, mapping, current)
			assert.False(t, covered(t, w.withholds, w.p2, w.y), "the community's live record Y serves after the negation")
		})
	}
}
