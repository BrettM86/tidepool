package ingest

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/blobwithhold"
	"tidepool/internal/errors"
	"tidepool/internal/identity"
	"tidepool/internal/materialize"
	"tidepool/internal/repo"
)

// COMMUNITY-REMOVAL WITHHOLDS UNDER CONCURRENT COMMITS, AND THEIR REASON CODE.
//
// Every repo commit takes one global postgres advisory transaction lock. The
// race tests hold that lock from a dedicated session, start the commit that
// must land FIRST and wait until it queues on the lock, then start the second
// commit and wait until it queues too: everything the second call reads before
// its commit is read before the first commit lands. Releasing the lock grants
// the queued waiters in arrival order, so the interleaving is fixed.

// raceCommitLockKey is internal/repo's commitAdvisoryLockKey.
const raceCommitLockKey int64 = 0x7469646570636d

// commitLockWaiters counts the sessions queued on the commit lock.
func commitLockWaiters(ctx context.Context, database *sql.DB) (int, error) {
	var waiting int
	err := database.QueryRowContext(ctx, `
		SELECT count(*) FROM pg_locks
		WHERE locktype = 'advisory' AND NOT granted
		  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
		  AND classid::bigint = $1 AND objid::bigint = $2`,
		uint64(raceCommitLockKey)>>32, uint64(raceCommitLockKey)&0xffffffff).Scan(&waiting)
	return waiting, err
}

// commitInOrder runs first and second concurrently with second's pre-commit
// reads happening before first's commit, and first's commit landing before
// second's. It returns once both calls returned, failing the test on either
// call's error.
func commitInOrder(t *testing.T, h *harness, first, second func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	lockSession, err := h.db.Conn(ctx)
	require.NoError(t, err)
	defer func() { _ = lockSession.Close() }()
	_, err = lockSession.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, raceCommitLockKey)
	require.NoError(t, err)
	unlocked := false
	unlock := func() {
		if unlocked {
			return
		}
		unlocked = true
		_, err := lockSession.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, raceCommitLockKey)
		require.NoError(t, err)
	}
	defer unlock()

	results := make(chan error, 2)
	waitForWaiters := func(want int, why string) {
		t.Helper()
		for {
			waiting, err := commitLockWaiters(ctx, h.db)
			require.NoError(t, err)
			if waiting >= want {
				return
			}
			select {
			case err := <-results:
				t.Fatalf("%s: a call returned before queuing on the commit lock: %v", why, err)
			case <-ctx.Done():
				t.Fatalf("%s: timed out waiting for %d commit-lock waiters", why, want)
			case <-time.After(10 * time.Millisecond):
			}
		}
	}

	go func() { results <- first(ctx) }()
	waitForWaiters(1, "the first commit")
	go func() { results <- second(ctx) }()
	waitForWaiters(2, "the second commit")
	unlock()

	for range 2 {
		select {
		case err := <-results:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Fatal("timed out waiting for the racing commits")
		}
	}
}

// storedCIDOfSeed is the CID of the blobs row under U holding seed's fixture
// bytes.
func storedCIDOfSeed(t *testing.T, h *harness, seed string) string {
	t.Helper()
	var found string
	require.NoError(t, h.db.QueryRowContext(context.Background(),
		`SELECT cid FROM blobs WHERE did = $1 AND bytes = $2`, ubUserDID, rwImageBytes(seed)).Scan(&found),
		"%s's bytes are stored under U", seed)
	return found
}

// editedPageObject serves postID's edit carrying one image per seed and returns
// it as the AP object a re-fetch or an Update hands the materializer.
func (w *upstreamBanWorld) editedPageObject(slug, version string, seeds ...string) *ap.Object {
	w.h.t.Helper()
	page := w.rwPage(w.communityC, slug, version, w.rwImageAttachments(slug+"-"+version, seeds...))
	w.h.serveObject("/post/"+slug, page)
	raw, err := json.Marshal(page)
	require.NoError(w.h.t, err)
	object, err := ap.ParseObject(raw)
	require.NoError(w.h.t, err)
	return object
}

// activeRemovalRowsOf lists the subject's active community-removal rows.
func activeRemovalRowsOf(t *testing.T, h *harness, subjectURI string) []blobwithhold.Withhold {
	t.Helper()
	cids, err := func() ([]string, error) {
		tx, err := h.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
		if err != nil {
			return nil, err
		}
		defer func() { _ = tx.Rollback() }()
		return blobwithhold.ActiveSubjectRemovalCIDs(context.Background(), tx, subjectURI)
	}()
	require.NoError(t, err)
	var rows []blobwithhold.Withhold
	for _, c := range cids {
		for _, row := range activeRemovalWithholds(t, h, c) {
			if row.SubjectURI == subjectURI {
				rows = append(rows, row)
			}
		}
	}
	return rows
}

// TestAnEditCommittedAfterARemovalItRacedWithholdsItsNewImage (race A).
//
// GIVEN U's postv2 with image A in C; WHEN C's moderator removal commits first
// and a re-materialization that swaps A for B, which read the post's removal
// state before the removal landed, commits second THEN B is refused under the
// removal's code and A stays withheld for the post (only a restore lifts).
func TestAnEditCommittedAfterARemovalItRacedWithholdsItsNewImage(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	const slug = "rw-race-a"
	postID := world.imagePost(world.communityC, slug, "race-a-image-a")
	subject := atURIOf(t, h, postID)
	imageA := storedCIDOfSeed(t, h, "race-a-image-a")
	mapping, err := h.objects.GetByAPID(context.Background(), postID)
	require.NoError(t, err)
	edit := world.editedPageObject(slug, "2026-08-21T12:00:00Z", "race-a-image-b")

	commitInOrder(t, h,
		func(ctx context.Context) error { return h.mat.RemovePost(ctx, mapping, rwSummary) },
		func(ctx context.Context) error {
			_, err := h.mat.HandleUpdate(ctx, edit, world.communityC.id)
			return err
		})

	imageB := storedCIDOfSeed(t, h, "race-a-image-b")
	require.Equal(t, []string{imageB}, recordBlobCIDs(t, h, postID), "precondition: the edit committed image B")
	require.Equal(t, materialize.RemovalCodeModeratorDiscretion, removalCode(t, h, ubCommunityCDID, postID),
		"precondition: the removal stands")
	assertBlobRefused(t, h, ubUserDID, imageB, "the new image of a post removed before the edit committed")
	assertOneRemovalWithhold(t, h, imageB, subject, ubUserDID)
	assertOneRemovalWithhold(t, h, imageA, subject, ubUserDID)
	assertBlobRefused(t, h, ubUserDID, imageA, "the image a still-removed post dropped")
}

// TestABackfillRacingAnEditOfARemovedPostAddsOnlyWhatTheEditLeftMissing (race
// A, back-fill).
//
// GIVEN U's postv2 with image A, removed by C (A withheld); WHEN an edit
// swapping A for B commits first and a back-fill run, which read the post
// before the edit landed, commits second THEN B stays refused, A keeps the
// one row the removal wrote (the back-fill adds no second one), and the
// back-fill reports the post already withheld.
func TestABackfillRacingAnEditOfARemovedPostAddsOnlyWhatTheEditLeftMissing(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	const slug = "rw-race-a-backfill"
	postID := world.imagePost(world.communityC, slug, "race-ab-image-a")
	subject := atURIOf(t, h, postID)
	imageA := storedCIDOfSeed(t, h, "race-ab-image-a")
	world.removeFromC(slug, postID)
	assertOneRemovalWithhold(t, h, imageA, subject, ubUserDID)
	edit := world.editedPageObject(slug, "2026-08-21T12:00:00Z", "race-ab-image-b")

	var report *materialize.RemovalWithholdBackfillReport
	commitInOrder(t, h,
		func(ctx context.Context) error {
			_, err := h.mat.HandleUpdate(ctx, edit, world.communityC.id)
			return err
		},
		func(ctx context.Context) error {
			var err error
			report, err = h.mat.BackfillRemovalWithholds(ctx, []string{ubCommunityCDID}, false)
			return err
		})

	imageB := storedCIDOfSeed(t, h, "race-ab-image-b")
	require.Equal(t, []string{imageB}, recordBlobCIDs(t, h, postID), "precondition: the edit committed image B")
	assertBlobRefused(t, h, ubUserDID, imageB, "the edit's image of a removed post, after a racing back-fill")
	assertOneRemovalWithhold(t, h, imageB, subject, ubUserDID)
	assertOneRemovalWithhold(t, h, imageA, subject, ubUserDID)
	assertBlobRefused(t, h, ubUserDID, imageA, "the image a still-removed post dropped")
	assert.Empty(t, report.Added, "the edit already withheld everything the post references")
	assert.Equal(t, 1, report.AlreadyWithheld)
}

// TestABackfillRacingARestoreOfARemovedPostWithholdsNothing (race B,
// back-fill).
//
// GIVEN U's postv2 with image A, removed by C (A withheld); WHEN C's restore
// commits first and a back-fill run, which listed C's removal record before
// the restore landed, commits second THEN the back-fill reports and writes no
// addition and the post keeps no community-removal row, in a dry run and a
// real one.
//
// The back-fill runs on a second repo.Manager over the same database, as a
// second process would: one Manager's per-DID mutex would hold the back-fill
// off C until the restore returned, so it would list C after the restore and
// never take the stale-listing path this test pins.
func TestABackfillRacingARestoreOfARemovedPostWithholdsNothing(t *testing.T) {
	for _, dryRun := range []bool{true, false} {
		name := "real run"
		if dryRun {
			name = "dry run"
		}
		t.Run(name, func(t *testing.T) {
			world := newUpstreamBanWorld(t)
			h := world.h
			const slug = "rw-race-b-backfill"
			postID := world.imagePost(world.communityC, slug, "race-bb-image-a")
			subject := atURIOf(t, h, postID)
			imageA := storedCIDOfSeed(t, h, "race-bb-image-a")
			world.removeFromC(slug, postID)
			assertOneRemovalWithhold(t, h, imageA, subject, ubUserDID)
			mapping, err := h.objects.GetByAPID(context.Background(), postID)
			require.NoError(t, err)

			secondManager, err := repo.NewManager(h.db, identity.NewActorKeys(h.actors, h.custodian), nil)
			require.NoError(t, err)
			secondOptions := h.materializerOptions
			secondOptions.Repos = secondManager
			secondMaterializer, err := materialize.New(secondOptions)
			require.NoError(t, err)

			var report *materialize.RemovalWithholdBackfillReport
			commitInOrder(t, h,
				func(ctx context.Context) error { return h.mat.RestorePost(ctx, mapping) },
				func(ctx context.Context) error {
					var err error
					report, err = secondMaterializer.BackfillRemovalWithholds(ctx, []string{ubCommunityCDID}, dryRun)
					return err
				})

			require.Empty(t, removalCode(t, h, ubCommunityCDID, postID), "precondition: the restore deleted the removal")
			assert.Empty(t, report.Added, "the back-fill must not withhold a subject restored after its walk listed the removal")
			assert.Zero(t, report.Failed)
			assert.Empty(t, activeRemovalRowsOf(t, h, subject), "a restored post keeps no community-removal row")
			assertBlobServed(t, h, ubUserDID, imageA, "the image of a post restored before the back-fill committed")
		})
	}
}

// TestAnEditCommittedAfterARestoreItRacedLeavesThePostUnwithheld (race B).
//
// GIVEN U's postv2 with image A, removed by C (A withheld); WHEN C's restore
// commits first and an edit swapping A for B, which read the post as removed
// before the restore landed, commits second THEN neither image has an active
// community-removal row and both are served.
func TestAnEditCommittedAfterARestoreItRacedLeavesThePostUnwithheld(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	const slug = "rw-race-b"
	postID := world.imagePost(world.communityC, slug, "race-b-image-a")
	subject := atURIOf(t, h, postID)
	imageA := storedCIDOfSeed(t, h, "race-b-image-a")
	world.removeFromC(slug, postID)
	assertOneRemovalWithhold(t, h, imageA, subject, ubUserDID)
	mapping, err := h.objects.GetByAPID(context.Background(), postID)
	require.NoError(t, err)
	edit := world.editedPageObject(slug, "2026-08-21T12:00:00Z", "race-b-image-b")

	commitInOrder(t, h,
		func(ctx context.Context) error { return h.mat.RestorePost(ctx, mapping) },
		func(ctx context.Context) error {
			_, err := h.mat.HandleUpdate(ctx, edit, world.communityC.id)
			return err
		})

	imageB := storedCIDOfSeed(t, h, "race-b-image-b")
	require.Equal(t, []string{imageB}, recordBlobCIDs(t, h, postID), "precondition: the edit committed image B")
	require.Empty(t, removalCode(t, h, ubCommunityCDID, postID), "precondition: the restore deleted the removal")
	assert.Empty(t, activeRemovalRowsOf(t, h, subject), "a restored post keeps no community-removal row")
	assertBlobServed(t, h, ubUserDID, imageB, "the image of a restored post")
	assertBlobServed(t, h, ubUserDID, imageA, "the image of a restored post's earlier version")
}

// TestTheBackfillWithholdsUnderTheRemovalRecordsCode.
//
// GIVEN a removal of U's image post written before withholds existed, with the
// code spam; WHEN the back-fill runs THEN the post's row carries spam.
func TestTheBackfillWithholdsUnderTheRemovalRecordsCode(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	ctx := context.Background()
	postID := world.imagePost(world.communityC, "rw-code-backfill", "code-backfill-image")
	postURI := atURIOf(t, h, postID)
	imageCID := storedCIDOfSeed(t, h, "code-backfill-image")
	_, postRecordCID, err := h.manager.GetRecord(ctx, ubUserDID, materialize.CollectionPostV2, mustMappingRKey(t, h, postID))
	require.NoError(t, err)
	rkey := testDigestRKey(postURI)
	_, err = h.manager.ApplyOps(ctx, ubCommunityCDID, []repo.RecordOp{
		{Action: repo.OpActionDelete, Collection: materialize.CollectionAcceptance, RKey: rkey},
		{Action: repo.OpActionUpdate, Collection: materialize.CollectionRemoval, RKey: rkey, Record: map[string]any{
			"$type":     materialize.CollectionRemoval,
			"subject":   map[string]any{"uri": postURI, "cid": postRecordCID},
			"code":      "spam",
			"createdAt": "2026-08-25T12:00:00.000Z",
		}},
	})
	require.NoError(t, err)
	require.Empty(t, activeRemovalWithholds(t, h, imageCID), "precondition: the prior removal wrote no withhold")

	report := runBackfill(t, h, map[string]any{"dry_run": false})
	require.Len(t, report.Added, 1, "the back-fill adds the post's row")

	rows := activeRemovalWithholds(t, h, imageCID)
	if assert.Len(t, rows, 1) {
		assert.Equal(t, "spam", rows[0].ReasonCode, "the row carries the removal record's code")
	}
}

// TestAModeratorRemovalAndACommunityBanWithholdUnderTheirOwnCodes.
//
// GIVEN U's image post X in C and U's community-ban fixture in C; WHEN C's
// moderator removes X THEN X's row carries moderator-discretion; WHEN U edits X
// to a new image THEN the new row keeps moderator-discretion; WHEN C then bans
// U with removeData THEN every row of the ban fixture carries author-banned.
func TestAModeratorRemovalAndACommunityBanWithholdUnderTheirOwnCodes(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	const slug = "rw-code-moderator"
	postID := world.imagePost(world.communityC, slug, "code-moderator-image")
	fixture := world.seedCommunityBanFixture("rw-code-ban")

	world.removeFromC(slug, postID)
	rows := activeRemovalWithholds(t, h, storedCIDOfSeed(t, h, "code-moderator-image"))
	if assert.Len(t, rows, 1) {
		assert.Equal(t, materialize.RemovalCodeModeratorDiscretion, rows[0].ReasonCode, "a moderator removal's row")
	}

	edit := world.editedPageObject(slug, "2026-08-22T12:00:00Z", "code-moderator-image-edited")
	_, err := h.mat.HandleUpdate(context.Background(), edit, world.communityC.id)
	require.NoError(t, err)
	rows = activeRemovalWithholds(t, h, storedCIDOfSeed(t, h, "code-moderator-image-edited"))
	if assert.Len(t, rows, 1) {
		assert.Equal(t, materialize.RemovalCodeModeratorDiscretion, rows[0].ReasonCode,
			"an edit of a removed post keeps the removal's code")
	}

	const banID = "https://lemmy.world/activities/announce/block/rw-code-ban"
	world.announceCommunityBlock(world.communityC, banID, ubUserID, nil, ubBanTime)
	assertProcessed(t, h, banID)
	banned := append(append([]string{}, fixture.postCIDs...),
		fixture.commentCID[fixture.comments[0]], fixture.commentCID[fixture.comments[1]])
	for _, c := range banned {
		rows := activeRemovalWithholds(t, h, c)
		if assert.Len(t, rows, 1, "%s: one row for the banned content", c) {
			assert.Equal(t, materialize.RemovalCodeAuthorBanned, rows[0].ReasonCode, "%s: a community ban's row", c)
		}
	}
}

// TestARestoreRacingADeleteOfARemovedPostLeavesItsImageRefused (race C).
//
// GIVEN U's postv2 with image A, removed by C (A withheld); WHEN U's post
// record delete commits first and C's restore, which read the post's current
// version before the delete landed, commits second THEN the restore publishes
// no acceptance and lifts no row: C's removal record and A's community-removal
// row stand, A's bytes stay stored, and getBlob refuses A until an operator
// acts.
//
// The racing delete is the record delete alone, the commit an author delete
// ends with. HandleAuthorDeleteRecord first deletes the post's acceptance in C,
// a second commit that would queue ahead of the restore on the commit lock and
// let the restore commit before the record delete; the commit lock gives no
// way to order the restore between the two.
func TestARestoreRacingADeleteOfARemovedPostLeavesItsImageRefused(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	const slug = "rw-race-c-post"
	postID := world.imagePost(world.communityC, slug, "race-c-post-image")
	subject := atURIOf(t, h, postID)
	imageA := storedCIDOfSeed(t, h, "race-c-post-image")
	world.removeFromC(slug, postID)
	assertOneRemovalWithhold(t, h, imageA, subject, ubUserDID)
	mapping, err := h.objects.GetByAPID(context.Background(), postID)
	require.NoError(t, err)

	commitInOrder(t, h,
		func(ctx context.Context) error {
			_, err := h.manager.DeleteRecord(ctx, ubUserDID, materialize.CollectionPostV2, mapping.RKey)
			return err
		},
		func(ctx context.Context) error { return h.mat.RestorePost(ctx, mapping) })

	_, _, err = h.manager.GetRecord(context.Background(), ubUserDID, materialize.CollectionPostV2, mapping.RKey)
	require.True(t, errors.IsNotFound(err), "precondition: the delete committed: %v", err)
	assert.False(t, acceptanceExists(t, h, ubCommunityCDID, postID),
		"a restore must not accept a post deleted before its commit")
	assert.Equal(t, materialize.RemovalCodeModeratorDiscretion, removalCode(t, h, ubCommunityCDID, postID),
		"a restore whose subject is gone leaves the removal standing")
	assertBlobStored(t, h, ubUserDID, imageA, "the bytes stay until an operator acts")
	assertRemovalWithholdStands(t, h, imageA, subject, "a restore whose subject is gone lifts no row")
	assertBlobRefused(t, h, ubUserDID, imageA, "the removed, deleted post's image")
}

// TestARestoreRacingAnAuthorDeleteOfARemovedCommentLeavesItsImageRefused (race
// C, comment).
//
// GIVEN U's bridged comment with a seeded image, removed by C's moderator
// (image withheld); WHEN U's author delete commits first and C's restore, which
// resolved the comment's community before the delete landed, commits second
// THEN the restore deletes nothing: C's removal record and the image's
// community-removal row stand, the bytes stay stored, and getBlob refuses it.
func TestARestoreRacingAnAuthorDeleteOfARemovedCommentLeavesItsImageRefused(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	const slug = "rw-race-c-comment"
	rootID := world.postBy(ubOtherUserID, world.communityC, slug+"-root", ubBeforeBan, false)
	commentID, imageCID := world.imageComment(world.communityC, slug, rootID, ubBeforeBan2, slug+"-image")
	subject := atURIOf(t, h, commentID)
	h.announceDeleteAs(world.communityC, world.communityC.id+"/activities/announce/delete/"+slug,
		"https://lemmy.world/activities/delete/"+slug, commentID, modActorID, ptr(rwSummary))
	require.Equal(t, materialize.RemovalCodeModeratorDiscretion, removalCode(t, h, ubCommunityCDID, commentID),
		"precondition: C's removal record for the comment stands")
	assertOneRemovalWithhold(t, h, imageCID, subject, ubUserDID)
	mapping, err := h.objects.GetByAPID(context.Background(), commentID)
	require.NoError(t, err)

	var deleted bool
	commitInOrder(t, h,
		func(ctx context.Context) error { return h.mat.HandleAuthorDeleteRecord(ctx, commentID) },
		func(ctx context.Context) error {
			var err error
			deleted, err = h.mat.RestoreBridgedComment(ctx, mapping)
			return err
		})

	assertDeleted(t, h, commentID, "precondition: the author delete committed")
	assert.False(t, deleted, "a restore whose subject is gone deletes no removal record")
	assert.Equal(t, materialize.RemovalCodeModeratorDiscretion, removalCode(t, h, ubCommunityCDID, commentID),
		"a restore whose subject is gone leaves the removal standing")
	assertBlobStored(t, h, ubUserDID, imageCID, "the bytes stay until an operator acts")
	assertRemovalWithholdStands(t, h, imageCID, subject, "a restore whose subject is gone lifts no row")
	assertBlobRefused(t, h, ubUserDID, imageCID, "the removed, deleted comment's image")
}

// TestARestoreRacingAnEditOfARemovedPostFailsAndItsRetryPinsTheEdit (race C,
// edit).
//
// GIVEN U's postv2 with image A, removed by C (A withheld); WHEN an edit
// swapping A for B commits first and C's restore, which pinned the post's
// version before the edit landed, commits second THEN the restore fails and
// commits nothing (the removal stands, no acceptance, both images refused);
// WHEN the restore is retried THEN it accepts the edited version and B is
// served.
func TestARestoreRacingAnEditOfARemovedPostFailsAndItsRetryPinsTheEdit(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	const slug = "rw-race-c-edit"
	postID := world.imagePost(world.communityC, slug, "race-c-edit-image-a")
	imageA := storedCIDOfSeed(t, h, "race-c-edit-image-a")
	world.removeFromC(slug, postID)
	mapping, err := h.objects.GetByAPID(context.Background(), postID)
	require.NoError(t, err)
	edit := world.editedPageObject(slug, "2026-08-21T12:00:00Z", "race-c-edit-image-b")

	var restoreErr error
	commitInOrder(t, h,
		func(ctx context.Context) error {
			_, err := h.mat.HandleUpdate(ctx, edit, world.communityC.id)
			return err
		},
		func(ctx context.Context) error {
			restoreErr = h.mat.RestorePost(ctx, mapping)
			return nil
		})

	imageB := storedCIDOfSeed(t, h, "race-c-edit-image-b")
	require.Equal(t, []string{imageB}, recordBlobCIDs(t, h, postID), "precondition: the edit committed image B")
	require.Error(t, restoreErr, "a restore pinning a version edited before its commit fails")
	assert.False(t, acceptanceExists(t, h, ubCommunityCDID, postID), "the failed restore accepts nothing")
	assert.Equal(t, materialize.RemovalCodeModeratorDiscretion, removalCode(t, h, ubCommunityCDID, postID),
		"the failed restore leaves the removal standing")
	assertBlobRefused(t, h, ubUserDID, imageB, "the edited image while the removal stands")
	assertBlobRefused(t, h, ubUserDID, imageA, "the dropped image while the removal stands")

	require.NoError(t, h.mat.RestorePost(context.Background(), mapping), "the retry")
	_, editedCID, err := h.manager.GetRecord(context.Background(), ubUserDID, materialize.CollectionPostV2, mapping.RKey)
	require.NoError(t, err)
	acceptance, _, err := h.manager.GetRecord(context.Background(), ubCommunityCDID, materialize.CollectionAcceptance,
		testDigestRKey(mapping.ATURI))
	require.NoError(t, err, "the retry accepts the post")
	assert.Equal(t, editedCID, acceptance["subject"].(map[string]any)["cid"], "the retry pins the edited version")
	assert.Empty(t, removalCode(t, h, ubCommunityCDID, postID), "the retry deletes the removal")
	assertBlobServed(t, h, ubUserDID, imageB, "the restored post's image")
}
