package ingest

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/blobwithhold"
	"tidepool/internal/materialize"
	"tidepool/internal/repo"
	"tidepool/internal/store"
)

// BACK-FILLING COMMUNITY-REMOVAL WITHHOLDS.
//
// Removal records written before community removals withheld blobs have no
// withhold rows. POST /admin/moderation/backfill-removal-withholds walks every
// Tidepool community repo's removal records and gives each hosted subject one
// community-removal row per CID its current record references; a subject whose
// record is not in a Tidepool-hosted repo is skipped and counted. A dry run
// reports what it would add and writes nothing.
//
// The response contract:
//
//	{"dry_run": bool,
//	 "added": [{"subject": at-uri, "community": community DID, "cids": [cid, ...]}],
//	 "already_withheld": int,
//	 "skipped_unhosted": int,
//	 "failed": int}
const backfillRemovalWithholdsPath = "/admin/moderation/backfill-removal-withholds"

// rwNativeAuthorDID is a Coves author whose repo Tidepool does not host.
const rwNativeAuthorDID = "did:plc:rwnativeauthorbtenaaaaaa"

type backfillReport struct {
	DryRun bool `json:"dry_run"`
	Added  []struct {
		Subject   string   `json:"subject"`
		Community string   `json:"community"`
		CIDs      []string `json:"cids"`
	} `json:"added"`
	AlreadyWithheld int `json:"already_withheld"`
	SkippedUnhosted int `json:"skipped_unhosted"`
	Failed          int `json:"failed"`
}

type backfillAddition struct {
	subject   string
	community string
	cids      []string
}

// runBackfill posts body to the back-fill route; its first assertion is on the
// answer's status, then it decodes the report.
func runBackfill(t *testing.T, h *harness, body map[string]any) backfillReport {
	t.Helper()
	response := h.adminRequest(http.MethodPost, backfillRemovalWithholdsPath, body)
	require.Equal(t, http.StatusOK, response.Code, "the back-fill answers its report: %s", response.Body.String())
	var report backfillReport
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &report), response.Body.String())
	return report
}

func additionsOf(report backfillReport) []backfillAddition {
	var additions []backfillAddition
	for _, added := range report.Added {
		cids := append([]string{}, added.CIDs...)
		additions = append(additions, backfillAddition{
			subject: added.Subject, community: added.Community, cids: sortedStrings(cids...),
		})
	}
	return additions
}

// seedPriorRemoval writes communityDID's removal record of subjectURI straight
// to the community repo, as a removal written before withholds existed: no
// withhold rows. withdraw also deletes the subject's acceptance in the same
// commit, as a post removal does.
func seedPriorRemoval(t *testing.T, h *harness, communityDID, subjectURI, subjectCID string, withdraw bool) {
	t.Helper()
	rkey := testDigestRKey(subjectURI)
	var ops []repo.RecordOp
	if withdraw {
		ops = append(ops, repo.RecordOp{Action: repo.OpActionDelete, Collection: materialize.CollectionAcceptance, RKey: rkey})
	}
	ops = append(ops, repo.RecordOp{
		Action:     repo.OpActionUpdate,
		Collection: materialize.CollectionRemoval,
		RKey:       rkey,
		Record: map[string]any{
			"$type":     materialize.CollectionRemoval,
			"subject":   map[string]any{"uri": subjectURI, "cid": subjectCID},
			"code":      materialize.RemovalCodeModeratorDiscretion,
			"reason":    "removed before withholds existed",
			"createdAt": time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC).Format("2006-01-02T15:04:05.000Z"),
		},
	})
	_, err := h.manager.ApplyOps(context.Background(), communityDID, ops)
	require.NoError(t, err)
	_, _, err = h.manager.GetRecord(context.Background(), communityDID, materialize.CollectionRemoval, rkey)
	require.NoError(t, err, "precondition: the prior removal of %s stands in %s", subjectURI, communityDID)
}

// TestTheRemovalWithholdBackfillWithholdsEveryHostedRemovedSubjectOnce (B10).
//
// GIVEN removal records written with no withholds for U's image postv2 in C,
// U's comment with a seeded image in D, and a native post (its DID not hosted
// by Tidepool) in C, with D no longer followed (follow state none); WHEN the
// back-fill runs as a dry run THEN it lists both bridged subjects with their
// CIDs, counts the native one skipped, and writes no row; WHEN it runs for real, twice THEN each bridged subject has exactly
// one active community-removal row per CID and getBlob refuses them, and the
// second run adds nothing (already_withheld 2, skipped_unhosted 1). A request
// without dry_run is refused.
func TestTheRemovalWithholdBackfillWithholdsEveryHostedRemovedSubjectOnce(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	ctx := context.Background()

	postID := world.imagePost(world.communityC, "rw-b10-post", "rw-b10-post-image")
	postURI := atURIOf(t, h, postID)
	postCIDs := recordBlobCIDs(t, h, postID)
	_, postRecordCID, err := h.manager.GetRecord(ctx, ubUserDID, materialize.CollectionPostV2, mustMappingRKey(t, h, postID))
	require.NoError(t, err)
	seedPriorRemoval(t, h, ubCommunityCDID, postURI, postRecordCID, true)

	rootID := world.postBy(ubOtherUserID, world.communityD, "rw-b10-root", ubBeforeBan, false)
	commentID, commentCID := world.imageComment(world.communityD, "rw-b10-comment", rootID, ubBeforeBan2, "rw-b10-comment-image")
	commentURI := atURIOf(t, h, commentID)
	_, commentRecordCID, err := h.manager.GetRecord(ctx, ubUserDID, materialize.CollectionComment, mustMappingRKey(t, h, commentID))
	require.NoError(t, err)
	seedPriorRemoval(t, h, ubCommunityDDID, commentURI, commentRecordCID, false)

	nativeURI := "at://" + rwNativeAuthorDID + "/" + materialize.CollectionPostV2 + "/3lzrwnativepost1"
	seedPriorRemoval(t, h, ubCommunityCDID, nativeURI, "bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqrsqxi3jgxte", false)

	// The back-fill walks every hosted community repo, not only the followed
	// ones: a community unfollowed since its removal still has removals to
	// withhold.
	require.NoError(t, h.communities.SetFollowState(ctx, world.communityD.id, store.FollowStateNone))

	allCIDs := append(append([]string{}, postCIDs...), commentCID)
	for _, c := range allCIDs {
		require.Empty(t, activeRemovalWithholds(t, h, c), "precondition: the prior removals wrote no withhold for %s", c)
		assertBlobServed(t, h, ubUserDID, c, "precondition: served before the back-fill")
	}
	wantAdded := []backfillAddition{
		{subject: postURI, community: ubCommunityCDID, cids: postCIDs},
		{subject: commentURI, community: ubCommunityDDID, cids: []string{commentCID}},
	}

	dryRun := runBackfill(t, h, map[string]any{"dry_run": true})
	assert.True(t, dryRun.DryRun)
	assert.ElementsMatch(t, wantAdded, additionsOf(dryRun), "the dry run lists both bridged subjects and their CIDs")
	assert.Zero(t, dryRun.AlreadyWithheld)
	assert.Equal(t, 1, dryRun.SkippedUnhosted, "the native subject is skipped and counted")
	for _, c := range allCIDs {
		state, err := blobwithhold.NewStore(h.db).Inspect(ctx, c)
		require.NoError(t, err)
		assert.Empty(t, state.Withholds, "a dry run writes no withhold row for %s", c)
		assertBlobServed(t, h, ubUserDID, c, "a dry run withholds nothing")
	}

	first := runBackfill(t, h, map[string]any{"dry_run": false})
	assert.False(t, first.DryRun)
	assert.ElementsMatch(t, wantAdded, additionsOf(first), "the real run adds what the dry run listed")
	assert.Zero(t, first.AlreadyWithheld)
	assert.Equal(t, 1, first.SkippedUnhosted)

	second := runBackfill(t, h, map[string]any{"dry_run": false})
	assert.Empty(t, second.Added, "a second run has nothing to add")
	assert.Equal(t, 2, second.AlreadyWithheld, "both bridged subjects are already withheld")
	assert.Equal(t, 1, second.SkippedUnhosted)

	for _, c := range postCIDs {
		assertOneRemovalWithhold(t, h, c, postURI, ubUserDID)
		assertBlobStored(t, h, ubUserDID, c, "the back-fill keeps the bytes")
		assertBlobRefused(t, h, ubUserDID, c, "a back-filled removed post's image")
	}
	assertOneRemovalWithhold(t, h, commentCID, commentURI, ubUserDID)
	assertBlobStored(t, h, ubUserDID, commentCID, "the back-fill keeps the bytes")
	assertBlobRefused(t, h, ubUserDID, commentCID, "a back-filled removed comment's image")

	for _, body := range []map[string]any{nil, {}} {
		response := h.adminRequest(http.MethodPost, backfillRemovalWithholdsPath, body)
		assert.Equal(t, http.StatusBadRequest, response.Code, "dry_run is required (body %v): %s", body, response.Body.String())
	}
}

// TestTheRemovalWithholdBackfillWithholdsTheRemovalsOfATombstonedCommunity.
//
// GIVEN community C's removal of U's image postv2, written with no withholds,
// and C then deleted upstream (its actor tombstoned, so its repo takes no new
// writes), and community D's removal of U's image comment, listed after C
// (follow state none); WHEN the back-fill runs, dry and then for real THEN
// neither run stops at C: both subjects are listed and then withheld, one row
// per CID, and nothing is counted failed.
func TestTheRemovalWithholdBackfillWithholdsTheRemovalsOfATombstonedCommunity(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	ctx := context.Background()

	postID := world.imagePost(world.communityC, "rw-tomb-post", "rw-tomb-post-image")
	postURI := atURIOf(t, h, postID)
	postCIDs := recordBlobCIDs(t, h, postID)
	_, postRecordCID, err := h.manager.GetRecord(ctx, ubUserDID, materialize.CollectionPostV2, mustMappingRKey(t, h, postID))
	require.NoError(t, err)
	seedPriorRemoval(t, h, ubCommunityCDID, postURI, postRecordCID, true)

	rootID := world.postBy(ubOtherUserID, world.communityD, "rw-tomb-root", ubBeforeBan, false)
	commentID, commentCID := world.imageComment(world.communityD, "rw-tomb-comment", rootID, ubBeforeBan2, "rw-tomb-comment-image")
	commentURI := atURIOf(t, h, commentID)
	_, commentRecordCID, err := h.manager.GetRecord(ctx, ubUserDID, materialize.CollectionComment, mustMappingRKey(t, h, commentID))
	require.NoError(t, err)
	seedPriorRemoval(t, h, ubCommunityDDID, commentURI, commentRecordCID, false)
	require.NoError(t, h.communities.SetFollowState(ctx, world.communityD.id, store.FollowStateNone))

	require.NoError(t, h.mat.DeleteActor(ctx, world.communityC.id))
	actor, err := h.actors.GetByAPActorID(ctx, world.communityC.id)
	require.NoError(t, err)
	require.Equal(t, store.ConsentStateDeleted, actor.ConsentState, "precondition: C's actor is tombstoned")
	_, _, err = h.manager.GetRecord(ctx, ubCommunityCDID, materialize.CollectionRemoval, testDigestRKey(postURI))
	require.NoError(t, err, "precondition: C's removal record outlives the tombstone")
	_, _, err = h.manager.GetRecord(ctx, ubUserDID, materialize.CollectionPostV2, mustMappingRKey(t, h, postID))
	require.NoError(t, err, "precondition: U's removed post still lives in U's repo")

	wantAdded := []backfillAddition{
		{subject: postURI, community: ubCommunityCDID, cids: postCIDs},
		{subject: commentURI, community: ubCommunityDDID, cids: []string{commentCID}},
	}
	dryRun := runBackfill(t, h, map[string]any{"dry_run": true})
	assert.ElementsMatch(t, wantAdded, additionsOf(dryRun), "the dry run lists the tombstoned community's subject and goes on to D")
	assert.Zero(t, dryRun.Failed)

	committed := runBackfill(t, h, map[string]any{"dry_run": false})
	assert.ElementsMatch(t, wantAdded, additionsOf(committed), "the real run withholds the tombstoned community's subject and goes on to D")
	assert.Zero(t, committed.Failed)
	for _, c := range postCIDs {
		assertOneRemovalWithhold(t, h, c, postURI, ubUserDID)
	}
	assertOneRemovalWithhold(t, h, commentCID, commentURI, ubUserDID)
}

// TestTheRemovalWithholdBackfillCountsAFailedRemovalAndGoesOn.
//
// GIVEN community C (listed first) holding a removal record that names no
// subject, and community D (follow state none, listed after C) holding a
// prior removal of U's image comment; WHEN the back-fill runs for real THEN
// it answers 200, counts C's record as failed, and still withholds D's
// subject.
func TestTheRemovalWithholdBackfillCountsAFailedRemovalAndGoesOn(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	ctx := context.Background()

	_, err := h.manager.ApplyOps(ctx, ubCommunityCDID, []repo.RecordOp{{
		Action: repo.OpActionUpdate, Collection: materialize.CollectionRemoval, RKey: "3lzrwbrokenrm1",
		Record: map[string]any{
			"$type":     materialize.CollectionRemoval,
			"code":      materialize.RemovalCodeModeratorDiscretion,
			"createdAt": time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC).Format("2006-01-02T15:04:05.000Z"),
		},
	}})
	require.NoError(t, err)

	rootID := world.postBy(ubOtherUserID, world.communityD, "rw-fail-root", ubBeforeBan, false)
	commentID, commentCID := world.imageComment(world.communityD, "rw-fail-comment", rootID, ubBeforeBan2, "rw-fail-comment-image")
	commentURI := atURIOf(t, h, commentID)
	_, commentRecordCID, err := h.manager.GetRecord(ctx, ubUserDID, materialize.CollectionComment, mustMappingRKey(t, h, commentID))
	require.NoError(t, err)
	seedPriorRemoval(t, h, ubCommunityDDID, commentURI, commentRecordCID, false)
	require.NoError(t, h.communities.SetFollowState(ctx, world.communityD.id, store.FollowStateNone))

	report := runBackfill(t, h, map[string]any{"dry_run": false})
	assert.Equal(t, 1, report.Failed, "C's subjectless removal is counted failed")
	assert.ElementsMatch(t, []backfillAddition{
		{subject: commentURI, community: ubCommunityDDID, cids: []string{commentCID}},
	}, additionsOf(report), "the walk goes on to D after C's failure")
	assertOneRemovalWithhold(t, h, commentCID, commentURI, ubUserDID)
}

func mustMappingRKey(t *testing.T, h *harness, apID string) string {
	t.Helper()
	mapping, err := h.objects.GetByAPID(context.Background(), apID)
	require.NoError(t, err)
	return mapping.RKey
}
