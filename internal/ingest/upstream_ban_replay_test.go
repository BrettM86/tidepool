package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/echo"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// REPLAYING HISTORICAL UPSTREAM BANS.
//
// Before upstream bans removed bridged content, the bridge processed (and
// skipped) them. POST /admin/inbox/replay-bans re-reads those already-processed
// inbox events through the same ban logic: a dry run lists what each
// qualifying ban would remove and changes nothing; a real run removes it; a
// second real run finds nothing left.
//
// The response contract:
//
//	{"dry_run": bool,
//	 "bans": [{"activity_id": the inbox event's activity id,
//	           "subject": the banned actor's AP id,
//	           "scope": "site" | "community",
//	           "community": the community's AP id ("" for a site ban),
//	           "removed": [at-uris, sorted]}],
//	 "removed_total": int}
//
// "bans" lists only qualifying bans that select at least one record, in inbox
// event id order.
const (
	replayBansPath    = "/admin/inbox/replay-bans"
	replayBystanderID = "https://lemmy.world/u/Bystander"
	// replayLegacyRKey is the rkey of V's pre-flip post in C's repo.
	replayLegacyRKey = "3kjzl5kcb2s3a"
)

var replayOtherUserDID = testDIDFor("OtherPoster", "lemmy.ml")

// TestReplayingProcessedUpstreamBansRemovesWhatTheyWouldHaveRemoved (B5).
//
// GIVEN already-processed inbox events for (i) U's home site ban, (ii) C's
// announced ban of bridged user V, and (iii) a foreign instance's site ban of
// U, with U, V and bystander W holding content before and after those bans —
// including V's legacy post in C's repo and a V comment whose community is
// recorded nowhere but its thread; WHEN the operator replays bans as a dry run
// THEN the listing names exactly (i) and (ii) and their pre-ban content and
// nothing changes; WHEN replayed for real THEN exactly that content is removed;
// WHEN replayed again THEN nothing is left to remove.
func TestReplayingProcessedUpstreamBansRemovesWhatTheyWouldHaveRemoved(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h
	ctx := context.Background()
	h.newRemoteActor(replayBystanderID, person(replayBystanderID, "Bystander", nil))

	var (
		siteBanReceived      = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
		communityBanReceived = time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC)
		// Later than U's post-ban post: were the foreign ban to qualify, it
		// would select that post too.
		foreignBanReceived = time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	)

	// W: the untouched control in C.
	bystanderPostC := world.postBy(replayBystanderID, world.communityC, "ub-rp-w-c1", "2026-08-09T12:00:00Z", false)

	// U: pre-ban content in C and D, and a post after the site ban.
	userPostC := world.postBy(ubUserID, world.communityC, "ub-rp-u-c1", "2026-08-10T12:00:00Z", false)
	userCommentC := world.commentBy(ubUserID, world.communityC, "ub-rp-u-c1-reply", bystanderPostC, "2026-08-11T12:00:00Z")
	userPostD := world.postBy(ubUserID, world.communityD, "ub-rp-u-d1", "2026-08-12T12:00:00Z", false)
	userPostAfter := world.postBy(ubUserID, world.communityC, "ub-rp-u-c2", "2026-09-15T00:00:00Z", false)

	// V: a pre-ban postv2 in C, a pre-ban comment in C whose mapping carries
	// no community, a pre-ban legacy post in C's repo; a pre-ban post in D
	// and a post in C after the community ban.
	otherPostC := world.postBy(ubOtherUserID, world.communityC, "ub-rp-v-c1", "2026-08-13T12:00:00Z", false)
	otherCommentC := world.commentBy(ubOtherUserID, world.communityC, "ub-rp-v-c1-reply", otherPostC, "2026-08-14T12:00:00Z")
	clearCommunityDID(t, h, otherCommentC)
	const otherLegacyC = "https://lemmy.ml/post/ub-rp-v-legacy"
	otherLegacyURI := putLegacyPost(t, h, ubCommunityCDID, replayOtherUserDID, otherLegacyC, replayLegacyRKey, time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC))
	otherPostD := world.postBy(ubOtherUserID, world.communityD, "ub-rp-v-d1", "2026-08-16T12:00:00Z", false)
	otherPostAfter := world.postBy(ubOtherUserID, world.communityC, "ub-rp-v-c2", "2026-09-15T00:00:00Z", false)

	// The three bans, already processed (skipped) long ago.
	const (
		siteBanID      = ubHomeHost + "/activities/block/ub-rp-site"
		communityBanID = "https://lemmy.world/activities/announce/block/ub-rp-community"
		foreignBanID   = ubOtherHost + "/activities/block/ub-rp-foreign"
	)
	enqueueProcessed(t, h, world.admin, siteBlock(siteBanID, ubAdminID, ubUserID, ubHomeRoot, nil), siteBanReceived)
	enqueueProcessed(t, h, world.communityC,
		communityBlockAnnounce(world.communityC, communityBanID, ubOtherUserID, nil), communityBanReceived)
	enqueueProcessed(t, h, world.otherAdmin,
		siteBlock(foreignBanID, ubOtherAdminID, ubUserID, ubHomeRoot, nil), foreignBanReceived)

	siteRemoved := sortedStrings(
		atURIOf(t, h, userPostC), atURIOf(t, h, userCommentC), atURIOf(t, h, userPostD))
	communityRemoved := sortedStrings(
		atURIOf(t, h, otherPostC), atURIOf(t, h, otherCommentC), otherLegacyURI)
	listing := func(dryRun bool) string {
		return mustJSON(t, map[string]any{
			"dry_run": dryRun,
			"bans": []any{
				map[string]any{
					"activity_id": siteBanID,
					"subject":     ubUserID,
					"scope":       "site",
					"community":   "",
					"removed":     siteRemoved,
				},
				map[string]any{
					"activity_id": communityBanID,
					"subject":     ubOtherUserID,
					"scope":       "community",
					"community":   world.communityC.id,
					"removed":     communityRemoved,
				},
			},
			"removed_total": 6,
		})
	}

	// Unauthenticated: refused.
	unauthenticated := httptest.NewRequest(http.MethodPost, "https://"+bridgeHost+replayBansPath,
		strings.NewReader(`{"dry_run":true}`))
	unauthenticatedRecorder := httptest.NewRecorder()
	h.router.ServeHTTP(unauthenticatedRecorder, unauthenticated)
	assert.Equal(t, http.StatusUnauthorized, unauthenticatedRecorder.Code)

	// Dry run: the listing, and nothing mutated.
	before := replayStateSnapshot(t, h)
	dryRun := h.adminRequest(http.MethodPost, replayBansPath, map[string]any{"dry_run": true})
	require.Equal(t, http.StatusOK, dryRun.Code, dryRun.Body.String())
	assert.JSONEq(t, listing(true), dryRun.Body.String())
	after := replayStateSnapshot(t, h)
	for table, rows := range before {
		assert.Equal(t, rows, after[table], "a dry run mutates nothing: %s changed", table)
	}

	// Real run: the same listing, and exactly that content removed.
	realRun := h.adminRequest(http.MethodPost, replayBansPath, map[string]any{"dry_run": false})
	require.Equal(t, http.StatusOK, realRun.Code, realRun.Body.String())
	assert.JSONEq(t, listing(false), realRun.Body.String())

	assertDeleted(t, h, userPostC, "U's pre-ban post in C: site ban")
	assertDeleted(t, h, userCommentC, "U's pre-ban comment in C: site ban")
	assertDeleted(t, h, userPostD, "U's pre-ban post in D: a site ban spans every community")
	assertLive(t, h, userPostAfter, "U's post after the site ban")

	assert.False(t, acceptanceExists(t, h, ubCommunityCDID, otherPostC), "V's pre-ban post is withdrawn from C")
	assert.Equal(t, materialize.RemovalCodeAuthorBanned, removalCode(t, h, ubCommunityCDID, otherPostC),
		"C records why: its author was banned")
	otherPostMapping, err := h.objects.GetByAPID(ctx, otherPostC)
	require.NoError(t, err)
	_, _, err = h.manager.GetRecord(ctx, replayOtherUserDID, materialize.CollectionPostV2, otherPostMapping.RKey)
	assert.NoError(t, err, "V's postv2 record itself stays: a community ban is community-scoped")
	assertDeleted(t, h, otherCommentC, "V's pre-ban comment in C, community resolved through its thread")
	assertDeleted(t, h, otherLegacyC, "V's pre-ban legacy post in C's repo")
	assertLive(t, h, otherPostD, "a community ban does not reach D")
	assert.True(t, acceptanceExists(t, h, ubCommunityDDID, otherPostD), "V's post stays accepted in D")
	assertLive(t, h, otherPostAfter, "V's post after the community ban")
	assert.True(t, acceptanceExists(t, h, ubCommunityCDID, otherPostAfter), "V's post-ban post stays accepted in C")

	assertLive(t, h, bystanderPostC, "W was banned by no one")
	assert.True(t, acceptanceExists(t, h, ubCommunityCDID, bystanderPostC), "W's post stays accepted in C")
	assert.Empty(t, removalCode(t, h, ubCommunityCDID, bystanderPostC), "W's post has no removal")

	// Again: nothing left to remove.
	again := h.adminRequest(http.MethodPost, replayBansPath, map[string]any{"dry_run": false})
	require.Equal(t, http.StatusOK, again.Code, again.Body.String())
	assert.JSONEq(t, `{"dry_run": false, "bans": [], "removed_total": 0}`, again.Body.String())
}

// enqueueProcessed delivers activity through the inbox (so the row holds the
// verified payload, actor and ordering key production stores) WITHOUT
// draining, then stamps it received at receivedAt and processed a minute
// later: the shape of a ban the bridge skipped before bans removed content.
func enqueueProcessed(t *testing.T, h *harness, signer *remoteActor, activity map[string]any, receivedAt time.Time) {
	t.Helper()
	enqueueRecorded(t, h, signer, activity, receivedAt,
		`UPDATE inbox_events SET received_at = $1, processed_at = $2 WHERE activity_id = $3`)
}

// enqueuePoisoned is enqueueProcessed for a ban whose processing gave up: the
// row is stamped failed a minute after receipt and was never processed.
func enqueuePoisoned(t *testing.T, h *harness, signer *remoteActor, activity map[string]any, receivedAt time.Time) {
	t.Helper()
	enqueueRecorded(t, h, signer, activity, receivedAt,
		`UPDATE inbox_events SET received_at = $1, failed_at = $2, processed_at = NULL,
			error = 'gave up after repeated failures' WHERE activity_id = $3`)
}

// enqueueRecorded delivers activity WITHOUT draining and stamps its row with
// stamp, which takes ($1 receivedAt, $2 a minute later, $3 the activity id).
func enqueueRecorded(t *testing.T, h *harness, signer *remoteActor, activity map[string]any, receivedAt time.Time, stamp string) {
	t.Helper()
	require.Equal(t, http.StatusAccepted, h.deliver(signer, activity))
	activityID := activity["id"].(string)
	result, err := h.db.ExecContext(context.Background(), stamp,
		receivedAt, receivedAt.Add(time.Minute), activityID)
	require.NoError(t, err)
	rows, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows, "precondition: %s is queued", activityID)
}

// clearCommunityDID leaves apID's mapping with no recorded community, the
// shape of rows written before migration 016, and checks the community is
// still derivable the way every other reader derives it.
func clearCommunityDID(t *testing.T, h *harness, apID string) {
	t.Helper()
	ctx := context.Background()
	result, err := h.db.ExecContext(ctx, `UPDATE ap_objects SET community_did = NULL WHERE ap_id = $1`, apID)
	require.NoError(t, err)
	rows, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows)
	mapping, err := h.objects.GetByAPID(ctx, apID)
	require.NoError(t, err)
	require.Empty(t, mapping.CommunityDID, "precondition: no community recorded on the mapping")
	communityDID, err := materialize.CommunityDIDOf(ctx, h.manager, mapping)
	require.NoError(t, err)
	require.Equal(t, ubCommunityCDID, communityDID, "precondition: the community is derivable from the thread")
}

// putLegacyPost writes a pre-flip post: deprecated collection, in the
// community's repo, authored by authorDID. It returns the post's at-uri.
func putLegacyPost(t *testing.T, h *harness, communityDID, authorDID, apID, rkey string, published time.Time) string {
	t.Helper()
	ctx := context.Background()
	commit, err := h.manager.PutRecord(ctx, communityDID, materialize.CollectionPost, rkey, map[string]any{
		"$type":     materialize.CollectionPost,
		"community": communityDID,
		"author":    authorDID,
		"createdAt": published.Format("2006-01-02T15:04:05.000Z"),
		"title":     "a pre-flip post",
	})
	require.NoError(t, err)
	_, err = h.objects.PutMapping(ctx, store.APObjectMapping{
		APID:           apID,
		APType:         "Page",
		OriginInstance: "lemmy.ml",
		Origin:         store.OriginFediverse,
		DID:            communityDID,
		AuthorDID:      authorDID,
		CommunityDID:   communityDID,
		Collection:     materialize.CollectionPost,
		RKey:           rkey,
		CID:            commit.RecordCID,
		PublishedAt:    &published,
	})
	require.NoError(t, err)
	return "at://" + communityDID + "/" + materialize.CollectionPost + "/" + rkey
}

func atURIOf(t *testing.T, h *harness, apID string) string {
	t.Helper()
	mapping, err := h.objects.GetByAPID(context.Background(), apID)
	require.NoError(t, err)
	require.NotEmpty(t, mapping.ATURI)
	return mapping.ATURI
}

func sortedStrings(values ...string) []string {
	sort.Strings(values)
	return values
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	require.NoError(t, err)
	return string(raw)
}

// replayStateSnapshot captures everything a ban replay could change: object
// mappings (deleted_at included), every repo's head and records (acceptance
// and removal records live in the community repos), blobs, bans, tombstones,
// moderation state, the firehose, and the inbox events' timestamps.
func replayStateSnapshot(t *testing.T, h *harness) map[string][]string {
	t.Helper()
	ctx := context.Background()
	queries := map[string]string{
		"ap_objects":        `SELECT * FROM ap_objects`,
		"repo_state":        `SELECT did, head_cid, rev FROM repo_state`,
		"blobs":             `SELECT did, cid, size FROM blobs`,
		"community_bans":    `SELECT * FROM community_bans`,
		"ap_tombstones":     `SELECT * FROM ap_tombstones`,
		"object_moderation": `SELECT * FROM object_moderation`,
		"firehose_events":   `SELECT count(*) AS events FROM firehose_events`,
		"inbox_events": `SELECT activity_id, received_at, processed_at, failed_at, attempts, error
			FROM inbox_events`,
	}
	snapshot := map[string][]string{}
	for table, query := range queries {
		rows, err := h.db.QueryContext(ctx, `SELECT row_to_json(x)::text FROM (`+query+`) x ORDER BY 1`)
		require.NoError(t, err, table)
		for rows.Next() {
			var row string
			require.NoError(t, rows.Scan(&row), table)
			snapshot[table] = append(snapshot[table], row)
		}
		require.NoError(t, rows.Err(), table)
		require.NoError(t, rows.Close(), table)
	}
	for _, did := range snapshot["repo_state"] {
		var state struct {
			DID string `json:"did"`
		}
		require.NoError(t, json.Unmarshal([]byte(did), &state))
		entries, err := h.manager.ListRecords(ctx, state.DID)
		require.NoError(t, err)
		for _, entry := range entries {
			snapshot["records"] = append(snapshot["records"],
				fmt.Sprintf("%s/%s/%s %s", state.DID, entry.Collection, entry.Rkey, entry.CID))
		}
	}
	sort.Strings(snapshot["records"])
	require.NotEmpty(t, snapshot["records"], "precondition: the snapshot sees the repos")
	return snapshot
}

// TestReplayingBansDeletesACommentWhoseThreadRootAnEarlierBanDeleted (B5,
// review).
//
// GIVEN already-processed inbox events for (i) U's home site ban and then (ii)
// C's ban of bridged user V, with U's post P in C and V's comment on P — its
// mapping carrying no community — both published before either ban; WHEN the
// operator replays bans as a dry run THEN (i) lists P and (ii) lists V's
// comment; WHEN replayed for real THEN (i) deleting P does not hide V's comment
// from (ii): the listing equals the dry run's and both are deleted.
func TestReplayingBansDeletesACommentWhoseThreadRootAnEarlierBanDeleted(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h

	rootPost := world.postBy(ubUserID, world.communityC, "ub-rprd-u-c1", "2026-08-10T12:00:00Z", false)
	otherComment := world.commentBy(ubOtherUserID, world.communityC, "ub-rprd-v-c1-reply", rootPost, "2026-08-11T12:00:00Z")
	clearCommunityDID(t, h, otherComment)

	const (
		siteBanID      = ubHomeHost + "/activities/block/ub-rprd-site"
		communityBanID = "https://lemmy.world/activities/announce/block/ub-rprd-community"
	)
	enqueueProcessed(t, h, world.admin, siteBlock(siteBanID, ubAdminID, ubUserID, ubHomeRoot, nil),
		time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	enqueueProcessed(t, h, world.communityC,
		communityBlockAnnounce(world.communityC, communityBanID, ubOtherUserID, nil),
		time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC))

	listing := func(dryRun bool) string {
		return mustJSON(t, map[string]any{
			"dry_run": dryRun,
			"bans": []any{
				map[string]any{
					"activity_id": siteBanID,
					"subject":     ubUserID,
					"scope":       "site",
					"community":   "",
					"removed":     []string{atURIOf(t, h, rootPost)},
				},
				map[string]any{
					"activity_id": communityBanID,
					"subject":     ubOtherUserID,
					"scope":       "community",
					"community":   world.communityC.id,
					"removed":     []string{atURIOf(t, h, otherComment)},
				},
			},
			"removed_total": 2,
		})
	}

	dryRun := h.adminRequest(http.MethodPost, replayBansPath, map[string]any{"dry_run": true})
	require.Equal(t, http.StatusOK, dryRun.Code, dryRun.Body.String())
	assert.JSONEq(t, listing(true), dryRun.Body.String())
	assertLive(t, h, rootPost, "a dry run deletes nothing")
	assertLive(t, h, otherComment, "a dry run deletes nothing")

	realRun := h.adminRequest(http.MethodPost, replayBansPath, map[string]any{"dry_run": false})
	require.Equal(t, http.StatusOK, realRun.Code, realRun.Body.String())
	assert.JSONEq(t, listing(false), realRun.Body.String(),
		"the real run removes exactly what the dry run listed")
	assertDeleted(t, h, rootPost, "U's pre-ban post: site ban")
	assertDeleted(t, h, otherComment,
		"V's pre-ban comment in C, its community resolved through a thread root the site ban already deleted")
}

// TestReplayingBansSkipsAnEchoOfOurOwnPersonasBan (B5, review).
//
// GIVEN followed community C announcing a Block whose inner actor is one of
// OUR personas (a Coves moderator's own ban coming back to us) naming bridged
// user V with removeData, and V's content in C published before it: the live
// dispatch drops it as an echo and removes nothing (the control); WHEN the same
// event, already processed, is replayed THEN the replay drops it too — the dry
// run lists nothing and the real run removes nothing.
func TestReplayingBansSkipsAnEchoOfOurOwnPersonasBan(t *testing.T) {
	const echoBanID = "https://lemmy.world/activities/announce/block/ub-echo-ban"
	echoBan := func(world *upstreamBanWorld) map[string]any {
		return communityBlockAnnounce(world.communityC, echoBanID, ubOtherUserID,
			map[string]any{"actor": evPersonaID})
	}
	// setup builds V's pre-ban post and comment in C and the persona.
	setup := func(t *testing.T) (*upstreamBanWorld, string, string) {
		world := newUpstreamBanWorld(t)
		mintPersona(t, world.h)
		otherPost := world.postBy(ubOtherUserID, world.communityC, "ub-echo-v-c1", ubBeforeBan, false)
		otherComment := world.commentBy(ubOtherUserID, world.communityC, "ub-echo-v-c1-reply", otherPost, ubBeforeBan2)
		return world, otherPost, otherComment
	}
	assertUntouched := func(t *testing.T, h *harness, otherPost, otherComment, why string) {
		t.Helper()
		assertLive(t, h, otherPost, why)
		assert.True(t, acceptanceExists(t, h, ubCommunityCDID, otherPost), "V's post stays accepted in C: %s", why)
		assert.Empty(t, removalCode(t, h, ubCommunityCDID, otherPost), "no removal record: %s", why)
		assertLive(t, h, otherComment, why)
	}

	t.Run("live dispatch drops it as an echo", func(t *testing.T) {
		world, otherPost, otherComment := setup(t)
		before := echo.Drops(echo.ClassLocalActor)
		world.deliverAt(world.communityC, echoBan(world), &ubBanTime)
		assertProcessed(t, world.h, echoBanID)
		assert.Equal(t, before+1, echo.Drops(echo.ClassLocalActor),
			"dropped as an echo of our own persona's Block")
		assertUntouched(t, world.h, otherPost, otherComment, "an echo removes nothing live")
	})

	t.Run("replay drops it as an echo", func(t *testing.T) {
		world, otherPost, otherComment := setup(t)
		h := world.h
		enqueueProcessed(t, h, world.communityC, echoBan(world), ubBanTime)

		dryRun := h.adminRequest(http.MethodPost, replayBansPath, map[string]any{"dry_run": true})
		require.Equal(t, http.StatusOK, dryRun.Code, dryRun.Body.String())
		assert.JSONEq(t, `{"dry_run": true, "bans": [], "removed_total": 0}`, dryRun.Body.String(),
			"the replay lists no echo the live dispatch would have dropped")

		realRun := h.adminRequest(http.MethodPost, replayBansPath, map[string]any{"dry_run": false})
		require.Equal(t, http.StatusOK, realRun.Code, realRun.Body.String())
		assert.JSONEq(t, `{"dry_run": false, "bans": [], "removed_total": 0}`, realRun.Body.String())
		assertUntouched(t, h, otherPost, otherComment, "a replayed echo removes nothing")
	})
}

// TestReplayingBansRefusesABodyWithoutDryRun: dry_run is required, so an empty
// body or {} is refused and a qualifying processed ban removes nothing.
func TestReplayingBansRefusesABodyWithoutDryRun(t *testing.T) {
	for _, tc := range []struct {
		name string
		body map[string]any
	}{
		{name: "empty body", body: nil},
		{name: "empty object", body: map[string]any{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			world := newUpstreamBanWorld(t)
			h := world.h
			userPost := world.postBy(ubUserID, world.communityC, "ub-rpbody-u-c1", ubBeforeBan, false)
			enqueueProcessed(t, h, world.admin,
				siteBlock(ubHomeHost+"/activities/block/ub-rpbody-site", ubAdminID, ubUserID, ubHomeRoot, nil), ubBanTime)

			response := h.adminRequest(http.MethodPost, replayBansPath, tc.body)
			assert.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
			assertLive(t, h, userPost, "a refused request removes nothing")
		})
	}
}

// TestReplayingBansIncludesABanWhoseEventWasPoisoned (review).
//
// GIVEN U's home site ban recorded in an inbox event that was poisoned (it
// failed, and was never marked processed) and U's content published before
// it; WHEN the operator replays bans THEN the dry run lists that ban and its
// content, and the real run removes it: a ban that failed live is exactly one
// the replay exists to apply.
func TestReplayingBansIncludesABanWhoseEventWasPoisoned(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h

	userPost := world.postBy(ubUserID, world.communityC, "ub-rppoison-u-c1", ubBeforeBan, false)
	userComment := world.commentBy(ubUserID, world.communityC, "ub-rppoison-u-c1-reply", userPost, ubBeforeBan2)

	const siteBanID = ubHomeHost + "/activities/block/ub-rppoison-site"
	enqueuePoisoned(t, h, world.admin, siteBlock(siteBanID, ubAdminID, ubUserID, ubHomeRoot, nil), ubBanTime)

	listing := func(dryRun bool) string {
		return mustJSON(t, map[string]any{
			"dry_run": dryRun,
			"bans": []any{
				map[string]any{
					"activity_id": siteBanID,
					"subject":     ubUserID,
					"scope":       "site",
					"community":   "",
					"removed":     sortedStrings(atURIOf(t, h, userPost), atURIOf(t, h, userComment)),
				},
			},
			"removed_total": 2,
		})
	}

	dryRun := h.adminRequest(http.MethodPost, replayBansPath, map[string]any{"dry_run": true})
	require.Equal(t, http.StatusOK, dryRun.Code, dryRun.Body.String())
	assert.JSONEq(t, listing(true), dryRun.Body.String(), "the dry run lists the poisoned ban")

	realRun := h.adminRequest(http.MethodPost, replayBansPath, map[string]any{"dry_run": false})
	require.Equal(t, http.StatusOK, realRun.Code, realRun.Body.String())
	assert.JSONEq(t, listing(false), realRun.Body.String())
	assertDeleted(t, h, userPost, "U's pre-ban post: the poisoned site ban, replayed")
	assertDeleted(t, h, userComment, "U's pre-ban comment: the poisoned site ban, replayed")
}

// TestReplayingBansContinuesPastABanThatFails (review).
//
// GIVEN two recorded qualifying bans — first C's ban of U, whose content in C
// includes a comment whose community is recorded nowhere and whose record no
// longer decodes, so selecting it fails; then V's home site ban — WHEN the
// operator replays bans, dry run or real, THEN the response is 200, the first
// ban's entry carries an error, and the second ban is still listed and (in a
// real run) applied: one broken ban must not strand every ban after it.
// removed_total counts only what was removed.
func TestReplayingBansContinuesPastABanThatFails(t *testing.T) {
	for _, dryRun := range []bool{true, false} {
		t.Run(fmt.Sprintf("dry_run %t", dryRun), func(t *testing.T) {
			world := newUpstreamBanWorld(t)
			h := world.h

			userPost := world.postBy(ubUserID, world.communityC, "ub-rpfail-u-c1", ubBeforeBan, false)
			userComment := world.commentBy(ubUserID, world.communityC, "ub-rpfail-u-c1-reply", userPost, ubBeforeBan2)
			clearCommunityDID(t, h, userComment)
			corruptRecord(t, h, userComment)
			otherPost := world.postBy(ubOtherUserID, world.communityD, "ub-rpfail-v-d1", ubBeforeBan3, false)

			const (
				failingBanID = "https://lemmy.world/activities/announce/block/ub-rpfail-community"
				siteBanID    = ubOtherHost + "/activities/block/ub-rpfail-site"
			)
			enqueueProcessed(t, h, world.communityC,
				communityBlockAnnounce(world.communityC, failingBanID, ubUserID, nil), ubBanTime)
			enqueueProcessed(t, h, world.otherAdmin,
				siteBlock(siteBanID, ubOtherAdminID, ubOtherUserID, ubOtherRoot, nil), ubBanTime.Add(time.Hour))

			response := h.adminRequest(http.MethodPost, replayBansPath, map[string]any{"dry_run": dryRun})
			require.Equal(t, http.StatusOK, response.Code,
				"one failing ban does not fail the replay: %s", response.Body.String())
			var report struct {
				DryRun bool `json:"dry_run"`
				Bans   []struct {
					ActivityID string   `json:"activity_id"`
					Removed    []string `json:"removed"`
					Error      string   `json:"error"`
				} `json:"bans"`
				RemovedTotal int `json:"removed_total"`
			}
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &report), response.Body.String())
			assert.Equal(t, dryRun, report.DryRun)
			require.Len(t, report.Bans, 2, "both bans are reported: %s", response.Body.String())
			assert.Equal(t, failingBanID, report.Bans[0].ActivityID)
			assert.NotEmpty(t, report.Bans[0].Error, "the failing ban says it failed")
			assert.Equal(t, siteBanID, report.Bans[1].ActivityID)
			assert.Empty(t, report.Bans[1].Error, "the second ban succeeded")
			assert.Equal(t, []string{atURIOf(t, h, otherPost)}, report.Bans[1].Removed)
			assert.Equal(t, 1, report.RemovedTotal, "only the second ban's removal counts")

			if dryRun {
				assertLive(t, h, otherPost, "a dry run deletes nothing")
			} else {
				assertDeleted(t, h, otherPost, "V's pre-ban post: the ban after the failing one is applied")
			}
			assert.True(t, acceptanceExists(t, h, ubCommunityCDID, userPost),
				"the failing ban withdrew nothing of U's")
		})
	}
}

// corruptRecord replaces apID's record block with bytes that do not decode,
// leaving the repo's tree intact: reading the record fails, not "not found".
func corruptRecord(t *testing.T, h *harness, apID string) {
	t.Helper()
	ctx := context.Background()
	mapping, err := h.objects.GetByAPID(ctx, apID)
	require.NoError(t, err)
	result, err := h.db.ExecContext(ctx,
		`UPDATE blocks SET bytes = $1 WHERE did = $2 AND cid = $3`, []byte{0xff}, mapping.DID, mapping.CID)
	require.NoError(t, err)
	rows, err := result.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, int64(1), rows, "precondition: %s's record block is stored", apID)
	_, _, err = h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	require.Error(t, err, "precondition: %s's record no longer decodes", apID)
	require.False(t, errors.IsNotFound(err), "precondition: a read failure, not a missing record: %v", err)
}

// TestReplayingOverlappingBansOfOneAuthorListsEachActionOnce (review).
//
// GIVEN recorded bans of bridged user V — first C's community ban, then V's
// home site ban — and V's pre-ban postv2, comment and legacy post in C plus a
// post and a comment in D; WHEN the operator replays bans THEN the community
// ban lists what it acts on in C (the postv2 it withdraws, the comment and
// legacy post it deletes), the site ban lists the postv2 again (it deletes the
// record the community ban only withdrew) and V's content in D, but not the
// comment or legacy post the community ban already deleted; the dry run's
// listing equals the real run's; and the postv2 record is gone afterwards.
func TestReplayingOverlappingBansOfOneAuthorListsEachActionOnce(t *testing.T) {
	world := newUpstreamBanWorld(t)
	h := world.h

	otherPostC := world.postBy(ubOtherUserID, world.communityC, "ub-rpov-v-c1", ubBeforeBan, false)
	otherCommentC := world.commentBy(ubOtherUserID, world.communityC, "ub-rpov-v-c1-reply", otherPostC, ubBeforeBan2)
	const otherLegacyC = "https://lemmy.ml/post/ub-rpov-v-legacy"
	otherLegacyURI := putLegacyPost(t, h, ubCommunityCDID, replayOtherUserDID, otherLegacyC, replayLegacyRKey,
		time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC))
	otherPostD := world.postBy(ubOtherUserID, world.communityD, "ub-rpov-v-d1", ubBeforeBan3, false)
	otherCommentD := world.commentBy(ubOtherUserID, world.communityD, "ub-rpov-v-d1-reply", otherPostD, ubBeforeBan4)

	const (
		communityBanID = "https://lemmy.world/activities/announce/block/ub-rpov-community"
		siteBanID      = ubOtherHost + "/activities/block/ub-rpov-site"
	)
	enqueueProcessed(t, h, world.communityC,
		communityBlockAnnounce(world.communityC, communityBanID, ubOtherUserID, nil), ubBanTime)
	enqueueProcessed(t, h, world.otherAdmin,
		siteBlock(siteBanID, ubOtherAdminID, ubOtherUserID, ubOtherRoot, nil), ubBanTime.Add(24*time.Hour))

	postCURI := atURIOf(t, h, otherPostC)
	listing := func(dryRun bool) string {
		return mustJSON(t, map[string]any{
			"dry_run": dryRun,
			"bans": []any{
				map[string]any{
					"activity_id": communityBanID,
					"subject":     ubOtherUserID,
					"scope":       "community",
					"community":   world.communityC.id,
					"removed":     sortedStrings(postCURI, atURIOf(t, h, otherCommentC), otherLegacyURI),
				},
				map[string]any{
					"activity_id": siteBanID,
					"subject":     ubOtherUserID,
					"scope":       "site",
					"community":   "",
					"removed":     sortedStrings(postCURI, atURIOf(t, h, otherPostD), atURIOf(t, h, otherCommentD)),
				},
			},
			"removed_total": 6,
		})
	}

	dryRun := h.adminRequest(http.MethodPost, replayBansPath, map[string]any{"dry_run": true})
	require.Equal(t, http.StatusOK, dryRun.Code, dryRun.Body.String())
	assert.JSONEq(t, listing(true), dryRun.Body.String())

	realRun := h.adminRequest(http.MethodPost, replayBansPath, map[string]any{"dry_run": false})
	require.Equal(t, http.StatusOK, realRun.Code, realRun.Body.String())
	assert.JSONEq(t, listing(false), realRun.Body.String(), "the real run does exactly what the dry run listed")

	assertDeleted(t, h, otherPostC, "V's postv2: withdrawn from C by the community ban, deleted by the site ban")
	assertDeleted(t, h, otherCommentC, "V's comment in C: the community ban")
	assertDeleted(t, h, otherLegacyC, "V's legacy post in C's repo: the community ban")
	assertDeleted(t, h, otherPostD, "V's post in D: the site ban")
	assertDeleted(t, h, otherCommentD, "V's comment in D: the site ban")
}

// TestReplayingBansAppliesTheAnnounceGates (review): a recorded announced ban
// the live dispatch would have refused before deciding it — the Block targets
// something other than the announcing community, or the announcing community
// is no longer followed — lists nothing and removes nothing on replay.
func TestReplayingBansAppliesTheAnnounceGates(t *testing.T) {
	for _, tc := range []struct {
		name     string
		extra    map[string]any
		unfollow bool
	}{
		{name: "the Block targets the instance, not the announcing community",
			extra: map[string]any{"target": ubWorldRoot}},
		{name: "the announcing community is no longer followed", unfollow: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			world := newUpstreamBanWorld(t)
			h := world.h

			otherPost := world.postBy(ubOtherUserID, world.communityC, "ub-rpgate-v-c1", ubBeforeBan, false)
			otherComment := world.commentBy(ubOtherUserID, world.communityC, "ub-rpgate-v-c1-reply", otherPost, ubBeforeBan2)
			const banID = "https://lemmy.world/activities/announce/block/ub-rpgate"
			enqueueProcessed(t, h, world.communityC,
				communityBlockAnnounce(world.communityC, banID, ubOtherUserID, tc.extra), ubBanTime)
			if tc.unfollow {
				require.NoError(t, h.communities.SetFollowState(context.Background(), groupID, store.FollowStateNone))
			}

			for _, dryRun := range []bool{true, false} {
				response := h.adminRequest(http.MethodPost, replayBansPath, map[string]any{"dry_run": dryRun})
				require.Equal(t, http.StatusOK, response.Code, response.Body.String())
				assert.JSONEq(t, fmt.Sprintf(`{"dry_run": %t, "bans": [], "removed_total": 0}`, dryRun),
					response.Body.String())
			}
			assertLive(t, h, otherPost, tc.name)
			assert.True(t, acceptanceExists(t, h, ubCommunityCDID, otherPost), "V's post stays accepted in C: %s", tc.name)
			assert.Empty(t, removalCode(t, h, ubCommunityCDID, otherPost), "no removal record: %s", tc.name)
			assertLive(t, h, otherComment, tc.name)
		})
	}
}
