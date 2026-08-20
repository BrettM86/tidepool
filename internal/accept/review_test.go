package accept

import (
	"context"
	"database/sql"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/consume"
	"tidepool/internal/store"
)

// Round 4: the /second-opinion review findings — 6 HIGH + importants.

// seedCommunityState registers a bridged community in a given follow state.
func seedCommunityState(t *testing.T, conn *sql.DB, did, apid, name string, state store.FollowState) {
	t.Helper()
	_, err := store.NewCommunities(conn).UpsertCommunity(context.Background(), store.Community{
		APGroupID:         apid,
		DID:               did,
		PreferredUsername: name,
		Instance:          acCommunityHost,
		FollowState:       state,
	})
	require.NoError(t, err, "seed community in state %s", state)
}

func admissionRowsForPost(t *testing.T, conn *sql.DB, postURI string) int {
	t.Helper()
	var n int
	require.NoError(t, conn.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM admissions WHERE post_uri = $1`, postURI).Scan(&n))
	return n
}

// ---------------------------------------------------------------------------
// H1 — FollowState gate (SECURITY): admission requires an ACCEPTED Follow
// ---------------------------------------------------------------------------

func TestH1_AdmissionRequiresAcceptedFollowState(t *testing.T) {
	for _, state := range []store.FollowState{store.FollowStateNone, store.FollowStatePending} {
		t.Run(string(state), func(t *testing.T) {
			conn := acceptanceDB(t)
			ctx := context.Background()
			seedCommunityState(t, conn, acCommunityDID, acCommunityAPID, acCommunityName, state)
			repos := newRepos(t, conn)
			enq := realEnqueuer(t, conn)
			dispatcher := wireDispatcher(t, conn, engineWith(t, conn, repos, enq), enq)

			require.NoError(t, dispatcher.HandleEvent(ctx,
				postEvent("create", acPostRKey, acRevCreate, acPostCID, acPostTimeUS, pv2Record())))

			status, code := admissionOf(t, conn, acCommunityDID, acPostURI)
			assert.Equal(t, StatusRejected, status,
				"a post to a community Tidepool has no ACCEPTED Follow to must be rejected — a "+
					"communities row's mere existence is not authority to sign an acceptance into it")
			assert.Equal(t, DecisionCommunityNotFollowed, code)
			_, ok := acceptanceSubjectCID(t, repos, acCommunityDID, acPostURI)
			assert.False(t, ok, "no acceptance is signed for an unfollowed community")
			assert.Zero(t, countRows(t, conn, "outbound_activities"), "and nothing is enqueued")
		})
	}

	t.Run("accepted control", func(t *testing.T) {
		conn := acceptanceDB(t)
		ctx := context.Background()
		seedCommunityState(t, conn, acCommunityDID, acCommunityAPID, acCommunityName, store.FollowStateAccepted)
		repos := newRepos(t, conn)
		enq := realEnqueuer(t, conn)
		dispatcher := wireDispatcher(t, conn, engineWith(t, conn, repos, enq), enq)

		require.NoError(t, dispatcher.HandleEvent(ctx,
			postEvent("create", acPostRKey, acRevCreate, acPostCID, acPostTimeUS, pv2Record())))

		status, _ := admissionOf(t, conn, acCommunityDID, acPostURI)
		assert.Equal(t, StatusAccepted, status, "an ACCEPTED-follow community admits normally")
		_, ok := acceptanceSubjectCID(t, repos, acCommunityDID, acPostURI)
		assert.True(t, ok)
	})
}

// ---------------------------------------------------------------------------
// H2 — lexicon $type binding (SECURITY): validate against postv2 specifically
// ---------------------------------------------------------------------------

func TestH2_RecordMustBeAPostV2NotAnyValidLexicon(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	seedBridgedCommunity(t, conn)
	repos := newRepos(t, conn)
	enq := realEnqueuer(t, conn)
	dispatcher := wireDispatcher(t, conn, engineWith(t, conn, repos, enq), enq)

	// A record whose $type is a DIFFERENT known lexicon (actor.profile has no
	// required fields, so this validates against ITS schema) but carries a full
	// postv2 body. The engine must validate against social.coves.community.postv2
	// SPECIFICALLY — trusting the record's self-declared $type would let a
	// profile-shaped (or comment-shaped) record be signed as a community post.
	confused := pv2Record(func(r map[string]any) { r["$type"] = "social.coves.actor.profile" })
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("create", acPostRKey, acRevCreate, acPostCID, acPostTimeUS, confused)))

	status, code := admissionOf(t, conn, acCommunityDID, acPostURI)
	assert.Equal(t, StatusRejected, status,
		"a record that isn't a postv2 must be rejected even if it is a valid instance of its "+
			"own declared type — WE sign the acceptance, so the type is bound to the postv2 schema")
	assert.Equal(t, DecisionLexiconInvalid, code)
	_, ok := acceptanceSubjectCID(t, repos, acCommunityDID, acPostURI)
	assert.False(t, ok, "no acceptance is signed over a non-postv2 record")
}

// ---------------------------------------------------------------------------
// H3 — community immutability for REJECTIONS (reads the ADMISSIONS ledger)
// ---------------------------------------------------------------------------

func TestH3_RejectedPostCannotBeMovedToAnotherCommunity(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	seedBridgedCommunity(t, conn)  // community A (accepted follow)
	seedBridgedCommunityB(t, conn) // community B (accepted follow)
	repos := newRepos(t, conn)
	enq := realEnqueuer(t, conn)
	dispatcher := wireDispatcher(t, conn, engineWith(t, conn, repos, enq), enq)

	// Opted out → the create is REJECTED in community A. A rejection writes an
	// admissions row (community_did=A) but NO outbound_objects row.
	_, err := store.NewFederationPrefs(conn).Upsert(ctx, store.FederationPref{
		DID: acAuthorDID, Source: store.FederationPrefSourceRecord,
	})
	require.NoError(t, err)
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("create", acPostRKey, acRevCreate, acPostCID, acPostTimeUS, pv2Record())))
	require.Equal(t, 1, admissionRowCount(t, conn, acCommunityDID, acPostURI),
		"precondition: rejected in A")
	require.Zero(t, countRows(t, conn, "outbound_objects"),
		"precondition: a rejection writes no outbound_objects — so immutability can't read it")

	// The same post at-uri is now UPDATED targeting community B. The engine must
	// read the prior community from the ADMISSIONS LEDGER (the only surviving
	// state for a rejected post) and DISCARD the move.
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("update", acPostRKey, acRevUpdate, acPostCID2, acPostTimeUS+1,
			pv2Record(func(r map[string]any) { r["community"] = acCommunityB_DID }))))

	assert.Zero(t, admissionRowCount(t, conn, acCommunityB_DID, acPostURI),
		"a community-moving edit must write NOTHING to the target community, even for a "+
			"post that was only ever rejected")
	assert.Equal(t, 1, admissionRowCount(t, conn, acCommunityDID, acPostURI),
		"the original community's admission row is untouched")
	assert.Equal(t, 1, admissionRowsForPost(t, conn, acPostURI),
		"one post_uri must never hold admissions rows under two communities")
}

// ---------------------------------------------------------------------------
// H4 — seq-bump idempotency: unchanged content must not mint a second activity
// ---------------------------------------------------------------------------

func TestH4_ReadmitOfUnchangedAcceptedPostDoesNotMintASecondActivity(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	seedBridgedCommunity(t, conn)
	repos := newRepos(t, conn)
	enq := realEnqueuer(t, conn)
	engine := engineWith(t, conn, repos, enq)
	dispatcher := wireDispatcher(t, conn, engine, enq)

	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("create", acPostRKey, acRevCreate, acPostCID, acPostTimeUS, pv2Record())))
	require.Equal(t, 1, countRows(t, conn, "outbound_activities"))
	originalID := consume.ActivityID(acUserOrigin, acPostURI, "create", 0)

	// Readmit the already-accepted, UNCHANGED post (a redelivery-shaped re-run).
	_, err := engine.Readmit(ctx, acPostURI)
	require.NoError(t, err)

	assert.Equal(t, 1, countRows(t, conn, "outbound_activities"),
		"re-running an unchanged accepted post must NOT mint a second Create{Page}: the "+
			"outbound_objects seq must be guarded on content change (like TombstoneTx guards on "+
			"tombstoned_at), or every redelivery/readmit invents a new activity id")
	var seq int
	require.NoError(t, conn.QueryRowContext(ctx,
		`SELECT last_activity_seq FROM outbound_objects WHERE at_uri = $1`, acPostURI).Scan(&seq))
	assert.Equal(t, 0, seq, "the activity seq must not bump when the content is unchanged")

	var kept string
	require.NoError(t, conn.QueryRowContext(ctx,
		`SELECT activity_id FROM outbound_activities`).Scan(&kept))
	assert.Equal(t, originalID, kept, "the one activity keeps its ORIGINAL id")
}

// ---------------------------------------------------------------------------
// H5 — a corrective edit after an admission-revoked removal AUTO-RESTORES
// (RULING: propose auto-restore; the admission-revocation was OUR decision)
// ---------------------------------------------------------------------------

func TestH5_CorrectiveEditAfterAdmissionRemovalAutoRestores(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	seedBridgedCommunity(t, conn)
	repos := newRepos(t, conn)
	enq := realEnqueuer(t, conn)
	dispatcher := wireDispatcher(t, conn, engineWith(t, conn, repos, enq), enq)

	// Accepted, then edited titleless → REMOVED (admission-revoked removal stands).
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("create", acPostRKey, acRevCreate, acPostCID, acPostTimeUS, pv2Record())))
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("update", acPostRKey, acRevUpdate, acPostCID2, acPostTimeUS+1,
			pv2Record(func(r map[string]any) { delete(r, "title") }))))
	_, removed := removalStandsAt(t, repos, acCommunityDID, acPostURI)
	require.True(t, removed, "precondition: an admission-revoked removal stands")

	// A LATER corrective edit that now PASSES admission (title restored). It must
	// NOT error out of AdmitPost (that would redrive forever against the standing
	// removal); the admission-revocation was the bridge's own decision, so a
	// corrective edit auto-restores.
	err := dispatcher.HandleEvent(ctx,
		postEvent("update", acPostRKey, "3lzpostrev004", acPostCID, acPostTimeUS+2, pv2Record()))
	require.NoError(t, err,
		"a corrective edit must be a DECIDED outcome, not a transient error that redrives forever "+
			"against ErrRemovalStands")

	_, stillRemoved := removalStandsAt(t, repos, acCommunityDID, acPostURI)
	assert.False(t, stillRemoved, "the removal is deleted on auto-restore")
	_, ok := acceptanceSubjectCID(t, repos, acCommunityDID, acPostURI)
	assert.True(t, ok, "a fresh acceptance is written")
	status, _ := admissionOf(t, conn, acCommunityDID, acPostURI)
	assert.Equal(t, StatusAccepted, status, "the ledger is back to accepted")

	// Lemmy received a Delete{Page} at removal, so the restore re-adds it with a
	// Create{Page} (op derived from state — the live copy is gone), not Update.
	assert.Equal(t, 2, activityKindCount(t, conn, "Create"),
		"the restore enqueues a Create{Page} (Lemmy has no live copy after the Delete)")
	assert.Equal(t, 1, activityKindCount(t, conn, "Delete"))
}

// ---------------------------------------------------------------------------
// H6 — op derived from outbound STATE, not the Jetstream commit operation
// ---------------------------------------------------------------------------

func TestH6_UpdateEventForNeverAcceptedPostEmitsCreate(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	seedBridgedCommunity(t, conn)
	repos := newRepos(t, conn)
	enq := realEnqueuer(t, conn)
	dispatcher := wireDispatcher(t, conn, engineWith(t, conn, repos, enq), enq)

	// An UPDATE event for a post Lemmy has NEVER seen (no outbound state). Lemmy
	// got no Create, so this must federate as Create{Page}, not Update{Page} —
	// the op is a function of what Lemmy already holds, not of the commit operation.
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("update", acPostRKey, acRevUpdate, acPostCID, acPostTimeUS, pv2Record())))

	assert.Equal(t, 1, activityKindCount(t, conn, "Create"),
		"an update of a never-federated post is a Create{Page}: op comes from outbound-state "+
			"presence, not commit.Operation")
	assert.Zero(t, activityKindCount(t, conn, "Update"),
		"Lemmy never received a Create, so an Update would reference an object it does not have")
	wantCreateID := consume.ActivityID(acUserOrigin, acPostURI, "create", 0)
	var id string
	require.NoError(t, conn.QueryRowContext(ctx,
		`SELECT activity_id FROM outbound_activities`).Scan(&id))
	assert.Equal(t, wantCreateID, id, "the activity id uses op=create")
}

func TestH6_UpdateOfAcceptedPostEmitsUpdate(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	seedBridgedCommunity(t, conn)
	repos := newRepos(t, conn)
	enq := realEnqueuer(t, conn)
	dispatcher := wireDispatcher(t, conn, engineWith(t, conn, repos, enq), enq)

	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("create", acPostRKey, acRevCreate, acPostCID, acPostTimeUS, pv2Record())))
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("update", acPostRKey, acRevUpdate, acPostCID2, acPostTimeUS+1, pv2Record())))

	assert.Equal(t, 1, activityKindCount(t, conn, "Update"),
		"an edit of an accepted post (Lemmy has a live copy) federates as Update{Page}")
}

// ---------------------------------------------------------------------------
// Important — title cap counts RUNES, not bytes
// ---------------------------------------------------------------------------

func TestImportant_TitleCapCountsRunesNotBytes(t *testing.T) {
	t.Run("150 multibyte runes accepted", func(t *testing.T) {
		conn := acceptanceDB(t)
		ctx := context.Background()
		seedBridgedCommunity(t, conn)
		repos := newRepos(t, conn)
		enq := realEnqueuer(t, conn)
		dispatcher := wireDispatcher(t, conn, engineWith(t, conn, repos, enq), enq)

		// 150 CJK runes = 450 bytes: over the byte cap but well under the 200-rune
		// (grapheme) cap Lemmy actually enforces.
		title := strings.Repeat("あ", 150)
		require.NoError(t, dispatcher.HandleEvent(ctx,
			postEvent("create", acPostRKey, acRevCreate, acPostCID, acPostTimeUS,
				pv2Record(func(r map[string]any) { r["title"] = title }))))

		status, code := admissionOf(t, conn, acCommunityDID, acPostURI)
		assert.Equal(t, StatusAccepted, status,
			"a 150-rune multibyte title (450 bytes) must be ACCEPTED: the cap counts runes, not bytes")
		assert.Empty(t, code)
		_, ok := acceptanceSubjectCID(t, repos, acCommunityDID, acPostURI)
		assert.True(t, ok)
	})

	t.Run("201 runes rejected", func(t *testing.T) {
		conn := acceptanceDB(t)
		ctx := context.Background()
		seedBridgedCommunity(t, conn)
		enq := realEnqueuer(t, conn)
		dispatcher := wireDispatcher(t, conn, engineWith(t, conn, newRepos(t, conn), enq), enq)

		require.NoError(t, dispatcher.HandleEvent(ctx,
			postEvent("create", acPostRKey, acRevCreate, acPostCID, acPostTimeUS,
				pv2Record(func(r map[string]any) { r["title"] = strings.Repeat("x", lemmyTitleCap+1) }))))

		_, code := admissionOf(t, conn, acCommunityDID, acPostURI)
		assert.Equal(t, DecisionTitleTooLong, code, "201 runes exceeds the cap")
	})
}

// ---------------------------------------------------------------------------
// Important — a removal's createdAt is the DECISION time, not the post's
// ---------------------------------------------------------------------------

func TestImportant_RemovalCreatedAtIsDecisionTimeNotPublication(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	seedBridgedCommunity(t, conn)
	repos := newRepos(t, conn)
	enq := realEnqueuer(t, conn)

	decisionTime := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	engine := engineWith(t, conn, repos, enq, func(o *Options) { o.Now = func() time.Time { return decisionTime } })
	dispatcher := wireDispatcher(t, conn, engine, enq)

	// An OLD post (published in 2020), accepted, then edited titleless → removed.
	oldPublished := "2020-01-01T00:00:00.000Z"
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("create", acPostRKey, acRevCreate, acPostCID, acPostTimeUS,
			pv2Record(func(r map[string]any) { r["createdAt"] = oldPublished }))))
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("update", acPostRKey, acRevUpdate, acPostCID2, acPostTimeUS+1,
			pv2Record(func(r map[string]any) { r["createdAt"] = oldPublished; delete(r, "title") }))))

	removal, ok := removalStandsAt(t, repos, acCommunityDID, acPostURI)
	require.True(t, ok)
	assert.Equal(t, "2026-08-13T12:00:00.000Z", removal["createdAt"],
		"a removal's createdAt is WHEN THE ENGINE DECIDED (injected now), not the post's 2020 "+
			"publication time — a removal is a claim about the decision, not the content")
}

// ---------------------------------------------------------------------------
// Admin — readmit error mapping (regression pins for the existing handler)
// ---------------------------------------------------------------------------

func TestAdminReadmit_UnknownPostIs404(t *testing.T) {
	conn := acceptanceDB(t)
	engine := engineWith(t, conn, newRepos(t, conn), realEnqueuer(t, conn))
	router := newAdminRouter(t, conn, engine)

	rec := adminRequest(t, router, http.MethodPost, "/admin/admissions/readmit", adminToken,
		`{"post":"at://did:plc:nobody/social.coves.community.postv2/nope"}`)
	assert.Equal(t, http.StatusNotFound, rec.Code, "readmit of a post with no admission row is 404")
}

func TestAdminReadmit_LegacyEmptySnapshotIs422(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	// A ledger row with NO stored snapshot (a legacy/pre-migration-022 decision).
	require.NoError(t, NewAdmissions(conn).Record(ctx, Admission{
		AuthorDID:    acAuthorDID,
		CommunityDID: acCommunityDID,
		PostURI:      acPostURI,
		Status:       StatusRejected,
		DecisionCode: DecisionOptedOut,
		// EvaluatedSnapshot left nil → stored as '{}'.
	}))
	engine := engineWith(t, conn, newRepos(t, conn), realEnqueuer(t, conn))
	router := newAdminRouter(t, conn, engine)

	rec := adminRequest(t, router, http.MethodPost, "/admin/admissions/readmit", adminToken,
		`{"post":"`+acPostURI+`"}`)
	assert.Equal(t, http.StatusUnprocessableEntity, rec.Code,
		"a readmit whose record snapshot did not survive is a 422, never a silent no-op")
}
