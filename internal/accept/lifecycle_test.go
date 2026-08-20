package accept

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/acceptrec"
	"tidepool/internal/consume"
	"tidepool/internal/errors"
	"tidepool/internal/repo"
	"tidepool/internal/store"
)

// Round 2: the edit / author-delete lifecycle and the rest of admission policy.
// These build on the round-1 harness in outer_acceptance_test.go (acceptanceDB,
// newRepos, realEnqueuer, wireDispatcher, seedBridgedCommunity, the ac* world,
// and the query helpers).

// A second bridged community + a repinned CID the edit lifecycle needs.
const (
	acCommunityB_DID   = "did:plc:z72i7hdynmk6r22z27h6tvur"
	acCommunityB_APID  = "https://lemmy.world/c/science"
	acCommunityB_Name  = "science"
	acCommunityB_Inbox = "https://lemmy.world/c/science/inbox"

	acPostCID2 = "bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqrsqxi3jgxte"

	acRevCreate = acPostRev
	acRevUpdate = "3lzpostrev002"
	acRevDelete = "3lzpostrev003"
)

// ---------------------------------------------------------------------------
// Round-2 harness
// ---------------------------------------------------------------------------

// engineWith builds an Engine with the round-1 seams plus the round-2 additions
// (APActors for the paused check, a mutable Options for the rate cap), letting
// each test override one field.
func engineWith(t *testing.T, conn *sql.DB, repos *repo.Manager, enqueuer consume.OutboundEnqueuer, mutate ...func(*Options)) *Engine {
	t.Helper()
	opts := Options{
		Repos:       repos,
		Enqueuer:    enqueuer,
		Actors:      newMinter(t, conn),
		Resolver:    stubResolver{},
		Communities: store.NewCommunities(conn),
		Objects:     store.NewOutboundObjects(conn),
		Prefs:       store.NewFederationPrefs(conn),
		Admissions:  NewAdmissions(conn),
		APActors:    store.NewAPActors(conn),
		UserOrigin:  acUserOrigin,
	}
	for _, m := range mutate {
		m(&opts)
	}
	engine, err := NewEngine(opts)
	require.NoError(t, err)
	return engine
}

// pv2Record builds a valid postv2 record; the mutators shape the invalid /
// variant cases (titleless, over-cap, moved community, malformed createdAt).
func pv2Record(mutate ...func(map[string]any)) map[string]any {
	r := map[string]any{
		"$type":     "social.coves.community.postv2",
		"community": acCommunityDID,
		"title":     "hello from atproto",
		"content":   "the body of the post",
		"createdAt": "2026-08-12T10:00:00.000Z",
	}
	for _, m := range mutate {
		m(r)
	}
	return r
}

// postEvent assembles a postv2 commit event. A delete carries no record and no
// CID (that absence is the whole reason outbound_objects exists).
func postEvent(op, rkey, rev, cid string, timeUS int64, record map[string]any) *consume.JetstreamEvent {
	return &consume.JetstreamEvent{
		DID:    acAuthorDID,
		TimeUS: timeUS,
		Kind:   "commit",
		Commit: &consume.CommitEvent{
			Rev:        rev,
			Operation:  op,
			Collection: "social.coves.community.postv2",
			RKey:       rkey,
			CID:        cid,
			Record:     record,
		},
	}
}

func seedBridgedCommunityB(t *testing.T, conn *sql.DB) {
	t.Helper()
	_, err := store.NewCommunities(conn).UpsertCommunity(context.Background(), store.Community{
		APGroupID:         acCommunityB_APID,
		DID:               acCommunityB_DID,
		PreferredUsername: acCommunityB_Name,
		Instance:          acCommunityHost,
		FollowState:       store.FollowStateAccepted,
	})
	require.NoError(t, err, "seed second bridged community")
}

// seedPausedActor inserts an ap_actors row for the author with delivery_paused
// set: the paused admission check reads exactly this.
func seedPausedActor(t *testing.T, conn *sql.DB, paused bool) {
	t.Helper()
	_, err := conn.ExecContext(context.Background(), `
		INSERT INTO ap_actors (did, kind, actor_id, normalized_origin, local_part,
		                       rsa_key_sealed, rsa_key_version, public_key_pem, delivery_paused)
		VALUES ($1, 'person', $2, 'coves.social', 'author', '\x00'::bytea, 1, 'pem', $3)`,
		acAuthorDID, acUserOrigin+"/ap/actor/"+acAuthorDID, paused)
	require.NoError(t, err, "seed ap_actors row for the author")
}

func activityKindCount(t *testing.T, conn *sql.DB, kind string) int {
	t.Helper()
	var n int
	require.NoError(t, conn.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM outbound_activities WHERE kind = $1`, kind).Scan(&n))
	return n
}

// acceptanceSubjectCID returns the CID the acceptance at postURI's rkey pins, or
// "" (with found=false) when no acceptance stands.
func acceptanceSubjectCID(t *testing.T, repos *repo.Manager, communityDID, postURI string) (string, bool) {
	t.Helper()
	rec, _, err := repos.GetRecord(context.Background(), communityDID, acceptrec.CollectionAcceptance, acceptrec.SubjectRKey(postURI))
	if errors.IsNotFound(err) {
		return "", false
	}
	require.NoError(t, err)
	subject, _ := rec["subject"].(map[string]any)
	cid, _ := subject["cid"].(string)
	return cid, true
}

func removalStandsAt(t *testing.T, repos *repo.Manager, communityDID, postURI string) (map[string]any, bool) {
	t.Helper()
	rec, _, err := repos.GetRecord(context.Background(), communityDID, acceptrec.CollectionRemoval, acceptrec.SubjectRKey(postURI))
	if errors.IsNotFound(err) {
		return nil, false
	}
	require.NoError(t, err)
	return rec, true
}

// admissionRowCount reports how many admissions rows exist for a post.
func admissionRowCount(t *testing.T, conn *sql.DB, communityDID, postURI string) int {
	t.Helper()
	var n int
	require.NoError(t, conn.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM admissions WHERE community_did = $1 AND post_uri = $2`,
		communityDID, postURI).Scan(&n))
	return n
}

// admittedCreate runs a fresh accepted create and returns the ctx for reuse.
func admittedCreate(t *testing.T, dispatcher *consume.Dispatcher) {
	t.Helper()
	require.NoError(t, dispatcher.HandleEvent(context.Background(),
		postEvent("create", acPostRKey, acRevCreate, acPostCID, acPostTimeUS, pv2Record())))
}

// ---------------------------------------------------------------------------
// E1 — edit repin: UPDATE with a new CID re-pins the acceptance + Update{Page}
// ---------------------------------------------------------------------------

func TestEditRepinsAcceptanceAndEnqueuesUpdatePage(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	seedBridgedCommunity(t, conn)
	repos := newRepos(t, conn)
	dispatcher := wireDispatcher(t, conn, engineWith(t, conn, repos, realEnqueuer(t, conn)), realEnqueuer(t, conn))

	admittedCreate(t, dispatcher)
	cid, ok := acceptanceSubjectCID(t, repos, acCommunityDID, acPostURI)
	require.True(t, ok)
	require.Equal(t, acPostCID, cid, "precondition: the create pinned CID v1")

	// The edit: same post, NEW CID, same community, still a valid title.
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("update", acPostRKey, acRevUpdate, acPostCID2, acPostTimeUS+1, pv2Record())))

	// The acceptance now pins the NEW CID at the SAME digest rkey.
	cid, ok = acceptanceSubjectCID(t, repos, acCommunityDID, acPostURI)
	require.True(t, ok, "the acceptance must still stand after an edit")
	assert.Equal(t, acPostCID2, cid,
		"an edit re-pins the acceptance to the new CID at the same digest rkey (a repin, not a new record)")

	// Exactly one Update{Page}, under the update activity id with the bumped seq.
	assert.Equal(t, 1, activityKindCount(t, conn, "Update"),
		"an edit that still passes admission enqueues exactly one Update{Page}")
	wantUpdateID := consume.ActivityID(acUserOrigin, acPostURI, "update", 1)
	var n int
	require.NoError(t, conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM outbound_activities WHERE activity_id = $1 AND kind = 'Update'`, wantUpdateID).Scan(&n))
	assert.Equal(t, 1, n, "the Update{Page} id is ActivityID(origin, postURI, update, seq=1)")

	// The outbound snapshot moved to the new version.
	stored, err := store.NewOutboundObjects(conn).GetByATURI(ctx, acPostURI)
	require.NoError(t, err)
	assert.Equal(t, acPostCID2, stored.LastCID, "the outbound_objects snapshot follows the edit")

	// The ledger stays accepted, now against the new CID.
	status, code := admissionOf(t, conn, acCommunityDID, acPostURI)
	assert.Equal(t, StatusAccepted, status)
	assert.Empty(t, code)
	var evaluated, accepted string
	require.NoError(t, conn.QueryRowContext(ctx,
		`SELECT evaluated_cid, accepted_cid FROM admissions WHERE community_did = $1 AND post_uri = $2`,
		acCommunityDID, acPostURI).Scan(&evaluated, &accepted))
	assert.Equal(t, acPostCID2, evaluated, "the ledger records the edited CID it evaluated")
	assert.Equal(t, acPostCID2, accepted, "and the CID it re-accepted")

	// Replaying the edit enqueues nothing new (rev gate + idempotent id).
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("update", acPostRKey, acRevUpdate, acPostCID2, acPostTimeUS+1, pv2Record())))
	assert.Equal(t, 1, activityKindCount(t, conn, "Update"), "a replayed edit is a no-op")
}

// ---------------------------------------------------------------------------
// E2 — edit fails admission: an accepted post edited titleless is REMOVED
// ---------------------------------------------------------------------------

func TestEditThatFailsAdmissionRemovesAndEnqueuesDelete(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	seedBridgedCommunity(t, conn)
	repos := newRepos(t, conn)
	dispatcher := wireDispatcher(t, conn, engineWith(t, conn, repos, realEnqueuer(t, conn)), realEnqueuer(t, conn))

	admittedCreate(t, dispatcher)
	_, ok := acceptanceSubjectCID(t, repos, acCommunityDID, acPostURI)
	require.True(t, ok, "precondition: the post was accepted")

	// The edit strips the title — Lemmy rejects a titleless post, so a post that
	// WAS accepted and is edited to fail admission is REMOVED (not merely left
	// alone): acceptance deleted + removal written + Delete{Page} enqueued.
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("update", acPostRKey, acRevUpdate, acPostCID2, acPostTimeUS+1,
			pv2Record(func(r map[string]any) { delete(r, "title") }))),
		"an admission-failing edit is a decided outcome (removal), not an event that retries forever")

	_, ok = acceptanceSubjectCID(t, repos, acCommunityDID, acPostURI)
	assert.False(t, ok, "the acceptance must be gone: the edited post no longer qualifies")

	removal, ok := removalStandsAt(t, repos, acCommunityDID, acPostURI)
	require.True(t, ok, "a removal must stand at the digest rkey (atomic with the acceptance delete)")
	assert.Equal(t, RemovalCodeAdmissionRevoked, removal["code"],
		"the removal carries the admission-revoked code (PROPOSED — see the constant's ruling flag)")
	subject, _ := removal["subject"].(map[string]any)
	assert.Equal(t, acPostURI, subject["uri"], "the removal pins the post at-uri")

	assert.Equal(t, 1, activityKindCount(t, conn, "Delete"),
		"a removed post's Delete{Page} withdraws it from Lemmy")

	status, code := admissionOf(t, conn, acCommunityDID, acPostURI)
	assert.Equal(t, StatusRemoved, status,
		"a post that WAS accepted then fails re-admission is REMOVED, not rejected (it federated once)")
	assert.Equal(t, DecisionTitleRequired, code,
		"the ledger records the SPECIFIC cause even though the firehose removal code is the open-set one")
}

// ---------------------------------------------------------------------------
// E3 — author-delete: tombstone deletes the acceptance (NO removal) + Delete{Page}
// ---------------------------------------------------------------------------

func TestAuthorDeleteRemovesAcceptanceWithoutRemovalRecord(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	seedBridgedCommunity(t, conn)
	repos := newRepos(t, conn)
	dispatcher := wireDispatcher(t, conn, engineWith(t, conn, repos, realEnqueuer(t, conn)), realEnqueuer(t, conn))

	admittedCreate(t, dispatcher)
	_, ok := acceptanceSubjectCID(t, repos, acCommunityDID, acPostURI)
	require.True(t, ok, "precondition: the post was accepted")

	// The author deletes their postv2. The delete commit carries no record and no
	// CID; everything the Delete{Page} needs is read from outbound_objects.
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("delete", acPostRKey, acRevDelete, "", acPostTimeUS+2, nil)),
		"an author delete must reach the engine and take the acceptance down")

	_, ok = acceptanceSubjectCID(t, repos, acCommunityDID, acPostURI)
	assert.False(t, ok, "the acceptance must be deleted with the author's post")

	_, hasRemoval := removalStandsAt(t, repos, acCommunityDID, acPostURI)
	assert.False(t, hasRemoval,
		"author deletion is NOT moderation, so NO removal record is written — the acceptance "+
			"just goes away")

	assert.Equal(t, 1, activityKindCount(t, conn, "Delete"),
		"the retraction is one Delete{Page}, built from the stored outbound state")

	stored, err := store.NewOutboundObjects(conn).GetByATURI(ctx, acPostURI)
	require.NoError(t, err)
	assert.True(t, stored.IsTombstoned(), "the post's outbound state is tombstoned")

	// RULING (flagged): the admissions ledger row is DELETED on author-delete —
	// the post no longer exists to re-decide, and there is no removal record to
	// explain, so a 'removed' row would falsely imply a moderation removal. See
	// the report for the audit-history alternative.
	assert.Zero(t, admissionRowCount(t, conn, acCommunityDID, acPostURI),
		"the ledger row is removed: the decided post is gone and no removal record stands for it")

	// Replaying the delete is a no-op.
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("delete", acPostRKey, acRevDelete, "", acPostTimeUS+2, nil)))
	assert.Equal(t, 1, activityKindCount(t, conn, "Delete"), "a replayed author delete is a no-op")
}

// ---------------------------------------------------------------------------
// E4 — community immutability: an UPDATE that MOVES the post is discarded whole
// ---------------------------------------------------------------------------

func TestUpdateThatMovesCommunityIsDiscarded(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	seedBridgedCommunity(t, conn)
	seedBridgedCommunityB(t, conn)
	repos := newRepos(t, conn)
	dispatcher := wireDispatcher(t, conn, engineWith(t, conn, repos, realEnqueuer(t, conn)), realEnqueuer(t, conn))

	admittedCreate(t, dispatcher) // accepted into community A

	// The hijack: an edit whose `community` names community B. The lexicon makes
	// `community` immutable; the whole event is discarded.
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("update", acPostRKey, acRevUpdate, acPostCID2, acPostTimeUS+1,
			pv2Record(func(r map[string]any) { r["community"] = acCommunityB_DID }))),
		"a community-moving edit is discarded quietly, not dead-lettered")

	// Community B must have NO acceptance for this post.
	_, movedIn := acceptanceSubjectCID(t, repos, acCommunityB_DID, acPostURI)
	assert.False(t, movedIn,
		"the engine must refuse to move the post: no acceptance may appear in the target community")

	// Community A's acceptance is untouched (still the original version — the
	// event was discarded WHOLE, so not even a repin lands).
	cid, ok := acceptanceSubjectCID(t, repos, acCommunityDID, acPostURI)
	require.True(t, ok, "the original acceptance stands")
	assert.Equal(t, acPostCID, cid, "the original community keeps the version it accepted; nothing moved")

	// No second activity was enqueued.
	assert.Equal(t, 1, countRows(t, conn, "outbound_activities"),
		"a discarded community-move enqueues nothing")

	// The ledger still binds the post to community A.
	assert.Equal(t, 1, admissionRowCount(t, conn, acCommunityDID, acPostURI),
		"the post's admission stays under the original community")
	assert.Zero(t, admissionRowCount(t, conn, acCommunityB_DID, acPostURI),
		"and no admission row is written for the target community")
}

// ---------------------------------------------------------------------------
// E5 — the rest of admission policy (each records a distinct decision_code)
// ---------------------------------------------------------------------------

func TestPausedAuthorPostIsRejected(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	seedBridgedCommunity(t, conn)
	seedPausedActor(t, conn, true) // the author already has an actor, now paused
	repos := newRepos(t, conn)
	dispatcher := wireDispatcher(t, conn, engineWith(t, conn, repos, realEnqueuer(t, conn)), realEnqueuer(t, conn))

	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("create", acPostRKey, acRevCreate, acPostCID, acPostTimeUS, pv2Record())))

	status, code := admissionOf(t, conn, acCommunityDID, acPostURI)
	assert.Equal(t, StatusRejected, status)
	assert.Equal(t, DecisionPaused, code,
		"a paused (#account) author's post is rejected with a distinct paused code — delivery is halted")
	_, ok := acceptanceSubjectCID(t, repos, acCommunityDID, acPostURI)
	assert.False(t, ok, "no acceptance for a paused author")
	assert.Zero(t, countRows(t, conn, "outbound_activities"), "and nothing enqueued")
}

func TestTitleRequiredAndTooLongAreRejectedOnCreate(t *testing.T) {
	t.Run("titleless create", func(t *testing.T) {
		conn := acceptanceDB(t)
		ctx := context.Background()
		seedBridgedCommunity(t, conn)
		repos := newRepos(t, conn)
		dispatcher := wireDispatcher(t, conn, engineWith(t, conn, repos, realEnqueuer(t, conn)), realEnqueuer(t, conn))

		require.NoError(t, dispatcher.HandleEvent(ctx,
			postEvent("create", acPostRKey, acRevCreate, acPostCID, acPostTimeUS,
				pv2Record(func(r map[string]any) { delete(r, "title") }))),
			"a titleless post is a recorded rejection, not an error that retries forever")

		status, code := admissionOf(t, conn, acCommunityDID, acPostURI)
		assert.Equal(t, StatusRejected, status)
		assert.Equal(t, DecisionTitleRequired, code,
			"a media-only postv2 rejects with title-required until a title-derivation decision exists")
		assert.Zero(t, countRows(t, conn, "outbound_activities"))
	})

	t.Run("over-cap create", func(t *testing.T) {
		conn := acceptanceDB(t)
		ctx := context.Background()
		seedBridgedCommunity(t, conn)
		repos := newRepos(t, conn)
		dispatcher := wireDispatcher(t, conn, engineWith(t, conn, repos, realEnqueuer(t, conn)), realEnqueuer(t, conn))

		long := strings.Repeat("x", lemmyTitleCap+1)
		require.NoError(t, dispatcher.HandleEvent(ctx,
			postEvent("create", acPostRKey, acRevCreate, acPostCID, acPostTimeUS,
				pv2Record(func(r map[string]any) { r["title"] = long }))))

		status, code := admissionOf(t, conn, acCommunityDID, acPostURI)
		assert.Equal(t, StatusRejected, status)
		assert.Equal(t, DecisionTitleTooLong, code,
			"a title over Lemmy's 200-char cap rejects with title-too-long")
	})
}

func TestRateCapRejectsBeyondThePerAuthorPerCommunityLimit(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	seedBridgedCommunity(t, conn)
	repos := newRepos(t, conn)
	// A deliberately tiny cap: two accepted posts, then the third is refused.
	enq := realEnqueuer(t, conn)
	dispatcher := wireDispatcher(t, conn,
		engineWith(t, conn, repos, enq, func(o *Options) { o.MaxPerAuthorPerCommunity = 2 }), enq)

	rkeys := []string{"3lzpostrate01", "3lzpostrate02", "3lzpostrate03"}
	revs := []string{"3lzraterev001", "3lzraterev002", "3lzraterev003"}
	for i, rkey := range rkeys {
		require.NoError(t, dispatcher.HandleEvent(ctx,
			postEvent("create", rkey, revs[i], acPostCID, acPostTimeUS+int64(i), pv2Record())))
	}

	third := "at://" + acAuthorDID + "/social.coves.community.postv2/" + rkeys[2]
	status, code := admissionOf(t, conn, acCommunityDID, third)
	assert.Equal(t, StatusRejected, status)
	assert.Equal(t, DecisionRateLimit, code,
		"the third accepted post in one community by one author exceeds the cap of 2 and is rate-limited")

	// Exactly two acceptances stand (the first two).
	var accepted int
	require.NoError(t, conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM admissions WHERE community_did = $1 AND status = 'accepted'`,
		acCommunityDID).Scan(&accepted))
	assert.Equal(t, 2, accepted, "only the posts under the cap are accepted")
}

func TestLexiconInvalidPostIsRejected(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	seedBridgedCommunity(t, conn)
	repos := newRepos(t, conn)
	dispatcher := wireDispatcher(t, conn, engineWith(t, conn, repos, realEnqueuer(t, conn)), realEnqueuer(t, conn))

	// createdAt is REQUIRED and must be an atproto datetime string; a number is a
	// clear lexicon type violation, distinct from any admission-policy check.
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("create", acPostRKey, acRevCreate, acPostCID, acPostTimeUS,
			pv2Record(func(r map[string]any) { r["createdAt"] = 12345 }))),
		"a post we sign the acceptance for must fail closed on invalid input — recorded, not errored")

	status, code := admissionOf(t, conn, acCommunityDID, acPostURI)
	assert.Equal(t, StatusRejected, status)
	assert.Equal(t, DecisionLexiconInvalid, code,
		"strict lexicon validation of the native input: invalid → rejection lexicon-invalid")
	_, ok := acceptanceSubjectCID(t, repos, acCommunityDID, acPostURI)
	assert.False(t, ok, "no acceptance is signed over input that does not validate")
}

// Ban / community-tombstone / parent-lock are TASK 17 (moderation + community
// lifecycle). Pinned here as a skipped placeholder so the boundary is explicit
// and the engine does NOT silently implement them in task 16.
func TestBanAndCommunityLifecycleChecksAreTask17(t *testing.T) {
	t.Skip("author-banned, community-unfollowed/tombstoned, and parent-locked admission " +
		"checks belong to task 17 (echo-moderation + community lifecycle); task 16 does not implement them")
}

// ---------------------------------------------------------------------------
// E6 — re-acceptance race / replay + no retroactive acceptance
// ---------------------------------------------------------------------------

// A post rejected while its author was opted out stays rejected after the author
// re-enables and posts something NEW: content authored while opted out never
// federates (decision 11 — the author reposts).
func TestRejectedWhileOptedOutStaysRejectedAfterReEnable(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	seedBridgedCommunity(t, conn)
	repos := newRepos(t, conn)
	dispatcher := wireDispatcher(t, conn, engineWith(t, conn, repos, realEnqueuer(t, conn)), realEnqueuer(t, conn))

	// Opted out: the first post is rejected.
	_, err := store.NewFederationPrefs(conn).Upsert(ctx, store.FederationPref{
		DID: acAuthorDID, Source: store.FederationPrefSourceRecord,
	})
	require.NoError(t, err)

	firstRKey := "3lzpostopt0a"
	firstURI := "at://" + acAuthorDID + "/social.coves.community.postv2/" + firstRKey
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("create", firstRKey, "3lzoptrev001", acPostCID, acPostTimeUS, pv2Record())))
	status, code := admissionOf(t, conn, acCommunityDID, firstURI)
	require.Equal(t, StatusRejected, status)
	require.Equal(t, DecisionOptedOut, code)

	// Re-enable: deleting the opt-out record restores default-on.
	require.NoError(t, store.NewFederationPrefs(conn).Delete(ctx, acAuthorDID))

	// A NEW post is accepted.
	secondRKey := "3lzpostopt0b"
	secondURI := "at://" + acAuthorDID + "/social.coves.community.postv2/" + secondRKey
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("create", secondRKey, "3lzoptrev002", acPostCID, acPostTimeUS+1, pv2Record())))
	status2, _ := admissionOf(t, conn, acCommunityDID, secondURI)
	assert.Equal(t, StatusAccepted, status2, "a new post after re-enabling federates")

	// The OLD post stays rejected — no retroactive acceptance.
	status, code = admissionOf(t, conn, acCommunityDID, firstURI)
	assert.Equal(t, StatusRejected, status,
		"the post authored while opted out stays rejected: re-enabling does not federate it retroactively")
	assert.Equal(t, DecisionOptedOut, code)
	_, ok := acceptanceSubjectCID(t, repos, acCommunityDID, firstURI)
	assert.False(t, ok, "the old post has no acceptance")
}
