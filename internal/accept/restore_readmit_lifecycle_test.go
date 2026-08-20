package accept

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// THREE HOLES IN THE RESTORE / READMIT / DELETE LIFECYCLE.
//
// The engine reads TWO lifecycle facts off ONE flag — outbound_objects
// .tombstoned_at: "does Lemmy hold a live copy" (Create vs Update) and "was this
// post ever accepted" (a failing edit is a REMOVAL, not a fresh rejection). The
// upsert deliberately never clears that flag, so a post that comes BACK through
// the engine's own auto-restore is live outward while its row still says it is
// dead — and both derived facts are then wrong for the rest of the post's life.
//
// The other two are doors: an author-delete that walks past a never-accepted
// post's ledger row (leaving a snapshot a later readmit signs a fresh acceptance
// from, for a record that no longer exists), and Readmit re-deciding a banned
// author's accepted post WITHOUT the carve-out AdmitPost has — withdrawing what
// the moderators deliberately kept and enqueueing a Delete{Page} at the very
// community that banned them.

// outboundRow reads the post's outbound state (tombstone included).
func outboundRow(t *testing.T, conn *sql.DB, atURI string) *store.OutboundObject {
	t.Helper()
	stored, err := store.NewOutboundObjects(conn).GetByATURI(context.Background(), atURI)
	require.NoError(t, err, "the post must have outbound state")
	return stored
}

// ---------------------------------------------------------------------------
// 1 — the auto-restore must clear the outbound tombstone
// ---------------------------------------------------------------------------

func TestAutoRestoreClearsTheOutboundTombstone(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	seedBridgedCommunity(t, conn)
	repos := newRepos(t, conn)
	enq := realEnqueuer(t, conn)
	dispatcher := wireDispatcher(t, conn, engineWith(t, conn, repos, enq), enq)

	// accepted → edited titleless → REMOVED (the outbound row is tombstoned and
	// Lemmy got a Delete{Page}).
	admittedCreate(t, dispatcher)
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("update", acPostRKey, acRevUpdate, acPostCID2, acPostTimeUS+1,
			pv2Record(func(r map[string]any) { delete(r, "title") }))))
	require.True(t, outboundRow(t, conn, acPostURI).IsTombstoned(),
		"precondition: the removal tombstoned the outbound row")

	// The corrective edit auto-restores: the removal is deleted, a fresh
	// acceptance written, and a Create{Page} enqueued because Lemmy's copy is gone.
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("update", acPostRKey, "3lzpostrev004", acPostCID, acPostTimeUS+2, pv2Record())))
	_, ok := acceptanceSubjectCID(t, repos, acCommunityDID, acPostURI)
	require.True(t, ok, "precondition: the restore wrote a fresh acceptance")
	require.Equal(t, 2, activityKindCount(t, conn, "Create"),
		"precondition: the restore re-added the post to Lemmy with a Create{Page}")

	assert.False(t, outboundRow(t, conn, acPostURI).IsTombstoned(),
		"a restore RE-PUBLISHES the post: the acceptance stands and Lemmy holds a live copy "+
			"again, so the outbound row must not keep saying the post is dead — the engine reads "+
			"live-ness off exactly this flag")

	// The proof that live-ness is what the flag means: the NEXT ordinary edit
	// must federate as Update{Page}, because Lemmy has the restored copy. A row
	// still marked tombstoned makes it a second Create for an object the peer
	// already holds.
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("update", acPostRKey, "3lzpostrev005", acPostCID2, acPostTimeUS+3, pv2Record())))

	assert.Equal(t, 1, activityKindCount(t, conn, "Update"),
		"the edit after a restore is an Update{Page}: Lemmy holds the copy the restore re-created")
	assert.Equal(t, 2, activityKindCount(t, conn, "Create"),
		"and it must NOT mint a third Create for an object the peer already has")
}

// The other half of the same flag: "was this post ever accepted" decides whether
// a now-failing edit is a REMOVAL (withdraw the acceptance, Delete{Page}) or a
// plain rejection that writes nothing outward. After a restore the post IS
// accepted, so a failing edit must remove it — otherwise the ledger says
// 'rejected' while the restore's acceptance and Lemmy's copy stay live, which is
// exactly the divergence the removal path exists to prevent.
func TestAFailingEditAfterARestoreIsARemovalNotAPlainRejection(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	seedBridgedCommunity(t, conn)
	repos := newRepos(t, conn)
	enq := realEnqueuer(t, conn)
	dispatcher := wireDispatcher(t, conn, engineWith(t, conn, repos, enq), enq)

	admittedCreate(t, dispatcher)
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("update", acPostRKey, acRevUpdate, acPostCID2, acPostTimeUS+1,
			pv2Record(func(r map[string]any) { delete(r, "title") }))))
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("update", acPostRKey, "3lzpostrev004", acPostCID, acPostTimeUS+2, pv2Record())))
	_, ok := acceptanceSubjectCID(t, repos, acCommunityDID, acPostURI)
	require.True(t, ok, "precondition: the post was restored and stands accepted")
	require.Equal(t, 1, activityKindCount(t, conn, "Delete"),
		"precondition: exactly one Delete so far (the original removal)")

	// The author breaks it again.
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("update", acPostRKey, "3lzpostrev005", acPostCID2, acPostTimeUS+3,
			pv2Record(func(r map[string]any) { delete(r, "title") }))))

	status, code := admissionOf(t, conn, acCommunityDID, acPostURI)
	assert.Equal(t, StatusRemoved, status,
		"the post was LIVE (restored), so an edit that fails re-admission REMOVES it — recording "+
			"a plain rejection would leave the restore's acceptance standing and Lemmy still showing it")
	assert.Equal(t, DecisionTitleRequired, code)

	_, stillAccepted := acceptanceSubjectCID(t, repos, acCommunityDID, acPostURI)
	assert.False(t, stillAccepted, "the acceptance the restore wrote must be withdrawn")
	_, removed := removalStandsAt(t, repos, acCommunityDID, acPostURI)
	assert.True(t, removed, "and a removal must stand in its place")
	assert.Equal(t, 2, activityKindCount(t, conn, "Delete"),
		"Lemmy must be told to drop the copy the restore gave it")
	assert.True(t, outboundRow(t, conn, acPostURI).IsTombstoned(),
		"and the outbound row is tombstoned again")
}

// ---------------------------------------------------------------------------
// 2 — an author-delete of a NEVER-ACCEPTED post must take its ledger row with it
// ---------------------------------------------------------------------------

// A rejected post has NO outbound_objects row — that is its normal state, not an
// edge case — so an author-delete that early-returns on the missing row leaves
// the admissions row AND its evaluated_snapshot behind. /admin/admissions/readmit
// then re-runs the decision from that snapshot and signs a fresh acceptance whose
// strongRef points at a record the author deleted: the community endorsing, and
// the bridge federating, content that no longer exists.
func TestAuthorDeleteOfANeverAcceptedPostDropsItsLedgerRow(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	seedBridgedCommunity(t, conn)
	repos := newRepos(t, conn)
	enq := realEnqueuer(t, conn)
	engine := engineWith(t, conn, repos, enq)
	dispatcher := wireDispatcher(t, conn, engine, enq)

	// Opted out → REJECTED. A rejection writes a ledger row (with the snapshot a
	// readmit re-decides from) and no outbound state at all.
	_, err := store.NewFederationPrefs(conn).Upsert(ctx, store.FederationPref{
		DID: acAuthorDID, Source: store.FederationPrefSourceRecord,
	})
	require.NoError(t, err)
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("create", acPostRKey, acRevCreate, acPostCID, acPostTimeUS, pv2Record())))
	status, _ := admissionOf(t, conn, acCommunityDID, acPostURI)
	require.Equal(t, StatusRejected, status, "precondition: rejected, so no outbound row exists")
	require.Zero(t, countRows(t, conn, "outbound_objects"), "precondition: no outbound state")

	// The author deletes the postv2. The record is gone from their repo.
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("delete", acPostRKey, acRevDelete, "", acPostTimeUS+1, nil)))

	assert.Zero(t, admissionRowsForPost(t, conn, acPostURI),
		"the deleted post's ledger row must go too: its evaluated_snapshot is a readmit's input, "+
			"and a snapshot that outlives the record is a re-admission of content the author erased")

	// The rejection's CAUSE now clears — the author re-enables federation. This is
	// the ordinary shape of a readmit: an operator re-runs a decision whose reason
	// went away. The record it would re-admit, however, no longer exists.
	_, err = store.NewFederationPrefs(conn).Upsert(ctx, store.FederationPref{
		DID: acAuthorDID, Enabled: true, Source: store.FederationPrefSourceRecord,
	})
	require.NoError(t, err)

	// The door the surviving snapshot opens: a readmit must not be able to sign
	// an acceptance for a record that is gone.
	result, rerr := engine.Readmit(ctx, acPostURI)
	if rerr == nil {
		assert.False(t, result.Enqueued,
			"a readmit of a deleted post must not enqueue anything")
		assert.NotEqual(t, StatusAccepted, result.Status,
			"and it must not report the deleted post as accepted")
	} else {
		assert.True(t, errors.IsNotFound(rerr),
			"a readmit of a post the engine no longer holds a decision for is a 404, got %v", rerr)
	}
	_, signed := acceptanceSubjectCID(t, repos, acCommunityDID, acPostURI)
	assert.False(t, signed,
		"NO acceptance may be signed for a postv2 the author deleted — the strongRef would pin "+
			"a record that no longer exists and federate content that was erased")
	assert.Zero(t, countRows(t, conn, "outbound_activities"),
		"and nothing may be enqueued for it")
}

// ---------------------------------------------------------------------------
// 3 — Readmit needs AdmitPost's author-banned carve-out
// ---------------------------------------------------------------------------

// An admin readmit of an ACCEPTED post whose author is banned re-decides
// author-banned with a prior acceptance standing. AdmitPost carves that case out
// deliberately — a ban is author-state, not a judgement of the post, and the
// moderators who banned without removeData chose to leave it up. Readmit
// duplicates the transition WITHOUT the carve-out: it withdraws the acceptance,
// writes a removal, and enqueues a Delete{Page} at the community that banned the
// author, through the admin door.
func TestReadmitOfABannedAuthorsAcceptedPostRefusesWithoutRemovingIt(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	seedBridgedCommunity(t, conn)
	repos := newRepos(t, conn)
	enq := realEnqueuer(t, conn)
	engine := engineWith(t, conn, repos, enq)
	dispatcher := wireDispatcher(t, conn, engine, enq)

	admittedCreate(t, dispatcher)
	before := admissionRow(t, conn, acCommunityDID, acPostURI)
	require.Equal(t, StatusAccepted, before.Status, "precondition: the post is accepted")
	require.NotEmpty(t, before.AcceptedCID, "precondition: the acceptance pins a CID")

	banAuthor(t, conn, "banned without removeData: this post stays up")

	result, err := engine.Readmit(ctx, acPostURI)
	require.NoError(t, err,
		"a readmit against a standing ban is a DECIDED outcome the operator reads, not a 500")

	assert.False(t, result.Enqueued, "nothing is enqueued: the post is already where it belongs")
	assert.Equal(t, StatusAccepted, result.Status,
		"the result reports the acceptance that still STANDS — reporting 'removed' would describe "+
			"a withdrawal this call must not perform")
	assert.Equal(t, DecisionAuthorBanned, result.DecisionCode,
		"with the refusal's cause, so the operator sees WHY the readmit changed nothing")

	// Nothing outward moved.
	cid, ok := acceptanceSubjectCID(t, repos, acCommunityDID, acPostURI)
	require.True(t, ok, "the acceptance the moderators chose to leave up must still stand")
	assert.Equal(t, acPostCID, cid, "still pinning the version that was accepted")
	_, removed := removalStandsAt(t, repos, acCommunityDID, acPostURI)
	assert.False(t, removed,
		"no removal record: an admin readmit must not invent a removal the moderators never made")
	assert.Zero(t, activityKindCount(t, conn, "Delete"),
		"and NO Delete{Page} at the community that banned the author — the outbound echo every "+
			"moderation path exists to avoid, reached through the admin door")
	assert.False(t, outboundRow(t, conn, acPostURI).IsTombstoned(),
		"the outbound row stays live: Lemmy still holds the copy the ban deliberately left alone")

	// And the ledger keeps the post in the removeData purge set while recording why.
	assertStillPurgeable(t, conn, before)
}
