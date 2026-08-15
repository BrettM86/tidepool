package votes

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ingest"
	"tidepool/internal/store"
)

// TASK 17e — THE RE-CAST DIVERGENCE: THE PEER HOLDS A VOTE WE DO NOT CLAIM.
//
// 17b found this and deferred it here. Re-casting a delivered vote re-upserts
// the SAME outbound_votes row back to 'pending' under a new activity id, while
// the peer goes on holding the old vote in the old direction. Transient while
// the new delivery is in flight; PERMANENT the moment it poisons — nothing
// re-drives a poisoned delivery on its own, and the reseed subtracts only
// 'delivered' rows, so the community's score keeps counting a vote we have
// stopped accounting for and will never correct.
//
// THE FIXTURE IS THE ENTIRE TEST, and it must be DRIVEN, not assembled. This is
// 17b's own blind spot by name: "the fixture nobody writes is the one where the
// row's history has more than one step." Every state below is produced by the
// real path —
//
//	consume.Dispatcher.HandleEvent (vote commit)  → outbound_votes + a Dislike
//	outbound.Worker.DeliverNext                   → delivered, ledger flipped
//	consume.Dispatcher.HandleEvent (the re-cast)  → SAME row reset to pending,
//	                                                new activity id, a Like
//	outbound.Worker.DeliverNext (failing sender)  → that Like POISONS
//
// — because a hand-inserted row would be some steady state a fixture author
// chose, and the whole condition here is a row whose history has two steps that
// disagree.
//
// AND IT MUST NOT BE READ FROM THE VOTE ROW. worker.voteCallback resolves via
// GetByActivityID and returns nil on NotFound, so a delivery already in flight
// when the re-cast lands settles into silence: its id no longer matches
// current_activity_id and the callback no-ops. The row is what the bug erases.
// outbound_activities is append-only and carries the subject in parent_at_uri
// from both vote enqueue sites, so the activity/delivery history is the only
// durable record of what the peer was actually told.

// recastWorld drives one persona's vote through a history and hands back the
// pieces the assertions need.
type recastWorld struct {
	*lifecycle
	subject string
}

func newRecastWorld(t *testing.T, maxAttempts int) *recastWorld {
	t.Helper()
	l := newLifecycle(t, maxAttempts)
	return &recastWorld{lifecycle: l, subject: l.subjectURI}
}

// deliveredVoteActivity is the activity id of the vote the peer accepted, read
// from the delivery history rather than from the vote row — the same evidence
// the sweep has to use, so the assertion cannot pass through a path the sweep
// cannot see.
func deliveredVoteActivity(t *testing.T, w *recastWorld) string {
	t.Helper()
	var id string
	require.NoError(t, w.db.QueryRow(`
		SELECT a.activity_id
		  FROM outbound_activities a
		  JOIN outbound_deliveries d ON d.activity_id = a.activity_id
		 WHERE a.kind IN ('Like', 'Dislike') AND d.state = 'delivered'
		 ORDER BY d.seq DESC
		 LIMIT 1`).Scan(&id),
		"precondition: a vote really was delivered to the peer")
	return id
}

// sweep runs the real reconciler over this world and returns its report.
func sweep(t *testing.T, w *recastWorld) ingest.DivergenceReport {
	t.Helper()
	reconciler, err := ingest.NewDivergenceReconciler(ingest.DivergenceOptions{DB: w.db})
	require.NoError(t, err)
	report, err := reconciler.Sweep(context.Background())
	require.NoError(t, err)
	return report
}

func recastEntries(report ingest.DivergenceReport) []ingest.DivergenceEntry {
	var found []ingest.DivergenceEntry
	for _, entry := range report.Entries {
		if entry.Class == ingest.DivergenceVoteRecastUndelivered {
			found = append(found, entry)
		}
	}
	return found
}

// ---------------------------------------------------------------------------
// The divergence
// ---------------------------------------------------------------------------

// TestRecastDivergence_APoisonedRecastLeavesThePeerHoldingTheOldVote is the
// cycle's contract.
func TestRecastDivergence_APoisonedRecastLeavesThePeerHoldingTheOldVote(t *testing.T) {
	w := newRecastWorld(t, 1) // one attempt, so the re-cast's delivery poisons
	ctx := context.Background()

	// --- STEP 1: a down-vote, delivered for real.
	w.castVote(t, "3lztprev00001", directionDown)
	w.deliver(t)
	require.Equal(t, []string{"Dislike"}, w.sender.kinds(),
		"precondition: the peer received a DISLIKE")
	require.Equal(t, string(store.DeliveredStateDelivered), w.state(t))
	held := deliveredVoteActivity(t, w)

	// --- STEP 2: the user changes their mind. The SAME record is rewritten, so
	//     the row resets to pending under a new activity id while the peer's
	//     copy of the old vote is untouched.
	w.sender.fail(fmt.Errorf("lemmy is unreachable"))
	w.castVote(t, "3lztprev00002", directionUp)
	require.Equal(t, string(store.DeliveredStatePending), w.state(t),
		"precondition: the re-cast reset the row — this is the step that erases our record "+
			"of what the peer holds")

	// --- STEP 3: and the new vote never lands.
	w.deliver(t)
	var poisoned int
	require.NoError(t, w.db.QueryRow(
		`SELECT COUNT(*) FROM outbound_deliveries WHERE state = 'poisoned'`).Scan(&poisoned))
	require.Equal(t, 1, poisoned,
		"precondition: the re-cast's delivery POISONED, which is what makes this permanent "+
			"rather than a moment in flight")

	// --- THEN: the store names the pair, citing what the peer is holding.
	found, err := store.NewDivergences(w.db).RecastDivergences(ctx)
	require.NoError(t, err)
	require.Len(t, found, 1,
		"exactly one divergence: the peer is counting a Dislike this bridge no longer claims. "+
			"Our vote row says 'pending' — it was reset by the re-cast — so nothing in the "+
			"ledger records that a vote of ours stands on that instance, and the reseed "+
			"subtracts only 'delivered' rows. Read from the vote row this condition is "+
			"invisible by construction; only the append-only activity history still knows")
	assert.Equal(t, tpNativeDID, found[0].ActorDID)
	assert.Equal(t, w.subject, found[0].SubjectATURI,
		"the pair (actor, subject) IS the identity of a vote — only one may be live at a time — "+
			"and the subject is what an operator checks the tally of")
	assert.Equal(t, held, found[0].DeliveredActivityID,
		"citing the DELIVERED activity: the id the peer accepted is the only handle anyone has "+
			"on what they are actually counting, and it is what a manual Undo would have to "+
			"embed. Citing the pending one would name the vote nobody received")

	// --- AND the operator surface says the same thing.
	report := sweep(t, w)
	entries := recastEntries(report)
	require.Len(t, entries, 1, "the sweep reports it under its own class")
	assert.Equal(t, w.subject, entries[0].Subject)
	assert.Contains(t, entries[0].Detail, held,
		"the entry's detail carries the delivered activity id, because a class and a subject "+
			"alone do not tell an operator what the peer is holding")
	assert.Contains(t, entries[0].Detail, tpNativeDID, "and the actor it belongs to")
	assert.Equal(t, 1, report.Counts[ingest.DivergenceVoteRecastUndelivered])
}

// ---------------------------------------------------------------------------
// The two controls — both must be silent
// ---------------------------------------------------------------------------

// TestRecastDivergence_ACleanlyDeliveredVoteIsNotADivergence is the ordinary
// case, and it is most of the table: a vote cast once and delivered once.
//
// The peer holds exactly what our state claims. If this reported, the class
// would name every vote the bridge has ever cast — the failure mode that looks
// like thoroughness, on the highest-volume table in the system.
func TestRecastDivergence_ACleanlyDeliveredVoteIsNotADivergence(t *testing.T) {
	w := newRecastWorld(t, 5)

	w.castVote(t, "3lztprev00001", directionDown)
	w.deliver(t)
	require.Equal(t, string(store.DeliveredStateDelivered), w.state(t),
		"precondition: cast once, delivered once — the state the whole system is usually in")

	found, err := store.NewDivergences(w.db).RecastDivergences(context.Background())
	require.NoError(t, err)
	assert.Empty(t, found,
		"a delivered vote nobody re-cast is not a divergence: the peer holds exactly what we "+
			"claim. Reporting it would put every vote in the system in this class, and the one "+
			"real case would be indistinguishable from the background")
	assert.Empty(t, recastEntries(sweep(t, w)))
}

// TestRecastDivergence_ADeliveredThenUndoneVoteIsNotADivergence is the case
// that separates "the peer holds something" from "the peer once held
// something".
//
// The Undo delivered, so the peer holds NOTHING — and the vote row is gone,
// deleted by the Undo's own callback. An implementation reasoning from the
// activity history alone, without weighing the Undo, sees a delivered Dislike
// with no live row and reports it forever: a permanent finding about a vote that
// was correctly withdrawn.
func TestRecastDivergence_ADeliveredThenUndoneVoteIsNotADivergence(t *testing.T) {
	w := newRecastWorld(t, 5)

	w.castVote(t, "3lztprev00001", directionDown)
	w.deliver(t)
	require.Equal(t, string(store.DeliveredStateDelivered), w.state(t))

	w.deleteVote(t, "3lztprev00002")
	w.deliver(t)
	require.Equal(t, []string{"Dislike", "Undo"}, w.sender.kinds(),
		"precondition: the withdrawal reached the peer")
	require.Equal(t, "", w.state(t),
		"precondition: and the row is gone, cleared by the Undo's delivery callback — so the "+
			"ONLY remaining evidence is the activity history this sweep reads")

	found, err := store.NewDivergences(w.db).RecastDivergences(context.Background())
	require.NoError(t, err)
	assert.Empty(t, found,
		"a vote whose Undo was DELIVERED leaves the peer holding nothing, so there is nothing "+
			"to reconcile. The delivered Dislike is still in the activity history and always "+
			"will be — the history is append-only — so a sweep that looks only for 'a delivered "+
			"vote with no live row' reports this pair forever, about a withdrawal that worked")
	assert.Empty(t, recastEntries(sweep(t, w)))
}

// ---------------------------------------------------------------------------
// 17e review — the case that makes this class usable at all
// ---------------------------------------------------------------------------

// TestRecastDivergence_ASuccessfulRecastIsNotADivergence is the ORDINARY vote
// flip, and it is most of what this table does.
//
// A flip is an in-place upsert: current_activity_id moves to the new id,
// delivered_state resets to pending, and NO Undo is enqueued — Lemmy takes a
// bare opposite vote as a replacement (17b measured this; a flip is not an Undo
// followed by a vote). So once the new vote delivers, the OLD delivered activity
// matches NEITHER exclusion: the ledger names the new id, and no Undo exists to
// pair with it.
//
// Left unpinned, every vote change the bridge has ever federated becomes a
// permanent entry, and the report grows monotonically with ordinary use. The
// detail on each would claim the peer holds a vote whose delivery never landed —
// both halves false — and an operator acting on it would retract the vote the
// user currently holds.
//
// Driven through the real path for the same reason the poisoned case is: the
// state that matters here is produced by the SECOND step of a two-step history,
// and no hand-written row has a history.
func TestRecastDivergence_ASuccessfulRecastIsNotADivergence(t *testing.T) {
	w := newRecastWorld(t, 5)

	// Down, delivered.
	w.castVote(t, "3lztprev00001", directionDown)
	w.deliver(t)
	require.Equal(t, []string{"Dislike"}, w.sender.kinds())
	require.Equal(t, string(store.DeliveredStateDelivered), w.state(t))

	// Flipped to up — and this time it LANDS.
	w.castVote(t, "3lztprev00002", directionUp)
	w.deliver(t)
	require.Equal(t, []string{"Dislike", "Like"}, w.sender.kinds(),
		"precondition: the flip really went out as a bare opposite vote — Lemmy's replacement "+
			"semantics, with no Undo between them")
	require.Equal(t, string(store.DeliveredStateDelivered), w.state(t),
		"precondition: and the ledger caught up, so our accounting names what the peer holds")

	found, err := store.NewDivergences(w.db).RecastDivergences(context.Background())
	require.NoError(t, err)
	assert.Empty(t, found,
		"a flip that DELIVERED is not a divergence — it is the system working. The old activity "+
			"stays in the append-only history forever and no Undo will ever join it, because a "+
			"flip does not produce one; only the ledger naming the NEW id says the account is "+
			"settled. Reported, this class would grow with every vote change the bridge has "+
			"ever federated, each entry claiming the peer holds a vote that never landed — "+
			"both halves false — and acting on one would retract the vote the user has right now")
	assert.Empty(t, recastEntries(sweep(t, w)))
}

// TestRecastDivergence_AnUndoOlderThanTheDeliveredVoteDoesNotSuppressIt pins the
// TIME direction of the Undo exclusion.
//
// The exclusion asks whether a delivered Undo followed this vote. Drop the
// `>=` and any Undo for the pair suppresses the finding — including one from an
// earlier incarnation of the same (actor, subject), which the append-only
// history keeps forever. The sequence below is reachable and its consequence is
// permanent: the stale Undo from the FIRST vote silently hides a real divergence
// created two votes later, and nothing else in the system reports it.
//
// The second incarnation is a NEW vote record with its own rkey, which is what a
// client that deleted a vote and voted again produces. (Re-using the first
// record's rkey does not work: the activity id derives from the at-uri and a seq
// that restarts at 1 once the row is gone, so the re-created vote reproduces the
// FIRST vote's activity id, finds the already-delivered delivery standing, and
// never goes out. Recorded for the conductor — it is outside 17e.)
func TestRecastDivergence_AnUndoOlderThanTheDeliveredVoteDoesNotSuppressIt(t *testing.T) {
	w := newRecastWorld(t, 1) // the last delivery must poison
	ctx := context.Background()

	// 1) A vote, delivered. 2) Withdrawn, and the Undo delivers — so the
	//    history now holds a delivered Undo for this pair, forever.
	w.castVote(t, "3lztprev00001", directionDown)
	w.deliver(t)
	w.deleteVote(t, "3lztprev00002")
	w.deliver(t)
	require.Equal(t, []string{"Dislike", "Undo"}, w.sender.kinds())
	require.Equal(t, "", w.state(t), "precondition: the withdrawal completed and cleared the row")

	// 3) The user votes again — a new record — and it lands. This is the vote
	//    the peer holds from here on.
	const secondRKey = "3lztemporalvot2"
	castVoteRecord(t, w, secondRKey, "3lztprev00003", directionUp)
	w.deliver(t)
	require.Equal(t, []string{"Dislike", "Undo", "Like"}, w.sender.kinds(),
		"precondition: the second vote really went out")
	require.Equal(t, string(store.DeliveredStateDelivered), voteRecordState(t, w, secondRKey))
	held := deliveredVoteActivity(t, w)

	// 4) And they change it once more — this time the delivery poisons.
	w.sender.fail(fmt.Errorf("lemmy is unreachable"))
	castVoteRecord(t, w, secondRKey, "3lztprev00004", directionDown)
	w.deliver(t)
	var poisoned int
	require.NoError(t, w.db.QueryRow(
		`SELECT COUNT(*) FROM outbound_deliveries WHERE state = 'poisoned'`).Scan(&poisoned))
	require.Equal(t, 1, poisoned, "precondition: the last delivery poisoned")

	found, err := store.NewDivergences(w.db).RecastDivergences(ctx)
	require.NoError(t, err)
	require.Len(t, found, 1,
		"the peer is holding the Like from step 3, and the Undo in this history is OLDER than "+
			"it — it withdrew a different incarnation of the same (actor, subject) pair. An "+
			"exclusion that accepted any Undo at all would let that stale row suppress this "+
			"finding forever: the history is append-only, so the old Undo never goes away, and "+
			"the divergence it hides is permanent and reported nowhere else")
	assert.Equal(t, held, found[0].DeliveredActivityID,
		"and the entry cites the vote the peer actually holds — the one delivered AFTER the "+
			"Undo, not the withdrawn one")
}

// castVoteRecord drives a vote COMMIT for an arbitrary record key, so a history
// can contain more than one vote RECORD for the same subject — which is what a
// client that deleted a vote and voted again produces.
func castVoteRecord(t *testing.T, w *recastWorld, rkey, rev, direction string) {
	t.Helper()
	frame := fmt.Sprintf(
		`{"did":%q,"time_us":9500,"kind":"commit","commit":{"rev":%q,"operation":"create",`+
			`"collection":"social.coves.feed.vote","rkey":%q,"cid":%q,`+
			`"record":{"$type":"social.coves.feed.vote","subject":{"uri":%q,"cid":%q},`+
			`"direction":%q,"createdAt":"2026-08-14T10:00:00.000Z"}}}`,
		tpNativeDID, rev, rkey, testCID, w.subject, testCID, direction)
	w.handle(t, frame)
}

// voteRecordState reads one vote record's delivered_state, or "" when the row
// is gone.
func voteRecordState(t *testing.T, w *recastWorld, rkey string) string {
	t.Helper()
	var state string
	err := w.db.QueryRow(`SELECT delivered_state FROM outbound_votes WHERE vote_at_uri = $1`,
		"at://"+tpNativeDID+"/social.coves.feed.vote/"+rkey).Scan(&state)
	if err == sql.ErrNoRows {
		return ""
	}
	require.NoError(t, err)
	return state
}
