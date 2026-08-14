package accept

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/acceptrec"
	"tidepool/internal/repo"
)

// TERMINALITY IS DECIDED ACROSS TWO OPERATIONS, AND THE GAP IS THE BUG.
//
// The edit path reads the standing removal's `code` and then acts on it:
// admission-revoked auto-restores (delete the removal, write an acceptance, run
// the outbound side effect), anything else is terminal. Between those two
// operations the removal can CHANGE — and acceptrec.Restore deletes whichever
// removal is current, not the one that was inspected.
//
// So the moderation reversal we closed at the frame above is still reachable
// through a narrower window: read admission-revoked, moderator replaces it with
// their own removal, restore deletes theirs. The window is small; the loss is
// the same, and it is silent — the firehose shows an acceptance, and the
// moderators' record is simply gone.
//
// The property is one sentence: AN EDIT MAY ONLY REVERSE THE EXACT REMOVAL IT
// INSPECTED. acceptrec already does compare-and-set with ExpectPrevCID on the
// acceptance for the same reason; these tests pin the behaviour, not that
// mechanism.

// racingRepos interposes on the two operations the decision spans. It wraps a
// real *repo.Manager, so every commit and read is the production path — only the
// interleaving is arranged.
type racingRepos struct {
	acceptrec.RepoManager

	mu sync.Mutex
	// afterRemovalRead fires once, after the Nth read of a removal record, and
	// receives the manager so it can mutate the repo. N counts from 1.
	afterRemovalRead func()
	readsBeforeFire  int
	removalReads     int
	fired            bool

	// beforeRemovalDelete fires once, immediately before a commit that deletes a
	// removal — i.e. exactly when the moderator's write would land between the
	// read and the restore.
	beforeRemovalDelete func()
	deleteFired         bool
}

func (r *racingRepos) GetRecord(ctx context.Context, did, collection, rkey string) (map[string]any, string, error) {
	record, cid, err := r.RepoManager.GetRecord(ctx, did, collection, rkey)
	if collection != acceptrec.CollectionRemoval {
		return record, cid, err
	}
	r.mu.Lock()
	r.removalReads++
	fire := r.afterRemovalRead != nil && !r.fired && r.removalReads >= r.readsBeforeFire
	if fire {
		r.fired = true
	}
	hook := r.afterRemovalRead
	r.mu.Unlock()
	if fire {
		hook()
	}
	return record, cid, err
}

func (r *racingRepos) ApplyOpsTx(ctx context.Context, did string, ops []repo.RecordOp, sideEffect repo.TxSideEffect) (*repo.CommitResult, error) {
	// WHICH removal-delete is the restore's? Not "the one without a
	// precondition" — that was true only until Restore grew one, and a fixture
	// whose discriminator the fix invalidates stops firing silently.
	//
	// The stable difference is what each precondition MEANS:
	//
	//	AcceptSubject: ExpectPrevCID = ""       — "expect NO removal to exist"
	//	Restore:       ExpectPrevCID = <a CID>  — "expect THIS removal to exist"
	//
	// An implementation cannot swap those without inverting what the operations
	// do, so this discriminator survives any correct version of the fix —
	// including the one that has no precondition at all, which is the build this
	// test has to go red against.
	deletesRemoval := false
	for _, op := range ops {
		if op.Action != repo.OpActionDelete || op.Collection != acceptrec.CollectionRemoval {
			continue
		}
		if op.ExpectPrevCID == nil || *op.ExpectPrevCID != "" {
			deletesRemoval = true
		}
	}
	r.mu.Lock()
	fire := deletesRemoval && r.beforeRemovalDelete != nil && !r.deleteFired
	if fire {
		r.deleteFired = true
	}
	hook := r.beforeRemovalDelete
	r.mu.Unlock()
	if fire {
		hook()
	}
	return r.RepoManager.ApplyOpsTx(ctx, did, ops, sideEffect)
}

// TestEditCannotDeleteARemovalItNeverInspected is the forward race, and the one
// that loses the moderator's decision.
//
// The engine reads admission-revoked — OUR removal, correctly reversible — and
// while it is deciding, a moderator removes the post for their own reasons. The
// restore then deletes the record it never read.
func TestEditCannotDeleteARemovalItNeverInspected(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	repos := newRepos(t, conn)
	seedBridgedCommunity(t, conn)

	racing := &racingRepos{RepoManager: repos}
	enqueuer := realEnqueuer(t, conn)
	engine := wireEngine(t, conn, racing, enqueuer)
	dispatcher := wireDispatcher(t, conn, engine, enqueuer)

	// A post is accepted, then an edit fails admission: OUR removal stands.
	admittedCreate(t, dispatcher)
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("update", acPostRKey, acRevUpdate, acPostCID2, acPostTimeUS+1,
			pv2Record(func(r map[string]any) { r["title"] = "" }))))
	removal, ok := removalStandsAt(t, repos, acCommunityDID, acPostURI)
	require.True(t, ok, "precondition: an admission-revoked removal stands")
	require.Equal(t, RemovalCodeAdmissionRevoked, removal["code"])

	// THE RACE: a moderator replaces it with their own removal in the window
	// between the engine reading the code and the restore committing.
	racing.beforeRemovalDelete = func() {
		_, err := acceptrec.Remove(ctx, repos, acCommunityDID, acPostURI, acPostCID,
			"moderator-discretion", "removed by a human", time.Now(), nil)
		require.NoError(t, err, "the moderator's removal lands mid-decision")
	}

	activitiesBefore := activityKindCount(t, conn, "Update")
	createsBefore := activityKindCount(t, conn, "Create")

	// The corrective edit: admission passes now.
	editErr := dispatcher.HandleEvent(ctx,
		postEvent("update", acPostRKey, acRevDelete, acPostCID2, acPostTimeUS+2, pv2Record()))
	require.True(t, racing.deleteFired, "the fixture must have raced the commit")
	// Logged, not asserted: the reversal below happens with the edit reporting
	// SUCCESS, which is why nothing upstream notices.
	t.Logf("the edit reported: %v", editErr)

	surviving, ok := removalStandsAt(t, repos, acCommunityDID, acPostURI)
	assert.True(t, ok,
		"the MODERATOR's removal must survive: the edit inspected a different record, and "+
			"deleting whichever removal happens to be current is the reversal we already "+
			"closed once — reachable again through a smaller window")
	if ok {
		assert.Equal(t, "moderator-discretion", surviving["code"],
			"and it must still be theirs, not replaced by our restore's idea of the state")
	}

	_, accepted := acceptanceSubjectCID(t, repos, acCommunityDID, acPostURI)
	assert.False(t, accepted,
		"no acceptance may be written against a removal we never read: on the firehose that "+
			"is the post coming back, published by the community that removed it")

	assert.Equal(t, activitiesBefore, activityKindCount(t, conn, "Update"),
		"and nothing may be enqueued — the side effect rides the restore commit, so a "+
			"restore that should not have happened pushes the post back at the community")
	assert.Equal(t, createsBefore, activityKindCount(t, conn, "Create"))
}

// TestVanishedRemovalIsNotRecordedAsTerminal is the reverse race.
//
// The engine reads a moderator-discretion removal and decides the edit is
// terminal — but the moderators lift the removal before that decision is
// recorded. The ledger then carries a moderator-removed row for a post that is
// not removed, which is the surface an operator reads when the author asks why
// their edit did nothing.
//
// The edit must stay recoverable: whatever the engine does with the decision, a
// redrive must admit the post, because by then nothing stands against it.
func TestVanishedRemovalIsNotRecordedAsTerminal(t *testing.T) {
	conn := acceptanceDB(t)
	ctx := context.Background()
	repos := newRepos(t, conn)
	seedBridgedCommunity(t, conn)

	racing := &racingRepos{RepoManager: repos}
	enqueuer := realEnqueuer(t, conn)
	engine := wireEngine(t, conn, racing, enqueuer)
	dispatcher := wireDispatcher(t, conn, engine, enqueuer)

	admittedCreate(t, dispatcher)
	_, err := acceptrec.Remove(ctx, repos, acCommunityDID, acPostURI, acPostCID,
		"moderator-discretion", "removed by a human", time.Now(), nil)
	require.NoError(t, err, "precondition: a moderator removal stands")

	// THE RACE: the moderators restore the post after the engine has read their
	// removal and before it records the decision. The second removal read is the
	// engine's own (the first belongs to acceptrec's removal guard).
	racing.readsBeforeFire = 2
	racing.afterRemovalRead = func() {
		_, rerr := repos.ApplyOps(ctx, acCommunityDID, []repo.RecordOp{
			{Action: repo.OpActionDelete, Collection: acceptrec.CollectionRemoval,
				RKey: acceptrec.SubjectRKey(acPostURI)},
		})
		require.NoError(t, rerr, "the moderators lift the removal mid-decision")
	}

	_ = dispatcher.HandleEvent(ctx,
		postEvent("update", acPostRKey, acRevUpdate, acPostCID2, acPostTimeUS+1, pv2Record()))
	require.True(t, racing.fired, "the fixture must have raced the decision")

	status, code := admissionOf(t, conn, acCommunityDID, acPostURI)
	assert.NotEqual(t, StatusRemoved, status,
		"the ledger must not record a terminal moderator removal that no longer stands: it "+
			"is what an operator reads to answer 'why did my edit do nothing', and here the "+
			"answer would be a removal nobody can find (code=%q)", code)

	// And the edit is recoverable: a redrive finds no removal and admits it.
	require.NoError(t, dispatcher.HandleEvent(ctx,
		postEvent("update", acPostRKey, acRevDelete, acPostCID2, acPostTimeUS+2, pv2Record())))
	cid, accepted := acceptanceSubjectCID(t, repos, acCommunityDID, acPostURI)
	assert.True(t, accepted,
		"a redrive after the removal was lifted must admit the edit — otherwise a post the "+
			"moderators reinstated stays invisible until its author edits again")
	assert.Equal(t, acPostCID2, cid, "pinning the version the redrive evaluated")
}
