package repo

import (
	"context"
	"database/sql"
	stderrors "errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

// ApplyOpsTx generalizes the single-op PutRecordTx side-effect seam to the
// multi-op commit: acceptance transitions (create-accept, update-repin,
// edit-fail, author-delete) each ride ONE ApplyOpsTx whose side effect is the
// outbound enqueue, so the community-repo commit and the enqueue land together
// or not at all. These pins clone TestPutRecordTxSideEffectAtomicity for the
// batch path — including the subtle trap that the side effect must ALSO run on
// the all-inert NoOp branch (the at-least-once redelivery enqueue), where no
// repo commit happens but the side effect still has to be made durable.

// applyOpsTxMarker is a scratch table a side effect writes into so a test can
// prove the side effect's OWN write persisted (or rolled back) independently of
// the record ops. It survives across the test binary; each test truncates it.
func applyOpsTxMarker(t *testing.T, database *sql.DB) {
	t.Helper()
	_, err := database.Exec(`CREATE TABLE IF NOT EXISTS applyops_tx_marker (note TEXT NOT NULL)`)
	require.NoError(t, err)
	_, err = database.Exec(`TRUNCATE applyops_tx_marker`)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = database.Exec(`DROP TABLE IF EXISTS applyops_tx_marker`) })
}

func markerCount(t *testing.T, database *sql.DB) int {
	t.Helper()
	var n int
	require.NoError(t, database.QueryRow(`SELECT COUNT(*) FROM applyops_tx_marker`).Scan(&n))
	return n
}

// writeMarker returns a side effect that inserts one marker row, then returns
// the given error (nil to succeed).
func writeMarker(ctx context.Context, note string, ret error) TxSideEffect {
	return func(_ context.Context, tx *sql.Tx, _ *CommitResult) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO applyops_tx_marker (note) VALUES ($1)`, note); err != nil {
			return err
		}
		return ret
	}
}

// TestApplyOpsTx_SideEffectErrorRollsBackBoth: a side effect that errors inside
// ApplyOpsTx rolls back the record ops AND anything the side effect itself
// wrote. Nothing persists: the record the batch would have deleted still
// exists, the record it would have written is absent, no firehose event, and
// the side effect's marker row is gone.
func TestApplyOpsTx_SideEffectErrorRollsBackBoth(t *testing.T) {
	manager, database, _ := testManager(t)
	applyOpsTxMarker(t, database)
	ctx := context.Background()

	seeded, err := manager.PutRecord(ctx, testDID, testOtherCollection, testRKey(1), testRecord("accepted"))
	require.NoError(t, err)
	headBefore, revBefore, err := manager.Head(ctx, testDID)
	require.NoError(t, err)

	sentinel := stderrors.New("side effect refused")
	_, err = manager.ApplyOpsTx(ctx, testDID, []RecordOp{
		{Action: OpActionDelete, Collection: testOtherCollection, RKey: testRKey(1)},
		{Action: OpActionCreate, Collection: testCollection, RKey: testRKey(2), Record: testRecord("removed")},
	}, writeMarker(ctx, "phantom", sentinel))
	require.ErrorIs(t, err, sentinel,
		"a side effect erroring inside ApplyOpsTx must surface, not be swallowed")

	_, _, err = manager.GetRecord(ctx, testDID, testOtherCollection, testRKey(1))
	assert.NoError(t, err, "the deleted record must be rolled back with the failed side effect")
	_, _, err = manager.GetRecord(ctx, testDID, testCollection, testRKey(2))
	assert.True(t, errors.IsNotFound(err), "the written record must not survive a failed side effect")

	head, rev, err := manager.Head(ctx, testDID)
	require.NoError(t, err)
	assert.Equal(t, headBefore, head, "a rolled-back batch must not advance the head")
	assert.Equal(t, revBefore, rev)
	assert.Empty(t, eventsSince(t, manager, testDID, seeded.Seq),
		"no firehose event may follow the seed commit when the side effect failed")
	assert.Zero(t, markerCount(t, database),
		"the side effect's OWN write must roll back too: record and bookkeeping are one unit")
}

// TestApplyOpsTx_AllInertStillRunsSideEffectAndCommits is the trap. A batch
// where every op is inert — a delete of a missing record plus a byte-identical
// re-put — produces NO new commit and NO firehose event (the NoOp branch). But
// the side effect STILL runs and its write STILL commits, because that side
// effect is the at-least-once outbound enqueue a redelivery must re-fire even
// though the acceptance record did not change.
func TestApplyOpsTx_AllInertStillRunsSideEffectAndCommits(t *testing.T) {
	manager, database, _ := testManager(t)
	applyOpsTxMarker(t, database)
	ctx := context.Background()

	seeded, err := manager.PutRecord(ctx, testDID, testCollection, testRKey(1), testRecord("same"))
	require.NoError(t, err)
	headBefore, revBefore, err := manager.Head(ctx, testDID)
	require.NoError(t, err)
	eventsBefore := eventCount(t, manager, testDID)

	res, err := manager.ApplyOpsTx(ctx, testDID, []RecordOp{
		// Byte-identical re-put: keeps the batch a WRITE (so it is NOT the
		// genesis-delete-only branch) while changing nothing.
		{Action: OpActionUpdate, Collection: testCollection, RKey: testRKey(1), Record: testRecord("same")},
		// Delete of a record that is not there: inert.
		{Action: OpActionDelete, Collection: testOtherCollection, RKey: testRKey(2)},
	}, writeMarker(ctx, "redelivery-enqueue", nil))
	require.NoError(t, err)
	require.NotNil(t, res)

	assert.True(t, res.NoOp, "an all-inert batch reports NoOp")
	assert.Zero(t, res.Seq, "a NoOp batch emits no firehose event, so it has no seq")
	assert.Equal(t, seeded.CommitCID, res.CommitCID, "the head is unchanged")

	head, rev, err := manager.Head(ctx, testDID)
	require.NoError(t, err)
	assert.Equal(t, headBefore, head, "an all-inert batch must not advance the head")
	assert.Equal(t, revBefore, rev)
	assert.Equal(t, eventsBefore, eventCount(t, manager, testDID),
		"an all-inert batch must not emit a firehose event")

	assert.Equal(t, 1, markerCount(t, database),
		"the side effect must STILL run on the NoOp branch and its write must be durable: "+
			"the outbound enqueue is at-least-once, so a redelivery whose acceptance record is "+
			"byte-identical must re-fire the enqueue even though no commit happened")
}

// TestApplyOpsTx_SuccessRunsSideEffectInSameCommit: a batch that really changes
// records runs the side effect in the same transaction — the side effect
// observes the commit result, its write lands, and the record ops all land.
func TestApplyOpsTx_SuccessRunsSideEffectInSameCommit(t *testing.T) {
	manager, database, _ := testManager(t)
	applyOpsTxMarker(t, database)
	ctx := context.Background()

	seeded, err := manager.PutRecord(ctx, testDID, testOtherCollection, testRKey(1), testRecord("accepted"))
	require.NoError(t, err)

	var hookRes *CommitResult
	res, err := manager.ApplyOpsTx(ctx, testDID, []RecordOp{
		{Action: OpActionDelete, Collection: testOtherCollection, RKey: testRKey(1)},
		{Action: OpActionCreate, Collection: testCollection, RKey: testRKey(2), Record: testRecord("removed")},
	}, func(sctx context.Context, tx *sql.Tx, r *CommitResult) error {
		hookRes = r
		// The firehose row for this commit must already be visible in the tx.
		var n int
		if err := tx.QueryRowContext(sctx,
			`SELECT COUNT(*) FROM firehose_events WHERE seq = $1`, r.Seq).Scan(&n); err != nil {
			return err
		}
		if n != 1 {
			return stderrors.New("firehose event not visible to side effect")
		}
		_, err := tx.ExecContext(sctx, `INSERT INTO applyops_tx_marker (note) VALUES ('committed')`)
		return err
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, hookRes, "the side effect must run on the committed branch")

	assert.False(t, res.NoOp, "a batch that changes records is a real commit")
	assert.Equal(t, res.Seq, hookRes.Seq, "the side effect observes this commit's result")
	assert.Greater(t, res.Seq, seeded.Seq, "a new firehose event was emitted")

	assert.Equal(t, 1, markerCount(t, database), "the side effect's write commits with the record ops")
	_, _, err = manager.GetRecord(ctx, testDID, testOtherCollection, testRKey(1))
	assert.True(t, errors.IsNotFound(err), "the deleted record is gone")
	_, _, err = manager.GetRecord(ctx, testDID, testCollection, testRKey(2))
	assert.NoError(t, err, "the written record exists")
}

// TestApplyOpsTx_GenesisDeleteRunsSideEffect: an all-delete batch against a
// community that has NO repo yet (state == nil, KeyUseDelete) currently returns
// early NoOp WITHOUT running the side effect. Per the at-least-once side-effect
// contract (the enqueue must fire even when the repo commit is inert), the side
// effect MUST still run and commit — a DeleteAcceptance-shaped retraction whose
// community repo was never created must still enqueue its Delete{Page}.
//
// RULING (flagged): the side effect runs here too, for the SAME reason the
// len(emitted)==0 branch runs it. If the coordinator rules this an intentional
// no-side-effect exit instead, this pin is the place to invert.
func TestApplyOpsTx_GenesisDeleteRunsSideEffect(t *testing.T) {
	manager, database, _ := testManager(t)
	applyOpsTxMarker(t, database)
	ctx := context.Background()

	// testDID has no repo_state row (no PutRecord ran), so state == nil and the
	// all-delete batch takes the genesis-delete branch.
	res, err := manager.ApplyOpsTx(ctx, testDID, []RecordOp{
		{Action: OpActionDelete, Collection: testCollection, RKey: testRKey(1)},
		{Action: OpActionDelete, Collection: testOtherCollection, RKey: testRKey(2)},
	}, writeMarker(ctx, "genesis-delete-enqueue", nil))
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.True(t, res.NoOp, "no records exist, so the batch commits no records")

	assert.Equal(t, 1, markerCount(t, database),
		"the side effect must run on the genesis-delete branch too: the at-least-once outbound "+
			"enqueue has to fire even when the community repo was never created")
}
