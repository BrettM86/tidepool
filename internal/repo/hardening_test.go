package repo

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

// Task 11 hardening tests: account events on the durable log, the
// transactional PutRecord side effect, blob deletion, and batched pruning.

func TestAppendAccountEvent(t *testing.T) {
	manager, _, _ := testManager(t)
	ctx := t.Context()

	// A commit first, so ordering (account AFTER the scrub commits) is
	// observable.
	commit, err := manager.PutRecord(ctx, testDID, testCollection, testRKey(1), testRecord("doomed"))
	require.NoError(t, err)

	seq, err := manager.AppendAccountEvent(ctx, testDID, false, AccountStatusDeleted)
	require.NoError(t, err)
	assert.Greater(t, seq, commit.Seq, "account event sequenced after the commit")

	events, err := manager.ListEvents(ctx, 0, 10)
	require.NoError(t, err)
	require.Len(t, events, 2)

	assert.Equal(t, EventKindCommit, events[0].Kind)
	assert.Equal(t, commit.Seq, events[0].Seq)

	account := events[1]
	assert.Equal(t, EventKindAccount, account.Kind)
	assert.Equal(t, seq, account.Seq)
	assert.Equal(t, testDID, account.DID)
	assert.False(t, account.AccountActive)
	assert.Equal(t, AccountStatusDeleted, account.AccountStatus)
	assert.Empty(t, account.CommitCID, "account rows carry no commit payload")
	assert.Empty(t, account.CAR)
	assert.Empty(t, account.Ops)

	// SeqBounds counts account events like any other retained event.
	oldest, newest, err := manager.SeqBounds(ctx)
	require.NoError(t, err)
	assert.Equal(t, commit.Seq, oldest)
	assert.Equal(t, seq, newest)

	// Malformed DID fails closed.
	_, err = manager.AppendAccountEvent(ctx, "not-a-did", false, AccountStatusDeleted)
	require.Error(t, err)
	assert.True(t, errors.IsValidation(err))
}

// TestFirehoseEventKindShapeCheck pins the kind-shape CHECK (migration 011)
// at the DB level with raw inserts: an account row must carry account_active,
// a commit row must not carry account fields, and an ACTIVE account frame
// must not carry a status (status is inactive-only in the model/emitter).
func TestFirehoseEventKindShapeCheck(t *testing.T) {
	_, database, _ := testManager(t)

	// (a) kind='account' with NULL account_active is rejected.
	_, err := database.Exec(`
		INSERT INTO firehose_events (kind, did, account_active, account_status, created_at)
		VALUES ('account', $1, NULL, NULL, NOW())`, testDID)
	require.Error(t, err, "account row must carry a non-NULL account_active")

	// (b) kind='commit' carrying account fields is rejected (even with an
	// otherwise well-formed commit payload).
	_, err = database.Exec(`
		INSERT INTO firehose_events (kind, did, commit_cid, rev, ops, car, account_active, created_at)
		VALUES ('commit', $1, 'bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqrsqxi3jgxte',
		        'rev1', '[]'::jsonb, ''::bytea, false, NOW())`, testDID)
	require.Error(t, err, "commit row must not carry account fields")

	// (c) kind='account' active=true with a non-NULL status is rejected.
	_, err = database.Exec(`
		INSERT INTO firehose_events (kind, did, account_active, account_status, created_at)
		VALUES ('account', $1, true, 'deleted', NOW())`, testDID)
	require.Error(t, err, "an active account frame must not carry a status")

	// Control: the well-formed shapes the emitter actually writes are
	// accepted — an inactive+deleted frame and an active+NULL-status frame.
	_, err = database.Exec(`
		INSERT INTO firehose_events (kind, did, account_active, account_status, created_at)
		VALUES ('account', $1, false, 'deleted', NOW())`, testDID)
	require.NoError(t, err, "inactive account frame with a status is valid")
	_, err = database.Exec(`
		INSERT INTO firehose_events (kind, did, account_active, account_status, created_at)
		VALUES ('account', $1, true, NULL, NOW())`, testDID)
	require.NoError(t, err, "active account frame with NULL status is valid")
}

// TestPutRecordTxSideEffectAtomicity: the side effect and the record write
// land together or not at all, and the hook also runs on the NoOp re-put.
func TestPutRecordTxSideEffectAtomicity(t *testing.T) {
	manager, database, _ := testManager(t)
	ctx := t.Context()
	rkey := testRKey(2)

	// A failing side effect rolls the record write back — no record, no
	// repo, no firehose event.
	sentinel := stderrors.New("side effect refused")
	_, err := manager.PutRecordTx(ctx, testDID, testCollection, rkey, testRecord("phantom"),
		func(context.Context, *sql.Tx, *CommitResult) error { return sentinel })
	require.ErrorIs(t, err, sentinel)
	_, _, err = manager.GetRecord(ctx, testDID, testCollection, rkey)
	assert.True(t, errors.IsNotFound(err), "record must not survive a failed side effect")
	var eventCount int
	require.NoError(t, database.QueryRow(`SELECT COUNT(*) FROM firehose_events`).Scan(&eventCount))
	assert.Zero(t, eventCount, "no firehose event for a rolled-back commit")

	// A successful side effect observes the commit result and can write in
	// the same transaction.
	var hookRes *CommitResult
	res, err := manager.PutRecordTx(ctx, testDID, testCollection, rkey, testRecord("real"),
		func(_ context.Context, tx *sql.Tx, r *CommitResult) error {
			hookRes = r
			// Prove tx is live and the firehose row is already visible in it.
			var n int
			return tx.QueryRowContext(ctx,
				`SELECT COUNT(*) FROM firehose_events WHERE seq = $1`, r.Seq).Scan(&n)
		})
	require.NoError(t, err)
	require.NotNil(t, hookRes)
	assert.Equal(t, res.RecordCID, hookRes.RecordCID)
	assert.False(t, hookRes.NoOp)

	// The idempotent re-put still runs the hook (bookkeeping refresh), with
	// NoOp set and no new firehose event.
	hookRes = nil
	res2, err := manager.PutRecordTx(ctx, testDID, testCollection, rkey, testRecord("real"),
		func(_ context.Context, _ *sql.Tx, r *CommitResult) error {
			hookRes = r
			return nil
		})
	require.NoError(t, err)
	require.NotNil(t, hookRes, "the side effect must run on the NoOp path too")
	assert.True(t, hookRes.NoOp)
	assert.Equal(t, res.RecordCID, res2.RecordCID)
	require.NoError(t, database.QueryRow(`SELECT COUNT(*) FROM firehose_events`).Scan(&eventCount))
	assert.Equal(t, 1, eventCount, "a NoOp re-put emits no new event")
}

func TestDeleteBlobAndDeleteBlobsForDID(t *testing.T) {
	manager, _, _ := testManager(t)
	ctx := t.Context()

	blob1, err := manager.PutBlob(ctx, testDID, "image/png", []byte("png-bytes-1"))
	require.NoError(t, err)
	blob2, err := manager.PutBlob(ctx, testDID, "image/png", []byte("png-bytes-2"))
	require.NoError(t, err)
	const otherDID = "did:plc:yk4dd2qkboz2yv6tpubpc6co"
	other, err := manager.PutBlob(ctx, otherDID, "image/png", []byte("png-bytes-3"))
	require.NoError(t, err)

	cid1 := blob1.Ref.String()
	require.NoError(t, manager.DeleteBlob(ctx, testDID, cid1))
	_, _, err = manager.GetBlob(ctx, testDID, cid1)
	assert.True(t, errors.IsNotFound(err))
	// Idempotent: deleting again is a no-op success.
	require.NoError(t, manager.DeleteBlob(ctx, testDID, cid1))

	n, err := manager.DeleteBlobsForDID(ctx, testDID)
	require.NoError(t, err)
	assert.Equal(t, int64(1), n, "only the remaining blob under the DID")
	_, _, err = manager.GetBlob(ctx, testDID, blob2.Ref.String())
	assert.True(t, errors.IsNotFound(err))

	// The other DID's blob is untouched.
	data, _, err := manager.GetBlob(ctx, otherDID, other.Ref.String())
	require.NoError(t, err)
	assert.Equal(t, []byte("png-bytes-3"), data)
}

// TestPruneEventsBatched drives PruneEvents across multiple DELETE batches
// (rows inserted raw — committing 2500 real records would dominate the
// suite's runtime) and checks the retained suffix stays contiguous.
func TestPruneEventsBatched(t *testing.T) {
	manager, database, _ := testManager(t)
	ctx := t.Context()

	const expired = 2500 // > 2 × pruneEventsBatchSize
	_, err := database.Exec(fmt.Sprintf(`
		INSERT INTO firehose_events (did, commit_cid, rev, ops, car, created_at)
		SELECT '%s', 'bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqrsqxi3jgxte',
		       'rev' || n, '[]'::jsonb, ''::bytea, NOW() - INTERVAL '2 days'
		FROM generate_series(1, %d) AS n`, testDID, expired))
	require.NoError(t, err)
	// One fresh commit that must survive.
	fresh, err := manager.PutRecord(ctx, testDID, testCollection, testRKey(3), testRecord("fresh"))
	require.NoError(t, err)

	n, err := manager.PruneEvents(ctx, time.Now().Add(-24*time.Hour))
	require.NoError(t, err)
	assert.Equal(t, int64(expired), n)

	events, err := manager.ListEvents(ctx, 0, 10)
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, fresh.Seq, events[0].Seq)
}
