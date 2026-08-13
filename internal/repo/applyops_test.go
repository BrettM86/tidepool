package repo

import (
	"context"
	"sync"
	"testing"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/testutil"
)

// ApplyOps is the multi-op commit primitive: several ops on ONE repo in ONE
// commit. Moderation needs it — Coves writes acceptance/removal transitions
// with applyWrites so the firehose never carries a half-completed action, and
// a consumer that saw "acceptance deleted" without "removal written" would
// render a post as neither accepted nor removed until the second commit
// landed. Two sequential commits cannot provide that; one commit can.
//
// API SHAPE, chosen to match what commitWrite already models:
//
//   - RecordOp carries (Collection, RKey, Record) because that is the vocabulary
//     every existing write method takes, not the stored Op's flattened Path.
//   - Action reuses the package's OpAction. A delete is OpActionDelete; a put
//     may be spelled either create or update, because which one it REALLY was
//     is decided by the MST and rendered by opFromIndigoOp — the caller does not
//     get to assert it onto the firehose.
//   - ExpectPrevCID is *string, not string, because the package already models
//     "no precondition" as a nil *casPrecondition and "must not exist" as an
//     empty expectCID. Collapsing them into one string would make the zero value
//     mean "must not exist", which is the dangerous default.
//   - The return is the existing *CommitResult: one commit, so one CommitCID,
//     one Rev, one Seq, one NoOp.

const testOtherCollection = "social.coves.community.acceptance"

// refusingKeys is a tombstoned actor's key custody: no key for writes (the
// repo is frozen), but deletes still get one — scrubbing IS the intent of
// consent revocation. It mirrors identity.ActorKeys' consent gate, which has
// its own tests; here it exists so the repo layer's KeyUse routing for a
// BATCH can be asserted without dragging in the identity package.
type refusingKeys struct {
	key *atcrypto.PrivateKeyK256

	mu   sync.Mutex
	uses []KeyUse
}

func (r *refusingKeys) SigningKey(_ context.Context, did string, use KeyUse) (atcrypto.PrivateKey, error) {
	r.mu.Lock()
	r.uses = append(r.uses, use)
	r.mu.Unlock()
	if use == KeyUseWrite {
		return nil, errors.NewTombstonedError("actor", did)
	}
	return r.key, nil
}

func (r *refusingKeys) recordedUses() []KeyUse {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]KeyUse(nil), r.uses...)
}

// eventCount counts firehose rows for a DID.
func eventCount(t *testing.T, m *Manager, did string) int {
	t.Helper()
	events, err := m.ListEvents(context.Background(), 0, 1000)
	require.NoError(t, err)
	n := 0
	for _, event := range events {
		if event.DID == did {
			n++
		}
	}
	return n
}

// eventsSince returns the events for a DID after seq.
func eventsSince(t *testing.T, m *Manager, did string, seq int64) []*Event {
	t.Helper()
	events, err := m.ListEvents(context.Background(), seq, 1000)
	require.NoError(t, err)
	var out []*Event
	for _, event := range events {
		if event.DID == did {
			out = append(out, event)
		}
	}
	return out
}

// TestApplyOps_TwoOpsOneCommit (M1): the whole point of the primitive. A
// delete in one collection and a put in another must arrive as ONE commit —
// one seq, one rev, one commit CID — with both ops on its event.
func TestApplyOps_TwoOpsOneCommit(t *testing.T) {
	manager, _, _ := testManager(t)
	ctx := context.Background()

	// Seed the record the batch will delete.
	seeded, err := manager.PutRecord(ctx, testDID, testOtherCollection, testRKey(1), testRecord("accepted"))
	require.NoError(t, err)
	baseSeq := seeded.Seq

	res, err := manager.ApplyOps(ctx, testDID, []RecordOp{
		{Action: OpActionDelete, Collection: testOtherCollection, RKey: testRKey(1)},
		{Action: OpActionCreate, Collection: testCollection, RKey: testRKey(2), Record: testRecord("removed")},
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.False(t, res.NoOp, "a batch that changes records is a real commit")

	events := eventsSince(t, manager, testDID, baseSeq)
	require.Len(t, events, 1,
		"a multi-op batch must produce EXACTLY ONE firehose event: a consumer that saw the "+
			"acceptance delete without the removal write would render the post as neither")
	event := events[0]
	assert.Equal(t, res.Seq, event.Seq)
	assert.Equal(t, res.CommitCID, event.CommitCID)
	require.Len(t, event.Ops, 2, "both ops must ride the one commit")

	byPath := map[string]Op{}
	for _, op := range event.Ops {
		byPath[op.Path] = op
	}
	deleted, ok := byPath[testOtherCollection+"/"+testRKey(1)]
	require.True(t, ok, "the delete op must be on the event, got %v", byPath)
	assert.Equal(t, OpActionDelete, deleted.Action)
	created, ok := byPath[testCollection+"/"+testRKey(2)]
	require.True(t, ok, "the put op must be on the event, got %v", byPath)
	assert.Equal(t, OpActionCreate, created.Action)

	// And the repo actually changed.
	_, _, err = manager.GetRecord(ctx, testDID, testOtherCollection, testRKey(1))
	assert.True(t, errors.IsNotFound(err), "the deleted record must be gone")
	_, _, err = manager.GetRecord(ctx, testDID, testCollection, testRKey(2))
	assert.NoError(t, err, "the written record must exist")
}

// TestApplyOps_RollbackLeavesNothingBehind (M2): a batch is all-or-nothing.
// One bad op must leave the repo exactly as it was — the deleted record still
// present, the new one absent, no event — because a partial moderation action
// is worse than none: it publishes a state nobody decided on.
func TestApplyOps_RollbackLeavesNothingBehind(t *testing.T) {
	manager, _, _ := testManager(t)
	ctx := context.Background()

	seeded, err := manager.PutRecord(ctx, testDID, testOtherCollection, testRKey(1), testRecord("accepted"))
	require.NoError(t, err)
	headBefore, revBefore, err := manager.Head(ctx, testDID)
	require.NoError(t, err)
	eventsBefore := eventCount(t, manager, testDID)

	_, err = manager.ApplyOps(ctx, testDID, []RecordOp{
		{Action: OpActionDelete, Collection: testOtherCollection, RKey: testRKey(1)},
		// Invalid: a record with no $type is refused by validateRecord.
		{Action: OpActionCreate, Collection: testCollection, RKey: testRKey(2),
			Record: map[string]any{"text": "no type"}},
	})
	require.Error(t, err, "an invalid op must fail the whole batch")

	_, _, err = manager.GetRecord(ctx, testDID, testOtherCollection, testRKey(1))
	assert.NoError(t, err, "the delete must be rolled back with the batch")
	_, _, err = manager.GetRecord(ctx, testDID, testCollection, testRKey(2))
	assert.True(t, errors.IsNotFound(err), "the failed put must leave nothing behind")

	head, rev, err := manager.Head(ctx, testDID)
	require.NoError(t, err)
	assert.Equal(t, headBefore, head, "a rolled-back batch must not advance the repo head")
	assert.Equal(t, revBefore, rev)
	assert.Equal(t, eventsBefore, eventCount(t, manager, testDID),
		"a rolled-back batch must not leave a firehose event")
	assert.Empty(t, eventsSince(t, manager, testDID, seeded.Seq),
		"no event may follow the seed commit")
}

// TestApplyOps_MissingDeleteIsTolerated (M3): unlike the single DeleteRecord,
// which errors NotFound, a delete op for a record that is already gone is a
// no-op WITHIN the batch. The heal flows re-run these batches on redelivery,
// and the second run necessarily finds the delete already applied; erroring
// there would abort the batch and prevent the other ops from healing.
func TestApplyOps_MissingDeleteIsTolerated(t *testing.T) {
	manager, _, _ := testManager(t)
	ctx := context.Background()

	// Establish the repo so this is not a genesis-commit special case.
	seeded, err := manager.PutRecord(ctx, testDID, testCollection, testRKey(9), testRecord("seed"))
	require.NoError(t, err)

	// Single-op DeleteRecord's behaviour, pinned as the contrast.
	_, err = manager.DeleteRecord(ctx, testDID, testOtherCollection, testRKey(1))
	require.True(t, errors.IsNotFound(err),
		"precondition: the single-op delete errors on a missing record")

	res, err := manager.ApplyOps(ctx, testDID, []RecordOp{
		{Action: OpActionDelete, Collection: testOtherCollection, RKey: testRKey(1)}, // never existed
		{Action: OpActionCreate, Collection: testCollection, RKey: testRKey(2), Record: testRecord("removed")},
	})
	require.NoError(t, err,
		"a delete for a missing record must be tolerated inside a batch, or a redelivered "+
			"heal can never re-run the ops beside it")
	require.NotNil(t, res)

	_, _, err = manager.GetRecord(ctx, testDID, testCollection, testRKey(2))
	assert.NoError(t, err, "the other ops must still land")

	events := eventsSince(t, manager, testDID, seeded.Seq)
	require.Len(t, events, 1)
	assert.Len(t, events[0].Ops, 1,
		"the tolerated missing delete must not be reported as an op that happened")
}

// TestApplyOps_AllNoOpCommitsNothing (M4): a batch of ops that each change
// nothing must commit nothing. Redelivery runs these batches repeatedly, and
// a commit per redelivery would churn the community repo and make Coves
// re-run admission on a decision nothing changed about.
func TestApplyOps_AllNoOpCommitsNothing(t *testing.T) {
	manager, _, _ := testManager(t)
	ctx := context.Background()

	seeded, err := manager.PutRecord(ctx, testDID, testCollection, testRKey(1), testRecord("same"))
	require.NoError(t, err)
	headBefore, revBefore, err := manager.Head(ctx, testDID)
	require.NoError(t, err)
	eventsBefore := eventCount(t, manager, testDID)

	res, err := manager.ApplyOps(ctx, testDID, []RecordOp{
		// Byte-identical re-put.
		{Action: OpActionUpdate, Collection: testCollection, RKey: testRKey(1), Record: testRecord("same")},
		// Delete of a record that is not there.
		{Action: OpActionDelete, Collection: testOtherCollection, RKey: testRKey(2)},
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.True(t, res.NoOp, "a batch where every op is inert must report NoOp")
	assert.Zero(t, res.Seq, "a NoOp batch emits no firehose event, so it has no seq")
	assert.Equal(t, seeded.CommitCID, res.CommitCID, "the head is unchanged, so the result carries it")

	head, rev, err := manager.Head(ctx, testDID)
	require.NoError(t, err)
	assert.Equal(t, headBefore, head, "an all-no-op batch must not advance the head")
	assert.Equal(t, revBefore, rev)
	assert.Equal(t, eventsBefore, eventCount(t, manager, testDID),
		"an all-no-op batch must not emit a firehose event")
}

// TestApplyOps_TombstonedRepoRefusesMixedBatchAllowsPureDelete (M5): key
// custody is what freezes a consent-revoked actor's repo, and it decides per
// KeyUse. A batch containing ANY put is a write and must be refused; a batch
// of pure deletes is a scrub and must go through — that is the whole point of
// releasing the key for deletes. The danger is a batch asking for
// KeyUseDelete because "it has a delete in it" and smuggling a put past the
// consent gate.
func TestApplyOps_TombstonedRepoRefusesMixedBatchAllowsPureDelete(t *testing.T) {
	database := testutil.DB(t)
	testutil.Truncate(t, database, "blocks", "repo_state", "firehose_events")
	key, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)

	// Seed with permissive custody, then freeze the repo.
	seedKeys := &staticKeys{key: key}
	seeder, err := NewManager(database, seedKeys, nil)
	require.NoError(t, err)
	ctx := context.Background()
	_, err = seeder.PutRecord(ctx, testDID, testOtherCollection, testRKey(1), testRecord("accepted"))
	require.NoError(t, err)
	_, err = seeder.PutRecord(ctx, testDID, testCollection, testRKey(3), testRecord("post"))
	require.NoError(t, err)

	frozenKeys := &refusingKeys{key: key}
	frozen, err := NewManager(database, frozenKeys, nil)
	require.NoError(t, err)
	eventsBefore := eventCount(t, frozen, testDID)

	// A mixed batch is a WRITE: refused.
	_, err = frozen.ApplyOps(ctx, testDID, []RecordOp{
		{Action: OpActionDelete, Collection: testOtherCollection, RKey: testRKey(1)},
		{Action: OpActionCreate, Collection: testCollection, RKey: testRKey(2), Record: testRecord("removal")},
	})
	require.Error(t, err, "a batch containing a put must not commit to a tombstoned repo")
	assert.True(t, errors.IsTombstoned(err),
		"the refusal must be the consent gate's tombstoned error, not something a caller retries: %v", err)
	assert.Equal(t, []KeyUse{KeyUseWrite}, frozenKeys.recordedUses(),
		"a batch containing any put must request KeyUseWrite; asking for KeyUseDelete because "+
			"the batch also deletes would smuggle a put past the consent gate")

	_, _, err = frozen.GetRecord(ctx, testDID, testOtherCollection, testRKey(1))
	assert.NoError(t, err, "the refused batch must not have deleted anything")
	assert.Equal(t, eventsBefore, eventCount(t, frozen, testDID),
		"a refused batch must not emit a firehose event")

	// A pure-delete batch is a SCRUB: allowed.
	res, err := frozen.ApplyOps(ctx, testDID, []RecordOp{
		{Action: OpActionDelete, Collection: testOtherCollection, RKey: testRKey(1)},
		{Action: OpActionDelete, Collection: testCollection, RKey: testRKey(3)},
	})
	require.NoError(t, err, "scrubbing a tombstoned actor's records must always be possible")
	require.NotNil(t, res)
	assert.False(t, res.NoOp)

	_, _, err = frozen.GetRecord(ctx, testDID, testOtherCollection, testRKey(1))
	assert.True(t, errors.IsNotFound(err), "the scrub must have landed")
	_, _, err = frozen.GetRecord(ctx, testDID, testCollection, testRKey(3))
	assert.True(t, errors.IsNotFound(err))
}
