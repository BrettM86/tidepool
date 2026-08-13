package consume

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// Second-opinion C2 (CRITICAL): the outbound write, the gate advance, and the
// enqueue must be ONE atomic unit.
//
// Today the handler writes outbound_objects (its own autocommit transaction),
// then advances the gate, then enqueues. If the enqueue fails, the row and the
// gate advance are already committed while the handler returns an error — so
// the connector retries or redrives, the gate now says "already applied", and
// the retry either double-enqueues or, worse, the NEXT genuine write bumps the
// seq off a phantom base and mints a second, distinct activity id for the same
// operation. A peer then sees two Creates for one comment.
//
// The fix is to run the outbound write on the gate's OWN transaction
// (UpsertTx / TombstoneTx) and commit only after the enqueue succeeds, so an
// enqueue failure rolls the row AND the gate advance back together. These
// tests pin the rollback: after an injected enqueue failure, NO row and NO
// gate row survive, and a replay is free to recover.

func TestAtomicity_CommentCreateEnqueueFailureLeavesNoCommittedState(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)

	enqueuer := &failingEnqueuer{err: fmt.Errorf("delivery queue unreachable")}
	fixture := newDispatchFixture(t, database, func(opts *Options) { opts.Enqueuer = enqueuer })

	const rkey = "3lzatom000001"
	atURI := commentATURIFor(dispatchNativeDID, rkey)

	err := fixture.handle(t, commentFrameFull(dispatchNativeDID, dispatchRev, rkey, "create", "hi", acceptRootATURI))
	require.Error(t, err, "an enqueue failure must fail the event so the connector retries it")
	require.Positive(t, enqueuer.calls, "the enqueue was actually attempted")

	_, err = store.NewOutboundObjects(database).GetByATURI(context.Background(), atURI)
	require.Error(t, err)
	assert.True(t, errors.IsNotFound(err),
		"NO outbound_objects row may survive: the row is written on the gate's own "+
			"transaction and committed only after the enqueue succeeds, so an enqueue "+
			"failure rolls it back")

	assert.Zero(t, countRows(t, database, "jetstream_record_revs"),
		"and NO gate row: the advance and the write share one transaction. A committed "+
			"row under an unadvanced gate is the exact split that lets a replay bump the "+
			"seq into a SECOND activity id for one comment")
}

func TestAtomicity_ReplayAfterEnqueueFailureRecoversToOneActivityID(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)

	const rkey = "3lzatom000002"
	atURI := commentATURIFor(dispatchNativeDID, rkey)
	frame := commentFrameFull(dispatchNativeDID, dispatchRev, rkey, "create", "hi", acceptRootATURI)

	// First delivery: enqueue fails, everything rolls back.
	failing := &failingEnqueuer{err: fmt.Errorf("delivery queue unreachable")}
	fix1 := newDispatchFixture(t, database, func(opts *Options) { opts.Enqueuer = failing })
	require.Error(t, fix1.handle(t, frame))

	// The redrive: same frame, same rev, healthy queue now.
	fix2 := newDispatchFixture(t, database)
	require.NoError(t, fix2.handle(t, frame),
		"the retry succeeds because the failed attempt left no gate row to reject it")

	stored, err := store.NewOutboundObjects(database).GetByATURI(context.Background(), atURI)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, 0, stored.LastActivitySeq,
		"the recovered create is seq 0 — the rolled-back attempt consumed no seq, so the "+
			"activity id is the one and only id for this create")

	calls := fix2.enqueuer.Calls()
	require.Len(t, calls, 1, "exactly one intent survives the failure-then-recovery")
	assert.Equal(t, ActivityID(acceptUserOrigin, atURI, "create", 0), calls[0].Intent.ActivityID(),
		"and its id is stable: a peer never sees two Creates for one comment")
}

func TestAtomicity_VoteCreateEnqueueFailureLeavesNoCommittedState(t *testing.T) {
	database := dispatchTestDB(t)
	seedBridgedCommunity(t, database)
	seedThreadRoot(t, database)

	enqueuer := &failingEnqueuer{err: fmt.Errorf("delivery queue unreachable")}
	fixture := newDispatchFixture(t, database, func(opts *Options) { opts.Enqueuer = enqueuer })

	const rkey = "3lzatom000003"
	voteATURI := voteATURIFor(dispatchNativeDID, rkey)

	err := fixture.handle(t, voteFrame(dispatchNativeDID, dispatchRev, rkey, acceptRootATURI, "up"))
	require.Error(t, err)

	_, err = store.NewOutboundVotes(database).GetByATURI(context.Background(), voteATURI)
	require.Error(t, err)
	assert.True(t, errors.IsNotFound(err),
		"NO outbound_votes row survives an enqueue failure: the current_activity_id it "+
			"stores must match the id the Like actually went out under, and a row "+
			"committed for a Like that never left would strand a wrong id for the Undo")

	assert.Zero(t, countRows(t, database, "jetstream_record_revs"),
		"and the gate is unadvanced, so the redrive replays cleanly")
}
