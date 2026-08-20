package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

// UNTOMBSTONING IS AN EXPLICIT DECISION, AND IT IS THE ONLY WAY THE FLAG COMES
// OFF.
//
// Upsert deliberately preserves tombstoned_at — a tombstoned row that receives a
// later write keeps its tombstone — because clearing it must never be a side
// effect of writing content. But the acceptance engine RE-PUBLISHES a withdrawn
// post (its auto-restore of an admission-revoked removal, and a readmit), and it
// reads live-ness off exactly this flag. So the clear needs a door of its own,
// with a contract sharp enough that the engine can ride it on the same
// transaction as the acceptance: it must not touch the activity-id counter, it
// must be a no-op over a live row, and a missing row must be an error rather
// than a silent skip.

// tombstonedObject writes the test object and tombstones it, returning the
// tombstoned row.
func tombstonedObject(t *testing.T, repo OutboundObjects) *OutboundObject {
	t.Helper()
	ctx := context.Background()
	_, err := repo.Upsert(ctx, testOutboundObject())
	require.NoError(t, err)
	dead, err := repo.Tombstone(ctx, testCommentATURI)
	require.NoError(t, err)
	require.True(t, dead.IsTombstoned(), "precondition: the row is tombstoned")
	return dead
}

func TestOutboundObjects_UntombstoneTxClearsTheFlagWithoutSpendingASeq(t *testing.T) {
	database := outboundTestDB(t)
	repo := NewOutboundObjects(database)
	ctx := context.Background()

	dead := tombstonedObject(t, repo)
	require.Equal(t, 1, dead.LastActivitySeq,
		"precondition: the tombstone spent seq 1 on its Delete")

	tx, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	live, err := repo.UntombstoneTx(ctx, tx, testCommentATURI)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	assert.False(t, live.IsTombstoned(),
		"the object is live outward again, so the row must stop saying it is dead")
	assert.Equal(t, dead.LastActivitySeq, live.LastActivitySeq,
		"the seq is the activity-id counter and the caller's upsert on the same transaction "+
			"already decided this publication's id: bumping here would mint a second id for one "+
			"activity")

	got, err := repo.GetByATURI(ctx, testCommentATURI)
	require.NoError(t, err)
	assert.False(t, got.IsTombstoned(), "and the clear is what committed")
}

func TestOutboundObjects_UntombstoneTxRollsBackWithItsTransaction(t *testing.T) {
	database := outboundTestDB(t)
	repo := NewOutboundObjects(database)
	ctx := context.Background()

	tombstonedObject(t, repo)

	tx, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = repo.UntombstoneTx(ctx, tx, testCommentATURI)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())

	got, err := repo.GetByATURI(ctx, testCommentATURI)
	require.NoError(t, err)
	assert.True(t, got.IsTombstoned(),
		"a rolled-back re-publication must leave the tombstone standing: the acceptance record "+
			"and the live-ness fact ride ONE transaction, so neither may survive the other")
}

func TestOutboundObjects_UntombstoneTxOfALiveRowIsANoOp(t *testing.T) {
	database := outboundTestDB(t)
	repo := NewOutboundObjects(database)
	ctx := context.Background()

	stored, err := repo.Upsert(ctx, testOutboundObject())
	require.NoError(t, err)
	require.False(t, stored.IsTombstoned(), "precondition: never tombstoned")

	tx, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	live, err := repo.UntombstoneTx(ctx, tx, testCommentATURI)
	require.NoError(t, err,
		"the caller re-publishes on EVERY accept and cannot know whether the post was withdrawn; "+
			"clearing a flag that is already clear is success, not an error")
	require.NoError(t, tx.Commit())

	assert.False(t, live.IsTombstoned())
	assert.Equal(t, stored.LastActivitySeq, live.LastActivitySeq, "and it spends no seq")
	assert.WithinDuration(t, stored.UpdatedAt, live.UpdatedAt, 0,
		"updated_at moves only when a tombstone was actually cleared, so the ordinary accept "+
			"path does not churn the row's timestamp")
}

func TestOutboundObjects_UntombstoneTxRejectsMissingRowAndNilTx(t *testing.T) {
	database := outboundTestDB(t)
	repo := NewOutboundObjects(database)
	ctx := context.Background()

	tx, err := database.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = repo.UntombstoneTx(ctx, tx, "at://did:plc:nobody/social.coves.community.comment/nope")
	require.Error(t, err,
		"re-publishing an object we hold no state for is a bug: a silent no-op would let the "+
			"engine commit an acceptance whose outbound row never existed")
	assert.True(t, errors.IsNotFound(err), "want NotFound, got %v", err)
	require.NoError(t, tx.Rollback())

	_, err = repo.UntombstoneTx(ctx, nil, testCommentATURI)
	require.Error(t, err, "UntombstoneTx with a nil tx must not silently fall back to the pool")
	assert.True(t, errors.IsValidation(err), "want validation error, got %v", err)
}
