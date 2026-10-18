package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"testing"

	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

func TestBridgedActors_UpsertActorOnInsert(t *testing.T) {
	const primaryHandle = "alice.lemmy-world.tidepool.test"
	const extraHandle = "alice-extra.lemmy-world.tidepool.test"

	t.Run("new actor commits row claim and callback write", func(t *testing.T) {
		database := testDB(t)
		actors := NewBridgedActors(database)
		ctx := t.Context()
		calls := 0
		stored, err := actors.UpsertActorOnInsert(ctx, claimActor(testAPActorID, testDID, primaryHandle),
			func(ctx context.Context, tx *sql.Tx, inserted *BridgedActor) error {
				calls++
				if inserted == nil || inserted.ID == 0 || inserted.DID != testDID || inserted.Handle != primaryHandle {
					return stderrors.New("callback did not receive stored actor with ID, DID and handle")
				}
				_, writeErr := tx.ExecContext(ctx, `INSERT INTO handle_claims (handle, actor_id) VALUES ($1, $2)`, extraHandle, inserted.ID)
				return writeErr
			})
		require.NoError(t, err)
		require.NotNil(t, stored)
		require.Positive(t, stored.ID)
		require.Equal(t, testDID, stored.DID)
		require.Equal(t, primaryHandle, stored.Handle)
		require.Equal(t, 1, calls)
		require.Equal(t, []int64{stored.ID}, claimsFor(t, database, primaryHandle))
		require.Equal(t, []int64{stored.ID}, claimsFor(t, database, extraHandle))
		readBack, err := actors.GetByAPActorID(ctx, testAPActorID)
		require.NoError(t, err)
		require.Equal(t, stored.ID, readBack.ID)
	})

	t.Run("callback error rolls back actor claim and callback write", func(t *testing.T) {
		database := testDB(t)
		actors := NewBridgedActors(database)
		ctx := t.Context()
		sentinel := stderrors.New("reject mint")
		calls := 0
		_, err := actors.UpsertActorOnInsert(ctx, claimActor(testAPActorID, testDID, primaryHandle),
			func(ctx context.Context, tx *sql.Tx, inserted *BridgedActor) error {
				calls++
				if inserted == nil || inserted.ID == 0 || inserted.DID != testDID || inserted.Handle != primaryHandle {
					return stderrors.New("callback did not receive stored actor with ID, DID and handle")
				}
				if _, writeErr := tx.ExecContext(ctx, `INSERT INTO handle_claims (handle, actor_id) VALUES ($1, $2)`, extraHandle, inserted.ID); writeErr != nil {
					return writeErr
				}
				return sentinel
			})
		require.ErrorIs(t, err, sentinel)
		require.Equal(t, 1, calls)
		_, err = actors.GetByAPActorID(ctx, testAPActorID)
		require.True(t, errors.IsNotFound(err), "actor must roll back: %v", err)
		require.Empty(t, claimsFor(t, database, primaryHandle))
		require.Empty(t, claimsFor(t, database, extraHandle))
	})

	t.Run("refresh updates existing row without callback", func(t *testing.T) {
		database := testDB(t)
		actors := NewBridgedActors(database)
		ctx := t.Context()
		first, err := actors.UpsertActor(ctx, claimActor(testAPActorID, testDID, primaryHandle))
		require.NoError(t, err)
		refresh := claimActor(testAPActorID, testDID, primaryHandle)
		refresh.PreferredUsername = "alice refreshed"
		calls := 0
		updated, err := actors.UpsertActorOnInsert(ctx, refresh,
			func(context.Context, *sql.Tx, *BridgedActor) error {
				calls++
				return stderrors.New("callback must not run on refresh")
			})
		require.NoError(t, err)
		require.NotNil(t, updated)
		require.Equal(t, 0, calls)
		require.Equal(t, first.ID, updated.ID)
		require.Equal(t, "alice refreshed", updated.PreferredUsername)
		readBack, err := actors.GetByAPActorID(ctx, testAPActorID)
		require.NoError(t, err)
		require.Equal(t, "alice refreshed", readBack.PreferredUsername)
		require.Equal(t, []int64{first.ID}, claimsFor(t, database, primaryHandle))
	})
}
