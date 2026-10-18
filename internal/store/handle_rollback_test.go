package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	internalerrors "tidepool/internal/errors"
	"tidepool/internal/testutil"
)

func TestBridgedActors_ListRollbackCandidates(t *testing.T) {
	db := testDB(t)
	actors := NewBridgedActors(db)
	ctx := t.Context()
	for _, row := range []struct {
		did, name, handle, previous, pending string
		deleted                              bool
	}{
		{"did:plc:rollbackrenamed", "renamed", "renamed.example.com.ap.tidepool.test", "renamed.example-com.tidepool.test", "", false},
		{"did:plc:rollbackpending", "pending", "pending.example.com.ap.tidepool.test", "pending.example-com.tidepool.test", "pending.example-com.tidepool.test", false},
		{"did:plc:forwardpending", "forward", "forward.example-com.tidepool.test", "", "forward.example.com.ap.tidepool.test", false},
		{"did:plc:rolledback", "rolledback", "rolledback.example-com.tidepool.test", "rolledback.example.com.ap.tidepool.test", "", false},
		{"did:plc:newmint", "newmint", "newmint.example.com.ap.tidepool.test", "", "", false},
		{"did:plc:deletedrename", "deleted", "deleted.example.com.ap.tidepool.test", "deleted.example-com.tidepool.test", "", true},
		{"did:plc:neverrenamed", "never", "never.example-com.tidepool.test", "", "", false},
	} {
		actor := testActor()
		actor.APActorID = "https://example.com/u/" + row.name
		actor.DID = row.did
		actor.Handle = row.handle
		_, err := actors.UpsertActor(ctx, actor)
		require.NoError(t, err)
		if row.previous != "" {
			_, err = db.ExecContext(ctx, `UPDATE bridged_actors SET previous_handle = $1 WHERE did = $2`, row.previous, row.did)
			require.NoError(t, err)
		}
		if row.pending != "" {
			require.NoError(t, actors.SetPendingHandle(ctx, row.did, row.pending))
		}
		if row.deleted {
			require.NoError(t, actors.SetConsentState(ctx, actor.APActorID, ConsentStateDeleted))
		}
	}

	got, err := actors.ListRollbackCandidates(ctx, "tidepool.test", "ap.tidepool.test", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"did:plc:rollbackrenamed", "did:plc:rollbackpending", "did:plc:forwardpending"}, got)
	got, err = actors.ListRollbackCandidates(ctx, "tidepool.test", "ap.tidepool.test",
		[]string{"did:plc:deletedrename", "did:plc:rolledback", "did:plc:rollbackrenamed"})
	require.NoError(t, err)
	require.Equal(t, []string{"did:plc:rollbackrenamed"}, got)
}

func TestBridgedActors_AbandonPendingHandleAtomic(t *testing.T) {
	for _, tc := range []struct {
		name, expected string
		// marked records a revert POST before the abandon.
		marked, commits, callbackFails, deleted bool
	}{
		{"commits after a revert was submitted", "alice.example.com.ap.tidepool.test", true, true, false, false},
		{"commits without a revert submission", "alice.example.com.ap.tidepool.test", false, true, false, false},
		{"callback fails", "alice.example.com.ap.tidepool.test", true, false, true, false},
		{"stale expectation", "other.example.com.ap.tidepool.test", true, false, false, false},
		{"tombstoned", "alice.example.com.ap.tidepool.test", true, false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := testDB(t)
			testutil.Truncate(t, db, "firehose_events")
			actors := NewBridgedActors(db)
			ctx := t.Context()
			actor := testActor()
			actor.Handle = "alice.example-com.tidepool.test"
			stored, err := actors.UpsertActor(ctx, actor)
			require.NoError(t, err)
			require.NotNil(t, stored)
			require.NoError(t, actors.SetPendingHandle(ctx, testDID, "alice.example.com.ap.tidepool.test"))
			seededAt := time.Date(2026, 9, 1, 10, 30, 0, 0, time.UTC)
			_, err = db.ExecContext(ctx, `UPDATE bridged_actors SET previous_handle = $1, handle_migrated_at = $2 WHERE did = $3`,
				"older.example.com.ap.tidepool.test", seededAt, testDID)
			require.NoError(t, err)
			require.NoError(t, actors.RecordHandleMigrationError(ctx, testDID, HandleMigrationPLCRejected, "Invalid signature"))
			if tc.marked {
				require.NoError(t, actors.MarkPendingRevertSubmitted(ctx, testDID, "alice.example.com.ap.tidepool.test"))
			}
			if tc.deleted {
				require.NoError(t, actors.SetConsentState(ctx, testAPActorID, ConsentStateDeleted))
			}

			callbackError := errors.New("reject identity event")
			called := false
			callback := func(ctx context.Context, tx *sql.Tx, actor *BridgedActor) error {
				called = true
				if actor == nil {
					return errors.New("abandon callback received nil actor")
				}
				if _, err := tx.ExecContext(ctx, `INSERT INTO firehose_events (kind, did, identity_handle, created_at)
					VALUES ('identity', $1, $2, clock_timestamp())`, testDID, actor.Handle); err != nil {
					return err
				}
				if tc.callbackFails {
					return callbackError
				}
				return nil
			}
			abandoned, err := actors.AbandonPendingHandle(ctx, testDID, tc.expected, callback)
			if tc.commits {
				require.NoError(t, err)
				require.Equal(t, tc.marked, called, "the callback runs only after a recorded revert submission")
				require.False(t, revertSubmitted(t, db, testDID), "abandon clears the revert marker")
				require.Equal(t, pendingRenameRow{
					Handle: "alice.example-com.tidepool.test", PreviousHandle: "older.example.com.ap.tidepool.test", Migrated: true,
				}, readPendingRenameRow(t, db, testDID))
				require.NotNil(t, abandoned)
				require.Equal(t, "alice.example-com.tidepool.test", abandoned.Handle)
				require.Zero(t, pendingClaimCount(t, db, "alice.example.com.ap.tidepool.test"))
				var migratedAt time.Time
				require.NoError(t, db.QueryRowContext(ctx, `SELECT handle_migrated_at FROM bridged_actors WHERE did = $1`, testDID).Scan(&migratedAt))
				require.True(t, migratedAt.Equal(seededAt))
				var code, message sql.NullString
				require.NoError(t, db.QueryRowContext(ctx, `SELECT handle_migration_error, handle_migration_message FROM bridged_actors WHERE did = $1`, testDID).Scan(&code, &message))
				require.False(t, code.Valid)
				require.False(t, message.Valid)
				var handles []string
				rows, err := db.QueryContext(ctx, `SELECT identity_handle FROM firehose_events WHERE kind = 'identity' AND did = $1 ORDER BY seq`, testDID)
				require.NoError(t, err)
				defer rows.Close()
				for rows.Next() {
					var handle string
					require.NoError(t, rows.Scan(&handle))
					handles = append(handles, handle)
				}
				require.NoError(t, rows.Err())
				if tc.marked {
					require.Equal(t, []string{"alice.example-com.tidepool.test"}, handles)
				} else {
					require.Empty(t, handles)
				}
				return
			}
			require.Equal(t, tc.marked, revertSubmitted(t, db, testDID), "a refused abandon keeps the revert marker")
			switch {
			case tc.callbackFails:
				require.ErrorIs(t, err, callbackError)
			case tc.deleted:
				require.True(t, internalerrors.IsTombstoned(err), "a tombstoned actor is frozen, got %v", err)
			default:
				require.True(t, internalerrors.IsAlreadyExists(err), "a stale expectation is a conflict, got %v", err)
			}
			require.Nil(t, abandoned)
			require.Equal(t, pendingRenameRow{
				Handle: "alice.example-com.tidepool.test", PendingHandle: "alice.example.com.ap.tidepool.test",
				PreviousHandle: "older.example.com.ap.tidepool.test", Migrated: true,
			}, readPendingRenameRow(t, db, testDID))
			require.Equal(t, 1, pendingClaimCount(t, db, "alice.example.com.ap.tidepool.test"))
			var migratedAt time.Time
			var code, message sql.NullString
			require.NoError(t, db.QueryRowContext(ctx, `SELECT handle_migrated_at, handle_migration_error, handle_migration_message FROM bridged_actors WHERE did = $1`, testDID).
				Scan(&migratedAt, &code, &message))
			require.True(t, migratedAt.Equal(seededAt))
			require.Equal(t, sql.NullString{String: "plc_rejected", Valid: true}, code)
			require.Equal(t, sql.NullString{String: "Invalid signature", Valid: true}, message)
			var events int
			require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM firehose_events WHERE did = $1`, testDID).Scan(&events))
			require.Zero(t, events)
		})
	}
}

func TestBridgedActors_FinalizeHandleRenameRefusesStaleExpectation(t *testing.T) {
	db := testDB(t)
	testutil.Truncate(t, db, "firehose_events")
	actors := NewBridgedActors(db)
	ctx := t.Context()
	actor := testActor()
	actor.Handle = "alice.example-com.tidepool.test"
	_, err := actors.UpsertActor(ctx, actor)
	require.NoError(t, err)
	require.NoError(t, actors.SetPendingHandle(ctx, testDID, "alice.example.com.ap.tidepool.test"))
	before := readPendingRenameRow(t, db, testDID)

	called := false
	renamed, err := actors.FinalizeHandleRename(ctx, testDID, "other.example.com.ap.tidepool.test",
		func(context.Context, *sql.Tx, *BridgedActor) error {
			called = true
			return nil
		})
	require.True(t, internalerrors.IsAlreadyExists(err), "PLC was confirmed at another handle, got %v", err)
	require.Nil(t, renamed)
	require.False(t, called, "no #identity may be emitted for a handle PLC was not confirmed at")
	require.Equal(t, before, readPendingRenameRow(t, db, testDID))
	require.Equal(t, 1, pendingClaimCount(t, db, "alice.example.com.ap.tidepool.test"))
	require.Equal(t, 1, pendingClaimCount(t, db, "alice.example-com.tidepool.test"))
}

func revertSubmitted(t *testing.T, db *sql.DB, did string) bool {
	t.Helper()
	var marked bool
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT pending_revert_submitted_at IS NOT NULL
		FROM bridged_actors WHERE did = $1`, did).Scan(&marked))
	return marked
}

func TestBridgedActors_MarkPendingRevertSubmitted(t *testing.T) {
	const pending = "alice.example.com.ap.tidepool.test"
	for _, tc := range []struct {
		name, expected string
		deleted        bool
		wantMarked     bool
	}{
		{"marks the expected pending handle", pending, false, true},
		{"stale expectation", "other.example.com.ap.tidepool.test", false, false},
		{"tombstoned", pending, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := testDB(t)
			actors := NewBridgedActors(db)
			ctx := t.Context()
			actor := testActor()
			actor.Handle = "alice.example-com.tidepool.test"
			_, err := actors.UpsertActor(ctx, actor)
			require.NoError(t, err)
			require.NoError(t, actors.SetPendingHandle(ctx, testDID, pending))
			if tc.deleted {
				require.NoError(t, actors.SetConsentState(ctx, testAPActorID, ConsentStateDeleted))
			}
			err = actors.MarkPendingRevertSubmitted(ctx, testDID, tc.expected)
			if tc.wantMarked {
				require.NoError(t, err)
			} else {
				require.True(t, internalerrors.IsAlreadyExists(err), "no POST may follow, got %v", err)
			}
			require.Equal(t, tc.wantMarked, revertSubmitted(t, db, testDID))
		})
	}
}

// A marker left by an earlier rollback attempt must not emit #identity for a
// later pending handle.
func TestBridgedActors_NewPendingHandleOrFinalizeClearsRevertMarker(t *testing.T) {
	const pending = "alice.example.com.ap.tidepool.test"
	db := testDB(t)
	actors := NewBridgedActors(db)
	ctx := t.Context()
	actor := testActor()
	actor.Handle = "alice.example-com.tidepool.test"
	_, err := actors.UpsertActor(ctx, actor)
	require.NoError(t, err)
	require.NoError(t, actors.SetPendingHandle(ctx, testDID, pending))
	require.NoError(t, actors.MarkPendingRevertSubmitted(ctx, testDID, pending))
	require.NoError(t, actors.SetPendingHandle(ctx, testDID, pending))
	require.False(t, revertSubmitted(t, db, testDID), "a new attempt starts without a revert marker")
	require.NoError(t, actors.MarkPendingRevertSubmitted(ctx, testDID, pending))
	_, err = actors.FinalizeHandleRename(ctx, testDID, pending, nil)
	require.NoError(t, err)
	require.False(t, revertSubmitted(t, db, testDID), "finalize clears the revert marker")
}
