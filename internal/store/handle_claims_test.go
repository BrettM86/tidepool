package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

func claimsFor(t *testing.T, db *sql.DB, handle string) []int64 {
	t.Helper()
	rows, err := db.Query(`SELECT actor_id FROM handle_claims WHERE handle = $1`, handle)
	require.NoError(t, err)
	defer rows.Close()
	var actorIDs []int64
	for rows.Next() {
		var actorID int64
		require.NoError(t, rows.Scan(&actorID))
		actorIDs = append(actorIDs, actorID)
	}
	require.NoError(t, rows.Err())
	return actorIDs
}

func handlesClaimedBy(t *testing.T, db *sql.DB, actorID int64) []string {
	t.Helper()
	rows, err := db.Query(`SELECT handle FROM handle_claims WHERE actor_id = $1 ORDER BY handle`, actorID)
	require.NoError(t, err)
	defer rows.Close()
	var handles []string
	for rows.Next() {
		var handle string
		require.NoError(t, rows.Scan(&handle))
		handles = append(handles, handle)
	}
	require.NoError(t, rows.Err())
	return handles
}

func rawClaim(t *testing.T, db *sql.DB, handle string, actorID int64) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO handle_claims (handle, actor_id) VALUES ($1, $2)`, handle, actorID)
	require.NoError(t, err)
}

func claimActor(apActorID, did, handle string) BridgedActor {
	actor := testActor()
	actor.APActorID = apActorID
	actor.DID = did
	actor.Handle = handle
	return actor
}

func requireHandleConflict(t *testing.T, err error) {
	t.Helper()
	require.True(t, errors.IsAlreadyExists(err), "expected handle conflict, got %v", err)
	var conflict errors.ConflictError
	require.True(t, stderrors.As(err, &conflict), "expected ConflictError, got %v", err)
	require.Equal(t, "bridged_actor", conflict.Resource)
	require.Equal(t, "handle", conflict.Field)
}

func TestHandleClaims_NewActor(t *testing.T) {
	db := testDB(t)
	repo := NewBridgedActors(db)
	ctx := context.Background()
	actor, err := repo.UpsertActor(ctx, claimActor(testAPActorID, testDID, "alice.lemmy-world.tidepool.test"))
	require.NoError(t, err)
	require.Equal(t, []int64{actor.ID}, claimsFor(t, db, "alice.lemmy-world.tidepool.test"))
}

func TestHandleClaims_RawClaimBlocksNewActor(t *testing.T) {
	db := testDB(t)
	repo := NewBridgedActors(db)
	ctx := context.Background()
	owner, err := repo.UpsertActor(ctx, claimActor(testAPActorID, testDID, "owner.lemmy-world.tidepool.test"))
	require.NoError(t, err)
	rawClaim(t, db, "alice.lemmy-world.tidepool.test", owner.ID)

	_, err = repo.UpsertActor(ctx, claimActor("https://lemmy.world/u/bob", testSecondDID, "alice.lemmy-world.tidepool.test"))
	requireHandleConflict(t, err)
	_, err = repo.GetByAPActorID(ctx, "https://lemmy.world/u/bob")
	require.True(t, errors.IsNotFound(err), "rejected actor must not be stored: %v", err)
	require.Equal(t, []int64{owner.ID}, claimsFor(t, db, "alice.lemmy-world.tidepool.test"))
	var rejectedClaims int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM handle_claims WHERE actor_id IN (SELECT id FROM bridged_actors WHERE ap_actor_id = $1)`, "https://lemmy.world/u/bob").Scan(&rejectedClaims))
	require.Zero(t, rejectedClaims)
}

func TestHandleClaims_Transfer(t *testing.T) {
	for _, tc := range []struct {
		name         string
		conflictKind string
	}{
		{name: "unclaimed destination"},
		{name: "raw claim by another actor", conflictKind: "raw"},
		{name: "legacy handle held by another actor", conflictKind: "legacy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := testDB(t)
			repo := NewBridgedActors(db)
			ctx := context.Background()
			original, err := repo.UpsertActor(ctx, claimActor(testAPActorID, testDID, "alice-old.lemmy-world.tidepool.test"))
			require.NoError(t, err)
			var other *BridgedActor
			if tc.conflictKind != "" {
				otherHandle := "bob.lemmy-world.tidepool.test"
				if tc.conflictKind == "legacy" {
					otherHandle = "alice-new.lemmy-world.tidepool.test"
				}
				other, err = repo.UpsertActor(ctx, claimActor("https://lemmy.world/u/bob", testSecondDID, otherHandle))
				require.NoError(t, err)
				if tc.conflictKind == "raw" {
					rawClaim(t, db, "alice-new.lemmy-world.tidepool.test", other.ID)
				} else {
					_, err = db.Exec(`DELETE FROM handle_claims WHERE handle = $1`, "alice-new.lemmy-world.tidepool.test")
					require.NoError(t, err)
				}
			}

			_, err = repo.UpsertActor(ctx, claimActor(testAPActorID, testDID, "alice-new.lemmy-world.tidepool.test"))
			if tc.conflictKind != "" {
				requireHandleConflict(t, err)
				stored, getErr := repo.GetByAPActorID(ctx, testAPActorID)
				require.NoError(t, getErr)
				require.Equal(t, "alice-old.lemmy-world.tidepool.test", stored.Handle)
				require.Equal(t, []int64{original.ID}, claimsFor(t, db, "alice-old.lemmy-world.tidepool.test"))
				require.Equal(t, []string{"alice-old.lemmy-world.tidepool.test"}, handlesClaimedBy(t, db, original.ID))
				if tc.conflictKind == "raw" {
					require.Equal(t, []int64{other.ID}, claimsFor(t, db, "alice-new.lemmy-world.tidepool.test"))
				} else {
					require.Empty(t, claimsFor(t, db, "alice-new.lemmy-world.tidepool.test"))
				}
				return
			}
			require.NoError(t, err)
			stored, err := repo.GetByAPActorID(ctx, testAPActorID)
			require.NoError(t, err)
			require.Equal(t, "alice-new.lemmy-world.tidepool.test", stored.Handle)
			require.Equal(t, []int64{original.ID}, claimsFor(t, db, "alice-new.lemmy-world.tidepool.test"))
			require.Empty(t, claimsFor(t, db, "alice-old.lemmy-world.tidepool.test"))
		})
	}
}

func TestHandleClaims_NoopUpsert(t *testing.T) {
	for _, tc := range []struct {
		name       string
		legacy     bool
		incoming   string
		tombstoned bool
	}{
		{name: "same handle", incoming: "alice.lemmy-world.tidepool.test"},
		{name: "empty handle"},
		{name: "tombstoned actor with reserved replacement", incoming: "alice-new.lemmy-world.tidepool.test", tombstoned: true},
		{name: "legacy same handle", legacy: true, incoming: "alice.lemmy-world.tidepool.test"},
		{name: "legacy empty handle", legacy: true},
		{name: "legacy tombstoned actor with reserved replacement", legacy: true, incoming: "alice-new.lemmy-world.tidepool.test", tombstoned: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := testDB(t)
			repo := NewBridgedActors(db)
			ctx := context.Background()
			actor, err := repo.UpsertActor(ctx, claimActor(testAPActorID, testDID, "alice.lemmy-world.tidepool.test"))
			require.NoError(t, err)
			if tc.legacy {
				_, err = db.Exec(`DELETE FROM handle_claims WHERE handle = $1`, "alice.lemmy-world.tidepool.test")
				require.NoError(t, err)
			}
			if tc.tombstoned {
				other, upsertErr := repo.UpsertActor(ctx, claimActor("https://lemmy.world/u/bob", testSecondDID, "bob.lemmy-world.tidepool.test"))
				require.NoError(t, upsertErr)
				rawClaim(t, db, "alice-new.lemmy-world.tidepool.test", other.ID)
				require.NoError(t, repo.SetConsentState(ctx, testAPActorID, ConsentStateDeleted))
			}

			_, err = repo.UpsertActor(ctx, claimActor(testAPActorID, testDID, tc.incoming))
			require.NoError(t, err)
			stored, err := repo.GetByAPActorID(ctx, testAPActorID)
			require.NoError(t, err)
			require.Equal(t, "alice.lemmy-world.tidepool.test", stored.Handle)
			if tc.legacy {
				require.Empty(t, handlesClaimedBy(t, db, actor.ID))
				require.Empty(t, claimsFor(t, db, "alice.lemmy-world.tidepool.test"))
			} else {
				require.Equal(t, []string{"alice.lemmy-world.tidepool.test"}, handlesClaimedBy(t, db, actor.ID))
				require.Equal(t, []int64{actor.ID}, claimsFor(t, db, "alice.lemmy-world.tidepool.test"))
			}
			if tc.tombstoned {
				other, getErr := repo.GetByAPActorID(ctx, "https://lemmy.world/u/bob")
				require.NoError(t, getErr)
				require.Equal(t, []int64{other.ID}, claimsFor(t, db, "alice-new.lemmy-world.tidepool.test"))
			}
		})
	}
}

func TestHandleClaims_RacingReservation(t *testing.T) {
	db := testDB(t)
	repo := NewBridgedActors(db)
	ctx := context.Background()
	owner, err := repo.UpsertActor(ctx, claimActor(testAPActorID, testDID, "owner.lemmy-world.tidepool.test"))
	require.NoError(t, err)

	reservationConn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { _ = reservationConn.Close() }()
	var reservationPID int
	require.NoError(t, reservationConn.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&reservationPID))
	reservation, err := reservationConn.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer reservation.Rollback()
	_, err = reservation.ExecContext(ctx, `INSERT INTO handle_claims (handle, actor_id) VALUES ($1, $2)`, "alice.lemmy-world.tidepool.test", owner.ID)
	require.NoError(t, err)

	upsertCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, upsertErr := repo.UpsertActor(upsertCtx, claimActor("https://lemmy.world/u/bob", testSecondDID, "alice.lemmy-world.tidepool.test"))
		result <- upsertErr
	}()

	// Observe a backend blocked by the reservation transaction, rather than
	// inferring ordering from a delay or from starting the goroutine.
	deadline := time.Now().Add(5 * time.Second)
	for {
		select {
		case upsertErr := <-result:
			t.Fatalf("upsert completed before waiting for reservation: %v", upsertErr)
		default:
		}
		var waiting bool
		err = db.QueryRowContext(upsertCtx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_stat_activity
				WHERE datname = current_database()
				  AND state = 'active' AND wait_event_type = 'Lock'
				  AND $1 = ANY(pg_blocking_pids(pid))
			)`, reservationPID).Scan(&waiting)
		require.NoError(t, err)
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("upsert did not wait on the uncommitted handle reservation")
		}
		time.Sleep(10 * time.Millisecond) // Poll interval only; the lock wait is the synchronization.
	}
	require.NoError(t, reservation.Commit())
	select {
	case err = <-result:
		requireHandleConflict(t, err)
	case <-upsertCtx.Done():
		t.Fatal("upsert did not finish after the reservation committed")
	}
	_, err = repo.GetByAPActorID(ctx, "https://lemmy.world/u/bob")
	require.True(t, errors.IsNotFound(err), "rejected actor must not be stored: %v", err)
	require.Equal(t, []int64{owner.ID}, claimsFor(t, db, "alice.lemmy-world.tidepool.test"))
}

func TestHandleClaims_DeleteActorRowReleasesHandle(t *testing.T) {
	db := testDB(t)
	repo := NewBridgedActors(db)
	ctx := context.Background()
	actor, err := repo.UpsertActor(ctx, claimActor(testAPActorID, testDID, "alice.lemmy-world.tidepool.test"))
	require.NoError(t, err)
	require.Equal(t, []int64{actor.ID}, claimsFor(t, db, "alice.lemmy-world.tidepool.test"))

	require.NoError(t, repo.DeleteActorRow(ctx, testAPActorID, testDID, nil))
	require.Empty(t, claimsFor(t, db, "alice.lemmy-world.tidepool.test"))
	taken, err := repo.HandleTaken(ctx, "alice.lemmy-world.tidepool.test")
	require.NoError(t, err)
	require.False(t, taken)
}

func TestHandleClaims_HandleTaken(t *testing.T) {
	for _, tc := range []struct {
		name      string
		legacy    bool
		raw       bool
		wantTaken bool
	}{
		{name: "legacy handle without claim", legacy: true, wantTaken: true},
		{name: "raw claim only", raw: true, wantTaken: true},
		{name: "unused handle"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := testDB(t)
			repo := NewBridgedActors(db)
			ctx := context.Background()
			if tc.legacy || tc.raw {
				ownerHandle := "owner.lemmy-world.tidepool.test"
				if tc.legacy {
					ownerHandle = "alice.lemmy-world.tidepool.test"
				}
				owner, err := repo.UpsertActor(ctx, claimActor(testAPActorID, testDID, ownerHandle))
				require.NoError(t, err)
				if tc.legacy {
					_, err = db.Exec(`DELETE FROM handle_claims WHERE handle = $1`, "alice.lemmy-world.tidepool.test")
					require.NoError(t, err)
					require.Empty(t, claimsFor(t, db, "alice.lemmy-world.tidepool.test"))
				} else {
					rawClaim(t, db, "alice.lemmy-world.tidepool.test", owner.ID)
					require.Equal(t, []int64{owner.ID}, claimsFor(t, db, "alice.lemmy-world.tidepool.test"))
				}
			}
			taken, err := repo.HandleTaken(ctx, "alice.lemmy-world.tidepool.test")
			require.NoError(t, err)
			require.Equal(t, tc.wantTaken, taken)
		})
	}
}
