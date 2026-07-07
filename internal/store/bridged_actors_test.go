package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

func TestBridgedActors_UpsertIsIdempotent(t *testing.T) {
	database := testDB(t)
	repo := NewBridgedActors(database)
	ctx := context.Background()

	first, err := repo.UpsertActor(ctx, testActor())
	require.NoError(t, err)
	assert.Equal(t, ConsentStateOK, first.ConsentState)
	assert.NotZero(t, first.CreatedAt)

	// Re-upserting with new non-empty values updates the mutable fields
	// and nothing else.
	updated := testActor()
	updated.Handle = "alice-renamed.lemmy-world.tidepool.example"
	updated.SigningKeyEncrypted = []byte("rotated-key-bytes")
	second, err := repo.UpsertActor(ctx, updated)
	require.NoError(t, err)

	assert.Equal(t, first.ID, second.ID, "upsert must reuse the existing row")
	assert.Equal(t, "alice-renamed.lemmy-world.tidepool.example", second.Handle)
	assert.Equal(t, []byte("rotated-key-bytes"), second.SigningKeyEncrypted)
	assert.Equal(t, first.DID, second.DID)
	assert.Equal(t, first.CreatedAt, second.CreatedAt)

	var total int
	require.NoError(t, database.QueryRow(`SELECT COUNT(*) FROM bridged_actors`).Scan(&total))
	assert.Equal(t, 1, total)
}

func TestBridgedActors_UpsertPreservesHandleAndKeyWhenOmitted(t *testing.T) {
	repo := NewBridgedActors(testDB(t))
	ctx := context.Background()

	first, err := repo.UpsertActor(ctx, testActor())
	require.NoError(t, err)
	require.NotEmpty(t, first.Handle)
	require.NotEmpty(t, first.SigningKeyEncrypted)

	// A profile refresh built purely from AP data carries no handle and no
	// key material; upserting it must not clobber the escrowed values.
	refresh := testActor()
	refresh.Handle = ""
	refresh.SigningKeyEncrypted = nil
	refreshed, err := repo.UpsertActor(ctx, refresh)
	require.NoError(t, err)

	assert.Equal(t, first.Handle, refreshed.Handle, "empty handle must not clobber the stored handle")
	assert.Equal(t, first.SigningKeyEncrypted, refreshed.SigningKeyEncrypted,
		"nil signing key must not clobber the escrowed key")
}

func TestBridgedActors_UpsertOnDeletedActorIsFrozen(t *testing.T) {
	repo := NewBridgedActors(testDB(t))
	ctx := context.Background()

	original, err := repo.UpsertActor(ctx, testActor())
	require.NoError(t, err)
	require.NoError(t, repo.SetConsentState(ctx, testAPActorID, ConsentStateDeleted))

	// A tombstoned actor is frozen: the upsert modifies nothing — not even
	// the normally-mutable handle and key — and returns the stored row.
	attempt := testActor()
	attempt.Handle = "necromancer.lemmy-world.tidepool.example"
	attempt.SigningKeyEncrypted = []byte("fresh-key-bytes")
	frozen, err := repo.UpsertActor(ctx, attempt)
	require.NoError(t, err)

	assert.Equal(t, original.ID, frozen.ID)
	assert.Equal(t, ConsentStateDeleted, frozen.ConsentState, "consent must stay deleted")
	assert.Equal(t, original.Handle, frozen.Handle, "handle must stay frozen on a deleted actor")
	assert.Equal(t, original.SigningKeyEncrypted, frozen.SigningKeyEncrypted,
		"signing key must stay frozen on a deleted actor")

	stored, err := repo.GetByAPActorID(ctx, testAPActorID)
	require.NoError(t, err)
	assert.Equal(t, ConsentStateDeleted, stored.ConsentState)
	assert.Equal(t, original.Handle, stored.Handle)
	assert.Equal(t, original.SigningKeyEncrypted, stored.SigningKeyEncrypted)
}

func TestBridgedActors_UpsertPreservesConsentState(t *testing.T) {
	repo := NewBridgedActors(testDB(t))
	ctx := context.Background()

	_, err := repo.UpsertActor(ctx, testActor())
	require.NoError(t, err)
	require.NoError(t, repo.SetConsentState(ctx, testAPActorID, ConsentStateNoBridge))

	// A profile refresh (upsert) must not silently flip consent back,
	// even though the incoming actor states ConsentStateOK.
	refreshed, err := repo.UpsertActor(ctx, testActor())
	require.NoError(t, err)
	assert.Equal(t, ConsentStateNoBridge, refreshed.ConsentState)
}

func TestBridgedActors_UpsertRejectsIdentityDrift(t *testing.T) {
	repo := NewBridgedActors(testDB(t))
	ctx := context.Background()

	original, err := repo.UpsertActor(ctx, testActor())
	require.NoError(t, err)

	// Same AP actor id arriving with a different DID must be a conflict,
	// not a silent success that keeps the old DID.
	drifted := testActor()
	drifted.DID = testSecondDID
	_, err = repo.UpsertActor(ctx, drifted)
	assert.True(t, errors.IsAlreadyExists(err), "changed DID must conflict, got %v", err)

	// Same for a changed actor type.
	retyped := testActor()
	retyped.ActorType = ActorTypeGroup
	_, err = repo.UpsertActor(ctx, retyped)
	assert.True(t, errors.IsAlreadyExists(err), "changed actor_type must conflict, got %v", err)

	// The stored row is untouched by the rejected upserts.
	stored, err := repo.GetByAPActorID(ctx, testAPActorID)
	require.NoError(t, err)
	assert.Equal(t, original.DID, stored.DID)
	assert.Equal(t, original.ActorType, stored.ActorType)
}

func TestBridgedActors_Get(t *testing.T) {
	repo := NewBridgedActors(testDB(t))
	ctx := context.Background()

	stored, err := repo.UpsertActor(ctx, testActor())
	require.NoError(t, err)

	byAPID, err := repo.GetByAPActorID(ctx, testAPActorID)
	require.NoError(t, err)
	assert.Equal(t, stored.ID, byAPID.ID)

	byDID, err := repo.GetByDID(ctx, testDID)
	require.NoError(t, err)
	assert.Equal(t, stored.ID, byDID.ID)

	_, err = repo.GetByAPActorID(ctx, "https://lemmy.world/u/nobody")
	assert.True(t, errors.IsNotFound(err), "expected IsNotFound, got %v", err)
	_, err = repo.GetByDID(ctx, testSecondDID)
	assert.True(t, errors.IsNotFound(err), "expected IsNotFound, got %v", err)
}

func TestBridgedActors_GetByHandle(t *testing.T) {
	repo := NewBridgedActors(testDB(t))
	ctx := context.Background()

	stored, err := repo.UpsertActor(ctx, testActor())
	require.NoError(t, err)

	byHandle, err := repo.GetByHandle(ctx, stored.Handle)
	require.NoError(t, err)
	assert.Equal(t, stored.ID, byHandle.ID)

	_, err = repo.GetByHandle(ctx, "nobody.lemmy-world.tidepool.example")
	assert.True(t, errors.IsNotFound(err), "expected IsNotFound, got %v", err)

	// Actors without a handle yet (NULL) must not match anything.
	unhandled := testActor()
	unhandled.APActorID = "https://lemmy.world/u/bob"
	unhandled.DID = testSecondDID
	unhandled.Handle = ""
	_, err = repo.UpsertActor(ctx, unhandled)
	require.NoError(t, err)
	_, err = repo.GetByHandle(ctx, "")
	assert.True(t, errors.IsNotFound(err), "empty handle must not match NULL-handle rows")
}

func TestBridgedActors_ConsentStateTransitions(t *testing.T) {
	repo := NewBridgedActors(testDB(t))
	ctx := context.Background()

	_, err := repo.UpsertActor(ctx, testActor())
	require.NoError(t, err)

	// ok -> nobridge (actor added #nobridge to their bio) and back
	// (they removed it).
	require.NoError(t, repo.SetConsentState(ctx, testAPActorID, ConsentStateNoBridge))
	actor, err := repo.GetByAPActorID(ctx, testAPActorID)
	require.NoError(t, err)
	assert.Equal(t, ConsentStateNoBridge, actor.ConsentState)

	require.NoError(t, repo.SetConsentState(ctx, testAPActorID, ConsentStateOK))
	actor, err = repo.GetByAPActorID(ctx, testAPActorID)
	require.NoError(t, err)
	assert.Equal(t, ConsentStateOK, actor.ConsentState)

	// Delete(Actor) tombstones; deleting twice is idempotent.
	require.NoError(t, repo.SetConsentState(ctx, testAPActorID, ConsentStateDeleted))
	require.NoError(t, repo.SetConsentState(ctx, testAPActorID, ConsentStateDeleted))

	// Deleted is terminal.
	err = repo.SetConsentState(ctx, testAPActorID, ConsentStateOK)
	assert.True(t, errors.IsValidation(err), "leaving deleted must fail validation, got %v", err)
	err = repo.SetConsentState(ctx, testAPActorID, ConsentStateNoBridge)
	assert.True(t, errors.IsValidation(err), "leaving deleted must fail validation, got %v", err)

	actor, err = repo.GetByAPActorID(ctx, testAPActorID)
	require.NoError(t, err)
	assert.Equal(t, ConsentStateDeleted, actor.ConsentState)

	// Unknown states and unknown actors are rejected.
	err = repo.SetConsentState(ctx, testAPActorID, ConsentState("banished"))
	assert.True(t, errors.IsValidation(err), "expected validation error, got %v", err)
	err = repo.SetConsentState(ctx, "https://lemmy.world/u/nobody", ConsentStateOK)
	assert.True(t, errors.IsNotFound(err), "expected IsNotFound, got %v", err)
}

func TestBridgedActors_MarkProfileSynced(t *testing.T) {
	repo := NewBridgedActors(testDB(t))
	ctx := context.Background()

	stored, err := repo.UpsertActor(ctx, testActor())
	require.NoError(t, err)
	assert.Nil(t, stored.ProfileSyncedAt)

	syncedAt := time.Date(2026, 6, 1, 8, 30, 0, 0, time.UTC)
	require.NoError(t, repo.MarkProfileSynced(ctx, testAPActorID, syncedAt))

	actor, err := repo.GetByAPActorID(ctx, testAPActorID)
	require.NoError(t, err)
	require.NotNil(t, actor.ProfileSyncedAt)
	assert.True(t, actor.ProfileSyncedAt.Equal(syncedAt))

	err = repo.MarkProfileSynced(ctx, "https://lemmy.world/u/nobody", syncedAt)
	assert.True(t, errors.IsNotFound(err), "expected IsNotFound, got %v", err)
}

// TestBridgedActors_UpsertValidation is pure input validation: it never
// touches postgres, so it runs without TIDEPOOL_TEST_DATABASE_URL.
func TestBridgedActors_UpsertValidation(t *testing.T) {
	repo := NewBridgedActors(nil)
	ctx := context.Background()

	cases := []struct {
		name   string
		mutate func(*BridgedActor)
	}{
		{"empty ap_actor_id", func(a *BridgedActor) { a.APActorID = "" }},
		{"invalid actor_type", func(a *BridgedActor) { a.ActorType = "service" }},
		{"invalid did", func(a *BridgedActor) { a.DID = "not-a-did" }},
		{"invalid handle", func(a *BridgedActor) { a.Handle = "no spaces allowed" }},
		{"invalid consent_state", func(a *BridgedActor) { a.ConsentState = "banished" }},
		// Consent must never default: the zero value failing open to
		// "consented" would be a consent bug.
		{"empty consent_state", func(a *BridgedActor) { a.ConsentState = "" }},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			actor := testActor()
			testCase.mutate(&actor)
			_, err := repo.UpsertActor(ctx, actor)
			assert.True(t, errors.IsValidation(err), "expected validation error, got %v", err)
		})
	}
}

func TestBridgedActors_DIDCollisionIsConflict(t *testing.T) {
	repo := NewBridgedActors(testDB(t))
	ctx := context.Background()

	_, err := repo.UpsertActor(ctx, testActor())
	require.NoError(t, err)

	// A different AP actor must not claim the same DID.
	collision := testActor()
	collision.APActorID = "https://lemmy.world/u/mallory"
	collision.Handle = "mallory.lemmy-world.tidepool.example"
	_, err = repo.UpsertActor(ctx, collision)
	assert.True(t, errors.IsAlreadyExists(err), "expected IsAlreadyExists, got %v", err)
}

func TestBridgedActors_HandleCollisionIsConflict(t *testing.T) {
	repo := NewBridgedActors(testDB(t))
	ctx := context.Background()

	_, err := repo.UpsertActor(ctx, testActor())
	require.NoError(t, err)

	// atproto handles are 1:1 with DIDs: a different actor (different AP
	// id, different DID) must not claim the same handle.
	collision := testActor()
	collision.APActorID = "https://lemmy.world/u/mallory"
	collision.DID = testSecondDID
	_, err = repo.UpsertActor(ctx, collision)
	assert.True(t, errors.IsAlreadyExists(err), "expected IsAlreadyExists, got %v", err)

	// Unassigned handles (NULL) may repeat freely.
	noHandle := testActor()
	noHandle.APActorID = "https://lemmy.world/u/newcomer"
	noHandle.DID = testSecondDID
	noHandle.Handle = ""
	_, err = repo.UpsertActor(ctx, noHandle)
	assert.NoError(t, err, "actors without handles must not collide")
}
