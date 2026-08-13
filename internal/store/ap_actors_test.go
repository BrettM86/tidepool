package store

import (
	"bytes"
	"context"
	"database/sql"
	stderrors "errors"
	"testing"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/testutil"
)

// Fixtures for the coves.social user-origin actors of task 13.
// normalized_origin is the scheme-less lowercase host — exactly what Host
// routing yields at webfinger time — while actor_id carries the full URL.
const (
	apActorOrigin       = "coves.social"
	apActorVanityOrigin = "vanity.example"
	apActorLocalPart    = "alice"
	apActorThirdDID     = "did:plc:z72i7hdynmk6r22z27h6tvur"
	apActorPublicKeyPEM = "-----BEGIN PUBLIC KEY-----\nTEST\n-----END PUBLIC KEY-----\n"
)

func apActorURL(origin, did string) string { return "https://" + origin + "/ap/actor/" + did }

// apActorsTestDB is testDB for a table that does not exist yet: the truncate
// is best-effort so the harness never fails on the missing table (that is
// what the tests themselves must report).
func apActorsTestDB(t *testing.T) *sql.DB {
	t.Helper()
	database := testutil.DB(t)
	_, _ = database.ExecContext(context.Background(), `TRUNCATE ap_actors RESTART IDENTITY`)
	return database
}

// requireAPActorsTable fails with an actionable message when migration 017
// has not created the table, so schema-level tests never pass vacuously (a
// missing table errors every statement, including the ones asserting an
// error).
func requireAPActorsTable(t *testing.T, database *sql.DB) {
	t.Helper()
	var name sql.NullString
	err := database.QueryRowContext(context.Background(),
		`SELECT to_regclass('public.ap_actors')::text`).Scan(&name)
	require.NoError(t, err)
	require.True(t, name.Valid,
		"migration 017 must create table ap_actors (did PK, kind, actor_id, "+
			"normalized_origin, local_part, rsa_key_sealed, rsa_key_version, "+
			"public_key_pem, enabled, enabled_at, disabled_at, delivery_paused, "+
			"display_name, summary, avatar_url, created_at, updated_at)")
}

func testAPActor() APActor {
	return APActor{
		DID:              testDID,
		Kind:             ActorTypePerson,
		ActorID:          apActorURL(apActorOrigin, testDID),
		NormalizedOrigin: apActorOrigin,
		LocalPart:        apActorLocalPart,
		RSAKeySealed:     []byte{0x01, 0xde, 0xad, 0xbe, 0xef},
		RSAKeyVersion:    1,
		PublicKeyPEM:     apActorPublicKeyPEM,
	}
}

func TestAPActors_CreateGetRoundTrip(t *testing.T) {
	database := apActorsTestDB(t)
	repo := NewAPActors(database)
	ctx := t.Context()

	want := testAPActor()
	created, err := repo.Create(ctx, want)
	require.NoError(t, err)
	require.NotNil(t, created, "Create must return the stored row")

	for _, actor := range []*APActor{created, mustGetByDID(t, repo, testDID)} {
		assert.Equal(t, want.DID, actor.DID)
		assert.Equal(t, ActorTypePerson, actor.Kind)
		assert.Equal(t, want.ActorID, actor.ActorID)
		assert.Equal(t, want.NormalizedOrigin, actor.NormalizedOrigin)
		assert.Equal(t, want.LocalPart, actor.LocalPart)
		assert.True(t, bytes.Equal(want.RSAKeySealed, actor.RSAKeySealed),
			"sealed key must round-trip byte for byte")
		assert.Equal(t, want.RSAKeyVersion, actor.RSAKeyVersion)
		assert.Equal(t, want.PublicKeyPEM, actor.PublicKeyPEM)

		// Creation is always enabled and unpaused: federation is default-on
		// (decision 11), and the input's zero-value lifecycle fields must
		// not be able to mint a silently-disabled actor.
		assert.True(t, actor.Enabled, "a freshly created actor federates")
		require.NotNil(t, actor.EnabledAt, "enabled_at is stamped at creation")
		assert.Nil(t, actor.DisabledAt)
		assert.False(t, actor.DeliveryPaused)

		// Profile cache starts empty; task 14 fills it.
		assert.Empty(t, actor.DisplayName)
		assert.Empty(t, actor.Summary)
		assert.Empty(t, actor.AvatarURL)

		assert.False(t, actor.CreatedAt.IsZero())
		assert.False(t, actor.UpdatedAt.IsZero())
	}

	// The webfinger lookup key.
	byLocal, err := repo.GetByOriginLocalPart(ctx, apActorOrigin, apActorLocalPart)
	require.NoError(t, err)
	require.NotNil(t, byLocal, "GetByOriginLocalPart must find the actor")
	assert.Equal(t, testDID, byLocal.DID)

	// Misses.
	_, err = repo.GetByDID(ctx, testSecondDID)
	assert.True(t, errors.IsNotFound(err), "unknown DID must be IsNotFound, got %v", err)
	_, err = repo.GetByOriginLocalPart(ctx, apActorOrigin, "nobody")
	assert.True(t, errors.IsNotFound(err), "unknown local part must be IsNotFound, got %v", err)
	_, err = repo.GetByOriginLocalPart(ctx, apActorVanityOrigin, apActorLocalPart)
	assert.True(t, errors.IsNotFound(err),
		"the lookup is scoped to the origin: alice@coves.social must not answer for vanity.example, got %v", err)
}

func TestAPActors_UniquenessAndVanityOrigins(t *testing.T) {
	database := apActorsTestDB(t)
	repo := NewAPActors(database)
	ctx := t.Context()

	_, err := repo.Create(ctx, testAPActor())
	require.NoError(t, err)

	// (a) The same local part under the SAME origin collides.
	sameLocal := testAPActor()
	sameLocal.DID = testSecondDID
	sameLocal.ActorID = apActorURL(apActorOrigin, testSecondDID)
	_, err = repo.Create(ctx, sameLocal)
	requireConflict(t, err, "a second alice@coves.social must conflict")

	// (b) ... but the same local part under a DIFFERENT origin coexists
	// (decision 10: vanity origins are not a global namespace).
	vanity := testAPActor()
	vanity.DID = testSecondDID
	vanity.ActorID = apActorURL(apActorVanityOrigin, testSecondDID)
	vanity.NormalizedOrigin = apActorVanityOrigin
	vanityActor, err := repo.Create(ctx, vanity)
	require.NoError(t, err, "alice@vanity.example must coexist with alice@coves.social")
	require.NotNil(t, vanityActor)

	// Each resolves only under its own origin.
	first, err := repo.GetByOriginLocalPart(ctx, apActorOrigin, apActorLocalPart)
	require.NoError(t, err)
	require.NotNil(t, first)
	assert.Equal(t, testDID, first.DID)
	second, err := repo.GetByOriginLocalPart(ctx, apActorVanityOrigin, apActorLocalPart)
	require.NoError(t, err)
	require.NotNil(t, second)
	assert.Equal(t, testSecondDID, second.DID)

	// (c) A duplicate actor_id conflicts even with a fresh DID and local part.
	dupActorID := testAPActor()
	dupActorID.DID = apActorThirdDID
	dupActorID.LocalPart = "bob"
	_, err = repo.Create(ctx, dupActorID)
	requireConflict(t, err, "actor_id is globally unique")

	// (d) A duplicate DID conflicts (the primary key).
	dupDID := testAPActor()
	dupDID.ActorID = apActorURL(apActorOrigin, apActorThirdDID)
	dupDID.LocalPart = "carol"
	_, err = repo.Create(ctx, dupDID)
	requireConflict(t, err, "one actor per DID")
}

func TestAPActors_LifecycleAndProfileUpdates(t *testing.T) {
	database := apActorsTestDB(t)
	repo := NewAPActors(database)
	ctx := t.Context()

	created, err := repo.Create(ctx, testAPActor())
	require.NoError(t, err)
	require.NotNil(t, created)
	require.NotNil(t, created.EnabledAt)
	firstEnabledAt := *created.EnabledAt

	// Disable: stamped, not deleted.
	require.NoError(t, repo.SetEnabled(ctx, testDID, false))
	disabled := mustGetByDID(t, repo, testDID)
	assert.False(t, disabled.Enabled)
	require.NotNil(t, disabled.DisabledAt, "disabling stamps disabled_at")
	assert.NotNil(t, disabled.EnabledAt, "the previous enable is not erased")

	// Re-enable: clears disabled_at, re-stamps enabled_at.
	require.NoError(t, repo.SetEnabled(ctx, testDID, true))
	reEnabled := mustGetByDID(t, repo, testDID)
	assert.True(t, reEnabled.Enabled)
	assert.Nil(t, reEnabled.DisabledAt, "re-enabling clears disabled_at")
	require.NotNil(t, reEnabled.EnabledAt)
	assert.False(t, reEnabled.EnabledAt.Before(firstEnabledAt),
		"re-enabling re-stamps enabled_at")

	// Pause / unpause (the transient #account state — identity survives).
	require.NoError(t, repo.SetPaused(ctx, testDID, true))
	paused := mustGetByDID(t, repo, testDID)
	assert.True(t, paused.DeliveryPaused)
	assert.True(t, paused.Enabled, "pausing delivery must not disable the actor")
	require.NoError(t, repo.SetPaused(ctx, testDID, false))
	assert.False(t, mustGetByDID(t, repo, testDID).DeliveryPaused)

	// Profile refresh: cache fields move, identity fields do not. This is
	// the handle-change path (task 14) — the local part is FROZEN.
	before := mustGetByDID(t, repo, testDID)
	profile := APActorProfile{
		DisplayName: "Alice",
		Summary:     "posts about tide pools",
		AvatarURL:   "https://cdn.example/alice.png",
	}
	require.NoError(t, repo.UpdateProfile(ctx, testDID, profile))
	updated := mustGetByDID(t, repo, testDID)
	assert.Equal(t, profile.DisplayName, updated.DisplayName)
	assert.Equal(t, profile.Summary, updated.Summary)
	assert.Equal(t, profile.AvatarURL, updated.AvatarURL)
	assert.True(t, updated.UpdatedAt.After(before.UpdatedAt), "updated_at must advance")
	assert.Equal(t, before.LocalPart, updated.LocalPart,
		"the local part is frozen at creation: a profile refresh must never re-derive it")
	assert.Equal(t, before.ActorID, updated.ActorID)
	assert.Equal(t, before.CreatedAt, updated.CreatedAt)

	// Missing actor: every mutator reports it.
	assert.True(t, errors.IsNotFound(repo.SetEnabled(ctx, testSecondDID, false)),
		"SetEnabled on an unknown DID must be IsNotFound")
	assert.True(t, errors.IsNotFound(repo.SetPaused(ctx, testSecondDID, true)),
		"SetPaused on an unknown DID must be IsNotFound")
	assert.True(t, errors.IsNotFound(repo.UpdateProfile(ctx, testSecondDID, profile)),
		"UpdateProfile on an unknown DID must be IsNotFound")
}

// TestAPActors_KindCheckConstraint pins the CHECK at the DB level: the store
// exposes no API for group actors (RESERVED for Scope B), so the schema is
// the only thing standing between a typo and a garbage actor kind.
func TestAPActors_KindCheckConstraint(t *testing.T) {
	database := apActorsTestDB(t)
	requireAPActorsTable(t, database)
	ctx := t.Context()

	const insert = `
		INSERT INTO ap_actors (
			did, kind, actor_id, normalized_origin, local_part,
			rsa_key_sealed, rsa_key_version, public_key_pem
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

	_, err := database.ExecContext(ctx, insert,
		testDID, "bogus", apActorURL(apActorOrigin, testDID),
		apActorOrigin, apActorLocalPart, []byte{0x01}, 1, apActorPublicKeyPEM)
	require.Error(t, err, "kind must be constrained to person|group")
	var pqErr *pq.Error
	require.True(t, stderrors.As(err, &pqErr), "expected a postgres error, got %v", err)
	assert.EqualValues(t, "23514", pqErr.Code,
		"kind must be rejected by a CHECK constraint (23514), got %s: %v", pqErr.Code, err)

	// group is reserved but legal: Scope B mints them, no code path yet.
	_, err = database.ExecContext(ctx, insert,
		testSecondDID, string(ActorTypeGroup), apActorURL(apActorOrigin, testSecondDID),
		apActorOrigin, "technology", []byte{0x01}, 1, apActorPublicKeyPEM)
	require.NoError(t, err, "kind=group must be accepted (reserved for Scope B)")
}

func mustGetByDID(t *testing.T, repo APActors, did string) *APActor {
	t.Helper()
	actor, err := repo.GetByDID(context.Background(), did)
	require.NoError(t, err)
	require.NotNil(t, actor, "GetByDID(%s) must return the stored row", did)
	return actor
}

// requireConflict asserts a uniqueness violation surfaced as the store's
// mapped ConflictError (not a raw pq error leaking through).
func requireConflict(t *testing.T, err error, msg string) {
	t.Helper()
	require.Error(t, err, msg)
	assert.True(t, errors.IsAlreadyExists(err), "%s: want IsAlreadyExists, got %v", msg, err)
	var conflict errors.ConflictError
	if assert.True(t, stderrors.As(err, &conflict),
		"%s: uniqueness must map to a ConflictError, got %T (%v)", msg, err, err) {
		assert.NotEmpty(t, conflict.Field, "%s: the conflicting field must be named", msg)
	}
}
