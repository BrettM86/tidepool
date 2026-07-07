package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

func TestAPObjects_PutMapping_InsertAndDeriveATURI(t *testing.T) {
	repo := NewAPObjects(testDB(t))
	ctx := context.Background()

	stored, err := repo.PutMapping(ctx, testMapping())
	require.NoError(t, err)

	assert.Equal(t, testExpectedATURI, stored.ATURI, "ATURI must be derived from did/collection/rkey")
	assert.Equal(t, testAPObjectID, stored.APID)
	assert.Equal(t, testCID, stored.CID)
	assert.NotZero(t, stored.ID)
	assert.NotZero(t, stored.IndexedAt)
	assert.Nil(t, stored.DeletedAt)
	require.NotNil(t, stored.PublishedAt)
}

func TestAPObjects_PutMapping_OriginDefaultsToFediverse(t *testing.T) {
	repo := NewAPObjects(testDB(t))
	ctx := context.Background()

	// An empty origin means "fediverse" — the common case for ingested
	// content; only bridge-emitted writes state OriginBridge explicitly.
	stored, err := repo.PutMapping(ctx, testMapping())
	require.NoError(t, err)
	assert.Equal(t, OriginFediverse, stored.Origin)

	bridged := testMapping()
	bridged.APID = "https://tidepool.example/objects/echo-1"
	bridged.RKey = "3jzfcijpj2z3a"
	bridged.Origin = OriginBridge
	storedBridge, err := repo.PutMapping(ctx, bridged)
	require.NoError(t, err)
	assert.Equal(t, OriginBridge, storedBridge.Origin)

	// Origin updates on re-put like the other mutable fields.
	bridged.Origin = ""
	storedBridge, err = repo.PutMapping(ctx, bridged)
	require.NoError(t, err)
	assert.Equal(t, OriginFediverse, storedBridge.Origin, "empty origin defaults to fediverse on update too")
}

func TestAPObjects_PutMapping_UpsertIsIdempotent(t *testing.T) {
	database := testDB(t)
	repo := NewAPObjects(database)
	ctx := context.Background()

	first, err := repo.PutMapping(ctx, testMapping())
	require.NoError(t, err)

	// Re-ingesting the same AP object (now at a new record version) must
	// update in place, not create a second row.
	updated := testMapping()
	updated.CID = testUpdatedCID
	second, err := repo.PutMapping(ctx, updated)
	require.NoError(t, err)

	assert.Equal(t, first.ID, second.ID, "upsert must reuse the existing row")
	assert.Equal(t, first.ATURI, second.ATURI, "deterministic rkeys keep the at-uri stable")
	assert.Equal(t, testUpdatedCID, second.CID)
	assert.False(t, second.IndexedAt.Before(first.IndexedAt), "indexed_at must refresh on upsert")

	var total int
	require.NoError(t, database.QueryRow(`SELECT COUNT(*) FROM ap_objects`).Scan(&total))
	assert.Equal(t, 1, total, "idempotent re-ingestion must not create extra rows")
}

func TestAPObjects_GetByAPIDAndATURI(t *testing.T) {
	repo := NewAPObjects(testDB(t))
	ctx := context.Background()

	stored, err := repo.PutMapping(ctx, testMapping())
	require.NoError(t, err)

	byAPID, err := repo.GetByAPID(ctx, testAPObjectID)
	require.NoError(t, err)
	assert.Equal(t, stored.ID, byAPID.ID)

	byATURI, err := repo.GetByATURI(ctx, stored.ATURI)
	require.NoError(t, err)
	assert.Equal(t, stored.ID, byATURI.ID)

	_, err = repo.GetByAPID(ctx, "https://lemmy.world/post/does-not-exist")
	assert.True(t, errors.IsNotFound(err), "miss must satisfy IsNotFound, got %v", err)

	_, err = repo.GetByATURI(ctx, "at://did:plc:missing/social.coves.community.post/3aaaaaaaaaa2a")
	assert.True(t, errors.IsNotFound(err), "miss must satisfy IsNotFound, got %v", err)
}

func TestAPObjects_ResolveStrongRef(t *testing.T) {
	repo := NewAPObjects(testDB(t))
	ctx := context.Background()

	// Branch 1: missing. IsNotFound is the materializer's trigger to fetch
	// the ancestor chain — it must NOT read as tombstoned.
	_, _, err := repo.ResolveStrongRef(ctx, testAPObjectID)
	assert.True(t, errors.IsNotFound(err), "unmaterialized parent must be IsNotFound, got %v", err)
	assert.False(t, errors.IsTombstoned(err), "missing must not read as tombstoned")

	// Branch 2: present.
	stored, err := repo.PutMapping(ctx, testMapping())
	require.NoError(t, err)

	atURI, cid, err := repo.ResolveStrongRef(ctx, testAPObjectID)
	require.NoError(t, err)
	assert.Equal(t, stored.ATURI, atURI)
	assert.Equal(t, testCID, cid)

	// Branch 3: tombstoned. IsTombstoned tells the materializer to drop
	// the subtree; it must NOT read as not-found, which would trigger a
	// consent-violating re-fetch of deleted content.
	require.NoError(t, repo.SoftDelete(ctx, testAPObjectID))
	_, _, err = repo.ResolveStrongRef(ctx, testAPObjectID)
	assert.True(t, errors.IsTombstoned(err), "soft-deleted object must be IsTombstoned, got %v", err)
	assert.False(t, errors.IsNotFound(err), "tombstoned must not read as not-found")
}

func TestAPObjects_SoftDeleteAndRevive(t *testing.T) {
	repo := NewAPObjects(testDB(t))
	ctx := context.Background()

	_, err := repo.PutMapping(ctx, testMapping())
	require.NoError(t, err)

	require.NoError(t, repo.SoftDelete(ctx, testAPObjectID))

	// Gets still return the tombstoned row so callers can see the state.
	deleted, err := repo.GetByAPID(ctx, testAPObjectID)
	require.NoError(t, err)
	assert.True(t, deleted.IsDeleted())

	// Deleting again is an idempotent no-op that preserves the original
	// tombstone time; deleting a missing object is a not-found error.
	require.NoError(t, repo.SoftDelete(ctx, testAPObjectID))
	redeleted, err := repo.GetByAPID(ctx, testAPObjectID)
	require.NoError(t, err)
	require.NotNil(t, redeleted.DeletedAt)
	assert.True(t, redeleted.DeletedAt.Equal(*deleted.DeletedAt),
		"re-delete must not move the original tombstone time")
	err = repo.SoftDelete(ctx, "https://lemmy.world/post/never-existed")
	assert.True(t, errors.IsNotFound(err), "expected IsNotFound, got %v", err)

	// Re-materialization revives the mapping.
	revived, err := repo.PutMapping(ctx, testMapping())
	require.NoError(t, err)
	assert.False(t, revived.IsDeleted())

	_, _, err = repo.ResolveStrongRef(ctx, testAPObjectID)
	assert.NoError(t, err, "revived mapping must resolve again")
}

// TestAPObjects_PutMapping_Validation is pure input validation: it never
// touches postgres, so it runs without TIDEPOOL_TEST_DATABASE_URL.
func TestAPObjects_PutMapping_Validation(t *testing.T) {
	repo := NewAPObjects(nil)
	ctx := context.Background()

	cases := []struct {
		name   string
		mutate func(*APObjectMapping)
	}{
		{"empty ap_id", func(m *APObjectMapping) { m.APID = "" }},
		{"empty ap_type", func(m *APObjectMapping) { m.APType = "" }},
		{"empty origin_instance", func(m *APObjectMapping) { m.OriginInstance = "" }},
		{"invalid origin", func(m *APObjectMapping) { m.Origin = "mastodon" }},
		{"empty cid", func(m *APObjectMapping) { m.CID = "" }},
		{"malformed cid", func(m *APObjectMapping) { m.CID = "not a cid!" }},
		{"cidv0", func(m *APObjectMapping) { m.CID = "QmbWqxBEKC3P8tqsKc98xmWNzrzDtRLMiMPL8wBuTGsMnR" }},
		{"invalid did", func(m *APObjectMapping) { m.DID = "not-a-did" }},
		{"invalid collection", func(m *APObjectMapping) { m.Collection = "not an nsid" }},
		{"invalid rkey", func(m *APObjectMapping) { m.RKey = "has spaces!" }},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			mapping := testMapping()
			testCase.mutate(&mapping)
			_, err := repo.PutMapping(ctx, mapping)
			assert.True(t, errors.IsValidation(err), "expected validation error, got %v", err)
		})
	}
}

func TestAPObjects_PutMapping_ATURICollisionIsConflict(t *testing.T) {
	repo := NewAPObjects(testDB(t))
	ctx := context.Background()

	_, err := repo.PutMapping(ctx, testMapping())
	require.NoError(t, err)

	// A different AP object claiming the same (did, collection, rkey) —
	// and therefore the same at-uri — is a caller bug surfaced as a
	// conflict, not silently absorbed.
	collision := testMapping()
	collision.APID = "https://lemmy.world/post/67890"
	_, err = repo.PutMapping(ctx, collision)
	assert.True(t, errors.IsAlreadyExists(err), "expected IsAlreadyExists, got %v", err)
}
