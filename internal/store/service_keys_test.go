package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

func TestServiceKeys_CreateAndGet(t *testing.T) {
	repo := NewServiceKeys(testDB(t))
	ctx := context.Background()

	pemBytes := []byte("-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----\n")
	created, err := repo.Create(ctx, "service-actor", pemBytes)
	require.NoError(t, err)
	assert.NotZero(t, created.ID)
	assert.Equal(t, "service-actor", created.Name)
	assert.Equal(t, pemBytes, created.PrivateKeyPEM)
	assert.False(t, created.CreatedAt.IsZero())

	got, err := repo.Get(ctx, "service-actor")
	require.NoError(t, err)
	assert.Equal(t, created.ID, got.ID)
	assert.Equal(t, pemBytes, got.PrivateKeyPEM)
}

func TestServiceKeys_CreateExistingNameConflicts(t *testing.T) {
	repo := NewServiceKeys(testDB(t))
	ctx := context.Background()

	original := []byte("original-key-pem")
	_, err := repo.Create(ctx, "service-actor", original)
	require.NoError(t, err)

	// Create is create-once: the second insert must conflict and must NOT
	// overwrite the stored key (a rotated key would invalidate the actor
	// document other instances already served).
	_, err = repo.Create(ctx, "service-actor", []byte("usurper-key-pem"))
	require.Error(t, err)
	assert.True(t, errors.IsAlreadyExists(err), "second create must satisfy IsAlreadyExists, got %v", err)

	got, err := repo.Get(ctx, "service-actor")
	require.NoError(t, err)
	assert.Equal(t, original, got.PrivateKeyPEM, "losing create must not clobber the stored key")
}

func TestServiceKeys_GetMissingIsNotFound(t *testing.T) {
	repo := NewServiceKeys(testDB(t))

	_, err := repo.Get(context.Background(), "nope")
	require.Error(t, err)
	assert.True(t, errors.IsNotFound(err), "missing key must satisfy IsNotFound, got %v", err)
}

func TestServiceKeys_CreateValidation(t *testing.T) {
	repo := NewServiceKeys(testDB(t))
	ctx := context.Background()

	_, err := repo.Create(ctx, "", []byte("pem"))
	assert.True(t, errors.IsValidation(err), "empty name must fail validation, got %v", err)

	_, err = repo.Create(ctx, "service-actor", nil)
	assert.True(t, errors.IsValidation(err), "empty key must fail validation, got %v", err)
}

func TestServiceKeys_DistinctNamesCoexist(t *testing.T) {
	repo := NewServiceKeys(testDB(t))
	ctx := context.Background()

	_, err := repo.Create(ctx, "service-actor", []byte("key-a"))
	require.NoError(t, err)
	_, err = repo.Create(ctx, "some-future-key", []byte("key-b"))
	require.NoError(t, err)

	got, err := repo.Get(ctx, "some-future-key")
	require.NoError(t, err)
	assert.Equal(t, []byte("key-b"), got.PrivateKeyPEM)
}
