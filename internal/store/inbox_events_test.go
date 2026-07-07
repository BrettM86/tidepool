package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

const testActivityID = "https://lemmy.world/activities/announce/abc-123"

func TestInboxEvents_RecordEventDeduplicates(t *testing.T) {
	repo := NewInboxEvents(testDB(t))
	ctx := context.Background()

	isNew, err := repo.RecordEvent(ctx, testActivityID, "Announce")
	require.NoError(t, err)
	assert.True(t, isNew, "first delivery must be new")

	// Redelivery of the same activity id is not an error, just not new.
	isNew, err = repo.RecordEvent(ctx, testActivityID, "Announce")
	require.NoError(t, err)
	assert.False(t, isNew, "second delivery must be deduplicated")

	event, err := repo.GetEvent(ctx, testActivityID)
	require.NoError(t, err)
	assert.Equal(t, "Announce", event.Type)
	assert.NotZero(t, event.ReceivedAt)
	assert.Nil(t, event.ProcessedAt)
	assert.Empty(t, event.Error)
}

func TestInboxEvents_MarkProcessed(t *testing.T) {
	repo := NewInboxEvents(testDB(t))
	ctx := context.Background()

	_, err := repo.RecordEvent(ctx, testActivityID, "Announce")
	require.NoError(t, err)

	require.NoError(t, repo.MarkProcessed(ctx, testActivityID))

	event, err := repo.GetEvent(ctx, testActivityID)
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt)
	assert.Empty(t, event.Error)

	err = repo.MarkProcessed(ctx, "https://lemmy.world/activities/missing")
	assert.True(t, errors.IsNotFound(err), "expected IsNotFound, got %v", err)
}

func TestInboxEvents_MarkFailedThenRecovers(t *testing.T) {
	repo := NewInboxEvents(testDB(t))
	ctx := context.Background()

	_, err := repo.RecordEvent(ctx, testActivityID, "Create")
	require.NoError(t, err)

	require.NoError(t, repo.MarkFailed(ctx, testActivityID, "parent object fetch timed out"))

	event, err := repo.GetEvent(ctx, testActivityID)
	require.NoError(t, err)
	assert.Nil(t, event.ProcessedAt, "failed events stay unprocessed for retry")
	assert.Equal(t, "parent object fetch timed out", event.Error)

	// A later successful retry clears the error.
	require.NoError(t, repo.MarkProcessed(ctx, testActivityID))
	event, err = repo.GetEvent(ctx, testActivityID)
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt)
	assert.Empty(t, event.Error)

	err = repo.MarkFailed(ctx, "https://lemmy.world/activities/missing", "boom")
	assert.True(t, errors.IsNotFound(err), "expected IsNotFound, got %v", err)
}

func TestInboxEvents_MarkFailedOnProcessedEventIsNoOp(t *testing.T) {
	repo := NewInboxEvents(testDB(t))
	ctx := context.Background()

	_, err := repo.RecordEvent(ctx, testActivityID, "Create")
	require.NoError(t, err)
	require.NoError(t, repo.MarkProcessed(ctx, testActivityID))

	// A late failure report from a stale worker must not un-process an
	// event a successful retry already completed: no-op success.
	require.NoError(t, repo.MarkFailed(ctx, testActivityID, "stale worker reporting late"))

	event, err := repo.GetEvent(ctx, testActivityID)
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt, "processed event must stay processed")
	assert.Empty(t, event.Error, "late failure must not record an error on a processed event")
}

// TestInboxEvents_Validation is pure input validation: it never touches
// postgres, so it runs without TIDEPOOL_TEST_DATABASE_URL.
func TestInboxEvents_Validation(t *testing.T) {
	repo := NewInboxEvents(nil)
	ctx := context.Background()

	_, err := repo.RecordEvent(ctx, "", "Announce")
	assert.True(t, errors.IsValidation(err), "expected validation error, got %v", err)

	_, err = repo.RecordEvent(ctx, testActivityID, "")
	assert.True(t, errors.IsValidation(err), "expected validation error, got %v", err)

	err = repo.MarkFailed(ctx, testActivityID, "")
	assert.True(t, errors.IsValidation(err), "empty failure message must be rejected, got %v", err)
}

func TestInboxEvents_GetEventMissing(t *testing.T) {
	repo := NewInboxEvents(testDB(t))
	ctx := context.Background()

	_, err := repo.GetEvent(ctx, "https://lemmy.world/activities/missing")
	assert.True(t, errors.IsNotFound(err), "expected IsNotFound, got %v", err)
}
