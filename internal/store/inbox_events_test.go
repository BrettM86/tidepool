package store

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

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

	// The worker must hold the claim to record an outcome: the fencing token
	// is the ClaimedUntil ClaimNext stamped.
	claimed, err := repo.ClaimNext(ctx, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed.ClaimedUntil)
	token := *claimed.ClaimedUntil

	applied, err := repo.MarkProcessed(ctx, testActivityID, token)
	require.NoError(t, err)
	assert.True(t, applied, "the claim holder's outcome must be applied")

	event, err := repo.GetEvent(ctx, testActivityID)
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt)
	assert.Empty(t, event.Error)

	_, err = repo.MarkProcessed(ctx, "https://lemmy.world/activities/missing", token)
	assert.True(t, errors.IsNotFound(err), "expected IsNotFound, got %v", err)
}

func TestInboxEvents_MarkFailedThenRecovers(t *testing.T) {
	repo := NewInboxEvents(testDB(t))
	ctx := context.Background()

	_, err := repo.RecordEvent(ctx, testActivityID, "Create")
	require.NoError(t, err)

	// Claim so the event is leased (MarkFailed leaves the lease intact, so a
	// later MarkProcessed by the same worker still holds a valid token).
	claimed, err := repo.ClaimNext(ctx, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed.ClaimedUntil)
	token := *claimed.ClaimedUntil

	require.NoError(t, repo.MarkFailed(ctx, testActivityID, "parent object fetch timed out"))

	event, err := repo.GetEvent(ctx, testActivityID)
	require.NoError(t, err)
	assert.Nil(t, event.ProcessedAt, "failed events stay unprocessed for retry")
	assert.Equal(t, "parent object fetch timed out", event.Error)

	// A later successful retry clears the error.
	applied, err := repo.MarkProcessed(ctx, testActivityID, token)
	require.NoError(t, err)
	assert.True(t, applied)
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
	claimed, err := repo.ClaimNext(ctx, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed.ClaimedUntil)
	applied, err := repo.MarkProcessed(ctx, testActivityID, *claimed.ClaimedUntil)
	require.NoError(t, err)
	require.True(t, applied)

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

// TestInboxEvents_DeepBacklogDoesNotBlockOtherKeys pins the task-12 claim
// query's semantics AND its reason to exist: a community whose head is
// backing off hides its (arbitrarily deep) younger backlog without hiding
// other keys, and once the head becomes claimable it wins again in id order.
// The old NOT EXISTS scan gave the same answers in O(backlog) per claim; the
// skip-scan gives them in O(pending keys) — the answers must not move.
func TestInboxEvents_DeepBacklogDoesNotBlockOtherKeys(t *testing.T) {
	database := testDB(t)
	repo := NewInboxEvents(database)
	ctx := context.Background()

	const backlog = 500
	const bigKey = "https://lemmy.world/c/big"
	for i := 0; i < backlog; i++ {
		_, err := repo.Enqueue(ctx, InboxEvent{
			ActivityID:  fmt.Sprintf("%s/big/%d", testActivityID, i),
			Type:        "Announce",
			OrderingKey: bigKey,
		})
		require.NoError(t, err)
	}
	_, err := repo.Enqueue(ctx, InboxEvent{
		ActivityID:  testActivityID + "/small",
		Type:        "Announce",
		OrderingKey: "https://lemmy.world/c/small",
	})
	require.NoError(t, err)

	// Claim the big community's head and put it into backoff — the classic
	// failing-event pileup.
	head, err := repo.ClaimNext(ctx, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, bigKey, head.OrderingKey)
	applied, err := repo.Release(ctx, head.ActivityID, "transient failure",
		time.Now().Add(time.Hour), *head.ClaimedUntil)
	require.NoError(t, err)
	require.True(t, applied)

	// The 499 younger big-community events are invisible; the other key's
	// event is the only claimable one.
	small, err := repo.ClaimNext(ctx, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, "https://lemmy.world/c/small", small.OrderingKey)

	// Nothing else claimable while big backs off and small is leased.
	_, err = repo.ClaimNext(ctx, time.Minute)
	require.Error(t, err)
	assert.True(t, errors.IsNotFound(err))

	// Head becomes due again → it is claimed before every younger sibling.
	_, err = database.ExecContext(ctx,
		`UPDATE inbox_events SET next_attempt_at = CURRENT_TIMESTAMP WHERE activity_id = $1`,
		head.ActivityID)
	require.NoError(t, err)
	again, err := repo.ClaimNext(ctx, time.Minute)
	require.NoError(t, err)
	assert.Equal(t, head.ActivityID, again.ActivityID,
		"the key's head must be re-claimed before any younger sibling")
}

// TestInboxEvents_ReleaseParkedHandsBackTheClaimAttempt: a park reschedules the
// event like Release and gives back the attempt its own claim took, so the
// earlier failure stays counted and the park costs nothing.
func TestInboxEvents_ReleaseParkedHandsBackTheClaimAttempt(t *testing.T) {
	repo := NewInboxEvents(testDB(t))
	ctx := context.Background()

	_, err := repo.RecordEvent(ctx, testActivityID, "Create")
	require.NoError(t, err)

	first, err := repo.ClaimNext(ctx, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, first.ClaimedUntil)
	applied, err := repo.Release(ctx, testActivityID, "remote 503",
		time.Now().Add(-time.Second), *first.ClaimedUntil)
	require.NoError(t, err)
	require.True(t, applied)

	second, err := repo.ClaimNext(ctx, time.Minute)
	require.NoError(t, err)
	require.Equal(t, 2, second.Attempts)
	require.NotNil(t, second.ClaimedUntil)

	nextAttempt := time.Now().Add(time.Hour).UTC()
	applied, err = repo.ReleaseParked(ctx, testActivityID, "mint rate exceeded", nextAttempt, *second.ClaimedUntil)
	require.NoError(t, err)
	assert.True(t, applied, "the claim holder parks its own event")

	event, err := repo.GetEvent(ctx, testActivityID)
	require.NoError(t, err)
	assert.Equal(t, 1, event.Attempts, "only the park's own claim is handed back")
	assert.Nil(t, event.ClaimedUntil, "the lease is cleared")
	assert.WithinDuration(t, nextAttempt, event.NextAttemptAt, time.Second)
	assert.Equal(t, "mint rate exceeded", event.Error)
	assert.Nil(t, event.ProcessedAt)
	assert.Nil(t, event.FailedAt)
}

// TestInboxEvents_ReleaseParkedRefusals: every refused park leaves the row
// exactly as it was.
func TestInboxEvents_ReleaseParkedRefusals(t *testing.T) {
	const missingActivityID = "https://lemmy.world/activities/missing"

	claim := func(t *testing.T, repo InboxEvents) *InboxEvent {
		t.Helper()
		claimed, err := repo.ClaimNext(context.Background(), time.Minute)
		require.NoError(t, err)
		require.NotNil(t, claimed.ClaimedUntil)
		return claimed
	}

	tests := []struct {
		name       string
		activityID string
		message    string
		// arrange leaves the event in the state under test and returns the
		// token the park presents.
		arrange func(t *testing.T, database *sql.DB, repo InboxEvents) time.Time
		// wantErr is nil when the park is a discarded no-op rather than an error.
		wantErr func(error) bool
	}{
		{
			name:       "stale token after a re-claim",
			activityID: testActivityID,
			message:    "mint rate exceeded",
			arrange: func(t *testing.T, database *sql.DB, repo InboxEvents) time.Time {
				staleToken := *claim(t, repo).ClaimedUntil
				_, err := database.ExecContext(context.Background(),
					`UPDATE inbox_events SET claimed_until = CURRENT_TIMESTAMP - interval '1 minute' WHERE activity_id = $1`,
					testActivityID)
				require.NoError(t, err)
				require.Equal(t, 2, claim(t, repo).Attempts)
				return staleToken
			},
		},
		{
			// MarkProcessed clears claimed_until, so only raw SQL can put a
			// current token on a processed row and leave the processed_at guard
			// as the sole refusal.
			name:       "processed event with its token still on the row",
			activityID: testActivityID,
			message:    "mint rate exceeded",
			arrange: func(t *testing.T, database *sql.DB, repo InboxEvents) time.Time {
				token := *claim(t, repo).ClaimedUntil
				_, err := database.ExecContext(context.Background(),
					`UPDATE inbox_events SET processed_at = CURRENT_TIMESTAMP WHERE activity_id = $1`,
					testActivityID)
				require.NoError(t, err)
				return token
			},
		},
		{
			name:       "missing id",
			activityID: missingActivityID,
			message:    "mint rate exceeded",
			arrange: func(t *testing.T, _ *sql.DB, repo InboxEvents) time.Time {
				return *claim(t, repo).ClaimedUntil
			},
			wantErr: errors.IsNotFound,
		},
		{
			name:       "empty message",
			activityID: testActivityID,
			message:    "",
			arrange: func(t *testing.T, _ *sql.DB, repo InboxEvents) time.Time {
				return *claim(t, repo).ClaimedUntil
			},
			wantErr: errors.IsValidation,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database := testDB(t)
			repo := NewInboxEvents(database)
			ctx := context.Background()

			_, err := repo.RecordEvent(ctx, testActivityID, "Create")
			require.NoError(t, err)
			token := test.arrange(t, database, repo)
			before, err := repo.GetEvent(ctx, testActivityID)
			require.NoError(t, err)

			applied, err := repo.ReleaseParked(ctx, test.activityID, test.message, time.Now().Add(time.Hour), token)
			if test.wantErr == nil {
				require.NoError(t, err)
			} else {
				assert.True(t, test.wantErr(err), "unexpected error: %v", err)
			}
			assert.False(t, applied, "a refused park is not applied")

			after, err := repo.GetEvent(ctx, testActivityID)
			require.NoError(t, err)
			assert.Equal(t, before, after, "a refused park writes nothing")
		})
	}
}
