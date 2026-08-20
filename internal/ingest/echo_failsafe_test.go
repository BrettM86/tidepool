package ingest

import (
	"context"
	stderrors "errors"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/echo"
	"tidepool/internal/errors"
)

// THE DISPATCHER'S HALF OF THE FAIL-SAFE DIRECTION.
//
// internal/echo pins that a store failure surfaces as a retryable error, and
// internal/votes pins what its probe does with one. The dispatcher's own
// contract — what happens to the EVENT when the classification cannot be made —
// was the asymmetry: only an injectable classifier can exercise it, and until
// EchoClassifier became an interface there was no way in.
//
// Both wrong answers are available and both are permanent:
//
//   - treat the failure as "not ours" and the echo re-materializes (a
//     duplicate, recoverable);
//   - treat it as "ours" and genuine Lemmy content is dropped for good.
//
// The only honest answer is neither: leave the event alone and try again. So
// the event must NOT be poisoned, must NOT be marked processed, and the failing
// attempt must leave no partial work behind — because a retry that finds
// half-applied state is not a retry.

// failingClassifier is a classifier that cannot answer.
type failingClassifier struct{ err error }

func (f failingClassifier) Classify(context.Context, *ap.Object) (echo.Identity, error) {
	return echo.Identity{Class: echo.ClassNone}, f.err
}

// swapClassifier rebuilds the dispatcher (and the queue drain() pumps) with a
// different echo classifier, mirroring swapHandlerStores.
//
// The retry schedule is deliberately SLOW, not fast. drain() pumps until the
// queue is empty, so a short backoff lets one drain re-claim the same event
// until it hits the attempt cap and poisons — which is the queue's correct
// universal rule for a retryable error (exempting the classifier would let a
// permanently failing one spin forever), but it would make this test a race
// between the backoff and the drain loop. An hour of backoff bounds the failing
// phase to exactly ONE attempt, and the redelivery is brought forward
// explicitly below. MaxAttempts is raised as a second, independent guard: even
// if someone later shortens the delay, the cap is out of reach.
func (h *harness) swapClassifier(classifier EchoClassifier) {
	h.t.Helper()
	handler, err := NewHandler(HandlerOptions{
		Materializer:   h.mat,
		Fetcher:        h.client,
		Objects:        h.objects,
		Actors:         h.actors,
		Communities:    h.communities,
		Tombstones:     h.tombstones,
		Records:        h.manager,
		Votes:          h.votes,
		Backfill:       h.backfills,
		Echo:           classifier,
		ServiceActorID: h.service.ID,
		Logger:         slog.New(slog.NewTextHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	require.NoError(h.t, err)
	h.handler = handler
	queue, err := NewQueue(QueueOptions{
		Events:         h.events,
		Processor:      handler,
		Workers:        1,
		MaxAttempts:    20,
		RetryBaseDelay: time.Hour,
		Lease:          time.Minute,
	})
	require.NoError(h.t, err)
	h.queue = queue
}

// TestEchoClassificationFailureRetriesAndRecovers: an announced activity whose
// classification fails is left for the next attempt, and the next attempt
// completes it.
func TestEchoClassificationFailureRetriesAndRecovers(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()

	boom := stderrors.New("connection reset by peer")
	h.swapClassifier(failingClassifier{err: boom})
	dropsBefore := dropSnapshot()
	opsBefore := len(h.firehoseOps())

	announce := loadFixture(t, "announce_create_page_lemmy_world.json")
	announceID := announce["id"].(string)
	require.Equal(t, http.StatusAccepted, h.deliver(group, announce))
	h.drain()

	// The event survives, unfinished.
	event, err := h.events.GetEvent(ctx, announceID)
	require.NoError(t, err)
	assert.Nil(t, event.FailedAt,
		"a classification that could not be MADE is not a verdict about the activity: "+
			"poisoning the event throws away genuine content over a database hiccup")
	assert.Nil(t, event.ProcessedAt,
		"and it must not be marked processed either — nothing was decided")
	assert.Equal(t, 1, event.Attempts,
		"exactly one attempt: the backoff parks the event rather than spinning it against a "+
			"store that is still down")
	assert.Contains(t, event.Error, boom.Error(),
		"the transient cause is kept on the row, or the retry is undiagnosable")

	// The failing attempt left NOTHING behind.
	_, err = h.objects.GetByAPID(ctx, pageID)
	assert.True(t, errors.IsNotFound(err),
		"no mapping may be written on a failed classification: the guard runs BEFORE "+
			"materialization precisely so a retry starts from a clean slate (err=%v)", err)
	tombstoned, err := h.tombstones.ExistsFor(ctx, pageID, groupID)
	require.NoError(t, err)
	assert.False(t, tombstoned, "and no tombstone marker")
	assert.Equal(t, opsBefore, len(h.firehoseOps()), "and no commit")
	for _, class := range echoClasses {
		assert.Equal(t, dropsBefore[class], echo.Drops(class),
			"and no drop counter (%s): a failure is not a suppression, and counting it as "+
				"one would hide the outage inside the metric that reports on the guard", class)
	}

	// The retry is the whole point: with the classifier healthy again, the SAME
	// event completes. A contract that only says "do not poison" would be
	// satisfied by an event that never drains.
	//
	// The backoff window belongs to the queue, so rather than sleep through it,
	// bring the redelivery forward — the clock is not what is under test, the
	// retry is. (CURRENT_TIMESTAMP, because the claim predicate reads the
	// DATABASE clock, not the process's.)
	_, err = h.db.ExecContext(ctx,
		`UPDATE inbox_events SET next_attempt_at = CURRENT_TIMESTAMP WHERE activity_id = $1`,
		announceID)
	require.NoError(t, err)

	h.swapClassifier(h.classifier)
	h.drain()

	event, err = h.events.GetEvent(ctx, announceID)
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt,
		"the redelivery completes: the event was PARKED by the failure, not lost to it")
	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err,
		"and the genuine Lemmy post finally materializes — which is what makes the retry "+
			"the recoverable direction")
	assert.False(t, mapping.IsDeleted())
}
