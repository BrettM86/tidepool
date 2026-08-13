package outbound

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// Second-opinion H3: a consent-blocked CREATE must cancel ONLY the create, not
// sweep away the actor's pending retractions (Delete/Undo). A blanket
// CancelForActor on consent-block would silently drop the very take-downs an
// opted-out user relies on.

func TestWorker_ConsentBlockDoesNotCancelPendingRetractions(t *testing.T) {
	conn := workerTestDB(t)
	seedWorkerActor(t, conn, false, false) // disabled: outward work is consent-blocked

	// Three pending deliveries for the same actor on the same serial line, the
	// Create enqueued first so it is the claimed head.
	createID := seedDelivery(t, conn, "Create", "", createPayload("c"))
	deleteID := seedDelivery(t, conn, "Delete", "", createPayload("d"))
	undoID := seedDelivery(t, conn, "Undo", "", createPayload("u"))

	sender := &fakeSender{}
	w := newWorker(t, conn, sender, nil)

	worked, err := w.DeliverNext(context.Background())
	require.NoError(t, err)
	assert.True(t, worked)

	assert.Equal(t, store.DeliveryStateCancelled, getDelivery(t, conn, createID).State,
		"the consent-blocked create is cancelled")
	assert.Equal(t, store.DeliveryStatePending, getDelivery(t, conn, deleteID).State,
		"a pending DELETE must survive a create's consent block — a retraction is exempt from consent")
	assert.Equal(t, store.DeliveryStatePending, getDelivery(t, conn, undoID).State,
		"a pending UNDO must survive too — cancelling it would strand a vote retraction")

	assert.Zero(t, sender.count(), "the blocked create POSTs nothing")
}
