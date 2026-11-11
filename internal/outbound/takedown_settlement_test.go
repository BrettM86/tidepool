package outbound

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/consume"
	"tidepool/internal/store"
)

// TestTakedown_AnUnsettledCreateIsDeletedOnceItSettles (B12): a native post's
// Create reached Lemmy but its local settlement had not finished when the
// record `!takedown` committed: (a) the accepted_at stamp failed and the row
// was held ledger_unsettled; (b) the POST was still in flight; (c) as (a), and
// the first settlement retry also failed to enqueue the Delete. Once the
// settlement completes, accepted_at is stamped and Lemmy has received the
// Create exactly once and then the takedown Delete; the restore sends
// Undo{Delete} and never a second Create.
func TestTakedown_AnUnsettledCreateIsDeletedOnceItSettles(t *testing.T) {
	createID := consume.ActivityID(outUserOrigin, tdPostATURI, "create", 0)

	// failTheStamp POSTs the Create with the accepted_at stamp failing, so the
	// delivery is held for settlement, then lets the stamp work again.
	failTheStamp := func(t *testing.T, w *takedownWorld) {
		drop := failWrites(t, w.conn, "tidepool_test_fail_accepted_stamp", "BEFORE UPDATE", "outbound_objects",
			"OLD.accepted_at IS NULL AND NEW.accepted_at IS NOT NULL")
		w.enqueuePostCreate(t)
		worked, err := w.worker.DeliverNext(context.Background())
		require.NoError(t, err)
		require.True(t, worked)
		drop()
		require.Equal(t, []string{"Create " + tdPostAPID}, w.receivedLines(t), "precondition: Lemmy has the Create")
		held := w.deliveryOf(t, "Create")
		require.Equal(t, store.DeliveryStatePending, held.State, "precondition: the Create is held, not terminal")
		require.Equal(t, store.DeliveryHeldForSettlement, held.LastErrorClass,
			"precondition: the Create is held for settlement")
		require.False(t, rawAccepted(t, w.conn, tdPostATURI), "precondition: the stamp did not commit")
	}

	for _, tc := range []struct {
		name string
		// takeDown leaves Lemmy holding the Create, the post's settlement
		// unfinished, and the record label committed.
		takeDown func(t *testing.T, w *takedownWorld)
	}{
		{"the accepted stamp failed", func(t *testing.T, w *takedownWorld) {
			failTheStamp(t, w)
			w.commitLabel(t, tdPostATURI, false)
		}},
		{"the POST was in flight", func(t *testing.T, w *takedownWorld) {
			w.enqueuePostCreate(t)
			w.commitLabelWhilePOSTInFlight(t, tdPostATURI)
		}},
		{"the accepted stamp failed, then the Delete enqueue failed", func(t *testing.T, w *takedownWorld) {
			failTheStamp(t, w)
			w.commitLabel(t, tdPostATURI, false)

			drop := failWrites(t, w.conn, "tidepool_test_fail_delete_enqueue", "BEFORE INSERT", "outbound_activities",
				"NEW.kind = 'Delete'")
			w.makeDue(t, createID)
			worked, err := w.worker.DeliverNext(context.Background())
			require.NoError(t, err)
			require.True(t, worked, "the settlement retry ran")
			drop()

			assert.False(t, rawAccepted(t, w.conn, tdPostATURI),
				"a settlement whose Delete enqueue failed leaves accepted_at null")
			held := w.deliveryOf(t, "Create")
			assert.Equal(t, store.DeliveryStatePending, held.State, "the Create is still held")
			assert.Equal(t, store.DeliveryHeldForSettlement, held.LastErrorClass,
				"the Create is still held for settlement")
			assert.Equal(t, []string{"Create " + tdPostAPID}, w.receivedLines(t),
				"the failed settlement retry sends nothing")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newTakedownWorld(t)
			tc.takeDown(t, w)

			w.drain(t)
			assert.True(t, rawAccepted(t, w.conn, tdPostATURI), "the settlement stamps accepted_at")
			assert.Equal(t, store.DeliveryStateDelivered, w.deliveryStateOf(t, createID),
				"the Create's delivery is settled")
			require.Equal(t, []string{"Create " + tdPostAPID, "Delete " + tdPostAPID}, w.receivedLines(t),
				"Lemmy has the Create exactly once, then the takedown Delete")
			assertTakedownDelete(t, w.received()[1])

			w.commitLabel(t, tdPostATURI, true)
			w.drain(t)
			require.Equal(t, []string{
				"Create " + tdPostAPID,
				"Delete " + tdPostAPID,
				"Undo{Delete} " + tdPostAPID,
			}, w.receivedLines(t), "the restore sends Undo{Delete} and never a second Create")
			got := w.received()
			assertTakedownUndo(t, got[2], got[1])
		})
	}
}
