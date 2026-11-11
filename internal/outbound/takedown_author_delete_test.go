package outbound

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"tidepool/internal/consume"
	"tidepool/internal/store"
)

// TestTakedown_ARestoreNeverPublishesWhatTheAuthorDeleted: work held while an
// object (or the parent it replies to) is taken down, whose author then deletes
// the object, is never delivered when the takedown lifts: from the hold on,
// Lemmy receives no Create, Update or Undo{Delete} of it, and the held row ends cancelled rather than queued.
func TestTakedown_ARestoreNeverPublishesWhatTheAuthorDeleted(t *testing.T) {
	for _, tc := range []struct {
		name string
		// hold puts the held row in place and returns its activity id and
		// the AP id of the object the author deletes.
		hold func(t *testing.T, w *takedownWorld) (heldID, objectAPID string)
		// remove is the author's delete.
		remove func(t *testing.T, w *takedownWorld)
		// lifted is the subject whose label is negated.
		lifted string
	}{
		{
			name: "a Create held before acceptance",
			hold: func(t *testing.T, w *takedownWorld) (string, string) {
				w.enqueuePostCreate(t)
				w.commitLabel(t, tdPostATURI, false)
				return consume.ActivityID(outUserOrigin, tdPostATURI, "create", 0), tdPostAPID
			},
			remove: func(t *testing.T, w *takedownWorld) { w.deletePost(t, tdPost) },
			lifted: tdPostATURI,
		},
		{
			name: "an Update held after acceptance",
			hold: func(t *testing.T, w *takedownWorld) (string, string) {
				w.deliverPost(t)
				w.commitLabel(t, tdPostATURI, false)
				w.drain(t)
				w.enqueuePost(t, tdPost, "update", 1)
				return consume.ActivityID(outUserOrigin, tdPostATURI, "update", 1), tdPostAPID
			},
			remove: func(t *testing.T, w *takedownWorld) { w.deletePost(t, tdPost) },
			lifted: tdPostATURI,
		},
		{
			name: "a reply held under a removed parent",
			hold: func(t *testing.T, w *takedownWorld) (string, string) {
				w.deliverPost(t)
				w.commitLabel(t, tdPostATURI, false)
				w.enqueueReply(t, tdReplyToPost)
				return tdReplyToPost.createID(), tdReplyToPost.apID()
			},
			remove: func(t *testing.T, w *takedownWorld) { w.deleteReply(t, tdReplyToPost) },
			lifted: tdPostATURI,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newTakedownWorld(t)
			heldID, objectAPID := tc.hold(t, w)
			w.drain(t)
			assert.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, heldID), "precondition: the work is held")
			before := len(w.received())

			tc.remove(t, w)
			w.drain(t)
			w.commitLabel(t, tc.lifted, true)
			w.drain(t)

			for _, activity := range w.receivedSince(before) {
				line := receivedLine(t, activity.body)
				assert.NotEqual(t, "Create "+objectAPID, line, "the deleted object's Create is never delivered")
				assert.NotEqual(t, "Update "+objectAPID, line, "the deleted object's Update is never delivered")
				assert.NotEqual(t, "Undo{Delete} "+objectAPID, line, "the deleted object is never restored")
			}
			assert.Equal(t, store.DeliveryStateCancelled, w.deliveryStateOf(t, heldID),
				"the held work of a deleted object is cancelled")
		})
	}
}
