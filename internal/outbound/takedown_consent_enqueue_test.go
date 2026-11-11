package outbound

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/consume"
	"tidepool/internal/store"
)

// consentEnqueueLines is receivedLines with each Create/Update's title
// appended, so two edits of one object can be told apart.
func (w *takedownWorld) consentEnqueueLines(t *testing.T) []string {
	t.Helper()
	lines := []string{}
	for _, activity := range w.received() {
		line := receivedLine(t, activity.body)
		if object, ok := activity.body["object"].(map[string]any); ok {
			if kind, _ := activity.body["type"].(string); kind == "Create" || kind == "Update" {
				name, _ := object["name"].(string)
				line += " " + name
			}
		}
		lines = append(lines, line)
	}
	return lines
}

// TestTakedown_AnEditAfterTheRestoreIsHeldOnConsentLandsLast: a paused
// author's takedown restore Undo{Delete} is held on consent with the edit
// queued behind it. An edit enqueued after that hold inherits it, so the
// unpause delivers the Undo, the older edit, then the newest edit: Lemmy ends
// on the latest state. Covered both when the worker runs while the author is
// still paused (the new edit is claimed during the pause) and when it does not
// (the release appends the held rows behind whatever is pending).
func TestTakedown_AnEditAfterTheRestoreIsHeldOnConsentLandsLast(t *testing.T) {
	for _, tc := range []struct {
		name             string
		drainWhilePaused bool
	}{
		{name: "claimed while paused", drainWhilePaused: true},
		{name: "unpaused before claimed", drainWhilePaused: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newTakedownWorld(t)
			ctx := context.Background()
			w.deliverPost(t)
			w.commitLabel(t, tdPostATURI, false)
			w.drain(t)

			actors := store.NewAPActors(w.conn)
			require.NoError(t, actors.SetPaused(ctx, tdAuthorDID, true))
			w.commitLabel(t, tdPostATURI, true)
			w.enqueuePost(t, tdPost, "update", 1)
			w.drain(t)
			undo := w.deliveryOf(t, "Undo")
			require.Equal(t, "consent", w.heldReasonOf(t, undo.ActivityID),
				"precondition: the paused author's restore Undo is held on consent")
			olderID := consume.ActivityID(outUserOrigin, tdPostATURI, "update", 1)
			require.Equal(t, "consent", w.heldReasonOf(t, olderID),
				"precondition: the edit queued behind it is held with it")

			w.enqueuePost(t, tdPost, "update", 2)
			newestID := consume.ActivityID(outUserOrigin, tdPostATURI, "update", 2)
			assert.Equal(t, store.DeliveryStateHeld, w.deliveryStateOf(t, newestID),
				"the edit enqueued after the hold is held")
			assert.Equal(t, "consent", w.heldReasonOf(t, newestID), "on consent")
			if tc.drainWhilePaused {
				w.drain(t)
			}

			w.restoreConsent(t, tdAuthorDID, func(tx *sql.Tx) {
				_, err := tx.ExecContext(ctx, `UPDATE ap_actors SET delivery_paused = false WHERE did = $1`, tdAuthorDID)
				require.NoError(t, err)
			})
			w.drain(t)
			assert.Equal(t, []string{
				"Create " + tdPostAPID + " " + tdPost.title,
				"Delete " + tdPostAPID,
				"Undo{Delete} " + tdPostAPID,
				"Update " + tdPostAPID + " " + tdPost.title + " (edit 1)",
				"Update " + tdPostAPID + " " + tdPost.title + " (edit 2)",
			}, w.consentEnqueueLines(t), "the restore goes first and the newest edit lands last")
		})
	}
}
