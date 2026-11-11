package ingest

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/labeling"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/echo"
	"tidepool/internal/labels"
	"tidepool/internal/labels/labelstest"
	"tidepool/internal/materialize"
	"tidepool/internal/outbound"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// TestAnAnnouncedTakedownUndoOfOurOwnLeavesAModeratorRemovalInForce (B6): a
// native comment Lemmy's moderator removed is taken down and restored by
// Tidepool (Delete then Undo{Delete} through the takedowns path); when Lemmy
// announces that Undo{Delete} back, echo suppression drops it and the
// moderator's removal (object_moderation and the removal record) stands.
func TestAnAnnouncedTakedownUndoOfOurOwnLeavesAModeratorRemovalInForce(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)
	seedNativeComment(t, world)
	commentATURI, commentAPID := mtDirectReply.atURI(), mtDirectReply.apID()

	// --- Lemmy's moderator removes the comment.
	reason := "rule 3: no personal attacks"
	h.announceDeleteWithSummary(world.groupA,
		"https://lemmy.world/activities/announce/delete/td-mod-removal", commentAPID, &reason)
	before, found := removalStateFor(t, h.db, commentATURI)
	require.True(t, found && before.removed, "precondition: the moderator removed the comment")
	_, recordCIDBefore, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionRemoval, testDigestRKey(commentATURI))
	require.NoError(t, err, "precondition: the removal record stands")

	// --- The comment had reached Lemmy (accepted), so a takedown sends a Delete.
	_, err = h.db.ExecContext(ctx,
		`UPDATE outbound_objects SET accepted_at = now() WHERE at_uri = $1`, commentATURI)
	require.NoError(t, err)

	// --- Tidepool takes it down and restores it through the takedowns path.
	committer := newTakedownLabelCommitter(t, h)
	committer.commit(t, commentATURI, materialize.LabelTakedown, false)
	committer.commit(t, commentATURI, materialize.LabelTakedown, true)
	undoPayload := onlyActivityPayload(t, h.db, mtDirectReply.did, "Undo")

	// --- Lemmy announces our Undo{Delete} back.
	dropsBefore := map[echo.Class]int64{}
	for _, class := range echoClasses {
		dropsBefore[class] = echo.Drops(class)
	}
	announceID := "https://lemmy.world/activities/announce/undo/td-echo-of-ours"
	require.Equal(t, http.StatusAccepted, h.deliver(world.groupA, echoAnnounce(announceID, undoPayload)))
	h.drain()

	after, found := removalStateFor(t, h.db, commentATURI)
	assert.True(t, found && after.removed, "the moderator's removal stays in force")
	assert.Equal(t, before, after, "object_moderation is unchanged")
	_, recordCIDAfter, err := h.manager.GetRecord(ctx,
		world.communityADID, materialize.CollectionRemoval, testDigestRKey(commentATURI))
	if assert.NoError(t, err, "the removal record still stands") {
		assert.Equal(t, recordCIDBefore, recordCIDAfter, "the removal record is unchanged")
	}
	for _, class := range echoClasses {
		want := dropsBefore[class]
		if class == echo.ClassLocalActivity {
			want++
		}
		assert.Equal(t, want, echo.Drops(class),
			"the echo drops as %q: it carries the id of an activity we sent", echo.ClassLocalActivity)
	}
	event, err := h.events.GetEvent(ctx, announceID)
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt, "the echo is processed (skipped), not left pending")
	assert.Nil(t, event.FailedAt, "and never poisoned: %s", event.Error)
}

// newTakedownLabelCommitter is newLabelCommitter with the takedowns applier
// registered beside the materializer's, the way production wires the store.
func newTakedownLabelCommitter(t *testing.T, h *harness) *labelCommitter {
	t.Helper()
	testutil.Truncate(t, h.db, "coves_labels", "coves_label_cursor")
	for _, table := range []string{"outbound_takedown_sources", "outbound_takedowns"} {
		var exists bool
		require.NoError(t, h.db.QueryRow(`SELECT to_regclass($1) IS NOT NULL`, table).Scan(&exists))
		if exists {
			testutil.Truncate(t, h.db, table)
		}
	}
	enqueuer, err := outbound.NewEnqueuer(outbound.EnqueuerOptions{
		DB:         h.db,
		Translator: outbound.NewTranslator(mtUserOrigin, ""),
		Inboxes:    outbound.NewInboxResolver(h.client, time.Minute),
		Actors:     store.NewAPActors(h.db),
		UserOrigin: mtUserOrigin,
	})
	require.NoError(t, err)
	takedowns, err := outbound.NewTakedowns(outbound.TakedownsOptions{DB: h.db, Enqueuer: enqueuer})
	require.NoError(t, err)
	key := labelstest.NewKey(t)
	return &labelCommitter{
		store: labels.NewPostgresStore(h.db, h.mat.LabelApplier(), takedowns.LabelApplier()),
		sign: func(l labeling.Label) labeling.Label {
			l.SourceDID = labelSourceDID
			return labelstest.Sign(t, key, l)
		},
	}
}

// onlyActivityPayload returns the one outbound activity of kind signed by
// actorDID, decoded.
func onlyActivityPayload(t *testing.T, db *sql.DB, actorDID, kind string) map[string]any {
	t.Helper()
	rows, err := db.Query(`SELECT payload FROM outbound_activities WHERE actor_did = $1 AND kind = $2`, actorDID, kind)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var payloads [][]byte
	for rows.Next() {
		var payload []byte
		require.NoError(t, rows.Scan(&payload))
		payloads = append(payloads, payload)
	}
	require.NoError(t, rows.Err())
	require.Len(t, payloads, 1, "the takedowns path enqueued exactly one %s from %s", kind, actorDID)
	var activity map[string]any
	require.NoError(t, json.Unmarshal(payloads[0], &activity))
	return activity
}
