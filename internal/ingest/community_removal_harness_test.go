package ingest

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/labels"
	"tidepool/internal/labels/labelstest"
	"tidepool/internal/store"
)

// Shared fixtures for the bridged community removal tests: a bare-DID
// `!takedown` on a bridged community's DID removes the community.

const (
	// communityTakedown is the label value that removes a community.
	communityTakedown = "!takedown"

	// removedGroupID is the community the tests remove, on the fixture host.
	removedGroupID  = "https://lemmy.world/c/removed"
	removedUsername = "removed"
	// controlGroupID is a community the same follow list names that nothing
	// removes.
	controlGroupID = "https://lemmy.world/c/control"
)

// newCommunityLabelCommitter commits labels through a store running only
// admin's community applier, as the label consumer would.
func newCommunityLabelCommitter(t *testing.T, h *harness, admin *Admin) *labelCommitter {
	t.Helper()
	return newLabelCommitterWith(t, h, admin.CommunityLabelApplier())
}

// removeCommunity commits the community !takedown on did.
func (c *labelCommitter) removeCommunity(t *testing.T, did string) {
	t.Helper()
	c.commit(t, did, communityTakedown, false)
}

// restoreCommunity commits the negation of the community !takedown on did.
func (c *labelCommitter) restoreCommunity(t *testing.T, did string) {
	t.Helper()
	c.commit(t, did, communityTakedown, true)
}

// communityDID reads the DID of the community stored under apGroupID.
func (h *harness) communityDID(apGroupID string) string {
	h.t.Helper()
	community, err := h.communities.GetByAPGroupID(context.Background(), apGroupID)
	require.NoError(h.t, err)
	require.NotEmpty(h.t, community.DID)
	return community.DID
}

// followStateOf reads the follow state of the community stored under
// apGroupID.
func (h *harness) followStateOf(apGroupID string) store.FollowState {
	h.t.Helper()
	community, err := h.communities.GetByAPGroupID(context.Background(), apGroupID)
	require.NoError(h.t, err)
	return community.FollowState
}

// deliveredSince gives the groups named by the Follows and the Undo{Follow}s
// the fake Lemmy inbox received after mark (a deliveryCount), each in delivery order.
func (h *harness) deliveredSince(mark int) (follows, undos []string) {
	h.t.Helper()
	h.mu.Lock()
	delivered := slices.Clone(h.inboxLog[mark:])
	h.mu.Unlock()
	for _, raw := range delivered {
		activity, err := ap.ParseObject(raw)
		require.NoError(h.t, err)
		require.NotNil(h.t, activity.Object)
		switch activity.Type {
		case ap.TypeFollow:
			follows = append(follows, activity.Object.ID)
		case ap.TypeUndo:
			require.NotNil(h.t, activity.Object.Object)
			undos = append(undos, activity.Object.Object.ID)
		default:
			require.Failf(h.t, "unexpected delivery", "%s", raw)
		}
	}
	return follows, undos
}

// failLabelCursorUpdates makes every update of the label cursor raise until
// the returned restore runs (also run at cleanup), so a label commit fails
// after its appliers ran and its transaction rolls back.
func failLabelCursorUpdates(t *testing.T, db *sql.DB) (restore func()) {
	t.Helper()
	restore = func() {
		_, err := db.Exec(`
			DROP TRIGGER IF EXISTS community_removal_test_fail_cursor ON coves_label_cursor;
			DROP FUNCTION IF EXISTS community_removal_test_fail_cursor();`)
		require.NoError(t, err)
	}
	restore()
	t.Cleanup(restore)
	_, err := db.Exec(`
		CREATE FUNCTION community_removal_test_fail_cursor() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			RAISE EXCEPTION 'community removal test: label cursor update refused';
		END $$;
		CREATE TRIGGER community_removal_test_fail_cursor BEFORE UPDATE ON coves_label_cursor
			FOR EACH ROW EXECUTE FUNCTION community_removal_test_fail_cursor();`)
	require.NoError(t, err)
	return restore
}

// staticLabelerResolver resolves the labeler to a labelstest server and key.
type staticLabelerResolver struct {
	endpoint string
	key      atcrypto.PublicKey
}

func (r staticLabelerResolver) Resolve(_ context.Context, did string) (labels.Labeler, error) {
	return labels.Labeler{DID: did, Endpoint: r.endpoint, Key: r.key}, nil
}

// runLabelConsumer starts a labels.Consumer on labelStore against server,
// waits until the server sees its subscription at cursor (the consumer
// activates the stored labels before it subscribes), then stops it.
func runLabelConsumer(t *testing.T, labelStore labels.Store, server *labelstest.Server,
	key atcrypto.PrivateKey, cursor int64) {
	t.Helper()
	consumer, err := labels.NewConsumer(labels.ConsumerOptions{
		LabelerDID: labelSourceDID,
		Resolver:   staticLabelerResolver{endpoint: server.URL, key: labelstest.PublicKey(t, key)},
		Store:      labelStore,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		MinBackoff: 10 * time.Millisecond,
		MaxBackoff: 50 * time.Millisecond,
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		consumer.Run(ctx)
	}()
	defer func() {
		cancel()
		select {
		case <-returned:
		case <-time.After(5 * time.Second):
			t.Fatal("the label consumer did not stop within 5s")
		}
	}()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case seen := <-server.Connections():
			if seen == cursor {
				return
			}
		case <-returned:
			t.Fatal("the label consumer returned before subscribing")
		case <-deadline:
			t.Fatalf("the label server saw no subscription at cursor %d within 5s", cursor)
		}
	}
}

// acceptLatestFollow delivers group's Accept of the latest Follow the fake
// Lemmy inbox received for it, then drains the queue.
func (h *harness) acceptLatestFollow(group *remoteActor) {
	h.t.Helper()
	h.mu.Lock()
	delivered := slices.Clone(h.inboxLog)
	h.mu.Unlock()
	for index := len(delivered) - 1; index >= 0; index-- {
		follow, err := ap.ParseObject(delivered[index])
		require.NoError(h.t, err)
		if follow.Type != ap.TypeFollow || follow.Object == nil || follow.Object.ID != group.id {
			continue
		}
		status := h.deliver(group, map[string]any{
			"id":     fmt.Sprintf("%s/activities/accept/follow-%d", group.id, index),
			"type":   "Accept",
			"actor":  group.id,
			"object": map[string]any{"id": follow.ID, "type": "Follow", "actor": h.service.ID, "object": group.id},
		})
		require.Equal(h.t, http.StatusAccepted, status)
		h.drain()
		return
	}
	require.Failf(h.t, "no Follow to accept", "the fake Lemmy inbox received no Follow for %s", group.id)
}

// rigGroup is the signing handle of a Group the instance rig serves: every rig
// Group is published on the service actor's key.
func (r *instanceRig) rigGroup(id string) *remoteActor {
	return &remoteActor{id: id, key: r.h.service.Key}
}

// useBackfiller rebuilds the dispatcher (and the queue drain() pumps) so a
// follow Accept triggers backfill instead of the recording one.
func (h *harness) useBackfiller(backfill Backfiller) {
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
		Backfill:       backfill,
		Echo:           h.classifier,
		ServiceActorID: h.service.ID,
		Logger:         slog.New(slog.NewTextHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	require.NoError(h.t, err)
	h.handler = handler
	queue, err := NewQueue(QueueOptions{
		Events:      h.events,
		Processor:   handler,
		Workers:     1,
		MaxAttempts: 3,
		Lease:       time.Minute,
		Logger:      slog.New(slog.NewTextHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	require.NoError(h.t, err)
	h.queue = queue
}
