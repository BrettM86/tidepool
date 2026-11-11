package ingest

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/store"
)

// Bridged community removal racing the follow paths: an Accept, the follow
// retrier, a manual subscribe, an unsubscribe and a running backfill walk,
// each interleaved with the label's commit.

// labelCommitHoldKey is the advisory lock holdLabelCommits holds; distinct from
// testutil's process lock and the repo commit lock.
const labelCommitHoldKey int64 = 0x636f6d6d686c64 // "commhld"

// holdLabelCommits makes every label commit wait, after its appliers ran and
// before its cursor update, until the returned release runs (also run at
// cleanup). A held commit keeps every row lock its appliers took.
func holdLabelCommits(t *testing.T, db *sql.DB) (release func()) {
	t.Helper()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, labelCommitHoldKey)
	require.NoError(t, err)
	drop := func() {
		_, err := db.Exec(`
			DROP TRIGGER IF EXISTS community_removal_test_hold_cursor ON coves_label_cursor;
			DROP FUNCTION IF EXISTS community_removal_test_hold_cursor();`)
		require.NoError(t, err)
	}
	drop()
	_, err = db.Exec(fmt.Sprintf(`
		CREATE FUNCTION community_removal_test_hold_cursor() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			PERFORM pg_advisory_xact_lock(%d);
			RETURN NEW;
		END $$;
		CREATE TRIGGER community_removal_test_hold_cursor BEFORE UPDATE ON coves_label_cursor
			FOR EACH ROW EXECUTE FUNCTION community_removal_test_hold_cursor();`, labelCommitHoldKey))
	require.NoError(t, err)
	var once sync.Once
	release = func() {
		once.Do(func() {
			_, err := conn.ExecContext(ctx, `SELECT pg_advisory_unlock($1)`, labelCommitHoldKey)
			require.NoError(t, err)
			require.NoError(t, conn.Close())
			drop()
		})
	}
	t.Cleanup(release)
	return release
}

// awaitLockWaiters polls until at least want backends of this database wait
// on a lock of the given wait events.
func awaitLockWaiters(t *testing.T, db *sql.DB, want int, events ...string) {
	t.Helper()
	deadline := time.Now().Add(stallDeadline)
	for {
		var waiting int
		require.NoError(t, db.QueryRow(`
			SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND wait_event_type = 'Lock' AND wait_event = ANY($1)`,
			events).Scan(&waiting))
		if waiting >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d backends wait on %v, want %d", waiting, events, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// lastDeliveredType is the activity type of the latest POST the fake Lemmy
// inbox received.
func (h *harness) lastDeliveredType() string {
	h.t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	require.NotEmpty(h.t, h.inboxLog)
	activity, err := ap.ParseObject(h.inboxLog[len(h.inboxLog)-1])
	require.NoError(h.t, err)
	return activity.Type
}

// latestFollowID is the id of the latest Follow the fake Lemmy inbox received
// for groupIRI.
func (h *harness) latestFollowID(groupIRI string) string {
	h.t.Helper()
	h.mu.Lock()
	delivered := slices.Clone(h.inboxLog)
	h.mu.Unlock()
	for index := len(delivered) - 1; index >= 0; index-- {
		follow, err := ap.ParseObject(delivered[index])
		require.NoError(h.t, err)
		if follow.Type == ap.TypeFollow && follow.Object != nil && follow.Object.ID == groupIRI {
			return follow.ID
		}
	}
	require.Failf(h.t, "no Follow", "the fake Lemmy inbox received no Follow for %s", groupIRI)
	return ""
}

// newTestFollowRetrier is a FollowRetrier over client whose threshold no fresh
// send passes.
func (h *harness) newTestFollowRetrier(client FollowClient) *FollowRetrier {
	h.t.Helper()
	return h.newTestFollowRetrierWithMaxAttempts(client, 5)
}

// newTestFollowRetrierWithMaxAttempts is newTestFollowRetrier with a budget of
// maxAttempts Follows per community.
func (h *harness) newTestFollowRetrierWithMaxAttempts(client FollowClient, maxAttempts int) *FollowRetrier {
	h.t.Helper()
	retrier, err := NewFollowRetrier(FollowRetrierOptions{
		Client:      client,
		Communities: h.communities,
		Service:     h.service,
		ResendAfter: time.Hour,
		MaxAttempts: maxAttempts,
	})
	require.NoError(h.t, err)
	return retrier
}

// TestCommunityRemoval_AcceptDuringTheRemovalLeavesTheCommunityUnfollowed:
// C is pending. The label's transaction has removed C and holds C's row when
// C's Accept is processed: the Accept read C pending before the removal
// committed. Once the removal commits, C stays unfollowed and the Accept
// triggers no backfill.
func TestCommunityRemoval_AcceptDuringTheRemovalLeavesTheCommunityUnfollowed(t *testing.T) {
	h := newHarness(t)
	group := h.serveCommunityGroup(removedGroupID, removedUsername)
	rec := h.adminRequest(http.MethodPost, "/admin/communities", map[string]any{"community": removedGroupID})
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	followID := h.latestFollowID(removedGroupID)
	communityDID := h.communityDID(removedGroupID)
	committer := newCommunityLabelCommitter(t, h, h.admin)
	release := holdLabelCommits(t, h.db)

	removal := startCall("label commit", func() error {
		return committer.attempt(t, communityDID, communityTakedown, false)
	})
	awaitLockWaiters(t, h.db, 1, "advisory")
	status := h.deliver(group, map[string]any{
		"id":     removedGroupID + "/activities/accept/follow-race",
		"type":   "Accept",
		"actor":  removedGroupID,
		"object": map[string]any{"id": followID, "type": "Follow", "actor": h.service.ID, "object": removedGroupID},
	})
	require.Equal(t, http.StatusAccepted, status)
	accept := startCall("Accept", func() error {
		h.drain()
		return nil
	})
	awaitLockWaiters(t, h.db, 1, "transactionid", "tuple")
	release()

	require.NoError(t, removal.result(t))
	require.NoError(t, accept.result(t))
	assert.Equal(t, store.FollowStateNone, h.followStateOf(removedGroupID),
		"an Accept that read C pending before the removal leaves C unfollowed")
	assert.Zero(t, h.backfills.count(), "the skipped Accept triggers no backfill")
}

// raceFollowClient is the follow retrier's view of Lemmy, running a hook
// before its first FetchActor and before the first Follow it sends.
type raceFollowClient struct {
	FollowClient
	mu           sync.Mutex
	beforeFetch  func()
	beforeFollow func()
}

func (c *raceFollowClient) FetchActor(ctx context.Context, iri string) (*ap.Object, error) {
	c.mu.Lock()
	hook := c.beforeFetch
	c.beforeFetch = nil
	c.mu.Unlock()
	if hook != nil {
		hook()
	}
	return c.FollowClient.FetchActor(ctx, iri)
}

func (c *raceFollowClient) SendActivity(ctx context.Context, inboxURL string, activity any) error {
	var hook func()
	if object, ok := activity.(*ap.Object); ok && object.Type == ap.TypeFollow {
		c.mu.Lock()
		hook = c.beforeFollow
		c.beforeFollow = nil
		c.mu.Unlock()
	}
	if hook != nil {
		hook()
	}
	return c.FollowClient.SendActivity(ctx, inboxURL, activity)
}

// TestCommunityRemoval_FollowRetrierRacingTheRemoval: the retrier has claimed
// C's stale pending Follow when the removal commits. Removed before the
// retrier sends, C gets no Follow; removed while the Follow is in flight (the
// removal's Undo{Follow} can reach Lemmy first), the retrier takes its Follow
// back, so the last activity C receives is an Undo{Follow}.
func TestCommunityRemoval_FollowRetrierRacingTheRemoval(t *testing.T) {
	for _, tc := range []struct {
		name        string
		inFlight    bool
		wantFollows []string
		wantUndos   []string
	}{
		{name: "removed between the claim and the send", wantUndos: []string{removedGroupID}},
		{
			name:        "removed while the Follow is in flight",
			inFlight:    true,
			wantFollows: []string{removedGroupID},
			wantUndos:   []string{removedGroupID, removedGroupID},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			ctx := context.Background()
			h.subscribeCommunityURL(removedGroupID, removedUsername)
			_, err := h.db.ExecContext(ctx, `
				UPDATE communities
				SET follow_state = 'pending', follow_requested_at = now() - interval '1 day'
				WHERE ap_group_id = $1`, removedGroupID)
			require.NoError(t, err)
			communityDID := h.communityDID(removedGroupID)
			committer := newCommunityLabelCommitter(t, h, h.admin)
			var removalErr error
			remove := func() { removalErr = committer.attempt(t, communityDID, communityTakedown, false) }
			client := &raceFollowClient{FollowClient: h.client}
			if tc.inFlight {
				client.beforeFollow = remove
			} else {
				client.beforeFetch = remove
			}
			mark := h.deliveryCount()

			h.newTestFollowRetrier(client).Sweep(ctx)

			require.NoError(t, removalErr)
			follows, undos := h.deliveredSince(mark)
			assert.Equal(t, tc.wantFollows, follows, "Follows delivered")
			assert.Equal(t, tc.wantUndos, undos, "Undo{Follow}s delivered")
			assert.Equal(t, ap.TypeUndo, h.lastDeliveredType(), "C's last activity takes the follow back")
			assert.Equal(t, store.FollowStateNone, h.followStateOf(removedGroupID))
		})
	}
}

// TestCommunityRemoval_UnsubscribeDuringTheRemovalIsNotUndoneByTheNegation:
// C followed by a manual subscribe is removed, then unsubscribed by
// DELETE /admin/communities while the removal stands. The negation follows C
// again neither itself nor through the follow retrier.
func TestCommunityRemoval_UnsubscribeDuringTheRemovalIsNotUndoneByTheNegation(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.subscribeCommunityURL(removedGroupID, removedUsername)
	communityDID := h.communityDID(removedGroupID)
	committer := newCommunityLabelCommitter(t, h, h.admin)
	committer.removeCommunity(t, communityDID)

	rec := h.adminRequest(http.MethodDelete, "/admin/communities", map[string]any{"community": removedGroupID})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	mark := h.deliveryCount()

	committer.commit(t, communityDID, communityTakedown, true)
	h.newTestFollowRetrier(h.client).Sweep(ctx)

	follows, undos := h.deliveredSince(mark)
	assert.Empty(t, follows, "the negation does not follow an unsubscribed community")
	assert.Empty(t, undos)
	assert.Equal(t, store.FollowStateNone, h.followStateOf(removedGroupID))
}

// TestCommunityRemoval_UndoFollowIsSentAfterTheLabelCommits: the removal's
// Undo{Follow} reaches Lemmy only once the label's transaction has committed,
// so the removal is visible from another connection when it arrives.
func TestCommunityRemoval_UndoFollowIsSentAfterTheLabelCommits(t *testing.T) {
	h := newHarness(t)
	h.subscribeCommunityURL(removedGroupID, removedUsername)
	communityDID := h.communityDID(removedGroupID)
	committer := newCommunityLabelCommitter(t, h, h.admin)
	var (
		mu                  sync.Mutex
		undos               int
		committedAtEachUndo []bool
	)
	h.mu.Lock()
	h.onInbox = func(body []byte) {
		activity, err := ap.ParseObject(body)
		if err != nil || activity.Type != ap.TypeUndo {
			return
		}
		var committed bool
		queryErr := h.db.QueryRow(`
			SELECT EXISTS (SELECT 1 FROM community_removal_suppressions
			               WHERE community_did = $1 AND cleared_at IS NULL)`, communityDID).Scan(&committed)
		mu.Lock()
		defer mu.Unlock()
		undos++
		committedAtEachUndo = append(committedAtEachUndo, queryErr == nil && committed)
	}
	h.mu.Unlock()

	committer.removeCommunity(t, communityDID)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, undos, "one Undo{Follow}")
	assert.Equal(t, []bool{true}, committedAtEachUndo, "the removal is committed when its Undo{Follow} arrives")
}

// TestCommunityRemoval_SubscribeRacingTheRemovalTakesItsFollowBack: the label
// on C commits while a manual subscribe's Follow is in flight (C is not yet
// removed when subscribe checks). Subscribe answers 409 and takes the Follow
// back: one Follow, one Undo{Follow}, C unfollowed.
func TestCommunityRemoval_SubscribeRacingTheRemovalTakesItsFollowBack(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.serveCommunityGroup(removedGroupID, removedUsername)
	committer := newCommunityLabelCommitter(t, h, h.admin)
	var (
		once       sync.Once
		removalErr error
	)
	h.mu.Lock()
	h.onInbox = func(body []byte) {
		activity, err := ap.ParseObject(body)
		if err != nil || activity.Type != ap.TypeFollow {
			return
		}
		once.Do(func() {
			community, err := h.communities.GetByAPGroupID(ctx, removedGroupID)
			if err != nil {
				removalErr = err
				return
			}
			removalErr = committer.attempt(t, community.DID, communityTakedown, false)
		})
	}
	h.mu.Unlock()
	mark := h.deliveryCount()

	rec := h.adminRequest(http.MethodPost, "/admin/communities", map[string]any{"community": removedGroupID})

	require.NoError(t, removalErr)
	assert.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	follows, undos := h.deliveredSince(mark)
	assert.Equal(t, []string{removedGroupID}, follows, "subscribe's Follow")
	assert.Equal(t, []string{removedGroupID}, undos, "subscribe takes its Follow back")
	assert.Equal(t, store.FollowStateNone, h.followStateOf(removedGroupID))
}

// labelOnCollectFetcher commits a label once the backfill walk has collected
// the outbox and before it materializes any item.
type labelOnCollectFetcher struct {
	BackfillFetcher
	outbox    string
	onCollect func()
}

func (f *labelOnCollectFetcher) FetchCollection(ctx context.Context, iri string, visit func(*ap.Object) error) error {
	err := f.BackfillFetcher.FetchCollection(ctx, iri, visit)
	if iri == f.outbox && f.onCollect != nil {
		f.onCollect()
		f.onCollect = nil
	}
	return err
}

// TestCommunityRemoval_RunningBackfillWalkStopsAtTheRemoval: the label on C
// commits while a walk of C is running. The walk materializes none of the
// items it has not reached and leaves last_backfill_at unset.
func TestCommunityRemoval_RunningBackfillWalkStopsAtTheRemoval(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.subscribeTechnology()
	serveOutboxFixtures(t, h)
	communityDID := h.communityDID(groupID)
	committer := newCommunityLabelCommitter(t, h, h.admin)
	var removalErr error
	fetcher := &labelOnCollectFetcher{
		BackfillFetcher: h.client,
		outbox:          "https://lemmy.world/c/technology/outbox",
		onCollect: func() {
			removalErr = committer.attempt(t, communityDID, communityTakedown, false)
		},
	}
	options := h.backfillOptions(h.mat, 10)
	options.Fetcher = fetcher
	backfill, err := NewBackfill(options)
	require.NoError(t, err)
	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)

	runErr := backfill.Run(ctx, community, true)

	require.NoError(t, removalErr)
	assert.NoError(t, runErr)
	for _, id := range []string{pageID, secondPageID} {
		_, err := h.objects.GetByAPID(ctx, id)
		assert.Error(t, err, "outbox post %s is not materialized after the removal", id)
	}
	assert.Nil(t, h.lastBackfillAt(groupID), "the stopped walk leaves last_backfill_at unset")
}
