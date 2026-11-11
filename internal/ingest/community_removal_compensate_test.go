package ingest

import (
	"context"
	stderrors "errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/store"
)

// The Undo{Follow} that takes back a Follow sent while a community removal
// committed: when it does not go out, the removal owes it and the follow
// retrier sends it.

// removalStore is the harness's postgres communities store with its removal
// side, for test doubles that override one removal method.
type removalStore struct {
	store.Communities
	store.CommunityRemovals
}

func newRemovalStore(h *harness) removalStore {
	return removalStore{Communities: h.communities, CommunityRemovals: h.communities.(store.CommunityRemovals)}
}

// removalReadFailingCommunities fails every removal read from the failFrom-th
// on (counting from 1).
type removalReadFailingCommunities struct {
	removalStore
	mu       sync.Mutex
	reads    int
	failFrom int
}

func (c *removalReadFailingCommunities) CommunityRemoved(ctx context.Context, did string) (bool, error) {
	c.mu.Lock()
	c.reads++
	read := c.reads
	c.mu.Unlock()
	if read >= c.failFrom {
		return false, stderrors.New("test: removal read fails")
	}
	return c.removalStore.CommunityRemoved(ctx, did)
}

// completeFailingCommunities fails every CompleteOwedUnfollow.
type completeFailingCommunities struct{ removalStore }

func (completeFailingCommunities) CompleteOwedUnfollow(context.Context, int64) (bool, error) {
	return false, stderrors.New("test: completing the owed Undo{Follow} fails")
}

// owedUnfollowCount is how many Undo{Follow}s removals owe.
func (h *harness) owedUnfollowCount() int {
	h.t.Helper()
	owed, err := h.communities.(store.CommunityRemovals).ListOwedUnfollows(context.Background())
	require.NoError(h.t, err)
	return len(owed)
}

// staleRetrierRaceCommunity subscribes C and ages its Follow so the follow
// retrier claims it.
func staleRetrierRaceCommunity(t *testing.T, h *harness) (communityDID string) {
	t.Helper()
	h.subscribeCommunityURL(removedGroupID, removedUsername)
	_, err := h.db.ExecContext(context.Background(), `
		UPDATE communities
		SET follow_state = 'pending', follow_requested_at = now() - interval '1 day'
		WHERE ap_group_id = $1`, removedGroupID)
	require.NoError(t, err)
	return h.communityDID(removedGroupID)
}

// TestCommunityRemoval_SubscribeWhoseTakeBackFailsOwesTheUndoFollow: the
// label on C commits while a subscribe's Follow is in flight, and the
// subscribe's Undo{Follow} taking it back fails. The removal owes the
// Undo{Follow}, and the follow retrier's next sweep sends it.
func TestCommunityRemoval_SubscribeWhoseTakeBackFailsOwesTheUndoFollow(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.serveCommunityGroup(removedGroupID, removedUsername)
	committer := newCommunityLabelCommitter(t, h, h.admin)
	options := h.adminOptions()
	options.Client = undoFailingClient{FollowClient: h.client}
	failing, err := NewAdmin(options)
	require.NoError(t, err)
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

	_, err = failing.subscribe(ctx, removedGroupID, store.FollowProvenanceManual)

	require.NoError(t, removalErr)
	var removed *communityRemovedError
	require.ErrorAs(t, err, &removed, "subscribe refuses the removed community")
	follows, undos := h.deliveredSince(mark)
	require.Equal(t, []string{removedGroupID}, follows)
	require.Empty(t, undos, "the take-back failed")
	assert.Equal(t, 1, h.owedUnfollowCount(), "the removal owes the failed take-back")

	h.newTestFollowRetrier(h.client).Sweep(ctx)

	_, undos = h.deliveredSince(mark)
	assert.Equal(t, []string{removedGroupID}, undos, "the sweep takes the Follow back")
	assert.Equal(t, ap.TypeUndo, h.lastDeliveredType())
	assert.Zero(t, h.owedUnfollowCount())
	assert.Equal(t, store.FollowStateNone, h.followStateOf(removedGroupID))
}

// TestCommunityRemoval_RetrierWhoseTakeBackFailsOwesTheUndoFollow: C is
// removed while the follow retrier's Follow is in flight, and the retrier's
// Undo{Follow} taking it back fails. The removal owes the Undo{Follow}, and
// the next sweep sends it.
func TestCommunityRemoval_RetrierWhoseTakeBackFailsOwesTheUndoFollow(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	communityDID := staleRetrierRaceCommunity(t, h)
	committer := newCommunityLabelCommitter(t, h, h.admin)
	var removalErr error
	client := &raceFollowClient{FollowClient: undoFailingClient{FollowClient: h.client}}
	client.beforeFollow = func() { removalErr = committer.attempt(t, communityDID, communityTakedown, false) }

	h.newTestFollowRetrier(client).Sweep(ctx)

	require.NoError(t, removalErr)
	require.Equal(t, ap.TypeFollow, h.lastDeliveredType(), "the retrier's take-back failed")
	assert.Equal(t, 1, h.owedUnfollowCount(), "the removal owes the failed take-back")
	mark := h.deliveryCount()

	h.newTestFollowRetrier(h.client).Sweep(ctx)

	follows, undos := h.deliveredSince(mark)
	assert.Empty(t, follows)
	assert.Equal(t, []string{removedGroupID}, undos, "the sweep takes the Follow back")
	assert.Zero(t, h.owedUnfollowCount())
}

// TestCommunityRemoval_RetrierRemovalReadFailingAfterTheSend: the follow
// retrier's removal read after its Follow fails. When C was removed while the
// Follow was in flight, the removal owes an Undo{Follow}, which the next sweep
// sends; when it was not, nothing is owed and C stays pending.
func TestCommunityRemoval_RetrierRemovalReadFailingAfterTheSend(t *testing.T) {
	for _, tc := range []struct {
		name      string
		remove    bool
		wantOwed  int
		wantUndos []string
		wantState store.FollowState
	}{
		{name: "removed while the Follow is in flight", remove: true, wantOwed: 1,
			wantUndos: []string{removedGroupID}, wantState: store.FollowStateNone},
		{name: "not removed", wantState: store.FollowStatePending},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			ctx := context.Background()
			communityDID := staleRetrierRaceCommunity(t, h)
			committer := newCommunityLabelCommitter(t, h, h.admin)
			var removalErr error
			client := &raceFollowClient{FollowClient: h.client}
			if tc.remove {
				client.beforeFollow = func() { removalErr = committer.attempt(t, communityDID, communityTakedown, false) }
			}
			failing := &removalReadFailingCommunities{removalStore: newRemovalStore(h), failFrom: 2}
			retrier, err := NewFollowRetrier(FollowRetrierOptions{
				Client: client, Communities: failing, Service: h.service, ResendAfter: time.Hour,
			})
			require.NoError(t, err)

			retrier.Sweep(ctx)

			require.NoError(t, removalErr)
			assert.Equal(t, tc.wantOwed, h.owedUnfollowCount())
			mark := h.deliveryCount()

			h.newTestFollowRetrier(h.client).Sweep(ctx)

			_, undos := h.deliveredSince(mark)
			assert.Equal(t, tc.wantUndos, undos)
			assert.Zero(t, h.owedUnfollowCount())
			assert.Equal(t, tc.wantState, h.followStateOf(removedGroupID))
		})
	}
}

// TestCommunityRemoval_DeliveredUndoNotCompletedThenFollowedAgainOwesAFollow:
// the follow retrier delivers C's owed Undo{Follow} but fails to complete it,
// then the removal is negated and C subscribed and accepted again. Lemmy may
// have applied that Undo{Follow} after the new Follow, so the next sweep
// re-pends C and sends a fresh Follow rather than dropping the owed
// Undo{Follow} as unsent.
func TestCommunityRemoval_DeliveredUndoNotCompletedThenFollowedAgainOwesAFollow(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	group, communityDID, committer := removeWithFailedUndo(t, h)
	failing, err := NewFollowRetrier(FollowRetrierOptions{
		Client: h.client, Communities: completeFailingCommunities{newRemovalStore(h)}, Service: h.service,
		ResendAfter: time.Hour,
	})
	require.NoError(t, err)
	mark := h.deliveryCount()

	failing.Sweep(ctx)

	_, undos := h.deliveredSince(mark)
	require.Equal(t, []string{removedGroupID}, undos, "the Undo{Follow} is delivered")
	require.Equal(t, 1, h.owedUnfollowCount(), "its completion failed")
	followAgainWhileTheUndoIsHeld(t, h, group, func() {
		committer.commit(t, communityDID, communityTakedown, true)
	})
	mark = h.deliveryCount()

	h.newTestFollowRetrier(h.client).Sweep(ctx)

	assert.Zero(t, h.owedUnfollowCount())
	assert.Equal(t, []string{ap.TypeFollow}, h.deliveredTypesSince(mark), "the sweep sends a fresh Follow")
	assert.Equal(t, store.FollowStatePending, h.followStateOf(removedGroupID))
	h.acceptLatestFollow(group)
	assert.Equal(t, store.FollowStateAccepted, h.followStateOf(removedGroupID))
}
