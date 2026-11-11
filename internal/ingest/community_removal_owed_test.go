package ingest

import (
	"context"
	stderrors "errors"
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/labeling"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/labels"
	"tidepool/internal/store"
)

// The follow work a community removal and its negation owe Lemmy: the
// removal's Undo{Follow} survives a failed send, and the negation's Follow is
// sent before any Accept counts.

// undoFailingClient is Lemmy refusing every Undo{Follow}; other activities
// reach the fake inbox.
type undoFailingClient struct{ FollowClient }

func (c undoFailingClient) SendActivity(ctx context.Context, inboxURL string, activity any) error {
	if object, ok := activity.(*ap.Object); ok && object.Type == ap.TypeUndo {
		return stderrors.New("test: Lemmy refuses the Undo{Follow}")
	}
	return c.FollowClient.SendActivity(ctx, inboxURL, activity)
}

// removeWithFailedUndo subscribes C by hand, then removes it through an admin
// whose Undo{Follow} fails, so the removal's Undo{Follow} reaches no one.
func removeWithFailedUndo(t *testing.T, h *harness) (group *remoteActor, communityDID string, committer *labelCommitter) {
	t.Helper()
	group = h.subscribeCommunityURL(removedGroupID, removedUsername)
	communityDID = h.communityDID(removedGroupID)
	options := h.adminOptions()
	options.Client = undoFailingClient{FollowClient: h.client}
	failing, err := NewAdmin(options)
	require.NoError(t, err)
	committer = newCommunityLabelCommitter(t, h, failing)
	mark := h.deliveryCount()

	committer.removeCommunity(t, communityDID)

	follows, undos := h.deliveredSince(mark)
	require.Empty(t, follows)
	require.Empty(t, undos, "the removal's Undo{Follow} failed")
	require.Equal(t, store.FollowStateNone, h.followStateOf(removedGroupID))
	return group, communityDID, committer
}

// TestCommunityRemoval_FailedUndoFollowIsSentByTheFollowRetrier: the removal's
// Undo{Follow} fails. The follow retrier's next sweep sends it, and the sweep
// after sends nothing.
func TestCommunityRemoval_FailedUndoFollowIsSentByTheFollowRetrier(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	removeWithFailedUndo(t, h)
	retrier := h.newTestFollowRetrier(h.client)
	mark := h.deliveryCount()

	retrier.Sweep(ctx)

	follows, undos := h.deliveredSince(mark)
	assert.Empty(t, follows)
	assert.Equal(t, []string{removedGroupID}, undos, "the sweep sends the owed Undo{Follow}")

	retrier.Sweep(ctx)

	follows, undos = h.deliveredSince(mark)
	assert.Empty(t, follows)
	assert.Equal(t, []string{removedGroupID}, undos, "the Undo{Follow} is no longer owed")
	assert.Equal(t, store.FollowStateNone, h.followStateOf(removedGroupID))
}

// TestCommunityRemoval_FailedUndoFollowIsSentAtTheFollowRetriersStart: the
// removal's Undo{Follow} failed before a restart. The follow retrier sends it
// when it starts, before its first interval passes.
func TestCommunityRemoval_FailedUndoFollowIsSentAtTheFollowRetriersStart(t *testing.T) {
	h := newHarness(t)
	removeWithFailedUndo(t, h)
	retrier, err := NewFollowRetrier(FollowRetrierOptions{
		Client:      h.client,
		Communities: h.communities,
		Service:     h.service,
		ResendAfter: time.Hour,
		Interval:    time.Hour,
		MaxAttempts: 5,
	})
	require.NoError(t, err)
	mark := h.deliveryCount()
	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		retrier.Run(ctx)
	}()
	defer func() {
		cancel()
		<-returned
	}()

	deadline := time.Now().Add(stallDeadline)
	for {
		_, undos := h.deliveredSince(mark)
		if len(undos) > 0 {
			assert.Equal(t, []string{removedGroupID}, undos, "the start sends the owed Undo{Follow}")
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the follow retrier sent no Undo{Follow} at its start")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestCommunityRemoval_NegationWhileTheUndoFollowIsOwedSendsOnlyTheFollow: the
// removal's Undo{Follow} failed, then the negation follows C again. The follow
// retrier sends C a Follow and no Undo{Follow}, which would unfollow C after
// it.
func TestCommunityRemoval_NegationWhileTheUndoFollowIsOwedSendsOnlyTheFollow(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	_, communityDID, committer := removeWithFailedUndo(t, h)
	committer.commit(t, communityDID, communityTakedown, true)
	retrier := h.newTestFollowRetrier(h.client)
	mark := h.deliveryCount()

	retrier.Sweep(ctx)
	retrier.Sweep(ctx)

	follows, undos := h.deliveredSince(mark)
	assert.Equal(t, []string{removedGroupID}, follows, "the retrier follows C again, once")
	assert.Empty(t, undos, "no Undo{Follow} follows the restored Follow")
	assert.Equal(t, store.FollowStatePending, h.followStateOf(removedGroupID))
}

// TestCommunityRemoval_AcceptBeforeTheRestoredFollowIsSentIsRefused: C,
// followed by hand, is removed and the removal negated. Before the follow
// retrier sends the owed Follow, Lemmy redelivers its Accept of the Follow
// sent before the removal. C stays pending, the owed Follow is sent at the
// next sweep, and its Accept follows C.
func TestCommunityRemoval_AcceptBeforeTheRestoredFollowIsSentIsRefused(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	group := h.subscribeCommunityURL(removedGroupID, removedUsername)
	oldFollowID := h.latestFollowID(removedGroupID)
	communityDID := h.communityDID(removedGroupID)
	committer := newCommunityLabelCommitter(t, h, h.admin)
	committer.removeCommunity(t, communityDID)
	committer.commit(t, communityDID, communityTakedown, true)

	community, err := h.communities.GetByAPGroupID(ctx, removedGroupID)
	require.NoError(t, err)
	require.Equal(t, store.FollowStatePending, community.FollowState)
	assert.Nil(t, community.FollowRequestedAt, "the restored Follow is owed, not sent")
	assert.Zero(t, community.FollowAttempts, "an owed Follow is not a counted attempt")

	status := h.deliver(group, map[string]any{
		"id":     removedGroupID + "/activities/accept/follow-redelivered",
		"type":   "Accept",
		"actor":  removedGroupID,
		"object": map[string]any{"id": oldFollowID, "type": "Follow", "actor": h.service.ID, "object": removedGroupID},
	})
	require.Equal(t, http.StatusAccepted, status)
	h.drain()

	assert.Equal(t, store.FollowStatePending, h.followStateOf(removedGroupID),
		"an Accept before the restored Follow is sent does not follow C")
	mark := h.deliveryCount()

	h.newTestFollowRetrier(h.client).Sweep(ctx)

	follows, undos := h.deliveredSince(mark)
	assert.Equal(t, []string{removedGroupID}, follows, "the retrier sends the owed Follow")
	assert.Empty(t, undos)
	community, err = h.communities.GetByAPGroupID(ctx, removedGroupID)
	require.NoError(t, err)
	assert.Equal(t, 1, community.FollowAttempts, "the sent Follow is the first attempt")

	h.acceptLatestFollow(group)

	assert.Equal(t, store.FollowStateAccepted, h.followStateOf(removedGroupID),
		"the Accept of the restored Follow follows C")
}

// undoBlockingClient holds every Undo{Follow} until release is closed, after
// signalling entered; other activities reach the fake inbox at once.
type undoBlockingClient struct {
	FollowClient
	entered chan struct{}
	release chan struct{}
}

func newUndoBlockingClient(inner FollowClient) *undoBlockingClient {
	return &undoBlockingClient{FollowClient: inner, entered: make(chan struct{}, 1), release: make(chan struct{})}
}

func (c *undoBlockingClient) SendActivity(ctx context.Context, inboxURL string, activity any) error {
	if object, ok := activity.(*ap.Object); ok && object.Type == ap.TypeUndo {
		select {
		case c.entered <- struct{}{}:
		default:
		}
		select {
		case <-c.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return c.FollowClient.SendActivity(ctx, inboxURL, activity)
}

// waitEntered waits until an Undo{Follow} is held.
func (c *undoBlockingClient) waitEntered(t *testing.T) {
	t.Helper()
	select {
	case <-c.entered:
	case <-time.After(stallDeadline):
		t.Fatal("no Undo{Follow} was sent")
	}
}

// deliveredTypesSince gives the type of every activity the fake Lemmy inbox
// received after mark, in delivery order.
func (h *harness) deliveredTypesSince(mark int) []string {
	h.t.Helper()
	h.mu.Lock()
	delivered := slices.Clone(h.inboxLog[mark:])
	h.mu.Unlock()
	types := make([]string, 0, len(delivered))
	for _, raw := range delivered {
		activity, err := ap.ParseObject(raw)
		require.NoError(h.t, err)
		types = append(types, activity.Type)
	}
	return types
}

// followAgainWhileTheUndoIsHeld negates C's removal, subscribes C by hand and
// accepts that Follow, all while an Undo{Follow} of C is held in flight.
func followAgainWhileTheUndoIsHeld(t *testing.T, h *harness, group *remoteActor, negate func()) {
	t.Helper()
	negate()
	rec := h.adminRequest(http.MethodPost, "/admin/communities", map[string]any{"community": removedGroupID})
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	status := h.deliver(group, map[string]any{
		"id":     removedGroupID + "/activities/accept/follow-again",
		"type":   "Accept",
		"actor":  removedGroupID,
		"object": map[string]any{"id": h.latestFollowID(removedGroupID), "type": "Follow", "actor": h.service.ID, "object": removedGroupID},
	})
	require.Equal(t, http.StatusAccepted, status)
	h.drain()
	require.Equal(t, store.FollowStateAccepted, h.followStateOf(removedGroupID),
		"C is followed again before the held Undo{Follow} lands")
}

// assertRefollowOwedAfterTheHeldUndo: the held Undo{Follow} landed after C's
// new Follow, so Lemmy no longer has Tidepool as a follower. C is pending with
// its Follow owed with a fresh attempt budget, no removal owes an
// Undo{Follow}, and the next sweep of a retrier allowing maxAttempts sends a
// Follow after the Undo{Follow}.
func assertRefollowOwedAfterTheHeldUndo(t *testing.T, h *harness, mark, maxAttempts int) {
	t.Helper()
	ctx := context.Background()
	community, err := h.communities.GetByAPGroupID(ctx, removedGroupID)
	require.NoError(t, err)
	assert.Equal(t, store.FollowStatePending, community.FollowState,
		"a Follow the Undo{Follow} overtook leaves C pending, not accepted")
	assert.Nil(t, community.FollowRequestedAt, "the replacement Follow is owed, not sent")
	assert.Zero(t, community.FollowAttempts, "the replacement Follow starts a fresh attempt budget")
	owed, err := h.communities.(store.CommunityRemovals).ListOwedUnfollows(ctx)
	require.NoError(t, err)
	assert.Empty(t, owed, "the delivered Undo{Follow} is no longer owed")

	h.newTestFollowRetrierWithMaxAttempts(h.client, maxAttempts).Sweep(ctx)

	assert.Equal(t, []string{ap.TypeFollow, ap.TypeUndo, ap.TypeFollow}, h.deliveredTypesSince(mark),
		"the subscribe's Follow, the held Undo{Follow}, then the sweep's replacement Follow")
}

// TestCommunityRemoval_RetrierUndoOvertakingAFollowAgainOwesAFollow: the
// follow retrier lists C's owed Undo{Follow} and holds its delivery while the
// removal is negated and C is subscribed and accepted again. The Undo{Follow}
// then lands; C is re-pended with its Follow owed, which the same sweep's
// claim sends after the Undo{Follow}. It holds at a budget of one attempt,
// which the subscribe's Follow used.
func TestCommunityRemoval_RetrierUndoOvertakingAFollowAgainOwesAFollow(t *testing.T) {
	for _, maxAttempts := range []int{5, 1} {
		t.Run(fmt.Sprintf("max attempts %d", maxAttempts), func(t *testing.T) {
			retrierUndoOvertakingAFollowAgainOwesAFollow(t, maxAttempts)
		})
	}
}

func retrierUndoOvertakingAFollowAgainOwesAFollow(t *testing.T, maxAttempts int) {
	h := newHarness(t)
	group, communityDID, committer := removeWithFailedUndo(t, h)
	blocking := newUndoBlockingClient(h.client)
	retrier := h.newTestFollowRetrierWithMaxAttempts(blocking, maxAttempts)
	mark := h.deliveryCount()
	swept := make(chan struct{})
	go func() {
		defer close(swept)
		retrier.Sweep(context.Background())
	}()
	blocking.waitEntered(t)

	followAgainWhileTheUndoIsHeld(t, h, group, func() {
		committer.commit(t, communityDID, communityTakedown, true)
	})
	close(blocking.release)
	<-swept

	assert.Equal(t, []string{ap.TypeFollow, ap.TypeUndo, ap.TypeFollow}, h.deliveredTypesSince(mark),
		"the subscribe's Follow, the held Undo{Follow}, then the sweep's replacement Follow")
	assert.Equal(t, store.FollowStatePending, h.followStateOf(removedGroupID),
		"a Follow the Undo{Follow} overtook leaves C pending, not accepted")
	owed, err := h.communities.(store.CommunityRemovals).ListOwedUnfollows(context.Background())
	require.NoError(t, err)
	assert.Empty(t, owed, "the delivered Undo{Follow} is no longer owed")
	h.acceptLatestFollow(group)
	assert.Equal(t, store.FollowStateAccepted, h.followStateOf(removedGroupID),
		"the Accept of the replacement Follow follows C")
}

// TestCommunityRemoval_LabelUndoOvertakingAFollowAgainOwesAFollow: the
// removal's own Undo{Follow} is held while the removal is negated and C is
// subscribed and accepted again. The Undo{Follow} then lands; C is owed a
// fresh Follow, which the next sweep sends. It holds at a budget of one
// attempt, which the subscribe's Follow used.
func TestCommunityRemoval_LabelUndoOvertakingAFollowAgainOwesAFollow(t *testing.T) {
	for _, maxAttempts := range []int{5, 1} {
		t.Run(fmt.Sprintf("max attempts %d", maxAttempts), func(t *testing.T) {
			labelUndoOvertakingAFollowAgainOwesAFollow(t, maxAttempts)
		})
	}
}

func labelUndoOvertakingAFollowAgainOwesAFollow(t *testing.T, maxAttempts int) {
	h := newHarness(t)
	group := h.subscribeCommunityURL(removedGroupID, removedUsername)
	communityDID := h.communityDID(removedGroupID)
	blocking := newUndoBlockingClient(h.client)
	options := h.adminOptions()
	options.Client = blocking
	blockingAdmin, err := NewAdmin(options)
	require.NoError(t, err)
	committer := newCommunityLabelCommitter(t, h, blockingAdmin)
	mark := h.deliveryCount()
	removed := make(chan error, 1)
	go func() {
		removed <- committer.store.Commit(context.Background(), labels.Cursor{Seq: 1},
			committer.sign(labeling.Label{URI: communityDID, Val: communityTakedown}))
	}()
	blocking.waitEntered(t)

	followAgainWhileTheUndoIsHeld(t, h, group, func() {
		negated := true
		require.NoError(t, committer.store.Commit(context.Background(), labels.Cursor{Seq: 2},
			committer.sign(labeling.Label{URI: communityDID, Val: communityTakedown, Negated: &negated})))
	})
	close(blocking.release)
	require.NoError(t, <-removed)

	assertRefollowOwedAfterTheHeldUndo(t, h, mark, maxAttempts)
}
