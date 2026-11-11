package ingest

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/labels"
	"tidepool/internal/labels/labelstest"
	"tidepool/internal/store"
)

// Bridged community removal: a Coves `!takedown` label on a bridged
// community's bare DID unfollows the community and keeps every record; no
// follow path follows it again while the label stands.

// TestCommunityRemoval_LabelUnfollowsAndKeepsRecords: the label on C's DID
// sends exactly one Undo{Follow} to C and leaves it unfollowed, whether C was
// accepted or still pending, and keeps C's row and its bridged post's mapping.
// A label on a DID that is not a bridged community sends nothing and changes
// no community (a guard: the applier ignores what is not a community).
func TestCommunityRemoval_LabelUnfollowsAndKeepsRecords(t *testing.T) {
	const (
		nativePersonDID = "did:plc:communityremovalnativeaa"
		unknownDID      = "did:plc:communityremovalunknownb"
	)
	for _, tc := range []struct {
		name string
		// pending leaves C pending (no Accept, no post) instead of accepted
		// with one bridged post.
		pending   bool
		subject   func(communityDID string) string
		wantUndos []string
		wantState store.FollowState
	}{
		{
			name:      "accepted community",
			subject:   func(communityDID string) string { return communityDID },
			wantUndos: []string{groupID},
			wantState: store.FollowStateNone,
		},
		{
			name:      "pending community",
			pending:   true,
			subject:   func(communityDID string) string { return communityDID },
			wantUndos: []string{groupID},
			wantState: store.FollowStateNone,
		},
		{
			name:      "native person DID",
			subject:   func(string) string { return nativePersonDID },
			wantState: store.FollowStateAccepted,
		},
		{
			name:      "unknown DID",
			subject:   func(string) string { return unknownDID },
			wantState: store.FollowStateAccepted,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			ctx := context.Background()
			_, err := store.NewAPActors(h.db).Create(ctx, store.APActor{
				DID:              nativePersonDID,
				Kind:             store.ActorTypePerson,
				ActorID:          "https://coves.social/ap/actor/" + nativePersonDID,
				NormalizedOrigin: "coves.social",
				LocalPart:        "removalnative",
				RSAKeySealed:     []byte("sealed"),
				RSAKeyVersion:    1,
				PublicKeyPEM:     "pem",
			})
			require.NoError(t, err)
			var postID string
			if tc.pending {
				h.technologyGroup()
				h.serveTechnologyWebfinger()
				rec := h.adminRequest(http.MethodPost, "/admin/communities",
					map[string]any{"community": "!technology@lemmy.world"})
				require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
			} else {
				h.serveLemmyWorldContent()
				group := h.subscribeTechnology()
				postID = h.bridgePost(group, "50510001")
			}
			communityDID := h.communityDID(groupID)
			committer := newCommunityLabelCommitter(t, h, h.admin)
			mark := h.deliveryCount()

			committer.removeCommunity(t, tc.subject(communityDID))

			follows, undos := h.deliveredSince(mark)
			assert.Equal(t, tc.wantUndos, undos, "Undo{Follow}s delivered")
			assert.Empty(t, follows, "Follows delivered")
			assert.Equal(t, tc.wantState, h.followStateOf(groupID))
			assert.Equal(t, testDIDFor("technology", "lemmy.world"), h.communityDID(groupID),
				"the community row and its DID remain")
			if postID != "" {
				mapping, err := h.objects.GetByAPID(ctx, postID)
				require.NoError(t, err, "the bridged post's mapping remains")
				assert.False(t, mapping.IsDeleted(), "the bridged post is not deleted")
			}
		})
	}
}

// TestCommunityRemoval_RolledBackLabelLeavesTheFollow: the label commit fails
// after the applier ran (the label cursor update raises), so the cursor stays,
// C stays accepted, Lemmy is sent no Undo{Follow} and nothing suppresses C: a
// subscribe answers the idempotent 200. The same label committed again once the fault is gone
// removes C, and the subscribe is then refused.
func TestCommunityRemoval_RolledBackLabelLeavesTheFollow(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.subscribeTechnology()
	communityDID := h.communityDID(groupID)
	committer := newCommunityLabelCommitter(t, h, h.admin)
	restore := failLabelCursorUpdates(t, h.db)
	mark := h.deliveryCount()

	require.Error(t, committer.attempt(t, communityDID, communityTakedown, false))

	follows, undos := h.deliveredSince(mark)
	assert.Empty(t, undos, "a rolled-back removal sends no Undo{Follow}")
	assert.Empty(t, follows)

	cursor, err := labels.NewPostgresStore(h.db).Cursor(ctx)
	require.NoError(t, err)
	assert.Equal(t, labels.Cursor{Epoch: 0, Seq: 0}, cursor, "the failed commit does not advance the cursor")
	assert.Equal(t, store.FollowStateAccepted, h.followStateOf(groupID))
	rec := h.adminRequest(http.MethodPost, "/admin/communities", map[string]any{"community": groupID})
	assert.Equal(t, http.StatusOK, rec.Code, "a rolled-back removal leaves no suppression: %s", rec.Body.String())

	restore()
	committer.removeCommunity(t, communityDID)

	assert.Equal(t, store.FollowStateNone, h.followStateOf(groupID))
	rec = h.adminRequest(http.MethodPost, "/admin/communities", map[string]any{"community": groupID})
	assert.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
}

// removalSweepRow is a B2 row driven by one follow-list sweep: the follow
// list names removedGroupID one way and controlGroupID too. The sweep must
// follow only the control and report removedGroupID suppressed.
func removalSweepRow(followList string, arrange func(rig *instanceRig)) func(t *testing.T) {
	return func(t *testing.T) {
		h := newHarness(t)
		rig := newInstanceRig(t, h, followList, instanceRigOptions{})
		arrange(rig)
		h.subscribeCommunityURL(removedGroupID, removedUsername)
		newCommunityLabelCommitter(t, h, h.admin).removeCommunity(t, h.communityDID(removedGroupID))
		mark := h.deliveryCount()

		result := rig.sweep()

		assert.Equal(t, []string{removedGroupID}, result.Suppressed, "suppressed")
		assert.Equal(t, []string{controlGroupID}, result.Subscribed, "subscribed")
		assert.Empty(t, result.Failed, "failed")
		assert.Empty(t, result.Unsubscribed, "unsubscribed")
		follows, undos := h.deliveredSince(mark)
		assert.Equal(t, []string{controlGroupID}, follows, "the sweep follows the control only")
		assert.Empty(t, undos)
		assert.Equal(t, store.FollowStateNone, h.followStateOf(removedGroupID))
		assert.Equal(t, store.FollowStatePending, h.followStateOf(controlGroupID))
	}
}

// TestCommunityRemoval_EveryFollowPathHonorsTheRemoval: from C removed by the
// label, no follow path follows C again. Rows a to c are sweeps whose follow
// list also names a control community, which is followed (the sweep ran).
func TestCommunityRemoval_EveryFollowPathHonorsTheRemoval(t *testing.T) {
	const host = lemmyFixtureHost
	for _, tc := range []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "a: the follow list names C",
			run: removalSweepRow(`communities:
  - "https://lemmy.world/c/removed"
  - "https://lemmy.world/c/control"
`, func(rig *instanceRig) { rig.serveGroup(controlGroupID, nil) }),
		},
		{
			name: "b: an instance entry whose listing lists C",
			run: removalSweepRow(instanceFollowList(host), func(rig *instanceRig) {
				rig.fixture.add(host, lemmyCommunity{name: removedUsername})
				rig.addCommunity(host, "control")
			}),
		},
		{
			name: "c: an instance entry whose post walk finds C",
			run: removalSweepRow(instanceFollowList(host), func(rig *instanceRig) {
				rig.addCommunity(host, "control")
				rig.post(host, removedGroupID)
			}),
		},
		{
			name: "d: C forced back to a stale pending",
			run: func(t *testing.T) {
				h := newHarness(t)
				ctx := context.Background()
				h.subscribeCommunityURL(removedGroupID, removedUsername)
				newCommunityLabelCommitter(t, h, h.admin).removeCommunity(t, h.communityDID(removedGroupID))
				_, err := h.db.ExecContext(ctx, `
					UPDATE communities
					SET follow_state = 'pending', follow_requested_at = now() - interval '1 day'
					WHERE ap_group_id = $1`, removedGroupID)
				require.NoError(t, err)
				retrier, err := NewFollowRetrier(FollowRetrierOptions{
					Client:      h.client,
					Communities: h.communities,
					Service:     h.service,
					ResendAfter: time.Hour,
					MaxAttempts: 3,
				})
				require.NoError(t, err)
				mark := h.deliveryCount()

				retrier.Sweep(ctx)

				follows, _ := h.deliveredSince(mark)
				assert.Empty(t, follows, "the retrier re-sends no Follow to a removed community")
			},
		},
		{
			name: "e: POST /admin/communities for C",
			run: func(t *testing.T) {
				h := newHarness(t)
				h.subscribeCommunityURL(removedGroupID, removedUsername)
				newCommunityLabelCommitter(t, h, h.admin).removeCommunity(t, h.communityDID(removedGroupID))
				mark := h.deliveryCount()

				rec := h.adminRequest(http.MethodPost, "/admin/communities",
					map[string]any{"community": removedGroupID})

				assert.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
				follows, _ := h.deliveredSince(mark)
				assert.Empty(t, follows)
				assert.Equal(t, store.FollowStateNone, h.followStateOf(removedGroupID))
			},
		},
		{
			name: "f: backfill of C, by the admin route and by the real queue",
			run: func(t *testing.T) {
				rig := newQueueRig(t, 5)
				h := rig.h
				blockers, removed := rig.communities[:4], rig.communities[4]
				committer := newCommunityLabelCommitter(t, h, h.admin)

				// A forced walk of C queued before the removal, behind four
				// walks holding every slot.
				rig.trigger(t, blockers...)
				rig.fetcher.awaitEntries(t, 4)
				rig.backfill.TriggerAsync(removed, true)
				committer.removeCommunity(t, testDIDFor("queued4", "lemmy.world"))
				rig.fetcher.releaseAll()
				rig.drain(t)

				rec := h.adminRequest(http.MethodPost, "/admin/communities/backfill",
					map[string]any{"community": removed.APGroupID})
				assert.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
				assert.Zero(t, h.backfills.count(), "the admin route queues no backfill")

				current, err := h.communities.GetByAPGroupID(context.Background(), removed.APGroupID)
				require.NoError(t, err)
				rig.backfill.TriggerAsync(current, true)
				rig.backfill.RetryUnfinished(current)
				rig.drain(t)

				assert.Zero(t, rig.fetcher.entriesFor(removed.APGroupID), "walks of the removed community")
				for _, blocker := range blockers {
					assert.Equal(t, 1, rig.fetcher.entriesFor(blocker.APGroupID), "walks of %s", blocker.APGroupID)
				}
			},
		},
		{
			name: "g: the label arrives before Tidepool has C",
			run: func(t *testing.T) {
				h := newHarness(t)
				const unseenGroupID = "https://lemmy.world/c/unseen"
				newCommunityLabelCommitter(t, h, h.admin).removeCommunity(t, testDIDFor("unseen", "lemmy.world"))
				h.serveCommunityGroup(unseenGroupID, "unseen")
				mark := h.deliveryCount()

				rec := h.adminRequest(http.MethodPost, "/admin/communities",
					map[string]any{"community": unseenGroupID})

				assert.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
				follows, _ := h.deliveredSince(mark)
				assert.Empty(t, follows)
			},
		},
	} {
		t.Run(tc.name, tc.run)
	}
}

// TestCommunityRemoval_StoredLabelIsActivatedOnStart: the label was stored at
// seq 1 by a store with no community applier. A consumer whose store has the
// applier, started at cursor 1 with no new frame, removes C: one
// Undo{Follow}, state none, a subscribe refused. A second start sends nothing
// more, and the negation after it re-follows C through the follow retrier (the
// second activation kept the state at removal).
func TestCommunityRemoval_StoredLabelIsActivatedOnStart(t *testing.T) {
	h := newHarness(t)
	h.subscribeCommunityURL(removedGroupID, removedUsername)
	communityDID := h.communityDID(removedGroupID)
	committer := newLabelCommitterWith(t, h)
	committer.removeCommunity(t, communityDID)
	require.Equal(t, store.FollowStateAccepted, h.followStateOf(removedGroupID),
		"a store with no community applier stores the label only")

	committer.store = labels.NewPostgresStore(h.db, h.admin.CommunityLabelApplier())
	server := labelstest.NewServer(t, func(int64) labelstest.Reply { return labelstest.Reply{Hold: true} })
	mark := h.deliveryCount()

	runLabelConsumer(t, committer.store, server, committer.key, 1)

	follows, undos := h.deliveredSince(mark)
	assert.Equal(t, []string{removedGroupID}, undos, "activation sends one Undo{Follow}")
	assert.Empty(t, follows)
	assert.Equal(t, store.FollowStateNone, h.followStateOf(removedGroupID))
	rec := h.adminRequest(http.MethodPost, "/admin/communities", map[string]any{"community": removedGroupID})
	assert.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())

	runLabelConsumer(t, committer.store, server, committer.key, 1)

	follows, undos = h.deliveredSince(mark)
	assert.Equal(t, []string{removedGroupID}, undos, "a second start sends no second Undo{Follow}")
	assert.Empty(t, follows)

	committer.commit(t, communityDID, communityTakedown, true)
	h.newTestFollowRetrier(h.client).Sweep(context.Background())

	follows, undos = h.deliveredSince(mark)
	assert.Equal(t, []string{removedGroupID}, follows, "the negation re-follows C once, through the retrier")
	assert.Equal(t, []string{removedGroupID}, undos)
	assert.Equal(t, store.FollowStatePending, h.followStateOf(removedGroupID))
}
