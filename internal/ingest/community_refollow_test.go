package ingest

import (
	"context"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/store"
)

// explicitRemovedFollowList names C, and nothing else, as an explicit entry.
const explicitRemovedFollowList = `communities:
  - "https://lemmy.world/c/removed"
`

// TestCommunityRemoval_NegationFollowsAgainOnlyWhenStillWanted: the negation
// of C's removal follows C again only when C was followed (accepted or pending)
// at the removal and the way it was followed still wants it: a manual
// subscribe always does, a follow-list entry or an instance entry only while
// the follow list still names C. The negation itself sends nothing: it leaves
// C pending with its Follow owed, and the FollowRetrier's next sweep sends it.
// Otherwise C is left unfollowed, and can be subscribed again by hand. An
// unreadable follow list wants nothing; a manual subscribe of a community
// already pending keeps the provenance of the path that followed it first.
// Neither the label nor the negation edits the follow list file.
func TestCommunityRemoval_NegationFollowsAgainOnlyWhenStillWanted(t *testing.T) {
	const host = lemmyFixtureHost
	removedOutbox := removedGroupID + "/outbox"

	// sweepFollow follows C through one follow-list sweep, and with accept
	// delivers C's Accept.
	sweepFollow := func(accept bool) func(t *testing.T, h *harness, rig *instanceRig) {
		return func(t *testing.T, h *harness, rig *instanceRig) {
			mark := h.deliveryCount()
			result := rig.sweep()
			require.Equal(t, []string{removedGroupID}, result.Subscribed, "the sweep follows C")
			follows, _ := h.deliveredSince(mark)
			require.Equal(t, []string{removedGroupID}, follows)
			if accept {
				h.acceptLatestFollow(rig.rigGroup(removedGroupID))
				require.Equal(t, store.FollowStateAccepted, h.followStateOf(removedGroupID))
			} else {
				require.Equal(t, store.FollowStatePending, h.followStateOf(removedGroupID))
			}
		}
	}

	for _, tc := range []struct {
		name string
		// followList, when set, is the follow list of a reconciler the Admin
		// is wired with (SetFollowReconciler). Unset, the Admin has none.
		followList string
		// serve publishes C (and lists or posts it) before follow runs.
		serve func(rig *instanceRig)
		// follow brings C to its state at the removal, by the path it names.
		follow func(t *testing.T, h *harness, rig *instanceRig)
		// listAtNegation, when set, replaces the follow list before the
		// negation.
		listAtNegation string
		// wantFollow is whether the negation leaves C one Follow owed, which
		// the FollowRetrier sends.
		wantFollow bool
		// backfilled walks C's outbox on each Accept with a real Backfill.
		backfilled bool
	}{
		{
			name:       "a: accepted by its follow-list entry, still listed",
			followList: explicitRemovedFollowList,
			serve: func(rig *instanceRig) {
				rig.serveGroup(removedGroupID, map[string]any{"outbox": removedOutbox})
				rig.serveEmptyOutbox(removedGroupID)
			},
			follow:     sweepFollow(true),
			wantFollow: true,
			backfilled: true,
		},
		{
			name:           "b: accepted by its follow-list entry, the entry dropped",
			followList:     explicitRemovedFollowList,
			serve:          func(rig *instanceRig) { rig.serveGroup(removedGroupID, nil) },
			follow:         sweepFollow(true),
			listAtNegation: "communities: []\n",
		},
		{
			name: "c: accepted by a manual subscribe, no follow list",
			follow: func(t *testing.T, h *harness, _ *instanceRig) {
				h.subscribeCommunityURL(removedGroupID, removedUsername)
			},
			wantFollow: true,
		},
		{
			name:       "d: accepted by its instance entry's listing, the host still listed",
			followList: instanceFollowList(host),
			serve:      func(rig *instanceRig) { rig.addCommunity(host, removedUsername) },
			follow:     sweepFollow(true),
			wantFollow: true,
		},
		{
			name:       "e: never followed, though the follow list names it",
			followList: explicitRemovedFollowList,
			serve:      func(rig *instanceRig) { rig.serveGroup(removedGroupID, nil) },
			follow: func(t *testing.T, h *harness, _ *instanceRig) {
				community, err := h.mat.EnsureCommunity(context.Background(), &ap.Object{ID: removedGroupID})
				require.NoError(t, err)
				require.Equal(t, store.FollowStateNone, community.FollowState)
			},
		},
		{
			name:       "f: pending by its follow-list entry, still listed",
			followList: explicitRemovedFollowList,
			serve:      func(rig *instanceRig) { rig.serveGroup(removedGroupID, nil) },
			follow:     sweepFollow(false),
			wantFollow: true,
		},
		{
			name:       "g: accepted by its instance entry's post walk, the entry still configured",
			followList: instanceFollowList(host),
			serve: func(rig *instanceRig) {
				rig.addPostOnlyCommunity(host, removedUsername)
				rig.post(host, removedGroupID)
			},
			follow:     sweepFollow(true),
			wantFollow: true,
		},
		{
			name:       "h: accepted by its instance entry's post walk, then excluded",
			followList: instanceFollowList(host),
			serve: func(rig *instanceRig) {
				rig.addPostOnlyCommunity(host, removedUsername)
				rig.post(host, removedGroupID)
			},
			follow: sweepFollow(true),
			listAtNegation: `communities: []
instances:
  - host: lemmy.world
    exclude:
      - "https://lemmy.world/c/removed"
`,
		},
		{
			name:           "i: accepted by its follow-list entry, the follow list unreadable at the negation",
			followList:     explicitRemovedFollowList,
			serve:          func(rig *instanceRig) { rig.serveGroup(removedGroupID, nil) },
			follow:         sweepFollow(true),
			listAtNegation: "communities: [\n",
		},
		{
			name:       "j: pending by its follow-list entry, then subscribed by hand and accepted, the entry dropped",
			followList: explicitRemovedFollowList,
			serve:      func(rig *instanceRig) { rig.serveGroup(removedGroupID, nil) },
			follow: func(t *testing.T, h *harness, rig *instanceRig) {
				sweepFollow(false)(t, h, rig)
				mark := h.deliveryCount()
				rec := h.adminRequest(http.MethodPost, "/admin/communities", map[string]any{"community": removedGroupID})
				require.Equal(t, http.StatusAccepted, rec.Code, "the subscribe of pending C: %s", rec.Body.String())
				follows, _ := h.deliveredSince(mark)
				require.Equal(t, []string{removedGroupID}, follows, "the subscribe sends C a Follow")
				h.acceptLatestFollow(rig.rigGroup(removedGroupID))
				require.Equal(t, store.FollowStateAccepted, h.followStateOf(removedGroupID))
			},
			listAtNegation: "communities: []\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			var backfill *Backfill
			if tc.backfilled {
				backfill = newBackfillFetching(t, h, h.client, context.Background())
				t.Cleanup(backfill.Wait)
				h.useBackfiller(backfill)
			}
			waitForBackfill := func() {
				t.Helper()
				require.NoError(t, startCall("Backfill.Wait", func() error {
					backfill.Wait()
					return nil
				}).result(t))
			}
			var rig *instanceRig
			if tc.followList != "" {
				rig = newInstanceRig(t, h, tc.followList, instanceRigOptions{})
				h.admin.SetFollowReconciler(rig.reconciler)
				tc.serve(rig)
			}
			tc.follow(t, h, rig)
			outboxFetches := 0
			if tc.backfilled {
				waitForBackfill()
				require.NotNil(t, h.lastBackfillAt(removedGroupID), "the first Accept's walk finished: C is fresh")
				outboxFetches = h.hitCount(urlPath(t, removedOutbox))
				require.Equal(t, 1, outboxFetches, "the first Accept's walk fetched the outbox")
			}
			communityDID := h.communityDID(removedGroupID)
			committer := newCommunityLabelCommitter(t, h, h.admin)

			committer.removeCommunity(t, communityDID)

			assert.Equal(t, store.FollowStateNone, h.followStateOf(removedGroupID), "the label removes C")
			if !tc.wantFollow {
				rec := h.adminRequest(http.MethodPost, "/admin/communities", map[string]any{"community": removedGroupID})
				assert.Equal(t, http.StatusConflict, rec.Code, "removed C refuses a subscribe: %s", rec.Body.String())
			}
			if tc.listAtNegation != "" {
				rig.writeFollowList(tc.listAtNegation)
			}
			var listBytes []byte
			if rig != nil {
				var err error
				listBytes, err = os.ReadFile(rig.path)
				require.NoError(t, err)
			}
			mark := h.deliveryCount()

			committer.commit(t, communityDID, communityTakedown, true)

			follows, undos := h.deliveredSince(mark)
			assert.Empty(t, undos, "the negation sends no Undo{Follow}")
			assert.Empty(t, follows, "the negation itself sends no Follow")
			if rig != nil {
				after, err := os.ReadFile(rig.path)
				require.NoError(t, err)
				assert.Equal(t, string(listBytes), string(after), "the follow list file is unchanged")
			}
			if !tc.wantFollow {
				h.newTestFollowRetrier(h.client).Sweep(context.Background())
				follows, _ = h.deliveredSince(mark)
				assert.Empty(t, follows, "no Follow is owed")
				assert.Equal(t, store.FollowStateNone, h.followStateOf(removedGroupID))

				rec := h.adminRequest(http.MethodPost, "/admin/communities", map[string]any{"community": removedGroupID})

				assert.Equal(t, http.StatusAccepted, rec.Code, "the negation cleared the removal: %s", rec.Body.String())
				follows, _ = h.deliveredSince(mark)
				assert.Equal(t, []string{removedGroupID}, follows, "the subscribe follows C")
				assert.Equal(t, store.FollowStatePending, h.followStateOf(removedGroupID))
				return
			}
			assert.Equal(t, store.FollowStatePending, h.followStateOf(removedGroupID),
				"the negation leaves C pending, its Follow owed")

			h.newTestFollowRetrier(h.client).Sweep(context.Background())

			follows, _ = h.deliveredSince(mark)
			require.Equal(t, []string{removedGroupID}, follows, "the retrier follows C again, once")
			assert.Equal(t, store.FollowStatePending, h.followStateOf(removedGroupID))
			if !tc.backfilled {
				return
			}

			h.acceptLatestFollow(rig.rigGroup(removedGroupID))
			waitForBackfill()

			assert.Equal(t, store.FollowStateAccepted, h.followStateOf(removedGroupID))
			assert.Equal(t, outboxFetches+1, h.hitCount(urlPath(t, removedOutbox)),
				"the Accept after the restoration walks C although its last backfill is fresh")
			follows, _ = h.deliveredSince(mark)
			assert.Equal(t, []string{removedGroupID}, follows, "no second Follow")
		})
	}
}
