package ingest

import (
	"fmt"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// requireWalkReturnedNone asserts the walks are one clean walk of host that
// returned none of ids, so a sweep that tries one of them took it from what an
// earlier walk found.
func requireWalkReturnedNone(t *testing.T, walks []recordedPostWalk, host string, ids ...string) {
	t.Helper()
	require.Len(t, walks, 1, "one walk of each listed host")
	require.Equal(t, host, walks[0].host)
	require.NoError(t, walks[0].walk.Err)
	for _, id := range ids {
		require.NotContains(t, walks[0].walk.ActorIDs, id, "this walk must not return it")
	}
}

// serveFailingGroupOrigin answers 500 for the actor document of the community
// name, on any host, and returns its fetch count.
func serveFailingGroupOrigin(rig *instanceRig, name string) func() int {
	origin := rig.h.serveActorOrigin("/c/"+name, map[string]any{})
	origin.answer(http.StatusInternalServerError)
	return origin.fetchCount
}

// TestSweep_PostOnlyCommunityIsAddedLikeAListedOne: the host's community list
// returns only listed, and its one post is in post-only. A post-only community
// that is excluded, tracked or named in communities is never a candidate; the
// named one's Group fails, so a second try would show as a second fetch and a
// second Failed entry. Fetches are counted only where post-only is not added: a
// successful add fetches its Group more than once.
func TestSweep_PostOnlyCommunityIsAddedLikeAListedOne(t *testing.T) {
	const (
		host       = lemmyFixtureHost
		listedID   = "https://lemmy.world/c/listed"
		postOnlyID = "https://lemmy.world/c/post-only"
	)
	servedGroup := func(rig *instanceRig) func() int {
		rig.addPostOnlyCommunity(host, "post-only")
		return func() int { return rig.fetchCounts(postOnlyID)[0] }
	}
	for _, tc := range []struct {
		name       string
		followList string
		// arrange serves post-only's actor document and returns its fetch count.
		arrange        func(rig *instanceRig) func() int
		wantSubscribed []string
		wantFailed     []string
		// wantFetches is checked when post-only is not added.
		wantFetches int
	}{
		{
			name:           "a community only a post names",
			followList:     instanceFollowList(host),
			arrange:        servedGroup,
			wantSubscribed: []string{postOnlyID, listedID},
			wantFailed:     []string{},
		},
		{
			name: "excluded by the host's entry",
			followList: `communities: []
instances:
  - host: lemmy.world
    exclude:
      - "!post-only@lemmy.world"
`,
			arrange:        servedGroup,
			wantSubscribed: []string{listedID},
			wantFailed:     []string{},
		},
		{
			name:       "already tracked",
			followList: instanceFollowList(host),
			arrange: func(rig *instanceRig) func() int {
				rig.track(store.FollowStateAccepted, true, postOnlyID)
				return servedGroup(rig)
			},
			wantSubscribed: []string{listedID},
			wantFailed:     []string{},
		},
		{
			name: "named in communities, with a Group that fails",
			followList: `communities:
  - "https://lemmy.world/c/post-only"
instances:
  - host: lemmy.world
`,
			arrange:        func(rig *instanceRig) func() int { return serveFailingGroupOrigin(rig, "post-only") },
			wantSubscribed: []string{listedID},
			wantFailed:     []string{postOnlyID},
			wantFetches:    1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			rig := newInstanceRig(t, h, tc.followList, instanceRigOptions{})
			rig.addCommunity(host, "listed")
			fetches := tc.arrange(rig)
			rig.post(host, postOnlyID)

			result := rig.sweep()

			assert.Equal(t, tc.wantSubscribed, result.Subscribed)
			assert.Equal(t, tc.wantFailed, result.Failed)
			assert.Empty(t, result.Unsubscribed)
			if !slices.Contains(tc.wantSubscribed, postOnlyID) {
				assert.Equal(t, tc.wantFetches, fetches(), "post-only's actor document fetches")
			}
			follows, undos := rig.deliveredSince(0)
			assert.Equal(t, tc.wantSubscribed, follows)
			assert.Empty(t, undos)
			for _, id := range tc.wantSubscribed {
				assert.Equal(t, store.FollowStatePending, rig.followState(id), id)
			}
		})
	}
}

// TestSweep_PostDiscoveredCommunitiesShareTheBatch: mixed-01 to mixed-31. The
// community list returns 01 and the odd ones from 03, and posts name 01 and the
// even ones, so 16 communities come from the post walk, 01 from both sources at
// once, and 15 from the list alone. Newest first, the posts name 01, then 30
// down to 02, against actor id order. The first sweep takes the 16 the walk
// found, each once and in actor id order, then the lowest listed ones up to 20;
// the second takes the listed rest. New posts in 01 and 02 before it, both
// added by then, do not add them again.
func TestSweep_PostDiscoveredCommunitiesShareTheBatch(t *testing.T) {
	const host = lemmyFixtureHost
	h := newHarness(t)
	rig := newInstanceRig(t, h, instanceFollowList(host), instanceRigOptions{postLimit: 2})
	mixed := numberedIDs("https://lemmy.world/c/mixed-", 31)
	rig.addCommunity(host, "mixed-01")
	for number := 3; number <= 31; number += 2 {
		rig.addCommunity(host, fmt.Sprintf("mixed-%02d", number))
	}
	for number := 2; number <= 30; number += 2 {
		rig.addPostOnlyCommunity(host, fmt.Sprintf("mixed-%02d", number))
		rig.post(host, mixed[number-1])
	}
	rig.post(host, mixed[0])
	firstBatch := []string{
		mixed[0], mixed[1], mixed[3], mixed[5], mixed[7], mixed[9], mixed[11], mixed[13], mixed[15],
		mixed[17], mixed[19], mixed[21], mixed[23], mixed[25], mixed[27], mixed[29],
		mixed[2], mixed[4], mixed[6], mixed[8],
	}
	secondBatch := []string{
		mixed[10], mixed[12], mixed[14], mixed[16], mixed[18], mixed[20], mixed[22], mixed[24],
		mixed[26], mixed[28], mixed[30],
	}

	result := rig.sweep()

	require.Equal(t, firstBatch, result.Subscribed, "what the walk found first, then the lowest listed ones, each once")
	requireConverged(t, result)

	rig.post(host, mixed[0], mixed[1])

	result = rig.sweep()

	assert.Equal(t, secondBatch, result.Subscribed, "the listed leftovers")
	requireConverged(t, result)
	follows, undos := rig.deliveredSince(0)
	assert.Equal(t, slices.Concat(firstBatch, secondBatch), follows)
	assert.Empty(t, undos)
}

// TestSweep_PostOnlyLeftoversDoNotHoldTheListingPosition: a cap of 2 pages over
// 101 tracked communities, so a host that moves reads page 3 next and a host
// that holds reads pages 1 and 2 again. Its 21 post-only communities leave one
// over, which the listing never returned. post-only-21 is posted in first, so the
// second sweep's walk does not return it.
func TestSweep_PostOnlyLeftoversDoNotHoldTheListingPosition(t *testing.T) {
	const host = lemmyFixtureHost
	h := newHarness(t)
	rig := newInstanceRig(t, h, instanceFollowList(host), instanceRigOptions{maxPages: 2, postLimit: 2})
	rig.fixture.addCommunities(host, 101)
	rig.track(store.FollowStateAccepted, true, lemmyActorIDs(host, 1, 101)...)
	postOnly := numberedIDs("https://lemmy.world/c/post-only-", 21)
	for _, name := range numberedIDs("post-only-", 21) {
		rig.addPostOnlyCommunity(host, name)
	}
	rig.post(host, postOnly[20])
	rig.post(host, postOnly[:20]...)
	mark := rig.requestCount()

	result := rig.sweep()

	require.Equal(t, postOnly[:20], result.Subscribed)
	requireConverged(t, result)
	require.Equal(t, oldPages(host, 1, 2), rig.pagesSince(mark))

	mark = rig.requestCount()
	walkMark := rig.walkCount()

	result = rig.sweep()

	assert.Equal(t, oldPages(host, 3, 3), rig.pagesSince(mark), "a post-only leftover does not hold the listing")
	assert.Equal(t, postOnly[20:], result.Subscribed)
	requireConverged(t, result)
	requireWalkReturnedNone(t, rig.walksSince(walkMark), host, postOnly[20])
}

// TestSweep_StoppedPostDiscoveredCommunitiesStayCandidates: the first sweep's
// walk finds held-01 and held-02 while adds are stopped. The tracked community's
// posts are the newest, and a page holds one post, so the walk of the sweep after
// the stop ends returns neither held community: what it adds comes from the
// first walk. A community excluded while held is dropped from what was found:
// once the exclusion is lifted, a third sweep whose walk again returns neither
// does not try it.
func TestSweep_StoppedPostDiscoveredCommunitiesStayCandidates(t *testing.T) {
	const (
		host         = lemmyFixtureHost
		trackedID    = "https://lemmy.world/c/tracked"
		firstHeldID  = "https://lemmy.world/c/held-01"
		secondHeldID = "https://lemmy.world/c/held-02"
	)
	for _, tc := range []struct {
		name string
		stop func(rig *instanceRig)
		// between runs after the stopped sweep, before the stop ends.
		between   func(rig *instanceRig)
		end       func(rig *instanceRig)
		wantAdded []string
		// wantNotFetched lists held communities never tried.
		wantNotFetched []string
		// includeAgain runs a third sweep with the exclusion lifted and no new held post.
		includeAgain bool
	}{
		{
			name:      "a paused budget",
			stop:      func(rig *instanceRig) { rig.budget.RateLimited("3600") },
			end:       func(rig *instanceRig) { rig.clock.Advance(time.Hour) },
			wantAdded: []string{firstHeldID, secondHeldID},
		},
		{
			name:      "a backfill backlog over its limit",
			stop:      func(rig *instanceRig) { rig.h.backfills.backlog = 21 },
			end:       func(rig *instanceRig) { rig.h.backfills.backlog = 0 },
			wantAdded: []string{firstHeldID, secondHeldID},
		},
		{
			name: "a paused budget, with held-02 excluded before it ends",
			stop: func(rig *instanceRig) { rig.budget.RateLimited("3600") },
			between: func(rig *instanceRig) {
				rig.writeFollowList(`communities: []
instances:
  - host: lemmy.world
    exclude:
      - "!held-02@lemmy.world"
`)
			},
			end:            func(rig *instanceRig) { rig.clock.Advance(time.Hour) },
			wantAdded:      []string{firstHeldID},
			wantNotFetched: []string{secondHeldID},
			includeAgain:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			rig := newInstanceRig(t, h, instanceFollowList(host), instanceRigOptions{postLimit: 1})
			rig.addCommunity(host, "tracked")
			rig.track(store.FollowStateAccepted, true, trackedID)
			rig.addPostOnlyCommunity(host, "held-01")
			rig.addPostOnlyCommunity(host, "held-02")
			rig.post(host, firstHeldID, secondHeldID, trackedID)
			tc.stop(rig)

			result := rig.sweep()

			require.Empty(t, result.Subscribed)
			requireConverged(t, result)
			require.Equal(t, []int{0, 0}, rig.fetchCounts(firstHeldID, secondHeldID), "nothing is tried while adds are stopped")

			rig.post(host, trackedID)
			if tc.between != nil {
				tc.between(rig)
			}
			tc.end(rig)
			walkMark := rig.walkCount()

			result = rig.sweep()

			assert.Equal(t, tc.wantAdded, result.Subscribed)
			requireConverged(t, result)
			for _, id := range tc.wantNotFetched {
				assert.Equal(t, []int{0}, rig.fetchCounts(id), "%s is excluded", id)
			}
			requireWalkReturnedNone(t, rig.walksSince(walkMark), host, firstHeldID, secondHeldID)
			if !tc.includeAgain {
				return
			}

			rig.writeFollowList(instanceFollowList(host))
			rig.post(host, trackedID)
			walkMark = rig.walkCount()

			result = rig.sweep()

			assert.NotContains(t, result.Subscribed, secondHeldID, "excluding it dropped it from what was found")
			assert.Equal(t, []int{0}, rig.fetchCounts(secondHeldID), "not tried without being seen in a post again")
			requireConverged(t, result)
			requireWalkReturnedNone(t, rig.walksSince(walkMark), host, secondHeldID)
		})
	}
}

// TestSweep_SkippedPostOnlyCommunityIsRetriedFromWhatWasFound: aa-skipped sorts
// ahead of everything, and is found by the first sweep's walk only: the tracked
// community's post is newer and a page holds one post, so later walks stop
// before it. The second sweep, a second before the skip ends, has 20 healthy
// listed candidates, and tries all of them.
func TestSweep_SkippedPostOnlyCommunityIsRetriedFromWhatWasFound(t *testing.T) {
	const (
		host      = lemmyFixtureHost
		skippedID = "https://lemmy.world/c/aa-skipped"
		trackedID = "https://lemmy.world/c/tracked"
	)
	for _, tc := range []struct {
		name string
		// serve serves aa-skipped's actor document and returns its fetch count.
		serve func(rig *instanceRig) func() int
		skip  time.Duration
	}{
		{
			name: "a #nobridge refusal",
			serve: func(rig *instanceRig) func() int {
				rig.serveGroup(skippedID, map[string]any{"summary": "<p>#nobridge</p>"})
				return func() int { return rig.fetchCounts(skippedID)[0] }
			},
			skip: 24 * time.Hour,
		},
		{
			name:  "a Group that fails",
			serve: func(rig *instanceRig) func() int { return serveFailingGroupOrigin(rig, "aa-skipped") },
			skip:  time.Hour,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			rig := newInstanceRig(t, h, instanceFollowList(host), instanceRigOptions{postLimit: 1})
			rig.addCommunity(host, "tracked")
			rig.track(store.FollowStateAccepted, true, trackedID)
			fetches := tc.serve(rig)
			rig.post(host, skippedID, trackedID)

			result := rig.sweep()

			require.Equal(t, []string{skippedID}, result.Failed)
			require.Empty(t, result.Subscribed)
			require.Equal(t, 1, fetches())

			healthy := numberedIDs("https://lemmy.world/c/healthy-", 20)
			for _, name := range numberedIDs("healthy-", 20) {
				rig.addCommunity(host, name)
			}
			rig.clock.Advance(tc.skip - time.Second)
			walkMark := rig.walkCount()

			result = rig.sweep()

			require.Equal(t, healthy, result.Subscribed, "a skipped community uses no slot")
			require.Empty(t, result.Failed)
			require.Equal(t, 1, fetches(), "inside its skip")
			requireWalkReturnedNone(t, rig.walksSince(walkMark), host, skippedID)

			rig.clock.Advance(time.Second)
			walkMark = rig.walkCount()

			result = rig.sweep()

			assert.Equal(t, []string{skippedID}, result.Failed, "tried again once the skip ends")
			assert.Empty(t, result.Subscribed)
			assert.Equal(t, 2, fetches())
			requireWalkReturnedNone(t, rig.walksSince(walkMark), host, skippedID)
			follows, undos := rig.deliveredSince(0)
			assert.Equal(t, healthy, follows)
			assert.Empty(t, undos)
		})
	}
}

// TestSweep_FailedPostWalkKeepsItsMark: pages hold 2 posts. The first sweep's
// clean walk reads posts 1 to 3 and leaves its mark at post 3. Posts 4 to 9 then
// make pages [9 8] [7 6] [5 4] [3 2], page 1 in page-one alone, and the second
// sweep's walk fails. The third sweep's walk must read back to post 3; a mark
// moved to post 9 would end it after page 1.
func TestSweep_FailedPostWalkKeepsItsMark(t *testing.T) {
	const (
		host      = lemmyFixtureHost
		pageOneID = "https://lemmy.world/c/page-one"
	)
	tracked := []string{"https://lemmy.world/c/tracked-01", "https://lemmy.world/c/tracked-02"}
	for _, tc := range []struct {
		name         string
		failedCursor string
		// wantCursors are the failing walk's requests.
		wantCursors    []string
		wantSubscribed []string
		wantLater      []string
	}{
		{
			name:           "on a later request",
			failedCursor:   "P8",
			wantCursors:    []string{"", "P8"},
			wantSubscribed: []string{pageOneID},
			wantLater:      []string{},
		},
		{
			name:           "on the first request",
			failedCursor:   "",
			wantCursors:    []string{""},
			wantSubscribed: []string{},
			wantLater:      []string{pageOneID},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			rig := newInstanceRig(t, h, instanceFollowList(host), instanceRigOptions{postLimit: 2})
			rig.addCommunity(host, "tracked-01")
			rig.addCommunity(host, "tracked-02")
			rig.track(store.FollowStateAccepted, true, tracked...)
			rig.addPostOnlyCommunity(host, "page-one")
			rig.post(host, tracked[0], tracked[1], tracked[0])
			mark := rig.postRequestCount()

			result := rig.sweep()

			require.Equal(t, []string{"", "P2"}, rig.postCursorsSince(mark, host), "a first walk reads to the short page")
			require.Empty(t, result.Subscribed)
			requireConverged(t, result)

			rig.post(host, tracked[0], tracked[1], tracked[0], tracked[1], pageOneID, pageOneID)
			rig.fixture.failPosts(host, tc.failedCursor, http.StatusInternalServerError)
			mark = rig.postRequestCount()

			result = rig.sweep()

			require.Equal(t, tc.wantCursors, rig.postCursorsSince(mark, host))
			require.Equal(t, tc.wantSubscribed, result.Subscribed, "the communities of the pages read before the failure")
			requireConverged(t, result)
			require.Equal(t, []store.FollowState{store.FollowStateAccepted, store.FollowStateAccepted},
				[]store.FollowState{rig.followState(tracked[0]), rig.followState(tracked[1])}, "nothing is unfollowed")

			rig.fixture.clearInjections()
			mark = rig.postRequestCount()

			result = rig.sweep()

			assert.Equal(t, []string{"", "P8", "P6", "P4"}, rig.postCursorsSince(mark, host),
				"the walk after the failure reads back to the old mark, post 3")
			assert.Equal(t, tc.wantLater, result.Subscribed)
			requireConverged(t, result)
			follows, undos := rig.deliveredSince(0)
			assert.Equal(t, []string{pageOneID}, follows)
			assert.Empty(t, undos)
		})
	}
}

// TestSweep_ReaddedInstanceEntryWalksFromTheStart: the host's three posts are in
// an excluded community, so no sweep adds anything. Pages hold 2 posts: a walk
// with no mark reads [3 2] and the short [1], one from the mark at post 3 stops
// after [3 2].
func TestSweep_ReaddedInstanceEntryWalksFromTheStart(t *testing.T) {
	const (
		host  = lemmyFixtureHost
		quiet = `communities: []
instances:
  - host: lemmy.world
    exclude:
      - "!quiet@lemmy.world"
`
	)
	h := newHarness(t)
	rig := newInstanceRig(t, h, quiet, instanceRigOptions{postLimit: 2})
	rig.post(host, "https://lemmy.world/c/quiet", "https://lemmy.world/c/quiet", "https://lemmy.world/c/quiet")

	for _, step := range []struct {
		name        string
		followList  string
		wantCursors []string
	}{
		{name: "the first walk has no mark", wantCursors: []string{"", "P2"}},
		{name: "the next starts from the mark", wantCursors: []string{""}},
		{name: "a host no longer listed is not walked", followList: "communities: []\n", wantCursors: []string{}},
		{name: "the re-added host has no mark", followList: quiet, wantCursors: []string{"", "P2"}},
	} {
		if step.followList != "" {
			rig.writeFollowList(step.followList)
		}
		mark := rig.postRequestCount()

		result := rig.sweep()

		require.Equal(t, step.wantCursors, rig.postCursorsSince(mark, host), step.name)
		require.Empty(t, result.Subscribed, step.name)
		requireConverged(t, result)
	}
}
