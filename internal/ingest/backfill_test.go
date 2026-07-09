package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/materialize"
)

const secondPageID = "https://lemmy.world/post/49122698"

// newBackfill builds a real Backfill over the harness stack.
func newBackfill(t *testing.T, h *harness, maxPosts int) *Backfill {
	t.Helper()
	b, err := NewBackfill(BackfillOptions{
		Fetcher:      h.client,
		Materializer: h.mat,
		Communities:  h.communities,
		Tombstones:   h.tombstones,
		MaxPosts:     maxPosts,
	})
	require.NoError(t, err)
	return b
}

// serveOutboxFixtures registers the outbox fixture plus everything its
// items need: both pages' authors and a replies collection advertised on
// the first page.
func serveOutboxFixtures(t *testing.T, h *harness) {
	t.Helper()
	h.serveLemmyWorldContent()
	h.serveObject("/u/Aweigh", person("https://lemmy.world/u/Aweigh", "Aweigh", nil))

	outboxRaw, err := os.ReadFile(filepath.Join("..", "ap", "testdata", "outbox_lemmy_world.json"))
	require.NoError(t, err)
	var outbox map[string]any
	require.NoError(t, json.Unmarshal(outboxRaw, &outbox))
	// Advertise a replies collection on the newest page (Lemmy doesn't
	// today, but the spec covers "if advertised").
	items := outbox["orderedItems"].([]any)
	page := items[0].(map[string]any)["object"].(map[string]any)["object"].(map[string]any)
	page["replies"] = pageID + "/replies"
	h.serveObject("/c/technology/outbox", outbox)

	replyID := "https://lemmy.world/comment/3001"
	h.serveObject("/u/replier", person("https://lemmy.world/u/replier", "replier", nil))
	h.serveObject("/post/49131386/replies", map[string]any{
		"type":         "OrderedCollection",
		"id":           pageID + "/replies",
		"totalItems":   1,
		"orderedItems": []any{note(replyID, "https://lemmy.world/u/replier", pageID, "a backfilled reply", "2026-07-07T05:00:00.000000Z")},
	})
}

// TestBackfillProducesMappedHistory is the DoD backfill check: paging the
// group outbox materializes the posts (newest first) and each advertised
// replies collection, then stamps last_backfill_at.
func TestBackfillProducesMappedHistory(t *testing.T) {
	h := newHarness(t)
	h.subscribeTechnology()
	serveOutboxFixtures(t, h)
	b := newBackfill(t, h, 10)
	ctx := context.Background()

	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	require.NoError(t, b.Run(ctx, community, true))

	// Both outbox posts landed.
	for _, id := range []string{pageID, secondPageID} {
		mapping, err := h.objects.GetByAPID(ctx, id)
		require.NoError(t, err, "outbox post %s must be materialized", id)
		assert.Equal(t, materialize.CollectionPost, mapping.Collection)
	}
	// The advertised reply landed too.
	replyMapping, err := h.objects.GetByAPID(ctx, "https://lemmy.world/comment/3001")
	require.NoError(t, err, "advertised replies must be backfilled")
	assert.Equal(t, materialize.CollectionComment, replyMapping.Collection)

	community, err = h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	require.NotNil(t, community.LastBackfillAt, "a clean run stamps last_backfill_at")

	// A fresh un-forced trigger is skipped (resumable-freshness contract).
	before := *community.LastBackfillAt
	require.NoError(t, b.Run(ctx, community, false))
	community, err = h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	assert.WithinDuration(t, before, *community.LastBackfillAt, time.Second,
		"an un-forced re-trigger inside the freshness window must not re-run")
}

// recordingSeeder captures CountSeeder invocations; a non-nil err makes
// every call fail.
type recordingSeeder struct {
	mu     sync.Mutex
	seeded []string
	err    error
}

func (s *recordingSeeder) SeedPostCounts(_ context.Context, postAPID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seeded = append(s.seeded, postAPID)
	return s.err
}

// TestBackfillSeedsVoteCounts: every materialized post gets exactly one
// seeding call (replies do not — comments are not seeded in v1), and the
// seeder is best-effort: a failing one is still invoked per post but never
// affects the run's outcome. Nil-seeder safety is exercised by every other
// backfill test (newBackfill leaves Seeder unset).
func TestBackfillSeedsVoteCounts(t *testing.T) {
	h := newHarness(t)
	h.subscribeTechnology()
	serveOutboxFixtures(t, h)
	ctx := context.Background()

	seeded := func(seeder *recordingSeeder) *Backfill {
		b, err := NewBackfill(BackfillOptions{
			Fetcher:      h.client,
			Materializer: h.mat,
			Communities:  h.communities,
			Tombstones:   h.tombstones,
			Seeder:       seeder,
			MaxPosts:     10,
		})
		require.NoError(t, err)
		return b
	}

	seeder := &recordingSeeder{}
	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	require.NoError(t, seeded(seeder).Run(ctx, community, true))
	assert.Equal(t, []string{pageID, secondPageID}, seeder.seeded,
		"each materialized post is seeded once (newest first); replies are not")

	// A failing seeder is invisible to the run: no error, and the clean
	// completion still stamps last_backfill_at.
	failing := &recordingSeeder{err: fmt.Errorf("origin API is down")}
	require.NoError(t, seeded(failing).Run(ctx, community, true),
		"seeding failures must never fail the backfill")
	assert.Equal(t, []string{pageID, secondPageID}, failing.seeded,
		"the failing seeder is still invoked per post")
	community, err = h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	assert.NotNil(t, community.LastBackfillAt,
		"a run with seeding failures is still a clean completion")
}

// TestBackfillHonorsMaxPosts: the post cap stops the walk early.
func TestBackfillHonorsMaxPosts(t *testing.T) {
	h := newHarness(t)
	h.subscribeTechnology()
	serveOutboxFixtures(t, h)
	b := newBackfill(t, h, 1)
	ctx := context.Background()

	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	require.NoError(t, b.Run(ctx, community, true))

	_, err = h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err, "the newest post lands")
	_, err = h.objects.GetByAPID(ctx, secondPageID)
	assert.True(t, errors.IsNotFound(err), "posts past BACKFILL_MAX_POSTS are not materialized")
}

// TestBackfillSkipsTombstonedReplies: a reply still advertised in the origin's
// replies collection but with a recorded Delete (delivery/collection race) must
// NOT be resurrected during backfill — the same funnel rule the live path and
// materializeOutboxItem enforce (Finding J).
func TestBackfillSkipsTombstonedReplies(t *testing.T) {
	h := newHarness(t)
	h.subscribeTechnology()
	serveOutboxFixtures(t, h)
	b := newBackfill(t, h, 10)
	ctx := context.Background()

	const replyID = "https://lemmy.world/comment/3001"
	require.NoError(t, h.tombstones.Record(ctx, replyID))

	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	require.NoError(t, b.Run(ctx, community, true))

	// The post carrying the replies collection still lands.
	_, err = h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err, "the post still materializes")
	// The tombstoned reply is not resurrected.
	_, err = h.objects.GetByAPID(ctx, replyID)
	assert.True(t, errors.IsNotFound(err), "a tombstoned reply must not be backfilled")
}

// serveTruncatingOutbox re-serves the group outbox as a next-pointer loop so
// FetchCollection returns a CollectionTruncatedError after collecting both
// posts. Requires serveOutboxFixtures to have registered the authors/pages.
func serveTruncatingOutbox(t *testing.T, h *harness) {
	t.Helper()
	outboxRaw, err := os.ReadFile(filepath.Join("..", "ap", "testdata", "outbox_lemmy_world.json"))
	require.NoError(t, err)
	var outbox map[string]any
	require.NoError(t, json.Unmarshal(outboxRaw, &outbox))
	items := outbox["orderedItems"].([]any)

	const (
		p1 = "https://lemmy.world/c/technology/outbox/page1"
		p2 = "https://lemmy.world/c/technology/outbox/page2"
	)
	// Header points at page1; page1→page2→page1 loops, so the walk truncates
	// after both items are collected (never reaching a natural end).
	h.serveObject("/c/technology/outbox", map[string]any{
		"type":  "OrderedCollection",
		"id":    "https://lemmy.world/c/technology/outbox",
		"first": p1,
	})
	h.serveObject("/c/technology/outbox/page1", map[string]any{
		"type": "OrderedCollectionPage", "id": p1, "next": p2,
		"orderedItems": []any{items[0]},
	})
	h.serveObject("/c/technology/outbox/page2", map[string]any{
		"type": "OrderedCollectionPage", "id": p2, "next": p1,
		"orderedItems": []any{items[1]},
	})
}

// TestBackfillTruncatedWalkLeavesResumable: a truncated outbox walk still
// materializes everything collected, but is NOT a clean completion —
// last_backfill_at stays nil so an un-forced re-trigger actually re-walks
// instead of skipping inside the freshness window (Finding L).
func TestBackfillTruncatedWalkLeavesResumable(t *testing.T) {
	h := newHarness(t)
	h.subscribeTechnology()
	serveOutboxFixtures(t, h)
	serveTruncatingOutbox(t, h)
	b := newBackfill(t, h, 100)
	ctx := context.Background()

	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	require.NoError(t, b.Run(ctx, community, true), "truncation is not a fatal error")

	// Both collected posts still materialized.
	for _, id := range []string{pageID, secondPageID} {
		_, err := h.objects.GetByAPID(ctx, id)
		require.NoError(t, err, "collected post %s must still materialize", id)
	}
	// The truncated run is not a completion: last_backfill_at stays nil.
	community, err = h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	require.Nil(t, community.LastBackfillAt,
		"a truncated walk must leave last_backfill_at unset so a re-trigger re-walks")

	// An un-forced re-trigger is NOT skipped (nil last_backfill_at) — it
	// re-walks the outbox, proving resumability.
	before := h.hitCount("/c/technology/outbox")
	require.NoError(t, b.Run(ctx, community, false))
	assert.Greater(t, h.hitCount("/c/technology/outbox"), before,
		"an un-forced re-trigger must re-walk when last_backfill_at is unset")
}

// TestBackfillPartialFailureLeavesResumable: a hard (non-skip) item failure
// leaves last_backfill_at unset so the next trigger retries the walk.
func TestBackfillPartialFailureLeavesResumable(t *testing.T) {
	h := newHarness(t)
	h.subscribeTechnology()
	serveOutboxFixtures(t, h)
	// One good item (the real fixture's fully-embedded page) plus one broken
	// item whose cross-authority object 500s: a hard failure (not a skip), so
	// failures>0.
	outboxRaw, err := os.ReadFile(filepath.Join("..", "ap", "testdata", "outbox_lemmy_world.json"))
	require.NoError(t, err)
	var outbox map[string]any
	require.NoError(t, json.Unmarshal(outboxRaw, &outbox))
	goodItem := outbox["orderedItems"].([]any)[0]

	const brokenID = "https://other.test/post/broken"
	h.mux.HandleFunc("GET /post/broken", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	h.serveObject("/c/technology/outbox", map[string]any{
		"type":       "OrderedCollection",
		"id":         "https://lemmy.world/c/technology/outbox",
		"totalItems": 2,
		"orderedItems": []any{
			goodItem,
			// A broken item: cross-authority id with no embedded body forces a
			// fetch that 500s.
			map[string]any{
				"type":   "Create",
				"actor":  personID,
				"object": map[string]any{"id": brokenID},
			},
		},
	})
	b := newBackfill(t, h, 100)
	ctx := context.Background()

	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	err = b.Run(ctx, community, true)
	require.Error(t, err, "a hard item failure surfaces as a run error")

	// The good post still landed.
	_, err = h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err, "the healthy item still materializes")

	community, err = h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	require.Nil(t, community.LastBackfillAt,
		"a partial-failure run must leave last_backfill_at unset (resumable)")
}

// TestBackfillSkipsTombstonedObjects: deleted-upstream markers hold during
// backfill too.
func TestBackfillSkipsTombstonedObjects(t *testing.T) {
	h := newHarness(t)
	h.subscribeTechnology()
	serveOutboxFixtures(t, h)
	b := newBackfill(t, h, 10)
	ctx := context.Background()

	require.NoError(t, h.tombstones.Record(ctx, pageID))
	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	require.NoError(t, b.Run(ctx, community, true))

	_, err = h.objects.GetByAPID(ctx, pageID)
	assert.True(t, errors.IsNotFound(err), "a tombstoned id must not be backfilled")
	_, err = h.objects.GetByAPID(ctx, secondPageID)
	require.NoError(t, err, "other posts still land")
}
