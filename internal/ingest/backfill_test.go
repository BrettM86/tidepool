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

	"tidepool/internal/ap"
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
		// The same guard the dispatcher runs, and required for the same reason:
		// a community's outbox carries OUR federated content once native users
		// participate, and this path reaches the materializer with no envelope
		// in front of it.
		Echo:     h.classifier,
		MaxPosts: maxPosts,
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
		assert.Equal(t, materialize.CollectionPostV2, mapping.Collection)
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
	mu          sync.Mutex
	seeded      []string
	communities []string
	err         error
}

func (s *recordingSeeder) SeedPostCounts(_ context.Context, postAPID, communityIRI string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seeded = append(s.seeded, postAPID)
	s.communities = append(s.communities, communityIRI)
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

	seeder := &recordingSeeder{}
	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	require.NoError(t, newSeededBackfill(t, h, seeder).Run(ctx, community, true))
	assert.Equal(t, []string{pageID, secondPageID}, seeder.seeded,
		"each materialized post is seeded once (newest first); replies are not")
	// The seeder must learn which community each post was backfilled from:
	// a post's own host may differ from the community's host, and only the
	// community's host is trusted to report the post's score.
	assert.Equal(t, []string{"https://lemmy.world/c/technology", "https://lemmy.world/c/technology"},
		seeder.communities,
		"each seeding call carries the backfilled community's AP Group IRI")

	// A failing seeder is invisible to the run: no error, and the clean
	// completion still stamps last_backfill_at.
	failing := &recordingSeeder{err: fmt.Errorf("origin API is down")}
	require.NoError(t, newSeededBackfill(t, h, failing).Run(ctx, community, true),
		"seeding failures must never fail the backfill")
	assert.Equal(t, []string{pageID, secondPageID}, failing.seeded,
		"the failing seeder is still invoked per post")
	community, err = h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	assert.NotNil(t, community.LastBackfillAt,
		"a run with seeding failures is still a clean completion")
}

// TestBackfillDoesNotSeedPostsOfAnotherCommunity: a community's outbox can
// list a post that declares a DIFFERENT community. A backfill walks one
// community's history, so content it reaches is bound to that community: the
// foreign post is not materialized at all (its own community's Announce or
// backfill is where it lands), and so it is never seeded from the walked
// community's host either. The walked community's own post is still
// materialized and seeded, so the test cannot pass by doing nothing.
func TestBackfillDoesNotSeedPostsOfAnotherCommunity(t *testing.T) {
	h := newHarness(t)
	h.subscribeTechnology()
	serveOutboxFixtures(t, h)
	const foreignCommunity = "https://lemmy.ml/c/linux"
	h.subscribeCommunityURL(foreignCommunity, "linux")

	// Re-serve the technology outbox with its second post re-addressed to the
	// foreign community (audience and the to-list group IRI).
	outboxRaw, err := os.ReadFile(filepath.Join("..", "ap", "testdata", "outbox_lemmy_world.json"))
	require.NoError(t, err)
	var outbox map[string]any
	require.NoError(t, json.Unmarshal(outboxRaw, &outbox))
	items := outbox["orderedItems"].([]any)
	foreign := items[1].(map[string]any)["object"].(map[string]any)["object"].(map[string]any)
	require.Equal(t, secondPageID, foreign["id"])
	foreign["audience"] = foreignCommunity
	foreign["to"] = []any{foreignCommunity, "https://www.w3.org/ns/activitystreams#Public"}
	h.serveObject("/c/technology/outbox", outbox)

	seeder := &recordingSeeder{}
	b := newSeededBackfill(t, h, seeder)
	ctx := context.Background()
	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	require.NoError(t, b.Run(ctx, community, true))

	mapping, err := h.objects.GetByAPID(ctx, "https://lemmy.world/post/49131386")
	require.NoError(t, err, "the walked community's own post must be materialized")
	assert.Equal(t, materialize.CollectionPostV2, mapping.Collection)
	_, err = h.objects.GetByAPID(ctx, "https://lemmy.world/post/49122698")
	assert.True(t, errors.IsNotFound(err),
		"a post declaring another community must not be materialized by the walk (err=%v)", err)
	// Only the walked community's own post is seeded.
	assert.Equal(t, []string{"https://lemmy.world/post/49131386"}, seeder.seeded,
		"a post declaring another community must not be seeded from the walked community's host")
	assert.Equal(t, []string{"https://lemmy.world/c/technology"}, seeder.communities)
}

// TestBackfillBindsContentToTheWalkedCommunity: a backfill of technology
// materializes only content that belongs to technology. A post whose origin
// names another community, and a comment — listed in the outbox or in a
// technology post's replies collection — that replies into a co-hosted
// community's thread, are not materialized. A post on another instance that
// names technology still lands, in technology.
func TestBackfillBindsContentToTheWalkedCommunity(t *testing.T) {
	const (
		xAuthorID = "https://sopuli.example/u/xPoster"
		techPost  = "https://lemmy.world/post/tech-replies-1"
	)
	page := func(id, author, audience string) map[string]any {
		return map[string]any{
			"type":         "Page",
			"id":           id,
			"attributedTo": author,
			"to":           []any{audience, ap.PublicAudience},
			"audience":     audience,
			"name":         "a post at " + id,
			"source":       map[string]any{"content": "body of " + id, "mediaType": "text/markdown"},
			"published":    "2026-07-09T14:00:00.000000Z",
		}
	}
	create := func(obj map[string]any) map[string]any {
		return map[string]any{
			"type":   "Create",
			"id":     obj["id"].(string) + "/create",
			"actor":  obj["attributedTo"],
			"object": obj,
		}
	}
	techPostWithReplies := page(techPost, personID, groupID)
	techPostWithReplies["replies"] = techPost + "/replies"

	rows := []struct {
		name string
		// items is technology's outbox.
		items []any
		// served are the origin documents the walk dereferences.
		served map[string]map[string]any
		// fetchedPath must be dereferenced by the walk, so a refusal is the
		// binding and not a fixture miss.
		fetchedPath string
		targetID    string
		// wantCommunityDID is where the target lands; empty means it must not
		// be materialized.
		wantCommunityDID string
	}{
		{
			name:  "outbox post whose origin names linux",
			items: []any{create(page("https://sopuli.example/post/x-into-linux", xAuthorID, groupID))},
			served: map[string]map[string]any{
				"/post/x-into-linux": page("https://sopuli.example/post/x-into-linux", xAuthorID, linuxCommunityID),
			},
			fetchedPath: "/post/x-into-linux",
			targetID:    "https://sopuli.example/post/x-into-linux",
		},
		{
			name: "outbox comment replying to linux's post",
			items: []any{create(note("https://lemmy.world/comment/outbox-into-linux", personID, linuxPostID,
				"an outbox reply into linux", "2026-07-09T14:30:00.000000Z"))},
			fetchedPath: "/c/technology/outbox",
			targetID:    "https://lemmy.world/comment/outbox-into-linux",
		},
		{
			// The refusal row above, aimed at technology's own post: the same
			// embedded outbox Note shape does materialize, so that refusal is the
			// binding and not an outbox shape the walk never processes.
			name: "outbox comment replying to technology's post",
			items: []any{create(note("https://lemmy.world/comment/outbox-into-tech", personID, pageID,
				"an outbox reply into technology", "2026-07-09T14:30:00.000000Z"))},
			fetchedPath:      "/c/technology/outbox",
			targetID:         "https://lemmy.world/comment/outbox-into-tech",
			wantCommunityDID: testDIDFor("technology", "lemmy.world"),
		},
		{
			name:  "replies-collection comment replying to linux's post",
			items: []any{create(techPostWithReplies)},
			served: map[string]map[string]any{
				"/post/tech-replies-1/replies": {
					"type":       "OrderedCollection",
					"id":         techPost + "/replies",
					"totalItems": 1,
					"orderedItems": []any{note("https://lemmy.world/comment/replies-into-linux", personID,
						linuxPostID, "a listed reply into linux", "2026-07-09T14:45:00.000000Z")},
				},
			},
			fetchedPath: "/post/tech-replies-1/replies",
			targetID:    "https://lemmy.world/comment/replies-into-linux",
		},
		{
			name:  "cross-instance post naming technology",
			items: []any{create(page("https://sopuli.example/post/x-into-tech", xAuthorID, groupID))},
			served: map[string]map[string]any{
				"/post/x-into-tech": page("https://sopuli.example/post/x-into-tech", xAuthorID, groupID),
			},
			fetchedPath:      "/post/x-into-tech",
			targetID:         "https://sopuli.example/post/x-into-tech",
			wantCommunityDID: testDIDFor("technology", "lemmy.world"),
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			h := newHarness(t)
			h.subscribeTechnology()
			h.serveLemmyWorldContent()
			h.coHostedCommunityPost()
			h.serveObject("/u/xPoster", person(xAuthorID, "xPoster", nil))
			for path, doc := range row.served {
				h.serveObject(path, doc)
			}
			h.serveObject("/c/technology/outbox", map[string]any{
				"type":         "OrderedCollection",
				"id":           groupID + "/outbox",
				"totalItems":   len(row.items),
				"orderedItems": row.items,
			})
			ctx := context.Background()
			community, err := h.communities.GetByAPGroupID(ctx, groupID)
			require.NoError(t, err)

			require.NoError(t, newBackfill(t, h, 10).Run(ctx, community, true))

			require.Equal(t, 1, h.hitCount(row.fetchedPath), "the walk dereferences %s", row.fetchedPath)
			mapping, err := h.objects.GetByAPID(ctx, row.targetID)
			if row.wantCommunityDID == "" {
				assert.True(t, errors.IsNotFound(err),
					"%s must not be materialized by technology's backfill (err=%v)", row.targetID, err)
				return
			}
			require.NoError(t, err, "%s must be materialized", row.targetID)
			assert.Equal(t, row.wantCommunityDID, mapping.CommunityDID)
		})
	}
}

// newSeededBackfill is newBackfill with a CountSeeder wired in.
func newSeededBackfill(t *testing.T, h *harness, seeder CountSeeder) *Backfill {
	t.Helper()
	b, err := NewBackfill(BackfillOptions{
		Fetcher:      h.client,
		Materializer: h.mat,
		Communities:  h.communities,
		Tombstones:   h.tombstones,
		Seeder:       seeder,
		Echo:         h.classifier,
		MaxPosts:     10,
	})
	require.NoError(t, err)
	return b
}

// TestBackfillDoesNotSeedFromARetargetedAudience: a post stays in the
// community it was first materialized into, whatever a later copy's audience
// says. A hostile host authors a post into an honest community, then
// re-addresses it to a community it hosts itself and lists it in that
// community's outbox. The re-addressed body is embedded on the hostile host's
// own authority, so it is used as-is; the post still lives in the honest
// community, and the hostile community's host must not be asked for its counts.
func TestBackfillDoesNotSeedFromARetargetedAudience(t *testing.T) {
	h := newHarness(t)
	h.subscribeTechnology()
	const (
		hostileCommunity = "https://hostile.example/c/takeover"
		hostilePost      = "https://hostile.example/post/777"
		hostileAuthor    = "https://hostile.example/u/mallory"
	)
	hostileGroup := h.subscribeCommunityURL(hostileCommunity, "takeover")
	// subscribeCommunityURL's Group advertises no outbox; re-serve it with one.
	h.serveActorDoc(hostileCommunity, map[string]any{
		"type":              "Group",
		"id":                hostileCommunity,
		"preferredUsername": "takeover",
		"inbox":             hostileCommunity + "/inbox",
		"outbox":            hostileCommunity + "/outbox",
		"endpoints":         map[string]any{"sharedInbox": "https://hostile.example/inbox"},
		"published":         "2024-01-01T00:00:00.000000Z",
	}, &hostileGroup.key.PublicKey)
	h.serveObject("/u/mallory", person(hostileAuthor, "mallory", nil))

	addressedTo := func(community string) map[string]any {
		return map[string]any{
			"id":           hostilePost,
			"type":         "Page",
			"attributedTo": hostileAuthor,
			"audience":     community,
			"to":           []any{community, "https://www.w3.org/ns/activitystreams#Public"},
			"name":         "a post that changes communities",
			"content":      "<p>retargeted</p>",
			"published":    "2026-08-13T09:00:00.000000Z",
		}
	}
	outboxOf := func(community string, items ...any) map[string]any {
		return map[string]any{
			"type":         "OrderedCollection",
			"id":           community + "/outbox",
			"totalItems":   len(items),
			"orderedItems": items,
		}
	}

	// Walk 1: the honest community lists the post addressed to itself. The
	// post's id is cross-authority with lemmy.world, so its body is fetched
	// from the hostile origin — which, at this point, addresses the honest
	// community.
	h.serveObject("/post/777", addressedTo("https://lemmy.world/c/technology"))
	h.serveObject("/c/technology/outbox",
		outboxOf("https://lemmy.world/c/technology", addressedTo("https://lemmy.world/c/technology")))

	seeder := &recordingSeeder{}
	b := newSeededBackfill(t, h, seeder)
	ctx := context.Background()
	honest, err := h.communities.GetByAPGroupID(ctx, "https://lemmy.world/c/technology")
	require.NoError(t, err)
	require.NoError(t, b.Run(ctx, honest, true))
	mapping, err := h.objects.GetByAPID(ctx, hostilePost)
	require.NoError(t, err, "the post must materialize into the honest community")
	require.Equal(t, honest.DID, mapping.CommunityDID)
	require.Equal(t, 1, h.hitCount("/post/777"), "walk 1 fetches the cross-authority body from its origin")

	// Walk 2: the hostile community lists the same post, now addressed to
	// itself. Same authority as the hostile outbox, so the embedded body is
	// trusted without a fetch.
	h.serveObject("/c/takeover/outbox", outboxOf(hostileCommunity, addressedTo(hostileCommunity)))
	hostile, err := h.communities.GetByAPGroupID(ctx, hostileCommunity)
	require.NoError(t, err)
	require.NoError(t, b.Run(ctx, hostile, true))
	require.Equal(t, 1, h.hitCount("/post/777"),
		"walk 2 must use the embedded body (same authority as the hostile outbox)")

	// The stored binding wins: the post is still the honest community's.
	mapping, err = h.objects.GetByAPID(ctx, hostilePost)
	require.NoError(t, err)
	assert.Equal(t, honest.DID, mapping.CommunityDID,
		"a retargeted audience must not move a materialized post")

	// Walk 1 seeded the post from the honest community; walk 2 seeded
	// nothing, so the hostile host was never asked for that post's counts.
	assert.Equal(t, []string{"https://hostile.example/post/777"}, seeder.seeded,
		"a post living in another community must not be seeded from the walked (hostile) community's host")
	assert.Equal(t, []string{"https://lemmy.world/c/technology"}, seeder.communities)
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
	// Scoped to the community being backfilled: its own marker must hold on
	// its own walk (the global-marker case is covered below).
	require.NoError(t, h.tombstones.Record(ctx, replyID, groupID))

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

	// An origin-authorized (global) marker: visible on every community's walk.
	require.NoError(t, h.tombstones.Record(ctx, pageID, ""))
	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	require.NoError(t, b.Run(ctx, community, true))

	_, err = h.objects.GetByAPID(ctx, pageID)
	assert.True(t, errors.IsNotFound(err), "a tombstoned id must not be backfilled")
	_, err = h.objects.GetByAPID(ctx, secondPageID)
	require.NoError(t, err, "other posts still land")
}
