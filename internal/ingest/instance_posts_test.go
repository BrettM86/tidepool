package ingest

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

// postMinute is minute n of 2026-10-01 UTC, the published time of the post
// tests' posts.
func postMinute(n int) time.Time {
	return time.Date(2026, time.October, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(n) * time.Minute)
}

// postedPosts are posts first to last, post n published at postMinute(n) in
// https://lemmy.world/c/posted-n.
func postedPosts(first, last int) []lemmyPost {
	posts := make([]lemmyPost, 0, last-first+1)
	for n := first; n <= last; n++ {
		posts = append(posts, lemmyPost{
			id: int64(n), published: postMinute(n), community: fmt.Sprintf("https://lemmy.world/c/posted-%d", n),
		})
	}
	return posts
}

// postWalkQuery is the query every post walk request carries, with the
// page_cursor when cursor is not empty.
func postWalkQuery(limit, cursor string) url.Values {
	query := url.Values{"type_": {"Local"}, "sort": {"New"}, "show_nsfw": {"true"}, "limit": {limit}}
	if cursor != "" {
		query.Set("page_cursor", cursor)
	}
	return query
}

// requireMark compares two marks, published times as instants.
func requireMark(t *testing.T, want, got PostMark) {
	t.Helper()
	require.Equal(t, want.PostID, got.PostID, "mark post id, in %+v", got)
	require.True(t, want.Published.Equal(got.Published), "mark published: want %s, got %s", want.Published, got.Published)
}

// postLogHas reports whether one record at level (all levels when empty) holds
// every text in its message or attributes.
func postLogHas(t *testing.T, logs *syncBuffer, level string, texts ...string) bool {
	t.Helper()
	for _, record := range listingLogRecords(t, logs, level) {
		line := fmt.Sprint(record)
		if !slices.ContainsFunc(texts, func(text string) bool { return !strings.Contains(line, text) }) {
			return true
		}
	}
	return false
}

// TestRecentPostCommunities_WalksToTheShortPage: 120 posts make two full pages
// of 50 and a short third of 20. Post 7 is featured and published after every
// other post, so it leads page 1 and must not set Newest; post 60 is in a
// community on another host.
func TestRecentPostCommunities_WalksToTheShortPage(t *testing.T) {
	communities := []string{
		"https://lemmy.world/c/alpha", "https://lemmy.world/c/beta",
		"https://lemmy.world/c/gamma", "https://lemmy.world/c/delta",
	}
	fixture := newLemmyFixture(t)
	for id := 1; id <= 120; id++ {
		post := lemmyPost{id: int64(id), published: postMinute(id), community: communities[id%4]}
		switch id {
		case 7:
			post.featuredLocal = true
			post.published = time.Date(2026, time.October, 2, 0, 0, 0, 0, time.UTC)
			post.community = "https://lemmy.world/c/pinned"
		case 60:
			post.community = "https://other.example/c/foreign"
		}
		fixture.addPosts(lemmyFixtureHost, post)
	}
	lister, logs := newFixtureLister(t, fixture, 0)

	walk := lister.RecentPostCommunities(context.Background(), lemmyFixtureHost, PostMark{})

	requests := fixture.recordedPosts()
	require.Len(t, requests, 3, "two full pages, then the short third ends the walk")
	// Page 1 is post 7 then 120 down to 72, page 2 is 71 down to 22.
	for index, cursor := range []string{"", "P48", "P16"} {
		assert.Equal(t, lemmyFixtureHost, requests[index].host)
		assert.Equal(t, "https", requests[index].scheme)
		assert.Equal(t, listingUserAgent, requests[index].userAgent)
		assert.Equal(t, postWalkQuery("50", cursor), requests[index].query, "request %d", index+1)
	}
	assert.ElementsMatch(t, []string{
		"https://lemmy.world/c/alpha", "https://lemmy.world/c/beta", "https://lemmy.world/c/gamma",
		"https://lemmy.world/c/delta", "https://lemmy.world/c/pinned",
	}, walk.ActorIDs, "every community once, the foreign one dropped")
	requireMark(t, PostMark{Published: time.Date(2026, time.October, 1, 2, 0, 0, 0, time.UTC), PostID: 120}, walk.Newest)
	assert.NoError(t, walk.Err)
	assert.True(t, postLogHas(t, logs, "WARN", "https://other.example/c/foreign", lemmyFixtureHost),
		"the dropped post's community is logged with the host")
}

// TestRecentPostCommunities_StopsAtTheMark: with a page of 2, two older
// featured posts lead, then 13, 12 and 11, newer than the mark (post 10 at minute
// 10). The page holding the first non-featured post at or before the mark is the
// last read. Posts at or before the mark are in communities already returned, so
// the rows do not depend on whether their communities are kept.
func TestRecentPostCommunities_StopsAtTheMark(t *testing.T) {
	mark := PostMark{Published: time.Date(2026, time.October, 1, 0, 10, 0, 0, time.UTC), PostID: 10}
	leading := []lemmyPost{
		{id: 4, published: postMinute(4), featuredLocal: true, community: "https://lemmy.world/c/pinned"},
		{id: 3, published: postMinute(3), featuredLocal: true, community: "https://lemmy.world/c/pinned"},
		{id: 13, published: postMinute(13), community: "https://lemmy.world/c/thirteen"},
		{id: 12, published: postMinute(12), community: "https://lemmy.world/c/twelve"},
		{id: 11, published: postMinute(11), community: "https://lemmy.world/c/eleven"},
	}
	for _, tc := range []struct {
		name        string
		rest        []lemmyPost
		wantCursors []string
		wantIDs     []string
	}{
		{
			// Pages: [4 3] [13 12] [11 10] [9 8]. Post 10 is the mark itself.
			name: "the post at the mark",
			rest: []lemmyPost{
				{id: 10, published: postMinute(10), community: "https://lemmy.world/c/eleven"},
				{id: 9, published: postMinute(9), community: "https://lemmy.world/c/twelve"},
				{id: 8, published: postMinute(8), community: "https://lemmy.world/c/twelve"},
			},
			wantCursors: []string{"", "P3", "Pc"},
			wantIDs: []string{
				"https://lemmy.world/c/pinned", "https://lemmy.world/c/thirteen",
				"https://lemmy.world/c/twelve", "https://lemmy.world/c/eleven",
			},
		},
		{
			// Pages: [4 3] [13 12] [11 9] [8 7]. Post 9 has the mark's published time.
			name: "the mark's published time with a lower id",
			rest: []lemmyPost{
				{id: 9, published: postMinute(10), community: "https://lemmy.world/c/eleven"},
				{id: 8, published: postMinute(8), community: "https://lemmy.world/c/twelve"},
				{id: 7, published: postMinute(7), community: "https://lemmy.world/c/twelve"},
			},
			wantCursors: []string{"", "P3", "Pc"},
			wantIDs: []string{
				"https://lemmy.world/c/pinned", "https://lemmy.world/c/thirteen",
				"https://lemmy.world/c/twelve", "https://lemmy.world/c/eleven",
			},
		},
		{
			// Pages: [4 3] [13 12] [11 14] [9 8] [7 6]. Post 14 has the mark's
			// published time and a higher id, so it is newer than the mark.
			name: "the mark's published time with a higher id does not stop it",
			rest: []lemmyPost{
				{id: 14, published: postMinute(10), community: "https://lemmy.world/c/fourteen"},
				{id: 9, published: postMinute(9), community: "https://lemmy.world/c/twelve"},
				{id: 8, published: postMinute(8), community: "https://lemmy.world/c/twelve"},
				{id: 7, published: postMinute(7), community: "https://lemmy.world/c/twelve"},
				{id: 6, published: postMinute(6), community: "https://lemmy.world/c/twelve"},
			},
			wantCursors: []string{"", "P3", "Pc", "Pe"},
			wantIDs: []string{
				"https://lemmy.world/c/pinned", "https://lemmy.world/c/thirteen", "https://lemmy.world/c/twelve",
				"https://lemmy.world/c/eleven", "https://lemmy.world/c/fourteen",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newLemmyFixture(t)
			fixture.addPosts(lemmyFixtureHost, leading...)
			fixture.addPosts(lemmyFixtureHost, tc.rest...)
			lister, _ := newFixtureListerWith(t, fixture, LemmyListerOptions{PostLimit: 2})

			walk := lister.RecentPostCommunities(context.Background(), lemmyFixtureHost, mark)

			assert.Equal(t, tc.wantCursors, fixture.postCursorsRequested(lemmyFixtureHost),
				"not stopped by the featured posts, and no request after the page reaching the mark")
			assert.ElementsMatch(t, tc.wantIDs, walk.ActorIDs)
			requireMark(t, PostMark{Published: time.Date(2026, time.October, 1, 0, 13, 0, 0, time.UTC), PostID: 13}, walk.Newest)
			assert.NoError(t, walk.Err)
		})
	}
}

// TestRecentPostCommunities_CapStopsTheWalkWithoutFailing: posts 1 to 10 make
// five full pages of 2, and a cap of 2 reads [10 9] and [8 7].
func TestRecentPostCommunities_CapStopsTheWalkWithoutFailing(t *testing.T) {
	for _, tc := range []struct {
		name  string
		since PostMark
	}{
		{"a zero mark", PostMark{}},
		{"a mark on page 5", PostMark{Published: time.Date(2026, time.October, 1, 0, 1, 0, 0, time.UTC), PostID: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newLemmyFixture(t)
			fixture.addPosts(lemmyFixtureHost, postedPosts(1, 10)...)
			lister, logs := newFixtureListerWith(t, fixture, LemmyListerOptions{MaxPostPages: 2, PostLimit: 2})

			walk := lister.RecentPostCommunities(context.Background(), lemmyFixtureHost, tc.since)

			assert.Equal(t, []string{"", "P9"}, fixture.postCursorsRequested(lemmyFixtureHost))
			assert.ElementsMatch(t, []string{
				"https://lemmy.world/c/posted-10", "https://lemmy.world/c/posted-9",
				"https://lemmy.world/c/posted-8", "https://lemmy.world/c/posted-7",
			}, walk.ActorIDs)
			require.NoError(t, walk.Err, "reaching the cap is not a failure")
			requireMark(t, PostMark{Published: time.Date(2026, time.October, 1, 0, 10, 0, 0, time.UTC), PostID: 10}, walk.Newest)
			assert.True(t, postLogHas(t, logs, "INFO", lemmyFixtureHost), "the cap is logged at info with the host")
			assert.False(t, postLogHas(t, logs, "", "post walk failed"))
		})
	}
}

// TestRecentPostCommunities_FailedWalkReturnsWhatItHas: posts 1 to 10 in pages
// of 2. Page 1 is [10 9]; the second request, after P9, fails.
func TestRecentPostCommunities_FailedWalkReturnsWhatItHas(t *testing.T) {
	// postView is one post as the walk reads it.
	postView := func(id int, published string) string {
		view := `{"post":{"id":%d,"published":%q,"featured_local":false},` +
			`"community":{"actor_id":"https://lemmy.world/c/injected"}}`
		return fmt.Sprintf(view, id, published)
	}
	for _, tc := range []struct {
		name   string
		inject func(*lemmyFixture)
	}{
		{"500", func(fixture *lemmyFixture) {
			fixture.failPosts(lemmyFixtureHost, "P9", http.StatusInternalServerError)
		}},
		{"body over 8 MiB", func(fixture *lemmyFixture) {
			fixture.padPosts(lemmyFixtureHost, "P9", 8*1024*1024+1)
		}},
		{"full page with no next_page", func(fixture *lemmyFixture) {
			fixture.servePostsBody(lemmyFixtureHost, "P9", `{"posts":[`+
				postView(8, "2026-10-01T00:08:00.000000Z")+","+postView(7, "2026-10-01T00:07:00.000000Z")+`]}`)
		}},
		{"post with no published", func(fixture *lemmyFixture) {
			fixture.servePostsBody(lemmyFixtureHost, "P9", `{"posts":[`+
				postView(8, "2026-10-01T00:08:00.000000Z")+","+
				`{"post":{"id":7,"featured_local":false},"community":{"actor_id":"https://lemmy.world/c/injected"}}`+
				`],"next_page":"P7"}`)
		}},
		{"body that does not decode", func(fixture *lemmyFixture) {
			fixture.servePostsBody(lemmyFixtureHost, "P9", "<html>bad gateway</html>")
		}},
		{"no posts array", func(fixture *lemmyFixture) {
			fixture.servePostsBody(lemmyFixtureHost, "P9", `{}`)
		}},
		{"more posts than the limit", func(fixture *lemmyFixture) {
			fixture.servePostsBody(lemmyFixtureHost, "P9", `{"posts":[`+
				postView(8, "2026-10-01T00:08:00.000000Z")+","+postView(7, "2026-10-01T00:07:00.000000Z")+","+
				postView(6, "2026-10-01T00:06:00.000000Z")+`],"next_page":"P6"}`)
		}},
		{"post with no id", func(fixture *lemmyFixture) {
			fixture.servePostsBody(lemmyFixtureHost, "P9", `{"posts":[`+
				postView(8, "2026-10-01T00:08:00.000000Z")+","+
				`{"post":{"published":"2026-10-01T00:07:00.000000Z","featured_local":false},`+
				`"community":{"actor_id":"https://lemmy.world/c/injected"}}`+
				`],"next_page":"P8"}`)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newLemmyFixture(t)
			fixture.addPosts(lemmyFixtureHost, postedPosts(1, 10)...)
			tc.inject(fixture)
			lister, logs := newFixtureListerWith(t, fixture, LemmyListerOptions{PostLimit: 2})

			walk := lister.RecentPostCommunities(context.Background(), lemmyFixtureHost, PostMark{})

			assert.Equal(t, []string{"", "P9"}, fixture.postCursorsRequested(lemmyFixtureHost), "the walk stops at the failure")
			assert.ElementsMatch(t, []string{
				"https://lemmy.world/c/posted-10", "https://lemmy.world/c/posted-9",
			}, walk.ActorIDs, "the first page's communities, and none from the failed one")
			assert.Error(t, walk.Err)
			assert.True(t, postLogHas(t, logs, "", "post walk failed", lemmyFixtureHost),
				"the failure is logged with the host")
		})
	}
}

func TestRecentPostCommunities_ReadsABodyOfExactlyTheSizeLimit(t *testing.T) {
	fixture := newLemmyFixture(t)
	fixture.addPosts(lemmyFixtureHost, postedPosts(1, 4)...)
	fixture.padPosts(lemmyFixtureHost, "P3", 8*1024*1024)
	lister, _ := newFixtureListerWith(t, fixture, LemmyListerOptions{PostLimit: 2})

	walk := lister.RecentPostCommunities(context.Background(), lemmyFixtureHost, PostMark{})

	require.NoError(t, walk.Err)
	assert.Equal(t, []string{"", "P3", "P1"}, fixture.postCursorsRequested(lemmyFixtureHost))
	assert.ElementsMatch(t, []string{
		"https://lemmy.world/c/posted-4", "https://lemmy.world/c/posted-3",
		"https://lemmy.world/c/posted-2", "https://lemmy.world/c/posted-1",
	}, walk.ActorIDs)
}

func TestNewLemmyLister_ValidatesThePostLimit(t *testing.T) {
	for _, tc := range []struct {
		postLimit int
		wantError bool
	}{
		{-1, true},
		{51, true},
		{1, false},
		{50, false},
	} {
		t.Run(fmt.Sprint(tc.postLimit), func(t *testing.T) {
			_, err := NewLemmyLister(LemmyListerOptions{
				Client: http.DefaultClient, Scheme: "https", UserAgent: listingUserAgent, PostLimit: tc.postLimit,
			})
			if !tc.wantError {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.True(t, errors.IsValidation(err), "got %v", err)
		})
	}
}

func TestRecentPostCommunities_ZeroOptionsTakeTheDefaults(t *testing.T) {
	t.Run("a zero PostLimit asks for 50", func(t *testing.T) {
		fixture := newLemmyFixture(t)
		fixture.addPosts(lemmyFixtureHost, postedPosts(1, 3)...)
		lister, _ := newFixtureLister(t, fixture, 0)

		walk := lister.RecentPostCommunities(context.Background(), lemmyFixtureHost, PostMark{})

		requests := fixture.recordedPosts()
		require.Len(t, requests, 1)
		assert.Equal(t, postWalkQuery("50", ""), requests[0].query)
		assert.ElementsMatch(t, []string{
			"https://lemmy.world/c/posted-3", "https://lemmy.world/c/posted-2", "https://lemmy.world/c/posted-1",
		}, walk.ActorIDs)
	})
	t.Run("a zero MaxPostPages takes 20", func(t *testing.T) {
		fixture := newLemmyFixture(t)
		fixture.addPosts(lemmyFixtureHost, postedPosts(1, 50)...) // 25 full pages of 2.
		lister, logs := newFixtureListerWith(t, fixture, LemmyListerOptions{PostLimit: 2})

		walk := lister.RecentPostCommunities(context.Background(), lemmyFixtureHost, PostMark{})

		assert.Len(t, fixture.recordedPosts(), 20)
		assert.Len(t, walk.ActorIDs, 40, "posts 50 down to 11")
		assert.Contains(t, walk.ActorIDs, "https://lemmy.world/c/posted-11")
		assert.NotContains(t, walk.ActorIDs, "https://lemmy.world/c/posted-10")
		require.NoError(t, walk.Err)
		assert.True(t, postLogHas(t, logs, "INFO", lemmyFixtureHost), "the cap is logged at info with the host")
	})
}
