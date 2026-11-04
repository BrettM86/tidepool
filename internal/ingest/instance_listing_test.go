package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

const listingUserAgent = "tidepool-listing-test/1.0"

// oldPosition and newPosition spell positions out with Lemmy's literal sort
// names, not the production constants.
func oldPosition(page int) ListingPosition { return ListingPosition{Sort: "Old", Page: page} }

func newPosition(page int, boundary time.Time) ListingPosition {
	return ListingPosition{Sort: "New", Page: page, Boundary: boundary}
}

// requirePosition compares two positions, boundaries as instants.
func requirePosition(t *testing.T, want, got ListingPosition) {
	t.Helper()
	require.Equal(t, want.Sort, got.Sort, "next sort, in %+v", got)
	require.Equal(t, want.Page, got.Page, "next page, in %+v", got)
	require.True(t, want.Boundary.Equal(got.Boundary), "next boundary: want %s, got %s", want.Boundary, got.Boundary)
}

// newFixtureLister is a real lister over the fixture. Its log lines are JSON,
// so a test can read their level and attributes.
func newFixtureLister(t *testing.T, fixture *lemmyFixture, maxPages int) (*LemmyLister, *syncBuffer) {
	t.Helper()
	return newFixtureListerWith(t, fixture, LemmyListerOptions{MaxPages: maxPages})
}

// newFixtureListerWith is newFixtureLister with the caps and page size a test
// sets in options; the client, scheme, user agent and logger are the fixture's.
func newFixtureListerWith(t *testing.T, fixture *lemmyFixture, options LemmyListerOptions) (*LemmyLister, *syncBuffer) {
	t.Helper()
	logs := &syncBuffer{}
	options.Client = fixture.client()
	options.Scheme = "https"
	options.UserAgent = listingUserAgent
	options.Logger = slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	lister, err := NewLemmyLister(options)
	require.NoError(t, err)
	return lister, logs
}

// listingLogRecords decodes the lister's log lines. An empty level gives all.
func listingLogRecords(t *testing.T, logs *syncBuffer, level string) []map[string]any {
	t.Helper()
	var records []map[string]any
	for line := range strings.Lines(logs.String()) {
		var record map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		if level == "" || record["level"] == level {
			delete(record, "time")
			records = append(records, record)
		}
	}
	return records
}

// listingLogMentions reports whether any record's message or attributes hold text.
func listingLogMentions(t *testing.T, logs *syncBuffer, level, text string) bool {
	t.Helper()
	for _, record := range listingLogRecords(t, logs, level) {
		if strings.Contains(fmt.Sprint(record), text) {
			return true
		}
	}
	return false
}

func TestNewLemmyLister_RejectsNilClient(t *testing.T) {
	_, err := NewLemmyLister(LemmyListerOptions{Scheme: "https", UserAgent: listingUserAgent})
	require.Error(t, err)
	assert.True(t, errors.IsValidation(err), "got %v", err)
}

func TestLemmyLister_ReadsEveryPageToTheShortOne(t *testing.T) {
	for _, tc := range []struct {
		name      string
		count     int
		wantPages []int
	}{
		{"no communities", 0, []int{1}},
		{"one short page", 49, []int{1}},
		{"exactly full pages need one more request", 100, []int{1, 2, 3}},
		{"partly filled last page", 120, []int{1, 2, 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newLemmyFixture(t)
			fixture.addCommunities(lemmyFixtureHost, tc.count)
			lister, _ := newFixtureLister(t, fixture, 0)

			listing := lister.ListCommunities(context.Background(), lemmyFixtureHost, oldPosition(1))

			requests := fixture.recorded()
			require.Len(t, requests, len(tc.wantPages), "one request per page and no more")
			for index, page := range tc.wantPages {
				request := requests[index]
				assert.Equal(t, lemmyFixtureHost, request.host)
				assert.Equal(t, "https", request.scheme)
				assert.Equal(t, listingUserAgent, request.userAgent)
				assert.Equal(t, url.Values{
					"type_": {"Local"}, "show_nsfw": {"true"}, "limit": {"50"},
					"page": {strconv.Itoa(page)}, "sort": {"Old"},
				}, request.query)
			}
			assert.ElementsMatch(t, lemmyActorIDs(lemmyFixtureHost, 1, tc.count), listing.ActorIDs)
			assert.NoError(t, listing.Err)
			requirePosition(t, oldPosition(1), listing.Next)
		})
	}
}

func TestLemmyLister_UsesTheConfiguredScheme(t *testing.T) {
	fixture := newLemmyFixture(t)
	fixture.addCommunities("lemmy", 1) // Its actor_id is https, so this lister drops it.
	fixture.add("lemmy", lemmyCommunity{name: "plain", actorID: "http://lemmy/c/plain"})
	lister, err := NewLemmyLister(LemmyListerOptions{
		Client: fixture.client(), Scheme: "http", UserAgent: listingUserAgent,
		Logger: slog.New(slog.DiscardHandler),
	})
	require.NoError(t, err)

	listing := lister.ListCommunities(context.Background(), "lemmy", oldPosition(1))

	require.NoError(t, listing.Err)
	requests := fixture.recorded()
	require.Len(t, requests, 1)
	assert.Equal(t, "http", requests[0].scheme)
	assert.Equal(t, []string{"http://lemmy/c/plain"}, listing.ActorIDs)
}

// A dropped result still counts toward the page's fullness: page 1 below holds
// 49 good results plus the row's, and page 2 must still be read.
func TestLemmyLister_DropsResultsFromOtherHosts(t *testing.T) {
	for _, tc := range []struct {
		name     string
		actorID  string
		wantKept bool
	}{
		{"another host", "https://other.example/c/foreign", false},
		{"listed host only as user info", "https://lemmy.world@other.example/c/foreign", false},
		{"listed host only as a prefix", "https://lemmy.world.other.example/c/foreign", false},
		{"listed host only in the path", "https://other.example/lemmy.world/c/foreign", false},
		{"not http", "ftp://lemmy.world/c/foreign", false},
		{"no scheme", "lemmy.world/c/foreign", false},
		{"does not parse", "http://lemmy.world:port/c/foreign", false},
		// resolveCommunity matches the scheme exactly, so these reach WebFinger on
		// the host after the @.
		{"uppercase http scheme", "HTTP://lemmy.world/c/x@lemmy.world", false},
		{"uppercase https scheme", "HTTPS://lemmy.world/c/x@other.example", false},
		{"mixed case scheme", "hTTps://lemmy.world/c/x@other.example", false},
		{"not the configured scheme", "http://lemmy.world/c/foreign", false},
		{"user info on the listed host", "https://someone@lemmy.world/c/foreign", false},
		{"empty user info on the listed host", "https://@lemmy.world/c/foreign", false},
		{"a query", "https://lemmy.world/c/foreign?page=2", false},
		{"a fragment", "https://lemmy.world/c/foreign#top", false},
		{"host in another case is the same host", "https://LEMMY.World/c/upper", true},
		{"a port does not change the host", "https://lemmy.world:8536/c/ported", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newLemmyFixture(t)
			fixture.addCommunities(lemmyFixtureHost, 20)
			fixture.add(lemmyFixtureHost, lemmyCommunity{name: "foreign", actorID: tc.actorID})
			fixture.addCommunities(lemmyFixtureHost, 32) // Ordinals 22 to 53: 50 on page 1, 3 on page 2.
			lister, logs := newFixtureLister(t, fixture, 0)

			listing := lister.ListCommunities(context.Background(), lemmyFixtureHost, oldPosition(1))

			assert.Equal(t, []int{1, 2}, fixture.pagesRequested(lemmyFixtureHost, "Old"),
				"a dropped result must not make a full page short")
			want := append(lemmyActorIDs(lemmyFixtureHost, 1, 20), lemmyActorIDs(lemmyFixtureHost, 22, 53)...)
			if tc.wantKept {
				want = append(want, tc.actorID)
			}
			assert.ElementsMatch(t, want, listing.ActorIDs)
			assert.Equal(t, !tc.wantKept, listingLogMentions(t, logs, "", tc.actorID),
				"a dropped result is logged, a kept one is not")
			assert.NoError(t, listing.Err)
		})
	}
}

func TestLemmyLister_StartsAtTheGivenPosition(t *testing.T) {
	for _, tc := range []struct {
		name      string
		start     ListingPosition
		wantSort  string
		wantPages []int
		wantHolds []string
	}{
		{
			name: "sort=Old past page 1", start: oldPosition(3),
			wantSort: "Old", wantPages: []int{3, 4, 5}, wantHolds: lemmyActorIDs(lemmyFixtureHost, 101, 230),
		},
		{
			// Page 2 holds ordinals 131 to 180, page 3 ordinals 81 to 130, and
			// ordinal 100 is on page 3.
			name: "sort=New past page 1 ends at the boundary", start: newPosition(2, lemmyPublishedAt(100)),
			wantSort: "New", wantPages: []int{2, 3}, wantHolds: lemmyActorIDs(lemmyFixtureHost, 101, 180),
		},
		{
			name: "a short sort=New page ends the pass", start: newPosition(5, lemmyPublishedAt(1).Add(-time.Hour)),
			wantSort: "New", wantPages: []int{5}, wantHolds: lemmyActorIDs(lemmyFixtureHost, 1, 30),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newLemmyFixture(t)
			fixture.addCommunities(lemmyFixtureHost, 230)
			lister, logs := newFixtureLister(t, fixture, 0)

			listing := lister.ListCommunities(context.Background(), lemmyFixtureHost, tc.start)

			assert.Equal(t, tc.wantPages, fixture.pagesRequested(lemmyFixtureHost, tc.wantSort))
			assert.Subset(t, listing.ActorIDs, tc.wantHolds)
			assert.NoError(t, listing.Err)
			requirePosition(t, oldPosition(1), listing.Next)
			assert.Empty(t, listingLogRecords(t, logs, "WARN"), "the pass ended by meeting its end")
		})
	}
}

func TestLemmyLister_FailedReadReturnsWhatItHas(t *testing.T) {
	boundary := lemmyPublishedAt(1).Add(-time.Hour)
	failPage := func(sort string, page int) func(*lemmyFixture) {
		return func(fixture *lemmyFixture) {
			fixture.failPage(lemmyFixtureHost, sort, page, http.StatusInternalServerError)
		}
	}
	serveBody := func(page int, body string) func(*lemmyFixture) {
		return func(fixture *lemmyFixture) {
			fixture.servePageBody(lemmyFixtureHost, "Old", page, body)
		}
	}
	for _, tc := range []struct {
		name      string
		start     ListingPosition
		inject    func(*lemmyFixture)
		wantSort  string
		wantPages []int
		wantIDs   []string
		wantNext  ListingPosition
	}{
		{
			name: "500 on page 3", start: oldPosition(1), inject: failPage("Old", 3),
			wantSort: "Old", wantPages: []int{1, 2, 3},
			wantIDs: lemmyActorIDs(lemmyFixtureHost, 1, 100), wantNext: oldPosition(3),
		},
		{
			name: "oversized body on page 3", start: oldPosition(1),
			inject: func(fixture *lemmyFixture) {
				fixture.padPage(lemmyFixtureHost, "Old", 3, maxListingBodyBytes+1)
			},
			wantSort: "Old", wantPages: []int{1, 2, 3},
			wantIDs: lemmyActorIDs(lemmyFixtureHost, 1, 100), wantNext: oldPosition(3),
		},
		{
			name: "body that does not decode", start: oldPosition(1), inject: serveBody(3, "<html>bad gateway</html>"),
			wantSort: "Old", wantPages: []int{1, 2, 3},
			wantIDs: lemmyActorIDs(lemmyFixtureHost, 1, 100), wantNext: oldPosition(3),
		},
		{
			name: "published that does not decode", start: oldPosition(1),
			inject:   serveBody(3, `{"communities":[{"community":{"id":1,"name":"a","actor_id":"https://lemmy.world/c/a","published":"yesterday"}}]}`),
			wantSort: "Old", wantPages: []int{1, 2, 3},
			wantIDs: lemmyActorIDs(lemmyFixtureHost, 1, 100), wantNext: oldPosition(3),
		},
		{
			name: "empty object", start: oldPosition(1), inject: serveBody(3, `{}`),
			wantSort: "Old", wantPages: []int{1, 2, 3},
			wantIDs: lemmyActorIDs(lemmyFixtureHost, 1, 100), wantNext: oldPosition(3),
		},
		{
			name: "null document", start: oldPosition(1), inject: serveBody(3, `null`),
			wantSort: "Old", wantPages: []int{1, 2, 3},
			wantIDs: lemmyActorIDs(lemmyFixtureHost, 1, 100), wantNext: oldPosition(3),
		},
		{
			name: "null communities", start: oldPosition(1), inject: serveBody(3, `{"communities":null}`),
			wantSort: "Old", wantPages: []int{1, 2, 3},
			wantIDs: lemmyActorIDs(lemmyFixtureHost, 1, 100), wantNext: oldPosition(3),
		},
		{
			name: "error document with status 200", start: oldPosition(1), inject: serveBody(3, `{"error":"unknown"}`),
			wantSort: "Old", wantPages: []int{1, 2, 3},
			wantIDs: lemmyActorIDs(lemmyFixtureHost, 1, 100), wantNext: oldPosition(3),
		},
		{
			name: "no communities array on the first page", start: oldPosition(1), inject: serveBody(1, `{}`),
			wantSort: "Old", wantPages: []int{1}, wantNext: oldPosition(1),
		},
		{
			name: "result with no published", start: oldPosition(1),
			inject:   serveBody(3, `{"communities":[{"community":{"id":1,"name":"a","actor_id":"https://lemmy.world/c/a"}}]}`),
			wantSort: "Old", wantPages: []int{1, 2, 3},
			wantIDs: lemmyActorIDs(lemmyFixtureHost, 1, 100), wantNext: oldPosition(3),
		},
		{
			name: "result with a null published", start: oldPosition(1),
			inject:   serveBody(3, `{"communities":[{"community":{"id":1,"name":"a","actor_id":"https://lemmy.world/c/a","published":null}}]}`),
			wantSort: "Old", wantPages: []int{1, 2, 3},
			wantIDs: lemmyActorIDs(lemmyFixtureHost, 1, 100), wantNext: oldPosition(3),
		},
		{
			name: "result with the zero published in sort=New", start: newPosition(2, boundary),
			inject: func(fixture *lemmyFixture) {
				fixture.servePageBody(lemmyFixtureHost, "New", 3,
					`{"communities":[{"community":{"id":1,"name":"a","actor_id":"https://lemmy.world/c/a","published":"0001-01-01T00:00:00Z"}}]}`)
			},
			wantSort: "New", wantPages: []int{2, 3},
			wantIDs: lemmyActorIDs(lemmyFixtureHost, 131, 180), wantNext: newPosition(3, boundary),
		},
		{
			name: "more results than a page holds", start: oldPosition(1),
			inject: func(fixture *lemmyFixture) {
				fixture.servePageBody(lemmyFixtureHost, "Old", 3,
					fixture.page(fixture.hosts[lemmyFixtureHost], "Old", 1, 51))
			},
			wantSort: "Old", wantPages: []int{1, 2, 3},
			wantIDs: lemmyActorIDs(lemmyFixtureHost, 1, 100), wantNext: oldPosition(3),
		},
		{
			name: "first request of a fresh read", start: oldPosition(1), inject: failPage("Old", 1),
			wantSort: "Old", wantPages: []int{1}, wantNext: oldPosition(1),
		},
		{
			name: "first request of a read resumed in sort=Old", start: oldPosition(4), inject: failPage("Old", 4),
			wantSort: "Old", wantPages: []int{4}, wantNext: oldPosition(1),
		},
		{
			name: "first request of a sort=New pass", start: newPosition(1, boundary), inject: failPage("New", 1),
			wantSort: "New", wantPages: []int{1}, wantNext: oldPosition(1),
		},
		{
			name: "first request of a read resumed in sort=New", start: newPosition(3, boundary), inject: failPage("New", 3),
			wantSort: "New", wantPages: []int{3}, wantNext: oldPosition(1),
		},
		{
			name: "later request of a resumed read", start: oldPosition(3), inject: failPage("Old", 4),
			wantSort: "Old", wantPages: []int{3, 4},
			wantIDs: lemmyActorIDs(lemmyFixtureHost, 101, 150), wantNext: oldPosition(4),
		},
		{
			name: "later request of a resumed sort=New read keeps the boundary", start: newPosition(2, boundary), inject: failPage("New", 3),
			wantSort: "New", wantPages: []int{2, 3},
			wantIDs: lemmyActorIDs(lemmyFixtureHost, 131, 180), wantNext: newPosition(3, boundary),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newLemmyFixture(t)
			fixture.addCommunities(lemmyFixtureHost, 230)
			tc.inject(fixture)
			lister, logs := newFixtureLister(t, fixture, 0)

			listing := lister.ListCommunities(context.Background(), lemmyFixtureHost, tc.start)

			assert.Equal(t, tc.wantPages, fixture.pagesRequested(lemmyFixtureHost, tc.wantSort),
				"the read stops at the failure")
			assert.ElementsMatch(t, tc.wantIDs, listing.ActorIDs)
			assert.Error(t, listing.Err)
			requirePosition(t, tc.wantNext, listing.Next)
			assert.True(t, listingLogMentions(t, logs, "", lemmyFixtureHost), "a failed listing is logged with its host")
		})
	}
}

func TestLemmyLister_ReadsABodyOfExactlyTheSizeLimit(t *testing.T) {
	fixture := newLemmyFixture(t)
	fixture.addCommunities(lemmyFixtureHost, 3)
	fixture.padPage(lemmyFixtureHost, "Old", 1, maxListingBodyBytes)
	lister, _ := newFixtureLister(t, fixture, 0)

	listing := lister.ListCommunities(context.Background(), lemmyFixtureHost, oldPosition(1))

	assert.NoError(t, listing.Err)
	assert.ElementsMatch(t, lemmyActorIDs(lemmyFixtureHost, 1, 3), listing.ActorIDs)
}

// The walk of one list in steps of the cap: each read stops at the cap with no
// failure, and the next starts where it left off.
func TestLemmyLister_CapStopsAReadWithoutFailing(t *testing.T) {
	fixture := newLemmyFixture(t)
	fixture.addCommunities(lemmyFixtureHost, 230) // Five pages, the last short.
	lister, logs := newFixtureLister(t, fixture, 2)
	ctx := context.Background()

	listing := lister.ListCommunities(ctx, lemmyFixtureHost, oldPosition(1))
	assert.Equal(t, []int{1, 2}, fixture.pagesRequested(lemmyFixtureHost, "Old"))
	assert.ElementsMatch(t, lemmyActorIDs(lemmyFixtureHost, 1, 100), listing.ActorIDs)
	require.NoError(t, listing.Err, "reaching the cap is not a failure")
	requirePosition(t, oldPosition(3), listing.Next)
	assert.True(t, listingLogMentions(t, logs, "INFO", lemmyFixtureHost), "the cap is logged at info")
	assert.Empty(t, listingLogRecords(t, logs, "WARN"))
	assert.Empty(t, listingLogRecords(t, logs, "ERROR"))

	listing = lister.ListCommunities(ctx, lemmyFixtureHost, listing.Next)
	assert.Equal(t, []int{1, 2, 3, 4}, fixture.pagesRequested(lemmyFixtureHost, "Old"))
	assert.ElementsMatch(t, lemmyActorIDs(lemmyFixtureHost, 101, 200), listing.ActorIDs)
	require.NoError(t, listing.Err)
	requirePosition(t, oldPosition(5), listing.Next)

	listing = lister.ListCommunities(ctx, lemmyFixtureHost, listing.Next)
	assert.Equal(t, []int{1, 2, 3, 4, 5}, fixture.pagesRequested(lemmyFixtureHost, "Old"))
	assert.ElementsMatch(t, lemmyActorIDs(lemmyFixtureHost, 201, 230), listing.ActorIDs)
	require.NoError(t, listing.Err)
	requirePosition(t, oldPosition(1), listing.Next)
}

// The boundary is the last result of the page as Lemmy served it, so a dropped
// last result still sets it.
func TestLemmyLister_DroppedLastResultStillSetsTheBoundary(t *testing.T) {
	fixture := newLemmyFixture(t)
	fixture.addCommunities(lemmyFixtureHost, 4999)
	fixture.add(lemmyFixtureHost, lemmyCommunity{name: "foreign", actorID: "https://other.example/c/foreign"})
	lister, _ := newFixtureLister(t, fixture, 0)

	listing := lister.ListCommunities(context.Background(), lemmyFixtureHost, oldPosition(100))

	assert.ElementsMatch(t, lemmyActorIDs(lemmyFixtureHost, 4951, 4999), listing.ActorIDs)
	require.NoError(t, listing.Err)
	requirePosition(t, newPosition(1, lemmyPublishedAt(5000)), listing.Next)
}

func TestLemmyLister_WalksBothEndsOfTheList(t *testing.T) {
	for _, tc := range []struct {
		name             string
		count            int
		maxPages         int
		wantReads        int
		wantNewPages     int
		wantDistinct     int
		wantMissing      []int
		wantReachedLimit int
	}{
		{name: "4,999 never sends sort=New", count: 4999, wantReads: 1, wantDistinct: 4999},
		{name: "5,000 stops on sort=New page 1", count: 5000, wantReads: 2, wantNewPages: 1, wantDistinct: 5000},
		{name: "5,001 stops on sort=New page 1", count: 5001, wantReads: 2, wantNewPages: 1, wantDistinct: 5001},
		{name: "9,999 meets the boundary on sort=New page 100", count: 9999, wantReads: 2, wantNewPages: 100, wantDistinct: 9999},
		{name: "10,000 fills sort=New page 100", count: 10000, wantReads: 2, wantNewPages: 100, wantDistinct: 10000, wantReachedLimit: 1},
		{
			name: "10,001 loses the middle community", count: 10001, wantReads: 2, wantNewPages: 100,
			wantDistinct: 10000, wantMissing: []int{5001}, wantReachedLimit: 1,
		},
		{
			name: "10,001 at a cap of 30", count: 10001, maxPages: 30, wantReads: 8, wantNewPages: 100,
			wantDistinct: 10000, wantMissing: []int{5001}, wantReachedLimit: 1,
		},
		{
			// The third sort=New page, in a read of its own, holds the boundary.
			name: "the boundary survives a capped sort=New read", count: 5101, maxPages: 2,
			wantReads: 52, wantNewPages: 3, wantDistinct: 5101,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newLemmyFixture(t) // A rejected request fails this subtest at cleanup.
			fixture.addCommunities(lemmyFixtureHost, tc.count)
			lister, logs := newFixtureLister(t, fixture, tc.maxPages)

			seen := map[string]struct{}{}
			reachedLimit := 0
			position := oldPosition(1)
			for read := range tc.wantReads {
				listing := lister.ListCommunities(context.Background(), lemmyFixtureHost, position)
				require.NoError(t, listing.Err, "read %d", read+1)
				if listing.ReachedLimit {
					reachedLimit++
				}
				for _, actorID := range listing.ActorIDs {
					seen[actorID] = struct{}{}
				}
				position = listing.Next
				if read+1 < tc.wantReads {
					require.NotEqual(t, oldPosition(1), position, "wrapped early, after read %d", read+1)
				}
			}
			requirePosition(t, oldPosition(1), position)

			oldPages := fixture.pagesRequested(lemmyFixtureHost, "Old")
			require.Len(t, oldPages, 100, "every sort=Old page is read once")
			assert.Equal(t, 1, oldPages[0])
			assert.Equal(t, 100, oldPages[99])
			newPages := fixture.pagesRequested(lemmyFixtureHost, "New")
			require.Len(t, newPages, tc.wantNewPages, "sort=New pages read")
			for index, page := range newPages {
				assert.Equal(t, index+1, page)
			}

			assert.Len(t, seen, tc.wantDistinct)
			for ordinal := 1; ordinal <= tc.count; ordinal++ {
				_, returned := seen[lemmyActorID(lemmyFixtureHost, ordinal)]
				require.Equal(t, !slices.Contains(tc.wantMissing, ordinal), returned, "community %d", ordinal)
			}

			assert.Equal(t, tc.wantReachedLimit, reachedLimit, "listings reporting the limit")
			assert.Empty(t, listingLogRecords(t, logs, "WARN"), "the sweep warns, when the host moves")
		})
	}
}
