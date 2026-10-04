package votes

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
)

func TestLemmyPostAPIURL(t *testing.T) {
	tests := []struct {
		name   string
		apID   string
		want   string
		wantOK bool
	}{
		{"lemmy post", "https://lemmy.world/post/49131386",
			"https://lemmy.world/api/v3/post?id=49131386", true},
		{"http with port (tests)", "http://127.0.0.1:8080/post/7",
			"http://127.0.0.1:8080/api/v3/post?id=7", true},
		{"comment", "https://lemmy.world/comment/123", "", false},
		// 17b: the subtraction of our own delivered votes is only correct for a
		// subject whose ORIGIN total includes them — a Lemmy post. For a NATIVE
		// post the origin is us: Coves holds the native tally in its own column,
		// there is no external total to net against, and subtracting would
		// corrupt a number nobody seeded. Nothing enforces that with a check;
		// it is enforced HERE, by the shapes this parser refuses.
		{"a native post's AP id",
			"https://coves.social/ap/object/did:plc:ewvi7nxzyoun6zhxrhs64oiz/social.coves.community.postv2/3lznative0001",
			"", false},
		{"a native object on a vanity origin",
			"https://vanity.example/ap/object/did:plc:ewvi7nxzyoun6zhxrhs64oiz/social.coves.community.postv2/3lznative0002",
			"", false},
		{"non-numeric id", "https://lemmy.world/post/abc", "", false},
		{"trailing path", "https://lemmy.world/post/1/extra", "", false},
		{"empty id", "https://lemmy.world/post/", "", false},
		{"not a url", "::::", "", false},
		{"wrong scheme", "ftp://lemmy.world/post/1", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := lemmyPostAPIURL(tt.apID)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSeedPostCountsFromFakeLemmyAPI(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)

	// The fake Lemmy public API. Tests always talk to loopback httptest
	// servers, never real instances; the guarded client needs
	// allowPrivate=true for that (same rule as every other AP test).
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v3/post", func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "321", r.URL.Query().Get("id"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"post_view":{"counts":{"upvotes":128,"downvotes":9,"score":119}}}`)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	postID := server.URL + "/post/321"
	bridgeSubject(t, objects, postID, "3jzfcijpj2z2a")

	seeder, err := NewLemmySeeder(agg,
		ap.NewGuardedHTTPClient(true, 5*time.Second), "tidepool-test/0", nil)
	require.NoError(t, err)
	require.NoError(t, seeder.SeedPostCounts(context.Background(), postID, server.URL+"/c/test"))

	up, down, found := counts(t, database, postID)
	require.True(t, found)
	assert.Equal(t, 128, up)
	assert.Equal(t, 9, down)
}

func TestSeedPostCountsNonLemmyShapeIsNoOp(t *testing.T) {
	database := testDB(t)
	agg, _ := testAggregator(t, database)
	seeder, err := NewLemmySeeder(agg, ap.NewGuardedHTTPClient(true, time.Second), "", nil)
	require.NoError(t, err)

	// Comments (and anything else that is not /post/N) are not seeded in v1;
	// no fetch happens and no error is returned.
	require.NoError(t, seeder.SeedPostCounts(context.Background(),
		"https://lemmy.world/comment/555", "https://lemmy.world/c/test"))
	_, _, found := counts(t, database, "https://lemmy.world/comment/555")
	assert.False(t, found)
}

// TestSeedPostCountsMalformedResponseKeepsBaseline: a 200 whose body lacks
// the expected post_view.counts nesting (a proxy error page served as JSON,
// renamed fields, the wrong endpoint) or is truncated must return an error
// and must NOT seed — re-seeding replaces the baseline, so decoding absent
// fields to zero would silently overwrite a previously good baseline with
// 0/0 on a backfill redo.
func TestSeedPostCountsMalformedResponseKeepsBaseline(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)

	tests := []struct {
		name string
		body string
	}{
		{"empty object", `{}`},
		{"missing counts", `{"post_view":{"creator_banned":false}}`},
		{"missing count fields", `{"post_view":{"counts":{"score":119}}}`},
		{"error page as json", `{"error":"couldnt_find_post"}`},
		{"truncated json", `{"post_view":{"counts":{"upvotes":128,`},
		{"not json at all", `<html>502 Bad Gateway</html>`},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprint(w, tt.body)
			}))
			t.Cleanup(server.Close)

			postID := fmt.Sprintf("%s/post/%d", server.URL, i+1)
			bridgeSubject(t, objects, postID, fmt.Sprintf("3jzfcijpj2z%da", i))
			// A good baseline from an earlier run: the broken response must
			// leave it untouched.
			require.NoError(t, agg.SeedAggregates(context.Background(), postID, 42, 7))

			seeder, err := NewLemmySeeder(agg,
				ap.NewGuardedHTTPClient(true, 5*time.Second), "tidepool-test/0", nil)
			require.NoError(t, err)
			assert.Error(t, seeder.SeedPostCounts(context.Background(), postID, server.URL+"/c/test"))

			up, down, found := counts(t, database, postID)
			require.True(t, found)
			assert.Equal(t, 42, up, "a malformed response must not overwrite the baseline")
			assert.Equal(t, 7, down, "a malformed response must not overwrite the baseline")
		})
	}
}

func TestSeedPostCountsAPIFailureReturnsError(t *testing.T) {
	database := testDB(t)
	agg, _ := testAggregator(t, database)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)

	seeder, err := NewLemmySeeder(agg, ap.NewGuardedHTTPClient(true, time.Second), "", nil)
	require.NoError(t, err)
	// The caller (backfill) logs and moves on; the seeder just reports it.
	assert.Error(t, seeder.SeedPostCounts(context.Background(), server.URL+"/post/1", server.URL+"/c/test"))
}

// hostilePostOrigin is a post author's own instance that lies about the
// post's score. It answers every request with an inflated count in both the
// /api/v3/post and /api/v3/resolve_object shapes, and counts the requests it
// received. A cross-host post must never be seeded from here.
func hostilePostOrigin(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"post_view":{"counts":{"upvotes":999999,"downvotes":999999}},`+
			`"post":{"post":{"ap_id":"x"},"counts":{"upvotes":999999,"downvotes":999999}}}`)
	}))
	t.Cleanup(server.Close)
	return server, &hits
}

// communityHostResolving is the community's instance. Anonymous
// resolve_object only searches its local DB; when q is the post's AP id it
// answers with hitBody under 200, otherwise with Lemmy 0.19's real miss,
// 400 {"error":"couldnt_find_object"}. An empty hitBody makes every query a
// miss.
func communityHostResolving(t *testing.T, postAPID, hitBody string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v3/resolve_object", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if hitBody == "" || r.URL.Query().Get("q") != postAPID {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprint(w, `{"error":"couldnt_find_object"}`)
			return
		}
		_, _ = fmt.Fprint(w, hitBody)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

// TestSeedPostCountsCrossHostAsksTheCommunityHost is the repro: a post
// authored on host A but posted into a community on host B must be seeded
// from B's resolve_object view of the post, never from A. A is the post's
// self-asserted origin and could set any score it likes.
func TestSeedPostCountsCrossHostAsksTheCommunityHost(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)

	postOrigin, postOriginHits := hostilePostOrigin(t)
	postID := postOrigin.URL + "/post/7"
	communityHost := communityHostResolving(t, postID,
		`{"post":{"post":{"ap_id":"`+postID+`"},"counts":{"post_id":7,"upvotes":42,"downvotes":3,"score":39}}}`)
	communityIRI := communityHost.URL + "/c/news"
	bridgeSubject(t, objects, postID, "3jzfcijpj2z7b")

	seeder, err := NewLemmySeeder(agg,
		ap.NewGuardedHTTPClient(true, 5*time.Second), "tidepool-test/0", nil)
	require.NoError(t, err)
	require.NoError(t, seeder.SeedPostCounts(context.Background(), postID, communityIRI))

	up, down, found := counts(t, database, postID)
	require.True(t, found, "the community host's counts must be seeded")
	assert.Equal(t, 42, up, "the baseline comes from the community host, not the post's host")
	assert.Equal(t, 3, down, "the baseline comes from the community host, not the post's host")
	assert.Equal(t, int64(0), postOriginHits.Load(), "the post's own host must never be contacted")
}

// TestSeedPostCountsCrossHostUnknownToCommunityHostKeepsBaseline: when the
// community host has no local copy of the post (400 couldnt_find_object),
// seeding is a quiet no-op — no fallback to the post's own host, and an
// earlier baseline is left as it was.
func TestSeedPostCountsCrossHostUnknownToCommunityHostKeepsBaseline(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)

	postOrigin, postOriginHits := hostilePostOrigin(t)
	postID := postOrigin.URL + "/post/7"
	communityHost := communityHostResolving(t, postID, "")
	communityIRI := communityHost.URL + "/c/news"
	bridgeSubject(t, objects, postID, "3jzfcijpj2z8b")
	require.NoError(t, agg.SeedAggregates(context.Background(), postID, 5, 1))

	seeder, err := NewLemmySeeder(agg,
		ap.NewGuardedHTTPClient(true, 5*time.Second), "tidepool-test/0", nil)
	require.NoError(t, err)
	assert.NoError(t, seeder.SeedPostCounts(context.Background(), postID, communityIRI),
		"a post the community host does not know is not a seeding failure")

	up, down, found := counts(t, database, postID)
	require.True(t, found)
	assert.Equal(t, 5, up, "a resolve miss must leave the earlier baseline unchanged")
	assert.Equal(t, 1, down, "a resolve miss must leave the earlier baseline unchanged")
	assert.Equal(t, int64(0), postOriginHits.Load(), "the post's own host must never be contacted")
}

// TestSeedPostCountsCrossHostCommunityHostFailureKeepsBaseline: when the
// community host's resolve_object fails outright (HTTP 500 — not a miss),
// seeding reports an error, leaves an earlier baseline as it was, and never
// falls back to the post's own host.
func TestSeedPostCountsCrossHostCommunityHostFailureKeepsBaseline(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)

	postOrigin, postOriginHits := hostilePostOrigin(t)
	postID := postOrigin.URL + "/post/7"
	communityHost := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	t.Cleanup(communityHost.Close)
	communityIRI := communityHost.URL + "/c/news"
	bridgeSubject(t, objects, postID, "3jzfcijpj2z9b")
	require.NoError(t, agg.SeedAggregates(context.Background(), postID, 5, 1))

	seeder, err := NewLemmySeeder(agg,
		ap.NewGuardedHTTPClient(true, 5*time.Second), "tidepool-test/0", nil)
	require.NoError(t, err)
	assert.Error(t, seeder.SeedPostCounts(context.Background(), postID, communityIRI),
		"a failing community host is a seeding failure, not a miss")

	up, down, found := counts(t, database, postID)
	require.True(t, found)
	assert.Equal(t, 5, up, "a community-host failure must leave the earlier baseline unchanged")
	assert.Equal(t, 1, down, "a community-host failure must leave the earlier baseline unchanged")
	assert.Equal(t, int64(0), postOriginHits.Load(), "the post's own host must never be contacted")
}

// TestSeedPostCountsCrossHostNotFoundIsAnError: Lemmy reports a
// resolve_object miss as 400 couldnt_find_object, never 404. A plain-text 404
// means the community host is not answering as Lemmy (a proxy, a renamed
// route, not Lemmy at all), so it is a seeding failure: an error, an earlier
// baseline left as it was, and no fallback to the post's own host.
func TestSeedPostCountsCrossHostNotFoundIsAnError(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)

	postOrigin, postOriginHits := hostilePostOrigin(t)
	postID := postOrigin.URL + "/post/7"
	communityHost := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "404 page not found", http.StatusNotFound)
	}))
	t.Cleanup(communityHost.Close)
	communityIRI := communityHost.URL + "/c/news"
	bridgeSubject(t, objects, postID, "3jzfcijpj2zab")
	require.NoError(t, agg.SeedAggregates(context.Background(), postID, 5, 1))

	seeder, err := NewLemmySeeder(agg,
		ap.NewGuardedHTTPClient(true, 5*time.Second), "tidepool-test/0", nil)
	require.NoError(t, err)
	assert.Error(t, seeder.SeedPostCounts(context.Background(), postID, communityIRI),
		"a 404 from the community host is not Lemmy's miss answer")

	up, down, found := counts(t, database, postID)
	require.True(t, found)
	assert.Equal(t, 5, up, "a 404 must leave the earlier baseline unchanged")
	assert.Equal(t, 1, down, "a 404 must leave the earlier baseline unchanged")
	assert.Equal(t, int64(0), postOriginHits.Load(), "the post's own host must never be contacted")
}

// TestSeedPostCountsUnusableCommunityIRIIsAnError: a Lemmy-shaped post with
// a community IRI that names no host (empty, or a DID passed where the IRI
// belongs) is a caller bug, not a quiet miss: it returns an error, and the
// post's own host is never asked in its place.
func TestSeedPostCountsUnusableCommunityIRIIsAnError(t *testing.T) {
	database := testDB(t)
	agg, _ := testAggregator(t, database)

	tests := []struct {
		name         string
		communityIRI string
	}{
		{"empty", ""},
		{"a DID instead of the IRI", "did:plc:abc123"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			postOrigin, postOriginHits := hostilePostOrigin(t)
			seeder, err := NewLemmySeeder(agg,
				ap.NewGuardedHTTPClient(true, 5*time.Second), "tidepool-test/0", nil)
			require.NoError(t, err)

			assert.Error(t, seeder.SeedPostCounts(context.Background(), postOrigin.URL+"/post/7", tt.communityIRI))
			assert.Equal(t, int64(0), postOriginHits.Load(), "the post's own host must never be contacted")
		})
	}
}

// TestSeedPostCountsCrossHostMalformedResolveKeepsBaseline: the cross-host
// twin of TestSeedPostCountsMalformedResponseKeepsBaseline. A 200
// resolve_object answer without post.counts.{upvotes,downvotes} (an empty
// object, or a post without counts) is an error and leaves an earlier
// baseline as it was.
func TestSeedPostCountsCrossHostMalformedResolveKeepsBaseline(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)

	tests := []struct {
		name string
		body string
		rkey string
	}{
		{"empty object", `{}`, "3jzfcijpj2zac"},
		{"post without counts", `{"post":{"post":{"ap_id":"x"}}}`, "3jzfcijpj2zad"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			postOrigin, postOriginHits := hostilePostOrigin(t)
			postID := postOrigin.URL + "/post/7"
			communityHost := communityHostResolving(t, postID, tt.body)
			bridgeSubject(t, objects, postID, tt.rkey)
			require.NoError(t, agg.SeedAggregates(context.Background(), postID, 5, 1))

			seeder, err := NewLemmySeeder(agg,
				ap.NewGuardedHTTPClient(true, 5*time.Second), "tidepool-test/0", nil)
			require.NoError(t, err)
			assert.Error(t, seeder.SeedPostCounts(context.Background(), postID, communityHost.URL+"/c/news"))

			up, down, found := counts(t, database, postID)
			require.True(t, found)
			assert.Equal(t, 5, up, "a malformed resolve answer must not overwrite the baseline")
			assert.Equal(t, 1, down, "a malformed resolve answer must not overwrite the baseline")
			assert.Equal(t, int64(0), postOriginHits.Load(), "the post's own host must never be contacted")
		})
	}
}
