package votes

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
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
	require.NoError(t, seeder.SeedPostCounts(context.Background(), postID))

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
		"https://lemmy.world/comment/555"))
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
			assert.Error(t, seeder.SeedPostCounts(context.Background(), postID))

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
	assert.Error(t, seeder.SeedPostCounts(context.Background(), server.URL+"/post/1"))
}
