package votes

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// xrpcResponse mirrors the getVoteAggregates output schema.
type xrpcResponse struct {
	Aggregates []struct {
		URI       string `json:"uri"`
		Upvotes   int    `json:"upvotes"`
		Downvotes int    `json:"downvotes"`
		UpdatedAt string `json:"updatedAt"`
	} `json:"aggregates"`
}

// newXRPCRouter mounts the side channel with test-friendly rate limits.
func newXRPCRouter(t *testing.T, database *sql.DB, perSecond float64, burst int) chi.Router {
	t.Helper()
	xrpc, err := NewXRPC(XRPCOptions{DB: database, RatePerSecond: perSecond, RateBurst: burst})
	require.NoError(t, err)
	router := chi.NewRouter()
	xrpc.Routes(router)
	return router
}

// get performs one getVoteAggregates request with the given raw query.
func get(t *testing.T, router chi.Router, rawQuery string) *httptest.ResponseRecorder {
	t.Helper()
	return getFrom(t, router, rawQuery, "")
}

// getFrom is get with an explicit client RemoteAddr (empty keeps the
// httptest default).
func getFrom(t *testing.T, router chi.Router, rawQuery, remoteAddr string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet,
		"https://bridge.test/xrpc/"+NSIDGetVoteAggregates+"?"+rawQuery, nil)
	if remoteAddr != "" {
		req.RemoteAddr = remoteAddr
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

// testATURI builds a valid at-uri under the test repo for the given rkey.
func testATURI(rkey string) string {
	return "at://" + testDID + "/" + testCollection + "/" + rkey
}

func TestGetVoteAggregates(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	postURI := bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	commentURI := bridgeSubject(t, objects, subjectComment, "3jzfcijpj2z3a")
	ctx := context.Background()

	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 2), voterBob, subjectPost), ""))
	require.NoError(t, agg.ApplyVote(ctx, dislike(activityID(t, 3), voterCarol, subjectComment), ""))

	router := newXRPCRouter(t, database, 1000, 1000)
	unknown := "at://" + testDID + "/" + testCollection + "/3jzzzzzzzzzza"
	rec := get(t, router, "uris="+url.QueryEscape(commentURI)+
		"&uris="+url.QueryEscape(unknown)+"&uris="+url.QueryEscape(postURI))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, "public, max-age=30", rec.Header().Get("Cache-Control"))
	assert.Contains(t, rec.Header().Get("Content-Type"), "application/json")

	var out xrpcResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Aggregates, 2, "the unknown uri must be omitted, not an error")

	// Request order is preserved.
	assert.Equal(t, commentURI, out.Aggregates[0].URI)
	assert.Equal(t, 0, out.Aggregates[0].Upvotes)
	assert.Equal(t, 1, out.Aggregates[0].Downvotes)
	assert.Equal(t, postURI, out.Aggregates[1].URI)
	assert.Equal(t, 2, out.Aggregates[1].Upvotes)
	assert.Equal(t, 0, out.Aggregates[1].Downvotes)
	assert.NotEmpty(t, out.Aggregates[0].UpdatedAt)
}

func TestGetVoteAggregatesCommaSeparated(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	postURI := bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	commentURI := bridgeSubject(t, objects, subjectComment, "3jzfcijpj2z3a")
	ctx := context.Background()
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 1), voterAlice, subjectPost), ""))
	require.NoError(t, agg.ApplyVote(ctx, like(activityID(t, 2), voterBob, subjectComment), ""))

	router := newXRPCRouter(t, database, 1000, 1000)
	rec := get(t, router, "uris="+url.QueryEscape(postURI+","+commentURI))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var out xrpcResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out.Aggregates, 2)
}

func TestGetVoteAggregatesHundredURIBatch(t *testing.T) {
	database := testDB(t)
	agg, objects := testAggregator(t, database)
	postURI := bridgeSubject(t, objects, subjectPost, "3jzfcijpj2z2a")
	require.NoError(t, agg.ApplyVote(context.Background(),
		like(activityID(t, 1), voterAlice, subjectPost), ""))
	router := newXRPCRouter(t, database, 1000, 1000)

	// Exactly 100 uris (99 unknown + 1 real) is accepted.
	uris := make([]string, 0, 100)
	for i := 0; i < 99; i++ {
		uris = append(uris, fmt.Sprintf("at://%s/%s/3jz%09dza", testDID, testCollection, i))
	}
	uris = append(uris, postURI)
	params := make([]string, len(uris))
	for i, uri := range uris {
		params[i] = "uris=" + url.QueryEscape(uri)
	}
	rec := get(t, router, strings.Join(params, "&"))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var out xrpcResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Aggregates, 1)
	assert.Equal(t, postURI, out.Aggregates[0].URI)

	// The 101st tips it over: InvalidRequest.
	params = append(params, "uris="+url.QueryEscape(
		"at://"+testDID+"/"+testCollection+"/3jzoverflowza"))
	rec = get(t, router, strings.Join(params, "&"))
	require.Equal(t, http.StatusBadRequest, rec.Code)
	var xrpcErr map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &xrpcErr))
	assert.Equal(t, "InvalidRequest", xrpcErr["error"])
}

func TestGetVoteAggregatesMissingURIs(t *testing.T) {
	database := testDB(t)
	router := newXRPCRouter(t, database, 1000, 1000)

	for _, query := range []string{"", "uris=", "uris=%2C%2C"} {
		rec := get(t, router, query)
		require.Equal(t, http.StatusBadRequest, rec.Code, "query %q", query)
		var xrpcErr map[string]string
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &xrpcErr))
		assert.Equal(t, "InvalidRequest", xrpcErr["error"])
	}
}

func TestGetVoteAggregatesMalformedURI(t *testing.T) {
	database := testDB(t)
	router := newXRPCRouter(t, database, 1000, 1000)

	for _, uri := range []string{
		"not-a-uri",
		"https://lemmy.world/post/100",
		"at://",
		"at://" + testDID + "/not_an_nsid/3jzfcijpj2z2a",
	} {
		rec := get(t, router, "uris="+url.QueryEscape(uri))
		require.Equal(t, http.StatusBadRequest, rec.Code, "uri %q", uri)
		var xrpcErr map[string]string
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &xrpcErr))
		assert.Equal(t, "InvalidRequest", xrpcErr["error"])
		assert.Contains(t, xrpcErr["message"], "malformed at-uri")
	}

	// One malformed uri poisons the whole batch, even alongside valid ones.
	rec := get(t, router, "uris="+url.QueryEscape(testATURI("3jzfcijpj2z2a"))+
		"&uris="+url.QueryEscape("not-a-uri"))
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestParseURIs(t *testing.T) {
	a, b, c := testATURI("3jzaaaaaaaaaa"), testATURI("3jzbbbbbbbbbb"), testATURI("3jzcccccccccc")

	t.Run("trims whitespace", func(t *testing.T) {
		uris, err := parseURIs([]string{"  " + a + " , " + b + "\t"})
		require.NoError(t, err)
		assert.Equal(t, []string{a, b}, uris)
	})

	t.Run("dedupes preserving order", func(t *testing.T) {
		uris, err := parseURIs([]string{a, b, a, b, a})
		require.NoError(t, err)
		assert.Equal(t, []string{a, b}, uris)
	})

	t.Run("mixes commas and repeated params", func(t *testing.T) {
		uris, err := parseURIs([]string{a + "," + b, c, b})
		require.NoError(t, err)
		assert.Equal(t, []string{a, b, c}, uris)
	})

	t.Run("skips empty values", func(t *testing.T) {
		uris, err := parseURIs([]string{",,", "", " , "})
		require.NoError(t, err)
		assert.Empty(t, uris)
	})

	t.Run("rejects malformed at-uris", func(t *testing.T) {
		_, err := parseURIs([]string{a, "not-a-uri"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "malformed at-uri")
	})

	t.Run("caps the raw count before dedupe", func(t *testing.T) {
		// Exactly 100 raw values pass, even though they collapse to one.
		params := make([]string, maxURIsPerQuery)
		for i := range params {
			params[i] = a
		}
		uris, err := parseURIs(params)
		require.NoError(t, err)
		assert.Equal(t, []string{a}, uris)

		// The 101st raw value is over the cap despite being a duplicate.
		_, err = parseURIs(append(params, a))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "too many uris")
	})
}

func TestGetVoteAggregatesRateLimitPerIP(t *testing.T) {
	database := testDB(t)
	// One-token buckets with negligible refill: exhausting one client's
	// bucket must not touch another client's.
	router := newXRPCRouter(t, database, 0.001, 1)
	query := "uris=" + url.QueryEscape(testATURI("3jzfcijpj2z2a"))

	require.Equal(t, http.StatusOK, getFrom(t, router, query, "192.0.2.10:4000").Code)
	require.Equal(t, http.StatusTooManyRequests, getFrom(t, router, query, "192.0.2.10:4001").Code,
		"same IP, different port: one bucket")
	assert.Equal(t, http.StatusOK, getFrom(t, router, query, "192.0.2.11:4000").Code,
		"a different IP gets its own bucket")
}

func TestGetVoteAggregatesRateLimited(t *testing.T) {
	database := testDB(t)
	// A bucket of 2 with negligible refill: the third request must be
	// refused with RateLimitExceeded.
	router := newXRPCRouter(t, database, 0.001, 2)
	query := "uris=" + url.QueryEscape("at://"+testDID+"/"+testCollection+"/3jzfcijpj2z2a")

	require.Equal(t, http.StatusOK, get(t, router, query).Code)
	require.Equal(t, http.StatusOK, get(t, router, query).Code)
	rec := get(t, router, query)
	require.Equal(t, http.StatusTooManyRequests, rec.Code)
	var xrpcErr map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &xrpcErr))
	assert.Equal(t, "RateLimitExceeded", xrpcErr["error"])
}
