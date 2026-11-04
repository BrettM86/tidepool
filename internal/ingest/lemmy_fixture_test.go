package ingest

import (
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The limits of Lemmy 0.19.20, the e2e/lemmy/Dockerfile pin, read from
// crates/db_schema/src/utils.rs. They are literals here on purpose: a fixture
// that imported the production constants would move with them.
const (
	lemmyFixtureMaxPage      = 100
	lemmyFixtureMaxLimit     = 50
	lemmyFixtureDefaultLimit = 10

	lemmyListPath = "/api/v3/community/list"

	// Lemmy's own 400 bodies, captured from the pinned image. Diesel displays a
	// QueryBuilderError as its inner message, with no prefix, and Lemmy uses the
	// page message for a page below 1 and the limit message for a limit below 1.
	lemmyPageLimitBody  = `{"error":"unknown","message":"Page limit is > 100"}`
	lemmyFetchLimitBody = `{"error":"unknown","message":"Fetch limit is > 50"}`

	lemmyPublishedFormat = "2006-01-02T15:04:05.000000Z"
	// lemmySchemeHeader carries the scheme the client asked for across
	// rewriteTransport, which sends everything over plain HTTP.
	lemmySchemeHeader = "X-Lemmy-Fixture-Scheme"

	lemmyPostListPath = "/api/v3/post/list"
	// Lemmy wraps a limit outside 1 to 50 in CouldntGetPosts, and answers a
	// page_cursor naming no post (or not "P" and hex) with the cursor parse error,
	// read before the limit (crates/apub/src/api/list_posts.rs).
	lemmyCouldntGetPostsBody = `{"error":"couldnt_get_posts"}`
	lemmyBadCursorBody       = `{"error":"unknown","message":"Could not parse pagination token"}`
	// lemmyProbeHeader marks a request as a calibration probe: if the fixture
	// refuses it, the refusal does not fail the owning test.
	lemmyProbeHeader = "X-Lemmy-Fixture-Probe"
)

// lemmyFirstPublished is when the community at ordinal 1 was created. Later
// ordinals follow a minute apart, so published order is ordinal order.
var lemmyFirstPublished = time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)

// lemmyPublishedAt is the default published time of the community at ordinal.
func lemmyPublishedAt(ordinal int) time.Time {
	return lemmyFirstPublished.Add(time.Duration(ordinal-1) * time.Minute)
}

// lemmyCommunityName is the default name of the community at ordinal.
func lemmyCommunityName(ordinal int) string {
	return fmt.Sprintf("community-%05d", ordinal)
}

// lemmyActorID is the default actor_id of the community at ordinal.
func lemmyActorID(host string, ordinal int) string {
	return "https://" + host + "/c/" + lemmyCommunityName(ordinal)
}

// lemmyActorIDs lists the default actor_ids of ordinals first to last.
func lemmyActorIDs(host string, first, last int) []string {
	ids := make([]string, 0, max(last-first+1, 0))
	for ordinal := first; ordinal <= last; ordinal++ {
		ids = append(ids, lemmyActorID(host, ordinal))
	}
	return ids
}

// lemmyCommunity is one community a host holds. Any field left zero takes the
// default for its ordinal, the host's count of communities plus one.
type lemmyCommunity struct {
	name      string
	actorID   string
	published time.Time
}

// lemmyPost is one local post a host holds, in a community that is local, not
// hidden, not removed and not deleted: the only posts an anonymous type_=Local
// read returns.
type lemmyPost struct {
	id            int64
	published     time.Time
	featuredLocal bool
	// community is the actor_id of the post's community.
	community string
	// nsfw marks the post's community NSFW: it is listed only with show_nsfw=true.
	nsfw bool
}

// lemmyPostKey names the post list request of one host that carries cursor as
// its page_cursor. The empty cursor is a request with no page_cursor.
type lemmyPostKey struct {
	host   string
	cursor string
}

// lemmyListRequest is one request the fixture answered.
type lemmyListRequest struct {
	host      string
	scheme    string
	query     url.Values
	userAgent string
	status    int
}

// lemmyPageKey names a page of one host in one sort.
type lemmyPageKey struct {
	host string
	sort string
	page int
}

// lemmyPageInjection replaces what one page answers. Exactly one field is set.
type lemmyPageInjection struct {
	// status answers with that status and an error body.
	status int
	// body answers 200 with these bytes.
	body string
	// size answers 200 with the real page, padded to exactly this many bytes.
	size int
}

// lemmyFixture is a strict stand-in for Lemmy's community list and post list
// endpoints. One server plays every host through rewriteTransport and tells them
// apart by the request's Host. A request Lemmy would refuse gets Lemmy's 400, and
// fails the owning test at cleanup unless the test declared it with
// expectRejected or marked it with lemmyProbeHeader: the lister reads a 400 as a
// failed page, so it could otherwise walk straight past a limit it breaks and
// still pass. Every listing, post walk and sweep test uses it.
//
// The post list models only type_=Local&sort=New, and refuses the deprecated
// page parameter, which the walk must never send.
type lemmyFixture struct {
	t      testing.TB
	server *httptest.Server

	mutex              sync.Mutex
	hosts              map[string][]lemmyCommunity
	injections         map[lemmyPageKey]lemmyPageInjection
	requests           []lemmyListRequest
	posts              map[string][]lemmyPost
	postInjections     map[lemmyPostKey]lemmyPageInjection
	postRequests       []lemmyListRequest
	rejected           []string
	expectedRejections int
}

func newLemmyFixture(t testing.TB) *lemmyFixture {
	t.Helper()
	fixture := &lemmyFixture{
		t:              t,
		hosts:          map[string][]lemmyCommunity{},
		injections:     map[lemmyPageKey]lemmyPageInjection{},
		posts:          map[string][]lemmyPost{},
		postInjections: map[lemmyPostKey]lemmyPageInjection{},
	}
	fixture.server = httptest.NewServer(fixture)
	t.Cleanup(func() {
		fixture.server.Close()
		fixture.mutex.Lock()
		defer fixture.mutex.Unlock()
		if len(fixture.rejected) != fixture.expectedRejections {
			t.Errorf("the Lemmy fixture rejected %d requests and the test declared %d: %v",
				len(fixture.rejected), fixture.expectedRejections, fixture.rejected)
		}
	})
	return fixture
}

// client is an HTTP client whose every request reaches this fixture.
func (f *lemmyFixture) client() *http.Client {
	return &http.Client{Transport: lemmyFixtureTransport{target: f.server.Listener.Addr().String()}}
}

// lemmyFixtureTransport is rewriteTransport, keeping the scheme for the record.
type lemmyFixtureTransport struct{ target string }

func (rt lemmyFixtureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.Header.Set(lemmySchemeHeader, req.URL.Scheme)
	return rewriteTransport(rt).RoundTrip(clone)
}

// add gives host one community, at the next ordinal.
func (f *lemmyFixture) add(host string, community lemmyCommunity) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	ordinal := len(f.hosts[host]) + 1
	if community.name == "" {
		community.name = lemmyCommunityName(ordinal)
	}
	if community.actorID == "" {
		community.actorID = "https://" + host + "/c/" + community.name
	}
	if community.published.IsZero() {
		community.published = lemmyPublishedAt(ordinal)
	}
	f.hosts[host] = append(f.hosts[host], community)
}

// addCommunities gives host count default communities. A count of 0 makes an
// instance that holds nothing: a request for a host never added is rejected.
func (f *lemmyFixture) addCommunities(host string, count int) {
	f.mutex.Lock()
	if _, known := f.hosts[host]; !known {
		f.hosts[host] = nil
	}
	f.mutex.Unlock()
	for range count {
		f.add(host, lemmyCommunity{})
	}
}

// addPosts gives host these posts, making the host known if it was not. Post ids
// are unique per host, as Lemmy's are per instance.
func (f *lemmyFixture) addPosts(host string, posts ...lemmyPost) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	if _, known := f.hosts[host]; !known {
		f.hosts[host] = nil
	}
	for _, post := range posts {
		if slices.ContainsFunc(f.posts[host], func(held lemmyPost) bool { return held.id == post.id }) {
			f.t.Errorf("the Lemmy fixture already holds post %d on %s", post.id, host)
			continue
		}
		f.posts[host] = append(f.posts[host], post)
	}
}

// failPosts makes the post list request of host with this page_cursor ("" for
// none) answer status until clearInjections.
func (f *lemmyFixture) failPosts(host, cursor string, status int) {
	f.injectPosts(lemmyPostKey{host, cursor}, lemmyPageInjection{status: status})
}

// servePostsBody makes the post list request of host with this page_cursor
// answer 200 with body until clearInjections.
func (f *lemmyFixture) servePostsBody(host, cursor, body string) {
	f.injectPosts(lemmyPostKey{host, cursor}, lemmyPageInjection{body: body})
}

// padPosts makes the post list request of host with this page_cursor answer its
// real page as valid JSON of exactly size bytes, padded as padPage pads.
func (f *lemmyFixture) padPosts(host, cursor string, size int) {
	f.injectPosts(lemmyPostKey{host, cursor}, lemmyPageInjection{size: size})
}

func (f *lemmyFixture) injectPosts(key lemmyPostKey, injection lemmyPageInjection) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.postInjections[key] = injection
}

// failPage makes the page answer status until clearInjections.
func (f *lemmyFixture) failPage(host, sort string, page, status int) {
	f.inject(lemmyPageKey{host, sort, page}, lemmyPageInjection{status: status})
}

// servePageBody makes the page answer 200 with body until clearInjections.
func (f *lemmyFixture) servePageBody(host, sort string, page int, body string) {
	f.inject(lemmyPageKey{host, sort, page}, lemmyPageInjection{body: body})
}

// padPage makes the page answer its real results as valid JSON of exactly size
// bytes. The padding sits before the results, so a reader that stops at a byte
// limit sees truncated JSON rather than a whole document.
func (f *lemmyFixture) padPage(host, sort string, page, size int) {
	f.inject(lemmyPageKey{host, sort, page}, lemmyPageInjection{size: size})
}

func (f *lemmyFixture) inject(key lemmyPageKey, injection lemmyPageInjection) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.injections[key] = injection
}

// clearInjections puts every page, of both endpoints, back to its real answer.
func (f *lemmyFixture) clearInjections() {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.injections = map[lemmyPageKey]lemmyPageInjection{}
	f.postInjections = map[lemmyPostKey]lemmyPageInjection{}
}

// expectRejected declares how many rejected requests this test sends on purpose.
func (f *lemmyFixture) expectRejected(count int) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.expectedRejections = count
}

// recorded is every request so far other than post list requests, in arrival
// order.
func (f *lemmyFixture) recorded() []lemmyListRequest {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return slices.Clone(f.requests)
}

// recordedPosts is every post list request so far, in arrival order.
func (f *lemmyFixture) recordedPosts() []lemmyListRequest {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return slices.Clone(f.postRequests)
}

// postCursorsRequested lists the page_cursor of each post list request of host,
// in arrival order, "" for a request with none.
func (f *lemmyFixture) postCursorsRequested(host string) []string {
	var cursors []string
	for _, request := range f.recordedPosts() {
		if request.host == host {
			cursors = append(cursors, request.query.Get("page_cursor"))
		}
	}
	return cursors
}

// pagesRequested lists the pages asked of host in sort, in arrival order.
func (f *lemmyFixture) pagesRequested(host, sort string) []int {
	var pages []int
	for _, request := range f.recorded() {
		if request.host != host || request.query.Get("sort") != sort {
			continue
		}
		page, err := strconv.Atoi(request.query.Get("page"))
		require.NoError(f.t, err)
		pages = append(pages, page)
	}
	return pages
}

func (f *lemmyFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	status, body := f.record(r)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}

// record answers the request and keeps it.
func (f *lemmyFixture) record(r *http.Request) (int, string) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	isPostList := r.URL.Path == lemmyPostListPath
	var status int
	var body, refusal string
	if isPostList {
		status, body, refusal = f.answerPosts(r)
	} else {
		status, body, refusal = f.answer(r)
	}
	if refusal != "" && r.Header.Get(lemmyProbeHeader) == "" {
		f.rejected = append(f.rejected, r.Host+r.URL.RequestURI()+": "+refusal)
	}
	request := lemmyListRequest{
		host: r.Host, scheme: r.Header.Get(lemmySchemeHeader), query: r.URL.Query(),
		userAgent: r.UserAgent(), status: status,
	}
	if isPostList {
		f.postRequests = append(f.postRequests, request)
	} else {
		f.requests = append(f.requests, request)
	}
	return status, body
}

// answer gives the response, and refusal when Lemmy, or this model of it, would
// refuse the request.
func (f *lemmyFixture) answer(r *http.Request) (status int, body, refusal string) {
	if r.Method != http.MethodGet || r.URL.Path != lemmyListPath {
		return http.StatusNotFound, `{"error":"not_found"}`, "not the community list endpoint"
	}
	query := r.URL.Query()
	page, pageGiven, pageErr := lemmyQueryInt(query, "page")
	limit, limitGiven, limitErr := lemmyQueryInt(query, "limit")
	if pageErr != nil || limitErr != nil {
		return http.StatusBadRequest, `{"error":"invalid_query"}`, "page or limit is not an integer"
	}
	if !pageGiven {
		page = 1
	}
	if !limitGiven {
		limit = lemmyFixtureDefaultLimit
	}
	if page < 1 || page > lemmyFixtureMaxPage {
		return http.StatusBadRequest, lemmyPageLimitBody, "page out of range"
	}
	if limit < 1 || limit > lemmyFixtureMaxLimit {
		return http.StatusBadRequest, lemmyFetchLimitBody, "limit out of range"
	}
	sort := query.Get("sort")
	if sort != "Old" && sort != "New" {
		return http.StatusBadRequest, `{"error":"invalid_query"}`, "this fixture models only sort=Old and sort=New"
	}
	communities, known := f.hosts[r.Host]
	if !known {
		return http.StatusNotFound, `{"error":"not_found"}`, "unknown host"
	}

	injection, injected := f.injections[lemmyPageKey{r.Host, sort, page}]
	if injected && injection.status != 0 {
		return injection.status, `{"error":"unknown","message":"injected failure"}`, ""
	}
	if injected && injection.body != "" {
		return http.StatusOK, injection.body, ""
	}
	document := f.page(communities, sort, page, limit)
	if injected {
		document = f.pad(document, injection.size)
	}
	return http.StatusOK, document, ""
}

func lemmyQueryInt(query url.Values, key string) (value int, given bool, err error) {
	if !query.Has(key) {
		return 0, false, nil
	}
	value, err = strconv.Atoi(query.Get(key))
	return value, true, err
}

// lemmyCommunityViewDocument is a CommunityView, with the fields a list reader
// can see.
type lemmyCommunityViewDocument struct {
	Community  lemmyCommunityDocument `json:"community"`
	Subscribed string                 `json:"subscribed"`
	Blocked    bool                   `json:"blocked"`
}

type lemmyCommunityDocument struct {
	ID        int    `json:"id"`
	Name      string `json:"name"`
	ActorID   string `json:"actor_id"`
	Published string `json:"published"`
	NSFW      bool   `json:"nsfw"`
}

// page encodes one page of communities in sort order.
func (f *lemmyFixture) page(communities []lemmyCommunity, sort string, page, limit int) string {
	ordered := make([]lemmyCommunityViewDocument, 0, len(communities))
	for index, community := range communities {
		ordered = append(ordered, lemmyCommunityViewDocument{
			Community: lemmyCommunityDocument{
				ID: index + 1, Name: community.name, ActorID: community.actorID,
				Published: community.published.UTC().Format(lemmyPublishedFormat),
			},
			Subscribed: "NotSubscribed",
		})
	}
	slices.SortStableFunc(ordered, func(a, b lemmyCommunityViewDocument) int {
		order := strings.Compare(a.Community.Published, b.Community.Published)
		if sort == "New" {
			return -order
		}
		return order
	})
	first := min((page-1)*limit, len(ordered))
	last := min(first+limit, len(ordered))
	encoded, err := json.Marshal(map[string]any{"communities": ordered[first:last]})
	if err != nil {
		f.t.Errorf("encode a Lemmy page: %v", err) // On the server goroutine: no FailNow.
	}
	return string(encoded)
}

// pad grows a JSON object to exactly size bytes with a leading unknown member.
func (f *lemmyFixture) pad(document string, size int) string {
	const prefix = `{"padding":"`
	const suffix = `",`
	padding := size - len(prefix) - len(suffix) - (len(document) - 1)
	if padding < 0 {
		f.t.Errorf("the page does not fit in %d bytes", size) // On the server goroutine: no FailNow.
		return document
	}
	return prefix + strings.Repeat("x", padding) + suffix + document[1:]
}

// answerPosts gives the post list response, and refusal when Lemmy, or this
// model of it, would refuse the request.
func (f *lemmyFixture) answerPosts(r *http.Request) (status int, body, refusal string) {
	if r.Method != http.MethodGet {
		return http.StatusNotFound, `{"error":"not_found"}`, "not a GET of the post list"
	}
	if _, known := f.hosts[r.Host]; !known {
		return http.StatusNotFound, `{"error":"not_found"}`, "unknown host"
	}
	query := r.URL.Query()
	if query.Has("page") {
		return http.StatusBadRequest, `{"error":"invalid_query"}`, "the post walk must not send the deprecated page parameter"
	}
	if query.Get("type_") != "Local" || query.Get("sort") != "New" {
		return http.StatusBadRequest, `{"error":"invalid_query"}`, "this fixture models only type_=Local and sort=New"
	}
	showNSFW := query.Get("show_nsfw")
	if query.Has("show_nsfw") && showNSFW != "true" && showNSFW != "false" {
		return http.StatusBadRequest, `{"error":"invalid_query"}`, "show_nsfw is not a boolean"
	}
	posts := f.posts[r.Host]
	var after *lemmyPost
	if query.Has("page_cursor") {
		post, found := lemmyCursorPost(posts, query.Get("page_cursor"))
		if !found {
			return http.StatusBadRequest, lemmyBadCursorBody, "page_cursor names no post"
		}
		after = &post
	}
	limit, limitGiven, limitErr := lemmyQueryInt(query, "limit")
	if limitErr != nil {
		return http.StatusBadRequest, `{"error":"invalid_query"}`, "limit is not an integer"
	}
	if !limitGiven {
		limit = lemmyFixtureDefaultLimit
	}
	if limit < 1 || limit > lemmyFixtureMaxLimit {
		return http.StatusBadRequest, lemmyCouldntGetPostsBody, "limit out of range"
	}

	injection, injected := f.postInjections[lemmyPostKey{r.Host, query.Get("page_cursor")}]
	if injected && injection.status != 0 {
		return injection.status, `{"error":"unknown","message":"injected failure"}`, ""
	}
	if injected && injection.body != "" {
		return http.StatusOK, injection.body, ""
	}
	document := f.postPage(r.Host, posts, after, showNSFW == "true", limit)
	if injected {
		document = f.pad(document, injection.size)
	}
	return http.StatusOK, document, ""
}

// lemmyCursorPost is the post a page_cursor names. Lemmy drops the first
// character and reads the rest as a hex post id (PaginationCursor::read).
func lemmyCursorPost(posts []lemmyPost, cursor string) (lemmyPost, bool) {
	if len(cursor) < 2 {
		return lemmyPost{}, false
	}
	id, err := strconv.ParseInt(cursor[1:], 16, 32)
	if err != nil {
		return lemmyPost{}, false
	}
	index := slices.IndexFunc(posts, func(post lemmyPost) bool { return post.id == id })
	if index < 0 {
		return lemmyPost{}, false
	}
	return posts[index], true
}

// lemmyPostOrder is Lemmy's sort=New order with no community_id
// (crates/db_views/src/post_view.rs): featured_local first, then published
// descending, then post id descending.
func lemmyPostOrder(a, b lemmyPost) int {
	if a.featuredLocal != b.featuredLocal {
		if a.featuredLocal {
			return -1
		}
		return 1
	}
	if order := b.published.Compare(a.published); order != 0 {
		return order
	}
	return cmp.Compare(b.id, a.id)
}

// lemmyPostViewDocument is a PostView, with the fields a post list reader can
// see that matter here.
type lemmyPostViewDocument struct {
	Post      lemmyPostDocument          `json:"post"`
	Community lemmyPostCommunityDocument `json:"community"`
	Counts    lemmyPostCountsDocument    `json:"counts"`
}

type lemmyPostDocument struct {
	ID                int64  `json:"id"`
	Name              string `json:"name"`
	APID              string `json:"ap_id"`
	Published         string `json:"published"`
	Local             bool   `json:"local"`
	NSFW              bool   `json:"nsfw"`
	Removed           bool   `json:"removed"`
	Deleted           bool   `json:"deleted"`
	FeaturedLocal     bool   `json:"featured_local"`
	FeaturedCommunity bool   `json:"featured_community"`
}

type lemmyPostCommunityDocument struct {
	ID        int    `json:"id"`
	Name      string `json:"name"`
	ActorID   string `json:"actor_id"`
	Published string `json:"published"`
	Local     bool   `json:"local"`
	Hidden    bool   `json:"hidden"`
	Removed   bool   `json:"removed"`
	Deleted   bool   `json:"deleted"`
	NSFW      bool   `json:"nsfw"`
}

type lemmyPostCountsDocument struct {
	PostID    int64  `json:"post_id"`
	Published string `json:"published"`
}

// lemmyPostsDocument is GetPostsResponse. Lemmy skips a None next_page, so an
// empty page has no next_page member at all.
type lemmyPostsDocument struct {
	Posts    []lemmyPostViewDocument `json:"posts"`
	NextPage string                  `json:"next_page,omitempty"`
}

// postPage encodes the page of posts strictly after the cursor post, in Lemmy's
// order. The cursor is a key, not an offset, so posts added since it was issued
// shift nothing.
func (f *lemmyFixture) postPage(host string, posts []lemmyPost, after *lemmyPost, showNSFW bool, limit int) string {
	ordered := slices.Clone(posts)
	slices.SortFunc(ordered, lemmyPostOrder)
	communityIDs := map[string]int{}
	for _, post := range posts {
		if _, seen := communityIDs[post.community]; !seen {
			communityIDs[post.community] = len(communityIDs) + 1
		}
	}
	document := lemmyPostsDocument{Posts: []lemmyPostViewDocument{}}
	for _, post := range ordered {
		if len(document.Posts) == limit {
			break
		}
		if after != nil && lemmyPostOrder(post, *after) <= 0 {
			continue
		}
		if post.nsfw && !showNSFW {
			continue
		}
		published := post.published.UTC().Format(lemmyPublishedFormat)
		document.Posts = append(document.Posts, lemmyPostViewDocument{
			Post: lemmyPostDocument{
				ID: post.id, Name: fmt.Sprintf("post %d", post.id),
				APID:      fmt.Sprintf("https://%s/post/%d", host, post.id),
				Published: published, Local: true, NSFW: post.nsfw, FeaturedLocal: post.featuredLocal,
			},
			Community: lemmyPostCommunityDocument{
				ID: communityIDs[post.community], Name: post.community[strings.LastIndexByte(post.community, '/')+1:],
				ActorID: post.community, Published: lemmyFirstPublished.Format(lemmyPublishedFormat),
				Local: true, NSFW: post.nsfw,
			},
			Counts: lemmyPostCountsDocument{PostID: post.id, Published: published},
		})
	}
	if len(document.Posts) > 0 {
		document.NextPage = fmt.Sprintf("P%x", document.Posts[len(document.Posts)-1].Post.ID)
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		f.t.Errorf("encode a Lemmy post page: %v", err) // On the server goroutine: no FailNow.
	}
	return string(encoded)
}

const lemmyFixtureHost = "lemmy.world"

// lemmyGet sends one raw request to host through the fixture's client.
func lemmyGet(t *testing.T, fixture *lemmyFixture, host, rawQuery string) (status int, body string) {
	t.Helper()
	response, err := fixture.client().Get("http://" + host + lemmyListPath + "?" + rawQuery)
	require.NoError(t, err)
	defer func() { _ = response.Body.Close() }()
	raw, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	return response.StatusCode, string(raw)
}

// lemmyListedNames decodes a 200 body to its community names, in order.
func lemmyListedNames(t *testing.T, body string) []string {
	t.Helper()
	var document struct {
		Communities []lemmyCommunityViewDocument `json:"communities"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &document))
	names := make([]string, 0, len(document.Communities))
	for _, view := range document.Communities {
		names = append(names, view.Community.Name)
	}
	return names
}

func TestLemmyFixture_RefusesWhatLemmyRefuses(t *testing.T) {
	for _, tc := range []struct {
		name     string
		query    string
		wantBody string
	}{
		{"page 101", "sort=Old&page=101", lemmyPageLimitBody},
		{"page 0", "sort=Old&page=0", lemmyPageLimitBody},
		{"limit 51", "sort=Old&limit=51", lemmyFetchLimitBody},
		{"limit 0", "sort=Old&limit=0", lemmyFetchLimitBody},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newLemmyFixture(t)
			fixture.addCommunities(lemmyFixtureHost, 1)
			fixture.expectRejected(1)

			status, body := lemmyGet(t, fixture, lemmyFixtureHost, tc.query)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tc.wantBody, body)
		})
	}
}

func TestLemmyFixture_ServesTheLimitsThemselves(t *testing.T) {
	fixture := newLemmyFixture(t)
	fixture.addCommunities(lemmyFixtureHost, 1)

	status, body := lemmyGet(t, fixture, lemmyFixtureHost, "sort=Old&page=100&limit=50")
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, `{"communities":[]}`, body)
}

func TestLemmyFixture_PagesAndOrders(t *testing.T) {
	fixture := newLemmyFixture(t)
	fixture.addCommunities(lemmyFixtureHost, 12)
	fixture.add(lemmyFixtureHost, lemmyCommunity{
		name: "backdated", published: lemmyFirstPublished.Add(-time.Hour),
	})

	for _, tc := range []struct {
		name  string
		query string
		want  []string
	}{
		{
			"no limit gives 10, oldest first", "sort=Old",
			[]string{
				"backdated", "community-00001", "community-00002", "community-00003", "community-00004",
				"community-00005", "community-00006", "community-00007", "community-00008", "community-00009",
			},
		},
		{"second page of Old", "sort=Old&page=2&limit=5", []string{
			"community-00005", "community-00006", "community-00007", "community-00008", "community-00009",
		}},
		{"New is newest first", "sort=New&limit=3", []string{"community-00012", "community-00011", "community-00010"}},
		{"last page of New is short", "sort=New&page=3&limit=6", []string{"backdated"}},
		{"past the end is empty", "sort=Old&page=4&limit=5", []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := lemmyGet(t, fixture, lemmyFixtureHost, tc.query)
			require.Equal(t, http.StatusOK, status, body)
			assert.Equal(t, tc.want, lemmyListedNames(t, body))
		})
	}
}

func TestLemmyFixture_ServesTheViewShape(t *testing.T) {
	fixture := newLemmyFixture(t)
	fixture.addCommunities(lemmyFixtureHost, 1)

	_, body := lemmyGet(t, fixture, lemmyFixtureHost, "sort=Old")
	assert.JSONEq(t, `{"communities":[{
		"community":{
			"id":1,"name":"community-00001","actor_id":"https://lemmy.world/c/community-00001",
			"published":"2020-01-01T00:00:00.000000Z","nsfw":false
		},
		"subscribed":"NotSubscribed","blocked":false
	}]}`, body)
}

func TestLemmyFixture_TellsHostsApartAndGrows(t *testing.T) {
	fixture := newLemmyFixture(t)
	fixture.addCommunities("lemmy.world", 1)
	fixture.addCommunities("lemmy.ml", 2)

	_, body := lemmyGet(t, fixture, "lemmy.world", "sort=Old")
	assert.Equal(t, []string{"community-00001"}, lemmyListedNames(t, body))
	_, body = lemmyGet(t, fixture, "lemmy.ml", "sort=Old")
	assert.Equal(t, []string{"community-00001", "community-00002"}, lemmyListedNames(t, body))

	fixture.add("lemmy.world", lemmyCommunity{name: "created-later"})
	_, body = lemmyGet(t, fixture, "lemmy.world", "sort=New&limit=1")
	assert.Equal(t, []string{"created-later"}, lemmyListedNames(t, body))
}

func TestLemmyFixture_InjectsPageAnswers(t *testing.T) {
	fixture := newLemmyFixture(t)
	fixture.addCommunities(lemmyFixtureHost, 2)
	fixture.failPage(lemmyFixtureHost, "Old", 1, http.StatusInternalServerError)
	fixture.servePageBody(lemmyFixtureHost, "Old", 2, "not json")
	fixture.padPage(lemmyFixtureHost, "New", 1, 1024)

	status, _ := lemmyGet(t, fixture, lemmyFixtureHost, "sort=Old")
	assert.Equal(t, http.StatusInternalServerError, status)
	status, body := lemmyGet(t, fixture, lemmyFixtureHost, "sort=Old&page=2")
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "not json", body)
	status, body = lemmyGet(t, fixture, lemmyFixtureHost, "sort=New")
	assert.Equal(t, http.StatusOK, status)
	assert.Len(t, body, 1024)
	assert.Equal(t, []string{"community-00002", "community-00001"}, lemmyListedNames(t, body),
		"a padded page is still the real page")

	fixture.clearInjections()
	status, body = lemmyGet(t, fixture, lemmyFixtureHost, "sort=Old")
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, []string{"community-00001", "community-00002"}, lemmyListedNames(t, body))
}

func TestLemmyFixture_RecordsRequests(t *testing.T) {
	fixture := newLemmyFixture(t)
	fixture.addCommunities(lemmyFixtureHost, 1)

	request, err := http.NewRequest(http.MethodGet, "https://lemmy.world"+lemmyListPath+"?sort=New&page=3", nil)
	require.NoError(t, err)
	request.Header.Set("User-Agent", "fixture-test/1.0")
	response, err := fixture.client().Do(request)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())

	assert.Equal(t, []lemmyListRequest{{
		host: "lemmy.world", scheme: "https", query: url.Values{"sort": {"New"}, "page": {"3"}},
		userAgent: "fixture-test/1.0", status: http.StatusOK,
	}}, fixture.recorded())
	assert.Equal(t, []int{3}, fixture.pagesRequested("lemmy.world", "New"))
	assert.Empty(t, fixture.pagesRequested("lemmy.world", "Old"))
}

// recordingTB stands in for the test that owns a fixture, so the fixture's own
// failures can be observed instead of failing this suite.
type recordingTB struct {
	testing.TB
	cleanups []func()
	failures []string
}

func (r *recordingTB) Helper() {}

func (r *recordingTB) Cleanup(cleanup func()) { r.cleanups = append(r.cleanups, cleanup) }

func (r *recordingTB) Errorf(format string, args ...any) {
	r.failures = append(r.failures, fmt.Sprintf(format, args...))
}

func (r *recordingTB) runCleanups() {
	for _, cleanup := range slices.Backward(r.cleanups) {
		cleanup()
	}
}

func TestLemmyFixture_RejectedRequestFailsItsTest(t *testing.T) {
	for _, tc := range []struct {
		name     string
		queries  []string
		declared int
		wantFail bool
	}{
		{"undeclared page 101", []string{"sort=Old&page=101"}, 0, true},
		{"undeclared limit 51", []string{"sort=Old&limit=51"}, 0, true},
		{"undeclared missing sort", []string{"page=1"}, 0, true},
		{"declared rejections", []string{"sort=Old&page=101", "sort=Old&limit=51"}, 2, false},
		{"more rejections than declared", []string{"sort=Old&page=101", "sort=Old&limit=51"}, 1, true},
		{"a declared rejection that never came", []string{"sort=Old"}, 1, true},
		{"nothing rejected", []string{"sort=Old&page=100&limit=50"}, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := &recordingTB{TB: t}
			fixture := newLemmyFixture(owner)
			fixture.addCommunities(lemmyFixtureHost, 1)
			fixture.expectRejected(tc.declared)
			for _, query := range tc.queries {
				lemmyGet(t, fixture, lemmyFixtureHost, query)
			}
			owner.runCleanups()
			assert.Equal(t, tc.wantFail, len(owner.failures) > 0, "failures: %v", owner.failures)
		})
	}
}

func TestLemmyFixture_RequestForAnUnknownHostFailsItsTest(t *testing.T) {
	owner := &recordingTB{TB: t}
	fixture := newLemmyFixture(owner)

	status, _ := lemmyGet(t, fixture, "nothing.example", "sort=Old")
	assert.Equal(t, http.StatusNotFound, status)
	owner.runCleanups()
	assert.NotEmpty(t, owner.failures)
}

// lemmyPostGet sends one raw post list request to host through the fixture's
// client, marked as a calibration probe when probe is set.
func lemmyPostGet(t *testing.T, fixture *lemmyFixture, host, rawQuery string, probe bool) (status int, body string) {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, "http://"+host+lemmyPostListPath+"?"+rawQuery, nil)
	require.NoError(t, err)
	if probe {
		request.Header.Set(lemmyProbeHeader, "calibration")
	}
	response, err := fixture.client().Do(request)
	require.NoError(t, err)
	defer func() { _ = response.Body.Close() }()
	raw, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	return response.StatusCode, string(raw)
}

// lemmyPostPage decodes a 200 post list body to its post ids, in order, and its
// next_page, with hasNext false when the member is absent.
func lemmyPostPage(t *testing.T, body string) (ids []int64, nextPage string, hasNext bool) {
	t.Helper()
	var document struct {
		Posts []struct {
			Post struct {
				ID int64 `json:"id"`
			} `json:"post"`
		} `json:"posts"`
		NextPage *string `json:"next_page"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &document))
	ids = []int64{}
	for _, view := range document.Posts {
		ids = append(ids, view.Post.ID)
	}
	if document.NextPage == nil {
		return ids, "", false
	}
	return ids, *document.NextPage, true
}

// lemmyFixturePostMinute is a published time on the fixture tests' post clock.
func lemmyFixturePostMinute(minute int) time.Time {
	return time.Date(2026, time.October, 1, 0, minute, 0, 0, time.UTC)
}

func TestLemmyFixture_PostListProbesGetLemmysRefusals(t *testing.T) {
	for _, tc := range []struct {
		name     string
		query    string
		wantBody string
	}{
		{"limit 51", "type_=Local&sort=New&limit=51", lemmyCouldntGetPostsBody},
		{"limit 0", "type_=Local&sort=New&limit=0", lemmyCouldntGetPostsBody},
		{"cursor naming no post", "type_=Local&sort=New&limit=50&page_cursor=P3e7", lemmyBadCursorBody},
		{"cursor that is not hex", "type_=Local&sort=New&limit=50&page_cursor=Pzz", lemmyBadCursorBody},
		{"cursor read before the limit", "type_=Local&sort=New&limit=51&page_cursor=P3e7", lemmyBadCursorBody},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := &recordingTB{TB: t}
			fixture := newLemmyFixture(owner)
			fixture.addPosts(lemmyFixtureHost, lemmyPost{
				id: 1, published: lemmyFixturePostMinute(1), community: "https://lemmy.world/c/a",
			})

			status, body := lemmyPostGet(t, fixture, lemmyFixtureHost, tc.query, true)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, tc.wantBody, body)
			owner.runCleanups()
			assert.Empty(t, owner.failures, "a refused probe does not fail its test")
		})
	}
}

func TestLemmyFixture_PostListRefusalFailsItsTest(t *testing.T) {
	for _, tc := range []struct {
		name     string
		host     string
		query    string
		probe    bool
		wantFail bool
	}{
		{"limit 51", lemmyFixtureHost, "type_=Local&sort=New&limit=51", false, true},
		{"cursor naming no post", lemmyFixtureHost, "type_=Local&sort=New&page_cursor=P3e7", false, true},
		{"the deprecated page", lemmyFixtureHost, "type_=Local&sort=New&limit=50&page=1", false, true},
		{"the deprecated page marked as a probe", lemmyFixtureHost, "type_=Local&sort=New&limit=50&page=1", true, false},
		{"a sort the fixture does not model", lemmyFixtureHost, "type_=Local&sort=Hot&limit=50", false, true},
		{"a listing type the fixture does not model", lemmyFixtureHost, "type_=All&sort=New&limit=50", false, true},
		{"an unknown host", "nothing.example", "type_=Local&sort=New&limit=50", false, true},
		{"nothing refused", lemmyFixtureHost, "type_=Local&sort=New&show_nsfw=true&limit=50&page_cursor=P1", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := &recordingTB{TB: t}
			fixture := newLemmyFixture(owner)
			fixture.addPosts(lemmyFixtureHost, lemmyPost{
				id: 1, published: lemmyFixturePostMinute(1), community: "https://lemmy.world/c/a",
			})

			lemmyPostGet(t, fixture, tc.host, tc.query, tc.probe)
			owner.runCleanups()
			assert.Equal(t, tc.wantFail, len(owner.failures) > 0, "failures: %v", owner.failures)
		})
	}
}

func TestLemmyFixture_PostListPagesAndOrders(t *testing.T) {
	fixture := newLemmyFixture(t)
	fixture.addPosts(lemmyFixtureHost,
		lemmyPost{id: 1, published: lemmyFixturePostMinute(1), community: "https://lemmy.world/c/a"},
		lemmyPost{id: 2, published: lemmyFixturePostMinute(2), community: "https://lemmy.world/c/b"},
		lemmyPost{id: 3, published: lemmyFixturePostMinute(3), featuredLocal: true, community: "https://lemmy.world/c/c"},
		lemmyPost{id: 4, published: lemmyFixturePostMinute(2), community: "https://lemmy.world/c/d"},
		lemmyPost{id: 5, published: lemmyFixturePostMinute(5), community: "https://lemmy.world/c/e"},
		lemmyPost{id: 6, published: lemmyFixturePostMinute(6), community: "https://lemmy.world/c/f", nsfw: true},
	)

	for _, step := range []struct {
		name     string
		query    string
		wantIDs  []int64
		wantNext string
	}{
		{"no limit gives 10, featured first, then published and id descending", "type_=Local&sort=New&show_nsfw=true",
			[]int64{3, 6, 5, 4, 2, 1}, "P1"},
		{"NSFW is hidden without show_nsfw", "type_=Local&sort=New&limit=2", []int64{3, 5}, "P5"},
		{"first page", "type_=Local&sort=New&show_nsfw=true&limit=2", []int64{3, 6}, "P6"},
		{"the cursor continues after the post it names", "type_=Local&sort=New&show_nsfw=true&limit=2&page_cursor=P6",
			[]int64{5, 4}, "P4"},
	} {
		status, body := lemmyPostGet(t, fixture, lemmyFixtureHost, step.query, false)
		require.Equal(t, http.StatusOK, status, body)
		ids, nextPage, hasNext := lemmyPostPage(t, body)
		assert.Equal(t, step.wantIDs, ids, step.name)
		assert.True(t, hasNext, step.name)
		assert.Equal(t, step.wantNext, nextPage, step.name)
	}

	// A post created after the first page shifts nothing: it sorts before the
	// cursor post.
	fixture.addPosts(lemmyFixtureHost,
		lemmyPost{id: 7, published: lemmyFixturePostMinute(7), community: "https://lemmy.world/c/g"})
	status, body := lemmyPostGet(t, fixture, lemmyFixtureHost, "type_=Local&sort=New&show_nsfw=true&limit=2&page_cursor=P4", false)
	require.Equal(t, http.StatusOK, status, body)
	ids, nextPage, hasNext := lemmyPostPage(t, body)
	assert.Equal(t, []int64{2, 1}, ids)
	assert.True(t, hasNext)
	assert.Equal(t, "P1", nextPage)

	status, body = lemmyPostGet(t, fixture, lemmyFixtureHost, "type_=Local&sort=New&show_nsfw=true&limit=2&page_cursor=P1", false)
	require.Equal(t, http.StatusOK, status, body)
	assert.Equal(t, `{"posts":[]}`, body, "an empty page has no next_page")

	status, body = lemmyPostGet(t, fixture, lemmyFixtureHost, "type_=Local&sort=New&show_nsfw=true&limit=2", false)
	require.Equal(t, http.StatusOK, status, body)
	ids, nextPage, _ = lemmyPostPage(t, body)
	assert.Equal(t, []int64{3, 7}, ids, "the new post leads a fresh walk")
	assert.Equal(t, "P7", nextPage)
}

func TestLemmyFixture_PostListCursorIsLowercaseHex(t *testing.T) {
	fixture := newLemmyFixture(t)
	fixture.addPosts(lemmyFixtureHost,
		lemmyPost{id: 255, published: lemmyFixturePostMinute(2), community: "https://lemmy.world/c/a"},
		lemmyPost{id: 254, published: lemmyFixturePostMinute(1), community: "https://lemmy.world/c/a"},
	)

	_, body := lemmyPostGet(t, fixture, lemmyFixtureHost, "type_=Local&sort=New&limit=1", false)
	_, nextPage, _ := lemmyPostPage(t, body)
	assert.Equal(t, "Pff", nextPage)
	status, body := lemmyPostGet(t, fixture, lemmyFixtureHost, "type_=Local&sort=New&limit=1&page_cursor=Pff", false)
	require.Equal(t, http.StatusOK, status, body)
	ids, _, _ := lemmyPostPage(t, body)
	assert.Equal(t, []int64{254}, ids)
}

func TestLemmyFixture_PostListServesTheViewShape(t *testing.T) {
	fixture := newLemmyFixture(t)
	fixture.addPosts(lemmyFixtureHost, lemmyPost{
		id: 12, published: lemmyFixturePostMinute(3), featuredLocal: true, community: "https://lemmy.world/c/pinned",
	})

	_, body := lemmyPostGet(t, fixture, lemmyFixtureHost, "type_=Local&sort=New&show_nsfw=true&limit=50", false)
	assert.JSONEq(t, `{"posts":[{
		"post":{
			"id":12,"name":"post 12","ap_id":"https://lemmy.world/post/12",
			"published":"2026-10-01T00:03:00.000000Z","local":true,"nsfw":false,
			"removed":false,"deleted":false,"featured_local":true,"featured_community":false
		},
		"community":{
			"id":1,"name":"pinned","actor_id":"https://lemmy.world/c/pinned",
			"published":"2020-01-01T00:00:00.000000Z","local":true,"hidden":false,
			"removed":false,"deleted":false,"nsfw":false
		},
		"counts":{"post_id":12,"published":"2026-10-01T00:03:00.000000Z"}
	}],"next_page":"Pc"}`, body)
}

func TestLemmyFixture_PostListInjectsAnswers(t *testing.T) {
	fixture := newLemmyFixture(t)
	fixture.addPosts(lemmyFixtureHost,
		lemmyPost{id: 2, published: lemmyFixturePostMinute(2), community: "https://lemmy.world/c/b"},
		lemmyPost{id: 1, published: lemmyFixturePostMinute(1), community: "https://lemmy.world/c/a"},
	)
	fixture.failPosts(lemmyFixtureHost, "", http.StatusInternalServerError)
	fixture.servePostsBody(lemmyFixtureHost, "P2", "not json")
	fixture.padPosts(lemmyFixtureHost, "P1", 1024)

	status, _ := lemmyPostGet(t, fixture, lemmyFixtureHost, "type_=Local&sort=New&limit=1", false)
	assert.Equal(t, http.StatusInternalServerError, status)
	status, body := lemmyPostGet(t, fixture, lemmyFixtureHost, "type_=Local&sort=New&limit=1&page_cursor=P2", false)
	assert.Equal(t, http.StatusOK, status)
	assert.Equal(t, "not json", body)
	status, body = lemmyPostGet(t, fixture, lemmyFixtureHost, "type_=Local&sort=New&limit=1&page_cursor=P1", false)
	assert.Equal(t, http.StatusOK, status)
	assert.Len(t, body, 1024)
	ids, _, hasNext := lemmyPostPage(t, body)
	assert.Equal(t, []int64{}, ids, "a padded page is still the real page")
	assert.False(t, hasNext)

	fixture.clearInjections()
	status, body = lemmyPostGet(t, fixture, lemmyFixtureHost, "type_=Local&sort=New&limit=1", false)
	assert.Equal(t, http.StatusOK, status)
	ids, nextPage, _ := lemmyPostPage(t, body)
	assert.Equal(t, []int64{2}, ids)
	assert.Equal(t, "P2", nextPage)
}

// Post list requests are recorded apart, so recorded and pagesRequested, and
// the sweep helpers over them, keep counting community list pages only.
func TestLemmyFixture_RecordsPostListRequestsApart(t *testing.T) {
	fixture := newLemmyFixture(t)
	fixture.addCommunities(lemmyFixtureHost, 1)
	fixture.addPosts(lemmyFixtureHost,
		lemmyPost{id: 1, published: lemmyFixturePostMinute(1), community: "https://lemmy.world/c/a"})

	lemmyGet(t, fixture, lemmyFixtureHost, "sort=Old&page=1")
	request, err := http.NewRequest(http.MethodGet,
		"https://lemmy.world"+lemmyPostListPath+"?type_=Local&sort=New&limit=1", nil)
	require.NoError(t, err)
	request.Header.Set("User-Agent", "fixture-test/1.0")
	response, err := fixture.client().Do(request)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	lemmyPostGet(t, fixture, lemmyFixtureHost, "type_=Local&sort=New&limit=1&page_cursor=P1", false)

	assert.Equal(t, []int{1}, fixture.pagesRequested(lemmyFixtureHost, "Old"))
	assert.Len(t, fixture.recorded(), 1)
	assert.Equal(t, []lemmyListRequest{
		{
			host: "lemmy.world", scheme: "https",
			query:     url.Values{"type_": {"Local"}, "sort": {"New"}, "limit": {"1"}},
			userAgent: "fixture-test/1.0", status: http.StatusOK,
		},
		{
			host: "lemmy.world", scheme: "http",
			query:     url.Values{"type_": {"Local"}, "sort": {"New"}, "limit": {"1"}, "page_cursor": {"P1"}},
			userAgent: "Go-http-client/1.1", status: http.StatusOK,
		},
	}, fixture.recordedPosts())
	assert.Equal(t, []string{"", "P1"}, fixture.postCursorsRequested(lemmyFixtureHost))
	assert.Empty(t, fixture.postCursorsRequested("lemmy.ml"))
}
