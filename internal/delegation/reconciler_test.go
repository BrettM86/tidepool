package delegation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/store"
)

const (
	delegationZoneRoot = "tdpl.example"
	delegationZoneID   = "zone-123"
	delegationToken    = "cf-test-token-7f3a"
	delegationPath     = "/client/v4/zones/zone-123/dns_records"
)

type fakeLabelSource struct {
	labels         []store.InstanceLabel
	err            error
	contributions  map[string][]store.Contribution
	requests       *[]contributionRequest
	pageErrors     map[string]map[int]error
	outrightActors map[string][]fakeOutrightActor
	outrightError  error
	acceptances    fakeAcceptanceChecker
}

type contributionRequest struct {
	label  string
	after  store.ContributionCursor
	cutoff time.Time
}

type fakeOutrightActor struct {
	createdAt         time.Time
	followedCommunity bool
}

func (s fakeLabelSource) ListInstanceLabels(_ context.Context, _ string) ([]store.InstanceLabel, error) {
	return s.labels, s.err
}

func (s fakeLabelSource) ListLabelContributions(_ context.Context, _ string, label string, cutoff time.Time, after store.ContributionCursor, limit int) ([]store.Contribution, error) {
	page := 1
	if s.requests != nil {
		for _, request := range *s.requests {
			if request.label == label {
				page++
			}
		}
		*s.requests = append(*s.requests, contributionRequest{label: label, after: after, cutoff: cutoff})
	}
	if err := s.pageErrors[label][page]; err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}
	rows := append([]store.Contribution(nil), s.contributions[label]...)
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].IndexedAt.Equal(rows[j].IndexedAt) {
			return rows[i].ID < rows[j].ID
		}
		return rows[i].IndexedAt.Before(rows[j].IndexedAt)
	})
	var result []store.Contribution
	for _, row := range rows {
		if row.IndexedAt.After(cutoff) || !after.IndexedAt.IsZero() &&
			(row.IndexedAt.Before(after.IndexedAt) || row.IndexedAt.Equal(after.IndexedAt) && row.ID <= after.ID) {
			continue
		}
		result = append(result, row)
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (s fakeLabelSource) ListOutrightQualifiedLabels(_ context.Context, _ string, labels []string, grandfatherCutoff time.Time) ([]store.OutrightQualification, error) {
	if s.outrightError != nil {
		return nil, s.outrightError
	}
	var qualified []store.OutrightQualification
	for _, label := range labels {
		for _, actor := range s.outrightActors[label] {
			if actor.followedCommunity || actor.createdAt.Before(grandfatherCutoff) {
				qualified = append(qualified, store.OutrightQualification{Label: label, QualifiedAt: actor.createdAt})
				break
			}
		}
	}
	return qualified, nil
}

// acceptanceKey names an acceptance by the community that holds it and its
// subject, so a check against the wrong community finds nothing.
type acceptanceKey struct {
	communityDID, subjectURI string
}

func contributionAcceptance(candidate store.Contribution) acceptanceKey {
	return acceptanceKey{communityDID: candidate.CommunityDID, subjectURI: candidate.ATURI}
}

type fakeAcceptanceChecker map[acceptanceKey]struct {
	standing bool
	err      error
}

func (checker fakeAcceptanceChecker) AcceptanceStands(_ context.Context, communityDID, subjectURI string) (bool, error) {
	answer := checker[acceptanceKey{communityDID: communityDID, subjectURI: subjectURI}]
	return answer.standing, answer.err
}

var delegationTestNow = time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)

// testContribution builds a candidate; a comment hangs under a standing legacy
// post root, which needs no acceptance check.
func testContribution(label, collection string, indexedAt time.Time, id int64) store.Contribution {
	contribution := store.Contribution{
		ATURI:      fmt.Sprintf("at://did:plc:delegationauthor/%s/%s-%d", collection, label, id),
		Collection: collection, CommunityDID: "did:plc:followedcommunity", IndexedAt: indexedAt, ID: id,
	}
	if collection == "social.coves.community.comment" {
		contribution.RootATURI = fmt.Sprintf("at://did:plc:followedcommunity/social.coves.community.post/%s-root-%d", label, id)
		contribution.RootCollection = "social.coves.community.post"
		contribution.RootCommunityDID = "did:plc:followedcommunity"
	}
	return contribution
}

type fakeDNSRecord struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Name    string `json:"name"`
	Content string `json:"content"`
	TTL     int    `json:"ttl"`
}

type fakeCloudflareRequest struct {
	Method        string
	Path          string
	Query         url.Values
	Authorization string
	Body          map[string]any
}

type fakeCloudflareAPI struct {
	mu              sync.Mutex
	server          *httptest.Server
	records         []fakeDNSRecord
	requests        []fakeCloudflareRequest
	listingFailPage int
	listingFailCode int
	listingFailBody string
	createFailCode  int
	createFailBody  string
	failedPostLabel string
	postCount       int
	// pageSize overrides the default listing page size when positive.
	pageSize int
	// omitResultInfo drops result_info from listing responses.
	omitResultInfo bool
	// reportedTotalPages overrides result_info.total_pages when positive.
	reportedTotalPages int
}

func newFakeCloudflareAPI(t *testing.T, records []fakeDNSRecord) *fakeCloudflareAPI {
	t.Helper()
	fake := &fakeCloudflareAPI{records: append([]fakeDNSRecord{}, records...)}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.serveHTTP))
	t.Cleanup(func() {
		fake.server.Close()
		for _, request := range fake.snapshot() {
			require.NotContains(t, []string{http.MethodDelete, http.MethodPut, http.MethodPatch}, request.Method,
				"delegation must never modify or delete existing records")
			require.Equal(t, "Bearer "+delegationToken, request.Authorization, "every request must authenticate")
			require.Equal(t, delegationPath, request.Path)
		}
	})
	return fake
}

func (f *fakeCloudflareAPI) snapshot() []fakeCloudflareRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fakeCloudflareRequest(nil), f.requests...)
}

func (f *fakeCloudflareAPI) failedLabel() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.failedPostLabel
}

func (f *fakeCloudflareAPI) serveHTTP(w http.ResponseWriter, r *http.Request) {
	request := fakeCloudflareRequest{
		Method: r.Method, Path: r.URL.Path, Query: r.URL.Query(), Authorization: r.Header.Get("Authorization"),
	}
	var bodyError error
	if r.Method == http.MethodPost {
		bodyError = json.NewDecoder(r.Body).Decode(&request.Body)
	}
	f.mu.Lock()
	f.requests = append(f.requests, request)
	if r.Method == http.MethodPost {
		f.postCount++
	}
	postPosition := f.postCount
	f.mu.Unlock()
	if bodyError != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if request.Authorization != "Bearer "+delegationToken {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	if request.Path != delegationPath {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	switch r.Method {
	case http.MethodGet:
		page := 1
		if raw := request.Query.Get("page"); raw != "" {
			var err error
			page, err = strconv.Atoi(raw)
			if err != nil || page < 1 {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
		}
		if page == f.listingFailPage {
			w.WriteHeader(f.listingFailCode)
			_, _ = w.Write([]byte(f.listingFailBody))
			return
		}
		pageSize := 2 // Deliberately ignore per_page, type and name filters.
		if f.pageSize > 0 {
			pageSize = f.pageSize
		}
		start := min((page-1)*pageSize, len(f.records))
		end := min(start+pageSize, len(f.records))
		// Cloudflare reports total_pages 0 for an empty result set.
		totalPages := (len(f.records) + pageSize - 1) / pageSize
		if f.reportedTotalPages > 0 {
			totalPages = f.reportedTotalPages
		}
		response := map[string]any{
			"success": true, "errors": []any{}, "messages": []any{}, "result": f.records[start:end],
		}
		if !f.omitResultInfo {
			response["result_info"] = map[string]int{"page": page, "per_page": pageSize, "count": end - start,
				"total_count": len(f.records), "total_pages": totalPages}
		}
		_ = json.NewEncoder(w).Encode(response)
	case http.MethodPost:
		if postPosition == 2 && f.createFailBody != "" {
			name, _ := request.Body["name"].(string)
			f.mu.Lock()
			f.failedPostLabel = strings.TrimSuffix(name, "."+delegationZoneRoot)
			f.mu.Unlock()
			w.WriteHeader(f.createFailCode)
			_, _ = w.Write([]byte(f.createFailBody))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true, "errors": []any{}, "messages": []any{}, "result": request.Body,
		})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func newTestReconciler(t *testing.T, fake *fakeCloudflareAPI, labels fakeLabelSource) (*Reconciler, *bytes.Buffer) {
	t.Helper()
	if labels.contributions == nil {
		labels.contributions = make(map[string][]store.Contribution)
		for _, label := range labels.labels {
			if label.HasLiveActor {
				labels.contributions[label.Label] = []store.Contribution{
					testContribution(label.Label, "social.coves.community.comment", delegationTestNow.Add(-2*time.Hour), 1),
				}
			}
		}
	}
	client, err := NewCloudflareClient(CloudflareOptions{
		BaseURL: fake.server.URL + "/client/v4", Token: delegationToken, ZoneID: delegationZoneID,
		HTTPClient: ap.NewGuardedHTTPClient(true, 5*time.Second),
	})
	require.NoError(t, err)
	var log bytes.Buffer
	reconciler, err := NewReconciler(Options{
		Client: client, Labels: labels, ZoneRoot: delegationZoneRoot,
		Nameservers:   []string{"ns1.tdpl.example", "ns2.tdpl.example"},
		Logger:        slog.New(slog.NewTextHandler(&log, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Contributions: labels, Acceptances: labels.acceptances, Now: func() time.Time { return delegationTestNow },
	})
	require.NoError(t, err)
	return reconciler, &log
}

func requestsByMethod(requests []fakeCloudflareRequest, method string) []fakeCloudflareRequest {
	var matching []fakeCloudflareRequest
	for _, request := range requests {
		if request.Method == method {
			matching = append(matching, request)
		}
	}
	return matching
}

func requireDelegationResult(t *testing.T, result Result, created, delegated, withoutLive []string, conflicts []Conflict, failed []Failure) {
	t.Helper()
	require.ElementsMatch(t, created, result.Created, "Created")
	require.ElementsMatch(t, delegated, result.AlreadyDelegated, "AlreadyDelegated")
	require.ElementsMatch(t, withoutLive, result.DelegatedWithoutLiveActors, "DelegatedWithoutLiveActors")
	require.ElementsMatch(t, conflicts, result.Conflicting, "Conflicting")
	require.ElementsMatch(t, failed, result.Failed, "Failed")
}

func TestReconcileCreatesMissingLiveDelegation(t *testing.T) {
	fake := newFakeCloudflareAPI(t, nil)
	r, _ := newTestReconciler(t, fake, fakeLabelSource{labels: []store.InstanceLabel{{Label: "a", HasLiveActor: true}}})
	result, err := r.Reconcile(context.Background())
	require.NoError(t, err)
	requireDelegationResult(t, result, []string{"a"}, nil, nil, nil, nil)
	posts := requestsByMethod(fake.snapshot(), http.MethodPost)
	require.Len(t, posts, 1)
	require.Equal(t, delegationPath, posts[0].Path)
	require.Equal(t, "Bearer "+delegationToken, posts[0].Authorization)
	require.Equal(t, map[string]any{"type": "NS", "name": "a.tdpl.example", "content": "ns1.tdpl.example", "ttl": float64(3600)}, posts[0].Body)
	require.NotContains(t, posts[0].Body, "proxied")
}

func TestReconcileReadsAllPagesAndMatchesOnlyApexNS(t *testing.T) {
	fake := newFakeCloudflareAPI(t, []fakeDNSRecord{
		{Type: "NS", Name: "a.tdpl.example", Content: "NS1.tdpl.example."},
		{Type: "NS", Name: "b.tdpl.example", Content: "ns2.tdpl.example"},
		{Type: "NS", Name: "C.TDPL.example.", Content: "ns1.tdpl.example"},
		{Type: "NS", Name: "x.a2.tdpl.example", Content: "ns1.tdpl.example"},
		{Type: "A", Name: "d.tdpl.example", Content: "ns1.tdpl.example"},
		{Type: "A", Name: "e.tdpl.example", Content: "192.0.2.2"},
	})
	r, _ := newTestReconciler(t, fake, fakeLabelSource{labels: []store.InstanceLabel{
		{Label: "a", HasLiveActor: true}, {Label: "b", HasLiveActor: true},
		{Label: "c", HasLiveActor: true}, {Label: "a2", HasLiveActor: true}, {Label: "d", HasLiveActor: true},
	}})
	result, err := r.Reconcile(context.Background())
	require.NoError(t, err)
	requireDelegationResult(t, result, []string{"a2", "d"}, []string{"a", "b", "c"}, nil, nil, nil)
	requests := fake.snapshot()
	var pages []int
	for _, request := range requestsByMethod(requests, http.MethodGet) {
		page := 1
		if request.Query.Get("page") != "" {
			parsedPage, parseErr := strconv.Atoi(request.Query.Get("page"))
			require.NoError(t, parseErr)
			page = parsedPage
		}
		pages = append(pages, page)
	}
	require.ElementsMatch(t, []int{1, 2, 3}, pages)
	var postNames []string
	for _, request := range requestsByMethod(requests, http.MethodPost) {
		postNames = append(postNames, request.Body["name"].(string))
	}
	require.ElementsMatch(t, []string{"a2.tdpl.example", "d.tdpl.example"}, postNames)
}

func TestReconcileWarnsAndPreservesForeignDelegations(t *testing.T) {
	for _, tc := range []struct {
		name      string
		records   []fakeDNSRecord
		delegated []string
	}{
		{name: "foreign only", records: []fakeDNSRecord{{Type: "NS", Name: "conflicted.tdpl.example", Content: "other-provider.example"}}},
		{name: "configured and foreign", records: []fakeDNSRecord{
			{Type: "NS", Name: "conflicted.tdpl.example", Content: "ns1.tdpl.example"},
			{Type: "NS", Name: "conflicted.tdpl.example", Content: "other-provider.example"},
		}, delegated: []string{"conflicted"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeCloudflareAPI(t, tc.records)
			r, log := newTestReconciler(t, fake, fakeLabelSource{labels: []store.InstanceLabel{{Label: "conflicted", HasLiveActor: true}}})
			result, err := r.Reconcile(context.Background())
			require.NoError(t, err)
			requireDelegationResult(t, result, nil, tc.delegated, nil,
				[]Conflict{{Label: "conflicted", Contents: []string{"other-provider.example"}}}, nil)
			require.Contains(t, log.String(), "level=WARN")
			require.Contains(t, log.String(), "conflicted")
			require.Contains(t, log.String(), "other-provider.example")
			requests := fake.snapshot()
			require.NotEmpty(t, requests)
			for _, request := range requests {
				require.Equal(t, http.MethodGet, request.Method)
			}
		})
	}
}

func TestReconcileCreatesOnlyForLiveActors(t *testing.T) {
	fake := newFakeCloudflareAPI(t, nil)
	r, _ := newTestReconciler(t, fake, fakeLabelSource{labels: []store.InstanceLabel{
		{Label: "live1", HasLiveActor: true}, {Label: "dead1", HasLiveActor: false},
	}})
	result, err := r.Reconcile(context.Background())
	require.NoError(t, err)
	requireDelegationResult(t, result, []string{"live1"}, nil, nil, nil, nil)
	posts := requestsByMethod(fake.snapshot(), http.MethodPost)
	require.Len(t, posts, 1)
	require.Equal(t, "live1.tdpl.example", posts[0].Body["name"])
}

func TestReconcileReportsDelegationsWithoutLiveActors(t *testing.T) {
	fake := newFakeCloudflareAPI(t, []fakeDNSRecord{
		{Type: "NS", Name: "gone.tdpl.example", Content: "ns1.tdpl.example"},
		{Type: "NS", Name: "ghost.tdpl.example", Content: "ns2.tdpl.example"},
		{Type: "NS", Name: "foreign.tdpl.example", Content: "other-provider.example"},
	})
	r, log := newTestReconciler(t, fake, fakeLabelSource{labels: []store.InstanceLabel{
		{Label: "gone", HasLiveActor: false}, {Label: "foreign", HasLiveActor: false},
	}})
	result, err := r.Reconcile(context.Background())
	require.NoError(t, err)
	requireDelegationResult(t, result, nil, nil, []string{"gone", "ghost"},
		[]Conflict{{Label: "foreign", Contents: []string{"other-provider.example"}}}, nil)
	require.Contains(t, log.String(), "gone")
	require.Contains(t, log.String(), "ghost")
	require.Empty(t, requestsByMethod(fake.snapshot(), http.MethodPost))
}

func TestReconcileContinuesAfterSecondCreateFails(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		message string
	}{
		{name: "HTTP 500", status: http.StatusInternalServerError, message: "cloudflare create temporarily unavailable"},
		{name: "success false", status: http.StatusOK, message: "cloudflare NS record rejected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeCloudflareAPI(t, nil)
			fake.createFailCode = tc.status
			fake.createFailBody = fmt.Sprintf(`{"success":false,"errors":[{"code":81057,"message":%q}],"messages":[],"result":null}`, tc.message)
			r, log := newTestReconciler(t, fake, fakeLabelSource{labels: []store.InstanceLabel{
				{Label: "first", HasLiveActor: true}, {Label: "second", HasLiveActor: true},
				{Label: "third", HasLiveActor: true},
			}})
			result, err := r.Reconcile(context.Background())
			require.Error(t, err)
			posts := requestsByMethod(fake.snapshot(), http.MethodPost)
			require.Len(t, posts, 3)
			failedLabel := fake.failedLabel()
			require.NotEmpty(t, failedLabel)
			require.Equal(t, strings.TrimSuffix(posts[1].Body["name"].(string), ".tdpl.example"), failedLabel)
			var created []string
			for _, label := range []string{"first", "second", "third"} {
				if label != failedLabel {
					created = append(created, label)
				}
			}
			require.ElementsMatch(t, created, result.Created)
			require.Empty(t, result.AlreadyDelegated)
			require.Empty(t, result.Conflicting)
			require.Empty(t, result.DelegatedWithoutLiveActors)
			require.Len(t, result.Failed, 1)
			require.Equal(t, failedLabel, result.Failed[0].Label)
			require.Contains(t, result.Failed[0].Reason, tc.message)
			require.NotContains(t, err.Error(), delegationToken)
			require.NotContains(t, log.String(), delegationToken)
		})
	}
}

func TestReconcileAbortsBeforeCreatingOnListingFailure(t *testing.T) {
	for _, tc := range []struct {
		name   string
		page   int
		status int
		body   string
	}{
		{name: "first page HTTP 500", page: 1, status: http.StatusInternalServerError,
			body: `{"success":false,"errors":[{"code":1000,"message":"listing unavailable"}],"messages":[],"result":null}`},
		{name: "second page success false", page: 2, status: http.StatusOK,
			body: `{"success":false,"errors":[{"code":81057,"message":"listing refused on page two"}],"messages":[],"result":null}`},
		{name: "first page malformed JSON", page: 1, status: http.StatusOK, body: `{not-json`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeCloudflareAPI(t, []fakeDNSRecord{
				{Type: "A", Name: "one.tdpl.example", Content: "192.0.2.1"},
				{Type: "A", Name: "two.tdpl.example", Content: "192.0.2.2"},
				{Type: "A", Name: "three.tdpl.example", Content: "192.0.2.3"},
			})
			fake.listingFailPage, fake.listingFailCode, fake.listingFailBody = tc.page, tc.status, tc.body
			r, log := newTestReconciler(t, fake, fakeLabelSource{labels: []store.InstanceLabel{{Label: "missing", HasLiveActor: true}}})
			_, err := r.Reconcile(context.Background())
			require.Error(t, err)
			requests := fake.snapshot()
			require.Empty(t, requestsByMethod(requests, http.MethodPost))
			var pages []int
			for _, request := range requestsByMethod(requests, http.MethodGet) {
				page := 1
				if request.Query.Get("page") != "" {
					parsedPage, parseErr := strconv.Atoi(request.Query.Get("page"))
					require.NoError(t, parseErr)
					page = parsedPage
				}
				pages = append(pages, page)
			}
			if tc.page == 2 {
				require.Contains(t, pages, 2)
			}
			require.NotContains(t, err.Error(), delegationToken)
			require.NotContains(t, log.String(), delegationToken)
		})
	}
}

func TestReconcileAbortsWhenListingOmitsResultInfo(t *testing.T) {
	var records []fakeDNSRecord
	for index := range 100 {
		records = append(records, fakeDNSRecord{Type: "NS", Name: fmt.Sprintf("filler%03d.tdpl.example", index), Content: "ns1.tdpl.example"})
	}
	records = append(records, fakeDNSRecord{Type: "NS", Name: "conflicted.tdpl.example", Content: "other-provider.example"})
	fake := newFakeCloudflareAPI(t, records)
	fake.pageSize = 100
	fake.omitResultInfo = true
	r, log := newTestReconciler(t, fake, fakeLabelSource{labels: []store.InstanceLabel{{Label: "conflicted", HasLiveActor: true}}})
	_, err := r.Reconcile(context.Background())
	require.Empty(t, requestsByMethod(fake.snapshot(), http.MethodPost))
	require.Error(t, err)
	require.Contains(t, err.Error(), "result_info")
	require.NotContains(t, err.Error(), delegationToken)
	require.NotContains(t, log.String(), delegationToken)
}

func TestReconcileAbortsWhenListingExceedsPageCap(t *testing.T) {
	// The listing cap is 100 pages of 100 records, well above the zone quota.
	const listingPageCap = 100
	fake := newFakeCloudflareAPI(t, nil)
	fake.reportedTotalPages = listingPageCap + 1
	r, log := newTestReconciler(t, fake, fakeLabelSource{labels: []store.InstanceLabel{{Label: "missing", HasLiveActor: true}}})
	_, err := r.Reconcile(context.Background())
	requests := fake.snapshot()
	require.Empty(t, requestsByMethod(requests, http.MethodPost))
	require.Len(t, requestsByMethod(requests, http.MethodGet), listingPageCap)
	require.Error(t, err)
	require.NotContains(t, err.Error(), delegationToken)
	require.NotContains(t, log.String(), delegationToken)
}

func TestReconcileDoesNotClassifyLabelsWhenSourceFails(t *testing.T) {
	fake := newFakeCloudflareAPI(t, []fakeDNSRecord{{Type: "NS", Name: "ghost.tdpl.example", Content: "ns1.tdpl.example"}})
	r, _ := newTestReconciler(t, fake, fakeLabelSource{err: errors.New("label source unavailable")})
	result, err := r.Reconcile(context.Background())
	require.Error(t, err)
	require.Empty(t, requestsByMethod(fake.snapshot(), http.MethodPost))
	require.Empty(t, result.DelegatedWithoutLiveActors)
}

func TestReconcileRequiresStandingContributionAtLeastOneHourOld(t *testing.T) {
	for _, tc := range []struct {
		name       string
		collection string
		indexedAt  time.Time
		standing   bool
		checkError error
		created    bool
	}{
		{name: "standing postv2", collection: "social.coves.community.postv2", indexedAt: delegationTestNow.Add(-2 * time.Hour), standing: true, created: true},
		{name: "removed postv2", collection: "social.coves.community.postv2", indexedAt: delegationTestNow.Add(-2 * time.Hour)},
		{name: "young comment", collection: "social.coves.community.comment", indexedAt: delegationTestNow.Add(-10 * time.Minute)},
		{name: "comment at cutoff", collection: "social.coves.community.comment", indexedAt: time.Date(2026, 10, 10, 11, 0, 0, 0, time.UTC), created: true},
		{name: "comment one second past cutoff", collection: "social.coves.community.comment", indexedAt: time.Date(2026, 10, 10, 11, 0, 1, 0, time.UTC)},
		{name: "legacy post needs no acceptance check", collection: "social.coves.community.post", indexedAt: delegationTestNow.Add(-2 * time.Hour), checkError: errors.New("legacy post must not invoke checker"), created: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeCloudflareAPI(t, nil)
			candidate := testContribution("candidate", tc.collection, tc.indexedAt, 7)
			var requests []contributionRequest
			r, _ := newTestReconciler(t, fake, fakeLabelSource{
				labels:        []store.InstanceLabel{{Label: "candidate", HasLiveActor: true}},
				contributions: map[string][]store.Contribution{"candidate": {candidate}},
				requests:      &requests,
				acceptances:   fakeAcceptanceChecker{contributionAcceptance(candidate): {standing: tc.standing, err: tc.checkError}},
			})
			result, err := r.Reconcile(context.Background())
			require.NoError(t, err)
			if tc.created {
				require.ElementsMatch(t, []string{"candidate"}, result.Created)
				require.Empty(t, result.Pending)
				require.ElementsMatch(t, []string{"candidate.tdpl.example"}, postNames(fake.snapshot()))
			} else {
				require.ElementsMatch(t, []string{"candidate"}, result.Pending)
				require.Empty(t, result.Created)
				require.Empty(t, requestsByMethod(fake.snapshot(), http.MethodPost))
			}
			require.Empty(t, result.Failed)
		})
	}
}

func TestReconcileCommentQualifiesOnlyWhileItsThreadRootStands(t *testing.T) {
	type answer = struct {
		standing bool
		err      error
	}
	const (
		rootATURI        = "at://did:plc:delegationrootauthor/social.coves.community.postv2/thread-root"
		rootCommunityDID = "did:plc:delegationrootcommunity"
	)
	for _, tc := range []struct {
		name           string
		rootCollection string
		acceptances    func(comment store.Contribution) fakeAcceptanceChecker
		created        bool
	}{
		{name: "standing postv2 root", rootCollection: "social.coves.community.postv2", created: true,
			acceptances: func(store.Contribution) fakeAcceptanceChecker {
				return fakeAcceptanceChecker{{communityDID: rootCommunityDID, subjectURI: rootATURI}: answer{standing: true}}
			}},
		{name: "removed postv2 root", rootCollection: "social.coves.community.postv2",
			acceptances: func(store.Contribution) fakeAcceptanceChecker {
				return fakeAcceptanceChecker{{communityDID: rootCommunityDID, subjectURI: rootATURI}: answer{}}
			}},
		{name: "root accepted only in the comment's community", rootCollection: "social.coves.community.postv2",
			acceptances: func(comment store.Contribution) fakeAcceptanceChecker {
				return fakeAcceptanceChecker{{communityDID: comment.CommunityDID, subjectURI: rootATURI}: answer{standing: true}}
			}},
		{name: "acceptance of the comment is not the root's", rootCollection: "social.coves.community.postv2",
			acceptances: func(comment store.Contribution) fakeAcceptanceChecker {
				return fakeAcceptanceChecker{
					{communityDID: rootCommunityDID, subjectURI: comment.ATURI}:     answer{standing: true},
					{communityDID: comment.CommunityDID, subjectURI: comment.ATURI}: answer{standing: true},
				}
			}},
		{name: "legacy post root needs no acceptance check", rootCollection: "social.coves.community.post", created: true,
			acceptances: func(store.Contribution) fakeAcceptanceChecker {
				return fakeAcceptanceChecker{{communityDID: rootCommunityDID, subjectURI: rootATURI}: answer{err: errors.New("legacy root must not invoke checker")}}
			}},
		{name: "comment without a root", rootCollection: "",
			acceptances: func(store.Contribution) fakeAcceptanceChecker { return fakeAcceptanceChecker{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeCloudflareAPI(t, nil)
			comment := testContribution("thread", "social.coves.community.comment", delegationTestNow.Add(-2*time.Hour), 9)
			comment.RootATURI, comment.RootCollection, comment.RootCommunityDID = rootATURI, tc.rootCollection, rootCommunityDID
			if tc.rootCollection == "" {
				comment.RootATURI, comment.RootCommunityDID = "", ""
			}
			r, _ := newTestReconciler(t, fake, fakeLabelSource{
				labels:        []store.InstanceLabel{{Label: "thread", HasLiveActor: true}},
				contributions: map[string][]store.Contribution{"thread": {comment}},
				acceptances:   tc.acceptances(comment),
			})
			result, err := r.Reconcile(context.Background())
			require.NoError(t, err)
			require.Empty(t, result.Failed)
			if tc.created {
				require.ElementsMatch(t, []string{"thread"}, result.Created)
				require.ElementsMatch(t, []string{"thread.tdpl.example"}, postNames(fake.snapshot()))
				return
			}
			require.ElementsMatch(t, []string{"thread"}, result.Pending)
			require.Empty(t, result.Created)
			require.Empty(t, postNames(fake.snapshot()))
		})
	}
}

func postNames(requests []fakeCloudflareRequest) []string {
	var names []string
	for _, request := range requestsByMethod(requests, http.MethodPost) {
		names = append(names, request.Body["name"].(string))
	}
	return names
}

func removedPostv2Candidates(label string) ([]store.Contribution, fakeAcceptanceChecker) {
	rows := make([]store.Contribution, 0, 101)
	checker := make(fakeAcceptanceChecker)
	for index := range 100 {
		candidate := testContribution(label, "social.coves.community.postv2", delegationTestNow.Add(-2*time.Hour), int64(index+1))
		rows = append(rows, candidate)
		checker[contributionAcceptance(candidate)] = struct {
			standing bool
			err      error
		}{}
	}
	return rows, checker
}

func TestReconcilePagesPastOneHundredRemovedPostv2s(t *testing.T) {
	rows, checker := removedPostv2Candidates("paged")
	rows = append(rows, testContribution("paged", "social.coves.community.comment", delegationTestNow.Add(-2*time.Hour), 101))
	var requests []contributionRequest
	fake := newFakeCloudflareAPI(t, nil)
	r, _ := newTestReconciler(t, fake, fakeLabelSource{
		labels:        []store.InstanceLabel{{Label: "paged", HasLiveActor: true}},
		contributions: map[string][]store.Contribution{"paged": rows}, requests: &requests, acceptances: checker,
	})
	result, err := r.Reconcile(context.Background())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"paged"}, result.Created)
	require.Empty(t, result.Pending)
	require.Empty(t, result.Failed)
	require.ElementsMatch(t, []string{"paged.tdpl.example"}, postNames(fake.snapshot()))
	require.GreaterOrEqual(t, len(requests), 2)
	require.Equal(t, "paged", requests[1].label)
	require.Equal(t, store.ContributionCursor{IndexedAt: delegationTestNow.Add(-2 * time.Hour), ID: 100}, requests[1].after)
}

func TestReconcileEndsAfterEmptySecondCandidatePage(t *testing.T) {
	rows, checker := removedPostv2Candidates("exhausted")
	var requests []contributionRequest
	fake := newFakeCloudflareAPI(t, nil)
	r, _ := newTestReconciler(t, fake, fakeLabelSource{
		labels:        []store.InstanceLabel{{Label: "exhausted", HasLiveActor: true}},
		contributions: map[string][]store.Contribution{"exhausted": rows}, requests: &requests, acceptances: checker,
	})
	result, err := r.Reconcile(context.Background())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"exhausted"}, result.Pending)
	require.Empty(t, result.Created)
	require.Empty(t, result.Failed)
	require.Empty(t, postNames(fake.snapshot()))
	require.GreaterOrEqual(t, len(requests), 2)
	require.Equal(t, store.ContributionCursor{IndexedAt: delegationTestNow.Add(-2 * time.Hour), ID: 100}, requests[1].after)
}

func TestReconcileContinuesAfterQualificationFailures(t *testing.T) {
	for _, tc := range []struct {
		name       string
		checkerErr error
		pageErrors map[string]map[int]error
		candidates []store.Contribution
		reason     string
	}{
		{name: "acceptance checker error", checkerErr: errors.New("acceptance repo unavailable"),
			candidates: []store.Contribution{testContribution("a", "social.coves.community.postv2", delegationTestNow.Add(-2*time.Hour), 1)}, reason: "acceptance repo unavailable"},
		{name: "second contribution page error", pageErrors: map[string]map[int]error{"a": {2: errors.New("candidate page unavailable")}},
			candidates: func() []store.Contribution { rows, _ := removedPostv2Candidates("a"); return rows }(), reason: "candidate page unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeCloudflareAPI(t, nil)
			checker := make(fakeAcceptanceChecker)
			for _, candidate := range tc.candidates {
				checker[contributionAcceptance(candidate)] = struct {
					standing bool
					err      error
				}{err: tc.checkerErr}
			}
			var requests []contributionRequest
			r, _ := newTestReconciler(t, fake, fakeLabelSource{
				labels: []store.InstanceLabel{{Label: "a", HasLiveActor: true}, {Label: "b", HasLiveActor: true}},
				contributions: map[string][]store.Contribution{"a": tc.candidates,
					"b": {testContribution("b", "social.coves.community.comment", delegationTestNow.Add(-2*time.Hour), 201)}},
				acceptances: checker, pageErrors: tc.pageErrors, requests: &requests,
			})
			result, err := r.Reconcile(context.Background())
			require.ErrorContains(t, err, tc.reason)
			require.ElementsMatch(t, []string{"b"}, result.Created)
			require.Empty(t, result.Pending)
			require.Len(t, result.Failed, 1)
			require.Equal(t, "a", result.Failed[0].Label)
			require.Contains(t, result.Failed[0].Reason, tc.reason)
			require.ElementsMatch(t, []string{"b.tdpl.example"}, postNames(fake.snapshot()))
		})
	}
}

func TestReconcileAbortsBeforeCreatesOnOutrightQueryError(t *testing.T) {
	fake := newFakeCloudflareAPI(t, nil)
	r, _ := newTestReconciler(t, fake, fakeLabelSource{
		labels:        []store.InstanceLabel{{Label: "a", HasLiveActor: true}, {Label: "b", HasLiveActor: true}},
		outrightError: errors.New("outright query unavailable"),
	})
	_, err := r.Reconcile(context.Background())
	require.ErrorContains(t, err, "outright query unavailable")
	require.Empty(t, postNames(fake.snapshot()))
}

func TestReconcileGrandfathersOnlyActorsBeforeCutoff(t *testing.T) {
	fake := newFakeCloudflareAPI(t, nil)
	r, _ := newTestReconciler(t, fake, fakeLabelSource{
		labels:        []store.InstanceLabel{{Label: "p", HasLiveActor: true}, {Label: "q", HasLiveActor: true}},
		contributions: map[string][]store.Contribution{},
		pageErrors:    map[string]map[int]error{"p": {1: errors.New("grandfathered label must not fetch candidates")}},
		outrightActors: map[string][]fakeOutrightActor{
			"p": {{createdAt: time.Date(2026, 10, 2, 23, 59, 59, 0, time.UTC)}},
			"q": {{createdAt: time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)}},
		},
	})
	result, err := r.Reconcile(context.Background())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"p"}, result.Created)
	require.ElementsMatch(t, []string{"q"}, result.Pending)
	require.Empty(t, result.Failed)
	require.ElementsMatch(t, []string{"p.tdpl.example"}, postNames(fake.snapshot()))
}
