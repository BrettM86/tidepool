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
	labels []store.InstanceLabel
	err    error
}

func (s fakeLabelSource) ListInstanceLabels(_ context.Context, _ string) ([]store.InstanceLabel, error) {
	return s.labels, s.err
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
	client, err := NewCloudflareClient(CloudflareOptions{
		BaseURL: fake.server.URL + "/client/v4", Token: delegationToken, ZoneID: delegationZoneID,
		HTTPClient: ap.NewGuardedHTTPClient(true, 5*time.Second),
	})
	require.NoError(t, err)
	var log bytes.Buffer
	reconciler, err := NewReconciler(Options{
		Client: client, Labels: labels, ZoneRoot: delegationZoneRoot,
		Nameservers: []string{"ns1.tdpl.example", "ns2.tdpl.example"},
		Logger:      slog.New(slog.NewTextHandler(&log, &slog.HandlerOptions{Level: slog.LevelDebug})),
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
