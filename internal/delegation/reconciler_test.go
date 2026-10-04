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
	"regexp"
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
	// unfilteredOutright is returned by ListOutrightQualifiedLabels whatever labels were requested.
	unfilteredOutright []store.OutrightQualification
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
	qualified := append([]store.OutrightQualification(nil), s.unfilteredOutright...)
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
	ID            string `json:"id"`
	Type          string `json:"type"`
	Name          string `json:"name"`
	Content       string `json:"content"`
	TTL           int    `json:"ttl"`
	CreatedOn     string `json:"created_on,omitempty"`
	omitCreatedOn bool
}

const cloudflareTimestampLayout = "2006-01-02T15:04:05.000000Z07:00"

type fakeCloudflareRequest struct {
	Method        string
	Path          string
	Query         url.Values
	Authorization string
	Body          map[string]any
}

type fakeCloudflareAPI struct {
	mu               sync.Mutex
	server           *httptest.Server
	records          []fakeDNSRecord
	requests         []fakeCloudflareRequest
	listingFailPage  int
	listingFailCode  int
	listingFailBody  string
	createFailCode   int
	createFailBody   string
	failedPostLabel  string
	failPostLabels   map[string]bool
	now              func() time.Time
	maxNSRecords     int
	postCount        int
	listingCount     int
	failFirstListing bool
	holdMethod       string
	holdEntered      chan struct{}
	holdRelease      chan struct{}
	// pageSize overrides the default listing page size when positive.
	pageSize int
	// omitResultInfo drops result_info from listing responses.
	omitResultInfo bool
	// reportedTotalPages overrides result_info.total_pages when positive.
	reportedTotalPages int
}

func newFakeCloudflareAPI(t *testing.T, records []fakeDNSRecord) *fakeCloudflareAPI {
	t.Helper()
	fake := &fakeCloudflareAPI{records: append([]fakeDNSRecord{}, records...),
		now: func() time.Time { return delegationTestNow }, maxNSRecords: 1000}
	for index := range fake.records {
		if fake.records[index].CreatedOn == "" && !fake.records[index].omitCreatedOn {
			fake.records[index].CreatedOn = delegationTestNow.Add(-3 * time.Hour).Format(cloudflareTimestampLayout)
		}
	}
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

// holdFirstRequest blocks only the first request of method after recording it.
// Cleanup releases the handler before the fake server's Close cleanup runs.
func (f *fakeCloudflareAPI) holdFirstRequest(t *testing.T, method string) (<-chan struct{}, func()) {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	f.mu.Lock()
	f.holdMethod, f.holdEntered, f.holdRelease = method, entered, release
	f.mu.Unlock()
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	return entered, unblock
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
	if r.Method == http.MethodGet {
		f.listingCount++
	}
	if r.Method == http.MethodPost {
		f.postCount++
	}
	postPosition := f.postCount
	listingPosition := f.listingCount
	hold := r.Method == f.holdMethod && (r.Method == http.MethodGet && listingPosition == 1 || r.Method == http.MethodPost && postPosition == 1)
	entered, release := f.holdEntered, f.holdRelease
	f.mu.Unlock()
	if hold {
		close(entered)
		<-release
	}
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
		if page == f.listingFailPage || f.failFirstListing && listingPosition == 1 {
			w.WriteHeader(f.listingFailCode)
			_, _ = w.Write([]byte(f.listingFailBody))
			return
		}
		pageSize := 2 // Deliberately ignore per_page, type and name filters.
		if f.pageSize > 0 {
			pageSize = f.pageSize
		}
		f.mu.Lock()
		records := append([]fakeDNSRecord(nil), f.records...)
		f.mu.Unlock()
		start := min((page-1)*pageSize, len(records))
		end := min(start+pageSize, len(records))
		// Cloudflare reports total_pages 0 for an empty result set.
		totalPages := (len(records) + pageSize - 1) / pageSize
		if f.reportedTotalPages > 0 {
			totalPages = f.reportedTotalPages
		}
		response := map[string]any{
			"success": true, "errors": []any{}, "messages": []any{}, "result": records[start:end],
		}
		if !f.omitResultInfo {
			response["result_info"] = map[string]int{"page": page, "per_page": pageSize, "count": end - start,
				"total_count": len(records), "total_pages": totalPages}
		}
		_ = json.NewEncoder(w).Encode(response)
	case http.MethodPost:
		name, _ := request.Body["name"].(string)
		label := strings.TrimSuffix(name, "."+delegationZoneRoot)
		if f.failPostLabels[label] || postPosition == 2 && f.createFailBody != "" {
			f.mu.Lock()
			f.failedPostLabel = label
			f.mu.Unlock()
			if f.failPostLabels[label] {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"success":false,"errors":[{"message":"create refused"}],"result":null}`))
			} else {
				w.WriteHeader(f.createFailCode)
				_, _ = w.Write([]byte(f.createFailBody))
			}
			return
		}
		f.mu.Lock()
		f.records = append(f.records, fakeDNSRecord{ID: fmt.Sprintf("created-%d", postPosition),
			Type: "NS", Name: name, Content: request.Body["content"].(string),
			CreatedOn: f.now().Format(cloudflareTimestampLayout)})
		f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true, "errors": []any{}, "messages": []any{}, "result": request.Body,
		})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func newTestReconciler(t *testing.T, fake *fakeCloudflareAPI, labels fakeLabelSource, interval ...time.Duration) (*Reconciler, *bytes.Buffer) {
	t.Helper()
	return newTestReconcilerWithSource(t, fake, &labels, interval...)
}

func newTestReconcilerWithSource(t *testing.T, fake *fakeCloudflareAPI, labels *fakeLabelSource, interval ...time.Duration) (*Reconciler, *bytes.Buffer) {
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
	var configuredInterval time.Duration
	if len(interval) > 0 {
		configuredInterval = interval[0]
	}
	reconciler, err := NewReconciler(Options{
		Client: client, Labels: labels, ZoneRoot: delegationZoneRoot,
		Nameservers:   []string{"ns1.tdpl.example", "ns2.tdpl.example"},
		Logger:        slog.New(slog.NewTextHandler(&log, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Contributions: labels, Acceptances: labels.acceptances, Now: func() time.Time { return fake.now() },
		MaxNSRecords: fake.maxNSRecords, Interval: configuredInterval,
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

func configuredFakeRecords(count, recent int) []fakeDNSRecord {
	records := make([]fakeDNSRecord, 0, count)
	for index := range count {
		record := fakeDNSRecord{ID: fmt.Sprintf("existing-%02d", index), Type: "NS",
			Name: fmt.Sprintf("existing-%02d.tdpl.example", index), Content: "ns1.tdpl.example"}
		if index < recent {
			record.CreatedOn = delegationTestNow.Add(-30 * time.Minute).Format(cloudflareTimestampLayout)
		}
		records = append(records, record)
	}
	return records
}

func delegationLogLines(log string, level string, attributes ...string) []string {
	var matching []string
	for _, line := range strings.Split(strings.TrimSpace(log), "\n") {
		if !regexp.MustCompile(`(^| )level=` + regexp.QuoteMeta(level) + `( |$)`).MatchString(line) {
			continue
		}
		matches := true
		for _, attribute := range attributes {
			if !regexp.MustCompile(`(^| )` + regexp.QuoteMeta(attribute) + `( |$)`).MatchString(line) {
				matches = false
				break
			}
		}
		if matches {
			matching = append(matching, line)
		}
	}
	return matching
}

func TestReconcileRejectsRecordsWithoutValidCreatedOn(t *testing.T) {
	for _, tc := range []struct {
		name   string
		record fakeDNSRecord
	}{
		{name: "missing created_on", record: fakeDNSRecord{ID: "rec-broken", Type: "NS", Name: "broken.tdpl.example", Content: "ns1.tdpl.example", omitCreatedOn: true}},
		{name: "invalid created_on", record: fakeDNSRecord{ID: "rec-broken", Type: "NS", Name: "broken.tdpl.example", Content: "ns1.tdpl.example", CreatedOn: "yesterday"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeCloudflareAPI(t, []fakeDNSRecord{tc.record})
			r, _ := newTestReconciler(t, fake, fakeLabelSource{labels: []store.InstanceLabel{{Label: "qualifying", HasLiveActor: true}}})
			_, err := r.Reconcile(context.Background())
			require.ErrorContains(t, err, "rec-broken")
			require.Empty(t, postNames(fake.snapshot()))
		})
	}
}

func TestReconcileCountsConfiguredNSRecordsAcrossZone(t *testing.T) {
	fake := newFakeCloudflareAPI(t, []fakeDNSRecord{
		{Type: "NS", Name: "a.tdpl.example", Content: "ns1.tdpl.example"},
		{Type: "NS", Name: "b.tdpl.example", Content: "ns1.tdpl.example"},
		{Type: "NS", Name: "c.tdpl.example", Content: "ns1.tdpl.example"},
		{Type: "NS", Name: "d.tdpl.example", Content: "ns1.tdpl.example"},
		{Type: "NS", Name: "d.tdpl.example", Content: "ns2.tdpl.example"},
		{Type: "NS", Name: "tdpl.example", Content: "ns1.tdpl.example"},
		{Type: "NS", Name: "x.e.tdpl.example", Content: "ns1.tdpl.example"},
		{Type: "NS", Name: "f.tdpl.example", Content: "other-provider.example"},
		{Type: "A", Name: "ns1.tdpl.example", Content: "ns1.tdpl.example"},
	})
	fake.maxNSRecords = 100
	r, _ := newTestReconciler(t, fake, fakeLabelSource{labels: []store.InstanceLabel{{Label: "q", HasLiveActor: true}}})
	result, err := r.Reconcile(context.Background())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"q.tdpl.example"}, postNames(fake.snapshot()))
	require.ElementsMatch(t, []string{"q"}, result.Created)
	require.Equal(t, 8, result.DelegatedRecords)
	require.Equal(t, 100, result.Ceiling)
}

func TestReconcileCountsConfiguredNSRecordsOutsideBridgeHostname(t *testing.T) {
	outside := fakeDNSRecord{ID: "outside", Type: "NS", Name: "elsewhere.example", Content: "ns1.tdpl.example",
		CreatedOn: delegationTestNow.Add(-30 * time.Minute).Format(cloudflareTimestampLayout)}
	t.Run("ceiling", func(t *testing.T) {
		fake := newFakeCloudflareAPI(t, append(configuredFakeRecords(9, 0), outside))
		fake.maxNSRecords = 10
		r, _ := newTestReconciler(t, fake, fakeLabelSource{labels: []store.InstanceLabel{{Label: "qualified", HasLiveActor: true}}})
		result, err := r.Reconcile(context.Background())
		require.ErrorIs(t, err, ErrCeilingReached)
		require.Empty(t, postNames(fake.snapshot()))
		require.Empty(t, result.Created)
		require.ElementsMatch(t, []string{"qualified"}, result.Deferred)
		require.Equal(t, 10, result.DelegatedRecords)
	})
	t.Run("hourly budget", func(t *testing.T) {
		fake := newFakeCloudflareAPI(t, append(configuredFakeRecords(19, 19), outside))
		r, _ := newTestReconciler(t, fake, fakeLabelSource{labels: []store.InstanceLabel{{Label: "qualified", HasLiveActor: true}}})
		result, err := r.Reconcile(context.Background())
		require.NoError(t, err)
		require.Empty(t, postNames(fake.snapshot()))
		require.Empty(t, result.Created)
		require.ElementsMatch(t, []string{"qualified"}, result.Deferred)
		require.Equal(t, 20, result.DelegatedRecords)
	})
}

func TestReconcileCreatesOnlyForEligibleOutrightLabels(t *testing.T) {
	fake := newFakeCloudflareAPI(t, []fakeDNSRecord{
		{Type: "NS", Name: "foreign.tdpl.example", Content: "other-provider.example"},
		{Type: "NS", Name: "delegated.tdpl.example", Content: "ns1.tdpl.example"},
	})
	qualifiedAt := delegationTestNow.Add(-48 * time.Hour)
	r, _ := newTestReconciler(t, fake, fakeLabelSource{
		labels: []store.InstanceLabel{
			{Label: "eligible", HasLiveActor: true}, {Label: "foreign", HasLiveActor: true},
			{Label: "delegated", HasLiveActor: true}, {Label: "dormant", HasLiveActor: false},
		},
		contributions: map[string][]store.Contribution{},
		// The source ignores the requested labels and also returns ineligible ones.
		unfilteredOutright: []store.OutrightQualification{
			{Label: "eligible", QualifiedAt: qualifiedAt}, {Label: "foreign", QualifiedAt: qualifiedAt},
			{Label: "delegated", QualifiedAt: qualifiedAt}, {Label: "dormant", QualifiedAt: qualifiedAt},
			{Label: "unlisted", QualifiedAt: qualifiedAt},
		},
	})
	result, err := r.Reconcile(context.Background())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"eligible.tdpl.example"}, postNames(fake.snapshot()))
	requireDelegationResult(t, result, []string{"eligible"}, []string{"delegated"}, nil,
		[]Conflict{{Label: "foreign", Contents: []string{"other-provider.example"}}}, nil)
	require.Empty(t, result.Deferred)
	require.Empty(t, result.Pending)
	require.Equal(t, 2, result.DelegatedRecords)
}

func TestReconcileStopsAtCeilingOldestFirst(t *testing.T) {
	for _, tc := range []struct {
		name     string
		existing int
		created  []string
		deferred []string
		count    int
	}{
		{name: "full ceiling", existing: 10, deferred: []string{"alpha", "zulu"}, count: 2},
		{name: "one place left", existing: 9, created: []string{"zulu"}, deferred: []string{"alpha"}, count: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeCloudflareAPI(t, configuredFakeRecords(tc.existing, 0))
			fake.maxNSRecords = 10
			r, log := newTestReconciler(t, fake, fakeLabelSource{
				labels: []store.InstanceLabel{{Label: "alpha", HasLiveActor: true}, {Label: "zulu", HasLiveActor: true}},
				contributions: map[string][]store.Contribution{
					"alpha": {testContribution("alpha", "social.coves.community.comment", delegationTestNow.Add(-2*time.Hour), 1)},
					"zulu":  {testContribution("zulu", "social.coves.community.comment", delegationTestNow.Add(-5*time.Hour), 2)},
				},
			})
			result, err := r.Reconcile(context.Background())
			require.ErrorIs(t, err, ErrCeilingReached)
			require.ElementsMatch(t, tc.created, result.Created)
			var names []string
			for _, label := range tc.created {
				names = append(names, label+".tdpl.example")
			}
			require.ElementsMatch(t, names, postNames(fake.snapshot()))
			require.ElementsMatch(t, tc.deferred, result.Deferred)
			require.Equal(t, 10, result.DelegatedRecords)
			require.Equal(t, 10, result.Ceiling)
			require.Len(t, delegationLogLines(log.String(), "ERROR", "ceiling=10", fmt.Sprintf("deferred=%d", tc.count)), 1)
			require.Empty(t, delegationLogLines(log.String(), "WARN", "ceiling=10"))
		})
	}
}

func TestReconcileWarnsNearCeilingWithoutQualifyingLabelsLeft(t *testing.T) {
	for _, tc := range []struct {
		name                string
		ceiling, existing   int
		qualifying, pending bool
		wantCount           int
	}{
		{name: "full with pending only", ceiling: 10, existing: 10, pending: true, wantCount: 10},
		{name: "create fills ceiling and leaves pending", ceiling: 10, existing: 9, qualifying: true, pending: true, wantCount: 10},
		{name: "create reaches eighty percent", ceiling: 10, existing: 7, qualifying: true, wantCount: 8},
		{name: "below eighty percent with pending", ceiling: 10, existing: 7, pending: true},
		{name: "two of three is below eighty percent", ceiling: 3, existing: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeCloudflareAPI(t, configuredFakeRecords(tc.existing, 0))
			fake.maxNSRecords = tc.ceiling
			source := fakeLabelSource{contributions: map[string][]store.Contribution{}}
			if tc.qualifying {
				source.labels = append(source.labels, store.InstanceLabel{Label: "qualified", HasLiveActor: true})
				source.contributions["qualified"] = []store.Contribution{testContribution("qualified", "social.coves.community.comment", delegationTestNow.Add(-2*time.Hour), 1)}
			}
			if tc.pending {
				source.labels = append(source.labels, store.InstanceLabel{Label: "pending", HasLiveActor: true})
				source.contributions["pending"] = []store.Contribution{testContribution("pending", "social.coves.community.comment", delegationTestNow.Add(-10*time.Minute), 2)}
			}
			r, log := newTestReconciler(t, fake, source)
			result, err := r.Reconcile(context.Background())
			require.NoError(t, err)
			if tc.qualifying {
				require.ElementsMatch(t, []string{"qualified"}, result.Created)
				require.ElementsMatch(t, []string{"qualified.tdpl.example"}, postNames(fake.snapshot()))
			} else {
				require.Empty(t, result.Created)
				require.Empty(t, postNames(fake.snapshot()))
			}
			if tc.pending {
				require.ElementsMatch(t, []string{"pending"}, result.Pending)
			} else {
				require.Empty(t, result.Pending)
			}
			require.Empty(t, result.Deferred)
			require.Empty(t, delegationLogLines(log.String(), "ERROR", fmt.Sprintf("ceiling=%d", tc.ceiling)))
			if tc.wantCount > 0 {
				require.Len(t, delegationLogLines(log.String(), "WARN", fmt.Sprintf("records=%d", tc.wantCount), fmt.Sprintf("ceiling=%d", tc.ceiling)), 1)
			} else {
				require.Empty(t, delegationLogLines(log.String(), "WARN", fmt.Sprintf("ceiling=%d", tc.ceiling)))
			}
			require.Equal(t, tc.existing+len(result.Created), result.DelegatedRecords)
			require.Equal(t, tc.ceiling, result.Ceiling)
		})
	}
}

func TestReconcileCeilingTakesPrecedenceOverExhaustedBudget(t *testing.T) {
	fake := newFakeCloudflareAPI(t, configuredFakeRecords(25, 20))
	fake.maxNSRecords = 25
	r, log := newTestReconciler(t, fake, fakeLabelSource{labels: []store.InstanceLabel{{Label: "qualified", HasLiveActor: true}}})
	result, err := r.Reconcile(context.Background())
	require.ErrorIs(t, err, ErrCeilingReached)
	require.Empty(t, postNames(fake.snapshot()))
	require.Empty(t, result.Created)
	require.ElementsMatch(t, []string{"qualified"}, result.Deferred)
	require.Equal(t, 25, result.DelegatedRecords)
	require.Equal(t, 25, result.Ceiling)
	require.Len(t, delegationLogLines(log.String(), "ERROR", "ceiling=25", "deferred=1"), 1)
	for _, line := range delegationLogLines(log.String(), "WARN", "deferred=1") {
		require.NotRegexp(t, `(^| )ceiling=25( |$)`, line)
	}
}

func TestReconcileFailedCreateDoesNotConsumeCeiling(t *testing.T) {
	fake := newFakeCloudflareAPI(t, configuredFakeRecords(8, 0))
	fake.maxNSRecords = 10
	fake.failPostLabels = map[string]bool{"zulu": true}
	r, log := newTestReconciler(t, fake, fakeLabelSource{
		labels: []store.InstanceLabel{{Label: "alpha", HasLiveActor: true}, {Label: "middle", HasLiveActor: true}, {Label: "zulu", HasLiveActor: true}},
		contributions: map[string][]store.Contribution{
			"zulu":   {testContribution("zulu", "social.coves.community.comment", delegationTestNow.Add(-5*time.Hour), 1)},
			"middle": {testContribution("middle", "social.coves.community.comment", delegationTestNow.Add(-4*time.Hour), 2)},
			"alpha":  {testContribution("alpha", "social.coves.community.comment", delegationTestNow.Add(-3*time.Hour), 3)},
		},
	})
	result, err := r.Reconcile(context.Background())
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrCeilingReached)
	require.ElementsMatch(t, []string{"zulu.tdpl.example", "middle.tdpl.example", "alpha.tdpl.example"}, postNames(fake.snapshot()))
	require.Len(t, result.Failed, 1)
	require.Equal(t, "zulu", result.Failed[0].Label)
	require.ElementsMatch(t, []string{"middle", "alpha"}, result.Created)
	require.Empty(t, result.Deferred)
	require.Equal(t, 10, result.DelegatedRecords)
	require.Equal(t, 10, result.Ceiling)
	require.Empty(t, delegationLogLines(log.String(), "ERROR", "ceiling=10"))
}

func TestReconcileRebuildsHourlyBudgetFromListedRecords(t *testing.T) {
	for _, tc := range []struct {
		name             string
		records          []fakeDNSRecord
		created          []string
		deferred         []string
		delegatedRecords int
		budgetWarning    int
	}{
		{name: "a recent twenty exhaust budget", records: configuredFakeRecords(20, 20), deferred: []string{"zulu", "middle", "alpha"}, delegatedRecords: 20, budgetWarning: 3},
		{name: "b records aged sixty-one minutes do not spend budget", records: func() []fakeDNSRecord {
			records := configuredFakeRecords(20, 0)
			for index := range records {
				records[index].CreatedOn = delegationTestNow.Add(-61 * time.Minute).Format(cloudflareTimestampLayout)
			}
			return records
		}(), created: []string{"zulu", "middle", "alpha"}, delegatedRecords: 23},
		{name: "c eighteen recent records leave two requests", records: configuredFakeRecords(20, 18), created: []string{"zulu", "middle"}, deferred: []string{"alpha"}, delegatedRecords: 22, budgetWarning: 1},
		{name: "d root and deeper records spend budget", records: func() []fakeDNSRecord {
			records := configuredFakeRecords(20, 20)
			records[18].Name = "tdpl.example"
			records[19].Name = "x.nested.tdpl.example"
			return records
		}(), deferred: []string{"zulu", "middle", "alpha"}, delegatedRecords: 20, budgetWarning: 3},
		{name: "e foreign records do not spend budget", records: append(configuredFakeRecords(18, 18),
			fakeDNSRecord{Type: "NS", Name: "foreign1.tdpl.example", Content: "other-provider.example", CreatedOn: delegationTestNow.Add(-30 * time.Minute).Format(cloudflareTimestampLayout)},
			fakeDNSRecord{Type: "NS", Name: "foreign2.tdpl.example", Content: "other-provider.example", CreatedOn: delegationTestNow.Add(-30 * time.Minute).Format(cloudflareTimestampLayout)},
			fakeDNSRecord{Type: "NS", Name: "foreign3.tdpl.example", Content: "other-provider.example", CreatedOn: delegationTestNow.Add(-30 * time.Minute).Format(cloudflareTimestampLayout)},
		), created: []string{"zulu", "middle"}, deferred: []string{"alpha"}, delegatedRecords: 20, budgetWarning: 1},
		{name: "f records exactly sixty minutes old do not spend budget", records: func() []fakeDNSRecord {
			records := configuredFakeRecords(20, 0)
			for index := range records {
				records[index].CreatedOn = delegationTestNow.Add(-60 * time.Minute).Format(cloudflareTimestampLayout)
			}
			return records
		}(), created: []string{"zulu", "middle", "alpha"}, delegatedRecords: 23},
		{name: "g more than twenty recent records floor budget at zero", records: configuredFakeRecords(25, 25), deferred: []string{"zulu", "middle", "alpha"}, delegatedRecords: 25, budgetWarning: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeCloudflareAPI(t, tc.records)
			r, log := newTestReconciler(t, fake, fakeLabelSource{
				labels: []store.InstanceLabel{{Label: "alpha", HasLiveActor: true}, {Label: "middle", HasLiveActor: true}, {Label: "zulu", HasLiveActor: true}},
				contributions: map[string][]store.Contribution{
					"zulu":   {testContribution("zulu", "social.coves.community.comment", delegationTestNow.Add(-5*time.Hour), 1)},
					"middle": {testContribution("middle", "social.coves.community.comment", delegationTestNow.Add(-4*time.Hour), 2)},
					"alpha":  {testContribution("alpha", "social.coves.community.comment", delegationTestNow.Add(-3*time.Hour), 3)},
				},
			})
			result, err := r.Reconcile(context.Background())
			require.NoError(t, err)
			var names []string
			for _, label := range tc.created {
				names = append(names, label+".tdpl.example")
			}
			require.ElementsMatch(t, names, postNames(fake.snapshot()))
			require.ElementsMatch(t, tc.created, result.Created)
			require.ElementsMatch(t, tc.deferred, result.Deferred)
			require.Equal(t, tc.delegatedRecords, result.DelegatedRecords)
			require.Equal(t, 1000, result.Ceiling)
			if tc.budgetWarning > 0 {
				lines := delegationLogLines(log.String(), "WARN", fmt.Sprintf("deferred=%d", tc.budgetWarning))
				require.Len(t, lines, 1)
				for _, line := range lines {
					require.NotRegexp(t, `(^| )ceiling=`, line)
				}
			} else {
				require.Empty(t, delegationLogLines(log.String(), "WARN", "deferred=3"))
				require.Empty(t, delegationLogLines(log.String(), "WARN", "deferred=1"))
			}
		})
	}
}

func TestReconcileFailedRequestsSpendHourlyBudget(t *testing.T) {
	fake := newFakeCloudflareAPI(t, configuredFakeRecords(15, 15))
	fake.failPostLabels = map[string]bool{"zulu": true, "yankee": true, "xray": true}
	source := fakeLabelSource{contributions: make(map[string][]store.Contribution)}
	ages := map[string]int{"zulu": 8, "yankee": 7, "xray": 6, "whiskey": 5, "victor": 4, "uniform": 3, "tango": 2, "sierra": 1}
	for index, label := range []string{"sierra", "uniform", "whiskey", "zulu", "tango", "victor", "xray", "yankee"} {
		source.labels = append(source.labels, store.InstanceLabel{Label: label, HasLiveActor: true})
		// Qualification ages deliberately disagree with the source and alphabetical orders.
		source.contributions[label] = []store.Contribution{testContribution(label, "social.coves.community.comment", delegationTestNow.Add(-time.Duration(ages[label])*time.Hour), int64(index+1))}
	}
	r, log := newTestReconciler(t, fake, source)
	result, err := r.Reconcile(context.Background())
	require.Error(t, err)
	require.ElementsMatch(t, []string{"zulu.tdpl.example", "yankee.tdpl.example", "xray.tdpl.example", "whiskey.tdpl.example", "victor.tdpl.example"}, postNames(fake.snapshot()))
	require.Len(t, requestsByMethod(fake.snapshot(), http.MethodPost), 5)
	require.Len(t, result.Failed, 3)
	require.ElementsMatch(t, []string{"zulu", "yankee", "xray"}, []string{result.Failed[0].Label, result.Failed[1].Label, result.Failed[2].Label})
	require.ElementsMatch(t, []string{"whiskey", "victor"}, result.Created)
	require.ElementsMatch(t, []string{"uniform", "tango", "sierra"}, result.Deferred)
	require.Len(t, delegationLogLines(log.String(), "WARN", "deferred=3"), 1)
}

func TestReconcileSpreadsCreatesAcrossRollingHour(t *testing.T) {
	current := delegationTestNow
	fake := newFakeCloudflareAPI(t, nil)
	fake.now = func() time.Time { return current }
	source := &fakeLabelSource{
		contributions:  make(map[string][]store.Contribution),
		outrightActors: make(map[string][]fakeOutrightActor),
	}
	// Reverse-numbered names put age in the opposite order from alphabetical
	// order. The source permutation is neither alphabetical nor chronological.
	for index := range 25 {
		label := fmt.Sprintf("old%02d", 25-index)
		source.labels = append(source.labels, store.InstanceLabel{Label: fmt.Sprintf("old%02d", 25-(index*7)%25), HasLiveActor: true})
		qualifiedAt := delegationTestNow.Add(time.Duration(-120+index) * time.Minute)
		if index == 2 || index == 7 || index == 12 || index == 18 || index == 23 {
			source.outrightActors[label] = []fakeOutrightActor{{createdAt: qualifiedAt, followedCommunity: true}}
		} else {
			source.contributions[label] = []store.Contribution{testContribution(label, "social.coves.community.comment", qualifiedAt, int64(index+1))}
		}
	}
	r, log := newTestReconcilerWithSource(t, fake, source)
	first, err := r.Reconcile(context.Background())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{
		"old25.tdpl.example", "old24.tdpl.example", "old23.tdpl.example", "old22.tdpl.example", "old21.tdpl.example",
		"old20.tdpl.example", "old19.tdpl.example", "old18.tdpl.example", "old17.tdpl.example", "old16.tdpl.example",
		"old15.tdpl.example", "old14.tdpl.example", "old13.tdpl.example", "old12.tdpl.example", "old11.tdpl.example",
		"old10.tdpl.example", "old09.tdpl.example", "old08.tdpl.example", "old07.tdpl.example", "old06.tdpl.example",
	}, postNames(fake.snapshot()))
	require.ElementsMatch(t, []string{"old05", "old04", "old03", "old02", "old01"}, first.Deferred)
	require.Len(t, first.Created, 20)
	require.Len(t, delegationLogLines(log.String(), "WARN", "deferred=5"), 1)

	current = delegationTestNow.Add(59 * time.Minute)
	second, err := r.Reconcile(context.Background())
	require.NoError(t, err)
	require.Len(t, postNames(fake.snapshot()), 20, "no creates while the first pass is inside the rolling hour")
	require.Empty(t, second.Created)
	require.ElementsMatch(t, []string{"old05", "old04", "old03", "old02", "old01"}, second.Deferred)

	// Every new qualification predates the pass-3 cutoff but follows the five
	// deferred qualifications. new24 and new23 tie within the first fifteen.
	for index := range 30 {
		label := fmt.Sprintf("new%02d", 30-index)
		source.labels = append(source.labels, store.InstanceLabel{Label: fmt.Sprintf("new%02d", 30-(index*7)%30), HasLiveActor: true})
		ageIndex := index
		if index == 7 {
			ageIndex = 6
		}
		source.contributions[label] = []store.Contribution{testContribution(label, "social.coves.community.comment", delegationTestNow.Add(time.Duration(-90+ageIndex)*time.Minute), int64(100+index))}
	}
	current = delegationTestNow.Add(61 * time.Minute)
	third, err := r.Reconcile(context.Background())
	require.NoError(t, err)
	allPosts := postNames(fake.snapshot())
	require.Len(t, allPosts, 40, "the third pass must issue exactly twenty new requests")
	require.Equal(t, []string{
		"old05.tdpl.example", "old04.tdpl.example", "old03.tdpl.example", "old02.tdpl.example", "old01.tdpl.example",
		"new30.tdpl.example", "new29.tdpl.example", "new28.tdpl.example", "new27.tdpl.example", "new26.tdpl.example",
		"new25.tdpl.example", "new23.tdpl.example", "new24.tdpl.example", "new22.tdpl.example", "new21.tdpl.example",
		"new20.tdpl.example", "new19.tdpl.example", "new18.tdpl.example", "new17.tdpl.example", "new16.tdpl.example",
	}, allPosts[20:])
	require.ElementsMatch(t, []string{
		"new15", "new14", "new13", "new12", "new11", "new10", "new09", "new08", "new07", "new06",
		"new05", "new04", "new03", "new02", "new01",
	}, third.Deferred)
	require.Len(t, third.Created, 20)
}

func TestNewReconcilerRequiresPositiveMaxNSRecords(t *testing.T) {
	for _, tc := range []struct {
		name string
		max  int
	}{{name: "zero", max: 0}, {name: "negative", max: -1}} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeCloudflareAPI(t, nil)
			client, err := NewCloudflareClient(CloudflareOptions{
				BaseURL: fake.server.URL + "/client/v4", Token: delegationToken, ZoneID: delegationZoneID,
				HTTPClient: ap.NewGuardedHTTPClient(true, 5*time.Second),
			})
			require.NoError(t, err)
			source := &fakeLabelSource{contributions: make(map[string][]store.Contribution)}
			_, err = NewReconciler(Options{
				Client: client, Labels: source, Contributions: source, Acceptances: fakeAcceptanceChecker{},
				ZoneRoot: delegationZoneRoot, Nameservers: []string{"ns1.tdpl.example", "ns2.tdpl.example"},
				Logger: slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)), Now: func() time.Time { return delegationTestNow },
				MaxNSRecords: tc.max,
			})
			require.Error(t, err)
		})
	}
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
