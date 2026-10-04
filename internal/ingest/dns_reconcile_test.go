package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/delegation"
	"tidepool/internal/store"
)

type dnsTestRecord struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Name      string `json:"name"`
	Content   string `json:"content"`
	CreatedOn string `json:"created_on"`
}

type dnsTestRequest struct {
	Method string
	Path   string
	Body   map[string]any
}

type dnsTestCloudflare struct {
	server       *httptest.Server
	mu           sync.Mutex
	records      []dnsTestRecord
	requests     []dnsTestRequest
	failListing  bool
	failCreation bool
	// listingDelay holds the zone listing GET before it answers, so a pass
	// can outlast a server's write timeout.
	listingDelay time.Duration
}

func newDNSTestCloudflare(t *testing.T) *dnsTestCloudflare {
	t.Helper()
	fake := &dnsTestCloudflare{}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.serveHTTP))
	t.Cleanup(fake.server.Close)
	return fake
}

func (fake *dnsTestCloudflare) serveHTTP(w http.ResponseWriter, r *http.Request) {
	request := dnsTestRequest{Method: r.Method, Path: r.URL.Path}
	if r.Method == http.MethodPost {
		if err := json.NewDecoder(r.Body).Decode(&request.Body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	fake.mu.Lock()
	fake.requests = append(fake.requests, request)
	records := append([]dnsTestRecord(nil), fake.records...)
	failListing, failCreation, listingDelay := fake.failListing, fake.failCreation, fake.listingDelay
	fake.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if r.URL.Path != "/client/v4/zones/zone-ingest/dns_records" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	switch r.Method {
	case http.MethodGet:
		time.Sleep(listingDelay)
		if failListing {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"success":false,"errors":[{"message":"dns-listing-intentional-failure"}],"result":null}`))
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true, "errors": []any{}, "result": records,
			"result_info": map[string]int{"page": 1, "total_pages": 1},
		})
	case http.MethodPost:
		if failCreation {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"success":false,"errors":[{"message":"dns-create-intentional-failure"}],"result":null}`))
			return
		}
		_, _ = w.Write([]byte(`{"success":true,"errors":[],"result":{"id":"new-record"}}`))
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (fake *dnsTestCloudflare) snapshot() []dnsTestRequest {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return append([]dnsTestRequest(nil), fake.requests...)
}

type dnsTestAcceptance struct{}

func (dnsTestAcceptance) AcceptanceStands(context.Context, string, string) (bool, error) {
	return true, nil
}

func configureDNSTestReconciler(t *testing.T, h *harness, fake *dnsTestCloudflare, ceiling int) {
	t.Helper()
	client, err := delegation.NewCloudflareClient(delegation.CloudflareOptions{
		BaseURL: fake.server.URL + "/client/v4", Token: "cf-ingest-test-token", ZoneID: "zone-ingest",
		HTTPClient: ap.NewGuardedHTTPClient(true, 5*time.Second),
	})
	require.NoError(t, err)
	// Both the reconciler and the admin API log into h.logs, so a test sees
	// every line a forced pass writes, whichever component writes it.
	logger := slog.New(slog.NewTextHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	reconciler, err := delegation.NewReconciler(delegation.Options{
		Client: client, Labels: h.actors, Contributions: h.actors, Acceptances: dnsTestAcceptance{},
		ZoneRoot: "tdpl.example", Nameservers: []string{"ns1.tdpl.example", "ns2.tdpl.example"},
		MaxNSRecords: ceiling, Now: func() time.Time { return time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC) },
		Logger: logger,
	})
	require.NoError(t, err)
	h.admin.logger = logger
	h.admin.SetDNSReconciler(reconciler)
}

// dnsTestLogLines returns the captured log lines whose message is message.
func dnsTestLogLines(h *harness, message string) []string {
	var lines []string
	for _, line := range strings.Split(h.logs.String(), "\n") {
		if strings.Contains(line, `msg="`+message+`"`) {
			lines = append(lines, line)
		}
	}
	return lines
}

func seedDNSTestActor(t *testing.T, h *harness, label string, createdAt string, live bool) {
	t.Helper()
	did := "did:plc:ingestdnstest" + label + "00000000000000001"
	actorID := "https://lemmy.example/u/dns-" + label
	_, err := h.actors.UpsertActor(context.Background(), store.BridgedActor{
		APActorID: actorID, ActorType: store.ActorTypePerson, DID: did,
		Handle: "alice." + label + ".tdpl.example", ConsentState: store.ConsentStateOK,
	})
	require.NoError(t, err)
	_, err = h.db.ExecContext(context.Background(), `UPDATE bridged_actors SET created_at = $2 WHERE did = $1`, did, createdAt)
	require.NoError(t, err)
	if !live {
		require.NoError(t, h.actors.SetConsentState(context.Background(), actorID, store.ConsentStateDeleted))
	}
}

func dnsTestNS(label, content string) dnsTestRecord {
	return dnsTestRecord{
		ID: "record-" + label, Type: "NS", Name: label + ".tdpl.example", Content: content,
		CreatedOn: "2026-10-04T00:00:00Z",
	}
}

func dnsTestJSON(t *testing.T, rec *httptest.ResponseRecorder, status int) map[string]any {
	t.Helper()
	require.Equal(t, status, rec.Code, rec.Body.String())
	require.Equal(t, "application/json", strings.Split(rec.Header().Get("Content-Type"), ";")[0])
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	return body
}

func TestDNSReconcileRequiresAdminBearer(t *testing.T) {
	h := newHarness(t)
	fake := newDNSTestCloudflare(t)
	configureDNSTestReconciler(t, h, fake, 7)
	for _, tc := range []struct {
		name, authorization string
	}{{"no bearer", ""}, {"wrong bearer", "Bearer wrong-token"}} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "https://"+bridgeHost+"/admin/dns/reconcile", nil)
			if tc.authorization != "" {
				req.Header.Set("Authorization", tc.authorization)
			}
			rec := httptest.NewRecorder()
			h.router.ServeHTTP(rec, req)
			assert.Equal(t, http.StatusUnauthorized, rec.Code)
			assert.Empty(t, fake.snapshot(), "unauthenticated requests must not reach Cloudflare")
		})
	}
}

func TestDNSReconcileAdminReportsDelegation(t *testing.T) {
	h := newHarness(t)
	fake := newDNSTestCloudflare(t)
	fake.records = []dnsTestRecord{
		dnsTestNS("c", "ns1.tdpl.example"), dnsTestNS("d", "foreign.ns.example"),
		dnsTestNS("e", "ns2.tdpl.example"),
	}
	seedDNSTestActor(t, h, "a", "2026-10-02T00:00:00Z", true)
	seedDNSTestActor(t, h, "b", "2026-10-04T00:00:00Z", true)
	seedDNSTestActor(t, h, "c", "2026-10-04T00:00:00Z", true)
	seedDNSTestActor(t, h, "d", "2026-10-04T00:00:00Z", true)
	seedDNSTestActor(t, h, "e", "2026-10-04T00:00:00Z", false)
	configureDNSTestReconciler(t, h, fake, 7)

	body := dnsTestJSON(t, h.adminRequest(http.MethodPost, "/admin/dns/reconcile", nil), http.StatusOK)
	assert.Equal(t, []any{"a"}, body["created"])
	assert.Equal(t, []any{"b"}, body["pending"])
	assert.Equal(t, []any{"c"}, body["already_delegated"])
	assert.Equal(t, []any{map[string]any{"label": "d", "contents": []any{"foreign.ns.example"}}}, body["conflicting"])
	assert.Equal(t, []any{"e"}, body["delegated_without_live_actors"])
	assert.Equal(t, float64(3), body["delegated_records"])
	assert.Equal(t, float64(7), body["ceiling"])
	summaries := dnsTestLogLines(h, "delegation pass finished")
	require.Len(t, summaries, 1, "a forced pass logs its summary once: %s", h.logs.String())
	assert.Contains(t, summaries[0], "level=INFO")
	assert.Contains(t, summaries[0], " component=delegation ")
	assert.Contains(t, summaries[0], " created=[a] ")
	assert.Contains(t, summaries[0], " pending=[b] ")
	requests := fake.snapshot()
	require.Len(t, requests, 2, "the POST must complete before the response is returned")
	assert.Equal(t, dnsTestRequest{Method: http.MethodPost, Path: "/client/v4/zones/zone-ingest/dns_records",
		Body: map[string]any{"type": "NS", "name": "a.tdpl.example", "content": "ns1.tdpl.example", "ttl": float64(3600)}}, requests[1])
}

func TestDNSReconcileAdminErrors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		setup   func(*dnsTestCloudflare)
		ceiling int
	}{
		{"listing failure", func(fake *dnsTestCloudflare) { fake.failListing = true }, 7},
		{"ceiling reached", func(fake *dnsTestCloudflare) {
			fake.records = []dnsTestRecord{dnsTestNS("occupied", "ns1.tdpl.example")}
		}, 1},
		{"creation failure", func(fake *dnsTestCloudflare) { fake.failCreation = true }, 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			fake := newDNSTestCloudflare(t)
			tc.setup(fake)
			seedDNSTestActor(t, h, "a", "2026-10-02T00:00:00Z", true)
			if tc.name == "ceiling reached" {
				seedDNSTestActor(t, h, "b", "2026-10-04T00:00:00Z", true)
			}
			configureDNSTestReconciler(t, h, fake, tc.ceiling)
			body := dnsTestJSON(t, h.adminRequest(http.MethodPost, "/admin/dns/reconcile", nil), http.StatusInternalServerError)
			message, ok := body["error"].(string)
			require.True(t, ok, "error must be a JSON string: %v", body)
			result, ok := body["result"].(map[string]any)
			require.True(t, ok, "result must be a JSON object: %v", body)
			switch tc.name {
			case "listing failure":
				assert.Contains(t, message, "dns-listing-intentional-failure")
			case "ceiling reached":
				assert.Contains(t, message, delegation.ErrCeilingReached.Error())
				assert.Equal(t, []any{"a"}, result["deferred"])
				assert.Equal(t, []any{"b"}, result["pending"])
				assert.Equal(t, float64(1), result["ceiling"])
				assert.Equal(t, float64(1), result["delegated_records"])
			case "creation failure":
				assert.Contains(t, message, "dns-create-intentional-failure")
				failures, ok := result["failed"].([]any)
				require.True(t, ok, "failed must be a JSON array: %v", result)
				require.Len(t, failures, 1)
				failure, ok := failures[0].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "a", failure["label"])
				assert.NotEmpty(t, failure["reason"])
			}
			for _, request := range fake.snapshot() {
				if tc.name == "ceiling reached" {
					assert.Equal(t, http.MethodGet, request.Method, "ceiling must prevent every POST")
				}
			}
			summaries := dnsTestLogLines(h, "delegation pass finished")
			require.Len(t, summaries, 1, "a failed forced pass still logs its summary: %s", h.logs.String())
			failures := dnsTestLogLines(h, "delegation pass failed")
			require.Len(t, failures, 1, "a failed forced pass logs its error once: %s", h.logs.String())
			assert.Contains(t, failures[0], "level=ERROR")
			assert.Contains(t, failures[0], " component=delegation ")
			assert.Empty(t, dnsTestLogLines(h, "DNS delegation reconciliation failed"))
			if tc.name == "ceiling reached" {
				assert.Contains(t, summaries[0], " pending=[b] ")
				assert.Contains(t, summaries[0], " deferred_count=1 ")
			}
			if tc.name == "creation failure" {
				requests := fake.snapshot()
				require.Len(t, requests, 2, "a failed create must reach Cloudflare")
				assert.Equal(t, http.MethodPost, requests[1].Method)
				assert.Equal(t, "a.tdpl.example", requests[1].Body["name"])
			}
		})
	}
}

func TestDNSReconcileAdminCancelledRequestLogsNoError(t *testing.T) {
	h := newHarness(t)
	fake := newDNSTestCloudflare(t)
	seedDNSTestActor(t, h, "a", "2026-10-02T00:00:00Z", true)
	configureDNSTestReconciler(t, h, fake, 7)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodPost, "https://"+bridgeHost+"/admin/dns/reconcile", nil).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+testAdminToken)
	h.router.ServeHTTP(httptest.NewRecorder(), req)
	assert.NotContains(t, h.logs.String(), "level=ERROR", "a pass ended by the request's own cancellation is not a failure")
	assert.Empty(t, fake.snapshot(), "a cancelled request must not reach Cloudflare")
}

func TestDNSReconcileAdminOutlastsServerWriteTimeout(t *testing.T) {
	h := newHarness(t)
	fake := newDNSTestCloudflare(t)
	fake.listingDelay = 600 * time.Millisecond
	seedDNSTestActor(t, h, "a", "2026-10-02T00:00:00Z", true)
	configureDNSTestReconciler(t, h, fake, 7)
	server := httptest.NewUnstartedServer(h.router)
	server.Config.WriteTimeout = 200 * time.Millisecond
	server.Start()
	t.Cleanup(server.Close)

	req, err := http.NewRequest(http.MethodPost, server.URL+"/admin/dns/reconcile", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+testAdminToken)
	resp, err := server.Client().Do(req)
	require.NoError(t, err, "a pass longer than the server's write timeout must still deliver its result")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/json", strings.Split(resp.Header.Get("Content-Type"), ";")[0])
	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, []any{"a"}, body["created"])
}

func TestDNSReconcileAdminUnconfigured(t *testing.T) {
	h := newHarness(t)
	rec := h.adminRequest(http.MethodPost, "/admin/dns/reconcile", nil)
	assert.Equal(t, http.StatusNotImplemented, rec.Code, rec.Body.String())
}
