package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/config"
	"tidepool/internal/delegation"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

type startupCloudflareRequest struct {
	Method        string
	Path          string
	Query         string
	Authorization string
	Body          map[string]any
}

type startupCloudflareAPI struct {
	server       *httptest.Server
	records      []startupDNSRecord
	firstGET     chan struct{}
	releaseGET   chan struct{}
	releaseOnce  sync.Once
	post         chan startupCloudflareRequest
	mutex        sync.Mutex
	requests     []startupCloudflareRequest
	unauthorized bool
	blockFirst   bool
}

type startupDNSRecord struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Name      string `json:"name"`
	Content   string `json:"content"`
	CreatedOn string `json:"created_on"`
}

func newStartupCloudflareAPI(t *testing.T, blockFirst bool) *startupCloudflareAPI {
	t.Helper()
	fake := &startupCloudflareAPI{
		firstGET:   make(chan struct{}),
		releaseGET: make(chan struct{}),
		post:       make(chan startupCloudflareRequest, 8),
		blockFirst: blockFirst,
	}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.serveHTTP))
	t.Cleanup(func() {
		fake.release()
		fake.server.Close()
	})
	return fake
}

func (fake *startupCloudflareAPI) release() {
	fake.releaseOnce.Do(func() { close(fake.releaseGET) })
}

func (fake *startupCloudflareAPI) snapshot() ([]startupCloudflareRequest, bool) {
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	return append([]startupCloudflareRequest(nil), fake.requests...), fake.unauthorized
}

func (fake *startupCloudflareAPI) serveHTTP(w http.ResponseWriter, r *http.Request) {
	request := startupCloudflareRequest{
		Method:        r.Method,
		Path:          r.URL.Path,
		Query:         r.URL.RawQuery,
		Authorization: r.Header.Get("Authorization"),
	}
	if r.Method == http.MethodPost {
		if err := json.NewDecoder(r.Body).Decode(&request.Body); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}
	fake.mutex.Lock()
	fake.requests = append(fake.requests, request)
	first := len(fake.requests) == 1 && r.Method == http.MethodGet
	if request.Authorization != "Bearer cf-startup-token" {
		fake.unauthorized = true
	}
	fake.mutex.Unlock()
	if first {
		close(fake.firstGET)
		if fake.blockFirst {
			select {
			case <-fake.releaseGET:
			case <-r.Context().Done():
				return
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if request.Authorization != "Bearer cf-startup-token" {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"success":false,"errors":[{"code":9109,"message":"unauthorized"}],"messages":[],"result":null}`))
		return
	}
	if r.URL.Path != "/client/v4/zones/zone-startup/dns_records" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	switch r.Method {
	case http.MethodGet:
		page, err := strconv.Atoi(r.URL.Query().Get("page"))
		if err != nil || page < 1 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		fake.mutex.Lock()
		records := append([]startupDNSRecord(nil), fake.records...)
		fake.mutex.Unlock()
		const pageSize = 2
		start := min((page-1)*pageSize, len(records))
		end := min(start+pageSize, len(records))
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true, "errors": []any{}, "messages": []any{}, "result": records[start:end],
			"result_info": map[string]int{"page": page, "per_page": pageSize, "count": end - start,
				"total_count": len(records), "total_pages": max(1, (len(records)+pageSize-1)/pageSize)},
		})
	case http.MethodPost:
		_, _ = w.Write([]byte(`{"success":true,"errors":[],"messages":[],"result":{"id":"new-record","type":"NS","name":"lemmy-example.tdpl.example","content":"a.ns.example","ttl":3600}}`))
		fake.post <- request
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func startupDelegationConfig(allowPrivate bool) *config.Config {
	return &config.Config{
		BridgeHostname:         "tdpl.example",
		DNSListen:              "127.0.0.1:5300",
		DNSNameservers:         []string{"a.ns.example", "b.ns.example"},
		CloudflareAPIToken:     "cf-startup-token",
		CloudflareZoneID:       "zone-startup",
		DelegationMaxNSRecords: 100,
		AllowPrivateAddresses:  allowPrivate,
	}
}

type startupAcceptanceChecker struct{}

func (startupAcceptanceChecker) AcceptanceStands(context.Context, string, string) (bool, error) {
	return true, nil
}

type recordingDNSReconcilerSetter struct {
	received []*delegation.Reconciler
}

func (setter *recordingDNSReconcilerSetter) SetDNSReconciler(reconciler *delegation.Reconciler) {
	setter.received = append(setter.received, reconciler)
}

func seedStartupFollowedCommunity(t *testing.T, ctx context.Context, conn *sql.DB, groupID, did string) {
	t.Helper()
	communities := store.NewCommunities(conn)
	_, err := communities.UpsertCommunity(ctx, store.Community{
		APGroupID: groupID, DID: did, PreferredUsername: "followed", Instance: "lemmy.example",
	})
	require.NoError(t, err)
	require.NoError(t, communities.SetFollowState(ctx, groupID, store.FollowStateAccepted))
}

// seedStartupComment seeds a comment the community announced, under a
// standing legacy post root in the community's repo.
func seedStartupComment(t *testing.T, ctx context.Context, conn *sql.DB, actorDID, communityDID, suffix string, indexedAt time.Time) {
	t.Helper()
	objects := store.NewAPObjects(conn)
	root, err := objects.PutMapping(ctx, store.APObjectMapping{
		APID: "https://lemmy.example/post/" + suffix, APType: "Page", OriginInstance: "lemmy.example",
		DID: communityDID, Collection: "social.coves.community.post", RKey: "post-" + suffix,
		CID: "bafyreiaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	})
	require.NoError(t, err)
	_, err = objects.PutMapping(ctx, store.APObjectMapping{
		APID: "https://lemmy.example/comment/" + suffix, APType: "Note", OriginInstance: "lemmy.example",
		DID: actorDID, AuthorDID: actorDID, CommunityDID: communityDID, ThreadRootATURI: root.ATURI,
		Collection: "social.coves.community.comment", RKey: "comment-" + suffix,
		CID: "bafyreiaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	})
	require.NoError(t, err)
	require.NoError(t, objects.MarkCommunityAnnounced(ctx, "https://lemmy.example/comment/"+suffix))
	_, err = conn.ExecContext(ctx, `UPDATE ap_objects SET indexed_at = $2 WHERE ap_id = $1`, "https://lemmy.example/comment/"+suffix, indexedAt)
	require.NoError(t, err)
}

func TestStartDelegationStartsBackgroundPass(t *testing.T) {
	conn := testutil.DB(t)
	testutil.Truncate(t, conn, "ap_objects", "bridged_actors", "communities")
	actors := store.NewBridgedActors(conn)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	_, err := actors.UpsertActor(ctx, store.BridgedActor{
		APActorID:    "https://lemmy.example/u/alice-startup",
		ActorType:    store.ActorTypePerson,
		DID:          "did:plc:cmddelegationstart000000001",
		Handle:       "alice.lemmy-example.tdpl.example",
		ConsentState: store.ConsentStateOK,
	})
	require.NoError(t, err)
	now := time.Now()
	_, err = conn.ExecContext(ctx, `UPDATE bridged_actors SET created_at = $2 WHERE did = $1`, "did:plc:cmddelegationstart000000001", now)
	require.NoError(t, err)
	seedStartupFollowedCommunity(t, ctx, conn, "https://lemmy.example/c/startup", "did:plc:cmddelegationcommunity00001")
	seedStartupComment(t, ctx, conn, "did:plc:cmddelegationstart000000001", "did:plc:cmddelegationcommunity00001", "startup", now.Add(-2*time.Hour))

	fake := newStartupCloudflareAPI(t, true)
	setter := &recordingDNSReconcilerSetter{}
	finished := make(chan slog.Record, 1)
	logger := slog.New(startupPassSignal{Handler: testLogger().Handler(), finished: finished})
	type startupResult struct {
		reconciler *delegation.Reconciler
		err        error
	}
	returned := make(chan startupResult, 1)
	startDone := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		select {
		case <-startDone:
		case <-time.After(5 * time.Second):
			t.Error("startDelegation goroutine did not finish")
		}
	})
	go func() {
		defer close(startDone)
		reconciler, err := startDelegation(ctx, startupDelegationConfig(true), actors, startupAcceptanceChecker{},
			setter, fake.server.URL+"/client/v4", logger)
		returned <- startupResult{reconciler, err}
	}()
	// Whichever happens first, the returned value must be valid and the GET
	// must start; the GET stays blocked until after both conditions hold.
	var reconciler *delegation.Reconciler
	select {
	case result := <-returned:
		require.NoError(t, result.err)
		require.NotNil(t, result.reconciler)
		reconciler = result.reconciler
	case <-fake.firstGET:
	case <-time.After(5 * time.Second):
		t.Fatal("neither startDelegation returned nor the background GET started")
	}
	select {
	case <-fake.firstGET:
	case <-time.After(5 * time.Second):
		t.Fatal("background pass never sent its first listing GET")
	}
	if reconciler == nil {
		select {
		case result := <-returned:
			require.NoError(t, result.err)
			require.NotNil(t, result.reconciler, "startDelegation must return before the listing GET finishes")
			reconciler = result.reconciler
		case <-time.After(5 * time.Second):
			t.Fatal("startDelegation blocked on the held listing GET")
		}
	}
	if assert.Len(t, setter.received, 1, "admin must receive the reconciler exactly once") {
		assert.Same(t, reconciler, setter.received[0], "admin must receive the exact reconciler returned")
	}
	fake.release()
	select {
	case request := <-fake.post:
		assert.Equal(t, http.MethodPost, request.Method)
		assert.Equal(t, "/client/v4/zones/zone-startup/dns_records", request.Path)
		assert.Equal(t, "Bearer cf-startup-token", request.Authorization)
		assert.Equal(t, "NS", request.Body["type"])
		assert.Equal(t, "lemmy-example.tdpl.example", request.Body["name"])
		assert.Equal(t, "a.ns.example", request.Body["content"])
		assert.Equal(t, float64(3600), request.Body["ttl"])
		assert.NotContains(t, request.Body, "proxied")
	case <-time.After(5 * time.Second):
		t.Fatal("background pass did not create the delegation")
	}
	select {
	case record := <-finished:
		assert.Equal(t, slog.LevelInfo, record.Level)
		assert.Equal(t, "delegation pass finished", record.Message)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not log a delegation pass finished summary")
	}
	requests, unauthorized := fake.snapshot()
	assert.False(t, unauthorized, "every Cloudflare request must use the configured bearer token")
	for _, request := range requests {
		assert.Equal(t, "/client/v4/zones/zone-startup/dns_records", request.Path)
		assert.Equal(t, "Bearer cf-startup-token", request.Authorization)
		assert.Contains(t, []string{http.MethodGet, http.MethodPost}, request.Method)
	}
}

func TestStartDelegationDisabledWithoutToken(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	fake := newStartupCloudflareAPI(t, false)
	cfg := startupDelegationConfig(true)
	cfg.CloudflareAPIToken = ""
	setter := &recordingDNSReconcilerSetter{}
	reconciler, err := startDelegation(ctx, cfg, nil, startupAcceptanceChecker{}, setter, fake.server.URL+"/client/v4", testLogger())
	require.NoError(t, err)
	assert.Nil(t, reconciler)
	assert.Empty(t, setter.received, "disabled delegation must not call the admin setter")
	requests, _ := fake.snapshot()
	assert.Empty(t, requests)
}

type startupLabelSource struct {
	store.BridgedActors
}

func (startupLabelSource) ListInstanceLabels(context.Context, string) ([]store.InstanceLabel, error) {
	return []store.InstanceLabel{{Label: "lemmy-example", HasLiveActor: true}}, nil
}

type startupErrorSignal struct {
	slog.Handler
	errors chan slog.Record
}

type startupPassSignal struct {
	slog.Handler
	finished chan slog.Record
}

func (handler startupPassSignal) Handle(ctx context.Context, record slog.Record) error {
	err := handler.Handler.Handle(ctx, record)
	if record.Message == "delegation pass finished" {
		handler.finished <- record.Clone()
	}
	return err
}

func TestStartDelegationQualifiesStartupLabels(t *testing.T) {
	conn := testutil.DB(t)
	testutil.Truncate(t, conn, "ap_objects", "bridged_actors", "communities")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	now := time.Now()
	actors := store.NewBridgedActors(conn)
	for _, actor := range []struct {
		label string
		did   string
		kind  store.ActorType
	}{
		{label: "l", did: "did:plc:cmddelegationlongcontent001", kind: store.ActorTypePerson},
		{label: "m", did: "did:plc:cmddelegationyoungcontent01", kind: store.ActorTypePerson},
		{label: "g", did: "did:plc:cmddelegationgroup00000001", kind: store.ActorTypeGroup},
	} {
		actorID := "https://lemmy.example/u/" + actor.label
		if actor.kind == store.ActorTypeGroup {
			actorID = "https://lemmy.example/c/g"
		}
		_, err := actors.UpsertActor(ctx, store.BridgedActor{
			APActorID: actorID,
			ActorType: actor.kind, DID: actor.did, Handle: "alice." + actor.label + ".tdpl.example", ConsentState: store.ConsentStateOK,
		})
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, `UPDATE bridged_actors SET created_at = $2 WHERE did = $1`, actor.did, now)
		require.NoError(t, err)
	}
	seedStartupFollowedCommunity(t, ctx, conn, "https://lemmy.example/c/qualification", "did:plc:cmddelegationfollowed000001")
	seedStartupFollowedCommunity(t, ctx, conn, "https://lemmy.example/c/g", "did:plc:cmddelegationgroup00000001")
	seedStartupComment(t, ctx, conn, "did:plc:cmddelegationlongcontent001", "did:plc:cmddelegationfollowed000001", "old", now.Add(-2*time.Hour))
	seedStartupComment(t, ctx, conn, "did:plc:cmddelegationyoungcontent01", "did:plc:cmddelegationfollowed000001", "young", now.Add(-10*time.Minute))

	fake := newStartupCloudflareAPI(t, false)
	finished := make(chan slog.Record, 1)
	logger := slog.New(startupPassSignal{Handler: testLogger().Handler(), finished: finished})
	setter := &recordingDNSReconcilerSetter{}
	reconciler, err := startDelegation(ctx, startupDelegationConfig(true), actors, startupAcceptanceChecker{},
		setter, fake.server.URL+"/client/v4", logger)
	require.NoError(t, err)
	require.NotNil(t, reconciler)
	var record slog.Record
	select {
	case record = <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("startup delegation pass did not finish")
	}
	// Drain POST notifications: even a broken pass that creates M must never block the HTTP handler.
	for {
		select {
		case <-fake.post:
		default:
			goto drained
		}
	}
drained:
	requests, unauthorized := fake.snapshot()
	require.False(t, unauthorized)
	var names []string
	for _, request := range requests {
		if request.Method == http.MethodPost {
			names = append(names, request.Body["name"].(string))
		}
	}
	require.ElementsMatch(t, []string{"g.tdpl.example", "l.tdpl.example"}, names)
	pendingCount, ok := attr(record, "pending_count")
	require.True(t, ok, "startup pass should log pending count")
	require.Equal(t, int64(1), pendingCount.Int64())
	pending, ok := attr(record, "pending")
	require.True(t, ok, "startup pass should log pending labels")
	require.Equal(t, []string{"m"}, pending.Any())
}

func (handler startupErrorSignal) Handle(ctx context.Context, record slog.Record) error {
	err := handler.Handler.Handle(ctx, record)
	if record.Level == slog.LevelError {
		select {
		case handler.errors <- record.Clone():
		default:
		}
	}
	return err
}

type startupCeilingSignal struct {
	slog.Handler
	finished chan slog.Record
	errors   chan slog.Record
}

func (handler startupCeilingSignal) Handle(ctx context.Context, record slog.Record) error {
	err := handler.Handler.Handle(ctx, record)
	if record.Level == slog.LevelError {
		select {
		case handler.errors <- record.Clone():
		default:
		}
	}
	if record.Message == "delegation pass finished" {
		handler.finished <- record.Clone()
	}
	return err
}

func TestStartDelegationHonorsConfiguredCeiling(t *testing.T) {
	conn := testutil.DB(t)
	testutil.Truncate(t, conn, "ap_objects", "bridged_actors", "communities")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	now := time.Now()
	actors := store.NewBridgedActors(conn)
	_, err := actors.UpsertActor(ctx, store.BridgedActor{
		APActorID: "https://lemmy.example/u/ceiling-startup", ActorType: store.ActorTypePerson,
		DID: "did:plc:cmddelegationceiling000001", Handle: "ceiling.lemmy-example.tdpl.example", ConsentState: store.ConsentStateOK,
	})
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, `UPDATE bridged_actors SET created_at = $2 WHERE did = $1`, "did:plc:cmddelegationceiling000001", now)
	require.NoError(t, err)
	seedStartupFollowedCommunity(t, ctx, conn, "https://lemmy.example/c/ceiling", "did:plc:cmddelegationceilinggroup001")
	seedStartupComment(t, ctx, conn, "did:plc:cmddelegationceiling000001", "did:plc:cmddelegationceilinggroup001", "ceiling-startup", now.Add(-2*time.Hour))

	fake := newStartupCloudflareAPI(t, false)
	fake.records = []startupDNSRecord{
		{ID: "existing-1", Type: "NS", Name: "existing-1.tdpl.example", Content: "a.ns.example", CreatedOn: now.Add(-3 * time.Hour).Format("2006-01-02T15:04:05.000000Z07:00")},
		{ID: "existing-2", Type: "NS", Name: "existing-2.tdpl.example", Content: "a.ns.example", CreatedOn: now.Add(-3 * time.Hour).Format("2006-01-02T15:04:05.000000Z07:00")},
	}
	finished := make(chan slog.Record, 1)
	errors := make(chan slog.Record, 16)
	logger := slog.New(startupCeilingSignal{Handler: testLogger().Handler(), finished: finished, errors: errors})
	cfg := startupDelegationConfig(true)
	cfg.DelegationMaxNSRecords = 2
	setter := &recordingDNSReconcilerSetter{}
	reconciler, err := startDelegation(ctx, cfg, actors, startupAcceptanceChecker{}, setter, fake.server.URL+"/client/v4", logger)
	require.NoError(t, err)
	require.NotNil(t, reconciler)
	var summary slog.Record
	select {
	case summary = <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("startup delegation pass did not finish")
	}
	requests, unauthorized := fake.snapshot()
	require.False(t, unauthorized)
	require.NotEmpty(t, requests, "startup must list Cloudflare records")
	for _, request := range requests {
		require.Equal(t, http.MethodGet, request.Method, "the full ceiling must prevent every POST")
	}
	// The reconciler logs its ceiling Error before the summary, so it is already buffered.
	ceilingLogged := false
	for !ceilingLogged {
		select {
		case record := <-errors:
			if value, ok := attr(record, "ceiling"); ok && value.Int64() == 2 {
				ceilingLogged = true
			}
		default:
			t.Fatal("startup pass did not log an Error with ceiling=2 before its summary")
		}
	}
	for _, expected := range []struct {
		name  string
		value int64
	}{{"deferred_count", 1}, {"delegated_records", 2}, {"ceiling", 2}} {
		value, ok := attr(summary, expected.name)
		require.True(t, ok, "startup summary must carry %s", expected.name)
		require.Equal(t, expected.value, value.Int64(), expected.name)
	}
}

func TestStartDelegationGuardsPrivateAddresses(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	fake := newStartupCloudflareAPI(t, false)
	logger, _ := capturingLogger()
	errors := make(chan slog.Record, 1)
	logger = slog.New(startupErrorSignal{Handler: logger.Handler(), errors: errors})
	setter := &recordingDNSReconcilerSetter{}
	reconciler, err := startDelegation(ctx, startupDelegationConfig(false), startupLabelSource{}, startupAcceptanceChecker{},
		setter, fake.server.URL+"/client/v4", logger)
	require.NoError(t, err)
	require.NotNil(t, reconciler)
	select {
	case record := <-errors:
		assert.Equal(t, slog.LevelError, record.Level)
	case <-time.After(5 * time.Second):
		t.Fatal("background pass did not log its guarded-client failure")
	}
	requests, _ := fake.snapshot()
	assert.Empty(t, requests, "the guarded client must refuse loopback before reaching Cloudflare")
}
