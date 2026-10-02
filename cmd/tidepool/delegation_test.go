package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
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
	firstGET     chan struct{}
	releaseGET   chan struct{}
	releaseOnce  sync.Once
	post         chan startupCloudflareRequest
	mutex        sync.Mutex
	requests     []startupCloudflareRequest
	unauthorized bool
	blockFirst   bool
}

func newStartupCloudflareAPI(t *testing.T, blockFirst bool) *startupCloudflareAPI {
	t.Helper()
	fake := &startupCloudflareAPI{
		firstGET:   make(chan struct{}),
		releaseGET: make(chan struct{}),
		post:       make(chan startupCloudflareRequest, 1),
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
		_, _ = w.Write([]byte(`{"success":true,"errors":[],"messages":[],"result":[],"result_info":{"page":1,"per_page":2,"count":0,"total_count":0,"total_pages":1}}`))
	case http.MethodPost:
		_, _ = w.Write([]byte(`{"success":true,"errors":[],"messages":[],"result":{"id":"new-record","type":"NS","name":"lemmy-example.tdpl.example","content":"a.ns.example","ttl":3600}}`))
		fake.post <- request
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func startupDelegationConfig(allowPrivate bool) *config.Config {
	return &config.Config{
		BridgeHostname:        "tdpl.example",
		DNSListen:             "127.0.0.1:5300",
		DNSNameservers:        []string{"a.ns.example", "b.ns.example"},
		CloudflareAPIToken:    "cf-startup-token",
		CloudflareZoneID:      "zone-startup",
		AllowPrivateAddresses: allowPrivate,
	}
}

func TestStartDelegationStartsBackgroundPass(t *testing.T) {
	conn := testutil.DB(t)
	testutil.Truncate(t, conn, "bridged_actors")
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

	fake := newStartupCloudflareAPI(t, true)
	type startupResult struct {
		reconciler *delegation.Reconciler
		err        error
	}
	returned := make(chan startupResult, 1)
	go func() {
		reconciler, err := startDelegation(ctx, startupDelegationConfig(true), actors,
			fake.server.URL+"/client/v4", testLogger())
		returned <- startupResult{reconciler, err}
	}()
	// Whichever happens first, the returned value must be valid and the GET
	// must start; the GET stays blocked until after both conditions hold.
	returnedAlready := false
	select {
	case result := <-returned:
		require.NoError(t, result.err)
		require.NotNil(t, result.reconciler)
		returnedAlready = true
	case <-fake.firstGET:
	case <-time.After(5 * time.Second):
		t.Fatal("neither startDelegation returned nor the background GET started")
	}
	select {
	case <-fake.firstGET:
	case <-time.After(5 * time.Second):
		t.Fatal("background pass never sent its first listing GET")
	}
	if !returnedAlready {
		select {
		case result := <-returned:
			require.NoError(t, result.err)
			require.NotNil(t, result.reconciler, "startDelegation must return before the listing GET finishes")
		case <-time.After(5 * time.Second):
			t.Fatal("startDelegation blocked on the held listing GET")
		}
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
	reconciler, err := startDelegation(ctx, cfg, nil, fake.server.URL+"/client/v4", testLogger())
	require.NoError(t, err)
	assert.Nil(t, reconciler)
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

func TestStartDelegationGuardsPrivateAddresses(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	fake := newStartupCloudflareAPI(t, false)
	logger, _ := capturingLogger()
	errors := make(chan slog.Record, 1)
	logger = slog.New(startupErrorSignal{Handler: logger.Handler(), errors: errors})
	reconciler, err := startDelegation(ctx, startupDelegationConfig(false), startupLabelSource{},
		fake.server.URL+"/client/v4", logger)
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
