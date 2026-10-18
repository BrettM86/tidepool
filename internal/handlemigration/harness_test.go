package handlemigration

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/identity"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

type plcRequest struct {
	Method string
	Path   string
	Body   map[string]any
	// Arrived is the fixture clock's time when the request arrived.
	Arrived time.Time
}

type plcFixture struct {
	mutex              sync.Mutex
	requests           []plcRequest
	onArrival          func(plcRequest) error
	onRequest          func(plcRequest, *http.Request) error
	hookErrors         []error
	postStatus         int
	postStatuses       map[string]int
	postStatusSequence map[string][]int
	postCounts         map[string]int
	postBodies         map[string]string
	landPosts          map[string]bool
	getStatusAfterPost map[string]int
	getStatusSequence  map[string][]int
	getBodies          map[string]string
	heads              map[string][]map[string]any
	getCounts          map[string]int
	last               map[string]any
	// now stamps each request's arrival; the harness sets the virtual clock.
	now func() time.Time
	// postHeaders and getHeaders are set on every POST and /log/last
	// response for a DID path.
	postHeaders map[string]map[string]string
	getHeaders  map[string]map[string]string
}

func (p *plcFixture) serve(w http.ResponseWriter, r *http.Request) {
	request := plcRequest{Method: r.Method, Path: r.URL.Path}
	if r.Method == http.MethodPost {
		if err := json.NewDecoder(r.Body).Decode(&request.Body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	p.mutex.Lock()
	if p.now != nil {
		request.Arrived = p.now()
	}
	p.requests = append(p.requests, request)
	hook := p.onArrival
	requestHook := p.onRequest
	status := p.postStatus
	if selected := p.postStatuses[r.URL.Path]; selected != 0 {
		status = selected
	}
	postBody := p.postBodies[r.URL.Path]
	if r.Method == http.MethodPost {
		if p.postCounts == nil {
			p.postCounts = make(map[string]int)
		}
		index := p.postCounts[r.URL.Path]
		p.postCounts[r.URL.Path]++
		if sequence := p.postStatusSequence[r.URL.Path]; len(sequence) > 0 {
			status = sequence[min(index, len(sequence)-1)]
		}
	}
	p.mutex.Unlock()
	if hook != nil {
		if err := hook(request); err != nil {
			p.mutex.Lock()
			p.hookErrors = append(p.hookErrors, err)
			p.mutex.Unlock()
		}
	}
	if requestHook != nil {
		if err := requestHook(request, r); err != nil {
			p.mutex.Lock()
			p.hookErrors = append(p.hookErrors, err)
			p.mutex.Unlock()
		}
	}
	if r.Method == http.MethodGet && len(r.URL.Path) > len("/log/last") &&
		r.URL.Path[len(r.URL.Path)-len("/log/last"):] == "/log/last" {
		path := r.URL.Path[:len(r.URL.Path)-len("/log/last")]
		p.mutex.Lock()
		if p.getCounts == nil {
			p.getCounts = make(map[string]int)
		}
		index := p.getCounts[path]
		p.getCounts[path]++
		head := p.last
		if sequence := p.heads[path]; len(sequence) > 0 {
			head = sequence[min(index, len(sequence)-1)]
		}
		getStatus := 0
		if sequence := p.getStatusSequence[path]; len(sequence) > 0 {
			getStatus = sequence[min(index, len(sequence)-1)]
		}
		getBody := p.getBodies[path]
		for name, value := range p.getHeaders[path] {
			w.Header().Set(name, value)
		}
		if p.getStatusAfterPost[path] != 0 {
			for _, prior := range p.requests[:len(p.requests)-1] {
				if prior.Method == http.MethodPost && prior.Path == path {
					getStatus = p.getStatusAfterPost[path]
					break
				}
			}
		}
		p.mutex.Unlock()
		if getStatus != 0 && getStatus != http.StatusOK {
			w.WriteHeader(getStatus)
			_, _ = w.Write([]byte(getBody))
			return
		}
		_ = json.NewEncoder(w).Encode(head)
		return
	}
	if r.Method == http.MethodPost {
		if status == 0 {
			status = http.StatusOK
		}
		p.mutex.Lock()
		if status >= 200 && status < 300 || p.landPosts[r.URL.Path] {
			if p.heads == nil {
				p.heads = make(map[string][]map[string]any)
			}
			p.heads[r.URL.Path] = []map[string]any{request.Body}
		}
		for name, value := range p.postHeaders[r.URL.Path] {
			w.Header().Set(name, value)
		}
		p.mutex.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(postBody))
		return
	}
	http.NotFound(w, r)
}

func (p *plcFixture) snapshot() ([]plcRequest, []error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	return append([]plcRequest(nil), p.requests...), append([]error(nil), p.hookErrors...)
}

type actorRequest struct {
	Method string
	Path   string
}

type actorFixture struct {
	mutex      sync.Mutex
	requests   []actorRequest
	handlers   map[string]http.HandlerFunc
	onArrival  func(actorRequest) error
	hookErrors []error
}

func (f *actorFixture) serve(w http.ResponseWriter, r *http.Request) {
	request := actorRequest{Method: r.Method, Path: r.URL.Path}
	f.mutex.Lock()
	f.requests = append(f.requests, request)
	hook := f.onArrival
	handler := f.handlers[r.URL.Path]
	f.mutex.Unlock()
	if hook != nil {
		if err := hook(request); err != nil {
			f.mutex.Lock()
			f.hookErrors = append(f.hookErrors, err)
			f.mutex.Unlock()
		}
	}
	if handler == nil {
		http.NotFound(w, r)
		return
	}
	handler(w, r)
}

func (f *actorFixture) snapshot() ([]actorRequest, []error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return append([]actorRequest(nil), f.requests...), append([]error(nil), f.hookErrors...)
}

type rewriteTransport struct{ target string }

type virtualClock struct {
	mutex sync.Mutex
	now   time.Time
}

func newVirtualClock() *virtualClock {
	return &virtualClock{now: time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)}
}

func (c *virtualClock) Now() time.Time {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.now
}

func (c *virtualClock) Advance(delay time.Duration) {
	c.mutex.Lock()
	c.now = c.now.Add(delay)
	c.mutex.Unlock()
}

func (c *virtualClock) Wait(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.Advance(delay)
	return nil
}

func (rt rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.Host = req.URL.Host
	clone.URL.Scheme = "http"
	clone.URL.Host = rt.target
	return http.DefaultTransport.RoundTrip(clone)
}

type renameHarness struct {
	db          *sql.DB
	actors      store.BridgedActors
	actorDocs   *actorFixture
	actorTarget string
	actorClient *ap.Client
	logger      *slog.Logger
	minter      *identity.Minter
	runner      *Runner
	plc         *plcFixture
	rotationKey *atcrypto.PrivateKeyK256
	clock       *virtualClock
	retryMutex  sync.Mutex
	retryDelays []time.Duration
}

func (h *renameHarness) actorFetcher(allowPrivateAddresses bool) *ap.Client {
	return ap.NewClient(ap.ClientOptions{
		HTTPClient:            &http.Client{Transport: rewriteTransport{target: h.actorTarget}, Timeout: 2 * time.Second},
		AllowPrivateAddresses: allowPrivateAddresses, MaxAttempts: 1,
	})
}

func (h *renameHarness) delays() []time.Duration {
	h.retryMutex.Lock()
	defer h.retryMutex.Unlock()
	return append([]time.Duration(nil), h.retryDelays...)
}

func newRenameHarness(t *testing.T) *renameHarness {
	return newRenameHarnessWithActors(t, nil)
}

func newRenameHarnessWithActors(t *testing.T, wrap func(store.BridgedActors) store.BridgedActors) *renameHarness {
	return newRenameHarnessWithTransport(t, wrap, nil)
}

func newRenameHarnessWithTransport(t *testing.T, wrap func(store.BridgedActors) store.BridgedActors, wrapTransport func(http.RoundTripper) http.RoundTripper) *renameHarness {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database, "bridged_actors", "handle_claims", "firehose_events")
	var runsTable sql.NullString
	require.NoError(t, database.QueryRowContext(t.Context(), `SELECT to_regclass('public.handle_migration_runs')`).Scan(&runsTable))
	if runsTable.Valid {
		testutil.Truncate(t, database, "handle_migration_runs")
	}
	actors := store.NewBridgedActors(database)
	minterActors := actors
	if wrap != nil {
		minterActors = wrap(actors)
	}
	rotationKey, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	publicKey, err := rotationKey.PublicKey()
	require.NoError(t, err)
	last := map[string]any{
		"type": "plc_operation", "rotationKeys": []any{publicKey.DIDKey()},
		"verificationMethods": map[string]any{"atproto": "did:key:zTestSigningKey"},
		"services":            map[string]any{"atproto_pds": map[string]any{"type": "AtprotoPersonalDataServer", "endpoint": "https://tidepool.test"}},
		"alsoKnownAs":         []any{"at://alice.lemmy-world.tidepool.test"}, "prev": nil,
	}
	unsigned, err := atdata.MarshalCBOR(last)
	require.NoError(t, err)
	signature, err := rotationKey.HashAndSign(unsigned)
	require.NoError(t, err)
	last["sig"] = base64.RawURLEncoding.EncodeToString(signature)
	plc := &plcFixture{last: last}
	server := httptest.NewServer(http.HandlerFunc(plc.serve))
	t.Cleanup(server.Close)
	actorDocs := &actorFixture{handlers: make(map[string]http.HandlerFunc)}
	actorServer := httptest.NewServer(http.HandlerFunc(actorDocs.serve))
	t.Cleanup(actorServer.Close)
	custodian, err := identity.NewCustodian([]byte("0123456789abcdef0123456789abcdef"))
	require.NoError(t, err)
	client := ap.NewGuardedHTTPClient(true, 10*time.Second)
	if wrapTransport != nil {
		client.Transport = wrapTransport(client.Transport)
	}
	h := &renameHarness{db: database, actors: actors, plc: plc, rotationKey: rotationKey, clock: newVirtualClock(),
		actorDocs: actorDocs, actorTarget: actorServer.Listener.Addr().String()}
	plc.now = h.clock.Now
	h.actorClient = h.actorFetcher(true)
	minter, err := identity.NewMinter(identity.MinterOptions{
		PLCDirectoryURL: server.URL, RotationKey: rotationKey, Custodian: custodian,
		BridgeHostname: "tidepool.test", HandleZoneRoot: "ap.tidepool.test", Actors: minterActors,
		HTTPClient: client, PLCRetryWait: func(ctx context.Context, delay time.Duration) error {
			h.retryMutex.Lock()
			h.retryDelays = append(h.retryDelays, delay)
			h.retryMutex.Unlock()
			return nil
		},
	})
	require.NoError(t, err)
	runner, err := NewRunner(Options{
		Actors: actors, Identity: minter, ActorFetcher: h.actorClient,
		BridgeHostname: "tidepool.test", HandleZoneRoot: "ap.tidepool.test",
		ActorsPerSecond: 2, Now: h.clock.Now, Wait: h.clock.Wait,
	})
	require.NoError(t, err)
	require.NotNil(t, runner)
	h.minter, h.runner = minter, runner
	return h
}

func (h *renameHarness) runnerWithActors(t *testing.T, actors store.BridgedActors) *Runner {
	t.Helper()
	runner, err := NewRunner(Options{Actors: actors, Identity: h.minter, ActorFetcher: h.actorClient,
		BridgeHostname: "tidepool.test", HandleZoneRoot: "ap.tidepool.test", Logger: h.logger,
		ActorsPerSecond: 2, Now: h.clock.Now, Wait: h.clock.Wait})
	require.NoError(t, err)
	require.NotNil(t, runner)
	return runner
}

func (h *renameHarness) insertOldActor(t *testing.T, did, apID, oldHandle, preferredUsername string) *store.BridgedActor {
	t.Helper()
	actor, err := h.actors.UpsertActor(t.Context(), store.BridgedActor{
		APActorID: apID, ActorType: store.ActorTypePerson, DID: did, Handle: oldHandle,
		PreferredUsername: preferredUsername, ConsentState: store.ConsentStateOK,
	})
	require.NoError(t, err)
	require.NotNil(t, actor)
	return actor
}

type renameRow struct {
	Handle           string
	PendingHandle    string
	PreviousHandle   string
	HandleMigratedAt *time.Time
}

func (h *renameHarness) row(t *testing.T, did string) renameRow {
	t.Helper()
	var row renameRow
	err := h.db.QueryRowContext(t.Context(), `SELECT COALESCE(handle, ''), COALESCE(pending_handle, ''),
		COALESCE(previous_handle, ''), handle_migrated_at FROM bridged_actors WHERE did = $1`, did).
		Scan(&row.Handle, &row.PendingHandle, &row.PreviousHandle, &row.HandleMigratedAt)
	require.NoError(t, err)
	return row
}

func (h *renameHarness) claim(t *testing.T, handle string) (int64, bool) {
	t.Helper()
	var actorID int64
	err := h.db.QueryRowContext(t.Context(), `SELECT actor_id FROM handle_claims WHERE handle = $1`, handle).Scan(&actorID)
	if err == sql.ErrNoRows {
		return 0, false
	}
	require.NoError(t, err)
	return actorID, true
}

func (h *renameHarness) identityHandles(t *testing.T, did string) []string {
	t.Helper()
	rows, err := h.db.QueryContext(t.Context(), `SELECT identity_handle FROM firehose_events WHERE kind = 'identity' AND did = $1 ORDER BY seq`, did)
	require.NoError(t, err)
	defer rows.Close()
	var handles []string
	for rows.Next() {
		var handle string
		require.NoError(t, rows.Scan(&handle))
		handles = append(handles, handle)
	}
	require.NoError(t, rows.Err())
	return handles
}

// The HTTP hook reads through a separate database connection, proving the
// reservation was committed before any PLC request leaves the runner.
func (h *renameHarness) reservationAtPLC(did, handle string, actorID int64) {
	h.reservationAtPLCWithResolution(did, handle, actorID, nil)
}

// Check committed claims and, for rollback, both names at the first PLC read.
func (h *renameHarness) reservationAtPLCWithResolution(did, handle string, actorID int64, resolving []string) {
	h.plc.mutex.Lock()
	h.plc.onArrival = func(request plcRequest) error {
		if request.Path != "/"+did && request.Path != "/"+did+"/log/last" {
			return nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		var pending string
		var owner int64
		err := h.db.QueryRowContext(ctx, `SELECT COALESCE(pending_handle, '') FROM bridged_actors WHERE did = $1`, did).Scan(&pending)
		if err != nil {
			return err
		}
		if pending != handle {
			return fmt.Errorf("pending handle at PLC: got %q, want %q", pending, handle)
		}
		err = h.db.QueryRowContext(ctx, `SELECT actor_id FROM handle_claims WHERE handle = $1`, handle).Scan(&owner)
		if err != nil {
			return err
		}
		if owner != actorID {
			return fmt.Errorf("claim owner at PLC: got %d, want %d", owner, actorID)
		}
		if request.Method == http.MethodGet && len(resolving) != 0 {
			resolver := identity.NewStoreResolver(h.actors, "tidepool.test", "did:web:tidepool.test")
			for _, candidate := range resolving {
				resolved, err := resolver.ResolveHandle(ctx, candidate)
				if err != nil {
					return fmt.Errorf("resolve %q at PLC: %w", candidate, err)
				}
				if resolved != did {
					return fmt.Errorf("resolve %q at PLC: got %q, want %q", candidate, resolved, did)
				}
			}
		}
		return nil
	}
	h.plc.mutex.Unlock()
}
