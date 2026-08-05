package ingest

import (
	"bytes"
	"context"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base32"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/identity"
	"tidepool/internal/materialize"
	"tidepool/internal/repo"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// Fixture ids (internal/ap/testdata, captured off live Lemmy).
const (
	groupID     = "https://lemmy.world/c/technology"
	personID    = "https://lemmy.world/u/LeftLeaningFreedomFighters"
	pageID      = "https://lemmy.world/post/49131386"
	noteID      = "https://lemmy.zip/comment/27485395"
	noteAuthor  = "https://lemmy.zip/u/tixooo"
	parentNote  = "https://sh.itjust.works/comment/26248018"
	parentActor = "https://sh.itjust.works/u/DemandtheOxfordComma"

	bridgeHost     = "bridge.test"
	testServiceDID = "did:web:bridge.test"
	testAdminToken = "test-admin-token"
)

// testKEK seals test signing keys.
var testKEK = []byte("0123456789abcdef0123456789abcdef")

// pngBytes is a valid 1x1 transparent PNG (blob fetches).
var pngBytes = []byte{
	0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d,
	0x49, 0x48, 0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
	0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4, 0x89, 0x00, 0x00, 0x00,
	0x0d, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9c, 0x62, 0x00, 0x01, 0x00, 0x00,
	0x05, 0x00, 0x01, 0x0d, 0x0a, 0x2d, 0xb4, 0x00, 0x00, 0x00, 0x00, 0x49,
	0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
}

var jpegBytes = []byte{0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10, 0x4a, 0x46, 0x49, 0x46, 0x00, 0xff, 0xd9}

// rewriteTransport routes every outbound request to the local fixture
// server, preserving the original URL via the Host header (same trick as
// the materialize tests): fixture JSON keeps its real fediverse ids while
// tests never touch the network.
type rewriteTransport struct{ target string }

func (rt rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.Host = req.URL.Host
	clone.URL.Scheme = "http"
	clone.URL.Host = rt.target
	return http.DefaultTransport.RoundTrip(clone)
}

// fakeMinter mints deterministic local identities (no PLC directory).
type fakeMinter struct {
	custodian *identity.Custodian
	mu        sync.Mutex
	mints     int
}

func (f *fakeMinter) MintActor(_ context.Context, req identity.MintRequest) (*identity.Identity, error) {
	f.mu.Lock()
	f.mints++
	f.mu.Unlock()
	key, err := atcrypto.GeneratePrivateKeyK256()
	if err != nil {
		return nil, err
	}
	pub, err := key.PublicKey()
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256([]byte(req.PreferredUsername + "@" + req.Instance))
	did := "did:plc:" + strings.ToLower(base32.StdEncoding.EncodeToString(sum[:]))[:24]
	sealed, err := f.custodian.EncryptActorKey(did, key)
	if err != nil {
		return nil, err
	}
	handle := strings.ToLower(req.PreferredUsername) + "." +
		strings.ReplaceAll(req.Instance, ".", "-") + "." + bridgeHost
	return &identity.Identity{
		DID: did, Handle: handle, DIDKey: pub.DIDKey(), SigningKeyEncrypted: sealed,
	}, nil
}

// testDIDFor mirrors fakeMinter's derivation for assertions.
func testDIDFor(username, instance string) string {
	sum := sha256.Sum256([]byte(username + "@" + instance))
	return "did:plc:" + strings.ToLower(base32.StdEncoding.EncodeToString(sum[:]))[:24]
}

// remoteActor is a fake fediverse actor that can sign deliveries to the
// bridge's inbox.
type remoteActor struct {
	id  string
	key *rsa.PrivateKey
}

func (a *remoteActor) signer() *ap.Signer { return ap.NewSigner(a.id+"#main-key", a.key) }

// recordingBackfill captures follow-accepted triggers.
type recordingBackfill struct {
	mu       sync.Mutex
	triggers []string
}

func (r *recordingBackfill) TriggerAsync(c *store.Community, _ bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.triggers = append(r.triggers, c.APGroupID)
}

func (r *recordingBackfill) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.triggers)
}

// recordingVotes captures vote-aggregator hand-offs.
type recordingVotes struct {
	mu        sync.Mutex
	applied   []string
	retracted []string
}

func (v *recordingVotes) ApplyVote(_ context.Context, vote *ap.Object, _ string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.applied = append(v.applied, vote.Type+" "+refID(vote.Object))
	return nil
}

func (v *recordingVotes) RetractVote(_ context.Context, vote *ap.Object, _ string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.retracted = append(v.retracted, vote.Type+" "+refID(vote.Object))
	return nil
}

type harness struct {
	t *testing.T

	router      chi.Router
	queue       *Queue
	handler     *Handler
	inbox       *Inbox
	admin       *Admin
	client      *ap.Client
	mat         *materialize.Materializer
	manager     *repo.Manager
	minter      *fakeMinter
	service     *ap.ServiceActor
	objects     store.APObjects
	actors      store.BridgedActors
	communities store.Communities
	tombstones  store.Tombstones
	events      store.InboxEvents
	backfills   *recordingBackfill
	votes       *recordingVotes

	mux      *http.ServeMux
	fixtures *httptest.Server
	mu       sync.Mutex
	served   map[string][]byte
	hits     map[string]int
	inboxLog [][]byte // POSTs captured at the fake Lemmy shared inbox
}

// serviceRSAKey is generated once: the bridge service actor's key is not
// under test and RSA keygen is the slowest part of harness setup.
var (
	serviceRSAKey     *rsa.PrivateKey
	serviceRSAKeyOnce sync.Once
)

func testServiceActor(t *testing.T) *ap.ServiceActor {
	t.Helper()
	serviceRSAKeyOnce.Do(func() {
		key, err := ap.GenerateRSAKey()
		require.NoError(t, err)
		serviceRSAKey = key
	})
	return &ap.ServiceActor{
		ID:       "https://" + bridgeHost + "/actor",
		Hostname: bridgeHost,
		Key:      serviceRSAKey,
	}
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database,
		"ap_objects", "bridged_actors", "communities", "inbox_events",
		"ap_tombstones", "blocks", "repo_state", "firehose_events", "blobs")

	custodian, err := identity.NewCustodian(testKEK)
	require.NoError(t, err)
	actors := store.NewBridgedActors(database)
	objects := store.NewAPObjects(database)
	communities := store.NewCommunities(database)
	tombstones := store.NewTombstones(database)
	events := store.NewInboxEvents(database)
	manager, err := repo.NewManager(database, identity.NewActorKeys(actors, custodian), nil)
	require.NoError(t, err)

	h := &harness{
		t:           t,
		objects:     objects,
		actors:      actors,
		communities: communities,
		tombstones:  tombstones,
		events:      events,
		manager:     manager,
		served:      map[string][]byte{},
		hits:        map[string]int{},
	}
	h.mux = http.NewServeMux()
	h.fixtures = httptest.NewServer(h.mux)
	t.Cleanup(h.fixtures.Close)

	// The fake Lemmy shared inbox: captures the bridge's outbound
	// deliveries (Follow, Undo{Follow}).
	h.mux.HandleFunc("POST /inbox", func(w http.ResponseWriter, r *http.Request) {
		body, _ := readAll(r)
		h.mu.Lock()
		h.inboxLog = append(h.inboxLog, body)
		h.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	h.mux.HandleFunc("/pictrs/image/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".jpeg") || strings.HasSuffix(r.URL.Path, ".jpg") {
			w.Header().Set("Content-Type", "image/jpeg")
			_, _ = w.Write(jpegBytes)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(pngBytes)
	})

	h.service = testServiceActor(t)
	h.client = ap.NewClient(ap.ClientOptions{
		HTTPClient:            &http.Client{Transport: rewriteTransport{target: h.fixtures.Listener.Addr().String()}},
		Signer:                h.service.Signer(),
		AllowPrivateAddresses: true,
		PerHostRPS:            100000,
		PerHostBurst:          100000,
		MaxAttempts:           1,
	})

	h.minter = &fakeMinter{custodian: custodian}
	h.mat, err = materialize.New(materialize.Options{
		Fetcher:          h.client,
		Objects:          objects,
		Actors:           actors,
		Communities:      communities,
		Repos:            manager,
		Minter:           h.minter,
		ServiceDID:       testServiceDID,
		StrictValidation: true,
	})
	require.NoError(t, err)

	h.backfills = &recordingBackfill{}
	h.votes = &recordingVotes{}
	h.handler, err = NewHandler(HandlerOptions{
		Materializer:   h.mat,
		Fetcher:        h.client,
		Objects:        objects,
		Actors:         actors,
		Communities:    communities,
		Tombstones:     tombstones,
		Records:        manager,
		Votes:          h.votes,
		Backfill:       h.backfills,
		ServiceActorID: h.service.ID,
	})
	require.NoError(t, err)

	h.queue, err = NewQueue(QueueOptions{
		Events:      events,
		Processor:   h.handler,
		Workers:     1,
		MaxAttempts: 3,
		Lease:       time.Minute,
	})
	require.NoError(t, err)

	h.inbox, err = NewInbox(InboxOptions{
		Verifier: ap.NewVerifier(h.client),
		Events:   events,
		Queue:    h.queue,
		Service:  h.service,
		Fetcher:  h.client,
	})
	require.NoError(t, err)

	h.admin, err = NewAdmin(AdminOptions{
		Token:        testAdminToken,
		Client:       h.client,
		Materializer: h.mat,
		Communities:  communities,
		Service:      h.service,
		Backfill:     h.backfills,
		Sweeper:      h.handler,
	})
	require.NoError(t, err)

	router := chi.NewRouter()
	h.inbox.Routes(router)
	h.admin.Routes(router)
	h.router = router
	return h
}

func readAll(r *http.Request) ([]byte, error) {
	defer func() { _ = r.Body.Close() }()
	var buf bytes.Buffer
	_, err := buf.ReadFrom(r.Body)
	return buf.Bytes(), err
}

// serveJSON serves (or re-serves) body at path, counting hits.
func (h *harness) serveJSON(path string, body []byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, registered := h.served[path]; !registered {
		h.mux.HandleFunc("GET "+path, func(w http.ResponseWriter, r *http.Request) {
			h.mu.Lock()
			current := h.served[path]
			h.hits[path]++
			h.mu.Unlock()
			w.Header().Set("Content-Type", ap.ContentTypeActivityJSON)
			_, _ = w.Write(current)
		})
	}
	h.served[path] = body
}

func (h *harness) serveObject(path string, obj map[string]any) {
	h.t.Helper()
	body, err := json.Marshal(obj)
	require.NoError(h.t, err)
	h.serveJSON(path, body)
}

func (h *harness) hitCount(path string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.hits[path]
}

// loadFixture parses a testdata fixture into a Go map for tweaking.
func loadFixture(t *testing.T, file string) map[string]any {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "ap", "testdata", file))
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(body, &doc))
	return doc
}

// urlPath extracts the path of an absolute AP id.
func urlPath(t *testing.T, id string) string {
	t.Helper()
	idx := strings.Index(id, "//")
	require.GreaterOrEqual(t, idx, 0)
	rest := id[idx+2:]
	slash := strings.IndexByte(rest, '/')
	require.Greater(t, slash, 0)
	return rest[slash:]
}

// newRemoteActor generates an RSA key for an actor, serves its document
// (synthetic or fixture-derived) with the matching public key, and returns
// the signing handle for deliveries.
func (h *harness) newRemoteActor(id string, doc map[string]any) *remoteActor {
	h.t.Helper()
	key, err := ap.GenerateRSAKey()
	require.NoError(h.t, err)
	h.serveActorDoc(id, doc, &key.PublicKey)
	return &remoteActor{id: id, key: key}
}

// serveActorDoc publishes an actor document whose publicKey is pub.
func (h *harness) serveActorDoc(id string, doc map[string]any, pub *rsa.PublicKey) {
	h.t.Helper()
	publicPEM, err := ap.EncodePublicKeyPEM(pub)
	require.NoError(h.t, err)
	doc["publicKey"] = map[string]any{
		"id":           id + "#main-key",
		"owner":        id,
		"publicKeyPem": string(publicPEM),
	}
	h.serveObject(urlPath(h.t, id), doc)
}

// person builds a minimal synthetic Person document.
func person(id, username string, extra map[string]any) map[string]any {
	doc := map[string]any{
		"type":              "Person",
		"id":                id,
		"preferredUsername": username,
		"inbox":             id + "/inbox",
		"published":         "2024-01-01T00:00:00.000000Z",
	}
	for k, v := range extra {
		doc[k] = v
	}
	return doc
}

// note builds a minimal synthetic Lemmy comment.
func note(id, author, inReplyTo, markdown, published string) map[string]any {
	return map[string]any{
		"type":         "Note",
		"id":           id,
		"attributedTo": author,
		"to":           []any{ap.PublicAudience},
		"audience":     groupID,
		"content":      "<p>" + markdown + "</p>",
		"source":       map[string]any{"content": markdown, "mediaType": "text/markdown"},
		"published":    published,
		"inReplyTo":    inReplyTo,
	}
}

// technologyGroup returns the lemmy.world group fixture as a remote actor
// (its published RSA key replaced with a locally generated one so the actor
// can sign deliveries).
func (h *harness) technologyGroup() *remoteActor {
	h.t.Helper()
	doc := loadFixture(h.t, "group_lemmy_world.json")
	return h.newRemoteActor(groupID, doc)
}

// serveLemmyWorldContent registers the standard page/person fixtures.
func (h *harness) serveLemmyWorldContent() {
	h.t.Helper()
	page, err := os.ReadFile(filepath.Join("..", "ap", "testdata", "page_lemmy_world.json"))
	require.NoError(h.t, err)
	h.serveJSON("/post/49131386", page)
	personDoc, err := os.ReadFile(filepath.Join("..", "ap", "testdata", "person_lemmy_world.json"))
	require.NoError(h.t, err)
	h.serveJSON("/u/LeftLeaningFreedomFighters", personDoc)
}

// bridgePost materializes one post under an ap id the caller owns, so the
// test controls what that id's origin path serves. serveJSON's fixture
// handlers always answer 200 and cannot be re-registered, but the
// interesting authorization cases (404, 500, a redirect) are all non-200 —
// and the announce's embedded Page is same-authority, so bridging it needs
// no fetch of the post path at all.
func (h *harness) bridgePost(group *remoteActor, slug string) string {
	h.t.Helper()
	postID := "https://lemmy.world/post/" + slug
	announce := loadFixture(h.t, "announce_create_page_lemmy_world.json")
	announce["id"] = "https://lemmy.world/activities/announce/create/" + slug
	create := announce["object"].(map[string]any)
	create["id"] = "https://lemmy.world/activities/create/" + slug
	create["object"].(map[string]any)["id"] = postID
	require.Equal(h.t, http.StatusAccepted, h.deliver(group, announce))
	h.drain()
	mapping, err := h.objects.GetByAPID(context.Background(), postID)
	require.NoError(h.t, err)
	require.False(h.t, mapping.IsDeleted())
	return postID
}

// deliver signs and posts an activity to the bridge inbox, returning the
// HTTP status.
func (h *harness) deliver(actor *remoteActor, activity map[string]any) int {
	h.t.Helper()
	return h.deliverTo("/inbox", actor, activity)
}

func (h *harness) deliverTo(path string, actor *remoteActor, activity map[string]any) int {
	h.t.Helper()
	body, err := json.Marshal(activity)
	require.NoError(h.t, err)
	req := httptest.NewRequest(http.MethodPost, "https://"+bridgeHost+path, bytes.NewReader(body))
	req.Header.Set("Content-Type", ap.ContentTypeActivityJSON)
	require.NoError(h.t, actor.signer().SignRequest(req, body))
	rec := httptest.NewRecorder()
	h.router.ServeHTTP(rec, req)
	return rec.Code
}

// drain synchronously processes queued events until the queue idles.
func (h *harness) drain() {
	h.t.Helper()
	for h.queue.processNext(context.Background()) {
	}
}

// adminRequest performs an authenticated admin API call.
func (h *harness) adminRequest(method, path string, body map[string]any) *httptest.ResponseRecorder {
	h.t.Helper()
	var reader *bytes.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		require.NoError(h.t, err)
		reader = bytes.NewReader(raw)
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, "https://"+bridgeHost+path, reader)
	req.Header.Set("Authorization", "Bearer "+testAdminToken)
	rec := httptest.NewRecorder()
	h.router.ServeHTTP(rec, req)
	return rec
}

// subscribeTechnology drives the whole follow lifecycle with the fixture
// community: admin subscribe → Follow captured → Accept delivered →
// follow_state accepted. Returns the community's signing handle.
func (h *harness) subscribeTechnology() *remoteActor {
	h.t.Helper()
	group := h.technologyGroup()
	webfinger, err := os.ReadFile(filepath.Join("..", "ap", "testdata", "webfinger_group.json"))
	require.NoError(h.t, err)
	h.serveJSON("/.well-known/webfinger", webfinger)

	rec := h.adminRequest(http.MethodPost, "/admin/communities",
		map[string]any{"community": "!technology@lemmy.world"})
	require.Equal(h.t, http.StatusAccepted, rec.Code, rec.Body.String())

	// The bridge delivered a Follow to the fake Lemmy inbox.
	h.mu.Lock()
	require.NotEmpty(h.t, h.inboxLog, "subscribe must deliver a Follow")
	followRaw := h.inboxLog[len(h.inboxLog)-1]
	h.mu.Unlock()
	follow, err := ap.ParseObject(followRaw)
	require.NoError(h.t, err)
	require.Equal(h.t, ap.TypeFollow, follow.Type)
	require.Equal(h.t, h.service.ID, follow.Actor.ID)
	require.Equal(h.t, groupID, follow.Object.ID)
	require.True(h.t, strings.HasPrefix(follow.ID, "https://"+bridgeHost+"/activities/follow/"),
		"follow activity id %q must live under the bridge's BaseURL", follow.ID)

	// Lemmy answers with Accept{Follow}, signed by the community.
	status := h.deliver(group, map[string]any{
		"id":     "https://lemmy.world/activities/accept/follow-1",
		"type":   "Accept",
		"actor":  groupID,
		"object": map[string]any{"id": follow.ID, "type": "Follow", "actor": h.service.ID, "object": groupID},
	})
	require.Equal(h.t, http.StatusAccepted, status)
	h.drain()

	community, err := h.communities.GetByAPGroupID(context.Background(), groupID)
	require.NoError(h.t, err)
	require.Equal(h.t, store.FollowStateAccepted, community.FollowState)
	return group
}

// firehoseOps flattens all firehose event op paths.
func (h *harness) firehoseOps() []string {
	h.t.Helper()
	events, err := h.manager.ListEvents(context.Background(), 0, 1000)
	require.NoError(h.t, err)
	var paths []string
	for _, evt := range events {
		for _, op := range evt.Ops {
			paths = append(paths, op.Path)
		}
	}
	return paths
}
