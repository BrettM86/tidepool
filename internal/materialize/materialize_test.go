package materialize

import (
	"context"
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
	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/identity"
	"tidepool/internal/repo"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// Fixture object ids (from internal/ap/testdata, captured off live Lemmy).
const (
	pageID   = "https://lemmy.world/post/49131386"
	personID = "https://lemmy.world/u/LeftLeaningFreedomFighters"
	groupID  = "https://lemmy.world/c/technology"
	noteID   = "https://lemmy.zip/comment/27485395"

	testServiceDID = "did:web:tidepool.test"
)

// testKEK is a fixed 32-byte key for sealing test signing keys.
var testKEK = []byte("0123456789abcdef0123456789abcdef")

// pngBytes is a valid 1x1 transparent PNG; fixed bytes keep blob CIDs (and
// therefore golden records) deterministic.
var pngBytes = []byte{
	0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d,
	0x49, 0x48, 0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
	0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4, 0x89, 0x00, 0x00, 0x00,
	0x0d, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9c, 0x62, 0x00, 0x01, 0x00, 0x00,
	0x05, 0x00, 0x01, 0x0d, 0x0a, 0x2d, 0xb4, 0x00, 0x00, 0x00, 0x00, 0x49,
	0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
}

// jpegBytes is a minimal JPEG header; the handler labels it image/jpeg.
var jpegBytes = []byte{0xff, 0xd8, 0xff, 0xe0, 0x00, 0x10, 0x4a, 0x46, 0x49, 0x46, 0x00, 0xff, 0xd9}

// rewriteTransport routes every outbound request — whatever its host — to
// the local fixture server, preserving the original URL for handler
// dispatch via the Host header. Fixture JSON keeps its real lemmy.world ids
// (which keeps handles, provenance lines, and golden files deterministic)
// while tests never touch the network.
type rewriteTransport struct{ target string }

func (rt rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	clone.Host = req.URL.Host
	clone.URL.Scheme = "http"
	clone.URL.Host = rt.target
	return http.DefaultTransport.RoundTrip(clone)
}

// fakeMinter mints deterministic did:plc identities locally (no PLC
// directory): the DID is a hash of user@instance, the signing key is real
// so repo commits sign and verify. It counts every mint, so a test can
// prove a path minted nothing (a PLC DID, once minted, is permanent).
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
		strings.ReplaceAll(req.Instance, ".", "-") + ".tidepool.test"
	return &identity.Identity{
		DID:                 did,
		Handle:              handle,
		DIDKey:              pub.DIDKey(),
		SigningKeyEncrypted: sealed,
	}, nil
}

// mintCount returns how many identities the fake minter has minted so far.
func (f *fakeMinter) mintCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mints
}

// testDIDFor mirrors fakeMinter's DID derivation for assertions.
func testDIDFor(username, instance string) string {
	sum := sha256.Sum256([]byte(username + "@" + instance))
	return "did:plc:" + strings.ToLower(base32.StdEncoding.EncodeToString(sum[:]))[:24]
}

type harness struct {
	t           *testing.T
	m           *Materializer
	manager     *repo.Manager
	objects     store.APObjects
	actors      store.BridgedActors
	communities store.Communities
	minter      *fakeMinter
	mux         *http.ServeMux
	fixtures    *httptest.Server
	scrubbed    *recordingScrubber
	// hits counts GETs per served path (see hitCount).
	hitsMu sync.Mutex
	hits   map[string]int
	// requests logs every request the fixture server received, matched or
	// not (see fixtureRequests); nodeInfo maps a Host to its nodeinfo
	// fixture (see serveNodeInfo).
	requestsMu sync.Mutex
	requests   []*fixtureRequest
	nodeInfoMu sync.Mutex
	nodeInfo   map[string]http.Handler
}

// fixtureRequest is one request seen at the fixture server boundary.
// Status stays 0 while the handler is still running.
type fixtureRequest struct {
	Host, Path string
	Status     int
}

// statusRecorder captures the status a fixture handler wrote.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.status == 0 {
		s.status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.status == 0 {
		s.status = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

// withNodeInfoDiscoveryDeadline sets the materializer's nodeinfo deadline.
func withNodeInfoDiscoveryDeadline(d time.Duration) func(*Options) {
	return func(o *Options) { o.NodeInfoDiscoveryDeadline = d }
}

// recordingScrubber records ScrubVoter calls (the task-11 vote-scrub hook).
type recordingScrubber struct {
	mu  sync.Mutex
	ids []string
}

func (r *recordingScrubber) ScrubVoter(_ context.Context, voterAPID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ids = append(r.ids, voterAPID)
	return nil
}

func (r *recordingScrubber) calls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ids...)
}

// newHarness wires the full materialization stack over the real test
// database and a local fixture server standing in for the fediverse.
// configure adjusts the materializer options before New.
func newHarness(t *testing.T, configure ...func(*Options)) *harness {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database,
		"ap_objects", "bridged_actors", "communities",
		"blocks", "repo_state", "firehose_events", "blobs", "instances")

	custodian, err := identity.NewCustodian(testKEK)
	require.NoError(t, err)
	actors := store.NewBridgedActors(database)
	objects := store.NewAPObjects(database)
	communities := store.NewCommunities(database)
	manager, err := repo.NewManager(database, identity.NewActorKeys(actors, custodian), nil)
	require.NoError(t, err)

	mux := http.NewServeMux()
	h := &harness{t: t, mux: mux, hits: map[string]int{}, nodeInfo: map[string]http.Handler{}}
	// Every request is logged at the server boundary, including the ones
	// the mux answers with its default 404.
	fixtures := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entry := &fixtureRequest{Host: r.Host, Path: r.URL.Path}
		h.requestsMu.Lock()
		h.requests = append(h.requests, entry)
		h.requestsMu.Unlock()
		recorder := &statusRecorder{ResponseWriter: w}
		mux.ServeHTTP(recorder, r)
		status := recorder.status
		if status == 0 {
			status = http.StatusOK
		}
		h.requestsMu.Lock()
		entry.Status = status
		h.requestsMu.Unlock()
	}))
	t.Cleanup(fixtures.Close)
	h.fixtures = fixtures

	client := ap.NewClient(ap.ClientOptions{
		HTTPClient:            &http.Client{Transport: rewriteTransport{target: fixtures.Listener.Addr().String()}},
		AllowPrivateAddresses: true,
		PerHostRPS:            100000, // deep-chain tests fetch dozens of objects from one "host"
		PerHostBurst:          100000,
		MaxAttempts:           1, // fixture errors are deterministic; retries just slow tests
	})

	scrubbed := &recordingScrubber{}
	minter := &fakeMinter{custodian: custodian}
	options := Options{
		Fetcher:          client,
		Objects:          objects,
		Actors:           actors,
		Communities:      communities,
		Repos:            manager,
		Minter:           minter,
		Votes:            scrubbed,
		ServiceDID:       testServiceDID,
		StrictValidation: true, // tests always validate against the vendored lexicons
		Instances:        store.NewInstances(database),
		// Same rewrite as the AP client: nodeinfo discovery for any host
		// lands on the fixture server, never the network.
		NodeInfoClient: &http.Client{Transport: rewriteTransport{target: fixtures.Listener.Addr().String()}},
		UserAgent:      "tidepool-test/1.0",
	}
	for _, apply := range configure {
		apply(&options)
	}
	m, err := New(options)
	require.NoError(t, err)

	h.m, h.manager = m, manager
	h.objects, h.actors, h.communities, h.minter = objects, actors, communities, minter
	h.scrubbed = scrubbed
	// Nodeinfo fixtures are per Host, but mux patterns are path-only: one
	// dispatcher serves both nodeinfo paths and 404s hosts with no fixture.
	nodeInfoDispatch := func(w http.ResponseWriter, r *http.Request) {
		h.nodeInfoMu.Lock()
		handler, ok := h.nodeInfo[r.Host]
		h.nodeInfoMu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}
		handler.ServeHTTP(w, r)
	}
	mux.HandleFunc("/.well-known/nodeinfo", nodeInfoDispatch)
	mux.HandleFunc(nodeInfoDocumentPath, nodeInfoDispatch)
	// Every pictrs-style image path serves fixed bytes by extension.
	mux.HandleFunc("/pictrs/image/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".jpeg") || strings.HasSuffix(r.URL.Path, ".jpg") {
			w.Header().Set("Content-Type", "image/jpeg")
			_, _ = w.Write(jpegBytes)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(pngBytes)
	})
	return h
}

// serveFixture serves a task-02 fixture file at its object path.
func (h *harness) serveFixture(path, fixtureFile string) {
	h.t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "ap", "testdata", fixtureFile))
	require.NoError(h.t, err)
	h.serveJSON(path, body)
}

// serveObject serves a synthetic AP object (given as a Go map).
func (h *harness) serveObject(path string, obj map[string]any) {
	h.t.Helper()
	body, err := json.Marshal(obj)
	require.NoError(h.t, err)
	h.serveJSON(path, body)
}

func (h *harness) serveJSON(path string, body []byte) {
	h.mux.HandleFunc("GET "+path, func(w http.ResponseWriter, r *http.Request) {
		h.hitsMu.Lock()
		h.hits[path]++
		h.hitsMu.Unlock()
		w.Header().Set("Content-Type", ap.ContentTypeActivityJSON)
		_, _ = w.Write(body)
	})
}

// hitCount is how many times a path registered through serveJSON (and so
// serveObject/serveFixture) has been fetched.
func (h *harness) hitCount(path string) int {
	h.hitsMu.Lock()
	defer h.hitsMu.Unlock()
	return h.hits[path]
}

// nodeInfoDocumentPath is where nodeinfo fixtures serve the linked
// schema-2.0 document.
const nodeInfoDocumentPath = "/nodeinfo/2.0"

// nodeInfoFixture answers nodeinfo discovery the way a Lemmy instance
// does: the well-known listing links a 2.0 document naming software.
func nodeInfoFixture(software string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/.well-known/nodeinfo":
			_ = json.NewEncoder(w).Encode(map[string]any{"links": []any{map[string]any{
				"rel":  "http://nodeinfo.diaspora.software/ns/schema/2.0",
				"href": "https://" + r.Host + nodeInfoDocumentPath,
			}}})
		default:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"version":   "2.0",
				"software":  map[string]any{"name": software, "version": "0.19.0"},
				"protocols": []any{"activitypub"},
			})
		}
	})
}

// serveNodeInfo makes host answer nodeinfo discovery with software,
// replacing any earlier nodeinfo fixture for host.
func (h *harness) serveNodeInfo(host, software string) {
	h.serveNodeInfoHandler(host, nodeInfoFixture(software))
}

// serveNodeInfoHandler routes both nodeinfo paths for host to handler.
func (h *harness) serveNodeInfoHandler(host string, handler http.Handler) {
	h.nodeInfoMu.Lock()
	defer h.nodeInfoMu.Unlock()
	h.nodeInfo[host] = handler
}

// fixtureRequests is every request the fixture server has received so
// far, in arrival order.
func (h *harness) fixtureRequests() []fixtureRequest {
	h.requestsMu.Lock()
	defer h.requestsMu.Unlock()
	out := make([]fixtureRequest, 0, len(h.requests))
	for _, entry := range h.requests {
		out = append(out, *entry)
	}
	return out
}

// requestCount is how many requests for path arrived with Host host.
func (h *harness) requestCount(host, path string) int {
	count := 0
	for _, entry := range h.fixtureRequests() {
		if entry.Host == host && entry.Path == path {
			count++
		}
	}
	return count
}

// nodeInfoRequestCount is how many nodeinfo requests (well-known listing
// plus linked document) arrived for host.
func (h *harness) nodeInfoRequestCount(host string) int {
	return h.requestCount(host, "/.well-known/nodeinfo") + h.requestCount(host, nodeInfoDocumentPath)
}

// serveLemmyWorldFixtures registers the standard page/person/group trio.
func (h *harness) serveLemmyWorldFixtures() {
	h.serveFixture("/post/49131386", "page_lemmy_world.json")
	h.serveFixture("/u/LeftLeaningFreedomFighters", "person_lemmy_world.json")
	h.serveFixture("/c/technology", "group_lemmy_world.json")
}

// loadFixtureObject parses a fixture into an ap.Object (the shape the
// materializer receives from the ingestion layer).
func loadFixtureObject(t *testing.T, fixtureFile string) *ap.Object {
	t.Helper()
	body, err := os.ReadFile(filepath.Join("..", "ap", "testdata", fixtureFile))
	require.NoError(t, err)
	obj, err := ap.ParseObject(body)
	require.NoError(t, err)
	return obj
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

// note builds a minimal synthetic Note (Lemmy comment shape).
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

func (h *harness) firehoseEvents() []*repo.Event {
	h.t.Helper()
	events, err := h.manager.ListEvents(context.Background(), 0, 1000)
	require.NoError(h.t, err)
	return events
}

func countMappings(t *testing.T, h *harness, apID string) int {
	t.Helper()
	if _, err := h.objects.GetByAPID(context.Background(), apID); err != nil {
		if errors.IsNotFound(err) {
			return 0
		}
		t.Fatalf("count mappings: %v", err)
	}
	return 1
}

// TestMaterializePostEndToEnd runs the whole pipeline on the captured
// lemmy.world fixtures: community + author bridged and profiled, post
// committed into the AUTHOR's repo as a postv2 (PLAN.md decision 20 —
// authorship IS the repo, so there is no in-record author), external-link
// embed with a fetched thumbnail blob, mapping row written.
func TestMaterializePostEndToEnd(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	res, err := h.m.MaterializePost(ctx, loadFixtureObject(t, "page_lemmy_world.json"), groupID)
	require.NoError(t, err)
	require.False(t, res.NoOp)

	communityDID := testDIDFor("technology", "lemmy.world")
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
	assert.Equal(t, authorDID, res.DID, "posts live in the AUTHOR's repo")

	// The record satisfies the Coves postv2 consumer's security checks.
	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	assert.Equal(t, authorDID, mapping.DID)
	assert.Equal(t, authorDID, mapping.AuthorDID)
	assert.Equal(t, CollectionPostV2, mapping.Collection)

	record, _, err := h.manager.GetRecord(ctx, authorDID, CollectionPostV2, mapping.RKey)
	require.NoError(t, err)
	assert.Equal(t, communityDID, record["community"], "the post names the community it was submitted to")
	assert.NotContains(t, record, "author",
		"authorship is the repo the record lives in; an author field would invite a consumer to trust it instead")
	assert.Contains(t, record["title"], "DRAM price-fixing")

	// Bridged provenance: who wrote it upstream, and where it came from.
	originalAuthor, ok := record["originalAuthor"].(map[string]any)
	require.True(t, ok, "a bridged postv2 carries originalAuthor")
	assert.Equal(t, personID, originalAuthor["apId"])
	assert.Equal(t, "lemmy.world", originalAuthor["instance"])
	federatedFrom, ok := record["federatedFrom"].(map[string]any)
	require.True(t, ok, "a bridged postv2 carries federatedFrom")
	assert.Equal(t, "lemmy", federatedFrom["platform"])
	assert.Equal(t, "lemmy.world", federatedFrom["instance"])

	embed, ok := record["embed"].(map[string]any)
	require.True(t, ok, "link post must carry an external embed")
	assert.Equal(t, "social.coves.embed.external", embed["$type"])

	// The embed's thumbnail blob lives in the repo holding the record — a
	// blob ref resolves against that repo and nowhere else.
	blobs := atdata.ExtractBlobs(record)
	require.NotEmpty(t, blobs, "the fixture post embeds a thumbnail blob")
	_, _, err = h.manager.GetBlob(ctx, authorDID, cid.Cid(blobs[0].Ref).String())
	require.NoError(t, err, "the post's embed blob is stored under the author's DID")

	// Profiles exist with rkey self, in the right repos.
	_, _, err = h.manager.GetRecord(ctx, communityDID, CollectionCommunityProfile, ProfileRKey)
	require.NoError(t, err)
	profile, _, err := h.manager.GetRecord(ctx, authorDID, CollectionActorProfile, ProfileRKey)
	require.NoError(t, err)
	assert.Contains(t, profile["description"], "bridged from @LeftLeaningFreedomFighters@lemmy.world",
		"actor bios carry the provenance line in the lexicon's description field")
	assert.NotContains(t, profile, "bio",
		"social.coves.actor.profile has no bio property; the AppView reads description")

	// The communities row tracks the bridged group.
	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	assert.Equal(t, communityDID, community.DID)
	assert.Equal(t, "technology", community.PreferredUsername)
}

// TestEmissionOrdering is the DoD guarantee: community profile and author
// profile commits precede the content commit referencing them, asserted on
// firehose_events seq.
func TestEmissionOrdering(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()

	_, err := h.m.MaterializePost(context.Background(), loadFixtureObject(t, "page_lemmy_world.json"), groupID)
	require.NoError(t, err)

	seqOf := func(collection string) int64 {
		for _, evt := range h.firehoseEvents() {
			for _, op := range evt.Ops {
				if strings.HasPrefix(op.Path, collection+"/") {
					return evt.Seq
				}
			}
		}
		t.Fatalf("no firehose event carries a %s op", collection)
		return 0
	}
	communityProfileSeq := seqOf(CollectionCommunityProfile)
	actorProfileSeq := seqOf(CollectionActorProfile)
	postSeq := seqOf(CollectionPostV2)

	assert.Less(t, communityProfileSeq, postSeq,
		"community profile must be committed before the post that references it")
	assert.Less(t, actorProfileSeq, postSeq,
		"author profile must be committed before the post that references it")
}

// TestIdempotentRematerialize: materializing the same object twice yields
// the identical at-uri and CID, a single mapping row, and no second commit
// or firehose event.
func TestIdempotentRematerialize(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()
	page := loadFixtureObject(t, "page_lemmy_world.json")

	first, err := h.m.MaterializePost(ctx, page, groupID)
	require.NoError(t, err)
	require.False(t, first.NoOp)
	eventsBefore := len(h.firehoseEvents())

	second, err := h.m.MaterializePost(ctx, page, groupID)
	require.NoError(t, err)
	assert.True(t, second.NoOp, "identical re-materialization must be a repo no-op")
	assert.Equal(t, first.ATURI, second.ATURI)
	assert.Equal(t, first.CID, second.CID)
	assert.Equal(t, eventsBefore, len(h.firehoseEvents()),
		"a no-op re-put must not emit firehose events")

	var mappingCount int
	db := testutil.DB(t)
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM ap_objects WHERE ap_id = $1`, pageID).Scan(&mappingCount))
	assert.Equal(t, 1, mappingCount, "one mapping row per AP object")
}

// TestNobridgeActorNeverMaterialized: an author carrying #nobridge is never
// minted and their post is dropped with a skip.
func TestNobridgeActorNeverMaterialized(t *testing.T) {
	h := newHarness(t)
	h.serveFixture("/c/technology", "group_lemmy_world.json")
	h.serveObject("/u/optout", person("https://lemmy.world/u/optout", "optout", map[string]any{
		"summary": "<p>I opt out. #nobridge</p>",
	}))
	pageObj := loadFixtureObject(t, "page_lemmy_world.json")
	pageObj.AttributedTo = ap.Refs{ap.Object{ID: "https://lemmy.world/u/optout"}}

	_, err := h.m.MaterializePost(context.Background(), pageObj, groupID)
	require.Error(t, err)
	assert.True(t, IsSkip(err), "nobridge must be a skip, not a failure: %v", err)

	_, err = h.actors.GetByAPActorID(context.Background(), "https://lemmy.world/u/optout")
	assert.True(t, errors.IsNotFound(err), "no bridged_actors row is created for opted-out actors")
	assert.Equal(t, 0, countMappings(t, h, pageID), "the post must not be materialized")
}

// TestPostWithoutPublishedSkipped: no usable published time → no
// deterministic rkey → fail closed with a skip.
func TestPostWithoutPublishedSkipped(t *testing.T) {
	h := newHarness(t)
	pageObj := loadFixtureObject(t, "page_lemmy_world.json")
	pageObj.Published = nil

	_, err := h.m.MaterializePost(context.Background(), pageObj, groupID)
	require.Error(t, err)
	assert.True(t, IsSkip(err))
}

// TestAvatarBlobStoredAndServed: the profile avatar is fetched, stored in
// the blobs table under the actor's DID, and referenced from the record.
func TestAvatarBlobStored(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	_, err := h.m.EnsureActor(ctx, &ap.Object{ID: personID})
	require.NoError(t, err)

	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
	record, _, err := h.manager.GetRecord(ctx, authorDID, CollectionActorProfile, ProfileRKey)
	require.NoError(t, err)

	raw, err := json.Marshal(record["avatar"])
	require.NoError(t, err)
	var blobJSON struct {
		Type     string `json:"$type"`
		MimeType string `json:"mimeType"`
		Size     int64  `json:"size"`
		Ref      struct {
			Link string `json:"$link"`
		} `json:"ref"`
	}
	require.NoError(t, json.Unmarshal(raw, &blobJSON))
	assert.Equal(t, "blob", blobJSON.Type)
	assert.Equal(t, "image/png", blobJSON.MimeType)
	assert.Equal(t, int64(len(pngBytes)), blobJSON.Size)

	data, mimeType, err := h.manager.GetBlob(ctx, authorDID, blobJSON.Ref.Link)
	require.NoError(t, err)
	assert.Equal(t, "image/png", mimeType)
	assert.Equal(t, pngBytes, data)
}

// TestOversizedOrWrongTypeMediaDropped: media violating slot caps or type
// allowlists is omitted (fail closed) while the profile itself lands.
func TestOversizedOrWrongTypeMediaDropped(t *testing.T) {
	h := newHarness(t)
	// Avatar: bigger than the 1 MB avatar slot. Banner: a non-image type.
	h.mux.HandleFunc("GET /media/huge.png", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(make([]byte, 1_000_001))
	})
	h.mux.HandleFunc("GET /media/page.html", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html>not an image</html>"))
	})
	h.serveObject("/u/mediahog", person("https://lemmy.world/u/mediahog", "mediahog", map[string]any{
		"icon":  map[string]any{"type": "Image", "url": "https://lemmy.world/media/huge.png"},
		"image": map[string]any{"type": "Image", "url": "https://lemmy.world/media/page.html"},
	}))

	ctx := context.Background()
	_, err := h.m.EnsureActor(ctx, &ap.Object{ID: "https://lemmy.world/u/mediahog"})
	require.NoError(t, err, "media failures must not block the profile")

	record, _, err := h.manager.GetRecord(ctx,
		testDIDFor("mediahog", "lemmy.world"), CollectionActorProfile, ProfileRKey)
	require.NoError(t, err)
	assert.NotContains(t, record, "avatar", "oversized avatar must be dropped")
	assert.NotContains(t, record, "banner", "non-image banner must be dropped")
}

// TestPublicOnlyAudienceFallsBackToAddressing: an `audience` carrying only
// the AS2 public collection names no community, in every spelling. The ingest
// layer's communityIRIFrom already reads it that way, and a materializer that
// did not would EnsureCommunity("as:Public") — a retryable failure that backs
// the whole ordering key off into poison over a perfectly ordinary post. The
// group in `to` answers instead.
func TestPublicOnlyAudienceFallsBackToAddressing(t *testing.T) {
	for _, spelling := range []string{"as:Public", "Public", ap.PublicAudience} {
		// A FRESH harness per spelling: the community is now a record FIELD
		// rather than the record's repo, and carry-forward pins that field
		// after the first materialization — so reusing one harness would let
		// the first spelling's answer stand in for the other two.
		h := newHarness(t)
		h.serveLemmyWorldFixtures()
		ctx := context.Background()

		pageObj := loadFixtureObject(t, "page_lemmy_world.json")
		pageObj.Audience = ap.Audience{spelling}
		pageObj.To = ap.Audience{groupID, ap.PublicAudience}

		res, err := h.m.MaterializePost(ctx, pageObj, groupID)
		require.NoError(t, err, "audience %q must not be mistaken for a community", spelling)
		assert.Equal(t, testDIDFor("LeftLeaningFreedomFighters", "lemmy.world"), res.DID,
			"the post lands in the author's repo")

		mapping, err := h.objects.GetByAPID(ctx, pageID)
		require.NoError(t, err)
		record, _, err := h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
		require.NoError(t, err)
		assert.Equal(t, testDIDFor("technology", "lemmy.world"), record["community"],
			"the post names the community found in `to`, not the public collection")
	}
}
