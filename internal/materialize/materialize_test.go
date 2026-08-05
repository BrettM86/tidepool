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

	"github.com/bluesky-social/indigo/atproto/atcrypto"
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
// so repo commits sign and verify.
type fakeMinter struct {
	custodian *identity.Custodian
}

func (f *fakeMinter) MintActor(_ context.Context, req identity.MintRequest) (*identity.Identity, error) {
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
	mux         *http.ServeMux
	fixtures    *httptest.Server
	scrubbed    *recordingScrubber
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
func newHarness(t *testing.T) *harness {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database,
		"ap_objects", "bridged_actors", "communities",
		"blocks", "repo_state", "firehose_events", "blobs")

	custodian, err := identity.NewCustodian(testKEK)
	require.NoError(t, err)
	actors := store.NewBridgedActors(database)
	objects := store.NewAPObjects(database)
	communities := store.NewCommunities(database)
	manager, err := repo.NewManager(database, identity.NewActorKeys(actors, custodian), nil)
	require.NoError(t, err)

	mux := http.NewServeMux()
	fixtures := httptest.NewServer(mux)
	t.Cleanup(fixtures.Close)

	client := ap.NewClient(ap.ClientOptions{
		HTTPClient:            &http.Client{Transport: rewriteTransport{target: fixtures.Listener.Addr().String()}},
		AllowPrivateAddresses: true,
		PerHostRPS:            100000, // deep-chain tests fetch dozens of objects from one "host"
		PerHostBurst:          100000,
		MaxAttempts:           1, // fixture errors are deterministic; retries just slow tests
	})

	scrubbed := &recordingScrubber{}
	m, err := New(Options{
		Fetcher:          client,
		Objects:          objects,
		Actors:           actors,
		Communities:      communities,
		Repos:            manager,
		Minter:           &fakeMinter{custodian: custodian},
		Votes:            scrubbed,
		ServiceDID:       testServiceDID,
		StrictValidation: true, // tests always validate against the vendored lexicons
	})
	require.NoError(t, err)

	h := &harness{
		t: t, m: m, manager: manager,
		objects: objects, actors: actors, communities: communities,
		mux: mux, fixtures: fixtures, scrubbed: scrubbed,
	}
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
		w.Header().Set("Content-Type", ap.ContentTypeActivityJSON)
		_, _ = w.Write(body)
	})
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
// committed into the community repo with author = the person's DID,
// external-link embed with a fetched thumbnail blob, mapping row written.
func TestMaterializePostEndToEnd(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	res, err := h.m.MaterializePost(ctx, loadFixtureObject(t, "page_lemmy_world.json"))
	require.NoError(t, err)
	require.False(t, res.NoOp)

	communityDID := testDIDFor("technology", "lemmy.world")
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
	assert.Equal(t, communityDID, res.DID, "posts live in the COMMUNITY's repo")

	// The record satisfies the Coves post consumer's security checks.
	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	assert.Equal(t, communityDID, mapping.DID)
	assert.Equal(t, authorDID, mapping.AuthorDID)
	assert.Equal(t, CollectionPost, mapping.Collection)

	record, _, err := h.manager.GetRecord(ctx, communityDID, CollectionPost, mapping.RKey)
	require.NoError(t, err)
	assert.Equal(t, communityDID, record["community"], "repo DID must equal record.community")
	assert.Equal(t, authorDID, record["author"])
	assert.Contains(t, record["title"], "DRAM price-fixing")
	embed, ok := record["embed"].(map[string]any)
	require.True(t, ok, "link post must carry an external embed")
	assert.Equal(t, "social.coves.embed.external", embed["$type"])

	// Profiles exist with rkey self, in the right repos.
	_, _, err = h.manager.GetRecord(ctx, communityDID, CollectionCommunityProfile, ProfileRKey)
	require.NoError(t, err)
	profile, _, err := h.manager.GetRecord(ctx, authorDID, CollectionActorProfile, ProfileRKey)
	require.NoError(t, err)
	assert.Contains(t, profile["bio"], "bridged from @LeftLeaningFreedomFighters@lemmy.world",
		"actor bios carry the provenance line")

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

	_, err := h.m.MaterializePost(context.Background(), loadFixtureObject(t, "page_lemmy_world.json"))
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
	postSeq := seqOf(CollectionPost)

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

	first, err := h.m.MaterializePost(ctx, page)
	require.NoError(t, err)
	require.False(t, first.NoOp)
	eventsBefore := len(h.firehoseEvents())

	second, err := h.m.MaterializePost(ctx, page)
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

	_, err := h.m.MaterializePost(context.Background(), pageObj)
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

	_, err := h.m.MaterializePost(context.Background(), pageObj)
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
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	for _, spelling := range []string{"as:Public", "Public", ap.PublicAudience} {
		pageObj := loadFixtureObject(t, "page_lemmy_world.json")
		pageObj.Audience = ap.Audience{spelling}
		pageObj.To = ap.Audience{groupID, ap.PublicAudience}

		res, err := h.m.MaterializePost(ctx, pageObj)
		require.NoError(t, err, "audience %q must not be mistaken for a community", spelling)
		assert.Equal(t, testDIDFor("technology", "lemmy.world"), res.DID,
			"the post lands in the community named by to")
	}
}
