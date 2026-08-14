package ingest

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/consume"
	"tidepool/internal/echo"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/outbound"
	"tidepool/internal/personas"
	"tidepool/internal/store"
)

// THE BACKFILL BYPASSES BOTH GUARDS.
//
// Backfill.materializeOutboxItem and backfillReplies call MaterializePost /
// MaterializeComment DIRECTLY — past suppressEcho (no envelope reaches them)
// and past materializeContent's legacy origin=bridge check (that funnel is not
// on this path at all). A bridged community's outbox contains OUR federated
// content the moment native users participate, and any re-trigger re-walks it.
//
// Two things then happen, both silent:
//
//   - MaterializePost calls EnsureActor(attributedTo) BEFORE it reads any
//     mapping, so our own persona acquires a bridged_actors row — the mint
//     oracle echo_bare_test.go asserts can never happen, reached by an ordinary
//     admin action;
//   - PutMapping rewrites our bridge-origin mapping with the default
//     origin=fediverse, which makes the classifier answer ClassNone for that id
//     from then on. The echo guard disables itself for the post, permanently,
//     and nothing anywhere reports it.
//
// The package doc claims the invariant holds "on any path, however it arrives".
// This is a path.
const (
	bfUserOrigin = "https://coves.social"
	bfAuthorDID  = "did:plc:bfbackfillauthor01"
	bfActorID    = bfUserOrigin + "/ap/actor/" + bfAuthorDID
	bfPostRKey   = "3lzbackfill0001"
	bfPostATURI  = "at://" + bfAuthorDID + "/social.coves.community.postv2/" + bfPostRKey
	bfPostAPID   = bfUserOrigin + "/ap/object/" + bfAuthorDID +
		"/social.coves.community.postv2/" + bfPostRKey
	bfPostCID = "bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqrsqxi3jgxte"
)

// TestBackfillDoesNotReMaterializeOurOwnContent is H2.
//
// The community's outbox carries BOTH kinds of history — a post we federated
// out and a post a Lemmy human wrote — because that is what a bridged
// community's outbox looks like once anyone native has posted. The backfill
// must walk past the first and materialize the second.
func TestBackfillDoesNotReMaterializeOurOwnContent(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.subscribeTechnology()
	h.serveLemmyWorldContent()

	// Our origin serves the object for real: the outbox item is cross-authority
	// with the outbox host, so the walk RE-FETCHES it rather than trusting the
	// embedded body — and that fetch succeeds. The bypass is not a fixture
	// artifact.
	userOrigin, err := personas.New(personas.Options{
		DB:         h.db,
		Custodian:  h.custodian,
		UserOrigin: bfUserOrigin,
	})
	require.NoError(t, err)
	selfFetches := &atomic.Int64{}
	h.mux.Handle("/ap/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		selfFetches.Add(1)
		userOrigin.ServeHTTP(w, r)
	}))
	_, err = userOrigin.CreateActorForDID(ctx, bfAuthorDID, "bfauthor.coves.social")
	require.NoError(t, err)

	communityDID := testDIDFor("technology", "lemmy.world")
	_, err = store.NewOutboundObjects(h.db).Upsert(ctx, store.OutboundObject{
		ATURI:              bfPostATURI,
		APObjectID:         bfPostAPID,
		LastCID:            bfPostCID,
		LastRev:            "3lzbfrev000001",
		CommunityDID:       communityDID,
		CommunityAPID:      groupID,
		TranslatedSnapshot: bfSnapshot(t),
	})
	require.NoError(t, err)

	enqueuer, err := outbound.NewEnqueuer(outbound.EnqueuerOptions{
		DB:         h.db,
		Translator: outbound.NewTranslator(bfUserOrigin),
		Inboxes:    outbound.NewInboxResolver(h.client, time.Minute),
		Actors:     store.NewAPActors(h.db),
		UserOrigin: bfUserOrigin,
	})
	require.NoError(t, err)
	enqueueAs(t, h.db, enqueuer, bfAuthorDID, consume.PostIntent{
		Op:            "create",
		ATURI:         bfPostATURI,
		ID:            consume.ActivityID(bfUserOrigin, bfPostATURI, "create", 0),
		CommunityAPID: groupID,
		Snapshot:      bfSnapshot(t),
	})
	ourMapping, err := h.objects.GetByAPID(ctx, bfPostAPID)
	require.NoError(t, err, "precondition: our post is mapped bridge-origin")
	require.Equal(t, store.OriginBridge, ourMapping.Origin)

	// The community's outbox: our federated post, then a genuine Lemmy post.
	h.serveObject("/c/technology/outbox", map[string]any{
		"@context":     "https://www.w3.org/ns/activitystreams",
		"type":         "OrderedCollection",
		"id":           groupID + "/outbox",
		"totalItems":   2,
		"orderedItems": []any{bfAnnouncedPage(bfPostAPID, bfActorID), bfAnnouncedPage(pageID, personID)},
	})

	before := dropSnapshot()
	opsBefore := len(h.firehoseOps())
	selfFetches.Store(0)

	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	backfill := newBackfill(t, h, 10)
	// Non-fatal: today this run FAILS the item (it tries to write a postv2 into
	// the native author's repo, which the bridge does not host), and the state
	// assertions below are the ones that say what actually happened. A skipped
	// item is not a failed one — after the fix the whole run is clean.
	assert.NoError(t, backfill.Run(ctx, community, true),
		"walking past our own content is a SKIP, not an error: an errored item leaves the "+
			"community's backfill permanently unresumable-looking and retries forever")

	// --- Our own post: untouched. ---
	_, err = h.actors.GetByAPActorID(ctx, bfActorID)
	assert.True(t, errors.IsNotFound(err),
		"NO bridged actor may be minted for our own persona: MaterializePost calls "+
			"EnsureActor before it reads any mapping, so an ordinary re-backfill is a mint "+
			"oracle for every native user who has posted (err=%v)", err)

	after, err := h.objects.GetByAPID(ctx, bfPostAPID)
	require.NoError(t, err)
	assert.Equal(t, store.OriginBridge, after.Origin,
		"our mapping must stay origin=bridge: re-materializing rewrites it to the default "+
			"origin=fediverse, and the classifier then answers ClassNone for this id forever "+
			"— the echo guard silently disabling itself for the post")
	assert.Equal(t, ourMapping.ATURI, after.ATURI, "and it must still name the record we federated")
	assert.Equal(t, int64(0), selfFetches.Load(),
		"our own object must not even be dereferenced: the backfill has no envelope, so the "+
			"id itself is the only thing that can stop it")

	// --- The genuine Lemmy post in the SAME outbox: backfilled normally. ---
	lemmyMapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err,
		"the community's real history must still backfill: suppression here must key on "+
			"whose content it is, never on 'this outbox contains something of ours'")
	assert.Equal(t, store.OriginFediverse, lemmyMapping.Origin)
	assert.Equal(t, materialize.CollectionPostV2, lemmyMapping.Collection)
	assert.Greater(t, len(h.firehoseOps()), opsBefore,
		"and it commits: a backfill that drops everything is the failure mode this control exists for")

	// Attribution: the drop is counted like every other, under the class that
	// identified it (the object id is ours).
	for _, class := range echoClasses {
		want := before[class]
		if class == echo.ClassMappedObject {
			want++
		}
		assert.Equal(t, want, echo.Drops(class),
			"counter %q after a backfill drop: an uncounted drop on this path is exactly how "+
				"the bypass stayed invisible", class)
	}
}

// bfAnnouncedPage is one outbox item in Lemmy's shape: Announce{Create{Page}}.
func bfAnnouncedPage(pageAPID, author string) map[string]any {
	return map[string]any{
		"id":    pageAPID + "/announce",
		"type":  "Announce",
		"actor": groupID,
		"to":    []any{"https://www.w3.org/ns/activitystreams#Public"},
		"object": map[string]any{
			"id":    pageAPID + "/create",
			"type":  "Create",
			"actor": author,
			"object": map[string]any{
				"id":           pageAPID,
				"type":         "Page",
				"attributedTo": author,
				"audience":     groupID,
				"to":           []any{"https://www.w3.org/ns/activitystreams#Public", groupID},
				"name":         "a post in the community's history",
				"content":      "<p>backfilled</p>",
				"published":    "2026-08-13T09:00:00.000000Z",
			},
		},
	}
}

func bfSnapshot(t *testing.T) []byte {
	t.Helper()
	snap, err := json.Marshal(map[string]any{
		"atUri":      bfPostATURI,
		"cid":        bfPostCID,
		"rev":        "3lzbfrev000001",
		"collection": "social.coves.community.postv2",
		"record": map[string]any{
			"$type":     "social.coves.community.postv2",
			"community": testDIDFor("technology", "lemmy.world"),
			"title":     "a post in the community's history",
			"content":   "backfilled",
			"createdAt": "2026-08-13T09:00:00.000Z",
		},
		"communityApId": groupID,
	})
	require.NoError(t, err)
	return snap
}
