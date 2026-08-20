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
	"tidepool/internal/materialize"
	"tidepool/internal/outbound"
	"tidepool/internal/personas"
	"tidepool/internal/store"
)

// CHARACTERIZATION (task 17a): the three-layer sandwich and the ancestor
// short-circuit. These pin behaviour that already works, because echo
// suppression is one wrong predicate away from eating it.
//
// The load-bearing case is C2: a Lemmy user replying to a native post. Its
// inReplyTo IS one of our object URLs, so any suppression rule that looks at
// "does this envelope mention something of ours" instead of "is this envelope
// OURS" swallows genuine community content — invisibly, since a dropped reply
// leaves no trace anywhere.
const (
	csUserOrigin = "https://coves.social"

	// Layer 1: the native post, federated out to the community.
	csAuthorDID = "did:plc:cssandwichauthor01"
	csAuthorID  = csUserOrigin + "/ap/actor/" + csAuthorDID
	csPostRKey  = "3lzcssandwich01"
	csPostATURI = "at://" + csAuthorDID + "/social.coves.community.postv2/" + csPostRKey
	csPostAPID  = csUserOrigin + "/ap/object/" + csAuthorDID + "/social.coves.community.postv2/" + csPostRKey
	csPostCID   = "bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqrsqxi3jgxte"
	csPostRev   = "3lzcsrev000001"

	// Layer 2: a Lemmy human replying to it.
	csReplier      = "https://lemmy.world/u/replier"
	csLemmyComment = "https://lemmy.world/comment/9001"
	// ...and a Lemmy reply to that Lemmy reply, whose parent is fediverse-origin
	// (the control for the short-circuit counter).
	csLemmyComment2 = "https://lemmy.world/comment/9002"

	// Layer 3: a native user replying to the Lemmy comment.
	csNativeCommenterDID = "did:plc:cssandwichnative02"
	csNativeCommenterID  = csUserOrigin + "/ap/actor/" + csNativeCommenterDID
	csReplyRKey          = "3lzcsnative0002"
	csReplyATURI         = "at://" + csNativeCommenterDID + "/social.coves.community.comment/" + csReplyRKey
	csReplyAPID          = csUserOrigin + "/ap/object/" + csNativeCommenterDID +
		"/social.coves.community.comment/" + csReplyRKey
	csReplyCID = "bafyreievgu2ty7qbiaaom5zhmkznsnajuzideek3lo7e65dwqlrvrxnmo4"
)

// sandwich is the world these tests share: the native post federated out, our
// origin really serving it, and a self-fetch counter over that surface.
type sandwich struct {
	group       *remoteActor
	enqueuer    *outbound.Enqueuer
	selfFetches *atomic.Int64
}

func setupSandwich(t *testing.T, h *harness) sandwich {
	t.Helper()
	ctx := context.Background()
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()

	// The user origin serves our object for real, so "it was not fetched" is a
	// statement about the walk and not about a broken fixture: if the ancestor
	// walk DID dereference our URL, it would succeed and the only evidence
	// would be this counter.
	userOrigin, err := personas.New(personas.Options{
		DB:         h.db,
		Custodian:  h.custodian,
		UserOrigin: csUserOrigin,
	})
	require.NoError(t, err)
	selfFetches := &atomic.Int64{}
	h.mux.Handle("/ap/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		selfFetches.Add(1)
		userOrigin.ServeHTTP(w, r)
	}))
	for _, actor := range []struct{ did, handle string }{
		{csAuthorDID, "author.coves.social"},
		{csNativeCommenterDID, "nativecommenter.coves.social"},
	} {
		_, err := userOrigin.CreateActorForDID(ctx, actor.did, actor.handle)
		require.NoError(t, err, "mint %s", actor.did)
	}

	_, err = store.NewOutboundObjects(h.db).Upsert(ctx, store.OutboundObject{
		ATURI:              csPostATURI,
		APObjectID:         csPostAPID,
		LastCID:            csPostCID,
		LastRev:            csPostRev,
		CommunityDID:       testDIDFor("technology", "lemmy.world"),
		CommunityAPID:      groupID,
		TranslatedSnapshot: csPostSnapshot(t),
	})
	require.NoError(t, err)

	enqueuer, err := outbound.NewEnqueuer(outbound.EnqueuerOptions{
		DB:         h.db,
		Translator: outbound.NewTranslator(csUserOrigin),
		Inboxes:    outbound.NewInboxResolver(h.client, time.Minute),
		Actors:     store.NewAPActors(h.db),
		UserOrigin: csUserOrigin,
	})
	require.NoError(t, err)

	// Layer 1 federates: this is what writes the bridge-origin ap_objects
	// mapping (with its at-uri and cid) that the ancestor walk anchors on.
	enqueueAs(t, h.db, enqueuer, csAuthorDID, consume.PostIntent{
		Op:            "create",
		ATURI:         csPostATURI,
		ID:            consume.ActivityID(csUserOrigin, csPostATURI, "create", 0),
		CommunityAPID: groupID,
		Snapshot:      csPostSnapshot(t),
	})
	mapping, err := h.objects.GetByAPID(ctx, csPostAPID)
	require.NoError(t, err, "precondition: the native post is mapped bridge-origin")
	require.Equal(t, csPostATURI, mapping.ATURI)
	require.NotEmpty(t, mapping.CID, "the anchor needs a cid: reply refs are strongRefs")

	selfFetches.Store(0)
	return sandwich{group: group, enqueuer: enqueuer, selfFetches: selfFetches}
}

func csPostSnapshot(t *testing.T) []byte {
	t.Helper()
	snap, err := json.Marshal(map[string]any{
		"atUri":      csPostATURI,
		"cid":        csPostCID,
		"rev":        csPostRev,
		"collection": "social.coves.community.postv2",
		"record": map[string]any{
			"$type":     "social.coves.community.postv2",
			"community": testDIDFor("technology", "lemmy.world"),
			"title":     "A native post the fediverse can reply to",
			"content":   "layer one of the sandwich",
			"createdAt": "2026-08-13T09:00:00.000Z",
		},
		"communityApId": groupID,
	})
	require.NoError(t, err)
	return snap
}

// announceCreateNote wraps a Note in the community's Create fan-out.
func announceCreateNote(announceID, author string, note map[string]any) map[string]any {
	return echoAnnounce(announceID, map[string]any{
		"id":       announceID + "/create",
		"type":     "Create",
		"actor":    author,
		"audience": groupID,
		"object":   note,
	})
}

// TestLemmyReplyToNativePostMaterializes is C1 + C2: the ancestor short-circuit
// and the data-loss guard that rides on it.
func TestLemmyReplyToNativePostMaterializes(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := setupSandwich(t, h)
	h.serveObject("/u/replier", person(csReplier, "replier", nil))
	comment := note(csLemmyComment, csReplier, csPostAPID, "a fediverse reply to native content",
		"2026-08-13T10:00:00.000000Z")
	h.serveObject("/comment/9001", comment)

	dropsBefore := map[echo.Class]int64{}
	for _, class := range echoClasses {
		dropsBefore[class] = echo.Drops(class)
	}
	require.Equal(t, http.StatusAccepted, h.deliver(world.group,
		announceCreateNote("https://lemmy.world/activities/announce/create/cs-9001", csReplier, comment)))
	h.drain()

	// C2: the reply LANDS. Echo suppression must key on whose activity this is,
	// never on what it mentions — an inbound reply to native content names our
	// object by definition.
	mapping, err := h.objects.GetByAPID(ctx, csLemmyComment)
	require.NoError(t, err,
		"FP-2: a genuine Lemmy reply to a native post MUST materialize — dropping it is "+
			"invisible in production, because a comment that never lands leaves no trace")
	assert.Equal(t, store.OriginFediverse, mapping.Origin,
		"the reply is the fediverse's, not ours")
	assert.Equal(t, materialize.CollectionComment, mapping.Collection)
	assert.Equal(t, testDIDFor("replier", "lemmy.world"), mapping.DID,
		"it lands in the LEMMY author's own repo")

	record, _, err := h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	require.NoError(t, err)
	reply, ok := record["reply"].(map[string]any)
	require.True(t, ok, "a reply record carries reply refs, got %#v", record)
	parentRef, _ := reply["parent"].(map[string]any)
	rootRef, _ := reply["root"].(map[string]any)
	assert.Equal(t, csPostATURI, parentRef["uri"],
		"the parent ref resolves to the NATIVE post's at-uri, in the author's repo")
	assert.Equal(t, csPostATURI, rootRef["uri"], "and so does the thread root")

	// C1: it anchored on the existing mapping instead of dereferencing our own
	// URL. The origin is live in this fixture, so a fetch would have SUCCEEDED
	// and gone unnoticed without this counter.
	assert.Equal(t, int64(0), world.selfFetches.Load(),
		"the ancestor walk must short-circuit on the bridge-origin mapping: fetching our "+
			"own object URL re-enters materialization for a record we already hold, and "+
			"makes a remote round trip part of an answer we already have")

	// Nothing about this is an echo. (Deltas, not absolutes: the expvar counters
	// are process-global and every other test in this package feeds them.)
	for _, class := range echoClasses {
		if class == echo.ClassAncestorShortCircuit {
			continue
		}
		assert.Equal(t, dropsBefore[class], echo.Drops(class),
			"genuine remote content must not touch the announce-drop counters (%s)", class)
	}
}

// TestAncestorShortCircuitCountsItsOwnClass is C4. The short-circuit is a
// SUPPRESSION — the walk declines to fetch and re-materialize one of our own
// objects — and it gets its own counter, because a false-positive spike is only
// diagnosable in the split.
//
// The control half matters as much as the positive: anchoring on a FEDIVERSE
// parent is ordinary threading, not a short-circuit of ours, and counting it
// would bury the signal under every inbound comment in the network.
func TestAncestorShortCircuitCountsItsOwnClass(t *testing.T) {
	h := newHarness(t)
	world := setupSandwich(t, h)
	h.serveObject("/u/replier", person(csReplier, "replier", nil))
	comment := note(csLemmyComment, csReplier, csPostAPID, "reply to native content",
		"2026-08-13T10:00:00.000000Z")
	h.serveObject("/comment/9001", comment)

	before := echo.Drops(echo.ClassAncestorShortCircuit)
	require.Equal(t, http.StatusAccepted, h.deliver(world.group,
		announceCreateNote("https://lemmy.world/activities/announce/create/cs-9001", csReplier, comment)))
	h.drain()
	assert.Equal(t, before+1, echo.Drops(echo.ClassAncestorShortCircuit),
		"anchoring on a BRIDGE-ORIGIN parent is the ancestor short-circuit, and it is counted "+
			"under its own class — not the announce-drop classes, which count envelopes")

	// Control: a Lemmy reply to that LEMMY comment anchors too, but on
	// fediverse-origin content. That is not a short-circuit of ours.
	after := echo.Drops(echo.ClassAncestorShortCircuit)
	child := note(csLemmyComment2, csReplier, csLemmyComment, "reply to the reply",
		"2026-08-13T10:05:00.000000Z")
	h.serveObject("/comment/9002", child)
	require.Equal(t, http.StatusAccepted, h.deliver(world.group,
		announceCreateNote("https://lemmy.world/activities/announce/create/cs-9002", csReplier, child)))
	h.drain()
	assert.Equal(t, after, echo.Drops(echo.ClassAncestorShortCircuit),
		"anchoring on ORDINARY fediverse content must not be counted: at inbound comment "+
			"volume it would drown the signal this counter exists to give")
}

// TestThreeLayerSandwich is C3: native post → Lemmy reply → native reply to that
// Lemmy reply, with every ref resolving to the right at-uri in the right repo.
//
// SEAM NOTE: layer 3 is driven through the real Translator/Enqueuer with the
// CommentIntent the Jetstream consumer builds (consume/comments.go). Comments
// do not ride the acceptance engine — only posts do — so this is the production
// path minus the firehose consumer, not a stand-in for it.
func TestThreeLayerSandwich(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := setupSandwich(t, h)
	h.serveObject("/u/replier", person(csReplier, "replier", nil))
	comment := note(csLemmyComment, csReplier, csPostAPID, "layer two",
		"2026-08-13T10:00:00.000000Z")
	h.serveObject("/comment/9001", comment)

	// Layer 2 lands.
	require.Equal(t, http.StatusAccepted, h.deliver(world.group,
		announceCreateNote("https://lemmy.world/activities/announce/create/cs-9001", csReplier, comment)))
	h.drain()
	lemmyMapping, err := h.objects.GetByAPID(ctx, csLemmyComment)
	require.NoError(t, err)

	// Layer 3: a native user replies to the Lemmy comment. The consumer resolves
	// the parent through ap_objects (the Lemmy comment's at-uri) and addresses
	// the AP object id it federates under.
	snapshot, err := json.Marshal(map[string]any{
		"atUri":      csReplyATURI,
		"cid":        csReplyCID,
		"rev":        "3lzcsreplyrev01",
		"collection": "social.coves.community.comment",
		"record": map[string]any{
			"$type": "social.coves.community.comment",
			"reply": map[string]any{
				"root":   map[string]any{"uri": csPostATURI, "cid": csPostCID},
				"parent": map[string]any{"uri": lemmyMapping.ATURI, "cid": lemmyMapping.CID},
			},
			"content":   "layer three, from atproto",
			"createdAt": "2026-08-13T10:10:00.000Z",
		},
		"parentAtUri":   lemmyMapping.ATURI,
		"parentApId":    csLemmyComment,
		"communityApId": groupID,
	})
	require.NoError(t, err)
	intent := consume.CommentIntent{
		Op:            "create",
		ATURI:         csReplyATURI,
		ID:            consume.ActivityID(csUserOrigin, csReplyATURI, "create", 0),
		CommunityAPID: groupID,
		ParentAPID:    csLemmyComment,
		Snapshot:      snapshot,
	}
	enqueueAs(t, h.db, world.enqueuer, csNativeCommenterDID, intent)

	// The outbound Note points at the LEMMY comment's AP id — the outward half
	// of the sandwich.
	stored, err := store.NewOutboundActivities(h.db).Get(ctx, intent.ID)
	require.NoError(t, err)
	var activity map[string]any
	require.NoError(t, json.Unmarshal(stored.Payload, &activity))
	outNote, ok := activity["object"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, csLemmyComment, outNote["inReplyTo"],
		"a native reply to a Lemmy comment is addressed to the Lemmy comment's AP id")
	assert.Equal(t, csNativeCommenterID, outNote["attributedTo"],
		"...and attributed to the replying persona, not the post's author")

	// Every layer resolves to the right at-uri in the right repo.
	postMapping, err := h.objects.GetByAPID(ctx, csPostAPID)
	require.NoError(t, err)
	assert.Equal(t, csPostATURI, postMapping.ATURI, "layer 1: the native author's repo")
	assert.Equal(t, testDIDFor("replier", "lemmy.world"), lemmyMapping.DID,
		"layer 2: the Lemmy author's mirrored repo")
	record, _, err := h.manager.GetRecord(ctx, lemmyMapping.DID, lemmyMapping.Collection, lemmyMapping.RKey)
	require.NoError(t, err)
	reply, _ := record["reply"].(map[string]any)
	rootRef, _ := reply["root"].(map[string]any)
	assert.Equal(t, csPostATURI, rootRef["uri"], "layer 2's root is layer 1")

	replyMapping, err := h.objects.GetByAPID(ctx, csReplyAPID)
	require.NoError(t, err, "layer 3 is mapped bridge-origin at enqueue")
	assert.Equal(t, csReplyATURI, replyMapping.ATURI,
		"layer 3: the replying persona's OWN repo (author-owned, not the community's)")
	assert.Equal(t, store.OriginBridge, replyMapping.Origin)

	// And the loop closes: layer 3's own AP id is recognizably ours, so its
	// echo will be caught when the community announces it back.
	classifier, err := echo.New(echo.Options{
		Objects:         h.objects,
		OutboundObjects: store.NewOutboundObjects(h.db),
		Activities:      store.NewOutboundActivities(h.db),
		Actors:          store.NewAPActors(h.db),
	})
	require.NoError(t, err)
	identity, err := classifier.Identify(ctx, csReplyAPID)
	require.NoError(t, err)
	assert.Equal(t, echo.ClassMappedObject, identity.Class,
		"the third layer is ours the moment it is enqueued — before it is ever delivered")
	assert.Equal(t, csReplyATURI, identity.ATURI)
}
