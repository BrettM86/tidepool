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

// BARE (un-announced) echo suppression — the Delete and Undo branches of
// Process (handler.go dispatch), which the announce-envelope guard never sees.
//
// WHY THIS TEST EXISTS, AND WHY IT IS NOT DEAD CODE. A remote peer cannot
// produce these deliveries today: authorizeDelete requires
// SameAuthority(target, signer) for a bare delete, our object ids live on the
// user origin, and signature verification resolves the signer's actor document
// from that origin. The harness legitimately controls signing, so the fixture
// is constructible here even though the network cannot construct it.
//
// That reachability argument is a property of TODAY'S ROUTING, not of the
// invariant — and task 17c rewrites exactly these switches (it adds Remove,
// Lock, Block and their Undo forms, and it populates mapping.CommunityDID,
// which is what turned M1's accidental drop into a destructive one). The
// invariant being pinned is the durable thing:
//
//	the bridge NEVER processes its own activity as inbound content,
//	on any path, however it arrives.
//
// The failure it prevents is the one 17a already paid to fix once: an author's
// own delete coming home and being written up as a community-signed moderator
// removal, published to the firehose, where nothing downstream can tell it from
// a real one.
const (
	bdUserOrigin = "https://coves.social"
	bdAuthorDID  = "did:plc:bdbareauthor000001"
	bdActorID    = bdUserOrigin + "/ap/actor/" + bdAuthorDID
	bdPostRKey   = "3lzbaredelete01"
	bdPostATURI  = "at://" + bdAuthorDID + "/social.coves.community.postv2/" + bdPostRKey
	bdPostAPID   = bdUserOrigin + "/ap/object/" + bdAuthorDID +
		"/social.coves.community.postv2/" + bdPostRKey
	bdPostCID = "bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqrsqxi3jgxte"
)

// bareWorld is a native post federated out and then deleted by its author, with
// the user origin really serving both ids and a counter over that surface.
type bareWorld struct {
	persona       *remoteActor
	communityDID  string
	digestRKey    string
	deletePayload map[string]any
	selfFetches   *atomic.Int64
}

func setupBareEchoWorld(t *testing.T, h *harness) bareWorld {
	t.Helper()
	ctx := context.Background()
	h.subscribeTechnology()
	h.serveLemmyWorldContent()
	communityDID := testDIDFor("technology", "lemmy.world")

	// The user origin serves our object and our activities for real. Without
	// that, a restore path's fetch would 404 and the test would pass for the
	// wrong reason — the point is that dereferencing our own id SUCCEEDS and
	// must still never happen.
	userOrigin, err := personas.New(personas.Options{
		DB:         h.db,
		Custodian:  h.custodian,
		UserOrigin: bdUserOrigin,
	})
	require.NoError(t, err)
	selfFetches := &atomic.Int64{}
	h.mux.Handle("/ap/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		selfFetches.Add(1)
		userOrigin.ServeHTTP(w, r)
	}))
	_, err = userOrigin.CreateActorForDID(ctx, bdAuthorDID, "bdauthor.coves.social")
	require.NoError(t, err, "mint the native author's persona")
	// The signing document is registered at the actor's EXACT path, which
	// outranks the "/ap/" prefix above: signature verification therefore never
	// touches the counter, which is left measuring object and activity fetches
	// — the ones that would mean we dereferenced our own content.
	persona := h.newRemoteActor(bdActorID, person(bdActorID, "bdauthor", nil))

	_, err = store.NewOutboundObjects(h.db).Upsert(ctx, store.OutboundObject{
		ATURI:              bdPostATURI,
		APObjectID:         bdPostAPID,
		LastCID:            bdPostCID,
		LastRev:            "3lzbarerev0001",
		CommunityDID:       communityDID,
		CommunityAPID:      groupID,
		TranslatedSnapshot: bdSnapshot(t),
	})
	require.NoError(t, err)

	enqueuer, err := outbound.NewEnqueuer(outbound.EnqueuerOptions{
		DB:         h.db,
		Translator: outbound.NewTranslator(bdUserOrigin),
		Inboxes:    outbound.NewInboxResolver(h.client, time.Minute),
		Actors:     store.NewAPActors(h.db),
		UserOrigin: bdUserOrigin,
	})
	require.NoError(t, err)
	enqueueAs(t, h.db, enqueuer, bdAuthorDID, consume.PostIntent{
		Op:            "create",
		ATURI:         bdPostATURI,
		ID:            consume.ActivityID(bdUserOrigin, bdPostATURI, "create", 0),
		CommunityAPID: groupID,
		Snapshot:      bdSnapshot(t),
	})
	deleteIntent := consume.PostIntent{
		Op:            "delete",
		ATURI:         bdPostATURI,
		ID:            consume.ActivityID(bdUserOrigin, bdPostATURI, "delete", 1),
		CommunityAPID: groupID,
		Snapshot:      bdSnapshot(t),
	}
	enqueueAs(t, h.db, enqueuer, bdAuthorDID, deleteIntent)

	stored, err := store.NewOutboundActivities(h.db).Get(ctx, deleteIntent.ID)
	require.NoError(t, err)
	var payload map[string]any
	require.NoError(t, json.Unmarshal(stored.Payload, &payload))

	// The acceptance the admission wrote, so a spurious removal has something
	// real to destroy.
	digest := testDigestRKey(bdPostATURI)
	_, err = h.manager.PutRecord(ctx, communityDID, materialize.CollectionAcceptance, digest,
		map[string]any{
			"$type":     materialize.CollectionAcceptance,
			"subject":   map[string]any{"uri": bdPostATURI, "cid": bdPostCID},
			"createdAt": "2026-08-13T09:00:00.000Z",
		})
	require.NoError(t, err)

	selfFetches.Store(0)
	return bareWorld{
		persona:       persona,
		communityDID:  communityDID,
		digestRKey:    digest,
		deletePayload: payload,
		selfFetches:   selfFetches,
	}
}

func bdSnapshot(t *testing.T) []byte {
	t.Helper()
	snap, err := json.Marshal(map[string]any{
		"atUri":      bdPostATURI,
		"cid":        bdPostCID,
		"rev":        "3lzbarerev0001",
		"collection": "social.coves.community.postv2",
		"record": map[string]any{
			"$type":     "social.coves.community.postv2",
			"community": testDIDFor("technology", "lemmy.world"),
			"title":     "A native post whose own Delete must not come back in",
			"content":   "bare-path echo suppression",
			"createdAt": "2026-08-13T09:00:00.000Z",
		},
		"communityApId": groupID,
	})
	require.NoError(t, err)
	return snap
}

// dropSnapshot captures every echo counter, for delta assertions (the expvar
// counters are process-global).
func dropSnapshot() map[echo.Class]int64 {
	snapshot := map[echo.Class]int64{}
	for _, class := range echoClasses {
		snapshot[class] = echo.Drops(class)
	}
	return snapshot
}

// assertOnlyClassMoved asserts exactly one class advanced, by exactly one.
func assertOnlyClassMoved(t *testing.T, before map[echo.Class]int64, want echo.Class, why string) {
	t.Helper()
	for _, class := range echoClasses {
		expected := before[class]
		if class == want {
			expected++
		}
		assert.Equal(t, expected, echo.Drops(class),
			"counter %q after a drop expected to classify %q: %s", class, want, why)
	}
}

// TestBareDeleteOfOurOwnObjectIsDropped: the bare Delete branch has no
// envelope for the announce guard to read, and the ordinary path it falls into
// is destructive — it lays a tombstone marker for the id and hands the target
// to the materializer's delete.
//
// The marker is the quiet half. A marker on our own object id suppresses that
// id's own later Create, and a soft-deleted mapping makes ResolveStrongRef
// answer Tombstoned — which drops every genuine Lemmy reply to the post, whole
// subtrees at a time, with nothing written anywhere to say why.
func TestBareDeleteOfOurOwnObjectIsDropped(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := setupBareEchoWorld(t, h)
	before := dropSnapshot()
	commitsBefore := len(communityEvents(t, h, world.communityDID))

	require.Equal(t, http.StatusAccepted, h.deliver(world.persona, world.deletePayload),
		"the inbox accepts it; the drop is a processing decision")
	h.drain()

	tombstoned, err := h.tombstones.ExistsFor(ctx, bdPostAPID, "")
	require.NoError(t, err)
	assert.False(t, tombstoned,
		"NO tombstone marker for our own id: it would suppress this object's own later "+
			"Create, and a tombstoned ancestor drops every Lemmy reply beneath it")

	mapping, err := h.objects.GetByAPID(ctx, bdPostAPID)
	require.NoError(t, err)
	assert.False(t, mapping.IsDeleted(),
		"and the mapping stays live: soft-deleting it makes ResolveStrongRef answer "+
			"Tombstoned, which silently drops whole reply subtrees")
	assert.Equal(t, store.OriginBridge, mapping.Origin)

	_, _, err = h.manager.GetRecord(ctx, world.communityDID, materialize.CollectionRemoval, world.digestRKey)
	assert.True(t, errors.IsNotFound(err),
		"no removal record may be written for our own delete (err=%v)", err)
	_, _, err = h.manager.GetRecord(ctx, world.communityDID, materialize.CollectionAcceptance, world.digestRKey)
	assert.NoError(t, err, "and the acceptance is undisturbed")
	assert.Equal(t, commitsBefore, len(communityEvents(t, h, world.communityDID)),
		"an echo produces no commit in the community repo")

	assertOnlyClassMoved(t, before, echo.ClassLocalActivity,
		"the bare Delete IS one of our activities — outer-in, its own id answers first")

	event, err := h.events.GetEvent(ctx, world.deletePayload["id"].(string))
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt, "processed, not left pending")
	assert.Nil(t, event.FailedAt, "and never poisoned: %s", event.Error)
}

// TestBareUndoOfOurOwnDeleteIsDropped: the Undo branch, whose MAPPED restore
// path is the more dangerous of the two. It re-fetches the target from its own
// authority and re-materializes whatever comes back — so for one of our own
// object URLs it would dereference our own origin and feed our own post back
// through inbound materialization, minting a bridged actor for our own persona
// on the way.
func TestBareUndoOfOurOwnDeleteIsDropped(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := setupBareEchoWorld(t, h)
	before := dropSnapshot()
	opsBefore := len(h.firehoseOps())

	// The outer Undo carries a REMOTE id (peers re-mint them), so the identity
	// that gives it away is the actor: our own persona.
	undoID := "https://lemmy.world/activities/undo/bare-echo-1"
	require.Equal(t, http.StatusAccepted, h.deliver(world.persona, map[string]any{
		"@context": "https://www.w3.org/ns/activitystreams",
		"id":       undoID,
		"type":     "Undo",
		"actor":    bdActorID,
		"object":   world.deletePayload,
	}))
	h.drain()

	assert.Equal(t, int64(0), world.selfFetches.Load(),
		"the restore path must never dereference our own object URL: that fetch is what "+
			"authorizes the restore AND supplies the body, so following it feeds our own "+
			"post back in as if a stranger had served it")
	assert.Equal(t, opsBefore, len(h.firehoseOps()),
		"and nothing may be re-materialized — no commit, in any repo")

	_, err := h.actors.GetByAPActorID(ctx, bdActorID)
	assert.True(t, errors.IsNotFound(err),
		"above all, NO bridged actor may be minted for our own persona: that is the mint "+
			"oracle, and it would give one of our users a second, fediverse-side identity "+
			"(err=%v)", err)

	mapping, err := h.objects.GetByAPID(ctx, bdPostAPID)
	require.NoError(t, err)
	assert.False(t, mapping.IsDeleted(), "the mapping is untouched")

	assertOnlyClassMoved(t, before, echo.ClassLocalActor,
		"the outer Undo's id is the peer's, so the walk's next question — its ACTOR — is "+
			"what identifies it as ours")

	event, err := h.events.GetEvent(ctx, undoID)
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt, "processed, not left pending")
	assert.Nil(t, event.FailedAt, "and never poisoned: %s", event.Error)
}

// TestGenuineBareDeletesAndUndosStillApply is the control, and it is the half
// that would hurt more if it broke. Suppressing our own bare activities must
// not become "ignore bare deletes and undos": a fediverse instance deleting its
// OWN content, and a Lemmy human retracting their OWN vote, are the ordinary
// traffic these branches exist for, and losing them is silent.
func TestGenuineBareDeletesAndUndosStillApply(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	author := h.newRemoteActor(personID, person(personID, "LeftLeaningFreedomFighters", nil))

	require.Equal(t, http.StatusAccepted,
		h.deliver(group, loadFixture(t, "announce_create_page_lemmy_world.json")))
	h.drain()
	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err, "precondition: the Lemmy post is materialized")
	require.False(t, mapping.IsDeleted())

	before := dropSnapshot()

	// (a) A bare Delete from the origin instance for its OWN object.
	require.Equal(t, http.StatusAccepted, h.deliver(author, loadFixture(t, "delete_page.json")))
	h.drain()
	deleted, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	assert.True(t, deleted.IsDeleted(),
		"a genuine bare Delete must still tombstone the object: this is the fediverse "+
			"deleting its own content, and dropping it leaves us serving records their "+
			"authors removed")

	// (b) A bare Like and its bare Undo from a real Lemmy human.
	require.Equal(t, http.StatusAccepted, h.deliver(author, map[string]any{
		"id":     "https://lemmy.world/activities/like/genuine-bare-1",
		"type":   "Like",
		"actor":  personID,
		"object": pageID,
	}))
	require.Equal(t, http.StatusAccepted, h.deliver(author, map[string]any{
		"id":    "https://lemmy.world/activities/undo/genuine-bare-1",
		"type":  "Undo",
		"actor": personID,
		"object": map[string]any{
			"id":     "https://lemmy.world/activities/like/genuine-bare-1",
			"type":   "Like",
			"actor":  personID,
			"object": pageID,
		},
	}))
	h.drain()
	h.votes.mu.Lock()
	applied, retracted := len(h.votes.applied), len(h.votes.retracted)
	h.votes.mu.Unlock()
	assert.Equal(t, 1, applied, "a genuine bare vote still reaches the aggregator")
	assert.Equal(t, 1, retracted,
		"and so does its Undo: the aggregator's own voter probe decides votes, and it "+
			"answers 'not ours' for every fediverse human")

	for _, class := range echoClasses {
		assert.Equal(t, before[class], echo.Drops(class),
			"no echo counter may move for genuine remote traffic (%s) — a false positive "+
				"here deletes nothing visibly and loses everything", class)
	}
}
