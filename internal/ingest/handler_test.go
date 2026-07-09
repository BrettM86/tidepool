package ingest

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// mintCount returns how many identities the fake minter has minted so far.
func (f *fakeMinter) mintCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mints
}

// followedCommunity registers a second subscribed community (accepted) on an
// arbitrary host that can sign inbox deliveries — the attacker's community in
// the announced-authorization tests. It skips the WebFinger/Follow lifecycle
// and writes the communities row directly.
func (h *harness) followedCommunity(id, username, instance string) *remoteActor {
	h.t.Helper()
	actor := h.newRemoteActor(id, map[string]any{
		"type":              "Group",
		"id":                id,
		"preferredUsername": username,
		"inbox":             id + "/inbox",
		"published":         "2024-01-01T00:00:00.000000Z",
	})
	ctx := context.Background()
	_, err := h.communities.UpsertCommunity(ctx, store.Community{
		APGroupID:         id,
		DID:               testDIDFor(username, instance),
		PreferredUsername: username,
		Instance:          instance,
	})
	require.NoError(h.t, err)
	require.NoError(h.t, h.communities.SetFollowState(ctx, id, store.FollowStateAccepted))
	return actor
}

// TestAnnounceCreatePageEndToEnd is the definition-of-done flow: subscribe
// → Accept → Announce{Create{Page}} delivered by the fake Lemmy → the post
// record is visible on the task-04 firehose.
func TestAnnounceCreatePageEndToEnd(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()

	status := h.deliver(group, loadFixture(t, "announce_create_page_lemmy_world.json"))
	require.Equal(t, http.StatusAccepted, status)
	h.drain()

	// The event processed cleanly.
	event, err := h.events.GetEvent(context.Background(),
		"https://lemmy.world/activities/announce/create/6a91b0d9-c1e5-45d6-be8b-ce248306867e")
	require.NoError(t, err)
	require.NotNil(t, event.ProcessedAt)
	assert.Empty(t, event.Error)

	// The post is mapped and lives in the community repo.
	mapping, err := h.objects.GetByAPID(context.Background(), pageID)
	require.NoError(t, err)
	communityDID := testDIDFor("technology", "lemmy.world")
	assert.Equal(t, communityDID, mapping.DID)
	assert.Equal(t, materialize.CollectionPost, mapping.Collection)

	// ... and is visible via the firehose (task 04 reads the same log).
	var postOps int
	for _, path := range h.firehoseOps() {
		if strings.HasPrefix(path, materialize.CollectionPost+"/") {
			postOps++
		}
	}
	assert.Equal(t, 1, postOps, "the post commit must be on the firehose")
}

// TestAnnounceFromUnfollowedCommunityDropped: content pushed by an actor
// the bridge never subscribed to is skipped, not materialized.
func TestAnnounceFromUnfollowedCommunityDropped(t *testing.T) {
	h := newHarness(t)
	// The group actor exists (and can sign), but no subscription happened.
	group := h.technologyGroup()
	h.serveLemmyWorldContent()

	status := h.deliver(group, loadFixture(t, "announce_create_page_lemmy_world.json"))
	require.Equal(t, http.StatusAccepted, status)
	h.drain()

	event, err := h.events.GetEvent(context.Background(),
		"https://lemmy.world/activities/announce/create/6a91b0d9-c1e5-45d6-be8b-ce248306867e")
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt, "skips are processed, not retried")
	_, err = h.objects.GetByAPID(context.Background(), pageID)
	assert.True(t, errors.IsNotFound(err), "unfollowed content must not be materialized")
}

// TestOutOfOrderComments: a child comment arrives before its parent — the
// parent-fetch path materializes the whole chain, and the later parent
// delivery is an idempotent no-op. Both comments land.
func TestOutOfOrderComments(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()

	parentID := "https://lemmy.world/comment/1001"
	childID := "https://lemmy.world/comment/1002"
	author := "https://lemmy.world/u/threadster"
	h.serveObject("/u/threadster", person(author, "threadster", nil))
	parentDoc := note(parentID, author, pageID, "parent comment", "2026-07-07T04:00:00.000000Z")
	childDoc := note(childID, author, parentID, "child comment", "2026-07-07T04:05:00.000000Z")
	h.serveObject("/comment/1001", parentDoc)
	h.serveObject("/comment/1002", childDoc)

	announce := func(id string, inner map[string]any) map[string]any {
		return map[string]any{
			"id":       id,
			"type":     "Announce",
			"actor":    groupID,
			"to":       []any{ap.PublicAudience},
			"audience": groupID,
			"object": map[string]any{
				"id":       id + "/create",
				"type":     "Create",
				"actor":    author,
				"audience": groupID,
				"object":   inner,
			},
		}
	}

	// Child first (out of order): the ancestor walk fetches the parent
	// comment and the page.
	require.Equal(t, http.StatusAccepted,
		h.deliver(group, announce("https://lemmy.world/activities/announce/child", childDoc)))
	// Parent second.
	require.Equal(t, http.StatusAccepted,
		h.deliver(group, announce("https://lemmy.world/activities/announce/parent", parentDoc)))
	h.drain()

	ctx := context.Background()
	parentMapping, err := h.objects.GetByAPID(ctx, parentID)
	require.NoError(t, err, "parent must land via the ancestor-fetch path")
	childMapping, err := h.objects.GetByAPID(ctx, childID)
	require.NoError(t, err)
	pageMapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err, "the thread's page is materialized as the root")

	// The child's reply refs point at the parent and root at the page.
	record, _, err := h.manager.GetRecord(ctx, childMapping.DID, childMapping.Collection, childMapping.RKey)
	require.NoError(t, err)
	reply, ok := record["reply"].(map[string]any)
	require.True(t, ok)
	parentRef, _ := reply["parent"].(map[string]any)
	rootRef, _ := reply["root"].(map[string]any)
	assert.Equal(t, parentMapping.ATURI, parentRef["uri"])
	assert.Equal(t, pageMapping.ATURI, rootRef["uri"])

	// Both processed, exactly one comment record each (idempotent re-put).
	var commentOps int
	for _, path := range h.firehoseOps() {
		if strings.HasPrefix(path, materialize.CollectionComment+"/") {
			commentOps++
		}
	}
	assert.Equal(t, 2, commentOps, "each comment commits exactly once")
}

// TestAnnounceLikeRoutedToVotes: Announce{Like} goes to the vote-aggregator
// seam (task 07), not the materializer.
func TestAnnounceLikeRoutedToVotes(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()

	status := h.deliver(group, loadFixture(t, "announce_like.json"))
	require.Equal(t, http.StatusAccepted, status)
	h.drain()

	require.Len(t, h.votes.applied, 1)
	assert.Equal(t, "Like "+pageID, h.votes.applied[0])
}

// TestEchoSuppression: an inbound activity whose object maps to a record
// the bridge itself created (origin=bridge) is dropped.
func TestEchoSuppression(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	ctx := context.Background()

	// Simulate a future write-side record the bridge emitted for this AP id.
	sum, err := multihash.Sum([]byte("bridge-emitted record"), multihash.SHA2_256, -1)
	require.NoError(t, err)
	communityDID := testDIDFor("technology", "lemmy.world")
	_, err = h.objects.PutMapping(ctx, store.APObjectMapping{
		APID:           pageID,
		APType:         "Page",
		OriginInstance: "lemmy.world",
		Origin:         store.OriginBridge,
		DID:            communityDID,
		Collection:     materialize.CollectionPost,
		RKey:           "3lbzzzzzzzz2a",
		CID:            cid.NewCidV1(cid.DagCBOR, sum).String(),
	})
	require.NoError(t, err)

	opsBefore := len(h.firehoseOps())
	status := h.deliver(group, loadFixture(t, "announce_create_page_lemmy_world.json"))
	require.Equal(t, http.StatusAccepted, status)
	h.drain()

	event, err := h.events.GetEvent(ctx,
		"https://lemmy.world/activities/announce/create/6a91b0d9-c1e5-45d6-be8b-ce248306867e")
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt, "echoes are processed (skipped), not retried")
	assert.Equal(t, opsBefore, len(h.firehoseOps()), "an echo must not produce commits")

	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	assert.Equal(t, store.OriginBridge, mapping.Origin, "the bridge-origin mapping is untouched")
}

// TestCreateAfterDeleteTombstone closes the task-05 gap: a Delete arriving
// for a never-materialized object must prevent a later Create from
// resurrecting it; Undo{Delete} restores.
func TestCreateAfterDeleteTombstone(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	author := h.newRemoteActor(personID, person(personID, "LeftLeaningFreedomFighters", nil))
	ctx := context.Background()

	// 1. Delete arrives FIRST (out of order), for an object never seen.
	deleteActivity := loadFixture(t, "delete_page.json")
	require.Equal(t, http.StatusAccepted, h.deliver(author, deleteActivity))
	h.drain()

	tombstoned, err := h.tombstones.Exists(ctx, pageID)
	require.NoError(t, err)
	assert.True(t, tombstoned, "a delete of an unseen id must leave a tombstone marker")

	// 2. The Create arrives late: it must NOT materialize.
	require.Equal(t, http.StatusAccepted,
		h.deliver(group, loadFixture(t, "announce_create_page_lemmy_world.json")))
	h.drain()
	_, err = h.objects.GetByAPID(ctx, pageID)
	assert.True(t, errors.IsNotFound(err), "create-after-delete must not resurrect the object")

	// 3. Undo{Delete}: the origin restored the post; the bridge re-fetches
	// and materializes it.
	require.Equal(t, http.StatusAccepted, h.deliver(author, map[string]any{
		"id":     "https://lemmy.world/activities/undo/delete-1",
		"type":   "Undo",
		"actor":  personID,
		"object": deleteActivity,
	}))
	h.drain()

	tombstoned, err = h.tombstones.Exists(ctx, pageID)
	require.NoError(t, err)
	assert.False(t, tombstoned, "undo must clear the tombstone marker")
	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err, "the restored object must be materialized")
	assert.False(t, mapping.IsDeleted())
}

// TestDeleteMaterializedObject: the normal delete flow — record removed,
// mapping soft-deleted, re-delivered Create cannot resurrect it.
func TestDeleteMaterializedObject(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	author := h.newRemoteActor(personID, person(personID, "LeftLeaningFreedomFighters", nil))
	ctx := context.Background()

	announce := loadFixture(t, "announce_create_page_lemmy_world.json")
	require.Equal(t, http.StatusAccepted, h.deliver(group, announce))
	h.drain()
	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)

	require.Equal(t, http.StatusAccepted, h.deliver(author, loadFixture(t, "delete_page.json")))
	h.drain()

	mapping, err = h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	assert.True(t, mapping.IsDeleted(), "the mapping must be soft-deleted")
	_, _, err = h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	assert.Error(t, err, "the record must be deleted from the repo")

	// A re-delivered Create (new activity id, same object) must not revive it.
	announce["id"] = "https://lemmy.world/activities/announce/create/redelivery"
	require.Equal(t, http.StatusAccepted, h.deliver(group, announce))
	h.drain()
	mapping, err = h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	assert.True(t, mapping.IsDeleted(), "a re-delivered create must not resurrect deleted content")
}

// TestCrossAuthorityDeleteDropped: a bare Delete signed by an actor on a
// different instance than the object is unauthorized and dropped.
func TestCrossAuthorityDeleteDropped(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	mallory := h.newRemoteActor("https://evil.example/u/mallory", person("https://evil.example/u/mallory", "mallory", nil))
	ctx := context.Background()

	require.Equal(t, http.StatusAccepted,
		h.deliver(group, loadFixture(t, "announce_create_page_lemmy_world.json")))
	h.drain()

	require.Equal(t, http.StatusAccepted, h.deliver(mallory, map[string]any{
		"id":     "https://evil.example/activities/delete/1",
		"type":   "Delete",
		"actor":  mallory.id,
		"object": pageID,
	}))
	h.drain()

	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	assert.False(t, mapping.IsDeleted(), "a cross-authority delete must be dropped")
}

// TestNobridgeCommentDropped is the DoD consent check: a comment whose
// author opted out via #nobridge is dropped with the reason logged (the
// skip), and no identity is minted for them.
func TestNobridgeCommentDropped(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	ctx := context.Background()

	optOutID := "https://lemmy.world/u/optout"
	h.serveObject("/u/optout", person(optOutID, "optout", map[string]any{
		"summary": "<p>please leave me alone #nobridge</p>",
	}))
	commentID := "https://lemmy.world/comment/2001"
	commentDoc := note(commentID, optOutID, pageID, "my private opinion", "2026-07-07T06:00:00.000000Z")
	h.serveObject("/comment/2001", commentDoc)

	require.Equal(t, http.StatusAccepted, h.deliver(group, map[string]any{
		"id":       "https://lemmy.world/activities/announce/nobridge-comment",
		"type":     "Announce",
		"actor":    groupID,
		"audience": groupID,
		"object": map[string]any{
			"id":       "https://lemmy.world/activities/create/nobridge-comment",
			"type":     "Create",
			"actor":    optOutID,
			"audience": groupID,
			"object":   commentDoc,
		},
	}))
	h.drain()

	event, err := h.events.GetEvent(ctx, "https://lemmy.world/activities/announce/nobridge-comment")
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt, "the skip is logged and the event completed")

	_, err = h.objects.GetByAPID(ctx, commentID)
	assert.True(t, errors.IsNotFound(err), "the comment must not be materialized")
	_, err = h.actors.GetByAPActorID(ctx, optOutID)
	assert.True(t, errors.IsNotFound(err), "no identity is minted for an opted-out actor")
}

// TestConsentTransitionOnProfileUpdate: a bridged author adds #nobridge;
// the self-signed Update{Person} scrubs their content and flips consent
// (reversible nobridge, not deleted).
func TestConsentTransitionOnProfileUpdate(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	author := h.newRemoteActor(personID, person(personID, "LeftLeaningFreedomFighters", nil))
	ctx := context.Background()

	require.Equal(t, http.StatusAccepted,
		h.deliver(group, loadFixture(t, "announce_create_page_lemmy_world.json")))
	h.drain()
	actorRow, err := h.actors.GetByAPActorID(ctx, personID)
	require.NoError(t, err)
	require.Equal(t, store.ConsentStateOK, actorRow.ConsentState)

	// The author edits their profile to carry #nobridge; their instance
	// delivers Update{Person} signed by the author.
	updated := person(personID, "LeftLeaningFreedomFighters", map[string]any{
		"summary": "<p>done with bridges #nobridge</p>",
	})
	require.Equal(t, http.StatusAccepted, h.deliver(author, map[string]any{
		"id":     "https://lemmy.world/activities/update/person-1",
		"type":   "Update",
		"actor":  personID,
		"object": updated,
	}))
	h.drain()

	actorRow, err = h.actors.GetByAPActorID(ctx, personID)
	require.NoError(t, err)
	assert.Equal(t, store.ConsentStateNoBridge, actorRow.ConsentState,
		"profile update with #nobridge must flip consent (reversibly)")

	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	assert.True(t, mapping.IsDeleted(), "existing content must be scrubbed on opt-out")
}

// TestDeleteActorTombstonesRepo: Delete(Actor) scrubs and terminally
// tombstones the bridged actor.
func TestDeleteActorTombstonesRepo(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	author := h.newRemoteActor(personID, person(personID, "LeftLeaningFreedomFighters", nil))
	ctx := context.Background()

	require.Equal(t, http.StatusAccepted,
		h.deliver(group, loadFixture(t, "announce_create_page_lemmy_world.json")))
	h.drain()

	require.Equal(t, http.StatusAccepted, h.deliver(author, map[string]any{
		"id":     "https://lemmy.world/activities/delete/actor-1",
		"type":   "Delete",
		"actor":  personID,
		"object": personID,
	}))
	h.drain()

	actorRow, err := h.actors.GetByAPActorID(ctx, personID)
	require.NoError(t, err)
	assert.Equal(t, store.ConsentStateDeleted, actorRow.ConsentState, "Delete(Actor) is terminal")
	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	assert.True(t, mapping.IsDeleted(), "the deleted actor's content is scrubbed")
}

// TestForgedEmbeddedContentRefetched: content embedded in an announce whose
// id lives on ANOTHER instance is re-fetched from its origin — the
// delivered (potentially forged) body is never trusted.
func TestForgedEmbeddedContentRefetched(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	ctx := context.Background()

	// The announce embeds a lemmy.zip comment with FORGED content; the real
	// note (served by lemmy.zip) says something else. Chain fixtures for the
	// ancestor walk: parent note on sh.itjust.works replying to the page.
	noteDoc := loadFixture(t, "note_lemmy_zip.json")
	h.serveObject("/comment/27485395", noteDoc)
	h.serveObject("/u/tixooo", person(noteAuthor, "tixooo", nil))
	h.serveObject("/comment/26248018",
		note(parentNote, parentActor, pageID, "the parent comment", "2026-07-07T04:30:00.000000Z"))
	h.serveObject("/u/DemandtheOxfordComma", person(parentActor, "DemandtheOxfordComma", nil))

	announce := loadFixture(t, "announce_create_note.json")
	forged := announce["object"].(map[string]any)["object"].(map[string]any)
	forged["content"] = "<p>FORGED BODY</p>"
	forged["source"] = map[string]any{"content": "FORGED BODY", "mediaType": "text/markdown"}

	require.Equal(t, http.StatusAccepted, h.deliver(group, announce))
	h.drain()

	mapping, err := h.objects.GetByAPID(ctx, noteID)
	require.NoError(t, err, "the comment must land (from its origin)")
	record, _, err := h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	require.NoError(t, err)
	content, _ := record["content"].(string)
	assert.NotContains(t, content, "FORGED", "the forged embedded body must be discarded")
	assert.Contains(t, content, "theme for all of human history", "the origin's body is used")
}

// TestAnnouncedDeleteOfOwnPost (Finding 1, positive): a followed community
// announcing a Delete of its OWN post (same host, same authority) is
// authorized and soft-deletes the record.
func TestAnnouncedDeleteOfOwnPost(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	ctx := context.Background()

	require.Equal(t, http.StatusAccepted,
		h.deliver(group, loadFixture(t, "announce_create_page_lemmy_world.json")))
	h.drain()
	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	require.False(t, mapping.IsDeleted())

	require.Equal(t, http.StatusAccepted, h.deliver(group, map[string]any{
		"id":       "https://lemmy.world/activities/announce/delete-own",
		"type":     "Announce",
		"actor":    groupID,
		"audience": groupID,
		"object": map[string]any{
			"id":     "https://lemmy.world/activities/delete/own-post",
			"type":   "Delete",
			"actor":  groupID,
			"object": pageID,
		},
	}))
	h.drain()

	mapping, err = h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	assert.True(t, mapping.IsDeleted(), "a community may delete its own announced post")
}

// TestCrossAuthorityAnnouncedDeleteDropped (Finding 1, negative): a followed
// community on one instance cannot delete content hosted on ANOTHER instance
// by wrapping the Delete in an Announce. The old code skipped all authority
// checks whenever announcer != "".
func TestCrossAuthorityAnnouncedDeleteDropped(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	ctx := context.Background()

	require.Equal(t, http.StatusAccepted,
		h.deliver(group, loadFixture(t, "announce_create_page_lemmy_world.json")))
	h.drain()

	// A different subscribed community, on evil.example, announces a Delete of
	// the lemmy.world post — content it does not host.
	evil := h.followedCommunity("https://evil.example/c/foo", "foo", "evil.example")
	require.Equal(t, http.StatusAccepted, h.deliver(evil, map[string]any{
		"id":       "https://evil.example/activities/announce/delete-victim",
		"type":     "Announce",
		"actor":    "https://evil.example/c/foo",
		"audience": "https://evil.example/c/foo",
		"object": map[string]any{
			"id":     "https://evil.example/activities/delete/victim",
			"type":   "Delete",
			"actor":  "https://evil.example/c/foo",
			"object": pageID,
		},
	}))
	h.drain()

	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	assert.False(t, mapping.IsDeleted(), "a community must not delete another instance's content")
	tombstoned, err := h.tombstones.Exists(ctx, pageID)
	require.NoError(t, err)
	assert.False(t, tombstoned, "an unauthorized announced delete must not record a tombstone")
}

// TestAnnouncedActorDeleteOfCoHostedActorDropped (Finding 1, actor path): even
// on its own authority, a community may delete only ITSELF, never a co-hosted
// OTHER actor whose bridged presence spans other communities (the terminal
// DeleteActor scrub).
func TestAnnouncedActorDeleteOfCoHostedActorDropped(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	ctx := context.Background()

	// The post author is bridged as a co-hosted actor on lemmy.world.
	require.Equal(t, http.StatusAccepted,
		h.deliver(group, loadFixture(t, "announce_create_page_lemmy_world.json")))
	h.drain()
	actorRow, err := h.actors.GetByAPActorID(ctx, personID)
	require.NoError(t, err)
	require.Equal(t, store.ConsentStateOK, actorRow.ConsentState)

	require.Equal(t, http.StatusAccepted, h.deliver(group, map[string]any{
		"id":       "https://lemmy.world/activities/announce/delete-cohost",
		"type":     "Announce",
		"actor":    groupID,
		"audience": groupID,
		"object": map[string]any{
			"id":     "https://lemmy.world/activities/delete/cohost-actor",
			"type":   "Delete",
			"actor":  groupID,
			"object": personID,
		},
	}))
	h.drain()

	actorRow, err = h.actors.GetByAPActorID(ctx, personID)
	require.NoError(t, err)
	assert.Equal(t, store.ConsentStateOK, actorRow.ConsentState,
		"a community must not tombstone a co-hosted actor via announce")
	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	assert.False(t, mapping.IsDeleted(), "the co-hosted actor's content stays live")
}

// TestBareProfileUpdateForUnknownActorDropped (Finding 2): a self-signed evil
// actor sends a bare Update{Person} naming an unknown target IRI. The old code
// reached RefreshActor, which fetched the target (SSRF/fetch-oracle) and minted
// a PLC DID. The refresh-only gate drops it before any fetch or mint.
func TestBareProfileUpdateForUnknownActorDropped(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	evil := h.newRemoteActor("https://evil.example/u/mallory",
		person("https://evil.example/u/mallory", "mallory", nil))

	// The attacker-chosen target: an unknown actor on a victim host, served
	// with a hit counter so we can prove it is never fetched.
	target := "https://victim.example/u/target"
	h.serveObject("/u/target", person(target, "target", nil))
	mintsBefore := h.minter.mintCount()

	require.Equal(t, http.StatusAccepted, h.deliver(evil, map[string]any{
		"id":     "https://evil.example/activities/update/ssrf",
		"type":   "Update",
		"actor":  evil.id,
		"object": person(target, "target", nil),
	}))
	h.drain()

	assert.Equal(t, 0, h.hitCount("/u/target"), "the unknown target IRI must never be fetched")
	assert.Equal(t, mintsBefore, h.minter.mintCount(), "no identity minted for an unknown actor")
	_, err := h.actors.GetByAPActorID(ctx, target)
	assert.True(t, errors.IsNotFound(err), "the unknown target must not be bridged")
}

// TestAnnouncedObjectForDifferentCommunityDropped (Finding 3): a followed
// community may fan out only its OWN content. An Announce whose embedded object
// names a different community (even same-host) must not inject into that
// community's repo.
func TestAnnouncedObjectForDifferentCommunityDropped(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	ctx := context.Background()

	otherCommunity := "https://lemmy.world/c/otherthing"
	injectedID := "https://lemmy.world/post/88888"
	author := "https://lemmy.world/u/injector"
	h.serveObject("/u/injector", person(author, "injector", nil))
	page := map[string]any{
		"type":         "Page",
		"id":           injectedID,
		"attributedTo": author,
		"to":           []any{ap.PublicAudience},
		"audience":     otherCommunity, // NOT the announcing community
		"name":         "injected post",
		"source":       map[string]any{"content": "injected", "mediaType": "text/markdown"},
		"published":    "2026-07-07T07:00:00.000000Z",
	}
	h.serveObject("/post/88888", page)

	require.Equal(t, http.StatusAccepted, h.deliver(group, map[string]any{
		"id":       "https://lemmy.world/activities/announce/inject",
		"type":     "Announce",
		"actor":    groupID,
		"audience": groupID,
		"object": map[string]any{
			"id":     "https://lemmy.world/activities/create/inject",
			"type":   "Create",
			"actor":  author,
			"object": page,
		},
	}))
	h.drain()

	_, err := h.objects.GetByAPID(ctx, injectedID)
	assert.True(t, errors.IsNotFound(err), "content for a non-announced community must be dropped")
	_, err = h.communities.GetByAPGroupID(ctx, otherCommunity)
	assert.True(t, errors.IsNotFound(err), "the other community must not be bridged")
}

// TestUndoDeleteRollsBackWhenRematerializeSkips (Finding 4): if the restore's
// re-materialization is declined (a skip), the compensation must re-soft-delete
// the mapping and re-record the tombstone — never leave a live mapping without
// a record.
func TestUndoDeleteRollsBackWhenRematerializeSkips(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	author := h.newRemoteActor(personID, person(personID, "LeftLeaningFreedomFighters", nil))
	ctx := context.Background()

	postID := "https://lemmy.world/post/70001"
	buildPage := func(withCommunity bool) map[string]any {
		p := map[string]any{
			"type":         "Page",
			"id":           postID,
			"attributedTo": personID,
			"to":           []any{ap.PublicAudience},
			"name":         "a post",
			"source":       map[string]any{"content": "body", "mediaType": "text/markdown"},
			"published":    "2026-07-07T08:00:00.000000Z",
		}
		if withCommunity {
			p["audience"] = groupID
		}
		return p
	}
	h.serveObject("/post/70001", buildPage(true))

	require.Equal(t, http.StatusAccepted, h.deliver(group, map[string]any{
		"id":       "https://lemmy.world/activities/announce/create-70001",
		"type":     "Announce",
		"actor":    groupID,
		"audience": groupID,
		"object": map[string]any{
			"id":       "https://lemmy.world/activities/create/70001",
			"type":     "Create",
			"actor":    personID,
			"audience": groupID,
			"object":   buildPage(true),
		},
	}))
	h.drain()
	mapping, err := h.objects.GetByAPID(ctx, postID)
	require.NoError(t, err)
	require.False(t, mapping.IsDeleted())

	// Delete it (bare, on the author's own authority).
	require.Equal(t, http.StatusAccepted, h.deliver(author, map[string]any{
		"id":     "https://lemmy.world/activities/delete/70001",
		"type":   "Delete",
		"actor":  personID,
		"object": postID,
	}))
	h.drain()
	mapping, err = h.objects.GetByAPID(ctx, postID)
	require.NoError(t, err)
	require.True(t, mapping.IsDeleted())
	tombstoned, err := h.tombstones.Exists(ctx, postID)
	require.NoError(t, err)
	require.True(t, tombstoned)

	// The origin re-serves the post but now WITHOUT a community, so
	// re-materialization skips ("post names no community").
	h.serveObject("/post/70001", buildPage(false))

	require.Equal(t, http.StatusAccepted, h.deliver(author, map[string]any{
		"id":    "https://lemmy.world/activities/undo/70001",
		"type":  "Undo",
		"actor": personID,
		"object": map[string]any{
			"id":     "https://lemmy.world/activities/delete/70001",
			"type":   "Delete",
			"actor":  personID,
			"object": postID,
		},
	}))
	h.drain()

	mapping, err = h.objects.GetByAPID(ctx, postID)
	require.NoError(t, err)
	assert.True(t, mapping.IsDeleted(), "a declined restore must re-soft-delete the mapping")
	tombstoned, err = h.tombstones.Exists(ctx, postID)
	require.NoError(t, err)
	assert.True(t, tombstoned, "a declined restore must retain the tombstone")
}

// TestLateAcceptAfterUnfollowIgnored (Finding 5): after an operator
// unsubscribes (state → none), a late/re-delivered Accept must not flip the
// community back to accepted nor trigger a fresh backfill.
func TestLateAcceptAfterUnfollowIgnored(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	ctx := context.Background()

	// The real Accept during subscribe triggered exactly one backfill.
	require.Equal(t, 1, h.backfills.count())

	// The operator unsubscribes.
	require.NoError(t, h.communities.SetFollowState(ctx, groupID, store.FollowStateNone))

	// Lemmy re-delivers a late Accept for the follow we already withdrew.
	require.Equal(t, http.StatusAccepted, h.deliver(group, map[string]any{
		"id":    "https://lemmy.world/activities/accept/follow-late",
		"type":  "Accept",
		"actor": groupID,
		"object": map[string]any{
			"id":     "https://lemmy.world/activities/follow/late",
			"type":   "Follow",
			"actor":  h.service.ID,
			"object": groupID,
		},
	}))
	h.drain()

	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	assert.Equal(t, store.FollowStateNone, community.FollowState,
		"a late Accept must not re-subscribe an unfollowed community")
	assert.Equal(t, 1, h.backfills.count(), "no fresh backfill for an unfollowed community")
}
