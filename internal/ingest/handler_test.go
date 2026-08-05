package ingest

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// mintCount returns how many identities the fake minter has minted so far.
func (f *fakeMinter) mintCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mints
}

// announceCreate delivers Announce{Create{obj}} from a followed community —
// the shape Lemmy fans out — and drains the queue.
func (h *harness) announceCreate(group *remoteActor, activityID string, obj map[string]any) {
	h.t.Helper()
	require.Equal(h.t, http.StatusAccepted, h.deliver(group, map[string]any{
		"id":       activityID,
		"type":     "Announce",
		"actor":    group.id,
		"audience": group.id,
		"object": map[string]any{
			"id":       activityID + "/create",
			"type":     "Create",
			"actor":    obj["attributedTo"],
			"audience": obj["audience"],
			"object":   obj,
		},
	}))
	h.drain()
}

// announceDelete delivers Announce{Delete{targetID}} from a followed
// community, attributed to actor, and drains the queue.
func (h *harness) announceDelete(group *remoteActor, activityID, actor, targetID string) {
	h.t.Helper()
	require.Equal(h.t, http.StatusAccepted, h.deliver(group, map[string]any{
		"id":       activityID,
		"type":     "Announce",
		"actor":    group.id,
		"audience": group.id,
		"object": map[string]any{
			"id":     activityID + "/delete",
			"type":   "Delete",
			"actor":  actor,
			"object": targetID,
		},
	}))
	h.drain()
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

// tombstoneAnnouncers lists the raw marker rows for an ap id. ExistsFor
// cannot answer "whose row is it": a global marker is visible in every scope,
// so it masks exactly the per-announcer removals the scoping rules are about.
func (h *harness) tombstoneAnnouncers(apID string) []string {
	h.t.Helper()
	rows, err := testutil.DB(h.t).Query(
		`SELECT announcer FROM ap_tombstones WHERE ap_id = $1 ORDER BY announcer`, apID)
	require.NoError(h.t, err)
	defer func() { require.NoError(h.t, rows.Close()) }()
	announcers := []string{}
	for rows.Next() {
		var announcer string
		require.NoError(h.t, rows.Scan(&announcer))
		announcers = append(announcers, announcer)
	}
	require.NoError(h.t, rows.Err())
	return announcers
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

// TestBareVoteCrossAuthorityDropped: the inbox binds only the TOP-LEVEL
// activity's actor to the HTTP signature, so vote dispatch must not let a
// signer submit or retract votes attributed to other instances' users. A
// bare Like claiming a foreign actor dies at the inbox binding; a bare
// Undo{Like} carries the unverified actor INSIDE the undo, so the dispatch
// layer drops it — as a processed skip, never a retry that would wedge the
// ordering key.
func TestBareVoteCrossAuthorityDropped(t *testing.T) {
	h := newHarness(t)
	mallory := h.newRemoteActor("https://evil.example/u/mallory",
		person("https://evil.example/u/mallory", "mallory", nil))
	ctx := context.Background()

	// A bare Like attributed to a victim on another instance never clears
	// the inbox's actor/signer authority binding.
	status := h.deliver(mallory, map[string]any{
		"id":     "https://evil.example/activities/like/1",
		"type":   "Like",
		"actor":  "https://lemmy.world/u/victim",
		"object": pageID,
	})
	assert.Equal(t, http.StatusForbidden, status,
		"a bare vote with a cross-authority actor must be rejected at the inbox")

	// The Undo passes the inbox (the OUTER actor is mallory's own) but
	// attributes the undone vote to the victim: dropped in dispatch.
	require.Equal(t, http.StatusAccepted, h.deliver(mallory, map[string]any{
		"id":    "https://evil.example/activities/undo/1",
		"type":  "Undo",
		"actor": mallory.id,
		"object": map[string]any{
			"id":     "https://lemmy.world/activities/like/1",
			"type":   "Like",
			"actor":  "https://lemmy.world/u/victim",
			"object": pageID,
		},
	}))
	h.drain()

	event, err := h.events.GetEvent(ctx, "https://evil.example/activities/undo/1")
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt, "the drop is a processed skip, never a retry")
	assert.Empty(t, h.votes.applied)
	assert.Empty(t, h.votes.retracted, "a cross-authority undo must never reach the aggregator")
}

// TestBareVoteSameAuthorityRouted: bare votes and undos attributed to a user
// on the signer's own instance reach the aggregator (host granularity — the
// instance is the trust unit, as with bare Delete).
func TestBareVoteSameAuthorityRouted(t *testing.T) {
	h := newHarness(t)
	voter := h.newRemoteActor(personID, person(personID, "LeftLeaningFreedomFighters", nil))

	require.Equal(t, http.StatusAccepted, h.deliver(voter, map[string]any{
		"id":     "https://lemmy.world/activities/like/bare-1",
		"type":   "Like",
		"actor":  personID,
		"object": pageID,
	}))
	require.Equal(t, http.StatusAccepted, h.deliver(voter, map[string]any{
		"id":    "https://lemmy.world/activities/undo/bare-1",
		"type":  "Undo",
		"actor": personID,
		"object": map[string]any{
			"id":     "https://lemmy.world/activities/like/bare-1",
			"type":   "Like",
			"actor":  personID,
			"object": pageID,
		},
	}))
	h.drain()

	require.Len(t, h.votes.applied, 1)
	assert.Equal(t, "Like "+pageID, h.votes.applied[0])
	require.Len(t, h.votes.retracted, 1)
	assert.Equal(t, "Like "+pageID, h.votes.retracted[0])
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
// resurrecting it; Undo{Delete} retracts the marker so a fresh Create lands
// again. The undo itself does NOT materialize — there is no mapping to
// restore, and fetching an unmapped id off an undo is the mint oracle
// handleUndoDelete refuses.
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

	tombstoned, err := h.tombstones.ExistsFor(ctx, pageID, "")
	require.NoError(t, err)
	assert.True(t, tombstoned, "a delete of an unseen id must leave a tombstone marker")

	// 2. The Create arrives late: it must NOT materialize.
	require.Equal(t, http.StatusAccepted,
		h.deliver(group, loadFixture(t, "announce_create_page_lemmy_world.json")))
	h.drain()
	_, err = h.objects.GetByAPID(ctx, pageID)
	assert.True(t, errors.IsNotFound(err), "create-after-delete must not resurrect the object")

	// 3. Undo{Delete}: the origin retracted the delete. Nothing was ever
	// materialized, so the undo only clears the marker.
	require.Equal(t, http.StatusAccepted, h.deliver(author, map[string]any{
		"id":     "https://lemmy.world/activities/undo/delete-1",
		"type":   "Undo",
		"actor":  personID,
		"object": deleteActivity,
	}))
	h.drain()

	tombstoned, err = h.tombstones.ExistsFor(ctx, pageID, "")
	require.NoError(t, err)
	assert.False(t, tombstoned, "undo must clear the tombstone marker")
	_, err = h.objects.GetByAPID(ctx, pageID)
	assert.True(t, errors.IsNotFound(err),
		"an undo for an unmapped id restores nothing; only the marker is retracted")

	// 4. ...and with the marker gone, a re-delivered Create materializes the
	// post normally — which is how restored-before-first-sight content lands.
	announce := loadFixture(t, "announce_create_page_lemmy_world.json")
	announce["id"] = "https://lemmy.world/activities/announce/create/after-undo"
	require.Equal(t, http.StatusAccepted, h.deliver(group, announce))
	h.drain()
	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err, "a create after the marker is retracted must materialize")
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
// announcing a Delete of its OWN post is authorized and soft-deletes the
// record. (Same host here, but that is incidental: the post is mapped into
// the announcing community's repo, and repo membership — not host authority
// — is what authorizes the delete.)
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
// community cannot delete content mapped into a DIFFERENT community's repo
// by wrapping the Delete in an Announce. (Authorization is repo membership,
// not host authority: the victim post here is mapped into the technology
// community's repo, so evil.example's announce drops.)
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
	tombstoned, err := h.tombstones.ExistsFor(ctx, pageID, evil.id)
	require.NoError(t, err)
	assert.False(t, tombstoned, "an unauthorized announced delete must not record a tombstone")
}

// TestAnnouncedDeleteOfRemoteAuthorPost: the normal remote-author federation
// shape — a post whose ap_id lives on the AUTHOR's instance, posted into a
// community on another instance, deleted via the community's Announce. The
// old same-authority rule dropped every such delete (prod incident: a
// jlai.lu author's deleted post in fediverse@lemmy.world survived as a
// duplicate); membership in the announcing community's repo authorizes it.
func TestAnnouncedDeleteOfRemoteAuthorPost(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	ctx := context.Background()

	const (
		remotePersonID = "https://sopuli.example/u/remoteAuthor"
		remotePageID   = "https://sopuli.example/post/777"
	)
	h.newRemoteActor(remotePersonID, person(remotePersonID, "remoteAuthor", nil))
	h.serveObject("/post/777", map[string]any{
		"type":         "Page",
		"id":           remotePageID,
		"attributedTo": remotePersonID,
		"to":           []any{groupID, ap.PublicAudience},
		"name":         "cross-instance post",
		"audience":     groupID,
		"published":    "2026-07-07T03:27:37.028201Z",
	})

	// The community announces the Create; the embedded object is cross-
	// authority to the signer, so the bridge re-fetches it from its origin.
	require.Equal(t, http.StatusAccepted, h.deliver(group, map[string]any{
		"id":       "https://lemmy.world/activities/announce/create/remote-author",
		"type":     "Announce",
		"actor":    groupID,
		"audience": groupID,
		"object":   map[string]any{"id": remotePageID},
	}))
	h.drain()
	mapping, err := h.objects.GetByAPID(ctx, remotePageID)
	require.NoError(t, err)
	require.False(t, mapping.IsDeleted())

	// The author deletes; the delete fans out through the community.
	require.Equal(t, http.StatusAccepted, h.deliver(group, map[string]any{
		"id":       "https://lemmy.world/activities/announce/delete/remote-author",
		"type":     "Announce",
		"actor":    groupID,
		"audience": groupID,
		"object": map[string]any{
			"id":     "https://sopuli.example/activities/delete/777",
			"type":   "Delete",
			"actor":  remotePersonID,
			"object": remotePageID,
		},
	}))
	h.drain()

	mapping, err = h.objects.GetByAPID(ctx, remotePageID)
	require.NoError(t, err)
	assert.True(t, mapping.IsDeleted(),
		"an announced delete of a remote-author post in the community's own repo must apply")
	_, _, err = h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	assert.Error(t, err, "the record must be deleted from the repo")
}

// TestAnnouncedDeleteBeforeCreateCrossAuthority: the prod race — the Delete
// announce is processed while the Create is still materializing (or before
// it arrives at all). An unmapped cross-authority target must still record
// the tombstone marker so the late Create cannot resurrect the object.
func TestAnnouncedDeleteBeforeCreateCrossAuthority(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	ctx := context.Background()

	const (
		remotePersonID = "https://sopuli.example/u/remoteAuthor"
		remotePageID   = "https://sopuli.example/post/778"
	)
	h.newRemoteActor(remotePersonID, person(remotePersonID, "remoteAuthor", nil))
	h.serveObject("/post/778", map[string]any{
		"type":         "Page",
		"id":           remotePageID,
		"attributedTo": remotePersonID,
		"to":           []any{groupID, ap.PublicAudience},
		"name":         "deleted before create landed",
		"audience":     groupID,
		"published":    "2026-07-07T03:27:37.028201Z",
	})

	require.Equal(t, http.StatusAccepted, h.deliver(group, map[string]any{
		"id":       "https://lemmy.world/activities/announce/delete/early",
		"type":     "Announce",
		"actor":    groupID,
		"audience": groupID,
		"object": map[string]any{
			"id":     "https://sopuli.example/activities/delete/778",
			"type":   "Delete",
			"actor":  remotePersonID,
			"object": remotePageID,
		},
	}))
	h.drain()

	tombstoned, err := h.tombstones.ExistsFor(ctx, remotePageID, groupID)
	require.NoError(t, err)
	assert.True(t, tombstoned,
		"an announced delete of an unseen cross-authority id must leave a tombstone marker")

	require.Equal(t, http.StatusAccepted, h.deliver(group, map[string]any{
		"id":       "https://lemmy.world/activities/announce/create/late",
		"type":     "Announce",
		"actor":    groupID,
		"audience": groupID,
		"object":   map[string]any{"id": remotePageID},
	}))
	h.drain()
	_, err = h.objects.GetByAPID(ctx, remotePageID)
	assert.True(t, errors.IsNotFound(err), "create-after-delete must not resurrect the object")
}

// unseenRemotePost serves a never-materialized cross-authority post addressed
// to the technology community — the delete-before-create target shape — and
// returns its (signing-capable) author and id.
func (h *harness) unseenRemotePost(slug string) (*remoteActor, string) {
	h.t.Helper()
	const authorID = "https://sopuli.example/u/remoteAuthor"
	author := h.newRemoteActor(authorID, person(authorID, "remoteAuthor", nil))
	id := "https://sopuli.example/post/" + slug
	h.serveObject("/post/"+slug, map[string]any{
		"type":         "Page",
		"id":           id,
		"attributedTo": authorID,
		"to":           []any{groupID, ap.PublicAudience},
		"name":         "deleted before create landed",
		"audience":     groupID,
		"published":    "2026-07-07T03:27:37.028201Z",
	})
	return author, id
}

// TestAnnouncedDeleteMarkerIsScopedToAnnouncer is the cross-community
// suppression pin. authorizeDelete admits an announced Delete of an UNMAPPED
// id from ANY followed community (that allowance is what closes the
// delete-before-create race), so an unscoped marker would hand every followed
// community a veto over arbitrary ap_ids — including ids belonging to other
// communities — for the whole retention window. The marker is scoped to its
// announcer instead: it holds in that community's own context and nowhere
// else, so a DIFFERENT community's Create for the same id still materializes.
func TestAnnouncedDeleteMarkerIsScopedToAnnouncer(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	ctx := context.Background()
	_, postID := h.unseenRemotePost("779")

	// A second followed community announces a Delete of an id it has nothing
	// to do with — never materialized, so nothing but the marker happens.
	evil := h.followedCommunity("https://evil.example/c/foo", "foo", "evil.example")
	h.announceDelete(evil, "https://evil.example/activities/announce/delete/cross-suppress",
		evil.id, postID)

	tombstoned, err := h.tombstones.ExistsFor(ctx, postID, evil.id)
	require.NoError(t, err)
	require.True(t, tombstoned, "the announcer's own marker is recorded")
	tombstoned, err = h.tombstones.ExistsFor(ctx, postID, groupID)
	require.NoError(t, err)
	assert.False(t, tombstoned, "another community must not see it")

	// The community the post actually belongs to announces its Create.
	require.Equal(t, http.StatusAccepted, h.deliver(group, map[string]any{
		"id":       "https://lemmy.world/activities/announce/create/779",
		"type":     "Announce",
		"actor":    groupID,
		"audience": groupID,
		"object":   map[string]any{"id": postID},
	}))
	h.drain()

	mapping, err := h.objects.GetByAPID(ctx, postID)
	require.NoError(t, err,
		"one community's tombstone marker must not suppress another community's content")
	assert.False(t, mapping.IsDeleted())
	// The marker itself is untouched — it is scoped, not discarded.
	tombstoned, err = h.tombstones.ExistsFor(ctx, postID, evil.id)
	require.NoError(t, err)
	assert.True(t, tombstoned)
}

// TestBareDeleteBeforeCreateSuppressesAnnouncedCreate is the other half of
// the scoping rule, in the same shape: a delete that passed the bare
// same-authority check carries the target id's OWN origin authority, so its
// marker is global and suppresses the late announced Create that the
// community-scoped marker above could not.
func TestBareDeleteBeforeCreateSuppressesAnnouncedCreate(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	ctx := context.Background()
	author, postID := h.unseenRemotePost("780")

	require.Equal(t, http.StatusAccepted, h.deliver(author, map[string]any{
		"id":     "https://sopuli.example/activities/delete/780",
		"type":   "Delete",
		"actor":  author.id,
		"object": postID,
	}))
	h.drain()

	for _, scope := range []string{"", groupID, "https://evil.example/c/foo"} {
		tombstoned, err := h.tombstones.ExistsFor(ctx, postID, scope)
		require.NoError(t, err)
		assert.True(t, tombstoned, "an origin-authorized marker is visible in every scope")
	}

	require.Equal(t, http.StatusAccepted, h.deliver(group, map[string]any{
		"id":       "https://lemmy.world/activities/announce/create/780",
		"type":     "Announce",
		"actor":    groupID,
		"audience": groupID,
		"object":   map[string]any{"id": postID},
	}))
	h.drain()
	_, err := h.objects.GetByAPID(ctx, postID)
	assert.True(t, errors.IsNotFound(err),
		"a global marker must still stop the create-after-delete race")
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
	tombstoned, err := h.tombstones.ExistsFor(ctx, postID, "")
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
	tombstoned, err = h.tombstones.ExistsFor(ctx, postID, "")
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

// TestAnnouncedDeleteOfComment: the comment counterpart of
// TestAnnouncedDeleteOfOwnPost. Comments commit into their AUTHOR's repo (only
// posts land in the community's), so a membership rule read off mapping.DID
// drops EVERY announced comment delete — including this one, where author and
// community share an instance. The thread root, which does live in the
// community's repo, is what authorizes it.
func TestAnnouncedDeleteOfComment(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	ctx := context.Background()

	require.Equal(t, http.StatusAccepted,
		h.deliver(group, loadFixture(t, "announce_create_page_lemmy_world.json")))
	h.drain()

	const (
		commenterID = "https://lemmy.world/u/commenter"
		commentID   = "https://lemmy.world/comment/3001"
	)
	h.serveObject("/u/commenter", person(commenterID, "commenter", nil))
	commentDoc := note(commentID, commenterID, pageID, "a comment", "2026-07-07T05:00:00.000000Z")
	h.serveObject("/comment/3001", commentDoc)
	h.announceCreate(group, "https://lemmy.world/activities/announce/create/3001", commentDoc)

	mapping, err := h.objects.GetByAPID(ctx, commentID)
	require.NoError(t, err)
	require.Equal(t, materialize.CollectionComment, mapping.Collection)
	require.Equal(t, testDIDFor("commenter", "lemmy.world"), mapping.DID,
		"comments live in the author's repo — the premise of this test")
	require.NotEqual(t, testDIDFor("technology", "lemmy.world"), mapping.DID)

	// The author deletes; Lemmy fans the Delete out through the community.
	h.announceDelete(group, "https://lemmy.world/activities/announce/delete/3001",
		commenterID, commentID)

	mapping, err = h.objects.GetByAPID(ctx, commentID)
	require.NoError(t, err)
	assert.True(t, mapping.IsDeleted(), "an announced comment delete must apply")
	_, _, err = h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	assert.Error(t, err, "the comment record must be deleted from the author's repo")
}

// TestAnnouncedDeleteOfRemoteAuthorComment: the same flow with the author on
// ANOTHER instance — the normal federation shape, where neither the comment's
// ap_id host nor its repo belongs to the announcing community. Only the thread
// root ties it to the community.
func TestAnnouncedDeleteOfRemoteAuthorComment(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	ctx := context.Background()

	require.Equal(t, http.StatusAccepted,
		h.deliver(group, loadFixture(t, "announce_create_page_lemmy_world.json")))
	h.drain()

	const (
		remoteCommenterID = "https://sopuli.example/u/sopuliCommenter"
		remoteCommentID   = "https://sopuli.example/comment/9001"
	)
	h.serveObject("/u/sopuliCommenter", person(remoteCommenterID, "sopuliCommenter", nil))
	commentDoc := note(remoteCommentID, remoteCommenterID, pageID,
		"a cross-instance comment", "2026-07-07T05:10:00.000000Z")
	h.serveObject("/comment/9001", commentDoc)
	h.announceCreate(group, "https://lemmy.world/activities/announce/create/9001", commentDoc)

	mapping, err := h.objects.GetByAPID(ctx, remoteCommentID)
	require.NoError(t, err)
	require.Equal(t, testDIDFor("sopuliCommenter", "sopuli.example"), mapping.DID)

	h.announceDelete(group, "https://lemmy.world/activities/announce/delete/9001",
		remoteCommenterID, remoteCommentID)

	mapping, err = h.objects.GetByAPID(ctx, remoteCommentID)
	require.NoError(t, err)
	assert.True(t, mapping.IsDeleted(),
		"a remote author's comment in the community's thread must be deletable by the community")
}

// TestAnnouncedDeleteFromSiblingCommunityDropped pins the tightening: the
// announcer must own the content, not merely share a host with it. A SECOND
// community on the very same instance (lemmy.world) announces deletes of the
// technology community's post and of a comment in its thread — both drop,
// leaving no tombstone. v1's host-authority rule authorized exactly this; a
// "membership OR same host" reading of the new rule would too.
func TestAnnouncedDeleteFromSiblingCommunityDropped(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	ctx := context.Background()

	require.Equal(t, http.StatusAccepted,
		h.deliver(group, loadFixture(t, "announce_create_page_lemmy_world.json")))
	h.drain()

	const (
		commenterID = "https://lemmy.world/u/commenter"
		commentID   = "https://lemmy.world/comment/3002"
	)
	h.serveObject("/u/commenter", person(commenterID, "commenter", nil))
	commentDoc := note(commentID, commenterID, pageID, "a comment", "2026-07-07T05:20:00.000000Z")
	h.serveObject("/comment/3002", commentDoc)
	h.announceCreate(group, "https://lemmy.world/activities/announce/create/3002", commentDoc)

	// A sibling community, co-hosted on lemmy.world, that the bridge also follows.
	sibling := h.followedCommunity("https://lemmy.world/c/otherthing", "otherthing", "lemmy.world")
	h.announceDelete(sibling, "https://lemmy.world/activities/announce/sibling-delete-post",
		commenterID, pageID)
	h.announceDelete(sibling, "https://lemmy.world/activities/announce/sibling-delete-comment",
		commenterID, commentID)

	postMapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	assert.False(t, postMapping.IsDeleted(), "a sibling community must not delete another's post")
	commentMapping, err := h.objects.GetByAPID(ctx, commentID)
	require.NoError(t, err)
	assert.False(t, commentMapping.IsDeleted(), "a sibling community must not delete another's comment")
	for _, apID := range []string{pageID, commentID} {
		tombstoned, err := h.tombstones.ExistsFor(ctx, apID, sibling.id)
		require.NoError(t, err)
		assert.False(t, tombstoned, "an unauthorized announced delete must not record a tombstone")
	}
}

// TestAnnouncedUndoDeleteOfUnmappedIDDoesNotInject: the delete path admits an
// unmapped announced target (so its tombstone marker can close the
// delete-before-create race), but the RESTORE path must not — otherwise a
// followed community's Undo{Delete{<any IRI>}} is a fetch oracle and DID mint
// that materializes never-seen objects while bypassing the content funnel's
// echo, tombstone, and community-binding checks.
func TestAnnouncedUndoDeleteOfUnmappedIDDoesNotInject(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	evil := h.followedCommunity("https://evil.example/c/foo", "foo", "evil.example")

	// The attacker-chosen target: a never-seen object on a victim host, served
	// with a hit counter so we can prove it is never fetched.
	const (
		victimPost      = "https://victim.example/post/999"
		victimAuthor    = "https://victim.example/u/victim"
		victimCommunity = "https://victim.example/c/somewhere"
	)
	h.serveObject("/u/victim", person(victimAuthor, "victim", nil))
	h.serveObject("/post/999", map[string]any{
		"type":         "Page",
		"id":           victimPost,
		"attributedTo": victimAuthor,
		"to":           []any{ap.PublicAudience},
		"audience":     victimCommunity,
		"name":         "never seen by the bridge",
		"source":       map[string]any{"content": "body", "mediaType": "text/markdown"},
		"published":    "2026-07-07T09:00:00.000000Z",
	})
	mintsBefore := h.minter.mintCount()

	require.Equal(t, http.StatusAccepted, h.deliver(evil, map[string]any{
		"id":       "https://evil.example/activities/announce/undo-delete-unmapped",
		"type":     "Announce",
		"actor":    evil.id,
		"audience": evil.id,
		"object": map[string]any{
			"id":    "https://evil.example/activities/undo/unmapped",
			"type":  "Undo",
			"actor": evil.id,
			"object": map[string]any{
				"id":     "https://evil.example/activities/delete/unmapped",
				"type":   "Delete",
				"actor":  evil.id,
				"object": victimPost,
			},
		},
	}))
	h.drain()

	event, err := h.events.GetEvent(ctx, "https://evil.example/activities/announce/undo-delete-unmapped")
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt, "the drop is a processed skip, never a retry")
	assert.Equal(t, 0, h.hitCount("/post/999"),
		"a restore for an id the bridge never deleted must not fetch it")
	assert.Equal(t, mintsBefore, h.minter.mintCount(), "no identity minted for an unseen object")
	_, err = h.objects.GetByAPID(ctx, victimPost)
	assert.True(t, errors.IsNotFound(err), "the unseen object must not be materialized")
	_, err = h.communities.GetByAPGroupID(ctx, victimCommunity)
	assert.True(t, errors.IsNotFound(err), "the named community must not be bridged")
}

// TestAnnouncedUndoDeleteIntoAnotherCommunityDropped: prior state is present
// (the community deleted its own post), so the restore clears the fetch gate —
// but the origin now serves the object addressed to a DIFFERENT community.
// Re-materializing would EnsureCommunity() that other community and write the
// record into its repo, so the restore must drop, mirroring the announced-
// content binding check.
func TestAnnouncedUndoDeleteIntoAnotherCommunityDropped(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	ctx := context.Background()

	const (
		postID         = "https://lemmy.world/post/70002"
		otherCommunity = "https://lemmy.world/c/otherthing"
	)
	page := func(audience string) map[string]any {
		return map[string]any{
			"type":         "Page",
			"id":           postID,
			"attributedTo": personID,
			"to":           []any{ap.PublicAudience},
			"audience":     audience,
			"name":         "a post that moves",
			"source":       map[string]any{"content": "body", "mediaType": "text/markdown"},
			"published":    "2026-07-07T09:30:00.000000Z",
		}
	}
	// The other community is fetchable (an unfollowed, co-hosted Group): without
	// the binding check the restore's re-materialization would EnsureCommunity()
	// it — mint its DID and write the post into its repo.
	h.serveObject("/c/otherthing", map[string]any{
		"type":              "Group",
		"id":                otherCommunity,
		"preferredUsername": "otherthing",
		"inbox":             otherCommunity + "/inbox",
		"published":         "2024-01-01T00:00:00.000000Z",
	})
	h.serveObject("/post/70002", page(groupID))
	h.announceCreate(group, "https://lemmy.world/activities/announce/create/70002", page(groupID))
	mapping, err := h.objects.GetByAPID(ctx, postID)
	require.NoError(t, err)
	require.False(t, mapping.IsDeleted())

	h.announceDelete(group, "https://lemmy.world/activities/announce/delete/70002", personID, postID)
	mapping, err = h.objects.GetByAPID(ctx, postID)
	require.NoError(t, err)
	require.True(t, mapping.IsDeleted())

	// The origin restores the post, but now addressed to another community.
	h.serveObject("/post/70002", page(otherCommunity))
	require.Equal(t, http.StatusAccepted, h.deliver(group, map[string]any{
		"id":       "https://lemmy.world/activities/announce/undo-delete/70002",
		"type":     "Announce",
		"actor":    groupID,
		"audience": groupID,
		"object": map[string]any{
			"id":    "https://lemmy.world/activities/undo/70002",
			"type":  "Undo",
			"actor": groupID,
			"object": map[string]any{
				"id":     "https://lemmy.world/activities/delete/70002",
				"type":   "Delete",
				"actor":  personID,
				"object": postID,
			},
		},
	}))
	h.drain()

	mapping, err = h.objects.GetByAPID(ctx, postID)
	require.NoError(t, err)
	assert.True(t, mapping.IsDeleted(), "a restore into another community must not revive the mapping")
	tombstoned, err := h.tombstones.ExistsFor(ctx, postID, groupID)
	require.NoError(t, err)
	assert.True(t, tombstoned, "the tombstone marker survives a dropped restore")
	_, err = h.communities.GetByAPGroupID(ctx, otherCommunity)
	assert.True(t, errors.IsNotFound(err), "the other community must not be bridged")
}

// TestBareReferenceCreateCannotDodgeScopedTombstone pins WHERE the
// create-after-delete check may read its community scope from. Markers are
// community-scoped, so the lookup needs a community context — and before the
// object is resolved a BARE delivery offers only one: the delivered body's
// own audience, which the deliverer wrote. A Create carrying nothing but
// {"id": X} names no community at all, so a lookup keyed off that body reads
// global markers only and sails straight past the community-scoped marker a
// delete-before-create left for exactly that id — from ANY signer with a
// valid signature, for content that is not theirs. That is the marker's core
// case, so the scoped half of the check runs after resolveDelivered, against
// the audience the ORIGIN serves.
func TestBareReferenceCreateCannotDodgeScopedTombstone(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	ctx := context.Background()
	author, postID := h.unseenRemotePost("781")

	// The community the post belongs to announces its Delete before the Create
	// ever lands: a marker in that community's scope, nothing materialized.
	h.announceDelete(group, "https://lemmy.world/activities/announce/delete/781",
		author.id, postID)
	tombstoned, err := h.tombstones.ExistsFor(ctx, postID, groupID)
	require.NoError(t, err)
	require.True(t, tombstoned, "the delete-before-create marker is the premise")

	// An unrelated instance delivers a bare Create whose object is nothing but
	// a reference to the tombstoned id.
	mallory := h.newRemoteActor("https://evil.example/u/mallory",
		person("https://evil.example/u/mallory", "mallory", nil))
	require.Equal(t, http.StatusAccepted, h.deliver(mallory, map[string]any{
		"id":     "https://evil.example/activities/create/dodge",
		"type":   "Create",
		"actor":  mallory.id,
		"object": map[string]any{"id": postID},
	}))
	h.drain()

	event, err := h.events.GetEvent(ctx, "https://evil.example/activities/create/dodge")
	require.NoError(t, err)
	assert.NotNil(t, event.ProcessedAt, "the drop is a processed skip, never a retry")
	_, err = h.objects.GetByAPID(ctx, postID)
	assert.True(t, errors.IsNotFound(err),
		"a bare-reference create must not dodge the community-scoped marker")
	tombstoned, err = h.tombstones.ExistsFor(ctx, postID, groupID)
	require.NoError(t, err)
	assert.True(t, tombstoned, "the marker itself is untouched")
}

// TestBareUndoDeleteOfUnmappedActorNeverFetchesOrMints: Delete-then-Undo of
// an id the bridge never bridged is a two-activity mint primitive if the undo
// treats a marker as evidence worth FETCHING on. Both activities here are
// authorized (an instance may always delete an id on its own host), and under
// the old flow the marker satisfied the restore's prior-state gate, the
// target got fetched, and HandleUpdate turned a Person into a PLC mint. An
// unmapped undo restores nothing, so it must reach neither the network nor
// the minter — it only retracts the marker.
func TestBareUndoDeleteOfUnmappedActorNeverFetchesOrMints(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	mallory := h.newRemoteActor("https://evil.example/u/mallory",
		person("https://evil.example/u/mallory", "mallory", nil))
	const ghost = "https://evil.example/u/ghost"
	h.serveObject("/u/ghost", person(ghost, "ghost", nil))
	mintsBefore := h.minter.mintCount()

	require.Equal(t, http.StatusAccepted, h.deliver(mallory, map[string]any{
		"id":     "https://evil.example/activities/delete/ghost",
		"type":   "Delete",
		"actor":  mallory.id,
		"object": ghost,
	}))
	h.drain()
	tombstoned, err := h.tombstones.ExistsFor(ctx, ghost, "")
	require.NoError(t, err)
	require.True(t, tombstoned, "the delete lays the marker the undo would trade on")

	require.Equal(t, http.StatusAccepted, h.deliver(mallory, map[string]any{
		"id":    "https://evil.example/activities/undo/ghost",
		"type":  "Undo",
		"actor": mallory.id,
		"object": map[string]any{
			"id":     "https://evil.example/activities/delete/ghost",
			"type":   "Delete",
			"actor":  mallory.id,
			"object": ghost,
		},
	}))
	h.drain()

	assert.Equal(t, 0, h.hitCount("/u/ghost"),
		"a restore for an id with no mapping must never fetch its target")
	assert.Equal(t, mintsBefore, h.minter.mintCount(), "...and must never mint an identity")
	_, err = h.actors.GetByAPActorID(ctx, ghost)
	assert.True(t, errors.IsNotFound(err), "the target must not be bridged")
	tombstoned, err = h.tombstones.ExistsFor(ctx, ghost, "")
	require.NoError(t, err)
	assert.False(t, tombstoned, "the undo still retracts the marker it was entitled to lay")
}

// TestAnnouncedUndoDeleteCannotManufactureRestore is the announced half of
// the same primitive, and the reason the unmapped branch clears markers
// exactly: a followed community can always manufacture "prior state" for an
// arbitrary id (announce Delete{<any IRI>} — the allowance that closes the
// delete-before-create race), so its Undo must buy nothing. No fetch, and no
// authority beyond its own row: the origin-authorized global marker for the
// same id — the one that actually suppresses the id everywhere — survives.
func TestAnnouncedUndoDeleteCannotManufactureRestore(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()

	const (
		victimPost      = "https://victim.example/post/998"
		victimAuthor    = "https://victim.example/u/victim"
		victimCommunity = "https://victim.example/c/somewhere"
	)
	victim := h.newRemoteActor(victimAuthor, person(victimAuthor, "victim", nil))
	h.serveObject("/post/998", map[string]any{
		"type":         "Page",
		"id":           victimPost,
		"attributedTo": victimAuthor,
		"to":           []any{ap.PublicAudience},
		"audience":     victimCommunity,
		"name":         "never seen by the bridge",
		"source":       map[string]any{"content": "body", "mediaType": "text/markdown"},
		"published":    "2026-07-07T09:00:00.000000Z",
	})
	mintsBefore := h.minter.mintCount()

	// The victim's own instance deletes the post before the bridge ever saw
	// it: an ORIGIN-authorized (global) marker.
	require.Equal(t, http.StatusAccepted, h.deliver(victim, map[string]any{
		"id":     "https://victim.example/activities/delete/998",
		"type":   "Delete",
		"actor":  victimAuthor,
		"object": victimPost,
	}))
	h.drain()

	// An unrelated followed community lays its own marker for that id, then
	// undoes it.
	evil := h.followedCommunity("https://evil.example/c/foo", "foo", "evil.example")
	h.announceDelete(evil, "https://evil.example/activities/announce/delete/998",
		evil.id, victimPost)
	require.Equal(t, []string{"", evil.id}, h.tombstoneAnnouncers(victimPost),
		"both markers coexist (composite key)")

	require.Equal(t, http.StatusAccepted, h.deliver(evil, map[string]any{
		"id":       "https://evil.example/activities/announce/undo-delete/998",
		"type":     "Announce",
		"actor":    evil.id,
		"audience": evil.id,
		"object": map[string]any{
			"id":    "https://evil.example/activities/undo/998",
			"type":  "Undo",
			"actor": evil.id,
			"object": map[string]any{
				"id":     "https://evil.example/activities/delete/998",
				"type":   "Delete",
				"actor":  evil.id,
				"object": victimPost,
			},
		},
	}))
	h.drain()

	assert.Equal(t, 0, h.hitCount("/post/998"),
		"an unmapped restore must not fetch the id it manufactured state for")
	assert.Equal(t, mintsBefore, h.minter.mintCount(), "no identity minted")
	_, err := h.objects.GetByAPID(ctx, victimPost)
	assert.True(t, errors.IsNotFound(err), "the unseen object must not be materialized")
	assert.Equal(t, []string{""}, h.tombstoneAnnouncers(victimPost),
		"the community clears its OWN row; the origin-authorized marker outranks it")
	tombstoned, err := h.tombstones.ExistsFor(ctx, victimPost, evil.id)
	require.NoError(t, err)
	assert.True(t, tombstoned, "the surviving global marker still suppresses the id everywhere")
}

// TestAnnouncedRestoreWithoutAudienceDropped: an announced restore must NAME
// the announcing community, not merely fail to contradict it. A body with no
// audience passed the old "empty is fine" binding vacuously — which is how a
// sibling community could revive a comment another community had deleted:
// the comment is already soft-deleted, so the reply.root membership check has
// nothing left to read and admits the activity (idempotence for re-delivered
// deletes), leaving the post-fetch binding as the only guard. Real Lemmy
// bodies always carry audience, so demanding it costs nothing.
func TestAnnouncedRestoreWithoutAudienceDropped(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	ctx := context.Background()

	require.Equal(t, http.StatusAccepted,
		h.deliver(group, loadFixture(t, "announce_create_page_lemmy_world.json")))
	h.drain()

	const (
		commenterID = "https://lemmy.world/u/commenter"
		commentID   = "https://lemmy.world/comment/3003"
	)
	h.serveObject("/u/commenter", person(commenterID, "commenter", nil))
	// No audience: comments derive their community from the thread root, so
	// this materializes and deletes normally — the field only matters to the
	// restore's binding check.
	commentDoc := map[string]any{
		"type":         "Note",
		"id":           commentID,
		"attributedTo": commenterID,
		"to":           []any{ap.PublicAudience},
		"content":      "<p>a comment</p>",
		"source":       map[string]any{"content": "a comment", "mediaType": "text/markdown"},
		"published":    "2026-07-07T05:30:00.000000Z",
		"inReplyTo":    pageID,
	}
	h.serveObject("/comment/3003", commentDoc)
	h.announceCreate(group, "https://lemmy.world/activities/announce/create/3003", commentDoc)
	mapping, err := h.objects.GetByAPID(ctx, commentID)
	require.NoError(t, err)
	require.False(t, mapping.IsDeleted())

	// The owning community deletes it...
	h.announceDelete(group, "https://lemmy.world/activities/announce/delete/3003",
		commenterID, commentID)
	mapping, err = h.objects.GetByAPID(ctx, commentID)
	require.NoError(t, err)
	require.True(t, mapping.IsDeleted())

	// ...and a co-hosted sibling community the bridge also follows tries to
	// bring it back.
	sibling := h.followedCommunity("https://lemmy.world/c/otherthing", "otherthing", "lemmy.world")
	require.Equal(t, http.StatusAccepted, h.deliver(sibling, map[string]any{
		"id":       "https://lemmy.world/activities/announce/sibling-undo-3003",
		"type":     "Announce",
		"actor":    sibling.id,
		"audience": sibling.id,
		"object": map[string]any{
			"id":    "https://lemmy.world/activities/undo/sibling-3003",
			"type":  "Undo",
			"actor": sibling.id,
			"object": map[string]any{
				"id":     "https://lemmy.world/activities/delete/sibling-3003",
				"type":   "Delete",
				"actor":  commenterID,
				"object": commentID,
			},
		},
	}))
	h.drain()

	mapping, err = h.objects.GetByAPID(ctx, commentID)
	require.NoError(t, err)
	assert.True(t, mapping.IsDeleted(),
		"a restore whose body names no community must not revive the mapping")
	_, _, err = h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	assert.Error(t, err, "the record stays deleted")
}

// TestAnnouncedRestoreOfChangedTypeDropped: the re-materialization behind a
// restore is HandleUpdate, which dispatches on the FETCHED type rather than
// on the mapping. An id that used to serve a post and now serves a Person
// would therefore turn a content restore into an actor mint/refresh — with no
// audience to fail the community binding, since actor documents carry none.
// The mapping's collection is the invariant: only a consistent type restores.
func TestAnnouncedRestoreOfChangedTypeDropped(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	ctx := context.Background()

	const postID = "https://lemmy.world/post/70004"
	page := map[string]any{
		"type":         "Page",
		"id":           postID,
		"attributedTo": personID,
		"to":           []any{ap.PublicAudience},
		"audience":     groupID,
		"name":         "a post that changes shape",
		"source":       map[string]any{"content": "body", "mediaType": "text/markdown"},
		"published":    "2026-07-07T10:30:00.000000Z",
	}
	h.serveObject("/post/70004", page)
	h.announceCreate(group, "https://lemmy.world/activities/announce/create/70004", page)
	h.announceDelete(group, "https://lemmy.world/activities/announce/delete/70004", personID, postID)
	mapping, err := h.objects.GetByAPID(ctx, postID)
	require.NoError(t, err)
	require.True(t, mapping.IsDeleted())
	mintsBefore := h.minter.mintCount()

	// The origin now serves an ACTOR at the post's id.
	h.serveObject("/post/70004", person(postID, "shapeshifter", nil))
	require.Equal(t, http.StatusAccepted, h.deliver(group, map[string]any{
		"id":       "https://lemmy.world/activities/announce/undo-delete/70004",
		"type":     "Announce",
		"actor":    groupID,
		"audience": groupID,
		"object": map[string]any{
			"id":    "https://lemmy.world/activities/undo/70004",
			"type":  "Undo",
			"actor": groupID,
			"object": map[string]any{
				"id":     "https://lemmy.world/activities/delete/70004",
				"type":   "Delete",
				"actor":  personID,
				"object": postID,
			},
		},
	}))
	h.drain()

	mapping, err = h.objects.GetByAPID(ctx, postID)
	require.NoError(t, err)
	assert.True(t, mapping.IsDeleted(), "a type-mismatched restore must not revive the mapping")
	assert.Equal(t, mintsBefore, h.minter.mintCount(), "a content restore must never mint an actor")
	_, err = h.actors.GetByAPActorID(ctx, postID)
	assert.True(t, errors.IsNotFound(err), "the post's id must not become a bridged actor")
}

// TestUndoDeleteRejectsCrossAuthorityRedirect: the restore's re-fetch IS its
// authorization ("the origin must serve the object again") AND the body that
// goes back into the repo, so an open redirect off the origin would both
// license the restore and choose its content. Pinned like the delete sweep's
// fetch (TestSweepDeletedRejectsCrossAuthorityRedirect), one call over.
func TestUndoDeleteRejectsCrossAuthorityRedirect(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	author := h.newRemoteActor(personID, person(personID, "LeftLeaningFreedomFighters", nil))
	ctx := context.Background()

	const slug = "restore-redirect"
	postID := "https://lemmy.world/post/" + slug
	h.mux.HandleFunc("GET /post/"+slug, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", "https://evil.example/post/attacker-restore")
		w.WriteHeader(http.StatusFound)
	})
	// The attacker host serves a well-formed post claiming the VICTIM's id, so
	// only the redirect pin — not the self-asserted-id binding, which compares
	// against the requested IRI — can refuse it.
	h.serveObject("/post/attacker-restore", map[string]any{
		"type":         "Page",
		"id":           postID,
		"attributedTo": personID,
		"to":           []any{ap.PublicAudience},
		"audience":     groupID,
		"name":         "ATTACKER RESTORE",
		"source":       map[string]any{"content": "attacker body", "mediaType": "text/markdown"},
		"published":    "2026-07-07T11:00:00.000000Z",
	})
	require.Equal(t, postID, h.bridgePost(group, slug))

	require.Equal(t, http.StatusAccepted, h.deliver(author, map[string]any{
		"id":     "https://lemmy.world/activities/delete/" + slug,
		"type":   "Delete",
		"actor":  personID,
		"object": postID,
	}))
	h.drain()
	mapping, err := h.objects.GetByAPID(ctx, postID)
	require.NoError(t, err)
	require.True(t, mapping.IsDeleted())

	require.Equal(t, http.StatusAccepted, h.deliver(author, map[string]any{
		"id":    "https://lemmy.world/activities/undo/" + slug,
		"type":  "Undo",
		"actor": personID,
		"object": map[string]any{
			"id":     "https://lemmy.world/activities/delete/" + slug,
			"type":   "Delete",
			"actor":  personID,
			"object": postID,
		},
	}))
	h.drain()

	event, err := h.events.GetEvent(ctx, "https://lemmy.world/activities/undo/"+slug)
	require.NoError(t, err)
	assert.NotNil(t, event.FailedAt, "an off-authority redirect is permanent, so the event poisons")
	assert.Contains(t, event.Error, "authority",
		"the refusal must name the authority hop, not some later failure")
	mapping, err = h.objects.GetByAPID(ctx, postID)
	require.NoError(t, err)
	assert.True(t, mapping.IsDeleted(), "a redirected restore must not revive the record")
	tombstoned, err := h.tombstones.ExistsFor(ctx, postID, "")
	require.NoError(t, err)
	assert.True(t, tombstoned, "...nor clear the marker")
}

// oneShotMissingActors hides ONE ap id from the first bridged_actors lookup
// and delegates every later one: the deterministic stand-in for the mid-mint
// window, where a row is absent when authorization reads it and present when
// the materializer does.
type oneShotMissingActors struct {
	store.BridgedActors
	apID string
	mu   sync.Mutex
	hid  bool
}

func (s *oneShotMissingActors) GetByAPActorID(ctx context.Context, apID string) (*store.BridgedActor, error) {
	if s.hideOnce(apID) {
		return nil, errors.NewNotFoundError("bridged_actor", apID)
	}
	return s.BridgedActors.GetByAPActorID(ctx, apID)
}

func (s *oneShotMissingActors) hideOnce(apID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if apID != s.apID || s.hid {
		return false
	}
	s.hid = true
	return true
}

// oneShotMissingObjects is oneShotMissingActors for the mapping table: the
// actor ROW lands before the profile mapping, so a faithful mid-mint window
// hides both from the same first look.
type oneShotMissingObjects struct {
	store.APObjects
	apID string
	mu   sync.Mutex
	hid  bool
}

func (s *oneShotMissingObjects) GetByAPID(ctx context.Context, apID string) (*store.APObjectMapping, error) {
	s.mu.Lock()
	first := apID == s.apID && !s.hid
	if first {
		s.hid = true
	}
	s.mu.Unlock()
	if first {
		return nil, errors.NewNotFoundError("ap_object", apID)
	}
	return s.APObjects.GetByAPID(ctx, apID)
}

// swapHandlerStores rebuilds the dispatcher (and the queue drain() pumps)
// over substitute store views. The inbox keeps its own wiring: events are
// claimed from the database, so only the processor's identity matters here.
func (h *harness) swapHandlerStores(objects store.APObjects, actors store.BridgedActors) {
	h.t.Helper()
	handler, err := NewHandler(HandlerOptions{
		Materializer:   h.mat,
		Fetcher:        h.client,
		Objects:        objects,
		Actors:         actors,
		Communities:    h.communities,
		Tombstones:     h.tombstones,
		Records:        h.manager,
		Votes:          h.votes,
		Backfill:       h.backfills,
		ServiceActorID: h.service.ID,
	})
	require.NoError(h.t, err)
	h.handler = handler
	queue, err := NewQueue(QueueOptions{
		Events:      h.events,
		Processor:   handler,
		Workers:     1,
		MaxAttempts: 3,
		Lease:       time.Minute,
	})
	require.NoError(h.t, err)
	h.queue = queue
}

// TestAnnouncedDeleteCannotScrubMidMintActor closes the actor-vs-content
// TOCTOU. authorizeDelete classifies the target with the rows it can see, and
// during a mint there are none — neither bridged_actors nor the profile
// mapping has landed — so the delete is admitted as an unmapped announced id
// (the allowance that closes the delete-before-create race). If the
// materializer then re-derived the branch from a FRESH bridged_actors read,
// the row that landed in between would turn an unrelated community's content
// delete into that actor's terminal scrub. The classification travels with
// the dispatch instead.
func TestAnnouncedDeleteCannotScrubMidMintActor(t *testing.T) {
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	ctx := context.Background()

	require.Equal(t, http.StatusAccepted,
		h.deliver(group, loadFixture(t, "announce_create_page_lemmy_world.json")))
	h.drain()
	actorRow, err := h.actors.GetByAPActorID(ctx, personID)
	require.NoError(t, err)
	require.Equal(t, store.ConsentStateOK, actorRow.ConsentState)

	h.swapHandlerStores(
		&oneShotMissingObjects{APObjects: h.objects, apID: personID},
		&oneShotMissingActors{BridgedActors: h.actors, apID: personID},
	)
	evil := h.followedCommunity("https://evil.example/c/foo", "foo", "evil.example")
	h.announceDelete(evil, "https://evil.example/activities/announce/delete/mid-mint",
		evil.id, personID)

	actorRow, err = h.actors.GetByAPActorID(ctx, personID)
	require.NoError(t, err)
	assert.Equal(t, store.ConsentStateOK, actorRow.ConsentState,
		"an announced content delete must never reach the terminal actor scrub")
	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	assert.False(t, mapping.IsDeleted(), "the actor's content stays live")
	_, _, err = h.manager.GetRecord(ctx, actorRow.DID,
		materialize.CollectionActorProfile, materialize.ProfileRKey)
	assert.NoError(t, err, "the profile record survives too")
}
