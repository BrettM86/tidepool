package materialize

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// TestUpdateRePutsSameRkey: an Update{Page} with edited content re-puts the
// record under the SAME rkey/at-uri with a new CID, and the mapping tracks
// the new CID.
func TestUpdateRePutsSameRkey(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()
	page := loadFixtureObject(t, "page_lemmy_world.json")

	first, err := h.m.MaterializePost(ctx, page)
	require.NoError(t, err)

	edited := loadFixtureObject(t, "page_lemmy_world.json")
	edited.Source = &ap.Source{Content: "edited body text", MediaType: "text/markdown"}
	second, err := h.m.HandleUpdate(ctx, edited)
	require.NoError(t, err)

	assert.Equal(t, first.ATURI, second.ATURI, "updates re-put under the same at-uri")
	assert.NotEqual(t, first.CID, second.CID, "edited content must produce a new CID")
	assert.False(t, second.NoOp)

	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	assert.Equal(t, second.CID, mapping.CID, "the mapping tracks the latest CID")

	record, _, err := h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	require.NoError(t, err)
	assert.Equal(t, "edited body text", record["content"])
}

// TestUpdatePersonRefreshesProfile: Update{Person} re-materializes the
// profile immediately, TTL or not.
func TestUpdatePersonRefreshesProfile(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	_, err := h.m.EnsureActor(ctx, &ap.Object{ID: personID})
	require.NoError(t, err)

	updated := loadFixtureObject(t, "person_lemmy_world.json")
	updated.Name = "Renamed Neelix"
	res, err := h.m.HandleUpdate(ctx, updated)
	require.NoError(t, err)

	record, _, err := h.manager.GetRecord(ctx, res.DID, CollectionActorProfile, ProfileRKey)
	require.NoError(t, err)
	assert.Equal(t, "Renamed Neelix", record["displayName"])
}

// TestHandleDeleteObject: Delete for a bridged post removes the record and
// tombstones the mapping; a second delete and an unknown id are no-ops.
func TestHandleDeleteObject(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	_, err := h.m.MaterializePost(ctx, loadFixtureObject(t, "page_lemmy_world.json"))
	require.NoError(t, err)
	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)

	require.NoError(t, h.m.HandleDelete(ctx, pageID))

	_, _, err = h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	assert.True(t, errors.IsNotFound(err), "the record must be deleted from the repo")
	_, _, err = h.objects.ResolveStrongRef(ctx, pageID)
	assert.True(t, errors.IsTombstoned(err), "the mapping must be soft-deleted")

	// Idempotent re-delete and unknown-object delete are silent no-ops.
	require.NoError(t, h.m.HandleDelete(ctx, pageID))
	require.NoError(t, h.m.HandleDelete(ctx, "https://lemmy.world/post/never-seen"))
}

// TestDeleteActorScrubsEverything is the Delete(Actor) flow: every record
// the actor authored is tombstoned FIRST — including posts living in
// community repos — then the consent state flips to deleted (terminal), and
// no new content for them materializes.
func TestDeleteActorScrubsEverything(t *testing.T) {
	h := newHarness(t)
	leaf := serveThread(t, h)
	ctx := context.Background()

	// carol authors the leaf comment; the fixture person authors the post.
	_, err := h.m.MaterializeComment(ctx, leaf)
	require.NoError(t, err)

	authorAP := personID // fixture person: authored the post in the community repo
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
	postMapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	require.Equal(t, authorDID, postMapping.AuthorDID)

	require.NoError(t, h.m.DeleteActor(ctx, authorAP))

	// Their post (in the COMMUNITY repo) is gone.
	_, _, err = h.manager.GetRecord(ctx, postMapping.DID, postMapping.Collection, postMapping.RKey)
	assert.True(t, errors.IsNotFound(err), "authored post in the community repo must be scrubbed")
	_, _, err = h.objects.ResolveStrongRef(ctx, pageID)
	assert.True(t, errors.IsTombstoned(err))

	// Their profile (in their own repo) is gone.
	_, _, err = h.manager.GetRecord(ctx, authorDID, CollectionActorProfile, ProfileRKey)
	assert.True(t, errors.IsNotFound(err), "own-repo records must be scrubbed")

	// Consent is terminally deleted; new content is skipped.
	actor, err := h.actors.GetByAPActorID(ctx, authorAP)
	require.NoError(t, err)
	assert.Equal(t, store.ConsentStateDeleted, actor.ConsentState)
	_, err = h.m.EnsureActor(ctx, &ap.Object{ID: authorAP})
	require.Error(t, err)
	assert.True(t, IsSkip(err))

	// Untouched bystanders: alice's comment survives.
	c1Mapping, err := h.objects.GetByAPID(ctx, "https://lemmy.world/comment/1001")
	require.NoError(t, err)
	assert.False(t, c1Mapping.IsDeleted())

	// Idempotent AND a terminal fixpoint: a replayed Delete(Actor) carrying a
	// distinct activity id (so inbox dedup did not absorb it) is a no-op
	// success that must NOT re-scrub or append a SECOND #account frame —
	// ConsentStateDeleted short-circuits before re-emitting.
	before := len(h.firehoseEvents())
	require.NoError(t, h.m.DeleteActor(ctx, authorAP))
	assert.Len(t, h.firehoseEvents(), before,
		"a second DeleteActor on an already-deleted actor emits no new firehose event")
}

// TestDeleteActorViaHandleDelete: HandleDelete recognizes actor ids and
// routes them through the scrub flow.
func TestDeleteActorViaHandleDelete(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	_, err := h.m.EnsureActor(ctx, &ap.Object{ID: personID})
	require.NoError(t, err)
	require.NoError(t, h.m.HandleDelete(ctx, personID))

	actor, err := h.actors.GetByAPActorID(ctx, personID)
	require.NoError(t, err)
	assert.Equal(t, store.ConsentStateDeleted, actor.ConsentState)
}

// TestNobridgeAddedOnRefresh: an already-bridged actor who adds #nobridge
// flips to nobridge on the next (forced) refresh and content stops.
func TestNobridgeAddedOnRefresh(t *testing.T) {
	h := newHarness(t)
	h.serveObject("/u/flipflop", person("https://lemmy.world/u/flipflop", "flipflop", nil))
	ctx := context.Background()

	_, err := h.m.EnsureActor(ctx, &ap.Object{ID: "https://lemmy.world/u/flipflop"})
	require.NoError(t, err)

	// The actor edits their bio to include the marker.
	updated := person("https://lemmy.world/u/flipflop", "flipflop", map[string]any{
		"summary": "<p>done with this #nobridge</p>",
	})
	_, err = h.m.RefreshActor(ctx, objectFromMap(t, updated))
	require.Error(t, err)
	assert.True(t, IsSkip(err))

	actor, err := h.actors.GetByAPActorID(ctx, "https://lemmy.world/u/flipflop")
	require.NoError(t, err)
	assert.Equal(t, store.ConsentStateNoBridge, actor.ConsentState)
}

// TestListByActorDIDSpine sanity-checks the store addition this task made:
// author_did finds cross-repo posts.
func TestListByActorDIDSpine(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	_, err := h.m.MaterializePost(ctx, loadFixtureObject(t, "page_lemmy_world.json"))
	require.NoError(t, err)

	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
	mappings, err := h.objects.ListByActorDID(ctx, authorDID)
	require.NoError(t, err)

	var collections []string
	for _, mapping := range mappings {
		collections = append(collections, mapping.Collection)
	}
	assert.Contains(t, collections, CollectionPost,
		"posts in community repos are found via author_did")
	assert.Contains(t, collections, CollectionActorProfile,
		"own-repo records are found via did")

	db := testutil.DB(t)
	var authorInDB string
	require.NoError(t, db.QueryRow(
		`SELECT author_did FROM ap_objects WHERE ap_id = $1`, pageID).Scan(&authorInDB))
	assert.Equal(t, authorDID, authorInDB)
}
