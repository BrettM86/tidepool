package materialize

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// Repin semantics. An acceptance strongRef PINS a version: the lexicon tells
// consumers not to render a CID the acceptance does not name, so every time a
// post's CID moves — an upstream edit, a vote-stats stamp — the acceptance has
// to follow or the post silently falls out of its community. Re-pinning is not
// a new acceptance: the community decided once, and only the pin moves.

// TestUpstreamEditRepinsAcceptance (E1): a Lemmy edit rewrites the postv2, so
// its CID moves. The acceptance at the SAME digest rkey must follow it, and
// must carry the ORIGINAL createdAt — a restamped createdAt would tell Coves
// the community accepted this post at edit time, which is a different claim
// about a moderation-relevant timestamp.
func TestUpstreamEditRepinsAcceptance(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	page := loadFixtureObject(t, "page_lemmy_world.json")
	rkey, err := recordRKey(page)
	require.NoError(t, err)

	first, err := h.m.MaterializePost(ctx, page)
	require.NoError(t, err)
	require.False(t, first.NoOp)

	communityDID := testDIDFor("technology", "lemmy.world")
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
	postURI := "at://" + authorDID + "/" + testPostV2Collection + "/" + rkey
	acceptanceRKey := acceptanceFor(t, authorDID, rkey)

	before, _, err := h.manager.GetRecord(ctx, communityDID, testAcceptanceCollection, acceptanceRKey)
	require.NoError(t, err)
	originalCreatedAt, _ := before["createdAt"].(string)
	require.NotEmpty(t, originalCreatedAt)
	beforeSubject, ok := before["subject"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, first.CID, cidString(beforeSubject["cid"]))

	// The author edits the post upstream.
	edited := loadFixtureObject(t, "page_lemmy_world.json")
	edited.Source = &ap.Source{Content: "edited body text", MediaType: "text/markdown"}
	res, err := h.m.HandleUpdate(ctx, edited)
	require.NoError(t, err)
	require.False(t, res.NoOp, "an edited body is a real commit")
	require.NotEqual(t, first.CID, res.CID, "the edit must move the post's CID")

	after, _, err := h.manager.GetRecord(ctx, communityDID, testAcceptanceCollection, acceptanceRKey)
	require.NoError(t, err,
		"the repin must land at the SAME digest rkey — a post has exactly one acceptance per community")
	afterSubject, ok := after["subject"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, postURI, afterSubject["uri"], "the subject uri does not move on an edit")
	assert.Equal(t, res.CID, cidString(afterSubject["cid"]),
		"the acceptance must pin the EDITED version; still pinning the old CID means Coves stops "+
			"rendering the post the moment its author edits it")
	assert.Equal(t, originalCreatedAt, after["createdAt"],
		"a repin is not a new acceptance: createdAt must be carried forward, not restamped")
}

// TestRedeliveredEditDoesNotChurnEitherRepo (E2): the same Update arriving
// twice is one of the commonest things AP delivery does. The second pass must
// be inert in BOTH repos — the postv2 re-put is byte-identical and so is the
// acceptance, so both must reach their repo layer's no-op path rather than
// minting a pair of firehose events for nothing.
func TestRedeliveredEditDoesNotChurnEitherRepo(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	page := loadFixtureObject(t, "page_lemmy_world.json")
	_, err := h.m.MaterializePost(ctx, page)
	require.NoError(t, err)

	communityDID := testDIDFor("technology", "lemmy.world")
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")

	editedOnce := loadFixtureObject(t, "page_lemmy_world.json")
	editedOnce.Source = &ap.Source{Content: "edited body text", MediaType: "text/markdown"}
	_, err = h.m.HandleUpdate(ctx, editedOnce)
	require.NoError(t, err)

	authorEvents := eventsForDID(t, h, authorDID)
	communityEvents := eventsForDID(t, h, communityDID)
	// Self-proof: both repos must actually be receiving events, or the
	// before/after comparison below would be 0 == 0 and prove nothing.
	require.Positive(t, authorEvents)
	require.Positive(t, communityEvents)

	// The queue redelivers the identical Update.
	editedAgain := loadFixtureObject(t, "page_lemmy_world.json")
	editedAgain.Source = &ap.Source{Content: "edited body text", MediaType: "text/markdown"}
	again, err := h.m.HandleUpdate(ctx, editedAgain)
	require.NoError(t, err)
	assert.True(t, again.NoOp, "an unchanged redelivered edit is a no-op at the postv2 commit")

	assert.Equal(t, authorEvents, eventsForDID(t, h, authorDID),
		"a redelivered edit must not mint an author-repo firehose event")
	assert.Equal(t, communityEvents, eventsForDID(t, h, communityDID),
		"a redelivered edit must not mint a community-repo firehose event: the acceptance re-put "+
			"is byte-identical and must reach the repo layer's no-op path")
}

// TestStaleAcceptanceRepinnedDespiteNoOpStamp (S1) is the crash-window heal
// for the stats path. The stamp lands in the AUTHOR's repo and the repin in
// the COMMUNITY's, so a crash between them leaves an acceptance pinning a CID
// that no longer exists — and to Coves' consumers that post has fallen out of
// the community. The next sweep carries the SAME counts, so the stamp itself
// no-ops without committing. If the repin is driven by "did the stamp
// commit?", nothing ever heals that post; it must be driven by the PIN
// MISMATCH — the acceptance names a CID the record no longer has.
func TestStaleAcceptanceRepinnedDespiteNoOpStamp(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	page := loadFixtureObject(t, "page_lemmy_world.json")
	rkey, err := recordRKey(page)
	require.NoError(t, err)

	first, err := h.m.MaterializePost(ctx, page)
	require.NoError(t, err)
	require.False(t, first.NoOp)

	communityDID := testDIDFor("technology", "lemmy.world")
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
	postURI := "at://" + authorDID + "/" + testPostV2Collection + "/" + rkey
	acceptanceRKey := acceptanceFor(t, authorDID, rkey)

	original, _, err := h.manager.GetRecord(ctx, communityDID, testAcceptanceCollection, acceptanceRKey)
	require.NoError(t, err)
	originalCreatedAt, _ := original["createdAt"].(string)
	require.NotEmpty(t, originalCreatedAt)

	// The vote refresher stamps counts: the post's CID moves.
	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	stamped, err := h.m.SetBridgedStats(ctx, mapping, 42, 3, statsAsOf)
	require.NoError(t, err)
	require.False(t, stamped.NoOp)
	require.NotEqual(t, first.CID, stamped.CID, "stamping stats must move the post's CID")

	// Simulate the crash: the stats commit landed, the repin did not — the
	// acceptance still pins the PRE-STAMP version.
	stale := map[string]any{
		"$type":     CollectionAcceptance,
		"subject":   strongRef(postURI, first.CID),
		"createdAt": originalCreatedAt,
	}
	_, err = h.manager.PutRecord(ctx, communityDID, CollectionAcceptance, acceptanceRKey, stale)
	require.NoError(t, err)

	// The next sweep carries the SAME counts, so the stamp no-ops. Only the
	// pin mismatch is left to notice the post is unrenderable.
	refreshed, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	again, err := h.m.SetBridgedStats(ctx, refreshed, 42, 3, statsAsOf.Add(time.Hour))
	require.NoError(t, err)
	require.True(t, again.NoOp, "unchanged counts must not re-commit the post record")

	healed, _, err := h.manager.GetRecord(ctx, communityDID, testAcceptanceCollection, acceptanceRKey)
	require.NoError(t, err)
	subject, ok := healed["subject"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, stamped.CID, cidString(subject["cid"]),
		"a stale acceptance must be repinned to the CURRENT record CID even though the stamp "+
			"no-opped: driving the repin off the stamp's commit leaves crash-window posts "+
			"invisible until somebody edits them upstream")
	assert.Equal(t, originalCreatedAt, healed["createdAt"],
		"healing a pin is not a new acceptance")
}

// TestLegacyPostStatsWritesNoAcceptance (S2) is the mixed-era control. Posts
// materialized before the flip stay in the community's repo under the
// deprecated collection and are never migrated; Coves indexes them directly,
// with no acceptance in the model at all. A stats stamp on one must therefore
// write no acceptance — inventing one would announce a second, contradictory
// visibility mechanism for a post Coves already renders.
func TestLegacyPostStatsWritesNoAcceptance(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	community, err := h.m.EnsureCommunity(ctx, &ap.Object{ID: groupID})
	require.NoError(t, err)
	author, err := h.m.EnsureActor(ctx, &ap.Object{ID: personID})
	require.NoError(t, err)

	// A pre-flip post: in the COMMUNITY's repo, under the deprecated
	// collection, with the in-record author the old lexicon required.
	const legacyAPID = "https://lemmy.world/post/1234"
	const legacyRKey = "3kjzl5kcb2s2v"
	published := time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC)
	legacy := map[string]any{
		"$type":     CollectionPost,
		"community": community.DID,
		"author":    author.DID,
		"createdAt": recordDatetime(published),
		"title":     "a pre-flip post",
	}
	commit, err := h.manager.PutRecord(ctx, community.DID, CollectionPost, legacyRKey, legacy)
	require.NoError(t, err)

	mapping, err := h.objects.PutMapping(ctx, store.APObjectMapping{
		APID:           legacyAPID,
		APType:         "Page",
		OriginInstance: "lemmy.world",
		Origin:         store.OriginFediverse,
		DID:            community.DID,
		AuthorDID:      author.DID,
		CommunityDID:   community.DID,
		Collection:     CollectionPost,
		RKey:           legacyRKey,
		CID:            commit.RecordCID,
		PublishedAt:    &published,
	})
	require.NoError(t, err)

	res, err := h.m.SetBridgedStats(ctx, mapping, 12, 4, statsAsOf)
	require.NoError(t, err)
	require.False(t, res.NoOp, "the first stamp on a legacy post is a real commit")

	legacyURI := "at://" + community.DID + "/" + CollectionPost + "/" + legacyRKey
	_, _, err = h.manager.GetRecord(ctx, community.DID, CollectionAcceptance, SubjectRKey(legacyURI))
	assert.True(t, errors.IsNotFound(err),
		"a legacy post must not acquire an acceptance at its subject digest (err=%v)", err)

	entries, err := h.manager.ListRecords(ctx, community.DID)
	require.NoError(t, err)
	for _, entry := range entries {
		assert.NotEqual(t, CollectionAcceptance, entry.Collection,
			"no acceptance record may exist in the community repo for the legacy era (rkey %s)", entry.Rkey)
	}
}
