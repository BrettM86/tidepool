package materialize

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
	"tidepool/internal/testutil"
)

// Acceptance-write semantics beyond the outer contract. The outer test
// (TestOuterAcceptance_PostV2Flip) pins that an acceptance exists in the
// community repo at the digest rkey, strongRef-pinning the postv2's uri and
// CID. These pin how that write must BEHAVE: it heals, it stays off the
// mapping spine, and it does not churn the community repo.

// acceptanceFor returns the community-repo coordinates of a post's acceptance
// record, deriving the digest rkey independently (see postv2_flip_test.go).
func acceptanceFor(t *testing.T, authorDID, rkey string) string {
	t.Helper()
	return testSubjectRkey("at://" + authorDID + "/" + testPostV2Collection + "/" + rkey)
}

// TestAcceptanceHealsOnRedelivery (A1): the postv2 and its acceptance land in
// TWO repos, so they cannot share a transaction — a crash between the two
// commits leaves a post nobody can see (Coves renders community surfaces from
// acceptance records only). Nothing retries that gap on its own; the heal has
// to be redelivery, and redelivery of an unchanged post is a NoOp at the
// postv2 commit. So a NoOp postv2 commit MUST still drive the acceptance
// write — an implementation that writes the acceptance only when the post
// commit produced a new CID never heals, and the post stays invisible until
// somebody edits it upstream.
func TestAcceptanceHealsOnRedelivery(t *testing.T) {
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
	acceptanceRKey := acceptanceFor(t, authorDID, rkey)

	// Recorded, not required: this is the outer contract's assertion, and
	// keeping it non-fatal means the heal assertion below is what this test
	// actually reports on.
	_, _, err = h.manager.GetRecord(ctx, communityDID, testAcceptanceCollection, acceptanceRKey)
	assert.NoError(t, err, "precondition (outer contract): the first materialization writes an acceptance")

	// Simulate the crash window: the postv2 committed, the acceptance did
	// not. Tolerates the record already being absent — either way, after this
	// the community holds no acceptance for the post.
	if _, derr := h.manager.DeleteRecord(ctx, communityDID, testAcceptanceCollection, acceptanceRKey); derr != nil {
		require.True(t, errors.IsNotFound(derr), "unexpected delete failure: %v", derr)
	}

	// The queue redelivers the same Create.
	second, err := h.m.MaterializePost(ctx, page)
	require.NoError(t, err)
	assert.True(t, second.NoOp, "an unchanged redelivery is an idempotent no-op at the postv2 commit")

	_, _, err = h.manager.GetRecord(ctx, communityDID, testAcceptanceCollection, acceptanceRKey)
	require.NoError(t, err,
		"redelivery must heal a missing acceptance: the postv2 re-put is a NoOp, so an "+
			"acceptance write conditioned on a fresh post CID would leave the post permanently "+
			"invisible in the community")
}

// TestAcceptanceIsNotOnTheMappingSpine (A2): ap_objects maps AP objects to
// the records materialized FROM them. An acceptance has no AP object behind
// it — the bridge mints it as the community's own attestation — so it must
// never take a mapping row. A mapping would give it an ap_id it does not
// have, put it in front of every spine consumer (ResolveStrongRef, the
// delete/scrub walks, ListByActorDID), and let an announced delete for the
// POST address the acceptance through the same key space.
//
// Red today on the shared precondition (no acceptance is written at all);
// the distinctive assertions bite the moment GREEN lands the write.
func TestAcceptanceIsNotOnTheMappingSpine(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	page := loadFixtureObject(t, "page_lemmy_world.json")
	rkey, err := recordRKey(page)
	require.NoError(t, err)

	_, err = h.m.MaterializePost(ctx, page)
	require.NoError(t, err)

	communityDID := testDIDFor("technology", "lemmy.world")
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
	acceptanceRKey := acceptanceFor(t, authorDID, rkey)

	_, _, err = h.manager.GetRecord(ctx, communityDID, testAcceptanceCollection, acceptanceRKey)
	require.NoError(t, err,
		"precondition (outer contract): an acceptance record must exist to be kept off the spine")

	db := testutil.DB(t)

	// (a) No mapping row names the acceptance — by collection or by at-uri.
	var byCollection int
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM ap_objects WHERE collection = $1`, testAcceptanceCollection).Scan(&byCollection))
	assert.Zero(t, byCollection, "an acceptance record must not take an ap_objects row")

	var byATURI int
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM ap_objects WHERE at_uri LIKE $1`,
		"%/"+testAcceptanceCollection+"/%").Scan(&byATURI))
	assert.Zero(t, byATURI, "no mapping's at-uri may point at an acceptance record")

	// (b) The community's spine listing shows its profile and nothing else
	// the post created; the post's own mapping is the only one materializing
	// this Page produced.
	communityMappings, err := h.objects.ListByActorDID(ctx, communityDID)
	require.NoError(t, err)
	for _, mapping := range communityMappings {
		assert.NotEqual(t, testAcceptanceCollection, mapping.Collection,
			"the acceptance must be invisible to ListByActorDID")
	}

	var forPage int
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM ap_objects WHERE ap_id = $1`, pageID).Scan(&forPage))
	assert.Equal(t, 1, forPage, "materializing one Page produces exactly one mapping: its postv2")
}

// TestAcceptanceRedeliveryDoesNotChurnCommunityRepo (A3): the acceptance rkey
// is deterministic and its content is fixed by the post's uri+CID, so
// re-materializing an unchanged post must re-put an IDENTICAL acceptance —
// a NoOp commit. If it churned (a fresh createdAt, or an unconditional
// commit), every redelivery would mint a community-repo firehose event, and
// Coves' consumers would re-run admission on a post nothing changed about.
//
// Red today on the shared precondition; the churn assertion bites once the
// write lands.
func TestAcceptanceRedeliveryDoesNotChurnCommunityRepo(t *testing.T) {
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
	acceptanceRKey := acceptanceFor(t, authorDID, rkey)

	acceptance, acceptanceCID, err := h.manager.GetRecord(ctx, communityDID, testAcceptanceCollection, acceptanceRKey)
	require.NoError(t, err, "precondition (outer contract): the first materialization writes an acceptance")

	eventsBefore := eventsForDID(t, h, communityDID)

	// Redeliver the identical post.
	second, err := h.m.MaterializePost(ctx, loadFixtureObject(t, "page_lemmy_world.json"))
	require.NoError(t, err)
	assert.True(t, second.NoOp, "an unchanged redelivery is a no-op at the postv2 commit")

	assert.Equal(t, eventsBefore, eventsForDID(t, h, communityDID),
		"an unchanged redelivery must not emit a community-repo firehose event: the acceptance "+
			"re-put is byte-identical, so it must reach the repo layer's NoOp path")

	rebuilt, rebuiltCID, err := h.manager.GetRecord(ctx, communityDID, testAcceptanceCollection, acceptanceRKey)
	require.NoError(t, err)
	assert.Equal(t, acceptanceCID, rebuiltCID, "the acceptance CID must not churn across redelivery")
	assert.Equal(t, acceptance["createdAt"], rebuilt["createdAt"],
		"createdAt must be derived from the post, not from wall-clock time at write")
}

// eventsForDID counts the firehose events belonging to one repo.
func eventsForDID(t *testing.T, h *harness, did string) int {
	t.Helper()
	var n int
	for _, event := range h.firehoseEvents() {
		if event.DID == did {
			n++
		}
	}
	return n
}
