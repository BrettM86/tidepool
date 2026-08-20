package materialize

import (
	"context"
	"crypto/sha256"
	"encoding/base32"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/errors"
)

// Collections the flip targets. Spelled out as literals (not the package's
// constants) so this outer contract keeps asserting the WIRE names Coves'
// consumers subscribe to even if the constants are renamed or re-pointed.
const (
	testPostV2Collection     = "social.coves.community.postv2"
	testAcceptanceCollection = "social.coves.community.acceptance"
	testLegacyPostCollection = "social.coves.community.post"
)

// testSubjectRkeyEncoding / testSubjectRkey are an INDEPENDENT re-derivation
// of the acceptance record key — unpadded lowercase base32 of the SHA-256
// digest of the canonical subject at-uri (fixed 52 chars). Deliberately not a
// call into any production helper: a test that used the helper could not
// detect a bug in it, because both sides of the comparison would move
// together. This mirrors Coves' own contract tier
// (github.com/BrettM86/coves, tests/e2e/author_post_contract_test.go), which re-derives for
// exactly this reason; a silent fork here forks acceptance identity between
// the two systems.
var testSubjectRkeyEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

func testSubjectRkey(subjectURI string) string {
	digest := sha256.Sum256([]byte(subjectURI))
	return strings.ToLower(testSubjectRkeyEncoding.EncodeToString(digest[:]))
}

// TestOuterAcceptance_PostV2Flip is the outer contract of the postv2 flip
// (task 19, PLAN.md decision 20): a Lemmy Page arriving through the
// materializer for a bridged community must produce
//
//   - a social.coves.community.postv2 record in the AUTHOR's repo at the
//     deterministic TID rkey, with no in-record `author` (authorship IS the
//     repo) and the bridge's originalAuthor/federatedFrom provenance,
//   - a social.coves.community.acceptance record in the COMMUNITY's repo at
//     the digest rkey, strongRef-pinning that exact postv2 version,
//   - an ap_objects mapping pointing at the author repo and the postv2
//     collection,
//   - and NOTHING under the deprecated social.coves.community.post
//     collection in either repo.
func TestOuterAcceptance_PostV2Flip(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	page := loadFixtureObject(t, "page_lemmy_world.json")
	rkey, err := recordRKey(page)
	require.NoError(t, err)

	res, err := h.m.MaterializePost(ctx, page)
	require.NoError(t, err)
	require.False(t, res.NoOp)

	communityDID := testDIDFor("technology", "lemmy.world")
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")

	// ---- 1. postv2 lives in the AUTHOR's repo -----------------------------
	assert.Equal(t, authorDID, res.DID, "postv2 records live in the AUTHOR's repo, not the community's")

	post, postCID, err := h.manager.GetRecord(ctx, authorDID, testPostV2Collection, rkey)
	require.NoError(t, err,
		"expected a %s record in the author repo %s at rkey %s", testPostV2Collection, authorDID, rkey)
	require.NotEmpty(t, postCID)

	assert.Equal(t, testPostV2Collection, post["$type"])
	assert.Equal(t, communityDID, post["community"], "postv2.community is the community's DID")
	assert.NotEmpty(t, post["createdAt"], "postv2.createdAt is required")
	assert.NotContains(t, post, "author", "postv2 carries NO author field — authorship is the repo")
	assert.Contains(t, post["title"], "DRAM price-fixing", "post content is carried over unchanged")

	originalAuthor, ok := post["originalAuthor"].(map[string]any)
	require.True(t, ok, "postv2.originalAuthor must be a populated object, got %#v", post["originalAuthor"])
	assert.Equal(t, personID, originalAuthor["apId"], "originalAuthor.apId is the origin actor IRI")
	assert.Equal(t, "lemmy.world", originalAuthor["instance"])
	handle, _ := originalAuthor["handle"].(string)
	assert.Contains(t, strings.ToLower(handle), "leftleaningfreedomfighters",
		"originalAuthor.handle names the origin-platform account")

	federatedFrom, ok := post["federatedFrom"].(map[string]any)
	require.True(t, ok, "postv2.federatedFrom must be a populated object, got %#v", post["federatedFrom"])
	assert.Equal(t, "lemmy", federatedFrom["platform"])
	assert.Equal(t, "lemmy.world", federatedFrom["instance"])

	// ---- 2. acceptance lives in the COMMUNITY's repo ----------------------
	postURI := "at://" + authorDID + "/" + testPostV2Collection + "/" + rkey
	assert.Equal(t, postURI, res.ATURI, "the result at-uri is the author-repo postv2 uri")

	acceptanceRKey := testSubjectRkey(postURI)
	require.Len(t, acceptanceRKey, 52, "digest rkeys are a fixed 52 chars")

	acceptance, _, err := h.manager.GetRecord(ctx, communityDID, testAcceptanceCollection, acceptanceRKey)
	require.NoError(t, err,
		"expected a %s record in the community repo %s at digest rkey %s",
		testAcceptanceCollection, communityDID, acceptanceRKey)

	assert.Equal(t, testAcceptanceCollection, acceptance["$type"])
	assert.NotEmpty(t, acceptance["createdAt"], "acceptance.createdAt is required")
	subject, ok := acceptance["subject"].(map[string]any)
	require.True(t, ok, "acceptance.subject must be a strongRef, got %#v", acceptance["subject"])
	assert.Equal(t, postURI, subject["uri"], "acceptance pins the postv2 at-uri")
	assert.Equal(t, postCID, cidString(subject["cid"]),
		"acceptance pins the exact accepted postv2 version")

	// ---- 3. the ap_objects mapping points at the author repo -------------
	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	assert.Equal(t, authorDID, mapping.DID, "the mapping's repo is the author's")
	assert.Equal(t, testPostV2Collection, mapping.Collection)
	assert.Equal(t, rkey, mapping.RKey)

	// ---- 4. nothing under the deprecated collection ----------------------
	for _, did := range []string{communityDID, authorDID} {
		_, _, err := h.manager.GetRecord(ctx, did, testLegacyPostCollection, rkey)
		assert.True(t, errors.IsNotFound(err),
			"no record may be written under the deprecated %s collection in %s (err=%v)",
			testLegacyPostCollection, did, err)
	}
	for _, did := range []string{communityDID, authorDID} {
		entries, err := h.manager.ListRecords(ctx, did)
		require.NoError(t, err)
		for _, entry := range entries {
			assert.NotEqual(t, testLegacyPostCollection, entry.Collection,
				"repo %s still holds a deprecated post record at %s", did, entry.Rkey)
		}
	}
}

// cidString normalizes a strongRef cid value: repo records round-trip through
// the atproto data model, where a CID-link may surface as a typed value or as
// the {"$link": "..."} JSON form.
func cidString(v any) string {
	switch value := v.(type) {
	case string:
		return value
	case map[string]any:
		if link, ok := value["$link"].(string); ok {
			return link
		}
	case interface{ String() string }:
		return value.String()
	}
	return ""
}
