package materialize

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
)

// asOf is a fixed sample time for the bridgedStats tests.
var statsAsOf = time.Date(2026, 7, 10, 9, 0, 0, 0, time.UTC)

// testValidCID is a well-formed CIDv1 for the lexicon-validation strongRefs
// (the validator checks CID syntax, not reachability).
const testValidCID = "bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqrsqxi3jgxte"

// TestSetBridgedStatsStampsRecord: SetBridgedStats writes the counts onto the
// record, keeps the ap_objects mapping CID in sync, and the stamped record
// still lexicon-validates (the harness runs StrictValidation).
func TestSetBridgedStatsStampsRecord(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	created, err := h.m.MaterializePost(ctx, loadFixtureObject(t, "page_lemmy_world.json"))
	require.NoError(t, err)
	require.False(t, created.NoOp)

	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)

	res, err := h.m.SetBridgedStats(ctx, mapping, 42, 3, statsAsOf)
	require.NoError(t, err)
	assert.False(t, res.NoOp, "the first stamp is a real commit")
	assert.NotEqual(t, created.CID, res.CID, "adding bridgedStats mints a new CID")

	record, _, err := h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	require.NoError(t, err)
	stats, ok := record["bridgedStats"].(map[string]any)
	require.True(t, ok, "the record carries bridgedStats")
	assert.EqualValues(t, 42, stats["upvotes"])
	assert.EqualValues(t, 3, stats["downvotes"])
	assert.Equal(t, recordDatetimeMicros(statsAsOf), stats["asOf"])

	// The mapping CID tracks the stamped version (new comments strongRef it).
	refreshed, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	assert.Equal(t, res.CID, refreshed.CID, "the mapping CID follows the stamped record")
}

// TestSetBridgedStatsUnchangedCountsNoOp: re-stamping the same counts with a
// LATER asOf is a no-op (no commit, no firehose event) — asOf-only churn must
// not hit the firehose.
func TestSetBridgedStatsUnchangedCountsNoOp(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	_, err := h.m.MaterializePost(ctx, loadFixtureObject(t, "page_lemmy_world.json"))
	require.NoError(t, err)
	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)

	first, err := h.m.SetBridgedStats(ctx, mapping, 10, 1, statsAsOf)
	require.NoError(t, err)
	require.False(t, first.NoOp)
	eventsAfterFirst := len(h.firehoseEvents())

	// Same counts, a later asOf: no commit, no new event.
	mapping, err = h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	second, err := h.m.SetBridgedStats(ctx, mapping, 10, 1, statsAsOf.Add(time.Hour))
	require.NoError(t, err)
	assert.True(t, second.NoOp, "unchanged counts must not commit")
	assert.Equal(t, eventsAfterFirst, len(h.firehoseEvents()), "no firehose event for an asOf-only bump")

	record, _, err := h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	require.NoError(t, err)
	stats := record["bridgedStats"].(map[string]any)
	assert.Equal(t, recordDatetimeMicros(statsAsOf), stats["asOf"], "asOf did not churn")
}

// TestSetBridgedStatsChangedCountsCommits: different counts DO commit and move
// asOf forward.
func TestSetBridgedStatsChangedCountsCommits(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	_, err := h.m.MaterializePost(ctx, loadFixtureObject(t, "page_lemmy_world.json"))
	require.NoError(t, err)
	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)

	_, err = h.m.SetBridgedStats(ctx, mapping, 10, 1, statsAsOf)
	require.NoError(t, err)
	mapping, err = h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)

	later := statsAsOf.Add(time.Hour)
	res, err := h.m.SetBridgedStats(ctx, mapping, 11, 1, later)
	require.NoError(t, err)
	assert.False(t, res.NoOp, "a changed upvote count commits")

	record, _, err := h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	require.NoError(t, err)
	stats := record["bridgedStats"].(map[string]any)
	assert.EqualValues(t, 11, stats["upvotes"])
	assert.Equal(t, recordDatetimeMicros(later), stats["asOf"])
}

// TestSetBridgedStatsRecordDeleted: a stamp for a record deleted out from
// under its aggregate returns the record-gone sentinel (the refresher's
// permanent skip) — distinct from a bare NotFound so an unrelated NotFound
// deeper in the commit stays transient.
func TestSetBridgedStatsRecordDeleted(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	_, err := h.m.MaterializePost(ctx, loadFixtureObject(t, "page_lemmy_world.json"))
	require.NoError(t, err)
	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)

	require.NoError(t, h.m.HandleDelete(ctx, pageID))

	_, err = h.m.SetBridgedStats(ctx, mapping, 5, 0, statsAsOf)
	assert.True(t, errors.IsRecordGone(err), "a stamp for a deleted record is record-gone, got %v", err)
	assert.False(t, errors.IsNotFound(err), "record-gone must NOT satisfy IsNotFound")
}

// TestEditCarriesBridgedStatsForward is the core carry-forward guarantee: a
// Lemmy EDIT rebuilds the record from AP data (which never carries stats), and
// the rebuild must NOT drop an existing bridgedStats field.
func TestEditCarriesBridgedStatsForward(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	page := loadFixtureObject(t, "page_lemmy_world.json")
	_, err := h.m.MaterializePost(ctx, page)
	require.NoError(t, err)
	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)

	// The refresher stamps the counts.
	_, err = h.m.SetBridgedStats(ctx, mapping, 40, 2, statsAsOf)
	require.NoError(t, err)

	// A later edit rebuilds from AP data with no stats field.
	edited := loadFixtureObject(t, "page_lemmy_world.json")
	edited.Source = &ap.Source{Content: "edited body text", MediaType: "text/markdown"}
	res, err := h.m.HandleUpdate(ctx, edited)
	require.NoError(t, err)
	require.False(t, res.NoOp, "an edited body is a real commit")

	record, _, err := h.manager.GetRecord(ctx, res.DID, CollectionPost, mapping.RKey)
	require.NoError(t, err)
	assert.Equal(t, "edited body text", record["content"], "the edit applied")
	stats, ok := record["bridgedStats"].(map[string]any)
	require.True(t, ok, "the edit must not drop bridgedStats")
	assert.EqualValues(t, 40, stats["upvotes"])
	assert.EqualValues(t, 2, stats["downvotes"])
	assert.Equal(t, recordDatetimeMicros(statsAsOf), stats["asOf"])
}

// TestUnchangedReingestAfterStatsIsNoOp: once stats are stamped, re-ingesting
// the IDENTICAL post (deterministic rkey → re-put) must stay an idempotent
// no-op — carry-forward keeps the rebuilt record byte-identical, so no
// spurious firehose event churns the counts away.
func TestUnchangedReingestAfterStatsIsNoOp(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()

	page := loadFixtureObject(t, "page_lemmy_world.json")
	_, err := h.m.MaterializePost(ctx, page)
	require.NoError(t, err)
	mapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	stamped, err := h.m.SetBridgedStats(ctx, mapping, 7, 1, statsAsOf)
	require.NoError(t, err)

	eventsAfterStamp := len(h.firehoseEvents())

	// Re-ingest the identical post (a re-delivery).
	again, err := h.m.MaterializePost(ctx, loadFixtureObject(t, "page_lemmy_world.json"))
	require.NoError(t, err)
	assert.True(t, again.NoOp, "an unchanged re-ingest after stamping must be a no-op")
	assert.Equal(t, stamped.CID, again.CID, "carry-forward keeps the CID identical")
	assert.Equal(t, eventsAfterStamp, len(h.firehoseEvents()), "no extra firehose event")
}

// TestBridgedStatsRecordsLexiconValidate pins that a bridgedStats-bearing post
// and comment validate against the vendored Coves lexicons, and that a
// malformed one (missing required asOf) is rejected — the synced #bridgedStats
// def is what the emission path relies on.
func TestBridgedStatsRecordsLexiconValidate(t *testing.T) {
	h := newHarness(t)
	stats := map[string]any{
		"upvotes":   int64(12),
		"downvotes": int64(4),
		"asOf":      recordDatetime(statsAsOf),
	}

	post := map[string]any{
		"$type":        CollectionPost,
		"community":    testServiceDID,
		"author":       testServiceDID,
		"createdAt":    recordDatetime(statsAsOf),
		"title":        "a bridged post",
		"bridgedStats": stats,
	}
	require.NoError(t, h.m.validateRecord(post), "a bridgedStats post must validate")

	comment := map[string]any{
		"$type": CollectionComment,
		"reply": map[string]any{
			"root":   strongRef("at://"+testServiceDID+"/"+CollectionPost+"/3jzfcijpj2z2a", testValidCID),
			"parent": strongRef("at://"+testServiceDID+"/"+CollectionPost+"/3jzfcijpj2z2a", testValidCID),
		},
		"content":      "a bridged comment",
		"createdAt":    recordDatetime(statsAsOf),
		"bridgedStats": stats,
	}
	require.NoError(t, h.m.validateRecord(comment), "a bridgedStats comment must validate")

	// Missing the required asOf is a validation error.
	bad := map[string]any{
		"$type":     CollectionPost,
		"community": testServiceDID,
		"author":    testServiceDID,
		"createdAt": recordDatetime(statsAsOf),
		"bridgedStats": map[string]any{
			"upvotes":   int64(1),
			"downvotes": int64(0),
		},
	}
	require.Error(t, h.m.validateRecord(bad), "bridgedStats without asOf must fail validation")
}

// TestBridgedStatsAsOfMicrosecondPrecision pins that asOf renders at
// microsecond precision: two aggregate versions in the SAME millisecond
// (concurrent voters under clock_timestamp) must produce DISTINCT asOf strings,
// or a consumer's newer-or-equal guard (and the watermark bookkeeping) would
// conflate them. The old millisecond dialect is the regression it guards.
func TestBridgedStatsAsOfMicrosecondPrecision(t *testing.T) {
	v1 := time.Date(2026, 7, 10, 9, 0, 0, 123456*1000, time.UTC) // …09:00:00.123456Z
	v2 := v1.Add(3 * time.Microsecond)                           // …09:00:00.123459Z
	require.Equal(t, v1.UnixMilli(), v2.UnixMilli(), "the two versions are in the same millisecond")

	assert.NotEqual(t, recordDatetimeMicros(v1), recordDatetimeMicros(v2),
		"two versions in the same ms must render distinct asOf strings at microsecond precision")
	assert.Equal(t, recordDatetime(v1), recordDatetime(v2),
		"millisecond precision would conflate them — this is the regression the microsecond asOf avoids")
}

// TestCommentEditCarriesReplyRefsForward is the reply-ref carry-forward
// guarantee (Coves treats reply root/parent CIDs as immutable across updates):
// after the parent post is stats-stamped — churning its CID — a Lemmy comment
// EDIT must reuse the ORIGINAL stored reply refs, not re-resolve them to the
// churned CID, or Coves rejects the rebuilt comment as thread hijacking.
func TestCommentEditCarriesReplyRefsForward(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	h.serveObject("/u/alice", person("https://lemmy.world/u/alice", "alice", nil))
	ctx := context.Background()

	_, err := h.m.MaterializePost(ctx, loadFixtureObject(t, "page_lemmy_world.json"))
	require.NoError(t, err)

	c1 := note("https://lemmy.world/comment/1001", "https://lemmy.world/u/alice",
		pageID, "original comment", "2026-07-07T04:00:00.000000Z")
	h.serveObject("/comment/1001", c1)
	_, err = h.m.MaterializeComment(ctx, objectFromMap(t, c1))
	require.NoError(t, err)

	c1Mapping, err := h.objects.GetByAPID(ctx, "https://lemmy.world/comment/1001")
	require.NoError(t, err)
	origRecord, _, err := h.manager.GetRecord(ctx, c1Mapping.DID, c1Mapping.Collection, c1Mapping.RKey)
	require.NoError(t, err)
	origRoot, ok := extractStrongRef(origRecord, "reply", "root")
	require.True(t, ok)
	origParent, ok := extractStrongRef(origRecord, "reply", "parent")
	require.True(t, ok)

	// Stamp the parent post: bridgedStats mints a new version, so the post's
	// mapping CID (what a re-resolve would return) changes.
	pageMapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err)
	stamped, err := h.m.SetBridgedStats(ctx, pageMapping, 9, 1, statsAsOf)
	require.NoError(t, err)
	require.NotEqual(t, pageMapping.CID, stamped.CID, "stamping the post changes its CID")

	// A Lemmy edit rebuilds the comment from AP data. The object is passed
	// directly (its parent is already mapped), so no re-fetch/re-serve is needed.
	edited := note("https://lemmy.world/comment/1001", "https://lemmy.world/u/alice",
		pageID, "edited comment body", "2026-07-07T04:00:00.000000Z")
	res, err := h.m.HandleUpdate(ctx, objectFromMap(t, edited))
	require.NoError(t, err)
	require.False(t, res.NoOp, "an edited body is a real commit")

	rebuilt, _, err := h.manager.GetRecord(ctx, c1Mapping.DID, c1Mapping.Collection, c1Mapping.RKey)
	require.NoError(t, err)
	assert.Equal(t, "edited comment body", rebuilt["content"], "the edit applied")
	gotRoot, ok := extractStrongRef(rebuilt, "reply", "root")
	require.True(t, ok)
	gotParent, ok := extractStrongRef(rebuilt, "reply", "parent")
	require.True(t, ok)
	assert.Equal(t, origRoot, gotRoot,
		"reply.root must be carried forward verbatim, NOT re-resolved to the churned post CID")
	assert.Equal(t, origParent, gotParent, "reply.parent must be carried forward verbatim")
}
