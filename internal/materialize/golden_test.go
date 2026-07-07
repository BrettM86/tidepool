package materialize

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Golden tests: the task-02 fixtures (real objects captured off lemmy.world
// / lemmy.zip) are materialized through the full pipeline and the produced
// records — read back from the repos — are compared byte-for-byte against
// testdata/*.golden.json. Everything in the pipeline is deterministic on
// purpose (deterministic DIDs from the fake minter, deterministic TID
// rkeys, fixed image bytes → fixed blob CIDs), so these files pin the exact
// wire-visible output of the materializer. Regenerate with:
//
//	GOLDEN_UPDATE=1 go test ./internal/materialize/ -run TestGolden
func assertGolden(t *testing.T, name string, record map[string]any) {
	t.Helper()
	got, err := json.MarshalIndent(record, "", "  ")
	require.NoError(t, err)
	got = append(got, '\n')

	path := filepath.Join("testdata", name+".golden.json")
	if os.Getenv("GOLDEN_UPDATE") != "" {
		require.NoError(t, os.MkdirAll("testdata", 0o755))
		require.NoError(t, os.WriteFile(path, got, 0o644))
		t.Logf("updated %s", path)
		return
	}
	want, err := os.ReadFile(path)
	require.NoError(t, err, "missing golden file; run with GOLDEN_UPDATE=1 to create it")
	require.Equal(t, string(want), string(got),
		"record diverged from %s (run with GOLDEN_UPDATE=1 to regenerate after intentional changes)", path)
}

func (h *harness) recordFor(t *testing.T, apID string) map[string]any {
	t.Helper()
	mapping, err := h.objects.GetByAPID(context.Background(), apID)
	require.NoError(t, err)
	record, _, err := h.manager.GetRecord(context.Background(),
		mapping.DID, mapping.Collection, mapping.RKey)
	require.NoError(t, err)
	return record
}

// TestGoldenPostAndProfiles pins the full output of materializing the
// captured lemmy.world Page: the community profile, the author profile
// (with avatar/banner blobs and the provenance bio line), and the post
// record (external embed with fetched thumbnail).
func TestGoldenPostAndProfiles(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()

	_, err := h.m.MaterializePost(context.Background(), loadFixtureObject(t, "page_lemmy_world.json"))
	require.NoError(t, err)

	assertGolden(t, "community_profile", h.recordFor(t, groupID))
	assertGolden(t, "actor_profile", h.recordFor(t, personID))
	assertGolden(t, "post", h.recordFor(t, pageID))
}

// TestGoldenComment pins the comment record for the captured lemmy.zip
// Note, whose parent chain (a sh.itjust.works comment under the lemmy.world
// page) is pulled in through the missing-parent protocol.
func TestGoldenComment(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	h.serveObject("/u/tixooo", person("https://lemmy.zip/u/tixooo", "tixooo", nil))
	h.serveObject("/u/DemandtheOxfordComma",
		person("https://sh.itjust.works/u/DemandtheOxfordComma", "DemandtheOxfordComma", nil))
	// The note fixture's real parent, synthesized as a reply to the page.
	h.serveObject("/comment/26248018", note(
		"https://sh.itjust.works/comment/26248018",
		"https://sh.itjust.works/u/DemandtheOxfordComma",
		pageID,
		"It has always been this way.",
		"2026-07-07T05:00:00.000000Z"))

	_, err := h.m.MaterializeComment(context.Background(), loadFixtureObject(t, "note_lemmy_zip.json"))
	require.NoError(t, err)

	assertGolden(t, "comment", h.recordFor(t, noteID))
	assertGolden(t, "comment_parent", h.recordFor(t, "https://sh.itjust.works/comment/26248018"))

	// The reply refs in the golden are real at-uris; double-check they
	// resolve through the mapping spine.
	parentURI, _, err := h.objects.ResolveStrongRef(context.Background(),
		"https://sh.itjust.works/comment/26248018")
	require.NoError(t, err)
	record := h.recordFor(t, noteID)
	parent, ok := extractStrongRef(record, "reply", "parent")
	require.True(t, ok)
	require.Equal(t, parentURI, parent["uri"])
}

// TestGoldenRecordsValidateAgainstLexicons re-validates every golden file
// against the vendored Coves lexicons — the golden corpus can never drift
// into shapes Coves would reject. (Materialization already validates in
// strict mode; this guards the checked-in files themselves.)
func TestGoldenRecordsValidateAgainstLexicons(t *testing.T) {
	entries, err := filepath.Glob(filepath.Join("testdata", "*.golden.json"))
	require.NoError(t, err)
	require.NotEmpty(t, entries, "golden files must exist (run GOLDEN_UPDATE=1 once)")

	// A materializer only for its validator (strict).
	h := newHarness(t)
	_ = h

	for _, path := range entries {
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		var record map[string]any
		require.NoError(t, json.Unmarshal(raw, &record))
		require.NoError(t, h.m.validateRecord(record), "golden file %s", path)
	}
}
