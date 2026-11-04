package materialize

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// serveThread registers a 3-deep synthetic comment chain hanging off the
// fixture page:
//
//	page (lemmy.world, fixture)
//	└─ c1 by alice@lemmy.world
//	   └─ c2 by bob@sh.itjust.works
//	      └─ c3 by carol@lemmy.zip
//
// and returns the leaf as an ap.Object. Only c3 is delivered; c2, c1, and
// the page must be pulled in by the missing-parent protocol.
func serveThread(t *testing.T, h *harness) *ap.Object {
	t.Helper()
	return serveThreadOptingOut(t, h, "")
}

// serveThreadOptingOut is serveThread with the named author's document
// carrying #nobridge.
func serveThreadOptingOut(t *testing.T, h *harness, optedOut string) *ap.Object {
	t.Helper()
	extra := func(username string) map[string]any {
		if username != optedOut {
			return nil
		}
		return map[string]any{"summary": "<p>#nobridge</p>"}
	}
	h.serveLemmyWorldFixtures()
	h.serveObject("/u/alice", person("https://lemmy.world/u/alice", "alice", extra("alice")))
	h.serveObject("/u/bob", person("https://sh.itjust.works/u/bob", "bob", extra("bob")))
	h.serveObject("/u/carol", person("https://lemmy.zip/u/carol", "carol", extra("carol")))

	c1 := note("https://lemmy.world/comment/1001", "https://lemmy.world/u/alice",
		pageID, "first!", "2026-07-07T04:00:00.000000Z")
	c2 := note("https://sh.itjust.works/comment/2002", "https://sh.itjust.works/u/bob",
		"https://lemmy.world/comment/1001", "replying to first", "2026-07-07T04:10:00.000000Z")
	c3 := note("https://lemmy.zip/comment/3003", "https://lemmy.zip/u/carol",
		"https://sh.itjust.works/comment/2002", "deep reply", "2026-07-07T04:20:00.000000Z")
	h.serveObject("/comment/1001", c1)
	h.serveObject("/comment/2002", c2)
	h.serveObject("/comment/3003", c3)

	obj := objectFromMap(t, c3)
	return obj
}

func objectFromMap(t *testing.T, m map[string]any) *ap.Object {
	t.Helper()
	raw, err := json.Marshal(m)
	require.NoError(t, err)
	obj, err := ap.ParseObject(raw)
	require.NoError(t, err)
	return obj
}

// TestMissingParentChainThreeDeep is the missing-parent-protocol money
// test: delivering only the deepest comment materializes the whole
// ancestor chain — page first, then each comment oldest-first — with
// correct reply.root/reply.parent strongRefs throughout.
func TestMissingParentChainThreeDeep(t *testing.T) {
	h := newHarness(t)
	leaf := serveThread(t, h)
	ctx := context.Background()

	res, err := h.m.MaterializeComment(ctx, leaf, groupID)
	require.NoError(t, err)

	pageMapping, err := h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err, "the root page must have been fetched and materialized")
	c1Mapping, err := h.objects.GetByAPID(ctx, "https://lemmy.world/comment/1001")
	require.NoError(t, err)
	c2Mapping, err := h.objects.GetByAPID(ctx, "https://sh.itjust.works/comment/2002")
	require.NoError(t, err)
	c3Mapping, err := h.objects.GetByAPID(ctx, "https://lemmy.zip/comment/3003")
	require.NoError(t, err)
	assert.Equal(t, c3Mapping.ATURI, res.ATURI)

	// Comments live in their AUTHORS' repos.
	assert.Equal(t, testDIDFor("alice", "lemmy.world"), c1Mapping.DID)
	assert.Equal(t, testDIDFor("bob", "sh.itjust.works"), c2Mapping.DID)
	assert.Equal(t, testDIDFor("carol", "lemmy.zip"), c3Mapping.DID)

	// reply refs: root always points at the page; parent at the immediate
	// ancestor.
	assertReply := func(mapping *store.APObjectMapping, wantRootURI, wantParentURI string) {
		record, _, err := h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
		require.NoError(t, err)
		root, ok := extractStrongRef(record, "reply", "root")
		require.True(t, ok)
		parent, ok := extractStrongRef(record, "reply", "parent")
		require.True(t, ok)
		assert.Equal(t, wantRootURI, root["uri"])
		assert.Equal(t, wantParentURI, parent["uri"])
	}
	assertReply(c1Mapping, pageMapping.ATURI, pageMapping.ATURI)
	assertReply(c2Mapping, pageMapping.ATURI, c1Mapping.ATURI)
	assertReply(c3Mapping, pageMapping.ATURI, c2Mapping.ATURI)

	// Emission order: every profile precedes the content that references
	// it, and ancestors precede descendants.
	var order []string
	for _, evt := range h.firehoseEvents() {
		for _, op := range evt.Ops {
			order = append(order, evt.DID+"/"+op.Path)
		}
	}
	indexOf := func(did, pathPrefix string) int {
		for i, entry := range order {
			if entry == did+"/"+pathPrefix ||
				(len(entry) > len(did+"/"+pathPrefix) && entry[:len(did+"/"+pathPrefix)] == did+"/"+pathPrefix) {
				return i
			}
		}
		t.Fatalf("no firehose op for %s/%s in %v", did, pathPrefix, order)
		return -1
	}
	pageIdx := indexOf(pageMapping.DID, CollectionPost)
	c1Idx := indexOf(c1Mapping.DID, CollectionComment)
	c2Idx := indexOf(c2Mapping.DID, CollectionComment)
	c3Idx := indexOf(c3Mapping.DID, CollectionComment)
	assert.Less(t, pageIdx, c1Idx, "page before first comment")
	assert.Less(t, c1Idx, c2Idx, "ancestors before descendants")
	assert.Less(t, c2Idx, c3Idx, "ancestors before descendants")
}

// TestMaterializeComment_ReportsCommittedAncestors: an error return must not
// lose them. Each is committed whatever became of the comment, and only the
// caller can recheck what happened to it during a wait.
func TestMaterializeComment_ReportsCommittedAncestors(t *testing.T) {
	const (
		c1 = "https://lemmy.world/comment/1001"
		c2 = "https://sh.itjust.works/comment/2002"
	)
	acceptanceFailure := stderrors.New("acceptance refused")

	for _, tc := range []struct {
		name string
		// optedOut is the author whose document carries #nobridge.
		optedOut string
		// pageMapped commits the root page first, so the chain anchors on it.
		pageMapped bool
		// failsAcceptance fails every post's acceptance.
		failsAcceptance bool
		skipped         bool
		ancestors       []string
	}{
		{name: "whole chain", ancestors: []string{pageID, c1, c2}},
		{name: "chain anchored on a mapped page", pageMapped: true, ancestors: []string{c1, c2}},
		{name: "comment refused for its author", optedOut: "carol", skipped: true, ancestors: []string{pageID, c1, c2}},
		{name: "ancestor refused for its author", optedOut: "bob", skipped: true, ancestors: []string{pageID, c1}},
		{name: "first ancestor refused", optedOut: "alice", pageMapped: true, skipped: true},
		{name: "root page committed but its acceptance failed", failsAcceptance: true, ancestors: []string{pageID}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			leaf := serveThreadOptingOut(t, h, tc.optedOut)
			if tc.pageMapped {
				_, err := h.m.MaterializePost(context.Background(), loadFixtureObject(t, "page_lemmy_world.json"), groupID)
				require.NoError(t, err)
			}
			if tc.failsAcceptance {
				h.m.removalCheck = func(context.Context, string, string) (bool, error) {
					return false, acceptanceFailure
				}
			}

			res, err := h.m.MaterializeComment(context.Background(), leaf, groupID)

			switch {
			case tc.skipped:
				require.True(t, IsSkip(err), "got %v", err)
			case tc.failsAcceptance:
				require.ErrorIs(t, err, acceptanceFailure)
			default:
				require.NoError(t, err)
			}
			if tc.ancestors == nil {
				assert.Nil(t, res, "nothing committed, so nothing to report")
				return
			}
			require.NotNil(t, res)
			assert.Equal(t, tc.ancestors, res.Ancestors)
			for _, id := range tc.ancestors {
				_, err := h.objects.GetByAPID(context.Background(), id)
				assert.NoError(t, err, "%s is reported as committed", id)
			}
		})
	}
}

// TestCommentCycleGuard: an inReplyTo cycle is detected during the walk —
// before anything is written — and the subtree is dropped.
func TestCommentCycleGuard(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	h.serveObject("/u/alice", person("https://lemmy.world/u/alice", "alice", nil))
	a := note("https://lemmy.world/comment/9001", "https://lemmy.world/u/alice",
		"https://lemmy.world/comment/9002", "a", "2026-07-07T04:00:00.000000Z")
	b := note("https://lemmy.world/comment/9002", "https://lemmy.world/u/alice",
		"https://lemmy.world/comment/9001", "b", "2026-07-07T04:01:00.000000Z")
	h.serveObject("/comment/9001", a)
	h.serveObject("/comment/9002", b)

	_, err := h.m.MaterializeComment(context.Background(), objectFromMap(t, a), groupID)
	require.Error(t, err)
	assert.True(t, IsSkip(err), "a cycle must be a skip: %v", err)
	assert.Empty(t, h.firehoseEvents(), "cycle detection happens before any write")
}

// TestCommentDepthCap: a chain deeper than maxAncestorDepth is dropped.
func TestCommentDepthCap(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	h.serveObject("/u/alice", person("https://lemmy.world/u/alice", "alice", nil))

	depth := maxAncestorDepth + 3
	for i := 1; i <= depth; i++ {
		parent := pageID
		if i > 1 {
			parent = fmt.Sprintf("https://lemmy.world/comment/d%d", i-1)
		}
		h.serveObject(fmt.Sprintf("/comment/d%d", i),
			note(fmt.Sprintf("https://lemmy.world/comment/d%d", i),
				"https://lemmy.world/u/alice", parent,
				fmt.Sprintf("depth %d", i), "2026-07-07T04:00:00.000000Z"))
	}

	leafBody := note(fmt.Sprintf("https://lemmy.world/comment/d%d", depth),
		"https://lemmy.world/u/alice",
		fmt.Sprintf("https://lemmy.world/comment/d%d", depth-1),
		"too deep", "2026-07-07T05:00:00.000000Z")
	_, err := h.m.MaterializeComment(context.Background(), objectFromMap(t, leafBody), groupID)
	require.Error(t, err)
	assert.True(t, IsSkip(err), "over-deep chains must be skipped: %v", err)
	assert.Empty(t, h.firehoseEvents(), "the cap fires before any write")
}

// TestTombstonedParentDropsSubtree: a soft-deleted parent mapping means the
// reply subtree is dropped, never re-fetched.
func TestTombstonedParentDropsSubtree(t *testing.T) {
	h := newHarness(t)
	leaf := serveThread(t, h)
	ctx := context.Background()

	// Materialize the whole thread, then tombstone the middle comment.
	_, err := h.m.MaterializeComment(ctx, leaf, groupID)
	require.NoError(t, err)
	require.NoError(t, h.objects.SoftDelete(ctx, "https://sh.itjust.works/comment/2002"))

	// A new reply under the tombstoned comment is dropped.
	h.serveObject("/comment/4004", note("https://lemmy.zip/comment/4004",
		"https://lemmy.zip/u/carol", "https://sh.itjust.works/comment/2002",
		"reply to deleted", "2026-07-07T06:00:00.000000Z"))
	reply := objectFromMap(t, note("https://lemmy.zip/comment/4004",
		"https://lemmy.zip/u/carol", "https://sh.itjust.works/comment/2002",
		"reply to deleted", "2026-07-07T06:00:00.000000Z"))

	_, err = h.m.MaterializeComment(ctx, reply, groupID)
	require.Error(t, err)
	assert.True(t, IsSkip(err), "tombstoned parent must drop the subtree: %v", err)
	assert.True(t, errsIsNotFoundFalse(err), "a tombstone is not a missing parent")
	assert.Equal(t, 0, countMappings(t, h, "https://lemmy.zip/comment/4004"))
}

// TestCommentWithUnfetchableParent: parent 404s upstream → subtree dropped.
func TestCommentWithUnfetchableParent(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	h.serveObject("/u/carol", person("https://lemmy.zip/u/carol", "carol", nil))
	// /comment/nowhere is never registered → fixture server 404s it.

	leaf := objectFromMap(t, note("https://lemmy.zip/comment/5005",
		"https://lemmy.zip/u/carol", "https://lemmy.world/comment/nowhere",
		"orphan", "2026-07-07T06:00:00.000000Z"))
	_, err := h.m.MaterializeComment(context.Background(), leaf, groupID)
	require.Error(t, err)
	assert.True(t, IsSkip(err), "unfetchable parent must skip the subtree: %v", err)
}

func errsIsNotFoundFalse(err error) bool { return !errors.IsNotFound(err) }
