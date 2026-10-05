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

// Community binding. Content reaches the bridge because a community delivered
// it, and that community is the only one the delivery can speak for. Every
// entry point therefore takes the bound community's AP Group IRI, and content
// that would land anywhere else — a Page naming another Group, a comment whose
// thread lives in another community, an edit retargeting stored content — is
// refused before anything is minted.
//
// The world these tests share: C and D are CO-HOSTED communities on
// lemmy.world, so nothing about authority tells them apart; the content and
// its authors live on a separate host, lemmy.zip. Every Group and Person a
// call could touch is served as a valid document, so a refusal can only come
// from the binding — never from a missing fixture or a consent marker.
const (
	bindingCommunityC   = groupID      // https://lemmy.world/c/technology
	bindingCommunityD   = otherGroupID // https://lemmy.world/c/elsewhere
	bindingUnknownGroup = "https://lemmy.world/c/strangers"
	bindingAuthor       = "https://lemmy.zip/u/xavier"
	bindingReplier      = "https://lemmy.zip/u/yvonne"
	bindingChainAuthor  = "https://lemmy.zip/u/zora"
)

// serveBindingWorld serves the Groups and Persons described above.
func serveBindingWorld(h *harness) {
	h.serveFixture("/c/technology", "group_lemmy_world.json")
	h.serveObject("/c/elsewhere", group(bindingCommunityD, "elsewhere", nil))
	h.serveObject("/c/strangers", group(bindingUnknownGroup, "strangers", nil))
	h.serveObject("/u/xavier", person(bindingAuthor, "xavier", nil))
	h.serveObject("/u/yvonne", person(bindingReplier, "yvonne", nil))
	h.serveObject("/u/zora", person(bindingChainAuthor, "zora", nil))
}

// ensureCommunities bridges the given Groups, the state a followed community
// is in before it delivers anything.
func ensureCommunities(t *testing.T, h *harness, groupIRIs ...string) {
	t.Helper()
	for _, iri := range groupIRIs {
		_, err := h.m.EnsureCommunity(context.Background(), &ap.Object{ID: iri})
		require.NoError(t, err)
	}
}

// noteIn is note() addressed to a given community rather than groupID.
func noteIn(id, author, inReplyTo, community, markdown, published string) map[string]any {
	doc := note(id, author, inReplyTo, markdown, published)
	doc["audience"] = community
	return doc
}

// contentState is everything a materialization can create or change: one
// entry per mapping (with the CID and community binding it currently holds),
// every bridged actor, every community row, and the firehose length.
type contentState struct {
	Mappings    []string
	Actors      []string
	Communities []string
	Events      int
}

func captureContentState(t *testing.T, h *harness) contentState {
	t.Helper()
	db := testutil.DB(t)
	column := func(query string) []string {
		rows, err := db.Query(query)
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()
		var out []string
		for rows.Next() {
			var value string
			require.NoError(t, rows.Scan(&value))
			out = append(out, value)
		}
		require.NoError(t, rows.Err())
		return out
	}
	return contentState{
		Mappings: column(`SELECT ap_id || ' ' || cid || ' ' || COALESCE(community_did, '-') || ' ' ||
			(deleted_at IS NULL)::text FROM ap_objects ORDER BY ap_id`),
		Actors:      column(`SELECT ap_actor_id FROM bridged_actors ORDER BY ap_actor_id`),
		Communities: column(`SELECT ap_group_id FROM communities ORDER BY ap_group_id`),
		Events:      len(h.firehoseEvents()),
	}
}

func assertActorAbsent(t *testing.T, h *harness, apActorID string) {
	t.Helper()
	_, err := h.actors.GetByAPActorID(context.Background(), apActorID)
	assert.True(t, errors.IsNotFound(err), "actor %s must not be bridged (err=%v)", apActorID, err)
}

func assertCommunityAbsent(t *testing.T, h *harness, apGroupID string) {
	t.Helper()
	_, err := h.communities.GetByAPGroupID(context.Background(), apGroupID)
	assert.True(t, errors.IsNotFound(err), "community %s must not have a row (err=%v)", apGroupID, err)
	assertActorAbsent(t, h, apGroupID)
}

// clearCommunityColumn turns a mapping into a pre-016 row, whose community
// binding is derived from its record rather than read off the column.
func clearCommunityColumn(t *testing.T, apID string) {
	t.Helper()
	res, err := testutil.DB(t).Exec(`UPDATE ap_objects SET community_did = NULL WHERE ap_id = $1`, apID)
	require.NoError(t, err)
	n, err := res.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, 1, n, "no mapping to clear for %s", apID)
}

// TestPostBoundToAnotherCommunityIsRefused (B1): a Page delivered for C that
// names another Group is not C's to deliver. It is refused before the Group
// or the author is bridged — whether the Group it names is a co-hosted
// community the bridge already follows or one it has never seen.
func TestPostBoundToAnotherCommunityIsRefused(t *testing.T) {
	cases := []struct {
		name     string
		audience string
	}{
		{name: "page names co-hosted community D", audience: bindingCommunityD},
		{name: "page names an unknown Group", audience: bindingUnknownGroup},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			serveBindingWorld(h)
			ensureCommunities(t, h, bindingCommunityC, bindingCommunityD)
			ctx := context.Background()

			const postID = "https://lemmy.zip/post/91001"
			post := page(postID, bindingAuthor, tc.audience, "addressed elsewhere", "2026-10-01T10:00:00.000000Z")
			h.serveObject("/post/91001", post)
			before := captureContentState(t, h)

			res, err := h.m.MaterializePost(ctx, mustObject(t, post), bindingCommunityC)
			require.Error(t, err, "C may not deliver a post that names %s", tc.audience)
			assert.Nil(t, res)
			assert.True(t, IsSkip(err), "a post outside the bound community is a skip, got %v", err)

			assert.Equal(t, before, captureContentState(t, h), "a refused post writes nothing")
			assert.Equal(t, 0, countMappings(t, h, postID))
			assertActorAbsent(t, h, bindingAuthor)
			assertCommunityAbsent(t, h, bindingUnknownGroup)
		})
	}
}

// TestCommentBoundToAnotherCommunityIsRefused (B3, B4, B6): a comment
// delivered for C must hang in a thread that belongs to C. The thread is
// decided by the comment's ancestry, so whichever way that ancestry leaves C —
// an unmapped root Page naming another Group, an already-mapped parent that
// belongs to D, a fetched ancestor whose body claims a comment stored in D, or
// a Page naming D that itself replies into C — the whole delivery is refused
// before any ancestor is materialized or rewritten, or any author is bridged. A parent whose bound community C has
// no communities row cannot be checked at all, and fails closed.
//
// Every leaf claims C as its own audience, the way a hostile or confused
// delivery would.
func TestCommentBoundToAnotherCommunityIsRefused(t *testing.T) {
	const (
		postInD    = "https://lemmy.zip/post/94001"
		commentInD = "https://lemmy.zip/comment/94002"
		leafID     = "https://lemmy.zip/comment/94099"
	)
	// threadInD materializes a post and a comment in D, delivered by D.
	threadInD := func(t *testing.T, h *harness) {
		t.Helper()
		ctx := context.Background()
		post := page(postInD, bindingAuthor, bindingCommunityD, "a thread in D", "2026-10-01T11:00:00.000000Z")
		h.serveObject("/post/94001", post)
		_, err := h.m.MaterializePost(ctx, mustObject(t, post), bindingCommunityD)
		require.NoError(t, err)
		comment := noteIn(commentInD, bindingAuthor, postInD, bindingCommunityD,
			"a comment in D", "2026-10-01T11:01:00.000000Z")
		h.serveObject("/comment/94002", comment)
		_, err = h.m.MaterializeComment(ctx, mustObject(t, comment), bindingCommunityD)
		require.NoError(t, err)
	}
	// postInC materializes a post in C, delivered by C, and returns its id.
	postInC := func(t *testing.T, h *harness) string {
		t.Helper()
		const anchorInC = "https://lemmy.zip/post/95001"
		post := page(anchorInC, bindingAuthor, bindingCommunityC, "a thread in C", "2026-10-01T10:30:00.000000Z")
		h.serveObject("/post/95001", post)
		_, err := h.m.MaterializePost(context.Background(), mustObject(t, post), bindingCommunityC)
		require.NoError(t, err)
		return anchorInC
	}
	// unmappedRootIn serves (without materializing) a root Page naming the
	// given Group and a comment under it, and returns that comment's id.
	unmappedRootIn := func(h *harness, community string) string {
		const rootID = "https://lemmy.zip/post/93001"
		const middleID = "https://lemmy.zip/comment/93002"
		h.serveObject("/post/93001", page(rootID, bindingAuthor, community, "a root elsewhere",
			"2026-10-01T12:00:00.000000Z"))
		h.serveObject("/comment/93002", noteIn(middleID, bindingChainAuthor, rootID, community,
			"a reply elsewhere", "2026-10-01T12:01:00.000000Z"))
		return middleID
	}

	cases := []struct {
		name string
		// prepare builds the world and returns the leaf's inReplyTo.
		prepare func(t *testing.T, h *harness) string
		// absent are mappings the refused call would otherwise have created.
		absent []string
		// verify, when set, checks stored content the refusal must leave alone.
		verify func(t *testing.T, h *harness)
		// leaf, when set, replaces the default leaf delivered under parentID.
		leaf func(parentID string) map[string]any
	}{
		{
			name: "unmapped root page names co-hosted community D",
			prepare: func(t *testing.T, h *harness) string {
				return unmappedRootIn(h, bindingCommunityD)
			},
			absent: []string{"https://lemmy.zip/post/93001", "https://lemmy.zip/comment/93002"},
		},
		{
			name: "unmapped root page names an unknown Group",
			prepare: func(t *testing.T, h *harness) string {
				return unmappedRootIn(h, bindingUnknownGroup)
			},
			absent: []string{"https://lemmy.zip/post/93001", "https://lemmy.zip/comment/93002"},
		},
		{
			name: "parent is a mapped post in D",
			prepare: func(t *testing.T, h *harness) string {
				threadInD(t, h)
				return postInD
			},
		},
		{
			name: "parent is a mapped comment in D",
			prepare: func(t *testing.T, h *harness) string {
				threadInD(t, h)
				return commentInD
			},
		},
		{
			name: "unmapped comment chain anchors on a comment in D",
			prepare: func(t *testing.T, h *harness) string {
				threadInD(t, h)
				const middleID = "https://lemmy.zip/comment/94003"
				h.serveObject("/comment/94003", noteIn(middleID, bindingChainAuthor, commentInD,
					bindingCommunityD, "a reply in D", "2026-10-01T11:02:00.000000Z"))
				return middleID
			},
			absent: []string{"https://lemmy.zip/comment/94003"},
		},
		{
			name: "parent is a legacy comment in D with no community_did column",
			prepare: func(t *testing.T, h *harness) string {
				threadInD(t, h)
				clearCommunityColumn(t, postInD)
				clearCommunityColumn(t, commentInD)
				return commentInD
			},
		},
		{
			// The leaf's parent is an unmapped IRI on D's comment host whose
			// body claims the id of D's stored comment — same authority, so the
			// id is accepted — and re-parents it under a post in C. The walk
			// would anchor in C and then commit that body over D's comment.
			name: "unmapped parent IRI serves a body aliasing a stored comment in D",
			prepare: func(t *testing.T, h *harness) string {
				threadInD(t, h)
				anchorInC := postInC(t, h)
				const aliasIRI = "https://lemmy.zip/comment/95010"
				h.serveObject("/comment/95010", noteIn(commentInD, bindingAuthor, anchorInC, bindingCommunityC,
					"a comment rewritten into C", "2026-10-01T11:01:00.000000Z"))
				return aliasIRI
			},
			absent: []string{"https://lemmy.zip/comment/95010"},
			verify: func(t *testing.T, h *harness) {
				ctx := context.Background()
				mapping, err := h.objects.GetByAPID(ctx, commentInD)
				require.NoError(t, err)
				record, _, err := h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
				require.NoError(t, err)
				assert.Equal(t, "a comment in D", record["content"], "D's comment keeps its own content")
				resolved, err := CommunityDIDOf(ctx, h.manager, mapping)
				require.NoError(t, err)
				assert.Equal(t, testDIDFor("elsewhere", "lemmy.world"), resolved, "D's comment stays in D")
			},
		},
		{
			// A fetched Page ends the walk even when it carries an inReplyTo.
			// This one names D and points at an unmapped comment under a post
			// in C: the walk must refuse it on fetch, before that older comment
			// is fetched or written.
			name: "unmapped page naming D replies to an unmapped comment in C",
			prepare: func(t *testing.T, h *harness) string {
				anchorInC := postInC(t, h)
				const (
					pageInD    = "https://lemmy.zip/post/95101"
					commentInC = "https://lemmy.zip/comment/95102"
				)
				h.serveObject("/comment/95102", noteIn(commentInC, bindingChainAuthor, anchorInC, bindingCommunityC,
					"a comment in C", "2026-10-01T10:31:00.000000Z"))
				pageDoc := page(pageInD, bindingAuthor, bindingCommunityD, "a page in D with a parent",
					"2026-10-01T10:32:00.000000Z")
				pageDoc["inReplyTo"] = commentInC
				h.serveObject("/post/95101", pageDoc)
				return pageInD
			},
			absent: []string{"https://lemmy.zip/post/95101", "https://lemmy.zip/comment/95102"},
			verify: func(t *testing.T, h *harness) {
				assert.NotZero(t, h.hitCount("/post/95101"), "control: the page itself was fetched")
				assert.Equal(t, 0, h.hitCount("/comment/95102"), "nothing older than the Page is fetched")
			},
		},
		{
			// The leaf's parent X is unmapped and serves a Note claiming another
			// unmapped id X' on the same host, under a Page in C. Neither id is
			// stored, so nothing anchors the alias: committing it would bind
			// content fetched from X under X'. Refused before anything is
			// minted — even though X' itself serves a valid Note in C.
			name: "unmapped parent IRI serves a body aliasing another unmapped id",
			prepare: func(t *testing.T, h *harness) string {
				const (
					pageInC   = "https://lemmy.zip/post/95201"
					aliasIRI  = "https://lemmy.zip/comment/95202"
					aliasedID = "https://lemmy.zip/comment/95203"
				)
				h.serveObject("/post/95201", page(pageInC, bindingAuthor, bindingCommunityC, "a page in C",
					"2026-10-01T10:50:00.000000Z"))
				aliased := noteIn(aliasedID, bindingChainAuthor, pageInC, bindingCommunityC,
					"a comment under another id", "2026-10-01T10:51:00.000000Z")
				h.serveObject("/comment/95202", aliased)
				h.serveObject("/comment/95203", aliased)
				return aliasIRI
			},
			absent: []string{"https://lemmy.zip/post/95201", "https://lemmy.zip/comment/95202",
				"https://lemmy.zip/comment/95203"},
		},
		{
			// The leaf is refused on its own terms (attributedTo on another
			// host than its id), so the unmapped chain above it — all in C —
			// must not be materialized on its way to that refusal.
			name: "leaf with a cross-authority author over an unmapped chain in C",
			prepare: func(t *testing.T, h *harness) string {
				h.serveObject("/u/mallory", person("https://lemmy.world/u/mallory", "mallory", nil))
				return unmappedRootIn(h, bindingCommunityC)
			},
			leaf: func(parentID string) map[string]any {
				return note(leafID, "https://lemmy.world/u/mallory", parentID, "a reply signed by another host",
					"2026-10-01T14:00:00.000000Z")
			},
			absent: []string{"https://lemmy.zip/post/93001", "https://lemmy.zip/comment/93002"},
		},
		{
			// The refusal sits in the MIDDLE of the unmapped chain: M, between
			// A and the leaf, is attributed to a person on another host than
			// its own id. Every ancestor is in C, and the chain is refused all
			// the same — so neither the Page nor A above M may be committed on
			// the way to M's refusal, nor their authors bridged.
			name: "middle ancestor with a cross-authority author in an unmapped chain in C",
			prepare: func(t *testing.T, h *harness) string {
				const middleID = "https://lemmy.zip/comment/93003"
				h.serveObject("/u/mallory", person("https://lemmy.world/u/mallory", "mallory", nil))
				h.serveObject("/comment/93003", noteIn(middleID, "https://lemmy.world/u/mallory",
					unmappedRootIn(h, bindingCommunityC), bindingCommunityC, "a reply signed by another host",
					"2026-10-01T12:02:00.000000Z"))
				return middleID
			},
			absent: []string{"https://lemmy.zip/post/93001", "https://lemmy.zip/comment/93002",
				"https://lemmy.zip/comment/93003"},
			verify: func(t *testing.T, h *harness) {
				assertActorAbsent(t, h, bindingAuthor)
				assertActorAbsent(t, h, "https://lemmy.world/u/mallory")
			},
		},
		{
			// Same, for a leaf with no content: a comment is nothing but its
			// content, so it will be dropped, and its ancestors with it.
			name: "leaf with no content over an unmapped chain in C",
			prepare: func(t *testing.T, h *harness) string {
				return unmappedRootIn(h, bindingCommunityC)
			},
			leaf: func(parentID string) map[string]any {
				doc := note(leafID, bindingReplier, parentID, "", "2026-10-01T14:00:00.000000Z")
				doc["content"] = ""
				delete(doc, "source")
				return doc
			},
			absent: []string{"https://lemmy.zip/post/93001", "https://lemmy.zip/comment/93002"},
		},
		{
			// The alias claims a comment already stored in C and keeps it under
			// C's post. Same community, but still an unmapped IRI speaking for a
			// stored id: the stored comment is not rewritten and the alias is
			// not mapped.
			name: "unmapped parent IRI serves a body aliasing a stored comment in C",
			prepare: func(t *testing.T, h *harness) string {
				anchorInC := postInC(t, h)
				const (
					commentInC = "https://lemmy.zip/comment/95301"
					aliasIRI   = "https://lemmy.zip/comment/95302"
				)
				comment := noteIn(commentInC, bindingAuthor, anchorInC, bindingCommunityC,
					"a comment in C", "2026-10-01T10:40:00.000000Z")
				h.serveObject("/comment/95301", comment)
				_, err := h.m.MaterializeComment(context.Background(), mustObject(t, comment), bindingCommunityC)
				require.NoError(t, err)
				h.serveObject("/comment/95302", noteIn(commentInC, bindingAuthor, anchorInC, bindingCommunityC,
					"a comment rewritten through an alias", "2026-10-01T10:40:00.000000Z"))
				return aliasIRI
			},
			absent: []string{"https://lemmy.zip/comment/95302"},
			verify: func(t *testing.T, h *harness) {
				ctx := context.Background()
				mapping, err := h.objects.GetByAPID(ctx, "https://lemmy.zip/comment/95301")
				require.NoError(t, err)
				record, recordCID, err := h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
				require.NoError(t, err)
				assert.Equal(t, "a comment in C", record["content"], "C's comment keeps its own content")
				assert.Equal(t, mapping.CID, recordCID, "C's comment record is the version its mapping names")
			},
		},
		{
			// The alias claims a comment in C that has since been deleted.
			// A deleted ancestor drops the subtree; it is never re-fetched or
			// resurrected through an alias.
			name: "unmapped parent IRI serves a body aliasing a deleted comment",
			prepare: func(t *testing.T, h *harness) string {
				anchorInC := postInC(t, h)
				const (
					deletedInC = "https://lemmy.zip/comment/95401"
					aliasIRI   = "https://lemmy.zip/comment/95402"
				)
				comment := noteIn(deletedInC, bindingAuthor, anchorInC, bindingCommunityC,
					"a comment since deleted", "2026-10-01T10:45:00.000000Z")
				_, err := h.m.MaterializeComment(context.Background(), mustObject(t, comment), bindingCommunityC)
				require.NoError(t, err)
				require.NoError(t, h.m.HandleDeleteRecord(context.Background(), deletedInC))
				h.serveObject("/comment/95402", noteIn(deletedInC, bindingAuthor, anchorInC, bindingCommunityC,
					"a deleted comment brought back", "2026-10-01T10:45:00.000000Z"))
				return aliasIRI
			},
			absent: []string{"https://lemmy.zip/comment/95402"},
			verify: func(t *testing.T, h *harness) {
				mapping, err := h.objects.GetByAPID(context.Background(), "https://lemmy.zip/comment/95401")
				require.NoError(t, err)
				assert.True(t, mapping.IsDeleted(), "the deleted comment stays deleted")
			},
		},
		{
			name: "parent is in C but C has no communities row",
			prepare: func(t *testing.T, h *harness) string {
				const postInC = "https://lemmy.zip/post/96001"
				post := page(postInC, bindingAuthor, bindingCommunityC, "a thread in C", "2026-10-01T13:00:00.000000Z")
				h.serveObject("/post/96001", post)
				_, err := h.m.MaterializePost(context.Background(), mustObject(t, post), bindingCommunityC)
				require.NoError(t, err)
				res, err := testutil.DB(t).Exec(`DELETE FROM communities WHERE ap_group_id = $1`, bindingCommunityC)
				require.NoError(t, err)
				n, err := res.RowsAffected()
				require.NoError(t, err)
				require.EqualValues(t, 1, n, "precondition: C had a communities row to remove")
				return postInC
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			serveBindingWorld(h)
			ensureCommunities(t, h, bindingCommunityC, bindingCommunityD)
			ctx := context.Background()

			parentID := tc.prepare(t, h)
			leaf := note(leafID, bindingReplier, parentID, "a reply delivered by C", "2026-10-01T14:00:00.000000Z")
			if tc.leaf != nil {
				leaf = tc.leaf(parentID)
			}
			before := captureContentState(t, h)

			res, err := h.m.MaterializeComment(ctx, mustObject(t, leaf), bindingCommunityC)
			require.Error(t, err, "C may not deliver a comment whose thread is not C's")
			assert.Nil(t, res)
			assert.True(t, IsSkip(err), "a comment outside the bound community is a skip, got %v", err)

			assert.Equal(t, before, captureContentState(t, h),
				"a refused comment writes nothing: no ancestor, no community, no author")
			assert.Equal(t, 0, countMappings(t, h, leafID))
			for _, apID := range tc.absent {
				assert.Equal(t, 0, countMappings(t, h, apID), "ancestor %s must not be materialized", apID)
			}
			assertActorAbsent(t, h, bindingReplier)
			assertActorAbsent(t, h, bindingChainAuthor)
			assertCommunityAbsent(t, h, bindingUnknownGroup)
			if tc.verify != nil {
				tc.verify(t, h)
			}
		})
	}
}

// TestDeletedCommentRedeliveredOverUnmappedChainIsRefused: a comment whose
// mapping is soft-deleted stays deleted. Re-delivered by the community it was
// bound to — now replying to an unmapped Note chain in that same community —
// it is a skip refused before the walk: none of the ancestors is fetched or
// materialized, none of their authors is bridged, and the leaf is not
// resurrected.
func TestDeletedCommentRedeliveredOverUnmappedChainIsRefused(t *testing.T) {
	const (
		anchorInC = "https://lemmy.zip/post/99001"
		leafID    = "https://lemmy.zip/comment/99002"
		rootInC   = "https://lemmy.zip/post/99003"
		middleID  = "https://lemmy.zip/comment/99004"
	)
	h := newHarness(t)
	serveBindingWorld(h)
	ensureCommunities(t, h, bindingCommunityC, bindingCommunityD)
	ctx := context.Background()

	// The leaf is first posted in C, under a post in C by its own author, and
	// then deleted.
	anchor := page(anchorInC, bindingReplier, bindingCommunityC, "a thread in C", "2026-10-01T16:00:00.000000Z")
	h.serveObject("/post/99001", anchor)
	_, err := h.m.MaterializePost(ctx, mustObject(t, anchor), bindingCommunityC)
	require.NoError(t, err)
	original := note(leafID, bindingReplier, anchorInC, "a reply delivered by C", "2026-10-01T16:01:00.000000Z")
	_, err = h.m.MaterializeComment(ctx, mustObject(t, original), bindingCommunityC)
	require.NoError(t, err)
	require.NoError(t, h.objects.SoftDelete(ctx, leafID))

	// The re-delivery hangs it under an unmapped chain in C whose authors the
	// bridge has never seen.
	h.serveObject("/post/99003", page(rootInC, bindingAuthor, bindingCommunityC, "a root in C",
		"2026-10-01T15:00:00.000000Z"))
	h.serveObject("/comment/99004", noteIn(middleID, bindingChainAuthor, rootInC, bindingCommunityC,
		"a reply in C", "2026-10-01T15:01:00.000000Z"))
	redelivered := note(leafID, bindingReplier, middleID, "a reply delivered by C again",
		"2026-10-01T16:01:00.000000Z")
	before := captureContentState(t, h)

	res, err := h.m.MaterializeComment(ctx, mustObject(t, redelivered), bindingCommunityC)
	require.Error(t, err, "a deleted comment is not re-materialized")
	assert.Nil(t, res)
	assert.True(t, IsSkip(err), "a deleted comment re-delivered is a skip, got %v", err)

	assert.Equal(t, before, captureContentState(t, h),
		"a refused comment writes nothing: no ancestor, no author, no event")
	assert.Equal(t, 0, countMappings(t, h, rootInC), "the root Page must not be materialized")
	assert.Equal(t, 0, countMappings(t, h, middleID), "the middle Note must not be materialized")
	assert.Equal(t, 0, h.hitCount("/post/99003"), "the root Page is never fetched")
	assert.Equal(t, 0, h.hitCount("/comment/99004"), "the middle Note is never fetched")
	assertActorAbsent(t, h, bindingAuthor)
	assertActorAbsent(t, h, bindingChainAuthor)

	mapping, err := h.objects.GetByAPID(ctx, leafID)
	require.NoError(t, err)
	assert.True(t, mapping.IsDeleted(), "the leaf mapping stays deleted")
}

// TestEmptyBoundCommunityFailsClosed (B6): a caller that cannot say which
// community delivered the content gets a validation error from every content
// entry point, and nothing is written. An empty binding is a programming
// error, never "unbound".
func TestEmptyBoundCommunityFailsClosed(t *testing.T) {
	const (
		postID    = "https://lemmy.zip/post/97001"
		commentID = "https://lemmy.zip/comment/97002"
	)
	post := page(postID, bindingAuthor, bindingCommunityC, "a post in C", "2026-10-01T15:00:00.000000Z")
	comment := note(commentID, bindingReplier, postID, "a comment in C", "2026-10-01T15:01:00.000000Z")

	cases := []struct {
		name string
		call func(ctx context.Context, t *testing.T, h *harness) (*Result, error)
	}{
		{name: "MaterializePost", call: func(ctx context.Context, t *testing.T, h *harness) (*Result, error) {
			return h.m.MaterializePost(ctx, mustObject(t, post), "")
		}},
		{name: "MaterializeComment", call: func(ctx context.Context, t *testing.T, h *harness) (*Result, error) {
			return h.m.MaterializeComment(ctx, mustObject(t, comment), "")
		}},
		{name: "HandleUpdate with a Page", call: func(ctx context.Context, t *testing.T, h *harness) (*Result, error) {
			return h.m.HandleUpdate(ctx, mustObject(t, post), "")
		}},
		{name: "HandleUpdate with a Note", call: func(ctx context.Context, t *testing.T, h *harness) (*Result, error) {
			return h.m.HandleUpdate(ctx, mustObject(t, comment), "")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			serveBindingWorld(h)
			h.serveObject("/post/97001", post)
			ctx := context.Background()
			before := captureContentState(t, h)

			res, err := tc.call(ctx, t, h)
			require.Error(t, err, "an empty bound community must be refused")
			assert.Nil(t, res)
			assert.True(t, errors.IsValidation(err), "an empty bound community is a validation error, got %v", err)

			assert.Equal(t, before, captureContentState(t, h), "a refused call writes nothing")
			assert.Equal(t, 0, countMappings(t, h, postID))
			assert.Equal(t, 0, countMappings(t, h, commentID))
			assertCommunityAbsent(t, h, bindingCommunityC)
			assertActorAbsent(t, h, bindingAuthor)
			assertActorAbsent(t, h, bindingReplier)
		})
	}
}

// TestCommentChainRootedInBoundCommunityMaterializes (B7, guard): the binding
// refuses foreign threads, not unfamiliar ones. A comment delivered for C
// whose whole three-level ancestry is unmapped, rooted at a Page naming C,
// still pulls in every ancestor — and bridges C itself when C has no row yet,
// because the root names the bound community.
func TestCommentChainRootedInBoundCommunityMaterializes(t *testing.T) {
	h := newHarness(t)
	leaf := serveThread(t, h)
	ctx := context.Background()

	_, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.True(t, errors.IsNotFound(err), "precondition: C has no communities row yet (err=%v)", err)

	_, err = h.m.MaterializeComment(ctx, leaf, groupID)
	require.NoError(t, err)

	communityC := testDIDFor("technology", "lemmy.world")
	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err, "the root names the bound community, so C is bridged")
	assert.Equal(t, communityC, community.DID)

	for _, apID := range []string{
		pageID,
		"https://lemmy.world/comment/1001",
		"https://sh.itjust.works/comment/2002",
		"https://lemmy.zip/comment/3003",
	} {
		mapping, err := h.objects.GetByAPID(ctx, apID)
		require.NoError(t, err, "%s must be materialized", apID)
		resolved, err := CommunityDIDOf(ctx, h.manager, mapping)
		require.NoError(t, err)
		assert.Equal(t, communityC, resolved, "%s belongs to C", apID)
	}
}

// staleMappingObjects answers GetByAPID for one id as though that id had no
// mapping yet, and delegates everything else to the real store: the reads a
// delivery makes just before a concurrent delivery of the same id commits.
//
// staleReads bounds the stale window. Zero answers every read of staleAPID
// stale, so the concurrent commit is never seen. N answers the first N reads
// stale and every later one from the real store: the concurrent commit landed
// after the delivery's own binding checks and before its commit read the
// mapping. Reads are counted, not timed, so the interleaving is deterministic.
type staleMappingObjects struct {
	store.APObjects
	staleAPID  string
	staleReads int
	reads      int
}

func (s *staleMappingObjects) GetByAPID(ctx context.Context, apID string) (*store.APObjectMapping, error) {
	if apID == s.staleAPID {
		s.reads++
		if s.staleReads == 0 || s.reads <= s.staleReads {
			return nil, errors.NewNotFoundError("ap_object", apID)
		}
	}
	return s.APObjects.GetByAPID(ctx, apID)
}

// TestFirstMaterializationRaceKeepsRecordAndMapping: two deliveries of one
// unmapped comment, bound to D and to C, can both read "no mapping". When D's
// commits first, C's must not overwrite the record under the mapping D's
// commit bound: the comment stays D's — record, reply refs and mapping alike.
// The stale read is simulated deterministically by a Materializer whose store
// reports the comment unmapped.
func TestFirstMaterializationRaceKeepsRecordAndMapping(t *testing.T) {
	h := newHarness(t)
	serveBindingWorld(h)
	ensureCommunities(t, h, bindingCommunityC, bindingCommunityD)
	ctx := context.Background()

	const (
		postInD   = "https://lemmy.zip/post/98001"
		postInC   = "https://lemmy.zip/post/98002"
		commentID = "https://lemmy.zip/comment/98003"
		published = "2026-10-01T16:01:00.000000Z"
	)
	for _, p := range []struct{ path, id, community string }{
		{"/post/98001", postInD, bindingCommunityD},
		{"/post/98002", postInC, bindingCommunityC},
	} {
		doc := page(p.id, bindingAuthor, p.community, "a thread", "2026-10-01T16:00:00.000000Z")
		h.serveObject(p.path, doc)
		_, err := h.m.MaterializePost(ctx, mustObject(t, doc), p.community)
		require.NoError(t, err)
	}
	postInDMapping, err := h.objects.GetByAPID(ctx, postInD)
	require.NoError(t, err)

	inD := noteIn(commentID, bindingReplier, postInD, bindingCommunityD, "a comment in D", published)
	first, err := h.m.MaterializeComment(ctx, mustObject(t, inD), bindingCommunityD)
	require.NoError(t, err)

	inC := noteIn(commentID, bindingReplier, postInC, bindingCommunityC, "a comment moved into C", published)
	h.serveObject("/comment/98003", inC)
	before := captureContentState(t, h)

	racing := *h.m
	racing.objects = &staleMappingObjects{APObjects: h.objects, staleAPID: commentID}
	res, err := racing.MaterializeComment(ctx, mustObject(t, inC), bindingCommunityC)
	require.Error(t, err, "the losing delivery must not commit over the winner's record")
	assert.Nil(t, res)
	assert.True(t, IsSkip(err), "losing a first-materialization race is a skip, got %v", err)

	assert.Equal(t, before, captureContentState(t, h), "the losing delivery writes nothing")
	mapping, err := h.objects.GetByAPID(ctx, commentID)
	require.NoError(t, err)
	assert.Equal(t, testDIDFor("elsewhere", "lemmy.world"), mapping.CommunityDID)
	assert.Equal(t, first.ATURI, mapping.ATURI)
	assert.Equal(t, first.CID, mapping.CID)
	record, recordCID, err := h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	require.NoError(t, err)
	assert.Equal(t, first.CID, recordCID, "the record is still the version the mapping names")
	assert.Equal(t, "a comment in D", record["content"])
	reply, _ := record["reply"].(map[string]any)
	parent, _ := reply["parent"].(map[string]any)
	assert.Equal(t, postInDMapping.ATURI, parent["uri"], "the comment still replies to D's post")
}

// TestPartiallyStaleRaceKeepsRecordAndMapping: the same race with a narrower
// window. The losing delivery, bound to C, runs every binding check while the
// content is still unmapped; the winner, bound to D, commits; only then does
// the loser's commit read the mapping. Finding it there is not licence to
// commit as an edit of D's content: the delivery was checked as a first
// materialization for C, and nothing checked it against D's binding. It is
// refused, and D's record and mapping are left exactly as the winner wrote
// them.
//
// staleReads is how many reads of the content's mapping the delivery makes
// BEFORE its commit reads it, counted on the sequential path: a post reads it
// once (MaterializePost's existing-mapping check); a comment twice
// (MaterializeComment's stored-mapping check and commitCommentLeaf's). The
// read after those is the commit's own, and it sees the winner. A fix that
// re-reads the mapping before the commit only moves that read into the
// winner's view earlier, which must refuse just the same.
func TestPartiallyStaleRaceKeepsRecordAndMapping(t *testing.T) {
	const (
		postInD   = "https://lemmy.zip/post/98201"
		postInC   = "https://lemmy.zip/post/98202"
		racedPost = "https://lemmy.zip/post/98203"
		racedNote = "https://lemmy.zip/comment/98204"
	)
	communityD := testDIDFor("elsewhere", "lemmy.world")

	cases := []struct {
		name       string
		apID       string
		staleReads int
		// win is the winner's delivery, bound to D.
		win func(ctx context.Context, t *testing.T, h *harness) *Result
		// lose is the losing delivery, bound to C, through the racing store.
		lose func(ctx context.Context, t *testing.T, h *harness, m *Materializer) (*Result, error)
		// verify checks the winner's record survived.
		verify func(t *testing.T, h *harness, record map[string]any)
	}{
		{
			name:       "postv2 post",
			apID:       racedPost,
			staleReads: 1,
			win: func(ctx context.Context, t *testing.T, h *harness) *Result {
				doc := page(racedPost, bindingAuthor, bindingCommunityD, "a post in D", "2026-10-01T16:10:00.000000Z")
				res, err := h.m.MaterializePost(ctx, mustObject(t, doc), bindingCommunityD)
				require.NoError(t, err)
				return res
			},
			lose: func(ctx context.Context, t *testing.T, h *harness, m *Materializer) (*Result, error) {
				doc := page(racedPost, bindingAuthor, bindingCommunityC, "a post moved into C",
					"2026-10-01T16:10:00.000000Z")
				h.serveObject("/post/98203", doc)
				return m.MaterializePost(ctx, mustObject(t, doc), bindingCommunityC)
			},
			verify: func(t *testing.T, h *harness, record map[string]any) {
				assert.Equal(t, "a post in D", record["title"])
				assert.Equal(t, communityD, record["community"], "the post still names D")
			},
		},
		{
			name:       "comment",
			apID:       racedNote,
			staleReads: 2,
			win: func(ctx context.Context, t *testing.T, h *harness) *Result {
				for _, p := range []struct{ path, id, community string }{
					{"/post/98201", postInD, bindingCommunityD},
					{"/post/98202", postInC, bindingCommunityC},
				} {
					doc := page(p.id, bindingAuthor, p.community, "a thread", "2026-10-01T16:00:00.000000Z")
					h.serveObject(p.path, doc)
					_, err := h.m.MaterializePost(ctx, mustObject(t, doc), p.community)
					require.NoError(t, err)
				}
				doc := noteIn(racedNote, bindingReplier, postInD, bindingCommunityD, "a comment in D",
					"2026-10-01T16:11:00.000000Z")
				res, err := h.m.MaterializeComment(ctx, mustObject(t, doc), bindingCommunityD)
				require.NoError(t, err)
				return res
			},
			lose: func(ctx context.Context, t *testing.T, h *harness, m *Materializer) (*Result, error) {
				doc := noteIn(racedNote, bindingReplier, postInC, bindingCommunityC, "a comment moved into C",
					"2026-10-01T16:11:00.000000Z")
				h.serveObject("/comment/98204", doc)
				return m.MaterializeComment(ctx, mustObject(t, doc), bindingCommunityC)
			},
			verify: func(t *testing.T, h *harness, record map[string]any) {
				assert.Equal(t, "a comment in D", record["content"])
				postInDMapping, err := h.objects.GetByAPID(context.Background(), postInD)
				require.NoError(t, err)
				reply, _ := record["reply"].(map[string]any)
				parent, _ := reply["parent"].(map[string]any)
				assert.Equal(t, postInDMapping.ATURI, parent["uri"], "the comment still replies to D's post")
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			serveBindingWorld(h)
			ensureCommunities(t, h, bindingCommunityC, bindingCommunityD)
			ctx := context.Background()

			first := tc.win(ctx, t, h)
			before := captureContentState(t, h)

			racing := *h.m
			racing.objects = &staleMappingObjects{APObjects: h.objects, staleAPID: tc.apID, staleReads: tc.staleReads}
			res, err := tc.lose(ctx, t, h, &racing)
			require.Error(t, err, "the losing delivery must not commit over the winner's record")
			assert.Nil(t, res)
			assert.True(t, IsSkip(err), "losing a first-materialization race is a skip, got %v", err)

			assert.Equal(t, before, captureContentState(t, h), "the losing delivery writes nothing")
			mapping, err := h.objects.GetByAPID(ctx, tc.apID)
			require.NoError(t, err)
			assert.Equal(t, communityD, mapping.CommunityDID)
			assert.Equal(t, first.ATURI, mapping.ATURI)
			assert.Equal(t, first.CID, mapping.CID)
			record, recordCID, err := h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
			require.NoError(t, err)
			assert.Equal(t, first.CID, recordCID, "the record is still the version the mapping names")
			tc.verify(t, h, record)
		})
	}
}

// TestDeletedLegacyCommentKeepsItsCommunityForRestore: a pre-016 comment
// mapping has no community_did, and its community is derived from the records.
// Deleting the comment deletes the very record that derivation reads, so the
// delete persists the derived community on the mapping first — otherwise a
// later restore, bound to the comment's own community, could no longer prove
// the comment is that community's and would be refused.
func TestDeletedLegacyCommentKeepsItsCommunityForRestore(t *testing.T) {
	h := newHarness(t)
	serveBindingWorld(h)
	ensureCommunities(t, h, bindingCommunityC)
	ctx := context.Background()

	const (
		postID    = "https://lemmy.zip/post/98101"
		commentID = "https://lemmy.zip/comment/98102"
	)
	post := page(postID, bindingAuthor, bindingCommunityC, "a thread in C", "2026-10-01T17:00:00.000000Z")
	h.serveObject("/post/98101", post)
	_, err := h.m.MaterializePost(ctx, mustObject(t, post), bindingCommunityC)
	require.NoError(t, err)
	comment := noteIn(commentID, bindingReplier, postID, bindingCommunityC, "a comment in C",
		"2026-10-01T17:01:00.000000Z")
	h.serveObject("/comment/98102", comment)
	_, err = h.m.MaterializeComment(ctx, mustObject(t, comment), bindingCommunityC)
	require.NoError(t, err)
	clearCommunityColumn(t, commentID)

	require.NoError(t, h.m.HandleDeleteRecord(ctx, commentID))
	deleted, err := h.objects.GetByAPID(ctx, commentID)
	require.NoError(t, err)
	require.True(t, deleted.IsDeleted(), "precondition: the delete soft-deleted the mapping")
	assert.Equal(t, testDIDFor("technology", "lemmy.world"), deleted.CommunityDID,
		"the delete persists the community it derived before the record is gone")

	// The restore path: clear the soft delete, then re-materialize the body.
	require.NoError(t, h.objects.Restore(ctx, commentID))
	_, err = h.m.HandleUpdate(ctx, mustObject(t, comment), bindingCommunityC)
	require.NoError(t, err, "a restore bound to the comment's own community re-materializes it")

	restored, err := h.objects.GetByAPID(ctx, commentID)
	require.NoError(t, err)
	assert.False(t, restored.IsDeleted())
	assert.Equal(t, testDIDFor("technology", "lemmy.world"), restored.CommunityDID)
	record, _, err := h.manager.GetRecord(ctx, restored.DID, restored.Collection, restored.RKey)
	require.NoError(t, err, "the comment's record is back")
	assert.Equal(t, "a comment in C", record["content"])
}
