package materialize

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/store"
	"tidepool/internal/testutil"
)

// TestLemmyReplyToNativeCommentBridges: a Lemmy reply whose inReplyTo is a
// NATIVE Coves comment — one Tidepool federated out, whose record lives in the
// author's own PDS and not in Tidepool's repo store — commits into the Lemmy
// author's repo with reply.parent on the native comment and reply.root on the
// thread's root post. The root comes from the bridge's own outbound snapshot
// (rootAtUri), never from the reply.root the native record asserts about
// itself. A deeper Lemmy reply under the bridged one inherits the same root.
func TestLemmyReplyToNativeCommentBridges(t *testing.T) {
	const (
		userOrigin = "https://coves.social"

		nativeCommenterDID = "did:plc:nativecommenteraaaaaaaaa"
		nativeCommentRKey  = "3m2bbbbbbbb22"
		nativeCommentCID   = "bafyreic5fxhlpwo3i5bura7mnfzgw2pabtebn7r2okojgqqcp2qqngdbwq"
		nativeCommentATURI = "at://did:plc:nativecommenteraaaaaaaaa/social.coves.community.comment/3m2bbbbbbbb22"
		nativeCommentAPID  = userOrigin + "/ap/object/did:plc:nativecommenteraaaaaaaaa/social.coves.community.comment/3m2bbbbbbbb22"

		// What the native record claims as its root: no such post exists.
		bogusRootATURI = "at://did:plc:bogusrootaaaaaaaaaaaaaaa/social.coves.community.postv2/3m2zzzzzzzz22"
		bogusRootCID   = "bafyreievgu2ty7qbiaaom5zhmkznsnajuzideek3lo7e65dwqlrvrxnmo4"

		replyID  = "https://lemmy.zip/comment/7001"
		deeperID = "https://sh.itjust.works/comment/7002"
	)

	rows := []struct {
		name      string
		root      store.APObjectMapping
		rootATURI string
		rootCID   string
	}{
		{
			name: "native postv2 root",
			root: store.APObjectMapping{
				APID:           userOrigin + "/ap/object/did:plc:nativeposteraaaaaaaaaaaa/social.coves.community.postv2/3m2aaaaaaaa22",
				APType:         "Page",
				OriginInstance: "coves.social",
				Origin:         store.OriginBridge,
				DID:            "did:plc:nativeposteraaaaaaaaaaaa",
				AuthorDID:      "did:plc:nativeposteraaaaaaaaaaaa",
				Collection:     CollectionPostV2,
				RKey:           "3m2aaaaaaaa22",
				CID:            "bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqrsqxi3jgxte",
			},
			rootATURI: "at://did:plc:nativeposteraaaaaaaaaaaa/social.coves.community.postv2/3m2aaaaaaaa22",
			rootCID:   "bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqrsqxi3jgxte",
		},
		{
			name: "lemmy page root",
			root: store.APObjectMapping{
				APID:           "https://lemmy.world/post/7000",
				APType:         "Page",
				OriginInstance: "lemmy.world",
				Origin:         store.OriginFediverse,
				DID:            "did:plc:lemmyposteraaaaaaaaaaaaa",
				AuthorDID:      "did:plc:lemmyposteraaaaaaaaaaaaa",
				Collection:     CollectionPostV2,
				RKey:           "3m2cccccccc22",
				CID:            "bafyreid4wq42vsigqkklplahll4htzlxlsngqoj5s75bvuqyyl5ghweusy",
			},
			rootATURI: "at://did:plc:lemmyposteraaaaaaaaaaaaa/social.coves.community.postv2/3m2cccccccc22",
			rootCID:   "bafyreid4wq42vsigqkklplahll4htzlxlsngqoj5s75bvuqyyl5ghweusy",
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			f := newNativeParentFixture(t)
			h := f.h
			ctx := context.Background()

			// The thread root post, mapped in ap_objects.
			f.putMapping(t, row.root)

			// The native comment as Tidepool federated it: a bridge-origin
			// mapping (its record is in the author's PDS, not here) plus the
			// outbound row whose snapshot recorded the thread root.
			f.putMapping(t, store.APObjectMapping{
				APID:           nativeCommentAPID,
				APType:         "Note",
				OriginInstance: "coves.social",
				Origin:         store.OriginBridge,
				DID:            nativeCommenterDID,
				AuthorDID:      nativeCommenterDID,
				Collection:     CollectionComment,
				RKey:           nativeCommentRKey,
				CID:            nativeCommentCID,
			})
			f.putOutbound(t, nativeCommentATURI, nativeCommentAPID, nativeCommentCID, map[string]any{
				"atUri":      nativeCommentATURI,
				"cid":        nativeCommentCID,
				"collection": CollectionComment,
				"record": map[string]any{
					"$type":   CollectionComment,
					"content": "native comment",
					"reply": map[string]any{
						"root":   map[string]any{"uri": bogusRootATURI, "cid": bogusRootCID},
						"parent": map[string]any{"uri": row.rootATURI, "cid": row.rootCID},
					},
					"createdAt": "2026-11-14T10:00:00.000Z",
				},
				"parentAtUri":   row.rootATURI,
				"parentApId":    row.root.APID,
				"rootAtUri":     row.rootATURI,
				"communityApId": groupID,
			})

			replyRefs := func(mapping *store.APObjectMapping) (parent, root map[string]any) {
				t.Helper()
				record, _, err := h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
				require.NoError(t, err)
				parent, ok := extractStrongRef(record, "reply", "parent")
				require.True(t, ok, "committed comment carries reply.parent")
				root, ok = extractStrongRef(record, "reply", "root")
				require.True(t, ok, "committed comment carries reply.root")
				return parent, root
			}

			// Step 1: the Lemmy reply to the native comment.
			reply := objectFromMap(t, note(replyID, "https://lemmy.zip/u/carol",
				nativeCommentAPID, "reply to a native comment", "2026-11-14T11:00:00.000000Z"))
			_, err := h.m.MaterializeComment(ctx, reply, groupID)
			require.NoError(t, err, "a Lemmy reply to a native comment must bridge")

			replyMapping, err := h.objects.GetByAPID(ctx, replyID)
			require.NoError(t, err)
			assert.Equal(t, testDIDFor("carol", "lemmy.zip"), replyMapping.DID,
				"the reply lives in the Lemmy author's repo")
			assert.Equal(t, CollectionComment, replyMapping.Collection)
			parent, root := replyRefs(replyMapping)
			assert.Equal(t, map[string]any{"uri": nativeCommentATURI, "cid": nativeCommentCID}, parent)
			assert.Equal(t, map[string]any{"uri": row.rootATURI, "cid": row.rootCID}, root,
				"the root comes from the bridge's snapshot, not the native record's own reply.root")

			// Step 2: a deeper Lemmy reply under the bridged one.
			deeper := objectFromMap(t, note(deeperID, "https://sh.itjust.works/u/bob",
				replyID, "deeper reply", "2026-11-14T12:00:00.000000Z"))
			_, err = h.m.MaterializeComment(ctx, deeper, groupID)
			require.NoError(t, err)

			deeperMapping, err := h.objects.GetByAPID(ctx, deeperID)
			require.NoError(t, err)
			assert.Equal(t, testDIDFor("bob", "sh.itjust.works"), deeperMapping.DID)
			parent, root = replyRefs(deeperMapping)
			assert.Equal(t, map[string]any{"uri": replyMapping.ATURI, "cid": replyMapping.CID}, parent)
			assert.Equal(t, map[string]any{"uri": row.rootATURI, "cid": row.rootCID}, root)
		})
	}
}

// nativeParentFixture is the shared setup for a Lemmy reply to a native Coves
// comment: a harness with outbound state wired, the lemmy.world community, and
// the two Lemmy authors the replies come from.
type nativeParentFixture struct {
	h         *harness
	outbound  store.OutboundObjects
	community *store.Community
}

func newNativeParentFixture(t *testing.T) *nativeParentFixture {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database, "outbound_objects")
	outbound := store.NewOutboundObjects(database)
	h := newHarness(t, func(o *Options) { o.OutboundObjects = outbound })
	h.serveFixture("/c/technology", "group_lemmy_world.json")
	h.serveObject("/u/carol", person("https://lemmy.zip/u/carol", "carol", nil))
	h.serveObject("/u/bob", person("https://sh.itjust.works/u/bob", "bob", nil))
	community, err := h.m.EnsureCommunity(context.Background(), &ap.Object{ID: groupID})
	require.NoError(t, err)
	require.Equal(t, testDIDFor("technology", "lemmy.world"), community.DID)
	return &nativeParentFixture{h: h, outbound: outbound, community: community}
}

// putMapping stores an ap_objects mapping bound to the fixture's community.
func (f *nativeParentFixture) putMapping(t *testing.T, mapping store.APObjectMapping) {
	t.Helper()
	mapping.CommunityDID = f.community.DID
	_, err := f.h.objects.PutMapping(context.Background(), mapping)
	require.NoError(t, err)
}

// putOutbound stores an outbound_objects row with the given snapshot.
func (f *nativeParentFixture) putOutbound(t *testing.T, atURI, apID, cid string, snapshot map[string]any) {
	t.Helper()
	encoded, err := json.Marshal(snapshot)
	require.NoError(t, err)
	_, err = f.outbound.Upsert(context.Background(), store.OutboundObject{
		ATURI:              atURI,
		APObjectID:         apID,
		LastCID:            cid,
		LastRev:            "3m2bbbbbbbb22",
		CommunityDID:       f.community.DID,
		CommunityAPID:      groupID,
		TranslatedSnapshot: encoded,
		Depth:              1,
	})
	require.NoError(t, err)
}

// TestLemmyReplyToNativeCommentRootResolution covers how the thread root of a
// Lemmy reply to a native comment is resolved from the bridge's outbound
// snapshot, and when the reply is refused instead. A refusal is either a skip
// (the thread can never be bridged as stated) or a retryable error (bridge
// state is missing that should be there); neither commits a mapping.
func TestLemmyReplyToNativeCommentRootResolution(t *testing.T) {
	const (
		userOrigin = "https://coves.social"

		nativeCommenterDID = "did:plc:nativecommenteraaaaaaaaa"
		nativeCommentRKey  = "3m2bbbbbbbb22"
		nativeCommentCID   = "bafyreic5fxhlpwo3i5bura7mnfzgw2pabtebn7r2okojgqqcp2qqngdbwq"
		nativeCommentATURI = "at://did:plc:nativecommenteraaaaaaaaa/social.coves.community.comment/3m2bbbbbbbb22"
		nativeCommentAPID  = userOrigin + "/ap/object/did:plc:nativecommenteraaaaaaaaa/social.coves.community.comment/3m2bbbbbbbb22"

		nativePostDID   = "did:plc:nativeposteraaaaaaaaaaaa"
		nativePostRKey  = "3m2aaaaaaaa22"
		nativePostCID   = "bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqrsqxi3jgxte"
		nativePostATURI = "at://did:plc:nativeposteraaaaaaaaaaaa/social.coves.community.postv2/3m2aaaaaaaa22"
		nativePostAPID  = userOrigin + "/ap/object/did:plc:nativeposteraaaaaaaaaaaa/social.coves.community.postv2/3m2aaaaaaaa22"

		// A native comment between the root post and the replied-to comment.
		middleCommenterDID = "did:plc:middlecommenteraaaaaaaaa"
		middleCommentRKey  = "3m2dddddddd22"
		middleCommentCID   = "bafyreievgu2ty7qbiaaom5zhmkznsnajuzideek3lo7e65dwqlrvrxnmo4"
		middleCommentATURI = "at://did:plc:middlecommenteraaaaaaaaa/social.coves.community.comment/3m2dddddddd22"
		middleCommentAPID  = userOrigin + "/ap/object/did:plc:middlecommenteraaaaaaaaa/social.coves.community.comment/3m2dddddddd22"

		// A fediverse-origin Lemmy comment mapped in ap_objects.
		lemmyCommenterDID = "did:plc:lemmycommenteraaaaaaaaaa"
		lemmyCommentRKey  = "3m2eeeeeeee22"
		lemmyCommentCID   = "bafyreid4wq42vsigqkklplahll4htzlxlsngqoj5s75bvuqyyl5ghweusy"
		lemmyCommentATURI = "at://did:plc:lemmycommenteraaaaaaaaaa/social.coves.community.comment/3m2eeeeeeee22"
		lemmyCommentAPID  = "https://lemmy.world/comment/7099"

		replyID = "https://lemmy.zip/comment/7001"
	)

	nativePost := store.APObjectMapping{
		APID:           nativePostAPID,
		APType:         "Page",
		OriginInstance: "coves.social",
		Origin:         store.OriginBridge,
		DID:            nativePostDID,
		AuthorDID:      nativePostDID,
		Collection:     CollectionPostV2,
		RKey:           nativePostRKey,
		CID:            nativePostCID,
	}
	nativeComment := store.APObjectMapping{
		APID:           nativeCommentAPID,
		APType:         "Note",
		OriginInstance: "coves.social",
		Origin:         store.OriginBridge,
		DID:            nativeCommenterDID,
		AuthorDID:      nativeCommenterDID,
		Collection:     CollectionComment,
		RKey:           nativeCommentRKey,
		CID:            nativeCommentCID,
	}
	middleComment := store.APObjectMapping{
		APID:           middleCommentAPID,
		APType:         "Note",
		OriginInstance: "coves.social",
		Origin:         store.OriginBridge,
		DID:            middleCommenterDID,
		AuthorDID:      middleCommenterDID,
		Collection:     CollectionComment,
		RKey:           middleCommentRKey,
		CID:            middleCommentCID,
	}
	lemmyComment := store.APObjectMapping{
		APID:           lemmyCommentAPID,
		APType:         "Note",
		OriginInstance: "lemmy.world",
		Origin:         store.OriginFediverse,
		DID:            lemmyCommenterDID,
		AuthorDID:      lemmyCommenterDID,
		Collection:     CollectionComment,
		RKey:           lemmyCommentRKey,
		CID:            lemmyCommentCID,
	}

	// commentSnapshot is the outbound snapshot of the native comment, with
	// parentAtUri/rootAtUri as given; rootAtUri is left out when absent.
	commentSnapshot := func(parentATURI, parentAPID string, rootATURI *string) map[string]any {
		snapshot := map[string]any{
			"atUri":      nativeCommentATURI,
			"cid":        nativeCommentCID,
			"collection": CollectionComment,
			"record": map[string]any{
				"$type":   CollectionComment,
				"content": "native comment",
				"reply": map[string]any{
					"root":   map[string]any{"uri": nativePostATURI, "cid": nativePostCID},
					"parent": map[string]any{"uri": parentATURI, "cid": nativePostCID},
				},
				"createdAt": "2026-11-14T10:00:00.000Z",
			},
			"parentAtUri":   parentATURI,
			"parentApId":    parentAPID,
			"communityApId": groupID,
		}
		if rootATURI != nil {
			snapshot["rootAtUri"] = *rootATURI
		}
		return snapshot
	}
	ptr := func(s string) *string { return &s }

	type outcome int
	const (
		wantCommit outcome = iota
		wantSkip
		wantRetryableError
	)

	rows := []struct {
		name     string
		setup    func(t *testing.T, f *nativeParentFixture)
		want     outcome
		wantRoot map[string]any
	}{
		{
			name: "depth-2 native chain roots on the post, not the intermediate comment",
			setup: func(t *testing.T, f *nativeParentFixture) {
				f.putMapping(t, nativePost)
				f.putMapping(t, middleComment)
				f.putMapping(t, nativeComment)
				f.putOutbound(t, nativeCommentATURI, nativeCommentAPID, nativeCommentCID,
					commentSnapshot(middleCommentATURI, middleCommentAPID, ptr(nativePostATURI)))
			},
			want:     wantCommit,
			wantRoot: map[string]any{"uri": nativePostATURI, "cid": nativePostCID},
		},
		{
			name: "native parent comment's outbound row is tombstoned",
			setup: func(t *testing.T, f *nativeParentFixture) {
				f.putMapping(t, nativePost)
				f.putMapping(t, nativeComment)
				f.putOutbound(t, nativeCommentATURI, nativeCommentAPID, nativeCommentCID,
					commentSnapshot(nativePostATURI, nativePostAPID, ptr(nativePostATURI)))
				_, err := f.outbound.Tombstone(context.Background(), nativeCommentATURI)
				require.NoError(t, err)
			},
			want: wantSkip,
		},
		{
			name: "snapshot has no rootAtUri key",
			setup: func(t *testing.T, f *nativeParentFixture) {
				f.putMapping(t, nativePost)
				f.putMapping(t, nativeComment)
				f.putOutbound(t, nativeCommentATURI, nativeCommentAPID, nativeCommentCID,
					commentSnapshot(nativePostATURI, nativePostAPID, nil))
			},
			want: wantSkip,
		},
		{
			name: "snapshot rootAtUri is empty",
			setup: func(t *testing.T, f *nativeParentFixture) {
				f.putMapping(t, nativePost)
				f.putMapping(t, nativeComment)
				f.putOutbound(t, nativeCommentATURI, nativeCommentAPID, nativeCommentCID,
					commentSnapshot(nativePostATURI, nativePostAPID, ptr("")))
			},
			want: wantSkip,
		},
		{
			name: "rootAtUri names a comment mapping, not a post",
			setup: func(t *testing.T, f *nativeParentFixture) {
				f.putMapping(t, lemmyComment)
				f.putMapping(t, nativeComment)
				f.putOutbound(t, nativeCommentATURI, nativeCommentAPID, nativeCommentCID,
					commentSnapshot(lemmyCommentATURI, lemmyCommentAPID, ptr(lemmyCommentATURI)))
			},
			want: wantSkip,
		},
		{
			name: "native postv2 root's outbound row is tombstoned",
			setup: func(t *testing.T, f *nativeParentFixture) {
				f.putMapping(t, nativePost)
				f.putMapping(t, nativeComment)
				f.putOutbound(t, nativePostATURI, nativePostAPID, nativePostCID, map[string]any{
					"atUri":         nativePostATURI,
					"cid":           nativePostCID,
					"collection":    CollectionPostV2,
					"record":        map[string]any{"$type": CollectionPostV2, "title": "native post"},
					"communityApId": groupID,
				})
				_, err := f.outbound.Tombstone(context.Background(), nativePostATURI)
				require.NoError(t, err)
				f.putOutbound(t, nativeCommentATURI, nativeCommentAPID, nativeCommentCID,
					commentSnapshot(nativePostATURI, nativePostAPID, ptr(nativePostATURI)))
			},
			want: wantSkip,
		},
		{
			name: "native parent comment has no outbound row",
			setup: func(t *testing.T, f *nativeParentFixture) {
				f.putMapping(t, nativePost)
				f.putMapping(t, nativeComment)
			},
			want: wantRetryableError,
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			f := newNativeParentFixture(t)
			ctx := context.Background()
			row.setup(t, f)

			reply := objectFromMap(t, note(replyID, "https://lemmy.zip/u/carol",
				nativeCommentAPID, "reply to a native comment", "2026-11-14T11:00:00.000000Z"))
			_, err := f.h.m.MaterializeComment(ctx, reply, groupID)

			switch row.want {
			case wantCommit:
				require.NoError(t, err)
				replyMapping, err := f.h.objects.GetByAPID(ctx, replyID)
				require.NoError(t, err)
				record, _, err := f.h.manager.GetRecord(ctx, replyMapping.DID, replyMapping.Collection, replyMapping.RKey)
				require.NoError(t, err)
				parent, ok := extractStrongRef(record, "reply", "parent")
				require.True(t, ok, "committed comment carries reply.parent")
				root, ok := extractStrongRef(record, "reply", "root")
				require.True(t, ok, "committed comment carries reply.root")
				assert.Equal(t, map[string]any{"uri": nativeCommentATURI, "cid": nativeCommentCID}, parent)
				assert.Equal(t, row.wantRoot, root)
				return
			case wantSkip:
				require.Error(t, err)
				assert.True(t, stderrors.Is(err, ErrSkipped), "want a skip, got %v", err)
			case wantRetryableError:
				require.Error(t, err)
				assert.False(t, stderrors.Is(err, ErrSkipped), "want a retryable error, not a skip: %v", err)
			}
			_, err = f.h.objects.GetByAPID(ctx, replyID)
			assert.True(t, errors.IsNotFound(err), "a refused reply commits no mapping, got %v", err)
		})
	}
}
