package ingest

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
)

// TestBareContentDeliveryIsNeverMaterialized: content reaches a bridged
// community only through that community's own Announce. A bare Create/Update
// is signed by whoever sent it, and nothing about that signature says the
// community ever saw the content — yet materializing a post mints a bridged
// actor for its author and writes the community's acceptance on the
// community's behalf. So any host could inject posts and comments the real
// Lemmy community never carried, minting an identity per attempt. A bare
// content delivery is therefore a processed skip, decided before any
// outbound fetch or mint; genuine Lemmy traffic loses nothing, because Lemmy
// announces every piece of community content.
func TestBareContentDeliveryIsNeverMaterialized(t *testing.T) {
	const (
		editorID   = "https://lemmy.world/u/bareEditor"
		postID     = "https://lemmy.world/post/bare-edit-1"
		strangerID = "https://sopuli.example/u/bareStranger"
		replierID  = "https://sopuli.example/u/bareReplier"
		injectedID = "https://sopuli.example/post/bare-injected-1"
		replyID    = "https://sopuli.example/comment/bare-reply-1"
	)

	type bareCase struct {
		name string
		// personaInbox delivers to the user origin's persona inbox instead of
		// the bridge's shared inbox.
		personaInbox bool
		// senderID (username senderUsername) signs the bare delivery.
		senderID, senderUsername string
		activity                 map[string]any
		// originPaths are served documents the drop must never fetch while
		// processing (the signature check at the inbox may fetch the key).
		originPaths []string
		// served registers the origin documents the row's ids resolve to.
		served map[string]map[string]any
		check  func(t *testing.T, h *harness, group *remoteActor)
	}

	injectedPage := map[string]any{
		"type":         "Page",
		"id":           injectedID,
		"attributedTo": strangerID,
		"to":           []any{groupID, ap.PublicAudience},
		"audience":     groupID,
		"name":         "a post lemmy.world never carried",
		"source":       map[string]any{"content": "injected body", "mediaType": "text/markdown"},
		"published":    "2026-07-08T17:00:00.000000Z",
	}
	editedPage := map[string]any{
		"type":         "Page",
		"id":           postID,
		"attributedTo": editorID,
		"to":           []any{groupID, ap.PublicAudience},
		"audience":     groupID,
		"name":         "edited title",
		"source":       map[string]any{"content": "edited body", "mediaType": "text/markdown"},
		"published":    "2026-07-08T16:00:00.000000Z",
		"updated":      "2026-07-08T19:00:00.000000Z",
	}
	reply := note(replyID, replierID, postID, "a reply sent to the persona", "2026-07-08T18:00:00.000000Z")
	// A Mastodon-style reply: no audience, so it names no community.
	audiencelessReply := map[string]any{
		"type":         "Note",
		"id":           "https://sopuli.example/comment/bare-tooter-reply-1",
		"attributedTo": "https://sopuli.example/u/bareTooter",
		"to":           []any{ap.PublicAudience},
		"cc":           []any{editorID},
		"content":      "<p>a reply that names no community</p>",
		"published":    "2026-07-08T18:30:00.000000Z",
		"inReplyTo":    postID,
	}

	cases := []bareCase{
		{
			name:           "bare Create{Page} from an instance the community never announced",
			senderID:       strangerID,
			senderUsername: "bareStranger",
			activity: map[string]any{
				"id":     "https://sopuli.example/activities/create/bare-injected-1",
				"type":   "Create",
				"actor":  strangerID,
				"to":     []any{groupID, ap.PublicAudience},
				"object": injectedPage,
			},
			originPaths: []string{"/u/bareStranger", "/post/bare-injected-1"},
			served:      map[string]map[string]any{"/post/bare-injected-1": injectedPage},
			check: func(t *testing.T, h *harness, _ *remoteActor) {
				ctx := context.Background()
				_, err := h.objects.GetByAPID(ctx, injectedID)
				assert.True(t, errors.IsNotFound(err), "a bare post must not be materialized")
				_, err = h.actors.GetByAPActorID(ctx, strangerID)
				assert.True(t, errors.IsNotFound(err), "a bare post must not bridge its author")
				assert.Equal(t, 1, h.firehoseOpCount(materialize.CollectionPostV2),
					"only the announced post is in any author repo")
				assert.Equal(t, 1, h.firehoseOpCount(materialize.CollectionAcceptance),
					"only the announced post is accepted in the community repo")
			},
		},
		{
			name:           "bare Create{Note} reply at a persona inbox, then the same Note announced",
			personaInbox:   true,
			senderID:       replierID,
			senderUsername: "bareReplier",
			activity: map[string]any{
				"id":     "https://sopuli.example/activities/create/bare-reply-1",
				"type":   "Create",
				"actor":  replierID,
				"to":     []any{ap.PublicAudience},
				"cc":     []any{groupID, editorID},
				"object": reply,
			},
			originPaths: []string{"/u/bareReplier", "/comment/bare-reply-1"},
			served:      map[string]map[string]any{"/comment/bare-reply-1": reply},
			check: func(t *testing.T, h *harness, group *remoteActor) {
				ctx := context.Background()
				_, err := h.objects.GetByAPID(ctx, replyID)
				assert.True(t, errors.IsNotFound(err), "a bare reply must not be materialized")
				_, err = h.actors.GetByAPActorID(ctx, replierID)
				assert.True(t, errors.IsNotFound(err), "a bare reply must not bridge its author")
				assert.Equal(t, 0, h.firehoseOpCount(materialize.CollectionComment),
					"no comment record in any author repo")

				// The community's own fan-out of the same reply still lands: the
				// drop loses nothing Lemmy actually carries.
				h.announceCreate(group, "https://lemmy.world/activities/announce/create/bare-reply-1", reply)
				mapping, err := h.objects.GetByAPID(ctx, replyID)
				require.NoError(t, err, "the announced reply must be materialized")
				assert.Equal(t, materialize.CollectionComment, mapping.Collection)
				assert.Equal(t, testDIDFor("bareReplier", "sopuli.example"), mapping.DID)
			},
		},
		{
			// A Mastodon-style reply names no community, so nothing past the
			// bare-content drop would reject it.
			name:           "bare Create{Note} reply with no audience at a persona inbox",
			personaInbox:   true,
			senderID:       "https://sopuli.example/u/bareTooter",
			senderUsername: "bareTooter",
			activity: map[string]any{
				"id":     "https://sopuli.example/activities/create/bare-tooter-reply-1",
				"type":   "Create",
				"actor":  "https://sopuli.example/u/bareTooter",
				"to":     []any{ap.PublicAudience},
				"cc":     []any{editorID},
				"object": audiencelessReply,
			},
			originPaths: []string{"/u/bareTooter", "/comment/bare-tooter-reply-1"},
			served:      map[string]map[string]any{"/comment/bare-tooter-reply-1": audiencelessReply},
			check: func(t *testing.T, h *harness, _ *remoteActor) {
				ctx := context.Background()
				_, err := h.objects.GetByAPID(ctx, "https://sopuli.example/comment/bare-tooter-reply-1")
				assert.True(t, errors.IsNotFound(err), "a bare reply with no audience must not be materialized")
				_, err = h.actors.GetByAPActorID(ctx, "https://sopuli.example/u/bareTooter")
				assert.True(t, errors.IsNotFound(err), "a bare reply with no audience must not bridge its author")
				assert.Equal(t, 0, h.firehoseOpCount(materialize.CollectionComment),
					"no comment record in any author repo")
			},
		},
		{
			name:           "bare Update{Page} from the genuine author of an announced post",
			senderID:       editorID,
			senderUsername: "bareEditor",
			activity: map[string]any{
				"id":     "https://lemmy.world/activities/update/bare-edit-1",
				"type":   "Update",
				"actor":  editorID,
				"to":     []any{groupID, ap.PublicAudience},
				"object": editedPage,
			},
			// The origin serves the edited body, so a regression that re-fetches
			// the bare object shows up as a hit here.
			originPaths: []string{"/u/bareEditor", "/post/bare-edit-1"},
			served:      map[string]map[string]any{"/post/bare-edit-1": editedPage},
			check: func(t *testing.T, h *harness, _ *remoteActor) {
				ctx := context.Background()
				mapping, err := h.objects.GetByAPID(ctx, postID)
				require.NoError(t, err)
				record, _, err := h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
				require.NoError(t, err)
				assert.Equal(t, "original title", record["title"], "a bare edit must not be applied")
				assert.Equal(t, "original body", record["content"], "a bare edit must not be applied")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			group := h.subscribeTechnology()
			ctx := context.Background()

			// The community's own post, by a lemmy.world author who can sign:
			// the reply's parent and the edit's target.
			editor := h.newRemoteActor(editorID, person(editorID, "bareEditor", nil))
			h.announceCreate(group, "https://lemmy.world/activities/announce/create/bare-edit-1", map[string]any{
				"type":         "Page",
				"id":           postID,
				"attributedTo": editorID,
				"to":           []any{groupID, ap.PublicAudience},
				"audience":     groupID,
				"name":         "original title",
				"source":       map[string]any{"content": "original body", "mediaType": "text/markdown"},
				"published":    "2026-07-08T16:00:00.000000Z",
			})
			_, err := h.objects.GetByAPID(ctx, postID)
			require.NoError(t, err, "the announced post is the premise")

			// Every sender's actor document is servable, so a refusal is the
			// drop and not a failed fetch.
			sender := editor
			if tc.senderID != editorID {
				sender = h.newRemoteActor(tc.senderID, person(tc.senderID, tc.senderUsername, nil))
			}
			for path, doc := range tc.served {
				h.serveObject(path, doc)
			}

			activityID, ok := tc.activity["id"].(string)
			require.True(t, ok, "every row's activity carries a string id")
			deliver := h.deliver
			if tc.personaInbox {
				deliver = h.deliverToUserInbox
			}
			droppedBefore := ContentBareDropped.Value()
			require.Equal(t, http.StatusAccepted, deliver(sender, tc.activity))
			mintsBefore := h.minter.mintCount()
			hitsBefore := map[string]int{}
			for _, path := range tc.originPaths {
				hitsBefore[path] = h.hitCount(path)
			}
			h.drain()

			event, err := h.events.GetEvent(ctx, activityID)
			require.NoError(t, err)
			assert.NotNil(t, event.ProcessedAt, "the drop is a processed skip")
			assert.Nil(t, event.FailedAt, "the drop is never poisoned")
			assert.Equal(t, 1, event.Attempts, "the drop is never retried")
			assert.Equal(t, int64(1), ContentBareDropped.Value()-droppedBefore,
				"tidepool_content_bare_dropped rises once per dropped bare content delivery")
			assert.Equal(t, mintsBefore, h.minter.mintCount(), "a bare content delivery must never mint")
			for _, path := range tc.originPaths {
				assert.Equal(t, hitsBefore[path], h.hitCount(path),
					"a bare content delivery must not fetch %s", path)
			}

			tc.check(t, h, group)
		})
	}
}

// TestBareUndoDeleteNeverRematerializesContent: an Undo{Delete} of mapped
// content re-fetches and re-materializes it — a fetch, a record write, and
// possibly a mint — so it is content arriving, and content arrives only
// through its community's Announce. Lemmy sends a restore as
// Announce{Undo{Delete}}, so a bare Undo{Delete} of any mapped post or comment,
// soft-deleted or live, is a processed skip decided before any fetch: a
// deleted target stays deleted, and a live target keeps the record it has even
// when its origin now serves an edited body. The community's announced restore
// of a deleted target still brings it back.
func TestBareUndoDeleteNeverRematerializesContent(t *testing.T) {
	const (
		authorID  = "https://lemmy.world/u/restoreAuthor"
		postID    = "https://lemmy.world/post/bare-restore-1"
		commentID = "https://lemmy.world/comment/bare-restore-1"
	)
	page := map[string]any{
		"type":         "Page",
		"id":           postID,
		"attributedTo": authorID,
		"to":           []any{groupID, ap.PublicAudience},
		"audience":     groupID,
		"name":         "a post by restoreAuthor",
		"source":       map[string]any{"content": "original post body", "mediaType": "text/markdown"},
		"published":    "2026-07-08T17:00:00.000000Z",
	}
	editedPage := map[string]any{
		"type":         "Page",
		"id":           postID,
		"attributedTo": authorID,
		"to":           []any{groupID, ap.PublicAudience},
		"audience":     groupID,
		"name":         "a post by restoreAuthor",
		"source":       map[string]any{"content": "edited post body", "mediaType": "text/markdown"},
		"published":    "2026-07-08T17:00:00.000000Z",
		"updated":      "2026-07-08T19:00:00.000000Z",
	}
	comment := note(commentID, authorID, postID, "original comment body", "2026-07-08T17:30:00.000000Z")

	rows := []struct {
		name       string
		targetID   string
		targetPath string
		collection string
		// live leaves the target mapped and undeleted, with its origin serving
		// an edited body; otherwise the author deletes it through the community
		// first.
		live bool
		// wantMarkers are the target's tombstone rows before and after the drop.
		wantMarkers []string
		// wantContent is the record's content: the live record kept as it is,
		// or the deleted record once the community's announced restore lands.
		wantContent string
	}{
		{
			name:        "soft-deleted post",
			targetID:    postID,
			targetPath:  "/post/bare-restore-1",
			collection:  materialize.CollectionPostV2,
			wantMarkers: []string{groupID},
			wantContent: "original post body",
		},
		{
			name:        "soft-deleted comment",
			targetID:    commentID,
			targetPath:  "/comment/bare-restore-1",
			collection:  materialize.CollectionComment,
			wantMarkers: []string{groupID},
			wantContent: "original comment body",
		},
		{
			name:        "live post whose origin now serves an edit",
			targetID:    postID,
			targetPath:  "/post/bare-restore-1",
			collection:  materialize.CollectionPostV2,
			live:        true,
			wantMarkers: []string{},
			wantContent: "original post body",
		},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			h := newHarness(t)
			group := h.subscribeTechnology()
			ctx := context.Background()
			author := h.newRemoteActor(authorID, person(authorID, "restoreAuthor", nil))
			// The origin keeps serving both objects, so a restore that fetched
			// would succeed: a refusal below is the drop, not a failed fetch.
			h.serveObject("/post/bare-restore-1", page)
			h.serveObject("/comment/bare-restore-1", comment)
			h.announceCreate(group, "https://lemmy.world/activities/announce/create/bare-restore-post", page)
			h.announceCreate(group, "https://lemmy.world/activities/announce/create/bare-restore-comment", comment)

			if row.live {
				h.serveObject("/post/bare-restore-1", editedPage)
			} else {
				h.announceDelete(group, "https://lemmy.world/activities/announce/delete/bare-restore-1",
					authorID, row.targetID)
			}
			premise, err := h.objects.GetByAPID(ctx, row.targetID)
			require.NoError(t, err)
			require.Equal(t, !row.live, premise.IsDeleted(), "the target's deleted state is the premise")
			require.Equal(t, row.collection, premise.Collection)
			require.Equal(t, row.wantMarkers, h.tombstoneAnnouncers(row.targetID),
				"the target's tombstone rows are the premise")

			const undoID = "https://lemmy.world/activities/undo/bare-restore-1"
			deleteActivity := map[string]any{
				"id":     "https://lemmy.world/activities/delete/bare-restore-1",
				"type":   "Delete",
				"actor":  authorID,
				"object": row.targetID,
			}
			droppedBefore := ContentBareDropped.Value()
			require.Equal(t, http.StatusAccepted, h.deliver(author, map[string]any{
				"id":     undoID,
				"type":   "Undo",
				"actor":  authorID,
				"object": deleteActivity,
			}))
			mintsBefore := h.minter.mintCount()
			hitsBefore := h.hitCount(row.targetPath)
			opsBefore := len(h.firehoseOps())
			h.drain()

			event, err := h.events.GetEvent(ctx, undoID)
			require.NoError(t, err)
			assert.NotNil(t, event.ProcessedAt, "the drop is a processed skip")
			assert.Nil(t, event.FailedAt, "the drop is never poisoned")
			assert.Equal(t, 1, event.Attempts, "the drop is never retried")
			assert.Equal(t, int64(1), ContentBareDropped.Value()-droppedBefore,
				"tidepool_content_bare_dropped rises once for the dropped bare restore")
			assert.Equal(t, hitsBefore, h.hitCount(row.targetPath), "a bare restore must not fetch its target")
			assert.Equal(t, mintsBefore, h.minter.mintCount(), "a bare restore must never mint")
			assert.Equal(t, opsBefore, len(h.firehoseOps()), "a bare restore must write no record")
			assert.Equal(t, row.wantMarkers, h.tombstoneAnnouncers(row.targetID),
				"a bare restore must leave the target's tombstone rows as they were")
			mapping, err := h.objects.GetByAPID(ctx, row.targetID)
			require.NoError(t, err)
			if row.live {
				assert.False(t, mapping.IsDeleted(), "a bare restore must not delete a live mapping")
				record, _, err := h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
				require.NoError(t, err)
				assert.Equal(t, row.wantContent, record["content"],
					"a bare restore must not apply the origin's edited body")
				return
			}
			assert.True(t, mapping.IsDeleted(), "a bare restore must not revive the mapping")
			_, _, err = h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
			assert.True(t, errors.IsNotFound(err), "a bare restore must not rewrite the record (err=%v)", err)

			// The community's own restore of the same target still lands.
			require.Equal(t, http.StatusAccepted, h.deliver(group, map[string]any{
				"id":       "https://lemmy.world/activities/announce/undo/bare-restore-1",
				"type":     "Announce",
				"actor":    groupID,
				"audience": groupID,
				"object": map[string]any{
					"id":       undoID,
					"type":     "Undo",
					"actor":    authorID,
					"audience": groupID,
					"object":   deleteActivity,
				},
			}))
			h.drain()

			restored, err := h.objects.GetByAPID(ctx, row.targetID)
			require.NoError(t, err)
			assert.False(t, restored.IsDeleted(), "the announced restore revives the mapping")
			record, _, err := h.manager.GetRecord(ctx, restored.DID, restored.Collection, restored.RKey)
			require.NoError(t, err, "the announced restore rewrites the record")
			assert.Equal(t, row.wantContent, record["content"])
			assert.Equal(t, int64(1), ContentBareDropped.Value()-droppedBefore,
				"the announced restore is not a bare drop")
		})
	}
}
