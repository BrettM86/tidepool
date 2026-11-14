package ingest

import (
	"context"
	stderrors "errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// TestBackfillDeferEvent (D1): DeferEvent keeps an Announce of content as a
// backfill_deferred_items row in the community that signed it: the content
// IRI, the community's group id and DID, the dead host the cause names (else
// the content IRI's host), the object's type, first due 1h after the
// deferral. Votes, deletes, undos and an Announce from a community the bridge
// does not have are not deferrable and write nothing.
func TestBackfillDeferEvent(t *testing.T) {
	const (
		otherGroupID = "https://lemmy.world/c/unknown"
		createIRI    = "https://x.example/activities/create/1"
		authorIRI    = "https://x.example/u/gone"
	)
	refused := func(host string) error {
		return errors.HostGoneError{Host: host, Reason: errors.HostGoneReasonConnectionRefused}
	}
	xGone := refused("x.example")
	badGateway := stderrors.New("ingest: fetch https://z.example/post/9: unexpected status 502")
	firstRetry := time.Date(2026, 11, 8, 13, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		actorID string
		payload map[string]any
		cause   error
		// wantRow, when set, is the one row DeferEvent writes; ObjectIRI empty
		// means deferred=false and no row.
		wantRow store.DeferredItem
	}{
		{
			name:    "Announce(Create typed Note)",
			actorID: groupID,
			payload: announceOf(map[string]any{
				"id": createIRI, "type": "Create", "actor": authorIRI,
				"object": map[string]any{
					"id": "https://x.example/comment/1", "type": "Note", "attributedTo": authorIRI,
					"inReplyTo": "https://lemmy.world/post/1", "content": "<p>hi</p>",
				},
			}),
			cause:   xGone,
			wantRow: store.DeferredItem{ObjectIRI: "https://x.example/comment/1", Host: "x.example", ObjectType: "Note"},
		},
		{
			name:    "Announce(Create with a bare content IRI)",
			actorID: groupID,
			payload: announceOf(map[string]any{
				"id": createIRI, "type": "Create", "actor": authorIRI,
				"object": "https://x.example/post/2",
			}),
			cause:   xGone,
			wantRow: store.DeferredItem{ObjectIRI: "https://x.example/post/2", Host: "x.example", ObjectType: ""},
		},
		{
			name:    "Announce(Update typed Page)",
			actorID: groupID,
			payload: announceOf(map[string]any{
				"id": "https://x.example/activities/update/3", "type": "Update", "actor": authorIRI,
				"object": map[string]any{
					"id": "https://x.example/post/3", "type": "Page", "attributedTo": authorIRI,
					"name": "an edited post", "audience": groupID,
				},
			}),
			cause:   xGone,
			wantRow: store.DeferredItem{ObjectIRI: "https://x.example/post/3", Host: "x.example", ObjectType: "Page"},
		},
		{
			name:    "Announce(bare IRI)",
			actorID: groupID,
			payload: announceOf("https://x.example/post/4"),
			cause:   xGone,
			wantRow: store.DeferredItem{ObjectIRI: "https://x.example/post/4", Host: "x.example", ObjectType: ""},
		},
		{
			name:    "an unreachable ancestor names the ancestor's host",
			actorID: groupID,
			payload: announceOf(map[string]any{
				"id": "https://lemmy.world/activities/create/5", "type": "Create", "actor": "https://lemmy.world/u/replier",
				"object": map[string]any{
					"id": "https://lemmy.world/comment/5", "type": "Note", "attributedTo": "https://lemmy.world/u/replier",
					"inReplyTo": "https://x.example/post/4", "content": "<p>reply</p>",
				},
			}),
			cause:   refused("x.example"),
			wantRow: store.DeferredItem{ObjectIRI: "https://lemmy.world/comment/5", Host: "x.example", ObjectType: "Note"},
		},
		{
			name:    "a failure that is not host-gone names the content IRI's host",
			actorID: groupID,
			payload: announceOf(map[string]any{
				"id": "https://z.example/activities/create/9", "type": "Create", "actor": "https://z.example/u/a",
				"object": map[string]any{
					"id": "https://z.example/post/9", "type": "Page", "attributedTo": "https://z.example/u/a",
					"name": "a post", "audience": groupID,
				},
			}),
			cause:   badGateway,
			wantRow: store.DeferredItem{ObjectIRI: "https://z.example/post/9", Host: "z.example", ObjectType: "Page"},
		},
		{
			name:    "Announce(Like)",
			actorID: groupID,
			payload: announceOf(map[string]any{
				"id": "https://x.example/activities/like/1", "type": "Like", "actor": authorIRI,
				"object": "https://x.example/post/4",
			}),
			cause: xGone,
		},
		{
			name:    "Announce(Delete)",
			actorID: groupID,
			payload: announceOf(map[string]any{
				"id": "https://x.example/activities/delete/1", "type": "Delete", "actor": authorIRI,
				"object": "https://x.example/post/4",
			}),
			cause: xGone,
		},
		{
			name:    "Undo(Like)",
			actorID: groupID,
			payload: map[string]any{
				"id": "https://lemmy.world/activities/undo/1", "type": "Undo", "actor": groupID,
				"object": map[string]any{
					"id": "https://x.example/activities/like/1", "type": "Like", "actor": authorIRI,
					"object": "https://x.example/post/4",
				},
			},
			cause: xGone,
		},
		{
			name:    "a community the bridge does not have",
			actorID: otherGroupID,
			payload: map[string]any{
				"id": "https://lemmy.world/activities/announce/unknown", "type": "Announce", "actor": otherGroupID,
				"object": map[string]any{
					"id": createIRI, "type": "Create", "actor": authorIRI,
					"object": map[string]any{
						"id": "https://x.example/comment/1", "type": "Note", "attributedTo": authorIRI,
						"inReplyTo": "https://lemmy.world/post/1", "content": "<p>hi</p>",
					},
				},
			},
			cause: xGone,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.subscribeTechnology()
			b := newBackfill(t, h, 100)
			b.now = func() time.Time { return backfillNow }

			event := &store.InboxEvent{
				ActivityID:  tt.payload["id"].(string),
				Type:        tt.payload["type"].(string),
				Payload:     []byte(mustJSON(t, tt.payload)),
				ActorID:     tt.actorID,
				OrderingKey: tt.actorID,
			}
			deferred, err := b.DeferEvent(context.Background(), event, tt.cause)
			require.NoError(t, err)

			if tt.wantRow.ObjectIRI == "" {
				assert.False(t, deferred, "the event is not deferrable")
				assert.Empty(t, deferredRows(t, h), "nothing is deferred")
				return
			}
			want := tt.wantRow
			want.CommunityAPID = groupID
			want.CommunityDID = h.communityDID(groupID)
			want.Attempts = 0
			want.CreatedAt = backfillNow
			want.NextRetryAt = firstRetry
			assert.True(t, deferred, "the event is deferred")
			assert.Equal(t, []store.DeferredItem{want}, deferredRows(t, h))
		})
	}
}

// announceOf is the technology community's Announce of object.
func announceOf(object any) map[string]any {
	return map[string]any{
		"@context": "https://www.w3.org/ns/activitystreams",
		"id":       "https://lemmy.world/activities/announce/1",
		"type":     "Announce",
		"actor":    groupID,
		"to":       []any{"https://www.w3.org/ns/activitystreams#Public"},
		"object":   object,
	}
}

// TestBackfillObjectNotServedNamesTheEventsContent (F2): ObjectNotServed is
// true only for an event DeferEvent would accept whose non-JSON response is
// the event's own content IRI, on a host whose nodeinfo answers JSON. A
// Delete or Undo verification fetch, or an Announce whose author fetch
// answered HTML, is not a skip.
func TestBackfillObjectNotServedNamesTheEventsContent(t *testing.T) {
	const (
		pageIRI   = "https://x.example/post/5"
		authorIRI = "https://x.example/u/gone"
	)
	htmlAt := func(url string) error {
		return fmt.Errorf("ingest: fetch %s: %w", url, ap.NonJSONResponseError{URL: url, ContentType: "text/html"})
	}
	createPage := announceOf(map[string]any{
		"id": "https://x.example/activities/create/5", "type": "Create", "actor": authorIRI,
		"object": map[string]any{
			"id": pageIRI, "type": "Page", "attributedTo": authorIRI,
			"name": "a post", "audience": groupID,
		},
	})
	tests := []struct {
		name    string
		payload map[string]any
		cause   error
		want    bool
	}{
		{
			name: "Announce(Delete) whose target answers HTML",
			payload: announceOf(map[string]any{
				"id": "https://x.example/activities/delete/1", "type": "Delete", "actor": authorIRI,
				"object": "https://x.example/post/4",
			}),
			cause: htmlAt("https://x.example/post/4"),
			want:  false,
		},
		{
			name: "Undo(Like) whose target answers HTML",
			payload: map[string]any{
				"id": "https://lemmy.world/activities/undo/1", "type": "Undo", "actor": groupID,
				"object": map[string]any{
					"id": "https://x.example/activities/like/1", "type": "Like", "actor": authorIRI,
					"object": "https://x.example/post/4",
				},
			},
			cause: htmlAt("https://x.example/post/4"),
			want:  false,
		},
		{
			name:    "Announce(Create Page) whose content answers HTML",
			payload: createPage,
			cause:   htmlAt(pageIRI),
			want:    true,
		},
		{
			name:    "Announce(Create Page) whose author answers HTML",
			payload: createPage,
			cause:   htmlAt(authorIRI),
			want:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.subscribeTechnology()
			rig := newUnreachableRig(t, h, nil, map[string]probeAnswer{unreachableHost: probeJSON})

			event := &store.InboxEvent{
				ActivityID:  tt.payload["id"].(string),
				Type:        tt.payload["type"].(string),
				Payload:     []byte(mustJSON(t, tt.payload)),
				ActorID:     groupID,
				OrderingKey: groupID,
			}
			assert.Equal(t, tt.want, rig.backfill.ObjectNotServed(context.Background(), event, tt.cause))
		})
	}
}

// TestBackfillDeferEventRefreshesAnOldRow (F6): an inbox deferral of an
// object already deferred in the same community 80 days ago, with retries
// spent, refreshes the row: created_at is the new deferral's, attempts 0,
// and the next retry the earlier of the old one and 1h after now.
func TestBackfillDeferEventRefreshesAnOldRow(t *testing.T) {
	const (
		commentIRI = "https://x.example/comment/1"
		authorIRI  = "https://x.example/u/gone"
	)
	ctx := context.Background()
	h := newHarness(t)
	h.subscribeTechnology()
	b := newBackfill(t, h, 100)
	b.now = func() time.Time { return backfillNow }
	old := store.DeferredItem{
		ObjectIRI: commentIRI, CommunityAPID: groupID, CommunityDID: h.communityDID(groupID),
		Host: "x.example", ObjectType: "Note", Attempts: 7,
		CreatedAt:   time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC),
		NextRetryAt: time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC),
	}
	require.NoError(t, store.NewDeferredBackfillItems(h.db).DeferItem(ctx, old))

	payload := announceOf(map[string]any{
		"id": "https://x.example/activities/create/1", "type": "Create", "actor": authorIRI,
		"object": map[string]any{
			"id": commentIRI, "type": "Note", "attributedTo": authorIRI,
			"inReplyTo": "https://lemmy.world/post/1", "content": "<p>hi</p>",
		},
	})
	event := &store.InboxEvent{
		ActivityID: payload["id"].(string), Type: "Announce",
		Payload: []byte(mustJSON(t, payload)), ActorID: groupID, OrderingKey: groupID,
	}
	deferred, err := b.DeferEvent(ctx, event,
		errors.HostGoneError{Host: "x.example", Reason: errors.HostGoneReasonConnectionRefused})
	require.NoError(t, err)
	assert.True(t, deferred)

	want := old
	want.Attempts = 0
	want.CreatedAt = backfillNow
	want.NextRetryAt = time.Date(2026, 11, 8, 13, 0, 0, 0, time.UTC)
	assert.Equal(t, []store.DeferredItem{want}, deferredRows(t, h),
		"the inbox deferral refreshes the old row's created_at, attempts and next retry")
}
