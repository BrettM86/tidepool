package outbound

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/consume"
)

// Task 15 cycle B (Page half): the Translator renders a native POST as
// Create/Update{Page} or a bare Delete. The Page/Note split is the crux —
// verified against internal/ap/testdata/announce_create_page_lemmy_world.json:
// a Page carries the community in `to` (alongside as:Public), where a Note
// carries it in `cc`. name is REQUIRED (Lemmy rejects a titleless Page), a link
// embed becomes attachment [{type:Link,href}], nsfw becomes sensitive, audience
// is the community, and attributedTo is a SINGLE STRING.
//
// The producer of a PostIntent is task 16's acceptance engine; task 15 owns
// this translation, so it is pinned here at the translator boundary.

const (
	pageUserOrigin   = "https://coves.social"
	pageCommunityAPI = "https://lemmy.world/c/technology"
	pageActorID      = "https://coves.social/ap/actor/did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	pageAuthorDID    = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
	pagePostRKey     = "3lzpost22222aa"
	pagePostCID      = "bafyreib2rxk3rybk3aobmv5cjuql3bm2twh4jo5uxgf5kpqrsqxi3jgxte"
	pagePostATURI    = "at://" + pageAuthorDID + "/social.coves.community.postv2/" + pagePostRKey
	pagePostAPID     = pageUserOrigin + "/ap/object/" + pageAuthorDID + "/social.coves.community.postv2/" + pagePostRKey
	pageLinkHref     = "https://www.tomshardware.com/pc-components/dram/samsung-sk-hynix-and-micron-face-a-third-dram-price-fixing-lawsuit"
	pageTitle        = "Inside the history of DRAM price-fixing lawsuits"
	pageBody         = "two decades of failed cases, and why HBM changes the math"
)

// postSnapshot builds the durable snapshot the acceptance engine stores for a
// post (the same envelope commentSnapshot uses): the postv2 record plus context.
func postSnapshot(t *testing.T, record map[string]any) []byte {
	t.Helper()
	snap, err := json.Marshal(map[string]any{
		"atUri":         pagePostATURI,
		"cid":           pagePostCID,
		"rev":           "3lzpostrev0001",
		"collection":    "social.coves.community.postv2",
		"record":        record,
		"communityApId": pageCommunityAPI,
	})
	require.NoError(t, err)
	return snap
}

func linkPostRecord() map[string]any {
	return map[string]any{
		"$type":     "social.coves.community.postv2",
		"community": "did:plc:44ybard66vv44zksje25o7dz",
		"title":     pageTitle,
		"content":   pageBody,
		"embed": map[string]any{
			"$type":    "social.coves.embed.external",
			"external": map[string]any{"uri": pageLinkHref, "title": "Tom's Hardware"},
		},
		"createdAt": "2026-07-07T03:27:37.028Z",
	}
}

func TestTranslator_Page(t *testing.T) {
	tr := NewTranslator(pageUserOrigin)

	cases := []struct {
		name     string
		intent   consume.PostIntent
		wantKind string
		validate func(t *testing.T, activity map[string]any)
	}{
		{
			name: "create link post -> Create{Page}",
			intent: consume.PostIntent{
				Op:            "create",
				ATURI:         pagePostATURI,
				ID:            consume.ActivityID(pageUserOrigin, pagePostATURI, "create", 0),
				CommunityAPID: pageCommunityAPI,
				Snapshot:      postSnapshot(t, linkPostRecord()),
			},
			wantKind: "Create",
			validate: func(t *testing.T, activity map[string]any) {
				page := mustMap(t, activity["object"], "object")
				assert.Equal(t, "Page", page["type"], "a post renders as a Page, not a Note")
				assert.Equal(t, pagePostAPID, page["id"], "the Page id is the served object URL")

				to := mustStringSet(t, page["to"], "Page.to")
				assert.Contains(t, to, pageCommunityAPI,
					"the Page/Note split: a Page carries the community in `to` (a Note carries it in cc)")
				assert.Contains(t, to, ap.PublicAudience, "to must also include as:Public")

				cc := mustStringSet(t, page["cc"], "Page.cc")
				assert.NotContains(t, cc, pageCommunityAPI,
					"the community must NOT be in a Page's cc — that is the Note shape")

				assert.Equal(t, pageTitle, page["name"],
					"name is REQUIRED and comes from the record's title")
				assert.Equal(t, pageCommunityAPI, page["audience"], "audience = the community AP id")

				attributedTo, isString := page["attributedTo"].(string)
				assert.True(t, isString, "attributedTo MUST be a single string, not an array")
				assert.Equal(t, pageActorID, attributedTo)

				assert.NotEmpty(t, page["content"], "the HTML content is carried")
				source := mustMap(t, page["source"], "Page.source")
				assert.Equal(t, pageBody, source["content"], "the markdown source is carried verbatim")
				assert.Equal(t, "text/markdown", source["mediaType"])

				attach := mustSlice(t, page["attachment"], "Page.attachment")
				require.NotEmpty(t, attach, "a link post carries a Link attachment")
				link := mustMap(t, attach[0], "attachment[0]")
				assert.Equal(t, "Link", link["type"],
					"a link embed maps to attachment [{type:Link,href}] (Lemmy reads the FIRST attachment)")
				assert.Equal(t, pageLinkHref, link["href"])

				assert.Equal(t, false, page["sensitive"], "a non-nsfw post is sensitive:false")
			},
		},
		{
			name: "create nsfw text post -> sensitive",
			intent: consume.PostIntent{
				Op:            "create",
				ATURI:         pagePostATURI,
				ID:            consume.ActivityID(pageUserOrigin, pagePostATURI, "create", 0),
				CommunityAPID: pageCommunityAPI,
				Snapshot: postSnapshot(t, map[string]any{
					"$type":     "social.coves.community.postv2",
					"community": "did:plc:44ybard66vv44zksje25o7dz",
					"title":     "a spicy text post",
					"content":   "body text",
					"labels": map[string]any{
						"$type":  "com.atproto.label.defs#selfLabels",
						"values": []any{map[string]any{"val": "nsfw"}},
					},
					"createdAt": "2026-07-07T03:27:37.028Z",
				}),
			},
			wantKind: "Create",
			validate: func(t *testing.T, activity map[string]any) {
				page := mustMap(t, activity["object"], "object")
				assert.Equal(t, "a spicy text post", page["name"])
				assert.Equal(t, true, page["sensitive"],
					"a post self-labelled nsfw renders sensitive:true")
			},
		},
		{
			name: "update -> Update{Page}",
			intent: consume.PostIntent{
				Op:            "update",
				ATURI:         pagePostATURI,
				ID:            consume.ActivityID(pageUserOrigin, pagePostATURI, "update", 1),
				CommunityAPID: pageCommunityAPI,
				Snapshot:      postSnapshot(t, linkPostRecord()),
			},
			wantKind: "Update",
			validate: func(t *testing.T, activity map[string]any) {
				page := mustMap(t, activity["object"], "object")
				assert.Equal(t, "Page", page["type"], "an edit re-federates the same Page shape")
				to := mustStringSet(t, page["to"], "Page.to")
				assert.Contains(t, to, pageCommunityAPI, "the Page/Note split holds on Update too")
			},
		},
		{
			name: "delete -> Delete with NO summary",
			intent: consume.PostIntent{
				Op:            "delete",
				ATURI:         pagePostATURI,
				ID:            consume.ActivityID(pageUserOrigin, pagePostATURI, "delete", 1),
				CommunityAPID: pageCommunityAPI,
				Snapshot:      postSnapshot(t, linkPostRecord()),
			},
			wantKind: "Delete",
			validate: func(t *testing.T, activity map[string]any) {
				assert.Equal(t, pagePostAPID, activity["object"],
					"a self-delete's object is the bare object URL")
				_, hasSummary := activity["summary"]
				assert.False(t, hasSummary,
					"a self-delete carries NO summary: Lemmy reads a summary as a mod-removal reason")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, err := tr.Translate(pageActorID, tc.intent)
			require.NoError(t, err, "the Page translator must render %s", tc.name)
			require.NotNil(t, out)
			assert.Equal(t, tc.wantKind, out.Kind)
			assert.Equal(t, tc.intent.ID, out.ActivityID)

			var activity map[string]any
			require.NoError(t, json.Unmarshal(out.Payload, &activity),
				"the payload must be valid activity JSON")
			assert.Equal(t, tc.wantKind, activity["type"], "the outer activity type")
			assert.Equal(t, pageActorID, activity["actor"], "the activity actor is the persona")
			tc.validate(t, activity)
		})
	}
}

// ---- local JSON helpers (asMap/asStringSet live in the outer test file) ----

func mustMap(t *testing.T, v any, what string) map[string]any {
	t.Helper()
	m, ok := v.(map[string]any)
	require.True(t, ok, "%s must be a JSON object, got %T", what, v)
	return m
}

func mustSlice(t *testing.T, v any, what string) []any {
	t.Helper()
	s, ok := v.([]any)
	require.True(t, ok, "%s must be a JSON array, got %T", what, v)
	return s
}

func mustStringSet(t *testing.T, v any, what string) []string {
	t.Helper()
	switch typed := v.(type) {
	case nil:
		return nil
	case string:
		return []string{typed}
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			s, ok := item.(string)
			require.True(t, ok, "%s array must hold strings, got %T", what, item)
			out = append(out, s)
		}
		return out
	default:
		require.Failf(t, "bad shape", "%s must be a string or array, got %T", what, v)
		return nil
	}
}
