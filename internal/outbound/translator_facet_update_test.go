package outbound

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/consume"
)

func TestTranslator_FacetBodiesAreRenderedOnUpdate(t *testing.T) {
	postRecord := map[string]any{
		"title":   "Edited post",
		"content": "Edited bold post",
		"facets": []any{translatorUpdateFacet(7, 11, map[string]any{
			"$type": "social.coves.richtext.facet#bold",
		})},
	}
	commentSnapshot, err := json.Marshal(map[string]any{
		"atUri":      "at://did:plc:comment/social.coves.community.comment/rk",
		"collection": "social.coves.community.comment",
		"record": map[string]any{
			"content": "Edited code reply",
			"facets": []any{translatorUpdateFacet(7, 11, map[string]any{
				"$type": "social.coves.richtext.facet#code",
			})},
		},
		"parentAtUri":   pagePostATURI,
		"parentApId":    pagePostAPID,
		"communityApId": pageCommunityAPI,
	})
	require.NoError(t, err)

	tests := []struct {
		name         string
		intent       consume.Intent
		wantMarkdown string
		wantHTML     string
	}{
		{
			name: "post Update",
			intent: consume.PostIntent{
				Op: "update", ATURI: pagePostATURI, ID: "https://coves.social/ap/activity/post-update",
				CommunityAPID: pageCommunityAPI, Snapshot: postSnapshot(t, postRecord),
			},
			wantMarkdown: "Edited **bold** post",
			wantHTML:     "<p>Edited <strong>bold</strong> post</p>\n",
		},
		{
			name: "comment Update",
			intent: consume.CommentIntent{
				Op: "update", ATURI: "at://did:plc:comment/social.coves.community.comment/rk",
				ID: "https://coves.social/ap/activity/comment-update", CommunityAPID: pageCommunityAPI,
				ParentAPID: pagePostAPID, Snapshot: commentSnapshot,
			},
			wantMarkdown: "Edited `code` reply",
			wantHTML:     "<p>Edited <code>code</code> reply</p>\n",
		},
	}

	translator := NewTranslator(pageUserOrigin)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			translated, err := translator.Translate(pageActorID, test.intent)
			require.NoError(t, err)
			assert.Equal(t, "Update", translated.Kind)

			var activity map[string]any
			require.NoError(t, json.Unmarshal(translated.Payload, &activity))
			assert.Equal(t, "Update", activity["type"])
			object := mustMap(t, activity["object"], "Update.object")
			source := mustMap(t, object["source"], "Update.object.source")
			assert.Equal(t, test.wantMarkdown, source["content"])
			assert.Equal(t, test.wantHTML, object["content"])
		})
	}
}

func translatorUpdateFacet(start, end int, feature map[string]any) map[string]any {
	return map[string]any{
		"index":    map[string]any{"byteStart": start, "byteEnd": end},
		"features": []any{feature},
	}
}
