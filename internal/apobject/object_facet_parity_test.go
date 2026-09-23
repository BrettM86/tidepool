package apobject

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
)

func TestBuildPage_UsesNoteFacetBodySemanticsAndPreservesPageFields(t *testing.T) {
	const (
		actorID      = "https://coves.example/ap/actor/did:plc:author"
		communityID  = "https://lemmy.example/c/testing"
		objectURL    = "https://coves.example/ap/object/did:plc:author/social.coves.community.postv2/rk"
		published    = "2026-09-13T10:00:00.000Z"
		plaintext    = "Read bold and docs."
		wantMarkdown = "Read **bold** and [docs](https://example.com/guide)."
		wantHTML     = "<p>Read <strong>bold</strong> and <a href=\"https://example.com/guide\">docs</a>.</p>\n"
	)
	record := map[string]any{
		"title":     "Facet parity",
		"content":   plaintext,
		"createdAt": published,
		"facets": []any{
			testFacet(5, 9, map[string]any{"$type": boldFacetType}),
			testFacet(14, 18, map[string]any{"$type": covesLinkFacetType, "uri": "https://example.com/guide"}),
		},
		"labels": map[string]any{
			"values": []any{map[string]any{"val": "nsfw"}},
		},
	}

	page, err := BuildPage(actorID, communityID, objectURL, record)
	require.NoError(t, err)
	note := BuildNote(actorID, communityID, "parent", objectURL, record)

	assert.Equal(t, map[string]any{"content": wantMarkdown, "mediaType": "text/markdown"}, page["source"])
	assert.Equal(t, wantHTML, page["content"])
	assert.Equal(t, note["source"], page["source"], "Page and Note must use identical facet-derived Markdown")
	assert.Equal(t, note["content"], page["content"], "Page and Note must use identical structural HTML")

	assert.Equal(t, "Page", page["type"])
	assert.Equal(t, objectURL, page["id"])
	assert.Equal(t, actorID, page["attributedTo"])
	assert.Equal(t, []string{communityID, ap.PublicAudience}, page["to"])
	assert.Equal(t, []string{}, page["cc"])
	assert.Equal(t, "Facet parity", page["name"])
	assert.Equal(t, communityID, page["audience"])
	assert.Equal(t, "text/html", page["mediaType"])
	assert.Equal(t, published, page["published"])
	assert.Equal(t, true, page["sensitive"])
}

func TestRenderObject_JSONSnapshotsMatchDirectFacetBuilders(t *testing.T) {
	const origin = "https://coves.example"
	tests := []struct {
		name              string
		snapshot          []byte
		wantSourceContent string
		wantHTML          string
		direct            func(t *testing.T) map[string]any
	}{
		{
			name: "postv2",
			snapshot: []byte(`{
				"atUri":"at://did:plc:post/social.coves.community.postv2/rk",
				"collection":"social.coves.community.postv2",
				"communityApId":"https://lemmy.example/c/testing",
				"record":{
					"title":"Snapshot post",
					"content":"Read bold now",
					"facets":[{"index":{"byteStart":5,"byteEnd":9},"features":[{"$type":"social.coves.richtext.facet#bold"}]}]
				}
			}`),
			wantSourceContent: "Read **bold** now",
			wantHTML:          "<p>Read <strong>bold</strong> now</p>\n",
			direct: func(t *testing.T) map[string]any {
				page, err := BuildPage(
					origin+"/ap/actor/did:plc:post",
					"https://lemmy.example/c/testing",
					origin+"/ap/object/did:plc:post/social.coves.community.postv2/rk",
					map[string]any{
						"title": "Snapshot post", "content": "Read bold now",
						"facets": []any{testFacet(5, 9, map[string]any{"$type": boldFacetType})},
					})
				require.NoError(t, err)
				return page
			},
		},
		{
			name: "comment",
			snapshot: []byte(`{
				"atUri":"at://did:plc:comment/social.coves.community.comment/rk",
				"collection":"social.coves.community.comment",
				"communityApId":"https://lemmy.example/c/testing",
				"parentApId":"https://lemmy.example/post/1",
				"record":{
					"content":"Make bold reply",
					"facets":[{"index":{"byteStart":5,"byteEnd":9},"features":[{"$type":"social.coves.richtext.facet#bold"}]}]
				}
			}`),
			wantSourceContent: "Make **bold** reply",
			wantHTML:          "<p>Make <strong>bold</strong> reply</p>\n",
			direct: func(t *testing.T) map[string]any {
				return BuildNote(
					origin+"/ap/actor/did:plc:comment",
					"https://lemmy.example/c/testing",
					"https://lemmy.example/post/1",
					origin+"/ap/object/did:plc:comment/social.coves.community.comment/rk",
					map[string]any{
						"content": "Make bold reply",
						"facets":  []any{testFacet(5, 9, map[string]any{"$type": boldFacetType})},
					})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parsed, err := ParseSnapshot(test.snapshot)
			require.NoError(t, err)
			record := parsed["record"].(map[string]any)
			facet := record["facets"].([]any)[0].(map[string]any)
			index := facet["index"].(map[string]any)
			assert.IsType(t, float64(0), index["byteStart"], "JSON snapshot offsets decode as float64")

			served, err := RenderObject(origin, test.snapshot)
			require.NoError(t, err)
			source, ok := served["source"].(map[string]any)
			require.True(t, ok, "RenderObject source must be an object")
			assert.Equal(t, test.wantSourceContent, source["content"])
			assert.Equal(t, "text/markdown", source["mediaType"])
			assert.Equal(t, test.wantHTML, served["content"])

			direct := test.direct(t)
			assert.Equal(t, direct["source"], served["source"])
			assert.Equal(t, direct["content"], served["content"])
		})
	}
}
