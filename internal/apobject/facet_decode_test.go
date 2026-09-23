package apobject

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildNote_DecodesValidFacetRangesAndIgnoresMalformedFacets(t *testing.T) {
	const plaintext = "é bold tail"
	bold := map[string]any{"$type": "social.coves.richtext.facet#bold"}
	facet := func(start, end, features any) map[string]any {
		return map[string]any{
			"index":    map[string]any{"byteStart": start, "byteEnd": end},
			"features": features,
		}
	}

	var decodedBold map[string]any
	require.NoError(t, json.Unmarshal([]byte(`{
		"index":{"byteStart":3,"byteEnd":7},
		"features":[{"$type":"social.coves.richtext.facet#bold"}]
	}`), &decodedBold))
	withMalformedSibling := func(sibling any) []any { return []any{decodedBold, sibling} }

	tests := []struct {
		name   string
		facets []any
	}{
		{name: "JSON-decoded float64 offsets", facets: []any{decodedBold}},
		{name: "native integer offsets", facets: []any{facet(3, 7, []any{bold})}},
		{name: "non-integral offset", facets: withMalformedSibling(facet(0.5, 2, []any{bold}))},
		{name: "negative offset", facets: withMalformedSibling(facet(-1.0, 2.0, []any{bold}))},
		{name: "empty range", facets: withMalformedSibling(facet(0.0, 0.0, []any{bold}))},
		{name: "reversed range", facets: withMalformedSibling(facet(2.0, 1.0, []any{bold}))},
		{name: "huge range", facets: withMalformedSibling(facet(1e20, 1e20+1e6, []any{bold}))},
		{name: "out-of-bounds range", facets: withMalformedSibling(facet(0.0, 100.0, []any{bold}))},
		{name: "range splits UTF-8 rune", facets: withMalformedSibling(facet(1.0, 2.0, []any{bold}))},
		{name: "facet is not a map", facets: withMalformedSibling("malformed")},
		{name: "index is not a map", facets: withMalformedSibling(map[string]any{
			"index": "malformed", "features": []any{bold},
		})},
		{name: "features is not an array", facets: withMalformedSibling(facet(0.0, 2.0, "malformed"))},
		{name: "feature is not a map", facets: withMalformedSibling(facet(0.0, 2.0, []any{"malformed"}))},
		{name: "feature type is not a string", facets: withMalformedSibling(facet(0.0, 2.0, []any{
			map[string]any{"$type": 1.0},
		}))},
		{name: "malformed feature beside valid feature", facets: []any{
			facet(3.0, 7.0, []any{"malformed", bold}),
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			note := BuildNote("actor", "community", "parent", "object", map[string]any{
				"content": plaintext,
				"facets":  test.facets,
			})
			source := note["source"].(map[string]any)
			assert.Equal(t, "é **bold** tail", source["content"],
				"the valid bold facet must render and malformed facets must not alter source text")
			assert.Equal(t, "<p>é <strong>bold</strong> tail</p>\n", note["content"],
				"HTML must preserve all visible text while rendering the valid bold facet")
		})
	}
}
