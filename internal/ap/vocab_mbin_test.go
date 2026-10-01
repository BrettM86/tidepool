package ap

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Fixture provenance: page_mbin_fedia_io.json is hand-built (no live fetch)
// from the output shape of Mbin's EntryPageFactory for a link entry. Mbin
// sends `source` as the bare external URL string instead of Lemmy's
// {content, mediaType} markdown object; that string must never become the
// object's markdown source.
const (
	mbinPageFixture = "page_mbin_fedia_io.json"
	mbinPageID      = "https://fedia.io/m/startrek@startrek.website/t/4194502"
	mbinAuthorID    = "https://fedia.io/u/troed"
	mbinGroupID     = "https://startrek.website/c/startrek"
	mbinVideoURL    = "https://video.troed.se/w/5FqDa2jVqCcQWxetJEivnw"
)

// assertNoMarkdownSource accepts either a nil Source or one with no
// content: which of the two a parser produces is not the contract, only that
// the URL string is never read as markdown.
func assertNoMarkdownSource(t *testing.T, page *Object) {
	t.Helper()
	if page.Source != nil {
		assert.Empty(t, page.Source.Content,
			"Mbin's string source is a URL, never markdown source content")
	}
}

func TestParseMbinLinkPageWithStringSource(t *testing.T) {
	page, err := ParseObject(loadFixture(t, mbinPageFixture))
	require.NoError(t, err, "an Mbin Page with a string source must parse")

	assert.Equal(t, TypePage, page.Type)
	assert.Equal(t, mbinPageID, page.ID)
	assert.Equal(t, mbinAuthorID, page.AttributedTo.FirstID())
	assert.Equal(t, "Star Trek clip shared from fedia.io", page.Name)
	assert.Equal(t, "<p>Mbin link post body text</p>", page.Content)
	require.Len(t, page.Attach, 1)
	assert.Equal(t, "Link", page.Attach[0].Type)
	assert.Equal(t, mbinVideoURL, page.Attach[0].Href)
	assertNoMarkdownSource(t, page)
}

func TestParseAnnouncedMbinLinkPageWithStringSource(t *testing.T) {
	// The inbox shape: the community announces the Mbin author's Create with
	// the Page inline.
	announce, err := json.Marshal(map[string]any{
		"type":  "Announce",
		"id":    "https://startrek.website/activities/announce/create/mbin-4194502",
		"actor": mbinGroupID,
		"to":    []any{PublicAudience},
		"cc":    []any{mbinGroupID + "/followers"},
		"object": map[string]any{
			"type":     "Create",
			"id":       "https://fedia.io/f/object/7c1b2f4e-3d5a-4b8e-9f10-2a6c8d4e5b71",
			"actor":    mbinAuthorID,
			"to":       []any{mbinGroupID, PublicAudience},
			"cc":       []any{mbinAuthorID + "/followers"},
			"audience": mbinGroupID,
			"object":   json.RawMessage(loadFixture(t, mbinPageFixture)),
		},
	})
	require.NoError(t, err)

	activity, err := ParseObject(announce)
	require.NoError(t, err, "an announced Mbin Page with a string source must parse")

	require.NotNil(t, activity.Object, "Announce carries the Create")
	require.NotNil(t, activity.Object.Object, "Create carries the Page")
	page := activity.Object.Object
	assert.Equal(t, TypePage, page.Type)
	assert.Equal(t, mbinPageID, page.ID)
	assertNoMarkdownSource(t, page)
}

func TestMarshalMbinLinkPageNeverEmitsStringSource(t *testing.T) {
	page, err := ParseObject(loadFixture(t, mbinPageFixture))
	require.NoError(t, err, "an Mbin Page with a string source must parse")

	out, err := json.Marshal(page)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out, &fields))

	// Only the `source` key is inspected: the URL legitimately stays under
	// `attachment`.
	assert.NotContains(t, fields, "source",
		"a string source parses to no Source, so none may be re-emitted")
}

const sourceShapePageID = "https://lemmy.test/post/1"

// sourceShapePage builds a minimal Page with sourceJSON spliced in raw as the
// `source` value; an empty sourceJSON omits the key entirely.
func sourceShapePage(sourceJSON string) []byte {
	fields := `"type":"Page","id":"` + sourceShapePageID + `","content":"<p>body</p>"`
	if sourceJSON != "" {
		fields += `,"source":` + sourceJSON
	}
	return []byte("{" + fields + "}")
}

// TestParseObjectSourceShapes pins that `source` is optional in every shape:
// only a JSON object is read as markdown source, and any other shape leaves
// Source nil instead of failing the object (and with it, a whole outbox page).
func TestParseObjectSourceShapes(t *testing.T) {
	cases := []struct {
		name       string
		sourceJSON string
		want       *Source
	}{
		{name: "absent", sourceJSON: "", want: nil},
		{name: "null", sourceJSON: `null`, want: nil},
		{name: "empty string", sourceJSON: `""`, want: nil},
		{
			name:       "markdown object",
			sourceJSON: `{"content":"**hi**","mediaType":"text/markdown"}`,
			want:       &Source{Content: "**hi**", MediaType: "text/markdown"},
		},
		{name: "number", sourceJSON: `42`, want: nil},
		{name: "array", sourceJSON: `["a","b"]`, want: nil},
		{name: "bool", sourceJSON: `true`, want: nil},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			page, err := ParseObject(sourceShapePage(testCase.sourceJSON))
			require.NoError(t, err, "a Page with source %s must parse", testCase.sourceJSON)
			assert.Equal(t, testCase.want, page.Source)
		})
	}
}

// TestParseObjectMalformedSourceObjectErrorNamesSource pins that a source
// object with a wrong-typed field still fails, and that the error names the
// `source` field and the object rather than reading like the top-level
// `content` was wrong.
func TestParseObjectMalformedSourceObjectErrorNamesSource(t *testing.T) {
	_, err := ParseObject(sourceShapePage(`{"content":5}`))
	require.Error(t, err)
	assert.ErrorContains(t, err, "source")
	assert.ErrorContains(t, err, sourceShapePageID)
	var typeError *json.UnmarshalTypeError
	assert.ErrorAs(t, err, &typeError, "the wrap must keep the decode cause")
}
