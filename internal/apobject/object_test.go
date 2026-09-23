package apobject

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/html"
)

// Second-opinion (important): a post's external embed href must pass a scheme
// allowlist before it is rendered as a Link attachment. A javascript:/data: uri
// federated as a clickable Link is a stored-XSS-shaped hazard on every peer that
// renders it.

func buildPageWithEmbedURI(t *testing.T, uri string) map[string]any {
	t.Helper()
	page, err := BuildPage(
		"https://coves.social/ap/actor/did:plc:x",
		"https://lemmy.world/c/tech",
		"https://coves.social/ap/object/did:plc:x/social.coves.community.postv2/rk",
		map[string]any{
			"title": "a post",
			"embed": map[string]any{
				"$type":    "social.coves.embed.external",
				"external": map[string]any{"uri": uri},
			},
		})
	require.NoError(t, err)
	return page
}

func TestBuildPage_RejectsUnsafeEmbedSchemes(t *testing.T) {
	for _, uri := range []string{
		"javascript:alert(1)",
		"data:text/html,<script>alert(1)</script>",
		"vbscript:msgbox(1)",
		"file:///etc/passwd",
	} {
		page := buildPageWithEmbedURI(t, uri)
		_, has := page["attachment"]
		assert.Falsef(t, has,
			"an unsafe embed scheme (%s) must NOT be rendered as a Link attachment", uri)
	}
}

func TestBuildPage_KeepsSafeEmbedLink(t *testing.T) {
	page := buildPageWithEmbedURI(t, "https://example.com/article")
	attach, ok := page["attachment"].([]any)
	require.True(t, ok, "a safe https link is rendered as an attachment")
	require.NotEmpty(t, attach)
	link, ok := attach[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Link", link["type"])
	assert.Equal(t, "https://example.com/article", link["href"])
}

func TestBuildNote_EscapesFacetFreePlaintextForLemmy(t *testing.T) {
	const plaintext = "Inline *stars* [brackets] <script>alert(1)</script> &copy; C:\\temp\r\n" +
		"# heading\r\n" +
		"> quote\r\n" +
		"- list\r\n" +
		"``` fence\r\n" +
		"::: spoiler Warning\r\n\r\n" +
		"After blank line"
	const expectedMarkdown = "Inline \\*stars\\* \\[brackets\\] \\<script\\>alert(1)\\</script\\> \\&copy; C:\\\\temp\n" +
		"\\# heading\n" +
		"\\> quote\n" +
		"\\- list\n" +
		"\\`\\`\\` fence\n" +
		"\\::: spoiler Warning\n\n" +
		"After blank line"

	note := BuildNote("actor", "community", "parent", "object", map[string]any{"content": plaintext})
	source, ok := note["source"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, expectedMarkdown, source["content"],
		"facet-free canonical plaintext must be escaped as literal Lemmy Markdown")

	content, ok := note["content"].(string)
	require.True(t, ok)
	document, err := html.Parse(strings.NewReader(content))
	require.NoError(t, err)

	var paragraphs []*html.Node
	var unexpectedElements []string
	var walk func(*html.Node)
	walk = func(node *html.Node) {
		if node.Type == html.ElementNode {
			if node.Data == "p" {
				paragraphs = append(paragraphs, node)
			}
			switch node.Data {
			case "html", "head", "body", "p":
			default:
				unexpectedElements = append(unexpectedElements, node.Data)
			}
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(document)
	assert.Empty(t, unexpectedElements, "literal Markdown and raw HTML must not create elements")
	require.Len(t, paragraphs, 2)

	text := func(root *html.Node) string {
		var visible strings.Builder
		var collect func(*html.Node)
		collect = func(node *html.Node) {
			if node.Type == html.TextNode {
				visible.WriteString(node.Data)
			}
			for child := node.FirstChild; child != nil; child = child.NextSibling {
				collect(child)
			}
		}
		collect(root)
		return visible.String()
	}
	assert.Equal(t, "Inline *stars* [brackets] <script>alert(1)</script> &copy; C:\\temp\n"+
		"# heading\n> quote\n- list\n``` fence\n::: spoiler Warning", text(paragraphs[0]))
	assert.Equal(t, "After blank line", text(paragraphs[1]))
}

func TestBuildNote_EscapesAdditionalFacetFreeBlockMarkdownMarkers(t *testing.T) {
	const (
		plaintext        = "+ list\n\n1. numbered\n\nsetext candidate\n==="
		expectedMarkdown = "\\+ list\n\n1\\. numbered\n\nsetext candidate\n\\=\\=\\="
	)
	note := BuildNote("actor", "community", "parent", "object", map[string]any{"content": plaintext})
	source, ok := note["source"].(map[string]any)
	require.True(t, ok)
	markdown, ok := source["content"].(string)
	require.True(t, ok)
	assert.Equal(t, expectedMarkdown, markdown)

	content, ok := note["content"].(string)
	require.True(t, ok)
	for _, document := range []*html.Node{parseRenderedMarkdown(t, markdown), parseTestHTML(t, content)} {
		for _, element := range []string{"ul", "ol", "h1", "h2", "h3", "h4", "h5", "h6"} {
			assert.Empty(t, findTestElements(document, element),
				"facet-free block-like plaintext must not create <%s>", element)
		}
		paragraphs := findTestElements(document, "p")
		require.Len(t, paragraphs, 3)
		assert.Equal(t, "+ list", testHTMLText(paragraphs[0]))
		assert.Equal(t, "1. numbered", testHTMLText(paragraphs[1]))
		assert.Equal(t, "setext candidate\n===", testHTMLText(paragraphs[2]))
	}
}

func TestBuildNote_FacetFreeCommonMarkSyntaxStaysLiteral(t *testing.T) {
	type testCase struct {
		name             string
		plaintext        string
		expectedMarkdown string
	}
	tests := []testCase{
		{name: "strikethrough", plaintext: "~~strike~~", expectedMarkdown: `\~\~strike\~\~`},
		{name: "superscript", plaintext: "^super^", expectedMarkdown: `\^super\^`},
		{name: "parenthesized ordered marker", plaintext: "1) ordered", expectedMarkdown: `1\) ordered`},
		{name: "four-space indented prose", plaintext: "    indented prose", expectedMarkdown: "&#32;   indented prose"},
	}
	for offset, name := range []string{"one", "two", "three"} {
		indent := strings.Repeat(" ", offset+1)
		tests = append(tests,
			testCase{name: name + "-space indented list", plaintext: indent + "- list", expectedMarkdown: indent + `\- list`},
			testCase{name: name + "-space indented setext", plaintext: "title\n" + indent + "===", expectedMarkdown: "title\n&#32;" + indent[1:] + `\=\=\=`},
			testCase{name: name + "-space indented spoiler", plaintext: indent + "::: spoiler Warning", expectedMarkdown: indent + `\::: spoiler Warning`},
		)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, test.plaintext, nil)

			assert.Equal(t, test.expectedMarkdown, markdown)
			for _, document := range []*html.Node{parseTestHTML(t, content), parseRenderedMarkdown(t, markdown)} {
				assert.Equal(t, strings.TrimSpace(test.plaintext), strings.TrimSpace(testHTMLText(document)))
				for _, element := range []string{
					"del", "sup", "ul", "ol", "h1", "h2", "h3", "h4", "h5", "h6", "pre", "code", "details",
				} {
					assert.Empty(t, findTestElements(document, element),
						"facet-free plaintext must not create <%s>", element)
				}
			}
		})
	}
}

func TestBuildNote_FacetFreeMarkdownEscapingEdgeCases(t *testing.T) {
	tests := []struct {
		name             string
		plaintext        string
		expectedMarkdown string
		expectedHTML     string
	}{
		{name: "exclamation mark", plaintext: "wow! ![alt](x)", expectedMarkdown: `wow\! \!\[alt\](x)`, expectedHTML: "<p>wow! ![alt](x)</p>\n"},
		{name: "ordered marker alone at end of text", plaintext: "2020.", expectedMarkdown: `2020\.`, expectedHTML: "<p>2020.</p>\n"},
		{name: "parenthesized ordered marker alone at end of text", plaintext: "7)", expectedMarkdown: `7\)`, expectedHTML: "<p>7)</p>\n"},
		{name: "ordered marker alone at end of line", plaintext: "2020.\nnext", expectedMarkdown: "2020\\.\nnext", expectedHTML: "<p>2020.\nnext</p>\n"},
		{name: "ordered marker alone before CRLF", plaintext: "2020.\r\nnext", expectedMarkdown: "2020\\.\nnext", expectedHTML: "<p>2020.\nnext</p>\n"},
		{name: "leading tab", plaintext: "\tindented prose", expectedMarkdown: "&#9;indented prose", expectedHTML: "<p>indented prose</p>\n"},
		{name: "spaces then tab", plaintext: "  \tindented prose", expectedMarkdown: "&#32; \tindented prose", expectedHTML: "<p>indented prose</p>\n"},
		{name: "tab on later line", plaintext: "first\n\tsecond", expectedMarkdown: "first\n&#9;second", expectedHTML: "<p>first\n\tsecond</p>\n"},
		{name: "three spaces then tab", plaintext: "first\n   \tsecond", expectedMarkdown: "first\n&#32;  \tsecond", expectedHTML: "<p>first\n   \tsecond</p>\n"},
		{name: "setext escaping with long line cache", plaintext: "title\n  == =\nnot = setext\n===x",
			expectedMarkdown: "title\n&#32; \\=\\= \\=\nnot = setext\n===x", expectedHTML: "<p>title\n  == =\nnot = setext\n===x</p>\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, test.plaintext, nil)

			assert.Equal(t, test.expectedMarkdown, markdown)
			assert.Equal(t, test.expectedHTML, content)
			for _, element := range []string{"ol", "ul", "pre", "code", "img", "h1", "h2"} {
				assert.Empty(t, findTestElements(parseRenderedMarkdown(t, markdown), element),
					"facet-free plaintext must not create <%s>", element)
			}
		})
	}
}

// Lemmy's markdown-it treats a lone carriage return as a line break, so every
// line-start escape and quote prefix must apply after a lone \r too. goldmark
// does not, so these assert the literal Markdown instead of parsing it.
func TestBuildNote_LoneCarriageReturnIsALineBreak(t *testing.T) {
	tests := []struct {
		name             string
		plaintext        string
		facets           []any
		expectedMarkdown string
		expectedHTML     string
	}{
		{name: "setext underline", plaintext: "a\r===",
			expectedMarkdown: "a\n\\=\\=\\=", expectedHTML: "<p>a\n===</p>\n"},
		{name: "ordered list marker", plaintext: "a\r1. item",
			expectedMarkdown: "a\n1\\. item", expectedHTML: "<p>a\n1. item</p>\n"},
		{name: "spoiler fence after blank line", plaintext: "a\r\r::: spoiler x",
			expectedMarkdown: "a\n\n\\::: spoiler x", expectedHTML: "<p>a</p>\n<p>::: spoiler x</p>\n"},
		{name: "spoiler closing fence inside spoiler", plaintext: "hidden\r:::\rrevealed",
			facets:           []any{testFacet(0, len("hidden\r:::\rrevealed"), testSpoilerFeature(""))},
			expectedMarkdown: "::: spoiler Spoiler\nhidden\n\\:::\nrevealed\n:::",
			expectedHTML:     "<details>\n<summary>Spoiler</summary>\n<p>hidden\n:::\nrevealed</p>\n</details>\n"},
		{name: "code block inside quote", plaintext: "before\nfirst\r```\rsecond\nafter",
			facets: []any{
				testFacet(1, len("before\nfirst\r```\rsecond\naft"), testBlockquoteFeature()),
				testFacet(len("before\n"), len("before\nfirst\r```\rsecond"), testCodeBlockFeature("")),
			},
			expectedMarkdown: "> before\n>\n> ````\n> first\n> ```\n> second\n> ````\n>\n> after",
			expectedHTML: "<blockquote>\n<p>before</p>\n<pre><code>first\n```\nsecond</code></pre>\n" +
				"<p>after</p>\n</blockquote>\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, test.plaintext, test.facets)

			assert.Equal(t, test.expectedMarkdown, markdown)
			assert.Equal(t, test.expectedHTML, content)
		})
	}
}

func TestBuildNote_LongSetextCandidateLineRendersInLinearTime(t *testing.T) {
	const equalsCount = 500_000
	plaintext := "title\n" + strings.Repeat("=", equalsCount)
	expectedMarkdown := "title\n" + strings.Repeat(`\=`, equalsCount)

	rendered := make(chan string, 1)
	go func() {
		markdown, _ := buildTestNote(t, plaintext, nil)
		rendered <- markdown
	}()
	select {
	case markdown := <-rendered:
		assert.Equal(t, expectedMarkdown, markdown)
	case <-time.After(5 * time.Second):
		t.Fatal("rendering a 500KB setext candidate line must not take quadratic time")
	}
}
