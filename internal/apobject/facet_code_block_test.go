package apobject

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/html"
)

const testCodeBlockFacetType = "social.coves.richtext.facet#codeBlock"

func TestBuildNote_CodeBlockExpandsPartialMultilineRangeAndPreservesLiteralSource(t *testing.T) {
	const (
		plaintext = "before\nfmt.Println(\"``` * <tag> &copy; C:\\\\tmp\")\nsecond_line # [x]\nafter"
		codeText  = "fmt.Println(\"``` * <tag> &copy; C:\\\\tmp\")\nsecond_line # [x]"
	)
	start := strings.Index(plaintext, "Println")
	end := strings.Index(plaintext, " # [x]")
	markdown, content := buildTestNote(t, plaintext, []any{
		testFacet(start, end, testCodeBlockFeature("go_1+x-")),
	})

	const expectedMarkdown = "before\n\n````go_1+x-\n" + codeText + "\n````\n\nafter"
	assert.Equal(t, expectedMarkdown, markdown)
	assert.Equal(t, "<p>before</p>\n<pre><code class=\"language-go_1+x-\">"+
		"fmt.Println(&#34;``` * &lt;tag&gt; &amp;copy; C:\\\\tmp&#34;)\nsecond_line # [x]"+
		"</code></pre>\n<p>after</p>\n", content)
	requireCodeBlock(t, parseTestHTML(t, content), codeText, "language-go_1+x-")
	requireCodeBlock(t, parseRenderedMarkdown(t, markdown), codeText+"\n", "language-go_1+x-")
}

func TestBuildNote_CodeBlockUsesMinimumFenceAndValidatesLanguage(t *testing.T) {
	const plaintext = "a*b <tag> &copy; C:\\tmp"
	tests := []struct {
		name     string
		feature  map[string]any
		info     string
		language string
	}{
		{name: "language absent", feature: testCodeBlockFeature(""), info: ""},
		{name: "safe forty byte language", feature: testCodeBlockFeature(strings.Repeat("a", 37) + "_+-"), info: strings.Repeat("a", 37) + "_+-", language: "language-" + strings.Repeat("a", 37) + "_+-"},
		{name: "over forty bytes", feature: testCodeBlockFeature(strings.Repeat("a", 41)), info: ""},
		{name: "space", feature: testCodeBlockFeature("go lang"), info: ""},
		{name: "non ASCII", feature: testCodeBlockFeature("gö"), info: ""},
		{name: "HTML", feature: testCodeBlockFeature("<script>"), info: ""},
		{name: "newline", feature: testCodeBlockFeature("go\n```\nattack"), info: ""},
		{name: "wrong type", feature: map[string]any{"$type": testCodeBlockFacetType, "language": 7}, info: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, plaintext, []any{
				testFacet(0, len(plaintext), test.feature),
			})

			expectedMarkdown := "```" + test.info + "\n" + plaintext + "\n```"
			assert.Equal(t, expectedMarkdown, markdown,
				"a code block without source backticks uses the minimum three-character fence")
			code := requireCodeBlock(t, parseTestHTML(t, content), plaintext, test.language)
			assert.Empty(t, findTestElements(code, "script"), "a language hint cannot inject structural HTML")
			requireCodeBlock(t, parseRenderedMarkdown(t, markdown), plaintext+"\n", test.language)
		})
	}
}

func TestBuildNote_CodeBlockExpandedToEmptyLineIsIgnored(t *testing.T) {
	const (
		plaintext    = "before\n\nafter"
		expectedHTML = "<p>before</p>\n<p>after</p>\n"
	)
	emptyLineStart := len("before\n")
	markdown, content := buildTestNote(t, plaintext, []any{
		testFacet(emptyLineStart, emptyLineStart+1, testCodeBlockFeature("go")),
	})

	assert.Equal(t, plaintext, markdown)
	assert.Equal(t, expectedHTML, content)
	for _, document := range []*html.Node{parseTestHTML(t, content), parseRenderedMarkdown(t, markdown)} {
		assert.Empty(t, findTestElements(document, "pre"))
		assert.Empty(t, findTestElements(document, "code"))
		paragraphs := findTestElements(document, "p")
		require.Len(t, paragraphs, 2)
		assert.Equal(t, "before", testHTMLText(paragraphs[0]))
		assert.Equal(t, "after", testHTMLText(paragraphs[1]))
	}
}

func TestBuildNote_CodeBlockWinsOverOverlappingStylingExceptEnclosingBlockquote(t *testing.T) {
	const (
		plaintext        = "raw *value* <tag> &copy; C:\\tmp"
		expectedMarkdown = "> ```go\n> raw *value* <tag> &copy; C:\\tmp\n> ```"
	)
	code := testCodeBlockFeature("go")
	quote := testBlockquoteFeature(1)
	bold := map[string]any{"$type": emphasisFacetTypePrefix + "bold"}
	italic := map[string]any{"$type": emphasisFacetTypePrefix + "italic"}
	strike := map[string]any{"$type": emphasisFacetTypePrefix + "strikethrough"}
	link := linkFeature("https://example.com/code")
	inlineCode := codeFeature()
	tests := []struct {
		name   string
		facets []any
	}{
		{name: "source order", facets: []any{
			testFacet(0, len(plaintext), quote),
			testFacet(0, len(plaintext), code),
			testFacet(4, len(plaintext)-4, bold, italic, strike, link, inlineCode),
		}},
		{name: "reverse order and duplicates", facets: []any{
			testFacet(4, len(plaintext)-4, inlineCode, link, strike, italic, bold),
			testFacet(0, len(plaintext), code, code),
			testFacet(0, len(plaintext), quote, quote),
			testFacet(0, len(plaintext), code),
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, plaintext, test.facets)

			assert.Equal(t, expectedMarkdown, markdown)
			document := parseTestHTML(t, content)
			quotes := findTestElements(document, "blockquote")
			require.Len(t, quotes, 1)
			codeElement := requireCodeBlock(t, quotes[0], plaintext, "language-go")
			require.Same(t, quotes[0], codeElement.Parent.Parent,
				"the enclosing blockquote may contain the code block")
			for _, element := range []string{"strong", "em", "del", "a"} {
				assert.Empty(t, findTestElements(document, element), "codeBlock must suppress overlapping <%s>", element)
			}
			rendered := parseRenderedMarkdown(t, markdown)
			renderedQuotes := findTestElements(rendered, "blockquote")
			require.Len(t, renderedQuotes, 1)
			requireCodeBlock(t, renderedQuotes[0], plaintext+"\n", "language-go")
		})
	}
}

func TestBuildNote_CodeBlockWinsOverSameLineHeading(t *testing.T) {
	const (
		plaintext        = "raw *value*"
		expectedMarkdown = "```\nraw *value*\n```"
		expectedHTML     = "<pre><code>raw *value*</code></pre>\n"
	)
	code := testFacet(0, len(plaintext), testCodeBlockFeature(""))
	heading := testFacet(len("raw "), len(plaintext), testHeadingFeature(2))
	for _, facets := range [][]any{{code, heading}, {heading, code}} {
		markdown, content := buildTestNote(t, plaintext, facets)

		assert.Equal(t, expectedMarkdown, markdown)
		assert.Equal(t, expectedHTML, content)
	}
}

func TestBuildNote_CodeBlockNestedInsideWiderBlockquotePreservesEnclosingQuote(t *testing.T) {
	const (
		plaintext        = "quoted > before\nraw *code* <tag>\nquoted # after"
		codeText         = "raw *code* <tag>"
		expectedMarkdown = "> quoted \\> before\n>\n> ```go\n> raw *code* <tag>\n> ```\n>\n> quoted \\# after"
		expectedHTML     = "<blockquote>\n<p>quoted &gt; before</p>\n" +
			"<pre><code class=\"language-go\">raw *code* &lt;tag&gt;</code></pre>\n" +
			"<p>quoted # after</p>\n</blockquote>\n"
	)
	expectedQuote := requireSingleQuoteParagraphs(t, parseRenderedMarkdown(t, expectedMarkdown), "quoted > before", "quoted # after")
	expectedCode := requireCodeBlock(t, expectedQuote, codeText+"\n", "language-go")
	require.Same(t, expectedQuote, expectedCode.Parent.Parent)

	codeStart := strings.Index(plaintext, codeText)
	quote := testFacet(1, len(plaintext)-1, testBlockquoteFeature(1))
	code := testFacet(codeStart+4, codeStart+len(codeText)-2, testCodeBlockFeature("go"))
	tests := []struct {
		name   string
		facets []any
	}{
		{name: "quote first", facets: []any{quote, code}},
		{name: "code first", facets: []any{code, quote}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, plaintext, test.facets)

			assert.Equal(t, expectedMarkdown, markdown)
			assert.Equal(t, expectedHTML, content)
			t.Run("direct HTML structure", func(t *testing.T) {
				directQuote := requireSingleQuoteParagraphs(t, parseTestHTML(t, content), "quoted > before", "quoted # after")
				directCode := requireCodeBlock(t, directQuote, codeText, "language-go")
				require.Same(t, directQuote, directCode.Parent.Parent,
					"the wider blockquote must directly contain the nested code block")
			})
			t.Run("source Markdown structure", func(t *testing.T) {
				renderedQuote := requireSingleQuoteParagraphs(t, parseRenderedMarkdown(t, markdown), "quoted > before", "quoted # after")
				renderedCode := requireCodeBlock(t, renderedQuote, codeText+"\n", "language-go")
				require.Same(t, renderedQuote, renderedCode.Parent.Parent,
					"the source Markdown must preserve the same quote/code containment")
			})
		})
	}
}

func TestBuildNote_TwoCodeBlocksInsideOneQuoteRenderInSourceOrderDeterministically(t *testing.T) {
	const (
		plaintext        = "intro\nfirst code\nmiddle\nsecond code\noutro"
		expectedMarkdown = "> intro\n>\n> ```go\n> first code\n> ```\n>\n> middle\n>\n" +
			"> ```rust\n> second code\n> ```\n>\n> outro"
		expectedHTML = "<blockquote>\n<p>intro</p>\n<pre><code class=\"language-go\">first code</code></pre>\n" +
			"<p>middle</p>\n<pre><code class=\"language-rust\">second code</code></pre>\n<p>outro</p>\n</blockquote>\n"
	)
	firstStart := strings.Index(plaintext, "first code")
	secondStart := strings.Index(plaintext, "second code")
	facets := []any{
		testFacet(0, len(plaintext), testBlockquoteFeature(1)),
		testFacet(firstStart, firstStart+len("first code"), testCodeBlockFeature("go")),
		testFacet(secondStart, secondStart+len("second code"), testCodeBlockFeature("rust")),
	}

	markdownResults := make(map[string]int)
	contentResults := make(map[string]int)
	for range 200 {
		markdown, content := buildTestNote(t, plaintext, facets)
		markdownResults[markdown]++
		contentResults[content]++
	}

	assert.Equal(t, map[string]int{expectedMarkdown: 200}, markdownResults)
	assert.Equal(t, map[string]int{expectedHTML: 200}, contentResults)
}

func TestBuildNote_CodeBlockTakesQuoteLevelOnlyFromSurvivingQuote(t *testing.T) {
	const (
		plaintext        = "code\nmore"
		expectedMarkdown = "```\ncode\n```\n\nmore"
		expectedHTML     = "<pre><code>code</code></pre>\n<p>more</p>\n"
	)
	facets := []any{
		testFacet(0, len("code"), testCodeBlockFeature("")),
		testFacet(0, len("code"), testBlockquoteFeature(1)),
		testFacet(0, len(plaintext), testBlockquoteFeature(2)),
	}

	markdown, content := buildTestNote(t, plaintext, facets)

	assert.Equal(t, expectedMarkdown, markdown,
		"crossing quotes are dropped, so neither may wrap the equal-bounds code block")
	assert.Equal(t, expectedHTML, content)
	assert.Empty(t, findTestElements(parseRenderedMarkdown(t, markdown), "blockquote"))
}

func TestBuildNote_InvalidCodeBlockRuneBoundaryRemainsVisiblePlaintext(t *testing.T) {
	const (
		malformedText    = "é *literal* <tag> &copy; C:\\tmp"
		validText        = "valid *code*"
		plaintext        = malformedText + "\n" + validText
		expectedMarkdown = "é \\*literal\\* \\<tag\\> \\&copy; C:\\\\tmp\n\n```go\nvalid *code*\n```"
		expectedHTML     = "<p>é *literal* &lt;tag&gt; &amp;copy; C:\\tmp</p>\n" +
			"<pre><code class=\"language-go\">valid *code*</code></pre>\n"
	)
	validStart := len(malformedText) + 1
	markdown, content := buildTestNote(t, plaintext, []any{
		testFacet(1, len(malformedText), testCodeBlockFeature("go")),
		testFacet(validStart, len(plaintext), testCodeBlockFeature("go")),
	})

	assert.Equal(t, expectedMarkdown, markdown)
	assert.Equal(t, expectedHTML, content)
	document := parseTestHTML(t, content)
	requireCodeBlock(t, document, validText, "language-go")
	paragraphs := findTestElements(document, "p")
	require.Len(t, paragraphs, 1)
	assert.Equal(t, malformedText, testHTMLText(paragraphs[0]),
		"a range splitting an original UTF-8 rune must degrade without losing visible text")
}

func testCodeBlockFeature(language string) map[string]any {
	feature := map[string]any{"$type": testCodeBlockFacetType}
	if language != "" {
		feature["language"] = language
	}
	return feature
}

func requireCodeBlock(t *testing.T, document *html.Node, expectedText, expectedClass string) *html.Node {
	t.Helper()
	preElements := findTestElements(document, "pre")
	require.Len(t, preElements, 1, "expected exactly one structural <pre> element")
	codeElements := findTestElements(preElements[0], "code")
	require.Len(t, codeElements, 1, "expected <pre> to contain exactly one <code> element")
	code := codeElements[0]
	require.Same(t, preElements[0], code.Parent)
	assert.Equal(t, expectedText, testHTMLText(code))
	assert.Equal(t, expectedClass, testHTMLAttribute(code, "class"))
	return code
}
