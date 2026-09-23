package apobject

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/html"
)

const (
	testBlockquoteFacetType = "social.coves.richtext.facet#blockquote"
	testHeadingFacetType    = "social.coves.richtext.facet#heading"
)

func TestBuildNote_HeadingFacetsRenderExactLevelsAndEscapeLiteralMarkers(t *testing.T) {
	const plaintext = "Title *stars* # hash > marker"

	for level := 1; level <= 6; level++ {
		t.Run(fmt.Sprintf("level %d", level), func(t *testing.T) {
			expectedMarkdown := strings.Repeat("#", level) + ` Title \*stars\* \# hash \> marker`
			expectedHTML := fmt.Sprintf("<h%d>Title *stars* # hash &gt; marker</h%d>\n", level, level)
			requireSemanticElement(t, parseRenderedMarkdown(t, expectedMarkdown), fmt.Sprintf("h%d", level), plaintext)
			markdown, content := buildTestNote(t, plaintext, []any{
				testFacet(0, len(plaintext), testHeadingFeature(level)),
			})

			assert.Equal(t, expectedMarkdown, markdown)
			assert.Equal(t, expectedHTML, content)
			requireSemanticElement(t, parseTestHTML(t, content), fmt.Sprintf("h%d", level), plaintext)
			requireSemanticElement(t, parseRenderedMarkdown(t, markdown), fmt.Sprintf("h%d", level), plaintext)
		})
	}

	t.Run("JSON decoded integral level", func(t *testing.T) {
		markdown, content := buildTestNote(t, plaintext, []any{
			testFacet(0, len(plaintext), testHeadingFeature(float64(4))),
		})

		assert.Equal(t, `#### Title \*stars\* \# hash \> marker`, markdown)
		assert.Equal(t, "<h4>Title *stars* # hash &gt; marker</h4>\n", content)
		requireSemanticElement(t, parseTestHTML(t, content), "h4", plaintext)
	})
}

func TestBuildNote_InvalidHeadingFacetsRemainVisiblePlaintext(t *testing.T) {
	const (
		plaintext        = "# literal > marker"
		expectedMarkdown = `\# literal \> marker`
		expectedHTML     = "<p># literal &gt; marker</p>\n"
	)
	tests := []struct {
		name    string
		feature map[string]any
	}{
		{name: "missing required level", feature: map[string]any{"$type": testHeadingFacetType}},
		{name: "level below range", feature: testHeadingFeature(0)},
		{name: "level above range", feature: testHeadingFeature(7)},
		{name: "fractional level", feature: testHeadingFeature(2.5)},
		{name: "string level", feature: testHeadingFeature("2")},
		{name: "boolean level", feature: testHeadingFeature(true)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, plaintext, []any{
				testFacet(0, len(plaintext), test.feature),
			})

			assert.Equal(t, expectedMarkdown, markdown)
			assert.Equal(t, expectedHTML, content)
			requireNoBlockElements(t, parseTestHTML(t, content))
			requireNoBlockElements(t, parseRenderedMarkdown(t, markdown))
		})
	}

	t.Run("more than one line", func(t *testing.T) {
		const (
			multiline        = "first #\nsecond >"
			expectedMarkdown = "first \\#\nsecond \\>"
		)
		markdown, content := buildTestNote(t, multiline, []any{
			testFacet(0, len(multiline), testHeadingFeature(2)),
		})

		assert.Equal(t, expectedMarkdown, markdown)
		assert.Equal(t, "<p>first #\nsecond &gt;</p>\n", content)
		requireNoBlockElements(t, parseTestHTML(t, content))
		requireNoBlockElements(t, parseRenderedMarkdown(t, markdown))
	})

	t.Run("empty line", func(t *testing.T) {
		const withEmptyLine = "before\n\nafter"
		emptyLineStart := len("before\n")
		markdown, content := buildTestNote(t, withEmptyLine, []any{
			testFacet(emptyLineStart, emptyLineStart+1, testHeadingFeature(2)),
		})

		assert.Equal(t, withEmptyLine, markdown)
		assert.Equal(t, "<p>before</p>\n<p>after</p>\n", content)
		requireNoBlockElements(t, parseTestHTML(t, content))
		requireNoBlockElements(t, parseRenderedMarkdown(t, markdown))
	})
}

func TestBuildNote_BlockquoteFacetsRenderDefaultAndExactLevels(t *testing.T) {
	const plaintext = "quoted > marker # hash *stars*"
	tests := []struct {
		name    string
		level   int
		feature map[string]any
	}{
		{name: "absent level defaults to one", level: 1, feature: testBlockquoteFeature()},
		{name: "explicit level one", level: 1, feature: testBlockquoteFeature(float64(1))},
		{name: "explicit level two", level: 2, feature: testBlockquoteFeature(float64(2))},
		{name: "explicit level three", level: 3, feature: testBlockquoteFeature(float64(3))},
		{name: "explicit level four", level: 4, feature: testBlockquoteFeature(float64(4))},
		{name: "explicit level five", level: 5, feature: testBlockquoteFeature(float64(5))},
		{name: "explicit level six", level: 6, feature: testBlockquoteFeature(float64(6))},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expectedMarkdown := strings.Repeat("> ", test.level) + `quoted \> marker \# hash \*stars\*`
			expectedHTML := strings.Repeat("<blockquote>\n", test.level) +
				"<p>quoted &gt; marker # hash *stars*</p>\n" + strings.Repeat("</blockquote>\n", test.level)
			requireQuoteBlocks(t, parseRenderedMarkdown(t, expectedMarkdown), []quoteBlockExpectation{{test.level, plaintext}})
			markdown, content := buildTestNote(t, plaintext, []any{
				testFacet(0, len(plaintext), test.feature),
			})

			assert.Equal(t, expectedMarkdown, markdown)
			assert.Equal(t, expectedHTML, content)
			requireQuoteBlocks(t, parseTestHTML(t, content), []quoteBlockExpectation{{test.level, plaintext}})
			requireQuoteBlocks(t, parseRenderedMarkdown(t, markdown), []quoteBlockExpectation{{test.level, plaintext}})
		})
	}

	t.Run("one multiline facet prefixes every line as one quote block", func(t *testing.T) {
		const (
			multiline        = "first > marker\nsecond # marker"
			expectedMarkdown = "> first \\> marker\n> second \\# marker"
			expectedHTML     = "<blockquote>\n<p>first &gt; marker\nsecond # marker</p>\n</blockquote>\n"
		)
		expectedQuotes := []quoteBlockExpectation{{1, "first > marker\nsecond # marker"}}
		requireQuoteBlocks(t, parseRenderedMarkdown(t, expectedMarkdown), expectedQuotes)
		markdown, content := buildTestNote(t, multiline, []any{
			testFacet(0, len(multiline), testBlockquoteFeature()),
		})

		assert.Equal(t, expectedMarkdown, markdown)
		assert.Equal(t, expectedHTML, content)
		requireQuoteBlocks(t, parseTestHTML(t, content), expectedQuotes)
		requireQuoteBlocks(t, parseRenderedMarkdown(t, markdown), expectedQuotes)
	})
}

func TestBuildNote_BlockquoteSpanningWhitespaceBlankLineRendersMultipleParagraphs(t *testing.T) {
	const (
		plaintext        = "first paragraph\n \t \nsecond paragraph"
		expectedMarkdown = "> first paragraph\n>  \t \n> second paragraph"
		expectedHTML     = "<blockquote>\n<p>first paragraph</p>\n<p>second paragraph</p>\n</blockquote>\n"
	)
	requireSingleQuoteParagraphs(t, parseRenderedMarkdown(t, expectedMarkdown), "first paragraph", "second paragraph")

	markdown, content := buildTestNote(t, plaintext, []any{
		testFacet(0, len(plaintext), testBlockquoteFeature()),
	})

	assert.Equal(t, expectedMarkdown, markdown)
	assert.Equal(t, expectedHTML, content)
	t.Run("direct HTML structure", func(t *testing.T) {
		requireSingleQuoteParagraphs(t, parseTestHTML(t, content), "first paragraph", "second paragraph")
	})
	t.Run("source Markdown structure", func(t *testing.T) {
		requireSingleQuoteParagraphs(t, parseRenderedMarkdown(t, markdown), "first paragraph", "second paragraph")
	})
}

func TestBuildNote_InvalidExplicitBlockquoteLevelsRemainVisiblePlaintext(t *testing.T) {
	const (
		plaintext        = "quoted > marker"
		expectedMarkdown = `quoted \> marker`
		expectedHTML     = "<p>quoted &gt; marker</p>\n"
	)
	tests := []struct {
		name  string
		level any
	}{
		{name: "below range", level: 0},
		{name: "above range", level: 7},
		{name: "fractional", level: 1.5},
		{name: "string", level: "2"},
		{name: "boolean", level: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, plaintext, []any{
				testFacet(0, len(plaintext), testBlockquoteFeature(test.level)),
			})

			assert.Equal(t, expectedMarkdown, markdown,
				"a present malformed level must not silently become the default quote level")
			assert.Equal(t, expectedHTML, content)
			assert.Empty(t, findTestElements(parseTestHTML(t, content), "blockquote"))
			assert.Empty(t, findTestElements(parseRenderedMarkdown(t, markdown), "blockquote"))
		})
	}
}

func TestBuildNote_PartialBlockRangesExpandUsingOriginalUTF8AndCRLFOffsets(t *testing.T) {
	const plaintext = "é preface\r\nHéading *literal* > marker\r\nqµoted # marker > literal\r\ntail"
	headingLineStart := len("é preface\r\n")
	headingLineEnd := headingLineStart + len("Héading *literal* > marker")
	quoteLineStart := headingLineEnd + len("\r\n")

	// Both partial ranges begin at a multibyte rune boundary inside their line;
	// another multibyte rune and a CRLF precede them in the original byte stream.
	headingStart := headingLineStart + len("H")
	headingEnd := headingLineStart + len("Héading *literal")
	quoteStart := quoteLineStart + len("q")
	quoteEnd := quoteLineStart + len("qµoted # marker")
	require.Greater(t, headingStart, len([]rune(plaintext[:headingStart])))
	require.Equal(t, "é", string([]rune(plaintext[headingStart:])[:1]))
	require.Equal(t, "µ", string([]rune(plaintext[quoteStart:])[:1]))

	markdown, content := buildTestNote(t, plaintext, []any{
		testFacet(headingStart, headingEnd, testHeadingFeature(3)),
		testFacet(quoteStart, quoteEnd, testBlockquoteFeature()),
	})
	const expectedMarkdown = "é preface\n### Héading \\*literal\\* \\> marker\n" +
		"> qµoted \\# marker \\> literal\n\ntail"
	const expectedHTML = "<p>é preface</p>\n" +
		"<h3>Héading *literal* &gt; marker</h3>\n" +
		"<blockquote>\n<p>qµoted # marker &gt; literal</p>\n</blockquote>\n" +
		"<p>tail</p>\n"
	requireSemanticElement(t, parseRenderedMarkdown(t, expectedMarkdown), "h3", "Héading *literal* > marker")
	requireQuoteBlocks(t, parseRenderedMarkdown(t, expectedMarkdown), []quoteBlockExpectation{{1, "qµoted # marker > literal"}})

	assert.Equal(t, expectedMarkdown, markdown)
	assert.Equal(t, expectedHTML, content)
	requireSemanticElement(t, parseTestHTML(t, content), "h3", "Héading *literal* > marker")
	requireQuoteBlocks(t, parseTestHTML(t, content), []quoteBlockExpectation{{1, "qµoted # marker > literal"}})
	requireSemanticElement(t, parseRenderedMarkdown(t, markdown), "h3", "Héading *literal* > marker")
	requireQuoteBlocks(t, parseRenderedMarkdown(t, markdown), []quoteBlockExpectation{{1, "qµoted # marker > literal"}})
}

func TestBuildNote_BlockFacetsRejectRangesSplittingOriginalUTF8Runes(t *testing.T) {
	const (
		plaintext        = "é > literal"
		expectedMarkdown = `é \> literal`
		expectedHTML     = "<p>é &gt; literal</p>\n"
	)
	tests := []struct {
		name    string
		feature map[string]any
	}{
		{name: "heading", feature: testHeadingFeature(2)},
		{name: "blockquote", feature: testBlockquoteFeature(2)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, plaintext, []any{
				testFacet(1, len(plaintext), test.feature),
			})

			assert.Equal(t, expectedMarkdown, markdown)
			assert.Equal(t, expectedHTML, content)
			requireNoBlockElements(t, parseTestHTML(t, content))
			requireNoBlockElements(t, parseRenderedMarkdown(t, markdown))
		})
	}
}

func TestBuildNote_DistinctAdjacentQuotesPreserveBlocksDepthAndDeterminism(t *testing.T) {
	const plaintext = "first > marker\nsecond # marker\ndeep * marker\nshallower > marker"
	first := testBlockFacetForText(t, plaintext, "first > marker", testBlockquoteFeature(1))
	second := testBlockFacetForText(t, plaintext, "second # marker", testBlockquoteFeature(1))
	deep := testBlockFacetForText(t, plaintext, "deep * marker", testBlockquoteFeature(3))
	shallower := testBlockFacetForText(t, plaintext, "shallower > marker", testBlockquoteFeature(2))
	firstWithDuplicateFeatures := testBlockFacetForText(t, plaintext, "first > marker",
		testBlockquoteFeature(1), testBlockquoteFeature(1))
	tests := []struct {
		name   string
		facets []any
	}{
		{name: "source order", facets: []any{first, second, deep, shallower}},
		{name: "reverse order with duplicates", facets: []any{
			shallower, deep, second, firstWithDuplicateFeatures, deep, second,
		}},
	}
	const expectedMarkdown = "> first \\> marker\n\n" +
		"> second \\# marker\n\n" +
		"> > > deep \\* marker\n\n" +
		"> > shallower \\> marker"
	const expectedHTML = "<blockquote>\n<p>first &gt; marker</p>\n</blockquote>\n" +
		"<blockquote>\n<p>second # marker</p>\n</blockquote>\n" +
		"<blockquote>\n<blockquote>\n<blockquote>\n<p>deep * marker</p>\n</blockquote>\n</blockquote>\n</blockquote>\n" +
		"<blockquote>\n<blockquote>\n<p>shallower &gt; marker</p>\n</blockquote>\n</blockquote>\n"
	expectedQuotes := []quoteBlockExpectation{
		{depth: 1, text: "first > marker"},
		{depth: 1, text: "second # marker"},
		{depth: 3, text: "deep * marker"},
		{depth: 2, text: "shallower > marker"},
	}
	requireQuoteBlocks(t, parseRenderedMarkdown(t, expectedMarkdown), expectedQuotes)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, plaintext, test.facets)

			assert.Equal(t, expectedMarkdown, markdown,
				"adjacent facet ranges are distinct quotes, while exact duplicates are idempotent")
			assert.Equal(t, expectedHTML, content)
			requireQuoteBlocks(t, parseTestHTML(t, content), expectedQuotes)
			requireQuoteBlocks(t, parseRenderedMarkdown(t, markdown), expectedQuotes)
		})
	}
}

func TestBuildNote_IncompatibleBlockquoteRangesAllDegradeDeterministically(t *testing.T) {
	const (
		plaintext        = "alpha > marker\nbravo # marker\ncharlie * marker\ndelta _ marker"
		expectedMarkdown = "alpha \\> marker\nbravo \\# marker\ncharlie \\* marker\ndelta \\_ marker"
		expectedHTML     = "<p>alpha &gt; marker\nbravo # marker\ncharlie * marker\ndelta _ marker</p>\n"
	)
	outer := testFacet(strings.Index(plaintext, "alpha")+1, strings.Index(plaintext, "charlie")+4,
		testBlockquoteFeature(1))
	crossing := testFacet(strings.Index(plaintext, "bravo")+1, strings.Index(plaintext, "delta")+3,
		testBlockquoteFeature(2))
	nested := testFacet(strings.Index(plaintext, "bravo")+2, strings.Index(plaintext, "bravo")+4,
		testBlockquoteFeature(3))
	orders := [][]any{
		{outer, crossing, nested},
		{outer, nested, crossing},
		{crossing, outer, nested},
		{crossing, nested, outer},
		{nested, outer, crossing},
		{nested, crossing, outer},
	}

	for orderIndex, facets := range orders {
		t.Run(fmt.Sprintf("facet order %d", orderIndex), func(t *testing.T) {
			markdownResults := make(map[string]int)
			contentResults := make(map[string]int)
			for range 32 {
				markdown, content := buildTestNote(t, plaintext, facets)
				markdownResults[markdown]++
				contentResults[content]++
			}

			assert.Equal(t, map[string]int{expectedMarkdown: 32}, markdownResults)
			assert.Equal(t, map[string]int{expectedHTML: 32}, contentResults)
			for markdown := range markdownResults {
				assert.Empty(t, findTestElements(parseRenderedMarkdown(t, markdown), "blockquote"))
			}
			for content := range contentResults {
				assert.Empty(t, findTestElements(parseTestHTML(t, content), "blockquote"))
			}
		})
	}
}

func TestBuildNote_HeadingWinsSameLineBlockquoteConflictIndependentOfOrder(t *testing.T) {
	const (
		plaintext        = "section > marker"
		expectedMarkdown = `## section \> marker`
		expectedHTML     = "<h2>section &gt; marker</h2>\n"
	)
	heading := testHeadingFeature(2)
	quote := testBlockquoteFeature(3)
	tests := []struct {
		name   string
		facets []any
	}{
		{name: "same facet heading first", facets: []any{testFacet(0, len(plaintext), heading, quote)}},
		{name: "same facet quote first", facets: []any{testFacet(0, len(plaintext), quote, heading)}},
		{name: "separate facets heading first", facets: []any{
			testFacet(0, len(plaintext), heading), testFacet(0, len(plaintext), quote),
		}},
		{name: "separate facets quote first with duplicates", facets: []any{
			testFacet(0, len(plaintext), quote), testFacet(0, len(plaintext), heading),
			testFacet(0, len(plaintext), quote), testFacet(0, len(plaintext), heading),
		}},
	}

	// A heading carries a required, specific semantic rank, so it takes
	// precedence. The lower-priority quote annotation degrades without losing
	// visible text instead of producing order-dependent nested block markup.
	requireSemanticElement(t, parseRenderedMarkdown(t, expectedMarkdown), "h2", plaintext)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, plaintext, test.facets)

			assert.Equal(t, expectedMarkdown, markdown)
			assert.Equal(t, expectedHTML, content)
			requireSemanticElement(t, parseTestHTML(t, content), "h2", plaintext)
			assert.Empty(t, findTestElements(parseTestHTML(t, content), "blockquote"))
			requireSemanticElement(t, parseRenderedMarkdown(t, markdown), "h2", plaintext)
			assert.Empty(t, findTestElements(parseRenderedMarkdown(t, markdown), "blockquote"))
		})
	}
}

func TestBuildNote_HeadingInsideWiderQuoteRendersNested(t *testing.T) {
	const (
		plaintext        = "quoted intro\nTitle\nquoted outro"
		expectedMarkdown = "> quoted intro\n> ## Title\n> quoted outro"
		expectedHTML     = "<blockquote>\n<p>quoted intro</p>\n<h2>Title</h2>\n<p>quoted outro</p>\n</blockquote>\n"
	)
	quote := testFacet(0, len(plaintext), testBlockquoteFeature(1))
	heading := testBlockFacetForText(t, plaintext, "Title", testHeadingFeature(2))
	expectedQuote := requireSingleQuoteParagraphs(t, parseRenderedMarkdown(t, expectedMarkdown), "quoted intro", "quoted outro")
	requireSemanticElement(t, expectedQuote, "h2", "Title")

	for _, facets := range [][]any{{quote, heading}, {heading, quote}} {
		markdown, content := buildTestNote(t, plaintext, facets)

		assert.Equal(t, expectedMarkdown, markdown)
		assert.Equal(t, expectedHTML, content)
	}
}

func testBlockFacetForText(t *testing.T, plaintext, text string, features ...any) map[string]any {
	t.Helper()
	require.Equal(t, 1, strings.Count(plaintext, text), "facet text must be unique in the fixture")
	start := strings.Index(plaintext, text)
	require.NotEqual(t, -1, start)
	return testFacet(start, start+len(text), features...)
}

func testHeadingFeature(level any) map[string]any {
	return map[string]any{"$type": testHeadingFacetType, "level": level}
}

func testBlockquoteFeature(level ...any) map[string]any {
	feature := map[string]any{"$type": testBlockquoteFacetType}
	if len(level) != 0 {
		feature["level"] = level[0]
	}
	return feature
}

type quoteBlockExpectation struct {
	depth int
	text  string
}

func requireQuoteBlocks(t *testing.T, document *html.Node, expected []quoteBlockExpectation) {
	t.Helper()
	allQuotes := findTestElements(document, "blockquote")
	topLevelQuotes := make([]*html.Node, 0, len(allQuotes))
	for _, quote := range allQuotes {
		if quote.Parent == nil || quote.Parent.Data != "blockquote" {
			topLevelQuotes = append(topLevelQuotes, quote)
		}
	}
	require.Len(t, topLevelQuotes, len(expected), "expected distinct top-level quote blocks")

	for i, want := range expected {
		quoteChain := findTestElements(topLevelQuotes[i], "blockquote")
		require.Len(t, quoteChain, want.depth, "quote %d must have the requested nesting depth", i)
		for depth := 1; depth < len(quoteChain); depth++ {
			assert.Same(t, quoteChain[depth-1], quoteChain[depth].Parent,
				"quote %d must be one unbranched nesting chain", i)
		}
		paragraphs := findTestElements(quoteChain[len(quoteChain)-1], "p")
		require.Len(t, paragraphs, 1, "quote %d must contain one paragraph", i)
		assert.Equal(t, want.text, testHTMLText(paragraphs[0]))
	}
}

func requireSingleQuoteParagraphs(t *testing.T, document *html.Node, expected ...string) *html.Node {
	t.Helper()
	quotes := findTestElements(document, "blockquote")
	require.Len(t, quotes, 1, "expected exactly one structural <blockquote> element")
	paragraphs := findTestElements(quotes[0], "p")
	require.Len(t, paragraphs, len(expected), "quote must preserve paragraph boundaries")
	for i, text := range expected {
		assert.Equal(t, text, testHTMLText(paragraphs[i]))
	}
	return quotes[0]
}

func requireNoBlockElements(t *testing.T, document *html.Node) {
	t.Helper()
	for level := 1; level <= 6; level++ {
		assert.Empty(t, findTestElements(document, fmt.Sprintf("h%d", level)))
	}
	assert.Empty(t, findTestElements(document, "blockquote"))
}
