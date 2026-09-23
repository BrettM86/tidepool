package apobject

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/html"
)

const emphasisFacetTypePrefix = "social.coves.richtext.facet#"

func TestBuildNote_RendersEmphasisAndStrikethroughFacets(t *testing.T) {
	tests := []struct {
		name             string
		feature          string
		expectedMarkdown string
		expectedElement  string
	}{
		{name: "bold", feature: "bold", expectedMarkdown: "before **styled** after", expectedElement: "strong"},
		{name: "italic", feature: "italic", expectedMarkdown: "before *styled* after", expectedElement: "em"},
		{name: "strikethrough", feature: "strikethrough", expectedMarkdown: "before ~~styled~~ after", expectedElement: "del"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, "before styled after", []any{
				emphasisFacet(7, 13, test.feature),
			})

			assert.Equal(t, test.expectedMarkdown, markdown)
			requireSemanticElement(t, parseTestHTML(t, content), test.expectedElement, "styled")
			requireSemanticElement(t, parseRenderedMarkdown(t, markdown), test.expectedElement, "styled")
		})
	}
}

func TestBuildNote_StacksSameRangeFeaturesDeterministically(t *testing.T) {
	const expectedMarkdown = "***~~styled~~***"
	requireEmphasisElements(t, parseRenderedMarkdown(t, expectedMarkdown), "styled")

	tests := []struct {
		name   string
		facets []any
	}{
		{name: "feature order bold italic strike", facets: []any{
			emphasisFacet(0, 6, "bold", "italic", "strikethrough"),
		}},
		{name: "feature order strike bold italic", facets: []any{
			emphasisFacet(0, 6, "strikethrough", "bold", "italic"),
		}},
		{name: "feature order italic strike bold", facets: []any{
			emphasisFacet(0, 6, "italic", "strikethrough", "bold"),
		}},
		{name: "facet order bold italic strike", facets: []any{
			emphasisFacet(0, 6, "bold"),
			emphasisFacet(0, 6, "italic"),
			emphasisFacet(0, 6, "strikethrough"),
		}},
		{name: "facet order strike italic bold", facets: []any{
			emphasisFacet(0, 6, "strikethrough"),
			emphasisFacet(0, 6, "italic"),
			emphasisFacet(0, 6, "bold"),
		}},
	}

	var firstContent string
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, "styled", test.facets)

			assert.Equal(t, expectedMarkdown, markdown)
			requireEmphasisElements(t, parseTestHTML(t, content), "styled")
			requireEmphasisElements(t, parseRenderedMarkdown(t, markdown), "styled")
			if firstContent == "" {
				firstContent = content
			} else {
				assert.Equal(t, firstContent, content, "feature and facet order must not affect structural HTML")
			}
		})
	}
}

func TestBuildNote_RendersNestedEmphasisDeterministically(t *testing.T) {
	const (
		plaintext        = "outer middle inner end"
		expectedMarkdown = "**outer *middle ~~inner~~* end**"
	)
	requireNestedEmphasis(t, parseRenderedMarkdown(t, expectedMarkdown))

	outer := emphasisFacet(0, len(plaintext), "bold")
	middle := emphasisFacet(6, 18, "italic")
	inner := emphasisFacet(13, 18, "strikethrough")
	orders := []struct {
		name   string
		facets []any
	}{
		{name: "outer middle inner", facets: []any{outer, middle, inner}},
		{name: "outer inner middle", facets: []any{outer, inner, middle}},
		{name: "middle outer inner", facets: []any{middle, outer, inner}},
		{name: "middle inner outer", facets: []any{middle, inner, outer}},
		{name: "inner outer middle", facets: []any{inner, outer, middle}},
		{name: "inner middle outer", facets: []any{inner, middle, outer}},
	}

	var firstContent string
	for _, order := range orders {
		t.Run(order.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, plaintext, order.facets)

			assert.Equal(t, expectedMarkdown, markdown)
			requireNestedEmphasis(t, parseTestHTML(t, content))
			requireNestedEmphasis(t, parseRenderedMarkdown(t, markdown))
			if firstContent == "" {
				firstContent = content
			} else {
				assert.Equal(t, firstContent, content, "facet order must not affect nested structural HTML")
			}
		})
	}
}

func TestBuildNote_AdjacentAndPunctuationBoundariesProduceStructuralMarkdown(t *testing.T) {
	tests := []struct {
		name             string
		plaintext        string
		facets           []any
		expectedMarkdown string
		elements         map[string]string
	}{
		{
			name:             "adjacent bold and italic ranges",
			plaintext:        "bolditalic",
			facets:           []any{emphasisFacet(0, 4, "bold"), emphasisFacet(4, 10, "italic")},
			expectedMarkdown: "**bold***italic*",
			elements:         map[string]string{"strong": "bold", "em": "italic"},
		},
		{
			name:             "italic range includes surrounding punctuation",
			plaintext:        "say (soft), now",
			facets:           []any{emphasisFacet(4, 10, "italic")},
			expectedMarkdown: "say *(soft)*, now",
			elements:         map[string]string{"em": "(soft)"},
		},
		{
			name:             "strikethrough range ends in punctuation",
			plaintext:        "remove (gone). now",
			facets:           []any{emphasisFacet(7, 14, "strikethrough")},
			expectedMarkdown: "remove ~~(gone).~~ now",
			elements:         map[string]string{"del": "(gone)."},
		},
		{
			name:             "bold symbol range beside whitespace",
			plaintext:        "love ♥ now",
			facets:           []any{emphasisFacet(len("love "), len("love ♥"), "bold")},
			expectedMarkdown: "love **♥** now",
			elements:         map[string]string{"strong": "♥"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requireSemanticElements(t, parseRenderedMarkdown(t, test.expectedMarkdown), test.elements)
			markdown, content := buildTestNote(t, test.plaintext, test.facets)

			assert.Equal(t, test.expectedMarkdown, markdown)
			requireSemanticElements(t, parseTestHTML(t, content), test.elements)
			requireSemanticElements(t, parseRenderedMarkdown(t, markdown), test.elements)
		})
	}
}

func TestBuildNote_EmphasisWithPunctuationFlankedByWordCharactersDegradesToPlaintext(t *testing.T) {
	tests := []struct {
		name      string
		plaintext string
		start     int
		end       int
		feature   string
	}{
		{name: "italic parenthesized range", plaintext: "a(b)c", start: 1, end: 4, feature: "italic"},
		{name: "italic starts with punctuation after word", plaintext: "a(b c", start: 1, end: 3, feature: "italic"},
		{name: "bold ends with punctuation before word", plaintext: "a b)c", start: 2, end: 4, feature: "bold"},
		{name: "bold reversed parentheses", plaintext: "a)b(c", start: 1, end: 4, feature: "bold"},
		// CommonMark and markdown-it count Unicode symbols (category S) as
		// punctuation when deciding whether a delimiter run is flanking.
		{name: "bold symbol between word characters", plaintext: "a♥b", start: 1, end: len("a♥"), feature: "bold"},
		{name: "italic currency symbol between word characters", plaintext: "a€b", start: 1, end: len("a€"), feature: "italic"},
		{name: "strikethrough math symbol between word characters", plaintext: "a∑b", start: 1, end: len("a∑"), feature: "strikethrough"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, test.plaintext, []any{
				emphasisFacet(test.start, test.end, test.feature),
			})

			assert.Equal(t, test.plaintext, markdown)
			for _, document := range []*html.Node{parseTestHTML(t, content), parseRenderedMarkdown(t, markdown)} {
				requireNoEmphasisElements(t, document)
				assert.Equal(t, test.plaintext, strings.TrimSpace(testHTMLText(document)))
			}
		})
	}
}

func TestBuildNote_InvalidEmphasisBoundariesDegradeToPlaintext(t *testing.T) {
	tests := []struct {
		name         string
		plaintext    string
		start        int
		end          int
		expectedHTML string
	}{
		{
			name: "range crosses paragraphs", plaintext: "first\n\nsecond", start: 0, end: len("first\n\nsecond"),
			expectedHTML: "<p>first</p>\n<p>second</p>\n",
		},
		{
			name: "range starts with whitespace", plaintext: "a styled z", start: 1, end: 8,
			expectedHTML: "<p>a styled z</p>\n",
		},
		{
			name: "range ends with whitespace", plaintext: "a styled z", start: 2, end: 9,
			expectedHTML: "<p>a styled z</p>\n",
		},
		{
			name: "range starts and ends with whitespace", plaintext: "a styled z", start: 1, end: 9,
			expectedHTML: "<p>a styled z</p>\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			facets := []any{
				emphasisFacet(test.start, test.end, "strikethrough", "italic", "bold"),
			}
			markdown, content := buildTestNote(t, test.plaintext, facets)

			assert.Equal(t, test.plaintext, markdown, "an unsafe delimiter range must be ignored as a unit")
			assert.Equal(t, test.expectedHTML, content)
			requireNoEmphasisElements(t, parseTestHTML(t, content))
			requireNoEmphasisElements(t, parseRenderedMarkdown(t, markdown))
		})
	}
}

func TestBuildNote_EmphasisAcrossWhitespaceOnlyBlankLineDegradesToEscapedPlaintext(t *testing.T) {
	const (
		plaintext        = "a *literal*\n \nb [literal]"
		expectedMarkdown = "a \\*literal\\*\n \nb \\[literal\\]"
		expectedHTML     = "<p>a *literal*</p>\n<p>b [literal]</p>\n"
	)
	markdown, content := buildTestNote(t, plaintext, []any{
		emphasisFacet(0, len(plaintext), "bold", "italic", "strikethrough"),
	})

	assert.Equal(t, expectedMarkdown, markdown)
	assert.Equal(t, expectedHTML, content)
	for _, document := range []*html.Node{parseTestHTML(t, content), parseRenderedMarkdown(t, markdown)} {
		requireNoEmphasisElements(t, document)
		paragraphs := findTestElements(document, "p")
		require.Len(t, paragraphs, 2)
		assert.Equal(t, "a *literal*", testHTMLText(paragraphs[0]))
		assert.Equal(t, "b [literal]", testHTMLText(paragraphs[1]))
	}
}

func TestBuildNote_CrossingInlineRangesDegradeToPlaintextIndependentOfFacetOrder(t *testing.T) {
	const (
		plaintext    = "abcdef"
		expectedHTML = "<p>abcdef</p>\n"
	)
	bold := emphasisFacet(0, 4, "bold")
	italic := emphasisFacet(2, 6, "italic")

	for _, facets := range [][]any{
		{bold, italic},
		{italic, bold}, // Reversed input must choose the same safe fallback.
	} {
		markdown, content := buildTestNote(t, plaintext, facets)

		assert.Equal(t, plaintext, markdown, "incompatible crossing annotations must not emit broken delimiters")
		assert.Equal(t, expectedHTML, content)
		assert.Equal(t, plaintext, strings.TrimSpace(testHTMLText(parseRenderedMarkdown(t, markdown))))
		requireNoEmphasisElements(t, parseTestHTML(t, content))
		requireNoEmphasisElements(t, parseRenderedMarkdown(t, markdown))
	}
}

func TestBuildNote_TouchingSameFeatureRangesMergeWithoutDelimiterCollision(t *testing.T) {
	tests := []struct {
		name             string
		plaintext        string
		feature          string
		split            int
		expectedMarkdown string
		expectedHTML     string
	}{
		{name: "italic", plaintext: "ab", feature: "italic", split: 1,
			expectedMarkdown: "*ab*", expectedHTML: "<p><em>ab</em></p>\n"},
		{name: "bold", plaintext: "ab", feature: "bold", split: 1,
			expectedMarkdown: "**ab**", expectedHTML: "<p><strong>ab</strong></p>\n"},
		{name: "strikethrough", plaintext: "foobar", feature: "strikethrough", split: 3,
			expectedMarkdown: "~~foobar~~", expectedHTML: "<p><del>foobar</del></p>\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			first := emphasisFacet(0, test.split, test.feature)
			second := emphasisFacet(test.split, len(test.plaintext), test.feature)
			for _, facets := range [][]any{{first, second}, {second, first}, {first, first, second}} {
				markdown, content := buildTestNote(t, test.plaintext, facets)

				assert.Equal(t, test.expectedMarkdown, markdown)
				assert.Equal(t, test.expectedHTML, content)
			}
		})
	}
}

func TestBuildNote_ContainedAndOverlappingSameFeatureRangesMergeIntoTheirUnion(t *testing.T) {
	const plaintext = "abcdefghij"
	tests := []struct {
		name             string
		facets           []any
		expectedMarkdown string
		expectedHTML     string
	}{
		{name: "bold contained", facets: []any{emphasisFacet(0, 10, "bold"), emphasisFacet(2, 5, "bold")},
			expectedMarkdown: "**abcdefghij**", expectedHTML: "<p><strong>abcdefghij</strong></p>\n"},
		{name: "italic contained", facets: []any{emphasisFacet(0, 10, "italic"), emphasisFacet(2, 5, "italic")},
			expectedMarkdown: "*abcdefghij*", expectedHTML: "<p><em>abcdefghij</em></p>\n"},
		{name: "strikethrough contained", facets: []any{emphasisFacet(0, 10, "strikethrough"), emphasisFacet(2, 5, "strikethrough")},
			expectedMarkdown: "~~abcdefghij~~", expectedHTML: "<p><del>abcdefghij</del></p>\n"},
		{name: "bold contained sharing the start", facets: []any{emphasisFacet(0, 10, "bold"), emphasisFacet(0, 5, "bold")},
			expectedMarkdown: "**abcdefghij**", expectedHTML: "<p><strong>abcdefghij</strong></p>\n"},
		{name: "bold overlapping", facets: []any{emphasisFacet(0, 5, "bold"), emphasisFacet(3, 8, "bold")},
			expectedMarkdown: "**abcdefgh**ij", expectedHTML: "<p><strong>abcdefgh</strong>ij</p>\n"},
		{name: "italic overlapping", facets: []any{emphasisFacet(0, 5, "italic"), emphasisFacet(3, 8, "italic")},
			expectedMarkdown: "*abcdefgh*ij", expectedHTML: "<p><em>abcdefgh</em>ij</p>\n"},
		{name: "strikethrough overlapping", facets: []any{emphasisFacet(0, 5, "strikethrough"), emphasisFacet(3, 8, "strikethrough")},
			expectedMarkdown: "~~abcdefgh~~ij", expectedHTML: "<p><del>abcdefgh</del>ij</p>\n"},
		{name: "union still nests another feature", facets: []any{
			emphasisFacet(0, 10, "bold"), emphasisFacet(2, 5, "bold"), emphasisFacet(2, 5, "italic"),
		}, expectedMarkdown: "**ab*cde*fghij**", expectedHTML: "<p><strong>ab<em>cde</em>fghij</strong></p>\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reversed := make([]any, len(test.facets))
			for i, facet := range test.facets {
				reversed[len(test.facets)-1-i] = facet
			}
			for _, facets := range [][]any{test.facets, reversed} {
				markdown, content := buildTestNote(t, plaintext, facets)

				assert.Equal(t, test.expectedMarkdown, markdown)
				assert.Equal(t, test.expectedHTML, content)
			}
		})
	}
}

func TestBuildNote_IndentedCodeDetectionUsesWholeSourceLine(t *testing.T) {
	tests := []struct {
		name             string
		plaintext        string
		start            int
		end              int
		expectedMarkdown string
		expectedHTML     string
	}{
		{name: "four spaces before bold", plaintext: "    bold line", start: 4, end: 8,
			expectedMarkdown: "&#32;   **bold** line", expectedHTML: "<p><strong>bold</strong> line</p>\n"},
		{name: "tab before bold on later line", plaintext: "first\n\tbold line", start: 7, end: 11,
			expectedMarkdown: "first\n&#9;**bold** line", expectedHTML: "<p>first\n\t<strong>bold</strong> line</p>\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, test.plaintext, []any{emphasisFacet(test.start, test.end, "bold")})

			assert.Equal(t, test.expectedMarkdown, markdown)
			assert.Equal(t, test.expectedHTML, content)
			rendered := parseRenderedMarkdown(t, markdown)
			assert.Empty(t, findTestElements(rendered, "pre"), "indented prose must not become a code block")
			requireSemanticElement(t, rendered, "strong", "bold")
		})
	}
}

// Emphasis whose emitted delimiter run would sit between a word character and
// punctuation (a link bracket or another delimiter) cannot open or close in
// CommonMark, so it is dropped from both outputs instead of leaking literal
// asterisks into Lemmy.
func TestBuildNote_IntrawordEmphasisBesidePunctuationDelimitersIsDropped(t *testing.T) {
	const target = "https://example.com/docs"
	link := map[string]any{"$type": emphasisFacetTypePrefix + "link", "uri": target}
	bold := map[string]any{"$type": emphasisFacetTypePrefix + "bold"}
	strikethrough := map[string]any{"$type": emphasisFacetTypePrefix + "strikethrough"}
	tests := []struct {
		name             string
		plaintext        string
		facets           []any
		expectedMarkdown string
		expectedHTML     string
	}{
		{name: "link opens with bold after word", plaintext: "foobar baz",
			facets:           []any{testFacet(3, 10, bold), testFacet(3, 6, link)},
			expectedMarkdown: "foo[bar](" + target + ") baz",
			expectedHTML:     "<p>foo<a href=\"" + target + "\">bar</a> baz</p>\n"},
		{name: "link closes with bold before word", plaintext: "bar bazqux",
			facets:           []any{testFacet(0, 7, bold), testFacet(4, 7, link)},
			expectedMarkdown: "bar [baz](" + target + ")qux",
			expectedHTML:     "<p>bar <a href=\"" + target + "\">baz</a>qux</p>\n"},
		{name: "stacked bold and strikethrough inside word", plaintext: "foobar baz",
			facets:           []any{testFacet(3, 6, bold, strikethrough)},
			expectedMarkdown: "foobar baz",
			expectedHTML:     "<p>foobar baz</p>\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, test.plaintext, test.facets)

			assert.Equal(t, test.expectedMarkdown, markdown)
			assert.Equal(t, test.expectedHTML, content)
			rendered := parseRenderedMarkdown(t, markdown)
			requireNoEmphasisElements(t, rendered)
			assert.Equal(t, test.plaintext, strings.TrimSpace(testHTMLText(rendered)))
		})
	}
}

func requireEmphasisElements(t *testing.T, document *html.Node, text string) {
	t.Helper()
	requireSemanticElements(t, document, map[string]string{"strong": text, "em": text, "del": text})
}

func requireSemanticElements(t *testing.T, document *html.Node, elements map[string]string) {
	t.Helper()
	for name, text := range elements {
		requireSemanticElement(t, document, name, text)
	}
}

func requireSemanticElement(t *testing.T, document *html.Node, name, text string) *html.Node {
	t.Helper()
	elements := findTestElements(document, name)
	require.Len(t, elements, 1, "expected one structural <%s> element", name)
	assert.Equal(t, text, testHTMLText(elements[0]))
	return elements[0]
}

func requireNestedEmphasis(t *testing.T, document *html.Node) {
	t.Helper()
	strike := requireSemanticElement(t, document, "del", "inner")
	require.NotNil(t, strike.Parent)
	assert.Equal(t, "em", strike.Parent.Data)
	assert.Equal(t, "middle inner", testHTMLText(strike.Parent))
	require.NotNil(t, strike.Parent.Parent)
	assert.Equal(t, "strong", strike.Parent.Parent.Data)
	assert.Equal(t, "outer middle inner end", testHTMLText(strike.Parent.Parent))
}

func requireNoEmphasisElements(t *testing.T, document *html.Node) {
	t.Helper()
	for _, name := range []string{"strong", "em", "del"} {
		assert.Empty(t, findTestElements(document, name), "invalid ranges must not create <%s>", name)
	}
}
