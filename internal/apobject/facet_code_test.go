package apobject

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/html"
)

const codeFacetType = "social.coves.richtext.facet#code"

func TestBuildNote_InlineCodeUsesMinimumDelimiterAndPreservesLiteralBytes(t *testing.T) {
	const (
		plaintext        = `before *[x]_ <tag>&copy; C:\temp after`
		codeText         = `*[x]_ <tag>&copy; C:\temp`
		expectedMarkdown = "before `*[x]_ <tag>&copy; C:\\temp` after"
		expectedHTML     = "<p>before <code>*[x]_ &lt;tag&gt;&amp;copy; C:\\temp</code> after</p>\n"
	)
	start := len("before ")
	markdown, content := buildTestNote(t, plaintext, []any{
		testFacet(start, start+len(codeText), codeFeature()),
	})

	assert.Equal(t, expectedMarkdown, markdown, "code bytes must not receive prose Markdown escaping")
	assert.Equal(t, expectedHTML, content)
	requireCodeText(t, parseTestHTML(t, content), codeText)
	requireCodeText(t, parseRenderedMarkdown(t, markdown), codeText)
}

func TestBuildNote_InlineCodeChoosesDelimiterAndPaddingByContent(t *testing.T) {
	tests := []struct {
		name             string
		prefix           string
		code             string
		suffix           string
		expectedMarkdown string
		expectedHTML     string
	}{
		{
			name:             "embedded backtick run requires longer delimiter",
			prefix:           "run ",
			code:             "a `` b",
			suffix:           " now",
			expectedMarkdown: "run ```a `` b``` now",
			expectedHTML:     "<p>run <code>a `` b</code> now</p>\n",
		},
		{
			name:             "backticks at both endpoints require padding",
			prefix:           "run ",
			code:             "`edge`",
			suffix:           " now",
			expectedMarkdown: "run `` `edge` `` now",
			expectedHTML:     "<p>run <code>`edge`</code> now</p>\n",
		},
		{
			name:             "backtick at opening endpoint requires padding",
			prefix:           "run ",
			code:             "`edge",
			suffix:           " now",
			expectedMarkdown: "run `` `edge `` now",
			expectedHTML:     "<p>run <code>`edge</code> now</p>\n",
		},
		{
			name:             "backtick at closing endpoint requires padding",
			prefix:           "run ",
			code:             "edge`",
			suffix:           " now",
			expectedMarkdown: "run `` edge` `` now",
			expectedHTML:     "<p>run <code>edge`</code> now</p>\n",
		},
		{
			name:             "leading space needs no padding",
			prefix:           "run:",
			code:             " leading",
			suffix:           "; done",
			expectedMarkdown: "run:` leading`; done",
			expectedHTML:     "<p>run:<code> leading</code>; done</p>\n",
		},
		{
			name:             "trailing space needs no padding",
			prefix:           "run:",
			code:             "trailing ",
			suffix:           "; done",
			expectedMarkdown: "run:`trailing `; done",
			expectedHTML:     "<p>run:<code>trailing </code>; done</p>\n",
		},
		{
			name:             "spaces at both endpoints require preservation padding",
			prefix:           "run:",
			code:             " both ",
			suffix:           "; done",
			expectedMarkdown: "run:`  both  `; done",
			expectedHTML:     "<p>run:<code> both </code>; done</p>\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plaintext := test.prefix + test.code + test.suffix
			start := len(test.prefix)
			markdown, content := buildTestNote(t, plaintext, []any{
				testFacet(start, start+len(test.code), codeFeature()),
			})

			requireCodeText(t, parseRenderedMarkdown(t, test.expectedMarkdown), test.code)
			assert.Equal(t, test.expectedMarkdown, markdown)
			assert.Equal(t, test.expectedHTML, content)
			requireCodeText(t, parseTestHTML(t, content), test.code)
			requireCodeText(t, parseRenderedMarkdown(t, markdown), test.code)
		})
	}
}

func TestBuildNote_UnsafeInlineCodeRangesDegradeToPlaintext(t *testing.T) {
	tests := []struct {
		name             string
		plaintext        string
		start            int
		end              int
		expectedMarkdown string
		expectedHTML     string
	}{
		{
			name:             "all whitespace",
			plaintext:        "before   after",
			start:            len("before"),
			end:              len("before   "),
			expectedMarkdown: "before   after",
			expectedHTML:     "<p>before   after</p>\n",
		},
		{
			name:             "multiline",
			plaintext:        "before one*\ntwo after",
			start:            len("before "),
			end:              len("before one*\ntwo"),
			expectedMarkdown: "before one\\*\ntwo after",
			expectedHTML:     "<p>before one*\ntwo after</p>\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, test.plaintext, []any{
				testFacet(test.start, test.end, codeFeature()),
			})

			assert.Equal(t, test.expectedMarkdown, markdown)
			assert.Equal(t, test.expectedHTML, content)
			assert.Empty(t, findTestElements(parseTestHTML(t, content), "code"))
			assert.Empty(t, findTestElements(parseRenderedMarkdown(t, markdown), "code"))
		})
	}
}

func TestBuildNote_InlineCodeWinsOverSameRangeEmphasisAndLinkIndependentOfOrder(t *testing.T) {
	const (
		plaintext        = `before raw*[x]<b>&copy;\path after`
		codeText         = `raw*[x]<b>&copy;\path`
		expectedMarkdown = "before `raw*[x]<b>&copy;\\path` after"
		expectedHTML     = "<p>before <code>raw*[x]&lt;b&gt;&amp;copy;\\path</code> after</p>\n"
	)
	start, end := len("before "), len("before ")+len(codeText)
	code := codeFeature()
	bold := map[string]any{"$type": emphasisFacetTypePrefix + "bold"}
	link := linkFeature("https://example.com/code")
	tests := []struct {
		name   string
		facets []any
	}{
		{name: "feature order code emphasis link", facets: []any{
			testFacet(start, end, code, bold, link),
		}},
		{name: "feature order link emphasis code", facets: []any{
			testFacet(start, end, link, bold, code),
		}},
		{name: "facet order code emphasis link", facets: []any{
			testFacet(start, end, code),
			testFacet(start, end, bold),
			testFacet(start, end, link),
		}},
		{name: "facet order link emphasis code", facets: []any{
			testFacet(start, end, link),
			testFacet(start, end, bold),
			testFacet(start, end, code),
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, plaintext, test.facets)

			assert.Equal(t, expectedMarkdown, markdown)
			assert.Equal(t, expectedHTML, content)
			requireCodeText(t, parseTestHTML(t, content), codeText)
			requireCodeText(t, parseRenderedMarkdown(t, markdown), codeText)
			for _, element := range []string{"strong", "em", "del", "a"} {
				assert.Empty(t, findTestElements(parseTestHTML(t, content), element))
				assert.Empty(t, findTestElements(parseRenderedMarkdown(t, markdown), element))
			}
		})
	}
}

func TestBuildNote_InlineCodeWinsOverPartiallyOverlappingEmphasisAndLinkIndependentOfOrder(t *testing.T) {
	const (
		plaintext        = "say code target now"
		codeText         = "code target"
		expectedMarkdown = "say `code target` now"
		expectedHTML     = "<p>say <code>code target</code> now</p>\n"
	)
	code := testFacet(len("say "), len("say code target"), codeFeature())
	link := testFacet(0, len("say code"), linkFeature("https://example.com/code"))
	bold := testFacet(len("say code "), len(plaintext), map[string]any{
		"$type": emphasisFacetTypePrefix + "bold",
	})
	tests := []struct {
		name   string
		facets []any
	}{
		{name: "code link emphasis", facets: []any{code, link, bold}},
		{name: "emphasis link code", facets: []any{bold, link, code}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, plaintext, test.facets)

			assert.Equal(t, expectedMarkdown, markdown)
			assert.Equal(t, expectedHTML, content)
			requireCodeText(t, parseTestHTML(t, content), codeText)
			requireCodeText(t, parseRenderedMarkdown(t, markdown), codeText)
			for _, element := range []string{"strong", "a"} {
				assert.Empty(t, findTestElements(parseTestHTML(t, content), element))
				assert.Empty(t, findTestElements(parseRenderedMarkdown(t, markdown), element))
			}
		})
	}
}

func TestBuildNote_NestedInlineCodeRangesDegradeToPlaintextIndependentOfOrder(t *testing.T) {
	const plaintext = "run outer inner tail now"
	outer := testFacet(len("run "), len("run outer inner tail"), codeFeature())
	inner := testFacet(len("run outer "), len("run outer inner"), codeFeature())

	for _, test := range []struct {
		name   string
		facets []any
	}{
		{name: "outer first", facets: []any{outer, inner}},
		{name: "inner first", facets: []any{inner, outer}},
	} {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, plaintext, test.facets)

			assert.Equal(t, plaintext, markdown)
			for _, document := range []*html.Node{parseTestHTML(t, content), parseRenderedMarkdown(t, markdown)} {
				assert.Empty(t, findTestElements(document, "code"), "conflicting nested code must not create code spans")
				assert.Equal(t, plaintext, strings.TrimSpace(testHTMLText(document)))
			}
		})
	}
}

func TestBuildNote_DuplicateInlineCodeFeaturesDoNotMultiplyDelimiters(t *testing.T) {
	const (
		plaintext        = "run value now"
		expectedMarkdown = "run `value` now"
		expectedHTML     = "<p>run <code>value</code> now</p>\n"
	)
	start, end := len("run "), len("run value")
	code := codeFeature()
	markdown, content := buildTestNote(t, plaintext, []any{
		testFacet(start, end, code, code),
		testFacet(start, end, code),
	})

	assert.Equal(t, expectedMarkdown, markdown)
	assert.Equal(t, expectedHTML, content)
	requireCodeText(t, parseTestHTML(t, content), "value")
	requireCodeText(t, parseRenderedMarkdown(t, markdown), "value")
}

func TestBuildNote_TouchingInlineCodeRangesMergeIntoOneSpan(t *testing.T) {
	tests := []struct {
		name             string
		plaintext        string
		split            int
		expectedMarkdown string
		expectedHTML     string
	}{
		{name: "plain letters", plaintext: "ab", split: 1,
			expectedMarkdown: "`ab`", expectedHTML: "<p><code>ab</code></p>\n"},
		{name: "different delimiter lengths", plaintext: "a`b", split: 2,
			expectedMarkdown: "``a`b``", expectedHTML: "<p><code>a`b</code></p>\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			first := testFacet(0, test.split, codeFeature())
			second := testFacet(test.split, len(test.plaintext), codeFeature())
			for _, facets := range [][]any{{first, second}, {second, first}} {
				markdown, content := buildTestNote(t, test.plaintext, facets)

				assert.Equal(t, test.expectedMarkdown, markdown)
				assert.Equal(t, test.expectedHTML, content)
				requireCodeText(t, parseRenderedMarkdown(t, markdown), test.plaintext)
			}
		})
	}
}

func codeFeature() map[string]any {
	return map[string]any{"$type": codeFacetType}
}

func requireCodeText(t *testing.T, document *html.Node, expected string) {
	t.Helper()
	codes := findTestElements(document, "code")
	require.Len(t, codes, 1, "expected exactly one structural <code> element")
	assert.Equal(t, expected, testHTMLText(codes[0]))
}
