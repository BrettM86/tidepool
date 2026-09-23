package apobject

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/html"
)

const linkFacetType = "social.coves.richtext.facet#link"

func TestBuildNote_LinkFacetsRenderSafeTargets(t *testing.T) {
	tests := []struct {
		name             string
		plaintext        string
		start            int
		end              int
		uri              string
		expectedMarkdown string
		expectedHref     string
		expectedText     string
	}{
		{
			name:             "HTTP target with query string",
			plaintext:        "read docs now",
			start:            len("read "),
			end:              len("read docs"),
			uri:              "http://example.com/docs?one=1&two=2",
			expectedMarkdown: "read [docs](http://example.com/docs?one=1&two=2) now",
			expectedHref:     "http://example.com/docs?one=1&two=2",
			expectedText:     "docs",
		},
		{
			name:             "HTTPS target and Markdown punctuation in visible text",
			plaintext:        "open [docs]*_ now",
			start:            len("open "),
			end:              len("open [docs]*_"),
			uri:              `https://example.com/a(b)?q=hello world/雪&path=one\two "title"`,
			expectedMarkdown: `open [\[docs\]\*\_](https://example.com/a\(b\)?q=hello%20world/%E9%9B%AA&path=one%5Ctwo%20%22title%22) now`,
			expectedHref:     `https://example.com/a(b)?q=hello%20world/%E9%9B%AA&path=one%5Ctwo%20%22title%22`,
			expectedText:     "[docs]*_",
		},
		{
			// Internationalized hosts stay allowed; Go's url package emits them
			// as percent-encoded UTF-8, which is a valid RFC 3986 reg-name.
			name:             "internationalized host",
			plaintext:        "read docs now",
			start:            len("read "),
			end:              len("read docs"),
			uri:              "https://例え.jp/docs",
			expectedMarkdown: "read [docs](https://%E4%BE%8B%E3%81%88.jp/docs) now",
			expectedHref:     "https://%E4%BE%8B%E3%81%88.jp/docs",
			expectedText:     "docs",
		},
		{
			name:             "IPv4 host with port",
			plaintext:        "read docs now",
			start:            len("read "),
			end:              len("read docs"),
			uri:              "http://192.0.2.1:8080/docs",
			expectedMarkdown: "read [docs](http://192.0.2.1:8080/docs) now",
			expectedHref:     "http://192.0.2.1:8080/docs",
			expectedText:     "docs",
		},
		{
			name:             "sub-delimiters in host",
			plaintext:        "read docs now",
			start:            len("read "),
			end:              len("read docs"),
			uri:              "https://a-b_c~d!$'*+,;=.example/docs",
			expectedMarkdown: "read [docs](https://a-b_c~d!$'*+,;=.example/docs) now",
			expectedHref:     "https://a-b_c~d!$'*+,;=.example/docs",
			expectedText:     "docs",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, test.plaintext, []any{
				testFacet(test.start, test.end, linkFeature(test.uri)),
			})

			assert.Equal(t, test.expectedMarkdown, markdown)
			assertLink(t, parseTestHTML(t, content), test.expectedHref, test.expectedText)
			assertLink(t, parseRenderedMarkdown(t, markdown), test.expectedHref, test.expectedText)
		})
	}
}

func TestBuildNote_LinkFacetsFailClosedToEscapedVisibleText(t *testing.T) {
	const (
		plaintext        = "click [me] *now*"
		expectedMarkdown = `click \[me\] \*now\*`
		expectedHTML     = "<p>click [me] *now*</p>\n"
	)
	start, end := len("click "), len(plaintext)
	tests := []struct {
		name     string
		features []any
	}{
		{name: "javascript scheme", features: []any{linkFeature("javascript:alert(1)")}},
		{name: "data scheme", features: []any{linkFeature("data:text/html,<script>alert(1)</script>")}},
		{name: "file scheme", features: []any{linkFeature("file:///etc/passwd")}},
		{name: "non-web scheme", features: []any{linkFeature("mailto:user@example.com")}},
		{name: "angle brackets in host", features: []any{linkFeature("https://ex<b>ample.com/docs")}},
		{name: "quote in host", features: []any{linkFeature(`https://ex"ample.com/docs`)}},
		{name: "percent-encoded percent in host", features: []any{linkFeature("https://ex%25ample.com/docs")}},
		// markdown-it percent-encodes the brackets of an IPv6 literal, so
		// Lemmy's link would point at an invalid host while the HTML would not.
		{name: "bracketed IPv6 host", features: []any{linkFeature("http://[2001:db8::1]/docs")}},
		{name: "missing URI", features: []any{map[string]any{"$type": linkFacetType}}},
		{name: "non-string URI", features: []any{map[string]any{"$type": linkFacetType, "uri": 42.0}}},
		{name: "mention", features: []any{map[string]any{
			"$type": "social.coves.richtext.facet#mention", "did": "did:plc:example",
		}}},
		{name: "unknown feature", features: []any{map[string]any{
			"$type": "social.coves.richtext.facet#rainbow",
		}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, plaintext, []any{
				testFacet(start, end, test.features...),
			})

			assert.Equal(t, expectedMarkdown, markdown)
			assert.Equal(t, expectedHTML, content)
			assertNoLinks(t, parseTestHTML(t, content), plaintext)
			assertNoLinks(t, parseRenderedMarkdown(t, markdown), plaintext)
		})
	}
}

func TestBuildNote_CompetingLinkTargetsFailClosedDeterministically(t *testing.T) {
	const (
		plaintext        = "click [me] *now*"
		expectedMarkdown = `click \[me\] \*now\*`
	)
	start, end := len("click "), len(plaintext)
	first := linkFeature("https://first.example/path")
	second := linkFeature("https://second.example/path")
	tests := []struct {
		name   string
		facets []any
	}{
		{name: "features first then second", facets: []any{testFacet(start, end, first, second)}},
		{name: "features second then first", facets: []any{testFacet(start, end, second, first)}},
		{name: "facets first then second", facets: []any{
			testFacet(start, end, first), testFacet(start, end, second),
		}},
		{name: "facets second then first", facets: []any{
			testFacet(start, end, second), testFacet(start, end, first),
		}},
	}

	var firstContent string
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, plaintext, test.facets)

			assert.Equal(t, expectedMarkdown, markdown)
			assertNoLinks(t, parseTestHTML(t, content), plaintext)
			assertNoLinks(t, parseRenderedMarkdown(t, markdown), plaintext)
			if firstContent == "" {
				firstContent = content
			} else {
				assert.Equal(t, firstContent, content, "competing link order must not affect HTML fallback")
			}
		})
	}
}

func TestBuildNote_NestedLinksWithDifferentTargetsDegradeToPlaintextIndependentOfOrder(t *testing.T) {
	const plaintext = "outer inner tail"
	outer := testFacet(0, len(plaintext), linkFeature("https://outer.example/path"))
	inner := testFacet(len("outer "), len("outer inner"), linkFeature("https://inner.example/path"))

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
				assertNoLinks(t, document, plaintext)
			}
		})
	}
}

func TestBuildNote_LinkEntityLookingDestinationRemainsLiteralAfterParsing(t *testing.T) {
	const (
		plaintext = "open docs now"
		target    = "https://example.com/search?first=1&copy;=2&#38;=3"
	)
	markdown, content := buildTestNote(t, plaintext, []any{
		testFacet(len("open "), len("open docs"), linkFeature(target)),
	})

	assertLink(t, parseTestHTML(t, content), target, "docs")
	assertLink(t, parseRenderedMarkdown(t, markdown), target, "docs")
}

func TestBuildNote_LinkAcrossCRLFWhitespaceOnlyBlankLineDegradesToEscapedPlaintext(t *testing.T) {
	const (
		plaintext        = "a *literal*\r\n \t\r\nb [literal]"
		expectedMarkdown = "a \\*literal\\*\n \t\nb \\[literal\\]"
		expectedHTML     = "<p>a *literal*</p>\n<p>b [literal]</p>\n"
	)
	markdown, content := buildTestNote(t, plaintext, []any{
		testFacet(0, len(plaintext), linkFeature("https://example.com/unsafe-span")),
	})

	assert.Equal(t, expectedMarkdown, markdown)
	assert.Equal(t, expectedHTML, content)
	for _, document := range []*html.Node{parseTestHTML(t, content), parseRenderedMarkdown(t, markdown)} {
		assertNoLinks(t, document, "a *literal*\nb [literal]")
		paragraphs := findTestElements(document, "p")
		require.Len(t, paragraphs, 2)
		assert.Equal(t, "a *literal*", testHTMLText(paragraphs[0]))
		assert.Equal(t, "b [literal]", testHTMLText(paragraphs[1]))
	}
}

func TestBuildNote_ValidLinkSurvivesUnknownMalformedAndDuplicateFeaturesDeterministically(t *testing.T) {
	const (
		plaintext        = "visit [docs]*_ now"
		expectedMarkdown = `visit [\[docs\]\*\_](https://example.com/docs) now`
		expectedHref     = "https://example.com/docs"
		expectedText     = "[docs]*_"
	)
	start, end := len("visit "), len("visit [docs]*_")
	valid := linkFeature(expectedHref)
	unknown := map[string]any{"$type": "social.coves.richtext.facet#future"}
	missingURI := map[string]any{"$type": linkFacetType}
	nonStringURI := map[string]any{"$type": linkFacetType, "uri": true}
	tests := []struct {
		name   string
		facets []any
	}{
		{name: "feature order valid first", facets: []any{
			testFacet(start, end, valid, valid, unknown, "malformed", missingURI, nonStringURI),
		}},
		{name: "feature order valid last", facets: []any{
			testFacet(start, end, nonStringURI, missingURI, "malformed", unknown, valid, valid),
		}},
		{name: "facet order valid first", facets: []any{
			testFacet(start, end, valid),
			testFacet(start, end, valid),
			testFacet(start, end, unknown),
			"malformed",
			testFacet(start, end, missingURI),
			testFacet(start, end, nonStringURI),
		}},
		{name: "facet order valid last", facets: []any{
			testFacet(start, end, nonStringURI),
			testFacet(start, end, missingURI),
			"malformed",
			testFacet(start, end, unknown),
			testFacet(start, end, valid),
			testFacet(start, end, valid),
		}},
	}

	var firstContent string
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, plaintext, test.facets)

			assert.Equal(t, expectedMarkdown, markdown)
			assertLink(t, parseTestHTML(t, content), expectedHref, expectedText)
			assertLink(t, parseRenderedMarkdown(t, markdown), expectedHref, expectedText)
			if firstContent == "" {
				firstContent = content
			} else {
				assert.Equal(t, firstContent, content, "feature and facet order must not affect link HTML")
			}
		})
	}
}

func TestBuildNote_LiteralExclamationBeforeLinkCannotFormImage(t *testing.T) {
	const (
		target           = "https://example.com/image.png"
		expectedMarkdown = `\![photo](https://example.com/image.png)`
		expectedHTML     = "<p>!<a href=\"https://example.com/image.png\">photo</a></p>\n"
	)
	markdown, content := buildTestNote(t, "!photo", []any{testFacet(1, 6, linkFeature(target))})

	assert.Equal(t, expectedMarkdown, markdown)
	assert.Equal(t, expectedHTML, content)
	rendered := parseRenderedMarkdown(t, markdown)
	assert.Empty(t, findTestElements(rendered, "img"), "a literal ! must not turn a link into an image")
	assertLink(t, rendered, target, "photo")
}

func TestBuildNote_TouchingSameTargetLinkRangesMerge(t *testing.T) {
	const target = "https://example.com/docs"
	first := testFacet(0, 1, linkFeature(target))
	second := testFacet(1, 2, linkFeature(target))
	for _, facets := range [][]any{{first, second}, {second, first}} {
		markdown, content := buildTestNote(t, "ab", facets)

		assert.Equal(t, "[ab]("+target+")", markdown)
		assert.Equal(t, "<p><a href=\""+target+"\">ab</a></p>\n", content)
	}
}

func TestBuildNote_LinkRangesTrimSurroundingWhitespace(t *testing.T) {
	const target = "https://example.com/docs"
	tests := []struct {
		name             string
		plaintext        string
		facets           []any
		expectedMarkdown string
		expectedHTML     string
	}{
		{name: "trailing paragraph break", plaintext: "a\n\nb",
			facets:           []any{testFacet(0, 2, linkFeature(target))},
			expectedMarkdown: "[a](" + target + ")\n\nb",
			expectedHTML:     "<p><a href=\"" + target + "\">a</a></p>\n<p>b</p>\n"},
		{name: "trailing newline before heading", plaintext: "see docs\ntitle",
			facets: []any{
				testFacet(4, 9, linkFeature(target)),
				testFacet(9, 14, testHeadingFeature(float64(1))),
			},
			expectedMarkdown: "see [docs](" + target + ")\n# title",
			expectedHTML:     "<p>see <a href=\"" + target + "\">docs</a></p>\n<h1>title</h1>\n"},
		{name: "trailing carriage return of CRLF", plaintext: "docs\r\nnext",
			facets:           []any{testFacet(0, 5, linkFeature(target))},
			expectedMarkdown: "[docs](" + target + ")\nnext",
			expectedHTML:     "<p><a href=\"" + target + "\">docs</a>\nnext</p>\n"},
		{name: "leading space", plaintext: "see docs",
			facets:           []any{testFacet(3, 8, linkFeature(target))},
			expectedMarkdown: "see [docs](" + target + ")",
			expectedHTML:     "<p>see <a href=\"" + target + "\">docs</a></p>\n"},
		{name: "whitespace only", plaintext: "a  b",
			facets:           []any{testFacet(1, 3, linkFeature(target))},
			expectedMarkdown: "a  b",
			expectedHTML:     "<p>a  b</p>\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, test.plaintext, test.facets)

			assert.Equal(t, test.expectedMarkdown, markdown)
			assert.Equal(t, test.expectedHTML, content)
		})
	}
}

func linkFeature(uri any) map[string]any {
	return map[string]any{"$type": linkFacetType, "uri": uri}
}

func assertLink(t *testing.T, document *html.Node, expectedHref, expectedText string) {
	t.Helper()
	links := findTestElements(document, "a")
	require.Len(t, links, 1, "expected exactly one structural link")
	assert.Equal(t, expectedText, testHTMLText(links[0]))
	assert.Equal(t, expectedHref, testHTMLAttribute(links[0], "href"))
	assert.Empty(t, testHTMLAttribute(links[0], "title"), "destination punctuation must not become a link title")
}

func assertNoLinks(t *testing.T, document *html.Node, expectedText string) {
	t.Helper()
	assert.Empty(t, findTestElements(document, "a"), "unsupported link facets must not create anchors")
	assert.Equal(t, expectedText, strings.TrimSpace(testHTMLText(document)))
}
