package apobject

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/html"
)

const testSpoilerFacetType = "social.coves.richtext.facet#spoiler"

func TestBuildNote_SpoilerIsolatesPartialLineAndKeepsCompatibleInlineFormatting(t *testing.T) {
	const (
		plaintext        = "before visible secret hidden after visible"
		hiddenText       = "secret hidden"
		expectedMarkdown = "before visible\n\n::: spoiler Ending\nsecret **hidden**\n:::\n\nafter visible"
		expectedHTML     = "<p>before visible</p>\n<details>\n<summary>Ending</summary>\n" +
			"<p>secret <strong>hidden</strong></p>\n</details>\n<p>after visible</p>\n"
	)
	spoilerStart := strings.Index(plaintext, hiddenText)
	spoilerEnd := spoilerStart + len(hiddenText)
	boldStart := strings.Index(plaintext, "hidden")
	spoilerFacet := testFacet(spoilerStart, spoilerEnd, testSpoilerFeature("Ending"))
	boldFacet := testFacet(boldStart, boldStart+len("hidden"),
		map[string]any{"$type": emphasisFacetTypePrefix + "bold"})
	tests := []struct {
		name   string
		facets []any
	}{
		{name: "source order", facets: []any{spoilerFacet, boldFacet}},
		{name: "reverse order with duplicate", facets: []any{boldFacet, spoilerFacet, spoilerFacet}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, plaintext, test.facets)

			assert.Equal(t, expectedMarkdown, markdown)
			assert.Equal(t, expectedHTML, content)
			document := parseTestHTML(t, content)
			details := requireSpoiler(t, document, "Ending", hiddenText)
			requireSemanticElement(t, details, "strong", "hidden")
			paragraphs := findTestElements(document, "p")
			require.Len(t, paragraphs, 3)
			assert.Equal(t, "before visible", testHTMLText(paragraphs[0]))
			assert.Equal(t, hiddenText, testHTMLText(paragraphs[1]))
			assert.Equal(t, "after visible", testHTMLText(paragraphs[2]))
		})
	}
}

func TestBuildNote_SpoilerReasonsCannotInjectMarkupOrContainers(t *testing.T) {
	const plaintext = "hidden"
	tests := []struct {
		name    string
		feature map[string]any
	}{
		{name: "absent", feature: map[string]any{"$type": testSpoilerFacetType}},
		{name: "empty", feature: map[string]any{"$type": testSpoilerFacetType, "reason": ""}},
		{name: "whitespace", feature: testSpoilerFeature(" \t ")},
		{name: "wrong type", feature: map[string]any{"$type": testSpoilerFacetType, "reason": 7}},
		{name: "newline", feature: testSpoilerFeature("Ending\n:::\nvisible injection")},
		{name: "HTML", feature: testSpoilerFeature("<img src=x onerror=alert(1)>")},
		{name: "Markdown", feature: testSpoilerFeature("**Ending** [click](javascript:alert(1))")},
		{name: "container characters", feature: testSpoilerFeature("::: spoiler injected")},
		{name: "over byte cap", feature: testSpoilerFeature(strings.Repeat("a", 129))},
		// lemmy-ui validates the title with /^spoiler\s+(.*)$/ after
		// String.prototype.trim: "." stops at U+2028 and U+2029, and a title of
		// only JavaScript whitespace (which includes U+FEFF) trims to nothing.
		{name: "line separator", feature: testSpoilerFeature("Ending\u2028soon")},
		{name: "paragraph separator", feature: testSpoilerFeature("Ending\u2029soon")},
		{name: "next line", feature: testSpoilerFeature("Ending\u0085soon")},
		{name: "byte order mark only", feature: testSpoilerFeature("\ufeff")},
		{name: "leading byte order mark", feature: testSpoilerFeature("\ufeffEnding")},
		{name: "trailing byte order mark", feature: testSpoilerFeature("Ending\ufeff")},
		{name: "no-break space only", feature: testSpoilerFeature("\u00a0")},
		{name: "ideographic space only", feature: testSpoilerFeature("\u3000")},
		{name: "trailing line separator", feature: testSpoilerFeature("Ending\u2028")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, plaintext, []any{
				testFacet(0, len(plaintext), test.feature),
			})

			assert.Equal(t, "::: spoiler Spoiler\nhidden\n:::", markdown,
				"an absent or unsafe reason must use the default title, since lemmy-ui only opens a spoiler with a title")
			details := requireSpoiler(t, parseTestHTML(t, content), "Spoiler", plaintext)
			assert.Empty(t, findTestElements(details, "img"))
			assert.Empty(t, findTestElements(details, "a"))
			assert.NotContains(t, content, "onerror")
			assert.NotContains(t, content, "javascript:")
		})
	}
}

func TestBuildNote_SpoilerReasonKeepsInteriorUnicodeSpace(t *testing.T) {
	markdown, content := buildTestNote(t, "hidden", []any{
		testFacet(0, len("hidden"), testSpoilerFeature("Ending\u00a0soon")),
	})

	assert.Equal(t, "::: spoiler Ending\u00a0soon\nhidden\n:::", markdown,
		"lemmy-ui's \\s+(.*) accepts a title with interior Unicode spaces")
	requireSpoiler(t, parseTestHTML(t, content), "Ending\u00a0soon", "hidden")
}

func TestBuildNote_SpoilerReasonEnforcesGraphemeCap(t *testing.T) {
	const plaintext = "hidden"
	safeReason := strings.Repeat("界", 31) + "e\u0301"
	overGraphemeCap := safeReason + "界"
	tests := []struct {
		name             string
		reason           string
		expectedMarkdown string
		expectedSummary  string
	}{
		{
			name:             "32 graphemes retained",
			reason:           safeReason,
			expectedMarkdown: "::: spoiler " + safeReason + "\nhidden\n:::",
			expectedSummary:  safeReason,
		},
		{
			name:             "33 graphemes omitted",
			reason:           overGraphemeCap,
			expectedMarkdown: "::: spoiler Spoiler\nhidden\n:::",
			expectedSummary:  "Spoiler",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.LessOrEqual(t, len(test.reason), 128)
			markdown, content := buildTestNote(t, plaintext, []any{
				testFacet(0, len(plaintext), testSpoilerFeature(test.reason)),
			})

			assert.Equal(t, test.expectedMarkdown, markdown)
			requireSpoiler(t, parseTestHTML(t, content), test.expectedSummary, plaintext)
		})
	}
}

func TestBuildNote_ConflictingSpoilersMergeAndStayHidden(t *testing.T) {
	const (
		conflictingText = "alpha beta gamma delta"
		validText       = "safe hidden"
		plaintext       = conflictingText + "\n" + validText
		validMarkdown   = "::: spoiler Safe\nsafe hidden\n:::"
		validHTML       = "<details>\n<summary>Safe</summary>\n<p>safe hidden</p>\n</details>\n"
	)
	validStart := len(conflictingText) + 1
	valid := testFacet(validStart, len(plaintext), testSpoilerFeature("Safe"))
	tests := []struct {
		name             string
		first            map[string]any
		second           map[string]any
		expectedMarkdown string
		expectedHTML     string
	}{
		{
			name:             "same range with different reasons uses the default title",
			first:            testFacet(0, len("alpha beta"), testSpoilerFeature("first")),
			second:           testFacet(0, len("alpha beta"), testSpoilerFeature("second")),
			expectedMarkdown: "::: spoiler Spoiler\nalpha beta\n:::\n\ngamma delta\n\n" + validMarkdown,
			expectedHTML: "<details>\n<summary>Spoiler</summary>\n<p>alpha beta</p>\n</details>\n" +
				"<p>gamma delta</p>\n" + validHTML,
		},
		{
			name:             "nested renders only the outermost",
			first:            testFacet(0, len("alpha beta gamma"), testSpoilerFeature("outer")),
			second:           testFacet(len("alpha "), len("alpha beta"), testSpoilerFeature("inner")),
			expectedMarkdown: "::: spoiler outer\nalpha beta gamma\n:::\n\ndelta\n\n" + validMarkdown,
			expectedHTML: "<details>\n<summary>outer</summary>\n<p>alpha beta gamma</p>\n</details>\n" +
				"<p>delta</p>\n" + validHTML,
		},
		{
			name:             "crossing with different reasons merges under the default title",
			first:            testFacet(0, len("alpha beta"), testSpoilerFeature("first")),
			second:           testFacet(len("alpha "), len("alpha beta gamma"), testSpoilerFeature("second")),
			expectedMarkdown: "::: spoiler Spoiler\nalpha beta gamma\n:::\n\ndelta\n\n" + validMarkdown,
			expectedHTML: "<details>\n<summary>Spoiler</summary>\n<p>alpha beta gamma</p>\n</details>\n" +
				"<p>delta</p>\n" + validHTML,
		},
		{
			name:             "crossing with the same reason keeps it",
			first:            testFacet(0, len("alpha beta"), testSpoilerFeature("shared")),
			second:           testFacet(len("alpha "), len("alpha beta gamma"), testSpoilerFeature("shared")),
			expectedMarkdown: "::: spoiler shared\nalpha beta gamma\n:::\n\ndelta\n\n" + validMarkdown,
			expectedHTML: "<details>\n<summary>shared</summary>\n<p>alpha beta gamma</p>\n</details>\n" +
				"<p>delta</p>\n" + validHTML,
		},
	}

	for _, test := range tests {
		for _, order := range []struct {
			name   string
			facets []any
		}{
			{name: "source order", facets: []any{test.first, test.second, valid}},
			{name: "reverse order with duplicates", facets: []any{valid, test.second, test.first, valid, test.second, test.first}},
		} {
			t.Run(test.name+"/"+order.name, func(t *testing.T) {
				markdown, content := buildTestNote(t, plaintext, order.facets)

				assert.Equal(t, test.expectedMarkdown, markdown)
				assert.Equal(t, test.expectedHTML, content)
				assert.Len(t, findTestElements(parseTestHTML(t, content), "details"), 2,
					"conflicting spoilers must merge into one hidden container, never degrade to visible text")
			})
		}
	}
}

func TestBuildNote_SpoilerWidensToCoverTouchedBlocks(t *testing.T) {
	const (
		codePlaintext = "secret *literal* <tag>"
		codeMarkdown  = "::: spoiler Ending\n```\nsecret *literal* <tag>\n```\n:::"
		codeHTML      = "<details>\n<summary>Ending</summary>\n" +
			"<pre><code>secret *literal* &lt;tag&gt;</code></pre>\n</details>\n"
	)
	code := testFacet(0, len(codePlaintext), testCodeBlockFeature(""))
	partialSpoiler := testFacet(len("secret "), len(codePlaintext), testSpoilerFeature("Ending"))
	insideSpoiler := testFacet(len("secret "), len("secret *literal*"), testSpoilerFeature("Ending"))

	const (
		quotePlaintext = "intro secret\nquoted line\nafter"
		quoteMarkdown  = "intro\n\n::: spoiler Spoiler\nsecret\n> quoted line\n:::\n\nafter"
		quoteHTML      = "<p>intro</p>\n<details>\n<summary>Spoiler</summary>\n<p>secret</p>\n" +
			"<blockquote>\n<p>quoted line</p>\n</blockquote>\n</details>\n<p>after</p>\n"
	)
	quote := testFacet(strings.Index(quotePlaintext, "quoted"), strings.Index(quotePlaintext, " line"),
		testBlockquoteFeature(1))
	crossingQuoteSpoiler := testFacet(strings.Index(quotePlaintext, "secret"), strings.Index(quotePlaintext, " line"),
		testSpoilerFeature(""))

	const (
		headingPlaintext = "Big secret title\nbody"
		headingMarkdown  = "::: spoiler Spoiler\n## Big secret title\n:::\n\nbody"
		headingHTML      = "<details>\n<summary>Spoiler</summary>\n<h2>Big secret title</h2>\n</details>\n<p>body</p>\n"
	)
	heading := testFacet(0, len("Big secret title"), testHeadingFeature(2))
	headingSpoiler := testFacet(len("Big "), len("Big secret"), testSpoilerFeature(""))

	const (
		chainPlaintext = "a secret\ncode line\nb secret"
		chainMarkdown  = "a\n\n::: spoiler Spoiler\nsecret\n\n```\ncode line\n```\n\nb secret\n:::"
		chainHTML      = "<p>a</p>\n<details>\n<summary>Spoiler</summary>\n<p>secret</p>\n" +
			"<pre><code>code line</code></pre>\n<p>b secret</p>\n</details>\n"
	)
	chainCodeStart := strings.Index(chainPlaintext, "code")
	chainCode := testFacet(chainCodeStart, chainCodeStart+len("code line"), testCodeBlockFeature(""))
	chainFirst := testFacet(strings.Index(chainPlaintext, "secret"), chainCodeStart+len("code"),
		testSpoilerFeature("first"))
	chainSecond := testFacet(strings.Index(chainPlaintext, "line"), len(chainPlaintext),
		testSpoilerFeature("second"))

	tests := []struct {
		name             string
		plaintext        string
		facets           []any
		expectedMarkdown string
		expectedHTML     string
	}{
		{name: "spoiler partially overlapping a code block", plaintext: codePlaintext,
			facets: []any{code, partialSpoiler}, expectedMarkdown: codeMarkdown, expectedHTML: codeHTML},
		{name: "spoiler partially overlapping a code block reversed with duplicates", plaintext: codePlaintext,
			facets: []any{partialSpoiler, code, partialSpoiler, code}, expectedMarkdown: codeMarkdown, expectedHTML: codeHTML},
		{name: "spoiler inside a code block", plaintext: codePlaintext,
			facets: []any{insideSpoiler, code}, expectedMarkdown: codeMarkdown, expectedHTML: codeHTML},
		{name: "spoiler crossing a quote", plaintext: quotePlaintext,
			facets: []any{crossingQuoteSpoiler, quote}, expectedMarkdown: quoteMarkdown, expectedHTML: quoteHTML},
		{name: "spoiler inside a heading", plaintext: headingPlaintext,
			facets: []any{headingSpoiler, heading}, expectedMarkdown: headingMarkdown, expectedHTML: headingHTML},
		{name: "widening creates a new overlap that merges", plaintext: chainPlaintext,
			facets: []any{chainSecond, chainCode, chainFirst}, expectedMarkdown: chainMarkdown, expectedHTML: chainHTML},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, test.plaintext, test.facets)

			assert.Equal(t, test.expectedMarkdown, markdown)
			assert.Equal(t, test.expectedHTML, content)
		})
	}
}

func TestBuildNote_SpoilerContainerOutlastsColonRunsInsideCodeBlock(t *testing.T) {
	const (
		plaintext        = "hidden\n```\n:::\nafter"
		expectedMarkdown = ":::: spoiler Twist\nhidden\n\n````\n```\n:::\n````\n::::\n\nafter"
		expectedHTML     = "<details>\n<summary>Twist</summary>\n<p>hidden</p>\n" +
			"<pre><code>```\n:::</code></pre>\n</details>\n<p>after</p>\n"
	)
	codeStart := strings.Index(plaintext, "```")
	codeEnd := strings.Index(plaintext, "\nafter")
	markdown, content := buildTestNote(t, plaintext, []any{
		testFacet(codeStart, codeEnd, testCodeBlockFeature("")),
		testFacet(0, codeEnd, testSpoilerFeature("Twist")),
	})

	assert.Equal(t, expectedMarkdown, markdown,
		"a code line of colons must not close the spoiler container, so the container marker outgrows it")
	assert.Equal(t, expectedHTML, content)
}

func TestBuildNote_SpoilerInsideQuoteRendersNestedInTheQuote(t *testing.T) {
	const (
		plaintext        = "quoted intro\nsecret\nquoted outro"
		expectedMarkdown = "> quoted intro\n>\n> ::: spoiler Spoiler\n> secret\n> :::\n>\n> quoted outro"
		expectedHTML     = "<blockquote>\n<p>quoted intro</p>\n<details>\n<summary>Spoiler</summary>\n" +
			"<p>secret</p>\n</details>\n<p>quoted outro</p>\n</blockquote>\n"
	)
	secretStart := strings.Index(plaintext, "secret")
	quote := testFacet(0, len(plaintext), testBlockquoteFeature(1))
	spoiler := testFacet(secretStart, secretStart+len("secret"), testSpoilerFeature(""))
	for _, facets := range [][]any{{quote, spoiler}, {spoiler, quote}} {
		markdown, content := buildTestNote(t, plaintext, facets)

		assert.Equal(t, expectedMarkdown, markdown)
		assert.Equal(t, expectedHTML, content)
	}
}

func TestBuildNote_LiteralSpoilerFenceLineCannotTerminateAnnotatedContent(t *testing.T) {
	const (
		plaintext        = "before\nhidden\n:::\nstill hidden\nafter"
		hiddenText       = "hidden\n:::\nstill hidden"
		expectedMarkdown = "before\n\n::: spoiler Twist\nhidden\n\\:::\nstill hidden\n:::\n\nafter"
		expectedHTML     = "<p>before</p>\n<details>\n<summary>Twist</summary>\n" +
			"<p>hidden\n:::\nstill hidden</p>\n</details>\n<p>after</p>\n"
	)
	start := strings.Index(plaintext, hiddenText)
	markdown, content := buildTestNote(t, plaintext, []any{
		testFacet(start, start+len(hiddenText), testSpoilerFeature("Twist")),
	})

	assert.Equal(t, expectedMarkdown, markdown)
	assert.Equal(t, expectedHTML, content)
	details := requireSpoiler(t, parseTestHTML(t, content), "Twist", hiddenText)
	assert.Equal(t, hiddenText, testHTMLText(findTestElements(details, "p")[0]))
	assert.Equal(t, 1, strings.Count(markdown, "\n:::\n"),
		"only the generated final fence may remain an unescaped closing container line")
}

func TestBuildNote_IndentedLiteralSpoilerFencesStayInsideAnnotatedContent(t *testing.T) {
	for spaces, name := range []string{"one space", "two spaces", "three spaces"} {
		t.Run(name, func(t *testing.T) {
			indent := strings.Repeat(" ", spaces+1)
			plaintext := "hidden\n" + indent + ":::\nstill hidden\n" +
				indent + "::: spoiler Nested\nalso hidden"
			expectedMarkdown := "::: spoiler Twist\nhidden\n" + indent + "\\:::\nstill hidden\n" +
				indent + "\\::: spoiler Nested\nalso hidden\n:::"

			markdown, content := buildTestNote(t, plaintext, []any{
				testFacet(0, len(plaintext), testSpoilerFeature("Twist")),
			})

			assert.Equal(t, expectedMarkdown, markdown)
			details := requireSpoiler(t, parseTestHTML(t, content), "Twist", plaintext)
			assert.Len(t, findTestElements(details, "details"), 1,
				"literal fences must not terminate or nest the annotated spoiler")
		})
	}
}

func testSpoilerFeature(reason string) map[string]any {
	feature := map[string]any{"$type": testSpoilerFacetType}
	if reason != "" {
		feature["reason"] = reason
	}
	return feature
}

func requireSpoiler(t *testing.T, document *html.Node, expectedSummary, expectedText string) *html.Node {
	t.Helper()
	detailsElements := findTestElements(document, "details")
	require.Len(t, detailsElements, 1, "expected exactly one structural <details> element")
	details := detailsElements[0]
	assert.Empty(t, testHTMLAttribute(details, "open"), "spoilers must be closed by default")
	summaries := findTestElements(details, "summary")
	require.Len(t, summaries, 1, "a spoiler must have a reveal control")
	assert.Equal(t, expectedSummary, testHTMLText(summaries[0]))
	paragraphs := findTestElements(details, "p")
	require.Len(t, paragraphs, 1)
	assert.Equal(t, expectedText, testHTMLText(paragraphs[0]))
	return details
}
