package apobject

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Coves shows every newline in a body as a line break, while CommonMark joins
// lines separated by a single newline into one paragraph. These tests pin the
// hard breaks that keep Coves' line structure on Lemmy.

func TestBuildNote_SingleNewlinesBecomeHardLineBreaks(t *testing.T) {
	plaintext := "My general questions:\n- Do you expect privacy?\n- Would it bother you?\n\nFor context, see below."

	markdown, content := buildTestNote(t, plaintext, nil)

	assert.Equal(t,
		"My general questions: \\\n\\- Do you expect privacy? \\\n\\- Would it bother you?\n\nFor context, see below.",
		markdown)
	assert.Equal(t,
		"<p>My general questions:<br>\n- Do you expect privacy?<br>\n- Would it bother you?</p>\n<p>For context, see below.</p>\n",
		content)

	rendered := parseRenderedMarkdown(t, markdown)
	assert.Len(t, findTestElements(rendered, "br"), 2)
	assert.Len(t, findTestElements(rendered, "p"), 2)
	assert.Empty(t, findTestElements(rendered, "li"), "plaintext dashes must stay literal, not become a list")
	assert.Equal(t, testHTMLText(parseTestHTML(t, content)), testHTMLText(rendered))
}

func TestBuildNote_CRLFSingleNewlinesBecomeHardLineBreaks(t *testing.T) {
	markdown, content := buildTestNote(t, "first\r\nsecond", nil)

	assert.Equal(t, "first \\\nsecond", markdown)
	assert.Equal(t, "<p>first<br>\nsecond</p>\n", content)
}

func TestBuildNote_NoHardBreakAtParagraphOrBodyEdges(t *testing.T) {
	tests := []struct {
		name             string
		plaintext        string
		expectedMarkdown string
		expectedHTML     string
	}{
		{"blank line", "one\n\ntwo", "one\n\ntwo", "<p>one</p>\n<p>two</p>\n"},
		{"whitespace-only blank line", "one\n  \ntwo", "one\n  \ntwo", "<p>one</p>\n<p>two</p>\n"},
		{"trailing newline", "one\n", "one\n", "<p>one</p>\n"},
		{"leading newline", "\none", "\none", "<p>one</p>\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, test.plaintext, nil)
			assert.Equal(t, test.expectedMarkdown, markdown)
			assert.Equal(t, test.expectedHTML, content)
			assert.Empty(t, findTestElements(parseRenderedMarkdown(t, markdown), "br"))
		})
	}
}

func TestBuildNote_HardLineBreaksKeepEmphasisAndLinks(t *testing.T) {
	plaintext := "line one bold\nsee docs\nend"
	boldStart := strings.Index(plaintext, "bold")
	docsStart := strings.Index(plaintext, "docs")
	facets := []any{
		emphasisFacet(boldStart, boldStart+len("bold"), "bold"),
		testFacet(docsStart, docsStart+len("docs"), map[string]any{"$type": covesLinkFacetType, "uri": "https://example.com/docs"}),
	}

	markdown, content := buildTestNote(t, plaintext, facets)

	assert.Equal(t, "line one **bold** \\\nsee [docs](https://example.com/docs) \\\nend", markdown)
	assert.Equal(t,
		"<p>line one <strong>bold</strong><br>\nsee <a href=\"https://example.com/docs\">docs</a><br>\nend</p>\n",
		content)

	rendered := parseRenderedMarkdown(t, markdown)
	assert.Len(t, findTestElements(rendered, "br"), 2)
	assert.Len(t, findTestElements(rendered, "strong"), 1)
	links := findTestElements(rendered, "a")
	if assert.Len(t, links, 1) {
		assert.Equal(t, "https://example.com/docs", testHTMLAttribute(links[0], "href"))
	}
}

func TestBuildNote_HardLineBreaksInsideBlockquote(t *testing.T) {
	plaintext := "quoted one\nquoted two"

	markdown, content := buildTestNote(t, plaintext, []any{
		testFacet(0, len(plaintext), map[string]any{"$type": blockquoteFacetType}),
	})

	assert.Equal(t, "> quoted one \\\n> quoted two", markdown)
	assert.Equal(t, "<blockquote>\n<p>quoted one<br>\nquoted two</p>\n</blockquote>\n", content)
	quotes := findTestElements(parseRenderedMarkdown(t, markdown), "blockquote")
	if assert.Len(t, quotes, 1) {
		assert.Len(t, findTestElements(quotes[0], "br"), 1)
	}
}

func TestBuildNote_NoHardBreakBeforeOrAfterHeadingOrCodeBlock(t *testing.T) {
	plaintext := "intro\nTitle\nbody"

	titleStart := strings.Index(plaintext, "Title")
	markdown, content := buildTestNote(t, plaintext, []any{
		testFacet(titleStart, titleStart+len("Title"), map[string]any{"$type": headingFacetType, "level": 2}),
	})

	assert.Equal(t, "intro\n## Title\nbody", markdown)
	assert.Equal(t, "<p>intro</p>\n<h2>Title</h2>\n<p>body</p>\n", content)
	assert.Len(t, findTestElements(parseRenderedMarkdown(t, markdown), "h2"), 1)

	code := "intro\nfmt.Println()\nx := 1\nafter"
	codeStart := strings.Index(code, "fmt")
	markdown, content = buildTestNote(t, code, []any{
		testFacet(codeStart, strings.Index(code, "\nafter"), map[string]any{"$type": codeBlockFacetType}),
	})
	assert.Equal(t, "intro\n\n```\nfmt.Println()\nx := 1\n```\n\nafter", markdown)
	assert.Equal(t, "<p>intro</p>\n<pre><code>fmt.Println()\nx := 1</code></pre>\n<p>after</p>\n", content)
	assert.Len(t, findTestElements(parseRenderedMarkdown(t, markdown), "pre"), 1)
}

// markdown-it trims a paragraph with JavaScript's trim, which also strips
// no-break and other Unicode spaces, so a hard break with only such lines
// after it (or before it) in its paragraph would leave a literal backslash on
// Lemmy. Between text lines, a spacer line of them keeps its breaks.
func TestBuildNote_UnicodeWhitespaceOnlyLines(t *testing.T) {
	tests := []struct {
		name             string
		plaintext        string
		expectedMarkdown string
		expectedHTML     string
	}{
		{"trailing no-break space line", "one\n\u00a0", "one\n\u00a0", "<p>one</p>\n"},
		{"no-break space line before blank line", "one\n\u00a0\n\ntwo", "one\n\u00a0\n\ntwo", "<p>one</p>\n<p>two</p>\n"},
		{"leading byte order mark line", "\ufeff\none", "\ufeff\none", "<p>\ufeff\none</p>\n"},
		{"no-break space spacer line between text", "one\n\u00a0\ntwo", "one \\\n\u00a0 \\\ntwo", "<p>one<br>\n\u00a0<br>\ntwo</p>\n"},
		{"two spacer lines between text", "one\n\u00a0\n\u00a0\ntwo", "one \\\n\u00a0 \\\n\u00a0 \\\ntwo", "<p>one<br>\n\u00a0<br>\n\u00a0<br>\ntwo</p>\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNote(t, test.plaintext, nil)
			assert.Equal(t, test.expectedMarkdown, markdown)
			assert.Equal(t, test.expectedHTML, content)
		})
	}
}

func TestBuildNote_HardLineBreakInsideEmphasis(t *testing.T) {
	plaintext := "say a\nb now"
	start := strings.Index(plaintext, "a\nb")

	markdown, content := buildTestNote(t, plaintext, []any{emphasisFacet(start, start+len("a\nb"), "bold")})

	assert.Equal(t, "say **a \\\nb** now", markdown)
	assert.Equal(t, "<p>say <strong>a<br>\nb</strong> now</p>\n", content)
	strong := findTestElements(parseRenderedMarkdown(t, markdown), "strong")
	if assert.Len(t, strong, 1) {
		assert.Len(t, findTestElements(strong[0], "br"), 1)
	}
}

// lemmy-ui's markdown-it linkify accepts a backslash inside a URL path, so a
// bare URL ending a line must be separated from the hard break's backslash.
func TestBuildNote_HardLineBreakAfterBareURLKeepsURLIntact(t *testing.T) {
	markdown, content := buildTestNote(t, "read https://example.com/article\nnext", nil)

	assert.Equal(t, "read https://example.com/article \\\nnext", markdown)
	assert.Equal(t, "<p>read https://example.com/article<br>\nnext</p>\n", content)
}
