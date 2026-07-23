package materialize

import (
	"context"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
)

// --- test helpers over the wire-form facet list ---

func facetIndex(t *testing.T, f any) (int, int) {
	t.Helper()
	m, ok := f.(map[string]any)
	require.True(t, ok, "facet must be a map")
	idx, ok := m["index"].(map[string]any)
	require.True(t, ok, "facet must carry an index")
	return idx["byteStart"].(int), idx["byteEnd"].(int)
}

func facetFeatures(t *testing.T, f any) []map[string]any {
	t.Helper()
	features, ok := f.(map[string]any)["features"].([]any)
	require.True(t, ok, "facet must carry features")
	out := make([]map[string]any, len(features))
	for i, feat := range features {
		out[i] = feat.(map[string]any)
	}
	return out
}

// findFacets returns every facet carrying the given feature type suffix
// ("heading", "blockquote", ...).
func findFacets(t *testing.T, facets []any, suffix string) []any {
	t.Helper()
	var out []any
	for _, f := range facets {
		for _, feat := range facetFeatures(t, f) {
			if feat["$type"] == facetTypePrefix+suffix {
				out = append(out, f)
				break
			}
		}
	}
	return out
}

func requireOneFacet(t *testing.T, facets []any, suffix string) any {
	t.Helper()
	found := findFacets(t, facets, suffix)
	require.Len(t, found, 1, "want exactly one %s facet", suffix)
	return found[0]
}

func facetText(t *testing.T, content string, f any) string {
	t.Helper()
	start, end := facetIndex(t, f)
	require.GreaterOrEqual(t, start, 0)
	require.Less(t, start, end, "facet range must be non-empty")
	require.LessOrEqual(t, end, len(content), "facet must not slice outside content")
	return content[start:end]
}

// requireWholeLines asserts the block-facet convention: the range starts at a
// line start, ends at a line end, and excludes the trailing newline.
func requireWholeLines(t *testing.T, content string, f any) {
	t.Helper()
	start, end := facetIndex(t, f)
	if start > 0 {
		assert.Equal(t, byte('\n'), content[start-1], "block facet must start at a line start")
	}
	if end < len(content) {
		assert.Equal(t, byte('\n'), content[end], "block facet must end at a line end")
	}
	assert.NotEqual(t, byte('\n'), content[end-1], "block facet must exclude the trailing newline")
}

// --- conversion tests ---

// TestRichTextBlockConventions mirrors Coves' TestBlockFacetConventions: a
// bridged-Lemmy shape (heading, two-level nested quote, fenced code) must
// produce whole-line block ranges, DISJOINT quote ranges with increasing
// level, and marker-free canonical text.
func TestRichTextBlockConventions(t *testing.T) {
	md := "## The Button\n\n> They said\n>\n> > Do not press\n\nUse:\n\n```go\nfmt.Println(\"hi\")\n```"
	content, facets := bridgedRichText(md, 10000, 100000)

	assert.Equal(t, "The Button\n\nThey said\n\nDo not press\n\nUse:\n\nfmt.Println(\"hi\")", content)

	heading := requireOneFacet(t, facets, "heading")
	assert.Equal(t, "The Button", facetText(t, content, heading))
	assert.Equal(t, 2, facetFeatures(t, heading)[0]["level"])
	requireWholeLines(t, content, heading)

	quotes := findFacets(t, facets, "blockquote")
	require.Len(t, quotes, 2)
	assert.Equal(t, "They said", facetText(t, content, quotes[0]))
	assert.Equal(t, 1, facetFeatures(t, quotes[0])[0]["level"])
	assert.Equal(t, "Do not press", facetText(t, content, quotes[1]))
	assert.Equal(t, 2, facetFeatures(t, quotes[1])[0]["level"])
	for _, q := range quotes {
		requireWholeLines(t, content, q)
	}
	// Disjoint, not containment: level 1 ends before level 2 starts.
	_, q1End := facetIndex(t, quotes[0])
	q2Start, _ := facetIndex(t, quotes[1])
	assert.LessOrEqual(t, q1End, q2Start, "nested quotes must be disjoint ranges")

	code := requireOneFacet(t, facets, "codeBlock")
	assert.Equal(t, "fmt.Println(\"hi\")", facetText(t, content, code))
	assert.Equal(t, "go", facetFeatures(t, code)[0]["language"])
	requireWholeLines(t, content, code)
}

func TestRichTextInlineFacets(t *testing.T) {
	// Multibyte text before the annotations pins byte (not rune) offsets.
	md := "héllo **bold** *ital* ~~gone~~ `x := 1` [text](https://x.example/p)"
	content, facets := bridgedRichText(md, 10000, 100000)

	assert.Equal(t, "héllo bold ital gone x := 1 text", content)
	assert.Equal(t, "bold", facetText(t, content, requireOneFacet(t, facets, "bold")))
	assert.Equal(t, "ital", facetText(t, content, requireOneFacet(t, facets, "italic")))
	assert.Equal(t, "gone", facetText(t, content, requireOneFacet(t, facets, "strikethrough")))
	assert.Equal(t, "x := 1", facetText(t, content, requireOneFacet(t, facets, "code")))

	link := requireOneFacet(t, facets, "link")
	assert.Equal(t, "text", facetText(t, content, link))
	assert.Equal(t, "https://x.example/p", facetFeatures(t, link)[0]["uri"])
}

func TestRichTextBoldItalicShareOneFacet(t *testing.T) {
	content, facets := bridgedRichText("***both***", 10000, 100000)
	assert.Equal(t, "both", content)
	require.Len(t, facets, 1, "identical ranges must merge into one facet")
	types := []string{}
	for _, feat := range facetFeatures(t, facets[0]) {
		types = append(types, feat["$type"].(string))
	}
	assert.ElementsMatch(t,
		[]string{facetTypePrefix + "bold", facetTypePrefix + "italic"}, types)
}

// TestRichTextUnsafeLinkSchemes: non-http(s) destinations keep their text but
// must not become link facets — the facet uri lands as a clickable in Coves
// clients, and format:"uri" alone would let javascript: through.
func TestRichTextUnsafeLinkSchemes(t *testing.T) {
	content, facets := bridgedRichText(
		"[click](javascript:alert(1)) and [rel](/local/path) and <user@x.example>", 10000, 100000)
	assert.Equal(t, "click and rel and user@x.example", content)
	assert.Empty(t, findFacets(t, facets, "link"),
		"javascript:, relative, and mailto destinations must not produce link facets")
}

func TestRichTextEmptyLinkTextFallsBackToURL(t *testing.T) {
	content, facets := bridgedRichText("[](https://x.example/only)", 10000, 100000)
	assert.Equal(t, "https://x.example/only", content)
	link := requireOneFacet(t, facets, "link")
	assert.Equal(t, content, facetText(t, content, link))
}

// TestRichTextEmptyLinkMidDocument pins the empty-render detection against a
// PENDING separator: mid-document the annotated node's start is taken before
// the separator flushes, so a b.Len()-based check would never fire and the
// whole paragraph would vanish.
func TestRichTextEmptyLinkMidDocument(t *testing.T) {
	content, facets := bridgedRichText("intro\n\n[](https://x.example/p)", 10000, 100000)
	assert.Equal(t, "intro\n\nhttps://x.example/p", content)
	link := requireOneFacet(t, facets, "link")
	assert.Equal(t, "https://x.example/p", facetText(t, content, link))

	content, facets = bridgedRichText("intro\n\n![](https://pictrs.example/x.png)", 10000, 100000)
	assert.Equal(t, "intro\n\nimage", content)
	link = requireOneFacet(t, facets, "link")
	assert.Equal(t, "image", facetText(t, content, link))
	assert.Equal(t, "https://pictrs.example/x.png", facetFeatures(t, link)[0]["uri"])
}

// TestRichTextUnescapesEscapesAndEntities: goldmark resolves backslash
// escapes and entity references at RENDER time, so ast.Text segments carry
// them verbatim — and the HTML→markdown path routinely escapes prose
// ("2 * 3" → "2 \* 3"). The stored plaintext must show what the author saw.
func TestRichTextUnescapesEscapesAndEntities(t *testing.T) {
	content, facets := bridgedRichText(`2 \* 3 is \*not bold\*`, 10000, 100000)
	assert.Equal(t, "2 * 3 is *not bold*", content)
	assert.Empty(t, facets, "escaped markers must not produce facets")

	content, _ = bridgedRichText("AT&amp;T &#x2764; ok", 10000, 100000)
	assert.Equal(t, "AT&T ❤ ok", content)

	// Destinations carry the same source-level escapes; the stored uri must
	// be the resolved one, not the escaped bytes.
	content, facets = bridgedRichText(`[x](https://a.example/p\(q\)?a=1&amp;b=2)`, 10000, 100000)
	assert.Equal(t, "x", content)
	link := requireOneFacet(t, facets, "link")
	assert.Equal(t, "https://a.example/p(q)?a=1&b=2", facetFeatures(t, link)[0]["uri"])

	// Code spans keep their bytes raw — escapes are content there.
	content, _ = bridgedRichText("`\\*raw\\*`", 10000, 100000)
	assert.Equal(t, `\*raw\*`, content)
}

func TestRichTextAutoLink(t *testing.T) {
	content, facets := bridgedRichText("see https://lemmy.zip/post/1 now", 10000, 100000)
	assert.Equal(t, "see https://lemmy.zip/post/1 now", content)
	link := requireOneFacet(t, facets, "link")
	assert.Equal(t, "https://lemmy.zip/post/1", facetText(t, content, link))
	assert.Equal(t, "https://lemmy.zip/post/1", facetFeatures(t, link)[0]["uri"])
}

func TestRichTextImageDegradesToLinkedAltText(t *testing.T) {
	content, facets := bridgedRichText(
		"![a cat](https://img.example/cat.png) and ![](https://img.example/dog.png)", 10000, 100000)
	assert.Equal(t, "a cat and image", content)
	links := findFacets(t, facets, "link")
	require.Len(t, links, 2)
	assert.Equal(t, "a cat", facetText(t, content, links[0]))
	assert.Equal(t, "https://img.example/cat.png", facetFeatures(t, links[0])[0]["uri"])
	assert.Equal(t, "image", facetText(t, content, links[1]))
}

// TestRichTextQuoteDepthClamps: per the lexicon, "Writers must clamp source
// nesting deeper than 6 to level 6".
func TestRichTextQuoteDepthClamps(t *testing.T) {
	md := "> 1\n> > 2\n> > > 3\n> > > > 4\n> > > > > 5\n> > > > > > 6\n> > > > > > > 7\n> > > > > > > > 8"
	content, facets := bridgedRichText(md, 10000, 100000)
	assert.Equal(t, "1\n\n2\n\n3\n\n4\n\n5\n\n6\n\n7\n\n8", content)

	quotes := findFacets(t, facets, "blockquote")
	require.Len(t, quotes, 8)
	for i, q := range quotes {
		want := min(i+1, 6)
		assert.Equal(t, want, facetFeatures(t, q)[0]["level"], "quote %d", i+1)
	}
}

// TestRichTextQuoteResumesAfterNested: a quote whose level-1 text continues
// after a nested quote yields TWO disjoint level-1 facets around the level-2
// range (adjacent same-level facets render as separate blocks by convention;
// resumed text must not be swallowed into either neighbor).
func TestRichTextQuoteResumesAfterNested(t *testing.T) {
	md := "> before\n>\n> > inner\n>\n> after"
	content, facets := bridgedRichText(md, 10000, 100000)
	assert.Equal(t, "before\n\ninner\n\nafter", content)

	quotes := findFacets(t, facets, "blockquote")
	require.Len(t, quotes, 3)
	assert.Equal(t, "before", facetText(t, content, quotes[0]))
	assert.Equal(t, 1, facetFeatures(t, quotes[0])[0]["level"])
	assert.Equal(t, "inner", facetText(t, content, quotes[1]))
	assert.Equal(t, 2, facetFeatures(t, quotes[1])[0]["level"])
	assert.Equal(t, "after", facetText(t, content, quotes[2]))
	assert.Equal(t, 1, facetFeatures(t, quotes[2])[0]["level"])
}

// TestRichTextQuoteCoversConsecutiveBlocks: consecutive same-depth quoted
// paragraphs (and a heading among them) sit under ONE blockquote facet, with
// the heading annotated by containment — cross-type nesting is allowed.
func TestRichTextQuoteCoversConsecutiveBlocks(t *testing.T) {
	md := "> ## Title\n>\n> first para\n>\n> second para"
	content, facets := bridgedRichText(md, 10000, 100000)
	assert.Equal(t, "Title\n\nfirst para\n\nsecond para", content)

	quote := requireOneFacet(t, facets, "blockquote")
	assert.Equal(t, content, facetText(t, content, quote),
		"one facet must cover the whole same-depth run")
	heading := requireOneFacet(t, facets, "heading")
	assert.Equal(t, "Title", facetText(t, content, heading))
}

// TestRichTextQuoteThroughListStaysDisjoint: a quote nested through a list
// inside a quote is invisible to renderQuote's direct-child scan — the outer
// run's range would contain the inner quote's, which the lexicon forbids.
// The post-walk split must leave every blockquote pair disjoint and every
// range on whole lines.
func TestRichTextQuoteThroughListStaysDisjoint(t *testing.T) {
	md := "> before\n> - item\n>   > deep\n> after"
	content, facets := bridgedRichText(md, 10000, 100000)
	assert.Equal(t, "before\n\n• item\n\ndeep\nafter", content)

	quotes := findFacets(t, facets, "blockquote")
	require.Len(t, quotes, 2)
	assert.Equal(t, "before\n\n• item", facetText(t, content, quotes[0]))
	assert.Equal(t, 1, facetFeatures(t, quotes[0])[0]["level"])
	// "after" lazily continues the inner quote's paragraph (CommonMark), so
	// it belongs to the level-2 range.
	assert.Equal(t, "deep\nafter", facetText(t, content, quotes[1]))
	assert.Equal(t, 2, facetFeatures(t, quotes[1])[0]["level"])
	for i, a := range quotes {
		requireWholeLines(t, content, a)
		for j, b := range quotes {
			if i == j {
				continue
			}
			as, ae := facetIndex(t, a)
			bs, be := facetIndex(t, b)
			assert.False(t, as <= bs && be <= ae,
				"blockquote %d must not contain blockquote %d", i, j)
		}
	}
}

// TestRichTextBlockFacetsInListItems: "- # heading" writes the "• " marker
// and the heading on the same line, so the block facet must swallow the
// marker to stay whole-line — readers render "• title" as a heading line,
// which is the intended degradation.
func TestRichTextBlockFacetsInListItems(t *testing.T) {
	content, facets := bridgedRichText("- # title", 10000, 100000)
	assert.Equal(t, "• title", content)
	heading := requireOneFacet(t, facets, "heading")
	assert.Equal(t, "• title", facetText(t, content, heading))
	requireWholeLines(t, content, heading)

	content, facets = bridgedRichText("- ```\n  code\n  ```", 10000, 100000)
	assert.Equal(t, "• code", content)
	code := requireOneFacet(t, facets, "codeBlock")
	assert.Equal(t, "• code", facetText(t, content, code))
	requireWholeLines(t, content, code)
}

func TestRichTextLists(t *testing.T) {
	md := "- one\n- two\n  - sub\n\n3. third\n4. fourth"
	content, facets := bridgedRichText(md, 10000, 100000)
	assert.Equal(t, "• one\n• two\n  • sub\n\n3. third\n4. fourth", content,
		"list markers are normalized into the text (ordinals keep the source start)")
	assert.Empty(t, facets, "lists carry no facet by design")
}

func TestRichTextTableDegradesToCodeBlock(t *testing.T) {
	md := "| Name | Age |\n|------|-----|\n| **Bob** | 42 |"
	content, facets := bridgedRichText(md, 10000, 100000)
	assert.Equal(t, "Name | Age\n--- | ---\nBob | 42", content)

	require.Len(t, facets, 1, "a table is exactly one codeBlock facet, no inline facets inside")
	code := requireOneFacet(t, facets, "codeBlock")
	assert.Equal(t, content, facetText(t, content, code))
	requireWholeLines(t, content, code)
}

func TestRichTextHeadingIsSingleLine(t *testing.T) {
	// Setext headings may span source lines; the facet must cover one line.
	content, facets := bridgedRichText("Two\nLines\n===", 10000, 100000)
	assert.Equal(t, "Two Lines", content)
	heading := requireOneFacet(t, facets, "heading")
	assert.Equal(t, "Two Lines", facetText(t, content, heading))
	assert.Equal(t, 1, facetFeatures(t, heading)[0]["level"])
}

func TestRichTextCodeLanguageOverCapDropped(t *testing.T) {
	lang := strings.Repeat("x", maxCodeLanguageBytes+1)
	content, facets := bridgedRichText("```"+lang+"\ncode\n```", 10000, 100000)
	assert.Equal(t, "code", content)
	code := requireOneFacet(t, facets, "codeBlock")
	_, hasLang := facetFeatures(t, code)[0]["language"]
	assert.False(t, hasLang,
		"an over-cap language hint is dropped from the facet (Coves would drop the whole facet)")
}

func TestRichTextHardBreaksAndRules(t *testing.T) {
	content, _ := bridgedRichText("one  \ntwo<br>three\n\n---\n\nfour", 10000, 100000)
	assert.Equal(t, "one\ntwo\nthree\n\n———\n\nfour", content)
}

// TestRichTextPlainTextPassesThrough: a body with no markup at all converts
// to itself with no facets — the overwhelmingly common case.
func TestRichTextPlainTextPassesThrough(t *testing.T) {
	content, facets := bridgedRichText("just words, 5 > 3, nothing else", 10000, 100000)
	assert.Equal(t, "just words, 5 > 3, nothing else", content)
	assert.Nil(t, facets)
}

// TestRichTextHTMLOnlyBodyFallsBack: markup that renders to no visible text
// falls back to the TAG-STRIPPED markdown — live tags must never be stored
// in a field clients treat as plaintext — so the fallback can come out
// empty, and callers treat "" as no content.
func TestRichTextHTMLOnlyBodyFallsBack(t *testing.T) {
	content, facets := bridgedRichText("<details></details>", 10000, 100000)
	assert.Equal(t, "", content)
	assert.Nil(t, facets)

	content, facets = bridgedRichText("<details>visible</details>", 10000, 100000)
	assert.Equal(t, "visible", content)
	assert.Nil(t, facets)

	content, _ = bridgedRichText("<img onerror=alert(1) src=x>", 10000, 100000)
	assert.Equal(t, "", content, "a lone active tag must not survive the fallback")
}

// TestRichTextRawHTMLAnchorsAndBreaks: inline <a href> arrives as
// RawHTML/Text/RawHTML siblings — the text survives on its own, but the URL
// must become a link facet (the old markdown passthrough preserved it, so
// dropping it silently would be a regression). <br> variants beyond the bare
// tag must still break the line, and unsafe anchor schemes get no facet.
func TestRichTextRawHTMLAnchorsAndBreaks(t *testing.T) {
	content, facets := bridgedRichText(
		`before <a href="https://keep.example/x">linked words</a> after`, 10000, 100000)
	assert.Equal(t, "before linked words after", content)
	link := requireOneFacet(t, facets, "link")
	assert.Equal(t, "linked words", facetText(t, content, link))
	assert.Equal(t, "https://keep.example/x", facetFeatures(t, link)[0]["uri"])

	content, _ = bridgedRichText(`two<br clear="all">three`, 10000, 100000)
	assert.Equal(t, "two\nthree", content)

	content, facets = bridgedRichText(`<a href="javascript:alert(1)">x</a>`, 10000, 100000)
	assert.Equal(t, "x", content)
	assert.Empty(t, findFacets(t, facets, "link"),
		"an unsafe anchor scheme keeps the text but gets no facet")
}

// TestRichTextCRLFCodeBlock: \r\n line endings must never leak into records.
func TestRichTextCRLFCodeBlock(t *testing.T) {
	content, facets := bridgedRichText("```\nline1\r\nline2\r\n```", 10000, 100000)
	assert.Equal(t, "line1\nline2", content)
	assert.NotContains(t, content, "\r")
	code := requireOneFacet(t, facets, "codeBlock")
	requireWholeLines(t, content, code)
}

func TestRichTextHTMLBlockKeepsVisibleText(t *testing.T) {
	content, _ := bridgedRichText("<div>kept text</div>\n\nafter", 10000, 100000)
	assert.Contains(t, content, "kept text")
	assert.Contains(t, content, "after")
	assert.NotContains(t, content, "<div>")
}

// TestRichTextTruncationClampsFacets: facets past the truncation cut are
// dropped, straddlers are clamped to the surviving prefix — a range slicing
// outside the content would be dropped whole by Coves' ingest.
func TestRichTextTruncationClampsFacets(t *testing.T) {
	md := "aaaa **bbbb** cccc **dddd**"
	// A grapheme cap of 12 keeps 11 clusters plus the ellipsis.
	content, facets := bridgedRichText(md, 12, 100000)
	assert.Equal(t, "aaaa bbbb c…", content)

	bolds := findFacets(t, facets, "bold")
	require.Len(t, bolds, 1, "the facet past the cut must be dropped")
	assert.Equal(t, "bbbb", facetText(t, content, bolds[0]))

	// A facet straddling the cut clamps to the kept prefix.
	content, facets = bridgedRichText("aaaa **bbbb**", 8, 100000)
	assert.Equal(t, "aaaa bb…", content)
	bolds = findFacets(t, facets, "bold")
	require.Len(t, bolds, 1)
	assert.Equal(t, "bb", facetText(t, content, bolds[0]))
}

// TestRichTextTruncationMidRuneClamp: the appended ellipsis (E2 80 A6)
// shares lead bytes with U+2000–U+2FFF punctuation (’ — • ™), so the byte
// compare against the original can overcount 1–2 bytes into the ellipsis —
// a straddling facet would clamp to a byteEnd that splits a UTF-8 sequence,
// and Coves' ingest drops such facets whole.
func TestRichTextTruncationMidRuneClamp(t *testing.T) {
	for _, punct := range []string{"’", "™"} {
		// The grapheme cap cuts right where the next original char is punct.
		content, facets := bridgedRichText("aaaa **bb"+punct+"x** more", 8, 100000)
		assert.Equal(t, "aaaa bb…", content)
		require.True(t, utf8.ValidString(content))
		for _, f := range facets {
			start, end := facetIndex(t, f)
			assert.True(t, utf8.ValidString(content[start:end]),
				"clamped facet must not split a UTF-8 sequence")
		}
		bolds := findFacets(t, facets, "bold")
		require.Len(t, bolds, 1)
		assert.Equal(t, "bb", facetText(t, content, bolds[0]))
	}
}

// TestRichTextByteCapTruncation exercises the BYTE phase of truncateText:
// it cuts without trimming, so the surviving prefix can end in '\n' — the
// bridge must trim it, or a clamped block facet would include a trailing
// newline the convention forbids.
func TestRichTextByteCapTruncation(t *testing.T) {
	md := "line one\n\n> quoted line\n\nmore text here"
	content, facets := bridgedRichText(md, 100000, 23)
	assert.Equal(t, "line one\n\nquoted line", content)
	assert.False(t, strings.HasSuffix(content, "\n"))
	for _, f := range facets {
		facetText(t, content, f) // asserts bounds against the final string
	}
	quote := requireOneFacet(t, facets, "blockquote")
	assert.Equal(t, "quoted line", facetText(t, content, quote))
	requireWholeLines(t, content, quote)
}

// TestRichTextDeepQuoteNestingBounded: recursion is depth-guarded, so a
// nesting bomb converts quickly and without panic. Below the guard the
// emitted levels still clamp at the lexicon's 6.
func TestRichTextDeepQuoteNestingBounded(t *testing.T) {
	content, facets := bridgedRichText(strings.Repeat("> ", 60)+"deep", 10000, 100000)
	assert.Equal(t, "deep", content)
	quote := requireOneFacet(t, facets, "blockquote")
	assert.Equal(t, maxQuoteLevel, facetFeatures(t, quote)[0]["level"])

	// 500 levels sails past maxRenderDepth: whatever comes out, it must come
	// out fast, panic-free, and with every facet in bounds.
	content, facets = bridgedRichText(strings.Repeat("> ", 500)+"deep", 10000, 100000)
	for _, f := range facets {
		facetText(t, content, f)
	}
}

// TestRichTextOverCapInputTruncated: input beyond maxBytes+parseSlack is cut
// before parsing (bounding adversarial parse work at the storage cap), on a
// rune boundary.
func TestRichTextOverCapInputTruncated(t *testing.T) {
	md := strings.Repeat("wörd ", 3000) // 18000 bytes, far past 1000+parseSlack
	content, _ := bridgedRichText(md, 100000, 1000)
	assert.LessOrEqual(t, len(content), 1000)
	assert.True(t, utf8.ValidString(content))
	assert.True(t, strings.HasPrefix(content, "wörd wörd"))
}

func TestRichTextFacetCap(t *testing.T) {
	var sb strings.Builder
	for range 250 {
		sb.WriteString("**b** ")
	}
	content, facets := bridgedRichText(sb.String(), 10000, 100000)
	assert.Len(t, facets, maxFacets, "facet list must cap at the lexicon's 200")
	// The kept facets are the EARLIEST ones: the first facet sits at the
	// first span's offset and starts strictly increase from there.
	firstStart, _ := facetIndex(t, facets[0])
	assert.Equal(t, 0, firstStart)
	prev := -1
	for _, f := range facets {
		start, _ := facetIndex(t, f)
		assert.Greater(t, start, prev, "facet starts must strictly increase")
		prev = start
		assert.Equal(t, "b", facetText(t, content, f))
	}
}

// TestRichTextDuplicateFeatureDeduped: ****x**** parses as nested bolds on
// one range; a repeated $type on a merged facet adds nothing.
func TestRichTextDuplicateFeatureDeduped(t *testing.T) {
	content, facets := bridgedRichText("****x****", 10000, 100000)
	assert.Equal(t, "x", content)
	require.Len(t, facets, 1)
	features := facetFeatures(t, facets[0])
	require.Len(t, features, 1, "identical features on one range must dedupe")
	assert.Equal(t, facetTypePrefix+"bold", features[0]["$type"])
}

func TestRichTextEmptyAndBlank(t *testing.T) {
	content, facets := bridgedRichText("", 10000, 100000)
	assert.Equal(t, "", content)
	assert.Nil(t, facets)
}

// TestMaterializePostStoresRichText pins the record-level wiring end to end:
// a page whose source markdown carries markup lands in the repo as stripped
// plaintext with a facets array that survives the storage round trip.
func TestMaterializePostStoresRichText(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	page := loadFixtureObject(t, "page_lemmy_world.json")
	page.Source = &ap.Source{
		Content:   "## Season 4\n\n> quoted\n\n**bold** [link](https://x.example/)",
		MediaType: "text/markdown",
	}

	_, err := h.m.MaterializePost(context.Background(), page)
	require.NoError(t, err)

	record := h.recordFor(t, pageID)
	assert.Equal(t, "Season 4\n\nquoted\n\nbold link", record["content"])
	facets, ok := record["facets"].([]any)
	require.True(t, ok, "record must carry facets")
	var types []string
	for _, f := range facets {
		for _, feat := range f.(map[string]any)["features"].([]any) {
			types = append(types, feat.(map[string]any)["$type"].(string))
		}
	}
	assert.ElementsMatch(t, []string{
		facetTypePrefix + "heading",
		facetTypePrefix + "blockquote",
		facetTypePrefix + "bold",
		facetTypePrefix + "link",
	}, types)
}

// TestRichTextKitchenSinkInBounds is the property backstop: every facet on a
// messy real-world-ish document slices inside the content, and block facets
// sit on whole lines.
func TestRichTextKitchenSinkInBounds(t *testing.T) {
	md := "cross-posted from: https://lemmy.zip/post/68248986\n\n" +
		"> cross-posted from: https://lemmy.zip/post/68248912\n>\n" +
		"> > ## The Button is back for Season 4! 🎉\n> >\n" +
		"> > Sign up at [thebutton.social](https://thebutton.social/) for **bonuses**.\n> >\n" +
		"> > 1. press\n> > 2. wait\n\n" +
		"| a | b |\n|---|---|\n| é | 🚀 |\n\n" +
		"```rust\nfn main() {}\n```\n\n- • weird\n- \\*escaped\\*\n\n---\n\ndone `x` ~~y~~"
	content, facets := bridgedRichText(md, 10000, 100000)
	require.NotEmpty(t, facets)
	for _, f := range facets {
		facetText(t, content, f) // asserts bounds
		for _, feat := range facetFeatures(t, f) {
			switch feat["$type"] {
			case facetTypePrefix + "blockquote", facetTypePrefix + "heading", facetTypePrefix + "codeBlock":
				requireWholeLines(t, content, f)
			}
		}
	}
}

// TestMaterializeCommentStoresRichText mirrors the post wiring test through
// the comment path: markdown in a Note's source lands in the comment record
// as stripped plaintext plus facets.
func TestMaterializeCommentStoresRichText(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	h.serveObject("/u/alice", person("https://lemmy.world/u/alice", "alice", nil))
	commentID := "https://lemmy.world/comment/9001"
	c1 := note(commentID, "https://lemmy.world/u/alice",
		pageID, "> quoted\n\n**bold** reply", "2026-07-07T04:00:00.000000Z")
	h.serveObject("/comment/9001", c1)

	_, err := h.m.MaterializeComment(context.Background(), objectFromMap(t, c1))
	require.NoError(t, err)

	record := h.recordFor(t, commentID)
	assert.Equal(t, "quoted\n\nbold reply", record["content"])
	facets, ok := record["facets"].([]any)
	require.True(t, ok, "comment record must carry facets")
	var types []string
	for _, f := range facets {
		for _, feat := range f.(map[string]any)["features"].([]any) {
			types = append(types, feat.(map[string]any)["$type"].(string))
		}
	}
	assert.ElementsMatch(t, []string{
		facetTypePrefix + "blockquote",
		facetTypePrefix + "bold",
	}, types)
}
