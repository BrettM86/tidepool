package materialize

import (
	"bytes"
	"html"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/extension"
	east "github.com/yuin/goldmark/extension/ast"
	"github.com/yuin/goldmark/parser"
	"github.com/yuin/goldmark/text"
	"github.com/yuin/goldmark/util"
)

// Markdown→richtext strategy: Coves' lexicons store canonical PLAINTEXT with
// advisory social.coves.richtext.facet annotations — not markdown (the
// "content supports rich text via facets" model every Coves client renders).
// So the bridge parses the Lemmy markdown (CommonMark + GFM strikethrough,
// tables, and linkify — Lemmy's own dialect) and emits stripped text plus
// facets:
//
//   - heading/blockquote/codeBlock/spoiler ranges span whole lines, excluding
//     the trailing newline; nested quotes become DISJOINT ranges with
//     increasing level (quote-in-quote containment is forbidden; cross-type
//     containment is fine), clamped at level 6
//   - Lemmy's non-CommonMark `::: spoiler Title` containers parse via
//     spoiler.go and become a spoiler facet whose reason is the title
//   - inline bold/italic/strikethrough/code/link map directly
//   - lists have no facet by design: markers are normalized into the text
//     ("• ", "1. ") and degrade as plain lines
//   - tables degrade to delimited plaintext lines under a codeBlock facet
//
// The text must stay readable with every facet ignored (the lexicon's
// open-union rule) — that is why markers are stripped rather than kept.
//
// The caps mirror Coves' validator (internal/core/richtext): its Jetstream
// ingest DROPS any facet that violates them, so the bridge self-enforces
// rather than silently losing annotations downstream.
const (
	facetTypePrefix      = "social.coves.richtext.facet#"
	maxFacets            = 200
	maxFeaturesPerFacet  = 20
	maxCodeLanguageBytes = 40
	maxQuoteLevel        = 6
	maxHeadingLevel      = 6

	// The lexicon's caps on a spoiler facet's optional `reason`.
	maxSpoilerReasonGraphemes = 32
	maxSpoilerReasonBytes     = 128

	// parseSlack is how much markdown beyond the byte cap still gets parsed:
	// markup renders shorter than its source (markers, link URLs), so a small
	// slack keeps legitimate near-cap bodies intact while bounding
	// attacker-controlled parse work at the storage cap rather than the 5 MiB
	// transport cap (goldmark's cost is superlinear on adversarial nesting).
	parseSlack = 4096
	// maxRenderDepth caps AST render recursion: content nested this deep is
	// adversarial by construction, so deeper nodes render nothing rather than
	// risk stack exhaustion on unauthenticated federation input.
	maxRenderDepth = 200

	// maxEntityLen bounds the window scanned for an entity's closing ';'
	// (the longest HTML5 named reference is 33 bytes).
	maxEntityLen = 48
)

var richTextMarkdown = goldmark.New(
	goldmark.WithExtensions(extension.Strikethrough, extension.Table, extension.Linkify),
	goldmark.WithParserOptions(parser.WithBlockParsers(
		util.Prioritized(spoilerBlockParser{}, spoilerParserPriority),
	)),
)

// bareURLRe finds an http(s) URL in already-rendered plaintext. The leading
// word boundary keeps "xhttps://…" from matching; the tail is trimmed by
// trimURLTail, which the character class deliberately leaves to do its job.
var bareURLRe = regexp.MustCompile(`(?i)\bhttps?://[^\s<>]+`)

// brTagRe matches every <br> variant (<br>, <br/>, <br clear=all>):
// federated HTML uses them all, and a missed one concatenates words.
var brTagRe = regexp.MustCompile(`(?i)^<br\b[^>]*>$`)

// anchorOpenRe/anchorCloseRe implement minimal raw-HTML <a href> support: a
// single-level, quoted-href anchor whose open and close tags arrive in the
// same inline run. The inner text arrives as sibling Text nodes and survives
// regardless; without this the URL an HTML-writing federated app put on its
// link would be silently destroyed. Anything fancier (nested anchors,
// unquoted hrefs) deliberately degrades to plain text.
var (
	anchorOpenRe  = regexp.MustCompile(`(?is)^<a\s[^>]*?href\s*=\s*(?:"([^"]*)"|'([^']*)')[^>]*>$`)
	anchorCloseRe = regexp.MustCompile(`(?i)^</a\s*>$`)
)

// facetSpan is one feature over a byte range of the plaintext under
// construction. Spans with identical ranges merge into one wire facet at
// finalization (***text*** yields bold+italic on the same range).
type facetSpan struct {
	start, end int
	feature    map[string]any
}

// richTextConverter walks the goldmark AST accumulating plaintext and facet
// spans. Separators between blocks are written LAZILY (requestSep/writeText)
// so that a block which renders to nothing never leaves a dangling blank
// line, and nextPos always names the byte where real content will start —
// which is what keeps block facet ranges anchored to line starts.
type richTextConverter struct {
	source []byte
	b      strings.Builder
	sep    string // pending separator, flushed before the next content write
	facets []facetSpan

	// lineStart is the byte offset where the buffer's current line began —
	// what re-anchors a block facet that starts mid-line (after a list-item
	// marker) back to its line start.
	lineStart int
	// depth counts render recursion; see enterDepth.
	depth int

	// htmlLinkStart/htmlLinkURI carry the pending raw-HTML <a> state between
	// its open and close tags (renderRawHTML).
	htmlLinkStart int
	htmlLinkURI   string

	facetsOff  bool // inside a table: cell text renders without facets
	singleLine bool // inside a heading: line breaks render as spaces
}

// --- feature constructors ---
//
// Each constructor owns its feature's lexicon constraints, so no call site
// can emit a value Coves' ingest sanitizer would reject.

func featureOf(name string) map[string]any {
	return map[string]any{"$type": facetTypePrefix + name}
}

func headingFeature(level int) map[string]any {
	f := featureOf("heading")
	f["level"] = min(max(level, 1), maxHeadingLevel)
	return f
}

// quoteFeature clamps source nesting deeper than the lexicon's 6 to level 6
// (deeper quotes flatten into adjacent level-6 blocks, which readers render
// as separate quotes).
func quoteFeature(level int) map[string]any {
	f := featureOf("blockquote")
	f["level"] = min(level, maxQuoteLevel)
	return f
}

// codeBlockFeature drops (not truncates) a language hint over the lexicon's
// 40-byte cap: a clipped hint is garbage, and Coves' sanitizer would drop
// the whole facet.
func codeBlockFeature(language string) map[string]any {
	f := featureOf("codeBlock")
	if language != "" && len(language) <= maxCodeLanguageBytes {
		f["language"] = language
	}
	return f
}

// spoilerFeature carries the container's title as the lexicon's optional
// `reason`. Unlike codeBlockFeature's language, an over-cap reason is
// TRUNCATED rather than dropped: a clipped label still names what is hidden,
// where a clipped language tag is meaningless.
func spoilerFeature(reason string) map[string]any {
	f := featureOf("spoiler")
	if r := truncateText(strings.TrimSpace(reason), maxSpoilerReasonGraphemes, maxSpoilerReasonBytes); r != "" {
		f["reason"] = r
	}
	return f
}

// linkFeature gates every bridge-authored clickable uri behind the http(s)
// scheme check — the lexicon's format:"uri" would happily carry javascript:
// into clients. ok=false means keep the text, emit no facet.
func linkFeature(uri string) (map[string]any, bool) {
	if !isSafeLinkScheme(uri) {
		return nil, false
	}
	f := featureOf("link")
	f["uri"] = uri
	return f, true
}

// bridgedRichText converts a markdown body into the (content, facets) pair a
// record stores, applying the lexicon's grapheme/byte caps to the text and
// clamping facets against the truncated result — a byte range that slices
// outside the content would be dropped by Coves' ingest sanitizer. When
// conversion yields no visible text at all (a body that was only raw HTML),
// the markdown is stored tag-stripped (modulo the same caps) with no facets:
// degraded rendering beats dropped content, but live tags never land in a
// field clients treat as plaintext — so the result can be empty, and callers
// must treat "" as no content.
func bridgedRichText(md string, maxGraphemes, maxBytes int) (string, []any) {
	// Bound attacker-controlled parse work at the storage cap: the plaintext
	// is truncated to maxBytes anyway, so parsing beyond the cap (plus slack)
	// is pure waste. Cut on a rune boundary; truncateText re-caps precisely.
	if maxBytes > 0 && len(md) > maxBytes+parseSlack {
		cut := maxBytes + parseSlack
		for cut > 0 && !utf8.RuneStart(md[cut]) {
			cut--
		}
		md = md[:cut]
	}

	plain, spans := richTextFromMarkdown(md)
	if strings.TrimSpace(plain) == "" {
		return truncateText(strings.TrimSpace(stripTags(md)), maxGraphemes, maxBytes), nil
	}
	truncated := truncateText(plain, maxGraphemes, maxBytes)
	if truncated != plain {
		// The byte-cap path cuts without trimming, so the prefix can end in
		// '\n' — and a block facet clamped there would include a trailing
		// newline the convention forbids. TrimRight is safe for the grapheme
		// path too: its "…" is not in the cut set.
		truncated = strings.TrimRight(truncated, " \n\t")
		// The truncated text is a byte prefix of the original (with an
		// ellipsis appended on the grapheme-cap path only — the byte-cap path
		// appends nothing); clamp every span to the surviving prefix. A block
		// facet clamped mid-line is legal — readers extend it to line
		// boundaries.
		kept := commonPrefixLen(plain, truncated)
		// The byte compare can overcount into the appended ellipsis (E2 80
		// A6) when the next original rune shares its lead bytes (U+2000–
		// U+2FFF: ’ — • ™ …), leaving kept mid-rune — and a facet clamped
		// there would carry a byteEnd splitting a UTF-8 sequence, which
		// Coves' ingest drops whole. Walk back to the rune boundary.
		for kept > 0 && kept < len(truncated) && !utf8.RuneStart(truncated[kept]) {
			kept--
		}
		clamped := make([]facetSpan, 0, len(spans))
		for _, s := range spans {
			if s.start >= kept {
				continue
			}
			if s.end > kept {
				s.end = kept
			}
			clamped = append(clamped, s)
		}
		spans = clamped
	}
	return truncated, finalizeFacets(spans)
}

// richTextFromMarkdown parses markdown and renders it to plaintext plus raw
// facet spans (unmerged, unsorted — finalizeFacets produces the wire form).
func richTextFromMarkdown(md string) (string, []facetSpan) {
	source := []byte(md)
	root := richTextMarkdown.Parser().Parse(text.NewReader(source))
	c := &richTextConverter{source: source}
	c.renderBlocks(root, 0)
	rendered := c.b.String()
	spans := splitContainingQuotes(rendered, c.facets)
	return rendered, append(spans, bareURLSpans(rendered, spans)...)
}

// bareURLSpans linkifies http(s) URLs that goldmark's Linkify extension never
// saw. Linkify runs over the SOURCE bytes during inline parsing, but backslash
// escapes are only resolved at RENDER time (unescapeMarkdown) — so a federated
// app that escapes punctuation on the way out (PieFed writes
// `https\://example.com`) yields plaintext that reads as a URL and carries no
// facet at all. Scanning the finished plaintext catches those, and any other
// linkify miss, at the point where offsets are already final.
//
// Ranges already spoken for are skipped: an existing link (Linkify worked, or
// the author wrote `[url-ish text](other-url)` — whose text must keep pointing
// at the destination the author chose), and code/codeBlock, where literal text
// must never become clickable.
func bareURLSpans(rendered string, existing []facetSpan) []facetSpan {
	blocked := blockedLinkRanges(existing)
	var out []facetSpan
	// Regexp matches arrive in ascending order and blocked is disjoint and
	// sorted, so one forward cursor decides every overlap — a body that is
	// thousands of code spans around thousands of URLs stays linear.
	next := 0
	for _, m := range bareURLRe.FindAllStringIndex(rendered, -1) {
		start := m[0]
		end := start + len(trimURLTail(rendered[start:m[1]]))
		if start >= end {
			continue
		}
		for next < len(blocked) && blocked[next][1] <= start {
			next++
		}
		if next < len(blocked) && blocked[next][0] < end {
			continue
		}
		if feature, ok := linkFeature(rendered[start:end]); ok {
			out = append(out, facetSpan{start: start, end: end, feature: feature})
		}
	}
	return out
}

// blockedLinkRanges collects the ranges bareURLSpans must not touch, merged
// into disjoint sorted intervals. Link and code ranges genuinely nest
// (`[`x`](url)`), so merging — not just sorting — is what makes a single
// forward cursor correct.
func blockedLinkRanges(spans []facetSpan) [][2]int {
	var ranges [][2]int
	for _, s := range spans {
		switch s.feature["$type"] {
		case facetTypePrefix + "link", facetTypePrefix + "code", facetTypePrefix + "codeBlock":
			ranges = append(ranges, [2]int{s.start, s.end})
		}
	}
	if len(ranges) == 0 {
		return nil
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i][0] < ranges[j][0] })
	merged := ranges[:1]
	for _, r := range ranges[1:] {
		last := &merged[len(merged)-1]
		if r[0] <= last[1] {
			last[1] = max(last[1], r[1])
		} else {
			merged = append(merged, r)
		}
	}
	return merged
}

// trimURLTail applies GFM's extended-autolink tail rules to a candidate match:
// trailing punctuation belongs to the sentence, not the link, and a closing
// bracket counts only when the URL itself opened one (Wikipedia-style paths).
func trimURLTail(u string) string {
	for u != "" {
		switch u[len(u)-1] {
		case '?', '!', '.', ',', ':', ';', '*', '_', '~', '\'', '"':
		case ')':
			if strings.Count(u, ")") <= strings.Count(u, "(") {
				return u
			}
		case ']':
			if strings.Count(u, "]") <= strings.Count(u, "[") {
				return u
			}
		default:
			return u
		}
		u = u[:len(u)-1]
	}
	return u
}

// requestSep asks for a separator before the next written content. Competing
// requests keep the longer separator ("\n\n" over "\n"); nothing is ever
// written at the very start of the buffer.
func (c *richTextConverter) requestSep(s string) {
	if c.b.Len() == 0 {
		return
	}
	if len(s) > len(c.sep) {
		c.sep = s
	}
}

// writeText appends content, flushing any pending separator first, and
// tracks the current line start so block facets can re-anchor to it.
func (c *richTextConverter) writeText(s string) {
	if s == "" {
		return
	}
	if c.sep != "" {
		c.b.WriteString(c.sep)
		c.sep = ""
		// Separators are all-newline strings, so a flush opens a new line.
		c.lineStart = c.b.Len()
	}
	if i := strings.LastIndexByte(s, '\n'); i >= 0 {
		c.lineStart = c.b.Len() + i + 1
	}
	c.b.WriteString(s)
}

// nextPos returns the byte offset where the next written content will start,
// accounting for the pending separator without forcing it out. Facet starts
// are always taken from here; if the annotated node then renders nothing,
// the range's end (b.Len()) stays at or before the recorded start and
// addFacet's start >= end guard suppresses the span.
func (c *richTextConverter) nextPos() int {
	return c.b.Len() + len(c.sep)
}

// blockFacetStart returns the line-aligned start for a block facet. With a
// separator pending the next content opens a fresh line; mid-line (a list
// item's "• " marker already written) the facet anchors at the tracked line
// start so it swallows the marker — whole-line-correct, and readers render
// "• heading" as a heading line, which is the intended degradation.
func (c *richTextConverter) blockFacetStart() int {
	if c.sep != "" || c.b.Len() == 0 {
		return c.nextPos()
	}
	return c.lineStart
}

func (c *richTextConverter) addFacet(start, end int, feature map[string]any) {
	if start >= end || c.facetsOff {
		return
	}
	c.facets = append(c.facets, facetSpan{start: start, end: end, feature: feature})
}

// enterDepth guards render recursion (blocks, quotes, lists, inlines):
// markdown nested beyond maxRenderDepth is adversarial by construction, so
// deeper nodes render nothing — the goldmark AST itself is already built,
// but the converter's recursion must not chase it into the stack limit.
func (c *richTextConverter) enterDepth() bool {
	if c.depth >= maxRenderDepth {
		return false
	}
	c.depth++
	return true
}

func (c *richTextConverter) leaveDepth() { c.depth-- }

func (c *richTextConverter) renderBlocks(parent ast.Node, quoteLevel int) {
	for n := parent.FirstChild(); n != nil; n = n.NextSibling() {
		c.requestSep("\n\n")
		c.renderBlock(n, quoteLevel)
	}
}

func (c *richTextConverter) renderBlock(n ast.Node, quoteLevel int) {
	if !c.enterDepth() {
		return
	}
	defer c.leaveDepth()
	switch t := n.(type) {
	case *ast.Paragraph, *ast.TextBlock:
		c.renderInlines(n)
	case *ast.Heading:
		c.renderHeading(t)
	case *ast.Blockquote:
		c.renderQuote(t, quoteLevel+1)
	case *ast.FencedCodeBlock:
		c.renderCodeBlock(n, string(t.Language(c.source)))
	case *ast.CodeBlock:
		c.renderCodeBlock(n, "")
	case *ast.List:
		c.renderList(t, quoteLevel, 0)
	case *ast.ThematicBreak:
		c.writeText("———")
	case *ast.HTMLBlock:
		c.renderHTMLBlock(t)
	case *spoilerNode:
		c.renderSpoiler(t, quoteLevel)
	default:
		if n.Kind() == east.KindTable {
			c.renderTable(n)
			return
		}
		// Unknown block container: degrade to its children's text.
		c.renderBlocks(n, quoteLevel)
	}
}

// renderHeading emits the heading text as a single line (the lexicon says a
// heading facet spans one whole line, so setext headings' soft breaks become
// spaces) annotated with a heading facet carrying the level.
func (c *richTextConverter) renderHeading(h *ast.Heading) {
	start := c.blockFacetStart()
	prev := c.singleLine
	c.singleLine = true
	c.renderInlines(h)
	c.singleLine = prev
	c.addFacet(start, c.b.Len(), headingFeature(h.Level))
}

// renderQuote renders a blockquote's children, covering each run of
// consecutive non-quote blocks with a single blockquote facet at this level.
// A nested quote interrupts the run and recurses with level+1, producing the
// lexicon's shape: DISJOINT ranges with increasing level, never containment.
// (A quote nested through a non-quote container — a list — is invisible
// here; splitContainingQuotes repairs those after the walk.)
func (c *richTextConverter) renderQuote(q ast.Node, level int) {
	if !c.enterDepth() {
		return
	}
	defer c.leaveDepth()
	runStart := -1
	flush := func() {
		if runStart >= 0 {
			c.addFacet(runStart, c.b.Len(), quoteFeature(level))
			runStart = -1
		}
	}
	for n := q.FirstChild(); n != nil; n = n.NextSibling() {
		c.requestSep("\n\n")
		if n.Kind() == ast.KindBlockquote {
			flush()
			c.renderQuote(n, level+1)
			continue
		}
		blockStart := c.blockFacetStart()
		before := c.nextPos()
		c.renderBlock(n, level)
		if runStart < 0 && c.b.Len() > before {
			runStart = blockStart
		}
	}
	flush()
}

// renderCodeBlock emits the literal code (fence markers stripped, interior
// whitespace preserved, trailing newlines trimmed — ranges exclude the
// trailing newline) under a codeBlock facet. CRLF line endings are
// normalized so '\r' never lands in records.
func (c *richTextConverter) renderCodeBlock(n ast.Node, language string) {
	var buf bytes.Buffer
	lines := n.Lines()
	for i := 0; i < lines.Len(); i++ {
		line := lines.At(i)
		buf.Write(line.Value(c.source))
	}
	code := strings.ReplaceAll(buf.String(), "\r\n", "\n")
	code = strings.TrimRight(code, "\r\n")
	if code == "" {
		return
	}
	start := c.blockFacetStart()
	c.writeText(code)
	c.addFacet(start, c.b.Len(), codeBlockFeature(language))
}

// renderSpoiler emits the container's contents (fence lines already consumed
// by the parser) under a spoiler facet carrying the title as `reason`. The
// title itself stays out of the plaintext: duplicating it there would read as
// a stray line for any client that ignores the facet, and `reason` is where
// the lexicon puts it.
func (c *richTextConverter) renderSpoiler(s *spoilerNode, quoteLevel int) {
	start := c.blockFacetStart()
	c.renderBlocks(s, quoteLevel)
	c.addFacet(start, c.b.Len(), spoilerFeature(s.Reason))
}

// renderList normalizes list markers into the canonical text — "• " bullets
// and "N. " ordinals, two spaces of indent per nesting level, one item per
// line. No facet by design: the lexicon deliberately has no list feature
// because the marked-up plaintext already reads correctly.
func (c *richTextConverter) renderList(l *ast.List, quoteLevel, indent int) {
	if !c.enterDepth() {
		return
	}
	defer c.leaveDepth()
	ordinal := l.Start
	if ordinal == 0 {
		ordinal = 1
	}
	for item := l.FirstChild(); item != nil; item = item.NextSibling() {
		c.requestSep("\n")
		marker := strings.Repeat("  ", indent)
		if l.IsOrdered() {
			marker += strconv.Itoa(ordinal) + ". "
			ordinal++
		} else {
			marker += "• "
		}
		c.writeText(marker)
		first := true
		for b := item.FirstChild(); b != nil; b = b.NextSibling() {
			if !first {
				c.requestSep("\n")
			}
			first = false
			if nested, ok := b.(*ast.List); ok {
				c.renderList(nested, quoteLevel, indent+1)
			} else {
				c.renderBlock(b, quoteLevel)
			}
		}
	}
}

// renderTable degrades a GFM table to " | "-delimited plaintext lines (with
// a dashed rule under the header) covered by a single codeBlock facet, the
// deliberate monospace fallback until the lexicon grows a table feature.
// Inline facets are suppressed inside: annotations under the monospace block
// would fight the fallback rendering.
func (c *richTextConverter) renderTable(tbl ast.Node) {
	start := c.blockFacetStart()
	prev := c.facetsOff
	c.facetsOff = true
	for row := tbl.FirstChild(); row != nil; row = row.NextSibling() {
		c.requestSep("\n")
		columns := 0
		first := true
		for cell := row.FirstChild(); cell != nil; cell = cell.NextSibling() {
			if !first {
				c.writeText(" | ")
			}
			first = false
			columns++
			c.renderInlines(cell)
		}
		if row.Kind() == east.KindTableHeader && columns > 0 {
			c.requestSep("\n")
			rule := make([]string, columns)
			for i := range rule {
				rule[i] = "---"
			}
			c.writeText(strings.Join(rule, " | "))
		}
	}
	c.facetsOff = prev
	c.addFacet(start, c.b.Len(), codeBlockFeature(""))
}

// renderHTMLBlock degrades a raw HTML block to its visible text (tags
// stripped, whitespace collapsed). Active elements are neutralized in
// layers: stripActiveHTML covers the source-markdown path, the HTML→
// markdown library drops script/style during conversion, and stripTags here
// neutralizes whatever else survives.
func (c *richTextConverter) renderHTMLBlock(n *ast.HTMLBlock) {
	var buf bytes.Buffer
	lines := n.Lines()
	for i := 0; i < lines.Len(); i++ {
		line := lines.At(i)
		buf.Write(line.Value(c.source))
	}
	if n.HasClosure() {
		buf.Write(n.ClosureLine.Value(c.source))
	}
	c.writeText(stripTags(buf.String()))
}

func (c *richTextConverter) renderInlines(parent ast.Node) {
	for n := parent.FirstChild(); n != nil; n = n.NextSibling() {
		c.renderInline(n)
	}
}

func (c *richTextConverter) renderInline(n ast.Node) {
	if !c.enterDepth() {
		return
	}
	defer c.leaveDepth()
	switch t := n.(type) {
	case *ast.Text:
		if seg := string(t.Segment.Value(c.source)); t.IsRaw() {
			c.writeText(seg)
		} else {
			c.writeText(unescapeMarkdown(seg))
		}
		if t.SoftLineBreak() || t.HardLineBreak() {
			c.writeText(c.lineBreak())
		}
	case *ast.String:
		// IsCode/IsRaw strings keep their bytes verbatim, mirroring
		// goldmark's own renderer split.
		if t.IsCode() || t.IsRaw() {
			c.writeText(string(t.Value))
		} else {
			c.writeText(unescapeMarkdown(string(t.Value)))
		}
	case *ast.Emphasis:
		name := "italic"
		if t.Level >= 2 {
			name = "bold"
		}
		c.inlineSpan(n, featureOf(name))
	case *east.Strikethrough:
		c.inlineSpan(n, featureOf("strikethrough"))
	case *ast.CodeSpan:
		c.renderCodeSpan(t)
	case *ast.Link:
		c.renderLink(t)
	case *ast.AutoLink:
		c.renderAutoLink(t)
	case *ast.Image:
		c.renderImage(t)
	case *ast.RawHTML:
		c.renderRawHTML(t)
	default:
		// Unknown inline: degrade to its children's text.
		c.renderInlines(n)
	}
}

// lineBreak is what a soft/hard markdown line break renders as: a newline,
// except inside headings where the facet must stay on a single line.
func (c *richTextConverter) lineBreak() string {
	if c.singleLine {
		return " "
	}
	return "\n"
}

// inlineSpan renders n's children and annotates the produced range.
func (c *richTextConverter) inlineSpan(n ast.Node, feature map[string]any) {
	start := c.nextPos()
	c.renderInlines(n)
	c.addFacet(start, c.b.Len(), feature)
}

// renderCodeSpan emits the literal code (backticks stripped) under an inline
// code facet. CommonMark renders line endings inside code spans as spaces.
func (c *richTextConverter) renderCodeSpan(cs *ast.CodeSpan) {
	var buf bytes.Buffer
	for child := cs.FirstChild(); child != nil; child = child.NextSibling() {
		if txt, ok := child.(*ast.Text); ok {
			buf.Write(txt.Segment.Value(c.source))
		}
	}
	code := strings.ReplaceAll(buf.String(), "\n", " ")
	start := c.nextPos()
	c.writeText(code)
	c.addFacet(start, c.b.Len(), featureOf("code"))
}

// renderLink emits the link text (or the URL itself for []() links with no
// text) with a link facet. Non-http(s) destinations keep their text but get
// no facet (linkFeature's fail-closed scheme rule).
func (c *richTextConverter) renderLink(l *ast.Link) {
	// Destinations carry source-level backslash escapes and entities just
	// like text segments; resolve them before the scheme check and storage
	// so the stored uri is the one the author meant.
	uri := unescapeMarkdown(string(l.Destination))
	start := c.nextPos()
	c.renderInlines(l)
	if c.nextPos() == start {
		c.writeText(uri)
	}
	if feature, ok := linkFeature(uri); ok {
		c.addFacet(start, c.b.Len(), feature)
	}
}

// renderAutoLink writes the autolinked text. Email autolinks deliberately
// get no facet: the scheme gate is http/https only, so an address (mailto:
// or bare) stays plain text.
func (c *richTextConverter) renderAutoLink(al *ast.AutoLink) {
	start := c.nextPos()
	c.writeText(string(al.Label(c.source)))
	if feature, ok := linkFeature(string(al.URL(c.source))); ok {
		c.addFacet(start, c.b.Len(), feature)
	}
}

// renderImage degrades an inline image to its alt text ("image" when the
// author gave none) linked to the image URL — the closest plaintext
// rendering; the record-level embed path handles actual image display.
func (c *richTextConverter) renderImage(img *ast.Image) {
	uri := unescapeMarkdown(string(img.Destination))
	start := c.nextPos()
	c.renderInlines(img)
	if c.nextPos() == start {
		c.writeText("image")
	}
	if feature, ok := linkFeature(uri); ok {
		c.addFacet(start, c.b.Len(), feature)
	}
}

// renderRawHTML drops inline tags (their inner text arrives as sibling Text
// nodes), keeping <br>'s line-break effect and minimal <a href> support: an
// opening anchor records where its text will start, the matching close emits
// a link facet over the written span. Single-level only — a new <a> resets
// any unclosed predecessor, and an anchor never closed emits no facet.
func (c *richTextConverter) renderRawHTML(n *ast.RawHTML) {
	var buf bytes.Buffer
	for i := 0; i < n.Segments.Len(); i++ {
		seg := n.Segments.At(i)
		buf.Write(seg.Value(c.source))
	}
	tag := strings.TrimSpace(buf.String())
	switch {
	case brTagRe.MatchString(tag):
		c.writeText(c.lineBreak())
	case anchorCloseRe.MatchString(tag):
		if c.htmlLinkURI != "" {
			if feature, ok := linkFeature(c.htmlLinkURI); ok {
				c.addFacet(c.htmlLinkStart, c.b.Len(), feature)
			}
			c.htmlLinkURI = ""
		}
	default:
		if m := anchorOpenRe.FindStringSubmatch(tag); m != nil {
			uri := m[1]
			if uri == "" {
				uri = m[2]
			}
			c.htmlLinkStart = c.nextPos()
			// Attribute values are HTML: entity-unescape before the scheme
			// gate so an &amp;-joined query string stores the real uri.
			c.htmlLinkURI = html.UnescapeString(uri)
		}
	}
}

// splitContainingQuotes enforces the lexicon's DISJOINT-quotes shape for
// nestings the tree walk cannot see: a quote reached through a non-quote
// container (a list inside a quote) leaves the outer run's range containing
// the inner quote's — forbidden containment. Every containing blockquote
// span is split into the pieces around its contained spans; pieces keep the
// outer's feature (and level) and re-align to whole lines, because a cut at
// a contained span's edge would otherwise sit on the separator newlines.
func splitContainingQuotes(rendered string, spans []facetSpan) []facetSpan {
	isQuote := func(s facetSpan) bool {
		return s.feature["$type"] == facetTypePrefix+"blockquote"
	}
	out := make([]facetSpan, 0, len(spans))
	for i, s := range spans {
		if !isQuote(s) {
			out = append(out, s)
			continue
		}
		// Collect the ranges of every other quote span strictly inside s
		// (identical ranges are not containment — they merge at finalize).
		var holes [][2]int
		for j, inner := range spans {
			if j == i || !isQuote(inner) {
				continue
			}
			if inner.start >= s.start && inner.end <= s.end &&
				(inner.start > s.start || inner.end < s.end) {
				holes = append(holes, [2]int{inner.start, inner.end})
			}
		}
		if len(holes) == 0 {
			out = append(out, s)
			continue
		}
		sort.Slice(holes, func(a, b int) bool { return holes[a][0] < holes[b][0] })
		merged := holes[:1]
		for _, h := range holes[1:] {
			last := &merged[len(merged)-1]
			if h[0] <= last[1] {
				last[1] = max(last[1], h[1])
			} else {
				merged = append(merged, h)
			}
		}
		pieceStart := s.start
		for k := 0; k <= len(merged); k++ {
			pieceEnd := s.end
			if k < len(merged) {
				pieceEnd = merged[k][0]
			}
			ps, pe := pieceStart, pieceEnd
			for pe > ps && rendered[pe-1] == '\n' {
				pe--
			}
			for ps < pe && rendered[ps] == '\n' {
				ps++
			}
			if ps < pe {
				out = append(out, facetSpan{start: ps, end: pe, feature: s.feature})
			}
			if k < len(merged) {
				pieceStart = merged[k][1]
			}
		}
	}
	return out
}

// finalizeFacets produces the wire-form facet list: invalid spans dropped,
// sorted by start (containing ranges before contained on ties), identical
// ranges merged into one facet with stacked features, and both lexicon caps
// applied keeping the earliest entries. Returns nil when nothing survives —
// callers omit the record field entirely (nil-means-absent).
func finalizeFacets(spans []facetSpan) []any {
	valid := make([]facetSpan, 0, len(spans))
	for _, s := range spans {
		if s.start < s.end {
			valid = append(valid, s)
		}
	}
	if len(valid) == 0 {
		return nil
	}
	sort.SliceStable(valid, func(i, j int) bool {
		if valid[i].start != valid[j].start {
			return valid[i].start < valid[j].start
		}
		return valid[i].end > valid[j].end
	})

	var out []any
	for i := 0; i < len(valid) && len(out) < maxFacets; {
		j := i
		var features []any
		for ; j < len(valid) && valid[j].start == valid[i].start && valid[j].end == valid[i].end; j++ {
			if len(features) >= maxFeaturesPerFacet {
				continue
			}
			// A repeated $type on one range adds nothing (****x**** parses
			// as nested bolds sharing a range) — keep the first of each.
			duplicate := false
			for _, existing := range features {
				if existing.(map[string]any)["$type"] == valid[j].feature["$type"] {
					duplicate = true
					break
				}
			}
			if !duplicate {
				features = append(features, valid[j].feature)
			}
		}
		out = append(out, map[string]any{
			"index": map[string]any{
				"byteStart": valid[i].start,
				"byteEnd":   valid[i].end,
			},
			"features": features,
		})
		i = j
	}
	return out
}

// commonPrefixLen returns the length in bytes of the longest common prefix
// of a and b — how much of the original text survived truncation.
func commonPrefixLen(a, b string) int {
	n := min(len(a), len(b))
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

// unescapeMarkdown resolves what goldmark defers to RENDER time: ast.Text
// segments still carry backslash escapes (`\*`) and HTML entity/numeric
// references (`&amp;`) verbatim — goldmark's own HTML renderer resolves
// both in its writer. The bridge renders to plaintext, and the HTML→markdown
// path routinely escapes prose ("2 * 3" → "2 \* 3"), so skipping this would
// store literal backslashes and entities. Backslash handling mirrors
// CommonMark (only ASCII punctuation is escapable); entities resolve one at
// a time through stdlib html.UnescapeString, in the same left-to-right pass
// so an escaped ampersand never re-triggers entity parsing. Code spans and
// code blocks never come here — raw is correct there.
func unescapeMarkdown(s string) string {
	if !strings.ContainsAny(s, `\&`) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '\\' && i+1 < len(s) && util.IsPunct(s[i+1]) {
			i++
			b.WriteByte(s[i])
			continue
		}
		if ch == '&' {
			if semi := strings.IndexByte(s[i:min(i+maxEntityLen, len(s))], ';'); semi > 0 {
				candidate := s[i : i+semi+1]
				if resolved := html.UnescapeString(candidate); resolved != candidate {
					b.WriteString(resolved)
					i += semi
					continue
				}
			}
		}
		b.WriteByte(ch)
	}
	return b.String()
}
