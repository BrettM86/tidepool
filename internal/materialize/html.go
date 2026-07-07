package materialize

import (
	"strings"

	htmltomarkdown "github.com/JohannesKaufmann/html-to-markdown/v2"
	"github.com/rivo/uniseg"

	"tidepool/internal/ap"
)

// HTML→markdown strategy: Lemmy sends both rendered HTML (`content`) and the
// author's original markdown (`source.content`, mediaType text/markdown) on
// nearly every object. The original markdown is always preferred — it is
// exactly what the author wrote and needs no conversion. Only when `source`
// is absent (some PieFed/Mbin objects, older Lemmy) do we convert the HTML,
// using the established JohannesKaufmann/html-to-markdown library (v2)
// rather than a hand-rolled converter: it handles entity decoding, nested
// lists, blockquotes, and — security-relevant here — drops <script>/<style>
// elements entirely during conversion. Coves' lexicons treat content as
// markdown, so no facet generation is needed; links stay inline as
// [text](url).

// markdownFromObject returns the object's body as markdown: the AP source
// markdown when present, else the HTML content converted. Empty when the
// object has neither (e.g. title-only link posts).
func markdownFromObject(obj *ap.Object) string {
	if obj.Source != nil && strings.TrimSpace(obj.Source.Content) != "" {
		mt := obj.Source.MediaType
		if mt == "" || strings.HasPrefix(mt, "text/markdown") || strings.HasPrefix(mt, "text/plain") {
			return stripActiveHTML(strings.TrimSpace(obj.Source.Content))
		}
	}
	return htmlToMarkdown(obj.Content)
}

// stripActiveHTML removes raw-HTML element blocks that carry executable or
// active content (CommonMark permits raw HTML inside markdown). The HTML→
// markdown conversion path drops these during conversion; the preferred
// source-markdown path is stored closer to verbatim, so it needs the same
// scrub to give equivalent safety at this trust boundary.
func stripActiveHTML(s string) string {
	for _, tag := range []string{"script", "style", "iframe", "object", "embed"} {
		s = stripElementBlocks(s, tag)
	}
	return strings.TrimSpace(s)
}

// stripElementBlocks deletes every `<tag ...>...</tag>` span (and an
// unterminated trailing `<tag ...`), case-insensitively, requiring a tag-name
// boundary so `<scripting>` is not mistaken for `<script>`.
func stripElementBlocks(s, tag string) string {
	open := "<" + tag
	closeTag := "</" + tag + ">"
	searchFrom := 0
	for {
		lower := strings.ToLower(s)
		rel := strings.Index(lower[searchFrom:], open)
		if rel < 0 {
			return s
		}
		i := searchFrom + rel
		after := i + len(open)
		// Require a tag-name boundary; otherwise it's a false match
		// (`<scripting>`) — resume the search past it.
		if after < len(s) {
			if c := lower[after]; c != ' ' && c != '>' && c != '/' && c != '\t' && c != '\n' {
				searchFrom = after
				continue
			}
		}
		j := strings.Index(lower[after:], closeTag)
		if j < 0 {
			return s[:i] // unterminated block: drop to end
		}
		s = s[:i] + s[after+j+len(closeTag):]
		searchFrom = i
	}
}

// htmlToMarkdown converts rendered HTML to markdown. Conversion failures
// (malformed HTML that even the tolerant parser rejects) degrade to the
// stripped plain text rather than dropping the content.
func htmlToMarkdown(html string) string {
	if strings.TrimSpace(html) == "" {
		return ""
	}
	md, err := htmltomarkdown.ConvertString(html)
	if err != nil {
		return strings.TrimSpace(stripTags(html))
	}
	return strings.TrimSpace(md)
}

// stripTags is the degraded fallback when markdown conversion fails: drop
// everything between angle brackets. Crude, but only reachable for HTML the
// real parser refused; better a rough text body than dropped content.
func stripTags(html string) string {
	var b strings.Builder
	inTag := false
	for _, r := range html {
		switch {
		case r == '<':
			inTag = true
		case r == '>':
			inTag = false
			b.WriteByte(' ')
		case !inTag:
			b.WriteRune(r)
		}
	}
	return strings.Join(strings.Fields(b.String()), " ")
}

// graphemeCount counts grapheme clusters (the lexicon maxGraphemes unit).
func graphemeCount(s string) int { return uniseg.GraphemeClusterCount(s) }

// truncateGraphemes caps a string at max grapheme clusters (the unit
// atproto lexicon maxGraphemes counts), appending an ellipsis when it had
// to cut. Coves counts graphemes with the same library (rivo/uniseg).
func truncateGraphemes(s string, max int) string {
	if max <= 0 {
		return ""
	}
	if uniseg.GraphemeClusterCount(s) <= max {
		return s
	}
	g := uniseg.NewGraphemes(s)
	var b strings.Builder
	count := 0
	for g.Next() && count < max-1 {
		b.WriteString(g.Str())
		count++
	}
	return strings.TrimRight(b.String(), " \n\t") + "…"
}

// truncateText caps a string against BOTH the lexicon grapheme budget and
// the byte budget (maxLength). The two are not proportional — a ZWJ-emoji
// grapheme is ~25 bytes — so a value inside maxGraphemes can still blow
// maxLength and fail validation. Grapheme-trim first, then drop whole
// clusters until the bytes fit, so multi-byte runes are never split.
func truncateText(s string, maxGraphemes, maxBytes int) string {
	s = truncateGraphemes(s, maxGraphemes)
	if maxBytes <= 0 || len(s) <= maxBytes {
		return s
	}
	g := uniseg.NewGraphemes(s)
	var b strings.Builder
	for g.Next() {
		if b.Len()+len(g.Str()) > maxBytes {
			break
		}
		b.WriteString(g.Str())
	}
	return b.String()
}
