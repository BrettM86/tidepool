package materialize

import (
	"strings"
	"testing"

	"github.com/rivo/uniseg"
	"github.com/stretchr/testify/assert"

	"tidepool/internal/ap"
)

func TestMarkdownFromObjectPrefersSource(t *testing.T) {
	obj := &ap.Object{
		Content: "<p>rendered <strong>HTML</strong></p>",
		Source: &ap.Source{
			Content:   "original *markdown* ",
			MediaType: "text/markdown",
		},
	}
	assert.Equal(t, "original *markdown*", markdownFromObject(obj),
		"the author's original markdown must win over rendered HTML")
}

func TestMarkdownFromObjectConvertsHTMLWhenNoSource(t *testing.T) {
	obj := &ap.Object{
		Content: `<p>Hello <a href="https://x.example">link</a> &amp; <em>emphasis</em></p>`,
	}
	assert.Equal(t, "Hello [link](https://x.example) & *emphasis*", markdownFromObject(obj))
}

func TestMarkdownFromObjectIgnoresNonMarkdownSource(t *testing.T) {
	obj := &ap.Object{
		Content: "<p>the html</p>",
		Source:  &ap.Source{Content: "<div>not markdown</div>", MediaType: "text/html"},
	}
	assert.Equal(t, "the html", markdownFromObject(obj),
		"a non-markdown source must fall back to converting content")
}

func TestHTMLToMarkdownStripsScriptsAndStyles(t *testing.T) {
	md := htmlToMarkdown(`<p>safe</p><script>alert("xss")</script><style>p{display:none}</style>`)
	assert.Equal(t, "safe", md)
	assert.NotContains(t, md, "alert")
	assert.NotContains(t, md, "display")
}

func TestHTMLToMarkdownEmpty(t *testing.T) {
	assert.Equal(t, "", htmlToMarkdown(""))
	assert.Equal(t, "", htmlToMarkdown("   \n"))
}

func TestStripTagsFallback(t *testing.T) {
	assert.Equal(t, "a b", stripTags("<p>a</p><p>b</p>"))
}

func TestTruncateGraphemes(t *testing.T) {
	assert.Equal(t, "short", truncateGraphemes("short", 10), "under the cap: untouched")

	long := strings.Repeat("ab", 200)
	got := truncateGraphemes(long, 50)
	assert.LessOrEqual(t, uniseg.GraphemeClusterCount(got), 50)
	assert.True(t, strings.HasSuffix(got, "…"))

	// Grapheme clusters, not runes/bytes: family emoji is many runes but
	// one grapheme.
	family := strings.Repeat("👨‍👩‍👧‍👦", 5)
	assert.Equal(t, family, truncateGraphemes(family, 5), "5 graphemes fit a cap of 5")
	cut := truncateGraphemes(family, 3)
	assert.LessOrEqual(t, uniseg.GraphemeClusterCount(cut), 3)
}

func TestBioWithProvenance(t *testing.T) {
	provenance := provenanceLine("@", "alice", "lemmy.world")

	t.Run("empty bio is just the provenance line", func(t *testing.T) {
		assert.Equal(t, provenance, bioWithProvenance("", provenance, 256, 2560))
	})

	t.Run("bio and line both fit", func(t *testing.T) {
		got := bioWithProvenance("I like turtles.", provenance, 256, 2560)
		assert.Equal(t, "I like turtles.\n\n"+provenance, got)
		assert.LessOrEqual(t, uniseg.GraphemeClusterCount(got), 256)
	})

	t.Run("long bio truncates, provenance survives intact", func(t *testing.T) {
		got := bioWithProvenance(strings.Repeat("x", 400), provenance, 256, 2560)
		assert.LessOrEqual(t, uniseg.GraphemeClusterCount(got), 256)
		assert.True(t, strings.HasSuffix(got, provenance),
			"the provenance line must never be truncated away")
	})
}
