package apobject

import (
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testImageFacetType = "social.coves.richtext.facet#image"

	testImageCID0 = "bafkreihak3l6grklnl53pnqjtrwfph7wyojvrsaazb7nmptucuhobephqy"
	testImageCID1 = "bafkreihh33brlfst3dssji2k4fllfvevfp6q6c2zrqfm2soybkt2pq3lui"
	testImageCID2 = "bafkreifqrcn632tp7gjowacdghbw42hrerxvk4jrw56asf4tvrw3p75s7q"

	// The proxy URLs of the fixture CIDs for did:plc:abc under https://img.example.
	testImageURL0 = "https://img.example/img/content_full/plain/did:plc:abc/bafkreihak3l6grklnl53pnqjtrwfph7wyojvrsaazb7nmptucuhobephqy"
	testImageURL1 = "https://img.example/img/content_full/plain/did:plc:abc/bafkreihh33brlfst3dssji2k4fllfvevfp6q6c2zrqfm2soybkt2pq3lui"
	testImageURL2 = "https://img.example/img/content_full/plain/did:plc:abc/bafkreifqrcn632tp7gjowacdghbw42hrerxvk4jrw56asf4tvrw3p75s7q"
)

// testMedia is where the fixture comment's (at://did:plc:abc/...) images are served.
var testMedia = MediaSource{BaseURL: "https://img.example", AuthorDID: "did:plc:abc"}

func imageFeature(index any) map[string]any {
	return map[string]any{"$type": testImageFacetType, "index": index}
}

// galleryEntry builds one social.coves.embed.images entry with a blob ref.
func galleryEntry(cid, mimeType, alt string) map[string]any {
	return map[string]any{
		"image": map[string]any{
			"$type":    "blob",
			"ref":      map[string]any{"$link": cid},
			"mimeType": mimeType,
			"size":     float64(1234),
		},
		"alt": alt,
	}
}

func imagesEmbed(entries ...any) map[string]any {
	return map[string]any{"$type": "social.coves.embed.images", "images": entries}
}

// twoImageGallery is index 0 (alt "one", CID0) and index 1 (alt "two", CID1).
func twoImageGallery() map[string]any {
	return imagesEmbed(
		galleryEntry(testImageCID0, "image/png", "one"),
		galleryEntry(testImageCID1, "image/jpeg", "two"),
	)
}

func TestBuildNote_ImageFacetPlacesItsGalleryImage(t *testing.T) {
	tests := []struct {
		name             string
		embed            map[string]any
		expectedMarkdown string
		expectedHTML     string
		roundTripAlt     string // alt a CommonMark parse of the source yields; "" skips the check
	}{
		{
			name:             "alt with brackets, backslash and a line break",
			embed:            imagesEmbed(galleryEntry(testImageCID0, "image/png", "a [b] \\c\nd")),
			expectedMarkdown: `see ![a \[b\] \\c d](` + testImageURL0 + `) now`,
			expectedHTML:     `<p>see <img src="` + testImageURL0 + `" alt="a [b] \c d"> now</p>` + "\n",
			roundTripAlt:     `a [b] \c d`,
		},
		{
			name: "gallery inside imagesWithAttachment",
			embed: map[string]any{
				"$type": "social.coves.embed.imagesWithAttachment",
				"images": map[string]any{
					"images": []any{galleryEntry(testImageCID0, "image/png", "one")},
				},
				"attachment": map[string]any{
					"$type":    "social.coves.embed.external",
					"external": map[string]any{"uri": "https://example.com/guide"},
				},
			},
			expectedMarkdown: "see ![one](" + testImageURL0 + ") now",
			expectedHTML:     `<p>see <img src="` + testImageURL0 + `" alt="one"> now</p>` + "\n",
			roundTripAlt:     "one",
		},
		{
			name:             "alt with quotes, ampersand and angle brackets",
			embed:            imagesEmbed(galleryEntry(testImageCID0, "image/png", `x" onerror="bad & <b>`)),
			expectedMarkdown: `see ![x" onerror="bad & <b>](` + testImageURL0 + `) now`,
			expectedHTML:     `<p>see <img src="` + testImageURL0 + `" alt="x&#34; onerror=&#34;bad &amp; &lt;b&gt;"> now</p>` + "\n",
		},
		{
			name:             "alt with CRLF line break",
			embed:            imagesEmbed(galleryEntry(testImageCID0, "image/png", "a\r\nb")),
			expectedMarkdown: "see ![a b](" + testImageURL0 + ") now",
			expectedHTML:     `<p>see <img src="` + testImageURL0 + `" alt="a b"> now</p>` + "\n",
			roundTripAlt:     "a b",
		},
		{
			name:             "empty alt",
			embed:            imagesEmbed(galleryEntry(testImageCID0, "image/png", "")),
			expectedMarkdown: "see ![](" + testImageURL0 + ") now",
			expectedHTML:     `<p>see <img src="` + testImageURL0 + `" alt=""> now</p>` + "\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNoteWithEmbed(t, "see pic now",
				[]any{testFacet(4, 7, imageFeature(float64(0)))}, test.embed, testMedia)

			assert.Equal(t, test.expectedMarkdown, markdown)
			assert.Equal(t, test.expectedHTML, content)
			if test.roundTripAlt != "" {
				images := findTestElements(parseRenderedMarkdown(t, markdown), "img")
				require.Len(t, images, 1)
				assert.Equal(t, testImageURL0, testHTMLAttribute(images[0], "src"))
				assert.Equal(t, test.roundTripAlt, testHTMLAttribute(images[0], "alt"))
			}
		})
	}
}

func TestBuildNote_AuthorImageSyntaxStaysEscapedWithoutFacets(t *testing.T) {
	markdown, content := buildTestNoteWithEmbed(t, "![x](https://evil.example/y.png)", nil,
		twoImageGallery(), testMedia)

	assert.Equal(t, `\!\[x\](https://evil.example/y.png)`, markdown)
	assert.Equal(t, "<p>![x](https://evil.example/y.png)</p>\n", content)
	assert.Empty(t, findTestElements(parseRenderedMarkdown(t, markdown), "img"))
}

// Each row's first facet breaks one Coves FilterImageFacets rule and renders
// its covered text; where the row allows it, a valid sibling placement of
// index 1 shows the gallery is otherwise placed.
func TestBuildNote_ImageFacetsCovesFiltersFallBackToCoveredText(t *testing.T) {
	const plaintext = "see pic and cat"
	sibling := testFacet(12, 15, imageFeature(float64(1)))
	withSibling := "see pic and ![two](" + testImageURL1 + ")"
	withSiblingHTML := `<p>see pic and <img src="` + testImageURL1 + `" alt="two"></p>` + "\n"

	tests := []struct {
		name             string
		plaintext        string
		facets           []any
		noGallery        bool
		expectedMarkdown string
		expectedHTML     string
	}{
		{name: "index past the gallery",
			facets:           []any{testFacet(4, 7, imageFeature(float64(2))), sibling},
			expectedMarkdown: withSibling, expectedHTML: withSiblingHTML},
		{name: "negative index",
			facets:           []any{testFacet(4, 7, imageFeature(float64(-1))), sibling},
			expectedMarkdown: withSibling, expectedHTML: withSiblingHTML},
		{name: "fractional index",
			facets:           []any{testFacet(4, 7, imageFeature(1.5)), sibling},
			expectedMarkdown: withSibling, expectedHTML: withSiblingHTML},
		{name: "string index",
			facets:           []any{testFacet(4, 7, imageFeature("0")), sibling},
			expectedMarkdown: withSibling, expectedHTML: withSiblingHTML},
		{name: "missing index",
			facets: []any{
				testFacet(4, 7, map[string]any{"$type": testImageFacetType}), sibling,
			},
			expectedMarkdown: withSibling, expectedHTML: withSiblingHTML},
		{name: "second feature in the facet keeps the other feature",
			facets: []any{
				testFacet(4, 7, imageFeature(float64(0)), map[string]any{"$type": boldFacetType}), sibling,
			},
			expectedMarkdown: "see **pic** and ![two](" + testImageURL1 + ")",
			expectedHTML:     `<p>see <strong>pic</strong> and <img src="` + testImageURL1 + `" alt="two"></p>` + "\n"},
		{name: "null second feature",
			facets:           []any{testFacet(4, 7, imageFeature(float64(0)), nil), sibling},
			expectedMarkdown: withSibling, expectedHTML: withSiblingHTML},
		{name: "wholly inside a code range",
			facets: []any{
				testFacet(4, 7, codeFeature()), testFacet(4, 7, imageFeature(float64(0))), sibling,
			},
			expectedMarkdown: "see `pic` and ![two](" + testImageURL1 + ")",
			expectedHTML:     `<p>see <code>pic</code> and <img src="` + testImageURL1 + `" alt="two"></p>` + "\n"},
		{name: "wholly inside a code block", plaintext: "see pic\ncat",
			facets: []any{
				testFacet(0, 7, testCodeBlockFeature("")),
				testFacet(4, 7, imageFeature(float64(0))),
				testFacet(8, 11, imageFeature(float64(1))),
			},
			expectedMarkdown: "```\nsee pic\n```\n\n![two](" + testImageURL1 + ")",
			expectedHTML:     "<pre><code>see pic</code></pre>\n" + `<p><img src="` + testImageURL1 + `" alt="two"></p>` + "\n"},
		{name: "ends past the content",
			facets: []any{
				testFacet(12, 16, imageFeature(float64(0))), testFacet(4, 7, imageFeature(float64(1))),
			},
			expectedMarkdown: "see ![two](" + testImageURL1 + ") and cat",
			expectedHTML:     `<p>see <img src="` + testImageURL1 + `" alt="two"> and cat</p>` + "\n"},
		{name: "empty range",
			facets:           []any{testFacet(4, 4, imageFeature(float64(0))), sibling},
			expectedMarkdown: withSibling, expectedHTML: withSiblingHTML},
		{name: "negative range start",
			facets:           []any{testFacet(-1, 7, imageFeature(float64(0))), sibling},
			expectedMarkdown: withSibling, expectedHTML: withSiblingHTML},
		{name: "missing range",
			facets: []any{
				map[string]any{"features": []any{imageFeature(float64(0))}}, sibling,
			},
			expectedMarkdown: withSibling, expectedHTML: withSiblingHTML},
		{name: "second disjoint facet naming a placed index",
			facets: []any{
				testFacet(4, 7, imageFeature(float64(0))), testFacet(12, 15, imageFeature(float64(0))),
			},
			expectedMarkdown: "see ![one](" + testImageURL0 + ") and cat",
			expectedHTML:     `<p>see <img src="` + testImageURL0 + `" alt="one"> and cat</p>` + "\n"},
		{name: "duplicate index listed opposite to text order",
			facets: []any{
				testFacet(12, 15, imageFeature(float64(0))), testFacet(4, 7, imageFeature(float64(0))),
			},
			expectedMarkdown: "see pic and ![one](" + testImageURL0 + ")",
			expectedHTML:     `<p>see pic and <img src="` + testImageURL0 + `" alt="one"></p>` + "\n"},
		{name: "failed facet does not mark its index placed",
			facets: []any{
				testFacet(4, 7, codeFeature()),
				testFacet(4, 7, imageFeature(float64(0))),
				testFacet(12, 15, imageFeature(float64(0))),
			},
			expectedMarkdown: "see `pic` and ![one](" + testImageURL0 + ")",
			expectedHTML:     `<p>see <code>pic</code> and <img src="` + testImageURL0 + `" alt="one"></p>` + "\n"},
		{name: "no gallery",
			facets:           []any{testFacet(4, 7, imageFeature(float64(0)))},
			noGallery:        true,
			expectedMarkdown: "see pic and cat",
			expectedHTML:     "<p>see pic and cat</p>\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			text := test.plaintext
			if text == "" {
				text = plaintext
			}
			embed := twoImageGallery()
			if test.noGallery {
				embed = nil
			}
			markdown, content := buildTestNoteWithEmbed(t, text, test.facets, embed, testMedia)

			assert.Equal(t, test.expectedMarkdown, markdown)
			assert.Equal(t, test.expectedHTML, content)
		})
	}
}

// 03-c: every kept image renders. Intersecting kept image spans merge into one
// span replaced by their images in facets-array order; any other inline or
// block range crossing a kept image span drops to plain escaped text.
func TestBuildNote_KeptImagesWinConflictingRanges(t *testing.T) {
	tests := []struct {
		name             string
		plaintext        string
		facets           []any
		expectedMarkdown string
		expectedHTML     string
	}{
		{name: "image partially overlapping inline code", plaintext: "ab cd ef",
			facets: []any{
				testFacet(0, 5, codeFeature()), testFacet(3, 8, imageFeature(float64(0))),
			},
			expectedMarkdown: "ab ![one](" + testImageURL0 + ")",
			expectedHTML:     `<p>ab <img src="` + testImageURL0 + `" alt="one"></p>` + "\n"},
		{name: "image partially overlapping a code block", plaintext: "abc\ndef",
			facets: []any{
				testFacet(0, 2, testCodeBlockFeature("")), testFacet(1, 5, imageFeature(float64(0))),
			},
			expectedMarkdown: "a![one](" + testImageURL0 + ")ef",
			expectedHTML:     `<p>a<img src="` + testImageURL0 + `" alt="one">ef</p>` + "\n"},
		{name: "image spanning a paragraph break", plaintext: "x a\n\nb y",
			facets:           []any{testFacet(2, 6, imageFeature(float64(0)))},
			expectedMarkdown: "x ![one](" + testImageURL0 + ") y",
			expectedHTML:     `<p>x <img src="` + testImageURL0 + `" alt="one"> y</p>` + "\n"},
		{name: "link crossing the image", plaintext: "see pic now",
			facets: []any{
				testFacet(0, 6, linkFeature("https://example.com/docs")), testFacet(4, 7, imageFeature(float64(0))),
			},
			expectedMarkdown: "see ![one](" + testImageURL0 + ") now",
			expectedHTML:     `<p>see <img src="` + testImageURL0 + `" alt="one"> now</p>` + "\n"},
		{name: "emphasis crossing the image", plaintext: "see pic now",
			facets: []any{
				emphasisFacet(5, 11, "bold"), testFacet(4, 7, imageFeature(float64(0))),
			},
			expectedMarkdown: "see ![one](" + testImageURL0 + ") now",
			expectedHTML:     `<p>see <img src="` + testImageURL0 + `" alt="one"> now</p>` + "\n"},
		{name: "intersecting images in array order", plaintext: "xx pic yy",
			facets: []any{
				testFacet(3, 6, imageFeature(float64(0))), testFacet(4, 8, imageFeature(float64(1))),
			},
			expectedMarkdown: "xx ![one](" + testImageURL0 + ")![two](" + testImageURL1 + ")y",
			expectedHTML: `<p>xx <img src="` + testImageURL0 + `" alt="one">` +
				`<img src="` + testImageURL1 + `" alt="two">y</p>` + "\n"},
		{name: "intersecting images listed opposite to their starts", plaintext: "xx pic yy",
			facets: []any{
				testFacet(4, 8, imageFeature(float64(1))), testFacet(3, 6, imageFeature(float64(0))),
			},
			expectedMarkdown: "xx ![two](" + testImageURL1 + ")![one](" + testImageURL0 + ")y",
			expectedHTML: `<p>xx <img src="` + testImageURL1 + `" alt="two">` +
				`<img src="` + testImageURL0 + `" alt="one">y</p>` + "\n"},
		{name: "range splitting a rune widens to the whole rune", plaintext: "xé y",
			facets:           []any{testFacet(1, 2, imageFeature(float64(0)))},
			expectedMarkdown: "x![one](" + testImageURL0 + ") y",
			expectedHTML:     `<p>x<img src="` + testImageURL0 + `" alt="one"> y</p>` + "\n"},
		{name: "spoiler widens to cover an image crossing its end", plaintext: "secret\nopen pic",
			facets: []any{
				testFacet(0, 6, testSpoilerFeature("")), testFacet(4, 12, imageFeature(float64(0))),
			},
			expectedMarkdown: "::: spoiler Spoiler\nsecr![one](" + testImageURL0 + ")\n:::\n\npic",
			expectedHTML: "<details>\n<summary>Spoiler</summary>\n" +
				`<p>secr<img src="` + testImageURL0 + `" alt="one"></p>` + "\n</details>\n<p>pic</p>\n"},
		{name: "image wholly inside a spoiler", plaintext: "secret pic\nafter",
			facets: []any{
				testFacet(0, 10, testSpoilerFeature("")), testFacet(7, 10, imageFeature(float64(0))),
			},
			expectedMarkdown: "::: spoiler Spoiler\nsecret ![one](" + testImageURL0 + ")\n:::\n\nafter",
			expectedHTML: "<details>\n<summary>Spoiler</summary>\n" +
				`<p>secret <img src="` + testImageURL0 + `" alt="one"></p>` + "\n</details>\n<p>after</p>\n"},
		{name: "heading crossing the image", plaintext: "Title pic\nbody",
			facets: []any{
				testFacet(0, 9, testHeadingFeature(float64(1))), testFacet(6, 12, imageFeature(float64(0))),
			},
			expectedMarkdown: "Title ![one](" + testImageURL0 + ")dy",
			expectedHTML:     `<p>Title <img src="` + testImageURL0 + `" alt="one">dy</p>` + "\n"},
		{name: "quote crossing the image", plaintext: "quote pic\nnext",
			facets: []any{
				testFacet(0, 9, testBlockquoteFeature()), testFacet(6, 12, imageFeature(float64(0))),
			},
			expectedMarkdown: "quote ![one](" + testImageURL0 + ")xt",
			expectedHTML:     `<p>quote <img src="` + testImageURL0 + `" alt="one">xt</p>` + "\n"},
		{name: "quote wholly containing the image", plaintext: "see pic",
			facets: []any{
				testFacet(0, 7, testBlockquoteFeature()), testFacet(4, 7, imageFeature(float64(0))),
			},
			expectedMarkdown: "> see ![one](" + testImageURL0 + ")",
			expectedHTML:     "<blockquote>\n" + `<p>see <img src="` + testImageURL0 + `" alt="one"></p>` + "\n</blockquote>\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			markdown, content := buildTestNoteWithEmbed(t, test.plaintext, test.facets, twoImageGallery(), testMedia)

			assert.Equal(t, test.expectedMarkdown, markdown)
			assert.Equal(t, test.expectedHTML, content)
			assert.True(t, utf8.ValidString(markdown), "source must stay valid UTF-8")
			assert.True(t, utf8.ValidString(content), "content must stay valid UTF-8")
		})
	}
}

func TestGalleryImages_SkipsEntriesWithoutAServableBlob(t *testing.T) {
	record := map[string]any{
		"embed": imagesEmbed(
			map[string]any{
				"image": map[string]any{"$type": "blob", "mimeType": "image/png", "size": float64(1)},
				"alt":   "no ref",
			},
			galleryEntry("not-a-cid", "image/png", "bad cid"),
			galleryEntry(testImageCID0, "image/svg+xml", "svg"),
			galleryEntry(testImageCID0, "image/jpeg", "jpeg"),
			galleryEntry(testImageCID1, "image/webp", "webp"),
			galleryEntry(testImageCID2, "image/gif", "gif"),
		),
	}

	images := GalleryImages(record, testMedia)

	require.Len(t, images, 6, "one entry per gallery entry, in record order")
	for position := range 3 {
		assert.Empty(t, images[position].URL, "entry %d has no servable blob", position)
	}
	assert.Equal(t, GalleryImage{URL: testImageURL0, Alt: "jpeg"}, images[3])
	assert.Equal(t, GalleryImage{URL: testImageURL1, Alt: "webp"}, images[4])
	assert.Equal(t, GalleryImage{URL: testImageURL2, Alt: "gif"}, images[5])
}

func TestGalleryImages_ReadsImagesWithAttachmentGallery(t *testing.T) {
	record := map[string]any{
		"embed": map[string]any{
			"$type": "social.coves.embed.imagesWithAttachment",
			"images": map[string]any{
				"images": []any{galleryEntry(testImageCID1, "image/png", "nested")},
			},
			"attachment": map[string]any{
				"$type":    "social.coves.embed.external",
				"external": map[string]any{"uri": "https://example.com/guide"},
			},
		},
	}

	assert.Equal(t, []GalleryImage{{URL: testImageURL1, Alt: "nested"}}, GalleryImages(record, testMedia))
}

func TestGalleryImages_ZeroMediaSourceBuildsNoURLs(t *testing.T) {
	record := map[string]any{"embed": twoImageGallery()}

	images := GalleryImages(record, MediaSource{})

	require.Len(t, images, 2)
	assert.Empty(t, images[0].URL)
	assert.Empty(t, images[1].URL)
}

func TestBuildNote_PlacementAtSkippedGalleryEntryRendersCoveredText(t *testing.T) {
	embed := imagesEmbed(
		galleryEntry("not-a-cid", "image/png", "skipped"),
		galleryEntry(testImageCID1, "image/png", "two"),
	)
	markdown, content := buildTestNoteWithEmbed(t, "see pic and cat", []any{
		testFacet(4, 7, imageFeature(float64(0))),
		testFacet(12, 15, imageFeature(float64(1))),
	}, embed, testMedia)

	assert.Equal(t, "see pic and ![two]("+testImageURL1+")", markdown)
	assert.Equal(t, `<p>see pic and <img src="`+testImageURL1+`" alt="two"></p>`+"\n", content)
}

// A placed image between lines keeps the hard line breaks on both sides of it
// in Markdown and HTML alike (merge df320d0 kept the HTML text offset for
// hardLineBreaks while skipping text inside the image span).
func TestBuildNote_PlacedImageKeepsHardLineBreaks(t *testing.T) {
	markdown, content := buildTestNoteWithEmbed(t, "Intro\nalt\nMore",
		[]any{testFacet(6, 9, imageFeature(float64(0)))},
		imagesEmbed(galleryEntry(testImageCID0, "image/png", "alt")), testMedia)

	assert.Equal(t, "Intro \\\n![alt]("+testImageURL0+") \\\nMore", markdown)
	assert.Equal(t, "<p>Intro<br>\n"+`<img src="`+testImageURL0+`" alt="alt">`+"<br>\nMore</p>\n", content)
}
