package apobject

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Unplaced gallery images federate as attachments: on a Page the first becomes
// attachment[0] (Lemmy's image post) and the rest are appended to the body, or
// all of them when a link takes attachment[0]; on a Note they all go in
// attachment[] and the body is left alone.

const (
	testImageCID3 = "bafkreifyh5asaeftkul3uvl32iprfvoxkdu6vxgzu5sbrgyegasexzqmjm"
	testImageCID4 = "bafkreiencivqj7vts7guk4ervydleiuj6oabnhtcopzhbqvew67l2fiifq"
	testImageCID5 = "bafkreib65u3sdwngqsgjmnofnhc24i3ueexxgtvcyxqqrln4vpovww5t3u"
	testImageCID6 = "bafkreibdcxiwfuyg5bsiu2oc7mlz4v57o6mvhfomtvq7qn4m6exqcrxcku"
	testImageCID7 = "bafkreiaz4imuebllawckzizixj5qpvrthhrpxycs2esfl3c2iooly3auki"

	testImageURL3 = "https://img.example/img/content_full/plain/did:plc:abc/bafkreifyh5asaeftkul3uvl32iprfvoxkdu6vxgzu5sbrgyegasexzqmjm"
	testImageURL4 = "https://img.example/img/content_full/plain/did:plc:abc/bafkreiencivqj7vts7guk4ervydleiuj6oabnhtcopzhbqvew67l2fiifq"
	testImageURL5 = "https://img.example/img/content_full/plain/did:plc:abc/bafkreib65u3sdwngqsgjmnofnhc24i3ueexxgtvcyxqqrln4vpovww5t3u"
	testImageURL6 = "https://img.example/img/content_full/plain/did:plc:abc/bafkreibdcxiwfuyg5bsiu2oc7mlz4v57o6mvhfomtvq7qn4m6exqcrxcku"
	testImageURL7 = "https://img.example/img/content_full/plain/did:plc:abc/bafkreiaz4imuebllawckzizixj5qpvrthhrpxycs2esfl3c2iooly3auki"
)

// imagesWithAttachmentEmbed wraps a gallery and an attachment embed in a
// social.coves.embed.imagesWithAttachment embed.
func imagesWithAttachmentEmbed(attachment map[string]any, entries ...any) map[string]any {
	return map[string]any{
		"$type":      "social.coves.embed.imagesWithAttachment",
		"images":     map[string]any{"images": entries},
		"attachment": attachment,
	}
}

func externalAttachment(uri string) map[string]any {
	return map[string]any{
		"$type":    "social.coves.embed.external",
		"external": map[string]any{"uri": uri},
	}
}

// assertAttachment checks the object's whole "attachment" value against the
// expected wire JSON, or that the key is absent when expectedJSON is "".
func assertAttachment(t *testing.T, object map[string]any, expectedJSON string) {
	t.Helper()
	attachment, has := object["attachment"]
	if expectedJSON == "" {
		assert.False(t, has, "no attachment key expected, got %#v", attachment)
		return
	}
	require.True(t, has, "attachment key missing")
	actual, err := json.Marshal(attachment)
	require.NoError(t, err)
	assert.JSONEq(t, expectedJSON, string(actual))
}

func assertBody(t *testing.T, object map[string]any, expectedMarkdown, expectedHTML string) {
	t.Helper()
	source, ok := object["source"].(map[string]any)
	require.True(t, ok, "source must be an object")
	assert.Equal(t, expectedMarkdown, source["content"])
	assert.Equal(t, expectedHTML, object["content"])
}

func TestBuildPage_UnplacedGalleryImages(t *testing.T) {
	twoImages := []any{
		galleryEntry(testImageCID0, "image/png", "one"),
		galleryEntry(testImageCID1, "image/jpeg", "two"),
	}
	bothAppended := "hello\n\n![one](" + testImageURL0 + ")\n\n![two](" + testImageURL1 + ")"
	bothAppendedHTML := "<p>hello</p>\n" +
		`<p><img src="` + testImageURL0 + `" alt="one"></p>` + "\n" +
		`<p><img src="` + testImageURL1 + `" alt="two"></p>` + "\n"
	secondAppended := "hello\n\n![two](" + testImageURL1 + ")"
	secondAppendedHTML := "<p>hello</p>\n" + `<p><img src="` + testImageURL1 + `" alt="two"></p>` + "\n"
	firstImage := `[{"type":"Image","mediaType":"image/png","url":"` + testImageURL0 + `","name":"one"}]`

	tests := []struct {
		name               string
		content            string
		facets             []any
		embed              map[string]any
		expectedAttachment string // "" means no attachment key
		expectedMarkdown   string
		expectedHTML       string
	}{
		{
			name:               "single image with alt",
			content:            "hello",
			embed:              imagesEmbed(galleryEntry(testImageCID0, "image/png", "a photo")),
			expectedAttachment: `[{"type":"Image","mediaType":"image/png","url":"` + testImageURL0 + `","name":"a photo"}]`,
			expectedMarkdown:   "hello",
			expectedHTML:       "<p>hello</p>\n",
		},
		{
			name:               "single image with empty alt",
			content:            "hello",
			embed:              imagesEmbed(galleryEntry(testImageCID1, "image/jpeg", "")),
			expectedAttachment: `[{"type":"Image","mediaType":"image/jpeg","url":"` + testImageURL1 + `","name":""}]`,
			expectedMarkdown:   "hello",
			expectedHTML:       "<p>hello</p>\n",
		},
		{
			name:    "eight-image gallery",
			content: "gallery",
			embed: imagesEmbed(
				galleryEntry(testImageCID0, "image/png", "img 0"),
				galleryEntry(testImageCID1, "image/jpeg", "img 1"),
				galleryEntry(testImageCID2, "image/png", "img 2"),
				galleryEntry(testImageCID3, "image/jpeg", "img 3"),
				galleryEntry(testImageCID4, "image/png", "img 4"),
				galleryEntry(testImageCID5, "image/jpeg", "img 5"),
				galleryEntry(testImageCID6, "image/png", "img 6"),
				galleryEntry(testImageCID7, "image/jpeg", "img 7"),
			),
			expectedAttachment: `[{"type":"Image","mediaType":"image/png","url":"` + testImageURL0 + `","name":"img 0"}]`,
			expectedMarkdown: "gallery" +
				"\n\n![img 1](" + testImageURL1 + ")" +
				"\n\n![img 2](" + testImageURL2 + ")" +
				"\n\n![img 3](" + testImageURL3 + ")" +
				"\n\n![img 4](" + testImageURL4 + ")" +
				"\n\n![img 5](" + testImageURL5 + ")" +
				"\n\n![img 6](" + testImageURL6 + ")" +
				"\n\n![img 7](" + testImageURL7 + ")",
			expectedHTML: "<p>gallery</p>\n" +
				`<p><img src="` + testImageURL1 + `" alt="img 1"></p>` + "\n" +
				`<p><img src="` + testImageURL2 + `" alt="img 2"></p>` + "\n" +
				`<p><img src="` + testImageURL3 + `" alt="img 3"></p>` + "\n" +
				`<p><img src="` + testImageURL4 + `" alt="img 4"></p>` + "\n" +
				`<p><img src="` + testImageURL5 + `" alt="img 5"></p>` + "\n" +
				`<p><img src="` + testImageURL6 + `" alt="img 6"></p>` + "\n" +
				`<p><img src="` + testImageURL7 + `" alt="img 7"></p>` + "\n",
		},
		{
			name:    "appended image alt with brackets, backslash, line break and HTML metachars",
			content: "hello",
			embed: imagesEmbed(
				galleryEntry(testImageCID0, "image/png", "one"),
				galleryEntry(testImageCID1, "image/jpeg", "a [b] \\c\nd <x> & \"q\""),
			),
			expectedAttachment: firstImage,
			expectedMarkdown:   `hello` + "\n\n" + `![a \[b\] \\c d <x> & "q"](` + testImageURL1 + `)`,
			expectedHTML: "<p>hello</p>\n" +
				`<p><img src="` + testImageURL1 + `" alt="a [b] \c d &lt;x&gt; &amp; &#34;q&#34;"></p>` + "\n",
		},
		{
			name:               "link with http scheme keeps its Link and appends every image",
			content:            "hello",
			embed:              imagesWithAttachmentEmbed(externalAttachment("http://example.test/article"), twoImages...),
			expectedAttachment: `[{"type":"Link","href":"http://example.test/article"}]`,
			expectedMarkdown:   bothAppended,
			expectedHTML:       bothAppendedHTML,
		},
		{
			name:               "link with https scheme keeps its Link and appends every image",
			content:            "hello",
			embed:              imagesWithAttachmentEmbed(externalAttachment("https://example.test/article"), twoImages...),
			expectedAttachment: `[{"type":"Link","href":"https://example.test/article"}]`,
			expectedMarkdown:   bothAppended,
			expectedHTML:       bothAppendedHTML,
		},
		{
			name:             "javascript link yields no attachment and appends every image",
			content:          "hello",
			embed:            imagesWithAttachmentEmbed(externalAttachment("javascript:alert(1)"), twoImages...),
			expectedMarkdown: bothAppended,
			expectedHTML:     bothAppendedHTML,
		},
		{
			name:             "data link yields no attachment and appends every image",
			content:          "hello",
			embed:            imagesWithAttachmentEmbed(externalAttachment("data:text/html,<script>alert(1)</script>"), twoImages...),
			expectedMarkdown: bothAppended,
			expectedHTML:     bothAppendedHTML,
		},
		{
			name:               "link with one placed image appends only the unplaced one",
			content:            "see pic now",
			facets:             []any{testFacet(4, 7, imageFeature(float64(0)))},
			embed:              imagesWithAttachmentEmbed(externalAttachment("https://example.test/article"), twoImages...),
			expectedAttachment: `[{"type":"Link","href":"https://example.test/article"}]`,
			expectedMarkdown:   "see ![one](" + testImageURL0 + ") now\n\n![two](" + testImageURL1 + ")",
			expectedHTML: `<p>see <img src="` + testImageURL0 + `" alt="one"> now</p>` + "\n" +
				`<p><img src="` + testImageURL1 + `" alt="two"></p>` + "\n",
		},
		{
			name:    "placed image in a multi-line body keeps its hard breaks and the unplaced image is appended",
			content: "Intro\nalt\nMore",
			facets:  []any{testFacet(6, 9, imageFeature(float64(0)))},
			embed: imagesWithAttachmentEmbed(externalAttachment("https://example.test/article"),
				galleryEntry(testImageCID0, "image/png", "alt"),
				galleryEntry(testImageCID1, "image/jpeg", "two"),
			),
			expectedAttachment: `[{"type":"Link","href":"https://example.test/article"}]`,
			expectedMarkdown:   "Intro \\\n![alt](" + testImageURL0 + ") \\\nMore\n\n![two](" + testImageURL1 + ")",
			expectedHTML: "<p>Intro<br>\n" + `<img src="` + testImageURL0 + `" alt="alt">` + "<br>\nMore</p>\n" +
				`<p><img src="` + testImageURL1 + `" alt="two"></p>` + "\n",
		},
		{
			name:    "quoted post is dropped and images render as a plain gallery",
			content: "hello",
			embed: imagesWithAttachmentEmbed(map[string]any{
				"$type": "social.coves.embed.post",
				"post": map[string]any{
					"uri": "at://did:plc:quoted/social.coves.community.postv2/rq",
					"cid": testImageCID5,
				},
			}, twoImages...),
			expectedAttachment: firstImage,
			expectedMarkdown:   secondAppended,
			expectedHTML:       secondAppendedHTML,
		},
		{
			name:    "one placed and two unplaced",
			content: "see pic now",
			facets:  []any{testFacet(4, 7, imageFeature(float64(1)))},
			embed: imagesEmbed(
				galleryEntry(testImageCID0, "image/png", "one"),
				galleryEntry(testImageCID1, "image/jpeg", "two"),
				galleryEntry(testImageCID2, "image/png", "three"),
			),
			expectedAttachment: firstImage,
			expectedMarkdown:   "see ![two](" + testImageURL1 + ") now\n\n![three](" + testImageURL2 + ")",
			expectedHTML: `<p>see <img src="` + testImageURL1 + `" alt="two"> now</p>` + "\n" +
				`<p><img src="` + testImageURL2 + `" alt="three"></p>` + "\n",
		},
		{
			name:    "placement wholly inside a code facet is filtered and its image stays unplaced",
			content: "see pic now",
			facets: []any{
				testFacet(4, 7, codeFeature()),
				testFacet(4, 7, imageFeature(float64(0))),
			},
			embed:              imagesEmbed(twoImages...),
			expectedAttachment: firstImage,
			expectedMarkdown:   "see `pic` now\n\n![two](" + testImageURL1 + ")",
			expectedHTML: "<p>see <code>pic</code> now</p>\n" +
				`<p><img src="` + testImageURL1 + `" alt="two"></p>` + "\n",
		},
		{
			name:    "skipped blob at index 0 is never sent",
			content: "see pic now",
			facets:  []any{testFacet(4, 7, imageFeature(float64(2)))},
			embed: imagesEmbed(
				galleryEntry("not-a-cid", "image/png", "skipped"),
				galleryEntry(testImageCID1, "image/jpeg", "two"),
				galleryEntry(testImageCID2, "image/png", "three"),
			),
			expectedAttachment: `[{"type":"Image","mediaType":"image/jpeg","url":"` + testImageURL1 + `","name":"two"}]`,
			expectedMarkdown:   "see ![three](" + testImageURL2 + ") now",
			expectedHTML:       `<p>see <img src="` + testImageURL2 + `" alt="three"> now</p>` + "\n",
		},
		{
			name:               "empty text with two images",
			content:            "",
			embed:              imagesEmbed(twoImages...),
			expectedAttachment: firstImage,
			expectedMarkdown:   "![two](" + testImageURL1 + ")",
			expectedHTML:       `<p><img src="` + testImageURL1 + `" alt="two"></p>` + "\n",
		},
		{
			name:    "all images placed carries no attachment",
			content: "see pic and cat",
			facets: []any{
				testFacet(4, 7, imageFeature(float64(0))),
				testFacet(12, 15, imageFeature(float64(1))),
			},
			embed:            imagesEmbed(twoImages...),
			expectedMarkdown: "see ![one](" + testImageURL0 + ") and ![two](" + testImageURL1 + ")",
			expectedHTML: `<p>see <img src="` + testImageURL0 + `" alt="one"> and ` +
				`<img src="` + testImageURL1 + `" alt="two"></p>` + "\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := map[string]any{
				"title":   "a post",
				"content": test.content,
				"embed":   test.embed,
			}
			if test.facets != nil {
				record["facets"] = test.facets
			}
			page, err := BuildPage(
				"https://coves.social/ap/actor/did:plc:abc",
				"https://lemmy.world/c/tech",
				"https://coves.social/ap/object/did:plc:abc/social.coves.community.postv2/rk",
				testMedia, record)
			require.NoError(t, err)

			assertAttachment(t, page, test.expectedAttachment)
			assertBody(t, page, test.expectedMarkdown, test.expectedHTML)
		})
	}
}

func TestBuildNote_UnplacedGalleryImagesAreAttachments(t *testing.T) {
	twoImages := imagesEmbed(
		galleryEntry(testImageCID0, "image/png", "one"),
		galleryEntry(testImageCID1, "image/jpeg", "two"),
	)

	tests := []struct {
		name               string
		content            string
		facets             []any
		embed              map[string]any
		expectedAttachment string // "" means no attachment key
		expectedMarkdown   string
		expectedHTML       string
	}{
		{
			name:    "two unplaced images in gallery order, body untouched",
			content: "hello",
			embed:   twoImages,
			expectedAttachment: `[` +
				`{"type":"Image","mediaType":"image/png","url":"` + testImageURL0 + `","name":"one"},` +
				`{"type":"Image","mediaType":"image/jpeg","url":"` + testImageURL1 + `","name":"two"}]`,
			expectedMarkdown: "hello",
			expectedHTML:     "<p>hello</p>\n",
		},
		{
			name:               "image-only comment",
			content:            "",
			embed:              imagesEmbed(galleryEntry(testImageCID1, "image/jpeg", "")),
			expectedAttachment: `[{"type":"Image","mediaType":"image/jpeg","url":"` + testImageURL1 + `","name":""}]`,
			expectedMarkdown:   "",
			expectedHTML:       "<p></p>\n",
		},
		{
			name:               "placed image stays inline and only the unplaced one is attached",
			content:            "see pic now",
			facets:             []any{testFacet(4, 7, imageFeature(float64(0)))},
			embed:              twoImages,
			expectedAttachment: `[{"type":"Image","mediaType":"image/jpeg","url":"` + testImageURL1 + `","name":"two"}]`,
			expectedMarkdown:   "see ![one](" + testImageURL0 + ") now",
			expectedHTML:       `<p>see <img src="` + testImageURL0 + `" alt="one"> now</p>` + "\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := map[string]any{
				"content": test.content,
				"embed":   test.embed,
			}
			if test.facets != nil {
				record["facets"] = test.facets
			}
			note := BuildNote("actor", "community", "parent", "object", testMedia, record)

			assertAttachment(t, note, test.expectedAttachment)
			assertBody(t, note, test.expectedMarkdown, test.expectedHTML)
		})
	}
}
