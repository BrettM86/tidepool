package materialize

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"image/color"
	"image/gif"
	"image/jpeg"
	"image/png"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const aspectPostID = "https://lemmy.world/post/70301"

type aspectImage struct {
	Image       json.RawMessage `json:"image"`
	Alt         string          `json:"alt"`
	AspectRatio *struct {
		Width  int `json:"width"`
		Height int `json:"height"`
	} `json:"aspectRatio"`
}

func pngFixture(t *testing.T, width, height int) []byte {
	t.Helper()
	var buffer bytes.Buffer
	require.NoError(t, png.Encode(&buffer, filledImage(width, height, color.NRGBA{R: 10, G: 20, B: 30, A: 255})))
	return buffer.Bytes()
}

func jpegFixture(t *testing.T, width, height int) []byte {
	t.Helper()
	var buffer bytes.Buffer
	require.NoError(t, jpeg.Encode(&buffer, filledImage(width, height, color.NRGBA{R: 200, G: 100, B: 50, A: 255}), nil))
	return buffer.Bytes()
}

func gifFixture(t *testing.T, width, height int) []byte {
	t.Helper()
	var buffer bytes.Buffer
	require.NoError(t, gif.Encode(&buffer, filledImage(width, height, color.NRGBA{R: 0, G: 0, B: 255, A: 255}), nil))
	return buffer.Bytes()
}

// webpLosslessHeader is a VP8L WebP holding only its header: enough for a
// DecodeConfig, which is all the materializer reads.
func webpLosslessHeader(width, height int) []byte {
	bits := uint32(width-1) | uint32(height-1)<<14
	payload := []byte{0x2f, byte(bits), byte(bits >> 8), byte(bits >> 16), byte(bits >> 24)}
	chunk := append([]byte("VP8L"), binary.LittleEndian.AppendUint32(nil, uint32(len(payload)))...)
	chunk = append(chunk, payload...)
	if len(payload)%2 == 1 {
		chunk = append(chunk, 0)
	}
	out := append([]byte("RIFF"), binary.LittleEndian.AppendUint32(nil, uint32(4+len(chunk)))...)
	out = append(out, "WEBP"...)
	return append(out, chunk...)
}

// exifFixture describes the APP1 EXIF segment withEXIF inserts.
type exifFixture struct {
	orientation uint16
	bigEndian   bool
	// otherTagsFirst puts Make and Model entries before Orientation, as
	// cameras write them.
	otherTagsFirst bool
	// count overrides the Orientation entry's value count (default 1).
	count uint32
	// jfifFirst puts an APP0 JFIF segment before APP1, and fillBytes puts
	// 0xff fill bytes before the APP1 marker.
	jfifFirst bool
	fillBytes int
}

// withEXIF inserts an APP1 EXIF segment carrying an Orientation tag right
// after the JPEG's SOI marker.
func withEXIF(t *testing.T, data []byte, fixture exifFixture) []byte {
	t.Helper()
	require.True(t, bytes.HasPrefix(data, []byte{0xff, 0xd8}), "fixture: must be a JPEG")
	var order binary.AppendByteOrder = binary.LittleEndian
	tiff := []byte("II*\x00")
	if fixture.bigEndian {
		order = binary.BigEndian
		tiff = []byte("MM\x00*")
	}
	count := fixture.count
	if count == 0 {
		count = 1
	}
	type entry struct {
		tag, kind uint16
		count     uint32
		value     uint32
	}
	var entries []entry
	if fixture.otherTagsFirst {
		// ASCII values longer than four bytes would live out of line; these
		// point past the IFD, which the parser never follows.
		entries = append(entries, entry{0x010f, 2, 6, 0x40}, entry{0x0110, 2, 6, 0x48})
	}
	entries = append(entries, entry{0x0112, 3, count, uint32(fixture.orientation)})

	tiff = order.AppendUint32(tiff, 8) // IFD0 offset
	tiff = order.AppendUint16(tiff, uint16(len(entries)))
	for _, e := range entries {
		tiff = order.AppendUint16(tiff, e.tag)
		tiff = order.AppendUint16(tiff, e.kind)
		tiff = order.AppendUint32(tiff, e.count)
		if e.kind == 3 {
			tiff = order.AppendUint16(tiff, uint16(e.value))
			tiff = append(tiff, 0, 0) // value padding
		} else {
			tiff = order.AppendUint32(tiff, e.value)
		}
	}
	tiff = order.AppendUint32(tiff, 0) // no next IFD
	segment := append([]byte("Exif\x00\x00"), tiff...)

	out := []byte{0xff, 0xd8}
	if fixture.jfifFirst {
		jfif := []byte("JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00")
		out = append(out, 0xff, 0xe0)
		out = binary.BigEndian.AppendUint16(out, uint16(len(jfif)+2))
		out = append(out, jfif...)
	}
	out = append(out, bytes.Repeat([]byte{0xff}, fixture.fillBytes)...)
	out = append(out, 0xff, 0xe1)
	out = binary.BigEndian.AppendUint16(out, uint16(len(segment)+2))
	out = append(out, segment...)
	return append(out, data[2:]...)
}

// servedMedia is one file the test serves under https://lemmy.world/media/.
type servedMedia struct {
	contentType string
	data        func(t *testing.T) []byte
}

// rotatedJPEG serves a 160x40 JPEG with the given EXIF segment.
func rotatedJPEG(fixture exifFixture) servedMedia {
	return servedMedia{"image/jpeg", func(t *testing.T) []byte {
		return withEXIF(t, jpegFixture(t, 160, 40), fixture)
	}}
}

// TestImageEmbedAspectRatio: every bridged post image records the width and
// height of the bytes stored, as displayed (EXIF rotation applied), so Coves
// clients size the frame before the image loads. An image whose dimensions
// cannot be read still lands, without an aspectRatio.
func TestImageEmbedAspectRatio(t *testing.T) {
	type wantImage struct {
		width, height int // 0 means no aspectRatio
	}
	cases := []struct {
		name string
		// media maps a served path under https://lemmy.world/media/ to its
		// content type and bytes.
		media map[string]servedMedia
		// attachments are the paths the Page attaches as Images, in order;
		// thumbnail is a path served only as the Page's image.
		attachments []string
		thumbnail   string
		want        []wantImage
	}{
		{
			name:        "tall PNG",
			media:       map[string]servedMedia{"tall.png": {"image/png", func(t *testing.T) []byte { return pngFixture(t, 300, 900) }}},
			attachments: []string{"tall.png"},
			want:        []wantImage{{300, 900}},
		},
		{
			name: "gallery of JPEG, GIF and WebP keeps each image's own ratio",
			media: map[string]servedMedia{
				"wide.jpg":    {"image/jpeg", func(t *testing.T) []byte { return jpegFixture(t, 120, 80) }},
				"square.gif":  {"image/gif", func(t *testing.T) []byte { return gifFixture(t, 50, 50) }},
				"narrow.webp": {"image/webp", func(t *testing.T) []byte { return webpLosslessHeader(640, 1600) }},
			},
			attachments: []string{"wide.jpg", "square.gif", "narrow.webp"},
			want:        []wantImage{{120, 80}, {50, 50}, {640, 1600}},
		},
		{
			name:        "JPEG rotated 90 degrees by EXIF reports displayed dimensions",
			media:       map[string]servedMedia{"rotated.jpg": rotatedJPEG(exifFixture{orientation: 6})},
			attachments: []string{"rotated.jpg"},
			want:        []wantImage{{40, 160}},
		},
		{
			name:        "big-endian EXIF orientation 8 also swaps",
			media:       map[string]servedMedia{"rotated.jpg": rotatedJPEG(exifFixture{orientation: 8, bigEndian: true})},
			attachments: []string{"rotated.jpg"},
			want:        []wantImage{{40, 160}},
		},
		{
			name: "transposed and transverse orientations 5 and 7 swap",
			media: map[string]servedMedia{
				"five.jpg":  rotatedJPEG(exifFixture{orientation: 5}),
				"seven.jpg": rotatedJPEG(exifFixture{orientation: 7}),
			},
			attachments: []string{"five.jpg", "seven.jpg"},
			want:        []wantImage{{40, 160}, {40, 160}},
		},
		{
			name: "EXIF orientations 3 and 4 keep dimensions",
			media: map[string]servedMedia{
				"three.jpg": rotatedJPEG(exifFixture{orientation: 3}),
				"four.jpg":  rotatedJPEG(exifFixture{orientation: 4}),
			},
			attachments: []string{"three.jpg", "four.jpg"},
			want:        []wantImage{{160, 40}, {160, 40}},
		},
		{
			name: "camera layout: JFIF segment, fill bytes and Make/Model before Orientation",
			media: map[string]servedMedia{"camera.jpg": rotatedJPEG(exifFixture{
				orientation: 6, otherTagsFirst: true, jfifFirst: true, fillBytes: 3,
			})},
			attachments: []string{"camera.jpg"},
			want:        []wantImage{{40, 160}},
		},
		{
			name:        "malformed Orientation entry omits the ratio rather than guess",
			media:       map[string]servedMedia{"bad.jpg": rotatedJPEG(exifFixture{orientation: 6, count: 2})},
			attachments: []string{"bad.jpg"},
			want:        []wantImage{{0, 0}},
		},
		{
			name: "AVIF records the transcoded image's dimensions",
			media: map[string]servedMedia{"picture.avif": {"image/avif", func(t *testing.T) []byte {
				return encodeAVIFFixture(t, filledImage(64, 48, color.NRGBA{R: 1, G: 2, B: 3, A: 255}), avifFixtureOptions)
			}}},
			attachments: []string{"picture.avif"},
			want:        []wantImage{{64, 48}},
		},
		{
			name:        "unreadable dimensions keep the image without a ratio",
			media:       map[string]servedMedia{"broken.png": {"image/png", func(t *testing.T) []byte { return []byte("not really a png") }}},
			attachments: []string{"broken.png"},
			want:        []wantImage{{0, 0}},
		},
		{
			name:      "thumbnail-only image post",
			media:     map[string]servedMedia{"thumb.jpg": {"image/jpeg", func(t *testing.T) []byte { return jpegFixture(t, 90, 270) }}},
			thumbnail: "thumb.jpg",
			want:      []wantImage{{90, 270}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			ctx := context.Background()
			h.serveLemmyWorldFixtures()
			for path, served := range tc.media {
				data := served.data(t)
				contentType := served.contentType
				h.mux.HandleFunc("GET /media/"+path, func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", contentType)
					_, _ = w.Write(data)
				})
			}

			post := page(aspectPostID, personID, groupID, "an image post", "2026-11-09T10:00:00.000000Z")
			var attachments []any
			for _, path := range tc.attachments {
				attachments = append(attachments, map[string]any{
					"type": "Image", "href": "https://lemmy.world/media/" + path, "name": "alt for " + path,
				})
			}
			if attachments != nil {
				post["attachment"] = attachments
			}
			if tc.thumbnail != "" {
				post["image"] = map[string]any{"type": "Image", "url": "https://lemmy.world/media/" + tc.thumbnail}
			}
			_, err := h.m.MaterializePost(ctx, mustObject(t, post), groupID)
			require.NoError(t, err)

			record := h.recordFor(t, aspectPostID)
			require.Contains(t, record, "embed")
			raw, err := json.Marshal(record["embed"])
			require.NoError(t, err)
			var embed struct {
				Type   string        `json:"$type"`
				Images []aspectImage `json:"images"`
			}
			require.NoError(t, json.Unmarshal(raw, &embed))
			assert.Equal(t, "social.coves.embed.images", embed.Type)
			require.Len(t, embed.Images, len(tc.want))
			for i, want := range tc.want {
				got := embed.Images[i]
				assert.NotEmpty(t, got.Image, "image %d keeps its blob", i)
				if i < len(tc.attachments) {
					assert.Equal(t, "alt for "+tc.attachments[i], got.Alt, "image %d keeps its alt", i)
				}
				if want.width == 0 {
					assert.Nil(t, got.AspectRatio, "image %d must carry no aspectRatio", i)
					continue
				}
				require.NotNil(t, got.AspectRatio, "image %d must carry an aspectRatio", i)
				assert.Equal(t, want.width, got.AspectRatio.Width, "image %d width", i)
				assert.Equal(t, want.height, got.AspectRatio.Height, "image %d height", i)
			}
		})
	}
}

// TestEXIFOrientationMalformedSegments: a truncated or nonsense EXIF
// segment reads as malformed instead of panicking, and a JPEG with no EXIF
// reads as unrotated.
func TestEXIFOrientationMalformedSegments(t *testing.T) {
	base := jpegFixture(t, 16, 8)
	valid := withEXIF(t, base, exifFixture{orientation: 6})
	tiff := valid[2+4+6:] // past SOI, the APP1 header and "Exif\x00\x00"

	t.Run("valid", func(t *testing.T) {
		orientation, ok := jpegOrientation(valid)
		assert.True(t, ok)
		assert.Equal(t, 6, orientation)
	})
	t.Run("no exif", func(t *testing.T) {
		orientation, ok := jpegOrientation(base)
		assert.True(t, ok)
		assert.Equal(t, 1, orientation)
	})

	malformedJPEG := map[string][]byte{
		"not a jpeg":              []byte("GIF89a"),
		"truncated after header":  valid[:2+4+6+4],
		"truncated inside ifd":    valid[:2+4+6+12],
		"only fill bytes":         {0xff, 0xd8, 0xff, 0xff, 0xff},
		"segment length too long": append(append([]byte{0xff, 0xd8, 0xff, 0xe1, 0xff, 0xff}, "Exif\x00\x00"...), 0),
		"bad byte order":          append(append([]byte{0xff, 0xd8, 0xff, 0xe1, 0x00, 0x10}, "Exif\x00\x00"...), "XX\x00*\x00\x00\x00\x08"...),
	}
	for name, data := range malformedJPEG {
		t.Run(name, func(t *testing.T) {
			_, ok := jpegOrientation(data)
			assert.False(t, ok)
		})
	}

	malformedTIFF := map[string][]byte{
		"short header":           tiff[:6],
		"ifd offset past end":    append([]byte("II*\x00"), 0xff, 0xff, 0xff, 0x7f),
		"ifd offset near uint32": append([]byte("II*\x00"), 0xfe, 0xff, 0xff, 0xff),
		"entry past end":         tiff[:8+2+6],
	}
	for name, data := range malformedTIFF {
		t.Run("tiff "+name, func(t *testing.T) {
			_, ok := tiffOrientation(data)
			assert.False(t, ok)
		})
	}
}
