package materialize

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
	"testing"

	"github.com/gen2brain/avif"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
)

const (
	avifPostID   = "https://lemmy.world/post/70201"
	avifActorID  = "https://lemmy.world/u/avifactor"
	avifMediaURL = "https://lemmy.world/media/picture.avif"
)

// avifFixtureOptions keep fixture encodes fast. Passing any Options drops
// Encode's 4:2:0 default and the zero ChromaSubsampling is 4:4:4, so the
// subsampling is spelled out.
var avifFixtureOptions = avif.Options{
	Quality:           60,
	QualityAlpha:      100,
	Speed:             10,
	ChromaSubsampling: image.YCbCrSubsampleRatio444,
}

// recordingLogHandler keeps every record the materializer logs.
type recordingLogHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (r *recordingLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (r *recordingLogHandler) Handle(_ context.Context, record slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, record.Clone())
	return nil
}

func (r *recordingLogHandler) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *recordingLogHandler) WithGroup(string) slog.Handler      { return r }

func (r *recordingLogHandler) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.records)
}

// warnedAboutURL reports whether a WARN record logged after the first since
// records carries a url attr equal to url.
func (r *recordingLogHandler) warnedAboutURL(url string, since int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, record := range r.records[since:] {
		if record.Level != slog.LevelWarn {
			continue
		}
		found := false
		record.Attrs(func(attr slog.Attr) bool {
			if attr.Key == "url" && attr.Value.String() == url {
				found = true
				return false
			}
			return true
		})
		if found {
			return true
		}
	}
	return false
}

func encodeAVIFFixture(t *testing.T, img image.Image, options avif.Options) []byte {
	t.Helper()
	var buffer bytes.Buffer
	require.NoError(t, avif.Encode(&buffer, img, options), "fixture: avif.Encode")
	return buffer.Bytes()
}

func filledImage(width, height int, fill color.NRGBA) *image.NRGBA {
	img := image.NewNRGBA(image.Rect(0, 0, width, height))
	for i := 0; i < len(img.Pix); i += 4 {
		img.Pix[i], img.Pix[i+1], img.Pix[i+2], img.Pix[i+3] = fill.R, fill.G, fill.B, fill.A
	}
	return img
}

// halfTransparentImage is opaque on its right half and fully transparent on
// its left half.
func halfTransparentImage(width, height int) *image.NRGBA {
	img := filledImage(width, height, color.NRGBA{R: 30, G: 120, B: 220, A: 255})
	for y := 0; y < height; y++ {
		for x := 0; x < width/2; x++ {
			img.SetNRGBA(x, y, color.NRGBA{})
		}
	}
	return img
}

// translucentNoiseImage is deterministic colour noise with alpha in 64..191.
func translucentNoiseImage(width, height int, seed uint64) *image.NRGBA {
	random := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	img := image.NewNRGBA(image.Rect(0, 0, width, height))
	for i := 0; i < len(img.Pix); i += 4 {
		img.Pix[i] = uint8(random.UintN(256))
		img.Pix[i+1] = uint8(random.UintN(256))
		img.Pix[i+2] = uint8(random.UintN(256))
		img.Pix[i+3] = uint8(64 + random.UintN(128))
	}
	return img
}

// withMajorBrand rewrites the leading ftyp box's major brand, leaving the
// compatible brands (which still list avif) untouched.
func withMajorBrand(t *testing.T, data []byte, brand string) []byte {
	t.Helper()
	require.Equal(t, "ftyp", string(data[4:8]), "fixture: file must open with ftyp")
	ftypEnd := int(binary.BigEndian.Uint32(data[0:4]))
	compatible := false
	for offset := 16; offset+4 <= ftypEnd; offset += 4 {
		if string(data[offset:offset+4]) == "avif" {
			compatible = true
		}
	}
	require.True(t, compatible, "fixture: compatible brands must still list avif")
	patched := bytes.Clone(data)
	copy(patched[8:12], brand)
	return patched
}

// childBox finds the first box of boxType laid end to end in data[start:end],
// returning its bounds. Only 32-bit box sizes are walked.
func childBox(t *testing.T, data []byte, start, end int, boxType string) (boxStart, boxEnd int) {
	t.Helper()
	for offset := start; offset+8 <= end; {
		size := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		require.GreaterOrEqual(t, size, 8, "fixture: box at %d has an unsupported size", offset)
		if string(data[offset+4:offset+8]) == boxType {
			return offset, offset + size
		}
		offset += size
	}
	t.Fatalf("fixture: no %s box", boxType)
	return 0, 0
}

// withEmptyPrimaryItem replaces meta's pitm box with one that has no payload
// and shrinks meta to match — gen2brain/avif v0.6.0's DecodeConfig panics on it.
func withEmptyPrimaryItem(t *testing.T, data []byte) []byte {
	t.Helper()
	metaStart, metaEnd := childBox(t, data, 0, len(data), "meta")
	// meta is a FullBox: version and flags precede its children.
	pitmStart, pitmEnd := childBox(t, data, metaStart+12, metaEnd, "pitm")
	emptyPitm := []byte{0, 0, 0, 8, 'p', 'i', 't', 'm'}
	removed := (pitmEnd - pitmStart) - len(emptyPitm)

	patched := make([]byte, 0, len(data)-removed)
	patched = append(patched, data[:pitmStart]...)
	patched = append(patched, emptyPitm...)
	patched = append(patched, data[pitmEnd:]...)
	binary.BigEndian.PutUint32(patched[metaStart:], uint32(metaEnd-metaStart-removed))
	return patched
}

// assertStoredImage checks a record blob ref and the bytes stored under it:
// both carry wantMime, and the bytes decode as that format at the given size.
func assertStoredImage(t *testing.T, h *harness, did string, rawRef json.RawMessage, wantMime string, wantWidth, wantHeight int) {
	t.Helper()
	var ref struct {
		Type     string `json:"$type"`
		MimeType string `json:"mimeType"`
		Size     int64  `json:"size"`
		Ref      struct {
			Link string `json:"$link"`
		} `json:"ref"`
	}
	require.NoError(t, json.Unmarshal(rawRef, &ref))
	assert.Equal(t, "blob", ref.Type)
	assert.Equal(t, wantMime, ref.MimeType, "record blob ref mimeType")

	data, storedMime, err := h.manager.GetBlob(context.Background(), did, ref.Ref.Link)
	require.NoError(t, err)
	assert.Equal(t, wantMime, storedMime, "stored blob mime type")
	assert.Equal(t, int64(len(data)), ref.Size, "record blob ref size matches the stored bytes")

	var decoded image.Image
	switch wantMime {
	case "image/jpeg":
		require.True(t, bytes.HasPrefix(data, []byte{0xff, 0xd8}),
			"stored bytes must be a real JPEG (FF D8), got prefix % x", data[:min(len(data), 12)])
		decoded, err = jpeg.Decode(bytes.NewReader(data))
	case "image/png":
		decoded, err = png.Decode(bytes.NewReader(data))
	default:
		t.Fatalf("unexpected wantMime %q", wantMime)
	}
	require.NoError(t, err, "stored bytes must decode as %s", wantMime)
	assert.Equal(t, image.Rect(0, 0, wantWidth, wantHeight), decoded.Bounds(), "stored image dimensions")
}

// TestAVIFMediaTranscodedOnStore: AVIF media is never stored as AVIF — the
// lexicon slots accept png/jpeg/webp(/gif) only. It is detected from the
// Content-Type (whatever its case or parameters) or from the bytes (a server
// that mislabels it), transcoded to JPEG or, when it carries transparency, to
// PNG, and the record ref and the stored blob both carry the transcoder's
// type. The slot's own size cap applies to the transcoded output, and an
// AVIF that fails to decode is omitted while the record still lands.
func TestAVIFMediaTranscodedOnStore(t *testing.T) {
	cases := []struct {
		name string
		// profile: the media is the actor's icon (and, with servesBanner,
		// also its banner); otherwise it is a post's image attachment.
		profile      bool
		servesBanner bool
		// attachmentType is the post attachment's AS2 type; "" means Image.
		attachmentType string
		media          func(t *testing.T) []byte
		contentType    string
		// wantMime is the embed image (post) or avatar (profile) type; ""
		// means the slot must be omitted.
		wantMime              string
		wantBannerMime        string
		wantWidth, wantHeight int
		// wantOmittedWarning: a direct fetch of the URL yields (nil, nil)
		// and logs a WARN carrying the url.
		wantOmittedWarning bool
	}{
		{
			// A mixed-case Content-Type with parameters on an AVIF whose
			// major brand is mif1 (avif only among the compatible brands)
			// is still transcoded end to end.
			name: "parameterised mixed-case AVIF Content-Type on a compatible-brand AVIF",
			media: func(t *testing.T) []byte {
				encoded := encodeAVIFFixture(t, filledImage(64, 48, color.NRGBA{R: 200, G: 80, B: 40, A: 255}), avifFixtureOptions)
				return withMajorBrand(t, encoded, "mif1")
			},
			contentType: "Image/AVIF; charset=binary",
			wantMime:    "image/jpeg", wantWidth: 64, wantHeight: 48,
		},
		{
			name: "AVIF bytes mislabeled image/jpeg",
			media: func(t *testing.T) []byte {
				return encodeAVIFFixture(t, filledImage(40, 30, color.NRGBA{R: 20, G: 160, B: 90, A: 255}), avifFixtureOptions)
			},
			contentType: "image/jpeg",
			wantMime:    "image/jpeg", wantWidth: 40, wantHeight: 30,
		},
		{
			// The ftyp names mif1 as its major brand and avif only among the
			// compatible brands, and the server calls it a JPEG: detection
			// must read the compatible brands.
			name: "AVIF with a non-avif major brand mislabeled image/jpeg",
			media: func(t *testing.T) []byte {
				encoded := encodeAVIFFixture(t, filledImage(48, 36, color.NRGBA{R: 140, G: 60, B: 180, A: 255}), avifFixtureOptions)
				return withMajorBrand(t, encoded, "mif1")
			},
			contentType: "image/jpeg",
			wantMime:    "image/jpeg", wantWidth: 48, wantHeight: 36,
		},
		{
			// A Link attachment with no mediaType is an image only by its
			// href's extension, and .avif must count as one.
			name:           "attachment identified as an image only by its .avif extension",
			attachmentType: "Link",
			media: func(t *testing.T) []byte {
				return encodeAVIFFixture(t, filledImage(56, 40, color.NRGBA{R: 60, G: 130, B: 210, A: 255}), avifFixtureOptions)
			},
			contentType: "image/avif",
			wantMime:    "image/jpeg", wantWidth: 56, wantHeight: 40,
		},
		{
			name:    "avatar with transparency",
			profile: true,
			media: func(t *testing.T) []byte {
				return encodeAVIFFixture(t, halfTransparentImage(32, 24), avifFixtureOptions)
			},
			contentType: "image/avif",
			wantMime:    "image/png", wantWidth: 32, wantHeight: 24,
		},
		{
			// 600×600 translucent noise: the AVIF download is about
			// 115 KB, under the 1,000,000-byte avatar cap, but its PNG
			// transcode is about 1.4 MB — over the avatar cap, under the
			// 2,000,000-byte banner cap. The same bytes as the banner prove
			// each slot passes its own limit to the transcoder.
			name:         "avatar whose transcode exceeds the avatar cap",
			profile:      true,
			servesBanner: true,
			media: func(t *testing.T) []byte {
				encoded := encodeAVIFFixture(t, translucentNoiseImage(600, 600, 4), avif.Options{
					Quality:           30,
					QualityAlpha:      30,
					Speed:             10,
					ChromaSubsampling: image.YCbCrSubsampleRatio420,
				})
				require.Less(t, len(encoded), 1_000_000, "fixture: the download must fit the avatar cap")
				return encoded
			},
			contentType:    "image/avif",
			wantMime:       "",
			wantBannerMime: "image/png", wantWidth: 600, wantHeight: 600,
		},
		{
			// Labelled image/jpeg so that without byte detection the bytes
			// would be stored as a JPEG: only AVIF detection plus a failed
			// decode omits them.
			name: "malformed AVIF with an empty pitm box",
			media: func(t *testing.T) []byte {
				encoded := encodeAVIFFixture(t, filledImage(16, 16, color.NRGBA{R: 90, G: 90, B: 90, A: 255}), avifFixtureOptions)
				return withEmptyPrimaryItem(t, encoded)
			},
			contentType:        "image/jpeg",
			wantMime:           "",
			wantOmittedWarning: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			logs := &recordingLogHandler{}
			h := newHarness(t, func(o *Options) { o.Logger = slog.New(logs) })
			ctx := context.Background()
			media := tc.media(t)
			h.mux.HandleFunc("GET /media/picture.avif", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", tc.contentType)
				_, _ = w.Write(media)
			})

			var did string
			var record map[string]any
			if tc.profile {
				extra := map[string]any{"icon": map[string]any{"type": "Image", "url": avifMediaURL}}
				if tc.servesBanner {
					extra["image"] = map[string]any{"type": "Image", "url": avifMediaURL}
				}
				h.serveObject("/u/avifactor", person(avifActorID, "avifactor", extra))
				_, err := h.m.EnsureActor(ctx, &ap.Object{ID: avifActorID})
				require.NoError(t, err, "media outcomes must not block the profile")

				did = testDIDFor("avifactor", "lemmy.world")
				record, _, err = h.manager.GetRecord(ctx, did, CollectionActorProfile, ProfileRKey)
				require.NoError(t, err)

				if tc.wantMime == "" {
					assert.NotContains(t, record, "avatar", "the avatar must be omitted")
				} else {
					require.Contains(t, record, "avatar", "the avatar must be stored")
					raw, err := json.Marshal(record["avatar"])
					require.NoError(t, err)
					assertStoredImage(t, h, did, raw, tc.wantMime, tc.wantWidth, tc.wantHeight)
				}
				if tc.wantBannerMime == "" {
					assert.NotContains(t, record, "banner", "no banner is expected")
				} else {
					require.Contains(t, record, "banner", "the banner must be stored")
					raw, err := json.Marshal(record["banner"])
					require.NoError(t, err)
					assertStoredImage(t, h, did, raw, tc.wantBannerMime, tc.wantWidth, tc.wantHeight)
				}
			} else {
				h.serveLemmyWorldFixtures()
				post := page(avifPostID, personID, groupID, "an avif post", "2026-10-20T10:00:00.000000Z")
				attachmentType := tc.attachmentType
				if attachmentType == "" {
					attachmentType = "Image"
				}
				post["attachment"] = []any{map[string]any{"type": attachmentType, "href": avifMediaURL, "name": "a picture"}}
				_, err := h.m.MaterializePost(ctx, mustObject(t, post), groupID)
				require.NoError(t, err, "media outcomes must not block the post")

				did = testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
				record = h.recordFor(t, avifPostID)

				if tc.wantMime == "" {
					assert.NotContains(t, record, "embed", "the image must be omitted")
				} else {
					require.Contains(t, record, "embed", "the image embed must be stored")
					raw, err := json.Marshal(record["embed"])
					require.NoError(t, err)
					var embed struct {
						Type   string `json:"$type"`
						Images []struct {
							Image json.RawMessage `json:"image"`
						} `json:"images"`
					}
					require.NoError(t, json.Unmarshal(raw, &embed))
					assert.Equal(t, "social.coves.embed.images", embed.Type)
					require.Len(t, embed.Images, 1)
					assertStoredImage(t, h, did, embed.Images[0].Image, tc.wantMime, tc.wantWidth, tc.wantHeight)
				}
			}

			if tc.wantOmittedWarning {
				before := logs.count()
				blob, err := h.m.fetchBlobClassified(ctx, did, avifMediaURL, slotEmbedImage)
				assert.NoError(t, err, "an undecodable image is no removal signal from the origin")
				assert.Nil(t, blob, "an undecodable image must not be stored")
				assert.True(t, logs.warnedAboutURL(avifMediaURL, before),
					"omitting the image must log a WARN carrying the url")
			}
		})
	}
}
