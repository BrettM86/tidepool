package materialize

import (
	"bytes"
	"context"
	"encoding/json"
	"image"
	"image/color/palette"
	"image/gif"
	"image/jpeg"
	"image/png"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
)

const (
	oversizedPostID   = "https://lemmy.world/post/70401"
	oversizedActorID  = "https://lemmy.world/u/oversizedactor"
	oversizedMediaURL = "https://lemmy.world/media/oversized-picture"
)

// opaqueNoiseImage is deterministic opaque colour noise: it compresses
// badly, so its encoded size scales with its pixel count.
func opaqueNoiseImage(width, height int, seed uint64) *image.NRGBA {
	random := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	img := image.NewNRGBA(image.Rect(0, 0, width, height))
	for i := 0; i < len(img.Pix); i += 4 {
		img.Pix[i] = uint8(random.UintN(256))
		img.Pix[i+1] = uint8(random.UintN(256))
		img.Pix[i+2] = uint8(random.UintN(256))
		img.Pix[i+3] = 255
	}
	return img
}

func noiseJPEG(t *testing.T, width, height int) []byte {
	t.Helper()
	var buffer bytes.Buffer
	require.NoError(t, jpeg.Encode(&buffer, opaqueNoiseImage(width, height, 7), &jpeg.Options{Quality: 90}))
	return buffer.Bytes()
}

func noisePNG(t *testing.T, width, height int) []byte {
	t.Helper()
	var buffer bytes.Buffer
	require.NoError(t, png.Encode(&buffer, opaqueNoiseImage(width, height, 8)))
	return buffer.Bytes()
}

func noiseStaticGIF(t *testing.T, width, height int) []byte {
	t.Helper()
	var buffer bytes.Buffer
	require.NoError(t, gif.Encode(&buffer, opaqueNoiseImage(width, height, 9), nil))
	return buffer.Bytes()
}

// noiseAnimatedGIF is a GIF of frames frames of palette-index noise.
func noiseAnimatedGIF(t *testing.T, width, height, frames int) []byte {
	t.Helper()
	random := rand.New(rand.NewPCG(10, 11))
	animation := &gif.GIF{}
	for range frames {
		frame := image.NewPaletted(image.Rect(0, 0, width, height), palette.Plan9)
		for i := range frame.Pix {
			frame.Pix[i] = uint8(random.UintN(256))
		}
		animation.Image = append(animation.Image, frame)
		animation.Delay = append(animation.Delay, 10)
	}
	var buffer bytes.Buffer
	require.NoError(t, gif.EncodeAll(&buffer, animation))
	return buffer.Bytes()
}

// storedMedia reads a record blob ref and the bytes stored under it, checking
// the ref and the store agree on type and size.
func storedMedia(t *testing.T, h *harness, did string, rawRef json.RawMessage) (data []byte, mimeType string) {
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
	require.Equal(t, "blob", ref.Type)
	data, storedMime, err := h.manager.GetBlob(context.Background(), did, ref.Ref.Link)
	require.NoError(t, err)
	assert.Equal(t, ref.MimeType, storedMime, "record blob ref mimeType matches the stored blob")
	assert.Equal(t, int64(len(data)), ref.Size, "record blob ref size matches the stored bytes")
	return data, storedMime
}

type oversizedEntry int

const (
	entryPostAttachment oversizedEntry = iota
	entryProfileAvatar
	entryProfileBanner
	entryLinkPageImage
	entryLinkUnfurl
)

type oversizedOutcome int

const (
	// outcomeFitted: stored as JPEG or PNG within maxStored bytes and
	// maxLongEdge pixels, keeping the source's proportions.
	outcomeFitted oversizedOutcome = iota
	// outcomeIdentical: stored byte-for-byte as served.
	outcomeIdentical
	// outcomeOmitted: the record lands without the image and a WARN
	// carrying the media url is logged.
	outcomeOmitted
)

// TestOversizedMediaFittedIntoSlot: still media over its slot's store cap is
// downscaled and re-encoded until it fits, through every entry path that
// stores media (post image attachment, profile avatar and banner, link
// thumbnail from the Page image or the unfurl). Animated media over the cap
// and media over the download budget are still omitted, and media under the
// cap is stored as served.
func TestOversizedMediaFittedIntoSlot(t *testing.T) {
	const (
		storeCap512K = 512 * 1024
		fiveMiB      = 5 << 20
		oneMiB       = 1 << 20
	)
	cases := []struct {
		name  string
		entry oversizedEntry
		// downloadBytes and blobBytes configure MaxMediaDownloadBytes and
		// MaxBlobBytes (0 = default).
		downloadBytes, blobBytes int64
		media                    func(t *testing.T) []byte
		contentType              string
		// servedOver / servedUnder are fixture-size preconditions on the
		// served bytes (0 = unchecked).
		servedOver, servedUnder int64
		outcome                 oversizedOutcome
		// sourceWidth/sourceHeight are the source proportions a fitted
		// image keeps (within 1px).
		sourceWidth, sourceHeight int
		maxStored                 int64
		maxLongEdge               int
		// wantMime, when set, is the only acceptable fitted type.
		wantMime string
	}{
		{
			// I1 (acceptance), R5.
			name:        "post image attachment over the store cap",
			entry:       entryPostAttachment,
			blobBytes:   storeCap512K,
			media:       func(t *testing.T) []byte { return noiseJPEG(t, 1620, 1080) },
			contentType: "image/jpeg",
			servedOver:  1_400_000, servedUnder: 25 << 20,
			outcome:     outcomeFitted,
			sourceWidth: 1620, sourceHeight: 1080,
			maxStored: storeCap512K, maxLongEdge: 4096,
			wantMime: "image/jpeg",
		},
		{
			// I1b (R3): a source over 5 MiB, under the 25 MiB download
			// budget, fits the default 5 MiB store cap.
			name:        "post image attachment over 5 MiB within the download budget",
			entry:       entryPostAttachment,
			media:       func(t *testing.T) []byte { return noiseJPEG(t, 3300, 2200) },
			contentType: "image/jpeg",
			servedOver:  fiveMiB, servedUnder: 25 << 20,
			outcome:     outcomeFitted,
			sourceWidth: 3300, sourceHeight: 2200,
			maxStored: fiveMiB, maxLongEdge: 4096,
			wantMime: "image/jpeg",
		},
		{
			// I2.
			name:        "avatar PNG over the 1,000,000-byte avatar cap",
			entry:       entryProfileAvatar,
			media:       func(t *testing.T) []byte { return noisePNG(t, 1600, 1600) },
			contentType: "image/png",
			servedOver:  1_000_000, servedUnder: 25 << 20,
			outcome:     outcomeFitted,
			sourceWidth: 1600, sourceHeight: 1600,
			maxStored: 1_000_000, maxLongEdge: 1000,
		},
		{
			// I2 / R1: a static GIF is fitted before the slot's type
			// check, so the avatar (which does not accept GIF) stores the
			// PNG or JPEG output.
			name:        "static GIF avatar over the avatar cap",
			entry:       entryProfileAvatar,
			media:       func(t *testing.T) []byte { return noiseStaticGIF(t, 1100, 1100) },
			contentType: "image/gif",
			servedOver:  1_000_000, servedUnder: 25 << 20,
			outcome:     outcomeFitted,
			sourceWidth: 1100, sourceHeight: 1100,
			maxStored: 1_000_000, maxLongEdge: 1000,
		},
		{
			// R8.
			name:        "banner JPEG over the 2,000,000-byte banner cap",
			entry:       entryProfileBanner,
			media:       func(t *testing.T) []byte { return noiseJPEG(t, 3600, 1200) },
			contentType: "image/jpeg",
			servedOver:  2_000_000, servedUnder: 25 << 20,
			outcome:     outcomeFitted,
			sourceWidth: 3600, sourceHeight: 1200,
			maxStored: 2_000_000, maxLongEdge: 3000,
		},
		{
			// I3, the Page image path.
			name:        "link thumbnail from the Page image over the store cap",
			entry:       entryLinkPageImage,
			blobBytes:   oneMiB,
			media:       func(t *testing.T) []byte { return noiseJPEG(t, 2400, 1260) },
			contentType: "image/jpeg",
			servedOver:  oneMiB, servedUnder: 25 << 20,
			outcome:     outcomeFitted,
			sourceWidth: 2400, sourceHeight: 1260,
			maxStored: oneMiB, maxLongEdge: 2000,
		},
		{
			// I3, the unfurl path.
			name:        "unfurled link thumbnail over the store cap",
			entry:       entryLinkUnfurl,
			blobBytes:   oneMiB,
			media:       func(t *testing.T) []byte { return noiseJPEG(t, 2400, 1260) },
			contentType: "image/jpeg",
			servedOver:  oneMiB, servedUnder: 25 << 20,
			outcome:     outcomeFitted,
			sourceWidth: 2400, sourceHeight: 1260,
			maxStored: oneMiB, maxLongEdge: 2000,
		},
		{
			// I4 (guard).
			name:        "animated GIF post image over the store cap",
			entry:       entryPostAttachment,
			blobBytes:   storeCap512K,
			media:       func(t *testing.T) []byte { return noiseAnimatedGIF(t, 600, 600, 2) },
			contentType: "image/gif",
			servedOver:  storeCap512K, servedUnder: 25 << 20,
			outcome: outcomeOmitted,
		},
		{
			// I5 (guard).
			name:        "post image attachment under the store cap",
			entry:       entryPostAttachment,
			blobBytes:   storeCap512K,
			media:       func(t *testing.T) []byte { return noiseJPEG(t, 200, 150) },
			contentType: "image/jpeg",
			servedUnder: storeCap512K,
			outcome:     outcomeIdentical,
		},
		{
			// I6: over the download budget, under the store cap.
			name:          "post image attachment over the download budget",
			entry:         entryPostAttachment,
			downloadBytes: oneMiB,
			blobBytes:     fiveMiB,
			media:         func(t *testing.T) []byte { return noiseJPEG(t, 1620, 1080) },
			contentType:   "image/jpeg",
			servedOver:    oneMiB, servedUnder: fiveMiB,
			outcome: outcomeOmitted,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			media := tc.media(t)
			if tc.servedOver > 0 && int64(len(media)) <= tc.servedOver {
				t.Fatalf("fixture: served media is %d bytes, must be over %d", len(media), tc.servedOver)
			}
			if tc.servedUnder > 0 && int64(len(media)) >= tc.servedUnder {
				t.Fatalf("fixture: served media is %d bytes, must be under %d", len(media), tc.servedUnder)
			}

			logs := &recordingLogHandler{}
			configure := []func(*Options){func(o *Options) { o.Logger = slog.New(logs) }}
			if tc.entry == entryLinkUnfurl {
				configure = append(configure, func(o *Options) {
					o.LinkPreviews = &recordingLinkPreviewer{thumbURL: oversizedMediaURL}
				})
			}
			h := newHarnessWithMediaBudget(t, tc.downloadBytes, tc.blobBytes, configure...)
			h.mux.HandleFunc("GET /media/oversized-picture", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", tc.contentType)
				_, _ = w.Write(media)
			})
			ctx := context.Background()

			// rawRef is the slot's blob ref; nil when the slot is empty.
			var rawRef json.RawMessage
			var did string
			var aspectRatio *struct {
				Width  int `json:"width"`
				Height int `json:"height"`
			}
			switch tc.entry {
			case entryProfileAvatar, entryProfileBanner:
				field, apField := "avatar", "icon"
				if tc.entry == entryProfileBanner {
					field, apField = "banner", "image"
				}
				h.serveObject("/u/oversizedactor", person(oversizedActorID, "oversizedactor", map[string]any{
					apField: map[string]any{"type": "Image", "url": oversizedMediaURL},
				}))
				_, err := h.m.EnsureActor(ctx, &ap.Object{ID: oversizedActorID})
				require.NoError(t, err, "media outcomes must not block the profile")
				did = testDIDFor("oversizedactor", "lemmy.world")
				record, _, err := h.manager.GetRecord(ctx, did, CollectionActorProfile, ProfileRKey)
				require.NoError(t, err)
				if value, ok := record[field]; ok {
					rawRef, err = json.Marshal(value)
					require.NoError(t, err)
				}
			case entryPostAttachment:
				h.serveLemmyWorldFixtures()
				post := page(oversizedPostID, personID, groupID, "an oversized image post", "2026-11-22T10:00:00.000000Z")
				post["attachment"] = []any{map[string]any{"type": "Image", "href": oversizedMediaURL, "name": "a picture"}}
				_, err := h.m.MaterializePost(ctx, mustObject(t, post), groupID)
				require.NoError(t, err, "media outcomes must not block the post")
				did = testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
				record := h.recordFor(t, oversizedPostID)
				if value, ok := record["embed"]; ok {
					raw, err := json.Marshal(value)
					require.NoError(t, err)
					var embed struct {
						Type   string        `json:"$type"`
						Images []aspectImage `json:"images"`
					}
					require.NoError(t, json.Unmarshal(raw, &embed))
					require.Equal(t, "social.coves.embed.images", embed.Type)
					require.Len(t, embed.Images, 1)
					rawRef = embed.Images[0].Image
					aspectRatio = embed.Images[0].AspectRatio
				}
			case entryLinkPageImage, entryLinkUnfurl:
				var pageImage map[string]any
				if tc.entry == entryLinkPageImage {
					pageImage = map[string]any{"type": "Image", "url": oversizedMediaURL}
				}
				record := materializeLinkThumbnailPost(t, h, linkThumbnailPage(t, linkThumbnailVideoLink, pageImage))
				did = testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
				external := externalOf(t, record)
				if value, ok := external["thumb"]; ok {
					var err error
					rawRef, err = json.Marshal(value)
					require.NoError(t, err)
				}
			}

			switch tc.outcome {
			case outcomeOmitted:
				assert.Empty(t, string(rawRef), "the image must be omitted")
				assert.True(t, logs.warnedAboutURL(oversizedMediaURL, 0),
					"omitting the image must log a WARN carrying the url")

			case outcomeIdentical:
				require.NotNil(t, rawRef, "the image must be stored")
				data, mimeType := storedMedia(t, h, did, rawRef)
				assert.Equal(t, tc.contentType, mimeType)
				assert.True(t, bytes.Equal(media, data), "under-cap media is stored byte-for-byte as served")

			case outcomeFitted:
				require.NotNil(t, rawRef, "the oversized image must be fitted into its slot, not omitted")
				data, mimeType := storedMedia(t, h, did, rawRef)
				if tc.wantMime != "" {
					assert.Equal(t, tc.wantMime, mimeType, "fitted image type")
				} else {
					assert.Contains(t, []string{"image/jpeg", "image/png"}, mimeType, "fitted image type")
				}
				assert.LessOrEqual(t, int64(len(data)), tc.maxStored, "fitted image must fit the store cap")

				var decoded image.Image
				var err error
				switch mimeType {
				case "image/jpeg":
					decoded, err = jpeg.Decode(bytes.NewReader(data))
				case "image/png":
					decoded, err = png.Decode(bytes.NewReader(data))
				default:
					t.Fatalf("fitted image has type %q", mimeType)
				}
				require.NoError(t, err, "stored bytes must decode as %s", mimeType)
				width, height := decoded.Bounds().Dx(), decoded.Bounds().Dy()
				assert.LessOrEqual(t, max(width, height), tc.maxLongEdge, "fitted image long edge")
				// Proportions: height within 1px of width scaled by the
				// literal source ratio.
				wantHeight := float64(width) * float64(tc.sourceHeight) / float64(tc.sourceWidth)
				assert.InDelta(t, wantHeight, float64(height), 1.0,
					"fitted %dx%d must keep the %dx%d proportions", width, height, tc.sourceWidth, tc.sourceHeight)

				if tc.entry == entryPostAttachment {
					require.NotNil(t, aspectRatio, "a fitted post image carries an aspectRatio")
					assert.Equal(t, width, aspectRatio.Width, "aspectRatio width is the stored image's")
					assert.Equal(t, height, aspectRatio.Height, "aspectRatio height is the stored image's")
				}
			}
		})
	}
}
