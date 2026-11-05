package materialize

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"image/color"
	"image/jpeg"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/linkpreview"
)

const (
	linkThumbnailVideoLink = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
	linkThumbnailOEmbedURL = "https://i.ytimg.com/vi/dQw4w9WgXcQ/hqdefault.jpg"
)

// linkThumbnailJPEG encodes a real JPEG of one solid color, so the Lemmy
// image and the unfurled image carry distinguishable bytes.
func linkThumbnailJPEG(t *testing.T, width, height int, fill color.NRGBA) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, jpeg.Encode(&buf, filledImage(width, height, fill), nil))
	return buf.Bytes()
}

// linkThumbnailPage is a lemmy.world link Page whose attachment is link.
// pageImage is the Page's `image` value; nil leaves it out.
func linkThumbnailPage(t *testing.T, link string, pageImage map[string]any) *ap.Object {
	t.Helper()
	page := map[string]any{
		"type":         "Page",
		"id":           pageID,
		"attributedTo": personID,
		"to":           []any{groupID, ap.PublicAudience},
		"name":         "A shared video",
		"mediaType":    "text/html",
		"attachment":   []any{map[string]any{"href": link, "type": "Link"}},
		"published":    "2026-07-07T03:27:37.028201Z",
		"audience":     groupID,
	}
	if pageImage != nil {
		page["image"] = pageImage
	}
	return objectFromMap(t, page)
}

// serveYouTubeOEmbed answers the oEmbed endpoint for linkThumbnailVideoLink
// the way YouTube does, and serves its thumbnail_url as thumb.
func (h *harness) serveYouTubeOEmbed(thumb []byte) {
	h.mux.HandleFunc("GET /oembed", func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		if r.Host != "www.youtube.com" || query.Get("url") != linkThumbnailVideoLink || query.Get("format") != "json" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"type":          "video",
			"title":         "A shared video",
			"thumbnail_url": linkThumbnailOEmbedURL,
		})
	})
	h.mux.HandleFunc("GET /vi/dQw4w9WgXcQ/hqdefault.jpg", func(w http.ResponseWriter, r *http.Request) {
		if r.Host != "i.ytimg.com" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "image/jpeg")
		_, _ = w.Write(thumb)
	})
}

// withConcreteLinkPreviews wires the real linkpreview.Resolver over the
// harness's fixture-rewriting transport (the NodeInfoClient's), so its
// literal https://www.youtube.com requests land on the fixture server.
func withConcreteLinkPreviews(o *Options) {
	o.LinkPreviews = linkpreview.NewResolver(&http.Client{Transport: o.NodeInfoClient.Transport}, "test-agent")
}

type linkPreviewCall struct {
	link        string
	calledAt    time.Time
	deadline    time.Time
	hasDeadline bool
}

// recordingLinkPreviewer answers every call with a fixed result, after
// delay, and records each call's link and context deadline.
type recordingLinkPreviewer struct {
	thumbURL string
	err      error
	delay    time.Duration

	mu    sync.Mutex
	calls []linkPreviewCall
}

func (r *recordingLinkPreviewer) ThumbnailURL(ctx context.Context, link string) (string, error) {
	deadline, ok := ctx.Deadline()
	r.mu.Lock()
	r.calls = append(r.calls, linkPreviewCall{link: link, calledAt: time.Now(), deadline: deadline, hasDeadline: ok})
	r.mu.Unlock()
	time.Sleep(r.delay)
	return r.thumbURL, r.err
}

func (r *recordingLinkPreviewer) recordedCalls() []linkPreviewCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]linkPreviewCall(nil), r.calls...)
}

// materializeLinkThumbnailPost materializes page and returns the stored
// post record.
func materializeLinkThumbnailPost(t *testing.T, h *harness, page *ap.Object) map[string]any {
	t.Helper()
	h.serveLemmyWorldFixtures()
	_, err := h.m.MaterializePost(context.Background(), page, groupID)
	require.NoError(t, err)
	return h.recordFor(t, pageID)
}

// externalOf returns the record's social.coves.embed.external `external`.
func externalOf(t *testing.T, record map[string]any) map[string]any {
	t.Helper()
	embed, ok := record["embed"].(map[string]any)
	require.True(t, ok, "link post must carry an embed, got %v", record["embed"])
	require.Equal(t, "social.coves.embed.external", embed["$type"])
	external, ok := embed["external"].(map[string]any)
	require.True(t, ok, "external embed must carry external")
	return external
}

// thumbBytes reads the external thumb blob back from the post author's repo.
func thumbBytes(t *testing.T, h *harness, external map[string]any) []byte {
	t.Helper()
	thumb, ok := external["thumb"]
	require.True(t, ok, "external embed must carry a thumb blob")
	raw, err := json.Marshal(thumb)
	require.NoError(t, err)
	var ref struct {
		Type     string `json:"$type"`
		MimeType string `json:"mimeType"`
		Ref      struct {
			Link string `json:"$link"`
		} `json:"ref"`
	}
	require.NoError(t, json.Unmarshal(raw, &ref))
	require.Equal(t, "blob", ref.Type)
	require.Equal(t, "image/jpeg", ref.MimeType)
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
	data, mimeType, err := h.manager.GetBlob(context.Background(), authorDID, ref.Ref.Link)
	require.NoError(t, err, "the thumb blob is stored under the post author's DID")
	assert.Equal(t, "image/jpeg", mimeType)
	return data
}

// TestLinkThumbnailYouTubeUnfurl (B5, acceptance): a YouTube link Page with
// no image gets the oEmbed thumbnail as its external thumb, through the real
// resolver.
func TestLinkThumbnailYouTubeUnfurl(t *testing.T) {
	unfurled := linkThumbnailJPEG(t, 3, 3, color.NRGBA{B: 255, A: 255})
	h := newHarness(t, withConcreteLinkPreviews)
	h.serveYouTubeOEmbed(unfurled)

	record := materializeLinkThumbnailPost(t, h, linkThumbnailPage(t, linkThumbnailVideoLink, nil))

	external := externalOf(t, record)
	assert.Equal(t, linkThumbnailVideoLink, external["uri"])
	assert.Equal(t, unfurled, thumbBytes(t, h, external))
}

// TestLinkThumbnailPrecedence (B6): a fetchable Lemmy image wins without
// consulting oEmbed; a missing or unfetchable one falls back to the unfurl.
func TestLinkThumbnailPrecedence(t *testing.T) {
	lemmyBytes := linkThumbnailJPEG(t, 2, 2, color.NRGBA{R: 255, A: 255})
	unfurledBytes := linkThumbnailJPEG(t, 3, 3, color.NRGBA{B: 255, A: 255})

	tests := []struct {
		name            string
		pageImage       map[string]any
		want            []byte
		wantNoOEmbedHit bool
	}{
		{
			name:            "fetchable Lemmy image wins",
			pageImage:       map[string]any{"type": "Image", "url": "https://lemmy.world/lemmythumb/a.jpg"},
			want:            lemmyBytes,
			wantNoOEmbedHit: true,
		},
		{
			name:      "Lemmy image 404 falls back to unfurl",
			pageImage: map[string]any{"type": "Image", "url": "https://lemmy.world/lemmythumb/missing.jpg"},
			want:      unfurledBytes,
		},
		{
			name:      "Lemmy image without url falls back to unfurl",
			pageImage: map[string]any{"type": "Image"},
			want:      unfurledBytes,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, withConcreteLinkPreviews)
			h.serveYouTubeOEmbed(unfurledBytes)
			h.mux.HandleFunc("GET /lemmythumb/a.jpg", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "image/jpeg")
				_, _ = w.Write(lemmyBytes)
			})

			record := materializeLinkThumbnailPost(t, h, linkThumbnailPage(t, linkThumbnailVideoLink, tt.pageImage))

			external := externalOf(t, record)
			assert.Equal(t, linkThumbnailVideoLink, external["uri"])
			assert.Equal(t, tt.want, thumbBytes(t, h, external))
			if tt.wantNoOEmbedHit {
				assert.Equal(t, 0, h.requestCount("www.youtube.com", "/oembed"),
					"a fetchable Lemmy image means the link is not unfurled")
			}
		})
	}
}

// TestLinkThumbnailFailureNonFatal (B7): every unfurl failure leaves the
// post materialized with its external embed and no thumb.
func TestLinkThumbnailFailureNonFatal(t *testing.T) {
	tests := []struct {
		name     string
		previews *recordingLinkPreviewer
	}{
		{name: "resolver error", previews: &recordingLinkPreviewer{err: errors.New("unfurl failed")}},
		{name: "resolver finds no thumbnail", previews: &recordingLinkPreviewer{}},
		{name: "thumbnail URL 404s", previews: &recordingLinkPreviewer{thumbURL: "https://thumbs.example/missing.jpg"}},
		{name: "thumbnail URL is not an image", previews: &recordingLinkPreviewer{thumbURL: "https://thumbs.example/thumbhtml/page.jpg"}},
		{name: "resolver deadline exceeded", previews: &recordingLinkPreviewer{err: context.DeadlineExceeded}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, func(o *Options) { o.LinkPreviews = tt.previews })
			h.mux.HandleFunc("GET /thumbhtml/page.jpg", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
				_, _ = w.Write([]byte("<html><body>not an image</body></html>"))
			})

			record := materializeLinkThumbnailPost(t, h, linkThumbnailPage(t, linkThumbnailVideoLink, nil))

			external := externalOf(t, record)
			assert.Equal(t, linkThumbnailVideoLink, external["uri"])
			assert.NotContains(t, external, "thumb")
		})
	}
}

// TestLinkThumbnailResolverDeadline (B7): the resolver runs under a context
// deadline of at most 10s.
func TestLinkThumbnailResolverDeadline(t *testing.T) {
	previews := &recordingLinkPreviewer{}
	h := newHarness(t, func(o *Options) { o.LinkPreviews = previews })

	materializeLinkThumbnailPost(t, h, linkThumbnailPage(t, linkThumbnailVideoLink, nil))

	calls := previews.recordedCalls()
	require.Len(t, calls, 1)
	assert.Equal(t, linkThumbnailVideoLink, calls[0].link)
	require.True(t, calls[0].hasDeadline, "the resolver must run under a deadline")
	assert.LessOrEqual(t, calls[0].deadline.Sub(calls[0].calledAt), 11*time.Second,
		"the resolver deadline is at most 10s (+1s slack) from the call")
}

// TestLinkThumbnailDisabledWithoutResolver (B7): with no LinkPreviews the
// link is never unfurled.
func TestLinkThumbnailDisabledWithoutResolver(t *testing.T) {
	h := newHarness(t)
	h.serveYouTubeOEmbed(linkThumbnailJPEG(t, 3, 3, color.NRGBA{B: 255, A: 255}))

	record := materializeLinkThumbnailPost(t, h, linkThumbnailPage(t, linkThumbnailVideoLink, nil))

	external := externalOf(t, record)
	assert.NotContains(t, external, "thumb")
	assert.Equal(t, 0, h.requestCount("www.youtube.com", "/oembed"))
}

// TestLinkThumbnailNotConsulted (B8): image posts and posts with no link
// never reach the resolver.
func TestLinkThumbnailNotConsulted(t *testing.T) {
	tests := []struct {
		name          string
		attachment    []any
		wantEmbedType any
	}{
		{
			name: "image attachment post",
			attachment: []any{map[string]any{
				"type":      "Document",
				"url":       "https://lemmy.world/pictrs/image/attached.jpeg",
				"mediaType": "image/jpeg",
			}},
			wantEmbedType: "social.coves.embed.images",
		},
		{
			name:       "post with no link and no image",
			attachment: []any{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			previews := &recordingLinkPreviewer{thumbURL: linkThumbnailOEmbedURL}
			h := newHarness(t, func(o *Options) { o.LinkPreviews = previews })
			page := objectFromMap(t, map[string]any{
				"type":         "Page",
				"id":           pageID,
				"attributedTo": personID,
				"to":           []any{groupID, ap.PublicAudience},
				"name":         "Not a link post",
				"mediaType":    "text/html",
				"attachment":   tt.attachment,
				"published":    "2026-07-07T03:27:37.028201Z",
				"audience":     groupID,
			})

			record := materializeLinkThumbnailPost(t, h, page)

			if tt.wantEmbedType == nil {
				assert.NotContains(t, record, "embed")
			} else {
				embed, ok := record["embed"].(map[string]any)
				require.True(t, ok, "image post must carry an embed")
				assert.Equal(t, tt.wantEmbedType, embed["$type"])
			}
			assert.Empty(t, previews.recordedCalls(), "the resolver must not be consulted")
		})
	}
}

// mediaDeadlineFetcher passes every call through to the harness's fetcher
// and records the context deadline of each FetchMedia call by URL.
type mediaDeadlineFetcher struct {
	Fetcher

	mu        sync.Mutex
	deadlines map[string]time.Time // zero when the call had no deadline
}

func (f *mediaDeadlineFetcher) FetchMedia(ctx context.Context, iri string, maxBytes int64) ([]byte, string, error) {
	deadline, _ := ctx.Deadline()
	f.mu.Lock()
	f.deadlines[iri] = deadline
	f.mu.Unlock()
	return f.Fetcher.FetchMedia(ctx, iri, maxBytes)
}

func (f *mediaDeadlineFetcher) deadlineFor(iri string) (time.Time, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	deadline, ok := f.deadlines[iri]
	return deadline, ok
}

// TestLinkThumbnailSharedDeadline: one deadline covers resolving the link
// and downloading the unfurled thumbnail. The resolver takes 250ms, so a
// fresh timeout started for the download would end after the resolver's.
func TestLinkThumbnailSharedDeadline(t *testing.T) {
	const thumbURL = "https://thumbs.example/shared-deadline/thumb.jpg"
	unfurled := linkThumbnailJPEG(t, 3, 3, color.NRGBA{B: 255, A: 255})
	previews := &recordingLinkPreviewer{thumbURL: thumbURL, delay: 250 * time.Millisecond}
	fetcher := &mediaDeadlineFetcher{deadlines: map[string]time.Time{}}
	h := newHarness(t, func(o *Options) {
		o.LinkPreviews = previews
		fetcher.Fetcher = o.Fetcher
		o.Fetcher = fetcher
	})
	h.mux.HandleFunc("GET /shared-deadline/thumb.jpg", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/jpeg")
		_, _ = w.Write(unfurled)
	})
	h.serveLemmyWorldFixtures()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, err := h.m.MaterializePost(ctx, linkThumbnailPage(t, linkThumbnailVideoLink, nil), groupID)
	require.NoError(t, err)

	external := externalOf(t, h.recordFor(t, pageID))
	assert.Equal(t, unfurled, thumbBytes(t, h, external))
	calls := previews.recordedCalls()
	require.Len(t, calls, 1)
	require.True(t, calls[0].hasDeadline, "the resolver must run under a deadline")
	fetchDeadline, fetched := fetcher.deadlineFor(thumbURL)
	require.True(t, fetched, "the unfurled thumbnail was downloaded")
	require.False(t, fetchDeadline.IsZero(), "the thumbnail download must run under a deadline")
	assert.False(t, fetchDeadline.After(calls[0].deadline),
		"the thumbnail download deadline %s must not extend past the resolver deadline %s",
		fetchDeadline.Format(time.RFC3339Nano), calls[0].deadline.Format(time.RFC3339Nano))
}

// TestLinkThumbnailSkippedWhenContextEnded: when the ingest context ends
// before the unfurl is reached (here during the Lemmy image fetch), the
// resolver is not called.
func TestLinkThumbnailSkippedWhenContextEnded(t *testing.T) {
	previews := &recordingLinkPreviewer{thumbURL: linkThumbnailOEmbedURL}
	h := newHarness(t, func(o *Options) { o.LinkPreviews = previews })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.mux.HandleFunc("GET /lemmythumb/cancels.jpg", func(w http.ResponseWriter, r *http.Request) {
		cancel()
		http.NotFound(w, r)
	})
	h.serveLemmyWorldFixtures()
	page := linkThumbnailPage(t, linkThumbnailVideoLink,
		map[string]any{"type": "Image", "url": "https://lemmy.world/lemmythumb/cancels.jpg"})

	_, _ = h.m.MaterializePost(ctx, page, groupID) // the ended context may fail the ingest

	require.Equal(t, 1, h.requestCount("lemmy.world", "/lemmythumb/cancels.jpg"), "the Lemmy image fetch was reached")
	assert.Empty(t, previews.recordedCalls(), "the resolver must not be called after the context ended")
}
