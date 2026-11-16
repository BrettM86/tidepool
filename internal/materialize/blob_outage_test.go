package materialize

import (
	"context"
	"database/sql"
	stderrors "errors"
	"image/color"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/blobstore"
	"tidepool/internal/errors"
	"tidepool/internal/repo"
	"tidepool/internal/testutil"
)

// errByteStoreDown is what failingBlobStore.Put returns while failing.
var errByteStoreDown = stderrors.New("test byte store is down")

// failingBlobStore is the Postgres byte store whose Put fails while failing
// is set, standing in for an unreachable object store.
// While waitForContext is set, Put instead blocks until its context ends
// and returns the context's error, standing in for a store that is still
// working when the caller's deadline expires; beforeWait, when set, runs
// first. A non-zero delay makes Put wait that long (or until its context
// ends, returning the context's error) before any of the above, standing in
// for a slow or hanging store.
type failingBlobStore struct {
	*blobstore.Postgres
	failing        atomic.Bool
	waitForContext atomic.Bool
	beforeWait     func()
	delay          atomic.Int64
}

func (s *failingBlobStore) Put(ctx context.Context, tx *sql.Tx, did, cid, mimeType string, data []byte) error {
	if delay := time.Duration(s.delay.Load()); delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	if s.failing.Load() {
		return errByteStoreDown
	}
	if s.waitForContext.Load() {
		if s.beforeWait != nil {
			s.beforeWait()
		}
		<-ctx.Done()
		return ctx.Err()
	}
	return s.Postgres.Put(ctx, tx, did, cid, mimeType, data)
}

// newOutageHarness is newHarness over a byte store the test can fail.
func newOutageHarness(t *testing.T, configure ...func(*Options)) (*harness, *failingBlobStore) {
	t.Helper()
	byteStore := &failingBlobStore{Postgres: blobstore.NewPostgres(testutil.DB(t))}
	h := newHarnessWithRepoOptions(t, []repo.Option{repo.WithBlobStore(byteStore)}, configure...)
	return h, byteStore
}

// TestPostImageByteStoreOutageFailsPost (D4, acceptance): an image post whose
// image fetches fine but whose byte store is down fails with
// IsBlobStoreUnavailable and writes no record, mapping or commit for the
// post. Author and community are materialized first, with the store up.
func TestPostImageByteStoreOutageFailsPost(t *testing.T) {
	thumbJPEG := linkThumbnailJPEG(t, 2, 2, color.NRGBA{G: 255, A: 255})
	tests := []struct {
		name      string
		configure []func(*Options)
		// post builds the post; postID is its AP id.
		post   func(t *testing.T, h *harness) *ap.Object
		postID string
		// imageHost/imagePath is the fetch the row must reach.
		imageHost, imagePath string
	}{
		{
			name: "attachment image embed",
			post: func(t *testing.T, h *harness) *ap.Object {
				h.mux.HandleFunc("GET /media/postv2-embed.png", func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "image/png")
					_, _ = w.Write(embedImageBytes)
				})
				imagePost := page(imagePageID, personID, groupID, "an image post", "2026-07-08T10:00:00.000000Z")
				imagePost["attachment"] = []any{map[string]any{
					"type":      "Image",
					"href":      embedImageURL,
					"mediaType": "image/png",
				}}
				return mustObject(t, imagePost)
			},
			postID:    imagePageID,
			imageHost: "lemmy.world",
			imagePath: "/media/postv2-embed.png",
		},
		{
			name: "link post with a direct thumbnail",
			post: func(t *testing.T, h *harness) *ap.Object {
				h.mux.HandleFunc("GET /lemmythumb/outage.jpg", func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "image/jpeg")
					_, _ = w.Write(thumbJPEG)
				})
				return linkThumbnailPage(t, linkThumbnailVideoLink,
					map[string]any{"type": "Image", "url": "https://lemmy.world/lemmythumb/outage.jpg"})
			},
			postID:    pageID,
			imageHost: "lemmy.world",
			imagePath: "/lemmythumb/outage.jpg",
		},
		{
			name:      "link post with an unfurled thumbnail",
			configure: []func(*Options){withConcreteLinkPreviews},
			post: func(t *testing.T, h *harness) *ap.Object {
				h.serveYouTubeOEmbed(thumbJPEG)
				return linkThumbnailPage(t, linkThumbnailVideoLink, nil)
			},
			postID:    pageID,
			imageHost: "i.ytimg.com",
			imagePath: "/vi/dQw4w9WgXcQ/hqdefault.jpg",
		},
		{
			name: "image-only post from the page image",
			post: func(t *testing.T, h *harness) *ap.Object {
				h.mux.HandleFunc("GET /pictrs/image/outage.jpg", func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "image/jpeg")
					_, _ = w.Write(thumbJPEG)
				})
				// No attachment and no url: the page image alone makes the
				// images embed.
				imagePost := page(imagePageID, personID, groupID, "an image post", "2026-07-08T10:00:00.000000Z")
				imagePost["image"] = map[string]any{"type": "Image", "url": "https://lemmy.world/pictrs/image/outage.jpg"}
				return mustObject(t, imagePost)
			},
			postID:    imagePageID,
			imageHost: "lemmy.world",
			imagePath: "/pictrs/image/outage.jpg",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, byteStore := newOutageHarness(t, tt.configure...)
			h.serveLemmyWorldFixtures()
			ctx := context.Background()
			post := tt.post(t, h)

			_, err := h.m.EnsureCommunity(ctx, &ap.Object{ID: groupID})
			require.NoError(t, err)
			_, err = h.m.EnsureActor(ctx, &ap.Object{ID: personID})
			require.NoError(t, err)

			communityDID := testDIDFor("technology", "lemmy.world")
			authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
			communityHead, _, err := h.manager.Head(ctx, communityDID)
			require.NoError(t, err)
			authorHead, _, err := h.manager.Head(ctx, authorDID)
			require.NoError(t, err)
			eventsBefore := len(h.firehoseEvents())

			byteStore.failing.Store(true)
			_, err = h.m.MaterializePost(ctx, post, groupID)

			assert.Equal(t, 1, h.requestCount(tt.imageHost, tt.imagePath),
				"the row must reach its image fetch")
			require.Error(t, err, "a byte store outage must fail the post")
			assert.True(t, errors.IsBlobStoreUnavailable(err), "err=%v", err)
			assert.Equal(t, 0, countMappings(t, h, tt.postID), "no ap_id mapping for the failed post")
			gotCommunityHead, _, err := h.manager.Head(ctx, communityDID)
			require.NoError(t, err)
			assert.Equal(t, communityHead, gotCommunityHead, "no commit in the community repo")
			gotAuthorHead, _, err := h.manager.Head(ctx, authorDID)
			require.NoError(t, err)
			assert.Equal(t, authorHead, gotAuthorHead, "no commit in the author repo")
			assert.Equal(t, eventsBefore, len(h.firehoseEvents()), "no firehose event for the failed post")
		})
	}
}

// TestPostUnfurlDeadlineOmitsThumb (D4): the unfurl's own deadline bounds
// the resolve and the origin download only. A download it cuts is an origin
// failure, so the link post commits without the thumb. A byte store that
// hangs past that deadline and then fails is a store outage: the post fails
// with IsBlobStoreUnavailable and commits nothing, so it is retried with its
// thumb. The parent context ending during the store still fails the post.
// The first two subtests each wait out the full linkPreviewTimeout.
func TestPostUnfurlDeadlineOmitsThumb(t *testing.T) {
	thumbJPEG := linkThumbnailJPEG(t, 2, 2, color.NRGBA{G: 255, A: 255})
	setUp := func(t *testing.T, serve func(h *harness)) (*harness, *failingBlobStore) {
		t.Helper()
		h, byteStore := newOutageHarness(t, withConcreteLinkPreviews)
		h.serveLemmyWorldFixtures()
		serve(h)
		ctx := context.Background()
		_, err := h.m.EnsureCommunity(ctx, &ap.Object{ID: groupID})
		require.NoError(t, err)
		_, err = h.m.EnsureActor(ctx, &ap.Object{ID: personID})
		require.NoError(t, err)
		return h, byteStore
	}
	serveThumb := func(h *harness) { h.serveYouTubeOEmbed(thumbJPEG) }

	t.Run("store outage outlasting the unfurl deadline fails the post", func(t *testing.T) {
		h, byteStore := setUp(t, serveThumb)
		ctx := context.Background()
		communityDID := testDIDFor("technology", "lemmy.world")
		authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
		communityHead, _, err := h.manager.Head(ctx, communityDID)
		require.NoError(t, err)
		authorHead, _, err := h.manager.Head(ctx, authorDID)
		require.NoError(t, err)
		eventsBefore := len(h.firehoseEvents())
		byteStore.delay.Store(int64(linkPreviewTimeout + time.Second))
		byteStore.failing.Store(true)

		_, err = h.m.MaterializePost(ctx, linkThumbnailPage(t, linkThumbnailVideoLink, nil), groupID)

		assert.Equal(t, 1, h.requestCount("i.ytimg.com", "/vi/dQw4w9WgXcQ/hqdefault.jpg"),
			"the unfurled thumbnail must be fetched")
		require.Error(t, err, "a store outage outlasting the unfurl deadline must fail the post")
		assert.True(t, errors.IsBlobStoreUnavailable(err), "err=%v", err)
		assert.Equal(t, 0, countMappings(t, h, pageID), "no ap_id mapping for the failed post")
		gotCommunityHead, _, err := h.manager.Head(ctx, communityDID)
		require.NoError(t, err)
		assert.Equal(t, communityHead, gotCommunityHead, "no commit in the community repo")
		gotAuthorHead, _, err := h.manager.Head(ctx, authorDID)
		require.NoError(t, err)
		assert.Equal(t, authorHead, gotAuthorHead, "no commit in the author repo")
		assert.Equal(t, eventsBefore, len(h.firehoseEvents()), "no firehose event for the failed post")
	})

	t.Run("unfurl deadline expires during the origin download", func(t *testing.T) {
		var thumbRequests atomic.Int32
		h, _ := setUp(t, func(h *harness) {
			h.mux.HandleFunc("GET /oembed", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"type":"video","title":"A shared video","thumbnail_url":"` + linkThumbnailOEmbedURL + `"}`))
			})
			h.mux.HandleFunc("GET /vi/dQw4w9WgXcQ/hqdefault.jpg", func(w http.ResponseWriter, r *http.Request) {
				thumbRequests.Add(1)
				// Hang until the client gives up, well past the unfurl deadline.
				select {
				case <-r.Context().Done():
				case <-time.After(linkPreviewTimeout + 5*time.Second):
				}
				w.Header().Set("Content-Type", "image/jpeg")
				_, _ = w.Write(thumbJPEG)
			})
		})

		_, err := h.m.MaterializePost(context.Background(), linkThumbnailPage(t, linkThumbnailVideoLink, nil), groupID)
		require.NoError(t, err, "an unfurl deadline cutting the origin download must not fail the post")

		assert.GreaterOrEqual(t, thumbRequests.Load(), int32(1), "the unfurled thumbnail must be requested")
		assert.Equal(t, 1, countMappings(t, h, pageID))
		external := externalOf(t, h.recordFor(t, pageID))
		assert.Equal(t, linkThumbnailVideoLink, external["uri"])
		assert.NotContains(t, external, "thumb", "the timed-out thumb is left out")
	})

	t.Run("parent context ends during the store", func(t *testing.T) {
		h, byteStore := setUp(t, serveThumb)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		byteStore.beforeWait = cancel
		byteStore.waitForContext.Store(true)

		_, err := h.m.MaterializePost(ctx, linkThumbnailPage(t, linkThumbnailVideoLink, nil), groupID)
		require.Error(t, err, "the parent context ending must fail the post")
		assert.True(t, stderrors.Is(err, context.Canceled), "err=%v", err)
		assert.Equal(t, 0, countMappings(t, h, pageID), "no ap_id mapping for the failed post")
	})
}

// TestPostImageOriginFailureOmitsImage (D4 guard): with the byte store up,
// an image the origin will not serve usably (404, wrong content type,
// oversize) is left out and the post still commits.
func TestPostImageOriginFailureOmitsImage(t *testing.T) {
	attachmentPost := func(t *testing.T, href string) *ap.Object {
		imagePost := page(imagePageID, personID, groupID, "an image post", "2026-07-08T10:00:00.000000Z")
		imagePost["attachment"] = []any{map[string]any{
			"type":      "Image",
			"href":      href,
			"mediaType": "image/png",
		}}
		return mustObject(t, imagePost)
	}
	tests := []struct {
		name   string
		post   func(t *testing.T) *ap.Object
		postID string
		// wantExternal is true for a link post: the external embed stays,
		// without a thumb. Otherwise the post carries no embed at all.
		wantExternal bool
	}{
		{
			name:   "attachment image 404",
			post:   func(t *testing.T) *ap.Object { return attachmentPost(t, "https://lemmy.world/media/gone.png") },
			postID: imagePageID,
		},
		{
			name:   "attachment image wrong content type",
			post:   func(t *testing.T) *ap.Object { return attachmentPost(t, "https://lemmy.world/media/page.png") },
			postID: imagePageID,
		},
		{
			name:   "attachment image oversize",
			post:   func(t *testing.T) *ap.Object { return attachmentPost(t, "https://lemmy.world/media/huge.png") },
			postID: imagePageID,
		},
		{
			name: "direct link thumbnail 404",
			post: func(t *testing.T) *ap.Object {
				return linkThumbnailPage(t, linkThumbnailVideoLink,
					map[string]any{"type": "Image", "url": "https://lemmy.world/lemmythumb/gone.jpg"})
			},
			postID:       pageID,
			wantExternal: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.serveLemmyWorldFixtures()
			h.mux.HandleFunc("GET /media/page.png", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/html")
				_, _ = w.Write([]byte("<html>not an image</html>"))
			})
			h.mux.HandleFunc("GET /media/huge.png", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "image/png")
				_, _ = w.Write(make([]byte, 10_000_001))
			})
			ctx := context.Background()

			_, err := h.m.MaterializePost(ctx, tt.post(t), groupID)
			require.NoError(t, err, "an unusable origin image must not fail the post")

			assert.Equal(t, 1, countMappings(t, h, tt.postID))
			record := h.recordFor(t, tt.postID)
			if tt.wantExternal {
				external := externalOf(t, record)
				assert.Equal(t, linkThumbnailVideoLink, external["uri"])
				assert.NotContains(t, external, "thumb")
				return
			}
			assert.NotContains(t, record, "embed", "the failed image leaves no embed")
		})
	}
}
