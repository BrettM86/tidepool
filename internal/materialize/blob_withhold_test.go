package materialize

import (
	"context"
	"encoding/json"
	"image/color"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/blobwithhold"
	"tidepool/internal/errors"
	"tidepool/internal/testutil"
)

// withheldBlobRef is a record blob ref reduced to what B12 compares.
type withheldBlobRef struct {
	CID      string
	MimeType string
	Size     int64
}

// blobRefOf decodes a record's blob value (atdata.Blob or its JSON map).
func blobRefOf(t *testing.T, value any) withheldBlobRef {
	t.Helper()
	raw, err := json.Marshal(value)
	require.NoError(t, err)
	var ref struct {
		Type     string `json:"$type"`
		MimeType string `json:"mimeType"`
		Size     int64  `json:"size"`
		Ref      struct {
			Link string `json:"$link"`
		} `json:"ref"`
	}
	require.NoError(t, json.Unmarshal(raw, &ref))
	require.Equal(t, "blob", ref.Type, "not a blob ref: %s", raw)
	return withheldBlobRef{CID: ref.Ref.Link, MimeType: ref.MimeType, Size: ref.Size}
}

// embedImageRef returns the single social.coves.embed.images image ref.
func embedImageRef(t *testing.T, record map[string]any) withheldBlobRef {
	t.Helper()
	require.Contains(t, record, "embed", "the post must carry its image embed")
	raw, err := json.Marshal(record["embed"])
	require.NoError(t, err)
	var embed struct {
		Type   string `json:"$type"`
		Images []struct {
			Image json.RawMessage `json:"image"`
		} `json:"images"`
	}
	require.NoError(t, json.Unmarshal(raw, &embed))
	require.Equal(t, "social.coves.embed.images", embed.Type)
	require.Len(t, embed.Images, 1)
	var image any
	require.NoError(t, json.Unmarshal(embed.Images[0].Image, &image))
	return blobRefOf(t, image)
}

// blobHoldersOfCID returns every DID holding a blobs row for cidStr.
func blobHoldersOfCID(t *testing.T, cidStr string) []string {
	t.Helper()
	rows, err := testutil.DB(t).Query(`SELECT did FROM blobs WHERE cid = $1 ORDER BY did`, cidStr)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	var dids []string
	for rows.Next() {
		var did string
		require.NoError(t, rows.Scan(&did))
		dids = append(dids, did)
	}
	require.NoError(t, rows.Err())
	return dids
}

// cleanupWithhold removes this test's withhold rows for cidStr.
func cleanupWithhold(t *testing.T, cidStr string) {
	t.Helper()
	t.Cleanup(func() {
		if _, err := testutil.DB(t).Exec(`DELETE FROM blob_withholds WHERE cid = $1`, cidStr); err != nil {
			t.Errorf("cleanup blob_withholds: %v", err)
		}
	})
}

// operatorWithhold places an operator withhold on cidStr and schedules its
// cleanup.
func operatorWithhold(t *testing.T, withholds *blobwithhold.Store, cidStr string) {
	t.Helper()
	cleanupWithhold(t, cidStr)
	_, err := withholds.Withhold(context.Background(), blobwithhold.OperatorRequest{
		CID:        cidStr,
		ReasonCode: blobwithhold.ReasonIllegalContent,
		Actor:      "b12-operator",
	})
	require.NoError(t, err)
}

// TestWithheldBlobRefusedButRefKept (B12, acceptance): when a withhold
// refuses to store media, the materializer still writes the blob ref (same
// CID, mime, size) into the record — the record is never rebuilt without its
// image — and stores no bytes.
func TestWithheldBlobRefusedButRefKept(t *testing.T) {
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")

	cases := []struct {
		name string
		run  func(t *testing.T, h *harness, withholds *blobwithhold.Store)
	}{
		{
			// A stored post image is withheld, then the post is edited
			// upstream: the rebuilt record keeps the ref; after the lift the
			// image is served again with no further upstream change.
			name: "postv2 embed image re-materialized via update, then lifted",
			run: func(t *testing.T, h *harness, withholds *blobwithhold.Store) {
				ctx := context.Background()
				const postID = "https://lemmy.world/post/70301"
				imageBytes := []byte("b12-postv2-embed-image-withheld-then-lifted")
				h.serveLemmyWorldFixtures()
				h.mux.HandleFunc("GET /media/b12-embed.png", func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "image/png")
					_, _ = w.Write(imageBytes)
				})
				post := page(postID, personID, groupID, "a b12 image post", "2026-10-21T10:00:00.000000Z")
				post["attachment"] = []any{map[string]any{
					"type": "Image", "href": "https://lemmy.world/media/b12-embed.png",
					"mediaType": "image/png", "name": "a picture",
				}}
				_, err := h.m.MaterializePost(ctx, mustObject(t, post), groupID)
				require.NoError(t, err)
				stored := embedImageRef(t, h.recordFor(t, postID))
				require.Equal(t, "image/png", stored.MimeType)
				require.Equal(t, int64(len(imageBytes)), stored.Size)

				operatorWithhold(t, withholds, stored.CID)

				post["source"] = map[string]any{"content": "edited body text", "mediaType": "text/markdown"}
				_, err = h.m.HandleUpdate(ctx, mustObject(t, post), groupID)
				require.NoError(t, err)
				record := h.recordFor(t, postID)
				require.Equal(t, "edited body text", record["content"], "the update landed")
				assert.Equal(t, stored, embedImageRef(t, record),
					"the rebuilt record keeps the withheld image's ref")

				_, _, err = h.manager.GetBlob(ctx, authorDID, stored.CID)
				assert.True(t, errors.IsNotFound(err),
					"while withheld the image answers as absent, got %v", err)

				_, err = withholds.Lift(ctx, stored.CID, "b12-operator")
				require.NoError(t, err)
				data, mimeType, err := h.manager.GetBlob(ctx, authorDID, stored.CID)
				require.NoError(t, err, "after the lift the image is served again")
				assert.Equal(t, imageBytes, data)
				assert.Equal(t, "image/png", mimeType)
			},
		},
		{
			// The actor switches from one avatar to one whose CID is
			// withheld with its bytes deleted: the profile carries the new
			// ref (carry-forward of the old one cannot pass) and no bytes.
			name: "profile avatar refresh to a withheld avatar with bytes deleted",
			run: func(t *testing.T, h *harness, withholds *blobwithhold.Store) {
				ctx := context.Background()
				const actorID = "https://lemmy.world/u/b12avatar"
				firstBytes := []byte("b12-avatar-first-not-withheld")
				withheldBytes := []byte("b12-avatar-withheld-and-bytes-deleted")
				h.mux.HandleFunc("GET /media/b12-avatar-first.png", func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "image/png")
					_, _ = w.Write(firstBytes)
				})
				h.mux.HandleFunc("GET /media/b12-avatar-withheld.png", func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "image/png")
					_, _ = w.Write(withheldBytes)
				})
				var actorDoc atomic.Value
				actorDoc.Store(person(actorID, "b12avatar", map[string]any{
					"icon": map[string]any{"type": "Image", "url": "https://lemmy.world/media/b12-avatar-first.png"},
				}))
				h.mux.HandleFunc("GET /u/b12avatar", func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", ap.ContentTypeActivityJSON)
					_ = json.NewEncoder(w).Encode(actorDoc.Load())
				})

				actor, err := h.m.EnsureActor(ctx, &ap.Object{ID: actorID})
				require.NoError(t, err)
				profile, _, err := h.manager.GetRecord(ctx, actor.DID, CollectionActorProfile, ProfileRKey)
				require.NoError(t, err)
				require.Contains(t, profile, "avatar", "setup: the first avatar is stored")
				first := blobRefOf(t, profile["avatar"])

				// The withheld avatar's bytes once existed elsewhere and were
				// deleted under the withhold.
				elsewhere, err := h.manager.PutBlob(ctx, testDIDFor("b12elsewhere", "lemmy.world"), "image/png", withheldBytes)
				require.NoError(t, err)
				withheldCID := cid.Cid(elsewhere.Ref).String()
				require.NotEqual(t, first.CID, withheldCID)
				operatorWithhold(t, withholds, withheldCID)
				_, err = withholds.DeleteBytes(ctx, withheldCID, "b12-operator")
				require.NoError(t, err)

				actorDoc.Store(person(actorID, "b12avatar", map[string]any{
					"icon": map[string]any{"type": "Image", "url": "https://lemmy.world/media/b12-avatar-withheld.png"},
				}))
				_, err = h.m.RefreshActor(ctx, &ap.Object{ID: actorID})
				require.NoError(t, err)
				profile, _, err = h.manager.GetRecord(ctx, actor.DID, CollectionActorProfile, ProfileRKey)
				require.NoError(t, err)
				require.Contains(t, profile, "avatar", "the refreshed profile keeps an avatar ref")
				assert.Equal(t, withheldBlobRef{CID: withheldCID, MimeType: "image/png", Size: int64(len(withheldBytes))},
					blobRefOf(t, profile["avatar"]), "the profile carries the withheld avatar's ref")
				assert.Empty(t, blobHoldersOfCID(t, withheldCID),
					"no blobs row may exist for a withheld CID whose bytes were deleted")
			},
		},
		{
			// The external thumb's CID is withheld before the post is ever
			// seen: nothing is stored, the ref is still written.
			name: "external thumb withheld before first materialization",
			run: func(t *testing.T, h *harness, withholds *blobwithhold.Store) {
				// The raw sha-256 CIDv1 of thumbBytes, computed once outside
				// the code under test.
				const thumbCID = "bafkreifxr246m5pgxozyx5mkmkaazwe2nov2fxgkbyco6iftdfwn656oh4"
				thumb := []byte("b12-external-thumb-withheld-before-first-materialization")
				h.mux.HandleFunc("GET /media/b12-thumb.jpg", func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "image/jpeg")
					_, _ = w.Write(thumb)
				})
				operatorWithhold(t, withholds, thumbCID)

				record := materializeLinkThumbnailPost(t, h, linkThumbnailPage(t, "https://example.com/b12-article",
					map[string]any{"type": "Image", "url": "https://lemmy.world/media/b12-thumb.jpg"}))
				external := externalOf(t, record)
				require.Contains(t, external, "thumb", "the external embed keeps the withheld thumb's ref")
				assert.Equal(t, withheldBlobRef{CID: thumbCID, MimeType: "image/jpeg", Size: int64(len(thumb))},
					blobRefOf(t, external["thumb"]))
				assert.Empty(t, blobHoldersOfCID(t, thumbCID), "a withheld thumb must not be stored")
			},
		},
		{
			// AVIF is transcoded before storing: the withheld CID is the
			// stored JPEG's. Withheld with bytes deleted, an update rebuilds
			// the record with that same ref and re-stores nothing.
			name: "AVIF embed image withheld by its transcoded CID",
			run: func(t *testing.T, h *harness, withholds *blobwithhold.Store) {
				ctx := context.Background()
				const postID = "https://lemmy.world/post/70302"
				avifBytes := encodeAVIFFixture(t, filledImage(44, 33, color.NRGBA{R: 12, G: 34, B: 210, A: 255}), avifFixtureOptions)
				h.serveLemmyWorldFixtures()
				h.mux.HandleFunc("GET /media/b12-picture.avif", func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "image/avif")
					_, _ = w.Write(avifBytes)
				})
				post := page(postID, personID, groupID, "a b12 avif post", "2026-10-21T11:00:00.000000Z")
				post["attachment"] = []any{map[string]any{
					"type": "Image", "href": "https://lemmy.world/media/b12-picture.avif", "name": "a picture",
				}}
				_, err := h.m.MaterializePost(ctx, mustObject(t, post), groupID)
				require.NoError(t, err)
				stored := embedImageRef(t, h.recordFor(t, postID))
				require.Equal(t, "image/jpeg", stored.MimeType, "setup: the AVIF was transcoded")
				require.Equal(t, []string{authorDID}, blobHoldersOfCID(t, stored.CID), "setup: the transcoded blob is stored")

				operatorWithhold(t, withholds, stored.CID)
				_, err = withholds.DeleteBytes(ctx, stored.CID, "b12-operator")
				require.NoError(t, err)

				post["source"] = map[string]any{"content": "edited avif body", "mediaType": "text/markdown"}
				_, err = h.m.HandleUpdate(ctx, mustObject(t, post), groupID)
				require.NoError(t, err)
				record := h.recordFor(t, postID)
				require.Equal(t, "edited avif body", record["content"], "the update landed")
				assert.Equal(t, stored, embedImageRef(t, record),
					"the rebuilt record keeps the transcoded blob's ref")
				assert.Empty(t, blobHoldersOfCID(t, stored.CID),
					"the withheld transcoded blob must not be re-stored")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			tc.run(t, h, blobwithhold.NewStore(testutil.DB(t)))
		})
	}
}

// TestDeletedPostRedeliveryFetchesNoMedia: a re-materialization of a Page
// whose mapping is soft-deleted is refused before the record is built, so its
// media is neither fetched nor stored. Storing it would leave bytes under the
// author's DID that no record references and no withhold covers — the author
// delete may have just deleted that image's bytes and lifted its withhold.
func TestDeletedPostRedeliveryFetchesNoMedia(t *testing.T) {
	h := newHarness(t)
	h.serveLemmyWorldFixtures()
	ctx := context.Background()
	authorDID := testDIDFor("LeftLeaningFreedomFighters", "lemmy.world")
	const postID = "https://lemmy.world/post/70399"

	var imageRequests atomic.Int32
	h.mux.HandleFunc("GET /media/deleted-post-embed.png", func(w http.ResponseWriter, _ *http.Request) {
		imageRequests.Add(1)
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write([]byte("deleted-post-embed-image-bytes"))
	})

	post := page(postID, personID, groupID, "a post deleted upstream", "2026-10-21T10:00:00.000000Z")
	_, err := h.m.MaterializePost(ctx, mustObject(t, post), groupID)
	require.NoError(t, err)
	require.NoError(t, h.objects.SoftDelete(ctx, postID))

	var blobsBefore int
	require.NoError(t, testutil.DB(t).QueryRow(
		`SELECT count(*) FROM blobs WHERE did = $1`, authorDID).Scan(&blobsBefore))

	// A stale edit of the deleted post, now carrying an image.
	post["attachment"] = []any{map[string]any{
		"type": "Image", "href": "https://lemmy.world/media/deleted-post-embed.png",
		"mediaType": "image/png", "name": "a picture",
	}}
	res, err := h.m.MaterializePost(ctx, mustObject(t, post), groupID)
	require.Error(t, err)
	assert.Nil(t, res)
	assert.True(t, IsSkip(err), "a deleted post's redelivery is a skip, got %v", err)
	assert.Contains(t, err.Error(), "object was deleted upstream; not resurrecting")
	assert.False(t, errors.IsCollectionImmutable(err))

	assert.Zero(t, imageRequests.Load(), "a deleted post's media must not be fetched")
	var blobsAfter int
	require.NoError(t, testutil.DB(t).QueryRow(
		`SELECT count(*) FROM blobs WHERE did = $1`, authorDID).Scan(&blobsAfter))
	assert.Equal(t, blobsBefore, blobsAfter, "no blob may be stored for a deleted post")

	after, err := h.objects.GetByAPID(ctx, postID)
	require.NoError(t, err)
	assert.True(t, after.IsDeleted(), "the post mapping must stay deleted")
}
