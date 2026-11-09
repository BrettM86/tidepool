package outbound

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/apobject"
	"tidepool/internal/consume"
)

// A comment's #image facet places its gallery image under the configured media
// base, in the repo its at-uri names, on both the delivered Create{Note} and the
// served Note. The base is only joined into strings; nothing fetches it.

const (
	noteMediaBase  = "https://img.example"
	noteMediaATURI = "at://did:plc:abc/social.coves.community.comment/r1"
)

func noteMediaSnapshot(t *testing.T) []byte {
	t.Helper()
	snap, err := json.Marshal(map[string]any{
		"atUri":      noteMediaATURI,
		"collection": "social.coves.community.comment",
		"record": map[string]any{
			"content": "see pic now",
			"facets": []any{map[string]any{
				"index": map[string]any{"byteStart": 4, "byteEnd": 7},
				"features": []any{map[string]any{
					"$type": "social.coves.richtext.facet#image", "index": 0,
				}},
			}},
			"embed": map[string]any{
				"$type": "social.coves.embed.images",
				"images": []any{map[string]any{
					"image": map[string]any{
						"$type":    "blob",
						"ref":      map[string]any{"$link": "bafkreihak3l6grklnl53pnqjtrwfph7wyojvrsaazb7nmptucuhobephqy"},
						"mimeType": "image/png",
						"size":     1234,
					},
					"alt": "a photo",
				}},
			},
			"createdAt": "2026-08-12T10:00:00.000Z",
		},
		"parentAtUri":   "at://did:plc:p/social.coves.community.postv2/rp",
		"parentApId":    "https://lemmy.world/post/1",
		"communityApId": "https://lemmy.world/c/tech",
	})
	require.NoError(t, err)
	return snap
}

func assertNoteImagePlaced(t *testing.T, note map[string]any) {
	t.Helper()
	source := mustMap(t, note["source"], "Note.source")
	assert.Equal(t,
		"see ![a photo](https://img.example/img/content_full/plain/did:plc:abc/bafkreihak3l6grklnl53pnqjtrwfph7wyojvrsaazb7nmptucuhobephqy) now",
		source["content"],
		"the source places the gallery image at the facet, under the media base, in the at-uri's repo")
	assert.Equal(t,
		`<p>see <img src="https://img.example/img/content_full/plain/did:plc:abc/bafkreihak3l6grklnl53pnqjtrwfph7wyojvrsaazb7nmptucuhobephqy" alt="a photo"> now</p>`+"\n",
		note["content"],
		"the content places the gallery image at the facet, under the media base, in the at-uri's repo")
}

func TestTranslator_CommentImageFacetUsesMediaBase(t *testing.T) {
	tr := NewTranslator("https://coves.social", noteMediaBase)
	out, err := tr.Translate("https://coves.social/ap/actor/did:plc:abc", consume.CommentIntent{
		Op:            "create",
		ATURI:         noteMediaATURI,
		ID:            "https://coves.social/ap/activity/deadbeef",
		CommunityAPID: "https://lemmy.world/c/tech",
		ParentAPID:    "https://lemmy.world/post/1",
		Snapshot:      noteMediaSnapshot(t),
	})
	require.NoError(t, err)
	require.Equal(t, "Create", out.Kind)

	var activity map[string]any
	require.NoError(t, json.Unmarshal(out.Payload, &activity))
	assertNoteImagePlaced(t, mustMap(t, activity["object"], "object"))
}

func TestRenderObject_CommentImageFacetUsesMediaBase(t *testing.T) {
	served, err := apobject.RenderObject("https://coves.social", noteMediaBase, noteMediaSnapshot(t))
	require.NoError(t, err)
	assert.Equal(t, "Note", served["type"])
	assertNoteImagePlaced(t, served)
}

// An image-only comment (empty content, two unplaced gallery images) renders a
// Create whose Note carries both images in attachment[] and an empty body.
func TestTranslator_ImageOnlyCommentCreatesNoteWithAttachments(t *testing.T) {
	snap, err := json.Marshal(map[string]any{
		"atUri":      noteMediaATURI,
		"collection": "social.coves.community.comment",
		"record": map[string]any{
			"content": "",
			"embed": map[string]any{
				"$type": "social.coves.embed.images",
				"images": []any{
					map[string]any{
						"image": map[string]any{
							"$type":    "blob",
							"ref":      map[string]any{"$link": "bafkreihak3l6grklnl53pnqjtrwfph7wyojvrsaazb7nmptucuhobephqy"},
							"mimeType": "image/png",
							"size":     1234,
						},
						"alt": "a photo",
					},
					map[string]any{
						"image": map[string]any{
							"$type":    "blob",
							"ref":      map[string]any{"$link": "bafkreihh33brlfst3dssji2k4fllfvevfp6q6c2zrqfm2soybkt2pq3lui"},
							"mimeType": "image/jpeg",
							"size":     1234,
						},
						"alt": "",
					},
				},
			},
			"createdAt": "2026-08-12T10:00:00.000Z",
		},
		"parentAtUri":   "at://did:plc:p/social.coves.community.postv2/rp",
		"parentApId":    "https://lemmy.world/post/1",
		"communityApId": "https://lemmy.world/c/tech",
	})
	require.NoError(t, err)

	tr := NewTranslator("https://coves.social", noteMediaBase)
	out, err := tr.Translate("https://coves.social/ap/actor/did:plc:abc", consume.CommentIntent{
		Op:            "create",
		ATURI:         noteMediaATURI,
		ID:            "https://coves.social/ap/activity/deadbeef",
		CommunityAPID: "https://lemmy.world/c/tech",
		ParentAPID:    "https://lemmy.world/post/1",
		Snapshot:      snap,
	})
	require.NoError(t, err)
	require.Equal(t, "Create", out.Kind)

	var activity map[string]any
	require.NoError(t, json.Unmarshal(out.Payload, &activity))
	object, err := json.Marshal(activity["object"])
	require.NoError(t, err)
	assert.JSONEq(t, `{
		"type": "Note",
		"id": "https://coves.social/ap/object/did:plc:abc/social.coves.community.comment/r1",
		"attributedTo": "https://coves.social/ap/actor/did:plc:abc",
		"to": ["https://www.w3.org/ns/activitystreams#Public"],
		"cc": ["https://lemmy.world/c/tech"],
		"audience": "https://lemmy.world/c/tech",
		"inReplyTo": "https://lemmy.world/post/1",
		"published": "2026-08-12T10:00:00.000Z",
		"mediaType": "text/html",
		"content": "<p></p>\n",
		"source": {"content": "", "mediaType": "text/markdown"},
		"attachment": [
			{"type": "Image", "mediaType": "image/png", "name": "a photo",
			 "url": "https://img.example/img/content_full/plain/did:plc:abc/bafkreihak3l6grklnl53pnqjtrwfph7wyojvrsaazb7nmptucuhobephqy"},
			{"type": "Image", "mediaType": "image/jpeg", "name": "",
			 "url": "https://img.example/img/content_full/plain/did:plc:abc/bafkreihh33brlfst3dssji2k4fllfvevfp6q6c2zrqfm2soybkt2pq3lui"}
		]
	}`, string(object))
}
