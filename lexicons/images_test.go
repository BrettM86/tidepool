package lexicons

import (
	"testing"

	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/bluesky-social/indigo/atproto/lexicon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// validateAs is validate for any record NSID: the same
// atdata.UnmarshalJSON then lexicon.ValidateRecord path the materializer uses.
func validateAs(t *testing.T, nsid, raw string) error {
	t.Helper()
	catalog, err := Catalog()
	require.NoError(t, err)
	record, err := atdata.UnmarshalJSON([]byte(raw))
	require.NoError(t, err, "fixture must be valid JSON in the atproto data model")
	return lexicon.ValidateRecord(catalog, record, nsid, lexicon.ValidateFlags(0))
}

func TestPostImagesWithAttachmentEmbed(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{
			name: "images with an external link attachment",
			raw: `{
				"$type": "social.coves.community.postv2",
				"community": "did:plc:communityabc123",
				"title": "Photos from the trail",
				"content": "See the gallery and the trail guide.",
				"createdAt": "2026-11-05T12:00:00.000Z",
				"embed": {
					"$type": "social.coves.embed.imagesWithAttachment",
					"images": {
						"images": [
							{
								"image": {"$type": "blob", "ref": {"$link": "bafkreib33oum3wmf36mezw4nv2nf3kolsdorvwtxfsjm7aj3rdekmbrlqy"}, "mimeType": "image/jpeg", "size": 12345},
								"alt": "A trail through pine trees"
							}
						]
					},
					"attachment": {
						"$type": "social.coves.embed.external",
						"external": {"uri": "https://example.com/trail-guide"}
					}
				}
			}`,
			wantErr: false,
		},
		{
			name: "attachment is required",
			raw: `{
				"$type": "social.coves.community.postv2",
				"community": "did:plc:communityabc123",
				"title": "Photos from the trail",
				"content": "See the gallery and the trail guide.",
				"createdAt": "2026-11-05T12:00:00.000Z",
				"embed": {
					"$type": "social.coves.embed.imagesWithAttachment",
					"images": {
						"images": [
							{
								"image": {"$type": "blob", "ref": {"$link": "bafkreib33oum3wmf36mezw4nv2nf3kolsdorvwtxfsjm7aj3rdekmbrlqy"}, "mimeType": "image/jpeg", "size": 12345},
								"alt": "A trail through pine trees"
							}
						]
					}
				}
			}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateAs(t, "social.coves.community.postv2", tt.raw)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCommentImageFacet(t *testing.T) {
	commentWithImageIndex := func(index string) string {
		return `{
			"$type": "social.coves.community.comment",
			"reply": {
				"root": {"uri": "at://did:plc:authorabc123/social.coves.community.postv2/3kabcdefghij2", "cid": "bafyreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"},
				"parent": {"uri": "at://did:plc:authorabc123/social.coves.community.postv2/3kabcdefghij2", "cid": "bafyreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"}
			},
			"content": "Here is the view: [photo]",
			"createdAt": "2026-11-05T12:00:00.000Z",
			"facets": [
				{
					"index": {"byteStart": 18, "byteEnd": 25},
					"features": [{"$type": "social.coves.richtext.facet#image", "index": ` + index + `}]
				}
			],
			"embed": {
				"$type": "social.coves.embed.images",
				"images": [
					{
						"image": {"$type": "blob", "ref": {"$link": "bafkreib33oum3wmf36mezw4nv2nf3kolsdorvwtxfsjm7aj3rdekmbrlqy"}, "mimeType": "image/jpeg", "size": 12345},
						"alt": "A lake at sunset"
					}
				]
			}
		}`
	}

	tests := []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{name: "image facet places the first image", raw: commentWithImageIndex("0"), wantErr: false},
		{name: "image facet index above 7 is rejected", raw: commentWithImageIndex("8"), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateAs(t, "social.coves.community.comment", tt.raw)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
