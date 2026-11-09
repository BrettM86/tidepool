package apobject

import (
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/ipfs/go-cid"
)

const (
	externalEmbedType             = "social.coves.embed.external"
	imagesEmbedType               = "social.coves.embed.images"
	imagesWithAttachmentEmbedType = "social.coves.embed.imagesWithAttachment"

	// imageProxyPath is the Coves image proxy route for a full-size image:
	// <base>/img/content_full/plain/<did>/<cid>.
	imageProxyPath = "/img/content_full/plain/"
)

// servableImageMimeTypes are the blob types the Coves image proxy serves.
var servableImageMimeTypes = map[string]bool{
	"image/png":  true,
	"image/jpeg": true,
	"image/webp": true,
	"image/gif":  true,
}

// MediaSource says where a record's gallery images are served: BaseURL is
// COVES_MEDIA_BASE_URL and AuthorDID is the record's at-uri repo. The zero
// value yields no image URLs.
type MediaSource struct {
	BaseURL   string
	AuthorDID string
}

// GalleryImage is one record gallery entry: its proxy URL ("" when the entry
// is skipped) and its alt text.
type GalleryImage struct {
	URL string
	Alt string
}

// GalleryImages returns one entry per record gallery entry in record order,
// read from embed.images (social.coves.embed.images) or
// embed.images.images (social.coves.embed.imagesWithAttachment). An entry's
// URL is "" when it has no blob ref, its $link is not a CID, its mimeType is
// not one the proxy serves, or media cannot build URLs (zero value or an
// invalid DID). No network is involved.
func GalleryImages(record map[string]any, media MediaSource) []GalleryImage {
	entries := galleryEntries(record)
	if len(entries) == 0 {
		return nil
	}
	canBuild := media.BaseURL != "" && media.AuthorDID != ""
	if canBuild {
		if _, err := syntax.ParseDID(media.AuthorDID); err != nil {
			canBuild = false
		}
	}
	images := make([]GalleryImage, len(entries))
	for position, raw := range entries {
		entry, _ := raw.(map[string]any)
		alt, _ := entry["alt"].(string)
		images[position].Alt = alt
		if !canBuild {
			continue
		}
		if blobCID, ok := servableBlobCID(entry["image"]); ok {
			images[position].URL = media.BaseURL + imageProxyPath + media.AuthorDID + "/" + blobCID
		}
	}
	return images
}

// galleryMediaType returns the blob mimeType of the record's gallery entry at
// position.
func galleryMediaType(record map[string]any, position int) string {
	entry, _ := galleryEntries(record)[position].(map[string]any)
	blob, _ := entry["image"].(map[string]any)
	mimeType, _ := blob["mimeType"].(string)
	return mimeType
}

// galleryEntries returns the record's raw gallery entries, or nil when the
// embed carries no image gallery.
func galleryEntries(record map[string]any) []any {
	embed, _ := record["embed"].(map[string]any)
	embedType, _ := embed["$type"].(string)
	switch embedType {
	case imagesEmbedType:
		entries, _ := embed["images"].([]any)
		return entries
	case imagesWithAttachmentEmbedType:
		gallery, _ := embed["images"].(map[string]any)
		entries, _ := gallery["images"].([]any)
		return entries
	default:
		return nil
	}
}

// servableBlobCID returns the canonical CID string of a blob the proxy can
// serve.
func servableBlobCID(raw any) (string, bool) {
	blob, ok := raw.(map[string]any)
	if !ok {
		return "", false
	}
	mimeType, _ := blob["mimeType"].(string)
	if !servableImageMimeTypes[mimeType] {
		return "", false
	}
	ref, _ := blob["ref"].(map[string]any)
	link, _ := ref["$link"].(string)
	if link == "" {
		return "", false
	}
	parsed, err := cid.Decode(link)
	if err != nil || !parsed.Defined() {
		return "", false
	}
	return parsed.String(), true
}
