package materialize

import (
	"context"
	"errors"
	"mime"
	"net/http"
	"strings"

	"github.com/bluesky-social/indigo/atproto/atdata"

	"tidepool/internal/ap"
	"tidepool/internal/media"
)

// blobSlot describes the lexicon slot a fetched image will fill: its
// size cap and accepted mime types come straight from the vendored lexicon
// definitions. Everything fails closed — wrong type or oversize means no
// blob, never a truncated or mislabeled one.
type blobSlot struct {
	name    string
	maxSize int64
	accept  []string
}

var (
	// social.coves.actor.profile / community.profile avatar+banner.
	slotAvatar = blobSlot{"avatar", 1_000_000, []string{"image/png", "image/jpeg", "image/webp"}}
	slotBanner = blobSlot{"banner", 2_000_000, []string{"image/png", "image/jpeg", "image/webp"}}
	// social.coves.embed.images#image (posts allow gif).
	slotEmbedImage = blobSlot{"embed image", 10_000_000, []string{"image/png", "image/jpeg", "image/webp", "image/gif"}}
	// social.coves.embed.external#external thumb.
	slotExternalThumb = blobSlot{"external thumb", 6_000_000, []string{"image/png", "image/jpeg", "image/webp"}}
)

// fetchBlob downloads remote media through the SSRF-guarded AP client and
// stores it as a blob in the DID's repo, returning the blob ref for the
// record. AVIF is transcoded to JPEG, or PNG when it has transparency,
// because no lexicon slot accepts it. Media is always optional: every
// failure (unreachable, oversize, wrong content type, failed transcode) logs
// a reason and returns nil so the caller omits the image rather than
// dropping the record.
func (m *Materializer) fetchBlob(ctx context.Context, did, url string, slot blobSlot) *atdata.Blob {
	blob, _ := m.fetchBlobClassified(ctx, did, url, slot)
	return blob
}

// fetchBlobClassified is fetchBlob that also reports the ORIGIN fetch error,
// so carry-forward callers can distinguish a permanent removal (IsNotFound
// for 404/401/403, IsTombstoned for 410 — the image is gone at the source)
// from a transient failure (timeout/5xx/dial — the image is probably still
// there). Only the download error is surfaced; unusable-media outcomes
// (wrong content type, oversize, failed AVIF transcode — including a busy
// decoder — or blob-store failure) return (nil, nil), meaning no removal
// signal: a post image is omitted this pass, and profile media carries the
// stored blob forward.
func (m *Materializer) fetchBlobClassified(ctx context.Context, did, url string, slot blobSlot) (*atdata.Blob, error) {
	if url == "" {
		return nil, nil
	}
	budget := slot.maxSize
	if m.maxBlob < budget {
		budget = m.maxBlob
	}
	data, contentType, err := m.fetcher.FetchMedia(ctx, url, budget)
	if err != nil {
		m.logger.Warn("media fetch failed; omitting image",
			"slot", slot.name, "url", url, "error", err)
		return nil, err
	}
	mimeType := normalizeImageMime(contentType, data)
	// No slot accepts AVIF, and servers mislabel it, so it is detected from
	// the header or the bytes and transcoded under the same budget.
	if mimeType == "image/avif" || media.IsAVIF(data) {
		data, mimeType, err = media.TranscodeAVIF(ctx, data, budget)
		if err != nil {
			// A busy decoder or a cancelled ingest says nothing about the
			// media, so it is logged apart from bad media.
			if reason := transcodeSkipReason(err); reason != "" {
				m.logger.Warn("media transcode skipped; omitting image",
					"slot", slot.name, "url", url, "reason", reason, "error", err)
				return nil, nil
			}
			m.logger.Warn("media transcode failed; omitting image",
				"slot", slot.name, "url", url, "error", err)
			return nil, nil
		}
	}
	if !slotAccepts(slot, mimeType) {
		m.logger.Warn("media has unacceptable content type; omitting image",
			"slot", slot.name, "url", url, "content_type", mimeType)
		return nil, nil
	}
	blob, err := m.repos.PutBlob(ctx, did, mimeType, data)
	if err != nil {
		m.logger.Warn("blob store failed; omitting image",
			"slot", slot.name, "url", url, "did", did, "error", err)
		return nil, nil
	}
	return blob, nil
}

// transcodeSkipReason names why a transcode never looked at the media (the
// decoder was busy, or the context ended), or returns "" when the error is
// about the media itself.
func transcodeSkipReason(err error) string {
	switch {
	case errors.Is(err, media.ErrDecoderBusy):
		return "decoder busy"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "context ended"
	default:
		return ""
	}
}

// normalizeImageMime parses the server's Content-Type; when it is absent,
// generic, or unparseable the bytes are sniffed instead (remote instances
// sometimes serve images as application/octet-stream).
func normalizeImageMime(contentType string, data []byte) string {
	parsed, _, err := mime.ParseMediaType(contentType)
	if err != nil || parsed == "" || parsed == "application/octet-stream" {
		parsed, _, err = mime.ParseMediaType(http.DetectContentType(data))
		if err != nil {
			return ""
		}
	}
	return strings.ToLower(parsed)
}

func slotAccepts(slot blobSlot, mimeType string) bool {
	for _, accepted := range slot.accept {
		if mimeType == accepted {
			return true
		}
	}
	return false
}

// imageURL extracts the URL of an AP Image object (icon/image fields):
// Lemmy uses {type: Image, url: "..."}.
func imageURL(img *ap.Object) string {
	if img == nil {
		return ""
	}
	if u := img.URLString(); u != "" {
		return u
	}
	return img.ID
}
