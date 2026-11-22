package materialize

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"strings"

	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/ipfs/go-cid"

	"tidepool/internal/ap"
	tperrors "tidepool/internal/errors"
	"tidepool/internal/media"
)

// blobSlot describes the lexicon slot a fetched image will fill: its
// size cap and accepted mime types come straight from the vendored lexicon
// definitions. maxLongEdge caps the long edge, in pixels, of a still image
// re-encoded because it was over the size cap. Everything fails closed — a
// wrong type, or oversize media that cannot be shrunk to fit, means no blob,
// never a truncated or mislabeled one.
type blobSlot struct {
	name        string
	maxSize     int64
	accept      []string
	maxLongEdge int
}

var (
	// social.coves.actor.profile / community.profile avatar+banner.
	slotAvatar = blobSlot{"avatar", 1_000_000, []string{"image/png", "image/jpeg", "image/webp"}, 1000}
	slotBanner = blobSlot{"banner", 2_000_000, []string{"image/png", "image/jpeg", "image/webp"}, 3000}
	// social.coves.embed.images#image (posts allow gif). 4096 matches the
	// AVIF transcode's output cap, so tall comics keep a readable width.
	slotEmbedImage = blobSlot{"embed image", 10_000_000, []string{"image/png", "image/jpeg", "image/webp", "image/gif"}, 4096}
	// social.coves.embed.external#external thumb.
	slotExternalThumb = blobSlot{"external thumb", 6_000_000, []string{"image/png", "image/jpeg", "image/webp"}, 2000}
)

// fetchBlob downloads remote media through the SSRF-guarded AP client and
// stores it as a blob in the DID's repo, returning the blob ref for the
// record. AVIF is transcoded to JPEG, or PNG when it has transparency,
// because no lexicon slot accepts it. A still image over the slot's size
// cap is shrunk and re-encoded until it fits (see media.FitStill). Problems
// with the media itself (unreachable at origin, over the download budget,
// animated and oversize, wrong content type, failed transcode or fit)
// log a reason and return (nil, nil) so the caller omits the image rather
// than dropping the record. A byte-store failure is not about the media: it
// returns an error satisfying errors.IsBlobStoreUnavailable, and the caller
// fails the materialization so the work is retried with its image.
func (m *Materializer) fetchBlob(ctx context.Context, did, url string, slot blobSlot) (*atdata.Blob, error) {
	blob, err := m.fetchBlobClassified(ctx, did, url, slot)
	if tperrors.IsBlobStoreUnavailable(err) {
		return nil, err
	}
	return blob, nil
}

// fetchBlobClassified is fetchBlob that also reports the ORIGIN fetch error,
// so carry-forward callers can distinguish a permanent removal (IsNotFound
// for 404/401/403, IsTombstoned for 410 — the image is gone at the source)
// from a transient failure (timeout/5xx/dial — the image is probably still
// there). Unusable-media outcomes (wrong content type, oversize media that
// cannot be fitted, failed AVIF transcode — including a busy decoder)
// return (nil, nil), meaning no
// removal signal: a post image is omitted this pass, and profile media
// carries the stored blob forward. A byte-store failure returns an error
// satisfying errors.IsBlobStoreUnavailable, which callers check before any
// origin classification. A store refused by a blob withhold returns the
// computed ref with a nil error: the record keeps the ref, no bytes stored.
func (m *Materializer) fetchBlobClassified(ctx context.Context, did, url string, slot blobSlot) (*atdata.Blob, error) {
	blob, _, err := m.fetchMedia(ctx, did, url, slot)
	return blob, err
}

// fetchMedia is fetchBlobClassified that also returns the bytes the blob ref
// describes (after any transcode or fit), for callers that read the image
// header.
// The bytes are non-nil exactly when the blob is, including a withheld blob
// whose bytes were not stored.
func (m *Materializer) fetchMedia(ctx context.Context, did, url string, slot blobSlot) (*atdata.Blob, []byte, error) {
	return m.fetchAndStoreMedia(ctx, ctx, did, url, slot)
}

// fetchAndStoreMedia is fetchMedia with the origin download bounded by
// fetchCtx and the transcode, fit and byte-store write by storeCtx, so a
// caller's download deadline never cuts the store and turns a store outage
// into a missing image.
//
// The download is bounded by the download budget, the stored blob by the
// slot's cap clamped by MaxBlobBytes. Media between the two is fitted
// before the slot's type check, so an oversized still GIF reaches a slot
// that does not take GIF as JPEG or PNG; media under the store cap is
// stored as served.
func (m *Materializer) fetchAndStoreMedia(fetchCtx, storeCtx context.Context, did, url string, slot blobSlot) (*atdata.Blob, []byte, error) {
	if url == "" {
		return nil, nil, nil
	}
	storeCap := min(slot.maxSize, m.maxBlob)
	data, contentType, err := m.fetcher.FetchMedia(fetchCtx, url, m.maxDownload)
	if err != nil {
		m.logger.Warn("media fetch failed; omitting image",
			"slot", slot.name, "url", url, "error", err)
		return nil, nil, err
	}
	mimeType := normalizeImageMime(contentType, data)
	// No slot accepts AVIF, and servers mislabel it, so it is detected from
	// the header or the bytes and transcoded under the store cap.
	if mimeType == "image/avif" || media.IsAVIF(data) {
		data, mimeType, err = media.TranscodeAVIF(storeCtx, data, storeCap)
		if err != nil {
			// A busy decoder or a cancelled ingest says nothing about the
			// media, so it is logged apart from bad media.
			if reason := transcodeSkipReason(err); reason != "" {
				m.logger.Warn("media transcode skipped; omitting image",
					"slot", slot.name, "url", url, "reason", reason, "error", err)
				return nil, nil, nil
			}
			m.logger.Warn("media transcode failed; omitting image",
				"slot", slot.name, "url", url, "error", err)
			return nil, nil, nil
		}
	}
	if int64(len(data)) > storeCap {
		data, mimeType, err = media.FitStill(storeCtx, data, mimeType, storeCap, slot.maxLongEdge)
		if err != nil {
			if reason := transcodeSkipReason(err); reason != "" {
				m.logger.Warn("media fit skipped; omitting image",
					"slot", slot.name, "url", url, "reason", reason, "error", err)
				return nil, nil, nil
			}
			m.logger.Warn("media over slot cap could not be fitted; omitting image",
				"slot", slot.name, "url", url, "error", err)
			return nil, nil, nil
		}
	}
	if !slotAccepts(slot, mimeType) {
		m.logger.Warn("media has unacceptable content type; omitting image",
			"slot", slot.name, "url", url, "content_type", mimeType)
		return nil, nil, nil
	}
	blob, err := m.repos.PutBlob(storeCtx, did, mimeType, data)
	if tperrors.IsBlobWithheld(err) && blob != nil {
		// A withhold refused the bytes, not the reference: the record keeps
		// the ref so lifting the withhold serves the image again without a
		// further upstream edit.
		m.logger.Info("media withheld; keeping blob ref without storing bytes",
			"slot", slot.name, "did", did, "cid", cid.Cid(blob.Ref).String())
		return blob, data, nil
	}
	if tperrors.IsBlobStoreUnavailable(err) {
		// The byte store failing says nothing about the media; PutBlob has
		// logged it. The caller fails the materialization so it is retried.
		return nil, nil, fmt.Errorf("materialize: store %s from %s: %w", slot.name, url, err)
	}
	if err != nil {
		m.logger.Warn("blob store refused media; omitting image",
			"slot", slot.name, "url", url, "did", did, "error", err)
		return nil, nil, nil
	}
	return blob, data, nil
}

// transcodeSkipReason names why a transcode or fit never finished looking at
// the media (the decoder was busy, or the context ended), or returns "" when
// the error is about the media itself.
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
