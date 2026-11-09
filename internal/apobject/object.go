// Package apobject owns the AP OBJECT wire shape (Note/Page). It is the single
// source both the outbound Translator (which wraps the object in a
// Create/Update activity for delivery) and the user-origin serving surface (GET
// /ap/object/{did}/{collection}/{rkey}, via RenderObject) render from — so a
// peer that re-fetches a delivered object by id gets byte-compatible addressing
// (the same to/cc/audience split Lemmy parsed on delivery). It lives in its own
// low-level package (importing only ap + errors) so both the delivery layer and
// the identity-serving layer can share it without an import cycle.
package apobject

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/rivo/uniseg"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
)

// ContextActivityStreams is the AS2 context every object/activity we emit
// carries. The Note/Page shapes use only core AS2 terms (attributedTo, source,
// mediaType, audience), so the plain context is sufficient for Lemmy 0.19.20.
const ContextActivityStreams = "https://www.w3.org/ns/activitystreams"

// The lexicon limits on a native record's rendered body: content per record
// type (social.coves.community.postv2 and social.coves.community.comment),
// facets shared by both, and features per social.coves.richtext.facet. A string
// maxLength counts UTF-8 bytes.
const (
	MaximumPostContentBytes        = 500000
	MaximumPostContentGraphemes    = 50000
	MaximumCommentContentBytes     = 100000
	MaximumCommentContentGraphemes = 10000
	MaximumFacets                  = 200
	MaximumFeaturesPerFacet        = 20
)

// CheckPostRecordLimits rejects a postv2 record whose content or facets exceed
// the postv2 lexicon limits. See checkRecordLimits.
func CheckPostRecordLimits(record map[string]any) error {
	return checkRecordLimits(record, MaximumPostContentBytes, MaximumPostContentGraphemes)
}

// CheckCommentRecordLimits rejects a comment record whose content or facets
// exceed the comment lexicon limits. See checkRecordLimits.
func CheckCommentRecordLimits(record map[string]any) error {
	return checkRecordLimits(record, MaximumCommentContentBytes, MaximumCommentContentGraphemes)
}

// checkRecordLimits rejects a native record whose content or facets exceed the
// lexicon limits. A postv2 is lexicon-validated by the acceptance engine before
// it is ever snapshotted; a comment is not validated anywhere upstream, so every
// render path checks this before rendering. An over-limit record is REJECTED,
// never rendered with facets dropped: dropping a spoiler facet would publish the
// text it hides. Only the shape the lexicon bounds is counted here — a facets
// value that is not an array is left to the renderer, which ignores it.
func checkRecordLimits(record map[string]any, maximumContentBytes, maximumContentGraphemes int) error {
	content, _ := record["content"].(string)
	if len(content) > maximumContentBytes {
		return errors.NewValidationError("record.content",
			fmt.Sprintf("exceeds the lexicon maximum of %d bytes", maximumContentBytes))
	}
	if uniseg.GraphemeClusterCount(content) > maximumContentGraphemes {
		return errors.NewValidationError("record.content",
			fmt.Sprintf("exceeds the lexicon maximum of %d graphemes", maximumContentGraphemes))
	}
	facets, _ := record["facets"].([]any)
	if len(facets) > MaximumFacets {
		return errors.NewValidationError("record.facets",
			fmt.Sprintf("%d facets exceed the lexicon maximum of %d", len(facets), MaximumFacets))
	}
	for position, raw := range facets {
		facet, _ := raw.(map[string]any)
		features, _ := facet["features"].([]any)
		if len(features) > MaximumFeaturesPerFacet {
			return errors.NewValidationError("record.facets.features",
				fmt.Sprintf("facet %d has %d features, over the lexicon maximum of %d",
					position, len(features), MaximumFeaturesPerFacet))
		}
	}
	return nil
}

// BuildNote renders a comment as an AP Note. The Note carries the community in
// cc (the Note half of the Page/Note addressing split), to ⊇ as:Public,
// attributedTo as a SINGLE STRING, and HTML content alongside its markdown
// source. inReplyTo is emitted only when a parent is known. The caller runs
// CheckCommentRecordLimits first; BuildNote does not. media says where the
// record's gallery images are served, for the #image placements it renders;
// the gallery images no placement consumed go in attachment[] as Image
// objects, in gallery order, and never into the body.
func BuildNote(actorID, communityAPID, parentAPID, objectURL string, media MediaSource, record map[string]any) map[string]any {
	source, _ := record["content"].(string)
	gallery := GalleryImages(record, media)
	markdown, content, unplaced := renderBody(source, record["facets"], gallery)
	note := map[string]any{
		"type":         "Note",
		"id":           objectURL,
		"attributedTo": actorID,
		"to":           []string{ap.PublicAudience},
		"cc":           []string{communityAPID},
		"audience":     communityAPID,
		"mediaType":    "text/html",
		"content":      content,
		"source": map[string]any{
			"content":   markdown,
			"mediaType": "text/markdown",
		},
	}
	if len(unplaced) != 0 {
		attachment := make([]any, len(unplaced))
		for i, position := range unplaced {
			attachment[i] = imageAttachment(record, gallery, position)
		}
		note["attachment"] = attachment
	}
	if parentAPID != "" {
		note["inReplyTo"] = parentAPID
	}
	if published, ok := record["createdAt"].(string); ok && published != "" {
		note["published"] = published
	}
	return note
}

// BuildPage renders a post as an AP Page. The Page half of the split puts the
// community in `to` (alongside as:Public) and leaves cc empty — the crux Lemmy
// 0.19.20 keys on to tell a top-level post from a reply. name is REQUIRED
// (Lemmy rejects a titleless Page); attachment[0] is the post's link or
// image (see pageAttachment), and gallery images no #image placement consumed
// that do not fit there are appended to the body; an nsfw self-label becomes
// sensitive:true. media says where the record's gallery images are served.
func BuildPage(actorID, communityAPID, objectURL string, media MediaSource, record map[string]any) (map[string]any, error) {
	title, _ := record["title"].(string)
	if title == "" {
		return nil, errors.NewValidationError("title", "a Page requires a name (Lemmy rejects a titleless post)")
	}
	if err := CheckPostRecordLimits(record); err != nil {
		return nil, err
	}
	source, _ := record["content"].(string)
	gallery := GalleryImages(record, media)
	markdown, content, unplaced := renderBody(source, record["facets"], gallery)
	attachment, appended := pageAttachment(record, gallery, unplaced)
	markdown, content = appendImages(markdown, content, appended)
	page := map[string]any{
		"type":         "Page",
		"id":           objectURL,
		"attributedTo": actorID,
		// The Page/Note split: community in `to`, NOT cc.
		"to":        []string{communityAPID, ap.PublicAudience},
		"cc":        []string{},
		"name":      title,
		"audience":  communityAPID,
		"mediaType": "text/html",
		"content":   content,
		"source": map[string]any{
			"content":   markdown,
			"mediaType": "text/markdown",
		},
		"sensitive": hasNSFWLabel(record),
	}
	if attachment != nil {
		page["attachment"] = attachment
	}
	if published, ok := record["createdAt"].(string); ok && published != "" {
		page["published"] = published
	}
	return page, nil
}

// pageAttachment returns a Page's attachment (Lemmy reads the FIRST
// attachment as the post's link or image) and the unplaced gallery images to
// append to its body. unplaced are gallery positions in gallery order:
//   - social.coves.embed.external: the Link attachment, nothing appended;
//   - social.coves.embed.imagesWithAttachment whose attachment is
//     social.coves.embed.external: the Link attachment, every unplaced image
//     appended;
//   - social.coves.embed.images, or imagesWithAttachment with any other
//     attachment (a quoted post is dropped): the first unplaced image as an
//     Image attachment, the rest appended.
//
// No attachment (nil) when there is nothing to attach.
func pageAttachment(record map[string]any, gallery []GalleryImage, unplaced []int) ([]any, []GalleryImage) {
	embed, _ := record["embed"].(map[string]any)
	embedType, _ := embed["$type"].(string)
	switch embedType {
	case externalEmbedType:
		return linkAttachment(embed), nil
	case imagesWithAttachmentEmbedType:
		inner, _ := embed["attachment"].(map[string]any)
		if innerType, _ := inner["$type"].(string); innerType == externalEmbedType {
			return linkAttachment(inner), galleryImagesAt(gallery, unplaced)
		}
	case imagesEmbedType:
	default:
		return nil, nil
	}
	if len(unplaced) == 0 {
		return nil, nil
	}
	return []any{imageAttachment(record, gallery, unplaced[0])}, galleryImagesAt(gallery, unplaced[1:])
}

// linkAttachment maps a social.coves.embed.external embed (top-level, or an
// imagesWithAttachment's attachment) to [{type:Link, href}], or nil when it
// has no uri.
//
// The href is scheme-checked: a javascript:/data:/vbscript:/file: uri rendered
// as a clickable Link would be a stored-XSS-shaped hazard on every peer that
// renders it, so an unsafe scheme drops the attachment (fail closed) rather than
// federating it.
func linkAttachment(embed map[string]any) []any {
	external, ok := embed["external"].(map[string]any)
	if !ok {
		return nil
	}
	href, _ := external["uri"].(string)
	if href == "" || !isSafeLinkScheme(href) {
		return nil
	}
	return []any{map[string]any{"type": "Link", "href": href}}
}

// imageAttachment renders the gallery image at position as an AS2 Image.
// name is always present, "" for an empty alt.
func imageAttachment(record map[string]any, gallery []GalleryImage, position int) map[string]any {
	return map[string]any{
		"type":      "Image",
		"mediaType": galleryMediaType(record, position),
		"url":       gallery[position].URL,
		"name":      gallery[position].Alt,
	}
}

func galleryImagesAt(gallery []GalleryImage, positions []int) []GalleryImage {
	images := make([]GalleryImage, len(positions))
	for i, position := range positions {
		images[i] = gallery[position]
	}
	return images
}

// appendImages appends images to a rendered body, each as its own paragraph:
// separated from the body and each other by a blank line in markdown, and as
// <p><img></p> lines after the HTML. An empty body yields only the images, not
// the renderer's empty paragraph.
func appendImages(markdown, content string, images []GalleryImage) (string, string) {
	if len(images) == 0 {
		return markdown, content
	}
	var markdownBuilder, htmlBuilder strings.Builder
	markdownBuilder.WriteString(markdown)
	if markdown != "" {
		htmlBuilder.WriteString(content)
	}
	for i := range images {
		if markdownBuilder.Len() != 0 {
			markdownBuilder.WriteString("\n\n")
		}
		writeMarkdownImages(&markdownBuilder, images[i:i+1])
		htmlBuilder.WriteString("<p>")
		writeHTMLImages(&htmlBuilder, images[i:i+1])
		htmlBuilder.WriteString("</p>\n")
	}
	return markdownBuilder.String(), htmlBuilder.String()
}

// isSafeLinkScheme restricts a bridge-authored clickable URI to http/https. The
// lexicon's format:"uri" accepts javascript:/data:/vbscript:/file:, which a
// downstream client rendering the remote-controlled link as clickable would
// treat as a scripting or local-file URI.
func isSafeLinkScheme(uri string) bool {
	lower := strings.ToLower(strings.TrimSpace(uri))
	return strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://")
}

// hasNSFWLabel reports whether the record self-labels nsfw
// (com.atproto.label.defs#selfLabels with a value of "nsfw").
func hasNSFWLabel(record map[string]any) bool {
	labels, ok := record["labels"].(map[string]any)
	if !ok {
		return false
	}
	values, ok := labels["values"].([]any)
	if !ok {
		return false
	}
	for _, raw := range values {
		if entry, ok := raw.(map[string]any); ok {
			if val, _ := entry["val"].(string); val == "nsfw" {
				return true
			}
		}
	}
	return false
}

// RenderObject renders the AP object a served /ap/object/{did}/{collection}/{rkey}
// URL returns, from the durable outbound snapshot — the same Note/Page shape the
// Translator delivered, so a peer re-fetching the object by id sees consistent
// addressing. userOrigin derives the object's id and its author's actor id; no
// DB lookup happens (every id is in the snapshot or its at-uri). mediaBaseURL
// (COVES_MEDIA_BASE_URL) is where the record's gallery images are served, the
// same base the Translator renders under.
func RenderObject(userOrigin, mediaBaseURL string, snapshot []byte) (map[string]any, error) {
	snap, err := ParseSnapshot(snapshot)
	if err != nil {
		return nil, err
	}
	atURI, _ := snap["atUri"].(string)
	if atURI == "" {
		return nil, errors.NewValidationError("snapshot.atUri", "must not be empty")
	}
	trimmed := strings.TrimPrefix(atURI, "at://")
	objectURL := userOrigin + "/ap/object/" + trimmed
	did := trimmed
	if slash := strings.IndexByte(trimmed, '/'); slash >= 0 {
		did = trimmed[:slash]
	}
	actorID := userOrigin + "/ap/actor/" + did
	community, _ := snap["communityApId"].(string)
	parentAPID, _ := snap["parentApId"].(string)
	record, _ := snap["record"].(map[string]any)
	collection, _ := snap["collection"].(string)
	media := MediaSource{BaseURL: mediaBaseURL, AuthorDID: did}

	var object map[string]any
	if strings.Contains(collection, "postv2") {
		object, err = BuildPage(actorID, community, objectURL, media, record)
		if err != nil {
			return nil, err
		}
	} else {
		if err := CheckCommentRecordLimits(record); err != nil {
			return nil, err
		}
		object = BuildNote(actorID, community, parentAPID, objectURL, media, record)
	}
	if edited, _ := snap[SnapshotEditedKey].(bool); edited {
		SetUpdated(object, snap)
	}
	// Served standalone (not embedded in an activity), so it carries its own
	// context.
	object["@context"] = ContextActivityStreams
	return object, nil
}

// SnapshotEditedKey marks a snapshot whose activity was an Update, so the served
// object carries the same `updated` the Update delivered.
const SnapshotEditedKey = "edited"

// SetUpdated sets object["updated"] to the edit's time: the snapshot's rev as a
// TID, else its Jetstream timeUs. Lemmy 0.19.20 applies an Update only when its
// updated is strictly later than the copy it holds (its stored updated, else
// published, which is record.createdAt). A time at or before record.createdAt,
// or no time at all, becomes createdAt + 1 ms. That clears published for the
// first edit, but every later floored edit repeats the same time, so Lemmy
// applies only the first of them. Nothing is set when neither the rev, timeUs
// nor createdAt yields a time.
func SetUpdated(object, snapshot map[string]any) {
	var edited time.Time
	rev, _ := snapshot["rev"].(string)
	if tid, err := syntax.ParseTID(rev); err == nil {
		edited = tid.Time()
	} else if timeUs, ok := snapshot["timeUs"].(float64); ok && timeUs > 0 {
		edited = time.UnixMicro(int64(timeUs))
	}
	record, _ := snapshot["record"].(map[string]any)
	createdAt, _ := record["createdAt"].(string)
	if published, err := time.Parse(time.RFC3339, createdAt); err == nil && !edited.After(published) {
		edited = published.Add(time.Millisecond)
	}
	if edited.IsZero() {
		return
	}
	object["updated"] = edited.UTC().Format(time.RFC3339Nano)
}

// ParseSnapshot decodes the consumer's durable snapshot into a generic map so
// the object builders can read the raw record and resolved thread context
// without a round-trip through a typed AP object.
func ParseSnapshot(raw []byte) (map[string]any, error) {
	if len(raw) == 0 {
		return nil, errors.NewValidationError("snapshot", "must not be empty")
	}
	var snap map[string]any
	if err := json.Unmarshal(raw, &snap); err != nil {
		return nil, fmt.Errorf("apobject: decode snapshot: %w", err)
	}
	return snap, nil
}
