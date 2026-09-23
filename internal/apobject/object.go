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

	"github.com/rivo/uniseg"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
)

// ContextActivityStreams is the AS2 context every object/activity we emit
// carries. The Note/Page shapes use only core AS2 terms (attributedTo, source,
// mediaType, audience), so the plain context is sufficient for Lemmy 0.19.20.
const ContextActivityStreams = "https://www.w3.org/ns/activitystreams"

// The lexicon limits on a native record's rendered body, shared by
// social.coves.community.comment and social.coves.community.postv2 (content,
// facets) and social.coves.richtext.facet (features). A string maxLength counts
// UTF-8 bytes.
const (
	MaximumContentBytes     = 100000
	MaximumContentGraphemes = 10000
	MaximumFacets           = 200
	MaximumFeaturesPerFacet = 20
)

// CheckRecordLimits rejects a native record whose content or facets exceed the
// lexicon limits. A postv2 is lexicon-validated by the acceptance engine before
// it is ever snapshotted; a comment is not validated anywhere upstream, so every
// render path checks this before rendering. An over-limit record is REJECTED,
// never rendered with facets dropped: dropping a spoiler facet would publish the
// text it hides. Only the shape the lexicon bounds is counted here — a facets
// value that is not an array is left to the renderer, which ignores it.
func CheckRecordLimits(record map[string]any) error {
	content, _ := record["content"].(string)
	if len(content) > MaximumContentBytes {
		return errors.NewValidationError("record.content",
			fmt.Sprintf("exceeds the lexicon maximum of %d bytes", MaximumContentBytes))
	}
	if uniseg.GraphemeClusterCount(content) > MaximumContentGraphemes {
		return errors.NewValidationError("record.content",
			fmt.Sprintf("exceeds the lexicon maximum of %d graphemes", MaximumContentGraphemes))
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
// CheckRecordLimits first; BuildNote does not.
func BuildNote(actorID, communityAPID, parentAPID, objectURL string, record map[string]any) map[string]any {
	source, _ := record["content"].(string)
	markdown, content := renderBody(source, record["facets"])
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
// (Lemmy rejects a titleless Page); a link embed becomes a Link attachment;
// an nsfw self-label becomes sensitive:true.
func BuildPage(actorID, communityAPID, objectURL string, record map[string]any) (map[string]any, error) {
	title, _ := record["title"].(string)
	if title == "" {
		return nil, errors.NewValidationError("title", "a Page requires a name (Lemmy rejects a titleless post)")
	}
	if err := CheckRecordLimits(record); err != nil {
		return nil, err
	}
	source, _ := record["content"].(string)
	markdown, content := renderBody(source, record["facets"])
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
	if attachment := linkAttachment(record); attachment != nil {
		page["attachment"] = attachment
	}
	if published, ok := record["createdAt"].(string); ok && published != "" {
		page["published"] = published
	}
	return page, nil
}

// linkAttachment maps a social.coves.embed.external embed to Lemmy's
// attachment [{type:Link, href}] (Lemmy reads the FIRST attachment as the
// post's link). An image embed (social.coves.embed.images) is NOT handled here:
// its blobs live in the author's PDS and need a blob→PDS-URL seam to render as
// attachment [{type:Image, url}] — a task follow-up. A post with no external
// embed carries no attachment.
//
// The href is scheme-checked: a javascript:/data:/vbscript:/file: uri rendered
// as a clickable Link would be a stored-XSS-shaped hazard on every peer that
// renders it, so an unsafe scheme drops the attachment (fail closed) rather than
// federating it.
func linkAttachment(record map[string]any) []any {
	embed, ok := record["embed"].(map[string]any)
	if !ok {
		return nil
	}
	if kind, _ := embed["$type"].(string); kind != "social.coves.embed.external" {
		return nil
	}
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
// DB lookup happens (every id is in the snapshot or its at-uri).
func RenderObject(userOrigin string, snapshot []byte) (map[string]any, error) {
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

	var object map[string]any
	if strings.Contains(collection, "postv2") {
		object, err = BuildPage(actorID, community, objectURL, record)
		if err != nil {
			return nil, err
		}
	} else {
		if err := CheckRecordLimits(record); err != nil {
			return nil, err
		}
		object = BuildNote(actorID, community, parentAPID, objectURL, record)
	}
	// Served standalone (not embedded in an activity), so it carries its own
	// context.
	object["@context"] = ContextActivityStreams
	return object, nil
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
