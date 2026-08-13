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
	"html"
	"strings"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
)

// ContextActivityStreams is the AS2 context every object/activity we emit
// carries. The Note/Page shapes use only core AS2 terms (attributedTo, source,
// mediaType, audience), so the plain context is sufficient for Lemmy 0.19.20.
const ContextActivityStreams = "https://www.w3.org/ns/activitystreams"

// BuildNote renders a comment as an AP Note. The Note carries the community in
// cc (the Note half of the Page/Note addressing split), to ⊇ as:Public,
// attributedTo as a SINGLE STRING, and HTML content alongside its markdown
// source. inReplyTo is emitted only when a parent is known.
func BuildNote(actorID, communityAPID, parentAPID, objectURL string, record map[string]any) map[string]any {
	source, _ := record["content"].(string)
	note := map[string]any{
		"type":         "Note",
		"id":           objectURL,
		"attributedTo": actorID,
		"to":           []string{ap.PublicAudience},
		"cc":           []string{communityAPID},
		"audience":     communityAPID,
		"mediaType":    "text/html",
		"content":      renderHTML(source),
		"source": map[string]any{
			"content":   source,
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
	source, _ := record["content"].(string)
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
		"content":   renderHTML(source),
		"source": map[string]any{
			"content":   source,
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
	if href == "" {
		return nil
	}
	return []any{map[string]any{"type": "Link", "href": href}}
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

// renderHTML wraps the markdown source in the minimal HTML Lemmy stores as
// `content` (the source itself rides `source.content`). Paragraphs are split on
// blank lines and escaped so the source's own angle brackets cannot inject
// markup.
func renderHTML(source string) string {
	source = strings.ReplaceAll(source, "\r\n", "\n")
	blocks := strings.Split(source, "\n\n")
	var b strings.Builder
	for _, block := range blocks {
		block = strings.TrimSpace(block)
		if block == "" {
			continue
		}
		b.WriteString("<p>")
		b.WriteString(html.EscapeString(block))
		b.WriteString("</p>\n")
	}
	if b.Len() == 0 {
		return "<p></p>\n"
	}
	return b.String()
}
