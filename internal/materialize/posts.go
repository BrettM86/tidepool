package materialize

import (
	"context"
	"strings"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
)

// MaterializePost translates a Lemmy Page (or Article) into a
// social.coves.community.post record, written into the COMMUNITY's repo
// with author = the bridged user's DID (the Coves post consumer validates
// repo DID == record.community). Community and author profiles are ensured
// — and therefore committed — before the post itself, preserving the
// emission-ordering guarantee.
func (m *Materializer) MaterializePost(ctx context.Context, page *ap.Object) (*Result, error) {
	if page == nil || page.ID == "" {
		return nil, errors.NewValidationError("page", "must carry an AP object id")
	}
	if page.Type != ap.TypePage && page.Type != ap.TypeArticle {
		return nil, errors.NewValidationError("page",
			"object "+page.ID+" has type "+page.Type+", want Page or Article")
	}
	rkey, err := recordRKey(page)
	if err != nil {
		return nil, err
	}

	groupRef := communityRef(page)
	if groupRef == nil {
		return nil, skip(page.ID, "post names no community (no audience/to group IRI)")
	}
	community, err := m.EnsureCommunity(ctx, groupRef)
	if err != nil {
		return nil, err
	}
	authorRef := page.AttributedTo.First()
	if authorRef == nil || authorRef.ID == "" {
		return nil, skip(page.ID, "post has no attributedTo author")
	}
	author, err := m.EnsureActor(ctx, authorRef)
	if err != nil {
		return nil, err
	}

	record := m.buildPostRecord(ctx, page, community.DID, author.DID)
	return m.commitRecord(ctx, community.DID, CollectionPost, rkey, record, page, author.DID)
}

// communityRef finds the community Group IRI a post belongs to: Lemmy sets
// `audience` (FEP-1b12); older objects carry the group in `to`/`cc` next to
// the public collection. Public-collection IRIs are skipped in EVERY list,
// audience included — the ingest layer's communityIRIFrom does the same, and
// a divergence there is not cosmetic: an `audience: ["as:Public"]` post that
// ingest reads as "names no community" would arrive here as
// EnsureCommunity("as:Public"), whose failure is retryable and would back the
// whole ordering key off into poison.
func communityRef(page *ap.Object) *ap.Object {
	for _, iri := range page.Audience {
		if iri != "" && !isPublicCollection(iri) {
			return &ap.Object{ID: iri}
		}
	}
	for _, list := range []ap.Audience{page.To, page.Cc} {
		for _, iri := range list {
			if iri == "" || isPublicCollection(iri) {
				continue
			}
			// Heuristic for addressing lists that mix users and groups:
			// Lemmy community IRIs live under /c/.
			if strings.Contains(iri, "/c/") {
				return &ap.Object{ID: iri}
			}
		}
	}
	return nil
}

// isPublicCollection reports whether an addressing IRI is the AS2 public
// collection. All three spellings appear in the wild (Lemmy always writes the
// full IRI); ap.Object.IsPublic and ingest.isPublicIRI accept the same set.
func isPublicCollection(iri string) bool {
	return iri == ap.PublicAudience || iri == "as:Public" || iri == "Public"
}

// buildPostRecord translates the Page fields per the community.post
// lexicon. Embed media is fetched into the community repo's blob store;
// media failures degrade to a post without the affected embed.
func (m *Materializer) buildPostRecord(ctx context.Context, page *ap.Object, communityDID, authorDID string) map[string]any {
	record := map[string]any{
		"$type":     CollectionPost,
		"community": communityDID,
		"author":    authorDID,
		"createdAt": recordDatetime(page.Published.Time),
	}
	if page.Name != "" {
		record["title"] = truncateText(page.Name, 300, 3000)
	}
	if content := markdownFromObject(page); content != "" {
		// bridgedRichText can reduce an HTML-only body to nothing once tags
		// are stripped; a post is still valid without content (title/embed
		// carry it), so the field is simply omitted.
		if body, facets := bridgedRichText(content, 10000, 100000); body != "" {
			record["content"] = body
			if len(facets) > 0 {
				record["facets"] = facets
			}
		}
	}
	if langs := recordLangs(page.Language); len(langs) > 0 {
		record["langs"] = langs
	}
	if page.Sensitive != nil && *page.Sensitive {
		record["labels"] = selfLabels("nsfw")
	}
	if embed := m.buildPostEmbed(ctx, page, communityDID); embed != nil {
		record["embed"] = embed
	}
	return record
}

// buildPostEmbed maps the Page's attachments onto the post embed union:
// image attachments → social.coves.embed.images (blobs in the community
// repo); a link attachment (or `url`) → social.coves.embed.external with
// the Lemmy-provided thumbnail as thumb (no og-image fetching in v1 — the
// AP object already carries the thumbnail when there is one).
func (m *Materializer) buildPostEmbed(ctx context.Context, page *ap.Object, communityDID string) map[string]any {
	var images []any
	var externalLink *ap.Link
	for i := range page.Attach {
		link := &page.Attach[i]
		href := link.Href
		if href == "" {
			href = link.URL // PieFed image attachments carry url
		}
		if href == "" {
			continue
		}
		if isImageAttachment(link) {
			if len(images) >= 8 {
				continue // lexicon maxLength
			}
			blob := m.fetchBlob(ctx, communityDID, href, slotEmbedImage)
			if blob == nil {
				continue
			}
			image := map[string]any{"image": *blob}
			if link.Name != "" {
				image["alt"] = truncateText(link.Name, 1000, 10000)
			}
			images = append(images, image)
		} else if externalLink == nil && isSafeLinkScheme(href) {
			// Remember the resolved href (Href or the PieFed `url` fallback),
			// not link.Href — which may be empty when only `url` was set.
			externalLink = &ap.Link{Href: href, Name: link.Name}
		}
	}
	if len(images) > 0 {
		return map[string]any{
			"$type":  "social.coves.embed.images",
			"images": images,
		}
	}

	uri := ""
	title := ""
	if externalLink != nil {
		uri = externalLink.Href
		title = externalLink.Name
	} else if u := page.URLString(); u != "" && u != page.ID && isSafeLinkScheme(u) {
		// Older Lemmy put the shared link in `url` instead of `attachment`.
		uri = u
	}
	if uri == "" {
		// No link, no images. A Lemmy thumbnail alone (image posts sometimes
		// deliver the pictrs file in `image`) still makes an images embed.
		if thumbURL := imageURL(page.Image); thumbURL != "" {
			if blob := m.fetchBlob(ctx, communityDID, thumbURL, slotEmbedImage); blob != nil {
				return map[string]any{
					"$type":  "social.coves.embed.images",
					"images": []any{map[string]any{"image": *blob}},
				}
			}
		}
		return nil
	}

	external := map[string]any{"uri": uri}
	if title != "" {
		external["title"] = truncateGraphemes(title, 500)
	}
	if thumbURL := imageURL(page.Image); thumbURL != "" {
		if blob := m.fetchBlob(ctx, communityDID, thumbURL, slotExternalThumb); blob != nil {
			external["thumb"] = *blob
		}
	}
	return map[string]any{
		"$type":    "social.coves.embed.external",
		"external": external,
	}
}

// isSafeLinkScheme restricts bridge-authored clickable URIs (external
// embeds, link facets) to http/https. The lexicon's format:"uri" accepts
// javascript:/data:/vbscript:, which a downstream client rendering the
// remote-actor-controlled link as clickable would treat as a scripting URI.
// Fail closed: an unsafe scheme drops the embed or facet rather than
// erroring the whole record.
func isSafeLinkScheme(uri string) bool {
	lower := strings.ToLower(strings.TrimSpace(uri))
	return strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://")
}

// isImageAttachment reports whether an attachment is an image: an explicit
// image/* mediaType, an AS2 Image type, or a href with a well-known image
// extension (Lemmy usually sets mediaType; pictrs URLs carry extensions).
func isImageAttachment(link *ap.Link) bool {
	if strings.HasPrefix(strings.ToLower(link.MediaType), "image/") {
		return true
	}
	if link.Type == ap.TypeImage {
		return true
	}
	href := strings.ToLower(link.Href)
	if href == "" {
		href = strings.ToLower(link.URL)
	}
	for _, ext := range []string{".png", ".jpg", ".jpeg", ".webp", ".gif"} {
		if strings.HasSuffix(href, ext) {
			return true
		}
	}
	return false
}
