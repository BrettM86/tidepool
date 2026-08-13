package materialize

import (
	"context"
	"fmt"
	"strings"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// MaterializePost translates a Lemmy Page (or Article) into a
// social.coves.community.postv2 record, written into the AUTHOR's repo
// (PLAN.md decision 20: authorship IS the repo, so the record carries no
// `author` field, and `community` names the community it was submitted to).
// Community and author profiles are ensured — and therefore committed —
// before the post itself, preserving the emission-ordering guarantee.
//
// An object the bridge has already materialized is re-committed EXACTLY
// where its mapping says it lives — same repo, collection and rkey. That is
// what keeps a post from being relocated by an upstream edit, and it is also
// how the two eras coexist: a post first materialized under the deprecated
// collection keeps being updated there (no migration is planned; Coves
// indexes both), while everything new is a postv2.
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

	did, collection, authorDID := author.DID, CollectionPostV2, author.DID
	if existing, err := m.objects.GetByAPID(ctx, page.ID); err == nil {
		did, collection, rkey = existing.DID, existing.Collection, existing.RKey
		// The repo a postv2 lives in IS its authorship claim, so authorship is
		// fixed at first materialization. attributedTo on an updated Page is
		// proposed by whoever delivered the update: honouring a changed value
		// would write the record into an unrelated bridged user's repo — the
		// strongest authorship statement atproto has — and strand the real
		// author's copy. The stored mapping is the authority on who authored a
		// bridged object, exactly as the stored record is on its community.
		if existing.AuthorDID != "" {
			authorDID = existing.AuthorDID
		}
	} else if !errors.IsNotFound(err) {
		return nil, fmt.Errorf("materialize: check mapping for %s: %w", page.ID, err)
	}

	// The blob DID is the repo the record lands in, not the community: a blob
	// ref resolves against the repo its record lives in, so embed media
	// fetched anywhere else is unresolvable for every consumer.
	var record map[string]any
	if collection == CollectionPost {
		record = m.buildPostRecord(ctx, page, community.DID, author.DID, did)
	} else {
		// Provenance is built from the FETCHED actor document, never from the
		// page's own attributedTo: on a content path that object is authored by
		// whoever sent the content. actorDoc(allowEmbedded=false) is the same
		// authority-bound fetch the actor path trusts, and it is re-read here
		// rather than borrowed from EnsureActor because EnsureActor skips the
		// fetch for a profile still inside its TTL — provenance that appeared
		// or vanished with profile freshness would churn the record (and the
		// firehose) on every re-materialization.
		authorDoc, err := m.actorDoc(ctx, authorRef, false)
		if err != nil {
			return nil, err
		}
		record = m.buildPostV2Record(ctx, page, community.DID, author, authorDoc, did)
	}
	res, err := m.commitRecord(ctx, did, collection, rkey, record, page, authorDID, community.DID)
	if err != nil {
		return nil, err
	}
	if collection == CollectionPostV2 {
		// The community is read back off the COMMITTED record, not from the
		// EnsureCommunity above: on an update whose audience was retargeted,
		// carryForwardFields has restored the immutable stored community by
		// now, and accepting into the audience's community instead would plant
		// an attestation in a community that was never given the post.
		//
		// A failure here fails the whole materialization on purpose. The
		// alternative — logging and returning the post as materialized — would
		// leave a post that exists and is invisible, with nothing left to
		// retry it; this way redelivery heals it.
		acceptingDID, _ := record["community"].(string)
		if err := m.acceptPost(ctx, acceptingDID, res.ATURI, res.CID, page.Published.Time); err != nil {
			return nil, err
		}
	}
	return res, nil
}

// buildPostV2Record translates the Page per the community.postv2 lexicon:
// the shared post fields, plus the provenance a bridged record owes its
// readers. There is deliberately no `author` — the record's repo is the
// authorship claim, and a field restating it would invite a consumer to
// trust the field over the repo.
func (m *Materializer) buildPostV2Record(ctx context.Context, page *ap.Object, communityDID string, author *store.BridgedActor, authorDoc *ap.Object, repoDID string) map[string]any {
	record := map[string]any{
		"$type":     CollectionPostV2,
		"community": communityDID,
		"createdAt": recordDatetime(page.Published.Time),
	}
	m.fillPostContent(ctx, record, page, repoDID)
	record["originalAuthor"] = originalAuthorOf(author, authorDoc)
	record["federatedFrom"] = federatedFromOf(page)
	return record
}

// originalAuthorOf names the account on the ORIGIN platform. It is provenance,
// never an authorship claim about the repo (the lexicon says so explicitly),
// and empty fields are omitted rather than written blank — a reader cannot
// tell an empty string from an assertion that the value is empty.
func originalAuthorOf(author *store.BridgedActor, authorDoc *ap.Object) map[string]any {
	out := map[string]any{"apId": author.APActorID}
	if instance := (&ap.Object{ID: author.APActorID}).Host(); instance != "" {
		out["instance"] = instance
	}
	if handle := originAccountName(author); handle != "" {
		out["handle"] = handle
	}
	// ONLY the fetched document may name the account. The inline attributedTo
	// object a page carries is written by the sending instance, so honouring it
	// would let any remote host hang an arbitrary display name on a real
	// account in a field consumers render. A document that asserts no name
	// leaves the field absent: an absence is not licence to substitute one.
	if authorDoc != nil && authorDoc.Name != "" {
		out["displayName"] = authorDoc.Name
	}
	return out
}

// originAccountName is the account's name ON THE ORIGIN PLATFORM — the
// username a reader would type to find them there — NOT the bridged
// tidepool handle, which names a repo on this side of the bridge and would
// mislead anyone trying to trace the post back to its source.
//
// It is read off the actor's CANONICAL AP id, whose last path segment is the
// origin username (Lemmy `/u/Name`, PieFed and Mastodon alike). That id is
// authority-bound at mint time, so unlike an inlined `attributedTo` — which
// is attacker-influenced on every content path — it cannot be spelled by
// whoever authored the post. The bridged handle's local part is the fallback
// for an id with no usable segment; it is the same derivation
// preferredUsernameFor already uses, and it loses the original casing, which
// is why it is second rather than first.
func originAccountName(author *store.BridgedActor) string {
	if idx := strings.LastIndexByte(author.APActorID, '/'); idx >= 0 {
		if name := author.APActorID[idx+1:]; name != "" {
			return name
		}
	}
	if idx := strings.IndexByte(author.Handle, '.'); idx > 0 {
		return author.Handle[:idx]
	}
	return author.Handle
}

// federatedFromOf names where the post came from. Lemmy is the only origin
// platform the bridge speaks (PLAN.md decision 9 defers PieFed/Mbin to their
// own verification), so the platform is a constant rather than a guess off
// the instance's software.
func federatedFromOf(page *ap.Object) map[string]any {
	out := map[string]any{"platform": "lemmy", "apId": page.ID}
	if instance := page.Host(); instance != "" {
		out["instance"] = instance
	}
	return out
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

// buildPostRecord translates the Page fields per the DEPRECATED
// community.post lexicon. Reached only for a post already materialized under
// that collection, which keeps being updated where it lies.
func (m *Materializer) buildPostRecord(ctx context.Context, page *ap.Object, communityDID, authorDID, repoDID string) map[string]any {
	record := map[string]any{
		"$type":     CollectionPost,
		"community": communityDID,
		"author":    authorDID,
		"createdAt": recordDatetime(page.Published.Time),
	}
	m.fillPostContent(ctx, record, page, repoDID)
	return record
}

// fillPostContent adds the fields both post eras share — title, body and
// facets, langs, self-labels, embed. Embed media is fetched into repoDID's
// blob store (the repo holding the record); media failures degrade to a post
// without the affected embed.
func (m *Materializer) fillPostContent(ctx context.Context, record map[string]any, page *ap.Object, repoDID string) {
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
	if embed := m.buildPostEmbed(ctx, page, repoDID); embed != nil {
		record["embed"] = embed
	}
}

// buildPostEmbed maps the Page's attachments onto the post embed union:
// image attachments → social.coves.embed.images (blobs in repoDID, the repo
// that will hold the record); a link attachment (or `url`) →
// social.coves.embed.external with the Lemmy-provided thumbnail as thumb (no
// og-image fetching in v1 — the AP object already carries the thumbnail when
// there is one).
func (m *Materializer) buildPostEmbed(ctx context.Context, page *ap.Object, repoDID string) map[string]any {
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
			blob := m.fetchBlob(ctx, repoDID, href, slotEmbedImage)
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
			if blob := m.fetchBlob(ctx, repoDID, thumbURL, slotEmbedImage); blob != nil {
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
		if blob := m.fetchBlob(ctx, repoDID, thumbURL, slotExternalThumb); blob != nil {
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
