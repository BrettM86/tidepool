package materialize

import (
	"context"
	"fmt"

	"tidepool/internal/ap"
	"tidepool/internal/echo"
	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// maxAncestorDepth caps how many unmapped ancestors a comment's inReplyTo
// chain may pull in before the subtree is dropped (runaway threads,
// adversarial chains).
const maxAncestorDepth = 50

// MaterializeComment translates a Lemmy Note into a
// social.coves.community.comment record in the AUTHOR's repo, with
// reply.root/reply.parent strongRefs resolved through ap_objects.
//
// Missing-parent protocol: when the parent AP id is unmapped, the inReplyTo
// chain is walked upward (signed fetches) until it reaches an object we
// have already materialized or the root Page, then the collected ancestors
// are materialized oldest-first before the comment itself. The walk carries
// a depth cap and a cycle guard, and — because it completes before any
// write — a chain that dead-ends (tombstoned/unfetchable ancestor, one refused
// on its own terms, or one outside the delivering community) drops the whole
// subtree without leaving partial ancestors behind from this call. An
// author's consent (nobridge, deleted) — the leaf author's included — is
// checked only when that record is committed, so a record refused for its
// author still drops the subtree from there down, but the older ancestors
// already committed stay.
func (m *Materializer) MaterializeComment(ctx context.Context, note *ap.Object, communityIRI string) (*Result, error) {
	if err := requireBoundCommunityIRI(communityIRI); err != nil {
		return nil, err
	}
	if note == nil || note.ID == "" {
		return nil, errors.NewValidationError("note", "must carry an AP object id")
	}
	if note.Type != ap.TypeNote {
		return nil, errors.NewValidationError("note",
			"object "+note.ID+" has type "+note.Type+", want Note")
	}
	if note.InReplyTo == nil || note.InReplyTo.ID == "" {
		return nil, skip(note.ID, "comment has no inReplyTo")
	}
	// The leaf's own refusals need no ancestor, so they come before the walk:
	// a comment dropped for its own reasons must not leave its ancestors, or
	// their authors, behind.
	draft, err := draftComment(note)
	if err != nil {
		return nil, err
	}

	// A comment already stored belongs to the thread it was first posted in.
	// An Update re-parenting it into the delivering community's thread does
	// not make it that community's, so it is refused before the new ancestry
	// is even walked. commitCommentLeaf repeats the check for every
	// record it commits, ancestors included.
	if _, err := m.storedCommentMapping(ctx, note.ID, communityIRI); err != nil {
		return nil, err
	}

	ancestors, err := m.collectUnmappedAncestors(ctx, note, communityIRI)
	if err != nil {
		return nil, err
	}
	for _, ancestor := range ancestors {
		if err := m.materializeAncestor(ctx, ancestor, communityIRI); err != nil {
			// A skipped ancestor (nobridge/deleted author) takes the rest of
			// the subtree with it — placeholder-free by design — but the
			// older ancestors committed before it stay.
			return nil, err
		}
	}
	return m.commitCommentLeaf(ctx, note, draft, communityIRI)
}

// countAncestorShortCircuit records the anchor as an echo suppression when the
// parent is one of OUR OWN objects: the walk declines to fetch and
// re-materialize a record the bridge itself federated, which is the ancestor
// short-circuit and is counted under its own class.
//
// Anchoring on a FEDIVERSE parent is ordinary threading — every inbound reply
// to an already-seen Lemmy comment lands here — and must not be counted, or the
// class drowns in exactly the volume the split exists to see through.
//
// The origin comes from a point read on the branch that already returns, rather
// than from widening ResolveStrongRef: a shared read seam should not grow a
// return value to feed a metric. That also makes the read's failure harmless —
// it is observability, never the walk's decision, so an unreadable mapping
// leaves the counter alone instead of failing a comment that anchored fine.
func (m *Materializer) countAncestorShortCircuit(ctx context.Context, parentID string) {
	mapping, err := m.objects.GetByAPID(ctx, parentID)
	if err != nil {
		m.logger.Debug("ancestor anchor origin unreadable; short-circuit not counted",
			"parent", parentID, "error", err)
		return
	}
	if mapping.Origin == store.OriginBridge {
		echo.CountDrop(echo.ClassAncestorShortCircuit)
	}
}

// unmappedAncestor is one object the ancestor walk fetched, with the draft
// its own checks produced when it is a Note (nil for the root Page).
type unmappedAncestor struct {
	object *ap.Object
	draft  *commentDraft
}

// collectUnmappedAncestors walks note's inReplyTo chain upward until it
// hits an already-mapped object or the thread's root Page, returning the
// unmapped ancestors oldest-first. Nothing is written during the walk.
//
// Every fetched Note is drafted as it is reached, so an ancestor that would
// be refused on its own terms refuses the whole chain here, before an older
// ancestor — or its author — is committed on the way to it.
//
// The walk is also where the thread is bound to communityIRI: wherever the
// chain ends — on a mapped parent or on the root Page — that end must belong
// to the delivering community. Deciding it here, before any write, is what
// keeps a refused thread from leaving ancestors, authors or a community
// behind.
func (m *Materializer) collectUnmappedAncestors(ctx context.Context, note *ap.Object, communityIRI string) ([]unmappedAncestor, error) {
	var chain []unmappedAncestor
	seen := map[string]bool{note.ID: true}
	current := note

	for {
		if current.InReplyTo == nil || current.InReplyTo.ID == "" {
			// current is the top of the thread but not a Page (those end the
			// walk where they are fetched). Not reached for a Note: the leaf
			// and every fetched ancestor were drafted, and draftComment
			// refuses a parentless one.
			return chain, nil
		}
		parentID := current.InReplyTo.ID
		if seen[parentID] {
			return nil, skip(note.ID, fmt.Sprintf("inReplyTo cycle at %s", parentID))
		}
		seen[parentID] = true

		_, _, err := m.objects.ResolveStrongRef(ctx, parentID)
		switch {
		case err == nil:
			// Anchored: the parent is already materialized, and its stored
			// community decides the whole thread's.
			if err := m.requireAnchorInBoundCommunity(ctx, parentID, note.ID, communityIRI); err != nil {
				return nil, err
			}
			m.countAncestorShortCircuit(ctx, parentID)
			return chain, nil
		case errors.IsTombstoned(err):
			// The parent was deleted: the subtree is dropped, never
			// re-fetched (consent-relevant).
			return nil, skip(note.ID, fmt.Sprintf("ancestor %s is tombstoned", parentID))
		case errors.IsNotFound(err):
			// Unmapped: fetch it and keep walking.
		default:
			return nil, fmt.Errorf("materialize: resolve parent %s of %s: %w", parentID, note.ID, err)
		}

		if len(chain) >= maxAncestorDepth {
			return nil, skip(note.ID,
				fmt.Sprintf("ancestor chain exceeds depth cap (%d)", maxAncestorDepth))
		}
		parent, err := m.fetcher.FetchObject(ctx, parentID)
		switch {
		case err == nil:
		case errors.IsTombstoned(err):
			return nil, skip(note.ID, fmt.Sprintf("ancestor %s is tombstoned upstream", parentID))
		case errors.IsNotFound(err):
			return nil, skip(note.ID, fmt.Sprintf("ancestor %s is unavailable upstream", parentID))
		default:
			return nil, fmt.Errorf("materialize: fetch ancestor %s of %s: %w", parentID, note.ID, err)
		}
		// Bind the self-asserted id to the requested IRI: commitRecord keys the
		// ap_objects mapping on parent.ID, so a body claiming another id —
		// another instance's, or a stored object's on the same host — would
		// commit content fetched from one IRI under someone else's. Nothing is
		// lost by refusing: the child names parentID, so a parent mapped under
		// any other id could never resolve it. Empty id inherits the requested
		// IRI.
		switch {
		case parent.ID == "":
			parent.ID = parentID
		case !ap.SameAuthority(parent.ID, parentID):
			return nil, skip(note.ID,
				fmt.Sprintf("ancestor %s served a cross-authority id %s", parentID, parent.ID))
		case parent.ID != parentID:
			return nil, skip(note.ID,
				fmt.Sprintf("ancestor %s served a body claiming another id %s", parentID, parent.ID))
		}
		if parent.Type == ap.TypePage || parent.Type == ap.TypeArticle {
			// A Page is the thread root whatever it replies to, so the walk
			// ends here and nothing older is fetched or materialized. Its
			// community is compared by IRI, not DID: the root may name a
			// community the bridge has not bridged yet, and that is fine when
			// it is the delivering one.
			chain = append([]unmappedAncestor{{object: parent}}, chain...)
			if ref := communityRef(parent); ref == nil || ref.ID != communityIRI {
				return nil, skip(note.ID,
					fmt.Sprintf("thread root %s is not in the delivering community %s", parent.ID, communityIRI))
			}
			return chain, nil
		}
		if parent.Type != ap.TypeNote {
			return nil, skip(note.ID, fmt.Sprintf("ancestor %s has unsupported type %s", parent.ID, parent.Type))
		}
		draft, err := draftComment(parent)
		if err != nil {
			return nil, err
		}
		chain = append([]unmappedAncestor{{object: parent, draft: draft}}, chain...)
		current = parent
	}
}

// requireAnchorInBoundCommunity refuses note when the already-materialized
// object anchorID, on which its ancestry ends, belongs to a community other
// than communityIRI.
func (m *Materializer) requireAnchorInBoundCommunity(ctx context.Context, anchorID, noteID, communityIRI string) error {
	anchor, err := m.objects.GetByAPID(ctx, anchorID)
	if err != nil {
		return fmt.Errorf("materialize: load anchor mapping %s of %s: %w", anchorID, noteID, err)
	}
	anchorCommunity, err := CommunityDIDOf(ctx, m.repos, anchor)
	if err != nil {
		return err
	}
	return m.requireBoundCommunity(ctx, anchorCommunity, communityIRI, noteID)
}

// storedCommentMapping returns commentID's existing mapping, or nil when it
// has none, refusing the comment when the stored one was deleted upstream or
// belongs to a community other than communityIRI. Which community owns a
// comment is decided at first materialization; a later delivery is an edit
// and cannot move it.
func (m *Materializer) storedCommentMapping(ctx context.Context, commentID, communityIRI string) (*store.APObjectMapping, error) {
	existing, err := m.objects.GetByAPID(ctx, commentID)
	switch {
	case err == nil:
	case errors.IsNotFound(err):
		return nil, nil
	default:
		return nil, fmt.Errorf("materialize: check mapping for %s: %w", commentID, err)
	}
	// The refusal commitRecord would make, made before the ancestor walk: a
	// deleted comment re-delivered under an unmapped chain must not fetch,
	// commit or bridge the authors of ancestors it will never be written
	// under. An announced restore clears the soft delete before it gets here.
	if existing.IsDeleted() {
		return nil, skip(commentID, "object was deleted upstream; not resurrecting")
	}
	stored, err := CommunityDIDOf(ctx, m.repos, existing)
	if err != nil {
		return nil, err
	}
	if err := m.requireBoundCommunity(ctx, stored, communityIRI, commentID); err != nil {
		return nil, err
	}
	return existing, nil
}

// materializeAncestor writes one fetched ancestor: the root Page through the
// post path, Notes as comment leaves from the draft the walk made (their own
// parents are guaranteed mapped — the chain is processed oldest-first).
func (m *Materializer) materializeAncestor(ctx context.Context, ancestor unmappedAncestor, communityIRI string) error {
	if ancestor.draft == nil {
		_, err := m.MaterializePost(ctx, ancestor.object, communityIRI)
		return err
	}
	_, err := m.commitCommentLeaf(ctx, ancestor.object, ancestor.draft, communityIRI)
	return err
}

// commentDraft is what a Note yields on its own, before anything is read or
// minted: its record key, its author reference and its content.
type commentDraft struct {
	rkey      string
	authorRef *ap.Object
	body      string
	facets    []any
}

// draftComment runs the checks a comment fails on its own terms, with no
// ancestor, mapping or actor involved.
func draftComment(note *ap.Object) (*commentDraft, error) {
	// A comment must reply to something. A parentless Note reaching this path
	// is a thread rooted at a non-Page object (e.g. a Mastodon status that
	// federated in as a Lemmy comment); drop the subtree rather than deref a
	// nil inReplyTo later.
	if note.InReplyTo == nil || note.InReplyTo.ID == "" {
		return nil, skip(note.ID, "comment thread roots at a non-Page object")
	}
	rkey, err := recordRKey(note)
	if err != nil {
		return nil, err
	}
	authorRef := note.AttributedTo.First()
	if authorRef == nil || authorRef.ID == "" {
		return nil, skip(note.ID, "comment has no attributedTo author")
	}
	// Before anything is minted: a forged attribution must not cost the actor
	// it names a DID.
	if err := requireSameAuthorityAuthor(note, authorRef); err != nil {
		return nil, err
	}
	content := markdownFromObject(note)
	if content == "" {
		return nil, skip(note.ID, "comment has no content")
	}
	body, facets := bridgedRichText(content, 3000, 30000)
	if body == "" {
		// An HTML-only body can reduce to nothing once tags are stripped; a
		// comment is nothing but its content, so drop it like a bodiless one.
		return nil, skip(note.ID, "comment has no content")
	}
	return &commentDraft{rkey: rkey, authorRef: authorRef, body: body, facets: facets}, nil
}

// commitCommentLeaf writes a single drafted comment whose parent is already
// materialized: it binds the comment to its parent's thread and community and
// commits it. The parent must belong to communityIRI; the ancestor walk has
// already established that for the thread, and the check here holds it for
// each record actually committed.
func (m *Materializer) commitCommentLeaf(ctx context.Context, note *ap.Object, draft *commentDraft, communityIRI string) (*Result, error) {
	// Resolved before the author is bridged, so a parent outside the bound
	// community refuses the comment without minting anyone.
	reply, communityDID, err := m.resolveReplyRefs(ctx, note)
	if err != nil {
		return nil, err
	}
	if err := m.requireBoundCommunity(ctx, communityDID, communityIRI, note.ID); err != nil {
		return nil, err
	}
	// The stored comment's own community is checked too: an ancestor the walk
	// found unmapped can have been stored since by a concurrent delivery, and
	// its parent being in the bound community does not make the stored
	// comment that community's.
	existing, err := m.storedCommentMapping(ctx, note.ID, communityIRI)
	if err != nil {
		return nil, err
	}
	author, err := m.EnsureActor(ctx, draft.authorRef)
	if err != nil {
		return nil, err
	}

	did, rkey, authorDID := author.DID, draft.rkey, author.DID
	if existing != nil {
		// The repo a comment lives in IS its authorship claim, so authorship —
		// and with it the record's coordinates — is fixed at first
		// materialization, exactly as MaterializePost pins a post's. attributedTo
		// on an updated Note is proposed by whoever delivered the update:
		// honouring a changed value would sign the record with an unrelated
		// bridged user's repo key and strand the real author's copy live at its
		// old at-uri. rkey is pinned with it because it is derived from
		// `published`, which an edit can also restate.
		//
		// The collection is NOT pinned: unlike posts, comments never moved
		// between collections, so CollectionComment is the only answer in either
		// era and re-deriving it cannot relocate anything.
		did, rkey = existing.DID, existing.RKey
		if existing.AuthorDID != "" {
			authorDID = existing.AuthorDID
		}
	}

	record := map[string]any{
		"$type":     CollectionComment,
		"reply":     reply,
		"content":   draft.body,
		"createdAt": recordDatetime(note.Published.Time),
	}
	if len(draft.facets) > 0 {
		record["facets"] = draft.facets
	}
	if langs := recordLangs(note.Language); len(langs) > 0 {
		record["langs"] = langs
	}
	if note.Sensitive != nil && *note.Sensitive {
		record["labels"] = selfLabels("nsfw")
	}
	return m.commitRecord(ctx, did, CollectionComment, rkey, record, note, authorDID, communityDID)
}

// resolveReplyRefs builds the reply {root, parent} strongRefs for a
// comment. parent comes straight from the mapping; root is the parent
// itself when the parent is a post, otherwise the parent comment's own
// stored reply.root (every materialized comment carries it, so one record
// read resolves the thread root without walking AP again).
//
// It also returns the community the thread belongs to, taken from the
// PARENT's mapping. A comment record has nowhere to state its community, so
// this is the only moment the bridge knows it cheaply — and recording it is
// what lets an announced delete or vote authorize a comment without walking
// back up the thread through records that may since have been removed.
func (m *Materializer) resolveReplyRefs(ctx context.Context, note *ap.Object) (map[string]any, string, error) {
	parentID := note.InReplyTo.ID
	parentURI, parentCID, err := m.objects.ResolveStrongRef(ctx, parentID)
	switch {
	case err == nil:
	case errors.IsTombstoned(err):
		return nil, "", skip(note.ID, fmt.Sprintf("parent %s is tombstoned", parentID))
	case errors.IsNotFound(err):
		return nil, "", skip(note.ID, fmt.Sprintf("parent %s is not materialized", parentID))
	default:
		return nil, "", fmt.Errorf("materialize: resolve parent %s of %s: %w", parentID, note.ID, err)
	}

	parentMapping, err := m.objects.GetByAPID(ctx, parentID)
	if err != nil {
		return nil, "", fmt.Errorf("materialize: load parent mapping %s: %w", parentID, err)
	}
	communityDID, err := CommunityDIDOf(ctx, m.repos, parentMapping)
	if err != nil {
		return nil, "", err
	}

	reply := map[string]any{"parent": strongRef(parentURI, parentCID)}
	if parentMapping.Collection == CollectionPost || parentMapping.Collection == CollectionPostV2 {
		// A post of EITHER era is the thread root. Both collections are asked
		// about because they coexist indefinitely: postv2 for everything
		// materialized since the flip, the deprecated collection for the posts
		// already written before it. Recognising only one era would send the
		// other down the parent-is-a-comment path, which looks for a reply.root
		// a post never carries.
		reply["root"] = strongRef(parentURI, parentCID)
		return reply, communityDID, nil
	}

	// Parent is a comment: reuse its stored reply.root.
	parentRecord, _, err := m.repos.GetRecord(ctx, parentMapping.DID, parentMapping.Collection, parentMapping.RKey)
	if err != nil {
		return nil, "", fmt.Errorf("materialize: read parent comment %s: %w", parentMapping.ATURI, err)
	}
	root, ok := extractStrongRef(parentRecord, "reply", "root")
	if !ok {
		return nil, "", fmt.Errorf("materialize: parent comment %s carries no reply.root", parentMapping.ATURI)
	}
	reply["root"] = root
	return reply, communityDID, nil
}

// extractStrongRef digs a {uri, cid} pair out of a decoded record.
func extractStrongRef(record map[string]any, path ...string) (map[string]any, bool) {
	current := record
	for _, key := range path {
		next, ok := current[key].(map[string]any)
		if !ok {
			return nil, false
		}
		current = next
	}
	uri, uriOK := current["uri"].(string)
	cid, cidOK := current["cid"].(string)
	if !uriOK || !cidOK || uri == "" || cid == "" {
		return nil, false
	}
	return strongRef(uri, cid), true
}
