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
// write — a chain that dead-ends (tombstoned/nobridge/unfetchable ancestor)
// drops the whole subtree without leaving partial ancestors behind from
// this call.
func (m *Materializer) MaterializeComment(ctx context.Context, note *ap.Object) (*Result, error) {
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

	ancestors, err := m.collectUnmappedAncestors(ctx, note)
	if err != nil {
		return nil, err
	}
	for _, ancestor := range ancestors {
		if err := m.materializeAncestor(ctx, ancestor); err != nil {
			// A skipped ancestor (nobridge/deleted author, tombstone) takes
			// the whole subtree with it — placeholder-free by design.
			return nil, err
		}
	}
	return m.materializeCommentLeaf(ctx, note)
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

// collectUnmappedAncestors walks note's inReplyTo chain upward until it
// hits an already-mapped object or the thread's root Page, returning the
// unmapped ancestors oldest-first. Nothing is written during the walk.
func (m *Materializer) collectUnmappedAncestors(ctx context.Context, note *ap.Object) ([]*ap.Object, error) {
	var chain []*ap.Object
	seen := map[string]bool{note.ID: true}
	current := note

	for {
		if current.InReplyTo == nil || current.InReplyTo.ID == "" {
			// current is the top of the thread (normally the Page). It was
			// fetched (unmapped), so it is already in chain; the walk ends.
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
			// Anchored: the parent is already materialized.
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
		// Bind the self-asserted id to the fetch authority: commitRecord keys
		// the ap_objects mapping on parent.ID, so a host serving a body that
		// claims another instance's id would forge content under the victim's
		// canonical id. Empty id inherits the requested IRI.
		if parent.ID == "" {
			parent.ID = parentID
		} else if !ap.SameAuthority(parent.ID, parentID) {
			return nil, skip(note.ID,
				fmt.Sprintf("ancestor %s served a cross-authority id %s", parentID, parent.ID))
		}
		chain = append([]*ap.Object{parent}, chain...)
		current = parent
	}
}

// materializeAncestor writes one fetched ancestor: Pages through the post
// path, Notes as comment leaves (their own parents are guaranteed mapped —
// the chain is processed oldest-first).
func (m *Materializer) materializeAncestor(ctx context.Context, ancestor *ap.Object) error {
	switch ancestor.Type {
	case ap.TypePage, ap.TypeArticle:
		_, err := m.MaterializePost(ctx, ancestor)
		return err
	case ap.TypeNote:
		_, err := m.materializeCommentLeaf(ctx, ancestor)
		return err
	default:
		return skip(ancestor.ID, "ancestor has unsupported type "+ancestor.Type)
	}
}

// materializeCommentLeaf writes a single comment whose parent is already
// materialized.
func (m *Materializer) materializeCommentLeaf(ctx context.Context, note *ap.Object) (*Result, error) {
	// A comment must reply to something. A parentless Note reaching this path
	// is a thread rooted at a non-Page object (e.g. a Mastodon status that
	// federated in as a Lemmy comment); drop the subtree rather than deref a
	// nil inReplyTo below.
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
	author, err := m.EnsureActor(ctx, authorRef)
	if err != nil {
		return nil, err
	}

	did, authorDID := author.DID, author.DID
	if existing, err := m.objects.GetByAPID(ctx, note.ID); err == nil {
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
	} else if !errors.IsNotFound(err) {
		return nil, fmt.Errorf("materialize: check mapping for %s: %w", note.ID, err)
	}

	reply, communityDID, err := m.resolveReplyRefs(ctx, note)
	if err != nil {
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
	record := map[string]any{
		"$type":     CollectionComment,
		"reply":     reply,
		"content":   body,
		"createdAt": recordDatetime(note.Published.Time),
	}
	if len(facets) > 0 {
		record["facets"] = facets
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
