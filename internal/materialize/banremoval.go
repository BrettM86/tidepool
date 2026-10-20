package materialize

import (
	"context"
	"fmt"
	"time"

	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/ipfs/go-cid"

	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// SelectBannedContent lists the content an upstream ban with removeData takes
// down: the bridged author's LIVE posts and comments published strictly before
// the ban (a mapping with no published time is weighed by when it was indexed),
// in every community when communityDID is empty, or in that one community.
//
// Lemmy removed exactly this content at ban time and nothing published after
// it, so the cutoff is the whole of what makes a delayed or replayed ban safe:
// whatever the author wrote since is not the ban's business.
//
// Profiles are never selected — a ban removes content, not the account. Under a
// community ban a postv2 that community already removed is left out, because
// its removal record already says it is gone from there; under a site ban it is
// selected anyway, since the record itself goes. Selection writes nothing,
// which is what lets a dry run list it.
func (m *Materializer) SelectBannedContent(ctx context.Context, authorDID, communityDID string, before time.Time) ([]*store.APObjectMapping, error) {
	mappings, err := m.objects.ListByActorDID(ctx, authorDID)
	if err != nil {
		return nil, fmt.Errorf("materialize: list content by %s: %w", authorDID, err)
	}
	var selected []*store.APObjectMapping
	var undetermined []string
	for _, mapping := range mappings {
		switch mapping.Collection {
		case CollectionPost, CollectionPostV2, CollectionComment:
		default:
			continue
		}
		published := mapping.IndexedAt
		if mapping.PublishedAt != nil {
			published = *mapping.PublishedAt
		}
		if !published.Before(before) {
			continue
		}
		if communityDID != "" {
			inCommunity, determined, err := m.bannedInCommunity(ctx, mapping, communityDID)
			if err != nil {
				return nil, err
			}
			if !determined {
				undetermined = append(undetermined, mapping.ATURI)
				continue
			}
			if !inCommunity {
				continue
			}
		}
		selected = append(selected, mapping)
	}
	if len(undetermined) > 0 {
		m.logger.Warn("community ban skipped content whose community cannot be determined",
			"author_did", authorDID, "community_did", communityDID,
			"skipped", len(undetermined), "at_uris", undetermined)
	}
	return selected, nil
}

// bannedInCommunity reports whether a community ban of communityDID still has
// mapping to act on: it is that community's content and, for a postv2, the
// community has not already removed it. determined is false when the content's
// community cannot be determined, which is never a match.
func (m *Materializer) bannedInCommunity(ctx context.Context, mapping *store.APObjectMapping, communityDID string) (inCommunity, determined bool, err error) {
	contentCommunity, err := CommunityDIDOf(ctx, m.repos, mapping)
	if err != nil {
		return false, false, fmt.Errorf("materialize: resolve community of %s: %w", mapping.ATURI, err)
	}
	if contentCommunity == "" && mapping.Collection == CollectionComment {
		contentCommunity, err = m.deletedThreadRootCommunity(ctx, mapping)
		if err != nil {
			return false, false, err
		}
	}
	if contentCommunity == "" {
		return false, false, nil
	}
	if contentCommunity != communityDID {
		return false, true, nil
	}
	if mapping.Collection != CollectionPostV2 {
		return true, true, nil
	}
	removed, err := m.removalStands(ctx, communityDID, SubjectRKey(mapping.ATURI))
	if err != nil {
		return false, true, err
	}
	return !removed, true, nil
}

// deletedThreadRootCommunity answers the community of a comment whose mapping
// records none and whose postv2 thread root's record is already gone — say, by
// an earlier ban of the root's author. deleteMapping keeps the community
// binding on the root's soft-deleted mapping when it has or can derive one
// (persisting a derived one is best-effort), so that binding still says whose
// thread the comment is in: a deleted root must not shield the replies under
// it. Empty means it cannot be determined.
//
// It lives here rather than in CommunityDIDOf because the authorization
// callers of that function hold only a record reader, and for them an
// unreadable thread root stays a refusal.
func (m *Materializer) deletedThreadRootCommunity(ctx context.Context, comment *store.APObjectMapping) (string, error) {
	record, err := readRecord(ctx, m.repos, comment.DID, comment.Collection, comment.RKey)
	if record == nil || err != nil {
		return "", err
	}
	rootDID, rootCollection, rootRKey := replyRootRef(record)
	if rootDID == "" || rootCollection != CollectionPostV2 {
		return "", nil
	}
	rootURI := "at://" + rootDID + "/" + rootCollection + "/" + rootRKey
	root, err := m.objects.GetByATURI(ctx, rootURI)
	if errors.IsNotFound(err) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("materialize: look up thread root %s of %s: %w", rootURI, comment.ATURI, err)
	}
	return root.CommunityDID, nil
}

// RemoveBannedContent takes down what SelectBannedContent selected.
//
// A SITE ban (communityDID empty) deletes every record, as the author deleting
// it would: the user's own instance removed it, so it is gone everywhere. A
// COMMUNITY ban speaks only for that community, so a postv2 — which lives in
// the author's repo and may stand in other communities' eyes — gets the
// community's own removal under RemovalCodeAuthorBanned and the record stays;
// comments and legacy posts have no acceptance to withdraw, so they are
// deleted.
//
// Nothing is enqueued: Lemmy already removed this content upstream, and an
// outbound Delete would be aimed at the instance that just acted. Each step is
// idempotent and a removed or deleted record drops out of the next selection,
// so a retry after a failure converges.
func (m *Materializer) RemoveBannedContent(ctx context.Context, mappings []*store.APObjectMapping, communityDID, reason string) error {
	var deletions []*store.APObjectMapping
	for _, mapping := range mappings {
		if BanWithdraws(communityDID, mapping) {
			if err := m.removePost(ctx, mapping, RemovalCodeAuthorBanned, reason); err != nil {
				return err
			}
			continue
		}
		deletions = append(deletions, mapping)
	}
	return m.deleteKeepingSharedBlobs(ctx, deletions)
}

// BanWithdraws reports whether a ban scoped to communityDID (empty for a site
// ban) withdraws mapping from that community rather than deleting it: only a
// community ban does, and only a postv2, the one kind of content with an
// acceptance to withdraw.
func BanWithdraws(communityDID string, mapping *store.APObjectMapping) bool {
	return communityDID != "" && mapping.Collection == CollectionPostV2
}

// deleteKeepingSharedBlobs deletes each mapping's record the way deleteMapping
// does, and with it every blob the record referenced that no other record left
// in the same repo still references.
//
// The sharing check is the difference from scrubActorRecords, which deletes
// unconditionally because it empties the whole account: here the author's
// later posts stay, and a spammer reusing one image across posts before and
// after the ban would otherwise lose it from the post that is allowed to stay.
// The author's profile, and other authors' legacy posts in a community repo,
// hold blobs in the same repo too.
//
// Each repo is read ONCE and the references are released record by record, so
// a ban selecting many records costs one walk per repo rather than one per
// record. That snapshot is taken once per repo, so a record committed to the
// same repo after the walk that reuses the same blob CID could lose that blob;
// the window is narrow and accepted.
//
// Blobs go BEFORE their record, for scrubActorRecords' reason: while the
// mapping is live a retry re-selects it and re-collects the same refs, and
// DeleteBlob tolerates rows that are already gone.
func (m *Materializer) deleteKeepingSharedBlobs(ctx context.Context, mappings []*store.APObjectMapping) error {
	var repoOrder []string
	byRepo := map[string][]*store.APObjectMapping{}
	for _, mapping := range mappings {
		if _, seen := byRepo[mapping.DID]; !seen {
			repoOrder = append(repoOrder, mapping.DID)
		}
		byRepo[mapping.DID] = append(byRepo[mapping.DID], mapping)
	}
	for _, did := range repoOrder {
		references, err := m.repoBlobReferences(ctx, did)
		if err != nil {
			return err
		}
		for _, mapping := range byRepo[did] {
			for _, blobCID := range references.release(mapping.Collection + "/" + mapping.RKey) {
				if err := m.deleteBlob(ctx, did, blobCID); err != nil {
					return fmt.Errorf("materialize: delete blob %s under %s: %w", blobCID, did, err)
				}
			}
			if err := m.deleteMapping(ctx, mapping); err != nil {
				return err
			}
		}
	}
	return nil
}

// blobReferences records which records in one repo reference which blobs.
type blobReferences struct {
	byBlob   map[string]map[string]struct{}
	byRecord map[string][]string
}

// repoBlobReferences walks did's current records. A repo that does not exist
// references nothing.
func (m *Materializer) repoBlobReferences(ctx context.Context, did string) (*blobReferences, error) {
	references := &blobReferences{
		byBlob:   map[string]map[string]struct{}{},
		byRecord: map[string][]string{},
	}
	entries, err := m.repos.ListRecords(ctx, did)
	if errors.IsNotFound(err) {
		return references, nil
	}
	if err != nil {
		return nil, fmt.Errorf("materialize: list records of %s: %w", did, err)
	}
	for _, entry := range entries {
		path := entry.Collection + "/" + entry.Rkey
		for _, blob := range atdata.ExtractBlobs(entry.Value) {
			blobCID := cid.Cid(blob.Ref).String()
			if references.byBlob[blobCID] == nil {
				references.byBlob[blobCID] = map[string]struct{}{}
			}
			references.byBlob[blobCID][path] = struct{}{}
			references.byRecord[path] = append(references.byRecord[path], blobCID)
		}
	}
	return references, nil
}

// release drops the record at path from the references and returns the blobs
// it was the last record to reference.
func (r *blobReferences) release(path string) []string {
	var unreferenced []string
	for _, blobCID := range r.byRecord[path] {
		holders, ok := r.byBlob[blobCID]
		if !ok {
			// The record referenced this blob twice; already released.
			continue
		}
		delete(holders, path)
		if len(holders) == 0 {
			delete(r.byBlob, blobCID)
			unreferenced = append(unreferenced, blobCID)
		}
	}
	delete(r.byRecord, path)
	return unreferenced
}
