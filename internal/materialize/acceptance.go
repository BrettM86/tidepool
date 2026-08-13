package materialize

import (
	"context"
	stderrors "errors"
	"fmt"
	"time"

	"tidepool/internal/acceptrec"
	"tidepool/internal/errors"
	"tidepool/internal/repo"
	"tidepool/internal/store"
)

// acceptPost writes the community's acceptance of one post: the attestation
// that makes the post VISIBLE in that community. Coves renders community
// surfaces from acceptance records alone, so a postv2 with no acceptance is a
// post nobody can see — the record exists, the author owns it, and it appears
// nowhere.
//
// This is MECHANISM, not policy. It attests what the bridge already decided by
// materializing the post; it does not decide whether the post should be in the
// community. The removal record for a subject shares this exact key (same
// digest, different collection), which is where a policy layer that had to
// refuse a re-acceptance would read.
//
// WHY THIS CANNOT SHARE THE POST'S TRANSACTION. The postv2 lands in the
// AUTHOR's repo and the acceptance in the COMMUNITY's — two repos, two
// commits, and a crash between them leaves the post invisible. Nothing
// reconciles that gap on its own, so the heal is redelivery: acceptPost runs
// on EVERY materialization of a post, including one whose postv2 commit was an
// idempotent no-op. Conditioning the acceptance on a fresh post CID would mean
// a post lost to the crash window stays lost until somebody edits it upstream.
//
// WHY createdAt IS DERIVED, NEVER STAMPED. Redelivery must produce
// byte-identical bytes so the repo layer's no-op path absorbs it. A wall-clock
// createdAt would mint a community-repo firehose event on every redelivery and
// make Coves re-run admission on a post nothing changed about. On a REPIN —
// the author edited, so the subject CID moved — the stored createdAt is
// carried forward rather than restamped: the community accepted this post
// once, and re-pinning the version it accepts is not a new acceptance.
func (m *Materializer) acceptPost(ctx context.Context, communityDID, postURI, postCID string, publishedAt time.Time) error {
	rkey := SubjectRKey(postURI)

	// TERMINALITY. A removal is exited only by an explicit restore, and a
	// fresh acceptance IS a restore — so writing one here would un-remove the
	// post. This path is reached by every redelivery and every backfill pass,
	// neither of which is anybody's decision to reinstate content, so a
	// standing removal wins and the acceptance is simply not written. The
	// restore path does not come through here: it deletes the removal and
	// writes the acceptance in ONE commit (RestorePost), so it never has to
	// argue with this guard.
	//
	// This pre-loop read is an OPTIMIZATION and a test seam (removalCheck): the
	// authoritative refusal is the commit-time removal guard inside
	// acceptrec.AcceptSubject, which holds even when this read answers stale.
	removed, err := m.removalCheck(ctx, communityDID, rkey)
	if err != nil {
		return err
	}
	if removed {
		m.logger.Debug("post is removed from the community; not re-accepting",
			"community_did", communityDID, "post", postURI)
		return nil
	}

	// The CAS/removal-guard mechanics live in acceptrec now, shared with the
	// acceptance engine so both build on ONE implementation. The materializer
	// drives it with a nil side effect: it attests what the bridge already
	// decided by materializing the post, and owns no outbound enqueue here.
	_, err = acceptrec.AcceptSubject(ctx, m.repos, communityDID, postURI, postCID, publishedAt, nil)
	if stderrors.Is(err, acceptrec.ErrRemovalStands) {
		// A removal appeared under the write: the community decided this post is
		// out, and that decision is terminal. A refused acceptance is not an
		// error — the removal simply stands.
		m.logger.Debug("post was removed from the community while accepting; not re-accepting",
			"community_did", communityDID, "post", postURI)
		return nil
	}
	if err != nil {
		return fmt.Errorf("materialize: accept %s into %s: %w", postURI, communityDID, err)
	}
	return nil
}

// removalStands reports whether the community currently holds a removal for
// the subject at rkey. Acceptance and removal share the digest key, so this is
// a lookup rather than a search.
func (m *Materializer) removalStands(ctx context.Context, communityDID, rkey string) (bool, error) {
	_, _, err := m.repos.GetRecord(ctx, communityDID, CollectionRemoval, rkey)
	switch {
	case err == nil:
		return true, nil
	case errors.IsNotFound(err):
		return false, nil
	default:
		return false, fmt.Errorf("materialize: read removal %s/%s/%s: %w",
			communityDID, CollectionRemoval, rkey, err)
	}
}

// RemovePost records a community's moderator removal of a post: the acceptance
// is deleted and a removal written IN ONE COMMIT, at the same digest rkey.
//
// Atomicity is the requirement, not an optimization. Two commits would put a
// window on the firehose where the acceptance is gone and the removal is not
// yet there, and a consumer reading that window sees a post that is neither
// accepted nor removed — a state nobody decided on.
//
// The AUTHOR'S POST IS NOT TOUCHED, and neither is its mapping. A community
// removing a post says where the post may appear, not whether it exists;
// deleting the author's record would let one community destroy content for
// every other, and tombstoning the mapping would block the post's later edits
// and votes from ever materializing again.
func (m *Materializer) RemovePost(ctx context.Context, mapping *store.APObjectMapping, reason string) error {
	communityDID, postURI, rkey, err := m.moderationTarget(ctx, mapping)
	if err != nil {
		return err
	}

	// The removal pins the version that was accepted when it was removed —
	// audit metadata, per the lexicon, since removal itself applies to the URI
	// across later edits. Read BEFORE the delete, or the pin is gone with it.
	// A missing acceptance (a crash-window heal, or a removal arriving before
	// the acceptance ever landed) falls back to the mapping's CID: the removal
	// is terminal on the URI either way, so an approximate pin is better than
	// refusing to record the moderator's decision.
	pinned := mapping.CID
	if acceptance, _, aerr := m.repos.GetRecord(ctx, communityDID, CollectionAcceptance, rkey); aerr == nil {
		if ref, ok := extractStrongRef(acceptance, "subject"); ok {
			// Only overwrite on a value we actually got: a blank pin would be
			// worse than the mapping's approximate one.
			if cid, ok := ref["cid"].(string); ok && cid != "" {
				pinned = cid
			}
		}
	} else if !errors.IsNotFound(aerr) {
		return fmt.Errorf("materialize: read acceptance %s/%s/%s: %w",
			communityDID, CollectionAcceptance, rkey, aerr)
	}

	removal := map[string]any{
		"$type":   CollectionRemoval,
		"subject": strongRef(postURI, pinned),
		// Lemmy sends no machine-readable code, so the open knownValues set's
		// catch-all applies. Inventing a narrower code (spam, rule-violation)
		// would be the bridge asserting a reason the moderator never gave.
		"code":      "moderator-discretion",
		"createdAt": recordDatetime(m.moderationStamp(ctx, communityDID, CollectionRemoval, rkey)),
	}
	// Omitted rather than written blank: Lemmy spells "no reason given" as an
	// empty summary, and an empty reason renders in a moderation log as a
	// blank explanation instead of as none.
	if reason != "" {
		removal["reason"] = reason
	}
	if err := m.validateRecord(removal); err != nil {
		return err
	}

	if _, err := m.repos.ApplyOps(ctx, communityDID, []repo.RecordOp{
		{Action: repo.OpActionDelete, Collection: CollectionAcceptance, RKey: rkey},
		{Action: repo.OpActionUpdate, Collection: CollectionRemoval, RKey: rkey, Record: removal},
	}); err != nil {
		return fmt.Errorf("materialize: remove %s from %s: %w", postURI, communityDID, err)
	}
	m.logger.Info("post removed from community by moderator",
		"community_did", communityDID, "post", postURI, "ap_id", mapping.APID)
	return nil
}

// RestorePost undoes a moderator removal: the removal is deleted and a fresh
// acceptance written IN ONE COMMIT, for the same reason the removal was
// atomic. It is a no-op when no removal stands, so a restore that arrives
// twice — or one for a post that was never removed — costs a read.
//
// The fresh acceptance pins the post's CURRENT version, not the one that was
// removed: the author may have edited it while it was out of the community,
// and re-accepting a version that is no longer there would leave the post
// pending re-acceptance the moment it came back.
func (m *Materializer) RestorePost(ctx context.Context, mapping *store.APObjectMapping) error {
	communityDID, postURI, rkey, err := m.moderationTarget(ctx, mapping)
	if err != nil {
		return err
	}
	removed, err := m.removalStands(ctx, communityDID, rkey)
	if err != nil {
		return err
	}
	if !removed {
		return nil
	}

	_, currentCID, err := m.repos.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	if err != nil {
		// No post to re-accept. The removal stays: it is terminal on the URI,
		// and an acceptance pinning nothing would be worse than none.
		if errors.IsNotFound(err) {
			m.logger.Warn("restore target has no record to re-accept; leaving the removal in place",
				"ap_id", mapping.APID, "at_uri", mapping.ATURI)
			return nil
		}
		return fmt.Errorf("materialize: read %s for restore: %w", mapping.ATURI, err)
	}

	acceptance := map[string]any{
		"$type":     CollectionAcceptance,
		"subject":   strongRef(postURI, currentCID),
		"createdAt": recordDatetime(m.moderationStamp(ctx, communityDID, CollectionAcceptance, rkey)),
	}
	if err := m.validateRecord(acceptance); err != nil {
		return err
	}

	if _, err := m.repos.ApplyOps(ctx, communityDID, []repo.RecordOp{
		{Action: repo.OpActionDelete, Collection: CollectionRemoval, RKey: rkey},
		{Action: repo.OpActionUpdate, Collection: CollectionAcceptance, RKey: rkey, Record: acceptance},
	}); err != nil {
		return fmt.Errorf("materialize: restore %s into %s: %w", postURI, communityDID, err)
	}
	m.logger.Info("post restored to community by moderator",
		"community_did", communityDID, "post", postURI, "ap_id", mapping.APID)
	return nil
}

// moderationTarget resolves the community, subject uri and digest rkey a
// moderation transition acts on, refusing anything that is not a postv2.
// Moderation records are postv2-only: a pre-flip post has no acceptance to
// replace, and writing a removal for one would announce a visibility
// mechanism Coves does not consult for that collection.
func (m *Materializer) moderationTarget(ctx context.Context, mapping *store.APObjectMapping) (communityDID, postURI, rkey string, err error) {
	if mapping == nil {
		return "", "", "", errors.NewValidationError("mapping", "must not be nil")
	}
	if mapping.Collection != CollectionPostV2 {
		return "", "", "", errors.NewValidationError("mapping",
			"moderation records are only written for "+CollectionPostV2+", got "+mapping.Collection)
	}
	communityDID, err = CommunityDIDOf(ctx, m.repos, mapping)
	if err != nil {
		return "", "", "", err
	}
	if communityDID == "" {
		return "", "", "", errors.NewValidationError("mapping",
			"cannot moderate "+mapping.ATURI+": it binds to no community")
	}
	return communityDID, mapping.ATURI, SubjectRKey(mapping.ATURI), nil
}

// moderationStamp is the timestamp a moderation record carries: WHEN THE
// MODERATOR ACTED, which is now — a removal's createdAt is a claim about the
// moderator's decision, and deriving it from the post's publish time would
// date every removal to whenever the post happened to be written.
//
// Idempotency comes from carrying the STORED value forward instead: a
// redelivered moderation activity finds the record it already wrote, reuses
// its timestamp, and so rebuilds byte-identical bytes that reach the repo
// layer's no-op path rather than churning the community repo on every retry.
// Only the FIRST write stamps a clock.
func (m *Materializer) moderationStamp(ctx context.Context, communityDID, collection, rkey string) time.Time {
	stored, _, err := m.repos.GetRecord(ctx, communityDID, collection, rkey)
	if err != nil {
		return m.now()
	}
	when, ok := stored["createdAt"].(string)
	if !ok || when == "" {
		return m.now()
	}
	parsed, perr := time.Parse(time.RFC3339, when)
	if perr != nil {
		return m.now()
	}
	return parsed
}

// deleteAcceptance removes a post's acceptance from its community. Called
// before the post record itself goes: a crash in between then leaves a post
// that is merely invisible, where the reverse order would leave the community
// attesting to a record that no longer exists.
func (m *Materializer) deleteAcceptance(ctx context.Context, mapping *store.APObjectMapping) error {
	communityDID, err := CommunityDIDOf(ctx, m.repos, mapping)
	if err != nil {
		return err
	}
	if communityDID == "" {
		return nil
	}
	rkey := SubjectRKey(mapping.ATURI)
	if _, err := m.repos.DeleteRecord(ctx, communityDID, CollectionAcceptance, rkey); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("materialize: delete acceptance %s/%s/%s: %w",
			communityDID, CollectionAcceptance, rkey, err)
	}
	return nil
}
