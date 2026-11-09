package materialize

import (
	"context"
	stderrors "errors"
	"expvar"
	"fmt"
	"time"

	"tidepool/internal/errors"
	"tidepool/internal/repo"
	"tidepool/internal/store"
)

// The comment-removal record counters. They count COMMITS, not attempts: a
// redelivered removal that re-puts identical bytes, and a restore that finds no
// record to delete, change nothing in the community repo and are not counted.
var (
	// CommentRemovalRecordsWritten counts social.coves.community.removal
	// records written (or rewritten with a new reason) for a comment.
	CommentRemovalRecordsWritten = expvar.NewInt("tidepool_moderation_comment_removal_records_written")
	// CommentRemovalRecordsDeleted counts comment removal records deleted by a
	// restore.
	CommentRemovalRecordsDeleted = expvar.NewInt("tidepool_moderation_comment_removal_records_deleted")
)

// WriteCommentRemoval publishes a community's removal of one COMMENT into the
// community's repo: a social.coves.community.removal record at the digest rkey
// of the comment's at-uri, the same key derivation a post's removal uses.
//
// A comment has NO acceptance record, so unlike a post removal there is nothing
// to withdraw in the same commit: the batch is the one removal put. Re-removal
// updates the record at the same rkey (a new reason replaces the old one).
//
// createdAt is the caller's: the time the moderators removed the comment, which
// the caller holds durably (object_moderation.removed_at for a native comment).
// Passing the original time on a redelivery rebuilds byte-identical bytes, so
// the repo layer's NoOp path absorbs it and no commit is written.
//
// The comment itself is not touched: it lives in its author's repo, and a
// community removing it says where it may appear, not whether it exists.
//
// written reports whether a commit was made (false on the identical re-put).
func (m *Materializer) WriteCommentRemoval(ctx context.Context, communityDID, commentURI, commentCID, code, reason string, createdAt time.Time) (written bool, err error) {
	if communityDID == "" {
		return false, errors.NewValidationError("community_did", "must not be empty")
	}
	if commentURI == "" || commentCID == "" {
		return false, errors.NewValidationError("subject", "a comment removal must name the comment's uri and cid")
	}
	if code == "" {
		return false, errors.NewValidationError("code", "must not be empty")
	}
	removal := map[string]any{
		"$type":     CollectionRemoval,
		"subject":   strongRef(commentURI, commentCID),
		"code":      code,
		"createdAt": recordDatetime(createdAt),
	}
	// Omitted rather than written blank: Lemmy spells "no reason given" as an
	// empty summary, and an empty reason renders in a moderation log as a blank
	// explanation instead of as none.
	if reason != "" {
		// The removal lexicon's reason limits; a Lemmy summary has none.
		removal["reason"] = truncateText(reason, 1000, 10000)
	}
	if err := m.validateRecord(removal); err != nil {
		return false, err
	}

	// The removal withholds the blobs a bridged comment's current record
	// references, under the removal's code, in the same commit (also on the
	// identical re-put, so a redelivery converges); a native comment gets no
	// withhold.
	rkey := SubjectRKey(commentURI)
	res, err := m.repos.ApplyOpsTx(ctx, communityDID, []repo.RecordOp{
		{Action: repo.OpActionUpdate, Collection: CollectionRemoval, RKey: rkey, Record: removal},
	}, m.removalWithholdSync(commentURI, code))
	if err != nil {
		return false, fmt.Errorf("materialize: write removal of comment %s into %s: %w", commentURI, communityDID, err)
	}
	if res.NoOp {
		return false, nil
	}
	CommentRemovalRecordsWritten.Add(1)
	m.logger.Info("comment removal record written into community repo",
		"community_did", communityDID, "comment", commentURI, "rkey", rkey, "code", code)
	return true, nil
}

// DeleteCommentRemoval deletes a community's removal record for one comment:
// the restore half of WriteCommentRemoval. No acceptance is written in its
// place, because a comment never had one.
//
// It is idempotent: with no record at the comment's rkey the delete is inert
// and no commit is written, so a retried or re-delivered restore completes
// whatever an earlier attempt left behind and otherwise changes nothing.
//
// deleted reports whether a record was actually there and removed.
func (m *Materializer) DeleteCommentRemoval(ctx context.Context, communityDID, commentURI string) (deleted bool, err error) {
	if communityDID == "" {
		return false, errors.NewValidationError("community_did", "must not be empty")
	}
	if commentURI == "" {
		return false, errors.NewValidationError("subject", "must not be empty")
	}
	return m.deleteCommentRemoval(ctx, communityDID, commentURI, removalWithholdLift(commentURI))
}

// deleteCommentRemoval deletes the comment's removal record with lift, the
// side effect lifting its community-removal withholds, in the same commit. A
// lift failing with errRestoreSubjectGone commits nothing and reports no
// record deleted.
func (m *Materializer) deleteCommentRemoval(ctx context.Context, communityDID, commentURI string, lift repo.TxSideEffect) (deleted bool, err error) {
	rkey := SubjectRKey(commentURI)
	res, err := m.repos.ApplyOpsTx(ctx, communityDID, []repo.RecordOp{
		{Action: repo.OpActionDelete, Collection: CollectionRemoval, RKey: rkey},
	}, lift)
	if stderrors.Is(err, errRestoreSubjectGone) {
		m.logger.Warn("restored comment was deleted before the restore committed; leaving the removal in place",
			"community_did", communityDID, "comment", commentURI)
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("materialize: delete removal of comment %s from %s: %w", commentURI, communityDID, err)
	}
	if res.NoOp {
		return false, nil
	}
	CommentRemovalRecordsDeleted.Add(1)
	m.logger.Info("comment removal record deleted from community repo",
		"community_did", communityDID, "comment", commentURI, "rkey", rkey)
	return true, nil
}

// RemoveBridgedComment publishes a community's removal of a BRIDGED Lemmy
// comment: the removal record goes into the community that owns the comment's
// thread (CommunityDIDOf), and the comment's record and mapping are left alone,
// because a community removing a comment says where it may appear, not whether
// it exists.
//
// createdAt is the moderator's decision time: carried forward from a standing
// removal record (so a redelivery or a re-removal keeps the original time and a
// redelivery commits nothing), else now. The bridge holds no other durable copy
// of a bridged comment's removal time.
func (m *Materializer) RemoveBridgedComment(ctx context.Context, mapping *store.APObjectMapping, code, reason string) error {
	communityDID, err := m.commentModerationTarget(ctx, mapping)
	if err != nil {
		return err
	}
	return m.removeBridgedCommentFrom(ctx, communityDID, mapping, code, reason)
}

// removeBridgedCommentFrom writes communityDID's removal of a bridged comment,
// keeping a standing removal's createdAt. The caller has resolved the
// community: a community ban resolves it through a deleted thread root, which
// commentModerationTarget cannot.
func (m *Materializer) removeBridgedCommentFrom(ctx context.Context, communityDID string, mapping *store.APObjectMapping, code, reason string) error {
	createdAt := m.moderationStamp(ctx, communityDID, CollectionRemoval, SubjectRKey(mapping.ATURI))
	if _, err := m.WriteCommentRemoval(ctx, communityDID, mapping.ATURI, mapping.CID, code, reason, createdAt); err != nil {
		return err
	}
	return nil
}

// RestoreBridgedComment deletes the community's removal record for a bridged
// comment, the restore half of RemoveBridgedComment. It writes no acceptance (a
// comment has none) and is inert, with no commit, when no removal stands.
// deleted reports whether a record was there and removed.
//
// The comment record is re-read in the restore's commit, under the commit
// lock: a comment deleted since (an author delete keeps a removed comment's
// bytes, decision 2026-11-08) leaves the removal record and its withholds
// standing, and deleted is false.
func (m *Materializer) RestoreBridgedComment(ctx context.Context, mapping *store.APObjectMapping) (deleted bool, err error) {
	communityDID, err := m.commentModerationTarget(ctx, mapping)
	if err != nil {
		return false, err
	}
	return m.deleteCommentRemoval(ctx, communityDID, mapping.ATURI, m.hostedRestoreLift(mapping.ATURI, ""))
}

// commentModerationTarget resolves the community a bridged comment's removal
// record belongs in, refusing anything that is not a comment.
func (m *Materializer) commentModerationTarget(ctx context.Context, mapping *store.APObjectMapping) (string, error) {
	if mapping == nil {
		return "", errors.NewValidationError("mapping", "must not be nil")
	}
	if mapping.Collection != CollectionComment {
		return "", errors.NewValidationError("mapping",
			"comment removal records are only written for "+CollectionComment+", got "+mapping.Collection)
	}
	communityDID, err := CommunityDIDOf(ctx, m.repos, mapping)
	if err != nil {
		return "", err
	}
	if communityDID == "" {
		return "", errors.NewValidationError("mapping",
			"cannot moderate "+mapping.ATURI+": it binds to no community")
	}
	return communityDID, nil
}
