package materialize

import (
	"context"
	"expvar"
	"fmt"
	"time"

	"tidepool/internal/errors"
	"tidepool/internal/repo"
)

// CollectionBan is a community's ban of one account, written into the
// community's own repo. Coves enforces a community ban only from this record.
const CollectionBan = "social.coves.moderation.ban"

// The ban record counters. Like the comment-removal counters they count
// COMMITS: a redelivered ban that re-puts identical bytes, and an unban that
// finds no record to delete, change nothing and are not counted.
var (
	// BanRecordsWritten counts social.coves.moderation.ban records written (or
	// overwritten by a later ban of the same subject).
	BanRecordsWritten = expvar.NewInt("tidepool_moderation_ban_records_written")
	// BanRecordsDeleted counts ban records deleted by an unban.
	BanRecordsDeleted = expvar.NewInt("tidepool_moderation_ban_records_deleted")
)

// BanRKey derives the record key of a community's ban record for one subject:
// lowercase unpadded RFC 4648 base32 of SHA-256 over the subject DID, the
// encoding SubjectRKey applies to an at-uri. The ban lexicon fixes this key, so
// a community holds at most one ban record per subject and Coves indexes a ban
// record at no other key.
func BanRKey(subjectDID string) string {
	return SubjectRKey(subjectDID)
}

// BanRecord is the ban record standing in a community repo for one subject.
type BanRecord struct {
	// Subject is the DID the record names.
	Subject string
	// Reason is the moderator's reason, empty when the record carries none.
	Reason string
	// ExpiresAt is when the ban ends; nil for a permanent ban.
	ExpiresAt *time.Time
	// CID is the record's CID, the precondition a guarded delete names.
	CID string
}

// InForce reports whether the ban is still in force at now: permanent, or
// expiring after now.
func (b *BanRecord) InForce(now time.Time) bool {
	return b.ExpiresAt == nil || b.ExpiresAt.After(now)
}

// WriteBanRecord publishes a community's ban of subjectDID into the community
// repo at BanRKey(subjectDID). A later ban of the same subject replaces the
// record whole: a field the new ban does not carry (a reason, an expiry) is
// gone afterwards, never merged forward from the old record.
//
// reason is omitted when empty, and cut to the lexicon's 2000 graphemes /
// 20000 bytes when longer; expiresAt is omitted when nil (a permanent ban).
// createdAt is the caller's: the time the ban was received, so a redelivery of
// the same ban rebuilds byte-identical bytes and commits nothing.
//
// written reports whether a commit was made (false on the identical re-put).
func (m *Materializer) WriteBanRecord(ctx context.Context, communityDID, subjectDID, reason string, expiresAt *time.Time, createdAt time.Time) (written bool, err error) {
	return m.writeBanRecord(ctx, communityDID, subjectDID, reason, expiresAt, createdAt, nil)
}

// WriteBanRecordIfUnchanged is WriteBanRecord guarded by the record the caller
// read: expectedCID names the standing record, or is "" when none stood, in
// which case the write requires that none stands yet. A record written or
// deleted since the read fails the precondition (repo.ErrPreconditionFailed)
// and nothing is written, so a decision made about one record never
// overwrites another.
func (m *Materializer) WriteBanRecordIfUnchanged(ctx context.Context, communityDID, subjectDID, reason string, expiresAt *time.Time, createdAt time.Time, expectedCID string) (written bool, err error) {
	return m.writeBanRecord(ctx, communityDID, subjectDID, reason, expiresAt, createdAt, &expectedCID)
}

func (m *Materializer) writeBanRecord(ctx context.Context, communityDID, subjectDID, reason string, expiresAt *time.Time, createdAt time.Time, expectedCID *string) (bool, error) {
	if communityDID == "" {
		return false, errors.NewValidationError("community_did", "must not be empty")
	}
	if subjectDID == "" {
		return false, errors.NewValidationError("subject", "must not be empty")
	}
	ban := map[string]any{
		"$type":     CollectionBan,
		"subject":   subjectDID,
		"createdAt": recordDatetime(createdAt),
	}
	if reason != "" {
		// The ban lexicon's reason limits; a Block's summary has none.
		ban["reason"] = truncateText(reason, 2000, 20000)
	}
	if expiresAt != nil {
		ban["expiresAt"] = recordDatetime(*expiresAt)
	}
	if err := m.validateRecord(ban); err != nil {
		return false, err
	}
	rkey := BanRKey(subjectDID)
	res, err := m.repos.ApplyOps(ctx, communityDID, []repo.RecordOp{
		{Action: repo.OpActionUpdate, Collection: CollectionBan, RKey: rkey, Record: ban, ExpectPrevCID: expectedCID},
	})
	if err != nil {
		return false, fmt.Errorf("materialize: write ban record for %s into %s: %w", subjectDID, communityDID, err)
	}
	if res.NoOp {
		return false, nil
	}
	BanRecordsWritten.Add(1)
	m.logger.Info("ban record written into community repo",
		"community_did", communityDID, "subject_did", subjectDID, "rkey", rkey, "permanent", expiresAt == nil)
	return true, nil
}

// DeleteBanRecord deletes the community's ban record for subjectDID: the
// unban. With no record at the subject's rkey it is inert and commits nothing,
// so a retried or re-delivered unban completes whatever an earlier attempt
// left and otherwise changes nothing.
//
// deleted reports whether a record was there and removed.
func (m *Materializer) DeleteBanRecord(ctx context.Context, communityDID, subjectDID string) (deleted bool, err error) {
	return m.deleteBanRecord(ctx, communityDID, subjectDID, nil)
}

// DeleteBanRecordIfUnchanged deletes the ban record only while it is still the
// record expectedCID names. A ban written between the caller's read and this
// delete fails the precondition (repo.ErrPreconditionFailed, a retryable
// error) instead of being deleted on the strength of a decision about the
// record it replaced.
func (m *Materializer) DeleteBanRecordIfUnchanged(ctx context.Context, communityDID, subjectDID, expectedCID string) (deleted bool, err error) {
	if expectedCID == "" {
		return false, errors.NewValidationError("expected_cid", "must not be empty")
	}
	return m.deleteBanRecord(ctx, communityDID, subjectDID, &expectedCID)
}

func (m *Materializer) deleteBanRecord(ctx context.Context, communityDID, subjectDID string, expectedCID *string) (bool, error) {
	if communityDID == "" {
		return false, errors.NewValidationError("community_did", "must not be empty")
	}
	if subjectDID == "" {
		return false, errors.NewValidationError("subject", "must not be empty")
	}
	rkey := BanRKey(subjectDID)
	res, err := m.repos.ApplyOps(ctx, communityDID, []repo.RecordOp{
		{Action: repo.OpActionDelete, Collection: CollectionBan, RKey: rkey, ExpectPrevCID: expectedCID},
	})
	if err != nil {
		return false, fmt.Errorf("materialize: delete ban record for %s from %s: %w", subjectDID, communityDID, err)
	}
	if res.NoOp {
		return false, nil
	}
	BanRecordsDeleted.Add(1)
	m.logger.Info("ban record deleted from community repo",
		"community_did", communityDID, "subject_did", subjectDID, "rkey", rkey)
	return true, nil
}

// StandingBanRecord reads the ban record standing in the community repo for
// subjectDID, or nil (and no error) when none stands. A record whose expiresAt
// cannot be parsed is a validation error: this bridge writes no such record,
// and reading it as permanent or as absent would each decide an unban wrongly.
func (m *Materializer) StandingBanRecord(ctx context.Context, communityDID, subjectDID string) (*BanRecord, error) {
	if communityDID == "" {
		return nil, errors.NewValidationError("community_did", "must not be empty")
	}
	if subjectDID == "" {
		return nil, errors.NewValidationError("subject", "must not be empty")
	}
	stored, storedCID, err := m.repos.GetRecord(ctx, communityDID, CollectionBan, BanRKey(subjectDID))
	if errors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("materialize: read ban record for %s in %s: %w", subjectDID, communityDID, err)
	}
	standing := &BanRecord{CID: storedCID}
	standing.Subject, _ = stored["subject"].(string)
	standing.Reason, _ = stored["reason"].(string)
	if raw, present := stored["expiresAt"]; present {
		text, _ := raw.(string)
		when, perr := time.Parse(time.RFC3339Nano, text)
		if perr != nil {
			return nil, errors.NewValidationError("expiresAt",
				fmt.Sprintf("ban record for %s in %s carries an unreadable expiresAt %q", subjectDID, communityDID, text))
		}
		standing.ExpiresAt = &when
	}
	return standing, nil
}
