package ingest

import (
	"context"
	"expvar"
	"fmt"
	"time"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// BanRecordPublisher writes and deletes a community's ban record
// (social.coves.moderation.ban) in the community's repo, the only form of a
// community ban Coves enforces. *materialize.Materializer implements it; it is
// obtained by type assertion for the same reason AuthorModerator is.
type BanRecordPublisher interface {
	// WriteBanRecord writes (or overwrites whole) the ban record at the
	// subject's digest rkey. written is false when the identical record
	// already stood and no commit was made.
	WriteBanRecord(ctx context.Context, communityDID, subjectDID, reason string, expiresAt *time.Time, createdAt time.Time) (written bool, err error)
	// WriteBanRecordIfUnchanged is WriteBanRecord guarded by the record the
	// caller read: expectedCID names it, or is "" when none stood. A record
	// changed since fails with a retryable precondition error.
	WriteBanRecordIfUnchanged(ctx context.Context, communityDID, subjectDID, reason string, expiresAt *time.Time, createdAt time.Time, expectedCID string) (written bool, err error)
	// DeleteBanRecord deletes that record. deleted is false, with no commit,
	// when none stands.
	DeleteBanRecord(ctx context.Context, communityDID, subjectDID string) (deleted bool, err error)
	// DeleteBanRecordIfUnchanged deletes that record only while it is still
	// the record expectedCID names; a record rewritten since fails with a
	// retryable precondition error.
	DeleteBanRecordIfUnchanged(ctx context.Context, communityDID, subjectDID, expectedCID string) (deleted bool, err error)
	// StandingBanRecord reads the standing ban record, or nil when none stands.
	StandingBanRecord(ctx context.Context, communityDID, subjectDID string) (*materialize.BanRecord, error)
}

// BlockUnmintedSubject counts announced community Blocks and Undo{Block}s
// naming an actor that is neither one of our personas nor a bridged person
// this bridge minted: a Lemmy user who never posted anything we bridged. They
// have no DID, so no ban record can name them; Lemmy enforces the ban on its
// own side. It is the common case (every community we follow announces its own
// bans), and the counter is what tells "none of them were ours" from "we
// stopped receiving them".
var BlockUnmintedSubject = expvar.NewInt("tidepool_block_unminted_subject")

// banRecordPublisher is the surface that writes and deletes ban records, or an
// error naming the gap. The error is RETRYABLE, never a skip: a ban that
// cannot be published is a ban Coves never enforces.
func (h *Handler) banRecordPublisher() (BanRecordPublisher, error) {
	if h.banRecords == nil {
		return nil, fmt.Errorf(
			"ingest: no ban-record publishing surface is wired, so this ban cannot be published")
	}
	return h.banRecords, nil
}

// banCreatedAt is the createdAt of a ban record a live Block writes: the time
// the inbox received the Block, so a redelivery of the same event rebuilds the
// same bytes and commits nothing.
func banCreatedAt(receivedAt time.Time) time.Time {
	if receivedAt.IsZero() {
		return timeNow()
	}
	return receivedAt
}

// undoneBanExpiry is the expiry the Block an Undo reverses names, or nil when
// it names none. A present but unreadable expiry is read as absent, for the
// reason liftBan gives: the worst case is an unconditional lift of a ban the
// moderators have reversed, where refusing would keep it in force.
func undoneBanExpiry(block *ap.Object) *time.Time {
	expiry := block.BanExpiry()
	if expiry == nil || !expiry.Valid {
		return nil
	}
	when := expiry.Time
	return &when
}

// banSubjectKind is what a ban's subject, not one of our personas, is to this
// bridge.
type banSubjectKind int

const (
	// banSubjectMinted is a bridged Person with a DID: a ban record can name it.
	banSubjectMinted banSubjectKind = iota
	// banSubjectUnminted is a Lemmy user the bridge never minted.
	banSubjectUnminted
	// banSubjectNotPerson is a bridged actor that is not a Person (a Group,
	// whose DID is a community repo).
	banSubjectNotPerson
)

// banSubject resolves a ban subject's AP id against the bridged actors. The
// actor is returned only for banSubjectMinted.
func (h *Handler) banSubject(ctx context.Context, apID string) (*store.BridgedActor, banSubjectKind, error) {
	actor, err := h.actors.GetByAPActorID(ctx, apID)
	switch {
	case errors.IsNotFound(err):
		return nil, banSubjectUnminted, nil
	case err != nil:
		return nil, 0, fmt.Errorf("ingest: look up ban subject %s: %w", apID, err)
	case actor.ActorType != store.ActorTypePerson:
		return nil, banSubjectNotPerson, nil
	case actor.DID == "":
		return nil, banSubjectUnminted, nil
	}
	return actor, banSubjectMinted, nil
}

// handleBridgedSubjectBlock decides an announced community Block, or with
// banned=false its Undo, whose subject is not one of our personas.
//
// A minted bridged PERSON gets a ban record in the community repo, so Coves
// enforces the ban on the bridged DID; a Block with removeData then removes
// their earlier content as before (decideCommunityBan, removeBannedContent).
// A Block whose expiry cannot be read writes no record but still removes
// content, then fails validation.
// No community_bans row is written: that table gates our personas' outbound
// posts, and a bridged person's future content reaches us only through the
// community that banned them.
//
// An actor with no bridged Person row is skipped and counted: an unminted Lemmy
// user has no DID a record could name, and a bridged Group's DID is a community
// repo, not an account.
func (h *Handler) handleBridgedSubjectBlock(ctx context.Context, block *ap.Object, announcer *store.Community, banned bool, activityID string, receivedAt time.Time) error {
	subjectAPID := refID(block.Object)
	actor, kind, err := h.banSubject(ctx, subjectAPID)
	if err != nil {
		return err
	}
	switch kind {
	case banSubjectNotPerson:
		BlockForeignSubject.Add(1)
		return skip(block.ID,
			"announced Block names "+subjectAPID+", a bridged actor that is not a Person: its DID is "+
				"not an account a community can ban, so nothing is recorded or removed")
	case banSubjectUnminted:
		BlockUnmintedSubject.Add(1)
		return skip(block.ID,
			"announced Block names "+subjectAPID+", which is neither one of our personas nor a "+
				"bridged person this bridge minted: no DID exists for a ban record to name, and the "+
				"ban is enforced on the subject's own instance")
	}
	publisher, err := h.banRecordPublisher()
	if err != nil {
		return err
	}
	if !banned {
		return h.liftBridgedBan(ctx, block, announcer, actor.DID, publisher)
	}

	expiry := block.BanExpiry()
	if expiry != nil && !expiry.Valid {
		// No ban record, as for a native persona (see applyBan): written without
		// its expiry it would publish a permanent ban the moderator did not ask
		// for. removeData does not depend on the expiry, so the content removal
		// still runs, as it does for replay-bans; then the Block is refused.
		BlockExpiryUnreadable.Add(1)
		ban, err := h.decideCommunityBan(ctx, block, announcer, activityID, receivedAt)
		if err != nil {
			return err
		}
		if ban != nil {
			if err := h.removeBannedContent(ctx, ban); err != nil {
				return err
			}
		}
		return errors.NewValidationError("expires",
			"block for "+actor.DID+" carries an expiry that cannot be parsed; refusing to "+
				"publish it as a permanent ban")
	}
	var expiresAt *time.Time
	if expiry != nil {
		when := expiry.Time
		expiresAt = &when
	}
	// A Block already over when it arrived writes no record and replaces none:
	// written over a ban in force it would end an exclusion nobody lifted.
	// Judged at receipt, not at this attempt: a record write retried after the
	// expiry must still replace the record the Block replaced.
	lapsed := expiresAt != nil && !expiresAt.After(banCreatedAt(receivedAt))
	if !lapsed {
		if _, err := publisher.WriteBanRecord(ctx, announcer.DID, actor.DID, block.Summary, expiresAt,
			banCreatedAt(receivedAt)); err != nil {
			return fmt.Errorf("ingest: publish ban of %s in %s: %w", actor.DID, announcer.APGroupID, err)
		}
		h.logger.Info("community banned a bridged person; ban record published",
			"community", announcer.APGroupID, "subject_did", actor.DID,
			"expires", expiresAt, "activity", block.ID)
	}

	ban, err := h.decideCommunityBan(ctx, block, announcer, activityID, receivedAt)
	if err != nil {
		return err
	}
	if ban != nil {
		return h.removeBannedContent(ctx, ban)
	}
	if lapsed {
		BlockLapsedIgnored.Add(1)
		return skip(block.ID,
			"announced Block of a bridged person expired before it arrived and removes nothing: "+
				"no ban record is written, and a standing one is left as it is")
	}
	return nil
}

// liftBridgedBan is Undo{Block} for a bridged person: the ban record goes,
// under the stale-unban rule store.CommunityBans.Lift applies to our personas,
// judged against the standing record itself (a bridged person has no
// community_bans row). An Undo naming undone expiry T is stale, and refused,
// when the standing record is still in force and is permanent or expires after
// T: an old unban redriven after a re-ban. An Undo naming no expiry lifts
// whatever stands.
//
// The delete is guarded by the CID the decision read, so a ban written in
// between is not deleted on the strength of a decision about the one it
// replaced; the precondition miss is retryable, and the retry decides again.
func (h *Handler) liftBridgedBan(ctx context.Context, block *ap.Object, announcer *store.Community, subjectDID string, publisher BanRecordPublisher) error {
	standing, err := publisher.StandingBanRecord(ctx, announcer.DID, subjectDID)
	if err != nil {
		return fmt.Errorf("ingest: read ban record of %s in %s: %w", subjectDID, announcer.APGroupID, err)
	}
	if standing == nil {
		return skip(block.ID, fmt.Sprintf(
			"announced Undo{Block} for bridged person %s in %s, who holds no ban record there: "+
				"nothing to lift", subjectDID, announcer.APGroupID))
	}
	undoneExpiry := undoneBanExpiry(block)
	if undoneExpiry != nil && standing.InForce(timeNow()) &&
		(standing.ExpiresAt == nil || standing.ExpiresAt.After(*undoneExpiry)) {
		BlockUndoStaleRefused.Add(1)
		h.logger.Warn("announced Undo{Block} reverses a ban that is no longer the one in force",
			"community", announcer.APGroupID, "subject_did", subjectDID,
			"undone_expiry", undoneExpiry, "activity", block.ID)
		return skip(block.ID, fmt.Sprintf(
			"announced Undo{Block} for bridged person %s in %s undoes a ban expiring %s, but the "+
				"ban record standing there outlives it: the record is KEPT, because an Undo replayed "+
				"after a re-ban would lift it permanently and Lemmy will send no second Block",
			subjectDID, announcer.APGroupID, undoneExpiry))
	}
	if _, err := publisher.DeleteBanRecordIfUnchanged(ctx, announcer.DID, subjectDID, standing.CID); err != nil {
		return fmt.Errorf("ingest: delete ban record of %s from %s: %w", subjectDID, announcer.APGroupID, err)
	}
	h.logger.Info("community lifted a bridged person's ban; ban record deleted",
		"community", announcer.APGroupID, "subject_did", subjectDID, "activity", block.ID)
	return nil
}
