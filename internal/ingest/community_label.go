package ingest

import (
	"context"
	"database/sql"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"

	"tidepool/internal/errors"
	"tidepool/internal/labels"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// Bridged community removal. A Coves `!takedown` label on the bare DID of a
// bridged community (a communities row with that DID) removes the community:
// in the label's transaction the applier records the removal, with the
// community's follow state and provenance at that moment, and sets the follow
// state to none; once the transaction commits it sends Undo{Follow}. The
// community's row and its records stay. While the removal stands no follow path follows it (subscribe, the
// follow-list sweep, the follow retrier, backfill: see
// store.CommunityRemovals).
//
// The negation (or the label's lapse) clears the removal, and follows the
// community again when it was followed (accepted or pending) at the removal
// and the way it was followed still wants it: a manual subscribe always does,
// a follow-list or instance entry only while the current follow list desires
// the community. The negation records the community pending with no send time
// and last_backfill_at cleared, and sends nothing itself: the follow retrier's
// next sweep sends the Follow, and its Accept walks the outbox again.
//
// No network send runs inside the label's transaction, which holds the global
// repo commit lock and the community's locks. The removal's transaction
// records the Undo{Follow} as owed (store.MarkUnfollowOwedTx); a delivered
// send clears it, and a failed send (or a crash before it) leaves it for the
// follow retrier, which sends owed Undo{Follow}s at its start and every sweep.
// The community is unfollowed either way. Database failures fail the label's transaction. A label for a DID Tidepool has no community for changes nothing
// here; subscribe refuses that community once it is minted while the label
// stands.

// communityLabelSendTimeout bounds the Undo{Follow} a community removal sends
// after the label's transaction commits.
const communityLabelSendTimeout = 30 * time.Second

// CommunityLabelApplier is the label applier for bare-DID `!takedown` labels
// whose DID is a bridged community. It also implements labels.Activator.
func (a *Admin) CommunityLabelApplier() labels.Applier { return communityLabelApplier{admin: a} }

type communityLabelApplier struct{ admin *Admin }

// ApplyLabel removes the community while its `!takedown` stands, and
// restores it once the label no longer stands.
func (c communityLabelApplier) ApplyLabel(ctx context.Context, tx *sql.Tx, l labels.Stored) error {
	if !isCommunityLabel(l) {
		return nil
	}
	standing, err := labels.StandingTx(ctx, tx, l.URI, materialize.LabelTakedown)
	if err != nil {
		return err
	}
	if standing {
		return c.remove(ctx, tx, l)
	}
	return c.restore(ctx, tx, l.URI)
}

// Activate removes the community of a standing `!takedown` stored by a
// consumer without this applier. Idempotent: an active removal keeps the
// follow state and provenance it recorded, and an unfollowed community is sent
// nothing.
func (c communityLabelApplier) Activate(ctx context.Context, tx *sql.Tx, l labels.Stored) error {
	if !isCommunityLabel(l) {
		return nil
	}
	standing, err := labels.StandingTx(ctx, tx, l.URI, materialize.LabelTakedown)
	if err != nil || !standing {
		return err
	}
	return c.remove(ctx, tx, l)
}

// isCommunityLabel reports a `!takedown` on a bare DID.
func isCommunityLabel(l labels.Stored) bool {
	if l.Val != materialize.LabelTakedown {
		return false
	}
	_, err := syntax.ParseDID(l.URI)
	return err == nil
}

// remove records the removal of the community with l's DID and unfollows it.
func (c communityLabelApplier) remove(ctx context.Context, tx *sql.Tx, l labels.Stored) error {
	community, err := store.LockCommunityByDIDTx(ctx, tx, l.URI)
	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if _, err := store.AddCommunityRemovalTx(ctx, tx, store.CommunityRemoval{
		CommunityDID:         community.DID,
		LabelerDID:           l.Src,
		FollowStateAtRemoval: community.FollowState,
		ProvenanceAtRemoval:  community.FollowProvenance,
	}); err != nil {
		return err
	}
	if community.FollowState == store.FollowStateNone {
		return nil
	}
	if err := store.SetFollowStateTx(ctx, tx, community.APGroupID, store.FollowStateNone); err != nil {
		return err
	}
	removalID, err := store.MarkUnfollowOwedTx(ctx, tx, community.DID)
	if err != nil {
		return err
	}
	logger := c.admin.logger.With("community", community.APGroupID, "did", community.DID,
		"follow_state", string(community.FollowState))
	if err := labels.AfterCommit(ctx, func(ctx context.Context) {
		sendCtx, cancel := context.WithTimeout(ctx, communityLabelSendTimeout)
		defer cancel()
		if err := c.admin.sendUndoFollow(sendCtx, community.APGroupID); err != nil {
			logger.Error("community removed by label: Undo{Follow} not delivered; the follow retrier sends it",
				"error", err)
			return
		}
		if removals, ok := c.admin.communities.(store.CommunityRemovals); ok {
			refollowOwed, err := removals.CompleteOwedUnfollow(sendCtx, removalID)
			if err != nil {
				logger.Error("community removed by label: unfollowed, but the owed Undo{Follow} not completed; the follow retrier resends it, or owes a Follow if the community is followed again",
					"error", err)
				return
			}
			if refollowOwed {
				logger.Info("community removed by label: Undo{Follow} delivered after the community was followed again; the follow retrier sends its Follow")
				return
			}
		}
		logger.Info("community removed by label: unfollowed")
	}); err != nil {
		return err
	}
	return nil
}

// restore clears the removal of the community with did and follows it again
// when it is still wanted.
//
// An Undo{Follow} the removal still owes stays recorded. When the community is
// followed again it is pending here, and the follow retrier, which sends both
// the owed Undo{Follow} and the owed Follow, completes an owed Undo{Follow} of
// a followed community unsent (re-pending it with its Follow owed) before it
// claims Follows: the owed Undo{Follow} never reaches Lemmy after the
// restored Follow. An Undo{Follow} already in flight (the removal's own send,
// or the retrier's) can still land after a new Follow: its completion (store
// CompleteOwedUnfollow) then re-pends the community with its Follow owed, and
// when that completion fails the retrier's completion of the still-owed
// Undo{Follow} does. When it is not followed again the Undo{Follow} is still
// owed and still sent.
func (c communityLabelApplier) restore(ctx context.Context, tx *sql.Tx, did string) error {
	community, err := store.LockCommunityByDIDTx(ctx, tx, did)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}
	removal, err := store.ClearCommunityRemovalTx(ctx, tx, did)
	if err != nil || removal == nil || community == nil {
		return err
	}
	// The community's kept objects are reconciled with their origins once it
	// is ready (CommunityReconciler), whether or not it is followed again.
	if err := store.MarkCommunityReconcilePendingTx(ctx, tx, community.DID); err != nil {
		return err
	}
	if removal.FollowStateAtRemoval == store.FollowStateNone || community.FollowState != store.FollowStateNone {
		return nil
	}
	if !c.stillWanted(community, removal.ProvenanceAtRemoval) {
		c.admin.logger.Info("community removal negated: not followed again, no longer wanted",
			"community", community.APGroupID, "provenance", string(removal.ProvenanceAtRemoval))
		return nil
	}
	if err := store.RestoreFollowTx(ctx, tx, community.APGroupID, false); err != nil {
		return err
	}
	c.admin.logger.Info("community removal negated: Follow owed, sent by the follow retrier",
		"community", community.APGroupID, "did", community.DID)
	return nil
}

// stillWanted reports whether the path that followed the community still
// wants it: a manual subscribe always does; any other provenance (unknown
// included) only while the current follow list desires the community. With no
// follow list, or one that does not parse, only a manual subscribe is followed
// again; a later sweep of a list that parses follows a desired community then.
func (c communityLabelApplier) stillWanted(community *store.Community, provenance store.FollowProvenance) bool {
	if provenance == store.FollowProvenanceManual {
		return true
	}
	reconciler := c.admin.reconciler
	if reconciler == nil {
		return false
	}
	list, err := reconciler.parse(reconciler.path)
	if err != nil {
		c.admin.logger.Warn("community removal negated: follow list unreadable; a later sweep follows it if desired",
			"community", community.APGroupID, "error", err)
		return false
	}
	return list.Desires(community.APGroupID, community.Instance, community.PreferredUsername)
}
