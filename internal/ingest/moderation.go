package ingest

import (
	"context"
	"fmt"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// moderationState is the bridge-owned moderation store, or an error naming the
// gap. Every moderation path asks for it before deciding anything.
//
// The error is RETRYABLE and never a skip. A dispatcher whose mapping store is
// not also the moderation repository (a substitute view, a future alternate
// backend) can still serve every read path — but a lock it cannot record reads
// downstream as "no lock", and a removal it cannot record reads as "nobody
// moderated this". A decision that cannot be stored has to stay on the queue
// where an operator sees it, not be marked processed as though it were handled.
func (h *Handler) moderationState() (store.ObjectModeration, error) {
	if h.moderation == nil {
		return nil, fmt.Errorf(
			"ingest: no moderation state store is wired, so this decision cannot be recorded")
	}
	return h.moderation, nil
}

// moderateNativeComment decides an announced Delete of a NATIVE comment — a
// record in the AUTHOR's own repo that this bridge federated on their behalf.
//
// TWO SHAPES ARRIVE ON ONE ACTIVITY, and only `summary` tells them apart
// (PRESENCE, never emptiness — a moderator who typed no reason sends `""`). The
// post path already turns on exactly this, and the two outcomes here are as far
// apart as they are there:
//
//   - WITH a summary: a moderator removed the comment. The decision is recorded
//     bridge-side, bound to the community that made it, under the SAME code a
//     post removal writes.
//   - WITHOUT one: NOTHING is recorded. Writing moderator-discretion here would
//     assert that a moderator acted when none did — a removal naming a moderator
//     team that took no action — and it would stand until somebody sent an Undo
//     for something that never happened.
//
// Lemmy's convention says the summary-less shape IS the author's own delete, and
// that is why it records nothing. But the bridge cannot VERIFY that, and must
// not claim it: an Announce's inner actor is unauthenticated, and a truthful
// author self-delete never even reaches here — the echo classifier takes it by
// the inner actor first (a native comment's author is one of our personas). So
// what actually arrives on this branch is a summary-less announce whose
// attribution is foreign or unverifiable, and the counter and the reason say
// exactly that rather than narrating a motive.
//
// Both are TAKEN, and that is the point of the branch: falling through runs the
// v1 destructive path against the author's record, soft-deleting our own mapping
// and tombstoning our own AP id, after which moderateAnnouncedDelete declines
// forever on IsDeleted() and every Lemmy reply beneath the comment is dropped.
// So they share the taking and differ in everything else — including the skip
// REASON and the COUNTER, because "a moderator removed this" and "the author
// deleted it themselves" are opposite answers to the only question an operator
// ever asks here, and one line for both answers neither.
//
// Ownership is NOT re-checked here. authorizeDelete already established that the
// announcing community owns this mapping (the same conjunction the lock path
// uses), and a second copy of that rule is a second place for it to drift.
//
// NOTHING COVES-VISIBLE IS WRITTEN, deliberately: the removal lexicon is
// post-scoped, the comment-subject extension is Coves-owned and has not landed,
// and inventing a record shape here would publish a vocabulary the read path
// does not consult — a moderation decision that looks honored and hides nothing.
// The skip reason says so, because that gap is the whole of what an operator
// needs to know about this path today.
func (h *Handler) moderateNativeComment(ctx context.Context, del *ap.Object,
	mapping *store.APObjectMapping, announcer *store.Community) error {

	if !del.HasSummary() {
		NativeCommentSummarylessDelete.Add(1)
		return skip(mapping.APID,
			"announced delete of a native comment carries no summary, so it is not a moderator "+
				"removal: nothing is recorded, and the activity is taken rather than declined so "+
				"it cannot reach the path that destroys the author's record")
	}

	moderation, err := h.moderationState()
	if err != nil {
		return err
	}
	// announcer.DID is the community authorizeDelete just proved owns this
	// mapping, so the row is bound to the community that made the decision.
	if err := moderation.SetRemoval(ctx, store.ModeratedObject{
		ATURI:        mapping.ATURI,
		APID:         mapping.APID,
		CommunityDID: announcer.DID,
	}, materialize.RemovalCodeModeratorDiscretion, del.Summary); err != nil {
		return fmt.Errorf("ingest: record removal of %s: %w", mapping.ATURI, err)
	}
	NativeCommentRemoved.Add(1)
	h.logger.Info("community removed a native comment; recorded bridge-side",
		"ap_id", mapping.APID, "at_uri", mapping.ATURI,
		"community", announcer.APGroupID, "activity", del.ID)
	return skip(mapping.APID,
		"announced moderator removal of a native comment: recorded bridge-side under "+
			materialize.RemovalCodeModeratorDiscretion+"; nothing is published to the community "+
			"repo until the comment-subject removal lexicon lands")
}

// liftNativeCommentRemoval is the Undo of the above: the moderators reversed
// their decision, so the bridge-side removal is cleared.
//
// It runs for EVERY announced Undo{Delete} of a native comment, whether or not
// the undone Delete carried a summary — the same reasoning restoreNativeContent
// applies to posts. On the delete side the key separates two opposite actions,
// so presence has to decide; here both readings converge on the same
// non-destructive outcome (a removal that no longer stands), and requiring the
// key would only create a way for a real restore to be dropped, leaving a
// removal the moderators lifted standing forever.
//
// IT ALSO CLEARS THE LEGACY DELETE STATE, exactly as the post restore does, and
// for exactly the reason RESTORE-1 exists. A native comment mapping CAN be
// soft-deleted and tombstoned — the pre-17c-1 v1 path did both, and the
// origin-verified delete sweep still can — and if this Undo returned without
// clearing them, moderateAnnouncedDelete would decline forever on IsDeleted()
// and the comment would be permanently unmoderatable, by the community's own
// legitimate restore. Both are idempotent single statements and cost nothing in
// the ordinary case, where there is nothing to clear.
//
// Clearing a removal nobody made is a no-op success — a re-delivered Undo, or
// one from a community that never removed this comment — and it is reported as
// such rather than counted as another reversal.
func (h *Handler) liftNativeCommentRemoval(ctx context.Context, undo *ap.Object,
	mapping *store.APObjectMapping, announcer *store.Community, scope string) error {

	moderation, err := h.moderationState()
	if err != nil {
		return err
	}
	if err := h.tombstones.Remove(ctx, mapping.APID, scope); err != nil {
		return fmt.Errorf("ingest: clear tombstone for %s: %w", mapping.APID, err)
	}
	if err := h.objects.Restore(ctx, mapping.APID); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("ingest: restore mapping for %s: %w", mapping.APID, err)
	}
	cleared, err := moderation.ClearRemoval(ctx, mapping.ATURI, announcer.DID)
	if err != nil {
		return fmt.Errorf("ingest: clear removal of %s: %w", mapping.ATURI, err)
	}
	if !cleared {
		return skip(mapping.APID,
			"announced restore of a native comment that this community had not removed: "+
				"nothing to lift (a re-delivered undo, or an undo of a delete that recorded nothing)")
	}
	NativeCommentRemovalLifted.Add(1)
	h.logger.Info("community lifted its removal of a native comment",
		"ap_id", mapping.APID, "at_uri", mapping.ATURI,
		"community", announcer.APGroupID, "activity", undo.ID)
	return skip(mapping.APID,
		"announced restore of a native comment: the bridge-side removal is cleared; nothing "+
			"is published to the community repo until the comment-subject removal lexicon lands")
}

// handleLock applies an announced Lock — a community closing one of its threads
// — and, with locked=false, the Undo that lifts it.
//
// A lock is state NOBODY ELSE CAN HOLD. The post is the author's record, the
// acceptance says only that it was admitted, and Lemmy keeps the flag on its own
// post row; so the bridge records it (store.ObjectModeration) and the native
// comment consumer reads it back before federating a reply. Recording it and
// then federating a reply under it would be worse than not recording it at all:
// Lemmy rejects comments on locked posts server-side, so the reply buys a failed
// delivery, a retry loop and finally a poisoned row whose cause is a moderator
// decision nothing in the delivery names.
//
// AUTHORIZATION IS THE SAME CONJUNCTION AS EVERY OTHER ANNOUNCED MODERATION
// ACTION (decision 18), asked of the same function: the announcing community
// must OWN the target's mapping. Not authority equality — Lemmy co-hosts many
// communities per instance and SameAuthority is true across all of them, so
// authority alone would let one moderator team freeze every thread in every
// community beside theirs.
//
// Nothing here enqueues, and nothing can: the write is a bridge-side state
// write, and an inbound moderation action echoed back is an activity aimed at
// the moderators who just sent it.
func (h *Handler) handleLock(ctx context.Context, lock *ap.Object, announcer *store.Community, locked bool) error {
	if announcer == nil {
		// A bare Lock has no community behind it. The verb IS a community
		// decision — Lemmy announces every one of them through the group — so a
		// direct one is either a mistake or somebody claiming an authority the
		// signature does not carry.
		return skip(lock.ID, "bare lock is not a community decision; only an announced lock is")
	}
	targetID := refID(lock.Object)
	if targetID == "" {
		return errors.NewValidationError("lock", "lock carries no object id")
	}

	mapping, err := h.objects.GetByAPID(ctx, targetID)
	if errors.IsNotFound(err) {
		// Nothing was ever bridged under this id, so there is nothing to refuse
		// comments on. Unlike a Delete there is no marker worth laying: a lock
		// for content that does not exist here suppresses nothing, and a future
		// Create would carry no reply the lock could apply to anyway.
		return skip(lock.ID, "lock of an object the bridge has no mapping for: "+targetID)
	}
	if err != nil {
		return fmt.Errorf("ingest: look up mapping for lock of %s: %w", targetID, err)
	}
	if mapping.IsDeleted() {
		// A soft-deleted mapping is content already withdrawn: replies to it are
		// refused by subject resolution long before a lock could matter. It is
		// also the one state in which the authorization below cannot bind the
		// target to a community (the record its fallback reads is gone), so
		// declining here keeps an unbindable target from being recorded against
		// whichever community happened to announce.
		return skip(lock.ID, "lock of an already-deleted object: "+targetID)
	}
	if err := h.authorizeAnnouncedContentDelete(ctx, lock.ID, mapping, announcer); err != nil {
		return err
	}

	moderation, err := h.moderationState()
	if err != nil {
		return err
	}
	// announcer.DID is the community the authorization above just proved OWNS
	// this mapping — the same DID CommunityDIDOf answered with — so the row is
	// bound to the community that made the decision, not to whoever announced.
	if err := moderation.SetLock(ctx, store.ModeratedObject{
		ATURI:        mapping.ATURI,
		APID:         mapping.APID,
		CommunityDID: announcer.DID,
	}, locked); err != nil {
		return fmt.Errorf("ingest: record lock state for %s: %w", mapping.ATURI, err)
	}
	h.logger.Info("community lock state applied",
		"ap_id", mapping.APID, "at_uri", mapping.ATURI,
		"community", announcer.APGroupID, "locked", locked, "activity", lock.ID)
	return nil
}
