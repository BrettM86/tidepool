// Consent enforcement (PLAN.md locked decision 6, mirroring Bridgy Fed
// norms — the policy is documented in README "Consent policy"):
//
//   - #nobridge / #nobot in an actor's summary or tags blocks
//     materialization. The materializer scans on FIRST SIGHT (an opted-out
//     actor is never minted) and on every profile refresh; a previously
//     bridged actor who adds the marker gets their existing records
//     scrubbed and consent set to nobridge (reversible: removing the
//     marker restores bridging on the next refresh).
//   - Delete(Actor) tombstones the bridged repo: every record scrubbed,
//     consent set to deleted (terminal).
//
// This file wires the activity-side triggers: profile Updates (the "on
// profile Update" scan — RefreshActor/RefreshCommunity re-run the marker
// scan), Delete dispatch, and Undo{Delete} restores. The signature contract
// from task 05 is enforced here: an embedded actor document is trusted only
// when the verified signer IS that actor; anything else is re-fetched from
// its origin.

package ingest

import (
	"context"
	"fmt"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
)

// applyProfileUpdate handles Update{Person|Group}. The embedded document is
// passed to the materializer (which trusts it) ONLY when the verified
// signer is the updated actor itself; otherwise the update degrades to a
// forced re-fetch by IRI — same effect, no trust in the delivered body.
// Refresh* re-runs the consent-marker scan, so a profile that gained
// #nobridge is scrubbed and suppressed on this path.
//
// A BARE (non-announced) profile Update is refresh-only: it may re-materialize
// an actor/community the bridge ALREADY bridged, but must never mint or bridge
// a previously-unknown one. Otherwise any self-signed actor could drive an
// outbound fetch to an arbitrary target IRI (an SSRF / fetch-oracle) and burn
// the permanent-PLC mint budget, defeating the subscription-trust model.
// Announced profile updates ride the followed community's trust (handleAnnounce
// already authorized the announcer) and may ensure/refresh unknown actors.
func (h *Handler) applyProfileUpdate(ctx context.Context, actorDoc *ap.Object, signer, announcer string) error {
	if announcer == "" {
		known, err := h.isBridged(ctx, actorDoc.ID)
		if err != nil {
			return err
		}
		if !known {
			return skip(actorDoc.ID,
				"bare profile update for an actor we have not bridged (refresh-only, never mint)")
		}
	}
	ref := actorDoc
	if actorDoc.ID != signer {
		// Not self-signed: use a bare reference so the materializer
		// re-fetches the document from the actor's own origin.
		ref = &ap.Object{ID: actorDoc.ID}
	}
	var err error
	if actorDoc.Type == ap.TypeGroup {
		_, err = h.mat.RefreshCommunity(ctx, ref)
	} else {
		_, err = h.mat.RefreshActor(ctx, ref)
	}
	return err
}

// handleDelete processes Delete{object-or-actor}. announcer is the
// announcing community's AP id ("" when delivered bare).
//
// Authorization: a Delete announced by a followed community is trusted (the
// community moderates its own content — Lemmy only announces deletes for
// objects in its communities). A bare Delete must come from the deleted
// id's own authority (the actor deleting their content/account, or their
// instance acting for them). Anything else is dropped.
func (h *Handler) handleDelete(ctx context.Context, del *ap.Object, signer, announcer string) error {
	targetID := refID(del.Object)
	if targetID == "" {
		return errors.NewValidationError("delete", "delete carries no object id")
	}
	if err := h.authorizeDelete(ctx, del.ID, targetID, signer, announcer); err != nil {
		return err
	}

	// Record the tombstone marker BEFORE deleting: if this is a Delete for
	// an object we never materialized, the marker is the only thing
	// stopping a later (re-delivered, out-of-order) Create from
	// resurrecting it.
	if err := h.tombstones.Record(ctx, targetID); err != nil {
		return fmt.Errorf("ingest: record tombstone for %s: %w", targetID, err)
	}
	// HandleDelete branches actor vs object itself: a known actor id runs
	// the full Delete(Actor) scrub (records tombstoned, consent → deleted,
	// terminal); an object id deletes the record and soft-deletes its
	// mapping; an unknown id is a logged no-op.
	if err := h.mat.HandleDelete(ctx, targetID); err != nil {
		return err
	}
	return nil
}

// handleUndo processes Undo{Like|Dislike|Delete|Follow}.
func (h *Handler) handleUndo(ctx context.Context, undo *ap.Object, signer, announcer string) error {
	inner := undo.Object
	if inner == nil || inner.Type == "" {
		// A bare-IRI undo target is unactionable: we cannot know what kind
		// of activity is being undone without its body.
		return skip(undo.ID, "undo carries no inline activity")
	}
	switch inner.Type {
	case ap.TypeLike, ap.TypeDislike:
		return h.votes.RetractVote(ctx, inner, announcer)
	case ap.TypeDelete:
		return h.handleUndoDelete(ctx, undo, inner, signer, announcer)
	case ap.TypeFollow:
		// A remote undoing a follow of us — the bridge has no followers in
		// v1 (read-only), nothing to do.
		return skip(undo.ID, "undo of a follow is not applicable to the bridge")
	default:
		return skip(undo.ID, "unsupported undone activity type "+inner.Type)
	}
}

// handleUndoDelete restores an object whose Delete was previously applied:
// clear the create-after-delete marker, re-fetch the object from its origin
// (never trust the undo body), clear the mapping's soft delete, and
// re-materialize. Idempotent; a restore for content that is still gone
// upstream is a skip.
func (h *Handler) handleUndoDelete(ctx context.Context, undo, del *ap.Object, signer, announcer string) error {
	targetID := refID(del.Object)
	if targetID == "" {
		return skip(undo.ID, "undone delete carries no object id")
	}
	// Same authorization rule as the delete itself (a restore must not let a
	// cross-authority or co-hosted actor un-delete a victim's content).
	if err := h.authorizeDelete(ctx, undo.ID, targetID, signer, announcer); err != nil {
		return err
	}

	// The origin must actually serve the object again — a restore is only
	// as real as the content behind it.
	restored, err := h.fetchBound(ctx, targetID)
	if err != nil {
		return err
	}

	if err := h.tombstones.Remove(ctx, targetID); err != nil {
		return fmt.Errorf("ingest: clear tombstone for %s: %w", targetID, err)
	}
	if err := h.objects.Restore(ctx, targetID); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("ingest: restore mapping for %s: %w", targetID, err)
	}
	h.logger.Info("object restored upstream; re-materializing", "ap_id", targetID)
	if _, err = h.mat.HandleUpdate(ctx, restored); err != nil {
		// Compensation: the mapping's soft delete is already cleared and its
		// record was deleted from the repo. If re-materialization declines the
		// object (a skip — nobridge/deleted author/tombstoned ancestor — or a
		// validation error), leaving the mapping live would strand it WITHOUT a
		// record (downstream parent-lookup/echo/GetByAPID would treat it as
		// materialized). Roll back to the pre-undo state: re-soft-delete the
		// mapping and re-record the tombstone.
		if materialize.IsSkip(err) || errors.IsValidation(err) {
			if rerr := h.objects.SoftDelete(ctx, targetID); rerr != nil && !errors.IsNotFound(rerr) {
				return fmt.Errorf("ingest: re-soft-delete after declined restore of %s: %w", targetID, rerr)
			}
			if rerr := h.tombstones.Record(ctx, targetID); rerr != nil {
				return fmt.Errorf("ingest: re-record tombstone after declined restore of %s: %w", targetID, rerr)
			}
			h.logger.Info("restore re-materialization declined; rolled back to deleted state",
				"ap_id", targetID, "reason", err)
		}
		return err
	}
	return nil
}

// authorizeDelete enforces who may Delete (or Undo{Delete}) a target id.
//
//   - Bare (unannounced): only the target id's OWN authority may delete it —
//     the actor removing their content/account, or their instance acting for
//     them.
//   - Announced by a followed community: the target must live on the
//     announcing community's own authority (a community moderates only its own
//     instance's content). For a target that is itself a bridged ACTOR — the
//     terminal DeleteActor scrub — the community may delete only ITSELF, never
//     a co-hosted OTHER actor whose bridged presence spans other communities.
func (h *Handler) authorizeDelete(ctx context.Context, activityID, targetID, signer, announcer string) error {
	if announcer == "" {
		if !ap.SameAuthority(targetID, signer) {
			return skip(activityID, fmt.Sprintf(
				"delete of %s signed by cross-authority actor %s", targetID, signer))
		}
		return nil
	}
	if !ap.SameAuthority(targetID, announcer) {
		return skip(activityID, fmt.Sprintf(
			"announced delete of %s by cross-authority community %s", targetID, announcer))
	}
	if targetID != announcer {
		isActor, err := h.targetIsBridgedActor(ctx, targetID)
		if err != nil {
			return err
		}
		if isActor {
			return skip(activityID, fmt.Sprintf(
				"community %s may not delete co-hosted actor %s", announcer, targetID))
		}
	}
	return nil
}
