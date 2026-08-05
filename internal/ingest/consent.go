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
	"strings"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
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
// announcing community, already resolved by handleAnnounce (nil when the
// activity was delivered bare).
//
// Authorization: a Delete announced by a followed community is trusted for
// records that belong to that community — its own repo for posts, its
// thread root for comments (the community moderates the content posted into
// it, wherever the author lives). A bare Delete must come from the deleted
// id's own authority (the actor deleting their content/account, or their
// instance acting for them). An announced delete of an id the bridge has no
// mapping for is accepted too, but reaches only the tombstone marker below:
// nothing is removed because nothing was ever materialized, and the marker is
// scoped to the announcing community so it cannot suppress anyone else's
// content. Everything else drops.
func (h *Handler) handleDelete(ctx context.Context, del *ap.Object, signer string, announcer *store.Community) error {
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
	// resurrecting it. Scoped to the announcing community; a bare delivery
	// passed the same-authority origin check, so its marker is global.
	if err := h.tombstones.Record(ctx, targetID, announcerGroupID(announcer)); err != nil {
		return fmt.Errorf("ingest: record tombstone for %s: %w", targetID, err)
	}
	// Actor vs content is decided ONCE, by authorizeDelete, and the dispatch
	// here carries that decision into the materializer instead of letting it
	// re-derive one. An ANNOUNCED delete can only ever have been authorized
	// for CONTENT (every actor target — bridged_actors row or profile mapping
	// — is refused above), so it takes the content-only entry: HandleDelete
	// would re-read bridged_actors, and an actor being minted right now has
	// neither row when authorizeDelete looks and an actor row by the time the
	// materializer looks, which would turn an unrelated community's announced
	// delete into that actor's terminal scrub. A BARE delete keeps the full
	// dispatch — Delete(Actor) arrives on exactly that path, and an origin may
	// delete its own actor.
	deleteTarget := h.mat.HandleDelete
	if announcer != nil {
		deleteTarget = h.mat.HandleDeleteRecord
	}
	if err := deleteTarget(ctx, targetID); err != nil {
		return err
	}
	return nil
}

// announcerGroupID is the announcing community's AP group id, or "" for a
// bare delivery. As a Tombstones scope "" means global/origin-authorized —
// correct for a bare delivery, which reached here only by passing the
// same-authority check against the target id's own host.
func announcerGroupID(announcer *store.Community) string {
	if announcer == nil {
		return ""
	}
	return announcer.APGroupID
}

// handleUndo processes Undo{Like|Dislike|Delete|Follow}. announcer is the
// announcing community (nil when delivered bare).
func (h *Handler) handleUndo(ctx context.Context, undo *ap.Object, signer string, announcer *store.Community) error {
	inner := undo.Object
	if inner == nil || inner.Type == "" {
		// A bare-IRI undo target is unactionable: we cannot know what kind
		// of activity is being undone without its body.
		return skip(undo.ID, "undo carries no inline activity")
	}
	announcerID := announcerGroupID(announcer)
	switch inner.Type {
	case ap.TypeLike, ap.TypeDislike:
		// The inbox binds only the OUTER Undo's actor to the signature; the
		// inner vote's actor is unverified. A bare undo may therefore only
		// retract votes attributed to the signer's own instance — otherwise
		// any signer could retract other instances' users' votes. Announced
		// undos ride the announcing community's vouching, exactly like
		// announced votes (FEP-1b12 group fan-out).
		if announcer == nil {
			if err := h.authorizeBareVote(undo.ID, inner, signer); err != nil {
				return err
			}
		}
		return h.votes.RetractVote(ctx, inner, announcerID)
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

// handleUndoDelete undoes a Delete the bridge previously applied. What that
// means depends entirely on what the delete left behind, and the two cases
// are NOT variations of one flow:
//
//   - UNMAPPED (a marker and nothing else — the delete-before-create race).
//     Nothing was ever materialized, so there is nothing to restore and
//     NOTHING is fetched: the undo retracts the marker its own authorization
//     context laid and stops (retractDeleteMarker). Fetching here would make
//     Undo{Delete{<any IRI>}} an arbitrary-IRI fetch oracle and PLC mint for
//     anyone who can lay a marker first — and laying one is cheap by design:
//     authorizeDelete deliberately admits an UNMAPPED announced target (that
//     allowance is what closes the race at all), and a bare Delete of an id on
//     the signer's own host is always authorized. Two activities would
//     otherwise buy a mint, bypassing materializeContent's echo, tombstone,
//     and community-binding checks entirely.
//   - MAPPED. The id IS ours to restore: clear the marker and the mapping's
//     soft delete and re-materialize from the object the origin serves again.
//     That re-fetch is the authorization ("a restore is only as real as the
//     content behind it") AND the record body, so it is pinned to the target's
//     own authority, and what comes back must still be the KIND of thing the
//     mapping says it is.
//
// Idempotent throughout; a restore for content that is still gone upstream is
// a skip.
//
// Deliberate, operator-visible policy: a BARE restore of mapped content
// overrides a community's moderation delete of that content. It is bounded by
// the same-authority signer, an existing mapping, a pinned re-fetch and the
// type check — i.e. an origin re-serving content it previously bridged — and
// the origin re-serving an object is the strongest statement anyone makes
// about it, which is what the bridge mirrors.
func (h *Handler) handleUndoDelete(ctx context.Context, undo, del *ap.Object, signer string, announcer *store.Community) error {
	targetID := refID(del.Object)
	if targetID == "" {
		return skip(undo.ID, "undone delete carries no object id")
	}
	// Who may restore is who may delete: same rule, so an unrelated instance
	// or a community the target does not belong to cannot un-delete a
	// victim's content.
	if err := h.authorizeDelete(ctx, undo.ID, targetID, signer, announcer); err != nil {
		return err
	}
	scope := announcerGroupID(announcer)

	mapping, err := h.objects.GetByAPID(ctx, targetID)
	if errors.IsNotFound(err) {
		return h.retractDeleteMarker(ctx, undo.ID, targetID, scope)
	}
	if err != nil {
		return fmt.Errorf("ingest: look up mapping for restore of %s: %w", targetID, err)
	}

	// Pinned to the target's own authority: this fetch's answer is what
	// authorizes the restore AND what gets written into the repo, so an open
	// redirect on the origin must fail it rather than both license the restore
	// and choose its content (the delete sweep's fetch is pinned for the first
	// half of that reason alone). resolveDelivered's ordinary re-fetch stays
	// permissive on purpose: there the origin's answer is content only, and the
	// self-asserted-id binding already contains it.
	restored, err := h.fetchBoundSameAuthority(ctx, targetID)
	if err != nil {
		return err
	}
	// The re-materialization below is HandleUpdate, which dispatches on the
	// FETCHED type, not on the mapping: an id now serving a Person where a post
	// used to live would mint/refresh an ACTOR off a content restore, and a Page
	// where a comment lived would write into a community repo. Only a type
	// consistent with the mapping survives.
	if !restoredTypeMatches(mapping.Collection, restored.Type) {
		return skip(targetID, fmt.Sprintf(
			"restored object is a %q but %s is mapped as %s", restored.Type, targetID, mapping.Collection))
	}
	// An announced restore is bound to the announcing community exactly like
	// announced content (materializeContent's guard) and one notch tighter: the
	// restored body must NAME a community, and it must be the announcer's. The
	// materializer derives the target community from the object's own audience
	// and EnsureCommunity()s it, so letting an EMPTY audience pass would let a
	// community vouch for a restore into whatever the object turns out to name
	// — that vacuous pass is what let a sibling community revive another
	// community's soft-deleted comment. Real Lemmy bodies always carry audience,
	// so requiring it costs nothing.
	if announcer != nil {
		if objCommunity := communityIRIFrom(restored); objCommunity != announcer.APGroupID {
			return skip(targetID, fmt.Sprintf(
				"restored object names community %q but was announced by %s",
				objCommunity, announcer.APGroupID))
		}
	}

	if err := h.tombstones.Remove(ctx, targetID, scope); err != nil {
		return fmt.Errorf("ingest: clear tombstone for %s: %w", targetID, err)
	}
	if err := h.objects.Restore(ctx, targetID); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("ingest: restore mapping for %s: %w", targetID, err)
	}
	h.logger.Info("object restored upstream; re-materializing", "ap_id", targetID)
	if _, err = h.mat.HandleUpdate(ctx, restored); err != nil {
		// Compensation, for EVERY error class. The mapping's soft delete is
		// already cleared and its record was deleted from the repo, so leaving
		// the mapping live strands it WITHOUT a record (downstream
		// parent-lookup/echo/GetByAPID would read it as materialized). A skip or
		// validation error strands it immediately; a RETRYABLE failure strands it
		// just as permanently once the attempt cap poisons the event, which is
		// why the rollback is unconditional. The original error is returned
		// unchanged so retryable still retries — and the retry re-runs this whole
		// undo, which is idempotent (Remove/Restore again).
		if rerr := h.objects.SoftDelete(ctx, targetID); rerr != nil && !errors.IsNotFound(rerr) {
			return fmt.Errorf("ingest: re-soft-delete after failed restore of %s: %w", targetID, rerr)
		}
		// Same scope the delete would have used: the marker this authorization
		// context is entitled to lay. A bare undo that cleared other communities'
		// markers does not re-create them — it got here on the target id's own
		// authority, which outranks their claim regardless of how this ends.
		if rerr := h.tombstones.Record(ctx, targetID, scope); rerr != nil {
			return fmt.Errorf("ingest: re-record tombstone after failed restore of %s: %w", targetID, rerr)
		}
		h.logger.Info("restore re-materialization failed; rolled back to deleted state",
			"ap_id", targetID, "reason", err)
		return err
	}
	return nil
}

// restoredTypeMatches reports whether a re-fetched AP object is the kind of
// thing a mapping in the given collection was made from. Actor/community
// profiles map to no AP content type at all, so they never match: a restore
// is a CONTENT path, and un-deleting an actor is the consent machinery's job
// (Delete(Actor) is terminal by design).
func restoredTypeMatches(collection, apType string) bool {
	switch collection {
	case materialize.CollectionPost:
		return apType == ap.TypePage || apType == ap.TypeArticle
	case materialize.CollectionComment:
		return apType == ap.TypeNote
	default:
		return false
	}
}

// retractDeleteMarker is Undo{Delete} for an id with no mapping: the delete
// left a tombstone marker and nothing else, so the undo retracts that marker
// and reports a skip — no fetch, no materialization (handleUndoDelete says
// why). Prior state is still required: the marker must be one this
// authorization context can SEE (global, or its own), so a community cannot
// bootstrap off a marker another community laid. What it CLEARS is narrower
// still — an announced undo removes only that community's own row, leaving a
// global origin-authorized marker standing; a bare undo carries the target
// id's own authority and clears the id's markers outright.
func (h *Handler) retractDeleteMarker(ctx context.Context, activityID, targetID, scope string) error {
	tombstoned, err := h.tombstones.ExistsFor(ctx, targetID, scope)
	if err != nil {
		return fmt.Errorf("ingest: tombstone check for %s: %w", targetID, err)
	}
	if !tombstoned {
		return skip(activityID, "restore for an id the bridge never deleted: "+targetID)
	}
	if err := h.tombstones.Remove(ctx, targetID, scope); err != nil {
		return fmt.Errorf("ingest: clear tombstone for %s: %w", targetID, err)
	}
	h.logger.Info("delete marker retracted upstream; nothing was ever materialized",
		"ap_id", targetID, "scope", scope)
	return skip(activityID, "restore of an id that was never materialized: "+targetID+
		" (marker retracted; a fresh Create is what re-materializes it)")
}

// authorizeBareVote enforces who may cast (or retract) a BARE, un-announced
// vote: the vote's actor must live on the verified signer's authority — host
// granularity, the same instance-is-the-trust-unit rule as bare Delete (an
// instance may speak for its own users, never for another instance's).
// Mismatches (including an actorless vote) drop as processed skips, never
// retryable errors — a retry would wedge the ordering key over a vote.
func (h *Handler) authorizeBareVote(activityID string, vote *ap.Object, signer string) error {
	if actor := refID(vote.Actor); !ap.SameAuthority(actor, signer) {
		return skip(activityID, fmt.Sprintf(
			"bare vote attributed to cross-authority actor %q signed by %s", actor, signer))
	}
	return nil
}

// authorizeDelete enforces who may Delete (or Undo{Delete}) a target id.
// announcer is the announcing community (nil for a bare delivery).
//
//   - Bare (unannounced): only the target id's OWN authority may delete it —
//     the actor removing their content/account, or their instance acting for
//     them.
//   - Announced by a followed community: the community moderates the content
//     posted INTO it, wherever that content is hosted — a jlai.lu author's
//     post in a lemmy.world community carries a jlai.lu ap_id, and its
//     Delete fans out through the community's Announce (the normal remote-
//     author federation shape). MEMBERSHIP in the announcing community, not
//     the target's host, is therefore the test: posts commit into the
//     community's own repo (mapping.DID answers directly), comments into
//     their AUTHOR's repo (their thread root answers for them — see
//     authorizeAnnouncedCommentDelete). A target belonging elsewhere
//     (another community's content, even co-hosted on the announcer's
//     instance) drops. A target that is itself a bridged ACTOR — the
//     terminal DeleteActor scrub — may only be the community ITSELF, never a
//     person or another community; both a bridged_actors row and the profile
//     mapping refuse it, because a bridged actor always has a profile
//     mapping (EnsureActor commits rkey "self") but the actor ROW lands
//     first, and only checking the mapping would leave a mid-mint actor
//     deletable (HandleDelete keys its terminal scrub off that same row). An
//     UNMAPPED target is accepted so handleDelete's tombstone marker still
//     closes the delete-before-create window (a Delete can be processed
//     while its Create is mid-materialization). That allowance is contained
//     structurally, not by trust: the marker it lays is SCOPED to the
//     announcing community (migration 015), so it suppresses that id only
//     where that community's word counts and can no longer pre-suppress
//     another community's content for the retention window. And it reaches no
//     further: an Undo of such a delete never fetches or materializes the
//     unmapped target — it only retracts that same marker (handleUndoDelete).
func (h *Handler) authorizeDelete(ctx context.Context, activityID, targetID, signer string, announcer *store.Community) error {
	if announcer == nil {
		if !ap.SameAuthority(targetID, signer) {
			return skip(activityID, fmt.Sprintf(
				"delete of %s signed by cross-authority actor %s", targetID, signer))
		}
		return nil
	}
	if targetID == announcer.APGroupID {
		return nil
	}
	if _, err := h.actors.GetByAPActorID(ctx, targetID); err == nil {
		return skip(activityID, fmt.Sprintf(
			"community %s may not delete actor %s", announcer.APGroupID, targetID))
	} else if !errors.IsNotFound(err) {
		return fmt.Errorf("ingest: classify delete target %s: %w", targetID, err)
	}
	mapping, err := h.objects.GetByAPID(ctx, targetID)
	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("ingest: classify delete target %s: %w", targetID, err)
	}
	switch mapping.Collection {
	case materialize.CollectionActorProfile, materialize.CollectionCommunityProfile:
		// Belt-and-braces behind the actor-row check above: a profile mapping
		// whose actor row was scrubbed is still an actor, not content.
		return skip(activityID, fmt.Sprintf(
			"community %s may not delete actor %s", announcer.APGroupID, targetID))
	case materialize.CollectionComment:
		return h.authorizeAnnouncedCommentDelete(ctx, activityID, mapping, announcer)
	}
	if mapping.DID != announcer.DID {
		return skip(activityID, fmt.Sprintf(
			"announced delete of %s targets a record outside %s's repo", targetID, announcer.APGroupID))
	}
	return nil
}

// authorizeAnnouncedCommentDelete answers community membership for a
// COMMENT, which its mapping cannot: comments commit into their AUTHOR's
// repo (only posts land in the community's), so mapping.DID is the author's
// DID for every comment in every community — comparing it to the announcer's
// repo would drop every announced comment delete there is. The thread is the
// membership signal instead: the materializer guarantees reply.root on every
// comment and the thread's root post lives in the community's own repo, so
// one record read recovers the owning community's DID (the same derivation
// the vote aggregator uses to bind announced votes).
func (h *Handler) authorizeAnnouncedCommentDelete(ctx context.Context, activityID string, mapping *store.APObjectMapping, announcer *store.Community) error {
	if mapping.IsDeleted() {
		// Already soft-deleted: the record — and with it the reply.root this
		// check reads — is gone from the repo, so there is nothing left to
		// authorize against, and re-deleting is a downstream no-op. The
		// allowance is exactly that and no more: idempotence for re-delivered
		// deletes. The RESTORE path rides the same authorizeDelete call and is
		// therefore admitted here too, but it is not authorized here — its
		// guarantees are post-fetch (handleUndoDelete): the origin must serve
		// the object again over a pinned fetch, its type must match the
		// mapping, and an announced restore must NAME the announcing community.
		// Those are what stop a sibling community from reviving another
		// community's soft-deleted comment.
		return nil
	}
	record, _, err := h.records.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	if errors.IsNotFound(err) {
		// A live mapping with no record is a permanent inconsistency: a retry
		// would re-read the same missing record forever and wedge the
		// ordering key behind it. Log it and drop the delete.
		h.logger.Warn("announced comment delete: live mapping has no record",
			"ap_id", mapping.APID, "at_uri", mapping.ATURI)
		return skip(activityID, "comment "+mapping.ATURI+" has no record to authorize against")
	}
	if err != nil {
		return fmt.Errorf("ingest: read comment record %s: %w", mapping.ATURI, err)
	}
	rootDID := replyRootDID(record)
	if rootDID == "" {
		h.logger.Warn("announced comment delete: record carries no reply.root",
			"ap_id", mapping.APID, "at_uri", mapping.ATURI)
		return skip(activityID, "comment "+mapping.ATURI+" carries no reply.root to authorize against")
	}
	if rootDID != announcer.DID {
		return skip(activityID, fmt.Sprintf(
			"announced delete of %s targets a comment in a thread outside %s",
			mapping.APID, announcer.APGroupID))
	}
	return nil
}

// replyRootDID extracts the repo DID from a comment record's reply.root
// strongRef uri (at://did/collection/rkey). Malformed records yield "".
func replyRootDID(record map[string]any) string {
	reply, ok := record["reply"].(map[string]any)
	if !ok {
		return ""
	}
	root, ok := reply["root"].(map[string]any)
	if !ok {
		return ""
	}
	uri, _ := root["uri"].(string)
	rest, ok := strings.CutPrefix(uri, "at://")
	if !ok {
		return ""
	}
	did, _, _ := strings.Cut(rest, "/")
	return did
}
