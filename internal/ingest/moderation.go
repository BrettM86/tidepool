package ingest

import (
	"context"
	"expvar"
	"fmt"
	"time"

	"tidepool/internal/ap"
	"tidepool/internal/echo"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// AuthorModerator removes every post one author has ACCEPTED in one community —
// a ban's `removeData: true`. *materialize.Materializer implements it.
//
// It is a SEPARATE interface, obtained by type assertion on the Materializer the
// dispatcher already drives, rather than a method on that interface: the purge
// needs the admissions ledger, which only the materializer holds, and widening
// the interface would oblige every caller that constructs a dispatcher for
// unrelated reasons to implement a moderation transition it never invokes.
type AuthorModerator interface {
	// RemoveAuthorPosts returns how many posts were removed. code is the
	// removal's own machine-readable reason (author-banned here, never
	// moderator-discretion: this content was not judged, its author was).
	RemoveAuthorPosts(ctx context.Context, communityDID, authorDID, code, reason string) (int, error)
}

// The ban counters. Each names a DECIDED non-action, and they are separate
// because the three refusals are different findings an operator has to tell
// apart — a ban we ignored on purpose, a ban aimed at a scope we do not model,
// and a ban for somebody who is not our user — where a single number would read
// as "bans are not arriving".
var (
	// BlockDirectIgnored counts Blocks delivered DIRECTLY to the banned user's
	// inbox rather than announced by the community. Lemmy sends both; only the
	// announced one can be authorized (see ignoreDirectBlock).
	BlockDirectIgnored = expvar.NewInt("tidepool_block_direct_ignored")
	// BlockUnscopedTarget counts announced Blocks whose `target` is not a
	// community this bridge follows — an instance-wide (Site actor) ban, which
	// this scope does not model.
	BlockUnscopedTarget = expvar.NewInt("tidepool_block_unscoped_target")
	// BlockForeignSubject counts announced Blocks naming somebody who is not one
	// of our personas. A Lemmy user banned from a Lemmy community is entirely
	// their instance's business; we hold no state that could apply it.
	BlockForeignSubject = expvar.NewInt("tidepool_block_foreign_subject")
	// BlockLapsedIgnored counts announced Blocks whose expiry had already passed
	// when they arrived — a description of a ban that is over, applied to
	// nothing.
	BlockLapsedIgnored = expvar.NewInt("tidepool_block_lapsed_ignored")
	// BlockExpiryUnreadable counts Blocks refused because their expiry could not
	// be parsed. It is the counter that says "a peer is sending us a duration we
	// do not understand" — which, unlike most parse failures, would otherwise
	// have become a permanent ban.
	BlockExpiryUnreadable = expvar.NewInt("tidepool_block_expiry_unreadable")
	// BlockUndoStaleRefused counts announced Undo{Block}s that were NOT applied
	// because the ban standing for that pair outlives the one they reverse — an
	// old unban replayed after the moderators banned the author again. It is its
	// own number because it is the one refusal on this path that leaves a user
	// EXCLUDED: if it ever moves for a community's genuine unbans, an author is
	// serving a ban nobody is enforcing on the far side.
	BlockUndoStaleRefused = expvar.NewInt("tidepool_block_undo_stale_refused")
)

// timeNow is the clock the ban path weighs an expiry against. A package
// variable so a test can hold time still; production never replaces it.
var timeNow = time.Now

// ignoreDirectBlock is the DECIDED non-action at the other door.
//
// Lemmy sends a ban twice: announced through the community, and delivered
// directly to the banned user's inbox. They are not redundant copies — they are
// signed by different actors, and only one of them can be authorized.
// BlockUser's actor is the MODERATOR's Person, so on this path decision 18's
// conjunction (the signer IS the community that owns the target) CANNOT pass by
// construction: no implementation turns a person into a group. The announced
// copy is the authoritative one, and we follow every bridged community
// (decision 15), so nothing is lost by refusing this one.
//
// It is COUNTED rather than dropped at debug because a ban that arrives only by
// the path we ignore looks exactly like a ban that never arrived — and the day
// the announced path breaks, this counter is the only thing that tells those
// apart.
//
// The refusal holds even when the activity CLAIMS actor = the Group: the inbox
// binds the VERIFIED signer and never the claim (SEC-1), so a Person-signed
// Block is a Person-signed Block whatever it says about itself.
func (h *Handler) ignoreDirectBlock(block *ap.Object, signer string) error {
	BlockDirectIgnored.Add(1)
	h.logger.Info("ignoring a directly delivered Block",
		"activity", block.ID, "signer", signer, "target", refID(block.Target))
	return skip(block.ID,
		"a directly delivered Block is signed by the moderator's Person, which can never be "+
			"the community that owns the ban: the community's own announced copy is the "+
			"authoritative one and is the only path that records it")
}

// handleBlock applies an announced Block — a community banning a native author —
// and, with banned=false, the Undo that lifts it.
//
// A BAN IS AN INTERSECTION: this author, in this community. Both halves are
// recorded, because both readers need one each — the admission gate holds a
// community DID, the delivery queue holds only an ordering key — and every
// consequence below is scoped by the pair. Cancelling by author alone would
// unpublish them in every community they write to; by community alone would take
// the whole community dark over one user.
//
// AUTHORIZATION is decision 18's conjunction, arriving in its simplest form. For
// content verbs the target's community is read off a mapping; a Block names the
// community DIRECTLY in `target`, so the two conjuncts — the signer IS the
// community, and the ban is FOR that community — collapse into one comparison
// against the verified announcer. A target that names a community we follow but
// is not the announcer is one moderator team excluding somebody from another's
// space; a target that names no community we follow is an instance-wide ban this
// scope does not model. They are different findings and get different reasons.
func (h *Handler) handleBlock(ctx context.Context, block *ap.Object, announcer *store.Community, banned bool) error {
	if announcer == nil {
		// Unreachable from the dispatch (a bare Block is taken by
		// ignoreDirectBlock before it can get here), and refused anyway: without
		// a verified community there is nothing to authorize against.
		return skip(block.ID, "a Block with no announcing community authorizes nothing")
	}
	target := refID(block.Target)
	if target == "" {
		return errors.NewValidationError("block", "block names no target community")
	}
	if target != announcer.APGroupID {
		return h.refuseBlockTarget(ctx, block, target, announcer)
	}

	subjectAPID := refID(block.Object)
	if subjectAPID == "" {
		return errors.NewValidationError("block", "block names no subject actor")
	}
	// WHOSE ban is this? The subject is an AP actor id, and the only ids we can
	// act on are our own personas — the classifier answers that by ENTITY
	// EXISTENCE against the serving surface and hands back the DID, which is
	// exactly what echo.Identity carries the DID for. Parsing the id's path
	// would answer the same question from the shape of a URL a peer chose.
	identity, err := h.classifier.Classify(ctx, &ap.Object{ID: subjectAPID})
	if err != nil {
		return fmt.Errorf("ingest: identify ban subject %s: %w", subjectAPID, err)
	}
	if identity.Class != echo.ClassLocalActor || identity.DID == "" {
		BlockForeignSubject.Add(1)
		return skip(block.ID,
			"announced Block names an actor that is not one of our personas: a Lemmy user's ban "+
				"from a Lemmy community is enforced entirely on their instance, and we hold no "+
				"state that could apply it")
	}

	if !banned {
		return h.liftBan(ctx, block, announcer, identity.DID)
	}
	return h.applyBan(ctx, block, announcer, identity.DID)
}

// refuseBlockTarget decides an announced Block whose target is not the
// announcing community, and says WHICH of the two it is.
//
// The distinction cannot be drawn from the URL: Lemmy's Site actor is the
// instance apex, a prefix of every id on that host, so any substring test is
// vacuous. It is drawn from state instead — is this target a community we
// follow? — which is the same question every other announced verb answers.
func (h *Handler) refuseBlockTarget(ctx context.Context, block *ap.Object, target string, announcer *store.Community) error {
	_, err := h.communities.GetByAPGroupID(ctx, target)
	if errors.IsNotFound(err) {
		// An instance-wide ban (target = the Site actor) or a community we do
		// not federate. Either way the scope is not one we model: recording it
		// against the announcing community would UNDERSTATE it — the author is
		// excluded from every community on that instance and we would enforce it
		// in one — and dropping it silently leaves them posting into that
		// instance collecting 403s until their deliveries poison, with nothing
		// naming the cause.
		BlockUnscopedTarget.Add(1)
		h.logger.Warn("announced Block targets a scope this bridge does not model",
			"activity", block.ID, "target", target, "announcer", announcer.APGroupID)
		return skip(block.ID,
			"announced Block targets "+target+", which is not a community this bridge follows: "+
				"an instance-wide ban is a scope Tidepool does not model, so it is recorded nowhere "+
				"and the author keeps posting into that instance")
	}
	if err != nil {
		return fmt.Errorf("ingest: resolve block target %s: %w", target, err)
	}
	return skip(block.ID, fmt.Sprintf(
		"announced Block targets community %s but was announced by %s: a ban is a community's "+
			"ruling about its OWN space", target, announcer.APGroupID))
}

// applyBan records the exclusion and acts on everything it implies.
func (h *Handler) applyBan(ctx context.Context, block *ap.Object, announcer *store.Community, subjectDID string) error {
	if h.bans == nil {
		// Loud and retryable, never a skip: a ban we cannot store is one that
		// stops nothing from the author's next post onward, and marking the
		// event processed would leave the community believing we honoured it.
		return fmt.Errorf("ingest: no community-ban store is wired, so this ban cannot be recorded")
	}
	// THE EXPIRY IS DECIDED BEFORE ANY CONSEQUENCE, because every consequence
	// below is irreversible: cancelled deliveries are never re-queued, and
	// removeData's removals are terminal by design (no Undo{Block} restores
	// content). A ban's duration therefore has to be settled while doing nothing
	// is still an option.
	expiry := block.BanExpiry()
	switch {
	case expiry == nil:
		// No expiry: a permanent ban, which is the common case.
	case !expiry.Valid:
		// PRESENT BUT UNREADABLE. The parser keeps this apart from absent
		// precisely so it can be refused: treating it as "no expiry" records a
		// permanent exclusion the moderator did not ask for, and nothing would
		// ever correct it — Lemmy sends no activity when a ban lapses, so there
		// is no later message whose arrival could say "that should have ended".
		// From every side it would read as an ordinary permanent ban.
		//
		// A validation error POISONS rather than retries: the bytes will not
		// re-parse, and the honest outcome is a visible failure saying we did not
		// apply this ban, not a queue that re-reads the same string forever.
		BlockExpiryUnreadable.Add(1)
		return errors.NewValidationError("expires",
			"block for "+subjectDID+" carries an expiry that cannot be parsed; refusing to "+
				"store it as a permanent ban")
	}
	// ALREADY OVER when it arrived — delayed in a queue, redelivered after an
	// outage, replayed from a backfill. The ROW is still written below UNLESS a
	// ban that IS in force stands for this pair: an account of a ban that ended
	// is faithful and idempotent, but written over a live exclusion it ENDS one
	// the moderators never lifted (Ban() holds that guard, beside the statement,
	// because it is the same predicate Standing() reads).
	//
	// What a lapsed ban must NOT do is ACT. Every consequence here is one no
	// later activity can undo — a cancelled delivery is never re-queued, and a
	// removeData removal is terminal by design — so applying them over an
	// exclusion that has already ended is unrecoverable damage done on behalf of
	// a decision that expired. The cancellation is gated inside Ban() (one place,
	// beside the statement); the purge is gated here.
	lapsed := expiry != nil && expiry.Valid && !expiry.After(timeNow())

	ban := store.CommunityBan{
		CommunityDID:  announcer.DID,
		SubjectDID:    subjectDID,
		CommunityAPID: announcer.APGroupID,
		// The moderator's own words, kept because the SAME action already writes
		// them into any removal record it produces: a blank here beside a quoted
		// reason there tells an operator no reason was given.
		Reason:     block.Summary,
		RemoveData: block.RemoveData != nil && *block.RemoveData,
	}
	if expiry != nil {
		expires := expiry.Time
		ban.ExpiresAt = &expires
	}

	// The row and the cancellation of already-queued work commit together (see
	// store.CommunityBans.Ban): the first stops the author's next post, the
	// second stops the ones the queue is holding.
	cancelled, err := h.bans.Ban(ctx, ban)
	if err != nil {
		return fmt.Errorf("ingest: record ban on %s in %s: %w", subjectDID, announcer.APGroupID, err)
	}
	h.logger.Info("community banned a native author",
		"community", announcer.APGroupID, "subject_did", subjectDID,
		"expires", ban.ExpiresAt, "remove_data", ban.RemoveData,
		"lapsed", lapsed, "cancelled_deliveries", cancelled, "activity", block.ID)

	if lapsed {
		BlockLapsedIgnored.Add(1)
		return skip(block.ID,
			"announced Block expired before it arrived: the ban is recorded as sent (unless a "+
				"ban that IS in force stands for this author here, which a lapsed replay may not "+
				"weaken), but it is not in force — nothing was cancelled and nothing was removed, "+
				"because both are irreversible and this exclusion is already over")
	}
	if !ban.RemoveData {
		return nil
	}
	if h.authorMod == nil {
		return fmt.Errorf(
			"ingest: no author-moderation surface is wired, so removeData for %s in %s cannot be applied",
			subjectDID, announcer.APGroupID)
	}
	// Their content in THIS community, from the ledger that knows which posts
	// this community admitted. Nothing is enqueued and nothing can be: the
	// removal rides ApplyOps, which takes no side effect — and Lemmy has already
	// removed this content, so an outbound Delete would be aimed at the
	// moderators who just acted.
	removed, err := h.authorMod.RemoveAuthorPosts(ctx, announcer.DID, subjectDID,
		materialize.RemovalCodeAuthorBanned, block.Summary)
	if err != nil {
		return fmt.Errorf("ingest: remove %s's posts from %s: %w", subjectDID, announcer.APGroupID, err)
	}
	h.logger.Info("ban carried removeData; the author's posts in this community were removed",
		"community", announcer.APGroupID, "subject_did", subjectDID, "removed", removed)
	return nil
}

// liftBan is Undo{Block}: the exclusion goes, and NOTHING ELSE does.
//
// UNLESS THE UNDO IS STALE. An Undo can reach us twice — redriven from the
// dead-letter queue, replayed from a backfill — under an activity id the inbox
// has never seen, and by then the ban it reverses may have been replaced by a
// stronger one. Deleting the row on the strength of an old unban leaves the
// author unbanned here for good: Lemmy sends its Block once, and sends nothing
// afterwards that would say the ban is still on. The expiry the undone Block
// names is the guard (see store.CommunityBans.Lift), and it is a partial one —
// an Undo of a PERMANENT ban carries nothing to compare.
//
// Content removed under removeData STAYS REMOVED. Lemmy models restoration as a
// separate restore_data flag, so republishing here would reverse a decision
// nobody reversed and push the author's posts back at the community that removed
// them — the same harm as reversing a removal on an author's edit, arriving by
// another door.
func (h *Handler) liftBan(ctx context.Context, block *ap.Object, announcer *store.Community, subjectDID string) error {
	if h.bans == nil {
		return fmt.Errorf("ingest: no community-ban store is wired, so this ban cannot be lifted")
	}
	// The expiry the UNDONE Block names, which is the only description of the
	// reversed ban an Undo carries — and therefore the only thing that can tell a
	// current unban from an old one redriven under a new activity id. An expiry
	// that is present but UNREADABLE is passed as absent rather than refused:
	// applyBan poisons on that shape because reading it wrong makes a permanent
	// ban nobody asked for, while here the worst case is the pre-existing
	// behaviour (an unconditional lift), and refusing would leave an author
	// excluded by a ban the moderators have already reversed.
	var undoneExpiry *time.Time
	if expiry := block.BanExpiry(); expiry != nil && expiry.Valid {
		when := expiry.Time
		undoneExpiry = &when
	}
	lifted, retained, err := h.bans.Lift(ctx, announcer.DID, subjectDID, undoneExpiry)
	if err != nil {
		return fmt.Errorf("ingest: lift ban on %s in %s: %w", subjectDID, announcer.APGroupID, err)
	}
	if retained {
		// A ban IS standing and it outlives the one this Undo reverses, so this
		// Undo is not about it: an old unban, redriven or replayed after the
		// moderators banned this author again. Lifting it would be irreversible
		// (Lemmy sends no second Block), so it is refused — LOUDLY, because the
		// other reading is that a community's genuine unban did not take effect,
		// and only an operator can tell those apart.
		BlockUndoStaleRefused.Add(1)
		h.logger.Warn("announced Undo{Block} reverses a ban that is no longer the one in force",
			"community", announcer.APGroupID, "subject_did", subjectDID,
			"undone_expiry", undoneExpiry, "activity", block.ID)
		return skip(block.ID, fmt.Sprintf(
			"announced Undo{Block} for %s in %s undoes a ban expiring %s, but the ban standing "+
				"there outlives it: the exclusion is KEPT, because an Undo replayed after a "+
				"re-ban would lift it permanently and Lemmy will send no second Block",
			subjectDID, announcer.APGroupID, undoneExpiry))
	}
	if !lifted {
		return skip(block.ID, fmt.Sprintf(
			"announced Undo{Block} for %s in %s, which held no standing ban: nothing to lift "+
				"(a re-delivered undo, or one for a ban that lapsed on its own)",
			subjectDID, announcer.APGroupID))
	}
	h.logger.Info("community lifted a native author's ban",
		"community", announcer.APGroupID, "subject_did", subjectDID, "activity", block.ID)
	return nil
}

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
