package consume

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"

	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// The native-comment path: a social.coves.community.comment record in a Coves
// user's own repo that belongs to a thread the bridge federates.
//
// The DELETE case is what outbound_objects exists for. A Jetstream delete
// commit carries the repo DID, the collection and the rkey and NOTHING else:
// no record body, no CID, no reply refs. Every fact the Delete activity needs
// — the AP id, the community it is addressed to, the parent it hangs under —
// has to be read back out of state written at create time.

// maxCommentDepth is Lemmy's comment nesting limit (0.19.20). A comment AT the
// cap still federates; one below it never can, so it is made visible in the
// DLQ rather than dropped at debug.
const maxCommentDepth = 50

// handleComment applies one comment commit. tx is the rev-gate's transaction:
// the outbound_objects write rides it, so the state write, the gate advance
// and the enqueue commit together — a failed enqueue rolls all three back,
// which is what stops a committed row under an unadvanced gate from letting a
// replay bump the seq into a SECOND activity id for one comment.
func (d *Dispatcher) handleComment(ctx context.Context, tx *sql.Tx, did string, commit *CommitEvent) error {
	switch commit.Operation {
	case operationCreate, operationUpdate:
		return d.applyCommentWrite(ctx, tx, did, commit)
	case operationDelete:
		return d.applyCommentDelete(ctx, tx, did, commit)
	default:
		// Unreachable: validateCommitEnvelope rejects any other operation as
		// permanent before the gate. A claimed skip, since an operation this
		// build cannot name is not something a replay improves.
		d.logger.Debug("unknown comment operation",
			slog.String("operation", commit.Operation), slog.String("did", did))
		return nil
	}
}

// applyCommentWrite handles a create or an update — the two operations that
// push content OUTWARD.
//
// The ORDER of the steps is the design, and each one is a gate on the next:
//
//  1. the opt-out gate, so a user who said no never gets an AP identity
//     created for them and never has new content sent on their behalf;
//  2. the thread resolution, so a comment in a NATIVE community — which this
//     bridge has no business federating — is skipped before anything is
//     written or minted;
//  3. the depth cap, before any state or identity exists for a comment Lemmy
//     will never accept;
//  4. the lazy mint, THROUGH handle verification, because the local part it
//     derives is frozen at creation;
//  5. the outbound state, then the intent — state first, because the intent's
//     activity id comes from the seq the write bumps, and a delete one day has
//     nothing else to be built from.
func (d *Dispatcher) applyCommentWrite(ctx context.Context, tx *sql.Tx, did string, commit *CommitEvent) error {
	federating, err := d.mayFederate(ctx, did)
	if err != nil {
		return err
	}
	if !federating {
		// The residual split-thread case, explicitly chosen (decision 11): the
		// comment stays on the atproto side and the Lemmy side never sees it.
		//
		// PERMANENT, and it CLAIMS the gate: the author's "no" is not a fact
		// that changes underneath this frame, and a later re-enable federates
		// what they write next rather than what they wrote while opted out.
		d.logger.Debug("skipping comment from an opted-out author",
			slog.String("did", did), slog.String("rkey", commit.RKey),
			slog.String("operation", commit.Operation))
		return nil
	}

	atURI := commitRecordURI(did, commit)
	thread, err := d.commentThread(ctx, atURI, commit)
	if err != nil {
		return err
	}
	if thread == nil {
		// Most native comments live in native communities, and most updates
		// with no prior state are edits to a comment that never federated.
		// Dead-lettering either would bury the queue in events working exactly
		// as intended.
		//
		// UNCLAIMED. A reply can reach this consumer before the thing it replies
		// to has been materialized — the two arrive milliseconds apart and
		// nothing orders them — and a claimed gate row would swallow the
		// reconnect rewind that recovers it, so the comment would silently never
		// federate. The update case rides the same ruling for the same reason:
		// an edit resolves through the state its CREATE wrote, so it becomes
		// resolvable exactly when the create is recovered.
		return fmt.Errorf("%w: comment %s belongs to no thread this bridge federates (yet)",
			errSkipUnclaimed, atURI)
	}

	if thread.Depth > maxCommentDepth {
		// Named "depth" on purpose: the connector stores err.Error() as the
		// dead letter's last_error, and that string is all an operator
		// triaging the queue has to go on.
		return fmt.Errorf("%w: comment %s is at depth %d, beyond Lemmy's cap of %d",
			ErrPermanentEvent, atURI, thread.Depth, maxCommentDepth)
	}

	if err := d.refuseInLockedThread(ctx, atURI, thread); err != nil {
		return err
	}

	if err := d.refuseBannedAuthor(ctx, did, thread.CommunityDID, "comment "+atURI); err != nil {
		return err
	}

	if err := d.ensureActor(ctx, did); err != nil {
		return err
	}

	snapshot, err := commentSnapshot(atURI, commit, thread)
	if err != nil {
		return err
	}
	stored, err := d.objects.UpsertTx(ctx, tx, store.OutboundObject{
		ATURI:      atURI,
		APObjectID: d.apObjectID(did, commit),
		LastCID:    commit.CID,
		LastRev:    commit.Rev,
		// The community and the depth come from the THREAD, never from
		// anything the record asserts about itself — and on an update they
		// come from the state the create left, because an edit never moves a
		// comment between communities or up the thread.
		CommunityDID:       thread.CommunityDID,
		CommunityAPID:      thread.CommunityAPID,
		TranslatedSnapshot: snapshot,
		Depth:              thread.Depth,
	})
	if err != nil {
		return fmt.Errorf("write outbound state for %s: %w", atURI, err)
	}

	return d.enqueueComment(ctx, tx, did, commit.Operation, stored, thread.ParentATURI, thread.ParentAPID)
}

// refuseBannedAuthor refuses anything a BANNED author writes into the community
// that banned them (task 17c-3 review).
//
// The admission gate covers POSTS only — it lives in the acceptance engine,
// which never sees a comment or a vote — so without this a banned author's
// replies and votes keep enqueueing on that community's ordering key. Lemmy
// refuses a banned actor's activity, so each one fails, retries and poisons with
// the cause three tables away: the same harm refuseInLockedThread exists to
// prevent, over a cause that is even better known here, because WE recorded it.
//
// The ban's own arrival already swept this author's queued comments and votes —
// its cancellation joins on actor_did and ordering_key, not on activity kind —
// so only NEW writes escaped, which is exactly what this closes.
//
// PERMANENT, like the lock refusal and for the same reasons: the event
// dead-letters with a reason an operator can read, the cursor advances rather
// than blocking every other native user behind one banned account, and only a
// moderator lifting the ban (or its expiry) changes the answer.
//
// The `what` argument is the short label the DLQ shows ("comment at://…", "vote
// at://…"): both paths share this refusal, and last_error is the only place the
// queue says which kind of write was refused.
func (d *Dispatcher) refuseBannedAuthor(ctx context.Context, did, communityDID, what string) error {
	if communityDID == "" {
		// Nothing resolved a community, so there is no ban to ask about: the
		// caller has already decided this write goes nowhere.
		return nil
	}
	banned, err := d.bans.Standing(ctx, communityDID, did)
	if err != nil {
		return fmt.Errorf("read ban on %s in %s: %w", did, communityDID, err)
	}
	if !banned {
		return nil
	}
	return fmt.Errorf("%w: author-banned: %s is by an author %s has banned",
		ErrPermanentEvent, what, communityDID)
}

// refuseInLockedThread refuses a comment in a thread a community has LOCKED
// (task 17c-2). It is the read half of the lock: recording one and then
// federating a reply under it is worse than not recording it at all, because
// Lemmy rejects comments on locked posts server-side — the reply buys a failed
// delivery, a retry loop and finally a poisoned row whose cause is a moderator
// decision three tables away.
//
// It asks about the PARENT AND THE THREAD ROOT, because Lemmy locks threads and
// a check on the parent alone stops only direct replies: anyone can keep talking
// by hitting reply one level down, which is not an edge case but the ordinary
// shape of a conversation. Both are asked in ONE statement, and the scope stays
// per-object — a lock on one post says nothing about the community's other
// threads.
//
// The refusal is PERMANENT, and that is the whole design:
//
//   - it DEAD-LETTERS and the cursor moves on. A transient error would block
//     every other native user's traffic behind one locked thread, retrying a
//     decision only a moderator can change.
//   - it carries the REASON, "parent-locked" — the same code the admissions
//     ledger already spells for a post refused under a locked parent, so an
//     operator meets one vocabulary rather than two. The connector stores
//     err.Error() as the dead letter's last_error, and that string is the only
//     surface anyone triaging the queue — or answering the author asking where
//     their comment went — has to go on.
//   - it is a GATE, not a verdict on the record. It runs before the mint and
//     before any outbound state, and it leaves the rev gate un-advanced (the
//     gate transaction rolls back on any error), so the IDENTICAL comment —
//     same rkey, same rev, same cid — is admitted once the lock is lifted. A
//     refusal that advanced the gate would swallow that retry as a stale replay
//     and lose the comment for good.
//
// A store failure PROPAGATES as an ordinary (retryable) error: "we could not
// read the lock" must never be answered with "there is no lock", which is
// exactly the reply the lock exists to stop.
func (d *Dispatcher) refuseInLockedThread(ctx context.Context, atURI string, thread *resolvedThread) error {
	locked, err := d.moderation.LockedAmong(ctx, thread.ParentATURI, thread.RootATURI)
	if err != nil {
		return fmt.Errorf("read lock state of the thread above %s: %w", atURI, err)
	}
	if locked != "" {
		return fmt.Errorf("%w: parent-locked: comment %s is in a thread its community locked (%s)",
			ErrPermanentEvent, atURI, locked)
	}
	if thread.RootATURI != "" {
		return nil
	}

	// The thread could not be established (walkThreadRoot dead-ended on state
	// this consumer never wrote — every comment snapshot it has ever written
	// names its parent, and a post is at depth 0). "We do not know which thread
	// this is" must not be answered with "the thread is not locked" — that is
	// the same fail-open the empty root exists to prevent — but neither may it
	// strand replies forever under content nobody has moderated. So the question
	// narrows to the only one that can still matter: does this community hold
	// ANY lock the unknown root might be? If it holds none, there is provably
	// nothing to miss.
	//
	// This is NOT a community-scoped refusal, and it is not reachable from
	// fediverse content: a mapped subject's thread is answered from state the
	// materializer recorded, so it resolves or it is a thread root, never an
	// empty answer. A read that could FAIL here would turn one locked post into
	// a community-wide park of ordinary replies, which is why there is no read
	// on that path at all.
	held, err := d.moderation.CommunityHoldsAnyLock(ctx, thread.CommunityDID)
	if err != nil {
		return fmt.Errorf("read standing locks of %s: %w", thread.CommunityDID, err)
	}
	if !held {
		return nil
	}
	// RETRYABLE, deliberately — and the recovery is not instant, so it is worth
	// stating plainly: this comment first burns its in-line attempts, blocking
	// the consumer behind it for that schedule, and then DEAD-LETTERS. What
	// makes that acceptable rather than a loss is the redrive window: the
	// dead-lettered frame keeps this message, and a redrive after the lock is
	// lifted (or the row repaired) re-runs it against an answerable question.
	// Nothing about the comment is wrong, so it must not be discarded as
	// permanent; nothing about the state improves on its own, so it must not be
	// retried as though it would.
	return fmt.Errorf(
		"cannot tell whether comment %s is in a locked thread: the chain above it cannot be followed past %s, "+
			"and %s holds standing locks — redrive this once the lock is lifted or the row is repaired",
		atURI, thread.RootUnresolved, thread.CommunityDID)
}

// applyCommentDelete withdraws a comment, using ONLY state.
//
// The opt-out gate is deliberately absent. A delete only ever REMOVES content,
// so it is always safe to send, and it is the only way a user who has opted
// out can retract what is already on the fediverse — blocking it would leave
// the peer's copy standing forever, the exact opposite of what asking to stop
// federating means.
func (d *Dispatcher) applyCommentDelete(ctx context.Context, tx *sql.Tx, did string, commit *CommitEvent) error {
	atURI := commitRecordURI(did, commit)

	// Tombstone returns the state the Delete is built from in the same
	// statement that stamps it, and is idempotent: a redelivered delete
	// preserves the original tombstone time AND seq, so it reuses the activity
	// id the first one sent and the peer recognises it as the same activity.
	// It rides the gate transaction so the tombstone and the enqueue commit
	// together.
	dead, err := d.objects.TombstoneTx(ctx, tx, atURI)
	if errors.IsNotFound(err) {
		// A comment this bridge never federated. There is nothing to withdraw,
		// and most native comment deletes are exactly this.
		//
		// PERMANENT, and the claim is LOAD-BEARING: the gate row a delete leaves
		// IS the tombstone that rejects a stale create for the same URI, so
		// releasing it would let a replay past the delete resurrect content the
		// user removed.
		d.logger.Debug("skipping delete for a comment with no outbound state",
			slog.String("did", did), slog.String("rkey", commit.RKey))
		return nil
	}
	if err != nil {
		return fmt.Errorf("tombstone outbound state for %s: %w", atURI, err)
	}

	parent := d.parentFromSnapshot(dead.TranslatedSnapshot)
	return d.enqueueComment(ctx, tx, did, operationDelete, dead, parent.ATURI, parent.APID)
}

// enqueueComment hands one intent to task 15. The activity id comes from the
// seq the write just produced, so every applied operation gets its own stable
// id and a redelivery reuses it.
func (d *Dispatcher) enqueueComment(ctx context.Context, tx *sql.Tx, did, operation string, stored *store.OutboundObject, parentATURI, parentAPID string) error {
	intent := CommentIntent{
		Op:            operation,
		ATURI:         stored.ATURI,
		ID:            ActivityID(d.userOrigin, stored.ATURI, operation, stored.LastActivitySeq),
		CommunityAPID: stored.CommunityAPID,
		// Read off the stored outbound row rather than re-resolved: it is the
		// same community this comment was admitted into, and the mapping the
		// enqueuer writes needs it to be moderatable.
		CommunityDID: stored.CommunityDID,
		ParentAPID:   parentAPID,
		Snapshot:     stored.TranslatedSnapshot,
	}
	// parentATURI carries the causal dependency (decision 15): delivery must
	// not present a reply to a peer before the thing it replies to. On a
	// delete it comes from state, because the frame carries no reply refs. The
	// enqueue rides tx so it commits with the gate advance.
	if err := d.enqueuer.EnqueueActivity(ctx, tx, did, did, parentATURI, intent); err != nil {
		return fmt.Errorf("enqueue comment intent for %s: %w", stored.ATURI, err)
	}
	return nil
}

// resolvedThread is everything a comment needs from the thing it replies to.
// All of it comes from state the bridge already holds, never from the record.
type resolvedThread struct {
	ParentATURI   string
	ParentAPID    string
	CommunityDID  string
	CommunityAPID string
	// Depth is THIS comment's depth: the parent's recorded depth plus one.
	Depth int
	// RootATURI is the at-uri of the thread this comment hangs in — the post at
	// the top of it, whoever the immediate parent is. A community locks a
	// THREAD, so this is what the lock check asks about; the parent alone would
	// stop only direct replies, and the reply button under every existing
	// comment is the normal way a conversation continues.
	//
	// It is derived from the bridge's own state (the parent's recorded root, or
	// the parent itself when the parent IS a root), never from the record's
	// reply.root, which the author writes and could point anywhere.
	RootATURI string
	// RootUnresolved names the object the thread resolution stopped at, and why,
	// when RootATURI could not be established. It is DIAGNOSTIC ONLY — never
	// written to the snapshot, never inherited — and exists so the one error that
	// holds a comment for an undeterminable thread names something an operator
	// can open, instead of a thread nobody can look up.
	RootUnresolved *unresolvedThread
}

// commentThread resolves the thread context for a create or an update.
//
// An UPDATE reads it back from the state its create wrote rather than
// re-resolving: the record could name a different parent, and an edit is not
// allowed to move a comment between communities or up the thread. It also
// means an edit to a comment that never federated (the author was opted out at
// the time, or it predates the bridge) resolves to nothing and is skipped.
//
// The thread ROOT is read back the same way, and RESOLVED when the stored
// snapshot predates it (walkThreadRoot). It cannot be defaulted away: an update
// never re-resolves its thread, so a root the create path knows and the update
// path does not is a lock the create is refused by and the edit sails through —
// leaving the author of an existing reply a live, editable surface inside a
// closed thread.
func (d *Dispatcher) commentThread(ctx context.Context, atURI string, commit *CommitEvent) (*resolvedThread, error) {
	if commit.Operation == operationUpdate {
		stored, err := d.objects.GetByATURI(ctx, atURI)
		if errors.IsNotFound(err) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("read outbound state for %s: %w", atURI, err)
		}
		parent := d.parentFromSnapshot(stored.TranslatedSnapshot)
		rootATURI := rootFromSnapshot(stored.TranslatedSnapshot)
		var unresolved *unresolvedThread
		if rootATURI == "" {
			// Written before the root was recorded: climb this comment's own
			// parent chain rather than assuming anything. The successful edit
			// re-writes the snapshot below, so the walk happens once per row.
			if rootATURI, unresolved, err = d.walkThreadRoot(ctx, atURI); err != nil {
				return nil, err
			}
		}
		return &resolvedThread{
			ParentATURI:    parent.ATURI,
			ParentAPID:     parent.APID,
			CommunityDID:   stored.CommunityDID,
			CommunityAPID:  stored.CommunityAPID,
			Depth:          stored.Depth,
			RootATURI:      rootATURI,
			RootUnresolved: unresolved,
		}, nil
	}
	return d.resolveParent(ctx, commit)
}

// resolveParent finds the thing a new comment replies to, and places the
// comment one level below it. Reading the parent's RECORDED depth is what
// keeps the cap O(1) instead of walking the thread on every comment.
func (d *Dispatcher) resolveParent(ctx context.Context, commit *CommitEvent) (*resolvedThread, error) {
	parentATURI := replyRef(commit.Record, "parent")
	if parentATURI == "" {
		// A root-only reply hangs directly under the thread root.
		parentATURI = replyRef(commit.Record, "root")
	}
	if parentATURI == "" {
		return nil, nil
	}

	parent, err := d.resolveSubject(ctx, parentATURI)
	if err != nil || parent == nil {
		return nil, err
	}
	// The thread is the parent's thread — its recorded root, or the parent
	// itself when the parent is a root. Same shape as the depth above: inherited
	// from the parent's own state, which is what keeps both O(1) instead of
	// walking the thread on every comment.
	rootATURI, unresolved, err := d.threadRootOf(ctx, parent)
	if err != nil {
		return nil, err
	}
	return &resolvedThread{
		ParentATURI:    parent.ATURI,
		ParentAPID:     parent.APID,
		CommunityDID:   parent.CommunityDID,
		CommunityAPID:  parent.CommunityAPID,
		Depth:          parent.Depth + 1,
		RootATURI:      rootATURI,
		RootUnresolved: unresolved,
	}, nil
}

// ensureActor makes sure the author has an AP identity, resolving their handle
// first if they do not.
//
// The existence check comes FIRST and short-circuits everything: the local
// part is frozen, so re-resolving for an actor that already exists would put
// two network round-trips (PLC + well-known) in front of every comment to
// re-derive a name that can no longer change.
func (d *Dispatcher) ensureActor(ctx context.Context, did string) error {
	_, err := d.apActors.GetByDID(ctx, did)
	if err == nil {
		return nil
	}
	if !errors.IsNotFound(err) {
		return fmt.Errorf("look up actor for %s: %w", did, err)
	}

	// No fallback handle exists, by design: minting on a guess would freeze
	// the wrong local part, and freezing is not undoable. An unresolvable
	// handle FAILS the event so the connector's retry and the redriver can
	// recover it, rather than dropping the comment silently.
	handle, err := d.resolver.ResolveDIDHandle(ctx, did)
	if err != nil {
		return fmt.Errorf("resolve handle for %s: %w", did, err)
	}
	if _, err := d.actors.CreateActorForDID(ctx, did, handle); err != nil {
		return fmt.Errorf("mint actor for %s: %w", did, err)
	}
	return nil
}

// apObjectID is the AP id a native record federates as. It is derived from the
// at-uri's three parts rather than the rkey alone: rkeys are unique within one
// repo's collection, so an rkey-only id would collide across authors, and this
// URL has to identify exactly one record when a peer fetches it back.
func (d *Dispatcher) apObjectID(did string, commit *CommitEvent) string {
	return d.userOrigin + "/ap/object/" + did + "/" + commit.Collection + "/" + commit.RKey
}

// commentSnapshot is the durable state a Delete or a restore is rebuilt from.
// It keeps the RECORD as it arrived plus the context resolved around it,
// because the delete commit that arrives one day carries neither — including
// the PARENT, whose at-uri and AP id the Delete needs for causal ordering and
// addressing. Rendering all of it into ActivityPub vocabulary is task 15's;
// this is the input.
//
// The thread ROOT is here for a different consumer: the update path, which
// never re-resolves its thread and would otherwise have no way to know which
// conversation an edit belongs to. It is also what every LATER comment inherits
// its own root from, so recording it once at create time is what keeps the
// answer O(1) forever after.
func commentSnapshot(atURI string, commit *CommitEvent, thread *resolvedThread) ([]byte, error) {
	snapshot, err := json.Marshal(map[string]any{
		"atUri":         atURI,
		"cid":           commit.CID,
		"rev":           commit.Rev,
		"collection":    commit.Collection,
		"record":        commit.Record,
		"parentAtUri":   thread.ParentATURI,
		"parentApId":    thread.ParentAPID,
		"rootAtUri":     thread.RootATURI,
		"communityApId": thread.CommunityAPID,
	})
	if err != nil {
		// A record that will not marshal cannot be stored or delivered, and
		// retrying serializes exactly the same bytes.
		return nil, fmt.Errorf("%w: snapshot %s: %w", ErrPermanentEvent, atURI, err)
	}
	return snapshot, nil
}

// snapshotParent is the parent reference carried in a stored snapshot.
type snapshotParent struct {
	ATURI string `json:"parentAtUri"`
	APID  string `json:"parentApId"`
}

// parentFromSnapshot reads the parent back out of stored state. An unreadable
// or older snapshot yields empty strings rather than an error: the delete
// still has to go out, and delivery without the causal hint is better than a
// retraction that never leaves. The unmarshal error is not swallowed silently
// though — it is logged, because a snapshot that will not parse means every
// delete for that object loses its causal ordering, which an operator should
// be able to see rather than infer from missing parents downstream.
func (d *Dispatcher) parentFromSnapshot(snapshot []byte) snapshotParent {
	var parent snapshotParent
	if err := json.Unmarshal(snapshot, &parent); err != nil {
		d.logger.Debug("stored snapshot did not parse; delete proceeds without a parent hint",
			slog.String("error", err.Error()))
	}
	return parent
}

// rootFromSnapshot reads the thread root back out of stored state, or "" when
// the snapshot does not carry one (a row written before the root was recorded,
// or one that will not parse).
//
// The empty answer is NOT "no thread": every caller resolves it (threadRootOf,
// commentThread), because defaulting it to the object itself would quietly
// exempt every pre-existing nested comment from its thread's lock — and that
// does not self-heal, since the snapshot only advances on a successful edit and
// the edit is what would be wrongly allowed.
//
// A parse failure is deliberately indistinguishable from an absent key here:
// both mean "this snapshot cannot tell us", parentFromSnapshot already logs the
// unmarshal error for the same bytes, and the resolution the caller falls into
// is the correct response to either.
func rootFromSnapshot(snapshot []byte) string {
	var thread struct {
		RootATURI string `json:"rootAtUri"`
	}
	if err := json.Unmarshal(snapshot, &thread); err != nil {
		return ""
	}
	return thread.RootATURI
}

// replyRef reads reply.{name}.uri out of a decoded comment record.
func replyRef(record map[string]any, name string) string {
	reply, ok := record["reply"].(map[string]any)
	if !ok {
		return ""
	}
	ref, ok := reply[name].(map[string]any)
	if !ok {
		return ""
	}
	uri, _ := ref["uri"].(string)
	return uri
}

// The commit operations this consumer distinguishes.
const (
	operationCreate = "create"
	operationUpdate = "update"
)
