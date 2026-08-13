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
		d.logger.Debug("skipping comment with no federated thread",
			slog.String("did", did), slog.String("rkey", commit.RKey),
			slog.String("operation", commit.Operation))
		return nil
	}

	if thread.Depth > maxCommentDepth {
		// Named "depth" on purpose: the connector stores err.Error() as the
		// dead letter's last_error, and that string is all an operator
		// triaging the queue has to go on.
		return fmt.Errorf("%w: comment %s is at depth %d, beyond Lemmy's cap of %d",
			ErrPermanentEvent, atURI, thread.Depth, maxCommentDepth)
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
		ParentAPID:    parentAPID,
		Snapshot:      stored.TranslatedSnapshot,
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
}

// commentThread resolves the thread context for a create or an update.
//
// An UPDATE reads it back from the state its create wrote rather than
// re-resolving: the record could name a different parent, and an edit is not
// allowed to move a comment between communities or up the thread. It also
// means an edit to a comment that never federated (the author was opted out at
// the time, or it predates the bridge) resolves to nothing and is skipped.
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
		return &resolvedThread{
			ParentATURI:   parent.ATURI,
			ParentAPID:    parent.APID,
			CommunityDID:  stored.CommunityDID,
			CommunityAPID: stored.CommunityAPID,
			Depth:         stored.Depth,
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
	return &resolvedThread{
		ParentATURI:   parent.ATURI,
		ParentAPID:    parent.APID,
		CommunityDID:  parent.CommunityDID,
		CommunityAPID: parent.CommunityAPID,
		Depth:         parent.Depth + 1,
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
func commentSnapshot(atURI string, commit *CommitEvent, thread *resolvedThread) ([]byte, error) {
	snapshot, err := json.Marshal(map[string]any{
		"atUri":         atURI,
		"cid":           commit.CID,
		"rev":           commit.Rev,
		"collection":    commit.Collection,
		"record":        commit.Record,
		"parentAtUri":   thread.ParentATURI,
		"parentApId":    thread.ParentAPID,
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
