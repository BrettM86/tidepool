package consume

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// The native-comment path: a social.coves.community.comment record in a Coves
// user's own repo that belongs to a thread the bridge federates.
//
// Cycle F implements CREATE. Update, delete, the Lemmy depth cap and parents
// that are themselves comments are cycle H; they extend this file rather than
// replacing it, because the four steps below are the same for all of them.

// handleComment applies one comment commit.
//
// The ORDER of the steps is the design, and each one is a gate on the next:
//
//  1. the opt-out gate, so a user who said no never gets an AP identity
//     created for them;
//  2. the thread resolution, so a comment in a NATIVE community — which this
//     bridge has no business federating — is skipped before anything is
//     written or minted;
//  3. the lazy mint, THROUGH handle verification, because the local part it
//     derives is frozen at creation;
//  4. the outbound state, then the intent — state first, because the intent is
//     built from it and a delete one day has nothing else to be built from.
func (d *Dispatcher) handleComment(ctx context.Context, did string, commit *CommitEvent) error {
	if commit.Operation != operationCreate {
		// Update and delete are cycle H. Returning nil rather than an error
		// keeps the cursor moving; the rev gate has already claimed this
		// revision, which is what a later handler needs to stay ordered.
		d.logger.Debug("comment operation not handled yet",
			slog.String("operation", commit.Operation), slog.String("did", did))
		return nil
	}

	federating, err := d.mayFederate(ctx, did)
	if err != nil {
		return err
	}
	if !federating {
		// The residual split-thread case, explicitly chosen (decision 11): the
		// comment stays on the atproto side and the Lemmy side never sees it.
		d.logger.Debug("skipping comment from an opted-out author",
			slog.String("did", did), slog.String("rkey", commit.RKey))
		return nil
	}

	thread, err := d.resolveThread(ctx, commit)
	if err != nil {
		return err
	}
	if thread == nil {
		// Most native comments live in native communities. Dead-lettering
		// every one of them would bury the queue in events that are working
		// exactly as intended.
		d.logger.Debug("skipping comment with no federated parent",
			slog.String("did", did), slog.String("rkey", commit.RKey))
		return nil
	}

	if err := d.ensureActor(ctx, did); err != nil {
		return err
	}

	atURI := commitRecordURI(did, commit)
	snapshot, err := commentSnapshot(atURI, commit, thread)
	if err != nil {
		return err
	}

	stored, err := d.objects.Upsert(ctx, store.OutboundObject{
		ATURI:      atURI,
		APObjectID: d.apObjectID(did, commit),
		LastCID:    commit.CID,
		LastRev:    commit.Rev,
		// The community comes from the PARENT's mapping, never from anything
		// the comment asserts about itself: a record can claim any community
		// it likes, but its parent's mapping is what the bridge already
		// federated.
		CommunityDID:       thread.CommunityDID,
		CommunityAPID:      thread.CommunityAPID,
		TranslatedSnapshot: snapshot,
		Depth:              thread.Depth + 1,
	})
	if err != nil {
		return fmt.Errorf("write outbound state for %s: %w", atURI, err)
	}

	intent := CommentIntent{
		Op:            operationCreate,
		ATURI:         atURI,
		ID:            ActivityID(d.userOrigin, atURI, operationCreate, stored.LastActivitySeq),
		CommunityAPID: thread.CommunityAPID,
		ParentAPID:    thread.ParentAPID,
		Snapshot:      snapshot,
	}
	// parentATURI carries the causal dependency (decision 15): delivery must
	// not present a reply to a peer before the thing it replies to.
	if err := d.enqueuer.EnqueueActivity(ctx, did, did, thread.ParentATURI, intent); err != nil {
		return fmt.Errorf("enqueue comment intent for %s: %w", atURI, err)
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
	// Depth is the PARENT's reply depth; the comment sits one below it.
	Depth int
}

// resolveThread resolves the comment's parent through ap_objects and the
// parent's community through communities. A nil thread with a nil error means
// "not federated here" — a skip, not a failure.
func (d *Dispatcher) resolveThread(ctx context.Context, commit *CommitEvent) (*resolvedThread, error) {
	parentATURI := replyRef(commit.Record, "parent")
	if parentATURI == "" {
		// A comment with no reply.parent is a top-level comment shape this
		// task does not federate; root-only replies fall back to the root.
		parentATURI = replyRef(commit.Record, "root")
	}
	if parentATURI == "" {
		return nil, nil
	}

	parent, err := d.objectMappings.GetByATURI(ctx, parentATURI)
	if errors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("resolve comment parent %s: %w", parentATURI, err)
	}
	if parent.CommunityDID == "" {
		return nil, nil
	}

	community, err := d.communities.GetByDID(ctx, parent.CommunityDID)
	if errors.IsNotFound(err) {
		// The parent is mapped but its community is not one this bridge
		// federates, so there is nowhere to deliver to.
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("resolve community %s: %w", parent.CommunityDID, err)
	}

	thread := &resolvedThread{
		ParentATURI:   parentATURI,
		ParentAPID:    parent.APID,
		CommunityDID:  parent.CommunityDID,
		CommunityAPID: community.APGroupID,
	}
	// A parent that is itself a federated comment carries its own depth; a
	// parent that is a post has none, and its replies are depth 1.
	if parentState, err := d.objects.GetByATURI(ctx, parentATURI); err == nil {
		thread.Depth = parentState.Depth
	} else if !errors.IsNotFound(err) {
		return nil, fmt.Errorf("read parent outbound state %s: %w", parentATURI, err)
	}
	return thread, nil
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
// It keeps the RECORD as it arrived plus the context that was resolved around
// it, because the delete commit that arrives one day carries neither.
// Rendering it into ActivityPub vocabulary is task 15's; this is the input.
func commentSnapshot(atURI string, commit *CommitEvent, thread *resolvedThread) ([]byte, error) {
	snapshot, err := json.Marshal(map[string]any{
		"atUri":         atURI,
		"cid":           commit.CID,
		"rev":           commit.Rev,
		"collection":    commit.Collection,
		"record":        commit.Record,
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

// operationCreate is the commit operation that carries a new record.
const operationCreate = "create"
