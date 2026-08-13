package consume

import (
	"context"
	"fmt"
	"log/slog"

	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// The native-vote path: a social.coves.feed.vote record in a Coves user's own
// repo, cast on something this bridge federates.
//
// A vote DELETE commit names the vote record and NOTHING else — not the
// subject, not the direction, not the id the Like went out under. But an Undo
// has to EMBED the activity it withdraws. That gap is the whole reason
// outbound_votes exists, and it is why the row is written before the intent
// and outlives the record it describes.

// The vote directions this build understands. `direction` is an OPEN enum in
// the lexicon, so an unrecognised value is a forward-compatible record rather
// than a malformed one.
const (
	directionUp   = "up"
	directionDown = "down"
)

// handleVote applies one vote commit.
func (d *Dispatcher) handleVote(ctx context.Context, did string, commit *CommitEvent) error {
	switch commit.Operation {
	case operationCreate, operationUpdate:
		return d.applyVoteWrite(ctx, did, commit)
	case operationDelete:
		return d.applyVoteDelete(ctx, did, commit)
	default:
		d.logger.Debug("unknown vote operation",
			slog.String("operation", commit.Operation), slog.String("did", did))
		return nil
	}
}

// applyVoteWrite records a cast vote and enqueues the Like/Dislike.
//
// The step order mirrors the comment path, and for the same reasons: the
// opt-out gate first (a vote IS a federating interaction, so it mints — an
// earlier draft of this task missed that gate), then everything that decides
// whether the vote can federate at all, and only then the identity and the
// state.
func (d *Dispatcher) applyVoteWrite(ctx context.Context, did string, commit *CommitEvent) error {
	federating, err := d.mayFederate(ctx, did)
	if err != nil {
		return err
	}
	if !federating {
		d.logger.Debug("skipping vote from an opted-out author",
			slog.String("did", did), slog.String("rkey", commit.RKey))
		return nil
	}

	direction := stringField(commit.Record, "direction")
	if direction != directionUp && direction != directionDown {
		// Nothing is stored: guessing a direction would push a vote the user
		// never cast, and dead-lettering would turn a lexicon rollout into a
		// queue full of rows nobody can redrive.
		d.logger.Debug("skipping vote with an unrecognised direction",
			slog.String("did", did), slog.String("direction", direction))
		return nil
	}

	subjectATURI := refURI(commit.Record, "subject")
	if subjectATURI == "" {
		d.logger.Debug("skipping vote with no subject", slog.String("did", did))
		return nil
	}
	subject, err := d.resolveSubject(ctx, subjectATURI)
	if err != nil {
		return err
	}
	if subject == nil {
		// Native users vote in native communities constantly; dead-lettering
		// that would bury the queue.
		d.logger.Debug("skipping vote on a subject this bridge does not federate",
			slog.String("did", did), slog.String("subject", subjectATURI))
		return nil
	}

	if err := d.ensureActor(ctx, did); err != nil {
		return err
	}

	voteATURI := commitRecordURI(did, commit)
	// The seq is derived BEFORE the write so the stored id and the intent's id
	// are the same string: they must agree, or the Undo would withdraw an
	// activity the peer never saw. Upsert bumps from the same base — 0 for a
	// new row, +1 for a re-cast — so the two stay in step.
	seq := 0
	if existing, err := d.votes.GetByATURI(ctx, voteATURI); err == nil {
		seq = existing.ActivitySeq + 1
	} else if !errors.IsNotFound(err) {
		return fmt.Errorf("read vote state for %s: %w", voteATURI, err)
	}

	stored, err := d.votes.Upsert(ctx, store.OutboundVote{
		VoteATURI:    voteATURI,
		ActorDID:     did,
		SubjectATURI: subjectATURI,
		SubjectAPID:  subject.APID,
		CommunityDID: subject.CommunityDID,
		Direction:    direction,
		// Stored, not recomputed at delete time: by then the vote record is
		// gone and this id is the only handle on the activity the Undo has to
		// name.
		CurrentActivityID: ActivityID(d.userOrigin, voteATURI, operationCreate, seq),
		DeliveredState:    store.DeliveredStatePending,
	})
	if errors.IsAlreadyExists(err) {
		// A SECOND vote record for a subject this actor already has a live
		// vote on. The existing row is left exactly as it is — clobbering it
		// would strand the Undo still owed for the Like already delivered.
		//
		// TRANSIENT, not a skip: the newer record retries until the older
		// one's delete lands. A vote change reaching this consumer out of
		// order (the delete behind the re-cast) resolves itself on redrive,
		// where a skip would drop the user's new vote for good. If the delete
		// never comes the budget exhausts and the row is visible in the DLQ,
		// which is the right place for two sides disagreeing about what the
		// user's vote is.
		d.logger.Warn("a second live vote for one subject; retrying until the first is deleted",
			slog.String("did", did),
			slog.String("vote", voteATURI),
			slog.String("subject", subjectATURI))
		return err
	}
	if err != nil {
		return fmt.Errorf("write vote state for %s: %w", voteATURI, err)
	}

	return d.enqueuer.EnqueueActivity(ctx, did, did, subjectATURI, VoteIntent{
		Op:            operationCreate,
		VoteATURI:     voteATURI,
		SubjectAPID:   stored.SubjectAPID,
		Direction:     stored.Direction,
		ID:            stored.CurrentActivityID,
		CommunityAPID: subject.CommunityAPID,
	})
}

// applyVoteDelete withdraws a vote, using ONLY state.
//
// Like a comment delete, this is NOT gated on the opt-out: an Undo only ever
// removes something, and blocking it would leave the user's vote standing on
// the peer forever — the opposite of what asking to stop federating means.
func (d *Dispatcher) applyVoteDelete(ctx context.Context, did string, commit *CommitEvent) error {
	voteATURI := commitRecordURI(did, commit)

	stored, err := d.votes.GetByATURI(ctx, voteATURI)
	if errors.IsNotFound(err) {
		// No Undo may be sent for a Like no peer ever received.
		d.logger.Debug("skipping delete for a vote with no outbound state",
			slog.String("did", did), slog.String("vote", voteATURI))
		return nil
	}
	if err != nil {
		return fmt.Errorf("read vote state for %s: %w", voteATURI, err)
	}

	// Re-upserting the row bumps the seq — the Undo is the next activity, and
	// its id must not collide with the Like's — while keeping every other
	// column, CurrentActivityID above all: that is the id the Like was
	// delivered under, and the Undo has to embed it. The row SURVIVES: task 15
	// needs it to retry the Undo and clears it only once delivery succeeds.
	undone, err := d.votes.Upsert(ctx, *stored)
	if err != nil {
		return fmt.Errorf("bump vote state for %s: %w", voteATURI, err)
	}

	return d.enqueuer.EnqueueActivity(ctx, did, did, stored.SubjectATURI, VoteIntent{
		Op:          operationUndo,
		VoteATURI:   voteATURI,
		SubjectAPID: stored.SubjectAPID,
		// Read back from state: the delete commit carries neither, and an
		// Undo{Like} withdrawing a Dislike would move the peer's count the
		// wrong way.
		Direction:       stored.Direction,
		ID:              ActivityID(d.userOrigin, voteATURI, operationUndo, undone.ActivitySeq),
		InnerActivityID: stored.CurrentActivityID,
		CommunityAPID:   d.communityAPID(ctx, stored.CommunityDID),
	})
}

// operationUndo is not a commit operation: it is the outbound op a vote delete
// becomes, kept distinct from "delete" because an Undo names the activity it
// withdraws rather than an object.
const operationUndo = "undo"

// communityAPID resolves a community's AP Group id for addressing. A miss
// yields "" rather than an error: the withdrawal still has to go out, and task
// 15 can address it from the subject.
func (d *Dispatcher) communityAPID(ctx context.Context, communityDID string) string {
	if communityDID == "" {
		return ""
	}
	community, err := d.communities.GetByDID(ctx, communityDID)
	if err != nil {
		return ""
	}
	return community.APGroupID
}

// refURI reads a strong-ref's uri out of a decoded record.
func refURI(record map[string]any, name string) string {
	ref, ok := record[name].(map[string]any)
	if !ok {
		return ""
	}
	uri, _ := ref["uri"].(string)
	return uri
}
