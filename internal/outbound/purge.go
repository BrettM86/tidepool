package outbound

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"tidepool/internal/consume"
	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// Purger is task 17d's DESTRUCTIVE opt-out tier (decision 11's second tier): the
// user asked not merely that the bridge stop speaking for them, but that what it
// already said be withdrawn. It satisfies consume.RemoteContentDeleter.
//
// THREE CONSEQUENCES, AND EACH IS UNREACHABLE FROM THE OTHERS:
//
//  1. Delete{Person, removeData:true} to every inbox the actor's content
//     reached. The instances it misses keep serving those posts forever.
//  2. An Undo for every vote a peer still holds. Tidepool's aggregate is the
//     FEDIVERSE-ONLY tally and the reseed SUBTRACTS live delivered votes from
//     the origin's API count (task 17b), so a standing vote from a withdrawn
//     actor is a number the reseed keeps subtracting from a score readers see.
//  3. The actor document stops resolving — 410 Gone.
//
// IRREVERSIBLE, and reached only from an explicit enabled=false +
// deleteRemote=true record or a CONFIRMED account deletion — never inferred.
// Lemmy un-deletes a person on refetch; other software does not, so nothing here
// promises resurrection and no code path clears the tombstone.
//
// ONE TRANSACTION. A withdrawal that half-applied is the worst of both tiers:
// an actor tombstoned with no Delete sent is an identity that vanished while its
// content stayed, and a Delete sent with the votes left standing is a purge that
// keeps voting. Everything below either commits together or not at all — and
// because it is reached through the consumer, a rollback leaves the record's rev
// gate un-advanced and the whole request replayable.
type Purger struct {
	db          *sql.DB
	userOrigin  string
	enqueuer    *Enqueuer
	deliveries  store.OutboundDeliveries
	votes       store.OutboundVotes
	actors      store.APActors
	communities store.Communities
	logger      *slog.Logger
}

// NewPurger builds the destructive tier. The stores are defaulted from db, as
// every other constructor in this package does.
func NewPurger(db *sql.DB, userOrigin string, enqueuer *Enqueuer) *Purger {
	return &Purger{
		db:          db,
		userOrigin:  userOrigin,
		enqueuer:    enqueuer,
		deliveries:  store.NewOutboundDeliveries(db),
		votes:       store.NewOutboundVotes(db),
		actors:      store.NewAPActors(db),
		communities: store.NewCommunities(db),
		logger:      slog.Default(),
	}
}

// WithLogger returns the purger with a logger attached. Optional: the default is
// slog.Default(), and every line this tier writes is worth having.
func (p *Purger) WithLogger(logger *slog.Logger) *Purger {
	if logger != nil {
		p.logger = logger
	}
	return p
}

// personDeleteOp is the activity-id op string for a person withdrawal. It is
// distinct from every record operation so the deterministic id can never collide
// with an activity about the user's CONTENT — the preimage is
// (tag, subject, op, seq), and here the subject is the actor rather than a
// record.
const personDeleteOp = "delete-person"

// DeleteRemoteContent asks every peer that ever received this actor's content to
// delete it, retracts the votes they still hold, and stops serving the actor.
//
// IDEMPOTENT by construction rather than by a guard: the activity id is
// deterministic, the delivery insert returns the standing row instead of
// duplicating it, and the tombstone keeps its original timestamp. A retried
// purge therefore reaches inboxes the first attempt missed without re-sending
// anything to the ones it reached — which matters because this is the work most
// likely to be retried after a crash, and asking a peer twice risks deleting
// content a re-enabled user has since restored.
func (p *Purger) DeleteRemoteContent(ctx context.Context, did string) error {
	if did == "" {
		return errors.NewValidationError("did", "must not be empty")
	}

	// Both reads happen BEFORE the transaction opens: they are the inputs, and
	// holding a transaction open across them buys nothing.
	targets, err := p.deliveries.DistinctInboxesForActor(ctx, did)
	if err != nil {
		return err
	}
	liveVotes, err := p.votes.ListDeliveredForActor(ctx, did)
	if err != nil {
		return err
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("purge %s: begin: %w", did, err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := p.enqueuer.EnqueueFanOut(ctx, tx, did, consume.PersonDeleteIntent{
		ActorDID: did,
		// seq 0: a withdrawal happens once per identity, and the id must be the
		// SAME string on every retry so a peer recognises the redelivery as the
		// activity it already has.
		ID: consume.ActivityID(p.userOrigin, did, personDeleteOp, 0),
	}, targets); err != nil {
		return err
	}

	if err := p.undoLiveVotes(ctx, tx, did, liveVotes); err != nil {
		return err
	}

	// LAST, because it is the step that stops the actor being servable and the
	// enqueues above resolve that actor. It is also the one an operator will
	// read as "this user is gone", so it must not be true before the withdrawal
	// it announces has been written.
	if err := p.actors.TombstoneTx(ctx, tx, did); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("tombstone actor %s: %w", did, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("purge %s: commit: %w", did, err)
	}
	p.logger.Info("destructive opt-out applied; the withdrawal is irreversible",
		slog.String("did", did),
		slog.Int("inboxes", len(targets)),
		slog.Int("votes_retracted", len(liveVotes)))
	return nil
}

// undoLiveVotes retracts the votes peers still hold and marks them retracted.
//
// The state flip is the half that is easy to miss and impossible to see: without
// it the reseed keeps subtracting these votes from the origin's API tally for an
// actor that no longer exists, so a subject's served score drifts down and stays
// there. It is written HERE rather than waiting for the Undo's delivery
// callback, deliberately: a purge is terminal, and a withdrawn actor's vote must
// stop counting when we decide to withdraw it, not if and when a peer confirms.
// The cost is stated plainly — if the Undo never lands, we have stopped counting
// a vote the peer may still hold — and it is the right side to err on, because
// the alternative subtracts forever on behalf of somebody who is gone.
func (p *Purger) undoLiveVotes(ctx context.Context, tx *sql.Tx, did string, votes []store.OutboundVote) error {
	for i := range votes {
		vote := votes[i]
		// The community resolves BEFORE the row is bumped, so a failed lookup
		// rolls back rather than leaving a bumped seq behind an Undo that was
		// never enqueued (consume's vote delete draws the same line).
		community, err := p.communities.GetByDID(ctx, vote.CommunityDID)
		if err != nil {
			return fmt.Errorf("resolve community %s for vote %s: %w",
				vote.CommunityDID, vote.VoteATURI, err)
		}

		// One statement bumps the seq — the Undo is the next activity and its id
		// must not collide with the Like's — and flips the state. CurrentActivityID
		// is preserved by the upsert, which matters: it is the id the Like went out
		// under and the Undo has to embed it.
		vote.DeliveredState = store.DeliveredStateUndone
		bumped, err := p.votes.UpsertTx(ctx, tx, vote)
		if err != nil {
			return fmt.Errorf("retract vote %s: %w", vote.VoteATURI, err)
		}

		if err := p.enqueuer.EnqueueActivity(ctx, tx, did, did, vote.SubjectATURI, consume.VoteIntent{
			Op:          consume.OperationUndo,
			VoteATURI:   vote.VoteATURI,
			SubjectAPID: vote.SubjectAPID,
			// Read back from state, never guessed: an Undo{Like} withdrawing a
			// Dislike would move the peer's count the wrong way.
			Direction:       vote.Direction,
			ID:              consume.ActivityID(p.userOrigin, vote.VoteATURI, consume.OperationUndo, bumped.ActivitySeq),
			InnerActivityID: vote.CurrentActivityID,
			CommunityAPID:   community.APGroupID,
		}); err != nil {
			return err
		}
	}
	return nil
}
