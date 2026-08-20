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
//
//  2. An Undo for every vote a peer still holds. Tidepool's aggregate is the
//     FEDIVERSE-ONLY tally and the reseed SUBTRACTS live delivered votes from
//     the origin's API count (task 17b), so a standing vote from a withdrawn
//     actor is a number the reseed keeps subtracting from a score readers see.
//
//     "Standing" is wider than delivered_state='delivered', deliberately: a vote
//     whose delivery is HELD FOR SETTLEMENT is one the peer ALREADY ACCEPTED
//     with only our bookkeeping outstanding, and its settlement lands after this
//     withdrawal — so enumerating the settled rows alone would leave a real vote
//     on a real instance with nothing left to notice (ListStandingForActor).
//     The retraction is what makes that safe in both directions: `undone` is
//     terminal in SetDeliveredState, so the late settlement cannot put the vote
//     back.
//
//     RESIDUAL, and it is the one nothing here can close: a delivery CLAIMED and
//     mid-POST at this moment is indistinguishable from one that will never be
//     sent. If that POST lands after the purge, the peer holds a vote we never
//     retracted. Detecting it needs the peer's own state, which is
//     reconciliation — 17e's. A vote whose community row is GONE is the other
//     peer-side residual: its Undo has no address, so the peer is never told —
//     but the LOCAL retraction still lands (retractUnaddressableVotes), because
//     "stop counting this actor's votes" is this tier's own decision and needs
//     no inbox.
//
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
	prefs       store.FederationPrefs
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
		prefs:       store.NewFederationPrefs(db),
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

	// EVERY READ AND EVERY REMOTE LOOKUP HAPPENS BEFORE THE TRANSACTION OPENS.
	// Resolving a vote's community can touch the network (the inbox resolver
	// caches, but a cold entry fetches an actor document), and holding a
	// transaction open across N remote fetches keeps locks for as long as the
	// slowest peer takes to answer.
	targets, err := p.deliveries.DistinctInboxesForActor(ctx, did)
	if err != nil {
		return err
	}
	liveVotes, err := p.votes.ListStandingForActor(ctx, did)
	if err != nil {
		return err
	}
	retractable, unaddressable, err := p.addressableVotes(ctx, did, liveVotes)
	if err != nil {
		return err
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("purge %s: begin: %w", did, err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := p.enqueuer.EnqueueFanOut(ctx, tx, did, "", consume.PersonDeleteIntent{
		ActorDID: did,
		// seq 0: a withdrawal happens once per identity, and the id must be the
		// SAME string on every retry so a peer recognises the redelivery as the
		// activity it already has.
		ID: consume.ActivityID(p.userOrigin, did, personDeleteOp, 0),
	}, targets); err != nil {
		return err
	}

	if err := p.undoLiveVotes(ctx, tx, did, retractable); err != nil {
		return err
	}

	if err := p.retractUnaddressableVotes(ctx, tx, unaddressable); err != nil {
		return err
	}

	// The preference this withdrawal was asked for stops being a REQUEST here,
	// inside the same transaction as the withdrawal itself. Both doors reach
	// this code — an explicit deleteRemote record and a confirmed account
	// deletion — and only the marked row is protected from being cleared by a
	// later re-enable, so marking it anywhere but here would leave one door
	// open. A missing preference does not fail the purge: it is a hole in the
	// caller's ordering, not a reason to abandon a withdrawal already enqueued.
	if err := p.prefs.MarkPurgedTx(ctx, tx, did); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("mark purge committed for %s: %w", did, err)
	} else if errors.IsNotFound(err) {
		p.logger.Warn("purge committed with no federation preference to mark",
			slog.String("did", did))
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
		slog.Int("votes_retracted", len(retractable)),
		slog.Int("votes_unaddressable", len(unaddressable)))
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
func (p *Purger) undoLiveVotes(ctx context.Context, tx *sql.Tx, did string, votes []retractableVote) error {
	for _, retractable := range votes {
		vote := retractable.vote

		// One statement bumps the seq — the Undo is the next activity and its id
		// must not collide with the Like's — and flips the state. CurrentActivityID
		// is preserved by the upsert, which matters: it is the id the Like went out
		// under and the Undo has to embed it.
		vote.DeliveredState = store.DeliveredStateUndone
		bumped, err := p.votes.UpsertTx(ctx, tx, vote)
		if err != nil {
			return fmt.Errorf("retract vote %s: %w", vote.VoteATURI, err)
		}

		if err := p.enqueuer.EnqueueFanOut(ctx, tx, did, vote.SubjectATURI, consume.VoteIntent{
			Op:          consume.OperationUndo,
			VoteATURI:   vote.VoteATURI,
			SubjectAPID: vote.SubjectAPID,
			// Read back from state, never guessed: an Undo{Like} withdrawing a
			// Dislike would move the peer's count the wrong way.
			//
			// AND THE MISMATCH IS NOW REACHABLE, where it used to be
			// hypothetical. Since the upsert began keeping `delivered` through
			// a flip, a vote that was flipped but whose flip never delivered is
			// STANDING — ListStandingForActor returns it — so this Undo goes
			// out carrying the NEW direction and an InnerActivityID the peer
			// never saw, to retract the OLD vote they actually hold.
			//
			// What is expected to save it is that the translator spells the
			// inner object out in full — {type, id, actor, object} with actor
			// and object as real ids (outbound/translator.go) — so a peer
			// matching the retraction on (actor, object) drops the right vote
			// whatever the wrapped type and id say. WHETHER LEMMY MATCHES ON
			// THAT PAIR IS NOT ESTABLISHED IN THIS TREE: there is no
			// outbound-vote e2e, so nothing here has ever watched a real
			// instance answer this request. Treat it as an assumption carried
			// by the erasure path, not as a verified guarantee.
			Direction:       vote.Direction,
			ID:              consume.ActivityID(p.userOrigin, vote.VoteATURI, consume.OperationUndo, bumped.ActivitySeq),
			InnerActivityID: vote.CurrentActivityID,
			CommunityAPID:   retractable.communityAPID,
		}, []store.DeliveryTarget{{
			Inbox:       retractable.inbox,
			OrderingKey: retractable.communityAPID,
		}}); err != nil {
			return err
		}
	}
	return nil
}

// retractUnaddressableVotes records the retraction for votes whose Undo has
// nowhere to go — same transaction as everything else, and the same flip
// undoLiveVotes writes, minus the enqueue.
//
// "Stop counting this actor's votes" is the purge's OWN decision, and it does
// not need a peer: leaving the row `delivered` keeps 17b's reseed subtracting a
// tombstoned actor's vote from served scores forever, and the 17e recast sweep
// excludes retraction-shaped rows by design, so nothing downstream would ever
// correct it. The peer-side residual — an instance that still counts the vote —
// joins the other documented residuals; the log line in addressableVotes is its
// record. The upsert's seq bump is harmless here: no activity id is ever minted
// from it.
func (p *Purger) retractUnaddressableVotes(ctx context.Context, tx *sql.Tx, votes []store.OutboundVote) error {
	for _, vote := range votes {
		vote.DeliveredState = store.DeliveredStateUndone
		if _, err := p.votes.UpsertTx(ctx, tx, vote); err != nil {
			return fmt.Errorf("retract unaddressable vote %s: %w", vote.VoteATURI, err)
		}
	}
	return nil
}

// retractableVote is a live vote paired with the community AP id its Undo is
// addressed to — resolved before the transaction opens, so no remote lookup
// happens under a lock.
type retractableVote struct {
	vote          store.OutboundVote
	communityAPID string
	inbox         string
}

// addressableVotes resolves the addressing for each live vote and SPLITS OFF
// the ones that cannot be addressed at all, returned second.
//
// A community that has been deleted or unfollowed since the vote was cast has no
// row, and there is no inbox to send its Undo to. Treating that as fatal would
// hold the ENTIRE erasure hostage to one vote: the transaction rolls back, the
// Delete{Person} fan-out with it, the event replays into the same missing row
// and eventually dead-letters — a user's whole withdrawal lost to a community
// that no longer exists. But unaddressable is a fact about the DELIVERY only —
// the vote still gets its local retraction (retractUnaddressableVotes), so it
// is handed back rather than dropped.
//
// A genuine storage error is still fatal. "This community is gone" and "the
// database did not answer" are different facts, and only the first one is an
// answer.
func (p *Purger) addressableVotes(ctx context.Context, did string, votes []store.OutboundVote) ([]retractableVote, []store.OutboundVote, error) {
	out := make([]retractableVote, 0, len(votes))
	var unaddressable []store.OutboundVote
	for i := range votes {
		vote := votes[i]
		community, err := p.communities.GetByDID(ctx, vote.CommunityDID)
		if errors.IsNotFound(err) {
			p.logger.Warn("purge: a live vote's community is gone; retracting locally, the peer-side retraction cannot be sent",
				slog.String("did", did), slog.String("vote", vote.VoteATURI),
				slog.String("community_did", vote.CommunityDID))
			unaddressable = append(unaddressable, vote)
			continue
		}
		if err != nil {
			return nil, nil, fmt.Errorf("resolve community %s for vote %s: %w",
				vote.CommunityDID, vote.VoteATURI, err)
		}
		// The INBOX is resolved here too, and that is the whole reason this
		// function exists outside the transaction: resolving one can fetch a
		// remote actor document, and doing that with locks held on
		// outbound_deliveries stalls the worker — and anything else touching
		// those rows — for as long as the slowest peer takes to answer.
		inbox, err := p.enqueuer.Inbox(ctx, community.APGroupID)
		if err != nil {
			return nil, nil, fmt.Errorf("resolve inbox for %s: %w", community.APGroupID, err)
		}
		out = append(out, retractableVote{
			vote: vote, communityAPID: community.APGroupID, inbox: inbox,
		})
	}
	return out, unaddressable, nil
}
