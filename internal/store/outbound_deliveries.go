package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/lib/pq"

	"tidepool/internal/errors"
)

type postgresOutboundDeliveries struct {
	db *sql.DB
}

// NewOutboundDeliveries creates the postgres-backed outbound_deliveries
// repository — the per-inbox delivery queue (task 15).
func NewOutboundDeliveries(db *sql.DB) OutboundDeliveries {
	return &postgresOutboundDeliveries{db: db}
}

// deliveryColumns is the SELECT/RETURNING list for a full OutboundDelivery row.
const deliveryColumns = `
	seq, activity_id, target_inbox, ordering_key, state, attempts,
	next_attempt_at, claimed_until, delivered_at, last_status_code,
	last_error_class, response_excerpt, created_at, updated_at`

func (r *postgresOutboundDeliveries) Enqueue(ctx context.Context, delivery OutboundDelivery) (*OutboundDelivery, error) {
	// A fresh delivery is pending, unattempted, unclaimed: state, attempts,
	// next_attempt_at and seq are all defaulted by the table. A duplicate
	// (activity, inbox) pair violates the PK — mapped to AlreadyExists rather
	// than pre-checked, since a SELECT-then-INSERT races a concurrent enqueue.
	query := `
		INSERT INTO outbound_deliveries (activity_id, target_inbox, ordering_key)
		VALUES ($1, $2, $3)
		RETURNING` + deliveryColumns

	stored, err := scanOutboundDelivery(r.db.QueryRowContext(ctx, query,
		delivery.ActivityID, delivery.TargetInbox, delivery.OrderingKey))
	if err != nil {
		if _, ok := uniqueViolation(err); ok {
			return nil, errors.NewConflictError("outbound_delivery", "activity_inbox",
				delivery.ActivityID+" "+delivery.TargetInbox)
		}
		return nil, fmt.Errorf("enqueue outbound_delivery %q -> %q: %w",
			delivery.ActivityID, delivery.TargetInbox, err)
	}
	return stored, nil
}

func (r *postgresOutboundDeliveries) EnqueueTx(ctx context.Context, tx *sql.Tx, delivery OutboundDelivery) (*OutboundDelivery, error) {
	if tx == nil {
		return nil, errors.NewValidationError("tx", "must not be nil")
	}
	// IDEMPOTENT, unlike its pool-backed sibling above, and the difference is
	// deliberate on both sides.
	//
	// This one rides a CALLER'S transaction, where a unique violation is not an
	// error the caller can inspect and move past — postgres aborts the whole
	// transaction, taking the rev-gate advance riding it down too, so the event
	// replays forever. And a duplicate here is not a caller bug: ONE activity
	// fans out to MANY inboxes (Delete{Person} to every community an actor
	// delivered to), so a redelivery legitimately re-enqueues pairs that already
	// exist while others still need writing.
	//
	// The STANDING row wins. Re-enqueueing must never reset a delivery that has
	// since been delivered, cancelled by an opt-out, or poisoned — the row's
	// state is the record of what happened to it. That also settles the
	// co-hosted case: two communities sharing one inbox collapse to a single
	// delivery, keeping the ordering key of the first, because one POST to that
	// inbox is one POST however many communities it serves.
	query := `
		INSERT INTO outbound_deliveries (activity_id, target_inbox, ordering_key)
		VALUES ($1, $2, $3)
		ON CONFLICT (activity_id, target_inbox) DO NOTHING
		RETURNING` + deliveryColumns

	stored, err := scanOutboundDelivery(tx.QueryRowContext(ctx, query,
		delivery.ActivityID, delivery.TargetInbox, delivery.OrderingKey))
	if stderrors.Is(err, sql.ErrNoRows) {
		// DO NOTHING returns no row, so the delivery was already there. Read it
		// back on the same transaction: callers get the same contract either way
		// — a row that exists — and never have to tell the two apart.
		existing, rerr := scanOutboundDelivery(tx.QueryRowContext(ctx,
			`SELECT`+deliveryColumns+` FROM outbound_deliveries
			  WHERE activity_id = $1 AND target_inbox = $2`,
			delivery.ActivityID, delivery.TargetInbox))
		if rerr != nil {
			return nil, fmt.Errorf("read existing outbound_delivery %q -> %q: %w",
				delivery.ActivityID, delivery.TargetInbox, rerr)
		}
		return existing, nil
	}
	if err != nil {
		return nil, fmt.Errorf("enqueue outbound_delivery %q -> %q: %w",
			delivery.ActivityID, delivery.TargetInbox, err)
	}
	return stored, nil
}

func (r *postgresOutboundDeliveries) ClaimNext(ctx context.Context, lease time.Duration) (*OutboundDelivery, error) {
	if lease <= 0 {
		return nil, errors.NewValidationError("lease", "must be positive")
	}

	// Generalizes inbox_events.ClaimNext (task 12's lesson): the candidate is
	// the head of an ordering key — its min-seq PENDING row — that is also past
	// its next_attempt_at and unleased. Per-community serialization is
	// head-of-line: while an older pending sibling on a key exists (queued,
	// leased, or backing off) every younger delivery on that key is invisible;
	// a delivered/poisoned/cancelled sibling leaves the partial index and stops
	// blocking.
	//
	// The heads are found by a recursive CTE emulating a loose index scan over
	// idx_outbound_deliveries_queue (ordering_key, seq WHERE state='pending'):
	// one index descent per DISTINCT pending key jumps to each key's head, so
	// the claim finds the DISTINCT pending keys' heads without a per-row NOT
	// EXISTS. The heads are materialized with ARRAY(...) — not a plain IN or a
	// correlated EXISTS — so the head set is computed ONCE (the loose scan)
	// rather than re-derived per row. The outer SELECT then locks and re-applies
	// every claimability predicate on just that head set (a claim committed
	// between the CTE snapshot and the lock is then seen and skipped).
	//
	// NOTE on the outer re-fetch: seq is a BIGSERIAL ordering column, NOT the
	// primary key (the PK is (activity_id, target_inbox)) and has no standalone
	// index — so `c.seq = ANY(ARRAY(...))` is a re-check over the small head set,
	// NOT the indexed point-fetch inbox_events gets (there `id` IS the PK). For a
	// deep pending backlog the planner can only reach the head rows through the
	// partial (ordering_key, seq) index, so the re-check is not the strict
	// O(keys × log N) a seq index would give. A dedicated UNIQUE index on seq
	// (its own migration, so goose actually applies it) would restore the
	// point-fetch and is worth adding if this path ever profiles hot.
	// FOR UPDATE ... SKIP LOCKED lets concurrent workers race without
	// serializing on row locks; the UPDATE stamps the lease and counts the
	// attempt atomically.
	query := `
		UPDATE outbound_deliveries
		SET claimed_until = CURRENT_TIMESTAMP + make_interval(secs => $1),
		    attempts = attempts + 1,
		    updated_at = now()
		WHERE seq = (
			SELECT c.seq FROM outbound_deliveries c
			WHERE c.seq = ANY (ARRAY(
				WITH RECURSIVE key_heads AS (
					SELECT h.seq, h.ordering_key FROM (
						SELECT e.seq, e.ordering_key
						FROM outbound_deliveries e
						WHERE e.state = 'pending'
						ORDER BY e.ordering_key, e.seq
						LIMIT 1
					) h
					UNION ALL
					SELECT n.seq, n.ordering_key FROM key_heads k
					CROSS JOIN LATERAL (
						SELECT e.seq, e.ordering_key
						FROM outbound_deliveries e
						WHERE e.state = 'pending'
						  AND e.ordering_key > k.ordering_key
						ORDER BY e.ordering_key, e.seq
						LIMIT 1
					) n
				)
				SELECT seq FROM key_heads))
			  AND c.state = 'pending'
			  AND c.next_attempt_at <= CURRENT_TIMESTAMP
			  AND (c.claimed_until IS NULL OR c.claimed_until <= CURRENT_TIMESTAMP)
			ORDER BY c.seq
			LIMIT 1
			FOR UPDATE OF c SKIP LOCKED)
		RETURNING` + deliveryColumns

	delivery, err := scanOutboundDelivery(r.db.QueryRowContext(ctx, query, lease.Seconds()))
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFoundError("outbound_delivery", "next claimable")
		}
		return nil, fmt.Errorf("claim next outbound_delivery: %w", err)
	}
	return delivery, nil
}

func (r *postgresOutboundDeliveries) MarkDelivered(ctx context.Context, activityID, targetInbox string, lastStatusCode int, claimToken time.Time) (bool, bool, error) {
	// Fencing: only the worker still holding the claim (claimed_until ==
	// claimToken) may record the outcome, and only while the row is
	// non-terminal (state = 'pending'). A stale worker whose lease lapsed and
	// was re-claimed no longer matches, writes 0 rows, and is reported
	// applied=false so it cannot clobber the newer attempt.
	query := `
		WITH updated AS (
			UPDATE outbound_deliveries
			SET state = 'delivered', delivered_at = CURRENT_TIMESTAMP,
			    claimed_until = NULL, last_status_code = $3, updated_at = now()
			WHERE activity_id = $1 AND target_inbox = $2
			  AND state = 'pending'
			  AND claimed_until = $4
			RETURNING 1
		)
		SELECT
			EXISTS (SELECT 1 FROM outbound_deliveries WHERE activity_id = $1 AND target_inbox = $2),
			EXISTS (SELECT 1 FROM updated)`

	return r.markResult(ctx, "mark delivered", query,
		activityID, targetInbox, lastStatusCode, claimToken.UTC())
}

// releaseStatement is the fenced reschedule BOTH releases run, with exactly one
// slot: what the attempt ledger does. Release leaves it alone; ReleaseParked
// hands the claim's increment back.
//
// It is one template rather than two statements because the FENCE is what makes
// the give-back safe. `state = 'pending' AND claimed_until = $7` admits only the
// worker still holding the claim, so the only increment a park can subtract is
// the one its own claim just added. A second copy of this statement is a copy of
// that fence, and the day the two drift is the day a stale worker's late park
// un-counts an attempt the CURRENT claim genuinely spent — a delivery quietly
// gaining retries it already used, visible nowhere until a poison budget that
// should have stopped never does.
//
// The slot is filled from a CONSTANT below and never from input, and both
// expansions happen ONCE at package init (releaseQuery / releaseParkedQuery
// under it) rather than per call — so this template has exactly two finished
// statements, neither of which can be handed a runtime string, and no future
// `%` written into this SQL can be mangled by a formatting pass.
const releaseStatement = `
		WITH updated AS (
			UPDATE outbound_deliveries
			SET claimed_until = NULL, next_attempt_at = $3,
			    last_error_class = $4, response_excerpt = $5,
			    last_status_code = $6, updated_at = now()%s
			WHERE activity_id = $1 AND target_inbox = $2
			  AND state = 'pending'
			  AND claimed_until = $7
			RETURNING 1
		)
		SELECT
			EXISTS (SELECT 1 FROM outbound_deliveries WHERE activity_id = $1 AND target_inbox = $2),
			EXISTS (SELECT 1 FROM updated)`

// handBackClaimAttempt is ReleaseParked's ONE difference from Release. GREATEST
// floors the ledger at zero so an unmatched arithmetic edge can never write a
// negative attempt count (attempts is INT NOT NULL DEFAULT 0, so there is no
// NULL to guard).
const handBackClaimAttempt = `,
			    attempts = GREATEST(attempts - 1, 0)`

// The two finished statements, expanded once at init: a failure keeps its
// attempt, a park hands its own back.
var (
	releaseQuery       = fmt.Sprintf(releaseStatement, "")
	releaseParkedQuery = fmt.Sprintf(releaseStatement, handBackClaimAttempt)
)

func (r *postgresOutboundDeliveries) Release(ctx context.Context, activityID, targetInbox, errorClass, excerpt string, lastStatusCode int, nextAttempt, claimToken time.Time) (bool, bool, error) {
	// Fencing + non-terminal guard: only the current claim holder reschedules
	// (claimed_until == claimToken, state = 'pending'), so a stale worker's late
	// release cannot resurrect a delivery a newer attempt already drove to a
	// terminal state. The row stays pending with the lease cleared so a retry
	// can re-claim after the backoff. The attempt ClaimNext charged stays
	// charged: this delivery was TRIED.
	return r.markResult(ctx, "release", releaseQuery,
		activityID, targetInbox, nextAttempt.UTC(), errorClass, excerpt, lastStatusCode, claimToken.UTC())
}

// ReleaseParked is the ATTEMPT-NEUTRAL release: the same fenced reschedule as
// Release, with the claim's own increment handed back.
//
// THE INVARIANT IS "a park hands back exactly its own claim's increment". A
// delivery that was HELD — by the kill switch, a dry run, a causal wait — was
// never tried, but ClaimNext charges an attempt to every claim alike, so without
// the give-back a hold spends retry budget the delivery no longer has when the
// hold lifts, and a long enough hold poisons a delivery nothing ever attempted.
// The fence is what makes the decrement safe rather than merely convenient: only
// the worker whose claim added the increment can subtract one, so no park can
// reach an attempt another claim spent. A park is therefore net-zero and a real
// failure still costs exactly one.
//
// settleLater's Release deliberately keeps its increment instead of parking: a
// held-for-settlement delivery is already accepted by the peer and handle()
// short-circuits before re-POSTing it, so it can never re-poison and its growing
// attempt count only widens the backoff between bookkeeping retries.
func (r *postgresOutboundDeliveries) ReleaseParked(ctx context.Context, activityID, targetInbox, errorClass, excerpt string, lastStatusCode int, nextAttempt, claimToken time.Time) (bool, bool, error) {
	return r.markResult(ctx, "release parked", releaseParkedQuery,
		activityID, targetInbox, nextAttempt.UTC(), errorClass, excerpt, lastStatusCode, claimToken.UTC())
}

func (r *postgresOutboundDeliveries) MarkPoisoned(ctx context.Context, activityID, targetInbox, errorClass, excerpt string, lastStatusCode int, claimToken time.Time) (bool, bool, error) {
	// Fencing + non-terminal guard: only the current claim holder may poison
	// (claimed_until == claimToken, state = 'pending'). A poisoned delivery
	// leaves the pending partial index and stops blocking its ordering key.
	query := `
		WITH updated AS (
			UPDATE outbound_deliveries
			SET state = 'poisoned', claimed_until = NULL,
			    last_error_class = $3, response_excerpt = $4,
			    last_status_code = $5, updated_at = now()
			WHERE activity_id = $1 AND target_inbox = $2
			  AND state = 'pending'
			  AND claimed_until = $6
			RETURNING 1
		)
		SELECT
			EXISTS (SELECT 1 FROM outbound_deliveries WHERE activity_id = $1 AND target_inbox = $2),
			EXISTS (SELECT 1 FROM updated)`

	return r.markResult(ctx, "poison", query,
		activityID, targetInbox, errorClass, excerpt, lastStatusCode, claimToken.UTC())
}

// markResult runs a fenced (exists, applied) mark statement and maps a missing
// (activity, inbox) pair to NotFound.
func (r *postgresOutboundDeliveries) markResult(ctx context.Context, op, query string, args ...any) (bool, bool, error) {
	var exists, applied bool
	if err := r.db.QueryRowContext(ctx, query, args...).Scan(&exists, &applied); err != nil {
		return false, false, fmt.Errorf("%s outbound_delivery: %w", op, err)
	}
	return exists, applied, nil
}

func (r *postgresOutboundDeliveries) CancelForActor(ctx context.Context, actorDID string) (int64, error) {
	return cancelForActor(ctx, r.db, actorDID)
}

func (r *postgresOutboundDeliveries) CancelForActorTx(ctx context.Context, tx *sql.Tx, actorDID string) (int64, error) {
	if tx == nil {
		return 0, errors.NewValidationError("tx", "must not be nil")
	}
	return cancelForActor(ctx, tx, actorDID)
}

func (r *postgresOutboundDeliveries) CancelOutwardForActorTx(ctx context.Context, tx *sql.Tx, actorDID string) (int64, error) {
	if tx == nil {
		return 0, errors.NewValidationError("tx", "must not be nil")
	}
	return cancelOutwardForActor(ctx, tx, actorDID)
}

// DeliveryHeldForSettlement is the last_error_class of a delivery the PEER HAS
// ALREADY ACCEPTED whose local settlement — the causal stamp, the vote ledger —
// has not committed yet. The row deliberately stays `pending` so a worker can
// re-claim it and finish the bookkeeping WITHOUT repeating the POST (task 17b).
//
// The column is this package's, so the vocabulary lives here and the worker
// reads it from here: two copies of the string would let a cancellation and a
// resume disagree about which rows are held, which is exactly the bug the
// predicate below exists to prevent.
const DeliveryHeldForSettlement = "ledger_unsettled"

// DeliveryHeldForRefusalSettlement is the last_error_class of a vote Undo the
// PEER HAS REFUSED terminally whose local retraction (the outbound_votes row
// delete) has not committed yet. The row stays `pending` so a worker can
// re-claim it, finish the delete and then poison it, WITHOUT POSTing the Undo
// again.
const DeliveryHeldForRefusalSettlement = "refusal_unsettled"

// notHeldForSettlement is the term EVERY cancellation carries, and it is one
// constant rather than three because forgetting it is silent.
//
// It covers both holds: DeliveryHeldForSettlement (the peer accepted) and
// DeliveryHeldForRefusalSettlement (the peer refused a vote Undo terminally and
// the outbound_votes row delete is outstanding).
//
// A cancellation answers "this must not go out". A held delivery already WENT
// out: the peer has answered it, and the only thing outstanding is our own
// record of that. Cancelling is terminal, so the worker never returns to it and
// the settlement is stranded — and the stranding is invisible until it surfaces
// somewhere else entirely. Two places, both crossing sub-run boundaries: the
// vote reseed subtracts only `delivered` rows, so a stranded one over-counts a
// served score forever; and the destructive tier enumerates a purged actor's
// live votes from that same column, so the Undo an erasure owes the peer is
// never enqueued and an erased user's vote stands on an instance nobody told.
//
// It is the same rule as "terminal rows are untouched", one state over: a
// decision that arrives LATER may not rewrite the record of something that has
// already happened. There is nothing to stop here — the send is done.
//
// IS DISTINCT FROM rather than <>, and the reason is NOT that NULLs exist:
// last_error_class is NOT NULL DEFAULT ” (migration 020), so a plain <> is
// correct today and both forms cancel the ordinary never-failed row. The
// NULL-safe form is used because this one fragment is pasted into every
// cancellation there is, and under <> the day that column becomes nullable is
// the day EVERY cancellation silently stops matching the rows it exists to
// cancel — a failure that shows up as deliveries going out after a user asked
// us to stop, nowhere near the schema change that caused it.
//
// The value is interpolated from a CONSTANT and never from input, which is what
// lets one fragment drop into statements with different parameter counts. The
// column name is unqualified deliberately: outbound_activities (the only table
// any of these statements joins) has no such column, so it is unambiguous
// everywhere and stays correct whether the target is aliased or not.
const notHeldForSettlement = `
		  AND last_error_class IS DISTINCT FROM '` + DeliveryHeldForSettlement + `'
		  AND last_error_class IS DISTINCT FROM '` + DeliveryHeldForRefusalSettlement + `'`

// RetractionKinds are the activity kinds that TAKE CONTENT DOWN: a Delete of a
// post or comment, and the Undo of a vote. They are exempt from every consent
// decision, in the queue and at the worker alike.
//
// The asymmetry is the point. A consent withdrawal means "stop publishing for
// me"; a retraction is the only way an opted-out user removes what is ALREADY
// published. Cancelling one leaves that content standing on the peer forever,
// which is the opposite of what was asked — the worker states exactly this and
// skips its consent recheck for these kinds, and a cancellation that swept them
// would undo that decision one layer up, where nothing observes it.
//
// It is ONE list shared with outbound.isRetraction so the queue and the worker
// cannot drift into disagreeing about which activities a stopped user is still
// owed.
//
// One Undo is not a retraction in effect: the Undo{Delete} that restores a
// taken-down object republishes it. No cancellation takes it either, but the
// worker does not send it while its author is paused, disabled or opted out:
// it holds it on consent (HeldReasonConsent) with the object's queued work,
// and ReleaseConsentHeldTx puts them back when consent returns.
var RetractionKinds = []string{"Delete", "Undo"}

// notARetraction excludes those kinds from a cancellation. It reads from the
// joined outbound_activities row, so any statement using it must join a.
// cancelForActor is the SWEEPING cancel: every pending delivery of this actor's,
// retractions included, across every community. It is the kill switch and the
// operator's manual cancel — decisions that mean "stop the queue", not "stop
// publishing for this user".
//
// CancelOutwardForActor is the consent variant, and the difference between them
// is a user's ability to take their own content down. Two decisions, two
// statements: one predicate serving both is how the narrower decision silently
// acquires the wider one's reach.
func cancelForActor(ctx context.Context, ex execer, actorDID string) (int64, error) {
	query := `
		UPDATE outbound_deliveries d
		SET state = 'cancelled', claimed_until = NULL, updated_at = now()
		FROM outbound_activities a
		WHERE d.activity_id = a.activity_id
		  AND a.actor_did = $1
		  AND d.state = 'pending'` + notHeldForSettlement

	result, err := ex.ExecContext(ctx, query, actorDID)
	if err != nil {
		return 0, fmt.Errorf("cancel outbound_deliveries for actor %q: %w", actorDID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("cancel outbound_deliveries for actor %q: rows affected: %w", actorDID, err)
	}
	return affected, nil
}

// cancelOutwardForActor is the CONSENT withdrawal: park the actor's pending
// OUTWARD work — everything that publishes — while leaving their retractions to
// go out. Never poisoned (this is not a failure), terminal rows untouched, held
// settlements untouched, across every community because the decision is about
// the actor.
//
// TWO THINGS DEPEND ON THE RETRACTION EXEMPTION, and the second is not obvious:
//
//  1. A user who deletes a post and then opts out must still have the delete
//     delivered, or the post stays on Lemmy forever — the exact opposite of what
//     opting out means.
//  2. THE DESTRUCTIVE TIER'S OWN WITHDRAWAL. Delete{Person} and the vote Undos
//     are Deletes and Undos, enqueued by the purge on its own transaction. If a
//     later replay of the opt-out record ran a sweeping cancel, it would cancel
//     the erasure the previous attempt just committed — and nothing repairs it:
//     the delivery insert returns the standing (cancelled) row by design, and
//     the votes are already flipped, so the re-run enumerates nothing. Actor
//     tombstoned, peers never told, "destructive opt-out applied" in the log.
//
// Work held on a takedown pause (HeldReasonPause) is outward work waiting on
// the author's consent, so it is cancelled with the pending rows: otherwise an
// opt-in and the takedown's lift would publish what the opt-out withdrew.
// Rows held on consent (a takedown restore and its object's queued work) and
// on an object or parent takedown are left to their own releases.
func cancelOutwardForActor(ctx context.Context, ex execer, actorDID string) (int64, error) {
	query := `
		UPDATE outbound_deliveries d
		SET state = 'cancelled', claimed_until = NULL, held_reason = '', held_subject = '', updated_at = now()
		FROM outbound_activities a
		WHERE d.activity_id = a.activity_id
		  AND a.actor_did = $1
		  AND (d.state = 'pending' OR (d.state = 'held' AND d.held_reason = '` + HeldReasonPause + `'))` +
		notHeldForSettlement + `
		  AND a.kind <> ALL($2)`

	result, err := ex.ExecContext(ctx, query, actorDID, pq.Array(RetractionKinds))
	if err != nil {
		return 0, fmt.Errorf("cancel outward outbound_deliveries for actor %q: %w", actorDID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("cancel outward outbound_deliveries for actor %q: rows affected: %w",
			actorDID, err)
	}
	return affected, nil
}

func (r *postgresOutboundDeliveries) CancelForCommunity(ctx context.Context, orderingKey string) (int64, error) {
	// A community deleted or unfollowed out from under pending work: park every
	// PENDING delivery on the ordering key as cancelled, leaving terminal rows.
	query := `
		UPDATE outbound_deliveries
		SET state = 'cancelled', claimed_until = NULL, updated_at = now()
		WHERE ordering_key = $1 AND state = 'pending'` + notHeldForSettlement

	return r.cancel(ctx, "cancel outbound_deliveries for community", query, orderingKey)
}

// cancelPendingForActorInCommunity parks one actor's PENDING deliveries on ONE
// community's ordering key — the INTERSECTION its two neighbours above cannot
// express, and the shape a community ban needs.
//
// Both one-dimensional versions are wrong for a ban, in opposite directions:
// CancelForActor stops the author in every community they write to, and
// CancelForCommunity stops every author in this one. The ordering key is what
// makes the intersection reachable at all — co-hosted communities SHARE an
// inbox, so target_inbox says nothing about which community's traffic a row
// carries.
//
// Terminal rows are untouched, as everywhere else here: `delivered` cannot be
// un-sent (and the vote reseed subtracts exactly that state from the API tally,
// so rewriting it would move a number the user sees), and `poisoned` is an
// operator surface whose redrive is the recovery.
//
// One retraction is spared: the Undo of a Like/Dislike whose own delivery to
// the same community is `delivered`, or is pending but HELD FOR SETTLEMENT (the
// peer accepted it; only our bookkeeping is unfinished). A vote flip queues the
// old vote's Undo, and cancelling it would leave a vote the peer already holds
// standing. The match is on the community (ordering key), not the inbox: inbox
// rotation delivers to a fresh inbox without rewriting target_inbox, so the
// Undo can be queued at a different inbox than its Like. Everything else is cancelled, post and comment Deletes included,
// and so is an Undo whose Like/Dislike is still queued unsent: this statement
// cancels that vote too, so the peer never saw it. A spared Undo can still be
// refused by the peer for a banned actor; the worker then poisons it at once
// and finishes the retraction locally, which is the operator's visible outcome.
//
// It runs on a caller's transaction: the ban row and this cancellation are one
// decision, and store.CommunityBans.Ban commits them together.
func cancelPendingForActorInCommunity(ctx context.Context, ex execer, actorDID, orderingKey string) (int64, error) {
	result, err := ex.ExecContext(ctx, `
		UPDATE outbound_deliveries d
		SET state = 'cancelled', claimed_until = NULL, updated_at = now()
		FROM outbound_activities a
		WHERE d.activity_id = a.activity_id
		  AND a.actor_did = $1
		  AND d.ordering_key = $2
		  AND d.state = 'pending'`+notHeldForSettlement+`
		  AND NOT (
		      a.kind = 'Undo'
		      AND a.payload->'object'->>'type' IN ('Like', 'Dislike')
		      AND EXISTS (
		          SELECT 1 FROM outbound_deliveries inner_delivery
		          WHERE inner_delivery.activity_id = a.payload->'object'->>'id'
		            AND inner_delivery.ordering_key = d.ordering_key
		            AND (inner_delivery.state = 'delivered'
		                 OR (inner_delivery.state = 'pending'
		                     AND inner_delivery.last_error_class = '`+DeliveryHeldForSettlement+`'))))`, actorDID, orderingKey)
	if err != nil {
		return 0, fmt.Errorf("cancel outbound_deliveries for %q in %q: %w", actorDID, orderingKey, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("cancel outbound_deliveries for %q in %q: rows affected: %w",
			actorDID, orderingKey, err)
	}
	return affected, nil
}

func (r *postgresOutboundDeliveries) cancel(ctx context.Context, op, query string, arg string) (int64, error) {
	result, err := r.db.ExecContext(ctx, query, arg)
	if err != nil {
		return 0, fmt.Errorf("%s %q: %w", op, arg, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("%s %q: rows affected: %w", op, arg, err)
	}
	return affected, nil
}

func (r *postgresOutboundDeliveries) DistinctInboxesForActor(ctx context.Context, actorDID string) ([]DeliveryTarget, error) {
	if actorDID == "" {
		return nil, errors.NewValidationError("actor_did", "must not be empty")
	}
	// THE DELIVERY HISTORY IS THE ADDRESS BOOK. There is no other record of
	// which instances hold a user's content: an erasure has to go where the
	// content actually went, and that is these rows.
	//
	// DISTINCT ON the inbox, because the fan-out is per INSTANCE: co-hosted
	// communities share one inbox, and asking it twice sends the same instance
	// the same erasure twice. Each surviving row keeps a REAL ordering key from
	// the history, so the withdrawal serializes on a line the actor's other work
	// already uses rather than jumping an independent queue.
	//
	// EVERY state counts, terminal ones included. A delivered post is exactly
	// what has to be withdrawn; a poisoned or cancelled one may still have
	// reached the peer (the wire and the ledger disagree by definition in those
	// states), and reaching an instance that does not hold the content is
	// harmless while missing one that does is not.
	//
	// KNOWN GAP, and it is the honest boundary of what this can reach: an
	// instance that discovered the actor through search or WebFinger and never
	// received a delivery from us has no row here, so the erasure never reaches
	// it. Nothing in the bridge's state knows about that instance.
	query := `
		SELECT DISTINCT ON (d.target_inbox) d.target_inbox, d.ordering_key
		FROM outbound_deliveries d
		JOIN outbound_activities a ON a.activity_id = d.activity_id
		WHERE a.actor_did = $1
		ORDER BY d.target_inbox, d.seq`

	rows, err := r.db.QueryContext(ctx, query, actorDID)
	if err != nil {
		return nil, fmt.Errorf("list delivery inboxes for %q: %w", actorDID, err)
	}
	defer func() { _ = rows.Close() }()

	var targets []DeliveryTarget
	for rows.Next() {
		var target DeliveryTarget
		if err := rows.Scan(&target.Inbox, &target.OrderingKey); err != nil {
			return nil, fmt.Errorf("scan delivery inbox for %q: %w", actorDID, err)
		}
		targets = append(targets, target)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list delivery inboxes for %q: %w", actorDID, err)
	}
	return targets, nil
}

func (r *postgresOutboundDeliveries) Get(ctx context.Context, activityID, targetInbox string) (*OutboundDelivery, error) {
	query := `SELECT` + deliveryColumns + `
		FROM outbound_deliveries WHERE activity_id = $1 AND target_inbox = $2`
	delivery, err := scanOutboundDelivery(r.db.QueryRowContext(ctx, query, activityID, targetInbox))
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFoundError("outbound_delivery", activityID+" -> "+targetInbox)
		}
		return nil, fmt.Errorf("get outbound_delivery %q -> %q: %w", activityID, targetInbox, err)
	}
	return delivery, nil
}

// ParentDeliveryDisposition is what has become of the deliveries carrying a
// child's ACTUAL parent to one inbox — the causal gate's read on whether the
// parent can still land.
//
// THREE VALUES BECAUSE THE PARENT HAS THREE ENDINGS, and the third is the one a
// boolean could not express. A delivery goes terminal three ways, and the gate
// used to ask only "is it poisoned?": a CANCELLED parent — the consent recheck
// at claim time, an operator cancel, an actor or community sweep — answered
// false, left the pending index, and never got accepted_at, so its child waited
// on something that was never coming.
type ParentDeliveryDisposition string

const (
	// ParentDeliveryOpen means nothing has decided against the parent: it is
	// still pending, already delivered, or has no delivery row at all. The child
	// waits.
	ParentDeliveryOpen ParentDeliveryDisposition = "open"
	// ParentDeliveryPoisoned means a delivery of the parent poisoned. It
	// outranks every other reading — a poisoned parent is the loudest verdict
	// available and the one that predates this type.
	ParentDeliveryPoisoned ParentDeliveryDisposition = "poisoned"
	// ParentDeliveryCancelled means the parent HAS deliveries and every one of
	// them was cancelled: nothing is left that could make it land.
	ParentDeliveryCancelled ParentDeliveryDisposition = "cancelled"
)

func (r *postgresOutboundDeliveries) ParentDeliveryDisposition(ctx context.Context, parentATURI, targetInbox string) (ParentDeliveryDisposition, error) {
	// The parent's delivery is the one whose activity federated parentATURI as
	// its object: the activity payload's object.id is the served object URL,
	// which ends in "/ap/object/<did>/<collection>/<rkey>" — exactly the
	// at-uri's three parts. Match on that suffix so we need no origin here (and
	// DIDs/NSIDs/TIDs carry no LIKE metacharacters).
	//
	// ONE OBJECT CAN HAVE SEVERAL DELIVERIES to the same inbox — a Create and
	// every later Update carry the same object.id — so both readings are
	// aggregates over the whole set, and they are deliberately asymmetric:
	//
	//	poisoned  — ANY row. A poisoned delivery is a failure the peer may
	//	            already have half-seen, and the pre-existing contract is that
	//	            it condemns the descendant.
	//	cancelled — EVERY row, and at least one. A cancel is a decision about one
	//	            activity, not about the object: while a sibling is still
	//	            pending, something can yet make the parent land, and poisoning
	//	            the child out from under it would be a wrong answer arrived at
	//	            early. The "at least one" guard is what keeps the vacuous
	//	            all-of-nothing from reading as cancelled — a parent with NO
	//	            deliveries is a fediverse-origin object, whose children are
	//	            always eligible.
	suffix := strings.TrimPrefix(parentATURI, "at://")
	var poisoned, allCancelled bool
	err := r.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE d.state = 'poisoned') > 0,
			COUNT(*) > 0 AND COUNT(*) FILTER (WHERE d.state <> 'cancelled') = 0
		FROM outbound_deliveries d
		JOIN outbound_activities a ON a.activity_id = d.activity_id
		WHERE d.target_inbox = $2
		  AND a.payload -> 'object' ->> 'id' LIKE '%/ap/object/' || $1`,
		suffix, targetInbox).Scan(&poisoned, &allCancelled)
	if err != nil {
		return "", fmt.Errorf("read parent delivery disposition for %q: %w", parentATURI, err)
	}
	switch {
	case poisoned:
		return ParentDeliveryPoisoned, nil
	case allCancelled:
		return ParentDeliveryCancelled, nil
	default:
		return ParentDeliveryOpen, nil
	}
}

func (r *postgresOutboundDeliveries) CancelClaimed(ctx context.Context, activityID, targetInbox string, claimToken time.Time) (bool, bool, error) {
	// Fenced single-row cancel: only the current claim holder cancels, and only
	// while pending, so a stale worker cannot clobber a re-claim and — unlike
	// CancelForActor — an actor's OTHER pending deliveries (its retractions) are
	// left standing.
	query := `
		WITH updated AS (
			UPDATE outbound_deliveries
			SET state = 'cancelled', claimed_until = NULL, updated_at = now()
			WHERE activity_id = $1 AND target_inbox = $2
			  AND state = 'pending'
			  AND claimed_until = $3` + notHeldForSettlement + `
			RETURNING 1
		)
		SELECT
			EXISTS (SELECT 1 FROM outbound_deliveries WHERE activity_id = $1 AND target_inbox = $2),
			EXISTS (SELECT 1 FROM updated)`

	return r.markResult(ctx, "cancel claimed", query, activityID, targetInbox, claimToken.UTC())
}

func (r *postgresOutboundDeliveries) CountsByState(ctx context.Context) (map[DeliveryState]int, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT state, COUNT(*) FROM outbound_deliveries GROUP BY state`)
	if err != nil {
		return nil, fmt.Errorf("count outbound_deliveries by state: %w", err)
	}
	defer rows.Close()

	counts := make(map[DeliveryState]int)
	for rows.Next() {
		var state string
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			return nil, fmt.Errorf("scan delivery state count: %w", err)
		}
		counts[DeliveryState(state)] = n
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate delivery state counts: %w", err)
	}
	return counts, nil
}

func (r *postgresOutboundDeliveries) RedrivePoisoned(ctx context.Context, activityID, orderingKey string) (redriven, superseded int64, err error) {
	// Empty filters mean "any row", so the sweep can target one activity, one
	// community, or all poisoned deliveries. Reset attempts + next_attempt_at so
	// a redriven delivery gets a fresh budget and is immediately claimable.
	//
	// A SUPERSEDED vote Undo stays poisoned and is counted instead. Lemmy holds
	// one vote per (actor, post) and matches an Undo{vote} on that pair, not on
	// the inner id. Once the actor has a live vote row on the same post under a
	// different activity (a flip to a new rkey, or a same-rkey re-cast), that
	// vote replaces the old one on the peer, so the old Undo is not owed. Sent
	// now, it would withdraw the live vote on the peer while the row still
	// says it stands. A replacement counts only while the peer holds it or
	// still can: its row reads delivered, or a delivery of its activity is
	// still pending (held for settlement included). Redriving then could race
	// the replacement's delivery. A replacement whose deliveries were all
	// cancelled or poisoned will never reach the peer, so the old vote stands
	// there and the Undo is owed again. A live row under the Undo's own inner
	// id is the vote being withdrawn (the purge's shape), not a replacement.
	query := `
		WITH candidates AS (
			SELECT d.activity_id, d.target_inbox,
			       (a.kind = 'Undo'
			        AND a.payload->'object'->>'type' IN ('Like', 'Dislike')
			        AND EXISTS (
			            SELECT 1 FROM outbound_votes v
			            WHERE v.actor_did = a.actor_did
			              AND v.subject_ap_id = a.payload->'object'->>'object'
			              AND v.retraction_activity_id IS NULL
			              AND v.current_activity_id <> a.payload->'object'->>'id'
			              AND (v.delivered_state = 'delivered'
			                   OR EXISTS (
			                       SELECT 1 FROM outbound_deliveries r
			                       WHERE r.activity_id = v.current_activity_id
			                         AND r.state = 'pending')))) AS superseded
			FROM outbound_deliveries d
			JOIN outbound_activities a ON a.activity_id = d.activity_id
			WHERE d.state = 'poisoned'
			  AND ($1 = '' OR d.activity_id = $1)
			  AND ($2 = '' OR d.ordering_key = $2)
		), redriven AS (
			UPDATE outbound_deliveries d
			SET state = 'pending', attempts = 0, claimed_until = NULL, next_attempt_at = now(),
			    updated_at = now()
			FROM candidates c
			WHERE d.activity_id = c.activity_id
			  AND d.target_inbox = c.target_inbox
			  AND NOT c.superseded
			  AND d.state = 'poisoned'
			RETURNING 1
		)
		SELECT (SELECT COUNT(*) FROM redriven),
		       (SELECT COUNT(*) FROM candidates WHERE superseded)`
	if err := r.db.QueryRowContext(ctx, query, activityID, orderingKey).Scan(&redriven, &superseded); err != nil {
		return 0, 0, fmt.Errorf("redrive poisoned deliveries: %w", err)
	}
	return redriven, superseded, nil
}

func scanOutboundDelivery(row rowScanner) (*OutboundDelivery, error) {
	var delivery OutboundDelivery
	var state string
	var claimedUntil, deliveredAt sql.NullTime
	var lastStatus sql.NullInt64
	err := row.Scan(
		&delivery.Seq, &delivery.ActivityID, &delivery.TargetInbox, &delivery.OrderingKey,
		&state, &delivery.Attempts, &delivery.NextAttemptAt, &claimedUntil, &deliveredAt,
		&lastStatus, &delivery.LastErrorClass, &delivery.ResponseExcerpt,
		&delivery.CreatedAt, &delivery.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	delivery.State = DeliveryState(state)
	if claimedUntil.Valid {
		delivery.ClaimedUntil = &claimedUntil.Time
	}
	if deliveredAt.Valid {
		delivery.DeliveredAt = &deliveredAt.Time
	}
	if lastStatus.Valid {
		code := int(lastStatus.Int64)
		delivery.LastStatusCode = &code
	}
	return &delivery, nil
}

// Held-row reasons: the blocker a held delivery waits on.
const (
	// HeldReasonObject holds a delivery while its own object is taken down.
	HeldReasonObject = "object"
	// HeldReasonParent holds a reply while the parent it answers is taken down.
	HeldReasonParent = "parent"
	// HeldReasonConsent holds a takedown restore's Undo{Delete}, and the
	// restored object's queued Create/Update, while the author is paused,
	// disabled or opted out; the subject is the object's at-uri. Only
	// ReleaseConsentHeldTx releases it, never a takedown release.
	HeldReasonConsent = "consent"
	// HeldReasonPause holds a claimed outward delivery (a Create, Update or
	// vote) of a persona paused by a takedown source: an account label, or a
	// takendown or suspended #account status. The subject is the actor DID.
	// Like a consent hold it waits on the author, so only
	// ReleaseConsentHeldTx releases it, once the pause set clears.
	HeldReasonPause = "pause"
	// HeldReasonCommunity holds native work into a bridged community whose
	// `!takedown` stands (a vote, or a Create/Update with no takedown source of
	// its own) until the label is negated; the subject is the community DID.
	HeldReasonCommunity = "community"
)

// objectIDSuffix is the tail every served object URL of atURI ends in:
// "/ap/object/<did>/<collection>/<rkey>". The store has no origin, so a
// delivery is tied to its object by this suffix of the payload's object.id,
// compared with right() rather than LIKE because a record key may contain '_'.
func objectIDSuffix(atURI string) string {
	return "/ap/object/" + strings.TrimPrefix(atURI, "at://")
}

func (r *postgresOutboundDeliveries) HoldForObjectTx(ctx context.Context, tx *sql.Tx, atURI string) (int64, error) {
	return holdObjectRows(ctx, tx, atURI, HeldReasonObject)
}

func (r *postgresOutboundDeliveries) HoldObjectOnConsentTx(ctx context.Context, tx *sql.Tx, atURI string) (int64, error) {
	return holdObjectRows(ctx, tx, atURI, HeldReasonConsent)
}

// holdObjectRows holds atURI's own pending, unclaimed Create/Update rows that
// are not held for settlement, on reason with atURI as the subject.
func holdObjectRows(ctx context.Context, tx *sql.Tx, atURI, reason string) (int64, error) {
	if tx == nil {
		return 0, errors.NewValidationError("tx", "must not be nil")
	}
	if atURI == "" {
		return 0, errors.NewValidationError("at_uri", "must not be empty")
	}
	// Only the object's own publishing work (Create, Update) is held; its
	// retractions go out regardless. A row in lease is in flight and a row held
	// for settlement already reached the peer: both are left for the worker,
	// which checks the takedown set before it delivers.
	suffix := objectIDSuffix(atURI)
	result, err := tx.ExecContext(ctx, `
		UPDATE outbound_deliveries d
		SET state = 'held', held_reason = $2, held_subject = $1,
		    claimed_until = NULL, updated_at = now()
		FROM outbound_activities a
		WHERE d.activity_id = a.activity_id
		  AND d.state = 'pending'
		  AND (d.claimed_until IS NULL OR d.claimed_until <= CURRENT_TIMESTAMP)`+notHeldForSettlement+`
		  AND a.kind IN ('Create', 'Update')
		  AND right(a.payload -> 'object' ->> 'id', length($3)) = $3`,
		atURI, reason, suffix)
	if err != nil {
		return 0, fmt.Errorf("hold outbound_deliveries of %q: %w", atURI, err)
	}
	held, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("hold outbound_deliveries of %q: rows affected: %w", atURI, err)
	}
	return held, nil
}

func (r *postgresOutboundDeliveries) HoldCommunityWorkTx(ctx context.Context, tx *sql.Tx, communityAPID, communityDID string) (int64, error) {
	if tx == nil {
		return 0, errors.NewValidationError("tx", "must not be nil")
	}
	if communityAPID == "" {
		return 0, errors.NewValidationError("community_ap_id", "must not be empty")
	}
	if communityDID == "" {
		return 0, errors.NewValidationError("community_did", "must not be empty")
	}
	// Publishing work and votes only: retractions (a takedown Delete, a vote
	// Undo) go out regardless. Rows in lease or held for settlement are left
	// alone. The worker checks the community only when it claims a row: a row
	// whose POST is already in flight is delivered; one whose attempt fails
	// returns to pending and is held at its next claim.
	result, err := tx.ExecContext(ctx, `
		UPDATE outbound_deliveries d
		SET state = 'held', held_reason = $3, held_subject = $2,
		    claimed_until = NULL, updated_at = now()
		FROM outbound_activities a
		WHERE d.activity_id = a.activity_id
		  AND d.ordering_key = $1
		  AND d.state = 'pending'
		  AND (d.claimed_until IS NULL OR d.claimed_until <= CURRENT_TIMESTAMP)`+notHeldForSettlement+`
		  AND a.kind IN ('Create', 'Update', 'Like', 'Dislike')`,
		communityAPID, communityDID, HeldReasonCommunity)
	if err != nil {
		return 0, fmt.Errorf("hold outbound_deliveries into community %q: %w", communityAPID, err)
	}
	held, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("hold outbound_deliveries into community %q: rows affected: %w", communityAPID, err)
	}
	return held, nil
}

// heldRow is one held delivery a release examines, with its blockers' state.
type heldRow struct {
	activityID, targetInbox  string
	objectATURI, parentATURI string
	// restore: the row is a takedown restore's Undo{Delete} (held on consent).
	restore       bool
	objectRemoved bool
	parentRemoved bool
	// objectDeleted: the row publishes an object its author has since deleted
	// (its outbound state is tombstoned).
	objectDeleted bool
	// voteWithdrawn: the row is a Like/Dislike its vote no longer names as
	// live (WithdrawnVoteSQL).
	voteWithdrawn bool
	// vote: the row is a Like/Dislike. Its parent_at_uri is the voted
	// subject, which never holds it, so a removed parent does not re-key it.
	vote bool
}

// heldSubjectsReadHook, when set (tests only), runs after ReleaseHeldTx reads
// its subjects and before it locks them.
var heldSubjectsReadHook func()

// ReleaseHeldTx locks the subject and the objects, parents and parents'
// accounts (AccountSubjectOf: a bridged account's label removes its records)
// of the rows held on it in ONE sorted LockTakedownSubjectsTx call before the
// held rows are read. A row still blocked by its own object or its parent is
// rekeyed onto that blocker (a reply held on its parent and then taken down
// itself stays held on the parent, since a hold moves only pending rows), and
// that blocker's restore (a record restore, or an account-wide one, which
// takes the blocker's subject lock too) must not run between the read and the
// rekey: the restore would find no row held on the blocker, then the rekey
// would hold the row on a cleared blocker for good. A hold racing this
// release takes the subject's lock, so it either commits before the locked
// read (which finds its row) or rechecks after this tx commits (and sees the
// blocker cleared).
//
// Invariant: a row is settled only when its object, its parent and the
// parent's account are in the locked set. A hold that commits between the
// subject read and the lock adds a row whose subjects may be unlocked; when
// the locked read finds one, the attempt rolls back to a savepoint, which
// drops its advisory and row locks, and reruns with the grown set, still one
// sorted call per attempt.
func (r *postgresOutboundDeliveries) ReleaseHeldTx(ctx context.Context, tx *sql.Tx, subjectATURI string) (int64, error) {
	if tx == nil {
		return 0, errors.NewValidationError("tx", "must not be nil")
	}
	if subjectATURI == "" {
		return 0, errors.NewValidationError("subject_at_uri", "must not be empty")
	}
	if _, err := tx.ExecContext(ctx, `SAVEPOINT release_held`); err != nil {
		return 0, fmt.Errorf("release held outbound_deliveries on %q: savepoint: %w", subjectATURI, err)
	}
	for attempt := 1; attempt <= releaseHeldAttempts; attempt++ {
		held, covered, err := lockSubjectHeldRowsTx(ctx, tx, subjectATURI)
		if err != nil {
			return 0, err
		}
		if covered {
			if _, err := tx.ExecContext(ctx, `RELEASE SAVEPOINT release_held`); err != nil {
				return 0, fmt.Errorf("release held outbound_deliveries on %q: release savepoint: %w", subjectATURI, err)
			}
			return settleHeldRows(ctx, tx, held)
		}
		if _, err := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT release_held`); err != nil {
			return 0, fmt.Errorf("release held outbound_deliveries on %q: rollback to savepoint: %w", subjectATURI, err)
		}
	}
	return 0, fmt.Errorf("release held outbound_deliveries on %q: held rows outgrew the locked subjects %d times",
		subjectATURI, releaseHeldAttempts)
}

// subjectHeldWhere selects the rows ReleaseHeldTx settles: those held on the
// subject ($1). Rows held on consent or on a pause wait on their author, not
// on this subject: only ReleaseConsentHeldTx releases them.
const subjectHeldWhere = `d.held_subject = $1 AND d.held_reason NOT IN ('` +
	HeldReasonConsent + `', '` + HeldReasonPause + `')`

// lockSubjectHeldRowsTx is one attempt of ReleaseHeldTx: it reads the objects,
// parents and parents' accounts of the rows held on subjectATURI, locks them
// with the subject in one sorted call, then reads the held rows locked.
// covered is false when a read row has one of those outside the locked set;
// its rows must not be settled.
func lockSubjectHeldRowsTx(ctx context.Context, tx *sql.Tx, subjectATURI string) (held []heldRow, covered bool, err error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT s.subject
		FROM outbound_deliveries d
		JOIN outbound_activities a ON a.activity_id = d.activity_id
		CROSS JOIN LATERAL (VALUES (`+heldObjectATURI+`), (a.parent_at_uri),
		                           (`+accountSubjectSQL("a.parent_at_uri")+`)) AS s(subject)
		WHERE d.state = 'held' AND `+subjectHeldWhere+` AND s.subject <> ''`, subjectATURI)
	if err != nil {
		return nil, false, fmt.Errorf("read held row subjects on %q: %w", subjectATURI, err)
	}
	subjects := []string{subjectATURI}
	for rows.Next() {
		var subject string
		if err := rows.Scan(&subject); err != nil {
			_ = rows.Close()
			return nil, false, fmt.Errorf("scan held row subject on %q: %w", subjectATURI, err)
		}
		subjects = append(subjects, subject)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, false, fmt.Errorf("read held row subjects on %q: %w", subjectATURI, err)
	}
	if err := rows.Close(); err != nil {
		return nil, false, fmt.Errorf("read held row subjects on %q: %w", subjectATURI, err)
	}
	if heldSubjectsReadHook != nil {
		heldSubjectsReadHook()
	}
	if err := LockTakedownSubjectsTx(ctx, tx, subjects...); err != nil {
		return nil, false, err
	}
	held, err = readHeldRows(ctx, tx, subjectHeldWhere, subjectATURI)
	if err != nil {
		return nil, false, fmt.Errorf("read held outbound_deliveries on %q: %w", subjectATURI, err)
	}
	for _, row := range held {
		for _, subject := range []string{row.objectATURI, row.parentATURI, AccountSubjectOf(row.parentATURI)} {
			if subject != "" && !slices.Contains(subjects, subject) {
				return nil, false, nil
			}
		}
	}
	return held, true, nil
}

func (r *postgresOutboundDeliveries) ReleaseConsentHeldTx(ctx context.Context, tx *sql.Tx, actorDID string) (int64, error) {
	if tx == nil {
		return 0, errors.NewValidationError("tx", "must not be nil")
	}
	if actorDID == "" {
		return 0, errors.NewValidationError("actor_did", "must not be empty")
	}
	blocked, err := LockActorConsentTx(ctx, tx, actorDID)
	if err != nil {
		return 0, err
	}
	if blocked {
		return 0, cancelPauseHeldUnlessTakedownPausedTx(ctx, tx, actorDID)
	}
	// The actor's row lock (held by the caller's consent write, or taken
	// above) shuts out a worker's consent or pause hold for the rest of this
	// tx, so the subjects read here are every subject the actor's held rows
	// are on. A reply still blocked by its parent is rekeyed onto it, and the
	// parent's restore must not run between that read and the rekey.
	subjects, err := ConsentHeldSubjectsTx(ctx, tx, actorDID)
	if err != nil {
		return 0, err
	}
	if len(subjects) == 0 {
		return 0, nil
	}
	// Every blocker read below (each row's object and parent) is read under
	// its subject lock, all taken here in one sorted call. A takedown of the
	// object (Takedowns.AddSourceTx) and a restore of the object or the parent
	// (ReleaseHeldTx) take the same lock, so each either commits before the
	// read or waits for this tx.
	if err := LockTakedownSubjectsTx(ctx, tx, subjects...); err != nil {
		return 0, err
	}
	held, err := readHeldRows(ctx, tx, `d.held_reason IN ('`+HeldReasonConsent+`', '`+HeldReasonPause+
		`') AND a.actor_did = $1`, actorDID)
	if err != nil {
		return 0, fmt.Errorf("read consent-held outbound_deliveries of %q: %w", actorDID, err)
	}
	return settleHeldRows(ctx, tx, held)
}

// cancelPauseHeldUnlessTakedownPausedTx cancels actorDID's rows held on a
// pause (HeldReasonPause) once its consent is withdrawn for a reason that is
// not a takedown: it is paused only by deactivated or throttled, disabled, or
// opted out. Those rows waited for a takedown to lift; the withdrawn consent
// cancels them as the worker cancels outward work claimed under such a block.
// While a takedown source still pauses the persona they stay held. The caller
// holds the actor's row lock (LockActorConsentTx), which orders this against
// the worker's pause hold.
func cancelPauseHeldUnlessTakedownPausedTx(ctx context.Context, tx *sql.Tx, actorDID string) error {
	takedownPaused, err := TakedownPausedTx(ctx, tx, actorDID)
	if err != nil || takedownPaused {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE outbound_deliveries d
		SET state = 'cancelled', held_reason = '', held_subject = '', updated_at = now()
		FROM outbound_activities a
		WHERE d.activity_id = a.activity_id
		  AND a.actor_did = $1
		  AND d.state = 'held' AND d.held_reason = $2`, actorDID, HeldReasonPause); err != nil {
		return fmt.Errorf("cancel pause-held outbound_deliveries of %q: %w", actorDID, err)
	}
	return nil
}

// ConsentHeldSubjectsTx reads, in tx, every takedown subject whose blocker
// ReleaseConsentHeldTx reads for actorDID's rows held on consent or on a
// pause: each row's object (a consent row's subject; a pause row is held on
// the actor, so its object is read from its payload), its parent and the
// parent's account. An account-wide change that runs the consent release in
// its tx takes these with its own subjects in its one sorted
// LockTakedownSubjectsTx call.
func ConsentHeldSubjectsTx(ctx context.Context, tx *sql.Tx, actorDID string) ([]string, error) {
	if tx == nil {
		return nil, errors.NewValidationError("tx", "must not be nil")
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT s.subject
		FROM outbound_deliveries d
		JOIN outbound_activities a ON a.activity_id = d.activity_id
		CROSS JOIN LATERAL (VALUES (`+heldObjectATURI+`), (a.parent_at_uri),
		                           (`+accountSubjectSQL("a.parent_at_uri")+`)) AS s(subject)
		WHERE d.state = 'held' AND d.held_reason IN ($2, $3) AND a.actor_did = $1 AND s.subject <> ''`,
		actorDID, HeldReasonConsent, HeldReasonPause)
	if err != nil {
		return nil, fmt.Errorf("read consent-held subjects of %q: %w", actorDID, err)
	}
	var subjects []string
	for rows.Next() {
		var subject string
		if err := rows.Scan(&subject); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan consent-held subject of %q: %w", actorDID, err)
		}
		subjects = append(subjects, subject)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("read consent-held subjects of %q: %w", actorDID, err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("read consent-held subjects of %q: %w", actorDID, err)
	}
	return subjects, nil
}

// accountSubjectSQL is AccountSubjectOf in SQL: the bare DID authoring the
// record at-uri in the given expression, the empty string for anything else.
func accountSubjectSQL(uri string) string {
	return `COALESCE(substring((` + uri + `)::text FROM '^at://(did:[^/]*)/'), '')`
}

// accountHeldSubjectsReadHook, when set (tests only), runs after
// ReleaseAccountHeldTx reads its subjects and before it locks them.
var accountHeldSubjectsReadHook func()

// ReleaseAccountHeldTx releases, in tx, the replies held on a record of the
// bridged account accountDID (rows held on their parent, the parent authored
// by accountDID) once that account's `!takedown` no longer stands, deciding
// each row by ReleaseHeldTx's rules: a reply whose parent is still removed
// another way, or whose own object is removed, stays held on that blocker.
// The caller has written the label's negation or lapse in tx.
//
// Every subject it reads a blocker of (the account, each held row's parent and
// its account, and each held row's own object) is locked in ONE sorted
// LockTakedownSubjectsTx call before the held rows are read, so it cannot
// deadlock against a consent release or another multi-subject lock holder.
// A worker's or an enqueuer's hold on a parent also takes the parent's
// account subject, so once the account is locked neither commits a new hold
// on a record of this account until this tx ends.
//
// Invariant: a row is settled only when every subject it reads a blocker of
// is in the locked set. A hold that commits between the subject read and the
// lock adds a row whose object may be unlocked; settling it would race that
// object's restore (the restore finds no row held on the object, then the
// settle rekeys the row onto the cleared object, held for good). So when the
// locked read finds such a row, the attempt rolls back to a savepoint, which
// drops its advisory and row locks, and reruns with the grown set, still one
// sorted call per attempt. Returns how many rows were released.
func ReleaseAccountHeldTx(ctx context.Context, tx *sql.Tx, accountDID string) (int64, error) {
	if tx == nil {
		return 0, errors.NewValidationError("tx", "must not be nil")
	}
	if accountDID == "" {
		return 0, errors.NewValidationError("account_did", "must not be empty")
	}
	if _, err := tx.ExecContext(ctx, `SAVEPOINT release_account_held`); err != nil {
		return 0, fmt.Errorf("release held replies of account %q: savepoint: %w", accountDID, err)
	}
	for attempt := 1; attempt <= releaseHeldAttempts; attempt++ {
		held, covered, err := lockAccountHeldRowsTx(ctx, tx, accountDID)
		if err != nil {
			return 0, err
		}
		if covered {
			if _, err := tx.ExecContext(ctx, `RELEASE SAVEPOINT release_account_held`); err != nil {
				return 0, fmt.Errorf("release held replies of account %q: release savepoint: %w", accountDID, err)
			}
			return settleHeldRows(ctx, tx, held)
		}
		if _, err := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT release_account_held`); err != nil {
			return 0, fmt.Errorf("release held replies of account %q: rollback to savepoint: %w", accountDID, err)
		}
	}
	return 0, fmt.Errorf("release held replies of account %q: held rows outgrew the locked subjects %d times",
		accountDID, releaseHeldAttempts)
}

// releaseHeldAttempts bounds ReleaseHeldTx's and ReleaseAccountHeldTx's
// relocks. Each needs a hold committed in the short window between its subject
// read and its lock; past the bound the caller's tx fails and its caller
// retries.
const releaseHeldAttempts = 5

// lockAccountHeldRowsTx is one attempt of ReleaseAccountHeldTx: it reads the
// subjects of accountDID's parent-held rows, locks them in one sorted call,
// then reads the held rows locked. covered is false when a read row has a
// subject outside the locked set; its rows must not be settled.
func lockAccountHeldRowsTx(ctx context.Context, tx *sql.Tx, accountDID string) (held []heldRow, covered bool, err error) {
	prefix := "at://" + accountDID + "/"
	rows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT s.subject
		FROM outbound_deliveries d
		JOIN outbound_activities a ON a.activity_id = d.activity_id
		CROSS JOIN LATERAL (VALUES (d.held_subject), (a.parent_at_uri),
		                           (`+accountSubjectSQL("a.parent_at_uri")+`),
		                           (`+heldObjectATURI+`)) AS s(subject)
		WHERE d.state = 'held' AND d.held_reason = $2 AND starts_with(d.held_subject, $1)
		  AND s.subject <> ''`, prefix, HeldReasonParent)
	if err != nil {
		return nil, false, fmt.Errorf("read held reply subjects of account %q: %w", accountDID, err)
	}
	subjects := []string{accountDID}
	for rows.Next() {
		var subject string
		if err := rows.Scan(&subject); err != nil {
			_ = rows.Close()
			return nil, false, fmt.Errorf("scan held reply subject of account %q: %w", accountDID, err)
		}
		subjects = append(subjects, subject)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, false, fmt.Errorf("read held reply subjects of account %q: %w", accountDID, err)
	}
	if err := rows.Close(); err != nil {
		return nil, false, fmt.Errorf("read held reply subjects of account %q: %w", accountDID, err)
	}
	if accountHeldSubjectsReadHook != nil {
		accountHeldSubjectsReadHook()
	}
	if err := LockTakedownSubjectsTx(ctx, tx, subjects...); err != nil {
		return nil, false, err
	}
	held, err = readHeldRows(ctx, tx, `d.held_reason = '`+HeldReasonParent+`' AND starts_with(d.held_subject, $1)`, prefix)
	if err != nil {
		return nil, false, fmt.Errorf("read held replies of account %q: %w", accountDID, err)
	}
	for _, row := range held {
		for _, subject := range []string{row.objectATURI, row.parentATURI, AccountSubjectOf(row.parentATURI)} {
			if subject != "" && !slices.Contains(subjects, subject) {
				return nil, false, nil
			}
		}
	}
	return held, true, nil
}

// LockActorConsentTx share-locks actorDID's ap_actors row in tx and reports
// whether the actor's consent is withdrawn: it is disabled, delivery-paused,
// or has a federation opt-out (a missing preference is default-on). A missing
// actor is blocked: there is no persona to sign as. The share lock holds until
// tx ends, so a consent write (an unpause or an opt-in updates the row) either
// commits before this reads or waits for tx.
func LockActorConsentTx(ctx context.Context, tx *sql.Tx, actorDID string) (blocked bool, err error) {
	if tx == nil {
		return false, errors.NewValidationError("tx", "must not be nil")
	}
	var consented bool
	err = tx.QueryRowContext(ctx, `
		SELECT a.enabled AND NOT a.delivery_paused
		       AND COALESCE((SELECT p.enabled FROM federation_prefs p WHERE p.did = a.did), true)
		FROM ap_actors a
		WHERE a.did = $1
		FOR SHARE OF a`, actorDID).Scan(&consented)
	if stderrors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("read consent of %q: %w", actorDID, err)
	}
	return !consented, nil
}

// heldObjectATURI is the SQL expression for the at-uri of the object a held
// delivery d (with its activity a) publishes or restores. A consent-held row's
// object is its subject (a restore Undo's payload names the Delete, not the
// object); any other row's is read from its payload's object id.
const heldObjectATURI = `CASE WHEN d.held_reason = '` + HeldReasonConsent + `' THEN d.held_subject
	ELSE COALESCE('at://' || substring(a.payload -> 'object' ->> 'id' FROM '/ap/object/(.+)$'), '') END`

// HeldObjectATURISQL is heldObjectATURI for another package's query over a
// held delivery d and its activity a: lockAccountObjectsTx locks each held
// row's object before the releases it runs read the object's removal state.
const HeldObjectATURISQL = heldObjectATURI

// readHeldRows reads, in seq order and locked, the held rows matching where
// (a condition on d, the delivery, and a, its activity, with its arguments)
// with every blocker's state. Every blocker is rechecked, not only the one the
// row was held on: a reply held on its parent may since have been taken down
// itself. A consent-held row's object is its subject (a restore Undo's payload
// names the Delete, not the object).
func readHeldRows(ctx context.Context, tx *sql.Tx, where string, args ...any) ([]heldRow, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT d.activity_id, d.target_inbox, o.at_uri, a.parent_at_uri, a.kind = 'Undo',
		       EXISTS (SELECT 1 FROM outbound_takedown_sources s WHERE s.at_uri = o.at_uri),
		       a.parent_at_uri <> '' AND `+parentRemovedPredicate("a.parent_at_uri")+`,
		       a.kind IN ('Create', 'Update', 'Undo') AND EXISTS (
		           SELECT 1 FROM outbound_objects ob
		           WHERE ob.at_uri = o.at_uri AND ob.tombstoned_at IS NOT NULL),
		       `+withdrawnVoteSQL+`,
		       a.kind IN ('Like', 'Dislike')
		FROM outbound_deliveries d
		JOIN outbound_activities a ON a.activity_id = d.activity_id
		CROSS JOIN LATERAL (SELECT `+heldObjectATURI+` AS at_uri) o
		WHERE d.state = 'held' AND `+where+`
		ORDER BY d.seq
		FOR UPDATE OF d`, args...)
	if err != nil {
		return nil, err
	}
	var held []heldRow
	for rows.Next() {
		var row heldRow
		if err := rows.Scan(&row.activityID, &row.targetInbox, &row.objectATURI, &row.parentATURI, &row.restore,
			&row.objectRemoved, &row.parentRemoved, &row.objectDeleted, &row.voteWithdrawn, &row.vote); err != nil {
			_ = rows.Close()
			return nil, err
		}
		held = append(held, row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return held, nil
}

// settleHeldRows decides each held row, returning how many were released.
//
// A held row of an object its author deleted during the hold is cancelled
// whatever its blockers: a deleted object never comes back, and its Delete
// already went out (retractions are never held). So is a held Like/Dislike
// whose vote was deleted or re-cast during the hold: the deletion's Undo went
// out, and releasing the vote behind it would re-cast what the user withdrew
// (a re-cast's own activity is the one that stays). A row still blocked is
// re-keyed to a blocker that stands, so the release of that blocker finds it;
// a restore Undo whose object is removed again is cancelled instead, since
// the next restore sends its own Undo for the new episode.
//
// In seq order, one row at a time: each released row draws the next seq,
// which puts the released rows at the tail of their key, behind everything
// enqueued during the hold and in the order they were first queued.
func settleHeldRows(ctx context.Context, tx *sql.Tx, held []heldRow) (int64, error) {
	var released int64
	var err error
	for _, row := range held {
		switch {
		case row.objectDeleted, row.voteWithdrawn, row.restore && row.objectRemoved:
			_, err = tx.ExecContext(ctx, `
				UPDATE outbound_deliveries
				SET state = 'cancelled', held_reason = '', held_subject = '', updated_at = now()
				WHERE activity_id = $1 AND target_inbox = $2 AND state = 'held'`,
				row.activityID, row.targetInbox)
		case row.objectRemoved:
			err = rekeyHeld(ctx, tx, row, HeldReasonObject, row.objectATURI)
		case row.parentRemoved && !row.restore && !row.vote:
			err = rekeyHeld(ctx, tx, row, HeldReasonParent, row.parentATURI)
		default:
			_, err = tx.ExecContext(ctx, `
				UPDATE outbound_deliveries
				SET state = 'pending', held_reason = '', held_subject = '',
				    seq = nextval(pg_get_serial_sequence('outbound_deliveries', 'seq')),
				    updated_at = now()
				WHERE activity_id = $1 AND target_inbox = $2 AND state = 'held'`,
				row.activityID, row.targetInbox)
			if err == nil {
				released++
			}
		}
		if err != nil {
			return 0, fmt.Errorf("release held outbound_delivery %q -> %q: %w", row.activityID, row.targetInbox, err)
		}
	}
	return released, nil
}

// withdrawnVoteSQL is true for a delivery's activity a that is a Like/Dislike
// its vote no longer names as live: the outbound_votes row is gone (its Undo
// landed), retracting (a delete queued its Undo), undone (a purge retracted
// it), or names a newer activity (a re-cast replaced it).
const withdrawnVoteSQL = `a.kind IN ('Like', 'Dislike') AND NOT EXISTS (
		           SELECT 1 FROM outbound_votes v
		           WHERE v.actor_did = a.actor_did AND v.current_activity_id = a.activity_id
		             AND v.retraction_activity_id IS NULL
		             AND v.delivered_state <> '` + string(DeliveredStateUndone) + `')`

// CancelHeldVoteTx cancels, in tx, the held deliveries of the Like/Dislike
// activityID, which the caller's vote write withdraws (its Undo) or replaces
// (a re-cast). The caller writes the vote row and enqueues the Undo or the
// replacement in the same tx, after this: a release that locked the rows
// first commits before this statement proceeds, and the activity enqueued
// after it then sits behind the released row on the ordering key. Rows not
// held are left alone: a pending one goes out ahead of its Undo in order, and
// one the worker holds later is cancelled at its release (withdrawnVoteSQL).
func CancelHeldVoteTx(ctx context.Context, tx *sql.Tx, activityID string) error {
	if tx == nil {
		return errors.NewValidationError("tx", "must not be nil")
	}
	if activityID == "" {
		return errors.NewValidationError("activity_id", "must not be empty")
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE outbound_deliveries
		SET state = 'cancelled', held_reason = '', held_subject = '', updated_at = now()
		WHERE activity_id = $1 AND state = 'held'`, activityID); err != nil {
		return fmt.Errorf("cancel held outbound_deliveries of vote activity %q: %w", activityID, err)
	}
	return nil
}

// rekeyHeld keeps a held row held on the blocker that still stands.
func rekeyHeld(ctx context.Context, tx *sql.Tx, row heldRow, reason, subject string) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE outbound_deliveries
		SET held_reason = $3, held_subject = $4, updated_at = now()
		WHERE activity_id = $1 AND target_inbox = $2 AND state = 'held'
		  AND (held_reason <> $3 OR held_subject <> $4)`,
		row.activityID, row.targetInbox, reason, subject)
	if err != nil {
		return fmt.Errorf("re-key held outbound_delivery %q -> %q on %s %q: %w",
			row.activityID, row.targetInbox, reason, subject, err)
	}
	return nil
}

func checkHeldReason(reason, subject string) error {
	switch reason {
	case HeldReasonObject, HeldReasonParent, HeldReasonConsent, HeldReasonPause, HeldReasonCommunity:
	default:
		return errors.NewValidationError("held_reason", "unknown held reason "+reason)
	}
	if subject == "" {
		return errors.NewValidationError("held_subject", "must not be empty")
	}
	return nil
}

func (r *postgresOutboundDeliveries) HoldActivityTx(ctx context.Context, tx *sql.Tx, activityID, reason, subject string) (int64, error) {
	if tx == nil {
		return 0, errors.NewValidationError("tx", "must not be nil")
	}
	if err := checkHeldReason(reason, subject); err != nil {
		return 0, err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE outbound_deliveries
		SET state = 'held', held_reason = $2, held_subject = $3,
		    claimed_until = NULL, updated_at = now()
		WHERE activity_id = $1
		  AND state = 'pending'
		  AND (claimed_until IS NULL OR claimed_until <= CURRENT_TIMESTAMP)`+notHeldForSettlement,
		activityID, reason, subject)
	if err != nil {
		return 0, fmt.Errorf("hold outbound_deliveries of %q: %w", activityID, err)
	}
	held, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("hold outbound_deliveries of %q: rows affected: %w", activityID, err)
	}
	return held, nil
}

func (r *postgresOutboundDeliveries) HoldClaimedTx(ctx context.Context, tx *sql.Tx, activityID, targetInbox, reason, subject string, claimToken time.Time) (bool, bool, error) {
	if tx == nil {
		return false, false, errors.NewValidationError("tx", "must not be nil")
	}
	if err := checkHeldReason(reason, subject); err != nil {
		return false, false, err
	}
	// Fenced like every mark: only the current claim holder may hold the row,
	// and the claim's attempt is handed back as ReleaseParked does.
	query := `
		WITH updated AS (
			UPDATE outbound_deliveries
			SET state = 'held', held_reason = $3, held_subject = $4,
			    claimed_until = NULL, attempts = GREATEST(attempts - 1, 0),
			    updated_at = now()
			WHERE activity_id = $1 AND target_inbox = $2
			  AND state = 'pending'
			  AND claimed_until = $5
			RETURNING 1
		)
		SELECT
			EXISTS (SELECT 1 FROM outbound_deliveries WHERE activity_id = $1 AND target_inbox = $2),
			EXISTS (SELECT 1 FROM updated)`
	var exists, applied bool
	if err := tx.QueryRowContext(ctx, query, activityID, targetInbox, reason, subject, claimToken.UTC()).
		Scan(&exists, &applied); err != nil {
		return false, false, fmt.Errorf("hold outbound_delivery: %w", err)
	}
	return exists, applied, nil
}
