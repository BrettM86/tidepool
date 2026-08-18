package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
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
// under it) rather than per call — so there are exactly two finished statements
// in this package, neither of which can be handed a runtime string, and no
// future `%` written into this SQL can be mangled by a formatting pass.
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

// notHeldForSettlement is the term EVERY cancellation carries, and it is one
// constant rather than three because forgetting it is silent.
//
// A cancellation answers "this must not go out". A held delivery already WENT
// out: the peer holds the activity, and the only thing outstanding is our own
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
		  AND last_error_class IS DISTINCT FROM '` + DeliveryHeldForSettlement + `'`

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
func cancelOutwardForActor(ctx context.Context, ex execer, actorDID string) (int64, error) {
	query := `
		UPDATE outbound_deliveries d
		SET state = 'cancelled', claimed_until = NULL, updated_at = now()
		FROM outbound_activities a
		WHERE d.activity_id = a.activity_id
		  AND a.actor_did = $1
		  AND d.state = 'pending'` + notHeldForSettlement + `
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
		  AND d.state = 'pending'`+notHeldForSettlement, actorDID, orderingKey)
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

func (r *postgresOutboundDeliveries) RedrivePoisoned(ctx context.Context, activityID, orderingKey string) (int64, error) {
	// Empty filters pass through the NULLIF/COALESCE guard: a blank $2/$3 means
	// "any row", so the sweep can target one activity, one community, or all
	// poisoned deliveries. Reset attempts + next_attempt_at so a redriven
	// delivery gets a fresh budget and is immediately claimable.
	result, err := r.db.ExecContext(ctx, `
		UPDATE outbound_deliveries
		SET state = 'pending', attempts = 0, claimed_until = NULL, next_attempt_at = now(),
		    updated_at = now()
		WHERE state = 'poisoned'
		  AND ($1 = '' OR activity_id = $1)
		  AND ($2 = '' OR ordering_key = $2)`,
		activityID, orderingKey)
	if err != nil {
		return 0, fmt.Errorf("redrive poisoned deliveries: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("redrive poisoned deliveries: rows affected: %w", err)
	}
	return affected, nil
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
