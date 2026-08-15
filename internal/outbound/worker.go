package outbound

import (
	"context"
	"database/sql"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// DefaultLease bounds a delivery claim: long enough for a POST + retries, short
// enough that a crashed worker's delivery is re-claimable.
const DefaultLease = 2 * time.Minute

// WorkerOptions configures a Worker.
type WorkerOptions struct {
	// DB is the bridge database.
	DB *sql.DB
	// Activities / Deliveries are the split queue. Optional: nil from DB.
	Activities store.OutboundActivities
	Deliveries store.OutboundDeliveries
	// Objects gates causally (a bridge-origin parent must be accepted before
	// its child delivers) and is stamped accepted on delivery success. Optional.
	Objects store.OutboundObjects
	// Actors is the consent recheck at claim time: a disabled/paused actor's
	// create/update is cancelled, not delivered (delete/undo are exempt).
	// Optional: nil from DB.
	Actors store.APActors
	// Prefs is the federation opt-out half of the consent recheck (enabled=false
	// → an outward delivery is cancelled). Optional: nil from DB.
	Prefs store.FederationPrefs
	// Votes receives the delivery-success callbacks (decision 16): a Like/Dislike
	// success flips delivered_state; an Undo success clears the row. Optional:
	// nil from DB.
	Votes store.OutboundVotes
	// Signers yields the per-actor Signer each delivery is signed with.
	Signers SignerProvider
	// Inboxes re-resolves a rotated inbox once before poisoning.
	Inboxes InboxResolver
	// Sender POSTs the signed activity. *ap.Client satisfies it.
	Sender ActivitySender
	// Switches are the outbound kill switches + dry-run (decision 19). Nil means
	// AllowAll (everything enabled, no dry-run).
	Switches Switches
	// MaxAttempts caps delivery attempts before a retryable failure poisons.
	// Zero uses DefaultMaxDeliveryAttempts.
	MaxAttempts int
	// BackoffBase is the first retry-backoff step (doubles per attempt). Zero
	// uses DefaultBackoffBase; tests compress it.
	BackoffBase time.Duration
	// CausalWaitBudget is the WALL-CLOCK deadline a reply waits for its
	// bridge-origin parent to be accepted before poisoning parent_unaccepted.
	// It is measured from the delivery's creation, NOT its attempt count — a
	// causal wait must not consume the delivery-failure retry budget (a parent
	// legitimately takes minutes to be admitted). Zero uses
	// DefaultCausalWaitBudget.
	CausalWaitBudget time.Duration
	// Lease overrides DefaultLease.
	Lease time.Duration
	// Logger receives per-delivery outcomes. Nil uses slog.Default().
	Logger *slog.Logger
}

// Delivery retry bounds.
const (
	// DefaultMaxDeliveryAttempts caps attempts before a retryable failure
	// (transport/5xx/429) or an unaccepted parent poisons.
	DefaultMaxDeliveryAttempts = 8
	// DefaultBackoffBase is the first retry step; it doubles per attempt,
	// capped at one hour.
	DefaultBackoffBase = 30 * time.Second
	// DefaultCausalWaitBudget is the wall-clock window a reply waits for its
	// bridge-origin parent to be accepted before poisoning parent_unaccepted.
	DefaultCausalWaitBudget = 6 * time.Hour
)

// Worker claims one delivery at a time and carries it to a terminal state. It
// is the at-least-once engine: a crash between POST and MarkDelivered redelivers
// (Lemmy dedupes on our stable activity id, and its duplicate-activity response
// is classified DELIVERED, not poisoned).
type Worker struct {
	db               *sql.DB
	activities       store.OutboundActivities
	deliveries       store.OutboundDeliveries
	objects          store.OutboundObjects
	actors           store.APActors
	prefs            store.FederationPrefs
	votes            store.OutboundVotes
	signers          SignerProvider
	inboxes          InboxResolver
	sender           ActivitySender
	switches         Switches
	maxAttempts      int
	backoffBase      time.Duration
	causalWaitBudget time.Duration
	lease            time.Duration
	logger           *slog.Logger
}

// NewWorker wires a Worker.
func NewWorker(opts WorkerOptions) (*Worker, error) {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	lease := opts.Lease
	if lease <= 0 {
		lease = DefaultLease
	}
	activities := opts.Activities
	if activities == nil {
		activities = store.NewOutboundActivities(opts.DB)
	}
	deliveries := opts.Deliveries
	if deliveries == nil {
		deliveries = store.NewOutboundDeliveries(opts.DB)
	}
	objects := opts.Objects
	if objects == nil {
		objects = store.NewOutboundObjects(opts.DB)
	}
	prefs := opts.Prefs
	if prefs == nil {
		prefs = store.NewFederationPrefs(opts.DB)
	}
	votes := opts.Votes
	if votes == nil {
		votes = store.NewOutboundVotes(opts.DB)
	}
	var switches Switches = opts.Switches
	if switches == nil {
		switches = AllowAll{}
	}
	maxAttempts := opts.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = DefaultMaxDeliveryAttempts
	}
	backoffBase := opts.BackoffBase
	if backoffBase <= 0 {
		backoffBase = DefaultBackoffBase
	}
	causalWaitBudget := opts.CausalWaitBudget
	if causalWaitBudget <= 0 {
		causalWaitBudget = DefaultCausalWaitBudget
	}
	return &Worker{
		db:               opts.DB,
		activities:       activities,
		deliveries:       deliveries,
		objects:          objects,
		actors:           opts.Actors,
		prefs:            prefs,
		votes:            votes,
		signers:          opts.Signers,
		inboxes:          opts.Inboxes,
		sender:           opts.Sender,
		switches:         switches,
		maxAttempts:      maxAttempts,
		backoffBase:      backoffBase,
		causalWaitBudget: causalWaitBudget,
		lease:            lease,
		logger:           logger,
	}, nil
}

// Run drives DeliverNext in a loop until ctx is cancelled, sleeping idle when
// the queue drains. A per-delivery error is logged and the loop continues — one
// bad delivery must not stop the pipe.
func (w *Worker) Run(ctx context.Context, idle time.Duration) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		worked, err := w.DeliverNext(ctx)
		if err != nil {
			w.logger.Error("outbound delivery failed", "error", err)
		}
		if !worked {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(idle):
			}
		}
	}
}

// DeliverNext claims one processable delivery and carries it to a terminal
// state (delivered / poisoned / cancelled), parks it (kill switch / dry-run /
// causal wait), or reschedules it. It returns worked=true when a delivery was
// claimed and handled, worked=false (nil error) when the queue held nothing
// claimable. Only an infrastructure failure returns a non-nil error; every
// normal delivery outcome is a handled success.
func (w *Worker) DeliverNext(ctx context.Context) (worked bool, err error) {
	delivery, err := w.deliveries.ClaimNext(ctx, w.lease)
	if err != nil {
		if errors.IsNotFound(err) {
			return false, nil // empty queue
		}
		return false, fmt.Errorf("claim next delivery: %w", err)
	}
	if err := w.handle(ctx, delivery); err != nil {
		return true, err
	}
	return true, nil
}

// handle carries one claimed delivery to its outcome under its fencing token.
func (w *Worker) handle(ctx context.Context, delivery *store.OutboundDelivery) error {
	activity, err := w.activities.Get(ctx, delivery.ActivityID)
	if err != nil {
		return fmt.Errorf("load activity %s: %w", delivery.ActivityID, err)
	}

	// A delivery the peer ALREADY ACCEPTED, held because its local settlement
	// failed, resumes at the settlement — never at the wire. Re-POSTing would
	// re-send an activity the peer holds, and every gate below decides whether
	// to SEND: the kill switch, the causal wait and the consent recheck are all
	// answers to "should this go out?", asked after it already has. Cancelling
	// it here would strand the ledger it was held to settle.
	if delivery.LastErrorClass == deliveryLedgerUnsettled {
		return w.deliverSuccess(ctx, delivery, activity, statusOf(delivery))
	}

	// Kill switch (decision 19): an operator block PARKS the delivery — it stays
	// pending and resumes when the switch clears, never poisoned or cancelled.
	scope := DeliveryScope{
		ActorDID:      activity.ActorDID,
		CommunityAPID: delivery.OrderingKey,
		InboxHost:     hostOf(delivery.TargetInbox),
	}
	if !w.switches.OutboundAllowed(scope) {
		return w.park(ctx, delivery, "switch_parked", "outbound kill switch engaged")
	}
	if w.switches.DryRun() {
		w.logger.Info("dry-run: delivery translated but not POSTed",
			"activity", delivery.ActivityID, "inbox", delivery.TargetInbox)
		return w.park(ctx, delivery, "dry_run", "dry-run mode")
	}

	// Causal gate (decision 15): a reply must not be delivered before its
	// bridge-origin parent is accepted. Checked POST-claim on the row (not a
	// join in ClaimNext) so the loose-index-scan is untouched.
	switch w.causalStatus(ctx, delivery, activity) {
	case causalEligible:
		// fall through to consent + delivery
	case causalWait:
		// Held, NOT failed: keep it immediately re-eligible so it delivers the
		// instant its parent is accepted, and do not advance the poison budget
		// (the causal wait is wall-clock-bounded in causalStatus).
		return w.parkCausal(ctx, delivery, "parent_pending", "waiting for bridge-origin parent to be accepted")
	// THE CLASS NAMES COME FROM store, and so do cross_authority and signer
	// below. The reconciliation sweep excludes exactly these four from its
	// unknown-outcome report (store.neverReachedTheWireClasses): they carry
	// last_status_code 0 like a dial timeout does and mean the opposite — nothing
	// was sent, so the peer's state is not unknown, they simply do not have it.
	// Spelling them here as literals made that correspondence a comment; naming
	// the constants makes it the compiler's problem.
	case causalPoisonUnaccepted:
		return w.poison(ctx, delivery, store.PoisonClassParentUnaccepted,
			"causal wait budget exhausted; parent never accepted", 0)
	case causalPoisonParent:
		return w.poison(ctx, delivery, store.PoisonClassParentPoisoned,
			"parent delivery poisoned; descendant cannot land", 0)
	}

	// Consent recheck (retraction asymmetry): a Delete/Undo always goes out —
	// it is how an opted-out user takes down what is already federated. Outward
	// kinds are cancelled when the actor is disabled, paused, or opted out —
	// but ONLY this one claimed delivery, never the actor's pending retractions.
	if !isRetraction(activity.Kind) {
		blocked, err := w.consentBlocked(ctx, activity.ActorDID)
		if err != nil {
			return err
		}
		if blocked {
			_, applied, err := w.deliveries.CancelClaimed(ctx, delivery.ActivityID, delivery.TargetInbox, *delivery.ClaimedUntil)
			if err != nil {
				return fmt.Errorf("cancel delivery %s: %w", delivery.ActivityID, err)
			}
			if applied {
				metricCancelled.Add(1)
			}
			return nil
		}
	}

	// Defense in depth: never sign a POST to an inbox that is not same-authority
	// with the target community. The resolver already refuses a cross-authority
	// inbox at enqueue time; this catches a tampered or legacy stored target.
	if !ap.SameAuthority(delivery.OrderingKey, delivery.TargetInbox) {
		return w.poison(ctx, delivery, store.PoisonClassCrossAuthority,
			"target inbox is not same-authority with the community; refusing to deliver", 0)
	}

	return w.deliver(ctx, delivery, activity)
}

// deliver signs and POSTs the stored payload verbatim, then classifies the
// outcome.
func (w *Worker) deliver(ctx context.Context, delivery *store.OutboundDelivery, activity *store.OutboundActivity) error {
	signer, err := w.signers.SignerFor(ctx, activity.ActorDID)
	if err != nil {
		// A signer that cannot be resolved right now is transient (a KEK blip,
		// a not-yet-replicated actor): retry rather than poison.
		return w.releaseOrPoison(ctx, delivery, store.PoisonClassSigner, err.Error(), 0)
	}

	err = w.sender.SendActivityAs(ctx, signer, delivery.TargetInbox, json.RawMessage(activity.Payload))
	return w.classify(ctx, delivery, activity, signer, err)
}

// classify maps a POST outcome onto the retry taxonomy.
func (w *Worker) classify(ctx context.Context, delivery *store.OutboundDelivery, activity *store.OutboundActivity, signer *ap.Signer, err error) error {
	if err == nil {
		return w.deliverSuccess(ctx, delivery, activity, http.StatusAccepted)
	}

	var he ap.HTTPError
	if stderrors.As(err, &he) {
		switch {
		case isDuplicate(he):
			// Lemmy's received_activity dedupe (400 + "already received") is a
			// SUCCESS by our stable id: a redelivery after a crash is expected.
			return w.deliverSuccess(ctx, delivery, activity, he.StatusCode)
		case he.StatusCode == http.StatusNotFound ||
			he.StatusCode == http.StatusGone:
			// 404/410 is an endpoint-GONE signal: re-resolve the inbox once
			// before poisoning (a rotation must not become a poison).
			return w.rotateInbox(ctx, delivery, activity, signer, he)
		case he.StatusCode == http.StatusUnauthorized ||
			he.StatusCode == http.StatusRequestTimeout ||
			he.StatusCode == http.StatusTooManyRequests ||
			he.StatusCode >= 500:
			// 401 is an AUTH problem (our signature, their secure mode), not an
			// endpoint rotation: retry it, don't burn the single re-resolve.
			return w.releaseOrPoison(ctx, delivery, classForStatus(he.StatusCode), he.Body, he.StatusCode)
		default:
			// Other 4xx: a genuine rejection. Retried on a small budget, then
			// poisoned.
			return w.releaseOrPoison(ctx, delivery, "4xx", he.Body, he.StatusCode)
		}
	}
	// Transport failure (dial/TLS/timeout): transient.
	return w.releaseOrPoison(ctx, delivery, "transport", err.Error(), 0)
}

// rotateInbox handles a 401/404/410: re-resolve the community's inbox ONCE
// bypassing the cache (an endpoint rotation must not become a poison), retry,
// then deliver-or-poison.
func (w *Worker) rotateInbox(ctx context.Context, delivery *store.OutboundDelivery, activity *store.OutboundActivity, signer *ap.Signer, first ap.HTTPError) error {
	fresh, ok := w.inboxes.(FreshInboxResolver)
	if !ok {
		return w.releaseOrPoison(ctx, delivery, "inbox_gone", first.Body, first.StatusCode)
	}
	inbox, err := fresh.ResolveInboxFresh(ctx, delivery.OrderingKey)
	if err != nil {
		return w.releaseOrPoison(ctx, delivery, "inbox_resolve", err.Error(), first.StatusCode)
	}

	err = w.sender.SendActivityAs(ctx, signer, inbox, json.RawMessage(activity.Payload))
	if err == nil {
		return w.deliverSuccess(ctx, delivery, activity, http.StatusAccepted)
	}
	var he ap.HTTPError
	if stderrors.As(err, &he) && isDuplicate(he) {
		return w.deliverSuccess(ctx, delivery, activity, he.StatusCode)
	}
	// Still bad after the single re-resolve: the endpoint is genuinely gone.
	status := first.StatusCode
	if stderrors.As(err, &he) {
		status = he.StatusCode
	}
	return w.poison(ctx, delivery, "inbox_gone", "inbox still unreachable after re-resolution", status)
}

// deliverSuccess settles everything a successful POST implies, and marks the
// delivery delivered LAST.
//
// The order is the invariant. Marking delivered is TERMINAL — the queue never
// re-claims that row — so every write that happens after it is a write nothing
// will ever retry. Two of them matter:
//
//   - accepted_at (the causal gate): a parent observed delivered with its stamp
//     unset strands every child forever;
//   - the vote ledger: since task 17b, outbound_votes is an INPUT to the number
//     users read, so a delivery left terminal beside a 'pending' row over-counts
//     that subject permanently, and beside a stale 'delivered' row under-counts
//     it permanently. Nothing reconciles either — SeedAggregates only runs
//     behind a backfill.
//
// So the reachable states are (¬settled,¬delivered) and (settled,delivered),
// never the forbidden (¬settled,delivered). Both settlements are idempotent, so
// a retry that repeats them costs nothing.
//
// When a settlement fails, the delivery is HELD FOR SETTLEMENT rather than
// failed: see settleLater. Failing it would re-POST an activity the peer has
// already accepted; poisoning it would make the disagreement permanent, which
// is the whole bug.
func (w *Worker) deliverSuccess(ctx context.Context, delivery *store.OutboundDelivery, activity *store.OutboundActivity, status int) error {
	if err := w.stampAccepted(ctx, activity); err != nil {
		return w.settleLater(ctx, delivery, status,
			fmt.Errorf("stamp accepted for %s: %w", delivery.ActivityID, err))
	}
	if err := w.voteCallback(ctx, activity); err != nil {
		return w.settleLater(ctx, delivery, status, err)
	}
	_, applied, err := w.deliveries.MarkDelivered(ctx, delivery.ActivityID, delivery.TargetInbox, status, *delivery.ClaimedUntil)
	if err != nil {
		return fmt.Errorf("mark delivered %s: %w", delivery.ActivityID, err)
	}
	if !applied {
		// A stale claim: our lease lapsed mid-POST and another worker owns the
		// row now. The settlements above already ran — they are keyed on the
		// ACTIVITY, not on the claim, so the ledger is correct whichever worker
		// loses the fencing race, and the winner's redelivery repeats them
		// idempotently.
		return nil
	}
	metricDelivered.Add(1)
	return nil
}

// settleLater holds a delivery whose POST SUCCEEDED but whose settlement did
// not. The row stays PENDING — non-terminal, so the queue will come back to it
// — carrying deliveryLedgerUnsettled as its outcome class, which is what tells
// the next claim to resume at the settlement instead of at the wire.
//
// It never poisons. A poisoned delivery is terminal, and terminal-with-unsettled
// is exactly the permanent disagreement this exists to prevent; the failure here
// is a LOCAL write, so retrying is both safe and the only thing that can help.
// The backoff still spaces the retries (production's base is 30s), so a
// persistently broken local write does not spin the worker.
func (w *Worker) settleLater(ctx context.Context, delivery *store.OutboundDelivery, status int, cause error) error {
	w.logger.Warn("delivery accepted by the peer but not yet settled locally; holding for settlement",
		"activity", delivery.ActivityID, "inbox", delivery.TargetInbox,
		"attempts", delivery.Attempts, "error", cause)
	next := time.Now().Add(w.backoff(delivery.Attempts))
	if _, _, err := w.deliveries.Release(ctx, delivery.ActivityID, delivery.TargetInbox,
		deliveryLedgerUnsettled, cause.Error(), status, next, *delivery.ClaimedUntil); err != nil {
		return fmt.Errorf("hold %s for settlement: %w", delivery.ActivityID, err)
	}
	return nil
}

// stampAccepted opens the causal gate for this object's children: on a
// successful Create/Update, the object it federated is now accepted by its
// community. A NotFound (no outbound_objects row — a comment we never persisted,
// a vote) is not a failure and returns nil; a real store error is propagated so
// deliverSuccess withholds the delivered mark until the stamp can commit.
func (w *Worker) stampAccepted(ctx context.Context, activity *store.OutboundActivity) error {
	if activity.Kind != "Create" && activity.Kind != "Update" {
		return nil
	}
	atURI := objectATURIFromPayload(activity.Payload)
	if atURI == "" {
		return nil
	}
	if err := w.objects.SetAccepted(ctx, atURI); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}
	return nil
}

// voteCallback applies decision-16 delivery callbacks: a Like/Dislike success
// flips outbound_votes.delivered_state; an Undo success clears the row.
func (w *Worker) voteCallback(ctx context.Context, activity *store.OutboundActivity) error {
	switch activity.Kind {
	case "Like", "Dislike":
		vote, err := w.votes.GetByActivityID(ctx, activity.ActivityID)
		if errors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("resolve vote for %s: %w", activity.ActivityID, err)
		}
		if err := w.votes.SetDeliveredState(ctx, vote.VoteATURI, store.DeliveredStateDelivered); err != nil {
			return fmt.Errorf("flip vote %s delivered: %w", vote.VoteATURI, err)
		}
	case "Undo":
		innerID := innerObjectID(activity.Payload)
		if innerID == "" {
			return nil
		}
		vote, err := w.votes.GetByActivityID(ctx, innerID)
		if errors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("resolve vote for undo %s: %w", innerID, err)
		}
		if err := w.votes.Delete(ctx, vote.VoteATURI); err != nil {
			return fmt.Errorf("clear vote %s on undo: %w", vote.VoteATURI, err)
		}
	}
	return nil
}

// releaseOrPoison reschedules a retryable failure with backoff, or poisons it
// once the attempt cap is reached (the claim already bumped attempts).
func (w *Worker) releaseOrPoison(ctx context.Context, delivery *store.OutboundDelivery, class, excerpt string, status int) error {
	if delivery.Attempts >= w.maxAttempts {
		return w.poison(ctx, delivery, class, excerpt, status)
	}
	next := time.Now().Add(w.backoff(delivery.Attempts))
	_, _, err := w.deliveries.Release(ctx, delivery.ActivityID, delivery.TargetInbox, class, excerpt, status, next, *delivery.ClaimedUntil)
	if err != nil {
		return fmt.Errorf("release delivery %s: %w", delivery.ActivityID, err)
	}
	return nil
}

// poison marks the delivery permanently failed under its fencing token.
func (w *Worker) poison(ctx context.Context, delivery *store.OutboundDelivery, class, excerpt string, status int) error {
	_, _, err := w.deliveries.MarkPoisoned(ctx, delivery.ActivityID, delivery.TargetInbox, class, excerpt, status, *delivery.ClaimedUntil)
	if err != nil {
		return fmt.Errorf("poison delivery %s: %w", delivery.ActivityID, err)
	}
	metricPoisoned.Add(1)
	return nil
}

// deliveryLedgerUnsettled labels a delivery the peer accepted whose LOCAL
// settlement (the causal stamp, the vote ledger) has not committed yet. It is
// an outcome class, not an error class — park and parkCausal already use the
// same column for held-not-failed states — and it is the durable fact that lets
// a retry finish the job without repeating the POST.
//
// It is the STORE's constant, not a copy: every cancellation excludes rows
// carrying this class, and a second spelling here would let the resume path and
// the cancellations disagree about which rows are held — the disagreement being
// silent, and fatal in the direction where a cancel wins.
const deliveryLedgerUnsettled = store.DeliveryHeldForSettlement

// statusOf is the status a held delivery was accepted with, so its settlement
// records the same outcome the wire actually produced.
func statusOf(delivery *store.OutboundDelivery) int {
	if delivery.LastStatusCode == nil {
		return http.StatusAccepted
	}
	return *delivery.LastStatusCode
}

// parkDelay is how long a kill-switched or dry-run delivery waits before it can
// be re-claimed: long enough that a parked row does not spin the worker in a hot
// loop, short enough that clearing the switch resumes delivery promptly.
const parkDelay = 5 * time.Second

// park holds a kill-switched or dry-run delivery: it stays pending, scheduled a
// REAL delay into the future so it is not instantly re-claimable, and never
// poisons. Release does not touch the attempt counter, so a park is not a
// failure and does not itself advance the poison budget.
func (w *Worker) park(ctx context.Context, delivery *store.OutboundDelivery, class, reason string) error {
	next := time.Now().Add(parkDelay)
	_, _, err := w.deliveries.Release(ctx, delivery.ActivityID, delivery.TargetInbox, class, reason, 0, next, *delivery.ClaimedUntil)
	if err != nil {
		return fmt.Errorf("park delivery %s: %w", delivery.ActivityID, err)
	}
	metricParked.Add(1)
	return nil
}

// parkCausal holds a causally-ineligible delivery WITHOUT a future delay: a held
// child must become claimable the instant its bridge-origin parent is accepted
// (in practice the parent, a lower-seq delivery on the same serial line, is
// delivered first, so this rarely re-fires). Like park it never poisons and does
// not advance the poison budget; the causal wait is bounded by wall clock.
func (w *Worker) parkCausal(ctx context.Context, delivery *store.OutboundDelivery, class, reason string) error {
	_, _, err := w.deliveries.Release(ctx, delivery.ActivityID, delivery.TargetInbox, class, reason, 0, time.Now(), *delivery.ClaimedUntil)
	if err != nil {
		return fmt.Errorf("park (causal) delivery %s: %w", delivery.ActivityID, err)
	}
	metricParked.Add(1)
	return nil
}

// causalStatus classifies a delivery's causal eligibility.
type causalStatus int

const (
	causalEligible causalStatus = iota
	causalWait
	causalPoisonUnaccepted
	causalPoisonParent
)

func (w *Worker) causalStatus(ctx context.Context, delivery *store.OutboundDelivery, activity *store.OutboundActivity) causalStatus {
	if activity.ParentATURI == "" {
		return causalEligible
	}
	parent, err := w.objects.GetByATURI(ctx, activity.ParentATURI)
	if errors.IsNotFound(err) {
		// No outbound_objects row: the parent is a FEDIVERSE-origin object
		// (already on the peer), so the reply is always eligible. This is the
		// crux — a naive gate would poison every reply to a Lemmy post.
		return causalEligible
	}
	if err != nil {
		w.logger.Error("causal parent lookup failed", "parent", activity.ParentATURI, "error", err)
		return causalWait // transient: hold rather than poison on a lookup blip
	}
	if parent.IsAccepted() {
		return causalEligible
	}
	// Bridge-origin parent, not yet accepted. Poison ONLY if the child's ACTUAL
	// parent delivery is poisoned (it will never land) — keyed on parent_at_uri,
	// not seq-ancestry, so an unrelated poisoned row on the same line does not
	// poison this child.
	poisoned, err := w.deliveries.ParentDeliveryPoisoned(ctx, activity.ParentATURI, delivery.TargetInbox)
	if err != nil {
		w.logger.Error("parent-delivery poisoned check failed", "error", err)
		return causalWait
	}
	if poisoned {
		return causalPoisonParent
	}
	// Otherwise the parent is merely pending: WAIT, bounded by WALL CLOCK from
	// the delivery's creation — never by the attempt count, so a parent that
	// legitimately takes minutes is not poisoned just because the child was
	// claimed a few times.
	if time.Since(delivery.CreatedAt) >= w.causalWaitBudget {
		return causalPoisonUnaccepted
	}
	return causalWait
}

// consentBlocked reports whether an outward delivery for actorDID must be
// cancelled: the actor is disabled, delivery-paused, or has a federation
// opt-out. A missing pref MEANS default-on (not blocked).
func (w *Worker) consentBlocked(ctx context.Context, actorDID string) (bool, error) {
	actor, err := w.actors.GetByDID(ctx, actorDID)
	if err != nil {
		if errors.IsNotFound(err) {
			return true, nil // no actor to sign as: cannot deliver
		}
		return false, fmt.Errorf("load actor %s: %w", actorDID, err)
	}
	if !actor.Enabled || actor.DeliveryPaused {
		return true, nil
	}
	pref, err := w.prefs.Get(ctx, actorDID)
	if errors.IsNotFound(err) {
		return false, nil // default-on
	}
	if err != nil {
		return false, fmt.Errorf("load federation pref %s: %w", actorDID, err)
	}
	return !pref.Enabled, nil
}

// backoff is the retry delay for a given attempt count: backoffBase doubled per
// attempt, capped at one hour.
func (w *Worker) backoff(attempts int) time.Duration {
	const maxBackoff = time.Hour
	d := w.backoffBase
	for i := 1; i < attempts && d < maxBackoff; i++ {
		d *= 2
	}
	if d > maxBackoff {
		d = maxBackoff
	}
	return d
}

// isRetraction reports whether a kind is a take-down that is exempt from the
// consent recheck (a Delete or a vote Undo).
//
// The list is store.RetractionKinds, not a local copy: the QUEUE exempts exactly
// these kinds from a consent cancellation, and if the two definitions drifted,
// an activity this worker would still deliver could be cancelled out from under
// it — or the reverse.
func isRetraction(kind string) bool { return slices.Contains(store.RetractionKinds, kind) }

// isDuplicate reports whether an HTTPError is Lemmy's duplicate-activity
// response (a 400 whose body reports the activity was already received).
func isDuplicate(he ap.HTTPError) bool {
	return he.StatusCode == http.StatusBadRequest && strings.Contains(strings.ToLower(he.Body), "already")
}

// classForStatus labels a retryable HTTP status for the retry taxonomy.
func classForStatus(status int) string {
	switch {
	case status == http.StatusUnauthorized:
		return "unauthorized"
	case status == http.StatusRequestTimeout:
		return "timeout"
	case status == http.StatusTooManyRequests:
		return "rate_limited"
	case status >= 500:
		return "5xx"
	default:
		return "transient"
	}
}

// hostOf returns the lowercase host of a URL, or "" if it does not parse.
func hostOf(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return strings.ToLower(parsed.Hostname())
}

// objectATURIFromPayload recovers the at-uri of the object a Create/Update
// federated, from the object's id URL (/ap/object/{did}/{collection}/{rkey}), so
// its acceptance can be stamped. Returns "" when the payload carries no such id.
func objectATURIFromPayload(payload []byte) string {
	var activity struct {
		Object struct {
			ID string `json:"id"`
		} `json:"object"`
	}
	if err := json.Unmarshal(payload, &activity); err != nil {
		return ""
	}
	const marker = "/ap/object/"
	idx := strings.Index(activity.Object.ID, marker)
	if idx < 0 {
		return ""
	}
	suffix := activity.Object.ID[idx+len(marker):]
	if suffix == "" {
		return ""
	}
	return "at://" + suffix
}

// innerObjectID reads the embedded inner object's id from an Undo payload (the
// Like/Dislike activity id the Undo withdraws), which resolves the vote row.
func innerObjectID(payload []byte) string {
	var activity struct {
		Object struct {
			ID string `json:"id"`
		} `json:"object"`
	}
	if err := json.Unmarshal(payload, &activity); err != nil {
		return ""
	}
	return activity.Object.ID
}
