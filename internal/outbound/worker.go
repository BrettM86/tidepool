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
	"tidepool/internal/identity"
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
		// Held, NOT failed: it becomes re-eligible one short interval from now,
		// so it delivers within a beat of its parent being accepted, and the
		// poison budget does not advance (the causal wait is wall-clock-bounded
		// in causalStatus).
		return w.parkCausal(ctx, delivery, "parent_pending",
			"waiting for bridge-origin parent to be accepted", causalParkDelay)
	case causalLookupFailed:
		// The gate could not be READ. Holding is right — a store blip must not
		// poison somebody's reply — but on the worker's real backoff, not the
		// causal cadence: re-asking a database that just failed as fast as the
		// round trip allows is how a blip becomes an outage.
		return w.parkCausal(ctx, delivery, "parent_lookup_failed",
			"causal parent lookup failed; holding for retry", w.backoff(delivery.Attempts))
	// THE CLASS NAMES COME FROM store, and so do cross_authority and signer
	// below. The reconciliation sweep excludes exactly these from its
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
	case causalPoisonParentCancelled:
		return w.poison(ctx, delivery, store.PoisonClassParentCancelled,
			"every delivery of the parent was cancelled; descendant cannot land", 0)
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
		if identity.IsKeyUnsealable(err) {
			// The actor's key is well-formed ciphertext that opened under NO
			// configured KEK. Retrying re-reads the same bytes with the same
			// BRIDGE_KEK and gets the same answer, so the retry budget buys
			// nothing but hours of delay before poisoning under an excerpt that
			// says "message authentication failed" — which reads as corrupted
			// data and sends the operator to the database instead of the config.
			//
			// WHY THIS CANNOT FIRE DURING A SANCTIONED KEK ROTATION. The
			// rotation runbook has the bridge run with BRIDGE_KEK_PREVIOUS set
			// (NewCustodianWithPrevious opens under either key, so nothing is
			// unsealable), then re-seal every blob (identity.Reseal), and only
			// then unset the previous key. Reseal walks the two actor tables
			// FIRST and the plc-rotation row LAST, and refuses to exit zero
			// while any row is unmoved — so a worker that starts with the new
			// KEK alone can only have got past LoadOrCreateRotationKey (the
			// boot canary) on a database whose actor keys were already moved.
			// A process that reaches this line with an unsealable key is
			// therefore not mid-rotation: it is misconfigured, or the blob is
			// damaged. Both are terminal for this delivery.
			//
			// Terminal, not lost: a poisoned delivery is a dead-letter row an
			// operator redrives after fixing BRIDGE_KEK, which is the whole
			// point of failing loudly on the first attempt instead of quietly
			// on the last.
			w.logger.Error("actor signing key does not open under the configured BRIDGE_KEK; poisoning without retry",
				"activity", delivery.ActivityID, "actor", activity.ActorDID,
				"inbox", delivery.TargetInbox, "error", err)
			return w.poison(ctx, delivery, store.PoisonClassKEKMisconfigured, err.Error(), 0)
		}
		// Everything else a signer can fail with IS transient in the way the
		// retry budget assumes: an actor row that has not replicated yet, a
		// database blip. Those resolve on their own, and poisoning them would
		// turn seconds of lag into a permanent non-delivery.
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
			return w.duplicateDelivered(ctx, delivery, activity, he)
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

// duplicateDelivered records the received-activity dedupe as the success it is:
// Lemmy already holds this activity under our stable id, which is exactly what a
// redelivery after a crash is meant to discover.
//
// THE BODY IS LOGGED because this is the one branch that turns a 400 into a
// delivered row, and the delivered path stores no excerpt — so if the match ever
// fires on a rejection that merely resembles the dedupe, this line is the only
// record of what the peer actually said. Debug level: on a healthy bridge it
// fires only behind a crash-redelivery, and an operator chasing a wrong
// `delivered` is already turning the level up.
func (w *Worker) duplicateDelivered(ctx context.Context, delivery *store.OutboundDelivery, activity *store.OutboundActivity, he ap.HTTPError) error {
	w.logger.Debug("peer reports the activity was already received; classifying as delivered",
		"activity", delivery.ActivityID, "inbox", delivery.TargetInbox,
		"status", he.StatusCode, "body", he.Body)
	return w.deliverSuccess(ctx, delivery, activity, he.StatusCode)
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
		return w.duplicateDelivered(ctx, delivery, activity, he)
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

// poison marks the delivery permanently failed under its fencing token, and
// counts THE POISON THE FENCE ACCEPTED — the same rule countPark states for the
// park counter, one state over.
//
// MarkPoisoned carries the same (exists, applied) fence every other terminal
// mark does, and tidepool_outbound_poisoned is the number DEPLOY.md sends an
// operator to read as the queue's verdict (a redrive is decided off it). A stale
// worker whose lease lapsed poisons nothing — the fence says so — so counting
// its bounce puts a dead letter on the dashboard that does not exist anywhere in
// the table. The row is safe either way, which is precisely why the miscount
// would be the bounce's only trace, and why it is logged rather than passed over.
func (w *Worker) poison(ctx context.Context, delivery *store.OutboundDelivery, class, excerpt string, status int) error {
	_, applied, err := w.deliveries.MarkPoisoned(ctx, delivery.ActivityID, delivery.TargetInbox, class, excerpt, status, *delivery.ClaimedUntil)
	if err != nil {
		return fmt.Errorf("poison delivery %s: %w", delivery.ActivityID, err)
	}
	if !applied {
		w.logger.Warn("poison did not apply: claim lost or row terminal",
			"activity", delivery.ActivityID, "inbox", delivery.TargetInbox, "class", class)
		return nil
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
// poisons — park does not call poison, and no state here is terminal.
//
// It costs the delivery nothing either. ReleaseParked, not Release, is what
// settles a hold: it hands back the increment this claim's own ClaimNext charged,
// under the same fence, so a switch held for hours leaves the retry budget
// exactly where the operator found it (store/outbound_deliveries.go states why
// the fence is what makes that give-back safe). A held delivery reaches the
// attempt cap only through attempts it actually made.
func (w *Worker) park(ctx context.Context, delivery *store.OutboundDelivery, class, reason string) error {
	next := time.Now().Add(parkDelay)
	_, applied, err := w.deliveries.ReleaseParked(ctx, delivery.ActivityID, delivery.TargetInbox, class, reason, 0, next, *delivery.ClaimedUntil)
	if err != nil {
		return fmt.Errorf("park delivery %s: %w", delivery.ActivityID, err)
	}
	w.countPark(delivery, class, applied)
	return nil
}

// countPark records a park THE FENCE ACCEPTED, and only that one.
//
// tidepool_outbound_parked is how an operator sizes a held queue, and it is the
// only signal that can: a parked row still reads `pending`, exactly like one
// merely waiting its turn. A stale worker bouncing off a claim somebody else
// owns held nothing, so counting it inflates the one number an incident is read
// through.
//
// The bounce is logged rather than passed over in silence. The ROW is safe — the
// fence saw to that — but the wedged claim's own +1 stays on the ledger with
// nobody left to hand it back (the abandoned-claim leak in FOLLOWUPS.md), and
// this line is the only thing that connects an operator's "attempts climbing
// while the switch is held" to a lapsed lease rather than to the park path.
func (w *Worker) countPark(delivery *store.OutboundDelivery, class string, applied bool) {
	if !applied {
		w.logger.Warn("park did not apply: claim lost or row terminal",
			"activity", delivery.ActivityID, "inbox", delivery.TargetInbox, "class", class)
		return
	}
	metricParked.Add(1)
}

// causalParkDelay is how long a causally-held child waits before it can be
// re-claimed.
//
// SHORT, BECAUSE THE POINT OF THE HOLD IS TO END. In practice the parent is a
// lower-seq delivery on the same serial line and goes out first, so a child
// rarely cycles here at all; a second is small enough that a reply lands within
// a beat of its parent being accepted.
//
// NONZERO, BECAUSE now() IS A SPIN. ReleaseParked leaves the row pending, and a
// row scheduled at now() is claimable by the very next ClaimNext — while
// DeliverNext returns worked=true, so Run never sleeps. Any wait the parent does
// not promptly end therefore becomes a claim/park loop running at whatever rate
// the round trip allows, for as long as the wait lasts: two UPDATEs and two or
// three SELECTs per iteration, against the database the rest of the bridge is
// sharing. The cancelled-parent verdict above removes the case that could run
// that loop for the full six-hour budget; this delay is what keeps any FUTURE
// wait path from reintroducing it.
const causalParkDelay = time.Second

// parkCausal holds a causally-ineligible delivery for delay. Like park it never
// poisons, and like park it is attempt-neutral — which matters most here, since
// a child cycling through the hold would otherwise spend its whole retry budget
// waiting rather than trying.
//
// The wait's OUTCOME is still decided by the wall clock and not by the ledger:
// causalStatus poisons on the CausalWaitBudget deadline, so however many times a
// child cycles through this hold, what ends the wait is elapsed time.
func (w *Worker) parkCausal(ctx context.Context, delivery *store.OutboundDelivery, class, reason string, delay time.Duration) error {
	next := time.Now().Add(delay)
	_, applied, err := w.deliveries.ReleaseParked(ctx, delivery.ActivityID, delivery.TargetInbox, class, reason, 0, next, *delivery.ClaimedUntil)
	if err != nil {
		return fmt.Errorf("park (causal) delivery %s: %w", delivery.ActivityID, err)
	}
	w.countPark(delivery, class, applied)
	return nil
}

// causalStatus classifies a delivery's causal eligibility.
type causalStatus int

const (
	causalEligible causalStatus = iota
	causalWait
	// causalLookupFailed is a WAIT the store could not answer, kept apart from
	// causalWait because the two want different cadences: an ordinary wait is
	// ended by the parent landing (so it re-checks briskly), while a failed
	// lookup is ended by the database recovering (so it backs off).
	causalLookupFailed
	causalPoisonUnaccepted
	causalPoisonParent
	// causalPoisonParentCancelled is the THIRD way a parent goes terminal, and
	// the one the gate used to have no verdict for: cancelled. See
	// store.PoisonClassParentCancelled.
	causalPoisonParentCancelled
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
		return causalLookupFailed // transient: hold rather than poison on a lookup blip
	}
	if parent.IsAccepted() {
		return causalEligible
	}
	// Bridge-origin parent, not yet accepted. Decide against the child ONLY on
	// the child's ACTUAL parent deliveries — keyed on parent_at_uri, not
	// seq-ancestry, so an unrelated poisoned row on the same line does not
	// poison this child.
	//
	// TWO TERMINAL PARENTS, NOT ONE. A poisoned parent will never land; so will
	// one whose every delivery was CANCELLED, and that second case is what used
	// to fall through to the wait below — a wait for something already decided
	// against, which the delivery then spent its whole causal budget on.
	disposition, err := w.deliveries.ParentDeliveryDisposition(ctx, activity.ParentATURI, delivery.TargetInbox)
	if err != nil {
		w.logger.Error("parent-delivery disposition check failed", "error", err)
		return causalLookupFailed
	}
	switch disposition {
	case store.ParentDeliveryPoisoned:
		return causalPoisonParent
	case store.ParentDeliveryCancelled:
		return causalPoisonParentCancelled
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

// duplicateActivityPhrase is the received-activity dedupe, and it is the WHOLE
// phrase on purpose.
//
// This is the one classification that turns a rejection into a success, so it
// has to name the single rejection that IS one. Lemmy answers 400 with a family
// of slugs carrying the bare word "already" — banned_from_community,
// already_invalid, duplicate_title, "you have already been blocked" — and
// matching on that word alone read every one of them as a delivery:
//
//   - a Like the peer refused flipped outbound_votes.delivered_state, which is
//     an input to the score users are served and which nothing reconciles;
//   - stampAccepted opened the causal gate for an object that never landed, so
//     every child behind it was delivered into a rejection of its own;
//   - and the delivered path stores no excerpt, so the body that would have
//     shown an operator what happened was discarded.
//
// The phrase is what the peer actually says — the fake Lemmy in the outer
// acceptance test answers `{"error":"activity was already received"}`, which is
// the shape worker_test.go pins — and matching is done on a body lowercased with
// underscores folded to spaces, so a serialized enum (`already_received`) and
// the prose sentence are the same string here while none of the slugs above come
// near it.
const duplicateActivityPhrase = "already received"

// isDuplicate reports whether an HTTPError is that dedupe response.
func isDuplicate(he ap.HTTPError) bool {
	if he.StatusCode != http.StatusBadRequest {
		return false
	}
	normalized := strings.ReplaceAll(strings.ToLower(he.Body), "_", " ")
	return strings.Contains(normalized, duplicateActivityPhrase)
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
