package ingest

import (
	"context"
	"database/sql"
	"expvar"
	"fmt"
	"log/slog"
	"time"

	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// The reconciliation job (task 17e, PLAN.md decision 19): compare atproto state
// against outbound state, REPORT what disagrees, and change nothing.
//
// The prohibition is the design rather than a caveat on it. This sweep is the
// only thing that reads both sides, so if it healed and its rule were wrong,
// there would be no witness — it would silently rewrite the state it was
// measuring and every later sweep would agree with itself. Half of what it can
// see is genuinely unknowable from here (a poisoned delivery may or may not have
// reached the peer; a held settlement is a success that looks pending), and the
// actions available are the irreversible ones: re-sending, cancelling, deleting
// on an instance that does not un-delete. An operator reading a report can weigh
// that. A loop cannot.
//
// The shape mirrors FollowReconciler (reconcile.go): validated options, an
// exported Sweep the admin endpoint and tests drive synchronously, and a Run
// that loops over it.

// defaultDivergenceInterval is the sweep cadence when options leave it zero.
const defaultDivergenceInterval = 15 * time.Minute

// DefaultAcceptanceStaleAfter is how long a delivery may sit pending before its
// absence from the peer is worth an operator's attention.
//
// It is a named constant rather than a literal in a query because it is a
// POLICY — the line between "in flight" and "stuck" — and the person tuning it,
// or deciding whether the report is crying wolf, has to be able to find it.
//
// TWELVE HOURS, derived rather than picked. Two envelopes make a pending
// delivery legitimately old: the retry schedule (DefaultMaxDeliveryAttempts=8
// steps of DefaultBackoffBase=30s doubling to a one-hour cap, so roughly two to
// three hours before a failing delivery poisons) and the causal wait
// (DefaultCausalWaitBudget=6h, during which a reply sits pending for a parent
// that has not been accepted). A window inside either one reports the system
// working. Twelve hours clears both with room, and stays well inside "noticed
// the same day" — a queue that stopped moving this morning is in the report
// before the day ends.
const DefaultAcceptanceStaleAfter = 12 * time.Hour

// Divergence classes. The class is what an operator triages on, so it names the
// SHAPE of the disagreement rather than the query that found it.
const (
	// DivergencePersonaVoteEvent: an inbound vote_events row attributed to one
	// of our own personas. Expected count zero, forever — 17a's voter probe
	// refuses these before they are written — so one existing means the probe
	// was bypassed and the subject's tally counts our own vote twice: once as
	// this inbound event, once as the delivered outbound row 17b's reseed
	// subtracts from the origin's total.
	DivergencePersonaVoteEvent = "persona-vote-event"

	// DivergenceVoteRecastUndelivered: a peer is holding a vote this bridge no
	// longer claims. Re-casting a delivered vote resets the row to pending under
	// a new activity id; when that new delivery poisons, the peer keeps counting
	// the OLD vote while our ledger accounts for nothing — permanently, since
	// nothing re-drives a poisoned delivery and the reseed subtracts only
	// delivered rows.
	DivergenceVoteRecastUndelivered = "vote-recast-undelivered"

	// DivergenceDeliveryUnknownRefused / …Unanswered: a delivery we SENT and
	// never got confirmation for. Both names carry UNKNOWN because that is the
	// only true claim available: refused means the peer answered with a status
	// (evidence of non-application, not proof — a peer can apply and then fail
	// to answer), and unanswered means a transport failure that is silent about
	// everything. Neither may be folded into a class whose name asserts what the
	// peer holds.
	DivergenceDeliveryUnknownRefused    = "delivery-unknown-refused"
	DivergenceDeliveryUnknownUnanswered = "delivery-unknown-unanswered"

	// The undelivered-acceptance classes. A community's own repo says it
	// accepted the post — Coves shows it there — and the peer was never told.
	//
	// THREE CLASSES RATHER THAN ONE, because they are three different jobs. One
	// "undelivered" bucket would tell an operator how many posts are missing
	// from Lemmy and nothing about what to do about any of them, and it could
	// only ever be alerted on at the noise level of whichever kind is most
	// common.
	//
	// DivergenceAcceptanceCancelled: a DECISION took the delivery out of the
	// queue — a ban, an opt-out. Usually correct to leave exactly as it is; the
	// report exists so the resulting Coves-only post is visible rather than
	// silent.
	DivergenceAcceptanceCancelled = "acceptance-undelivered-cancelled"
	// DivergenceAcceptancePoisoned: the delivery FAILED. Redrivable, through an
	// operator surface that already exists (POST /admin/outbound/redrive).
	DivergenceAcceptancePoisoned = "acceptance-undelivered-poisoned"
	// DivergenceAcceptanceStale: still pending long past the window. Nothing is
	// wrong with the post — the QUEUE is not moving, which is an investigation
	// that starts at the worker rather than at the community.
	DivergenceAcceptanceStale = "acceptance-undelivered-stale"
)

// Divergence gauges. The "tidepool" prefix is load-bearing: scopedMetrics
// (follow.go) serves ONLY keys carrying it, so a gauge named without it is
// published to expvar and then filtered out of the endpoint — invisible in
// exactly the way a check that never runs is invisible.
const (
	MetricDivergencePersonaVoteEvents   = "tidepool_divergence_persona_vote_events"
	MetricDivergenceAcceptanceCancelled = "tidepool_divergence_acceptance_undelivered_cancelled"
	MetricDivergenceAcceptancePoisoned  = "tidepool_divergence_acceptance_undelivered_poisoned"
	MetricDivergenceAcceptanceStale     = "tidepool_divergence_acceptance_undelivered_stale"
	MetricDivergenceVoteRecast          = "tidepool_divergence_vote_recast_undelivered"
	MetricDivergenceUnknownRefused      = "tidepool_divergence_delivery_unknown_refused"
	MetricDivergenceUnknownUnanswered   = "tidepool_divergence_delivery_unknown_unanswered"
)

// divergenceUnswept is what a gauge reads before any sweep has completed.
//
// It is NEGATIVE for the same reason consume's dead-letter depth is: a count
// cannot be negative, so the value is unmistakable — where a 0 would claim the
// invariant is holding at exactly the moment nobody has checked. That
// distinction is the whole point of publishing these at all. The invariant this
// class reports is expected to hold forever, so the gauge spends its life at 0;
// if "never swept" also read 0, a broken schedule would look exactly like a
// healthy bridge, and the broken schedule is the one an operator needs to see.
const divergenceUnswept = -1

// The gauges are package-level and published at init, so the keys exist on
// /admin/metrics from process start rather than appearing only once something
// goes wrong. They are SET from each sweep's result (never added to): the
// number reports how many divergences stand right now, so it returns to zero by
// itself when a problem is resolved.
//
// expvar.Int, deliberately NOT expvar.Func. These are multi-table joins; behind
// a Func they would run on the /admin/metrics scrape — that is, on the endpoint
// an operator reads BECAUSE the system is already struggling.
//
// ONE GAUGE PER CLASS, for the same reason there are three classes: an operator
// alerts on them separately. A stale queue is a page, a cancelled acceptance is
// a note, and a single combined number can only be tuned for whichever of them
// is noisiest.
var (
	metricPersonaVoteEvents   = newDivergenceGauge(MetricDivergencePersonaVoteEvents)
	metricAcceptanceCancelled = newDivergenceGauge(MetricDivergenceAcceptanceCancelled)
	metricAcceptancePoisoned  = newDivergenceGauge(MetricDivergenceAcceptancePoisoned)
	metricAcceptanceStale     = newDivergenceGauge(MetricDivergenceAcceptanceStale)
	metricVoteRecast          = newDivergenceGauge(MetricDivergenceVoteRecast)
	metricUnknownRefused      = newDivergenceGauge(MetricDivergenceUnknownRefused)
	metricUnknownUnanswered   = newDivergenceGauge(MetricDivergenceUnknownUnanswered)
)

// divergenceGauges maps each class to the gauge that reports it, so publish
// cannot set one and forget another: a class added to the report without a
// gauge here fails to compile at the map literal rather than going unwatched.
var divergenceGauges = map[string]*expvar.Int{
	DivergencePersonaVoteEvent:          metricPersonaVoteEvents,
	DivergenceAcceptanceCancelled:       metricAcceptanceCancelled,
	DivergenceAcceptancePoisoned:        metricAcceptancePoisoned,
	DivergenceAcceptanceStale:           metricAcceptanceStale,
	DivergenceVoteRecastUndelivered:     metricVoteRecast,
	DivergenceDeliveryUnknownRefused:    metricUnknownRefused,
	DivergenceDeliveryUnknownUnanswered: metricUnknownUnanswered,
}

func newDivergenceGauge(name string) *expvar.Int {
	gauge := expvar.NewInt(name)
	gauge.Set(divergenceUnswept)
	return gauge
}

// DivergenceOptions configures a DivergenceReconciler.
type DivergenceOptions struct {
	// DB is the bridge database. BOTH sides of every comparison are local
	// (decision 19): a reconciler that needed a peer's state would be a
	// reconciler that talks to peers.
	DB *sql.DB
	// Interval is the sweep cadence for Run. Zero uses the default.
	Interval time.Duration
	// AcceptanceStaleAfter is how long a pending delivery may sit before its
	// acceptance is reported as stale. Zero uses DefaultAcceptanceStaleAfter.
	AcceptanceStaleAfter time.Duration
	// Logger receives sweep outcomes.
	Logger *slog.Logger
	// Divergences overrides the read-only store. It is nil in production and
	// constructed from DB; a test wraps it in a double that fails one read, to
	// prove a failed sweep publishes NOTHING rather than a zero that reads as
	// health. Same seam, same reason, as consume.Options' store overrides.
	Divergences store.Divergences
}

// DivergenceReconciler compares atproto state against outbound state and
// REPORTS what disagrees. It never writes: not to remote instances, and not to
// our own tables (PLAN.md decision 19).
//
// There is NO MUTEX, unlike FollowReconciler. That one serializes because two
// concurrent sweeps could each see a community as absent and subscribe it
// twice, and the racing EnsureCommunity calls can mint a permanent DID twice.
// Nothing here writes anything, so concurrent sweeps can only read the same rows
// and reach the same answer; a lock would buy nothing and would make the admin
// report queue behind a background pass.
type DivergenceReconciler struct {
	divergences store.Divergences
	interval    time.Duration
	staleAfter  time.Duration
	logger      *slog.Logger
}

// NewDivergenceReconciler validates options and builds the reconciler.
func NewDivergenceReconciler(opts DivergenceOptions) (*DivergenceReconciler, error) {
	if opts.DB == nil {
		return nil, errors.NewValidationError("db", "must not be nil")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	interval := opts.Interval
	if interval <= 0 {
		interval = defaultDivergenceInterval
	}
	staleAfter := opts.AcceptanceStaleAfter
	if staleAfter <= 0 {
		staleAfter = DefaultAcceptanceStaleAfter
	}
	return &DivergenceReconciler{
		// The read-only store, constructed here rather than injected: this
		// reconciler has exactly one dependency and it must be the one that
		// cannot write (store/divergence.go).
		divergences: orDefaultDivergences(opts.Divergences, store.NewDivergences(opts.DB)),
		interval:    interval,
		staleAfter:  staleAfter,
		logger:      logger,
	}, nil
}

// DivergenceEntry is one disagreement, named so an operator can act on it.
//
// Subject is what to look at — an at-uri, an activity id — because a class and a
// count alone give an operator a number they cannot investigate. Detail carries
// the why in words.
type DivergenceEntry struct {
	Class   string `json:"class"`
	Subject string `json:"subject"`
	Detail  string `json:"detail"`
}

// DivergenceReport is one sweep's findings: every entry, plus a count per class.
//
// Counts carries EVERY class the sweep checks, including the ones that found
// nothing. A report that listed only non-zero classes could not tell an operator
// which comparisons ran, so a class silently dropped from the sweep would read
// as a class with nothing to report.
type DivergenceReport struct {
	Entries []DivergenceEntry `json:"entries"`
	Counts  map[string]int    `json:"counts"`
	// Truncated reports that Entries was capped at MaxDivergenceEntries. The
	// COUNTS stay true when it is set — an operator must be able to see how big
	// the problem is even when the list of examples is bounded.
	Truncated bool `json:"truncated"`
}

// MaxDivergenceEntries caps the entries one report carries.
//
// A system that is diverging badly diverges in bulk — one broken community, one
// stopped queue — and an unbounded list is then a second outage: a response
// nobody can load, on the endpoint an operator reaches for BECAUSE something is
// wrong. The counts stay exact; only the examples are bounded.
const MaxDivergenceEntries = 500

// addEntry records one example, while there is room for one.
//
// The cap is applied HERE rather than by truncating a finished list, because a
// list that must first be built in full is not bounded at all: the sweep that
// hits this limit is the sweep running against a database with a stopped queue
// in it, and materialising every row of that before cutting it down would put
// the memory spike in exactly the process an operator is trying to keep alive.
//
// COUNTS ARE NOT TOUCHED HERE, deliberately: every caller counts what the
// comparison returned, not what fitted. A cap that also bounded the measurement
// would cap the number an operator escalates on at the size of a page, and
// "500" would then mean both "500" and "a catastrophe".
func (report *DivergenceReport) addEntry(entry DivergenceEntry) {
	if len(report.Entries) >= MaxDivergenceEntries {
		// SAY SO. A silently truncated list reads as the whole problem, and an
		// operator sizes an incident from what they can see.
		report.Truncated = true
		return
	}
	report.Entries = append(report.Entries, entry)
}

// newDivergenceReport is an EMPTY report with every class present at zero — and
// non-nil throughout, so the JSON is `{"entries":[],"counts":{...}}` rather than
// nulls a client has to special-case.
func newDivergenceReport() DivergenceReport {
	return DivergenceReport{
		Entries: []DivergenceEntry{},
		Counts: map[string]int{
			DivergencePersonaVoteEvent:          0,
			DivergenceAcceptanceCancelled:       0,
			DivergenceAcceptancePoisoned:        0,
			DivergenceAcceptanceStale:           0,
			DivergenceVoteRecastUndelivered:     0,
			DivergenceDeliveryUnknownRefused:    0,
			DivergenceDeliveryUnknownUnanswered: 0,
		},
	}
}

// Run sweeps once immediately, then on every interval tick until ctx is
// cancelled. A failed sweep is logged, never fatal: the next tick is the retry,
// and there is no state to leave half-applied because there is no state.
func (r *DivergenceReconciler) Run(ctx context.Context) {
	r.logger.Info("divergence reconciler started", "interval", r.interval)
	if _, err := r.Sweep(ctx); err != nil && ctx.Err() == nil {
		r.logger.Error("divergence reconciler: startup sweep failed", "error", err)
	}
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := r.Sweep(ctx); err != nil && ctx.Err() == nil {
				r.logger.Error("divergence reconciler: sweep failed", "error", err)
			}
		}
	}
}

// Sweep runs one comparison pass and returns what it found. Exported so the
// admin endpoint and tests can drive one synchronously, like
// FollowReconciler.Sweep.
//
// A READ FAILURE ABORTS THE WHOLE PASS. A report missing one class looks exactly
// like a report whose class found nothing, so returning the classes that
// happened to succeed would publish a claim about state nobody read — and the
// gauges would then be SET to a number that quietly excludes it. The error goes
// back instead, the gauges keep their previous values (see publish), and the
// operator sees a failed sweep rather than a clean one.
func (r *DivergenceReconciler) Sweep(ctx context.Context) (DivergenceReport, error) {
	report := newDivergenceReport()

	personaVotes, err := r.divergences.PersonaVoteEvents(ctx)
	if err != nil {
		return DivergenceReport{}, fmt.Errorf("divergence: read persona vote events: %w", err)
	}
	for _, vote := range personaVotes {
		report.addEntry(DivergenceEntry{
			Class:   DivergencePersonaVoteEvent,
			Subject: vote.ActivityID,
			Detail: fmt.Sprintf(
				"vote on %s is attributed to our own persona %s (%s), so that subject's tally "+
					"counts it twice: once as this inbound event and once as the delivered "+
					"outbound vote the reseed subtracts",
				vote.SubjectAPID, vote.VoterAPID, vote.ActorDID),
		})
	}
	// The COUNT is the comparison's length, never len(Entries): see addEntry.
	report.Counts[DivergencePersonaVoteEvent] = len(personaVotes)

	undelivered, err := r.divergences.UndeliveredAcceptances(ctx, r.staleAfter)
	if err != nil {
		return DivergenceReport{}, fmt.Errorf("divergence: read undelivered acceptances: %w", err)
	}
	for _, acceptance := range undelivered {
		class := acceptanceClass(acceptance.DeliveryState)
		if class == "" {
			// A delivery state this sweep has no class for. Logged rather than
			// filed under the nearest class: putting it in one would describe it
			// wrongly, and dropping it silently would make an unclassifiable
			// state look like a healthy one.
			r.logger.Warn("divergence: undelivered acceptance in an unclassified delivery state",
				"post", acceptance.PostURI, "state", acceptance.DeliveryState)
			continue
		}
		report.addEntry(DivergenceEntry{
			Class:   class,
			Subject: acceptance.PostURI,
			Detail: fmt.Sprintf(
				"community %s accepted this post and the peer was never told: its delivery is %s",
				acceptance.CommunityDID, acceptance.DeliveryState),
		})
		report.Counts[class]++
	}

	recasts, err := r.divergences.RecastDivergences(ctx)
	if err != nil {
		return DivergenceReport{}, fmt.Errorf("divergence: read recast divergences: %w", err)
	}
	for _, recast := range recasts {
		report.addEntry(DivergenceEntry{
			Class: DivergenceVoteRecastUndelivered,
			// The SUBJECT is what an operator checks the tally of; the delivered
			// activity id is what the peer is actually holding, and the only
			// handle a manual Undo could embed. Both are needed: the class and
			// the subject alone say a number is wrong without saying which vote
			// is making it wrong.
			Subject: recast.SubjectATURI,
			Detail: fmt.Sprintf(
				"the peer still holds vote activity %s cast by %s, which this bridge no longer "+
					"claims: the row was re-cast under a new activity id and that delivery never "+
					"landed, so nothing in the ledger accounts for the vote they are counting",
				recast.DeliveredActivityID, recast.ActorDID),
		})
	}
	report.Counts[DivergenceVoteRecastUndelivered] = len(recasts)

	unknown, err := r.divergences.UnknownDeliveryOutcomes(ctx)
	if err != nil {
		return DivergenceReport{}, fmt.Errorf("divergence: read unknown delivery outcomes: %w", err)
	}
	for _, outcome := range unknown {
		// TWO SUB-COUNTS, and the split is the only information these rows carry.
		// A peer that ANSWERED told us something a silent one did not — so the
		// two are never added together here, and the class an entry lands in is
		// decided by the stored fact of whether an answer arrived, not by
		// re-reading the status code (a 0 means nobody spoke, in both of the
		// spellings the delivery table stores that in).
		class := DivergenceDeliveryUnknownUnanswered
		detail := fmt.Sprintf(
			"we sent this %s to %s and got no answer at all (%s): it may never have arrived, or "+
				"it may have been applied and the response lost — %s is the instance to ask, "+
				"because we cannot tell from here",
			outcome.Kind, outcome.TargetInbox, outcome.LastErrorClass, outcome.TargetInbox)
		if outcome.Refused {
			class = DivergenceDeliveryUnknownRefused
			detail = fmt.Sprintf(
				"we sent this %s to %s and the peer answered %d (%s): that is evidence it was not "+
					"applied and not proof of it, since a peer can apply an activity and then "+
					"fail to respond",
				outcome.Kind, outcome.TargetInbox, outcome.LastStatusCode, outcome.LastErrorClass)
		}
		report.addEntry(DivergenceEntry{
			// The SUBJECT is the activity id: what was sent is the only handle
			// that exists here. There is no post at-uri to point at — these are
			// every kind of activity, and the acceptance classes are where a post
			// the peer never received is reported.
			Class:   class,
			Subject: outcome.ActivityID,
			Detail:  detail,
		})
		report.Counts[class]++
	}

	r.publish(report)
	return report, nil
}

// acceptanceClass maps a delivery state to the class an operator triages on.
// An unknown state yields "" — the caller reports that rather than guessing,
// because a state nobody has a response for is itself worth seeing.
func acceptanceClass(state store.DeliveryState) string {
	switch state {
	case store.DeliveryStateCancelled:
		return DivergenceAcceptanceCancelled
	case store.DeliveryStatePoisoned:
		return DivergenceAcceptancePoisoned
	case store.DeliveryStatePending:
		return DivergenceAcceptanceStale
	default:
		return ""
	}
}

// publish assigns the gauges from a COMPLETED sweep.
//
// It runs only on success, and that is the discipline the whole gauge design
// rests on: a failed sweep must leave the previous values standing rather than
// write a zero, because a zero written by a sweep that read nothing is a claim
// that everything is fine, made at the moment nobody could check.
func (r *DivergenceReconciler) publish(report DivergenceReport) {
	for class, gauge := range divergenceGauges {
		gauge.Set(int64(report.Counts[class]))
	}
	if len(report.Entries) > 0 {
		r.logger.Warn("divergence sweep found disagreements",
			"entries", len(report.Entries), "counts", report.Counts)
		return
	}
	r.logger.Debug("divergence sweep found nothing", "counts", report.Counts)
}

// orDefaultDivergences returns the override when a test supplies one.
func orDefaultDivergences(override, fallback store.Divergences) store.Divergences {
	if override != nil {
		return override
	}
	return fallback
}
