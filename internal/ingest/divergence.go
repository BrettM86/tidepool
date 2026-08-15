package ingest

import (
	"context"
	"database/sql"
	"expvar"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
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

// divergenceSweepTimeout bounds ONE pass, and it exists because of the pool
// rather than because of the clock.
//
// Every comparison here is a multi-table join taken from the SHARED connection
// pool — the same 25 connections serving inbound ingestion and the delivery
// worker — and there is no statement_timeout anywhere in this codebase. A
// pathological sweep with no deadline therefore does not merely run late: it
// holds pool connections while it runs, and the sweep is slowest exactly when
// the database is struggling, which is when federation most needs those
// connections. GET /admin/divergence makes that reachable from outside on an
// unrated GET, so an operator refreshing the report during an incident can
// deepen it.
//
// GENEROUS, because this is a background comparison and not a scrape: the
// precedent is consume's deadLetterScrapeTimeout (internal/consume/metrics.go),
// three seconds for a read taken on the metrics handler, and the reasoning
// inverts here. Nobody is blocked on a sweep, a real one on a large database
// legitimately takes seconds to a minute, and a deadline that fires on a merely
// slow pass would report failures that are not divergences. Two minutes is well
// past any healthy pass and well inside the 15-minute cadence, so a sweep can
// never queue behind its own predecessor.
//
// WHAT IT BUYS is that the failure is LOUD and BOUNDED: the pass aborts, the
// failure counter moves, the age gauge starts climbing, and the previous gauges
// keep standing — the same treatment any other failed read gets — instead of a
// query pinning a connection for as long as the database will let it.
const divergenceSweepTimeout = 2 * time.Minute

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
	// longer claims — the peer accepted the activity, and nothing in our
	// accounting covers it: no live delivered vote row names it, no delivered
	// Undo withdraws it, and no later delivered vote supersedes it.
	//
	// The NAME says re-cast because that is the common cause, but the class is
	// deliberately wider than its name and THREE populations reach it, with
	// different remedies: a re-cast whose new delivery poisoned; an Undo that
	// poisoned (the withdrawal failed, not the vote); and purge residue, where
	// the destructive opt-out tier marks a vote `undone` at decision time while
	// keeping current_activity_id. Whichever it is, it is permanent — nothing
	// re-drives a poisoned delivery, and the reseed subtracts only delivered
	// rows. The Detail states only what the row proves and leaves the cause to
	// the operator's reading of the ledger; do not narrow it back to a re-cast.
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

	// The FRESHNESS pair. Every gauge above reports a count that a sweep last
	// wrote, and a sweep that has stopped running leaves all seven standing at
	// their final values — a low, stable, entirely healthy-looking set. These two
	// are how an operator tells "nothing is diverging" from "nothing is
	// measuring": the age says when the standing numbers were established, and
	// the failure count says whether the sweep has been trying and losing.
	MetricDivergenceSweepAgeSeconds = "tidepool_divergence_sweep_age_seconds"
	MetricDivergenceSweepFailures   = "tidepool_divergence_sweep_failures"
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

// divergenceGauges maps each class to the gauge that reports it, and is the
// SINGLE DECLARATION of the sweep's class vocabulary: newDivergenceReport
// derives the report's Counts keys from this map, so the two key sets are one
// set by construction and cannot drift.
//
// They used to be two literals, under a comment claiming a class added without
// a gauge "fails to compile at the map literal". IT DOES NOT — a
// map[string]*expvar.Int literal is not exhaustive over any key set, nothing in
// Go compares two literals against each other, and a missing key read out of
// Counts yields 0, which is HEALTH. The claim was worse than the hole it
// described, because it told every later reader not to check. What actually
// holds the two together now is this derivation, and
// TestEveryDivergenceClassHasAGauge pins it against the report a sweep builds.
// Adding a class means adding it HERE and nowhere else.
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

// metricSweepFailures counts sweeps that aborted on a read error.
//
// It starts at 0 and 0 is an HONEST zero here, unlike the class gauges: a
// process that has never failed a sweep has genuinely failed none. What a
// counter alone cannot say is whether any sweep has RUN — zero failures is also
// what a scheduler that never started reports — which is what the age gauge
// below is for. The two are read together or neither means anything.
var metricSweepFailures = expvar.NewInt(MetricDivergenceSweepFailures)

// The publication guard. It protects the seven process-global gauges above, NOT
// the database reads (see DivergenceReconciler for why those need nothing).
var (
	// divergenceSweepGeneration numbers sweeps in the order they START, across
	// every reconciler in the process, because the gauges they publish to are
	// process-global too.
	divergenceSweepGeneration atomic.Uint64

	divergencePublishMu sync.Mutex
	// divergencePublishedGeneration is the generation of the newest sweep whose
	// numbers are STANDING in the gauges. A sweep whose generation is not above
	// it started earlier than the values on display and is dropped rather than
	// published. Guarded by divergencePublishMu.
	divergencePublishedGeneration uint64
	// divergenceLastPublished is when those standing numbers were established,
	// and the zero value means no sweep has ever published. It moves only when a
	// publication actually lands: a dropped stale sweep succeeded at reading, but
	// its numbers are not the ones being displayed, so counting it as freshness
	// would date the gauges to a pass whose results were thrown away. Guarded by
	// divergencePublishMu.
	divergenceLastPublished time.Time
)

// The age gauge, and the ONE place expvar.Func is right in this file.
//
// The seven class gauges are deliberately pushed rather than pulled because
// they are multi-table joins and a Func would run them on the scrape — on the
// endpoint an operator reads BECAUSE the system is struggling. This one reads a
// stored timestamp and subtracts, which costs nothing, and it must be pulled:
// an age pushed at publication time would be the one number that stops updating
// at exactly the moment it becomes the number that matters. That is
// consume.PublishMetrics' reasoning about cursor age (internal/consume/
// metrics.go), applied to the failure it does not cover — there, a value goes
// stale when the consumer stalls; here, ALL SEVEN GAUGES go stale together when
// the sweep stops, and they freeze at plausible, low, healthy-looking values. A
// permissions change or a statement timeout at 04:00 leaves them saying "0
// divergences" for the life of the process while the queue stops entirely.
//
// It reports divergenceUnswept before the first successful publication, for the
// same reason the class gauges do: a 0 there would say "swept just now".
func init() {
	expvar.Publish(MetricDivergenceSweepAgeSeconds, expvar.Func(func() any {
		divergencePublishMu.Lock()
		published := divergenceLastPublished
		divergencePublishMu.Unlock()
		if published.IsZero() {
			return float64(divergenceUnswept)
		}
		age := time.Since(published).Seconds()
		if age < 0 {
			// Clock movement, not a sweep from the future.
			return 0.0
		}
		return age
	}))
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
// THE READS ARE UNSERIALIZED, unlike FollowReconciler's. That one holds a lock
// across its whole pass because two concurrent sweeps could each see a community
// as absent and subscribe it twice, and the racing EnsureCommunity calls can
// mint a permanent DID twice. Nothing here writes anything, so concurrent
// passes can only read the same rows and reach the same answer — and a lock
// spanning the reads would make an operator's on-demand report queue behind a
// background pass, which is the request that is most urgent.
//
// THE PUBLICATION IS SERIALIZED, and that is a different hazard entirely: the
// seven gauges are process-global expvar.Ints shared by the Run loop and every
// GET /admin/divergence. Two passes assigning them interleave in randomized map
// order, so a scrape could read a set woven from two sweeps — numbers that never
// described one moment. Worse, sweeps take different times, so a background pass
// that started at T=0 and finished at T=4m could overwrite an on-demand pass
// that started at T=1m and published current state, walking the gauges BACKWARDS
// at precisely the moment an operator is refreshing them during an incident.
// divergencePublishMu makes each publication whole, and the generation compared
// under it drops a pass that started before the one already on display (see
// publish). The lock is held only for the assignment, never across a read.
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
//
// IT IS THE STORE'S CONSTANT, not a second number that happens to match. The
// cap has to be applied as a SQL LIMIT to bound anything (see
// store.MaxDivergenceExamples), so this is the same budget spelled where the
// report talks about it; two constants would drift, and the drift would be
// invisible — a larger budget here would just never be reached, and a smaller
// one would silently throw away rows the database was asked for.
const MaxDivergenceEntries = store.MaxDivergenceExamples

// divergenceExamples stages one sweep's examples per class, so the report's
// budget can be shared out rather than handed to whoever read first.
//
// WITHOUT IT, CLASS ORDER IS THE ALLOCATION. Entries were appended in the order
// the comparisons run, so one bulk class — a stopped queue, a broken community,
// the very situations this report exists for — consumed the entire budget and
// every later class arrived with a non-zero count and NOT ONE EXAMPLE. That is
// the exact failure DivergenceEntry.Subject exists to prevent: "a class and a
// count alone give an operator a number they cannot investigate", said of the
// classes that need investigating most, at the moment they need it.
//
// The staging is bounded by construction — each comparison is already limited
// to store.MaxDivergenceExamples rows at the database — so this holds at most
// one page per class and never the divergent population.
type divergenceExamples struct {
	// order is the classes in the order they were first seen, which is the
	// sweep's fixed comparison order. Deterministic on purpose: allocation that
	// depended on Go's randomized map iteration would hand different classes
	// their examples on every sweep, and an operator refreshing the report
	// would watch subjects appear and disappear with nothing changing.
	order   []string
	byClass map[string][]DivergenceEntry
}

func newDivergenceExamples() *divergenceExamples {
	return &divergenceExamples{byClass: make(map[string][]DivergenceEntry)}
}

func (e *divergenceExamples) add(entry DivergenceEntry) {
	if _, seen := e.byClass[entry.Class]; !seen {
		e.order = append(e.order, entry.Class)
	}
	e.byClass[entry.Class] = append(e.byClass[entry.Class], entry)
}

// fill deals the staged examples into the report, ONE PER CLASS PER ROUND until
// the budget runs out.
//
// Round-robin rather than an equal share computed up front, because an equal
// share wastes the budget: a sweep where one class has thousands and the others
// have three each would cap the big one at a quarter of the page and leave the
// rest of the page empty. Dealing a card at a time gives every class its
// examples first and then spends everything left on whoever still has rows, so a
// single-class incident still fills the page — which is what makes this
// compatible with the bound the report already promises.
func (e *divergenceExamples) fill(report *DivergenceReport) {
	for round := 0; ; round++ {
		dealt := false
		for _, class := range e.order {
			entries := e.byClass[class]
			if round >= len(entries) {
				continue
			}
			dealt = true
			report.addEntry(entries[round])
		}
		if !dealt {
			return
		}
	}
}

// addEntry records one example, while there is room for one.
//
// IT IS THE LAST OF THREE BOUNDS, NOT THE BOUND. The reads are limited at the
// database (store.MaxDivergenceExamples), because a list that must first be
// built in full is not bounded at all: the sweep that reaches this limit is the
// sweep running against a database with a stopped queue in it, and materialising
// every row of that before cutting it down would put the memory spike in exactly
// the process an operator is trying to keep alive. divergenceExamples then
// decides WHICH of those rows get the page. This function only refuses the ones
// that no longer fit.
//
// COUNTS ARE NOT TOUCHED HERE, deliberately: every caller counts what the
// comparison MEASURED — an exact COUNT(*) taken beside the limited read — not
// what fitted. A cap that also bounded the measurement would cap the number an
// operator escalates on at the size of a page, and "500" would then mean both
// "500" and "a catastrophe".
func (report *DivergenceReport) addEntry(entry DivergenceEntry) {
	if len(report.Entries) >= MaxDivergenceEntries {
		// SAY SO. A silently truncated list reads as the whole problem, and an
		// operator sizes an incident from what they can see. Note that this is
		// no longer the only way Truncated is set — a comparison whose count
		// exceeds the examples it returned truncated at the database, where
		// this function never sees the missing rows. See markTruncated.
		report.Truncated = true
		return
	}
	report.Entries = append(report.Entries, entry)
}

// markTruncated says whether the examples are fewer than the findings.
//
// IT IS THE ONLY HONEST TEST NOW THAT THE LIMIT IS IN SQL. addEntry can only
// notice truncation by being handed a row it has no room for, and a comparison
// bounded by `LIMIT 500` never hands over the 501st: the report would carry
// exactly 500 examples for a population of fifty thousand and claim to be
// complete. Comparing the counts — which are exact, by construction — against
// the examples catches every shape of it, including the one addEntry does see.
//
// It only ever SETS the flag. A truncation already noticed must not be cleared
// by a later recount, and the counts and the examples come from separate
// statements, so a row that arrives between them is a reason to be careful in
// one direction only.
func (report *DivergenceReport) markTruncated() {
	total := 0
	for _, count := range report.Counts {
		total += count
	}
	if total > len(report.Entries) {
		report.Truncated = true
	}
}

// newDivergenceReport is an EMPTY report with every class present at zero — and
// non-nil throughout, so the JSON is `{"entries":[],"counts":{...}}` rather than
// nulls a client has to special-case.
//
// The classes are DERIVED from divergenceGauges rather than listed again here.
// A second literal is a second declaration of the same vocabulary, and two
// literals drift: the class dropped from one of them is the one whose gauge then
// publishes 0 — health — for a comparison nobody is making. Deriving makes the
// two key sets the same set, and leaves exactly one place to edit when a class
// is added.
func newDivergenceReport() DivergenceReport {
	counts := make(map[string]int, len(divergenceGauges))
	for class := range divergenceGauges {
		counts[class] = 0
	}
	return DivergenceReport{
		Entries: []DivergenceEntry{},
		Counts:  counts,
	}
}

// Run sweeps once immediately, then on every interval tick until ctx is
// cancelled. A failed sweep is logged, never fatal: the next tick is the retry,
// and there is no state to leave half-applied because there is no state.
//
// The log line is not the only signal any more, and it must not be: this loop is
// the one that runs at 04:00 with nobody reading logs, and a permissions change
// or a statement timeout that fails every pass from then on would otherwise
// leave seven frozen, healthy-looking gauges behind it. Sweep counts each
// failure and the age gauge keeps climbing while they stand (see the metric
// constants), so a stopped sweep is visible on the same endpoint as its results.
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
// operator sees a failed sweep rather than a clean one. A DELIVERY STATE WITH NO
// CLASS aborts it the same way, for the same reason: see the acceptance loop.
//
// A FAILED PASS IS COUNTED, once, wherever it aborted. Leaving the previous
// values standing is right, but on its own it is silent: a sweep that fails
// forever leaves seven plausible numbers frozen and a log line nobody is
// watching. The counter and the age gauge are what distinguish those frozen
// numbers from a healthy bridge. The count is deferred rather than written at
// each return so a comparison added later cannot forget it, and it is skipped
// when ctx is already done because a sweep cut short by shutdown is not a
// failing sweep — inflating the counter on every restart would make the number
// mean "restarts plus failures", which is a number an operator cannot act on.
func (r *DivergenceReconciler) Sweep(ctx context.Context) (report DivergenceReport, err error) {
	// THE GUARD READS THE PARENT CONTEXT, NOT THE DEADLINED ONE BELOW. A sweep
	// that ran out of its own time is a FAILING sweep and must be counted: it is
	// the pathological pass this timeout exists to cut short, and the counter is
	// how anyone learns it happened. Only a sweep cut short by SHUTDOWN is
	// exempt, and that is the parent's cancellation, which is what this reads.
	defer func() {
		if err != nil && ctx.Err() == nil {
			metricSweepFailures.Add(1)
		}
	}()

	// Taken BEFORE the reads, so generations order sweeps by when they started
	// looking at the database — which is what makes a slow pass detectably older
	// than a fast one that started after it. Taking it at publication time would
	// order them by finish and defeat the whole guard.
	generation := divergenceSweepGeneration.Add(1)
	report = newDivergenceReport()
	// The examples are STAGED per class and dealt out at the end, so a bulk
	// class cannot spend the whole page before the later comparisons have run.
	// See divergenceExamples.
	examples := newDivergenceExamples()

	// ONE DEADLINE FOR THE WHOLE PASS, not one per read: what has to be bounded
	// is how long this sweep holds pool connections in total, and eight reads
	// with their own two-minute budgets would bound nothing. It is derived from
	// the caller's context so a shutdown still cancels immediately, and it
	// applies to the on-demand endpoint as well as the loop — a request context
	// carries whatever deadline the client chose, which for a curl is none.
	sweepCtx, cancel := context.WithTimeout(ctx, divergenceSweepTimeout)
	defer cancel()

	personaVotes, err := r.divergences.PersonaVoteEvents(sweepCtx)
	if err != nil {
		return DivergenceReport{}, fmt.Errorf("divergence: read persona vote events: %w", err)
	}
	for _, vote := range personaVotes {
		examples.add(DivergenceEntry{
			Class:   DivergencePersonaVoteEvent,
			Subject: vote.ActivityID,
			Detail: fmt.Sprintf(
				"vote on %s is attributed to our own persona %s (%s), so that subject's tally "+
					"counts it twice: once as this inbound event and once as the delivered "+
					"outbound vote the reseed subtracts",
				vote.SubjectAPID, vote.VoterAPID, vote.ActorDID),
		})
	}
	// THE COUNT IS ITS OWN MEASUREMENT, never len(entries) and never
	// len(Entries): the list above is bounded by store.MaxDivergenceExamples,
	// so its length is the size of a page rather than the size of the problem.
	// See addEntry.
	personaTotal, err := r.divergences.PersonaVoteEventCount(sweepCtx)
	if err != nil {
		return DivergenceReport{}, fmt.Errorf("divergence: count persona vote events: %w", err)
	}
	report.Counts[DivergencePersonaVoteEvent] = personaTotal

	undelivered, err := r.divergences.UndeliveredAcceptances(sweepCtx, r.staleAfter)
	if err != nil {
		return DivergenceReport{}, fmt.Errorf("divergence: read undelivered acceptances: %w", err)
	}
	for _, acceptance := range undelivered {
		class := acceptanceClass(acceptance.DeliveryState)
		if class == "" {
			// A delivery state this sweep has no class for ABORTS THE PASS, the
			// same treatment a read failure gets, and for the same reason: it
			// means our model of the schema is stale, so every number this sweep
			// is about to publish was computed by code that does not know what it
			// is looking at.
			//
			// IT USED TO WARN AND SKIP, which dropped the row from Entries AND
			// from every Counts key — the report then said three undelivered
			// acceptances when there were four, with the missing one visible only
			// in a log line, in a sweep whose entire premise is that logs are not
			// where an operator looks. A silent undercount is the one failure this
			// file cannot tolerate, because it is indistinguishable from health.
			//
			// A VISIBLE CLASS WAS THE ALTERNATIVE and it is worse here: a
			// catch-all class needs a gauge (divergenceGauges is the vocabulary),
			// and a gauge named for "states we do not understand" is a number
			// nobody can alert on or act on — while the aborted sweep already has
			// an operator-visible surface that says exactly this, the failure
			// counter plus the climbing age gauge, and leaves the last real
			// numbers standing rather than replacing them with numbers derived
			// from a schema we have misread.
			//
			// Unreachable today: the store's filter admits only cancelled,
			// poisoned and pending. It fires the day someone adds a delivery
			// state — which is precisely the change that must not land quietly.
			return DivergenceReport{}, fmt.Errorf(
				"divergence: undelivered acceptance %s is in delivery state %q, which this sweep "+
					"has no class for: the state vocabulary has changed and acceptanceClass has not",
				acceptance.PostURI, acceptance.DeliveryState)
		}
		examples.add(DivergenceEntry{
			Class:   class,
			Subject: acceptance.PostURI,
			Detail:  acceptanceDetail(acceptance),
		})
	}
	// COUNTED BY DELIVERY STATE, and mapped to classes through the SAME
	// function the entries are classified with, so the count and the example
	// for one post can never disagree about which class it belongs to.
	acceptanceCounts, err := r.divergences.UndeliveredAcceptanceCounts(sweepCtx, r.staleAfter)
	if err != nil {
		return DivergenceReport{}, fmt.Errorf("divergence: count undelivered acceptances: %w", err)
	}
	for state, count := range acceptanceCounts {
		class := acceptanceClass(state)
		if class == "" {
			// The same abort as above, at the count, and the count is where the
			// undercount would have done its real damage: an operator sizes an
			// incident from these numbers, and a class total that quietly omits a
			// state reads as a smaller problem rather than an unknown one.
			return DivergenceReport{}, fmt.Errorf(
				"divergence: %d undelivered acceptances are in delivery state %q, which this "+
					"sweep has no class for: the state vocabulary has changed and acceptanceClass "+
					"has not", count, state)
		}
		report.Counts[class] += count
	}

	recasts, err := r.divergences.RecastDivergences(sweepCtx)
	if err != nil {
		return DivergenceReport{}, fmt.Errorf("divergence: read recast divergences: %w", err)
	}
	for _, recast := range recasts {
		examples.add(DivergenceEntry{
			Class: DivergenceVoteRecastUndelivered,
			// The SUBJECT is what an operator checks the tally of; the delivered
			// activity id is what the peer is actually holding, and the only
			// handle a manual Undo could embed. Both are needed: the class and
			// the subject alone say a number is wrong without saying which vote
			// is making it wrong.
			Subject: recast.SubjectATURI,
			// THE DETAIL STATES WHAT THE ROW PROVES AND NOTHING ELSE. It used to
			// assert a re-cast happened, and the query does not establish that:
			// at least three populations reach this class — a re-cast whose new
			// delivery poisoned, an Undo that poisoned, and purge residue
			// (Purger.undoLiveVotes sets delivered_state='undone' at DECISION
			// time while keeping current_activity_id, so the pair is reported
			// from that moment until the Undo lands, and forever if it does
			// not). Naming one cause sends an operator to check
			// the wrong thing on two of the three, and a Detail is a claim in
			// exactly the way a metric name is. What the row DOES prove is the
			// delivery and the absence, so say that and let the ledger say why.
			Detail: fmt.Sprintf(
				"the peer accepted vote activity %s cast by %s and nothing in this bridge's "+
					"accounting covers it: no live delivered vote row names that activity and no "+
					"delivered Undo or later delivered vote for the pair supersedes it. Read the "+
					"activity's ledger row for the cause",
				recast.DeliveredActivityID, recast.ActorDID),
		})
	}
	recastTotal, err := r.divergences.RecastDivergenceCount(sweepCtx)
	if err != nil {
		return DivergenceReport{}, fmt.Errorf("divergence: count recast divergences: %w", err)
	}
	report.Counts[DivergenceVoteRecastUndelivered] = recastTotal

	unknown, err := r.divergences.UnknownDeliveryOutcomes(sweepCtx)
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
				"because we cannot tell from here.%s",
			outcome.Kind, outcome.TargetInbox, outcome.LastErrorClass, outcome.TargetInbox,
			unknownAcceptanceOverlap)
		if outcome.Refused {
			class = DivergenceDeliveryUnknownRefused
			detail = fmt.Sprintf(
				"we sent this %s to %s and the peer answered %d (%s): that is evidence it was not "+
					"applied and not proof of it, since a peer can apply an activity and then "+
					"fail to respond.%s",
				outcome.Kind, outcome.TargetInbox, outcome.LastStatusCode, outcome.LastErrorClass,
				unknownAcceptanceOverlap)
		}
		examples.add(DivergenceEntry{
			// The SUBJECT is the activity id: what was sent is the only handle
			// that exists here. There is no post at-uri to point at — these are
			// every kind of activity, and the acceptance classes are where a post
			// the peer never received is reported.
			Class:   class,
			Subject: outcome.ActivityID,
			Detail:  detail,
		})
	}
	// The same split, counted rather than listed. It is read as a pair from one
	// grouped statement so the two sub-counts cannot come from two different
	// spellings of "the peer answered" — which is precisely how a dial timeout
	// would end up in the refused bucket.
	unknownCounts, err := r.divergences.UnknownDeliveryOutcomeCounts(sweepCtx)
	if err != nil {
		return DivergenceReport{}, fmt.Errorf("divergence: count unknown delivery outcomes: %w", err)
	}
	report.Counts[DivergenceDeliveryUnknownRefused] = unknownCounts.Refused
	report.Counts[DivergenceDeliveryUnknownUnanswered] = unknownCounts.Unanswered

	// The staged examples become the report's page here, after every comparison
	// has had its say — dealing them out earlier would be the class-order
	// allocation this staging exists to remove.
	examples.fill(&report)
	// And last, the counts against the examples: the reads are bounded in SQL,
	// so this is where a report learns it is smaller than the problem it
	// describes.
	report.markTruncated()

	r.publish(generation, report)
	return report, nil
}

// acceptanceClass maps a delivery state to the class an operator triages on.
// An unknown state yields "" and the caller ABORTS THE SWEEP on it rather than
// guessing: a state nobody has a response for means the schema has moved under
// this sweep, and a report built on that is a set of numbers nobody can trust.
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

// unknownAcceptanceOverlap is the sentence that stops an operator adding two
// gauges that count one row.
//
// A poisoned delivery carrying an accepted post is reported TWICE by one sweep:
// under acceptance-undelivered-poisoned keyed by the post at-uri, and here
// keyed by the activity id. Both entries are wanted — see acceptanceDetail for
// why neither side is dropped — but only if the report says they are the same
// row, because two entries with two subjects and two counts otherwise read as
// two problems, and the sum reads as a total.
//
// It is stated CONDITIONALLY because this read cannot tell: the unknown classes
// cover every activity kind (votes, Undos, comments), and only a Create whose
// object is an accepted, unstamped post has a twin. Asserting the twin exists
// would be the same overclaim this whole class avoids.
const unknownAcceptanceOverlap = " If this activity carried a post its community had already " +
	"accepted, the same delivery is reported again under acceptance-undelivered-poisoned, keyed " +
	"by the post's at-uri: one row asked two questions, so the two counts must not be added."

// acceptanceDetail is what the report SAYS about an undelivered acceptance, and
// only ONE of the three states supports a definite claim.
//
// A DETAIL IS A CLAIM, exactly as a class name is. The comparison behind all
// three entries is `accepted_at IS NULL`, and that column is stamped only by
// delivery success (worker.stampAccepted), so its absence proves that success
// was never SETTLED HERE — never that the peer lacks the post:
//
//	cancelled — definite, and earned. The row left the queue without a POST (a
//	  ban, an opt-out), so the peer really was never told and nothing will ever
//	  carry it.
//	poisoned  — NOT definite. The activity reached the wire and the answer is
//	  unavailable, which is the entire justification for the delivery-unknown-*
//	  classes forty lines up. A Detail asserting non-delivery here would
//	  contradict those classes about the same row, in the same report.
//	stale     — NOT definite either. A pending delivery past the window has
//	  usually been ATTEMPTED (attempts is what drives the backoff), and a row
//	  that has been attempted is silent about whether any attempt landed. What
//	  is certain is that the queue is not finishing with it.
//
// THE POISONED CASE ALSO NAMES ITS OVERLAP. One poisoned acceptance Create is
// reported twice by this sweep — here, keyed by the post at-uri, and again
// under delivery-unknown-refused/unanswered keyed by the activity id — because
// the two comparisons ask different questions about the same row: "is this
// accepted post confirmed on the peer?" and "what became of this delivery?".
// They are stated as an overlap rather than resolved by dropping one, for three
// reasons. The unknown classes are the ONLY report of a poisoned vote or Undo,
// so excluding acceptance-carrying activities from them would leave those
// counts meaning "everything except posts" — a stranger claim than the overlap.
// Suppressing this entry instead would take the post's at-uri, its community,
// and the redrive action out of the report, leaving an activity id an operator
// cannot map back to a post. And excluding either way requires one query to
// re-derive the other's join — accepted admission, unstamped object, latest
// delivery, staleness window — which is the second-query-for-one-condition
// drift the class vocabulary is already defended against (see the lapsed-ban
// test). What the overlap really costs is an operator ADDING the gauges, so
// both Details say plainly that these two are one row.
func acceptanceDetail(acceptance store.UndeliveredAcceptance) string {
	switch acceptance.DeliveryState {
	case store.DeliveryStateCancelled:
		return fmt.Sprintf(
			"community %s accepted this post and the peer was never told: its delivery (%s) was "+
				"CANCELLED before any POST — a decision, a ban or an opt-out, took it out of the "+
				"queue — so the peer does not have it and nothing will ever carry it. This is the "+
				"one class here whose non-delivery is a fact rather than an inference",
			acceptance.CommunityDID, acceptance.ActivityID)
	case store.DeliveryStatePoisoned:
		return fmt.Sprintf(
			"community %s accepted this post and its delivery (%s) is POISONED (%s): accepted_at "+
				"was never stamped, which records only that success was never settled HERE. "+
				"Whether the peer applied it is not knowable from this bridge — the POST may have "+
				"been refused, may never have arrived, or may have been applied with the response "+
				"lost. The SAME delivery is reported again under delivery-unknown-refused or "+
				"delivery-unknown-unanswered, keyed by that activity id and carrying the inbox to "+
				"ask: two questions about one row, not two findings, so those counts and this one "+
				"must not be added together",
			acceptance.CommunityDID, acceptance.ActivityID, acceptance.LastErrorClass)
	case store.DeliveryStatePending:
		return fmt.Sprintf(
			"community %s accepted this post and its delivery (%s) is still PENDING far past the "+
				"staleness window: the QUEUE has stopped moving, and that investigation starts at "+
				"the worker rather than at the community. accepted_at was never stamped, which "+
				"records only that no delivery has settled HERE — an attempt that went out and was "+
				"never confirmed looks identical on these columns to one that never left — so "+
				"whether the peer holds this post is not knowable from here",
			acceptance.CommunityDID, acceptance.ActivityID)
	default:
		// Unreachable while acceptanceClass gates the caller: a state with no
		// class aborts the sweep before a Detail is ever composed. Kept
		// non-committal anyway, because the failure this file exists to prevent
		// is a sentence that claims more than the row establishes.
		return fmt.Sprintf(
			"community %s accepted this post and its delivery (%s) is in state %q, which this "+
				"sweep has no reading for: what the peer holds is not knowable from here",
			acceptance.CommunityDID, acceptance.ActivityID, acceptance.DeliveryState)
	}
}

// publish assigns the gauges from a COMPLETED sweep.
//
// It runs only on success, and that is the discipline the whole gauge design
// rests on: a failed sweep must leave the previous values standing rather than
// write a zero, because a zero written by a sweep that read nothing is a claim
// that everything is fine, made at the moment nobody could check.
//
// THE WHOLE ASSIGNMENT IS ONE CRITICAL SECTION, and an older sweep's numbers are
// dropped rather than written over a newer sweep's (see DivergenceReconciler for
// the hazard). generation is the caller's sweep number, taken before its reads.
//
// A CLASS WITH NO COUNT IS A SENTINEL, NEVER A ZERO. `report.Counts[class]`
// alone reads a missing key as 0 — the value that says "this invariant is
// holding" — so a comparison dropped from Sweep, or a gauge whose class the
// report does not carry, would broadcast health from a pass that measured
// nothing. Deriving Counts from this same map makes that unreachable by
// construction; the check stays because the failure it prevents is the one this
// whole file exists to prevent, and a construction can be undone by an edit that
// looks harmless.
func (r *DivergenceReconciler) publish(generation uint64, report DivergenceReport) {
	divergencePublishMu.Lock()
	defer divergencePublishMu.Unlock()

	if generation <= divergencePublishedGeneration {
		// A slower pass that started earlier finishing after a newer one. Its
		// report still goes back to its own caller — it was true when it was
		// read — but writing it here would move the gauges backwards.
		r.logger.Info("divergence: dropping a stale sweep's gauge publication",
			"sweep", generation, "published", divergencePublishedGeneration,
			"entries", len(report.Entries))
		return
	}
	divergencePublishedGeneration = generation
	divergenceLastPublished = time.Now()

	for class, gauge := range divergenceGauges {
		count, counted := report.Counts[class]
		if !counted {
			gauge.Set(divergenceUnswept)
			r.logger.Error("divergence: sweep produced no count for a gauged class, so its gauge "+
				"reports unswept rather than zero: a comparison for this class is missing from "+
				"Sweep, and zero would claim the invariant is holding",
				"class", class, "sweep", generation)
			continue
		}
		gauge.Set(int64(count))
	}
	for class, count := range report.Counts {
		if _, gauged := divergenceGauges[class]; !gauged {
			// The mirror failure: a comparison whose findings reach the report and
			// no gauge, so an operator alerting on the gauges never sees it. There
			// is nothing to set — say so loudly instead.
			r.logger.Error("divergence: sweep counted a class with no gauge, so nothing an "+
				"operator alerts on reports it",
				"class", class, "count", count, "sweep", generation)
		}
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
