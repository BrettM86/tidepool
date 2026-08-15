package ingest

import (
	"context"
	"database/sql"
	stderrors "errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/store"
)

// TASK 17e CYCLE 4 — THE HONESTY OF THE REPORT ITSELF.
//
// Everything up to here has been about finding divergences. This is about what
// the report is allowed to CLAIM, and it is where a reconciliation job most
// easily ships dishonestly: by publishing a number when it has none, by naming
// an uncertainty as a fact, or by proposing the repair that decision 19 forbids
// it from making.

// ---------------------------------------------------------------------------
// (a) Unknown means unknown, in the class names and in the metric names
// ---------------------------------------------------------------------------

// TestDivergenceMetricNamesForPoisonedDeliveriesSayUnknown looks trivial and is
// not.
//
// The two poisoned classes report deliveries this bridge SENT and never got
// confirmation for. We do not know whether the peer applied them: a transport
// timeout is silent about it, and even a refusal is evidence rather than proof
// — a peer can apply an activity and then fail to answer. A METRIC NAME IS A
// CLAIM, and it is the claim that survives: it ends up on a dashboard, in an
// alert rule, and in the sentence somebody says in an incident review. Named
// `_undelivered` or `_lost`, these counts assert something nobody can know, and
// the next reader who sees two adjacent numbers with confident names will add
// them into one.
//
// The precedent is deliberate: consume's deadLetterDepthUnavailable reports
// UNAVAILABLE rather than a plausible zero, for the same reason and in the same
// direction.
func TestDivergenceMetricNamesForPoisonedDeliveriesSayUnknown(t *testing.T) {
	for _, name := range []string{
		MetricDivergenceUnknownRefused,
		MetricDivergenceUnknownUnanswered,
		DivergenceDeliveryUnknownRefused,
		DivergenceDeliveryUnknownUnanswered,
	} {
		assert.Contains(t, name, "unknown",
			"%q must say UNKNOWN. It is the only true claim about a poisoned delivery, and the "+
				"name is what an operator reads at 3am with no context", name)
		assert.NotContains(t, name, "undelivered",
			"%q must NOT claim non-delivery: a refusal is evidence, not proof, and a transport "+
				"failure is silent — the activity may have been applied and the response lost", name)
		assert.NotContains(t, name, "lost",
			"%q must NOT claim the activity was lost: that is the same assertion in a friendlier "+
				"word, and it is equally unfounded", name)
	}

	assert.NotEqual(t, MetricDivergenceUnknownRefused, MetricDivergenceUnknownUnanswered,
		"and the two stay SEPARATE. A peer that answered told us something a silent one did "+
			"not; one number for both throws away the only distinction available and reads as "+
			"a total of things we know")
}

// TestDivergenceReportSeparatesRefusedFromUnansweredAndClaimsNothingElse drives
// both shapes through the sweep.
func TestDivergenceReportSeparatesRefusedFromUnansweredAndClaimsNothingElse(t *testing.T) {
	h := newHarness(t)
	world := newModerationWorld(t, h)
	h.admin.SetDivergenceReconciler(newDivergenceReconciler(t, h))
	deliverEverythingQueued(t, h.db)
	_ = world

	// Two poisoned deliveries that are NOT post acceptances, so what is under
	// test is the delivery-outcome question alone.
	refused := seedPoisonedComment(t, h.db, "dv-c4-refused", "4xx", 422)
	unanswered := seedPoisonedComment(t, h.db, "dv-c4-unanswered", "transport", 0)

	report := fetchDivergence(t, h)

	assert.Equal(t, []string{refused}, subjectsOfClass(report, DivergenceDeliveryUnknownRefused),
		"the peer ANSWERED this one — with a rejection — which is evidence of non-application "+
			"and not proof of it")
	assert.Equal(t, []string{unanswered}, subjectsOfClass(report, DivergenceDeliveryUnknownUnanswered),
		"and this one got no answer at all: it may never have arrived, or it may have been "+
			"applied and the response lost. Two sub-counts, because the two rows do not carry "+
			"the same amount of information")
	assert.Equal(t, 1, report.Counts[DivergenceDeliveryUnknownRefused])
	assert.Equal(t, 1, report.Counts[DivergenceDeliveryUnknownUnanswered])

	// Neither may appear in a class that asserts what the peer holds.
	for _, entry := range report.Entries {
		if entry.Subject != refused && entry.Subject != unanswered {
			continue
		}
		assert.Contains(t, entry.Class, "unknown",
			"an activity whose outcome is unknown may appear ONLY under a class that says so. "+
				"Counting it anywhere that claims the peer does — or does not — hold it turns "+
				"an unanswered question into a number somebody will act on")
	}

	assert.Equal(t, 1, gaugeValue(t, h, MetricDivergenceUnknownRefused))
	assert.Equal(t, 1, gaugeValue(t, h, MetricDivergenceUnknownUnanswered))
}

// TestDivergenceReportProposesNoRepair pins the omission decision 19 requires.
//
// The reconciler is the one component that reads both sides, so it is the one
// most tempted to fix what it sees — and least able to. It cannot know whether a
// poisoned row is stale relative to newer user intent, whether a cancelled
// delivery was cancelled on purpose, or whether the peer already applied the
// activity it is about to re-send. POST /admin/outbound/redrive exists and is
// deliberately refused unscoped: a HUMAN scoping a redrive is the design, and a
// report that came with a button would make that refusal decorative.
func TestDivergenceReportProposesNoRepair(t *testing.T) {
	h := newHarness(t)
	world := newModerationWorld(t, h)
	h.admin.SetDivergenceReconciler(newDivergenceReconciler(t, h))
	deliverEverythingQueued(t, h.db)
	_ = world
	seedPoisonedComment(t, h.db, "dv-c4-norepair", "transport", 0)
	// A divergence that is ALREADY reported, so the body under test actually
	// contains entries: an absence assertion over an empty report is true of a
	// report that proposes nothing and of one that found nothing, and only the
	// first is the claim being made here.
	insertVoteEvent(t, h.db, "https://lemmy.world/activities/like/dv-c4-norepair",
		actorIDOf(t, h.db, mtAuthorDID), "up")

	report := fetchDivergence(t, h)
	require.NotEmpty(t, report.Entries,
		"precondition: the report carries at least one entry, or the assertions below hold "+
			"vacuously over an empty list")

	rec := h.adminRequest(http.MethodGet, "/admin/divergence", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()

	for _, forbidden := range []string{`"action"`, `"remedy"`, `"repair"`, `"fix"`, `"suggested`} {
		assert.NotContains(t, body, forbidden,
			"the report must carry no %s field: a reconciler that recommends is one step from a "+
				"reconciler that acts, and the recommendation would be made without the two "+
				"things only a human has — whether the state is intended, and whether the peer "+
				"already applied what we are about to re-send", forbidden)
	}
}

// seedPoisonedComment writes a poisoned delivery for an activity that is NOT a
// post acceptance, and returns its activity id. status 0 means the peer never
// answered.
func seedPoisonedComment(t *testing.T, db *sql.DB, suffix, errorClass string, status int) string {
	t.Helper()
	ctx := context.Background()
	activityID := mtUserOrigin + "/ap/activity/" + suffix
	_, err := db.ExecContext(ctx, `
		INSERT INTO outbound_activities (activity_id, actor_did, kind, payload)
		VALUES ($1, $2, 'Create', '{"type":"Create","object":{"type":"Note"}}'::jsonb)`,
		activityID, mtAuthorDID)
	require.NoError(t, err)

	// NULL, not zero: a delivery that got no answer must not be stored as one
	// answered with 0.
	var statusArg any
	if status != 0 {
		statusArg = status
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO outbound_deliveries (activity_id, target_inbox, ordering_key, state,
		                                 last_error_class, last_status_code, created_at)
		VALUES ($1, $2, $3, 'poisoned', $4, $5, now() - interval '2 hours')`,
		activityID, "https://lemmy.world/inbox", groupID, errorClass, statusArg)
	require.NoError(t, err)
	return activityID
}

// ---------------------------------------------------------------------------
// (b) The endpoint contract
// ---------------------------------------------------------------------------

// failingDivergences fails ONE read, so a sweep that cannot see everything
// cannot publish anything.
type failingDivergences struct {
	store.Divergences
	mu    sync.Mutex
	calls int
	err   error
}

func (f *failingDivergences) PersonaVoteEvents(ctx context.Context) ([]store.PersonaVoteEvent, error) {
	f.mu.Lock()
	f.calls++
	err := f.err
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return f.Divergences.PersonaVoteEvents(ctx)
}

func (f *failingDivergences) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// TestDivergenceEndpoint_NotConfiguredIsNotImplemented is the nil-dependency
// pattern the rest of /admin already follows: a deployment without the sweep
// says so, rather than 404ing as if the operator mistyped the path.
func TestDivergenceEndpoint_NotConfiguredIsNotImplemented(t *testing.T) {
	h := newHarness(t)

	rec := h.adminRequest(http.MethodGet, "/admin/divergence", nil)
	assert.Equal(t, http.StatusNotImplemented, rec.Code,
		"with no reconciler wired the endpoint reports NOT IMPLEMENTED: an operator who gets a "+
			"404 goes looking for a typo, and one who gets an empty report concludes the bridge "+
			"is healthy")
}

// TestDivergenceEndpoint_AFailedSweepPublishesNothing is the one that matters.
//
// A sweep that cannot read one class must not publish ANY of them. Two failures
// are on the table and both are silent:
//
//   - a PARTIAL REPORT is indistinguishable from a class that found nothing, so
//     an operator reads "no persona votes" when the truth is "nobody looked";
//   - a ZERO WRITTEN BY A FAILED SWEEP is a claim of health made at exactly the
//     moment nobody could check, and it OVERWRITES the last real number — the
//     one an operator was watching.
func TestDivergenceEndpoint_AFailedSweepPublishesNothing(t *testing.T) {
	h := newHarness(t)
	world := newModerationWorld(t, h)
	deliverEverythingQueued(t, h.db)
	_ = world

	// A real divergence, so the gauges carry a NON-ZERO value a failure could
	// overwrite. A fixture whose healthy value is 0 cannot tell "left standing"
	// from "written as zero".
	personaActorID := actorIDOf(t, h.db, mtAuthorDID)
	insertVoteEvent(t, h.db, "https://lemmy.world/activities/like/dv-c4-fail", personaActorID, "up")

	h.admin.SetDivergenceReconciler(newDivergenceReconciler(t, h))
	require.Equal(t, http.StatusOK, h.adminRequest(http.MethodGet, "/admin/divergence", nil).Code)
	require.Equal(t, 1, gaugeValue(t, h, MetricDivergencePersonaVoteEvents),
		"precondition: a completed sweep published a real number")

	// Now the store starts failing.
	faulty := &failingDivergences{
		Divergences: store.NewDivergences(h.db),
		err:         stderrors.New("connection reset by peer"),
	}
	failing, err := NewDivergenceReconciler(DivergenceOptions{DB: h.db, Divergences: faulty})
	require.NoError(t, err)
	h.admin.SetDivergenceReconciler(failing)

	rec := h.adminRequest(http.MethodGet, "/admin/divergence", nil)
	assert.Equal(t, http.StatusInternalServerError, rec.Code,
		"a sweep that could not read every class is a FAILURE, not a report: returning the "+
			"classes that happened to succeed publishes a claim about state nobody read")
	assert.NotContains(t, rec.Body.String(), `"entries"`,
		"and no partial report rides out with the error — a report missing one class reads "+
			"exactly like a class that found nothing")
	require.NotZero(t, faulty.Calls(), "precondition: the sweep really did try to read")

	assert.Equal(t, 1, gaugeValue(t, h, MetricDivergencePersonaVoteEvents),
		"THE PREVIOUS VALUE STANDS. A failed sweep that wrote 0 would replace the last real "+
			"measurement with a claim of health, made at the moment nobody could check — and "+
			"the operator watching that gauge would see the problem disappear")
}

// TestDivergenceReportIsBounded pins the cap and the flag that admits it.
//
// A diverging system diverges in BULK — one broken community, one stopped queue
// — so an unbounded list is a second outage: a response nobody can load, served
// from the endpoint an operator reaches for because something is already wrong.
// The counts stay exact; only the examples are bounded, and the report says so
// rather than letting a truncated list read as the whole problem.
func TestDivergenceReportIsBounded(t *testing.T) {
	h := newHarness(t)
	world := newModerationWorld(t, h)
	h.admin.SetDivergenceReconciler(newDivergenceReconciler(t, h))
	deliverEverythingQueued(t, h.db)
	_ = world

	// MaxDivergenceEntries + 1 of ONE class, so the cap is unambiguous.
	personaActorID := actorIDOf(t, h.db, mtAuthorDID)
	overflowPersonaVotes(t, h.db, personaActorID, MaxDivergenceEntries+1)

	report := fetchDivergence(t, h)

	assert.Len(t, report.Entries, MaxDivergenceEntries,
		"the response carries at most MaxDivergenceEntries examples: a report that grows with "+
			"the outage is a report that cannot be read during one")
	assert.True(t, report.Truncated,
		"and it SAYS SO. A silently truncated list is worse than a long one: an operator sizes "+
			"the problem from what they can see, and %d examples that look complete argue for a "+
			"smaller incident than there is", MaxDivergenceEntries)
	assert.Equal(t, MaxDivergenceEntries+1, report.Counts[DivergencePersonaVoteEvent],
		"while the COUNT stays true — the cap bounds the examples, never the measurement, or "+
			"the number an operator escalates on would be capped at the size of a page")
}

// overflowPersonaVotes writes n inbound vote events attributed to one persona.
// One statement, because 501 round trips is a slow way to say "a lot".
func overflowPersonaVotes(t *testing.T, db *sql.DB, voterAPID string, n int) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `
		INSERT INTO vote_events (activity_id, voter_ap_id, subject_ap_id, direction)
		SELECT 'https://lemmy.world/activities/like/bulk-' || i, $1, $2, 'up'
		  FROM generate_series(1, $3) AS i`, voterAPID, mtPostAPID, n)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// (c) Run is a loop, not a one-shot that dies
// ---------------------------------------------------------------------------

// countingDivergences signals every read, so the test can wait for real sweeps
// instead of sleeping.
type countingDivergences struct {
	store.Divergences
	swept chan struct{}
	err   error
}

func (c *countingDivergences) PersonaVoteEvents(ctx context.Context) ([]store.PersonaVoteEvent, error) {
	select {
	case c.swept <- struct{}{}:
	default:
	}
	if c.err != nil {
		return nil, c.err
	}
	return c.Divergences.PersonaVoteEvents(ctx)
}

// TestDivergenceRunKeepsSweepingAfterAFailure is the difference between a
// reconciler and a cron job that dies at 3am.
//
// The sweep runs unattended, against a database that will occasionally refuse a
// connection. A failure that ended the loop would leave every gauge frozen at
// its last value — which is precisely the shape of a healthy system, so nothing
// would ever page — and the next real divergence would go unreported until
// somebody restarted the process.
func TestDivergenceRunKeepsSweepingAfterAFailure(t *testing.T) {
	h := newHarness(t)

	counting := &countingDivergences{
		Divergences: store.NewDivergences(h.db),
		swept:       make(chan struct{}, 8),
		err:         stderrors.New("connection reset by peer"),
	}
	reconciler, err := NewDivergenceReconciler(DivergenceOptions{
		DB:          h.db,
		Divergences: counting,
		Interval:    time.Millisecond,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		reconciler.Run(ctx)
		close(done)
	}()

	// Every sweep here FAILS. THREE signals is the assertion, not two: the first
	// is the startup pass, so a loop that dies on its first failing TICK still
	// produces two. Only the third proves it failed, logged, and came back.
	for i := 0; i < 3; i++ {
		select {
		case <-counting.swept:
		case <-time.After(5 * time.Second):
			t.Fatalf("Run stopped sweeping after %d failed sweeps: a reconciler that dies on a "+
				"transient database error leaves every gauge frozen at its last value, which "+
				"looks exactly like a healthy system and pages nobody", i)
		}
	}

	// And it stops when it is told to, rather than only when it breaks.
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run must return when its context is cancelled: a sweep that outlives shutdown " +
			"holds the process open and keeps reading a database that is going away")
	}
}

// TestDivergenceRunSweepsImmediately pins the startup pass.
//
// A reconciler whose first sweep waits a full interval publishes nothing at all
// for that interval — and the gauges read their unswept sentinel, so a fresh
// process looks like a broken one for as long as the cadence says.
func TestDivergenceRunSweepsImmediately(t *testing.T) {
	h := newHarness(t)

	counting := &countingDivergences{
		Divergences: store.NewDivergences(h.db),
		swept:       make(chan struct{}, 4),
	}
	reconciler, err := NewDivergenceReconciler(DivergenceOptions{
		DB:          h.db,
		Divergences: counting,
		Interval:    time.Hour, // long enough that only the startup pass can fire
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go reconciler.Run(ctx)

	select {
	case <-counting.swept:
	case <-time.After(5 * time.Second):
		t.Fatal("Run must sweep once IMMEDIATELY, before the first tick: otherwise a restarted " +
			"process publishes its unswept sentinel for a whole interval, and an operator " +
			"watching the gauges cannot tell a fresh start from a stalled sweep")
	}
}

// ---------------------------------------------------------------------------
// 17e review — a class with no gauge publishes health it never measured
// ---------------------------------------------------------------------------

// TestEveryDivergenceClassHasAGauge pins the two key sets against each other.
//
// publish() reads report.Counts[class] for each gauge, and a MISSING KEY yields
// zero — so a class whose gauge was forgotten does not go unwatched, it
// publishes 0: health, for a comparison nobody wired up. The reverse is as bad:
// a gauge with no class in the report is set from a map miss on every sweep and
// reads 0 forever, which is a number an operator can watch indefinitely while it
// measures nothing.
//
// The old comment on divergenceGauges claimed a missing entry "fails to compile
// at the map literal". IT DOES NOT — Go map literals are not exhaustive over any
// key set, and nothing in the language checks these two against each other. That
// claim is why nobody wrote this test, which is the failure mode worth naming:
// a compile-time guarantee asserted in prose is a runtime hole with a comment
// over it.
func TestEveryDivergenceClassHasAGauge(t *testing.T) {
	classes := make([]string, 0, len(newDivergenceReport().Counts))
	for class := range newDivergenceReport().Counts {
		classes = append(classes, class)
	}
	gauged := make([]string, 0, len(divergenceGauges))
	for class := range divergenceGauges {
		gauged = append(gauged, class)
	}

	assert.ElementsMatch(t, classes, gauged,
		"every class the report counts must have a gauge, and every gauge must have a class. A "+
			"class with no gauge is not merely unwatched: publish reads a missing key as 0 and "+
			"broadcasts HEALTH for a comparison nobody wired. A gauge with no class is set from "+
			"a map miss on every sweep and reads 0 forever while measuring nothing. Nothing in "+
			"Go checks these two literals against each other — the map literal does NOT fail to "+
			"compile, whatever the comment above it says")

	for class, gauge := range divergenceGauges {
		require.NotNil(t, gauge, "the gauge for %q must exist", class)
	}
}

// TestDivergenceReportIsNotTruncatedWhenItFits is the negative control for the
// cap, and it is cheap for a reason: an implementation that sets Truncated
// unconditionally passes the bounded test, and then every report an operator
// ever reads claims examples are missing. A flag that is always true carries no
// information, and the one it should have carried is "the number you are looking
// at is smaller than the problem".
func TestDivergenceReportIsNotTruncatedWhenItFits(t *testing.T) {
	h := newHarness(t)
	world := newModerationWorld(t, h)
	h.admin.SetDivergenceReconciler(newDivergenceReconciler(t, h))
	deliverEverythingQueued(t, h.db)
	_ = world

	// A handful of real divergences — far below the cap.
	personaActorID := actorIDOf(t, h.db, mtAuthorDID)
	overflowPersonaVotes(t, h.db, personaActorID, 3)

	report := fetchDivergence(t, h)
	require.Len(t, report.Entries, 3, "precondition: a report well inside the cap")
	assert.False(t, report.Truncated,
		"a report that FITS must not claim it was cut short: a flag that is always set tells an "+
			"operator nothing, and the thing it was supposed to tell them — that the list is "+
			"smaller than the problem — is exactly what they would stop believing")
}

// TestDivergenceStaleAfterOptionIsHonored pins that the configured window is the
// one actually applied.
//
// The reconciler takes AcceptanceStaleAfter and defaults it; replacing the field
// with the default constant at the call site leaves every other test green,
// because they all age their fixtures far past both values. An option nobody
// reads is worse than no option: an operator tunes it, observes no change, and
// concludes the report is broken in some deeper way.
func TestDivergenceStaleAfterOptionIsHonored(t *testing.T) {
	h := newHarness(t)
	world := newModerationWorld(t, h)
	deliverEverythingQueued(t, h.db)

	// One accepted post whose delivery has been pending for two hours: stale
	// under a one-minute window, in flight under a one-day one.
	stale := admitDivergencePost(t, h, world, "3lzdvopt00001", "3lzdvopt00011", 1_775_000_090_000_001)
	setDeliveryState(t, h.db, stale, "pending", "", 2*time.Hour)

	tight, err := NewDivergenceReconciler(DivergenceOptions{
		DB: h.db, AcceptanceStaleAfter: time.Minute,
	})
	require.NoError(t, err)
	tightReport, err := tight.Sweep(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, tightReport.Counts[DivergenceAcceptanceStale],
		"with a one-minute window a delivery pending for two hours is stale")

	relaxed, err := NewDivergenceReconciler(DivergenceOptions{
		DB: h.db, AcceptanceStaleAfter: 24 * time.Hour,
	})
	require.NoError(t, err)
	relaxedReport, err := relaxed.Sweep(context.Background())
	require.NoError(t, err)
	assert.Zero(t, relaxedReport.Counts[DivergenceAcceptanceStale],
		"and with a one-day window the SAME row is still in flight. If both sweeps agree, the "+
			"option is not reaching the query — an operator who widens the window to quiet a "+
			"noisy report would see nothing change and go looking for the fault somewhere else")
}

// ---------------------------------------------------------------------------
// (d) WHO GETS THE PAGE when the budget runs out
// ---------------------------------------------------------------------------

// TestDivergenceExamplesAreDealtAcrossClassesRatherThanFirstComeFirstServed is
// the only test in the suite that seeds TWO bulk classes, and that is why it
// exists.
//
// Every other fixture here overflows ONE class, so first-come-first-served and
// round-robin produce identical reports and neither is pinned. The failure the
// staging exists to prevent needs a second class to be visible at all: with
// entries appended in comparison order, the class swept FIRST spends the entire
// budget and every later class arrives with a non-zero count and NOT ONE
// EXAMPLE. That is the exact thing DivergenceEntry.Subject exists to prevent —
// "a class and a count alone give an operator a number they cannot investigate"
// — happening to the classes that need investigating most, at the moment they
// need it, because a bulk class is precisely what an incident looks like.
//
// The fixture is built on the sweep's own order: persona vote events are the
// FIRST comparison and the unknown-delivery classes are the LAST, so the small
// class here is the one a first-come allocation would starve.
func TestDivergenceExamplesAreDealtAcrossClassesRatherThanFirstComeFirstServed(t *testing.T) {
	h := newHarness(t)
	world := newModerationWorld(t, h)
	h.admin.SetDivergenceReconciler(newDivergenceReconciler(t, h))
	deliverEverythingQueued(t, h.db)
	_ = world

	// The BULK class, swept first: more persona vote events than the whole page.
	personaActorID := actorIDOf(t, h.db, mtAuthorDID)
	overflowPersonaVotes(t, h.db, personaActorID, MaxDivergenceEntries+1)

	// The SMALL class, swept last: three deliveries nobody answered. Three is
	// deliberately tiny — an operator can act on all of them, and a report that
	// shows none of them is the regression.
	late := []string{
		seedPoisonedComment(t, h.db, "dv-fair-1", "transport", 0),
		seedPoisonedComment(t, h.db, "dv-fair-2", "transport", 0),
		seedPoisonedComment(t, h.db, "dv-fair-3", "transport", 0),
	}

	report := fetchDivergence(t, h)

	require.Len(t, report.Entries, MaxDivergenceEntries,
		"precondition: the budget really is exhausted — with room to spare every class gets "+
			"everything and this test would pass without an allocation policy at all")
	require.Equal(t, MaxDivergenceEntries+1, report.Counts[DivergencePersonaVoteEvent],
		"precondition: the bulk class overflows the page")
	require.Equal(t, len(late), report.Counts[DivergenceDeliveryUnknownUnanswered],
		"precondition: the late class found exactly the rows this fixture seeded")

	assert.ElementsMatch(t, late, subjectsOfClass(report, DivergenceDeliveryUnknownUnanswered),
		"the class swept LAST still gets its examples. Appending entries in comparison order "+
			"hands the whole budget to whoever read first, and every later class then arrives "+
			"with a count an operator cannot investigate — no subject to look up, no inbox to "+
			"ask, on the endpoint they opened BECAUSE something is wrong")

	// Every class the sweep counted must be able to show its work.
	for class, count := range report.Counts {
		if count == 0 {
			continue
		}
		assert.NotEmpty(t, subjectsOfClass(report, class),
			"class %q has a count of %d and no example: a number with nothing to look at is the "+
				"one thing this report promises never to serve", class, count)
	}

	// ...AND THE BUDGET IS NOT SHARED OUT EQUALLY, which is the other way to get
	// this wrong. An equal share computed up front would cap the bulk class at a
	// fraction of the page and leave most of it empty, so a single-class incident
	// would come back with a quarter of the examples it could have had. Dealing a
	// card at a time gives every class its rows first and spends everything left
	// on whoever still has some.
	bulk := subjectsOfClass(report, DivergencePersonaVoteEvent)
	assert.Equal(t, MaxDivergenceEntries-len(late), len(bulk),
		"the bulk class keeps every slot the small classes did not need: round-robin fills the "+
			"page, an equal share wastes it, and the report's bound is only worth having if the "+
			"page is actually full")

	assert.True(t, report.Truncated,
		"and the report still says it is smaller than the problem it describes")
}

// ---------------------------------------------------------------------------
// (e) The sweep's own deadline, and what a failed pass leaves behind
// ---------------------------------------------------------------------------

// deadlineDivergences records the context the sweep hands its reads.
type deadlineDivergences struct {
	store.Divergences
	mu          sync.Mutex
	deadline    time.Time
	hasDeadline bool
}

func (d *deadlineDivergences) PersonaVoteEvents(ctx context.Context) ([]store.PersonaVoteEvent, error) {
	deadline, ok := ctx.Deadline()
	d.mu.Lock()
	d.deadline, d.hasDeadline = deadline, ok
	d.mu.Unlock()
	return d.Divergences.PersonaVoteEvents(ctx)
}

func (d *deadlineDivergences) Deadline() (time.Time, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.deadline, d.hasDeadline
}

// TestDivergenceSweepBoundsItsReadsWithADeadline pins divergenceSweepTimeout at
// the only place it is observable: the context the reads actually run under.
//
// The bound exists because of the POOL, not the clock. Every comparison is a
// multi-table join taken from the same 25 connections that serve inbound
// ingestion and the delivery worker, there is no statement_timeout anywhere in
// this codebase, and GET /admin/divergence makes a sweep reachable from outside
// on an unrated GET — so an operator refreshing the report during an incident
// can deepen the incident. A sweep with no deadline does not merely run late; it
// holds pool connections for as long as the database will let it, and it is
// slowest exactly when federation most needs them.
//
// THE CALLER HERE HAS NO DEADLINE OF ITS OWN, which is the production case: a
// curl carries none, and the background Run loop's context carries none either.
// If the sweep did not impose one, these reads would run unbounded.
func TestDivergenceSweepBoundsItsReadsWithADeadline(t *testing.T) {
	h := newHarness(t)

	watched := &deadlineDivergences{Divergences: store.NewDivergences(h.db)}
	reconciler, err := NewDivergenceReconciler(DivergenceOptions{DB: h.db, Divergences: watched})
	require.NoError(t, err)

	// context.Background(): no deadline, exactly like a curl and like Run.
	_, err = reconciler.Sweep(context.Background())
	require.NoError(t, err)

	deadline, ok := watched.Deadline()
	require.True(t, ok,
		"the reads must run under a DEADLINE the sweep imposed. The caller supplied none — that "+
			"is what a curl and the background loop both look like — so without one here a "+
			"pathological pass pins pool connections indefinitely, on the endpoint an operator "+
			"reaches for because the database is already struggling")
	remaining := time.Until(deadline)
	assert.LessOrEqual(t, remaining, divergenceSweepTimeout,
		"and the budget is divergenceSweepTimeout (%s), never longer: it has to be well inside "+
			"the sweep cadence or a slow pass queues behind its own predecessor", divergenceSweepTimeout)
	assert.Greater(t, remaining, divergenceSweepTimeout-time.Minute,
		"and generously long: a deadline that fired on a merely slow pass would report failures "+
			"that are not divergences, which is the one thing a report like this cannot afford")
}

// blockingDivergences is the pathological read: it stops on the context and
// never returns until that context is done.
//
// It is how a sweep is made to abort in a test without waiting out the real
// two-minute budget. The mechanism under test is the same one either way — the
// reads run under a context derived from the caller's, and they end when it ends
// — and driving it by cancellation exercises the derivation, which the deadline
// alone cannot.
type blockingDivergences struct {
	store.Divergences
	entered chan struct{}
}

func (b *blockingDivergences) PersonaVoteEvents(ctx context.Context) ([]store.PersonaVoteEvent, error) {
	select {
	case b.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestDivergenceSweepAbortsWhenItsContextEndsAndLeavesTheGaugesStanding drives a
// sweep that would never finish on its own.
//
// TWO CLAIMS, and the second is the one an operator lives with. The pass ABORTS
// rather than blocking forever — the reads run under the sweep's own context,
// derived from the caller's, so cancelling the caller reaches a read that is
// already inside the database. And the gauges KEEP THEIR PREVIOUS VALUES: a
// sweep that could not read must never write a zero, because a zero written by a
// pass that measured nothing is a claim of health made at the moment nobody
// could check, and it overwrites the last real number the operator was watching.
//
// THE FAILURE COUNTER DELIBERATELY DOES NOT MOVE HERE. This sweep was cut short
// by its CALLER, which is what shutdown looks like, and counting it would make
// the number mean "restarts plus failures" — a number nobody can act on. The
// counter's positive case is the failing-read test below.
func TestDivergenceSweepAbortsWhenItsContextEndsAndLeavesTheGaugesStanding(t *testing.T) {
	h := newHarness(t)
	world := newModerationWorld(t, h)
	deliverEverythingQueued(t, h.db)
	_ = world

	// A real divergence first, so the gauge carries a NON-ZERO value an aborted
	// sweep could overwrite. A fixture whose healthy value is 0 cannot tell "left
	// standing" from "written as zero".
	insertVoteEvent(t, h.db, "https://lemmy.world/activities/like/dv-abort",
		actorIDOf(t, h.db, mtAuthorDID), "up")
	h.admin.SetDivergenceReconciler(newDivergenceReconciler(t, h))
	require.Equal(t, http.StatusOK, h.adminRequest(http.MethodGet, "/admin/divergence", nil).Code)
	require.Equal(t, 1, gaugeValue(t, h, MetricDivergencePersonaVoteEvents),
		"precondition: a completed sweep published a real number")
	failuresBefore := gaugeValue(t, h, MetricDivergenceSweepFailures)

	blocking := &blockingDivergences{
		Divergences: store.NewDivergences(h.db),
		entered:     make(chan struct{}, 1),
	}
	stuck, err := NewDivergenceReconciler(DivergenceOptions{DB: h.db, Divergences: blocking})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	swept := make(chan error, 1)
	go func() { _, sweepErr := stuck.Sweep(ctx); swept <- sweepErr }()

	select {
	case <-blocking.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("precondition: the sweep must reach the read this test blocks in")
	}
	cancel()

	select {
	case sweepErr := <-swept:
		require.Error(t, sweepErr,
			"a sweep whose reads were cut short is a FAILURE, not an empty report: returning the "+
				"classes that happened to finish publishes a claim about state nobody read")
		assert.ErrorIs(t, sweepErr, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("the sweep must END when its context does. The reads run under a context DERIVED " +
			"from the caller's precisely so that a shutdown — or the two-minute budget — reaches " +
			"a query already inside the database; a sweep built on a fresh background context " +
			"holds its pool connections for as long as the database will let it, which is the " +
			"failure divergenceSweepTimeout exists to bound")
	}

	assert.Equal(t, 1, gaugeValue(t, h, MetricDivergencePersonaVoteEvents),
		"THE PREVIOUS VALUE STANDS. An aborted pass measured nothing, and a zero written by a "+
			"pass that measured nothing reads as health at exactly the moment nobody could check")
	assert.Equal(t, failuresBefore, gaugeValue(t, h, MetricDivergenceSweepFailures),
		"and the failure counter does NOT move for a sweep its own caller cancelled: that is "+
			"shutdown, and a counter inflated on every restart means 'restarts plus failures', "+
			"which is a number nobody can alert on")
}

// TestDivergenceSweepFailureAndAgeAreVisibleToAnOperator reads the two freshness
// metrics through the surface an operator reads them through.
//
// THEY ARE READ FROM /admin/metrics, NOT FROM expvar. scopedMetrics serves only
// keys carrying the tidepool prefix, so a metric named without it is published,
// behaves perfectly, and appears nowhere — indistinguishable from a check that
// never runs. A test that pokes the Var it just watched proves the sweep can
// call Set; it proves nothing about whether anyone can see the result.
//
// WHY THESE TWO EXIST AT ALL: the seven class gauges are all SET by a successful
// sweep, so a sweep that has stopped running leaves seven low, stable, entirely
// healthy-looking numbers behind it. The age says when those numbers were
// established and the failure count says whether the sweep has been trying and
// losing. Read together they are the difference between "nothing is diverging"
// and "nothing is measuring"; either one alone can be read as health.
func TestDivergenceSweepFailureAndAgeAreVisibleToAnOperator(t *testing.T) {
	h := newHarness(t)
	world := newModerationWorld(t, h)
	deliverEverythingQueued(t, h.db)
	_ = world

	insertVoteEvent(t, h.db, "https://lemmy.world/activities/like/dv-age",
		actorIDOf(t, h.db, mtAuthorDID), "up")
	h.admin.SetDivergenceReconciler(newDivergenceReconciler(t, h))
	require.Equal(t, http.StatusOK, h.adminRequest(http.MethodGet, "/admin/divergence", nil).Code)
	require.Equal(t, 1, gaugeValue(t, h, MetricDivergencePersonaVoteEvents),
		"precondition: a completed sweep published a real number")

	fresh := floatGaugeValue(t, h, MetricDivergenceSweepAgeSeconds)
	assert.GreaterOrEqual(t, fresh, 0.0,
		"a sweep has published, so the age is a real measurement rather than the unswept "+
			"sentinel: %s reads negative only before the first successful publication, because a "+
			"0 there would say 'swept just now' about a process that has never swept at all",
		MetricDivergenceSweepAgeSeconds)
	assert.Less(t, fresh, time.Minute.Seconds(),
		"and it is SECONDS old, because the sweep it dates finished during this test")

	// PULLED, NOT PUSHED, and this is the whole point of the age gauge. A value
	// written at publication time would be the one number that stops updating at
	// exactly the moment it starts to matter — a sweep that has stopped running
	// would report the age it had when it last ran, forever.
	climbing := floatGaugeValue(t, h, MetricDivergenceSweepAgeSeconds)
	assert.Greater(t, climbing, fresh,
		"%s must CLIMB between two reads of the same standing numbers. Pushed at publication it "+
			"would freeze with the class gauges it is supposed to date, and seven frozen gauges "+
			"beside a frozen age is exactly what a healthy bridge looks like",
		MetricDivergenceSweepAgeSeconds)

	// Now a sweep that fails on a read, with a live caller: this is a FAILING
	// pass, not a shutdown.
	failuresBefore := gaugeValue(t, h, MetricDivergenceSweepFailures)
	faulty := &failingDivergences{
		Divergences: store.NewDivergences(h.db),
		err:         stderrors.New("connection reset by peer"),
	}
	failing, err := NewDivergenceReconciler(DivergenceOptions{DB: h.db, Divergences: faulty})
	require.NoError(t, err)
	h.admin.SetDivergenceReconciler(failing)
	require.Equal(t, http.StatusInternalServerError,
		h.adminRequest(http.MethodGet, "/admin/divergence", nil).Code,
		"precondition: the sweep really did fail")
	require.NotZero(t, faulty.Calls(), "precondition: the sweep really did try to read")

	assert.Equal(t, failuresBefore+1, gaugeValue(t, h, MetricDivergenceSweepFailures),
		"%s must move on a failed pass. Leaving the previous gauges standing is right, and on "+
			"its own it is SILENT: a sweep that fails forever leaves seven plausible numbers "+
			"frozen and a log line nobody is watching, and this counter plus the climbing age is "+
			"the only thing that tells those apart from a healthy bridge",
		MetricDivergenceSweepFailures)

	afterFailure := floatGaugeValue(t, h, MetricDivergenceSweepAgeSeconds)
	assert.Greater(t, afterFailure, climbing,
		"and the age KEEPS CLIMBING through the failure: it dates the standing numbers, which a "+
			"failed sweep did not refresh. An age reset by a pass that published nothing would "+
			"report the stale gauges as fresh, which is the single reading that would hide a "+
			"sweep that has stopped working")
	assert.Equal(t, 1, gaugeValue(t, h, MetricDivergencePersonaVoteEvents),
		"while the class gauge keeps its last real value: the counter and the age exist so that "+
			"standing numbers can be told from measured ones, not so they can be overwritten")
}
