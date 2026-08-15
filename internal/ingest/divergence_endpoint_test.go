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
