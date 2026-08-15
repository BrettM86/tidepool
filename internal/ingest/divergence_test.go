package ingest

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TASK 17e — THE RECONCILIATION JOB, FROM OUTSIDE.
//
// Decision 19 asks for a sweep that compares atproto state against outbound
// state and reports divergence through metrics and an admin report — and NEVER
// self-heals. That prohibition is the whole design, not a caveat on it:
//
//   - A reconciler that WRITES has, by construction, no witness. It is the only
//     thing that reads both sides, so when it is wrong there is nothing left to
//     notice; a bad rule silently rewrites the state it was measuring, and the
//     next sweep agrees with itself.
//   - Half of what it finds is genuinely UNKNOWABLE from here. A poisoned
//     delivery may or may not have reached the peer; a held settlement is a
//     delivery that succeeded and looks pending. "Repairing" those means acting
//     on a guess, at instances that do not un-delete.
//   - And the actions available to it are the irreversible ones — re-sending,
//     cancelling, deleting on a peer. An operator reading a report can decide;
//     a loop cannot.
//
// So the outer contract has two halves, and the second is the load-bearing one:
// the report NAMES the divergence, and the sweep CHANGES NOTHING.
//
// THE FIXTURE is the first divergence class, produced through the real paths:
// a native post is admitted into a bridged community — the acceptance record is
// written into the community repo, so Coves shows the post in that community —
// and then the author opts out, which cancels the queued Create before it
// leaves. Lemmy therefore never hears of a post that the community's own repo
// says is in it. Nothing about that state is wrong to have; it is exactly what
// the two tiers of 17d promise. What is wrong is for nobody to be able to SEE
// it, which is what this sweep is for.

const (
	dvPostRKey = "3lzdvpost00001"
	dvPostRev  = "3lzdvrev000001"
	dvOptOut   = "3lzdvrev000002"

	// dvAcceptanceUndelivered is the report class for THIS fixture's divergence:
	// an acceptance whose delivery was CANCELLED. The classes are keyed on the
	// delivery state — cancelled / poisoned / stale — because the three need
	// three different operator responses; see the classification test below.
	// This one is the cancelled sibling because a cancellation is what the
	// opt-out in this fixture produces.
	dvAcceptanceUndelivered = "acceptance-undelivered-cancelled"

	// dvGauge is the sweep-set gauge for that class. The tidepool_ prefix is not
	// cosmetic: scopedMetrics (follow.go) filters /admin/metrics to keys with it,
	// so a gauge named anything else is invisible to the operator in exactly the
	// way a drop site that never fires is invisible.
	dvGauge = "tidepool_divergence_acceptance_undelivered_cancelled"
)

// snapshotTables enumerates the tables from the database itself. See
// allBaseTables for why the list is not written down here.

// TestDivergenceReportNamesAnUndeliveredAcceptanceAndWritesNothing is the OUTER
// acceptance test for 17e.
func TestDivergenceReportNamesAnUndeliveredAcceptanceAndWritesNothing(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	world := newModerationWorld(t, h)
	h.admin.SetDivergenceReconciler(newDivergenceReconciler(t, h))

	// --- GIVEN: everything the world already queued really did reach the
	//     community. This is the DISCRIMINATION half of the fixture: the world
	//     opens with an accepted post of its own, so without this the run holds
	//     two undelivered acceptances and a sweep that simply listed every
	//     acceptance would be indistinguishable from one that compares.
	deliverEverythingQueued(t, h.db)

	// --- AND: a second post accepted into community A, whose delivery is then
	//     cancelled by the author's own opt-out.
	admitPost(t, world, mtAuthorDID, dvPostRKey, world.communityADID, dvPostRev, 1_775_000_060_000_001)
	postURI := "at://" + mtAuthorDID + "/social.coves.community.postv2/" + dvPostRKey
	require.Equal(t, []string{"pending"}, deliveryStatesForPost(t, h.db, postURI),
		"precondition: the acceptance enqueued a Create that has not gone out yet")

	require.NoError(t, world.dispatcher.HandleEvent(ctx,
		odFederationEvent(t, mtAuthorDID, dvOptOut, "create", false, false, 1_775_000_061_000_001)))
	require.Equal(t, []string{"cancelled"}, deliveryStatesForPost(t, h.db, postURI),
		"precondition: and the opt-out cancelled it, so the peer will never receive this post")
	require.Equal(t, []string{"delivered"}, deliveryStatesForPost(t, h.db, mtPostATURI),
		"precondition: while the world's OTHER accepted post is delivered and stays delivered "+
			"— the two differ in delivery state and in nothing else")

	acceptanceStands(t, h, world.communityADID, postURI)

	// --- The state of the world, in full, immediately before the sweep.
	before := snapshotTables(t, h.db)

	// --- WHEN: the operator asks what diverges.
	rec := h.adminRequest(http.MethodGet, "/admin/divergence", nil)
	require.Equal(t, http.StatusOK, rec.Code,
		"GET /admin/divergence must serve the report (body: %s)", rec.Body.String())

	var report divergenceReport
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &report),
		"the report is the operator's whole interface to this sweep, so it is JSON with a "+
			"stable shape (body: %s)", rec.Body.String())

	// --- THEN: it names THIS post, under the class that says what is wrong.
	require.NotEmpty(t, report.Entries,
		"a report with no entries is the same output this sweep produces when everything is "+
			"healthy — and this world is not healthy: a community repo says it accepted a post "+
			"that no peer has ever been told about")
	var matched int
	for _, entry := range report.Entries {
		if entry.Class == dvAcceptanceUndelivered && entry.Subject == postURI {
			matched++
		}
	}
	assert.Equal(t, 1, matched,
		"the report must name %s under class %q, exactly once. Naming the class without the "+
			"SUBJECT gives an operator a number they cannot act on, and the only action "+
			"available to a human here — re-drive it, or accept that the post is Coves-only — "+
			"needs to know which post. Entries were: %+v", postURI, dvAcceptanceUndelivered, report.Entries)
	assert.Equal(t, 1, report.Counts[dvAcceptanceUndelivered],
		"and the report's own count agrees with its entries")

	// ...and NOT the post the community actually received. This is what makes
	// the class a comparison rather than a listing: the two posts differ in
	// nothing an acceptance-side query can see — same author, same community,
	// same repo, adjacent rkeys — and only the delivery state tells them apart.
	for _, entry := range report.Entries {
		assert.NotEqual(t, mtPostATURI, entry.Subject,
			"a DELIVERED acceptance is not a divergence: the community repo says the post is "+
				"in the community and the peer has it, which is the healthy state. Reporting it "+
				"turns the report into a list of every post the bridge has ever accepted, and an "+
				"operator who cannot tell the finding from the background stops reading it")
	}

	// --- AND: the gauge an operator watches reads 1, through the surface they
	//     actually watch it through.
	assert.Equal(t, 1, gaugeValue(t, h, dvGauge),
		"%s must read 1 on /admin/metrics. It is a SWEEP-SET gauge, not a counter: it reports "+
			"how many divergences stand RIGHT NOW, so it must be assigned from each sweep's "+
			"result — a counter that only ever adds would show a growing number for one "+
			"unchanging problem, and would never return to zero when it is resolved", dvGauge)

	// --- AND, THE POINT: the sweep changed NOTHING.
	after := snapshotTables(t, h.db)
	require.Equal(t, len(before), len(after),
		"the sweep must not CREATE or DROP a table either — the comparison is over whatever the "+
			"schema holds, so a new one appearing is itself a write")
	for table := range before {
		assert.Equal(t, before[table], after[table],
			"REPORTING IS THE WHOLE JOB: %s must be byte-identical across the sweep. A "+
				"reconciler that writes has no witness — it is the only thing that reads both "+
				"sides, so a wrong rule silently rewrites the state it was measuring and every "+
				"later sweep agrees with itself. And the writes available here are the "+
				"irreversible ones: re-driving a cancelled delivery publishes content the user "+
				"withdrew, deleting an acceptance takes a post out of a community on a guess, "+
				"and re-committing one writes to a repo whose author never asked. An operator "+
				"reading this report can decide between those; a loop cannot", table)
	}
}

// TASK 17e CYCLE 1, AT THE SWEEP — the standing persona-vote invariant.
//
// The join and its two lethal-confusion controls live at the store, beside the
// table name. What only this layer can pin is the OPERATOR CONTRACT: the
// invariant is reported as a named class with the offending row in it, and the
// gauge an operator watches is SET by every sweep — including to zero.
//
// Zero is the interesting value here, and it is why this assertion is not
// redundant with the store's. This invariant is expected to hold forever, so
// the gauge spends its whole life at 0; if a sweep that finds nothing simply
// never touches the Var, the key is ABSENT from /admin/metrics, and "healthy"
// is then indistinguishable from "the sweep never ran" — which is exactly the
// state a broken schedule produces, and the state an operator would most want
// to notice.
const (
	// dvPersonaVoteClass is the report class for an inbound vote attributed to
	// one of our own personas.
	dvPersonaVoteClass = "persona-vote-event"
	// dvPersonaVoteGauge is its sweep-set gauge. tidepool-prefixed or invisible.
	dvPersonaVoteGauge = "tidepool_divergence_persona_vote_events"
)

func TestDivergenceReportCountsPersonaVoteEventsAndPublishesZeroWhenHealthy(t *testing.T) {
	h := newHarness(t)
	world := newModerationWorld(t, h)
	h.admin.SetDivergenceReconciler(newDivergenceReconciler(t, h))
	_ = world

	// --- GIVEN: genuine Lemmy voters, and none of ours.
	insertVoteEvent(t, h.db, "https://lemmy.world/activities/like/dv-1",
		"https://lemmy.world/u/genuine", "up")
	insertVoteEvent(t, h.db, "https://lemmy.ml/activities/dislike/dv-2",
		"https://lemmy.ml/u/another", "down")

	// --- THEN: the sweep publishes the invariant as HOLDING.
	require.Equal(t, http.StatusOK, h.adminRequest(http.MethodGet, "/admin/divergence", nil).Code,
		"GET /admin/divergence must serve the report")
	assert.Equal(t, 0, gaugeValue(t, h, dvPersonaVoteGauge),
		"%s must be PUBLISHED AND ZERO while the invariant holds. A gauge only written when "+
			"something is found is absent exactly when everything is fine — so a healthy bridge "+
			"and a sweep that never ran look identical on /admin/metrics, and the second one is "+
			"the failure an operator most needs to see", dvPersonaVoteGauge)
	for _, entry := range fetchDivergence(t, h).Entries {
		assert.NotEqual(t, dvPersonaVoteClass, entry.Class,
			"and no entry: every vote in this world was cast by a real person on another "+
				"instance")
	}

	// --- GIVEN: one vote_events row attributed to one of OUR personas.
	personaActorID := actorIDOf(t, h.db, mtAuthorDID)
	echoedActivity := "https://lemmy.world/activities/like/dv-echo"
	insertVoteEvent(t, h.db, echoedActivity, personaActorID, "up")

	// --- THEN: the sweep names it.
	report := fetchDivergence(t, h)
	var matched int
	for _, entry := range report.Entries {
		if entry.Class == dvPersonaVoteClass && entry.Subject == echoedActivity {
			matched++
		}
	}
	assert.Equal(t, 1, matched,
		"the report must name the offending vote_events row under class %q. This invariant is "+
			"'zero forever', which is the kind of claim that stops being true silently: the "+
			"subject's tally now counts one vote twice — once as this inbound event, once as "+
			"the delivered outbound row the 17b reseed subtracts — and no other check in the "+
			"system will ever mention it. Entries were: %+v", dvPersonaVoteClass, report.Entries)
	assert.Equal(t, 1, report.Counts[dvPersonaVoteClass])
	assert.Equal(t, 1, gaugeValue(t, h, dvPersonaVoteGauge),
		"and the gauge moves to 1 with it: SET from this sweep's result, so it returns to zero "+
			"by itself once the row is cleaned up rather than reporting a problem that no longer "+
			"exists")
}

// insertVoteEvent writes one inbound vote row, as the aggregator would.
func insertVoteEvent(t *testing.T, db *sql.DB, activityID, voterAPID, direction string) {
	t.Helper()
	_, err := db.ExecContext(context.Background(), `
		INSERT INTO vote_events (activity_id, voter_ap_id, subject_ap_id, direction)
		VALUES ($1, $2, $3, $4)`, activityID, voterAPID, mtPostAPID, direction)
	require.NoError(t, err)
}

// actorIDOf reads a persona's AP actor id from storage rather than composing it
// from the origin and the DID: the id a vote must match is the one that was
// MINTED, and a test that rebuilds the string would keep passing if minting
// changed its shape.
func actorIDOf(t *testing.T, db *sql.DB, did string) string {
	t.Helper()
	var actorID string
	require.NoError(t, db.QueryRowContext(context.Background(),
		`SELECT actor_id FROM ap_actors WHERE did = $1`, did).Scan(&actorID),
		"the world must have minted a persona for %s", did)
	return actorID
}

// divergenceReport is the wire shape of GET /admin/divergence.
type divergenceReport struct {
	Entries []struct {
		Class   string `json:"class"`
		Subject string `json:"subject"`
		Detail  string `json:"detail"`
	} `json:"entries"`
	Counts map[string]int `json:"counts"`
	// Truncated says the entry list was capped. It is read from the WIRE
	// because that is where an operator reads it: a flag the sweep sets and the
	// endpoint drops would leave a truncated list looking complete.
	Truncated bool `json:"truncated"`
}

func fetchDivergence(t *testing.T, h *harness) divergenceReport {
	t.Helper()
	rec := h.adminRequest(http.MethodGet, "/admin/divergence", nil)
	require.Equal(t, http.StatusOK, rec.Code,
		"GET /admin/divergence must serve the report (body: %s)", rec.Body.String())
	var report divergenceReport
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &report),
		"the report is the operator's whole interface to this sweep (body: %s)", rec.Body.String())
	return report
}

// newDivergenceReconciler builds the reconciler over the harness's database,
// exactly as production would: both sides of every comparison are LOCAL, so the
// database is the only dependency there is. A reconciler that needed the AP
// client would already have failed decision 19.
func newDivergenceReconciler(t *testing.T, h *harness) *DivergenceReconciler {
	t.Helper()
	reconciler, err := NewDivergenceReconciler(DivergenceOptions{
		DB:     h.db,
		Logger: slog.New(slog.NewTextHandler(h.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	require.NoError(t, err)
	require.NotNil(t, reconciler)
	return reconciler
}

// deliveryStatesForPost lists the delivery states of the activities that
// carry ONE post, joined through the outbound state that records which AP id a
// record federates as. Two posts by one author into one community share an
// actor and an ordering key, so the per-actor helpers cannot separate them —
// and separating them is the whole fixture.
func deliveryStatesForPost(t *testing.T, db *sql.DB, postURI string) []string {
	t.Helper()
	return queryStrings(t, db, `
		SELECT d.state
		  FROM outbound_deliveries d
		  JOIN outbound_activities a ON a.activity_id = d.activity_id
		  JOIN outbound_objects o ON position(o.ap_object_id in a.payload::text) > 0
		 WHERE o.at_uri = $1
		 ORDER BY d.target_inbox`, postURI)
}

// deliverEverythingQueued marks every delivery standing in the queue as
// delivered — "the peer received all of this" — so the divergence the test
// then creates is the only one in the world.
//
// It is written as a fixture rather than by running a worker because the wire
// is not what is under test here, and it REQUIRES rows: if the world stops
// enqueueing on admission, the premise dies loudly here instead of quietly
// making the count assertion below pass for the wrong reason.
func deliverEverythingQueued(t *testing.T, db *sql.DB) {
	t.Helper()
	result, err := db.ExecContext(context.Background(), `
		UPDATE outbound_deliveries
		   SET state = 'delivered', claimed_until = NULL, last_status_code = 202, updated_at = now()
		 WHERE state = 'pending'`)
	require.NoError(t, err)
	affected, err := result.RowsAffected()
	require.NoError(t, err)
	require.NotZero(t, affected,
		"the world must have queued something to deliver: with an empty queue this fixture "+
			"proves nothing about telling delivered acceptances from undelivered ones")
}

// acceptanceStands asserts the community repo really does carry the acceptance
// for this post — the atproto half of the divergence. Without it, "the peer
// never got the post" would be true of a post that was never accepted either,
// which is not a divergence at all but ordinary rejection.
func acceptanceStands(t *testing.T, h *harness, communityDID, postURI string) {
	t.Helper()
	_, _, err := h.manager.GetRecord(context.Background(),
		communityDID, "social.coves.community.acceptance", testDigestRKey(postURI))
	require.NoError(t, err,
		"precondition: the community repo carries the acceptance, so Coves shows this post in "+
			"the community while Lemmy has never heard of it — that gap IS the divergence (%v)", err)
}

// gaugeValue reads one tidepool_ gauge from /admin/metrics.
//
// It goes through the ENDPOINT rather than through expvar directly, for two
// reasons. A test that reads the Var it just watched proves the sweep can call
// Set, not that an operator can see the result — and the prefix filter in
// scopedMetrics is exactly the kind of silent drop that a direct read cannot
// catch: a gauge named "divergence_…" behaves perfectly and appears nowhere.
func gaugeValue(t *testing.T, h *harness, name string) int {
	t.Helper()
	rec := h.adminRequest(http.MethodGet, "/admin/metrics", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var metrics map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &metrics),
		"/admin/metrics must stay parseable JSON (body: %s)", rec.Body.String())
	raw, ok := metrics[name]
	require.True(t, ok,
		"%s must be published on /admin/metrics. A gauge whose name does not start with "+
			"'tidepool' is filtered out by scopedMetrics and is indistinguishable from a "+
			"divergence that never fired. Published keys: %v", name, keysOf(metrics))
	var value int
	require.NoError(t, json.Unmarshal(raw, &value), "%s must be a number, got %s", name, raw)
	return value
}

// floatGaugeValue is gaugeValue for a metric that is not a whole number.
//
// The age gauge is seconds as a float — an expvar.Func over a stored timestamp —
// so reading it through gaugeValue would fail on the decimal rather than on
// anything true about the sweep. It goes through the ENDPOINT for the same
// reason gaugeValue does: the tidepool prefix filter in scopedMetrics is exactly
// the kind of silent drop a direct expvar read cannot catch.
func floatGaugeValue(t *testing.T, h *harness, name string) float64 {
	t.Helper()
	rec := h.adminRequest(http.MethodGet, "/admin/metrics", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	var metrics map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &metrics),
		"/admin/metrics must stay parseable JSON (body: %s)", rec.Body.String())
	raw, ok := metrics[name]
	require.True(t, ok,
		"%s must be published on /admin/metrics. A metric whose name does not start with "+
			"'tidepool' is filtered out by scopedMetrics and is invisible in exactly the way a "+
			"check that never runs is invisible. Published keys: %v", name, keysOf(metrics))
	var value float64
	require.NoError(t, json.Unmarshal(raw, &value), "%s must be a number, got %s", name, raw)
	return value
}

func keysOf(m map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	return keys
}

// snapshotTables captures every row of every named table as full-row JSON.
//
// WHOLE ROWS, not a column or a count. The write this guards against is not a
// hypothetical column-level bug: it is a well-meaning "while we are here, fix
// it" — a redrive that flips one state, a repin that moves a CID, a stamp on
// updated_at. Any of those changes some column of some row, and only a
// comparison that carries every column can say so. row_to_json also survives a
// migration adding a column, which a hand-listed projection would silently stop
// covering on the day the schema grows.
func snapshotTables(t *testing.T, db *sql.DB) map[string][]string {
	t.Helper()
	tables := allBaseTables(t, db)
	require.Greater(t, len(tables), 15,
		"the enumeration must actually find the schema: a query that returned a handful of "+
			"tables would make this whole assertion a spot-check wearing the clothes of an "+
			"exhaustive one")
	snapshot := make(map[string][]string, len(tables))
	for _, table := range tables {
		rows, err := db.QueryContext(context.Background(),
			`SELECT row_to_json(t)::text FROM `+table+` t ORDER BY 1`)
		require.NoError(t, err, "snapshot %s", table)
		var out []string
		for rows.Next() {
			var row string
			require.NoError(t, rows.Scan(&row))
			out = append(out, row)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		snapshot[table] = out
	}
	return snapshot
}

// TASK 17e CYCLE 2, AT THE SWEEP — the three classes, and the two posts that
// must not appear in any of them.
//
// The join and its false-positive control live at the store, beside the query.
// What only this layer can pin is what an operator ends up looking at: three
// SEPARATE classes with three SEPARATE counts and three SEPARATE gauges, so the
// report can be triaged. A single "undelivered" bucket would tell an operator
// how many posts are missing from Lemmy and nothing about what to do, and the
// three answers are genuinely different work: a cancelled delivery is usually
// correct and wants no action, a poisoned one is redrivable through a surface
// that already exists, and a stale one is a queue that has stopped moving.
const (
	dvUndeliveredCancelled = "acceptance-undelivered-cancelled"
	dvUndeliveredPoisoned  = "acceptance-undelivered-poisoned"
	dvUndeliveredStale     = "acceptance-undelivered-stale"

	dvGaugeCancelled = "tidepool_divergence_acceptance_undelivered_cancelled"
	dvGaugePoisoned  = "tidepool_divergence_acceptance_undelivered_poisoned"
	dvGaugeStale     = "tidepool_divergence_acceptance_undelivered_stale"
)

func TestDivergenceReportClassifiesUndeliveredAcceptancesByDeliveryState(t *testing.T) {
	h := newHarness(t)
	world := newModerationWorld(t, h)
	h.admin.SetDivergenceReconciler(newDivergenceReconciler(t, h))

	// The world's own accepted post really reached the community.
	deliverEverythingQueued(t, h.db)

	// Four more posts by the same author into the same community, differing
	// ONLY in what happened to their delivery.
	cancelled := admitDivergencePost(t, h, world, "3lzdvcls00001", "3lzdvcls00011", 1_775_000_070_000_001)
	poisoned := admitDivergencePost(t, h, world, "3lzdvcls00002", "3lzdvcls00012", 1_775_000_070_000_002)
	stale := admitDivergencePost(t, h, world, "3lzdvcls00003", "3lzdvcls00013", 1_775_000_070_000_003)
	// The IN-FLIGHT post: accepted moments ago, delivery still queued. This is
	// the ordinary state of every post between acceptance and delivery, and it
	// is the false positive that would put every healthy post in the report.
	inFlight := admitDivergencePost(t, h, world, "3lzdvcls00004", "3lzdvcls00014", 1_775_000_070_000_004)

	setDeliveryState(t, h.db, cancelled, "cancelled", "", 2*time.Hour)
	setDeliveryState(t, h.db, poisoned, "poisoned", "4xx", 2*time.Hour)
	// The age is expressed INDEPENDENTLY of the constant — longer than the
	// widest window the bounds test permits — so this fixture keeps meaning
	// "unambiguously stale" whatever value GREEN chooses, and cannot be made to
	// pass by moving the threshold.
	setDeliveryState(t, h.db, stale, "pending", "", 25*time.Hour)

	report := fetchDivergence(t, h)

	assert.Equal(t, []string{cancelled}, subjectsOfClass(report, dvUndeliveredCancelled),
		"a CANCELLED delivery is its own class: the post is on Coves and will never be on "+
			"Lemmy, and that was a decision — a ban or an opt-out — so the operator's job is "+
			"to know it happened, not to fix it")
	assert.Equal(t, []string{poisoned}, subjectsOfClass(report, dvUndeliveredPoisoned),
		"a POISONED delivery is its own class: this one failed and is redrivable through "+
			"POST /admin/outbound/redrive, which is a different action entirely")
	assert.Equal(t, []string{stale}, subjectsOfClass(report, dvUndeliveredStale),
		"and a STALE pending delivery is its own class: nothing is wrong with the post, the "+
			"QUEUE has stopped moving, and the investigation starts at the worker rather than "+
			"at the community")

	assert.Equal(t, 1, report.Counts[dvUndeliveredCancelled])
	assert.Equal(t, 1, report.Counts[dvUndeliveredPoisoned])
	assert.Equal(t, 1, report.Counts[dvUndeliveredStale],
		"each class carries its own count: one number for 'undelivered' hides which of three "+
			"unrelated problems the bridge has")

	// --- The two that must appear in NO class.
	for _, entry := range report.Entries {
		assert.NotEqual(t, inFlight, entry.Subject,
			"a post accepted moments ago whose delivery is still queued is the system WORKING. "+
				"Reporting it makes every healthy post a finding, and the report degrades into "+
				"a list of everything the bridge has ever accepted — which is the failure mode "+
				"that looks like thoroughness")
		assert.NotEqual(t, mtPostATURI, entry.Subject,
			"and neither is the post the peer actually received: accepted_at is stamped, which "+
				"is the one signal that says the delivery landed")
	}

	// --- The gauges an operator watches, one per class.
	assert.Equal(t, 1, gaugeValue(t, h, dvGaugeCancelled))
	assert.Equal(t, 1, gaugeValue(t, h, dvGaugePoisoned))
	assert.Equal(t, 1, gaugeValue(t, h, dvGaugeStale),
		"three gauges, because an operator alerts on them separately: a stale queue is a page, "+
			"a cancelled acceptance is a note, and one combined number can only ever be tuned "+
			"for whichever of them is noisiest")
}

// TestDivergenceStalenessThresholdIsANamedConstant pins the dial itself.
//
// A threshold written as a literal inside a query cannot be found by the person
// who has to change it, and cannot be reasoned about by the person deciding
// whether the report is crying wolf. The value is a policy — how long a delivery
// may sit before its absence from the peer is worth an operator's attention —
// and policies belong beside the sweep that applies them.
func TestDivergenceStalenessThresholdIsANamedConstant(t *testing.T) {
	assert.Greater(t, DefaultAcceptanceStaleAfter, time.Minute,
		"the window must be comfortably longer than a delivery takes, or every post in flight "+
			"is a finding")
	assert.LessOrEqual(t, DefaultAcceptanceStaleAfter, 24*time.Hour,
		"and short enough that a queue which stopped moving is noticed the same day: a window "+
			"measured in days is a report nobody can act on while it still matters")
}

// admitDivergencePost admits one more post by the fixture author into community
// A and returns its at-uri.
func admitDivergencePost(t *testing.T, h *harness, world moderationWorld, rkey, rev string, timeUS int64) string {
	t.Helper()
	admitPost(t, world, mtAuthorDID, rkey, world.communityADID, rev, timeUS)
	uri := "at://" + mtAuthorDID + "/social.coves.community.postv2/" + rkey
	require.Equal(t, []string{"pending"}, deliveryStatesForPost(t, h.db, uri),
		"precondition: %s was accepted and enqueued", rkey)
	return uri
}

// setDeliveryState drives one post's delivery into a terminal or aged state.
//
// The states are written directly because this test is about CLASSIFICATION,
// not about how a row reaches each state — the real cancellation path is driven
// end to end by the outer acceptance test, and a poisoned delivery costs eight
// failed HTTP attempts to reach honestly.
func setDeliveryState(t *testing.T, db *sql.DB, postURI, state, errorClass string, age time.Duration) {
	t.Helper()
	result, err := db.ExecContext(context.Background(), `
		UPDATE outbound_deliveries d
		   SET state = $2, last_error_class = $3,
		       created_at = now() - $4::interval, updated_at = now() - $4::interval
		  FROM outbound_activities a, outbound_objects o
		 WHERE a.activity_id = d.activity_id
		   AND position(o.ap_object_id in a.payload::text) > 0
		   AND o.at_uri = $1`,
		postURI, state, errorClass, fmt.Sprintf("%d seconds", int(age.Seconds())))
	require.NoError(t, err)
	affected, err := result.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, 1, affected, "exactly one delivery carries %s", postURI)
}

// subjectsOfClass lists the subjects reported under one class.
func subjectsOfClass(report divergenceReport, class string) []string {
	subjects := []string{}
	for _, entry := range report.Entries {
		if entry.Class == class {
			subjects = append(subjects, entry.Subject)
		}
	}
	return subjects
}

// TASK 17e — THE RESIDUAL A LAPSED BAN LEAVES BEHIND.
//
// 17c-3 recorded it as a known residual and left it there: a temporary ban
// cancels the banned author's queued deliveries, and when the ban expires those
// cancellations do not come back. `cancelled` is terminal, and nothing re-drives
// it. So the community repo keeps acceptance records for posts the peer will
// never receive, for an author who is no longer banned — a state nobody decided
// on and nobody is told about.
//
// It needs NO NEW COMPARISON. The rows are exactly cycle 2's: an accepted
// admission, no accepted_at stamp, a cancelled delivery. A ban-specific sweep
// would be a second join over the same tables answering the same question, and
// it would drift from this one — the two would disagree the first time either
// changed, and an operator would have two numbers for one problem. What the ban
// contributes is the REASON, which belongs in the entry's detail, not in a class
// of its own.
func TestALapsedBansCancellationsAreReportedByTheExistingCancelledClass(t *testing.T) {
	h := newHarness(t)
	world := newModerationWorld(t, h)
	h.admin.SetDivergenceReconciler(newDivergenceReconciler(t, h))
	deliverEverythingQueued(t, h.db)

	// --- GIVEN: a post accepted into community A...
	banned := admitDivergencePost(t, h, world, "3lzdvban00001", "3lzdvban00011", 1_775_000_080_000_001)

	// ...and a TEMPORARY ban that cancels its delivery. Driven through the real
	// announced Block, so the cancellation is the production one.
	h.announceBlock(world.groupA, "https://lemmy.world/activities/block/dv-1", mtAuthorDID, groupID,
		map[string]any{"expires": time.Now().Add(time.Hour).UTC().Format(time.RFC3339)})
	require.Equal(t, []string{"cancelled"}, deliveryStatesForPost(t, h.db, banned),
		"precondition: the ban cancelled the queued delivery")

	// --- AND: the ban lapses. Lemmy sends NO Undo when a temporary ban expires
	//     (17c-3), so this is exactly how the state arrives: the row stays, its
	//     expiry passes, and the author is federating again.
	lapseBan(t, h.db, world.communityADID, mtAuthorDID)

	report := fetchDivergence(t, h)

	// --- THEN: the post is reported, under the class it already belongs to.
	assert.Equal(t, []string{banned}, subjectsOfClass(report, dvUndeliveredCancelled),
		"a lapsed ban's residual IS an undelivered acceptance: the acceptance stands in the "+
			"community repo, the delivery is terminally cancelled, and the author is no longer "+
			"banned — so nothing will ever carry that post to the peer and nobody decided that")

	// --- AND: the sweep grew no ban-specific comparison.
	//
	// THE LIST IS EXHAUSTIVE ON PURPOSE, and it is the sweep's whole class
	// vocabulary in one place: every comparison 17e performs, and nothing else.
	// It fails in BOTH directions — a ban-specific class appearing here is the
	// case this test was written for, and a class disappearing is a comparison
	// that silently stopped running. Adding a genuinely new class means editing
	// this line, deliberately, which is the point.
	assert.ElementsMatch(t,
		[]string{
			DivergencePersonaVoteEvent,
			DivergenceAcceptanceCancelled,
			DivergenceAcceptancePoisoned,
			DivergenceAcceptanceStale,
			DivergenceVoteRecastUndelivered,
			DivergenceDeliveryUnknownRefused,
			DivergenceDeliveryUnknownUnanswered,
		},
		classesIn(report),
		"the report's CLASS SET must not have grown a BAN-SPECIFIC class: this residual is "+
			"cycle 2's rows read by cycle 2's query, and a ban-specific class would mean a "+
			"second join over the same tables answering the same question. Two queries for one "+
			"condition drift the first time either is touched, and then an operator has two "+
			"numbers and no way to tell which is stale")
}

// lapseBan moves a standing ban's expiry into the past — the way time does.
// Lemmy sends no Undo when a temporary ban runs out, so there is no activity to
// deliver here and nothing else to change: the ban row simply stops applying.
func lapseBan(t *testing.T, db *sql.DB, communityDID, subjectDID string) {
	t.Helper()
	result, err := db.ExecContext(context.Background(), `
		UPDATE community_bans SET expires_at = now() - interval '1 minute'
		 WHERE community_did = $1 AND subject_did = $2`, communityDID, subjectDID)
	require.NoError(t, err)
	affected, err := result.RowsAffected()
	require.NoError(t, err)
	require.EqualValues(t, 1, affected,
		"there must BE a ban to lapse: without one this test would be asserting about an "+
			"ordinary cancellation and would say nothing about bans at all")
}

// classesIn lists the classes the report accounts for — every key of Counts,
// including the ones that found nothing.
func classesIn(report divergenceReport) []string {
	classes := make([]string, 0, len(report.Counts))
	for class := range report.Counts {
		classes = append(classes, class)
	}
	return classes
}

// allBaseTables lists every base table in the public schema, minus goose's
// migration bookkeeping.
//
// ENUMERATED, NEVER LISTED. A hand-written list is a claim about the schema
// made at the moment it was typed, and it stops being true the next time
// anything is added — silently, in the direction that weakens the assertion.
// The previous version of this file watched 15 of 24 tables, and the gaps were
// exactly the ones that matter: ap_tombstones, which the admin endpoint
// registered four lines above /divergence writes, and communities, which the
// SIBLING reconciler converges by writing. "While we are here, tombstone what
// the peer never got" is the most plausible accidental repair anyone would add
// to this sweep, and the snapshot would not have seen it.
//
// goose_db_version is excluded because migrations are not the sweep's writes and
// a test-run migration would make every comparison here fail for the wrong
// reason.
func allBaseTables(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `
		SELECT table_name
		  FROM information_schema.tables
		 WHERE table_schema = 'public'
		   AND table_type = 'BASE TABLE'
		   AND table_name <> 'goose_db_version'
		 ORDER BY table_name`)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var tables []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		tables = append(tables, name)
	}
	require.NoError(t, rows.Err())
	return tables
}
