package store

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TASK 17e CYCLE 4 — WHAT WE DO NOT KNOW, KEPT AS NOT KNOWN.
//
// A poisoned delivery is one this bridge SENT and never got confirmation for.
// Whether the peer applied it is genuinely unknowable from here, and the two
// ways a delivery poisons are not equally informative:
//
//	REFUSED    — the peer answered with a status code. Evidence of
//	             non-application; NOT proof. A peer can apply an activity and
//	             then fail to respond, which is ordinary under load.
//	UNANSWERED — a transport failure, no status at all. Silent about
//	             everything: it may never have arrived, or it may have been
//	             applied and the response lost.
//
// So the read keeps them apart, and keeps both out of any answer about what the
// peer holds. 17b already relies on this: a poisoned vote's row deliberately
// KEEPS its activity id and error class so the uncertainty stays queryable —
// erasing it would turn "we do not know" into "it never happened".
//
// AGE IS DELIBERATELY NOT PART OF THE DEFINITION, unlike the stale-acceptance
// class: a poisoned delivery is terminal the moment it poisons — nothing
// re-drives it on its own — so a recent one and an old one are the same state.

const (
	dvUnknownInboxA = "https://lemmy.world/c/divergence/inbox"
	dvUnknownInboxB = "https://lemmy.zip/c/elsewhere/inbox"
)

// seedPoisonedDelivery writes one activity and its poisoned delivery. status 0
// means the peer never answered at all.
func seedPoisonedDelivery(t *testing.T, database *sql.DB, activityID, kind, inbox, errorClass string, status int) {
	t.Helper()
	ctx := context.Background()
	_, err := database.ExecContext(ctx, `
		INSERT INTO outbound_activities (activity_id, actor_did, kind, payload)
		VALUES ($1, $2, $3, '{"type":"Create"}'::jsonb)`, activityID, dvAuthorDID, kind)
	require.NoError(t, err)

	// The status is written EXACTLY as the caller says, including a literal 0 —
	// see seedUnansweredAsProductionWrites below for why that matters.
	statusArg := any(status)
	if status < 0 {
		// A negative status is this fixture's spelling of "the column is NULL",
		// which production does not write but the schema permits.
		statusArg = nil
	}
	_, err = database.ExecContext(ctx, `
		INSERT INTO outbound_deliveries (activity_id, target_inbox, ordering_key, state,
		                                 last_error_class, last_status_code, created_at)
		VALUES ($1, $2, $3, 'poisoned', $4, $5, now() - interval '2 hours')`,
		activityID, inbox, dvCommunityAPID, errorClass, statusArg)
	require.NoError(t, err)
}

// TestUnknownDeliveryOutcomes_SeparatesRefusedFromUnanswered is the contract.
func TestUnknownDeliveryOutcomes_SeparatesRefusedFromUnanswered(t *testing.T) {
	database := acceptanceTestDB(t)

	refusedID := "https://coves.social/ap/activity/dv-unknown-refused"
	unansweredID := "https://coves.social/ap/activity/dv-unknown-unanswered"
	seedPoisonedDelivery(t, database, refusedID, "Create", dvUnknownInboxA, "4xx", 422)
	seedPoisonedDelivery(t, database, unansweredID, "Create", dvUnknownInboxB, "transport", 0)

	// A delivery that DID land, so "found two" is not "found everything".
	seedPoisonedSibling(t, database)

	found, err := NewDivergences(database).UnknownDeliveryOutcomes(context.Background())
	require.NoError(t, err)
	require.Len(t, found, 2,
		"both poisoned deliveries are unknown outcomes, and the delivered one is not: a "+
			"delivery that was confirmed is the one case here we DO know")

	byID := map[string]UnknownDeliveryOutcome{}
	for _, outcome := range found {
		byID[outcome.ActivityID] = outcome
	}

	refused := byID[refusedID]
	assert.True(t, refused.Refused,
		"a peer that ANSWERED — even with a rejection — told us something, and that is the "+
			"only distinction available between these two rows")
	assert.Equal(t, 422, refused.LastStatusCode,
		"with the status it answered, because 422 and 503 send an operator to different places")
	assert.Equal(t, dvUnknownInboxA, refused.TargetInbox,
		"and the inbox: we cannot know whether they applied it, so the instance to ASK is the "+
			"most actionable thing this row carries")

	unanswered := byID[unansweredID]
	assert.False(t, unanswered.Refused,
		"a transport failure is silent: the request may never have arrived, or may have been "+
			"applied and the response lost. Reading that as 'the peer said no' invents an "+
			"answer nobody gave")
	assert.Zero(t, unanswered.LastStatusCode,
		"and there is no status to report — which is why Refused is stored rather than derived "+
			"from the code, so a 0 can never be read as 'the peer answered 0'")
	assert.Equal(t, "transport", unanswered.LastErrorClass,
		"the recorded class rides along: it is what an operator triages on and what 17b "+
			"deliberately preserved so the uncertainty stays queryable")
}

// seedPoisonedSibling writes a DELIVERED delivery beside the poisoned ones —
// the case we genuinely know the answer to, which must never appear among the
// unknowns.
func seedPoisonedSibling(t *testing.T, database *sql.DB) {
	t.Helper()
	ctx := context.Background()
	activityID := "https://coves.social/ap/activity/dv-unknown-delivered"
	_, err := database.ExecContext(ctx, `
		INSERT INTO outbound_activities (activity_id, actor_did, kind, payload)
		VALUES ($1, $2, 'Create', '{"type":"Create"}'::jsonb)`, activityID, dvAuthorDID)
	require.NoError(t, err)
	_, err = database.ExecContext(ctx, `
		INSERT INTO outbound_deliveries (activity_id, target_inbox, ordering_key, state,
		                                 last_status_code, delivered_at, created_at)
		VALUES ($1, $2, $3, 'delivered', 202, now(), now() - interval '2 hours')`,
		activityID, dvUnknownInboxA, dvCommunityAPID)
	require.NoError(t, err)
}

// TestUnknownDeliveryOutcomes_ATransportFailureIsUnansweredAsProductionWritesIt
// is the case the rest of this file could not see.
//
// PRODUCTION NEVER WRITES NULL HERE. worker.go passes a literal 0 for a
// transport failure (dial, TLS, timeout) and 0 again for an unresolvable
// signer, so "the peer never answered" reaches this table as a ZERO. Every
// other fixture in this file spelled it NULL, which made a predicate of
// `last_status_code IS NOT NULL` pass every test while filing every real dial
// timeout under REFUSED — the sub-count that asserts the peer answered. That is
// exactly the invented answer this file's header forbids, and it would have
// shipped behind green tests.
//
// The distinction has to be "did a status actually come back", and the only
// spelling of that which survives contact with the writer is a value test.
func TestUnknownDeliveryOutcomes_ATransportFailureIsUnansweredAsProductionWritesIt(t *testing.T) {
	database := acceptanceTestDB(t)

	// The two shapes the worker really produces.
	timeoutID := "https://coves.social/ap/activity/dv-unknown-timeout"
	seedPoisonedDelivery(t, database, timeoutID, "Create", dvUnknownInboxA, "transport", 0)
	seedPoisonedDelivery(t, database, timeoutID+"-tls", "Create", dvUnknownInboxB, "transport", 0)
	// And a genuine refusal beside them, so "everything is unanswered" cannot
	// pass either: the two must come apart.
	refusedID := "https://coves.social/ap/activity/dv-unknown-503"
	seedPoisonedDelivery(t, database, refusedID, "Create", dvUnknownInboxA, "5xx", 503)

	found, err := NewDivergences(database).UnknownDeliveryOutcomes(context.Background())
	require.NoError(t, err)
	require.Len(t, found, 3)

	byID := map[string]UnknownDeliveryOutcome{}
	for _, outcome := range found {
		byID[outcome.ActivityID] = outcome
	}

	assert.False(t, byID[timeoutID].Refused,
		"a transport failure is stored with last_status_code = 0 — the literal the worker "+
			"passes, not NULL — and it means NO ANSWER CAME BACK. A predicate that tests for "+
			"NULL instead of for a real status files every dial timeout on the network under "+
			"the count that says the peer replied, which is precisely the answer nobody gave")
	assert.False(t, byID[timeoutID+"-tls"].Refused,
		"and so is a TLS failure mid-handshake: same literal 0, same silence about whether the "+
			"peer ever saw the request")
	assert.True(t, byID[refusedID].Refused,
		"while a real 503 IS an answer: the two must come apart on the value, or the "+
			"distinction the sub-counts exist for does not exist")
	assert.Equal(t, 503, byID[refusedID].LastStatusCode)
}

// TestUnknownDeliveryOutcomes_ANullStatusIsAlsoUnanswered keeps the defensive
// case. No writer produces a NULL here today — the column is nullable and every
// caller passes an int — so this pins the schema's remaining freedom rather than
// a live path, and it exists so that a future writer which does leave it NULL
// cannot land the row in the "the peer answered" bucket by default.
func TestUnknownDeliveryOutcomes_ANullStatusIsAlsoUnanswered(t *testing.T) {
	database := acceptanceTestDB(t)

	nullID := "https://coves.social/ap/activity/dv-unknown-null"
	seedPoisonedDelivery(t, database, nullID, "Create", dvUnknownInboxA, "transport", -1)

	found, err := NewDivergences(database).UnknownDeliveryOutcomes(context.Background())
	require.NoError(t, err)
	require.Len(t, found, 1)
	assert.False(t, found[0].Refused,
		"an absent status is not an answer either: NULL and 0 must land in the same bucket, or "+
			"the meaning of the sub-count depends on which writer produced the row")
	assert.Zero(t, found[0].LastStatusCode,
		"and it is reported as 0 rather than as a NULL a JSON reader would see as null: the "+
			"field is a status, and there was none")
}

// TestUnknownDeliveryOutcomes_ACancelledDeliveryIsNotUnknown separates "we do
// not know" from "it was never sent".
//
// A cancelled delivery never reached the wire — a ban or an opt-out took it out
// of the queue — so there is nothing uncertain about it: the peer does not have
// it, and cycle 2's classes say so. Folding it in here would put a KNOWN
// non-delivery into the bucket whose whole meaning is that the answer is
// unavailable, and the unknown counts are exactly the numbers an operator must
// be able to trust as small.
func TestUnknownDeliveryOutcomes_ACancelledDeliveryIsNotUnknown(t *testing.T) {
	database := acceptanceTestDB(t)
	ctx := context.Background()

	activityID := "https://coves.social/ap/activity/dv-unknown-cancelled"
	_, err := database.ExecContext(ctx, `
		INSERT INTO outbound_activities (activity_id, actor_did, kind, payload)
		VALUES ($1, $2, 'Create', '{"type":"Create"}'::jsonb)`, activityID, dvAuthorDID)
	require.NoError(t, err)
	_, err = database.ExecContext(ctx, `
		INSERT INTO outbound_deliveries (activity_id, target_inbox, ordering_key, state, created_at)
		VALUES ($1, $2, $3, 'cancelled', now() - interval '2 hours')`,
		activityID, dvUnknownInboxA, dvCommunityAPID)
	require.NoError(t, err)

	// One genuinely unknown row beside it, so an empty result cannot pass for
	// the right answer.
	seedPoisonedDelivery(t, database, "https://coves.social/ap/activity/dv-unknown-live",
		"Create", dvUnknownInboxA, "transport", 0)

	found, err := NewDivergences(database).UnknownDeliveryOutcomes(ctx)
	require.NoError(t, err)
	require.Len(t, found, 1, "only the poisoned delivery is an unknown outcome")
	assert.Equal(t, "https://coves.social/ap/activity/dv-unknown-live", found[0].ActivityID,
		"a CANCELLED delivery never reached the wire, so its outcome is known: the peer does "+
			"not have it. Counting it as unknown inflates the one number whose value is that "+
			"it is small and honest")
}

// ---------------------------------------------------------------------------
// "We do not know" requires that we SENT it
// ---------------------------------------------------------------------------

// TestUnknownDeliveryOutcomes_APoisonThatNeverReachedTheWireIsNotUnknown is the
// completeness half of this class, and it is the same rule that excludes a
// cancelled delivery — one step later in the worker.
//
// Four poison classes are decided BEFORE any POST is made (worker.go): a causal
// wait that expired (parent_unaccepted), a parent that poisoned
// (parent_poisoned), an inbox that is not same-authority with its community
// (cross_authority), and a signer that would not resolve (signer). All four
// record status 0, which is the same shape a dial timeout leaves behind — and
// they mean the opposite. Nothing was ever sent, so the peer's state is not
// unknown at all: they do not have it.
//
// The cost of getting this wrong is precise. The unanswered bucket's Detail
// names an instance for the operator to go ASK, and its whole value is that the
// number is small and every row in it is a real question. Filing decisions the
// bridge made itself under "we do not know" both inflates that number and sends
// someone to ask a stranger about a request that never left the building.
func TestUnknownDeliveryOutcomes_APoisonThatNeverReachedTheWireIsNotUnknown(t *testing.T) {
	database := acceptanceTestDB(t)

	// The four never-wire classes, exactly as the worker writes them.
	neverSent := []string{"parent_unaccepted", "parent_poisoned", "cross_authority", "signer"}
	for _, class := range neverSent {
		seedPoisonedDelivery(t, database,
			"https://coves.social/ap/activity/dv-neverwire-"+class,
			"Create", dvUnknownInboxA, class, 0)
	}

	// Two genuine unknowns beside them: one that got no answer, one refused.
	// Without these the assertion would be satisfied by a query that returned
	// nothing at all.
	sentSilently := "https://coves.social/ap/activity/dv-neverwire-transport"
	sentRefused := "https://coves.social/ap/activity/dv-neverwire-4xx"
	seedPoisonedDelivery(t, database, sentSilently, "Create", dvUnknownInboxA, "transport", 0)
	seedPoisonedDelivery(t, database, sentRefused, "Create", dvUnknownInboxB, "4xx", 422)

	found, err := NewDivergences(database).UnknownDeliveryOutcomes(context.Background())
	require.NoError(t, err)

	ids := make([]string, 0, len(found))
	for _, outcome := range found {
		ids = append(ids, outcome.ActivityID)
	}
	assert.ElementsMatch(t, []string{sentSilently, sentRefused}, ids,
		"only deliveries that REACHED THE WIRE are unknown. %v are all decided before any POST "+
			"is made — a causal wait that expired, a poisoned parent, a cross-authority inbox, "+
			"an unresolvable signer — so the peer does not have them and nothing about their "+
			"outcome is uncertain. They carry status 0 like a dial timeout does, which is the "+
			"whole trap: identical column, opposite meaning. Counting them inflates the one "+
			"number whose value is that it is small, and the unanswered bucket's own detail "+
			"sends an operator to ask an instance about a request that never left this process",
		neverSent)
}

// ---------------------------------------------------------------------------
// The counts are the measurement; the list is a page of it
// ---------------------------------------------------------------------------

// TestUnknownDeliveryOutcomeCounts_MeasureThePopulationTheExamplesOnlySampleFrom
// pins the last of the four count/list pairs, and this class is where the
// bounded read is least obvious and most needed.
//
// A peer that goes away poisons everything aimed at it, so this population
// arrives in bulk by nature: one unreachable instance is thousands of rows, all
// with the same target inbox and the same silence. The two sub-counts are the
// numbers an operator judges that by, and their whole worth is that they are
// small and honest — a count bounded at the page would read as five hundred
// open questions no matter how many there were.
//
// THE SPLIT SURVIVES THE BULK, which is the second thing under test. The counts
// are grouped by the same expression the list selects Refused with
// (unknownDeliveryRefused), so five hundred silent transport failures cannot
// tip into the sub-count that says the peer answered — the exact drift that
// would happen if the two statements spelled "the peer spoke" differently.
func TestUnknownDeliveryOutcomeCounts_MeasureThePopulationTheExamplesOnlySampleFrom(t *testing.T) {
	database := acceptanceTestDB(t)
	ctx := context.Background()

	// One unreachable instance: more silent deliveries than the page holds.
	const overflowing = MaxDivergenceExamples + 1
	seedPoisonedDeliveriesInBulk(t, database, overflowing)
	// And two that a peer really answered, so the split cannot be satisfied by a
	// total and the refused bucket has something in it to be wrong about.
	refusedA := "https://coves.social/ap/activity/dv-count-refused-a"
	refusedB := "https://coves.social/ap/activity/dv-count-refused-b"
	seedPoisonedDelivery(t, database, refusedA, "Create", dvUnknownInboxA, "4xx", 422)
	seedPoisonedDelivery(t, database, refusedB, "Create", dvUnknownInboxB, "5xx", 503)
	// The outcome we DO know, which belongs in neither number.
	seedPoisonedSibling(t, database)

	divergences := NewDivergences(database)

	found, err := divergences.UnknownDeliveryOutcomes(ctx)
	require.NoError(t, err)
	require.Len(t, found, MaxDivergenceExamples,
		"the examples are bounded at the database: an unreachable instance is thousands of rows, "+
			"and a report that grows with the outage cannot be read during one")

	counts, err := divergences.UnknownDeliveryOutcomeCounts(ctx)
	require.NoError(t, err)

	assert.Equal(t, overflowing, counts.Unanswered,
		"the COUNT is the true measurement: every one of these got no answer at all, and the "+
			"example budget bounds what an operator can read rather than what the sweep measured")
	assert.Greater(t, counts.Unanswered, len(found),
		"and it EXCEEDS the examples, which is the contract — a count capped at %d would report "+
			"the size of a page while an instance was silently dropping everything we sent it",
		MaxDivergenceExamples)
	assert.Equal(t, 2, counts.Refused,
		"while the peers that ANSWERED stay their own number. The split is the only information "+
			"these rows carry, and it is computed by the same expression the list selects Refused "+
			"with: two spellings of 'the peer spoke' is precisely how five hundred dial timeouts "+
			"end up in the count that says they replied")
}

// seedPoisonedDeliveriesInBulk writes n poisoned deliveries that got no answer
// at all — one unreachable instance, as the worker records it.
//
// The status is a literal 0 rather than NULL because that is what production
// writes for a transport failure; a fixture that spelled it NULL here would let
// a NULL-based discriminator file every one of these under REFUSED and still
// pass.
func seedPoisonedDeliveriesInBulk(t *testing.T, database *sql.DB, n int) {
	t.Helper()
	ctx := context.Background()
	activityID := "'https://coves.social/ap/activity/dv-bulk-unknown-' || i"

	_, err := database.ExecContext(ctx, `
		INSERT INTO outbound_activities (activity_id, actor_did, kind, payload)
		SELECT `+activityID+`, $1, 'Create', '{"type":"Create"}'::jsonb
		  FROM generate_series(1, $2) AS i`, dvAuthorDID, n)
	require.NoError(t, err, "seed %d activities", n)

	_, err = database.ExecContext(ctx, `
		INSERT INTO outbound_deliveries (activity_id, target_inbox, ordering_key, state,
		                                 last_error_class, last_status_code, created_at)
		SELECT `+activityID+`, $1, $2, 'poisoned', 'transport', 0, now() - interval '2 hours'
		  FROM generate_series(1, $3) AS i`, dvUnknownInboxA, dvCommunityAPID, n)
	require.NoError(t, err, "seed %d deliveries", n)
}
