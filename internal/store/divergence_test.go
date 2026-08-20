package store

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/testutil"
)

// TASK 17e CYCLE 1 — THE STANDING PERSONA-VOTE INVARIANT, AT THE JOIN.
//
// The invariant is one sentence: a vote this bridge cast on a native user's
// behalf must never appear in vote_events. It is where our own echo would land
// if the 17a voter probe were ever bypassed, and the cost is exact and
// permanent — the subject's served tally counts that one vote twice, once as a
// live inbound event and once as the delivered outbound row 17b's reseed
// subtracts from the origin's total.
//
// Expected count: ZERO, forever. A reconciler is the right shape for it because
// zero-forever is precisely the kind of claim that quietly stops being true:
// nothing else re-checks it, and the damage is a number nobody can trace back.
//
// THE TABLE NAME IS THE WHOLE TEST. This file exists here, beside the query,
// because the lethal mistake is one word:
//
//	ap_actors      — OUR native personas (Coves users we federate FOR)
//	bridged_actors — REAL LEMMY HUMANS mirrored INTO atproto (we hold their DIDs)
//
// Both tables hold DIDs we minted and AP ids we can spell, so the confusion is
// easy and the assertion "the probe finds our own actors" cannot tell them
// apart. Probing bridged_actors matches EVERY genuine inbound vote — every vote
// on the network arrives from a Lemmy human, and every Lemmy human who has ever
// voted has a bridged_actors row — so the reconciler would report every real
// vote as our own echo, and any later code trusting that signal takes the vote
// pipeline dark: every community's tally goes to zero.

const (
	dvPersonaDID     = "did:plc:dvpersona0000000001"
	dvPersonaActorID = "https://coves.social/ap/actor/" + dvPersonaDID
	dvSubjectAPID    = "https://lemmy.world/post/9001"

	// A real Lemmy human, mirrored into atproto. Their AP id is a LEMMY url and
	// their DID is one we minted — the exact shape that makes the two tables
	// confusable.
	dvHumanAPID = "https://lemmy.world/u/genuine"
	dvHumanDID  = "did:plc:dvbridgedhuman00001"
)

func divergenceTestDB(t *testing.T) *sql.DB {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database,
		"vote_events", "vote_aggregates", "ap_actors", "bridged_actors",
		"outbound_activities", "outbound_deliveries", "outbound_votes")
	return database
}

// seedPersona writes one native persona: the identity the bridge federates FOR.
func seedPersona(t *testing.T, database *sql.DB, did, actorID string) {
	t.Helper()
	_, err := database.ExecContext(context.Background(), `
		INSERT INTO ap_actors (did, kind, actor_id, normalized_origin, local_part,
		                       rsa_key_sealed, rsa_key_version, public_key_pem)
		VALUES ($1, 'person', $2, 'coves.social', $3, '\x00'::bytea, 1, 'pem')`,
		did, actorID, "p"+did[len(did)-6:])
	require.NoError(t, err)
}

// seedBridgedHuman writes one real Lemmy human mirrored into atproto.
func seedBridgedHuman(t *testing.T, database *sql.DB, apID, did string) {
	t.Helper()
	_, err := database.ExecContext(context.Background(), `
		INSERT INTO bridged_actors (ap_actor_id, actor_type, did)
		VALUES ($1, 'person', $2)`, apID, did)
	require.NoError(t, err)
}

// seedVoteEvent writes one inbound vote exactly as the aggregator would.
func seedVoteEvent(t *testing.T, database *sql.DB, activityID, voterAPID, direction string) {
	t.Helper()
	_, err := database.ExecContext(context.Background(), `
		INSERT INTO vote_events (activity_id, voter_ap_id, subject_ap_id, direction)
		VALUES ($1, $2, $3, $4)`, activityID, voterAPID, dvSubjectAPID, direction)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// The invariant
// ---------------------------------------------------------------------------

// TestPersonaVoteEvents_GenuineVotersAreNotOurOwn is the healthy world: real
// Lemmy people voting on a bridged post, and a persona that has never had an
// inbound vote attributed to it.
func TestPersonaVoteEvents_GenuineVotersAreNotOurOwn(t *testing.T) {
	database := divergenceTestDB(t)
	seedPersona(t, database, dvPersonaDID, dvPersonaActorID)
	seedBridgedHuman(t, database, dvHumanAPID, dvHumanDID)
	seedVoteEvent(t, database, "https://lemmy.world/activities/like/1", dvHumanAPID, "up")
	seedVoteEvent(t, database, "https://lemmy.world/activities/dislike/2", "https://lemmy.ml/u/other", "down")

	found, err := NewDivergences(database).PersonaVoteEvents(context.Background())
	require.NoError(t, err)
	assert.Empty(t, found,
		"the ordinary state of the world is ZERO: every vote here was cast by a real person on "+
			"another instance, and reporting any of them would make the healthy case "+
			"indistinguishable from the broken one")
}

// TestPersonaVoteEvents_APersonasOwnVoteIsFound is the divergence.
//
// THE FIXTURE IS BUILT SO AN ID-KEYED PROBE CANNOT PASS IT. Decision 16
// originally proposed matching inbound votes against outbound_activities by
// ACTIVITY ID; the 17a plan review overturned it on measured behaviour —
// Lemmy 0.19 reconstructs Announce{Undo{Like}} with a FRESHLY GENERATED inner
// activity id, and types it "Like" even when the live vote is a Dislike. So the
// row below carries an id that appears in no outbound_activities row, while the
// persona's real Like sits there under a different one. An implementation that
// probed ids finds nothing, reports zero, and looks like it is working.
func TestPersonaVoteEvents_APersonasOwnVoteIsFound(t *testing.T) {
	database := divergenceTestDB(t)
	ctx := context.Background()
	seedPersona(t, database, dvPersonaDID, dvPersonaActorID)
	seedBridgedHuman(t, database, dvHumanAPID, dvHumanDID)

	// The activity the bridge really sent for this persona: a DISLIKE, under an
	// id of our own minting.
	ourActivityID := "https://coves.social/ap/activity/" + repeatHex('d')
	_, err := database.ExecContext(ctx, `
		INSERT INTO outbound_activities (activity_id, actor_did, kind, payload)
		VALUES ($1, $2, 'Dislike', '{"type":"Dislike"}'::jsonb)`, ourActivityID, dvPersonaDID)
	require.NoError(t, err)

	// What came back: our own persona's vote, echoed in with an id Lemmy
	// generated and a direction that does not match what we sent.
	echoedID := "https://lemmy.world/activities/like/reconstructed-99"
	seedVoteEvent(t, database, echoedID, dvPersonaActorID, "up")
	// ...beside a genuine vote, so "found something" is not the same as "found
	// everything".
	seedVoteEvent(t, database, "https://lemmy.world/activities/like/3", dvHumanAPID, "up")

	found, err := NewDivergences(database).PersonaVoteEvents(ctx)
	require.NoError(t, err)
	require.Len(t, found, 1,
		"exactly the persona's row is a divergence: the join is voter_ap_id = "+
			"ap_actors.actor_id, an IDENTITY match. An activity-id probe against "+
			"outbound_activities misses this row — Lemmy reconstructs the echo with a fresh id "+
			"(and the wrong type) — while still finding nothing to report, which is the failure "+
			"mode that looks exactly like success")
	assert.Equal(t, echoedID, found[0].ActivityID)
	assert.Equal(t, dvPersonaActorID, found[0].VoterAPID)
	assert.Equal(t, dvSubjectAPID, found[0].SubjectAPID,
		"the SUBJECT rides along: it is the tally that is wrong, and an operator cannot check "+
			"a count they cannot name")
	assert.Equal(t, dvPersonaDID, found[0].ActorDID,
		"and the persona it belongs to, which is where the investigation starts")
}

// ---------------------------------------------------------------------------
// CONTROL 1 — the table name
// ---------------------------------------------------------------------------

// TestPersonaVoteEvents_ABridgedHumansVoteIsNeverOurOwn is the control for the
// one-word mistake.
//
// The fixture makes the swap MEASURABLE rather than hypothetical: a real Lemmy
// human with a bridged_actors row, voting, exactly as the whole network does all
// day. Under the correct table this is invisible. Under bridged_actors it is a
// reported divergence — and so is every other vote the bridge has ever received.
func TestPersonaVoteEvents_ABridgedHumansVoteIsNeverOurOwn(t *testing.T) {
	database := divergenceTestDB(t)
	seedPersona(t, database, dvPersonaDID, dvPersonaActorID)
	seedBridgedHuman(t, database, dvHumanAPID, dvHumanDID)

	// Three genuine humans, one of them mirrored, all voting normally.
	seedVoteEvent(t, database, "https://lemmy.world/activities/like/10", dvHumanAPID, "up")
	seedVoteEvent(t, database, "https://lemmy.world/activities/like/11", "https://lemmy.world/u/second", "up")
	seedVoteEvent(t, database, "https://lemmy.ml/activities/dislike/12", "https://lemmy.ml/u/third", "down")

	found, err := NewDivergences(database).PersonaVoteEvents(context.Background())
	require.NoError(t, err)
	assert.Empty(t, found,
		"a BRIDGED ACTOR is a real Lemmy human we mirrored INTO atproto — not one of our "+
			"personas — and their votes are the entire inbound vote stream. Probing "+
			"bridged_actors instead of ap_actors reports every genuine vote on the network as "+
			"our own echo: every community's tally reads as double-counted, and anything that "+
			"acts on this signal takes the vote pipeline dark. The two tables are confusable "+
			"because both hold DIDs we minted, and no assertion about 'finding our actors' can "+
			"tell them apart — only this one can")
}

// ---------------------------------------------------------------------------
// The known-narrow edge, documented rather than claimed
// ---------------------------------------------------------------------------

// TestPersonaVoteEvents_ANonCanonicalSpellingIsNotMatched_KNOWNNARROW records a
// limitation of the invariant, and it deliberately asserts what the code DOES
// rather than what the invariant's name promises.
//
// The probe that PREVENTS these rows (echo.identifyActor) normalizes: it
// compares a NORMALIZED HOST plus scheme against ap_actors.normalized_origin, so
// "https://Coves.social:443/ap/actor/{did}" is ours to the probe. The join here
// is exact string equality, and so was migration 023's one-time cleanup — which
// means a legacy row spelled non-canonically survived the cleanup AND is missed
// by this sweep.
//
// The consequence, stated plainly so the next reader is not misled by a green
// gauge: THIS GAUGE READING ZERO IS NOT PROOF THAT NO PERSONA VOTE EXISTS. It
// proves no persona vote exists SPELLED EXACTLY AS THE PERSONA'S actor_id.
// Production is empty of such rows (write-back has never been deployed and no
// code path here writes a non-canonical voter id), which is why the narrow form
// is accepted rather than fixed; closing it means re-implementing the
// normalization in SQL, and that is a followup, not a silent assumption.
func TestPersonaVoteEvents_ANonCanonicalSpellingIsNotMatched_KNOWNNARROW(t *testing.T) {
	database := divergenceTestDB(t)
	seedPersona(t, database, dvPersonaDID, dvPersonaActorID)

	// The same identity, spelled the way a legacy row might carry it: explicit
	// default port and mixed case. echo.identifyActor calls this ours.
	seedVoteEvent(t, database,
		"https://lemmy.world/activities/like/legacy-1",
		"https://Coves.social:443/ap/actor/"+dvPersonaDID, "up")

	found, err := NewDivergences(database).PersonaVoteEvents(context.Background())
	require.NoError(t, err)
	assert.Empty(t, found,
		"DOCUMENTED NARROWNESS, not a claim of correctness: the join is exact equality, so a "+
			"non-canonically spelled persona id is invisible to it — and was equally invisible "+
			"to migration 023's cleanup, which used the same exact equality. A zero here "+
			"therefore means 'no exactly-spelled persona vote', NOT 'no persona vote'. If this "+
			"test ever fails because the join learned to normalize, that is an improvement: "+
			"delete this test and say so in the report")
}

// ---------------------------------------------------------------------------
// 17e review — the exclusion's SECOND conjunct, decided rather than inherited
// ---------------------------------------------------------------------------

// TestRecastDivergences_ALedgerThatNamesTheVoteButCallsItPendingIsReported
// decides a state the query has an opinion about and nobody wrote down.
//
// The re-cast exclusion suppresses a finding only when the ledger BOTH names
// this exact activity as the live vote AND calls it delivered. Drop the second
// conjunct and every fixture in the suite stays green, because no fixture has
// ever produced the in-between: current_activity_id pointing at an activity a
// delivery row says was delivered, while delivered_state reads 'pending'.
//
// THE ANSWER IS THAT IT MUST BE REPORTED, and the reason is the same one that
// makes this class worth having. The delivery is the durable fact — the peer
// accepted that activity — and the ledger is the number the reseed reads. A row
// saying 'pending' subtracts nothing, so the community's served score counts a
// vote of ours as a stranger's, permanently, and the vote row that would
// normally be corrected by voteCallback is the one thing that already failed to
// be. "Our ledger half-claims it" is not an account of what the peer holds; only
// a full claim is.
//
// NO WRITER PRODUCES THIS TODAY — voteCallback flips the row to delivered on the
// same success that marks the delivery, and a re-cast moves the id rather than
// resetting the state under it. That is exactly why it is written here: an
// implementation that dropped the conjunct would look identical on every state
// the system currently reaches, and would then swallow this one silently on the
// day some future path leaves the pair half-written.
func TestRecastDivergences_ALedgerThatNamesTheVoteButCallsItPendingIsReported(t *testing.T) {
	database := divergenceTestDB(t)
	ctx := context.Background()

	subject := "at://" + dvPersonaDID + "/social.coves.community.postv2/3lzdvrcpost1"
	activityID := "https://coves.social/ap/activity/dv-recast-halfclaimed"
	seedDeliveredVoteActivity(t, database, activityID, dvPersonaDID, subject, "Like")

	// The ledger names THIS activity as the live vote — and calls it pending.
	_, err := database.ExecContext(ctx, `
		INSERT INTO outbound_votes (vote_at_uri, actor_did, subject_at_uri, subject_ap_id,
		                            community_did, direction, current_activity_id, delivered_state)
		VALUES ($1, $2, $3, $4, $5, 'up', $6, 'pending')`,
		"at://"+dvPersonaDID+"/social.coves.feed.vote/3lzdvrcvote1", dvPersonaDID, subject,
		"https://lemmy.world/post/9002", dvCommunityDID, activityID)
	require.NoError(t, err)

	found, err := NewDivergences(database).RecastDivergences(ctx)
	require.NoError(t, err)
	require.Len(t, found, 1,
		"a HALF-CLAIM is not a claim. The delivery row is durable proof the peer accepted this "+
			"activity, while the ledger says 'pending' — which the reseed reads as 'subtract "+
			"nothing', so the community's served score counts our own vote as a stranger's "+
			"forever. The exclusion exists for the case where our accounting is COMPLETE; "+
			"anything less is the divergence itself, and dropping the delivered_state conjunct "+
			"makes this state silently disappear from a report that has no other way to see it")
	assert.Equal(t, activityID, found[0].DeliveredActivityID)
	assert.Equal(t, subject, found[0].SubjectATURI)
}

// TestRecastDivergenceCount_MeasuresThePopulationTheExamplesOnlySampleFrom is
// this comparison's half of the same contract the acceptance counts carry:
// Counts is the true measurement, Entries is a page of it.
//
// The bulk shape is not hypothetical for this class. Votes are the
// highest-volume thing the bridge sends, and the population here is "a peer is
// holding a vote we no longer claim" — which arrives in bulk exactly when it
// matters: an instance that went away mid-flight poisons every re-cast and Undo
// aimed at it, and the count is how an operator learns whether that is three
// votes or fifty thousand. A count bounded with the examples would report the
// size of a page and stop moving while the problem grew.
//
// THE COUNT WRAPS THE SAME DISTINCT SELECT the list runs, and the DISTINCT is
// part of the definition rather than tidiness: one activity may have several
// deliveries (the fan-out schema) and one delivered copy is ONE thing the peer
// holds. Counting the un-deduplicated join would report a number larger than the
// list it labels, on the rows an operator is sizing.
func TestRecastDivergenceCount_MeasuresThePopulationTheExamplesOnlySampleFrom(t *testing.T) {
	database := divergenceTestDB(t)
	ctx := context.Background()

	const overflowing = MaxDivergenceExamples + 1
	seedRecastDivergencesInBulk(t, database, overflowing)

	// THE EXCLUSION CONTROL, because a count is only worth pinning if the
	// population it counts is a comparison. This vote is delivered AND our ledger
	// still names it as the live, delivered vote — so we account for what the peer
	// holds and there is nothing to reconcile. Without it, a query that reported
	// every delivered vote would satisfy every assertion below while naming most
	// of the highest-volume table in the system.
	covered := "https://coves.social/ap/activity/dv-count-covered"
	coveredSubject := "at://" + dvPersonaDID + "/social.coves.community.postv2/3lzdvcntkeep"
	seedDeliveredVoteActivity(t, database, covered, dvPersonaDID, coveredSubject, "Like")
	_, err := database.ExecContext(ctx, `
		INSERT INTO outbound_votes (vote_at_uri, actor_did, subject_at_uri, subject_ap_id,
		                            community_did, direction, current_activity_id, delivered_state)
		VALUES ($1, $2, $3, $4, $5, 'up', $6, 'delivered')`,
		"at://"+dvPersonaDID+"/social.coves.feed.vote/3lzdvcntkeep", dvPersonaDID, coveredSubject,
		"https://lemmy.world/post/9003", dvCommunityDID, covered)
	require.NoError(t, err)

	divergences := NewDivergences(database)

	found, err := divergences.RecastDivergences(ctx)
	require.NoError(t, err)
	require.Len(t, found, MaxDivergenceExamples,
		"the examples are bounded at the database by the same budget the report spends")
	for _, entry := range found {
		assert.NotEqual(t, covered, entry.DeliveredActivityID,
			"and the fully-accounted vote is not among them: our ledger names this exact activity "+
				"as the live vote AND calls it delivered, which is the one state that says the "+
				"peer holds nothing we have not accounted for")
	}

	total, err := divergences.RecastDivergenceCount(ctx)
	require.NoError(t, err)
	assert.Equal(t, overflowing, total,
		"the COUNT is exact and unbounded — the same comparison, with no LIMIT — so it counts the "+
			"population rather than the page")
	assert.Greater(t, total, len(found),
		"and it EXCEEDS the examples. That is the contract these two statements exist to keep: "+
			"the list is a sample an operator investigates, the count is the size they escalate "+
			"on, and a count capped at %d would stop growing exactly when the incident does",
		MaxDivergenceExamples)
}

// seedRecastDivergencesInBulk writes n delivered vote activities that NOTHING in
// the ledger accounts for — the shape this class reports.
//
// Each carries its OWN subject, so no two supersede each other: the comparison
// excludes an earlier delivered vote when a LATER delivered vote for the same
// (actor, subject) pair follows it, which is how an ordinary vote flip stays out
// of the report. Sharing one subject across the bulk would leave exactly one row
// standing and quietly turn this into a fixture of size 1.
func seedRecastDivergencesInBulk(t *testing.T, database *sql.DB, n int) {
	t.Helper()
	ctx := context.Background()
	activityID := "'https://coves.social/ap/activity/dv-bulk-recast-' || i"
	subject := "'at://" + dvPersonaDID + "/social.coves.community.postv2/3lzdvbulkrc' || i"

	_, err := database.ExecContext(ctx, `
		INSERT INTO outbound_activities (activity_id, actor_did, kind, payload, parent_at_uri)
		SELECT `+activityID+`, $1, 'Like', '{"type":"Like"}'::jsonb, `+subject+`
		  FROM generate_series(1, $2) AS i`, dvPersonaDID, n)
	require.NoError(t, err, "seed %d vote activities", n)

	_, err = database.ExecContext(ctx, `
		INSERT INTO outbound_deliveries (activity_id, target_inbox, ordering_key, state,
		                                 last_status_code, delivered_at)
		SELECT `+activityID+`, $1, $2, 'delivered', 202, now()
		  FROM generate_series(1, $3) AS i`,
		"https://lemmy.world/inbox", "https://lemmy.world/c/technology", n)
	require.NoError(t, err, "seed %d deliveries", n)
}

// seedDeliveredVoteActivity writes one vote activity with a delivered delivery —
// the durable evidence a peer was told.
func seedDeliveredVoteActivity(t *testing.T, database *sql.DB, activityID, actorDID, subject, kind string) {
	t.Helper()
	ctx := context.Background()
	_, err := database.ExecContext(ctx, `
		INSERT INTO outbound_activities (activity_id, actor_did, kind, payload, parent_at_uri)
		VALUES ($1, $2, $3, '{"type":"Like"}'::jsonb, $4)`, activityID, actorDID, kind, subject)
	require.NoError(t, err)
	_, err = database.ExecContext(ctx, `
		INSERT INTO outbound_deliveries (activity_id, target_inbox, ordering_key, state,
		                                 last_status_code, delivered_at)
		VALUES ($1, $2, $3, 'delivered', 202, now())`,
		activityID, "https://lemmy.world/inbox", "https://lemmy.world/c/technology")
	require.NoError(t, err)
}
