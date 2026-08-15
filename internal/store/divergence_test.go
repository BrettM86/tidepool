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
