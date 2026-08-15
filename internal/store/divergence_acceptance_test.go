package store

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/testutil"
)

// TASK 17e CYCLE 2 — AN ACCEPTANCE THAT NEVER REACHED THE PEER.
//
// This is the divergence a Coves user can SEE. The community's own repo carries
// an acceptance record, so Coves renders the post in that community; Lemmy was
// never told. Both sides are local — the acceptance ledger, and
// outbound_objects.accepted_at, which is stamped ONLY by delivery success and is
// therefore the one honest "did it land" signal in the schema.
//
// THE CLASSIFICATION IS THE POINT, because the three reasons need three
// different operator responses, and lumping them makes the report unusable:
//
//	cancelled — a DECISION took it out of the queue (a ban, an opt-out). Usually
//	            correct to leave exactly as it is; the report exists so the
//	            resulting Coves-only post is visible rather than silent.
//	poisoned  — a delivery that FAILED. Redrivable, and the operator surface for
//	            that already exists (POST /admin/outbound/redrive).
//	stale     — still pending long after it should have gone. Nothing is wrong
//	            with the post; the QUEUE is not moving, which is a different
//	            investigation entirely.
//
// AND THE FALSE POSITIVE IS THE WHOLE TEST. A query that reported every
// acceptance would satisfy every "is it found?" assertion here and be worse than
// no report at all: an operator who cannot tell the finding from the background
// stops reading it. So every case below carries a delivered sibling that must be
// absent, and the siblings differ from the findings in delivery state and in
// nothing else.

const (
	dvCommunityDID   = "did:plc:dvcommunity00000001"
	dvCommunityAPID  = "https://lemmy.world/c/divergence"
	dvCommunityInbox = "https://lemmy.world/c/divergence/inbox"
	dvAuthorDID      = "did:plc:dvauthor000000000001"
	dvUserOrigin     = "https://coves.social"

	// dvStaleAfter is the window the tests pass explicitly. The production
	// default is a named constant beside the sweep; what is pinned here is the
	// BOUNDARY behaviour, at one minute either side of whatever window it is.
	dvStaleAfter = time.Hour
)

// dvPost describes one accepted post to seed, and the delivery it is waiting on.
type dvPost struct {
	rkey string
	// state / errorClass / age describe the delivery. age is how long ago the
	// delivery row was created.
	state      DeliveryState
	errorClass string
	age        time.Duration
	// accepted stamps outbound_objects.accepted_at — "the peer took it".
	accepted bool
	// noDelivery omits the delivery row entirely.
	noDelivery bool
}

func (p dvPost) uri() string {
	return "at://" + dvAuthorDID + "/social.coves.community.postv2/" + p.rkey
}

func (p dvPost) apObjectID() string {
	return dvUserOrigin + "/ap/object/" + dvAuthorDID + "/social.coves.community.postv2/" + p.rkey
}

func (p dvPost) activityID() string {
	return dvUserOrigin + "/ap/activity/" + p.rkey
}

// seedAcceptedPost writes the whole local world for one accepted post: the
// acceptance ledger row, the outbound object it federates as, and (unless the
// case says otherwise) the activity and delivery carrying it.
//
// It is written through raw SQL on purpose. The engine that produces this state
// lives two packages up, and the sweep must read the state as it IS on disk —
// including states no current writer produces, which is exactly what a
// divergence report is for.
func seedAcceptedPost(t *testing.T, database *sql.DB, post dvPost) {
	t.Helper()
	ctx := context.Background()

	acceptedAt := "NULL"
	if post.accepted {
		acceptedAt = "now()"
	}
	_, err := database.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO outbound_objects (at_uri, ap_object_id, community_did, community_ap_id,
		                              translated_snapshot, accepted_at)
		VALUES ($1, $2, $3, $4, '{"type":"Page"}'::jsonb, %s)`, acceptedAt),
		post.uri(), post.apObjectID(), dvCommunityDID, dvCommunityAPID)
	require.NoError(t, err, "seed outbound_objects for %s", post.rkey)

	_, err = database.ExecContext(ctx, `
		INSERT INTO admissions (community_did, post_uri, author_did, status, decision_code)
		VALUES ($1, $2, $3, 'accepted', '')`,
		dvCommunityDID, post.uri(), dvAuthorDID)
	require.NoError(t, err, "seed admissions for %s", post.rkey)

	if post.noDelivery {
		return
	}

	// The payload carries the object id, exactly as the translator writes it —
	// it is the same correspondence the delivery worker uses to stamp
	// accepted_at when the Create succeeds.
	_, err = database.ExecContext(ctx, `
		INSERT INTO outbound_activities (activity_id, actor_did, kind, payload)
		VALUES ($1, $2, 'Create', $3::jsonb)`,
		post.activityID(), dvAuthorDID,
		fmt.Sprintf(`{"id":%q,"type":"Create","actor":%q,"object":{"type":"Page","id":%q}}`,
			post.activityID(), dvUserOrigin+"/ap/actor/"+dvAuthorDID, post.apObjectID()))
	require.NoError(t, err, "seed outbound_activities for %s", post.rkey)

	_, err = database.ExecContext(ctx, `
		INSERT INTO outbound_deliveries (activity_id, target_inbox, ordering_key, state,
		                                 last_error_class, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, now() - $6::interval, now() - $6::interval)`,
		post.activityID(), dvCommunityInbox, dvCommunityAPID, string(post.state), post.errorClass,
		fmt.Sprintf("%d seconds", int(post.age.Seconds())))
	require.NoError(t, err, "seed outbound_deliveries for %s", post.rkey)
}

// acceptanceTestDB truncates every table this comparison reads. It starts from
// the cycle-1 list rather than a second one of its own: this sweep READS every
// table any other test writes, so a divergence fixture is the worst possible
// place for two truncate lists to drift apart.
func acceptanceTestDB(t *testing.T) *sql.DB {
	t.Helper()
	database := divergenceTestDB(t)
	testutil.Truncate(t, database, "admissions", "outbound_objects")
	return database
}

// urisOf lists the reported post uris, so assertions read as sets.
func urisOf(found []UndeliveredAcceptance) []string {
	uris := make([]string, 0, len(found))
	for _, entry := range found {
		uris = append(uris, entry.PostURI)
	}
	return uris
}

// ---------------------------------------------------------------------------
// The three classes, and the sibling that must not appear
// ---------------------------------------------------------------------------

// TestUndeliveredAcceptances_ClassifiesByDeliveryState is the cycle's contract.
func TestUndeliveredAcceptances_ClassifiesByDeliveryState(t *testing.T) {
	database := acceptanceTestDB(t)

	cancelled := dvPost{rkey: "3lzdvacc00001", state: DeliveryStateCancelled, age: 2 * time.Hour}
	poisoned := dvPost{rkey: "3lzdvacc00002", state: DeliveryStatePoisoned, errorClass: "4xx", age: 2 * time.Hour}
	stale := dvPost{rkey: "3lzdvacc00003", state: DeliveryStatePending, age: 2 * time.Hour}
	// THE FALSE-POSITIVE CONTROL. Same author, same community, same repo,
	// adjacent rkey — it differs from the three above in delivery state and in
	// nothing else that any acceptance-side query can see.
	landed := dvPost{rkey: "3lzdvacc00004", state: DeliveryStateDelivered, age: 2 * time.Hour, accepted: true}

	for _, post := range []dvPost{cancelled, poisoned, stale, landed} {
		seedAcceptedPost(t, database, post)
	}

	found, err := NewDivergences(database).UndeliveredAcceptances(context.Background(), dvStaleAfter)
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{cancelled.uri(), poisoned.uri(), stale.uri()}, urisOf(found),
		"exactly the three undelivered acceptances, and NOT the one that landed. A query that "+
			"reported every acceptance would pass every 'is it found' assertion in this file "+
			"while being worse than no report: an operator who cannot tell the finding from the "+
			"background stops reading it, and the one real divergence goes out with the noise")

	byURI := map[string]UndeliveredAcceptance{}
	for _, entry := range found {
		byURI[entry.PostURI] = entry
	}
	require.Len(t, byURI, 3, "one row per post")

	assert.Equal(t, DeliveryStateCancelled, byURI[cancelled.uri()].DeliveryState,
		"the delivery state rides along, because the three reasons are three different "+
			"investigations: a cancelled delivery was a DECISION (a ban, an opt-out) and is "+
			"usually correct to leave alone")
	assert.Equal(t, DeliveryStatePoisoned, byURI[poisoned.uri()].DeliveryState,
		"a poisoned one FAILED and is redrivable through an operator surface that already exists")
	assert.Equal(t, DeliveryStatePending, byURI[stale.uri()].DeliveryState,
		"and a stale pending one says nothing is wrong with the POST — the queue is not moving, "+
			"which is a different problem with a different fix")

	for _, entry := range found {
		assert.Equal(t, dvCommunityDID, entry.CommunityDID,
			"every entry names the community whose repo carries the acceptance: that repo is "+
				"where an operator looks, and one post can be accepted into more than one")
	}
}

// ---------------------------------------------------------------------------
// The staleness boundary
// ---------------------------------------------------------------------------

// TestUndeliveredAcceptances_PendingIsStaleOnlyPastTheThreshold pins the EDGE
// rather than a value deep inside the window.
//
// A pending delivery is the ordinary state of every post between acceptance and
// delivery, so the threshold is the only thing separating "in flight" from
// "stuck". Tested at one minute either side, a threshold that drifts — or one
// applied to the wrong column, or with the comparison inverted — fails here
// instead of quietly reporting every post the bridge accepts.
func TestUndeliveredAcceptances_PendingIsStaleOnlyPastTheThreshold(t *testing.T) {
	database := acceptanceTestDB(t)

	justInside := dvPost{rkey: "3lzdvage00001", state: DeliveryStatePending, age: dvStaleAfter - time.Minute}
	justOutside := dvPost{rkey: "3lzdvage00002", state: DeliveryStatePending, age: dvStaleAfter + time.Minute}
	seedAcceptedPost(t, database, justInside)
	seedAcceptedPost(t, database, justOutside)

	found, err := NewDivergences(database).UndeliveredAcceptances(context.Background(), dvStaleAfter)
	require.NoError(t, err)

	assert.Equal(t, []string{justOutside.uri()}, urisOf(found),
		"only the delivery PAST the window is a divergence. A post accepted a minute ago and "+
			"not yet delivered is the system working — reporting it makes every healthy post a "+
			"finding, and the report becomes a list of everything the bridge has ever accepted")

	// And the window is a real dial, not a constant baked into the SQL: widening
	// it past both ages must empty the result.
	found, err = NewDivergences(database).UndeliveredAcceptances(context.Background(), 3*time.Hour)
	require.NoError(t, err)
	assert.Empty(t, found,
		"with a wider window neither delivery is stale yet: the threshold has to be the "+
			"parameter, or an operator tuning it changes nothing")
}

// ---------------------------------------------------------------------------
// The one that looks exactly like the problem and is the opposite
// ---------------------------------------------------------------------------

// TestUndeliveredAcceptances_AHeldSettlementIsNotADivergence is the case this
// report would otherwise cry wolf on.
//
// A delivery HELD FOR SETTLEMENT is pending, old, and carries an unstamped
// accepted_at — pixel-identical to a stuck delivery on every column this query
// reads. It is the opposite: the peer ALREADY ACCEPTED the activity, and the
// only thing outstanding is our own bookkeeping, which the worker resumes on its
// next claim. The stamp is missing precisely BECAUSE the settlement that writes
// it is the step that failed.
//
// Counting it means the report fires on every settlement retry — a routine,
// self-healing event — and a report that cries wolf is worse than no report:
// the operator who learns to ignore it also ignores the cancelled acceptance
// sitting next to it, which is the finding that never self-heals.
func TestUndeliveredAcceptances_AHeldSettlementIsNotADivergence(t *testing.T) {
	database := acceptanceTestDB(t)

	held := dvPost{
		rkey:       "3lzdvheld00001",
		state:      DeliveryStatePending,
		errorClass: DeliveryHeldForSettlement,
		age:        2 * time.Hour,
	}
	// A genuinely stuck delivery beside it, so "found nothing" cannot pass by
	// the query being broken: the two differ ONLY in last_error_class.
	stuck := dvPost{rkey: "3lzdvheld00002", state: DeliveryStatePending, age: 2 * time.Hour}
	seedAcceptedPost(t, database, held)
	seedAcceptedPost(t, database, stuck)

	found, err := NewDivergences(database).UndeliveredAcceptances(context.Background(), dvStaleAfter)
	require.NoError(t, err)

	assert.Equal(t, []string{stuck.uri()}, urisOf(found),
		"the held delivery is NOT a divergence and the stuck one is. They are identical on "+
			"every column this query reads except last_error_class = %q, and that one column "+
			"inverts the meaning: held means the peer ALREADY ACCEPTED it and our own "+
			"bookkeeping is outstanding — the accepted_at stamp is missing precisely because "+
			"writing it is the step that failed. Reporting it fires on every settlement retry, "+
			"and an operator who learns to ignore this report also ignores the cancelled "+
			"acceptance beside it, which is the one that never heals itself",
		DeliveryHeldForSettlement)
}

// ---------------------------------------------------------------------------
// The OTHER exclusion, pinned on its own
// ---------------------------------------------------------------------------

// TestUndeliveredAcceptances_AnEditThatFailedIsNotAPostThatNeverLanded pins
// `accepted_at IS NULL` as an INDEPENDENTLY NECESSARY term.
//
// Every other case in this file is excluded twice over — the delivered sibling
// has both a stamped accepted_at and a delivered delivery — so either term
// alone keeps the whole suite green and neither is actually pinned. This is the
// state that separates them, and it is ordinary: a post that federated fine, and
// an EDIT that did not.
//
// The classification reads the LATEST delivery, which here is the cancelled
// Update. On the delivery state alone this post is "an acceptance that never
// reached the peer" — and that is false in the way that matters most to an
// operator: the peer HAS this post. Only the edit is missing. Reporting it
// sends someone to investigate a community view that is correct, and worse, it
// is the most common shape in the report once editing sees real use, so the
// class fills up with posts that are on Lemmy right now.
func TestUndeliveredAcceptances_AnEditThatFailedIsNotAPostThatNeverLanded(t *testing.T) {
	database := acceptanceTestDB(t)

	// The post itself: Create delivered, and the stamp to prove the peer took it.
	edited := dvPost{rkey: "3lzdvedit00001", state: DeliveryStateDelivered, age: 3 * time.Hour, accepted: true}
	seedAcceptedPost(t, database, edited)
	// The edit, later and cancelled — the newest delivery for this object.
	seedFollowUpDelivery(t, database, edited, "Update", DeliveryStateCancelled, "", time.Hour)

	// A post that genuinely never landed, so "found nothing" cannot pass by the
	// query being broken. The two differ ONLY in accepted_at.
	never := dvPost{rkey: "3lzdvedit00002", state: DeliveryStateCancelled, age: 3 * time.Hour}
	seedAcceptedPost(t, database, never)

	found, err := NewDivergences(database).UndeliveredAcceptances(context.Background(), dvStaleAfter)
	require.NoError(t, err)

	assert.Equal(t, []string{never.uri()}, urisOf(found),
		"a post whose CREATE landed is not an undelivered acceptance, however its later edits "+
			"fared: accepted_at is stamped, the peer has the post, and the community view is "+
			"correct. The delivery-state predicate cannot see that difference — the newest "+
			"delivery here is cancelled either way — so accepted_at IS NULL is doing this work "+
			"alone. Without it the class fills with posts that are on Lemmy right now, and the "+
			"one post that never arrived is lost among them")
}

// seedFollowUpDelivery adds a LATER activity and delivery for a post that
// already has one — an edit, a re-delivery — so the fixture can express a post
// whose history has more than one step.
func seedFollowUpDelivery(t *testing.T, database *sql.DB, post dvPost, kind string,
	state DeliveryState, errorClass string, age time.Duration) {
	t.Helper()
	ctx := context.Background()
	activityID := post.activityID() + "-" + kind

	_, err := database.ExecContext(ctx, `
		INSERT INTO outbound_activities (activity_id, actor_did, kind, payload)
		VALUES ($1, $2, $3, $4::jsonb)`,
		activityID, dvAuthorDID, kind,
		fmt.Sprintf(`{"id":%q,"type":%q,"actor":%q,"object":{"type":"Page","id":%q}}`,
			activityID, kind, dvUserOrigin+"/ap/actor/"+dvAuthorDID, post.apObjectID()))
	require.NoError(t, err, "seed follow-up %s for %s", kind, post.rkey)

	_, err = database.ExecContext(ctx, `
		INSERT INTO outbound_deliveries (activity_id, target_inbox, ordering_key, state,
		                                 last_error_class, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, now() - $6::interval, now() - $6::interval)`,
		activityID, dvCommunityInbox, dvCommunityAPID, string(state), errorClass,
		fmt.Sprintf("%d seconds", int(age.Seconds())))
	require.NoError(t, err, "seed follow-up delivery for %s", post.rkey)
}
