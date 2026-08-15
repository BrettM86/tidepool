package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// The reconciliation sweep's reads (task 17e, decision 19).
//
// READ-ONLY BY CONSTRUCTION, and that is the point of giving the sweep its own
// store file rather than adding methods to the existing repositories: decision
// 19 forbids self-healing writes, and a reconciler whose store cannot write is
// a reconciler that cannot heal by accident. Nothing in this file may ever grow
// an Exec — there is none below, and adding one would make this the only
// component that both reads a divergence and can act on it, which is exactly
// the shape decision 19 forbids.

// PersonaVoteEvent is one INBOUND vote_events row whose voter resolves to one
// of our own native personas — a vote the bridge cast on a user's behalf,
// arriving back as if a stranger had cast it.
//
// Expected count: ZERO, forever. Task 17a's probe refuses such a row before it
// is written, so one existing means the probe was bypassed and the subject's
// tally is double-counting our own echo: once as an inbound event, once as the
// delivered outbound vote the 17b reseed subtracts.
type PersonaVoteEvent struct {
	// ActivityID is the inbound activity the row was written under.
	ActivityID string
	// VoterAPID is the AP actor id the vote was attributed to.
	VoterAPID string
	// SubjectAPID is the object voted on — what an operator needs to know is
	// mis-counted.
	SubjectAPID string
	// ActorDID is the persona that id belongs to.
	ActorDID string
}

// UndeliveredAcceptance is a post the community's own repo says it accepted —
// Coves renders it in that community — whose federation never reached the peer.
//
// The two sides are the acceptance ledger (admissions, status accepted) and
// outbound_objects.accepted_at, which is stamped ONLY by delivery success and is
// therefore the honest "did it land" signal. DeliveryState carries WHY, because
// the three reasons need different operator responses: a cancelled delivery was
// a decision (a ban, an opt-out) and is usually correct to leave alone; a
// poisoned one is a delivery that failed and may be redrivable; a pending one
// stuck past the staleness window is a queue that is not moving.
type UndeliveredAcceptance struct {
	// PostURI is the postv2 at-uri in the author's repo — the subject an
	// operator investigates.
	PostURI string
	// CommunityDID is the bridged community whose repo carries the acceptance.
	CommunityDID string
	// ActivityID is the outbound activity that was to carry it, "" when the
	// acceptance never enqueued one.
	ActivityID string
	// DeliveryState is the state of that activity's delivery, and is what the
	// sweep classifies on.
	DeliveryState DeliveryState
	// LastErrorClass is the delivery's outcome class, which is how a delivery
	// HELD FOR SETTLEMENT is told from a stuck one.
	LastErrorClass string
}

// Divergences reads the comparisons the reconciliation sweep reports on. Every
// method is a READ; both sides of every comparison are local.
type Divergences interface {
	// PersonaVoteEvents lists inbound vote events attributed to our own
	// personas, joining vote_events.voter_ap_id to ap_actors.actor_id.
	PersonaVoteEvents(ctx context.Context) ([]PersonaVoteEvent, error)

	// UndeliveredAcceptances lists accepted posts whose federation never
	// reached the peer. A pending delivery counts only once it is older than
	// staleAfter: until then it is in flight, which is the ordinary state of
	// every post between acceptance and delivery.
	UndeliveredAcceptances(ctx context.Context, staleAfter time.Duration) ([]UndeliveredAcceptance, error)

	// RecastDivergences lists (actor, subject) pairs where a peer is holding a
	// vote this bridge no longer claims — see RecastDivergence.
	RecastDivergences(ctx context.Context) ([]RecastDivergence, error)

	// UnknownDeliveryOutcomes lists poisoned deliveries: activities we SENT and
	// never got confirmation for — see UnknownDeliveryOutcome.
	UnknownDeliveryOutcomes(ctx context.Context) ([]UnknownDeliveryOutcome, error)
}

// UnknownDeliveryOutcome is a delivery whose result this bridge DOES NOT KNOW.
//
// A poisoned delivery is one we sent and never got confirmation for, and the
// two ways that happens are not equally informative:
//
//	REFUSED    — the peer answered with a status code. That is EVIDENCE OF
//	             non-application, not proof: a peer can apply an activity and
//	             then fail to respond, and several implementations do exactly
//	             that under load.
//	UNANSWERED — a transport failure with no status at all. Silent about
//	             everything: the request may never have arrived, or may have
//	             been applied and the response lost.
//
// Neither is a fact about what the peer holds, which is why this is its own
// read rather than a column on the undelivered-acceptance comparison. The
// separation is kept all the way to the metric names: the only true claim about
// these rows is that we do not know.
type UnknownDeliveryOutcome struct {
	// ActivityID is what was sent.
	ActivityID string
	// TargetInbox is who it was sent to — the instance an operator would have
	// to ask, since we cannot.
	TargetInbox string
	// Kind is the activity kind, so an operator can tell a lost vote from a
	// lost post without a second query.
	Kind string
	// LastErrorClass is the delivery's recorded failure class.
	LastErrorClass string
	// LastStatusCode is the status the peer answered with, or 0 when it never
	// answered.
	LastStatusCode int
	// Refused reports whether a status code came back at all. It is the whole
	// distinction between the two sub-counts, and it is stored rather than
	// derived so that "0" cannot be read as "the peer said 0".
	Refused bool
}

// RecastDivergence is a vote a peer HOLDS that our own state does not claim.
//
// It is produced by the re-cast race 17b recorded and deferred: re-casting a
// delivered vote re-upserts the SAME row back to pending under a new activity
// id, while the peer still holds the old vote in the old direction. Transient
// while the new delivery is in flight — and PERMANENT the moment it poisons.
//
// IT CANNOT BE READ FROM THE VOTE ROW, which is what makes it a reconciliation
// item rather than a query. worker.voteCallback resolves its row through
// GetByActivityID and returns nil on NotFound, so when a delivery that was
// already in flight lands AFTER a re-cast, its id no longer matches
// current_activity_id and the settlement silently no-ops. The row is precisely
// the evidence the bug erases. outbound_activities is append-only and its
// parent_at_uri carries the subject at-uri from both vote enqueue sites, so the
// activity/delivery history is the durable record of what each peer was
// actually told.
type RecastDivergence struct {
	// ActorDID is the persona whose vote it is.
	ActorDID string
	// SubjectATURI is the thing voted on — the pair (actor, subject) is the
	// identity of a vote, since only one may be live at a time.
	SubjectATURI string
	// DeliveredActivityID is the vote activity the PEER ACCEPTED: what they are
	// counting right now, and the only thing an operator can reconcile against.
	DeliveredActivityID string
}

type postgresDivergences struct{ db *sql.DB }

// NewDivergences creates the postgres-backed read-only divergence reader.
func NewDivergences(db *sql.DB) Divergences { return &postgresDivergences{db: db} }

func (r *postgresDivergences) PersonaVoteEvents(ctx context.Context) ([]PersonaVoteEvent, error) {
	// THE JOIN IS ON IDENTITY, NEVER ON ACTIVITY IDS. Decision 16 originally
	// proposed matching inbound votes against outbound_activities by activity
	// id; the 17a plan review overturned it on measured behaviour — Lemmy 0.19
	// reconstructs Announce{Undo{Like}} with a FRESHLY GENERATED inner activity
	// id and types it "Like" even when the live vote is a Dislike. An id-keyed
	// probe therefore matches none of the echoes it exists to find, reports
	// zero, and is indistinguishable from a healthy bridge. The voter's identity
	// is the only stable handle.
	//
	// ap_actors — NEVER bridged_actors. ap_actors holds OUR native personas, the
	// Coves users this bridge federates FOR. bridged_actors holds REAL LEMMY
	// HUMANS mirrored INTO atproto, whose DIDs we minted and whose votes are the
	// entire inbound vote stream. Probing bridged_actors here would report every
	// genuine vote on the network as our own echo — every community's tally
	// reading as double-counted — and the two tables are confusable precisely
	// because both carry DIDs we minted and AP ids we can spell.
	//
	// Exact string equality, deliberately narrow: migration 023's one-time
	// cleanup used the same equality, so this reports exactly the population
	// that cleanup would have removed. A zero therefore means "no
	// exactly-spelled persona vote", not "no persona vote" — see the KNOWNNARROW
	// case in divergence_test.go, which pins that limit rather than papering it.
	query := `
		SELECT v.activity_id, v.voter_ap_id, v.subject_ap_id, a.did
		  FROM vote_events v
		  JOIN ap_actors a ON a.actor_id = v.voter_ap_id
		 ORDER BY v.activity_id`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list persona vote events: %w", err)
	}
	defer func() { _ = rows.Close() }()

	// Non-nil, so a caller rendering this straight to JSON emits [] rather than
	// null: an operator reading "null" cannot tell an empty result from a field
	// the sweep never filled.
	found := make([]PersonaVoteEvent, 0)
	for rows.Next() {
		var event PersonaVoteEvent
		if err := rows.Scan(&event.ActivityID, &event.VoterAPID, &event.SubjectAPID, &event.ActorDID); err != nil {
			return nil, fmt.Errorf("scan persona vote event: %w", err)
		}
		found = append(found, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list persona vote events: %w", err)
	}
	return found, nil
}

func (r *postgresDivergences) UndeliveredAcceptances(ctx context.Context, staleAfter time.Duration) ([]UndeliveredAcceptance, error) {
	// BOTH SIDES ARE LOCAL, and the second one is the whole comparison.
	//
	//   admissions.status = 'accepted' — the community's repo carries an
	//     acceptance record, so Coves renders the post in that community.
	//   outbound_objects.accepted_at IS NULL — the peer never confirmed it. That
	//     column is stamped ONLY by delivery success (worker.stampAccepted), so
	//     it is the one signal in the schema that means "it landed" rather than
	//     "we tried".
	//
	// The activity join is the SAME correspondence the worker uses to stamp
	// that column: a Create/Update payload names the object it federates in
	// object.id, and that id is outbound_objects.ap_object_id. Read as a JSONB
	// path rather than a substring search, so an id that merely appears
	// somewhere in another activity's payload cannot masquerade as this one's.
	//
	// LATEST DELIVERY FIRST, then classify. Picking the newest attempt (by seq)
	// and asking what state IT is in describes the situation now; filtering
	// first and taking the newest survivor would report a post whose older
	// attempt was cancelled while a fresh one is still in flight.
	//
	// KNOWN GAP, stated rather than silently swallowed: an acceptance with NO
	// delivery row at all (the LEFT JOINs yield NULL) is not reported, because
	// the three classes here are all delivery states and it has none. It is a
	// real divergence — nothing will ever carry that post — and it needs its own
	// class rather than being folded into one that would misdescribe it.
	query := `
		WITH latest AS (
			SELECT DISTINCT ON (o.at_uri)
			       o.at_uri            AS post_uri,
			       o.community_did     AS community_did,
			       d.activity_id       AS activity_id,
			       d.state             AS delivery_state,
			       d.last_error_class  AS last_error_class,
			       d.created_at        AS delivery_created_at
			  FROM admissions adm
			  JOIN outbound_objects o ON o.at_uri = adm.post_uri
			  LEFT JOIN outbound_activities a
			         ON a.payload -> 'object' ->> 'id' = o.ap_object_id
			  LEFT JOIN outbound_deliveries d ON d.activity_id = a.activity_id
			 WHERE adm.status = 'accepted'
			   AND o.accepted_at IS NULL
			 ORDER BY o.at_uri, d.seq DESC NULLS LAST
		)
		SELECT post_uri,
		       community_did,
		       COALESCE(activity_id, ''),
		       COALESCE(delivery_state, ''),
		       COALESCE(last_error_class, '')
		  FROM latest
		 WHERE delivery_state IN ($1, $2)
		    OR (delivery_state = $3
		        AND last_error_class <> $4
		        AND delivery_created_at < now() - $5::interval)
		 ORDER BY post_uri`

	// A HELD SETTLEMENT IS NOT A DIVERGENCE, and it is the one case this report
	// would otherwise cry wolf on. It is pending, old, and unstamped — identical
	// to a stuck delivery on every column above except this one — and it means
	// the opposite: the peer ALREADY ACCEPTED the activity, and accepted_at is
	// missing precisely because writing it is the step that failed. The worker
	// resumes it on its next claim. Reporting it fires the sweep on every
	// settlement retry, and an operator who learns to ignore this report also
	// ignores the cancelled acceptance beside it, which is the finding that
	// never heals itself.
	rows, err := r.db.QueryContext(ctx, query,
		string(DeliveryStateCancelled), string(DeliveryStatePoisoned),
		string(DeliveryStatePending), DeliveryHeldForSettlement,
		fmt.Sprintf("%d seconds", int(staleAfter.Seconds())))
	if err != nil {
		return nil, fmt.Errorf("list undelivered acceptances: %w", err)
	}
	defer func() { _ = rows.Close() }()

	found := make([]UndeliveredAcceptance, 0)
	for rows.Next() {
		var entry UndeliveredAcceptance
		var state string
		if err := rows.Scan(&entry.PostURI, &entry.CommunityDID, &entry.ActivityID,
			&state, &entry.LastErrorClass); err != nil {
			return nil, fmt.Errorf("scan undelivered acceptance: %w", err)
		}
		entry.DeliveryState = DeliveryState(state)
		found = append(found, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list undelivered acceptances: %w", err)
	}
	return found, nil
}

func (r *postgresDivergences) RecastDivergences(ctx context.Context) ([]RecastDivergence, error) {
	// THE EVIDENCE IS THE HISTORY, NEVER THE VOTE ROW. outbound_activities is
	// append-only and its parent_at_uri carries the subject at-uri from both
	// vote enqueue sites, so a Like/Dislike with a DELIVERED delivery is durable
	// proof of what a peer was told — which is exactly what the vote row stops
	// being the moment a re-cast rewrites it.
	//
	// The row appears below only as an EXCLUSION, and the direction matters: it
	// can suppress a finding, never create one. If our ledger still names this
	// exact activity as the live vote AND still calls it delivered, then we
	// account for what the peer holds and there is nothing to reconcile. When
	// the row has been reset, retracted or deleted — every shape this bug takes
	// — the row cannot answer, and the history stands on its own.
	//
	// TWO INDEPENDENT EXCLUSIONS, because they answer different questions and
	// each is the whole defence against a different way of ruining this report:
	//
	//   the ledger still claims it — without this, EVERY cleanly delivered vote
	//     is a finding. That is most of the highest-volume table in the system,
	//     and a class that names all of it hides the one case that matters.
	//   a delivered Undo followed it — without this, a withdrawal that WORKED is
	//     reported forever. The history is append-only, so the delivered
	//     Like/Dislike never goes away; only the Undo beside it says the peer
	//     holds nothing now.
	//
	// The Undo comparison is by time rather than by id on purpose: an Undo names
	// the activity it withdraws in its payload, but a re-cast mints new ids, so
	// id-chasing would miss an Undo that withdrew an earlier incarnation of the
	// same (actor, subject) vote. Only one vote per pair may be live at a time,
	// which is what makes "any delivered Undo at or after this delivery" the
	// right question.
	//
	// DISTINCT because one activity may have several deliveries (the fan-out
	// schema); one delivered copy is one thing the peer holds.
	query := `
		SELECT DISTINCT a.actor_did, a.parent_at_uri, a.activity_id
		  FROM outbound_activities a
		  JOIN outbound_deliveries d
		    ON d.activity_id = a.activity_id AND d.state = $1
		 WHERE a.kind IN ($2, $3)
		   AND a.parent_at_uri <> ''
		   AND NOT EXISTS (
		         SELECT 1
		           FROM outbound_votes v
		          WHERE v.actor_did = a.actor_did
		            AND v.subject_at_uri = a.parent_at_uri
		            AND v.current_activity_id = a.activity_id
		            AND v.delivered_state = $4)
		   AND NOT EXISTS (
		         SELECT 1
		           FROM outbound_activities u
		           JOIN outbound_deliveries ud
		             ON ud.activity_id = u.activity_id AND ud.state = $1
		          WHERE u.kind = $5
		            AND u.actor_did = a.actor_did
		            AND u.parent_at_uri = a.parent_at_uri
		            AND u.created_at >= a.created_at)
		 ORDER BY a.actor_did, a.parent_at_uri, a.activity_id`

	rows, err := r.db.QueryContext(ctx, query,
		string(DeliveryStateDelivered), "Like", "Dislike",
		string(DeliveredStateDelivered), "Undo")
	if err != nil {
		return nil, fmt.Errorf("list recast divergences: %w", err)
	}
	defer func() { _ = rows.Close() }()

	found := make([]RecastDivergence, 0)
	for rows.Next() {
		var entry RecastDivergence
		if err := rows.Scan(&entry.ActorDID, &entry.SubjectATURI, &entry.DeliveredActivityID); err != nil {
			return nil, fmt.Errorf("scan recast divergence: %w", err)
		}
		found = append(found, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list recast divergences: %w", err)
	}
	return found, nil
}

func (r *postgresDivergences) UnknownDeliveryOutcomes(ctx context.Context) ([]UnknownDeliveryOutcome, error) {
	// POISONED ONLY, and every other state is excluded for a reason that is not
	// symmetry:
	//
	//   delivered — the peer confirmed it. The one outcome here we DO know.
	//   cancelled — it never reached the wire: a ban or an opt-out took it out of
	//     the queue, so the peer does not have it and that is a fact, not a
	//     question. Cycle 2's cancelled class already names it. Folding it in
	//     would put a KNOWN non-delivery into the bucket whose entire meaning is
	//     that the answer is unavailable, and these are the two numbers an
	//     operator has to be able to trust as small.
	//   pending — still in flight, or held for settlement. Nothing is unresolved
	//     about a delivery that has not finished trying.
	//
	// AGE IS NOT PART OF THE DEFINITION, unlike the stale-acceptance class: a
	// poisoned delivery is terminal the moment it poisons — nothing re-drives it
	// — so a recent one and an old one are the same state and the same unknown.
	//
	// REFUSED IS COMPUTED FROM WHETHER A PEER ANSWERED, NOT FROM THE NUMBER, and
	// this is the one subtle thing in the query. The column is nullable, but the
	// worker does not use the NULL: classify() passes a literal 0 for a transport
	// failure and for an unresolvable signer (worker.go), and MarkPoisoned writes
	// that 0. So "the peer never answered" arrives in this table in TWO
	// spellings, NULL and 0, and only a predicate that treats both as silence
	// keeps a dial timeout out of the sub-count that says the peer spoke. It is
	// selected as a boolean rather than left to the caller to infer, so that no
	// reader downstream can rediscover the wrong rule from LastStatusCode.
	query := `
		SELECT d.activity_id,
		       d.target_inbox,
		       a.kind,
		       d.last_error_class,
		       COALESCE(d.last_status_code, 0),
		       COALESCE(d.last_status_code, 0) > 0
		  FROM outbound_deliveries d
		  JOIN outbound_activities a ON a.activity_id = d.activity_id
		 WHERE d.state = $1
		 ORDER BY d.activity_id, d.target_inbox`

	rows, err := r.db.QueryContext(ctx, query, string(DeliveryStatePoisoned))
	if err != nil {
		return nil, fmt.Errorf("list unknown delivery outcomes: %w", err)
	}
	defer func() { _ = rows.Close() }()

	found := make([]UnknownDeliveryOutcome, 0)
	for rows.Next() {
		var entry UnknownDeliveryOutcome
		if err := rows.Scan(&entry.ActivityID, &entry.TargetInbox, &entry.Kind,
			&entry.LastErrorClass, &entry.LastStatusCode, &entry.Refused); err != nil {
			return nil, fmt.Errorf("scan unknown delivery outcome: %w", err)
		}
		found = append(found, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list unknown delivery outcomes: %w", err)
	}
	return found, nil
}
