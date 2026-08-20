package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lib/pq"
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
//
// AND THAT IS NOT LEFT TO THE COMMENT. Every read below runs inside BEGIN …
// READ ONLY (see readOnlyTx), so Postgres itself refuses a write here — the one
// form of enforcement that outlives everyone who has read this paragraph.

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

// MaxDivergenceExamples is the EXAMPLE BUDGET, and it is applied as a SQL
// LIMIT rather than by cutting a finished slice down.
//
// The sweep's report caps the examples it carries (ingest.MaxDivergenceEntries,
// which is this constant), and a cap applied in Go after the rows arrive bounds
// nothing that matters: the sweep that reaches the cap is the sweep running
// against a stopped queue, and every one of those rows would already have been
// scanned into a slice, in the process an operator is trying to keep alive, on
// an endpoint they reached for BECAUSE it is struggling. So the bound goes to
// the database — every list below returns at most this many rows.
//
// THE COUNTS ARE NOT BOUNDED WITH THEM. Each list has a matching exact
// COUNT(*), because the count is what an operator SIZES the incident from: a
// number capped at the length of a page would make "500" mean both "500" and
// "a catastrophe". That is the whole reason these are two statements rather
// than one truncated read.
const MaxDivergenceExamples = 500

// Divergences reads the comparisons the reconciliation sweep reports on. Every
// method is a READ; both sides of every comparison are local.
//
// EACH COMPARISON IS TWO METHODS — bounded examples, exact count — and the two
// are deliberately not folded into one call that returns both. The list
// signatures are what the reconciler's failure-injection doubles wrap, and more
// importantly the pair states the contract in the type: a caller that wants a
// number cannot get a truncated one by accident, and a caller that wants
// examples cannot mistake how many there were.
//
// The count is read on EVERY sweep rather than only when a list comes back
// full. It could be skipped in the common case — a list under the limit is its
// own exact count — but that would leave the counting queries dark until the
// first incident, which is the one moment nobody wants to discover them for the
// first time. Running them always makes every existing test that asserts a
// count a test of the counting query too. The cost is one extra aggregate pass
// per comparison per sweep; see FOLLOWUPS.md for what that pass costs on the
// acceptance leg specifically.
type Divergences interface {
	// PersonaVoteEvents lists inbound vote events attributed to our own
	// personas, joining vote_events.voter_ap_id to ap_actors.actor_id. At most
	// MaxDivergenceExamples rows.
	PersonaVoteEvents(ctx context.Context) ([]PersonaVoteEvent, error)

	// PersonaVoteEventCount is how many such rows exist, unbounded by the
	// example budget.
	PersonaVoteEventCount(ctx context.Context) (int, error)

	// UndeliveredAcceptances lists accepted posts whose federation never
	// reached the peer. A pending delivery counts only once it is older than
	// staleAfter: until then it is in flight, which is the ordinary state of
	// every post between acceptance and delivery. At most
	// MaxDivergenceExamples rows.
	UndeliveredAcceptances(ctx context.Context, staleAfter time.Duration) ([]UndeliveredAcceptance, error)

	// UndeliveredAcceptanceCounts is how many such posts exist, KEYED BY
	// DELIVERY STATE — the same split the sweep's three acceptance classes are
	// derived from, so the caller maps states to classes in exactly one place
	// and a state this sweep has no class for arrives as itself rather than
	// folded into a total.
	UndeliveredAcceptanceCounts(ctx context.Context, staleAfter time.Duration) (map[DeliveryState]int, error)

	// RecastDivergences lists (actor, subject) pairs where a peer is holding a
	// vote this bridge no longer claims — see RecastDivergence. At most
	// MaxDivergenceExamples rows.
	RecastDivergences(ctx context.Context) ([]RecastDivergence, error)

	// RecastDivergenceCount is how many such pairs exist.
	RecastDivergenceCount(ctx context.Context) (int, error)

	// UnknownDeliveryOutcomes lists poisoned deliveries: activities we SENT and
	// never got confirmation for — see UnknownDeliveryOutcome. At most
	// MaxDivergenceExamples rows.
	UnknownDeliveryOutcomes(ctx context.Context) ([]UnknownDeliveryOutcome, error)

	// UnknownDeliveryOutcomeCounts is how many such deliveries exist, split the
	// same way the entries are — see UnknownDeliveryCounts.
	UnknownDeliveryOutcomeCounts(ctx context.Context) (UnknownDeliveryCounts, error)
}

// UnknownDeliveryCounts is the refused/unanswered split, counted rather than
// listed.
//
// It is a struct rather than two ints returned side by side because the split
// is the only information these rows carry, and the two numbers must never be
// added: a peer that ANSWERED told us something a silent one did not. A single
// value returned here would be a total of things we do not know, which is a
// number nobody can act on.
type UnknownDeliveryCounts struct {
	// Refused is deliveries where a status code came back.
	Refused int
	// Unanswered is deliveries where nothing came back at all.
	Unanswered int
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
// delivered vote rewrites the SAME row to the new direction under a NEW
// activity id, while the peer still holds the old vote in the old direction.
// Transient while the new delivery is in flight — and PERMANENT the moment it
// poisons.
//
// IT CANNOT BE READ FROM THE VOTE ROW, which is what makes it a reconciliation
// item rather than a query. The row keeps `delivered` through the flip (the
// upsert guard defends that state), so it still says a vote of ours stands
// here — but current_activity_id has moved to the new activity, so it can no
// longer say WHICH one, and which one is the entire content of this finding.
// worker.voteCallback resolves its row through GetByActivityID and returns nil
// on NotFound, so when a delivery that was already in flight lands AFTER a
// re-cast, its id no longer matches current_activity_id and the settlement
// silently no-ops — nothing writes the old activity back into the row, ever.
// outbound_activities is append-only and its
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

// readOnlyTx opens the transaction every read in this file runs in, and it is
// the ONLY enforcement of this file's one rule that survives an edit.
//
// "This reconciler cannot write" was a comment at the top of the file plus a
// test that snapshots every table before and after a sweep. Both are worth
// having and neither stops the write: the comment is advice, and the snapshot
// test only fails AFTER someone has already added an Exec and run it. BEGIN …
// READ ONLY moves the rule into the database, which refuses the statement
// outright (25006, read_only_sql_transaction) — so a self-healing write bolted
// on here fails on the day it is written, in the author's own test run, instead
// of the day it corrupts the state the sweep exists to measure. That is decision
// 19 enforced by something other than good intentions.
//
// ONE TRANSACTION PER READ, not one per sweep. The sweep's eight reads are eight
// independent comparisons that already tolerate each other moving — nothing here
// is cross-checked between two of them — and a pass-wide snapshot would hold a
// single pool connection for the whole two-minute budget, which is the opposite
// of what the sweep timeout was added for. The cost is a BEGIN and a ROLLBACK
// per read, on a job that runs every fifteen minutes.
//
// It always ROLLBACKs, never COMMITs: a read-only transaction has nothing to
// make durable, and an ending that cannot possibly persist anything is the one
// that matches what this file claims about itself.
func (r *postgresDivergences) readOnlyTx(ctx context.Context) (*sql.Tx, func(), error) {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, nil, fmt.Errorf("begin read-only transaction: %w", err)
	}
	return tx, func() { _ = tx.Rollback() }, nil
}

// personaVoteEventsFrom is the POPULATION, written once and shared by the
// example list and the count. Two spellings of one comparison drift, and the
// drift is silent in exactly the direction that matters: a count taken over a
// slightly different join reports a size for a population nobody listed.
const personaVoteEventsFrom = `FROM vote_events v
		  JOIN ap_actors a ON a.actor_id = v.voter_ap_id`

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
	//
	// The LIMIT is the example budget reaching the database; the count below
	// reads the same population with the same FROM clause and no bound.
	query := `
		SELECT v.activity_id, v.voter_ap_id, v.subject_ap_id, a.did
		  ` + personaVoteEventsFrom + `
		 ORDER BY v.activity_id
		 LIMIT $1`

	tx, done, err := r.readOnlyTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("list persona vote events: %w", err)
	}
	defer done()

	rows, err := tx.QueryContext(ctx, query, MaxDivergenceExamples)
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

// PersonaVoteEventCount counts the same population PersonaVoteEvents lists,
// with no LIMIT. No ORDER BY either — ordering an aggregate is work whose only
// product is a sort the count throws away.
func (r *postgresDivergences) PersonaVoteEventCount(ctx context.Context) (int, error) {
	tx, done, err := r.readOnlyTx(ctx)
	if err != nil {
		return 0, fmt.Errorf("count persona vote events: %w", err)
	}
	defer done()

	var total int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) `+personaVoteEventsFrom).Scan(&total); err != nil {
		return 0, fmt.Errorf("count persona vote events: %w", err)
	}
	return total, nil
}

// undeliveredAcceptanceLatest and undeliveredAcceptanceFilter are the
// comparison, written ONCE and shared by the example list and the per-state
// counts. Restating either in a second query is how a count comes to describe a
// population the list does not: the CTE decides which delivery is the current
// one, and the filter decides which of those is a divergence.
//
// BOTH SIDES ARE LOCAL, and the second one is the whole comparison.
//
//	admissions.status = 'accepted' — the community's repo carries an
//	  acceptance record, so Coves renders the post in that community.
//	outbound_objects.accepted_at IS NULL — the peer never confirmed it. That
//	  column is stamped ONLY by delivery success (worker.stampAccepted), so
//	  it is the one signal in the schema that means "it landed" rather than
//	  "we tried".
//
// THE ACCEPTANCE JOIN IS ON THE PAIR, not on the post alone. admissions is keyed
// (community_did, post_uri) — one post may be admitted by several communities —
// while outbound_objects is keyed by at_uri and carries its OWN community_did.
// Joining on post_uri alone would let the accepted status come from one
// community's admission and the reported CommunityDID from another community's
// object row, so the report would name a community that did not accept it. No
// writer produces that shape today (each post federates to the community that
// admitted it), which is exactly why the predicate is worth spelling: it costs
// nothing and it states the invariant instead of relying on it.
//
// The activity join is the SAME correspondence the worker uses to stamp that
// column: a Create/Update payload names the object it federates in object.id,
// and that id is outbound_objects.ap_object_id. Read as a JSONB path rather
// than a substring search, so an id that merely appears somewhere in another
// activity's payload cannot masquerade as this one's. NOTHING INDEXES THAT
// EXPRESSION — it is a full pass over outbound_activities per sweep, twice now
// that the count is its own statement; see FOLLOWUPS.md, which carries the
// EXPLAIN and the candidate expression index.
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
const undeliveredAcceptanceLatest = `
		WITH latest AS (
			SELECT DISTINCT ON (o.at_uri)
			       o.at_uri            AS post_uri,
			       o.community_did     AS community_did,
			       d.activity_id       AS activity_id,
			       d.state             AS delivery_state,
			       d.last_error_class  AS last_error_class,
			       d.created_at        AS delivery_created_at
			  FROM admissions adm
			  JOIN outbound_objects o
			    ON o.at_uri = adm.post_uri
			   AND o.community_did = adm.community_did
			  LEFT JOIN outbound_activities a
			         ON a.payload -> 'object' ->> 'id' = o.ap_object_id
			  LEFT JOIN outbound_deliveries d ON d.activity_id = a.activity_id
			 WHERE adm.status = 'accepted'
			   AND o.accepted_at IS NULL
			 ORDER BY o.at_uri, d.seq DESC NULLS LAST
		)`

// undeliveredAcceptanceFilter selects the divergent rows out of that CTE.
//
// A HELD SETTLEMENT IS NOT A DIVERGENCE, and it is the one case this report
// would otherwise cry wolf on. It is pending, old, and unstamped — identical
// to a stuck delivery on every column above except this one — and it means
// the opposite: the peer ALREADY ACCEPTED the activity, and accepted_at is
// missing precisely because writing it is the step that failed. The worker
// resumes it on its next claim. Reporting it fires the sweep on every
// settlement retry, and an operator who learns to ignore this report also
// ignores the cancelled acceptance beside it, which is the finding that
// never heals itself.
const undeliveredAcceptanceFilter = `
		 WHERE delivery_state IN ($1, $2)
		    OR (delivery_state = $3
		        AND last_error_class <> $4
		        AND delivery_created_at < now() - $5::interval)`

// undeliveredAcceptanceArgs binds that filter. One helper, so the list and the
// count cannot bind $1..$5 in two different orders — which would leave two
// queries that both run and disagree.
func undeliveredAcceptanceArgs(staleAfter time.Duration) []any {
	return []any{
		string(DeliveryStateCancelled), string(DeliveryStatePoisoned),
		string(DeliveryStatePending), DeliveryHeldForSettlement,
		fmt.Sprintf("%d seconds", int(staleAfter.Seconds())),
	}
}

func (r *postgresDivergences) UndeliveredAcceptances(ctx context.Context, staleAfter time.Duration) ([]UndeliveredAcceptance, error) {
	// The comparison itself is undeliveredAcceptanceLatest +
	// undeliveredAcceptanceFilter, above: this adds the columns an operator
	// reads, a stable order, and the example budget.
	query := undeliveredAcceptanceLatest + `
		SELECT post_uri,
		       community_did,
		       COALESCE(activity_id, ''),
		       COALESCE(delivery_state, ''),
		       COALESCE(last_error_class, '')
		  FROM latest` + undeliveredAcceptanceFilter + `
		 ORDER BY post_uri
		 LIMIT $6`

	tx, done, err := r.readOnlyTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("list undelivered acceptances: %w", err)
	}
	defer done()

	rows, err := tx.QueryContext(ctx, query,
		append(undeliveredAcceptanceArgs(staleAfter), MaxDivergenceExamples)...)
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

// UndeliveredAcceptanceCounts counts the same population, grouped by the
// delivery state the sweep classifies on.
//
// GROUPED RATHER THAN TOTALLED, because the three classes are three different
// jobs — a cancelled acceptance is a note, a stopped queue is a page — and a
// single total could only ever be alerted on at the noise level of whichever
// kind is most common. Grouping here also keeps the state-to-class mapping in
// the one place that already owns it (ingest.acceptanceClass): a state this
// sweep has no class for arrives as its own key rather than inflating a class
// that would misdescribe it.
func (r *postgresDivergences) UndeliveredAcceptanceCounts(ctx context.Context, staleAfter time.Duration) (map[DeliveryState]int, error) {
	query := undeliveredAcceptanceLatest + `
		SELECT COALESCE(delivery_state, ''), count(*)
		  FROM latest` + undeliveredAcceptanceFilter + `
		 GROUP BY 1`

	tx, done, err := r.readOnlyTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("count undelivered acceptances: %w", err)
	}
	defer done()

	rows, err := tx.QueryContext(ctx, query, undeliveredAcceptanceArgs(staleAfter)...)
	if err != nil {
		return nil, fmt.Errorf("count undelivered acceptances: %w", err)
	}
	defer func() { _ = rows.Close() }()

	counts := make(map[DeliveryState]int)
	for rows.Next() {
		var state string
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			return nil, fmt.Errorf("scan undelivered acceptance count: %w", err)
		}
		counts[DeliveryState(state)] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("count undelivered acceptances: %w", err)
	}
	return counts, nil
}

// recastDivergenceRows is the comparison, shared by the example list and the
// count so the two cannot come to describe different populations.
//
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
// the row has moved to a new activity id, been retracted, or been deleted —
// every shape this bug takes — the row cannot answer, and the history stands
// on its own.
//
// THREE INDEPENDENT EXCLUSIONS, because they answer different questions and
// each is the whole defence against a different way of ruining this report:
//
//	the ledger still claims it — without this, EVERY cleanly delivered vote
//	  is a finding. That is most of the highest-volume table in the system,
//	  and a class that names all of it hides the one case that matters.
//	a delivered Undo followed it — without this, a withdrawal that WORKED is
//	  reported forever. The history is append-only, so the delivered
//	  Like/Dislike never goes away; only the Undo beside it says the peer
//	  holds nothing now.
//	a LATER DELIVERED VOTE followed it — without this, every successful vote
//	  FLIP is a finding, forever. A flip is an in-place upsert
//	  (consume.applyVoteWrite): current_activity_id moves to the new
//	  activity, and NO Undo is enqueued, because Lemmy holds one vote per
//	  (person, object) and REPLACES it on a bare opposite vote. So once the
//	  new vote delivers, the old delivered activity satisfies neither
//	  exclusion above — the ledger names the new id and no Undo will ever
//	  join it — and the append-only history keeps it forever. A later
//	  delivered Like/Dislike for the same pair supersedes an earlier one
//	  EXACTLY as a delivered Undo does, and that is the only reason this is
//	  correct rather than merely convenient.
//
// Both time exclusions compare by TIME rather than by id on purpose: an Undo
// names the activity it withdraws in its payload, but a re-cast mints new
// ids, so id-chasing would miss an Undo that withdrew an earlier incarnation
// of the same (actor, subject) vote. Only one vote per pair may be live at a
// time, which is what makes "any delivered Undo at or after this delivery"
// the right question.
//
// The superseding-vote comparison is a TUPLE, (created_at, activity_id),
// because created_at cannot be trusted to separate them: it defaults to
// now(), which is the TRANSACTION timestamp, so any two vote activities
// written in one transaction carry it identically, and across transactions
// the column is only microsecond-resolution. A bare `>` would then exclude neither
// from the other and report BOTH; a bare `>=` would exclude both and report
// NEITHER, silently swallowing the poisoned re-cast this class exists for.
// The tuple gives a total order regardless of clock resolution, and the
// activity id is the tiebreak because it is the primary key — unique by
// construction, so the order is total and stable across sweeps.
//
// IT MUST NOT WEAKEN THE TRUE POSITIVE, and it does not: the later vote must
// itself be DELIVERED. A re-cast whose new delivery poisoned has no later
// delivered vote, so the old activity the peer is still counting is still
// reported — which is the entire point of the class.
//
// DISTINCT because one activity may have several deliveries (the fan-out
// schema); one delivered copy is one thing the peer holds.
const recastDivergenceRows = `
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
		   AND NOT EXISTS (
		         SELECT 1
		           FROM outbound_activities n
		           JOIN outbound_deliveries nd
		             ON nd.activity_id = n.activity_id AND nd.state = $1
		          WHERE n.kind IN ($2, $3)
		            AND n.actor_did = a.actor_did
		            AND n.parent_at_uri = a.parent_at_uri
		            AND (n.created_at, n.activity_id) > (a.created_at, a.activity_id))`

// recastDivergenceArgs binds that comparison's $1..$5. One helper, so the list
// and the count cannot bind them in two different orders.
func recastDivergenceArgs() []any {
	return []any{
		string(DeliveryStateDelivered), "Like", "Dislike",
		string(DeliveredStateDelivered), "Undo",
	}
}

func (r *postgresDivergences) RecastDivergences(ctx context.Context) ([]RecastDivergence, error) {
	// The comparison is recastDivergenceRows, above; this adds the operator's
	// stable order and the example budget.
	query := recastDivergenceRows + `
		 ORDER BY a.actor_did, a.parent_at_uri, a.activity_id
		 LIMIT $6`

	tx, done, err := r.readOnlyTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("list recast divergences: %w", err)
	}
	defer done()

	rows, err := tx.QueryContext(ctx, query, append(recastDivergenceArgs(), MaxDivergenceExamples)...)
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

// RecastDivergenceCount counts the same comparison, unbounded.
//
// It wraps the DISTINCT select rather than counting the join, because the
// DISTINCT is part of the definition: one activity may have several deliveries
// (the fan-out schema), and one delivered copy is ONE thing the peer holds.
// count(*) over the un-deduplicated join would report a number larger than the
// list it labels, on the exact rows an operator is trying to size.
func (r *postgresDivergences) RecastDivergenceCount(ctx context.Context) (int, error) {
	query := `SELECT count(*) FROM (` + recastDivergenceRows + `) AS divergent`
	tx, done, err := r.readOnlyTx(ctx)
	if err != nil {
		return 0, fmt.Errorf("count recast divergences: %w", err)
	}
	defer done()

	var total int
	if err := tx.QueryRowContext(ctx, query, recastDivergenceArgs()...).Scan(&total); err != nil {
		return 0, fmt.Errorf("count recast divergences: %w", err)
	}
	return total, nil
}

// The poison classes the worker decides BEFORE any POST is attempted, declared
// HERE and written by internal/outbound/worker.go through these names.
//
// THE DECLARATION IS SHARED SO THE TWO SIDES CANNOT DRIFT IN SPELLING. The
// worker used to write these as string literals at its own call sites while the
// denylist below repeated them, and nothing compiled the two lists against each
// other: a respelling on either side would have silently moved the known
// non-deliveries into the unknown-outcome report, whose entire worth is that its
// numbers stay small enough to trust. The compiler now refuses that. What it
// still cannot catch is a FURTHER never-wire class introduced as a fresh literal
// — the worker's wire classes (transport, 4xx, 5xx …) are literals, so the
// surrounding style invites one — which is why a new class belongs in this block
// and in neverReachedTheWireClasses, and why the store test that seeds them all
// by name (divergence_unknown_test.go) is the pin on the set.
const (
	// PoisonClassParentUnaccepted: the causal wait budget expired and the parent
	// was never accepted, so the reply was never offered to anyone.
	PoisonClassParentUnaccepted = "parent_unaccepted"
	// PoisonClassParentPoisoned: the parent delivery poisoned; a descendant
	// cannot land, so it is not attempted.
	PoisonClassParentPoisoned = "parent_poisoned"
	// PoisonClassParentCancelled: every delivery of the parent was CANCELLED —
	// a consent recheck, an operator cancel, an actor or community sweep — so
	// the parent is terminal, was never accepted, and nothing is left that could
	// make it land. The descendant is decided here rather than left waiting out
	// the causal budget against a parent that is never coming.
	PoisonClassParentCancelled = "parent_cancelled"
	// PoisonClassCrossAuthority: the stored target inbox is not same-authority
	// with its community, and the worker refuses to sign a POST to it.
	PoisonClassCrossAuthority = "cross_authority"
	// PoisonClassSigner: SignerFor could not resolve a signing key, so
	// SendActivityAs is never called. This one poisons only after the attempt
	// budget is exhausted (releaseOrPoison), which makes it LOOK like a retried
	// wire failure on every column; it is not one.
	PoisonClassSigner = "signer"
	// PoisonClassKEKMisconfigured: the actor's signing key is well-formed
	// ciphertext that opened under NO configured KEK — the material at rest was
	// sealed under a different BRIDGE_KEK than this process holds. Split out of
	// PoisonClassSigner because the two need OPPOSITE handling and send an
	// operator to opposite places: a missing actor row resolves itself and is
	// retried, while a wrong KEK gives the same answer on every attempt and
	// poisons on the FIRST one (worker.go says why that is safe mid-rotation).
	// Under the generic signer class this arrives as a retried wire-ish failure
	// whose excerpt reads "message authentication failed", which looks like data
	// corruption; the class is the only place the word KEK appears in the
	// dead-letter table.
	PoisonClassKEKMisconfigured = "kek_misconfigured"
)

// neverReachedTheWireClasses are those classes as the divergence sweep
// reads them.
//
// All of them are written with last_status_code 0 — the same shape a dial timeout
// leaves behind, and the opposite meaning. Nothing was sent, so the peer's
// state is not unknown at all: they do not have it. That is the rule that
// already excludes a cancelled delivery, one step later in the worker.
//
// A DENYLIST RATHER THAN AN ALLOWLIST, deliberately. Every other poison class
// (transport, 4xx, 5xx, unauthorized, timeout, rate_limited, transient,
// inbox_gone, inbox_resolve) is recorded after a POST was attempted, so
// "unknown" is the honest default for a class this list has not heard of: an
// allowlist would silently drop a genuinely unknown delivery out of the report
// the day the worker grows a new wire class, and a divergence that vanishes is
// worse than one that is over-reported with its class name attached. The cost
// runs the other way — a new never-wire poison class must be added to the block
// above AND to this list, or it inflates these counts.
var neverReachedTheWireClasses = []string{
	PoisonClassParentUnaccepted, PoisonClassParentPoisoned, PoisonClassParentCancelled,
	PoisonClassCrossAuthority, PoisonClassSigner, PoisonClassKEKMisconfigured,
}

// unknownDeliveryOutcomeRows is the population, shared by the example list and
// the counts.
//
// POISONED ONLY, and every other state is excluded for a reason that is not
// symmetry:
//
//	delivered — the peer confirmed it. The one outcome here we DO know.
//	cancelled — it never reached the wire: a ban or an opt-out took it out of
//	  the queue, so the peer does not have it and that is a fact, not a
//	  question. Cycle 2's cancelled class already names it. Folding it in
//	  would put a KNOWN non-delivery into the bucket whose entire meaning is
//	  that the answer is unavailable, and these are the two numbers an
//	  operator has to be able to trust as small.
//	pending — still in flight, or held for settlement. Nothing is unresolved
//	  about a delivery that has not finished trying.
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
//
// AND IT MUST HAVE BEEN SENT. Several poison classes are decided before any
// POST — see neverReachedTheWireClasses — and they carry status 0 exactly
// like a transport failure does. Filing them here would put a KNOWN
// non-delivery in the bucket whose whole meaning is that the answer is
// unavailable, inflate the one number whose worth depends on staying small,
// and send an operator to ask a stranger about a request that never left
// this process. last_error_class is NOT NULL DEFAULT ” (migration 020), so
// `<> ALL` cannot go NULL and quietly drop a row.
const unknownDeliveryOutcomeRows = `
		  FROM outbound_deliveries d
		  JOIN outbound_activities a ON a.activity_id = d.activity_id
		 WHERE d.state = $1
		   AND d.last_error_class <> ALL($2)`

// unknownDeliveryRefused is the refused/unanswered discriminator, written once
// so the list and the counts cannot disagree about which sub-count a row
// belongs to. NULL and 0 are both silence — see above.
const unknownDeliveryRefused = `COALESCE(d.last_status_code, 0) > 0`

// unknownDeliveryOutcomeArgs binds $1..$2 for both statements.
func unknownDeliveryOutcomeArgs() []any {
	return []any{string(DeliveryStatePoisoned), pq.Array(neverReachedTheWireClasses)}
}

func (r *postgresDivergences) UnknownDeliveryOutcomes(ctx context.Context) ([]UnknownDeliveryOutcome, error) {
	query := `
		SELECT d.activity_id,
		       d.target_inbox,
		       a.kind,
		       d.last_error_class,
		       COALESCE(d.last_status_code, 0),
		       ` + unknownDeliveryRefused + unknownDeliveryOutcomeRows + `
		 ORDER BY d.activity_id, d.target_inbox
		 LIMIT $3`

	tx, done, err := r.readOnlyTx(ctx)
	if err != nil {
		return nil, fmt.Errorf("list unknown delivery outcomes: %w", err)
	}
	defer done()

	rows, err := tx.QueryContext(ctx, query,
		append(unknownDeliveryOutcomeArgs(), MaxDivergenceExamples)...)
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

// UnknownDeliveryOutcomeCounts counts the same population, split by whether a
// peer answered.
//
// The split is made HERE, by the same expression the list selects Refused with,
// rather than by counting twice with two predicates: two spellings of "the peer
// spoke" is exactly how a dial timeout ends up in the sub-count that says it
// did. GROUP BY over that one boolean cannot produce a third bucket, and a
// missing group is an honest zero — no rows of that kind exist.
func (r *postgresDivergences) UnknownDeliveryOutcomeCounts(ctx context.Context) (UnknownDeliveryCounts, error) {
	query := `SELECT ` + unknownDeliveryRefused + `, count(*)` + unknownDeliveryOutcomeRows + `
		 GROUP BY 1`

	tx, done, err := r.readOnlyTx(ctx)
	if err != nil {
		return UnknownDeliveryCounts{}, fmt.Errorf("count unknown delivery outcomes: %w", err)
	}
	defer done()

	rows, err := tx.QueryContext(ctx, query, unknownDeliveryOutcomeArgs()...)
	if err != nil {
		return UnknownDeliveryCounts{}, fmt.Errorf("count unknown delivery outcomes: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var counts UnknownDeliveryCounts
	for rows.Next() {
		var refused bool
		var count int
		if err := rows.Scan(&refused, &count); err != nil {
			return UnknownDeliveryCounts{}, fmt.Errorf("scan unknown delivery outcome count: %w", err)
		}
		if refused {
			counts.Refused = count
			continue
		}
		counts.Unanswered = count
	}
	if err := rows.Err(); err != nil {
		return UnknownDeliveryCounts{}, fmt.Errorf("count unknown delivery outcomes: %w", err)
	}
	return counts, nil
}
