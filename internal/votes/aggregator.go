// Package votes is the bridge-side vote aggregation store (task 07, PLAN.md
// locked decision 7): Lemmy Like/Dislike activities never become records —
// they maintain per-subject aggregate counts served over one sanctioned
// side-channel XRPC, social.coves.bridge.getVoteAggregates.
//
// The counting model: AP vote delivery is a stream of per-voter state
// changes, not increments. Voters flip votes (Like → Undo{Like} → Dislike),
// instances re-deliver activities, and Undos arrive for votes the bridge
// never saw. vote_events therefore keeps at most one live (non-undone) row
// per (voter, subject) — the voter's current vote — with every activity id
// recorded for dedupe, and vote_aggregates holds the served totals,
// recomputed from the live events inside the same transaction. The aggregate
// row doubles as the per-subject write lock: every mutation locks it first,
// so concurrent workers can never interleave a stale recompute.
package votes

import (
	"context"
	"database/sql"
	stderrors "errors"
	"expvar"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"tidepool/internal/ap"
	"tidepool/internal/echo"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/ratelimit"
	"tidepool/internal/store"
)

// voteWarnInterval throttles each of the aggregator's rare-but-loud notices: a
// suppressed voter, and a clamped seed baseline. Both are rare by construction,
// so a sampler costs nothing in steady state — but each announces itself at
// volume in exactly the failure it exists to expose (a probe that has started
// matching GENUINE voters; an origin whose totals no longer contain the votes
// we wrote back), and one line per vote or per backfilled post would bury it.
//
// The interval is shared; the SAMPLERS are not. Their causes are correlated —
// switching write-back on produces echoed votes AND clamped baselines — so one
// sampler would let each signal suppress the other in every window of precisely
// the incident both lines exist to describe.
const voteWarnInterval = time.Second

// SeedBaselineClamped counts seeds whose computed baseline came out NEGATIVE
// and was clamped to zero: the origin's total for that DIRECTION was smaller
// than the votes we can already account for on that subject.
//
// Read it for what it is — a floor breach, not a discard detector. It can only
// fire where api_total < live + ours, i.e. on subjects whose entire fediverse
// tally is smaller than the deficit; on any post with a real score, an origin
// silently discarding the votes we write back (a restrictive Lemmy
// FederationMode, decision 16's named risk) understates the served tally with
// the raw baseline still comfortably positive, and this counter stays at zero.
// SeedOursSubtracted is the signal that covers those subjects.
//
// The "tidepool_" prefix is load-bearing: the admin metrics surface serves ONLY
// that prefix, so a counter named without it is published to expvar and then
// filtered straight back out — indistinguishable from a counter that never
// fires.
var SeedBaselineClamped = expvar.NewInt("tidepool_vote_seed_baseline_clamped")

// SeedOursSubtracted totals the delivered outbound votes netted out of seeded
// baselines, across directions and subjects.
//
// It is the volume half of the clamp's signal, and unlike the clamp it advances
// on ordinary healthy subjects: it says how many votes the bridge BELIEVES the
// origin is holding for our personas. Compared against a Lemmy-side sample of
// the same posts, a persistent gap is a discard being absorbed silently —
// which the clamp only ever catches on near-zero-score subjects.
var SeedOursSubtracted = expvar.NewInt("tidepool_vote_seed_ours_subtracted")

// Vote directions (vote_events.direction).
const (
	directionUp   = "up"
	directionDown = "down"
)

// MaxSeededCount is the upper sanity cap on one seeded baseline value
// (task 11). Seeds come from a REMOTE instance's public API — a hostile or
// broken origin must not be able to inject absurd baselines into served
// scores. The largest scores on the biggest Lemmy instances are low five
// figures; one million is comfortably above anything real while making a
// deliberately poisoned 2^31 baseline a validation error the seeder logs
// and drops (the previous baseline survives).
const MaxSeededCount = 1_000_000

// RecordReader is the slice of *repo.Manager the aggregator hands to
// materialize.CommunityDIDOf: the reader it uses to derive a subject's
// community when the mapping's community_did is unset (rows written before
// migration 016), by reading the postv2's `community` field or walking a
// comment's reply.root to its thread root. A mapping that carries the column
// needs no read at all.
type RecordReader interface {
	GetRecord(ctx context.Context, did, collection, rkey string) (record map[string]any, recordCID string, err error)
}

// VoterProbe answers whether a voter is one of the bridge's OWN minted
// personas (ap_actors) rather than a genuine remote human. *echo.Classifier
// satisfies it: the actor route's lookup is exactly this question, including
// the vanity-origin rule and the fail-safe error direction.
//
// This guard is NOT redundant with ingest's envelope classifier. That one asks
// an ENVELOPE question at the dispatch boundary ("is this announced traffic
// ours?"); this one asks a VOTER question at the MUTATION site, and so also
// covers callers that never pass through handleAnnounce at all — the bare
// /ap/inbox vote branch, the community-outbox backfill, and the seed paths.
type VoterProbe interface {
	Identify(ctx context.Context, apID string) (echo.Identity, error)
}

// Aggregator implements ingest.VoteAggregator over the vote_aggregates /
// vote_events tables. It owns its SQL (multi-statement transactions across
// both tables, like internal/repo) and uses store.APObjects only to resolve
// the voted-on subject to its materialized record. Communities and records
// bind announced votes to the announcing community (a followed community may
// only vouch for votes on its OWN content, never inflate or deflate another
// community's scores).
type Aggregator struct {
	db          *sql.DB
	objects     store.APObjects
	communities store.Communities
	records     RecordReader
	voters      VoterProbe
	echoLog     *ratelimit.Sampler
	clampLog    *ratelimit.Sampler
	logger      *slog.Logger
}

// NewAggregator validates dependencies and builds an Aggregator.
func NewAggregator(db *sql.DB, objects store.APObjects, communities store.Communities, records RecordReader, voters VoterProbe, logger *slog.Logger) (*Aggregator, error) {
	if db == nil {
		return nil, errors.NewValidationError("db", "must not be nil")
	}
	if objects == nil {
		return nil, errors.NewValidationError("objects", "must not be nil")
	}
	if communities == nil {
		return nil, errors.NewValidationError("communities", "must not be nil")
	}
	if records == nil {
		return nil, errors.NewValidationError("records", "must not be nil")
	}
	// REQUIRED, not optional. A nil probe is an aggregator that counts our own
	// votes as inbound ones — silently, and only in whichever binary forgot to
	// pass it, which is exactly how this guard was left out of production while
	// every test had it. There is no caller for whom "no echo suppression" is
	// the right behaviour, so it is not expressible.
	if voters == nil {
		return nil, errors.NewValidationError("voters", "must not be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Aggregator{db: db, objects: objects, communities: communities, records: records,
		voters:   voters,
		echoLog:  ratelimit.NewSampler(voteWarnInterval),
		clampLog: ratelimit.NewSampler(voteWarnInterval),
		logger:   logger}, nil
}

// ApplyVote records one Like or Dislike: insert the activity (duplicate
// activity id → no-op), supersede the voter's previous vote on the subject,
// and recompute the aggregate — all in one transaction. Votes on subjects
// the bridge never materialized (or whose mapping is soft-deleted) are
// dropped and logged at debug: the v1 decision is no pending bucket, and
// vote volume makes anything louder than debug unusable. Malformed votes
// (no activity id, voter, or subject) are dropped the same way — there is
// nothing to retry and poisoning the ordering key over a vote helps nobody.
func (a *Aggregator) ApplyVote(ctx context.Context, vote *ap.Object, communityIRI string) error {
	if vote == nil {
		return nil
	}
	direction, ok := directionFor(vote.Type)
	if !ok {
		a.logger.Debug("vote dropped: not a Like/Dislike", "type", vote.Type, "activity", vote.ID)
		return nil
	}
	voter, subject := refID(vote.Actor), refID(vote.Object)
	if vote.ID == "" || voter == "" || subject == "" {
		a.logger.Debug("vote dropped: missing activity id, voter, or subject",
			"activity", vote.ID, "voter", voter, "subject", subject, "community", communityIRI)
		return nil
	}

	// The voter probe runs BEFORE anything is read or locked for this subject:
	// a suppressed vote must leave no trace at all, and lockAggregate would
	// mint a 0/0 vote_aggregates row that the XRPC contract reads as "this
	// subject has been voted on". Outside inTx by construction, so it adds no
	// lock-ordering hazard to the transaction below.
	ours, err := a.isOurPersona(ctx, voter)
	if err != nil {
		return err
	}
	if ours {
		a.dropEchoedVote("vote", vote, voter, subject, communityIRI)
		return nil
	}

	mapping, err := a.subjectMapping(ctx, subject)
	if err != nil {
		return err
	}
	if mapping == nil {
		a.logger.Debug("vote dropped: subject not bridged",
			"subject", subject, "activity", vote.ID, "community", communityIRI)
		return nil
	}
	if communityIRI != "" {
		ok, err := a.subjectBelongsToCommunity(ctx, mapping, communityIRI)
		if err != nil {
			return err
		}
		if !ok {
			a.logger.Debug("vote dropped: subject not in announcing community",
				"subject", subject, "activity", vote.ID, "community", communityIRI)
			return nil
		}
	}

	return a.inTx(ctx, func(tx *sql.Tx) error {
		// Dedupe BEFORE creating the aggregate row: a duplicate activity id
		// aimed at a DIFFERENT subject must not mint a spurious 0/0 aggregate
		// for a never-voted subject (the XRPC contract omits never-voted
		// uris). A plain SELECT before the lock has no lock-ordering hazard;
		// the race with a concurrent first delivery of the same id is closed
		// by the unique constraint on the insert below.
		var seen bool
		if err := tx.QueryRowContext(ctx, `
			SELECT EXISTS (SELECT 1 FROM vote_events WHERE activity_id = $1)`,
			vote.ID).Scan(&seen); err != nil {
			return fmt.Errorf("dedupe vote event %q: %w", vote.ID, err)
		}
		if seen {
			a.logger.Debug("vote dropped: duplicate activity id", "activity", vote.ID)
			return nil
		}

		if err := lockAggregate(ctx, tx, subject, mapping.ATURI); err != nil {
			return err
		}

		// Dedupe backstop: the activity id is unique forever. A concurrent
		// first delivery that slipped past the probe above changes nothing.
		result, err := tx.ExecContext(ctx, `
			INSERT INTO vote_events (activity_id, voter_ap_id, subject_ap_id, direction)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (activity_id) DO NOTHING`,
			vote.ID, voter, subject, direction)
		if err != nil {
			return fmt.Errorf("insert vote event %q: %w", vote.ID, err)
		}
		inserted, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("insert vote event %q: rows affected: %w", vote.ID, err)
		}
		if inserted == 0 {
			a.logger.Debug("vote dropped: duplicate activity id", "activity", vote.ID)
			return nil
		}

		// Supersede: a voter has one current vote per subject. The previous
		// live vote (same or opposite direction — re-like and flip both land
		// here) is marked undone; only the new event stays live.
		if _, err := tx.ExecContext(ctx, `
			UPDATE vote_events
			SET undone = TRUE
			WHERE subject_ap_id = $1 AND voter_ap_id = $2 AND NOT undone AND activity_id <> $3`,
			subject, voter, vote.ID); err != nil {
			return fmt.Errorf("supersede votes by %q on %q: %w", voter, subject, err)
		}

		return recomputeAggregate(ctx, tx, subject)
	})
}

// RetractVote undoes a previously applied vote (Undo{Like|Dislike}): the
// voter's live vote on the subject is marked undone and the aggregate
// recomputed.
//
// What Lemmy actually federates (measured by the e2e suite against a real
// Lemmy 0.19, and NOT what task 07 originally assumed):
//
//   - flip (up → down): a bare Dislike, no Undo at all — ApplyVote's
//     supersede handles it;
//   - clear (score 0): Announce{Undo{Like}} whose inner vote is
//     RECONSTRUCTED — a freshly generated activity id, and typed "Like"
//     even when the voter's live vote is a Dislike.
//
// So the inner vote's id and type are hints, not truth. The retraction
// runs in two steps: first an id-targeted update (correct for
// implementations that inline the original activity, and what keeps a
// REPLAYED Undo{Like id=A} after a re-like id=B from retracting B); if that
// matches nothing and the id was never seen at all (Lemmy's regenerated
// id, or no id), fall back to retracting the voter's current live vote on
// the subject regardless of direction — Undo means "remove my vote". The
// (voter, subject) scoping plus the announced-undo community binding keep
// forged undos harmless.
//
// Everything that cannot be acted on is a logged no-op, never an error: a
// nil or bare-IRI vote.Object/vote.Actor, an undo for a voter with no live
// vote (out-of-order delivery, or history that only exists as a seeded
// baseline), a replayed undo naming an already-superseded activity, and an
// announced undo whose subject does not belong to the announcing community.
func (a *Aggregator) RetractVote(ctx context.Context, vote *ap.Object, communityIRI string) error {
	if vote == nil {
		return nil
	}
	direction, ok := directionFor(vote.Type)
	if !ok {
		a.logger.Debug("vote retraction dropped: not a Like/Dislike", "type", vote.Type, "activity", vote.ID)
		return nil
	}
	voter, subject := refID(vote.Actor), refID(vote.Object)
	if voter == "" || subject == "" {
		a.logger.Debug("vote retraction dropped: missing voter or subject",
			"activity", vote.ID, "voter", voter, "subject", subject, "community", communityIRI)
		return nil
	}

	// The SAME probe on the retraction path, and for a sharper reason than
	// symmetry: step 2 below retracts the voter's live vote regardless of
	// activity id, so an unsuppressed echo of our own Undo would retract a row
	// this very guard stopped us from writing — a phantom retraction driven by
	// whatever that persona's history happens to hold.
	ours, err := a.isOurPersona(ctx, voter)
	if err != nil {
		return err
	}
	if ours {
		a.dropEchoedVote("vote retraction", vote, voter, subject, communityIRI)
		return nil
	}

	if communityIRI != "" {
		mapping, err := a.subjectMapping(ctx, subject)
		if err != nil {
			return err
		}
		if mapping == nil {
			a.logger.Debug("vote retraction dropped: subject not bridged",
				"subject", subject, "activity", vote.ID, "community", communityIRI)
			return nil
		}
		ok, err := a.subjectBelongsToCommunity(ctx, mapping, communityIRI)
		if err != nil {
			return err
		}
		if !ok {
			a.logger.Debug("vote retraction dropped: subject not in announcing community",
				"subject", subject, "activity", vote.ID, "community", communityIRI)
			return nil
		}
	}

	return a.inTx(ctx, func(tx *sql.Tx) error {
		// The aggregate row is the per-subject lock; no row means no vote was
		// ever counted for this subject, so there is nothing to retract.
		var locked int
		err := tx.QueryRowContext(ctx, `
			SELECT 1 FROM vote_aggregates WHERE subject_ap_id = $1 FOR UPDATE`,
			subject).Scan(&locked)
		if stderrors.Is(err, sql.ErrNoRows) {
			a.logger.Debug("vote retraction dropped: no aggregate for subject",
				"subject", subject, "voter", voter)
			return nil
		}
		if err != nil {
			return fmt.Errorf("lock vote aggregate for %q: %w", subject, err)
		}

		// Step 1: target the undone activity by its own id (correct for
		// implementations that inline the original vote). The voter/subject
		// predicates stay as defense — a forged undo naming someone else's
		// activity id retracts nothing. No direction predicate: the id is
		// the authoritative target and the inner type is unreliable.
		var retracted int64
		if vote.ID != "" {
			result, err := tx.ExecContext(ctx, `
				UPDATE vote_events
				SET undone = TRUE
				WHERE subject_ap_id = $1 AND voter_ap_id = $2 AND NOT undone AND activity_id = $3`,
				subject, voter, vote.ID)
			if err != nil {
				return fmt.Errorf("retract vote %q by %q on %q: %w", vote.ID, voter, subject, err)
			}
			if retracted, err = result.RowsAffected(); err != nil {
				return fmt.Errorf("retract vote %q by %q on %q: rows affected: %w", vote.ID, voter, subject, err)
			}
			if retracted == 0 {
				// Zero rows means either a REPLAY (the id is known but its
				// vote was already undone/superseded — must retract nothing,
				// above all not a newer re-vote by the same voter) or a
				// Lemmy-style RECONSTRUCTED inner vote (fresh id the bridge
				// has never seen). Distinguish by whether the id exists.
				var known bool
				if err := tx.QueryRowContext(ctx, `
					SELECT EXISTS (SELECT 1 FROM vote_events WHERE activity_id = $1)`,
					vote.ID).Scan(&known); err != nil {
					return fmt.Errorf("probe undone activity %q: %w", vote.ID, err)
				}
				if known {
					a.logger.Debug("vote retraction dropped: replayed undo of a superseded vote",
						"subject", subject, "voter", voter, "activity", vote.ID)
					return nil
				}
			}
		}

		// Step 2: unknown or absent id — "remove my vote". Retract the
		// voter's current live vote on the subject regardless of direction
		// (the reconstructed inner vote is typed Like even when the live
		// vote is a Dislike).
		if retracted == 0 {
			result, err := tx.ExecContext(ctx, `
				UPDATE vote_events
				SET undone = TRUE
				WHERE subject_ap_id = $1 AND voter_ap_id = $2 AND NOT undone`,
				subject, voter)
			if err != nil {
				return fmt.Errorf("retract vote by %q on %q: %w", voter, subject, err)
			}
			if retracted, err = result.RowsAffected(); err != nil {
				return fmt.Errorf("retract vote by %q on %q: rows affected: %w", voter, subject, err)
			}
		}
		if retracted == 0 {
			a.logger.Debug("vote retraction dropped: no live vote to retract",
				"subject", subject, "voter", voter, "direction", direction)
			return nil
		}
		return recomputeAggregate(ctx, tx, subject)
	})
}

// SeedAggregates imports a baseline (upvotes, downvotes) for a bridged
// subject from its origin's public API — history whose individual Like
// activities the bridge never saw (Lemmy outboxes announce historical votes
// only sparsely). Live vote_events stack on top of the baseline.
//
// WHAT THE SERVED AGGREGATE MEANS (task 17b). The Coves appview (a separate
// repo) keeps native and bridged tallies in SEPARATE columns on its post
// record — bridged_upvote_count beside the native count — so what Tidepool
// serves is the FEDIVERSE-ONLY tally:
//
//	served(subject) = api_total(subject) − { our personas' votes Lemmy currently holds }
//
// The origin's counts are a TOTAL, and two populations inside it are already
// accounted for elsewhere:
//
//   - votes that ALSO federated live and sit in vote_events as live rows (any
//     vote cast after the community was subscribed). Keeping them counts those
//     voters twice — once in the baseline, once in the recompute's live term;
//   - votes TIDEPOOL ITSELF wrote back for native users, which Lemmy is holding
//     and reporting. Coves counts those in its NATIVE column, so keeping them
//     counts one person's single vote twice across the two columns in the UI.
//
// So the baseline is stored NET of both, per direction, clamped at zero. See
// reportSeed for what the seed publishes about that: the volume it subtracted,
// and — on the rare subject whose whole tally is smaller than the deficit — the
// clamp that would otherwise absorb the difference in silence.
//
// The subtraction lives in the BASELINE rather than the served columns because
// recomputeAggregate rewrites the served columns on every single inbound vote:
// a correction applied there would be undone minutes later by the next voter,
// with nothing connecting the drift back to the seed.
//
// A re-seed (backfill redo) is also the drift healer: a voter counted only in
// the baseline who later flips federates a bare Dislike (Lemmy sends no Undo on
// flips), leaving the retired upvote in the baseline next to the new live
// downvote — until the next re-seed converges the served totals back. But
// nothing re-seeds PERIODICALLY: the one caller is ingest's Backfill.seedCounts
// on its post walk, behind SEED_COUNTS_FROM_API, and an un-forced trigger skips
// the whole run inside the freshness window — so for a quiet community "heals
// on the next re-seed" can mean "heals when an admin forces a backfill", and
// may mean never.
//
// Three residual races span the origin API fetch and this transaction. The
// first two are transient and self-healing on the next re-seed, with the caveat
// above (the pre-fix over-count race was PERMANENT and compounding); the third
// heals on the FLIP'S DELIVERY rather than on a re-seed, so re-seeding inside
// its window reproduces it rather than converging it:
//   - under-count by one: a vote federates AFTER the fetch but is live here, so
//     it is net-subtracted from the baseline yet not present in the fetched
//     total;
//   - over-count by one (the mirror): a vote already IN the fetched total whose
//     federated activity arrives AFTER this seed tx — the net-of-live
//     subtraction cannot yet see it as a live row, so the baseline keeps it AND
//     the later live event adds it again, until the next re-seed reconciles;
//   - direction incoherence during a re-cast window, outbound side: a native
//     user RE-CASTS a vote they had already delivered. The CLOBBER this bullet
//     used to describe is closed — OutboundVotes.Upsert now keeps 'delivered'
//     through a flip, so the row stays in the `ours` term instead of dropping
//     out of it entirely, and the vote is no longer invisible to this seed.
//     What remains is narrower and is a DIFFERENT error, not the same one:
//     the row's direction is already the NEW one while the peer still holds
//     the OLD, so the subtraction lands on the wrong side of the tally — the
//     direction the peer holds is not subtracted, and the direction it does
//     not hold is. It heals when the flip delivers or the vote is undone — and
//     if the flip's delivery POISONS, neither event ever comes: the wrong-side
//     subtraction then recurs on every re-seed until an undo, and the standing
//     divergence is RecastDivergence's finding. It is signalled while it lasts
//     ONLY when the mis-subtraction breaches the zero floor (see the ours.*
//     binding in SeedAggregates: SeedBaselineClamped fires on the breach, and
//     unrelated votes in the same direction can absorb the error silently —
//     the counters witness the clamping shape, not every window). The "what
//     the peer holds" vs "what the user
//     wants" column pair once proposed here was REJECTED with reasons; they
//     are recorded in FOLLOWUPS.md so it is not re-proposed.
//
// Subjects not present in ap_objects are dropped and logged at debug, like
// ApplyVote.
func (a *Aggregator) SeedAggregates(ctx context.Context, subjectAPID string, upvotes, downvotes int) error {
	if subjectAPID == "" {
		return errors.NewValidationError("subject_ap_id", "must not be empty")
	}
	if upvotes < 0 || downvotes < 0 {
		return errors.NewValidationError("counts", "must not be negative")
	}
	if upvotes > MaxSeededCount || downvotes > MaxSeededCount {
		return errors.NewValidationError("counts",
			fmt.Sprintf("exceed the seeded-count sanity cap of %d (up=%d down=%d)", MaxSeededCount, upvotes, downvotes))
	}

	mapping, err := a.subjectMapping(ctx, subjectAPID)
	if err != nil {
		return err
	}
	if mapping == nil {
		a.logger.Debug("vote seed dropped: subject not bridged", "subject", subjectAPID)
		return nil
	}
	atURI := mapping.ATURI

	// Seed facts are collected inside the transaction and reported AFTER it
	// commits: recomputeAggregate can still fail and roll the whole seed back,
	// and a counter advanced for a seed that never happened — then advanced
	// again by the caller's retry — is worse than no counter.
	var seed seedOutcome
	if err := a.inTx(ctx, func(tx *sql.Tx) error {
		// Upsert-and-lock the aggregate row first — the per-subject
		// serialization point every mutation goes through — so the live-count
		// read below cannot interleave with a concurrent ApplyVote/RetractVote
		// on the same subject.
		if err := lockAggregate(ctx, tx, subjectAPID, atURI); err != nil {
			return err
		}
		// ONE statement for both subtrahends, deliberately: a second statement
		// would read on its own READ COMMITTED snapshot (inTx takes the default
		// isolation), so the two counts could come from different moments —
		// creating exactly the torn read the aggregate lock exists to prevent.
		//
		// ours.* is the votes LEMMY CURRENTLY HOLDS for our personas — exactly
		// per ROW, only APPROXIMATELY per DIRECTION — and the predicate is the
		// POSITIVE EQUALITY delivered_state = 'delivered':
		//
		//	pending (first try, retrying, poisoned) → the peer does not hold it
		//	delivered                               → it does: subtract
		//	delivered, Undo in flight               → still subtract; consume's
		//	                                          applyVoteDelete re-upserts
		//	                                          'delivered' precisely because the
		//	                                          peer has not yet processed the
		//	                                          withdrawal
		//	row gone (Undo delivered)               → nothing to subtract
		//
		// THE DIRECTION IS THE APPROXIMATE HALF, and only inside a re-cast
		// window. The row's `direction` tracks the newest INTENT while
		// `delivered_state` describes a delivery that already happened, so
		// after a flip the two come from different moments (see
		// store.DeliveredStateDelivered). This subtracts the flip's direction
		// from an origin total that still contains the old one: the direction
		// the peer really holds is left in the baseline, and the direction it
		// does not hold is subtracted from a total that never contained it.
		// Per row the term is RIGHT — the vote is counted among `ours`, which
		// is what the upsert guard bought — and per direction it is wrong
		// until the flip delivers or the vote is undone.
		//
		// It is signalled ONLY when the wrong-side subtraction breaches the
		// zero floor: SeedOursSubtracted counts the row (it also counts every
		// healthy delivered vote, so it identifies routine work, not this
		// window), and GREATEST(0, …) trips SeedBaselineClamped plus the
		// sampled clamp Warn naming direction, deficit_* and ours_* — but
		// unrelated votes in the subtracted direction can keep the raw
		// baseline non-negative, in which case the misread is absorbed with
		// NO distinguishing signal. The two directional errors can also
		// CANCEL in the served number, so vote_aggregates alone shows a
		// healthy subject either way. The clamping shape — the one that does
		// signal — is pinned exactly as it stands by
		// TestReseedDuringARecastWindowMisreadsBothDirections; the silent
		// shape has no witness here, and a standing one is RecastDivergence's
		// to report.
		//
		// A negation ("NOT undone", "<> 'pending'") would be WRONG TODAY, not
		// merely future-hostile: 'undone' has a live writer — 17d's purge
		// (outbound.Purger.undoLiveVotes) retracts a withdrawn actor's votes
		// through the upsert — and it records OUR decision to stop counting
		// at purge time, not the peer's acceptance (store.DeliveredStateUndone).
		// A negation would resume subtracting a withdrawn actor's votes; this
		// equality stays correct. A poisoned Undo leaves a delivered row
		// subtracting forever, which is the same hazard decision 16 cites for
		// banning queue-history arithmetic: "delivered Likes minus delivered
		// Undos" gets that row permanently wrong.
		//
		// The read takes no row locks. The seed holds the aggregate lock and
		// reads outbound_votes lock-free; the delivery worker locks
		// outbound_votes and never touches vote_aggregates — so there is no
		// cycle, and FOR UPDATE would both create one and park a backfill's
		// seeding behind in-flight HTTP deliveries.
		if err := tx.QueryRowContext(ctx, `
			UPDATE vote_aggregates a
			SET seeded_upvotes = GREATEST(0, $2 - live.up - ours.up),
			    seeded_downvotes = GREATEST(0, $3 - live.down - ours.down)
			FROM (
				SELECT
					COUNT(*) FILTER (WHERE direction = 'up') AS up,
					COUNT(*) FILTER (WHERE direction = 'down') AS down
				FROM vote_events
				WHERE subject_ap_id = $1 AND NOT undone
			) live, (
				SELECT
					COUNT(*) FILTER (WHERE direction = 'up') AS up,
					COUNT(*) FILTER (WHERE direction = 'down') AS down
				FROM outbound_votes
				WHERE subject_ap_id = $1 AND delivered_state = 'delivered'
			) ours
			WHERE a.subject_ap_id = $1
			RETURNING $2 - live.up - ours.up, $3 - live.down - ours.down,
			          live.up, live.down, ours.up, ours.down`,
			subjectAPID, upvotes, downvotes).Scan(
			&seed.rawUp, &seed.rawDown, &seed.liveUp, &seed.liveDown,
			&seed.oursUp, &seed.oursDown); err != nil {
			return fmt.Errorf("seed vote aggregate for %q: %w", subjectAPID, err)
		}
		return recomputeAggregate(ctx, tx, subjectAPID)
	}); err != nil {
		return err
	}
	a.reportSeed(subjectAPID, upvotes, downvotes, seed)
	return nil
}

// seedOutcome is what one committed seed observed: the raw (pre-clamp) signed
// baselines, and the two subtrahends they were computed from.
type seedOutcome struct {
	rawUp, rawDown   int64
	liveUp, liveDown int64
	oursUp, oursDown int64
}

// reportSeed publishes what a COMMITTED seed observed.
//
// SeedOursSubtracted is the routine half: how many of our personas' votes this
// seed believes the origin is holding. It advances on healthy subjects, which
// is the point — a persistent gap against a Lemmy-side sample is how a silent
// discard shows up on posts with real scores.
//
// The clamp is the exceptional half, and it makes GREATEST(0, …) visible: a
// negative raw baseline means the origin's total for that direction is smaller
// than what we can already account for, and the clamp then absorbs the deficit
// silently. Counters always advance (a sampled counter counts nothing); only
// the line is sampled, because a backfill seeds one subject per post and a
// systemic cause produces one line per post for the whole run.
func (a *Aggregator) reportSeed(subject string, apiUp, apiDown int, seed seedOutcome) {
	if ours := seed.oursUp + seed.oursDown; ours > 0 {
		SeedOursSubtracted.Add(ours)
	}
	if seed.rawUp >= 0 && seed.rawDown >= 0 {
		return
	}
	SeedBaselineClamped.Add(1)
	// Both directions are reported, always: when both breach, naming one hides
	// the other, and the pair is what says whether the cause is directional.
	direction := directionUp
	switch {
	case seed.rawUp < 0 && seed.rawDown < 0:
		direction = directionUp + "+" + directionDown
	case seed.rawDown < 0:
		direction = directionDown
	}
	if a.clampLog.Allow(time.Now()) {
		a.logger.Warn("vote seed baseline clamped: the origin's total is short of the votes we can account for",
			"subject", subject, "direction", direction,
			"deficit_up", min64(seed.rawUp, 0), "deficit_down", min64(seed.rawDown, 0),
			"api_up", apiUp, "api_down", apiDown,
			"live_up", seed.liveUp, "live_down", seed.liveDown,
			"ours_up", seed.oursUp, "ours_down", seed.oursDown)
	}
}

// min64 reports the smaller of two int64s (the deficit fields log 0 for a
// direction that did not breach, rather than a positive baseline that would
// read as one).
func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// ScrubVoter erases every vote_events row a voter ever produced — the vote
// counterpart of the record scrub on Delete(Actor)/consent revocation
// (task 11; the rows tie an identifiable AP actor id to their voting
// history, which must not outlive their account). Live votes are removed
// from served counts too: every affected aggregate is locked (in
// deterministic subject order, so concurrent per-subject writers cannot
// deadlock against the scrub) and recomputed in the same transaction.
// Seeded baselines are untouched — they were never per-voter data. A voter
// with no rows is a no-op success.
func (a *Aggregator) ScrubVoter(ctx context.Context, voterAPID string) error {
	if voterAPID == "" {
		return errors.NewValidationError("voter_ap_id", "must not be empty")
	}
	return a.inTx(ctx, func(tx *sql.Tx) error {
		// Lock the aggregates the voter is KNOWN to have voted on first,
		// ordered, exactly like the single-subject paths do (lock → mutate →
		// recompute).
		locked := map[string]bool{}
		rows, err := tx.QueryContext(ctx, `
			SELECT subject_ap_id
			FROM vote_aggregates
			WHERE subject_ap_id IN (
				SELECT DISTINCT subject_ap_id FROM vote_events WHERE voter_ap_id = $1
			)
			ORDER BY subject_ap_id
			FOR UPDATE`, voterAPID)
		if err != nil {
			return fmt.Errorf("lock aggregates for voter %q: %w", voterAPID, err)
		}
		for rows.Next() {
			var subject string
			if err := rows.Scan(&subject); err != nil {
				_ = rows.Close()
				return fmt.Errorf("scan scrub subject for %q: %w", voterAPID, err)
			}
			locked[subject] = true
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("iterate scrub subjects for %q: %w", voterAPID, err)
		}
		_ = rows.Close()

		// DELETE ... RETURNING is the authoritative recompute set: it names
		// EVERY subject a row was actually removed from. That is a superset of
		// the pre-DELETE snapshot when a concurrent AddVote committed a vote on
		// a not-previously-voted subject between the lock above and this DELETE
		// — a subject that would otherwise be deleted-but-never-recomputed
		// (phantom count). Recomputing only the snapshot is exactly the
		// lost-update bug.
		var deleted int64
		affected := map[string]bool{}
		delRows, err := tx.QueryContext(ctx,
			`DELETE FROM vote_events WHERE voter_ap_id = $1 RETURNING subject_ap_id`, voterAPID)
		if err != nil {
			return fmt.Errorf("scrub vote events for %q: %w", voterAPID, err)
		}
		for delRows.Next() {
			var subject string
			if err := delRows.Scan(&subject); err != nil {
				_ = delRows.Close()
				return fmt.Errorf("scan deleted scrub subject for %q: %w", voterAPID, err)
			}
			deleted++
			affected[subject] = true
		}
		if err := delRows.Err(); err != nil {
			_ = delRows.Close()
			return fmt.Errorf("iterate deleted scrub subjects for %q: %w", voterAPID, err)
		}
		_ = delRows.Close()

		// Newly appearing subjects (in the delete set but not the snapshot):
		// lock them before recompute, in subject order, preserving the
		// deterministic ORDER BY subject_ap_id lock discipline (task 07) so we
		// do not introduce a deadlock.
		var newSubjects []string
		for subject := range affected {
			if !locked[subject] {
				newSubjects = append(newSubjects, subject)
			}
		}
		sort.Strings(newSubjects)
		for _, subject := range newSubjects {
			var got string
			err := tx.QueryRowContext(ctx, `
				SELECT subject_ap_id FROM vote_aggregates
				WHERE subject_ap_id = $1 FOR UPDATE`, subject).Scan(&got)
			if stderrors.Is(err, sql.ErrNoRows) {
				// No aggregate row for this subject (the concurrent writer's
				// row was itself removed): nothing to lock or recompute.
				continue
			}
			if err != nil {
				return fmt.Errorf("lock newly appearing aggregate %q for voter %q: %w", subject, voterAPID, err)
			}
			locked[subject] = true
		}

		// Recompute every locked aggregate (snapshot ∪ newly-appearing rows =
		// exactly the subjects with an aggregate row that lost one of this
		// voter's votes), in deterministic subject order.
		recomputeSet := make([]string, 0, len(locked))
		for subject := range locked {
			recomputeSet = append(recomputeSet, subject)
		}
		sort.Strings(recomputeSet)
		for _, subject := range recomputeSet {
			if err := recomputeAggregate(ctx, tx, subject); err != nil {
				return err
			}
		}
		if deleted > 0 {
			a.logger.Info("vote events scrubbed for deleted voter",
				"voter", voterAPID, "events", deleted, "subjects", len(recomputeSet))
		}
		return nil
	})
}

// pruneVoteEventsBatchSize bounds one DELETE inside PruneUndoneEvents.
const pruneVoteEventsBatchSize = 1000

// PruneUndoneEvents deletes undone (superseded or retracted) vote_events
// rows older than the cutoff, in batches, returning how many were removed.
// Live rows are NEVER pruned — they are the counts. Retention trade-off,
// accepted and mirrored from ap_tombstones: an undone row is also the
// dedupe record for its activity id, so pruning re-opens replay for
// activities older than the retention window — but real federation queues
// redeliver over hours-to-days, not months, and VOTE_EVENT_RETENTION's
// default (90 days) dwarfs that horizon.
func (a *Aggregator) PruneUndoneEvents(ctx context.Context, cutoff time.Time) (int64, error) {
	var total int64
	for {
		result, err := a.db.ExecContext(ctx, `
			DELETE FROM vote_events WHERE id IN (
				SELECT id FROM vote_events
				WHERE undone AND created_at < $1
				LIMIT $2
			)`, cutoff, pruneVoteEventsBatchSize)
		if err != nil {
			return total, fmt.Errorf("votes: prune undone events before %s: %w", cutoff.Format(time.RFC3339), err)
		}
		n, err := result.RowsAffected()
		if err != nil {
			return total, fmt.Errorf("votes: prune undone events: rows affected: %w", err)
		}
		total += n
		if n < pruneVoteEventsBatchSize {
			return total, nil
		}
	}
}

// isOurPersona reports whether the voter is one of the personas the bridge
// mints and SPEAKS AS (ap_actors) — a vote of ours coming home, which must
// never be counted as inbound.
//
// The probe is the classifier's actor route, so it distinguishes the one thing
// that matters: a bridged actor is a real fediverse human the bridge MIRRORS
// into atproto, holding a DID we minted and a bridged_actors row, and their
// votes are genuine. Matching that table instead would drop every inbound vote
// in the network and take the served tallies to zero.
//
// An error is returned, never absorbed into a verdict: "not ours" on a failed
// read double-counts our own vote, "ours" loses a real one, and only the retry
// is honest.
func (a *Aggregator) isOurPersona(ctx context.Context, voter string) (bool, error) {
	identity, err := a.voters.Identify(ctx, voter)
	if err != nil {
		return false, fmt.Errorf("probe voter %q: %w", voter, err)
	}
	return identity.Class == echo.ClassLocalActor, nil
}

// dropEchoedVote records a suppressed vote: the per-class echo counter plus a
// sampled INFO line. INFO, not Debug, and deliberately unlike the surrounding
// vote drops: those are ordinary traffic, while this one is the only visible
// evidence if the probe ever starts matching genuine voters — which would
// silently zero every community's tallies.
func (a *Aggregator) dropEchoedVote(what string, vote *ap.Object, voter, subject, communityIRI string) {
	echo.CountDrop(echo.ClassLocalActor)
	if a.echoLog.Allow(time.Now()) {
		a.logger.Info(what+" dropped: voter is one of our own personas",
			"activity", vote.ID, "type", vote.Type, "voter", voter,
			"subject", subject, "community", communityIRI)
	}
}

// subjectMapping resolves a voted-on AP id to its materialized mapping. It
// returns nil (drop the vote) when the subject was never materialized or its
// mapping is soft-deleted — voting on deleted content stays a no-op.
func (a *Aggregator) subjectMapping(ctx context.Context, subject string) (*store.APObjectMapping, error) {
	mapping, err := a.objects.GetByAPID(ctx, subject)
	if errors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("votes: resolve subject %s: %w", subject, err)
	}
	if mapping.IsDeleted() {
		return nil, nil
	}
	return mapping, nil
}

// subjectBelongsToCommunity reports whether a bridged subject's content
// belongs to the announcing community — the vote counterpart of ingest's
// announced-content and announced-delete authority checks. Without it, ONE
// malicious followed community could Announce Like/Dislike/Undo against ANY
// bridged subject and skew other communities' scores (fabricated voter
// strings need no mint).
//
// The binding is by community DID, not IRI authority: Lemmy hosts a post's
// AP object on the AUTHOR's instance, so a legitimate cross-instance-authored
// post would fail any SameAuthority(subject, announcer) check. Which community
// a subject belongs to is materialize.CommunityDIDOf's question — the same one
// ingest's announced-delete authorization asks, answered in one place so the
// two cannot drift into disagreeing about who owns a piece of content.
//
// A subject that cannot be bound counts for NOBODY: an unbindable subject is
// indistinguishable from another community's, and the cost of erring the safe
// way is a vote that does not move a counter.
func (a *Aggregator) subjectBelongsToCommunity(ctx context.Context, mapping *store.APObjectMapping, communityIRI string) (bool, error) {
	community, err := a.communities.GetByAPGroupID(ctx, communityIRI)
	if errors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("votes: resolve announcing community %s: %w", communityIRI, err)
	}
	subjectCommunityDID, err := materialize.CommunityDIDOf(ctx, a.records, mapping)
	if err != nil {
		return false, fmt.Errorf("votes: bind subject %s to a community: %w", mapping.APID, err)
	}
	return subjectCommunityDID != "" && subjectCommunityDID == community.DID, nil
}

// inTx runs fn inside a transaction, committing on nil and rolling back on
// error.
func (a *Aggregator) inTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("votes: begin tx: %w", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("votes: commit tx: %w", err)
	}
	return nil
}

// lockAggregate upserts the subject's aggregate row, taking its row lock —
// the per-subject serialization point every vote mutation goes through
// (DO UPDATE always fires, so the lock is taken on the existing-row path
// too). Counts are recomputed later in the same transaction.
func lockAggregate(ctx context.Context, tx *sql.Tx, subject, atURI string) error {
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO vote_aggregates (subject_ap_id, subject_at_uri)
		VALUES ($1, $2)
		ON CONFLICT (subject_ap_id) DO UPDATE SET subject_at_uri = EXCLUDED.subject_at_uri`,
		subject, atURI); err != nil {
		return fmt.Errorf("lock vote aggregate for %q: %w", subject, err)
	}
	return nil
}

// recomputeAggregate rewrites the subject's served totals from the seeded
// baseline plus the live (non-undone) events. Recompute-per-subject over
// incremental arithmetic: a Lemmy post sees at most a few thousand votes,
// and recomputing inside the locking transaction cannot drift.
//
// updated_at uses clock_timestamp() (the real wall time at statement
// execution), NOT CURRENT_TIMESTAMP (which is transaction-START time). Under
// concurrent voters (INGEST_WORKERS > 1) two commits' transaction-start times
// can order oppositely to their aggregate-row-lock acquisition, so
// CURRENT_TIMESTAMP could stamp a later-committed vote with an EARLIER
// updated_at — landing it below the stats refresher's already-advanced
// watermark, never to be emitted (permanent staleness on a then-quiet
// subject). clock_timestamp() is taken while THIS transaction holds the
// per-subject aggregate row lock, so per-subject updated_at is monotonic and
// every committed vote strictly advances it past any prior watermark.
func recomputeAggregate(ctx context.Context, tx *sql.Tx, subject string) error {
	if _, err := tx.ExecContext(ctx, `
		UPDATE vote_aggregates a
		SET upvotes = a.seeded_upvotes + live.up,
		    downvotes = a.seeded_downvotes + live.down,
		    updated_at = clock_timestamp()
		FROM (
			SELECT
				COUNT(*) FILTER (WHERE direction = 'up') AS up,
				COUNT(*) FILTER (WHERE direction = 'down') AS down
			FROM vote_events
			WHERE subject_ap_id = $1 AND NOT undone
		) live
		WHERE a.subject_ap_id = $1`,
		subject); err != nil {
		return fmt.Errorf("recompute vote aggregate for %q: %w", subject, err)
	}
	return nil
}

// directionFor maps an AP activity type to a vote direction.
func directionFor(activityType string) (string, bool) {
	switch activityType {
	case ap.TypeLike:
		return directionUp, true
	case ap.TypeDislike:
		return directionDown, true
	}
	return "", false
}

// refID returns the id of a possibly-nil object reference.
func refID(obj *ap.Object) string {
	if obj == nil {
		return ""
	}
	return obj.ID
}
