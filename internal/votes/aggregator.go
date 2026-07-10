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
	"fmt"
	"log/slog"
	"strings"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// Vote directions (vote_events.direction).
const (
	directionUp   = "up"
	directionDown = "down"
)

// RecordReader is the slice of *repo.Manager the aggregator uses to read a
// bridged comment's stored record: the record's reply.root strongRef names
// the thread's root post in the community repo, which is how an announced
// comment vote is bound to its announcing community.
type RecordReader interface {
	GetRecord(ctx context.Context, did, collection, rkey string) (record map[string]any, recordCID string, err error)
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
	logger      *slog.Logger
}

// NewAggregator validates dependencies and builds an Aggregator.
func NewAggregator(db *sql.DB, objects store.APObjects, communities store.Communities, records RecordReader, logger *slog.Logger) (*Aggregator, error) {
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
	if logger == nil {
		logger = slog.Default()
	}
	return &Aggregator{db: db, objects: objects, communities: communities, records: records, logger: logger}, nil
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
// only sparsely). Live vote_events stack on top of the baseline; re-seeding
// (backfill redo) overwrites the baseline idempotently. Known drift: a voter
// counted only in the baseline who later flips federates a bare Dislike
// (Lemmy sends no Undo on flips), so the retired upvote stays in the
// baseline next to the new live downvote; a clear sends an Undo the
// fallback cannot act on (no live vote row). Accepted for v1; the baseline
// refreshes on re-seed.
// Subjects not present in ap_objects are dropped and logged at debug, like
// ApplyVote.
func (a *Aggregator) SeedAggregates(ctx context.Context, subjectAPID string, upvotes, downvotes int) error {
	if subjectAPID == "" {
		return errors.NewValidationError("subject_ap_id", "must not be empty")
	}
	if upvotes < 0 || downvotes < 0 {
		return errors.NewValidationError("counts", "must not be negative")
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

	return a.inTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO vote_aggregates (
				subject_ap_id, subject_at_uri,
				seeded_upvotes, seeded_downvotes, upvotes, downvotes
			) VALUES ($1, $2, $3, $4, $3, $4)
			ON CONFLICT (subject_ap_id) DO UPDATE SET
				subject_at_uri = EXCLUDED.subject_at_uri,
				seeded_upvotes = EXCLUDED.seeded_upvotes,
				seeded_downvotes = EXCLUDED.seeded_downvotes`,
			subjectAPID, atURI, upvotes, downvotes); err != nil {
			return fmt.Errorf("seed vote aggregate for %q: %w", subjectAPID, err)
		}
		// Fold any live events that landed before the seed into the totals.
		return recomputeAggregate(ctx, tx, subjectAPID)
	})
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
// post would fail any SameAuthority(subject, announcer) check. Posts are
// written into the community's own repo (PLAN.md decision 3), so the
// mapping's DID IS the community DID. Comments live in the author's repo;
// their stored record's reply.root strongRef names the thread's root post in
// the community repo, so one record read recovers the community DID.
func (a *Aggregator) subjectBelongsToCommunity(ctx context.Context, mapping *store.APObjectMapping, communityIRI string) (bool, error) {
	community, err := a.communities.GetByAPGroupID(ctx, communityIRI)
	if errors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("votes: resolve announcing community %s: %w", communityIRI, err)
	}
	switch mapping.Collection {
	case materialize.CollectionPost:
		return mapping.DID == community.DID, nil
	case materialize.CollectionComment:
		record, _, err := a.records.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
		if errors.IsNotFound(err) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("votes: read comment record %s: %w", mapping.ATURI, err)
		}
		rootDID := replyRootDID(record)
		return rootDID != "" && rootDID == community.DID, nil
	default:
		// Votes bind to posts and comments only.
		return false, nil
	}
}

// replyRootDID extracts the repo DID from a comment record's reply.root
// strongRef uri (at://did/collection/rkey). Malformed records yield "".
func replyRootDID(record map[string]any) string {
	reply, ok := record["reply"].(map[string]any)
	if !ok {
		return ""
	}
	root, ok := reply["root"].(map[string]any)
	if !ok {
		return ""
	}
	uri, _ := root["uri"].(string)
	rest, ok := strings.CutPrefix(uri, "at://")
	if !ok {
		return ""
	}
	did, _, _ := strings.Cut(rest, "/")
	return did
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
func recomputeAggregate(ctx context.Context, tx *sql.Tx, subject string) error {
	if _, err := tx.ExecContext(ctx, `
		UPDATE vote_aggregates a
		SET upvotes = a.seeded_upvotes + live.up,
		    downvotes = a.seeded_downvotes + live.down,
		    updated_at = CURRENT_TIMESTAMP
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
