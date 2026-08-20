package materialize

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"time"

	"tidepool/internal/errors"
	"tidepool/internal/repo"
	"tidepool/internal/store"
)

// bridgedStatsField is the optional post/comment record field the bridge uses
// to assert the origin platform's aggregate vote counts (the Coves
// #bridgedStats def: {upvotes, downvotes, asOf}). Records are BORN without it
// (the create path never sets it); the vote-stats refresher adds it and keeps
// it fresh, and commitRecord carries it forward across Lemmy edits.
const bridgedStatsField = "bridgedStats"

// maxStatsCommitAttempts bounds the compare-and-swap retry loop below. A
// mismatch means the record changed between the read and the commit (a Lemmy
// edit, or a concurrent stats stamp); a handful of retries absorbs realistic
// churn, and exhausting them is reported as a transient error so the refresher
// leaves the row dirty and retries next sweep.
const maxStatsCommitAttempts = 4

// EmitBridgedStats folds an aggregate's counts onto its materialized record
// and reports whether a real commit happened (a NoOp — counts unchanged —
// reports committed=false). It is the narrow seam votes.StatsEmitter is defined
// over, so internal/votes needs neither *materialize.Result nor an import of
// this package to drive the sweep. SetBridgedStats keeps the rich Result for
// this package's own tests.
func (m *Materializer) EmitBridgedStats(ctx context.Context, mapping *store.APObjectMapping, upvotes, downvotes int, asOf time.Time) (committed bool, err error) {
	res, err := m.SetBridgedStats(ctx, mapping, upvotes, downvotes, asOf)
	if err != nil {
		return false, err
	}
	return res != nil && !res.NoOp, nil
}

// SetBridgedStats writes upvotes/downvotes (sampled as of asOf) onto the
// bridgedStats field of the record named by mapping, keeping the ap_objects
// mapping CID in sync — record + mapping land in ONE commit transaction, the
// same PutRecord+PutMappingTx discipline commitRecord uses (task 11).
//
// The read (GetRecord) happens OUTSIDE the commit serialization, so the write
// goes through PutRecordCAS with the read's CID as an optimistic-concurrency
// precondition: if a Lemmy edit or another stamp changed the record in
// between, the commit fails ErrPreconditionFailed and this re-reads and
// retries rather than silently reverting the edit or resurrecting a
// just-deleted record. Inside the commit the mapping's soft-delete is
// re-checked too, so a refresher racing a Delete cannot un-tombstone the
// mapping via PutMappingTx's unconditional deleted_at = NULL.
//
// Contract the refresher relies on:
//   - Counts already equal to what the record carries (a re-emit where only
//     asOf would move) → returns a NoOp Result WITHOUT committing. Re-putting
//     would mint a fresh CID and a firehose event just to bump a timestamp;
//     that churn is pure noise for relays and the AppView, so it is skipped.
//   - Record gone (deleted between the vote landing and this call — AP
//     delivery is unordered — or its mapping soft-deleted inside the commit)
//     → errors.IsRecordGone, a sentinel DISTINCT from a bare NotFound so the
//     refresher advances only on a genuine record-gone, never on a NotFound
//     raised deeper in the commit (a missing bridged_actor row or signing
//     key).
//   - Repo frozen by a consent revocation → errors.IsTombstoned (the commit's
//     signing-key consent gate refuses writes to a tombstoned actor). Both
//     record-gone and tombstoned are permanent; the refresher advances its
//     watermark past them.
func (m *Materializer) SetBridgedStats(ctx context.Context, mapping *store.APObjectMapping, upvotes, downvotes int, asOf time.Time) (*Result, error) {
	if mapping == nil {
		return nil, errors.NewValidationError("mapping", "must not be nil")
	}
	if upvotes < 0 || downvotes < 0 {
		return nil, errors.NewValidationError("counts", "must not be negative")
	}

	for attempt := 0; ; attempt++ {
		record, prevCID, err := m.repos.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
		if err != nil {
			if errors.IsNotFound(err) {
				// The record was deleted out from under its aggregate. Translate
				// only THIS read's NotFound into the record-gone sentinel — the
				// refresher's permanent skip — so a NotFound from elsewhere in the
				// commit stays transient.
				return nil, fmt.Errorf("materialize: stats target %s gone: %w", mapping.ATURI, errors.ErrRecordGone)
			}
			return nil, fmt.Errorf("materialize: read record for stats %s: %w", mapping.ATURI, err)
		}

		// Counts unchanged? Re-emitting would only move asOf, minting a new CID
		// and a firehose event for nothing. Skip the commit entirely (the caller
		// still advances its watermark).
		//
		// The ACCEPTANCE is still reconciled first, against prevCID — the CID
		// this read just proved the record has, never mapping.CID, which is a
		// row read before the commit that produced it and can name a version
		// that no longer exists. The repin has to be driven by the pin, not by
		// whether this stamp committed: a crash between a stats commit and its
		// repin leaves an acceptance pinning a dead CID, and every later sweep
		// carries the same counts and lands here. Returning early would make
		// this the one branch that can SEE the stale pin and the only one that
		// never fixes it, so the post stays out of its community until somebody
		// edits it upstream. An already-correct pin costs one record read and
		// commits nothing.
		if up, down, ok := bridgedStatsCounts(record); ok && up == upvotes && down == downvotes {
			if err := m.repinAcceptance(ctx, mapping, prevCID); err != nil {
				return nil, err
			}
			return &Result{DID: mapping.DID, ATURI: mapping.ATURI, CID: mapping.CID, NoOp: true}, nil
		}

		// asOf renders at microsecond precision (recordDatetimeMicros), the
		// resolution vote_aggregates.updated_at actually carries — so two
		// versions in the same millisecond stay distinguishable to a consumer's
		// asOf guard.
		record[bridgedStatsField] = map[string]any{
			"upvotes":   int64(upvotes),
			"downvotes": int64(downvotes),
			"asOf":      recordDatetimeMicros(asOf),
		}
		if err := m.validateRecord(record); err != nil {
			return nil, err
		}

		updated := *mapping
		res, err := m.repos.PutRecordCAS(ctx, mapping.DID, mapping.Collection, mapping.RKey, record, prevCID,
			func(ctx context.Context, tx *sql.Tx, res *repo.CommitResult) error {
				// Resurrection guard: re-check the mapping's soft-delete INSIDE
				// the commit tx. The CAS precondition already refuses to re-create
				// a record deleted from the repo; this additionally refuses to
				// un-tombstone the MAPPING (PutMappingTx clears deleted_at
				// unconditionally). The legitimate restore path clears deleted_at
				// via objects.Restore BEFORE its rebuild commit, so it passes here.
				deleted, derr := m.objects.DeletedInTx(ctx, tx, mapping.APID)
				switch {
				case derr == nil:
					if deleted {
						return fmt.Errorf("materialize: stats target %s soft-deleted mid-commit: %w", mapping.ATURI, errors.ErrRecordGone)
					}
				case errors.IsNotFound(derr):
					return fmt.Errorf("materialize: stats target %s mapping vanished mid-commit: %w", mapping.ATURI, errors.ErrRecordGone)
				default:
					return derr
				}
				updated.CID = res.RecordCID
				_, mapErr := m.objects.PutMappingTx(ctx, tx, updated)
				return mapErr
			})
		if stderrors.Is(err, repo.ErrPreconditionFailed) {
			if attempt+1 < maxStatsCommitAttempts {
				continue // the record changed under us; re-read and retry
			}
			// Persistent churn: leave it for the next sweep (transient, not a
			// record-gone — the record is very much alive, just moving).
			return nil, fmt.Errorf("materialize: stats for %s: record kept changing across %d attempts: %w", mapping.ATURI, maxStatsCommitAttempts, err)
		}
		if err != nil {
			return nil, fmt.Errorf("materialize: put stats for %s: %w", mapping.ATURI, err)
		}
		if err := m.repinAcceptance(ctx, &updated, res.RecordCID); err != nil {
			return nil, err
		}
		return &Result{DID: mapping.DID, ATURI: mapping.ATURI, CID: res.RecordCID, NoOp: res.NoOp}, nil
	}
}

// repinAcceptance re-points a postv2's acceptance at the version this stamp
// just produced. A stats stamp rewrites the record, so its CID moves — and an
// acceptance pinning the OLD CID is, to Coves' consumers, an acceptance of a
// version that no longer exists: the lexicon tells them not to render the new
// CID under it, so a post would fall out of its community every time the vote
// sweep touched it. Nothing about the community's decision changed, so the
// stored createdAt is carried forward and only the pin moves.
//
// A repin cannot join the stats commit — that lands in the AUTHOR's repo and
// this in the COMMUNITY's — so it is a second commit with the same heal
// property as the create path: redelivery re-runs it. It is skipped, rather
// than guessed at, when the mapping cannot say where or when to write; the
// next materialization of the post heals it.
//
// It runs on EVERY stats pass, the no-op ones included, because it reconciles
// a pin rather than announcing a commit: recordCID is whatever the caller has
// just proved the record holds, and an acceptance already naming it re-puts
// byte-identically and reaches the repo layer's no-op path. Gating this on
// "did the stamp commit?" would leave a crash-window pin permanently stale,
// since the sweep that follows such a crash carries unchanged counts.
func (m *Materializer) repinAcceptance(ctx context.Context, mapping *store.APObjectMapping, recordCID string) error {
	if mapping.Collection != CollectionPostV2 {
		return nil
	}
	// Resolved rather than read straight off the column: a mapping written
	// before migration 016 carries no community_did, and bailing on that
	// would leave exactly the oldest posts — the ones most likely to be
	// stats-swept — pinned to a dead CID forever.
	communityDID, err := CommunityDIDOf(ctx, m.repos, mapping)
	if err != nil {
		return err
	}
	if communityDID == "" || mapping.PublishedAt == nil {
		m.logger.Debug("cannot repin acceptance; leaving it to the next materialization",
			"ap_id", mapping.APID, "at_uri", mapping.ATURI,
			"community_did", communityDID, "has_published", mapping.PublishedAt != nil)
		return nil
	}
	if err := m.acceptPost(ctx, communityDID, mapping.ATURI, recordCID, *mapping.PublishedAt); err != nil {
		return err
	}

	// ORPHAN GUARD. The stats commit and this repin are separate commits in
	// separate repos, so a delete can land between them: deleteMapping removes
	// the acceptance and the post, and this write then re-creates an acceptance
	// for a record that no longer exists — a community attesting to nothing,
	// which no later delete will revisit because the mapping is already
	// tombstoned. Re-read the mapping and compensate if that happened.
	current, err := m.objects.GetByAPID(ctx, mapping.APID)
	switch {
	case err == nil && !current.IsDeleted():
		return nil
	case err != nil && !errors.IsNotFound(err):
		return fmt.Errorf("materialize: re-check mapping after repin %s: %w", mapping.APID, err)
	}
	m.logger.Warn("post was deleted while its acceptance was being repinned; withdrawing the acceptance",
		"ap_id", mapping.APID, "at_uri", mapping.ATURI)
	rkey := SubjectRKey(mapping.ATURI)
	if _, derr := m.repos.DeleteRecord(ctx, communityDID, CollectionAcceptance, rkey); derr != nil && !errors.IsNotFound(derr) {
		return fmt.Errorf("materialize: withdraw orphaned acceptance %s/%s/%s: %w",
			communityDID, CollectionAcceptance, rkey, derr)
	}
	return nil
}

// bridgedStatsCounts reads the upvotes/downvotes a record's bridgedStats field
// currently asserts. ok is false when the field is absent or malformed (the
// record has never been stats-stamped). Integers arrive as int64 through the
// CBOR read path; the other numeric arms tolerate a JSON-decoded record.
func bridgedStatsCounts(record map[string]any) (up, down int, ok bool) {
	stats, ok := record[bridgedStatsField].(map[string]any)
	if !ok {
		return 0, 0, false
	}
	up, upOK := asInt(stats["upvotes"])
	down, downOK := asInt(stats["downvotes"])
	return up, down, upOK && downOK
}

func asInt(v any) (int, bool) {
	switch n := v.(type) {
	case int64:
		return int(n), true
	case int:
		return n, true
	case float64:
		return int(n), true
	default:
		return 0, false
	}
}
