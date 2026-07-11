package materialize

import (
	"context"
	"fmt"

	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/ipfs/go-cid"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/repo"
	"tidepool/internal/store"
)

// HandleUpdate re-materializes the object carried by an AP Update activity.
// Deterministic rkeys make this a re-put under the same record key: posts
// and comments flow through the same paths as creation (an Update for a
// never-seen object simply materializes it), and actor/community updates
// force a profile refresh regardless of the TTL.
func (m *Materializer) HandleUpdate(ctx context.Context, obj *ap.Object) (*Result, error) {
	if obj == nil || obj.ID == "" {
		return nil, errors.NewValidationError("object", "must carry an AP object id")
	}
	switch obj.Type {
	case ap.TypePerson:
		if _, err := m.RefreshActor(ctx, obj); err != nil {
			return nil, err
		}
		return m.resultForMapping(ctx, obj.ID)
	case ap.TypeGroup:
		if _, err := m.RefreshCommunity(ctx, obj); err != nil {
			return nil, err
		}
		return m.resultForMapping(ctx, obj.ID)
	case ap.TypePage, ap.TypeArticle:
		return m.MaterializePost(ctx, obj)
	case ap.TypeNote:
		return m.MaterializeComment(ctx, obj)
	default:
		return nil, skip(obj.ID, "unsupported Update object type "+obj.Type)
	}
}

// HandleDelete processes an AP Delete (or Tombstone) for an object or an
// actor. Actor ids trigger the full Delete(Actor) scrub; object ids delete
// the single record and soft-delete its mapping. Unknown ids are a logged
// no-op (nothing was ever bridged). Idempotent throughout.
func (m *Materializer) HandleDelete(ctx context.Context, apID string) error {
	if apID == "" {
		return errors.NewValidationError("ap_id", "must not be empty")
	}

	// Actor deletion first: actors also have a profile mapping row, and a
	// Delete(Actor) must scrub everything, not just the profile record.
	if _, err := m.actors.GetByAPActorID(ctx, apID); err == nil {
		return m.DeleteActor(ctx, apID)
	} else if !errors.IsNotFound(err) {
		return fmt.Errorf("materialize: look up actor for delete %s: %w", apID, err)
	}

	mapping, err := m.objects.GetByAPID(ctx, apID)
	if errors.IsNotFound(err) {
		// Nothing bridged under this id. Usually a delete for content we
		// never materialized (skipped/opted-out); but it can also be a Delete
		// racing the crash window between PutRecord and PutMapping (the record
		// is on the firehose with no mapping yet). Warn, not Debug, so that
		// rarer case is observable — re-delivery of the original Create heals
		// the mapping and a subsequent Delete then finds it.
		m.logger.Warn("delete for unmapped object; nothing to delete", "ap_id", apID)
		return nil
	}
	if err != nil {
		return fmt.Errorf("materialize: look up mapping for delete %s: %w", apID, err)
	}
	return m.deleteMapping(ctx, mapping)
}

// SuppressActor stops bridging an actor that opted out (#nobridge/#nobot)
// after previously being bridged: it scrubs every record they authored
// (and the blobs those records referenced, plus their vote_events rows),
// then records ConsentStateNoBridge. Unlike DeleteActor this is reversible
// — if the marker later disappears upstream, EnsureActor restores bridging
// (re-materialization re-fetches media, so scrubbed blobs come back too).
// Task 06 calls this when it discovers a consent-marker transition out of
// band.
func (m *Materializer) SuppressActor(ctx context.Context, apActorID string) error {
	actor, err := m.actors.GetByAPActorID(ctx, apActorID)
	if errors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("materialize: look up actor %s: %w", apActorID, err)
	}
	n, err := m.scrubActorRecords(ctx, actor.DID)
	if err != nil {
		return err
	}
	if err := m.scrubActorVotes(ctx, apActorID); err != nil {
		return err
	}
	if err := m.actors.SetConsentState(ctx, apActorID, store.ConsentStateNoBridge); err != nil {
		return fmt.Errorf("materialize: record nobridge for %s: %w", apActorID, err)
	}
	m.logger.Info("actor opted out; bridged records scrubbed and bridging suspended",
		"ap_actor_id", apActorID, "did", actor.DID, "records", n)
	return nil
}

// scrubActorRecords tombstones every record an actor authored — records in
// their own repo (comments, profile) and posts written into community repos
// (author_did) — and deletes the blobs those records referenced (read out
// of each record BEFORE its delete commit; post images live under the
// COMMUNITY's DID, which nothing else would ever clean up). Idempotent:
// already-deleted mappings are no-ops, and blob deletes tolerate missing
// rows. Shared by DeleteActor (terminal) and the nobridge suppression paths
// (reversible), so the consent decision is separate from the scrub
// mechanism.
func (m *Materializer) scrubActorRecords(ctx context.Context, actorDID string) (int, error) {
	mappings, err := m.objects.ListByActorDID(ctx, actorDID)
	if err != nil {
		return 0, fmt.Errorf("materialize: list records for %s: %w", actorDID, err)
	}
	for _, mapping := range mappings {
		// Delete the referenced blobs BEFORE soft-deleting the mapping, and
		// fail the whole scrub (a retryable error — NOT a skip) if any blob
		// delete fails. A swallowed failure orphans a blob that getBlob would
		// serve forever: post images live under the COMMUNITY's DID, which
		// nothing else cleans up. The ordering keeps the retry able to heal:
		// while the mapping is still live, ListByActorDID re-lists it and the
		// record is still readable, so the retry re-collects the same blob
		// refs and re-deletes them (DeleteBlob tolerates already-missing
		// rows). Soft-deleting the mapping first would drop it out of
		// ListByActorDID (deleted_at IS NULL) and strand the orphan.
		for _, blobCID := range m.recordBlobRefs(ctx, mapping) {
			if err := m.deleteBlob(ctx, mapping.DID, blobCID); err != nil {
				return 0, fmt.Errorf("materialize: scrub blob %s under %s: %w", blobCID, mapping.DID, err)
			}
		}
		if err := m.deleteMapping(ctx, mapping); err != nil {
			return 0, err
		}
	}
	return len(mappings), nil
}

// recordBlobRefs reads a mapping's current record and extracts every blob
// CID it references. Best-effort by design: a record already missing from
// the repo (crash window on a previous scrub run) just means no blobs to
// collect.
func (m *Materializer) recordBlobRefs(ctx context.Context, mapping *store.APObjectMapping) []string {
	if mapping.IsDeleted() {
		return nil
	}
	record, _, err := m.repos.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
	if err != nil {
		if !errors.IsNotFound(err) {
			m.logger.Warn("blob scrub: read record failed; skipping its blobs",
				"at_uri", mapping.ATURI, "error", err)
		}
		return nil
	}
	blobs := atdata.ExtractBlobs(record)
	refs := make([]string, 0, len(blobs))
	for _, blob := range blobs {
		refs = append(refs, cid.Cid(blob.Ref).String())
	}
	return refs
}

// scrubActorVotes erases the actor's vote_events rows via the configured
// VoteScrubber (nil = not wired, skip).
func (m *Materializer) scrubActorVotes(ctx context.Context, apActorID string) error {
	if m.votes == nil {
		return nil
	}
	if err := m.votes.ScrubVoter(ctx, apActorID); err != nil {
		return fmt.Errorf("materialize: scrub votes for %s: %w", apActorID, err)
	}
	return nil
}

// DeleteActor handles Delete(Actor): every record the actor authored is
// tombstoned FIRST (delete commits — the repo layer releases signing keys
// for deletes even after a consent flip), their vote_events rows are
// scrubbed, then the consent state is marked deleted (terminal), freezing
// the repo — and an #account{active:false, status:"deleted"} event is
// appended to the firehose so subscribers (relays above all) purge the repo
// instead of inferring its death from the delete commits. The order means a
// crash between any two steps re-runs cleanly: deletes and vote scrubs are
// idempotent, re-tombstoning is a no-op success, and the final account
// event still lands.
func (m *Materializer) DeleteActor(ctx context.Context, apActorID string) error {
	actor, err := m.actors.GetByAPActorID(ctx, apActorID)
	if errors.IsNotFound(err) {
		m.logger.Debug("delete for unbridged actor; nothing to do", "ap_actor_id", apActorID)
		return nil
	}
	if err != nil {
		return fmt.Errorf("materialize: look up actor %s: %w", apActorID, err)
	}

	// Terminal fixpoint: a second Delete carrying a distinct activity id (so
	// inbox dedup did not absorb it) must not re-scrub and, above all, must
	// not append a SECOND #account frame. ConsentStateDeleted is terminal —
	// the repo is already tombstoned and its scrub already emitted — so stop
	// here.
	if actor.ConsentState == store.ConsentStateDeleted {
		m.logger.Debug("delete for already-deleted actor; terminal, nothing to do",
			"ap_actor_id", apActorID, "did", actor.DID)
		return nil
	}

	// Everything they authored: records in their own repo (comments,
	// profile) plus posts written into community repos (author_did) — and
	// the blobs those records referenced (scrubActorRecords).
	n, err := m.scrubActorRecords(ctx, actor.DID)
	if err != nil {
		return err
	}
	if err := m.scrubActorVotes(ctx, apActorID); err != nil {
		return err
	}
	// The actor's own repo is now terminally frozen (getBlob refuses
	// deactivated repos and nothing re-materializes into it): drop every
	// remaining blob under their DID, including superseded profile media no
	// live record referenced anymore.
	if _, err := m.repos.DeleteBlobsForDID(ctx, actor.DID); err != nil {
		return fmt.Errorf("materialize: scrub blobs for %s: %w", actor.DID, err)
	}

	if err := m.actors.SetConsentState(ctx, apActorID, store.ConsentStateDeleted); err != nil {
		return fmt.Errorf("materialize: tombstone actor %s: %w", apActorID, err)
	}
	// AFTER the scrub commits and the consent flip, so the frame is the
	// last thing a per-repo-ordered consumer sees. Emitted even when the
	// actor authored nothing (an #account frame for a bare repo is still
	// correct), and re-emitted on idempotent re-runs (harmless).
	if _, err := m.repos.AppendAccountEvent(ctx, actor.DID, false, repo.AccountStatusDeleted); err != nil {
		return fmt.Errorf("materialize: append account event for %s: %w", actor.DID, err)
	}
	m.logger.Info("actor deleted upstream; bridged records scrubbed and repo tombstoned",
		"ap_actor_id", apActorID, "did", actor.DID, "records", n)
	return nil
}

// deleteMapping deletes one record and soft-deletes its mapping,
// idempotently: an already-deleted mapping is a no-op, a record missing
// from the repo (crash between delete and soft-delete on a previous run)
// is tolerated.
func (m *Materializer) deleteMapping(ctx context.Context, mapping *store.APObjectMapping) error {
	if mapping.IsDeleted() {
		return nil
	}
	if _, err := m.repos.DeleteRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("materialize: delete record %s: %w", mapping.ATURI, err)
	}
	if err := m.objects.SoftDelete(ctx, mapping.APID); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("materialize: soft-delete mapping %s: %w", mapping.APID, err)
	}
	m.logger.Info("bridged record deleted", "ap_id", mapping.APID, "at_uri", mapping.ATURI)
	return nil
}

// resultForMapping loads the current mapping for an AP id into a Result
// (profile refresh paths return the profile record's coordinates).
func (m *Materializer) resultForMapping(ctx context.Context, apID string) (*Result, error) {
	mapping, err := m.objects.GetByAPID(ctx, apID)
	if err != nil {
		return nil, fmt.Errorf("materialize: load mapping for %s: %w", apID, err)
	}
	return &Result{DID: mapping.DID, ATURI: mapping.ATURI, CID: mapping.CID}, nil
}
