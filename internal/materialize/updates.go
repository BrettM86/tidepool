package materialize

import (
	"context"
	"fmt"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
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
// after previously being bridged: it scrubs every record they authored, then
// records ConsentStateNoBridge. Unlike DeleteActor this is reversible — if
// the marker later disappears upstream, EnsureActor restores bridging. Task
// 06 calls this when it discovers a consent-marker transition out of band.
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
	if err := m.actors.SetConsentState(ctx, apActorID, store.ConsentStateNoBridge); err != nil {
		return fmt.Errorf("materialize: record nobridge for %s: %w", apActorID, err)
	}
	m.logger.Info("actor opted out; bridged records scrubbed and bridging suspended",
		"ap_actor_id", apActorID, "did", actor.DID, "records", n)
	return nil
}

// scrubActorRecords tombstones every record an actor authored — records in
// their own repo (comments, profile) and posts written into community repos
// (author_did). Idempotent: already-deleted mappings are no-ops. Shared by
// DeleteActor (terminal) and the nobridge suppression paths (reversible), so
// the consent decision is separate from the scrub mechanism.
func (m *Materializer) scrubActorRecords(ctx context.Context, actorDID string) (int, error) {
	mappings, err := m.objects.ListByActorDID(ctx, actorDID)
	if err != nil {
		return 0, fmt.Errorf("materialize: list records for %s: %w", actorDID, err)
	}
	for _, mapping := range mappings {
		if err := m.deleteMapping(ctx, mapping); err != nil {
			return 0, err
		}
	}
	return len(mappings), nil
}

// DeleteActor handles Delete(Actor): every record the actor authored is
// tombstoned FIRST (delete commits — the repo layer releases signing keys
// for deletes even after a consent flip), then the consent state is marked
// deleted (terminal), freezing the repo. The order means a crash between
// the two steps re-runs cleanly: deletes are idempotent, and the final
// consent flip still lands.
func (m *Materializer) DeleteActor(ctx context.Context, apActorID string) error {
	actor, err := m.actors.GetByAPActorID(ctx, apActorID)
	if errors.IsNotFound(err) {
		m.logger.Debug("delete for unbridged actor; nothing to do", "ap_actor_id", apActorID)
		return nil
	}
	if err != nil {
		return fmt.Errorf("materialize: look up actor %s: %w", apActorID, err)
	}

	// Everything they authored: records in their own repo (comments,
	// profile) plus posts written into community repos (author_did).
	n, err := m.scrubActorRecords(ctx, actor.DID)
	if err != nil {
		return err
	}

	if err := m.actors.SetConsentState(ctx, apActorID, store.ConsentStateDeleted); err != nil {
		return fmt.Errorf("materialize: tombstone actor %s: %w", apActorID, err)
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
