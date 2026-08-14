package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"

	"github.com/lib/pq"

	"tidepool/internal/errors"
)

// object_moderation is the bridge-owned moderation state described on the
// ObjectModeration interface and in migration 025. The methods hang off the
// ap_objects repository — the callers that need them all hold one — but the
// ROW is separate, because putMapping rewrites a mapping wholesale and a
// re-pin must never clear a lock.

func (r *postgresAPObjects) SetLock(ctx context.Context, object ModeratedObject, locked bool) error {
	if object.ATURI == "" {
		return errors.NewValidationError("at_uri", "must not be empty")
	}
	if object.CommunityDID == "" {
		// The binding is not bookkeeping: an unbound row is one any co-hosted
		// community's Undo{Lock} could clear, so a write that cannot name the
		// deciding community is refused rather than stored unbound.
		return errors.NewValidationError("community_did", "must not be empty")
	}

	if !locked {
		// Scoped to the holder. Authorization upstream already established that
		// this community owns the target's mapping, so this is the same rule
		// stated where the row lives — and a lift for an object nobody locked
		// updates nothing, which is the right outcome for a re-delivered Undo.
		if _, err := r.db.ExecContext(ctx, `
			UPDATE object_moderation
			   SET locked_at = NULL, updated_at = now()
			 WHERE at_uri = $1 AND community_did = $2`,
			object.ATURI, object.CommunityDID); err != nil {
			return fmt.Errorf("clear lock on %q: %w", object.ATURI, err)
		}
		return nil
	}

	if object.APID == "" {
		return errors.NewValidationError("ap_id", "must not be empty")
	}
	if _, err := r.db.ExecContext(ctx, `
		INSERT INTO object_moderation (at_uri, ap_id, community_did, locked_at)
		VALUES ($1, $2, $3, now())
		ON CONFLICT (at_uri) DO UPDATE SET
			ap_id = EXCLUDED.ap_id,
			community_did = EXCLUDED.community_did,
			-- COALESCE, never EXCLUDED outright: a re-delivered Announce{Lock}
			-- is the SAME decision arriving twice, and re-stamping it would walk
			-- the moderators' timestamp forward every time Lemmy retried.
			locked_at = COALESCE(object_moderation.locked_at, EXCLUDED.locked_at),
			updated_at = now()`,
		object.ATURI, object.APID, object.CommunityDID); err != nil {
		return fmt.Errorf("record lock on %q: %w", object.ATURI, err)
	}
	return nil
}

func (r *postgresAPObjects) SetRemoval(ctx context.Context, object ModeratedObject, code, reason string) error {
	if object.ATURI == "" {
		return errors.NewValidationError("at_uri", "must not be empty")
	}
	if object.APID == "" {
		return errors.NewValidationError("ap_id", "must not be empty")
	}
	if object.CommunityDID == "" {
		// Same rule as a lock: an unbound decision is one any co-hosted
		// community's Undo could lift.
		return errors.NewValidationError("community_did", "must not be empty")
	}
	if code == "" {
		// A removal with no code is a decision with no machine-readable why, and
		// the admin surface reading this row has nothing else to go on.
		return errors.NewValidationError("removal_code", "must not be empty")
	}
	if _, err := r.db.ExecContext(ctx, `
		INSERT INTO object_moderation (
			at_uri, ap_id, community_did, removed_at, removal_code, removal_reason)
		VALUES ($1, $2, $3, now(), $4, $5)
		ON CONFLICT (at_uri) DO UPDATE SET
			ap_id = EXCLUDED.ap_id,
			community_did = EXCLUDED.community_did,
			-- COALESCE, as for a lock: a re-delivered Delete is the same removal
			-- arriving twice, and re-stamping would walk the moderators' timestamp
			-- forward every time Lemmy retried.
			removed_at = COALESCE(object_moderation.removed_at, EXCLUDED.removed_at),
			removal_code = EXCLUDED.removal_code,
			removal_reason = EXCLUDED.removal_reason,
			updated_at = now()`,
		object.ATURI, object.APID, object.CommunityDID, code, reason); err != nil {
		return fmt.Errorf("record removal of %q: %w", object.ATURI, err)
	}
	return nil
}

func (r *postgresAPObjects) ClearRemoval(ctx context.Context, atURI, communityDID string) error {
	if atURI == "" {
		return errors.NewValidationError("at_uri", "must not be empty")
	}
	if communityDID == "" {
		return errors.NewValidationError("community_did", "must not be empty")
	}
	// The code and the reason go with it: they describe a decision that no
	// longer stands, and leaving them behind would let an admin surface read a
	// live reason off a lifted removal.
	if _, err := r.db.ExecContext(ctx, `
		UPDATE object_moderation
		   SET removed_at = NULL, removal_code = '', removal_reason = '', updated_at = now()
		 WHERE at_uri = $1 AND community_did = $2`,
		atURI, communityDID); err != nil {
		return fmt.Errorf("clear removal of %q: %w", atURI, err)
	}
	return nil
}

func (r *postgresAPObjects) LockedAmong(ctx context.Context, atURIs ...string) (string, error) {
	// The caller names a thread — a parent and a root, sometimes the same object
	// twice — so the empties and duplicates it may hold are filtered here rather
	// than at every call site.
	candidates := make([]string, 0, len(atURIs))
	for _, atURI := range atURIs {
		if atURI != "" && !containsString(candidates, atURI) {
			candidates = append(candidates, atURI)
		}
	}
	if len(candidates) == 0 {
		return "", nil
	}
	var locked string
	err := r.db.QueryRowContext(ctx, `
		SELECT at_uri FROM object_moderation
		 WHERE at_uri = ANY($1) AND locked_at IS NOT NULL
		 LIMIT 1`, pq.Array(candidates)).Scan(&locked)
	if stderrors.Is(err, sql.ErrNoRows) {
		// No row at all: nobody has ever moderated any of these objects, which is
		// true of almost every object. Not a NotFound error — the question asked
		// is "is anything here locked", and "no" is a complete answer to it.
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read lock state for %v: %w", candidates, err)
	}
	return locked, nil
}

func (r *postgresAPObjects) CommunityHoldsAnyLock(ctx context.Context, communityDID string) (bool, error) {
	if communityDID == "" {
		return false, errors.NewValidationError("community_did", "must not be empty")
	}
	var held bool
	// EXISTS, not a count: the caller only asks whether there is anything at all
	// that could have locked a thread it could not name.
	if err := r.db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM object_moderation
			 WHERE community_did = $1 AND locked_at IS NOT NULL)`,
		communityDID).Scan(&held); err != nil {
		return false, fmt.Errorf("read standing locks of %q: %w", communityDID, err)
	}
	return held, nil
}

// containsString reports whether the slice already holds s. The candidate sets
// here are two or three elements, so a scan beats building a map.
func containsString(values []string, s string) bool {
	for _, v := range values {
		if v == s {
			return true
		}
	}
	return false
}
