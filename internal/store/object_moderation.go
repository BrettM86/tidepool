package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"slices"

	"github.com/lib/pq"

	"tidepool/internal/errors"
)

// postgresObjectModeration is the object_moderation repository: the moderation
// state the bridge itself owns, described on the ObjectModeration interface and
// in migration 025.
//
// It is its own repository over its own table, and postgresAPObjects EMBEDS it
// so a holder of the concrete mapping store can be type-asserted to
// ObjectModeration (see ingest.NewHandler's default) — a wiring convenience that
// deliberately does NOT widen the APObjects interface, because a store held for
// strongRef resolution must not carry moderation mutators.
type postgresObjectModeration struct {
	db *sql.DB
}

// NewObjectModeration creates the postgres-backed object_moderation repository.
func NewObjectModeration(db *sql.DB) ObjectModeration {
	return &postgresObjectModeration{db: db}
}

func (r *postgresObjectModeration) SetLock(ctx context.Context, object ModeratedObject, locked bool) error {
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

func (r *postgresObjectModeration) SetRemoval(ctx context.Context, object ModeratedObject, code, reason string) error {
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

func (r *postgresObjectModeration) ClearRemoval(ctx context.Context, atURI, communityDID string) (bool, error) {
	if atURI == "" {
		return false, errors.NewValidationError("at_uri", "must not be empty")
	}
	if communityDID == "" {
		return false, errors.NewValidationError("community_did", "must not be empty")
	}
	// The code and the reason go with it: they describe a decision that no
	// longer stands, and leaving them behind would let an admin surface read a
	// live reason off a lifted removal.
	//
	// `removed_at IS NOT NULL` is what makes the row count meaningful: without
	// it, an UPDATE that blanked already-blank columns would report one row
	// affected and the caller would log a reversal that reversed nothing.
	result, err := r.db.ExecContext(ctx, `
		UPDATE object_moderation
		   SET removed_at = NULL, removal_code = '', removal_reason = '', updated_at = now()
		 WHERE at_uri = $1 AND community_did = $2 AND removed_at IS NOT NULL`,
		atURI, communityDID)
	if err != nil {
		return false, fmt.Errorf("clear removal of %q: %w", atURI, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("clear removal of %q: rows affected: %w", atURI, err)
	}
	return affected > 0, nil
}

func (r *postgresObjectModeration) LockedAmong(ctx context.Context, atURIs ...string) (string, error) {
	// The caller names a thread — a parent and a root, sometimes the same object
	// twice — so the empties and duplicates it may hold are filtered here rather
	// than at every call site.
	candidates := make([]string, 0, len(atURIs))
	for _, atURI := range atURIs {
		if atURI != "" && !slices.Contains(candidates, atURI) {
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

func (r *postgresObjectModeration) CommunityHoldsAnyLock(ctx context.Context, communityDID string) (bool, error) {
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
