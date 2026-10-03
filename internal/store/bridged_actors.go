package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/lib/pq"

	"tidepool/internal/errors"
)

type postgresBridgedActors struct {
	db *sql.DB
}

// NewBridgedActors creates the postgres-backed bridged_actors repository.
func NewBridgedActors(db *sql.DB) BridgedActors {
	return &postgresBridgedActors{db: db}
}

const bridgedActorColumns = `
	id, ap_actor_id, actor_type, did, COALESCE(handle, ''),
	signing_key, consent_state, profile_synced_at, created_at`

func (r *postgresBridgedActors) UpsertActor(ctx context.Context, actor BridgedActor) (*BridgedActor, error) {
	if err := validateBridgedActor(&actor); err != nil {
		return nil, err
	}

	// On conflict:
	//   - handle and signing_key are sticky: an upsert built from AP data
	//     alone (profile refresh) carries neither, and must never clobber
	//     escrowed values with NULL. Non-empty new values do overwrite.
	//   - did, actor_type, consent_state, and created_at never change
	//     (identity is immutable once minted; consent only moves through
	//     SetConsentState).
	//   - the DO UPDATE's WHERE freezes tombstoned rows entirely and
	//     refuses identity drift; both surface as "no row returned" and
	//     are disambiguated by re-reading the stored row below.
	query := `
		INSERT INTO bridged_actors (
			ap_actor_id, actor_type, did, handle,
			signing_key, consent_state
		) VALUES ($1, $2, $3, NULLIF($4, ''), $5, $6)
		ON CONFLICT (ap_actor_id) DO UPDATE SET
			handle = COALESCE(NULLIF(EXCLUDED.handle, ''), bridged_actors.handle),
			signing_key = COALESCE(EXCLUDED.signing_key, bridged_actors.signing_key)
		WHERE bridged_actors.consent_state <> 'deleted'
		  AND bridged_actors.did = EXCLUDED.did
		  AND bridged_actors.actor_type = EXCLUDED.actor_type
		RETURNING` + bridgedActorColumns

	row := r.db.QueryRowContext(ctx, query,
		actor.APActorID, string(actor.ActorType), actor.DID, actor.Handle,
		actor.SigningKeyEncrypted, string(actor.ConsentState),
	)
	stored, err := scanBridgedActor(row)
	if stderrors.Is(err, sql.ErrNoRows) {
		// The DO UPDATE's WHERE excluded the existing row: either the
		// caller's identity fields diverge from the stored ones (conflict)
		// or the actor is tombstoned (return the frozen row unchanged).
		existing, getErr := r.GetByAPActorID(ctx, actor.APActorID)
		if getErr != nil {
			return nil, fmt.Errorf("upsert bridged_actor %q: recheck after excluded update: %w", actor.APActorID, getErr)
		}
		if existing.DID != actor.DID {
			return nil, errors.NewConflictError("bridged_actor", "did", actor.DID)
		}
		if existing.ActorType != actor.ActorType {
			return nil, errors.NewConflictError("bridged_actor", "actor_type", string(actor.ActorType))
		}
		return existing, nil
	}
	if err != nil {
		if constraint, ok := uniqueViolation(err); ok {
			switch constraint {
			case "bridged_actors_did_key":
				return nil, errors.NewConflictError("bridged_actor", "did", actor.DID)
			case "bridged_actors_handle_key":
				return nil, errors.NewConflictError("bridged_actor", "handle", actor.Handle)
			}
		}
		return nil, fmt.Errorf("upsert bridged_actor %q: %w", actor.APActorID, err)
	}
	return stored, nil
}

func (r *postgresBridgedActors) GetByAPActorID(ctx context.Context, apActorID string) (*BridgedActor, error) {
	query := `SELECT` + bridgedActorColumns + ` FROM bridged_actors WHERE ap_actor_id = $1`
	actor, err := scanBridgedActor(r.db.QueryRowContext(ctx, query, apActorID))
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFoundError("bridged_actor", apActorID)
		}
		return nil, fmt.Errorf("get bridged_actor by ap_actor_id %q: %w", apActorID, err)
	}
	return actor, nil
}

func (r *postgresBridgedActors) GetByDID(ctx context.Context, did string) (*BridgedActor, error) {
	query := `SELECT` + bridgedActorColumns + ` FROM bridged_actors WHERE did = $1`
	actor, err := scanBridgedActor(r.db.QueryRowContext(ctx, query, did))
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFoundError("bridged_actor", did)
		}
		return nil, fmt.Errorf("get bridged_actor by did %q: %w", did, err)
	}
	return actor, nil
}

func (r *postgresBridgedActors) GetByHandle(ctx context.Context, handle string) (*BridgedActor, error) {
	query := `SELECT` + bridgedActorColumns + ` FROM bridged_actors WHERE handle = $1`
	actor, err := scanBridgedActor(r.db.QueryRowContext(ctx, query, handle))
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFoundError("bridged_actor", handle)
		}
		return nil, fmt.Errorf("get bridged_actor by handle %q: %w", handle, err)
	}
	return actor, nil
}

func (r *postgresBridgedActors) SetConsentState(ctx context.Context, apActorID string, state ConsentState) error {
	if !state.Valid() {
		return errors.NewValidationError("consent_state", fmt.Sprintf("unknown state %q", state))
	}

	// Deleted is terminal: the update's WHERE refuses to move an actor out
	// of it (setting deleted on an already-deleted actor stays an
	// idempotent no-op success). One statement reads the current state and
	// attempts the update atomically, so "not found" and "terminal state"
	// are distinguished without a racy follow-up query.
	query := `
		WITH current AS (
			SELECT consent_state FROM bridged_actors WHERE ap_actor_id = $1
		), attempted AS (
			UPDATE bridged_actors
			SET consent_state = $2
			WHERE ap_actor_id = $1 AND (consent_state <> 'deleted' OR $2 = 'deleted')
			RETURNING 1
		)
		SELECT (SELECT consent_state FROM current),
		       EXISTS (SELECT 1 FROM attempted)`

	var currentState sql.NullString
	var updated bool
	err := r.db.QueryRowContext(ctx, query, apActorID, string(state)).Scan(&currentState, &updated)
	if err != nil {
		return fmt.Errorf("set consent_state for %q: %w", apActorID, err)
	}
	if !currentState.Valid {
		return errors.NewNotFoundError("bridged_actor", apActorID)
	}
	if !updated {
		return errors.NewValidationError("consent_state",
			fmt.Sprintf("cannot transition out of terminal state %q", currentState.String))
	}
	return nil
}

func (r *postgresBridgedActors) MarkProfileSynced(ctx context.Context, apActorID string, syncedAt time.Time) error {
	query := `UPDATE bridged_actors SET profile_synced_at = $2 WHERE ap_actor_id = $1`
	result, err := r.db.ExecContext(ctx, query, apActorID, syncedAt)
	if err != nil {
		return fmt.Errorf("mark profile synced for %q: %w", apActorID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("mark profile synced for %q: rows affected: %w", apActorID, err)
	}
	if affected == 0 {
		return errors.NewNotFoundError("bridged_actor", apActorID)
	}
	return nil
}

func validateBridgedActor(actor *BridgedActor) error {
	if actor.APActorID == "" {
		return errors.NewValidationError("ap_actor_id", "must not be empty")
	}
	if !actor.ActorType.Valid() {
		return errors.NewValidationError("actor_type",
			fmt.Sprintf("must be %q or %q, got %q", ActorTypePerson, ActorTypeGroup, actor.ActorType))
	}
	if _, err := syntax.ParseDID(actor.DID); err != nil {
		return errors.NewValidationError("did", err.Error())
	}
	if actor.Handle != "" {
		if _, err := syntax.ParseHandle(actor.Handle); err != nil {
			return errors.NewValidationError("handle", err.Error())
		}
	}
	// Consent must be stated explicitly: the zero value failing open to
	// "consented" would be a consent bug, so "" is rejected outright.
	if !actor.ConsentState.Valid() {
		return errors.NewValidationError("consent_state",
			fmt.Sprintf("must be stated explicitly (%q, %q, or %q), got %q",
				ConsentStateOK, ConsentStateNoBridge, ConsentStateDeleted, actor.ConsentState))
	}
	return nil
}

func scanBridgedActor(row rowScanner) (*BridgedActor, error) {
	var actor BridgedActor
	var actorType, consentState string
	err := row.Scan(
		&actor.ID, &actor.APActorID, &actorType, &actor.DID, &actor.Handle,
		&actor.SigningKeyEncrypted, &consentState,
		&actor.ProfileSyncedAt, &actor.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	actor.ActorType = ActorType(actorType)
	actor.ConsentState = ConsentState(consentState)
	return &actor, nil
}

// ListInstanceLabels returns each instance label under zoneRoot and whether it
// has at least one actor that has not been deleted.
func (r *postgresBridgedActors) ListInstanceLabels(ctx context.Context, zoneRoot string) ([]InstanceLabel, error) {
	root := normalizeZoneRoot(zoneRoot)
	query := `
		SELECT split_part(lower(handle), '.', 2) AS label,
		       bool_or(consent_state <> $2) AS has_live_actor
		FROM bridged_actors
		WHERE ` + instanceHandleScope + `
		GROUP BY label
		ORDER BY label`
	rows, err := r.db.QueryContext(ctx, query, root, string(ConsentStateDeleted))
	if err != nil {
		return nil, fmt.Errorf("list instance labels under %q: %w", root, err)
	}
	defer rows.Close()
	var labels []InstanceLabel
	for rows.Next() {
		var label InstanceLabel
		if err := rows.Scan(&label.Label, &label.HasLiveActor); err != nil {
			return nil, fmt.Errorf("scan instance label under %q: %w", root, err)
		}
		labels = append(labels, label)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read instance labels under %q: %w", root, err)
	}
	return labels, nil
}

// instanceHandleScope is shared by all label queries so the suffix and exact
// handle depth agree with ListInstanceLabels even for multi-label zone roots.
const instanceHandleScope = `right(lower(handle), length($1) + 1) = '.' || $1
	AND cardinality(string_to_array(lower(handle), '.')) = cardinality(string_to_array($1, '.')) + 2`

func normalizeZoneRoot(zoneRoot string) string {
	return strings.TrimSuffix(strings.ToLower(strings.TrimSpace(zoneRoot)), ".")
}

func (r *postgresBridgedActors) ListLabelContributions(ctx context.Context, zoneRoot, label string, cutoff time.Time, after ContributionCursor, limit int) ([]Contribution, error) {
	root := normalizeZoneRoot(zoneRoot)
	// The zero cursor has no timestamp in Postgres; NULL bypasses the keyset
	// predicate only for the first page.
	var cursorTime any
	if !after.IndexedAt.IsZero() {
		cursorTime = after.IndexedAt
	}
	query := `
		SELECT object.at_uri, object.collection, community.did, object.indexed_at, object.id,
		       thread_root.at_uri, thread_root.collection,
		       CASE WHEN thread_root.collection = 'social.coves.community.post'
		            THEN thread_root.did ELSE NULLIF(thread_root.community_did, '') END
		FROM bridged_actors
		JOIN ap_objects AS object ON object.author_did = bridged_actors.did
		LEFT JOIN ap_objects AS thread_root ON thread_root.at_uri = object.thread_root_at_uri
		JOIN communities AS community ON community.did =
			CASE
				WHEN object.collection = 'social.coves.community.post' THEN object.did
				WHEN object.collection = 'social.coves.community.comment' THEN
					COALESCE(NULLIF(object.community_did, ''),
						CASE WHEN thread_root.collection = 'social.coves.community.post'
							THEN thread_root.did ELSE NULLIF(thread_root.community_did, '') END)
				ELSE NULLIF(object.community_did, '')
			END
			AND community.follow_state = 'accepted'
		WHERE ` + instanceHandleScope + `
		  AND split_part(lower(handle), '.', 2) = $3
		  AND bridged_actors.consent_state <> $2
		  AND object.deleted_at IS NULL
		  AND object.arrival = 'community_announced'
		  -- A comment counts only while its thread root stands: removing a thread
		  -- hides its comments without a per-comment delete. A root that was never
		  -- mapped cannot be shown to stand. The caller checks a postv2 root's
		  -- acceptance, which this table does not record.
		  AND (object.collection <> 'social.coves.community.comment'
		       OR (thread_root.id IS NOT NULL AND thread_root.deleted_at IS NULL))
		  AND object.collection IN ('social.coves.community.post', 'social.coves.community.postv2', 'social.coves.community.comment')
		  AND object.indexed_at <= $4
		  AND ($5::timestamptz IS NULL OR (object.indexed_at, object.id) > ($5::timestamptz, $6))
		ORDER BY object.indexed_at, object.id
		LIMIT $7`
	rows, err := r.db.QueryContext(ctx, query, root, string(ConsentStateDeleted), label, cutoff, cursorTime, after.ID, limit)
	if err != nil {
		return nil, fmt.Errorf("list contributions for label %q under %q: %w", label, root, err)
	}
	defer rows.Close()
	var contributions []Contribution
	for rows.Next() {
		var contribution Contribution
		var rootATURI, rootCollection, rootCommunityDID sql.NullString
		if err := rows.Scan(&contribution.ATURI, &contribution.Collection, &contribution.CommunityDID, &contribution.IndexedAt, &contribution.ID,
			&rootATURI, &rootCollection, &rootCommunityDID); err != nil {
			return nil, fmt.Errorf("scan contributions for label %q under %q: %w", label, root, err)
		}
		contribution.RootATURI, contribution.RootCollection, contribution.RootCommunityDID =
			rootATURI.String, rootCollection.String, rootCommunityDID.String
		contributions = append(contributions, contribution)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read contributions for label %q under %q: %w", label, root, err)
	}
	return contributions, nil
}

func (r *postgresBridgedActors) ListOutrightQualifiedLabels(ctx context.Context, zoneRoot string, labels []string, grandfatherCutoff time.Time) ([]OutrightQualification, error) {
	if len(labels) == 0 {
		return nil, nil
	}
	root := normalizeZoneRoot(zoneRoot)
	query := `
		SELECT split_part(lower(handle), '.', 2) AS label, MIN(bridged_actors.created_at)
		FROM bridged_actors
		WHERE ` + instanceHandleScope + `
		  AND bridged_actors.consent_state <> $2
		  AND split_part(lower(handle), '.', 2) = ANY($3::text[])
		  AND (bridged_actors.created_at < $4 OR EXISTS (
			SELECT 1 FROM communities
			WHERE communities.did = bridged_actors.did AND communities.follow_state = 'accepted'
		  ))
		GROUP BY label
		ORDER BY label`
	rows, err := r.db.QueryContext(ctx, query, root, string(ConsentStateDeleted), pq.Array(labels), grandfatherCutoff)
	if err != nil {
		return nil, fmt.Errorf("list outright qualified labels under %q: %w", root, err)
	}
	defer rows.Close()
	var qualified []OutrightQualification
	for rows.Next() {
		var entry OutrightQualification
		if err := rows.Scan(&entry.Label, &entry.QualifiedAt); err != nil {
			return nil, fmt.Errorf("scan outright qualified labels under %q: %w", root, err)
		}
		qualified = append(qualified, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read outright qualified labels under %q: %w", root, err)
	}
	return qualified, nil
}
