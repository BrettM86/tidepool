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
	COALESCE(preferred_username, ''), signing_key, consent_state, profile_synced_at, created_at,
	COALESCE(pending_handle, ''), COALESCE(previous_handle, ''), handle_migrated_at`

func (r *postgresBridgedActors) UpsertActor(ctx context.Context, actor BridgedActor) (*BridgedActor, error) {
	return r.UpsertActorOnInsert(ctx, actor, nil)
}

// UpsertActorOnInsert runs onInsert only for an inserted actor, after its
// handle claim is written and before the enclosing transaction commits.
func (r *postgresBridgedActors) UpsertActorOnInsert(ctx context.Context, actor BridgedActor, onInsert func(context.Context, *sql.Tx, *BridgedActor) error) (*BridgedActor, error) {
	if err := validateBridgedActor(&actor); err != nil {
		return nil, err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("upsert bridged_actor: begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Lock an existing row so its prior handle is stable until the claim
	// transfer commits. A missing row gets its id from the upsert below.
	var previousHandle string
	err = tx.QueryRowContext(ctx, `SELECT COALESCE(handle, '') FROM bridged_actors WHERE ap_actor_id = $1 FOR UPDATE`, actor.APActorID).Scan(&previousHandle)
	if err != nil && !stderrors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("upsert bridged_actor: read prior handle: %w", err)
	}

	// On conflict:
	//   - handle and signing_key are sticky: an upsert built from AP data
	//     alone (profile refresh) carries neither, and must never clobber
	//     escrowed values with NULL. Non-empty new values do overwrite.
	//   - preferred_username is sticky when omitted; a fetched non-empty
	//     preferredUsername replaces the previous value exactly as received.
	//   - did, actor_type, consent_state, and created_at never change
	//     (identity is immutable once minted; consent only moves through
	//     SetConsentState).
	//   - the DO UPDATE's WHERE freezes tombstoned rows entirely and
	//     refuses identity drift; both surface as "no row returned" and
	//     are disambiguated by re-reading the stored row below.
	// xmax is zero on the inserted tuple and nonzero on ON CONFLICT UPDATE;
	// the earlier SELECT cannot identify an insert if another writer races it.
	query := `
		INSERT INTO bridged_actors (
			ap_actor_id, actor_type, did, handle,
			signing_key, consent_state, preferred_username
		) VALUES ($1, $2, $3, NULLIF($4, ''), $5, $6, NULLIF($7, ''))
		ON CONFLICT (ap_actor_id) DO UPDATE SET
			handle = COALESCE(NULLIF(EXCLUDED.handle, ''), bridged_actors.handle),
			signing_key = COALESCE(EXCLUDED.signing_key, bridged_actors.signing_key),
			preferred_username = COALESCE(EXCLUDED.preferred_username, bridged_actors.preferred_username)
		WHERE bridged_actors.consent_state <> 'deleted'
		  AND bridged_actors.did = EXCLUDED.did
		  AND bridged_actors.actor_type = EXCLUDED.actor_type
		RETURNING` + bridgedActorColumns + `, (xmax = 0)`

	row := tx.QueryRowContext(ctx, query,
		actor.APActorID, string(actor.ActorType), actor.DID, actor.Handle,
		actor.SigningKeyEncrypted, string(actor.ConsentState), actor.PreferredUsername,
	)
	stored, inserted, err := scanBridgedActorRow(row, true)
	if stderrors.Is(err, sql.ErrNoRows) {
		// The DO UPDATE's WHERE excluded the existing row: either the
		// caller's identity fields diverge from the stored ones (conflict)
		// or the actor is tombstoned (return the frozen row unchanged).
		existing, getErr := scanBridgedActor(tx.QueryRowContext(ctx,
			`SELECT`+bridgedActorColumns+` FROM bridged_actors WHERE ap_actor_id = $1`, actor.APActorID))
		if getErr != nil {
			return nil, fmt.Errorf("upsert bridged_actor %q: recheck after excluded update: %w", actor.APActorID, getErr)
		}
		if existing.DID != actor.DID {
			return nil, errors.NewConflictError("bridged_actor", "did", actor.DID)
		}
		if existing.ActorType != actor.ActorType {
			return nil, errors.NewConflictError("bridged_actor", "actor_type", string(actor.ActorType))
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("upsert bridged_actor: commit frozen row: %w", err)
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
	if stored.Handle != "" && stored.Handle != previousHandle {
		_, err = tx.ExecContext(ctx, `INSERT INTO handle_claims (handle, actor_id) VALUES ($1, $2)`, stored.Handle, stored.ID)
		if err != nil {
			if constraint, ok := uniqueViolation(err); ok && constraint == "handle_claims_pkey" {
				return nil, errors.NewConflictError("bridged_actor", "handle", stored.Handle)
			}
			return nil, fmt.Errorf("upsert bridged_actor: claim handle: %w", err)
		}
		if previousHandle != "" {
			if _, err := tx.ExecContext(ctx, `DELETE FROM handle_claims WHERE handle = $1 AND actor_id = $2`, previousHandle, stored.ID); err != nil {
				return nil, fmt.Errorf("upsert bridged_actor: release prior handle: %w", err)
			}
		}
	}
	if inserted && onInsert != nil {
		if err := onInsert(ctx, tx, stored); err != nil {
			return nil, fmt.Errorf("upsert bridged_actor: on insert: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("upsert bridged_actor: commit row and claim: %w", err)
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

func (r *postgresBridgedActors) GetByHandleOrPending(ctx context.Context, handle string) (*BridgedActor, error) {
	query := `SELECT` + bridgedActorColumns + ` FROM bridged_actors WHERE handle = $1 OR pending_handle = $1`
	actor, err := scanBridgedActor(r.db.QueryRowContext(ctx, query, handle))
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFoundError("bridged_actor", handle)
		}
		return nil, fmt.Errorf("get bridged_actor by handle or pending handle %q: %w", handle, err)
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

func (r *postgresBridgedActors) MarkProfileSynced(ctx context.Context, apActorID string, syncedAt time.Time, preferredUsername string) error {
	query := `UPDATE bridged_actors SET profile_synced_at = $2,
		preferred_username = COALESCE(NULLIF($3, ''), preferred_username)
		WHERE ap_actor_id = $1`
	result, err := r.db.ExecContext(ctx, query, apActorID, syncedAt, preferredUsername)
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

func (r *postgresBridgedActors) SetPreferredUsernameByDID(ctx context.Context, did, preferredUsername string) error {
	if preferredUsername == "" || strings.ContainsRune(preferredUsername, 0) {
		return errors.NewValidationError("preferred_username", "must not be empty or contain a NUL byte")
	}
	result, err := r.db.ExecContext(ctx,
		`UPDATE bridged_actors SET preferred_username = $2
		WHERE did = $1 AND consent_state <> 'deleted'`, did, preferredUsername)
	if err != nil {
		return fmt.Errorf("set preferred username for %s: %w", did, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("set preferred username for %s: rows affected: %w", did, err)
	}
	if affected == 0 {
		return errors.NewNotFoundError("bridged_actor", did)
	}
	return nil
}

// DeleteActorRow runs onDelete only when a row was deleted, in the delete's
// transaction, so the row, its cascaded handle claim and whatever onDelete
// writes commit or roll back together.
func (r *postgresBridgedActors) DeleteActorRow(ctx context.Context, apActorID, did string, onDelete func(context.Context, *sql.Tx) error) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("delete bridged actor row for %q: begin transaction: %w", apActorID, err)
	}
	defer tx.Rollback()

	query := `DELETE FROM bridged_actors WHERE ap_actor_id = $1 AND did = $2`
	result, err := tx.ExecContext(ctx, query, apActorID, did)
	if err != nil {
		return fmt.Errorf("delete bridged actor row for %q: %w", apActorID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete bridged actor row for %q: rows affected: %w", apActorID, err)
	}
	if affected == 0 {
		return errors.NewNotFoundError("bridged_actor", apActorID)
	}
	if onDelete != nil {
		if err := onDelete(ctx, tx); err != nil {
			return fmt.Errorf("delete bridged actor row for %q: on delete: %w", apActorID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("delete bridged actor row for %q: commit: %w", apActorID, err)
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
	// Postgres TEXT cannot hold NUL; the name is stored exactly as received or
	// not at all.
	if strings.ContainsRune(actor.PreferredUsername, 0) {
		return errors.NewValidationError("preferred_username", "must not contain a NUL byte")
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
	actor, _, err := scanBridgedActorRow(row, false)
	return actor, err
}

func scanBridgedActorRow(row rowScanner, includeInserted bool) (*BridgedActor, bool, error) {
	var actor BridgedActor
	var actorType, consentState string
	var inserted bool
	fields := []any{
		&actor.ID, &actor.APActorID, &actorType, &actor.DID, &actor.Handle,
		&actor.PreferredUsername, &actor.SigningKeyEncrypted, &consentState,
		&actor.ProfileSyncedAt, &actor.CreatedAt, &actor.PendingHandle,
		&actor.PreviousHandle, &actor.HandleMigratedAt,
	}
	if includeInserted {
		fields = append(fields, &inserted)
	}
	err := row.Scan(fields...)
	if err != nil {
		return nil, false, err
	}
	actor.ActorType = ActorType(actorType)
	actor.ConsentState = ConsentState(consentState)
	return &actor, inserted, nil
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

// HandleTaken checks both claims and legacy actor handles.
func (r *postgresBridgedActors) HandleTaken(ctx context.Context, handle string) (bool, error) {
	var taken bool
	err := r.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM handle_claims WHERE handle = $1)
		OR EXISTS (SELECT 1 FROM bridged_actors WHERE handle = $1)`, handle).Scan(&taken)
	if err != nil {
		return false, fmt.Errorf("check handle availability: %w", err)
	}
	return taken, nil
}

func (r *postgresBridgedActors) SetPendingHandle(ctx context.Context, did, handle string) error {
	if _, err := syntax.ParseHandle(handle); err != nil {
		return errors.NewValidationError("handle", err.Error())
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("set pending handle for %s: begin transaction: %w", did, err)
	}
	defer tx.Rollback()

	var actorID int64
	var consentState ConsentState
	var currentHandle string
	if err := tx.QueryRowContext(ctx, `SELECT id, consent_state, COALESCE(handle, '')
		FROM bridged_actors WHERE did = $1 FOR UPDATE`, did).Scan(&actorID, &consentState, &currentHandle); err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return errors.NewNotFoundError("bridged_actor", did)
		}
		return fmt.Errorf("set pending handle for %s: lock actor: %w", did, err)
	}
	if consentState == ConsentStateDeleted {
		return errors.NewTombstonedError("bridged_actor", did)
	}
	if handle == currentHandle {
		return errors.NewValidationError("pending_handle", "must differ from the current handle")
	}
	var holderID int64
	err = tx.QueryRowContext(ctx, `SELECT id FROM bridged_actors WHERE handle = $1 AND id <> $2 FOR UPDATE`, handle, actorID).Scan(&holderID)
	if err == nil {
		return errors.NewConflictError("bridged_actor", "handle", handle)
	}
	if !stderrors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("set pending handle for %s: check assigned handle: %w", did, err)
	}
	err = tx.QueryRowContext(ctx, `INSERT INTO handle_claims (handle, actor_id) VALUES ($1, $2)
		ON CONFLICT (handle) DO UPDATE SET actor_id = handle_claims.actor_id
		WHERE handle_claims.actor_id = EXCLUDED.actor_id RETURNING actor_id`, handle, actorID).Scan(&holderID)
	if stderrors.Is(err, sql.ErrNoRows) {
		return errors.NewConflictError("bridged_actor", "handle", handle)
	}
	if err != nil {
		return fmt.Errorf("set pending handle for %s: claim %q: %w", did, handle, err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE bridged_actors SET pending_handle = $1, pending_revert_submitted_at = NULL,
		handle_migration_error = NULL, handle_migration_message = NULL WHERE id = $2`, handle, actorID); err != nil {
		if constraint, ok := uniqueViolation(err); ok && constraint == "bridged_actors_pending_handle_key" {
			return errors.NewConflictError("bridged_actor", "handle", handle)
		}
		return fmt.Errorf("set pending handle for %s: update actor: %w", did, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("set pending handle for %s: commit: %w", did, err)
	}
	return nil
}

// HandleMigrationErrorCode is a permanent rename failure recorded on an actor.
type HandleMigrationErrorCode string

const (
	HandleMigrationPLCRejected         HandleMigrationErrorCode = "plc_rejected"
	HandleMigrationPLCNotControlled    HandleMigrationErrorCode = "plc_not_controlled"
	HandleMigrationHandleInvalid       HandleMigrationErrorCode = "handle_invalid"
	HandleMigrationUsernameUnavailable HandleMigrationErrorCode = "username_unavailable"
)

func (r *postgresBridgedActors) RecordHandleMigrationError(ctx context.Context, did string, code HandleMigrationErrorCode, message string) error {
	switch code {
	case HandleMigrationPLCRejected, HandleMigrationPLCNotControlled, HandleMigrationHandleInvalid, HandleMigrationUsernameUnavailable:
	default:
		return errors.NewValidationError("handle_migration_error", "unknown error code")
	}
	result, err := r.db.ExecContext(ctx, `UPDATE bridged_actors SET handle_migration_error = $1,
		handle_migration_message = $2 WHERE did = $3`, code, message, did)
	if err != nil {
		return fmt.Errorf("record handle migration error for %s: %w", did, err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("record handle migration error for %s: count rows: %w", did, err)
	}
	if count == 0 {
		return errors.NewNotFoundError("bridged_actor", did)
	}
	return nil
}

func (r *postgresBridgedActors) FinalizeHandleRename(ctx context.Context, did, expectedPending string, onFinalize func(ctx context.Context, tx *sql.Tx, renamed *BridgedActor) error) (*BridgedActor, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("finalize handle rename for %s: begin transaction: %w", did, err)
	}
	defer tx.Rollback()

	var actorID int64
	var consentState ConsentState
	var oldHandle, pendingHandle string
	err = tx.QueryRowContext(ctx, `SELECT id, consent_state, COALESCE(handle, ''), COALESCE(pending_handle, '')
		FROM bridged_actors WHERE did = $1 FOR UPDATE`, did).Scan(&actorID, &consentState, &oldHandle, &pendingHandle)
	if stderrors.Is(err, sql.ErrNoRows) {
		return nil, errors.NewNotFoundError("bridged_actor", did)
	}
	if err != nil {
		return nil, fmt.Errorf("finalize handle rename for %s: lock actor: %w", did, err)
	}
	if consentState == ConsentStateDeleted {
		return nil, errors.NewTombstonedError("bridged_actor", did)
	}
	if pendingHandle == "" {
		return nil, errors.NewValidationError("pending_handle", "must not be empty")
	}
	if pendingHandle != expectedPending {
		return nil, fmt.Errorf("finalize handle rename for %s: %w", did, stalePendingHandleError(expectedPending))
	}
	if pendingHandle == oldHandle {
		return nil, errors.NewValidationError("pending_handle", "must differ from the current handle")
	}
	query := `UPDATE bridged_actors SET previous_handle = handle, handle = pending_handle,
		pending_handle = NULL, pending_revert_submitted_at = NULL, handle_migrated_at = now(),
		handle_migration_error = NULL, handle_migration_message = NULL WHERE id = $1 RETURNING` + bridgedActorColumns
	renamed, err := scanBridgedActor(tx.QueryRowContext(ctx, query, actorID))
	if err != nil {
		return nil, fmt.Errorf("finalize handle rename for %s: update actor: %w", did, err)
	}
	if oldHandle != "" {
		if _, err := tx.ExecContext(ctx, `DELETE FROM handle_claims WHERE handle = $1 AND actor_id = $2`, oldHandle, actorID); err != nil {
			return nil, fmt.Errorf("finalize handle rename for %s: release old claim: %w", did, err)
		}
	}
	if onFinalize != nil {
		if err := onFinalize(ctx, tx, renamed); err != nil {
			return nil, fmt.Errorf("finalize handle rename for %s: on finalize: %w", did, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("finalize handle rename for %s: commit: %w", did, err)
	}
	return renamed, nil
}

func (r *postgresBridgedActors) IsForwardRenameCandidate(ctx context.Context, did, bridgeHostname, handleZoneRoot string) (bool, error) {
	bridge, zone := handleFormatSuffixes(bridgeHostname, handleZoneRoot)
	var candidate bool
	err := r.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM bridged_actors
		WHERE did = $3 AND consent_state <> 'deleted' AND `+oldFormatHandlePredicate("handle")+`)`,
		bridge, zone, did).Scan(&candidate)
	if err != nil {
		return false, fmt.Errorf("check forward rename candidate %s: %w", did, err)
	}
	return candidate, nil
}

func (r *postgresBridgedActors) ListForwardRenameCandidates(ctx context.Context, bridgeHostname, handleZoneRoot string) ([]string, error) {
	bridge, zone := handleFormatSuffixes(bridgeHostname, handleZoneRoot)
	rows, err := r.db.QueryContext(ctx, `SELECT did FROM bridged_actors
		WHERE consent_state <> 'deleted' AND `+oldFormatHandlePredicate("handle")+`
		ORDER BY id`, bridge, zone)
	if err != nil {
		return nil, fmt.Errorf("list forward rename candidates: %w", err)
	}
	defer rows.Close()
	var dids []string
	for rows.Next() {
		var did string
		if err := rows.Scan(&did); err != nil {
			return nil, fmt.Errorf("scan forward rename candidate: %w", err)
		}
		dids = append(dids, did)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read forward rename candidates: %w", err)
	}
	return dids, nil
}

func (r *postgresBridgedActors) ListLivePersonAPActorIDs(ctx context.Context) ([]string, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT ap_actor_id FROM bridged_actors
		WHERE actor_type = $1 AND consent_state <> $2
		ORDER BY id`, string(ActorTypePerson), string(ConsentStateDeleted))
	if err != nil {
		return nil, fmt.Errorf("list live person actors: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan live person actor: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read live person actors: %w", err)
	}
	return ids, nil
}

// These suffixes include the label boundary. Column names passed to the SQL
// predicate helpers below are fixed by the caller, never supplied by a request.
func handleFormatSuffixes(bridgeHostname, handleZoneRoot string) (string, string) {
	return "." + normalizeZoneRoot(bridgeHostname), "." + normalizeZoneRoot(handleZoneRoot)
}

func newFormatHandlePredicate(column string) string {
	return fmt.Sprintf("right(lower(%s), length($2)) = $2", column)
}

func oldFormatHandlePredicate(column string) string {
	return fmt.Sprintf("(right(lower(%s), length($1)) = $1 AND right(lower(%s), length($2)) <> $2)", column, column)
}

// Keep this predicate shared with Progress so the reported count describes
// precisely the same set as the list (before its optional DID filter).
func rollbackCandidatePredicate() string {
	return `(consent_state <> 'deleted' AND ((` + newFormatHandlePredicate("handle") +
		` AND ` + oldFormatHandlePredicate("previous_handle") + ` AND pending_handle IS NULL) OR ` +
		oldFormatHandlePredicate("pending_handle") + ` OR (` + oldFormatHandlePredicate("handle") +
		` AND ` + newFormatHandlePredicate("pending_handle") + `)))`
}

func (r *postgresBridgedActors) ListRollbackCandidates(ctx context.Context, bridgeHostname, handleZoneRoot string, dids []string) ([]string, error) {
	bridge, zone := handleFormatSuffixes(bridgeHostname, handleZoneRoot)
	rows, err := r.db.QueryContext(ctx, `SELECT did FROM bridged_actors WHERE `+rollbackCandidatePredicate()+`
		AND ($3::text[] IS NULL OR did = ANY($3::text[])) ORDER BY id`, bridge, zone, pq.Array(dids))
	if err != nil {
		return nil, fmt.Errorf("list rollback candidates: %w", err)
	}
	defer rows.Close()
	var candidates []string
	for rows.Next() {
		var did string
		if err := rows.Scan(&did); err != nil {
			return nil, fmt.Errorf("scan rollback candidate: %w", err)
		}
		candidates = append(candidates, did)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read rollback candidates: %w", err)
	}
	return candidates, nil
}

// stalePendingHandleError reports that pending_handle no longer holds the
// handle the caller expected, for example the one PLC was confirmed at.
func stalePendingHandleError(expectedPending string) error {
	return fmt.Errorf("pending handle is not %q: %w", expectedPending, errors.ErrAlreadyExists)
}

func (r *postgresBridgedActors) MarkPendingRevertSubmitted(ctx context.Context, did, expectedPending string) error {
	result, err := r.db.ExecContext(ctx, `UPDATE bridged_actors SET pending_revert_submitted_at = now()
		WHERE did = $1 AND pending_handle = $2 AND consent_state <> 'deleted'`, did, expectedPending)
	if err != nil {
		return fmt.Errorf("mark pending revert submitted for %s: %w", did, err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("mark pending revert submitted for %s: count rows: %w", did, err)
	}
	if count == 0 {
		return fmt.Errorf("mark pending revert submitted for %s: no live actor: %w", did, stalePendingHandleError(expectedPending))
	}
	return nil
}

func (r *postgresBridgedActors) AbandonPendingHandle(ctx context.Context, did, expectedPending string, onPLCReverted func(ctx context.Context, tx *sql.Tx, actor *BridgedActor) error) (*BridgedActor, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("abandon pending handle for %s: begin transaction: %w", did, err)
	}
	defer tx.Rollback()

	var actorID int64
	var consentState ConsentState
	var pendingHandle string
	var revertSubmitted bool
	err = tx.QueryRowContext(ctx, `SELECT id, consent_state, COALESCE(pending_handle, ''),
		pending_revert_submitted_at IS NOT NULL FROM bridged_actors WHERE did = $1 FOR UPDATE`, did).
		Scan(&actorID, &consentState, &pendingHandle, &revertSubmitted)
	if stderrors.Is(err, sql.ErrNoRows) {
		return nil, errors.NewNotFoundError("bridged_actor", did)
	}
	if err != nil {
		return nil, fmt.Errorf("abandon pending handle for %s: lock actor: %w", did, err)
	}
	if consentState == ConsentStateDeleted {
		return nil, errors.NewTombstonedError("bridged_actor", did)
	}
	if pendingHandle == "" || pendingHandle != expectedPending {
		return nil, fmt.Errorf("abandon pending handle for %s: %w", did, stalePendingHandleError(expectedPending))
	}
	updated, err := scanBridgedActor(tx.QueryRowContext(ctx, `UPDATE bridged_actors
		SET pending_handle = NULL, pending_revert_submitted_at = NULL,
		    handle_migration_error = NULL, handle_migration_message = NULL
		WHERE id = $1 RETURNING`+bridgedActorColumns, actorID))
	if err != nil {
		return nil, fmt.Errorf("abandon pending handle for %s: update actor: %w", did, err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM handle_claims WHERE handle = $1 AND actor_id = $2`, expectedPending, actorID); err != nil {
		return nil, fmt.Errorf("abandon pending handle for %s: release claim: %w", did, err)
	}
	if revertSubmitted && onPLCReverted != nil {
		if err := onPLCReverted(ctx, tx, updated); err != nil {
			return nil, fmt.Errorf("abandon pending handle for %s: on PLC reverted: %w", did, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("abandon pending handle for %s: commit: %w", did, err)
	}
	return updated, nil
}
