package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"

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
