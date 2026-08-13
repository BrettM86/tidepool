package store

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"time"

	"tidepool/internal/errors"
)

// APActor is a Coves user's ActivityPub identity on a user origin: a Person
// actor keyed by the user's DID, whose RSA signing key is sealed under
// BRIDGE_KEK (task 13).
type APActor struct {
	// DID is the Coves user's atproto DID; it is the row's primary key.
	DID string
	// Kind is person or group. Group is RESERVED for Scope B: no code path
	// mints one yet, but the CHECK constraint admits it.
	Kind ActorType
	// ActorID is the FULL actor URL, origin included (decision 10: every
	// derived URL — webfinger href, keyId, signing identity — comes from
	// this stored value, never from AP_USER_ORIGIN at read time).
	ActorID string
	// NormalizedOrigin and LocalPart are the webfinger lookup key
	// (lowercased). They are UNIQUE TOGETHER, not globally: vanity origins
	// must be able to host the same local part.
	NormalizedOrigin string
	LocalPart        string
	// RSAKeySealed is the AP signing key sealed by identity.Custodian's RSA
	// surface, AAD-bound to DID. RSAKeyVersion makes rotation definable.
	RSAKeySealed  []byte
	RSAKeyVersion int
	// PublicKeyPEM is the actor's published RSA public key (SPKI PEM). The
	// PRIVATE half is never stored in the clear.
	PublicKeyPEM string
	// Enabled gates webfinger resolution (disabled actors' local parts do
	// not resolve; their actor documents stay fetchable — task 17 owns
	// scrub semantics). EnabledAt/DisabledAt record the last transition.
	Enabled    bool
	EnabledAt  *time.Time
	DisabledAt *time.Time
	// DeliveryPaused is the transient #account state (decision 19):
	// delivery stops, identity stays.
	DeliveryPaused bool
	// Profile cache, refreshed from the appview/PDS (task 14 owns sync).
	DisplayName string
	Summary     string
	AvatarURL   string

	CreatedAt time.Time
	UpdatedAt time.Time
}

// APActorProfile is the mutable profile cache of an APActor.
type APActorProfile struct {
	DisplayName string
	Summary     string
	AvatarURL   string
}

type postgresAPActors struct {
	db *sql.DB
}

// NewAPActors creates the postgres-backed ap_actors repository.
func NewAPActors(db *sql.DB) APActors {
	return &postgresAPActors{db: db}
}

const apActorColumns = `
	did, kind, actor_id, normalized_origin, local_part,
	rsa_key_sealed, rsa_key_version, public_key_pem,
	enabled, enabled_at, disabled_at, delivery_paused,
	display_name, summary, avatar_url, created_at, updated_at`

func (r *postgresAPActors) Create(ctx context.Context, actor APActor) (*APActor, error) {
	// The lifecycle and profile columns are deliberately absent from the
	// insert list: federation is default-on (decision 11), so a created
	// actor is always enabled and unpaused and takes those values from the
	// schema defaults. Reading them off the argument would let a caller
	// that merely forgot to set Enabled mint a silently dead actor — the
	// zero value of a bool is exactly the dangerous direction here.
	// Disabling is an explicit SetEnabled call, never a side effect of
	// creation.
	query := `
		INSERT INTO ap_actors (
			did, kind, actor_id, normalized_origin, local_part,
			rsa_key_sealed, rsa_key_version, public_key_pem
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING` + apActorColumns

	row := r.db.QueryRowContext(ctx, query,
		actor.DID, string(actor.Kind), actor.ActorID,
		actor.NormalizedOrigin, actor.LocalPart,
		actor.RSAKeySealed, actor.RSAKeyVersion, actor.PublicKeyPEM,
	)
	stored, err := scanAPActor(row)
	if err != nil {
		// Mapped from the constraint name rather than pre-checked: a
		// SELECT-then-INSERT pre-check races with a concurrent mint.
		if constraint, ok := uniqueViolation(err); ok {
			switch constraint {
			case "ap_actors_pkey":
				return nil, errors.NewConflictError("ap_actor", "did", actor.DID)
			case "ap_actors_actor_id_key":
				return nil, errors.NewConflictError("ap_actor", "actor_id", actor.ActorID)
			case "ap_actors_origin_local_part_key":
				return nil, errors.NewConflictError("ap_actor", "local_part", actor.LocalPart)
			}
		}
		return nil, fmt.Errorf("create ap_actor %q: %w", actor.DID, err)
	}
	return stored, nil
}

func (r *postgresAPActors) GetByDID(ctx context.Context, did string) (*APActor, error) {
	query := `SELECT` + apActorColumns + ` FROM ap_actors WHERE did = $1`
	actor, err := scanAPActor(r.db.QueryRowContext(ctx, query, did))
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFoundError("ap_actor", did)
		}
		return nil, fmt.Errorf("get ap_actor by did %q: %w", did, err)
	}
	return actor, nil
}

func (r *postgresAPActors) GetByOriginLocalPart(ctx context.Context, normalizedOrigin, localPart string) (*APActor, error) {
	// Both halves of the key are in the WHERE: the lookup is scoped to the
	// routed Host, so alice@vanity.example never answers for
	// alice@coves.social.
	query := `SELECT` + apActorColumns + `
		FROM ap_actors WHERE normalized_origin = $1 AND local_part = $2`
	actor, err := scanAPActor(r.db.QueryRowContext(ctx, query, normalizedOrigin, localPart))
	if err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewNotFoundError("ap_actor", localPart+"@"+normalizedOrigin)
		}
		return nil, fmt.Errorf("get ap_actor by origin/local_part %q@%q: %w", localPart, normalizedOrigin, err)
	}
	return actor, nil
}

func (r *postgresAPActors) SetEnabled(ctx context.Context, did string, enabled bool) error {
	// Both transitions are stamped, and disabled_at is CLEARED on re-enable
	// so the column answers "is this actor currently disabled, and since
	// when" rather than "was it ever disabled". enabled_at is not cleared on
	// disable: the last enable is history worth keeping.
	query := `
		UPDATE ap_actors SET
			enabled = $2,
			enabled_at = CASE WHEN $2 THEN now() ELSE enabled_at END,
			disabled_at = CASE WHEN $2 THEN NULL ELSE now() END,
			updated_at = now()
		WHERE did = $1`
	return r.execOne(ctx, "set enabled", did, query, did, enabled)
}

func (r *postgresAPActors) SetPaused(ctx context.Context, did string, paused bool) error {
	// delivery_paused alone: pausing delivery must not disable the actor,
	// so the enabled columns are untouched.
	query := `UPDATE ap_actors SET delivery_paused = $2, updated_at = now() WHERE did = $1`
	return r.execOne(ctx, "set paused", did, query, did, paused)
}

func (r *postgresAPActors) UpdateProfile(ctx context.Context, did string, profile APActorProfile) error {
	// The SET list is the profile cache and nothing else. local_part and
	// actor_id are absent on purpose: task 14 reaches this method on handle
	// changes, and re-deriving the local part there would break every
	// federated mention of the old handle.
	query := `
		UPDATE ap_actors SET
			display_name = $2, summary = $3, avatar_url = $4, updated_at = now()
		WHERE did = $1`
	return r.execOne(ctx, "update profile", did, query,
		did, profile.DisplayName, profile.Summary, profile.AvatarURL)
}

// execOne runs a single-row mutator and reports a missed DID as NotFound.
func (r *postgresAPActors) execOne(ctx context.Context, what, did, query string, args ...any) error {
	result, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("%s for ap_actor %q: %w", what, did, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s for ap_actor %q: rows affected: %w", what, did, err)
	}
	if affected == 0 {
		return errors.NewNotFoundError("ap_actor", did)
	}
	return nil
}

func scanAPActor(row rowScanner) (*APActor, error) {
	var actor APActor
	var kind string
	var enabledAt, disabledAt sql.NullTime
	err := row.Scan(
		&actor.DID, &kind, &actor.ActorID,
		&actor.NormalizedOrigin, &actor.LocalPart,
		&actor.RSAKeySealed, &actor.RSAKeyVersion, &actor.PublicKeyPEM,
		&actor.Enabled, &enabledAt, &disabledAt, &actor.DeliveryPaused,
		&actor.DisplayName, &actor.Summary, &actor.AvatarURL,
		&actor.CreatedAt, &actor.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	actor.Kind = ActorType(kind)
	if enabledAt.Valid {
		actor.EnabledAt = &enabledAt.Time
	}
	if disabledAt.Valid {
		actor.DisabledAt = &disabledAt.Time
	}
	return &actor, nil
}
