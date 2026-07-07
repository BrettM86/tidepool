// Package store is Tidepool's persistence spine: one repository per table,
// behind interfaces, using raw parameterized SQL over database/sql. Later
// tasks (identity, materializer, ingestion) consume these interfaces.
//
// Conventions shared by all repositories:
//   - Writes are idempotent upserts (ON CONFLICT) keyed on the AP id.
//   - Misses return errors satisfying errors.IsNotFound.
//   - Deletes are soft (deleted_at) where the schema supports them.
package store

import (
	"context"
	"time"
)

// APObjects maps AP object ids to the atproto records they materialized
// as, and back. Every materialization writes a mapping; every strongRef
// resolution reads one.
type APObjects interface {
	// PutMapping idempotently upserts a mapping keyed on APID. It validates
	// DID, Collection, RKey, and CID, derives ATURI from the first three,
	// and returns the stored row. An empty Origin defaults to
	// OriginFediverse (bridge-emitted writes must say OriginBridge
	// explicitly). Re-putting an existing APID updates APType, Origin, DID,
	// Collection, RKey, ATURI, CID, and PublishedAt, refreshes IndexedAt,
	// and clears any soft delete (re-materialization revives the mapping).
	PutMapping(ctx context.Context, mapping APObjectMapping) (*APObjectMapping, error)

	// GetByAPID returns the mapping for an AP object id, including
	// soft-deleted rows (callers can check IsDeleted to detect tombstones).
	GetByAPID(ctx context.Context, apID string) (*APObjectMapping, error)

	// GetByATURI returns the mapping for an at-uri, including soft-deleted
	// rows.
	GetByATURI(ctx context.Context, atURI string) (*APObjectMapping, error)

	// ResolveStrongRef resolves an AP object id to the (at-uri, cid) pair a
	// strongRef needs. The two failure modes are deliberately distinct so
	// the materializer can branch on them:
	//   - a missing object returns an error satisfying errors.IsNotFound —
	//     the trigger to fetch and materialize the ancestor chain;
	//   - a soft-deleted object returns an error satisfying
	//     errors.IsTombstoned (and NOT IsNotFound) — the subtree must be
	//     dropped, never re-fetched.
	ResolveStrongRef(ctx context.Context, apID string) (atURI string, cid string, err error)

	// SoftDelete marks the mapping for an AP object id as deleted, in one
	// atomic statement. Deleting an already-deleted mapping is a no-op that
	// preserves the original tombstone time; a missing mapping is an error
	// satisfying errors.IsNotFound.
	SoftDelete(ctx context.Context, apID string) error
}

// BridgedActors registers fediverse actors bridged into atproto and their
// escrowed signing keys and consent state.
type BridgedActors interface {
	// UpsertActor idempotently upserts an actor keyed on APActorID and
	// returns the stored row. ConsentState must be stated explicitly (the
	// zero value is rejected; there is no fail-open default). On conflict:
	//   - Handle and SigningKeyEncrypted are sticky: an empty handle or nil
	//     key on the incoming actor (e.g. a profile refresh built purely
	//     from AP data) preserves the stored values; non-empty values
	//     overwrite them.
	//   - DID, ActorType, ConsentState, and CreatedAt never change.
	//     Identity is immutable once minted: an upsert whose DID or
	//     ActorType diverges from the stored row returns an error
	//     satisfying errors.IsAlreadyExists.
	//   - Tombstoned actors are frozen: upserting an actor whose stored
	//     ConsentState is ConsentStateDeleted modifies nothing and returns
	//     the stored row as-is (consent stays deleted).
	UpsertActor(ctx context.Context, actor BridgedActor) (*BridgedActor, error)

	// GetByAPActorID returns the actor for an AP actor id.
	GetByAPActorID(ctx context.Context, apActorID string) (*BridgedActor, error)

	// GetByDID returns the actor for a bridged DID.
	GetByDID(ctx context.Context, did string) (*BridgedActor, error)

	// SetConsentState transitions the actor's consent state. Deleted is
	// terminal: transitioning away from ConsentStateDeleted returns an
	// error satisfying errors.IsValidation (re-tombstoning an already
	// deleted actor stays a no-op success).
	SetConsentState(ctx context.Context, apActorID string, state ConsentState) error

	// MarkProfileSynced records when the actor's profile record was last
	// (re)materialized.
	MarkProfileSynced(ctx context.Context, apActorID string, syncedAt time.Time) error
}

// Communities tracks the AP groups the bridge subscribes to and their
// backfill progress.
type Communities interface {
	// UpsertCommunity idempotently upserts a community keyed on APGroupID
	// and returns the stored row. On conflict it updates PreferredUsername
	// only; follow state and timestamps are preserved, and DID and Instance
	// are immutable: an upsert whose DID or Instance diverges from the
	// stored row returns an error satisfying errors.IsAlreadyExists.
	UpsertCommunity(ctx context.Context, community Community) (*Community, error)

	// GetByAPGroupID returns the community for an AP group id.
	GetByAPGroupID(ctx context.Context, apGroupID string) (*Community, error)

	// GetByDID returns the community for a bridged repo DID.
	GetByDID(ctx context.Context, did string) (*Community, error)

	// SetFollowState transitions the Follow subscription state. Arbitrary
	// transitions are legal — the states are driven by external AP
	// activities (Accept, Reject, Undo) that arrive in whatever order the
	// remote instance sends them. FollowedAt stamps only on the transition
	// INTO accepted (a redelivered Accept does not re-stamp it) and clears
	// on none.
	SetFollowState(ctx context.Context, apGroupID string, state FollowState) error

	// SetLastBackfill records when an outbox backfill last completed.
	SetLastBackfill(ctx context.Context, apGroupID string, backfilledAt time.Time) error

	// ListByFollowState returns all communities in the given follow state,
	// ordered by creation time.
	ListByFollowState(ctx context.Context, state FollowState) ([]*Community, error)
}

// ServiceKeys persists the bridge's own long-lived keys (today: the service
// actor's AP-side RSA private key). Keys are create-once: there is no update
// or delete, so a stored key can never be silently rotated out from under
// signatures already in flight.
type ServiceKeys interface {
	// Create inserts a new named key and returns the stored row. An existing
	// name returns an error satisfying errors.IsAlreadyExists — callers that
	// lose a bootstrap race must Get the winner's key instead.
	Create(ctx context.Context, name string, privateKeyPEM []byte) (*ServiceKey, error)

	// Get returns the key for a purpose name. A missing key is an error
	// satisfying errors.IsNotFound.
	Get(ctx context.Context, name string) (*ServiceKey, error)
}

// InboxEvents deduplicates inbound AP activities and records processing
// outcomes. The queue-consumption side (ListPending and friends) is
// deliberately deferred to task 06, which owns the processing loop.
type InboxEvents interface {
	// RecordEvent inserts the activity if it has not been seen before.
	// It returns isNew=false (and no error) when the activity id was
	// already recorded — the caller should drop the duplicate delivery.
	RecordEvent(ctx context.Context, activityID, activityType string) (isNew bool, err error)

	// MarkProcessed stamps the event as successfully processed and clears
	// any recorded error.
	MarkProcessed(ctx context.Context, activityID string) error

	// MarkFailed records a processing error on an unprocessed event,
	// leaving it unprocessed so it can be retried or inspected. The message
	// must be non-empty. Failing an already-processed event is a no-op
	// success (a late failure report must not un-process a successful
	// retry); a missing event is an error satisfying errors.IsNotFound.
	MarkFailed(ctx context.Context, activityID string, message string) error

	// GetEvent returns the event for an activity id.
	GetEvent(ctx context.Context, activityID string) (*InboxEvent, error)
}
