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
	"database/sql"
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

	// PutMappingTx is PutMapping executed on an existing transaction — the
	// seam repo.TxSideEffect uses so a record commit and its mapping land
	// atomically (task 11 closed the PutRecord→PutMapping crash window).
	PutMappingTx(ctx context.Context, tx *sql.Tx, mapping APObjectMapping) (*APObjectMapping, error)

	// GetByAPID returns the mapping for an AP object id, including
	// soft-deleted rows (callers can check IsDeleted to detect tombstones).
	GetByAPID(ctx context.Context, apID string) (*APObjectMapping, error)

	// DeletedInTx reports whether the mapping for apID is currently
	// soft-deleted, read ON THE GIVEN TRANSACTION so a commit's side effect
	// can re-check consent state atomically with its write — the guard that
	// stops the vote-stats refresher from resurrecting a soft-deleted mapping
	// (PutMappingTx unconditionally clears deleted_at). A missing mapping is an
	// error satisfying errors.IsNotFound.
	DeletedInTx(ctx context.Context, tx *sql.Tx, apID string) (bool, error)

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

	// ListByActorDID returns all live (not soft-deleted) mappings whose
	// record either lives in the actor's repo (did) or was authored by the
	// actor into another repo (author_did). The second case is the LEGACY
	// post era only: those posts were written into the community's repo. A
	// postv2 and every comment live in the author's own repo, so did answers
	// for them. Task 05's Delete(Actor) scrub enumerates these.
	ListByActorDID(ctx context.Context, did string) ([]*APObjectMapping, error)

	// SoftDelete marks the mapping for an AP object id as deleted, in one
	// atomic statement. Deleting an already-deleted mapping is a no-op that
	// preserves the original tombstone time; a missing mapping is an error
	// satisfying errors.IsNotFound.
	SoftDelete(ctx context.Context, apID string) error

	// Restore clears a soft delete (Undo{Delete}/restore): the explicit
	// counterpart task 05 required so an un-deleted object can be
	// re-materialized (commitRecord refuses to resurrect a mapping whose
	// deleted_at is set). Restoring a live mapping is a no-op success; a
	// missing mapping is an error satisfying errors.IsNotFound.
	Restore(ctx context.Context, apID string) error
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

	// GetByHandle returns the actor for a bridged handle. Task 03 uses it
	// for handle-collision suffixing during minting and for
	// com.atproto.identity.resolveHandle.
	GetByHandle(ctx context.Context, handle string) (*BridgedActor, error)

	// SetConsentState transitions the actor's consent state. Deleted is
	// terminal: transitioning away from ConsentStateDeleted returns an
	// error satisfying errors.IsValidation (re-tombstoning an already
	// deleted actor stays a no-op success).
	SetConsentState(ctx context.Context, apActorID string, state ConsentState) error

	// MarkProfileSynced records when the actor's profile record was last
	// (re)materialized.
	MarkProfileSynced(ctx context.Context, apActorID string, syncedAt time.Time) error
}

// APActors persists the ActivityPub identities Coves users get on the user
// origin (task 13): one Person actor per DID, its sealed RSA key, and the
// webfinger lookup key (normalized_origin, local_part).
//
// The local part is FROZEN at creation: a handle change updates the profile
// cache only, never the local part, so a minted @alice@coves.social keeps
// resolving after the user renames.
type APActors interface {
	// Create inserts a new actor and returns the stored row. A created
	// actor is always enabled and unpaused (default-on federation,
	// decision 11): the lifecycle fields on the argument are ignored, and
	// disabling goes through SetEnabled. Uniqueness violations — did,
	// actor_id, or (normalized_origin, local_part) — return an error
	// satisfying errors.IsAlreadyExists, mapped from the constraint name
	// rather than pre-checked (a pre-check races).
	Create(ctx context.Context, actor APActor) (*APActor, error)

	// GetByDID returns the actor for a Coves DID. A miss is an error
	// satisfying errors.IsNotFound.
	GetByDID(ctx context.Context, did string) (*APActor, error)

	// GetByOriginLocalPart returns the actor for a (normalized origin,
	// local part) pair — the webfinger lookup, scoped to the routed Host so
	// vanity origins hosting the same local part stay distinct. A miss is
	// an error satisfying errors.IsNotFound.
	GetByOriginLocalPart(ctx context.Context, normalizedOrigin, localPart string) (*APActor, error)

	// SetEnabled toggles federation for an actor: disabling stamps
	// disabled_at, re-enabling clears it and re-stamps enabled_at. A
	// missing actor is an error satisfying errors.IsNotFound.
	SetEnabled(ctx context.Context, did string, enabled bool) error

	// SetPaused toggles delivery_paused (the transient #account state).
	// A missing actor is an error satisfying errors.IsNotFound.
	SetPaused(ctx context.Context, did string, paused bool) error

	// UpdateProfile refreshes the cached display name, summary, and avatar
	// and bumps updated_at. It NEVER touches local_part — the identity
	// handler (task 14) reaches this method on handle changes, and the
	// frozen local part is what keeps federated mentions resolving.
	// A missing actor is an error satisfying errors.IsNotFound.
	UpdateProfile(ctx context.Context, did string, profile APActorProfile) error
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

	// ClaimStalePendingFollows atomically claims communities stuck in pending
	// whose last Follow went out before requestedBefore (rows with a NULL
	// follow_requested_at — legacy pending rows — are included) and that have
	// consumed fewer than maxAttempts Follow sends. Claiming is a single
	// UPDATE ... RETURNING that increments follow_attempts and re-stamps
	// follow_requested_at on exactly the matched rows, row-locked, so
	// overlapping sweeps never double-claim and an Accept that flipped a row
	// to accepted between sweeps is never clobbered back to pending (the
	// WHERE only matches follow_state='pending'). The returned rows carry the
	// post-increment follow_attempts and are the ones the retrier must send a
	// fresh Follow to. The follow retrier's work query (Lemmy first-contact
	// Accept race).
	ClaimStalePendingFollows(ctx context.Context, requestedBefore time.Time, maxAttempts int) ([]*Community, error)
}

// ServiceKeys persists the bridge's own long-lived keys (today: the service
// actor's AP-side RSA private key). Keys are create-once: there is no update
// or delete, so a stored key can never be silently rotated out from under
// signatures already in flight.
type ServiceKeys interface {
	// Create inserts a new named key and returns the stored row. An existing
	// name returns an error satisfying errors.IsAlreadyExists — callers that
	// lose a bootstrap race must Get the winner's key instead. keyMaterial's
	// encoding is the caller's contract (see ServiceKey.KeyMaterial).
	Create(ctx context.Context, name string, keyMaterial []byte) (*ServiceKey, error)

	// Get returns the key for a purpose name. A missing key is an error
	// satisfying errors.IsNotFound.
	Get(ctx context.Context, name string) (*ServiceKey, error)
}

// InboxEvents deduplicates inbound AP activities and doubles as the durable
// postgres work queue the ingestion worker pool consumes (task 06). Rows are
// never deleted: a processed row IS the dedupe record for re-deliveries.
type InboxEvents interface {
	// RecordEvent inserts the activity if it has not been seen before.
	// It returns isNew=false (and no error) when the activity id was
	// already recorded — the caller should drop the duplicate delivery.
	RecordEvent(ctx context.Context, activityID, activityType string) (isNew bool, err error)

	// Enqueue inserts a full queue item (ActivityID, Type, Payload, ActorID,
	// OrderingKey are honored; the rest is defaulted). Like RecordEvent it
	// returns isNew=false when the activity id was already recorded, so a
	// duplicate delivery never re-enqueues work.
	Enqueue(ctx context.Context, event InboxEvent) (isNew bool, err error)

	// ClaimNext atomically claims the oldest processable event and
	// increments its attempt counter. An event is processable when it is
	// unprocessed, not poisoned, past its next_attempt_at, unleased (or the
	// lease expired), and — the per-community ordering guarantee — no older
	// unprocessed, unpoisoned event shares its ordering key. An empty queue
	// returns an error satisfying errors.IsNotFound.
	//
	// The returned event's ClaimedUntil is the fencing/claim token for this
	// claim: MarkProcessed/Release/MarkPoisoned require it so a worker whose
	// lease expired and was re-claimed by another cannot overwrite the newer
	// attempt's outcome.
	ClaimNext(ctx context.Context, lease time.Duration) (*InboxEvent, error)

	// MarkProcessed stamps the event as successfully processed and clears any
	// recorded error. claimToken must equal the ClaimedUntil returned by the
	// ClaimNext that produced this attempt. It returns applied=true when the
	// outcome was written; applied=false (no error) means the claim was stale
	// — the lease expired and another worker re-claimed the event, or the row
	// is already in a terminal state — so this outcome was discarded. A
	// missing event is an error satisfying errors.IsNotFound.
	MarkProcessed(ctx context.Context, activityID string, claimToken time.Time) (applied bool, err error)

	// Release records a processing failure and schedules the retry: error
	// message stored, lease cleared, next_attempt_at set. claimToken must
	// equal the claim's ClaimedUntil. It returns applied=true when the retry
	// was scheduled; applied=false (no error) means the claim was stale or
	// the event already completed, so the release was discarded. A missing
	// event is an error satisfying errors.IsNotFound.
	Release(ctx context.Context, activityID, message string, nextAttempt time.Time, claimToken time.Time) (applied bool, err error)

	// MarkPoisoned permanently fails the event: failed_at stamped, error
	// recorded, lease cleared. Poisoned events are skipped by ClaimNext and
	// stop blocking their ordering key. claimToken must equal the claim's
	// ClaimedUntil. It returns applied=true when the event was poisoned;
	// applied=false (no error) means the claim was stale or the event already
	// completed, so the poison was discarded. A missing event is an error
	// satisfying errors.IsNotFound.
	MarkPoisoned(ctx context.Context, activityID, message string, claimToken time.Time) (applied bool, err error)

	// MarkFailed records a processing error on an unprocessed event,
	// leaving it unprocessed so it can be retried or inspected. The message
	// must be non-empty. Failing an already-processed event is a no-op
	// success (a late failure report must not un-process a successful
	// retry); a missing event is an error satisfying errors.IsNotFound.
	MarkFailed(ctx context.Context, activityID string, message string) error

	// GetEvent returns the event for an activity id.
	GetEvent(ctx context.Context, activityID string) (*InboxEvent, error)
}

// OutboundObjects persists the state every outbound Delete and Update is
// rebuilt from (task 14, decision 14). A Jetstream delete commit carries the
// DID, collection and rkey and nothing else — no record body, no CID — so a
// Delete{Note} can only be built from what was written here at create time.
//
// Rows are TOMBSTONED, never removed: the row is what a late replay of the
// create is rejected against.
type OutboundObjects interface {
	// Upsert idempotently writes the outbound state keyed on ATURI and
	// returns the stored row. A new row starts at LastActivitySeq 0; every
	// later upsert of the same at-uri bumps it, so each applied operation
	// gets its own stable activity id. CreatedAt is preserved.
	//
	// The bump is safe ONLY because the rev gate runs first: a replayed
	// commit never reaches this method, so the seq (and therefore the
	// activity id) is stable under replay.
	Upsert(ctx context.Context, object OutboundObject) (*OutboundObject, error)

	// UpsertTx is Upsert on an existing transaction — the seam that lets the
	// rev-gate claim and the outbound state land in ONE commit. A nil tx is
	// an error satisfying errors.IsValidation.
	UpsertTx(ctx context.Context, tx *sql.Tx, object OutboundObject) (*OutboundObject, error)

	// GetByATURI returns the outbound state for an at-uri, tombstoned rows
	// included (callers check IsTombstoned). A miss is an error satisfying
	// errors.IsNotFound.
	GetByATURI(ctx context.Context, atURI string) (*OutboundObject, error)

	// Tombstone stamps tombstoned_at, bumps LastActivitySeq and returns the
	// full stored row — the state the Delete activity is built from, handed
	// back in the same statement that tombstones it so no read/write window
	// exists. Tombstoning an already-tombstoned row is a no-op success that
	// preserves the original tombstoned_at AND the seq (a redelivered delete
	// must reuse the id the first one sent). A missing row is an error
	// satisfying errors.IsNotFound.
	Tombstone(ctx context.Context, atURI string) (*OutboundObject, error)

	// TombstoneTx is Tombstone on an existing transaction. A nil tx is an
	// error satisfying errors.IsValidation.
	TombstoneTx(ctx context.Context, tx *sql.Tx, atURI string) (*OutboundObject, error)

	// SetAccepted stamps accepted_at — the causal-gating marker (task 15,
	// decision 15). Delivery SUCCESS sets it; a NULL accepted_at means the
	// object has not yet been delivered to its community, which is what keeps a
	// BRIDGE-origin child (a reply) ineligible until its parent lands.
	// Stamping an already-accepted row preserves the original time (a
	// redelivery must not move the causal boundary). A missing row is an error
	// satisfying errors.IsNotFound.
	SetAccepted(ctx context.Context, atURI string) error
}

// OutboundVotes persists the state an outbound Undo is rebuilt from (decision
// 16). A vote delete commit names the vote record and nothing else, so the
// direction and the activity id the Like/Dislike went out under have to be
// readable back from here.
type OutboundVotes interface {
	// Upsert idempotently writes the vote intent keyed on VoteATURI and
	// returns the stored row. An empty DeliveredState defaults to
	// DeliveredStatePending — the consumer records intent only; delivery is
	// task 15's to claim. A new row starts at ActivitySeq 0; re-upserting the
	// same vote at-uri bumps it. Writing a DIFFERENT vote at-uri for an
	// (ActorDID, SubjectATURI) pair that already has one returns an error
	// satisfying errors.IsAlreadyExists: one actor holds at most one live
	// vote per subject, and silently clobbering the old row would strand its
	// Undo.
	Upsert(ctx context.Context, vote OutboundVote) (*OutboundVote, error)

	// UpsertTx is Upsert on an existing transaction. A nil tx is an error
	// satisfying errors.IsValidation.
	UpsertTx(ctx context.Context, tx *sql.Tx, vote OutboundVote) (*OutboundVote, error)

	// GetByATURI returns the vote for a vote record's at-uri — the DELETE
	// path's lookup key, because a delete commit carries nothing else. A miss
	// is an error satisfying errors.IsNotFound.
	GetByATURI(ctx context.Context, voteATURI string) (*OutboundVote, error)

	// GetByActorSubject returns the actor's live vote on a subject — the
	// CREATE path's lookup, which asks "did this actor already vote here?".
	// A miss is an error satisfying errors.IsNotFound.
	GetByActorSubject(ctx context.Context, actorDID, subjectATURI string) (*OutboundVote, error)

	// GetByActivityID returns the vote whose CurrentActivityID equals
	// activityID — the DELIVERY callback's lookup (task 15). A Like/Dislike is
	// delivered under CurrentActivityID; an Undo embeds that same id as its
	// inner object, so both delivery-success callbacks resolve the vote row
	// from the one activity id. A miss is an error satisfying errors.IsNotFound.
	GetByActivityID(ctx context.Context, activityID string) (*OutboundVote, error)

	// SetDeliveredState transitions the delivery state. An unknown state is
	// an error satisfying errors.IsValidation; a missing vote is an error
	// satisfying errors.IsNotFound.
	SetDeliveredState(ctx context.Context, voteATURI string, state DeliveredState) error

	// Delete removes the vote state once its Undo is delivered. Deleting a
	// missing vote is a no-op success.
	Delete(ctx context.Context, voteATURI string) error
}

// FederationPrefs stores Coves users' federation preferences (decision 11).
//
// Federation is DEFAULT-ON and social.coves.bridge.federation is an OPT-OUT
// record, so an ABSENT row means enabled. Get therefore returns NotFound for
// a user who never said anything — it never invents an enabled row, because a
// caller that cannot tell "opted in" from "never spoke" cannot tell a
// re-enable from a first sighting either.
type FederationPrefs interface {
	// Upsert writes the preference keyed on DID and returns the stored row.
	// Every field is overwritten, so re-enabling (Enabled true) also clears a
	// previously requested DeleteRemote. Source must be stated explicitly:
	// the zero value is an error satisfying errors.IsValidation.
	Upsert(ctx context.Context, pref FederationPref) (*FederationPref, error)

	// Get returns the preference for a DID. A miss is an error satisfying
	// errors.IsNotFound and MEANS default-on, not "unknown".
	Get(ctx context.Context, did string) (*FederationPref, error)

	// Delete removes the preference — the record-delete path, which restores
	// the default-on state. Deleting a missing preference is a no-op success.
	Delete(ctx context.Context, did string) error
}

// Tombstones remembers AP object ids whose Delete arrived before (or
// without) a materialization — the create-after-delete gap: a Create
// delivered after its Delete must not resurrect content the origin removed.
// Undo{Delete} removes the marker.
//
// Markers are SCOPED to the authority that laid them (migration 015): the
// announcing community's AP group id, or "" for an origin-authorized marker
// (a bare same-authority Delete, the admin sweep's verified 410) which is
// global. Announced deletes are accepted for ids the bridge has no mapping
// for — that allowance is what closes the delete-before-create race — so an
// UNSCOPED marker would let any one followed community pre-suppress arbitrary
// ids belonging to OTHER communities for the whole retention window. Scoping
// keeps a community's reach inside its own content.
type Tombstones interface {
	// Record idempotently marks an AP id as deleted upstream, scoped to
	// announcer (the announcing community's AP group id; "" for an
	// origin-authorized, global marker).
	Record(ctx context.Context, apID, announcer string) error

	// ExistsFor reports whether the AP id carries a marker VISIBLE in
	// communityIRI's context: a global marker, or one laid by that same
	// community. A caller with no community context passes "" and sees only
	// global markers.
	ExistsFor(ctx context.Context, apID, communityIRI string) (bool, error)

	// Remove clears markers (Undo{Delete}/restore). A community clears its
	// OWN marker only — never the global one, which is origin-authorized and
	// outranks it; "" is that origin authority and clears every marker for the
	// id (see the implementation for why the read and write sides are
	// deliberately asymmetric). Removing a missing marker is a no-op success.
	Remove(ctx context.Context, apID, communityIRI string) error

	// Prune deletes markers recorded before the cutoff, in batches, and
	// returns how many were deleted. Retention trade-off, accepted: a
	// pruned marker re-opens the create-after-delete window for THAT id,
	// but out-of-order re-deliveries happen minutes apart, not months —
	// TOMBSTONE_RETENTION's default (30 days) is orders of magnitude above
	// any observed redelivery horizon.
	Prune(ctx context.Context, cutoff time.Time) (int64, error)
}

// OutboundActivities persists the canonical, immutable wire payloads outbound
// deliveries fan out from (task 15, decision 15). One activity id maps to one
// payload byte-string that GET /ap/activity/{hash} serves and a redelivery
// re-sends verbatim; a peer dedupes on the stable id. The payload never
// changes once written — an edit is a NEW activity, not a rewrite.
type OutboundActivities interface {
	// Insert idempotently writes one activity. It returns inserted=true when a
	// new row was written and inserted=false (no error) when the activity id
	// already existed: the ON CONFLICT DO NOTHING is deliberate — the payload
	// of an activity a peer may already hold must never be overwritten.
	Insert(ctx context.Context, activity OutboundActivity) (inserted bool, err error)

	// InsertTx is Insert on an existing transaction — the seam the enqueuer
	// uses so the activity, its deliveries and the rev-gate advance land in ONE
	// commit (an enqueue whose gate tx rolls back must leave no activity or
	// delivery row). A nil tx is an error satisfying errors.IsValidation.
	InsertTx(ctx context.Context, tx *sql.Tx, activity OutboundActivity) (inserted bool, err error)

	// Get returns the canonical activity for an id. A miss is an error
	// satisfying errors.IsNotFound.
	Get(ctx context.Context, activityID string) (*OutboundActivity, error)
}

// OutboundDeliveries is the per-inbox delivery queue (task 15). It generalizes
// the inbox_events fenced work queue: claimed_until fencing, per-ordering-key
// serialization via a loose index scan, SKIP LOCKED concurrency. The ordering
// key is the community AP id, so all deliveries bound for one community form a
// single serial line.
type OutboundDeliveries interface {
	// Enqueue writes one pending delivery keyed on (ActivityID, TargetInbox)
	// and returns the stored row. A duplicate (activity, inbox) is an error
	// satisfying errors.IsAlreadyExists.
	Enqueue(ctx context.Context, delivery OutboundDelivery) (*OutboundDelivery, error)

	// EnqueueTx is Enqueue on an existing transaction — rides the enqueuer's
	// gate tx. A nil tx is an error satisfying errors.IsValidation.
	EnqueueTx(ctx context.Context, tx *sql.Tx, delivery OutboundDelivery) (*OutboundDelivery, error)

	// ClaimNext atomically claims the oldest processable delivery and
	// increments its attempt counter. A delivery is processable when it is
	// pending, past its next_attempt_at, unleased (or the lease expired), and —
	// the per-community ordering guarantee — is the head (min Seq) of its
	// ordering key among pending rows: a younger delivery on a key is invisible
	// while an older PENDING sibling exists, and a delivered/poisoned/cancelled
	// sibling stops blocking. An empty queue returns an error satisfying
	// errors.IsNotFound.
	//
	// The returned delivery's ClaimedUntil is the fencing/claim token: the
	// Mark*/Release methods require it so a worker whose lease expired and was
	// re-claimed by another cannot clobber the newer attempt's outcome.
	ClaimNext(ctx context.Context, lease time.Duration) (*OutboundDelivery, error)

	// MarkDelivered stamps the delivery delivered (delivered_at set, lease
	// cleared), recording lastStatusCode. claimToken must equal the claim's
	// ClaimedUntil. It returns exists=false for a missing (activity, inbox);
	// applied=false (no error) when the claim was stale or the row already
	// terminal, so the outcome was discarded without a clobber.
	MarkDelivered(ctx context.Context, activityID, targetInbox string, lastStatusCode int, claimToken time.Time) (exists, applied bool, err error)

	// Release records a transient failure and schedules the retry (error class,
	// status and excerpt stored, lease cleared, next_attempt_at set), leaving
	// the delivery pending. claimToken must equal the claim's ClaimedUntil.
	// Same (exists, applied) split as MarkDelivered.
	Release(ctx context.Context, activityID, targetInbox, errorClass, excerpt string, lastStatusCode int, nextAttempt, claimToken time.Time) (exists, applied bool, err error)

	// MarkPoisoned permanently fails the delivery (state=poisoned, lease
	// cleared, error class/status/excerpt stored). Poisoned rows are skipped by
	// ClaimNext and stop blocking their ordering key. claimToken must equal the
	// claim's ClaimedUntil. Same (exists, applied) split.
	MarkPoisoned(ctx context.Context, activityID, targetInbox, errorClass, excerpt string, lastStatusCode int, claimToken time.Time) (exists, applied bool, err error)

	// CancelForActor moves every PENDING delivery of the actor's activities to
	// cancelled (the consent/kill-switch withdrawal — a disabled or paused
	// actor's create/update work is parked, never poisoned). Terminal
	// deliveries are untouched. Returns how many rows were cancelled.
	CancelForActor(ctx context.Context, actorDID string) (int64, error)

	// CancelForCommunity moves every PENDING delivery on an ordering key (a
	// community AP id) to cancelled — a community deleted or unfollowed out
	// from under pending work. Returns how many rows were cancelled.
	CancelForCommunity(ctx context.Context, orderingKey string) (int64, error)

	// Get returns the delivery for an (activity, inbox) pair. A miss is an
	// error satisfying errors.IsNotFound.
	Get(ctx context.Context, activityID, targetInbox string) (*OutboundDelivery, error)

	// HasPoisonedPredecessor reports whether an earlier delivery on the same
	// ordering key and target inbox (a lower seq) is poisoned — the causal
	// signal task 15's worker reads to distinguish a child whose bridge-origin
	// parent WILL NEVER land (parent_poisoned) from one merely waiting
	// (parent_unaccepted). Per-community serialization makes a lower-seq
	// delivery on the same line a causal ancestor.
	HasPoisonedPredecessor(ctx context.Context, orderingKey, targetInbox string, seq int64) (bool, error)

	// CountsByState returns the number of deliveries in each state — the
	// operator queue-inspect (GET /admin/outbound).
	CountsByState(ctx context.Context) (map[DeliveryState]int, error)

	// RedrivePoisoned resets poisoned deliveries back to pending for
	// redelivery, clearing the lease and rescheduling now. activityID and
	// orderingKey are optional filters (empty = no filter on that column); the
	// attempt counter is reset so a redriven delivery gets a fresh budget.
	// Returns how many rows were redriven.
	RedrivePoisoned(ctx context.Context, activityID, orderingKey string) (int64, error)
}
