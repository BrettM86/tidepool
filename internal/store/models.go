package store

import "time"

// ActorType is the kind of AP actor a bridged identity represents.
type ActorType string

const (
	ActorTypePerson ActorType = "person"
	ActorTypeGroup  ActorType = "group"
)

// Valid reports whether the value is a known actor type.
func (t ActorType) Valid() bool {
	switch t {
	case ActorTypePerson, ActorTypeGroup:
		return true
	}
	return false
}

// ConsentState tracks whether an AP actor allows bridging.
type ConsentState string

const (
	// ConsentStateOK means the actor has not opted out; bridging proceeds.
	ConsentStateOK ConsentState = "ok"
	// ConsentStateNoBridge means the actor opted out via #nobridge/#nobot;
	// no new content is materialized while in this state.
	ConsentStateNoBridge ConsentState = "nobridge"
	// ConsentStateDeleted means the actor was deleted upstream
	// (Delete(Actor)); the bridged repo is tombstoned. Terminal.
	ConsentStateDeleted ConsentState = "deleted"
)

// Valid reports whether the value is a known consent state. The zero value
// is deliberately invalid: consent must always be stated explicitly, never
// defaulted.
func (s ConsentState) Valid() bool {
	switch s {
	case ConsentStateOK, ConsentStateNoBridge, ConsentStateDeleted:
		return true
	}
	return false
}

// FollowState tracks the bridge's Follow subscription to an AP group.
type FollowState string

const (
	FollowStateNone     FollowState = "none"
	FollowStatePending  FollowState = "pending"
	FollowStateAccepted FollowState = "accepted"
)

// Valid reports whether the value is a known follow state.
func (s FollowState) Valid() bool {
	switch s {
	case FollowStateNone, FollowStatePending, FollowStateAccepted:
		return true
	}
	return false
}

// Origin discriminates which side of the bridge authored an AP object.
// Task 06's echo suppression drops inbound activities whose object maps to
// an OriginBridge row (our own writes reflected back by the fediverse).
type Origin string

const (
	// OriginFediverse marks content authored on the fediverse side.
	OriginFediverse Origin = "fediverse"
	// OriginBridge marks content the bridge itself emitted.
	OriginBridge Origin = "bridge"
)

// Valid reports whether the value is a known origin.
func (o Origin) Valid() bool {
	switch o {
	case OriginFediverse, OriginBridge:
		return true
	}
	return false
}

// APObjectMapping is one row of the ap_objects spine: the bidirectional
// mapping between an AP object and the atproto record it materialized as.
type APObjectMapping struct {
	ID             int64
	APID           string // canonical AP object id (URL)
	APType         string // AP type: Page, Note, Group, Person, ...
	OriginInstance string // host the object originated from, e.g. lemmy.world
	Origin         Origin // which side authored the object; defaults to fediverse
	DID            string // repo the record was written into
	// AuthorDID is the bridged actor who authored the record. It differs from
	// DID only in the LEGACY post era, whose posts were written into the
	// community's repo; for a postv2 and for comments the author's repo IS
	// DID, so the two are equal. Optional.
	AuthorDID string
	// CommunityDID is the community whose content this is — the membership
	// answer announced deletes and announced votes authorize against. Since
	// the author-owned flip it can no longer be read off DID (a postv2 lives
	// in the author's repo), so it is recorded at materialization time.
	// Optional: "" means unset, and readers fall back to deriving it from the
	// record (migration 016 says which rows that covers and why). Read it
	// through materialize.CommunityDIDOf, never compared directly — a direct
	// comparison silently treats every pre-016 row as belonging to nobody.
	CommunityDID string
	// ThreadRootATURI is the at-uri of the thread a materialized COMMENT hangs
	// in — its record's reply.root, recorded at materialization time because
	// that is the only moment the bridge knows it without re-reading the
	// record. Empty for posts (a post IS its own thread root) and for comments
	// materialized before migration 026.
	//
	// It is thread STRUCTURE, not moderation state: immutable for the life of
	// the record, and the answer to "which thread is this in?" that a lock on
	// the post above a Lemmy comment is read against.
	ThreadRootATURI string
	Collection      string     // record NSID, e.g. social.coves.community.post
	RKey            string     // deterministic TID rkey
	ATURI           string     // at://did/collection/rkey (derived; set by PutMapping)
	CID             string     // CID of the current record version
	PublishedAt     *time.Time // AP `published` time (may be absent upstream)
	IndexedAt       time.Time
	DeletedAt       *time.Time
}

// IsDeleted reports whether the mapping has been soft-deleted.
func (m *APObjectMapping) IsDeleted() bool { return m.DeletedAt != nil }

// ModeratedObject identifies the object a moderation decision applies to and
// the community that made it. All three fields travel together because none of
// them is derivable from another here: the at-uri is what the comment consumer
// reads back, the AP id is what the announcing community named, and the
// community DID is the binding without which any co-hosted community could
// lift the decision.
type ModeratedObject struct {
	ATURI        string
	APID         string
	CommunityDID string
}

// BridgedActor is a fediverse actor (person or group) that Tidepool has
// minted an atproto identity for.
type BridgedActor struct {
	ID                  int64
	APActorID           string // canonical AP actor id (URL)
	ActorType           ActorType
	DID                 string
	Handle              string // bridged handle; empty until assigned
	SigningKeyEncrypted []byte // escrowed signing key; AES-GCM ciphertext from task 03 on
	ConsentState        ConsentState
	ProfileSyncedAt     *time.Time
	CreatedAt           time.Time
}

// Community is an AP group the bridge follows (or is in the process of
// following), plus its backfill progress.
type Community struct {
	ID                int64
	APGroupID         string // canonical AP Group actor id (URL)
	DID               string // the community's bridged repo DID
	PreferredUsername string
	Instance          string // host, e.g. lemmy.world
	FollowState       FollowState
	FollowedAt        *time.Time
	LastBackfillAt    *time.Time
	CreatedAt         time.Time
	// FollowRequestedAt is when the most recent Follow activity went out
	// (stamped on every transition to pending — subscribe and automatic
	// re-send alike); FollowAttempts counts those sends since the last
	// unsubscribe. Together they bound the follow retrier (task 11, the
	// Lemmy first-contact Accept race).
	FollowRequestedAt *time.Time
	FollowAttempts    int
}

// CommunityBan is one community's exclusion of one native author.
//
// Every field is part of the decision, and two of them are the ones an
// implementation naturally drops: CommunityAPID, without which the delivery
// queue cannot scope a cancellation to this community, and ExpiresAt, without
// which every timed ban becomes permanent.
type CommunityBan struct {
	CommunityDID  string
	SubjectDID    string
	CommunityAPID string
	// ExpiresAt is nil for a permanent ban. Lemmy sends no activity when a
	// timed one lapses, so this is the only thing that ever lifts it.
	ExpiresAt *time.Time
	Reason    string
	// RemoveData records what the moderator asked for — that the author's
	// content in this community go too. It is acted on ONCE, when the ban lands;
	// the stored flag is the audit answer to "was their content purged?", never
	// an input to the Undo.
	RemoveData bool
}

// ServiceKey is one of the bridge's own long-lived keys, keyed by purpose
// name. KeyMaterial's encoding is per-row: plaintext PKCS#8 PEM for
// "service-actor" (the AP-side RSA signing key — the bridge's own service
// credential, not user key material), AES-GCM sealed ciphertext for
// "plc-rotation" (the escrow rotation key, sealed under BRIDGE_KEK by
// identity.Custodian). The column was renamed from private_key_pem in
// migration 013 because "PEM" lied for the sealed row.
type ServiceKey struct {
	ID          int64
	Name        string
	KeyMaterial []byte
	CreatedAt   time.Time
}

// DeliveredState tracks how far an outbound vote has travelled. The consumer
// (task 14) only ever writes pending; task 15 flips it on DELIVERY SUCCESS,
// never on enqueue — a state that claimed delivery before the wire confirmed
// it would make an Undo unsendable.
type DeliveredState string

const (
	// DeliveredStatePending means the intent is recorded but unconfirmed.
	DeliveredStatePending DeliveredState = "pending"
	// DeliveredStateDelivered means a peer accepted the Like/Dislike.
	DeliveredStateDelivered DeliveredState = "delivered"
	// DeliveredStateUndone is RESERVED and currently UNWRITTEN: task 15's worker
	// DELETES the outbound_votes row on a successful Undo (clear-on-Undo) rather
	// than transitioning it to "undone", so no code path ever sets this today.
	// It is kept in the enum and the CHECK constraint (the migration is applied)
	// against a future "keep the withdrawn-vote record" policy; Valid() still
	// accepts it so a hand-set or legacy row round-trips.
	DeliveredStateUndone DeliveredState = "undone"
)

// Valid reports whether the value is a known delivered state.
func (s DeliveredState) Valid() bool {
	switch s {
	case DeliveredStatePending, DeliveredStateDelivered, DeliveredStateUndone:
		return true
	}
	return false
}

// FederationPrefSource records where a federation preference came from: a
// social.coves.bridge.federation record the consumer saw, or a direct probe of
// the user's repo. It is stated explicitly — the zero value is invalid —
// because "we read this from a record" and "we went and asked" have different
// staleness, and a defaulted source hides which one applied.
type FederationPrefSource string

const (
	// FederationPrefSourceRecord means a Jetstream commit carried the record.
	FederationPrefSourceRecord FederationPrefSource = "record"
	// FederationPrefSourceProbe means the bridge fetched the record itself.
	FederationPrefSourceProbe FederationPrefSource = "probe"
)

// Valid reports whether the value is a known source.
func (s FederationPrefSource) Valid() bool {
	switch s {
	case FederationPrefSourceRecord, FederationPrefSourceProbe:
		return true
	}
	return false
}

// OutboundObject is the durable outbound state for one native record Tidepool
// federates outward (task 14, decision 14). It exists because a Jetstream
// DELETE commit carries the DID, collection and rkey and NOTHING else — no
// record body, no CID — so every fact a Delete{Note} needs must already be at
// rest here before the delete arrives.
type OutboundObject struct {
	// ATURI is the record's at-uri and the row's primary key.
	ATURI string
	// APObjectID is the AP id this record federates as.
	APObjectID string
	// LastCID and LastRev are PROVENANCE ONLY — what the last applied commit
	// looked like. The ordering gate is jetstream_record_revs, never this
	// column: a rev read from here is a check→write race by construction.
	LastCID string
	LastRev string
	// CommunityDID and CommunityAPID are the target community on both sides
	// of the bridge.
	CommunityDID  string
	CommunityAPID string
	// TranslatedSnapshot is the JSONB state task 15 renders the object and its
	// Delete from. (Task 17's restore is a delete-removal plus a fresh
	// acceptance, not a replay of these bytes — the snapshot is task 15's.)
	TranslatedSnapshot []byte
	// LastActivitySeq feeds ActivityID: create is 0, every applied
	// update/delete bumps it, so each operation gets its own stable id.
	LastActivitySeq int
	// Depth is the reply depth. Lemmy caps comment depth at 50.
	Depth        int
	CreatedAt    time.Time
	UpdatedAt    time.Time
	TombstonedAt *time.Time
	// AcceptedAt is the causal-gating marker (task 15, migration 020): stamped
	// on delivery success. NULL means "not yet accepted by its community", which
	// gates a bridge-origin child from delivering before its parent.
	AcceptedAt *time.Time
}

// IsTombstoned reports whether the record was deleted upstream. Tombstoned
// rows are KEPT: they are what a late replay is rejected against.
func (o *OutboundObject) IsTombstoned() bool { return o.TombstonedAt != nil }

// IsAccepted reports whether the object has been accepted by its community (its
// AP delivery succeeded). A bridge-origin parent gates its children until it is.
func (o *OutboundObject) IsAccepted() bool { return o.AcceptedAt != nil }

// OutboundVote is the durable outbound state for one native vote (decision
// 16). A vote DELETE commit names only the vote record, so direction and the
// activity id it was delivered under have to be readable back from here to
// build the Undo.
type OutboundVote struct {
	// VoteATURI is the vote record's at-uri and the row's primary key — the
	// delete path's only lookup key.
	VoteATURI string
	// ActorDID and SubjectATURI are UNIQUE TOGETHER: one actor holds at most
	// one live vote per subject.
	ActorDID     string
	SubjectATURI string
	SubjectAPID  string
	CommunityDID string
	// Direction is up or down.
	Direction string
	// CurrentActivityID is the id the Like/Dislike went out under; the Undo
	// must embed it.
	CurrentActivityID string
	DeliveredState    DeliveredState
	// ActivitySeq feeds ActivityID for this vote's operations.
	ActivitySeq int
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// FederationPref is a Coves user's federation preference (decision 11). The
// record is an OPT-OUT and federation is DEFAULT-ON, so an ABSENT row means
// enabled: this table only ever holds rows for users who said something.
type FederationPref struct {
	DID          string
	Enabled      bool
	DeleteRemote bool
	Source       FederationPrefSource
	UpdatedAt    time.Time
}

// InboxEvent is a received AP activity: the dedupe record AND the durable
// work-queue item task 06's worker pool consumes.
type InboxEvent struct {
	ID         int64
	ActivityID string // AP activity id (URL) — the dedupe key
	Type       string // AP activity type: Announce, Create, Like, ...
	// Payload is the raw activity JSON exactly as delivered (the
	// signature-verified request body). Nil on legacy rows.
	Payload []byte
	// ActorID is the AP actor the activity is bound to; the inbox verified
	// that it shares authority with the HTTP-signature signer.
	ActorID string
	// OrderingKey serializes processing: events sharing a key (normally the
	// community IRI) are handled strictly in arrival order.
	OrderingKey string
	// Attempts counts how many times a worker claimed this event.
	Attempts int
	// NextAttemptAt is the retry-backoff schedule; claimable when <= now.
	NextAttemptAt time.Time
	// ClaimedUntil is the current worker lease; nil/past means unclaimed. It
	// also serves as the fencing/claim token: ClaimNext stamps a fresh value
	// on every claim, and MarkProcessed/Release/MarkPoisoned require it so a
	// worker whose lease expired cannot overwrite a re-claim's outcome.
	ClaimedUntil *time.Time
	// FailedAt marks a poisoned event: permanently failed, skipped by the
	// queue, no longer blocking its ordering key.
	FailedAt    *time.Time
	ReceivedAt  time.Time
	ProcessedAt *time.Time
	Error       string // last processing error; empty if none
}

// DeliveryState tracks a single per-inbox delivery attempt through its
// terminal fates (task 15, decision 15). pending is the only non-terminal
// state; delivered/poisoned/cancelled are all final. cancelled (not poisoned)
// is the kill-switch/consent outcome — a delivery parked because the actor
// opted out or a community was unfollowed, never a failure the operator must
// triage.
type DeliveryState string

const (
	// DeliveryStatePending means the delivery is queued or backing off.
	DeliveryStatePending DeliveryState = "pending"
	// DeliveryStateDelivered means a peer accepted the activity (including
	// Lemmy's duplicate-activity response, which is a success by our stable
	// id).
	DeliveryStateDelivered DeliveryState = "delivered"
	// DeliveryStatePoisoned means the delivery permanently failed (a 4xx, an
	// attempt-cap breach, an unaccepted or poisoned parent).
	DeliveryStatePoisoned DeliveryState = "poisoned"
	// DeliveryStateCancelled means a consent/kill-switch withdrawal parked the
	// delivery: create/update for a disabled or paused actor, or a community
	// unfollowed out from under pending work. Never a failure.
	DeliveryStateCancelled DeliveryState = "cancelled"
)

// Valid reports whether the value is a known delivery state.
func (s DeliveryState) Valid() bool {
	switch s {
	case DeliveryStatePending, DeliveryStateDelivered, DeliveryStatePoisoned, DeliveryStateCancelled:
		return true
	}
	return false
}

// OutboundActivity is the canonical, IMMUTABLE wire payload for one activity id
// (task 15, decision 15). One row fans out to many outbound_deliveries; GET
// /ap/activity/{hash} serves Payload verbatim, and a redelivery re-sends it
// byte-for-byte so a peer dedupes on the stable id. Its payload never changes
// once written — a later edit is a NEW activity, not a rewrite of this one.
type OutboundActivity struct {
	// ActivityID is the deterministic AP activity id (consume.ActivityID) and
	// the row's primary key.
	ActivityID string
	// ActorDID is the persona whose key signs every delivery of this activity.
	ActorDID string
	// Kind is the AP activity type: Create, Update, Delete, Like, Dislike, Undo.
	Kind string
	// Payload is the canonical wire activity JSON, byte-stable after first write.
	Payload []byte
	// ParentATURI is the causal dependency (decision 15): a delivery for this
	// activity is ineligible until the parent's mapping is accepted. "" = none.
	ParentATURI string
	CreatedAt   time.Time
}

// OutboundDelivery is one delivery attempt of an activity to one inbox (task
// 15). It generalizes the inbox_events queue: ClaimedUntil is the same fencing
// token, OrderingKey (the community AP id) serializes deliveries per community,
// and the loose-index-scan head is the min-Seq pending row of a key.
type OutboundDelivery struct {
	// Seq is the monotonic ordering column the per-key serialization descends.
	Seq int64
	// ActivityID + TargetInbox are the composite primary key: one activity
	// fans out to many inboxes.
	ActivityID  string
	TargetInbox string
	// OrderingKey is the community AP id — deliveries sharing it are handled
	// strictly in Seq order.
	OrderingKey string
	// State is the delivery's fate (pending until terminal).
	State DeliveryState
	// Attempts counts how many times a worker claimed this delivery.
	Attempts int
	// NextAttemptAt is the retry-backoff schedule; claimable when <= now.
	NextAttemptAt time.Time
	// ClaimedUntil is the current worker lease AND the fencing/claim token;
	// nil/past means unclaimed. MarkDelivered/Release/MarkPoisoned require it.
	ClaimedUntil *time.Time
	// DeliveredAt stamps the successful delivery.
	DeliveredAt *time.Time
	// LastStatusCode is the last HTTP status seen (nil before any attempt
	// produced one).
	LastStatusCode *int
	// LastErrorClass is a coarse retry-taxonomy label (transport, 4xx, 5xx,
	// duplicate, attempt_cap, parent_unaccepted, parent_poisoned).
	LastErrorClass string
	// ResponseExcerpt is a bounded sample of the peer's response body.
	ResponseExcerpt string
	CreatedAt       time.Time
	UpdatedAt       time.Time
}
