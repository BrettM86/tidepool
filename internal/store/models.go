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
	APID           string     // canonical AP object id (URL)
	APType         string     // AP type: Page, Note, Group, Person, ...
	OriginInstance string     // host the object originated from, e.g. lemmy.world
	Origin         Origin     // which side authored the object; defaults to fediverse
	DID            string     // repo the record was written into
	Collection     string     // record NSID, e.g. social.coves.community.post
	RKey           string     // deterministic TID rkey
	ATURI          string     // at://did/collection/rkey (derived; set by PutMapping)
	CID            string     // CID of the current record version
	PublishedAt    *time.Time // AP `published` time (may be absent upstream)
	IndexedAt      time.Time
	DeletedAt      *time.Time
}

// IsDeleted reports whether the mapping has been soft-deleted.
func (m *APObjectMapping) IsDeleted() bool { return m.DeletedAt != nil }

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
}

// InboxEvent is a received AP activity, recorded for dedupe and
// processing bookkeeping.
type InboxEvent struct {
	ID          int64
	ActivityID  string // AP activity id (URL) — the dedupe key
	Type        string // AP activity type: Announce, Create, Like, ...
	ReceivedAt  time.Time
	ProcessedAt *time.Time
	Error       string // last processing error; empty if none
}
