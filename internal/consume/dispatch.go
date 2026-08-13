package consume

import (
	"context"
	"database/sql"
	"log/slog"

	"tidepool/internal/store"
)

// STUB (task 14 RED): the seams and the deterministic-id contract are pinned
// here; every handler body is still missing.

// ActorMinter is the lazy-mint seam onto task 13's personas service.
// *personas.Service satisfies it. Minting happens at the FIRST federating
// interaction (a comment, a vote) — never eagerly on an opt-in event.
type ActorMinter interface {
	CreateActorForDID(ctx context.Context, did, handle string) (*store.APActor, error)
}

// Intent is one outbound activity the consumer decided on. Intents are TYPED
// rather than pre-rendered AP: translation into ActivityPub vocabulary belongs
// to task 15, which owns the wire format.
type Intent interface {
	// ActivityID is the deterministic AP activity id this intent will be
	// delivered under (see ActivityID).
	ActivityID() string
}

// CommentIntent is a Create/Update/Delete of a native comment.
type CommentIntent struct {
	// Op is the commit operation: create, update or delete.
	Op string
	// ATURI is the comment record's at-uri.
	ATURI string
	// ID is the deterministic activity id.
	ID string
	// CommunityAPID is the target community's AP Group id.
	CommunityAPID string
	// ParentAPID is the AP object id of the thing replied to, resolved
	// through ap_objects (either origin).
	ParentAPID string
	// Snapshot is the translated state a Delete is rebuilt from — the delete
	// commit itself carries no record body.
	Snapshot []byte
}

// ActivityID reports the deterministic activity id.
func (i CommentIntent) ActivityID() string { return i.ID }

// VoteIntent is a Like/Dislike, or the Undo of one.
type VoteIntent struct {
	// Op is the commit operation: create or delete.
	Op string
	// VoteATURI is the vote record's at-uri — the delete path's lookup key.
	VoteATURI string
	// SubjectAPID is the AP object id of the thing voted on.
	SubjectAPID string
	// Direction is up or down, read back from outbound_votes on the delete
	// path (the delete commit tells us nothing else).
	Direction string
	// ID is the deterministic activity id.
	ID string
	// CommunityAPID is the target community's AP Group id.
	CommunityAPID string
}

// ActivityID reports the deterministic activity id.
func (i VoteIntent) ActivityID() string { return i.ID }

// OutboundEnqueuer is the task 15 seam. main.go wires a logging noop until
// task 15 swaps in the real delivery queue.
type OutboundEnqueuer interface {
	// EnqueueActivity hands one intent to delivery. orderingKey serializes
	// causally related work; parentATURI carries the causal dependency
	// (decision 15) so a reply is never delivered before its parent.
	EnqueueActivity(ctx context.Context, actorDID, orderingKey, parentATURI string, intent Intent) error
}

// Options configures a Dispatcher.
type Options struct {
	// DB is the bridge database.
	DB *sql.DB
	// Actors is the lazy-mint seam (task 13's personas service).
	Actors ActorMinter
	// Enqueuer is the task 15 outbound seam.
	Enqueuer OutboundEnqueuer
	// UserOrigin is AP_USER_ORIGIN: the origin every deterministic activity
	// id is minted under.
	UserOrigin string
	// Logger receives the drop reasons nobody sees from a metric.
	Logger *slog.Logger
}

// Dispatcher is the EventHandler this consumer runs: it routes one Jetstream
// event to the right per-collection handler, filters Tidepool-hosted repos,
// and is idempotent under full replay.
type Dispatcher struct {
	db         *sql.DB
	actors     ActorMinter
	enqueuer   OutboundEnqueuer
	userOrigin string
	logger     *slog.Logger
}

var _ EventHandler = (*Dispatcher)(nil)

// NewDispatcher builds the event dispatcher.
func NewDispatcher(opts Options) (*Dispatcher, error) {
	return &Dispatcher{
		db:         opts.DB,
		actors:     opts.Actors,
		enqueuer:   opts.Enqueuer,
		userOrigin: opts.UserOrigin,
		logger:     opts.Logger,
	}, nil // STUB: no validation
}

// HandleEvent routes one Jetstream event.
func (d *Dispatcher) HandleEvent(ctx context.Context, event *JetstreamEvent) error {
	return nil // STUB
}

// ActivityID derives the deterministic outbound activity id for one operation
// on one record (decision 12):
//
//	{origin}/ap/activity/{sha256(atURI + op + seq)}
//
// It is deterministic on purpose: a redelivery must reuse the id a peer has
// already seen, and a Delete must be buildable long after the record body is
// gone. seq comes from outbound_objects.last_activity_seq / outbound_votes
// .activity_seq — NOT from the CID, because deletes have none.
//
// This is the ONE exported id function; tasks 15/16/17 all call it.
func ActivityID(origin, atURI, op string, seq int) string {
	return "" // STUB
}
