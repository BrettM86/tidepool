package consume

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

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
	// InnerActivityID is the id of the Like/Dislike an Undo withdraws. An
	// Undo has to embed the activity it undoes, and by the time the delete
	// arrives the vote record is gone — so this is read back from
	// outbound_votes.current_activity_id, not recomputed.
	InnerActivityID string
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

// AcceptanceEngine is the task 16 seam. A native post to a bridged community
// is a community.postv2 record in the AUTHOR's repo; admission, the acceptance
// write in the community repo, and the outbound enqueue all ride ONE commit,
// which is why this handler hands the whole commit over rather than doing any
// of it itself. The engine, not this consumer, owns outbound_objects rows for
// posts.
type AcceptanceEngine interface {
	AdmitPost(ctx context.Context, did string, commit *CommitEvent) error
}

// RemoteContentDeleter is the task 17 DESTRUCTIVE seam: it asks peers to
// delete a user's already-federated content (federation record
// deleteRemote=true). Irreversible on the remote side — peers that honor it
// cannot restore what they dropped — so it is reached only from an explicit
// enabled=false + deleteRemote=true record, never inferred.
//
// Optional: a nil deleter means the deployment has no destructive tier wired
// yet. The preference is still RECORDED, so task 17 can act on the backlog
// when it lands.
type RemoteContentDeleter interface {
	DeleteRemoteContent(ctx context.Context, did string) error
}

// AccountTerminator is the task 17 TERMINAL seam: a repo whose account status
// is "deleted" (decision 19) means the user is gone, and their federated
// identity has to be withdrawn with Delete{Person}.
//
// It is a seam rather than an inline write because acting on a stale event is
// unrecoverable: the terminal tier re-verifies against PLC/the PDS before
// sending anything. Optional — a nil terminator means the tier is not wired
// yet, which is announced rather than silently treated as a pause.
type AccountTerminator interface {
	TerminateAccount(ctx context.Context, did string) error
}

// Options configures a Dispatcher.
type Options struct {
	// DB is the bridge database.
	DB *sql.DB
	// Actors is the lazy-mint seam (task 13's personas service).
	Actors ActorMinter
	// Enqueuer is the task 15 outbound seam.
	Enqueuer OutboundEnqueuer
	// Engine is the task 16 acceptance seam for community.postv2 events.
	// Optional: a nil engine drops postv2 events at debug.
	Engine AcceptanceEngine
	// RemoteDeleter is the task 17 destructive seam. Optional (see
	// RemoteContentDeleter).
	RemoteDeleter RemoteContentDeleter
	// Terminator is the task 17 terminal seam for status=deleted accounts.
	// Optional (see AccountTerminator).
	Terminator AccountTerminator
	// Records reads committed records so a subject's community can be derived
	// for mappings written before migration 016 filled community_did.
	// *repo.Manager satisfies it. Optional — see subjectCommunityDID.
	Records materialize.RecordGetter
	// Resolver verifies a DID's handle before the FIRST mint. Required: the
	// local part is frozen at creation, so minting without a verified handle
	// would freeze a guess.
	Resolver DIDResolver
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
	db            *sql.DB
	actors        ActorMinter
	enqueuer      OutboundEnqueuer
	engine        AcceptanceEngine
	remoteDeleter RemoteContentDeleter
	terminator    AccountTerminator
	resolver      DIDResolver
	prefs         store.FederationPrefs
	apActors      store.APActors
	// objects is the outbound state; objectMappings and communities are the
	// bridge's own record of what it already federated, which is where a
	// comment's thread and target community are resolved FROM.
	objectMappings store.APObjects
	objects        store.OutboundObjects
	votes          store.OutboundVotes
	communities    store.Communities
	records        materialize.RecordGetter
	hosted         *hostedRepos
	gate           *RevGate
	userOrigin     string
	logger         *slog.Logger
}

var _ EventHandler = (*Dispatcher)(nil)

// NewDispatcher builds the event dispatcher. DB, Actors, Enqueuer and
// UserOrigin are required; Engine and RemoteDeleter are the not-yet-landed
// seams and may be nil.
func NewDispatcher(opts Options) (*Dispatcher, error) {
	switch {
	case opts.DB == nil:
		return nil, errors.NewValidationError("DB", "must not be nil")
	case opts.Actors == nil:
		// Without the mint seam a first-time commenter's comment would be
		// federated from an identity that does not exist.
		return nil, errors.NewValidationError("Actors", "must not be nil")
	case opts.Enqueuer == nil:
		return nil, errors.NewValidationError("Enqueuer", "must not be nil")
	case opts.Resolver == nil:
		// Minting without a verified handle would freeze a guessed local part
		// forever, so there is no safe default to fall back to.
		return nil, errors.NewValidationError("Resolver", "must not be nil")
	case opts.UserOrigin == "":
		// Every outbound activity id is minted under this origin, and the id
		// is a wire contract: deriving one under "" would publish ids no peer
		// can resolve back to this bridge.
		return nil, errors.NewValidationError("UserOrigin", "must not be empty")
	}

	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Dispatcher{
		db:             opts.DB,
		actors:         opts.Actors,
		enqueuer:       opts.Enqueuer,
		engine:         opts.Engine,
		remoteDeleter:  opts.RemoteDeleter,
		terminator:     opts.Terminator,
		resolver:       opts.Resolver,
		prefs:          store.NewFederationPrefs(opts.DB),
		apActors:       store.NewAPActors(opts.DB),
		objectMappings: store.NewAPObjects(opts.DB),
		objects:        store.NewOutboundObjects(opts.DB),
		votes:          store.NewOutboundVotes(opts.DB),
		communities:    store.NewCommunities(opts.DB),
		records:        opts.Records,
		hosted:         newHostedRepos(opts.DB),
		gate:           NewRevGate(opts.DB),
		userOrigin:     opts.UserOrigin,
		logger:         logger,
	}, nil
}

// HandleEvent routes one Jetstream event. Every outcome that is not a storage
// failure is a SKIP that returns nil: a skip means the event is fully
// accounted for and the cursor must advance past it, while an error blocks the
// cursor and eventually dead-letters.
func (d *Dispatcher) HandleEvent(ctx context.Context, event *JetstreamEvent) error {
	switch event.Kind {
	case eventKindCommit:
		return d.handleCommit(ctx, event)
	case eventKindAccount:
		return d.handleAccount(ctx, event)
	case eventKindIdentity:
		return d.handleIdentity(ctx, event)
	default:
		d.logger.Debug("jetstream event kind not handled",
			slog.String("kind", event.Kind), slog.String("did", event.DID))
		return nil
	}
}

// Jetstream event kinds.
const (
	eventKindCommit   = "commit"
	eventKindAccount  = "account"
	eventKindIdentity = "identity"
)

// commitHandler applies one commit for a repo. Handlers run INSIDE the rev
// gate's claim, so they may assume the event is the newest one seen for that
// record URI and need no ordering logic of their own.
type commitHandler func(ctx context.Context, did string, commit *CommitEvent) error

// commitHandlerFor maps a collection to its handler, or nil when this
// dispatcher does not handle the collection. It is the SINGLE place that
// decides what is handled, so the pre-gate skip and the in-gate dispatch can
// never disagree about it.
func (d *Dispatcher) commitHandlerFor(collection string) commitHandler {
	switch collection {
	case CollectionFederation:
		return d.handleFederation
	case CollectionPostV2:
		return d.handlePostV2
	case CollectionComment:
		return d.handleComment
	case CollectionProfile:
		return d.handleProfile
	case CollectionVote:
		return d.handleVote
	}
	return nil
}

// handleCommit runs the two filters every commit passes before any handler
// sees it, then applies the handler under the rev gate.
//
// The ORDER of the three stages is the whole design:
//
//  1. unhandled collection → skip. Jetstream's wantedCollections should keep
//     these off the wire, but a shared feed or a redriven legacy frame still
//     delivers one, and dead-lettering every unrelated record would bury the
//     queue in noise.
//  2. Tidepool-hosted repo → skip, UPSTREAM of the gate. Both skips must
//     happen before the claim: a gate row written for an event no handler ran
//     would outlive this event and silently reject the legitimate one that
//     later carries the same record URI.
//  3. everything else → applyGated, which wraps ALL commit handling. The claim
//     is held across the handler, so a replay is rejected before the handler
//     runs rather than relying on each handler to be independently idempotent.
func (d *Dispatcher) handleCommit(ctx context.Context, event *JetstreamEvent) error {
	commit := event.Commit
	if commit == nil {
		// A commit frame with no commit body is malformed beyond repair;
		// retrying cannot make one appear.
		return fmt.Errorf("%w: commit event for %s carries no commit", ErrPermanentEvent, event.DID)
	}

	handler := d.commitHandlerFor(commit.Collection)
	if handler == nil {
		d.logger.Debug("skipping unhandled collection",
			slog.String("collection", commit.Collection), slog.String("did", event.DID))
		return nil
	}

	hosted, err := d.hosted.IsHosted(ctx, event.DID)
	if err != nil {
		return err
	}
	if hosted {
		// Tidepool's own commits — bridged content, acceptance and removal
		// records — already enqueued their outbound at write time. Consuming
		// them back would deliver everything twice.
		d.logger.Debug("skipping Tidepool-hosted repo",
			slog.String("did", event.DID), slog.String("collection", commit.Collection))
		return nil
	}

	return applyGated(ctx, d.gate, ConsumerNative, event.DID, commit, func() error {
		return handler(ctx, event.DID, commit)
	})
}

// handlePostV2 hands a native post to the task 16 acceptance engine. Admission,
// the acceptance write and the outbound enqueue must ride ONE commit, which
// only the engine can do — so this consumer hands over the whole commit and
// owns none of it, not even the outbound_objects row. Keeping no post state
// here is also what makes the lexicon's community-immutability rule
// enforceable in ONE place: there is no second copy of the answer to disagree
// with the engine's.
func (d *Dispatcher) handlePostV2(ctx context.Context, did string, commit *CommitEvent) error {
	if d.engine == nil {
		d.logger.Debug("no acceptance engine wired; skipping postv2",
			slog.String("did", did), slog.String("rkey", commit.RKey))
		return nil
	}

	// A DELETE passes through ungated. It carries no record, so there is no
	// community field to check — and gating on one it cannot see would drop
	// every author delete and strand the acceptance records those deletes
	// exist to take down. The engine already knows which posts it accepted
	// and can no-op the rest.
	if commit.Operation != operationDelete {
		communityDID := stringField(commit.Record, "community")
		if communityDID == "" {
			// The lexicon REQUIRES community. A post without one is malformed
			// and belongs in the DLQ, where a lexicon rollout mistake stays
			// visible instead of becoming a silent drop.
			return fmt.Errorf("%w: postv2 %s names no community", ErrPermanentEvent, commit.RKey)
		}
		bridged, err := d.isBridgedCommunity(ctx, communityDID)
		if err != nil {
			return err
		}
		if !bridged {
			// Most Coves posts are exactly this. Admitting one would write an
			// acceptance record into a community repo that has no business
			// existing.
			d.logger.Debug("skipping postv2 for a non-bridged community",
				slog.String("did", did), slog.String("community", communityDID))
			return nil
		}
	}

	return d.engine.AdmitPost(ctx, did, commit)
}

// isBridgedCommunity reports whether Tidepool federates the community. The
// communities table is the authority: a row is what makes a community bridged.
func (d *Dispatcher) isBridgedCommunity(ctx context.Context, communityDID string) (bool, error) {
	_, err := d.communities.GetByDID(ctx, communityDID)
	if errors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("look up community %s: %w", communityDID, err)
	}
	return true, nil
}

// handleAccount applies a #account status change (decision 19). Status is what
// distinguishes the transient states from deletion: active=false alone means
// deactivated, suspended, takendown or throttled, none of which is a deletion,
// so delivery pauses while the identity — and every federated reference to it
// — stays intact for when the user comes back.
func (d *Dispatcher) handleAccount(ctx context.Context, event *JetstreamEvent) error {
	account := event.Account
	if account == nil {
		return fmt.Errorf("%w: account event for %s carries no account", ErrPermanentEvent, event.DID)
	}
	did := account.DID
	if did == "" {
		did = event.DID
	}

	// The actor check comes first for EVERY status. Nothing was ever federated
	// under a DID with no AP identity, so there is no delivery to pause and
	// nothing for the terminal tier to withdraw — and minting an actor in
	// order to pause or delete it would create the very identity the event is
	// about losing.
	if _, err := d.apActors.GetByDID(ctx, did); err != nil {
		if errors.IsNotFound(err) {
			d.logger.Debug("account status for a DID with no actor",
				slog.String("did", did), slog.String("status", account.Status))
			return nil
		}
		return fmt.Errorf("look up actor for %s: %w", did, err)
	}

	if account.Status == accountStatusDeleted {
		// The ONE status that means gone. It goes to the tier that re-verifies
		// against PLC and the PDS before sending Delete{Person}, because
		// acting on a stale deletion event is unrecoverable.
		if d.terminator == nil {
			// Announced, and deliberately NOT degraded into a pause: a
			// deletion half-handled as a pause looks handled in the database
			// and is not.
			d.logger.Warn("account reported deleted but no terminal tier is wired",
				slog.String("did", did))
			return nil
		}
		if err := d.terminator.TerminateAccount(ctx, did); err != nil {
			return fmt.Errorf("terminate account %s: %w", did, err)
		}
		return nil
	}

	// Everything else is transient — deactivated, suspended, takendown,
	// throttled are all states a user comes back from. Delivery stops; the
	// identity, and every federated reference to it, survives.
	err := d.apActors.SetPaused(ctx, did, !account.Active)
	if errors.IsNotFound(err) {
		return nil // the actor vanished between the check and the write
	}
	return err
}

// accountStatusDeleted is the ONLY #account status that means deletion.
const accountStatusDeleted = "deleted"

// activityIDVersionTag prefixes every activity-id preimage. It exists so the
// derivation can CHANGE without colliding with ids already published: bump the
// tag and every id is different, rather than a new scheme silently producing
// an old id for a different activity.
const activityIDVersionTag = "tidepool:activity:v1"

// ActivityID derives the deterministic outbound activity id for one operation
// on one record (decision 12). The preimage is newline-DELIMITED, not
// concatenated:
//
//	{origin}/ap/activity/{hex(sha256(tag "\n" atURI "\n" op "\n" seq))}
//
// The delimiters are load-bearing. Plain concatenation makes ("…/3lz",
// "create", 12) and ("…/3lzcreate1", "", 2) hash identically, so two different
// activities would claim one id and the second would be silently swallowed by
// a peer that already has the first.
//
// It is deterministic on purpose: a redelivery must reuse the id a peer has
// already seen, and a Delete must be buildable long after the record body is
// gone. seq comes from outbound_objects.last_activity_seq / outbound_votes
// .activity_seq — NOT from the CID, because deletes have none.
//
// This is the ONE exported id function; tasks 15/16/17 all call it.
func ActivityID(origin, atURI, op string, seq int) string {
	preimage := strings.Join([]string{
		activityIDVersionTag,
		atURI,
		op,
		strconv.Itoa(seq),
	}, "\n")
	digest := sha256.Sum256([]byte(preimage))
	return origin + "/ap/activity/" + hex.EncodeToString(digest[:])
}
