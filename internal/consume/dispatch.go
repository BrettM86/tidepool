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

	"github.com/bluesky-social/indigo/atproto/syntax"

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
	// CommunityDID is that same community's bridged repo DID. It rides the
	// intent so the enqueuer can BIND the mapping it writes to a community
	// without growing a Communities dependency: every caller already holds the
	// resolved community, and the binding is what later authorizes announced
	// moderation of this object (materialize.CommunityDIDOf).
	CommunityDID string
	// ParentAPID is the AP object id of the thing replied to, resolved through
	// ap_objects OR the parent's own outbound state (a native accepted post or
	// an earlier native comment, which have no ap_objects mapping).
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

// PostIntent is a Create/Update/Delete of a native post (a bridged
// social.coves.community.postv2 record). Task 15 OWNS the Page translation, but
// this consumer does not construct a PostIntent: a post rides the acceptance
// engine (task 16), which admits the post, writes the acceptance record, and
// constructs the PostIntent for the same outbound enqueue — so the shape lives
// here (beside its Comment/Vote siblings) while the producer lives there.
type PostIntent struct {
	// Op is the commit operation: create, update or delete.
	Op string
	// ATURI is the post record's at-uri.
	ATURI string
	// ID is the deterministic activity id.
	ID string
	// CommunityAPID is the target community's AP Group id — for a Page it goes
	// in `to` (the Page/Note addressing split), not `cc`.
	CommunityAPID string
	// CommunityDID is that same community's bridged repo DID; see
	// CommentIntent.CommunityDID for why it travels on the intent.
	CommunityDID string
	// Snapshot is the translated state a Delete is rebuilt from and a
	// Create/Update{Page} is rendered from — the postv2 record plus resolved
	// context, same envelope shape as a comment's snapshot.
	Snapshot []byte
}

// ActivityID reports the deterministic activity id.
func (i PostIntent) ActivityID() string { return i.ID }

// PersonDeleteIntent withdraws a native user's whole identity from the
// fediverse: Delete{Person, removeData: true}, the destructive opt-out tier
// (decision 11's second tier, task 17d).
//
// It is ONE activity addressed to MANY inboxes — every instance this actor's
// content ever reached — which is what makes it the only intent here with no
// CommunityAPID: the targets come from the delivery history, not from a
// community, and the canonical payload is shared by every one of them.
//
// The shape lives here beside its siblings while the producer lives in
// outbound (the Purger), exactly as PostIntent's does.
//
// IRREVERSIBLE. Lemmy un-deletes a person on refetch; other software does not,
// and nothing in this codebase may promise resurrection.
type PersonDeleteIntent struct {
	// ActorDID is the user being withdrawn.
	ActorDID string
	// ID is the deterministic activity id.
	ID string
}

// ActivityID reports the deterministic activity id.
func (i PersonDeleteIntent) ActivityID() string { return i.ID }

// OutboundEnqueuer is the task 15 seam. main wires the real persisting enqueuer
// (outbound.Enqueuer, which writes outbound_activities/deliveries on the gate
// tx) whenever CONSUMER_ENABLED; the noop is the consumer-disabled default.
type OutboundEnqueuer interface {
	// EnqueueActivity hands one intent to delivery ON THE CALLER'S TX — the
	// enqueue must commit with the rev-gate advance the consumer is holding, or
	// a rolled-back gate would strand an activity a replay cannot reproduce.
	// orderingKey serializes causally related work; parentATURI carries the
	// causal dependency (decision 15) so a reply is never delivered before its
	// parent.
	EnqueueActivity(ctx context.Context, tx *sql.Tx, actorDID, orderingKey, parentATURI string, intent Intent) error
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
	// The store overrides below exist for fault injection in tests: each is
	// nil in production and constructed from DB. A test wraps the real store
	// in a double that fails one method, to prove the handler PROPAGATES the
	// failure (retry/redrive) rather than swallowing it into a default value.
	Objects        store.OutboundObjects
	Votes          store.OutboundVotes
	Communities    store.Communities
	ObjectMappings store.APObjects
	// Deliveries is the outbound queue the opt-out cancels an actor's pending
	// work in — on the rev-gate transaction, together with the actor mirror.
	Deliveries store.OutboundDeliveries
	// Bans reads whether the community a comment or vote is bound for has banned
	// its author (task 17c-3 review). The acceptance engine gates POSTS; nothing
	// else does, and a banned author's replies and votes enqueue to a community
	// that refuses them until each delivery poisons.
	Bans store.CommunityBans
	// Moderation is the bridge-owned moderation state the comment path reads to
	// refuse a reply in a locked thread. It is a SEPARATE store from
	// ObjectMappings on purpose: this consumer only ever reads it, while the
	// interface carries the mutators the ingest side writes with, and the
	// mapping store is held by half the bridge to resolve strongRefs.
	Moderation store.ObjectModeration
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
	moderation     store.ObjectModeration
	bans           store.CommunityBans
	deliveries     store.OutboundDeliveries
	records        materialize.RecordGetter
	hosted         *hostedRepos
	gate           *RevGate
	userOrigin     string
	logger         *slog.Logger
}

var _ EventHandler = (*Dispatcher)(nil)

// orDefault returns override when it is non-nil, else fallback. It exists so
// the fault-injection Options can replace one store without every call site
// spelling out the nil check.
func orDefault[T comparable](override, fallback T) T {
	var zero T
	if override != zero {
		return override
	}
	return fallback
}

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
	if opts.Engine == nil {
		// Warned ONCE, at construction, rather than on every postv2 frame: a
		// deployment without the acceptance engine (task 16) skips postv2
		// events pre-gate, and an operator should see that surface stated
		// plainly instead of inferring it from a silence in the metrics.
		logger.Warn("no acceptance engine wired: community.postv2 events will be skipped until task 16 lands")
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
		objectMappings: orDefault[store.APObjects](opts.ObjectMappings, store.NewAPObjects(opts.DB)),
		objects:        orDefault[store.OutboundObjects](opts.Objects, store.NewOutboundObjects(opts.DB)),
		votes:          orDefault[store.OutboundVotes](opts.Votes, store.NewOutboundVotes(opts.DB)),
		communities:    orDefault[store.Communities](opts.Communities, store.NewCommunities(opts.DB)),
		moderation:     orDefault[store.ObjectModeration](opts.Moderation, store.NewObjectModeration(opts.DB)),
		bans:           orDefault[store.CommunityBans](opts.Bans, store.NewCommunityBans(opts.DB)),
		deliveries:     orDefault[store.OutboundDeliveries](opts.Deliveries, store.NewOutboundDeliveries(opts.DB)),
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
	// time_us is the CURSOR position, and it is validated for EVERY kind before
	// anything else runs. A parseable frame carrying 0 (or negative) would let
	// the cursor sit at or before every retained event and replay the entire
	// store on the next reconnect. This can never become valid on retry.
	if event.TimeUS <= 0 {
		return fmt.Errorf("%w: time_us must be positive, got %d", ErrPermanentEvent, event.TimeUS)
	}

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
// record URI and need no ordering logic of their own. tx is that claim's
// transaction: a handler that writes durable outbound state writes it on tx
// (UpsertTx/TombstoneTx) so the write, the gate advance and the enqueue commit
// as one unit. Handlers that write nothing durable ignore it.
type commitHandler func(ctx context.Context, tx *sql.Tx, did string, commit *CommitEvent) error

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

	// SECURITY: the envelope is validated BEFORE any storage access. A commit
	// is attacker-influenced — a native user writes the record it carries — and
	// a did or rkey with a NUL byte reaches a TEXT column, turns an INSERT into
	// a transient error retried forever, and (if the error echoes the NUL) the
	// dead-letter fallback fails too, wedging the whole consumer on one frame.
	// Rejecting it here, permanently and with a sanitized message, dead-letters
	// it cleanly and the cursor moves on. This runs before the gate, so a
	// rejected event claims no gate row to shadow the legitimate record that
	// later reuses the URI.
	if err := validateCommitEnvelope(event.DID, commit); err != nil {
		return err
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

	// A postv2 with no acceptance engine wired is skipped BEFORE the gate, not
	// inside a handler: the whole point of leaving it unhandled is that a later
	// build WITH the engine replays and admits it, and a gate row claimed here
	// would make that replay a silent no-op, dropping the post forever.
	if commit.Collection == CollectionPostV2 && d.engine == nil {
		d.logger.Debug("skipping postv2: no acceptance engine wired",
			slog.String("did", event.DID), slog.String("rkey", commit.RKey))
		return nil
	}

	return applyGatedTx(ctx, d.gate, ConsumerNative, event.DID, commit, func(tx *sql.Tx) error {
		return handler(ctx, tx, event.DID, commit)
	})
}

// validateCommitEnvelope rejects a structurally malformed commit as PERMANENT,
// with a message that names the offending field and NEVER echoes raw bytes
// (strconv.Quote escapes a NUL to the four printable characters `\x00`, so the
// message stays NUL-free and valid UTF-8 for the last_error TEXT column and for
// an operator reading the DLQ).
func validateCommitEnvelope(did string, commit *CommitEvent) error {
	if _, err := syntax.ParseDID(did); err != nil {
		return fmt.Errorf("%w: repo DID %s is not a valid DID", ErrPermanentEvent, strconv.Quote(did))
	}
	switch commit.Operation {
	case operationCreate, operationUpdate, operationDelete:
	default:
		return fmt.Errorf("%w: commit operation %s is not create, update or delete",
			ErrPermanentEvent, strconv.Quote(commit.Operation))
	}
	if commit.Rev == "" {
		// A real wire frame always carries rev; an empty one would bypass the
		// gate (empty rev is the bypass sentinel) and replay forever.
		return fmt.Errorf("%w: commit for %s carries no rev", ErrPermanentEvent, strconv.Quote(commit.RKey))
	}
	if _, err := syntax.ParseRecordKey(commit.RKey); err != nil {
		return fmt.Errorf("%w: commit rkey %s is not a valid record key",
			ErrPermanentEvent, strconv.Quote(commit.RKey))
	}
	if commit.Operation != operationDelete && commit.CID == "" {
		// A create/update names the CID of what it wrote; missing means the
		// frame is truncated or forged.
		return fmt.Errorf("%w: %s commit for %s carries no CID",
			ErrPermanentEvent, commit.Operation, strconv.Quote(commit.RKey))
	}
	return nil
}

// handlePostV2 hands a native post to the task 16 acceptance engine. Admission,
// the acceptance write and the outbound enqueue must ride ONE commit, which
// only the engine can do — so this consumer hands over the whole commit and
// owns none of it, not even the outbound_objects row. Keeping no post state
// here is also what makes the lexicon's community-immutability rule
// enforceable in ONE place: there is no second copy of the answer to disagree
// with the engine's.
func (d *Dispatcher) handlePostV2(ctx context.Context, _ *sql.Tx, did string, commit *CommitEvent) error {
	// The nil-engine skip happens before the gate (see handleCommit), so a
	// non-nil engine is guaranteed here.

	// A DELETE passes through ungated on both the community check AND the
	// opt-out: it carries no record, so there is no community field to check —
	// and gating it would drop every author delete and strand the acceptance
	// records those deletes exist to take down. A delete is a retraction, the
	// only way an opted-out author takes down what is already federated, so it
	// must reach the engine even from a user who has since opted out. The
	// engine already knows which posts it accepted and can no-op the rest.
	if commit.Operation != operationDelete {
		// The opt-out check MOVED into the engine (task 16): an opted-out
		// author's post REACHES the engine, which records a distinct rejection
		// (decision_code opted-out) in the admissions ledger rather than the
		// consumer dropping it silently at debug. post.getStatus and the admin
		// surface both need the "why", and only the engine writes it. The
		// consumer keeps just one pre-gate check: a post must name a bridged
		// community for the engine to have a repo to reject it INTO.
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

	// The envelope DID is authoritative. A nested payload naming a DIFFERENT
	// DID is malformed or hostile — acting on the inner one lets a frame about
	// DID A mutate DID B — and cannot resolve itself on retry, so it is
	// rejected as permanent before any state is touched.
	if account.DID != "" && account.DID != event.DID {
		return fmt.Errorf("%w: account payload DID %s disagrees with the envelope DID %s",
			ErrPermanentEvent, strconv.Quote(account.DID), strconv.Quote(event.DID))
	}
	did := event.DID

	// seq is the ORDERING position, and it is validated here for the same
	// reason time_us is validated for every kind above. last_account_seq
	// defaults to 0 and the gate below skips anything at or below it, so a
	// frame carrying seq 0 — or none at all, which decodes to the same zero —
	// is indistinguishable from a stale replay ON THE VERY FIRST EVENT: a
	// Jetstream build or proxy that stopped emitting seq would take the whole
	// account tier dark, every frame logged as "stale or duplicate", metrics
	// clean. No retry adds a seq to a frame that has none, so it is dead-
	// lettered where an operator can see it instead of being skipped in silence.
	if account.Seq <= 0 {
		return fmt.Errorf("%w: account event for %s carries no positive seq, got %d",
			ErrPermanentEvent, strconv.Quote(did), account.Seq)
	}

	// The actor check comes first for EVERY status. Nothing was ever federated
	// under a DID with no AP identity, so there is no delivery to pause and
	// nothing for the terminal tier to withdraw — and minting an actor in
	// order to pause or delete it would create the very identity the event is
	// about losing. No seq is recorded on this skip: if the actor is later
	// minted, a redelivered event still applies.
	if _, err := d.apActors.GetByDID(ctx, did); err != nil {
		if errors.IsNotFound(err) {
			d.logger.Debug("account status for a DID with no actor",
				slog.String("did", did), slog.String("status", account.Status))
			return nil
		}
		return fmt.Errorf("look up actor for %s: %w", did, err)
	}

	// #account is UNGATED by the rev gate (it carries no rev), so its own
	// monotonic per-DID seq is the ordering guard: a stale replay from the
	// reconnect rewind (a pause at seq N redelivered after a reactivation at
	// N+1) must not flip a recovered user back. A seq at or below the last
	// applied one is a duplicate or a stale copy and is a no-op.
	//
	// The gate is CLAIMED rather than read (see applyAccountClaimed): the
	// redriver and the live connector share this dispatcher, so a read-then-act
	// gate lets both racers through — and on the deleted path both then reach a
	// seam that no peer can undo.
	return d.applyAccountClaimed(ctx, did, account.Seq, func(tx *sql.Tx) error {
		return d.applyAccountStatus(ctx, tx, did, account)
	})
}

// applyAccountStatus performs the transition for one #account frame. It runs
// under the claim, so it may assume this is the newest frame seen for the DID
// and that no other #account handler is running for it.
//
// ORDERING RULE, and it is the same one rev_gate.go's DEADLOCK NOTE states for
// commit handlers: the terminal tier opens its OWN transaction and writes the
// ap_actors row, so nothing here may write that row BEFORE the seam returns.
// Every write below happens after it.
func (d *Dispatcher) applyAccountStatus(ctx context.Context, tx *sql.Tx, did string, account *AccountEvent) error {
	if account.Status == accountStatusDeleted {
		// The ONE status that means gone. It goes to the tier that re-verifies
		// against PLC and the PDS before sending Delete{Person}, because
		// acting on a stale deletion event is unrecoverable.
		if d.terminator == nil {
			// Announced, and deliberately NOT degraded into a pause: a
			// deletion half-handled as a pause looks handled in the database
			// and is not.
			//
			// THE SEQ DOES NOT ADVANCE. Nothing was recorded and nothing was
			// sent, so advancing it would CONSUME the user's deletion: every
			// redelivery is then rejected as stale and a build that wires the
			// tier tomorrow can never act on the event that arrived today. The
			// sibling deleteRemote path can advance because it persists the
			// preference before reaching its seam; here the un-advanced seq is
			// the only record that the deletion is still owed.
			d.logger.Warn("account reported deleted but no terminal tier is wired; "+
				"the event is left unapplied so a build with the tier can still act on it",
				slog.String("did", did), slog.Int64("seq", account.Seq))
			return errAccountUnapplied
		}
		if err := d.terminator.TerminateAccount(ctx, did); err != nil {
			return fmt.Errorf("terminate account %s: %w", did, err)
		}
		return d.mirrorTerminalPreference(ctx, tx, did)
	}

	// Everything else is transient — deactivated, suspended, takendown,
	// throttled are all states a user comes back from. Delivery stops; the
	// identity, and every federated reference to it, survives.
	//
	// Note what this does NOT do: it never re-ENABLES an actor. Unpausing
	// restores delivery for an identity that still exists, while `enabled`
	// carries the terminal decisions — an opt-out, a confirmed deletion, a
	// withdrawal — and an active=true frame is not evidence that any of those
	// were reversed. A tombstoned actor's store refuses re-enabling outright;
	// this path simply never asks.
	return setActorPausedTx(ctx, tx, did, !account.Active)
}

// mirrorTerminalPreference brings the ap_actors mirror into agreement with
// federation_prefs after the terminal tier has run — the SAME invariant the
// opt-out door maintains (handleFederation writes the preference, then mirrors
// it onto the actor), applied at the deletion door, which never did.
//
// IT READS RATHER THAN DECIDES, because TerminateAccount returns nil for two
// opposite outcomes: a CONFIRMED deletion (which records enabled=false and, if
// the destructive seam is wired, tombstones the actor) and a confirmed LIVE
// account (which withdraws the tier's own stale request, leaving absence, which
// means default-on). The preference is the tier's own statement of which
// happened, so reading it is what keeps this from re-deciding a question that
// was answered against PLC.
//
// It COMPOSES with the purge rather than duplicating it: a wired destructive
// tier has already stamped tombstoned_at — terminal, and cleared by nothing —
// and this write agrees with it. An UNWIRED one leaves the preference as the
// only record, and without this the actor row went on saying the identity is
// live: still resolving through webfinger, still admissible, and a later
// active=true frame arriving at a row that never learned anything happened.
func (d *Dispatcher) mirrorTerminalPreference(ctx context.Context, tx *sql.Tx, did string) error {
	pref, err := d.prefs.Get(ctx, did)
	if errors.IsNotFound(err) {
		// Confirmed live: the tier withdrew its request and absence is
		// default-on. Disabling here would strand a user the confirm just
		// proved is still there.
		return nil
	}
	if err != nil {
		return fmt.Errorf("read federation preference for %s: %w", did, err)
	}
	if pref.Enabled {
		return nil
	}
	return d.mirrorActorEnabled(ctx, tx, did, false)
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
