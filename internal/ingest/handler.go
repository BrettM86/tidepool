package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"tidepool/internal/ap"
	"tidepool/internal/echo"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/ratelimit"
	"tidepool/internal/store"
)

// echoDropLogInterval throttles the echo-drop log (hostrouter's refusal log is
// the precedent). Echoes are rare by construction, so the sampler costs
// nothing in steady state — but a classifier that started matching genuine
// community traffic would emit one line per announced activity and bury the
// evidence that this log exists to preserve.
const echoDropLogInterval = time.Second

// Materializer is the slice of *materialize.Materializer the dispatcher
// drives (task 05's entry points).
type Materializer interface {
	MaterializePost(ctx context.Context, page *ap.Object) (*materialize.Result, error)
	MaterializeComment(ctx context.Context, note *ap.Object) (*materialize.Result, error)
	HandleUpdate(ctx context.Context, obj *ap.Object) (*materialize.Result, error)
	// HandleDelete branches actor vs content off a FRESH bridged_actors read;
	// HandleDeleteRecord is the content-only entry that never can. Callers
	// that already classified the target (handleDelete, SweepDeleted) use the
	// latter — see materialize.HandleDeleteRecord for the TOCTOU it closes.
	HandleDelete(ctx context.Context, apID string) error
	HandleDeleteRecord(ctx context.Context, apID string) error
	// RemovePost/RestorePost are the community-scoped moderation transitions:
	// they rewrite the community's acceptance and removal records and leave
	// the author's post where it is. Deleting content is a different verb, so
	// these are not reachable through the delete entry points above.
	//
	// reason may be EMPTY — Lemmy spells "removed, no reason given" as an
	// empty summary, and a summary-less delete from a non-author is treated as
	// a removal with none. Implementations omit the field from the record
	// rather than writing it blank: a blank reason renders in a moderation log
	// as an empty explanation instead of as no explanation.
	RemovePost(ctx context.Context, mapping *store.APObjectMapping, reason string) error
	RestorePost(ctx context.Context, mapping *store.APObjectMapping) error
	RefreshActor(ctx context.Context, actorRef *ap.Object) (*store.BridgedActor, error)
	RefreshCommunity(ctx context.Context, groupRef *ap.Object) (*store.Community, error)
	EnsureCommunity(ctx context.Context, groupRef *ap.Object) (*store.Community, error)
}

// Fetcher is the slice of *ap.Client the dispatcher uses to re-fetch
// objects it must not trust from a delivery. The same-authority variant is
// required wherever a fetch's ANSWER is the authorization decision — the
// delete sweep's 410 (sweep.go) and the restore's "the origin serves it
// again" (consent.go) — because a redirect off the object's own origin must
// not be able to answer those.
type Fetcher interface {
	FetchObject(ctx context.Context, iri string) (*ap.Object, error)
	FetchObjectSameAuthority(ctx context.Context, iri string) (*ap.Object, error)
}

// EchoClassifier answers whether an inbound envelope is the bridge's own
// traffic coming home (task 17a). *echo.Classifier satisfies it.
//
// It is an INTERFACE, like votes.VoterProbe, for one reason: the fail-safe this
// dispatcher owes — a classification that CANNOT be made must retry, never
// poison the event and never materialize — is only exercisable by injecting a
// classifier that fails, and a concrete type leaves that contract untestable.
type EchoClassifier interface {
	Classify(ctx context.Context, envelope *ap.Object) (echo.Identity, error)
}

// Backfiller is notified when a community's Follow is accepted (the
// backfill trigger). *Backfill implements it; tests inject recorders.
type Backfiller interface {
	TriggerAsync(community *store.Community, force bool)
}

// RecordGetter is the slice of the repo manager the dispatcher reads
// committed records back through: an announced comment delete is authorized
// by the comment's stored reply.root (see authorizeDelete), which no mapping
// column carries.
type RecordGetter interface {
	GetRecord(ctx context.Context, did, collection, rkey string) (record map[string]any, recordCID string, err error)
}

// HandlerOptions configures NewHandler. Materializer, Fetcher, Objects,
// Actors, Communities, Tombstones, Records, Votes, and ServiceActorID are
// required; Backfill and Logger are optional.
type HandlerOptions struct {
	Materializer Materializer
	Fetcher      Fetcher
	Objects      store.APObjects
	Actors       store.BridgedActors
	Communities  store.Communities
	Tombstones   store.Tombstones
	Records      RecordGetter
	Votes        VoteAggregator
	Backfill     Backfiller
	// Moderation is the bridge-owned moderation state announced Locks and
	// native-comment removals are recorded in (task 17c-2). Optional ONLY in the
	// wiring sense: when it is nil, NewHandler takes the moderation view of
	// Objects, which the postgres mapping store provides. A dispatcher that ends
	// up with neither refuses to moderate rather than silently dropping the
	// decision — see moderationState.
	Moderation store.ObjectModeration
	// Echo classifies inbound ids against the bridge's own serving surface so
	// an activity we sent never re-enters as content (task 17a).
	Echo EchoClassifier
	// ServiceActorID is the bridge's own AP actor id; Accepts must wrap a
	// Follow issued by it.
	ServiceActorID string
	Logger         *slog.Logger
}

// Handler dispatches verified, deduplicated inbox activities to the
// materializer, the vote aggregator, and the follow state machine. It is
// the queue's processor: a nil or IsSkip return marks the event processed,
// a validation error poisons it, anything else is retried with backoff.
type Handler struct {
	mat         Materializer
	fetcher     Fetcher
	objects     store.APObjects
	actors      store.BridgedActors
	communities store.Communities
	tombstones  store.Tombstones
	records     RecordGetter
	votes       VoteAggregator
	backfill    Backfiller
	moderation  store.ObjectModeration
	classifier  EchoClassifier
	echoLog     *ratelimit.Sampler
	serviceID   string
	logger      *slog.Logger
}

// NewHandler validates options and builds a Handler.
func NewHandler(opts HandlerOptions) (*Handler, error) {
	if opts.Materializer == nil {
		return nil, errors.NewValidationError("materializer", "must not be nil")
	}
	if opts.Fetcher == nil {
		return nil, errors.NewValidationError("fetcher", "must not be nil")
	}
	if opts.Objects == nil {
		return nil, errors.NewValidationError("objects", "must not be nil")
	}
	if opts.Actors == nil {
		return nil, errors.NewValidationError("actors", "must not be nil")
	}
	if opts.Communities == nil {
		return nil, errors.NewValidationError("communities", "must not be nil")
	}
	if opts.Tombstones == nil {
		return nil, errors.NewValidationError("tombstones", "must not be nil")
	}
	if opts.Records == nil {
		return nil, errors.NewValidationError("records", "must not be nil")
	}
	if opts.Votes == nil {
		return nil, errors.NewValidationError("votes", "must not be nil")
	}
	// REQUIRED, exactly as votes.NewAggregator requires its voter probe. The
	// same guard cannot be mandatory on one path and optional on another: a
	// dispatcher without it re-materializes our own content, mints bridged
	// actors for our own personas and self-moderates, silently, in whichever
	// binary forgot to pass it — which is how it was left out of production the
	// first time.
	if opts.Echo == nil {
		return nil, errors.NewValidationError("echo", "must not be nil")
	}
	if opts.ServiceActorID == "" {
		return nil, errors.NewValidationError("service_actor_id", "must not be empty")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	// The moderation state and the mapping store are two repositories over two
	// tables, and only the moderation paths may hold the first — so it is a
	// separate option rather than methods on APObjects, which the echo
	// classifier, the vote aggregator, the stats refresher, the materializer and
	// the enqueuer all hold to resolve strongRefs. The default keeps every
	// existing call site working: the postgres mapping store IS also that
	// repository, so callers that pass one and no moderation store get the
	// matching view of the same database rather than a nil.
	moderation := opts.Moderation
	if moderation == nil {
		if fromObjects, ok := opts.Objects.(store.ObjectModeration); ok {
			moderation = fromObjects
		}
	}
	return &Handler{
		mat:         opts.Materializer,
		fetcher:     opts.Fetcher,
		objects:     opts.Objects,
		actors:      opts.Actors,
		communities: opts.Communities,
		tombstones:  opts.Tombstones,
		records:     opts.Records,
		votes:       opts.Votes,
		backfill:    opts.Backfill,
		moderation:  moderation,
		classifier:  opts.Echo,
		echoLog:     ratelimit.NewSampler(echoDropLogInterval),
		serviceID:   opts.ServiceActorID,
		logger:      logger,
	}, nil
}

// Process handles one claimed queue event. The error contract mirrors the
// materializer's: nil or IsSkip → processed (skips are logged with their
// reason and never retried); IsValidation → poisoned; anything else →
// retryable.
func (h *Handler) Process(ctx context.Context, event *store.InboxEvent) error {
	if len(event.Payload) == 0 {
		return errors.NewValidationError("payload", "event carries no activity payload")
	}
	activity, err := ap.ParseObject(event.Payload)
	if err != nil {
		return errors.NewValidationError("payload", err.Error())
	}
	// The inbox verified the HTTP signature and bound the activity's actor
	// to the signer's authority; event.ActorID is that bound actor id.
	signer := event.ActorID
	if signer == "" {
		return errors.NewValidationError("actor_id", "event carries no verified actor")
	}

	switch activity.Type {
	case ap.TypeAnnounce:
		return h.handleAnnounce(ctx, activity, signer)
	case ap.TypeCreate, ap.TypeUpdate:
		return h.handleBareCreateUpdate(ctx, activity, signer)
	case ap.TypeDelete:
		// A bare Delete/Undo has no envelope for handleAnnounce's guard to
		// read, and the ordinary path each falls into is DESTRUCTIVE when the
		// target is one of our own ids: the delete lays a tombstone marker and
		// soft-deletes the mapping — which makes ResolveStrongRef answer
		// Tombstoned and silently drops every genuine Lemmy reply beneath the
		// post — while the undo's restore dereferences our own origin and
		// re-materializes what comes back, minting a bridged actor for our own
		// persona along the way.
		//
		// The suppression therefore runs HERE, before the handler: both lay
		// their marker before authorizing, so a check any later leaves behind
		// exactly the damage it was meant to prevent.
		if err := h.suppressEcho(ctx, activity.ID, activity); err != nil {
			return err
		}
		return h.handleDelete(ctx, activity, signer, nil)
	case ap.TypeUndo:
		// Same guard, same reason, on the branch whose restore path is the
		// more dangerous of the two (see the Delete case above).
		if err := h.suppressEcho(ctx, activity.ID, activity); err != nil {
			return err
		}
		return h.handleUndo(ctx, activity, signer, nil)
	case ap.TypeAccept:
		return h.handleAccept(ctx, activity, signer)
	case ap.TypeReject:
		return h.handleReject(ctx, activity, signer)
	case ap.TypeLike, ap.TypeDislike:
		// Bare votes (rare; Lemmy normally announces them via the group).
		// The inbox already bound this top-level activity's actor to the
		// signer's authority, so for a well-formed vote the check below is
		// redundant belt-and-braces; it keeps the dispatch layer's bare-vote
		// rule self-contained (and handleUndo, where the inner vote's actor
		// is NOT inbox-bound, shares it).
		if err := h.authorizeBareVote(activity.ID, activity, signer); err != nil {
			return err
		}
		return h.votes.ApplyVote(ctx, activity, "")
	default:
		return skip(activity.ID, "unsupported activity type "+activity.Type)
	}
}

// handleAnnounce unwraps FEP-1b12 group fan-out: Announce{Create|Update|
// Delete|Undo|Like|Dislike|...} from a community we follow.
func (h *Handler) handleAnnounce(ctx context.Context, announce *ap.Object, signer string) error {
	// Only communities the bridge subscribed to may push content. pending is
	// accepted too: Lemmy can start announcing before we processed its
	// Accept (both arrive on the same ordering key, but a re-delivered
	// Announce may overtake).
	community, err := h.communities.GetByAPGroupID(ctx, signer)
	if errors.IsNotFound(err) {
		return skip(announce.ID, "announce from actor we do not follow: "+signer)
	}
	if err != nil {
		return fmt.Errorf("ingest: look up announcing community %s: %w", signer, err)
	}
	if community.FollowState == store.FollowStateNone {
		return skip(announce.ID, "announce from unfollowed community "+signer)
	}

	inner := announce.Object
	if inner == nil || (inner.ID == "" && inner.Type == "") {
		return errors.NewValidationError("announce", "announce carries no object")
	}
	// Echo suppression on the RAW envelope, BEFORE anything is dereferenced.
	// The community fans our own activities straight back at us, and a bare
	// IRI that is ours must be dropped WITHOUT being fetched: dialing our own
	// origin to learn whether we minted an id is a round trip for an answer we
	// already hold, and it makes a remote redirect part of the decision.
	//
	// It runs AFTER the followed-community gate on purpose — an announce from
	// a community we do not follow must skip for THAT reason, or it lands in
	// the echo counters and corrupts the very signal that would expose a
	// classifier false positive.
	if err := h.suppressEcho(ctx, announce.ID, announce); err != nil {
		return err
	}
	// A bare-IRI announce (object is just an id): fetch it. FetchObject
	// fetches exactly inner.ID, and resolveDelivered below re-checks the
	// body's self-asserted id, so cross-host forgery cannot slip in.
	if inner.Type == "" {
		fetched, err := h.fetchBound(ctx, inner.ID)
		if err != nil {
			return err
		}
		inner = fetched
		// Classify the FETCHED body too: the IRI can be one we do not
		// recognize while the document behind it is our own activity re-served
		// under a different id. The embedded shapes need no second pass — the
		// walk above already descended through them.
		if err := h.suppressEcho(ctx, announce.ID, inner); err != nil {
			return err
		}
	}

	switch inner.Type {
	case ap.TypeCreate:
		return h.materializeContent(ctx, inner.Object, signer, false, signer)
	case ap.TypeUpdate:
		return h.materializeContent(ctx, inner.Object, signer, true, signer)
	case ap.TypePage, ap.TypeArticle, ap.TypeNote:
		// Some implementations announce the object itself, not the Create.
		return h.materializeContent(ctx, inner, signer, false, signer)
	case ap.TypeLike, ap.TypeDislike:
		return h.votes.ApplyVote(ctx, inner, signer)
	case ap.TypeDelete:
		// The already-resolved community travels with the activity: the
		// delete authorization needs its repo DID, and re-reading it there
		// would turn a "we do not follow it" miss into a retryable error on
		// an ordering key that would then never drain.
		return h.handleDelete(ctx, inner, signer, community)
	case ap.TypeUndo:
		return h.handleUndo(ctx, inner, signer, community)
	case ap.TypeLock:
		// A community closing one of its own threads. The Undo arrives on the
		// TypeUndo branch above and lands in the same handler with locked=false.
		return h.handleLock(ctx, inner, community, true)
	default:
		// Add, Remove, Block, ... — moderation activities the bridge does not
		// translate yet. Remove in particular is NOT content removal in Lemmy
		// (it is un-pin / demote-moderator, dispatched by `target`), so it must
		// never be folded in beside Lock on the assumption that it is.
		return skip(announce.ID, "unsupported announced activity type "+inner.Type)
	}
}

// suppressEcho drops an activity the bridge itself sent, whether it arrived
// announced by a community or delivered bare. Letting our own content back into
// materialization duplicates it, double-counts our own votes, and — worst —
// reads as MODERATION of our own records.
//
// It returns a skip when the envelope resolves to one of our own entities, nil
// when it is genuine remote traffic, and the classifier's error otherwise. A
// failed lookup is never a verdict: calling it "not ours" re-materializes the
// echo, calling it "ours" drops real Lemmy content permanently, and only the
// retry the wrapped error buys is honest.
func (h *Handler) suppressEcho(ctx context.Context, activityID string, envelope *ap.Object) error {
	identity, err := h.classifier.Classify(ctx, envelope)
	if err != nil {
		return fmt.Errorf("ingest: echo classification for %s: %w", activityID, err)
	}
	if identity.Class == echo.ClassNone {
		return nil
	}
	// Per-class, never a total: a spike in one class is a different bug from a
	// spike in another, and a false positive is only legible in the split.
	echo.CountDrop(identity.Class)
	// INFO, not Debug: this line is the human-readable half of the
	// false-positive detector, and genuine community content dropped as an
	// echo is invisible at Debug in production.
	if h.echoLog.Allow(time.Now()) {
		h.logger.Info("dropped an echo of our own activity",
			"activity_id", activityID,
			"class", string(identity.Class),
			"did", identity.DID,
			"at_uri", identity.ATURI)
	}
	return skip(activityID, "echo of our own "+string(identity.Class))
}

// handleBareCreateUpdate processes a Create/Update delivered directly by a
// user (or community) actor rather than through group fan-out.
func (h *Handler) handleBareCreateUpdate(ctx context.Context, activity *ap.Object, signer string) error {
	obj := activity.Object
	if obj == nil || (obj.ID == "" && obj.Type == "") {
		return errors.NewValidationError(activity.Type, "activity carries no object")
	}
	isUpdate := activity.Type == ap.TypeUpdate
	return h.materializeContent(ctx, obj, signer, isUpdate, "")
}

// materializeContent is the single content funnel: echo suppression,
// create-after-delete tombstones, embedded-object trust, followed-community
// checks, then the materializer. announcer is the announcing community's AP
// id ("" when the activity arrived bare).
func (h *Handler) materializeContent(ctx context.Context, obj *ap.Object, signer string, isUpdate bool, announcer string) error {
	if obj == nil || obj.ID == "" {
		return errors.NewValidationError("object", "content object carries no id")
	}

	// Profile updates ride the same rails (Announce{Update{Group}}, bare
	// Update{Person}) but have their own trust rule; nothing below applies.
	if obj.Type == ap.TypePerson || obj.Type == ap.TypeGroup {
		return h.applyProfileUpdate(ctx, obj, signer, announcer)
	}

	// Echo suppression: an activity whose object the bridge itself emitted
	// must never round-trip back in (write-side future-proofing; ap_objects
	// carries the origin flag since task 01).
	if mapping, err := h.objects.GetByAPID(ctx, obj.ID); err == nil {
		if mapping.Origin == store.OriginBridge {
			return skip(obj.ID, "echo of a bridge-authored object")
		}
	} else if !errors.IsNotFound(err) {
		return fmt.Errorf("ingest: echo check for %s: %w", obj.ID, err)
	}

	// Create-after-delete: a Delete for this id may have arrived before any
	// materialization (no mapping to tombstone — task 05's known gap). The
	// ap_tombstones marker closes it here, in the ingest layer.
	//
	// Markers are scoped to whoever laid them, so the lookup needs this
	// delivery's community context — and that context may only come from
	// somewhere the DELIVERY cannot choose. Announced: the announcer, which
	// the inbox bound to the HTTP signature, so a community's own marker
	// suppresses its own re-announce right here, before any outbound fetch.
	// Bare: nothing trustworthy names a community yet — the only candidate is
	// the delivered body's audience, and a Create carrying a bare reference
	// ({"id": X} with no type and no audience) names none at all, which would
	// read straight past the community-scoped marker that a delete-before-
	// create left for exactly this id. So the early check is global-only
	// (still free, and a globally tombstoned id costs no fetch), and the
	// community-scoped half runs below against the body resolveDelivered
	// actually vouches for.
	tombstoned, err := h.tombstones.ExistsFor(ctx, obj.ID, announcer)
	if err != nil {
		return fmt.Errorf("ingest: tombstone check for %s: %w", obj.ID, err)
	}
	if tombstoned {
		return skip(obj.ID, "object was deleted upstream before it was ever materialized")
	}

	obj, err = h.resolveDelivered(ctx, obj, signer)
	if err != nil {
		return err
	}

	// Bare deliveries must belong to a community the bridge follows; the
	// announce path already established that for its signer.
	if announcer == "" {
		communityIRI := communityIRIFrom(obj)
		if communityIRI == "" {
			return skip(obj.ID, "bare delivery names no community (no audience group IRI)")
		}
		// The community-scoped half of the create-after-delete check, deferred
		// from above: this audience comes from a body the origin served (or one
		// the signer vouched for on its own authority), not from a reference the
		// deliverer wrote, so a marker laid by the community this object claims
		// to belong to now applies to it.
		tombstoned, err := h.tombstones.ExistsFor(ctx, obj.ID, communityIRI)
		if err != nil {
			return fmt.Errorf("ingest: tombstone check for %s: %w", obj.ID, err)
		}
		if tombstoned {
			return skip(obj.ID, "object was deleted upstream before it was ever materialized")
		}
		community, err := h.communities.GetByAPGroupID(ctx, communityIRI)
		if errors.IsNotFound(err) {
			return skip(obj.ID, "bare delivery for a community we do not follow: "+communityIRI)
		}
		if err != nil {
			return fmt.Errorf("ingest: look up community %s: %w", communityIRI, err)
		}
		if community.FollowState == store.FollowStateNone {
			return skip(obj.ID, "bare delivery for unfollowed community "+communityIRI)
		}
	} else {
		// Announced content must belong to the announcing community itself: a
		// followed community may fan out only its own content, never claim
		// another community's (even one co-hosted on the same instance). Since
		// the flip the consequence is not a foreign write into a community repo
		// — a postv2 goes to its author's repo — but a false BINDING: the
		// materializer derives the target community from the object's own
		// audience, EnsureCommunity()s it, records it as the mapping's
		// community_did and writes that community's acceptance. Without this
		// guard an announcer could name any community it likes and hand it both
		// visibility over the post and moderation authority over it.
		if objCommunity := communityIRIFrom(obj); objCommunity != "" && objCommunity != announcer {
			return skip(obj.ID, fmt.Sprintf(
				"announced object names community %s but was announced by %s", objCommunity, announcer))
		}
	}

	switch obj.Type {
	case ap.TypePage, ap.TypeArticle:
		if isUpdate {
			_, err = h.mat.HandleUpdate(ctx, obj)
		} else {
			_, err = h.mat.MaterializePost(ctx, obj)
		}
	case ap.TypeNote:
		if isUpdate {
			_, err = h.mat.HandleUpdate(ctx, obj)
		} else {
			_, err = h.mat.MaterializeComment(ctx, obj)
		}
	default:
		return skip(obj.ID, "unsupported content type "+obj.Type)
	}
	return err
}

// resolveDelivered decides whether a delivered (embedded) object may be
// used as-is or must be re-fetched. The rule: an embedded copy is trusted
// only when its id lives on the signer's own authority — a community can
// vouch for content on its own instance, but content whose canonical id is
// on ANOTHER instance (a lemmy.zip post announced by a lemmy.world
// community, the normal federation case) is re-fetched from its origin so a
// malicious instance cannot forge bodies under a victim's id. Fetched
// bodies get the same self-asserted-id authority binding task 05 applies.
func (h *Handler) resolveDelivered(ctx context.Context, obj *ap.Object, signer string) (*ap.Object, error) {
	if obj.Type != "" && ap.SameAuthority(obj.ID, signer) {
		return obj, nil
	}
	return h.fetchBound(ctx, obj.ID)
}

// fetchBound fetches an object by IRI and binds the body's self-asserted id
// to the fetch authority (empty ids inherit the request IRI). Redirects stay
// permissive: here the origin's answer is CONTENT, and the id binding below
// is what keeps a redirect from forging another instance's object.
func (h *Handler) fetchBound(ctx context.Context, iri string) (*ap.Object, error) {
	return h.bindFetch(ctx, iri, h.fetcher.FetchObject)
}

// fetchBoundSameAuthority is fetchBound with the redirect authority pinned to
// the requested IRI — for the one dispatch fetch whose ANSWER is an
// authorization decision and not just content: the restore's "the origin
// serves this object again" (handleUndoDelete), which both licenses
// re-materializing the record and supplies its body. An open redirect on the
// origin would otherwise hand both to whoever it points at. The refusal is a
// validation error, so the event poisons instead of retrying against a
// redirect the origin is not about to withdraw.
func (h *Handler) fetchBoundSameAuthority(ctx context.Context, iri string) (*ap.Object, error) {
	return h.bindFetch(ctx, iri, h.fetcher.FetchObjectSameAuthority)
}

// bindFetch runs one of the Fetcher's fetches and applies the shared binding
// rules. Unavailable and tombstoned objects are skips: content that cannot be
// verified at its origin is dropped, not retried.
func (h *Handler) bindFetch(ctx context.Context, iri string,
	fetch func(context.Context, string) (*ap.Object, error)) (*ap.Object, error) {
	fetched, err := fetch(ctx, iri)
	switch {
	case err == nil:
	case errors.IsTombstoned(err):
		return nil, skip(iri, "object is tombstoned upstream")
	case errors.IsNotFound(err):
		return nil, skip(iri, "object is unavailable upstream")
	default:
		return nil, fmt.Errorf("ingest: fetch %s: %w", iri, err)
	}
	if fetched.ID == "" {
		fetched.ID = iri
	} else if !ap.SameAuthority(fetched.ID, iri) {
		return nil, skip(iri, fmt.Sprintf("fetched object served a cross-authority id %s", fetched.ID))
	}
	return fetched, nil
}

// handleAccept marks a community's Follow accepted. Lemmy signs the Accept
// with the community actor itself, so the verified signer must BE the
// community the embedded Follow names.
func (h *Handler) handleAccept(ctx context.Context, accept *ap.Object, signer string) error {
	communityID, err := h.followCommunity(ctx, accept, signer)
	if err != nil {
		return err
	}
	community, err := h.communities.GetByAPGroupID(ctx, communityID)
	if errors.IsNotFound(err) {
		return skip(accept.ID, "accept for a community we never followed: "+communityID)
	}
	if err != nil {
		return fmt.Errorf("ingest: look up community %s: %w", communityID, err)
	}
	switch community.FollowState {
	case store.FollowStateAccepted:
		// Idempotent re-delivery: already accepted, no state change and no
		// fresh backfill.
		return nil
	case store.FollowStateNone:
		// We unsubscribed (state cleared, Undo{Follow} sent) — Lemmy can retry
		// an Accept for hours. A late Accept must not silently re-subscribe us
		// nor trigger a backfill.
		return skip(accept.ID, "accept for a community we unfollowed: "+communityID)
	}
	// Only pending → accepted is a real transition (and the sole backfill
	// trigger).
	if err := h.communities.SetFollowState(ctx, communityID, store.FollowStateAccepted); err != nil {
		return fmt.Errorf("ingest: mark follow accepted for %s: %w", communityID, err)
	}
	h.logger.Info("community follow accepted", "community", communityID)
	if h.backfill != nil {
		community.FollowState = store.FollowStateAccepted
		h.backfill.TriggerAsync(community, false)
	}
	return nil
}

// handleReject clears a community's Follow (the remote refused or revoked
// the subscription).
func (h *Handler) handleReject(ctx context.Context, reject *ap.Object, signer string) error {
	communityID, err := h.followCommunity(ctx, reject, signer)
	if err != nil {
		return err
	}
	if _, err := h.communities.GetByAPGroupID(ctx, communityID); errors.IsNotFound(err) {
		return skip(reject.ID, "reject for a community we never followed: "+communityID)
	} else if err != nil {
		return fmt.Errorf("ingest: look up community %s: %w", communityID, err)
	}
	if err := h.communities.SetFollowState(ctx, communityID, store.FollowStateNone); err != nil {
		return fmt.Errorf("ingest: mark follow rejected for %s: %w", communityID, err)
	}
	h.logger.Warn("community follow rejected", "community", communityID)
	return nil
}

// followCommunity extracts and authorizes the community a Follow response
// (Accept/Reject) refers to: the embedded Follow must be ours (actor == the
// service actor) and the responding signer must be the community itself.
func (h *Handler) followCommunity(_ context.Context, response *ap.Object, signer string) (string, error) {
	follow := response.Object
	communityID := signer
	if follow != nil && follow.Type == ap.TypeFollow {
		if follow.Actor != nil && follow.Actor.ID != "" && follow.Actor.ID != h.serviceID {
			return "", skip(response.ID,
				"embedded follow was issued by "+follow.Actor.ID+", not the bridge")
		}
		if follow.Object != nil && follow.Object.ID != "" {
			communityID = follow.Object.ID
		}
	}
	if communityID != signer {
		return "", skip(response.ID, fmt.Sprintf(
			"follow response signed by %s for community %s (signer must be the community)",
			signer, communityID))
	}
	return communityID, nil
}

// isBridged reports whether an AP id already has an ap_objects mapping — the
// "already bridged?" signal used to keep bare profile Updates refresh-only
// (never mint). Every bridged actor/community has a profile mapping row (rkey
// "self"), and every bridged post/comment a content mapping, so a hit means
// the id is known; a miss means it was never materialized.
func (h *Handler) isBridged(ctx context.Context, apID string) (bool, error) {
	if _, err := h.objects.GetByAPID(ctx, apID); err == nil {
		return true, nil
	} else if errors.IsNotFound(err) {
		return false, nil
	} else {
		return false, fmt.Errorf("ingest: check bridged state for %s: %w", apID, err)
	}
}

// refID returns the id of a possibly-nil object reference.
func refID(obj *ap.Object) string {
	if obj == nil {
		return ""
	}
	return obj.ID
}

// communityIRIFrom finds the community Group IRI an object/activity is
// addressed to: Lemmy sets `audience` (FEP-1b12); older objects carry the
// group in to/cc next to the public collection (heuristic: Lemmy community
// IRIs live under /c/).
func communityIRIFrom(obj *ap.Object) string {
	for _, iri := range obj.Audience {
		if iri != "" && !isPublicIRI(iri) {
			return iri
		}
	}
	for _, list := range []ap.Audience{obj.To, obj.Cc} {
		for _, iri := range list {
			if iri == "" || isPublicIRI(iri) {
				continue
			}
			if containsCommunityPath(iri) {
				return iri
			}
		}
	}
	return ""
}

func isPublicIRI(iri string) bool {
	return iri == ap.PublicAudience || iri == "as:Public" || iri == "Public"
}

func containsCommunityPath(iri string) bool {
	// Lemmy community IRIs are https://host/c/name; keep the same heuristic
	// the materializer uses (Mbin /m/ deferred with it).
	for i := 0; i+3 <= len(iri); i++ {
		if iri[i] == '/' && iri[i+1] == 'c' && iri[i+2] == '/' {
			return true
		}
	}
	return false
}

// skip builds the shared log-and-never-retry error (the materializer's
// SkipError, so the queue's IsSkip check covers both layers).
func skip(apID, reason string) error {
	return &materialize.SkipError{APID: apID, Reason: reason}
}
