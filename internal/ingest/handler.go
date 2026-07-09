package ingest

import (
	"context"
	"fmt"
	"log/slog"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// Materializer is the slice of *materialize.Materializer the dispatcher
// drives (task 05's entry points).
type Materializer interface {
	MaterializePost(ctx context.Context, page *ap.Object) (*materialize.Result, error)
	MaterializeComment(ctx context.Context, note *ap.Object) (*materialize.Result, error)
	HandleUpdate(ctx context.Context, obj *ap.Object) (*materialize.Result, error)
	HandleDelete(ctx context.Context, apID string) error
	RefreshActor(ctx context.Context, actorRef *ap.Object) (*store.BridgedActor, error)
	RefreshCommunity(ctx context.Context, groupRef *ap.Object) (*store.Community, error)
	EnsureCommunity(ctx context.Context, groupRef *ap.Object) (*store.Community, error)
}

// Fetcher is the slice of *ap.Client the dispatcher uses to re-fetch
// objects it must not trust from a delivery.
type Fetcher interface {
	FetchObject(ctx context.Context, iri string) (*ap.Object, error)
}

// Backfiller is notified when a community's Follow is accepted (the
// backfill trigger). *Backfill implements it; tests inject recorders.
type Backfiller interface {
	TriggerAsync(community *store.Community, force bool)
}

// HandlerOptions configures NewHandler. Materializer, Fetcher, Objects,
// Communities, Tombstones, Votes, and ServiceActorID are required;
// Backfill and Logger are optional.
type HandlerOptions struct {
	Materializer Materializer
	Fetcher      Fetcher
	Objects      store.APObjects
	Communities  store.Communities
	Tombstones   store.Tombstones
	Votes        VoteAggregator
	Backfill     Backfiller
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
	communities store.Communities
	tombstones  store.Tombstones
	votes       VoteAggregator
	backfill    Backfiller
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
	if opts.Communities == nil {
		return nil, errors.NewValidationError("communities", "must not be nil")
	}
	if opts.Tombstones == nil {
		return nil, errors.NewValidationError("tombstones", "must not be nil")
	}
	if opts.Votes == nil {
		return nil, errors.NewValidationError("votes", "must not be nil")
	}
	if opts.ServiceActorID == "" {
		return nil, errors.NewValidationError("service_actor_id", "must not be empty")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{
		mat:         opts.Materializer,
		fetcher:     opts.Fetcher,
		objects:     opts.Objects,
		communities: opts.Communities,
		tombstones:  opts.Tombstones,
		votes:       opts.Votes,
		backfill:    opts.Backfill,
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
		return h.handleDelete(ctx, activity, signer, "")
	case ap.TypeUndo:
		return h.handleUndo(ctx, activity, signer, "")
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
	// A bare-IRI announce (object is just an id): fetch it. FetchObject
	// fetches exactly inner.ID, and resolveDelivered below re-checks the
	// body's self-asserted id, so cross-host forgery cannot slip in.
	if inner.Type == "" {
		fetched, err := h.fetchBound(ctx, inner.ID)
		if err != nil {
			return err
		}
		inner = fetched
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
		return h.handleDelete(ctx, inner, signer, signer)
	case ap.TypeUndo:
		return h.handleUndo(ctx, inner, signer, signer)
	default:
		// Lock, Add, Remove, Block, ... — moderation activities the bridge
		// does not translate in v1.
		return skip(announce.ID, "unsupported announced activity type "+inner.Type)
	}
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
	tombstoned, err := h.tombstones.Exists(ctx, obj.ID)
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
		// followed community may fan out only its own content, never inject
		// into a DIFFERENT community's repo (even one co-hosted on the same
		// instance). The materializer derives the target community from the
		// object's own audience and EnsureCommunity()s it, so without this the
		// announcer could name any community it likes.
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
// to the fetch authority (empty ids inherit the request IRI). Unavailable
// and tombstoned objects are skips: content that cannot be verified at its
// origin is dropped, not retried.
func (h *Handler) fetchBound(ctx context.Context, iri string) (*ap.Object, error) {
	fetched, err := h.fetcher.FetchObject(ctx, iri)
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

// targetIsBridgedActor reports whether an AP id is a bridged ACTOR (its
// mapping is a profile record), as opposed to a content object. A bridged
// actor always has a profile mapping (EnsureActor commits rkey "self"), so a
// harmful Delete(Actor) against a bridged victim is always detectable here;
// an unbridged actor is a materializer no-op regardless.
func (h *Handler) targetIsBridgedActor(ctx context.Context, apID string) (bool, error) {
	mapping, err := h.objects.GetByAPID(ctx, apID)
	if errors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("ingest: classify delete target %s: %w", apID, err)
	}
	return mapping.Collection == materialize.CollectionActorProfile ||
		mapping.Collection == materialize.CollectionCommunityProfile, nil
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
