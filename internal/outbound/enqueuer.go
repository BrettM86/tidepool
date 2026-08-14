package outbound

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"tidepool/internal/apobject"
	"tidepool/internal/consume"
	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// EnqueuerOptions configures an Enqueuer.
type EnqueuerOptions struct {
	// DB is the bridge database.
	DB *sql.DB
	// Translator renders intents into canonical AP activities.
	Translator *Translator
	// Inboxes resolves a community's target inbox at enqueue time (one delivery
	// row per target; v2 targets are single-community). The (activity, inbox)
	// pair is the delivery primary key, so the inbox is known when the row is
	// written.
	Inboxes InboxResolver
	// Actors resolves an actor DID to its AP actor id (the single-string
	// attributedTo the Translator addresses as).
	Actors store.APActors
	// Activities / Deliveries are the split queue. Optional: nil is
	// constructed from DB.
	Activities store.OutboundActivities
	Deliveries store.OutboundDeliveries
	// UserOrigin is AP_USER_ORIGIN.
	UserOrigin string
	// Logger receives drop reasons. Nil uses slog.Default().
	Logger *slog.Logger
}

// Enqueuer is the real OutboundEnqueuer (task 15): it translates one intent and
// writes its canonical activity plus one per-target delivery, INSIDE the rev
// gate transaction the consumer hands it — the enqueue must commit with the
// gate advance or a rolled-back gate would leave the activity/delivery behind
// and a replay could not reproduce it.
type Enqueuer struct {
	db         *sql.DB
	translator *Translator
	inboxes    InboxResolver
	actors     store.APActors
	activities store.OutboundActivities
	deliveries store.OutboundDeliveries
	apObjects  store.APObjects
	userOrigin string
	originHost string
	logger     *slog.Logger
}

// NewEnqueuer wires an Enqueuer.
func NewEnqueuer(opts EnqueuerOptions) (*Enqueuer, error) {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	activities := opts.Activities
	if activities == nil {
		activities = store.NewOutboundActivities(opts.DB)
	}
	deliveries := opts.Deliveries
	if deliveries == nil {
		deliveries = store.NewOutboundDeliveries(opts.DB)
	}
	// originHost labels bridge-emitted ap_objects rows (origin_instance): the
	// host a re-fetch of this object dials, which is our own origin.
	originHost := opts.UserOrigin
	if parsed, err := url.Parse(opts.UserOrigin); err == nil && parsed.Host != "" {
		originHost = strings.ToLower(parsed.Host)
	}
	return &Enqueuer{
		db:         opts.DB,
		translator: opts.Translator,
		inboxes:    opts.Inboxes,
		actors:     opts.Actors,
		activities: activities,
		deliveries: deliveries,
		apObjects:  store.NewAPObjects(opts.DB),
		userOrigin: opts.UserOrigin,
		originHost: originHost,
		logger:     logger,
	}, nil
}

// EnqueueActivity translates the intent and writes, ON THE CALLER'S TX, the
// canonical activity, the ap_objects mapping that makes the object fetchable,
// and one per-inbox delivery — so all of it commits with the gate advance or
// leaves nothing behind (a rolled-back gate must not strand a delivery a replay
// cannot reproduce).
//
// The community for both the inbox resolution and the delivery's ordering key
// is read from the intent (the per-community serial line), not the orderingKey
// argument: the consumer passes the actor DID there as a coarse hint, but a
// delivery is serialized and addressed by the target COMMUNITY.
func (e *Enqueuer) EnqueueActivity(ctx context.Context, tx *sql.Tx, actorDID, orderingKey, parentATURI string, intent consume.Intent) error {
	if tx == nil {
		return errors.NewValidationError("tx", "must not be nil")
	}
	actor, err := e.actors.GetByDID(ctx, actorDID)
	if err != nil {
		return fmt.Errorf("resolve actor %s: %w", actorDID, err)
	}
	translated, err := e.translator.Translate(actor.ActorID, intent)
	if err != nil {
		return fmt.Errorf("translate intent %s: %w", intent.ActivityID(), err)
	}

	// The activity is the atomicity anchor: ON CONFLICT DO NOTHING, so a
	// redelivered intent re-derives the same id and reports inserted=false.
	// Because the activity, its mapping and its delivery all ride ONE
	// transaction, an already-present activity id already has the other two —
	// so a re-enqueue returns here without re-inserting the delivery (which
	// would violate its PK and poison the tx).
	inserted, err := e.activities.InsertTx(ctx, tx, store.OutboundActivity{
		ActivityID:  intent.ActivityID(),
		ActorDID:    actorDID,
		Kind:        translated.Kind,
		Payload:     translated.Payload,
		ParentATURI: parentATURI,
	})
	if err != nil {
		return fmt.Errorf("insert outbound activity %s: %w", intent.ActivityID(), err)
	}
	if !inserted {
		return nil
	}

	// The object mapping (bridge-origin) makes GET /ap/object serve the record.
	// Votes have no servable object and a self-delete maps nothing new.
	if mapping, ok, err := e.objectMapping(intent); err != nil {
		return err
	} else if ok {
		if _, err := e.apObjects.PutMappingTx(ctx, tx, mapping); err != nil {
			return fmt.Errorf("map outbound object %s: %w", mapping.APID, err)
		}
	}

	community := communityOf(intent)
	inbox, err := e.inboxes.ResolveInbox(ctx, community)
	if err != nil {
		// No inbox means no delivery target: fail so the whole tx rolls back
		// rather than writing a delivery to nowhere.
		return fmt.Errorf("resolve inbox for community %s: %w", community, err)
	}
	if _, err := e.deliveries.EnqueueTx(ctx, tx, store.OutboundDelivery{
		ActivityID:  intent.ActivityID(),
		TargetInbox: inbox,
		OrderingKey: community,
	}); err != nil {
		return fmt.Errorf("enqueue delivery for %s: %w", intent.ActivityID(), err)
	}
	return nil
}

// objectMapping derives the bridge-origin ap_objects mapping for an intent that
// produces a servable object (a comment or post create/update). Votes have no
// object; a self-delete's object was mapped on its create. ok=false means no
// mapping is written.
func (e *Enqueuer) objectMapping(intent consume.Intent) (store.APObjectMapping, bool, error) {
	var atURI, apType, communityDID string
	var snapshot []byte
	switch typed := intent.(type) {
	case consume.CommentIntent:
		if typed.Op == "delete" {
			return store.APObjectMapping{}, false, nil
		}
		atURI, apType, snapshot = typed.ATURI, "Note", typed.Snapshot
		communityDID = typed.CommunityDID
	case consume.PostIntent:
		if typed.Op == "delete" {
			return store.APObjectMapping{}, false, nil
		}
		atURI, apType, snapshot = typed.ATURI, "Page", typed.Snapshot
		communityDID = typed.CommunityDID
	default:
		return store.APObjectMapping{}, false, nil
	}

	trimmed := strings.TrimPrefix(atURI, "at://")
	parts := strings.SplitN(trimmed, "/", 3)
	if len(parts) != 3 {
		return store.APObjectMapping{}, false,
			errors.NewValidationError("intent.atUri", "must be at://did/collection/rkey, got "+atURI)
	}
	snap, err := apobject.ParseSnapshot(snapshot)
	if err != nil {
		return store.APObjectMapping{}, false, err
	}
	cid, _ := snap["cid"].(string)
	return store.APObjectMapping{
		APID:           e.userOrigin + "/ap/object/" + trimmed,
		APType:         apType,
		OriginInstance: e.originHost,
		Origin:         store.OriginBridge,
		DID:            parts[0],
		// The AUTHOR is the at-uri's repo: an author-owned postv2 or comment
		// lives in their own repo, so DID and AuthorDID are the same DID here.
		// Recorded rather than left empty because deleteIsByAuthor reads this
		// column to tell a self-delete from a moderator removal, and an empty
		// one answers "not provably the author" for every native record.
		AuthorDID: parts[0],
		// The COMMUNITY this object was federated into. Without it
		// CommunityDIDOf answers "" for a bridge-origin mapping — an
		// author-owned postv2 lives in the AUTHOR's repo, so the community
		// cannot be read off DID — and every announced moderation action
		// against native content is refused before it is even evaluated.
		CommunityDID: communityDID,
		Collection:   parts[1],
		RKey:         parts[2],
		CID:          cid,
	}, true, nil
}

// communityOf reads the target community AP id off any intent — the per-
// community serial line every delivery is ordered and addressed by.
func communityOf(intent consume.Intent) string {
	switch typed := intent.(type) {
	case consume.CommentIntent:
		return typed.CommunityAPID
	case consume.PostIntent:
		return typed.CommunityAPID
	case consume.VoteIntent:
		return typed.CommunityAPID
	default:
		return ""
	}
}
