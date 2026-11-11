package outbound

import (
	"context"
	"database/sql"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"log/slog"
	"slices"

	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/lib/pq"

	"tidepool/internal/ap"
	"tidepool/internal/consume"
	"tidepool/internal/errors"
	"tidepool/internal/labels"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// TakedownsOptions configures Takedowns.
type TakedownsOptions struct {
	DB       *sql.DB
	Enqueuer *Enqueuer
	Logger   *slog.Logger
}

// Takedowns drives native objects' takedown source sets and acts on their
// transitions, inside the caller's transaction:
//
//   - empty to non-empty: the object's queued Create/Update rows are held, and
//     when the object has reached its community (accepted_at set) a takedown
//     Delete is enqueued from the author's persona and recorded as the
//     episode's Delete;
//   - non-empty to empty: when a Delete was sent in this episode, an
//     Undo{Delete} embedding it is enqueued, unless the author deleted the
//     object or the author's identity was withdrawn; then the held rows are
//     released to the tail of their keys, behind that Undo.
//
// It never calls the purger: a takedown is not an erasure.
type Takedowns struct {
	enqueuer   *Enqueuer
	takedowns  store.OutboundTakedowns
	objects    store.OutboundObjects
	deliveries store.OutboundDeliveries
	actors     store.APActors
	logger     *slog.Logger
}

// NewTakedowns builds Takedowns from opts.
func NewTakedowns(opts TakedownsOptions) (*Takedowns, error) {
	if opts.DB == nil {
		return nil, errors.NewValidationError("db", "must not be nil")
	}
	if opts.Enqueuer == nil {
		return nil, errors.NewValidationError("enqueuer", "must not be nil")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Takedowns{
		enqueuer:   opts.Enqueuer,
		takedowns:  store.NewOutboundTakedowns(opts.DB),
		objects:    store.NewOutboundObjects(opts.DB),
		deliveries: store.NewOutboundDeliveries(opts.DB),
		actors:     store.NewAPActors(opts.DB),
		logger:     logger,
	}, nil
}

// AddSourceTx adds src to atURI's takedown source set in tx and, when the set
// becomes non-empty, takes the object down.
func (t *Takedowns) AddSourceTx(ctx context.Context, tx *sql.Tx, atURI string, src store.TakedownSource) error {
	object, err := t.lockObject(ctx, tx, atURI)
	if err != nil {
		return err
	}
	transition, err := t.takedowns.AddSourceTx(ctx, tx, atURI, src)
	if err != nil {
		return err
	}
	if transition != store.TakedownBecameRemoved {
		return nil
	}
	// Taken before the Delete is enqueued, so a consent release of the
	// object's restore Undo{Delete} either commits first (the Undo draws its
	// seq ahead of this Delete) or waits for this tx and reads the object
	// removed (and cancels the Undo).
	if err := store.LockTakedownSubjectsTx(ctx, tx, atURI); err != nil {
		return err
	}
	if _, err := t.deliveries.HoldForObjectTx(ctx, tx, atURI); err != nil {
		return err
	}
	// An object never accepted by its community is not on the peer, so there
	// is nothing to delete there; its held Create carries it on restore.
	if object == nil || !object.IsAccepted() || object.IsTombstoned() {
		return nil
	}
	actor, err := t.authorPersona(ctx, atURI)
	if err != nil || actor == nil {
		return err
	}
	inbox, err := t.enqueuer.inboxes.ResolveInbox(ctx, object.CommunityAPID)
	if err != nil {
		return fmt.Errorf("resolve inbox for community %s: %w", object.CommunityAPID, err)
	}
	deleteID, err := enqueueOwedTakedownDeleteTx(ctx, tx, takedownDeleteWriter{
		translator: t.enqueuer.translator,
		takedowns:  t.takedowns,
		activities: t.enqueuer.activities,
		deliveries: t.deliveries,
	}, actor, object, inbox)
	if err != nil {
		return err
	}
	t.logger.Info("native object taken down", "at_uri", atURI, "delete", deleteID)
	return nil
}

// takedownDeleteWriter is what minting and enqueueing a takedown Delete
// needs: the takedown path and the worker's settlement both write one.
type takedownDeleteWriter struct {
	translator *Translator
	takedowns  store.OutboundTakedowns
	activities store.OutboundActivities
	deliveries store.OutboundDeliveries
}

// enqueueOwedTakedownDeleteTx enqueues, in tx, the takedown Delete of
// object's current episode from actor's persona to inbox on the community's
// ordering key, and records it as the episode's Delete. When the episode
// already has its Delete, nothing is written. It returns the episode's Delete
// activity id.
func enqueueOwedTakedownDeleteTx(ctx context.Context, tx *sql.Tx, w takedownDeleteWriter,
	actor *store.APActor, object *store.OutboundObject, inbox string) (string, error) {
	episode, err := w.takedowns.EpisodeTx(ctx, tx, object.ATURI)
	if err != nil {
		return "", err
	}
	if episode.DeleteActivityID != "" {
		return episode.DeleteActivityID, nil
	}
	deleted, err := w.translator.TakedownDelete(actor.ActorID, object.ATURI, object.CommunityAPID, episode.Number)
	if err != nil {
		return "", err
	}
	if _, err := w.activities.InsertTx(ctx, tx, store.OutboundActivity{
		ActivityID: deleted.ActivityID,
		ActorDID:   actor.DID,
		Kind:       deleted.Kind,
		Payload:    deleted.Payload,
	}); err != nil {
		return "", fmt.Errorf("insert takedown Delete %s: %w", deleted.ActivityID, err)
	}
	if _, err := w.deliveries.EnqueueTx(ctx, tx, store.OutboundDelivery{
		ActivityID:  deleted.ActivityID,
		TargetInbox: inbox,
		OrderingKey: object.CommunityAPID,
	}); err != nil {
		return "", fmt.Errorf("enqueue takedown Delete %s: %w", deleted.ActivityID, err)
	}
	if err := w.takedowns.RecordDeleteTx(ctx, tx, object.ATURI, deleted.ActivityID); err != nil {
		return "", err
	}
	return deleted.ActivityID, nil
}

// RemoveSourceTx removes src from atURI's takedown source set in tx and, when
// the set becomes empty, restores the object.
func (t *Takedowns) RemoveSourceTx(ctx context.Context, tx *sql.Tx, atURI string, src store.TakedownSource) error {
	object, err := t.lockObject(ctx, tx, atURI)
	if err != nil {
		return err
	}
	transition, err := t.takedowns.RemoveSourceTx(ctx, tx, atURI, src)
	if err != nil {
		return err
	}
	if transition != store.TakedownBecameClear {
		return nil
	}
	if err := t.undoDeleteTx(ctx, tx, atURI, object); err != nil {
		return err
	}
	if _, err := t.deliveries.ReleaseHeldTx(ctx, tx, atURI); err != nil {
		return err
	}
	t.logger.Info("native object restored", "at_uri", atURI)
	return nil
}

// undoDeleteTx enqueues the Undo of the episode's takedown Delete, when one
// was sent and the object may come back: an object its author deleted stays
// deleted, and a withdrawn identity's content is not republished.
func (t *Takedowns) undoDeleteTx(ctx context.Context, tx *sql.Tx, atURI string, object *store.OutboundObject) error {
	if object == nil || object.IsTombstoned() {
		return nil
	}
	episode, err := t.takedowns.EpisodeTx(ctx, tx, atURI)
	if err != nil {
		return err
	}
	if episode.DeleteActivityID == "" {
		return nil
	}
	actor, err := t.authorPersona(ctx, atURI)
	if err != nil || actor == nil {
		return err
	}
	undo, err := t.enqueuer.translator.TakedownUndo(actor.ActorID, object.CommunityAPID,
		episode.DeletePayload, episode.Number)
	if err != nil {
		return err
	}
	return t.enqueuer.enqueueTranslatedTx(ctx, tx, actor.DID, object.CommunityAPID, undo)
}

// lockObject locks atURI's outbound object in tx; nil when it has none yet.
func (t *Takedowns) lockObject(ctx context.Context, tx *sql.Tx, atURI string) (*store.OutboundObject, error) {
	object, err := t.objects.LockByATURITx(ctx, tx, atURI)
	if errors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return object, nil
}

// authorPersona is the persona of atURI's author (the at-uri's repo), or nil
// when the author has none or the destructive tier withdrew it.
func (t *Takedowns) authorPersona(ctx context.Context, atURI string) (*store.APActor, error) {
	parsed, err := syntax.ParseATURI(atURI)
	if err != nil {
		return nil, errors.NewValidationError("at_uri", err.Error())
	}
	actor, err := t.actors.GetByDID(ctx, parsed.Authority().String())
	if errors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("resolve author of %s: %w", atURI, err)
	}
	if actor.IsTombstoned() {
		return nil, nil
	}
	return actor, nil
}

// LabelApplier is the labels.Applier (and labels.Activator) for `!takedown`
// labels on native records and native personas.
//
// On a native record the label is the record-label source keyed by the
// labeler, added while the subject's label stands and removed when it is
// negated or lapses. On a record this bridge hosts (a bridged record) the
// standing label itself holds native replies to it, so its negation only
// releases them; withholding the record is the other appliers' work.
//
// On a bare DID naming a native person persona, the label is both the
// persona's account-label pause source and the account-label takedown source
// of every one of the persona's outbound objects, both keyed by the labeler;
// a negation or lapse removes both. A DID with no persona stays stored: the
// persona minted later is minted paused (store APActors.Create). On a bare
// DID naming a bridged person, the standing label holds native replies to the
// person's records, and its negation or lapse releases them. On a bare DID
// naming a bridged community (communities.did), the label is the
// community-label takedown source of every native object bound to the
// community, and the community's other queued work is held on it, until the
// negation or lapse (AddCommunitySourceTx, RemoveCommunitySourceTx). Other
// values are the other appliers' work.
func (t *Takedowns) LabelApplier() labels.Applier {
	return takedownLabelApplier{t: t}
}

type takedownLabelApplier struct{ t *Takedowns }

func (a takedownLabelApplier) ApplyLabel(ctx context.Context, tx *sql.Tx, l labels.Stored) error {
	return a.apply(ctx, tx, l)
}

// Activate brings the source set up to a standing label at start. Adding a
// source already in the set changes nothing, so a replay is idempotent.
func (a takedownLabelApplier) Activate(ctx context.Context, tx *sql.Tx, l labels.Stored) error {
	return a.apply(ctx, tx, l)
}

func (a takedownLabelApplier) apply(ctx context.Context, tx *sql.Tx, l labels.Stored) error {
	if l.Val != materialize.LabelTakedown {
		return nil
	}
	if _, err := syntax.ParseDID(l.URI); err == nil {
		applied, err := a.applyCommunity(ctx, tx, l)
		if err != nil || applied {
			return err
		}
		return a.applyAccount(ctx, tx, l)
	}
	record, native, err := recordKindTx(ctx, tx, l.URI)
	if err != nil || !record {
		return err
	}
	standing, err := labels.StandingTx(ctx, tx, l.URI, materialize.LabelTakedown)
	if err != nil {
		return err
	}
	if !native {
		// A bridged record has no takedown set here (the withhold applier
		// owns it); the label alone holds native replies to it, so its lift
		// releases them.
		if standing {
			return nil
		}
		_, err := a.t.deliveries.ReleaseHeldTx(ctx, tx, l.URI)
		return err
	}
	src := store.TakedownSource{Kind: store.TakedownSourceRecordLabel, Key: l.Src}
	if standing {
		return a.t.AddSourceTx(ctx, tx, l.URI, src)
	}
	return a.t.RemoveSourceTx(ctx, tx, l.URI, src)
}

// applyAccount brings a native persona's account-label pause source and its
// objects' account-label takedown source up to the bare-DID label's standing.
// The account lock comes first, so a mint of the same DID either commits
// before this reads ap_actors or reads this label after it commits; then the
// pause write takes the persona's ap_actors row before any object lock, the
// shared lock order. A purger-tombstoned persona's objects get no restore
// (RemoveSourceTx skips them) and the purger is never called.
func (a takedownLabelApplier) applyAccount(ctx context.Context, tx *sql.Tx, l labels.Stored) error {
	did := l.URI
	if err := store.LockAccountTx(ctx, tx, did); err != nil {
		return err
	}
	var kind string
	err := tx.QueryRowContext(ctx, `SELECT kind FROM ap_actors WHERE did = $1`, did).Scan(&kind)
	if stderrors.Is(err, sql.ErrNoRows) {
		return a.applyBridgedAccount(ctx, tx, did)
	}
	if err != nil {
		return fmt.Errorf("read persona of %s: %w", did, err)
	}
	if store.ActorType(kind) != store.ActorTypePerson {
		return nil
	}
	standing, err := labels.StandingTx(ctx, tx, did, materialize.LabelTakedown)
	if err != nil {
		return err
	}
	pause := store.PauseSource{Kind: store.PauseSourceAccountLabel, Key: l.Src}
	src := store.TakedownSource{Kind: store.TakedownSourceAccountLabel, Key: l.Src}
	if standing {
		if err := store.AddPauseSourceTx(ctx, tx, did, pause); err != nil {
			return err
		}
		return a.t.AddAccountSourceTx(ctx, tx, did, src)
	}
	if err := store.RemovePauseSourceTx(ctx, tx, did, pause); err != nil {
		return err
	}
	if err := a.t.RemoveAccountSourceTx(ctx, tx, did, src); err != nil {
		return err
	}
	// Last, after every object lock: once the pause set is clear, the work
	// held on the pause (and restores held on consent) goes back to the tail.
	// It releases nothing while another source still pauses the persona.
	if _, err := a.t.deliveries.ReleaseConsentHeldTx(ctx, tx, did); err != nil {
		return fmt.Errorf("release work held on the pause of %s: %w", did, err)
	}
	return nil
}

// applyCommunity applies a `!takedown` on a bridged community's bare DID (a
// communities row with that did) and reports whether l.URI named one. While
// the label stands every native object bound to the community carries the
// community-label source keyed by the labeler, and the community's other
// queued work is held on the community; a negation or lapse removes the
// source and releases that work. It decides by the label alone
// (labels.StandingTx), never by the ingest side's removal record, which in the
// same tx may not yet be written or cleared.
func (a takedownLabelApplier) applyCommunity(ctx context.Context, tx *sql.Tx, l labels.Stored) (bool, error) {
	var communityAPID string
	err := tx.QueryRowContext(ctx, `SELECT ap_group_id FROM communities WHERE did = $1`, l.URI).Scan(&communityAPID)
	if stderrors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read community of %s: %w", l.URI, err)
	}
	standing, err := labels.StandingTx(ctx, tx, l.URI, materialize.LabelTakedown)
	if err != nil {
		return true, err
	}
	community := Community{DID: l.URI, APID: communityAPID}
	src := store.TakedownSource{Kind: store.TakedownSourceCommunityLabel, Key: l.Src}
	if standing {
		return true, a.t.AddCommunitySourceTx(ctx, tx, community, src)
	}
	return true, a.t.RemoveCommunitySourceTx(ctx, tx, community, src)
}

// applyBridgedAccount applies an account `!takedown` on a DID with no
// persona. On a bridged person the standing label holds native replies to
// the person's records through the parent-removed predicate, at enqueue and
// at delivery, so nothing is written while it stands; its negation or lapse
// releases the replies held on any of the person's records
// (store.ReleaseAccountHeldTx, which takes every subject it reads in one
// sorted call). A bridged community DID, or a DID that is neither, is left
// alone; withholding the person's blobs is the materializer applier's work.
func (a takedownLabelApplier) applyBridgedAccount(ctx context.Context, tx *sql.Tx, did string) error {
	var actorType string
	err := tx.QueryRowContext(ctx, `SELECT actor_type FROM bridged_actors WHERE did = $1`, did).Scan(&actorType)
	if stderrors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read bridged actor type of %s: %w", did, err)
	}
	if store.ActorType(actorType) != store.ActorTypePerson {
		return nil
	}
	standing, err := labels.StandingTx(ctx, tx, did, materialize.LabelTakedown)
	if err != nil || standing {
		return err
	}
	if _, err := store.ReleaseAccountHeldTx(ctx, tx, did); err != nil {
		return fmt.Errorf("release replies held on the bridged account %s: %w", did, err)
	}
	return nil
}

// recordKindTx reports whether uri names a post or comment (a record of a
// federated collection), and if so whether it is native: in a repo this
// bridge does not host.
func recordKindTx(ctx context.Context, tx *sql.Tx, uri string) (record, native bool, err error) {
	parsed, err := syntax.ParseATURI(uri)
	if err != nil || !parsed.Authority().IsDID() || parsed.RecordKey() == "" {
		return false, false, nil
	}
	switch parsed.Collection().String() {
	case consume.CollectionPostV2, consume.CollectionComment:
	default:
		return false, false, nil
	}
	var hosted bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM repo_state WHERE did = $1)`,
		parsed.Authority().String()).Scan(&hosted); err != nil {
		return false, false, fmt.Errorf("read whether %s is hosted: %w", uri, err)
	}
	return true, !hosted, nil
}

// Takedown activity-id operations. Each episode of a takedown mints its own
// Delete and Undo ids, so a peer that saw episode 1 never dedupes episode 2's
// activities against them.
const (
	takedownDeleteOp = "takedown-delete"
	takedownUndoOp   = "takedown-undo"
)

// TakedownDelete renders the takedown Delete of the native object at atURI for
// one episode: a bare self-delete from the author's persona, with no summary,
// so Lemmy treats it as the author withdrawing the object (a summary would read
// as a moderator removal reason).
func (t *Translator) TakedownDelete(actorID, atURI, communityAPID string, episode int) (*TranslatedActivity, error) {
	if actorID == "" {
		return nil, errors.NewValidationError("actorID", "must not be empty")
	}
	if atURI == "" {
		return nil, errors.NewValidationError("atURI", "must not be empty")
	}
	if communityAPID == "" {
		return nil, errors.NewValidationError("communityApId", "must not be empty")
	}
	id := consume.ActivityID(t.userOrigin, atURI, takedownDeleteOp, episode)
	return t.finish(id, "Delete", "", t.deleteActivity(id, actorID, communityAPID, t.objectURL(atURI)))
}

// TakedownUndo renders the Undo of a takedown Delete. Its object is the Delete
// payload exactly as stored, because the peer matches an Undo against the
// activity it holds.
func (t *Translator) TakedownUndo(actorID, communityAPID string, deletePayload []byte, episode int) (*TranslatedActivity, error) {
	if actorID == "" {
		return nil, errors.NewValidationError("actorID", "must not be empty")
	}
	if communityAPID == "" {
		return nil, errors.NewValidationError("communityApId", "must not be empty")
	}
	var inner struct {
		ID   string `json:"id"`
		Type string `json:"type"`
	}
	if err := json.Unmarshal(deletePayload, &inner); err != nil {
		return nil, errors.NewValidationError("deletePayload", "must be a JSON activity: "+err.Error())
	}
	if inner.Type != "Delete" || inner.ID == "" {
		return nil, errors.NewValidationError("deletePayload", "must be a Delete activity with an id")
	}
	id := consume.ActivityID(t.userOrigin, inner.ID, takedownUndoOp, episode)
	return t.finish(id, "Undo", "", map[string]any{
		"@context": contextActivityStreams,
		"id":       id,
		"type":     "Undo",
		"actor":    actorID,
		"to":       []string{ap.PublicAudience},
		"cc":       []string{communityAPID},
		"audience": communityAPID,
		"object":   json.RawMessage(deletePayload),
	})
}

// AddAccountSourceTx adds src to the takedown source set of every outbound
// object of actorDID (every object in the actor's repo), taking each object
// down whose set becomes non-empty, as AddSourceTx does.
func (t *Takedowns) AddAccountSourceTx(ctx context.Context, tx *sql.Tx, actorDID string, src store.TakedownSource) error {
	objects, err := t.lockAccountObjectsTx(ctx, tx, actorDID)
	if err != nil {
		return err
	}
	for _, atURI := range objects {
		if err := t.AddSourceTx(ctx, tx, atURI, src); err != nil {
			return err
		}
	}
	return nil
}

// RemoveAccountSourceTx removes src from the takedown source set of every
// outbound object of actorDID, restoring each object whose set becomes empty,
// as RemoveSourceTx does.
func (t *Takedowns) RemoveAccountSourceTx(ctx context.Context, tx *sql.Tx, actorDID string, src store.TakedownSource) error {
	objects, err := t.lockAccountObjectsTx(ctx, tx, actorDID)
	if err != nil {
		return err
	}
	for _, atURI := range objects {
		if err := t.RemoveSourceTx(ctx, tx, atURI, src); err != nil {
			return err
		}
	}
	return nil
}

// lockAccountObjectsTx takes, in tx, every lock an account-wide source change
// needs, in the shared lock order, before any object's change runs: the
// account lock, the actor's ap_actors row, the actor's outbound_objects rows
// and then their outbound_takedowns rows (each in at-uri order), and then
// every takedown subject the per-object changes and the consent release after
// them lock (the actor DID, the objects, the own objects and parents of their
// held rows and those parents' accounts, and every subject of the actor's
// consent- and pause-held rows) in ONE sorted LockTakedownSubjectsTx call
// (store.LockTakedownSubjectsTx has the full order). Taking the subjects one
// object at a time would let a consent release, which locks its subjects in
// one sorted call, deadlock against it. The ap_actors row lock shuts out the
// worker's consent and pause holds until the tx ends.
//
// An enqueue of the actor's work takes the actor DID's subject lock, so once
// this holds it no new object of the actor commits until the tx ends, and the
// worker claims that object's work only after the pause commits. An object
// that committed between the objects read and the lock call is outside the
// set, and the worker may already have delivered it, unaware of the
// uncommitted pause. The objects and subjects are reread under the locks, and
// when they grew the attempt rolls back to a savepoint, which drops its row
// and advisory locks, and reruns with the grown set, still one sorted call per
// attempt. It returns the objects' at-uris in at-uri order.
func (t *Takedowns) lockAccountObjectsTx(ctx context.Context, tx *sql.Tx, actorDID string) ([]string, error) {
	if actorDID == "" {
		return nil, errors.NewValidationError("actor_did", "must not be empty")
	}
	if err := store.LockAccountTx(ctx, tx, actorDID); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx,
		`SELECT did FROM ap_actors WHERE did = $1 FOR NO KEY UPDATE`, actorDID); err != nil {
		return nil, fmt.Errorf("lock ap_actor %s: %w", actorDID, err)
	}
	if _, err := tx.ExecContext(ctx, `SAVEPOINT lock_account_subjects`); err != nil {
		return nil, fmt.Errorf("lock subjects of account %s: savepoint: %w", actorDID, err)
	}
	prefix := "at://" + actorDID + "/"
	for attempt := 1; attempt <= accountLockAttempts; attempt++ {
		objects, err := queryStrings(ctx, tx, `
			SELECT at_uri FROM outbound_objects
			WHERE starts_with(at_uri, $1)
			ORDER BY at_uri
			FOR UPDATE`, prefix)
		if err != nil {
			return nil, fmt.Errorf("lock outbound objects of %s: %w", actorDID, err)
		}
		if accountObjectsReadHook != nil {
			accountObjectsReadHook()
		}
		if len(objects) > 0 {
			if _, err := queryStrings(ctx, tx, `
				SELECT at_uri FROM outbound_takedowns
				WHERE at_uri = ANY($1)
				ORDER BY at_uri
				FOR UPDATE`, pq.Array(objects)); err != nil {
				return nil, fmt.Errorf("lock takedowns of %s: %w", actorDID, err)
			}
		}
		subjects, err := accountSubjectsTx(ctx, tx, actorDID, objects)
		if err != nil {
			return nil, err
		}
		if err := store.LockTakedownSubjectsTx(ctx, tx, subjects...); err != nil {
			return nil, err
		}
		// Reread without row locks: taking one after the advisory locks would
		// invert the lock order against an enqueue that holds its object row
		// and waits for the actor's subject lock.
		lockedObjects, err := queryStrings(ctx, tx, `
			SELECT at_uri FROM outbound_objects
			WHERE starts_with(at_uri, $1)
			ORDER BY at_uri`, prefix)
		if err != nil {
			return nil, fmt.Errorf("reread outbound objects of %s: %w", actorDID, err)
		}
		locked, err := accountSubjectsTx(ctx, tx, actorDID, lockedObjects)
		if err != nil {
			return nil, err
		}
		if !slices.ContainsFunc(locked, func(subject string) bool { return !slices.Contains(subjects, subject) }) {
			if _, err := tx.ExecContext(ctx, `RELEASE SAVEPOINT lock_account_subjects`); err != nil {
				return nil, fmt.Errorf("lock subjects of account %s: release savepoint: %w", actorDID, err)
			}
			if accountSubjectsLockedHook != nil {
				accountSubjectsLockedHook()
			}
			return objects, nil
		}
		if _, err := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT lock_account_subjects`); err != nil {
			return nil, fmt.Errorf("lock subjects of account %s: rollback to savepoint: %w", actorDID, err)
		}
	}
	return nil, fmt.Errorf("lock subjects of account %s: objects or held rows outgrew the locked subjects %d times",
		actorDID, accountLockAttempts)
}

// accountLockAttempts bounds lockAccountObjectsTx's relocks, as
// communityLockAttempts does the community's: past the bound the label tx or
// #account event fails and is retried.
const accountLockAttempts = 5

// accountSubjectsTx reads every takedown subject an account-wide change on
// actorDID with objects locks: the actor DID, the objects, the parents and own
// objects of the rows held on them, those parents' accounts, and the subjects
// of the actor's consent- and pause-held rows.
func accountSubjectsTx(ctx context.Context, tx *sql.Tx, actorDID string, objects []string) ([]string, error) {
	subjects := append([]string{actorDID}, objects...)
	if len(objects) > 0 {
		parents, err := queryStrings(ctx, tx, `
			SELECT DISTINCT a.parent_at_uri
			FROM outbound_deliveries d
			JOIN outbound_activities a ON a.activity_id = d.activity_id
			WHERE d.state = 'held' AND d.held_subject = ANY($1) AND a.parent_at_uri <> ''`,
			pq.Array(objects))
		if err != nil {
			return nil, fmt.Errorf("read held reply parents of %s: %w", actorDID, err)
		}
		// Each per-object restore (ReleaseHeldTx) also reads, and may rekey
		// onto, the own object of every row held on that object: a reply of
		// another author held on one of these objects as its parent.
		rowObjects, err := queryStrings(ctx, tx, `
			SELECT DISTINCT o.at_uri
			FROM outbound_deliveries d
			JOIN outbound_activities a ON a.activity_id = d.activity_id
			CROSS JOIN LATERAL (SELECT `+store.HeldObjectATURISQL+` AS at_uri) o
			WHERE d.state = 'held' AND d.held_subject = ANY($1) AND o.at_uri <> ''`,
			pq.Array(objects))
		if err != nil {
			return nil, fmt.Errorf("read held row objects of %s: %w", actorDID, err)
		}
		subjects = append(subjects, parents...)
		subjects = append(subjects, rowObjects...)
		for _, parent := range parents {
			subjects = append(subjects, store.AccountSubjectOf(parent))
		}
	}
	// The consent release that follows a lift in the same tx
	// (ReleaseConsentHeldTx) locks these too; taking them here leaves it only
	// subjects this tx already holds, except a consent-held row the enqueuer
	// commits after this read for a brand-new object. That row's subjects are
	// taken in a second call, which can deadlock (40P01); the callers roll
	// back and retry (store.LockTakedownSubjectsTx names the exception).
	consentHeld, err := store.ConsentHeldSubjectsTx(ctx, tx, actorDID)
	if err != nil {
		return nil, err
	}
	return append(subjects, consentHeld...), nil
}

// Community names a bridged community: its DID (the subject of its
// `!takedown`) and its AP Group id (the ordering key of native work into it).
type Community struct{ DID, APID string }

// AddCommunitySourceTx adds src to the takedown source set of every native
// outbound object bound to community (outbound_objects.community_did), taking
// each object down whose set becomes non-empty, as AddSourceTx does: its
// queued Create/Update rows are held and a Delete is sent only for an object
// the community accepted. Then the community's remaining queued publishing
// work and votes (a reply with no object row of its own, a vote) are held on
// the community. Delivered votes and retractions are left alone; nothing is
// cancelled. Adding a source already in a set changes nothing, so a replay is
// idempotent.
func (t *Takedowns) AddCommunitySourceTx(ctx context.Context, tx *sql.Tx, community Community, src store.TakedownSource) error {
	objects, err := t.lockCommunityObjectsTx(ctx, tx, community)
	if err != nil {
		return err
	}
	for _, atURI := range objects {
		if err := t.AddSourceTx(ctx, tx, atURI, src); err != nil {
			return err
		}
	}
	held, err := t.deliveries.HoldCommunityWorkTx(ctx, tx, community.APID, community.DID)
	if err != nil {
		return err
	}
	t.logger.Info("native work into a removed community held",
		"community", community.APID, "objects", len(objects), "held", held)
	return nil
}

// RemoveCommunitySourceTx removes src from the takedown source set of every
// native outbound object bound to community. Every object whose set becomes
// empty gets its Undo{Delete} (when a Delete was sent) first, so the restores
// lead the released work on the community's ordering key; then the work held
// on those objects, on the community, and on the community's bridged records
// as parents is released to the tail, each row rechecked against its other
// blockers. An object another source still removes stays down.
func (t *Takedowns) RemoveCommunitySourceTx(ctx context.Context, tx *sql.Tx, community Community, src store.TakedownSource) error {
	objects, err := t.lockCommunityObjectsTx(ctx, tx, community)
	if err != nil {
		return err
	}
	var cleared []string
	for _, atURI := range objects {
		object, err := t.lockObject(ctx, tx, atURI)
		if err != nil {
			return err
		}
		transition, err := t.takedowns.RemoveSourceTx(ctx, tx, atURI, src)
		if err != nil {
			return err
		}
		if transition != store.TakedownBecameClear {
			continue
		}
		if err := t.undoDeleteTx(ctx, tx, atURI, object); err != nil {
			return err
		}
		cleared = append(cleared, atURI)
	}
	parents, err := communityHeldParentsTx(ctx, tx, community.DID)
	if err != nil {
		return err
	}
	subjects := append(slices.Clone(cleared), community.DID)
	subjects = append(subjects, parents...)
	var released int64
	for _, subject := range subjects {
		n, err := t.deliveries.ReleaseHeldTx(ctx, tx, subject)
		if err != nil {
			return err
		}
		released += n
	}
	t.logger.Info("native work into a restored community released",
		"community", community.APID, "restored", len(cleared), "released", released)
	return nil
}

// lockCommunityObjectsTx takes, in tx, every lock a community-wide source
// change needs, in the shared lock order, before any object's change runs:
// the community's outbound_objects rows and then their outbound_takedowns rows
// (each in at-uri order), and then, in ONE sorted LockTakedownSubjectsTx call,
// every takedown subject the per-object changes and the releases after them
// lock: the objects, the community DID, the community's bridged records that
// replies are held on, and the own objects, parents and parents' accounts of
// every row held on any of those. Taking them one object at a time would let
// a consent release or another multi-subject holder deadlock against it.
//
// An enqueue or a worker hold that reads the community's removal takes the
// community DID's lock, so once this holds it no new hold on the community or
// on one of its records commits until the tx ends, and an enqueue that has
// not yet read the removal reads the label once this tx commits. One that
// committed between the objects read and the lock call can add a subject
// outside the locked set: a held row, or a new object bound to the community
// that the worker may already have delivered, unaware of the uncommitted
// label. The objects and subjects are reread under the locks, and when they
// grew the attempt rolls back to a savepoint, which drops its row and
// advisory locks, and reruns with the grown set, still one sorted call per
// attempt. It returns the objects' at-uris in at-uri order.
func (t *Takedowns) lockCommunityObjectsTx(ctx context.Context, tx *sql.Tx, community Community) ([]string, error) {
	if community.DID == "" || community.APID == "" {
		return nil, errors.NewValidationError("community", "must name a DID and an AP id")
	}
	if _, err := tx.ExecContext(ctx, `SAVEPOINT lock_community_subjects`); err != nil {
		return nil, fmt.Errorf("lock subjects of community %s: savepoint: %w", community.DID, err)
	}
	for attempt := 1; attempt <= communityLockAttempts; attempt++ {
		objects, err := lockCommunityObjectRowsTx(ctx, tx, community.DID)
		if err != nil {
			return nil, err
		}
		if communityObjectsReadHook != nil {
			communityObjectsReadHook()
		}
		subjects, err := communitySubjectsTx(ctx, tx, community.DID, objects)
		if err != nil {
			return nil, err
		}
		if err := store.LockTakedownSubjectsTx(ctx, tx, subjects...); err != nil {
			return nil, err
		}
		// Reread without row locks: taking one after the advisory locks would
		// invert the lock order against an enqueue that holds its object row
		// and waits for the community's subject lock.
		lockedObjects, err := queryStrings(ctx, tx, `
			SELECT at_uri FROM outbound_objects
			WHERE community_did = $1
			ORDER BY at_uri`, community.DID)
		if err != nil {
			return nil, fmt.Errorf("reread outbound objects of community %s: %w", community.DID, err)
		}
		locked, err := communitySubjectsTx(ctx, tx, community.DID, lockedObjects)
		if err != nil {
			return nil, err
		}
		if !slices.ContainsFunc(locked, func(subject string) bool { return !slices.Contains(subjects, subject) }) {
			if _, err := tx.ExecContext(ctx, `RELEASE SAVEPOINT lock_community_subjects`); err != nil {
				return nil, fmt.Errorf("lock subjects of community %s: release savepoint: %w", community.DID, err)
			}
			return objects, nil
		}
		if _, err := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT lock_community_subjects`); err != nil {
			return nil, fmt.Errorf("lock subjects of community %s: rollback to savepoint: %w", community.DID, err)
		}
	}
	return nil, fmt.Errorf("lock subjects of community %s: objects or held rows outgrew the locked subjects %d times",
		community.DID, communityLockAttempts)
}

// lockCommunityObjectRowsTx locks, in at-uri order, the outbound_objects rows
// bound to communityDID and then their outbound_takedowns rows, and returns
// the objects' at-uris.
func lockCommunityObjectRowsTx(ctx context.Context, tx *sql.Tx, communityDID string) ([]string, error) {
	objects, err := queryStrings(ctx, tx, `
		SELECT at_uri FROM outbound_objects
		WHERE community_did = $1
		ORDER BY at_uri
		FOR UPDATE`, communityDID)
	if err != nil {
		return nil, fmt.Errorf("lock outbound objects of community %s: %w", communityDID, err)
	}
	if len(objects) > 0 {
		if _, err := queryStrings(ctx, tx, `
			SELECT at_uri FROM outbound_takedowns
			WHERE at_uri = ANY($1)
			ORDER BY at_uri
			FOR UPDATE`, pq.Array(objects)); err != nil {
			return nil, fmt.Errorf("lock takedowns of community %s: %w", communityDID, err)
		}
	}
	return objects, nil
}

// accountObjectsReadHook, when set (tests only), runs in lockAccountObjectsTx
// after it locks the actor's objects and before it takes their subjects'
// locks.
var accountObjectsReadHook func()

// accountSubjectsLockedHook, when set (tests only), runs in
// lockAccountObjectsTx once it holds every subject lock, before it returns.
var accountSubjectsLockedHook func()

// communityObjectsReadHook, when set (tests only), runs in each attempt of
// lockCommunityObjectsTx after it locks the community's objects and before it
// takes their subjects' locks.
var communityObjectsReadHook func()

// communityLockAttempts bounds lockCommunityObjectsTx's relocks. Each needs a
// hold or an object committed in the short window between its objects read
// and its lock;
// past the bound the label tx fails and the consumer retries it.
const communityLockAttempts = 5

// communitySubjectsTx reads every takedown subject a community-wide change on
// communityDID with objects locks: the objects, the community DID, the
// community's bridged records with replies held on them, and the own object,
// parent and parent's account of every row held on any of those.
func communitySubjectsTx(ctx context.Context, tx *sql.Tx, communityDID string, objects []string) ([]string, error) {
	parents, err := communityHeldParentsTx(ctx, tx, communityDID)
	if err != nil {
		return nil, err
	}
	subjects := append(slices.Clone(objects), communityDID)
	subjects = append(subjects, parents...)
	rowSubjects, err := queryStrings(ctx, tx, `
		SELECT DISTINCT s.subject
		FROM outbound_deliveries d
		JOIN outbound_activities a ON a.activity_id = d.activity_id
		CROSS JOIN LATERAL (VALUES (`+store.HeldObjectATURISQL+`), (a.parent_at_uri),
		                           (COALESCE(substring(a.parent_at_uri FROM '^at://(did:[^/]*)/'), ''))) AS s(subject)
		WHERE d.state = 'held' AND d.held_subject = ANY($1) AND s.subject <> ''
		ORDER BY 1`, pq.Array(subjects))
	if err != nil {
		return nil, fmt.Errorf("read held row subjects of community %s: %w", communityDID, err)
	}
	subjects = append(subjects, rowSubjects...)
	slices.Sort(subjects)
	return slices.Compact(subjects), nil
}

// communityHeldParentsTx reads, in at-uri order, the records bound to the
// community (ap_objects.community_did) that replies are held on as their
// parent: the community's negation releases them. A record with no stored
// community is bound from its record by the materializer's label applier in
// the community `!takedown`'s transaction, before any reply can be held on it
// as the community's (the parent check reads the same column), so the column
// names it here.
func communityHeldParentsTx(ctx context.Context, tx *sql.Tx, communityDID string) ([]string, error) {
	parents, err := queryStrings(ctx, tx, `
		SELECT DISTINCT d.held_subject
		FROM outbound_deliveries d
		JOIN ap_objects o ON o.at_uri = d.held_subject
		WHERE d.state = 'held' AND d.held_reason = $2 AND o.community_did = $1
		ORDER BY 1`, communityDID, store.HeldReasonParent)
	if err != nil {
		return nil, fmt.Errorf("read held reply parents of community %s: %w", communityDID, err)
	}
	return parents, nil
}

// queryStrings runs a one-column query in tx and returns its values.
func queryStrings(ctx context.Context, tx *sql.Tx, query string, args ...any) ([]string, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	var values []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			_ = rows.Close()
			return nil, err
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	return values, rows.Close()
}
