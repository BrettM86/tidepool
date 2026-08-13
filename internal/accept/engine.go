// Package accept is the acceptance engine for bridged communities. A native
// user writes a social.coves.community.postv2 into their own repo targeting a
// bridged community; Tidepool — the community's key holder — decides ADMISSION
// and, on admit, writes the community-signed acceptance record while enqueueing
// the Create/Update/Delete{Page} for Lemmy delivery ATOMICALLY with it (both
// ride ONE acceptrec commit via its side effect). Un-accepted posts are
// invisible on both sides by construction.
//
// The engine is the POLICY layer over task 19's mechanics (acceptrec: digest
// rkeys, multi-op commits) and task 15's outbound (the enqueue). It owns the
// native-author lifecycle: lazy actor mint on first accepted post, re-acceptance
// on edits, acceptance-delete on author-delete, and the admissions ledger that
// records every rejection with a machine-readable reason.
package accept

import (
	"context"
	"database/sql"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/bluesky-social/indigo/atproto/lexicon"

	"tidepool/internal/acceptrec"
	"tidepool/internal/consume"
	"tidepool/internal/errors"
	"tidepool/internal/repo"
	"tidepool/internal/store"
	"tidepool/lexicons"
)

// Decision codes recorded on a rejected/removed admission (migration 021's
// decision_code). Distinct codes are what the admin surface needs; the
// firehose acceptance/removal records cannot carry them.
const (
	// DecisionOptedOut: the author has an opt-out federation record, so pushing
	// their post outward is exactly what they refused.
	DecisionOptedOut = "opted-out"
	// DecisionTitleRequired: a postv2 with no title (media-only) — Lemmy
	// rejects a titleless post, and no title-derivation product decision exists.
	DecisionTitleRequired = "title-required"
	// DecisionTitleTooLong: over Lemmy's 200-char title cap.
	DecisionTitleTooLong = "title-too-long"
	// DecisionPaused: the author's account is #account-paused (decision 19) —
	// deactivated/suspended/takendown/throttled. Delivery is halted, so a new
	// post is not admitted while the identity is paused (it can be re-driven).
	DecisionPaused = "paused"
	// DecisionRateLimit: the author exceeded the per-author-per-community accept
	// cap (Tidepool must not let one native account flood a Lemmy community it
	// vouches for).
	DecisionRateLimit = "rate-limit-exceeded"
	// DecisionLexiconInvalid: the postv2 failed strict lexicon validation. WE
	// sign the acceptance, so native input that does not validate is fail-closed.
	DecisionLexiconInvalid = "lexicon-invalid"
	// DecisionCommunityImmutable: an UPDATE tried to MOVE the post to a different
	// community than the one it was accepted into. The lexicon makes `community`
	// immutable; the whole event is discarded. (Recorded on the ORIGINAL
	// community's admissions row as a no-op annotation, if at all — the engine
	// writes nothing to the target community.)
	DecisionCommunityImmutable = "community-immutable"
)

// RemovalCodeAdmissionRevoked is the removal `code` written when a post that WAS
// accepted fails RE-admission (an edit made it titleless or over the cap).
//
// PROPOSED — FLAGGED FOR COORDINATOR RULING. The removal lexicon's knownValues
// (rule-violation, spam, off-topic, illegal-content, author-banned,
// moderator-discretion) are ALL moderation reasons, and an admission revocation
// is not a moderator's decision — writing moderator-discretion would assert a
// moderator acted when none did. knownValues is explicitly an OPEN set (peers
// may send unseen codes and they must still validate), so a precise new code is
// legal. The SPECIFIC cause (title-required / title-too-long) is recorded in the
// admissions ledger's decision_code; this open-set code is the firehose-visible
// one. Alternative if the coordinator prefers a knownValue: "rule-violation".
const RemovalCodeAdmissionRevoked = "admission-revoked"

// lemmyTitleCap is Lemmy 0.19.20's post-title length limit.
const lemmyTitleCap = 200

// Options wires an Engine.
type Options struct {
	// Repos is the community-repo commit surface (acceptrec drives it). A
	// *repo.Manager satisfies it.
	Repos acceptrec.RepoManager
	// Enqueuer is the task 15 outbound seam; the engine hands it the Page intent
	// as the acceptance commit's side effect.
	Enqueuer consume.OutboundEnqueuer
	// Actors lazily mints the native author's AP identity on the first accept.
	Actors consume.ActorMinter
	// Resolver bidirectionally verifies the author's handle before the first
	// mint (the local part is frozen at creation).
	Resolver consume.DIDResolver
	// Communities resolves a community DID to its AP Group id (for the intent's
	// addressing) and confirms it is still bridged.
	Communities store.Communities
	// Objects is the post's outbound state row (community_did, snapshot) a later
	// Delete is rebuilt from — the engine owns outbound_objects for posts.
	Objects store.OutboundObjects
	// Prefs reads the author's federation preference: the opt-out check MOVED
	// here from the consumer, so an opted-out author's post reaches the engine
	// and is RECORDED as a rejection rather than silently dropped upstream.
	Prefs store.FederationPrefs
	// Admissions is the decision ledger (migration 021).
	Admissions *Admissions
	// APActors reads the author's AP actor row for the delivery-paused admission
	// check (decision 19). OPTIONAL: nil skips the paused check (the seam is not
	// wired yet — flagged for the paused-rejection lifecycle).
	APActors store.APActors
	// MaxPerAuthorPerCommunity caps accepted posts by one author in one community
	// within the ledger (the per-author-per-community flood guard,
	// ADMISSION_MAX_PER_AUTHOR_PER_COMMUNITY). 0 means UNLIMITED (the generous
	// default); a positive value is the cap the rate check enforces.
	MaxPerAuthorPerCommunity int
	// UserOrigin is AP_USER_ORIGIN: the origin every deterministic activity id
	// is minted under.
	UserOrigin string
	// Logger receives drop reasons. Nil uses slog.Default().
	Logger *slog.Logger
}

// Engine admits native posts into bridged communities.
type Engine struct {
	repos           acceptrec.RepoManager
	enqueuer        consume.OutboundEnqueuer
	actors          consume.ActorMinter
	resolver        consume.DIDResolver
	communities     store.Communities
	objects         store.OutboundObjects
	prefs           store.FederationPrefs
	admissions      *Admissions
	apActors        store.APActors
	maxPerCommunity int
	catalog         *lexicon.BaseCatalog
	userOrigin      string
	logger          *slog.Logger
}

// The engine is the task 16 acceptance seam the dispatcher hands postv2 commits
// to.
var _ consume.AcceptanceEngine = (*Engine)(nil)

// NewEngine wires an Engine. All seams are required except Logger.
func NewEngine(opts Options) (*Engine, error) {
	switch {
	case opts.Repos == nil:
		return nil, errors.NewValidationError("Repos", "must not be nil")
	case opts.Enqueuer == nil:
		return nil, errors.NewValidationError("Enqueuer", "must not be nil")
	case opts.Actors == nil:
		return nil, errors.NewValidationError("Actors", "must not be nil")
	case opts.Resolver == nil:
		return nil, errors.NewValidationError("Resolver", "must not be nil")
	case opts.Communities == nil:
		return nil, errors.NewValidationError("Communities", "must not be nil")
	case opts.Objects == nil:
		return nil, errors.NewValidationError("Objects", "must not be nil")
	case opts.Prefs == nil:
		return nil, errors.NewValidationError("Prefs", "must not be nil")
	case opts.Admissions == nil:
		return nil, errors.NewValidationError("Admissions", "must not be nil")
	case opts.UserOrigin == "":
		return nil, errors.NewValidationError("UserOrigin", "must not be empty")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	// The vendored lexicon catalog validates native postv2 input strictly: WE
	// sign the acceptance, so input that does not validate is fail-closed. Loaded
	// once at construction — a broken vendored file fails startup, not admission.
	catalog, err := lexicons.Catalog()
	if err != nil {
		return nil, fmt.Errorf("accept: load lexicon catalog: %w", err)
	}
	return &Engine{
		repos:           opts.Repos,
		enqueuer:        opts.Enqueuer,
		actors:          opts.Actors,
		resolver:        opts.Resolver,
		communities:     opts.Communities,
		objects:         opts.Objects,
		prefs:           opts.Prefs,
		admissions:      opts.Admissions,
		apActors:        opts.APActors,
		maxPerCommunity: opts.MaxPerAuthorPerCommunity,
		catalog:         catalog,
		userOrigin:      opts.UserOrigin,
		logger:          logger,
	}, nil
}

// AdmitPost decides admission for one postv2 commit and, on admit, writes the
// community acceptance record while enqueueing the Page delivery atomically with
// it. create/update run admission on the event's content; delete takes the
// acceptance down and enqueues Delete{Page} from stored state.
func (e *Engine) AdmitPost(ctx context.Context, did string, commit *consume.CommitEvent) error {
	postURI := fmt.Sprintf("at://%s/%s/%s", did, commit.Collection, commit.RKey)

	// An author-delete carries no record body: the retraction is built entirely
	// from stored outbound state, and it is NOT moderation, so it takes the
	// acceptance down WITHOUT a removal record.
	if commit.Operation == operationDelete {
		return e.authorDelete(ctx, did, postURI)
	}

	communityDID, _ := commit.Record["community"].(string)
	if communityDID == "" {
		// The consumer already refuses a postv2 with no community, but the engine
		// re-asserts it: WE sign the acceptance, so a missing target is fail-closed.
		return errors.NewValidationError("community", "postv2 "+commit.RKey+" names no community")
	}

	// The post's current binding: whether it was already accepted (so a now-failing
	// re-admission is a REMOVAL, not a fresh rejection) and which community it is
	// bound to (so a community-moving edit is discarded whole). The engine writes
	// outbound_objects ONLY on accept, so a row here means the post federated.
	prior, priorBound, err := e.priorBinding(ctx, postURI)
	if err != nil {
		return err
	}

	// Decide admission. The order is deliberate (fail closed first, cheap policy
	// last): lexicon-validate → community-immutable → opt-out → paused → title →
	// rate cap. A discard means the whole event is dropped (nothing written to
	// either community); a non-empty code is a rejection/removal cause.
	code, discard, err := e.decide(ctx, did, commit, communityDID, prior, priorBound)
	if err != nil {
		return err
	}
	if discard {
		e.logger.Debug("discarding community-moving edit",
			slog.String("did", did), slog.String("post", postURI),
			slog.String("bound_community", prior.CommunityDID), slog.String("event_community", communityDID))
		return nil
	}

	if code != "" {
		// A post that WAS accepted and now fails re-admission is REMOVED (it
		// federated once, so leaving it alone would strand it live on Lemmy); one
		// that was never accepted is simply a recorded rejection.
		priorAccepted := priorBound && !prior.IsTombstoned()
		if priorAccepted {
			return e.removeAccepted(ctx, did, communityDID, postURI, commit, prior, code)
		}
		e.logger.Debug("rejecting postv2",
			slog.String("did", did), slog.String("post", postURI), slog.String("reason", code))
		return e.admissions.Record(ctx, Admission{
			AuthorDID:    did,
			CommunityDID: communityDID,
			PostURI:      postURI,
			Status:       StatusRejected,
			DecisionCode: code,
			EvaluatedCID: commit.CID,
			// The record body + context this decision was made against, so a later
			// force re-admit can re-run admission from stored state (a rejection
			// writes no outbound_objects, and the author's PDS is not local).
			EvaluatedSnapshot: evaluatedSnapshot(commit),
		})
	}

	return e.accept(ctx, did, communityDID, postURI, commit)
}

// operationDelete is the Jetstream commit operation for a record deletion.
const operationDelete = "delete"

// decide runs the admission checks in order and returns the rejection/removal
// code ("" = admit), or discard=true when the event must be dropped whole (a
// community-moving edit). The order fails closed first: garbage input never
// reaches a policy check, and a hijack (community move) is refused before the
// author's own preferences are consulted.
func (e *Engine) decide(ctx context.Context, did string, commit *consume.CommitEvent, communityDID string,
	prior *store.OutboundObject, priorBound bool) (code string, discard bool, err error) {

	// 1. Strict lexicon validation of the native input — fail closed.
	if !e.lexiconValid(commit.Record) {
		return DecisionLexiconInvalid, false, nil
	}

	// 2. Community immutability. The lexicon marks `community` immutable: an
	// UPDATE that names a different community than the post was accepted into is
	// a retarget, which means writing a NEW post — so the whole event is
	// discarded, not partially applied.
	if priorBound && prior.CommunityDID != communityDID {
		return "", true, nil
	}

	// 3. Opt-out (decision 11): content pushed outward is exactly what an
	// opted-out author refused.
	federating, err := e.mayFederate(ctx, did)
	if err != nil {
		return "", false, err
	}
	if !federating {
		return DecisionOptedOut, false, nil
	}

	// 4. Paused (#account, decision 19): delivery is halted while the identity is
	// deactivated/suspended/takendown/throttled, so a new post is not admitted.
	if e.apActors != nil {
		actor, err := e.apActors.GetByDID(ctx, did)
		switch {
		case err == nil:
			if actor.DeliveryPaused {
				return DecisionPaused, false, nil
			}
		case errors.IsNotFound(err):
			// No actor yet: an unseen author is not paused (it is minted on admit).
		default:
			return "", false, fmt.Errorf("accept: read actor for %s: %w", did, err)
		}
	}

	// 5. Title: required and within Lemmy's cap (postv2 title is OPTIONAL in the
	// lexicon, so this is admission policy, not validation).
	title, _ := commit.Record["title"].(string)
	if title == "" {
		return DecisionTitleRequired, false, nil
	}
	if len(title) > lemmyTitleCap {
		return DecisionTitleTooLong, false, nil
	}

	// 6. Rate cap: one author must not flood a community Tidepool vouches for.
	// Counts the author's currently-accepted posts in this community, excluding
	// this post so a repin never counts against itself. 0 means unlimited.
	if e.maxPerCommunity > 0 {
		postURI := fmt.Sprintf("at://%s/%s/%s", did, commit.Collection, commit.RKey)
		n, err := e.admissions.CountAccepted(ctx, did, communityDID, postURI)
		if err != nil {
			return "", false, err
		}
		if n >= e.maxPerCommunity {
			return DecisionRateLimit, false, nil
		}
	}

	return "", false, nil
}

// accept writes the community-signed acceptance and enqueues the Create/Update
// {Page} atomically with it (both ride ONE acceptrec commit via its side
// effect). A repin (UPDATE with a new CID) re-pins the same digest rkey and
// enqueues Update{Page}; a fresh create enqueues Create{Page}.
func (e *Engine) accept(ctx context.Context, did, communityDID, postURI string, commit *consume.CommitEvent) error {
	// The community's AP Group id is the Page's addressing target; the lookup
	// also re-confirms the community is one we federate.
	community, err := e.communities.GetByDID(ctx, communityDID)
	if err != nil {
		return fmt.Errorf("accept: resolve community %s: %w", communityDID, err)
	}

	// Lazy-mint the author BEFORE the acceptance tx: the outbound enqueue (the
	// acceptance commit's side effect) resolves the author's AP actor by DID on
	// the community-repo tx, so the actor row must already be committed and
	// visible when that side effect runs. This is the first federating
	// interaction, so the mint happens here rather than eagerly.
	if err := e.ensureActor(ctx, did); err != nil {
		return err
	}

	snapshot, err := json.Marshal(map[string]any{
		"atUri":         postURI,
		"cid":           commit.CID,
		"rev":           commit.Rev,
		"collection":    commit.Collection,
		"record":        commit.Record,
		"communityApId": community.APGroupID,
	})
	if err != nil {
		return fmt.Errorf("accept: snapshot %s: %w", postURI, err)
	}

	rkey := acceptrec.SubjectRKey(postURI)
	apObjectID := e.userOrigin + "/ap/object/" + did + "/" + commit.Collection + "/" + commit.RKey

	// The side effect rides the acceptance commit's transaction: the outbound
	// state row, the delivery enqueue, and the accepted ledger row all land WITH
	// the acceptance record or not at all. A failing enqueue returns the error,
	// which rolls the acceptance back too (ApplyOpsTx side-effect atomicity), and
	// AdmitPost propagates it so the event retries.
	sideEffect := func(sctx context.Context, tx *sql.Tx, _ *repo.CommitResult) error {
		stored, err := e.objects.UpsertTx(sctx, tx, store.OutboundObject{
			ATURI:              postURI,
			APObjectID:         apObjectID,
			LastCID:            commit.CID,
			LastRev:            commit.Rev,
			CommunityDID:       communityDID,
			CommunityAPID:      community.APGroupID,
			TranslatedSnapshot: snapshot,
			Depth:              0,
		})
		if err != nil {
			return fmt.Errorf("accept: write outbound state for %s: %w", postURI, err)
		}
		intent := consume.PostIntent{
			Op:            commit.Operation,
			ATURI:         postURI,
			ID:            consume.ActivityID(e.userOrigin, postURI, commit.Operation, stored.LastActivitySeq),
			CommunityAPID: community.APGroupID,
			Snapshot:      snapshot,
		}
		// A post has no causal parent, so orderingKey is the author DID and there
		// is no parentATURI.
		if err := e.enqueuer.EnqueueActivity(sctx, tx, did, did, "", intent); err != nil {
			return err
		}
		return e.admissions.RecordTx(sctx, tx, Admission{
			AuthorDID:         did,
			CommunityDID:      communityDID,
			PostURI:           postURI,
			Status:            StatusAccepted,
			EvaluatedCID:      commit.CID,
			AcceptanceRKey:    rkey,
			AcceptedCID:       commit.CID,
			EvaluatedSnapshot: evaluatedSnapshot(commit),
		})
	}

	if _, err := acceptrec.AcceptSubject(ctx, e.repos, communityDID, postURI, commit.CID,
		publishedAtOf(commit.Record), sideEffect); err != nil {
		return fmt.Errorf("accept: admit %s into %s: %w", postURI, communityDID, err)
	}
	return nil
}

// removeAccepted withdraws a post that WAS accepted and now fails re-admission:
// the acceptance is deleted and a removal (code admission-revoked) written in ONE
// commit, carrying the Delete{Page} enqueue as the side effect. The admissions
// ledger records the SPECIFIC cause (title-required, …) even though the
// firehose-visible removal code is the open-set admission-revoked one. The
// author's post record is untouched — a community removal says where the post may
// appear, not whether it exists.
func (e *Engine) removeAccepted(ctx context.Context, did, communityDID, postURI string,
	commit *consume.CommitEvent, prior *store.OutboundObject, code string) error {

	sideEffect := func(sctx context.Context, tx *sql.Tx, _ *repo.CommitResult) error {
		// Tombstone the outbound state (the post is out) and build the Delete
		// {Page} from it — the retraction carries no body of its own.
		dead, err := e.objects.TombstoneTx(sctx, tx, postURI)
		if err != nil {
			return fmt.Errorf("accept: tombstone outbound state for %s: %w", postURI, err)
		}
		intent := consume.PostIntent{
			Op:            operationDelete,
			ATURI:         postURI,
			ID:            consume.ActivityID(e.userOrigin, postURI, operationDelete, dead.LastActivitySeq),
			CommunityAPID: dead.CommunityAPID,
			Snapshot:      dead.TranslatedSnapshot,
		}
		if err := e.enqueuer.EnqueueActivity(sctx, tx, did, did, "", intent); err != nil {
			return err
		}
		return e.admissions.RecordTx(sctx, tx, Admission{
			AuthorDID:         did,
			CommunityDID:      communityDID,
			PostURI:           postURI,
			Status:            StatusRemoved,
			DecisionCode:      code,
			EvaluatedCID:      commit.CID,
			EvaluatedSnapshot: evaluatedSnapshot(commit),
		})
	}

	// The removal pins the version that was accepted when it was removed (audit
	// metadata); the code is the open-set admission-revoked, not a moderation
	// reason — no moderator acted.
	if _, err := acceptrec.Remove(ctx, e.repos, communityDID, postURI, prior.LastCID,
		RemovalCodeAdmissionRevoked, "", publishedAtOf(commit.Record), sideEffect); err != nil {
		return fmt.Errorf("accept: remove %s from %s: %w", postURI, communityDID, err)
	}
	return nil
}

// authorDelete takes an accepted post's acceptance down when its author deletes
// the postv2. Author deletion is NOT moderation, so NO removal record is written
// — the acceptance just goes away — and the Delete{Page} is built from stored
// outbound state (the delete commit carries no body). The ledger row is DELETED:
// the decided post is gone and no removal record stands to explain a 'removed'
// status. All of it rides ONE acceptrec commit, so it is atomic and idempotent
// under replay.
func (e *Engine) authorDelete(ctx context.Context, did, postURI string) error {
	stored, err := e.objects.GetByATURI(ctx, postURI)
	if errors.IsNotFound(err) {
		// A post this bridge never accepted: nothing to withdraw.
		e.logger.Debug("author delete for a post with no outbound state",
			slog.String("did", did), slog.String("post", postURI))
		return nil
	}
	if err != nil {
		return fmt.Errorf("accept: read outbound state for %s: %w", postURI, err)
	}
	communityDID := stored.CommunityDID

	sideEffect := func(sctx context.Context, tx *sql.Tx, _ *repo.CommitResult) error {
		dead, err := e.objects.TombstoneTx(sctx, tx, postURI)
		if err != nil {
			return fmt.Errorf("accept: tombstone outbound state for %s: %w", postURI, err)
		}
		intent := consume.PostIntent{
			Op:            operationDelete,
			ATURI:         postURI,
			ID:            consume.ActivityID(e.userOrigin, postURI, operationDelete, dead.LastActivitySeq),
			CommunityAPID: dead.CommunityAPID,
			Snapshot:      dead.TranslatedSnapshot,
		}
		if err := e.enqueuer.EnqueueActivity(sctx, tx, did, did, "", intent); err != nil {
			return err
		}
		return e.admissions.DeleteTx(sctx, tx, communityDID, postURI)
	}

	if _, err := acceptrec.DeleteAcceptance(ctx, e.repos, communityDID, postURI, sideEffect); err != nil {
		return fmt.Errorf("accept: author-delete %s from %s: %w", postURI, communityDID, err)
	}
	return nil
}

// priorBinding reads the post's outbound state — present only after an accept —
// so the engine knows whether it federated and which community it is bound to.
// A miss is reported as (nil, false, nil): a post the bridge has not accepted.
func (e *Engine) priorBinding(ctx context.Context, postURI string) (*store.OutboundObject, bool, error) {
	stored, err := e.objects.GetByATURI(ctx, postURI)
	if errors.IsNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("accept: read outbound state for %s: %w", postURI, err)
	}
	return stored, true, nil
}

// lexiconValid reports whether the postv2 record passes strict validation against
// the vendored lexicon catalog. Invalid input is fail-closed (rejected), never
// signed. A record with no $type, or one whose $type has no schema, is invalid.
func (e *Engine) lexiconValid(record map[string]any) bool {
	recordType, _ := record["$type"].(string)
	if recordType == "" {
		return false
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return false
	}
	data, err := atdata.UnmarshalJSON(raw)
	if err != nil {
		return false
	}
	return lexicon.ValidateRecord(e.catalog, data, recordType, lexicon.ValidateFlags(0)) == nil
}

// mayFederate reports whether the author permits outbound federation. A missing
// preference MEANS default-on (decision 11), not unknown.
func (e *Engine) mayFederate(ctx context.Context, did string) (bool, error) {
	pref, err := e.prefs.Get(ctx, did)
	if errors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("accept: read federation preference for %s: %w", did, err)
	}
	return pref.Enabled, nil
}

// ensureActor lazily mints the author's AP identity. CreateActorForDID is
// get-or-create, so a redelivery (or a second accepted post) reuses the existing
// actor rather than minting a second. The handle is resolved through the
// bidirectional verifier because the local part is frozen at creation.
func (e *Engine) ensureActor(ctx context.Context, did string) error {
	handle, err := e.resolver.ResolveDIDHandle(ctx, did)
	if err != nil {
		return fmt.Errorf("accept: resolve handle for %s: %w", did, err)
	}
	if _, err := e.actors.CreateActorForDID(ctx, did, handle); err != nil {
		return fmt.Errorf("accept: mint actor for %s: %w", did, err)
	}
	return nil
}

// publishedAtOf derives the timestamp the acceptance's createdAt is rendered
// from — the post's own createdAt, so a redelivery re-puts byte-identical bytes
// and the repo layer's no-op path absorbs it. An unparseable or absent value
// falls back to the zero time, which is still deterministic.
func publishedAtOf(record map[string]any) time.Time {
	if s, ok := record["createdAt"].(string); ok && s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

// ReadmitResult reports the outcome of a force re-admit (A2). Enqueued is true
// when the re-admission newly wrote/repinned the acceptance and enqueued a
// Create/Update{Page}; false when the post STILL fails admission (the result
// then carries the current rejection Status and DecisionCode).
type ReadmitResult struct {
	PostURI      string
	Status       string
	DecisionCode string
	Enqueued     bool
}

// ErrUnrecoverableReadmit is returned by Readmit when the post's admissions row
// carries no stored record snapshot ('{}' — a legacy row, or a decision made
// before migration 022). Admission cannot be re-run from nothing, and the postv2
// lives in the author's native PDS Tidepool does not host, so this surfaces as a
// distinct error (HTTP 422) rather than a silent no-op. A task-18
// com.atproto.repo.getRecord fetch would recover it.
var ErrUnrecoverableReadmit = stderrors.New("accept: no stored record snapshot to re-run admission from")

// Readmit force re-runs admission for ONE post (the mod-override seam task 17
// reuses for restore). It re-runs the SAME decide() path against the record
// snapshot stored on the post's admissions row (evaluated_snapshot, migration
// 022) — task 16 re-admits from STORED STATE, because the postv2 lives in the
// author's native PDS that Tidepool does not host, so there is no local record to
// re-read. Passes now → the acceptance is written/repinned and a Create/Update
// {Page} enqueued (Enqueued=true); still fails → the current rejection/removal is
// reported and nothing is enqueued.
func (e *Engine) Readmit(ctx context.Context, postATURI string) (*ReadmitResult, error) {
	adm, err := e.admissions.GetByPostURI(ctx, postATURI)
	if err != nil {
		return nil, err // NotFound flows through; the handler maps it to 404.
	}

	commit, did, ok := rebuildCommit(postATURI, adm.EvaluatedSnapshot)
	if !ok {
		return nil, ErrUnrecoverableReadmit
	}
	communityDID, _ := commit.Record["community"].(string)
	if communityDID == "" {
		communityDID = adm.CommunityDID
	}

	// Re-run the exact admission pipeline AdmitPost uses — no duplicated policy.
	prior, priorBound, err := e.priorBinding(ctx, postATURI)
	if err != nil {
		return nil, err
	}
	code, discard, err := e.decide(ctx, did, commit, communityDID, prior, priorBound)
	if err != nil {
		return nil, err
	}
	if discard {
		// The stored community no longer matches the event's — nothing is written;
		// reported as still-failing with the immutability cause.
		return &ReadmitResult{PostURI: postATURI, Status: adm.Status, DecisionCode: DecisionCommunityImmutable}, nil
	}
	if code != "" {
		priorAccepted := priorBound && !prior.IsTombstoned()
		if priorAccepted {
			// Was accepted, now fails: this is a removal, exactly as AdmitPost would.
			if err := e.removeAccepted(ctx, did, communityDID, postATURI, commit, prior, code); err != nil {
				return nil, err
			}
			return &ReadmitResult{PostURI: postATURI, Status: StatusRemoved, DecisionCode: code}, nil
		}
		// Still rejected: refresh the ledger with the current cause (idempotent),
		// enqueue nothing.
		if err := e.admissions.Record(ctx, Admission{
			AuthorDID:         did,
			CommunityDID:      communityDID,
			PostURI:           postATURI,
			Status:            StatusRejected,
			DecisionCode:      code,
			EvaluatedCID:      commit.CID,
			EvaluatedSnapshot: evaluatedSnapshot(commit),
		}); err != nil {
			return nil, err
		}
		return &ReadmitResult{PostURI: postATURI, Status: StatusRejected, DecisionCode: code}, nil
	}

	// Passes now: write/repin the acceptance and enqueue the Page (reusing accept()).
	if err := e.accept(ctx, did, communityDID, postATURI, commit); err != nil {
		return nil, err
	}
	return &ReadmitResult{PostURI: postATURI, Status: StatusAccepted, Enqueued: true}, nil
}

// evaluatedSnapshot serializes the postv2 record and the context a Readmit needs
// to rebuild the CommitEvent it re-runs admission against. It is stored on EVERY
// decision (accept, reject, remove). A marshal failure yields nil, which the
// store coalesces to '{}' — an unrecoverable readmit, never a wrong one.
func evaluatedSnapshot(commit *consume.CommitEvent) []byte {
	b, err := json.Marshal(map[string]any{
		"record":     commit.Record,
		"cid":        commit.CID,
		"rev":        commit.Rev,
		"operation":  commit.Operation,
		"collection": commit.Collection,
	})
	if err != nil {
		return nil
	}
	return b
}

// rebuildCommit reconstructs the CommitEvent (and its author DID) a Readmit
// re-runs admission against, from the post at-uri and the stored evaluated
// snapshot. ok=false means the snapshot did not survive (legacy '{}' or a
// malformed at-uri): the caller surfaces that as ErrUnrecoverableReadmit.
func rebuildCommit(postURI string, snapshot []byte) (commit *consume.CommitEvent, authorDID string, ok bool) {
	trimmed := strings.TrimPrefix(postURI, "at://")
	parts := strings.SplitN(trimmed, "/", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return nil, "", false
	}
	did, collection, rkey := parts[0], parts[1], parts[2]

	if len(snapshot) == 0 {
		return nil, "", false
	}
	var snap struct {
		Record    map[string]any `json:"record"`
		CID       string         `json:"cid"`
		Rev       string         `json:"rev"`
		Operation string         `json:"operation"`
	}
	if err := json.Unmarshal(snapshot, &snap); err != nil {
		return nil, "", false
	}
	if len(snap.Record) == 0 {
		// '{}' — a decision recorded before the snapshot column existed.
		return nil, "", false
	}
	op := snap.Operation
	if op == "" {
		op = "create"
	}
	return &consume.CommitEvent{
		Rev:        snap.Rev,
		Operation:  op,
		Collection: collection,
		RKey:       rkey,
		CID:        snap.CID,
		Record:     snap.Record,
	}, did, true
}
