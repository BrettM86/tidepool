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
	"unicode/utf8"

	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/bluesky-social/indigo/atproto/lexicon"

	"tidepool/internal/acceptrec"
	"tidepool/internal/consume"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
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
	// DecisionCommunityNotFollowed: the target community is not one Tidepool has
	// an ACCEPTED Follow to (follow_state none/pending). We are not its key holder
	// for federation purposes, so we must not sign an acceptance or deliver into
	// it. SECURITY: a communities row alone (existence) is not authority to bridge.
	DecisionCommunityNotFollowed = "community-not-followed"
	// DecisionModeratorRemoved: the author edited a post the COMMUNITY has
	// removed. The edit is not rejected — the author's record is theirs and it
	// stands — but it does not re-enter a community that removed it, and no
	// acceptance is written and nothing is enqueued.
	//
	// It is deliberately NOT RemovalCodeAdmissionRevoked: that code means "we
	// withdrew this and a corrective edit may restore it", which is precisely
	// the reasoning that must not reach a moderator's decision. The two must
	// stay distinguishable in the ledger, because they are the two branches of
	// what an edit against a standing removal is allowed to do.
	DecisionModeratorRemoved = "moderator-removed"
	// DecisionAuthorBanned: the community has BANNED this author (task 17c-3), so
	// nothing they write enters it until the ban is lifted or lapses.
	//
	// It is the same string the removal record's code uses — one decision, one
	// vocabulary — and it is taken from there rather than re-typed, because the
	// two surfaces answer the same operator question from opposite sides: the
	// ledger says why the post was refused, the removal record says why an older
	// one went. Two literals is how those drift apart.
	DecisionAuthorBanned = materialize.RemovalCodeAuthorBanned
)

// ErrModeratorRemovalStands reports that an edit was refused because the
// COMMUNITY has removed the post. It is a DECISION, not a failure: the ledger
// row is written, nothing is enqueued, and the live consume path treats it as
// handled. It exists so the operator surfaces cannot report the edit as
// accepted — Readmit maps it to a removed result, and without it the same call
// that records "removed / moderator-removed" answers 200 accepted/enqueued.
var ErrModeratorRemovalStands = stderrors.New("accept: a moderator removal stands")

// ErrAuthorBanned reports that the acceptance transaction found a ban the
// admission gate had not seen — the community banned this author between the
// two. Like the removal sentinel it is a DECISION rather than a failure: the
// transaction rolls back (so no acceptance, no outbound row, no delivery), the
// ledger records the rejection, and the live path treats the event as handled.
// Retrying would re-decide against a ban that is not going to move.
var ErrAuthorBanned = stderrors.New("accept: the author is banned from this community")

// errRemovalChanged reports that the removal an edit was deciding about is no
// longer the record it inspected — it vanished, or a different one replaced it.
// The decision is re-run against whatever stands now: an edit may only reverse
// the exact removal it read, and it may only record a terminal decision about
// one that is still there.
//
// It never escapes accept(): the retry loop consumes it.
var errRemovalChanged = stderrors.New("accept: the standing removal changed mid-decision")

// maxRemovalDecisionAttempts bounds that loop. Each pass is one repo read plus
// one commit attempt against a record a moderator is concurrently rewriting;
// three is generous for a human-paced race and cheap to spend, and running out
// surfaces as a retryable error rather than a guess.
const maxRemovalDecisionAttempts = 3

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
	// Bans reads whether a community has excluded the author (task 17c-3).
	//
	// OPTIONAL in the wiring sense only: when it is nil, NewEngine takes the ban
	// view of Communities, which the postgres communities store provides. It is
	// a separate option rather than methods on store.Communities because half
	// the bridge holds that interface to resolve follow state, and none of them
	// may reach an exclusion.
	Bans store.CommunityBans
	// APActors reads the author's AP actor row for the delivery-paused admission
	// check (decision 19). OPTIONAL: nil skips the paused check (the seam is not
	// wired yet — flagged for the paused-rejection lifecycle).
	APActors store.APActors
	// MaxPerAuthorPerCommunity caps accepted posts by one author in one community
	// within the ledger (the per-author-per-community flood guard,
	// ADMISSION_MAX_PER_AUTHOR_PER_COMMUNITY). 0 means UNLIMITED (the generous
	// default); a positive value is the cap the rate check enforces.
	MaxPerAuthorPerCommunity int
	// Now is the clock a moderation/removal record's createdAt is stamped from —
	// the DECISION time, not the post's publication time (a removal on an old post
	// is dated ~now). Injectable for tests. Nil uses time.Now.
	Now func() time.Time
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
	bans            store.CommunityBans
	apActors        store.APActors
	maxPerCommunity int
	catalog         *lexicon.BaseCatalog
	now             func() time.Time
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
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	// The vendored lexicon catalog validates native postv2 input strictly: WE
	// sign the acceptance, so input that does not validate is fail-closed. Loaded
	// once at construction — a broken vendored file fails startup, not admission.
	catalog, err := lexicons.Catalog()
	if err != nil {
		return nil, fmt.Errorf("accept: load lexicon catalog: %w", err)
	}
	// The ban store and the communities store are two repositories over two
	// tables; only the admission gate reads the first. The default keeps every
	// existing call site working — the postgres communities store IS also that
	// repository — without putting exclusions on an interface half the bridge
	// holds to resolve follow state.
	bans := opts.Bans
	if bans == nil {
		if fromCommunities, ok := opts.Communities.(store.CommunityBans); ok {
			bans = fromCommunities
		}
	}
	if bans == nil {
		// REQUIRED, like the echo classifier and for the same reason: this is a
		// gate, and a gate that is absent does not fail — it ADMITS. An engine
		// built with a Communities that is not ban-capable (a fake, a decorator,
		// a future backend) would accept every banned author's post into the
		// community that excluded them, silently, with no counter and no log,
		// and the deployment that did it would look identical to a correct one.
		// The write side already refuses loudly when it cannot record a ban; the
		// read side must not be the lenient half of the same feature.
		return nil, errors.NewValidationError("bans",
			"must not be nil: pass a store.CommunityBans, or a Communities that provides one")
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
		bans:            bans,
		apActors:        opts.APActors,
		maxPerCommunity: opts.MaxPerAuthorPerCommunity,
		catalog:         catalog,
		now:             now,
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
	// re-admission is a REMOVAL, not a fresh rejection). The engine writes
	// outbound_objects ONLY on accept, so a row here means the post federated.
	prior, priorBound, err := e.priorBinding(ctx, postURI)
	if err != nil {
		return err
	}
	// The community this post is already bound to, read from EITHER surviving
	// state — outbound_objects (accepted posts) OR the admissions ledger (a
	// rejected post has a ledger row but no outbound row). A post is bound to one
	// community forever; this is what makes community-immutability enforceable
	// even for a post that was only ever rejected.
	boundCommunity, err := e.boundCommunityOf(ctx, postURI, prior, priorBound)
	if err != nil {
		return err
	}

	// Decide admission. The order is deliberate (fail closed first, cheap policy
	// last): lexicon-validate → community-immutable → community-followed → opt-out
	// → paused → title → rate cap. A discard means the whole event is dropped
	// (nothing written to either community); a non-empty code is a rejection/
	// removal cause.
	code, discard, err := e.decide(ctx, did, commit, communityDID, boundCommunity)
	if err != nil {
		return err
	}
	if discard {
		// A community-moving edit: write NOTHING to the target community, and
		// annotate the ORIGINAL community's ledger row (status unchanged) so the
		// admin surface shows the attempted move instead of a silent drop.
		e.logger.Info("discarding community-moving edit",
			slog.String("did", did), slog.String("post", postURI),
			slog.String("bound_community", boundCommunity), slog.String("event_community", communityDID))
		return e.annotateCommunityImmutable(ctx, postURI)
	}

	if code != "" {
		// A post that WAS accepted and now fails re-admission is REMOVED (it
		// federated once, so leaving it alone would strand it live on Lemmy); one
		// that was never accepted is simply a recorded rejection.
		priorAccepted := priorBound && !prior.IsTombstoned()
		if priorAccepted && code == DecisionAuthorBanned {
			// EXCEPT for a ban, which is not a judgement of this post. Removing
			// here would do two things the ban itself deliberately did not:
			// strip content Lemmy KEPT (a ban without removeData leaves it
			// standing on their side, so we would be hiding a post they still
			// show), and ENQUEUE a Delete{Page} at the community that banned the
			// author — the outbound echo every other moderation path exists to
			// avoid. It would also record RemovalCodeAdmissionRevoked, whose
			// meaning is "a corrective edit may restore this", against a decision
			// documented to survive an unban: one cause, two removals, opposite
			// reversals, depending on which door the author knocked on.
			//
			// The ban already stopped everything new and cancelled everything
			// queued. The edit is simply refused, and the ledger says why.
			e.logger.Info("refusing a banned author's edit; the standing acceptance is left alone",
				slog.String("did", did), slog.String("post", postURI),
				slog.String("community", communityDID))
			return e.admissions.Record(ctx, Admission{
				AuthorDID:         did,
				CommunityDID:      communityDID,
				PostURI:           postURI,
				Status:            StatusRejected,
				DecisionCode:      code,
				EvaluatedCID:      commit.CID,
				EvaluatedSnapshot: e.evaluatedSnapshot(commit),
			})
		}
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
			EvaluatedSnapshot: e.evaluatedSnapshot(commit),
		})
	}

	if err := e.accept(ctx, did, communityDID, postURI, commit); err != nil {
		if stderrors.Is(err, ErrAuthorBanned) {
			// The ban landed between the gate and the commit. The transaction
			// rolled back, so nothing of this post exists outward; all that is
			// owed is the ledger row an operator reads when the moderators ask
			// why a banned author's post appeared — which it now will not.
			e.logger.Info("a ban landed mid-admission; the post was not accepted",
				slog.String("did", did), slog.String("post", postURI),
				slog.String("community", communityDID))
			return e.admissions.Record(ctx, Admission{
				AuthorDID:         did,
				CommunityDID:      communityDID,
				PostURI:           postURI,
				Status:            StatusRejected,
				DecisionCode:      DecisionAuthorBanned,
				EvaluatedCID:      commit.CID,
				EvaluatedSnapshot: e.evaluatedSnapshot(commit),
			})
		}
		if stderrors.Is(err, ErrModeratorRemovalStands) {
			// Decided and recorded inside accept(): the community removed this
			// post, so the edit does not re-enter it. Nothing is owed on the
			// live path — erroring here would redrive the commit forever
			// against a removal that is terminal by design.
			return nil
		}
		return err
	}
	return nil
}

// The AP op strings the deterministic activity id and the Page translation key
// on. operationDelete is also the Jetstream commit operation for a deletion; the
// others are DERIVED from outbound state, not read off the commit (see accept).
const (
	operationCreate = "create"
	operationUpdate = "update"
	operationDelete = "delete"
)

// decide runs the admission checks in order and returns the rejection/removal
// code ("" = admit), or discard=true when the event must be dropped whole (a
// community-moving edit). The order fails closed first: garbage input never
// reaches a policy check, and a hijack (community move) is refused before the
// author's own preferences are consulted.
func (e *Engine) decide(ctx context.Context, did string, commit *consume.CommitEvent,
	communityDID, boundCommunity string) (code string, discard bool, err error) {

	// 1. Strict lexicon validation of the native input, bound to the postv2
	// schema — fail closed. A marshal/unmarshal fault is an INTERNAL error
	// (retryable), NOT a permanent lexicon-invalid verdict.
	valid, err := e.lexiconValid(commit.Record)
	if err != nil {
		return "", false, err
	}
	if !valid {
		return DecisionLexiconInvalid, false, nil
	}

	// 2. Community immutability. The lexicon marks `community` immutable: an
	// UPDATE that names a different community than the post was bound to is a
	// retarget, which means writing a NEW post — so the whole event is discarded,
	// not partially applied.
	if boundCommunity != "" && boundCommunity != communityDID {
		return "", true, nil
	}

	// 3. Community follow gate (SECURITY): a communities row's mere existence is
	// NOT authority to sign an acceptance into it. We federate only communities
	// we hold an ACCEPTED Follow to; none/pending/unfollowed reject.
	community, err := e.communities.GetByDID(ctx, communityDID)
	if err != nil {
		if errors.IsNotFound(err) {
			return DecisionCommunityNotFollowed, false, nil
		}
		return "", false, fmt.Errorf("accept: resolve community %s: %w", communityDID, err)
	}
	if community.FollowState != store.FollowStateAccepted {
		return DecisionCommunityNotFollowed, false, nil
	}

	// 4. Community ban (task 17c-3): this community has excluded this author, so
	// nothing they write enters it. It sits here — after the community gate,
	// before the author's own preferences — because it is the community's
	// decision about its own space, and admitting the post would sign that
	// community's name to content from someone it has excluded.
	//
	// NewEngine refuses to build without a ban store, so this is never a
	// conditional check that quietly does not run.
	banned, err := e.bans.Standing(ctx, communityDID, did)
	if err != nil {
		return "", false, fmt.Errorf("accept: read ban on %s in %s: %w", did, communityDID, err)
	}
	if banned {
		return DecisionAuthorBanned, false, nil
	}

	// 5. Opt-out (decision 11): content pushed outward is exactly what an
	// opted-out author refused.
	federating, err := e.mayFederate(ctx, did)
	if err != nil {
		return "", false, err
	}
	if !federating {
		return DecisionOptedOut, false, nil
	}

	// 6. Paused (#account, decision 19): delivery is halted while the identity is
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

	// 7. Title: required and within Lemmy's cap (postv2 title is OPTIONAL in the
	// lexicon, so this is admission policy, not validation). The cap counts RUNES,
	// not bytes — Lemmy's limit is on grapheme length, so a multibyte title well
	// under 200 characters must not be rejected for being over 200 bytes.
	title, _ := commit.Record["title"].(string)
	if title == "" {
		return DecisionTitleRequired, false, nil
	}
	if utf8.RuneCountInString(title) > lemmyTitleCap {
		return DecisionTitleTooLong, false, nil
	}

	// 8. Rate cap: one author must not flood a community Tidepool vouches for.
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

	// The AP op is a function of what LEMMY already holds, not of the Jetstream
	// commit operation: Lemmy has a live copy only if a non-tombstoned outbound
	// row already exists. An UPDATE of a never-federated post is a Create{Page};
	// an edit after a Delete (removal / author-delete then restore) is a Create
	// too, because the live copy was withdrawn. This is read BEFORE the upsert
	// bumps the row.
	priorRow, priorErr := e.objects.GetByATURI(ctx, postURI)
	if priorErr != nil && !errors.IsNotFound(priorErr) {
		return fmt.Errorf("accept: read outbound state for %s: %w", postURI, priorErr)
	}
	wasLive := priorErr == nil && !priorRow.IsTombstoned()

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
		// THE BAN IS RE-ASKED HERE, inside the transaction that acts on the
		// answer. decide() read it before this transaction existed, and a ban is
		// not just a row: the transaction that writes one also CANCELS every
		// pending delivery the author has for this community. So a post that
		// passed the gate and then commits behind the ban lands an acceptance —
		// the community's own endorsement, in its own repo — plus a fresh
		// delivery the cancellation could never have caught, aimed at the
		// instance that just banned its author.
		//
		// It narrows the window rather than closing it: under READ COMMITTED a
		// ban committing after this read and before this commit is still
		// possible. What remains is microseconds wide and self-correcting on the
		// next edit, where the old shape was seconds wide and permanent.
		banned, err := e.bans.StandingTx(sctx, tx, communityDID, did)
		if err != nil {
			return fmt.Errorf("accept: re-read ban on %s in %s: %w", did, communityDID, err)
		}
		if banned {
			return fmt.Errorf("%w: %s in %s", ErrAuthorBanned, postURI, communityDID)
		}
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
		// Create unless Lemmy already holds a live copy AND this is a later
		// activity (seq bumped past the initial 0). The seq is guarded on CID
		// change (UpsertTx), so an unchanged redelivery keeps seq 0 and reuses the
		// original Create id rather than minting an Update to a peer.
		op := operationCreate
		if wasLive && stored.LastActivitySeq > 0 {
			op = operationUpdate
		}
		intent := consume.PostIntent{
			Op:            op,
			ATURI:         postURI,
			ID:            consume.ActivityID(e.userOrigin, postURI, op, stored.LastActivitySeq),
			CommunityAPID: community.APGroupID,
			// The mapping the enqueuer writes carries this binding; without it
			// no announced moderation of this post can ever be authorized.
			CommunityDID: communityDID,
			Snapshot:     snapshot,
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
			EvaluatedSnapshot: e.evaluatedSnapshot(commit),
		})
	}

	// The decision spans two operations — read the standing removal, then act on
	// it — and a moderator can write between them. Every such change re-runs the
	// whole decision against the state that is actually there; nothing is
	// decided from a record that has since moved.
	for attempt := 0; ; attempt++ {
		_, err = acceptrec.AcceptSubject(ctx, e.repos, communityDID, postURI, commit.CID,
			publishedAtOf(commit.Record), sideEffect)
		if stderrors.Is(err, acceptrec.ErrRemovalStands) {
			err = e.editAgainstRemoval(ctx, did, communityDID, postURI, commit, sideEffect)
			if stderrors.Is(err, errRemovalChanged) && attempt+1 < maxRemovalDecisionAttempts {
				continue
			}
			if stderrors.Is(err, errRemovalChanged) {
				return fmt.Errorf("accept: %s in %s: the standing removal kept changing across %d attempts",
					postURI, communityDID, maxRemovalDecisionAttempts)
			}
			return err
		}
		if err != nil {
			return fmt.Errorf("accept: admit %s into %s: %w", postURI, communityDID, err)
		}
		return nil
	}
}

// editAgainstRemoval decides what an edit may do when a removal already stands
// at the subject's rkey. WHOSE removal it is decides, and nothing else.
//
//   - admission-revoked is OUR OWN decision: a prior edit failed admission and
//     we withdrew the post. A corrective edit is exactly the event that should
//     reverse it, so it auto-restores — delete the removal, write a fresh
//     acceptance, enqueue (a Create, since Lemmy's live copy is gone), all on
//     the one restore commit.
//   - ANY OTHER CODE IS A MODERATOR'S DECISION AND IS TERMINAL. Reversing it
//     would delete the moderators' removal record, write an acceptance over it,
//     and — because the same side effect rides that commit — ENQUEUE the post
//     back to the community that removed it. Every other consequence of getting
//     this wrong is internal and correctable; that one is on the wire, at the
//     people who made the decision.
//
// A terminal removal is recorded in the ledger and nothing else happens: no
// commit, no enqueue, no error. The author's edit is not a failure — their
// record is theirs and it stands — it simply does not re-enter a community that
// has removed it, and the ledger is where an operator reads why.
func (e *Engine) editAgainstRemoval(ctx context.Context, did, communityDID, postURI string,
	commit *consume.CommitEvent, sideEffect repo.TxSideEffect) error {

	code, removalCID, err := e.standingRemoval(ctx, communityDID, postURI)
	if err != nil {
		return err
	}
	if code != RemovalCodeAdmissionRevoked {
		e.logger.Info("edit against a standing moderator removal: acceptance refused, nothing enqueued",
			"community_did", communityDID, "post", postURI, "removal_code", code)
		terminal := Admission{
			AuthorDID:         did,
			CommunityDID:      communityDID,
			PostURI:           postURI,
			Status:            StatusRemoved,
			DecisionCode:      DecisionModeratorRemoved,
			EvaluatedCID:      commit.CID,
			EvaluatedSnapshot: e.evaluatedSnapshot(commit),
		}
		// Re-verify before recording: the ledger is what an operator reads to
		// answer "why did my edit do nothing", and a moderator-removed row for a
		// removal that has since been lifted answers with a removal nobody can
		// find. The re-read is what turns "a removal stood when we looked" into
		// "one stands now"; if it has moved, the whole decision re-runs.
		current, currentCID, verr := e.standingRemoval(ctx, communityDID, postURI)
		if verr != nil {
			return verr
		}
		if currentCID != removalCID || current != code {
			return errRemovalChanged
		}
		if rerr := e.admissions.Record(ctx, terminal); rerr != nil {
			return rerr
		}
		// The decision is complete; the sentinel only tells the CALLER what was
		// decided. AdmitPost swallows it (nothing is owed on the live path);
		// Readmit reports it, so the admin surface stops claiming an acceptance
		// this very call refused to write.
		return ErrModeratorRemovalStands
	}
	// CAS on the removal we actually inspected: between the read above and this
	// commit a moderator may have replaced our admission-revoked removal with
	// their own, and deleting whichever removal happens to be current is the
	// reversal this whole branch exists to prevent — reachable again through a
	// smaller window. A mismatch commits nothing (the side effect included) and
	// re-runs the decision against their record.
	if _, rerr := acceptrec.Restore(ctx, e.repos, communityDID, postURI, commit.CID,
		removalCID, publishedAtOf(commit.Record), sideEffect); rerr != nil {
		if stderrors.Is(rerr, repo.ErrPreconditionFailed) {
			return errRemovalChanged
		}
		return fmt.Errorf("accept: restore %s into %s: %w", postURI, communityDID, rerr)
	}
	return nil
}

// standingRemoval reads the removal AcceptSubject refused against: its `code`,
// which decides whose decision it is, and its CID, which is the token every
// later step is checked against. The two ways this read can fail are different
// events and are reported differently:
//
//   - NOT FOUND is the benign race: the moderators restored the post between
//     the refusal and this read. errRemovalChanged says so and the decision
//     re-runs — the next AcceptSubject finds no removal and admits the edit.
//     Reporting it as "not ours, terminal" would strand a post whose removal no
//     longer exists.
//   - ANYTHING ELSE is an infrastructure failure (the repo store is down, a
//     timeout). It propagates as itself, so the retry budget is spent on a
//     message about a broken read rather than about a "standing removal" that
//     was never the problem.
//
// A record whose `code` is absent or not a string yields "", which the caller
// treats as a moderator's — terminal. That direction is deliberate: refusing an
// edit is recoverable, and pushing a removed post back at its community is not.
func (e *Engine) standingRemoval(ctx context.Context, communityDID, postURI string) (code, cid string, err error) {
	rkey := acceptrec.SubjectRKey(postURI)
	record, cid, err := e.repos.GetRecord(ctx, communityDID, acceptrec.CollectionRemoval, rkey)
	switch {
	case errors.IsNotFound(err):
		return "", "", fmt.Errorf("%w: %s in %s", errRemovalChanged, postURI, communityDID)
	case err != nil:
		return "", "", fmt.Errorf("accept: read standing removal %s/%s/%s: %w",
			communityDID, acceptrec.CollectionRemoval, rkey, err)
	}
	code, _ = record["code"].(string)
	return code, cid, nil
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
			EvaluatedSnapshot: e.evaluatedSnapshot(commit),
		})
	}

	// The removal pins the version that was accepted when it was removed (audit
	// metadata); the code is the open-set admission-revoked, not a moderation
	// reason — no moderator acted. createdAt is the DECISION time (the engine's
	// clock), NOT the post's publication time — a removal is a claim about when we
	// decided, so an old post removed today is dated today.
	if _, err := acceptrec.Remove(ctx, e.repos, communityDID, postURI, prior.LastCID,
		RemovalCodeAdmissionRevoked, "", e.now(), sideEffect); err != nil {
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

// lexiconValid reports whether the record is a valid social.coves.community
// .postv2, validated against THAT schema specifically. SECURITY: the type is
// bound to postv2, not trusted from the record's self-declared $type — WE sign
// the acceptance, so a profile- or comment-shaped record carrying a full postv2
// body must not be signed as a community post just because it is a valid instance
// of the type it claims.
//
// The bool is the schema VERDICT (false → a real lexicon-invalid rejection). The
// error is an INTERNAL fault (marshal/unmarshal) — retryable, and NOT a permanent
// lexicon-invalid decision, so the caller must not record a rejection for it.
func (e *Engine) lexiconValid(record map[string]any) (bool, error) {
	if t, _ := record["$type"].(string); t != materialize.CollectionPostV2 {
		e.logger.Debug("record rejected: $type is not a postv2",
			slog.String("type", t), slog.String("want", materialize.CollectionPostV2))
		return false, nil
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return false, fmt.Errorf("accept: marshal record for validation: %w", err)
	}
	data, err := atdata.UnmarshalJSON(raw)
	if err != nil {
		return false, fmt.Errorf("accept: decode record for validation: %w", err)
	}
	if verr := lexicon.ValidateRecord(e.catalog, data, materialize.CollectionPostV2, lexicon.ValidateFlags(0)); verr != nil {
		// A real schema verdict, logged with the offending field so an operator
		// can tell a genuine bad record from a validator disagreement.
		e.logger.Debug("record failed postv2 lexicon validation",
			slog.String("detail", verr.Error()))
		return false, nil
	}
	return true, nil
}

// boundCommunityOf returns the community a post is already bound to, from EITHER
// surviving state: the outbound_objects row (an accepted post) or, failing that,
// the admissions ledger (a rejected post keeps a ledger row but no outbound row).
// "" means the engine has never decided on this post. This is what makes
// one-community-per-post enforceable even for a post that was only ever rejected.
func (e *Engine) boundCommunityOf(ctx context.Context, postURI string, priorRow *store.OutboundObject, priorBound bool) (string, error) {
	if priorBound {
		return priorRow.CommunityDID, nil
	}
	adm, err := e.admissions.GetByPostURI(ctx, postURI)
	if errors.IsNotFound(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return adm.CommunityDID, nil
}

// annotateCommunityImmutable records the attempted community move on the post's
// EXISTING ledger row (status unchanged, decision_code = community-immutable) so
// the admin surface shows it, instead of a silent drop. It writes NOTHING to the
// target community — the row it updates is the one already keyed to the post's
// bound community.
func (e *Engine) annotateCommunityImmutable(ctx context.Context, postURI string) error {
	adm, err := e.admissions.GetByPostURI(ctx, postURI)
	if errors.IsNotFound(err) {
		return nil // nothing decided yet; nothing to annotate
	}
	if err != nil {
		return err
	}
	adm.DecisionCode = DecisionCommunityImmutable
	return e.admissions.Record(ctx, *adm)
}

// evaluatedSnapshot serializes the postv2 record and the context a Readmit needs
// to rebuild the CommitEvent it re-runs admission against. It is stored on EVERY
// decision (accept, reject, remove). A marshal failure is LOGGED (not silently
// masqueraded as a legacy row) and yields nil — the store coalesces that to '{}',
// which makes a later readmit surface as unrecoverable rather than re-run wrong.
func (e *Engine) evaluatedSnapshot(commit *consume.CommitEvent) []byte {
	b, err := json.Marshal(map[string]any{
		"record":     commit.Record,
		"cid":        commit.CID,
		"rev":        commit.Rev,
		"operation":  commit.Operation,
		"collection": commit.Collection,
	})
	if err != nil {
		e.logger.Warn("failed to marshal evaluated snapshot; readmit will be unrecoverable for this decision",
			slog.String("rkey", commit.RKey), slog.String("error", err.Error()))
		return nil
	}
	return b
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
	boundCommunity, err := e.boundCommunityOf(ctx, postATURI, prior, priorBound)
	if err != nil {
		return nil, err
	}
	code, discard, err := e.decide(ctx, did, commit, communityDID, boundCommunity)
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
			EvaluatedSnapshot: e.evaluatedSnapshot(commit),
		}); err != nil {
			return nil, err
		}
		return &ReadmitResult{PostURI: postATURI, Status: StatusRejected, DecisionCode: code}, nil
	}

	// Passes now: write/repin the acceptance and enqueue the Page (reusing accept()).
	if err := e.accept(ctx, did, communityDID, postATURI, commit); err != nil {
		if stderrors.Is(err, ErrModeratorRemovalStands) {
			// Admission passes, but the COMMUNITY removed this post: accept()
			// wrote the removed ledger row and refused the acceptance. Reporting
			// it as accepted/enqueued would have the same request answer 200
			// "accepted" while the row it just wrote says removed.
			return &ReadmitResult{PostURI: postATURI, Status: StatusRemoved,
				DecisionCode: DecisionModeratorRemoved}, nil
		}
		return nil, err
	}
	return &ReadmitResult{PostURI: postATURI, Status: StatusAccepted, Enqueued: true}, nil
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
