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
	"fmt"
	"log/slog"
	"time"

	"tidepool/internal/acceptrec"
	"tidepool/internal/consume"
	"tidepool/internal/errors"
	"tidepool/internal/repo"
	"tidepool/internal/store"
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
)

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
	// UserOrigin is AP_USER_ORIGIN: the origin every deterministic activity id
	// is minted under.
	UserOrigin string
	// Logger receives drop reasons. Nil uses slog.Default().
	Logger *slog.Logger
}

// Engine admits native posts into bridged communities.
type Engine struct {
	repos       acceptrec.RepoManager
	enqueuer    consume.OutboundEnqueuer
	actors      consume.ActorMinter
	resolver    consume.DIDResolver
	communities store.Communities
	objects     store.OutboundObjects
	prefs       store.FederationPrefs
	admissions  *Admissions
	userOrigin  string
	logger      *slog.Logger
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
	return &Engine{
		repos:       opts.Repos,
		enqueuer:    opts.Enqueuer,
		actors:      opts.Actors,
		resolver:    opts.Resolver,
		communities: opts.Communities,
		objects:     opts.Objects,
		prefs:       opts.Prefs,
		admissions:  opts.Admissions,
		userOrigin:  opts.UserOrigin,
		logger:      logger,
	}, nil
}

// AdmitPost decides admission for one postv2 commit and, on admit, writes the
// community acceptance record while enqueueing the Page delivery atomically with
// it. create/update run admission on the event's content; delete takes the
// acceptance down and enqueues Delete{Page} from stored state.
func (e *Engine) AdmitPost(ctx context.Context, did string, commit *consume.CommitEvent) error {
	// This cycle handles create/update admission. A delete (author retraction)
	// takes the acceptance down and enqueues Delete{Page} from stored state;
	// that lifecycle lands next cycle. A delete carries no record body, so there
	// is nothing to admit or reject here yet.
	if commit.Operation == operationDelete {
		return nil
	}

	postURI := fmt.Sprintf("at://%s/%s/%s", did, commit.Collection, commit.RKey)
	communityDID, _ := commit.Record["community"].(string)
	if communityDID == "" {
		// The consumer already refuses a postv2 with no community, but the engine
		// re-asserts it: WE sign the acceptance, so a missing target is fail-closed.
		return errors.NewValidationError("community", "postv2 "+commit.RKey+" names no community")
	}

	// The opt-out check MOVED here from the consumer: an opted-out author's post
	// REACHES the engine and is RECORDED as a rejection with a distinct
	// machine-readable reason — no acceptance written, nothing enqueued. Content
	// authored while opted out never federates (decision 11).
	federating, err := e.mayFederate(ctx, did)
	if err != nil {
		return err
	}
	if !federating {
		e.logger.Debug("rejecting postv2 from an opted-out author",
			slog.String("did", did), slog.String("post", postURI))
		return e.admissions.Record(ctx, Admission{
			CommunityDID: communityDID,
			PostURI:      postURI,
			Status:       StatusRejected,
			DecisionCode: DecisionOptedOut,
			EvaluatedCID: commit.CID,
		})
	}

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
			CommunityDID:   communityDID,
			PostURI:        postURI,
			Status:         StatusAccepted,
			EvaluatedCID:   commit.CID,
			AcceptanceRKey: rkey,
			AcceptedCID:    commit.CID,
		})
	}

	if _, err := acceptrec.AcceptSubject(ctx, e.repos, communityDID, postURI, commit.CID,
		publishedAtOf(commit.Record), sideEffect); err != nil {
		return fmt.Errorf("accept: admit %s into %s: %w", postURI, communityDID, err)
	}
	return nil
}

// operationDelete is the Jetstream commit operation for a record deletion.
const operationDelete = "delete"

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
