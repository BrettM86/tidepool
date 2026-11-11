package ingest

import (
	"context"
	"database/sql"
	"encoding/json"
	stderrors "errors"
	"expvar"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// Bridged community restore reconcile. While a community was removed
// Tidepool did not follow it, so whatever its Lemmy moderators and admins did
// meanwhile never arrived. The negation that clears the removal records the
// community's reconcile pending (store.MarkCommunityReconcilePendingTx). Once
// the community is ready (not removed; its follow not pending; an accepted
// follow's backfill stamped; a community not followed again is ready right
// away), a pass fetches every kept object (a live fediverse postv2 post or
// comment bound to the community) from its origin and applies the answer:
//
//   - live, under a standing removal record, the object on the community's
//     own instance (ap.SameAuthority with the group): restored (RestorePost,
//     RestoreBridgedComment), lifting its community-removal withholds;
//   - live, otherwise: nothing. An object on another instance is fetched from
//     that instance, which answers for its own deletions but not for the
//     community's removals, so its removal record stays;
//   - 404, or Lemmy's 400 {"error":"couldnt_find_post"} for a post
//     (couldnt_find_comment for a comment), its answer for a purged object:
//     deleted (HandleDeleteRecord);
//   - 410 or a Tombstone body: a moderator-discretion removal (RemovePost,
//     RemoveBridgedComment), unless a removal record already stands;
//   - anything else (a transport failure, including a dead instance; 5xx/429;
//     401/403; any other 400; an off-authority redirect): nothing, retried
//     by a later pass. The pinned-authority fetch never reports host-gone, so
//     a dead instance is retried like any other failure.
//
// A live or gone answer is applied only to the removal record it was fetched
// under: the pass reads the object's removal record (its CID, or none) before
// the fetch and again before applying, and a removal or restore that the
// community's moderators sent while the fetch was open (a different record,
// or one written or lifted) leaves the answer unapplied and the marker set,
// so a later pass fetches again. A Remove that rewrites a standing removal
// record byte for byte is not seen.
//
// A pass clears the pending marker, by compare-and-set on the value it read
// at its start, only when every kept object got a final outcome and no
// listing or apply failed. The first pass for a marker stamps its first
// attempt (store.StampCommunityReconcileFirstAttempt). Until DeadHostAfter
// has passed since then, a fetch retry keeps the marker; after it, a pass
// whose only non-final objects are fetch retries counts them unreachable
// (one Warn record naming their ap_ids), changes nothing for them, and
// clears the marker. A fetch failure never deletes or removes anything; a
// failed apply keeps the marker however old it is. Every apply is
// idempotent, so a pass cut short (a crash, a shutdown) is simply run again:
// processing is eventual, not exactly-once. Operator and label withholds are
// never touched:
// the restore and removal paths only write community-removal rows.
//
// Passes start from Backfill's OnComplete (TriggerAsync), right after the
// re-follow's clean walk, and from Run's sweeps, at start (restart resume)
// and every Interval (retries, and communities not followed again).

// OriginState is what a community reconcile pass learned about one kept
// object from its origin.
type OriginState int

const (
	OriginRetry   OriginState = iota // no final answer: transport, 5xx/429, 401/403, other 400, unparsable, or a 200 body that is not the requested object
	OriginLive                       // 200 with the requested id and a content type ingest accepts for the collection
	OriginGone404                    // 404, or Lemmy's 400 not-found code for the collection
	OriginGone410                    // 410, or a 200 Tombstone body
)

// defaultCommunityReconcileInterval is Run's sweep period when none is set.
const defaultCommunityReconcileInterval = 10 * time.Minute

// Community reconcile counters: each kept object a pass reaches adds to at
// most one of the outcome counters (a gone answer under a standing removal
// record adds to none), and each cleared marker adds one completed pass.
var (
	CommunityReconcileLive            = expvar.NewInt("tidepool_community_reconcile_live")
	CommunityReconcileRestored        = expvar.NewInt("tidepool_community_reconcile_restored")
	CommunityReconcileDeleted         = expvar.NewInt("tidepool_community_reconcile_deleted")
	CommunityReconcileRemoved         = expvar.NewInt("tidepool_community_reconcile_removed")
	CommunityReconcileRetry           = expvar.NewInt("tidepool_community_reconcile_retry")
	CommunityReconcileUnreachable     = expvar.NewInt("tidepool_community_reconcile_unreachable")
	CommunityReconcilePassesCompleted = expvar.NewInt("tidepool_community_reconcile_passes_completed")
)

// ClassifyOrigin maps the result of (*ap.Client).FetchObjectSameAuthority for
// the kept object apID of collection (materialize.CollectionPostV2 or
// materialize.CollectionComment) to an OriginState.
//
// A 200 counts as live only when the body's id is apID and its type is one
// ingest accepts for the collection (Page/Article for a post, Note for a
// comment). Any other 200 body ({}, a JSON error, another object) shows
// nothing about the requested object and is a retry: a live answer restores,
// so it is never taken from a body that is not the object. A Tombstone object
// handed in counts as gone only when its id is apID, but (*ap.Client) reports
// a 200 Tombstone body as errors.IsTombstoned without the body, so on that
// path, as for a 410, the id is not checked. That answer only writes a
// removal record, which a later live answer lifts.
//
// Lemmy answers a GET of a purged object 400 {"error":"couldnt_find_post"}
// for a post or {"error":"couldnt_find_comment"} for a comment: that exact
// code for the requested collection counts as a 404. Any other 400, including
// couldnt_find_person, couldnt_find_community or the other collection's code
// (Lemmy can return those while serializing an object that exists), is a
// retry.
func ClassifyOrigin(collection, apID string, obj *ap.Object, err error) OriginState {
	var refused ap.AccessRefusedError
	switch {
	case isLemmyNotFound(collection, err):
		return OriginGone404
	case err == nil && obj != nil && obj.ID != apID:
		return OriginRetry
	case err == nil && obj != nil && obj.IsTombstone():
		return OriginGone410
	case err == nil && obj != nil && restoredTypeMatches(collection, obj.Type):
		return OriginLive
	case err == nil:
		return OriginRetry
	case errors.IsTombstoned(err):
		return OriginGone410
	case stderrors.As(err, &refused):
		// A secure-mode refusal says nothing about whether the object exists.
		return OriginRetry
	case errors.IsNotFound(err):
		return OriginGone404
	default:
		return OriginRetry
	}
}

// isLemmyNotFound reports Lemmy's not-found answer to a GET of an object of
// collection: 400 with the JSON error couldnt_find_post for a post or
// couldnt_find_comment for a comment.
func isLemmyNotFound(collection string, err error) bool {
	var notFoundCode string
	switch collection {
	case materialize.CollectionPostV2:
		notFoundCode = "couldnt_find_post"
	case materialize.CollectionComment:
		notFoundCode = "couldnt_find_comment"
	default:
		return false
	}
	var httpErr ap.HTTPError
	if !stderrors.As(err, &httpErr) || httpErr.StatusCode != http.StatusBadRequest {
		return false
	}
	var lemmyError struct {
		Error string `json:"error"`
	}
	return json.Unmarshal([]byte(httpErr.Body), &lemmyError) == nil &&
		lemmyError.Error == notFoundCode
}

// OriginFetcher fetches a kept object from its origin.
type OriginFetcher interface {
	FetchObjectSameAuthority(ctx context.Context, iri string) (*ap.Object, error)
}

// CommunityReconcilerOptions wires a CommunityReconciler.
type CommunityReconcilerOptions struct {
	DB           *sql.DB
	Fetcher      OriginFetcher // production: *ap.Client
	Materializer *materialize.Materializer
	Interval     time.Duration // Run's sweep period
	Logger       *slog.Logger
}

// CommunityReconciler reconciles a restored community's kept objects with
// their origins.
type CommunityReconciler struct {
	db          *sql.DB
	fetcher     OriginFetcher
	mat         *materialize.Materializer
	communities store.Communities
	interval    time.Duration
	logger      *slog.Logger

	// baseCtx bounds TriggerAsync passes; Run cancels it when its own context
	// ends, so a shutdown stops them.
	baseCtx context.Context
	stop    context.CancelFunc

	mu sync.Mutex
	// running holds the DID of every community with a pass running.
	running map[string]bool
	wg      sync.WaitGroup
}

// NewCommunityReconciler builds a CommunityReconciler.
func NewCommunityReconciler(opts CommunityReconcilerOptions) (*CommunityReconciler, error) {
	if opts.DB == nil {
		return nil, errors.NewValidationError("db", "must not be nil")
	}
	if opts.Fetcher == nil {
		return nil, errors.NewValidationError("fetcher", "must not be nil")
	}
	if opts.Materializer == nil {
		return nil, errors.NewValidationError("materializer", "must not be nil")
	}
	interval := opts.Interval
	if interval <= 0 {
		interval = defaultCommunityReconcileInterval
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	baseCtx, stop := context.WithCancel(context.Background())
	return &CommunityReconciler{
		db:          opts.DB,
		fetcher:     opts.Fetcher,
		mat:         opts.Materializer,
		communities: store.NewCommunities(opts.DB),
		interval:    interval,
		logger:      logger,
		baseCtx:     baseCtx,
		stop:        stop,
		running:     map[string]bool{},
	}, nil
}

// Sweep runs one pass for every pending, ready community. It returns ctx.Err()
// when ctx is cancelled mid-pass; a pass that fails otherwise is logged and
// stays pending for the next sweep.
func (r *CommunityReconciler) Sweep(ctx context.Context) error {
	pending, err := store.ListReadyCommunityReconciles(ctx, r.db)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("ingest: community reconcile sweep: %w", err)
	}
	for _, community := range pending {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := r.claimedPass(ctx, community.DID); err != nil {
			return err
		}
	}
	return nil
}

// Run sweeps at start, then every Interval, until ctx ends. When it returns,
// running TriggerAsync passes are cancelled; Wait drains them.
func (r *CommunityReconciler) Run(ctx context.Context) {
	defer r.stop()
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		if err := r.Sweep(ctx); err != nil && ctx.Err() == nil {
			r.logger.Error("community reconcile sweep failed; the next sweep retries", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// TriggerAsync runs community's pass now, in the background, when its
// reconcile is pending and it is ready. A pass already running for the
// community is not doubled.
func (r *CommunityReconciler) TriggerAsync(community *store.Community) {
	if community == nil || community.DID == "" {
		return
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		if err := r.claimedPass(r.baseCtx, community.DID); err != nil && r.baseCtx.Err() == nil {
			r.logger.Error("community reconcile pass failed; the next sweep retries",
				"community", community.DID, "error", err)
		}
	}()
}

// Wait waits for TriggerAsync runs.
func (r *CommunityReconciler) Wait() { r.wg.Wait() }

// claimedPass runs did's pass unless one is already running. It re-reads the
// pending marker and readiness at the start, so the marker a pass clears is
// the one it read and a community removed again is not reconciled.
func (r *CommunityReconciler) claimedPass(ctx context.Context, did string) error {
	r.mu.Lock()
	if r.running[did] {
		r.mu.Unlock()
		return nil
	}
	r.running[did] = true
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.running, did)
		r.mu.Unlock()
	}()

	pending, err := store.ReadyCommunityReconcile(ctx, r.db, did)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		r.logger.Error("community reconcile: read pending reconcile", "community", did, "error", err)
		return nil
	}
	if pending == nil {
		return nil
	}
	return r.pass(ctx, *pending)
}

// reconcileCounts is one pass's outcome counts.
type reconcileCounts struct {
	live, restored, deleted, removed, retry, unreachable int
}

// pass reconciles the kept objects of one pending, ready community and logs
// its summary. It returns ctx.Err() when ctx ends mid-pass; any other failure
// leaves the pass incomplete, so the marker stays.
func (r *CommunityReconciler) pass(ctx context.Context, pending store.PendingCommunityReconcile) error {
	var counts reconcileCounts
	complete := false
	// fetchRetries are the ap ids of the kept objects whose origin gave no
	// answer this pass. They count as retries, or as unreachable when the
	// pass clears the marker past the dead-host window.
	var fetchRetries []string
	retriesUnreachable := false
	defer func() {
		retryOutcome := outcomeRetry
		if retriesUnreachable {
			retryOutcome = outcomeUnreachable
		}
		for range fetchRetries {
			counts.add(retryOutcome)
		}
		level := slog.LevelInfo
		if !complete {
			level = slog.LevelWarn
		}
		r.logger.Log(context.WithoutCancel(ctx), level, "community reconcile pass",
			"community", pending.DID, "complete", complete,
			"live", counts.live, "restored", counts.restored, "deleted", counts.deleted,
			"removed", counts.removed, "retry", counts.retry, "unreachable", counts.unreachable)
	}()

	firstAttemptAt, current, err := store.StampCommunityReconcileFirstAttempt(ctx, r.db, pending.DID, pending.PendingAt)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		r.logger.Error("community reconcile: stamp the first attempt", "community", pending.DID, "error", err)
		return nil
	}
	if !current {
		// A newer negation landed since the marker was read: its own pass runs next.
		return nil
	}

	objects, err := store.ListCommunityKeptObjects(ctx, r.db, pending.DID,
		[]string{materialize.CollectionPostV2, materialize.CollectionComment})
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		r.logger.Error("community reconcile: list kept objects", "community", pending.DID, "error", err)
		return nil
	}

	applyFailed := false
	for _, mapping := range objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		if stop, err := r.removedAgain(ctx, pending.DID); stop {
			return err
		}
		// The removal record the origin's answer is fetched under.
		removalBefore, err := r.mat.RemovalRecordCID(ctx, mapping)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.logger.Error("community reconcile: read removal before the fetch; the next pass retries",
				"community", pending.DID, "ap_id", mapping.APID, "error", err)
			applyFailed = true
			counts.add(outcomeRetry)
			continue
		}
		obj, fetchErr := r.fetcher.FetchObjectSameAuthority(ctx, mapping.APID)
		if err := ctx.Err(); err != nil {
			return err
		}
		// A removal can commit while the fetch is open: check again before
		// applying its answer.
		if stop, err := r.removedAgain(ctx, pending.DID); stop {
			return err
		}
		state := ClassifyOrigin(mapping.Collection, mapping.APID, obj, fetchErr)
		outcome, err := r.apply(ctx, mapping, pending.APGroupID, state, removalBefore)
		if stderrors.Is(err, errModeratedDuringFetch) {
			r.logger.Info("community reconcile: the object was moderated while its origin was fetched; the next pass fetches again",
				"community", pending.DID, "ap_id", mapping.APID)
			applyFailed = true
			counts.add(outcomeRetry)
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.logger.Error("community reconcile: apply the origin's answer; the next pass retries",
				"community", pending.DID, "ap_id", mapping.APID, "error", err)
			applyFailed = true
			counts.add(outcomeRetry)
			continue
		}
		if state == OriginRetry {
			args := []any{"community", pending.DID, "ap_id", mapping.APID, "first_attempt_at", firstAttemptAt, "error", fetchErr}
			if fetchErr == nil && obj != nil {
				// A 200 whose body is not the requested object: name what it was.
				args = append(args, "served_id", obj.ID, "served_type", obj.Type)
			}
			r.logger.Info("community reconcile: origin gave no answer", args...)
			fetchRetries = append(fetchRetries, mapping.APID)
			continue
		}
		counts.add(outcome)
	}
	if applyFailed {
		return nil
	}
	if len(fetchRetries) > 0 && time.Since(firstAttemptAt) < DeadHostAfter {
		return nil
	}

	cleared, err := store.ClearCommunityReconcile(ctx, r.db, pending.DID, pending.PendingAt)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		r.logger.Error("community reconcile: clear the pending reconcile", "community", pending.DID, "error", err)
		return nil
	}
	if !cleared {
		// A newer negation landed during the pass (its own pass runs next),
		// or the community was removed again.
		return nil
	}
	if len(fetchRetries) > 0 {
		retriesUnreachable = true
		r.logger.Warn("community reconcile: origins gave no answer for the dead-host window; counted unreachable, nothing changed for them",
			"community", pending.DID, "ap_ids", fetchRetries, "first_attempt_at", firstAttemptAt)
	}
	complete = true
	CommunityReconcilePassesCompleted.Add(1)
	return nil
}

// removedAgain reports whether the pass must stop because the community with
// did is removed again, or its removal could not be read (logged). The error
// is ctx.Err() when ctx ended, else nil.
func (r *CommunityReconciler) removedAgain(ctx context.Context, did string) (bool, error) {
	removed, err := communityRemoved(ctx, r.communities, did)
	if err != nil {
		if ctx.Err() != nil {
			return true, ctx.Err()
		}
		r.logger.Error("community reconcile: read community removal", "community", did, "error", err)
		return true, nil
	}
	if removed {
		r.logger.Info("community reconcile stopped: the community is removed again", "community", did)
	}
	return removed, nil
}

// reconcileOutcome is the counter bucket one object's answer lands in.
type reconcileOutcome int

const (
	outcomeNone reconcileOutcome = iota // a gone answer under a standing removal
	outcomeLive
	outcomeRestored
	outcomeDeleted
	outcomeRemoved
	outcomeRetry
	outcomeUnreachable // a fetch retry past the dead-host window, in a pass that clears the marker
)

func (c *reconcileCounts) add(outcome reconcileOutcome) {
	switch outcome {
	case outcomeLive:
		c.live++
		CommunityReconcileLive.Add(1)
	case outcomeRestored:
		c.restored++
		CommunityReconcileRestored.Add(1)
	case outcomeDeleted:
		c.deleted++
		CommunityReconcileDeleted.Add(1)
	case outcomeRemoved:
		c.removed++
		CommunityReconcileRemoved.Add(1)
	case outcomeRetry:
		c.retry++
		CommunityReconcileRetry.Add(1)
	case outcomeUnreachable:
		c.unreachable++
		CommunityReconcileUnreachable.Add(1)
	case outcomeNone:
	}
}

// removalUnchanged reads the object's removal record CID ("" for none) and
// returns it, or errModeratedDuringFetch when it is not removalBefore.
func (r *CommunityReconciler) removalUnchanged(ctx context.Context, mapping *store.APObjectMapping, removalBefore string) (string, error) {
	removal, err := r.mat.RemovalRecordCID(ctx, mapping)
	if err != nil {
		return "", fmt.Errorf("read removal of %s: %w", mapping.APID, err)
	}
	if removal != removalBefore {
		return "", errModeratedDuringFetch
	}
	return removal, nil
}

// errModeratedDuringFetch is apply's answer when the object's removal record
// changed while its origin was fetched: the answer is not applied.
var errModeratedDuringFetch = stderrors.New("the object's removal changed during the origin fetch")

// apply applies the origin's answer about one kept object of the community
// with groupID through the materializer's existing paths. removalBefore is the
// object's removal record CID ("" for none) read before the fetch.
func (r *CommunityReconciler) apply(ctx context.Context, mapping *store.APObjectMapping, groupID string, state OriginState, removalBefore string) (reconcileOutcome, error) {
	switch state {
	case OriginLive:
		return r.restore(ctx, mapping, groupID, removalBefore)
	case OriginGone404:
		if err := r.mat.HandleDeleteRecord(ctx, mapping.APID); err != nil {
			return outcomeRetry, fmt.Errorf("delete %s: %w", mapping.APID, err)
		}
		return outcomeDeleted, nil
	case OriginGone410:
		return r.remove(ctx, mapping, removalBefore)
	default:
		return outcomeRetry, nil
	}
}

// restore restores a live kept object under a standing removal record, when
// the object is on the instance of the community with groupID and the record
// is removalBefore, the one standing before the fetch. Another instance
// answers for its own deletions, not for the community's removals, so its live
// answer leaves a removal record standing.
func (r *CommunityReconciler) restore(ctx context.Context, mapping *store.APObjectMapping, groupID, removalBefore string) (reconcileOutcome, error) {
	if !ap.SameAuthority(mapping.APID, groupID) {
		return outcomeLive, nil
	}
	removal, err := r.removalUnchanged(ctx, mapping, removalBefore)
	if err != nil {
		return outcomeRetry, err
	}
	if removal == "" {
		return outcomeLive, nil
	}
	if mapping.Collection == materialize.CollectionComment {
		deleted, err := r.mat.RestoreBridgedComment(ctx, mapping)
		if err != nil {
			return outcomeRetry, fmt.Errorf("restore comment %s: %w", mapping.APID, err)
		}
		if !deleted {
			// The comment's record went before the restore committed: its
			// removal stays, as the restore path decides.
			return outcomeLive, nil
		}
		return outcomeRestored, nil
	}
	if err := r.mat.RestorePost(ctx, mapping); err != nil {
		return outcomeRetry, fmt.Errorf("restore post %s: %w", mapping.APID, err)
	}
	stillRemoved, err := r.mat.RemovalStands(ctx, mapping)
	if err != nil {
		return outcomeRetry, fmt.Errorf("read removal of %s: %w", mapping.APID, err)
	}
	if stillRemoved {
		// No current version to accept: the restore path leaves the removal.
		return outcomeLive, nil
	}
	return outcomeRestored, nil
}

// remove writes a moderator-discretion removal of a kept object its origin
// answers gone for, unless a removal record already stands, or one stood
// before the fetch (removalBefore) and has been lifted since.
func (r *CommunityReconciler) remove(ctx context.Context, mapping *store.APObjectMapping, removalBefore string) (reconcileOutcome, error) {
	removal, err := r.removalUnchanged(ctx, mapping, removalBefore)
	if err != nil {
		return outcomeRetry, err
	}
	if removal != "" {
		return outcomeNone, nil
	}
	if mapping.Collection == materialize.CollectionComment {
		err = r.mat.RemoveBridgedComment(ctx, mapping, materialize.RemovalCodeModeratorDiscretion, "")
	} else {
		err = r.mat.RemovePost(ctx, mapping, "")
	}
	if err != nil {
		return outcomeRetry, fmt.Errorf("remove %s: %w", mapping.APID, err)
	}
	return outcomeRemoved, nil
}
