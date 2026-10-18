// Package handlemigration renames bridged actors from the old handle format
// into the handle zone.
package handlemigration

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/identity"
	"tidepool/internal/repo"
	"tidepool/internal/store"
)

const (
	maxReservationAttempts = 5
	finalizeTimeout        = 10 * time.Second
)

// Identity is the PLC side of a rename.
type Identity interface {
	HandleFor(ctx context.Context, actorType store.ActorType, preferredUsername, instance string) (string, error)
	UpdateHandle(ctx context.Context, did, newHandle string, beforeSubmit func(context.Context) error) (identity.HandleUpdateResult, error)
}

// ActorFetcher fetches an actor document through the guarded AP client. A
// redirect off the actor IRI's origin fails the fetch, so a third-party host
// cannot choose the username.
type ActorFetcher interface {
	FetchActorSameAuthority(ctx context.Context, iri string) (*ap.Object, error)
}

// Options configures a Runner. ActorsPerSecond paces PLC handle update
// submissions: each POST, retries included, is sent at least
// 1/ActorsPerSecond after the previous one in the same run.
type Options struct {
	Actors          store.BridgedActors
	Identity        Identity
	ActorFetcher    ActorFetcher
	BridgeHostname  string
	HandleZoneRoot  string
	Logger          *slog.Logger
	ActorsPerSecond float64
	Now             func() time.Time
	Wait            func(ctx context.Context, delay time.Duration) error
}

// Runner renames named actors one at a time.
type Runner struct {
	actors         store.BridgedActors
	identity       Identity
	actorFetcher   ActorFetcher
	bridgeHostname string
	handleZoneRoot string
	logger         *slog.Logger
	interval       time.Duration
	now            func() time.Time
	wait           func(context.Context, time.Duration) error
}

// NewRunner builds a Runner.
func NewRunner(opts Options) (*Runner, error) {
	switch {
	case opts.Actors == nil:
		return nil, errors.NewValidationError("actors", "must not be nil")
	case opts.Identity == nil:
		return nil, errors.NewValidationError("identity", "must not be nil")
	case opts.ActorFetcher == nil:
		return nil, errors.NewValidationError("actor_fetcher", "must not be nil")
	case opts.BridgeHostname == "":
		return nil, errors.NewValidationError("bridge_hostname", "must not be empty")
	case opts.HandleZoneRoot == "":
		return nil, errors.NewValidationError("handle_zone_root", "must not be empty")
	case opts.ActorsPerSecond <= 0 || math.IsNaN(opts.ActorsPerSecond) || math.IsInf(opts.ActorsPerSecond, 0):
		return nil, errors.NewValidationError("actors_per_second", "must be a positive finite number")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	wait := opts.Wait
	if wait == nil {
		wait = func(ctx context.Context, delay time.Duration) error {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return ctx.Err()
			}
		}
	}
	return &Runner{
		actors: opts.Actors, identity: opts.Identity, actorFetcher: opts.ActorFetcher,
		bridgeHostname: opts.BridgeHostname, handleZoneRoot: opts.HandleZoneRoot,
		logger: logger, interval: time.Duration(float64(time.Second) / opts.ActorsPerSecond),
		now: now, wait: wait,
	}, nil
}

// Run processes the named DIDs in order, or all forward candidates when none are named.
func (r *Runner) Run(ctx context.Context, dids []string) error {
	if len(dids) == 0 {
		var err error
		dids, err = r.actors.ListForwardRenameCandidates(ctx, r.bridgeHostname, r.handleZoneRoot)
		if err != nil {
			return fmt.Errorf("list forward rename candidates: %w", err)
		}
	}
	var lastSubmission time.Time
	for _, did := range dids {
		if err := ctx.Err(); err != nil {
			return err
		}
		candidate, err := r.actors.IsForwardRenameCandidate(ctx, did, r.bridgeHostname, r.handleZoneRoot)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.logger.Error("check rename candidate failed", "did", did, "error", err)
			continue
		}
		if !candidate {
			continue
		}
		actor, err := r.actors.GetByDID(ctx, did)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.logger.Error("read rename actor failed", "did", did, "error", err)
			continue
		}
		target := actor.PendingHandle
		if target == "" && actor.PreferredUsername == "" {
			document, fetchErr := r.actorFetcher.FetchActorSameAuthority(ctx, actor.APActorID)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Logs never carry document fields or bodies.
			reason := ""
			var loggedErr error
			switch {
			case fetchErr != nil:
				reason = actorFetchFailureReason(fetchErr)
				var validationErr errors.ValidationError
				// A not-an-actor error quotes the document's type, so it is not logged.
				if !stderrors.As(fetchErr, &validationErr) || validationErr.Field != "actor" {
					loggedErr = fetchErr
				}
			case document == nil:
				reason = "actor fetch returned no document"
			case !document.IsActor():
				reason = "actor document is not an actor"
			case document.ID != actor.APActorID:
				reason = "actor document id mismatch"
			case document.PreferredUsername == "":
				reason = "actor document has no preferredUsername"
			case strings.ContainsRune(document.PreferredUsername, 0):
				reason = "actor document has invalid preferredUsername"
			}
			username := ""
			if reason == "" {
				username = document.PreferredUsername
			} else if fetchErr != nil && originGaveNoAnswer(fetchErr) {
				if name, ok := usernameFromActorID(actor.APActorID, actor.Handle); ok {
					// The name comes from the actor URL, so neither is logged.
					r.logger.Warn("rename username taken from actor id", "did", did, "username_source", "actor_id", "reason", reason)
					username = name
					reason = ""
				}
			}
			if reason != "" {
				if loggedErr != nil {
					r.logger.Warn("rename username fetch failed", "did", did, "reason", reason, "error", loggedErr)
				} else {
					r.logger.Warn("rename username fetch failed", "did", did, "reason", reason)
				}
				r.recordError(ctx, did, store.HandleMigrationUsernameUnavailable, reason)
				continue
			}
			if err := r.actors.SetPreferredUsernameByDID(ctx, did, username); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				r.logger.Error("save rename preferred username failed", "did", did, "error", err)
				continue
			}
			actor.PreferredUsername = username
		}
		reserved := false
		for attempt := 0; attempt < maxReservationAttempts; attempt++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			if target == "" {
				actorURL, parseError := url.Parse(actor.APActorID)
				if parseError != nil || actorURL.Hostname() == "" {
					err = errors.NewValidationError("ap_actor_id", "must have a valid hostname")
					break
				}
				target, err = r.identity.HandleFor(ctx, actor.ActorType, actor.PreferredUsername, actorURL.Hostname())
				if err != nil {
					break
				}
			}
			err = r.actors.SetPendingHandle(ctx, did, target)
			if err == nil {
				reserved = true
				break
			}
			// An actor with a stored target never gets a second one.
			if !errors.IsAlreadyExists(err) || actor.PendingHandle != "" || actor.PreferredUsername == "" {
				break
			}
			// A handle became occupied after the availability check. Rebuild
			// against the committed claims and legacy actor handles.
			target = ""
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !reserved {
			if errors.IsValidation(err) {
				r.recordError(ctx, did, store.HandleMigrationHandleInvalid, err.Error())
				continue
			}
			r.logger.Error("reserve rename handle failed", "did", did, "error", err)
			continue
		}
		if err := r.processPLC(ctx, did, target, &lastSubmission, plcHooks{}, func(ctx context.Context) error {
			return r.finalize(ctx, did, target)
		}); err != nil {
			return err
		}
	}
	return nil
}

// plcHooks are the direction-specific steps of processPLC. The zero value
// suits a rename that finalizes pending_handle.
type plcHooks struct {
	// beforeSubmit runs after pacing, right before each POST. An error sends
	// no POST and moves on to the next actor.
	beforeSubmit func(context.Context) error
	// notControlled, when set, handles a PLC head Tidepool does not control
	// in place of recording plc_not_controlled.
	notControlled func(context.Context) error
}

// processPLC is common to both directions. finish runs only after PLC is
// confirmed at target.
func (r *Runner) processPLC(ctx context.Context, did, target string, lastSubmission *time.Time, hooks plcHooks, finish func(context.Context) error) error {
	// A PLC rate limit applies to every DID the run would send, so the run
	// pauses and retries this actor instead of moving on.
	var result identity.HandleUpdateResult
	for {
		// The PLC operation cannot be undone, so re-check the tombstone right
		// before it. FinalizeHandleRename refuses a tombstone that lands later.
		current, err := r.actors.GetByDID(ctx, did)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.logger.Error("re-read rename actor failed", "did", did, "error", err)
			return nil
		}
		if current.ConsentState == store.ConsentStateDeleted {
			r.logger.Warn("rename skipped: actor tombstoned before PLC update", "did", did)
			return nil
		}
		// The gate spaces PLC submissions, not actor starts, so a slow actor
		// adds no delay of its own and there is no catch-up burst.
		var pacingErr, beforeSubmitErr error
		submitted := false
		paceSubmission := func(ctx context.Context) error {
			if !lastSubmission.IsZero() {
				if remaining := r.interval - r.now().Sub(*lastSubmission); remaining > 0 {
					if err := r.wait(ctx, remaining); err != nil {
						pacingErr = fmt.Errorf("pace handle migration: %w", err)
						return pacingErr
					}
				}
			}
			if err := ctx.Err(); err != nil {
				pacingErr = fmt.Errorf("pace handle migration: %w", err)
				return pacingErr
			}
			if hooks.beforeSubmit != nil {
				if err := hooks.beforeSubmit(ctx); err != nil {
					beforeSubmitErr = err
					return err
				}
			}
			*lastSubmission = r.now()
			submitted = true
			return nil
		}
		result, err = r.identity.UpdateHandle(ctx, did, target, paceSubmission)
		if err != nil {
			if pacingErr != nil && !submitted {
				// The gate stopped the run before any POST for this actor was sent.
				return pacingErr
			}
			if beforeSubmitErr != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				r.logger.Error("PLC handle update not sent: pre-submit step failed", "did", did, "error", beforeSubmitErr)
				return nil
			}
			if ctx.Err() != nil {
				r.logger.Error("PLC result unknown: handle update cancelled", "did", did, "pending_handle", target)
				return ctx.Err()
			}
			var rateLimited *identity.PLCRateLimitedError
			if stderrors.As(err, &rateLimited) {
				if err := r.pauseForRateLimit(ctx, did, rateLimited); err != nil {
					return err
				}
				continue
			}
			if stderrors.Is(err, identity.ErrPLCTransient) {
				r.logger.Error("PLC handle update transient failure", "did", did, "error", err)
			} else {
				r.logger.Error("PLC handle update failed", "did", did, "error", err)
			}
			return nil
		}
		break
	}
	switch result.Outcome {
	case identity.HandleUpdateNotControlled:
		if hooks.notControlled != nil {
			if err := hooks.notControlled(ctx); err != nil {
				r.logger.Error("handle a PLC head Tidepool does not control failed", "did", did, "error", err)
			}
			return nil
		}
		r.recordError(ctx, did, store.HandleMigrationPLCNotControlled, "")
		return nil
	case identity.HandleUpdateRejected:
		r.recordError(ctx, did, store.HandleMigrationPLCRejected, result.Message)
		return nil
	case identity.HandleUpdateApplied, identity.HandleUpdateAlreadyApplied:
	default:
		r.logger.Error("PLC handle update returned unknown outcome", "did", did, "outcome", result.Outcome)
		return nil
	}
	if err := finish(ctx); err != nil {
		r.logger.Error("PLC accepted, finalize not committed", "did", did, "target_handle", target, "error", err)
	}
	return nil
}

// pauseForRateLimit waits out a PLC 429 before the same actor is retried.
// Cancellation stops the run with the actor left pending.
func (r *Runner) pauseForRateLimit(ctx context.Context, did string, rateLimited *identity.PLCRateLimitedError) error {
	retryAfter := rateLimited.Headers["Retry-After"]
	now := r.now()
	pause := rateLimitPause(retryAfter, now)
	r.logger.Warn("PLC rate limited: pausing handle migration", "did", did, "pause", pause.String(),
		"resume_at", now.Add(pause), "retry_after", retryAfter, "headers", rateLimited.Headers)
	if err := r.wait(ctx, pause); err != nil {
		return fmt.Errorf("pause handle migration for PLC rate limit: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("pause handle migration for PLC rate limit: %w", err)
	}
	return nil
}

// actorFetchFailureReason never persists remote-controlled error text, URLs,
// document fields, or response bodies.
func actorFetchFailureReason(err error) string {
	var httpErr ap.HTTPError
	var validationErr errors.ValidationError
	var networkErr net.Error
	switch {
	case stderrors.As(err, &httpErr):
		return fmt.Sprintf("actor fetch failed: HTTP %d", httpErr.StatusCode)
	case stderrors.Is(err, context.DeadlineExceeded):
		return "actor fetch timed out"
	case stderrors.As(err, &networkErr) && networkErr.Timeout():
		return "actor fetch timed out"
	case errors.IsTombstoned(err):
		return "actor document tombstoned"
	case errors.IsNotFound(err):
		// The AP client maps 401 and 403 (secure-mode refusal) to not found.
		return "actor document not found or access refused (HTTP 401/403/404)"
	case stderrors.As(err, &validationErr) && validationErr.Field == "actor":
		return "actor document is not an actor"
	case stderrors.As(err, &validationErr) && validationErr.Field == "url":
		return "actor fetch refused: invalid or disallowed URL"
	case stderrors.As(err, &validationErr) && validationErr.Field == "redirect":
		return "actor fetch refused: redirect left origin or downgraded"
	default:
		return "actor fetch failed"
	}
}

// originGaveNoAnswer reports whether a fetch failed without the origin
// answering about the actor: a timeout, transport error, body cut short,
// HTTP 429 or 5xx, or 401/403/404 (mapped to not found). Every other failure
// is an answer and returns false: a tombstone, a refused fetch, another 4xx,
// or a response that could not be decoded.
func originGaveNoAnswer(err error) bool {
	var httpErr ap.HTTPError
	var validationErr errors.ValidationError
	var networkErr net.Error
	switch {
	// This case must stay before the net.Error check: *url.Error satisfies
	// net.Error, and the egress guard's refusal arrives inside one.
	case errors.IsTombstoned(err), stderrors.As(err, &validationErr):
		return false
	case stderrors.As(err, &httpErr):
		return httpErr.StatusCode == http.StatusTooManyRequests || httpErr.StatusCode >= 500
	default:
		return errors.IsNotFound(err) || stderrors.Is(err, context.DeadlineExceeded) ||
			stderrors.Is(err, io.ErrUnexpectedEOF) || stderrors.As(err, &networkErr)
	}
}

func (r *Runner) recordError(ctx context.Context, did string, code store.HandleMigrationErrorCode, message string) {
	if err := r.actors.RecordHandleMigrationError(ctx, did, code, message); err != nil {
		r.logger.Error("record handle migration error failed", "did", did, "code", code, "message", message, "error", err)
	}
}

// finalize commits a PLC-accepted rename even if ctx is cancelled, so a
// restart cannot leave PLC ahead of the database.
func (r *Runner) finalize(ctx context.Context, did, target string) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalizeTimeout)
	defer cancel()
	_, err := r.actors.FinalizeHandleRename(ctx, did, target,
		func(ctx context.Context, tx *sql.Tx, renamed *store.BridgedActor) error {
			_, err := repo.AppendIdentityEventTx(ctx, tx, did, renamed.Handle)
			return err
		})
	return err
}

func (r *Runner) Rollback(ctx context.Context, dids []string) error {
	if len(dids) == 0 {
		dids = nil
	}
	candidates, err := r.actors.ListRollbackCandidates(ctx, r.bridgeHostname, r.handleZoneRoot, dids)
	if err != nil {
		return fmt.Errorf("list rollback candidates: %w", err)
	}
	bridge := "." + strings.TrimSuffix(strings.ToLower(strings.TrimSpace(r.bridgeHostname)), ".")
	zone := "." + strings.TrimSuffix(strings.ToLower(strings.TrimSpace(r.handleZoneRoot)), ".")
	isNew := func(handle string) bool { return strings.HasSuffix(strings.ToLower(handle), zone) }
	isOld := func(handle string) bool {
		handle = strings.ToLower(handle)
		return strings.HasSuffix(handle, bridge) && !strings.HasSuffix(handle, zone)
	}
	var lastSubmission time.Time
	for _, did := range candidates {
		if err := ctx.Err(); err != nil {
			return err
		}
		actor, err := r.actors.GetByDID(ctx, did)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.logger.Error("read rollback actor failed", "did", did, "error", err)
			continue
		}
		if actor.ConsentState == store.ConsentStateDeleted {
			continue
		}
		switch {
		case isOld(actor.PendingHandle):
			if err := r.processPLC(ctx, did, actor.PendingHandle, &lastSubmission, plcHooks{}, func(ctx context.Context) error {
				return r.finalize(ctx, did, actor.PendingHandle)
			}); err != nil {
				return err
			}
		case isOld(actor.Handle) && isNew(actor.PendingHandle):
			pending := actor.PendingHandle
			abandon := func(ctx context.Context) error { return r.abandon(ctx, did, pending) }
			hooks := plcHooks{
				// Recorded before the POST, so a revert that lands while this
				// attempt fails to abandon still gets its #identity later.
				beforeSubmit: func(ctx context.Context) error {
					return r.actors.MarkPendingRevertSubmitted(ctx, did, pending)
				},
				// Tidepool did not sign a head it does not control, whether
				// or not it lists N, and cannot revert it, so the database
				// stops claiming N.
				notControlled: abandon,
			}
			if err := r.processPLC(ctx, did, actor.Handle, &lastSubmission, hooks, abandon); err != nil {
				return err
			}
		case actor.PendingHandle == "" && isNew(actor.Handle) && isOld(actor.PreviousHandle):
			target := actor.PreviousHandle
			if err := r.actors.SetPendingHandle(ctx, did, target); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				switch {
				case errors.IsAlreadyExists(err):
					r.recordError(ctx, did, store.HandleMigrationHandleInvalid, "previous handle is held by another actor")
				case errors.IsValidation(err):
					r.recordError(ctx, did, store.HandleMigrationHandleInvalid, err.Error())
				default:
					r.logger.Error("reserve rollback handle failed", "did", did, "error", err)
				}
				continue
			}
			if err := r.processPLC(ctx, did, target, &lastSubmission, plcHooks{}, func(ctx context.Context) error {
				return r.finalize(ctx, did, target)
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// abandon clears a mid-forward pending handle even if ctx is cancelled, so a
// restart cannot leave PLC reverted while N still resolves. The store appends
// #identity only if a revert POST was recorded for this pending handle, so a
// PLC head that never listed N emits nothing.
func (r *Runner) abandon(ctx context.Context, did, pending string) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalizeTimeout)
	defer cancel()
	_, err := r.actors.AbandonPendingHandle(ctx, did, pending,
		func(ctx context.Context, tx *sql.Tx, actor *store.BridgedActor) error {
			_, err := repo.AppendIdentityEventTx(ctx, tx, did, actor.Handle)
			return err
		})
	return err
}
