package ingest

import (
	"context"
	"log/slog"
	"time"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// Follow-retry defaults (task 11). The Lemmy first-contact Accept race:
// Lemmy initializes a newly-seen instance's federation cursor at the
// current max activity id, so the Accept answering the very FIRST Follow to
// a given Lemmy instance is usually skipped — the subscription sits in
// pending forever unless someone re-sends the Follow (fresh activity id →
// fresh Accept). The retrier automates what operators previously did by
// hand (and what the e2e harness does in subscribeCommunity).
const (
	// defaultFollowResendAfter is how long a subscription may sit pending
	// before its Follow is re-sent. Accepts normally arrive within seconds;
	// two minutes is far past any healthy exchange while still recovering
	// the race promptly.
	defaultFollowResendAfter = 2 * time.Minute
	// defaultFollowRetryInterval is the sweep cadence.
	defaultFollowRetryInterval = time.Minute
	// defaultFollowMaxAttempts bounds TOTAL Follow sends per subscription
	// (the admin subscribe's own send counts as the first). A community
	// that never answers five Follows is not racing — it is refusing,
	// unreachable, or misconfigured; the operator re-subscribing resets
	// the budget.
	defaultFollowMaxAttempts = 5
)

// FollowRetrierOptions configures NewFollowRetrier. Client, Communities,
// and Service are required; zero durations/counts take the defaults above.
type FollowRetrierOptions struct {
	Client      FollowClient
	Communities store.Communities
	Service     *ap.ServiceActor
	Logger      *slog.Logger

	// ResendAfter is the pending-age threshold; Interval the sweep cadence;
	// MaxAttempts the total-send budget. Tests shrink them.
	ResendAfter time.Duration
	Interval    time.Duration
	MaxAttempts int
}

// FollowRetrier re-sends the Follow for subscriptions stuck in pending.
type FollowRetrier struct {
	client      FollowClient
	communities store.Communities
	service     *ap.ServiceActor
	logger      *slog.Logger

	resendAfter time.Duration
	interval    time.Duration
	maxAttempts int
	now         func() time.Time
}

// NewFollowRetrier validates options and builds the retrier.
func NewFollowRetrier(opts FollowRetrierOptions) (*FollowRetrier, error) {
	if opts.Client == nil {
		return nil, errors.NewValidationError("client", "must not be nil")
	}
	if opts.Communities == nil {
		return nil, errors.NewValidationError("communities", "must not be nil")
	}
	if opts.Service == nil {
		return nil, errors.NewValidationError("service", "must not be nil")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	r := &FollowRetrier{
		client:      opts.Client,
		communities: opts.Communities,
		service:     opts.Service,
		logger:      logger,
		resendAfter: opts.ResendAfter,
		interval:    opts.Interval,
		maxAttempts: opts.MaxAttempts,
		now:         time.Now,
	}
	if r.resendAfter <= 0 {
		r.resendAfter = defaultFollowResendAfter
	}
	if r.interval <= 0 {
		r.interval = defaultFollowRetryInterval
	}
	if r.maxAttempts <= 0 {
		r.maxAttempts = defaultFollowMaxAttempts
	}
	return r, nil
}

// Run sweeps for stale pending subscriptions until ctx is cancelled. It sends
// the Undo{Follow}s community removals owe at its start, so one a send before
// a restart missed does not wait for the first interval.
func (r *FollowRetrier) Run(ctx context.Context) {
	r.sendOwedUnfollows(ctx)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.Sweep(ctx)
		}
	}
}

// Sweep sends the Undo{Follow}s community removals owe, then atomically claims
// every subscription pending past the threshold with attempt budget left and
// delivers a fresh Follow to each. The Undo{Follow}s go first, so none reaches
// a community after a Follow this sweep sends it. Exported so tests (and
// operators via a future admin hook) can drive one sweep synchronously.
func (r *FollowRetrier) Sweep(ctx context.Context) {
	r.sendOwedUnfollows(ctx)
	claimed, err := r.communities.ClaimStalePendingFollows(ctx, r.now().Add(-r.resendAfter), r.maxAttempts)
	if err != nil {
		r.logger.Error("follow retrier: claim stale pending subscriptions", "error", err)
		return
	}
	for _, community := range claimed {
		if ctx.Err() != nil {
			return
		}
		r.resend(ctx, community)
	}
}

// resend delivers a fresh Follow for a claimed subscription. The claim (a
// single row-locked UPDATE ... RETURNING in ClaimStalePendingFollows) has
// already consumed the attempt and re-stamped follow_requested_at, so resend
// itself never writes follow_state: an Accept that flipped this row to
// accepted between the claim and here is left as accepted (the claim never
// matched it, and even a redundant stale re-send is harmless — the bridge
// dedupes duplicate Follows). community.FollowAttempts carries the
// post-increment value, so a row at the budget is on its LAST send: no later
// sweep will ever claim it again, so exhaustion is logged loudly rather than
// leaving the misleading "will retry" on a subscription that is now stuck
// pending until an operator re-subscribes.
func (r *FollowRetrier) resend(ctx context.Context, community *store.Community) {
	logger := r.logger.With("community", community.APGroupID, "attempts", community.FollowAttempts)
	final := community.FollowAttempts >= r.maxAttempts

	// giveUpOrRetry logs a delivery failure loudly when the budget is spent
	// (this was the last claim this row will ever get) and as a routine
	// retry otherwise.
	giveUpOrRetry := func(what string, args ...any) {
		if final {
			args = append(args, "max_attempts", r.maxAttempts)
			logger.Error("follow retrier: giving up; subscription stuck pending, operator must re-subscribe: "+what, args...)
			return
		}
		logger.Warn("follow retrier: "+what+"; will retry after threshold", args...)
	}

	group, err := r.client.FetchActor(ctx, community.APGroupID)
	if err != nil {
		giveUpOrRetry("fetch group actor failed", "error", err)
		return
	}
	inbox := group.SharedInboxOrInbox()
	if inbox == "" {
		giveUpOrRetry("group advertises no inbox")
		return
	}
	follow, err := buildFollowActivity(r.service, community.APGroupID)
	if err != nil {
		logger.Error("follow retrier: build follow", "error", err)
		return
	}
	// The claim excluded removed communities, but a removal can commit
	// between the claim and the send.
	removed, err := communityRemoved(ctx, r.communities, community.DID)
	if err != nil {
		giveUpOrRetry("read community removal failed", "error", err)
		return
	}
	if removed {
		logger.Info("follow retrier: community removed since the claim; no Follow sent")
		return
	}
	if err := r.client.SendActivity(ctx, inbox, follow); err != nil {
		giveUpOrRetry("deliver follow failed", "error", err)
		return
	}
	// A removal that committed while the Follow was in flight may have sent
	// its Undo{Follow} first: take this Follow back. When the removal cannot
	// be read, or the take-back fails, an active removal owes the
	// Undo{Follow} (marking one owes nothing without an active removal), and
	// the next sweep sends it.
	removed, err = communityRemoved(ctx, r.communities, community.DID)
	if err != nil {
		logger.Warn("follow retrier: read community removal after the send failed; an active removal owes the Undo{Follow}",
			"activity_id", follow.ID, "error", err)
		r.markUnfollowOwed(ctx, logger, community.DID, follow.ID)
		return
	}
	if removed {
		if err := sendUndoFollowActivity(ctx, r.client, r.service, community.APGroupID); err != nil {
			logger.Warn("follow retrier: removed community's Follow not taken back; the next sweep sends the Undo{Follow}",
				"activity_id", follow.ID, "error", err)
			r.markUnfollowOwed(ctx, logger, community.DID, follow.ID)
			return
		}
		logger.Info("follow retrier: community removed during the send; Follow taken back", "activity_id", follow.ID)
		return
	}
	if final {
		logger.Error("follow retrier: final Follow sent; subscription still pending will require an operator to re-subscribe",
			"activity_id", follow.ID, "max_attempts", r.maxAttempts)
		return
	}
	logger.Info("follow re-sent for stale pending subscription", "activity_id", follow.ID)
}

// markUnfollowOwed records the Undo{Follow} taking back the Follow followID as
// owed by the community's active removal, if it has one.
func (r *FollowRetrier) markUnfollowOwed(ctx context.Context, logger *slog.Logger, did, followID string) {
	if err := markUnfollowOwed(ctx, r.communities, did); err != nil {
		logger.Error("follow retrier: Undo{Follow} not recorded as owed; a removal may not have taken this Follow back",
			"activity_id", followID, "error", err)
	}
}

// sendOwedUnfollows delivers every Undo{Follow} a community removal owes and
// completes each one delivered; a failed one stays owed for the next sweep. An
// owed Undo{Follow} of one Tidepool no longer has is cleared unsent. The
// follow state it checks is read before the send, so a community followed
// again while the Undo{Follow} is in flight may be unfollowed by it:
// completing it then re-pends the community with its Follow owed
// (CompleteOwedUnfollow), which the next sweep sends.
//
// An owed Undo{Follow} of a community followed again since its removal is not
// sent, as it would unfollow the new follow, but it is completed rather than
// cleared: an earlier send of it (the removal's own, or a sweep's whose
// completion failed) may have been delivered after the new Follow and undone
// it. Completing re-pends the community with its Follow owed; an extra Follow
// is harmless.
func (r *FollowRetrier) sendOwedUnfollows(ctx context.Context) {
	removals, ok := r.communities.(store.CommunityRemovals)
	if !ok {
		return
	}
	owed, err := removals.ListOwedUnfollows(ctx)
	if err != nil {
		r.logger.Error("follow retrier: list owed Undo{Follow}s", "error", err)
		return
	}
	for _, unfollow := range owed {
		if ctx.Err() != nil {
			return
		}
		logger := r.logger.With("community", unfollow.APGroupID, "removal_id", unfollow.RemovalID)
		if unfollow.APGroupID != "" && unfollow.FollowState == store.FollowStateNone {
			if err := sendUndoFollowActivity(ctx, r.client, r.service, unfollow.APGroupID); err != nil {
				logger.Warn("follow retrier: removed community's Undo{Follow} not delivered; will retry", "error", err)
				continue
			}
			refollowOwed, err := removals.CompleteOwedUnfollow(ctx, unfollow.RemovalID)
			if err != nil {
				logger.Error("follow retrier: unfollowed, but the owed Undo{Follow} not completed; the next sweep resends it, or owes a Follow if the community is followed again",
					"error", err)
				continue
			}
			if refollowOwed {
				logger.Info("follow retrier: Undo{Follow} delivered after the community was followed again; its Follow is owed")
				continue
			}
			logger.Info("follow retrier: removed community unfollowed")
			continue
		}
		if unfollow.APGroupID != "" {
			if _, err := removals.CompleteOwedUnfollow(ctx, unfollow.RemovalID); err != nil {
				logger.Error("follow retrier: complete owed Undo{Follow} of a community followed again", "error", err)
				continue
			}
			logger.Info("follow retrier: community followed again while its Undo{Follow} was owed; its Follow is owed")
			continue
		}
		if err := removals.ClearOwedUnfollow(ctx, unfollow.RemovalID); err != nil {
			logger.Error("follow retrier: clear owed Undo{Follow}", "error", err)
		}
	}
}
