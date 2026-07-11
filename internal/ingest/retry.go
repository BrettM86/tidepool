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

// Run sweeps for stale pending subscriptions until ctx is cancelled.
func (r *FollowRetrier) Run(ctx context.Context) {
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

// Sweep atomically claims every subscription pending past the threshold with
// attempt budget left and delivers a fresh Follow to each. Exported so tests
// (and operators via a future admin hook) can drive one sweep synchronously.
func (r *FollowRetrier) Sweep(ctx context.Context) {
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
	if err := r.client.SendActivity(ctx, inbox, follow); err != nil {
		giveUpOrRetry("deliver follow failed", "error", err)
		return
	}
	if final {
		logger.Error("follow retrier: final Follow sent; subscription still pending will require an operator to re-subscribe",
			"activity_id", follow.ID, "max_attempts", r.maxAttempts)
		return
	}
	logger.Info("follow re-sent for stale pending subscription", "activity_id", follow.ID)
}
