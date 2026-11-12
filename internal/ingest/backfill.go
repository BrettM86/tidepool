package ingest

import (
	"context"
	stderrors "errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"tidepool/internal/ap"
	"tidepool/internal/echo"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// Backfill defaults.
const (
	// DefaultBackfillMaxPosts caps how many posts one backfill run
	// materializes (BACKFILL_MAX_POSTS).
	DefaultBackfillMaxPosts = 100
	// defaultBackfillMinInterval is how fresh last_backfill_at must be for
	// an un-forced trigger (a re-delivered Accept) to be skipped.
	defaultBackfillMinInterval = time.Hour
	// DeadHostAfter is how long a restored community's reconcile keeps
	// retrying objects whose origin fetch fails (community_reconcile.go).
	DeadHostAfter = 72 * time.Hour
	// maxRepliesPerPost bounds one post's replies-collection walk.
	maxRepliesPerPost = 200
	// maxConcurrentBackfills bounds how many TriggerAsync walks run at once.
	maxConcurrentBackfills = 4
	// firstBackfillRetryDelay is the backoff after a community's first
	// unfinished walk. It doubles with each further one.
	firstBackfillRetryDelay = time.Hour
	// maxBackfillRetryDelay caps the backoff.
	maxBackfillRetryDelay = 24 * time.Hour
)

// BackfillFetcher is the slice of *ap.Client backfill needs.
type BackfillFetcher interface {
	FetchActor(ctx context.Context, iri string) (*ap.Object, error)
	FetchObject(ctx context.Context, iri string) (*ap.Object, error)
	FetchCollection(ctx context.Context, iri string, visit func(*ap.Object) error) error
}

// HostProber checks whether a host still answers as a fediverse server
// (*ap.Client implements it): nil means it served its nodeinfo discovery
// document as JSON.
type HostProber interface {
	ProbeNodeInfo(ctx context.Context, host string) error
}

// CountSeeder imports a backfilled post's historical vote counts from its
// community host's public API (task 07's votes.LemmySeeder; AP alone cannot
// provide them — outboxes announce historical Likes only sparsely). The
// community IRI passed must be the community the post is stored under: the
// seeder trusts that host for the post's counts and does not verify the
// binding, so callers must. Optional and best-effort: a nil seeder or a
// seeding failure never affects the backfill outcome.
type CountSeeder interface {
	SeedPostCounts(ctx context.Context, postAPID, communityIRI string) error
}

// BackfillOptions configures NewBackfill. Fetcher, Materializer,
// Communities, Tombstones, Instances, Prober, Deferred and Echo are required.
type BackfillOptions struct {
	Fetcher      BackfillFetcher
	Materializer Materializer
	Communities  store.Communities
	Tombstones   store.Tombstones
	// Instances tracks per-host host-gone streaks: a host-gone failure is
	// recorded into its host's streak, and a successful fetch clears it.
	// Required.
	Instances store.HostGoneStreaks
	// Prober checks a host's nodeinfo before an object its host answers with
	// an HTML page is skipped. Required.
	Prober HostProber
	// Deferred holds the items skipped because their host was unreachable,
	// for RetryDeferred. Required.
	Deferred store.DeferredBackfillItems
	// Echo keeps the bridge's own federated content out of the walk. A
	// bridged community's outbox holds OUR posts the moment a native user
	// participates, and this path reaches the materializer directly — past
	// the dispatcher's guard, which never sees an outbox item. Required for
	// the same reason the dispatcher's is (task 17a).
	Echo EchoClassifier
	// Seeder, when set, seeds each backfilled post's vote aggregates from
	// the community host's public API (config SEED_COUNTS_FROM_API).
	Seeder CountSeeder
	// MaxPosts caps posts per run (default 100, config.BackfillMaxPosts).
	MaxPosts int
	// MinInterval is the freshness window for un-forced triggers
	// (default 1h).
	MinInterval time.Duration
	// BaseContext is the root context TriggerAsync derives async runs from.
	// Wiring the server/run context here lets a mid-run backfill observe
	// shutdown (stop pulling remote pages) instead of running on an
	// unstoppable context.Background(). Defaults to context.Background().
	BaseContext context.Context
	Logger      *slog.Logger
	// OnComplete is called after a clean, stamped walk.
	OnComplete func(community *store.Community)
}

// Backfill pages a community's outbox after a Follow is accepted (and on
// demand), materializing history newest→oldest. Rate limiting rides on the
// AP client's per-host limiter; runs are serialized per community and
// resumable: a partial run leaves last_backfill_at unset so the next
// trigger re-walks (deterministic rkeys make re-materialization free).
// Its mints wait on the PLC write budget instead of being refused.
type Backfill struct {
	fetcher     BackfillFetcher
	mat         Materializer
	communities store.Communities
	tombstones  store.Tombstones
	instances   store.HostGoneStreaks
	prober      HostProber
	deferred    store.DeferredBackfillItems
	classifier  EchoClassifier
	seeder      CountSeeder
	maxPosts    int
	minInterval time.Duration
	baseCtx     context.Context
	logger      *slog.Logger
	now         func() time.Time
	onComplete  func(community *store.Community)

	// slots holds one entry per walk TriggerAsync has running.
	slots chan struct{}

	mu sync.Mutex
	// running holds every claimed community, queued for a slot or walking.
	running map[string]bool
	// retries holds the backoff of each community whose last walk left
	// last_backfill_at unset. It lives in memory, so a restart re-walks once.
	retries map[string]backfillRetry
	// wg tracks async runs so tests (and shutdown) can drain them.
	wg sync.WaitGroup
}

// backfillRetry is one community's retry schedule.
type backfillRetry struct {
	// failures counts consecutive unfinished walks.
	failures  int
	nextRetry time.Time
}

// NewBackfill validates options and builds a Backfill.
func NewBackfill(opts BackfillOptions) (*Backfill, error) {
	if opts.Fetcher == nil {
		return nil, errors.NewValidationError("fetcher", "must not be nil")
	}
	if opts.Materializer == nil {
		return nil, errors.NewValidationError("materializer", "must not be nil")
	}
	if opts.Communities == nil {
		return nil, errors.NewValidationError("communities", "must not be nil")
	}
	if opts.Tombstones == nil {
		return nil, errors.NewValidationError("tombstones", "must not be nil")
	}
	if opts.Instances == nil {
		return nil, errors.NewValidationError("instances", "must not be nil")
	}
	if opts.Prober == nil {
		return nil, errors.NewValidationError("prober", "must not be nil")
	}
	if opts.Deferred == nil {
		return nil, errors.NewValidationError("deferred", "must not be nil")
	}
	if opts.Echo == nil {
		return nil, errors.NewValidationError("echo", "must not be nil")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	baseCtx := opts.BaseContext
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	b := &Backfill{
		fetcher:     opts.Fetcher,
		mat:         opts.Materializer,
		communities: opts.Communities,
		tombstones:  opts.Tombstones,
		instances:   opts.Instances,
		prober:      opts.Prober,
		deferred:    opts.Deferred,
		classifier:  opts.Echo,
		seeder:      opts.Seeder,
		maxPosts:    opts.MaxPosts,
		minInterval: opts.MinInterval,
		baseCtx:     baseCtx,
		logger:      logger,
		now:         time.Now,
		onComplete:  opts.OnComplete,
		slots:       make(chan struct{}, maxConcurrentBackfills),
		running:     map[string]bool{},
		retries:     map[string]backfillRetry{},
	}
	if b.maxPosts <= 0 {
		b.maxPosts = DefaultBackfillMaxPosts
	}
	if b.minInterval <= 0 {
		b.minInterval = defaultBackfillMinInterval
	}
	return b, nil
}

// TriggerAsync queues a backfill walk in the background (the follow-accept
// hook and the admin endpoint) and never blocks. At most
// maxConcurrentBackfills walks run at once. A trigger for a community already
// queued or walking is dropped here, and shutdown empties the queue. A slot can
// take hours to come free, so an un-forced walk reads its community again once
// it has one (see currentCommunity). A forced walk applies no follow-state
// check, but no walk of a removed community starts (see removed).
func (b *Backfill) TriggerAsync(community *store.Community, force bool) {
	if b.claim(community, force, false) {
		b.startClaimed(community, force)
	}
}

// RetryUnfinished triggers a walk of a community whose last_backfill_at is
// unset, once its backoff has lapsed. A community with no retry state is due,
// so a restart re-walks it once. It never blocks. Accept and admin triggers do
// not consult the backoff, but every walk they run updates it.
func (b *Backfill) RetryUnfinished(community *store.Community) {
	if b.claim(community, false, true) {
		b.startClaimed(community, false)
	}
}

// UnfinishedBacklog counts the given communities whose last_backfill_at is
// unset and that are not inside a backoff. One whose backoff has lapsed counts:
// its retry is due.
func (b *Backfill) UnfinishedBacklog(communities []*store.Community) int {
	now := b.now()
	b.mu.Lock()
	defer b.mu.Unlock()
	unfinished := 0
	for _, community := range communities {
		if community.LastBackfillAt != nil {
			continue
		}
		if schedule, scheduled := b.retries[community.APGroupID]; scheduled && now.Before(schedule.nextRetry) {
			continue
		}
		unfinished++
	}
	return unfinished
}

// startClaimed queues a walk of a community the caller has claimed.
func (b *Backfill) startClaimed(community *store.Community, force bool) {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		defer b.release(community.APGroupID)
		select {
		case b.slots <- struct{}{}:
			defer func() { <-b.slots }()
		case <-b.baseCtx.Done():
		}
		// Checked after the select as well as by it: when a slot and the
		// cancellation are both ready the select picks either one.
		if err := b.baseCtx.Err(); err != nil {
			b.logger.Info("backfill dropped from the queue", "community", community.APGroupID, "error", err)
			return
		}
		if b.removed(b.baseCtx, community) {
			return
		}
		walked := community
		if !force {
			if walked = b.currentCommunity(community.APGroupID); walked == nil {
				return
			}
		}
		if err := b.walkClaimed(b.baseCtx, walked); err != nil {
			b.logger.Error("backfill failed", "community", community.APGroupID, "error", err)
		}
	}()
}

// currentCommunity reads a queued un-forced walk's community again. It is nil
// when the walk must not start: the community was unsubscribed or backfilled
// while queued, which leaves the retry state alone, or the read failed, which
// counts as an unfinished walk so the community stays on the schedule.
func (b *Backfill) currentCommunity(apGroupID string) *store.Community {
	community, err := b.communities.GetByAPGroupID(b.baseCtx, apGroupID)
	switch {
	case errors.IsNotFound(err):
		b.logger.Info("backfill dropped: community is gone", "community", apGroupID)
		return nil
	case err != nil:
		b.logger.Error("backfill dropped: read community", "community", apGroupID, "error", err)
		b.recordWalk(apGroupID, false)
		return nil
	case community.FollowState != store.FollowStateAccepted:
		b.logger.Info("backfill dropped: community is no longer followed",
			"community", apGroupID, "follow_state", community.FollowState)
		return nil
	case b.recentlyBackfilled(community):
		b.logger.Info("backfill dropped: recently completed",
			"community", apGroupID, "last_backfill_at", community.LastBackfillAt)
		return nil
	}
	return community
}

// Wait blocks until every async run finishes (shutdown/tests).
func (b *Backfill) Wait() { b.wg.Wait() }

// Run performs one backfill synchronously. force bypasses the
// last-backfill freshness check. Runs for the same community are
// serialized; a concurrent duplicate returns immediately.
func (b *Backfill) Run(ctx context.Context, community *store.Community, force bool) error {
	if !b.claim(community, force, false) {
		return nil
	}
	defer b.release(community.APGroupID)
	if b.removed(ctx, community) {
		return nil
	}
	return b.walkClaimed(ctx, community)
}

// removed reports that a claimed walk must not start because a Coves
// moderator removed its community. It is read when the walk starts, forced or
// not, so a walk queued before the removal is dropped too; walk reads it again
// before each item, for a removal that lands mid-walk. A failed read drops
// the walk as an unfinished one, so the community stays on the schedule.
func (b *Backfill) removed(ctx context.Context, community *store.Community) bool {
	removed, err := communityRemoved(ctx, b.communities, community.DID)
	switch {
	case err != nil:
		b.logger.Error("backfill dropped: read community removal", "community", community.APGroupID, "error", err)
		b.recordWalk(community.APGroupID, false)
		return true
	case removed:
		b.logger.Info("backfill dropped: community is removed", "community", community.APGroupID)
		return true
	}
	return false
}

// claim reserves community for one walk. It is false when the community was
// backfilled inside the freshness window (unless force), is already claimed, or
// is a retry inside its backoff; none of these touches the retry state. The
// backoff is read under the lock that takes the claim, so a retry cannot act on
// the state of a walk that has since ended.
func (b *Backfill) claim(community *store.Community, force, retry bool) bool {
	if !force && b.recentlyBackfilled(community) {
		b.logger.Info("backfill skipped: recently completed",
			"community", community.APGroupID, "last_backfill_at", community.LastBackfillAt)
		return false
	}

	now := b.now()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.running[community.APGroupID] {
		b.logger.Info("backfill already running", "community", community.APGroupID)
		return false
	}
	if schedule, scheduled := b.retries[community.APGroupID]; retry && scheduled && now.Before(schedule.nextRetry) {
		return false
	}
	b.running[community.APGroupID] = true
	return true
}

// recentlyBackfilled reports whether community is inside the freshness window.
func (b *Backfill) recentlyBackfilled(community *store.Community) bool {
	return community.LastBackfillAt != nil && time.Since(*community.LastBackfillAt) < b.minInterval
}

// release ends a claim.
func (b *Backfill) release(apGroupID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.running, apGroupID)
}

// walkClaimed walks a community the caller has claimed, with its mints waiting
// on the budget, and updates the retry state. The claim outlives the update, so
// no trigger can read the state of the walk before this one.
func (b *Backfill) walkClaimed(ctx context.Context, community *store.Community) error {
	stamped, err := b.walk(withBudgetWait(ctx), community)
	b.recordWalk(community.APGroupID, stamped)
	return err
}

// recordWalk updates the retry state after a walk. A stamped community is
// finished; any other walk backs off further, measured from the walk's end.
func (b *Backfill) recordWalk(apGroupID string, stamped bool) {
	end := b.now()
	b.mu.Lock()
	defer b.mu.Unlock()
	if stamped {
		delete(b.retries, apGroupID)
		return
	}
	retry := b.retries[apGroupID]
	retry.failures++
	retry.nextRetry = end.Add(backfillRetryDelay(retry.failures))
	b.retries[apGroupID] = retry
}

// backfillRetryDelay is the backoff after the given count of consecutive
// unfinished walks.
func backfillRetryDelay(failures int) time.Duration {
	delay := firstBackfillRetryDelay
	for range failures - 1 {
		delay *= 2
		if delay >= maxBackfillRetryDelay {
			return maxBackfillRetryDelay
		}
	}
	return delay
}

// walk pages the outbox and materializes it. stamped reports that
// last_backfill_at was recorded, which is the only clean completion: a failed
// or truncated walk, a group with no outbox and a failed stamp all leave it
// false. The walk's ctx carries the AP client's unreachable-host cache and the
// walk's host verdicts and probe answers, shared by the materializer's fetches
// through the same ctx.
func (b *Backfill) walk(ctx context.Context, community *store.Community) (stamped bool, err error) {
	ctx = withWalkHosts(ap.WithUnreachableHostCache(ctx))
	group, err := b.fetcher.FetchActor(ctx, community.APGroupID)
	if err != nil {
		return false, fmt.Errorf("ingest: backfill fetch group %s: %w", community.APGroupID, err)
	}
	if group.Outbox == "" {
		b.logger.Warn("backfill: group advertises no outbox", "community", community.APGroupID)
		return false, nil
	}

	// Collect first, materialize second: outboxes are newest-first, and the
	// spec wants newest→oldest materialization up to the cap. Lemmy
	// outboxes are a single OrderedCollection page; Mastodon-style paged
	// collections walk first/next.
	var items []*ap.Object
	walkTruncated := false
	err = b.fetcher.FetchCollection(ctx, group.Outbox, func(item *ap.Object) error {
		if len(items) >= b.maxPosts {
			return ap.ErrStop
		}
		clone := *item
		items = append(items, &clone)
		return nil
	})
	if err != nil {
		var truncated *ap.CollectionTruncatedError
		if stderrors.As(err, &truncated) {
			// The walk hit the page cap or a paging loop before the natural
			// end: everything collected is still materialized, but the run is
			// NOT a clean completion. We leave last_backfill_at unset (like the
			// failures>0 branch) so an un-forced re-trigger actually re-walks
			// rather than skipping inside the freshness window.
			walkTruncated = true
			b.logger.Warn("backfill: outbox walk truncated; older history not reached",
				"community", community.APGroupID, "pages", truncated.Pages, "next", truncated.Next)
		} else {
			return false, fmt.Errorf("ingest: backfill outbox %s: %w", group.Outbox, err)
		}
	}

	posts, failures, skipped, hostGoneSkipped := 0, 0, 0, 0
	for _, item := range items {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		// A removal that lands while the walk runs stops it unstamped, so
		// no further record of the community is materialized.
		removed, err := communityRemoved(ctx, b.communities, community.DID)
		if err != nil {
			return false, fmt.Errorf("ingest: backfill %s: read community removal: %w", community.APGroupID, err)
		}
		if removed {
			b.logger.Info("backfill stopped: community is removed",
				"community", community.APGroupID, "posts", posts, "skipped", skipped)
			return false, nil
		}
		ok, replyHostGone, err := b.materializeOutboxItem(ctx, item, community.APGroupID, community.DID)
		hostGoneSkipped += replyHostGone
		// Classified once: recordHostGone records the failure into the
		// host's streak, so it must run exactly once per failure.
		hostGone := errors.IsHostGone(err)
		deferrable := hostGone && b.recordHostGone(ctx, err)
		switch {
		case err == nil:
			if ok {
				posts++
			}
		case deferrable:
			// An unreachable origin host may only be lapsed, so backfill
			// skips the item and defers it for RetryDeferred: one
			// unreachable author host cannot keep the community from
			// completing. No live caller acts on host-gone. An item whose
			// deferral cannot be written is a failure, so the walk does not
			// stamp over it.
			if deferErr := b.deferDeadHostItem(ctx, outboxObject(item), community.APGroupID, community.DID, err); deferErr != nil {
				failures++
				b.logger.Warn("backfill item failed", "community", community.APGroupID, "item", item.ID, "error", deferErr)
				break
			}
			skipped++
			hostGoneSkipped++
			b.logger.Warn("backfill item skipped: origin host is gone",
				"community", community.APGroupID, "item", item.ID, "error", err)
		case !hostGone && materialize.IsSkip(err):
			skipped++
			b.logger.Info("backfill item skipped", "community", community.APGroupID, "reason", err.Error())
		default:
			failures++
			b.logger.Warn("backfill item failed", "community", community.APGroupID, "error", err)
		}
	}

	if failures > 0 {
		// Leave last_backfill_at untouched so the next trigger retries the
		// walk (resumable-by-redo; deterministic rkeys make it idempotent).
		return false, fmt.Errorf("ingest: backfill %s: %d of %d items failed (skipped %d, host_gone %d)",
			community.APGroupID, failures, len(items), skipped, hostGoneSkipped)
	}
	if walkTruncated {
		// Truncated walks are not clean completions: leave last_backfill_at
		// unset so an un-forced re-trigger re-walks from the top instead of
		// skipping inside the freshness window. Collected items already
		// materialized above; deterministic rkeys make the redo idempotent.
		b.logger.Info("backfill partial: truncated walk left resumable",
			"community", community.APGroupID, "posts", posts, "skipped", skipped, "host_gone", hostGoneSkipped)
		return false, nil
	}
	if err := b.communities.SetLastBackfill(ctx, community.APGroupID, time.Now()); err != nil {
		return false, fmt.Errorf("ingest: record backfill for %s: %w", community.APGroupID, err)
	}
	completeLevel := slog.LevelInfo
	if hostGoneSkipped > 0 {
		completeLevel = slog.LevelWarn
	}
	b.logger.Log(ctx, completeLevel, "backfill complete", "community", community.APGroupID,
		"posts", posts, "skipped", skipped, "host_gone", hostGoneSkipped)
	if b.onComplete != nil {
		b.onComplete(community)
	}
	return true, nil
}

// materializeOutboxItem unwraps one outbox entry (Announce{Create{Page}},
// Create{Page}, or a bare Page) and materializes the post plus its
// advertised replies. ok reports whether a post actually landed;
// replyHostGone counts the replies dropped because their host is gone.
// communityIRI and communityDID identify the community being backfilled.
func (b *Backfill) materializeOutboxItem(ctx context.Context, item *ap.Object, communityIRI, communityDID string) (ok bool, replyHostGone int, err error) {
	obj := outboxObject(item)
	if obj == nil {
		return false, 0, skip(item.ID, "outbox item carries no object")
	}
	if obj.ID == "" {
		return false, 0, skip(item.ID, "outbox item object has no id")
	}

	// Our own content, walked past. It runs on the UNRESOLVED node, before
	// resolveEmbedded: our object id is cross-authority with the outbox host,
	// so resolving would dereference our own origin to fetch back a record we
	// already hold — and MaterializePost mints the actor its attributedTo names
	// without checking whether the object is bridge-origin, minting a bridged
	// actor for our own persona. A skip, never an error: our post in a
	// community's history is an expected item, and failing it would leave every
	// run reporting failures and the community never cleanly backfilled.
	//
	// The question is asked of the unwrapped OBJECT, not the outbox envelope:
	// the community mints its own Announce/Create ids around our content, so
	// the object is the only node in the item that can be ours — and it is what
	// this path would materialize.
	if ours, err := b.suppressEcho(ctx, obj); err != nil {
		return false, 0, err
	} else if ours {
		return false, 0, skip(obj.ID, "outbox item is our own federated content")
	}

	// Same funnel rules as live deliveries: never resurrect deleted
	// content, trust embedded bodies only on the outbox host's authority.
	// The walk reads markers in the backfilled community's scope — its own,
	// plus origin-authorized ones; a marker another community laid says
	// nothing about this community's outbox.
	tombstoned, err := b.tombstones.ExistsFor(ctx, obj.ID, communityIRI)
	if err != nil {
		return false, 0, fmt.Errorf("ingest: tombstone check for %s: %w", obj.ID, err)
	}
	if tombstoned {
		return false, 0, skip(obj.ID, "object was deleted upstream")
	}
	obj, err = b.resolveEmbedded(ctx, obj, communityIRI)
	if err != nil {
		return false, 0, err
	}
	ok, replies, err := b.materializeResolved(ctx, obj, communityIRI, communityDID)
	return ok, replies.hostGone, err
}

// outboxObject unwraps an outbox entry's Announce, Create and Update
// envelopes to the object they carry; nil when there is none.
func outboxObject(item *ap.Object) *ap.Object {
	obj := item
	for obj != nil && (obj.Type == ap.TypeAnnounce || obj.Type == ap.TypeCreate || obj.Type == ap.TypeUpdate) {
		obj = obj.Object
	}
	return obj
}

// materializeResolved materializes an outbox object whose body is trusted
// (embedded on the community's authority or fetched from its origin) and
// that passed the echo and tombstone checks: a Page or Article as a post plus
// its replies, a Note as a comment. replies is the post's reply pass outcome.
func (b *Backfill) materializeResolved(ctx context.Context, obj *ap.Object, communityIRI, communityDID string) (ok bool, replies repliesOutcome, err error) {
	switch obj.Type {
	case ap.TypePage, ap.TypeArticle:
		res, err := b.mat.MaterializePost(ctx, obj, communityIRI)
		if err != nil {
			// A result means the post committed and its acceptance failed, so
			// the record is live like any other and the recheck applies. Any
			// other error wrote nothing, and may concern a mapping in another
			// community, which a marker in this one says nothing about.
			if res != nil {
				if _, removeErr := b.removeIfDeletedDuringMaterialize(ctx, obj.ID, communityIRI); removeErr != nil {
					b.logger.Warn("backfill post removal failed", "post", obj.ID, "error", removeErr)
				}
			}
			return false, repliesOutcome{}, err
		}
		if removed, err := b.removeIfDeletedDuringMaterialize(ctx, obj.ID, communityIRI); err != nil {
			return false, repliesOutcome{}, err
		} else if removed {
			return false, repliesOutcome{}, skip(obj.ID, "object was deleted upstream during the walk")
		}
		// A community's host is trusted only for its own posts' counts.
		// MaterializePost is bound to the walked community, so a post it
		// returns without a skip already belongs to it: another community's
		// post, or one stored in another community and retargeted at this
		// one, is refused above. The comparison is defense in depth on that
		// binding; an unknown binding (empty) seeds nothing.
		if res.CommunityDID != "" && res.CommunityDID == communityDID {
			b.seedCounts(ctx, obj.ID, communityIRI)
		} else {
			b.logger.Debug("backfill vote-count seeding skipped: stored community is not the walked one",
				"post", obj.ID, "walked_community", communityDID, "stored_community", res.CommunityDID)
		}
		replies, err = b.backfillReplies(ctx, obj, communityIRI, communityDID)
		if err != nil {
			// The post itself landed (and a retry's re-materialization is
			// free), but an aborted reply pass fails the ITEM: that feeds the
			// run's failures counter, which is what keeps last_backfill_at
			// unset and the run resumable.
			return false, replies, err
		}
		return true, replies, nil
	case ap.TypeNote:
		removed, err := b.materializeComment(ctx, obj, communityIRI)
		if err != nil {
			return false, repliesOutcome{}, err
		}
		if removed {
			return false, repliesOutcome{}, skip(obj.ID, "object was deleted upstream during the walk")
		}
		return true, repliesOutcome{}, nil
	default:
		return false, repliesOutcome{}, skip(obj.ID, "unsupported outbox object type "+obj.Type)
	}
}

// materializeComment materializes a comment and rechecks each record that
// committed: the ancestors, whatever became of the comment, then the comment.
// removed reports that the comment itself was deleted during the wait. A failed
// removal fails the call even where the comment was only skipped, so the walk
// does not count the item as handled.
func (b *Backfill) materializeComment(ctx context.Context, note *ap.Object, communityIRI string) (removed bool, err error) {
	res, err := b.mat.MaterializeComment(ctx, note, communityIRI)
	removeErr := b.removeDeletedAncestors(ctx, res, communityIRI)
	if err != nil {
		if removeErr != nil {
			return false, fmt.Errorf("%w (materializing %s: %v)", removeErr, note.ID, err)
		}
		return false, err
	}
	removed, err = b.removeIfDeletedDuringMaterialize(ctx, note.ID, communityIRI)
	return removed, stderrors.Join(removeErr, err)
}

// removeDeletedAncestors runs the recheck on the ancestors res reports. One
// failure does not stop the others. MaterializeComment commits an ancestor only
// in the walked community, so a marker in that community speaks for it.
func (b *Backfill) removeDeletedAncestors(ctx context.Context, res *materialize.Result, communityIRI string) error {
	if res == nil {
		return nil
	}
	var failures []error
	for _, ancestor := range res.Ancestors {
		if _, err := b.removeIfDeletedDuringMaterialize(ctx, ancestor, communityIRI); err != nil {
			failures = append(failures, err)
		}
	}
	return stderrors.Join(failures...)
}

// removeIfDeletedDuringMaterialize removes a record whose Delete arrived after
// the walk's tombstone check. Materializing can wait minutes on the PLC write
// budget, and a Delete landing in that time finds no mapping to remove. The
// result reports whether the record was removed. Both calls run detached from
// cancellation: the record is committed and every later walk skips the id at
// the first check, so a walk cancelled here would leave a deleted post live for
// good.
func (b *Backfill) removeIfDeletedDuringMaterialize(ctx context.Context, apID, communityIRI string) (bool, error) {
	detached, cancel := context.WithTimeout(context.WithoutCancel(ctx), sweepMutationTimeout)
	defer cancel()
	tombstoned, err := b.tombstones.ExistsFor(detached, apID, communityIRI)
	if err != nil {
		return false, fmt.Errorf("ingest: tombstone recheck for %s: %w", apID, err)
	}
	if !tombstoned {
		return false, nil
	}
	if err := b.mat.HandleDeleteRecord(detached, apID); err != nil {
		return false, fmt.Errorf("ingest: remove %s deleted during backfill: %w", apID, err)
	}
	return true, nil
}

// suppressEcho reports whether an outbox node is content the bridge itself
// federated, counting and logging the drop when it is.
//
// This is the same guard the dispatcher runs, at the only other place content
// enters: a backfill has no envelope and no signer, so nothing upstream of here
// can ask the question. An error is propagated rather than resolved into a
// verdict — the run is resumable and re-walks, where "not ours" would
// re-materialize our own post and "ours" would drop a community's history.
func (b *Backfill) suppressEcho(ctx context.Context, node *ap.Object) (bool, error) {
	identity, err := b.classifier.Classify(ctx, node)
	if err != nil {
		return false, fmt.Errorf("ingest: backfill echo check for %s: %w", node.ID, err)
	}
	if identity.Class == echo.ClassNone {
		return false, nil
	}
	echo.CountDrop(identity.Class)
	// INFO, like every other echo drop: this is the only record that a piece of
	// a community's history was deliberately walked past.
	b.logger.Info("backfill item skipped: our own federated content",
		"object", node.ID, "class", string(identity.Class),
		"did", identity.DID, "at_uri", identity.ATURI)
	return true, nil
}

// seedCounts imports a backfilled post's historical vote counts (task 07),
// asked of the backfilled community's host (communityIRI).
// Best-effort: failures are logged and never affect the run — a post with a
// zero score is strictly better than no post. Warn (matching the
// backfillReplies convention) so a systemic seeding outage is visible at
// default log levels; a cancellation during shutdown is not an outage.
func (b *Backfill) seedCounts(ctx context.Context, postAPID, communityIRI string) {
	if b.seeder == nil {
		return
	}
	if err := b.seeder.SeedPostCounts(ctx, postAPID, communityIRI); err != nil {
		if ctx.Err() != nil {
			b.logger.Debug("backfill vote-count seeding canceled",
				"post", postAPID, "community", communityIRI, "error", err)
			return
		}
		b.logger.Warn("backfill vote-count seeding failed",
			"post", postAPID, "community", communityIRI, "error", err)
	}
}

// backfillReplies pages a post's advertised replies collection.
// Content-shaped problems — a tombstoned reply, a non-Note, an unresolvable
// body — are logged, never fatal: replies are best-effort garnish on backfill.
// An echo classification that cannot be MADE is the one exception: per
// suppressEcho's contract the error propagates, aborting the reply pass so the
// caller counts a failure the way an outbox-item failure is counted and the
// run leaves last_backfill_at unset. A reply whose origin host is gone is a
// skip, deferred for RetryDeferred under communityIRI and communityDID; one
// whose failure cannot be recorded into its host's streak fails the pass the
// same way, after the rest of the collection is walked, with ErrReplyHostGone,
// so the run retries it, and so does one whose deferral cannot be written. A
// reply on a host that already failed host-gone in this walk is deferred
// without a fetch. A replies collection (or a later page) on a gone host
// defers the post itself under that host, so a later sweep re-walks its
// replies; when its failure cannot be recorded, or the deferral cannot be
// written, the pass fails the same way. Any other reply failure is logged and
// does not fail the pass. communityIRI is also the scope of the tombstone
// lookup below.
func (b *Backfill) backfillReplies(ctx context.Context, post *ap.Object, communityIRI, communityDID string) (outcome repliesOutcome, err error) {
	if post.Replies == nil || post.Replies.ID == "" {
		// Not advertised (or inline-only, which Lemmy never emits).
		return repliesOutcome{}, nil
	}
	hostGone := 0
	repliesIRI := post.Replies.ID
	count := 0
	var classifyErr error
	// unrecordedHostGoneErr is the first reply whose origin host is gone and
	// whose failure could not be recorded into the host's streak. The walk
	// goes on (other replies still land), and the pass then fails the item so
	// the run stays unstamped and retries.
	var unrecordedHostGoneErr error
	noteUnrecordedHostGone := func(replyID string, err error) {
		b.logger.Warn("backfill reply failed", "post", post.ID, "reply", replyID, "error", err)
		if unrecordedHostGoneErr == nil {
			// ErrReplyHostGone with the cause by %v, not %w: the item switch
			// must not classify the item as host-gone (a deferrable skip), since
			// this failure could not be recorded into the host's streak.
			unrecordedHostGoneErr = fmt.Errorf("ingest: backfill reply %s of %s: %w: %v",
				replyID, post.ID, ErrReplyHostGone, err)
		}
	}
	noteDeadHost := func(replyID string, err error) {
		hostGone++
		if replyID == "" {
			b.logger.Warn("backfill reply skipped: origin host is gone", "post", post.ID, "error", err)
			return
		}
		b.logger.Warn("backfill reply skipped: origin host is gone", "post", post.ID, "reply", replyID, "error", err)
	}
	// deferErr is the first reply (or the post, for its collection) whose
	// host-gone deferral failed.
	var deferErr error
	deferReply := func(reply *ap.Object, err error) {
		if failure := b.deferDeadHostItem(ctx, reply, communityIRI, communityDID, err); failure != nil {
			b.logger.Warn("backfill reply failed", "post", post.ID, "reply", reply.ID, "error", failure)
			if deferErr == nil {
				deferErr = failure
			}
			return
		}
		noteDeadHost(reply.ID, err)
	}
	err = b.fetcher.FetchCollection(ctx, repliesIRI, func(item *ap.Object) error {
		if count >= maxRepliesPerPost {
			return ap.ErrStop
		}
		count++
		note := *item
		// Before resolveEmbedded, for the same reason as the outbox item: a
		// reply of ours must not be dereferenced from our own origin, and a
		// native comment in a Lemmy thread is exactly what this collection
		// holds once a Coves user replies.
		if ours, err := b.suppressEcho(ctx, &note); err != nil {
			// Propagated, never resolved into a skip: suppressEcho's contract.
			// Remembered so the abort below is distinguishable from the walk's
			// own best-effort fetch failures.
			classifyErr = err
			return err
		} else if ours {
			return nil
		}
		if cause := knownGoneHost(ctx, &note, repliesIRI); cause != nil {
			deferReply(&note, cause)
			return nil
		}
		resolved, err := b.resolveEmbedded(ctx, &note, repliesIRI)
		replyHostGone := errors.IsHostGone(err)
		deferrable := replyHostGone && b.recordHostGone(ctx, err)
		switch {
		case err == nil:
		case deferrable:
			deferReply(&note, err)
			return nil
		case replyHostGone:
			noteUnrecordedHostGone(note.ID, err)
			return nil
		case materialize.IsSkip(err):
			b.logger.Info("backfill reply skipped", "post", post.ID, "reply", note.ID, "reason", err.Error())
			return nil
		default:
			b.logger.Warn("backfill reply failed", "post", post.ID, "reply", note.ID, "error", err)
			return nil
		}
		if resolved.Type != ap.TypeNote {
			return nil
		}
		// Same funnel rule as the live path and materializeOutboxItem: a reply
		// with a recorded Delete must never be resurrected, even if it still
		// lingers in the origin's replies collection (delivery/collection race).
		tombstoned, err := b.tombstones.ExistsFor(ctx, resolved.ID, communityIRI)
		if err != nil {
			b.logger.Warn("backfill reply tombstone check failed", "post", post.ID, "reply", resolved.ID, "error", err)
			return nil
		}
		if tombstoned {
			b.logger.Info("backfill reply skipped", "post", post.ID, "reason", "reply was deleted upstream")
			return nil
		}
		removed, err := b.materializeComment(ctx, resolved, communityIRI)
		replyHostGone = errors.IsHostGone(err)
		deferrable = replyHostGone && b.recordHostGone(ctx, err)
		switch {
		case deferrable:
			deferReply(resolved, err)
		case !replyHostGone && materialize.IsSkip(err):
			b.logger.Info("backfill reply skipped", "post", post.ID, "reason", err.Error())
		case replyHostGone:
			noteUnrecordedHostGone(resolved.ID, err)
		case err != nil:
			b.logger.Warn("backfill reply failed", "post", post.ID, "reply", resolved.ID, "error", err)
		case removed:
			b.logger.Info("backfill reply skipped", "post", post.ID, "reason", "reply was deleted upstream during the walk")
		}
		return nil
	})
	if classifyErr != nil {
		// The classifier error came back out through FetchCollection; return
		// the unwrapped cause so the run's failure names the check, not the
		// walk.
		return repliesOutcome{hostGone: hostGone}, classifyErr
	}
	var collectionGone error
	if err != nil && !stderrors.Is(err, ap.ErrCollectionTruncated) {
		b.logger.Warn("backfill replies walk failed", "post", post.ID, "error", err)
		// A gone host on the collection itself (first fetch or a later page)
		// is a skip once the failure is recorded into the host's streak: the
		// post is deferred under that host so a later sweep re-walks its
		// replies. An unrecorded failure fails the pass like a reply's.
		if errors.IsHostGone(err) {
			if b.recordHostGone(ctx, err) {
				if failure := b.deferDeadHostItem(ctx, post, communityIRI, communityDID, err); failure != nil {
					b.logger.Warn("backfill replies collection deferral failed", "post", post.ID, "error", failure)
					if deferErr == nil {
						deferErr = failure
					}
				} else {
					noteDeadHost("", err)
					collectionGone = err
				}
			} else if unrecordedHostGoneErr == nil {
				unrecordedHostGoneErr = fmt.Errorf("ingest: backfill replies %s of %s: %w: %v",
					repliesIRI, post.ID, ErrReplyHostGone, err)
			}
		}
	}
	return repliesOutcome{hostGone: hostGone, collectionGone: collectionGone}, stderrors.Join(unrecordedHostGoneErr, deferErr)
}

// repliesOutcome is what a post's reply pass left undone without failing.
type repliesOutcome struct {
	// hostGone counts the replies (and a replies collection) dropped because
	// their host is gone, for the walk's host_gone total.
	hostGone int
	// collectionGone is the host-gone failure of the replies collection (or a
	// later page) for which the post was deferred; nil when it was walked.
	collectionGone error
}

// recordHostGone records the host-gone failure err against its host's streak
// and reports whether the item can be deferred, so it is a skip. false means
// the item is an ordinary failure: err names no host, or the streak could not
// be recorded (logged). Call it exactly once per failure.
//
// Within a walk the verdict is kept per host, for host-level reasons only
// (hostLevelHostGone): a cached-verdict error (ap.ErrCachedHostVerdict)
// repeats an observation already made, so it reuses that verdict, or is not
// deferrable when the walk holds none, and records nothing.
func (b *Backfill) recordHostGone(ctx context.Context, err error) (deferrable bool) {
	host, ok := errors.HostGoneHost(err)
	if !ok {
		b.logger.Warn("backfill: host-gone error names no host", "error", err)
		return false
	}
	hosts := walkHostsFrom(ctx)
	if stderrors.Is(err, ap.ErrCachedHostVerdict) {
		deferrable, _ := hosts.verdict(host)
		return deferrable
	}
	hostLevel := hostLevelHostGone(err)
	goneSince, recordErr := b.instances.RecordHostGone(ctx, host, b.now())
	if recordErr != nil {
		b.logger.Warn("backfill: record host-gone streak failed", "host", host, "error", recordErr)
		if hostLevel {
			hosts.setVerdict(host, false)
		}
		return false
	}
	reason, _ := errors.HostGoneReason(err)
	b.logger.Info("backfill: origin host gone; deferring its items",
		"host", host, "reason", reason, "gone_since", goneSince)
	if hostLevel {
		hosts.setVerdict(host, true)
	}
	return true
}

// hostLevelHostGone reports whether a host-gone failure speaks for its whole
// host: every reason but redirect_to_root, parked_redirect and
// too_many_redirects, which concern the one URL fetched.
func hostLevelHostGone(err error) bool {
	switch reason, _ := errors.HostGoneReason(err); reason {
	case errors.HostGoneReasonRedirectToRoot, errors.HostGoneReasonParkedRedirect, errors.HostGoneReasonTooManyRedirects:
		return false
	}
	return true
}

// knownGoneHost returns a host-gone cause for a collection item that would be
// fetched from its origin (resolveEmbedded) when its host already failed
// host-gone in this walk or sweep, so the item is deferred without a fetch;
// nil otherwise.
func knownGoneHost(ctx context.Context, obj *ap.Object, sourceIRI string) error {
	if embeddedTrusted(obj, sourceIRI) {
		return nil
	}
	host, ok := ap.HostKey(obj.ID)
	if !ok {
		return nil
	}
	if gone, _ := walkHostsFrom(ctx).verdict(host); !gone {
		return nil
	}
	return fmt.Errorf("ingest: %s: origin host already failed host-gone in this walk: %w",
		obj.ID, errors.HostGoneError{Host: host})
}

// probeResult is what a host's nodeinfo probe says about the host.
type probeResult int

const (
	// probeAnswered: the host answered its probe with a JSON object.
	probeAnswered probeResult = iota
	// probeNotAnswered: the probe failed on the host's side.
	probeNotAnswered
	// probeUnknown: the probe was cut short by our ctx or failed on our side
	// (limiter, request budget, egress guard), so it says nothing about the
	// host.
	probeUnknown
)

// hostAnswersProbe probes host's nodeinfo, at most once per walk for a
// result that says something about the host; an unknown result is not kept.
func (b *Backfill) hostAnswersProbe(ctx context.Context, host string) probeResult {
	hosts := walkHostsFrom(ctx)
	if answered, known := hosts.probeAnswer(host); known {
		if answered {
			return probeAnswered
		}
		return probeNotAnswered
	}
	err := b.prober.ProbeNodeInfo(ctx, host)
	if err != nil && isLocalFetchError(ctx, err) {
		b.logger.Info("backfill: nodeinfo probe gave no answer", "host", host, "error", err)
		return probeUnknown
	}
	if err != nil {
		b.logger.Info("backfill: nodeinfo probe failed", "host", host, "error", err)
		hosts.setProbeAnswer(host, false)
		return probeNotAnswered
	}
	hosts.setProbeAnswer(host, true)
	return probeAnswered
}

// isLocalFetchError reports whether a fetch failed on our side rather than
// the remote host's: ctx ended, the request was canceled, the request budget
// (or another deadline of ours) ran out, the per-host limiter refused the
// wait, or the egress guard refused our own request URL (not a redirect
// target the origin chose). A response-header timeout is the
// remote host's: net/http reports it as a timeout that matches
// context.DeadlineExceeded through errors.Is, so a deadline counts as ours
// only when the context error itself is in err's chain.
func isLocalFetchError(ctx context.Context, err error) bool {
	if ctx.Err() != nil || stderrors.Is(err, context.Canceled) || chainHolds(err, context.DeadlineExceeded) {
		return true
	}
	if stderrors.Is(err, ap.ErrRateLimitWait) {
		return true
	}
	// A refusal of a redirect target is the origin's: it chose the target.
	var refusedHop ap.RedirectHopRefusedError
	if stderrors.As(err, &refusedHop) {
		return false
	}
	// The egress guard and URL checks report a refused request URL on field
	// "url"; a refused redirect is field "redirect" and is the origin's.
	var validation errors.ValidationError
	return stderrors.As(err, &validation) && validation.Field == "url"
}

// chainHolds reports whether target itself (by identity, not through an Is
// method) is in err's chain.
func chainHolds(err, target error) bool {
	for err != nil {
		if err == target {
			return true
		}
		switch unwrapper := err.(type) {
		case interface{ Unwrap() []error }:
			for _, inner := range unwrapper.Unwrap() {
				if chainHolds(inner, target) {
					return true
				}
			}
			return false
		case interface{ Unwrap() error }:
			err = unwrapper.Unwrap()
		default:
			return false
		}
	}
	return false
}

// walkHostsKey is the ctx key of a walk's walkHosts.
type walkHostsKey struct{}

// walkHosts holds one walk's or one RetryDeferred sweep's per-host
// knowledge: the verdict of each host that failed host-gone for a host-level
// reason (true when its items are deferred, or in a sweep rescheduled,
// without a fetch), and each probed host's nodeinfo answer.
type walkHosts struct {
	mu       sync.Mutex
	verdicts map[string]bool
	probes   map[string]bool
}

// withWalkHosts returns ctx carrying a fresh walkHosts.
func withWalkHosts(ctx context.Context) context.Context {
	return context.WithValue(ctx, walkHostsKey{}, &walkHosts{verdicts: map[string]bool{}, probes: map[string]bool{}})
}

// walkHostsFrom returns ctx's walkHosts, or nil outside a walk (its methods
// then know nothing and store nothing).
func walkHostsFrom(ctx context.Context) *walkHosts {
	hosts, _ := ctx.Value(walkHostsKey{}).(*walkHosts)
	return hosts
}

func (w *walkHosts) verdict(host string) (gone, known bool) {
	if w == nil {
		return false, false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	gone, known = w.verdicts[host]
	return gone, known
}

func (w *walkHosts) setVerdict(host string, gone bool) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.verdicts[host] = gone
}

func (w *walkHosts) probeAnswer(host string) (answered, known bool) {
	if w == nil {
		return false, false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	answered, known = w.probes[host]
	return answered, known
}

func (w *walkHosts) setProbeAnswer(host string, answered bool) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.probes[host] = answered
}

// clearHostGone ends the host-gone streak of iri's host after a successful
// fetch from it. A store failure is logged, never fatal to the item.
func (b *Backfill) clearHostGone(ctx context.Context, iri string) {
	host, ok := ap.HostKey(iri)
	if !ok {
		return
	}
	if err := b.instances.ClearHostGone(ctx, host); err != nil {
		b.logger.Warn("backfill: clear host-gone streak failed", "host", host, "error", err)
	}
}

// isLemmyNotFoundFor reports Lemmy's not-found answer for an embedded item of
// itemType: couldnt_find_post for a Page or Article, couldnt_find_comment for
// a Note. Any other type has no matching code.
func isLemmyNotFoundFor(itemType string, err error) bool {
	switch itemType {
	case ap.TypePage, ap.TypeArticle:
		return ap.IsLemmyNotFound(err, ap.LemmyCouldntFindPost)
	case ap.TypeNote:
		return ap.IsLemmyNotFound(err, ap.LemmyCouldntFindComment)
	}
	return false
}

// objectNotServed reports whether the fetch of iri failed because its host
// answered the exact object URL with a non-JSON page (no redirect) while the
// host still answers its nodeinfo probe: the host is up but does not serve
// the object as ActivityPub. It never feeds the host-gone streak.
func (b *Backfill) objectNotServed(ctx context.Context, iri string, err error) bool {
	var nonJSON ap.NonJSONResponseError
	if errors.IsHostGone(err) || !stderrors.As(err, &nonJSON) || nonJSON.Redirected {
		return false
	}
	host, ok := ap.HostKey(nonJSON.URL)
	if !ok || b.hostAnswersProbe(ctx, host) != probeAnswered {
		return false
	}
	b.logger.Info("backfill object skipped: its host does not serve it",
		"object", iri, "host", host, "reason", "object_not_served")
	return true
}

// resolveEmbedded applies the embedded-object trust rule to collection
// items: bodies whose id lives on the collection's own authority are used
// as-is, everything else is re-fetched from its origin (with the
// self-asserted-id binding).
func (b *Backfill) resolveEmbedded(ctx context.Context, obj *ap.Object, sourceIRI string) (*ap.Object, error) {
	if embeddedTrusted(obj, sourceIRI) {
		return obj, nil
	}
	return b.fetchFromOrigin(ctx, obj)
}

// embeddedTrusted reports whether resolveEmbedded uses obj's embedded body
// as-is: it is typed and lives on sourceIRI's authority.
func embeddedTrusted(obj *ap.Object, sourceIRI string) bool {
	return obj.Type != "" && ap.SameAuthority(obj.ID, sourceIRI)
}

// fetchFromOrigin fetches obj from its origin with the self-asserted-id
// binding. obj.Type, when known, selects Lemmy's not-found code. An object
// the origin no longer has, or does not serve, is a skip.
func (b *Backfill) fetchFromOrigin(ctx context.Context, obj *ap.Object) (*ap.Object, error) {
	fetched, err := b.fetcher.FetchObject(ctx, obj.ID)
	switch {
	case err == nil:
	case errors.IsTombstoned(err):
		return nil, skip(obj.ID, "object is tombstoned upstream")
	case errors.IsNotFound(err):
		return nil, skip(obj.ID, "object is unavailable upstream")
	case isLemmyNotFoundFor(obj.Type, err):
		return nil, skip(obj.ID, "object is purged upstream")
	case b.objectNotServed(ctx, obj.ID, err):
		return nil, skip(obj.ID, "the origin host is up but does not serve the object")
	default:
		// A host-gone error is returned as is (wrapped): the caller decides,
		// once per failure, whether it was recorded into the host's streak
		// and the item can be deferred (recordHostGone).
		return nil, fmt.Errorf("ingest: fetch %s: %w", obj.ID, err)
	}
	if fetched.ID == "" {
		fetched.ID = obj.ID
	} else if !ap.SameAuthority(fetched.ID, obj.ID) {
		return nil, skip(obj.ID, fmt.Sprintf("fetched object served a cross-authority id %s", fetched.ID))
	}
	// Only a response that passed the id check counts as the host serving
	// its own objects again.
	b.clearHostGone(ctx, obj.ID)
	return fetched, nil
}

// ErrReplyHostGone marks a reply pass failed because a reply's (or the
// replies collection's) origin host is gone and the failure could not be
// recorded into the host's streak. It is not errors.ErrHostGone: the item
// must not be classified host-gone, a deferrable skip. The failure is local,
// so a deferred retry leaves its row unchanged.
var ErrReplyHostGone = stderrors.New("origin host gone, not skipped")

// errOriginFailure marks a deferred item's origin fetch that failed on the
// origin's side without being a skip or host-gone: an HTTP error status, a
// non-JSON page, a header timeout, a truncated or unparseable body, a refused
// redirect, a DNS failure other than NXDOMAIN, a read reset.
var errOriginFailure = stderrors.New("origin fetch failed")

// deferredRetrySteps are the retries of a deferred item, measured from its
// first skip: 1h, 6h, 24h, 72h, 1 week, 2 weeks, 30 days and 90 days.
var deferredRetrySteps = []time.Duration{
	time.Hour, 6 * time.Hour, 24 * time.Hour, 72 * time.Hour,
	168 * time.Hour, 336 * time.Hour, 720 * time.Hour, 2160 * time.Hour,
}

// Reasons a deferred item is dropped without landing (the reason attr of
// "backfill deferred item dropped").
const (
	deferredDropCommunityRemoved    = "community_removed"
	deferredDropCommunityUnfollowed = "community_unfollowed"
	deferredDropRetriesExhausted    = "retries_exhausted"
)

// deferDeadHostItem keeps obj, skipped in the community because the host
// named by cause is gone, for RetryDeferred; its first retry is 1h out.
func (b *Backfill) deferDeadHostItem(ctx context.Context, obj *ap.Object, communityIRI, communityDID string, cause error) error {
	if obj == nil || obj.ID == "" {
		return fmt.Errorf("ingest: defer dead-host item in %s: item has no object id", communityIRI)
	}
	host, ok := errors.HostGoneHost(cause)
	if !ok {
		return fmt.Errorf("ingest: defer %s in %s: its dead-host cause names no host: %w", obj.ID, communityIRI, cause)
	}
	createdAt := b.now()
	nextRetryAt, _ := deferredRetryAt(createdAt, createdAt)
	err := b.deferred.DeferItem(ctx, store.DeferredItem{
		ObjectIRI:     obj.ID,
		CommunityAPID: communityIRI,
		CommunityDID:  communityDID,
		Host:          host,
		ObjectType:    obj.Type,
		NextRetryAt:   nextRetryAt,
		CreatedAt:     createdAt,
	})
	if err != nil {
		return fmt.Errorf("ingest: defer %s skipped on dead host %s: %w", obj.ID, host, err)
	}
	return nil
}

// RetryDeferred runs one sweep of the deferred items that are due, at most
// limit of them and at most store.MaxDueItemsPerHost per host, oldest due
// first. The sweep is one walk: it shares the unreachable-host cache and host
// verdicts, so a host that fails host-gone for a host-level reason (any but
// redirect_to_root, parked_redirect and too_many_redirects, which concern
// one URL) costs one
// request per sweep: its later items in the sweep count an attempt and are
// rescheduled unfetched, and replies on it are deferred unfetched. An item
// with no retry step left is still fetched. Its mints wait on the PLC
// budget. Each item is fetched from its origin again and materialized like an
// outbox item. A landed item, or one its origin no longer has, is deleted; a host
// failure counts an attempt and reschedules it, or drops it once the 90-day
// retry has run; an item of a removed community, or of one the bridge no
// longer follows (follow state none), is dropped unfetched. An item of a
// community whose follow is pending is kept unfetched with its attempts
// unchanged and moved to its next retry step (deferredRetryAt), so it does
// not stay due and crowd the batch; past its 90-day retry it is dropped. A
// local failure leaves the item as it was for the next sweep, or drops it
// once its 90-day retry has run. The error is
// the listing's or the ctx's, which ends the sweep.
func (b *Backfill) RetryDeferred(ctx context.Context, limit int) error {
	ctx = withBudgetWait(withWalkHosts(ap.WithUnreachableHostCache(ctx)))
	items, err := b.deferred.ListDueItems(ctx, b.now(), limit)
	if err != nil {
		return fmt.Errorf("ingest: list due deferred backfill items: %w", err)
	}
	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := b.retryDeferredItem(ctx, item); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if _, ok := deferredRetryAt(item.CreatedAt, b.now()); !ok {
				// The 90-day retry has run: a local failure on it ends the
				// item like a host failure would.
				if dropErr := b.dropDeferredItem(ctx, item, deferredDropRetriesExhausted, err); dropErr != nil {
					b.logger.Warn("backfill deferred item drop failed",
						"object", item.ObjectIRI, "community", item.CommunityAPID, "error", dropErr)
				}
				continue
			}
			b.logger.Warn("backfill deferred item retry failed; kept for the next sweep",
				"object", item.ObjectIRI, "community", item.CommunityAPID, "error", err)
			// A good fetch from the item's host before the local failure may
			// have cleared that host's streak, which pulls this row's
			// schedule to its created_at. The failure was not the host's, so the row
			// keeps the schedule it had (still due for the next sweep).
			if err := b.deferred.RescheduleItem(ctx, item.ObjectIRI, item.CommunityAPID, item.Attempts, item.NextRetryAt); err != nil {
				b.logger.Warn("backfill deferred item schedule restore failed",
					"object", item.ObjectIRI, "community", item.CommunityAPID, "error", err)
			}
		}
	}
	return nil
}

// retryDeferredItem retries one deferred item. An error is a local failure:
// the item is left unchanged.
func (b *Backfill) retryDeferredItem(ctx context.Context, item store.DeferredItem) error {
	community, err := b.communities.GetByAPGroupID(ctx, item.CommunityAPID)
	switch {
	case errors.IsNotFound(err):
		return b.dropDeferredItem(ctx, item, deferredDropCommunityRemoved, nil)
	case err != nil:
		return fmt.Errorf("ingest: read community %s: %w", item.CommunityAPID, err)
	}
	removed, err := communityRemoved(ctx, b.communities, community.DID)
	if err != nil {
		return fmt.Errorf("ingest: read community removal of %s: %w", item.CommunityAPID, err)
	}
	if removed {
		return b.dropDeferredItem(ctx, item, deferredDropCommunityRemoved, nil)
	}
	switch community.FollowState {
	case store.FollowStateAccepted:
	case store.FollowStatePending:
		return b.deferPendingCommunityItem(ctx, item)
	default:
		return b.dropDeferredItem(ctx, item, deferredDropCommunityUnfollowed, nil)
	}

	hosts := walkHostsFrom(ctx)
	// An item with no retry step left is fetched anyway: it is never dropped
	// without a fetch of its own.
	if gone, _ := hosts.verdict(item.Host); gone {
		if _, ok := deferredRetryAt(item.CreatedAt, b.now()); ok {
			return b.rescheduleDeferredItem(ctx, item,
				fmt.Errorf("ingest: origin host %s already failed host-gone in this sweep", item.Host))
		}
	}
	err = b.materializeDeferredItem(ctx, item, community)
	hostGone := errors.IsHostGone(err)
	if host, ok := errors.HostGoneHost(err); ok && hostGone && hostLevelHostGone(err) {
		hosts.setVerdict(host, true)
	}
	switch {
	case err == nil:
		b.logger.Info("backfill deferred item landed",
			"object", item.ObjectIRI, "community", item.CommunityAPID, "attempts", item.Attempts+1)
	case !hostGone && materialize.IsSkip(err):
		b.logger.Info("backfill deferred item skipped",
			"object", item.ObjectIRI, "community", item.CommunityAPID, "reason", err.Error())
	case hostGone || isOriginFailure(err):
		return b.rescheduleDeferredItem(ctx, item, err)
	default:
		return err
	}
	if err := b.deferred.DeleteItem(ctx, item.ObjectIRI, item.CommunityAPID); err != nil {
		return fmt.Errorf("ingest: delete deferred item: %w", err)
	}
	return nil
}

// materializeDeferredItem fetches a deferred item from its origin, whatever
// was embedded when it was skipped, and materializes it through the outbox
// item path, after the tombstone check and the echo check on the stored id
// (a tombstoned item, or our own content, is a skip with no fetch). A fetch
// failure the origin caused that is neither a skip nor host-gone is marked
// errOriginFailure. A post that lands while its replies collection fails
// host-gone returns that failure, so its row is rescheduled, not deleted.
func (b *Backfill) materializeDeferredItem(ctx context.Context, item store.DeferredItem, community *store.Community) error {
	tombstoned, err := b.tombstones.ExistsFor(ctx, item.ObjectIRI, community.APGroupID)
	if err != nil {
		return fmt.Errorf("ingest: tombstone check for %s: %w", item.ObjectIRI, err)
	}
	if tombstoned {
		return skip(item.ObjectIRI, "object was deleted upstream")
	}
	stored := &ap.Object{ID: item.ObjectIRI, Type: item.ObjectType}
	if ours, err := b.suppressEcho(ctx, stored); err != nil {
		return err
	} else if ours {
		return skip(item.ObjectIRI, "deferred item is our own federated content")
	}
	obj, err := b.fetchFromOrigin(ctx, stored)
	if err != nil {
		if !errors.IsHostGone(err) && !materialize.IsSkip(err) && !isLocalFetchError(ctx, err) {
			return fmt.Errorf("%w: %w", errOriginFailure, err)
		}
		return err
	}
	if ours, err := b.suppressEcho(ctx, obj); err != nil {
		return err
	} else if ours {
		return skip(obj.ID, "deferred item is our own federated content")
	}
	_, replies, err := b.materializeResolved(ctx, obj, community.APGroupID, community.DID)
	if err == nil && replies.collectionGone != nil {
		// The post landed, but its replies are still unwalked: the row is
		// the post's, so the retry is rescheduled as a host-gone failure.
		return replies.collectionGone
	}
	return err
}

// isOriginFailure reports a failure an origin caused that is neither a skip
// nor host-gone: the deferred item's origin fetch failing on the origin's
// side (errOriginFailure), or an HTTP error status or non-JSON page from any
// later fetch.
func isOriginFailure(err error) bool {
	var httpErr ap.HTTPError
	var nonJSON ap.NonJSONResponseError
	return stderrors.Is(err, errOriginFailure) || stderrors.As(err, &httpErr) || stderrors.As(err, &nonJSON)
}

// rescheduleDeferredItem counts a failed retry of item caused by its origin,
// and schedules the next retry or drops the item when none is left.
func (b *Backfill) rescheduleDeferredItem(ctx context.Context, item store.DeferredItem, cause error) error {
	nextRetryAt, ok := deferredRetryAt(item.CreatedAt, b.now())
	if !ok {
		return b.dropDeferredItem(ctx, item, deferredDropRetriesExhausted, cause)
	}
	attempts := item.Attempts + 1
	if err := b.deferred.RescheduleItem(ctx, item.ObjectIRI, item.CommunityAPID, attempts, nextRetryAt); err != nil {
		return fmt.Errorf("ingest: reschedule deferred item: %w", err)
	}
	b.logger.Info("backfill deferred item rescheduled",
		"object", item.ObjectIRI, "community", item.CommunityAPID, "host", item.Host,
		"attempts", attempts, "next_retry_at", nextRetryAt, "error", cause)
	return nil
}

// deferPendingCommunityItem keeps item, of a community whose follow is
// pending, unfetched: a pending follow is temporary (a lifted removal leaves a
// wanted community pending until its Accept). Its attempts are unchanged and
// it moves to its next retry step, so it does not stay due at its old time,
// sorting first in every sweep and starving the batch. Once its 90-day retry
// has run there is no next step, and it is dropped as retries exhausted.
func (b *Backfill) deferPendingCommunityItem(ctx context.Context, item store.DeferredItem) error {
	nextRetryAt, ok := deferredRetryAt(item.CreatedAt, b.now())
	if !ok {
		return b.dropDeferredItem(ctx, item, deferredDropRetriesExhausted, nil)
	}
	if err := b.deferred.RescheduleItem(ctx, item.ObjectIRI, item.CommunityAPID, item.Attempts, nextRetryAt); err != nil {
		return fmt.Errorf("ingest: reschedule deferred item of pending community: %w", err)
	}
	b.logger.Info("backfill deferred item kept for a pending follow",
		"object", item.ObjectIRI, "community", item.CommunityAPID, "host", item.Host,
		"attempts", item.Attempts, "next_retry_at", nextRetryAt)
	return nil
}

// dropDeferredItem deletes item without it landing, logging reason and the
// final cause (nil when the item was dropped unfetched). The logged host is
// the one cause names, or the row's when it names none.
func (b *Backfill) dropDeferredItem(ctx context.Context, item store.DeferredItem, reason string, cause error) error {
	if err := b.deferred.DeleteItem(ctx, item.ObjectIRI, item.CommunityAPID); err != nil {
		return fmt.Errorf("ingest: delete deferred item: %w", err)
	}
	host := item.Host
	if causeHost, ok := errors.HostGoneHost(cause); ok {
		host = causeHost
	}
	attrs := []any{"object", item.ObjectIRI, "community", item.CommunityAPID, "host", host,
		"attempts", item.Attempts, "created_at", item.CreatedAt, "reason", reason}
	if cause != nil {
		attrs = append(attrs, "error", cause)
	}
	b.logger.Info("backfill deferred item dropped", attrs...)
	return nil
}

// deferredRetryAt is when a deferred item created at createdAt is next
// retried, as of now: the first retry step after now. false when its retries
// are exhausted (the 90-day retry has run).
func deferredRetryAt(createdAt, now time.Time) (time.Time, bool) {
	for _, step := range deferredRetrySteps {
		if at := createdAt.Add(step); at.After(now) {
			return at, true
		}
	}
	return time.Time{}, false
}
