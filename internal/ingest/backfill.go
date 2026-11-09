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
	// DeadHostAfter is how long a host must keep failing as host-gone
	// before backfill skips its items.
	DeadHostAfter = 72 * time.Hour
	// maxRepliesPerPost bounds one post's replies-collection walk.
	maxRepliesPerPost = 200
	// maxConcurrentBackfills bounds how many TriggerAsync walks run at once.
	maxConcurrentBackfills = 4
	// firstBackfillRetryDelay is the backoff after a community's first
	// unfinished walk. It doubles with each further one.
	firstBackfillRetryDelay = time.Hour
	// maxBackfillRetryDelay caps the backoff. It must stay under
	// store.HostGoneStreakGap (48h), or a gone host's streak would lapse
	// between retries and never reach DeadHostAfter.
	maxBackfillRetryDelay = 24 * time.Hour
)

// BackfillFetcher is the slice of *ap.Client backfill needs.
type BackfillFetcher interface {
	FetchActor(ctx context.Context, iri string) (*ap.Object, error)
	FetchObject(ctx context.Context, iri string) (*ap.Object, error)
	FetchCollection(ctx context.Context, iri string, visit func(*ap.Object) error) error
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
// Communities, Tombstones, Instances and Echo are required.
type BackfillOptions struct {
	Fetcher      BackfillFetcher
	Materializer Materializer
	Communities  store.Communities
	Tombstones   store.Tombstones
	// Instances tracks per-host host-gone streaks: an item whose origin host
	// is gone is skipped only once the host has kept failing for
	// DeadHostAfter. Required.
	Instances store.HostGoneStreaks
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
	classifier  EchoClassifier
	seeder      CountSeeder
	maxPosts    int
	minInterval time.Duration
	baseCtx     context.Context
	logger      *slog.Logger
	now         func() time.Time

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
		classifier:  opts.Echo,
		seeder:      opts.Seeder,
		maxPosts:    opts.MaxPosts,
		minInterval: opts.MinInterval,
		baseCtx:     baseCtx,
		logger:      logger,
		now:         time.Now,
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
// it has one (see currentCommunity). A forced walk applies no state check at
// any point.
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
	return b.walkClaimed(ctx, community)
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
// false.
func (b *Backfill) walk(ctx context.Context, community *store.Community) (stamped bool, err error) {
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
		ok, replyHostGone, err := b.materializeOutboxItem(ctx, item, community.APGroupID, community.DID)
		hostGoneSkipped += replyHostGone
		// Classified once: recordHostGone records the failure into the
		// host's streak, so it must run exactly once per failure.
		hostGone := errors.IsHostGone(err)
		deadHost := hostGone && b.recordHostGone(ctx, err)
		switch {
		case err == nil:
			if ok {
				posts++
			}
		case deadHost:
			// A gone origin host may only be lapsed, which is why the live
			// path retries it; once it has stayed gone for DeadHostAfter,
			// backfill drops the item so one dead author host cannot keep
			// the community from ever completing. Before that it falls
			// through to an ordinary failure, retried on the walk backoff.
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
	return true, nil
}

// materializeOutboxItem unwraps one outbox entry (Announce{Create{Page}},
// Create{Page}, or a bare Page) and materializes the post plus its
// advertised replies. ok reports whether a post actually landed;
// replyHostGone counts the replies dropped because their host is dead.
// communityIRI and communityDID identify the community being backfilled.
func (b *Backfill) materializeOutboxItem(ctx context.Context, item *ap.Object, communityIRI, communityDID string) (ok bool, replyHostGone int, err error) {
	obj := item
	for obj != nil && (obj.Type == ap.TypeAnnounce || obj.Type == ap.TypeCreate || obj.Type == ap.TypeUpdate) {
		obj = obj.Object
	}
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
			return false, 0, err
		}
		if removed, err := b.removeIfDeletedDuringMaterialize(ctx, obj.ID, communityIRI); err != nil {
			return false, 0, err
		} else if removed {
			return false, 0, skip(obj.ID, "object was deleted upstream during the walk")
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
		replyHostGone, err = b.backfillReplies(ctx, obj, communityIRI)
		if err != nil {
			// The post itself landed (and a retry's re-materialization is
			// free), but an aborted reply pass fails the ITEM: that feeds the
			// run's failures counter, which is what keeps last_backfill_at
			// unset and the run resumable.
			return false, replyHostGone, err
		}
		return true, replyHostGone, nil
	case ap.TypeNote:
		removed, err := b.materializeComment(ctx, obj, communityIRI)
		if err != nil {
			return false, 0, err
		}
		if removed {
			return false, 0, skip(obj.ID, "object was deleted upstream during the walk")
		}
		return true, 0, nil
	default:
		return false, 0, skip(obj.ID, "unsupported outbox object type "+obj.Type)
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
// run leaves last_backfill_at unset. A reply whose origin host is gone but
// not yet for DeadHostAfter fails the pass the same way (after the rest of
// the collection is walked), so the run retries it; once the host is dead it
// is a skip. A skip here would be silent and
// permanent: the "clean" completion stamps the freshness window that blocks
// the re-walk, and the genuine reply never lands. communityIRI is the
// community being backfilled, carried for the scoped tombstone lookup below.
// hostGone counts the replies (and a replies collection) dropped because their
// host is dead, for the walk's host_gone total.
func (b *Backfill) backfillReplies(ctx context.Context, post *ap.Object, communityIRI string) (hostGone int, err error) {
	if post.Replies == nil || post.Replies.ID == "" {
		// Not advertised (or inline-only, which Lemmy never emits).
		return 0, nil
	}
	repliesIRI := post.Replies.ID
	count := 0
	var classifyErr error
	// youngHostGoneErr is the first reply whose origin host is gone but not
	// yet for DeadHostAfter. The walk goes on (other replies still land), and
	// the pass then fails the item so the run stays unstamped and retries.
	var youngHostGoneErr error
	noteYoungHostGone := func(replyID string, err error) {
		b.logger.Warn("backfill reply failed", "post", post.ID, "reply", replyID, "error", err)
		if youngHostGoneErr == nil {
			// %v, not %w: the failure is already recorded into the host's
			// streak, so the item switch must not classify it as host-gone
			// again.
			youngHostGoneErr = fmt.Errorf("ingest: backfill reply %s of %s: origin host gone, not yet skipped: %v",
				replyID, post.ID, err)
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
		resolved, err := b.resolveEmbedded(ctx, &note, repliesIRI)
		replyHostGone := errors.IsHostGone(err)
		deadHost := replyHostGone && b.recordHostGone(ctx, err)
		switch {
		case err == nil:
		case deadHost:
			noteDeadHost(note.ID, err)
			return nil
		case replyHostGone:
			noteYoungHostGone(note.ID, err)
			return nil
		default:
			b.logger.Info("backfill reply skipped", "post", post.ID, "error", err.Error())
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
		deadHost = replyHostGone && b.recordHostGone(ctx, err)
		switch {
		case deadHost:
			noteDeadHost(resolved.ID, err)
		case !replyHostGone && materialize.IsSkip(err):
			b.logger.Info("backfill reply skipped", "post", post.ID, "reason", err.Error())
		case replyHostGone:
			noteYoungHostGone(resolved.ID, err)
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
		return hostGone, classifyErr
	}
	if err != nil && !stderrors.Is(err, ap.ErrCollectionTruncated) {
		b.logger.Warn("backfill replies walk failed", "post", post.ID, "error", err)
		// A gone host on the collection itself (first fetch or a later page)
		// hides replies the same way a gone reply host does: fail the item
		// while the streak is young so the run stays unstamped and retries.
		// %v, not %w: the failure is recorded here, so the item switch must
		// not record it again.
		if errors.IsHostGone(err) {
			if !b.recordHostGone(ctx, err) {
				return hostGone, fmt.Errorf("ingest: backfill replies %s of %s: origin host gone, not yet skipped: %v",
					repliesIRI, post.ID, err)
			}
			noteDeadHost("", err)
		}
	}
	return hostGone, youngHostGoneErr
}

// recordHostGone records the host-gone failure err against its host's streak
// and reports whether the host has now been gone for at least DeadHostAfter
// (dead), so the item is a skip. false means the item is an ordinary failure:
// the streak is younger, err names no host, or the streak could not be
// recorded (logged). Call it exactly once per failure.
func (b *Backfill) recordHostGone(ctx context.Context, err error) (dead bool) {
	host, ok := errors.HostGoneHost(err)
	if !ok {
		b.logger.Warn("backfill: host-gone error names no host", "error", err)
		return false
	}
	now := b.now()
	goneSince, recordErr := b.instances.RecordHostGone(ctx, host, now)
	if recordErr != nil {
		b.logger.Warn("backfill: record host-gone streak failed", "host", host, "error", recordErr)
		return false
	}
	if now.Sub(goneSince) < DeadHostAfter {
		b.logger.Info("backfill: origin host gone, retrying until the dead-host threshold",
			"host", host, "gone_since", goneSince, "skip_after", goneSince.Add(DeadHostAfter))
		return false
	}
	b.logger.Info("backfill: origin host gone past the dead-host threshold",
		"host", host, "gone_since", goneSince)
	return true
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

// resolveEmbedded applies the embedded-object trust rule to collection
// items: bodies whose id lives on the collection's own authority are used
// as-is, everything else is re-fetched from its origin (with the
// self-asserted-id binding).
func (b *Backfill) resolveEmbedded(ctx context.Context, obj *ap.Object, sourceIRI string) (*ap.Object, error) {
	if obj.Type != "" && ap.SameAuthority(obj.ID, sourceIRI) {
		return obj, nil
	}
	fetched, err := b.fetcher.FetchObject(ctx, obj.ID)
	switch {
	case err == nil:
	case errors.IsTombstoned(err):
		return nil, skip(obj.ID, "object is tombstoned upstream")
	case errors.IsNotFound(err):
		return nil, skip(obj.ID, "object is unavailable upstream")
	default:
		// A host-gone error is returned as is (wrapped): the caller decides,
		// once per failure, whether the host has been gone long enough to
		// skip (recordHostGone).
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
