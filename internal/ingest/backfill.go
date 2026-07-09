package ingest

import (
	"context"
	stderrors "errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"tidepool/internal/ap"
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
	// maxRepliesPerPost bounds one post's replies-collection walk.
	maxRepliesPerPost = 200
)

// BackfillFetcher is the slice of *ap.Client backfill needs.
type BackfillFetcher interface {
	FetchActor(ctx context.Context, iri string) (*ap.Object, error)
	FetchObject(ctx context.Context, iri string) (*ap.Object, error)
	FetchCollection(ctx context.Context, iri string, visit func(*ap.Object) error) error
}

// CountSeeder imports a backfilled post's historical vote counts from its
// origin's public API (task 07's votes.LemmySeeder; AP alone cannot provide
// them — outboxes announce historical Likes only sparsely). Optional and
// best-effort: a nil seeder or a seeding failure never affects the backfill
// outcome.
type CountSeeder interface {
	SeedPostCounts(ctx context.Context, postAPID string) error
}

// BackfillOptions configures NewBackfill. Fetcher, Materializer,
// Communities, and Tombstones are required.
type BackfillOptions struct {
	Fetcher      BackfillFetcher
	Materializer Materializer
	Communities  store.Communities
	Tombstones   store.Tombstones
	// Seeder, when set, seeds each backfilled post's vote aggregates from
	// the origin's public API (config SEED_COUNTS_FROM_API).
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
type Backfill struct {
	fetcher     BackfillFetcher
	mat         Materializer
	communities store.Communities
	tombstones  store.Tombstones
	seeder      CountSeeder
	maxPosts    int
	minInterval time.Duration
	baseCtx     context.Context
	logger      *slog.Logger

	mu      sync.Mutex
	running map[string]bool
	// wg tracks async runs so tests (and shutdown) can drain them.
	wg sync.WaitGroup
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
		seeder:      opts.Seeder,
		maxPosts:    opts.MaxPosts,
		minInterval: opts.MinInterval,
		baseCtx:     baseCtx,
		logger:      logger,
		running:     map[string]bool{},
	}
	if b.maxPosts <= 0 {
		b.maxPosts = DefaultBackfillMaxPosts
	}
	if b.minInterval <= 0 {
		b.minInterval = defaultBackfillMinInterval
	}
	return b, nil
}

// TriggerAsync starts a backfill run in the background (the follow-accept
// hook and the admin endpoint). Duplicate triggers for a community already
// running are dropped.
func (b *Backfill) TriggerAsync(community *store.Community, force bool) {
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		if err := b.Run(b.baseCtx, community, force); err != nil {
			b.logger.Error("backfill failed", "community", community.APGroupID, "error", err)
		}
	}()
}

// Wait blocks until every async run finishes (shutdown/tests).
func (b *Backfill) Wait() { b.wg.Wait() }

// Run performs one backfill synchronously. force bypasses the
// last-backfill freshness check. Runs for the same community are
// serialized; a concurrent duplicate returns immediately.
func (b *Backfill) Run(ctx context.Context, community *store.Community, force bool) error {
	if !force && community.LastBackfillAt != nil &&
		time.Since(*community.LastBackfillAt) < b.minInterval {
		b.logger.Info("backfill skipped: recently completed",
			"community", community.APGroupID, "last_backfill_at", community.LastBackfillAt)
		return nil
	}

	b.mu.Lock()
	if b.running[community.APGroupID] {
		b.mu.Unlock()
		b.logger.Info("backfill already running", "community", community.APGroupID)
		return nil
	}
	b.running[community.APGroupID] = true
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		delete(b.running, community.APGroupID)
		b.mu.Unlock()
	}()

	group, err := b.fetcher.FetchActor(ctx, community.APGroupID)
	if err != nil {
		return fmt.Errorf("ingest: backfill fetch group %s: %w", community.APGroupID, err)
	}
	if group.Outbox == "" {
		b.logger.Warn("backfill: group advertises no outbox", "community", community.APGroupID)
		return nil
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
			return fmt.Errorf("ingest: backfill outbox %s: %w", group.Outbox, err)
		}
	}

	posts, failures := 0, 0
	for _, item := range items {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		ok, err := b.materializeOutboxItem(ctx, item, community.APGroupID)
		switch {
		case err == nil:
			if ok {
				posts++
			}
		case materialize.IsSkip(err):
			b.logger.Info("backfill item skipped", "community", community.APGroupID, "reason", err.Error())
		default:
			failures++
			b.logger.Warn("backfill item failed", "community", community.APGroupID, "error", err)
		}
	}

	if failures > 0 {
		// Leave last_backfill_at untouched so the next trigger retries the
		// walk (resumable-by-redo; deterministic rkeys make it idempotent).
		return fmt.Errorf("ingest: backfill %s: %d of %d items failed",
			community.APGroupID, failures, len(items))
	}
	if walkTruncated {
		// Truncated walks are not clean completions: leave last_backfill_at
		// unset so an un-forced re-trigger re-walks from the top instead of
		// skipping inside the freshness window. Collected items already
		// materialized above; deterministic rkeys make the redo idempotent.
		b.logger.Info("backfill partial: truncated walk left resumable",
			"community", community.APGroupID, "posts", posts)
		return nil
	}
	if err := b.communities.SetLastBackfill(ctx, community.APGroupID, time.Now()); err != nil {
		return fmt.Errorf("ingest: record backfill for %s: %w", community.APGroupID, err)
	}
	b.logger.Info("backfill complete", "community", community.APGroupID, "posts", posts)
	return nil
}

// materializeOutboxItem unwraps one outbox entry (Announce{Create{Page}},
// Create{Page}, or a bare Page) and materializes the post plus its
// advertised replies. ok reports whether a post actually landed.
func (b *Backfill) materializeOutboxItem(ctx context.Context, item *ap.Object, communityIRI string) (bool, error) {
	obj := item
	for obj != nil && (obj.Type == ap.TypeAnnounce || obj.Type == ap.TypeCreate || obj.Type == ap.TypeUpdate) {
		obj = obj.Object
	}
	if obj == nil {
		return false, skip(item.ID, "outbox item carries no object")
	}
	if obj.ID == "" {
		return false, skip(item.ID, "outbox item object has no id")
	}

	// Same funnel rules as live deliveries: never resurrect deleted
	// content, trust embedded bodies only on the outbox host's authority.
	tombstoned, err := b.tombstones.Exists(ctx, obj.ID)
	if err != nil {
		return false, fmt.Errorf("ingest: tombstone check for %s: %w", obj.ID, err)
	}
	if tombstoned {
		return false, skip(obj.ID, "object was deleted upstream")
	}
	obj, err = b.resolveEmbedded(ctx, obj, communityIRI)
	if err != nil {
		return false, err
	}

	switch obj.Type {
	case ap.TypePage, ap.TypeArticle:
		if _, err := b.mat.MaterializePost(ctx, obj); err != nil {
			return false, err
		}
		b.seedCounts(ctx, obj.ID)
		b.backfillReplies(ctx, obj)
		return true, nil
	case ap.TypeNote:
		if _, err := b.mat.MaterializeComment(ctx, obj); err != nil {
			return false, err
		}
		return true, nil
	default:
		return false, skip(obj.ID, "unsupported outbox object type "+obj.Type)
	}
}

// seedCounts imports a backfilled post's historical vote counts (task 07).
// Best-effort: failures are logged and never affect the run — a post with a
// zero score is strictly better than no post. Warn (matching the
// backfillReplies convention) so a systemic seeding outage is visible at
// default log levels; a cancellation during shutdown is not an outage.
func (b *Backfill) seedCounts(ctx context.Context, postAPID string) {
	if b.seeder == nil {
		return
	}
	if err := b.seeder.SeedPostCounts(ctx, postAPID); err != nil {
		if ctx.Err() != nil {
			b.logger.Debug("backfill vote-count seeding canceled", "post", postAPID, "error", err)
			return
		}
		b.logger.Warn("backfill vote-count seeding failed", "post", postAPID, "error", err)
	}
}

// backfillReplies pages a post's advertised replies collection. Failures
// are logged, never fatal — replies are best-effort garnish on backfill.
func (b *Backfill) backfillReplies(ctx context.Context, post *ap.Object) {
	if post.Replies == nil || post.Replies.ID == "" {
		// Not advertised (or inline-only, which Lemmy never emits).
		return
	}
	repliesIRI := post.Replies.ID
	count := 0
	err := b.fetcher.FetchCollection(ctx, repliesIRI, func(item *ap.Object) error {
		if count >= maxRepliesPerPost {
			return ap.ErrStop
		}
		count++
		note := *item
		resolved, err := b.resolveEmbedded(ctx, &note, repliesIRI)
		if err != nil {
			b.logger.Info("backfill reply skipped", "post", post.ID, "error", err.Error())
			return nil
		}
		if resolved.Type != ap.TypeNote {
			return nil
		}
		// Same funnel rule as the live path and materializeOutboxItem: a reply
		// with a recorded Delete must never be resurrected, even if it still
		// lingers in the origin's replies collection (delivery/collection race).
		tombstoned, err := b.tombstones.Exists(ctx, resolved.ID)
		if err != nil {
			b.logger.Warn("backfill reply tombstone check failed", "post", post.ID, "reply", resolved.ID, "error", err)
			return nil
		}
		if tombstoned {
			b.logger.Info("backfill reply skipped", "post", post.ID, "reason", "reply was deleted upstream")
			return nil
		}
		if _, err := b.mat.MaterializeComment(ctx, resolved); err != nil {
			if materialize.IsSkip(err) {
				b.logger.Info("backfill reply skipped", "post", post.ID, "reason", err.Error())
				return nil
			}
			b.logger.Warn("backfill reply failed", "post", post.ID, "error", err)
		}
		return nil
	})
	if err != nil && !stderrors.Is(err, ap.ErrCollectionTruncated) {
		b.logger.Warn("backfill replies walk failed", "post", post.ID, "error", err)
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
		return nil, fmt.Errorf("ingest: fetch %s: %w", obj.ID, err)
	}
	if fetched.ID == "" {
		fetched.ID = obj.ID
	} else if !ap.SameAuthority(fetched.ID, obj.ID) {
		return nil, skip(obj.ID, fmt.Sprintf("fetched object served a cross-authority id %s", fetched.ID))
	}
	return fetched, nil
}
