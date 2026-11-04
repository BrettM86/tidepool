package ingest

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// heldWalk blocks one walk inside FetchActor until release closes.
type heldWalk struct {
	entered chan struct{}
	release chan struct{}
}

// emptyOutbox answers what a walk asks for after FetchActor when the outbox
// holds nothing.
type emptyOutbox struct{}

func (emptyOutbox) FetchObject(context.Context, string) (*ap.Object, error) {
	return nil, stderrors.New("the outbox is empty, nothing to fetch")
}

func (emptyOutbox) FetchCollection(context.Context, string, func(*ap.Object) error) error {
	return nil
}

// walkFetcher is a BackfillFetcher counting walks at FetchActor. The group
// advertises an outbox that is always empty, so a walk that does not fail
// reaches SetLastBackfill.
type walkFetcher struct {
	emptyOutbox

	clock *manualClock
	// walkDuration is how far each walk moves the clock.
	walkDuration time.Duration

	mutex   sync.Mutex
	walks   int
	failing bool
	held    *heldWalk
}

func (f *walkFetcher) FetchActor(ctx context.Context, iri string) (*ap.Object, error) {
	f.mutex.Lock()
	f.walks++
	failing, held := f.failing, f.held
	f.held = nil
	f.mutex.Unlock()
	if held != nil {
		close(held.entered)
		select {
		case <-held.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f.clock.Advance(f.walkDuration)
	if failing {
		return nil, stderrors.New("group fetch failed")
	}
	return &ap.Object{ID: iri, Type: ap.TypeGroup, Outbox: iri + "/outbox"}, nil
}

func (f *walkFetcher) setFailing(failing bool) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.failing = failing
}

func (f *walkFetcher) holdNextWalk() *heldWalk {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.held = &heldWalk{entered: make(chan struct{}), release: make(chan struct{})}
	return f.held
}

func (f *walkFetcher) walkCount() int {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return f.walks
}

// stampingCommunities is the store.Communities methods a walk with an empty
// outbox reaches. Its community stays accepted and carries the last recorded
// stamp.
type stampingCommunities struct {
	store.Communities

	mutex    sync.Mutex
	stampErr error
	readErr  error
	attempts int
	stamp    *time.Time
}

func (c *stampingCommunities) GetByAPGroupID(_ context.Context, id string) (*store.Community, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.readErr != nil {
		return nil, c.readErr
	}
	return &store.Community{APGroupID: id, FollowState: store.FollowStateAccepted, LastBackfillAt: c.stamp}, nil
}

func (c *stampingCommunities) SetLastBackfill(_ context.Context, _ string, at time.Time) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.attempts++
	if c.stampErr == nil {
		c.stamp = &at
	}
	return c.stampErr
}

func (c *stampingCommunities) failStamps(err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.stampErr = err
}

func (c *stampingCommunities) failReads(err error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.readErr = err
}

// ageStamp moves the stamp out of the freshness window.
func (c *stampingCommunities) ageStamp() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	aged := time.Now().Add(-2 * defaultBackfillMinInterval)
	c.stamp = &aged
}

func (c *stampingCommunities) stampAttempts() int {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.attempts
}

// The walks here hold no items, so these never run.
type (
	unusedMaterializer struct{ Materializer }
	unusedTombstones   struct{ store.Tombstones }
	unusedClassifier   struct{ EchoClassifier }
)

// retryFixture drives Backfill's retry state on a manual clock, with no
// database. The snapshot its triggers pass never has last_backfill_at set.
type retryFixture struct {
	clock       *manualClock
	fetcher     *walkFetcher
	communities *stampingCommunities
	community   *store.Community
}

func newRetryFixture(t *testing.T) *retryFixture {
	t.Helper()
	clock := newManualClock(t)
	return &retryFixture{
		clock:       clock,
		fetcher:     &walkFetcher{clock: clock},
		communities: &stampingCommunities{},
		community:   &store.Community{APGroupID: groupID},
	}
}

func (f *retryFixture) newBackfill(t *testing.T) *Backfill {
	t.Helper()
	b, err := NewBackfill(BackfillOptions{
		Fetcher:      f.fetcher,
		Materializer: unusedMaterializer{},
		Communities:  f.communities,
		Tombstones:   unusedTombstones{},
		Echo:         unusedClassifier{},
		BaseContext:  t.Context(),
		Logger:       slog.New(slog.DiscardHandler),
	})
	require.NoError(t, err)
	b.now = f.clock.Now
	return b
}

// retry calls RetryUnfinished, drains the run and counts the walks it started.
func (f *retryFixture) retry(b *Backfill) int {
	before := f.fetcher.walkCount()
	b.RetryUnfinished(f.community)
	b.Wait()
	return f.fetcher.walkCount() - before
}

// requireNextWalkAt asserts no walk 1 second before the delay has passed and
// one walk exactly when it has.
func (f *retryFixture) requireNextWalkAt(t *testing.T, b *Backfill, delay time.Duration) {
	t.Helper()
	f.clock.Advance(delay - time.Second)
	require.Equal(t, 0, f.retry(b), "a walk 1 second before the %s backoff lapsed", delay)
	f.clock.Advance(time.Second)
	require.Equal(t, 1, f.retry(b), "walks when the %s backoff lapsed", delay)
}

// TestBackfillRetry_BacksOff: the walk takes 10 minutes of clock each time, so
// a backoff measured from the walk's start would come due early.
func TestBackfillRetry_BacksOff(t *testing.T) {
	fixture := newRetryFixture(t)
	fixture.fetcher.walkDuration = 10 * time.Minute
	fixture.fetcher.setFailing(true)
	b := fixture.newBackfill(t)

	require.Equal(t, 1, fixture.retry(b), "a community with no retry state is due")
	for _, backoff := range []time.Duration{
		time.Hour, 2 * time.Hour, 4 * time.Hour, 8 * time.Hour, 16 * time.Hour, 24 * time.Hour, 24 * time.Hour,
	} {
		fixture.requireNextWalkAt(t, b, backoff)
	}
}

func TestBackfillRetry_CleanCompletionClearsTheBackoff(t *testing.T) {
	fixture := newRetryFixture(t)
	fixture.fetcher.setFailing(true)
	b := fixture.newBackfill(t)
	require.Equal(t, 1, fixture.retry(b))
	fixture.requireNextWalkAt(t, b, time.Hour)

	fixture.fetcher.setFailing(false)
	fixture.requireNextWalkAt(t, b, 2*time.Hour)

	fixture.fetcher.setFailing(true)
	fixture.communities.ageStamp()
	require.Equal(t, 1, fixture.retry(b), "a clean completion leaves no retry state")
	fixture.requireNextWalkAt(t, b, time.Hour)
}

// TestBackfillRetry_NewBackfillWalksAtOnce: the state is in memory, so a
// restart re-walks once.
func TestBackfillRetry_NewBackfillWalksAtOnce(t *testing.T) {
	fixture := newRetryFixture(t)
	fixture.fetcher.setFailing(true)
	before := fixture.newBackfill(t)
	require.Equal(t, 1, fixture.retry(before))
	require.Equal(t, 0, fixture.retry(before), "inside the backoff")

	require.Equal(t, 1, fixture.retry(fixture.newBackfill(t)), "a new Backfill has no retry state")
}

// TestBackfillRetry_DroppedRunKeepsTheSchedule: the held walk is the second
// failure, so the next backoff is 2 hours. Counting a dropped call as a walk
// would make it 4 hours or clear it.
func TestBackfillRetry_DroppedRunKeepsTheSchedule(t *testing.T) {
	fixture := newRetryFixture(t)
	fixture.fetcher.setFailing(true)
	b := fixture.newBackfill(t)
	require.Equal(t, 1, fixture.retry(b))
	fixture.clock.Advance(time.Hour)

	held := fixture.fetcher.holdNextWalk()
	b.RetryUnfinished(fixture.community)
	select {
	case <-held.entered:
	case <-time.After(stallDeadline):
		t.Fatal("stalled: the retry never started a walk")
	}

	require.NoError(t, b.Run(t.Context(), fixture.community, true))
	b.RetryUnfinished(fixture.community)
	close(held.release)
	b.Wait()
	require.Equal(t, 2, fixture.fetcher.walkCount(), "both calls during the walk are dropped")

	fixture.requireNextWalkAt(t, b, 2*time.Hour)
}

// TestBackfillRetry_FailedStampKeepsTheBackoff: the walks are clean, but
// last_backfill_at stays unset, so each one counts as unfinished.
func TestBackfillRetry_FailedStampKeepsTheBackoff(t *testing.T) {
	fixture := newRetryFixture(t)
	fixture.communities.failStamps(stderrors.New("database unavailable"))
	b := fixture.newBackfill(t)

	require.Equal(t, 1, fixture.retry(b))
	fixture.requireNextWalkAt(t, b, time.Hour)
	fixture.requireNextWalkAt(t, b, 2*time.Hour)
	require.Equal(t, 3, fixture.communities.stampAttempts(), "every walk reached the stamp")
}

// TestBackfillRetry_ClaimReadsTheBackoffOfTheWalkBeforeIt: a walk ends and
// records a new backoff after the retry's clock read and before its claim. The
// prior entry has lapsed, so a stale read would walk inside the new backoff and
// double the failure count again.
func TestBackfillRetry_ClaimReadsTheBackoffOfTheWalkBeforeIt(t *testing.T) {
	fixture := newRetryFixture(t)
	fixture.fetcher.setFailing(true)
	b := fixture.newBackfill(t)
	b.retries[groupID] = backfillRetry{failures: 1, nextRetry: fixture.clock.Now().Add(-time.Second)}

	var walkEnded atomic.Bool
	b.now = func() time.Time {
		if walkEnded.CompareAndSwap(false, true) {
			b.recordWalk(groupID, false)
		}
		return fixture.clock.Now()
	}

	assert.Equal(t, 0, fixture.retry(b), "a walk inside the backoff the last walk recorded")
	require.True(t, walkEnded.Load(), "the retry must read the clock")
	b.mu.Lock()
	defer b.mu.Unlock()
	assert.Equal(t, 2, b.retries[groupID].failures, "the skipped walk must not count")
}

// TestBackfillRetry_ForgottenCommunityKeepsTheSchedule: a community gone from
// the store drops its queued walk without counting as a walk.
func TestBackfillRetry_ForgottenCommunityKeepsTheSchedule(t *testing.T) {
	fixture := newRetryFixture(t)
	fixture.fetcher.setFailing(true)
	b := fixture.newBackfill(t)
	require.Equal(t, 1, fixture.retry(b))
	fixture.clock.Advance(time.Hour)

	fixture.communities.failReads(errors.NewNotFoundError("community", groupID))
	require.Equal(t, 0, fixture.retry(b), "a walk of a community the store no longer has")
	b.mu.Lock()
	defer b.mu.Unlock()
	assert.Equal(t, 1, b.retries[groupID].failures, "a dropped walk leaves the retry state alone")
}

// TestBackfillRetry_FailedReadBacksOff: a queued walk that cannot read its
// community does not walk, and the sweep's next try waits out a backoff.
func TestBackfillRetry_FailedReadBacksOff(t *testing.T) {
	fixture := newRetryFixture(t)
	b := fixture.newBackfill(t)

	fixture.communities.failReads(stderrors.New("database unavailable"))
	require.Equal(t, 0, fixture.retry(b), "a walk of a community that could not be read")
	fixture.communities.failReads(nil)

	fixture.requireNextWalkAt(t, b, time.Hour)
}

// TestBackfillRetry_BacklogCountsUnfinishedOutsideABackoff: a stamped community
// never counts. An unstamped one counts unless it is inside a backoff, up to the
// moment the backoff lapses.
func TestBackfillRetry_BacklogCountsUnfinishedOutsideABackoff(t *testing.T) {
	fixture := newRetryFixture(t)
	fixture.fetcher.setFailing(true)
	b := fixture.newBackfill(t)
	stamp := fixture.clock.Now()
	lapsing := &store.Community{APGroupID: "https://lemmy.example/c/lapsing"}
	backingOff := &store.Community{APGroupID: "https://lemmy.example/c/backing-off"}
	communities := []*store.Community{
		{APGroupID: "https://lemmy.example/c/stamped-one", LastBackfillAt: &stamp},
		{APGroupID: "https://lemmy.example/c/stamped-two", LastBackfillAt: &stamp},
		{APGroupID: "https://lemmy.example/c/never-walked"},
		lapsing,
		backingOff,
	}

	b.RetryUnfinished(lapsing)
	b.Wait()
	fixture.clock.Advance(time.Hour - time.Second)
	b.RetryUnfinished(backingOff)
	b.Wait()

	assert.Equal(t, 1, b.UnfinishedBacklog(communities), "1 second before the first backoff lapses")

	fixture.clock.Advance(time.Second)

	assert.Equal(t, 2, b.UnfinishedBacklog(communities), "when the first backoff lapses")
}

// newBudgetedMaterializer is the harness materializer with its Minter replaced
// by a MintGate over a real Minter on the fixture's budget and scripted PLC.
func newBudgetedMaterializer(t *testing.T, h *harness) (*materialize.Materializer, *mintFixture) {
	t.Helper()
	fixture := newMintFixtureOver(t, h.actors, h.custodian)
	options := h.materializerOptions
	options.Minter = fixture.gate(t)
	materializer, err := materialize.New(options)
	require.NoError(t, err)
	return materializer, fixture
}

// TestBackfill_WaitsOutTheBudget is the acceptance test for waiting. The reply
// matters: backfillReplies logs and swallows a MaterializeComment failure, so a
// refused reply would be lost with the community still stamped. TriggerAsync is
// the production entry point; its call ends when its walk does.
func TestBackfill_WaitsOutTheBudget(t *testing.T) {
	const replyID = "https://lemmy.world/comment/3001"

	for _, tc := range []struct {
		name  string
		start func(ctx context.Context, b *Backfill, community *store.Community) error
	}{
		{
			name: "Run",
			start: func(ctx context.Context, b *Backfill, community *store.Community) error {
				return b.Run(ctx, community, true)
			},
		},
		{
			name: "TriggerAsync",
			start: func(_ context.Context, b *Backfill, community *store.Community) error {
				b.TriggerAsync(community, true)
				b.Wait()
				return nil
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.subscribeTechnology()
			serveOutboxFixtures(t, h)
			materializer, fixture := newBudgetedMaterializer(t, h)
			ctx := t.Context()
			options := h.backfillOptions(materializer, 10)
			options.BaseContext = ctx
			b, err := NewBackfill(options)
			require.NoError(t, err)
			community, err := h.communities.GetByAPGroupID(ctx, groupID)
			require.NoError(t, err)
			require.NoError(t, fixture.budget.Wait(ctx), "a fresh budget admits one write")

			run := startCall(tc.name, func() error { return tc.start(ctx, b, community) })
			// ctx ends before this runs, and the harness's own cleanup runs after
			// it, so a failed row leaves no walk running against the database.
			t.Cleanup(func() { <-run.finished })
			fixture.clock.AwaitWaiters(run)
			requireStillBlocked(t, run, tc.name+" waits for the budget")
			require.Empty(t, fixture.plc.recordedPosts())

			for step := 0; step < 10 && !run.returned(); step++ {
				fixture.clock.Advance(12 * time.Second)
				fixture.clock.AwaitWaiters(run)
			}
			require.True(t, run.returned(), "%s is still blocked after 2 minutes of clock", tc.name)
			require.NoError(t, run.result(t))

			for _, id := range []string{pageID, secondPageID, replyID} {
				_, err := h.objects.GetByAPID(ctx, id)
				require.NoError(t, err, "%s must be materialized", id)
			}
			require.Len(t, fixture.plc.recordedPosts(), 3, "one POST per author: two posts' and the reply's")
			community, err = h.communities.GetByAPGroupID(ctx, groupID)
			require.NoError(t, err)
			require.NotNil(t, community.LastBackfillAt)
		})
	}
}

// serveOutboxWithNote re-serves the group outbox as the first fixture page,
// advertising no replies, then one Note replying to it. Requires
// serveOutboxFixtures to have registered both authors.
func serveOutboxWithNote(t *testing.T, h *harness, noteID string) {
	t.Helper()
	outboxRaw, err := os.ReadFile(filepath.Join("..", "ap", "testdata", "outbox_lemmy_world.json"))
	require.NoError(t, err)
	var outbox map[string]any
	require.NoError(t, json.Unmarshal(outboxRaw, &outbox))
	h.serveObject("/c/technology/outbox", map[string]any{
		"type": "OrderedCollection",
		"id":   "https://lemmy.world/c/technology/outbox",
		"orderedItems": []any{
			outbox["orderedItems"].([]any)[0],
			map[string]any{
				"type":   "Create",
				"actor":  "https://lemmy.world/u/replier",
				"object": note(noteID, "https://lemmy.world/u/replier", pageID, "a note in the outbox", "2026-07-07T05:00:00.000000Z"),
			},
		},
	})
}

// unmappedParentID is the comment the rows below leave unmapped until a walk
// reaches it through its child.
const unmappedParentID = "https://lemmy.world/comment/3003"

// serveUnmappedParent serves a comment on the first fixture page, which no walk
// has seen yet, written by an author the walk has not minted.
func serveUnmappedParent(h *harness) {
	h.serveObject("/comment/3003", note(unmappedParentID, "https://lemmy.world/u/replier", pageID,
		"a parent the walk only reaches through its child", "2026-07-07T04:00:00.000000Z"))
}

// serveOutboxOfOneNote serves an outbox holding only a Note by author, replying
// to the unmapped parent: the parent and the root page commit as its ancestors.
func serveOutboxOfOneNote(h *harness, noteID, author string) {
	serveUnmappedParent(h)
	h.serveObject("/c/technology/outbox", map[string]any{
		"type": "OrderedCollection",
		"id":   "https://lemmy.world/c/technology/outbox",
		"orderedItems": []any{map[string]any{
			"type":   "Create",
			"actor":  author,
			"object": note(noteID, author, unmappedParentID, "a note under an unmapped parent", "2026-07-07T05:00:00.000000Z"),
		}},
	})
}

// TestBackfill_DeleteDuringHeldMintRemovesTheRecord: the Delete lands after the
// walk's tombstone check, while the mint for that item waits on the budget. It
// finds no mapping to delete, and later walks skip the id, so this walk is the
// only thing that can remove the record it commits. The other community's
// marker says nothing about this walk, so its post stays.
func TestBackfill_DeleteDuringHeldMintRemovesTheRecord(t *testing.T) {
	const (
		replyID = "https://lemmy.world/comment/3001"
		noteID  = "https://lemmy.world/comment/3002"
	)

	for _, tc := range []struct {
		name string
		// arrange, when set, replaces the outbox.
		arrange func(t *testing.T, h *harness)
		// mintsBeforeDelete is how many mints finish before the walk holds on
		// the mint for the marked item.
		mintsBeforeDelete int
		markedID          string
		markerCommunity   string
		deleted           bool
		// live is what the walk must still have committed.
		live []string
		// never is what the walk must not reach once its post is gone.
		never []string
	}{
		{
			name:            "post",
			markedID:        pageID,
			markerCommunity: groupID,
			deleted:         true,
			live:            []string{secondPageID},
			never:           []string{replyID},
		},
		{
			name:            "post with a global marker",
			markedID:        pageID,
			markerCommunity: "",
			deleted:         true,
			live:            []string{secondPageID},
			never:           []string{replyID},
		},
		{
			name:            "post with another community's marker",
			markedID:        pageID,
			markerCommunity: gamingGroupID,
			live:            []string{pageID, secondPageID, replyID},
		},
		{
			name:              "reply",
			mintsBeforeDelete: 1,
			markedID:          replyID,
			markerCommunity:   groupID,
			deleted:           true,
			live:              []string{pageID, secondPageID},
		},
		{
			name:              "outbox note",
			arrange:           func(t *testing.T, h *harness) { serveOutboxWithNote(t, h, noteID) },
			mintsBeforeDelete: 1,
			markedID:          noteID,
			markerCommunity:   groupID,
			deleted:           true,
			live:              []string{pageID},
		},
		{
			name: "unmapped parent of an outbox note",
			arrange: func(_ *testing.T, h *harness) {
				serveOutboxOfOneNote(h, noteID, "https://lemmy.world/u/replier")
			},
			markedID:        unmappedParentID,
			markerCommunity: groupID,
			deleted:         true,
			live:            []string{pageID, noteID},
		},
		{
			name: "root page committed as an ancestor",
			arrange: func(_ *testing.T, h *harness) {
				serveOutboxOfOneNote(h, noteID, "https://lemmy.world/u/replier")
			},
			markedID:        pageID,
			markerCommunity: groupID,
			deleted:         true,
			live:            []string{unmappedParentID, noteID},
		},
		{
			name: "ancestor with another community's marker",
			arrange: func(_ *testing.T, h *harness) {
				serveOutboxOfOneNote(h, noteID, "https://lemmy.world/u/replier")
			},
			markedID:        unmappedParentID,
			markerCommunity: gamingGroupID,
			live:            []string{pageID, unmappedParentID, noteID},
		},
		{
			name: "ancestor of a note refused for its author",
			arrange: func(_ *testing.T, h *harness) {
				h.serveObject("/u/optedout", person("https://lemmy.world/u/optedout", "optedout",
					map[string]any{"summary": "<p>#nobridge</p>"}))
				serveOutboxOfOneNote(h, noteID, "https://lemmy.world/u/optedout")
			},
			markedID:        unmappedParentID,
			markerCommunity: groupID,
			deleted:         true,
			live:            []string{pageID},
			never:           []string{noteID},
		},
		{
			name: "unmapped parent of a reply",
			arrange: func(_ *testing.T, h *harness) {
				serveUnmappedParent(h)
				h.serveObject("/post/49131386/replies", map[string]any{
					"type":       "OrderedCollection",
					"id":         pageID + "/replies",
					"totalItems": 1,
					"orderedItems": []any{note(replyID, "https://lemmy.world/u/replier", unmappedParentID,
						"a reply under an unmapped parent", "2026-07-07T05:00:00.000000Z")},
				})
			},
			mintsBeforeDelete: 1,
			markedID:          unmappedParentID,
			markerCommunity:   groupID,
			deleted:           true,
			live:              []string{pageID, secondPageID, replyID},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.subscribeTechnology()
			serveOutboxFixtures(t, h)
			if tc.arrange != nil {
				tc.arrange(t, h)
			}
			materializer, fixture := newBudgetedMaterializer(t, h)
			ctx := t.Context()
			seeder := &recordingSeeder{}
			options := h.backfillOptions(materializer, 10)
			options.Seeder = seeder
			b, err := NewBackfill(options)
			require.NoError(t, err)
			community, err := h.communities.GetByAPGroupID(ctx, groupID)
			require.NoError(t, err)
			require.NoError(t, fixture.budget.Wait(ctx), "a fresh budget admits one write")

			run := startCall("Run", func() error { return b.Run(ctx, community, true) })
			t.Cleanup(func() { <-run.finished })
			fixture.clock.AwaitWaiters(run)
			for step := 0; step < 10 && len(fixture.plc.recordedPosts()) < tc.mintsBeforeDelete; step++ {
				fixture.clock.Advance(12 * time.Second)
				fixture.clock.AwaitWaiters(run)
			}
			require.Len(t, fixture.plc.recordedPosts(), tc.mintsBeforeDelete)
			requireStillBlocked(t, run, "the walk holds on the marked item's mint")

			require.NoError(t, h.tombstones.Record(ctx, tc.markedID, tc.markerCommunity))

			for step := 0; step < 10 && !run.returned(); step++ {
				fixture.clock.Advance(12 * time.Second)
				fixture.clock.AwaitWaiters(run)
			}
			require.True(t, run.returned(), "the walk is still blocked after 2 minutes of clock")
			require.NoError(t, run.result(t), "an item deleted mid-walk is a skip, not a failure")

			if tc.deleted {
				mapping, err := h.objects.GetByAPID(ctx, tc.markedID)
				require.NoError(t, err)
				assert.True(t, mapping.IsDeleted(), "%s was deleted upstream during the walk", tc.markedID)
			}
			for _, id := range tc.live {
				mapping, err := h.objects.GetByAPID(ctx, id)
				require.NoError(t, err, "%s must be materialized", id)
				assert.False(t, mapping.IsDeleted(), "%s must stay live", id)
			}
			for _, id := range tc.never {
				_, err := h.objects.GetByAPID(ctx, id)
				assert.True(t, errors.IsNotFound(err), "%s belongs to the deleted post", id)
			}
			if tc.deleted {
				assert.NotContains(t, seeder.seeded, tc.markedID, "counts of a deleted post")
			}
			assert.NotNil(t, h.lastBackfillAt(groupID), "the walk finished cleanly")
		})
	}
}

// cancellingMaterializer ends the walk's context right after the post commits,
// with a Delete recorded in the same moment.
type cancellingMaterializer struct {
	Materializer

	cancel context.CancelFunc
	record func()
}

func (m *cancellingMaterializer) MaterializePost(ctx context.Context, page *ap.Object, communityIRI string) (*materialize.Result, error) {
	result, err := m.Materializer.MaterializePost(ctx, page, communityIRI)
	m.record()
	m.cancel()
	return result, err
}

// TestBackfill_CancelledWalkStillRemovesADeletedPost: the record is committed
// and later walks skip the id, so shutdown must not stop this removal.
func TestBackfill_CancelledWalkStillRemovesADeletedPost(t *testing.T) {
	h := newHarness(t)
	h.subscribeTechnology()
	serveOutboxFixtures(t, h)
	background := context.Background()
	ctx, cancel := context.WithCancel(background)
	defer cancel()

	b := newBackfillOver(t, h, &cancellingMaterializer{
		Materializer: h.mat,
		cancel:       cancel,
		record:       func() { require.NoError(t, h.tombstones.Record(background, pageID, groupID)) },
	}, 1)
	community, err := h.communities.GetByAPGroupID(background, groupID)
	require.NoError(t, err)
	require.Error(t, b.Run(ctx, community, true), "a cancelled walk cannot stamp")

	mapping, err := h.objects.GetByAPID(background, pageID)
	require.NoError(t, err)
	assert.True(t, mapping.IsDeleted(), "a cancelled walk must still remove a post deleted upstream")
}

// committedPostOutcome reports a post's real commit as a later failure would:
// outcome replaces what the materializer returned.
type committedPostOutcome struct {
	Materializer

	outcome func(committed *materialize.Result) (*materialize.Result, error)
}

func (m *committedPostOutcome) MaterializePost(ctx context.Context, page *ap.Object, communityIRI string) (*materialize.Result, error) {
	committed, err := m.Materializer.MaterializePost(ctx, page, communityIRI)
	if err != nil {
		return committed, err
	}
	return m.outcome(committed)
}

// TestBackfill_DeleteDuringHeldMintThenFailedAcceptance: the Delete lands while
// the post's mint waits, the post commits, and its acceptance fails. Later walks
// skip the id, so this walk must remove the post. Only a post that committed may
// be removed: a refusal before the commit can concern a mapping in another
// community, and HandleDeleteRecord is not scoped to one.
func TestBackfill_DeleteDuringHeldMintThenFailedAcceptance(t *testing.T) {
	failure := stderrors.New("materialize: accept post: repo unavailable")

	for _, tc := range []struct {
		name            string
		markerCommunity string
		// reportsCommit is whether the materializer returns the post beside
		// its error, which it does for an acceptance failure.
		reportsCommit bool
		// cancels ends the walk's context as the acceptance fails.
		cancels bool
		deleted bool
	}{
		{name: "acceptance error", markerCommunity: groupID, reportsCommit: true, deleted: true},
		{name: "walk cancelled during acceptance", markerCommunity: groupID, reportsCommit: true, cancels: true, deleted: true},
		{name: "acceptance error with another community's marker", markerCommunity: gamingGroupID, reportsCommit: true},
		{name: "refused before the commit", markerCommunity: groupID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.subscribeTechnology()
			serveOutboxFixtures(t, h)
			materializer, fixture := newBudgetedMaterializer(t, h)
			background := context.Background()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			b := newBackfillOver(t, h, &committedPostOutcome{
				Materializer: materializer,
				outcome: func(committed *materialize.Result) (*materialize.Result, error) {
					if tc.cancels {
						cancel()
					}
					if tc.reportsCommit {
						return committed, failure
					}
					return nil, failure
				},
			}, 1)
			community, err := h.communities.GetByAPGroupID(background, groupID)
			require.NoError(t, err)
			require.NoError(t, fixture.budget.Wait(ctx), "a fresh budget admits one write")

			run := startCall("Run", func() error { return b.Run(ctx, community, true) })
			t.Cleanup(func() { <-run.finished })
			fixture.clock.AwaitWaiters(run)
			requireStillBlocked(t, run, "the walk holds on the post's mint")
			require.NoError(t, h.tombstones.Record(background, pageID, tc.markerCommunity))

			for step := 0; step < 10 && !run.returned(); step++ {
				fixture.clock.Advance(12 * time.Second)
				fixture.clock.AwaitWaiters(run)
			}
			require.True(t, run.returned(), "the walk is still blocked after 2 minutes of clock")
			require.Error(t, run.result(t), "the original failure still fails the walk")

			mapping, err := h.objects.GetByAPID(background, pageID)
			require.NoError(t, err)
			assert.Equal(t, tc.deleted, mapping.IsDeleted())
			assert.Nil(t, h.lastBackfillAt(groupID), "a failed item leaves the walk unfinished")
		})
	}
}

// reportedAncestors returns a scripted comment outcome and fails the first
// removal it is asked for.
type reportedAncestors struct {
	Materializer

	ancestors []string
	outcome   error
	failure   error
	removals  []string
}

func (m *reportedAncestors) MaterializeComment(context.Context, *ap.Object, string) (*materialize.Result, error) {
	return &materialize.Result{Ancestors: m.ancestors}, m.outcome
}

func (m *reportedAncestors) HandleDeleteRecord(_ context.Context, apID string) error {
	m.removals = append(m.removals, apID)
	if len(m.removals) == 1 {
		return m.failure
	}
	return nil
}

// TestBackfill_FailedAncestorRemovalFailsTheItem: a skipped comment says nothing
// about an ancestor it committed, so a failed removal must keep the walk
// unfinished, and must not cost the other ancestors theirs.
func TestBackfill_FailedAncestorRemovalFailsTheItem(t *testing.T) {
	const (
		noteID  = "https://lemmy.world/comment/3002"
		otherID = "https://lemmy.world/comment/3004"
	)

	for _, tc := range []struct {
		name    string
		outcome error
	}{
		{name: "comment committed"},
		{name: "comment skipped", outcome: skip(noteID, "refused for its author")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.subscribeTechnology()
			serveOutboxFixtures(t, h)
			serveOutboxOfOneNote(h, noteID, "https://lemmy.world/u/replier")
			ctx := context.Background()
			for _, id := range []string{unmappedParentID, otherID} {
				require.NoError(t, h.tombstones.Record(ctx, id, groupID))
			}
			materializer := &reportedAncestors{
				Materializer: h.mat,
				ancestors:    []string{unmappedParentID, otherID},
				outcome:      tc.outcome,
				failure:      stderrors.New("repo unavailable"),
			}
			community, err := h.communities.GetByAPGroupID(ctx, groupID)
			require.NoError(t, err)

			require.Error(t, newBackfillOver(t, h, materializer, 1).Run(ctx, community, true))

			assert.Equal(t, []string{unmappedParentID, otherID}, materializer.removals)
			assert.Nil(t, h.lastBackfillAt(groupID), "a failed removal leaves the walk unfinished")
		})
	}
}

// parsedPage builds a minimal Page in the technology community.
func parsedPage(t *testing.T, id, author string) *ap.Object {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"type":         "Page",
		"id":           id,
		"attributedTo": author,
		"to":           []any{groupID, ap.PublicAudience},
		"audience":     groupID,
		"name":         "a post at " + id,
		"source":       map[string]any{"content": "body of " + id, "mediaType": "text/markdown"},
		"published":    "2026-07-09T14:00:00.000000Z",
	})
	require.NoError(t, err)
	page, err := ap.ParseObject(raw)
	require.NoError(t, err)
	return page
}

// TestMaterialize_WaitingMintsOfOneAuthorShareOneDID: both callers passed the
// actor check before either minted. PLC DIDs are permanent, so the second must
// look again once its token arrives instead of orphaning a DID.
func TestMaterialize_WaitingMintsOfOneAuthorShareOneDID(t *testing.T) {
	const authorID = "https://lemmy.world/u/sharedauthor"
	postIDs := []string{"https://lemmy.world/post/7100001", "https://lemmy.world/post/7100002"}

	h := newHarness(t)
	h.subscribeTechnology()
	h.serveObject("/u/sharedauthor", person(authorID, "sharedauthor", nil))
	materializer, fixture := newBudgetedMaterializer(t, h)
	ctx := t.Context()
	require.NoError(t, fixture.budget.Wait(ctx), "a fresh budget admits one write")

	calls := make([]*blockingCall, len(postIDs))
	for index, id := range postIDs {
		page := parsedPage(t, id, authorID)
		calls[index] = startCall(fmt.Sprintf("MaterializePost %d", index), func() error {
			_, err := materializer.MaterializePost(withBudgetWait(ctx), page, groupID)
			return err
		})
	}
	first, second := calls[0], calls[1]
	fixture.clock.AwaitWaiters(first, second)
	requireStillBlocked(t, first, "both callers wait in the budget")
	requireStillBlocked(t, second, "both callers wait in the budget")

	fixture.clock.Advance(12 * time.Second)
	awaitCondition(t, func() bool { return first.returned() || second.returned() }, "want the winner to return")
	fixture.clock.AwaitWaiters(first, second)
	require.NotEqual(t, first.returned(), second.returned(), "one token admits one caller")

	fixture.clock.Advance(12 * time.Second)
	require.NoError(t, first.result(t))
	require.NoError(t, second.result(t))

	posts := fixture.plc.recordedPosts()
	require.Len(t, posts, 1, "the second caller reuses the first caller's DID")
	row, err := h.actors.GetByAPActorID(ctx, authorID)
	require.NoError(t, err)
	assert.Equal(t, "/"+row.DID, posts[0].path)
	for _, id := range postIDs {
		mapping, err := h.objects.GetByAPID(ctx, id)
		require.NoError(t, err, "%s must be materialized", id)
		assert.Equal(t, row.DID, mapping.AuthorDID, "author of %s", id)
	}
}

// queuedWindow bounds a look for something that must not happen. A queued walk
// gives no signal to wait on, so this cannot be a channel. It orders nothing,
// and correct code passes it however slow the machine is.
const queuedWindow = 50 * time.Millisecond

// gatedWalkFetcher holds every walk in FetchActor until the test releases it.
// The outbox is empty, so a released walk reaches SetLastBackfill.
type gatedWalkFetcher struct {
	emptyOutbox

	mutex   sync.Mutex
	entries map[string]int
	// held maps each waiting walk's gate to its group.
	held map[chan struct{}]string
	// open lets every later walk through.
	open       bool
	live, peak int
}

func (f *gatedWalkFetcher) FetchActor(ctx context.Context, iri string) (*ap.Object, error) {
	gate := make(chan struct{})
	f.mutex.Lock()
	f.entries[iri]++
	f.live++
	f.peak = max(f.peak, f.live)
	if f.open {
		close(gate)
	} else {
		f.held[gate] = iri
	}
	f.mutex.Unlock()
	defer func() {
		f.mutex.Lock()
		defer f.mutex.Unlock()
		f.live--
		delete(f.held, gate)
	}()
	select {
	case <-gate:
		return &ap.Object{ID: iri, Type: ap.TypeGroup, Outbox: iri + "/outbox"}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// releaseOne lets one held walk through.
func (f *gatedWalkFetcher) releaseOne(t *testing.T) {
	t.Helper()
	f.mutex.Lock()
	defer f.mutex.Unlock()
	require.NotEmpty(t, f.held, "no walk is held")
	for gate := range f.held {
		delete(f.held, gate)
		close(gate)
		return
	}
}

// release lets the held walks of one community through.
func (f *gatedWalkFetcher) release(t *testing.T, iri string) {
	t.Helper()
	f.mutex.Lock()
	defer f.mutex.Unlock()
	released := 0
	for gate, heldIRI := range f.held {
		if heldIRI == iri {
			delete(f.held, gate)
			close(gate)
			released++
		}
	}
	require.NotZero(t, released, "no walk of %s is held", iri)
}

func (f *gatedWalkFetcher) releaseAll() {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.open = true
	for gate := range f.held {
		delete(f.held, gate)
		close(gate)
	}
}

func (f *gatedWalkFetcher) entriesFor(iri string) int {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return f.entries[iri]
}

func (f *gatedWalkFetcher) entryTotal() int {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	total := 0
	for _, count := range f.entries {
		total += count
	}
	return total
}

func (f *gatedWalkFetcher) peakWalks() int {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return f.peak
}

func (f *gatedWalkFetcher) awaitEntries(t *testing.T, want int) {
	t.Helper()
	awaitCondition(t, func() bool { return f.entryTotal() >= want }, "want %d walks started", want)
}

// requireEntriesHold asserts the count stays at want for queuedWindow.
func (f *gatedWalkFetcher) requireEntriesHold(t *testing.T, want int) {
	t.Helper()
	window := time.After(queuedWindow)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		require.Equal(t, want, f.entryTotal(), "walks started while every slot is held")
		select {
		case <-tick.C:
		case <-window:
			return
		}
	}
}

// newBackfillFetching is the harness Backfill over another fetcher, with a
// base context the test owns.
func newBackfillFetching(t *testing.T, h *harness, fetcher BackfillFetcher, base context.Context) *Backfill {
	t.Helper()
	options := h.backfillOptions(h.mat, 100)
	options.Fetcher = fetcher
	options.BaseContext = base
	options.Logger = slog.New(slog.DiscardHandler)
	b, err := NewBackfill(options)
	require.NoError(t, err)
	return b
}

// lastBackfillAt reads the stored stamp.
func (h *harness) lastBackfillAt(iri string) *time.Time {
	h.t.Helper()
	community, err := h.communities.GetByAPGroupID(context.Background(), iri)
	require.NoError(h.t, err)
	return community.LastBackfillAt
}

// queueRig is a real Backfill over gatedWalkFetcher and real accepted
// communities, so the stamps are real.
type queueRig struct {
	h           *harness
	fetcher     *gatedWalkFetcher
	backfill    *Backfill
	cancelBase  context.CancelFunc
	communities []*store.Community
	triggers    []*blockingCall
}

func newQueueRig(t *testing.T, communityCount int) *queueRig {
	t.Helper()
	h := newHarness(t)
	ctx := context.Background()
	base, cancelBase := context.WithCancel(ctx)
	rig := &queueRig{
		h:          h,
		fetcher:    &gatedWalkFetcher{entries: map[string]int{}, held: map[chan struct{}]string{}},
		cancelBase: cancelBase,
	}
	rig.backfill = newBackfillFetching(t, h, rig.fetcher, base)
	for index := range communityCount {
		username := fmt.Sprintf("queued%d", index)
		id := "https://lemmy.world/c/" + username
		_, err := h.communities.UpsertCommunity(ctx, store.Community{
			APGroupID:         id,
			DID:               testDIDFor(username, "lemmy.world"),
			PreferredUsername: username,
			Instance:          "lemmy.world",
		})
		require.NoError(t, err)
		require.NoError(t, h.communities.SetFollowState(ctx, id, store.FollowStateAccepted))
		community, err := h.communities.GetByAPGroupID(ctx, id)
		require.NoError(t, err)
		rig.communities = append(rig.communities, community)
	}
	// Runs before the harness's own cleanup, so a failed test leaves no walk
	// or trigger running against the database.
	t.Cleanup(func() {
		cancelBase()
		rig.fetcher.releaseAll()
		for _, trigger := range rig.triggers {
			<-trigger.finished
		}
		rig.backfill.Wait()
	})
	return rig
}

// trigger calls TriggerAsync for each community, in order, and fails at the
// stall deadline if a call blocks.
func (r *queueRig) trigger(t *testing.T, communities ...*store.Community) {
	t.Helper()
	call := startCall("TriggerAsync", func() error {
		for _, community := range communities {
			r.backfill.TriggerAsync(community, false)
		}
		return nil
	})
	r.triggers = append(r.triggers, call)
	require.NoError(t, call.result(t))
}

func (r *queueRig) drain(t *testing.T) {
	t.Helper()
	require.NoError(t, startCall("Wait", func() error {
		r.backfill.Wait()
		return nil
	}).result(t))
}

// TestBackfillTrigger_BoundsConcurrentWalks releases one walk at a time, each
// letting one queued walk start, so the walks stay at 4 until the queue is empty.
func TestBackfillTrigger_BoundsConcurrentWalks(t *testing.T) {
	rig := newQueueRig(t, 10)

	rig.trigger(t, rig.communities...)
	rig.fetcher.awaitEntries(t, 4)
	rig.fetcher.requireEntriesHold(t, 4)

	for released := 1; released <= 10; released++ {
		rig.fetcher.releaseOne(t)
		rig.fetcher.awaitEntries(t, min(4+released, 10))
	}
	rig.drain(t)

	assert.Equal(t, 4, rig.fetcher.peakWalks(), "peak concurrent walks")
	assert.Equal(t, 10, rig.fetcher.entryTotal(), "walks started")
	for _, community := range rig.communities {
		assert.NotNil(t, rig.h.lastBackfillAt(community.APGroupID), "%s must stamp", community.APGroupID)
	}
}

// TestBackfillTrigger_CancelEmptiesTheQueue: once the base context ends a
// queued community must not start, and a walk that stopped early must not stamp.
func TestBackfillTrigger_CancelEmptiesTheQueue(t *testing.T) {
	rig := newQueueRig(t, 10)

	rig.trigger(t, rig.communities...)
	rig.fetcher.awaitEntries(t, 4)

	rig.cancelBase()
	rig.drain(t)

	assert.Equal(t, 4, rig.fetcher.entryTotal(), "walks started")
	for _, community := range rig.communities {
		assert.Nil(t, rig.h.lastBackfillAt(community.APGroupID), "%s must not stamp", community.APGroupID)
	}
}

// TestBackfillTrigger_QueuedCommunityWalksOnce: a second trigger queued beside
// the first would take the slot the first one frees, and no running marker
// would stop it, so the release order leaves that slot free for it.
func TestBackfillTrigger_QueuedCommunityWalksOnce(t *testing.T) {
	rig := newQueueRig(t, 5)
	holders, fifth := rig.communities[:4], rig.communities[4]

	rig.trigger(t, holders...)
	rig.fetcher.awaitEntries(t, 4)
	rig.trigger(t, fifth)
	rig.trigger(t, fifth)
	rig.fetcher.requireEntriesHold(t, 4)

	rig.fetcher.release(t, holders[0].APGroupID)
	rig.fetcher.awaitEntries(t, 5)
	rig.fetcher.release(t, fifth.APGroupID)
	rig.fetcher.releaseAll()
	rig.drain(t)

	assert.Equal(t, 1, rig.fetcher.entriesFor(fifth.APGroupID), "walks of the twice-triggered community")
}

func TestBackfillTrigger_WalkingCommunityWalksOnce(t *testing.T) {
	rig := newQueueRig(t, 1)
	community := rig.communities[0]

	rig.trigger(t, community)
	rig.fetcher.awaitEntries(t, 1)
	rig.trigger(t, community)
	rig.fetcher.releaseAll()
	rig.drain(t)

	assert.Equal(t, 1, rig.fetcher.entriesFor(community.APGroupID), "walks")
}

// TestBackfillTrigger_QueuedWalkRereadsItsCommunity: the row changes while the
// walk waits for a slot. The snapshot taken at the trigger still says walk.
func TestBackfillTrigger_QueuedWalkRereadsItsCommunity(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(ctx context.Context, communities store.Communities, id string) error
	}{
		{
			name: "unsubscribed",
			change: func(ctx context.Context, communities store.Communities, id string) error {
				return communities.SetFollowState(ctx, id, store.FollowStateNone)
			},
		},
		{
			name: "already backfilled",
			change: func(ctx context.Context, communities store.Communities, id string) error {
				return communities.SetLastBackfill(ctx, id, time.Now())
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newQueueRig(t, 5)
			holders, queued := rig.communities[:4], rig.communities[4]

			rig.trigger(t, holders...)
			rig.fetcher.awaitEntries(t, 4)
			rig.trigger(t, queued)
			require.NoError(t, tc.change(context.Background(), rig.h.communities, queued.APGroupID))

			rig.fetcher.releaseAll()
			rig.drain(t)

			assert.Equal(t, 0, rig.fetcher.entriesFor(queued.APGroupID), "walks of the changed community")
			assert.Equal(t, 4, rig.fetcher.entryTotal(), "the holders still walked")
			rig.backfill.mu.Lock()
			defer rig.backfill.mu.Unlock()
			assert.NotContains(t, rig.backfill.retries, queued.APGroupID, "a dropped walk leaves the retry state alone")
		})
	}
}

// TestBackfillTrigger_QueuedForcedWalkIgnoresTheFollowState: the admin endpoint
// walks a community whatever its state, queued or not.
func TestBackfillTrigger_QueuedForcedWalkIgnoresTheFollowState(t *testing.T) {
	rig := newQueueRig(t, 5)
	holders, queued := rig.communities[:4], rig.communities[4]

	rig.trigger(t, holders...)
	rig.fetcher.awaitEntries(t, 4)
	rig.backfill.TriggerAsync(queued, true)
	require.NoError(t, rig.h.communities.SetFollowState(context.Background(), queued.APGroupID, store.FollowStateNone))

	rig.fetcher.releaseAll()
	rig.drain(t)

	assert.Equal(t, 1, rig.fetcher.entriesFor(queued.APGroupID), "walks of the forced community")
}

// groupOutcomeFetcher is the real client with walks counted per group at
// FetchActor, where a no-outbox group's walk ends, and the group fetch forced
// to fail or to advertise no outbox, or the outbox fetch forced to fail.
type groupOutcomeFetcher struct {
	BackfillFetcher

	mutex         sync.Mutex
	entries       map[string]int
	failing       bool
	noOutbox      bool
	outboxFailing bool
}

func (f *groupOutcomeFetcher) FetchActor(ctx context.Context, iri string) (*ap.Object, error) {
	f.mutex.Lock()
	f.entries[iri]++
	failing, noOutbox := f.failing, f.noOutbox
	f.mutex.Unlock()
	if failing {
		return nil, stderrors.New("group fetch answered 500")
	}
	group, err := f.BackfillFetcher.FetchActor(ctx, iri)
	if err != nil || !noOutbox {
		return group, err
	}
	bare := *group
	bare.Outbox = ""
	return &bare, nil
}

func (f *groupOutcomeFetcher) FetchCollection(ctx context.Context, iri string, visit func(*ap.Object) error) error {
	f.mutex.Lock()
	outboxFailing := f.outboxFailing
	f.mutex.Unlock()
	if outboxFailing {
		return stderrors.New("outbox fetch answered 500")
	}
	return f.BackfillFetcher.FetchCollection(ctx, iri, visit)
}

func (f *groupOutcomeFetcher) setFailing(failing bool) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.failing = failing
}

func (f *groupOutcomeFetcher) setNoOutbox(noOutbox bool) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.noOutbox = noOutbox
}

func (f *groupOutcomeFetcher) setOutboxFailing(failing bool) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.outboxFailing = failing
}

func (f *groupOutcomeFetcher) walksOf(iri string) int {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return f.entries[iri]
}

// newRetryingReconciler is newSwitchedReconciler with a real retrier.
func (h *harness) newRetryingReconciler(path string, backfill BackfillRetrier) *FollowReconciler {
	h.t.Helper()
	rec, err := NewFollowReconciler(FollowReconcilerOptions{
		Admin: h.admin, Path: path, Backfill: backfill, Lister: noCommunitiesLister{}, Posts: noPostsReader{},
		Budget: unlimitedBudget(h.t),
	})
	require.NoError(h.t, err)
	return rec
}

// sweepRig runs a real reconciler over a real Backfill on a manual clock.
type sweepRig struct {
	clock      *manualClock
	fetcher    *groupOutcomeFetcher
	backfill   *Backfill
	reconciler *FollowReconciler
}

func newSweepRig(t *testing.T, h *harness, followList string) *sweepRig {
	t.Helper()
	rig := &sweepRig{
		clock:   newManualClock(t),
		fetcher: &groupOutcomeFetcher{BackfillFetcher: h.client, entries: map[string]int{}},
	}
	rig.backfill = newBackfillFetching(t, h, rig.fetcher, context.Background())
	rig.backfill.now = rig.clock.Now
	rig.reconciler = h.newRetryingReconciler(writeFollowList(t, followList), rig.backfill)
	return rig
}

// sweep runs one reconcile pass and drains the walks it started.
func (r *sweepRig) sweep(t *testing.T) SweepResult {
	t.Helper()
	result, err := r.reconciler.Sweep(context.Background())
	require.NoError(t, err)
	require.Empty(t, result.Failed)
	r.backfill.Wait()
	return result
}

// sweepAt is one sweep in a schedule: its offset from the end of the first
// walk, the walks it must start, and whether the community is stamped after it.
type sweepAt struct {
	offset  time.Duration
	walks   int
	stamped bool
}

// TestSweep_RetriesUnfinishedBackfills is the acceptance test for retries. The
// first row recovers ("failed backfills retry"), and the stamp ends its retries.
// The next four never finish, each reaching a different exit of the walk. The
// truncated row is a clean walk that leaves the stamp unset, so its next window
// doubles.
func TestSweep_RetriesUnfinishedBackfills(t *testing.T) {
	for _, tc := range []struct {
		name string
		// arrange makes the first walk come out unfinished.
		arrange func(t *testing.T, h *harness, fetcher *groupOutcomeFetcher)
		// afterFirstWalk, when set, runs once the first walk has ended.
		afterFirstWalk func(fetcher *groupOutcomeFetcher)
		sweeps         []sweepAt
	}{
		{
			name: "failed walk recovers",
			arrange: func(_ *testing.T, _ *harness, fetcher *groupOutcomeFetcher) {
				fetcher.setFailing(true)
			},
			afterFirstWalk: func(fetcher *groupOutcomeFetcher) { fetcher.setFailing(false) },
			sweeps: []sweepAt{
				{offset: 59*time.Minute + 59*time.Second, walks: 0},
				{offset: time.Hour, walks: 1, stamped: true},
				{offset: 72 * time.Hour, walks: 0, stamped: true},
			},
		},
		{
			name: "failed walk keeps failing",
			arrange: func(_ *testing.T, _ *harness, fetcher *groupOutcomeFetcher) {
				fetcher.setFailing(true)
			},
			sweeps: []sweepAt{
				{offset: 59*time.Minute + 59*time.Second, walks: 0},
				{offset: time.Hour, walks: 1},
				{offset: time.Hour, walks: 0},
			},
		},
		{
			name: "outbox fetch fails",
			arrange: func(_ *testing.T, _ *harness, fetcher *groupOutcomeFetcher) {
				fetcher.setOutboxFailing(true)
			},
			sweeps: []sweepAt{
				{offset: 59*time.Minute + 59*time.Second, walks: 0},
				{offset: time.Hour, walks: 1},
				{offset: time.Hour, walks: 0},
			},
		},
		{
			name: "item fails",
			arrange: func(t *testing.T, h *harness, _ *groupOutcomeFetcher) {
				serveBrokenItemOutbox(t, h)
			},
			sweeps: []sweepAt{
				{offset: 59*time.Minute + 59*time.Second, walks: 0},
				{offset: time.Hour, walks: 1},
				{offset: time.Hour, walks: 0},
			},
		},
		{
			name: "group advertises no outbox",
			arrange: func(_ *testing.T, _ *harness, fetcher *groupOutcomeFetcher) {
				fetcher.setNoOutbox(true)
			},
			sweeps: []sweepAt{
				{offset: 59*time.Minute + 59*time.Second, walks: 0},
				{offset: time.Hour, walks: 1},
				{offset: time.Hour, walks: 0},
			},
		},
		{
			name: "truncated walk",
			arrange: func(t *testing.T, h *harness, _ *groupOutcomeFetcher) {
				serveTruncatingOutbox(t, h)
			},
			sweeps: []sweepAt{
				{offset: 59*time.Minute + 59*time.Second, walks: 0},
				{offset: time.Hour, walks: 1},
				{offset: time.Hour, walks: 0},
				{offset: time.Hour + 2*time.Hour - time.Second, walks: 0},
				{offset: time.Hour + 2*time.Hour, walks: 1},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.subscribeTechnology()
			serveOutboxFixtures(t, h)
			rig := newSweepRig(t, h, `
communities:
  - "!technology@lemmy.world"
`)
			tc.arrange(t, h, rig.fetcher)

			community, err := h.communities.GetByAPGroupID(context.Background(), groupID)
			require.NoError(t, err)
			rig.backfill.TriggerAsync(community, false)
			rig.backfill.Wait()
			require.Equal(t, 1, rig.fetcher.walksOf(groupID), "the first walk")
			require.Nil(t, h.lastBackfillAt(groupID), "the first walk must leave the community unstamped")
			if tc.afterFirstWalk != nil {
				tc.afterFirstWalk(rig.fetcher)
			}

			var elapsed time.Duration
			for _, sweep := range tc.sweeps {
				rig.clock.Advance(sweep.offset - elapsed)
				elapsed = sweep.offset

				before := rig.fetcher.walksOf(groupID)
				rig.sweep(t)
				require.Equal(t, sweep.walks, rig.fetcher.walksOf(groupID)-before, "walks from the sweep at %s", sweep.offset)
				require.Equal(t, sweep.stamped, h.lastBackfillAt(groupID) != nil, "stamped after the sweep at %s", sweep.offset)
			}
		})
	}
}

// TestSweep_RetriesOnlyAcceptedUnstampedListedCommunities: the unfinished
// community is the control. It shows the sweep does retry, so the zeros count.
func TestSweep_RetriesOnlyAcceptedUnstampedListedCommunities(t *testing.T) {
	const (
		unfinishedGroupID = "https://lemmy.world/c/unfinished"
		departingGroupID  = "https://lemmy.world/c/departing"
	)
	ctx := context.Background()
	h := newHarness(t)
	h.subscribeTechnology()
	h.subscribeCommunityURL(unfinishedGroupID, "unfinished")
	h.subscribeCommunityURL(departingGroupID, "departing")
	h.subscribeCommunityURL(gamingGroupID, "gaming")
	require.NoError(t, h.communities.SetFollowState(ctx, gamingGroupID, store.FollowStatePending))
	require.NoError(t, h.communities.SetLastBackfill(ctx, groupID, time.Now().Add(-2*time.Hour)),
		"older than the 1 hour freshness window, so a Run would really walk")

	rig := newSweepRig(t, h, `
communities:
  - "!technology@lemmy.world"
  - "https://lemmy.world/c/gaming"
  - "https://lemmy.world/c/unfinished"
`)

	result := rig.sweep(t)

	assert.Equal(t, []string{departingGroupID}, result.Unsubscribed)
	departing, err := h.communities.GetByAPGroupID(ctx, departingGroupID)
	require.NoError(t, err)
	assert.Equal(t, store.FollowStateNone, departing.FollowState)

	assert.Equal(t, 0, rig.fetcher.walksOf(groupID), "accepted and stamped")
	assert.Equal(t, 0, rig.fetcher.walksOf(gamingGroupID), "pending")
	assert.Equal(t, 0, rig.fetcher.walksOf(departingGroupID), "unfollowed by this sweep")
	assert.Equal(t, 1, rig.fetcher.walksOf(unfinishedGroupID), "accepted, unstamped and listed")
}

// TestSweep_FailedEntryStillRetriesUnfinishedBackfills: a list entry that stays
// refused, such as a #nobridge community, must not stop the retries of the rest
// of the file.
func TestSweep_FailedEntryStillRetriesUnfinishedBackfills(t *testing.T) {
	const unfinishedGroupID = "https://lemmy.world/c/unfinished"
	h := newHarness(t)
	h.subscribeCommunityURL(unfinishedGroupID, "unfinished")

	rig := newSweepRig(t, h, `
communities:
  - "!unresolvable@lemmy.world"
  - "https://lemmy.world/c/unfinished"
`)

	result, err := rig.reconciler.Sweep(context.Background())
	require.NoError(t, err)
	rig.backfill.Wait()

	assert.Equal(t, []string{"!unresolvable@lemmy.world"}, result.Failed)
	assert.Equal(t, 1, rig.fetcher.walksOf(unfinishedGroupID), "accepted, unstamped and listed")
}
