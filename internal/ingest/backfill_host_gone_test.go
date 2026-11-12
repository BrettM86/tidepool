package ingest

import (
	"context"
	stderrors "errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// seedHostGoneFailures records host-gone failures for host at each offset
// before backfillNow (oldest first) and requires they form one streak that
// began at the oldest.
func seedHostGoneFailures(t *testing.T, h *harness, host string, agos ...time.Duration) {
	t.Helper()
	ctx := context.Background()
	for _, ago := range agos {
		goneSince, err := h.materializerOptions.Instances.RecordHostGone(ctx, host, backfillNow.Add(-ago))
		require.NoError(t, err)
		require.True(t, backfillNow.Add(-agos[0]).Equal(goneSince),
			"seeded streak for %s starts %v before the backfill clock, got %v", host, agos[0], goneSince)
	}
}

// TestBackfillHostGoneDefersWhateverTheStreak: a host-gone outbox item is
// deferred on the walk that meets it, whatever the age of its host's streak
// (none, 48h, exactly DeadHostAfter): the run succeeds, stamps
// last_backfill_at, writes one deferred row first due 1h out, and records the
// failure into the host's streak.
func TestBackfillHostGoneDefersWhateverTheStreak(t *testing.T) {
	tests := []struct {
		name string
		// agos are the seeded failures before backfillNow, oldest first; each
		// gap is under store.HostGoneStreakGap, so they form one streak.
		agos []time.Duration
		// wantSince is other.test's streak start after the walk.
		wantSince time.Time
	}{
		{
			name:      "no prior streak",
			agos:      nil,
			wantSince: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC),
		},
		{
			name:      "48h-old streak",
			agos:      []time.Duration{48 * time.Hour, 24 * time.Hour},
			wantSince: time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC),
		},
		{
			name:      "streak exactly 72h old",
			agos:      []time.Duration{72 * time.Hour, 36 * time.Hour},
			wantSince: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.subscribeTechnology()
			serveOutboxFixtures(t, h)
			serveBrokenItemOutbox(t, h)
			if len(tt.agos) > 0 {
				seedHostGoneFailures(t, h, "other.test", tt.agos...)
			}
			opts := h.backfillOptions(h.mat, 100)
			opts.Fetcher = hostGoneFetcher{BackfillFetcher: h.client, goneIRI: "https://other.test/post/broken"}
			b, err := NewBackfill(opts)
			require.NoError(t, err)
			b.now = func() time.Time { return backfillNow }
			ctx := context.Background()

			community, err := h.communities.GetByAPGroupID(ctx, groupID)
			require.NoError(t, err)
			runErr := b.Run(ctx, community, true)

			_, err = h.objects.GetByAPID(ctx, pageID)
			require.NoError(t, err, "the healthy item still materializes")
			assert.NoError(t, runErr, "a host-gone item is deferred, not a run error")
			assert.NotNil(t, h.lastBackfillAt(groupID), "a walk whose only failure is a deferred host-gone item stamps last_backfill_at")
			// The broken item's embedded object carries no type.
			assert.Equal(t, walkDeferredRows(h, "other.test", [2]string{"https://other.test/post/broken", ""}),
				deferredRows(t, h), "the host-gone item is deferred, first due 1h after the walk")
			goneSince, err := h.materializerOptions.Instances.RecordHostGone(ctx, "other.test", backfillNow.Add(time.Hour))
			require.NoError(t, err)
			assert.True(t, tt.wantSince.Equal(goneSince), "other.test streak start = %v, want %v", goneSince, tt.wantSince)
		})
	}
}

// TestBackfillHostGoneReply: a reply in a post's replies collection whose
// origin fetch fails host-gone is deferred on the walk that meets it, whatever
// its host's streak: the run succeeds and stamps last_backfill_at, the reply
// gets one deferred row, and the failure is recorded into the host's streak.
func TestBackfillHostGoneReply(t *testing.T) {
	const (
		goneReply = "https://other.test/comment/77"
		goneHost  = "other.test"
	)
	tests := []struct {
		name      string
		seedDead  bool
		wantSince time.Time
	}{
		{name: "no prior streak", seedDead: false, wantSince: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)},
		{name: "dead streak", seedDead: true, wantSince: time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.subscribeTechnology()
			serveOutboxFixtures(t, h)
			// Replace the advertised replies collection with one reply whose id
			// is cross-authority, so the walk re-fetches it from other.test.
			h.serveObject("/post/49131386/replies", map[string]any{
				"type":         "OrderedCollection",
				"id":           pageID + "/replies",
				"totalItems":   1,
				"orderedItems": []any{note(goneReply, "https://other.test/u/ghost", pageID, "a reply from a gone host", "2026-07-07T05:00:00.000000Z")},
			})
			if tt.seedDead {
				seedDeadHostStreak(t, h, goneHost)
			}
			opts := h.backfillOptions(h.mat, 100)
			opts.Fetcher = hostGoneFetcher{BackfillFetcher: h.client, goneIRI: goneReply}
			b, err := NewBackfill(opts)
			require.NoError(t, err)
			b.now = func() time.Time { return backfillNow }
			ctx := context.Background()

			community, err := h.communities.GetByAPGroupID(ctx, groupID)
			require.NoError(t, err)
			runErr := b.Run(ctx, community, true)

			_, err = h.objects.GetByAPID(ctx, pageID)
			require.NoError(t, err, "the post carrying the replies collection still materializes")
			_, err = h.objects.GetByAPID(ctx, goneReply)
			assert.True(t, errors.IsNotFound(err), "the host-gone reply is not materialized")
			goneSince, err := h.materializerOptions.Instances.RecordHostGone(ctx, goneHost, backfillNow.Add(time.Hour))
			require.NoError(t, err)
			assert.True(t, tt.wantSince.Equal(goneSince), "%s streak start = %v, want %v", goneHost, goneSince, tt.wantSince)
			assert.NoError(t, runErr, "a host-gone reply is deferred, not a run error")
			assert.NotNil(t, h.lastBackfillAt(groupID), "a walk whose only failure is a deferred reply stamps last_backfill_at")
			assert.Equal(t, walkDeferredRows(h, goneHost, [2]string{goneReply, "Note"}), deferredRows(t, h),
				"the host-gone reply is deferred, first due 1h after the walk")
		})
	}
}

// repliesCollectionGoneFetcher fails the walk of the replies collection at
// goneIRI host-gone (naming other.test). With afterFirstPage set it first
// walks the real collection, as a later page failing after the first landed.
type repliesCollectionGoneFetcher struct {
	BackfillFetcher
	goneIRI        string
	afterFirstPage bool
}

func (f repliesCollectionGoneFetcher) FetchCollection(ctx context.Context, iri string, visit func(*ap.Object) error) error {
	if iri != f.goneIRI {
		return f.BackfillFetcher.FetchCollection(ctx, iri, visit)
	}
	if f.afterFirstPage {
		if err := f.BackfillFetcher.FetchCollection(ctx, iri, visit); err != nil {
			return err
		}
	}
	return hostGoneErr(iri+"?page=2", "other.test")
}

// TestBackfillHostGoneRepliesCollection: a post's replies collection (or a
// later page of it) whose fetch fails host-gone does not fail the walk,
// whatever the host's streak: the run succeeds, stamps last_backfill_at,
// records the failure into the host's streak, and defers the post itself
// under the collection's host, so its replies are not lost. A sweep while the
// collection still fails keeps the row, rescheduled like any host-gone retry;
// a later sweep, with the collection served again, re-fetches the post, lands
// its replies and removes the row.
func TestBackfillHostGoneRepliesCollection(t *testing.T) {
	const (
		goneHost = "other.test"
		replyID  = "https://lemmy.world/comment/3001"
	)
	repliesIRI := pageID + "/replies"
	tests := []struct {
		name           string
		afterFirstPage bool
		seedDead       bool
		wantSince      time.Time
	}{
		{name: "no prior streak: the collection fetch", wantSince: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)},
		{name: "no prior streak: a later page", afterFirstPage: true, wantSince: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)},
		{name: "dead streak: the collection fetch", seedDead: true, wantSince: time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)},
		{name: "dead streak: a later page", afterFirstPage: true, seedDead: true, wantSince: time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.subscribeTechnology()
			serveOutboxFixtures(t, h)
			// The post's origin copy advertises the same replies collection,
			// so the sweep's re-fetch of the post walks it.
			page := loadFixture(t, "page_lemmy_world.json")
			page["replies"] = repliesIRI
			h.serveObject("/post/49131386", page)
			if tt.seedDead {
				seedDeadHostStreak(t, h, goneHost)
			}
			opts := h.backfillOptions(h.mat, 100)
			opts.Fetcher = repliesCollectionGoneFetcher{BackfillFetcher: h.client, goneIRI: repliesIRI, afterFirstPage: tt.afterFirstPage}
			b, err := NewBackfill(opts)
			require.NoError(t, err)
			b.now = func() time.Time { return backfillNow }
			ctx := context.Background()

			community, err := h.communities.GetByAPGroupID(ctx, groupID)
			require.NoError(t, err)
			runErr := b.Run(ctx, community, true)

			_, err = h.objects.GetByAPID(ctx, pageID)
			require.NoError(t, err, "the post carrying the replies collection still materializes")
			goneSince, err := h.materializerOptions.Instances.RecordHostGone(ctx, goneHost, backfillNow.Add(time.Hour))
			require.NoError(t, err)
			assert.True(t, tt.wantSince.Equal(goneSince), "%s streak start = %v, want %v", goneHost, goneSince, tt.wantSince)
			assert.NoError(t, runErr, "a host-gone replies collection is not a run error")
			assert.NotNil(t, h.lastBackfillAt(groupID), "a walk whose only failure is a host-gone replies collection stamps last_backfill_at")
			assert.Equal(t, walkDeferredRows(h, goneHost, [2]string{pageID, "Page"}), deferredRows(t, h),
				"the post is deferred under the collection's host, first due 1h after the walk")

			// A sweep while the collection still fails: the post lands again,
			// but its row stays, rescheduled to its 6h retry with one attempt.
			stillGone, err := NewBackfill(opts)
			require.NoError(t, err)
			stillGone.now = func() time.Time { return backfillNow.Add(61 * time.Minute) }
			require.NoError(t, stillGone.RetryDeferred(ctx, 50))
			assert.Equal(t, []store.DeferredItem{{
				ObjectIRI: pageID, CommunityAPID: groupID, CommunityDID: h.communityDID(groupID),
				Host: goneHost, ObjectType: "Page", Attempts: 1,
				NextRetryAt: time.Date(2026, 11, 8, 18, 0, 0, 0, time.UTC),
				CreatedAt:   time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC),
			}}, deferredRows(t, h), "a sweep whose replies collection still fails keeps the post's row, at its 6h retry")

			sweep := newBackfill(t, h, 100)
			sweep.now = func() time.Time { return time.Date(2026, 11, 8, 18, 1, 0, 0, time.UTC) }
			require.NoError(t, sweep.RetryDeferred(ctx, 50))

			_, err = h.objects.GetByAPID(ctx, replyID)
			assert.NoError(t, err, "the sweep lands the reply once the collection is served")
			assert.Equal(t, []store.DeferredItem{}, deferredRows(t, h), "the post's row goes once its replies are walked")
		})
	}
}

// TestBackfillAuthorFetchClearsHostGoneStreak: with the materializer's
// fetcher wrapped by NewHostHealthFetcher, a successful author fetch
// (FetchActor) from a host with a streak ends that streak.
func TestBackfillAuthorFetchClearsHostGoneStreak(t *testing.T) {
	h := newHarness(t)
	h.subscribeTechnology()
	serveOutboxFixtures(t, h)
	const (
		authorHost = "lemmy.world"
		author     = "https://lemmy.world/u/Aweigh"
	)
	ctx := context.Background()
	_, err := h.actors.GetByAPActorID(ctx, author)
	require.True(t, errors.IsNotFound(err), "the author must be unknown so the materializer fetches it")
	seedHostGoneFailures(t, h, authorHost, 24*time.Hour)

	materializerOptions := h.materializerOptions
	materializerOptions.Fetcher = NewHostHealthFetcher(h.client, h.materializerOptions.Instances, slog.New(slog.DiscardHandler))
	materializer, err := materialize.New(materializerOptions)
	require.NoError(t, err)
	b := newBackfillOver(t, h, materializer, 100)
	b.now = func() time.Time { return backfillNow }

	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	require.NoError(t, b.Run(ctx, community, true))
	_, err = h.actors.GetByAPActorID(ctx, author)
	require.NoError(t, err, "the walk fetched and stored the author")

	// Without the clear, a failure 1h after the backfill clock continues the
	// seeded streak (last failure 25h earlier) and returns its 24h-old start.
	later := backfillNow.Add(time.Hour)
	goneSince, err := h.materializerOptions.Instances.RecordHostGone(ctx, authorHost, later)
	require.NoError(t, err)
	assert.True(t, later.Equal(goneSince),
		"a successful author fetch must clear %s's streak: goneSince = %v, want %v", authorHost, goneSince, later)
}

// fakeInnerFetcher returns fixed results and counts calls.
type fakeInnerFetcher struct {
	obj         *ap.Object
	err         error
	media       []byte
	contentType string
	calls       []string
}

func (f *fakeInnerFetcher) FetchObject(_ context.Context, iri string) (*ap.Object, error) {
	f.calls = append(f.calls, "object "+iri)
	return f.obj, f.err
}

func (f *fakeInnerFetcher) FetchActor(_ context.Context, iri string) (*ap.Object, error) {
	f.calls = append(f.calls, "actor "+iri)
	return f.obj, f.err
}

func (f *fakeInnerFetcher) FetchMedia(_ context.Context, iri string, maxBytes int64) ([]byte, string, error) {
	f.calls = append(f.calls, "media "+iri)
	return f.media, f.contentType, f.err
}

// fakeHostGoneStreaks records RecordHostGone and ClearHostGone calls. With
// recordErr set, RecordHostGone fails with it and a zero streak start.
type fakeHostGoneStreaks struct {
	mu        sync.Mutex
	cleared   []string
	recorded  []string
	clearErr  error
	recordErr error
}

func (s *fakeHostGoneStreaks) RecordHostGone(_ context.Context, host string, at time.Time) (time.Time, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recorded = append(s.recorded, host)
	if s.recordErr != nil {
		return time.Time{}, s.recordErr
	}
	return at, nil
}

func (s *fakeHostGoneStreaks) ClearHostGone(_ context.Context, host string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cleared = append(s.cleared, host)
	return s.clearErr
}

// TestHostHealthFetcher pins the decorator: a successful FetchObject or
// FetchActor clears the fetched IRI's host (lowercased, no port); an error
// clears nothing; results and errors pass through unchanged; a failing clear
// never fails the fetch; it never records a host-gone failure. A success
// whose returned object ID is on a different host clears nothing: the
// materializer rejects cross-authority documents, so they are no evidence the
// requested host is healthy.
func TestHostHealthFetcher(t *testing.T) {
	const iri = "https://Revived.Example:8443/post/1"
	fetchErr := stderrors.New("upstream boom")
	tests := []struct {
		name string
		call func(materialize.Fetcher) (*ap.Object, error)
		// returnedID is the ID of the object the inner fetcher returns on
		// success; empty means the requested IRI.
		returnedID  string
		innerErr    error
		clearErr    error
		wantCleared []string
		wantCall    string
	}{
		{
			name:        "successful FetchObject clears the host",
			call:        func(f materialize.Fetcher) (*ap.Object, error) { return f.FetchObject(context.Background(), iri) },
			wantCleared: []string{"revived.example"},
			wantCall:    "object " + iri,
		},
		{
			name:        "successful FetchActor clears the host",
			call:        func(f materialize.Fetcher) (*ap.Object, error) { return f.FetchActor(context.Background(), iri) },
			wantCleared: []string{"revived.example"},
			wantCall:    "actor " + iri,
		},
		{
			name:        "failed FetchObject clears nothing",
			call:        func(f materialize.Fetcher) (*ap.Object, error) { return f.FetchObject(context.Background(), iri) },
			innerErr:    fetchErr,
			wantCleared: nil,
			wantCall:    "object " + iri,
		},
		{
			name:        "failed FetchActor clears nothing",
			call:        func(f materialize.Fetcher) (*ap.Object, error) { return f.FetchActor(context.Background(), iri) },
			innerErr:    fetchErr,
			wantCleared: nil,
			wantCall:    "actor " + iri,
		},
		{
			name:        "a failing clear does not fail the fetch",
			call:        func(f materialize.Fetcher) (*ap.Object, error) { return f.FetchObject(context.Background(), iri) },
			clearErr:    stderrors.New("db down"),
			wantCleared: []string{"revived.example"},
			wantCall:    "object " + iri,
		},
		{
			name:        "FetchObject returning a cross-authority ID clears nothing",
			call:        func(f materialize.Fetcher) (*ap.Object, error) { return f.FetchObject(context.Background(), iri) },
			returnedID:  "https://elsewhere.example/post/1",
			wantCleared: nil,
			wantCall:    "object " + iri,
		},
		{
			name:        "FetchActor returning a cross-authority ID clears nothing",
			call:        func(f materialize.Fetcher) (*ap.Object, error) { return f.FetchActor(context.Background(), iri) },
			returnedID:  "https://elsewhere.example/post/1",
			wantCleared: nil,
			wantCall:    "actor " + iri,
		},
		{
			name:        "FetchObject returning the same host in another case and port clears the host",
			call:        func(f materialize.Fetcher) (*ap.Object, error) { return f.FetchObject(context.Background(), iri) },
			returnedID:  "https://revived.example/post/1",
			wantCleared: []string{"revived.example"},
			wantCall:    "object " + iri,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			returnedID := tt.returnedID
			if returnedID == "" {
				returnedID = iri
			}
			fetched := &ap.Object{ID: returnedID, Type: ap.TypeNote}
			inner := &fakeInnerFetcher{err: tt.innerErr}
			if tt.innerErr == nil {
				inner.obj = fetched
			}
			streaks := &fakeHostGoneStreaks{clearErr: tt.clearErr}
			fetcher := NewHostHealthFetcher(inner, streaks, slog.New(slog.DiscardHandler))

			obj, err := tt.call(fetcher)

			assert.Equal(t, []string{tt.wantCall}, inner.calls, "the inner fetcher is called once with the IRI")
			if tt.innerErr != nil {
				assert.Same(t, tt.innerErr, err, "the inner error passes through unchanged")
				assert.Nil(t, obj)
			} else {
				require.NoError(t, err)
				assert.Same(t, fetched, obj, "the inner result passes through unchanged")
			}
			assert.Equal(t, tt.wantCleared, streaks.cleared, "ClearHostGone calls")
			assert.Empty(t, streaks.recorded, "the decorator never records a host-gone failure")
		})
	}

	t.Run("FetchMedia passes through", func(t *testing.T) {
		inner := &fakeInnerFetcher{media: []byte("png"), contentType: "image/png"}
		streaks := &fakeHostGoneStreaks{}
		fetcher := NewHostHealthFetcher(inner, streaks, slog.New(slog.DiscardHandler))
		data, contentType, err := fetcher.FetchMedia(context.Background(), "https://Revived.Example/img.png", 1024)
		require.NoError(t, err)
		assert.Equal(t, []byte("png"), data)
		assert.Equal(t, "image/png", contentType)
		assert.Equal(t, []string{"media https://Revived.Example/img.png"}, inner.calls)
		assert.Empty(t, streaks.recorded)
	})
}

// TestBackfillHostGoneReplyAuthor: a reply whose body is served normally but
// whose author's actor document fails host-gone is deferred under the
// author's host, whatever that host's streak: the run succeeds and stamps
// last_backfill_at, and the reply gets its own deferred row.
func TestBackfillHostGoneReplyAuthor(t *testing.T) {
	const (
		replyID     = "https://lemmy.world/comment/3001"
		replyAuthor = "https://lemmy.world/u/replier"
		goneHost    = "lemmy.world"
	)
	tests := []struct {
		name      string
		seedDead  bool
		wantSince time.Time
	}{
		{name: "no prior streak", seedDead: false, wantSince: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)},
		{name: "dead streak", seedDead: true, wantSince: time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.subscribeTechnology()
			serveOutboxFixtures(t, h)
			ctx := context.Background()
			_, err := h.actors.GetByAPActorID(ctx, replyAuthor)
			require.True(t, errors.IsNotFound(err), "the reply author must be unknown so the materializer fetches it")
			if tt.seedDead {
				seedDeadHostStreak(t, h, goneHost)
			}
			materializerOptions := h.materializerOptions
			materializerOptions.Fetcher = actorHostGoneFetcher{Fetcher: h.client, goneActorIRI: replyAuthor}
			materializer, err := materialize.New(materializerOptions)
			require.NoError(t, err)
			b := newBackfillOver(t, h, materializer, 100)
			b.now = func() time.Time { return backfillNow }

			community, err := h.communities.GetByAPGroupID(ctx, groupID)
			require.NoError(t, err)
			runErr := b.Run(ctx, community, true)

			_, err = h.objects.GetByAPID(ctx, pageID)
			require.NoError(t, err, "the post carrying the replies collection still materializes")
			_, err = h.objects.GetByAPID(ctx, replyID)
			assert.True(t, errors.IsNotFound(err), "the reply whose author's host is gone is not materialized")
			goneSince, err := h.materializerOptions.Instances.RecordHostGone(ctx, goneHost, backfillNow.Add(time.Hour))
			require.NoError(t, err)
			assert.True(t, tt.wantSince.Equal(goneSince), "%s streak start = %v, want %v", goneHost, goneSince, tt.wantSince)
			assert.NoError(t, runErr, "a reply whose author's host is gone is deferred, not a run error")
			assert.NotNil(t, h.lastBackfillAt(groupID), "a walk whose only failure is a deferred reply stamps last_backfill_at")
			assert.Equal(t, walkDeferredRows(h, goneHost, [2]string{replyID, "Note"}), deferredRows(t, h),
				"the reply is deferred under its author's host, first due 1h after the walk")
		})
	}
}

// TestBackfillHostGoneRecordFailureIsAFailure: when the host-gone streak
// cannot be recorded, the host-gone outbox item, reply or replies collection
// is an ordinary failure, never a skip: the run errors, last_backfill_at
// stays unset and nothing is deferred.
func TestBackfillHostGoneRecordFailureIsAFailure(t *testing.T) {
	const goneReply = "https://other.test/comment/77"
	repliesIRI := pageID + "/replies"
	tests := []struct {
		name    string
		serve   func(t *testing.T, h *harness)
		fetcher func(h *harness) BackfillFetcher
	}{
		{
			name:  "an outbox item",
			serve: serveBrokenItemOutbox,
			fetcher: func(h *harness) BackfillFetcher {
				return hostGoneFetcher{BackfillFetcher: h.client, goneIRI: "https://other.test/post/broken"}
			},
		},
		{
			name: "a reply",
			serve: func(_ *testing.T, h *harness) {
				h.serveObject("/post/49131386/replies", map[string]any{
					"type":         "OrderedCollection",
					"id":           repliesIRI,
					"totalItems":   1,
					"orderedItems": []any{note(goneReply, "https://other.test/u/ghost", pageID, "a reply from a gone host", "2026-07-07T05:00:00.000000Z")},
				})
			},
			fetcher: func(h *harness) BackfillFetcher {
				return hostGoneFetcher{BackfillFetcher: h.client, goneIRI: goneReply}
			},
		},
		{
			name: "a replies collection fetch",
			fetcher: func(h *harness) BackfillFetcher {
				return repliesCollectionGoneFetcher{BackfillFetcher: h.client, goneIRI: repliesIRI}
			},
		},
		{
			name: "a replies collection's later page",
			fetcher: func(h *harness) BackfillFetcher {
				return repliesCollectionGoneFetcher{BackfillFetcher: h.client, goneIRI: repliesIRI, afterFirstPage: true}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.subscribeTechnology()
			serveOutboxFixtures(t, h)
			if tt.serve != nil {
				tt.serve(t, h)
			}
			streaks := &fakeHostGoneStreaks{recordErr: stderrors.New("db down")}
			opts := h.backfillOptions(h.mat, 100)
			opts.Fetcher = tt.fetcher(h)
			opts.Instances = streaks
			b, err := NewBackfill(opts)
			require.NoError(t, err)
			b.now = func() time.Time { return backfillNow }
			ctx := context.Background()

			community, err := h.communities.GetByAPGroupID(ctx, groupID)
			require.NoError(t, err)
			assert.Error(t, b.Run(ctx, community, true), "a host-gone failure whose streak cannot be recorded fails the walk")

			_, err = h.objects.GetByAPID(ctx, pageID)
			require.NoError(t, err, "the healthy post still materializes")
			assert.Equal(t, []string{"other.test"}, streaks.recorded, "the run tried to record the host-gone failure once")
			assert.Nil(t, h.lastBackfillAt(groupID), "a host-gone failure with a failed streak record must leave last_backfill_at unset")
			assert.Equal(t, []store.DeferredItem{}, deferredRows(t, h), "nothing is deferred")
		})
	}
}

// TestBackfillCrossAuthorityFetchKeepsHostGoneStreak: an outbox item whose
// origin fetch answers with a document naming another host's id is a skip,
// and is no evidence the origin host is serving its own objects again: the
// origin host's streak survives.
func TestBackfillCrossAuthorityFetchKeepsHostGoneStreak(t *testing.T) {
	h := newHarness(t)
	h.subscribeTechnology()
	const (
		revivedHost   = "revived.example"
		revivedPost   = "https://revived.example/post/888"
		revivedAuthor = "https://revived.example/u/zed"
		technology    = "https://lemmy.world/c/technology"
	)
	h.serveObject("/u/zed", person(revivedAuthor, "zed", nil))
	page := map[string]any{
		"id":           revivedPost,
		"type":         "Page",
		"attributedTo": revivedAuthor,
		"audience":     technology,
		"to":           []any{technology, "https://www.w3.org/ns/activitystreams#Public"},
		"name":         "a post whose origin serves another host's id",
		"content":      "<p>not mine</p>",
		"published":    "2026-11-07T09:00:00.000000Z",
	}
	served := map[string]any{}
	for k, v := range page {
		served[k] = v
	}
	served["id"] = "https://elsewhere.example/post/888"
	h.serveObject("/post/888", served)
	h.serveObject("/c/technology/outbox", map[string]any{
		"type":         "OrderedCollection",
		"id":           technology + "/outbox",
		"totalItems":   1,
		"orderedItems": []any{page},
	})
	seedDeadHostStreak(t, h, revivedHost)
	b := newBackfill(t, h, 100)
	b.now = func() time.Time { return backfillNow }
	ctx := context.Background()

	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	require.NoError(t, b.Run(ctx, community, true), "a cross-authority response is a skip")
	require.Equal(t, 1, h.hitCount("/post/888"), "the walk fetched the item from its origin")
	_, err = h.objects.GetByAPID(ctx, revivedPost)
	assert.True(t, errors.IsNotFound(err), "the cross-authority item is not materialized")

	goneSince, err := h.materializerOptions.Instances.RecordHostGone(ctx, revivedHost, backfillNow.Add(time.Hour))
	require.NoError(t, err)
	want := backfillNow.Add(-96 * time.Hour)
	assert.True(t, want.Equal(goneSince),
		"a cross-authority response must not clear %s's streak: goneSince = %v, want %v", revivedHost, goneSince, want)
}
