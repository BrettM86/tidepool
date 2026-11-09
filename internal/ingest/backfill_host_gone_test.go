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

// TestBackfillDeadHostThreshold pins DeadHostAfter at the backfill level: a
// host-gone item is skipped only once the host's streak is at least 72h old
// at the backfill clock. A younger streak keeps the item a failure, so the
// run errors and last_backfill_at stays unset.
func TestBackfillDeadHostThreshold(t *testing.T) {
	tests := []struct {
		name string
		// agos are the seeded failures before backfillNow, oldest first; each
		// gap is under store.HostGoneStreakGap, so they form one streak.
		agos     []time.Duration
		wantSkip bool
	}{
		{
			name:     "48h-old streak is a failure",
			agos:     []time.Duration{48 * time.Hour, 24 * time.Hour},
			wantSkip: false,
		},
		{
			name:     "streak exactly 72h old is a skip",
			agos:     []time.Duration{72 * time.Hour, 36 * time.Hour},
			wantSkip: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.subscribeTechnology()
			serveOutboxFixtures(t, h)
			serveBrokenItemOutbox(t, h)
			seedHostGoneFailures(t, h, "other.test", tt.agos...)
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
			if tt.wantSkip {
				require.NoError(t, runErr, "a host gone for DeadHostAfter is a skip, not a run error")
				require.NotNil(t, h.lastBackfillAt(groupID), "a walk whose only failure is a dead-host skip stamps last_backfill_at")
				return
			}
			require.Error(t, runErr, "a host gone for less than DeadHostAfter is an item failure")
			require.Nil(t, h.lastBackfillAt(groupID), "a host-gone failure inside DeadHostAfter must leave last_backfill_at unset")
		})
	}
}

// TestBackfillHostGoneReply: a reply in a post's replies collection whose
// origin fetch fails host-gone is a failure while the host's streak is young
// (the run errors and last_backfill_at stays unset, so the walk retries) and
// a skip once the streak is dead (the walk stamps).
func TestBackfillHostGoneReply(t *testing.T) {
	const (
		goneReply = "https://other.test/comment/77"
		goneHost  = "other.test"
	)
	tests := []struct {
		name     string
		seedDead bool
		wantSkip bool
	}{
		{name: "no prior streak: the reply fails the item", seedDead: false, wantSkip: false},
		{name: "dead streak: the reply is skipped", seedDead: true, wantSkip: true},
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
			// The reply fetch reached the gone host: the run recorded a failure
			// into the host's streak (started by the run, or the seeded one).
			wantSince := backfillNow
			if tt.seedDead {
				wantSince = backfillNow.Add(-96 * time.Hour)
			}
			goneSince, err := h.materializerOptions.Instances.RecordHostGone(ctx, goneHost, backfillNow.Add(time.Hour))
			require.NoError(t, err)
			assert.True(t, wantSince.Equal(goneSince), "%s streak start = %v, want %v", goneHost, goneSince, wantSince)
			if tt.wantSkip {
				require.NoError(t, runErr, "a reply on a dead host is a skip, not a run error")
				require.NotNil(t, h.lastBackfillAt(groupID), "a walk whose only failure is a dead-host reply stamps last_backfill_at")
				return
			}
			require.Error(t, runErr, "a reply on a host gone for less than DeadHostAfter fails the item")
			require.Nil(t, h.lastBackfillAt(groupID), "a young host-gone reply must leave last_backfill_at unset")
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
// later page of it) whose fetch fails host-gone holds the walk open while the
// host's streak is young (the run errors, last_backfill_at stays unset, and
// the failure starts the host's streak) and is a skip once the streak is dead
// (the walk stamps).
func TestBackfillHostGoneRepliesCollection(t *testing.T) {
	const goneHost = "other.test"
	repliesIRI := pageID + "/replies"
	tests := []struct {
		name           string
		afterFirstPage bool
		seedDead       bool
		wantSkip       bool
	}{
		{name: "no prior streak: the collection fetch fails the item", seedDead: false, wantSkip: false},
		{name: "no prior streak: a later page fails the item", afterFirstPage: true, seedDead: false, wantSkip: false},
		{name: "dead streak: the collection fetch is skipped", seedDead: true, wantSkip: true},
		{name: "dead streak: a later page is skipped", afterFirstPage: true, seedDead: true, wantSkip: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.subscribeTechnology()
			serveOutboxFixtures(t, h)
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
			// The collection fetch reached the gone host: the run recorded a
			// failure into the host's streak (started by the run, or the seeded one).
			wantSince := backfillNow
			if tt.seedDead {
				wantSince = backfillNow.Add(-96 * time.Hour)
			}
			goneSince, err := h.materializerOptions.Instances.RecordHostGone(ctx, goneHost, backfillNow.Add(time.Hour))
			require.NoError(t, err)
			assert.True(t, wantSince.Equal(goneSince), "%s streak start = %v, want %v", goneHost, goneSince, wantSince)
			if tt.wantSkip {
				require.NoError(t, runErr, "a replies collection on a dead host is a skip, not a run error")
				require.NotNil(t, h.lastBackfillAt(groupID), "a walk whose only failure is a dead-host replies collection stamps last_backfill_at")
				return
			}
			require.Error(t, runErr, "a replies collection on a host gone for less than DeadHostAfter fails the item")
			require.Nil(t, h.lastBackfillAt(groupID), "a young host-gone replies collection must leave last_backfill_at unset")
		})
	}
}

// TestBackfillAuthorFetchClearsHostGoneStreak: with the materializer's
// fetcher wrapped by NewHostHealthFetcher, a successful author fetch
// (FetchActor) from a host with a young streak ends that streak.
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
// whose author's actor document fails host-gone fails the item while the
// author host's streak is young (the run errors and last_backfill_at stays
// unset) and is a skip once the streak is dead (the walk stamps).
func TestBackfillHostGoneReplyAuthor(t *testing.T) {
	const (
		replyID     = "https://lemmy.world/comment/3001"
		replyAuthor = "https://lemmy.world/u/replier"
		goneHost    = "lemmy.world"
	)
	tests := []struct {
		name     string
		seedDead bool
		wantSkip bool
	}{
		{name: "no prior streak: the reply author fails the item", seedDead: false, wantSkip: false},
		{name: "dead streak: the reply author is skipped", seedDead: true, wantSkip: true},
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
			wantSince := backfillNow
			if tt.seedDead {
				wantSince = backfillNow.Add(-96 * time.Hour)
			}
			goneSince, err := h.materializerOptions.Instances.RecordHostGone(ctx, goneHost, backfillNow.Add(time.Hour))
			require.NoError(t, err)
			assert.True(t, wantSince.Equal(goneSince), "%s streak start = %v, want %v", goneHost, goneSince, wantSince)
			if tt.wantSkip {
				require.NoError(t, runErr, "a reply author on a dead host is a skip, not a run error")
				require.NotNil(t, h.lastBackfillAt(groupID), "a walk whose only failure is a dead-host reply author stamps last_backfill_at")
				return
			}
			require.Error(t, runErr, "a reply author on a host gone for less than DeadHostAfter fails the item")
			require.Nil(t, h.lastBackfillAt(groupID), "a young host-gone reply author must leave last_backfill_at unset")
		})
	}
}

// TestBackfillHostGoneRecordFailureIsAFailure: when the host-gone streak
// cannot be recorded, the host-gone item is an ordinary failure, never a
// skip: the run errors and last_backfill_at stays unset.
func TestBackfillHostGoneRecordFailureIsAFailure(t *testing.T) {
	h := newHarness(t)
	h.subscribeTechnology()
	serveOutboxFixtures(t, h)
	serveBrokenItemOutbox(t, h)
	streaks := &fakeHostGoneStreaks{recordErr: stderrors.New("db down")}
	opts := h.backfillOptions(h.mat, 100)
	opts.Fetcher = hostGoneFetcher{BackfillFetcher: h.client, goneIRI: "https://other.test/post/broken"}
	opts.Instances = streaks
	b, err := NewBackfill(opts)
	require.NoError(t, err)
	b.now = func() time.Time { return backfillNow }
	ctx := context.Background()

	community, err := h.communities.GetByAPGroupID(ctx, groupID)
	require.NoError(t, err)
	require.Error(t, b.Run(ctx, community, true), "a host-gone item whose streak cannot be recorded is an item failure")

	_, err = h.objects.GetByAPID(ctx, pageID)
	require.NoError(t, err, "the healthy item still materializes")
	assert.Equal(t, []string{"other.test"}, streaks.recorded, "the run tried to record the host-gone failure once")
	require.Nil(t, h.lastBackfillAt(groupID), "a host-gone item with a failed streak record must leave last_backfill_at unset")
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

// TestHostGoneTimingInvariant pins the ordering the dead-host streak relies
// on: a walk retried at the backoff cap records a failure before the streak
// lapses, and a host must fail across several retries before it is dead.
func TestHostGoneTimingInvariant(t *testing.T) {
	assert.Less(t, maxBackfillRetryDelay, store.HostGoneStreakGap,
		"maxBackfillRetryDelay must stay under store.HostGoneStreakGap")
	assert.Less(t, store.HostGoneStreakGap, DeadHostAfter,
		"store.HostGoneStreakGap must stay under DeadHostAfter")
}
