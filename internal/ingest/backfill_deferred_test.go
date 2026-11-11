package ingest

import (
	"context"
	stderrors "errors"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
	"tidepool/internal/materialize"
	"tidepool/internal/store"
)

// TestDeferredRetryAt pins the deferred-retry schedule: retries are measured
// from the item's first skip at 72h, 1 week, 2 weeks, 30 days and 90 days,
// each due at the first step strictly after now; once the 90-day retry has
// run there is none.
func TestDeferredRetryAt(t *testing.T) {
	createdAt := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		now    time.Time
		want   time.Time
		wantOK bool
	}{
		{name: "at the skip the first retry is 72h out", now: createdAt,
			want: time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC), wantOK: true},
		{name: "an hour after the skip (recovery pulled it forward) the next is 72h out", now: time.Date(2026, 11, 1, 1, 0, 0, 0, time.UTC),
			want: time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC), wantOK: true},
		{name: "at the 72h retry the next is 1 week out", now: time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC),
			want: time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC), wantOK: true},
		{name: "between 72h and 1 week the next is 1 week out", now: time.Date(2026, 11, 5, 4, 0, 0, 0, time.UTC),
			want: time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC), wantOK: true},
		{name: "at 1 week the next is 2 weeks out", now: time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC),
			want: time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC), wantOK: true},
		{name: "at 2 weeks the next is 30 days out", now: time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC),
			want: time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC), wantOK: true},
		{name: "at 30 days the next is 90 days out", now: time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC),
			want: time.Date(2027, 1, 30, 0, 0, 0, 0, time.UTC), wantOK: true},
		{name: "at 90 days there is no further retry", now: time.Date(2027, 1, 30, 0, 0, 0, 0, time.UTC),
			wantOK: false},
		{name: "past 90 days there is no further retry", now: time.Date(2027, 3, 6, 0, 0, 0, 0, time.UTC),
			wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := deferredRetryAt(createdAt, tt.now)
			require.Equal(t, tt.wantOK, ok, "a further retry exists")
			if tt.wantOK {
				assert.True(t, tt.want.Equal(got), "next retry = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestNewBackfillRequiresDeferred: a Backfill with nowhere to keep the items
// it skips on a dead host is refused, like one with no Instances or Prober.
func TestNewBackfillRequiresDeferred(t *testing.T) {
	_, err := NewBackfill(BackfillOptions{
		Fetcher:      &walkFetcher{clock: &manualClock{}},
		Materializer: unusedMaterializer{},
		Communities:  &stampingCommunities{},
		Tombstones:   unusedTombstones{},
		Instances:    unusedInstances{},
		Prober:       unusedProber{},
		Echo:         unusedClassifier{},
		Logger:       slog.New(slog.DiscardHandler),
	})
	require.Error(t, err, "NewBackfill with no Deferred store")
	assert.True(t, errors.IsValidation(err), "a missing Deferred store is a validation error: %v", err)
}

// failingPostMaterializer fails every post with a local error, after the
// object was fetched.
type failingPostMaterializer struct {
	Materializer
}

func (failingPostMaterializer) MaterializePost(context.Context, *ap.Object, string) (*materialize.Result, error) {
	return nil, stderrors.New("local materializer failure")
}

// TestBackfillRetryDeferred drives one RetryDeferred sweep per row, at
// backfillNow, over rows seeded through the store. Every due row is
// re-fetched from its origin: a served object lands and its row goes, as does
// one the origin no longer has; a host still unreachable counts an attempt
// and reschedules (or drops the row once the 90-day retry has run); a removed
// or unfollowed community or a tombstoned object drops its row with no fetch;
// a pending community's row is kept unfetched and moved to its next retry;
// a local error leaves the row as it was. The sweep honours its limit and
// shares one host cache, so a dead host costs one connection attempt per
// sweep.
func TestBackfillRetryDeferred(t *testing.T) {
	const (
		servedIRI   = "https://y.example/post/deferred-served"
		servedPath  = "/post/deferred-served"
		authorIRI   = "https://y.example/u/deferred"
		missingIRI  = "https://y.example/post/deferred-missing"
		missing1IRI = "https://y.example/post/deferred-missing-1"
		missing2IRI = "https://y.example/post/deferred-missing-2"
		missing3IRI = "https://y.example/post/deferred-missing-3"
		deadIRI1    = "https://x.example/post/1"
		deadIRI2    = "https://x.example/post/2"
		servingHost = "y.example"
		// repliesPostIRI is served, with a replies collection whose one reply
		// is on x.example (refusing; with no streak, a young host).
		repliesPostIRI  = "https://y.example/post/deferred-with-replies"
		repliesPostPath = "/post/deferred-with-replies"
		youngReplyIRI   = "https://x.example/comment/young"
		// erroringIRI answers 500; htmlIRI answers 200 text/html with no
		// redirect, on a host whose nodeinfo probe answers 404.
		erroringIRI = "https://y.example/post/deferred-500"
		htmlIRI     = "https://y.example/post/deferred-html"
		htmlPath    = "/post/deferred-html"
		// redirectFTPIRI and redirectUserinfoIRI answer 302 to a URL the
		// client's URL check refuses (an ftp scheme; userinfo).
		redirectFTPIRI       = "https://y.example/post/deferred-redirect-ftp"
		redirectFTPPath      = "/post/deferred-redirect-ftp"
		redirectUserinfoIRI  = "https://y.example/post/deferred-redirect-userinfo"
		redirectUserinfoPath = "/post/deferred-redirect-userinfo"
		// ownIRI is our own federated post (a bridge-origin mapping).
		ownHost = "coves.test"
		ownDID  = "did:plc:deferredownpost"
		ownRKey = "3ldeferredown1"
		ownIRI  = "https://" + ownHost + "/ap/object/" + ownDID + "/" + materialize.CollectionPostV2 + "/" + ownRKey
		ownPath = "/ap/object/" + ownDID + "/" + materialize.CollectionPostV2 + "/" + ownRKey
	)
	at := func(month time.Month, day, hour int) time.Time {
		return time.Date(2026, month, day, hour, 0, 0, 0, time.UTC)
	}
	// item is a row in the technology community; the run fills in its DID.
	item := func(iri, host string, attempts int, created, next time.Time) store.DeferredItem {
		return store.DeferredItem{
			ObjectIRI: iri, CommunityAPID: groupID, Host: host, ObjectType: "Page",
			Attempts: attempts, CreatedAt: created, NextRetryAt: next,
		}
	}
	type wantLog struct {
		msg, attr string
		levels    []string
	}
	tests := []struct {
		name       string
		seed       []store.DeferredItem
		limit      int
		deadStreak bool
		arrange    func(t *testing.T, h *harness)
		// beforeSecondSweep and secondSweepAt, when secondSweepAt is set, run
		// a second sweep at secondSweepAt after the first, with
		// beforeSecondSweep applied between them.
		beforeSecondSweep func(t *testing.T, h *harness)
		secondSweepAt     time.Time
		failPosts         bool
		want              []store.DeferredItem
		// wantDeadHostAttempts, when not -1, is the count of connection
		// attempts to x.example during the sweep.
		wantDeadHostAttempts int
		// wantServedFetches, when set, is the count of GETs of the served
		// object during the sweep.
		wantServedFetches int
		// unfetched are fixture paths that must see no GET during the sweep.
		unfetched       []string
		materialized    []string
		notMaterialized []string
		logs            []wantLog
	}{
		{
			name:                 "a due row whose object is now served lands and goes",
			seed:                 []store.DeferredItem{item(servedIRI, servingHost, 0, at(11, 5, 12), at(11, 8, 12))},
			want:                 []store.DeferredItem{},
			wantDeadHostAttempts: -1,
			wantServedFetches:    1,
			materialized:         []string{servedIRI},
		},
		{
			name:                 "a due row whose origin answers 404 goes",
			seed:                 []store.DeferredItem{item(missingIRI, servingHost, 0, at(11, 5, 12), at(11, 8, 12))},
			want:                 []store.DeferredItem{},
			wantDeadHostAttempts: -1,
			notMaterialized:      []string{missingIRI},
		},
		{
			name:                 "a due row on a host still refusing is rescheduled to the 1-week retry",
			seed:                 []store.DeferredItem{item(deadIRI1, unreachableHost, 0, at(11, 5, 12), at(11, 8, 12))},
			deadStreak:           true,
			want:                 []store.DeferredItem{item(deadIRI1, unreachableHost, 1, at(11, 5, 12), at(11, 12, 12))},
			wantDeadHostAttempts: 1,
			notMaterialized:      []string{deadIRI1},
		},
		{
			name:                 "a row pulled forward by recovery whose host still refuses is rescheduled to the 72h retry",
			seed:                 []store.DeferredItem{item(deadIRI1, unreachableHost, 0, at(11, 8, 11), at(11, 8, 11))},
			deadStreak:           true,
			want:                 []store.DeferredItem{item(deadIRI1, unreachableHost, 1, at(11, 8, 11), at(11, 11, 11))},
			wantDeadHostAttempts: 1,
		},
		{
			name:                 "a row whose 90-day retry fails is dropped",
			seed:                 []store.DeferredItem{item(deadIRI1, unreachableHost, 4, at(8, 10, 12), at(11, 8, 12))},
			deadStreak:           true,
			want:                 []store.DeferredItem{},
			wantDeadHostAttempts: 1,
			logs:                 []wantLog{{msg: "backfill deferred item dropped", attr: "reason=retries_exhausted", levels: []string{"INFO"}}},
		},
		{
			name: "a row of a removed community is dropped with no fetch",
			seed: []store.DeferredItem{item(deadIRI1, unreachableHost, 0, at(11, 5, 12), at(11, 8, 12))},
			arrange: func(t *testing.T, h *harness) {
				newCommunityLabelCommitter(t, h, h.admin).removeCommunity(t, h.communityDID(groupID))
			},
			want:                 []store.DeferredItem{},
			wantDeadHostAttempts: 0,
			logs:                 []wantLog{{msg: "backfill deferred item dropped", attr: "reason=community_removed", levels: []string{"INFO"}}},
		},
		{
			// The community still exists and is not removed, but the bridge
			// no longer follows it. Its object is served, so a fetch would
			// land it.
			name: "a row of an unfollowed community is dropped with no fetch",
			seed: []store.DeferredItem{item(servedIRI, servingHost, 0, at(11, 5, 12), at(11, 8, 12))},
			arrange: func(t *testing.T, h *harness) {
				require.NoError(t, h.communities.SetFollowState(context.Background(), groupID, store.FollowStateNone))
			},
			want:                 []store.DeferredItem{},
			wantDeadHostAttempts: -1,
			unfetched:            []string{servedPath, "/u/deferred"},
			notMaterialized:      []string{servedIRI},
			logs:                 []wantLog{{msg: "backfill deferred item dropped", attr: "reason=community_unfollowed", levels: []string{"INFO"}}},
		},
		{
			// A pending follow is temporary (a lifted removal restores a
			// wanted community as pending), so the row is kept, unfetched,
			// with its attempts unchanged. It is moved off its old due time to
			// its next retry step (1 week) so pending rows do not sort first
			// and starve the batch.
			name: "a row of a pending community is kept unfetched and moved to its next retry",
			seed: []store.DeferredItem{item(servedIRI, servingHost, 0, at(11, 5, 12), at(11, 8, 12))},
			arrange: func(t *testing.T, h *harness) {
				require.NoError(t, h.communities.SetFollowState(context.Background(), groupID, store.FollowStatePending))
			},
			want:                 []store.DeferredItem{item(servedIRI, servingHost, 0, at(11, 5, 12), at(11, 12, 12))},
			wantDeadHostAttempts: -1,
			unfetched:            []string{servedPath, "/u/deferred"},
			notMaterialized:      []string{servedIRI},
			logs:                 []wantLog{{msg: "backfill deferred item dropped", levels: nil}},
		},
		{
			// The follow is accepted after the pending sweep; the next sweep,
			// at the rescheduled time, fetches and lands the object.
			name: "a row kept while its community was pending lands once the follow is accepted",
			seed: []store.DeferredItem{item(servedIRI, servingHost, 0, at(11, 5, 12), at(11, 8, 12))},
			arrange: func(t *testing.T, h *harness) {
				require.NoError(t, h.communities.SetFollowState(context.Background(), groupID, store.FollowStatePending))
			},
			beforeSecondSweep: func(t *testing.T, h *harness) {
				require.NoError(t, h.communities.SetFollowState(context.Background(), groupID, store.FollowStateAccepted))
			},
			secondSweepAt:        at(11, 12, 12),
			want:                 []store.DeferredItem{},
			wantDeadHostAttempts: -1,
			wantServedFetches:    1,
			materialized:         []string{servedIRI},
		},
		{
			name: "the sweep processes at most limit due rows, oldest due first",
			seed: []store.DeferredItem{
				item(missing1IRI, servingHost, 0, at(11, 5, 10), at(11, 8, 10)),
				item(missing2IRI, servingHost, 0, at(11, 5, 11), at(11, 8, 11)),
				item(missing3IRI, servingHost, 0, at(11, 5, 13), at(11, 8, 13)),
			},
			limit: 1,
			want: []store.DeferredItem{
				item(missing2IRI, servingHost, 0, at(11, 5, 11), at(11, 8, 11)),
				item(missing3IRI, servingHost, 0, at(11, 5, 13), at(11, 8, 13)),
			},
			wantDeadHostAttempts: -1,
		},
		{
			name: "two due rows on one dead host cost one connection attempt and are both rescheduled",
			seed: []store.DeferredItem{
				item(deadIRI1, unreachableHost, 0, at(11, 5, 12), at(11, 8, 12)),
				item(deadIRI2, unreachableHost, 0, at(11, 5, 11), at(11, 8, 11)),
			},
			deadStreak: true,
			want: []store.DeferredItem{
				item(deadIRI1, unreachableHost, 1, at(11, 5, 12), at(11, 12, 12)),
				item(deadIRI2, unreachableHost, 1, at(11, 5, 11), at(11, 12, 11)),
			},
			wantDeadHostAttempts: 1,
		},
		{
			name: "a tombstoned row is dropped with no fetch",
			seed: []store.DeferredItem{item(deadIRI1, unreachableHost, 0, at(11, 5, 12), at(11, 8, 12))},
			arrange: func(t *testing.T, h *harness) {
				require.NoError(t, h.tombstones.Record(context.Background(), deadIRI1, groupID))
			},
			want:                 []store.DeferredItem{},
			wantDeadHostAttempts: 0,
		},
		{
			name:                 "a local error after a good fetch leaves the row as it was",
			seed:                 []store.DeferredItem{item(servedIRI, servingHost, 0, at(11, 5, 12), at(11, 8, 12))},
			failPosts:            true,
			want:                 []store.DeferredItem{item(servedIRI, servingHost, 0, at(11, 5, 12), at(11, 8, 12))},
			wantDeadHostAttempts: -1,
			wantServedFetches:    1,
			notMaterialized:      []string{servedIRI},
		},
		{
			name:                 "a row whose post lands but whose reply pass fails on a young host counts an attempt and is rescheduled",
			seed:                 []store.DeferredItem{item(repliesPostIRI, servingHost, 0, at(11, 5, 12), at(11, 8, 12))},
			want:                 []store.DeferredItem{item(repliesPostIRI, servingHost, 1, at(11, 5, 12), at(11, 12, 12))},
			wantDeadHostAttempts: -1,
			materialized:         []string{repliesPostIRI},
		},
		{
			name:                 "a row whose origin answers 500 counts an attempt and is rescheduled to the 1-week retry",
			seed:                 []store.DeferredItem{item(erroringIRI, servingHost, 0, at(11, 5, 12), at(11, 8, 12))},
			want:                 []store.DeferredItem{item(erroringIRI, servingHost, 1, at(11, 5, 12), at(11, 12, 12))},
			wantDeadHostAttempts: -1,
			notMaterialized:      []string{erroringIRI},
		},
		{
			name:                 "a row whose origin serves HTML while its probe fails counts an attempt and is rescheduled to the 1-week retry",
			seed:                 []store.DeferredItem{item(htmlIRI, servingHost, 0, at(11, 5, 12), at(11, 8, 12))},
			want:                 []store.DeferredItem{item(htmlIRI, servingHost, 1, at(11, 5, 12), at(11, 12, 12))},
			wantDeadHostAttempts: -1,
			notMaterialized:      []string{htmlIRI},
		},
		{
			name:                 "a row whose origin redirects to an ftp URL counts an attempt and is rescheduled to the 1-week retry",
			seed:                 []store.DeferredItem{item(redirectFTPIRI, servingHost, 0, at(11, 5, 12), at(11, 8, 12))},
			want:                 []store.DeferredItem{item(redirectFTPIRI, servingHost, 1, at(11, 5, 12), at(11, 12, 12))},
			wantDeadHostAttempts: -1,
			notMaterialized:      []string{redirectFTPIRI},
		},
		{
			name:                 "a row whose origin redirects to a URL with userinfo counts an attempt and is rescheduled to the 1-week retry",
			seed:                 []store.DeferredItem{item(redirectUserinfoIRI, servingHost, 0, at(11, 5, 12), at(11, 8, 12))},
			want:                 []store.DeferredItem{item(redirectUserinfoIRI, servingHost, 1, at(11, 5, 12), at(11, 12, 12))},
			wantDeadHostAttempts: -1,
			notMaterialized:      []string{redirectUserinfoIRI},
		},
		{
			name:                 "a local error on a row past its 90-day retry drops the row",
			seed:                 []store.DeferredItem{item(servedIRI, servingHost, 4, at(8, 1, 12), at(11, 8, 12))},
			failPosts:            true,
			want:                 []store.DeferredItem{},
			wantDeadHostAttempts: -1,
			notMaterialized:      []string{servedIRI},
			logs:                 []wantLog{{msg: "backfill deferred item dropped", attr: "reason=retries_exhausted", levels: []string{"INFO"}}},
		},
		{
			name: "a row whose object is our own federated content is dropped with no fetch",
			seed: []store.DeferredItem{item(ownIRI, ownHost, 0, at(11, 5, 12), at(11, 8, 12))},
			arrange: func(t *testing.T, h *harness) {
				_, err := h.objects.PutMapping(context.Background(), store.APObjectMapping{
					APID:           ownIRI,
					APType:         "Page",
					OriginInstance: ownHost,
					Origin:         store.OriginBridge,
					DID:            ownDID,
					AuthorDID:      ownDID,
					CommunityDID:   h.communityDID(groupID),
					Collection:     materialize.CollectionPostV2,
					RKey:           ownRKey,
					CID:            "bafyreievgu2ty7qbiaaom5zhmkznsnajuzideek3lo7e65dwqlrvrxnmo4",
				})
				require.NoError(t, err)
			},
			want:                 []store.DeferredItem{},
			wantDeadHostAttempts: -1,
			unfetched:            []string{ownPath},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.subscribeTechnology()
			ctx := context.Background()
			h.serveObject("/u/deferred", person(authorIRI, "deferred", nil))
			h.serveObject(servedPath, map[string]any{
				"id":           servedIRI,
				"type":         "Page",
				"attributedTo": authorIRI,
				"audience":     groupID,
				"to":           []any{groupID, ap.PublicAudience},
				"name":         "a post from a host that came back",
				"content":      "<p>served again</p>",
				"published":    "2026-11-07T09:00:00.000000Z",
			})
			h.serveObject(repliesPostPath, map[string]any{
				"id":           repliesPostIRI,
				"type":         "Page",
				"attributedTo": authorIRI,
				"audience":     groupID,
				"to":           []any{groupID, ap.PublicAudience},
				"name":         "a post whose reply is on a young host",
				"content":      "<p>with replies</p>",
				"published":    "2026-11-07T09:00:00.000000Z",
				"replies":      repliesPostIRI + "/replies",
			})
			h.serveObject(repliesPostPath+"/replies", map[string]any{
				"type":         "OrderedCollection",
				"id":           repliesPostIRI + "/replies",
				"totalItems":   1,
				"orderedItems": []any{note(youngReplyIRI, "https://x.example/u/gone", repliesPostIRI, "a reply on a young host", "2026-11-07T10:00:00.000000Z")},
			})
			h.serveStatus("/post/deferred-500", http.StatusInternalServerError)
			h.mux.HandleFunc("GET "+htmlPath, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/html")
				_, _ = w.Write([]byte("<html><body>the instance frontend</body></html>"))
			})
			h.mux.HandleFunc("GET "+redirectFTPPath, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Location", "ftp://z.example/x")
				w.WriteHeader(http.StatusFound)
			})
			h.mux.HandleFunc("GET "+redirectUserinfoPath, func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Location", "https://user@z.example/x")
				w.WriteHeader(http.StatusFound)
			})
			h.serveObject(ownPath, map[string]any{
				"id":           ownIRI,
				"type":         "Page",
				"attributedTo": "https://" + ownHost + "/ap/actor/" + ownDID,
				"audience":     groupID,
				"to":           []any{groupID, ap.PublicAudience},
				"name":         "our own post",
				"content":      "<p>ours</p>",
				"published":    "2026-11-07T09:00:00.000000Z",
			})
			for _, path := range []string{"/post/deferred-missing", "/post/deferred-missing-1", "/post/deferred-missing-2", "/post/deferred-missing-3"} {
				h.serveStatus(path, http.StatusNotFound)
			}
			if tt.deadStreak {
				seedDeadHostStreak(t, h, unreachableHost)
			}
			communityDID := h.communityDID(groupID)
			deferred := store.NewDeferredBackfillItems(h.db)
			for _, row := range tt.seed {
				row.CommunityDID = communityDID
				require.NoError(t, deferred.DeferItem(ctx, row))
			}
			seeded := []store.DeferredItem{}
			for _, row := range tt.seed {
				row.CommunityDID = communityDID
				seeded = append(seeded, row)
			}
			slices.SortFunc(seeded, func(a, b store.DeferredItem) int { return strings.Compare(a.ObjectIRI, b.ObjectIRI) })
			require.Equal(t, seeded, deferredRows(t, h), "the seeded rows are in the table before the sweep")
			if tt.arrange != nil {
				tt.arrange(t, h)
			}
			var adjust func(*BackfillOptions)
			if tt.failPosts {
				adjust = func(opts *BackfillOptions) {
					opts.Materializer = failingPostMaterializer{Materializer: opts.Materializer}
				}
			}
			rig := newUnreachableRigWith(t, h,
				map[string]error{unreachableHost: connectionRefused()},
				map[string]probeAnswer{unreachableHost: probeHTML}, adjust)
			limit := tt.limit
			if limit == 0 {
				limit = 50
			}

			_ = rig.backfill.RetryDeferred(ctx, limit)
			if !tt.secondSweepAt.IsZero() {
				if tt.beforeSecondSweep != nil {
					tt.beforeSecondSweep(t, h)
				}
				rig.backfill.now = func() time.Time { return tt.secondSweepAt }
				_ = rig.backfill.RetryDeferred(ctx, limit)
			}

			want := []store.DeferredItem{}
			for _, row := range tt.want {
				row.CommunityDID = communityDID
				want = append(want, row)
			}
			assert.Equal(t, want, deferredRows(t, h), "deferred rows after the sweep")
			if tt.wantDeadHostAttempts >= 0 {
				assert.Equal(t, tt.wantDeadHostAttempts, rig.transport.attemptsTo(unreachableHost),
					"connection attempts to x.example during the sweep")
			}
			if tt.wantServedFetches > 0 {
				assert.Equal(t, tt.wantServedFetches, h.hitCount(servedPath), "GETs of the served object during the sweep")
			}
			for _, path := range tt.unfetched {
				assert.Equal(t, 0, h.hitCount(path), "GETs of %s during the sweep", path)
			}
			for _, id := range tt.materialized {
				_, err := h.objects.GetByAPID(ctx, id)
				assert.NoError(t, err, "%s must be materialized", id)
			}
			requireNotMaterialized(t, h, tt.notMaterialized...)
			for _, log := range tt.logs {
				assert.Equal(t, log.levels, logLevels(h.logs.String(), log.msg, log.attr),
					"levels of %q lines carrying %s", log.msg, log.attr)
			}
		})
	}
}
