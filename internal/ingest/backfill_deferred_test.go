package ingest

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
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

// TestDeferredRetryAt pins the deferred-retry schedule: retries are measured
// from the item's first skip at 1h, 6h, 24h, 72h, 1 week, 2 weeks, 30 days
// and 90 days, each due at the first step strictly after now; once the
// 90-day retry has run there is none.
func TestDeferredRetryAt(t *testing.T) {
	createdAt := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		now    time.Time
		want   time.Time
		wantOK bool
	}{
		{name: "at the skip the first retry is 1h out", now: createdAt,
			want: time.Date(2026, 11, 1, 1, 0, 0, 0, time.UTC), wantOK: true},
		{name: "a second before 1h the next is 1h out", now: time.Date(2026, 11, 1, 0, 59, 59, 0, time.UTC),
			want: time.Date(2026, 11, 1, 1, 0, 0, 0, time.UTC), wantOK: true},
		{name: "at the 1h retry the next is 6h out", now: time.Date(2026, 11, 1, 1, 0, 0, 0, time.UTC),
			want: time.Date(2026, 11, 1, 6, 0, 0, 0, time.UTC), wantOK: true},
		{name: "between 1h and 6h the next is 6h out", now: time.Date(2026, 11, 1, 3, 0, 0, 0, time.UTC),
			want: time.Date(2026, 11, 1, 6, 0, 0, 0, time.UTC), wantOK: true},
		{name: "at the 6h retry the next is 24h out", now: time.Date(2026, 11, 1, 6, 0, 0, 0, time.UTC),
			want: time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC), wantOK: true},
		{name: "at the 24h retry the next is 72h out", now: time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC),
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
		{name: "a second before 90 days the next is 90 days out", now: time.Date(2027, 1, 29, 23, 59, 59, 0, time.UTC),
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
		// is on x.example (refusing, with no streak).
		repliesPostIRI  = "https://y.example/post/deferred-with-replies"
		repliesPostPath = "/post/deferred-with-replies"
		refusedReplyIRI = "https://x.example/comment/refused"
		// redirectRootIRI redirects to y.example's root, which answers HTML
		// (redirect_to_root); parkedIRI redirects off y.example to a parked
		// HTML page (parked_redirect). Both are host-gone for that URL only.
		redirectRootIRI  = "https://y.example/post/deferred-redirect-root"
		redirectRootPath = "/post/deferred-redirect-root"
		parkedIRI        = "https://y.example/post/deferred-parked"
		parkedPath       = "/post/deferred-parked"
		// loopIRI redirects to itself on y.example past the redirect limit
		// (too_many_redirects), host-gone for that URL only.
		loopIRI  = "https://y.example/post/deferred-loop"
		loopPath = "/post/deferred-loop"
		// edgeHost answers 522 for every path except edgeServedPath, its
		// author and its nodeinfo.
		edgeHost       = "e.example"
		edgeDownIRI    = "https://e.example/post/edge-down"
		edgeServedIRI  = "https://e.example/post/edge-served"
		edgeServedPath = "/post/edge-served"
		edgeAuthorIRI  = "https://e.example/u/edge"
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
		// failStreakRecords makes every RecordHostGone of the sweep fail.
		failStreakRecords bool
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
			name:                 "a row pulled forward by recovery whose host still refuses is rescheduled to the 6h retry",
			seed:                 []store.DeferredItem{item(deadIRI1, unreachableHost, 0, at(11, 8, 11), at(11, 8, 11))},
			deadStreak:           true,
			want:                 []store.DeferredItem{item(deadIRI1, unreachableHost, 1, at(11, 8, 11), at(11, 8, 17))},
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
			// The post's row goes; the reply on the refusing host (no streak)
			// gets its own row, first due 1h after this sweep.
			name: "a row whose post lands while its reply's host refuses goes, and the reply gets its own row",
			seed: []store.DeferredItem{item(repliesPostIRI, servingHost, 0, at(11, 5, 12), at(11, 8, 12))},
			want: []store.DeferredItem{{
				ObjectIRI: refusedReplyIRI, CommunityAPID: groupID, Host: unreachableHost, ObjectType: "Note",
				Attempts: 0, CreatedAt: backfillNow, NextRetryAt: firstDeferredRetry,
			}},
			wantDeadHostAttempts: -1,
			materialized:         []string{repliesPostIRI},
		},
		{
			// The reply's host-gone failure cannot be recorded into its
			// host's streak: a local failure, so the post's row is left as it
			// was for the next sweep.
			name:                 "a row whose post lands while its reply's host-gone failure cannot be recorded is left as it was",
			seed:                 []store.DeferredItem{item(repliesPostIRI, servingHost, 0, at(11, 5, 12), at(11, 8, 12))},
			failStreakRecords:    true,
			want:                 []store.DeferredItem{item(repliesPostIRI, servingHost, 0, at(11, 5, 12), at(11, 8, 12))},
			wantDeadHostAttempts: -1,
			materialized:         []string{repliesPostIRI},
		},
		{
			// redirect_to_root is about that one URL, not the host: the
			// host's next due row is still fetched and lands.
			name: "a row whose origin redirects to its root does not stop the host's next row",
			seed: []store.DeferredItem{
				item(redirectRootIRI, servingHost, 0, at(11, 5, 12), at(11, 8, 10)),
				item(servedIRI, servingHost, 0, at(11, 5, 12), at(11, 8, 12)),
			},
			want:                 []store.DeferredItem{item(redirectRootIRI, servingHost, 1, at(11, 5, 12), at(11, 12, 12))},
			wantDeadHostAttempts: -1,
			wantServedFetches:    1,
			materialized:         []string{servedIRI},
			notMaterialized:      []string{redirectRootIRI},
		},
		{
			name: "a row whose origin redirects to a parked page does not stop the host's next row",
			seed: []store.DeferredItem{
				item(parkedIRI, servingHost, 0, at(11, 5, 12), at(11, 8, 10)),
				item(servedIRI, servingHost, 0, at(11, 5, 12), at(11, 8, 12)),
			},
			want:                 []store.DeferredItem{item(parkedIRI, servingHost, 1, at(11, 5, 12), at(11, 12, 12))},
			wantDeadHostAttempts: -1,
			wantServedFetches:    1,
			materialized:         []string{servedIRI},
			notMaterialized:      []string{parkedIRI},
		},
		{
			name: "a row whose origin redirects in a loop does not stop the host's next row",
			seed: []store.DeferredItem{
				item(loopIRI, servingHost, 0, at(11, 5, 12), at(11, 8, 10)),
				item(servedIRI, servingHost, 0, at(11, 5, 12), at(11, 8, 12)),
			},
			want:                 []store.DeferredItem{item(loopIRI, servingHost, 1, at(11, 5, 12), at(11, 12, 12))},
			wantDeadHostAttempts: -1,
			wantServedFetches:    1,
			materialized:         []string{servedIRI},
			notMaterialized:      []string{loopIRI},
		},
		{
			// The host already failed host-gone in this sweep (522, a shared
			// verdict), but the row on its last retry step is fetched once
			// before it could be dropped; the host serves it, so it lands.
			name: "a row on its last retry step is fetched although its host already failed in the sweep",
			seed: []store.DeferredItem{
				item(edgeDownIRI, edgeHost, 0, at(11, 5, 12), at(11, 8, 10)),
				item(edgeServedIRI, edgeHost, 4, at(8, 1, 12), at(11, 8, 12)),
			},
			want:                 []store.DeferredItem{item(edgeDownIRI, edgeHost, 1, at(11, 5, 12), at(11, 12, 12))},
			wantDeadHostAttempts: -1,
			materialized:         []string{edgeServedIRI},
			notMaterialized:      []string{edgeDownIRI},
			logs:                 []wantLog{{msg: "backfill deferred item dropped", levels: nil}},
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
				"name":         "a post whose reply is on a refusing host",
				"content":      "<p>with replies</p>",
				"published":    "2026-11-07T09:00:00.000000Z",
				"replies":      repliesPostIRI + "/replies",
			})
			h.serveObject(repliesPostPath+"/replies", map[string]any{
				"type":         "OrderedCollection",
				"id":           repliesPostIRI + "/replies",
				"totalItems":   1,
				"orderedItems": []any{note(refusedReplyIRI, "https://x.example/u/gone", repliesPostIRI, "a reply on a refusing host", "2026-11-07T10:00:00.000000Z")},
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
			h.mux.HandleFunc("GET "+redirectRootPath, func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, "/", http.StatusFound)
			})
			h.mux.HandleFunc("GET "+servingHost+"/{$}", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/html")
				_, _ = w.Write([]byte("<html><body>welcome</body></html>"))
			})
			h.mux.HandleFunc("GET "+loopPath, func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, loopPath, http.StatusFound)
			})
			h.mux.HandleFunc("GET "+parkedPath, func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, "https://parked.example/", http.StatusFound)
			})
			h.mux.HandleFunc("GET parked.example/", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/html")
				_, _ = w.Write([]byte("<html><body>This domain is for sale</body></html>"))
			})
			serveEdgeHost(t, h, edgeHost, map[string]map[string]any{
				edgeServedPath: {
					"id":           edgeServedIRI,
					"type":         "Page",
					"attributedTo": edgeAuthorIRI,
					"audience":     groupID,
					"to":           []any{groupID, ap.PublicAudience},
					"name":         "a post its edge-down host still serves",
					"content":      "<p>served</p>",
					"published":    "2026-11-07T09:00:00.000000Z",
				},
				"/u/edge": person(edgeAuthorIRI, "edge", nil),
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
			adjust := func(opts *BackfillOptions) {
				if tt.failPosts {
					opts.Materializer = failingPostMaterializer{Materializer: opts.Materializer}
				}
				if tt.failStreakRecords {
					opts.Instances = &fakeHostGoneStreaks{recordErr: stderrors.New("db down")}
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

// sweepRow is the i-th of the rows a per-host cap test seeds on host: first
// skipped i minutes after 10:00 on the backfillNow day, at its 1h retry (due
// by backfillNow, oldest first).
func sweepRow(h *harness, host string, i int) store.DeferredItem {
	created := time.Date(2026, 11, 8, 10, i, 0, 0, time.UTC)
	return store.DeferredItem{
		ObjectIRI: fmt.Sprintf("https://%s/post/swept-%02d", host, i), CommunityAPID: groupID,
		CommunityDID: h.communityDID(groupID), Host: host, ObjectType: "Page",
		Attempts: 0, CreatedAt: created, NextRetryAt: created.Add(time.Hour),
	}
}

// seedSweepRows stores rows 0..count-1 of host and returns them as
// deferredRows reads them.
func seedSweepRows(t *testing.T, h *harness, host string, count int) []store.DeferredItem {
	t.Helper()
	deferred := store.NewDeferredBackfillItems(h.db)
	rows := []store.DeferredItem{}
	for i := range count {
		row := sweepRow(h, host, i)
		require.NoError(t, deferred.DeferItem(context.Background(), row))
		rows = append(rows, row)
	}
	require.Equal(t, rows, deferredRows(t, h), "the seeded rows are in the table before the sweep")
	return rows
}

// rescheduledToSixHours is row after one failed attempt at backfillNow: its
// 6h retry, measured from its first skip.
func rescheduledToSixHours(row store.DeferredItem) store.DeferredItem {
	row.Attempts = 1
	row.NextRetryAt = row.CreatedAt.Add(6 * time.Hour)
	return row
}

// TestBackfillRetryDeferredDeadHostCap: a sweep over 50 due rows on a host
// still gone makes one request to it, whatever the host-gone reason (a
// refused connection, which the AP client caches, or a CDN edge answering
// 522, which it does not). It attempts the first 20 of them, each rescheduled
// to its 6h retry, and leaves the other 30 as they were, still due.
func TestBackfillRetryDeferredDeadHostCap(t *testing.T) {
	tests := []struct {
		name     string
		failures map[string]error
		// edgeStatus, when set, is what x.example answers every request with.
		edgeStatus int
	}{
		{name: "a refused connection", failures: map[string]error{unreachableHost: connectionRefused()}},
		{name: "an edge answering 522", edgeStatus: 522},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.subscribeTechnology()
			seedDeadHostStreak(t, h, unreachableHost)
			var edgeRequests atomic.Int64
			if tt.edgeStatus != 0 {
				h.mux.HandleFunc("GET "+unreachableHost+"/", func(w http.ResponseWriter, _ *http.Request) {
					edgeRequests.Add(1)
					w.WriteHeader(tt.edgeStatus)
				})
			}
			rows := seedSweepRows(t, h, unreachableHost, 50)
			rig := newUnreachableRig(t, h, tt.failures, nil)

			require.NoError(t, rig.backfill.RetryDeferred(context.Background(), 50))

			requests := rig.transport.attemptsTo(unreachableHost) + rig.probes.requestsTo(unreachableHost) + int(edgeRequests.Load())
			assert.Equal(t, 1, requests, "requests to x.example during the sweep")
			want := []store.DeferredItem{}
			for i, row := range rows {
				if i < 20 {
					row = rescheduledToSixHours(row)
				}
				want = append(want, row)
			}
			assert.Equal(t, want, deferredRows(t, h), "the first 20 rows are rescheduled, the other 30 untouched")
		})
	}
}

// serveEdgeHost routes every request to host on the fixture server: a path in
// objects is served as JSON, its nodeinfo answers 404, and any other path
// answers 522, the way a CDN edge reports its origin down. It returns the
// count of requests to host.
func serveEdgeHost(t *testing.T, h *harness, host string, objects map[string]map[string]any) *atomic.Int64 {
	t.Helper()
	bodies := map[string][]byte{}
	for path, obj := range objects {
		body, err := json.Marshal(obj)
		require.NoError(t, err)
		bodies[path] = body
	}
	var requests atomic.Int64
	h.mux.HandleFunc("GET "+host+"/", func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if body, ok := bodies[r.URL.Path]; ok {
			w.Header().Set("Content-Type", ap.ContentTypeActivityJSON)
			_, _ = w.Write(body)
			return
		}
		if r.URL.Path == nodeInfoPath {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(522)
	})
	return &requests
}

// TestBackfillRepliesOnEdgeDownHostCostOneRequest: a post that lands with
// five replies on a host whose CDN edge answers 522 costs that host one
// request, in a walk and in a sweep alike: the first reply's 522 is the
// host's verdict, and the other four are deferred without a fetch. Each reply
// gets its own row, first due 1h out.
func TestBackfillRepliesOnEdgeDownHostCostOneRequest(t *testing.T) {
	const (
		edgeDownHost = "v.example"
		sweptPostIRI = "https://y.example/post/edge-replies"
		authorIRI    = "https://y.example/u/deferred"
	)
	replies := func(inReplyTo string) []any {
		items := []any{}
		for i := 1; i <= 5; i++ {
			items = append(items, note(fmt.Sprintf("https://v.example/comment/%d", i), "https://v.example/u/gone",
				inReplyTo, fmt.Sprintf("reply %d on an edge-down host", i), "2026-11-07T10:00:00.000000Z"))
		}
		return items
	}
	wantRows := [][2]string{
		{"https://v.example/comment/1", "Note"},
		{"https://v.example/comment/2", "Note"},
		{"https://v.example/comment/3", "Note"},
		{"https://v.example/comment/4", "Note"},
		{"https://v.example/comment/5", "Note"},
	}
	tests := []struct {
		name string
		// postIRI is the post that lands and carries the replies.
		postIRI string
		// serve registers the post and its replies collection.
		serve func(t *testing.T, h *harness)
		// pass runs the walk or the sweep.
		pass func(t *testing.T, rig *unreachableRig) error
	}{
		{
			name:    "a walk",
			postIRI: pageID,
			serve: func(t *testing.T, h *harness) {
				serveOutboxFixtures(t, h)
				h.serveObject("/post/49131386/replies", map[string]any{
					"type":         "OrderedCollection",
					"id":           pageID + "/replies",
					"totalItems":   5,
					"orderedItems": replies(pageID),
				})
			},
			pass: func(t *testing.T, rig *unreachableRig) error {
				err := rig.run(t)
				assert.NotNil(t, rig.h.lastBackfillAt(groupID), "a walk whose only failures are deferred replies stamps last_backfill_at")
				return err
			},
		},
		{
			name:    "a sweep",
			postIRI: sweptPostIRI,
			serve: func(t *testing.T, h *harness) {
				h.serveObject("/u/deferred", person(authorIRI, "deferred", nil))
				h.serveObject("/post/edge-replies", map[string]any{
					"id":           sweptPostIRI,
					"type":         "Page",
					"attributedTo": authorIRI,
					"audience":     groupID,
					"to":           []any{groupID, ap.PublicAudience},
					"name":         "a post whose replies are on an edge-down host",
					"content":      "<p>with replies</p>",
					"published":    "2026-11-07T09:00:00.000000Z",
					"replies":      sweptPostIRI + "/replies",
				})
				h.serveObject("/post/edge-replies/replies", map[string]any{
					"type":         "OrderedCollection",
					"id":           sweptPostIRI + "/replies",
					"totalItems":   5,
					"orderedItems": replies(sweptPostIRI),
				})
				require.NoError(t, store.NewDeferredBackfillItems(h.db).DeferItem(context.Background(), store.DeferredItem{
					ObjectIRI: sweptPostIRI, CommunityAPID: groupID, CommunityDID: h.communityDID(groupID),
					Host: "y.example", ObjectType: "Page", Attempts: 0,
					CreatedAt:   time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC),
					NextRetryAt: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC),
				}))
			},
			pass: func(t *testing.T, rig *unreachableRig) error {
				return rig.backfill.RetryDeferred(context.Background(), 50)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.subscribeTechnology()
			tt.serve(t, h)
			requests := serveEdgeHost(t, h, edgeDownHost, nil)
			rig := newUnreachableRig(t, h, nil, nil)

			require.NoError(t, tt.pass(t, rig))

			assert.Equal(t, int64(1), requests.Load(), "requests to v.example")
			assert.Equal(t, walkDeferredRows(h, edgeDownHost, wantRows...), deferredRows(t, h),
				"each reply has its own row, first due 1h out; the swept post's row is gone")
			_, err := h.objects.GetByAPID(context.Background(), tt.postIRI)
			assert.NoError(t, err, "the post carrying the replies lands")
		})
	}
}

// TestBackfillRetryDeferredRecoveredHostCap: a sweep over 50 due rows on a
// host that answers again attempts 20 object fetches from it, however large
// the batch limit, and counts attempts, not successes (an origin answering
// 502 still costs 20). The allowance is per sweep: a second sweep on the same
// Backfill attempts the next 20.
func TestBackfillRetryDeferredRecoveredHostCap(t *testing.T) {
	const recoveredHost = "w.example"
	tests := []struct {
		name   string
		status int
		limit  int
	}{
		{name: "the origin serves the objects", limit: 50},
		{name: "the origin answers 502 for every object", status: http.StatusBadGateway, limit: 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			h.subscribeTechnology()
			authorIRI := "https://" + recoveredHost + "/u/author"
			var objectFetches atomic.Int64
			h.mux.HandleFunc("GET "+recoveredHost+"/post/{id}", func(w http.ResponseWriter, r *http.Request) {
				objectFetches.Add(1)
				w.Header().Set("Content-Type", ap.ContentTypeActivityJSON)
				if tt.status != 0 {
					w.WriteHeader(tt.status)
					return
				}
				body, err := json.Marshal(map[string]any{
					"id":           "https://" + r.Host + "/post/" + r.PathValue("id"),
					"type":         "Page",
					"attributedTo": authorIRI,
					"audience":     groupID,
					"to":           []any{groupID, ap.PublicAudience},
					"name":         "a post from a host that came back",
					"content":      "<p>served again</p>",
					"published":    "2026-11-07T09:00:00.000000Z",
				})
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				_, _ = w.Write(body)
			})
			h.mux.HandleFunc("GET "+recoveredHost+"/u/author", func(w http.ResponseWriter, _ *http.Request) {
				body, err := json.Marshal(person(authorIRI, "author", nil))
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}
				w.Header().Set("Content-Type", ap.ContentTypeActivityJSON)
				_, _ = w.Write(body)
			})
			rows := seedSweepRows(t, h, recoveredHost, 50)
			rig := newUnreachableRig(t, h, nil, nil)
			ctx := context.Background()

			// After a sweep over attempted rows 0..attempted-1: each landed
			// (row gone, post mapped) or, on a 502, was rescheduled; the rest
			// are untouched and still due.
			check := func(sweep string, attempted int) {
				want := []store.DeferredItem{}
				for i, row := range rows {
					switch {
					case i >= attempted:
						want = append(want, row)
					case tt.status != 0:
						want = append(want, rescheduledToSixHours(row))
					}
				}
				assert.Equal(t, want, deferredRows(t, h), "deferred rows after the %s sweep", sweep)
				if tt.status != 0 {
					return
				}
				for i, row := range rows {
					_, err := h.objects.GetByAPID(ctx, row.ObjectIRI)
					if i < attempted {
						assert.NoError(t, err, "%s must be materialized after the %s sweep", row.ObjectIRI, sweep)
					} else {
						assert.True(t, errors.IsNotFound(err), "%s must not be materialized after the %s sweep", row.ObjectIRI, sweep)
					}
				}
			}

			require.NoError(t, rig.backfill.RetryDeferred(ctx, tt.limit))
			assert.Equal(t, int64(20), objectFetches.Load(), "object fetches from w.example in the first sweep")
			check("first", 20)

			require.NoError(t, rig.backfill.RetryDeferred(ctx, tt.limit))
			assert.Equal(t, int64(40), objectFetches.Load(), "object fetches from w.example after the second sweep")
			check("second", 40)
		})
	}
}

// TestBackfillDeferredScheduleSurvivesRestart: an item a walk defers at
// backfillNow is first due 1h later, and a brand-new Backfill over the same
// database honours that: its sweep at 59 minutes leaves the row unfetched, its
// sweep at 61 minutes lands the object and removes the row. Between the walk
// and the sweeps only the fixture changes (the host serves again); nothing
// clears the host's streak.
func TestBackfillDeferredScheduleSurvivesRestart(t *testing.T) {
	h := newHarness(t)
	h.subscribeTechnology()
	serveUnreachableOutbox(h, 1, false)
	walk := newUnreachableRig(t, h, map[string]error{unreachableHost: connectionRefused()}, nil)
	require.NoError(t, walk.run(t))
	require.Len(t, deferredRows(t, h), 1, "the walk defers the item")

	h.serveObject("/u/gone", person("https://x.example/u/gone", "gone", nil))
	h.serveObject("/post/1", map[string]any{
		"id":           unreachablePageIRI(1),
		"type":         "Page",
		"attributedTo": "https://x.example/u/gone",
		"audience":     groupID,
		"to":           []any{groupID, ap.PublicAudience},
		"name":         "post 1 from a host that came back",
		"content":      "<p>served again</p>",
		"published":    "2026-11-07T09:00:00.000000Z",
	})
	restarted := newBackfill(t, h, 100)
	ctx := context.Background()

	restarted.now = func() time.Time { return backfillNow.Add(59 * time.Minute) }
	require.NoError(t, restarted.RetryDeferred(ctx, 50))
	assert.Equal(t, 0, h.hitCount("/post/1"), "the row is not due 59 minutes after the walk")
	assert.Equal(t, walkDeferredRows(h, unreachableHost, [2]string{unreachablePageIRI(1), "Page"}),
		deferredRows(t, h), "the row is unchanged 59 minutes after the walk")

	restarted.now = func() time.Time { return backfillNow.Add(61 * time.Minute) }
	require.NoError(t, restarted.RetryDeferred(ctx, 50))
	assert.Equal(t, []store.DeferredItem{}, deferredRows(t, h), "the row goes once its object lands, 61 minutes after the walk")
	_, err := h.objects.GetByAPID(ctx, unreachablePageIRI(1))
	assert.NoError(t, err, "the deferred post is materialized 61 minutes after the walk")
}
