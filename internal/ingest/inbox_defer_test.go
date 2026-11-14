package ingest

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/ap"
	"tidepool/internal/store"
	"tidepool/internal/votes"
)

// deferGoneAuthor is the author of the content on x.example.
const deferGoneAuthor = "https://x.example/u/gone"

// newInboxDeferRig is newUnreachableRigWith with the dispatcher rebuilt over
// the rig's client and materializer, and the queue drain() pumps given the
// rig's Backfill as its Deferrer and the default attempt cap: one fixture
// client serves the handler's fetches, the materializer, the backfill and
// the nodeinfo prober.
func newInboxDeferRig(t *testing.T, h *harness, failures map[string]error, answers map[string]probeAnswer, adjust func(*BackfillOptions)) *unreachableRig {
	t.Helper()
	rig := newUnreachableRigWith(t, h, failures, answers, adjust)
	h.rebuildDispatch(rig.materializer, rig.client, QueueOptions{Deferrer: rig.backfill})
	return rig
}

// storedEvent reads activityID's inbox event row.
func (h *harness) storedEvent(activityID string) *store.InboxEvent {
	h.t.Helper()
	event, err := h.events.GetEvent(context.Background(), activityID)
	require.NoError(h.t, err)
	return event
}

// makeEventDue moves activityID's next attempt into the past so the next
// claim takes it, without sleeping through its backoff.
func (h *harness) makeEventDue(activityID string) {
	h.t.Helper()
	_, err := h.db.ExecContext(context.Background(),
		`UPDATE inbox_events SET next_attempt_at = CURRENT_TIMESTAMP - interval '1 second' WHERE activity_id = $1`,
		activityID)
	require.NoError(h.t, err)
}

// goneHostPage is a Page on x.example in the technology community.
func goneHostPage(id, name string) map[string]any {
	return map[string]any{
		"id":           id,
		"type":         "Page",
		"attributedTo": deferGoneAuthor,
		"audience":     groupID,
		"to":           []any{groupID, ap.PublicAudience},
		"name":         name,
		"content":      "<p>" + name + "</p>",
		"published":    "2026-11-13T08:00:00.000000Z",
	}
}

// serveLemmyCounts answers the technology host's resolve_object for postIRI
// with up and down; any other query is Lemmy's miss.
func serveLemmyCounts(h *harness, postIRI string, up, down int) {
	h.mux.HandleFunc("GET /api/v3/resolve_object", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("q") != postIRI {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"couldnt_find_object"}`))
			return
		}
		_, _ = fmt.Fprintf(w, `{"post":{"counts":{"upvotes":%d,"downvotes":%d}}}`, up, down)
	})
}

// voteCounts reads subject's served vote totals.
func voteCounts(t *testing.T, h *harness, subject string) [2]int {
	t.Helper()
	var counts [2]int
	err := h.db.QueryRowContext(context.Background(),
		`SELECT upvotes, downvotes FROM vote_aggregates WHERE subject_ap_id = $1`, subject).
		Scan(&counts[0], &counts[1])
	require.NoError(t, err, "a vote_aggregates row for %s", subject)
	return counts
}

// TestInboxDefersUnreachableContent (A1): an announced Note on an
// unreachable host, a reachable comment whose parent's host is unreachable,
// and an announced Page on an unreachable host are each processed on their
// first attempt, with one deferred row naming the content IRI, the community
// and the unreachable host, first due 1h later. The next event in the same
// community is processed in the same drain. Once the host answers, a
// RetryDeferred sweep materializes the content, and the Page's vote counts
// are seeded from the community host's counts API.
func TestInboxDefersUnreachableContent(t *testing.T) {
	const (
		noteIRI       = "https://x.example/comment/11"
		replyIRI      = "https://y.example/comment/21"
		replyAuthor   = "https://y.example/u/ywriter"
		goneParentIRI = "https://x.example/post/8"
		pageIRI       = "https://x.example/post/12"
	)
	tests := []struct {
		name string
		// arrange serves what is reachable at delivery and returns the
		// Announce, the deferred content IRI and its type.
		arrange func(t *testing.T, h *harness, group *remoteActor) (announce map[string]any, contentIRI, objectType string)
		// recover serves x.example's objects once it answers again.
		recover          func(t *testing.T, h *harness)
		wantMaterialized []string
		// wantCounts, when set, is the served (up, down) of the content.
		wantCounts *[2]int
	}{
		{
			name: "an announced Note on an unreachable host",
			arrange: func(t *testing.T, h *harness, group *remoteActor) (map[string]any, string, string) {
				parent := h.bridgePost(group, "a1-note-parent")
				comment := note(noteIRI, deferGoneAuthor, parent, "a reply from an unreachable host", "2026-11-13T08:00:00.000000Z")
				return announceCreateNote("https://lemmy.world/activities/announce/create/a1-note", deferGoneAuthor, comment),
					noteIRI, "Note"
			},
			recover: func(t *testing.T, h *harness) {
				h.serveObject("/u/gone", person(deferGoneAuthor, "gone", nil))
				parent := "https://lemmy.world/post/a1-note-parent"
				h.serveObject("/comment/11", note(noteIRI, deferGoneAuthor, parent, "a reply from an unreachable host", "2026-11-13T08:00:00.000000Z"))
			},
			wantMaterialized: []string{noteIRI},
		},
		{
			name: "a reachable comment whose parent's host is unreachable",
			arrange: func(t *testing.T, h *harness, _ *remoteActor) (map[string]any, string, string) {
				h.serveObject("/u/ywriter", person(replyAuthor, "ywriter", nil))
				comment := note(replyIRI, replyAuthor, goneParentIRI, "a reply to a post on an unreachable host", "2026-11-13T08:00:00.000000Z")
				h.serveObject("/comment/21", comment)
				return announceCreateNote("https://lemmy.world/activities/announce/create/a1-reply", replyAuthor, comment),
					replyIRI, "Note"
			},
			recover: func(t *testing.T, h *harness) {
				h.serveObject("/u/gone", person(deferGoneAuthor, "gone", nil))
				h.serveObject("/post/8", goneHostPage(goneParentIRI, "a parent on a host that came back"))
			},
			wantMaterialized: []string{replyIRI, goneParentIRI},
		},
		{
			name: "an announced Page on an unreachable host",
			arrange: func(t *testing.T, h *harness, _ *remoteActor) (map[string]any, string, string) {
				return announceCreateNote("https://lemmy.world/activities/announce/create/a1-page", deferGoneAuthor,
						goneHostPage(pageIRI, "a post from an unreachable host")),
					pageIRI, "Page"
			},
			recover: func(t *testing.T, h *harness) {
				h.serveObject("/u/gone", person(deferGoneAuthor, "gone", nil))
				h.serveObject("/post/12", goneHostPage(pageIRI, "a post from an unreachable host"))
			},
			wantMaterialized: []string{pageIRI},
			wantCounts:       &[2]int{17, 4},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			h := newHarness(t)
			group := h.subscribeTechnology()
			h.serveLemmyWorldContent()
			aggregator := withRealAggregator(t, h)
			serveLemmyCounts(h, pageIRI, 17, 4)
			rig := newInboxDeferRig(t, h,
				map[string]error{unreachableHost: connectionRefused()},
				map[string]probeAnswer{unreachableHost: probeJSON},
				func(opts *BackfillOptions) {
					seeder, err := votes.NewLemmySeeder(aggregator,
						&http.Client{Transport: rewriteTransport{target: h.fixtures.Listener.Addr().String()}},
						"tidepool-test/1.0", nil)
					require.NoError(t, err)
					opts.Seeder = seeder
				})
			announce, contentIRI, objectType := tt.arrange(t, h, group)
			// Same-authority with the community, so it bridges with no fetch.
			const behindPostID = "https://lemmy.world/post/a1-behind"
			behind := announceCreateNote("https://lemmy.world/activities/announce/create/a1-behind", personID, map[string]any{
				"id":           behindPostID,
				"type":         "Page",
				"attributedTo": personID,
				"audience":     groupID,
				"to":           []any{groupID, ap.PublicAudience},
				"name":         "the post queued behind",
				"content":      "<p>the post queued behind</p>",
				"published":    "2026-11-13T08:05:00.000000Z",
			})

			require.Equal(t, http.StatusAccepted, h.deliver(group, announce))
			require.Equal(t, http.StatusAccepted, h.deliver(group, behind))
			require.Equal(t, h.storedEvent(announce["id"].(string)).OrderingKey, h.storedEvent(behind["id"].(string)).OrderingKey,
				"precondition: both events are in the same ordering key")
			h.drain()

			failing := h.storedEvent(announce["id"].(string))
			assert.NotNil(t, failing.ProcessedAt, "the event whose content is on an unreachable host is processed on its first attempt")
			assert.Nil(t, failing.FailedAt, "the deferred event is not poisoned")
			assert.Equal(t, 1, failing.Attempts, "one attempt")
			assert.NotNil(t, h.storedEvent(behind["id"].(string)).ProcessedAt,
				"the next event in the same community is processed in the same drain")
			_, err := h.objects.GetByAPID(ctx, behindPostID)
			assert.NoError(t, err, "the post behind the deferred event is materialized")
			assert.Equal(t, walkDeferredRows(h, unreachableHost, [2]string{contentIRI, objectType}), deferredRows(t, h),
				"one deferred row for the content, on x.example, first due 1h later")
			requireNotMaterialized(t, h, contentIRI)

			rig.transport.heal(unreachableHost)
			tt.recover(t, h)
			rig.backfill.now = func() time.Time { return backfillNow.Add(61 * time.Minute) }
			require.NoError(t, rig.backfill.RetryDeferred(ctx, 50))

			assert.Equal(t, []store.DeferredItem{}, deferredRows(t, h), "the row goes once its content lands")
			for _, id := range tt.wantMaterialized {
				_, err := h.objects.GetByAPID(ctx, id)
				assert.NoError(t, err, "%s is materialized once its host answers", id)
			}
			if tt.wantCounts != nil {
				assert.Equal(t, *tt.wantCounts, voteCounts(t, h, contentIRI), "the recovered post's counts are seeded from the counts API")
			}
		})
	}
}

// TestInboxSkipsObjectNotServed (A2): an announced Page whose origin answers
// text/html for the object while its nodeinfo answers JSON is a skip on the
// first attempt: processed, nothing deferred, nothing materialized.
func TestInboxSkipsObjectNotServed(t *testing.T) {
	const htmlPageIRI = "https://y.example/post/31"
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.mux.HandleFunc("GET /post/31", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html><body>the instance frontend</body></html>"))
	})
	newInboxDeferRig(t, h, nil, map[string]probeAnswer{"y.example": probeJSON}, nil)
	page := goneHostPage(htmlPageIRI, "a post its host does not serve")
	page["attributedTo"] = "https://y.example/u/someone"
	announce := announceCreateNote("https://lemmy.world/activities/announce/create/a2-html", "https://y.example/u/someone", page)

	require.Equal(t, http.StatusAccepted, h.deliver(group, announce))
	h.drain()

	event := h.storedEvent(announce["id"].(string))
	assert.NotNil(t, event.ProcessedAt, "an object its live host does not serve is skipped on the first attempt")
	assert.Nil(t, event.FailedAt, "the skipped event is not poisoned")
	assert.Equal(t, 1, event.Attempts, "one attempt")
	assert.Equal(t, []store.DeferredItem{}, deferredRows(t, h), "a skip defers nothing")
	requireNotMaterialized(t, h, htmlPageIRI)
}

// TestInboxRetriesThenDefersBadGateway (A3): an announced Note whose origin
// answers 502 is released for retry on attempts 1 and 2, and deferred on
// attempt 3 with the object's host: processed, not poisoned.
func TestInboxRetriesThenDefersBadGateway(t *testing.T) {
	const badGatewayIRI = "https://z.example/comment/13"
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	newInboxDeferRig(t, h, nil, nil, nil)
	parent := h.bridgePost(group, "a3-parent")
	h.serveStatus("/comment/13", http.StatusBadGateway)
	announce := announceCreateNote("https://lemmy.world/activities/announce/create/a3-502", "https://z.example/u/a",
		note(badGatewayIRI, "https://z.example/u/a", parent, "a reply its origin fails to serve", "2026-11-13T08:00:00.000000Z"))
	activityID := announce["id"].(string)

	require.Equal(t, http.StatusAccepted, h.deliver(group, announce))
	for attempt := 1; attempt <= 2; attempt++ {
		h.drain()
		event := h.storedEvent(activityID)
		assert.Equal(t, attempt, event.Attempts, "attempt %d ran", attempt)
		assert.Nil(t, event.ProcessedAt, "attempt %d is released for retry, not processed", attempt)
		assert.Nil(t, event.FailedAt, "attempt %d is not poisoned", attempt)
		assert.Equal(t, []store.DeferredItem{}, deferredRows(t, h), "nothing is deferred after attempt %d", attempt)
		h.makeEventDue(activityID)
	}
	h.drain()

	event := h.storedEvent(activityID)
	assert.Equal(t, 3, event.Attempts, "three attempts")
	assert.NotNil(t, event.ProcessedAt, "the third failure is deferred and the event processed")
	assert.Nil(t, event.FailedAt, "the deferred event is not poisoned")
	assert.Equal(t, walkDeferredRows(h, "z.example", [2]string{badGatewayIRI, "Note"}), deferredRows(t, h),
		"one deferred row on the object's host")
	requireNotMaterialized(t, h, badGatewayIRI)
}

// TestDeferredCreateActivityRecovers (A4): a deferred row whose object IRI
// is a Create activity (DeferEvent's row for an Announce of a bare activity
// IRI: no object type) recovers by materializing the Create's inner Note.
func TestDeferredCreateActivityRecovers(t *testing.T) {
	const (
		createIRI = "https://x.example/activities/create/41"
		innerIRI  = "https://x.example/comment/42"
	)
	ctx := context.Background()
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	rig := newInboxDeferRig(t, h, nil, nil, nil)
	parent := h.bridgePost(group, "a4-parent")
	h.serveObject("/u/gone", person(deferGoneAuthor, "gone", nil))
	h.serveObject("/activities/create/41", map[string]any{
		"id":       createIRI,
		"type":     "Create",
		"actor":    deferGoneAuthor,
		"audience": groupID,
		"to":       []any{ap.PublicAudience},
		"object":   note(innerIRI, deferGoneAuthor, parent, "a reply announced as a bare activity IRI", "2026-11-13T08:00:00.000000Z"),
	})
	// Recovery fetches the inner content from its own URL (current state).
	h.serveObject("/comment/42", note(innerIRI, deferGoneAuthor, parent, "a reply announced as a bare activity IRI", "2026-11-13T08:00:00.000000Z"))
	row := store.DeferredItem{
		ObjectIRI: createIRI, CommunityAPID: groupID, CommunityDID: h.communityDID(groupID),
		Host: unreachableHost, ObjectType: "", Attempts: 0,
		CreatedAt: backfillNow, NextRetryAt: firstDeferredRetry,
	}
	require.NoError(t, store.NewDeferredBackfillItems(h.db).DeferItem(ctx, row))
	require.Equal(t, []store.DeferredItem{row}, deferredRows(t, h), "the row is in the table before the sweep")

	rig.backfill.now = func() time.Time { return backfillNow.Add(61 * time.Minute) }
	require.NoError(t, rig.backfill.RetryDeferred(ctx, 50))

	_, err := h.objects.GetByAPID(ctx, innerIRI)
	assert.NoError(t, err, "the Create's inner Note is materialized")
	assert.Equal(t, []store.DeferredItem{}, deferredRows(t, h), "the row goes once its content lands")
}

// TestDeferredCreateActivityRecoversCurrentState (F3, A4 variants): a
// deferred row naming a Create activity recovers the inner content's current
// state, fetched from its own URL, never the snapshot the Create embeds: a
// changed body lands as served now; a content URL that now answers a
// Tombstone, or an inner IRI with a local tombstone, lands nothing and the
// row is dropped.
func TestDeferredCreateActivityRecoversCurrentState(t *testing.T) {
	const (
		createIRI    = "https://x.example/activities/create/41"
		innerIRI     = "https://x.example/comment/42"
		embeddedBody = "the body embedded when it was announced"
		currentBody  = "the body the origin serves now"
	)
	tests := []struct {
		name string
		// arrange serves or records the inner content's current state.
		arrange func(t *testing.T, h *harness, parent string)
		// wantContent, when set, is the recovered record's content; empty
		// means nothing is materialized.
		wantContent string
	}{
		{
			name: "the content URL serves a changed body",
			arrange: func(t *testing.T, h *harness, parent string) {
				h.serveObject("/comment/42", note(innerIRI, deferGoneAuthor, parent, currentBody, "2026-11-13T08:00:00.000000Z"))
			},
			wantContent: currentBody,
		},
		{
			name: "the content URL answers a Tombstone",
			arrange: func(t *testing.T, h *harness, _ string) {
				h.serveTombstone("/comment/42", innerIRI, http.StatusGone)
			},
		},
		{
			name: "the inner IRI has a local tombstone",
			arrange: func(t *testing.T, h *harness, parent string) {
				h.serveObject("/comment/42", note(innerIRI, deferGoneAuthor, parent, currentBody, "2026-11-13T08:00:00.000000Z"))
				require.NoError(t, h.tombstones.Record(context.Background(), innerIRI, ""))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			h := newHarness(t)
			group := h.subscribeTechnology()
			h.serveLemmyWorldContent()
			rig := newInboxDeferRig(t, h, nil, nil, nil)
			parent := h.bridgePost(group, "a4-parent")
			h.serveObject("/u/gone", person(deferGoneAuthor, "gone", nil))
			h.serveObject("/activities/create/41", map[string]any{
				"id":       createIRI,
				"type":     "Create",
				"actor":    deferGoneAuthor,
				"audience": groupID,
				"to":       []any{ap.PublicAudience},
				"object":   note(innerIRI, deferGoneAuthor, parent, embeddedBody, "2026-11-13T08:00:00.000000Z"),
			})
			tt.arrange(t, h, parent)
			require.NoError(t, store.NewDeferredBackfillItems(h.db).DeferItem(ctx, store.DeferredItem{
				ObjectIRI: createIRI, CommunityAPID: groupID, CommunityDID: h.communityDID(groupID),
				Host: unreachableHost, ObjectType: "", Attempts: 0,
				CreatedAt: backfillNow, NextRetryAt: firstDeferredRetry,
			}))

			rig.backfill.now = func() time.Time { return backfillNow.Add(61 * time.Minute) }
			require.NoError(t, rig.backfill.RetryDeferred(ctx, 50))

			assert.Equal(t, []store.DeferredItem{}, deferredRows(t, h), "the row is gone after the sweep")
			if tt.wantContent == "" {
				requireNotMaterialized(t, h, innerIRI)
				return
			}
			mapping, err := h.objects.GetByAPID(ctx, innerIRI)
			require.NoError(t, err, "the inner Note is materialized")
			record, _, err := h.manager.GetRecord(ctx, mapping.DID, mapping.Collection, mapping.RKey)
			require.NoError(t, err)
			assert.Equal(t, tt.wantContent, record["content"], "the recovered content is the content URL's current body")
		})
	}
}

// TestRecoveredInboxContentIsCommunityAnnounced (F5): a comment deferred
// from the community's Announce, recovered by a RetryDeferred sweep, is
// recorded as community-announced; the ancestor the recovery fetched is not.
func TestRecoveredInboxContentIsCommunityAnnounced(t *testing.T) {
	const (
		replyIRI      = "https://y.example/comment/61"
		replyAuthor   = "https://y.example/u/ywriter"
		goneParentIRI = "https://x.example/post/62"
	)
	ctx := context.Background()
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	rig := newInboxDeferRig(t, h,
		map[string]error{unreachableHost: connectionRefused()},
		map[string]probeAnswer{unreachableHost: probeJSON}, nil)
	h.serveObject("/u/ywriter", person(replyAuthor, "ywriter", nil))
	comment := note(replyIRI, replyAuthor, goneParentIRI, "a reply to a post on an unreachable host", "2026-11-13T08:00:00.000000Z")
	h.serveObject("/comment/61", comment)
	announce := announceCreateNote("https://lemmy.world/activities/announce/create/f5-reply", replyAuthor, comment)

	require.Equal(t, http.StatusAccepted, h.deliver(group, announce))
	h.drain()
	require.Equal(t, walkDeferredRows(h, unreachableHost, [2]string{replyIRI, "Note"}), deferredRows(t, h),
		"precondition: the reply is deferred")

	rig.transport.heal(unreachableHost)
	h.serveObject("/u/gone", person(deferGoneAuthor, "gone", nil))
	h.serveObject("/post/62", goneHostPage(goneParentIRI, "a parent on a host that came back"))
	rig.backfill.now = func() time.Time { return backfillNow.Add(61 * time.Minute) }
	require.NoError(t, rig.backfill.RetryDeferred(ctx, 50))
	require.Equal(t, []store.DeferredItem{}, deferredRows(t, h), "precondition: the reply landed")

	assert.Equal(t, "community_announced", h.arrival(replyIRI), "the recovered inbox content was announced by its community")
	assert.Equal(t, "not_announced", h.arrival(goneParentIRI), "the fetched ancestor was not")
}

// TestRecoveredBackfillContentStaysNotAnnounced (F5): a comment deferred by
// a backfill walk (a row of source 'backfill'), recovered by a sweep, keeps
// arrival 'not_announced'.
func TestRecoveredBackfillContentStaysNotAnnounced(t *testing.T) {
	const commentIRI = "https://x.example/comment/71"
	ctx := context.Background()
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	rig := newInboxDeferRig(t, h, nil, nil, nil)
	parent := h.bridgePost(group, "f5-backfill-parent")
	h.serveObject("/u/gone", person(deferGoneAuthor, "gone", nil))
	h.serveObject("/comment/71", note(commentIRI, deferGoneAuthor, parent, "a reply a walk deferred", "2026-11-13T08:00:00.000000Z"))
	require.NoError(t, store.NewDeferredBackfillItems(h.db).DeferItem(ctx, store.DeferredItem{
		ObjectIRI: commentIRI, CommunityAPID: groupID, CommunityDID: h.communityDID(groupID),
		Host: unreachableHost, ObjectType: "Note", Attempts: 0,
		CreatedAt: backfillNow, NextRetryAt: firstDeferredRetry,
	}))
	var source string
	require.NoError(t, h.db.QueryRowContext(ctx,
		`SELECT source FROM backfill_deferred_items WHERE object_iri = $1`, commentIRI).Scan(&source))
	require.Equal(t, "backfill", source, "precondition: a walk's row is of source 'backfill'")

	rig.backfill.now = func() time.Time { return backfillNow.Add(61 * time.Minute) }
	require.NoError(t, rig.backfill.RetryDeferred(ctx, 50))
	require.Equal(t, []store.DeferredItem{}, deferredRows(t, h), "precondition: the comment landed")

	assert.Equal(t, "not_announced", h.arrival(commentIRI), "recovered backfill content is not community-announced")
}

// TestRecoveredInboxPostIsAnnouncedWhileRepliesHostIsGone (G2): an announced
// Page deferred from the community's Announce recovers while its replies
// collection's host is unreachable. The post lands and is community-announced
// at once; its row is kept for the replies, rescheduled to its next step
// (6h after the deferral) with one attempt counted.
func TestRecoveredInboxPostIsAnnouncedWhileRepliesHostIsGone(t *testing.T) {
	const (
		pageIRI      = "https://x.example/post/81"
		repliesHost  = "r.example"
		repliesIRI   = "https://r.example/post/81/replies"
		announceIRI  = "https://lemmy.world/activities/announce/create/g2-page"
		pageName     = "a post whose replies are on an unreachable host"
		deferredType = "Page"
	)
	ctx := context.Background()
	h := newHarness(t)
	group := h.subscribeTechnology()
	h.serveLemmyWorldContent()
	rig := newInboxDeferRig(t, h,
		map[string]error{unreachableHost: connectionRefused(), repliesHost: connectionRefused()},
		map[string]probeAnswer{unreachableHost: probeJSON, repliesHost: probeJSON}, nil)
	page := goneHostPage(pageIRI, pageName)
	page["replies"] = repliesIRI

	require.Equal(t, http.StatusAccepted, h.deliver(group, announceCreateNote(announceIRI, deferGoneAuthor, page)))
	h.drain()
	require.Equal(t, walkDeferredRows(h, unreachableHost, [2]string{pageIRI, deferredType}), deferredRows(t, h),
		"precondition: the post is deferred")

	rig.transport.heal(unreachableHost)
	h.serveObject("/u/gone", person(deferGoneAuthor, "gone", nil))
	h.serveObject("/post/81", page)
	rig.backfill.now = func() time.Time { return backfillNow.Add(61 * time.Minute) }
	require.NoError(t, rig.backfill.RetryDeferred(ctx, 50))

	_, err := h.objects.GetByAPID(ctx, pageIRI)
	require.NoError(t, err, "precondition: the post lands")
	assert.Equal(t, "community_announced", h.arrival(pageIRI),
		"the recovered inbox post is community-announced though its replies are still unreachable")
	assert.Equal(t, []store.DeferredItem{{
		ObjectIRI: pageIRI, CommunityAPID: groupID, CommunityDID: h.communityDID(groupID),
		Host: unreachableHost, ObjectType: deferredType, Attempts: 1,
		CreatedAt: backfillNow, NextRetryAt: time.Date(2026, 11, 8, 18, 0, 0, 0, time.UTC),
	}}, deferredRows(t, h), "the row is kept for the replies, rescheduled to its 6h step")
}
