//go:build e2e

package e2e

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	// plcBudgetRetryAfter is the Retry-After the fixture answers with, and
	// plcBudgetPause the same value as a duration.
	plcBudgetRetryAfter = "60"
	plcBudgetPause      = 60 * time.Second
	// plcBudgetTimeout bounds the wait for the renames and the post after the
	// 429. It must stay at least 4 minutes: chunk 03 moves this inbox retry to
	// 2 minutes plus jitter.
	plcBudgetTimeout = 5 * time.Minute
)

// plcFixturePost is one POST the PLC fixture saw, 429s included.
type plcFixturePost struct {
	Path   string    `json:"path"`
	Status int       `json:"status"`
	At     time.Time `json:"at"`
}

// readPLCFixturePosts returns every POST the fixture has seen, in arrival order.
func readPLCFixturePosts() ([]plcFixturePost, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Get(plcFixtureURL() + "/control/posts")
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET /control/posts: status %d", response.StatusCode)
	}
	var list struct {
		Posts []plcFixturePost `json:"posts"`
	}
	if err := json.NewDecoder(response.Body).Decode(&list); err != nil {
		return nil, fmt.Errorf("decode /control/posts: %w", err)
	}
	return list.Posts, nil
}

// armPLCFixtureRateLimit makes the fixture answer the next count POSTs 429.
func armPLCFixtureRateLimit(count int, retryAfter string) error {
	body, err := json.Marshal(map[string]any{"count": count, "retry_after": retryAfter})
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Post(plcFixtureURL()+"/control/rate-limit", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("POST /control/rate-limit: status %d", response.StatusCode)
	}
	return nil
}

// awaitMintRefusalBefore polls the inbox row until a retry records the mint
// rate refusal. Seeing it only at or after deadline fails the test: the mint
// path must run during the pause.
func awaitMintRefusalBefore(t *testing.T, db *sql.DB, activityID string, deadline time.Time) {
	t.Helper()
	ctx := t.Context()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastError sql.NullString
	for time.Now().Before(deadline) {
		if err := db.QueryRowContext(ctx, `SELECT error FROM inbox_events WHERE activity_id = $1`, activityID).Scan(&lastError); err != nil {
			t.Fatalf("read inbox event %s: %v", activityID, err)
		}
		if strings.Contains(lastError.String, "mint rate exceeded") {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for the mint refusal on %s: %v", activityID, ctx.Err())
		case <-ticker.C:
		}
	}
	t.Fatalf("inbox event %s showed no mint rate refusal before the pause ended; last error: %q", activityID, lastError.String)
}

// TestPLCBudget_RateLimitPausesMintsAndRenames: one 429 taken by a rename
// pauses the rename and an unrelated mint together, because both spend the
// one PLC write budget. Both finish after the pause.
func TestPLCBudget_RateLimitPausesMintsAndRenames(t *testing.T) {
	// An absent fixture fails here, before any stack setup.
	_, err := readPLCFixturePosts()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, armPLCFixtureRateLimit(0, plcBudgetRetryAfter)) })

	h := newHarness(t)
	db := openTidepoolDB(t)
	community, _ := setupSubscribedCommunity(t, h, "pbcom")

	type renamedActor struct{ did, handle string }
	var actors []renamedActor
	var dids []string
	for range 2 {
		username := h.uniqueName(t, "pbusr")
		user := h.registerUser(t, username)
		user.createPost(t, community.ID, "PLC budget "+username, "mint user repo")
		handle := bridgedHandle(username)
		var did string
		require.Eventually(t, func() bool {
			result, err := h.bridgeXRPC("/xrpc/com.atproto.identity.resolveHandle", url.Values{"handle": {handle}})
			if err != nil || result.status != http.StatusOK {
				return false
			}
			var resolved struct {
				Did string `json:"did"`
			}
			if json.Unmarshal(result.body, &resolved) != nil {
				return false
			}
			did = resolved.Did
			return did != ""
		}, eventTimeout, 250*time.Millisecond, "minted handle %s did not resolve", handle)
		actors = append(actors, renamedActor{did: did, handle: handle})
		dids = append(dids, did)
	}
	for _, actor := range actors {
		h.prepareOldFormatActor(t, actor.did)
	}
	newAuthorName := h.uniqueName(t, "pbnew")
	newAuthor := h.registerUser(t, newAuthorName)

	var lastInboxEvent int64
	require.NoError(t, db.QueryRowContext(t.Context(), `SELECT COALESCE(MAX(id), 0) FROM inbox_events`).Scan(&lastInboxEvent))
	cursor := cursorNow()
	baseline, err := readPLCFixturePosts()
	require.NoError(t, err)
	require.NoError(t, armPLCFixtureRateLimit(1, plcBudgetRetryAfter))

	body, err := json.Marshal(map[string]any{"dids": dids})
	require.NoError(t, err)
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, tidepoolURL()+"/admin/handles/migrate", bytes.NewReader(body))
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer "+adminToken())
	request.Header.Set("Content-Type", "application/json")
	response, err := h.http.Do(request)
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, response.StatusCode)
	require.NoError(t, response.Body.Close())

	limitedIndex := -1
	var limited plcFixturePost
	require.Eventually(t, func() bool {
		posts, err := readPLCFixturePosts()
		if err != nil {
			return false
		}
		for index := len(baseline); index < len(posts); index++ {
			if posts[index].Status == http.StatusTooManyRequests {
				limitedIndex, limited = index, posts[index]
				return true
			}
		}
		return false
	}, eventTimeout, 100*time.Millisecond, "the fixture must answer a rename POST 429")
	require.Contains(t, []string{"/" + actors[0].did, "/" + actors[1].did}, limited.Path, "the 429 must land on a rename")
	pauseEnd := limited.At.Add(plcBudgetPause)

	title := "PLC budget new author " + newAuthorName
	post := newAuthor.createPost(t, community.ID, title, "posted during the pause")
	event := awaitInboxEventNaming(t, db, post.APID, lastInboxEvent)
	// The fixture runs on this host, so the local clock can read its times.
	awaitMintRefusalBefore(t, db, event.activityID, pauseEnd)

	require.Eventually(t, func() bool {
		for _, actor := range actors {
			var handle, pending string
			err := db.QueryRowContext(t.Context(), `SELECT COALESCE(handle, ''), COALESCE(pending_handle, '')
				FROM bridged_actors WHERE did = $1`, actor.did).Scan(&handle, &pending)
			if err != nil || handle != actor.handle || pending != "" {
				return false
			}
		}
		var processed bool
		err := db.QueryRowContext(t.Context(), `SELECT processed_at IS NOT NULL FROM inbox_events WHERE activity_id = $1`, event.activityID).Scan(&processed)
		return err == nil && processed
	}, plcBudgetTimeout, 250*time.Millisecond, "both renames must finalize and the new author's post must be processed")

	listener := h.newListener(t, cursor, colPostV2)
	postEvent := listener.await("the new author's postv2", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "title")
		return e.Commit.Collection == colPostV2 && e.Commit.Operation == opCreate && got == title
	})
	h.awaitRecordPresent(t, postEvent.Did, colPostV2, postEvent.Commit.RKey, "the new author's post after the pause")

	posts, err := readPLCFixturePosts()
	require.NoError(t, err)
	afterLimit := posts[limitedIndex+1:]
	for _, recorded := range afterLimit {
		require.False(t, recorded.At.Before(pauseEnd), "POST to %s at %s is inside the pause that ends at %s",
			recorded.Path, recorded.At.Format(time.RFC3339Nano), pauseEnd.Format(time.RFC3339Nano))
	}
	for _, did := range append(dids, postEvent.Did) {
		require.True(t, slices.ContainsFunc(afterLimit, func(recorded plcFixturePost) bool {
			return recorded.Path == "/"+did
		}), "no POST to %s after the pause", did)
	}
}
