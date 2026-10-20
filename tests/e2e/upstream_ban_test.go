//go:build e2e

package e2e

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"
)

// A Lemmy SITE ban with "remove content" against a REAL Lemmy.
//
// Lemmy removes the banned user's posts and comments at ban time and, for a
// home-instance site ban, federates a DIRECT (never announced) Block whose
// target is the site actor and which carries removeData. The bridge must take
// the user's bridged post and comment down with it. The test also records the
// shape Tidepool actually received, because that shape is the assumption the
// bridge's ban handling rests on.
func TestUpstreamSiteBan_RemovesBridgedContent(t *testing.T) {
	h := newHarness(t)
	community, _ := setupSubscribedCommunity(t, h, "uban")

	username := h.uniqueName(t, "spammy")
	user := h.registerUser(t, username)
	banned := h.admin.person(t, username)
	siteActor := h.admin.siteActorID(t)

	cursor := cursorNow()
	l := h.newListener(t, cursor, colPostV2, colComment)

	title := "Banned author's post " + h.suffix
	post := user.createPost(t, community.ID, title, "will go with the site ban")
	postEv := l.await("postv2 create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "title")
		return e.Commit.Collection == colPostV2 && e.Commit.Operation == opCreate && got == title
	})
	user.createComment(t, post.ID, 0, "banned author's comment")
	commentEv := l.await("comment create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "reply", "parent", "uri")
		return e.Commit.Collection == colComment && e.Commit.Operation == opCreate && got == postEv.atURI()
	})

	h.awaitRecordPresent(t, postEv.Did, colPostV2, postEv.Commit.RKey, "bridged post before the ban")
	h.awaitRecordPresent(t, commentEv.Did, colComment, commentEv.Commit.RKey, "bridged comment before the ban")

	h.admin.banFromSite(t, banned.ID, "spam")

	// The shape Tidepool received: a direct Block of the user, targeting the
	// site actor, with removeData.
	block := awaitInboxBlockOf(t, openTidepoolDB(t), banned.ActorID)
	if got, _ := block["target"].(string); got != siteActor {
		t.Errorf("site ban Block target = %q, want the Lemmy site actor %q (payload %v)", got, siteActor, block)
	}
	if got, _ := block["removeData"].(bool); !got {
		t.Errorf("site ban Block removeData = %v, want true (payload %v)", block["removeData"], block)
	}

	h.awaitRecordGone(t, postEv.Did, colPostV2, postEv.Commit.RKey, "bridged post after the site ban")
	h.awaitRecordGone(t, commentEv.Did, colComment, commentEv.Commit.RKey, "bridged comment after the site ban")
}

// awaitInboxBlockOf polls Tidepool's inbox_events for a DIRECT Block (the
// event's own type is Block, not Announce) whose object is subjectActorID and
// returns its payload. On timeout it reports every event that names the
// subject, so a different delivery shape is recorded rather than lost.
func awaitInboxBlockOf(t *testing.T, db *sql.DB, subjectActorID string) map[string]any {
	t.Helper()
	ctx := t.Context()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.Now().Add(eventTimeout)
	for time.Now().Before(deadline) {
		var payload []byte
		err := db.QueryRowContext(ctx, `
			SELECT payload FROM inbox_events
			WHERE type = 'Block' AND convert_from(payload, 'UTF8')::jsonb->>'object' = $1
			ORDER BY id DESC LIMIT 1`, subjectActorID).Scan(&payload)
		switch {
		case err == nil:
			var block map[string]any
			if err := json.Unmarshal(payload, &block); err != nil {
				t.Fatalf("decode inbox Block payload: %v (%s)", err, truncate(payload, 300))
			}
			return block
		case !errors.Is(err, sql.ErrNoRows):
			t.Fatalf("query inbox_events for a Block of %s: %v", subjectActorID, err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for a Block of %s: %v", subjectActorID, ctx.Err())
		case <-ticker.C:
		}
	}
	rows, err := db.QueryContext(ctx, `
		SELECT type, convert_from(payload, 'UTF8') FROM inbox_events
		WHERE position($1 in convert_from(payload, 'UTF8')) > 0 ORDER BY id`, subjectActorID)
	if err != nil {
		t.Fatalf("no direct Block of %s within %s; listing what arrived failed: %v", subjectActorID, eventTimeout, err)
	}
	defer func() { _ = rows.Close() }()
	var seen []string
	for rows.Next() {
		var activityType, payload string
		if err := rows.Scan(&activityType, &payload); err != nil {
			t.Fatalf("scan inbox_events: %v", err)
		}
		seen = append(seen, activityType+": "+truncate([]byte(payload), 400))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate inbox_events: %v", err)
	}
	t.Fatalf("no direct Block of %s reached Tidepool within %s; events naming the subject: %q",
		subjectActorID, eventTimeout, seen)
	return nil
}

// A Lemmy ADMIN PURGE of a post against a REAL Lemmy.
//
// Purge deletes the post from Lemmy's database outright. Lemmy federates it
// as an announced moderator Delete, so the bridge treats it as a community
// removal: the acceptance is withdrawn and a removal record is written in the
// community's repo, while the author's postv2 stays in the author's repo. The
// test also records the activity Tidepool received and what the purged post's
// AP id serves afterwards, because those are the assumptions purge handling
// rests on.
func TestLemmyPostPurge_RemovesBridgedPost(t *testing.T) {
	h := newHarness(t)
	community, sub := setupSubscribedCommunity(t, h, "purge")

	user := h.registerUser(t, h.uniqueName(t, "purged"))

	cursor := cursorNow()
	l := h.newListener(t, cursor, colPostV2)

	title := "Purged post " + h.suffix
	post := user.createPost(t, community.ID, title, "will be purged by the admin")
	postEv := l.await("postv2 create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "title")
		return e.Commit.Collection == colPostV2 && e.Commit.Operation == opCreate && got == title
	})
	h.awaitRecordPresent(t, postEv.Did, colPostV2, postEv.Commit.RKey, "bridged post before the purge")
	digest := subjectRKey(postEv.atURI())
	h.awaitRecordPresent(t, sub.DID, colAcceptance, digest, "acceptance before the purge")

	// Every inbox event at or below this id predates the purge, including the
	// post's original Create (announced by the community).
	db := openTidepoolDB(t)
	var lastBeforePurge int64
	if err := db.QueryRowContext(t.Context(), `SELECT COALESCE(MAX(id), 0) FROM inbox_events`).Scan(&lastBeforePurge); err != nil {
		t.Fatalf("read the last inbox event id before the purge: %v", err)
	}

	h.admin.purgePost(t, post.ID, "purge e2e")

	event := awaitInboxEventNaming(t, db, post.APID, lastBeforePurge)
	t.Logf("purge of %s delivered to Tidepool as %s (activity id %s, inner %s, summary present: outer %v, inner %v)",
		post.APID, event.activityType, event.activityID, event.innerType, event.outerHasSummary, event.innerHasSummary)

	// End state: removed, not accepted — and the AUTHOR's post untouched.
	h.awaitRecordGone(t, sub.DID, colAcceptance, digest, "acceptance after the purge")
	h.awaitRecordPresent(t, sub.DID, colRemoval, digest, "removal after the purge")
	if _, found := h.bridgeGetRecord(t, postEv.Did, colPostV2, postEv.Commit.RKey); !found {
		t.Error("the author's postv2 was deleted by a purge — the purge arrives as a community " +
			"removal, which is community-scoped; the record belongs to the author")
	}

	status, body := fetchLemmyObject(t, h, post.APID)
	t.Logf("GET %s (application/activity+json) after the purge: status %d, body %s", post.APID, status, truncate(body, 200))
}

// inboxEventShape is what the e2e suite records about one delivered activity.
type inboxEventShape struct {
	activityType    string
	activityID      string
	innerType       string // the type of the activity's object, when it is an object
	outerHasSummary bool
	innerHasSummary bool
}

// awaitInboxEventNaming polls Tidepool's inbox_events for an event received
// after afterID whose payload names apID, and returns its shape. On timeout it
// fails listing every event received after afterID, so an unexpected delivery
// shape (or none) is recorded rather than lost.
func awaitInboxEventNaming(t *testing.T, db *sql.DB, apID string, afterID int64) inboxEventShape {
	t.Helper()
	ctx := t.Context()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.Now().Add(eventTimeout)
	for time.Now().Before(deadline) {
		var event inboxEventShape
		var payload []byte
		err := db.QueryRowContext(ctx, `
			SELECT type, activity_id, payload FROM inbox_events
			WHERE id > $1 AND position($2 in convert_from(payload, 'UTF8')) > 0
			ORDER BY id LIMIT 1`, afterID, apID).Scan(&event.activityType, &event.activityID, &payload)
		switch {
		case err == nil:
			var activity map[string]any
			if err := json.Unmarshal(payload, &activity); err != nil {
				t.Fatalf("decode inbox event %s payload: %v (%s)", event.activityID, err, truncate(payload, 300))
			}
			_, event.outerHasSummary = activity["summary"]
			if inner, ok := activity["object"].(map[string]any); ok {
				event.innerType, _ = inner["type"].(string)
				_, event.innerHasSummary = inner["summary"]
			}
			return event
		case !errors.Is(err, sql.ErrNoRows):
			t.Fatalf("query inbox_events for an event naming %s: %v", apID, err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for an inbox event naming %s: %v", apID, ctx.Err())
		case <-ticker.C:
		}
	}
	rows, err := db.QueryContext(ctx, `
		SELECT type, activity_id, COALESCE(convert_from(payload, 'UTF8'), '') FROM inbox_events
		WHERE id > $1 ORDER BY id`, afterID)
	if err != nil {
		t.Fatalf("no inbox event naming %s within %s; listing what arrived failed: %v", apID, eventTimeout, err)
	}
	defer func() { _ = rows.Close() }()
	var seen []string
	for rows.Next() {
		var eventType, eventID, payload string
		if err := rows.Scan(&eventType, &eventID, &payload); err != nil {
			t.Fatalf("scan inbox_events: %v", err)
		}
		seen = append(seen, eventType+" "+eventID+": "+truncate([]byte(payload), 400))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate inbox_events: %v", err)
	}
	t.Fatalf("no inbox event naming %s reached Tidepool within %s; every event received since the purge: %q",
		apID, eventTimeout, seen)
	return inboxEventShape{}
}

// fetchLemmyObject GETs an AP id from the local Lemmy as ActivityPub JSON.
// Lemmy's AP ids carry its compose-internal origin, so the request goes to
// the same path on the Lemmy URL the suite reaches from the host.
func fetchLemmyObject(t *testing.T, h *harness, apID string) (status int, body []byte) {
	t.Helper()
	parsed, err := url.Parse(apID)
	if err != nil {
		t.Fatalf("parse AP id %q: %v", apID, err)
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, lemmyURL()+parsed.EscapedPath(), nil)
	if err != nil {
		t.Fatalf("build request for %s: %v", apID, err)
	}
	req.Header.Set("Accept", "application/activity+json")
	resp, err := h.http.Do(req)
	if err != nil {
		t.Fatalf("fetch %s from lemmy: %v", apID, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s from lemmy: %v", apID, err)
	}
	return resp.StatusCode, body
}
