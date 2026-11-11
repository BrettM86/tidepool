//go:build e2e

package e2e

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
)

// PRD moderation gaps, chunk 03 (B14): an instance-admin removal of native
// records reaches Lemmy as the author's Delete and the restore as
// Undo{Delete}, on the community's own instance and, through the community's
// Announce, on a second instance that follows it. A refetch of the removed
// post's object id answers 410. A Lemmy moderator's removal of one comment
// stays in force after the Coves restore.
//
// lemmy2 is observed through its local copies, by the ids saved when the
// content first arrived there: nothing is re-resolved, so a refetch from
// Tidepool cannot stand in for the Announce.
func TestNativeTakedown_RemovalAndRestoreReachBothLemmyInstances(t *testing.T) {
	h := newHarness(t)
	lemmy2 := h.lemmy2Admin(t)
	community, sub := setupSubscribedCommunity(t, h, "ntkd")
	h.awaitAppViewCommunityIndexed(t, sub.DID)

	// ── A lemmy2 user follows the community before any content exists ─────
	follower := h.registerUserOn(t, lemmy2URL(), h.uniqueName(t, "ntkdfol"))
	remoteCommunityID := follower.resolveCommunity(t, "!"+community.Name+"@"+lemmyHostname(t))
	follower.followCommunity(t, remoteCommunityID)

	// ── native-alice's post and two comments, delivered to lemmy ──────────
	pds := &pdsClient{http: h.http}
	if err := pds.createSession(nativeHandle, nativePassword); err != nil {
		t.Fatalf("createSession as %q at %s: %v", nativeHandle, pdsURL(), err)
	}
	postRKey := syntax.NewTIDNow(0).String()
	title := fmt.Sprintf("native takedown %s %s", h.suffix, postRKey)
	postURI, postCID, err := pds.createRecord(colPostV2, postRKey, map[string]any{
		"$type":     colPostV2,
		"community": sub.DID,
		"title":     title,
		"content":   "a native post the instance admin removes and restores",
		"createdAt": time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("createRecord native post: %v", err)
	}
	h.awaitLemmyPostBody(t, community.ID, title, sub.DID, postURI)
	postID := h.awaitLemmyPostID(t, community.ID, title)

	postRef := map[string]any{"uri": postURI, "cid": postCID}
	createComment := func(rkey, content string) string {
		uri, _, err := pds.createRecord(colComment, rkey, map[string]any{
			"$type":     colComment,
			"reply":     map[string]any{"root": postRef, "parent": postRef},
			"content":   content,
			"createdAt": time.Now().UTC().Format(time.RFC3339),
		})
		if err != nil {
			t.Fatalf("createRecord native comment %q: %v", content, err)
		}
		return uri
	}
	firstBody := "the first native comment " + h.suffix
	secondBody := "the second native comment " + h.suffix
	firstURI := createComment(syntax.NewTIDNow(1).String(), firstBody)
	secondURI := createComment(syntax.NewTIDNow(2).String(), secondBody)
	firstID := h.awaitLemmyCommentID(t, postID, firstBody)
	secondID := h.awaitLemmyCommentID(t, postID, secondBody)

	// ── The same content on lemmy2, through the community's Announce ──────
	remotePostID := lemmy2.awaitPostID(t, remoteCommunityID, title)
	remoteFirstID := lemmy2.awaitCommentID(t, remotePostID, firstBody)
	lemmy2.awaitCommentID(t, remotePostID, secondBody)

	postAPID := h.admin.postAPID(t, postID)

	// ── A Lemmy moderator removes the second comment ──────────────────────
	h.admin.removeComment(t, secondID, true, "rule 3: no personal attacks")
	h.admin.awaitContentFlag(t, "comment", secondID, "removed", true, "after the moderator removal")

	// ── The instance admin removes the post and both comments ─────────────
	pdsAddress, err := url.Parse(pdsURL())
	if err != nil {
		t.Fatalf("parse pdsURL %q: %v", pdsURL(), err)
	}
	signIn := covesSignIn{
		AppViewURL:    covesAppViewURL(),
		DialOverrides: map[string]string{"localhost:3001": "127.0.0.1:" + pdsAddress.Port()},
	}
	adminToken := covesSignInAs(t, signIn, covesAdminHandle, covesAdminPassword)

	type removal struct {
		name    string
		uri     string
		indexed covesSubjectState
		removed covesSubjectState
	}
	removals := []*removal{
		{name: "post", uri: postURI},
		{name: "comment1", uri: firstURI},
		{name: "comment2", uri: secondURI},
	}
	for _, r := range removals {
		r.indexed = h.awaitCovesSubjectState(t, r.uri, adminToken, "the native "+r.name+" indexed", func(s covesSubjectState) bool {
			return s.RecordState == "present" && s.CurrentSubject != nil
		})
		r.removed = h.covesModerationMutation(t, "social.coves.moderation.removeContent", adminToken, map[string]any{
			"subject":         r.indexed.CurrentSubject,
			"expectedVersion": r.indexed.Version,
			"idempotencyKey":  "b14-remove-" + r.name + "-" + h.suffix,
			"reason":          covesReasonModeratorDiscretion,
		})
		if r.removed.LocalRemoval == nil || r.removed.LocalRemoval.ActionID == "" {
			t.Fatalf("removeContent of the native %s left no localRemoval action: %+v", r.name, r.removed)
		}
	}

	const whileRemoved = "while the instance removal stands"
	h.admin.awaitContentFlag(t, "post", postID, "deleted", true, whileRemoved)
	h.admin.awaitContentFlag(t, "comment", firstID, "deleted", true, whileRemoved)
	// The moderator-removed comment is delivered too, so it also gets the
	// author's Delete; seeing it here is what later shows its restore landed.
	h.admin.awaitContentFlag(t, "comment", secondID, "deleted", true, whileRemoved)
	lemmy2.awaitContentFlag(t, "post", remotePostID, "deleted", true, whileRemoved)
	lemmy2.awaitContentFlag(t, "comment", remoteFirstID, "deleted", true, whileRemoved)

	if status, body := h.fetchTidepoolObject(t, postAPID); status != http.StatusGone {
		t.Errorf("GET %s %s = HTTP %d (%s), want 410", postAPID, whileRemoved, status, truncate(body, 300))
	}

	// ── The instance admin restores all three ─────────────────────────────
	for _, r := range removals {
		h.covesModerationMutation(t, "social.coves.moderation.restoreContent", adminToken, map[string]any{
			"actionId":        r.removed.LocalRemoval.ActionID,
			"expectedVersion": r.removed.Version,
			"idempotencyKey":  "b14-restore-" + r.name + "-" + h.suffix,
			"reason":          covesReasonModeratorDiscretion,
			"reviewedSubject": r.indexed.CurrentSubject,
		})
	}

	const afterRestore = "after the instance restore"
	h.admin.awaitContentFlag(t, "post", postID, "deleted", false, afterRestore)
	h.admin.awaitContentFlag(t, "comment", firstID, "deleted", false, afterRestore)
	h.admin.awaitContentFlag(t, "comment", secondID, "deleted", false, afterRestore)
	lemmy2.awaitContentFlag(t, "post", remotePostID, "deleted", false, afterRestore)
	lemmy2.awaitContentFlag(t, "comment", remoteFirstID, "deleted", false, afterRestore)

	// The Coves restore undid only the author's Delete: the moderator's
	// removal on lemmy stands.
	flags, err := h.admin.contentFlags("comment", secondID)
	if err != nil {
		t.Fatalf("read comment %d on Lemmy %s %s: %v", secondID, h.admin.base(), afterRestore, err)
	}
	if !flags.Removed {
		t.Errorf("comment %d (%s) on Lemmy %s has removed = false %s, want true: the Coves restore lifted the Lemmy moderator's removal",
			secondID, secondURI, h.admin.base(), afterRestore)
	}
}

// lemmyContentFlags are the deletion flags Lemmy keeps on a post or comment:
// deleted by its creator, removed by a moderator.
type lemmyContentFlags struct {
	Deleted bool `json:"deleted"`
	Removed bool `json:"removed"`
}

// contentFlags reads the flags of the post or comment (kind "post" or
// "comment") with the given id on the client's Lemmy.
func (c *lemmyClient) contentFlags(kind string, id int) (lemmyContentFlags, error) {
	var out struct {
		PostView struct {
			Post lemmyContentFlags `json:"post"`
		} `json:"post_view"`
		CommentView struct {
			Comment lemmyContentFlags `json:"comment"`
		} `json:"comment_view"`
	}
	switch kind {
	case "post", "comment":
	default:
		return lemmyContentFlags{}, fmt.Errorf("unknown Lemmy content kind %q", kind)
	}
	if err := c.do(http.MethodGet, fmt.Sprintf("/api/v3/%s?id=%d", kind, id), nil, &out); err != nil {
		return lemmyContentFlags{}, err
	}
	if kind == "post" {
		return out.PostView.Post, nil
	}
	return out.CommentView.Comment, nil
}

// awaitContentFlag polls the post or comment on the client's Lemmy until its
// flag ("deleted" or "removed") is want.
func (c *lemmyClient) awaitContentFlag(t *testing.T, kind string, id int, flag string, want bool, when string) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	var last string
	for {
		flags, err := c.contentFlags(kind, id)
		if err != nil {
			last = "error: " + err.Error()
		} else {
			var got bool
			switch flag {
			case "deleted":
				got = flags.Deleted
			case "removed":
				got = flags.Removed
			default:
				t.Fatalf("unknown Lemmy content flag %q", flag)
			}
			if got == want {
				return
			}
			last = fmt.Sprintf("%+v", flags)
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s %d on Lemmy %s never had %s = %v %s within %s (last: %s)",
				kind, id, c.base(), flag, want, when, eventTimeout, last)
		}
		time.Sleep(time.Second)
	}
}

// postAPID reads the ActivityPub id Lemmy stored for a post: for a native
// post it is the Tidepool object id.
func (c *lemmyClient) postAPID(t *testing.T, postID int) string {
	t.Helper()
	var out struct {
		PostView struct {
			Post struct {
				APID string `json:"ap_id"`
			} `json:"post"`
		} `json:"post_view"`
	}
	if err := c.do(http.MethodGet, fmt.Sprintf("/api/v3/post?id=%d", postID), nil, &out); err != nil {
		t.Fatalf("read post %d on Lemmy %s: %v", postID, c.base(), err)
	}
	if out.PostView.Post.APID == "" {
		t.Fatalf("post %d on Lemmy %s has no ap_id", postID, c.base())
	}
	return out.PostView.Post.APID
}

// resolveCommunity resolves a community handle (!name@host) on the client's
// Lemmy, polling until the instance has fetched it, and returns its local id.
func (c *lemmyClient) resolveCommunity(t *testing.T, handle string) int {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	var last string
	for {
		var out struct {
			Community *struct {
				Community struct {
					ID int `json:"id"`
				} `json:"community"`
			} `json:"community"`
		}
		err := c.do(http.MethodGet, "/api/v3/resolve_object?q="+url.QueryEscape(handle), nil, &out)
		switch {
		case err != nil:
			last = "error: " + err.Error()
		case out.Community == nil || out.Community.Community.ID == 0:
			last = "resolved, but not to a community"
		default:
			return out.Community.Community.ID
		}
		if time.Now().After(deadline) {
			t.Fatalf("Lemmy %s never resolved %s within %s (last: %s)", c.base(), handle, eventTimeout, last)
		}
		time.Sleep(time.Second)
	}
}

// followCommunity follows a community as the client's user and polls until
// the community's instance accepted the follow.
func (c *lemmyClient) followCommunity(t *testing.T, communityID int) {
	t.Helper()
	if err := c.do(http.MethodPost, "/api/v3/community/follow",
		map[string]any{"community_id": communityID, "follow": true}, nil); err != nil {
		t.Fatalf("follow community %d on Lemmy %s: %v", communityID, c.base(), err)
	}
	deadline := time.Now().Add(eventTimeout)
	var last string
	for {
		var out struct {
			CommunityView struct {
				Subscribed string `json:"subscribed"`
			} `json:"community_view"`
		}
		err := c.do(http.MethodGet, fmt.Sprintf("/api/v3/community?id=%d", communityID), nil, &out)
		switch {
		case err != nil:
			last = "error: " + err.Error()
		case out.CommunityView.Subscribed == "Subscribed":
			return
		default:
			last = "subscribed = " + out.CommunityView.Subscribed
		}
		if time.Now().After(deadline) {
			t.Fatalf("follow of community %d on Lemmy %s was never accepted within %s (last: %s)",
				communityID, c.base(), eventTimeout, last)
		}
		time.Sleep(time.Second)
	}
}

// fetchTidepoolObject GETs a Tidepool AP object id as ActivityPub JSON. The id
// carries the bridge's compose-internal origin, so the request goes to the
// same path on the host-published bridge URL with the id's host as Host.
func (h *harness) fetchTidepoolObject(t *testing.T, apID string) (status int, body []byte) {
	t.Helper()
	parsed, err := url.Parse(apID)
	if err != nil {
		t.Fatalf("parse AP id %q: %v", apID, err)
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, tidepoolURL()+parsed.EscapedPath(), nil)
	if err != nil {
		t.Fatalf("build request for %s: %v", apID, err)
	}
	req.Host = parsed.Host
	req.Header.Set("Accept", "application/activity+json")
	resp, err := h.http.Do(req)
	if err != nil {
		t.Fatalf("GET %s (Host %s): %v", parsed.EscapedPath(), req.Host, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read GET %s (Host %s): %v", parsed.EscapedPath(), req.Host, err)
	}
	return resp.StatusCode, body
}
