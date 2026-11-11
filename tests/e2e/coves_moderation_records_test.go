//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
)

// The instance admin account pds-bootstrap creates on the reference PDS, whose
// DID the compose file passes to the AppView as MODERATION_ADMINS.
const (
	covesAdminHandle   = "coves-admin.pds.test"
	covesAdminPassword = "coves-admin-e2e-pass"
)

// covesSignInTimeout bounds one OAuth web sign-in against the stack.
const covesSignInTimeout = 60 * time.Second

// PRD moderation gaps, task T4 (D8): the removal and ban records Tidepool
// writes for a Lemmy moderator's actions reach Coves' served views through the
// relay and Jetstream. A bridged Lemmy comment is removed and restored, and
// the native persona native-alice is banned from the community and unbanned;
// native-alice and the instance admin act through sessions from the AppView's
// real OAuth web sign-in.
func TestCovesModeration_RemovalAndBanRecordsReachCovesViews(t *testing.T) {
	h := newHarness(t)
	community, sub := setupSubscribedCommunity(t, h, "cvmod")
	h.awaitAppViewCommunityIndexed(t, sub.DID)

	cursor := cursorNow()
	l := h.newListener(t, cursor, colPostV2, colComment)

	// ── A bridged Lemmy post and comment ──────────────────────────────────
	user := h.registerUser(t, h.uniqueName(t, "cvcarla"))
	title := "Coves moderation view " + h.suffix
	post := user.createPost(t, community.ID, title, "a thread whose comment a moderator removes")
	postEv := l.await("bridged postv2 create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "title")
		return e.Commit.Collection == colPostV2 && e.Commit.Operation == opCreate && got == title
	})
	postURI := postEv.atURI()
	commentBody := "a bridged comment Coves shows removed " + h.suffix
	comment := user.createComment(t, post.ID, 0, commentBody)
	commentEv := l.await("bridged comment create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "content")
		return e.Commit.Collection == colComment && e.Commit.Operation == opCreate && got == commentBody
	})
	commentURI := commentEv.atURI()

	// ── native-alice's Lemmy persona, minted by a native post ─────────────
	pds := &pdsClient{http: h.http}
	if err := pds.createSession(nativeHandle, nativePassword); err != nil {
		t.Fatalf("createSession as %q at %s: %v", nativeHandle, pdsURL(), err)
	}
	nativeRKey := syntax.NewTIDNow(0).String()
	nativeTitle := fmt.Sprintf("native-alice in a Coves-viewed community %s %s", h.suffix, nativeRKey)
	nativePostURI, _, err := pds.createRecord(colPostV2, nativeRKey, map[string]any{
		"$type":     colPostV2,
		"community": sub.DID,
		"title":     nativeTitle,
		"content":   "native-alice's persona is minted by this post",
		"createdAt": time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("createRecord native post: %v", err)
	}
	h.awaitLemmyPostBody(t, community.ID, nativeTitle, sub.DID, nativePostURI)
	alicePersonID := h.awaitLemmyPostCreatorID(t, community.ID, nativeTitle)

	// ── Real OAuth sessions for native-alice and the instance admin ───────
	pdsAddress, err := url.Parse(pdsURL())
	if err != nil {
		t.Fatalf("parse pdsURL %q: %v", pdsURL(), err)
	}
	signIn := covesSignIn{
		AppViewURL:    covesAppViewURL(),
		DialOverrides: map[string]string{"localhost:3001": "127.0.0.1:" + pdsAddress.Port()},
	}
	aliceToken := covesSignInAs(t, signIn, nativeHandle, nativePassword)
	adminToken := covesSignInAs(t, signIn, covesAdminHandle, covesAdminPassword)

	// ── Removal: Coves shows a community-removed placeholder ──────────────
	h.awaitCovesComment(t, postURI, commentURI, "the live bridged comment before the removal", func(view covesCommentView) bool {
		return !view.IsDeleted
	})
	h.admin.removeComment(t, comment.ID, true, "rule 3: no personal attacks")
	removed := h.awaitCovesComment(t, postURI, commentURI, "the community-removed placeholder", func(view covesCommentView) bool {
		return view.IsDeleted
	})
	if removed.DeletionReason == nil || *removed.DeletionReason != "moderator" {
		t.Errorf("removed comment deletionReason = %v, want %q", removed.DeletionReason, "moderator")
	}
	if len(removed.Record) != 0 && string(removed.Record) != "null" {
		t.Errorf("removed comment record = %s, want null or absent", removed.Record)
	}
	if removed.Moderation == nil {
		t.Errorf("removed comment carries no moderation view")
	} else {
		if removed.Moderation.State != "removed" {
			t.Errorf("removed comment moderation.state = %q, want %q", removed.Moderation.State, "removed")
		}
		var communityScoped bool
		for _, source := range removed.Moderation.Sources {
			if source.Scope.Kind == "community" && source.Scope.CommunityDID == sub.DID {
				communityScoped = true
			}
		}
		if !communityScoped {
			t.Errorf("removed comment moderation.sources = %+v, want one with scope kind %q and communityDid %q",
				removed.Moderation.Sources, "community", sub.DID)
		}
	}

	// ── An admin-only moderation read: the admin passes, alice does not ───
	subjectQuery := url.Values{"subject": {commentURI}}
	status, body, err := h.covesXRPC(http.MethodGet, "social.coves.moderation.getSubjectState", subjectQuery, nil, adminToken)
	if err != nil {
		t.Fatalf("getSubjectState as the instance admin: %v", err)
	}
	if status != http.StatusOK {
		t.Errorf("getSubjectState as the instance admin = HTTP %d (%s), want 200", status, truncate(body, 300))
	}
	status, body, err = h.covesXRPC(http.MethodGet, "social.coves.moderation.getSubjectState", subjectQuery, nil, aliceToken)
	if err != nil {
		t.Fatalf("getSubjectState as native-alice: %v", err)
	}
	if status != http.StatusForbidden || covesErrorName(body) != "Forbidden" {
		t.Errorf("getSubjectState as native-alice = HTTP %d (%s), want 403 Forbidden", status, truncate(body, 300))
	}

	// ── Restore: Coves shows the comment live again ───────────────────────
	h.admin.removeComment(t, comment.ID, false, "")
	restored := h.awaitCovesComment(t, postURI, commentURI, "the comment live after the restore", func(view covesCommentView) bool {
		return !view.IsDeleted
	})
	var restoredRecord struct {
		Content string `json:"content"`
	}
	if err := json.Unmarshal(restored.Record, &restoredRecord); err != nil {
		t.Fatalf("restored comment record %s: %v", restored.Record, err)
	}
	if restoredRecord.Content != commentBody {
		t.Errorf("restored comment record.content = %q, want %q", restoredRecord.Content, commentBody)
	}

	// ── Ban: alice sees the ban and is refused a post ─────────────────────
	h.admin.banFromCommunity(t, community.ID, alicePersonID, true, "rule 2: spam")
	h.awaitCovesViewerBanned(t, sub.DID, aliceToken, true)
	status, body, err = h.covesXRPC(http.MethodPost, "social.coves.community.post.create", nil, map[string]any{
		"community": sub.DID,
		"title":     "native-alice while banned " + h.suffix,
		"content":   "Coves must refuse this post",
	}, aliceToken)
	if err != nil {
		t.Fatalf("post.create as banned native-alice: %v", err)
	}
	if status != http.StatusForbidden || covesErrorName(body) != "Banned" {
		t.Errorf("post.create as banned native-alice = HTTP %d (%s), want 403 Banned", status, truncate(body, 300))
	}

	// ── Unban: alice can post again ───────────────────────────────────────
	h.admin.banFromCommunity(t, community.ID, alicePersonID, false, "")
	h.awaitCovesViewerBanned(t, sub.DID, aliceToken, false)
	status, body, err = h.covesXRPC(http.MethodPost, "social.coves.community.post.create", nil, map[string]any{
		"community": sub.DID,
		"title":     "native-alice after the unban " + h.suffix + " " + syntax.NewTIDNow(0).String(),
		"content":   "Coves takes this post",
	}, aliceToken)
	if err != nil {
		t.Fatalf("post.create as unbanned native-alice: %v", err)
	}
	if status != http.StatusOK {
		t.Errorf("post.create as unbanned native-alice = HTTP %d (%s), want 200", status, truncate(body, 300))
	}
}

// covesSignInAs signs handle in through the AppView's OAuth web flow and
// returns the sealed session token. Fatal on failure.
func covesSignInAs(t *testing.T, signIn covesSignIn, handle, password string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), covesSignInTimeout)
	defer cancel()
	token, err := signIn.SignIn(ctx, handle, password)
	if err != nil {
		t.Fatalf("OAuth sign-in as %s through %s: %v", handle, signIn.AppViewURL, err)
	}
	if token == "" {
		t.Fatalf("OAuth sign-in as %s through %s returned an empty session token", handle, signIn.AppViewURL)
	}
	return token
}

// covesXRPC calls an AppView XRPC method, with the sealed session as a bearer
// token when one is given, and returns the status and body.
func (h *harness) covesXRPC(method, nsid string, query url.Values, body any, token string) (int, []byte, error) {
	endpoint := covesAppViewURL() + "/xrpc/" + nsid
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	var reqBody io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		reqBody = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, endpoint, reqBody)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := h.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, raw, nil
}

// covesErrorName returns the XRPC error name of an error body, or "".
func covesErrorName(body []byte) string {
	var envelope struct {
		Error string `json:"error"`
	}
	_ = json.Unmarshal(body, &envelope)
	return envelope.Error
}

// covesCommentView is the slice of social.coves.community.comment.defs#commentView
// this test reads.
type covesCommentView struct {
	URI            string          `json:"uri"`
	IsDeleted      bool            `json:"isDeleted"`
	DeletionReason *string         `json:"deletionReason"`
	Record         json.RawMessage `json:"record"`
	Moderation     *struct {
		State   string `json:"state"`
		Sources []struct {
			AuthorityDID string `json:"authorityDid"`
			Scope        struct {
				Kind         string `json:"kind"`
				CommunityDID string `json:"communityDid"`
			} `json:"scope"`
		} `json:"sources"`
	} `json:"moderation"`
}

type covesThreadViewComment struct {
	Comment covesCommentView         `json:"comment"`
	Replies []covesThreadViewComment `json:"replies"`
}

func findCovesComment(threads []covesThreadViewComment, uri string) (covesCommentView, bool) {
	for _, thread := range threads {
		if thread.Comment.URI == uri {
			return thread.Comment, true
		}
		if view, ok := findCovesComment(thread.Replies, uri); ok {
			return view, true
		}
	}
	return covesCommentView{}, false
}

// awaitCovesComment polls the AppView's getComments for postURI until the
// comment commentURI is listed and satisfies ready, and returns its view.
func (h *harness) awaitCovesComment(t *testing.T, postURI, commentURI, desc string, ready func(covesCommentView) bool) covesCommentView {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	query := url.Values{"post": {postURI}, "sort": {"new"}, "limit": {"100"}}
	var last string
	for {
		status, body, err := h.covesXRPC(http.MethodGet, "social.coves.community.comment.getComments", query, nil, "")
		switch {
		case err != nil:
			last = "error: " + err.Error()
		case status != http.StatusOK:
			last = fmt.Sprintf("HTTP %d: %s", status, truncate(body, 300))
		default:
			var out struct {
				Comments []covesThreadViewComment `json:"comments"`
			}
			if err := json.Unmarshal(body, &out); err != nil {
				last = fmt.Sprintf("decode: %v (%s)", err, truncate(body, 300))
			} else if view, ok := findCovesComment(out.Comments, commentURI); !ok {
				last = "comment not listed: " + truncate(body, 300)
			} else if ready(view) {
				return view
			} else {
				last = fmt.Sprintf("comment listed but not yet %s: %+v", desc, view)
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("Coves getComments on %s never showed %s for %s within %s (last: %s)",
				postURI, desc, commentURI, eventTimeout, last)
		}
		time.Sleep(time.Second)
	}
}

// awaitCovesViewerBanned polls social.coves.community.get as the token's
// account until viewer.banned is want (absent reads as false).
func (h *harness) awaitCovesViewerBanned(t *testing.T, communityDID, token string, want bool) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	query := url.Values{"community": {communityDID}}
	var last string
	for {
		status, body, err := h.covesXRPC(http.MethodGet, "social.coves.community.get", query, nil, token)
		switch {
		case err != nil:
			last = "error: " + err.Error()
		case status != http.StatusOK:
			last = fmt.Sprintf("HTTP %d: %s", status, truncate(body, 300))
		default:
			var out struct {
				Viewer struct {
					Banned bool `json:"banned"`
				} `json:"viewer"`
			}
			if err := json.Unmarshal(body, &out); err != nil {
				last = fmt.Sprintf("decode: %v (%s)", err, truncate(body, 300))
			} else if out.Viewer.Banned == want {
				return
			} else {
				last = fmt.Sprintf("viewer.banned = %v: %s", out.Viewer.Banned, truncate(body, 300))
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("Coves community.get of %s never showed viewer.banned = %v within %s (last: %s)",
				communityDID, want, eventTimeout, last)
		}
		time.Sleep(time.Second)
	}
}
