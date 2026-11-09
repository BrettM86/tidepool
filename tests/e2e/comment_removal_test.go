//go:build e2e

package e2e

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
)

// PRD moderation gaps, task 02: a Lemmy moderator's comment removal is a
// social.coves.community.removal record in the community's repo, at the
// digest rkey of the comment's at-uri, and the restore deletes it. Two
// comments are moderated: a NATIVE Coves comment, written into the reference
// PDS and federated to Lemmy by the bridge, and a BRIDGED Lemmy comment. The
// bridged comment's own record is the author's and stays throughout.
func TestModeration_CommentRemoveAndRestoreWriteRemovalRecords(t *testing.T) {
	h := newHarness(t)
	community, sub := setupSubscribedCommunity(t, h, "cmod")

	cursor := cursorNow()
	l := h.newListener(t, cursor, colPostV2, colComment, colRemoval)

	// ── The bridged Lemmy comment ─────────────────────────────────────────
	user := h.registerUser(t, h.uniqueName(t, "carla"))
	title := "Comment moderation " + h.suffix
	post := user.createPost(t, community.ID, title, "a thread for moderated comments")
	l.await("postv2 create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "title")
		return e.Commit.Collection == colPostV2 && e.Commit.Operation == opCreate && got == title
	})
	bridgedBody := "a bridged comment to remove " + h.suffix
	bridged := user.createComment(t, post.ID, 0, bridgedBody)
	bridgedEv := l.await("bridged comment create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "content")
		return e.Commit.Collection == colComment && e.Commit.Operation == opCreate && got == bridgedBody
	})
	bridgedURI := bridgedEv.atURI()
	h.awaitRecordPresent(t, bridgedEv.Did, colComment, bridgedEv.Commit.RKey, "bridged comment before removal")

	// ── The native Coves comment ──────────────────────────────────────────
	pds := &pdsClient{http: h.http}
	if err := pds.createSession(nativeHandle, nativePassword); err != nil {
		t.Fatalf("createSession as the native bootstrap account %q at %s: %v", nativeHandle, pdsURL(), err)
	}
	nativePostRKey := syntax.NewTIDNow(0).String()
	nativeTitle := fmt.Sprintf("native thread %s %s", h.suffix, nativePostRKey)
	nativePostURI, nativePostCID, err := pds.createRecord(colPostV2, nativePostRKey, map[string]any{
		"$type":     colPostV2,
		"community": sub.DID,
		"title":     nativeTitle,
		"content":   "a native thread",
		"createdAt": time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("createRecord native post: %v", err)
	}
	h.awaitLemmyPostBody(t, community.ID, nativeTitle, sub.DID, nativePostURI)
	nativeLemmyPostID := h.awaitLemmyPostID(t, community.ID, nativeTitle)

	nativeBody := "a native comment to remove " + h.suffix
	nativeCommentURI, _, err := pds.createRecord(colComment, syntax.NewTIDNow(1).String(), map[string]any{
		"$type": colComment,
		"reply": map[string]any{
			"root":   map[string]any{"uri": nativePostURI, "cid": nativePostCID},
			"parent": map[string]any{"uri": nativePostURI, "cid": nativePostCID},
		},
		"content":   nativeBody,
		"createdAt": time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("createRecord native comment: %v", err)
	}
	nativeLemmyCommentID := h.awaitLemmyCommentID(t, nativeLemmyPostID, nativeBody)

	cases := []struct {
		name       string
		lemmyID    int
		commentURI string
	}{
		{name: "native comment", lemmyID: nativeLemmyCommentID, commentURI: nativeCommentURI},
		{name: "bridged comment", lemmyID: bridged.ID, commentURI: bridgedURI},
	}

	// ── A moderator removes each, with a reason ───────────────────────────
	const reason = "rule 3: no personal attacks"
	for _, c := range cases {
		h.admin.removeComment(t, c.lemmyID, true, reason)
	}
	for _, c := range cases {
		digest := subjectRKey(c.commentURI)
		removalEv := l.await(c.name+" removal record written", func(e *jsEvent) bool {
			uri, _ := fieldOf(e.Commit.Record, "subject", "uri")
			return e.Commit.Collection == colRemoval && e.Did == sub.DID && uri == c.commentURI
		})
		if removalEv.Commit.RKey != digest {
			t.Errorf("%s removal rkey = %q, want the subject digest %q", c.name, removalEv.Commit.RKey, digest)
		}
		if got := recordField(t, removalEv.Commit.Record, "code"); got != "moderator-discretion" {
			t.Errorf("%s removal code = %q, want %q", c.name, got, "moderator-discretion")
		}
		if got := recordField(t, removalEv.Commit.Record, "reason"); got != reason {
			t.Errorf("%s removal reason = %q, want %q", c.name, got, reason)
		}
		h.awaitRecordPresent(t, sub.DID, colRemoval, digest, c.name+" removal after removal")
	}
	if _, found := h.bridgeGetRecord(t, bridgedEv.Did, colComment, bridgedEv.Commit.RKey); !found {
		t.Error("the bridged comment's record was deleted by a moderator removal; it belongs to its author")
	}

	// ── The moderator restores each ───────────────────────────────────────
	for _, c := range cases {
		h.admin.removeComment(t, c.lemmyID, false, "")
	}
	for _, c := range cases {
		digest := subjectRKey(c.commentURI)
		l.await(c.name+" removal deleted by the restore", func(e *jsEvent) bool {
			return e.Commit.Collection == colRemoval && e.Commit.Operation == opDelete &&
				e.Did == sub.DID && e.Commit.RKey == digest
		})
		h.awaitRecordGone(t, sub.DID, colRemoval, digest, c.name+" removal after restore")
	}
	if _, found := h.bridgeGetRecord(t, bridgedEv.Did, colComment, bridgedEv.Commit.RKey); !found {
		t.Error("the bridged comment's record is missing after the restore")
	}
}

// awaitLemmyPostID polls the community's post list until the post titled title
// is there and returns its Lemmy id.
func (h *harness) awaitLemmyPostID(t *testing.T, communityID int, title string) int {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for {
		query := url.Values{"community_id": {fmt.Sprint(communityID)}, "sort": {"New"}, "limit": {"50"}}
		var out struct {
			Posts []struct {
				Post struct {
					ID   int    `json:"id"`
					Name string `json:"name"`
				} `json:"post"`
			} `json:"posts"`
		}
		err := h.admin.do(http.MethodGet, "/api/v3/post/list?"+query.Encode(), nil, &out)
		if err == nil {
			for _, p := range out.Posts {
				if p.Post.Name == title {
					return p.Post.ID
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("post %q never listed in Lemmy community %d within %s (last error: %v)",
				title, communityID, eventTimeout, err)
		}
		time.Sleep(time.Second)
	}
}

// awaitLemmyCommentID polls a Lemmy post's comments until one whose content
// contains body arrives and returns its Lemmy id.
func (h *harness) awaitLemmyCommentID(t *testing.T, postID int, body string) int {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for {
		query := url.Values{"post_id": {fmt.Sprint(postID)}, "sort": {"New"}, "limit": {"50"}}
		var out struct {
			Comments []struct {
				Comment struct {
					ID      int    `json:"id"`
					Content string `json:"content"`
				} `json:"comment"`
			} `json:"comments"`
		}
		err := h.admin.do(http.MethodGet, "/api/v3/comment/list?"+query.Encode(), nil, &out)
		if err == nil {
			for _, c := range out.Comments {
				if strings.Contains(c.Comment.Content, body) {
					return c.Comment.ID
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("comment %q never reached Lemmy post %d within %s (last error: %v)",
				body, postID, eventTimeout, err)
		}
		time.Sleep(time.Second)
	}
}
