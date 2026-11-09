//go:build e2e

package e2e

// Native edits → Lemmy, through the real stack.
//
// A native postv2 and a native comment on it are written on the reference PDS
// into a bridged community, federate to the real Lemmy container, and are then
// each edited on the PDS (com.atproto.repo.putRecord). Lemmy 0.19.20 applies an
// Update only when its object carries an `updated` later than the copy it
// holds, so the assertion is on what LEMMY STORED after the edit: the new post
// body and the new comment content, on the same post and the same comment.

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
)

func TestNativeEdit_LemmyAppliesPostAndCommentEdits(t *testing.T) {
	h := newHarness(t)

	owner := h.registerUser(t, h.uniqueName(t, "edit_own"))
	name := h.uniqueName(t, "edit_comm")
	community := owner.createCommunity(t, name, "native edits "+name)
	sub := h.subscribeCommunity(t, "!"+name+"@lemmy")
	if sub.DID == "" {
		t.Fatalf("subscribed %s but the bridge reported no community DID: %+v", name, sub)
	}

	pds := &pdsClient{http: h.http}
	if err := pds.createSession(nativeHandle, nativePassword); err != nil {
		t.Fatalf("createSession as the native bootstrap account %q at %s: %v", nativeHandle, pdsURL(), err)
	}

	// --- The post: create, see it on Lemmy. ---
	postRKey := syntax.NewTIDNow(0).String()
	title := fmt.Sprintf("native edit %s %s", h.suffix, postRKey)
	postBody := "original post body " + h.suffix
	post := map[string]any{
		"$type":     colPostV2,
		"community": sub.DID,
		"title":     title,
		"content":   postBody,
		"createdAt": time.Now().UTC().Format(time.RFC3339),
	}
	postURI, postCID, err := pds.createRecord(colPostV2, postRKey, post)
	if err != nil {
		t.Fatalf("createRecord post: %v", err)
	}
	t.Logf("wrote %s", postURI)
	if got := h.awaitLemmyPostBody(t, community.ID, title, sub.DID, postURI); got != postBody {
		t.Fatalf("lemmy stored post body %q before any edit, want %q", got, postBody)
	}

	// --- The comment on it: create, see it on Lemmy. ---
	commentRKey := syntax.NewTIDNow(1).String()
	commentContent := "original comment " + h.suffix
	postRef := map[string]any{"uri": postURI, "cid": postCID}
	comment := map[string]any{
		"$type":     colComment,
		"reply":     map[string]any{"root": postRef, "parent": postRef},
		"content":   commentContent,
		"createdAt": time.Now().UTC().Format(time.RFC3339),
	}
	commentURI, _, err := pds.createRecord(colComment, commentRKey, comment)
	if err != nil {
		t.Fatalf("createRecord comment: %v", err)
	}
	t.Logf("wrote %s", commentURI)
	commentID := h.awaitLemmyCommentByContent(t, community.ID, commentContent, commentURI)

	// --- Edit both on the PDS. ---
	editedPostBody := "edited post body " + h.suffix
	post["content"] = editedPostBody
	if _, _, err := pds.putRecord(colPostV2, postRKey, post); err != nil {
		t.Fatalf("putRecord post: %v", err)
	}
	editedCommentContent := "edited comment " + h.suffix
	comment["content"] = editedCommentContent
	if _, _, err := pds.putRecord(colComment, commentRKey, comment); err != nil {
		t.Fatalf("putRecord comment: %v", err)
	}

	h.awaitLemmyValue(t, "post "+postURI+" body", editedPostBody, func() (string, error) {
		body, found, err := h.lemmyPostBody(community.ID, title)
		if err == nil && !found {
			err = fmt.Errorf("post %q is no longer listed", title)
		}
		return body, err
	})
	h.awaitLemmyValue(t, "comment "+commentURI+" content", editedCommentContent, func() (string, error) {
		return h.lemmyCommentContent(commentID)
	})
}

// awaitLemmyCommentByContent polls the community's comments on Lemmy until one
// with exactly content arrives, and returns its Lemmy id.
func (h *harness) awaitLemmyCommentByContent(t *testing.T, communityID int, content, commentURI string) int {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	var lastErr error
	for {
		query := url.Values{
			"community_id": {fmt.Sprint(communityID)},
			"sort":         {"New"},
			"type_":        {"All"},
			"limit":        {"50"},
		}
		var out struct {
			Comments []struct {
				Comment struct {
					ID      int    `json:"id"`
					Content string `json:"content"`
				} `json:"comment"`
			} `json:"comments"`
		}
		lastErr = h.admin.do(http.MethodGet, "/api/v3/comment/list?"+query.Encode(), nil, &out)
		if lastErr == nil {
			for _, c := range out.Comments {
				if c.Comment.Content == content {
					return c.Comment.ID
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("comment %s (%q) never reached Lemmy community %d within %s (last list error: %v)",
				commentURI, content, communityID, eventTimeout, lastErr)
		}
		time.Sleep(time.Second)
	}
}

// lemmyCommentContent reads one comment's stored content from Lemmy.
func (h *harness) lemmyCommentContent(commentID int) (string, error) {
	var out struct {
		CommentView struct {
			Comment struct {
				Content string `json:"content"`
			} `json:"comment"`
		} `json:"comment_view"`
	}
	if err := h.admin.do(http.MethodGet, fmt.Sprintf("/api/v3/comment?id=%d", commentID), nil, &out); err != nil {
		return "", err
	}
	return out.CommentView.Comment.Content, nil
}

// awaitLemmyValue polls read until it returns want, failing with the last
// value and error seen when eventTimeout passes.
func (h *harness) awaitLemmyValue(t *testing.T, what, want string, read func() (string, error)) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for {
		got, err := read()
		if err == nil && got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("lemmy %s never became %q within %s (last value %q, last error: %v)",
				what, want, eventTimeout, got, err)
		}
		time.Sleep(time.Second)
	}
}
