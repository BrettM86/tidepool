//go:build e2e

package e2e

// Native postv2 and comment images that no #image facet places, through the
// real stack. A post's first unplaced image becomes the Page's Image
// attachment, so Lemmy stores it as the post URL with its alt text and fetches
// its metadata; the other unplaced images are appended to the post body. A
// comment's unplaced images ride as Note attachments, which Lemmy appends to
// the stored comment content itself.
//
// Every expected URL is written from the proxy URL contract's literal path
// segments against covesMediaOrigin(), the origin the bridge and Lemmy see on
// the compose network.

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
)

// covesMediaImageURL is a full-size proxy image URL as Lemmy stores it.
func covesMediaImageURL(did, cid string) string {
	return covesMediaOrigin() + "/img/content_full/plain/" + did + "/" + cid
}

// imageMarkdown is the markdown image node for alt and a proxy image URL.
func imageMarkdown(alt, did, cid string) string {
	return "![" + alt + "](" + covesMediaImageURL(did, cid) + ")"
}

// galleryEntry is one social.coves.embed.images entry.
func galleryEntry(blob map[string]any, alt string) map[string]any {
	return map[string]any{"image": blob, "alt": alt}
}

// imagesEmbed is a social.coves.embed.images embed over entries.
func imagesEmbed(entries ...map[string]any) map[string]any {
	images := make([]any, len(entries))
	for i, e := range entries {
		images[i] = e
	}
	return map[string]any{"$type": "social.coves.embed.images", "images": images}
}

// lemmyPostFields is the subset of a Lemmy post this suite asserts on.
type lemmyPostFields struct {
	ID             int    `json:"id"`
	Name           string `json:"name"`
	Body           string `json:"body"`
	URL            string `json:"url"`
	AltText        string `json:"alt_text"`
	URLContentType string `json:"url_content_type"`
	ThumbnailURL   string `json:"thumbnail_url"`
}

// lemmyPostByTitle reads the community's newest posts from Lemmy once and
// returns the post titled title. found is false with a nil err only when Lemmy
// answered and the post is not there.
func (h *harness) lemmyPostByTitle(communityID int, title string) (post lemmyPostFields, found bool, err error) {
	query := url.Values{
		"community_id": {fmt.Sprint(communityID)},
		"sort":         {"New"},
		"limit":        {"50"},
	}
	var out struct {
		Posts []struct {
			Post lemmyPostFields `json:"post"`
		} `json:"posts"`
	}
	if err := h.admin.do(http.MethodGet, "/api/v3/post/list?"+query.Encode(), nil, &out); err != nil {
		return lemmyPostFields{}, false, err
	}
	for _, p := range out.Posts {
		if p.Post.Name == title {
			return p.Post, true, nil
		}
	}
	return lemmyPostFields{}, false, nil
}

// awaitLemmyPost polls until the post titled title is on Lemmy and done
// accepts it, and returns it. On timeout it reports the last copy seen and the
// bridge's admission decision.
func (h *harness) awaitLemmyPost(t *testing.T, communityID int, title, communityDID, postURI, what string, done func(lemmyPostFields) bool) lemmyPostFields {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for {
		post, found, err := h.lemmyPostByTitle(communityID, title)
		if err == nil && found && done(post) {
			return post
		}
		if time.Now().After(deadline) {
			t.Fatalf("post %q (%s) on Lemmy community %d never showed %s within %s (found=%v, last copy %+v, last error: %v); bridge admission: %s",
				title, postURI, communityID, what, eventTimeout, found, post, err, h.admissionFor(communityDID, postURI))
		}
		time.Sleep(time.Second)
	}
}

// awaitLemmyCommentContaining polls the community's comments on Lemmy until
// one whose content contains marker arrives, and returns its content.
func (h *harness) awaitLemmyCommentContaining(t *testing.T, communityID int, marker, commentURI string) string {
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
					Content string `json:"content"`
				} `json:"comment"`
			} `json:"comments"`
		}
		lastErr = h.admin.do(http.MethodGet, "/api/v3/comment/list?"+query.Encode(), nil, &out)
		if lastErr == nil {
			for _, c := range out.Comments {
				if strings.Contains(c.Comment.Content, marker) {
					return c.Comment.Content
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("no comment containing %q (%s) reached Lemmy community %d within %s (last list error: %v)",
				marker, commentURI, communityID, eventTimeout, lastErr)
		}
		time.Sleep(time.Second)
	}
}

// TestNativeImage_UnplacedImagesFederate: native posts and comments carrying
// images that no #image facet places reach Lemmy as an image post URL with
// alt text and metadata, gallery images in the post body, an image beside a
// link, and comment images Lemmy appends to the comment.
func TestNativeImage_UnplacedImagesFederate(t *testing.T) {
	h := newHarness(t)

	owner := h.registerUser(t, h.uniqueName(t, "imgu_own"))
	name := h.uniqueName(t, "imgu_comm")
	community := owner.createCommunity(t, name, "unplaced images "+name)
	sub := h.subscribeCommunity(t, "!"+name+"@lemmy")
	if sub.DID == "" {
		t.Fatalf("subscribed %s but the bridge reported no community DID: %+v", name, sub)
	}

	pds := &pdsClient{http: h.http}
	if err := pds.createSession(nativeHandle, nativePassword); err != nil {
		t.Fatalf("createSession as the native bootstrap account %q at %s: %v", nativeHandle, pdsURL(), err)
	}

	// writePost creates a postv2 in the bridged community and returns its
	// title, uri, cid and rkey.
	writePost := func(t *testing.T, label, content string, embed map[string]any) (title, uri, cid, rkey string, record map[string]any) {
		t.Helper()
		rkey = syntax.NewTIDNow(0).String()
		title = fmt.Sprintf("unplaced %s %s %s", label, h.suffix, rkey)
		record = map[string]any{
			"$type":     colPostV2,
			"community": sub.DID,
			"title":     title,
			"content":   content,
			"createdAt": time.Now().UTC().Format(time.RFC3339),
		}
		if embed != nil {
			record["embed"] = embed
		}
		uri, cid, err := pds.createRecord(colPostV2, rkey, record)
		if err != nil {
			t.Fatalf("createRecord %s post: %v", label, err)
		}
		t.Logf("wrote %s", uri)
		return title, uri, cid, rkey, record
	}

	// writeComment creates a top-level comment on the post and returns its uri.
	writeComment := func(t *testing.T, label, postURI, postCID, content string, embed map[string]any) string {
		t.Helper()
		postRef := map[string]any{"uri": postURI, "cid": postCID}
		record := map[string]any{
			"$type":     colComment,
			"reply":     map[string]any{"root": postRef, "parent": postRef},
			"content":   content,
			"embed":     embed,
			"createdAt": time.Now().UTC().Format(time.RFC3339),
		}
		uri, _, err := pds.createRecord(colComment, syntax.NewTIDNow(1).String(), record)
		if err != nil {
			t.Fatalf("createRecord %s comment: %v", label, err)
		}
		t.Logf("wrote %s", uri)
		return uri
	}

	t.Run("image post sets url, alt text and metadata", func(t *testing.T) {
		h.awaitAppViewCommunityIndexed(t, sub.DID)
		blob, cid := h.uploadUniqueImage(t, pds, "unplaced single")
		const alt = "single unplaced square"

		title, uri, _, _, _ := writePost(t, "single", "single image post", imagesEmbed(galleryEntry(blob, alt)))

		wantURL := covesMediaImageURL(pds.did, cid)
		post := h.awaitLemmyPost(t, community.ID, title, sub.DID, uri, "url "+wantURL, func(p lemmyPostFields) bool {
			return p.URL == wantURL
		})
		if post.AltText != alt {
			t.Errorf("lemmy alt_text = %q, want %q", post.AltText, alt)
		}
		if strings.Contains(post.Body, wantURL) {
			t.Errorf("lemmy body %q repeats the image that is already the post url %s", post.Body, wantURL)
		}
		post = h.awaitLemmyPost(t, community.ID, title, sub.DID, uri, "url_content_type image/* and a thumbnail_url", func(p lemmyPostFields) bool {
			return strings.HasPrefix(p.URLContentType, "image/") && p.ThumbnailURL != ""
		})
		t.Logf("lemmy metadata: url_content_type=%q thumbnail_url=%q", post.URLContentType, post.ThumbnailURL)
	})

	t.Run("gallery puts the other images in the body", func(t *testing.T) {
		h.awaitAppViewCommunityIndexed(t, sub.DID)
		blob0, cid0 := h.uploadUniqueImage(t, pds, "gallery 0")
		blob1, cid1 := h.uploadUniqueImage(t, pds, "gallery 1")
		blob2, cid2 := h.uploadUniqueImage(t, pds, "gallery 2")

		title, uri, _, _, _ := writePost(t, "gallery", "gallery post", imagesEmbed(
			galleryEntry(blob0, "gallery first"),
			galleryEntry(blob1, "gallery second"),
			galleryEntry(blob2, "gallery third"),
		))

		wantURL := covesMediaImageURL(pds.did, cid0)
		post := h.awaitLemmyPost(t, community.ID, title, sub.DID, uri, "url "+wantURL, func(p lemmyPostFields) bool {
			return p.URL == wantURL
		})
		t.Logf("lemmy stored body: %q", post.Body)
		wantBody := "gallery post\n\n" + imageMarkdown("gallery second", pds.did, cid1) + "\n\n" + imageMarkdown("gallery third", pds.did, cid2)
		if post.Body != wantBody {
			t.Errorf("lemmy body = %q, want %q", post.Body, wantBody)
		}
		if post.AltText != "gallery first" {
			t.Errorf("lemmy alt_text = %q, want %q", post.AltText, "gallery first")
		}
	})

	t.Run("images with a link use the link as url", func(t *testing.T) {
		h.awaitAppViewCommunityIndexed(t, sub.DID)
		blob0, cid0 := h.uploadUniqueImage(t, pds, "link 0")
		blob1, cid1 := h.uploadUniqueImage(t, pds, "link 1")
		// A stack-local page that exists: the AppView's liveness endpoint.
		link := covesMediaOrigin() + "/xrpc/_health"

		title, uri, _, _, _ := writePost(t, "link", "images beside a link", map[string]any{
			"$type": "social.coves.embed.imagesWithAttachment",
			// Coves indexes the nested gallery's blobs only when it carries its $type.
			"images": map[string]any{"$type": "social.coves.embed.images", "images": []any{
				galleryEntry(blob0, "linked first"),
				galleryEntry(blob1, "linked second"),
			}},
			"attachment": map[string]any{
				"$type":    "social.coves.embed.external",
				"external": map[string]any{"uri": link},
			},
		})

		post := h.awaitLemmyPost(t, community.ID, title, sub.DID, uri, "url "+link, func(p lemmyPostFields) bool {
			return p.URL == link
		})
		t.Logf("lemmy stored body: %q", post.Body)
		wantBody := "images beside a link\n\n" + imageMarkdown("linked first", pds.did, cid0) + "\n\n" + imageMarkdown("linked second", pds.did, cid1)
		if post.Body != wantBody {
			t.Errorf("lemmy body = %q, want %q", post.Body, wantBody)
		}
	})

	t.Run("comment with an image", func(t *testing.T) {
		h.awaitAppViewCommunityIndexed(t, sub.DID)
		blob, cid := h.uploadUniqueImage(t, pds, "comment image")
		const alt = "comment square"

		title, postURI, postCID, _, _ := writePost(t, "comment parent", "parent of an image comment", nil)
		h.awaitLemmyPostBody(t, community.ID, title, sub.DID, postURI)

		text := "comment with an image " + h.suffix
		commentURI := writeComment(t, "image", postURI, postCID, text, imagesEmbed(galleryEntry(blob, alt)))

		content := h.awaitLemmyCommentContaining(t, community.ID, text, commentURI)
		t.Logf("lemmy stored comment: %q", content)
		want := imageMarkdown(alt, pds.did, cid)
		if n := strings.Count(content, want); n != 1 {
			t.Errorf("lemmy comment %q holds the image %q %d times, want exactly once", content, want, n)
		}
	})

	t.Run("image-only reply", func(t *testing.T) {
		h.awaitAppViewCommunityIndexed(t, sub.DID)
		blob, cid := h.uploadUniqueImage(t, pds, "image only reply")
		const alt = "reply square"

		title, postURI, postCID, _, _ := writePost(t, "reply parent", "parent of an image-only reply", nil)
		h.awaitLemmyPostBody(t, community.ID, title, sub.DID, postURI)

		commentURI := writeComment(t, "image-only", postURI, postCID, "", imagesEmbed(galleryEntry(blob, alt)))

		content := h.awaitLemmyCommentContaining(t, community.ID, covesMediaImageURL(pds.did, cid), commentURI)
		t.Logf("lemmy stored comment: %q", content)
		if want := imageMarkdown(alt, pds.did, cid); !strings.Contains(content, want) {
			t.Errorf("lemmy comment %q lacks the image %q", content, want)
		}
	})

	t.Run("text post edited to add an image", func(t *testing.T) {
		h.awaitAppViewCommunityIndexed(t, sub.DID)
		blob, cid := h.uploadUniqueImage(t, pds, "edit adds image")
		const alt = "edited in square"

		title, uri, _, rkey, record := writePost(t, "edit", "text post that gains an image", nil)
		before := h.awaitLemmyPost(t, community.ID, title, sub.DID, uri, "the text post", func(lemmyPostFields) bool { return true })
		if before.URL != "" {
			t.Fatalf("lemmy url = %q before the edit, want none for a text post", before.URL)
		}

		record["embed"] = imagesEmbed(galleryEntry(blob, alt))
		if _, _, err := pds.putRecord(colPostV2, rkey, record); err != nil {
			t.Fatalf("putRecord edit post: %v", err)
		}

		wantURL := covesMediaImageURL(pds.did, cid)
		after := h.awaitLemmyPost(t, community.ID, title, sub.DID, uri, "url "+wantURL+" after the edit", func(p lemmyPostFields) bool {
			return p.URL == wantURL
		})
		if after.ID != before.ID {
			t.Errorf("the edit landed on Lemmy post %d, want the original post %d", after.ID, before.ID)
		}
		if after.AltText != alt {
			t.Errorf("lemmy alt_text after the edit = %q, want %q", after.AltText, alt)
		}
		after = h.awaitLemmyPost(t, community.ID, title, sub.DID, uri, "url_content_type image/* and a thumbnail_url after the edit", func(p lemmyPostFields) bool {
			return strings.HasPrefix(p.URLContentType, "image/") && p.ThumbnailURL != ""
		})
		t.Logf("lemmy metadata after the edit: url=%q url_content_type=%q thumbnail_url=%q", after.URL, after.URLContentType, after.ThumbnailURL)
	})

	t.Run("image post edited to drop its embed", func(t *testing.T) {
		h.awaitAppViewCommunityIndexed(t, sub.DID)
		blob, cid := h.uploadUniqueImage(t, pds, "edit drops image")
		const alt = "edited out square"

		title, uri, _, rkey, record := writePost(t, "drop", "image post that loses its image", imagesEmbed(galleryEntry(blob, alt)))
		wantURL := covesMediaImageURL(pds.did, cid)
		before := h.awaitLemmyPost(t, community.ID, title, sub.DID, uri, "url "+wantURL+" before the edit", func(p lemmyPostFields) bool {
			return p.URL == wantURL
		})

		// Lemmy 0.19.20 cannot clear a post URL through an Update: insert_apub
		// upserts with an AsChangeset form, which skips None fields, so the old
		// url and alt_text stay (crates/db_schema/src/impls/post.rs insert_apub).
		// The test pins that the Update without an attachment is applied, by
		// its new body, and logs what Lemmy kept.
		const editedBody = "image post that lost its image"
		delete(record, "embed")
		record["content"] = editedBody
		if _, _, err := pds.putRecord(colPostV2, rkey, record); err != nil {
			t.Fatalf("putRecord edit post: %v", err)
		}

		after := h.awaitLemmyPost(t, community.ID, title, sub.DID, uri, "the edited body after the edit dropped the embed", func(p lemmyPostFields) bool {
			return p.Body == editedBody
		})
		if after.ID != before.ID {
			t.Errorf("the edit landed on Lemmy post %d, want the original post %d", after.ID, before.ID)
		}
		t.Logf("lemmy after the edit: url=%q url_content_type=%q thumbnail_url=%q alt_text=%q", after.URL, after.URLContentType, after.ThumbnailURL, after.AltText)
	})
}
