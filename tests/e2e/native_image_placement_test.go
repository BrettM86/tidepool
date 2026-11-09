//go:build e2e

package e2e

// Native postv2 images placed inline by an #image facet, through the real
// stack: the bridge renders the gallery image at the facet's position as a
// markdown image whose URL points at the Coves AppView image proxy, and holds
// the first delivery until that proxy serves the image.
//
// The URL in Lemmy's stored body is the in-stack origin the bridge is
// configured with (COVES_MEDIA_BASE_URL in docker-compose.e2e.yml), not the
// host-published covesAppViewURL: Lemmy and the bridge reach the AppView on
// the compose network.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
)

const facetImage = "social.coves.richtext.facet#image"

// mediaPendingClass is the outbound_deliveries.last_error_class the media
// readiness gate parks a delivery with.
const mediaPendingClass = "media_pending"

// covesMediaOrigin is the AppView image proxy origin as the bridge and Lemmy
// see it inside the compose network.
func covesMediaOrigin() string { return envOr("COVES_MEDIA_E2E_ORIGIN", "http://relay:8081") }

// imagePlacementPost is one native postv2 with a single gallery image placed by
// an #image facet over the word "pic" in "before pic after".
type imagePlacementPost struct {
	uri     string
	title   string
	created time.Time
}

const (
	imagePlacementContent = "before pic after"
	imagePlacementAlt     = "a solid colour square"
)

// createImagePlacementPost writes the postv2 into the native repo.
func createImagePlacementPost(t *testing.T, pds *pdsClient, suffix, communityDID string, blob map[string]any) imagePlacementPost {
	t.Helper()
	rkey := syntax.NewTIDNow(0).String()
	title := fmt.Sprintf("image placement %s %s", suffix, rkey)
	record := map[string]any{
		"$type":     colPostV2,
		"community": communityDID,
		"title":     title,
		"content":   imagePlacementContent,
		"facets": []any{
			facetOver(t, imagePlacementContent, "pic", map[string]any{"$type": facetImage, "index": 0}),
		},
		"embed": map[string]any{
			"$type": "social.coves.embed.images",
			"images": []any{
				map[string]any{"image": blob, "alt": imagePlacementAlt},
			},
		},
		"createdAt": time.Now().UTC().Format(time.RFC3339),
	}
	created := time.Now()
	uri, _, err := pds.createRecord(colPostV2, rkey, record)
	if err != nil {
		t.Fatalf("createRecord image placement post in %s: %v", communityDID, err)
	}
	t.Logf("wrote %s", uri)
	return imagePlacementPost{uri: uri, title: title, created: created}
}

// uploadUniqueImage uploads a run-unique copy of the testdata PNG fixture and
// asserts the AppView refuses it before any record references it.
func (h *harness) uploadUniqueImage(t *testing.T, pds *pdsClient, label string) (blob map[string]any, cid string) {
	t.Helper()
	base, err := os.ReadFile(filepath.Join("testdata", "image-indexed.png"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	blob, err = pds.uploadBlob(uniquePNG(t, base, h.suffix+" "+label), "image/png")
	if err != nil {
		t.Fatalf("uploadBlob: %v", err)
	}
	cid = blobCID(t, blob)
	imageURL := covesImageURL(pds.did, cid)
	got, err := h.fetchImage(http.MethodGet, imageURL)
	if err != nil {
		t.Fatalf("GET %s: %v", imageURL, err)
	}
	if got.status != http.StatusNotFound {
		t.Fatalf("GET %s before any post = %d, want 404: a 200 before the post means the CID is not unique to this run", imageURL, got.status)
	}
	return blob, cid
}

// expectedImageMarkdown is the image node Lemmy must store, written from the
// URL contract's literal path segments.
func expectedImageMarkdown(did, cid string) string {
	return "![" + imagePlacementAlt + "](" + covesMediaOrigin() + "/img/content_full/plain/" + did + "/" + cid + ")"
}

// apUserHost is AP_USER_ORIGIN's host (docker-compose.e2e.yml). The bridge's
// Host router serves native authors' AP objects only under it, so a request
// to the host-published bridge URL carries it as the Host header.
func apUserHost() string { return envOr("AP_USER_E2E_HOST", "coves-users") }

// fetchUserOriginObjectContent GETs the bridge's AP object for a native
// at:// URI under the user origin and returns its "content" field.
func (h *harness) fetchUserOriginObjectContent(t *testing.T, atURI string) string {
	t.Helper()
	objectPath := "/ap/object/" + strings.TrimPrefix(atURI, "at://")
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, tidepoolURL()+objectPath, nil)
	if err != nil {
		t.Fatalf("build request for %s: %v", objectPath, err)
	}
	req.Host = apUserHost()
	req.Header.Set("Accept", "application/activity+json")
	resp, err := h.http.Do(req)
	if err != nil {
		t.Fatalf("GET %s (Host %s): %v", objectPath, req.Host, err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read GET %s (Host %s): %v", objectPath, req.Host, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s (Host %s) = %d, want 200; body %s", objectPath, req.Host, resp.StatusCode, truncate(raw, 500))
	}
	var object struct {
		Content string `json:"content"`
	}
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatalf("decode GET %s (Host %s): %v; body %s", objectPath, req.Host, err, truncate(raw, 500))
	}
	return object.Content
}

// appViewCompose runs one docker compose action on the coves-appview service.
func appViewCompose(t *testing.T, action string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "compose", "-f", composeFile(t), action, "coves-appview")
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%s coves-appview: %v\n%s", action, err, out)
	}
	return nil
}

// pauseAppView freezes the AppView process. Its port stays bound in the relay's
// network namespace, so requests to it connect and then hang: never call the
// proxy while it is paused without a short timeout. The returned function
// unpauses it; a t.Cleanup unpauses it too if the test never did, because the
// rest of the suite needs the AppView.
func pauseAppView(t *testing.T) (unpause func()) {
	t.Helper()
	if err := appViewCompose(t, "pause"); err != nil {
		t.Fatal(err)
	}
	paused := true
	t.Cleanup(func() {
		if !paused {
			return
		}
		if err := appViewCompose(t, "unpause"); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})
	return func() {
		t.Helper()
		if err := appViewCompose(t, "unpause"); err != nil {
			t.Fatal(err)
		}
		paused = false
	}
}

// TestNativeImage_InlinePlacementFederatesProxyURL: a native postv2 whose one
// gallery image is placed by an #image facet reaches Lemmy with the image's
// proxy URL at the facet's position, and that URL serves the image.
func TestNativeImage_InlinePlacementFederatesProxyURL(t *testing.T) {
	h := newHarness(t)

	owner := h.registerUser(t, h.uniqueName(t, "imgp_own"))
	name := h.uniqueName(t, "imgp_comm")
	community := owner.createCommunity(t, name, "image placement "+name)
	sub := h.subscribeCommunity(t, "!"+name+"@lemmy")
	if sub.DID == "" {
		t.Fatalf("subscribed %s but the bridge reported no community DID: %+v", name, sub)
	}
	h.awaitAppViewCommunityIndexed(t, sub.DID)

	pds := &pdsClient{http: h.http}
	if err := pds.createSession(nativeHandle, nativePassword); err != nil {
		t.Fatalf("createSession as the native bootstrap account %q at %s: %v", nativeHandle, pdsURL(), err)
	}
	blob, cid := h.uploadUniqueImage(t, pds, "inline placement")

	post := createImagePlacementPost(t, pds, h.suffix, sub.DID, blob)

	body := h.awaitLemmyPostBody(t, community.ID, post.title, sub.DID, post.uri)
	t.Logf("lemmy stored body: %q", body)
	want := "before " + expectedImageMarkdown(pds.did, cid) + " after"
	if !strings.Contains(body, want) {
		t.Errorf("lemmy stored body %q lacks the image placed at the facet position %q", body, want)
	}
	if strings.Contains(body, "pic") {
		t.Errorf("lemmy stored body %q still holds the covered text %q; the placed image replaces it", body, "pic")
	}

	// The served AP object renders the placement as HTML under the media
	// base the bridge is configured with. The URL and alt hold no characters
	// HTML attribute escaping changes, so the expected tag is written literally.
	content := h.fetchUserOriginObjectContent(t, post.uri)
	wantTag := `<img src="` + covesMediaImageURL(pds.did, cid) + `" alt="` + imagePlacementAlt + `">`
	if !strings.Contains(content, wantTag) {
		t.Errorf("served AP object content %q lacks the placed image tag %q", content, wantTag)
	}

	imageURL := covesImageURL(pds.did, cid)
	got, err := h.fetchImage(http.MethodGet, imageURL)
	if err != nil {
		t.Fatalf("GET %s: %v", imageURL, err)
	}
	if got.status != http.StatusOK || !strings.HasPrefix(got.contentType, "image/") {
		t.Errorf("GET %s = %d %q, want 200 with Content-Type image/*", imageURL, got.status, got.contentType)
	}
}

// TestNativeImage_GateHoldsDeliveryUntilProxyServes: with the AppView paused
// after the blob upload, the bridge parks the post's delivery for media and
// Lemmy has no such post; once the AppView runs again and serves the image,
// the post arrives with the image placed.
func TestNativeImage_GateHoldsDeliveryUntilProxyServes(t *testing.T) {
	// holdWindow is how long Lemmy must stay without the post once the park
	// is observed; holdDeadline caps it from post creation, well under the
	// gate's 2-minute budget, after which the bridge delivers anyway.
	const (
		holdWindow    = 40 * time.Second
		holdDeadline  = 100 * time.Second
		minimumWindow = 20 * time.Second
	)

	h := newHarness(t)

	owner := h.registerUser(t, h.uniqueName(t, "imgg_own"))
	name := h.uniqueName(t, "imgg_comm")
	community := owner.createCommunity(t, name, "image gate "+name)
	sub := h.subscribeCommunity(t, "!"+name+"@lemmy")
	if sub.DID == "" {
		t.Fatalf("subscribed %s but the bridge reported no community DID: %+v", name, sub)
	}
	h.awaitAppViewCommunityIndexed(t, sub.DID)

	pds := &pdsClient{http: h.http}
	if err := pds.createSession(nativeHandle, nativePassword); err != nil {
		t.Fatalf("createSession as the native bootstrap account %q at %s: %v", nativeHandle, pdsURL(), err)
	}
	blob, cid := h.uploadUniqueImage(t, pds, "gate hold")
	db := openTidepoolDB(t)

	unpause := pauseAppView(t)
	post := createImagePlacementPost(t, pds, h.suffix, sub.DID, blob)

	h.awaitPostDeliveryParked(t, db, sub.DID, post.uri, mediaPendingClass, time.Time{})
	parkedAt := time.Now()
	holdEnd := parkedAt.Add(holdWindow)
	if capped := post.created.Add(holdDeadline); capped.Before(holdEnd) {
		holdEnd = capped
	}
	if holdEnd.Sub(parkedAt) < minimumWindow {
		t.Fatalf("the park was observed %s after the post was written, leaving a hold window of %s, want at least %s",
			parkedAt.Sub(post.created), holdEnd.Sub(parkedAt), minimumWindow)
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for time.Now().Before(holdEnd) {
		_, found, err := h.lemmyPostBody(community.ID, post.title)
		if err != nil {
			t.Fatalf("read Lemmy community %d while the AppView is paused: %v", community.ID, err)
		}
		if found {
			t.Fatalf("post %q reached Lemmy %s after the park, while the AppView was paused and its proxy could not serve the image",
				post.title, time.Since(parkedAt))
		}
		select {
		case <-t.Context().Done():
			t.Fatalf("holding for the paused AppView: %v", t.Context().Err())
		case <-ticker.C:
		}
	}

	unpause()

	body := h.awaitLemmyPostBody(t, community.ID, post.title, sub.DID, post.uri)
	t.Logf("lemmy stored body: %q", body)
	want := "before " + expectedImageMarkdown(pds.did, cid) + " after"
	if !strings.Contains(body, want) {
		t.Errorf("lemmy stored body %q lacks the image placed at the facet position %q", body, want)
	}
}
