//go:build e2e

package e2e

// The Coves AppView in the e2e stack: it indexes the stack's Jetstream and
// serves image blobs through its image proxy only for (DID, CID) pairs an
// indexed record references (Coves docs/IMAGE_URL_CONTRACT.md). Every later
// image e2e case reads images from it, so the helpers here are shared.

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"image/png"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
)

// appViewIndexTimeout bounds the wait for the AppView to index a community
// it saw on Jetstream.
const appViewIndexTimeout = 90 * time.Second

// appViewImageTimeout bounds the wait for the AppView to index a post and
// start serving its image.
const appViewImageTimeout = 60 * time.Second

// covesAppViewURL is the Coves AppView's host endpoint. The AppView shares
// the relay's network namespace, so it is published on the relay service.
func covesAppViewURL() string { return envOr("COVES_APPVIEW_E2E_URL", "http://localhost:8093") }

// covesImageURL builds the AppView's full-size image URL for an owner DID and
// blob CID, written from the URL contract's literal path segments rather than
// any production builder.
func covesImageURL(did, cid string) string {
	return covesAppViewURL() + "/img/content_full/plain/" + did + "/" + cid
}

// awaitAppViewCommunityIndexed polls the AppView until it has indexed the
// community. The AppView refuses a post whose community it has not indexed,
// so every image case calls this before posting.
func (h *harness) awaitAppViewCommunityIndexed(t *testing.T, communityDID string) {
	t.Helper()
	endpoint := covesAppViewURL() + "/xrpc/social.coves.community.get?community=" + url.QueryEscape(communityDID)
	deadline := time.Now().Add(appViewIndexTimeout)
	var last string
	for {
		resp, err := h.http.Get(endpoint)
		if err != nil {
			last = "error: " + err.Error()
		} else {
			raw, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
			last = fmt.Sprintf("status %d: %s", resp.StatusCode, truncate(raw, 200))
		}
		if time.Now().After(deadline) {
			t.Fatalf("AppView at %s never indexed community %s within %s (last: %s)",
				covesAppViewURL(), communityDID, appViewIndexTimeout, last)
		}
		time.Sleep(time.Second)
	}
}

// uploadBlob uploads raw bytes to the session's repo and returns the PDS's
// blob object unchanged ($type, ref, mimeType, size), ready to embed in a
// record.
func (c *pdsClient) uploadBlob(data []byte, mimeType string) (map[string]any, error) {
	req, err := http.NewRequest(http.MethodPost, pdsURL()+"/xrpc/com.atproto.repo.uploadBlob", bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", mimeType)
	req.Header.Set("Authorization", "Bearer "+c.accessJWT)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("pds uploadBlob: status %d: %s", resp.StatusCode, truncate(raw, 300))
	}
	var out struct {
		Blob map[string]any `json:"blob"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("pds uploadBlob: decode: %w (%s)", err, truncate(raw, 200))
	}
	if out.Blob == nil {
		return nil, fmt.Errorf("pds uploadBlob: response has no blob: %s", truncate(raw, 200))
	}
	return out.Blob, nil
}

// blobCID reads the CID from a blob object's ref.$link.
func blobCID(t *testing.T, blob map[string]any) string {
	t.Helper()
	ref, _ := blob["ref"].(map[string]any)
	cid, _ := ref["$link"].(string)
	if cid == "" {
		t.Fatalf("blob %v has no ref.$link", blob)
	}
	return cid
}

// imageResponse is one HEAD or GET of an image URL.
type imageResponse struct {
	status      int
	contentType string
	bodyLength  int
}

func (h *harness) fetchImage(method, imageURL string) (imageResponse, error) {
	req, err := http.NewRequest(method, imageURL, nil)
	if err != nil {
		return imageResponse{}, err
	}
	resp, err := h.http.Do(req)
	if err != nil {
		return imageResponse{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return imageResponse{}, err
	}
	return imageResponse{status: resp.StatusCode, contentType: resp.Header.Get("Content-Type"), bodyLength: len(body)}, nil
}

// TestCovesAppView_ServesIndexedImageBlob proves the AppView in the stack
// indexes a bridged community and a native postv2 from Jetstream, serves the
// post's image blob through its proxy, and refuses a blob that exists on the
// PDS but no indexed record references.
func TestCovesAppView_ServesIndexedImageBlob(t *testing.T) {
	h := newHarness(t)

	owner := h.registerUser(t, h.uniqueName(t, "img_own"))
	name := h.uniqueName(t, "img_comm")
	community := owner.createCommunity(t, name, "appview images "+name)
	sub := h.subscribeCommunity(t, "!"+name+"@lemmy")
	if sub.DID == "" {
		t.Fatalf("subscribed %s but the bridge reported no community DID: %+v", name, sub)
	}
	t.Logf("bridged community %s → %s", community.APID, sub.DID)

	h.awaitAppViewCommunityIndexed(t, sub.DID)

	pds := &pdsClient{http: h.http}
	if err := pds.createSession(nativeHandle, nativePassword); err != nil {
		t.Fatalf("createSession as the native bootstrap account %q at %s: %v", nativeHandle, pdsURL(), err)
	}

	// The PDS account and the fixture bytes are the same on every run, so the
	// committed fixtures alone would give the same (DID, CID) pairs as an
	// earlier run against the same stack. Each upload carries a tEXt chunk
	// with this run's suffix, which gives it a CID no earlier run produced.
	upload := func(fixture string) map[string]any {
		base, err := os.ReadFile(filepath.Join("testdata", fixture))
		if err != nil {
			t.Fatalf("read fixture %s: %v", fixture, err)
		}
		data := uniquePNG(t, base, h.suffix+" "+fixture)
		blob, err := pds.uploadBlob(data, "image/png")
		if err != nil {
			t.Fatalf("uploadBlob %s: %v", fixture, err)
		}
		return blob
	}
	blobIndexed := upload("image-indexed.png")
	blobUnreferenced := upload("image-unreferenced.png")
	cidIndexed := blobCID(t, blobIndexed)
	cidUnreferenced := blobCID(t, blobUnreferenced)
	if cidIndexed == cidUnreferenced {
		t.Fatalf("fixtures uploaded to the same CID %s; they must be distinct images", cidIndexed)
	}

	// No record references either blob yet, so the AppView must refuse both.
	// A 200 here means the CID is not unique to this run.
	for _, cid := range []string{cidIndexed, cidUnreferenced} {
		imageURL := covesImageURL(pds.did, cid)
		for _, method := range []string{http.MethodHead, http.MethodGet} {
			got, err := h.fetchImage(method, imageURL)
			if err != nil {
				t.Fatalf("%s %s: %v", method, imageURL, err)
			}
			if got.status != http.StatusNotFound {
				t.Fatalf("%s %s before any post = %d, want 404: a 200 before the post means the CID is not unique to this run", method, imageURL, got.status)
			}
		}
	}

	createImagePost := func(serial uint, communityDID string, blob map[string]any) string {
		rkey := syntax.NewTIDNow(serial).String()
		record := map[string]any{
			"$type":     colPostV2,
			"community": communityDID,
			"title":     fmt.Sprintf("appview image %s %s", h.suffix, rkey),
			"embed": map[string]any{
				"$type": "social.coves.embed.images",
				"images": []any{
					map[string]any{"image": blob, "alt": "solid colour square"},
				},
			},
			"createdAt": time.Now().UTC().Format(time.RFC3339),
		}
		uri, _, err := pds.createRecord(colPostV2, rkey, record)
		if err != nil {
			t.Fatalf("createRecord image post in %s: %v", communityDID, err)
		}
		t.Logf("wrote %s", uri)
		return uri
	}
	// The unreferenced blob is committed first, in a post to a community the
	// AppView never indexes, so it exists on the PDS without an indexed
	// reference. Jetstream delivers one repo's commits in order, so once the
	// indexed image is served the AppView has already processed this post.
	createImagePost(0, nativeCommunityDID, blobUnreferenced)
	createImagePost(1, sub.DID, blobIndexed)

	pdsBlobURL := pdsURL() + "/xrpc/com.atproto.sync.getBlob?did=" + url.QueryEscape(pds.did) + "&cid=" + url.QueryEscape(cidUnreferenced)
	pdsBlob, err := h.fetchImage(http.MethodGet, pdsBlobURL)
	if err != nil {
		t.Fatalf("GET %s: %v", pdsBlobURL, err)
	}
	if pdsBlob.status != http.StatusOK {
		t.Fatalf("PDS getBlob for the unreferenced blob = %d, want 200: the blob must exist upstream so the AppView's 404 comes from its reference check", pdsBlob.status)
	}

	indexedURL := covesImageURL(pds.did, cidIndexed)
	deadline := time.Now().Add(appViewImageTimeout)
	for {
		head, headErr := h.fetchImage(http.MethodHead, indexedURL)
		get, getErr := h.fetchImage(http.MethodGet, indexedURL)
		served := headErr == nil && getErr == nil &&
			head.status == http.StatusOK && strings.HasPrefix(head.contentType, "image/") &&
			get.status == http.StatusOK && strings.HasPrefix(get.contentType, "image/") && get.bodyLength > 0
		if served {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("AppView never served %s within %s: HEAD %+v (err %v), GET %+v (err %v); want 200 with Content-Type image/* and a non-empty GET body",
				indexedURL, appViewImageTimeout, head, headErr, get, getErr)
		}
		time.Sleep(time.Second)
	}

	unreferencedURL := covesImageURL(pds.did, cidUnreferenced)
	for _, method := range []string{http.MethodHead, http.MethodGet} {
		got, err := h.fetchImage(method, unreferencedURL)
		if err != nil {
			t.Fatalf("%s %s: %v", method, unreferencedURL, err)
		}
		if got.status != http.StatusNotFound {
			t.Errorf("%s %s = %d, want 404 for a blob no indexed record references", method, unreferencedURL, got.status)
		}
	}
}

// uniquePNG returns base with a tEXt chunk (keyword "tidepool-e2e", text
// label) inserted after IHDR, so the image decodes the same but its bytes,
// and so its blob CID, depend on label.
func uniquePNG(t *testing.T, base []byte, label string) []byte {
	t.Helper()
	signature := []byte("\x89PNG\r\n\x1a\n")
	if !bytes.HasPrefix(base, signature) {
		t.Fatalf("fixture is not a PNG: missing the 8-byte signature")
	}
	// The first chunk must be IHDR: 4-byte length, 4-byte type, data, 4-byte CRC.
	if len(base) < len(signature)+8 {
		t.Fatalf("PNG fixture ends before its first chunk header")
	}
	headerLength := binary.BigEndian.Uint32(base[len(signature):])
	headerType := string(base[len(signature)+4 : len(signature)+8])
	if headerType != "IHDR" || headerLength != 13 {
		t.Fatalf("PNG fixture's first chunk is %q with length %d, want IHDR with length 13", headerType, headerLength)
	}
	headerEnd := len(signature) + 8 + int(headerLength) + 4
	if len(base) < headerEnd {
		t.Fatalf("PNG fixture ends inside its IHDR chunk")
	}

	data := append([]byte("tidepool-e2e\x00"), label...)
	chunk := binary.BigEndian.AppendUint32(nil, uint32(len(data)))
	chunk = append(chunk, "tEXt"...)
	chunk = append(chunk, data...)
	chunk = binary.BigEndian.AppendUint32(chunk, crc32.ChecksumIEEE(chunk[4:]))

	out := make([]byte, 0, len(base)+len(chunk))
	out = append(out, base[:headerEnd]...)
	out = append(out, chunk...)
	out = append(out, base[headerEnd:]...)
	if _, err := png.Decode(bytes.NewReader(out)); err != nil {
		t.Fatalf("PNG with the tEXt chunk does not decode: %v", err)
	}
	return out
}
