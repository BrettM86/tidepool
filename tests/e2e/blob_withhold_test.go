//go:build e2e

package e2e

import (
	"bytes"
	"crypto/sha256"
	"image"
	"image/color"
	"image/png"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// TestBlobWithhold_OperatorWithholdSurvivesEditAndLifts (B14): an operator
// withholds a bridged Lemmy image post's blob through the admin API. The blob
// then answers getBlob as an absent blob would while the post record keeps
// referencing it; an upstream edit rebuilds the record with the same ref and
// does not bring the bytes back; the operator's lift serves the original
// bytes again with no further upstream change.
func TestBlobWithhold_OperatorWithholdSurvivesEditAndLifts(t *testing.T) {
	h := newHarness(t)
	community, _ := setupSubscribedCommunity(t, h, "withhold")
	user := h.registerUser(t, h.uniqueName(t, "wanda"))

	// Bytes unique to this run: a withhold left behind by a failed run must
	// not reach another test's (or another run's) image.
	imageURL := user.uploadImage(t, "withhold.png", makeUniqueTestPNG(t, h.suffix+"/"+t.Name()))

	l := h.newListener(t, cursorNow(), colPostV2)
	title := "Withheld image post " + h.suffix
	post := user.createImagePost(t, community.ID, title, imageURL, "withheld e2e image", false)

	createEv := l.await("image post create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "title")
		return e.Commit.Collection == colPostV2 && e.Commit.Operation == opCreate && got == title
	})
	authorDID, rkey := createEv.Did, createEv.Commit.RKey
	blobCID := stringAt(t, decodeRecord(t, createEv.Commit.Record), "embed", "images", "0", "image", "ref", "$link")
	if blobCID == "" {
		t.Fatal("embed image blob ref.$link is empty")
	}
	original, _ := h.bridgeGetBlob(t, authorDID, blobCID)

	lifted := false
	t.Cleanup(func() {
		if !lifted {
			_ = h.adminDo(http.MethodPost, "/admin/blobs/lift",
				map[string]any{"cid": blobCID, "actor": "e2e-cleanup"}, nil)
		}
	})
	if err := h.adminDo(http.MethodPost, "/admin/blobs/withhold", map[string]any{
		"cid": blobCID, "reason_code": "operator-discretion", "actor": "e2e-operator-alice",
	}, nil); err != nil {
		t.Fatalf("withhold %s: %v", blobCID, err)
	}

	assertBlobAbsent(t, h, authorDID, blobCID, "after the withhold")
	rec := h.awaitRecordPresent(t, authorDID, colPostV2, rkey, "withheld image post")
	if got, _ := softString(embedImage(t, rec.Value), "ref", "$link"); got != blobCID {
		t.Errorf("after the withhold the record's image ref = %q, want %q", got, blobCID)
	}

	const editedBody = "edited while the image is withheld"
	user.editPost(t, post.ID, editedBody)
	updateEv := l.await("withheld image post update", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "content")
		return e.Commit.Collection == colPostV2 && e.Commit.Operation == opUpdate &&
			e.Did == authorDID && e.Commit.RKey == rkey && got == editedBody
	})
	if got := stringAt(t, decodeRecord(t, updateEv.Commit.Record), "embed", "images", "0", "image", "ref", "$link"); got != blobCID {
		t.Errorf("the edited record's image ref = %q, want the withheld %q", got, blobCID)
	}
	rec, _ = h.bridgeGetRecord(t, authorDID, colPostV2, rkey)
	if got, _ := softString(rec.Value, "content"); got != editedBody {
		t.Errorf("getRecord content after the edit = %q, want %q", got, editedBody)
	}
	if got, _ := softString(embedImage(t, rec.Value), "ref", "$link"); got != blobCID {
		t.Errorf("getRecord image ref after the edit = %q, want %q", got, blobCID)
	}
	assertBlobAbsent(t, h, authorDID, blobCID, "after the upstream edit")

	if err := h.adminDo(http.MethodPost, "/admin/blobs/lift",
		map[string]any{"cid": blobCID, "actor": "e2e-operator-carol"}, nil); err != nil {
		t.Fatalf("lift %s: %v", blobCID, err)
	}
	lifted = true
	served, _ := h.bridgeGetBlob(t, authorDID, blobCID)
	if !bytes.Equal(served, original) {
		t.Errorf("after the lift getBlob served %d bytes, want the original %d bytes", len(served), len(original))
	}
}

// assertBlobAbsent asserts getBlob answers exactly as for a never-stored
// blob: 404 BlobNotFound, nothing that names a withhold.
func assertBlobAbsent(t *testing.T, h *harness, did, cid, when string) {
	t.Helper()
	res, err := h.bridgeXRPC("/xrpc/com.atproto.sync.getBlob", url.Values{"did": {did}, "cid": {cid}})
	if err != nil {
		t.Fatalf("getBlob(%s, %s) %s: %v", did, cid, when, err)
	}
	if res.status != http.StatusNotFound || res.errCode != "BlobNotFound" {
		t.Errorf("getBlob %s = status %d error %q, want 404 BlobNotFound: %s",
			when, res.status, res.errCode, truncate(res.body, 200))
	}
	if strings.Contains(strings.ToLower(string(res.body)), "withheld") {
		t.Errorf("getBlob %s body names the withhold: %s", when, truncate(res.body, 200))
	}
}

// embedImage returns the post record's single embed image blob value.
func embedImage(t *testing.T, value map[string]any) map[string]any {
	t.Helper()
	blob, ok := valueAt(t, value, "embed", "images", "0", "image").(map[string]any)
	if !ok {
		t.Fatalf("record has no embed image blob: %v", valueAt(t, value, "embed"))
	}
	return blob
}

// makeUniqueTestPNG is an 8x8 PNG whose pixels derive from seed, so each seed
// gets its own blob CID.
func makeUniqueTestPNG(t *testing.T, seed string) []byte {
	t.Helper()
	sum := sha256.Sum256([]byte(seed))
	img := image.NewRGBA(image.Rect(0, 0, 8, 8))
	for x := range 8 {
		for y := range 8 {
			img.Set(x, y, color.RGBA{R: sum[(x*8+y)%32], G: uint8(x * 32), B: uint8(y * 32), A: 0xff})
		}
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("encode test png: %v", err)
	}
	return buf.Bytes()
}
