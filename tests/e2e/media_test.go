//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"image"
	"image/color"
	"image/png"
	"strconv"
	"testing"
)

// makeTestPNG renders a small gradient PNG entirely in-process — no fixture
// files, nothing fetched (LOCAL-ONLY).
func makeTestPNG(t *testing.T) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 8, 8))
	for x := range 8 {
		for y := range 8 {
			img.Set(x, y, color.RGBA{R: uint8(x * 32), G: uint8(y * 32), B: 0x80, A: 0xff})
		}
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("encode test png: %v", err)
	}
	return buf.Bytes()
}

// decodeRecord decodes a firehose record for structural assertions that
// fieldOf (string-leaf, object-only paths) cannot express — array-valued
// embeds above all.
func decodeRecord(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	var rec map[string]any
	if err := json.Unmarshal(raw, &rec); err != nil {
		t.Fatalf("decode record: %v (%s)", err, truncate(raw, 300))
	}
	return rec
}

// valueAt walks a decoded JSON value by object keys and (numeric) array
// indexes, failing the test with the full path on any mismatch.
func valueAt(t *testing.T, root any, path ...string) any {
	t.Helper()
	cur := root
	for i, key := range path {
		switch node := cur.(type) {
		case map[string]any:
			next, ok := node[key]
			if !ok {
				t.Fatalf("record path %v: key %q missing at step %d", path, key, i)
			}
			cur = next
		case []any:
			idx, err := strconv.Atoi(key)
			if err != nil || idx < 0 || idx >= len(node) {
				t.Fatalf("record path %v: index %q invalid for array of %d at step %d", path, key, len(node), i)
			}
			cur = node[idx]
		default:
			t.Fatalf("record path %v: cannot descend into %T at step %d", path, cur, i)
		}
	}
	return cur
}

func stringAt(t *testing.T, root any, path ...string) string {
	t.Helper()
	v := valueAt(t, root, path...)
	s, ok := v.(string)
	if !ok {
		t.Fatalf("record path %v: %T is not a string", path, v)
	}
	return s
}

func numberAt(t *testing.T, root any, path ...string) float64 {
	t.Helper()
	v := valueAt(t, root, path...)
	f, ok := v.(float64)
	if !ok {
		t.Fatalf("record path %v: %T is not a number", path, v)
	}
	return f
}

// Scenario 9 (task 10): image post — a real pictrs upload through Lemmy's
// authenticated proxy, an NSFW image post, the bridge fetching and storing
// the blob, and `embed.images` + the nsfw self-label crossing the relay-fed
// wire (lexicon-validated by the listener's vetEvent like every consumed
// create/update). Closes the materializer test gap "embed.images never
// appears on the wire".
//
// Wire facts (verified against Lemmy 0.19.19 source): the Page for an image
// post carries attachment[0] = {"type":"Image","url":…,"name":<alt_text>} —
// mediaType is DROPPED for Image variants, so the type field (with an
// extension fallback) is what the materializer classifies on — plus
// sensitive=true and a top-level thumbnail image.
func TestImagePost_EmbedImagesAndNSFWLabel(t *testing.T) {
	h := newHarness(t)
	community, sub := setupSubscribedCommunity(t, h, "img")
	user := h.registerUser(t, h.uniqueName(t, "iris"))

	pngBytes := makeTestPNG(t)
	imageURL := user.uploadImage(t, "e2e.png", pngBytes)
	t.Logf("uploaded image: %s", imageURL)

	cursor := cursorNow()
	l := h.newListener(t, cursor, colPost)

	title := "Image post " + h.suffix
	const altText = "tiny e2e gradient"
	user.createImagePost(t, community.ID, title, imageURL, altText, true /* nsfw */)

	ev := l.await("image post create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "title")
		return e.Commit.Collection == colPost && e.Did == sub.DID &&
			e.Commit.Operation == opCreate && got == title
	})
	rec := decodeRecord(t, ev.Commit.Record)

	// The embed is the images arm (not external), with exactly one image
	// whose alt text survived Lemmy's alt_text → attachment.name → alt.
	if got := stringAt(t, rec, "embed", "$type"); got != "social.coves.embed.images" {
		t.Fatalf("embed $type = %q, want social.coves.embed.images", got)
	}
	images, ok := valueAt(t, rec, "embed", "images").([]any)
	if !ok || len(images) != 1 {
		t.Fatalf("embed.images = %v, want exactly 1 image", valueAt(t, rec, "embed", "images"))
	}
	if got := stringAt(t, rec, "embed", "images", "0", "alt"); got != altText {
		t.Errorf("embed.images[0].alt = %q, want %q", got, altText)
	}

	// The image value is a real blob ref: the bridge fetched the bytes and
	// committed a content-addressed blob, not a URL passthrough.
	if got := stringAt(t, rec, "embed", "images", "0", "image", "$type"); got != "blob" {
		t.Errorf("embed image $type = %q, want blob", got)
	}
	blobCID := stringAt(t, rec, "embed", "images", "0", "image", "ref", "$link")
	if blobCID == "" {
		t.Fatal("embed image blob ref.$link is empty")
	}
	mimeType := stringAt(t, rec, "embed", "images", "0", "image", "mimeType")
	if mimeType != "image/png" {
		t.Errorf("embed image mimeType = %q, want image/png", mimeType)
	}
	size := numberAt(t, rec, "embed", "images", "0", "image", "size")
	if size <= 0 {
		t.Errorf("embed image size = %v, want > 0", size)
	}

	// NSFW crossed the wire: Page.sensitive=true → nsfw self-label.
	if got := stringAt(t, rec, "labels", "$type"); got != "com.atproto.label.defs#selfLabels" {
		t.Errorf("labels $type = %q, want com.atproto.label.defs#selfLabels", got)
	}
	if got := stringAt(t, rec, "labels", "values", "0", "val"); got != "nsfw" {
		t.Errorf("labels.values[0].val = %q, want nsfw", got)
	}

	// The blob is stored and served through the bridge's getBlob (the
	// AppView's media path): posts live in the community repo, so the blob
	// does too. Byte length must match the record's size claim; the bytes
	// must decode as the image we uploaded (pict-rs 0.5 serves the original
	// alias unmodified, but dimensions — not byte identity — are the
	// contract worth pinning against a future pict-rs that re-encodes).
	data, contentType := h.bridgeGetBlob(t, sub.DID, blobCID)
	if int64(len(data)) != int64(size) {
		t.Errorf("getBlob returned %d bytes, record claims size %d", len(data), int64(size))
	}
	if contentType != mimeType {
		t.Errorf("getBlob Content-Type = %q, want the record's mimeType %q", contentType, mimeType)
	}
	decoded, err := png.Decode(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("stored blob does not decode as png: %v", err)
	}
	if b := decoded.Bounds(); b.Dx() != 8 || b.Dy() != 8 {
		t.Errorf("stored blob decodes to %dx%d, want the uploaded 8x8", b.Dx(), b.Dy())
	}
	if !bytes.Equal(data, pngBytes) {
		// Informational, not fatal: pict-rs may legally strip metadata or
		// re-encode; the dimension + size + mime assertions above are the
		// load-bearing ones.
		t.Logf("stored blob bytes differ from the upload (%d vs %d bytes) — pictrs transformed the image", len(data), len(pngBytes))
	}
}
