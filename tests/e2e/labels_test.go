//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"
)

// covesReasonModeratorDiscretion is the public reason the instance admin
// gives for the removal and the restore.
const covesReasonModeratorDiscretion = "social.coves.moderation.defs#reasonModeratorDiscretion"

// covesStrongRef is com.atproto.repo.strongRef.
type covesStrongRef struct {
	URI string `json:"uri"`
	CID string `json:"cid"`
}

// covesSubjectState is the slice of social.coves.moderation.defs#subjectState
// this test reads.
type covesSubjectState struct {
	Version        string          `json:"version"`
	RecordState    string          `json:"recordState"`
	CurrentSubject *covesStrongRef `json:"currentSubject"`
	LocalRemoval   *struct {
		ActionID string `json:"actionId"`
	} `json:"localRemoval"`
}

// PRD moderation gaps, chunk 02 (B12): an instance-admin removal on the stack
// AppView reaches Tidepool as a `!takedown` label from the AppView's did:web
// labeler, and Tidepool withholds the bridged post's image until the restore
// negates the label.
func TestLabels_InstanceRemovalWithholdsTheBridgedImageUntilTheRestore(t *testing.T) {
	h := newHarness(t)
	community, sub := setupSubscribedCommunity(t, h, "lblimg")
	h.awaitAppViewCommunityIndexed(t, sub.DID)
	user := h.registerUser(t, h.uniqueName(t, "lblrhea"))

	// Bytes unique to this run: a withhold left by a failed run must not reach
	// another run's image.
	imageURL := user.uploadImage(t, "labelled.png", makeUniqueTestPNG(t, h.suffix+"/"+t.Name()))

	l := h.newListener(t, cursorNow(), colPostV2)
	title := "Label-removed image post " + h.suffix
	user.createImagePost(t, community.ID, title, imageURL, "label-removed e2e image", false)
	createEv := l.await("image post create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "title")
		return e.Commit.Collection == colPostV2 && e.Commit.Operation == opCreate && got == title
	})
	authorDID := createEv.Did
	postURI := createEv.atURI()
	blobCID := stringAt(t, decodeRecord(t, createEv.Commit.Record), "embed", "images", "0", "image", "ref", "$link")
	if blobCID == "" {
		t.Fatal("embed image blob ref.$link is empty")
	}
	original := h.awaitBlobServed(t, authorDID, blobCID, nil, "before the removal")

	pdsAddress, err := url.Parse(pdsURL())
	if err != nil {
		t.Fatalf("parse pdsURL %q: %v", pdsURL(), err)
	}
	signIn := covesSignIn{
		AppViewURL:    covesAppViewURL(),
		DialOverrides: map[string]string{"localhost:3001": "127.0.0.1:" + pdsAddress.Port()},
	}
	adminToken := covesSignInAs(t, signIn, covesAdminHandle, covesAdminPassword)

	// ── Removal: the AppView emits !takedown, Tidepool withholds ──────────
	indexed := h.awaitCovesSubjectState(t, postURI, adminToken, "the bridged post indexed", func(s covesSubjectState) bool {
		return s.RecordState == "present" && s.CurrentSubject != nil
	})
	removed := h.covesModerationMutation(t, "social.coves.moderation.removeContent", adminToken, map[string]any{
		"subject":         indexed.CurrentSubject,
		"expectedVersion": indexed.Version,
		"idempotencyKey":  "b12-remove-" + h.suffix,
		"reason":          covesReasonModeratorDiscretion,
	})
	if removed.LocalRemoval == nil || removed.LocalRemoval.ActionID == "" {
		t.Fatalf("removeContent result state carries no localRemoval action: %+v", removed)
	}
	h.awaitBlobWithheld(t, authorDID, blobCID, "after the instance removal")

	// ── Restore: the label is negated, Tidepool serves the image again ────
	h.covesModerationMutation(t, "social.coves.moderation.restoreContent", adminToken, map[string]any{
		"actionId":        removed.LocalRemoval.ActionID,
		"expectedVersion": removed.Version,
		"idempotencyKey":  "b12-restore-" + h.suffix,
		"reason":          covesReasonModeratorDiscretion,
		"reviewedSubject": indexed.CurrentSubject,
	})
	h.awaitBlobServed(t, authorDID, blobCID, original, "after the restore")
}

// awaitCovesSubjectState polls getSubjectState for subject as the token's
// account until ready holds, and returns that state.
func (h *harness) awaitCovesSubjectState(t *testing.T, subject, token, desc string, ready func(covesSubjectState) bool) covesSubjectState {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	query := url.Values{"subject": {subject}}
	var last string
	for {
		status, body, err := h.covesXRPC(http.MethodGet, "social.coves.moderation.getSubjectState", query, nil, token)
		switch {
		case err != nil:
			last = "error: " + err.Error()
		case status != http.StatusOK:
			last = fmt.Sprintf("HTTP %d: %s", status, truncate(body, 300))
		default:
			var out struct {
				State covesSubjectState `json:"state"`
			}
			if err := json.Unmarshal(body, &out); err != nil {
				last = fmt.Sprintf("decode: %v (%s)", err, truncate(body, 300))
			} else if ready(out.State) {
				return out.State
			} else {
				last = fmt.Sprintf("state not yet %s: %s", desc, truncate(body, 300))
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("Coves getSubjectState of %s never showed %s within %s (last: %s)", subject, desc, eventTimeout, last)
		}
		time.Sleep(time.Second)
	}
}

// covesModerationMutation calls an admin moderation procedure and returns the
// mutationResult's state. Fatal unless the call answers 200.
func (h *harness) covesModerationMutation(t *testing.T, nsid, token string, input map[string]any) covesSubjectState {
	t.Helper()
	status, body, err := h.covesXRPC(http.MethodPost, nsid, nil, input, token)
	if err != nil {
		t.Fatalf("%s as the instance admin: %v", nsid, err)
	}
	if status != http.StatusOK {
		t.Fatalf("%s as the instance admin = HTTP %d (%s), want 200", nsid, status, truncate(body, 300))
	}
	var out struct {
		State covesSubjectState `json:"state"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("%s result %s: %v", nsid, truncate(body, 300), err)
	}
	return out.State
}

// awaitBlobServed polls Tidepool getBlob until it serves the blob (equal to
// want when want is non-nil) and returns the bytes.
func (h *harness) awaitBlobServed(t *testing.T, did, cid string, want []byte, when string) []byte {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	query := url.Values{"did": {did}, "cid": {cid}}
	var last string
	for {
		res, err := h.bridgeXRPC("/xrpc/com.atproto.sync.getBlob", query)
		switch {
		case err != nil:
			last = "error: " + err.Error()
		case res.status != http.StatusOK:
			last = fmt.Sprintf("status %d error %q: %s", res.status, res.errCode, truncate(res.body, 200))
		case want != nil && !bytes.Equal(res.body, want):
			last = fmt.Sprintf("served %d bytes, want the original %d bytes", len(res.body), len(want))
		default:
			return res.body
		}
		if time.Now().After(deadline) {
			t.Fatalf("getBlob(%s, %s) %s never served the image within %s (last: %s)", did, cid, when, eventTimeout, last)
		}
		time.Sleep(time.Second)
	}
}

// awaitBlobWithheld polls Tidepool getBlob until it answers 404 BlobNotFound,
// then holds it to assertBlobAbsent's full check.
func (h *harness) awaitBlobWithheld(t *testing.T, did, cid, when string) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	query := url.Values{"did": {did}, "cid": {cid}}
	var last string
	for {
		res, err := h.bridgeXRPC("/xrpc/com.atproto.sync.getBlob", query)
		switch {
		case err != nil:
			last = "error: " + err.Error()
		case res.status == http.StatusNotFound && res.errCode == "BlobNotFound":
			assertBlobAbsent(t, h, did, cid, when)
			return
		default:
			last = fmt.Sprintf("status %d error %q", res.status, res.errCode)
		}
		if time.Now().After(deadline) {
			t.Fatalf("getBlob(%s, %s) %s never answered BlobNotFound within %s (last: %s)", did, cid, when, eventTimeout, last)
		}
		time.Sleep(time.Second)
	}
}
