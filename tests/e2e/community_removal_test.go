//go:build e2e

package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"testing"
	"time"
)

// PRD moderation gaps, chunk 05 (B8): the Coves instance admin removes a
// bridged community that Tidepool follows through a whole-instance entry of
// the follow list. The removal reaches Tidepool as a `!takedown` label on the
// community's bare DID: Tidepool unfollows the community, a later sweep of the
// same instance entry does not follow it again, and Tidepool's getBlob refuses
// the community's bridged image. The restore is cleanup only (task 06 owns
// what it reconciles).
func TestCommunityRemoval_RemovedCommunityIsUnfollowedThroughAWholeInstanceSweep(t *testing.T) {
	h := newHarness(t)
	db := openTidepoolDB(t)
	lemmyHost := lemmyHostname(t)
	owner := h.registerUser(t, h.uniqueName(t, "cr_own"))
	list := h.installFollowList(t, time.Hour)

	// Registered after installFollowList, so it runs while this list is still
	// the running one: later scenarios must not inherit a follow of every
	// Lemmy community.
	t.Cleanup(func() {
		list.instances = nil
		list.writeSwitches(nil, nil)
		if result := h.reconcileSweep(t); len(result.Failed) > 0 {
			t.Errorf("removing the instances entry failed on %v", result.Failed)
		}
	})

	name := h.uniqueName(t, "cr_comm")
	community := owner.createCommunity(t, name, "community removal "+name)
	list.instances = []followListInstance{{host: lemmyHost}}
	list.writeSwitches(nil, nil)
	sweeps := &instanceSweeps{t: t, h: h, failed: map[string]bool{}}
	sweeps.until("the community followed through the instance entry", func(tracked map[string]bool) bool {
		return tracksAll(tracked, community)
	})
	// The suite's subscribe closes Lemmy's first-contact Accept race and waits
	// for accepted; the follow stays the instance entry's.
	sub := h.subscribeCommunity(t, "!"+name+"@"+lemmyHost)
	owner.awaitRemoteFollowers(t, community.ID, 1, "after the instance entry's follow is accepted")
	h.awaitAppViewCommunityIndexed(t, sub.DID)

	// Bytes unique to this run: a withhold left by a failed run must not reach
	// another run's image.
	imageURL := owner.uploadImage(t, "community-removal.png", makeUniqueTestPNG(t, h.suffix+"/"+t.Name()))
	l := h.newListener(t, cursorNow(), colPostV2)
	title := "Community removal image post " + h.suffix
	owner.createImagePost(t, community.ID, title, imageURL, "community removal e2e image", false)
	createEv := l.await("bridged image post create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "title")
		return e.Commit.Collection == colPostV2 && e.Commit.Operation == opCreate && got == title
	})
	authorDID := createEv.Did
	blobCID := stringAt(t, decodeRecord(t, createEv.Commit.Record), "embed", "images", "0", "image", "ref", "$link")
	if blobCID == "" {
		t.Fatal("embed image blob ref.$link is empty")
	}
	h.awaitBlobServed(t, authorDID, blobCID, nil, "before the community removal")

	pdsAddress, err := url.Parse(pdsURL())
	if err != nil {
		t.Fatalf("parse pdsURL %q: %v", pdsURL(), err)
	}
	signIn := covesSignIn{
		AppViewURL:    covesAppViewURL(),
		DialOverrides: map[string]string{"localhost:3001": "127.0.0.1:" + pdsAddress.Port()},
	}
	adminToken := covesSignInAs(t, signIn, covesAdminHandle, covesAdminPassword)
	communitySubject := "at://" + sub.DID

	// ── The instance admin removes the community ─────────────────────────
	indexed := h.awaitCovesSubjectState(t, communitySubject, adminToken, "the community indexed", func(s covesSubjectState) bool {
		return s.Version != ""
	})
	// Registered before the removal, so a failure after it still restores
	// the community before the instance entry's cleanup sweep runs.
	t.Cleanup(func() { h.restoreRemovedCommunity(t, db, sub.DID, adminToken, "b8-restore-"+h.suffix) })
	h.covesModerationMutation(t, "social.coves.moderation.removeCommunity", adminToken, map[string]any{
		"subject":         sub.DID,
		"expectedVersion": indexed.Version,
		"idempotencyKey":  "b8-remove-" + h.suffix,
		"reason":          covesReasonModeratorDiscretion,
	})
	// Appliers run in the label's commit transaction: once the label is
	// stored, Tidepool has applied the removal.
	awaitCovesTakedownLabel(t.Context(), t, db, sub.DID, false)

	// ── A sweep of the same instance entry does not follow it again ──────
	result := h.reconcileSweep(t)
	if !slices.Contains(result.Suppressed, community.APID) {
		t.Errorf("the sweep after the removal reported suppressed %v, want it to include %s", result.Suppressed, community.APID)
	}
	if slices.Contains(result.Subscribed, community.APID) {
		t.Errorf("the sweep after the removal subscribed the removed community %s", community.APID)
	}
	if h.trackedCommunities(t)[community.APID] {
		t.Errorf("the removed community %s is still tracked after the sweep", community.APID)
	}
	owner.awaitRemoteFollowers(t, community.ID, 0, "after the removal and the sweep")
	h.awaitBlobWithheld(t, authorDID, blobCID, "after the community removal")
}

// remoteFollowers is how many accounts of other instances follow the
// community on the client's Lemmy. Tidepool's service actor is the only one
// that follows a community the test created.
func (c *lemmyClient) remoteFollowers(communityID int) (int, error) {
	var out struct {
		CommunityView struct {
			Counts struct {
				Subscribers      *int `json:"subscribers"`
				SubscribersLocal *int `json:"subscribers_local"`
			} `json:"counts"`
		} `json:"community_view"`
	}
	if err := c.do(http.MethodGet, fmt.Sprintf("/api/v3/community?id=%d", communityID), nil, &out); err != nil {
		return 0, err
	}
	counts := out.CommunityView.Counts
	if counts.Subscribers == nil || counts.SubscribersLocal == nil {
		return 0, fmt.Errorf("community %d view carries no subscribers/subscribers_local counts", communityID)
	}
	return *counts.Subscribers - *counts.SubscribersLocal, nil
}

// awaitRemoteFollowers polls Lemmy until the community has want remote
// followers.
func (c *lemmyClient) awaitRemoteFollowers(t *testing.T, communityID, want int, when string) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	var last string
	for {
		got, err := c.remoteFollowers(communityID)
		switch {
		case err != nil:
			last = "error: " + err.Error()
		case got == want:
			return
		default:
			last = fmt.Sprintf("%d remote followers", got)
		}
		if time.Now().After(deadline) {
			t.Fatalf("Lemmy community %d %s never had %d remote followers within %s (last: %s)",
				communityID, when, want, eventTimeout, last)
		}
		time.Sleep(time.Second)
	}
}

// restoreRemovedCommunity restores the community through restoreCommunity
// and waits until Tidepool has applied the negation, so a failed run does not
// leave the community removed for the rest of the suite. A community with no
// active removal answers StateConflict, which is left alone. It runs as a
// cleanup: errors are reported, and t.Context() is already cancelled.
func (h *harness) restoreRemovedCommunity(t *testing.T, db *sql.DB, did, token, idempotencyKey string) {
	t.Helper()
	status, body, err := h.covesXRPC(http.MethodGet, "social.coves.moderation.getSubjectState",
		url.Values{"subject": {"at://" + did}}, nil, token)
	if err != nil || status != http.StatusOK {
		t.Errorf("cleanup: getSubjectState of at://%s = HTTP %d (%s), err %v", did, status, truncate(body, 300), err)
		return
	}
	var out struct {
		State covesSubjectState `json:"state"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Errorf("cleanup: decode getSubjectState of at://%s: %v", did, err)
		return
	}
	status, body, err = h.covesXRPC(http.MethodPost, "social.coves.moderation.restoreCommunity", nil, map[string]any{
		"subject":         did,
		"expectedVersion": out.State.Version,
		"idempotencyKey":  idempotencyKey,
		"reason":          covesReasonModeratorDiscretion,
	}, token)
	switch {
	case err != nil:
		t.Errorf("cleanup: restoreCommunity of %s: %v", did, err)
	case status == http.StatusOK:
		awaitCovesTakedownLabel(context.Background(), t, db, did, true)
	case covesErrorName(body) == "StateConflict":
		// No active removal: the removal never happened.
	default:
		t.Errorf("cleanup: restoreCommunity of %s = HTTP %d (%s)", did, status, truncate(body, 300))
	}
}
