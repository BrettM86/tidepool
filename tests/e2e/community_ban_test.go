//go:build e2e

package e2e

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
)

// colBan is the community-repo ban record Coves enforces community bans from.
const colBan = "social.coves.moderation.ban"

// PRD moderation gaps, task 04: a Lemmy moderator's community ban is a
// social.coves.moderation.ban record in the community's repo, at the digest
// rkey of the banned account's DID, and the unban deletes it. Two accounts are
// banned without "remove content": a BRIDGED Lemmy person, minted by posting
// into the community, and a NATIVE Coves persona, known to Lemmy through the
// post the bridge federated for it.
func TestModeration_CommunityBanAndUnbanWriteBanRecords(t *testing.T) {
	h := newHarness(t)
	community, sub := setupSubscribedCommunity(t, h, "cban")

	cursor := cursorNow()
	l := h.newListener(t, cursor, colPostV2)

	// ── The bridged Lemmy person ──────────────────────────────────────────
	username := h.uniqueName(t, "banme")
	user := h.registerUser(t, username)
	bridgedPerson := h.admin.person(t, username)
	title := "Community-banned author's post " + h.suffix
	user.createPost(t, community.ID, title, "its author gets banned from this community")
	postEv := l.await("bridged postv2 create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "title")
		return e.Commit.Collection == colPostV2 && e.Commit.Operation == opCreate && got == title
	})
	bridgedDID := postEv.Did

	// ── The native Coves persona ──────────────────────────────────────────
	pds := &pdsClient{http: h.http}
	if err := pds.createSession(nativeHandle, nativePassword); err != nil {
		t.Fatalf("createSession as the native bootstrap account %q at %s: %v", nativeHandle, pdsURL(), err)
	}
	nativeRKey := syntax.NewTIDNow(0).String()
	nativeTitle := fmt.Sprintf("native author to ban %s %s", h.suffix, nativeRKey)
	nativePostURI, _, err := pds.createRecord(colPostV2, nativeRKey, map[string]any{
		"$type":     colPostV2,
		"community": sub.DID,
		"title":     nativeTitle,
		"content":   "a native post whose author gets banned",
		"createdAt": time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("createRecord native post: %v", err)
	}
	parsed, err := syntax.ParseATURI(nativePostURI)
	if err != nil {
		t.Fatalf("parse native post uri %q: %v", nativePostURI, err)
	}
	nativeDID := parsed.Authority().String()
	h.awaitLemmyPostBody(t, community.ID, nativeTitle, sub.DID, nativePostURI)
	nativePersonID := h.awaitLemmyPostCreatorID(t, community.ID, nativeTitle)

	cases := []struct {
		name     string
		personID int
		did      string
	}{
		{name: "bridged person", personID: bridgedPerson.ID, did: bridgedDID},
		{name: "native persona", personID: nativePersonID, did: nativeDID},
	}

	// ── A moderator bans each from the community, keeping their content ───
	const reason = "rule 2: spam"
	for _, c := range cases {
		h.admin.banFromCommunity(t, community.ID, c.personID, true, reason)
	}
	for _, c := range cases {
		rec := h.awaitRecordPresent(t, sub.DID, colBan, subjectRKey(c.did), c.name+" ban record after the ban")
		if got, _ := rec.Value["subject"].(string); got != c.did {
			t.Errorf("%s ban record subject = %q, want the banned account's DID %q", c.name, got, c.did)
		}
		if got, _ := rec.Value["reason"].(string); got != reason {
			t.Errorf("%s ban record reason = %q, want %q", c.name, got, reason)
		}
		if _, has := rec.Value["expiresAt"]; has {
			t.Errorf("%s ban record carries expiresAt %v; a permanent ban has none", c.name, rec.Value["expiresAt"])
		}
	}

	// ── The moderator unbans each ─────────────────────────────────────────
	for _, c := range cases {
		h.admin.banFromCommunity(t, community.ID, c.personID, false, "")
	}
	for _, c := range cases {
		h.awaitRecordGone(t, sub.DID, colBan, subjectRKey(c.did), c.name+" ban record after the unban")
	}
}

// banFromCommunity is a MODERATOR action: ban (or, with ban false, unban)
// personID from communityID without removing their content. Lemmy federates
// it to the community's followers as an Announce{Block} (Announce{Undo{Block}}
// for the unban).
func (c *lemmyClient) banFromCommunity(t *testing.T, communityID, personID int, ban bool, reason string) {
	t.Helper()
	body := map[string]any{
		"community_id": communityID, "person_id": personID, "ban": ban, "remove_data": false,
	}
	if reason != "" {
		body["reason"] = reason
	}
	if err := c.do(http.MethodPost, "/api/v3/community/ban_user", body, nil); err != nil {
		t.Fatalf("community-ban (ban=%v) person %d from community %d: %v", ban, personID, communityID, err)
	}
}

// awaitLemmyPostCreatorID polls the community's post list until the post
// titled title is there and returns its creator's Lemmy person id.
func (h *harness) awaitLemmyPostCreatorID(t *testing.T, communityID int, title string) int {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for {
		query := url.Values{"community_id": {fmt.Sprint(communityID)}, "sort": {"New"}, "limit": {"50"}}
		var out struct {
			Posts []struct {
				Post struct {
					Name string `json:"name"`
				} `json:"post"`
				Creator struct {
					ID int `json:"id"`
				} `json:"creator"`
			} `json:"posts"`
		}
		err := h.admin.do(http.MethodGet, "/api/v3/post/list?"+query.Encode(), nil, &out)
		if err == nil {
			for _, p := range out.Posts {
				if p.Post.Name == title && p.Creator.ID != 0 {
					return p.Creator.ID
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("post %q never listed with a creator in Lemmy community %d within %s (last error: %v)",
				title, communityID, eventTimeout, err)
		}
		time.Sleep(time.Second)
	}
}
