//go:build e2e

package e2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/bluesky-social/indigo/atproto/syntax"
)

// PRD moderation gaps, chunk 04 (B14): an instance-admin account ban of
// native-alice reaches Lemmy as the author's Delete of every delivered object
// and the unban as Undo{Delete}, except for an object that has its own
// instance removal: that one stays deleted until its own restore, whichever
// of the two lifts comes first. Her deactivating and reactivating on the stack
// PDS during the ban changes nothing on Lemmy: a status pause and the ban are
// separate sources, and an `#account active` never lifts the ban.
//
// Each order gets its own post and comment, so the first order's restores
// cannot satisfy the second order's expectations.
func TestNativeAccountBan_UnbanRestoresThePostAndLeavesTheRemovedCommentDeleted(t *testing.T) {
	h := newHarness(t)
	community, sub := setupSubscribedCommunity(t, h, "nban")
	h.awaitAppViewCommunityIndexed(t, sub.DID)
	db := openTidepoolDB(t)

	pds := &pdsClient{http: h.http}
	if err := pds.createSession(nativeHandle, nativePassword); err != nil {
		t.Fatalf("createSession as %q at %s: %v", nativeHandle, pdsURL(), err)
	}
	pdsAddress, err := url.Parse(pdsURL())
	if err != nil {
		t.Fatalf("parse pdsURL %q: %v", pdsURL(), err)
	}
	signIn := covesSignIn{
		AppViewURL:    covesAppViewURL(),
		DialOverrides: map[string]string{"localhost:3001": "127.0.0.1:" + pdsAddress.Port()},
	}
	adminToken := covesSignInAs(t, signIn, covesAdminHandle, covesAdminPassword)
	accountSubject := "at://" + pds.did

	orders := []struct {
		name       string
		unbanFirst bool
	}{
		{name: "unban then restoreContent", unbanFirst: true},
		{name: "restoreContent then unban", unbanFirst: false},
	}
	for index, order := range orders {
		ok := t.Run(order.name, func(t *testing.T) {
			key := fmt.Sprintf("b14-%d-%s", index, h.suffix)

			// ── native-alice's post and comment, delivered to lemmy ───────
			postRKey := syntax.NewTIDNow(0).String()
			title := fmt.Sprintf("native account ban %s %s", h.suffix, postRKey)
			postURI, postCID, err := pds.createRecord(colPostV2, postRKey, map[string]any{
				"$type":     colPostV2,
				"community": sub.DID,
				"title":     title,
				"content":   "a native post whose author the instance admin bans and unbans",
				"createdAt": time.Now().UTC().Format(time.RFC3339),
			})
			if err != nil {
				t.Fatalf("createRecord native post: %v", err)
			}
			h.awaitLemmyPostBody(t, community.ID, title, sub.DID, postURI)
			postID := h.awaitLemmyPostID(t, community.ID, title)

			postRef := map[string]any{"uri": postURI, "cid": postCID}
			createComment := func(content string) string {
				t.Helper()
				uri, _, err := pds.createRecord(colComment, syntax.NewTIDNow(0).String(), map[string]any{
					"$type":     colComment,
					"reply":     map[string]any{"root": postRef, "parent": postRef},
					"content":   content,
					"createdAt": time.Now().UTC().Format(time.RFC3339),
				})
				if err != nil {
					t.Fatalf("createRecord native comment %q: %v", content, err)
				}
				return uri
			}
			commentBody := fmt.Sprintf("the instance removed native comment %s %d", h.suffix, index)
			commentURI := createComment(commentBody)
			commentID := h.awaitLemmyCommentID(t, postID, commentBody)

			// ── The instance admin removes the comment ────────────────────
			commentIndexed := h.awaitCovesSubjectState(t, commentURI, adminToken, "the native comment indexed", func(s covesSubjectState) bool {
				return s.RecordState == "present" && s.CurrentSubject != nil
			})
			commentRemoved := h.covesModerationMutation(t, "social.coves.moderation.removeContent", adminToken, map[string]any{
				"subject":         commentIndexed.CurrentSubject,
				"expectedVersion": commentIndexed.Version,
				"idempotencyKey":  key + "-remove-comment",
				"reason":          covesReasonModeratorDiscretion,
			})
			if commentRemoved.LocalRemoval == nil || commentRemoved.LocalRemoval.ActionID == "" {
				t.Fatalf("removeContent of the native comment left no localRemoval action: %+v", commentRemoved)
			}
			h.admin.awaitContentFlag(t, "comment", commentID, "deleted", true, "after the comment's instance removal")

			// ── The instance admin bans native-alice ──────────────────────
			unbanned := h.awaitCovesSubjectState(t, accountSubject, adminToken, "native-alice with no account ban", func(s covesSubjectState) bool {
				return s.Version != "" && s.LocalRemoval == nil
			})
			banned := h.covesModerationMutation(t, "social.coves.moderation.banAccount", adminToken, map[string]any{
				"subject":         pds.did,
				"expectedVersion": unbanned.Version,
				"idempotencyKey":  key + "-ban",
				"reason":          covesReasonModeratorDiscretion,
			})
			t.Cleanup(func() { h.liftStandingAccountBan(t, pds.did, adminToken, key+"-cleanup-unban") })
			if banned.LocalRemoval == nil || banned.LocalRemoval.ActionID == "" {
				t.Fatalf("banAccount of native-alice left no localRemoval action: %+v", banned)
			}

			const whileBanned = "while native-alice's account ban stands"
			h.admin.awaitContentFlag(t, "post", postID, "deleted", true, whileBanned)
			h.admin.awaitContentFlag(t, "comment", commentID, "deleted", true, whileBanned)

			// ── She deactivates and reactivates during the ban ────────────
			// Tidepool advances last_account_seq only once it has handled an
			// #account event, so each wait proves that event was applied.
			beforeDeactivation := accountSeqOf(t, db, pds.did)
			if err := pds.deactivateAccount(); err != nil {
				t.Fatalf("deactivateAccount as %s: %v", nativeHandle, err)
			}
			reactivated := false
			t.Cleanup(func() {
				if reactivated {
					return
				}
				if err := pds.activateAccount(); err != nil {
					t.Errorf("cleanup: activateAccount as %s: %v", nativeHandle, err)
				}
			})
			afterDeactivation := awaitAccountSeqAfter(t, db, pds.did, beforeDeactivation, "the deactivation")
			if err := pds.activateAccount(); err != nil {
				t.Fatalf("activateAccount as %s: %v", nativeHandle, err)
			}
			reactivated = true
			awaitAccountSeqAfter(t, db, pds.did, afterDeactivation, "the reactivation")

			const afterReactivation = "after native-alice reactivated during the ban"
			h.admin.requireContentFlag(t, "post", postID, "deleted", true, afterReactivation)
			h.admin.requireContentFlag(t, "comment", commentID, "deleted", true, afterReactivation)

			unban := func() {
				t.Helper()
				standing := h.awaitCovesSubjectState(t, accountSubject, adminToken, "native-alice's standing account ban", func(s covesSubjectState) bool {
					return s.LocalRemoval != nil
				})
				h.covesModerationMutation(t, "social.coves.moderation.unbanAccount", adminToken, map[string]any{
					"subject":         pds.did,
					"expectedVersion": standing.Version,
					"idempotencyKey":  key + "-unban",
					"reason":          covesReasonModeratorDiscretion,
				})
			}
			restoreComment := func() {
				t.Helper()
				removed := h.awaitCovesSubjectState(t, commentURI, adminToken, "the native comment's standing removal", func(s covesSubjectState) bool {
					return s.LocalRemoval != nil && s.LocalRemoval.ActionID != ""
				})
				h.covesModerationMutation(t, "social.coves.moderation.restoreContent", adminToken, map[string]any{
					"actionId":        removed.LocalRemoval.ActionID,
					"expectedVersion": removed.Version,
					"idempotencyKey":  key + "-restore-comment",
					"reason":          covesReasonModeratorDiscretion,
					"reviewedSubject": commentIndexed.CurrentSubject,
				})
			}

			if order.unbanFirst {
				// ── Order (i): unban, then the comment's restore ──────────
				unban()
				const afterUnban = "after the unban, with the comment's removal standing"
				h.admin.awaitContentFlag(t, "post", postID, "deleted", false, afterUnban)
				// A control comment enqueued after the unban shares the
				// community's delivery order, so its arrival proves any
				// restore the unban enqueued for the comment was delivered.
				controlBody := fmt.Sprintf("a control comment after the unban %s %d", h.suffix, index)
				createComment(controlBody)
				h.awaitLemmyCommentID(t, postID, controlBody)
				h.admin.requireContentFlag(t, "comment", commentID, "deleted", true, afterUnban)

				restoreComment()
				h.admin.awaitContentFlag(t, "comment", commentID, "deleted", false, "after the comment's own restore")
				h.admin.requireContentFlag(t, "post", postID, "deleted", false, "after the comment's own restore")
				return
			}

			// ── Order (ii): the comment's restore, then the unban ─────────
			restoreComment()
			awaitCovesLabelNegated(t, db, commentURI)
			const afterRestore = "after the comment's restore, with the account ban standing"
			h.admin.requireContentFlag(t, "comment", commentID, "deleted", true, afterRestore)
			h.admin.requireContentFlag(t, "post", postID, "deleted", true, afterRestore)

			unban()
			const afterUnban = "after the unban"
			h.admin.awaitContentFlag(t, "post", postID, "deleted", false, afterUnban)
			h.admin.awaitContentFlag(t, "comment", commentID, "deleted", false, afterUnban)
		})
		if !ok {
			// The next order's ban needs native-alice unbanned and active,
			// which the failed order's cleanup only attempts.
			t.FailNow()
		}
	}
}

// requireContentFlag reads the post or comment once and fails unless its flag
// ("deleted" or "removed") is want. Use it only after a barrier that proves
// the change it rules out would already have landed.
func (c *lemmyClient) requireContentFlag(t *testing.T, kind string, id int, flag string, want bool, when string) {
	t.Helper()
	flags, err := c.contentFlags(kind, id)
	if err != nil {
		t.Fatalf("read %s %d on Lemmy %s %s: %v", kind, id, c.base(), when, err)
	}
	var got bool
	switch flag {
	case "deleted":
		got = flags.Deleted
	case "removed":
		got = flags.Removed
	default:
		t.Fatalf("unknown Lemmy content flag %q", flag)
	}
	if got != want {
		t.Errorf("%s %d on Lemmy %s has %s = %v %s, want %v", kind, id, c.base(), flag, got, when, want)
	}
}

// liftStandingAccountBan unbans did when a ban still stands, so a failed run
// does not leave the shared native account banned for the rest of the suite.
// Errors are reported, never fatal: it runs as a cleanup.
func (h *harness) liftStandingAccountBan(t *testing.T, did, token, idempotencyKey string) {
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
	if out.State.LocalRemoval == nil {
		return
	}
	status, body, err = h.covesXRPC(http.MethodPost, "social.coves.moderation.unbanAccount", nil, map[string]any{
		"subject":         did,
		"expectedVersion": out.State.Version,
		"idempotencyKey":  idempotencyKey,
		"reason":          covesReasonModeratorDiscretion,
	}, token)
	if err != nil || status != http.StatusOK {
		t.Errorf("cleanup: unbanAccount of %s = HTTP %d (%s), err %v", did, status, truncate(body, 300), err)
	}
}

// deactivateAccount deactivates the session's account on the PDS.
func (c *pdsClient) deactivateAccount() error {
	return c.do(http.MethodPost, "com.atproto.server.deactivateAccount", map[string]any{}, nil)
}

// activateAccount reactivates the session's deactivated account on the PDS.
func (c *pdsClient) activateAccount() error {
	return c.do(http.MethodPost, "com.atproto.server.activateAccount", nil, nil)
}

// accountSeqOf reads the seq of the last #account event Tidepool applied to
// the persona of did.
func accountSeqOf(t *testing.T, db *sql.DB, did string) int64 {
	t.Helper()
	var seq int64
	if err := db.QueryRowContext(t.Context(),
		`SELECT last_account_seq FROM ap_actors WHERE did = $1`, did).Scan(&seq); err != nil {
		t.Fatalf("read last_account_seq of %s: %v", did, err)
	}
	return seq
}

// awaitAccountSeqAfter polls until Tidepool has applied an #account event for
// did with a seq after the given one, and returns the new seq.
func awaitAccountSeqAfter(t *testing.T, db *sql.DB, did string, after int64, event string) int64 {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	var last string
	for {
		var seq int64
		err := db.QueryRowContext(t.Context(),
			`SELECT last_account_seq FROM ap_actors WHERE did = $1`, did).Scan(&seq)
		switch {
		case err != nil:
			last = "error: " + err.Error()
		case seq > after:
			return seq
		default:
			last = fmt.Sprintf("last_account_seq = %d", seq)
		}
		if time.Now().After(deadline) {
			t.Fatalf("Tidepool never applied the #account event for %s of %s (seq after %d) within %s (last: %s)",
				event, did, after, eventTimeout, last)
		}
		time.Sleep(time.Second)
	}
}

// awaitCovesLabelNegated polls until Tidepool has committed the negation of
// the !takedown label on uri. Appliers run in the label's commit transaction,
// so the stored negation proves Tidepool has applied it.
func awaitCovesLabelNegated(t *testing.T, db *sql.DB, uri string) {
	t.Helper()
	awaitCovesTakedownLabel(t.Context(), t, db, uri, true)
}

// awaitCovesTakedownLabel polls until the latest !takedown label Tidepool
// committed on uri is a negation (negated) or the label itself (!negated).
// ctx is the caller's: a cleanup passes one t.Context() has not cancelled.
func awaitCovesTakedownLabel(ctx context.Context, t *testing.T, db *sql.DB, uri string, negated bool) {
	t.Helper()
	want := "the label"
	if negated {
		want = "its negation"
	}
	deadline := time.Now().Add(eventTimeout)
	var last string
	for {
		var neg bool
		err := db.QueryRowContext(ctx, `
			SELECT neg FROM coves_labels
			WHERE uri = $1 AND val = '!takedown'
			ORDER BY reset_epoch DESC, seq DESC
			LIMIT 1`, uri).Scan(&neg)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			last = "no !takedown label stored"
		case err != nil:
			last = "error: " + err.Error()
		case neg == negated:
			return
		default:
			last = fmt.Sprintf("the latest !takedown label has neg = %t", neg)
		}
		if time.Now().After(deadline) {
			t.Fatalf("Tidepool never stored %s of the !takedown label on %s within %s (last: %s)",
				want, uri, eventTimeout, last)
		}
		time.Sleep(time.Second)
	}
}
