//go:build e2e

package e2e

import (
	"testing"
)

// Scenario 18 (task 19): the moderation loop against a REAL Lemmy.
//
// This is the flow the postv2 flip exists for. A community's visibility
// decisions are records in the COMMUNITY's repo — acceptance while a post is
// visible, removal once a moderator takes it down — and they replace each
// other in ONE commit so the firehose never carries a half-completed
// moderation action. The author's post is never touched by a removal: it is
// the author's record, in the author's repo, and a community removing it from
// its own surface says nothing about the author's ownership of it.
//
// The three transitions asserted here are the three Lemmy actually sends
// (verified on the wire against 0.19.20):
//
//   - mod remove WITH a reason        → Announce{Delete} with summary "spam"
//   - mod restore                     → Announce{Undo{Delete}}
//   - author deletes their own post   → Announce{Delete} with NO summary key
//
// Both the FIREHOSE (a consumer tailing the stream must be able to follow the
// state) and the END-STATE repo reads are asserted: they are two independent
// observations of the same commits, and a bridge that emitted the right
// events while converging to the wrong repo state would pass only one of them.
func TestModeration_RemoveRestoreAndSelfDelete(t *testing.T) {
	h := newHarness(t)
	community, sub := setupSubscribedCommunity(t, h, "mod")

	username := h.uniqueName(t, "molly")
	user := h.registerUser(t, username)

	cursor := cursorNow()
	l := h.newListener(t, cursor, colPostV2, colAcceptance, colRemoval)

	title := "Moderated post " + h.suffix
	post := user.createPost(t, community.ID, title, "will be removed and restored")

	postEv := l.await("postv2 create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "title")
		return e.Commit.Collection == colPostV2 && e.Commit.Operation == opCreate && got == title
	})
	postURI := postEv.atURI()
	digest := subjectRKey(postURI)

	acceptEv := l.await("acceptance create", func(e *jsEvent) bool {
		uri, _ := fieldOf(e.Commit.Record, "subject", "uri")
		return e.Commit.Collection == colAcceptance && e.Did == sub.DID && uri == postURI
	})
	if acceptEv.Commit.RKey != digest {
		t.Fatalf("acceptance rkey = %q, want the subject digest %q", acceptEv.Commit.RKey, digest)
	}

	// ── 1. Moderator removes the post, with a reason ───────────────────────
	const reason = "spam wave"
	h.admin.removePost(t, post.ID, true, reason)

	removalEv := l.await("removal record written", func(e *jsEvent) bool {
		uri, _ := fieldOf(e.Commit.Record, "subject", "uri")
		return e.Commit.Collection == colRemoval && e.Did == sub.DID && uri == postURI
	})
	if removalEv.Commit.RKey != digest {
		t.Errorf("removal rkey = %q, want the SAME subject digest the acceptance used (%q) — "+
			"one derivation per subject is what lets a restore find and delete it",
			removalEv.Commit.RKey, digest)
	}
	if got := recordField(t, removalEv.Commit.Record, "code"); got != "moderator-discretion" {
		t.Errorf("removal code = %q, want %q (Lemmy sends no machine-readable code, so the "+
			"open knownValues set's default applies)", got, "moderator-discretion")
	}
	if got := recordField(t, removalEv.Commit.Record, "reason"); got != reason {
		t.Errorf("removal reason = %q, want the moderator's text %q", got, reason)
	}
	if got := recordField(t, removalEv.Commit.Record, "subject", "cid"); got != postEv.Commit.CID {
		t.Errorf("removal subject.cid = %q, want the version present at removal time %q",
			got, postEv.Commit.CID)
	}

	// The acceptance is withdrawn in the same breath.
	l.await("acceptance deleted by the removal", func(e *jsEvent) bool {
		return e.Commit.Collection == colAcceptance && e.Commit.Operation == opDelete &&
			e.Did == sub.DID && e.Commit.RKey == digest
	})

	// End state: removed, not accepted — and the AUTHOR's post untouched.
	h.awaitRecordGone(t, sub.DID, colAcceptance, digest, "acceptance after removal")
	h.awaitRecordPresent(t, sub.DID, colRemoval, digest, "removal after removal")
	if _, found := h.bridgeGetRecord(t, postEv.Did, colPostV2, postEv.Commit.RKey); !found {
		t.Error("the author's postv2 was deleted by a community removal — removal is " +
			"community-scoped; the record belongs to the author")
	}

	// ── 2. Moderator restores it ───────────────────────────────────────────
	h.admin.removePost(t, post.ID, false, "")

	l.await("removal deleted by the restore", func(e *jsEvent) bool {
		return e.Commit.Collection == colRemoval && e.Commit.Operation == opDelete &&
			e.Did == sub.DID && e.Commit.RKey == digest
	})
	reacceptEv := l.await("fresh acceptance after the restore", func(e *jsEvent) bool {
		uri, _ := fieldOf(e.Commit.Record, "subject", "uri")
		return e.Commit.Collection == colAcceptance && e.Did == sub.DID &&
			e.Commit.RKey == digest && uri == postURI
	})
	if reacceptEv.Commit.Operation == opDelete {
		t.Fatalf("expected the restore to WRITE an acceptance, got %s", reacceptEv)
	}

	h.awaitRecordGone(t, sub.DID, colRemoval, digest, "removal after restore")
	restored := h.awaitRecordPresent(t, sub.DID, colAcceptance, digest, "acceptance after restore")
	// The fresh acceptance pins whatever version the post is on NOW.
	current, found := h.bridgeGetRecord(t, postEv.Did, colPostV2, postEv.Commit.RKey)
	if !found {
		t.Fatal("the post vanished across the restore")
	}
	if got := stringAt(t, restored.Value, "subject", "cid"); got != current.CID {
		t.Errorf("restored acceptance pins cid %q, want the post's current cid %q", got, current.CID)
	}

	// ── 3. The AUTHOR deletes their own post ───────────────────────────────
	// No summary on the wire, so this is not moderation: the post goes, its
	// acceptance goes with it, and NO removal record may be fabricated
	// against an author nobody moderated.
	user.deletePost(t, post.ID)

	l.await("postv2 delete after the author's own delete", func(e *jsEvent) bool {
		return e.Commit.Collection == colPostV2 && e.Commit.Operation == opDelete &&
			e.Did == postEv.Did && e.Commit.RKey == postEv.Commit.RKey
	})
	l.await("acceptance delete after the author's own delete", func(e *jsEvent) bool {
		return e.Commit.Collection == colAcceptance && e.Commit.Operation == opDelete &&
			e.Did == sub.DID && e.Commit.RKey == digest
	})

	h.awaitRecordGone(t, postEv.Did, colPostV2, postEv.Commit.RKey, "postv2 after self-delete")
	h.awaitRecordGone(t, sub.DID, colAcceptance, digest, "acceptance after self-delete")
	if rec, found := h.bridgeGetRecord(t, sub.DID, colRemoval, digest); found {
		t.Errorf("a self-delete wrote a removal record — author deletion is not moderation, and "+
			"this puts a moderation action in the community's log against someone who was never "+
			"moderated: %+v", rec.Value)
	}

	// Nothing else may arrive for this subject in the trailing window — in
	// particular no re-acceptance resurrecting a post the author deleted.
	for _, ev := range l.drain(negativeWindow) {
		if ev.Kind != kindCommit || ev.Commit == nil {
			continue
		}
		if ev.Commit.RKey == digest && ev.Commit.Operation != opDelete {
			t.Errorf("post-deletion write for the deleted subject: %s", ev)
		}
	}
}

// TestMixedEra_LegacyPostDispatch is DELIBERATELY SKIPPED at this tier, and
// the skip is the honest answer rather than a gap nobody noticed.
//
// The mixed-era contract — a pre-flip post keeps v1 semantics (record in the
// COMMUNITY's repo under the deprecated collection, no acceptance, no removal
// on moderation) while new posts flow postv2 — needs a LEGACY post to exist.
// Nothing in this stack can produce one: the flip is in the binary under
// test, so every post it materializes is a postv2, and legacy records exist
// only in repos written by a pre-flip build. The bridge exposes no
// record-seeding seam either (the admin API is communities, backfill, reemit,
// sweep-deleted, metrics) — by design.
//
// The two ways to manufacture one here would both be worse than skipping:
// writing rows straight into the bridge's postgres, or adding a test-only
// write endpoint. Each fabricates the state this scenario is supposed to
// OBSERVE, so what it would prove is that the fabrication matches the
// assertion — while adding a production seam that exists solely to be
// bypassed by tests.
//
// Era dispatch is covered where the seam is legitimate — the unit tier, which
// constructs a legacy record and mapping directly against the store:
//
//	internal/materialize/postv2_repin_test.go   TestLegacyPostStatsWritesNoAcceptance
//	internal/ingest/moderation_test.go          TestLegacyPostModRemovalKeepsV1Semantics
//	internal/votes/postv2_binding_test.go       (legacy binding by repo DID)
//
// If a future task ever needs true end-to-end mixed-era coverage, the honest
// route is a stack that boots a PRE-FLIP image, materializes a post, then
// upgrades the binary in place — real legacy records, real pipeline.
func TestMixedEra_LegacyPostDispatch(t *testing.T) {
	t.Skip("no sanctioned seam to seed a pre-flip legacy post in the e2e stack; " +
		"era dispatch is covered at the unit tier (see this test's comment)")
}
