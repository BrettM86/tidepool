//go:build e2e

package e2e

// Task 10 lifecycle scenarios: consent (#nobridge), Delete(Actor),
// unsubscribe, community profile update. All assert through the relay-fed
// Jetstream like the rest of the suite; fine-grained repo lifecycle STATE
// is asserted against the bridge's own sync surface (bigsky serves no
// getRepoStatus and filters tombstoned repos from listRepos). Since task 11
// the bridge emits #account{active:false, status:"deleted"} on
// Delete(Actor), so TestDeleteActor additionally asserts the frame on the
// bridge's OWN firehose and the repo's DISAPPEARANCE from the relay's
// listRepos (bigsky tombstones the repo and purges its data on that frame).
//
// Negative-assertion discipline: every "nothing bridged" claim is bounded
// by a positive control. Where the control post shares the suppressed
// post's community (the consent scenario), the bridge's queue serializes
// them (per-community ordering key), so the control arriving on the wire
// PROVES the suppressed event's processing already concluded — the
// trailing drain is a belt, not the proof. The unsubscribe scenario CANNOT
// share a community (its target is unfollowed) and rests on weaker bounds;
// see TestUnsubscribe_StopsBridging's doc.

import (
	"net/url"
	"strings"
	"testing"
	"time"
)

// Scenario 10: consent lifecycle for #nobridge, both directions.
//
//  1. A never-bridged user with #nobridge in their bio posts → nothing
//     materializes and no DID is minted (bridgeNewActor refuses before
//     minting; no bridged_actors row exists, so the predicted handle does
//     not resolve).
//  2. The marker is removed (bio edit is Lemmy-local — 0.19.x federates no
//     Update{Person}) → the next post triggers a FRESH actor fetch (no
//     stored row means no TTL gate) → bridging begins: profile + post.
//  3. The now-bridged actor re-adds the marker → after PROFILE_REFRESH_TTL
//     the next activity's actor re-fetch discovers it → every existing
//     record is scrubbed with delete commits on the firehose, the trigger
//     post never bridges, and the repo stays ACTIVE (reversible posture —
//     nobridge is not the terminal Deleted).
func TestConsent_NobridgeLifecycle(t *testing.T) {
	h := newHarness(t)
	community, sub := setupSubscribedCommunity(t, h, "cns")

	markedName := h.uniqueName(t, "mallory")
	marked := h.registerUser(t, markedName)
	marked.saveBio(t, "opted out of bridging #nobridge")
	controlName := h.uniqueName(t, "norma")
	control := h.registerUser(t, controlName)

	cursor := cursorNow()
	// Unfiltered: a consent miss on ANY collection must be visible.
	l := h.newListener(t, cursor)

	// Phase 1: suppression. The control post is created after the marked
	// post in the SAME community, so its arrival proves the marked post's
	// processing already finished (same queue ordering key).
	suppressedTitle := "Suppressed " + h.suffix
	marked.createPost(t, community.ID, suppressedTitle, "must never bridge")
	control1Title := "Control one " + h.suffix
	control.createPost(t, community.ID, control1Title, "positive control")

	l.await("phase-1 control post", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "title")
		return e.Commit.Collection == colPost && e.Did == sub.DID && got == control1Title
	})
	assertNoMarkedOutput := func(phase, forbiddenTitle string) {
		t.Helper()
		for _, ev := range l.drain(negativeWindow) {
			if ev.Kind != kindCommit || ev.Commit == nil {
				continue
			}
			if got, _ := fieldOf(ev.Commit.Record, "title"); got == forbiddenTitle {
				t.Errorf("%s: suppressed post %q reached the firehose: %s", phase, forbiddenTitle, ev)
			}
			if got, _ := fieldOf(ev.Commit.Record, "displayName"); got == markedName && ev.Commit.Operation != opDelete {
				t.Errorf("%s: opted-out actor's profile reached the firehose: %s", phase, ev)
			}
		}
	}
	assertNoMarkedOutput("phase 1 (never-bridged suppression)", suppressedTitle)

	// No DID was minted: the predicted bridge handle must not exist.
	handle := bridgedHandle(markedName)
	if did, res := h.bridgeResolveHandle(t, handle); res.status != 400 || res.errCode != "HandleNotFound" {
		t.Fatalf("phase 1: handle %s resolves (status %d, err %q, did %q) — a DID was minted for an opted-out actor",
			handle, res.status, res.errCode, did)
	}

	// Phase 2: marker removed → bridging resumes on the next post. No TTL
	// wait needed: suppression stored NO row, so this is a fresh actor.
	marked.saveBio(t, "changed my mind, bridge me")
	resumeTitle := "Resumed " + h.suffix
	marked.createPost(t, community.ID, resumeTitle, "bridged after opt-in")

	profileEv := l.await("marked user's actor.profile after opt-in", func(e *jsEvent) bool {
		name, _ := fieldOf(e.Commit.Record, "displayName")
		return e.Commit.Collection == colActorProfile && name == markedName &&
			e.Commit.Operation == opCreate
	})
	markedDID := profileEv.Did
	resumeEv := l.await("resumed post create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "title")
		return e.Commit.Collection == colPost && e.Did == sub.DID &&
			e.Commit.Operation == opCreate && got == resumeTitle
	})
	if did, res := h.bridgeResolveHandle(t, handle); res.status != 200 || did != markedDID {
		t.Fatalf("phase 2: handle %s should resolve to %s after opt-in (status %d, did %q)",
			handle, markedDID, res.status, did)
	}

	// Phase 3: the bridged actor opts out. The bio edit does not federate;
	// the bridge discovers the marker on the next activity's actor
	// re-fetch, gated by PROFILE_REFRESH_TTL (2s in this stack) — wait out
	// the TTL relative to the LAST profile-sync stamp (at most the resume
	// post's materialization, which we have already observed).
	marked.saveBio(t, "going dark #nobridge")
	time.Sleep(profileStaleWait)
	scrubTriggerTitle := "Scrub trigger " + h.suffix
	marked.createPost(t, community.ID, scrubTriggerTitle, "discovers the marker, must not bridge")

	// The scrub: delete commits for the resumed post (community repo) and
	// the actor profile (author repo), each on the exact rkey observed at
	// create time.
	l.await("scrub delete of the resumed post", func(e *jsEvent) bool {
		return e.Commit.Collection == colPost && e.Commit.Operation == opDelete &&
			e.Did == sub.DID && e.Commit.RKey == resumeEv.Commit.RKey
	})
	l.await("scrub delete of the actor profile", func(e *jsEvent) bool {
		return e.Commit.Collection == colActorProfile && e.Commit.Operation == opDelete &&
			e.Did == markedDID && e.Commit.RKey == rkeySelf
	})

	// Bounded negative for the trigger post, again anchored by a same-
	// community control.
	control2Title := "Control two " + h.suffix
	control.createPost(t, community.ID, control2Title, "positive control after scrub")
	l.await("phase-3 control post", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "title")
		return e.Commit.Collection == colPost && e.Did == sub.DID && got == control2Title
	})
	assertNoMarkedOutput("phase 3 (bridged actor opted out)", scrubTriggerTitle)

	// Reversible posture: nobridge scrubs records but does NOT tombstone
	// the repo — it stays active on the bridge's sync surface (only the
	// terminal Deleted deactivates it; scenario TestDeleteActor pins that).
	status, res, err := h.bridgeGetRepoStatus(markedDID)
	if err != nil || res.status != 200 {
		t.Fatalf("getRepoStatus(%s): status %d, err %v", markedDID, res.status, err)
	}
	if !status.Active {
		t.Errorf("nobridge repo %s reports active=false (status %q) — nobridge must stay reversible, not tombstone", markedDID, status.Status)
	}
}

// Scenario 11: Delete(Actor). Lemmy account deletion federates ONE
// Delete{Person} (with removeData; no per-object deletes) → the bridge
// scrubs every record the actor authored — post in the COMMUNITY repo,
// comment + profile in the AUTHOR repo — with delete commits on the
// relay-fed firehose, then terminally tombstones the repo: getRepoStatus
// and listRepos report active:false, the handle stops resolving, and the
// content endpoints refuse with RepoDeactivated.
//
// This scenario end-to-ends TWO bridge affordances it originally exposed as
// missing (both observed live against Lemmy 0.19.19):
//   - Lemmy delivers send-to-all-instances activities (this Delete!) only
//     to peers with a stored Site actor row — the bridge serves an instance
//     actor at its origin apex so that row exists (ingest inbox.go Routes,
//     ap.ServiceActor.InstanceDocumentJSON);
//   - the Delete arrives signed by an actor its origin already serves as
//     410 Gone, so the inbox accepts it on independently-confirmed origin
//     tombstone evidence (inbox.go tombstonedSelfDelete).
//
// Task 11 added the #account{active:false, status:"deleted"} firehose frame
// (emitted after the scrub delete-commits): this scenario asserts the frame
// on the bridge's own subscribeRepos AND its relay-side consequence — bigsky
// consumes it, marks the repo tombstoned, and stops listing it.
func TestDeleteActor_ScrubsAndTombstones(t *testing.T) {
	h := newHarness(t)
	community, sub := setupSubscribedCommunity(t, h, "del")

	username := h.uniqueName(t, "walt")
	user := h.registerUser(t, username)

	cursor := cursorNow()
	// colCommunityProfile is included ONLY for the over-scrub guard below:
	// the community's own profile (rkey `self`) must SURVIVE its member's
	// deletion, and a delete of it could not be seen through a narrower
	// filter.
	l := h.newListener(t, cursor, colActorProfile, colPost, colComment, colCommunityProfile)

	title := "Doomed post " + h.suffix
	post := user.createPost(t, community.ID, title, "author will self-delete")

	profileEv := l.await("author actor.profile", func(e *jsEvent) bool {
		name, _ := fieldOf(e.Commit.Record, "displayName")
		return e.Commit.Collection == colActorProfile && name == username
	})
	authorDID := profileEv.Did
	postEv := l.await("post create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "title")
		return e.Commit.Collection == colPost && e.Did == sub.DID &&
			e.Commit.Operation == opCreate && got == title
	})
	user.createComment(t, post.ID, 0, "doomed comment")
	commentEv := l.await("comment create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "reply", "parent", "uri")
		return e.Commit.Collection == colComment && e.Commit.Operation == opCreate &&
			got == postEv.atURI()
	})
	if commentEv.Did != authorDID {
		t.Fatalf("comment repo %s != author repo %s", commentEv.Did, authorDID)
	}

	// Pre-delete: the handle resolves and the repo is active/served.
	handle := bridgedHandle(username)
	if did, res := h.bridgeResolveHandle(t, handle); res.status != 200 || did != authorDID {
		t.Fatalf("pre-delete: handle %s does not resolve to %s (status %d, did %q)", handle, authorDID, res.status, did)
	}

	// Subscribe to the bridge's OWN firehose (live tail) BEFORE the delete
	// so the #account frame cannot be missed.
	fhConn := h.dialBridgeFirehose(t)

	user.deleteAccount(t)

	// Scrub delete-commits for all three records, on their observed rkeys.
	l.await("delete of the post (community repo)", func(e *jsEvent) bool {
		return e.Commit.Collection == colPost && e.Commit.Operation == opDelete &&
			e.Did == sub.DID && e.Commit.RKey == postEv.Commit.RKey
	})
	l.await("delete of the comment (author repo)", func(e *jsEvent) bool {
		return e.Commit.Collection == colComment && e.Commit.Operation == opDelete &&
			e.Did == authorDID && e.Commit.RKey == commentEv.Commit.RKey
	})
	l.await("delete of the actor profile", func(e *jsEvent) bool {
		return e.Commit.Collection == colActorProfile && e.Commit.Operation == opDelete &&
			e.Did == authorDID && e.Commit.RKey == rkeySelf
	})

	// The #account frame on the bridge's own firehose: active:false,
	// status "deleted", emitted AFTER the scrub commits (the purge signal a
	// per-repo-ordered consumer sees last). This is the exact CBOR frame a
	// relay consumes.
	account := readBridgeAccountFrame(t, fhConn, authorDID, eventTimeout)
	if account.Active {
		t.Errorf("#account frame for %s reports active:true, want false", authorDID)
	}
	if account.Status == nil || *account.Status != "deleted" {
		t.Errorf("#account frame status = %v, want \"deleted\"", account.Status)
	}

	// Over-scrub guard: the scrub must delete EXACTLY the three records
	// awaited above. A bounded drain asserts no OTHER delete op appears on
	// either affected repo — above all the community.profile rkey `self`
	// (the community must survive its member's deletion), but also any
	// double-delete replay of the three. Without this window an over-scrub
	// would pass unnoticed: the awaits only prove the expected deletes
	// happened, not that nothing else died.
	for _, ev := range l.drain(negativeWindow) {
		if ev.Kind != kindCommit || ev.Commit == nil || ev.Commit.Operation != opDelete {
			continue
		}
		if ev.Did == sub.DID || ev.Did == authorDID {
			t.Errorf("over-scrub: unexpected extra delete after Delete(Actor): %s", ev)
		}
	}

	// Terminal tombstone on the bridge's sync surface. The consent flip is
	// written right after the scrub whose commits we just observed — poll
	// briefly rather than assume the write beat our read.
	deadline := time.Now().Add(eventTimeout)
	for {
		status, res, err := h.bridgeGetRepoStatus(authorDID)
		if err == nil && res.status == 200 && !status.Active {
			if status.Status != "deleted" {
				t.Errorf("getRepoStatus(%s).status = %q, want deleted", authorDID, status.Status)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("repo %s never deactivated after Delete(Actor): last status %+v (http %d, err %v)", authorDID, status, res.status, err)
		}
		time.Sleep(time.Second)
	}

	// The bridge's OWN listRepos still lists it — flagged inactive.
	repos := h.bridgeListRepos(t)
	entry, ok := repos[authorDID]
	if !ok {
		t.Errorf("bridge listRepos no longer lists tombstoned repo %s (it must, with active:false)", authorDID)
	} else if entry.Active || entry.Status != "deleted" {
		t.Errorf("bridge listRepos entry for %s = %+v, want active:false status:deleted", authorDID, entry)
	}

	// The handle stops resolving: tombstoned identity is frozen.
	if did, res := h.bridgeResolveHandle(t, handle); res.status != 400 || res.errCode != "HandleNotFound" {
		t.Errorf("post-delete: handle %s should refuse with 400 HandleNotFound (status %d, err %q, did %q)",
			handle, res.status, res.errCode, did)
	}

	// Content endpoints refuse the dead repo.
	for _, ep := range []string{"com.atproto.sync.getRepo", "com.atproto.sync.getLatestCommit"} {
		res, err := h.bridgeXRPC("/xrpc/"+ep, url.Values{"did": {authorDID}})
		if err != nil {
			t.Fatalf("%s(%s): %v", ep, authorDID, err)
		}
		if res.status != 400 || res.errCode != "RepoDeactivated" {
			t.Errorf("%s(%s) = status %d err %q, want 400 RepoDeactivated", ep, authorDID, res.status, res.errCode)
		}
	}

	// RELAY-SIDE CONSEQUENCE of the #account frame (flipped from the task-09
	// pin): bigsky consumes it, re-resolves the DID doc to confirm the
	// bridge is authoritative, marks the account tombstoned, and drops the
	// repo from its listRepos (source-verified against the pinned bigsky:
	// status "deleted" → tombstoned=true + carstore purge; listRepos filters
	// NOT tombstoned). Polled: the relay processes the frame asynchronously.
	relayDeadline := time.Now().Add(eventTimeout)
	for {
		relayRepos, err := h.relayListRepos()
		if err != nil {
			t.Fatalf("relay listRepos: %v", err)
		}
		if _, ok := relayRepos[authorDID]; !ok {
			break
		}
		if time.Now().After(relayDeadline) {
			t.Fatalf("relay still lists tombstoned repo %s after the #account frame — did the relay reject it?", authorDID)
		}
		time.Sleep(2 * time.Second)
	}
}

// Scenario 12: unsubscribe. DELETE /admin/communities sends Undo{Follow}
// and records follow_state=none; a new Lemmy post in that community then
// produces NO bridge output (Lemmy stops announcing to the lost follower,
// and the bridge drops announces from unfollowed communities anyway), while
// a still-subscribed community keeps flowing in the same window.
//
// Unlike the consent scenario, the control here is NECESSARILY a different
// community — the suppressed post's community is the one we unfollowed —
// so the control does NOT share the suppressed post's queue ordering key
// and its arrival proves nothing about the dead post's processing. The
// negative assertion instead rests on three weaker, stacked bounds:
// Lemmy's per-instance federation worker delivers sequentially (the
// control post's Announce leaving Lemmy implies the dead post's delivery
// window already passed — if Lemmy sent it at all to a lost follower);
// the stream-derived preEv.TimeUs floor (nothing on the unsubscribed
// community's repo strictly newer than its last legitimate event); and
// the bounded trailing drain.
func TestUnsubscribe_StopsBridging(t *testing.T) {
	h := newHarness(t)

	unsCommunity, unsSub := setupSubscribedCommunity(t, h, "uns")
	ctlCommunity, ctlSub := setupSubscribedCommunity(t, h, "ctl")

	user := h.registerUser(t, h.uniqueName(t, "ursula"))

	// Prove the doomed community flows BEFORE unsubscribing (otherwise the
	// negative below would pass vacuously on a broken subscription).
	preCursor := cursorNow()
	pre := h.newListener(t, preCursor, colPost)
	preTitle := "Pre-unsubscribe " + h.suffix
	user.createPost(t, unsCommunity.ID, preTitle, "flows while subscribed")
	preEv := pre.await("pre-unsubscribe post", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "title")
		return e.Commit.Collection == colPost && e.Did == unsSub.DID && got == preTitle
	})
	pre.close()

	handle := "!" + unsCommunity.Name + "@lemmy"
	h.unsubscribeCommunity(t, handle) // asserts the response says follow_state none
	// The admin list reports only accepted/pending communities (ingest
	// follow.go handleList), so an unfollowed one must DISAPPEAR from it.
	if state, ok := h.findCommunity(t, handle, unsSub.Community); ok {
		t.Fatalf("community %s still in the admin list after unsubscribe: %+v (the list reports accepted/pending only)", handle, state)
	}

	// Fresh listener for the negative window, cursored ON the pre-post
	// event itself — STREAM-derived, never the host clock. This pinned
	// Jetstream's cursor semantics (measured live with a ws probe against
	// the running stack): a cursor at or before the newest stored event
	// replays precisely from it (inclusive, µs-exact), a cursor beyond
	// server-now live-tails, but a cursor BETWEEN the newest event and
	// now — which is what any "cursor = now" subscription hits on a quiet
	// stream — replays the ENTIRE store. An existing event's time_us is
	// therefore the only cursor that reliably bounds this window; the one
	// known replayed event (the pre-post itself) is excluded by timestamp
	// in the sweep below.
	l := h.newListener(t, preEv.TimeUs)

	deadTitle := "Dead post " + h.suffix
	user.createPost(t, unsCommunity.ID, deadTitle, "must not bridge")
	liveTitle := "Live post " + h.suffix
	user.createPost(t, ctlCommunity.ID, liveTitle, "control keeps flowing")

	l.await("control community post", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "title")
		return e.Commit.Collection == colPost && e.Did == ctlSub.DID &&
			e.Commit.Operation == opCreate && got == liveTitle
	})

	// Bounded negative: nothing on the unsubscribed community's repo
	// STRICTLY NEWER than the pre-post (the newest thing it legitimately
	// emitted), and the dead post's title nowhere (belt against it landing
	// in a wrong repo).
	for _, ev := range l.drain(negativeWindow) {
		if ev.Kind != kindCommit || ev.Commit == nil {
			continue
		}
		if ev.Did == unsSub.DID && ev.TimeUs > preEv.TimeUs {
			// A trailing bridgedStats UPDATE on an ALREADY-bridged post is the
			// vote-stats refresher settling seeded counts (SEED_COUNTS_FROM_API
			// is on), not new content bridged after the Undo{Follow} —
			// unsubscribe stops NEW content, it does not roll back stats the
			// aggregates already hold. Tolerated, but ONLY as a pure stats
			// settle: carry-forward ships the whole record, so a genuine content
			// change could hide inside a stats-shaped update. Pin the pre-post's
			// title AND content unchanged so a real edit cannot pass as one.
			if isBridgedStatsUpdate(ev) {
				if got, _ := fieldOf(ev.Commit.Record, "title"); got != preTitle {
					t.Errorf("stats-shaped update on unsubscribed repo %s changed the title to %q (want the pre-post %q): %s",
						unsSub.DID, got, preTitle, ev)
				}
				if got, _ := fieldOf(ev.Commit.Record, "content"); got != "flows while subscribed" {
					t.Errorf("stats-shaped update on unsubscribed repo %s changed the content to %q (want the pre-post body): %s",
						unsSub.DID, got, ev)
				}
			} else {
				t.Errorf("unsubscribed community repo %s emitted after Undo{Follow}: %s", unsSub.DID, ev)
			}
		}
		if got, _ := fieldOf(ev.Commit.Record, "title"); got == deadTitle {
			t.Errorf("post in unsubscribed community reached the firehose: %s", ev)
		}
	}
}

// Scenario 13: community profile update. Editing a community in Lemmy
// (title + sidebar) federates Announce{Update{Group}} → the bridge
// re-materializes community.profile as an UPDATE on rkey `self` with the
// new displayName/description.
func TestCommunityUpdate_ProfileUpdateEvent(t *testing.T) {
	h := newHarness(t)

	cursor := cursorNow()
	l := h.newListener(t, cursor, colCommunityProfile)

	community, sub := setupSubscribedCommunity(t, h, "cup")
	createEv := l.await("community.profile create", func(e *jsEvent) bool {
		return e.Did == sub.DID && e.Commit.Collection == colCommunityProfile &&
			e.Commit.Operation == opCreate
	})

	newTitle := "Renamed " + h.suffix
	newDesc := "fresh sidebar " + h.suffix
	h.admin.editCommunity(t, community.ID, newTitle, newDesc)

	updateEv := l.await("community.profile update", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "displayName")
		return e.Did == sub.DID && e.Commit.Collection == colCommunityProfile &&
			e.Commit.Operation == opUpdate && got == newTitle
	})
	if updateEv.Commit.RKey != rkeySelf {
		t.Errorf("community.profile update rkey = %q, want %q", updateEv.Commit.RKey, rkeySelf)
	}
	if updateEv.Commit.CID == createEv.Commit.CID {
		t.Error("update event carries the create's cid — no new commit?")
	}
	// name (the immutable community slug) survives; description carries the
	// new sidebar text plus the bridge's provenance line.
	if got := recordField(t, updateEv.Commit.Record, "name"); got != community.Name {
		t.Errorf("community.profile name = %q, want the slug %q (renames change displayName, not name)", got, community.Name)
	}
	desc := recordField(t, updateEv.Commit.Record, "description")
	if !strings.Contains(desc, newDesc) {
		t.Errorf("community.profile description %q does not contain the new sidebar %q", desc, newDesc)
	}
	if !strings.Contains(desc, "bridged from") {
		t.Errorf("community.profile description %q lost the provenance line", desc)
	}
}
