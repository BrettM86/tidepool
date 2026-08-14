//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// setupSubscribedCommunity creates a fresh Lemmy community and subscribes
// the bridge to it via the admin API (WebFinger handle form, exercising
// webfinger against a real Lemmy — a task-06 deferred gap). It returns both
// sides of the mapping: the Lemmy community (for further API writes) and
// the bridge's view (DID, group IRI).
func setupSubscribedCommunity(t *testing.T, h *harness, prefix string) (lemmyCommunity, adminCommunity) {
	t.Helper()
	name := h.uniqueName(t, prefix)
	community := h.admin.createCommunity(t, name, "E2E "+name)
	sub := h.subscribeCommunity(t, "!"+name+"@lemmy")
	if sub.DID == "" {
		t.Fatalf("subscribed community %s has no DID", name)
	}
	return community, sub
}

// Scenario 1: Subscribe !testing@lemmy → community.profile appears on the
// firehose and validates against the Coves lexicon (as every create/update
// consumed by the suite does, via the listener's centralized vetting).
func TestSubscribe_CommunityProfileOnFirehose(t *testing.T) {
	h := newHarness(t)

	cursor := cursorNow()
	l := h.newListener(t, cursor, colCommunityProfile)

	community, sub := setupSubscribedCommunity(t, h, "sub")

	ev := l.await("community.profile create", func(e *jsEvent) bool {
		return e.Did == sub.DID && e.Commit.Collection == colCommunityProfile &&
			e.Commit.Operation == opCreate
	})
	if ev.Commit.RKey != rkeySelf {
		t.Errorf("community.profile rkey = %q, want %q", ev.Commit.RKey, rkeySelf)
	}
	if got := recordField(t, ev.Commit.Record, "name"); got != community.Name {
		t.Errorf("community.profile name = %q, want %q", got, community.Name)
	}
}

// Scenario 2: a Lemmy user shares a link → actor.profile AND community.post
// in the COMMUNITY's repo with author = the user's DID (PLAN.md locked
// decision 3) — and the shared url crosses the wire as an embed.external
// whose uri survives byte-identical (the classic CBOR→JSON breakage point).
//
// Ordering caveat (task 09 discovery): the bridge emits the author's
// profile strictly before the post on its OWN firehose, but the two records
// live in different repos (author vs community) and bigsky indexes its
// inbound stream with a parallel scheduler keyed by repo DID (indigo
// events/schedulers/parallel, 100 workers) — per-repo order survives the
// relay, CROSS-repo order does not. In practice the post usually overtakes
// the profile: the community DID is already known to the relay while the
// author's first-ever event costs a PLC resolution + handle verification.
// So this scenario asserts presence + the author linkage, not arrival
// order — and the Coves AppView, which consumes through relay
// infrastructure too, cannot rely on profile-before-post either
// (FOLLOWUPS.md "Relay pipeline").
func TestPost_ActorProfileThenPost(t *testing.T) {
	h := newHarness(t)
	community, sub := setupSubscribedCommunity(t, h, "post")

	username := h.uniqueName(t, "bob")
	user := h.registerUser(t, username)

	cursor := cursorNow()
	l := h.newListener(t, cursor, colActorProfile, colPostV2, colAcceptance)

	title := "Hello from " + username
	// A link post. The url stays on the compose network (LOCAL-ONLY): Lemmy
	// fetches it for opengraph metadata; the bridge itself never fetches the
	// link — it copies the AP fields into the embed.
	linkURL := "http://lemmy/?e2e=" + h.suffix
	post := user.createLinkPost(t, community.ID, title, "first bridged post", linkURL)
	t.Logf("created lemmy post %d (%s)", post.ID, post.APID)

	// Await both events in either order (await buffers non-matches, so a
	// post that overtook the profile through the relay is not lost).
	profileEv := l.await("actor.profile for "+username, func(e *jsEvent) bool {
		name, _ := fieldOf(e.Commit.Record, "displayName")
		return e.Commit.Collection == colActorProfile && name == username
	})
	if profileEv.Commit.RKey != rkeySelf {
		t.Errorf("actor.profile rkey = %q, want %q", profileEv.Commit.RKey, rkeySelf)
	}

	postEv := l.await("community.postv2 create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "title")
		return e.Commit.Collection == colPostV2 &&
			e.Commit.Operation == opCreate && got == title
	})

	// Authorship IS the repo (PLAN.md decision 20): the post is committed
	// into the AUTHOR's repo and carries no in-record author field at all.
	if postEv.Did != profileEv.Did {
		t.Errorf("postv2 landed in repo %q, want the AUTHOR's repo %q", postEv.Did, profileEv.Did)
	}
	if _, ok := fieldOf(postEv.Commit.Record, "author"); ok {
		t.Errorf("postv2 carries an author field — authorship is the repo it lives in: %s",
			truncate(postEv.Commit.Record, 300))
	}
	if got := recordField(t, postEv.Commit.Record, "community"); got != sub.DID {
		t.Errorf("post community = %q, want the community's DID %q", got, sub.DID)
	}
	if got := recordField(t, postEv.Commit.Record, "embed", "external", "uri"); got != linkURL {
		t.Errorf("post embed.external.uri = %q, want the shared link %q", got, linkURL)
	}

	// Bridged provenance: who wrote it upstream, and where it came from.
	// The lexicon leaves both shapes open, so THIS is the bridge's published
	// convention and the suite is where it is pinned.
	if got := recordField(t, postEv.Commit.Record, "originalAuthor", "apId"); !strings.Contains(got, username) {
		t.Errorf("originalAuthor.apId = %q, want the origin actor IRI for %s", got, username)
	}
	if got := recordField(t, postEv.Commit.Record, "originalAuthor", "instance"); got != "lemmy" {
		t.Errorf("originalAuthor.instance = %q, want %q", got, "lemmy")
	}
	if got := recordField(t, postEv.Commit.Record, "originalAuthor", "handle"); got != username {
		t.Errorf("originalAuthor.handle = %q, want the ORIGIN username %q (not the bridged handle)", got, username)
	}
	if got := recordField(t, postEv.Commit.Record, "federatedFrom", "platform"); got != "lemmy" {
		t.Errorf("federatedFrom.platform = %q, want %q", got, "lemmy")
	}
	if got := recordField(t, postEv.Commit.Record, "federatedFrom", "instance"); got != "lemmy" {
		t.Errorf("federatedFrom.instance = %q, want %q", got, "lemmy")
	}

	// The community's acceptance is what makes the post VISIBLE in it:
	// Coves renders community surfaces from acceptance records alone, so a
	// postv2 without one is a post nobody can see. It rides the firehose as
	// its own commit in the COMMUNITY's repo, at the subject digest rkey.
	acceptEv := l.await("community.acceptance create", func(e *jsEvent) bool {
		uri, _ := fieldOf(e.Commit.Record, "subject", "uri")
		return e.Commit.Collection == colAcceptance && uri == postEv.atURI()
	})
	if acceptEv.Did != sub.DID {
		t.Errorf("acceptance landed in repo %q, want the COMMUNITY's repo %q", acceptEv.Did, sub.DID)
	}
	if want := subjectRKey(postEv.atURI()); acceptEv.Commit.RKey != want {
		t.Errorf("acceptance rkey = %q, want the subject digest %q — Coves derives the same key, "+
			"and a mismatch silently forks acceptance identity between the two engines",
			acceptEv.Commit.RKey, want)
	}
	if got := recordField(t, acceptEv.Commit.Record, "subject", "cid"); got != postEv.Commit.CID {
		t.Errorf("acceptance subject.cid = %q, want the post's cid %q", got, postEv.Commit.CID)
	}
	h.awaitAcceptanceConverged(t, sub.DID, postEv.Did, postEv.Commit.RKey)
}

// Scenario 3: comment + nested reply → comment records in the AUTHOR's repo
// (PLAN.md locked decision 3: comments live in the bridged user's repo,
// pinned against the DID of the author's actor.profile) whose
// reply.root/reply.parent strongRefs resolve to the exact uri+cid of the
// earlier firehose events.
func TestComments_StrongRefsResolve(t *testing.T) {
	h := newHarness(t)
	community, sub := setupSubscribedCommunity(t, h, "cmt")

	username := h.uniqueName(t, "carol")
	user := h.registerUser(t, username)

	cursor := cursorNow()
	l := h.newListener(t, cursor, colActorProfile, colPostV2, colComment)

	title := "Comment thread " + h.suffix
	post := user.createPost(t, community.ID, title, "root post")

	// The author's profile pins the author's DID. Arrival order vs the post
	// is relay-dependent (see scenario 2) — await's buffering makes these
	// sequential awaits order-tolerant.
	profileEv := l.await("author actor.profile", func(e *jsEvent) bool {
		name, _ := fieldOf(e.Commit.Record, "displayName")
		return e.Commit.Collection == colActorProfile && name == username
	})
	authorDID := profileEv.Did

	postEv := l.await("post create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "title")
		return e.Commit.Collection == colPostV2 && e.Did == authorDID && got == title
	})
	// Post and comments now share one repo (the author's), so the thread's
	// community linkage lives in the record rather than in the repo DID.
	if got := recordField(t, postEv.Commit.Record, "community"); got != sub.DID {
		t.Errorf("post community = %q, want %q", got, sub.DID)
	}
	postURI, postCID := postEv.atURI(), postEv.Commit.CID

	comment := user.createComment(t, post.ID, 0, "top-level comment")
	commentEv := l.await("top-level comment create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "reply", "parent", "uri")
		return e.Commit.Collection == colComment && e.Commit.Operation == opCreate &&
			got == postURI
	})
	if commentEv.Did != authorDID {
		t.Errorf("comment emitted from repo %s, want the author's repo %s (comments live in the bridged user's repo)",
			commentEv.Did, authorDID)
	}

	// Top-level comment: parent and root are both the post, cid included.
	for _, ref := range []string{"parent", "root"} {
		if got := recordField(t, commentEv.Commit.Record, "reply", ref, "uri"); got != postURI {
			t.Errorf("comment reply.%s.uri = %q, want %q", ref, got, postURI)
		}
		if got := recordField(t, commentEv.Commit.Record, "reply", ref, "cid"); got != postCID {
			t.Errorf("comment reply.%s.cid = %q, want the post event's cid %q", ref, got, postCID)
		}
	}

	commentURI, commentCID := commentEv.atURI(), commentEv.Commit.CID

	user.createComment(t, post.ID, comment.ID, "nested reply")
	replyEv := l.await("nested reply create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "reply", "parent", "uri")
		return e.Commit.Collection == colComment && e.Commit.Operation == opCreate &&
			got == commentURI
	})
	if replyEv.Did != authorDID {
		t.Errorf("nested reply emitted from repo %s, want the author's repo %s", replyEv.Did, authorDID)
	}

	// Nested reply: parent = the comment, root = the post — resolved
	// against the uri+cid observed on the wire, i.e. exactly what a
	// strongRef-following AppView would look up.
	if got := recordField(t, replyEv.Commit.Record, "reply", "parent", "cid"); got != commentCID {
		t.Errorf("reply parent.cid = %q, want %q", got, commentCID)
	}
	if got := recordField(t, replyEv.Commit.Record, "reply", "root", "uri"); got != postURI {
		t.Errorf("reply root.uri = %q, want %q", got, postURI)
	}
	if got := recordField(t, replyEv.Commit.Record, "reply", "root", "cid"); got != postCID {
		t.Errorf("reply root.cid = %q, want %q", got, postCID)
	}
}

// Scenario 4: edit post → update event on the same rkey; delete comment →
// delete op on the firehose.
func TestUpdateAndDelete(t *testing.T) {
	h := newHarness(t)
	community, sub := setupSubscribedCommunity(t, h, "upd")

	username := h.uniqueName(t, "dave")
	user := h.registerUser(t, username)

	cursor := cursorNow()
	l := h.newListener(t, cursor, colPostV2, colAcceptance, colComment)

	title := "Editable " + h.suffix
	post := user.createPost(t, community.ID, title, "original body")
	postEv := l.await("post create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "title")
		return e.Commit.Collection == colPostV2 && e.Commit.Operation == opCreate &&
			got == title
	})
	l.await("acceptance for the new post", func(e *jsEvent) bool {
		uri, _ := fieldOf(e.Commit.Record, "subject", "uri")
		return e.Commit.Collection == colAcceptance && uri == postEv.atURI()
	})

	comment := user.createComment(t, post.ID, 0, "doomed comment")
	commentEv := l.await("comment create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "reply", "parent", "uri")
		return e.Commit.Collection == colComment && e.Commit.Operation == opCreate &&
			got == postEv.atURI()
	})

	user.editPost(t, post.ID, "edited body")
	updateEv := l.await("post update", func(e *jsEvent) bool {
		return e.Commit.Collection == colPostV2 && e.Commit.Operation == opUpdate &&
			e.Did == postEv.Did && e.Commit.RKey == postEv.Commit.RKey
	})
	if got := recordField(t, updateEv.Commit.Record, "content"); got != "edited body" {
		t.Errorf("updated post content = %q, want %q", got, "edited body")
	}
	if updateEv.Commit.CID == postEv.Commit.CID {
		t.Error("update event carries the same cid as the create — no new commit?")
	}

	// An edit moves the post's CID, so the community's acceptance must be
	// RE-PINNED to the new version — at the same rkey, and without
	// restamping createdAt (the community accepted this post once; re-pinning
	// the version it accepts is not a new acceptance). An acceptance left on
	// the old CID means Coves stops rendering the post the moment its author
	// edits it.
	repinEv := l.await("acceptance repin after the edit", func(e *jsEvent) bool {
		cid, _ := fieldOf(e.Commit.Record, "subject", "cid")
		return e.Commit.Collection == colAcceptance && e.Did == sub.DID &&
			e.Commit.RKey == subjectRKey(postEv.atURI()) && cid == updateEv.Commit.CID
	})
	if repinEv.Commit.Operation != opUpdate {
		t.Errorf("acceptance repin arrived as %q, want %q — the record is updated in place",
			repinEv.Commit.Operation, opUpdate)
	}
	h.awaitAcceptanceConverged(t, sub.DID, postEv.Did, postEv.Commit.RKey)

	user.deleteComment(t, comment.ID)
	deleteEv := l.await("comment delete", func(e *jsEvent) bool {
		return e.Commit.Collection == colComment && e.Commit.Operation == opDelete &&
			e.Did == commentEv.Did && e.Commit.RKey == commentEv.Commit.RKey
	})
	if len(deleteEv.Commit.Record) != 0 && string(deleteEv.Commit.Record) != "null" {
		t.Errorf("delete op unexpectedly carries a record: %s", truncate(deleteEv.Commit.Record, 200))
	}
}

// Scenario 5: votes — on posts AND comments — update the getVoteAggregates
// side channel and NEVER appear as records on the firehose (PLAN.md locked
// decision 7). Covers the full lifecycle: upvote, flip to downvote (a BARE
// Dislike), retract (an Undo carrying a reconstructed inner vote), and a
// comment vote (task 07's reply.root community-binding path).
func TestVotes_SideChannelOnly(t *testing.T) {
	h := newHarness(t)
	community, sub := setupSubscribedCommunity(t, h, "vote")

	author := h.registerUser(t, h.uniqueName(t, "erin"))
	voter := h.registerUser(t, h.uniqueName(t, "frank"))

	cursor := cursorNow()
	// No collection filter: watch EVERYTHING that hits the firehose while
	// votes flow.
	l := h.newListener(t, cursor)

	title := "Votable " + h.suffix
	post := author.createPost(t, community.ID, title, "vote on me")
	postEv := l.await("post create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "title")
		return e.Commit.Collection == colPostV2 && got == title
	})
	if got := recordField(t, postEv.Commit.Record, "community"); got != sub.DID {
		t.Errorf("post community = %q, want %q", got, sub.DID)
	}
	postURI := postEv.atURI()

	// Upvote → aggregates show it.
	voter.likePost(t, post.ID, 1)
	awaitAggregates(t, h, postURI, 1, 0)

	// Flip to downvote → Lemmy sends a BARE Dislike with no Undo at all
	// (measured against the pinned 0.19; see votes/aggregator.go). ApplyVote's
	// supersede marks the prior live upvote undone and applies the downvote.
	voter.likePost(t, post.ID, -1)
	awaitAggregates(t, h, postURI, 0, 1)

	// Retract → an Undo whose inner vote is RECONSTRUCTED: a freshly generated
	// activity id, and typed "Like" even though the live vote is a Dislike.
	// That is why the aggregator keys retraction on the voter, not the id.
	// Back to zero on both sides.
	voter.likePost(t, post.ID, 0)
	awaitAggregates(t, h, postURI, 0, 0)

	// Comment votes ride the same side channel (the aggregator binds a
	// comment to its community via reply.root — pinned here end-to-end).
	comment := author.createComment(t, post.ID, 0, "votable comment")
	commentEv := l.await("comment create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "reply", "parent", "uri")
		return e.Commit.Collection == colComment && e.Commit.Operation == opCreate &&
			got == postURI
	})
	voter.likeComment(t, comment.ID, 1)
	awaitAggregates(t, h, commentEv.atURI(), 1, 0)

	// POSITIVE emission assertion (the other half of locked decision 7): the
	// vote-stats refresher folds the comment's live upvote onto its record as a
	// bridgedStats UPDATE (STATS_REFRESH_INTERVAL=2s in this stack). Await that
	// update on the comment's rkey carrying upvotes=1 with a parseable asOf —
	// end-to-end proof the counts reach the AppView on the firehose, not only
	// the side-channel XRPC. The comment's final vote state is a stable 1/0 (no
	// further votes), so this target does not race a later count change.
	l.await("comment bridgedStats update (upvotes=1)", func(e *jsEvent) bool {
		if e.Commit.Collection != colComment || e.Commit.Operation != opUpdate || e.atURI() != commentEv.atURI() {
			return false
		}
		up, ok := bridgedStatsUpvotes(e.Commit.Record)
		return ok && up == 1 && bridgedStatsAsOfParses(e.Commit.Record)
	})

	// The whole time, nothing vote-shaped may have hit the firehose. The
	// listener's centralized vetting already fails fast on any unexpected
	// collection; this explicit unfiltered sweep is the belt to that
	// suspenders.
	for _, ev := range l.drain(3 * time.Second) {
		if ev.Kind == kindCommit && ev.Commit != nil && !expectedCollections[ev.Commit.Collection] {
			t.Errorf("unexpected collection on firehose during voting: %s", ev)
		}
	}
}

// awaitAggregates polls the side-channel XRPC until the subject shows the
// expected live counts (votes flow through Lemmy's federation queue and the
// bridge's inbox queue, so counts converge, not snap).
func awaitAggregates(t *testing.T, h *harness, uri string, up, down int64) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	var last voteAggregate
	for time.Now().Before(deadline) {
		aggs := h.getVoteAggregates(t, uri)
		last = aggs[uri]
		if last.Upvotes == up && last.Downvotes == down {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("aggregates for %s = %d up / %d down, want %d/%d", uri, last.Upvotes, last.Downvotes, up, down)
}

// Scenario 6: backfill — posts that existed BEFORE the bridge subscribed
// appear on the firehose after the Follow is accepted (outbox walk), with
// the community profile first and every post's author profile before the
// post itself; pre-existing vote counts are seeded from Lemmy's API
// (SEED_COUNTS_FROM_API, on in the e2e compose).
func TestBackfill_PreexistingPosts(t *testing.T) {
	h := newHarness(t)

	name := h.uniqueName(t, "bf")
	community := h.admin.createCommunity(t, name, "Backfill "+name)

	author := h.registerUser(t, h.uniqueName(t, "gina"))
	titles := make(map[string]bool, 3)
	var votedPost lemmyPost
	var votedTitle string
	for i := range 3 {
		title := fmt.Sprintf("Pre-existing %d %s", i, h.suffix)
		p := author.createPost(t, community.ID, title, fmt.Sprintf("history %d", i))
		titles[title] = true
		if i == 0 {
			votedPost, votedTitle = p, title
		}
	}
	// A second user upvotes one pre-existing post BEFORE the bridge knows
	// this community exists. Lemmy's API then reports upvotes=2 for it (the
	// author auto-like + this vote) — the number the backfill seeder must
	// carry over, since neither vote will ever federate as an activity.
	voter := h.registerUser(t, h.uniqueName(t, "hal"))
	voter.likePost(t, votedPost.ID, 1)

	cursor := cursorNow()
	l := h.newListener(t, cursor, colCommunityProfile, colActorProfile, colPostV2)

	sub := h.subscribeCommunity(t, "!"+name+"@lemmy")

	l.await("community.profile create", func(e *jsEvent) bool {
		return e.Did == sub.DID && e.Commit.Collection == colCommunityProfile
	})

	// All three historical posts must materialize (accept triggers the
	// outbox backfill), and every post's AUTHOR must appear on the firehose
	// as an actor.profile. The bridge emits each profile strictly before
	// its posts, but profile and post live in different repos and the relay
	// preserves only per-repo order (scenario 2's caveat) — so authorship
	// is asserted as presence (each author's profile arrives), not arrival
	// order.
	profileDIDs := map[string]bool{}
	postAuthors := map[string]string{} // author DID → one of their post titles
	votedURI := ""
	remaining := len(titles)
	for remaining > 0 {
		ev := l.await(fmt.Sprintf("backfilled post or author profile (%d posts to go)", remaining), func(e *jsEvent) bool {
			switch e.Commit.Collection {
			case colActorProfile:
				return true // consume every profile to track the author set
			case colPostV2:
				title, _ := fieldOf(e.Commit.Record, "title")
				return e.Commit.Operation == opCreate && titles[title]
			}
			return false
		})
		if ev.Commit.Collection == colActorProfile {
			profileDIDs[ev.Did] = true
			continue
		}
		title := recordField(t, ev.Commit.Record, "title")
		// The repo IS the authorship claim now — there is no author field to
		// read, so the post's own DID is the author whose profile must exist.
		postAuthors[ev.Did] = title
		if title == votedTitle {
			votedURI = ev.atURI()
		}
		delete(titles, title)
		remaining--
	}
	// Any author whose profile hasn't been consumed yet must still be in
	// flight (or already buffered by await): wait for each explicitly.
	for author, title := range postAuthors {
		if profileDIDs[author] {
			continue
		}
		l.await(fmt.Sprintf("actor.profile for the author of %q (%s)", title, author), func(e *jsEvent) bool {
			return e.Commit.Collection == colActorProfile && e.Did == author
		})
	}

	// The seeded baseline shows through the side channel: 2 up (author
	// auto-like + the pre-subscribe vote), 0 down.
	if votedURI == "" {
		t.Fatal("the voted pre-existing post never appeared on the firehose")
	}
	awaitAggregates(t, h, votedURI, 2, 0)
}

// Scenario 7: recovery across restart — bounce the Tidepool container and
// prove three things:
//
//  1. A forced backfill redo — confirmed to have RUN via the admin API's
//     last_backfill_at advancing — re-materializes everything with NO
//     duplicate commits (deterministic rkeys + identical-re-put-is-a-noop).
//  2. A "gap" post created right after /healthz, while Jetstream may still
//     be reconnecting, is delivered exactly once (cursor resume, not luck).
//  3. Replaying the ORIGINAL cursor after the bounce yields the entire
//     pre-restart history exactly once — keyed by (did, collection/rkey)
//     across ALL operations, so a broken replay emitting create+update for
//     the same record is caught too.
func TestRestart_ReplayIsIdempotent(t *testing.T) {
	h := newHarness(t)

	name := h.uniqueName(t, "rst")
	community := h.admin.createCommunity(t, name, "Restart "+name)
	author := h.registerUser(t, h.uniqueName(t, "hank"))

	title := "Survivor " + h.suffix
	author.createPost(t, community.ID, title, "pre-restart post")

	cursor := cursorNow()
	l := h.newListener(t, cursor, colCommunityProfile, colActorProfile, colPostV2)

	handle := "!" + name + "@lemmy"
	sub := h.subscribeCommunity(t, handle)

	firstEv := l.await("pre-restart post create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "title")
		return e.Commit.Collection == colPostV2 && got == title
	})
	if firstEv.Commit.Operation != opCreate {
		t.Fatalf("pre-restart post arrived as %q, want %q", firstEv.Commit.Operation, opCreate)
	}

	// Bounce the bridge. Jetstream exits when its upstream drops and docker
	// revives it (see docker-compose.e2e.yml), so the pre-restart listener's
	// connection is gone — close it deliberately first.
	l.close()
	h.restartTidepool(t)

	// Immediately after /healthz — deliberately NOT waiting for Jetstream's
	// own recovery — create a post into the reconnect gap. Wherever in the
	// recovery dance it lands, it must come out exactly once.
	gapTitle := "Gap " + h.suffix
	author.createPost(t, community.ID, gapTitle, "created in the recovery window")

	// Force a full backfill redo and wait for the admin API to report a NEW
	// last_backfill_at (the run is async): only after the redo has actually
	// finished is "it emitted no duplicates" an assertion rather than a race
	// against a slow redo.
	before, ok := h.findCommunity(t, handle, sub.Community)
	if !ok {
		t.Fatalf("community %s missing from the admin list after restart", handle)
	}
	h.triggerBackfill(t, handle)
	backfillDeadline := time.Now().Add(eventTimeout)
	for {
		state, found := h.findCommunity(t, handle, sub.Community)
		if found && state.LastBackfillAt != "" && state.LastBackfillAt != before.LastBackfillAt {
			t.Logf("backfill redo completed at %s (previous run: %q)", state.LastBackfillAt, before.LastBackfillAt)
			break
		}
		if time.Now().After(backfillDeadline) {
			t.Fatalf("backfill redo never completed: last_backfill_at stuck at %q", before.LastBackfillAt)
		}
		time.Sleep(time.Second)
	}

	// Replay everything from the ORIGINAL cursor. Key the accounting by
	// (did, collection/rkey) — NOT operation — so a create+update pair for
	// one record counts as the duplicate it is. EXCEPTION: the vote-stats
	// refresher legitimately emits one bridgedStats UPDATE per seeded/backfilled
	// post (SEED_COUNTS_FROM_API is on, so the backfill redo re-seeds these
	// posts and the sweeper folds the counts onto each record) — those are the
	// debounced feature, not a re-commit. They are excluded via statsDedup,
	// which recognises a stats emission as an update equal to the prior record
	// MODULO bridgedStats. A blanket isBridgedStatsUpdate would be too loose
	// here: once stamped, EVERY update carries the field via carry-forward, so a
	// non-idempotent rebuild that duplicated a record's CONTENT would also carry
	// it and slip past — the modulo comparison still catches that (its content
	// differs), so this assertion keeps its teeth.
	l2 := h.newListener(t, cursor, colCommunityProfile, colActorProfile, colPostV2)

	title2 := "Post-restart " + h.suffix
	author.createPost(t, community.ID, title2, "after the bounce")

	seen := map[string]int{}
	dedup := newStatsDedup()
	keyOf := func(ev *jsEvent) string {
		return fmt.Sprintf("%s %s/%s", ev.Did, ev.Commit.Collection, ev.Commit.RKey)
	}
	account := func(ev *jsEvent) {
		if ev.Kind != kindCommit || ev.Commit == nil {
			return
		}
		key := keyOf(ev)
		if dedup.isPureStatsEmission(ev, key) {
			return // the debounced bridgedStats emission, not a re-commit
		}
		seen[key]++
	}
	gapKey := ""
	sawNew, sawGap := false, false
	deadline := time.Now().Add(eventTimeout)
	for !(sawNew && sawGap) && time.Now().Before(deadline) {
		for _, ev := range l2.drain(time.Second) {
			if ev.Kind != kindCommit || ev.Commit == nil {
				continue
			}
			pureStats := dedup.isPureStatsEmission(ev, keyOf(ev))
			if !pureStats {
				seen[keyOf(ev)]++
			}
			if ev.Commit.Collection == colPostV2 && !pureStats {
				switch got, _ := fieldOf(ev.Commit.Record, "title"); got {
				case gapTitle:
					sawGap, gapKey = true, keyOf(ev)
				case title2:
					sawNew = true
				}
			}
		}
	}
	if !sawGap {
		t.Fatal("gap post (created during the recovery window) never reached jetstream — cursor resume dropped it")
	}
	if !sawNew {
		t.Fatal("post-restart post never reached jetstream — pipeline did not survive the restart")
	}
	// The backfill redo finished before l2 dialed; a short trailing drain
	// still catches any late duplicate emission in flight.
	for _, ev := range l2.drain(8 * time.Second) {
		account(ev)
	}

	// The replayed pre-restart post must appear EXACTLY once: zero would
	// mean Jetstream lost history, twice would mean the backfill redo
	// re-committed it (deterministic-rkey idempotency broken).
	firstKey := fmt.Sprintf("%s %s/%s", firstEv.Did, colPostV2, firstEv.Commit.RKey)
	if n := seen[firstKey]; n != 1 {
		t.Errorf("pre-restart post replayed %d times (want exactly 1): %s", n, firstKey)
	}
	if n := seen[gapKey]; n != 1 {
		t.Errorf("gap post appeared %d times (want exactly 1): %s", n, gapKey)
	}
	for key, n := range seen {
		if n > 1 {
			t.Errorf("record %s committed %d times after restart — duplicate emission", key, n)
		}
	}
}

// Scenario 8 (bonus, closes a task-06 deferred gap end-to-end): a burst of
// activity across TWO communities and several authors — concurrent queue
// workers (INGEST_WORKERS=4) with distinct ordering keys — lands every
// record exactly once, counting every commit per (did, collection/rkey)
// regardless of operation.
func TestBurst_ConcurrentIngestionExactlyOnce(t *testing.T) {
	h := newHarness(t)

	commA, _ := setupSubscribedCommunity(t, h, "ba")
	commB, _ := setupSubscribedCommunity(t, h, "bb")

	users := []*lemmyClient{
		h.registerUser(t, h.uniqueName(t, "ivy")),
		h.registerUser(t, h.uniqueName(t, "jack")),
		h.registerUser(t, h.uniqueName(t, "kim")),
	}

	cursor := cursorNow()
	l := h.newListener(t, cursor, colPostV2)

	const perCommunity = 6
	titles := make(map[string]bool, 2*perCommunity)
	for i := range perCommunity {
		for j, id := range []int{commA.ID, commB.ID} {
			title := fmt.Sprintf("Burst %d.%d %s", j, i, h.suffix)
			users[(i+j)%len(users)].createPost(t, id, title, "burst body")
			titles[title] = true
		}
	}

	// Account for EVERY commit on the two communities' repos by
	// (did, collection/rkey) — an update sneaking in after a create is a
	// duplicate commit on that record, not a separate event. Collection is
	// drain-based, not await-based: drain sees every live event exactly
	// once, so the accounting cannot double-count (await predicates must
	// stay pure now that non-matches are buffered and rescanned), and it is
	// inherently order-agnostic — which the relay's cross-repo reordering
	// demands anyway.
	// Scoped by TITLE, not by repo DID: postv2 records land in each author's
	// own repo (of which there are three here), so the two community DIDs no
	// longer identify this scenario's posts. The titles are unique per run
	// and are already the identity the exactly-once accounting turns on.
	seen := map[string]int{}
	keyTitle := map[string]string{}
	count := func(e *jsEvent) {
		if e.Kind != kindCommit || e.Commit == nil {
			return
		}
		if e.Commit.Collection != colPostV2 {
			return
		}
		title, ok := fieldOf(e.Commit.Record, "title")
		if !ok || !titles[title] {
			return
		}
		key := fmt.Sprintf("%s %s/%s", e.Did, e.Commit.Collection, e.Commit.RKey)
		seen[key]++
		keyTitle[key] = title
	}

	// Every post arrives…
	matched := map[string]bool{}
	deadline := time.Now().Add(burstTimeout)
	for len(matched) < len(titles) {
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d burst posts arrived within %s", len(matched), len(titles), burstTimeout)
		}
		for _, ev := range l.drain(time.Second) {
			count(ev)
			if ev.Kind != kindCommit || ev.Commit == nil || ev.Commit.Operation != opCreate {
				continue
			}
			if title, ok := fieldOf(ev.Commit.Record, "title"); ok && titles[title] {
				matched[title] = true
			}
		}
	}
	// …and exactly once: nothing trailing, no second commit on any rkey.
	for _, ev := range l.drain(4 * time.Second) {
		count(ev)
	}
	for key, n := range seen {
		if n != 1 {
			t.Errorf("record %s (%q) committed %d times, want exactly 1", key, keyTitle[key], n)
		}
	}
	// …and no burst title may have landed under two different rkeys.
	perTitle := map[string]int{}
	for _, title := range keyTitle {
		perTitle[title]++
	}
	for title := range titles {
		if perTitle[title] != 1 {
			t.Errorf("burst post %q landed on %d distinct records, want exactly 1", title, perTitle[title])
		}
	}
}
