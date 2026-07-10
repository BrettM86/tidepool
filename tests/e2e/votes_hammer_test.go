//go:build e2e

package e2e

import (
	"sync"
	"testing"
)

// Scenario 14: vote concurrency hammer — many real Lemmy voters on one
// post, cast in parallel bursts. What this proves is END-TO-END BURST
// EXACTNESS — a pile of near-simultaneous votes, then concurrent flips
// (bare opposite vote, no Undo) and clears (Undo with a RECONSTRUCTED
// inner Like) all landing with an exactly-correct final aggregate. It does
// NOT exercise same-row lock contention in the aggregator: one post means
// one community ordering key, so the bridge's queue fully serializes these
// deliveries (and Lemmy's per-instance federation worker delivers
// sequentially anyway) — row-lock contention coverage stays unit-level
// (FOLLOWUPS.md, vote hammer item). The final aggregate must be EXACTLY
// right after each burst:
//
//   - burst 1: 7 upvotes + 3 downvotes land concurrently → (7, 3);
//   - burst 2: the 7 upvoters flip to downvotes (Lemmy federates a bare
//     opposite vote, no Undo) while the 3 downvoters clear (Undo with a
//     RECONSTRUCTED inner Like — the task-07 wire quirk) → (0, 7).
//
// The post author's auto-upvote never federates (FOLLOWUPS "Author
// auto-upvotes do not federate"), so it is absent from every expectation.
// An unfiltered listener runs throughout: vetEvent fails the suite if
// anything vote-shaped ever becomes a firehose record (locked decision 7).
func TestVoteHammer_ConcurrentVotersExactAggregates(t *testing.T) {
	h := newHarness(t)
	community, sub := setupSubscribedCommunity(t, h, "vh")

	author := h.registerUser(t, h.uniqueName(t, "vera"))

	cursor := cursorNow()
	l := h.newListener(t, cursor) // unfiltered: watch EVERYTHING while votes fly

	title := "Hammer me " + h.suffix
	post := author.createPost(t, community.ID, title, "vote target")
	postEv := l.await("hammer post create", func(e *jsEvent) bool {
		got, _ := fieldOf(e.Commit.Record, "title")
		return e.Commit.Collection == colPost && e.Did == sub.DID &&
			e.Commit.Operation == opCreate && got == title
	})
	postURI := postEv.atURI()

	// Registration is sequential (it shares the harness name counter);
	// only the VOTES are concurrent — that is the contended path.
	const upVoters, downVoters = 7, 3
	voters := make([]*lemmyClient, 0, upVoters+downVoters)
	for i := range upVoters + downVoters {
		prefix := "vup"
		if i >= upVoters {
			prefix = "vdn"
		}
		voters = append(voters, h.registerUser(t, h.uniqueName(t, prefix)))
	}

	// burst fires one vote per voter concurrently and fails on any API
	// error (t.Fatalf must stay on the test goroutine).
	burst := func(desc string, score func(i int) int) {
		t.Helper()
		var wg sync.WaitGroup
		errs := make(chan error, len(voters))
		for i, v := range voters {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := v.likePostErr(post.ID, score(i)); err != nil {
					errs <- err
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("%s: vote failed: %v", desc, err)
		}
	}

	burst("burst 1", func(i int) int {
		if i < upVoters {
			return 1
		}
		return -1
	})
	awaitAggregates(t, h, postURI, upVoters, downVoters)

	burst("burst 2 (flips + clears)", func(i int) int {
		if i < upVoters {
			return -1 // flip: bare Dislike on the wire, no Undo
		}
		return 0 // clear: Undo with a reconstructed inner vote
	})
	awaitAggregates(t, h, postURI, 0, upVoters)

	// Belt on locked decision 7: everything consumed during the hammer was
	// already vetted (vetEvent Fatalfs on any unexpected collection inside
	// await AND drain), so the drain itself IS the assertion — it forces
	// whatever is still buffered/live through vetEvent so no vote-shaped
	// record can hide in the pending buffer. No explicit loop over the
	// result: it could never run (vetEvent fails first).
	l.drain(negativeWindow)
}
