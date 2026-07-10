//go:build e2e

package e2e

import (
	"fmt"
	"testing"
	"time"
)

// sweepReplayFloor is the minimum number of commit events the sweep must
// account before its non-vacuity claim holds. A full suite run emits well
// over a hundred commits (profiles, posts, comments, edits, deletes,
// backfill seeds, the 12-post burst alone); the floor is deliberately far
// below that so scenario tweaks never trip it, while a truncated replay
// (Jetstream's 24h event TTL on a long-lived stack, a recreated Jetstream
// container) or a cursor-semantics regression to live-tail — either of
// which would still deliver the fresh sentinel — cannot pass silently.
const sweepReplayFloor = 30

// Scenario 15: suite-end sweep. `go test` runs tests in source order with
// files compiled alphabetically, so this file's zz_ name (and the test's ZZ_
// prefix) makes it run AFTER every other scenario — deliberately: it replays
// the firehose history Jetstream still retains, from cursor 1, on a fresh
// unfiltered listener and re-vets every replayed event. That closes the one
// gap the per-scenario listeners leave: events emitted while NO unfiltered
// listener happened to be subscribed were never checked against the
// collection whitelist / lexicons. Here vetEvent (helpers.go) judges every
// single consumed commit:
//
//   - collection whitelist (votes NEVER become records — locked decision 7),
//   - lexicon validation of every create/update record,
//   - per-DID rev monotonicity from cursor 1 (the listener's lastRev starts
//     empty, so the full RETAINED history of every repo is checked in
//     order).
//
// Termination is a sentinel, not a bare quiet-window: one fresh community
// subscription emitted AFTER the listener dialed gives the replay a
// deterministic high-water mark (and doubles as the non-vacuity proof — the
// sweep demonstrably consumed the live edge of the stream).
//
// HONEST BOUNDS of the guarantee, weaker than "every event the suite ever
// emitted":
//
//   - Replay depth is Jetstream's event TTL (24h in the pinned image): a
//     stack older than the TTL — or a recreated Jetstream container — has
//     silently forgotten early history. The replay-floor assertions below
//     (sweepReplayFloor, plus at least one community.post create AND at
//     least one delete op, both of which every full suite run emits) turn
//     a truncated or live-tail-degenerated replay into a failure instead
//     of a vacuous pass on the sentinel alone.
//   - Termination is best-effort for relay-reordered stragglers: the
//     sentinel is only "newest event" in this one connection's ordering,
//     and the trailing 5s drain is a quiet window, not a proof — an event
//     the relay reorders past both is vetted by nobody.
//   - The zz_/ZZ_ "runs last" property is alphabetical file/test ordering
//     within one `go test` invocation; it does NOT survive -run selection.
//     Running this test alone is legal but only vets whatever history
//     survives in Jetstream's store — it cannot vouch for a suite that
//     never ran.
func TestZZ_SuiteEndSweep(t *testing.T) {
	h := newHarness(t)

	// Cursor 1 ≈ the beginning of Jetstream's retained history (its cursor
	// is a µs timestamp; the compose stack retains 24h — the whole run).
	l := h.newListener(t, 1)

	// Sentinel: a fresh community's profile create is the newest event the
	// bridge will emit, so replay + live tail must deliver it last-ish.
	name := h.uniqueName(t, "swp")
	h.admin.createCommunity(t, name, "Sweep sentinel "+name)
	sub := h.subscribeCommunity(t, "!"+name+"@lemmy")

	counts := map[string]int{} // "collection op" → count
	kinds := map[string]int{}  // non-commit kinds, informational
	repos := map[string]bool{}
	total, postCreates, deleteOps := 0, 0, 0
	account := func(evs []*jsEvent) (sentinel bool) {
		for _, ev := range evs {
			if ev.Kind != kindCommit || ev.Commit == nil {
				kinds[ev.Kind]++
				continue
			}
			total++
			repos[ev.Did] = true
			counts[ev.Commit.Collection+" "+ev.Commit.Operation]++
			if ev.Commit.Collection == colPost && ev.Commit.Operation == opCreate {
				postCreates++
			}
			if ev.Commit.Operation == opDelete {
				deleteOps++
			}
			if ev.Did == sub.DID && ev.Commit.Collection == colCommunityProfile &&
				ev.Commit.Operation == opCreate {
				sentinel = true
			}
		}
		return sentinel
	}

	deadline := time.Now().Add(sweepTimeout)
	sentinelSeen := false
	for !sentinelSeen {
		if time.Now().After(deadline) {
			t.Fatalf("sweep sentinel (community.profile create for %s) not reached within %s — replayed %d commits so far: %v",
				sub.DID, sweepTimeout, total, counts)
		}
		// drain vets every consumed event (vetEvent) and returns each
		// exactly once — the accounting below cannot double-count.
		sentinelSeen = account(l.drain(time.Second)) || sentinelSeen
	}
	// Trailing quiet window: stragglers that legally reordered past the
	// sentinel (cross-repo order does not survive the relay) still get
	// vetted and counted.
	account(l.drain(5 * time.Second))

	if total == 0 {
		t.Fatal("sweep consumed zero commit events — the replay was vacuous")
	}
	// Replay floor: the sentinel alone is satisfiable by a truncated replay
	// (Jetstream event TTL, recreated container) or a live-tail regression.
	// Every full suite run emits post creates and delete ops in bulk, so
	// their absence — or a suspiciously thin total — means the sweep did not
	// actually re-vet the suite's history.
	if total <= sweepReplayFloor {
		t.Errorf("sweep accounted only %d commits (floor %d) — replay looks truncated or live-tailed, not a suite-history re-vet: %v",
			total, sweepReplayFloor, counts)
	}
	if postCreates == 0 {
		t.Errorf("sweep saw no %s create — a full suite run always emits posts, so the replayed history is incomplete: %v",
			colPost, counts)
	}
	if deleteOps == 0 {
		t.Errorf("sweep saw no delete op in any collection — a full suite run always emits deletes (edits/deletes, scrubs), so the replayed history is incomplete: %v",
			counts)
	}
	summary := fmt.Sprintf("suite-end sweep: %d commits across %d repos, per collection/op: %v", total, len(repos), counts)
	if len(kinds) > 0 {
		summary += fmt.Sprintf(", non-commit kinds: %v", kinds)
	}
	t.Log(summary)
}
