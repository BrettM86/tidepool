//go:build e2e

package e2e

// Task 09: relay-specific assertions. Every OTHER scenario already
// exercises the relay implicitly — Jetstream consumes the relay's firehose,
// so no event reaches any listener without surviving bigsky's DID
// resolution (against the local PLC), per-commit signature verification,
// and indexing. The tests here assert the relay-side STATE that implicit
// transit doesn't: the bridge present in the crawled-PDS registry (the
// bridge-originated requestCrawl, task 09's definition of done) and repos
// listed/served by the relay's own sync surface.
//
// Posture notes (verified against the pinned image's source, indigo bgs/):
//   - Handle verification: the compose stack points bigsky's trial-host
//     resolver at the bridge (HANDLE_RESOLVER_HOSTS=tidepool), so bridged
//     handles — DNS-invisible names like alice.lemmy.tidepool — resolve via
//     the bridge's Host-header-keyed /.well-known/atproto-did and actually
//     verify. Had that failed, bigsky's createExternalUser treats handle
//     failure as NON-fatal (repo kept, handle marked invalid), so this is
//     belt on top of a safe default.
//   - Tombstones: bigsky FILTERS tombstoned/taken-down repos out of
//     listRepos and serves no getRepoStatus, and the bridge does not emit
//     #account frames yet (deferred to task 11) — so consent-revoked
//     (active:false) repo status is NOT yet observable through the relay.
//     Tracked in FOLLOWUPS.md.

import (
	"testing"
	"time"
)

// TestRelay_BridgeCrawledBySelfAnnouncement proves the bridge-originated
// requestCrawl: the bridge's startup RequestCrawlAll (RELAY_HOSTS +
// ALLOW_DEV_REQUEST_CRAWL, the REAL production code path) is the only
// requestCrawl sender in the stack — the compose bootstrap merely raises
// the relay's new-PDS-per-day limit from its fresh-database 0 — so the
// bridge hostname appearing in the relay's PDS registry as registered,
// with a live subscribeRepos connection, is the announcement observed
// end-to-end rather than eyeballed in logs.
func TestRelay_BridgeCrawledBySelfAnnouncement(t *testing.T) {
	h := newHarness(t)

	deadline := time.Now().Add(crawlTimeout)
	var last []relayPDS
	var lastErr error
	for {
		// Transient admin-API errors must not kill the poll — log and keep
		// waiting; only the deadline is fatal (with the last error shown).
		pdsList, err := h.relayPDSList()
		if err != nil {
			lastErr = err
			t.Logf("relay pds/list poll: %v (retrying)", err)
		} else {
			last = pdsList
			for _, pds := range last {
				if pds.Host != bridgeHostname() {
					continue
				}
				if !pds.Registered {
					break // present but unregistered: keep polling
				}
				if !pds.HasActiveConnection {
					break // crawl accepted, subscription still dialing
				}
				t.Logf("relay crawled the bridge: host=%s registered=%v active=%v repos=%d",
					pds.Host, pds.Registered, pds.HasActiveConnection, pds.RepoCount)
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("bridge host %q never became a registered+connected PDS on the relay within %s (registry: %+v, last poll error: %v)",
				bridgeHostname(), crawlTimeout, last, lastErr)
		}
		time.Sleep(2 * time.Second)
	}
}

// TestRelay_RepoCrawledAndServed drives one community through the full
// pipeline and then asserts the RELAY's own view of the resulting repo:
// it appears in the relay's listRepos with a head, and the relay serves
// getLatestCommit for it — i.e. the relay didn't just forward frames, it
// built (and can serve) validated repo state from the bridge's commits.
func TestRelay_RepoCrawledAndServed(t *testing.T) {
	h := newHarness(t)

	cursor := cursorNow()
	l := h.newListener(t, cursor, colCommunityProfile)

	_, sub := setupSubscribedCommunity(t, h, "rly")

	// The event arriving at Jetstream proves the relay accepted and
	// re-emitted the commit (Jetstream's upstream IS the relay).
	ev := l.await("community.profile through the relay", func(e *jsEvent) bool {
		return e.Did == sub.DID && e.Commit.Collection == colCommunityProfile &&
			e.Commit.Operation == opCreate
	})

	// The relay indexes asynchronously; poll its sync surface for the repo.
	// Transient listRepos errors are logged and waited out — only the
	// deadline is fatal.
	deadline := time.Now().Add(eventTimeout)
	var lastErr error
	for {
		repos, err := h.relayListRepos()
		if err != nil {
			lastErr = err
			t.Logf("relay listRepos poll: %v (retrying)", err)
		} else if head, ok := repos[sub.DID]; ok {
			if head == "" {
				t.Fatalf("relay listRepos shows %s with an empty head", sub.DID)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("repo %s never appeared in the relay's listRepos within %s (last poll error: %v)", sub.DID, eventTimeout, lastErr)
		}
		time.Sleep(time.Second)
	}

	// getLatestCommit must serve the repo, and — since the profile commit
	// has demonstrably been processed (it reached Jetstream) — at a rev no
	// older than that event's.
	cid, rev, err := h.relayGetLatestCommit(sub.DID)
	if err != nil {
		t.Fatalf("relay getLatestCommit(%s): %v", sub.DID, err)
	}
	if cid == "" || rev == "" {
		t.Fatalf("relay getLatestCommit(%s) returned empty cid/rev (%q/%q)", sub.DID, cid, rev)
	}
	if rev < ev.Commit.Rev {
		t.Errorf("relay head rev %q is OLDER than the profile event's rev %q — relay state lagging its own emissions", rev, ev.Commit.Rev)
	}
}
