package consume

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
)

// The hosted-repo filter (decision 14). Tidepool commits into the repos it
// hosts — bridged content, acceptance and removal records — and enqueues the
// outbound for those writes AT WRITE TIME. Consuming them back off Jetstream
// would deliver everything twice. So an event whose repo DID is a repo
// Tidepool commits into is dropped BEFORE any handler and before the rev gate:
// it must not even leave a gate row behind, because that row would later
// reject the legitimate event it shadows.
//
// Membership is a repo_state PRIMARY KEY lookup, not an enumeration of
// bridged_actors ∪ communities: repo_state has exactly one row per repo
// Tidepool writes to, which is the question being asked, and a PK probe stays
// O(1) as the deployment grows.

// hostedRepoProbe answers the membership question against storage. It is a
// separate seam from the cache so the caching policy can be tested without a
// database and the query without a cache.
type hostedRepoProbe func(ctx context.Context, did string) (bool, error)

// hostedRepos answers "does Tidepool commit into this repo?" with a cache.
//
// Only POSITIVE answers are cached, and this asymmetry is deliberate. A repo
// never stops being Tidepool-hosted, so a cached yes can never go stale. A
// cached NO can: the moment a community repo is minted its DID becomes hosted,
// and a consumer still holding "not hosted" would double-deliver everything
// that community writes until the entry expired — the exact failure this
// filter exists to prevent. A miss therefore re-probes, which costs one
// indexed primary-key lookup.
type hostedRepos struct {
	probe hostedRepoProbe

	mu     sync.RWMutex
	hosted map[string]struct{}
}

// newHostedRepos builds the filter over repo_state.
func newHostedRepos(db *sql.DB) *hostedRepos {
	return newHostedReposWithProbe(func(ctx context.Context, did string) (bool, error) {
		var exists bool
		err := db.QueryRowContext(ctx,
			`SELECT EXISTS (SELECT 1 FROM repo_state WHERE did = $1)`, did).Scan(&exists)
		return exists, err
	})
}

// newHostedReposWithProbe builds the filter over an arbitrary membership
// probe.
func newHostedReposWithProbe(probe hostedRepoProbe) *hostedRepos {
	return &hostedRepos{probe: probe, hosted: make(map[string]struct{})}
}

// IsHosted reports whether Tidepool commits into the repo for did.
//
// A probe failure is returned, never reported as "not hosted": failing open
// here would let Tidepool's own writes back into the pipeline and deliver
// everything twice, which is worse than the transient retry a returned error
// buys.
func (h *hostedRepos) IsHosted(ctx context.Context, did string) (bool, error) {
	h.mu.RLock()
	_, cached := h.hosted[did]
	h.mu.RUnlock()
	if cached {
		return true, nil
	}

	hosted, err := h.probe(ctx, did)
	if err != nil {
		return false, fmt.Errorf("probe hosted repo %q: %w", did, err)
	}
	if hosted {
		h.mu.Lock()
		h.hosted[did] = struct{}{}
		h.mu.Unlock()
	}
	return hosted, nil
}
