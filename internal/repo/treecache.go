package repo

import (
	"container/list"
	"sync"

	"github.com/bluesky-social/indigo/atproto/repo/mst"
	"github.com/ipfs/go-cid"
)

// DefaultTreeCacheSize is the number of per-DID MST trees kept in memory when
// no explicit size is configured. Each entry holds one fully-decoded tree, so
// memory scales with the sizes of the cached repos, not just the count —
// operators bridging many very large communities should tune MST_CACHE_SIZE
// down (or up, to keep more hot repos resident).
const DefaultTreeCacheSize = 512

// mstTreeCache is a bounded, LRU per-DID cache of decoded MST trees, keyed by
// DID and validated against the commit head the tree represents. It exists to
// make PutRecord cheap: loading a repo's tree from postgres is one SELECT per
// MST node (O(repo size) round-trips), so a big community's every commit paid
// a full-tree reload. Caching the live tree turns the steady-state commit into
// an in-memory O(log n) mutation with zero block reads.
//
// COHERENCE MODEL. The cache is a WRITE-PATH optimization only and is only
// ever touched while the caller holds that DID's per-DID write mutex
// (Manager.lockFor) — so all access to a given DID's entry is serialized. Two
// rules keep a cached tree exactly equal to a durably-committed head:
//
//  1. take() DETACHES the entry (removes it from the map) and hands back the
//     live tree pointer. The commit path then mutates that tree in place. If
//     the commit fails or rolls back, the mutated tree is simply never
//     re-inserted, so the next commit reloads a correct tree from postgres —
//     a mutated-but-uncommitted tree can never be observed.
//  2. put() re-attaches a tree ONLY under the head that is now durable in
//     postgres: the new commit CID after a successful commit, or the
//     unchanged head on the idempotent NoOp path (indigo leaves the tree
//     unmodified on a no-op insert). A tree is never cached under a head that
//     did not commit.
//
// take() also validates head: an entry whose head does not match the head just
// read under the commit's row lock is stale (another process committed) and is
// dropped rather than used. This string-equality check is ABA-safe: head CIDs
// never repeat, because NextRev chains a strictly-increasing rev from the
// stored prev rev under the commit locks and the rev is embedded in the
// signed commit block, so no two commits of a repo can hash to the same head.
// And even a repeated head would be harmless — the head is content-addressed,
// so equal heads mean identical trees, not merely coincidentally-equal labels.
//
// Reads (GetRecord/GetRecordProof/ExportCAR) deliberately do NOT use this
// cache: they run in their own snapshot read transactions and rely on the
// content-addressed blocks table — append-only apart from GC of
// head-unreachable blocks (see gc.go) — for consistency (see repo.go).
// Mixing a shared mutable tree into those paths would add coherence questions
// for no benefit, since reads never contend on the per-DID write mutex.
type mstTreeCache struct {
	mu      sync.Mutex
	max     int
	ll      *list.List               // front = most recently used
	entries map[string]*list.Element // did -> *cacheItem element
}

type cacheItem struct {
	did  string
	head string
	// root is the MST root CID (the commit's Data field) the tree encodes.
	// The commit path needs it as the firehose prevData without recomputing
	// it from the tree.
	root cid.Cid
	tree *mst.Tree
}

// newTreeCache builds a cache holding at most max trees (LRU eviction). A
// non-positive max disables caching (every take() misses), which keeps the
// commit path correct — just without the optimization.
func newTreeCache(max int) *mstTreeCache {
	return &mstTreeCache{
		max:     max,
		ll:      list.New(),
		entries: make(map[string]*list.Element),
	}
}

// take removes the cached tree for did and returns it when the cached entry
// matches head (the head just read inside the commit transaction). A miss —
// no entry, or an entry under a different head (a stale cross-process commit)
// — returns nil after evicting any stale entry. The returned tree is detached
// from the cache: the caller owns it until it re-inserts via put (only on a
// durable head) or drops it (on failure).
func (c *mstTreeCache) take(did, head string) (*mst.Tree, cid.Cid, bool) {
	if c == nil || c.max <= 0 {
		return nil, cid.Undef, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.entries[did]
	if !ok {
		return nil, cid.Undef, false
	}
	// Detach unconditionally; only return it if the head matches.
	delete(c.entries, did)
	c.ll.Remove(el)
	item := el.Value.(*cacheItem)
	if item.head != head {
		return nil, cid.Undef, false // stale (another writer/process advanced head)
	}
	return item.tree, item.root, true
}

// put installs tree as the cached tree for did under head/root, which MUST be
// the head durably committed in postgres and the MST root it encodes. It
// replaces any existing entry for did and evicts the least-recently-used
// entries beyond the size cap.
func (c *mstTreeCache) put(did, head string, root cid.Cid, tree *mst.Tree) {
	if c == nil || c.max <= 0 || tree == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[did]; ok {
		c.ll.Remove(el)
		delete(c.entries, did)
	}
	el := c.ll.PushFront(&cacheItem{did: did, head: head, root: root, tree: tree})
	c.entries[did] = el
	for c.ll.Len() > c.max {
		back := c.ll.Back()
		if back == nil {
			break
		}
		c.ll.Remove(back)
		delete(c.entries, back.Value.(*cacheItem).did)
	}
}

// len reports the number of cached trees (test/observability helper).
func (c *mstTreeCache) len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}
