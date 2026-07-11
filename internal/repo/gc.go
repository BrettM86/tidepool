package repo

// Blocks garbage collection (task 12). The commit path writes `blocks`
// append-only; GCBlocks below is the ONLY deleter. Before it existed the
// table simply grew forever ("append-only is load-bearing", task 03) — this
// file replaces that blanket rule with an explicit invariant every reader
// was audited against.
//
// THE INVARIANT: a block may be deleted only if it is BOTH
//
//	(a) unreachable from its repo's current head — not the head commit
//	    block, not a node of the head's live MST, not a record the live
//	    tree references — as computed in one REPEATABLE READ snapshot, AND
//	(b) older than the retention cutoff (blocks.created_at < cutoff),
//	    re-checked inside the DELETE statement itself.
//
// Why that is sufficient for every consumer of `blocks`:
//
//   - GetRecord / GetRecordProof / ExportCARTo read the head pointer and
//     walk its blocks inside a single REPEATABLE READ snapshot. A GC DELETE
//     committing after that snapshot began is invisible to it (MVCC). A GC
//     that committed before it only removed blocks unreachable from the
//     head at GC time — and any block reachable from a LATER head was
//     (re-)written by an intervening commit, which refreshes created_at
//     (the ON CONFLICT DO UPDATE in commitWrite), so rule (b) kept it.
//   - subscribeRepos replay reads firehose_events.car, which is
//     self-contained (commit + diff + record bytes inline) and never
//     touches `blocks`. Pruning superseded blocks cannot break replay.
//   - The commit path itself loads the tree behind the current head, whose
//     blocks are all reachable — rule (a) never touches them.
//   - A commit racing the sweep can make a previously-unreachable block
//     reachable again only by re-writing it: every block a commit NEWLY
//     references — anything not already reachable from its parent head —
//     is dirty in the MST diff or is the put's record block, so it IS in
//     newBlocks. (Blocks the commit references but does NOT re-write were
//     already reachable from the parent head, so rule (a) never made them
//     victims.) The re-write stamps created_at = clock_timestamp() in the
//     commit transaction. The DELETE re-evaluates created_at < cutoff
//     under READ COMMITTED row-lock semantics, so whichever way the two
//     serialize the block survives. This makes the retention window the
//     race guard: it only has to exceed one sweep's compute→delete gap
//     (seconds) and it defaults to days. One more assumption rides on it:
//     the cutoff comes from the APP clock (prune.Run passes
//     time.Now()-retention) while the refresh uses the DB's
//     clock_timestamp(), so app↔DB clock skew must stay small relative to
//     the retention window — retention must comfortably exceed skew plus
//     the compute→delete gap (the 72h default dwarfs both).
//
// If getRepo ever grows a real `since` diff export served from `blocks`
// history (rather than firehose_events), that consumer breaks rule (a)'s
// "current head only" audit and this GC must learn about it first.

import (
	"bytes"
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"slices"
	"time"

	indigorepo "github.com/bluesky-social/indigo/atproto/repo"

	"github.com/ipfs/go-cid"
	"github.com/lib/pq"

	"tidepool/internal/errors"
)

// gcDeleteBatchSize bounds one DELETE statement so a large unreachable
// backlog (first sweep after enabling GC) never holds row locks for a whole
// sweep — same discipline as pruneEventsBatchSize.
const gcDeleteBatchSize = 1000

// GCBlocks deletes blocks that are unreachable from their repo's current
// head AND older than cutoff (see the file-header invariant), returning the
// number deleted. It matches prune.Func and is wired through the shared
// internal/prune runner, which fails closed on a non-positive retention.
// One repo's failure (corrupt tree, vanished state) is logged and does not
// stop the sweep for the other repos; the joined error is returned so the
// runner still reports the sweep as failed.
func (m *Manager) GCBlocks(ctx context.Context, cutoff time.Time) (int64, error) {
	rows, err := m.db.QueryContext(ctx, `SELECT did FROM repo_state ORDER BY did`)
	if err != nil {
		return 0, fmt.Errorf("repo: list repos for blocks GC: %w", err)
	}
	var dids []string
	for rows.Next() {
		var did string
		if err := rows.Scan(&did); err != nil {
			rows.Close()
			return 0, fmt.Errorf("repo: scan repo did for blocks GC: %w", err)
		}
		dids = append(dids, did)
	}
	if err := stderrors.Join(rows.Err(), rows.Close()); err != nil {
		return 0, fmt.Errorf("repo: iterate repos for blocks GC: %w", err)
	}

	var total int64
	var errs []error
	for _, did := range dids {
		n, err := m.gcRepoBlocks(ctx, did, cutoff)
		total += n
		if err != nil {
			if ctx.Err() != nil {
				errs = append(errs, err)
				break
			}
			m.logger.Error("blocks GC failed for repo", "did", did, "error", err)
			errs = append(errs, fmt.Errorf("%s: %w", did, err))
		}
	}
	return total, stderrors.Join(errs...)
}

// gcRepoBlocks sweeps one repo: compute the victim set in a snapshot
// (unreachableBlocks), then delete it in short batches outside that snapshot
// (deleteUnreachable). The two phases are separate methods so tests can
// interleave a re-introducing commit between them and pin the DELETE-time
// created_at re-check.
func (m *Manager) gcRepoBlocks(ctx context.Context, did string, cutoff time.Time) (int64, error) {
	victims, err := m.unreachableBlocks(ctx, did, cutoff)
	if err != nil || len(victims) == 0 {
		return 0, err
	}
	return m.deleteUnreachable(ctx, did, victims, cutoff)
}

// deleteUnreachable is gcRepoBlocks' delete phase: remove the
// previously-computed victims in bounded batches, re-checking created_at
// against cutoff inside each DELETE (rule (b) of the invariant).
func (m *Manager) deleteUnreachable(ctx context.Context, did string, victims []string, cutoff time.Time) (int64, error) {
	// Sorted for determinism and debuggability (reproducible batch contents
	// across runs) — NOT a lock-ordering guarantee: within one
	// DELETE ... WHERE cid = ANY($2), postgres takes row locks in executor
	// scan order, not array order, so a deadlock against a concurrent
	// commit's inserts (or another sweep) stays possible in theory. Postgres
	// aborts one side, the sweep reports the error, and the next run
	// self-heals — that retry, not the sort, is the real guarantee.
	slices.Sort(victims)

	var total int64
	for start := 0; start < len(victims); start += gcDeleteBatchSize {
		batch := victims[start:min(start+gcDeleteBatchSize, len(victims))]
		// created_at < cutoff re-checked HERE, not just in the snapshot: this
		// is rule (b) of the invariant. A commit that re-introduced a victim
		// block since the snapshot refreshed its created_at, and the DELETE's
		// row-lock re-evaluation sees that refresh — the block survives.
		res, err := m.db.ExecContext(ctx, `
			DELETE FROM blocks WHERE did = $1 AND cid = ANY($2) AND created_at < $3`,
			did, pq.Array(batch), cutoff)
		if err != nil {
			return total, fmt.Errorf("repo: delete unreachable blocks for %s: %w", did, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return total, fmt.Errorf("repo: delete unreachable blocks for %s: rows affected: %w", did, err)
		}
		total += n
	}
	return total, nil
}

// unreachableBlocks returns the CIDs of blocks for did that are both older
// than cutoff and unreachable from the current head. The head read, the
// reachable-set walk, and the candidate listing all share one REPEATABLE
// READ snapshot, so the set is internally consistent; staleness against
// concurrent commits is what the created_at re-check in deleteUnreachable
// (and the commit path's created_at refresh) absorbs.
func (m *Manager) unreachableBlocks(ctx context.Context, did string, cutoff time.Time) ([]string, error) {
	tx, err := m.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return nil, fmt.Errorf("repo: begin read tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	state, err := readRepoState(ctx, tx, did, false)
	if errors.IsNotFound(err) {
		// Repo vanished between the sweep's listing and now; its blocks are
		// left alone (conservative — nothing without a head is ever swept).
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	head, err := cid.Parse(state.headCID)
	if err != nil {
		return nil, fmt.Errorf("repo: parse head cid %q for %s: %w", state.headCID, did, err)
	}
	src := &txBlockSource{tx: tx, did: did}
	headBlk, err := src.Get(ctx, head)
	if err != nil {
		return nil, fmt.Errorf("repo: read head commit %s for %s: %w", state.headCID, did, err)
	}
	var commit indigorepo.Commit
	if err := commit.UnmarshalCBOR(bytes.NewReader(headBlk.RawData())); err != nil {
		return nil, fmt.Errorf("repo: decode head commit %s for %s: %w", state.headCID, did, err)
	}

	reachable := map[string]struct{}{head.String(): {}}
	// The walk populates `reachable` (its seen set) as a side effect; record
	// bytes are never fetched — GC only needs the CIDs.
	if err := walkReachable(ctx, src, commit.Data, reachable,
		func(cid.Cid, []byte) error { return nil },
		func([]cid.Cid) error { return nil }); err != nil {
		return nil, err
	}

	rows, err := tx.QueryContext(ctx,
		`SELECT cid FROM blocks WHERE did = $1 AND created_at < $2`, did, cutoff)
	if err != nil {
		return nil, fmt.Errorf("repo: list expired blocks for %s: %w", did, err)
	}
	defer rows.Close()
	var victims []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, fmt.Errorf("repo: scan expired block for %s: %w", did, err)
		}
		if _, ok := reachable[c]; !ok {
			victims = append(victims, c)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repo: iterate expired blocks for %s: %w", did, err)
	}
	return victims, nil
}
