package repo

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	indigorepo "github.com/bluesky-social/indigo/atproto/repo"
	"github.com/bluesky-social/indigo/atproto/repo/mst"
	"github.com/bluesky-social/indigo/atproto/syntax"

	blockformat "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	car "github.com/ipld/go-car"
	carutil "github.com/ipld/go-car/util"

	"tidepool/internal/errors"
)

// OpAction is the kind of record mutation an Op describes. The values
// match the com.atproto.sync.subscribeRepos#repoOp action enum.
type OpAction string

const (
	OpActionCreate OpAction = "create"
	OpActionUpdate OpAction = "update"
	OpActionDelete OpAction = "delete"
)

// Op is one record mutation inside a commit, stored as JSON in
// firehose_events.ops. Task 04 maps these onto
// com.atproto.sync.subscribeRepos#repoOp (action/path/cid, with prev
// carrying the sync-v1.1 previous record CID for updates and deletes).
type Op struct {
	Action OpAction `json:"action"`
	Path   string   `json:"path"` // {collection}/{rkey}
	CID    string   `json:"cid,omitempty"`
	Prev   string   `json:"prev,omitempty"`
}

// opFromIndigoOp converts indigo's Operation to the stored form.
func opFromIndigoOp(op *indigorepo.Operation) Op {
	out := Op{Path: op.Path}
	switch {
	case op.IsCreate():
		out.Action = OpActionCreate
	case op.IsUpdate():
		out.Action = OpActionUpdate
	case op.IsDelete():
		out.Action = OpActionDelete
	}
	if op.Value != nil {
		out.CID = op.Value.String()
	}
	if op.Prev != nil {
		out.Prev = op.Prev.String()
	}
	return out
}

// firehoseEvent is one row of the durable subscribeRepos backlog.
type firehoseEvent struct {
	did       string
	commitCID cid.Cid
	prevData  *cid.Cid // MST root before this commit; nil on genesis
	sinceRev  string   // previous commit's rev; empty on genesis → NULL
	rev       string
	ops       []Op
	// blocks is the CAR slice content: the blocks new in this commit
	// (MST diff nodes, record blocks, and the commit block itself).
	blocks []blockformat.Block
}

// appendFirehoseEvent inserts the event row inside the commit transaction —
// commit and event are atomic by construction, so the stream can never miss
// a commit or observe one that later rolled back. It returns the seq cursor
// postgres assigned to the event.
func appendFirehoseEvent(ctx context.Context, tx *sql.Tx, ev firehoseEvent) (int64, error) {
	carSlice, err := writeCARSlice(ev.commitCID, ev.blocks)
	if err != nil {
		return 0, err
	}
	opsJSON, err := json.Marshal(ev.ops)
	if err != nil {
		return 0, fmt.Errorf("repo: marshal ops: %w", err)
	}
	var prevData *string
	if ev.prevData != nil {
		s := ev.prevData.String()
		prevData = &s
	}
	var sinceRev *string
	if ev.sinceRev != "" {
		sinceRev = &ev.sinceRev
	}
	var seq int64
	// created_at uses clock_timestamp(), not the CURRENT_TIMESTAMP default:
	// the default is transaction-start time, and a commit that waited on the
	// global advisory lock would be stamped older than it became visible —
	// skewing the retention pruner's age math against late-visible events.
	if err := tx.QueryRowContext(ctx, `
		INSERT INTO firehose_events (did, commit_cid, prev_data_cid, since_rev, rev, ops, car, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, clock_timestamp())
		RETURNING seq`,
		ev.did, ev.commitCID.String(), prevData, sinceRev, ev.rev, opsJSON, carSlice).Scan(&seq); err != nil {
		return 0, fmt.Errorf("repo: append firehose event for %s: %w", ev.did, err)
	}
	// Wake the subscribeRepos broadcaster (task 04). pg_notify inside the
	// commit transaction means the notification is delivered exactly when
	// the event becomes visible — and, because the global commit advisory
	// lock is still held here, notifications arrive in seq order.
	if _, err := tx.ExecContext(ctx,
		`SELECT pg_notify($1, $2)`, FirehoseNotifyChannel, strconv.FormatInt(seq, 10)); err != nil {
		return 0, fmt.Errorf("repo: notify firehose event %d: %w", seq, err)
	}
	return seq, nil
}

// AppendAccountEvent appends an #account row to the firehose log: the
// account-state signal (active=false + status) subscribers need to purge a
// repo instead of inferring its death from scrub delete-commits. It takes
// the same global commit advisory lock as record commits, so account rows
// share the "seq order == visibility order" guarantee and can never
// interleave incorrectly with the scrub commits that precede them. status
// uses the com.atproto account-status vocabulary (AccountStatusDeleted for
// the terminal consent flip); it is stored only when active is false.
func (m *Manager) AppendAccountEvent(ctx context.Context, did string, active bool, status string) (int64, error) {
	if _, err := syntax.ParseDID(did); err != nil {
		return 0, errors.NewValidationError("did", err.Error())
	}

	lock := m.lockFor(did)
	lock.Lock()
	defer lock.Unlock()

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("repo: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx,
		`SELECT pg_advisory_xact_lock($1)`, commitAdvisoryLockKey); err != nil {
		return 0, fmt.Errorf("repo: take commit advisory lock: %w", err)
	}

	var statusValue any
	if !active && status != "" {
		statusValue = status
	}
	var seq int64
	// clock_timestamp() for the same reason as commit events: stamp
	// visibility time, not transaction start (see appendFirehoseEvent).
	if err := tx.QueryRowContext(ctx, `
		INSERT INTO firehose_events (kind, did, account_active, account_status, created_at)
		VALUES ('account', $1, $2, $3, clock_timestamp())
		RETURNING seq`,
		did, active, statusValue).Scan(&seq); err != nil {
		return 0, fmt.Errorf("repo: append account event for %s: %w", did, err)
	}
	if _, err := tx.ExecContext(ctx,
		`SELECT pg_notify($1, $2)`, FirehoseNotifyChannel, strconv.FormatInt(seq, 10)); err != nil {
		return 0, fmt.Errorf("repo: notify account event %d: %w", seq, err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("repo: commit account event for %s: %w", did, err)
	}
	m.logger.Info("account event appended to firehose",
		"did", did, "active", active, "status", status, "seq", seq)
	return seq, nil
}

// writeCARSlice serializes blocks as a CARv1 stream rooted at the commit
// CID — exactly the `blocks` payload of a subscribeRepos #commit frame.
// The commit (root) block is written first — atproto CAR consumers
// conventionally expect the root at the front — followed by the remaining
// blocks in their original deterministic order.
func writeCARSlice(root cid.Cid, blks []blockformat.Block) ([]byte, error) {
	var buf bytes.Buffer
	if err := car.WriteHeader(&car.CarHeader{
		Roots:   []cid.Cid{root},
		Version: 1,
	}, &buf); err != nil {
		return nil, fmt.Errorf("repo: write CAR header: %w", err)
	}
	for _, blk := range blks {
		if blk.Cid().Equals(root) {
			if err := carutil.LdWrite(&buf, blk.Cid().Bytes(), blk.RawData()); err != nil {
				return nil, fmt.Errorf("repo: write CAR block %s: %w", blk.Cid(), err)
			}
		}
	}
	for _, blk := range blks {
		if blk.Cid().Equals(root) {
			continue
		}
		if err := carutil.LdWrite(&buf, blk.Cid().Bytes(), blk.RawData()); err != nil {
			return nil, fmt.Errorf("repo: write CAR block %s: %w", blk.Cid(), err)
		}
	}
	return buf.Bytes(), nil
}

// ExportCAR writes the DID's full repo as a CARv1 byte slice rooted at the
// current head commit — the reachable set only (commit + MST nodes + record
// blocks), the same content ExportCARTo streams. It buffers the whole CAR in
// memory; the sync surface uses ExportCARTo to stream instead. A missing repo
// satisfies errors.IsNotFound.
func (m *Manager) ExportCAR(ctx context.Context, did string) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.ExportCARTo(ctx, did, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ExportCARTo streams the DID's repo as a CARv1 stream rooted at the current
// head commit, writing blocks to w as they are fetched so memory stays
// bounded by one fetch batch (walkBatchSize blocks) plus a CID-string
// seen-set that grows with the reachable set, rather than the whole CAR
// (com.atproto.sync.getRepo can serve a large community repo without
// buffering it). It exports the REACHABLE SET from the
// current head — the commit block, every MST node on the live tree, and every
// record block those nodes reference — NOT every historical block ever stored
// for the DID. Superseded MST nodes and old record versions are omitted:
// correct CAR consumers (indigo's LoadRepoFromCAR, bigsky) traverse from the
// root commit and never look at unreachable blocks, so the export is smaller
// but semantically identical.
//
// This reachable-set export is independent of the read-consistency guarantees
// GetRecord/GetRecordProof and subscribeRepos replay rely on: those read the
// current head's blocks (append-only, content-addressed) and the self-
// contained firehose_events.car respectively — neither depends on the
// unreachable historical blocks omitted here. The read runs in a REPEATABLE
// READ snapshot so a concurrent blocks GC (which deletes only unreachable
// blocks) can never pull a block out from under the walk.
//
// Failure ordering: the repo-state read, head-CID parse, head-block fetch,
// and commit decode all happen before the first byte is written, so a missing
// repo or an unreadable/undecodable head commit returns an error with nothing
// written to w. The reachable walk itself (batched block fetches, MST node
// decodes, the missing-block corruption check) runs after bytes are flowing:
// any failure there truncates the stream mid-write, and the HTTP handler is
// responsible for making that visible to the client (it cannot be turned into
// a clean error response once the header is out).
func (m *Manager) ExportCARTo(ctx context.Context, did string, w io.Writer) error {
	tx, err := m.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return fmt.Errorf("repo: begin read tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	state, err := readRepoState(ctx, tx, did, false)
	if err != nil {
		return err
	}
	head, err := cid.Parse(state.headCID)
	if err != nil {
		return fmt.Errorf("repo: parse head cid %q for %s: %w", state.headCID, did, err)
	}
	src := &txBlockSource{tx: tx, did: did}
	headBlk, err := src.Get(ctx, head)
	if err != nil {
		return fmt.Errorf("repo: read head commit %s for %s: %w", state.headCID, did, err)
	}
	var commit indigorepo.Commit
	if err := commit.UnmarshalCBOR(bytes.NewReader(headBlk.RawData())); err != nil {
		return fmt.Errorf("repo: decode head commit %s for %s: %w", state.headCID, did, err)
	}

	// From here on we write to w; any error truncates the stream.
	if err := car.WriteHeader(&car.CarHeader{Roots: []cid.Cid{head}, Version: 1}, w); err != nil {
		return fmt.Errorf("repo: write CAR header: %w", err)
	}
	// Head commit block first (readers conventionally expect the root early).
	if err := carutil.LdWrite(w, head.Bytes(), headBlk.RawData()); err != nil {
		return fmt.Errorf("repo: write CAR block %s: %w", head, err)
	}
	// seen dedupes blocks (a record CID can appear under multiple keys, and an
	// MST subtree could be shared): it holds only CID strings, not bytes, so
	// memory stays proportional to the number of distinct reachable blocks.
	seen := map[string]struct{}{head.String(): {}}
	return walkReachableBlocks(ctx, src, commit.Data, seen, w)
}

// walkReachableBlocks streams the MST rooted at node — node blocks, then the
// record blocks their value entries reference, level by level — to w in CARv1
// LdWrite framing, skipping anything already emitted (seen).
func walkReachableBlocks(ctx context.Context, src *txBlockSource, node cid.Cid, seen map[string]struct{}, w io.Writer) error {
	writeBlock := func(c cid.Cid, raw []byte) error {
		if err := carutil.LdWrite(w, c.Bytes(), raw); err != nil {
			return fmt.Errorf("repo: write CAR block %s: %w", c, err)
		}
		return nil
	}
	return walkReachable(ctx, src, node, seen, writeBlock,
		func(records []cid.Cid) error {
			return forEachBlock(ctx, src, records, writeBlock)
		})
}

// walkBatchSize is how many blocks one walk fetch pulls from postgres. The
// reachable-set walk is round-trip bound (a 2k-record repo is ~2.7k blocks),
// so blocks are fetched in batches of this size — it is also the walk's
// memory bound: at most walkBatchSize block payloads are resident at once.
const walkBatchSize = 256

// walkReachable is the reachable-set walk ExportCARTo and GCBlocks share: a
// breadth-first walk over the MST rooted at node, fetching each level's node
// blocks in walkBatchSize batches. visitNode receives every node block (with
// its raw bytes, in batch order); visitRecords receives the record CIDs each
// level references — bytes deliberately not fetched, because GC only needs
// the CIDs (the export's visitRecords fetches them itself, batched). Every
// reachable CID (nodes and records) is added to seen, which both dedupes the
// walk and, for GC, IS the reachable set.
func walkReachable(ctx context.Context, src *txBlockSource, node cid.Cid, seen map[string]struct{},
	visitNode func(cid.Cid, []byte) error, visitRecords func([]cid.Cid) error) error {
	var level []cid.Cid
	if _, ok := seen[node.String()]; !ok {
		seen[node.String()] = struct{}{}
		level = append(level, node)
	}
	for len(level) > 0 {
		var children, records []cid.Cid
		err := forEachBlock(ctx, src, level, func(c cid.Cid, raw []byte) error {
			if err := visitNode(c, raw); err != nil {
				return err
			}
			nd, err := mst.NodeDataFromCBOR(bytes.NewReader(raw))
			if err != nil {
				return fmt.Errorf("repo: decode MST node %s: %w", c, err)
			}
			n := nd.Node(&c)
			for i := range n.Entries {
				e := &n.Entries[i]
				if e.IsValue() && e.Value != nil {
					if _, ok := seen[e.Value.String()]; !ok {
						seen[e.Value.String()] = struct{}{}
						records = append(records, *e.Value)
					}
				}
				if e.IsChild() && e.ChildCID != nil {
					if _, ok := seen[e.ChildCID.String()]; !ok {
						seen[e.ChildCID.String()] = struct{}{}
						children = append(children, *e.ChildCID)
					}
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		if len(records) > 0 {
			if err := visitRecords(records); err != nil {
				return err
			}
		}
		level = children
	}
	return nil
}

// forEachBlock fetches the given CIDs' bytes in walkBatchSize batches (one
// SELECT ... = ANY per batch) and invokes fn for each in the order given. A
// CID with no stored block is an error: the walk only asks for blocks the
// head's tree references, so a miss is corruption, never skippable.
func forEachBlock(ctx context.Context, src *txBlockSource, cids []cid.Cid, fn func(cid.Cid, []byte) error) error {
	for start := 0; start < len(cids); start += walkBatchSize {
		batch := cids[start:min(start+walkBatchSize, len(cids))]
		blocks, err := src.GetMany(ctx, batch)
		if err != nil {
			return err
		}
		for _, c := range batch {
			raw, ok := blocks[c.String()]
			if !ok {
				return fmt.Errorf("repo: block %s missing for %s (corrupt repo?)", c, src.did)
			}
			if err := fn(c, raw); err != nil {
				return err
			}
		}
	}
	return nil
}
