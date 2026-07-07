package repo

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	indigorepo "github.com/bluesky-social/indigo/atproto/repo"

	blockformat "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	car "github.com/ipld/go-car"
	carutil "github.com/ipld/go-car/util"
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
	if err := tx.QueryRowContext(ctx, `
		INSERT INTO firehose_events (did, commit_cid, prev_data_cid, since_rev, rev, ops, car)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING seq`,
		ev.did, ev.commitCID.String(), prevData, sinceRev, ev.rev, opsJSON, carSlice).Scan(&seq); err != nil {
		return 0, fmt.Errorf("repo: append firehose event for %s: %w", ev.did, err)
	}
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

// ExportCAR writes the DID's full repo as a CARv1 stream rooted at the
// current head commit. NOTE (task 04): until block garbage collection
// exists, this includes every historical block for the DID, not just the
// reachable set — harmless for CAR readers (they traverse from the root)
// but larger than a minimal export.
func (m *Manager) ExportCAR(ctx context.Context, did string) ([]byte, error) {
	tx, err := m.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("repo: begin read tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	state, err := readRepoState(ctx, tx, did, false)
	if err != nil {
		return nil, err
	}
	head, err := cid.Parse(state.headCID)
	if err != nil {
		return nil, fmt.Errorf("repo: parse head cid %q for %s: %w", state.headCID, did, err)
	}

	var buf bytes.Buffer
	if err := car.WriteHeader(&car.CarHeader{Roots: []cid.Cid{head}, Version: 1}, &buf); err != nil {
		return nil, fmt.Errorf("repo: write CAR header: %w", err)
	}

	// Head commit block first (readers conventionally expect the root
	// early), then everything else.
	src := &txBlockSource{tx: tx, did: did}
	headBlk, err := src.Get(ctx, head)
	if err != nil {
		return nil, fmt.Errorf("repo: read head commit %s for %s: %w", state.headCID, did, err)
	}
	if err := carutil.LdWrite(&buf, headBlk.Cid().Bytes(), headBlk.RawData()); err != nil {
		return nil, fmt.Errorf("repo: write CAR block %s: %w", head, err)
	}

	rows, err := tx.QueryContext(ctx,
		`SELECT cid, bytes FROM blocks WHERE did = $1 AND cid <> $2 ORDER BY created_at, cid`,
		did, state.headCID)
	if err != nil {
		return nil, fmt.Errorf("repo: list blocks for %s: %w", did, err)
	}
	defer rows.Close()
	for rows.Next() {
		var cidStr string
		var raw []byte
		if err := rows.Scan(&cidStr, &raw); err != nil {
			return nil, fmt.Errorf("repo: scan block for %s: %w", did, err)
		}
		c, err := cid.Parse(cidStr)
		if err != nil {
			return nil, fmt.Errorf("repo: parse stored cid %q for %s: %w", cidStr, did, err)
		}
		if err := carutil.LdWrite(&buf, c.Bytes(), raw); err != nil {
			return nil, fmt.Errorf("repo: write CAR block %s: %w", c, err)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repo: iterate blocks for %s: %w", did, err)
	}
	return buf.Bytes(), nil
}
