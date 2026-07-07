package repo

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"

	blockformat "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multihash"
)

// dagCBORPrefix is the CID shape every repo block uses: CIDv1, dag-cbor,
// sha2-256.
var dagCBORPrefix = cid.Prefix{
	Version:  1,
	Codec:    cid.DagCBOR,
	MhType:   multihash.SHA2_256,
	MhLength: 32,
}

// cidForBlock computes the dag-cbor CID for raw block bytes.
func cidForBlock(data []byte) (cid.Cid, error) {
	c, err := dagCBORPrefix.Sum(data)
	if err != nil {
		return cid.Undef, fmt.Errorf("repo: compute cid: %w", err)
	}
	return c, nil
}

// txBlockSource reads a single DID's blocks from postgres inside the commit
// transaction. It implements mst.MSTBlockSource / repo.RepoBlockSource
// (Get only). Misses return ipld.ErrNotFound so indigo's tree loader treats
// them per its own semantics — for our own repos every block is expected to
// be present, and a partial tree will surface as mst.ErrPartialTree on the
// next mutation.
type txBlockSource struct {
	tx  *sql.Tx
	did string
}

func (s *txBlockSource) Get(ctx context.Context, c cid.Cid) (blockformat.Block, error) {
	var raw []byte
	err := s.tx.QueryRowContext(ctx,
		`SELECT bytes FROM blocks WHERE did = $1 AND cid = $2`,
		s.did, c.String()).Scan(&raw)
	if stderrors.Is(err, sql.ErrNoRows) {
		return nil, ipld.ErrNotFound{Cid: c}
	}
	if err != nil {
		return nil, fmt.Errorf("repo: read block %s for %s: %w", c, s.did, err)
	}
	return blockformat.NewBlockWithCid(raw, c)
}

// memBlockstore is an ordered, in-memory blockstore capturing the blocks a
// commit produces (MST diff nodes via WriteDiffBlocks, plus record and
// commit blocks added directly). It implements the full
// go-ipfs-blockstore.Blockstore interface because indigo's WriteDiffBlocks
// asks for it, but only Put/Get/Has see real use.
type memBlockstore struct {
	order  []cid.Cid
	blocks map[cid.Cid]blockformat.Block
}

func newMemBlockstore() *memBlockstore {
	return &memBlockstore{blocks: make(map[cid.Cid]blockformat.Block)}
}

// ordered returns the captured blocks in insertion order.
func (m *memBlockstore) ordered() []blockformat.Block {
	out := make([]blockformat.Block, 0, len(m.order))
	for _, c := range m.order {
		out = append(out, m.blocks[c])
	}
	return out
}

func (m *memBlockstore) Put(_ context.Context, blk blockformat.Block) error {
	c := blk.Cid()
	if _, ok := m.blocks[c]; !ok {
		m.order = append(m.order, c)
		m.blocks[c] = blk
	}
	return nil
}

func (m *memBlockstore) PutMany(ctx context.Context, blks []blockformat.Block) error {
	for _, blk := range blks {
		if err := m.Put(ctx, blk); err != nil {
			return err
		}
	}
	return nil
}

func (m *memBlockstore) Get(_ context.Context, c cid.Cid) (blockformat.Block, error) {
	blk, ok := m.blocks[c]
	if !ok {
		return nil, ipld.ErrNotFound{Cid: c}
	}
	return blk, nil
}

func (m *memBlockstore) Has(_ context.Context, c cid.Cid) (bool, error) {
	_, ok := m.blocks[c]
	return ok, nil
}

func (m *memBlockstore) GetSize(_ context.Context, c cid.Cid) (int, error) {
	blk, ok := m.blocks[c]
	if !ok {
		return 0, ipld.ErrNotFound{Cid: c}
	}
	return len(blk.RawData()), nil
}

func (m *memBlockstore) DeleteBlock(_ context.Context, c cid.Cid) error {
	if _, ok := m.blocks[c]; ok {
		delete(m.blocks, c)
		for i, oc := range m.order {
			if oc.Equals(c) {
				m.order = append(m.order[:i], m.order[i+1:]...)
				break
			}
		}
	}
	return nil
}

func (m *memBlockstore) AllKeysChan(_ context.Context) (<-chan cid.Cid, error) {
	ch := make(chan cid.Cid, len(m.order))
	for _, c := range m.order {
		ch <- c
	}
	close(ch)
	return ch, nil
}

func (m *memBlockstore) HashOnRead(bool) {}
