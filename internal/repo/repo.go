// Package repo maintains a real, signed atproto repository (Merkle Search
// Tree + commit chain) per bridged DID, persisted in postgres. It is the Go
// port of arroba's virtual-PDS job, built on indigo's atproto/repo and
// atproto/repo/mst primitives: every write produces a properly signed v3
// commit with a monotonically increasing rev TID, stores the new blocks,
// and appends a firehose event in the same transaction (task 04 serves
// those events over com.atproto.sync.subscribeRepos).
package repo

import (
	"bytes"
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"log/slog"
	"sync"

	indigorepo "github.com/bluesky-social/indigo/atproto/repo"
	"github.com/bluesky-social/indigo/atproto/repo/mst"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/bluesky-social/indigo/atproto/syntax"

	blockformat "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"tidepool/internal/errors"
)

// KeyUse says what a signing key is being requested for, so key custody
// can apply consent policy per operation kind: tombstoned (consent-revoked)
// actors are frozen for new writes but their records must remain deletable.
type KeyUse int

const (
	// KeyUseWrite covers record creates and updates.
	KeyUseWrite KeyUse = iota
	// KeyUseDelete covers record deletions — including scrubbing a
	// tombstoned actor's records, which must always be possible (that IS
	// the intent of consent revocation).
	KeyUseDelete
)

// SigningKeys resolves the commit-signing key for a bridged DID.
// identity.ActorKeys implements it: tombstoned actors' keys are never
// released for KeyUseWrite (errors.IsTombstoned), which is what freezes
// their repos, but KeyUseDelete still releases the key so task 05's
// Delete(Actor) → scrub-records flow works after a consent flip.
type SigningKeys interface {
	SigningKey(ctx context.Context, did string, use KeyUse) (atcrypto.PrivateKey, error)
}

// commitAdvisoryLockKey is the postgres advisory-lock key every commit
// transaction takes (pg_advisory_xact_lock) before reading repo_state. It
// is ONE GLOBAL lock — not per-DID — on purpose, buying two guarantees:
//
//  1. Genesis safety across processes: SELECT ... FOR UPDATE on a missing
//     repo_state row locks nothing, so without this two processes could
//     both build a genesis commit for the same DID and the head upsert
//     would silently overwrite one of them.
//  2. Firehose cursor safety: firehose_events.seq (bigserial) is assigned
//     at INSERT time, but rows become visible in transaction-commit order.
//     Serializing all commits makes seq order equal commit-visibility
//     order, so a task-04 tailer doing `WHERE seq > cursor` can never
//     permanently skip an event.
//
// Globally serializing commits is acceptable at bridge write volume
// (PLAN.md: one deployment, full control of emission order). The per-DID
// mutex and the repo_state row lock are kept as fast-path/backstop.
//
// This key MUST stay distinct from internal/testutil's cross-package test
// lock key (0x7469646570, session-scoped, held for a whole test binary) —
// sharing it would deadlock every commit made from tests.
const commitAdvisoryLockKey int64 = 0x7469646570636d // "tidep"+"cm" (commit)

// Manager owns all bridged repos. Writes take a per-DID in-process mutex
// (local fairness), then a global advisory transaction lock
// (commitAdvisoryLockKey) that serializes every commit across processes —
// that lock, not the repo_state row lock, is what makes genesis commits
// race-free and firehose seq order match commit-visibility order. The row
// lock (SELECT ... FOR UPDATE) is kept as a backstop.
type Manager struct {
	db     *sql.DB
	keys   SigningKeys
	logger *slog.Logger

	mu sync.Mutex
	// locks is never evicted: it is bounded by the number of bridged
	// actors, and a stale mutex per DID is 8 bytes of pointer.
	locks map[string]*sync.Mutex
}

// NewManager builds the repo manager. db and keys must be non-nil; a nil
// logger falls back to slog.Default().
func NewManager(db *sql.DB, keys SigningKeys, logger *slog.Logger) (*Manager, error) {
	if db == nil {
		return nil, errors.NewValidationError("db", "must not be nil")
	}
	if keys == nil {
		return nil, errors.NewValidationError("keys", "must not be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		db:     db,
		keys:   keys,
		logger: logger,
		locks:  make(map[string]*sync.Mutex),
	}, nil
}

// CommitResult reports what a successful PutRecord/DeleteRecord did.
// Tasks 04/05 consume it to build subscribeRepos frames and ap_objects
// mappings without re-querying.
type CommitResult struct {
	// RecordCID is the CID of the written record; empty for deletes.
	RecordCID string
	// CommitCID is the signed commit block's CID — the repo head after
	// this write (for a NoOp re-put, the pre-existing head).
	CommitCID string
	// Rev is the commit's rev TID (the pre-existing rev on NoOp).
	Rev string
	// Seq is the firehose_events cursor assigned to this commit's event.
	// Zero on NoOp: no event was emitted.
	Seq int64
	// NoOp marks the idempotent re-put path: the identical record already
	// existed, so no new commit or firehose event was produced.
	NoOp bool
}

// PutRecord creates or updates a record and commits the change. The first
// write to a DID creates its repo (genesis commit). Re-putting an identical
// record is an idempotent no-op: no new commit or firehose event is
// emitted, and the result carries NoOp with the existing CID, head, and rev
// (deterministic rkeys make re-ingestion hit this path on purpose).
func (m *Manager) PutRecord(ctx context.Context, did, collection, rkey string, record map[string]any) (*CommitResult, error) {
	if err := validateRecord(record); err != nil {
		return nil, err
	}
	recordBytes, err := atdata.MarshalCBOR(record)
	if err != nil {
		return nil, fmt.Errorf("repo: encode record: %w", err)
	}
	c, err := cidForBlock(recordBytes)
	if err != nil {
		return nil, err
	}
	return m.commitWrite(ctx, did, collection, rkey, &c, recordBytes)
}

// DeleteRecord removes a record and commits the change. A missing record —
// or a repo that does not exist yet — is an error satisfying
// errors.IsNotFound. The result's RecordCID is empty.
func (m *Manager) DeleteRecord(ctx context.Context, did, collection, rkey string) (*CommitResult, error) {
	return m.commitWrite(ctx, did, collection, rkey, nil, nil)
}

// GetRecord reads the current version of a record. Missing repo or record
// is an error satisfying errors.IsNotFound.
func (m *Manager) GetRecord(ctx context.Context, did, collection, rkey string) (record map[string]any, recordCID string, err error) {
	path, _, err := validatePath(did, collection, rkey)
	if err != nil {
		return nil, "", err
	}

	// Note this tx runs READ COMMITTED, i.e. per-statement snapshots — it
	// does NOT freeze one snapshot across the reads below. Consistency
	// actually rests on blocks being content-addressed and append-only:
	// once the head pointer is read, every block it references is immutable
	// and present. Future block GC must preserve that property for any head
	// a reader may still hold.
	tx, err := m.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, "", fmt.Errorf("repo: begin read tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	state, err := readRepoState(ctx, tx, did, false)
	if err != nil {
		return nil, "", err
	}
	tree, _, err := loadTree(ctx, tx, did, state.headCID)
	if err != nil {
		return nil, "", err
	}
	valCID, err := tree.Get([]byte(path))
	if err != nil {
		return nil, "", fmt.Errorf("repo: MST get %s: %w", path, err)
	}
	if valCID == nil {
		return nil, "", errors.NewNotFoundError("record", fmt.Sprintf("at://%s/%s", did, path))
	}
	src := &txBlockSource{tx: tx, did: did}
	blk, err := src.Get(ctx, *valCID)
	if err != nil {
		return nil, "", fmt.Errorf("repo: read record block %s: %w", valCID, err)
	}
	record, err = atdata.UnmarshalCBOR(blk.RawData())
	if err != nil {
		return nil, "", fmt.Errorf("repo: decode record %s: %w", valCID, err)
	}
	return record, valCID.String(), nil
}

// Head returns the current head commit CID and rev for a DID. A repo that
// has never committed satisfies errors.IsNotFound.
func (m *Manager) Head(ctx context.Context, did string) (headCID string, rev string, err error) {
	state, err := m.readState(ctx, did)
	if err != nil {
		return "", "", err
	}
	return state.headCID, state.rev, nil
}

type repoState struct {
	headCID string
	rev     string
}

func (m *Manager) readState(ctx context.Context, did string) (*repoState, error) {
	var st repoState
	err := m.db.QueryRowContext(ctx,
		`SELECT head_cid, rev FROM repo_state WHERE did = $1`, did).Scan(&st.headCID, &st.rev)
	if stderrors.Is(err, sql.ErrNoRows) {
		return nil, errors.NewNotFoundError("repo", did)
	}
	if err != nil {
		return nil, fmt.Errorf("repo: read repo_state for %s: %w", did, err)
	}
	return &st, nil
}

// commitWrite is the single write path: PutRecord passes the new record CID
// and bytes, DeleteRecord passes nil. It serializes on the per-DID mutex
// and the global commit advisory lock, applies the mutation to the MST,
// signs a new commit, and persists blocks, head, and the firehose event in
// one transaction.
func (m *Manager) commitWrite(ctx context.Context, did, collection, rkey string, newCID *cid.Cid, recordBytes []byte) (*CommitResult, error) {
	path, parsedDID, err := validatePath(did, collection, rkey)
	if err != nil {
		return nil, err
	}
	use := KeyUseWrite
	if newCID == nil {
		use = KeyUseDelete
	}

	lock := m.lockFor(did)
	lock.Lock()
	defer lock.Unlock()

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("repo: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Global commit serialization; see commitAdvisoryLockKey. Released
	// automatically when the transaction commits or rolls back.
	if _, err := tx.ExecContext(ctx,
		`SELECT pg_advisory_xact_lock($1)`, commitAdvisoryLockKey); err != nil {
		return nil, fmt.Errorf("repo: take commit advisory lock: %w", err)
	}

	// The signing-key fetch is also the consent gate: tombstoned actors get
	// no key for writes (their repos are frozen), while deletes still get
	// one (scrubbing must survive a consent flip). It runs with the locks
	// held to narrow the TOCTOU against a concurrent consent change — but a
	// flip racing an in-flight commit still has a residual one-commit
	// window, because the consent row is read outside this transaction.
	// Full elimination needs the consent read inside the same tx (deferred).
	signingKey, err := m.keys.SigningKey(ctx, did, use)
	if err != nil {
		return nil, err
	}

	state, err := readRepoState(ctx, tx, did, true)
	if err != nil && !errors.IsNotFound(err) {
		return nil, err
	}

	var tree *mst.Tree
	var prevRev string
	var prevData *cid.Cid // MST root before this commit (firehose prevData)
	if state == nil {
		if newCID == nil {
			return nil, errors.NewNotFoundError("record", fmt.Sprintf("at://%s/%s", did, path))
		}
		empty := mst.NewEmptyTree()
		tree = &empty
	} else {
		prevRev = state.rev
		var headData cid.Cid
		tree, headData, err = loadTree(ctx, tx, did, state.headCID)
		if err != nil {
			return nil, err
		}
		prevData = &headData
	}

	// Note: indigo's mst.Tree.Remove returns (nil, nil) for a missing key —
	// NOT an error — so any ApplyOp error here is real corruption
	// (mst.ErrPartialTree etc.) and must surface as an internal error.
	// Missing-record deletes are detected by op.Prev == nil below.
	op, err := indigorepo.ApplyOp(tree, path, newCID)
	if err != nil {
		return nil, fmt.Errorf("repo: apply op %s: %w", path, err)
	}
	if newCID == nil && op.Prev == nil {
		return nil, errors.NewNotFoundError("record", fmt.Sprintf("at://%s/%s", did, path))
	}
	if newCID != nil && op.Prev != nil && op.Prev.Equals(*newCID) {
		// Identical re-put: idempotent no-op, keep the existing commit.
		// op.Prev != nil implies the repo exists, so state is non-nil here.
		return &CommitResult{
			RecordCID: newCID.String(),
			CommitCID: state.headCID,
			Rev:       prevRev,
			NoOp:      true,
		}, nil
	}

	// New blocks this commit introduces: MST diff nodes + the record block
	// (for puts) + the commit block, captured in order for the CAR slice.
	newBlocks := newMemBlockstore()
	newRoot, err := tree.WriteDiffBlocks(ctx, newBlocks)
	if err != nil {
		return nil, fmt.Errorf("repo: write MST diff: %w", err)
	}

	if newCID != nil {
		blk, err := blockformat.NewBlockWithCid(recordBytes, *newCID)
		if err != nil {
			return nil, fmt.Errorf("repo: build record block: %w", err)
		}
		if err := newBlocks.Put(ctx, blk); err != nil {
			return nil, err
		}
	}

	rev, err := NextRev(prevRev)
	if err != nil {
		return nil, fmt.Errorf("repo: next rev after %q for %s: %w", prevRev, did, err)
	}

	commit := indigorepo.Commit{
		DID:     parsedDID.String(),
		Version: indigorepo.ATPROTO_REPO_VERSION,
		Prev:    nil, // v3 commits carry no prev pointer; prevData rides the firehose event
		Data:    *newRoot,
		Rev:     rev.String(),
	}
	if err := commit.Sign(signingKey); err != nil {
		return nil, fmt.Errorf("repo: sign commit for %s: %w", did, err)
	}
	var commitBuf bytes.Buffer
	if err := commit.MarshalCBOR(&commitBuf); err != nil {
		return nil, fmt.Errorf("repo: encode commit: %w", err)
	}
	commitCID, err := cidForBlock(commitBuf.Bytes())
	if err != nil {
		return nil, err
	}
	commitBlock, err := blockformat.NewBlockWithCid(commitBuf.Bytes(), commitCID)
	if err != nil {
		return nil, fmt.Errorf("repo: build commit block: %w", err)
	}
	if err := newBlocks.Put(ctx, commitBlock); err != nil {
		return nil, err
	}

	for _, blk := range newBlocks.ordered() {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO blocks (did, cid, bytes) VALUES ($1, $2, $3) ON CONFLICT (did, cid) DO NOTHING`,
			did, blk.Cid().String(), blk.RawData()); err != nil {
			return nil, fmt.Errorf("repo: store block %s: %w", blk.Cid(), err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO repo_state (did, head_cid, rev) VALUES ($1, $2, $3)
		ON CONFLICT (did) DO UPDATE SET
			head_cid = EXCLUDED.head_cid,
			rev = EXCLUDED.rev,
			updated_at = CURRENT_TIMESTAMP`,
		did, commitCID.String(), rev.String()); err != nil {
		return nil, fmt.Errorf("repo: update repo_state for %s: %w", did, err)
	}

	// The firehose event rides the same transaction: a commit either
	// appears on the stream exactly once or does not exist at all.
	seq, err := appendFirehoseEvent(ctx, tx, firehoseEvent{
		did:       did,
		commitCID: commitCID,
		prevData:  prevData,
		sinceRev:  prevRev, // empty on genesis → NULL
		rev:       rev.String(),
		ops:       []Op{opFromIndigoOp(op)},
		blocks:    newBlocks.ordered(),
	})
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("repo: commit tx for %s: %w", did, err)
	}

	m.logger.Debug("repo commit",
		"did", did, "rev", rev.String(), "commit", commitCID.String(), "path", path, "seq", seq)
	res := &CommitResult{
		CommitCID: commitCID.String(),
		Rev:       rev.String(),
		Seq:       seq,
	}
	if newCID != nil {
		res.RecordCID = newCID.String()
	}
	return res, nil
}

// lockFor returns the per-DID write mutex, creating it on first use.
func (m *Manager) lockFor(did string) *sync.Mutex {
	m.mu.Lock()
	defer m.mu.Unlock()
	lock, ok := m.locks[did]
	if !ok {
		lock = &sync.Mutex{}
		m.locks[did] = lock
	}
	return lock
}

// readRepoState reads the head pointer, taking the row lock when forUpdate
// (write path). The row lock is only a backstop: cross-process commit
// serialization comes from the global advisory lock (commitAdvisoryLockKey)
// — FOR UPDATE on a missing row locks nothing, so it cannot protect genesis
// commits. A repo with no commits yet satisfies errors.IsNotFound.
func readRepoState(ctx context.Context, tx *sql.Tx, did string, forUpdate bool) (*repoState, error) {
	query := `SELECT head_cid, rev FROM repo_state WHERE did = $1`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	var st repoState
	err := tx.QueryRowContext(ctx, query, did).Scan(&st.headCID, &st.rev)
	if stderrors.Is(err, sql.ErrNoRows) {
		return nil, errors.NewNotFoundError("repo", did)
	}
	if err != nil {
		return nil, fmt.Errorf("repo: read repo_state for %s: %w", did, err)
	}
	return &st, nil
}

// loadTree loads the full MST behind a head commit from the DID's blocks.
// It returns the tree and the commit's data (MST root) CID.
func loadTree(ctx context.Context, tx *sql.Tx, did, headCID string) (*mst.Tree, cid.Cid, error) {
	head, err := cid.Parse(headCID)
	if err != nil {
		return nil, cid.Undef, fmt.Errorf("repo: parse head cid %q for %s: %w", headCID, did, err)
	}
	src := &txBlockSource{tx: tx, did: did}
	blk, err := src.Get(ctx, head)
	if err != nil {
		return nil, cid.Undef, fmt.Errorf("repo: read head commit %s for %s: %w", headCID, did, err)
	}
	var commit indigorepo.Commit
	if err := commit.UnmarshalCBOR(bytes.NewReader(blk.RawData())); err != nil {
		return nil, cid.Undef, fmt.Errorf("repo: decode head commit %s for %s: %w", headCID, did, err)
	}
	tree, err := mst.LoadTreeFromStore(ctx, src, commit.Data)
	if err != nil {
		return nil, cid.Undef, fmt.Errorf("repo: load MST for %s: %w", did, err)
	}
	return tree, commit.Data, nil
}

// validatePath validates the identifier triple and returns the MST path.
func validatePath(did, collection, rkey string) (string, syntax.DID, error) {
	parsedDID, err := syntax.ParseDID(did)
	if err != nil {
		return "", "", errors.NewValidationError("did", err.Error())
	}
	nsid, err := syntax.ParseNSID(collection)
	if err != nil {
		return "", "", errors.NewValidationError("collection", err.Error())
	}
	parsedRKey, err := syntax.ParseRecordKey(rkey)
	if err != nil {
		return "", "", errors.NewValidationError("rkey", err.Error())
	}
	return nsid.String() + "/" + parsedRKey.String(), parsedDID, nil
}

// validateRecord applies the checks record maps must pass before encoding:
// non-nil and carrying the $type every atproto record requires. Full data
// model validation happens implicitly in atdata.MarshalCBOR.
func validateRecord(record map[string]any) error {
	if record == nil {
		return errors.NewValidationError("record", "must not be nil")
	}
	t, ok := record["$type"].(string)
	if !ok || t == "" {
		return errors.NewValidationError("record", "must carry a non-empty $type string")
	}
	return nil
}
