package repo

// This file is the sync read API: everything task 04's com.atproto.sync.*
// surface needs from the repo layer, so internal/sync never issues raw SQL
// against repo tables. The write path (repo.go / events.go) appends
// firehose_events rows inside each commit transaction; this file reads them
// back for subscribeRepos, builds getRecord proof CARs, enumerates repos for
// listRepos/getRepoStatus, and prunes the event backlog per the retention
// policy.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"time"

	indigorepo "github.com/bluesky-social/indigo/atproto/repo"
	"github.com/bluesky-social/indigo/atproto/repo/mst"

	blockformat "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"

	"tidepool/internal/errors"
	"tidepool/internal/store"
)

// FirehoseNotifyChannel is the postgres LISTEN/NOTIFY channel every commit
// transaction notifies on (payload: the new event's seq as decimal text).
// Because pg_notify runs inside the commit transaction — while the global
// commit advisory lock is still held — notifications are delivered in commit
// order, exactly matching seq order. internal/sync's broadcaster LISTENs on
// this channel to wake subscriber outboxes without polling.
const FirehoseNotifyChannel = "tidepool_firehose"

// Event kinds (firehose_events.kind). Commit rows carry the #commit frame
// payload; account rows carry an #account frame (active/status) and no
// commit columns.
const (
	EventKindCommit  = "commit"
	EventKindAccount = "account"
)

// Account status tokens for #account frames (the com.atproto.sync.defs
// account-status vocabulary the bridge emits). Deleted is what Delete(Actor)
// / consent revocation uses: bigsky marks the repo tombstoned — dropping it
// from listRepos — and purges its carstore data, which is exactly the
// downstream purge the consent flip wants.
const (
	AccountStatusDeleted = "deleted"
)

// Event is one row of the durable subscribeRepos backlog, as read back for
// serving. For Kind == EventKindCommit the field semantics match the
// #commit frame of com.atproto.sync.subscribeRepos (see migration 006 for
// the storage notes); for EventKindAccount only Seq, DID, CreatedAt,
// AccountActive, and AccountStatus are meaningful.
type Event struct {
	// Seq is the firehose cursor (bigserial; gapless in visibility order,
	// though individual integers may be skipped by rolled-back writes).
	Seq int64
	// Kind discriminates commit vs account rows (EventKind*).
	Kind string
	// DID is the repo the commit belongs to (the frame's `repo` field).
	DID string
	// CommitCID is the signed commit block's CID.
	CommitCID string
	// PrevDataCID is the MST root before this commit (sync v1.1 prevData);
	// nil on a repo's genesis commit.
	PrevDataCID *string
	// SinceRev is the previous commit's rev (the frame's `since`); nil on
	// genesis.
	SinceRev *string
	// Rev is this commit's rev TID.
	Rev string
	// Ops are the record mutations in this commit.
	Ops []Op
	// CAR is the CARv1 slice for the frame's `blocks` field (root/commit
	// block first, then MST diff nodes and record blocks).
	CAR []byte
	// CreatedAt is when the commit landed (the frame's `time`).
	CreatedAt time.Time
	// AccountActive / AccountStatus carry the #account frame payload for
	// Kind == EventKindAccount rows.
	AccountActive bool
	AccountStatus string
}

// ListEvents returns up to limit firehose events with seq > sinceSeq, in seq
// order. Because every commit holds the global advisory lock until its
// transaction commits, seq order equals commit-visibility order and naive
// tailing with the last-seen seq can never permanently skip an event.
func (m *Manager) ListEvents(ctx context.Context, sinceSeq int64, limit int) ([]*Event, error) {
	if limit <= 0 {
		return nil, errors.NewValidationError("limit", "must be positive")
	}
	rows, err := m.db.QueryContext(ctx, `
		SELECT seq, kind, did, commit_cid, prev_data_cid, since_rev, rev, ops, car, created_at,
		       account_active, account_status
		FROM firehose_events
		WHERE seq > $1
		ORDER BY seq
		LIMIT $2`, sinceSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("repo: list firehose events after %d: %w", sinceSeq, err)
	}
	defer rows.Close()

	var out []*Event
	for rows.Next() {
		var ev Event
		var opsJSON []byte
		var commitCID, rev, accountStatus sql.NullString
		var accountActive sql.NullBool
		if err := rows.Scan(&ev.Seq, &ev.Kind, &ev.DID, &commitCID, &ev.PrevDataCID,
			&ev.SinceRev, &rev, &opsJSON, &ev.CAR, &ev.CreatedAt,
			&accountActive, &accountStatus); err != nil {
			return nil, fmt.Errorf("repo: scan firehose event: %w", err)
		}
		ev.CommitCID = commitCID.String
		ev.Rev = rev.String
		ev.AccountActive = accountActive.Bool
		ev.AccountStatus = accountStatus.String
		if len(opsJSON) > 0 {
			if err := json.Unmarshal(opsJSON, &ev.Ops); err != nil {
				return nil, fmt.Errorf("repo: decode ops for seq %d: %w", ev.Seq, err)
			}
		}
		out = append(out, &ev)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repo: iterate firehose events: %w", err)
	}
	return out, nil
}

// SeqBounds reports the oldest and newest retained firehose seq. When no
// events are retained (nothing written yet, or everything pruned) it returns
// oldest == newest+1, with newest being the last seq ever assigned (0 if no
// event has ever been written) — so `oldest > newest` is the emptiness
// signal, and cursor validation still works after aggressive pruning.
func (m *Manager) SeqBounds(ctx context.Context) (oldest, newest int64, err error) {
	err = m.db.QueryRowContext(ctx,
		`SELECT COALESCE(MIN(seq), 0), COALESCE(MAX(seq), 0) FROM firehose_events`).
		Scan(&oldest, &newest)
	if err != nil {
		return 0, 0, fmt.Errorf("repo: read firehose seq bounds: %w", err)
	}
	if newest > 0 {
		return oldest, newest, nil
	}
	// Empty table: recover the last-assigned seq from the sequence itself so
	// a fully pruned stream still rejects future cursors correctly. Resolve
	// the sequence relation via pg_get_serial_sequence so a rename cannot
	// silently break this fallback.
	var seqName string
	if err := m.db.QueryRowContext(ctx,
		`SELECT pg_get_serial_sequence('firehose_events', 'seq')`).Scan(&seqName); err != nil {
		return 0, 0, fmt.Errorf("repo: resolve firehose seq sequence: %w", err)
	}
	var lastValue int64
	var isCalled bool
	err = m.db.QueryRowContext(ctx,
		// seqName comes from postgres itself, not caller input.
		fmt.Sprintf(`SELECT last_value, is_called FROM %s`, seqName)).
		Scan(&lastValue, &isCalled)
	if err != nil {
		return 0, 0, fmt.Errorf("repo: read firehose seq sequence: %w", err)
	}
	// An event may have committed between the MIN/MAX read and the sequence
	// read; prefer real rows if they exist now (the sequence also counts
	// nextval calls from still-uncommitted transactions, so it can only
	// overshoot, never undershoot, the committed frontier).
	if err := m.db.QueryRowContext(ctx,
		`SELECT COALESCE(MIN(seq), 0), COALESCE(MAX(seq), 0) FROM firehose_events`).
		Scan(&oldest, &newest); err != nil {
		return 0, 0, fmt.Errorf("repo: re-read firehose seq bounds: %w", err)
	}
	if newest > 0 {
		return oldest, newest, nil
	}
	if !isCalled {
		return 1, 0, nil // never written
	}
	return lastValue + 1, lastValue, nil
}

// pruneEventsBatchSize bounds one DELETE statement inside PruneEvents. One
// unbatched DELETE over a large expired prefix holds row locks (and bloats
// one WAL transaction) for the whole sweep; batching keeps each statement
// short so commits — which contend on the same table — never stall behind
// retention.
const pruneEventsBatchSize = 1000

// PruneEvents deletes firehose events older than the cutoff. It prunes a
// strict seq prefix — everything up to the newest expired seq — rather than
// filtering on created_at row-by-row: commit-transaction start times are not
// perfectly ordered with seq assignment (CURRENT_TIMESTAMP is the tx start,
// the advisory lock is taken after BeginTx), and retention must never punch
// holes in the retained suffix, because subscribeRepos replay treats
// [oldest, newest] as complete. The DELETE runs in batches (each its own
// implicit transaction) from the oldest seq up, so a partial sweep still
// leaves a contiguous retained suffix. It returns the number of events
// deleted.
func (m *Manager) PruneEvents(ctx context.Context, cutoff time.Time) (int64, error) {
	// The boundary is computed once: everything at or below it is expired.
	var boundary int64
	if err := m.db.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(seq), 0) FROM firehose_events WHERE created_at < $1`,
		cutoff).Scan(&boundary); err != nil {
		return 0, fmt.Errorf("repo: find prune boundary before %s: %w", cutoff.Format(time.RFC3339), err)
	}
	if boundary == 0 {
		return 0, nil
	}
	var total int64
	for {
		res, err := m.db.ExecContext(ctx, `
			DELETE FROM firehose_events WHERE seq IN (
				SELECT seq FROM firehose_events WHERE seq <= $1 ORDER BY seq LIMIT $2
			)`, boundary, pruneEventsBatchSize)
		if err != nil {
			return total, fmt.Errorf("repo: prune firehose events before %s: %w", cutoff.Format(time.RFC3339), err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return total, fmt.Errorf("repo: prune firehose events: rows affected: %w", err)
		}
		total += n
		if n < pruneEventsBatchSize {
			return total, nil
		}
	}
}

// GetRecordProof builds the com.atproto.sync.getRecord response: a CARv1
// slice proving the record's inclusion in the current head commit. It
// contains, in order, the signed commit block (the CAR root), the MST node
// blocks on the path from the tree root to the leaf holding the record key,
// and the record block itself. A missing repo or record satisfies
// errors.IsNotFound (with Resource "repo" or "record" respectively).
func (m *Manager) GetRecordProof(ctx context.Context, did, collection, rkey string) ([]byte, error) {
	path, _, err := validatePath(did, collection, rkey)
	if err != nil {
		return nil, err
	}

	// Read consistency: same reasoning as GetRecord — blocks are
	// content-addressed and append-only, so once the head is read every
	// block it references is immutable and present.
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
	src := &txBlockSource{tx: tx, did: did}
	headBlk, err := src.Get(ctx, head)
	if err != nil {
		return nil, fmt.Errorf("repo: read head commit %s for %s: %w", state.headCID, did, err)
	}
	var commit indigorepo.Commit
	if err := commit.UnmarshalCBOR(bytes.NewReader(headBlk.RawData())); err != nil {
		return nil, fmt.Errorf("repo: decode head commit %s for %s: %w", state.headCID, did, err)
	}

	blocks := []blockformat.Block{headBlk}
	key := []byte(path)
	notFound := func() error {
		return errors.NewNotFoundError("record", fmt.Sprintf("at://%s/%s", did, path))
	}

	// Walk from the MST root toward the leaf, collecting each node block on
	// the path. The walk decodes stored node bytes directly (rather than
	// loading the whole tree) so the proof stays proportional to tree depth.
	cur := commit.Data
	var recordCID *cid.Cid
	for depth := 0; ; depth++ {
		// An MST over a repo path (max ~1KB keys) can never legitimately be
		// hundreds of levels deep; this bounds the walk against a corrupt
		// (cyclic) tree.
		if depth > 128 {
			return nil, fmt.Errorf("repo: MST walk for %s/%s exceeded max depth (corrupt tree?)", did, path)
		}
		blk, err := src.Get(ctx, cur)
		if err != nil {
			return nil, fmt.Errorf("repo: read MST node %s for %s: %w", cur, did, err)
		}
		blocks = append(blocks, blk)
		nd, err := mst.NodeDataFromCBOR(bytes.NewReader(blk.RawData()))
		if err != nil {
			return nil, fmt.Errorf("repo: decode MST node %s for %s: %w", cur, did, err)
		}
		node := nd.Node(&cur)

		found := false
		for i := range node.Entries {
			e := &node.Entries[i]
			if e.IsValue() && bytes.Equal(e.Key, key) {
				recordCID = e.Value
				found = true
				break
			}
		}
		if found {
			break
		}
		childCID := coveringChild(&node, key)
		if childCID == nil {
			return nil, notFound()
		}
		cur = *childCID
	}
	if recordCID == nil {
		return nil, notFound()
	}
	recBlk, err := src.Get(ctx, *recordCID)
	if err != nil {
		return nil, fmt.Errorf("repo: read record block %s for %s: %w", recordCID, did, err)
	}
	blocks = append(blocks, recBlk)

	return writeCARSlice(head, blocks)
}

// coveringChild returns the CID of the child subtree a key would live under
// in a decoded MST node, or nil when no child covers it (the key is absent).
// Mirrors indigo's unexported Node.findExistingChild: the candidate child is
// the most recent child entry, invalidated whenever a value entry with a
// smaller key follows it, and settled at the first value entry with a key >=
// the target.
func coveringChild(n *mst.Node, key []byte) *cid.Cid {
	var child *cid.Cid
	for i := range n.Entries {
		e := &n.Entries[i]
		if e.IsChild() {
			child = e.ChildCID
			continue
		}
		if e.IsValue() {
			if bytes.Compare(key, e.Key) <= 0 {
				break
			}
			child = nil
		}
	}
	return child
}

// RepoInfo describes one hosted repo for com.atproto.sync.listRepos /
// getRepoStatus. Active is false when the DID's bridged actor is tombstoned
// (consent revoked or deleted upstream); repos with no bridged_actors row
// (communities, the service actor) are always active.
type RepoInfo struct {
	DID     string
	HeadCID string
	Rev     string
	Active  bool
	// Status is the lexicon's account-status token when inactive
	// ("deleted" — the bridged actor was tombstoned by Delete(Actor)/
	// consent revocation, which is terminal); empty when active. The same
	// token rides the #account frame, so the HTTP surface and the firehose
	// agree.
	Status string
}

const repoInfoQuery = `
	SELECT r.did, r.head_cid, r.rev, COALESCE(a.consent_state, '')
	FROM repo_state r
	LEFT JOIN bridged_actors a ON a.did = r.did`

// ListRepos enumerates hosted repos in DID order, returning up to limit rows
// with did > cursorDID (empty cursor starts from the beginning). Pagination
// contract matches com.atproto.sync.listRepos: pass the last returned DID as
// the next cursor.
func (m *Manager) ListRepos(ctx context.Context, cursorDID string, limit int) ([]*RepoInfo, error) {
	if limit <= 0 {
		return nil, errors.NewValidationError("limit", "must be positive")
	}
	rows, err := m.db.QueryContext(ctx,
		repoInfoQuery+` WHERE r.did > $1 ORDER BY r.did LIMIT $2`, cursorDID, limit)
	if err != nil {
		return nil, fmt.Errorf("repo: list repos after %q: %w", cursorDID, err)
	}
	defer rows.Close()

	var out []*RepoInfo
	for rows.Next() {
		info, err := scanRepoInfo(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, info)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("repo: iterate repos: %w", err)
	}
	return out, nil
}

// GetRepoInfo returns the RepoInfo for one DID. A DID with no repo satisfies
// errors.IsNotFound.
func (m *Manager) GetRepoInfo(ctx context.Context, did string) (*RepoInfo, error) {
	row := m.db.QueryRowContext(ctx, repoInfoQuery+` WHERE r.did = $1`, did)
	info, err := scanRepoInfo(row.Scan)
	if stderrors.Is(err, sql.ErrNoRows) {
		return nil, errors.NewNotFoundError("repo", did)
	}
	if err != nil {
		return nil, err
	}
	return info, nil
}

func scanRepoInfo(scan func(...any) error) (*RepoInfo, error) {
	var info RepoInfo
	var consent string
	if err := scan(&info.DID, &info.HeadCID, &info.Rev, &consent); err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		return nil, fmt.Errorf("repo: scan repo info: %w", err)
	}
	info.Active = consent != string(store.ConsentStateDeleted)
	if !info.Active {
		info.Status = AccountStatusDeleted
	}
	return &info, nil
}
