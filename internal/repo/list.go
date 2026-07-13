package repo

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/ipfs/go-cid"
)

// RecordEntry is one record surfaced by ListRecords: its path plus the
// decoded value, ready to re-put.
type RecordEntry struct {
	Collection string
	Rkey       string
	CID        string
	Value      map[string]any
}

// ListDIDs returns every DID with a repo (a repo_state row), excluding
// tombstoned actors: a consent_state of 'deleted' freezes the bridged
// identity (task-01 semantics), so bulk operations like the admin re-emit
// must never touch those repos. Actors under 'nobridge' suppression stay
// listed — their records are already scrubbed, so walking them is a no-op.
func (m *Manager) ListDIDs(ctx context.Context) ([]string, error) {
	rows, err := m.db.QueryContext(ctx, `
		SELECT rs.did
		FROM repo_state rs
		LEFT JOIN bridged_actors ba ON ba.did = rs.did
		WHERE ba.consent_state IS DISTINCT FROM 'deleted'
		ORDER BY rs.did`)
	if err != nil {
		return nil, fmt.Errorf("repo: list dids: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var dids []string
	for rows.Next() {
		var did string
		if err := rows.Scan(&did); err != nil {
			return nil, fmt.Errorf("repo: scan did: %w", err)
		}
		dids = append(dids, did)
	}
	return dids, rows.Err()
}

// ListRecords walks the DID's current MST and returns every record with its
// decoded value, in tree (path) order. Missing repo is an error satisfying
// errors.IsNotFound. The walk runs on one REPEATABLE READ snapshot for the
// same consistency reasons documented on GetRecord.
func (m *Manager) ListRecords(ctx context.Context, did string) ([]RecordEntry, error) {
	if _, err := syntax.ParseDID(did); err != nil {
		return nil, fmt.Errorf("repo: invalid did %q: %w", did, err)
	}

	tx, err := m.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return nil, fmt.Errorf("repo: begin read tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	state, err := readRepoState(ctx, tx, did, false)
	if err != nil {
		return nil, err
	}
	tree, _, err := loadTree(ctx, tx, did, state.headCID)
	if err != nil {
		return nil, err
	}

	src := &txBlockSource{tx: tx, did: did}
	entries := []RecordEntry{}
	walkErr := tree.Walk(func(key []byte, val cid.Cid) error {
		path := string(key)
		collection, rkey, ok := strings.Cut(path, "/")
		if !ok {
			return fmt.Errorf("repo: malformed MST key %q in %s", path, did)
		}
		blk, err := src.Get(ctx, val)
		if err != nil {
			return fmt.Errorf("repo: read record block %s: %w", val, err)
		}
		value, err := atdata.UnmarshalCBOR(blk.RawData())
		if err != nil {
			return fmt.Errorf("repo: decode record %s: %w", val, err)
		}
		entries = append(entries, RecordEntry{
			Collection: collection,
			Rkey:       rkey,
			CID:        val.String(),
			Value:      value,
		})
		return nil
	})
	if walkErr != nil {
		return nil, walkErr
	}
	return entries, nil
}
