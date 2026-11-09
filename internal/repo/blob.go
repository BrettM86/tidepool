package repo

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"hash/fnv"

	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	"tidepool/internal/errors"
)

// rawBlobPrefix computes atproto blob CIDs: CIDv1, raw codec, sha-256 —
// the convention every PDS uses for blob refs (unlike record/commit blocks,
// which are dag-cbor).
var rawBlobPrefix = cid.Prefix{
	Version:  1,
	Codec:    uint64(cid.Raw),
	MhType:   multihash.SHA2_256,
	MhLength: 32,
}

// blobStoreNamespace scopes the per-(DID, CID) blob store lock. The
// two-argument advisory lock form occupies a lock space distinct from the
// one-argument commit lock (commitAdvisoryLockKey).
const blobStoreNamespace = 0x626c // "bl"

// lockBlobStore takes the (did, cidStr) blob store lock for the rest of tx:
// shared for a store, which holds it from its withhold check through its
// insert, exclusive for a delete that a store in flight must not outlive.
func lockBlobStore(ctx context.Context, tx *sql.Tx, did, cidStr string, shared bool) error {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(did + " " + cidStr)) // hash.Write never returns an error
	query := `SELECT pg_advisory_xact_lock($1, $2)`
	if shared {
		query = `SELECT pg_advisory_xact_lock_shared($1, $2)`
	}
	//nolint:gosec // wraparound is fine: this is a lock key, not a value
	if _, err := tx.ExecContext(ctx, query, blobStoreNamespace, int32(hash.Sum32())); err != nil {
		return fmt.Errorf("repo: take blob store lock %s for %s: %w", cidStr, did, err)
	}
	return nil
}

// PutBlob stores binary media (avatar, banner, post image) for a DID and
// returns the atdata.Blob ref to embed in a record. Blobs are
// content-addressed: re-putting identical bytes is an idempotent no-op.
// Blob writes do not create commits — the record that references the blob
// carries it onto the firehose.
//
// A (DID, CID) covered by an active blob withhold is not stored: PutBlob
// returns the computed ref together with an error satisfying
// errors.IsBlobWithheld, so a caller can keep the ref in its record while
// the bytes stay refused. The check and the insert run under the shared
// blob store lock (lockBlobStore), so a DeleteBlobTx that lifts a withhold
// with the bytes either waits for this store and deletes what it inserted,
// or commits first and this check sees its outcome.
func (m *Manager) PutBlob(ctx context.Context, did, mimeType string, data []byte) (*atdata.Blob, error) {
	if _, err := syntax.ParseDID(did); err != nil {
		return nil, errors.NewValidationError("did", err.Error())
	}
	if mimeType == "" {
		return nil, errors.NewValidationError("mime_type", "must not be empty")
	}
	if len(data) == 0 {
		return nil, errors.NewValidationError("blob", "must not be empty")
	}
	blobCID, err := rawBlobPrefix.Sum(data)
	if err != nil {
		return nil, fmt.Errorf("repo: compute blob cid: %w", err)
	}
	ref := &atdata.Blob{
		Ref:      atdata.CIDLink(blobCID),
		MimeType: mimeType,
		Size:     int64(len(data)),
	}
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("repo: store blob %s for %s: begin tx: %w", blobCID, did, err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := lockBlobStore(ctx, tx, did, blobCID.String(), true); err != nil {
		return nil, err
	}
	covered, err := m.withholds.CoversTx(ctx, tx, did, blobCID.String())
	if err != nil {
		return nil, fmt.Errorf("repo: blob withhold check %s for %s: %w", blobCID, did, err)
	}
	if covered {
		return ref, fmt.Errorf("repo: store blob %s for %s: %w", blobCID, did, errors.ErrBlobWithheld)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO blobs (did, cid, mime_type, size, bytes)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (did, cid) DO NOTHING`,
		did, blobCID.String(), mimeType, len(data), data); err != nil {
		return nil, fmt.Errorf("repo: store blob %s for %s: %w", blobCID, did, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("repo: store blob %s for %s: commit: %w", blobCID, did, err)
	}
	return ref, nil
}

// DeleteBlob removes one stored blob. Missing rows are a no-op success
// (scrubs are idempotent). Task 11's Delete(Actor)/nobridge scrub uses this
// for blobs the deleted actor's records referenced — including post images
// stored under COMMUNITY DIDs, which record deletion alone never touches.
// Caveat, accepted: blobs are content-addressed, so if two different records
// in the same repo embedded byte-identical media, scrubbing one deletes the
// blob out from under the other (a missing image, not missing content) —
// tracking cross-record blob references is not worth that edge at bridge
// scale.
//
// A CID under an active operator withhold is left in place: only the
// operator's delete-bytes action deletes those bytes. The withhold check is
// part of the DELETE statement, so a withhold committed concurrently is
// never raced past.
func (m *Manager) DeleteBlob(ctx context.Context, did, cidStr string) error {
	if _, err := cid.Parse(cidStr); err != nil {
		return errors.NewValidationError("cid", err.Error())
	}
	res, err := m.db.ExecContext(ctx, `
		DELETE FROM blobs WHERE did = $1 AND cid = $2
		AND NOT EXISTS (
			SELECT 1 FROM blob_withholds w
			WHERE w.cid = blobs.cid AND w.source = 'operator' AND w.lifted_at IS NULL
		)`, did, cidStr)
	if err != nil {
		return fmt.Errorf("repo: delete blob %s for %s: %w", cidStr, did, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("repo: delete blob %s for %s: rows affected: %w", cidStr, did, err)
	}
	if n > 0 {
		return nil
	}
	held, err := m.withholds.HasActiveOperator(ctx, cidStr)
	if err != nil {
		return fmt.Errorf("repo: delete blob %s for %s: %w", cidStr, did, err)
	}
	if held {
		m.logger.Info("blob under operator withhold kept on delete", "did", did, "cid", cidStr)
	}
	return nil
}

// DeleteBlobTx is DeleteBlob in the caller's transaction, for a delete that
// must commit together with other rows. gone reports whether no bytes remain
// stored under (did, cidStr) afterwards: false only when an active operator
// withhold kept them. It takes the blob store lock exclusively first, so a
// PutBlob in flight for (did, cidStr) commits its insert before the delete
// runs, and one not yet past its withhold check makes that check after tx
// commits.
func (m *Manager) DeleteBlobTx(ctx context.Context, tx *sql.Tx, did, cidStr string) (gone bool, err error) {
	if _, err := cid.Parse(cidStr); err != nil {
		return false, errors.NewValidationError("cid", err.Error())
	}
	if err := lockBlobStore(ctx, tx, did, cidStr, false); err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM blobs WHERE did = $1 AND cid = $2
		AND NOT EXISTS (
			SELECT 1 FROM blob_withholds w
			WHERE w.cid = blobs.cid AND w.source = 'operator' AND w.lifted_at IS NULL
		)`, did, cidStr); err != nil {
		return false, fmt.Errorf("repo: delete blob %s for %s: %w", cidStr, did, err)
	}
	var kept bool
	if err := tx.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM blobs WHERE did = $1 AND cid = $2)`, did, cidStr).Scan(&kept); err != nil {
		return false, fmt.Errorf("repo: delete blob %s for %s: check kept bytes: %w", cidStr, did, err)
	}
	if kept {
		m.logger.Info("blob under operator withhold kept on delete", "did", did, "cid", cidStr)
	}
	return !kept, nil
}

// DeleteBlobsForDID removes every blob stored under a DID — the terminal
// Delete(Actor) path for the actor's own (now frozen) repo, where getBlob
// already refuses to serve and nothing can reference the bytes again. It
// returns how many rows were deleted. Blobs whose CID is under an active
// operator withhold are kept: only the operator's delete-bytes action
// deletes those bytes.
func (m *Manager) DeleteBlobsForDID(ctx context.Context, did string) (int64, error) {
	if _, err := syntax.ParseDID(did); err != nil {
		return 0, errors.NewValidationError("did", err.Error())
	}
	res, err := m.db.ExecContext(ctx, `
		DELETE FROM blobs WHERE did = $1
		AND NOT EXISTS (
			SELECT 1 FROM blob_withholds w
			WHERE w.cid = blobs.cid AND w.source = 'operator' AND w.lifted_at IS NULL
		)`, did)
	if err != nil {
		return 0, fmt.Errorf("repo: delete blobs for %s: %w", did, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("repo: delete blobs for %s: rows affected: %w", did, err)
	}
	return n, nil
}

// GetBlob reads a stored blob by (did, cid). A missing blob is an error
// satisfying errors.IsNotFound. com.atproto.sync.getBlob serves this. A
// (DID, CID) covered by an active blob withhold answers exactly as a missing
// one.
func (m *Manager) GetBlob(ctx context.Context, did, cidStr string) (data []byte, mimeType string, err error) {
	if _, err := cid.Parse(cidStr); err != nil {
		return nil, "", errors.NewValidationError("cid", err.Error())
	}
	covered, err := m.withholds.Covers(ctx, did, cidStr)
	if err != nil {
		return nil, "", fmt.Errorf("repo: blob withhold check %s for %s: %w", cidStr, did, err)
	}
	if covered {
		return nil, "", errors.NewNotFoundError("blob", fmt.Sprintf("%s/%s", did, cidStr))
	}
	err = m.db.QueryRowContext(ctx,
		`SELECT bytes, mime_type FROM blobs WHERE did = $1 AND cid = $2`,
		did, cidStr).Scan(&data, &mimeType)
	if stderrors.Is(err, sql.ErrNoRows) {
		return nil, "", errors.NewNotFoundError("blob", fmt.Sprintf("%s/%s", did, cidStr))
	}
	if err != nil {
		return nil, "", fmt.Errorf("repo: read blob %s for %s: %w", cidStr, did, err)
	}
	return data, mimeType, nil
}
