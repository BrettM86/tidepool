package repo

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"

	"github.com/bluesky-social/indigo/atproto/atdata"
	"github.com/bluesky-social/indigo/atproto/syntax"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"

	"tidepool/internal/blobstore"
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
// per-CID lock (blobstore.LockCID) and then the shared blob store lock
// (blobstore.Lock), so a DeleteBlobTx that lifts a withhold with the bytes,
// or an operator DeleteBytes of the CID, either waits for this store and
// deletes what it inserted, or commits first and this check sees its
// outcome.
//
// Any other failure (beginning the transaction, taking a lock, the withhold
// check, the byte store write, or the commit) stores nothing and returns an
// error satisfying errors.IsBlobStoreUnavailable. It is logged at Error with
// code blob_object_put_failed unless the caller's own context ended, which
// is the caller's outcome, not a byte store failure.
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
	cidStr := blobCID.String()
	covered, err := m.storeBlob(ctx, did, cidStr, mimeType, data)
	if err != nil {
		if ctx.Err() == nil {
			m.logger.Error("blob store failed", "code", "blob_object_put_failed", "did", did, "cid", cidStr, "error", err)
		}
		return nil, fmt.Errorf("repo: store blob %s for %s: %w: %w", cidStr, did, errors.ErrBlobStoreUnavailable, err)
	}
	if covered {
		return ref, fmt.Errorf("repo: store blob %s for %s: %w", cidStr, did, errors.ErrBlobWithheld)
	}
	return ref, nil
}

// storeBlob runs PutBlob's transaction: the shared CID lock, the shared
// blob store lock, the withhold check, and the byte store write. covered reports a withhold that
// refused the store (nothing written, no error).
func (m *Manager) storeBlob(ctx context.Context, did, cidStr, mimeType string, data []byte) (covered bool, err error) {
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := blobstore.LockCID(ctx, tx, cidStr, true); err != nil {
		return false, err
	}
	if err := blobstore.Lock(ctx, tx, did, cidStr, true); err != nil {
		return false, err
	}
	covered, err = m.withholds.CoversTx(ctx, tx, did, cidStr)
	if err != nil {
		return false, fmt.Errorf("blob withhold check: %w", err)
	}
	if covered {
		return true, nil
	}
	if err := m.blobStore.Put(ctx, tx, did, cidStr, mimeType, data); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit: %w", err)
	}
	return false, nil
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
//
// In s3 mode the delete queues the object's delete in the same statement,
// and DeleteBlob then processes that queue row; a failed object delete is
// logged and left to the sweeper, not returned.
func (m *Manager) DeleteBlob(ctx context.Context, did, cidStr string) error {
	if _, err := cid.Parse(cidStr); err != nil {
		return errors.NewValidationError("cid", err.Error())
	}
	n, err := m.blobStore.Delete(ctx, m.db, did, cidStr)
	if err != nil {
		return fmt.Errorf("repo: delete blob %s for %s: %w", cidStr, did, err)
	}
	if n > 0 {
		m.blobStore.ProcessObjectDeletes(ctx, did, cidStr)
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
// commits. In s3 mode the object's delete is queued in tx and left to the
// sweeper: the exclusive lock tx holds would block processing it here.
func (m *Manager) DeleteBlobTx(ctx context.Context, tx *sql.Tx, did, cidStr string) (gone bool, err error) {
	if _, err := cid.Parse(cidStr); err != nil {
		return false, errors.NewValidationError("cid", err.Error())
	}
	if err := blobstore.Lock(ctx, tx, did, cidStr, false); err != nil {
		return false, err
	}
	if _, err := m.blobStore.Delete(ctx, tx, did, cidStr); err != nil {
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
// deletes those bytes. In s3 mode the objects' deletes are queued and
// processed as in DeleteBlob.
func (m *Manager) DeleteBlobsForDID(ctx context.Context, did string) (int64, error) {
	if _, err := syntax.ParseDID(did); err != nil {
		return 0, errors.NewValidationError("did", err.Error())
	}
	n, err := m.blobStore.DeleteForDID(ctx, m.db, did)
	if err != nil {
		return 0, fmt.Errorf("repo: delete blobs for %s: %w", did, err)
	}
	if n > 0 {
		m.blobStore.ProcessObjectDeletes(ctx, did, "")
	}
	return n, nil
}

// GetBlob reads a stored blob by (did, cid). A missing blob is an error
// satisfying errors.IsNotFound. com.atproto.sync.getBlob serves this. A
// (DID, CID) covered by an active blob withhold answers exactly as a missing
// one. A row whose bytes are held in an object store this Manager's byte
// store cannot read is an error wrapping blobstore.ErrBytesInObjectStore
// (not NotFound), logged at Error with code blob_bytes_in_object_store.
//
// In s3 mode a missing bucket object is NotFound, unlogged, when the row was
// deleted or the blob withheld since the row read; with the row still there
// and not withheld it is an internal error logged with code
// blob_object_missing. An object whose length differs from the row's size
// is logged with code blob_object_size_mismatch, and any other failed
// object read, with the caller's context still live, with code
// blob_object_read_failed. Other byte store failures (a row read, the
// caller's context ending) are returned unlogged. None of these is NotFound.
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
	data, mimeType, err = m.blobStore.Get(ctx, did, cidStr)
	if errors.IsNotFound(err) {
		return nil, "", err
	}
	if stderrors.Is(err, blobstore.ErrBytesInObjectStore) {
		m.logger.Error("blob bytes held in the object store", "code", "blob_bytes_in_object_store", "did", did, "cid", cidStr)
		return nil, "", fmt.Errorf("repo: read blob %s for %s: %w", cidStr, did, err)
	}
	if stderrors.Is(err, blobstore.ErrObjectMissing) {
		return nil, "", m.objectMissing(ctx, did, cidStr, err)
	}
	if stderrors.Is(err, blobstore.ErrObjectSizeMismatch) {
		m.logger.Error("blob object size differs from its row", "code", "blob_object_size_mismatch", "did", did, "cid", cidStr, "error", err)
		return nil, "", fmt.Errorf("repo: read blob %s for %s: %w", cidStr, did, err)
	}
	if err != nil {
		if stderrors.Is(err, blobstore.ErrObjectReadFailed) && ctx.Err() == nil {
			m.logger.Error("blob object read failed", "code", "blob_object_read_failed", "did", did, "cid", cidStr, "error", err)
		}
		return nil, "", fmt.Errorf("repo: read blob %s for %s: %w", cidStr, did, err)
	}
	return data, mimeType, nil
}

// objectMissing classifies a bucket object missing under a blobs row that
// GetBlob read: a row deleted or a blob withheld since then is NotFound;
// otherwise the object is lost, which is logged.
func (m *Manager) objectMissing(ctx context.Context, did, cidStr string, missing error) error {
	var rowKept bool
	if err := m.db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM blobs WHERE did = $1 AND cid = $2)`, did, cidStr).Scan(&rowKept); err != nil {
		return fmt.Errorf("repo: read blob %s for %s: re-read row: %w", cidStr, did, err)
	}
	covered := false
	if rowKept {
		var err error
		if covered, err = m.withholds.Covers(ctx, did, cidStr); err != nil {
			return fmt.Errorf("repo: blob withhold check %s for %s: %w", cidStr, did, err)
		}
	}
	if !rowKept || covered {
		return errors.NewNotFoundError("blob", fmt.Sprintf("%s/%s", did, cidStr))
	}
	m.logger.Error("blob object missing", "code", "blob_object_missing", "did", did, "cid", cidStr)
	return fmt.Errorf("repo: read blob %s for %s: %w", cidStr, did, missing)
}
