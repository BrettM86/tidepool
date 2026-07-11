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
	if _, err := m.db.ExecContext(ctx, `
		INSERT INTO blobs (did, cid, mime_type, size, bytes)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (did, cid) DO NOTHING`,
		did, blobCID.String(), mimeType, len(data), data); err != nil {
		return nil, fmt.Errorf("repo: store blob %s for %s: %w", blobCID, did, err)
	}
	return &atdata.Blob{
		Ref:      atdata.CIDLink(blobCID),
		MimeType: mimeType,
		Size:     int64(len(data)),
	}, nil
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
func (m *Manager) DeleteBlob(ctx context.Context, did, cidStr string) error {
	if _, err := cid.Parse(cidStr); err != nil {
		return errors.NewValidationError("cid", err.Error())
	}
	if _, err := m.db.ExecContext(ctx,
		`DELETE FROM blobs WHERE did = $1 AND cid = $2`, did, cidStr); err != nil {
		return fmt.Errorf("repo: delete blob %s for %s: %w", cidStr, did, err)
	}
	return nil
}

// DeleteBlobsForDID removes every blob stored under a DID — the terminal
// Delete(Actor) path for the actor's own (now frozen) repo, where getBlob
// already refuses to serve and nothing can reference the bytes again. It
// returns how many rows were deleted.
func (m *Manager) DeleteBlobsForDID(ctx context.Context, did string) (int64, error) {
	if _, err := syntax.ParseDID(did); err != nil {
		return 0, errors.NewValidationError("did", err.Error())
	}
	res, err := m.db.ExecContext(ctx, `DELETE FROM blobs WHERE did = $1`, did)
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
// satisfying errors.IsNotFound. com.atproto.sync.getBlob serves this.
func (m *Manager) GetBlob(ctx context.Context, did, cidStr string) (data []byte, mimeType string, err error) {
	if _, err := cid.Parse(cidStr); err != nil {
		return nil, "", errors.NewValidationError("cid", err.Error())
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
