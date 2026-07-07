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
