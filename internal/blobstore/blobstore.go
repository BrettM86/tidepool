// Package blobstore is the byte store seam for blob bytes.
//
// A blobs row always holds a blob's metadata (did, cid, mime_type, size).
// Where its bytes live depends on the mode:
//
//   - postgres (the only mode in this build): bytes sit in blobs.bytes.
//   - s3: bytes sit in an object store bucket, blobs.bytes is
//     NULL, and bucket objects whose rows were deleted wait in
//     blob_object_deletes until a sweep removes them.
//
// The repo Manager keeps every policy decision (validation, blob withholds,
// logging); a Store only moves bytes. Stores and deletes of one (did, cid)
// are serialized by Lock: a store holds it shared from its withhold check
// through its write, and a delete that a store in flight must not outlive
// takes it exclusively.
//
// This package is a leaf: it must not import repo or blobwithhold.
package blobstore

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"hash/fnv"

	"tidepool/internal/errors"
)

// LockNamespace scopes the per-(DID, CID) blob store lock (the two-argument
// advisory lock form): "bl". The two-argument form occupies a lock space
// distinct from the one-argument repo commit lock.
const LockNamespace = 0x626c

// CIDLockNamespace scopes the per-CID blob lock (two-argument advisory lock
// form, key hashed from the CID): "bc".
const CIDLockNamespace = 0x6263

// Querier is satisfied by *sql.DB and *sql.Tx.
type Querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Store is the byte store seam.
type Store interface {
	// Put stores data for (did, cid) inside the caller's tx. The caller has
	// already taken Lock shared and checked blob withholds; Put does neither.
	// Re-putting an existing (did, cid) is a no-op success. An implementation
	// may also use its own *sql.DB for a statement that must commit
	// independently of tx.
	Put(ctx context.Context, tx *sql.Tx, did, cid, mimeType string, data []byte) error
	// Get reads the bytes and mime type of (did, cid). No blobs row is an
	// error satisfying errors.IsNotFound. Get never checks withholds.
	Get(ctx context.Context, did, cid string) (data []byte, mimeType string, err error)
	// Delete removes (did, cid) through q (an autocommit *sql.DB or the
	// caller's *sql.Tx) and returns the rows deleted. A CID under an active
	// operator withhold is kept; that check is part of the delete, so a
	// withhold committed concurrently is never raced past.
	Delete(ctx context.Context, q Querier, did, cid string) (int64, error)
	// DeleteForDID removes every blob under did through q and returns the
	// rows deleted, keeping CIDs under an active operator withhold as Delete
	// does.
	DeleteForDID(ctx context.Context, q Querier, did string) (int64, error)
	// DeleteCID removes every owner's blob of cid through q and returns the
	// rows deleted, with no withhold exclusion: the operator's delete-bytes,
	// run under the exclusive LockCID.
	DeleteCID(ctx context.Context, q Querier, cid string) (int64, error)
	// ProcessObjectDeletes processes the queued bucket object deletes for
	// did and cid after the deleting statement committed; an empty did or
	// cid matches any. Failures are logged and their rows left to the
	// sweeper. The caller must hold no blob lock.
	ProcessObjectDeletes(ctx context.Context, did, cid string)
}

// Postgres stores blob bytes in the blobs table.
type Postgres struct {
	db *sql.DB
}

// NewPostgres builds the postgres byte store reading through db.
func NewPostgres(db *sql.DB) *Postgres { return &Postgres{db: db} }

// Put inserts the blobs row with its bytes in tx.
func (p *Postgres) Put(ctx context.Context, tx *sql.Tx, did, cid, mimeType string, data []byte) error {
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO blobs (did, cid, mime_type, size, bytes)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (did, cid) DO NOTHING`,
		did, cid, mimeType, len(data), data); err != nil {
		return fmt.Errorf("blobstore: insert blob %s for %s: %w", cid, did, err)
	}
	return nil
}

// Get reads the bytes from the blobs row. A row whose bytes are NULL (held
// in an object store this mode cannot reach) is an error wrapping
// ErrBytesInObjectStore.
func (p *Postgres) Get(ctx context.Context, did, cid string) ([]byte, string, error) {
	var (
		data     []byte
		mimeType string
	)
	err := p.db.QueryRowContext(ctx,
		`SELECT bytes, mime_type FROM blobs WHERE did = $1 AND cid = $2`,
		did, cid).Scan(&data, &mimeType)
	if stderrors.Is(err, sql.ErrNoRows) {
		return nil, "", errors.NewNotFoundError("blob", fmt.Sprintf("%s/%s", did, cid))
	}
	if err != nil {
		return nil, "", fmt.Errorf("blobstore: read blob %s for %s: %w", cid, did, err)
	}
	if data == nil {
		return nil, "", fmt.Errorf("blobstore: read blob %s for %s: %w", cid, did, ErrBytesInObjectStore)
	}
	return data, mimeType, nil
}

// Delete deletes the blobs row unless its CID is under an active operator
// withhold.
func (p *Postgres) Delete(ctx context.Context, q Querier, did, cid string) (int64, error) {
	res, err := q.ExecContext(ctx, `
		DELETE FROM blobs WHERE did = $1 AND cid = $2
		AND NOT EXISTS (
			SELECT 1 FROM blob_withholds w
			WHERE w.cid = blobs.cid AND w.source = 'operator' AND w.lifted_at IS NULL
		)`, did, cid)
	if err != nil {
		return 0, fmt.Errorf("blobstore: delete blob %s for %s: %w", cid, did, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("blobstore: delete blob %s for %s: rows affected: %w", cid, did, err)
	}
	return n, nil
}

// DeleteForDID deletes every blobs row under did whose CID is not under an
// active operator withhold.
func (p *Postgres) DeleteForDID(ctx context.Context, q Querier, did string) (int64, error) {
	res, err := q.ExecContext(ctx, `
		DELETE FROM blobs WHERE did = $1
		AND NOT EXISTS (
			SELECT 1 FROM blob_withholds w
			WHERE w.cid = blobs.cid AND w.source = 'operator' AND w.lifted_at IS NULL
		)`, did)
	if err != nil {
		return 0, fmt.Errorf("blobstore: delete blobs for %s: %w", did, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("blobstore: delete blobs for %s: rows affected: %w", did, err)
	}
	return n, nil
}

// DeleteCID deletes every owner's blobs row of cid.
func (p *Postgres) DeleteCID(ctx context.Context, q Querier, cid string) (int64, error) {
	res, err := q.ExecContext(ctx, `DELETE FROM blobs WHERE cid = $1`, cid)
	if err != nil {
		return 0, fmt.Errorf("blobstore: delete blobs of %s: %w", cid, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("blobstore: delete blobs of %s: rows affected: %w", cid, err)
	}
	return n, nil
}

// ProcessObjectDeletes does nothing: postgres mode queues no object deletes.
func (p *Postgres) ProcessObjectDeletes(context.Context, string, string) {}

// ErrBytesInObjectStore means a blobs row's bytes are NULL because they are
// held in an object store, which postgres mode cannot read.
var ErrBytesInObjectStore = stderrors.New("blob_bytes_in_object_store")

// Lock takes the (did, cid) blob store lock for the rest of tx: shared for a
// store, which holds it from its withhold check through its write, exclusive
// for a delete that a store in flight must not outlive.
func Lock(ctx context.Context, tx *sql.Tx, did, cid string, shared bool) error {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(did + " " + cid)) // hash.Write never returns an error
	query := `SELECT pg_advisory_xact_lock($1, $2)`
	if shared {
		query = `SELECT pg_advisory_xact_lock_shared($1, $2)`
	}
	//nolint:gosec // wraparound is fine: this is a lock key, not a value
	if _, err := tx.ExecContext(ctx, query, LockNamespace, int32(hash.Sum32())); err != nil {
		return fmt.Errorf("blobstore: take blob store lock %s for %s: %w", cid, did, err)
	}
	return nil
}

// LockCID takes the per-CID blob lock for the rest of tx: shared for a store
// (PutBlob takes it before the (did, cid) Lock), exclusive for a delete of
// the CID's bytes under every DID. Lock order is always LockCID before Lock.
func LockCID(ctx context.Context, tx *sql.Tx, cid string, shared bool) error {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(cid)) // hash.Write never returns an error
	query := `SELECT pg_advisory_xact_lock($1, $2)`
	if shared {
		query = `SELECT pg_advisory_xact_lock_shared($1, $2)`
	}
	//nolint:gosec // wraparound is fine: this is a lock key, not a value
	if _, err := tx.ExecContext(ctx, query, CIDLockNamespace, int32(hash.Sum32())); err != nil {
		return fmt.Errorf("blobstore: take blob cid lock %s: %w", cid, err)
	}
	return nil
}

// ModeError is a startup guard failure: the database holds Count rows that
// the configured mode cannot serve or finish. Code is
// "blob_bytes_in_object_store" or "blob_object_deletes_pending".
type ModeError struct {
	Code  string
	Count int64
}

func (e *ModeError) Error() string {
	switch e.Code {
	case "blob_bytes_in_object_store":
		return fmt.Sprintf("%s: %d blobs rows hold their bytes in the object store; postgres mode cannot serve them, run with BLOB_STORE=s3", e.Code, e.Count)
	case "blob_object_deletes_pending":
		return fmt.Sprintf("%s: %d queued bucket object deletes; drain blob_object_deletes in s3 mode before running with BLOB_STORE=postgres", e.Code, e.Count)
	default:
		return fmt.Sprintf("%s: %d rows", e.Code, e.Count)
	}
}

// CheckPostgresMode returns a *ModeError when db holds blobs rows with NULL
// bytes or queued bucket object deletes, either of which postgres mode
// cannot handle, and nil when postgres mode can run.
func CheckPostgresMode(ctx context.Context, db *sql.DB) error {
	var n int64
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM blobs WHERE bytes IS NULL`).Scan(&n); err != nil {
		return fmt.Errorf("blobstore: count object store blobs: %w", err)
	}
	if n > 0 {
		return &ModeError{Code: "blob_bytes_in_object_store", Count: n}
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM blob_object_deletes`).Scan(&n); err != nil {
		return fmt.Errorf("blobstore: count queued object deletes: %w", err)
	}
	if n > 0 {
		return &ModeError{Code: "blob_object_deletes_pending", Count: n}
	}
	return nil
}
