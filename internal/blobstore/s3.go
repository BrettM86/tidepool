package blobstore

import (
	"bytes"
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	gocid "github.com/ipfs/go-cid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"tidepool/internal/ap"
	"tidepool/internal/errors"
)

// Per-call S3 deadlines: PUT and GET, then DELETE, STAT and bucket HEAD.
var (
	objectTransferTimeout = 30 * time.Second
	objectRequestTimeout  = 10 * time.Second
)

// objectMaxRetries is minio-go's total attempt count per call (1 means no
// retry), kept small so a refused or failing call ends well inside its
// deadline; the work above the store is what retries.
const objectMaxRetries = 2

// objectDeleteSweepInterval is how often Run sweeps the object delete queue;
// a var so tests can shorten it.
var objectDeleteSweepInterval = time.Minute

// objectDeleteStuckAge is the queue age past which a failing object delete
// logs blob_object_delete_stuck.
const objectDeleteStuckAge = "1 hour"

// ErrObjectMissing means a blobs row holds no bytes and the bucket has no
// object for it.
var ErrObjectMissing = stderrors.New("blob_object_missing")

// ErrObjectSizeMismatch means the bucket object's length differs from the
// blobs row's size.
var ErrObjectSizeMismatch = stderrors.New("blob_object_size_mismatch")

// ErrObjectReadFailed means a bucket object read failed for a reason other
// than a missing object: a refused or failed request, a deadline, or a body
// read error.
var ErrObjectReadFailed = stderrors.New("blob_object_read_failed")

// S3Config configures the s3 byte store. Endpoint is an absolute URL with
// scheme.
type S3Config struct {
	Endpoint              string
	Region                string
	Bucket                string
	AccessKeyID           string
	SecretAccessKey       string
	AllowPrivateAddresses bool
	// Logger is the store's logger; nil means slog.Default().
	Logger *slog.Logger
	// Transport carries the store's requests; nil means the production
	// guarded transport.
	Transport http.RoundTripper
}

// S3 stores blob bytes in an object store bucket. The blobs row keeps the
// metadata with NULL bytes; rows written in postgres mode keep their bytes
// and are served from the row.
type S3 struct {
	db     *sql.DB
	client *minio.Client
	bucket string
	logger *slog.Logger
}

// NewS3 builds the s3 byte store reading rows through db. It makes no
// request; HeadBucket checks the bucket answers.
func NewS3(db *sql.DB, cfg S3Config) (*S3, error) {
	// No error repeats cfg.Endpoint: a malformed endpoint may hold
	// credentials. Only the host of a URL without userinfo is named.
	endpoint, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return nil, stderrors.New("blobstore: s3 endpoint is not a URL")
	}
	if endpoint.User != nil {
		return nil, stderrors.New("blobstore: s3 endpoint must not contain credentials")
	}
	if (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Host == "" {
		return nil, stderrors.New("blobstore: s3 endpoint must be an absolute http or https URL with a host")
	}
	if (endpoint.Path != "" && endpoint.Path != "/") || endpoint.RawQuery != "" || endpoint.ForceQuery ||
		endpoint.Fragment != "" {
		return nil, fmt.Errorf("blobstore: s3 endpoint for host %q must be scheme and host only", endpoint.Host)
	}
	for _, field := range []struct{ name, value string }{
		{"region", cfg.Region},
		{"bucket", cfg.Bucket},
		{"access key id", cfg.AccessKeyID},
		{"secret access key", cfg.SecretAccessKey},
	} {
		if field.value == "" {
			return nil, fmt.Errorf("blobstore: s3 %s is required", field.name)
		}
	}
	transport := cfg.Transport
	if transport == nil {
		transport = ap.NewGuardedHTTPClient(cfg.AllowPrivateAddresses, 0).Transport
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	client, err := minio.New(endpoint.Host, &minio.Options{
		Creds:        credentials.NewStaticV4(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		Secure:       endpoint.Scheme == "https",
		Transport:    transport,
		Region:       cfg.Region,
		BucketLookup: minio.BucketLookupPath,
		MaxRetries:   objectMaxRetries,
	})
	if err != nil {
		return nil, fmt.Errorf("blobstore: s3 client for %s: %w", endpoint.Host, err)
	}
	return &S3{db: db, client: client, bucket: cfg.Bucket, logger: logger}, nil
}

// ObjectKey builds the bucket key blobs/<did>/<cid>. It refuses an empty DID
// or CID, a DID that could leave the blobs/<did>/ prefix, and a CID that is
// not in its canonical string form.
func ObjectKey(did, cid string) (string, error) {
	if did == "" || did == "." || did == ".." || strings.Contains(did, "/") {
		return "", errors.NewValidationError("did", fmt.Sprintf("did %q cannot name a bucket object", did))
	}
	parsed, err := gocid.Parse(cid)
	if err != nil {
		return "", errors.NewValidationError("cid", fmt.Sprintf("cid %q does not parse: %v", cid, err))
	}
	if parsed.String() != cid {
		return "", errors.NewValidationError("cid", fmt.Sprintf("cid %q is not canonical", cid))
	}
	return "blobs/" + did + "/" + cid, nil
}

// HeadBucket checks the bucket answers. A missing bucket is an error.
func (s *S3) HeadBucket(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, objectRequestTimeout)
	defer cancel()
	exists, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return fmt.Errorf("blobstore: head bucket %s at %s: %w", s.bucket, s.client.EndpointURL().Host, err)
	}
	if !exists {
		return fmt.Errorf("blobstore: bucket %s at %s does not exist", s.bucket, s.client.EndpointURL().Host)
	}
	return nil
}

// Put stores data for (did, cid). With no blobs row it uploads data as the
// object blobs/<did>/<cid> (Content-MD5, Content-Type mimeType), then
// inserts the row with NULL bytes in tx. A row holding its bytes is kept as
// it is. A row with NULL bytes is checked against the bucket: an object in
// place is kept, a missing one is uploaded again. Before any upload a queue
// row for (did, cid) is committed on the store's own connection, so an
// object whose row never commits is still deleted by the sweeper; with the
// row committed, the sweeper drops the queue row.
func (s *S3) Put(ctx context.Context, tx *sql.Tx, did, cid, mimeType string, data []byte) error {
	key, err := ObjectKey(did, cid)
	if err != nil {
		return err
	}
	var bytesInObject bool
	err = tx.QueryRowContext(ctx,
		`SELECT bytes IS NULL FROM blobs WHERE did = $1 AND cid = $2`, did, cid).Scan(&bytesInObject)
	switch {
	case stderrors.Is(err, sql.ErrNoRows):
	case err != nil:
		return fmt.Errorf("blobstore: read blob %s for %s: %w", cid, did, err)
	case !bytesInObject:
		return nil
	default:
		exists, err := s.objectExists(ctx, key)
		if err != nil || exists {
			return err
		}
		if err := s.queueObjectDelete(ctx, did, cid); err != nil {
			return err
		}
		return s.putObject(ctx, key, mimeType, data)
	}
	if err := s.queueObjectDelete(ctx, did, cid); err != nil {
		return err
	}
	if err := s.putObject(ctx, key, mimeType, data); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO blobs (did, cid, mime_type, size, bytes)
		VALUES ($1, $2, $3, $4, NULL)
		ON CONFLICT (did, cid) DO NOTHING`,
		did, cid, mimeType, len(data)); err != nil {
		return fmt.Errorf("blobstore: insert blob %s for %s: %w", cid, did, err)
	}
	return nil
}

// queueObjectDelete commits a queue row for (did, cid) outside any tx.
func (s *S3) queueObjectDelete(ctx context.Context, did, cid string) error {
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO blob_object_deletes (did, cid) VALUES ($1, $2)
		ON CONFLICT DO NOTHING`, did, cid); err != nil {
		return fmt.Errorf("blobstore: queue object delete %s for %s: %w", cid, did, err)
	}
	return nil
}

func (s *S3) putObject(ctx context.Context, key, mimeType string, data []byte) error {
	ctx, cancel := context.WithTimeout(ctx, objectTransferTimeout)
	defer cancel()
	if _, err := s.client.PutObject(ctx, s.bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ContentType:      mimeType,
		SendContentMd5:   true,
		DisableMultipart: true,
	}); err != nil {
		return fmt.Errorf("blobstore: put object %s: %w", key, err)
	}
	return nil
}

// objectExists reports whether key is in the bucket. A missing object is
// (false, nil); any other failure is an error.
func (s *S3) objectExists(ctx context.Context, key string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, objectRequestTimeout)
	defer cancel()
	if _, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{}); err != nil {
		if minio.ToErrorResponse(err).Code == minio.NoSuchKey {
			return false, nil
		}
		return false, fmt.Errorf("blobstore: stat object %s: %w", key, err)
	}
	return true, nil
}

// Get reads (did, cid). No blobs row is an error satisfying
// errors.IsNotFound. A row holding bytes is served from the row; otherwise
// the object is read, a missing object is an error wrapping
// ErrObjectMissing and an object whose length differs from the row's size
// is an error wrapping ErrObjectSizeMismatch; any other failed object read
// is an error wrapping ErrObjectReadFailed. The mime type always comes from
// the row.
func (s *S3) Get(ctx context.Context, did, cid string) ([]byte, string, error) {
	var (
		data     []byte
		mimeType string
		size     int64
	)
	err := s.db.QueryRowContext(ctx,
		`SELECT bytes, mime_type, size FROM blobs WHERE did = $1 AND cid = $2`,
		did, cid).Scan(&data, &mimeType, &size)
	if stderrors.Is(err, sql.ErrNoRows) {
		return nil, "", errors.NewNotFoundError("blob", fmt.Sprintf("%s/%s", did, cid))
	}
	if err != nil {
		return nil, "", fmt.Errorf("blobstore: read blob %s for %s: %w", cid, did, err)
	}
	if data != nil {
		return data, mimeType, nil
	}
	key, err := ObjectKey(did, cid)
	if err != nil {
		return nil, "", err
	}
	data, err = s.getObject(ctx, key, size)
	if err != nil {
		return nil, "", err
	}
	return data, mimeType, nil
}

// getObject reads at most size+1 bytes of key, all under one transfer
// deadline that also covers the body read.
func (s *S3) getObject(ctx context.Context, key string, size int64) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, objectTransferTimeout)
	defer cancel()
	object, err := s.client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, objectReadError(key, err)
	}
	defer func() { _ = object.Close() }()
	data, err := io.ReadAll(io.LimitReader(object, size+1))
	if err != nil {
		return nil, objectReadError(key, err)
	}
	if int64(len(data)) != size {
		return nil, fmt.Errorf("blobstore: get object %s: read %d bytes, row size %d: %w",
			key, len(data), size, ErrObjectSizeMismatch)
	}
	return data, nil
}

func objectReadError(key string, err error) error {
	if minio.ToErrorResponse(err).Code == minio.NoSuchKey {
		return fmt.Errorf("blobstore: get object %s: %w", key, ErrObjectMissing)
	}
	return fmt.Errorf("blobstore: get object %s: %w: %w", key, ErrObjectReadFailed, err)
}

// activeOperatorWithhold excludes blobs rows whose CID is under an active
// operator withhold.
const activeOperatorWithhold = `NOT EXISTS (
	SELECT 1 FROM blob_withholds w
	WHERE w.cid = blobs.cid AND w.source = 'operator' AND w.lifted_at IS NULL
)`

// deleteAndQueue deletes the blobs rows matching where through q and queues
// their objects' deletes in the same statement. It returns the blobs rows
// deleted.
func deleteAndQueue(ctx context.Context, q Querier, what, where string, args ...any) (int64, error) {
	var n int64
	if err := q.QueryRowContext(ctx, `
		WITH d AS (DELETE FROM blobs WHERE `+where+` RETURNING did, cid),
		queued AS (
			INSERT INTO blob_object_deletes (did, cid) SELECT did, cid FROM d
			ON CONFLICT DO NOTHING
		)
		SELECT count(*) FROM d`, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("blobstore: delete %s: %w", what, err)
	}
	return n, nil
}

// Delete deletes the blobs row unless its CID is under an active operator
// withhold, and queues the object's delete in the same statement.
func (s *S3) Delete(ctx context.Context, q Querier, did, cid string) (int64, error) {
	return deleteAndQueue(ctx, q, "blob "+cid+" for "+did,
		`did = $1 AND cid = $2 AND `+activeOperatorWithhold, did, cid)
}

// DeleteForDID deletes the blobs rows under did whose CID is not under an
// active operator withhold, and queues their objects' deletes.
func (s *S3) DeleteForDID(ctx context.Context, q Querier, did string) (int64, error) {
	return deleteAndQueue(ctx, q, "blobs for "+did, `did = $1 AND `+activeOperatorWithhold, did)
}

// DeleteCID deletes every owner's blobs row of cid and queues their objects'
// deletes.
func (s *S3) DeleteCID(ctx context.Context, q Querier, cid string) (int64, error) {
	return deleteAndQueue(ctx, q, "blobs of "+cid, `cid = $1`, cid)
}

// ProcessObjectDeletes processes the queue rows for did and cid (empty
// matches any). Failures are logged; their rows stay for the sweeper.
func (s *S3) ProcessObjectDeletes(ctx context.Context, did, cid string) {
	_ = s.processObjectDeletes(ctx, did, cid) // every failure is logged where it happens
}

// SweepObjectDeletes runs one sweeper pass over every blob_object_deletes
// row, continuing past failures. Each failure is logged; the returned error
// joins them.
func (s *S3) SweepObjectDeletes(ctx context.Context) error {
	return s.processObjectDeletes(ctx, "", "")
}

// Run sweeps the object delete queue every minute until ctx is done.
func (s *S3) Run(ctx context.Context) {
	ticker := time.NewTicker(objectDeleteSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = s.SweepObjectDeletes(ctx) // every failure is logged by the sweep
		}
	}
}

// queuedObjectDelete is one blob_object_deletes row to process; overdue
// means it was queued more than objectDeleteStuckAge ago.
type queuedObjectDelete struct {
	did, cid string
	overdue  bool
}

// processObjectDeletes processes each queue row matching did and cid (empty
// matches any) and joins the failures.
func (s *S3) processObjectDeletes(ctx context.Context, did, cid string) error {
	pending, err := s.queuedObjectDeletes(ctx, did, cid)
	if err != nil {
		s.logger.Warn("queued blob object deletes not read", "code", "blob_object_delete_failed", "error", err)
		return err
	}
	var failures []error
	for _, row := range pending {
		if err := s.processObjectDelete(ctx, row.did, row.cid); err != nil {
			if row.overdue {
				s.logger.Error("blob object delete stuck", "code", "blob_object_delete_stuck",
					"did", row.did, "cid", row.cid, "error", err)
			}
			failures = append(failures, err)
		}
	}
	return stderrors.Join(failures...)
}

func (s *S3) queuedObjectDeletes(ctx context.Context, did, cid string) ([]queuedObjectDelete, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT did, cid, queued_at < now() - interval '`+objectDeleteStuckAge+`'
		FROM blob_object_deletes
		WHERE ($1 = '' OR did = $1) AND ($2 = '' OR cid = $2)
		ORDER BY did, cid`, did, cid)
	if err != nil {
		return nil, fmt.Errorf("blobstore: list queued object deletes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var pending []queuedObjectDelete
	for rows.Next() {
		var row queuedObjectDelete
		if err := rows.Scan(&row.did, &row.cid, &row.overdue); err != nil {
			return nil, fmt.Errorf("blobstore: scan queued object delete: %w", err)
		}
		pending = append(pending, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("blobstore: list queued object deletes: %w", err)
	}
	return pending, nil
}

// processObjectDelete processes one queue row under the exclusive (did, cid)
// lock: a live blobs row means the object is in use and only the queue row
// goes; otherwise the object is deleted, then the queue row. A failed
// object delete records the attempt and keeps the row.
func (s *S3) processObjectDelete(ctx context.Context, did, cid string) error {
	attempts, err := s.tryObjectDelete(ctx, did, cid)
	if err != nil {
		attributes := []any{"code", "blob_object_delete_failed", "did", did, "cid", cid, "error", err}
		if attempts > 0 {
			attributes = append(attributes, "attempts", attempts)
		}
		s.logger.Warn("blob object delete failed", attributes...)
	}
	return err
}

// tryObjectDelete is processObjectDelete's tx. attempts is the row's
// recorded attempt count after a failed object delete, otherwise 0.
func (s *S3) tryObjectDelete(ctx context.Context, did, cid string) (attempts int, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("blobstore: object delete %s for %s: begin: %w", cid, did, err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := Lock(ctx, tx, did, cid, false); err != nil {
		return 0, err
	}
	// FOR SHARE holds a live row until commit, so a delete of it waits for
	// the queue-row delete below to commit and its enqueue then re-creates
	// the row; a delete not yet committed makes this read wait for it.
	live := true
	var one int
	if err := tx.QueryRowContext(ctx,
		`SELECT 1 FROM blobs WHERE did = $1 AND cid = $2 FOR SHARE`, did, cid).Scan(&one); stderrors.Is(err, sql.ErrNoRows) {
		live = false
	} else if err != nil {
		return 0, fmt.Errorf("blobstore: object delete %s for %s: read row: %w", cid, did, err)
	}
	if !live {
		if removeErr := s.removeObject(ctx, did, cid); removeErr != nil {
			if err := tx.QueryRowContext(ctx, `
				UPDATE blob_object_deletes SET attempts = attempts + 1, last_error = $3
				WHERE did = $1 AND cid = $2 RETURNING attempts`,
				did, cid, removeErr.Error()).Scan(&attempts); err != nil {
				return 0, fmt.Errorf("%w; record attempt: %w", removeErr, err)
			}
			if err := tx.Commit(); err != nil {
				return 0, fmt.Errorf("%w; record attempt: commit: %w", removeErr, err)
			}
			return attempts, removeErr
		}
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM blob_object_deletes WHERE did = $1 AND cid = $2`, did, cid); err != nil {
		return 0, fmt.Errorf("blobstore: object delete %s for %s: drop queue row: %w", cid, did, err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("blobstore: object delete %s for %s: commit: %w", cid, did, err)
	}
	return 0, nil
}

// removeObject deletes the object for (did, cid). A missing object is
// success.
func (s *S3) removeObject(ctx context.Context, did, cid string) error {
	key, err := ObjectKey(did, cid)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, objectRequestTimeout)
	defer cancel()
	if err := s.client.RemoveObject(ctx, s.bucket, key, minio.RemoveObjectOptions{}); err != nil &&
		minio.ToErrorResponse(err).Code != minio.NoSuchKey {
		return fmt.Errorf("blobstore: delete object %s: %w", key, err)
	}
	return nil
}

var _ Store = (*S3)(nil)
