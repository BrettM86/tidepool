package blobstore

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"sort"
	"sync"

	gocid "github.com/ipfs/go-cid"
)

// defaultMigrationPageSize is how many rows one keyset page reads when
// MigrationOptions.PageSize is not positive.
const defaultMigrationPageSize = 100

// BlobKey names one blobs row.
type BlobKey struct{ DID, CID string }

// MigrationReport counts what MigratePostgresBytes did with each row it
// read. Copied rows now have their bytes in the bucket and NULL in the row;
// Skipped rows were deleted or already moved before the copy reached them;
// Mismatched rows hold bytes that do not hash to their CID and were left in
// place (listed in Mismatches, ordered by did then cid); Failed rows could
// not be uploaded or committed and keep their bytes for a re-run.
type MigrationReport struct {
	Copied, Skipped, Mismatched, Failed int
	Mismatches                          []BlobKey
}

// MigrationOptions tunes MigratePostgresBytes. Concurrency is how many rows
// are copied at once (not positive means 1); PageSize is how many rows each
// page reads (not positive means 100).
type MigrationOptions struct{ Concurrency, PageSize int }

// migrationOutcome is what copying one row came to.
type migrationOutcome int

const (
	migrationCopied migrationOutcome = iota
	migrationSkipped
	migrationMismatched
	migrationFailed
	// migrationStopped is a row ctx ended before it finished; it keeps its
	// bytes and is not counted, since the run's error says it stopped early.
	migrationStopped
)

// MigratePostgresBytes moves every blobs row still holding its bytes into
// the bucket. Rows are read in (did, cid) pages and each is copied in its own
// tx under the exclusive (did, cid) lock and a row lock: bytes that hash to
// the CID are uploaded, then the row's bytes are set NULL and committed. A
// row is never NULLed unless its object upload succeeded. Withholds do not
// matter: withheld rows are copied too. The error is non-nil only when a page
// cannot be read or ctx ends; the report then holds the counts so far.
func (s *S3) MigratePostgresBytes(ctx context.Context, options MigrationOptions) (MigrationReport, error) {
	concurrency := options.Concurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	pageSize := options.PageSize
	if pageSize <= 0 {
		pageSize = defaultMigrationPageSize
	}

	var (
		mu     sync.Mutex
		report MigrationReport
	)
	record := func(key BlobKey, outcome migrationOutcome) {
		mu.Lock()
		defer mu.Unlock()
		switch outcome {
		case migrationCopied:
			report.Copied++
		case migrationSkipped:
			report.Skipped++
		case migrationMismatched:
			report.Mismatched++
			report.Mismatches = append(report.Mismatches, key)
		case migrationFailed:
			report.Failed++
		}
	}
	finish := func(err error) (MigrationReport, error) {
		mu.Lock()
		defer mu.Unlock()
		sort.Slice(report.Mismatches, func(i, j int) bool {
			if report.Mismatches[i].DID != report.Mismatches[j].DID {
				return report.Mismatches[i].DID < report.Mismatches[j].DID
			}
			return report.Mismatches[i].CID < report.Mismatches[j].CID
		})
		return report, err
	}

	var cursor BlobKey
	for {
		if err := ctx.Err(); err != nil {
			return finish(fmt.Errorf("blobstore: migrate: %w", err))
		}
		page, err := s.migrationPage(ctx, cursor, pageSize)
		if err != nil {
			return finish(err)
		}
		if len(page) == 0 {
			return finish(nil)
		}
		cursor = page[len(page)-1]

		keys := make(chan BlobKey)
		var workers sync.WaitGroup
		for range min(concurrency, len(page)) {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for key := range keys {
					record(key, s.migrateRow(ctx, key))
				}
			}()
		}
	feed:
		for _, key := range page {
			select {
			case keys <- key:
			case <-ctx.Done():
				break feed
			}
		}
		close(keys)
		workers.Wait()
	}
}

// migrationPage reads up to pageSize keys of rows holding bytes, after
// cursor in (did, cid) order.
func (s *S3) migrationPage(ctx context.Context, cursor BlobKey, pageSize int) ([]BlobKey, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT did, cid FROM blobs
		WHERE bytes IS NOT NULL AND (did, cid) > ($1, $2)
		ORDER BY did, cid
		LIMIT $3`, cursor.DID, cursor.CID, pageSize)
	if err != nil {
		return nil, fmt.Errorf("blobstore: migrate: read page after %s/%s: %w", cursor.DID, cursor.CID, err)
	}
	defer func() { _ = rows.Close() }()
	var page []BlobKey
	for rows.Next() {
		var key BlobKey
		if err := rows.Scan(&key.DID, &key.CID); err != nil {
			return nil, fmt.Errorf("blobstore: migrate: scan page: %w", err)
		}
		page = append(page, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("blobstore: migrate: read page after %s/%s: %w", cursor.DID, cursor.CID, err)
	}
	return page, nil
}

// errMigrationUpload marks a row copy that failed at the object upload.
var errMigrationUpload = stderrors.New("blob_object_put_failed")

// migrateRow copies one row in its own tx. Every failure is logged here with
// the row's did and cid and counted as failed; the row keeps its bytes. A
// failure after ctx ended is the run stopping, not the row failing: it is
// neither logged nor counted.
func (s *S3) migrateRow(ctx context.Context, key BlobKey) migrationOutcome {
	outcome, err := s.tryMigrateRow(ctx, key)
	if err != nil {
		if ctx.Err() != nil {
			return migrationStopped
		}
		code := "blob_migration_failed"
		if stderrors.Is(err, errMigrationUpload) {
			code = "blob_object_put_failed"
		}
		s.logger.Error("blob migration failed", "code", code, "did", key.DID, "cid", key.CID, "error", err)
		return migrationFailed
	}
	return outcome
}

func (s *S3) tryMigrateRow(ctx context.Context, key BlobKey) (migrationOutcome, error) {
	objectKey, err := ObjectKey(key.DID, key.CID)
	if err != nil {
		return migrationFailed, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return migrationFailed, fmt.Errorf("blobstore: migrate %s for %s: begin: %w", key.CID, key.DID, err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := Lock(ctx, tx, key.DID, key.CID, false); err != nil {
		return migrationFailed, err
	}
	var (
		mimeType string
		data     []byte
	)
	err = tx.QueryRowContext(ctx,
		`SELECT mime_type, bytes FROM blobs WHERE did = $1 AND cid = $2 FOR UPDATE`,
		key.DID, key.CID).Scan(&mimeType, &data)
	switch {
	case stderrors.Is(err, sql.ErrNoRows):
		return migrationSkipped, nil
	case err != nil:
		return migrationFailed, fmt.Errorf("blobstore: migrate %s for %s: read row: %w", key.CID, key.DID, err)
	case data == nil:
		return migrationSkipped, nil
	}
	if !bytesMatchCID(key.CID, data) {
		return migrationMismatched, nil
	}
	if err := s.putObject(ctx, objectKey, mimeType, data); err != nil {
		return migrationFailed, fmt.Errorf("%w: %w", errMigrationUpload, err)
	}
	result, err := tx.ExecContext(ctx,
		`UPDATE blobs SET bytes = NULL WHERE did = $1 AND cid = $2 AND bytes IS NOT NULL`, key.DID, key.CID)
	if err != nil {
		return migrationFailed, fmt.Errorf("blobstore: migrate %s for %s: clear bytes: %w", key.CID, key.DID, err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return migrationFailed, fmt.Errorf("blobstore: migrate %s for %s: clear bytes: %w", key.CID, key.DID, err)
	}
	if updated == 0 {
		return migrationSkipped, nil
	}
	if err := tx.Commit(); err != nil {
		return migrationFailed, fmt.Errorf("blobstore: migrate %s for %s: commit: %w", key.CID, key.DID, err)
	}
	return migrationCopied, nil
}

// bytesMatchCID reports whether data hashes to cid under cid's own codec and
// multihash type. An unparseable cid never matches.
func bytesMatchCID(cid string, data []byte) bool {
	parsed, err := gocid.Parse(cid)
	if err != nil {
		return false
	}
	computed, err := parsed.Prefix().Sum(data)
	if err != nil {
		return false
	}
	return computed.Equals(parsed)
}
