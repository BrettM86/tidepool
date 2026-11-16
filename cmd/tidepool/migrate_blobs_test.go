package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/blobstore"
	"tidepool/internal/blobwithhold"
	tidepoolerrors "tidepool/internal/errors"
	"tidepool/internal/repo"
	"tidepool/internal/testutil"
)

// acceptCountingListener is a loopback TCP listener standing in for the
// database: every connection the command opens to DATABASE_URL is counted.
type acceptCountingListener struct {
	listener net.Listener
	accepts  atomic.Int64
	done     chan struct{}
}

func newAcceptCountingListener(t *testing.T) *acceptCountingListener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	counting := &acceptCountingListener{listener: listener, done: make(chan struct{})}
	go func() {
		defer close(counting.done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			counting.accepts.Add(1)
			_ = conn.Close()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-counting.done
	})
	return counting
}

// databaseURL is a postgres URL that dials the listener.
func (l *acceptCountingListener) databaseURL() string {
	return fmt.Sprintf("postgres://migrate:migrate@%s/migrate?sslmode=disable&connect_timeout=2", l.listener.Addr())
}

// acceptsSoFar closes the listener and returns how many connections it took.
func (l *acceptCountingListener) acceptsSoFar() int64 {
	_ = l.listener.Close()
	<-l.done
	return l.accepts.Load()
}

// recordingTransport counts the requests it carries, in all and by method.
// A nil next refuses every request; otherwise it forwards.
type recordingTransport struct {
	next     http.RoundTripper
	requests atomic.Int64

	mu       sync.Mutex
	byMethod map[string]int
}

// sent is how many requests with method it carried.
func (r *recordingTransport) sent(method string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.byMethod[method]
}

func (r *recordingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	r.requests.Add(1)
	r.mu.Lock()
	if r.byMethod == nil {
		r.byMethod = map[string]int{}
	}
	r.byMethod[request.Method]++
	r.mu.Unlock()
	if r.next == nil {
		return nil, errors.New("recording transport: no request may be sent")
	}
	return r.next.RoundTrip(request)
}

// TestRunMigrateBlobsRefusesUnlessS3 (B1a): BLOB_STORE unset or postgres is
// refused before the database is dialled or any request is sent.
func TestRunMigrateBlobsRefusesUnlessS3(t *testing.T) {
	for _, tc := range []struct {
		name      string
		blobStore string
	}{
		{name: "BLOB_STORE unset", blobStore: ""},
		{name: "BLOB_STORE postgres", blobStore: "postgres"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			database := newAcceptCountingListener(t)
			transport := &recordingTransport{}
			t.Setenv("DATABASE_URL", database.databaseURL())
			t.Setenv("ENVIRONMENT", "development")
			t.Setenv("BLOB_STORE", tc.blobStore)

			err := runMigrateBlobs(context.Background(), testLogger(), 4, transport)

			require.ErrorIs(t, err, errBlobStoreNotS3)
			assert.Zero(t, database.acceptsSoFar(), "database connections")
			assert.Zero(t, transport.requests.Load(), "bucket requests")
		})
	}
}

// TestRunMigrateBlobsRefusesNonLocalEndpointInDevelopment (B3): ENVIRONMENT
// unset is development, where a non-local BLOB_S3_ENDPOINT is refused before
// the database is dialled or any request is sent.
func TestRunMigrateBlobsRefusesNonLocalEndpointInDevelopment(t *testing.T) {
	database := newAcceptCountingListener(t)
	transport := &recordingTransport{}
	t.Setenv("DATABASE_URL", database.databaseURL())
	t.Setenv("ENVIRONMENT", "")
	t.Setenv("BLOB_STORE", "s3")
	t.Setenv("BLOB_S3_ENDPOINT", "https://s3.example.com")
	t.Setenv("BLOB_S3_REGION", "gra")
	t.Setenv("BLOB_S3_BUCKET", "tidepool-blobs")
	t.Setenv("BLOB_S3_ACCESS_KEY_ID", "access-key")
	t.Setenv("BLOB_S3_SECRET_ACCESS_KEY", "secret-key")

	err := runMigrateBlobs(context.Background(), testLogger(), 4, transport)

	require.Error(t, err)
	assert.NotErrorIs(t, err, errBlobStoreNotS3)
	assert.Zero(t, database.acceptsSoFar(), "database connections")
	assert.Zero(t, transport.requests.Load(), "bucket requests")
}

// TestParseMigrateBlobsArgs (B8): --concurrency defaults to 4 and must be a
// positive integer; any other argument is refused.
func TestParseMigrateBlobsArgs(t *testing.T) {
	for _, tc := range []struct {
		name            string
		args            []string
		wantConcurrency int
		wantError       bool
	}{
		{name: "no arguments", args: []string{}, wantConcurrency: 4},
		{name: "concurrency 8", args: []string{"--concurrency", "8"}, wantConcurrency: 8},
		{name: "concurrency 0", args: []string{"--concurrency", "0"}, wantError: true},
		{name: "concurrency -1", args: []string{"--concurrency", "-1"}, wantError: true},
		{name: "concurrency not an integer", args: []string{"--concurrency", "x"}, wantError: true},
		{name: "unknown flag", args: []string{"--bogus"}, wantError: true},
		{name: "extra positional", args: []string{"extra"}, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			concurrency, err := parseMigrateBlobsArgs(tc.args)
			if tc.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantConcurrency, concurrency)
		})
	}
}

// TestDispatchMigrateBlobs (B8): dispatch routes migrate-blobs to the
// command, and refuses a bad --concurrency before running it.
func TestDispatchMigrateBlobs(t *testing.T) {
	t.Run("routes to the command", func(t *testing.T) {
		t.Setenv("BLOB_STORE", "postgres")
		err := dispatch(testLogger(), []string{"migrate-blobs"})
		require.ErrorIs(t, err, errBlobStoreNotS3)
	})
	t.Run("concurrency 0 is refused before the command runs", func(t *testing.T) {
		t.Setenv("BLOB_STORE", "postgres")
		err := dispatch(testLogger(), []string{"migrate-blobs", "--concurrency", "0"})
		require.Error(t, err)
		assert.NotErrorIs(t, err, errBlobStoreNotS3)
		assert.Contains(t, err.Error(), "concurrency", "the refusal names the rejected flag")
	})
}

// migrateBlobsSummary is the summary line's counts.
type migrateBlobsSummary struct {
	Copied, Skipped, Mismatched, Failed int
}

// summaryFromLog returns the counts of the one JSON log line carrying all of
// copied, skipped, mismatched and failed.
func summaryFromLog(t *testing.T, log *bytes.Buffer) migrateBlobsSummary {
	t.Helper()
	var found []migrateBlobsSummary
	scanner := bufio.NewScanner(bytes.NewReader(log.Bytes()))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		var line map[string]any
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &line))
		counts := make([]int, 0, 4)
		for _, key := range []string{"copied", "skipped", "mismatched", "failed"} {
			value, ok := line[key].(float64)
			if !ok {
				break
			}
			counts = append(counts, int(value))
		}
		if len(counts) == 4 {
			found = append(found, migrateBlobsSummary{counts[0], counts[1], counts[2], counts[3]})
		}
	}
	require.NoError(t, scanner.Err())
	require.Len(t, found, 1, "exactly one summary line with copied, skipped, mismatched and failed")
	return found[0]
}

// migrateBlobsEnv sets exactly the variables migrate-blobs reads, pointed at
// the test database and bucket, and leaves config.Load unable to succeed.
func migrateBlobsEnv(t *testing.T, bucket *testutil.S3Bucket) {
	t.Helper()
	t.Setenv("DATABASE_URL", os.Getenv("TIDEPOOL_TEST_DATABASE_URL"))
	t.Setenv("ENVIRONMENT", "development")
	t.Setenv("BLOB_STORE", "s3")
	t.Setenv("BLOB_S3_ENDPOINT", bucket.Endpoint)
	t.Setenv("BLOB_S3_REGION", bucket.Region)
	t.Setenv("BLOB_S3_BUCKET", bucket.Name)
	t.Setenv("BLOB_S3_ACCESS_KEY_ID", bucket.AccessKeyID)
	t.Setenv("BLOB_S3_SECRET_ACCESS_KEY", bucket.SecretAccessKey)
	// An accidental config.Load fails on these.
	t.Setenv("BRIDGE_KEK", "not-a-key")
	t.Setenv("BRIDGE_SERVICE_DID", "")
}

// TestRunMigrateBlobsCopiesEveryRow (B2, acceptance): five rows with
// Postgres bytes across two DIDs, one under an active operator withhold and
// one under an active community withhold, all move to the bucket; getBlob
// serves the moved bytes, withheld rows stay hidden until lifted, and a
// second run copies nothing.
func TestRunMigrateBlobsCopiesEveryRow(t *testing.T) {
	const (
		firstDID     = "did:plc:migrateblobsfirstaaaaaaa"
		secondDID    = "did:plc:migrateblobssecondaaaaaa"
		communityDID = "did:plc:migrateblobscommunityaaa"
	)
	ctx := context.Background()
	database := testutil.DB(t)
	testutil.Truncate(t, database, "blobs", "blob_withholds", "blob_object_deletes")
	bucket := testutil.NewS3Bucket(t)

	key, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	seeder, err := repo.NewManager(database, checkerKeys{key: key}, testLogger())
	require.NoError(t, err)

	type seeded struct {
		did  string
		data []byte
		cid  string
	}
	rows := []seeded{
		{did: firstDID, data: []byte("migrate-blobs first one")},
		{did: firstDID, data: []byte("migrate-blobs first two")},
		{did: firstDID, data: []byte("migrate-blobs operator withheld")},
		{did: secondDID, data: []byte("migrate-blobs second one")},
		{did: secondDID, data: []byte("migrate-blobs community withheld")},
	}
	for i := range rows {
		rows[i].cid = testutil.RawBlobCID(t, rows[i].data)
		ref, err := seeder.PutBlob(ctx, rows[i].did, "image/png", rows[i].data)
		require.NoError(t, err)
		require.Equal(t, rows[i].cid, ref.Ref.String())
	}
	operatorWithheld, communityWithheld := rows[2], rows[4]

	withholds := blobwithhold.NewStore(database, blobstore.NewPostgres(database))
	_, err = withholds.Withhold(ctx, blobwithhold.OperatorRequest{
		CID: operatorWithheld.cid, ReasonCode: blobwithhold.ReasonOperatorDiscretion, Actor: "migrate-blobs-test",
	})
	require.NoError(t, err)
	require.NoError(t, blobwithhold.AddCommunityLabelWithholds(ctx, database, communityDID, communityWithheld.did, []string{communityWithheld.cid}))

	migrateBlobsEnv(t, bucket)

	var firstLog bytes.Buffer
	require.NoError(t, runMigrateBlobs(ctx, slog.New(slog.NewJSONHandler(&firstLog, nil)), 4, testutil.LocalS3Transport()))

	for _, row := range rows {
		var bytesNull bool
		require.NoError(t, database.QueryRowContext(ctx,
			`SELECT bytes IS NULL FROM blobs WHERE did = $1 AND cid = $2`, row.did, row.cid).Scan(&bytesNull))
		assert.True(t, bytesNull, "row %s/%s has NULL bytes", row.did, row.cid)
		assert.Equal(t, []testutil.StoredBlob{{DID: row.did, CID: row.cid}},
			testutil.BlobsHoldingBytes(t, database, bucket, row.data),
			"the object for %s/%s holds the original bytes", row.did, row.cid)
	}
	assert.Equal(t, migrateBlobsSummary{Copied: 5}, summaryFromLog(t, &firstLog))

	s3Store, err := blobstore.NewS3(database, blobstore.S3Config{
		Endpoint:              bucket.Endpoint,
		Region:                bucket.Region,
		Bucket:                bucket.Name,
		AccessKeyID:           bucket.AccessKeyID,
		SecretAccessKey:       bucket.SecretAccessKey,
		AllowPrivateAddresses: true,
		Logger:                testLogger(),
		Transport:             testutil.LocalS3Transport(),
	})
	require.NoError(t, err)
	reader, err := repo.NewManager(database, checkerKeys{key: key}, testLogger(), repo.WithBlobStore(s3Store))
	require.NoError(t, err)

	served := func(row seeded) {
		t.Helper()
		data, _, err := reader.GetBlob(ctx, row.did, row.cid)
		require.NoError(t, err, "getBlob %s/%s", row.did, row.cid)
		assert.Equal(t, row.data, data, "getBlob %s/%s serves the original bytes", row.did, row.cid)
	}
	for _, row := range []seeded{rows[0], rows[1], rows[3]} {
		served(row)
	}
	for _, row := range []seeded{operatorWithheld, communityWithheld} {
		_, _, err := reader.GetBlob(ctx, row.did, row.cid)
		assert.True(t, tidepoolerrors.IsNotFound(err), "withheld %s/%s is not found, got %v", row.did, row.cid, err)
	}

	_, err = withholds.Lift(ctx, operatorWithheld.cid, "migrate-blobs-test")
	require.NoError(t, err)
	require.NoError(t, blobwithhold.LiftCommunityLabelWithholds(ctx, database, communityDID))
	served(operatorWithheld)
	served(communityWithheld)

	var secondLog bytes.Buffer
	require.NoError(t, runMigrateBlobs(ctx, slog.New(slog.NewJSONHandler(&secondLog, nil)), 4, testutil.LocalS3Transport()))
	assert.Equal(t, migrateBlobsSummary{}, summaryFromLog(t, &secondLog))
}

// seedPostgresBlob stores data for did on the postgres byte store, so its
// row holds the bytes, and returns its CID.
func seedPostgresBlob(t *testing.T, database *sql.DB, did string, data []byte) string {
	t.Helper()
	key, err := atcrypto.GeneratePrivateKeyK256()
	require.NoError(t, err)
	seeder, err := repo.NewManager(database, checkerKeys{key: key}, testLogger())
	require.NoError(t, err)
	ref, err := seeder.PutBlob(context.Background(), did, "image/png", data)
	require.NoError(t, err)
	want := testutil.RawBlobCID(t, data)
	require.Equal(t, want, ref.Ref.String())
	return want
}

// requireNoObject fails unless the bucket has no object for (did, cid).
func requireNoObject(t *testing.T, bucket *testutil.S3Bucket, did, cid string) {
	t.Helper()
	_, found := testutil.ReadS3Object(t, bucket, "blobs/"+did+"/"+cid)
	assert.False(t, found, "no object for %s/%s", did, cid)
}

// migrateBlobsDB is the test database with the blob tables empty.
func migrateBlobsDB(t *testing.T) *sql.DB {
	t.Helper()
	database := testutil.DB(t)
	testutil.Truncate(t, database, "blobs", "blob_withholds", "blob_object_deletes")
	return database
}

// TestRunMigrateBlobsFailsOnTheBucketHead (B1b): a bucket absent on the test
// MinIO fails the startup HEAD before anything is copied: a HEAD was sent,
// no PUT, and the row keeps its bytes.
func TestRunMigrateBlobsFailsOnTheBucketHead(t *testing.T) {
	const did = "did:plc:migrateblobsheadaaaaaaaa"
	data := []byte("migrate-blobs wrong bucket")
	database := migrateBlobsDB(t)
	bucket := testutil.NewS3Bucket(t)
	cid := seedPostgresBlob(t, database, did, data)
	migrateBlobsEnv(t, bucket)
	t.Setenv("BLOB_S3_BUCKET", bucket.Name+"-absent")
	transport := &recordingTransport{next: testutil.LocalS3Transport()}

	err := runMigrateBlobs(context.Background(), testLogger(), 4, transport)

	require.Error(t, err)
	assert.Positive(t, transport.sent(http.MethodHead), "the bucket HEAD was sent")
	assert.Zero(t, transport.sent(http.MethodPut), "no object was uploaded")
	assert.Equal(t, []testutil.StoredBlob{{DID: did, CID: cid}},
		testutil.BlobsHoldingBytes(t, database, nil, data), "the row keeps its bytes")
}

// TestRunMigrateBlobsLeavesAMismatchedRow (B6): a row whose bytes do not
// hash to its CID keeps its bytes, gets no object, is counted mismatched and
// logged by did and cid; the good row is copied and the run succeeds.
func TestRunMigrateBlobsLeavesAMismatchedRow(t *testing.T) {
	const (
		goodDID = "did:plc:migrateblobsgoodaaaaaaaa"
		badDID  = "did:plc:migrateblobsbadaaaaaaaaa"
	)
	ctx := context.Background()
	goodData := []byte("migrate-blobs good row")
	badData := []byte("migrate-blobs bytes that are not the claimed blob")
	database := migrateBlobsDB(t)
	bucket := testutil.NewS3Bucket(t)
	goodCID := seedPostgresBlob(t, database, goodDID, goodData)
	badCID := testutil.RawBlobCID(t, []byte("migrate-blobs claimed blob"))
	_, err := database.ExecContext(ctx, `
		INSERT INTO blobs (did, cid, mime_type, size, bytes) VALUES ($1, $2, 'image/png', $3, $4)`,
		badDID, badCID, len(badData), badData)
	require.NoError(t, err)
	migrateBlobsEnv(t, bucket)

	var log bytes.Buffer
	err = runMigrateBlobs(ctx, slog.New(slog.NewJSONHandler(&log, nil)), 4, testutil.LocalS3Transport())

	require.NoError(t, err, "a mismatch alone does not fail the run")
	assert.Equal(t, migrateBlobsSummary{Copied: 1, Mismatched: 1}, summaryFromLog(t, &log))
	assert.True(t, logHasLineFor(t, &log, badDID, badCID), "a log line names the mismatched row's did and cid")
	assert.Equal(t, []testutil.StoredBlob{{DID: badDID, CID: badCID}},
		testutil.BlobsHoldingBytes(t, database, bucket, badData), "the mismatched row keeps its bytes")
	requireNoObject(t, bucket, badDID, badCID)
	assert.Equal(t, []testutil.StoredBlob{{DID: goodDID, CID: goodCID}},
		testutil.BlobsHoldingBytes(t, database, bucket, goodData), "the good row's bytes are in its object")
	var goodNull bool
	require.NoError(t, database.QueryRowContext(ctx,
		`SELECT bytes IS NULL FROM blobs WHERE did = $1 AND cid = $2`, goodDID, goodCID).Scan(&goodNull))
	assert.True(t, goodNull, "the good row has NULL bytes")
}

// logHasLineFor reports whether a JSON log line carries did and cid.
func logHasLineFor(t *testing.T, log *bytes.Buffer, did, cid string) bool {
	t.Helper()
	scanner := bufio.NewScanner(bytes.NewReader(log.Bytes()))
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		var line map[string]any
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &line))
		if line["did"] == did && line["cid"] == cid {
			return true
		}
	}
	require.NoError(t, scanner.Err())
	return false
}

// cutAtFirstPut cuts the MinIO proxy the moment the first PUT is sent, so
// the bucket HEAD before it goes through and the upload's connection fails.
type cutAtFirstPut struct {
	next  http.RoundTripper
	proxy *testutil.MinIOCut
	once  sync.Once
}

func (c *cutAtFirstPut) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.Method == http.MethodPut {
		c.once.Do(c.proxy.Cut)
	}
	return c.next.RoundTrip(request)
}

// TestRunMigrateBlobsMinIOCutAtTheUpload (B7): MinIO cut after the bucket
// HEAD, at the upload, fails the run with the row counted failed, its bytes
// kept and no object; once MinIO is back a re-run copies it.
func TestRunMigrateBlobsMinIOCutAtTheUpload(t *testing.T) {
	const did = "did:plc:migrateblobscutaaaaaaaaa"
	ctx := context.Background()
	data := []byte("migrate-blobs cut at the upload")
	database := migrateBlobsDB(t)
	bucket := testutil.NewS3Bucket(t)
	cid := seedPostgresBlob(t, database, did, data)
	endpoint, err := url.Parse(bucket.Endpoint)
	require.NoError(t, err)
	proxy := testutil.NewMinIOCut(t, endpoint.Host)
	migrateBlobsEnv(t, bucket)
	t.Setenv("BLOB_S3_ENDPOINT", proxy.Endpoint())

	var cutLog bytes.Buffer
	err = runMigrateBlobs(ctx, slog.New(slog.NewJSONHandler(&cutLog, nil)), 4,
		&cutAtFirstPut{next: testutil.LocalS3Transport(), proxy: proxy})

	require.Error(t, err, "a failed upload fails the run")
	assert.Equal(t, migrateBlobsSummary{Failed: 1}, summaryFromLog(t, &cutLog))
	assert.Equal(t, []testutil.StoredBlob{{DID: did, CID: cid}},
		testutil.BlobsHoldingBytes(t, database, nil, data), "the row keeps its bytes")
	requireNoObject(t, bucket, did, cid)

	proxy.Restore()
	var rerunLog bytes.Buffer
	require.NoError(t, runMigrateBlobs(ctx, slog.New(slog.NewJSONHandler(&rerunLog, nil)), 4, testutil.LocalS3Transport()))
	assert.Equal(t, migrateBlobsSummary{Copied: 1}, summaryFromLog(t, &rerunLog))
	var bytesNull bool
	require.NoError(t, database.QueryRowContext(ctx,
		`SELECT bytes IS NULL FROM blobs WHERE did = $1 AND cid = $2`, did, cid).Scan(&bytesNull))
	assert.True(t, bytesNull, "the re-run NULLs the row")
	assert.Equal(t, []testutil.StoredBlob{{DID: did, CID: cid}},
		testutil.BlobsHoldingBytes(t, database, bucket, data), "the object holds the original bytes")
}

// cancelAtFirstPut cancels the run's context the moment the first PUT is
// sent, then forwards it, so the bucket HEAD before it goes through and that
// upload fails with the cancellation.
type cancelAtFirstPut struct {
	next   http.RoundTripper
	cancel context.CancelFunc
	once   sync.Once
}

func (c *cancelAtFirstPut) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.Method == http.MethodPut {
		c.once.Do(c.cancel)
	}
	return c.next.RoundTrip(request)
}

// TestRunMigrateBlobsStoppedEarly (B7): a run cancelled at its first upload,
// as SIGINT or SIGTERM would, fails with the cancellation, still logs the
// summary, counts no more rows failed than were in flight, raises no
// blob_object_put_failed alarm, and every row keeps its bytes.
func TestRunMigrateBlobsStoppedEarly(t *testing.T) {
	const (
		did         = "did:plc:migrateblobsstoppedaaaa"
		rowCount    = 6
		concurrency = 1
	)
	database := migrateBlobsDB(t)
	bucket := testutil.NewS3Bucket(t)
	payloads := make([][]byte, rowCount)
	cids := make([]string, rowCount)
	for i := range rowCount {
		payloads[i] = []byte(fmt.Sprintf("migrate-blobs stopped early %d", i))
		cids[i] = seedPostgresBlob(t, database, did, payloads[i])
	}
	migrateBlobsEnv(t, bucket)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var log bytes.Buffer
	err := runMigrateBlobs(ctx, slog.New(slog.NewJSONHandler(&log, nil)), concurrency,
		&cancelAtFirstPut{next: testutil.LocalS3Transport(), cancel: cancel})

	require.ErrorIs(t, err, context.Canceled, "a stopped run fails with the cancellation")
	summary := summaryFromLog(t, &log)
	assert.LessOrEqual(t, summary.Failed, concurrency, "only rows in flight at the stop may count failed")
	assert.Zero(t, summary.Copied, "nothing was copied")
	assert.NotContains(t, log.String(), "blob_object_put_failed", "a cancelled upload is not an upload alarm")
	for i := range rowCount {
		assert.Equal(t, []testutil.StoredBlob{{DID: did, CID: cids[i]}},
			testutil.BlobsHoldingBytes(t, database, nil, payloads[i]), "row %d keeps its bytes", i)
		requireNoObject(t, bucket, did, cids[i])
	}
}

// TestRunMigrateBlobsProductionKeepsTheEgressGuard (B3): with the production
// transport (nil) and ENVIRONMENT=production, the egress guard refuses the
// loopback test MinIO at the bucket HEAD, so nothing is uploaded and the row
// keeps its bytes. Go never proxies a loopback address, so no proxy variable
// can route this request elsewhere.
func TestRunMigrateBlobsProductionKeepsTheEgressGuard(t *testing.T) {
	const did = "did:plc:migrateblobsguardaaaaaaa"
	data := []byte("migrate-blobs production guard")
	database := migrateBlobsDB(t)
	bucket := testutil.NewS3Bucket(t)
	cid := seedPostgresBlob(t, database, did, data)
	migrateBlobsEnv(t, bucket)
	t.Setenv("ENVIRONMENT", "production")

	err := runMigrateBlobs(context.Background(), testLogger(), 4, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "s3 bucket check", "the refusal comes at the bucket HEAD")
	assert.Contains(t, err.Error(), "is not an allowed egress target", "the egress guard refused the loopback endpoint")
	requireNoObject(t, bucket, did, cid)
	assert.Equal(t, []testutil.StoredBlob{{DID: did, CID: cid}},
		testutil.BlobsHoldingBytes(t, database, nil, data), "the row keeps its bytes")
}

// TestRunMigrateBlobsDevelopmentAllowsALocalEndpoint (B3): with the
// production transport (nil) and ENVIRONMENT=development, the guard lets the
// loopback test MinIO through and the row is copied.
func TestRunMigrateBlobsDevelopmentAllowsALocalEndpoint(t *testing.T) {
	const did = "did:plc:migrateblobsdevelopmentaa"
	ctx := context.Background()
	data := []byte("migrate-blobs development guard")
	database := migrateBlobsDB(t)
	bucket := testutil.NewS3Bucket(t)
	cid := seedPostgresBlob(t, database, did, data)
	migrateBlobsEnv(t, bucket)

	var log bytes.Buffer
	require.NoError(t, runMigrateBlobs(ctx, slog.New(slog.NewJSONHandler(&log, nil)), 4, nil))

	assert.Equal(t, migrateBlobsSummary{Copied: 1}, summaryFromLog(t, &log))
	assert.Equal(t, []testutil.StoredBlob{{DID: did, CID: cid}},
		testutil.BlobsHoldingBytes(t, database, bucket, data), "the object holds the original bytes")
	var bytesNull bool
	require.NoError(t, database.QueryRowContext(ctx,
		`SELECT bytes IS NULL FROM blobs WHERE did = $1 AND cid = $2`, did, cid).Scan(&bytesNull))
	assert.True(t, bytesNull, "the row has NULL bytes")
}
