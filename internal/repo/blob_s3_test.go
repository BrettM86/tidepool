package repo

import (
	"bytes"
	"context"
	"database/sql"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/blobstore"
	"tidepool/internal/blobwithhold"
	"tidepool/internal/errors"
	"tidepool/internal/testutil"
)

// recordedS3Request is one request the S3 store sent through an s3Proxy.
type recordedS3Request struct {
	Method string
	// Path is the decoded request path: /<bucket>/<object key> for an
	// object request (path-style addressing on a loopback endpoint).
	Path   string
	Header http.Header
	// RejectedWith is the status the hook answered with instead of
	// forwarding; 0 when the request reached MinIO.
	RejectedWith int
}

// s3ProxyHook sees every request before it is forwarded. It may run an
// action first (stall, mutate the bucket out of band, cancel something);
// returning a non-zero status answers the request with that status instead
// of forwarding it to MinIO.
type s3ProxyHook func(r *http.Request) (rejectStatus int)

// s3ProxyResponder sees every request the hook let through. Returning true
// means it answered the request itself, which is then not forwarded.
type s3ProxyResponder func(w http.ResponseWriter, r *http.Request) (handled bool)

// s3Proxy is a loopback recording reverse proxy in front of a test bucket's
// MinIO. Point the S3 store's Endpoint at URL to observe (Requests) or
// interfere with (SetHook) every request it makes; Close cuts MinIO off.
type s3Proxy struct {
	URL    string
	server *httptest.Server

	mu        sync.Mutex
	requests  []recordedS3Request
	hook      s3ProxyHook
	responder s3ProxyResponder
}

// newS3Proxy starts an s3Proxy forwarding to bucket's endpoint, closed at
// cleanup. The Host header is passed through unchanged, so SigV4 signatures
// made for the proxy's address verify at MinIO.
func newS3Proxy(t *testing.T, bucket *testutil.S3Bucket) *s3Proxy {
	t.Helper()
	target, err := url.Parse(bucket.Endpoint)
	require.NoError(t, err, "test bucket endpoint")
	require.NotEmpty(t, target.Host, "test bucket endpoint %q has no host", bucket.Endpoint)
	forward := httputil.NewSingleHostReverseProxy(target)
	forward.Transport = testutil.LocalS3Transport()
	proxy := &s3Proxy{}
	proxy.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxy.mu.Lock()
		hook := proxy.hook
		proxy.mu.Unlock()
		status := 0
		if hook != nil {
			status = hook(r)
		}
		proxy.mu.Lock()
		proxy.requests = append(proxy.requests, recordedS3Request{
			Method:       r.Method,
			Path:         r.URL.Path,
			Header:       r.Header.Clone(),
			RejectedWith: status,
		})
		proxy.mu.Unlock()
		if status != 0 {
			w.WriteHeader(status)
			return
		}
		proxy.mu.Lock()
		responder := proxy.responder
		proxy.mu.Unlock()
		if responder != nil && responder(w, r) {
			return
		}
		forward.ServeHTTP(w, r)
	}))
	proxy.URL = proxy.server.URL
	t.Cleanup(proxy.server.Close)
	return proxy
}

// SetHook installs (or, with nil, removes) the per-request hook.
func (p *s3Proxy) SetHook(hook s3ProxyHook) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.hook = hook
}

// SetResponder installs (or, with nil, removes) the responder.
func (p *s3Proxy) SetResponder(responder s3ProxyResponder) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.responder = responder
}

// Requests returns the requests recorded so far, oldest first.
func (p *s3Proxy) Requests() []recordedS3Request {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]recordedS3Request(nil), p.requests...)
}

// RequestsWithMethod returns the recorded requests with the given method.
func (p *s3Proxy) RequestsWithMethod(method string) []recordedS3Request {
	var matched []recordedS3Request
	for _, request := range p.Requests() {
		if request.Method == method {
			matched = append(matched, request)
		}
	}
	return matched
}

// Reset forgets the recorded requests.
func (p *s3Proxy) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requests = nil
}

// Close stops the proxy: later S3 requests through it fail to connect.
func (p *s3Proxy) Close() { p.server.Close() }

// s3TestEnv is an s3-mode Manager over a fresh test bucket, reached through
// a recording proxy.
type s3TestEnv struct {
	Manager  *Manager
	DB       *sql.DB
	Logs     *bytes.Buffer
	Logger   *slog.Logger
	Bucket   *testutil.S3Bucket
	Proxy    *s3Proxy
	Store    *blobstore.S3
	Withhold *blobwithhold.Store
}

// s3Manager builds a Manager in s3 mode: blobstore.NewS3 over a fresh test
// bucket, with the store's endpoint at a recording proxy in front of MinIO,
// injected with WithBlobStore. The store and the Manager log into one
// buffer, captured as in loggedManager; blobs, blob_object_deletes and
// blob_withholds are empty before and after the test. Withhold deletes
// bytes through the same store.
func s3Manager(t *testing.T, opts ...Option) *s3TestEnv {
	t.Helper()
	bucket := testutil.NewS3Bucket(t)
	require.NotNil(t, bucket.Client, "testutil.NewS3Bucket must return a bucket with a client")
	proxy := newS3Proxy(t, bucket)
	database := testutil.DB(t)
	testutil.Truncate(t, database, "blob_withholds")
	t.Cleanup(func() { testutil.Truncate(t, database, "blob_withholds") })
	logger, logs := capturedLogger()
	store, err := blobstore.NewS3(database, blobstore.S3Config{
		Endpoint:              proxy.URL,
		Region:                bucket.Region,
		Bucket:                bucket.Name,
		AccessKeyID:           bucket.AccessKeyID,
		SecretAccessKey:       bucket.SecretAccessKey,
		AllowPrivateAddresses: true,
		Logger:                logger,
		Transport:             testutil.LocalS3Transport(),
	})
	require.NoError(t, err)
	manager, database := managerWithLogger(t, logger, append([]Option{WithBlobStore(store)}, opts...)...)
	return &s3TestEnv{
		Manager:  manager,
		DB:       database,
		Logs:     logs,
		Logger:   logger,
		Bucket:   bucket,
		Proxy:    proxy,
		Store:    store,
		Withhold: blobwithhold.NewStore(database, store),
	}
}

// blobModeEnv is a Manager over one byte-store mode, with that mode's store.
// Bucket and Proxy are nil in postgres mode.
type blobModeEnv struct {
	Manager *Manager
	DB      *sql.DB
	Logs    *bytes.Buffer
	Store   blobstore.Store
	Bucket  *testutil.S3Bucket
	Proxy   *s3Proxy
}

// blobManagerMode is one byte-store mode a test runs in: env builds a
// Manager over that mode's store (as loggedManager or s3Manager).
type blobManagerMode struct {
	name string
	env  func(t *testing.T) blobModeEnv
}

// manager builds the mode's Manager and returns it with its database.
func (mode blobManagerMode) manager(t *testing.T) (*Manager, *sql.DB) {
	t.Helper()
	env := mode.env(t)
	return env.Manager, env.DB
}

// blobManagerModes is the shared mode table for tests that run in both
// byte-store modes: postgres (bytes in the blobs row) and s3 (bytes in a
// fresh test bucket).
func blobManagerModes() []blobManagerMode {
	return []blobManagerMode{
		{name: "postgres", env: func(t *testing.T) blobModeEnv {
			manager, database, logs := loggedManager(t)
			return blobModeEnv{Manager: manager, DB: database, Logs: logs, Store: blobstore.NewPostgres(database)}
		}},
		{name: "s3", env: func(t *testing.T) blobModeEnv {
			env := s3Manager(t)
			return blobModeEnv{Manager: env.Manager, DB: env.DB, Logs: env.Logs, Store: env.Store, Bucket: env.Bucket, Proxy: env.Proxy}
		}},
	}
}

// closedLoopbackEndpoint returns an http endpoint on a loopback port that
// was listened on and then closed, so connections to it are refused: MinIO
// cut off.
func closedLoopbackEndpoint(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	return "http://" + address
}

// cutOffS3Manager builds an s3-mode Manager whose store points at a closed
// loopback port. Logs are captured as in loggedManager.
func cutOffS3Manager(t *testing.T) (*Manager, *sql.DB, *bytes.Buffer) {
	t.Helper()
	database := testutil.DB(t)
	logger, logs := capturedLogger()
	store, err := blobstore.NewS3(database, blobstore.S3Config{
		Endpoint:              closedLoopbackEndpoint(t),
		Region:                "us-east-1",
		Bucket:                "tidepool-cut-off",
		AccessKeyID:           "cut-off-access-key",
		SecretAccessKey:       "cut-off-secret-key",
		AllowPrivateAddresses: true,
		Logger:                logger,
		Transport:             testutil.LocalS3Transport(),
	})
	require.NoError(t, err)
	manager, database := managerWithLogger(t, logger, WithBlobStore(store))
	return manager, database, logs
}

// readObject returns the object's bytes and Content-Type, read out of band.
func readObject(t *testing.T, bucket *testutil.S3Bucket, key string) ([]byte, string) {
	t.Helper()
	ctx := context.Background()
	info, err := bucket.Client.StatObject(ctx, bucket.Name, key, minio.StatObjectOptions{})
	require.NoError(t, err, "stat object %s", key)
	object, err := bucket.Client.GetObject(ctx, bucket.Name, key, minio.GetObjectOptions{})
	require.NoError(t, err, "get object %s", key)
	defer func() { _ = object.Close() }()
	data, err := io.ReadAll(object)
	require.NoError(t, err, "read object %s", key)
	return data, info.ContentType
}

// putObject stores an object out of band.
func putObject(t *testing.T, bucket *testutil.S3Bucket, key, contentType string, data []byte) {
	t.Helper()
	_, err := bucket.Client.PutObject(context.Background(), bucket.Name, key,
		bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{ContentType: contentType})
	require.NoError(t, err, "put object %s", key)
}

// bucketKeys lists every object key in the bucket.
func bucketKeys(t *testing.T, bucket *testutil.S3Bucket) []string {
	t.Helper()
	var keys []string
	for object := range bucket.Client.ListObjects(context.Background(), bucket.Name, minio.ListObjectsOptions{Recursive: true}) {
		require.NoError(t, object.Err, "list objects")
		keys = append(keys, object.Key)
	}
	return keys
}

// blobRow reads the blobs row for (did, cid); found is false without one.
func blobRow(t *testing.T, database *sql.DB, did, cid string) (mimeType string, size int64, data []byte, found bool) {
	t.Helper()
	err := database.QueryRowContext(context.Background(),
		`SELECT mime_type, size, bytes FROM blobs WHERE did = $1 AND cid = $2`, did, cid).
		Scan(&mimeType, &size, &data)
	if err == sql.ErrNoRows {
		return "", 0, nil, false
	}
	require.NoError(t, err)
	return mimeType, size, data, true
}

// TestPutBlobS3Mode (B4, acceptance): in s3 mode PutBlob writes a blobs row
// with NULL bytes and the object blobs/<did>/<cid> holding the bytes, sent
// with Content-MD5, and GetBlob reads them back; a withheld put writes
// neither the row nor the object.
func TestPutBlobS3Mode(t *testing.T) {
	t.Run("stores the object and a NULL-bytes row", func(t *testing.T) {
		const (
			cidStr = "bafkreihyaummzibbppbsqyvto7hn4rj6h55kuioaziat7o5qsdfhijk47y"
			key    = "blobs/did:plc:ewvi7nxzyoun6zhxrhs64oiz/bafkreihyaummzibbppbsqyvto7hn4rj6h55kuioaziat7o5qsdfhijk47y"
			md5    = "rChFRNov93jXkPdzZhaTDQ=="
		)
		data := []byte("b4 s3 put png bytes")
		env := s3Manager(t)
		ctx := context.Background()

		ref, err := env.Manager.PutBlob(ctx, testDID, "image/png", data)
		require.NoError(t, err)
		require.NotNil(t, ref)

		mimeType, size, rowBytes, found := blobRow(t, env.DB, testDID, cidStr)
		require.True(t, found, "the blobs row exists")
		assert.Equal(t, "image/png", mimeType)
		assert.EqualValues(t, 19, size)
		assert.Nil(t, rowBytes, "the bytes live in the bucket, not Postgres")

		objectBytes, contentType := readObject(t, env.Bucket, key)
		assert.Equal(t, []byte("b4 s3 put png bytes"), objectBytes)
		assert.Equal(t, "image/png", contentType)

		puts := env.Proxy.RequestsWithMethod(http.MethodPut)
		require.Len(t, puts, 1, "one object PUT")
		assert.Equal(t, md5, puts[0].Header.Get("Content-MD5"))

		got, gotMime, err := env.Manager.GetBlob(ctx, testDID, cidStr)
		require.NoError(t, err)
		assert.Equal(t, []byte("b4 s3 put png bytes"), got)
		assert.Equal(t, "image/png", gotMime)
	})

	t.Run("withheld put stores nothing", func(t *testing.T) {
		const cidStr = "bafkreifurblyar6kw2wzyqyws4adojlzq2mhcsbu62wvg5bvbki77pfitm"
		data := []byte("b4 s3 withheld put")
		env := s3Manager(t)
		ctx := context.Background()
		_, err := env.Withhold.Withhold(ctx, blobwithhold.OperatorRequest{
			CID:        cidStr,
			ReasonCode: blobwithhold.ReasonIllegalContent,
			Actor:      "blob-s3-test",
		})
		require.NoError(t, err)
		env.Proxy.Reset()

		_, err = env.Manager.PutBlob(ctx, testDID, "image/png", data)
		require.Error(t, err)
		assert.True(t, errors.IsBlobWithheld(err), "err=%v", err)

		_, _, _, found := blobRow(t, env.DB, testDID, cidStr)
		assert.False(t, found, "no blobs row")
		assert.Empty(t, bucketKeys(t, env.Bucket), "no object")
		assert.Empty(t, env.Proxy.RequestsWithMethod(http.MethodPut), "no PUT sent")
	})
}

// TestRePutBlobS3Mode (B5): a re-put in s3 mode repairs a missing object,
// skips the upload when the object is present, refuses (as unavailable)
// when the object's presence cannot be checked, and leaves a row that holds
// Postgres bytes as it is.
func TestRePutBlobS3Mode(t *testing.T) {
	ctx := context.Background()

	t.Run("object removed out of band is written again", func(t *testing.T) {
		const (
			cidStr = "bafkreihs5rnpeinozuuqhgiq2tirqfa6irklo4i5gii23r6latpeeime7m"
			key    = "blobs/did:plc:ewvi7nxzyoun6zhxrhs64oiz/bafkreihs5rnpeinozuuqhgiq2tirqfa6irklo4i5gii23r6latpeeime7m"
		)
		data := []byte("b5 s3 reput after object removed")
		env := s3Manager(t)
		_, err := env.Manager.PutBlob(ctx, testDID, "image/png", data)
		require.NoError(t, err)
		require.NoError(t, env.Bucket.Client.RemoveObject(ctx, env.Bucket.Name, key, minio.RemoveObjectOptions{}))

		_, err = env.Manager.PutBlob(ctx, testDID, "image/png", data)
		require.NoError(t, err)

		objectBytes, _ := readObject(t, env.Bucket, key)
		assert.Equal(t, []byte("b5 s3 reput after object removed"), objectBytes)
		got, _, err := env.Manager.GetBlob(ctx, testDID, cidStr)
		require.NoError(t, err)
		assert.Equal(t, []byte("b5 s3 reput after object removed"), got)
	})

	t.Run("object present skips the upload", func(t *testing.T) {
		const (
			cidStr = "bafkreighcqltj6t7o43m3fpqsm5ew26czox7degxlswkn65ybs2xtzr5t4"
			key    = "blobs/did:plc:ewvi7nxzyoun6zhxrhs64oiz/bafkreighcqltj6t7o43m3fpqsm5ew26czox7degxlswkn65ybs2xtzr5t4"
		)
		data := []byte("b5 s3 reput object present")
		env := s3Manager(t)
		_, err := env.DB.ExecContext(ctx, `
			INSERT INTO blobs (did, cid, mime_type, size, bytes)
			VALUES ($1, $2, 'image/png', 26, NULL)`, testDID, cidStr)
		require.NoError(t, err)
		putObject(t, env.Bucket, key, "image/png", data)
		env.Proxy.Reset()

		_, err = env.Manager.PutBlob(ctx, testDID, "image/png", data)
		require.NoError(t, err)
		assert.Empty(t, env.Proxy.RequestsWithMethod(http.MethodPut), "no PUT for an object already present")
	})

	t.Run("object check fails", func(t *testing.T) {
		const (
			cidStr = "bafkreia6pryaq5mv7lpknzmkdipdpsorm7dt63pwxt5q2u3s72q3sb3efe"
			key    = "blobs/did:plc:ewvi7nxzyoun6zhxrhs64oiz/bafkreia6pryaq5mv7lpknzmkdipdpsorm7dt63pwxt5q2u3s72q3sb3efe"
		)
		data := []byte("b5 s3 stat fails")
		env := s3Manager(t)
		_, err := env.DB.ExecContext(ctx, `
			INSERT INTO blobs (did, cid, mime_type, size, bytes)
			VALUES ($1, $2, 'image/png', 16, NULL)`, testDID, cidStr)
		require.NoError(t, err)
		objectPath := "/" + env.Bucket.Name + "/" + key
		env.Proxy.SetHook(func(r *http.Request) int {
			if r.Method == http.MethodHead && r.URL.Path == objectPath {
				return http.StatusInternalServerError
			}
			return 0
		})
		env.Proxy.Reset()

		_, err = env.Manager.PutBlob(ctx, testDID, "image/png", data)
		require.Error(t, err)
		assert.True(t, errors.IsBlobStoreUnavailable(err), "err=%v", err)
		assert.Empty(t, env.Proxy.RequestsWithMethod(http.MethodPut), "no PUT after a failed check")
	})

	t.Run("row holding Postgres bytes is kept", func(t *testing.T) {
		const cidStr = "bafkreihmd3dtffhw4unw3ixefy5zorbmrsj62qgl7l5byfqopw6bllfy24"
		data := []byte("b5 s3 postgres bytes kept")
		env := s3Manager(t)
		_, err := env.DB.ExecContext(ctx, `
			INSERT INTO blobs (did, cid, mime_type, size, bytes)
			VALUES ($1, $2, 'image/png', 25, $3)`, testDID, cidStr, []byte("b5 s3 postgres bytes kept"))
		require.NoError(t, err)

		_, err = env.Manager.PutBlob(ctx, testDID, "image/png", data)
		require.NoError(t, err)

		_, _, rowBytes, found := blobRow(t, env.DB, testDID, cidStr)
		require.True(t, found)
		assert.Equal(t, []byte("b5 s3 postgres bytes kept"), rowBytes, "the row keeps its Postgres bytes")
		assert.Empty(t, bucketKeys(t, env.Bucket), "no object written")
	})
}

// TestPutBlobS3StoreCutOff (B7): in s3 mode with MinIO cut off, PutBlob of
// new bytes fails as IsBlobStoreUnavailable, logs blob_object_put_failed
// once, and stores no row.
func TestPutBlobS3StoreCutOff(t *testing.T) {
	const cidStr = "bafkreia4cuuormrvqp3gfdgsadeckrtmyg5jebgvamab5d46froxnl7h3m"
	manager, database, logs := cutOffS3Manager(t)

	_, err := manager.PutBlob(context.Background(), testDID, "image/png", []byte("b7 s3 put store cut off"))
	require.Error(t, err)
	assert.True(t, errors.IsBlobStoreUnavailable(err), "err=%v", err)
	assert.Equal(t, 1, errorRecordsWithCode(t, logs, "blob_object_put_failed"))
	_, _, _, found := blobRow(t, database, testDID, cidStr)
	assert.False(t, found, "no blobs row")
}
