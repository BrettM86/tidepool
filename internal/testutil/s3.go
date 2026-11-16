package testutil

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/multiformats/go-multihash"
)

// s3RequestTimeout bounds each setup, cleanup and read request against the
// test MinIO, so an unreachable endpoint fails the test instead of hanging.
const s3RequestTimeout = 3 * time.Second

// S3Bucket is a fresh test bucket on the local test MinIO.
type S3Bucket struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	Region          string
	Name            string
	Client          *minio.Client
}

// NewS3Bucket creates a fresh bucket on the test MinIO named by
// TIDEPOOL_TEST_S3_ENDPOINT, _ACCESS_KEY_ID, _SECRET_ACCESS_KEY and _REGION
// (`make test` starts MinIO and sets them). It never reads the production
// BLOB_S3_* variables. An unset variable, an endpoint that is not local
// (localhost, a loopback IP, or the compose host "minio"), or a MinIO that
// does not answer fails the test; it never skips, so the s3-mode tests cannot
// pass by not running. The bucket and every object in it are removed at
// cleanup.
func NewS3Bucket(t testing.TB) *S3Bucket {
	t.Helper()

	bucket := &S3Bucket{
		Endpoint:        os.Getenv("TIDEPOOL_TEST_S3_ENDPOINT"),
		AccessKeyID:     os.Getenv("TIDEPOOL_TEST_S3_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("TIDEPOOL_TEST_S3_SECRET_ACCESS_KEY"),
		Region:          os.Getenv("TIDEPOOL_TEST_S3_REGION"),
	}
	var missing []string
	for name, value := range map[string]string{
		"TIDEPOOL_TEST_S3_ENDPOINT":          bucket.Endpoint,
		"TIDEPOOL_TEST_S3_ACCESS_KEY_ID":     bucket.AccessKeyID,
		"TIDEPOOL_TEST_S3_SECRET_ACCESS_KEY": bucket.SecretAccessKey,
		"TIDEPOOL_TEST_S3_REGION":            bucket.Region,
	} {
		if value == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("test MinIO variables unset: %s (run `make test-s3-up` and set the "+
			"TIDEPOOL_TEST_S3_* variables, or run `make test`)", strings.Join(missing, ", "))
	}

	endpoint, err := url.Parse(bucket.Endpoint)
	if err != nil || !endpoint.IsAbs() || endpoint.Host == "" {
		t.Fatalf("TIDEPOOL_TEST_S3_ENDPOINT %q must be an absolute URL such as http://127.0.0.1:9010", bucket.Endpoint)
	}
	if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
		t.Fatalf("TIDEPOOL_TEST_S3_ENDPOINT %q must use http or https", bucket.Endpoint)
	}
	if !isLocalS3Host(endpoint.Hostname()) {
		t.Fatalf("TIDEPOOL_TEST_S3_ENDPOINT host %q is not local; the test MinIO must be "+
			"localhost, a loopback IP, or minio", endpoint.Hostname())
	}

	client, err := minio.New(endpoint.Host, &minio.Options{
		Creds:        credentials.NewStaticV4(bucket.AccessKeyID, bucket.SecretAccessKey, ""),
		Secure:       endpoint.Scheme == "https",
		Region:       bucket.Region,
		BucketLookup: minio.BucketLookupPath,
		Transport:    LocalS3Transport(),
		// One attempt: the test MinIO is local, so a refused connection means
		// it is not running, and retrying only delays the failure.
		MaxRetries: 1,
	})
	if err != nil {
		t.Fatalf("build test MinIO client for %s: %v", endpoint.Host, err)
	}

	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatalf("generate test bucket name: %v", err)
	}
	name := "tidepool-test-" + hex.EncodeToString(suffix)

	ctx, cancel := context.WithTimeout(context.Background(), s3RequestTimeout)
	defer cancel()
	if err := client.MakeBucket(ctx, name, minio.MakeBucketOptions{Region: bucket.Region}); err != nil {
		t.Fatalf("create test bucket on %s (is the test MinIO running? `make test-s3-up`): %v", endpoint.Host, err)
	}
	t.Cleanup(func() { removeS3Bucket(t, client, name) })

	bucket.Name = name
	bucket.Client = client
	return bucket
}

// LocalS3Transport returns the transport for talking to the test MinIO. It
// ignores the proxy variables, so HTTP_PROXY cannot route test traffic off
// the machine, and it resolves each host itself and dials only loopback or
// private addresses, so a name such as "minio" that a DNS search domain
// resolves to a public address is refused. It cannot reuse
// ap.NewPrivateOnlyHTTPClient: internal/ap imports internal/store, whose
// tests import this package.
func LocalS3Transport() http.RoundTripper {
	dialer := &net.Dialer{Timeout: s3RequestTimeout}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		addresses, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, err
		}
		for _, candidate := range addresses {
			if !candidate.IP.IsLoopback() && !candidate.IP.IsPrivate() {
				return nil, fmt.Errorf("test MinIO host %s resolves to non-local address %s", host, candidate.IP)
			}
		}
		if len(addresses) == 0 {
			return nil, fmt.Errorf("test MinIO host %s resolves to no address", host)
		}
		return dialer.DialContext(ctx, network, net.JoinHostPort(addresses[0].IP.String(), port))
	}
	return transport
}

// isLocalS3Host reports whether host may serve the test MinIO.
func isLocalS3Host(host string) bool {
	if host == "localhost" || host == "minio" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// removeS3Bucket deletes every object in the bucket, then the bucket.
func removeS3Bucket(t testing.TB, client *minio.Client, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*s3RequestTimeout)
	defer cancel()
	for object := range client.ListObjects(ctx, name, minio.ListObjectsOptions{Recursive: true}) {
		if object.Err != nil {
			t.Errorf("list test bucket %s for cleanup: %v", name, object.Err)
			return
		}
		if err := client.RemoveObject(ctx, name, object.Key, minio.RemoveObjectOptions{}); err != nil {
			t.Errorf("remove object %s from test bucket %s: %v", object.Key, name, err)
			return
		}
	}
	if err := client.RemoveBucket(ctx, name); err != nil {
		t.Errorf("remove test bucket %s: %v", name, err)
	}
}

// StoredBlob names one blobs row.
type StoredBlob struct {
	DID string
	CID string
}

// BlobsHoldingBytes returns every blobs row whose stored bytes equal data,
// ordered by did, cid. A row's stored bytes are its Postgres bytes column
// when that is non-NULL, else the object blobs/<did>/<cid> in bucket (a
// missing object holds nothing). A nil bucket is postgres mode: rows with
// NULL bytes never match.
func BlobsHoldingBytes(t testing.TB, db *sql.DB, bucket *S3Bucket, data []byte) []StoredBlob {
	t.Helper()

	rows, err := db.Query(`
		SELECT did, cid, bytes IS NULL
		FROM blobs
		WHERE bytes = $1 OR bytes IS NULL
		ORDER BY did, cid`, data)
	if err != nil {
		t.Fatalf("query blobs holding bytes: %v", err)
	}
	type candidate struct {
		blob      StoredBlob
		inObjects bool
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.blob.DID, &c.blob.CID, &c.inObjects); err != nil {
			_ = rows.Close()
			t.Fatalf("scan blobs row: %v", err)
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate blobs rows: %v", err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close blobs rows: %v", err)
	}

	var holding []StoredBlob
	for _, c := range candidates {
		if !c.inObjects {
			holding = append(holding, c.blob)
			continue
		}
		if bucket == nil {
			continue
		}
		object, found := ReadS3Object(t, bucket, "blobs/"+c.blob.DID+"/"+c.blob.CID)
		if found && bytes.Equal(object, data) {
			holding = append(holding, c.blob)
		}
	}
	return holding
}

// RawBlobCID is the atproto blob CID (CIDv1, raw, sha-256) of data.
func RawBlobCID(t testing.TB, data []byte) string {
	t.Helper()
	sum, err := cid.Prefix{Version: 1, Codec: cid.Raw, MhType: multihash.SHA2_256, MhLength: -1}.Sum(data)
	if err != nil {
		t.Fatalf("compute blob cid: %v", err)
	}
	return sum.String()
}

// ReadS3Object returns the object's bytes, or found=false when it does not
// exist. Any other failure fails the test.
func ReadS3Object(t testing.TB, bucket *S3Bucket, key string) (data []byte, found bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), s3RequestTimeout)
	defer cancel()
	object, err := bucket.Client.GetObject(ctx, bucket.Name, key, minio.GetObjectOptions{})
	if err == nil {
		data, err = io.ReadAll(object)
		_ = object.Close()
	}
	if err != nil {
		if minio.ToErrorResponse(err).Code == minio.NoSuchKey {
			return nil, false
		}
		t.Fatalf("read object %s from test bucket %s: %v", key, bucket.Name, err)
	}
	return data, true
}
