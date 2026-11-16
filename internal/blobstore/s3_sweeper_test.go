package blobstore

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"

	"tidepool/internal/testutil"
)

// TestS3RunSweepsTheObjectDeleteQueue: Run, left to its own ticker, deletes
// a queued object whose blobs row is gone and drops the queue row, and it
// returns once its context is cancelled. Not parallel: it shortens the
// package-level sweep interval.
func TestS3RunSweepsTheObjectDeleteQueue(t *testing.T) {
	const (
		did = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
		cid = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"
		key = "blobs/" + did + "/" + cid
	)
	interval := objectDeleteSweepInterval
	objectDeleteSweepInterval = time.Millisecond
	t.Cleanup(func() { objectDeleteSweepInterval = interval })

	database := testutil.DB(t)
	testutil.Truncate(t, database, "blobs")
	testutil.Truncate(t, database, "blob_object_deletes")
	t.Cleanup(func() {
		testutil.Truncate(t, database, "blobs")
		testutil.Truncate(t, database, "blob_object_deletes")
	})

	bucket := testutil.NewS3Bucket(t)
	store, err := NewS3(database, S3Config{
		Endpoint:              bucket.Endpoint,
		Region:                bucket.Region,
		Bucket:                bucket.Name,
		AccessKeyID:           bucket.AccessKeyID,
		SecretAccessKey:       bucket.SecretAccessKey,
		AllowPrivateAddresses: true,
		Transport:             testutil.LocalS3Transport(),
	})
	require.NoError(t, err)

	data := []byte("sweeper run")
	putCtx, putCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer putCancel()
	_, err = bucket.Client.PutObject(putCtx, bucket.Name, key, bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: "image/png"})
	require.NoError(t, err)
	_, found := testutil.ReadS3Object(t, bucket, key)
	require.True(t, found, "the object exists before Run starts")

	_, err = database.ExecContext(context.Background(), `
		INSERT INTO blob_object_deletes (did, cid, queued_at, attempts)
		VALUES ($1, $2, now(), 0)`, did, cid)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		store.Run(ctx)
	}()

	queued := func() int {
		var n int
		require.NoError(t, database.QueryRow(
			`SELECT count(*) FROM blob_object_deletes WHERE did = $1 AND cid = $2`, did, cid).Scan(&n))
		return n
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, found = testutil.ReadS3Object(t, bucket, key)
		if queued() == 0 && !found {
			break
		}
		select {
		case <-done:
			t.Fatal("Run returned before its context was cancelled")
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("Run did not sweep within 10s: queue rows %d, object present %v", queued(), found)
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return within 5s of its context being cancelled")
	}
}
