package blobstore

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"tidepool/internal/testutil"
)

// TestS3CallsEndAtTheirDeadline (B12): with the caller's context live, a
// bucket HEAD, an object PUT, an object STAT (a Put over a row with NULL
// bytes) and an object GET against a bucket that stops
// answering each fail at the store's own per-call deadline instead of
// holding the caller (and its database connection and advisory locks)
// until the bucket recovers.
func TestS3CallsEndAtTheirDeadline(t *testing.T) {
	const (
		did    = "did:plc:ewvi7nxzyoun6zhxrhs64oiz"
		putCID = "bafkreih7e3ac4bza5bpi5wq5qx2ejpheku5l5ilz4kytb4ktnt64u5zfya"
		getCID = "bafkreib6eswcw2oh4b3v64okfaf2mjtn7u44lictfnqtsgycdatjqwiwoq"
		// statCID is the CID of "b12 deadline stat".
		statCID = "bafkreia5j4pexsdifjkzusawaqnhocp2xkzentou33sv4de7gfleryynma"
	)
	transfer, request := objectTransferTimeout, objectRequestTimeout
	objectTransferTimeout, objectRequestTimeout = 200*time.Millisecond, 200*time.Millisecond
	t.Cleanup(func() { objectTransferTimeout, objectRequestTimeout = transfer, request })

	// The stalled bucket: HEAD and PUT never answer; GET sends its headers
	// and part of the 22-byte body, then nothing more. Every handler is
	// released at cleanup, before the server closes.
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "image/png")
			w.Header().Set("Content-Length", "22")
			w.Header().Set("ETag", `"0123456789abcdef0123456789abcdef"`)
			w.Header().Set("Last-Modified", time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC).Format(http.TimeFormat))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("b12 dead"))
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })

	database := testutil.DB(t)
	testutil.Truncate(t, database, "blobs")
	t.Cleanup(func() { testutil.Truncate(t, database, "blobs") })
	_, err := database.ExecContext(context.Background(), `
		INSERT INTO blobs (did, cid, mime_type, size, bytes)
		VALUES ($1, $2, 'image/png', 22, NULL), ($1, $3, 'image/png', 17, NULL)`, did, getCID, statCID)
	require.NoError(t, err)

	store, err := NewS3(database, S3Config{
		Endpoint:              server.URL,
		Region:                "us-east-1",
		Bucket:                "tidepool-stalled",
		AccessKeyID:           "stalled-access-key",
		SecretAccessKey:       "stalled-secret-key",
		AllowPrivateAddresses: true,
	})
	require.NoError(t, err)

	for _, tc := range []struct {
		name string
		call func(ctx context.Context) error
	}{
		{name: "bucket HEAD", call: store.HeadBucket},
		{name: "object PUT", call: func(ctx context.Context) error {
			tx, beginErr := database.BeginTx(ctx, nil)
			if beginErr != nil {
				return beginErr
			}
			defer func() { _ = tx.Rollback() }()
			return store.Put(ctx, tx, did, putCID, "image/png", []byte("b12 deadline put"))
		}},
		{name: "object STAT", call: func(ctx context.Context) error {
			tx, beginErr := database.BeginTx(ctx, nil)
			if beginErr != nil {
				return beginErr
			}
			defer func() { _ = tx.Rollback() }()
			return store.Put(ctx, tx, did, statCID, "image/png", []byte("b12 deadline stat"))
		}},
		{name: "object GET", call: func(ctx context.Context) error {
			_, _, getErr := store.Get(ctx, did, getCID)
			return getErr
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			done := make(chan error, 1)
			go func() { done <- tc.call(context.Background()) }()
			select {
			case callErr := <-done:
				require.Error(t, callErr, "a stalled bucket fails the call")
			case <-time.After(5 * time.Second):
				t.Fatal("the call did not return within 5s with the caller's context live")
			}
		})
	}
}
