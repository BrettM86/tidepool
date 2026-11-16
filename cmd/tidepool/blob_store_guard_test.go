package main

import (
	"context"
	stderrors "errors"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/blobstore"
	"tidepool/internal/testutil"
)

// TestRunRefusesPostgresModeWithObjectStoreBytes (B7): with a blobs row whose
// bytes are NULL (held in the object store), a postgres-mode run stops boot
// with a *blobstore.ModeError naming blob_bytes_in_object_store and the
// count, logged at Error with that code. The HTTP port is held occupied, so a
// run that carried on past the guard returns a bind error instead.
func TestRunRefusesPostgresModeWithObjectStoreBytes(t *testing.T) {
	const cidStr = "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"
	database := testutil.DB(t)
	testutil.Truncate(t, database, "service_keys", "bridged_actors", "ap_actors", "inbox_events", "communities", "blobs")
	testutil.TruncateIfExists(t, database, "blob_object_deletes")
	t.Cleanup(func() { testutil.Truncate(t, database, "blobs") })
	_, err := database.ExecContext(context.Background(), `
		INSERT INTO blobs (did, cid, mime_type, size, bytes)
		VALUES ('did:plc:ewvi7nxzyoun6zhxrhs64oiz', $1, 'image/png', 11, NULL)`, cidStr)
	require.NoError(t, err, "a blobs row with NULL bytes")

	httpListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = httpListener.Close() }()
	httpAddress := httpListener.Addr().String()

	t.Setenv("ENVIRONMENT", "development")
	t.Setenv("DATABASE_URL", os.Getenv("TIDEPOOL_TEST_DATABASE_URL"))
	t.Setenv("LISTEN_ADDR", httpAddress)
	t.Setenv("DNS_LISTEN", "")
	t.Setenv("DNS_PUBLIC_IPV4", "127.0.0.1")
	t.Setenv("DNS_PUBLIC_IPV6", "")
	t.Setenv("DNS_NAMESERVERS", "")
	t.Setenv("CLOUDFLARE_API_TOKEN", "")
	t.Setenv("CLOUDFLARE_ZONE_ID", "")
	t.Setenv("BRIDGE_HOSTNAME", "tdpl.example")
	t.Setenv("BRIDGE_KEK", rotateTestKEK)
	t.Setenv("BRIDGE_KEK_PREVIOUS", "")
	t.Setenv("RELAY_HOSTS", "")
	t.Setenv("CONSUMER_ENABLED", "")
	t.Setenv("FOLLOW_LIST_PATH", "")
	t.Setenv("AP_USER_ORIGIN", "http://127.0.0.1:8091")
	t.Setenv("BLOB_STORE", "postgres")

	logger, records := capturingLogger()
	result := make(chan error, 1)
	go func() { result <- run(logger) }()
	select {
	case err := <-result:
		require.Error(t, err)
		require.NotContains(t, err.Error(), httpAddress, "boot must stop before HTTP binds")
		var modeErr *blobstore.ModeError
		require.True(t, stderrors.As(err, &modeErr), "want a *blobstore.ModeError; err=%v", err)
		assert.Equal(t, blobstore.ModeError{Code: "blob_bytes_in_object_store", Count: 1}, *modeErr)
		logged := 0
		for _, record := range *records {
			if code, ok := attr(record, "code"); ok && record.Level == slog.LevelError && code.String() == "blob_bytes_in_object_store" {
				logged++
				count, ok := attr(record, "count")
				require.True(t, ok, "the refusal log carries the count")
				assert.Equal(t, int64(1), count.Int64(), "the refusal log names the count")
			}
		}
		assert.Equal(t, 1, logged, "the refusal is logged at Error with its code and count")
	case <-time.After(60 * time.Second):
		t.Fatal("run did not return within 60 seconds")
	}
}
