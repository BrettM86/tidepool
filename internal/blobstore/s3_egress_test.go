package blobstore_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"tidepool/internal/blobstore"
)

// clearProxyEnv removes the proxy variables so an S3 request can only reach
// its endpoint directly.
func clearProxyEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy"} {
		t.Setenv(name, "")
	}
}

// countingServer is a loopback S3 stand-in that answers 200 to everything
// and counts the requests it received.
func countingServer(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	return server, &requests
}

func egressConfig(endpoint string, allowPrivate bool) blobstore.S3Config {
	return blobstore.S3Config{
		Endpoint:              endpoint,
		Region:                "us-east-1",
		Bucket:                "egress-test",
		AccessKeyID:           "egress-access-key",
		SecretAccessKey:       "egress-secret-key",
		AllowPrivateAddresses: allowPrivate,
	}
}

func headBucket(t *testing.T, store *blobstore.S3) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return store.HeadBucket(ctx)
}

// TestS3EgressRefusesPrivateAddresses (B2): with AllowPrivateAddresses false
// the S3 client never connects to a loopback or link-local endpoint; with it
// true the loopback endpoint is reached.
func TestS3EgressRefusesPrivateAddresses(t *testing.T) {
	t.Run("loopback refused", func(t *testing.T) {
		clearProxyEnv(t)
		server, requests := countingServer(t)
		store, err := blobstore.NewS3(nil, egressConfig(server.URL, false))
		require.NoError(t, err)

		assert.Error(t, headBucket(t, store))
		assert.Zero(t, requests.Load(), "the loopback server must receive no request")
	})
	t.Run("link-local metadata address refused", func(t *testing.T) {
		clearProxyEnv(t)
		store, err := blobstore.NewS3(nil, egressConfig("http://169.254.169.254", false))
		require.NoError(t, err)

		assert.Error(t, headBucket(t, store))
	})
	t.Run("loopback allowed", func(t *testing.T) {
		clearProxyEnv(t)
		server, requests := countingServer(t)
		store, err := blobstore.NewS3(nil, egressConfig(server.URL, true))
		require.NoError(t, err)

		assert.NoError(t, headBucket(t, store))
		assert.Positive(t, requests.Load(), "the loopback server must be reached")
	})
}
