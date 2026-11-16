package blobstore_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"tidepool/internal/blobstore"
)

// endpointPasswordSentinel is the userinfo password in an endpoint; no
// NewS3 error may carry it.
const endpointPasswordSentinel = "endpoint-password-sentinel"

// TestNewS3EndpointErrorsOmitCredentials: NewS3 refuses an endpoint with
// userinfo, a path, a query or a fragment, and its error never repeats the
// endpoint's credentials.
func TestNewS3EndpointErrorsOmitCredentials(t *testing.T) {
	for _, tc := range []struct{ name, endpoint string }{
		{name: "https with credentials", endpoint: "https://u:" + endpointPasswordSentinel + "@s3.example.com"},
		{name: "unparsable credentials", endpoint: "http://u:" + endpointPasswordSentinel + "%zz@localhost"},
		{name: "credentials without scheme", endpoint: "u:" + endpointPasswordSentinel + "@s3.example.com"},
		{name: "credentials and path", endpoint: "https://u:" + endpointPasswordSentinel + "@s3.example.com/bucket"},
		{name: "query", endpoint: "https://s3.example.com?token=" + endpointPasswordSentinel},
		{name: "fragment", endpoint: "https://s3.example.com#" + endpointPasswordSentinel},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := blobstore.NewS3(nil, egressConfig(tc.endpoint, false))
			require.Error(t, err)
			require.Nil(t, store)
			require.NotContains(t, err.Error(), endpointPasswordSentinel)
			require.NotContains(t, err.Error(), "u:")
		})
	}
}
