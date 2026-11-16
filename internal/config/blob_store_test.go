package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestLoad_BlobStore: BLOB_STORE unset or "postgres" selects postgres; any
// value other than postgres or s3 fails naming BLOB_STORE and the value.
func TestLoad_BlobStore(t *testing.T) {
	for _, tc := range []struct {
		name      string
		value     string
		unset     bool
		want      string
		wantError string
	}{
		{name: "unset selects postgres", unset: true, want: "postgres"},
		{name: "postgres", value: "postgres", want: "postgres"},
		{name: "unknown value", value: "bogus", wantError: "bogus"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearConfigEnv(t)
			t.Setenv("BLOB_STORE", tc.value)
			if tc.unset {
				require.NoError(t, os.Unsetenv("BLOB_STORE"))
			}
			cfg, err := Load(discardLogger())
			if tc.wantError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), "BLOB_STORE")
				require.Contains(t, err.Error(), tc.wantError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.BlobStore)
		})
	}
}

// secretSentinel is the BLOB_S3_SECRET_ACCESS_KEY value in rows that set it;
// no error may echo it.
const secretSentinel = "secret-sentinel-5f3a"

// endpointPasswordSentinel is the userinfo password (or query or fragment
// value) placed in a BLOB_S3_ENDPOINT row; no error may carry it.
const endpointPasswordSentinel = "endpoint-password-sentinel"

// setS3Env sets BLOB_STORE=s3 and all five BLOB_S3_* variables to literal
// values with a local endpoint.
func setS3Env(t *testing.T) {
	t.Helper()
	t.Setenv("BLOB_STORE", "s3")
	t.Setenv("BLOB_S3_ENDPOINT", "http://localhost:9000")
	t.Setenv("BLOB_S3_REGION", "gra")
	t.Setenv("BLOB_S3_BUCKET", "tidepool-blobs")
	t.Setenv("BLOB_S3_ACCESS_KEY_ID", "access-key-7c1e")
	t.Setenv("BLOB_S3_SECRET_ACCESS_KEY", secretSentinel)
}

// TestLoad_BlobStoreS3MissingVariable (B1): BLOB_STORE=s3 with exactly one
// BLOB_S3_* variable unset fails, naming that variable and never the secret.
func TestLoad_BlobStoreS3MissingVariable(t *testing.T) {
	for _, missing := range []string{
		"BLOB_S3_ENDPOINT",
		"BLOB_S3_REGION",
		"BLOB_S3_BUCKET",
		"BLOB_S3_ACCESS_KEY_ID",
		"BLOB_S3_SECRET_ACCESS_KEY",
	} {
		t.Run(missing, func(t *testing.T) {
			clearConfigEnv(t)
			setS3Env(t)
			require.NoError(t, os.Unsetenv(missing))

			_, err := Load(discardLogger())
			require.Error(t, err)
			require.Contains(t, err.Error(), missing)
			require.NotContains(t, err.Error(), secretSentinel)
		})
	}
}

// TestLoad_BlobStoreS3Configured (B1): BLOB_STORE=s3 with all five BLOB_S3_*
// set loads s3 mode with exactly those settings.
func TestLoad_BlobStoreS3Configured(t *testing.T) {
	for _, tc := range []struct {
		name       string
		production bool
		endpoint   string
	}{
		{name: "development with a local endpoint", endpoint: "http://localhost:9000"},
		{name: "production with an https endpoint", production: true, endpoint: "https://s3.gra.io.cloud.ovh.net"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.production {
				setProductionEnv(t)
			} else {
				clearConfigEnv(t)
			}
			setS3Env(t)
			t.Setenv("BLOB_S3_ENDPOINT", tc.endpoint)

			cfg, err := Load(discardLogger())
			require.NoError(t, err)
			require.Equal(t, "s3", cfg.BlobStore)
			require.Equal(t, S3Settings{
				Endpoint:        tc.endpoint,
				Region:          "gra",
				Bucket:          "tidepool-blobs",
				AccessKeyID:     "access-key-7c1e",
				SecretAccessKey: secretSentinel,
			}, cfg.BlobS3)
		})
	}
}

// TestLoad_BlobStorePostgresIgnoresS3Variables (B1): in postgres mode partial
// or malformed BLOB_S3_* values are ignored and BlobS3 stays zero.
func TestLoad_BlobStorePostgresIgnoresS3Variables(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("BLOB_STORE", "postgres")
	t.Setenv("BLOB_S3_ENDPOINT", "not a url ::")
	t.Setenv("BLOB_S3_SECRET_ACCESS_KEY", secretSentinel)

	cfg, err := Load(discardLogger())
	require.NoError(t, err)
	require.Equal(t, "postgres", cfg.BlobStore)
	require.Equal(t, S3Settings{}, cfg.BlobS3)
}

// TestLoadBlobStore_Endpoint (B2, B3): the endpoint must be an absolute URL;
// development (ENVIRONMENT=development or unset) accepts only local endpoints,
// production accepts http and https. Endpoints are parsed, never dialled.
func TestLoadBlobStore_Endpoint(t *testing.T) {
	const unset = "<unset>"
	for _, tc := range []struct {
		name        string
		environment string
		endpoint    string
		wantError   bool
	}{
		{name: "no scheme", environment: EnvironmentProduction, endpoint: "s3.example.com", wantError: true},
		{name: "development public https", environment: EnvironmentDevelopment, endpoint: "https://s3.example.com", wantError: true},
		{name: "unset environment public https", environment: unset, endpoint: "https://s3.example.com", wantError: true},
		{name: "development localhost", environment: EnvironmentDevelopment, endpoint: "http://localhost:9000"},
		{name: "development IPv4 loopback", environment: EnvironmentDevelopment, endpoint: "http://127.0.0.1:9000"},
		{name: "development IPv6 loopback", environment: EnvironmentDevelopment, endpoint: "http://[::1]:9000"},
		{name: "development compose minio", environment: EnvironmentDevelopment, endpoint: "http://minio:9000"},
		{name: "production https", environment: EnvironmentProduction, endpoint: "https://s3.example.com"},
		{name: "production http", environment: EnvironmentProduction, endpoint: "http://s3.example.com"},
		{name: "production https with credentials", environment: EnvironmentProduction, endpoint: "https://u:" + endpointPasswordSentinel + "@s3.example.com", wantError: true},
		{name: "production credentials without scheme", environment: EnvironmentProduction, endpoint: "u:" + endpointPasswordSentinel + "@s3.example.com", wantError: true},
		{name: "development unparsable credentials", environment: EnvironmentDevelopment, endpoint: "http://u:" + endpointPasswordSentinel + "%zz@localhost", wantError: true},
		{name: "production path-style bucket", environment: EnvironmentProduction, endpoint: "https://s3.example.com/bucket", wantError: true},
		{name: "production query", environment: EnvironmentProduction, endpoint: "https://s3.example.com?token=" + endpointPasswordSentinel, wantError: true},
		{name: "production fragment", environment: EnvironmentProduction, endpoint: "https://s3.example.com#" + endpointPasswordSentinel, wantError: true},
		{name: "production empty query", environment: EnvironmentProduction, endpoint: "https://s3.example.com?", wantError: true},
		{name: "production trailing slash", environment: EnvironmentProduction, endpoint: "https://s3.example.com/"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearConfigEnv(t)
			setS3Env(t)
			t.Setenv("BLOB_S3_ENDPOINT", tc.endpoint)
			t.Setenv("ENVIRONMENT", tc.environment)
			if tc.environment == unset {
				require.NoError(t, os.Unsetenv("ENVIRONMENT"))
			}

			settings, err := LoadBlobStore()
			if tc.wantError {
				require.Error(t, err)
				require.Contains(t, err.Error(), "BLOB_S3_ENDPOINT")
				require.NotContains(t, err.Error(), secretSentinel)
				require.NotContains(t, err.Error(), endpointPasswordSentinel)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "s3", settings.Mode)
			require.Equal(t, tc.endpoint, settings.S3.Endpoint)
		})
	}
}
