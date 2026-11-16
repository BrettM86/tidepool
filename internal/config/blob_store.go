package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
)

// Blob store modes (BLOB_STORE).
const (
	BlobStorePostgres = "postgres"
	BlobStoreS3       = "s3"
)

// S3Settings is the BLOB_S3_* object-store configuration. SecretAccessKey
// must never be logged or put in an error.
type S3Settings struct {
	Endpoint, Region, Bucket, AccessKeyID, SecretAccessKey string
}

// BlobStoreSettings is the parsed BLOB_STORE mode and, in s3 mode, its S3
// settings.
type BlobStoreSettings struct {
	// Mode is "postgres" or "s3".
	Mode string
	// S3 is the zero value unless Mode is "s3".
	S3 S3Settings
}

// LoadBlobStore reads BLOB_STORE, the BLOB_S3_* variables and ENVIRONMENT.
// BLOB_STORE unset or "postgres" selects postgres and ignores BLOB_S3_*.
// "s3" requires all five BLOB_S3_* variables; a partial set fails naming
// every missing one. BLOB_S3_ENDPOINT must be an absolute http or https URL
// of scheme and host only: no credentials, path, query or fragment. ENVIRONMENT defaults to development as in Load; in
// development the endpoint host must be localhost, a loopback IP or minio.
// It makes no network request, and no error carries the secret or the raw
// endpoint.
func LoadBlobStore() (BlobStoreSettings, error) {
	switch mode := os.Getenv("BLOB_STORE"); mode {
	case "", BlobStorePostgres:
		return BlobStoreSettings{Mode: BlobStorePostgres}, nil
	case BlobStoreS3:
	default:
		return BlobStoreSettings{}, fmt.Errorf("config: BLOB_STORE must be %s or %s, got %q",
			BlobStorePostgres, BlobStoreS3, mode)
	}

	environment := os.Getenv("ENVIRONMENT")
	if environment == "" {
		environment = EnvironmentDevelopment
	}
	if environment != EnvironmentDevelopment && environment != EnvironmentProduction {
		return BlobStoreSettings{}, fmt.Errorf("config: ENVIRONMENT must be %q or %q, got %q",
			EnvironmentDevelopment, EnvironmentProduction, environment)
	}

	settings := S3Settings{
		Endpoint:        os.Getenv("BLOB_S3_ENDPOINT"),
		Region:          os.Getenv("BLOB_S3_REGION"),
		Bucket:          os.Getenv("BLOB_S3_BUCKET"),
		AccessKeyID:     os.Getenv("BLOB_S3_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("BLOB_S3_SECRET_ACCESS_KEY"),
	}
	var missing []string
	for _, variable := range []struct{ name, value string }{
		{"BLOB_S3_ENDPOINT", settings.Endpoint},
		{"BLOB_S3_REGION", settings.Region},
		{"BLOB_S3_BUCKET", settings.Bucket},
		{"BLOB_S3_ACCESS_KEY_ID", settings.AccessKeyID},
		{"BLOB_S3_SECRET_ACCESS_KEY", settings.SecretAccessKey},
	} {
		if variable.value == "" {
			missing = append(missing, variable.name)
		}
	}
	if len(missing) > 0 {
		return BlobStoreSettings{}, fmt.Errorf("config: BLOB_STORE=s3 requires %s", strings.Join(missing, ", "))
	}

	if err := validateS3Endpoint(settings.Endpoint, environment); err != nil {
		return BlobStoreSettings{}, err
	}
	return BlobStoreSettings{Mode: BlobStoreS3, S3: settings}, nil
}

// validateS3Endpoint checks BLOB_S3_ENDPOINT is an absolute http or https
// URL of scheme and host (with optional port) only, the rule NewS3 applies,
// and, in development, that the host is local. No error repeats the raw
// value, which may hold credentials; only a host from a URL without userinfo
// is named.
func validateS3Endpoint(endpoint, environment string) error {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("config: BLOB_S3_ENDPOINT is not a URL")
	}
	if parsed.User != nil {
		return fmt.Errorf("config: BLOB_S3_ENDPOINT must not contain credentials")
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return fmt.Errorf("config: BLOB_S3_ENDPOINT must be an absolute http or https URL with a host")
	}
	if (parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return fmt.Errorf("config: BLOB_S3_ENDPOINT for host %q must be scheme and host only, with no path, query or fragment",
			parsed.Host)
	}
	if environment == EnvironmentDevelopment && !isLocalS3Host(parsed.Hostname()) {
		return fmt.Errorf("config: BLOB_S3_ENDPOINT host %q is not allowed in development; use localhost, a loopback IP or minio",
			parsed.Hostname())
	}
	return nil
}

// isLocalS3Host reports whether host is localhost, a loopback IP or the
// compose service name minio.
func isLocalS3Host(host string) bool {
	if host == "localhost" || host == "minio" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
