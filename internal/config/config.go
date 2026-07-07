// Package config loads Tidepool configuration from environment variables.
// In development, missing values fall back to logged dev defaults (Coves
// style, no config library). In production, required values must be set.
package config

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"
)

const (
	EnvironmentDevelopment = "development"
	EnvironmentProduction  = "production"
)

// Config holds all runtime configuration for the bridge.
type Config struct {
	// Environment is "development" or "production". Development enables
	// migrations-on-start and dev defaults for the other values.
	Environment string
	// DatabaseURL is the postgres connection string for bridge state.
	DatabaseURL string
	// ListenAddr is the address the HTTP server binds, e.g. ":8091".
	ListenAddr string
	// BridgeHostname is the public domain the bridge is served from,
	// e.g. "tidepool.example". Used for WebFinger, actor IDs, and handles.
	BridgeHostname string
	// PLCDirectoryURL is the did:plc directory used to mint and resolve DIDs.
	// The dev default is a LOCAL directory (docker compose `plc` profile);
	// production points at https://plc.directory. Nothing ever falls back to
	// the live directory implicitly.
	PLCDirectoryURL string
	// BridgeKEK is the 32-byte key-encryption key that seals per-actor
	// signing keys (and the escrow rotation key) at rest, AES-256-GCM.
	// Set BRIDGE_KEK to 64 hex chars or standard base64 of 32 bytes. The
	// development default is a fixed, publicly known key — never usable in
	// production, where BRIDGE_KEK is required.
	BridgeKEK []byte
	// BridgeServiceDID optionally pins a pre-provisioned service DID for the
	// bridge's own actor. Service-DID bootstrap is deferred: task 06 wires
	// the service actor; until then an empty value is handled gracefully
	// (the bridge hostname simply does not resolve to a DID).
	BridgeServiceDID string
	// UserAgent is sent on all outbound HTTP requests (signed fetches etc.).
	UserAgent string
	// FirehoseRetention is how long firehose events are kept for
	// subscribeRepos cursor replay (FIREHOSE_RETENTION, a Go duration,
	// default 72h). Consumers reconnecting with a cursor older than the
	// window get an OutdatedCursor #info frame and resume from the oldest
	// retained event.
	FirehoseRetention time.Duration
	// RelayHosts lists relays to send com.atproto.sync.requestCrawl to on
	// startup (RELAY_HOSTS, comma-separated, optional). In development the
	// request is logged instead of sent — dev hosts are not publicly
	// reachable and must never poke real relays.
	RelayHosts []string
	// AllowPrivateAddresses disables the SSRF egress guard, letting the AP
	// client fetch loopback/private/link-local/metadata addresses. It defaults
	// to false (guard on) and must only be enabled for local development or
	// tests that hit httptest servers on 127.0.0.1. Set ALLOW_PRIVATE_FETCH=1
	// to enable.
	AllowPrivateAddresses bool
}

// Load reads configuration from the environment. logger must not be nil;
// every dev default that gets applied is logged so local runs are explicit
// about what they picked.
func Load(logger *slog.Logger) (*Config, error) {
	environment := os.Getenv("ENVIRONMENT")
	if environment == "" {
		environment = EnvironmentDevelopment
		logger.Info("ENVIRONMENT not set, defaulting to development")
	}
	if environment != EnvironmentDevelopment && environment != EnvironmentProduction {
		return nil, fmt.Errorf("config: ENVIRONMENT must be %q or %q, got %q",
			EnvironmentDevelopment, EnvironmentProduction, environment)
	}
	isDevelopment := environment == EnvironmentDevelopment

	cfg := &Config{Environment: environment}

	var err error
	cfg.DatabaseURL, err = stringVar(logger, isDevelopment, "DATABASE_URL",
		"postgres://tidepool:tidepool@localhost:5442/tidepool_dev?sslmode=disable")
	if err != nil {
		return nil, err
	}
	cfg.ListenAddr, err = stringVar(logger, isDevelopment, "LISTEN_ADDR", ":8091")
	if err != nil {
		return nil, err
	}
	cfg.BridgeHostname, err = stringVar(logger, isDevelopment, "BRIDGE_HOSTNAME", "localhost")
	if err != nil {
		return nil, err
	}
	cfg.PLCDirectoryURL, err = stringVar(logger, isDevelopment, "PLC_DIRECTORY_URL", "http://localhost:3002")
	if err != nil {
		return nil, err
	}

	// The dev-default KEK is fixed and public (sha256 of a known string):
	// fine for local development, catastrophic in production, hence the
	// required-in-production rule shared with every other stringVar.
	kekEncoded, err := stringVar(logger, isDevelopment, "BRIDGE_KEK",
		"9a80812a2a5e298fe6b36ba6ba99f33ca42a7a5b1cae7ff43a4b338bbbdd6a34")
	if err != nil {
		return nil, err
	}
	cfg.BridgeKEK, err = decodeKEK(kekEncoded)
	if err != nil {
		return nil, err
	}

	// Optional in every environment: an operator may pre-provision the
	// bridge's service DID, otherwise identity bootstrap mints one.
	cfg.BridgeServiceDID = os.Getenv("BRIDGE_SERVICE_DID")

	// Firehose retention has a real default in every environment — it is a
	// tuning knob, not a deployment-specific value like hostnames or keys.
	retentionRaw := os.Getenv("FIREHOSE_RETENTION")
	if retentionRaw == "" {
		retentionRaw = "72h"
		logger.Info("FIREHOSE_RETENTION not set, using default", "value", retentionRaw)
	}
	cfg.FirehoseRetention, err = time.ParseDuration(retentionRaw)
	if err != nil {
		return nil, fmt.Errorf("config: FIREHOSE_RETENTION must be a Go duration (e.g. 72h): %w", err)
	}
	if cfg.FirehoseRetention <= 0 {
		return nil, fmt.Errorf("config: FIREHOSE_RETENTION must be positive, got %s", cfg.FirehoseRetention)
	}

	// Optional relay list for requestCrawl on startup.
	if raw := os.Getenv("RELAY_HOSTS"); raw != "" {
		for _, host := range strings.Split(raw, ",") {
			if host = strings.TrimSpace(host); host != "" {
				cfg.RelayHosts = append(cfg.RelayHosts, host)
			}
		}
	}

	// SSRF egress guard is on by default; only local dev/tests should relax
	// it. Accept it in development but refuse to let production disable the
	// guard silently.
	cfg.AllowPrivateAddresses = boolVar("ALLOW_PRIVATE_FETCH")
	if cfg.AllowPrivateAddresses && !isDevelopment {
		return nil, fmt.Errorf("config: ALLOW_PRIVATE_FETCH must not be set in production")
	}

	defaultUserAgent := fmt.Sprintf("tidepool/0.1 (+https://%s)", cfg.BridgeHostname)
	cfg.UserAgent = os.Getenv("USER_AGENT")
	if cfg.UserAgent == "" {
		cfg.UserAgent = defaultUserAgent
		logger.Info("USER_AGENT not set, using default", "value", defaultUserAgent)
	}

	return cfg, nil
}

// IsDevelopment reports whether the bridge runs with dev conveniences
// (migrations-on-start, defaulted config).
func (c *Config) IsDevelopment() bool {
	return c.Environment == EnvironmentDevelopment
}

// decodeKEK parses the BRIDGE_KEK value: 64 hex chars or standard base64,
// either way decoding to exactly 32 bytes.
func decodeKEK(encoded string) ([]byte, error) {
	encoded = strings.TrimSpace(encoded)
	if raw, err := hex.DecodeString(encoded); err == nil {
		if len(raw) != 32 {
			return nil, fmt.Errorf("config: BRIDGE_KEK must decode to 32 bytes, got %d", len(raw))
		}
		return raw, nil
	}
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("config: BRIDGE_KEK must be 64 hex chars or base64 of 32 bytes: %w", err)
	}
	if len(raw) != 32 {
		return nil, fmt.Errorf("config: BRIDGE_KEK must decode to 32 bytes, got %d", len(raw))
	}
	return raw, nil
}

// boolVar reports whether an environment variable is set to a truthy value
// ("1", "true", "yes", case-insensitive).
func boolVar(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// stringVar returns the value of an environment variable. When unset it
// falls back to the logged dev default in development and errors in
// production.
func stringVar(logger *slog.Logger, isDevelopment bool, name, devDefault string) (string, error) {
	if value := os.Getenv(name); value != "" {
		return value, nil
	}
	if !isDevelopment {
		return "", fmt.Errorf("config: %s is required in production", name)
	}
	logger.Info("environment variable not set, using dev default", "name", name, "value", devDefault)
	return devDefault, nil
}
