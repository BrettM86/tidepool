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
	"strconv"
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
	// MaxBlobBytes caps how many bytes of remote media (avatars, banners,
	// post images) the materializer will download and store per blob
	// (MAX_BLOB_BYTES, default 5 MiB). Individual lexicon slots impose
	// tighter caps (e.g. avatars max 1 MB); this is the outer transport
	// budget. Fails closed: oversized media is dropped, never truncated.
	MaxBlobBytes int64
	// ProfileRefreshTTL is how stale a bridged actor's materialized profile
	// may get before the materializer re-fetches and re-materializes it
	// (PROFILE_REFRESH_TTL, a Go duration, default 24h). AP Update{Person|
	// Group} activities refresh immediately regardless.
	ProfileRefreshTTL time.Duration
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
	// AdminToken is the bearer token protecting the /admin API (community
	// subscribe/unsubscribe/backfill). ADMIN_TOKEN; required in production,
	// dev default is a fixed, publicly known value.
	AdminToken string
	// BackfillMaxPosts caps how many posts one community backfill run
	// materializes (BACKFILL_MAX_POSTS, default 100).
	BackfillMaxPosts int
	// MintRatePerMinute is the sustained rate cap on inbound DID minting
	// (MINT_RATE_PER_MINUTE, default 60). Inbound AP activity can trigger
	// minting (post/comment authors), so it must be bounded — PLC
	// registrations are forever.
	MintRatePerMinute float64
	// MintBurst is the mint token bucket's burst size (MINT_BURST, default
	// 120) — sized to absorb a community backfill's author spike.
	MintBurst int
	// IngestWorkers is the inbox queue worker-pool size (INGEST_WORKERS,
	// default 4).
	IngestWorkers int
	// SeedCountsFromAPI enables seeding backfilled posts' vote aggregates
	// from the origin instance's public API (Lemmy's `counts` field) —
	// history whose individual Like activities AP never delivers
	// (SEED_COUNTS_FROM_API, default on; set to 0/false to disable).
	SeedCountsFromAPI bool
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

	// Blob budget and profile refresh are tuning knobs like retention:
	// real defaults in every environment.
	maxBlobRaw := os.Getenv("MAX_BLOB_BYTES")
	if maxBlobRaw == "" {
		cfg.MaxBlobBytes = 5 << 20 // 5 MiB
		logger.Info("MAX_BLOB_BYTES not set, using default", "value", cfg.MaxBlobBytes)
	} else {
		parsed, err := strconv.ParseInt(maxBlobRaw, 10, 64)
		if err != nil || parsed <= 0 {
			return nil, fmt.Errorf("config: MAX_BLOB_BYTES must be a positive integer, got %q", maxBlobRaw)
		}
		cfg.MaxBlobBytes = parsed
	}

	profileTTLRaw := os.Getenv("PROFILE_REFRESH_TTL")
	if profileTTLRaw == "" {
		profileTTLRaw = "24h"
		logger.Info("PROFILE_REFRESH_TTL not set, using default", "value", profileTTLRaw)
	}
	cfg.ProfileRefreshTTL, err = time.ParseDuration(profileTTLRaw)
	if err != nil {
		return nil, fmt.Errorf("config: PROFILE_REFRESH_TTL must be a Go duration (e.g. 24h): %w", err)
	}
	if cfg.ProfileRefreshTTL <= 0 {
		return nil, fmt.Errorf("config: PROFILE_REFRESH_TTL must be positive, got %s", cfg.ProfileRefreshTTL)
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

	// Admin API auth: like the KEK, the dev default is fixed and public —
	// required in production.
	cfg.AdminToken, err = stringVar(logger, isDevelopment, "ADMIN_TOKEN", "dev-admin-token")
	if err != nil {
		return nil, err
	}

	// Ingestion tuning knobs: real defaults in every environment.
	cfg.BackfillMaxPosts, err = intVar(logger, "BACKFILL_MAX_POSTS", 100)
	if err != nil {
		return nil, err
	}
	mintRate, err := intVar(logger, "MINT_RATE_PER_MINUTE", 60)
	if err != nil {
		return nil, err
	}
	cfg.MintRatePerMinute = float64(mintRate)
	cfg.MintBurst, err = intVar(logger, "MINT_BURST", 120)
	if err != nil {
		return nil, err
	}
	cfg.IngestWorkers, err = intVar(logger, "INGEST_WORKERS", 4)
	if err != nil {
		return nil, err
	}
	cfg.SeedCountsFromAPI, err = boolVarDefault(logger, "SEED_COUNTS_FROM_API", true)
	if err != nil {
		return nil, err
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

// intVar returns a positive-integer environment variable, falling back to a
// logged default in every environment (tuning knob semantics, like
// FIREHOSE_RETENTION).
func intVar(logger *slog.Logger, name string, fallback int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		logger.Info(name+" not set, using default", "value", fallback)
		return fallback, nil
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("config: %s must be a positive integer, got %q", name, raw)
	}
	return parsed, nil
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

// boolVarDefault parses a boolean environment variable with an explicit
// default when unset (tuning-knob semantics: same default in every
// environment, logged when applied). Unlike boolVar it rejects
// unrecognized values instead of silently reading them as false — a
// default-on flag "disabled" by a typo would be invisible.
func boolVarDefault(logger *slog.Logger, name string, fallback bool) (bool, error) {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	if raw == "" {
		logger.Info(name+" not set, using default", "value", fallback)
		return fallback, nil
	}
	switch raw {
	case "1", "true", "yes", "on":
		return true, nil
	case "0", "false", "no", "off":
		return false, nil
	}
	return false, fmt.Errorf("config: %s must be a boolean (1/0, true/false, yes/no, on/off), got %q", name, raw)
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
