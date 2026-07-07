// Package config loads Tidepool configuration from environment variables.
// In development, missing values fall back to logged dev defaults (Coves
// style, no config library). In production, required values must be set.
package config

import (
	"fmt"
	"log/slog"
	"os"
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
	PLCDirectoryURL string
	// BridgeServiceDID optionally pins a pre-provisioned service DID for the
	// bridge's own actor. Empty means task 03 will mint one on first run.
	BridgeServiceDID string
	// UserAgent is sent on all outbound HTTP requests (signed fetches etc.).
	UserAgent string
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

	// Optional in every environment: an operator may pre-provision the
	// bridge's service DID, otherwise identity bootstrap mints one.
	cfg.BridgeServiceDID = os.Getenv("BRIDGE_SERVICE_DID")

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
