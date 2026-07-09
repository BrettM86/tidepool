package config

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func clearConfigEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"ENVIRONMENT", "DATABASE_URL", "LISTEN_ADDR", "BRIDGE_HOSTNAME",
		"PLC_DIRECTORY_URL", "BRIDGE_SERVICE_DID", "USER_AGENT", "BRIDGE_KEK",
		"ADMIN_TOKEN", "BACKFILL_MAX_POSTS", "MINT_RATE_PER_MINUTE",
		"MINT_BURST", "INGEST_WORKERS",
	} {
		t.Setenv(name, "")
	}
}

func TestLoad_DevelopmentDefaults(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := Load(discardLogger())
	require.NoError(t, err)

	assert.Equal(t, EnvironmentDevelopment, cfg.Environment)
	assert.True(t, cfg.IsDevelopment())
	assert.Equal(t, "postgres://tidepool:tidepool@localhost:5442/tidepool_dev?sslmode=disable", cfg.DatabaseURL)
	assert.Equal(t, ":8091", cfg.ListenAddr)
	assert.Equal(t, "localhost", cfg.BridgeHostname)
	assert.Equal(t, "http://localhost:3002", cfg.PLCDirectoryURL,
		"the dev default PLC directory must be LOCAL, never the live plc.directory")
	assert.Empty(t, cfg.BridgeServiceDID, "service DID is optional")
	assert.Equal(t, "tidepool/0.1 (+https://localhost)", cfg.UserAgent)
	assert.Len(t, cfg.BridgeKEK, 32, "dev-default KEK decodes to 32 bytes")
}

func TestLoad_ExplicitValuesWin(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("DATABASE_URL", "postgres://example/db")
	t.Setenv("LISTEN_ADDR", ":9999")
	t.Setenv("BRIDGE_HOSTNAME", "tidepool.example")
	t.Setenv("PLC_DIRECTORY_URL", "https://plc.directory")
	t.Setenv("BRIDGE_SERVICE_DID", "did:plc:ewvi7nxzyoun6zhxrhs64oiz")
	t.Setenv("USER_AGENT", "custom-agent/1.0")

	cfg, err := Load(discardLogger())
	require.NoError(t, err)

	assert.Equal(t, "postgres://example/db", cfg.DatabaseURL)
	assert.Equal(t, ":9999", cfg.ListenAddr)
	assert.Equal(t, "tidepool.example", cfg.BridgeHostname)
	assert.Equal(t, "https://plc.directory", cfg.PLCDirectoryURL)
	assert.Equal(t, "did:plc:ewvi7nxzyoun6zhxrhs64oiz", cfg.BridgeServiceDID)
	assert.Equal(t, "custom-agent/1.0", cfg.UserAgent)
}

func TestLoad_ProductionRequiresValues(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("ENVIRONMENT", EnvironmentProduction)

	_, err := Load(discardLogger())
	require.Error(t, err, "production must not fall back to dev defaults")
	assert.Contains(t, err.Error(), "DATABASE_URL")
}

func TestLoad_ProductionWithAllValues(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("ENVIRONMENT", EnvironmentProduction)
	t.Setenv("DATABASE_URL", "postgres://prod/db")
	t.Setenv("LISTEN_ADDR", ":8080")
	t.Setenv("BRIDGE_HOSTNAME", "tidepool.example")
	t.Setenv("PLC_DIRECTORY_URL", "https://plc.directory")
	t.Setenv("BRIDGE_KEK", "sfDrM4bIeCJp01ZBTArLPJXNQlD7pcYFsod2An6UAF0=") // base64 form
	t.Setenv("ADMIN_TOKEN", "prod-admin-token")

	cfg, err := Load(discardLogger())
	require.NoError(t, err)
	assert.False(t, cfg.IsDevelopment())
	assert.Equal(t, "tidepool/0.1 (+https://tidepool.example)", cfg.UserAgent,
		"user agent default derives from the bridge hostname")
	assert.Len(t, cfg.BridgeKEK, 32, "base64 KEK decodes to 32 bytes")
	assert.Equal(t, "prod-admin-token", cfg.AdminToken)
	assert.Equal(t, 100, cfg.BackfillMaxPosts, "tuning knobs default in every environment")
	assert.Equal(t, float64(60), cfg.MintRatePerMinute)
	assert.Equal(t, 120, cfg.MintBurst)
	assert.Equal(t, 4, cfg.IngestWorkers)
}

func TestLoad_ProductionRequiresAdminToken(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("ENVIRONMENT", EnvironmentProduction)
	t.Setenv("DATABASE_URL", "postgres://prod/db")
	t.Setenv("LISTEN_ADDR", ":8080")
	t.Setenv("BRIDGE_HOSTNAME", "tidepool.example")
	t.Setenv("PLC_DIRECTORY_URL", "https://plc.directory")
	t.Setenv("BRIDGE_KEK", "sfDrM4bIeCJp01ZBTArLPJXNQlD7pcYFsod2An6UAF0=")

	_, err := Load(discardLogger())
	require.Error(t, err, "production must never run on the public dev-default admin token")
	assert.Contains(t, err.Error(), "ADMIN_TOKEN")
}

func TestLoad_ProductionRequiresKEK(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("ENVIRONMENT", EnvironmentProduction)
	t.Setenv("DATABASE_URL", "postgres://prod/db")
	t.Setenv("LISTEN_ADDR", ":8080")
	t.Setenv("BRIDGE_HOSTNAME", "tidepool.example")
	t.Setenv("PLC_DIRECTORY_URL", "https://plc.directory")

	_, err := Load(discardLogger())
	require.Error(t, err, "production must never run on the public dev-default KEK")
	assert.Contains(t, err.Error(), "BRIDGE_KEK")
}

func TestLoad_RejectsBadKEK(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("BRIDGE_KEK", "too-short")

	_, err := Load(discardLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "BRIDGE_KEK")
}

func TestLoad_RejectsUnknownEnvironment(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("ENVIRONMENT", "staging")

	_, err := Load(discardLogger())
	require.Error(t, err)
}
