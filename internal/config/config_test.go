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
		"PLC_DIRECTORY_URL", "BRIDGE_SERVICE_DID", "USER_AGENT",
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
	assert.Equal(t, "http://localhost:3002", cfg.PLCDirectoryURL)
	assert.Empty(t, cfg.BridgeServiceDID, "service DID is optional")
	assert.Equal(t, "tidepool/0.1 (+https://localhost)", cfg.UserAgent)
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

	cfg, err := Load(discardLogger())
	require.NoError(t, err)
	assert.False(t, cfg.IsDevelopment())
	assert.Equal(t, "tidepool/0.1 (+https://tidepool.example)", cfg.UserAgent,
		"user agent default derives from the bridge hostname")
}

func TestLoad_RejectsUnknownEnvironment(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("ENVIRONMENT", "staging")

	_, err := Load(discardLogger())
	require.Error(t, err)
}
