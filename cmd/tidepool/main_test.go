package main

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestDispatchRejectsUnknownCommands(t *testing.T) {
	err := dispatch(testLogger(), []string{"serve"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "usage: tidepool [migrate]")
}

func TestRunMigrationsRequiresDatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	err := runMigrations(testLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DATABASE_URL is required")
}
