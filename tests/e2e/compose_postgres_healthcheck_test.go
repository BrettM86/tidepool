package e2e_test

// No build tag, like compose_oneshots_test.go: a static check of
// docker-compose.e2e.yml that needs no Docker.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestComposePostgresHealthchecks_ProbeOverTCP checks that every pg_isready
// healthcheck connects over TCP. On a fresh volume the postgres image runs
// initdb against a temporary server that listens only on the Unix socket, then
// restarts. A socket probe reports that temporary server ready, so a dependent
// service started on service_healthy can connect during the restart and exit
// with "connection refused" (lemmy did, 2026-11-06). The temporary server
// never listens on TCP, so a probe of 127.0.0.1 waits for the real one.
func TestComposePostgresHealthchecks_ProbeOverTCP(t *testing.T) {
	composePath := filepath.Join("..", "..", "docker-compose.e2e.yml")
	raw, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatalf("read %s: %v", composePath, err)
	}
	var compose composeFile
	if err := yaml.Unmarshal(raw, &compose); err != nil {
		t.Fatalf("parse %s: %v", composePath, err)
	}
	probed := 0
	for name, service := range compose.Services {
		if service.Healthcheck.Kind == 0 {
			continue
		}
		var healthcheck struct {
			Test []string `yaml:"test"`
		}
		if err := service.Healthcheck.Decode(&healthcheck); err != nil {
			t.Fatalf("service %q: decode healthcheck: %v", name, err)
		}
		command := strings.Join(healthcheck.Test, " ")
		if !strings.Contains(command, "pg_isready") {
			continue
		}
		probed++
		if !strings.Contains(command, "-h 127.0.0.1") {
			t.Errorf("service %q healthcheck %q probes the Unix socket; add -h 127.0.0.1 so initdb's temporary server is not reported ready", name, command)
		}
	}
	if probed == 0 {
		t.Fatalf("%s has no pg_isready healthcheck; the check found nothing to verify", composePath)
	}
}
