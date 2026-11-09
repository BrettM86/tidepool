package e2e_test

// No build tag, like compose_oneshots_test.go: a static check of
// docker-compose.e2e.yml that needs no Docker.

import (
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestComposeCovesAppView_LongRunningServicesInDefaultStack checks that the
// Coves AppView and its Postgres are part of the default e2e stack, are
// long-running (healthchecked, so `up --wait` waits for them rather than
// treating them as one-shots), and that the AppView starts after the stack's
// PLC, Jetstream, PDS and its own database.
func TestComposeCovesAppView_LongRunningServicesInDefaultStack(t *testing.T) {
	composePath := filepath.Join("..", "..", "docker-compose.e2e.yml")
	raw, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatalf("read %s: %v", composePath, err)
	}
	var compose composeFile
	if err := yaml.Unmarshal(raw, &compose); err != nil {
		t.Fatalf("parse %s: %v", composePath, err)
	}
	oneShots := oneShotServices(compose)

	services := []struct {
		name      string
		dependsOn []string
	}{
		{name: "coves-appview-postgres"},
		{name: "coves-appview", dependsOn: []string{"plc", "jetstream", "pds", "coves-appview-postgres"}},
	}
	for _, want := range services {
		t.Run(want.name, func(t *testing.T) {
			service, ok := compose.Services[want.name]
			if !ok {
				t.Fatalf("%s has no service %q", composePath, want.name)
			}
			if len(service.Profiles) > 0 {
				t.Errorf("service %q has profiles %v; it must be default-enabled (no profiles)", want.name, service.Profiles)
			}
			if service.Healthcheck.Kind == 0 {
				t.Errorf("service %q has no healthcheck", want.name)
			}
			if slices.Contains(oneShots, want.name) {
				t.Errorf("service %q is derived as a one-shot (%v); it must be long-running", want.name, oneShots)
			}
			dependencies := composeDependencies(t, want.name, service.DependsOn)
			var got []string
			for name := range dependencies {
				got = append(got, name)
			}
			sort.Strings(got)
			for _, dependency := range want.dependsOn {
				if _, ok := dependencies[dependency]; !ok {
					t.Errorf("service %q depends_on %v lacks %q", want.name, got, dependency)
				}
			}
		})
	}
}

// TestComposeCovesAppView_HandleWellKnownHostsCoverStackHandles checks that
// every handle suffix the e2e stack puts on an atproto identity has a
// HANDLE_WELL_KNOWN_HOSTS entry on coves-appview. Coves skips DNS only for the
// suffixes listed there; any other handle is looked up through DNS TXT and the
// public HTTPS well-known, and coves-appview shares the relay's namespace,
// whose upstream resolvers are the host's.
func TestComposeCovesAppView_HandleWellKnownHostsCoverStackHandles(t *testing.T) {
	// The bridge serves /.well-known/atproto-did keyed on Host for every name
	// under BRIDGE_HOSTNAME (tidepool); the reference PDS answers for its own
	// handles on localhost:3001 in the relay's namespace.
	want := map[string]string{
		// New-format bridged handles: HANDLE_ZONE_ROOT defaults to
		// "ap." + BRIDGE_HOSTNAME (internal/config/config.go), giving
		// {name}.lemmy.ap.tidepool.
		".ap.tidepool": "tidepool:80",
		// Old-format bridged handles {name}.lemmy.tidepool, which the rename
		// tests put on identity events (prepareOldFormatActor in
		// tests/e2e/helpers.go, asserted in tests/e2e/rename_test.go).
		".lemmy.tidepool": "tidepool:80",
		// Native PDS handles: PDS_SERVICE_HANDLE_DOMAINS is .pds.test
		// (native-alice.pds.test, created by pds-bootstrap).
		".pds.test": "localhost:3001",
	}

	composePath := filepath.Join("..", "..", "docker-compose.e2e.yml")
	raw, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatalf("read %s: %v", composePath, err)
	}
	var compose composeFile
	if err := yaml.Unmarshal(raw, &compose); err != nil {
		t.Fatalf("parse %s: %v", composePath, err)
	}
	service, ok := compose.Services["coves-appview"]
	if !ok {
		t.Fatalf("%s has no service %q", composePath, "coves-appview")
	}
	value, ok := service.Environment["HANDLE_WELL_KNOWN_HOSTS"]
	if !ok {
		t.Fatalf("coves-appview sets no HANDLE_WELL_KNOWN_HOSTS")
	}

	got := make(map[string]string)
	for _, entry := range strings.Split(value, ",") {
		suffix, host, found := strings.Cut(strings.TrimSpace(entry), "=")
		if !found {
			t.Fatalf("HANDLE_WELL_KNOWN_HOSTS entry %q is not <suffix>=<host:port>", entry)
		}
		got[suffix] = host
	}
	for suffix, host := range want {
		if got[suffix] != host {
			t.Errorf("HANDLE_WELL_KNOWN_HOSTS %q maps %q to %q, want %q", value, suffix, got[suffix], host)
		}
	}
	for suffix, host := range got {
		if _, ok := want[suffix]; !ok {
			t.Errorf("HANDLE_WELL_KNOWN_HOSTS %q has unexpected entry %q=%q", value, suffix, host)
		}
	}
}
