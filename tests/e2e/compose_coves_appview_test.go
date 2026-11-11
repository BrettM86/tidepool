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

// TestComposeCovesAppView_ModerationAdminFromBootstrappedAccount checks how
// the instance admin's DID reaches the AppView. The reference PDS mints a
// fresh did:plc for the admin account on every stack, so the DID cannot be
// written into the compose file: pds-bootstrap creates the account and writes
// its DID to /coves-admin/admin.did on the shared coves-admin volume, and
// coves-appview, started only once pds-bootstrap has exited successfully,
// reads that file into MODERATION_ADMINS before it execs the server.
func TestComposeCovesAppView_ModerationAdminFromBootstrappedAccount(t *testing.T) {
	composePath := filepath.Join("..", "..", "docker-compose.e2e.yml")
	raw, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatalf("read %s: %v", composePath, err)
	}
	var compose composeFile
	if err := yaml.Unmarshal(raw, &compose); err != nil {
		t.Fatalf("parse %s: %v", composePath, err)
	}
	bootstrap, ok := compose.Services["pds-bootstrap"]
	if !ok {
		t.Fatalf("%s has no service %q", composePath, "pds-bootstrap")
	}
	appView, ok := compose.Services["coves-appview"]
	if !ok {
		t.Fatalf("%s has no service %q", composePath, "coves-appview")
	}

	dependency, ok := composeDependencies(t, "coves-appview", appView.DependsOn)["pds-bootstrap"]
	if !ok {
		t.Errorf("coves-appview does not depend on pds-bootstrap")
	} else if dependency.Condition != "service_completed_successfully" {
		t.Errorf("coves-appview depends on pds-bootstrap with condition %q, want %q",
			dependency.Condition, "service_completed_successfully")
	}

	if _, ok := compose.Volumes["coves-admin"]; !ok {
		t.Errorf("%s declares no top-level volume %q", composePath, "coves-admin")
	}
	if !slices.Contains(bootstrap.Volumes, "coves-admin:/coves-admin") {
		t.Errorf("pds-bootstrap volumes %q lack the writable mount %q", bootstrap.Volumes, "coves-admin:/coves-admin")
	}
	if !slices.Contains(appView.Volumes, "coves-admin:/coves-admin:ro") {
		t.Errorf("coves-appview volumes %q lack the read-only mount %q", appView.Volumes, "coves-admin:/coves-admin:ro")
	}

	bootstrapScript := composeCommandText(t, "pds-bootstrap", bootstrap.Command)
	if !strings.Contains(bootstrapScript, "/coves-admin/admin.did") {
		t.Errorf("pds-bootstrap's command never writes /coves-admin/admin.did:\n%s", bootstrapScript)
	}

	if got := composeCommandText(t, "coves-appview", appView.Entrypoint); got != "/bin/sh\n-ec" {
		t.Errorf("coves-appview entrypoint = %q, want [\"/bin/sh\", \"-ec\"]", got)
	}
	appViewScript := composeCommandText(t, "coves-appview", appView.Command)
	for _, want := range []string{
		`admin_did="$(cat /coves-admin/admin.did)"`,
		`[ -n "$admin_did" ] ||`,
		"unreadable or blank",
		`export MODERATION_ADMINS="$admin_did"`,
		"exec /app/coves-server",
	} {
		if !strings.Contains(appViewScript, want) {
			t.Errorf("coves-appview's command lacks %q:\n%s", want, appViewScript)
		}
	}
	if value, set := appView.Environment["MODERATION_ADMINS"]; set {
		t.Errorf("coves-appview's environment sets MODERATION_ADMINS to %q; it must come only from admin.did", value)
	}
}

// composeCommandText returns an entrypoint or command as the container
// receives it: list items joined by newlines, after Compose's interpolation
// has turned each `$$` into `$`. A `$` that starts neither `$$` nor `${` is
// one Compose itself would reject or expand, so it fails the test.
func composeCommandText(t *testing.T, serviceName string, node yaml.Node) string {
	t.Helper()
	var parts []string
	switch node.Kind {
	case 0:
		return ""
	case yaml.ScalarNode:
		parts = []string{node.Value}
	case yaml.SequenceNode:
		if err := node.Decode(&parts); err != nil {
			t.Fatalf("service %q: decode list at line %d: %v", serviceName, node.Line, err)
		}
	default:
		t.Fatalf("service %q: entrypoint/command at line %d is neither a string nor a list", serviceName, node.Line)
	}
	text := strings.Join(parts, "\n")
	var out strings.Builder
	for i := 0; i < len(text); i++ {
		if text[i] != '$' {
			out.WriteByte(text[i])
			continue
		}
		if i+1 < len(text) && text[i+1] == '$' {
			out.WriteByte('$')
			i++
			continue
		}
		if i+1 < len(text) && text[i+1] == '{' {
			out.WriteByte('$')
			continue
		}
		t.Fatalf("service %q: entrypoint/command has a bare $ that Compose interpolates (write $$ for the shell):\n%s", serviceName, text)
	}
	return out.String()
}
