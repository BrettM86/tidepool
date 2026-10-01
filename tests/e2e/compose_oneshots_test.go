package e2e_test

// This file deliberately carries NO build tag: it checks a static property of
// docker-compose.e2e.yml, needs neither Docker, the network nor Postgres, and
// so runs in a plain `go test ./...`. It is the external test package
// e2e_test because it uses nothing from package e2e; that keeps its names out
// of the tagged files' namespace when it is built with `-tags e2e`.

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// longRunningServicesWithoutHealthcheck are the default-enabled services that
// keep running but define no healthcheck: `dns` runs a scratch-based image
// with no shell to run one in, and `pictrs` has none defined. Every other
// default-enabled service without a healthcheck is treated as a one-shot, so
// a new long-running service without a healthcheck must be added here; until
// it is, the test fails for it, which is the safe direction.
var longRunningServicesWithoutHealthcheck = map[string]bool{
	"dns":    true,
	"pictrs": true,
}

// knownOneShotServices are the e2e stack's services whose containers exit by
// design once their job is done. The test derives the one-shots from the
// compose file; this list only asserts that the derivation still finds these,
// so it can never quietly find none.
var knownOneShotServices = []string{
	"tidepool-migrate",
	"relay-bootstrap",
	"pds-bootstrap",
}

// composeFile is the slice of a compose file this check reads.
type composeFile struct {
	Services map[string]composeService `yaml:"services"`
}

type composeService struct {
	Profiles []string `yaml:"profiles"`
	// Healthcheck is only checked for presence, so it stays a raw node.
	Healthcheck yaml.Node `yaml:"healthcheck"`
	// DependsOn stays a raw node because Compose accepts two shapes: the
	// short list form (`depends_on: [a, b]`, meaning service_started) and the
	// long map form (`depends_on: {a: {condition: ...}}`).
	DependsOn yaml.Node `yaml:"depends_on"`
}

// composeDependency is one entry of the long depends_on form.
type composeDependency struct {
	Condition string `yaml:"condition"`
}

// composeDependencies normalizes a service's depends_on into the long form,
// whichever shape the file uses. The short list form has no condition; it
// means service_started.
func composeDependencies(t *testing.T, serviceName string, node yaml.Node) map[string]composeDependency {
	t.Helper()
	switch node.Kind {
	case 0: // depends_on absent
		return nil
	case yaml.SequenceNode:
		var names []string
		if err := node.Decode(&names); err != nil {
			t.Fatalf("service %q: decode short-form depends_on: %v", serviceName, err)
		}
		dependencies := make(map[string]composeDependency, len(names))
		for _, name := range names {
			dependencies[name] = composeDependency{Condition: "service_started"}
		}
		return dependencies
	case yaml.MappingNode:
		var dependencies map[string]composeDependency
		if err := node.Decode(&dependencies); err != nil {
			t.Fatalf("service %q: decode long-form depends_on: %v", serviceName, err)
		}
		return dependencies
	default:
		t.Fatalf("service %q: depends_on at line %d is neither a list nor a map (yaml node kind %d)", serviceName, node.Line, node.Kind)
		return nil
	}
}

// oneShotServices returns, sorted, every service with no profiles and no
// healthcheck that is not in longRunningServicesWithoutHealthcheck.
func oneShotServices(compose composeFile) []string {
	var oneShots []string
	for name, service := range compose.Services {
		if len(service.Profiles) > 0 || service.Healthcheck.Kind != 0 || longRunningServicesWithoutHealthcheck[name] {
			continue
		}
		oneShots = append(oneShots, name)
	}
	sort.Strings(oneShots)
	return oneShots
}

// TestComposeOneShots_AwaitedToCompletionByDefaultService guards `make e2e`'s
// `docker compose up --wait`. With --wait, Compose fails the whole `up` if any
// container has exited, EXCEPT a service that some default-enabled service
// lists in depends_on with condition: service_completed_successfully — for
// that one Compose waits for a successful exit instead. A one-shot without
// such a dependent makes `up --wait` fail with "container ... exited (0)"
// whenever the wait poll sees it after it finished: a race, not a bug in the
// one-shot.
//
// The one-shots are derived from the compose file rather than listed, so a
// new one-shot is checked without anyone remembering to register it: every
// service with no profiles and no healthcheck counts as one, except those in
// longRunningServicesWithoutHealthcheck.
func TestComposeOneShots_AwaitedToCompletionByDefaultService(t *testing.T) {
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
	derived := make(map[string]bool, len(oneShots))
	for _, oneShot := range oneShots {
		derived[oneShot] = true
	}
	for _, known := range knownOneShotServices {
		if !derived[known] {
			t.Fatalf("known one-shot service %q is not among the one-shots derived from %s (%s): "+
				"the heuristic no longer finds it, because it gained a healthcheck or profiles, or was renamed or removed. "+
				"Update knownOneShotServices.",
				known, composePath, strings.Join(oneShots, ", "))
		}
	}

	dependenciesByService := make(map[string]map[string]composeDependency, len(compose.Services))
	for name, service := range compose.Services {
		dependenciesByService[name] = composeDependencies(t, name, service.DependsOn)
	}

	for _, oneShot := range oneShots {
		t.Run(oneShot, func(t *testing.T) {
			var awaitedBy []string
			// Dependents that name the one-shot but do not make Compose wait
			// for its successful exit, reported so the fix is obvious.
			var insufficient []string
			for dependentName, dependencies := range dependenciesByService {
				if dependentName == oneShot {
					continue
				}
				dependency, ok := dependencies[oneShot]
				if !ok {
					continue
				}
				dependent := compose.Services[dependentName]
				switch {
				case dependency.Condition != "service_completed_successfully":
					condition := dependency.Condition
					if condition == "" {
						condition = "service_started"
					}
					insufficient = append(insufficient, dependentName+" (condition: "+condition+")")
				case len(dependent.Profiles) > 0:
					insufficient = append(insufficient, dependentName+" (only enabled under profiles "+strings.Join(dependent.Profiles, ", ")+")")
				default:
					awaitedBy = append(awaitedBy, dependentName)
				}
			}

			if len(awaitedBy) > 0 {
				return
			}
			sort.Strings(insufficient)
			found := "no service depends on it at all"
			if len(insufficient) > 0 {
				found = "dependents that do not qualify: " + strings.Join(insufficient, "; ")
			}
			t.Errorf("one-shot service %q is not awaited to completion by any default-enabled service (%s).\n"+
				"Its container exits by design, so `docker compose up --wait` (make e2e) fails with "+
				"\"container tidepool-e2e-%s-1 exited (0)\" whenever the wait poll sees it after it exits.\n"+
				"Fix: list %q under depends_on of a service with no profiles, using "+
				"condition: service_completed_successfully. If it is a long-running service with no "+
				"healthcheck, add it to longRunningServicesWithoutHealthcheck instead.",
				oneShot, found, oneShot, oneShot)
		})
	}
}
