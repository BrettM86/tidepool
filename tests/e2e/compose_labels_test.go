package e2e_test

// Untagged like compose_oneshots_test.go: a static check of
// docker-compose.e2e.yml that needs neither Docker nor the network.

import (
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/bluesky-social/indigo/atproto/atcrypto"
	"gopkg.in/yaml.v3"
)

// covesAppViewInNetworkOrigin is where the AppView listens inside the stack
// (it shares relay's network namespace on port 8081).
const covesAppViewInNetworkOrigin = "relay:8081"

// TestComposeLabels_TidepoolReachesTheStackLabeler checks that the label
// consumer's wiring in the e2e stack holds together: Tidepool follows the
// AppView's own instance did:web, that did:web resolves over HTTPS through
// handle-tls (whose CA Tidepool trusts) to the AppView, the label signing key
// is not the #atproto key, and the #atproto_labeler endpoint the AppView
// publishes (APPVIEW_PUBLIC_URL) reaches the AppView from inside Tidepool's
// network namespace.
func TestComposeLabels_TidepoolReachesTheStackLabeler(t *testing.T) {
	composePath := filepath.Join("..", "..", "docker-compose.e2e.yml")
	raw, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatalf("read %s: %v", composePath, err)
	}
	var compose composeFile
	if err := yaml.Unmarshal(raw, &compose); err != nil {
		t.Fatalf("parse %s: %v", composePath, err)
	}
	service := func(name string) composeService {
		t.Helper()
		s, ok := compose.Services[name]
		if !ok {
			t.Fatalf("service %q is missing from %s", name, composePath)
		}
		return s
	}
	tidepool := service("tidepool")
	appView := service("coves-appview")
	handleTLS := service("handle-tls")

	t.Run("tidepool follows the AppView's did:web through handle-tls", func(t *testing.T) {
		labelerDID := tidepool.Environment["COVES_LABELER_DID"]
		instanceDID := appView.Environment["INSTANCE_DID"]
		if labelerDID == "" || labelerDID != instanceDID {
			t.Fatalf("tidepool COVES_LABELER_DID = %q, coves-appview INSTANCE_DID = %q; want the same non-empty DID", labelerDID, instanceDID)
		}
		identifier, ok := strings.CutPrefix(labelerDID, "did:web:")
		if !ok {
			t.Fatalf("COVES_LABELER_DID = %q; want a did:web", labelerDID)
		}
		host, err := url.PathUnescape(identifier)
		if err != nil || host == "" || strings.ContainsAny(host, ":/") {
			t.Fatalf("did:web %q: want a portless host (handle-tls serves 443), got %q (%v)", labelerDID, host, err)
		}
		if !slices.Contains(composeNetworkAliases(t, "handle-tls", handleTLS.Networks), host) {
			t.Errorf("handle-tls has no network alias %q, so tidepool cannot reach https://%s", host, host)
		}
		caddyfile := composeCommandText(t, "handle-tls", handleTLS.Command)
		if !caddySiteProxiesTo(caddyfile, host, covesAppViewInNetworkOrigin) {
			t.Errorf("handle-tls's Caddyfile has no site block %q proxying to %s:\n%s", host, covesAppViewInNetworkOrigin, caddyfile)
		}
	})

	t.Run("the label signing key is not the #atproto key", func(t *testing.T) {
		signingKey, err := atcrypto.ParsePrivateMultibase(appView.Environment["LABEL_SIGNING_KEY"])
		if err != nil {
			t.Fatalf("coves-appview LABEL_SIGNING_KEY: %v", err)
		}
		signingPublic, err := signingKey.PublicKey()
		if err != nil {
			t.Fatalf("LABEL_SIGNING_KEY public key: %v", err)
		}
		atprotoKey := appView.Environment["INSTANCE_ATPROTO_PUBLIC_KEY"]
		if _, err := atcrypto.ParsePublicMultibase(atprotoKey); err != nil {
			t.Fatalf("coves-appview INSTANCE_ATPROTO_PUBLIC_KEY %q: %v", atprotoKey, err)
		}
		if signingPublic.Multibase() == atprotoKey {
			t.Errorf("INSTANCE_ATPROTO_PUBLIC_KEY is the LABEL_SIGNING_KEY's public key %q; Coves refuses to start", atprotoKey)
		}
	})

	t.Run("the published labeler endpoint reaches the AppView from tidepool", func(t *testing.T) {
		publicURL := appView.Environment["APPVIEW_PUBLIC_URL"]
		hostPort, ok := strings.CutPrefix(publicURL, "http://")
		if !ok || hostPort == "" {
			t.Fatalf("coves-appview APPVIEW_PUBLIC_URL = %q; want an http:// origin", publicURL)
		}
		site := "http://" + hostPort
		var forwarders []string
		for name, s := range compose.Services {
			if s.NetworkMode != "service:tidepool" {
				continue
			}
			if caddySiteProxiesTo(composeCommandText(t, name, s.Command), site, covesAppViewInNetworkOrigin) {
				forwarders = append(forwarders, name)
			}
		}
		if len(forwarders) == 0 {
			t.Errorf("no service with network_mode service:tidepool serves %s proxying to %s; the #atproto_labeler endpoint %q is unreachable from tidepool", site, covesAppViewInNetworkOrigin, publicURL)
		}
	})
}

// composeNetworkAliases returns every alias the service declares across its
// networks. The list form of networks carries no aliases.
func composeNetworkAliases(t *testing.T, serviceName string, node yaml.Node) []string {
	t.Helper()
	if node.Kind != yaml.MappingNode {
		return nil
	}
	var networks map[string]struct {
		Aliases []string `yaml:"aliases"`
	}
	if err := node.Decode(&networks); err != nil {
		t.Fatalf("service %q: decode networks at line %d: %v", serviceName, node.Line, err)
	}
	var aliases []string
	for _, network := range networks {
		aliases = append(aliases, network.Aliases...)
	}
	return aliases
}

// caddySiteProxiesTo reports whether the Caddyfile text has a site block
// opened by `<address> {` whose body has `reverse_proxy <upstream>`.
func caddySiteProxiesTo(caddyfile, address, upstream string) bool {
	lines := strings.Split(caddyfile, "\n")
	for i, line := range lines {
		if strings.TrimSpace(line) != address+" {" {
			continue
		}
		depth := 1
		for _, bodyLine := range lines[i+1:] {
			trimmed := strings.TrimSpace(bodyLine)
			if depth == 1 && trimmed == "reverse_proxy "+upstream {
				return true
			}
			depth += strings.Count(trimmed, "{") - strings.Count(trimmed, "}")
			if depth <= 0 {
				break
			}
		}
	}
	return false
}
