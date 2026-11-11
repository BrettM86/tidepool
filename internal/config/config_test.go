package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"

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
		"DNS_LISTEN", "DNS_PUBLIC_IPV4", "DNS_PUBLIC_IPV6", "DNS_NAMESERVERS", "HANDLE_ZONE_ROOT", "DNS_UDP_RESPONSE_RATE", "HANDLE_MIGRATION_RATE",
		"CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ZONE_ID", "DELEGATION_MAX_NS_RECORDS",
		"PLC_DIRECTORY_URL", "BRIDGE_SERVICE_DID", "USER_AGENT", "BRIDGE_KEK",
		"BRIDGE_KEK_PREVIOUS",
		"ADMIN_TOKEN", "BACKFILL_MAX_POSTS", "MINT_RATE_PER_MINUTE",
		"MINT_BURST", "PLC_WRITES_PER_MINUTE", "INGEST_WORKERS", "BRIDGE_SCHEME",
		"ALLOW_PRIVATE_FETCH", "ALLOW_DEV_REQUEST_CRAWL", "RELAY_HOSTS",
		"AP_USER_ORIGIN", "AP_HOST_FALLTHROUGH_DEV",
		"CONSUMER_ENABLED", "JETSTREAM_URL", "COVES_MEDIA_BASE_URL",
		"OUTBOUND_WORKERS", "OUTBOUND_DRY_RUN", "OUTBOUND_DISABLED",
		"OUTBOUND_DISABLED_HOSTS", "OUTBOUND_DISABLED_COMMUNITIES",
		"OUTBOUND_DISABLED_ACTORS",
		"BRIDGED_VOTE_RECORDS", "BRIDGED_VOTE_RECORDS_DRY_RUN", "BRIDGED_VOTE_RECORDS_INTERVAL",
		"BRIDGED_VOTE_RECORDS_BATCH",
		"BRIDGED_VOTE_VERIFY_HOST_RPS", "BRIDGED_VOTE_VERIFY_GLOBAL_RPS", "BRIDGED_VOTE_VERIFY_WINDOW",
		"BRIDGED_VOTE_VERIFY_BATCH",
	} {
		t.Setenv(name, "")
	}
}

func TestLoad_DNSDisabledWithoutPublicAddresses(t *testing.T) {
	for _, environment := range []string{EnvironmentDevelopment, EnvironmentProduction} {
		for _, listen := range []string{"unset", "empty"} {
			t.Run(environment+"/"+listen, func(t *testing.T) {
				if environment == EnvironmentProduction {
					setProductionEnv(t)
				} else {
					clearConfigEnv(t)
				}
				if listen == "unset" {
					require.NoError(t, os.Unsetenv("DNS_LISTEN"))
				}
				for _, name := range []string{"DNS_PUBLIC_IPV4", "DNS_PUBLIC_IPV6", "DNS_NAMESERVERS"} {
					require.NoError(t, os.Unsetenv(name))
				}

				cfg, err := Load(discardLogger())
				require.NoError(t, err)
				require.Empty(t, cfg.DNSListen)
			})
		}
	}
}

func TestLoad_DNSEnabledConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name            string
		ipv4            string
		ipv6            string
		nameservers     string
		wantError       string
		wantNameservers []string
		wantIPv6        netip.Addr
	}{
		{name: "missing IPv4", wantError: "DNS_PUBLIC_IPV4"},
		{name: "invalid IPv4", ipv4: "not-an-ip", wantError: "DNS_PUBLIC_IPV4"},
		{name: "IPv6 in IPv4", ipv4: "2001:db8::1", wantError: "DNS_PUBLIC_IPV4"},
		{name: "IPv4 in IPv6", ipv4: "192.0.2.1", ipv6: "192.0.2.1", wantError: "DNS_PUBLIC_IPV6"},
		{name: "invalid IPv6", ipv4: "192.0.2.1", ipv6: "garbage", wantError: "DNS_PUBLIC_IPV6"},
		{name: "empty nameserver entry", ipv4: "192.0.2.1", nameservers: "ns1.example,,ns2.example", wantError: "DNS_NAMESERVERS"},
		{name: "default nameservers and no IPv6", ipv4: "192.0.2.1", wantNameservers: []string{"ns1.tdpl.example", "ns2.tdpl.example"}},
		{name: "canonical nameservers and IPv6", ipv4: "192.0.2.1", ipv6: "2001:db8::1", nameservers: " NS-A.example. , ns-b.example ",
			wantNameservers: []string{"ns-a.example", "ns-b.example"}, wantIPv6: netip.MustParseAddr("2001:db8::1")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearConfigEnv(t)
			t.Setenv("BRIDGE_HOSTNAME", "tdpl.example")
			t.Setenv("DNS_LISTEN", "127.0.0.1:5353")
			t.Setenv("DNS_PUBLIC_IPV4", tc.ipv4)
			t.Setenv("DNS_PUBLIC_IPV6", tc.ipv6)
			t.Setenv("DNS_NAMESERVERS", tc.nameservers)
			if tc.name == "default nameservers and no IPv6" {
				require.NoError(t, os.Unsetenv("DNS_NAMESERVERS"))
				require.NoError(t, os.Unsetenv("DNS_PUBLIC_IPV6"))
			}

			cfg, err := Load(discardLogger())
			if tc.wantError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantError)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "127.0.0.1:5353", cfg.DNSListen)
			require.Equal(t, netip.MustParseAddr("192.0.2.1"), cfg.DNSPublicIPv4)
			require.Equal(t, tc.wantIPv6, cfg.DNSPublicIPv6)
			require.Equal(t, tc.wantNameservers, cfg.DNSNameservers)
		})
	}
}

func TestLoad_DNSUDPResponseRate(t *testing.T) {
	for _, tc := range []struct {
		name      string
		value     string
		unset     bool
		wantRate  int
		wantError bool
	}{
		{name: "unset defaults to 200", unset: true, wantRate: 200},
		{name: "custom rate", value: "7", wantRate: 7},
		{name: "zero", value: "0", wantError: true},
		{name: "negative", value: "-1", wantError: true},
		{name: "not numeric", value: "abc", wantError: true},
		{name: "fractional", value: "1.5", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearConfigEnv(t)
			t.Setenv("BRIDGE_HOSTNAME", "tdpl.example")
			t.Setenv("DNS_LISTEN", "127.0.0.1:5353")
			t.Setenv("DNS_PUBLIC_IPV4", "192.0.2.1")
			if tc.unset {
				require.NoError(t, os.Unsetenv("DNS_UDP_RESPONSE_RATE"))
			} else {
				t.Setenv("DNS_UDP_RESPONSE_RATE", tc.value)
			}
			cfg, err := Load(discardLogger())
			if tc.wantError {
				require.Error(t, err)
				require.Contains(t, err.Error(), "DNS_UDP_RESPONSE_RATE")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantRate, cfg.DNSUDPResponseRate)
		})
	}
}

func TestLoad_PLCWritesPerMinute(t *testing.T) {
	for _, tc := range []struct {
		name      string
		value     string
		unset     bool
		wantRate  int
		wantError bool
	}{
		{name: "unset defaults to 5", unset: true, wantRate: 5},
		{name: "empty defaults to 5", value: "", wantRate: 5},
		{name: "custom rate", value: "2", wantRate: 2},
		{name: "zero", value: "0", wantError: true},
		{name: "not numeric", value: "abc", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearConfigEnv(t)
			if tc.unset {
				require.NoError(t, os.Unsetenv("PLC_WRITES_PER_MINUTE"))
			} else {
				t.Setenv("PLC_WRITES_PER_MINUTE", tc.value)
			}
			cfg, err := Load(discardLogger())
			if tc.wantError {
				require.Error(t, err)
				require.Contains(t, err.Error(), "PLC_WRITES_PER_MINUTE")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantRate, cfg.PLCWritesPerMinute)
		})
	}
}

func TestLoad_RemovedRateVariablesAreIgnored(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("MINT_RATE_PER_MINUTE", "junk")
	t.Setenv("MINT_BURST", "junk")
	t.Setenv("HANDLE_MIGRATION_RATE", "junk")

	_, err := Load(discardLogger())
	require.NoError(t, err)
}

func TestLoad_DNSRejectsInvalidDomainNames(t *testing.T) {
	longLabel := strings.Repeat("a", 64)
	for _, tc := range []struct {
		name           string
		bridgeHostname string
		nameservers    string
		wantError      string
	}{
		{name: "nameserver with empty label", bridgeHostname: "tdpl.example", nameservers: "ns1..example", wantError: "DNS_NAMESERVERS"},
		{name: "nameserver with 64-byte label", bridgeHostname: "tdpl.example", nameservers: "ns1.example," + longLabel + ".example", wantError: "DNS_NAMESERVERS"},
		{name: "nameserver with port", bridgeHostname: "tdpl.example", nameservers: "ns1.example:53", wantError: "DNS_NAMESERVERS"},
		{name: "bridge hostname with port", bridgeHostname: "tdpl.io:443", wantError: "BRIDGE_HOSTNAME"},
		{name: "bridge hostname with port and explicit nameservers", bridgeHostname: "tdpl.io:443", nameservers: "ns1.example", wantError: "BRIDGE_HOSTNAME"},
		{name: "bridge hostname with empty label", bridgeHostname: "tdpl..example", wantError: "BRIDGE_HOSTNAME"},
		{name: "bridge hostname with 64-byte label", bridgeHostname: longLabel + ".example", wantError: "BRIDGE_HOSTNAME"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearConfigEnv(t)
			t.Setenv("BRIDGE_HOSTNAME", tc.bridgeHostname)
			t.Setenv("DNS_LISTEN", "127.0.0.1:5300")
			t.Setenv("DNS_PUBLIC_IPV4", "192.0.2.1")
			t.Setenv("DNS_NAMESERVERS", tc.nameservers)

			_, err := Load(discardLogger())
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantError)
		})
	}
}

func TestLoad_DNSAcceptsSingleLabelZoneRoot(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("BRIDGE_HOSTNAME", "localhost")
	t.Setenv("DNS_LISTEN", "127.0.0.1:5300")
	t.Setenv("DNS_PUBLIC_IPV4", "127.0.0.1")

	cfg, err := Load(discardLogger())
	require.NoError(t, err)
	require.Equal(t, []string{"ns1.localhost", "ns2.localhost"}, cfg.DNSNameservers)
}

func TestLoad_CloudflareDelegationConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name       string
		token      string
		zoneID     string
		dnsEnabled bool
		maxRecords string
		unsetMax   bool
		wantMax    int
		wantErrors []string
	}{
		{name: "token unset disables delegation", unsetMax: true},
		{name: "token requires zone ID", token: "cf-config-token", dnsEnabled: true, maxRecords: "3000", wantErrors: []string{"CLOUDFLARE_ZONE_ID"}},
		{name: "token requires DNS listener", token: "cf-config-token", zoneID: "zone-config", maxRecords: "3000", wantErrors: []string{"CLOUDFLARE_API_TOKEN", "DNS_LISTEN"}},
		{name: "enabled values are carried", token: "cf-config-token", zoneID: "zone-config", dnsEnabled: true, maxRecords: "3000", wantMax: 3000},
		{name: "enabled ceiling unset", token: "cf-config-token", zoneID: "zone-config", dnsEnabled: true, unsetMax: true, wantErrors: []string{"DELEGATION_MAX_NS_RECORDS"}},
		{name: "enabled ceiling empty", token: "cf-config-token", zoneID: "zone-config", dnsEnabled: true, wantErrors: []string{"DELEGATION_MAX_NS_RECORDS"}},
		{name: "enabled ceiling nonnumeric", token: "cf-config-token", zoneID: "zone-config", dnsEnabled: true, maxRecords: "abc", wantErrors: []string{"DELEGATION_MAX_NS_RECORDS"}},
		{name: "enabled ceiling zero", token: "cf-config-token", zoneID: "zone-config", dnsEnabled: true, maxRecords: "0", wantErrors: []string{"DELEGATION_MAX_NS_RECORDS"}},
		{name: "enabled ceiling negative", token: "cf-config-token", zoneID: "zone-config", dnsEnabled: true, maxRecords: "-5", wantErrors: []string{"DELEGATION_MAX_NS_RECORDS"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearConfigEnv(t)
			t.Setenv("CLOUDFLARE_API_TOKEN", tc.token)
			t.Setenv("CLOUDFLARE_ZONE_ID", tc.zoneID)
			t.Setenv("DELEGATION_MAX_NS_RECORDS", tc.maxRecords)
			if tc.unsetMax {
				require.NoError(t, os.Unsetenv("DELEGATION_MAX_NS_RECORDS"))
			}
			if tc.dnsEnabled {
				t.Setenv("BRIDGE_HOSTNAME", "localhost")
				t.Setenv("DNS_LISTEN", "127.0.0.1:5300")
				t.Setenv("DNS_PUBLIC_IPV4", "127.0.0.1")
			}

			cfg, err := Load(discardLogger())
			if len(tc.wantErrors) > 0 {
				require.Error(t, err)
				for _, name := range tc.wantErrors {
					assert.Contains(t, err.Error(), name)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.token, cfg.CloudflareAPIToken)
			assert.Equal(t, tc.zoneID, cfg.CloudflareZoneID)
			if tc.wantMax != 0 {
				assert.Equal(t, tc.wantMax, cfg.DelegationMaxNSRecords)
			}
		})
	}
}

func TestLoad_BridgeHostnameWithPortLoadsWhenDNSDisabled(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("BRIDGE_HOSTNAME", "tdpl.io:443")

	cfg, err := Load(discardLogger())
	require.NoError(t, err)
	require.Equal(t, "tdpl.io:443", cfg.BridgeHostname)
	require.Empty(t, cfg.DNSListen)
	require.Empty(t, cfg.DNSNameservers)
}

func TestLoad_HandleZoneRoot(t *testing.T) {
	for _, tc := range []struct {
		name           string
		bridgeHostname string
		handleZoneRoot string
		wantRoot       string
		wantError      bool
	}{
		{name: "default", bridgeHostname: "tdpl.io", wantRoot: "ap.tdpl.io"},
		{name: "lowercase default", bridgeHostname: "TDPL.io", wantRoot: "ap.tdpl.io"},
		{name: "custom root", bridgeHostname: "tdpl.io", handleZoneRoot: "Handles.TDPL.io", wantRoot: "handles.tdpl.io"},
		{name: "default without bridge port", bridgeHostname: "tdpl.io:443", wantRoot: "ap.tdpl.io"},
		{name: "outside bridge domain", bridgeHostname: "tdpl.io", handleZoneRoot: "ap.example.com", wantError: true},
		{name: "equal to bridge domain", bridgeHostname: "tdpl.io", handleZoneRoot: "tdpl.io", wantError: true},
		{name: "suffix without label boundary", bridgeHostname: "tdpl.io", handleZoneRoot: "evil-tdpl.io", wantError: true},
		{name: "empty label", bridgeHostname: "tdpl.io", handleZoneRoot: "ap..tdpl.io", wantError: true},
		{name: "port", bridgeHostname: "tdpl.io", handleZoneRoot: "ap.tdpl.io:53", wantError: true},
		{name: "underscore", bridgeHostname: "tdpl.io", handleZoneRoot: "_ap.tdpl.io", wantError: true},
		{name: "leading hyphen", bridgeHostname: "tdpl.io", handleZoneRoot: "-ap.tdpl.io", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearConfigEnv(t)
			t.Setenv("BRIDGE_HOSTNAME", tc.bridgeHostname)
			if tc.handleZoneRoot == "" {
				require.NoError(t, os.Unsetenv("HANDLE_ZONE_ROOT"))
			} else {
				t.Setenv("HANDLE_ZONE_ROOT", tc.handleZoneRoot)
			}

			cfg, err := Load(discardLogger())
			if tc.wantError {
				require.Error(t, err)
				require.Contains(t, err.Error(), "HANDLE_ZONE_ROOT")
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantRoot, cfg.HandleZoneRoot)
		})
	}
}

// setProductionEnv sets every variable production requires, so a test about
// ONE of them can blank exactly that one. Adding a new required variable
// here (AP_USER_ORIGIN was the last) then updates every production test at
// once instead of breaking them one at a time.
func setProductionEnv(t *testing.T) {
	t.Helper()
	clearConfigEnv(t)
	t.Setenv("ENVIRONMENT", EnvironmentProduction)
	t.Setenv("DATABASE_URL", "postgres://prod/db")
	t.Setenv("LISTEN_ADDR", ":8080")
	t.Setenv("BRIDGE_HOSTNAME", "tidepool.example")
	t.Setenv("PLC_DIRECTORY_URL", "https://plc.directory")
	t.Setenv("BRIDGE_KEK", "sfDrM4bIeCJp01ZBTArLPJXNQlD7pcYFsod2An6UAF0=") // base64 form
	t.Setenv("ADMIN_TOKEN", "prod-admin-token")
	t.Setenv("AP_USER_ORIGIN", "https://coves.social")
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
	assert.Equal(t, "tidepool/0.1 (Coves bridge; +https://localhost)", cfg.UserAgent)
	assert.Equal(t, "coves/0.1 (+http://localhost:8091)", cfg.UserOriginUserAgent,
		"user-origin requests identify as Coves, pointing at the dev user origin")
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
	t.Setenv("AP_USER_ORIGIN", "https://people.example")

	cfg, err := Load(discardLogger())
	require.NoError(t, err)

	assert.Equal(t, "postgres://example/db", cfg.DatabaseURL)
	assert.Equal(t, ":9999", cfg.ListenAddr)
	assert.Equal(t, "tidepool.example", cfg.BridgeHostname)
	assert.Equal(t, "https://plc.directory", cfg.PLCDirectoryURL)
	assert.Equal(t, "did:plc:ewvi7nxzyoun6zhxrhs64oiz", cfg.BridgeServiceDID)
	assert.Equal(t, "custom-agent/1.0", cfg.UserAgent)
	assert.Equal(t, "coves/0.1 (+https://people.example)", cfg.UserOriginUserAgent,
		"USER_AGENT overrides only the bridge agent; the user-origin agent stays derived")
}

func TestLoad_ProductionRequiresValues(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("ENVIRONMENT", EnvironmentProduction)

	_, err := Load(discardLogger())
	require.Error(t, err, "production must not fall back to dev defaults")
	assert.Contains(t, err.Error(), "DATABASE_URL")
}

func TestLoad_ProductionWithAllValues(t *testing.T) {
	setProductionEnv(t)

	cfg, err := Load(discardLogger())
	require.NoError(t, err)
	assert.False(t, cfg.IsDevelopment())
	assert.Equal(t, "tidepool/0.1 (Coves bridge; +https://tidepool.example)", cfg.UserAgent,
		"user agent default derives from the bridge hostname")
	assert.Equal(t, "coves/0.1 (+https://coves.social)", cfg.UserOriginUserAgent,
		"user-origin agent derives from AP_USER_ORIGIN")
	assert.Len(t, cfg.BridgeKEK, 32, "base64 KEK decodes to 32 bytes")
	assert.Equal(t, "prod-admin-token", cfg.AdminToken)
	assert.Equal(t, 100, cfg.BackfillMaxPosts, "tuning knobs default in every environment")
	assert.Equal(t, 4, cfg.IngestWorkers)
}

func TestLoad_ProductionRequiresAdminToken(t *testing.T) {
	setProductionEnv(t)
	t.Setenv("ADMIN_TOKEN", "")

	_, err := Load(discardLogger())
	require.Error(t, err, "production must never run on the public dev-default admin token")
	assert.Contains(t, err.Error(), "ADMIN_TOKEN")
}

func TestLoad_ProductionRequiresKEK(t *testing.T) {
	setProductionEnv(t)
	t.Setenv("BRIDGE_KEK", "")

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

func TestLoad_BridgeScheme(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := Load(discardLogger())
	require.NoError(t, err)
	assert.Equal(t, "https", cfg.BridgeScheme, "default scheme is https")

	t.Setenv("BRIDGE_SCHEME", "http")
	cfg, err = Load(discardLogger())
	require.NoError(t, err)
	assert.Equal(t, "http", cfg.BridgeScheme, "http is allowed in development")

	t.Setenv("BRIDGE_SCHEME", "gopher")
	_, err = Load(discardLogger())
	require.Error(t, err, "unknown schemes are rejected")
}

func TestLoad_AllowDevRequestCrawl(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := Load(discardLogger())
	require.NoError(t, err)
	assert.False(t, cfg.AllowDevRequestCrawl, "off by default: dev logs requestCrawl instead of sending")

	t.Setenv("ALLOW_DEV_REQUEST_CRAWL", "1")
	cfg, err = Load(discardLogger())
	require.NoError(t, err)
	assert.True(t, cfg.AllowDevRequestCrawl, "the e2e harness turns real dev requestCrawl on")
}

func TestLoad_AllowDevRequestCrawlRefusedInProduction(t *testing.T) {
	setProductionEnv(t)
	t.Setenv("ALLOW_DEV_REQUEST_CRAWL", "1")

	_, err := Load(discardLogger())
	require.Error(t, err, "production always sends; the dev override set there is a config mistake")
	assert.Contains(t, err.Error(), "ALLOW_DEV_REQUEST_CRAWL")
}

func TestLoad_BridgeSchemeHTTPRefusedInProduction(t *testing.T) {
	setProductionEnv(t)
	t.Setenv("BRIDGE_SCHEME", "http")

	_, err := Load(discardLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "BRIDGE_SCHEME")
	assert.Contains(t, err.Error(), "production",
		"the refusal must come from the production branch, not generic scheme validation")
}

func TestLoad_APUserOrigin(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := Load(discardLogger())
	require.NoError(t, err)
	assert.Equal(t, "http://localhost:8091", cfg.APUserOrigin,
		"the dev default serves user actors off the local listener")

	t.Setenv("AP_USER_ORIGIN", "https://coves.social")
	cfg, err = Load(discardLogger())
	require.NoError(t, err)
	assert.Equal(t, "https://coves.social", cfg.APUserOrigin)
}

func TestLoad_APUserOriginRequiredInProduction(t *testing.T) {
	setProductionEnv(t)
	t.Setenv("AP_USER_ORIGIN", "")

	_, err := Load(discardLogger())
	require.Error(t, err,
		"the origin is baked into every minted actor_id; production must state it")
	assert.Contains(t, err.Error(), "AP_USER_ORIGIN")
}

// TestLoad_APUserOriginMustNotShadowBridgeHostname: bridged handles are
// subdomains of BRIDGE_HOSTNAME resolved off r.Host, so a user origin at or
// under that name would swallow the handle namespace — and the Host router
// could not tell the two surfaces apart.
func TestLoad_APUserOriginMustNotShadowBridgeHostname(t *testing.T) {
	for _, tc := range []struct {
		name       string
		hostname   string
		userOrigin string
		wantErr    bool
	}{
		{
			name:       "a distinct origin is fine",
			hostname:   "tidepool.example",
			userOrigin: "https://coves.social",
		},
		{
			name:       "the bridge hostname itself",
			hostname:   "tidepool.example",
			userOrigin: "https://tidepool.example",
			wantErr:    true,
		},
		{
			name:       "a subdomain of the bridge hostname",
			hostname:   "tidepool.example",
			userOrigin: "https://users.tidepool.example",
			wantErr:    true,
		},
		{
			name:       "a deep subdomain of the bridge hostname",
			hostname:   "tidepool.example",
			userOrigin: "https://ap.users.tidepool.example",
			wantErr:    true,
		},
		{
			// Label-boundary check, both directions: nottidepool.example is
			// not under tidepool.example.
			name:       "suffix-adjacent name is not a subdomain",
			hostname:   "tidepool.example",
			userOrigin: "https://nottidepool.example",
		},
		{
			// The dev defaults are exactly this shape: BRIDGE_HOSTNAME
			// localhost with the user origin on :8091. A different port is a
			// different authority, so the comparison is on host:port.
			name:       "same name on another port",
			hostname:   "localhost",
			userOrigin: "http://localhost:8091",
		},
		{
			name:       "missing scheme",
			hostname:   "tidepool.example",
			userOrigin: "coves.social",
			wantErr:    true,
		},
		{
			name:       "not a URL at all",
			hostname:   "tidepool.example",
			userOrigin: "://",
			wantErr:    true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearConfigEnv(t)
			t.Setenv("BRIDGE_HOSTNAME", tc.hostname)
			t.Setenv("AP_USER_ORIGIN", tc.userOrigin)

			cfg, err := Load(discardLogger())
			if tc.wantErr {
				require.Error(t, err, "AP_USER_ORIGIN %q with BRIDGE_HOSTNAME %q must be refused",
					tc.userOrigin, tc.hostname)
				assert.Contains(t, err.Error(), "AP_USER_ORIGIN")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.userOrigin, cfg.APUserOrigin)
		})
	}
}

// TestLoad_APHostFallthroughDev: unlike every other dev flag this one is ON
// by default in development — a laptop is reached by IP or tunnel hostname,
// and a default-off flag would make local runs 421 everything.
func TestLoad_APHostFallthroughDev(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := Load(discardLogger())
	require.NoError(t, err)
	assert.True(t, cfg.APHostFallthroughDev, "development falls through by default")

	t.Setenv("AP_HOST_FALLTHROUGH_DEV", "0")
	cfg, err = Load(discardLogger())
	require.NoError(t, err)
	assert.False(t, cfg.APHostFallthroughDev,
		"a developer must be able to rehearse the production posture locally")

	t.Setenv("AP_HOST_FALLTHROUGH_DEV", "maybe")
	_, err = Load(discardLogger())
	require.Error(t, err, "a default-ON flag disabled by a typo would be invisible")
}

func TestLoad_APHostFallthroughRefusedInProduction(t *testing.T) {
	setProductionEnv(t)
	t.Setenv("AP_HOST_FALLTHROUGH_DEV", "1")

	_, err := Load(discardLogger())
	require.Error(t, err,
		"an authenticated write surface must not answer under an attacker-chosen Host")
	assert.Contains(t, err.Error(), "AP_HOST_FALLTHROUGH_DEV")
}

// TestLoad_ProductionDefaultsFallthroughOff: leaving the flag unset in
// production must not inherit development's default-ON.
func TestLoad_ProductionDefaultsFallthroughOff(t *testing.T) {
	setProductionEnv(t)

	cfg, err := Load(discardLogger())
	require.NoError(t, err)
	assert.False(t, cfg.APHostFallthroughDev, "production never falls through")
}

// TestLoad_APUserOriginMustBeBareOrigin: the value is concatenated into
// every minted actor_id, keyId, and webfinger href. Anything past
// scheme://host — a trailing slash, a path, a query, a fragment, userinfo —
// is silently baked into identities that are FROZEN once minted, so it has
// to be refused at startup rather than discovered in a peer's parser.
func TestLoad_APUserOriginMustBeBareOrigin(t *testing.T) {
	for _, origin := range []string{
		"https://coves.social/",
		"https://coves.social/ap",
		"https://coves.social/ap/",
		"https://coves.social?x=1",
		"https://coves.social/#frag",
		"https://coves.social#frag",
		"https://user@coves.social",
		"https://user:pass@coves.social",
	} {
		t.Run(origin, func(t *testing.T) {
			clearConfigEnv(t)
			t.Setenv("BRIDGE_HOSTNAME", "tidepool.example")
			t.Setenv("AP_USER_ORIGIN", origin)

			_, err := Load(discardLogger())
			require.Error(t, err, "%q is not a bare origin", origin)
			assert.Contains(t, err.Error(), "AP_USER_ORIGIN")
		})
	}
}

// TestLoad_APUserOriginCanonicalized: two spellings of one authority must
// not mint two namespaces. The stored value is what actor_ids are built
// from, so canonicalization happens ONCE, here.
func TestLoad_APUserOriginCanonicalized(t *testing.T) {
	for _, tc := range []struct{ raw, want string }{
		{"https://coves.social:443", "https://coves.social"},
		{"http://coves.social:80", "http://coves.social"},
		{"HTTPS://Coves.Social", "https://coves.social"},
		{"https://coves.social.", "https://coves.social"},
		{"https://coves.social", "https://coves.social"},
		// A non-default port is part of the authority and must survive.
		{"http://localhost:8091", "http://localhost:8091"},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			clearConfigEnv(t)
			t.Setenv("BRIDGE_HOSTNAME", "tidepool.example")
			t.Setenv("AP_USER_ORIGIN", tc.raw)

			cfg, err := Load(discardLogger())
			require.NoError(t, err)
			assert.Equal(t, tc.want, cfg.APUserOrigin,
				"the canonical origin is what every minted actor_id carries")
		})
	}
}

// TestLoad_APUserOriginRequiresHTTPSInProduction: an http origin in
// production would publish actor ids that peers fetch in plaintext, and
// signature verification would carry over an unauthenticated channel.
func TestLoad_APUserOriginRequiresHTTPSInProduction(t *testing.T) {
	setProductionEnv(t)
	t.Setenv("AP_USER_ORIGIN", "http://coves.social")

	_, err := Load(discardLogger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AP_USER_ORIGIN")

	// Development still federates with a plain-HTTP Lemmy in the compose
	// network, so http stays legal there.
	clearConfigEnv(t)
	t.Setenv("BRIDGE_HOSTNAME", "tidepool.example")
	t.Setenv("AP_USER_ORIGIN", "http://coves.social")
	cfg, err := Load(discardLogger())
	require.NoError(t, err)
	assert.Equal(t, "http://coves.social", cfg.APUserOrigin)
}

// TestLoad_APUserOriginShadowCheckIsCanonical: the shadow check must run on
// the canonical host, or a second spelling of BRIDGE_HOSTNAME walks straight
// past it and takes over the bridged-handle namespace.
func TestLoad_APUserOriginShadowCheckIsCanonical(t *testing.T) {
	for _, origin := range []string{
		"https://tdpl.io:443",
		"https://tdpl.io.",
		"https://TDPL.IO",
		"https://users.tdpl.io:443",
		"https://users.TDPL.io.",
	} {
		t.Run(origin, func(t *testing.T) {
			clearConfigEnv(t)
			t.Setenv("BRIDGE_HOSTNAME", "tdpl.io")
			t.Setenv("AP_USER_ORIGIN", origin)

			_, err := Load(discardLogger())
			require.Error(t, err, "%q is BRIDGE_HOSTNAME wearing a different spelling", origin)
			assert.Contains(t, err.Error(), "AP_USER_ORIGIN")
		})
	}
}

// TestLoad_APUserOriginShadowCheckCanonicalizesTheBridgeSide: the check
// canonicalized only the ORIGIN side, so its own cited defence
// ("https://TDPL.IO:443") walked past it whenever the second spelling was on
// the BRIDGE_HOSTNAME side instead. Both sides must reduce with the SAME rule
// the Host router uses, or the check and the router disagree about what "the
// same authority" means — and the router is the one production listens to.
func TestLoad_APUserOriginShadowCheckCanonicalizesTheBridgeSide(t *testing.T) {
	for _, tc := range []struct {
		name     string
		hostname string
		origin   string
	}{
		{"bridge side carries the default port", "tdpl.io:443", "https://tdpl.io"},
		{"bridge side is uppercase", "TDPL.IO", "https://tdpl.io"},
		{"bridge side is fully qualified", "tdpl.io.", "https://tdpl.io"},
		{"bridge side spells the port and the case", "TDPL.IO:443", "https://tdpl.io"},
		{"bridge side carries a scheme", "https://tdpl.io", "https://tdpl.io"},
		{"both sides wear a different spelling", "TDPL.IO:443", "https://tdpl.io."},
		// The composed-mode trap: neither string equals the other, the check
		// passes, and then normalizeHost collapses BOTH to "tdpl.io" so the
		// router silently serves the two surfaces off one authority — in
		// production, where composition was never meant to happen.
		{"subdomain under a differently spelled bridge host", "TDPL.IO:443", "https://users.tdpl.io"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearConfigEnv(t)
			t.Setenv("BRIDGE_HOSTNAME", tc.hostname)
			t.Setenv("AP_USER_ORIGIN", tc.origin)

			_, err := Load(discardLogger())
			require.Error(t, err,
				"BRIDGE_HOSTNAME %q and AP_USER_ORIGIN %q name one authority to the Host router",
				tc.hostname, tc.origin)
			assert.Contains(t, err.Error(), "AP_USER_ORIGIN")
		})
	}
}

// ---------------------------------------------------------------------------
// Task 14 cycle K1: the Jetstream consumer's configuration.
//
// The consumer is default-OFF and stays that way until task 18 wires the e2e
// path, because it writes durable outbound state and hands work to delivery:
// a deployment that has not been wired end to end should not quietly start
// accumulating it.
// ---------------------------------------------------------------------------

func TestLoad_ConsumerIsDisabledByDefault(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := Load(discardLogger())
	require.NoError(t, err)

	assert.False(t, cfg.ConsumerEnabled,
		"the consumer is off unless a deployment says otherwise")
	assert.Empty(t, cfg.JetstreamURL,
		"and an unset JETSTREAM_URL is fine while it is off")
}

func TestLoad_EnabledConsumerRequiresAJetstreamURL(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("CONSUMER_ENABLED", "true")
	t.Setenv("COVES_MEDIA_BASE_URL", "http://localhost:8081")

	_, err := Load(discardLogger())
	require.Error(t, err,
		"a consumer with nowhere to dial would come up looking healthy and consume "+
			"NOTHING — silence is indistinguishable from a quiet stream, which is "+
			"exactly the failure the cursor and lag metrics exist to expose")
	assert.Contains(t, err.Error(), "JETSTREAM_URL",
		"the message must name the variable an operator has to set")
}

func TestLoad_EnabledConsumerAcceptsAWebSocketURL(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("CONSUMER_ENABLED", "1")
	t.Setenv("COVES_MEDIA_BASE_URL", "http://localhost:8081")
	t.Setenv("JETSTREAM_URL", "ws://localhost:6008/subscribe")

	cfg, err := Load(discardLogger())
	require.NoError(t, err)

	assert.True(t, cfg.ConsumerEnabled)
	assert.Equal(t, "ws://localhost:6008/subscribe", cfg.JetstreamURL)
}

func TestLoad_JetstreamURLMustBeAWebSocketURL(t *testing.T) {
	for _, raw := range []string{
		"https://jetstream.example/subscribe", // the scheme a copy-paste produces
		"jetstream.example",                   // no scheme at all
		"://nonsense",
	} {
		t.Run(raw, func(t *testing.T) {
			clearConfigEnv(t)
			t.Setenv("CONSUMER_ENABLED", "true")
			t.Setenv("COVES_MEDIA_BASE_URL", "http://localhost:8081")
			t.Setenv("JETSTREAM_URL", raw)

			_, err := Load(discardLogger())
			require.Error(t, err,
				"a bad URL must fail at BOOT: caught at dial time instead, it becomes a "+
					"reconnect loop that looks like an upstream outage")
			assert.Contains(t, err.Error(), "JETSTREAM_URL")
		})
	}
}

func TestLoad_JetstreamURLIsCarriedEvenWhileDisabled(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("JETSTREAM_URL", "wss://jetstream.example/subscribe")

	cfg, err := Load(discardLogger())
	require.NoError(t, err,
		"a URL configured ahead of the flag is not an error — that is how a deployment "+
			"is staged before being switched on")
	assert.False(t, cfg.ConsumerEnabled)
	assert.Equal(t, "wss://jetstream.example/subscribe", cfg.JetstreamURL)
}

func TestLoad_ConsumerEnabledRejectsATypo(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("CONSUMER_ENABLED", "yess")

	_, err := Load(discardLogger())
	require.Error(t, err,
		"boolVarDefault semantics: an unrecognised value is refused rather than read as "+
			"false, because a flag disabled by a typo is invisible")
	assert.Contains(t, err.Error(), "CONSUMER_ENABLED")
}

func TestLoad_OutboundDefaultsOff(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := Load(discardLogger())
	require.NoError(t, err)

	assert.Equal(t, 0, cfg.OutboundWorkers,
		"OUTBOUND_WORKERS defaults to 0: delivery is OFF until a deployment is wired end to end")
	assert.False(t, cfg.OutboundDryRun, "OUTBOUND_DRY_RUN defaults to false")
	assert.False(t, cfg.OutboundDisabled, "OUTBOUND_DISABLED defaults to false")
	assert.Empty(t, cfg.OutboundDisabledActors)
}

func TestLoad_OutboundWorkersParses(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("OUTBOUND_WORKERS", "4")

	cfg, err := Load(discardLogger())
	require.NoError(t, err)
	assert.Equal(t, 4, cfg.OutboundWorkers)
}

func TestLoad_OutboundWorkersRejectsNegative(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("OUTBOUND_WORKERS", "-1")

	_, err := Load(discardLogger())
	require.Error(t, err, "a negative worker count is a config error, not silently 0")
	assert.Contains(t, err.Error(), "OUTBOUND_WORKERS")
}

func TestLoad_OutboundDryRunParses(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("OUTBOUND_DRY_RUN", "true")

	cfg, err := Load(discardLogger())
	require.NoError(t, err)
	assert.True(t, cfg.OutboundDryRun)
}

func TestLoad_OutboundDisableListsParse(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("OUTBOUND_DISABLED", "1")
	t.Setenv("OUTBOUND_DISABLED_ACTORS", "did:plc:aaa,did:plc:bbb , ")

	cfg, err := Load(discardLogger())
	require.NoError(t, err)

	assert.True(t, cfg.OutboundDisabled, "the global kill switch parses")

	assert.Contains(t, cfg.OutboundDisabledActors, "did:plc:aaa")
	assert.Contains(t, cfg.OutboundDisabledActors, "did:plc:bbb")
	assert.Len(t, cfg.OutboundDisabledActors, 2, "blank entries are dropped, not stored as empty keys")
}

// loadCapturingLog runs Load over a JSON log and returns the config, the whole
// log, and its WARN lines.
func loadCapturingLog(t *testing.T) (*Config, string, []string) {
	t.Helper()
	var buffer bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buffer, &slog.HandlerOptions{Level: slog.LevelDebug}))
	cfg, err := Load(logger)
	require.NoError(t, err)

	var warnings []string
	for _, line := range strings.Split(strings.TrimSpace(buffer.String()), "\n") {
		var record struct {
			Level string `json:"level"`
		}
		require.NoError(t, json.Unmarshal([]byte(line), &record), line)
		if record.Level == slog.LevelWarn.String() {
			warnings = append(warnings, line)
		}
	}
	return cfg, buffer.String(), warnings
}

func linesNaming(lines []string, name string) []string {
	var named []string
	for _, line := range lines {
		if strings.Contains(line, name) {
			named = append(named, line)
		}
	}
	return named
}

func TestLoad_RetiredOutboundSwitchVarsWarnWithoutTheirValues(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("OUTBOUND_DISABLED_HOSTS", "hostprobe-one.example, hostprobe-two.example")
	t.Setenv("OUTBOUND_DISABLED_COMMUNITIES", "https://communityprobe.example/c/tech")

	cfg, log, warnings := loadCapturingLog(t)

	for _, tc := range []struct{ name, other string }{
		{"OUTBOUND_DISABLED_HOSTS", "OUTBOUND_DISABLED_COMMUNITIES"},
		{"OUTBOUND_DISABLED_COMMUNITIES", "OUTBOUND_DISABLED_HOSTS"},
	} {
		named := linesNaming(warnings, tc.name)
		if assert.Len(t, named, 1, "one warning for %s", tc.name) {
			assert.Contains(t, named[0], "communities.yaml")
			assert.NotContains(t, named[0], tc.other, "each variable has its own warning")
		}
	}

	rendered := fmt.Sprintf("%+v", *cfg)
	for _, probe := range []string{"hostprobe", "communityprobe"} {
		assert.NotContains(t, log, probe, "the value stays out of the log")
		assert.NotContains(t, rendered, probe, "the value sets no config")
	}
}

func TestLoad_RetiredOutboundSwitchVarWarnsOnlyWhenSet(t *testing.T) {
	const (
		hosts       = "OUTBOUND_DISABLED_HOSTS"
		communities = "OUTBOUND_DISABLED_COMMUNITIES"
	)
	for _, tc := range []struct {
		name    string
		present string
		value   string
		silent  string
		unset   bool
	}{
		{"hosts set, communities empty", hosts, "lemmy.world", communities, false},
		{"hosts set, communities unset", hosts, "lemmy.world", communities, true},
		{"communities set, hosts empty", communities, "https://lemmy.world/c/tech", hosts, false},
		{"communities set, hosts unset", communities, "https://lemmy.world/c/tech", hosts, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearConfigEnv(t)
			t.Setenv(tc.present, tc.value)
			if tc.unset {
				require.NoError(t, os.Unsetenv(tc.silent))
			}

			_, log, warnings := loadCapturingLog(t)

			assert.Len(t, linesNaming(warnings, tc.present), 1, "the set variable warns")
			assert.NotContains(t, log, tc.silent, "the other variable is not mentioned")
		})
	}
}

func TestLoad_CovesMediaBaseURLMustBeABareHTTPSOriginInProduction(t *testing.T) {
	for _, raw := range []string{
		"http://img.example",
		"https://img.example/x",
		"https://img.example/?q=1",
		"https://u@img.example",
	} {
		t.Run(raw, func(t *testing.T) {
			setProductionEnv(t)
			t.Setenv("COVES_MEDIA_BASE_URL", raw)

			_, err := Load(discardLogger())
			require.Error(t, err, "image URLs federate under this origin, so it must be a bare https origin")
			assert.Contains(t, err.Error(), "COVES_MEDIA_BASE_URL")
		})
	}
}

func TestLoad_CovesMediaBaseURLRequiredWhenConsumerEnabled(t *testing.T) {
	for _, environment := range []string{EnvironmentDevelopment, EnvironmentProduction} {
		t.Run(environment, func(t *testing.T) {
			if environment == EnvironmentProduction {
				setProductionEnv(t)
			} else {
				clearConfigEnv(t)
			}
			t.Setenv("CONSUMER_ENABLED", "true")
			t.Setenv("JETSTREAM_URL", "ws://localhost:6008/subscribe")

			_, err := Load(discardLogger())
			require.Error(t, err, "the enqueuer translates image placements, so it needs the media base")
			assert.Contains(t, err.Error(), "COVES_MEDIA_BASE_URL")
		})
	}
}

func TestLoad_CovesMediaBaseURLAccepted(t *testing.T) {
	tests := []struct {
		name        string
		environment string
		consumer    bool
		raw         string
		expected    string
	}{
		{name: "production https origin", environment: EnvironmentProduction, consumer: true,
			raw: "https://img.example", expected: "https://img.example"},
		{name: "development http origin with port", environment: EnvironmentDevelopment, consumer: true,
			raw: "http://relay:8081", expected: "http://relay:8081"},
		{name: "unset while the consumer is off", environment: EnvironmentProduction, consumer: false,
			raw: "", expected: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.environment == EnvironmentProduction {
				setProductionEnv(t)
			} else {
				clearConfigEnv(t)
			}
			if test.consumer {
				t.Setenv("CONSUMER_ENABLED", "true")
				t.Setenv("JETSTREAM_URL", "ws://localhost:6008/subscribe")
			}
			t.Setenv("COVES_MEDIA_BASE_URL", test.raw)

			cfg, err := Load(discardLogger())
			require.NoError(t, err)
			assert.Equal(t, test.expected, cfg.CovesMediaBaseURL)
		})
	}
}

// B18.
func TestLoad_BridgedVoteRecords(t *testing.T) {
	type switches struct {
		mode     string
		dryRun   bool
		interval time.Duration
		batch    int
	}
	tests := []struct {
		name string
		env  map[string]string
		want switches
		// wantErr names the variable a Load error must mention ("" = no error).
		wantErr string
	}{
		{name: "unset", want: switches{mode: "off", interval: time.Minute, batch: 20}},
		{name: "up", env: map[string]string{"BRIDGED_VOTE_RECORDS": "up"},
			want: switches{mode: "up", interval: time.Minute, batch: 20}},
		{name: "both", env: map[string]string{"BRIDGED_VOTE_RECORDS": "both"},
			want: switches{mode: "both", interval: time.Minute, batch: 20}},
		{name: "explicit off", env: map[string]string{"BRIDGED_VOTE_RECORDS": "off"},
			want: switches{mode: "off", interval: time.Minute, batch: 20}},
		{name: "dry run, interval and batch set", env: map[string]string{
			"BRIDGED_VOTE_RECORDS": "both", "BRIDGED_VOTE_RECORDS_DRY_RUN": "true",
			"BRIDGED_VOTE_RECORDS_INTERVAL": "5m", "BRIDGED_VOTE_RECORDS_BATCH": "7",
		}, want: switches{mode: "both", dryRun: true, interval: 5 * time.Minute, batch: 7}},
		{name: "invalid mode", env: map[string]string{"BRIDGED_VOTE_RECORDS": "sideways"},
			wantErr: "BRIDGED_VOTE_RECORDS"},
		{name: "zero interval", env: map[string]string{"BRIDGED_VOTE_RECORDS_INTERVAL": "0s"},
			wantErr: "BRIDGED_VOTE_RECORDS_INTERVAL"},
		{name: "negative interval", env: map[string]string{"BRIDGED_VOTE_RECORDS_INTERVAL": "-1m"},
			wantErr: "BRIDGED_VOTE_RECORDS_INTERVAL"},
		{name: "zero batch", env: map[string]string{"BRIDGED_VOTE_RECORDS_BATCH": "0"},
			wantErr: "BRIDGED_VOTE_RECORDS_BATCH"},
		{name: "negative batch", env: map[string]string{"BRIDGED_VOTE_RECORDS_BATCH": "-3"},
			wantErr: "BRIDGED_VOTE_RECORDS_BATCH"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clearConfigEnv(t)
			for name, value := range tc.env {
				t.Setenv(name, value)
			}

			cfg, err := Load(discardLogger())

			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, switches{
				mode:     cfg.BridgedVoteRecords,
				dryRun:   cfg.BridgedVoteRecordsDryRun,
				interval: cfg.BridgedVoteRecordsInterval,
				batch:    cfg.BridgedVoteRecordsBatch,
			})
		})
	}
}

// O9: origin verification pacing and window.
func TestLoad_BridgedVoteVerify(t *testing.T) {
	type verify struct {
		hostRPS, globalRPS float64
		window             time.Duration
		batch              int
	}
	defaults := verify{hostRPS: 1, globalRPS: 5, window: 144 * time.Hour, batch: 300}
	tests := []struct {
		name string
		env  map[string]string
		want verify
		// wantErr names the variable a Load error must mention ("" = no error).
		wantErr string
	}{
		{name: "unset", want: defaults},
		{name: "all set", env: map[string]string{
			"BRIDGED_VOTE_VERIFY_HOST_RPS": "0.5", "BRIDGED_VOTE_VERIFY_GLOBAL_RPS": "12",
			"BRIDGED_VOTE_VERIFY_WINDOW": "120h", "BRIDGED_VOTE_VERIFY_BATCH": "40",
		}, want: verify{hostRPS: 0.5, globalRPS: 12, window: 120 * time.Hour, batch: 40}},
		{name: "window just under seven days", env: map[string]string{"BRIDGED_VOTE_VERIFY_WINDOW": "167h59m"},
			want: verify{hostRPS: 1, globalRPS: 5, window: 167*time.Hour + 59*time.Minute, batch: 300}},
		{name: "zero host rps", env: map[string]string{"BRIDGED_VOTE_VERIFY_HOST_RPS": "0"},
			wantErr: "BRIDGED_VOTE_VERIFY_HOST_RPS"},
		{name: "negative host rps", env: map[string]string{"BRIDGED_VOTE_VERIFY_HOST_RPS": "-1"},
			wantErr: "BRIDGED_VOTE_VERIFY_HOST_RPS"},
		{name: "unparseable host rps", env: map[string]string{"BRIDGED_VOTE_VERIFY_HOST_RPS": "fast"},
			wantErr: "BRIDGED_VOTE_VERIFY_HOST_RPS"},
		{name: "zero global rps", env: map[string]string{"BRIDGED_VOTE_VERIFY_GLOBAL_RPS": "0"},
			wantErr: "BRIDGED_VOTE_VERIFY_GLOBAL_RPS"},
		{name: "negative global rps", env: map[string]string{"BRIDGED_VOTE_VERIFY_GLOBAL_RPS": "-5"},
			wantErr: "BRIDGED_VOTE_VERIFY_GLOBAL_RPS"},
		{name: "window seven days", env: map[string]string{"BRIDGED_VOTE_VERIFY_WINDOW": "168h"},
			wantErr: "BRIDGED_VOTE_VERIFY_WINDOW"},
		{name: "window over seven days", env: map[string]string{"BRIDGED_VOTE_VERIFY_WINDOW": "200h"},
			wantErr: "BRIDGED_VOTE_VERIFY_WINDOW"},
		{name: "zero window", env: map[string]string{"BRIDGED_VOTE_VERIFY_WINDOW": "0s"},
			wantErr: "BRIDGED_VOTE_VERIFY_WINDOW"},
		{name: "negative window", env: map[string]string{"BRIDGED_VOTE_VERIFY_WINDOW": "-1h"},
			wantErr: "BRIDGED_VOTE_VERIFY_WINDOW"},
		{name: "zero batch", env: map[string]string{"BRIDGED_VOTE_VERIFY_BATCH": "0"},
			wantErr: "BRIDGED_VOTE_VERIFY_BATCH"},
		{name: "negative batch", env: map[string]string{"BRIDGED_VOTE_VERIFY_BATCH": "-2"},
			wantErr: "BRIDGED_VOTE_VERIFY_BATCH"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clearConfigEnv(t)
			for name, value := range tc.env {
				t.Setenv(name, value)
			}

			cfg, err := Load(discardLogger())

			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, verify{
				hostRPS:   cfg.BridgedVoteVerifyHostRPS,
				globalRPS: cfg.BridgedVoteVerifyGlobalRPS,
				window:    cfg.BridgedVoteVerifyWindow,
				batch:     cfg.BridgedVoteVerifyBatch,
			})
		})
	}
}

func TestLoad_CovesLabelerDID(t *testing.T) {
	for _, tc := range []struct {
		name    string
		value   string
		want    string
		wantErr bool
	}{
		{name: "unset turns the consumer off", value: "", want: ""},
		{name: "a did:web is kept", value: "did:web:coves.social", want: "did:web:coves.social"},
		{name: "a bare hostname is not a DID", value: "coves.social", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearConfigEnv(t)
			if tc.value == "" {
				t.Setenv("COVES_LABELER_DID", "")
				require.NoError(t, os.Unsetenv("COVES_LABELER_DID"))
			} else {
				t.Setenv("COVES_LABELER_DID", tc.value)
			}

			cfg, err := Load(discardLogger())
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, cfg.CovesLabelerDID)
		})
	}
}
