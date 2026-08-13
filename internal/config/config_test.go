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
		"MINT_BURST", "INGEST_WORKERS", "BRIDGE_SCHEME",
		"ALLOW_PRIVATE_FETCH", "ALLOW_DEV_REQUEST_CRAWL", "RELAY_HOSTS",
		"AP_USER_ORIGIN", "AP_HOST_FALLTHROUGH_DEV",
		"CONSUMER_ENABLED", "JETSTREAM_URL",
		"OUTBOUND_WORKERS", "OUTBOUND_DRY_RUN", "OUTBOUND_DISABLED",
		"OUTBOUND_DISABLED_HOSTS", "OUTBOUND_DISABLED_COMMUNITIES",
		"OUTBOUND_DISABLED_ACTORS",
	} {
		t.Setenv(name, "")
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
	setProductionEnv(t)

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
	assert.Empty(t, cfg.OutboundDisabledHosts, "no hosts disabled by default")
	assert.Empty(t, cfg.OutboundDisabledCommunities)
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
	t.Setenv("OUTBOUND_DISABLED_HOSTS", "lemmy.world, sh.itjust.works")
	t.Setenv("OUTBOUND_DISABLED_COMMUNITIES", "https://lemmy.world/c/tech")
	t.Setenv("OUTBOUND_DISABLED_ACTORS", "did:plc:aaa,did:plc:bbb , ")

	cfg, err := Load(discardLogger())
	require.NoError(t, err)

	assert.True(t, cfg.OutboundDisabled, "the global kill switch parses")

	assert.Contains(t, cfg.OutboundDisabledHosts, "lemmy.world")
	assert.Contains(t, cfg.OutboundDisabledHosts, "sh.itjust.works",
		"comma-separated hosts are trimmed and split into a set")
	assert.Len(t, cfg.OutboundDisabledHosts, 2)

	assert.Contains(t, cfg.OutboundDisabledCommunities, "https://lemmy.world/c/tech")

	assert.Contains(t, cfg.OutboundDisabledActors, "did:plc:aaa")
	assert.Contains(t, cfg.OutboundDisabledActors, "did:plc:bbb")
	assert.Len(t, cfg.OutboundDisabledActors, 2, "blank entries are dropped, not stored as empty keys")
}
