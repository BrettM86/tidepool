package outbound

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Cycle J: the config-backed kill switches. A blocked scope is what the worker
// parks on; the zero value must behave exactly like AllowAll.

func set(items ...string) map[string]struct{} {
	s := make(map[string]struct{}, len(items))
	for _, item := range items {
		s[item] = struct{}{}
	}
	return s
}

var switchScope = DeliveryScope{
	ActorDID:      "did:plc:actor",
	CommunityAPID: "https://lemmy.world/c/tech",
	InboxHost:     "lemmy.world",
}

func TestConfigSwitches_EmptyIsAllowAll(t *testing.T) {
	var s ConfigSwitches
	assert.True(t, s.OutboundAllowed(switchScope),
		"the zero value blocks nothing — a deployment with no switches federates normally")
	assert.False(t, s.DryRun())
}

func TestConfigSwitches_GlobalDisableBlocksEverything(t *testing.T) {
	s := ConfigSwitches{Disabled: true}
	assert.False(t, s.OutboundAllowed(switchScope), "the global kill switch blocks all scopes")
	assert.False(t, s.OutboundAllowed(DeliveryScope{ActorDID: "other", CommunityAPID: "x", InboxHost: "y"}),
		"including scopes in no disabled set")
}

func TestConfigSwitches_ScopedDisableBlocksOnlyThatScope(t *testing.T) {
	cases := []struct {
		name string
		s    ConfigSwitches
	}{
		{"host", ConfigSwitches{DisabledHosts: set("lemmy.world")}},
		{"community", ConfigSwitches{DisabledCommunities: set("https://lemmy.world/c/tech")}},
		{"actor", ConfigSwitches{DisabledActors: set("did:plc:actor")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.False(t, tc.s.OutboundAllowed(switchScope),
				"a delivery whose %s is in the disabled set is blocked", tc.name)
			// A scope that shares none of the disabled dimensions is allowed.
			assert.True(t, tc.s.OutboundAllowed(DeliveryScope{
				ActorDID:      "did:plc:other",
				CommunityAPID: "https://lemmy.world/c/other",
				InboxHost:     "sh.itjust.works",
			}), "an unrelated scope still delivers")
		})
	}
}

func TestConfigSwitches_DryRun(t *testing.T) {
	s := ConfigSwitches{Dry: true}
	assert.True(t, s.DryRun(), "dry-run is reported independently of allow/deny")
	assert.True(t, s.OutboundAllowed(switchScope),
		"dry-run does not block: the worker still claims, translates and logs — it just POSTs nothing")
}
