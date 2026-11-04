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

// switchesWith returns switches holding the given disabled sets.
func switchesWith(hosts, communities map[string]struct{}) *ConfigSwitches {
	s := &ConfigSwitches{}
	s.SetDisabledSets(hosts, communities)
	return s
}

// hostScope and communityScope each exercise one dimension, so a block on the
// other dimension cannot conceal the result.
func hostScope(host string) DeliveryScope {
	return DeliveryScope{
		ActorDID:      "did:plc:actor",
		CommunityAPID: "https://open.example/c/open",
		InboxHost:     host,
	}
}

func communityScope(community string) DeliveryScope {
	return DeliveryScope{
		ActorDID:      "did:plc:actor",
		CommunityAPID: community,
		InboxHost:     "open.example",
	}
}

func TestConfigSwitches_EmptyIsAllowAll(t *testing.T) {
	s := &ConfigSwitches{}
	assert.True(t, s.OutboundAllowed(switchScope),
		"the zero value blocks nothing — a deployment with no switches federates normally")
	assert.False(t, s.DryRun())
}

func TestConfigSwitches_GlobalDisableBlocksEverything(t *testing.T) {
	s := &ConfigSwitches{Disabled: true}
	assert.False(t, s.OutboundAllowed(switchScope), "the global kill switch blocks all scopes")
	assert.False(t, s.OutboundAllowed(DeliveryScope{ActorDID: "other", CommunityAPID: "x", InboxHost: "y"}),
		"including scopes in no disabled set")
}

func TestConfigSwitches_ScopedDisableBlocksOnlyThatScope(t *testing.T) {
	cases := []struct {
		name string
		s    *ConfigSwitches
	}{
		{"host", switchesWith(set("lemmy.world"), nil)},
		{"community", switchesWith(nil, set("https://lemmy.world/c/tech"))},
		{"actor", &ConfigSwitches{DisabledActors: set("did:plc:actor")}},
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
	s := &ConfigSwitches{Dry: true}
	assert.True(t, s.DryRun(), "dry-run is reported independently of allow/deny")
	assert.True(t, s.OutboundAllowed(switchScope),
		"dry-run does not block: the worker still claims, translates and logs — it just POSTs nothing")
}

func TestConfigSwitches_SwapReplacesBothSets(t *testing.T) {
	const (
		hostA      = "a.example"
		hostB      = "b.example"
		communityA = "https://shared.example/c/a"
		communityB = "https://shared.example/c/b"
	)
	s := switchesWith(set(hostA), set(communityA))
	assert.False(t, s.OutboundAllowed(hostScope(hostA)), "before the swap, set A blocks its host")
	assert.False(t, s.OutboundAllowed(communityScope(communityA)), "before the swap, set A blocks its community")
	assert.True(t, s.OutboundAllowed(hostScope(hostB)), "before the swap, set B's host is open")
	assert.True(t, s.OutboundAllowed(communityScope(communityB)), "before the swap, set B's community is open")

	s.SetDisabledSets(set(hostB), set(communityB))

	assert.True(t, s.OutboundAllowed(hostScope(hostA)), "a host only A blocked is allowed")
	assert.True(t, s.OutboundAllowed(communityScope(communityA)), "a community only A blocked is allowed")
	assert.False(t, s.OutboundAllowed(hostScope(hostB)), "a host only B blocks is refused")
	assert.False(t, s.OutboundAllowed(communityScope(communityB)), "a community only B blocks is refused")
}

// A file that drops every switch entry parses to empty sets, and that has to
// lift the block.
func TestConfigSwitches_SwapToEmptySetsLiftsBlocks(t *testing.T) {
	s := switchesWith(set("a.example"), set("https://shared.example/c/a"))
	assert.False(t, s.OutboundAllowed(hostScope("a.example")))
	assert.False(t, s.OutboundAllowed(communityScope("https://shared.example/c/a")))

	s.SetDisabledSets(nil, nil)

	assert.True(t, s.OutboundAllowed(hostScope("a.example")))
	assert.True(t, s.OutboundAllowed(communityScope("https://shared.example/c/a")))
}

func TestConfigSwitches_CommunityMatchIsExact(t *testing.T) {
	s := switchesWith(nil, set("https://lemmy.world/c/Tech"))
	assert.False(t, s.OutboundAllowed(communityScope("https://lemmy.world/c/Tech")))
	assert.True(t, s.OutboundAllowed(communityScope("https://lemmy.world/c/tech")),
		"community ids are compared exactly, unlike hosts")
}

func TestConfigSwitches_SwapLeavesStaticSwitchesAlone(t *testing.T) {
	s := &ConfigSwitches{Dry: true, DisabledActors: set("did:plc:blocked")}
	s.SetDisabledSets(set("swapped.example"), nil)

	assert.False(t, s.OutboundAllowed(hostScope("swapped.example")), "the swap took effect")
	assert.True(t, s.DryRun(), "Dry survives a swap")
	assert.False(t, s.OutboundAllowed(DeliveryScope{
		ActorDID:      "did:plc:blocked",
		CommunityAPID: "https://open.example/c/open",
		InboxHost:     "open.example",
	}), "DisabledActors survives a swap")

	disabled := &ConfigSwitches{Disabled: true}
	disabled.SetDisabledSets(set("swapped.example"), nil)
	assert.False(t, disabled.OutboundAllowed(hostScope("open.example")), "Disabled survives a swap")
}
